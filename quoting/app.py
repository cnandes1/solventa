import json
import os
import threading
import time

import redis
import requests
from flask import Flask, jsonify

app = Flask(__name__)

# ------------------------------------------------------------------ settings
SERVICE_NAME = "quoting"
INSTANCE_ID = os.environ.get("INSTANCE_ID", "quoting-a")

REDIS_URL = os.environ.get("REDIS_URL", "redis://redis:6379/0")
HEALTH_CHANNEL = os.environ.get("HEALTH_CHANNEL", "health-heartbeat")

STREAM_PROFILE_UPDATED = os.environ.get("STREAM_PROFILE_UPDATED", "profile-updated")
CONSUMER_GROUP = os.environ.get("CONSUMER_GROUP", "quoting-cg")

PROFILING_URL = os.environ.get("PROFILING_URL", "http://profiling:7500").rstrip("/")

DEFAULT_RISK_SCORE = float(os.environ.get("DEFAULT_RISK_SCORE", "60"))

r = redis.Redis.from_url(REDIS_URL, decode_responses=True)


def _calculate_quote(risk_score: float) -> float:
    BASE_PREMIUM = 100.0
    return round(BASE_PREMIUM + risk_score * 3.5, 2)


# ------------------------------------------------------------------ materialized view
def _view_key(customer_id: str) -> str:
    return f"quote:view:{customer_id}"


def _read_view(customer_id: str):
    data = r.hgetall(_view_key(customer_id))
    if not data:
        return None
    return {
        "risk_score": float(data["risk_score"]),
        "version": int(data["version"]),
        "event_id": data["event_id"],
        "updated_at": float(data["updated_at"]),
    }


def _apply_event(customer_id: str, version: int, event_id: str, risk_score: float) -> bool:
    current = _read_view(customer_id)
    if current is not None and version <= current["version"]:
        return False

    r.hset(_view_key(customer_id), mapping={
        "risk_score": risk_score,
        "version": version,
        "event_id": event_id,
        "updated_at": time.time(),
    })
    return True


# --------------------------------------------------------------- event consumer
def _ensure_consumer_group() -> None:
    try:
        r.xgroup_create(STREAM_PROFILE_UPDATED, CONSUMER_GROUP, id="0", mkstream=True)
    except redis.exceptions.ResponseError as exc:
        if "BUSYGROUP" not in str(exc):
            raise


def _process_message(message_id: str, fields: dict) -> None:
    customer_id = fields.get("customerId")
    if not customer_id:
        r.xack(STREAM_PROFILE_UPDATED, CONSUMER_GROUP, message_id)
        return

    version = int(fields.get("version", "0"))
    event_id = fields.get("eventId", "")
    risk_score = float(fields.get("riskScore", DEFAULT_RISK_SCORE))

    applied = _apply_event(customer_id, version, event_id, risk_score)
    print(
        f"[consumer] customerId={customer_id} version={version} eventId={event_id} "
        f"applied={applied}",
        flush=True,
    )
    r.xack(STREAM_PROFILE_UPDATED, CONSUMER_GROUP, message_id)


def _claim_pending() -> None:
    try:
        pending = r.xpending_range(
            STREAM_PROFILE_UPDATED, CONSUMER_GROUP, min="-", max="+", count=100
        )
    except redis.exceptions.ResponseError:
        return

    for p in pending:
        message_id = p["message_id"]
        try:
            claimed = r.xclaim(
                STREAM_PROFILE_UPDATED, CONSUMER_GROUP, INSTANCE_ID,
                min_idle_time=0, message_ids=[message_id],
            )
        except redis.exceptions.ResponseError:
            continue
        for msg_id, fields in claimed:
            _process_message(msg_id, fields)


def _consumer_loop() -> None:
    _ensure_consumer_group()
    _claim_pending()

    while True:
        try:
            response = r.xreadgroup(
                CONSUMER_GROUP, INSTANCE_ID,
                {STREAM_PROFILE_UPDATED: ">"},
                count=10, block=5000,
            )
        except Exception as exc:
            print(f"[consumer] error reading stream: {exc}", flush=True)
            time.sleep(1)
            continue

        if not response:
            continue

        for _stream_name, messages in response:
            for message_id, fields in messages:
                try:
                    _process_message(message_id, fields)
                except Exception as exc:
                    print(f"[consumer] error processing {message_id}: {exc}", flush=True)


def _start_consumer() -> None:
    thread = threading.Thread(target=_consumer_loop, name="profile-consumer", daemon=True)
    thread.start()


# --------------------------------------------------------------------- routes
def _quote_response(customer_id: str):
    start = time.time()
    view = _read_view(customer_id)

    if view is not None:
        score = view["risk_score"]
        source = "materialized-view"
        version = view["version"]
    else:
        score = DEFAULT_RISK_SCORE
        source = "default"
        version = 0

    quote_value = _calculate_quote(score)
    latency_ms = (time.time() - start) * 1000.0

    return {
        "customer_id": customer_id,
        "risk_score": score,
        "quote": quote_value,
        "source": source,
        "version": version,
        "latency_ms": round(latency_ms, 1),
    }


@app.route("/quotes/<customer_id>", methods=["GET"])
def get_quote(customer_id):
    """Query (4Q): reads the materialized view."""
    return jsonify(_quote_response(customer_id)), 200


@app.route("/quotes/<customer_id>/request-refresh", methods=["POST"])
def request_refresh(customer_id):
    def _fire():
        try:
            requests.post(
                f"{PROFILING_URL}/profiles/{customer_id}/refresh",
                timeout=5,
            )
        except requests.RequestException as exc:
            print(f"[request-refresh] failed notifying Profiling: {exc}", flush=True)

    threading.Thread(target=_fire, daemon=True).start()
    return jsonify({"customer_id": customer_id, "status": "request_sent"}), 202


@app.route("/health", methods=["GET"])
def health():
    return jsonify({"status": "ok"}), 200


# --------------------------------------------------------- health echo (health bus)
def _health_echo_loop() -> None:
    pubsub = r.pubsub()
    pubsub.subscribe(HEALTH_CHANNEL)
    for message in pubsub.listen():
        if message.get("type") != "message":
            continue
        try:
            data = json.loads(message["data"])
        except (TypeError, ValueError):
            continue
        if data.get("type") != "ping":
            continue
        if data.get("serviceName") not in (SERVICE_NAME, INSTANCE_ID):
            continue
        try:
            r.publish(HEALTH_CHANNEL, json.dumps({
                "type": "echo",
                "correlationId": data.get("correlationId"),
                "serviceName": data.get("serviceName"),
                "instanceId": INSTANCE_ID,
                "timestamp": time.time(),
                "status": "ok",
            }))
        except Exception as exc:
            print(f"[health-echo] error publishing echo: {exc}", flush=True)


def _start_health_echo() -> None:
    thread = threading.Thread(target=_health_echo_loop, name="health-echo", daemon=True)
    thread.start()


_start_health_echo()
_start_consumer()

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 7000))
    app.run(host="0.0.0.0", port=port, threaded=True)
