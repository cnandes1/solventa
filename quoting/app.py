from __future__ import annotations

import json
import logging
import os
import threading
import time
import uuid
from collections import defaultdict
from datetime import datetime, timezone

import redis
import requests
from flask import Flask, jsonify, request

from storage import ProfileViewRepository

app = Flask(__name__)

SERVICE_NAME = "quoting"
INSTANCE_ID = os.environ.get("INSTANCE_ID", "quoting-a")
SQLITE_PATH = os.environ.get("SQLITE_PATH", f"/data/{INSTANCE_ID}.db")
MATERIALIZER_GROUP = os.environ.get("MATERIALIZER_GROUP", f"{INSTANCE_ID}-materializer")
REDIS_BUSINESS_URL = os.environ.get("REDIS_BUSINESS_URL", "redis://redis-business:6379/0")
REDIS_CONTROL_URL = os.environ.get("REDIS_CONTROL_URL", "redis://redis-control:6379/0")
PROFILING_URL = os.environ.get("PROFILING_URL", "http://profiling:7500").rstrip("/")
HEALTH_CHANNEL = os.environ.get("HEALTH_CHANNEL", "health-heartbeat")
MAX_PROFILE_AGE_SECONDS = float(os.environ.get("MAX_PROFILE_AGE_SECONDS", "300"))
PENDING_CLAIM_IDLE_MS = int(os.environ.get("PENDING_CLAIM_IDLE_MS", "5000"))
PROFILE_HEALTH_INTERVAL_SECONDS = float(os.environ.get("PROFILE_HEALTH_INTERVAL_SECONDS", "2"))
PROFILE_HEALTH_TIMEOUT_SECONDS = float(os.environ.get("PROFILE_HEALTH_TIMEOUT_SECONDS", "1"))
STREAM_PROFILE_UPDATED = "profile-updated"
STREAM_REFRESH_REQUESTS = "profile-refresh-requests"


def redis_client(url: str) -> redis.Redis:
    return redis.Redis.from_url(
        url,
        decode_responses=True,
        socket_connect_timeout=1,
        socket_timeout=5,
        health_check_interval=10,
        retry_on_timeout=True,
    )


business = redis_client(REDIS_BUSINESS_URL)
control = redis_client(REDIS_CONTROL_URL)
repository = ProfileViewRepository(SQLITE_PATH)


class Metrics:
    def __init__(self):
        self._lock = threading.Lock()
        self._values = defaultdict(int)

    def increment(self, name: str, amount: int = 1) -> None:
        with self._lock:
            self._values[name] += amount

    def snapshot(self) -> dict:
        with self._lock:
            return dict(self._values)


metrics = Metrics()
_state_lock = threading.Lock()
_business_available = False
_profiling_available = False
_health_waiters: dict[str, threading.Event] = {}
_materializer_paused = False


def log_event(event: str, **fields) -> None:
    print(json.dumps({
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "service": SERVICE_NAME,
        "instanceId": INSTANCE_ID,
        "event": event,
        **fields,
    }), flush=True)


def set_runtime_state(*, business_available: bool | None = None, profiling_available: bool | None = None) -> None:
    global _business_available, _profiling_available
    with _state_lock:
        if business_available is not None:
            _business_available = business_available
        if profiling_available is not None:
            _profiling_available = profiling_available


def runtime_state() -> dict:
    with _state_lock:
        return {
            "businessBusAvailable": _business_available,
            "profilingAvailable": _profiling_available,
            "materializerPaused": _materializer_paused,
        }


def materializer_paused() -> bool:
    with _state_lock:
        return _materializer_paused


def calculate_premium(risk_score: float) -> float:
    return round(100.0 + risk_score * 3.5, 2)


def ensure_group() -> None:
    try:
        business.xgroup_create(STREAM_PROFILE_UPDATED, MATERIALIZER_GROUP, id="0", mkstream=True)
    except redis.exceptions.ResponseError as exc:
        if "BUSYGROUP" not in str(exc):
            raise


def parse_event(fields: dict) -> dict:
    return {
        "eventId": fields["eventId"],
        "eventType": fields.get("eventType", "ProfileUpdated"),
        "correlationId": fields.get("correlationId", ""),
        "customerId": fields["customerId"],
        "version": int(fields["version"]),
        "riskScore": float(fields["riskScore"]),
        "riskLevel": fields.get("riskLevel"),
        "timestamp": fields["timestamp"],
    }


def process_message(message_id: str, fields: dict, recovered: bool = False) -> None:
    event = parse_event(fields)
    result = repository.apply_event(event)
    metrics.increment("events_consumed")
    metrics.increment(f"decision_{result['decision'].lower()}")
    if result["decision"] == "DUPLICATE":
        metrics.increment("events_duplicate_received")
    elif result["decision"] == "OLD_VERSION":
        metrics.increment("old_versions_rejected")
    if recovered:
        metrics.increment("pending_recovered")
    business.xack(STREAM_PROFILE_UPDATED, MATERIALIZER_GROUP, message_id)
    metrics.increment("events_acked")
    log_event(
        "PROFILE_MATERIALIZED",
        eventId=event["eventId"], correlationId=event["correlationId"],
        customerId=event["customerId"], profileVersion=event["version"],
        previousVersion=result["previousVersion"], result=result["decision"],
    )


def recover_pending() -> None:
    response = business.xautoclaim(
        STREAM_PROFILE_UPDATED,
        MATERIALIZER_GROUP,
        INSTANCE_ID,
        min_idle_time=PENDING_CLAIM_IDLE_MS,
        start_id="0-0",
        count=100,
    )
    messages = response[1] if len(response) > 1 else []
    if messages:
        metrics.increment("pending_claimed", len(messages))
    for message_id, fields in messages:
        process_message(message_id, fields, recovered=True)


def materializer_worker() -> None:
    last_recovery = 0.0
    while True:
        try:
            ensure_group()
            set_runtime_state(business_available=True)
            if not materializer_paused() and time.monotonic() - last_recovery >= max(1.0, PENDING_CLAIM_IDLE_MS / 1000.0):
                recover_pending()
                last_recovery = time.monotonic()
            response = business.xreadgroup(
                MATERIALIZER_GROUP,
                INSTANCE_ID,
                {STREAM_PROFILE_UPDATED: ">"},
                count=20,
                block=2000,
            )
            for _, messages in response:
                for message_id, fields in messages:
                    if materializer_paused():
                        metrics.increment("pending_created_for_experiment")
                        continue
                    try:
                        process_message(message_id, fields)
                    except Exception as exc:
                        log_event("PROFILE_MATERIALIZATION_ERROR", messageId=message_id, result=str(exc))
        except Exception as exc:
            set_runtime_state(business_available=False)
            log_event("MATERIALIZER_RECONNECT", result=str(exc))
            time.sleep(1)


def control_listener_worker() -> None:
    while True:
        pubsub = None
        try:
            pubsub = control.pubsub(ignore_subscribe_messages=True)
            pubsub.subscribe(HEALTH_CHANNEL)
            for message in pubsub.listen():
                data = json.loads(message["data"])
                if data.get("eventType") == "HealthPing" and data.get("targetInstanceId") in (INSTANCE_ID, SERVICE_NAME):
                    control.publish(HEALTH_CHANNEL, json.dumps({
                        "eventType": "HealthEcho",
                        "correlationId": data["correlationId"],
                        "instanceId": INSTANCE_ID,
                        "serviceName": SERVICE_NAME,
                        "timestamp": datetime.now(timezone.utc).isoformat(),
                        "status": "UP",
                    }))
                elif data.get("eventType") == "HealthEcho" and data.get("instanceId") == "profiling-1":
                    with _state_lock:
                        waiter = _health_waiters.get(data.get("correlationId"))
                    if waiter:
                        waiter.set()
        except Exception as exc:
            log_event("CONTROL_SUBSCRIBER_RECONNECT", result=str(exc))
            time.sleep(1)
        finally:
            if pubsub:
                pubsub.close()


def profiling_health_worker() -> None:
    while True:
        correlation_id = str(uuid.uuid4())
        waiter = threading.Event()
        with _state_lock:
            _health_waiters[correlation_id] = waiter
        try:
            control.publish(HEALTH_CHANNEL, json.dumps({
                "eventType": "HealthPing",
                "correlationId": correlation_id,
                "targetInstanceId": "profiling-1",
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "requestedBy": INSTANCE_ID,
            }))
            set_runtime_state(profiling_available=waiter.wait(PROFILE_HEALTH_TIMEOUT_SECONDS))
        except redis.RedisError:
            set_runtime_state(profiling_available=False)
        finally:
            with _state_lock:
                _health_waiters.pop(correlation_id, None)
        time.sleep(PROFILE_HEALTH_INTERVAL_SECONDS)


def profile_age_seconds(profile: dict) -> float:
    source_time = datetime.fromisoformat(profile["source_timestamp"].replace("Z", "+00:00"))
    return max(0.0, (datetime.now(timezone.utc) - source_time).total_seconds())


@app.get("/quotes/<customer_id>")
def get_quote(customer_id):
    started = time.monotonic()
    profile = repository.get(customer_id)
    if profile is None:
        metrics.increment("quotes_functional_failure")
        return jsonify({
            "error": "NO_VALID_PROFILE_AVAILABLE",
            "customerId": customer_id,
            "degraded": True,
            "resultCategory": "FUNCTIONAL_FAILURE",
        }), 503
    age_seconds = profile_age_seconds(profile)
    if age_seconds > MAX_PROFILE_AGE_SECONDS:
        metrics.increment("quotes_functional_failure")
        return jsonify({
            "error": "PROFILE_EXPIRED",
            "customerId": customer_id,
            "profileAgeSeconds": round(age_seconds, 1),
            "maxProfileAgeSeconds": MAX_PROFILE_AGE_SECONDS,
            "degraded": True,
            "resultCategory": "FUNCTIONAL_FAILURE",
        }), 503

    state = runtime_state()
    degraded = not state["businessBusAvailable"] or not state["profilingAvailable"]
    latency_ms = round((time.monotonic() - started) * 1000, 2)
    metrics.increment("quotes_success")
    metrics.increment("quotes_degraded_success" if degraded else "quotes_normal_success")
    return jsonify({
        "quoteId": str(uuid.uuid4()),
        "customerId": customer_id,
        "premium": calculate_premium(float(profile["risk_score"])),
        "profile": {
            "version": int(profile["version"]),
            "ageSeconds": round(age_seconds, 1),
            "source": "materialized-view",
            "riskLevel": profile["risk_level"],
        },
        "degraded": degraded,
        "resultCategory": "DEGRADED_SUCCESS" if degraded else "SUCCESS",
        "instanceId": INSTANCE_ID,
        "latencyMs": latency_ms,
    }), 200


@app.post("/quotes/<customer_id>/request-refresh")
def request_refresh(customer_id):
    event = {
        "eventId": str(uuid.uuid4()),
        "eventType": "ProfileRefreshRequested",
        "correlationId": str(uuid.uuid4()),
        "customerId": customer_id,
        "requestedAt": datetime.now(timezone.utc).isoformat(),
        "requestedBy": INSTANCE_ID,
    }
    try:
        business.xadd(STREAM_REFRESH_REQUESTS, event, maxlen=10000, approximate=True)
        metrics.increment("refresh_requests_published")
    except redis.RedisError as exc:
        return jsonify({"error": "BUSINESS_BUS_UNAVAILABLE", "detail": str(exc)}), 503
    return jsonify({"status": "REQUESTED", **event}), 202


@app.get("/sync/quotes/<customer_id>")
def get_sync_quote(customer_id):
    """Experimental synchronous baseline; never used by the EDA journey."""
    metrics.increment("sync_calls_quoting_to_profiling")
    try:
        response = requests.get(f"{PROFILING_URL}/profiles/{customer_id}/sync", timeout=2)
        response.raise_for_status()
        profile = response.json()
    except requests.RequestException as exc:
        metrics.increment("sync_quote_failures")
        return jsonify({"error": "SYNCHRONOUS_DEPENDENCY_FAILURE", "detail": str(exc)}), 503
    return jsonify({
        "customerId": customer_id,
        "premium": calculate_premium(float(profile["riskScore"])),
        "profileSource": profile["source"],
        "baseline": "SYNCHRONOUS",
    }), 200


@app.get("/materialized-profiles/<customer_id>")
def materialized_profile(customer_id):
    profile = repository.get(customer_id)
    return (jsonify(profile), 200) if profile else (jsonify({"error": "PROFILE_NOT_FOUND"}), 404)


@app.post("/admin/materializer")
def admin_materializer():
    global _materializer_paused
    body = request.get_json(silent=True) or {}
    with _state_lock:
        _materializer_paused = bool(body.get("paused", False))
        paused = _materializer_paused
    log_event("MATERIALIZER_STATE_CHANGED", result="PAUSED" if paused else "RUNNING")
    return jsonify({"status": "ok", "paused": paused}), 200


@app.get("/metrics")
def get_metrics():
    return jsonify({**metrics.snapshot(), "repository": repository.stats(), "runtime": runtime_state()}), 200


@app.get("/health")
def health():
    return jsonify({"status": "ok", "instanceId": INSTANCE_ID, "sqlitePath": SQLITE_PATH,
                    "runtime": runtime_state()}), 200


def start_background_workers() -> None:
    for target, name in (
        (materializer_worker, "profile-materializer"),
        (control_listener_worker, "control-listener"),
        (profiling_health_worker, "profiling-health"),
    ):
        threading.Thread(target=target, name=name, daemon=True).start()


if __name__ == "__main__":
    logging.getLogger("werkzeug").setLevel(logging.WARNING)
    start_background_workers()
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", "7000")), threaded=True)
