import json
import os
import threading
import time
import uuid

import redis
import requests
from flask import Flask, Response, jsonify, request

# ------------------------------------------------------------------ settings
def _parse_backends(raw: str) -> dict:
    backends = {}
    for item in raw.split(","):
        item = item.strip()
        if not item:
            continue
        if "=" in item:
            instance_id, url = item.split("=", 1)
        else:
            instance_id, url = item, item
        backends[instance_id.strip()] = url.strip()
    return backends


POOL_NAME = os.environ.get("POOL_NAME", "quoting")
BACKENDS = _parse_backends(os.environ.get(
    "BACKENDS",
    "quoting-a=http://quoting-a:7000,quoting-b=http://quoting-b:7001",
))

REDIS_URL = os.environ.get("REDIS_URL", "redis://redis:6379/0")
HEALTH_CHANNEL = os.environ.get("HEALTH_CHANNEL", "health-heartbeat")

PING_INTERVAL_SECONDS = float(os.environ.get("PING_INTERVAL_SECONDS", "5"))
PING_TIMEOUT_SECONDS = float(os.environ.get("PING_TIMEOUT_SECONDS", "2"))
FAILS_THRESHOLD = int(os.environ.get("FAILS_THRESHOLD", "2"))
SHADOW_CYCLES = int(os.environ.get("SHADOW_CYCLES", "2"))
PROXY_TIMEOUT_SECONDS = float(os.environ.get("PROXY_TIMEOUT_SECONDS", "5"))

app = Flask(__name__)
r = redis.Redis.from_url(REDIS_URL, decode_responses=True)

# --------------------------------------------------------------------- state
_lock = threading.Lock()
_state = {
    instance_id: {"state": "active", "fails": 0, "shadow_cycles": 0, "last_error": None, "url": url}
    for instance_id, url in BACKENDS.items()
}
_rr_counter = 0

_echoes_lock = threading.Lock()
_echoes: dict[str, dict] = {}


# ------------------------------------------------------------------ health subscriber
def _health_subscriber_loop() -> None:
    pubsub = r.pubsub()
    pubsub.subscribe(HEALTH_CHANNEL)
    for message in pubsub.listen():
        if message.get("type") != "message":
            continue
        try:
            data = json.loads(message["data"])
        except (TypeError, ValueError):
            continue
        if data.get("type") != "echo":
            continue
        correlation_id = data.get("correlationId")
        if not correlation_id:
            continue
        with _echoes_lock:
            _echoes[correlation_id] = data


def _start_health_subscriber() -> None:
    thread = threading.Thread(target=_health_subscriber_loop, name="health-subscriber", daemon=True)
    thread.start()


# ----------------------------------------------------------------------- ping-echo
def _ping_one(instance_id: str) -> tuple[str, bool, str | None]:
    correlation_id = str(uuid.uuid4())
    try:
        r.publish(HEALTH_CHANNEL, json.dumps({
            "type": "ping",
            "correlationId": correlation_id,
            "serviceName": instance_id,
            "timestamp": time.time(),
        }))
    except Exception as exc:
        return instance_id, False, f"error publishing ping: {exc}"

    deadline = time.time() + PING_TIMEOUT_SECONDS
    while time.time() < deadline:
        with _echoes_lock:
            echo = _echoes.pop(correlation_id, None)
        if echo is not None:
            return instance_id, True, None
        time.sleep(0.05)

    with _echoes_lock:
        _echoes.pop(correlation_id, None)
    return instance_id, False, "no HealthEcho within the timeout window"


def _update_state(instance_id: str, healthy: bool, error: str | None) -> None:
    info = _state[instance_id]

    if healthy:
        info["last_error"] = None
        if info["state"] == "down":
            # ACTIVE/DOWN/SHADOW: DOWN -> SHADOW (validation traffic, not
            # authoritative) before reintegrating.
            info["state"] = "shadow"
            info["fails"] = 0
            info["shadow_cycles"] = 0
        elif info["state"] == "shadow":
            info["fails"] = 0
            info["shadow_cycles"] += 1
            if info["shadow_cycles"] >= SHADOW_CYCLES:
                info["state"] = "active"
                info["shadow_cycles"] = 0
        else:  # active
            info["fails"] = 0
    else:
        info["fails"] += 1
        info["last_error"] = error
        if info["state"] in ("active", "shadow") and info["fails"] >= FAILS_THRESHOLD:
            info["state"] = "down"
            info["shadow_cycles"] = 0


def _monitor_cycle() -> None:
    if not BACKENDS:
        return

    results = [_ping_one(instance_id) for instance_id in BACKENDS]

    with _lock:
        for instance_id, healthy, error in results:
            _update_state(instance_id, healthy, error)


def _monitor_loop() -> None:
    while True:
        start = time.time()
        try:
            _monitor_cycle()
        except Exception as exc:
            print(f"[monitor] error in ping cycle: {exc}", flush=True)
        elapsed = time.time() - start
        time.sleep(max(0.0, PING_INTERVAL_SECONDS - elapsed))


def _choose_backend() -> str | None:
    """Chooses the destination for real traffic: round-robin among
    'active' instances."""
    global _rr_counter

    with _lock:
        active = [info["url"] for info in _state.values() if info["state"] == "active"]

    if not active:
        return None

    with _lock:
        idx = _rr_counter % len(active)
        _rr_counter += 1
    return active[idx]


# --------------------------------------------------------------------- routes
@app.route("/gateway/status", methods=["GET"])
def gateway_status():
    with _lock:
        instances = {
            instance_id: {
                "url": info["url"],
                "state": info["state"],
                "fails": info["fails"],
                "shadow_cycles": info["shadow_cycles"],
                "last_error": info["last_error"],
            }
            for instance_id, info in _state.items()
        }

    return jsonify({
        "pool": POOL_NAME,
        "instances": instances,
        "config": {
            "ping_interval_seconds": PING_INTERVAL_SECONDS,
            "ping_timeout_seconds": PING_TIMEOUT_SECONDS,
            "fails_threshold": FAILS_THRESHOLD,
            "shadow_cycles": SHADOW_CYCLES,
            "health_channel": HEALTH_CHANNEL,
        },
    }), 200


@app.route("/health", methods=["GET"])
def health():
    return jsonify({"status": "ok"}), 200


@app.route("/", defaults={"subpath": ""}, methods=["GET", "POST", "PUT", "PATCH", "DELETE"])
@app.route("/<path:subpath>", methods=["GET", "POST", "PUT", "PATCH", "DELETE"])
def proxy(subpath):
    target = _choose_backend()

    if target is None:
        return jsonify({
            "error": "no instance available",
        }), 503

    url = f"{target}/{subpath}" if subpath else target
    try:
        resp = requests.request(
            method=request.method,
            url=url,
            params=request.args,
            json=request.get_json(silent=True) if request.data else None,
            timeout=PROXY_TIMEOUT_SECONDS,
        )
        response = Response(resp.content, status=resp.status_code)
        response.headers["Content-Type"] = resp.headers.get("Content-Type", "application/json")
        response.headers["X-Gateway-Target"] = target
        return response
    except requests.RequestException as exc:
        return jsonify({
            "error": f"failed forwarding the request to {target}: {exc}",
            "target": target,
        }), 502


def _start_monitor() -> None:
    thread = threading.Thread(target=_monitor_loop, name="health-monitor", daemon=True)
    thread.start()


_start_health_subscriber()
_start_monitor()

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8080))
    app.run(host="0.0.0.0", port=port)
