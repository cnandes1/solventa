from __future__ import annotations

import json
import logging
import os
import threading
import time
import uuid
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone

import redis
import requests
from flask import Flask, Response, jsonify, request

from state import InstanceState

app = Flask(__name__)


def parse_backends(raw: str) -> dict[str, str]:
    parsed = {}
    for item in raw.split(","):
        if not item.strip():
            continue
        instance_id, url = item.split("=", 1)
        parsed[instance_id.strip()] = url.strip().rstrip("/")
    return parsed


BACKENDS = parse_backends(os.environ.get(
    "BACKENDS", "quoting-a=http://quoting-a:7000,quoting-b=http://quoting-b:7000"
))
REDIS_CONTROL_URL = os.environ.get("REDIS_CONTROL_URL", "redis://redis-control:6379/0")
HEALTH_CHANNEL = os.environ.get("HEALTH_CHANNEL", "health-heartbeat")
HEALTH_PING_INTERVAL_SECONDS = float(os.environ.get("HEALTH_PING_INTERVAL_SECONDS", "2"))
HEALTH_ECHO_TIMEOUT_SECONDS = float(os.environ.get("HEALTH_ECHO_TIMEOUT_SECONDS", "0.7"))
HEALTH_FAILURE_THRESHOLD = int(os.environ.get("HEALTH_FAILURE_THRESHOLD", "2"))
SHADOW_MIN_HEALTH_SUCCESSES = int(os.environ.get("SHADOW_MIN_HEALTH_SUCCESSES", "2"))
SHADOW_MIN_VALIDATIONS = int(os.environ.get("SHADOW_MIN_VALIDATIONS", "1"))
SHADOW_MIN_SECONDS = float(os.environ.get("SHADOW_MIN_SECONDS", "2"))
PROXY_TIMEOUT_SECONDS = float(os.environ.get("PROXY_TIMEOUT_SECONDS", "2"))

control = redis.Redis.from_url(
    REDIS_CONTROL_URL,
    decode_responses=True,
    socket_connect_timeout=1,
    socket_timeout=2,
    health_check_interval=10,
    retry_on_timeout=True,
)


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
_lock = threading.Lock()
_instances = {
    instance_id: InstanceState(
        instance_id,
        url,
        HEALTH_FAILURE_THRESHOLD,
        SHADOW_MIN_HEALTH_SUCCESSES,
        SHADOW_MIN_VALIDATIONS,
        SHADOW_MIN_SECONDS,
    )
    for instance_id, url in BACKENDS.items()
}
_echo_lock = threading.Lock()
_echo_waiters: dict[str, threading.Event] = {}
_round_robin = 0
_shadow_pool = ThreadPoolExecutor(max_workers=4, thread_name_prefix="shadow")


def log_event(event: str, **fields) -> None:
    print(json.dumps({
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "service": "gateway",
        "event": event,
        **fields,
    }), flush=True)


def log_transition(instance_id: str, transition: tuple[str, str] | None) -> None:
    if not transition:
        return
    old_state, new_state = transition
    metrics.increment(f"transition_{old_state.lower()}_{new_state.lower()}")
    log_event("INSTANCE_STATE_CHANGED", instanceId=instance_id,
              previousState=old_state, state=new_state)


def health_listener_worker() -> None:
    while True:
        pubsub = None
        try:
            pubsub = control.pubsub(ignore_subscribe_messages=True)
            pubsub.subscribe(HEALTH_CHANNEL)
            for message in pubsub.listen():
                data = json.loads(message["data"])
                if data.get("eventType") != "HealthEcho":
                    continue
                with _echo_lock:
                    waiter = _echo_waiters.get(data.get("correlationId"))
                if waiter:
                    metrics.increment("health_echo_received")
                    waiter.set()
        except Exception as exc:
            log_event("HEALTH_LISTENER_RECONNECT", result=str(exc))
            time.sleep(1)
        finally:
            if pubsub:
                pubsub.close()


def ping_instance(instance_id: str) -> tuple[bool | None, str | None]:
    correlation_id = str(uuid.uuid4())
    waiter = threading.Event()
    with _echo_lock:
        _echo_waiters[correlation_id] = waiter
    try:
        control.publish(HEALTH_CHANNEL, json.dumps({
            "eventType": "HealthPing",
            "correlationId": correlation_id,
            "targetInstanceId": instance_id,
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "requestedBy": "health-monitor",
        }))
        metrics.increment("health_ping_sent")
    except redis.RedisError as exc:
        metrics.increment("health_control_errors")
        with _echo_lock:
            _echo_waiters.pop(correlation_id, None)
        return None, f"control bus unavailable: {exc}"

    healthy = waiter.wait(HEALTH_ECHO_TIMEOUT_SECONDS)
    with _echo_lock:
        _echo_waiters.pop(correlation_id, None)
    return healthy, None if healthy else "HealthEcho timeout"


def monitor_worker() -> None:
    while True:
        started = time.monotonic()
        for instance_id in BACKENDS:
            healthy, error = ping_instance(instance_id)
            if healthy is None:
                continue
            with _lock:
                transition = _instances[instance_id].observe_health(healthy, error)
                if transition and transition[1] == "DOWN":
                    metrics.increment("instances_marked_down")
            log_transition(instance_id, transition)
        elapsed = time.monotonic() - started
        time.sleep(max(0.05, HEALTH_PING_INTERVAL_SECONDS - elapsed))


def choose_active(exclude: set[str] | None = None) -> tuple[str, str] | None:
    global _round_robin
    exclude = exclude or set()
    with _lock:
        active = [(instance_id, state.url) for instance_id, state in _instances.items()
                  if state.state == "ACTIVE" and instance_id not in exclude]
        if not active:
            return None
        selected = active[_round_robin % len(active)]
        _round_robin += 1
        return selected


def request_snapshot() -> dict:
    return {
        "method": request.method,
        "params": request.args.to_dict(flat=False),
        "data": request.get_data(),
        "headers": {key: value for key, value in request.headers.items()
                    if key.lower() in {"content-type", "accept", "x-correlation-id"}},
    }


def forward(instance_id: str, target: str, subpath: str, snapshot: dict) -> requests.Response:
    url = f"{target}/{subpath}" if subpath else target
    return requests.request(
        snapshot["method"],
        url,
        params=snapshot["params"],
        data=snapshot["data"],
        headers=snapshot["headers"],
        timeout=PROXY_TIMEOUT_SECONDS,
    )


def comparable_response(response: requests.Response) -> tuple:
    try:
        body = response.json()
    except ValueError:
        body = {}
    profile = body.get("profile") or {}
    return response.status_code, body.get("premium"), profile.get("version"), body.get("error")


def send_shadow(authoritative_signature: tuple, subpath: str, snapshot: dict) -> None:
    if snapshot["method"] != "GET":
        return
    with _lock:
        shadows = [(instance_id, state.url) for instance_id, state in _instances.items()
                   if state.state == "SHADOW"]
    for instance_id, target in shadows:
        try:
            response = forward(instance_id, target, subpath, snapshot)
            matches = comparable_response(response) == authoritative_signature
        except requests.RequestException:
            matches = False
        metrics.increment("shadow_requests")
        metrics.increment("shadow_matches" if matches else "shadow_mismatches")
        with _lock:
            transition = _instances[instance_id].observe_shadow_validation(matches)
        log_transition(instance_id, transition)


def flask_response(upstream: requests.Response, instance_id: str, failover: bool) -> Response:
    response = Response(upstream.content, status=upstream.status_code)
    response.headers["Content-Type"] = upstream.headers.get("Content-Type", "application/json")
    response.headers["X-Gateway-Target"] = instance_id
    response.headers["X-Gateway-Failover"] = str(failover).lower()
    return response


@app.get("/gateway/status")
def gateway_status():
    with _lock:
        instances = {instance_id: state.snapshot() for instance_id, state in _instances.items()}
    return jsonify({"instances": instances, "metrics": metrics.snapshot(), "config": {
        "healthPingIntervalSeconds": HEALTH_PING_INTERVAL_SECONDS,
        "healthEchoTimeoutSeconds": HEALTH_ECHO_TIMEOUT_SECONDS,
        "healthFailureThreshold": HEALTH_FAILURE_THRESHOLD,
        "shadowMinHealthSuccesses": SHADOW_MIN_HEALTH_SUCCESSES,
        "shadowMinValidations": SHADOW_MIN_VALIDATIONS,
        "shadowMinSeconds": SHADOW_MIN_SECONDS,
    }}), 200


@app.get("/metrics")
def get_metrics():
    return jsonify(metrics.snapshot()), 200


@app.get("/health")
def health():
    return jsonify({"status": "ok"}), 200


@app.route("/", defaults={"subpath": ""}, methods=["GET", "POST", "PUT", "PATCH", "DELETE"])
@app.route("/<path:subpath>", methods=["GET", "POST", "PUT", "PATCH", "DELETE"])
def proxy(subpath):
    metrics.increment("gateway_requests_total")
    snapshot = request_snapshot()
    primary = choose_active()
    if primary is None:
        metrics.increment("gateway_requests_failed")
        return jsonify({"error": "NO_ACTIVE_INSTANCE"}), 503

    primary_id, primary_url = primary
    try:
        upstream = forward(primary_id, primary_url, subpath, snapshot)
        _shadow_pool.submit(send_shadow, comparable_response(upstream), subpath, snapshot)
        metrics.increment("gateway_requests_success")
        return flask_response(upstream, primary_id, False)
    except requests.RequestException as exc:
        metrics.increment("gateway_primary_failures")
        with _lock:
            transition = _instances[primary_id].observe_health(False, str(exc))
        log_transition(primary_id, transition)

    retry = choose_active({primary_id})
    if retry is None:
        metrics.increment("gateway_failover_failed")
        metrics.increment("gateway_requests_failed")
        return jsonify({"error": "FAILOVER_TARGET_UNAVAILABLE", "primary": primary_id}), 503

    retry_id, retry_url = retry
    metrics.increment("gateway_failovers")
    try:
        upstream = forward(retry_id, retry_url, subpath, snapshot)
        metrics.increment("gateway_failover_success")
        metrics.increment("gateway_requests_success")
        _shadow_pool.submit(send_shadow, comparable_response(upstream), subpath, snapshot)
        return flask_response(upstream, retry_id, True)
    except requests.RequestException as exc:
        metrics.increment("gateway_failover_failed")
        metrics.increment("gateway_requests_failed")
        return jsonify({"error": "FAILOVER_FAILED", "detail": str(exc)}), 503


def start_background_workers() -> None:
    threading.Thread(target=health_listener_worker, name="health-listener", daemon=True).start()
    threading.Thread(target=monitor_worker, name="health-monitor", daemon=True).start()


if __name__ == "__main__":
    logging.getLogger("werkzeug").setLevel(logging.WARNING)
    start_background_workers()
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", "8080")), threaded=True)
