from __future__ import annotations

import hashlib
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

from circuit_breaker import CircuitBreaker, CircuitOpenError
from voting import Vote, decide_votes

app = Flask(__name__)

SERVICE_NAME = "profiling"
INSTANCE_ID = os.environ.get("INSTANCE_ID", "profiling-1")
REDIS_BUSINESS_URL = os.environ.get("REDIS_BUSINESS_URL", "redis://redis-business:6379/0")
REDIS_CONTROL_URL = os.environ.get("REDIS_CONTROL_URL", "redis://redis-control:6379/0")
HEALTH_CHANNEL = os.environ.get("HEALTH_CHANNEL", "health-heartbeat")
OPEN_FINANCE_URL = os.environ.get("OPEN_FINANCE_URL", "http://open-finance-mock:6000").rstrip("/")
OPEN_FINANCE_TIMEOUT_MS = int(os.environ.get("OPEN_FINANCE_TIMEOUT_MS", "700"))
CB_FAILURE_THRESHOLD = int(os.environ.get("CB_FAILURE_THRESHOLD", "3"))
CB_RECOVERY_TIMEOUT_SECONDS = float(os.environ.get("CB_RECOVERY_TIMEOUT_SECONDS", "15"))
PROFILE_CACHE_MAX_AGE_SECONDS = float(os.environ.get("PROFILE_CACHE_MAX_AGE_SECONDS", "3600"))
VOTING_TIMEOUT_MS = int(os.environ.get("VOTING_TIMEOUT_MS", "1000"))
VOTING_TOLERANCE = float(os.environ.get("VOTING_TOLERANCE", "2"))

STREAM_REFRESH_REQUESTS = "profile-refresh-requests"
STREAM_CALCULATION_REQUESTS = "profile-calculation-requests"
STREAM_VOTE_RESULTS = "profiling-results"
STREAM_PROFILE_UPDATED = "profile-updated"


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


def log_event(event: str, **fields) -> None:
    print(json.dumps({
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "service": SERVICE_NAME,
        "instanceId": INSTANCE_ID,
        "event": event,
        **fields,
    }), flush=True)


def on_circuit_transition(old_state: str, new_state: str) -> None:
    metrics.increment("circuit_transition_total")
    if new_state == "OPEN":
        metrics.increment("circuit_open_count")
    log_event("CIRCUIT_TRANSITION", previousState=old_state, state=new_state)


circuit = CircuitBreaker(
    CB_FAILURE_THRESHOLD,
    CB_RECOVERY_TIMEOUT_SECONDS,
    on_transition=on_circuit_transition,
)


def cache_key(customer_id: str) -> str:
    return f"profile:cache:{customer_id}"


def read_cache(customer_id: str) -> float | None:
    data = business.hgetall(cache_key(customer_id))
    if not data:
        return None
    age = time.time() - float(data["timestamp"])
    return float(data["riskScore"]) if age <= PROFILE_CACHE_MAX_AGE_SECONDS else None


def write_cache(customer_id: str, score: float) -> None:
    business.hset(cache_key(customer_id), mapping={"riskScore": score, "timestamp": time.time()})


def query_open_finance(customer_id: str) -> float:
    circuit.acquire_permission()
    metrics.increment("open_finance_calls")
    try:
        response = requests.get(
            f"{OPEN_FINANCE_URL}/risk/{customer_id}",
            timeout=OPEN_FINANCE_TIMEOUT_MS / 1000.0,
        )
        response.raise_for_status()
        score = float(response.json()["riskScore"])
    except requests.Timeout as exc:
        metrics.increment("open_finance_timeouts")
        circuit.record_failure()
        raise RuntimeError("OPEN_FINANCE_TIMEOUT") from exc
    except (requests.RequestException, KeyError, TypeError, ValueError) as exc:
        metrics.increment("open_finance_http_errors")
        circuit.record_failure()
        raise RuntimeError("OPEN_FINANCE_ERROR") from exc
    circuit.record_success()
    write_cache(customer_id, score)
    return score


def protected_score(customer_id: str) -> tuple[float | None, str]:
    try:
        return query_open_finance(customer_id), "OPEN_FINANCE"
    except (RuntimeError, CircuitOpenError):
        cached = read_cache(customer_id)
        if cached is not None:
            metrics.increment("fallback_count")
            return cached, "CACHE"
        return None, "NONE"


def deterministic_model_score(customer_id: str) -> float:
    digest = hashlib.sha256(customer_id.encode()).hexdigest()
    return float(10 + (int(digest[:8], 16) % 81))


_scenario_lock = threading.Lock()
_voting_scenario: dict[str, dict] = {}


def strategy_configuration(strategy_id: str) -> dict:
    with _scenario_lock:
        return dict(_voting_scenario.get(strategy_id, {}))


def run_strategy(strategy_id: str, customer_id: str) -> tuple[float | None, str, str]:
    config = strategy_configuration(strategy_id)
    mode = str(config.get("mode", "NORMAL")).upper()
    if mode == "TIMEOUT":
        time.sleep((VOTING_TIMEOUT_MS + 500) / 1000.0)
        return None, "TIMEOUT", "NONE"
    if mode == "ERROR":
        return None, "ERROR", "NONE"
    if "score" in config:
        return float(config["score"]), "SUCCESS", "CONFIGURED"
    if strategy_id == "A":
        score, source = protected_score(customer_id)
        return score, "SUCCESS" if score is not None else "ERROR", source
    base = deterministic_model_score(customer_id)
    adjustment = 0.5 if strategy_id == "B" else -0.5
    return round(max(0.0, min(100.0, base + adjustment)), 2), "SUCCESS", f"MODEL_{strategy_id}"


class VoteCoordinator:
    def __init__(self):
        self._lock = threading.Lock()
        self._pending: dict[str, dict] = {}

    def register(self, correlation_id: str) -> None:
        with self._lock:
            self._pending[correlation_id] = {
                "started": time.monotonic(),
                "votes": {},
                "event": threading.Event(),
            }

    def add(self, correlation_id: str, vote: Vote) -> bool:
        with self._lock:
            state = self._pending.get(correlation_id)
            if state is None:
                metrics.increment("invalid_correlations")
                return False
            state["votes"][vote.strategy_id] = vote
            if len(state["votes"]) == 3:
                state["event"].set()
            return True

    def wait(self, correlation_id: str) -> dict:
        with self._lock:
            state = self._pending[correlation_id]
            event = state["event"]
        complete = event.wait(VOTING_TIMEOUT_MS / 1000.0)
        with self._lock:
            state = self._pending.pop(correlation_id)
        duration_ms = round((time.monotonic() - state["started"]) * 1000, 1)
        votes = list(state["votes"].values())
        decision = decide_votes(votes, tolerance=VOTING_TOLERANCE)
        decision.update({
            "correlationId": correlation_id,
            "durationMs": duration_ms,
            "timeout": not complete,
            "votes": [v.__dict__ for v in sorted(votes, key=lambda item: item.strategy_id)],
        })
        metrics.increment("voting_total")
        if not complete:
            metrics.increment("voting_timeout_count")
        if decision["discrepancyDetected"]:
            metrics.increment("discrepancies_detected")
        return decision


coordinator = VoteCoordinator()


def ensure_group(stream: str, group: str) -> None:
    try:
        business.xgroup_create(stream, group, id="0", mkstream=True)
    except redis.exceptions.ResponseError as exc:
        if "BUSYGROUP" not in str(exc):
            raise


def publish(stream: str, payload: dict) -> str:
    message_id = business.xadd(stream, {k: str(v) for k, v in payload.items()}, maxlen=10000, approximate=True)
    metrics.increment("events_published")
    return message_id


def strategy_worker(strategy_id: str) -> None:
    group = f"profiling-strategy-{strategy_id.lower()}"
    while True:
        try:
            ensure_group(STREAM_CALCULATION_REQUESTS, group)
            response = business.xreadgroup(group, INSTANCE_ID, {STREAM_CALCULATION_REQUESTS: ">"}, count=10, block=2000)
            for _, messages in response:
                for message_id, fields in messages:
                    correlation_id = fields["correlationId"]
                    customer_id = fields["customerId"]
                    score, status, source = run_strategy(strategy_id, customer_id)
                    if status != "TIMEOUT":
                        publish(STREAM_VOTE_RESULTS, {
                            "eventId": str(uuid.uuid4()),
                            "eventType": "ProfilingResult",
                            "correlationId": correlation_id,
                            "strategyId": strategy_id,
                            "customerId": customer_id,
                            "riskScore": "" if score is None else score,
                            "status": status,
                            "profileSource": source,
                            "timestamp": datetime.now(timezone.utc).isoformat(),
                        })
                    business.xack(STREAM_CALCULATION_REQUESTS, group, message_id)
        except Exception as exc:
            log_event("STRATEGY_WORKER_ERROR", strategyId=strategy_id, result=str(exc))
            time.sleep(1)


def validator_worker() -> None:
    group = "profiling-validator"
    while True:
        try:
            ensure_group(STREAM_VOTE_RESULTS, group)
            response = business.xreadgroup(group, INSTANCE_ID, {STREAM_VOTE_RESULTS: ">"}, count=20, block=2000)
            for _, messages in response:
                for message_id, fields in messages:
                    score = float(fields["riskScore"]) if fields.get("riskScore") else None
                    vote = Vote(fields["strategyId"], score, fields.get("status", "ERROR"))
                    coordinator.add(fields["correlationId"], vote)
                    metrics.increment("voting_responses_received")
                    business.xack(STREAM_VOTE_RESULTS, group, message_id)
        except Exception as exc:
            log_event("VALIDATOR_ERROR", result=str(exc))
            time.sleep(1)


def risk_level(score: float) -> str:
    if score < 34:
        return "LOW"
    if score < 67:
        return "MEDIUM"
    return "HIGH"


def publish_profile_updated(customer_id: str, score: float, correlation_id: str) -> dict:
    version = business.incr(f"profile:version:{customer_id}")
    event = {
        "eventId": str(uuid.uuid4()),
        "eventType": "ProfileUpdated",
        "correlationId": correlation_id,
        "customerId": customer_id,
        "version": version,
        "riskScore": score,
        "riskLevel": risk_level(score),
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }
    publish(STREAM_PROFILE_UPDATED, event)
    business.hset(f"profile:last:{customer_id}", mapping={k: str(v) for k, v in event.items()})
    log_event("PROFILE_UPDATED", eventId=event["eventId"], correlationId=correlation_id,
              customerId=customer_id, profileVersion=version, result="PUBLISHED")
    return event


def calculate_profile(customer_id: str, correlation_id: str | None = None) -> tuple[dict, int]:
    correlation_id = correlation_id or str(uuid.uuid4())
    coordinator.register(correlation_id)
    publish(STREAM_CALCULATION_REQUESTS, {
        "eventId": str(uuid.uuid4()),
        "eventType": "ProfileCalculationRequested",
        "correlationId": correlation_id,
        "customerId": customer_id,
        "timestamp": datetime.now(timezone.utc).isoformat(),
    })
    vote = coordinator.wait(correlation_id)
    if vote["decision"] != "CONSENSUS":
        return {"error": "NO_VOTING_CONSENSUS", "customerId": customer_id, "vote": vote}, 503
    event = publish_profile_updated(customer_id, float(vote["finalScore"]), correlation_id)
    return {"customerId": customer_id, "event": event, "vote": vote,
            "circuitBreaker": circuit.snapshot()}, 200


def refresh_request_worker() -> None:
    group = "profiling-refresh-processor"
    while True:
        try:
            ensure_group(STREAM_REFRESH_REQUESTS, group)
            response = business.xreadgroup(group, INSTANCE_ID, {STREAM_REFRESH_REQUESTS: ">"}, count=10, block=2000)
            for _, messages in response:
                for message_id, fields in messages:
                    calculate_profile(fields["customerId"], fields.get("correlationId"))
                    business.xack(STREAM_REFRESH_REQUESTS, group, message_id)
                    metrics.increment("refresh_requests_acked")
        except Exception as exc:
            log_event("REFRESH_CONSUMER_ERROR", result=str(exc))
            time.sleep(1)


def health_echo_worker() -> None:
    while True:
        pubsub = None
        try:
            pubsub = control.pubsub(ignore_subscribe_messages=True)
            pubsub.subscribe(HEALTH_CHANNEL)
            for message in pubsub.listen():
                data = json.loads(message["data"])
                if data.get("eventType") != "HealthPing":
                    continue
                target = data.get("targetInstanceId")
                if target not in (INSTANCE_ID, SERVICE_NAME):
                    continue
                control.publish(HEALTH_CHANNEL, json.dumps({
                    "eventType": "HealthEcho",
                    "correlationId": data["correlationId"],
                    "instanceId": INSTANCE_ID,
                    "serviceName": SERVICE_NAME,
                    "timestamp": datetime.now(timezone.utc).isoformat(),
                    "status": "UP",
                }))
        except Exception as exc:
            log_event("HEALTH_SUBSCRIBER_RECONNECT", result=str(exc))
            time.sleep(1)
        finally:
            if pubsub:
                pubsub.close()


@app.post("/profiles/<customer_id>/refresh")
def refresh(customer_id):
    try:
        body, status = calculate_profile(customer_id)
        return jsonify(body), status
    except redis.RedisError as exc:
        return jsonify({"error": "BUSINESS_BUS_UNAVAILABLE", "detail": str(exc)}), 503


@app.get("/profiles/<customer_id>")
def get_profile(customer_id):
    try:
        profile = business.hgetall(f"profile:last:{customer_id}")
    except redis.RedisError as exc:
        return jsonify({"error": "BUSINESS_BUS_UNAVAILABLE", "detail": str(exc)}), 503
    if not profile:
        return jsonify({"error": "PROFILE_NOT_FOUND", "customerId": customer_id}), 404
    return jsonify({
        "customerId": customer_id,
        "riskScore": float(profile["riskScore"]),
        "riskLevel": profile["riskLevel"],
        "version": int(profile["version"]),
        "eventId": profile["eventId"],
        "timestamp": profile["timestamp"],
    }), 200


@app.get("/profiles/<customer_id>/sync")
def get_profile_sync(customer_id):
    score, source = protected_score(customer_id)
    if score is None:
        return jsonify({"error": "NO_VALID_PROFILE_AVAILABLE", "source": source}), 503
    return jsonify({"customerId": customer_id, "riskScore": score, "source": source}), 200


@app.post("/admin/voting-scenario")
def admin_voting_scenario():
    body = request.get_json(silent=True) or {}
    invalid = set(body) - {"A", "B", "C"}
    if invalid:
        return jsonify({"error": "INVALID_STRATEGY", "strategies": sorted(invalid)}), 400
    with _scenario_lock:
        _voting_scenario.clear()
        _voting_scenario.update({sid: dict(config) for sid, config in body.items()})
    return jsonify({"status": "ok", "scenario": _voting_scenario}), 200


@app.get("/circuit-state")
def circuit_state():
    return jsonify(circuit.snapshot()), 200


@app.post("/admin/circuit/reset")
def reset_circuit():
    circuit.reset()
    return jsonify({"status": "ok", "circuitBreaker": circuit.snapshot()}), 200


@app.get("/metrics")
def get_metrics():
    return jsonify(metrics.snapshot()), 200


@app.get("/health")
def health():
    dependencies = {}
    for name, client in (("redisBusiness", business), ("redisControl", control)):
        try:
            dependencies[name] = bool(client.ping())
        except redis.RedisError:
            dependencies[name] = False
    return jsonify({"status": "ok", "dependencies": dependencies}), 200


def start_background_workers() -> None:
    workers = [
        (validator_worker, "vote-validator"),
        (refresh_request_worker, "refresh-consumer"),
        (health_echo_worker, "health-echo"),
        *((lambda sid=sid: strategy_worker(sid), f"strategy-{sid}") for sid in ("A", "B", "C")),
    ]
    for target, name in workers:
        threading.Thread(target=target, name=name, daemon=True).start()


if __name__ == "__main__":
    logging.getLogger("werkzeug").setLevel(logging.WARNING)
    start_background_workers()
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", "7500")), threaded=True)
