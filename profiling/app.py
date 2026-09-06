import hashlib
import json
import os
import threading
import time
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed

import redis
import requests
from flask import Flask, jsonify, request

app = Flask(__name__)

# ------------------------------------------------------------------ settings
SERVICE_NAME = "profiling"
INSTANCE_ID = os.environ.get("INSTANCE_ID", "profiling-1")

REDIS_URL = os.environ.get("REDIS_URL", "redis://redis:6379/0")
HEALTH_CHANNEL = os.environ.get("HEALTH_CHANNEL", "health-heartbeat")

STREAM_PROFILE_UPDATED = os.environ.get("STREAM_PROFILE_UPDATED", "profile-updated")
STREAM_VOTE_RESULTS = os.environ.get("STREAM_VOTE_RESULTS", "profiling-vote-results")

RISK_PROVIDER_URL = os.environ.get("RISK_PROVIDER_URL", "http://risk-provider:6000").rstrip("/")
RISK_PROVIDER_TIMEOUT_SECONDS = float(os.environ.get("RISK_PROVIDER_TIMEOUT_SECONDS", "0.7"))

DEFAULT_RISK_SCORE = float(os.environ.get("DEFAULT_RISK_SCORE", "60"))

# Circuit breaker
CB_FAILS_THRESHOLD = int(os.environ.get("CB_FAILS_THRESHOLD", "3"))
CB_OPEN_SECONDS = float(os.environ.get("CB_OPEN_SECONDS", "15"))

# Vote by correlationId
VOTE_TIMEOUT_SECONDS = float(os.environ.get("VOTE_TIMEOUT_SECONDS", "1.0"))
VOTE_DISCREPANCY_THRESHOLD = float(os.environ.get("VOTE_DISCREPANCY_THRESHOLD", "15"))

r = redis.Redis.from_url(REDIS_URL, decode_responses=True)


# --------------------------------------------------------------- circuit breaker
class CircuitBreaker:
    def __init__(self, fails_threshold: int, open_seconds: float):
        self._fails_threshold = fails_threshold
        self._open_seconds = open_seconds
        self._lock = threading.Lock()
        self._state = "closed"  # closed | open | half_open
        self._fails = 0
        self._opened_at = 0.0

    def allows_attempt(self) -> bool:
        with self._lock:
            if self._state == "open":
                if time.time() - self._opened_at >= self._open_seconds:
                    self._state = "half_open"
                    return True
                return False
            return True

    def record_success(self) -> None:
        with self._lock:
            self._fails = 0
            self._state = "closed"

    def record_failure(self) -> None:
        with self._lock:
            self._fails += 1
            if self._state == "half_open" or self._fails >= self._fails_threshold:
                self._state = "open"
                self._opened_at = time.time()

    def state(self) -> dict:
        with self._lock:
            return {
                "state": self._state,
                "fails": self._fails,
                "fails_threshold": self._fails_threshold,
                "open_seconds": self._open_seconds,
            }


_circuit_breaker = CircuitBreaker(CB_FAILS_THRESHOLD, CB_OPEN_SECONDS)


# ------------------------------------------------------------------ profile cache
def _cache_key(customer_id: str) -> str:
    return f"profile:cache:{customer_id}"


def _read_cache(customer_id: str):
    data = r.hgetall(_cache_key(customer_id))
    if not data:
        return None
    return float(data["risk_score"])


def _write_cache(customer_id: str, score: float) -> None:
    r.hset(_cache_key(customer_id), mapping={
        "risk_score": score,
        "timestamp": time.time(),
    })


def _query_provider(customer_id: str) -> float:

    if not _circuit_breaker.allows_attempt():
        raise RuntimeError("circuit open: skipping call to provider")

    try:
        resp = requests.get(
            f"{RISK_PROVIDER_URL}/risk/{customer_id}",
            timeout=RISK_PROVIDER_TIMEOUT_SECONDS,
        )
        resp.raise_for_status()
        score = float(resp.json()["risk_score"])
    except (requests.RequestException, ValueError, KeyError) as exc:
        _circuit_breaker.record_failure()
        raise RuntimeError(f"failed querying provider: {exc}") from exc

    _circuit_breaker.record_success()
    return score


def _protected_score(customer_id: str) -> tuple[float, str]:
    try:
        score = _query_provider(customer_id)
        _write_cache(customer_id, score)
        return score, "provider"
    except Exception:
        cached = _read_cache(customer_id)
        if cached is not None:
            return cached, "cache"
        return DEFAULT_RISK_SCORE, "default"


# ------------------------------------------------------------------ strategies A/B/C
def _centered_variation(customer_id: str, salt: str, amplitude: float = 8.0) -> float:
    h = hashlib.sha256(f"{customer_id}:{salt}".encode()).hexdigest()
    v = (int(h[:8], 16) % 2000) / 100.0  # 0.00 - 19.99
    return (v - 10.0) * (amplitude / 10.0)


def _alternate_strategy_reference(customer_id: str) -> float:
    cached = _read_cache(customer_id)
    return cached if cached is not None else DEFAULT_RISK_SCORE


def _strategy_a(customer_id: str) -> float:
    score, _ = _protected_score(customer_id)
    return round(score, 2)


def _strategy_b(customer_id: str) -> float:
    base = _alternate_strategy_reference(customer_id)
    return round(max(0.0, min(100.0, base + _centered_variation(customer_id, "strategy-b"))), 2)


def _strategy_c(customer_id: str) -> float:
    base = _alternate_strategy_reference(customer_id)
    return round(max(0.0, min(100.0, base + _centered_variation(customer_id, "strategy-c"))), 2)


_STRATEGIES = {"A": _strategy_a, "B": _strategy_b, "C": _strategy_c}


def _publish_vote_result(correlation_id: str, customer_id: str, strategy_id: str, score: float | None, error: str | None) -> None:
    r.xadd(STREAM_VOTE_RESULTS, {
        "correlationId": correlation_id,
        "customerId": customer_id,
        "strategyId": strategy_id,
        "score": "" if score is None else str(score),
        "error": error or "",
        "timestamp": str(time.time()),
    })


def _run_strategy(correlation_id: str, customer_id: str, strategy_id: str) -> dict:
    try:
        score = _STRATEGIES[strategy_id](customer_id)
        _publish_vote_result(correlation_id, customer_id, strategy_id, score, None)
        return {"strategyId": strategy_id, "score": score, "error": None}
    except Exception as exc:  # a strategy must never bring down the vote
        _publish_vote_result(correlation_id, customer_id, strategy_id, None, str(exc))
        return {"strategyId": strategy_id, "score": None, "error": str(exc)}


def _vote(customer_id: str) -> dict:
    correlation_id = str(uuid.uuid4())
    votes: list[dict] = []

    with ThreadPoolExecutor(max_workers=3) as pool:
        futures = {
            pool.submit(_run_strategy, correlation_id, customer_id, sid): sid
            for sid in _STRATEGIES
        }
        try:
            for fut in as_completed(futures, timeout=VOTE_TIMEOUT_SECONDS):
                votes.append(fut.result())
        except TimeoutError:
            pass  # votes that didn't arrive in time are treated as missing

    valid_scores = [v["score"] for v in votes if v["score"] is not None]
    vote_incomplete = len(votes) < len(_STRATEGIES)

    if valid_scores:
        final_score = round(sum(valid_scores) / len(valid_scores), 2)
        discrepancy = (max(valid_scores) - min(valid_scores)) > VOTE_DISCREPANCY_THRESHOLD
    else:
        final_score = DEFAULT_RISK_SCORE
        discrepancy = False

    return {
        "correlationId": correlation_id,
        "votes": votes,
        "vote_incomplete": vote_incomplete,
        "discrepancy": discrepancy,
        "final_score": final_score,
    }


# ---------------------------------------------------------------- view/version
def _version_key(customer_id: str) -> str:
    return f"profile:version:{customer_id}"


def _view_key(customer_id: str) -> str:
    return f"profile:view:{customer_id}"


def _publish_profile_updated(customer_id: str, final_score: float, discrepancy: bool) -> dict:
    version = r.incr(_version_key(customer_id))
    event_id = str(uuid.uuid4())
    timestamp = time.time()

    payload = {
        "eventId": event_id,
        "version": str(version),
        "customerId": customer_id,
        "riskScore": str(final_score),
        "discrepancy": "true" if discrepancy else "false",
        "timestamp": str(timestamp),
    }
    r.xadd(STREAM_PROFILE_UPDATED, payload)
    r.hset(_view_key(customer_id), mapping=payload)
    return payload


# --------------------------------------------------------------------- routes
@app.route("/profiles/<customer_id>/refresh", methods=["POST"])
def refresh(customer_id):
    """Command: recomputes the profile (circuit breaker + vote) and
    publishes ProfileUpdated."""
    vote_result = _vote(customer_id)
    event = _publish_profile_updated(
        customer_id, vote_result["final_score"], vote_result["discrepancy"]
    )
    return jsonify({
        "customerId": customer_id,
        "event": event,
        "vote": vote_result,
        "circuit_breaker": _circuit_breaker.state(),
    }), 200


@app.route("/profiles/<customer_id>", methods=["GET"])
def get_profile(customer_id):
    """Query: reads the last published view, without recomputing anything."""
    view = r.hgetall(_view_key(customer_id))
    if not view:
        return jsonify({
            "customerId": customer_id,
            "riskScore": DEFAULT_RISK_SCORE,
            "version": 0,
            "source": "default",
        }), 200

    return jsonify({
        "customerId": customer_id,
        "riskScore": float(view["riskScore"]),
        "version": int(view["version"]),
        "eventId": view["eventId"],
        "discrepancy": view.get("discrepancy") == "true",
        "timestamp": float(view["timestamp"]),
        "source": "materialized-view",
    }), 200


@app.route("/profiles/<customer_id>/circuit-state", methods=["GET"])
def circuit_state(customer_id):
    return jsonify({"customerId": customer_id, "circuit_breaker": _circuit_breaker.state()}), 200


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

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 7500))
    app.run(host="0.0.0.0", port=port, threaded=True)
