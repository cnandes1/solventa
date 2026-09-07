import hashlib
import json
import logging
import os
import threading
import time
from datetime import datetime, timezone

from flask import Flask, jsonify, request

app = Flask(__name__)

_lock = threading.Lock()
_state = {
    "mode": os.environ.get("OPEN_FINANCE_MODE", "NORMAL").upper(),
    "latency_ms": int(os.environ.get("OPEN_FINANCE_LATENCY_MS", "50")),
}


def log_event(event: str, **fields) -> None:
    print(json.dumps({
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "service": "open-finance-mock",
        "event": event,
        **fields,
    }), flush=True)


def deterministic_score(customer_id: str) -> float:
    digest = hashlib.sha256(customer_id.encode()).hexdigest()
    return float(10 + (int(digest[:8], 16) % 81))


@app.route("/risk/<customer_id>", methods=["GET"])
def risk(customer_id):
    with _lock:
        mode = _state["mode"]
        latency_ms = _state["latency_ms"]

    if mode in {"NORMAL", "SLOW"} and latency_ms > 0:
        time.sleep(latency_ms / 1000.0)
    elif mode == "TIMEOUT":
        time.sleep(max(latency_ms, 5000) / 1000.0)

    log_event("RISK_REQUEST", customerId=customer_id, mode=mode, latencyMs=latency_ms)
    if mode == "HTTP_500":
        return jsonify({"error": "open finance internal error", "mode": mode}), 500
    if mode == "DOWN":
        return jsonify({"error": "open finance unavailable", "mode": mode}), 503

    return jsonify({
        "customerId": customer_id,
        "riskScore": deterministic_score(customer_id),
        "source": "OPEN_FINANCE",
        "mode": mode,
    }), 200


@app.route("/admin/mode", methods=["POST"])
def admin_mode():
    body = request.get_json(silent=True) or {}
    mode = str(body.get("mode", "")).upper()
    allowed = {"NORMAL", "SLOW", "HTTP_500", "TIMEOUT", "DOWN"}
    if mode not in allowed:
        return jsonify({"error": "INVALID_MODE", "allowed": sorted(allowed)}), 400
    with _lock:
        _state["mode"] = mode
        if "latencyMs" in body:
            _state["latency_ms"] = max(0, int(body["latencyMs"]))
        snapshot = dict(_state)
    log_event("MODE_CHANGED", mode=mode, latencyMs=snapshot["latency_ms"])
    return jsonify({"status": "ok", **snapshot}), 200


@app.route("/health", methods=["GET"])
def health():
    with _lock:
        snapshot = dict(_state)
    return jsonify({"status": "ok", **snapshot}), 200


if __name__ == "__main__":
    logging.getLogger("werkzeug").setLevel(logging.WARNING)
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", "6000")), threaded=True)
