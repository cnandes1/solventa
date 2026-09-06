import os
import random
import time

from flask import Flask, jsonify

app = Flask(__name__)

LATENCY_MS = float(os.environ.get("LATENCY_MS", "50"))
FAIL = os.environ.get("FAIL", "false").strip().lower() == "true"


@app.route("/risk/<customer_id>", methods=["GET"])
def risk(customer_id):
    if LATENCY_MS > 0:
        time.sleep(LATENCY_MS / 1000.0)

    if FAIL:
        return jsonify({"error": "risk provider unavailable"}), 500

    base = (abs(hash(customer_id)) % 60) + 10
    noise = random.randint(-5, 5)
    score = max(0, min(100, base + noise))

    return jsonify({
        "customer_id": customer_id,
        "risk_score": score,
        "source": "external_provider",
    }), 200


@app.route("/health", methods=["GET"])
def health():
    return jsonify({"status": "ok", "fail": FAIL, "latency_ms": LATENCY_MS}), 200


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 6000))
    app.run(host="0.0.0.0", port=port)
