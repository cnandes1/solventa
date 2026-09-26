"""Policy Decision Point for the AS-4 confidentiality experiment.

Receives the already-verified token claims (never the raw JWT), the resource
owner/tenant and the requested action, and returns PERMIT or DENY with a reason.
Every rule that does not explicitly PERMIT ends in DENY.
"""
from __future__ import annotations

import logging
import os

from flask import Flask, jsonify, request

app = Flask(__name__)

DELEGATION_PREFIX = "delegated:"


def decide(subject: dict, resource: dict, action: str) -> tuple[str, str]:
    owner = (resource.get("owner") or {}).get("customerId")
    if not subject.get("customerId") or not owner or not action:
        return "DENY", "MALFORMED_REQUEST"
    if not subject.get("tenantId") or subject.get("tenantId") != resource.get("tenantId"):
        return "DENY", "TENANT_MISMATCH"
    scopes = subject.get("scopes") or []
    if action not in scopes:
        return "DENY", "INSUFFICIENT_SCOPE"
    if subject["customerId"] == owner:
        return "PERMIT", "OWNER"
    if f"{DELEGATION_PREFIX}{owner}" in scopes:
        return "PERMIT", "DELEGATION"
    return "DENY", "OWNERSHIP_MISMATCH"


@app.post("/decisions")
def decisions():
    body = request.get_json(silent=True)
    if not isinstance(body, dict):
        return jsonify({"decision": "DENY", "reason": "MALFORMED_REQUEST"}), 400
    subject = body.get("subject") if isinstance(body.get("subject"), dict) else {}
    resource = body.get("resource") if isinstance(body.get("resource"), dict) else {}
    decision, reason = decide(subject, resource, str(body.get("action", "")))
    return jsonify({"decision": decision, "reason": reason}), 200


@app.get("/health")
def health():
    return jsonify({"status": "ok"}), 200


if __name__ == "__main__":
    logging.getLogger("werkzeug").setLevel(logging.WARNING)
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", "6200")), threaded=True)
