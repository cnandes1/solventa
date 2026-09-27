"""Test-only identity provider for the AS-4 confidentiality experiment.

It issues HS256 JWTs for whatever subject the runner asks for. It exists only to
produce valid, expired or under-privileged tokens on demand; it is NOT an
authentication service and must never be exposed outside the local experiment.
"""
from __future__ import annotations

import json
import logging
import os
import time
import uuid
from datetime import datetime, timezone

import jwt
from flask import Flask, jsonify, request

app = Flask(__name__)

SERVICE_NAME = "idp"
IDP_SIGNING_KEY = os.environ.get("IDP_SIGNING_KEY", "")
IDP_KEY_ID = os.environ.get("IDP_KEY_ID", "idp-test-2026-09")
IDP_ISSUER = os.environ.get("IDP_ISSUER", "solventa-test-idp")
IDP_AUDIENCE = os.environ.get("IDP_AUDIENCE", "solventa-gateway")
DEFAULT_TENANT_ID = os.environ.get("DEFAULT_TENANT_ID", "solventa")
DEFAULT_EXPIRES_IN_SECONDS = int(os.environ.get("DEFAULT_EXPIRES_IN_SECONDS", "3600"))


def log_event(event: str, **fields) -> None:
    print(json.dumps({
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "service": SERVICE_NAME,
        "event": event,
        **fields,
    }), flush=True)


@app.post("/tokens")
def issue_token():
    if not IDP_SIGNING_KEY:
        return jsonify({"error": "IDP_NOT_CONFIGURED"}), 500
    body = request.get_json(silent=True) or {}
    subject = str(body.get("sub", "")).strip()
    if not subject:
        return jsonify({"error": "SUBJECT_REQUIRED"}), 400
    scopes = body.get("scopes", ["quotes:read", "profiles:read"])
    if not isinstance(scopes, list) or not all(isinstance(scope, str) for scope in scopes):
        return jsonify({"error": "INVALID_SCOPES"}), 400
    now = int(time.time())
    expires_at = now + int(body.get("expiresIn", DEFAULT_EXPIRES_IN_SECONDS))
    claims = {
        "iss": IDP_ISSUER,
        "aud": body.get("aud", IDP_AUDIENCE),
        "sub": subject,
        "customerId": str(body.get("customerId", subject)),
        "tenantId": str(body.get("tenantId", DEFAULT_TENANT_ID)),
        "scopes": scopes,
        "iat": now,
        "exp": expires_at,
        "jti": str(uuid.uuid4()),
    }
    token = jwt.encode(claims, IDP_SIGNING_KEY, algorithm="HS256", headers={"kid": IDP_KEY_ID})
    log_event("TOKEN_ISSUED", sub=subject, tenantId=claims["tenantId"], scopes=scopes,
              kid=IDP_KEY_ID, expiresAt=expires_at, jti=claims["jti"])
    return jsonify({
        "access_token": token,
        "token_type": "Bearer",
        "expires_at": expires_at,
        "kid": IDP_KEY_ID,
        "warning": "TEST ONLY",
    }), 200


@app.get("/health")
def health():
    return jsonify({"status": "ok", "configured": bool(IDP_SIGNING_KEY)}), 200


if __name__ == "__main__":
    logging.getLogger("werkzeug").setLevel(logging.WARNING)
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", "6100")), threaded=True)
