"""Policy Enforcement Point for AS-4: JWT validation plus an external PDP decision.

Invariant: any failure to obtain an explicit PERMIT from the PDP (timeout,
connection error, unexpected status or body) ends in DENY. There is no flag to
turn this into fail-open.
"""
from __future__ import annotations

import json
import os
import re
import threading
import time
import uuid
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime, timezone

import jwt
import requests


def parse_keys(raw: str) -> dict[str, str]:
    return {str(kid): str(secret) for kid, secret in json.loads(raw or "{}").items()}


IDP_KEYS = parse_keys(os.environ.get("IDP_KEYS_JSON", "{}"))
IDP_ISSUER = os.environ.get("IDP_ISSUER", "solventa-test-idp")
IDP_AUDIENCE = os.environ.get("IDP_AUDIENCE", "solventa-gateway")
PDP_URL = os.environ.get("PDP_URL", "http://pdp:6200").rstrip("/")
PDP_TIMEOUT_MS = int(os.environ.get("PDP_TIMEOUT_MS", "300"))
RESOURCE_TENANT_ID = os.environ.get("RESOURCE_TENANT_ID", "solventa")

# (method, path pattern, resource type, action). Anything not listed is denied.
PROTECTED_ROUTES = (
    ("GET", re.compile(r"^quotes/([^/]+)$"), "quote", "quotes:read"),
    ("GET", re.compile(r"^sync/quotes/([^/]+)$"), "quote", "quotes:read"),
    ("POST", re.compile(r"^quotes/([^/]+)/request-refresh$"), "profile", "profiles:refresh"),
    ("GET", re.compile(r"^profiles/([^/]+)$"), "profile", "profiles:read"),
    ("GET", re.compile(r"^materialized-profiles/([^/]+)$"), "profile", "profiles:read"),
)
# Experiment control plane, documented as out of scope for AS-4 (same as today).
UNPROTECTED_PREFIXES = ("admin/",)

_session = requests.Session()
_session.mount("http://", requests.adapters.HTTPAdapter(pool_connections=4, pool_maxsize=32))


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


@dataclass
class Decision:
    status: str
    code: int
    reason: str
    body: dict = field(default_factory=dict)


def log_event(event: str, **fields) -> None:
    print(json.dumps({
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "service": "gateway",
        "event": event,
        **fields,
    }), flush=True)


class TokenError(Exception):
    def __init__(self, reason: str):
        super().__init__(reason)
        self.reason = reason


def bearer_token(headers) -> str:
    value = headers.get("Authorization", "")
    if not value:
        raise TokenError("MISSING_TOKEN")
    scheme, _, token = value.partition(" ")
    if scheme.lower() != "bearer" or not token.strip():
        raise TokenError("INVALID_TOKEN")
    return token.strip()


def validate_token(token: str) -> dict:
    try:
        kid = jwt.get_unverified_header(token).get("kid")
    except jwt.InvalidTokenError as exc:
        raise TokenError("INVALID_TOKEN") from exc
    secret = IDP_KEYS.get(kid)
    if secret is None:
        raise TokenError("UNKNOWN_KEY_ID")
    try:
        claims = jwt.decode(
            token,
            secret,
            algorithms=["HS256"],
            audience=IDP_AUDIENCE,
            issuer=IDP_ISSUER,
            options={"require": ["exp", "sub", "aud", "iss"]},
        )
    except jwt.ExpiredSignatureError as exc:
        raise TokenError("TOKEN_EXPIRED") from exc
    except jwt.InvalidSignatureError as exc:
        raise TokenError("INVALID_SIGNATURE") from exc
    except jwt.InvalidAudienceError as exc:
        raise TokenError("INVALID_AUDIENCE") from exc
    except jwt.InvalidTokenError as exc:
        raise TokenError("INVALID_TOKEN") from exc
    claims["kid"] = kid
    return claims


def resolve_resource(method: str, subpath: str) -> tuple[dict, str] | None:
    for route_method, pattern, resource_type, action in PROTECTED_ROUTES:
        match = pattern.match(subpath)
        if match and method == route_method:
            return {
                "type": resource_type,
                "owner": {"customerId": match.group(1)},
                "tenantId": RESOURCE_TENANT_ID,
            }, action
    return None


def query_pdp(subject: dict, resource: dict, action: str) -> tuple[str, str]:
    try:
        response = _session.post(
            f"{PDP_URL}/decisions",
            json={"subject": subject, "resource": resource, "action": action},
            timeout=PDP_TIMEOUT_MS / 1000.0,
        )
        body = response.json() if response.status_code == 200 else {}
    except (requests.RequestException, ValueError):
        return "DENY", "PDP_UNAVAILABLE"
    decision = body.get("decision") if isinstance(body, dict) else None
    if decision not in ("PERMIT", "DENY"):
        return "DENY", "PDP_UNAVAILABLE"
    return decision, str(body.get("reason", "UNSPECIFIED"))


DENY_ERRORS = {401: "UNAUTHORIZED", 403: "FORBIDDEN", 503: "AUTHORIZATION_UNAVAILABLE"}


def deny(code: int, reason: str) -> Decision:
    return Decision("DENY", code, reason, {"error": DENY_ERRORS[code], "reason": reason})


def evaluate(req, subpath: str) -> tuple[Decision, dict, dict | None, str | None]:
    try:
        claims = validate_token(bearer_token(req.headers))
    except TokenError as exc:
        return deny(401, exc.reason), {}, None, None
    subject = {
        "sub": claims["sub"],
        "customerId": str(claims.get("customerId", claims["sub"])),
        "tenantId": claims.get("tenantId"),
        "scopes": claims.get("scopes") if isinstance(claims.get("scopes"), list) else [],
    }
    resolved = resolve_resource(req.method, subpath)
    if resolved is None:
        return deny(403, "UNMAPPED_RESOURCE"), subject, None, None
    resource, action = resolved
    decision, reason = query_pdp(subject, resource, action)
    if decision == "PERMIT":
        return Decision("PERMIT", 200, reason), subject, resource, action
    return deny(503 if reason == "PDP_UNAVAILABLE" else 403, reason), subject, resource, action


def authorize(req, subpath: str) -> Decision:
    if subpath.startswith(UNPROTECTED_PREFIXES):
        return Decision("PERMIT", 200, "EXPERIMENT_CONTROL_PLANE")
    started = time.perf_counter()
    decision, subject, resource, action = evaluate(req, subpath)
    metrics.increment("authz_checks")
    metrics.increment("authz_duration_us_total", int((time.perf_counter() - started) * 1_000_000))
    metrics.increment(f"authz_{decision.status.lower()}")
    metrics.increment(f"authz_reason_{decision.reason.lower()}")
    log_event(
        "AUTHZ_DECISION",
        correlationId=req.headers.get("X-Correlation-Id") or str(uuid.uuid4()),
        decision=decision.status,
        reason=decision.reason,
        httpStatus=decision.code,
        method=req.method,
        path=f"/{subpath}",
        subject=subject.get("sub"),
        subjectCustomerId=subject.get("customerId"),
        ownerCustomerId=(resource or {}).get("owner", {}).get("customerId"),
        action=action,
    )
    return decision
