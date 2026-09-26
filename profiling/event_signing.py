"""HMAC-SHA256 signing of ProfileUpdated (AS-8). Mirrors quoting/event_verification.py."""
from __future__ import annotations

import hashlib
import hmac
import json

ALGORITHM = "HMAC-SHA256"
SCHEMA_VERSION = "1.1"
PRODUCER_ID = "profiling"
# Fields covered by the MAC, compared as the strings that travel on the stream.
SIGNED_FIELDS = (
    "eventId", "eventType", "correlationId", "customerId", "version",
    "riskScore", "riskLevel", "timestamp", "producerId", "schemaVersion",
)


def canonical_payload(fields: dict) -> bytes:
    core = {name: str(fields.get(name, "")) for name in SIGNED_FIELDS}
    return json.dumps(core, sort_keys=True, separators=(",", ":")).encode()


def compute_hash(fields: dict, secret: str) -> str:
    return hmac.new(secret.encode(), canonical_payload(fields), hashlib.sha256).hexdigest()


def sign_event(event: dict, key_id: str, secret: str) -> dict:
    if not key_id or not secret:
        raise RuntimeError("SIGNING_KEY_NOT_CONFIGURED")
    event.update({
        "producerId": PRODUCER_ID,
        "keyId": key_id,
        "algorithm": ALGORITHM,
        "schemaVersion": SCHEMA_VERSION,
    })
    event["payloadHash"] = compute_hash(event, secret)
    return event
