"""HMAC-SHA256 verification of ProfileUpdated (AS-8). Mirrors profiling/event_signing.py."""
from __future__ import annotations

import hashlib
import hmac
import json

ALGORITHM = "HMAC-SHA256"
SUPPORTED_SCHEMA_VERSIONS = ("1.1",)
INTEGRITY_FIELDS = ("payloadHash", "producerId", "keyId", "algorithm", "schemaVersion")
SIGNED_FIELDS = (
    "eventId", "eventType", "correlationId", "customerId", "version",
    "riskScore", "riskLevel", "timestamp", "producerId", "schemaVersion",
)


def canonical_payload(fields: dict) -> bytes:
    core = {name: str(fields.get(name, "")) for name in SIGNED_FIELDS}
    return json.dumps(core, sort_keys=True, separators=(",", ":")).encode()


def verify_event(fields: dict, known_keys: dict[str, str]) -> tuple[bool, str]:
    if not all(fields.get(name) for name in INTEGRITY_FIELDS):
        return False, "MISSING_INTEGRITY_FIELDS"
    if fields["algorithm"] != ALGORITHM:
        return False, "UNSUPPORTED_ALGORITHM"
    if fields["schemaVersion"] not in SUPPORTED_SCHEMA_VERSIONS:
        return False, "UNSUPPORTED_SCHEMA_VERSION"
    secret = known_keys.get(fields["keyId"])
    if secret is None:
        return False, "UNKNOWN_KEY"
    expected = hmac.new(secret.encode(), canonical_payload(fields), hashlib.sha256).hexdigest()
    if not hmac.compare_digest(expected, str(fields["payloadHash"])):
        return False, "SIGNATURE_MISMATCH"
    return True, "OK"
