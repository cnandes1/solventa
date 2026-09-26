import importlib.util
import os
from pathlib import Path

import pytest

import event_signing
import event_verification

ROOT = Path(__file__).resolve().parents[2]
KEY_ID = "test-key-unit"
SECRET = "TEST-ONLY-unit-hmac"
KNOWN = {KEY_ID: SECRET}


def wire(event: dict) -> dict:
    """What a consumer reads back from Redis Streams: every value as a string."""
    return {key: str(value) for key, value in event.items()}


def signed_event(**overrides) -> dict:
    event = {
        "eventId": "evt-1",
        "eventType": "ProfileUpdated",
        "correlationId": "corr-1",
        "customerId": "C001",
        "version": 10,
        "riskScore": 40.0,
        "riskLevel": "MEDIUM",
        "timestamp": "2026-09-06T15:00:00+00:00",
        **overrides,
    }
    return wire(event_signing.sign_event(event, KEY_ID, SECRET))


def test_producer_and_consumer_sign_the_same_fields():
    assert event_signing.SIGNED_FIELDS == event_verification.SIGNED_FIELDS
    event = signed_event()
    assert event_signing.canonical_payload(event) == event_verification.canonical_payload(event)


def test_signed_event_round_trips_through_the_stream():
    event = signed_event()
    assert event["algorithm"] == "HMAC-SHA256" and event["schemaVersion"] == "1.1"
    assert event_verification.verify_event(event, KNOWN) == (True, "OK")


@pytest.mark.parametrize("field, value", [("riskScore", "77.5"), ("version", "11"), ("customerId", "C002"),
                                          ("riskLevel", "LOW"), ("eventId", "evt-2"), ("producerId", "attacker")])
def test_any_signed_field_change_is_detected(field, value):
    event = signed_event()
    event[field] = value
    assert event_verification.verify_event(event, KNOWN) == (False, "SIGNATURE_MISMATCH")


@pytest.mark.parametrize("missing", event_verification.INTEGRITY_FIELDS)
def test_missing_integrity_fields(missing):
    event = signed_event()
    event.pop(missing)
    assert event_verification.verify_event(event, KNOWN) == (False, "MISSING_INTEGRITY_FIELDS")


def test_unknown_key_algorithm_and_schema():
    assert event_verification.verify_event(signed_event(), {"other": SECRET}) == (False, "UNKNOWN_KEY")
    assert event_verification.verify_event({**signed_event(), "algorithm": "NONE"}, KNOWN)[1] == "UNSUPPORTED_ALGORITHM"
    assert event_verification.verify_event({**signed_event(), "schemaVersion": "1.0"}, KNOWN)[1] == "UNSUPPORTED_SCHEMA_VERSION"


def test_rotation_keeps_previous_key_valid():
    old = signed_event()
    new = wire(event_signing.sign_event({**old, "eventId": "evt-2"}, "test-key-new", "TEST-ONLY-new"))
    keys = {**KNOWN, "test-key-new": "TEST-ONLY-new"}
    assert event_verification.verify_event(old, keys) == (True, "OK")
    assert event_verification.verify_event(new, keys) == (True, "OK")


def test_signing_without_key_refuses_to_publish():
    with pytest.raises(RuntimeError):
        event_signing.sign_event({"eventId": "x"}, "", "")


@pytest.fixture
def quoting_app(tmp_path, monkeypatch):
    monkeypatch.setenv("SQLITE_PATH", str(tmp_path / "view.db"))
    monkeypatch.setenv("KNOWN_KEYS_JSON", '{"%s": "%s"}' % (KEY_ID, SECRET))
    spec = importlib.util.spec_from_file_location("quoting_app", ROOT / "quoting" / "app.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    acks = []
    monkeypatch.setattr(module.business, "xack", lambda *args: acks.append(args[-1]))
    monkeypatch.setattr(module, "log_event", lambda *args, **kwargs: None)
    module.acks = acks
    return module


def test_rejected_event_is_acked_and_never_applied(quoting_app):
    forged = signed_event()
    forged["riskScore"] = "99.0"
    quoting_app.process_message("1-0", forged)
    assert quoting_app.acks == ["1-0"]
    assert quoting_app.repository.get("C001") is None
    assert quoting_app.metrics.snapshot()["integrity_rejected_signature_mismatch"] == 1

    # The genuine event with the same eventId is not masked as DUPLICATE.
    quoting_app.process_message("2-0", signed_event())
    assert quoting_app.repository.get("C001")["version"] == 10
    assert quoting_app.metrics.snapshot()["decision_applied"] == 1


def test_valid_duplicate_still_reports_duplicate(quoting_app):
    quoting_app.process_message("1-0", signed_event())
    quoting_app.process_message("2-0", signed_event())
    snapshot = quoting_app.metrics.snapshot()
    assert snapshot["integrity_verified"] == 2 and snapshot["decision_duplicate"] == 1
