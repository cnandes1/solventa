from storage import ProfileViewRepository


def event(event_id: str, version: int, score: float = 40.0) -> dict:
    return {
        "eventId": event_id,
        "customerId": "C001",
        "version": version,
        "riskScore": score,
        "riskLevel": "MEDIUM",
        "timestamp": "2026-09-06T12:00:00+00:00",
    }


def test_versioning_and_idempotency_are_transactional(tmp_path):
    repository = ProfileViewRepository(str(tmp_path / "view.db"))

    assert repository.apply_event(event("event-v5", 5))["decision"] == "APPLIED"
    assert repository.apply_event(event("event-v5", 5))["decision"] == "DUPLICATE"
    assert repository.apply_event(event("event-v4", 4, 10))["decision"] == "OLD_VERSION"
    assert repository.apply_event(event("event-v6", 6, 50))["decision"] == "APPLIED"

    profile = repository.get("C001")
    assert profile["version"] == 6
    assert profile["risk_score"] == 50
    assert repository.stats()["decisions"] == {"APPLIED": 2, "OLD_VERSION": 1}
