"""SEC-C0..SEC-C9 (AS-4, confidentiality) and SEC-I0..SEC-I9 (AS-8, integrity).

Dispatched from run_experiment.execute(). Every credential used here is a
TEST ONLY value that mirrors docker-compose.yaml.
"""
from __future__ import annotations

import json
import os
import re
import secrets
import subprocess
import sys
import threading
import time
import uuid
from datetime import datetime, timedelta, timezone

import run_experiment as core

sys.path.insert(0, str(core.ROOT / "profiling"))
import event_signing  # noqa: E402  (same canonicalization the producer uses)

URLS = core.URLS
QUOTING = ("quoting-a", "quoting-b")
STREAM_PROFILE_UPDATED = "profile-updated"
PRODUCER = ("profile_producer", os.environ.get("REDIS_PRODUCER_PASSWORD", "TEST-ONLY-redis-profile-producer-pw-2026-09"))
CONSUMER = ("profile_consumer", os.environ.get("REDIS_CONSUMER_PASSWORD", "TEST-ONLY-redis-profile-consumer-pw-2026-09"))
ANONYMOUS = None
SECRET_MARKER = "TEST-ONLY"


# ---------------------------------------------------------------- helpers

def issue_token(sub: str, tenant: str = core.DEFAULT_TENANT_ID, scopes: list[str] | None = None,
                exp_delta: int = 3600) -> str:
    return core.issue_token(sub, tenant, scopes, exp_delta)


def call_with_token(method: str, url: str, token: str | None, correlation_id: str | None = None):
    headers = {"X-Correlation-Id": correlation_id or str(uuid.uuid4())}
    if token is not None:
        headers["Authorization"] = f"Bearer {token}"
    return core.http(method, url, headers=headers, timeout=5)


def tamper_signature(token: str) -> str:
    """Flip one character in the middle of the signature segment.

    The last base64url character of an HS256 signature carries padding bits, so
    changing it may not change the decoded bytes; the middle one always does.
    """
    header, payload, signature = token.split(".")
    index = len(signature) // 2
    replacement = "A" if signature[index] != "A" else "B"
    return ".".join((header, payload, signature[:index] + replacement + signature[index + 1:]))


def redis_cli(credentials: tuple[str, str] | None, *args: str) -> str:
    command = ["docker", "compose", "exec", "-T", "redis-business", "redis-cli", "--no-auth-warning"]
    if credentials:
        command += ["--user", credentials[0], "--pass", credentials[1]]
    completed = subprocess.run([*command, *args], cwd=core.ROOT, capture_output=True, text=True, timeout=15)
    return (completed.stdout + completed.stderr).strip()


def last_signed_event(customer_id: str) -> dict:
    """Latest signed ProfileUpdated as stored by Profiling (read with producer credentials)."""
    lines = redis_cli(PRODUCER, "HGETALL", f"profile:last:{customer_id}").splitlines()
    event = dict(zip(lines[::2], lines[1::2]))
    if "payloadHash" not in event:
        raise RuntimeError(f"no signed event stored for {customer_id}: {event}")
    return event


def xadd_event(fields: dict, credentials=PRODUCER) -> str:
    args = [item for key, value in fields.items() for item in (key, str(value))]
    output = redis_cli(credentials, "XADD", STREAM_PROFILE_UPDATED, "*", *args)
    if not re.fullmatch(r"\d+-\d+", output):
        raise RuntimeError(f"XADD rejected: {output}")
    return output


def forged_copy(event: dict, **changes) -> dict:
    """A copy with a fresh eventId and a higher version, so idempotency or
    versioning could never hide a missing integrity check: without it, the
    forged event would be APPLIED."""
    forged = dict(event)
    forged.update({"eventId": str(uuid.uuid4()), "version": str(int(event["version"]) + 1000), **changes})
    return forged


def tamper_and_republish(event: dict) -> dict:
    forged = forged_copy(event, riskScore=str(round(float(event["riskScore"]) + 37.5, 2)))
    xadd_event(forged)  # original payloadHash kept
    return forged


def unsigned_and_republish(event: dict) -> dict:
    forged = forged_copy(event)
    for name in ("payloadHash", "producerId", "keyId", "algorithm", "schemaVersion"):
        forged.pop(name, None)
    xadd_event(forged)
    return forged


def rogue_signed_and_republish(event: dict) -> dict:
    forged = forged_copy(event)
    rogue_secret = f"{SECRET_MARKER}-rogue-{secrets.token_hex(16)}"
    event_signing.sign_event(forged, "test-key-rogue", rogue_secret)
    xadd_event(forged)
    return forged


def set_known_key(key_id: str | None = None, secret: str | None = None, reset: bool = False) -> None:
    body = {"reset": True} if reset else {"keyId": key_id, "secret": secret}
    for service in QUOTING:
        code, response, _ = core.http("POST", f"{URLS[service]}/admin/integrity/known-keys", body)
        if code != 200:
            raise RuntimeError(f"cannot configure known keys on {service}: {response}")


def set_signing_key(key_id: str | None = None, secret: str | None = None, reset: bool = False) -> None:
    body = {"reset": True} if reset else {"keyId": key_id, "secret": secret}
    code, response, _ = core.http("POST", f"{URLS['profiling']}/admin/integrity/signing-key", body)
    if code != 200:
        raise RuntimeError(f"cannot configure signing key: {response}")


def metrics(service: str) -> dict:
    code, body, _ = core.http("GET", f"{URLS[service]}/metrics")
    return body if code == 200 else {}


def quoting_metrics() -> dict[str, dict]:
    return {service: metrics(service) for service in QUOTING}


def delta(before: dict, after: dict, name: str) -> int:
    return int(after.get(name, 0)) - int(before.get(name, 0))


def deltas(before: dict[str, dict], after: dict[str, dict], name: str) -> dict[str, int]:
    return {service: delta(before[service], after[service], name) for service in QUOTING}


def wait_for_quoting_delta(before: dict[str, dict], name: str, minimum: int = 1, timeout: float = 20) -> dict[str, dict]:
    core.wait_until(
        f"{name} +{minimum} on {QUOTING}",
        lambda: all(v >= minimum for v in deltas(before, quoting_metrics(), name).values()),
        timeout=timeout,
    )
    return quoting_metrics()


def views(customer_id: str) -> dict[str, dict | None]:
    result = {}
    for service in QUOTING:
        code, body, _ = core.http("GET", f"{URLS[service]}/materialized-profiles/{customer_id}")
        result[service] = {"version": body.get("version"), "risk_score": body.get("risk_score")} if code == 200 else None
    return result


def compose_logs(since: datetime, *services: str) -> list[dict]:
    completed = subprocess.run(
        ["docker", "compose", "logs", "--no-log-prefix", "--since",
         (since - timedelta(seconds=5)).strftime("%Y-%m-%dT%H:%M:%SZ"), *services],
        cwd=core.ROOT, capture_output=True, text=True, timeout=30, check=True,
    )
    records = []
    for line in completed.stdout.splitlines():
        try:
            record = json.loads(line)
        except ValueError:
            record = {"raw": line}
        records.append(record)
    return records


def gateway_url(customer_id: str) -> str:
    return f"{URLS['gateway']}/quotes/{customer_id}"


def expect_denial(token: str | None, expected_code: int, expected_reason: str,
                  customer_id: str = "C001") -> dict:
    code, body, latency = call_with_token("GET", gateway_url(customer_id), token)
    reason = body.get("reason")
    return {
        "http_status": code,
        "reason": reason,
        "latency_ms": round(latency, 2),
        "evidence": f"status={code}, reason={reason}",
        "accepted": code == expected_code and reason == expected_reason,
    }


def e0_baseline() -> dict | None:
    path = core.RESULTS / "E0.json"
    if not path.exists():
        return None
    payload = json.loads(path.read_text())
    return {key: payload.get(key) for key in ("executedAt", "throughput", "p50", "p95", "p99")}


def overhead_vs_e0(result: dict) -> dict | None:
    baseline = e0_baseline()
    if not baseline or baseline.get("p95") is None:
        return None
    return {
        "baseline": baseline,
        "p95_delta_ms": round(result["p95"] - baseline["p95"], 2),
        "throughput_delta": round(result["throughput"] - baseline["throughput"], 2),
    }


# ------------------------------------------------------ AS-4 confidentiality

def sec_c0() -> dict:
    core.preload()
    token = issue_token("C001")
    codes = [call_with_token("GET", gateway_url("C001"), token)[0] for _ in range(20)]
    availability = round(100 * codes.count(200) / len(codes), 3)
    return {"requests": len(codes), "availability": availability,
            "evidence": f"availability={availability}%, status={sorted(set(map(str, codes)))}",
            "accepted": availability == 100.0}


def sec_c5() -> dict:
    core.preload()
    since = datetime.now(timezone.utc)
    correlation_id = str(uuid.uuid4())
    token = issue_token("C002", scopes=["quotes:read", "delegated:C001"])
    code, body, _ = call_with_token("GET", gateway_url("C001"), token, correlation_id)

    def audit():
        return next((r for r in compose_logs(since, "gateway")
                     if r.get("event") == "AUTHZ_DECISION" and r.get("correlationId") == correlation_id), None)

    record = core.wait_until("AUTHZ_DECISION audit record", audit, timeout=10)
    return {"http_status": code, "premium": body.get("premium"), "audit_record": record,
            "evidence": f"status={code}, audit={record.get('decision')}/{record.get('reason')}",
            "accepted": code == 200 and record.get("decision") == "PERMIT" and record.get("reason") == "DELEGATION"}


def sec_c7() -> dict:
    token = issue_token("C001")
    core.compose("stop", "pdp")
    try:
        samples = [call_with_token("GET", gateway_url("C001"), token)[:2] for _ in range(5)]
    finally:
        core.start_healthy("pdp")
    core.wait_until("PDP healthy", lambda: core.http("GET", f"{URLS['pdp']}/health")[0] == 200, timeout=30)
    codes = [code for code, _ in samples]
    reasons = sorted({body.get("reason") for _, body in samples})
    return {"http_statuses": codes, "reasons": reasons,
            "evidence": f"status={sorted(set(codes))}, reason={reasons}",
            "accepted": 200 not in codes and set(codes) == {503} and reasons == ["PDP_UNAVAILABLE"]}


def sec_c9() -> dict:
    core.preload()
    before = metrics("gateway")
    result = core.run_load()
    after = metrics("gateway")
    checks = delta(before, after, "authz_checks")
    duration_us = delta(before, after, "authz_duration_us_total")
    result["authz"] = {
        "checks": checks,
        "permits": delta(before, after, "authz_permit"),
        "denies": delta(before, after, "authz_deny"),
        "mean_overhead_ms": round(duration_us / checks / 1000, 3) if checks else None,
    }
    result["overhead_vs_e0"] = overhead_vs_e0(result)
    result["evidence"] = (f"availability={result['availability']}%, p95={result['p95']}ms, "
                          f"authz_mean={result['authz']['mean_overhead_ms']}ms")
    result["accepted"] = result["availability"] == 100.0 and result["authz"]["denies"] == 0
    return result


# ------------------------------------------------------------ AS-8 integrity

def sec_i0() -> dict:
    core.preload()
    before = quoting_metrics()
    code, body, _ = core.http("POST", f"{URLS['profiling']}/profiles/C001/refresh", timeout=8)
    if code != 200:
        raise RuntimeError(body)
    event = body["event"]
    after = wait_for_quoting_delta(before, "decision_applied")
    verified = deltas(before, after, "integrity_verified")
    rejected = deltas(before, after, "integrity_rejected")
    return {"event_version": event["version"], "key_id": event.get("keyId"),
            "payload_hash_present": bool(event.get("payloadHash")),
            "integrity_verified": verified, "integrity_rejected": rejected,
            "views": views("C001"),
            "evidence": f"keyId={event.get('keyId')}, verified={verified}, decision=APPLIED",
            "accepted": bool(event.get("payloadHash")) and all(v >= 1 for v in verified.values())
                        and not any(rejected.values())}


def forged_event_scenario(forge, expected_reason: str) -> dict:
    core.preload()
    original = last_signed_event("C001")
    views_before = views("C001")
    code, quote_before, _ = core.http("GET", gateway_url("C001"), headers=core.auth_headers("C001"))
    before = quoting_metrics()
    forged = forge(original)
    metric = f"integrity_rejected_{expected_reason.lower()}"
    after = wait_for_quoting_delta(before, metric)
    _, quote_after, _ = core.http("GET", gateway_url("C001"), headers=core.auth_headers("C001"))
    views_after = views("C001")
    rejected = deltas(before, after, metric)
    applied = deltas(before, after, "decision_applied")
    unchanged = views_before == views_after and quote_before.get("premium") == quote_after.get("premium")
    return {"forged_event_id": forged["eventId"], "forged_version": forged["version"],
            "rejected": rejected, "applied_delta": applied, "views_before": views_before,
            "views_after": views_after, "premium_before": quote_before.get("premium"),
            "premium_after": quote_after.get("premium"),
            "evidence": f"reason={expected_reason}, rejected={rejected}, premiumUnchanged={unchanged}",
            "accepted": code == 200 and all(v >= 1 for v in rejected.values())
                        and not any(applied.values()) and unchanged}


def acl_probe(attempts: dict[str, tuple]) -> dict:
    outputs = {name: redis_cli(credentials, *args) for name, (credentials, args) in attempts.items()}
    denied = {name: "NOPERM" in output for name, output in outputs.items()}
    return {"responses": outputs, "denied": denied}


def sec_i4() -> dict:
    before = quoting_metrics()
    probe = acl_probe({
        "anonymous_xadd": (ANONYMOUS, ("XADD", STREAM_PROFILE_UPDATED, "*", "eventId", "sec-i4-anon")),
        "consumer_xadd": (CONSUMER, ("XADD", STREAM_PROFILE_UPDATED, "*", "eventId", "sec-i4-consumer")),
    })
    time.sleep(3)
    after = quoting_metrics()
    consumed = deltas(before, after, "events_consumed")
    rejected = deltas(before, after, "integrity_rejected")
    probe.update({"events_consumed_delta": consumed, "integrity_rejected_delta": rejected,
                  "evidence": f"denied={probe['denied']}, consumed={consumed}"})
    probe["accepted"] = all(probe["denied"].values()) and not any(consumed.values()) and not any(rejected.values())
    return probe


def sec_i5() -> dict:
    read_group = ("XREADGROUP", "GROUP", "sec-i5-probe", "attacker", "COUNT", "1",
                  "STREAMS", STREAM_PROFILE_UPDATED, ">")
    probe = acl_probe({
        "anonymous_xreadgroup": (ANONYMOUS, read_group),
        "producer_xreadgroup": (PRODUCER, read_group),
        "producer_xrange": (PRODUCER, ("XRANGE", STREAM_PROFILE_UPDATED, "-", "+", "COUNT", "1")),
    })
    probe["evidence"] = f"denied={probe['denied']}"
    probe["accepted"] = all(probe["denied"].values())
    return probe


def sec_i6() -> dict:
    core.preload()
    old_key_id = last_signed_event("C001")["keyId"]
    new_key_id = "test-key-2026-10"
    new_secret = f"{SECRET_MARKER}-hmac-{secrets.token_hex(16)}"
    before = quoting_metrics()
    core.http("POST", f"{URLS['quoting-a']}/admin/materializer", {"paused": True})
    try:
        # Signed with the previous key, left pending in quoting-a until after the rotation.
        code, body, _ = core.http("POST", f"{URLS['profiling']}/profiles/C001/refresh", timeout=8)
        if code != 200:
            raise RuntimeError(body)
        set_known_key(new_key_id, new_secret)
        set_signing_key(new_key_id, new_secret)
        code, body, _ = core.http("POST", f"{URLS['profiling']}/profiles/C001/refresh", timeout=8)
        if code != 200:
            raise RuntimeError(body)
        rotated_event = body["event"]
        core.wait_until("quoting-b applies rotated event",
                        lambda: core.profile_version("quoting-b", "C001") >= int(rotated_event["version"]))
        core.http("POST", f"{URLS['quoting-a']}/admin/materializer", {"paused": False})
        core.wait_until("quoting-a recovers pending events",
                        lambda: core.profile_version("quoting-a", "C001") >= int(rotated_event["version"]))
        after = quoting_metrics()
    finally:
        core.http("POST", f"{URLS['quoting-a']}/admin/materializer", {"paused": False})
        set_signing_key(reset=True)
        set_known_key(reset=True)
    by_key = {service: {key: delta(before[service], after[service], f"integrity_verified_key_{key}")
                        for key in (old_key_id, new_key_id)} for service in QUOTING}
    rejected = deltas(before, after, "integrity_rejected")
    return {"previous_key_id": old_key_id, "new_key_id": new_key_id,
            "rotated_event_key_id": rotated_event.get("keyId"), "verified_by_key": by_key,
            "integrity_rejected": rejected, "views": views("C001"),
            "evidence": f"quoting-a verified={by_key['quoting-a']}, rejected={rejected}",
            "accepted": rotated_event.get("keyId") == new_key_id
                        and all(v >= 1 for v in by_key["quoting-a"].values())
                        and by_key["quoting-b"][new_key_id] >= 1 and not any(rejected.values())}


def sec_i7() -> dict:
    core.preload()
    event = last_signed_event("C001")
    before = quoting_metrics()
    xadd_event(event)
    after = wait_for_quoting_delta(before, "decision_duplicate")
    duplicates = deltas(before, after, "decision_duplicate")
    rejected = deltas(before, after, "integrity_rejected")
    return {"event_id": event["eventId"], "duplicates": duplicates, "integrity_rejected": rejected,
            "evidence": f"decision=DUPLICATE {duplicates}, rejected={rejected}",
            "accepted": all(v >= 1 for v in duplicates.values()) and not any(rejected.values())}


def sec_i8() -> dict:
    core.preload()
    before = quoting_metrics()
    stop = threading.Event()
    published: list[dict] = []

    def refresher() -> None:
        counter = 0
        while not stop.is_set():
            customer_id = core.CUSTOMERS[counter % len(core.CUSTOMERS)]
            code, body, _ = core.http("POST", f"{URLS['profiling']}/profiles/{customer_id}/refresh", timeout=8)
            if code == 200:
                published.append(body["event"])
            counter += 1
            stop.wait(0.2)

    thread = threading.Thread(target=refresher, daemon=True)
    thread.start()
    try:
        result = core.run_load()
    finally:
        stop.set()
        thread.join(timeout=10)
    after = wait_for_quoting_delta(before, "integrity_verified", minimum=len(published), timeout=30)
    verified = deltas(before, after, "integrity_verified")
    rejected = deltas(before, after, "integrity_rejected")
    result.update({"events_published": len(published), "integrity_verified": verified,
                   "integrity_rejected": rejected, "overhead_vs_e0": overhead_vs_e0(result)})
    result["evidence"] = (f"availability={result['availability']}%, signed={len(published)}, "
                          f"verified={verified}, rejected={rejected}, p95={result['p95']}ms")
    result["accepted"] = (result["availability"] == 100.0 and len(published) > 0
                          and all(v >= len(published) for v in verified.values()) and not any(rejected.values()))
    return result


def sec_i9() -> dict:
    core.preload()
    since = datetime.now(timezone.utc)
    original = last_signed_event("C001")
    before = quoting_metrics()
    forged = {
        "SIGNATURE_MISMATCH": tamper_and_republish(original)["eventId"],
        "MISSING_INTEGRITY_FIELDS": unsigned_and_republish(original)["eventId"],
        "UNKNOWN_KEY": rogue_signed_and_republish(original)["eventId"],
    }
    wait_for_quoting_delta(before, "integrity_rejected", minimum=3)
    audited_fields = ("timestamp", "service", "instanceId", "eventId", "customerId", "producerId", "keyId", "reason")

    def audit_records():
        records = [r for r in compose_logs(since, *QUOTING)
                   if r.get("event") == "INTEGRITY_CHECK" and r.get("result") == "REJECTED"
                   and r.get("eventId") in forged.values()]
        return records if len(records) >= 2 * len(forged) else None

    records = core.wait_until("INTEGRITY_CHECK audit records", audit_records, timeout=15)
    all_lines = compose_logs(since, "gateway", "profiling", *QUOTING)
    leaked = [line for line in all_lines if SECRET_MARKER in json.dumps(line)]
    reasons_by_event = {r["eventId"]: r["reason"] for r in records}
    return {
        "forged_events": forged,
        "audit_records": [{field: r.get(field) for field in audited_fields} for r in records],
        "log_lines_scanned": len(all_lines),
        "secret_material_in_logs": len(leaked),
        "evidence": f"reasons={sorted(set(reasons_by_event.values()))}, records={len(records)}, leaks={len(leaked)}",
        "accepted": all(reasons_by_event.get(event_id) == reason for reason, event_id in forged.items())
                    and not leaked,
    }


SCENARIOS = {
    "SEC-C0": sec_c0,
    "SEC-C1": lambda: expect_denial(None, 401, "MISSING_TOKEN"),
    "SEC-C2": lambda: expect_denial(issue_token("C001", exp_delta=-60), 401, "TOKEN_EXPIRED"),
    "SEC-C3": lambda: expect_denial(issue_token("C002"), 403, "OWNERSHIP_MISMATCH"),
    "SEC-C4": lambda: expect_denial(issue_token("C001", scopes=["profiles:read"]), 403, "INSUFFICIENT_SCOPE"),
    "SEC-C5": sec_c5,
    "SEC-C6": lambda: expect_denial(issue_token("C001", tenant="tenant-otro"), 403, "TENANT_MISMATCH"),
    "SEC-C7": sec_c7,
    "SEC-C8": lambda: expect_denial(tamper_signature(issue_token("C001")), 401, "INVALID_SIGNATURE"),
    "SEC-C9": sec_c9,
    "SEC-I0": sec_i0,
    "SEC-I1": lambda: forged_event_scenario(tamper_and_republish, "SIGNATURE_MISMATCH"),
    "SEC-I2": lambda: forged_event_scenario(unsigned_and_republish, "MISSING_INTEGRITY_FIELDS"),
    "SEC-I3": lambda: forged_event_scenario(rogue_signed_and_republish, "UNKNOWN_KEY"),
    "SEC-I4": sec_i4,
    "SEC-I5": sec_i5,
    "SEC-I6": sec_i6,
    "SEC-I7": sec_i7,
    "SEC-I8": sec_i8,
    "SEC-I9": sec_i9,
}


def execute(scenario: str) -> dict:
    return SCENARIOS[scenario]()
