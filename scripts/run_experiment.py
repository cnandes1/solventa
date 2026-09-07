#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import json
import subprocess
import threading
import time
import urllib.error
import urllib.request
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
RESULTS = ROOT / "results"
CUSTOMERS = ("C001", "C002", "C003")
URLS = {
    "gateway": "http://localhost:8080",
    "profiling": "http://localhost:7500",
    "quoting-a": "http://localhost:7002",
    "quoting-b": "http://localhost:7001",
    "open-finance": "http://localhost:6000",
}
HYPOTHESES = {
    "E0": "Baseline cuantitativo",
    "E1": "ASR-02: fallback ante Open Finance DOWN",
    "E2": "ASR-02: timeout ante Open Finance lento",
    "E3": "H1: Cotización usa estado local con Perfilamiento DOWN",
    "E4": "H1: recuperación e idempotencia de eventos",
    "E5": "H2: detección, retiro y failover de réplica",
    "E6": "H3: reintegro DOWN-SHADOW-ACTIVE",
    "E7": "H4: consenso excluye outlier",
    "E8": "H4: timeout decide con respuesta ausente",
    "E9": "H1: vista local sobrevive a Redis Business DOWN",
}
CRITERIA = {
    "E0": ("availability", "100% para perfiles precargados"),
    "E1": ("availability + circuit + fallback", "100%, circuito OPEN y fallback >= 3"),
    "E2": ("availability + timeout", "100%, circuito OPEN y timeouts >= 3"),
    "E3": ("availability + sync baseline", "100% EDA y falla del baseline síncrono"),
    "E4": ("pending recovery", "pendientes recuperados y misma versión A/B"),
    "E5": ("availability + routing", "100% y Quoting B en DOWN"),
    "E6": ("state transitions", "SHADOW observado antes de ACTIVE"),
    "E7": ("voting outlier", "resultado 40 y C marcado como outlier"),
    "E8": ("voting timeout", "resultado 40, C ausente y duración < 2.5 s"),
    "E9": ("availability", "100% desde SQLite con Redis Business DOWN"),
}


def http(method: str, url: str, body: dict | None = None, timeout: float = 5) -> tuple[int | None, dict, float]:
    data = json.dumps(body).encode() if body is not None else None
    request = urllib.request.Request(url, data=data, method=method, headers={"Content-Type": "application/json"})
    started = time.monotonic()
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            return response.status, json.loads(response.read() or b"{}"), (time.monotonic() - started) * 1000
    except urllib.error.HTTPError as exc:
        try:
            response_body = json.loads(exc.read() or b"{}")
        except ValueError:
            response_body = {"error": str(exc)}
        return exc.code, response_body, (time.monotonic() - started) * 1000
    except (OSError, TimeoutError, urllib.error.URLError) as exc:
        return None, {"error": str(exc)}, (time.monotonic() - started) * 1000


def compose(*args: str) -> None:
    subprocess.run(["docker", "compose", *args], cwd=ROOT, check=True)


def percentile(values: list[float], quantile: float) -> float:
    if not values:
        return 0.0
    ordered = sorted(values)
    index = min(len(ordered) - 1, max(0, int(round((len(ordered) - 1) * quantile))))
    return round(ordered[index], 2)


def run_load(duration_seconds: float = 6, workers: int = 8) -> dict:
    deadline = time.monotonic() + duration_seconds
    lock = threading.Lock()
    samples: list[tuple[int | None, float, str]] = []

    def worker(offset: int) -> None:
        counter = offset
        while time.monotonic() < deadline:
            customer_id = CUSTOMERS[counter % len(CUSTOMERS)]
            code, body, latency = http("GET", f"{URLS['gateway']}/quotes/{customer_id}", timeout=3)
            category = body.get("resultCategory", "TECHNICAL_FAILURE")
            with lock:
                samples.append((code, latency, category))
            counter += workers

    started = time.monotonic()
    with ThreadPoolExecutor(max_workers=workers) as pool:
        for index in range(workers):
            pool.submit(worker, index)
    elapsed = max(0.001, time.monotonic() - started)
    successes = sum(1 for code, _, category in samples if code == 200 and category in {"SUCCESS", "DEGRADED_SUCCESS"})
    latencies = [latency for _, latency, _ in samples]
    total = len(samples)
    return {
        "total_requests": total,
        "successful_requests": successes,
        "failed_requests": total - successes,
        "availability": round(100 * successes / total, 3) if total else 0,
        "throughput": round(total / elapsed, 2),
        "p50": percentile(latencies, 0.50),
        "p95": percentile(latencies, 0.95),
        "p99": percentile(latencies, 0.99),
        "categories": {name: sum(1 for _, _, category in samples if category == name)
                       for name in {sample[2] for sample in samples}},
    }


def set_open_finance(mode: str, latency_ms: int = 50) -> dict:
    code, body, _ = http("POST", f"{URLS['open-finance']}/admin/mode", {"mode": mode, "latencyMs": latency_ms})
    if code != 200:
        raise RuntimeError(f"cannot configure Open Finance mock: {body}")
    return body


def set_voting_scenario(body: dict) -> None:
    code, response, _ = http("POST", f"{URLS['profiling']}/admin/voting-scenario", body)
    if code != 200:
        raise RuntimeError(f"cannot configure voting: {response}")


def wait_until(description: str, predicate, timeout: float = 30, interval: float = 0.5):
    deadline = time.monotonic() + timeout
    last = None
    while time.monotonic() < deadline:
        last = predicate()
        if last:
            return last
        time.sleep(interval)
    raise RuntimeError(f"timeout waiting for {description}; last={last}")


def preload() -> dict[str, int]:
    versions = {}
    set_open_finance("NORMAL", 50)
    set_voting_scenario({})
    http("POST", f"{URLS['profiling']}/admin/circuit/reset")
    for customer_id in CUSTOMERS:
        code, body, _ = http("POST", f"{URLS['profiling']}/profiles/{customer_id}/refresh", timeout=8)
        if code != 200:
            raise RuntimeError(f"preload failed for {customer_id}: {body}")
        versions[customer_id] = int(body["event"]["version"])
    for service in ("quoting-a", "quoting-b"):
        for customer_id, version in versions.items():
            wait_until(
                f"{service} profile {customer_id} v{version}",
                lambda service=service, customer_id=customer_id, version=version:
                    profile_version(service, customer_id) >= version,
            )
        wait_until(
            f"{service} detects Profiling",
            lambda service=service: quoting_runtime_ready(service),
            timeout=10,
        )
    return versions


def profile_version(service: str, customer_id: str) -> int:
    code, body, _ = http("GET", f"{URLS[service]}/materialized-profiles/{customer_id}")
    return int(body.get("version", -1)) if code == 200 else -1


def quoting_runtime_ready(service: str) -> bool:
    code, body, _ = http("GET", f"{URLS[service]}/metrics")
    runtime = body.get("runtime", {}) if code == 200 else {}
    return bool(runtime.get("businessBusAvailable") and runtime.get("profilingAvailable"))


def scenario_load_only(scenario: str, setup=None, teardown=None) -> dict:
    preload()
    try:
        if setup:
            setup()
        metrics = run_load()
    finally:
        if teardown:
            teardown()
    metrics["accepted"] = metrics["availability"] == 100.0
    return metrics


def execute(scenario: str) -> dict:
    if scenario == "E0":
        return scenario_load_only(scenario)
    if scenario == "E1":
        preload()
        _, before, _ = http("GET", f"{URLS['profiling']}/metrics")
        try:
            set_open_finance("DOWN")
            for _ in range(4):
                http("POST", f"{URLS['profiling']}/profiles/C001/refresh", timeout=5)
            result = run_load()
            _, after, _ = http("GET", f"{URLS['profiling']}/metrics")
            _, circuit_state, _ = http("GET", f"{URLS['profiling']}/circuit-state")
        finally:
            set_open_finance("NORMAL", 50)
            http("POST", f"{URLS['profiling']}/admin/circuit/reset")
        result["profiling_metrics_delta"] = {
            "http_errors": after.get("open_finance_http_errors", 0) - before.get("open_finance_http_errors", 0),
            "fallback_count": after.get("fallback_count", 0) - before.get("fallback_count", 0),
        }
        result["circuit_state_during_failure"] = circuit_state["state"]
        result["accepted"] = (
            result["availability"] == 100.0
            and circuit_state["state"] == "OPEN"
            and result["profiling_metrics_delta"]["fallback_count"] >= 3
        )
        return result
    if scenario == "E2":
        preload()
        _, before, _ = http("GET", f"{URLS['profiling']}/metrics")
        try:
            set_open_finance("SLOW", 1500)
            for _ in range(4):
                http("POST", f"{URLS['profiling']}/profiles/C001/refresh", timeout=5)
            result = run_load()
            _, after, _ = http("GET", f"{URLS['profiling']}/metrics")
            _, circuit_state, _ = http("GET", f"{URLS['profiling']}/circuit-state")
        finally:
            set_open_finance("NORMAL", 50)
            http("POST", f"{URLS['profiling']}/admin/circuit/reset")
        result["profiling_metrics_delta"] = {
            "timeouts": after.get("open_finance_timeouts", 0) - before.get("open_finance_timeouts", 0),
            "fallback_count": after.get("fallback_count", 0) - before.get("fallback_count", 0),
        }
        result["circuit_state_during_failure"] = circuit_state["state"]
        result["accepted"] = (
            result["availability"] == 100.0
            and circuit_state["state"] == "OPEN"
            and result["profiling_metrics_delta"]["timeouts"] >= 3
        )
        return result
    if scenario == "E3":
        preload()
        compose("stop", "profiling")
        try:
            time.sleep(3)
            result = run_load()
            code, _, _ = http("GET", f"{URLS['gateway']}/sync/quotes/C001", timeout=3)
        finally:
            compose("start", "profiling")
        result["sync_baseline_failed"] = code != 200
        result["accepted"] = result["availability"] == 100.0 and result["sync_baseline_failed"]
        return result
    if scenario == "E4":
        versions = preload()
        http("POST", f"{URLS['quoting-a']}/admin/materializer", {"paused": True})
        try:
            for _ in range(3):
                code, body, _ = http("POST", f"{URLS['profiling']}/profiles/C001/refresh", timeout=8)
                if code != 200:
                    raise RuntimeError(body)
                versions["C001"] = int(body["event"]["version"])
            wait_until("quoting-b latest profile", lambda: profile_version("quoting-b", "C001") >= versions["C001"])
            time.sleep(6)
        finally:
            http("POST", f"{URLS['quoting-a']}/admin/materializer", {"paused": False})
        wait_until("quoting-a pending recovery", lambda: profile_version("quoting-a", "C001") >= versions["C001"])
        _, metrics_a, _ = http("GET", f"{URLS['quoting-a']}/metrics")
        return {"target_version": versions["C001"], "quoting_a_version": profile_version("quoting-a", "C001"),
                "quoting_b_version": profile_version("quoting-b", "C001"), "metrics": metrics_a,
                "accepted": metrics_a.get("pending_recovered", 0) >= 1}
    if scenario == "E5":
        preload()
        compose("stop", "quoting-b")
        try:
            result = run_load(8)
            status = wait_until("quoting-b DOWN", lambda: gateway_instance_state("quoting-b", "DOWN"))
            result["gateway_state"] = status
            result["accepted"] = result["availability"] == 100.0 and status["state"] == "DOWN"
            return result
        finally:
            compose("start", "quoting-b")
    if scenario == "E6":
        preload()
        compose("stop", "quoting-b")
        wait_until("quoting-b DOWN", lambda: gateway_instance_state("quoting-b", "DOWN"))
        compose("start", "quoting-b")
        deadline = time.monotonic() + 35
        seen_shadow = False
        while time.monotonic() < deadline:
            http("GET", f"{URLS['gateway']}/quotes/C001")
            _, status, _ = http("GET", f"{URLS['gateway']}/gateway/status")
            state = status.get("instances", {}).get("quoting-b", {}).get("state")
            seen_shadow = seen_shadow or state == "SHADOW"
            if state == "ACTIVE":
                return {"seen_shadow": seen_shadow, "final_state": state, "accepted": seen_shadow}
            time.sleep(0.5)
        return {"seen_shadow": seen_shadow, "final_state": state, "accepted": False}
    if scenario == "E7":
        set_voting_scenario({"A": {"mode": "NORMAL", "score": 40}, "B": {"mode": "NORMAL", "score": 40}, "C": {"mode": "NORMAL", "score": 90}})
        try:
            code, body, _ = http("POST", f"{URLS['profiling']}/profiles/VOTE-E7/refresh", timeout=5)
        finally:
            set_voting_scenario({})
        vote = body.get("vote", {})
        return {"http_status": code, "vote": vote,
                "accepted": code == 200 and vote.get("finalScore") == 40 and vote.get("outlierStrategies") == ["C"]}
    if scenario == "E8":
        set_voting_scenario({"A": {"mode": "NORMAL", "score": 40}, "B": {"mode": "NORMAL", "score": 40}, "C": {"mode": "TIMEOUT"}})
        started = time.monotonic()
        try:
            code, body, _ = http("POST", f"{URLS['profiling']}/profiles/VOTE-E8/refresh", timeout=5)
        finally:
            set_voting_scenario({})
        duration_ms = round((time.monotonic() - started) * 1000, 1)
        vote = body.get("vote", {})
        return {"http_status": code, "duration_ms": duration_ms, "vote": vote,
                "accepted": code == 200 and vote.get("finalScore") == 40 and vote.get("missingStrategies") == ["C"] and duration_ms < 2500}
    if scenario == "E9":
        preload()
        compose("stop", "redis-business")
        try:
            time.sleep(2)
            result = run_load(6)
        finally:
            compose("start", "redis-business")
        result["accepted"] = result["availability"] == 100.0
        return result
    raise ValueError(scenario)


def gateway_instance_state(instance_id: str, expected: str):
    code, body, _ = http("GET", f"{URLS['gateway']}/gateway/status")
    state = body.get("instances", {}).get(instance_id, {}) if code == 200 else {}
    return state if state.get("state") == expected else None


def save_result(scenario: str, result: dict) -> None:
    RESULTS.mkdir(exist_ok=True)
    payload = {
        "scenario": scenario,
        "hypothesis": HYPOTHESES[scenario],
        "executedAt": datetime.now(timezone.utc).isoformat(),
        **result,
    }
    (RESULTS / f"{scenario}.json").write_text(json.dumps(payload, indent=2, ensure_ascii=False) + "\n")
    summary_path = RESULTS / "summary.csv"
    rows = []
    if summary_path.exists():
        with summary_path.open(newline="") as handle:
            rows = [row for row in csv.DictReader(handle) if row["scenario"] != scenario]
    rows.append({
        "scenario": scenario,
        "total_requests": result.get("total_requests", ""),
        "successful_requests": result.get("successful_requests", ""),
        "failed_requests": result.get("failed_requests", ""),
        "availability": result.get("availability", ""),
        "throughput": result.get("throughput", ""),
        "p50": result.get("p50", ""),
        "p95": result.get("p95", ""),
        "p99": result.get("p99", ""),
        "hypothesis": HYPOTHESES[scenario],
        "accepted": result.get("accepted", False),
    })
    fields = list(rows[0])
    with summary_path.open("w", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        writer.writerows(sorted(rows, key=lambda row: row["scenario"]))
    write_acceptance_matrix()


def evidence_text(scenario: str, payload: dict) -> str:
    if scenario in {"E0", "E9"}:
        return f"availability={payload.get('availability')}%"
    if scenario == "E1":
        delta = payload.get("profiling_metrics_delta", {})
        return f"availability={payload.get('availability')}%, circuit={payload.get('circuit_state_during_failure')}, fallback={delta.get('fallback_count')}"
    if scenario == "E2":
        delta = payload.get("profiling_metrics_delta", {})
        return f"availability={payload.get('availability')}%, circuit={payload.get('circuit_state_during_failure')}, timeouts={delta.get('timeouts')}"
    if scenario == "E3":
        return f"availability={payload.get('availability')}%, syncFailed={payload.get('sync_baseline_failed')}"
    if scenario == "E4":
        metrics = payload.get("metrics", {})
        return f"A=v{payload.get('quoting_a_version')}, B=v{payload.get('quoting_b_version')}, recovered={metrics.get('pending_recovered')}"
    if scenario == "E5":
        return f"availability={payload.get('availability')}%, B={payload.get('gateway_state', {}).get('state')}"
    if scenario == "E6":
        return f"shadow={payload.get('seen_shadow')}, final={payload.get('final_state')}"
    if scenario == "E7":
        vote = payload.get("vote", {})
        return f"score={vote.get('finalScore')}, outliers={vote.get('outlierStrategies')}"
    if scenario == "E8":
        vote = payload.get("vote", {})
        return f"score={vote.get('finalScore')}, missing={vote.get('missingStrategies')}, duration={payload.get('duration_ms')}ms"
    return ""


def write_acceptance_matrix() -> None:
    rows = []
    for scenario in (f"E{i}" for i in range(10)):
        path = RESULTS / f"{scenario}.json"
        if not path.exists():
            continue
        payload = json.loads(path.read_text())
        metric, criterion = CRITERIA[scenario]
        rows.append({
            "hypothesis": HYPOTHESES[scenario],
            "scenario": scenario,
            "metric": metric,
            "criterion": criterion,
            "result": evidence_text(scenario, payload),
            "status": "PASS" if payload.get("accepted") else "FAIL",
        })
    with (RESULTS / "acceptance_matrix.csv").open("w", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=("hypothesis", "scenario", "metric", "criterion", "result", "status"))
        writer.writeheader()
        writer.writerows(rows)


def main() -> None:
    parser = argparse.ArgumentParser(description="Run reproducible Solventa availability scenarios")
    parser.add_argument("scenario", choices=[*(f"E{i}" for i in range(10)), "all"])
    args = parser.parse_args()
    scenarios = [f"E{i}" for i in range(10)] if args.scenario == "all" else [args.scenario]
    failures = 0
    for scenario in scenarios:
        print(f"\n[{scenario}] {HYPOTHESES[scenario]}")
        try:
            result = execute(scenario)
        except Exception as exc:
            result = {"accepted": False, "error": str(exc)}
        save_result(scenario, result)
        passed = bool(result.get("accepted"))
        failures += not passed
        print(json.dumps(result, indent=2, ensure_ascii=False))
        print("PASS" if passed else "FAIL")
    raise SystemExit(1 if failures else 0)


if __name__ == "__main__":
    main()
