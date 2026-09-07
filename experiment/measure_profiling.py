#!/usr/bin/env python3

import json
import sys
import time
import urllib.error
import urllib.request


def request_http(method, url, timeout=5):
    req = urllib.request.Request(url, method=method)
    start = time.time()
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            resp_body = r.read()
            latency = time.time() - start
            try:
                return r.status, json.loads(resp_body or b"null"), latency
            except ValueError:
                return r.status, None, latency
    except urllib.error.HTTPError as e:
        latency = time.time() - start
        try:
            return e.code, json.loads(e.read() or b"null"), latency
        except ValueError:
            return e.code, None, latency
    except (urllib.error.URLError, TimeoutError, OSError) as e:
        latency = time.time() - start
        return None, {"error": str(e)}, latency


def _run(base, n, customer_id, title):
    url = f"{base}/profiles/{customer_id}/refresh"
    print(f"{title}: {n} POST to {url}\n")

    successes = 0
    complete_votes = 0
    discrepancies = 0
    circuit_opened_at_some_point = False
    last_circuit_state = None

    for i in range(1, n + 1):
        code, body, latency = request_http("POST", url, timeout=5)
        latency_ms = latency * 1000
        if code == 200:
            successes += 1

        vote = (body or {}).get("vote", {})
        cb = (body or {}).get("circuitBreaker", {})
        last_circuit_state = cb.get("state")
        if cb.get("state") == "OPEN":
            circuit_opened_at_some_point = True
        if not vote.get("voteIncomplete", True):
            complete_votes += 1
        if vote.get("discrepancyDetected"):
            discrepancies += 1

        print(
            f"  [{i:02d}] code={code} latency_ms={latency_ms:.1f} "
            f"circuit={cb.get('state')} final_score={vote.get('finalScore')} "
            f"vote_incomplete={vote.get('voteIncomplete')} "
            f"discrepancy={vote.get('discrepancyDetected')}"
        )
        time.sleep(0.1)

    print("\n--- Result ---")
    print(f"Total requests:       {n}")
    print(f"200 responses:        {successes} ({100 * successes / n:.1f}%)")
    print(f"Complete vote 3/3:    {complete_votes} ({100 * complete_votes / n:.1f}%)")
    print(f"Rounds with discrepancy: {discrepancies}")
    print(f"Circuit opened at some point: {circuit_opened_at_some_point}")
    print(f"Last circuit state: {last_circuit_state}")

    if successes == n:
        print("PASS: no 5xx response reached the caller (the provider's failure, if any, was masked).")
    else:
        print("FAIL: there were responses other than 200 -- the error propagated outside Profiling.")

    return {
        "successes": successes,
        "complete_votes": complete_votes,
        "discrepancies": discrepancies,
        "circuit_opened_at_some_point": circuit_opened_at_some_point,
    }


def mode_normal(base, n, customer_id):
    result = _run(base, n, customer_id, "NORMAL mode (healthy provider expected)")
    if result["circuit_opened_at_some_point"]:
        print("FAIL: the circuit opened while the provider was supposedly healthy.")
    else:
        print("PASS: the circuit stayed closed throughout the normal scenario.")


def mode_degraded(base, n, customer_id):
    result = _run(base, n, customer_id, "DEGRADED mode (provider down/slow expected)")
    if result["circuit_opened_at_some_point"]:
        print("PASS: the circuit breaker opened after the provider's consecutive failures (prevents cascading failure).")
    else:
        print("FAIL (or not enough rounds): the circuit never opened; check CB_FAILS_THRESHOLD and that the provider is actually degraded.")


def mode_circuit(base, customer_id):
    url = f"{base}/circuit-state"
    code, body, _ = request_http("GET", url)
    print(json.dumps(body, indent=2, ensure_ascii=False))


def main():
    if len(sys.argv) < 2 or sys.argv[1] not in ("normal", "degraded", "circuit"):
        print(__doc__)
        sys.exit(1)

    mode = sys.argv[1]
    base = (sys.argv[2] if len(sys.argv) > 2 else "http://localhost:7500").rstrip("/")

    if mode == "circuit":
        customer_id = sys.argv[3] if len(sys.argv) > 3 else "1"
        mode_circuit(base, customer_id)
        return

    n = int(sys.argv[3]) if len(sys.argv) > 3 else 30
    customer_id = sys.argv[4] if len(sys.argv) > 4 else "1"

    if mode == "normal":
        mode_normal(base, n, customer_id)
    else:
        mode_degraded(base, n, customer_id)


if __name__ == "__main__":
    main()
