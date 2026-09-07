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


def mode_propagation(profiling_url, quoting_url, customer_id, timeout_s=60):
    refresh_url = f"{profiling_url}/profiles/{customer_id}/refresh"
    code, body, _ = request_http("POST", refresh_url, timeout=5)
    if code != 200:
        print(f"FAIL: could not refresh the profile (code={code}, body={body}).")
        return

    expected_version = int(body["event"]["version"])
    event_id = body["event"]["eventId"]
    print(f"Event published: eventId={event_id} version={expected_version}")
    print(f"Waiting for propagation to {quoting_url}/quotes/{customer_id} ...\n")

    view_url = f"{quoting_url}/quotes/{customer_id}"
    start = time.time()
    deadline = start + timeout_s

    while time.time() < deadline:
        code, body, _ = request_http("GET", view_url, timeout=5)
        actual_version = int(((body or {}).get("profile") or {}).get("version", -1))
        if code == 200 and actual_version >= expected_version:
            propagation_latency = time.time() - start
            print(f"Materialized view: {json.dumps(body, ensure_ascii=False)}")
            print(f"\nPASS: version {expected_version} propagated in {propagation_latency:.2f}s.")
            return
        time.sleep(0.5)

    print(f"FAIL: version {expected_version} did not propagate within {timeout_s}s "
          f"(check whether the consumer/redis/gateway are down on purpose -- "
          f"in that case this is expected until they recover).")


def mode_order(profiling_url, quoting_url, customer_id, n):
    refresh_url = f"{profiling_url}/profiles/{customer_id}/refresh"
    print(f"Triggering {n} consecutive refreshes against {refresh_url}\n")

    last_version = None
    for i in range(1, n + 1):
        code, body, latency = request_http("POST", refresh_url, timeout=5)
        if code == 200:
            last_version = int(body["event"]["version"])
            print(f"  [{i:02d}] version={last_version} latency_ms={latency * 1000:.1f}")
        else:
            print(f"  [{i:02d}] FAILED code={code}")

    if last_version is None:
        print("FAIL: no refresh succeeded.")
        return

    print(f"\nLast emitted version: {last_version}. Waiting for Quoting to converge...\n")
    view_url = f"{quoting_url}/quotes/{customer_id}"
    deadline = time.time() + 30
    while time.time() < deadline:
        code, body, _ = request_http("GET", view_url, timeout=5)
        actual_version = int(((body or {}).get("profile") or {}).get("version", -1))
        if code == 200 and actual_version >= last_version:
            print(f"Final materialized view: {json.dumps(body, ensure_ascii=False)}")
            if actual_version == last_version:
                print(f"PASS: the view landed exactly on the last version ({last_version}), no regression.")
            else:
                print(f"FAIL: the view landed on version {actual_version}, different from the last emitted ({last_version}).")
            return
        time.sleep(0.5)

    print(f"FAIL: the view did not converge to version {last_version} within the expected time.")


def main():
    if len(sys.argv) < 2 or sys.argv[1] not in ("propagation", "order"):
        print(__doc__)
        sys.exit(1)

    mode = sys.argv[1]
    profiling_url = (sys.argv[2] if len(sys.argv) > 2 else "http://localhost:7500").rstrip("/")
    quoting_url = (sys.argv[3] if len(sys.argv) > 3 else "http://localhost:7002").rstrip("/")
    customer_id = sys.argv[4] if len(sys.argv) > 4 else "1"

    if mode == "propagation":
        mode_propagation(profiling_url, quoting_url, customer_id)
    else:
        n = int(sys.argv[5]) if len(sys.argv) > 5 else 5
        mode_order(profiling_url, quoting_url, customer_id, n)


if __name__ == "__main__":
    main()
