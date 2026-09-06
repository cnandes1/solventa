#!/usr/bin/env python3

import json
import sys
import time
import urllib.error
import urllib.request


def request_http(method, url, body=None, timeout=5):
    """Executes an HTTP request and returns (code, decoded_json, latency_s)."""
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(
        url,
        data=data,
        method=method,
        headers={"Content-Type": "application/json"},
    )
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


def wait_for(url, condition, total_timeout=45, pause=1):
    """Retries a GET until the response satisfies the condition or time runs
    out. Polling/retry pattern."""
    deadline = time.time() + total_timeout
    last = None
    while time.time() < deadline:
        code, body, _ = request_http("GET", url)
        last = body
        if code == 200 and condition(body):
            return body
        time.sleep(pause)
    return last


def mode_monitor(base, interval):
    status_url = base + "/gateway/status"
    print(f"Monitoring {status_url} every {interval}s. Ctrl+C to stop.\n")
    try:
        while True:
            ts = time.strftime("%H:%M:%S")
            code, body, latency = request_http("GET", status_url, timeout=3)
            if code == 200 and body:
                instances = body.get("instances", {})
                summary = ", ".join(
                    f"{b}={info.get('state')}(fails={info.get('fails')})"
                    for b, info in instances.items()
                )
                print(f"[{ts}] {summary}")
            else:
                print(f"[{ts}] ERROR querying /gateway/status (code={code}): {body}")
            time.sleep(interval)
    except KeyboardInterrupt:
        print("\nMonitor stopped by the user.")


def mode_load(base, duration):
    print(f"Generating load against the Quoting pool via {base} for {duration}s...\n")

    end = time.time() + duration
    successes = 0
    errors = 0
    latencies = []
    counter = 0

    while time.time() < end:
        counter += 1
        customer_id = str((counter % 5) + 1)

        code, _, latency = request_http("GET", f"{base}/quotes/{customer_id}", timeout=5)
        latencies.append(latency)
        if code == 200:
            successes += 1
        else:
            errors += 1
            print(f"  [{time.strftime('%H:%M:%S')}] GET /quotes/{customer_id} failed (code={code})")

        code, _, latency = request_http("POST", f"{base}/quotes/{customer_id}/request-refresh", timeout=5)
        latencies.append(latency)
        if code in (200, 202):
            successes += 1
        else:
            errors += 1
            print(f"  [{time.strftime('%H:%M:%S')}] POST /quotes/{customer_id}/request-refresh failed (code={code})")

        time.sleep(0.5)

    total = successes + errors
    avg_latency = sum(latencies) / len(latencies) if latencies else 0
    max_latency = max(latencies) if latencies else 0

    print("\n--- Load result ---")
    print(f"Total requests: {total}")
    print(f"Successes: {successes} ({100 * successes / total:.1f}%)" if total else "Successes: 0")
    print(f"Errors:    {errors} ({100 * errors / total:.1f}%)" if total else "Errors: 0")
    print(f"Average latency: {avg_latency * 1000:.1f} ms")
    print(f"Max latency:     {max_latency * 1000:.1f} ms")


def main():
    if len(sys.argv) < 2 or sys.argv[1] not in ("monitor", "load"):
        print(__doc__)
        sys.exit(1)

    mode = sys.argv[1]
    base = (sys.argv[2] if len(sys.argv) > 2 else "http://localhost:8080").rstrip("/")

    if mode == "monitor":
        interval = float(sys.argv[3]) if len(sys.argv) > 3 else 2.0
        mode_monitor(base, interval)
    else:
        duration = float(sys.argv[3]) if len(sys.argv) > 3 else 30.0
        mode_load(base, duration)


if __name__ == "__main__":
    main()
