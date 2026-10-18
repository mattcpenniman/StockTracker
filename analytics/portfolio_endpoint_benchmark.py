"""Alternate real HTTP portfolio requests and verify exact JSON/native-cache parity.

python -m analytics.portfolio_endpoint_benchmark --before-url http://127.0.0.1:5007 \
    --after-url http://192.168.1.80:5005 --repeats 20 --output /tmp/results.json

Use two isolated serving processes with the same PostgreSQL data. A distinct
event_limit per pair changes the existing state cache key but not state math;
native-call deltas verify each first request is cold and its repeat is warm.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import math
from pathlib import Path
import statistics
import time
from urllib.request import urlopen


def fetch(url):
    start = time.perf_counter()
    with urlopen(url, timeout=120) as response:
        payload = json.load(response)
        assert response.status == 200, payload
    return payload, (time.perf_counter() - start) * 1000


def native_calls(base):
    payload, _ = fetch(base + "/api/health/analytics")
    assert payload["runtime"]["effective_backend"] == "rust", payload
    return payload["runtime"]["execution"]["native_calls"]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--before-url", required=True)
    parser.add_argument("--after-url", required=True)
    parser.add_argument("--repeats", type=int, default=20)
    parser.add_argument("--asof", default="2026-09-17T00:00:00Z", help="UTC cutoff or 'current' (may freshness-sync)")
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    if args.repeats < 2:
        parser.error("repeats must be at least two")
    bases = {"before": args.before_url.rstrip("/"), "after": args.after_url.rstrip("/")}
    # New for each invocation, including reruns against the same serving workers.
    nonce = time.time_ns()
    records = []
    for iteration in range(args.repeats + 1):
        path = f"/api/portfolio/state?timeframe=1D&event_limit={nonce + iteration}"
        if args.asof != "current":
            path += f"&asof={args.asof}"
        reference = None
        for label in (["before", "after"] if iteration % 2 else ["after", "before"]):
            base = bases[label]
            for mode in ("cold", "warm"):
                before = native_calls(base)
                payload, elapsed = fetch(base + path)
                delta = native_calls(base) - before
                assert payload["ok"] and payload["count"] > 0 and not payload["errors"], payload
                assert delta > 0 if mode == "cold" else delta == 0, (label, mode, delta)
                digest = hashlib.sha256(json.dumps(payload, sort_keys=True).encode()).hexdigest()
                if reference is None:
                    reference = digest
                assert digest == reference, (iteration, label, mode, "response changed")
                record = {"iteration": iteration, "variant": label, "cache": mode,
                          "ms": round(elapsed, 3), "count": payload["count"],
                          "native_calls": delta, "sha256": digest}
                if iteration:
                    records.append(record)
                print(json.dumps(record), flush=True)
    summaries = {}
    for label in bases:
        for mode in ("cold", "warm"):
            samples = sorted(r["ms"] for r in records if r["variant"] == label and r["cache"] == mode)
            summaries[label + "_" + mode] = {
                "median_ms": round(statistics.median(samples), 3),
                "p95_ms": samples[math.ceil(len(samples) * .95) - 1],
            }
    result = {"urls": bases, "asof": args.asof, "repeats": args.repeats,
              "parity": "exact JSON SHA256 on every cold/warm pair", "summary": summaries, "records": records}
    if args.output:
        args.output.write_text(json.dumps(result, indent=2) + "\n")
    print(json.dumps({"summary": summaries, "parity": result["parity"]}), flush=True)


if __name__ == "__main__":
    main()
