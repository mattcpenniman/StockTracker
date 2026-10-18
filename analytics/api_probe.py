"""Live API read/validation timing (no forecast writes; latest may freshness-sync).

python -m analytics.api_probe --base-url http://192.168.1.80:5005 --repeats 5
"""
from __future__ import annotations

import argparse
import hashlib
import json
import statistics
import time
from urllib.error import HTTPError
from urllib.request import Request, urlopen


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--base-url", required=True)
    parser.add_argument("--repeats", type=int, default=5)
    parser.add_argument("--asof", default="2026-10-01T00:00:00Z")
    args = parser.parse_args()
    if args.repeats < 1:
        parser.error("repeats must be positive")
    cases = [
        ("/api/health/analytics", None),
        ("/api/stocks?limit=10", None),
        ("/api/metadata/NVDA", None),
        (f"/api/state/NVDA?asof={args.asof}", None),
        (f"/api/state/NVDA/1Day?asof={args.asof}&hidets=true", None),
        (f"/api/events/NVDA?asof={args.asof}", None),
        (f"/api/events/NVDA/15Min?asof={args.asof}", None),
        (f"/api/futurestate/NVDA?asof={args.asof}&days=5", None),
        (f"/api/futurestate/NVDA/1Day?asof={args.asof}&days=5&side=short&stoploss=0.02&takeprofit=0.04", None),
        ("/api/state/batch", {"symbols": ["NVDA", "AAPL", "MSFT"], "asof": args.asof}),
        (f"/api/portfolio/state?asof={args.asof}", None),
        (f"/api/portfolio/futurestate?asof={args.asof}&days=5", None),
        ("/api/chart/NVDA?timeframe=1Day&limit=180", None),
        ("/api/chart/NVDA/latest?timeframe=1Day", None),
        ("/api/state/NVDA?timeframe=invalid", None),
        ("/api/futurestate/NVDA", None),
        ("/api/coverage/prepare", {"symbols": [], "start": "invalid", "end": "invalid"}),
        ("/api/stocks", {}),
        ("/api/chart/NVDA/sync", {"timeframe": "invalid"}),
    ]
    for path, body in cases:
        samples, statuses, hashes = [], [], set()
        payload = {}
        for _ in range(args.repeats):
            req = Request(args.base_url.rstrip("/") + path,
                          data=json.dumps(body).encode() if body is not None else None,
                          headers={"Content-Type": "application/json"})
            start = time.perf_counter()
            try:
                response = urlopen(req, timeout=120)
            except HTTPError as exc:
                response = exc
            with response:
                raw = response.read()
                statuses.append(response.status)
            samples.append((time.perf_counter() - start) * 1000)
            payload = json.loads(raw)
            hashes.add(hashlib.sha256(json.dumps(payload, sort_keys=True).encode()).hexdigest())
        ordered = sorted(samples)
        print(json.dumps({"path": path, "method": "POST" if body is not None else "GET",
                          "first_ms": round(samples[0], 3),
                          "median_ms": round(statistics.median(samples), 3),
                          "p95_ms": round(ordered[min(len(ordered)-1, int(len(ordered)*.95))], 3),
                          "statuses": statuses, "distinct_payloads": len(hashes),
                          "count": payload.get("count"), "errors": payload.get("errors"),
                          "error": payload.get("error")}), flush=True)


if __name__ == "__main__":
    main()
