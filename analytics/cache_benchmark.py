"""VEC-808 isolated HTTP cache benchmark, including legacy cache instrumentation.

Serve this file with the target checkout first on PYTHONPATH (or normally with
python -m analytics.cache_benchmark). The benchmark-only control routes must
never be installed in the deployed app. See README for reproduction.
"""
from __future__ import annotations

import argparse
from concurrent.futures import ThreadPoolExecutor
import hashlib
import json
from pathlib import Path
import resource
from threading import Barrier
import time
from unittest.mock import patch
from urllib.request import Request, urlopen

from analytics.portfolio_load import request, run_stage, summarize


HOT = "2026-03-17,2026-06-16,2026-09-16"
SWEEP = "2026-02-17,2026-02-24,2026-03-17,2026-03-24,2026-04-15,2026-04-22,2026-05-15,2026-05-22,2026-06-16,2026-07-15,2026-08-18,2026-09-16"


def retained_size(value, seen=None):
    """Measure retained built-in graphs without tracing allocations during load."""
    import sys
    seen = set() if seen is None else seen
    if id(value) in seen:
        return 0
    seen.add(id(value))
    size = sys.getsizeof(value)
    if isinstance(value, dict):
        size += sum(retained_size(k, seen) + retained_size(v, seen) for k, v in value.items())
    elif isinstance(value, (tuple, list, set)):
        size += sum(retained_size(v, seen) for v in value)
    return size


def serve(port):
    import app
    from analytics.service import AnalyticsService, _StateCache
    from flask import jsonify, request as flask_request
    from werkzeug.serving import run_simple

    # Count legacy get/set operations, without introducing single-flight or
    # changing its eviction/copy behavior. Counters are diagnostic observations.
    if not hasattr(_StateCache, "diagnostics"):
        old_get, old_set = _StateCache.get, _StateCache.set

        def get(cache, key):
            value = old_get(cache, key)
            counts = cache.__dict__.setdefault("counts", dict(hits=0, misses=0, evictions=0))
            counts["hits" if value is not None else "misses"] += 1
            return value

        def set_value(cache, key, value):
            evict = key not in cache._data and len(cache._data) >= cache.maxsize
            old_set(cache, key, value)
            cache.counts["evictions"] += int(evict)

        _StateCache.get, _StateCache.set = get, set_value

    @app.app.route("/_benchmark/cache", methods=["GET", "DELETE"])
    def diagnostics():
        if flask_request.method == "DELETE":
            # Client resets only after all previous traffic has drained.
            app.analytics_service = AnalyticsService()
        caches = {}
        for name in ("state", "events", "future"):
            cache = getattr(app.analytics_service, f"_{name}_cache")
            stats = cache.diagnostics() if hasattr(cache, "diagnostics") else {
                **cache.__dict__.get("counts", dict(hits=0, misses=0, evictions=0)),
                "entries": len(cache._data), "max_entries": cache.maxsize,
                "coalesced_waits": 0, "in_flight": 0,
            }
            stats["measured_retained_bytes"] = retained_size(cache._data)
            caches[name] = stats
        rss = int(Path("/proc/self/statm").read_text().split()[1]) * __import__("os").sysconf("SC_PAGE_SIZE")
        return jsonify(caches=caches, rss_bytes=rss, peak_rss_bytes=resource.getrusage(resource.RUSAGE_SELF).ru_maxrss * 1024)

    def prohibited(*args, **kwargs):
        raise AssertionError("Provider I/O prohibited in cache benchmark")

    with patch.object(app, "_init_db_if_needed"), \
         patch.object(app, "_sync_market_data", prohibited), \
         patch.object(app, "_sync_market_data_window", prohibited), \
         patch.object(app, "_backfill_analytics_symbol_if_needed", prohibited):
        run_simple("0.0.0.0", port, app.app, threaded=True, use_reloader=False)


def control(base, reset=False):
    with urlopen(Request(base + "/_benchmark/cache", method="DELETE" if reset else "GET"), timeout=30) as response:
        return json.load(response)


def run(base, label, output):
    result = {"label": label, "stages": [], "bursts": []}
    for workload, dates, rates, duration, concurrency in (
        ("hot", HOT, (5, 25), 15, 32), ("sweep", SWEEP, (1, 5), 30, 16),
    ):
        control(base, True)
        cases = [(endpoint, date, 20) for date in dates.split(",") for endpoint in ("state", "futurestate")]
        warmup = [request(base, *case, 120) for case in cases]
        if not all(r["valid"] for r in warmup):
            raise RuntimeError(f"Invalid warmup: {warmup}")
        for rate in rates:
            before = control(base)
            stage = run_stage(base, cases, rate, duration, concurrency, 120)
            stage.update(workload=workload, before=before, after=control(base))
            result["stages"].append(stage)
            output.write_text(json.dumps(result, indent=2) + "\n")
            print(json.dumps({"label": label, "workload": workload, "rate": rate, "summary": stage["summary"],
                              "rps": stage["completed_in_window_rps"], "after": stage["after"]}), flush=True)
    for endpoint in ("state", "futurestate"):
        before = control(base, True)
        barrier = Barrier(16)

        def cold(_):
            barrier.wait(10)
            return request(base, endpoint, "2026-03-17", 20, 120)

        start = time.perf_counter()
        with ThreadPoolExecutor(16) as pool:
            records = list(pool.map(cold, range(16)))
        elapsed = time.perf_counter() - start
        burst = dict(endpoint=endpoint, clients=16, seconds=elapsed, throughput_rps=16 / elapsed,
                     dropped=0, summary=summarize(records), before=before, after=control(base), records=records)
        result["bursts"].append(burst)
        output.write_text(json.dumps(result, indent=2) + "\n")
        print(json.dumps({"label": label, **{k: v for k, v in burst.items() if k != "records"}}), flush=True)
    if any(s["summary"]["failed"] for s in result["stages"] + result["bursts"]):
        raise SystemExit("HTTP/application failures observed")


def parity(base, compare, output):
    """Compare entire decoded HTTP responses, including quality and symbol errors."""
    from urllib.parse import urlencode
    cases = []
    for date in SWEEP.split(","):
        for endpoint in ("state", "futurestate"):
            query = urlencode(dict(timeframe="1D", asof=date, days=20, stoploss=.05, takeprofit=.10))
            cases.append((f"/api/portfolio/{endpoint}?{query}", None))
    for side in ("long", "short"):
        for days in (1, 10, 20):
            cases.append(("/api/futurestate/NVDA?" + urlencode(dict(
                timeframe="1D", asof="2026-03-17", days=days, side=side, stoploss=.02, takeprofit=.03)), None))
    for settings in ({}, {"buffer_pct": .01, "volume_confirmation": False}):
        cases.append(("/api/state/batch", {"symbols": ["NVDA", "MISSING", "SPY", "NVDA"],
                                         "timeframe": "1D", "asof": "2026-03-17", **settings}))
    cases.append(("/api/events/NVDA?asof=2026-03-17&event_limit=5", None))
    cases.append(("/api/state/NVDA?asof=2026-03-17T00:00:00Z", None))
    records = []
    for path, body in cases:
        payloads = []
        for url in (base, compare):
            req = Request(url + path, data=json.dumps(body).encode() if body else None,
                          headers={"Content-Type": "application/json"})
            with urlopen(req, timeout=120) as response:
                payloads.append(json.load(response))
        if payloads[0] != payloads[1]:
            raise AssertionError(f"Full-JSON parity failed: {path} {body}")
        encoded = json.dumps(payloads[0], sort_keys=True).encode()
        records.append(dict(path=path, body=body, equal=True, sha256=hashlib.sha256(encoded).hexdigest()))
    output.write_text(json.dumps(dict(cases=len(records), full_json_equal=True, records=records), indent=2) + "\n")
    print(f"Full-JSON parity: {len(records)} HTTP cases equal")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("mode", choices=("serve", "run", "parity"))
    parser.add_argument("--port", type=int, default=5000)
    parser.add_argument("--base-url", default="http://127.0.0.1:5008")
    parser.add_argument("--label", default="default")
    parser.add_argument("--compare-url", default="http://127.0.0.1:5009")
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    if args.mode == "serve":
        serve(args.port)
    else:
        if args.output is None:
            parser.error("--output required")
        if args.mode == "parity":
            parity(args.base_url.rstrip("/"), args.compare_url.rstrip("/"), args.output)
        else:
            run(args.base_url.rstrip("/"), args.label, args.output)


if __name__ == "__main__":
    main()
