"""Open-loop HTTP load generator for historical portfolio analytics (stdlib only).

python -m analytics.portfolio_load --base-url http://127.0.0.1:5008 \
    --dates 2024-03-15,2025-03-14,2026-03-13 --rates 1,5,20,50 \
    --duration 20 --concurrency 32 --output /tmp/portfolio-load.json
"""
from __future__ import annotations

import argparse
from collections import Counter
from concurrent.futures import ThreadPoolExecutor, wait, FIRST_COMPLETED
from datetime import date
import json
import math
from pathlib import Path
import platform
import time
from urllib.error import HTTPError
from urllib.parse import urlencode
from urllib.request import urlopen


def percentile(values, fraction):
    values = sorted(values)
    return values[max(0, math.ceil(len(values) * fraction) - 1)] if values else None


def request(base, endpoint, asof, days, timeout):
    query = {"timeframe": "1D", "asof": asof}
    if endpoint == "futurestate":
        query.update(days=days, stoploss=0.05, takeprofit=0.10)
    start = time.perf_counter()
    record = {"endpoint": endpoint, "date": asof, "status": None, "valid": False}
    try:
        with urlopen(base + "/api/portfolio/" + endpoint + "?" + urlencode(query), timeout=timeout) as response:
            body = response.read()
            record.update(status=response.status, bytes=len(body))
        payload = json.loads(body)
        key = "states" if endpoint == "state" else "future_states"
        count = payload.get("count", 0)
        record.update(count=count, symbol_errors=len(payload.get("errors", [])),
                      benchmark_errors=len(payload.get("benchmark_errors", [])),
                      stale_symbols=len(payload.get("data_quality", {}).get("stale_symbols", [])),
                      incomplete_windows=len(payload.get("data_quality", {}).get("incomplete_window_symbols", [])))
        record["valid"] = bool(record["status"] == 200 and payload.get("ok") and count > 0
                               and count == payload.get("portfolio_symbol_count")
                               and len(payload.get(key, [])) == count
                               and not record["symbol_errors"])
        if not record["valid"]:
            record["error"] = str(payload.get("error") or payload.get("errors") or "incomplete portfolio response")[:300]
    except HTTPError as exc:
        record.update(status=exc.code, error=str(exc))
    except Exception as exc:
        record["error"] = f"{type(exc).__name__}: {exc}"
    record["latency_ms"] = (time.perf_counter() - start) * 1000
    return record


def summarize(records):
    latencies = [r["latency_ms"] for r in records]
    return {"requests": len(records), "valid": sum(r["valid"] for r in records),
            "failed": sum(not r["valid"] for r in records),
            "status_counts": dict(Counter(str(r["status"]) for r in records)),
            "p50_ms": percentile(latencies, .5), "p95_ms": percentile(latencies, .95),
            "p99_ms": percentile(latencies, .99), "max_ms": max(latencies, default=None),
            "bytes": sum(r.get("bytes", 0) for r in records),
            "benchmark_error_observations": sum(r.get("benchmark_errors", 0) for r in records),
            "stale_symbol_observations": sum(r.get("stale_symbols", 0) for r in records),
            "incomplete_window_observations": sum(r.get("incomplete_windows", 0) for r in records)}


def run_stage(base, cases, rate, duration, concurrency, timeout):
    """No unbounded executor queue: shed arrivals when all client slots are busy.

    Arrivals have a fixed clock independent of response times. Scheduler-delayed
    arrivals are dropped rather than sent in a catch-up burst. Completion latency
    and dropped offered traffic are both reported (avoids coordinated omission).
    """
    records, pending = [], set()
    dropped_capacity = dropped_scheduler = 0
    arrivals = math.ceil(rate * duration)
    start = time.perf_counter()
    with ThreadPoolExecutor(max_workers=concurrency) as executor:
        for index in range(arrivals):
            target = start + index / rate
            time.sleep(max(0, target - time.perf_counter()))
            done = {future for future in pending if future.done()}
            pending -= done
            records.extend(future.result() for future in done)
            lag = time.perf_counter() - target
            if lag >= 1 / rate:
                dropped_scheduler += 1
            elif len(pending) >= concurrency:
                dropped_capacity += 1
            else:
                endpoint, asof, days = cases[index % len(cases)]
                pending.add(executor.submit(request, base, endpoint, asof, days, timeout))
        offer_end = start + duration
        time.sleep(max(0, offer_end - time.perf_counter()))
        completed_in_window = len(records) + sum(future.done() for future in pending)
        while pending:
            done, pending = wait(pending, return_when=FIRST_COMPLETED)
            records.extend(future.result() for future in done)
    elapsed = time.perf_counter() - start
    groups = {endpoint: summarize([r for r in records if r["endpoint"] == endpoint])
              for endpoint in sorted({case[0] for case in cases})}
    return {"offered_rps": rate, "offer_seconds": duration, "total_seconds_with_drain": elapsed,
            "offered_requests": arrivals, "dropped_capacity": dropped_capacity,
            "dropped_scheduler": dropped_scheduler, "completed_in_window": completed_in_window,
            "completed_in_window_rps": completed_in_window / duration,
            "throughput_with_drain_rps": len(records) / elapsed,
            "summary": summarize(records), "by_endpoint": groups,
            "by_date": {d: summarize([r for r in records if r["date"] == d])
                        for d in sorted({case[1] for case in cases})}, "records": records}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--base-url", required=True)
    parser.add_argument("--dates", required=True, help="Comma-separated historical YYYY-MM-DD cutoffs")
    parser.add_argument("--rates", default="1,5,20,50")
    parser.add_argument("--duration", type=float, default=20)
    parser.add_argument("--concurrency", type=int, default=32)
    parser.add_argument("--timeout", type=float, default=120)
    parser.add_argument("--days", type=int, default=20)
    parser.add_argument("--endpoint", choices=["state", "futurestate", "both"], default="both")
    parser.add_argument("--warmup", action="store_true", help="Request every case once before measurement")
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    try:
        dates = list(dict.fromkeys(date.fromisoformat(d).isoformat() for d in args.dates.split(",")))
        rates = [float(r) for r in args.rates.split(",")]
        if not rates or any(not math.isfinite(r) or r <= 0 for r in rates):
            raise ValueError("rates must be positive finite numbers")
        if not all(math.isfinite(v) and v > 0 for v in (args.duration, args.concurrency, args.timeout, args.days)):
            raise ValueError("duration, concurrency, timeout and days must be positive")
    except ValueError as exc:
        parser.error(str(exc))
    endpoints = ["state", "futurestate"] if args.endpoint == "both" else [args.endpoint]
    cases = [(e, d, args.days) for d in dates for e in endpoints]
    base = args.base_url.rstrip("/")
    result = {"configuration": {**vars(args), "output": str(args.output)},
              "client_python": platform.python_version(), "started_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
              "stages": []}
    if args.warmup:
        result["warmup"] = [request(base, *case, args.timeout) for case in cases]
        if not all(r["valid"] for r in result["warmup"]):
            args.output.write_text(json.dumps(result, indent=2) + "\n")
            raise SystemExit("Warmup failed validation; see output")
    for rate in rates:
        stage = run_stage(base, cases, rate, args.duration, args.concurrency, args.timeout)
        result["stages"].append(stage)
        args.output.write_text(json.dumps(result, indent=2) + "\n")
        print(json.dumps({k: v for k, v in stage.items() if k not in {"records", "by_date"}}), flush=True)
    if any(stage["summary"]["failed"] for stage in result["stages"]):
        raise SystemExit("HTTP/application failures observed; see output")


if __name__ == "__main__":
    main()
