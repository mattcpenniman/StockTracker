"""Read-only, alternating full-JSON future-label comparison on stored portfolios.

DATABASE_URL must point to PostgreSQL. No DDL/provider sync. Run each variant in
a fresh process with --rss-variant to compare process peak RSS independently.
The native implementation is experimental and never selected in production.
"""
from contextlib import contextmanager
import argparse
import json
import math
from pathlib import Path
import resource
import statistics
import time
from unittest.mock import patch

import app
from . import future_labels
from .data_access import AnalyticsRepository
from .service import AnalyticsService


class ReferenceService(AnalyticsService):
    _compute_future_state = AnalyticsService._compute_future_state_reference


def native_scan(windows, anchors, stop, take, side):
    from apollo_indicators import future_window_indices
    # Conversion, allocation, FFI and return conversion are all timed by caller.
    return future_window_indices([[(b.high, b.low) for b in bars] for bars in windows],
                                 anchors, stop, take, side == "short")


@contextmanager
def variant(name):
    with patch.object(app, "analytics_service", (ReferenceService if name == "reference" else AnalyticsService)(AnalyticsRepository())):
        if name == "rust":
            with patch.object(future_labels, "scan_windows", native_scan):
                yield
        else:
            yield


def percentile(values, p):
    return sorted(values)[max(0, math.ceil(len(values) * p) - 1)]


def peak_rss_kib():
    # Linux getrusage can retain a larger pre-exec parent high-water mark.
    status = Path("/proc/self/status")
    if status.exists():
        return int(next(line.split()[1] for line in status.read_text().splitlines() if line.startswith("VmHWM:")))
    return resource.getrusage(resource.RUSAGE_SELF).ru_maxrss


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dates", default="2026-03-17,2026-06-16,2026-09-16")
    parser.add_argument("--horizons", default="1,20,252")
    parser.add_argument("--repeats", type=int, default=10)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--rss-variant", choices=["reference", "python", "rust"])
    parser.add_argument("--serve", type=int, help="Serve one variant on this isolated port")
    args = parser.parse_args()
    if args.repeats < 1:
        parser.error("--repeats must be positive")
    names = [args.rss_variant] if args.rss_variant else ["reference", "python", "rust"]
    records = []
    def prohibited(*a, **kw):
        raise AssertionError("Provider I/O prohibited")
    with patch.object(app, "_init_db_if_needed"), \
         patch.object(app, "_sync_market_data", prohibited), \
         patch.object(app, "_sync_market_data_window", prohibited), \
         patch.object(app, "_backfill_analytics_symbol_if_needed", prohibited):
        if args.serve:
            from werkzeug.serving import run_simple
            with variant(args.rss_variant or "python"):
                run_simple("127.0.0.1", args.serve, app.app, threaded=True, use_reloader=False)
            return
        with app.app.test_client() as client:
            for date in args.dates.split(","):
                for horizon in map(int, args.horizons.split(",")):
                    path = f"/api/portfolio/futurestate?asof={date}&days={horizon}&stoploss=0.05&takeprofit=0.10"
                    for repeat in range(args.repeats + 1):
                        payloads = {}
                        for name in names[::1 if repeat % 2 else -1]:
                            with variant(name):
                                for cache in ("cold", "warm"):
                                    start = time.perf_counter()
                                    response = client.get(path)
                                    elapsed = (time.perf_counter() - start) * 1000
                                    payload = response.get_json()
                                    assert response.status_code == 200 and payload["count"] == payload["portfolio_symbol_count"] and not payload["errors"], payload
                                    assert payload["count"] > 0
                                    if cache == "cold":
                                        payloads[name] = payload
                                    else:
                                        assert payload == payloads[name]
                                    if repeat:
                                        records.append(dict(variant=name, date=date, horizon=horizon, cache=cache,
                                                            ms=elapsed, count=payload["count"]))
                        assert all(p == next(iter(payloads.values())) for p in payloads.values()), "Full JSON mismatch"
        # Separate instrumentation pass: never contaminate wall-clock samples.
        metrics = []
        for name in names:
            for horizon in map(int, args.horizons.split(",")):
                totals = dict(conversion_ms=0.0, scan_ms=0.0, queries=0, rows=0)
                frame = AnalyticsRepository._bar_frame
                typed = AnalyticsRepository._label_records
                scan = native_scan if name == "rust" else future_labels.scan_windows
                def timed(fn, key):
                    def wrapped(*a, **kw):
                        start = time.perf_counter()
                        result = fn(*a, **kw)
                        totals[key] += (time.perf_counter() - start) * 1000
                        return result
                    return wrapped
                from psycopg2.extensions import cursor
                from database import get_connection
                class CountCursor(cursor):
                    def execute(self, *a, **kw):
                        totals["queries"] += 1
                        return super().execute(*a, **kw)
                    def fetchall(self):
                        rows = super().fetchall()
                        totals["rows"] += len(rows)
                        return rows
                class CountRepository(AnalyticsRepository):
                    @contextmanager
                    def get_conn(self):
                        with get_connection(self.database_url) as conn:
                            class Connection:
                                def cursor(self):
                                    return conn.cursor(cursor_factory=CountCursor)
                            yield Connection()
                measured = CountRepository()
                with variant(name), patch.object(app.analytics_service, "repository", measured), \
                     patch.object(app, "_get_conn", measured.get_conn), \
                     patch.object(AnalyticsRepository, "_bar_frame", staticmethod(timed(frame, "conversion_ms"))), \
                     patch.object(AnalyticsRepository, "_label_records", staticmethod(timed(typed, "conversion_ms"))), \
                     patch.object(future_labels, "scan_windows", timed(scan, "scan_ms")), app.app.test_client() as client:
                    for cache in ("cold", "warm"):
                        for key in totals:
                            totals[key] = 0
                        response = client.get(f"/api/portfolio/futurestate?asof={args.dates.split(',')[0]}&days={horizon}&stoploss=0.05&takeprofit=0.10")
                        assert response.status_code == 200
                        metrics.append(dict(variant=name, horizon=horizon, cache=cache, **totals))
    summary = []
    for name in names:
        for horizon in map(int, args.horizons.split(",")):
            for cache in ("cold", "warm"):
                values = [r["ms"] for r in records if (r["variant"], r["horizon"], r["cache"]) == (name, horizon, cache)]
                summary.append(dict(variant=name, horizon=horizon, cache=cache, n=len(values),
                                    p50_ms=statistics.median(values), p95_ms=percentile(values, .95)))
    output = dict(records=records, summary=summary, instrumentation=metrics,
                  peak_rss_kib=peak_rss_kib(),
                  full_json_parity=True, profiled=False)
    args.output.write_text(json.dumps(output, indent=2) + "\n")
    print(json.dumps({k: v for k, v in output.items() if k != "records"}, indent=2))


if __name__ == "__main__":
    main()
