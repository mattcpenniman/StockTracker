"""Read-only stored-portfolio cold historical sweep benchmark, no DDL or sync.

Run each variant/date-count in a fresh process for independent peak RSS. The
fingerprints cover complete per-date state and label JSON, including errors and
quality. Compare fingerprints across variants, not only counts/numeric columns.
"""
import argparse
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
import hashlib
import json
from pathlib import Path
import statistics
from threading import Event
import time
from unittest.mock import patch

import pandas as pd
from psycopg2.extensions import cursor

import app
from database import get_connection
from .config import AnalyticsSettings
from .data_access import AnalyticsRepository
from .future_benchmark import peak_rss_kib, percentile
from .indicators import backend_diagnostics
from .service import AnalyticsService
from .utils import parse_asof


class MeasuredRepository(AnalyticsRepository):
    def __init__(self):
        super().__init__()
        self.queries = self.rows = 0

    @contextmanager
    def get_conn(self):
        owner = self

        class CountingCursor(cursor):
            def execute(self, *a, **kw):
                owner.queries += 1
                return super().execute(*a, **kw)

            def fetchall(self):
                rows = super().fetchall()
                owner.rows += len(rows)
                return rows

        with get_connection(self.database_url) as conn:
            class Connection:
                def cursor(self):
                    return conn.cursor(cursor_factory=CountingCursor)
            yield Connection()


def fingerprints(rows):
    return [hashlib.sha256(json.dumps(row, sort_keys=True, separators=(",", ":"), allow_nan=False).encode()).hexdigest()
            for row in rows]


def run(variant, symbols, dates):
    repo = MeasuredRepository()
    service = AnalyticsService(repo)
    before = backend_diagnostics()["execution"]
    start = time.perf_counter()
    if variant == "bulk":
        result = service.get_historical_portfolio(symbols, dates, AnalyticsSettings(), 20, .05, .10)
        rows = [{"features": row["features"], "labels": row["labels"]} for row in result["dates"]]
        execution = result["execution"]
    else:
        rows = []
        for date in dates:
            cutoff = parse_asof(date)
            rows.append(dict(features=service.get_batch_state(symbols, "1Day", cutoff, AnalyticsSettings()),
                             labels=service.get_batch_future_state(symbols, "1Day", cutoff, 20, .05, .10)))
        execution = None
    serialized = json.dumps(rows, separators=(",", ":"), allow_nan=False).encode()
    duration = time.perf_counter() - start
    after = backend_diagnostics()["execution"]
    feature_computations = (after["native_calls"] + after["python_fallback_calls"]
                            - before["native_calls"] - before["python_fallback_calls"])
    checks = []
    for row in rows:
        assert not row["features"]["errors"] and not row["labels"]["errors"], row
        assert len(row["features"]["states"]) == len(symbols)
        assert len(row["labels"]["future_states"]) == len(symbols)
        checks.append(dict(stale_features=row["features"]["data_quality"]["stale_symbols"],
                           stale_labels=row["labels"]["data_quality"]["stale_symbols"],
                           partial_labels=row["labels"]["data_quality"]["incomplete_window_symbols"],
                           benchmark_errors=row["features"]["benchmark_errors"]))
    return dict(duration_seconds=duration, samples_per_second=len(symbols) * len(dates) / duration,
                queries=repo.queries, rows=repo.rows, execution=execution,
                feature_computations=feature_computations,
                result_bytes=len(serialized), fingerprints=fingerprints(rows), quality=checks)


def impact(variant, symbols, dates, rate, jobs):
    interactive = AnalyticsService(AnalyticsRepository())
    cutoff = parse_asof("2026-09-16")
    expected = interactive.get_batch_state(symbols, "1Day", cutoff, AnalyticsSettings())
    assert not expected["errors"]

    def probe(seconds=None, done=None, started=None):
        records = []
        start = time.perf_counter()
        if started:
            started.set()
        index = 0
        while (time.perf_counter() - start < seconds if seconds is not None else not done.is_set()):
            delay = start + index / rate - time.perf_counter()
            if delay > 0:
                time.sleep(delay)
            t = time.perf_counter()
            payload = interactive.get_batch_state(symbols, "1Day", cutoff, AnalyticsSettings())
            # Include serialization to match a served interactive response.
            json.dumps(payload, separators=(",", ":"))
            records.append((time.perf_counter() - t) * 1000)
            assert payload == expected
            index += 1
        return records

    done, started = Event(), Event()
    with ThreadPoolExecutor(max_workers=1) as executor:
        future = executor.submit(probe, None, done, started)
        started.wait()
        start = time.perf_counter()
        try:
            results = [run(variant, symbols, dates) for _ in range(jobs)]
        finally:
            duration = time.perf_counter() - start
            done.set()
        loaded = future.result()
    idle = probe(seconds=duration)
    def summary(values):
        return dict(count=len(values), p50_ms=statistics.median(values), p95_ms=percentile(values, .95), max_ms=max(values))
    return dict(offered_interactive_rate=rate, duration_seconds=duration, jobs=jobs,
                loaded=summary(loaded), idle=summary(idle),
                concurrent_job_durations=[r["duration_seconds"] for r in results])


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--variant", choices=["sequential", "bulk"])
    parser.add_argument("--date-count", type=int, choices=[12, 60])
    parser.add_argument("--repeats", type=int, default=3)
    parser.add_argument("--impact-jobs", type=int, default=0)
    parser.add_argument("--probe-api", action="store_true", help="Also verify the JSON HTTP entrypoint against service results")
    parser.add_argument("--output", type=Path)
    parser.add_argument("--compare-files", type=Path, nargs="+", help="Compare saved complete-result fingerprints; no database I/O")
    args = parser.parse_args()
    if args.compare_files:
        outputs = [json.loads(path.read_text()) for path in args.compare_files]
        for count in (12, 60):
            group = [o for o in outputs if len(o["dates"]) == count]
            assert len(group) >= 2 and {o["variant"] for o in group} == {"sequential", "bulk"}
            first = group[0]
            for output in group:
                assert output["symbols"] == first["symbols"] and output["dates"] == first["dates"]
                assert all(r["fingerprints"] == first["records"][0]["fingerprints"] for r in output["records"]), "Full result mismatch"
            print(json.dumps(dict(date_count=count, full_result_parity=True,
                                 symbols=len(first["symbols"]), samples=count * len(first["symbols"]))))
        return
    if args.variant is None or args.date_count is None or args.output is None:
        parser.error("Specify --variant, --date-count and --output, or --compare-files")
    if args.repeats < 1:
        parser.error("repeats must be positive")
    dates = [d.strftime("%Y-%m-%d") for d in pd.date_range("2026-02-17", "2026-09-16", periods=args.date_count)]
    def prohibited(*a, **kw):
        raise AssertionError("Provider I/O prohibited")
    with patch.object(app, "_init_db_if_needed"), patch.object(app, "_sync_market_data", prohibited), \
         patch.object(app, "_sync_market_data_window", prohibited), \
         patch.object(app, "_backfill_analytics_symbol_if_needed", prohibited):
        with app.analytics_service.repository.get_conn() as conn, conn.cursor() as cur:
            cur.execute("SHOW default_transaction_read_only")
            assert cur.fetchone()[0] == "on", "Benchmark requires a read-only PostgreSQL DSN"
        symbols = app._portfolio_symbols()
        assert symbols
        records = [run(args.variant, symbols, dates) for _ in range(args.repeats)]
        assert all(r["fingerprints"] == records[0]["fingerprints"] for r in records)
        output = dict(variant=args.variant, dates=dates, symbols=symbols, records=records,
                      peak_rss_kib=peak_rss_kib())
        if args.probe_api:
            with patch.object(app, "analytics_service", AnalyticsService(AnalyticsRepository())), app.app.test_client() as client:
                started = time.perf_counter()
                response = client.post("/api/portfolio/history", json=dict(cutoffs=dates, days=20,
                    stop_loss_pct=.05, take_profit_pct=.10))
                payload = response.get_json()
                duration = time.perf_counter() - started
                assert response.status_code == 200 and payload["symbols"] == symbols, payload
                assert fingerprints([dict(features=r["features"], labels=r["labels"]) for r in payload["dates"]]) == records[0]["fingerprints"]
                output["api_probe"] = dict(status=response.status_code, full_result_parity=True,
                    duration_seconds=duration, wire_bytes=len(response.data), peak_rss_kib=peak_rss_kib())
        if args.impact_jobs:
            output["interactive_impact"] = impact(args.variant, symbols, dates, 5, args.impact_jobs)
        args.output.write_text(json.dumps(output, indent=2) + "\n")
        print(json.dumps({**{k: v for k, v in output.items() if k != "records"},
                          "records": [{k: v for k, v in r.items() if k not in ("fingerprints", "quality")} for r in records]}, indent=2))


if __name__ == "__main__":
    main()
