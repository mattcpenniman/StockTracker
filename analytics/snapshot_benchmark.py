"""Read-only cold endpoint parity/timings for typed snapshot versus full frame.

Run with DATABASE_URL enforcing default_transaction_read_only=on. No DDL or
provider synchronization. Separate --variant processes isolate peak RSS.
"""
import argparse
from contextlib import contextmanager
import hashlib
import json
import os
from pathlib import Path
import statistics
import time
from unittest.mock import patch

import app
from . import indicators, service
from .data_access import AnalyticsRepository
from .future_benchmark import peak_rss_kib
from .state_bars import StateBars


class ReferenceService(service.AnalyticsService):
    """Pre-VEC-809 state path, using the unchanged full-frame oracle."""
    def _compute_symbol_state(self, context, timeframe, asof_dt, settings):
        bars = self.repository.fetch_bars(context["symbol_id"], "1Day", asof_dt)
        if bars.empty:
            raise service.AnalyticsNotFoundError(
                f"No bars available for symbol '{context['symbol']}' on timeframe '1Day'.")
        features = indicators.compute_feature_frame(bars, "1Day", settings)
        price_timeframe = self._preferred_price_timeframe(timeframe, asof_dt)
        anchor = None
        if price_timeframe != "1Day":
            prices = self.repository.fetch_latest_bar(context["symbol_id"], price_timeframe, asof_dt)
            if prices.empty:
                price_timeframe = "1Day"
            else:
                anchor = prices.iloc[-1]
        return self._state_from_features(context, timeframe, asof_dt, settings, features, price_timeframe, anchor)


def fingerprint(payload):
    return hashlib.sha256(json.dumps(payload, sort_keys=True, allow_nan=False).encode()).hexdigest()


def prohibited(*args, **kwargs):
    raise AssertionError("Provider I/O prohibited")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--variant", choices=["reference", "snapshot", "both"], default="both")
    parser.add_argument("--counts", default="1,10,37")
    parser.add_argument("--dates", default="2026-03-17,2026-06-16,2026-09-16")
    parser.add_argument("--repeats", type=int, default=3)
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--fixture-bars", type=int, help="Create a private synthetic schema on ANALYTICS_TEST_DATABASE_URL")
    parser.add_argument("--explain", action="store_true", help="Capture actual read plans in a separate pass")
    parser.add_argument("--serve", type=int, help="Serve the selected variant on an isolated local port")
    args = parser.parse_args()
    if args.repeats < 1 or any(int(c) < 1 for c in args.counts.split(",")):
        parser.error("Positive repeats and counts are required")
    if args.fixture_bars is not None:
        from .batch_benchmark import batch_fixture
        from . import data_access
        from database import get_connection
        from psycopg2.extensions import make_dsn, parse_dsn
        dsn = os.environ.get("ANALYTICS_TEST_DATABASE_URL")
        if not dsn or args.fixture_bars < 1:
            parser.error("Use a disposable ANALYTICS_TEST_DATABASE_URL and positive fixture bars")
        with batch_fixture(dsn, max(2, *map(int, args.counts.split(","))), args.fixture_bars) as (dsn, end):
            readonly = make_dsn(dsn, options=parse_dsn(dsn)["options"] + " -cdefault_transaction_read_only=on")
            args.dates = str(end.date())
            with patch.object(data_access, "DATABASE_URL", readonly), patch.object(app, "_get_conn", lambda: get_connection(readonly)):
                run(args)
    else:
        run(args)


def run(args):
    repo = AnalyticsRepository()
    with repo.get_conn() as conn, conn.cursor() as cur:
        cur.execute("SHOW default_transaction_read_only")
        assert cur.fetchone()[0] == "on", "A read-only DATABASE_URL is required"
    records, metrics = [], []
    names = ["reference", "snapshot"] if args.variant == "both" else [args.variant]
    classes = dict(reference=ReferenceService, snapshot=service.AnalyticsService)
    membership = app._portfolio_symbols
    plans = []
    with patch.object(app, "_init_db_if_needed"), \
         patch.object(app, "_sync_market_data", prohibited), \
         patch.object(app, "_sync_market_data_window", prohibited), \
         patch.object(app, "_backfill_analytics_symbol_if_needed", prohibited):
        if args.serve:
            from werkzeug.serving import run_simple
            if len(names) != 1:
                raise ValueError("Select --variant reference or snapshot when serving")
            app.analytics_service = classes[names[0]](repo)
            run_simple("127.0.0.1", args.serve, app.app, threaded=True, use_reloader=False)
            return
        symbols = app._portfolio_symbols()
        for count in map(int, args.counts.split(",")):
            selected = symbols[:count]
            assert len(selected) == count
            for date in args.dates.split(","):
                expected = None
                for repeat in range(args.repeats):
                    for name in names[::1 if repeat % 2 else -1]:
                        with patch.object(app, "analytics_service", classes[name](repo)), \
                             patch.object(app, "_portfolio_symbols", side_effect=lambda **kw: membership(**kw)[:count]), app.app.test_client() as client:
                            start = time.perf_counter()
                            response = client.get(f"/api/portfolio/state?asof={date}")
                            elapsed = (time.perf_counter() - start) * 1000
                            payload = response.get_json()
                        assert response.status_code == 200 and payload["count"] == count and not payload["errors"], payload
                        if expected is None:
                            expected = payload
                        assert payload == expected, (count, date, name, "full JSON mismatch")
                        records.append(dict(variant=name, count=count, date=date, ms=elapsed, sha256=fingerprint(payload)))
                # Separate attribution pass: no wrappers on unprofiled samples.
                for name in names:
                    totals = dict(queries=0, rows=0, execute_ms=0., fetch_ms=0., conversion_ms=0., boundary_ms=0., native_ms=0.)
                    def timed(fn, key):
                        def wrapped(*a, **kw):
                            start = time.perf_counter()
                            try:
                                return fn(*a, **kw)
                            finally:
                                totals[key] += (time.perf_counter() - start) * 1000
                        return wrapped
                    from psycopg2.extensions import cursor
                    class CountCursor(cursor):
                        def execute(self, *a, **kw):
                            totals["queries"] += 1
                            return timed(super().execute, "execute_ms")(*a, **kw)
                        def fetchall(self):
                            rows = timed(super().fetchall, "fetch_ms")()
                            totals["rows"] += len(rows)
                            return rows
                    class CountRepository(AnalyticsRepository):
                        @contextmanager
                        def get_conn(self):
                            with repo.get_conn() as conn:
                                class Connection:
                                    def cursor(self):
                                        return conn.cursor(cursor_factory=CountCursor)
                                yield Connection()
                    measured = CountRepository()
                    native = indicators._rust_feature_columns
                    with patch.object(app, "analytics_service", classes[name](measured)), \
                         patch.object(app, "_get_conn", measured.get_conn), \
                         patch.object(app, "_portfolio_symbols", side_effect=lambda **kw: membership(**kw)[:count]), \
                         patch.object(AnalyticsRepository, "_bar_frame", staticmethod(timed(AnalyticsRepository._bar_frame, "conversion_ms"))), \
                         patch.object(StateBars, "from_rows", timed(StateBars.from_rows, "conversion_ms")), \
                         patch.object(indicators, "_rust_feature_columns", timed(native, "native_ms") if native else None), \
                         patch.object(indicators, "compute_feature_frame", timed(indicators.compute_feature_frame, "boundary_ms")), \
                         patch.object(service, "compute_feature_snapshot", timed(service.compute_feature_snapshot, "boundary_ms")), \
                         app.app.test_client() as client:
                        payload = client.get(f"/api/portfolio/state?asof={date}").get_json()
                        assert payload == expected
                    metrics.append(dict(variant=name, count=count, date=date, **totals))
        if args.explain:
            from unittest.mock import MagicMock
            contexts = repo.fetch_symbol_contexts(symbols[:10])
            ids = [c["symbol_id"] for c in contexts.values()]
            cutoff = app.parse_asof(args.dates.split(",")[-1])
            with repo.get_conn() as conn, conn.cursor() as cur:
                for latest, tf in ((False, "1Day"), (True, "15Min")):
                    connection = MagicMock()
                    capture = connection.__enter__.return_value.cursor.return_value.__enter__.return_value
                    capture.fetchall.return_value = []
                    with patch.object(repo, "get_conn", return_value=connection):
                        repo.fetch_state_bars_many(ids, tf, cutoff, latest=latest)
                    sql, params = capture.execute.call_args.args
                    cur.execute("EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) " + sql, params)
                    plans.append(dict(timeframe=tf, sql=sql, plan=cur.fetchone()[0]))
    summary = [dict(variant=n, count=c, median_ms=statistics.median(r["ms"] for r in records if r["variant"] == n and r["count"] == c))
               for n in names for c in map(int, args.counts.split(","))]
    result = dict(records=records, summary=summary, instrumentation=metrics, plans=plans,
                  backend=indicators.backend_diagnostics(), fixture_bars=args.fixture_bars,
                  peak_rss_kib=peak_rss_kib(), full_json_parity=True)
    args.output.write_text(json.dumps(result, indent=2) + "\n")
    print(json.dumps({k: v for k, v in result.items() if k != "records"}, indent=2))


if __name__ == "__main__":
    main()
