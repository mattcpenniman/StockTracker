"""Compare VEC-805 with pinned baseline using real PostgreSQL and exact JSON parity.

Set ANALYTICS_TEST_DATABASE_URL; creates/drops a private schema, never uses provider I/O.
python -m analytics.api_performance_benchmark --repeats 10
"""
from __future__ import annotations

import argparse
import ast
from contextlib import contextmanager
import json
import os
import statistics
import subprocess
import time
from unittest.mock import patch

import psycopg2
from psycopg2.extensions import cursor

import app
from analytics.data_access import AnalyticsRepository
from analytics.io_benchmark import price_fixture
from analytics.service import AnalyticsService
from database import get_connection, close_pools

BASELINE = "1c65793"


def baseline_function(name):
    source = subprocess.check_output(["git", "show", f"{BASELINE}:app.py"], text=True)
    node = next(n for n in ast.parse(source).body if isinstance(n, ast.FunctionDef) and n.name == name)
    node.decorator_list = []
    namespace = {}
    exec(compile(ast.Module(body=[node], type_ignores=[]), "baseline-app", "exec"), app.__dict__, namespace)
    return namespace[name]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repeats", type=int, default=10)
    args = parser.parse_args()
    if args.repeats < 1 or not os.environ.get("ANALYTICS_TEST_DATABASE_URL"):
        parser.error("Use positive repeats and set ANALYTICS_TEST_DATABASE_URL")
    namespace = {"__package__": "analytics"}
    source = subprocess.check_output(["git", "show", f"{BASELINE}:analytics/service.py"], text=True)
    exec(compile(source, "baseline-service", "exec"), namespace)
    old_service = namespace["AnalyticsService"]
    old_init = baseline_function("_init_db_if_needed")
    old_events = baseline_function("get_analytics_events")
    init, events = app._init_db_if_needed, app.get_analytics_events
    with price_fixture(os.environ["ANALYTICS_TEST_DATABASE_URL"], 8000, 20000) as (dsn, symbol_id):
        with psycopg2.connect(dsn) as conn, conn.cursor() as cur:
            cur.execute("INSERT INTO stock_forecasts (symbol, forecast_price, updated_date) VALUES ('BENCH', 110, '2020-06-01')")
        query_count = 0

        class CountingCursor(cursor):
            def execute(self, *args, **kwargs):
                nonlocal query_count
                query_count += 1
                return super().execute(*args, **kwargs)

        @contextmanager
        def connection():
            with get_connection(dsn) as conn:
                previous = conn.cursor_factory
                conn.cursor_factory = CountingCursor
                try:
                    yield conn
                finally:
                    conn.cursor_factory = previous

        cases = [
            ("/api/stocks", False),
            ("/api/chart/BENCH?limit=180", False),
            ("/api/chart/BENCH/latest", False),
            ("/api/events/BENCH?asof=2020-06-01T00:00:00Z", True),
            ("/api/events/BENCH/15Min?asof=2020-06-01T00:00:00Z", True),
            ("/api/portfolio/state?asof=2020-06-01T00:00:00Z", False),
            ("/api/portfolio/futurestate?asof=2020-06-01T00:00:00Z&days=5", False),
        ]
        with patch.object(app, "_get_conn", connection), \
             patch.object(app, "CHART_LATEST_AUTO_SYNC_MAX_AGE_DAYS", -1), \
             patch.object(app, "_sync_market_data", side_effect=AssertionError("provider I/O prohibited")), \
             patch.object(app, "_sync_market_data_window", side_effect=AssertionError("provider I/O prohibited")), \
             patch.object(app, "_backfill_analytics_symbol_if_needed", side_effect=AssertionError("backfill prohibited")), \
             app.app.test_client() as client:
            for path, cold_events in cases:
                timings = {key: [] for key in ("before", "after")}
                queries = {key: [] for key in timings}
                for iteration in range(args.repeats + 1):
                    payloads = {}
                    for label in (["before", "after"] if iteration % 2 else ["after", "before"]):
                        repo = AnalyticsRepository(dsn)
                        repo.get_conn = connection
                        service = (old_service if label == "before" else AnalyticsService)(repo)
                        with patch.object(app, "analytics_service", service), \
                             patch.object(app, "_init_db_if_needed", old_init if label == "before" else init), \
                             patch.dict(app.app.view_functions, get_analytics_events=old_events if label == "before" else events):
                            if not cold_events:
                                warm = client.get(path)
                                assert warm.status_code == 200, warm.get_json()
                            query_count = 0
                            start = time.perf_counter()
                            response = client.get(path)
                            elapsed = (time.perf_counter() - start) * 1000
                            assert response.status_code == 200, response.get_json()
                            payloads[label] = response.get_json()
                            if iteration:
                                timings[label].append(elapsed)
                                queries[label].append(query_count)
                    assert payloads["before"] == payloads["after"], path
                print(json.dumps({"path": path, "cache": "cold" if cold_events else "warm",
                                  "parity": "exact JSON", "baseline": BASELINE,
                                  **{label: {"median_ms": round(statistics.median(values), 3),
                                             "p95_ms": round(sorted(values)[min(len(values)-1, int(len(values)*.95))], 3),
                                             "queries": statistics.median(queries[label])}
                                     for label, values in timings.items()}}), flush=True)
    close_pools()


if __name__ == "__main__":
    main()
