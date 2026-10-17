"""Compare /data to its pre-VEC-781 implementation, with no price network calls.

Requires ANALYTICS_TEST_DATABASE_URL and git history containing --baseline-ref.
Creates/drops only a unique private schema using production DDL and indexes.
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

import numpy as np
import psycopg2
from psycopg2.extensions import cursor

import app as dashboard
from .io_benchmark import price_fixture


def baseline_namespace(ref="1b62127"):
    """Load the actual old helpers/route; no duplicated reference formulas."""
    source = subprocess.check_output(["git", "show", f"{ref}:app.py"], text=True)
    names = {"data", "_trend_states", "_rsi_14_values", "_macd_signals",
             "_latest_bar_times", "_ma_distance_pcts"}
    tree = ast.parse(source)
    functions = [node for node in tree.body if isinstance(node, ast.FunctionDef) and node.name in names]
    if {node.name for node in functions} != names:
        raise ValueError("Baseline must contain the pre-VEC-781 dashboard helpers")
    for node in functions:
        node.decorator_list = []
    namespace = dict(vars(dashboard))
    exec(compile(ast.Module(body=functions, type_ignores=[]), "dashboard-baseline", "exec"), namespace)
    return namespace


@contextmanager
def dashboard_fixture(database_url, count=100, bars=5000, mixed=False):
    with price_fixture(database_url, daily_count=0, intraday_count=0) as (dsn, _):
        with psycopg2.connect(dsn) as conn, conn.cursor() as cur:
            for index in range(count):
                symbol = f"D{index:03d}"
                cur.execute("INSERT INTO symbol_metadata (symbol) VALUES (%s) RETURNING id", (symbol,))
                symbol_id = cur.fetchone()[0]
                size = ([0, 1, 14, 15, 19, 20, 21, 25, 26, 33, 34, 35, 49, 50,
                         199, 200, 201, 220, 249, 250, 251, 5000][index % 22] if mixed else bars)
                cur.execute("""
                    INSERT INTO stock_bars (symbol_id, timeframe, bar_time, open, high, low, close, volume)
                    SELECT %s, '1Day', '2000-01-01'::timestamptz + n * interval '1 day',
                           100, 110, 90, 100 + sin(n / 3.0) * 5 + n * 0.001, 1000
                    FROM generate_series(0, %s - 1) n
                """, (symbol_id, size))
                cur.execute("""
                    INSERT INTO stock_forecasts (symbol, forecast_price, updated_date, hidden)
                    VALUES (%s, 125, '2026-10-17', %s)
                """, (symbol, mixed and index % 3 == 0))
            # A saved forecast with no metadata must also retain neutral/null fields.
            if mixed:
                cur.execute("INSERT INTO stock_forecasts (symbol, forecast_price, updated_date) VALUES ('MISSING', 125, '2026-10-17')")
            cur.execute("ANALYZE stock_bars")
            cur.execute("ANALYZE symbol_metadata")
        yield dsn


@contextmanager
def variants(dsn, baseline):
    # Alternate supplied live prices and unavailable prices to exercise MA fallback.
    prices = lambda symbols: {symbol: (None if index % 2 else 107.5) for index, symbol in enumerate(symbols)}
    with patch.object(dashboard, "_get_conn", lambda: psycopg2.connect(dsn)), \
         patch.object(dashboard, "_fetch_current_prices", prices), \
         patch.dict(baseline, _get_conn=dashboard._get_conn, _fetch_current_prices=prices):
        yield [("before", baseline["data"]), ("after", dashboard.data)]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--baseline-ref", default="1b62127")
    parser.add_argument("--repeats", type=int, default=30)
    parser.add_argument("--bars", type=int, default=5000)
    args = parser.parse_args()
    database_url = os.environ.get("ANALYTICS_TEST_DATABASE_URL")
    if not database_url or min(args.repeats, args.bars) < 1:
        parser.error("Set ANALYTICS_TEST_DATABASE_URL and positive counts")
    baseline = baseline_namespace(args.baseline_ref)
    print(f"baseline={args.baseline_ref}; bars/symbol={args.bars}; repeats={args.repeats}")
    print("Real PostgreSQL + Flask /data, including schema-init SQL; warm DB/OS caches; price provider stubbed.")
    for size in (10, 50, 100):
        with dashboard_fixture(database_url, size, args.bars) as dsn, variants(dsn, baseline) as implementations:
            stats = {}
            captured = []
            transferred = 0

            class CountingCursor(cursor):
                def execute(self, sql, params=None):
                    captured.append((sql, params))
                    return super().execute(sql, params)

                def fetchall(self):
                    nonlocal transferred
                    rows = super().fetchall()
                    transferred += len(rows)
                    return rows

            def connect():
                return psycopg2.connect(dsn, cursor_factory=CountingCursor)

            with dashboard.app.test_client() as client, \
                 patch.object(dashboard, "_get_conn", connect), patch.dict(baseline, _get_conn=connect):
                samples = {name: [] for name, _ in implementations}
                for iteration in range(args.repeats + 1):
                    payloads = {}
                    for name, handler in implementations[::1 if iteration % 2 else -1]:
                        captured.clear()
                        transferred = 0
                        with patch.dict(dashboard.app.view_functions, data=handler):
                            start = time.perf_counter()
                            response = client.get("/data")
                            elapsed = (time.perf_counter() - start) * 1000
                        assert response.status_code == 200, response.get_json()
                        payloads[name] = response.get_json()
                        if iteration:
                            samples[name].append(elapsed)
                        stats[name] = {"sql": list(captured), "rows_transferred": transferred}
                    assert payloads["before"] == payloads["after"]
            print(f"\n{size} symbols: exact full-response parity")
            for name, timings in samples.items():
                selects = [(sql, params) for sql, params in stats[name]["sql"] if sql.lstrip().upper().startswith("SELECT")]
                print(f"{name}: median={statistics.median(timings):.2f}ms p95={np.percentile(timings, 95):.2f}ms "
                      f"SQL={len(stats[name]['sql'])} SELECTs={len(selects)} transferred={stats[name]['rows_transferred']}")
                scanned = 0
                with psycopg2.connect(dsn) as conn, conn.cursor() as cur:
                    for sql, params in selects:
                        if "stock_bars" not in sql:
                            continue
                        cur.execute("EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) " + sql, params)
                        plan = cur.fetchone()[0][0]

                        def scan_rows(node):
                            own = 0
                            if node.get("Relation Name") == "stock_bars":
                                own = (node.get("Actual Rows", 0) + node.get("Rows Removed by Filter", 0)) * node.get("Actual Loops", 1)
                            return own + sum(scan_rows(child) for child in node.get("Plans", []))

                        scanned += scan_rows(plan["Plan"])
                        print("EXPLAIN " + name + " " + json.dumps(plan, separators=(",", ":")))
                print(f"{name}: stock_bars scan-node rows (rows + filtered, times loops)={scanned}")


if __name__ == "__main__":
    main()
