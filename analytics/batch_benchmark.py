"""VEC-784 real PostgreSQL/Flask parity and cold/warm batch measurements.

ANALYTICS_TEST_DATABASE_URL=... python -m analytics.batch_benchmark --repeats 20
Creates/drops a private schema. Providers must never run; DB/OS caches are warm.
"""
from __future__ import annotations

import argparse
import ast
from contextlib import contextmanager
from datetime import timedelta
import os
import statistics
import subprocess
import time
from unittest.mock import patch

import numpy as np
import psycopg2

import app as dashboard
from .dashboard_benchmark import dashboard_fixture
from .data_access import AnalyticsRepository
from .io_benchmark import measured_repository
from .service import AnalyticsService


BASELINE = "7e9e230"


def baseline_implementations(ref=BASELINE):
    source = subprocess.check_output(["git", "show", f"{ref}:analytics/service.py"], text=True)
    namespace = {"__package__": "analytics"}
    exec(compile(source, "batch-service-baseline", "exec"), namespace)
    service = namespace["AnalyticsService"]
    source = subprocess.check_output(["git", "show", f"{ref}:app.py"], text=True)
    names = {"get_analytics_batch_state", "get_portfolio_analytics_state", "get_portfolio_analytics_future_state"}
    nodes = [node for node in ast.parse(source).body if isinstance(node, ast.FunctionDef) and node.name in names]
    assert {node.name for node in nodes} == names
    for node in nodes:
        node.decorator_list = []
    namespace = dict(vars(dashboard))
    exec(compile(ast.Module(body=nodes, type_ignores=[]), "batch-routes-baseline", "exec"), namespace)
    return service, namespace


@contextmanager
def batch_fixture(database_url, count=100, bars=5000, mixed=False):
    with dashboard_fixture(database_url, count, bars, mixed) as dsn:
        with psycopg2.connect(dsn) as conn, conn.cursor() as cur:
            cur.execute("INSERT INTO symbol_metadata (symbol) VALUES ('SPY'), ('QQQ')")
            cur.execute("""
                INSERT INTO stock_bars (symbol_id, timeframe, bar_time, open, high, low, close, volume)
                SELECT sm.id, sb.timeframe, sb.bar_time, sb.open, sb.high, sb.low, sb.close, sb.volume
                FROM symbol_metadata sm CROSS JOIN stock_bars sb
                WHERE sm.symbol IN ('SPY', 'QQQ') AND sb.symbol_id = (SELECT id FROM symbol_metadata WHERE symbol='D001')
            """)
            cur.execute("SELECT max(bar_time) FROM stock_bars")
            end = cur.fetchone()[0]
            cur.execute("""
                INSERT INTO stock_bars (symbol_id, timeframe, bar_time, open, high, low, close, volume)
                SELECT id, '15Min', %s::timestamptz + n * interval '15 minutes',
                       100, 103, 97, 100 + n * 0.1, 1000
                FROM symbol_metadata CROSS JOIN generate_series(0, 39) n
                WHERE symbol NOT IN ('BENCH', 'D000')
            """, (end - timedelta(days=2),))
            cur.execute("""
                INSERT INTO symbol_sync_state (symbol_id, last_synced_at, last_successful_sync_at, last_bar_synced_at)
                SELECT id, %s, %s, %s FROM symbol_metadata
                ON CONFLICT (symbol_id) DO UPDATE SET last_synced_at=EXCLUDED.last_synced_at,
                    last_successful_sync_at=EXCLUDED.last_successful_sync_at, last_bar_synced_at=EXCLUDED.last_bar_synced_at
            """, (end, end, end))
            cur.execute("ANALYZE stock_bars")
        yield dsn, end


@contextmanager
def route_variant(dsn, label, before, namespace, service=None):
    cls = before if label == "before" else AnalyticsService
    repo = measured_repository(AnalyticsRepository, dsn) if service is None else service.repository
    service = cls(repo) if service is None else service
    handlers = {name: namespace[name] if label == "before" else getattr(dashboard, name)
                for name in ("get_analytics_batch_state", "get_portfolio_analytics_state", "get_portfolio_analytics_future_state")}
    with patch.object(dashboard, "analytics_service", service), \
         patch.object(dashboard, "_get_conn", repo.get_conn), \
         patch.object(dashboard, "_init_db_if_needed"), \
         patch.object(dashboard, "_sync_market_data", side_effect=AssertionError("Unexpected provider sync")), \
         patch.object(dashboard, "_sync_market_data_window"), \
         patch.dict(namespace, analytics_service=service, _sync_market_data_window=dashboard._sync_market_data_window), \
         patch.dict(dashboard.app.view_functions, handlers):
        yield service, repo


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repeats", type=int, default=20)
    parser.add_argument("--bars", type=int, default=5000)
    args = parser.parse_args()
    database_url = os.environ.get("ANALYTICS_TEST_DATABASE_URL")
    if not database_url or min(args.repeats, args.bars) < 1:
        parser.error("Set ANALYTICS_TEST_DATABASE_URL and use positive counts")
    before, namespace = baseline_implementations()
    print(f"Baseline {BASELINE}; {args.bars} daily bars/symbol; {args.repeats} alternating pairs; real PostgreSQL + Flask", flush=True)
    print("Cold/warm service caches; warm DB/OS; schema-init excluded; provider calls prohibited (intraday window sync stubbed).", flush=True)
    for count in (10, 50, 100):
        with batch_fixture(database_url, count, args.bars) as (dsn, end), \
             dashboard.app.test_client() as client, patch.object(dashboard, "_utcnow", return_value=end + timedelta(hours=1)):
            symbols = [f"D{i:03d}" for i in range(count)]
            midnight = (end - timedelta(days=2)).isoformat()
            timed = (end - timedelta(days=2) + timedelta(hours=4)).isoformat()
            cases = [
                ("batch-state", "post", "/api/state/batch", {"symbols": symbols, "asof": midnight}),
                ("portfolio-current", "get", "/api/portfolio/state", None),
                ("portfolio-future", "get", f"/api/portfolio/futurestate?asof={midnight.replace('+00:00', 'Z')}&days=20", None),
                ("portfolio-intraday-future", "get", f"/api/portfolio/futurestate?asof={timed.replace('+00:00', 'Z')}&days=20&side=short&stoploss=0.02&takeprofit=0.02", None),
            ]
            for case, method, url, body in cases:
                samples = {(label, mode): [] for label in ("before", "after") for mode in ("cold", "warm")}
                counts = {}
                for iteration in range(args.repeats + 1):
                    payloads = {}
                    for label in ("before", "after")[::1 if iteration % 2 else -1]:
                        with route_variant(dsn, label, before, namespace) as (_, repo):
                            for mode in ("cold", "warm"):
                                repo.queries = repo.rows = 0
                                start = time.perf_counter()
                                response = getattr(client, method)(url, json=body)
                                elapsed = (time.perf_counter() - start) * 1000
                                assert response.status_code == 200, response.get_json()
                                payloads[label, mode] = response.get_json()
                                counts[label, mode] = (repo.queries, repo.rows)
                                if iteration:
                                    samples[label, mode].append(elapsed)
                    assert all(payload == payloads["before", "cold"] for payload in payloads.values()), case
                for (label, mode), timings in samples.items():
                    print(f"{count} {case} {label} {mode}: median={statistics.median(timings):.2f}ms p95={np.percentile(timings, 95):.2f}ms queries/rows={counts[label, mode]}", flush=True)


if __name__ == "__main__":
    main()
