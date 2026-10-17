"""VEC-801: bounded, local PostgreSQL/Flask future-window measurements.

ANALYTICS_TEST_DATABASE_URL=... python -m analytics.future_sync_benchmark
Creates/drops a private schema. No live providers; controlled 50ms per fetch.
"""
from __future__ import annotations

import argparse
import ast
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from datetime import timedelta
import json
import os
import statistics
import subprocess
from threading import Barrier, Lock, local
import time
from unittest.mock import patch

import psycopg2

import app
from .batch_benchmark import batch_fixture
from .data_access import AnalyticsRepository
from .service import AnalyticsService, _StateCache

BASELINE = "0e1a054"


def baseline_routes():
    source = subprocess.check_output(["git", "show", f"{BASELINE}:app.py"], text=True)
    names = {"get_analytics_future_state", "get_portfolio_analytics_future_state"}
    nodes = [n for n in ast.parse(source).body if isinstance(n, ast.FunctionDef) and n.name in names]
    assert {n.name for n in nodes} == names
    for node in nodes:
        node.decorator_list = []
    namespace = dict(vars(app))
    exec(compile(ast.Module(body=nodes, type_ignores=[]), "future-baseline", "exec"), namespace)
    return namespace


@contextmanager
def variant(dsn, end, symbols, label, latency_ms, baseline):
    repo = AnalyticsRepository(dsn)
    service = AnalyticsService(repo)
    metrics = {"provider_calls": 0, "provider_ms": 0, "preparation_ms": 0, "analytics_ms": 0}
    lock = Lock()
    phases = local()
    session = end - timedelta(days=2)
    bars = [{"t": (session + timedelta(minutes=15 * n)).isoformat(),
             "o": 100, "h": 103, "l": 97, "c": 100 + n * .1, "v": 1000}
            for n in range(40)]

    def timed(key, fn):
        def wrapped(*args, **kwargs):
            nested = getattr(phases, key, False)
            setattr(phases, key, True)
            start = time.perf_counter()
            try:
                return fn(*args, **kwargs)
            finally:
                setattr(phases, key, nested)
                if not nested:
                    with lock:
                        metrics[key] += (time.perf_counter() - start) * 1000
        return wrapped

    def provider(symbol, timeframe, start, stop):
        assert symbol in symbols and timeframe == "15Min", (symbol, timeframe)
        with lock:
            metrics["provider_calls"] += 1
        time.sleep(latency_ms / 1000)
        return [bar for bar in bars if start <= app._parse_market_timestamp(bar["t"]) <= stop]

    prepare = timed("preparation_ms", app._prepare_window_coverage)
    sync = timed("preparation_ms", app._sync_market_data_window)
    with patch.object(app, "_get_conn", repo.get_conn), \
         patch.object(app, "analytics_service", service), \
         patch.object(app, "_portfolio_symbols", return_value=symbols), \
         patch.object(app, "_utcnow", return_value=end + timedelta(days=40)), \
         patch.object(app, "_fetch_alpaca_bars", side_effect=timed("provider_ms", provider)), \
         patch.object(app, "_prepare_window_coverage", side_effect=prepare), \
         patch.object(AnalyticsService, "get_future_state", timed("analytics_ms", AnalyticsService.get_future_state)), \
         patch.object(AnalyticsService, "get_batch_future_state", timed("analytics_ms", AnalyticsService.get_batch_future_state)), \
         patch.dict(baseline, _sync_market_data_window=sync, analytics_service=service,
                    _portfolio_symbols=app._portfolio_symbols), \
         patch.dict(app.app.view_functions, {name: baseline[name] if label == "before" else getattr(app, name)
                    for name in ("get_analytics_future_state", "get_portfolio_analytics_future_state")}):
        yield service, metrics


def request(url):
    with app.app.test_client() as client:
        start = time.perf_counter()
        response = client.get(url)
        elapsed = (time.perf_counter() - start) * 1000
    assert response.status_code == 200, response.get_json()
    return elapsed, response.get_json()


def clear_metrics(metrics):
    for key in metrics:
        metrics[key] = 0


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repeats", type=int, default=5)
    parser.add_argument("--latency-ms", type=float, default=50)
    args = parser.parse_args()
    baseline = baseline_routes()
    print(json.dumps({"baseline": BASELINE, "repeats": args.repeats, "provider_latency_ms": args.latency_ms}), flush=True)
    for count in (1, 10, 37):
        with batch_fixture(os.environ["ANALYTICS_TEST_DATABASE_URL"], count=count + 1, bars=5000) as (dsn, end):
            symbols = [f"D{i:03d}" for i in range(1, count + 1)]
            asof = end - timedelta(days=2) + timedelta(hours=4)
            urls = [f"/api/portfolio/futurestate?timeframe={tf}&asof={asof.isoformat().replace('+00:00', 'Z')}&days=20"
                    for tf in ("1D", "15Min")]
            for label in ("before", "after"):
                with variant(dsn, end, symbols, label, args.latency_ms, baseline) as (service, metrics):
                    start, stop = app._future_preparation_window(asof, 20)
                    with psycopg2.connect(dsn) as conn, conn.cursor() as cur:
                        cur.execute("DELETE FROM market_data_coverage")
                    clear_metrics(metrics)
                    elapsed, payload = request(urls[1])
                    assert payload["count"] == (0 if label == "after" else count), payload
                    assert len(payload["errors"]) == (count if label == "after" else 0)
                    print(json.dumps({"symbols": count, "variant": label, "mode": "uncovered-portfolio",
                                      "elapsed_ms": round(elapsed, 3), **metrics}), flush=True)
                    # Explicit bounded preparation, one symbol per call. Baseline
                    # already fetched above; remove coverage for a comparable run.
                    with psycopg2.connect(dsn) as conn, conn.cursor() as cur:
                        cur.execute("DELETE FROM market_data_coverage")
                    clear_metrics(metrics)
                    began = time.perf_counter()
                    for symbol in symbols:
                        app._prepare_window_coverage(symbol, "15Min", start, stop)
                    assert metrics["provider_calls"] == count
                    print(json.dumps({"symbols": count, "variant": label, "mode": "explicit-preparation",
                                      "elapsed_ms": round((time.perf_counter() - began) * 1000, 3), **metrics}), flush=True)
                    for url in urls:
                        expected = None
                        for mode in ("covered-cold-cache", "covered-warm-cache"):
                            clear_metrics(metrics)
                            samples = []
                            for _ in range(args.repeats):
                                if mode == "covered-cold-cache":
                                    service._future_cache = _StateCache()
                                elapsed, payload = request(url)
                                assert payload["count"] == count and not payload["errors"], payload
                                # Sync metadata changes between baseline requests;
                                # substantive label values must remain identical.
                                values = [p["future_state"] for p in payload["future_states"]]
                                expected = values if expected is None else expected
                                assert values == expected
                                samples.append(elapsed)
                            assert metrics["provider_calls"] == (0 if label == "after" or "timeframe=1D" in url else count * args.repeats)
                            print(json.dumps({"symbols": count, "variant": label, "url": url, "mode": mode,
                                              "median_ms": round(statistics.median(samples), 3),
                                              **{k: round(v / args.repeats, 3) for k, v in metrics.items()}}), flush=True)
                    if count == 1:
                        with psycopg2.connect(dsn) as conn, conn.cursor() as cur:
                            cur.execute("DELETE FROM market_data_coverage")
                        clear_metrics(metrics)
                        barrier = Barrier(6)
                        def concurrent(_):
                            barrier.wait(timeout=10)
                            return request(urls[1].replace("/api/portfolio/futurestate", "/api/futurestate/D001"))
                        with ThreadPoolExecutor(max_workers=6) as pool:
                            results = list(pool.map(concurrent, range(6)))
                        assert all(p["window"]["future_bar_count"] == 20 for _, p in results)
                        assert metrics["provider_calls"] == 1
                        print(json.dumps({"symbols": 1, "variant": label, "mode": "six-concurrent-missing",
                                          "median_ms": round(statistics.median(t for t, _ in results), 3), **metrics}), flush=True)


if __name__ == "__main__":
    main()
