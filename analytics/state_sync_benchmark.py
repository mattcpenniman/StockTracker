"""VEC-783: real PostgreSQL/Flask state reads with controlled provider latency.

ANALYTICS_TEST_DATABASE_URL=... python -m analytics.state_sync_benchmark --repeats 20
Creates/drops a private schema. No external provider is contacted.
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
from threading import Barrier, Lock
import time
from unittest.mock import patch

import numpy as np
import psycopg2

import app
from .batch_benchmark import batch_fixture
from .data_access import AnalyticsRepository
from .service import AnalyticsService, _StateCache


BASELINE = "9cb7aa7"


def baseline_namespace():
    source = subprocess.check_output(["git", "show", f"{BASELINE}:app.py"], text=True)
    names = {"get_analytics_state", "_sync_market_data_window", "_sync_market_data",
             "_sync_current_analytics_data_if_needed", "_backfill_analytics_symbol_if_needed"}
    nodes = [n for n in ast.parse(source).body if isinstance(n, ast.FunctionDef) and n.name in names]
    assert {n.name for n in nodes} == names
    for node in nodes:
        node.decorator_list = []
    namespace = dict(vars(app))
    exec(compile(ast.Module(body=nodes, type_ignores=[]), "state-sync-baseline", "exec"), namespace)
    return namespace


class CountingCache(_StateCache):
    def __init__(self):
        super().__init__()
        self.hits = self.misses = 0

    def get(self, key):
        result = super().get(key)
        if result is None:
            self.misses += 1
        else:
            self.hits += 1
        return result


@contextmanager
def variant(dsn, end, label, latency_ms, baseline):
    repo = AnalyticsRepository(dsn)
    service = AnalyticsService(repo)
    service._state_cache = CountingCache()
    state = {"provider_calls": 0}
    lock = Lock()
    session = end - timedelta(days=2)
    bars = [{"t": (session + timedelta(minutes=15 * n)).isoformat(),
             "o": 100, "h": 103, "l": 97, "c": 100 + n * 0.1, "v": 1000}
            for n in range(40)]

    def provider(symbol, timeframe, start, stop):
        assert symbol == "D001" and timeframe == "15Min", (symbol, timeframe)
        with lock:
            state["provider_calls"] += 1
        time.sleep(latency_ms / 1000)
        return [bar for bar in bars if start <= app._parse_market_timestamp(bar["t"]) <= stop]

    now = end + timedelta(hours=1)
    with patch.object(app, "_get_conn", repo.get_conn), \
            patch.object(app, "analytics_service", service), \
            patch.object(app, "_utcnow", return_value=now), \
            patch.object(app, "_fetch_alpaca_bars", side_effect=provider), \
            patch.dict(baseline, _get_conn=repo.get_conn, analytics_service=service,
                       _utcnow=app._utcnow, _fetch_alpaca_bars=app._fetch_alpaca_bars), \
            patch.dict(app.app.view_functions, get_analytics_state=(baseline["get_analytics_state"]
                       if label == "before" else app.get_analytics_state)):
        yield service, state


def reset(dsn, service, missing):
    service._state_cache = CountingCache()
    if missing:
        with psycopg2.connect(dsn) as conn, conn.cursor() as cur:
            cur.execute("DELETE FROM stock_bars WHERE timeframe='15Min' AND symbol_id=(SELECT id FROM symbol_metadata WHERE symbol='D001')")
            cur.execute("DELETE FROM market_data_coverage WHERE symbol_id=(SELECT id FROM symbol_metadata WHERE symbol='D001')")


def request(url):
    with app.app.test_client() as client:
        start = time.perf_counter()
        response = client.get(url)
        elapsed = (time.perf_counter() - start) * 1000
    assert response.status_code == 200, response.get_json()
    return elapsed, response.get_json()


def comparable(payload):
    payload = json.loads(json.dumps(payload))
    payload.get("data_quality", {}).pop("last_sync_timestamp", None)
    return payload


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repeats", type=int, default=20)
    parser.add_argument("--latency-ms", type=float, default=50)
    parser.add_argument("--bars", type=int, default=5000)
    args = parser.parse_args()
    baseline = baseline_namespace()
    with batch_fixture(os.environ["ANALYTICS_TEST_DATABASE_URL"], count=2, bars=args.bars) as (dsn, end):
        session = end - timedelta(days=2)
        timed = (session + timedelta(hours=4)).isoformat().replace("+00:00", "Z")
        date = session.date().isoformat()
        urls = [f"/api/state/D001?timeframe=1D&asof={date}",
                f"/api/state/D001?timeframe=15Min&asof={timed}",
                f"/api/state/D001?timeframe=1D&asof={date}&hidets=true"]
        print(json.dumps({"baseline": BASELINE, "repeats": args.repeats,
                          "provider_latency_ms": args.latency_ms, "daily_bars": args.bars}), flush=True)
        for url in urls:
            for mode in ("covered-cold-cache", "covered-warm-cache", "missing-cold-cache"):
                expected = None
                for label in ("before", "after"):
                    with variant(dsn, end, label, args.latency_ms, baseline) as (service, state):
                        # Warmup restores the provider fixture and verifies the
                        # after-hours tail once. Exclude its counts and timing.
                        reset(dsn, service, True)
                        request(url)
                        samples = []
                        hits = misses = 0
                        state["provider_calls"] = 0
                        for _ in range(args.repeats):
                            if mode != "covered-warm-cache":
                                reset(dsn, service, mode == "missing-cold-cache")
                            cache = service._state_cache
                            h, m = cache.hits, cache.misses
                            elapsed, payload = request(url)
                            samples.append(elapsed)
                            hits += cache.hits - h
                            misses += cache.misses - m
                            if expected is None:
                                expected = comparable(payload)
                            assert comparable(payload) == expected, (url, mode, label)
                        print(json.dumps({"url": url, "mode": mode, "variant": label,
                                          "median_ms": round(statistics.median(samples), 3),
                                          "p95_ms": round(float(np.percentile(samples, 95)), 3),
                                          **state, "cache_hits": hits, "cache_misses": misses}), flush=True)
        url = urls[1]
        for label in ("before", "after"):
            with variant(dsn, end, label, args.latency_ms, baseline) as (service, state):
                reset(dsn, service, True)
                barrier = Barrier(8)
                def concurrent(_):
                    barrier.wait(timeout=10)
                    return request(url)
                with ThreadPoolExecutor(max_workers=8) as executor:
                    results = list(executor.map(concurrent, range(8)))
                assert all(comparable(p) == comparable(results[0][1]) for _, p in results)
                print(json.dumps({"mode": "eight-concurrent-missing", "variant": label,
                                  "median_ms": round(statistics.median(t for t, _ in results), 3),
                                  "p95_ms": round(float(np.percentile([t for t, _ in results], 95)), 3),
                                  **state, "cache_hits": service._state_cache.hits,
                                  "cache_misses": service._state_cache.misses}), flush=True)


if __name__ == "__main__":
    main()
