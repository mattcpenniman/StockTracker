"""VEC-785 real PostgreSQL endpoint benchmark (private schema, no providers).

ANALYTICS_TEST_DATABASE_URL=... python -m analytics.pool_benchmark --repeats 10
Before uses the pre-pool psycopg2.connect factory; all SQL/service/routes match.
"""
from concurrent.futures import ThreadPoolExecutor
from datetime import timedelta
import argparse
import json
import os
import statistics
import threading
import time
from unittest.mock import patch

import numpy as np
import psycopg2
from psycopg2.extensions import make_dsn

import app
import database
from .batch_benchmark import batch_fixture
from .data_access import AnalyticsRepository
from .service import AnalyticsService


def measure(dsn, label, url, body, repeats, workers, cold):
    tag = "vec785_" + label
    dsn = make_dsn(dsn, application_name=tag)
    repo = AnalyticsRepository(dsn)
    pool = database.ConnectionPool(dsn, size=4)
    factory = psycopg2.connect
    creations = 0
    count_lock = threading.Lock()

    def connect(*args, **kwargs):
        nonlocal creations
        conn = factory(*args, **kwargs)
        with count_lock:
            creations += 1
        return conn

    def before():
        # Identical to the old get_conn: connection context commits/rolls back;
        # CPython reference release closes the connection after each DB block.
        return connect(dsn)

    samples = []
    peaks = {"connections": 0, "active": 0, "idle_in_transaction": 0}
    stop = threading.Event()
    monitor = factory(dsn)
    monitor.autocommit = True

    def snapshot():
        with monitor.cursor() as cur:
            cur.execute("""SELECT count(*), count(*) FILTER (WHERE state='active'),
                           count(*) FILTER (WHERE state LIKE 'idle in transaction%%')
                           FROM pg_stat_activity WHERE application_name=%s AND pid <> pg_backend_pid()""", (tag,))
            return dict(zip(peaks, cur.fetchone()))

    def observe():
        while not stop.wait(0.005):
            for key, value in snapshot().items():
                peaks[key] = max(peaks[key], value)

    def request(_):
        if cold:
            # Cold runs are sequential, so replacing the shared service is safe.
            app.analytics_service = AnalyticsService(repo)
        with app.app.test_client() as client:
            start = time.perf_counter()
            response = client.post(url, json=body) if body else client.get(url)
            elapsed = (time.perf_counter() - start) * 1000
            assert response.status_code == 200, response.get_json()
            return elapsed, response.get_json()

    try:
        with patch.object(database.psycopg2, "connect", connect), \
             patch.object(repo, "get_conn", before if label == "before" else pool.connection), \
             patch.object(app, "_get_conn", repo.get_conn), \
             patch.object(app, "analytics_service", AnalyticsService(repo)), \
             patch.object(app, "_init_db_if_needed"), \
             patch.object(app, "_sync_market_data", side_effect=AssertionError("Unexpected provider")), \
             patch.object(app, "_sync_market_data_window"):
            _, expected = request(0)
            # Counts include pool/endpoint warmup; latency samples exclude it.
            thread = threading.Thread(target=observe)
            thread.start()
            start = time.perf_counter()
            try:
                with ThreadPoolExecutor(max_workers=workers) as executor:
                    for elapsed, payload in executor.map(request, range(repeats)):
                        assert payload == expected
                        samples.append(elapsed)
                duration = time.perf_counter() - start
            finally:
                stop.set()
                thread.join()
            final = snapshot()
        pool.close()
        after_close = snapshot()
        assert after_close["connections"] == 0, after_close
        assert final["idle_in_transaction"] == 0, final
        return {"variant": label, "median_ms": round(statistics.median(samples), 2),
                "p95_ms": round(float(np.percentile(samples, 95)), 2),
                "requests_per_second": round(repeats / duration, 2),
                "created": creations, "peak": peaks, "final": final,
                "after_close": after_close}, expected
    finally:
        pool.close()
        monitor.close()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repeats", type=int, default=10)
    parser.add_argument("--bars", type=int, default=5000)
    parser.add_argument("--load-requests", type=int, default=80)
    args = parser.parse_args()
    dsn = os.environ["ANALYTICS_TEST_DATABASE_URL"]
    for count in (10, 50, 100):
        with batch_fixture(dsn, count=count, bars=args.bars) as (private, end):
            date = (end - timedelta(days=2)).isoformat().replace("+00:00", "Z")
            body = {"symbols": [f"D{i:03d}" for i in range(count)], "asof": date}
            cases = [("single-warm", f"/api/state/D001?asof={date}", None, 1, False),
                     ("batch-cold", "/api/state/batch", body, 1, True),
                     ("batch-warm", "/api/state/batch", body, 1, False),
                     ("future-warm", f"/api/portfolio/futurestate?asof={date}&days=20", None, 1, False),
                     ("batch-warm-concurrent", "/api/state/batch", body, 8, False)]
            for index, (name, url, data, workers, cold) in enumerate(cases):
                expected = None
                repeats = args.load_requests if workers > 1 else args.repeats
                for label in ("before", "after")[::1 if index % 2 else -1]:
                    result, payload = measure(private, label, url, data, repeats, workers, cold)
                    assert not payload.get("errors"), payload.get("errors")
                    if expected is not None:
                        assert payload == expected
                    expected = payload
                    print(json.dumps(dict(symbols=count, case=name, workers=workers, requests=repeats, **result)), flush=True)


if __name__ == "__main__":
    main()
