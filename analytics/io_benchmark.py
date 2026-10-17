"""PostgreSQL price-read parity and cold-cache endpoint benchmark.

Run with ANALYTICS_TEST_DATABASE_URL pointing to a disposable PostgreSQL database:
    python -m analytics.io_benchmark --repeats 20
Only a uniquely named schema is created and removed; no market-data sync runs.
"""
from __future__ import annotations

import argparse
from contextlib import contextmanager
import os
import statistics
import time
import uuid
from unittest.mock import patch

import numpy as np
import pandas as pd
import psycopg2
from psycopg2.extensions import cursor, make_dsn

from .data_access import AnalyticsRepository
from .service import AnalyticsService


class UnboundedPriceRepository(AnalyticsRepository):
    """Pre-VEC-780 reads, including the old Python-side window truncation."""

    def fetch_latest_bar(self, symbol_id, timeframe, asof=None):
        return self.fetch_bars(symbol_id, timeframe, asof)

    def fetch_future_bars(self, symbol_id, timeframe, after, limit):
        return self.fetch_bars(
            symbol_id, timeframe, start=after + pd.Timedelta(microseconds=1),
        ).head(limit).copy()


@contextmanager
def price_fixture(database_url: str, daily_count: int = 10000, intraday_count: int = 200000):
    """Use production DDL and indexes in a private schema, even on failure."""
    from app import _init_db_if_needed

    schema = "price_reads_" + uuid.uuid4().hex
    admin = psycopg2.connect(database_url)
    admin.autocommit = True
    with admin.cursor() as cur:
        cur.execute(f'CREATE SCHEMA "{schema}"')
    dsn = make_dsn(database_url, options=f"-csearch_path={schema}")
    try:
        with patch("app._get_conn", lambda: psycopg2.connect(dsn)):
            _init_db_if_needed()
        with psycopg2.connect(dsn) as conn, conn.cursor() as cur:
            cur.execute("INSERT INTO symbol_metadata (symbol) VALUES ('BENCH') RETURNING id")
            symbol_id = cur.fetchone()[0]
            for timeframe, count, start, interval in (
                ("1Day", daily_count, "2000-01-01", "1 day"),
                ("15Min", intraday_count, "2020-01-01", "15 minutes"),
            ):
                cur.execute("""
                    INSERT INTO stock_bars (symbol_id, timeframe, bar_time, open, high, low, close, volume)
                    SELECT %s, %s, %s::timestamptz + n * %s::interval,
                           100 + sin(n / 20.0), 103 + sin(n / 20.0),
                           97 + sin(n / 20.0), 100 + sin(n / 20.0), 1000 + n %% 100
                    FROM generate_series(0, %s - 1) AS n
                """, (symbol_id, timeframe, start, interval, count))
            cur.execute("ANALYZE stock_bars")
        yield dsn, symbol_id
    finally:
        with admin.cursor() as cur:
            cur.execute(f'DROP SCHEMA "{schema}" CASCADE')
        admin.close()


def measured_repository(repository_type, dsn):
    class MeasuredRepository(repository_type):
        queries = 0
        rows = 0

        def get_conn(self):
            owner = self

            class CountingCursor(cursor):
                def execute(self, *args, **kwargs):
                    owner.queries += 1
                    return super().execute(*args, **kwargs)

                def fetchone(self):
                    row = super().fetchone()
                    owner.rows += int(row is not None)
                    return row

                def fetchall(self):
                    rows = super().fetchall()
                    owner.rows += len(rows)
                    return rows

            return psycopg2.connect(self.database_url, cursor_factory=CountingCursor)

    return MeasuredRepository(dsn)


def explain(dsn, symbol_id, anchor):
    repo = AnalyticsRepository(dsn)
    with repo.get_conn() as conn, conn.cursor() as cur:
        for label, operation in (
            ("latest", lambda: repo.fetch_latest_bar(symbol_id, "15Min", anchor)),
            ("forward", lambda: repo.fetch_future_bars(symbol_id, "15Min", anchor, 20)),
        ):
            # Capture the actual repository SQL, rather than an imitation of it.
            from unittest.mock import MagicMock
            connection = MagicMock()
            capture = connection.__enter__.return_value.cursor.return_value.__enter__.return_value
            capture.fetchall.return_value = []
            with patch.object(repo, "get_conn", return_value=connection):
                operation()
            sql, params = capture.execute.call_args.args
            cur.execute("EXPLAIN (ANALYZE, BUFFERS) " + sql, params)
            print(label + ":\n" + "\n".join(row[0] for row in cur.fetchall()))


def main():
    from app import app

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repeats", type=int, default=20)
    parser.add_argument("--daily-bars", type=int, default=10000)
    parser.add_argument("--intraday-bars", type=int, default=200000)
    args = parser.parse_args()
    dsn = os.environ.get("ANALYTICS_TEST_DATABASE_URL")
    if not dsn or min(args.repeats, args.daily_bars, args.intraday_bars) < 1:
        parser.error("Set ANALYTICS_TEST_DATABASE_URL and use positive counts")
    with price_fixture(dsn, args.daily_bars, args.intraday_bars) as (dsn, symbol_id):
        anchor = pd.Timestamp("2020-01-01", tz="UTC") + pd.Timedelta(minutes=15 * (args.intraday_bars // 2))
        explain(dsn, symbol_id, anchor)
        date = anchor.strftime("%Y-%m-%dT00:00:00Z")
        urls = [
            f"/api/state/BENCH?timeframe=1D&asof={date}",
            f"/api/state/BENCH?timeframe=15Min&asof={anchor.isoformat().replace('+00:00', 'Z')}",
            f"/api/futurestate/BENCH?timeframe=1D&asof={date}&days=20",
            f"/api/futurestate/BENCH?timeframe=15Min&asof={anchor.isoformat().replace('+00:00', 'Z')}&days=20&stoploss=0.02&takeprofit=0.02&side=short",
        ]
        print(f"Fixture: {args.daily_bars} daily + {args.intraday_bars} intraday bars; {args.repeats} measured requests per variant")
        print("Cold service caches; PostgreSQL/OS caches warm; sync mocked; real PostgreSQL reads and Flask routes")
        with app.test_client() as client, patch("app._sync_current_analytics_data_if_needed"), patch("app._sync_market_data_window"):
            for url in urls:
                samples = {"before": [], "after": []}
                counts = {}
                for iteration in range(args.repeats + 1):
                    payloads = {}
                    # Alternate order to reduce ordering bias; first pair is warmup.
                    variants = [("before", UnboundedPriceRepository), ("after", AnalyticsRepository)]
                    for label, cls in variants[::1 if iteration % 2 else -1]:
                        repo = measured_repository(cls, dsn)
                        with patch("app.analytics_service", AnalyticsService(repo)):
                            start = time.perf_counter()
                            response = client.get(url)
                            elapsed = (time.perf_counter() - start) * 1000
                        assert response.status_code == 200, response.get_json()
                        payloads[label] = response.get_json()
                        counts[label] = (repo.queries, repo.rows)
                        if iteration:
                            samples[label].append(elapsed)
                    assert payloads["before"] == payloads["after"], url
                print(url)
                for label, timings in samples.items():
                    print(f"  {label}: median {statistics.median(timings):.2f} ms; p95 {np.percentile(timings, 95):.2f} ms; queries/rows {counts[label]}")


if __name__ == "__main__":
    main()
