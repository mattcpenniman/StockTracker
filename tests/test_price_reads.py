from __future__ import annotations

import os
import unittest
from unittest.mock import MagicMock, patch

import pandas as pd
import psycopg2

from analytics.config import AnalyticsSettings
from analytics.data_access import AnalyticsRepository
from analytics.io_benchmark import UnboundedPriceRepository, price_fixture
from analytics.service import AnalyticsNotFoundError, AnalyticsService
from analytics.utils import parse_asof


class PriceQueryTests(unittest.TestCase):
    def setUp(self):
        self.repo = AnalyticsRepository()
        self.connection = MagicMock()
        self.cursor = self.connection.__enter__.return_value.cursor.return_value.__enter__.return_value
        self.cursor.fetchall.return_value = []
        self.patch = patch.object(self.repo, "get_conn", return_value=self.connection)
        self.patch.start()
        self.addCleanup(self.patch.stop)

    def test_inclusive_anchor_uses_utc_and_parameterized_limit(self):
        asof = pd.Timestamp("2023-01-03T09:30:00-05:00")
        result = self.repo.fetch_latest_bar(1, "15Min", asof)
        sql, params = self.cursor.execute.call_args.args
        self.assertIn("bar_time <= %s", sql)
        self.assertTrue(sql.endswith("ORDER BY bar_time DESC LIMIT %s"))
        self.assertEqual(params, [1, "15Min", asof.tz_convert("UTC"), 1])
        self.assertEqual(str(params[2].tzinfo), "UTC")
        self.assertEqual(list(result.columns), ["timestamp", "open", "high", "low", "close", "volume"])
        self.assertTrue(result.empty)

    def test_unbounded_asof_and_strict_forward_window(self):
        self.repo.fetch_latest_bar(1, "1Day")
        sql, params = self.cursor.execute.call_args.args
        self.assertNotIn("bar_time <=", sql)
        self.assertEqual(params, [1, "1Day", 1])
        anchor = pd.Timestamp("2023-01-03T09:30:00.000001-05:00")
        self.repo.fetch_future_bars(1, "15Min", anchor, 20)
        sql, params = self.cursor.execute.call_args.args
        self.assertIn("bar_time > %s", sql)
        self.assertTrue(sql.endswith("ORDER BY bar_time ASC LIMIT %s"))
        self.assertEqual(params, [1, "15Min", anchor.tz_convert("UTC"), 20])
        with self.assertRaises(ValueError):
            self.repo.fetch_future_bars(1, "15Min", anchor, 0)


@unittest.skipUnless(os.environ.get("ANALYTICS_TEST_DATABASE_URL"), "Set ANALYTICS_TEST_DATABASE_URL for PostgreSQL parity")
class PostgreSQLPriceParityTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.fixture = price_fixture(os.environ["ANALYTICS_TEST_DATABASE_URL"], intraday_count=40)
        cls.dsn, cls.symbol_id = cls.fixture.__enter__()
        cls.addClassCleanup(cls.fixture.__exit__, None, None, None)
        # Sparse trading days and intraday gaps must count as bars, not elapsed time.
        with psycopg2.connect(cls.dsn) as conn, conn.cursor() as cur:
            cur.execute("DELETE FROM stock_bars WHERE timeframe = '1Day' AND extract(isodow FROM bar_time) IN (6, 7)")
            cur.execute("DELETE FROM stock_bars WHERE timeframe = '15Min' AND extract(minute FROM bar_time) = 30")

    def setUp(self):
        self.repo = AnalyticsRepository(self.dsn)
        self.before = AnalyticsService(UnboundedPriceRepository(self.dsn))
        self.after = AnalyticsService(self.repo)

    def assert_parity(self, method, *args, **kwargs):
        try:
            expected = getattr(self.before, method)(*args, **kwargs)
        except AnalyticsNotFoundError as exc:
            with self.assertRaises(AnalyticsNotFoundError) as actual:
                getattr(self.after, method)(*args, **kwargs)
            self.assertEqual(str(actual.exception), str(exc))
            return None
        actual = getattr(self.after, method)(*args, **kwargs)
        self.assertEqual(actual, expected)
        return actual

    def test_state_daily_intraday_timezone_and_daily_fallback(self):
        for timeframe in ("1Day", "15Min", "1Hour"):
            for asof in (None, "2020-01-01", "2020-01-01T00:00:00Z", "2020-01-01T04:15:00Z", "2019-12-31T23:15:00-05:00", "1999-01-01"):
                with self.subTest(timeframe=timeframe, asof=asof):
                    self.assert_parity("get_symbol_state", "BENCH", timeframe, parse_asof(asof), AnalyticsSettings())

    def test_future_daily_intraday_partial_missing_and_both_sides(self):
        for timeframe in ("1Day", "15Min", "1Hour"):
            for asof in ("2020-01-03", "2020-01-03T00:00:00Z", "2020-01-01T04:15:00Z", "2019-12-31T23:15:00-05:00", "1999-01-01", "2040-01-01T00:00:00Z"):
                for days in (1, 5, 100):
                    for side in ("long", "short"):
                        with self.subTest(timeframe=timeframe, asof=asof, days=days, side=side):
                            self.assert_parity("get_future_state", "BENCH", timeframe, parse_asof(asof), days,
                                               stop_loss_pct=0.02, take_profit_pct=0.02, side=side)

    def test_chronological_window_excludes_anchor_and_later_extremes(self):
        anchor = parse_asof("2020-01-03")  # Friday; next daily bar is Monday.
        window = self.repo.fetch_future_bars(self.symbol_id, "1Day", anchor, 2)
        self.assertEqual(window["timestamp"].dt.strftime("%Y-%m-%d").tolist(), ["2020-01-06", "2020-01-07"])
        with psycopg2.connect(self.dsn) as conn, conn.cursor() as cur:
            cur.execute("UPDATE stock_bars SET high = 100000 WHERE timeframe = '15Min' AND bar_time = '2020-01-01T09:45:00Z'")
        result = self.assert_parity("get_future_state", "BENCH", "15Min", parse_asof("2020-01-01T04:15:00Z"), 2,
                                    stop_loss_pct=0.02, take_profit_pct=0.02)
        self.assertLess(result["future_state"]["max_price"], 100000)
        self.assertEqual(result["future_state"]["first_threshold_hit"], "both")
        self.assertEqual(result["future_state"]["first_threshold_hit_timestamp"], "2020-01-01T04:45:00Z")
        partial = self.assert_parity("get_future_state", "BENCH", "15Min", parse_asof("2020-01-01T09:00:00Z"), 5)
        self.assertFalse(partial["window"]["complete_window"])
        self.assertEqual(partial["window"]["future_bar_count"], 2)
        self.assertIsNone(partial["window"]["target_end_timestamp"])

    def test_repository_anchor_boundaries_and_microsecond_successor(self):
        anchor = pd.Timestamp("2020-01-01T04:15:00Z")
        for asof in (anchor, anchor + pd.Timedelta(minutes=10)):
            latest = self.repo.fetch_latest_bar(self.symbol_id, "15Min", asof)
            self.assertEqual(latest.iloc[0]["timestamp"], anchor)
        with psycopg2.connect(self.dsn) as conn, conn.cursor() as cur:
            cur.execute("""
                INSERT INTO stock_bars (symbol_id, timeframe, bar_time, open, high, low, close, volume)
                VALUES (%s, '1Min', %s, 100, 101, 99, 100, 1000),
                       (%s, '1Min', %s, 100, 102, 98, 101, 1000)
            """, (self.symbol_id, anchor, self.symbol_id, anchor + pd.Timedelta(microseconds=1)))
        self.assert_parity("get_future_state", "BENCH", "1Min", anchor, 1)
        self.assertEqual(len(self.repo.fetch_latest_bar(self.symbol_id, "1Min")), 1)

    def test_batch_retains_per_symbol_errors(self):
        for method, args in (
            ("get_batch_state", (AnalyticsSettings(),)),
            ("get_batch_future_state", (5,)),
        ):
            result = self.assert_parity(method, ["BENCH", "MISSING"], "1Day", parse_asof("2020-01-03T00:00:00Z"), *args)
            self.assertEqual(result["errors"][0]["symbol"], "MISSING")


if __name__ == "__main__":
    unittest.main()
