"""Real PostgreSQL split ingestion, independent factor math and MVCC checks."""
from concurrent.futures import ThreadPoolExecutor
from contextlib import ExitStack
from datetime import timedelta
import os
import threading
import unittest
from unittest.mock import patch

import pandas as pd

import app as dashboard
from analytics.config import AnalyticsSettings
from analytics.data_access import AnalyticsRepository
from analytics.io_benchmark import price_fixture
from analytics.service import AnalyticsService
from database import get_connection


@unittest.skipUnless(os.environ.get("ANALYTICS_TEST_DATABASE_URL"), "requires PostgreSQL")
class AdjustmentHistoryTests(unittest.TestCase):
    def setUp(self):
        self.stack = ExitStack()
        self.addCleanup(self.stack.close)
        self.dsn, self.sid = self.stack.enter_context(price_fixture(
            os.environ["ANALYTICS_TEST_DATABASE_URL"], 0, 0))
        self.stack.enter_context(patch.object(dashboard, "_get_conn", lambda: get_connection(self.dsn)))
        self.stack.enter_context(patch.object(dashboard, "_fetch_alpaca_latest_quote", return_value=None))
        self.stack.enter_context(patch.object(dashboard, "_fetch_alpaca_latest_bar", return_value=None))
        self.repo = AnalyticsRepository(self.dsn)
        self.service = AnalyticsService(self.repo)
        self.settings = AnalyticsSettings()
        self.start = pd.Timestamp("2024-01-02T05:00Z").to_pydatetime()
        self.boundary = self.start + timedelta(days=40)
        self.end = self.boundary + timedelta(days=2)
        self.ratio = 1
        self.mode = "split"
        self.calls = []
        self.provider = self.stack.enter_context(patch.object(dashboard, "_fetch_alpaca_bars", side_effect=self.fetch))
        self.stack.enter_context(patch.object(dashboard, "_alpaca_bar_adjustment_for_timeframe", side_effect=lambda tf: self.mode))

    def rows(self, tf, *, adjusted=True, future=True):
        # Forward ratio = new shares / old shares; reverse ratio < 1.
        rows = []
        for n in range(43 if future else 40):
            t = self.start + timedelta(days=n, hours=10 if tf != "1Day" else 0)
            factor = self.ratio if adjusted and n < 40 else 1
            raw = 100 + n if n < 40 else (100 + n) / self.ratio
            increment = 1 if n < 40 else 1 / self.ratio
            rows.append(dict(t=t.isoformat(), o=raw / factor, h=(raw + 2 * increment) / factor,
                             l=(raw - 2 * increment) / factor, c=(raw + increment) / factor,
                             v=int(10000 * factor), vw=raw / factor, n=100))
        return rows

    def fetch(self, symbol, tf, start, end):
        self.calls.append((tf, start, end))
        return [bar for bar in self.rows(tf, adjusted=self.mode != "raw")
                if start <= pd.Timestamp(bar["t"]) <= end]

    def seed(self):
        dashboard._ensure_symbol_metadata("BENCH")
        for tf in ("1Day", "15Min", "1Hour", "5Min", "1Min"):
            dashboard._upsert_stock_bars(self.sid, tf, self.rows(tf, adjusted=False, future=False))

    def sync(self):
        # Only ask for the new action window. Stored pre-action history must change too.
        return dashboard._sync_market_data_window("BENCH", "1Day", self.boundary, self.end)

    def revision(self):
        return self.repo.fetch_symbol_context("BENCH")["adjustment_revision"]

    def test_forward_reverse_splits_prices_inverse_volume_returns_indicators_and_caches(self):
        for ratio in (10, 0.1):
            with self.subTest(ratio=ratio):
                self.service = AnalyticsService(self.repo)
                with get_connection(self.dsn) as conn, conn.cursor() as cur:
                    cur.execute("DELETE FROM stock_bars; DELETE FROM market_data_coverage; DELETE FROM market_data_basis")
                self.ratio = 1
                self.seed()
                old = self.service.get_symbol_state("BENCH", "1Day", self.boundary, self.settings)
                self.service.get_events("BENCH", "1Day", self.boundary, self.settings)
                self.service.get_future_state("BENCH", "1Day", self.start + timedelta(days=35), 3)
                self.ratio = ratio
                dashboard._sync_market_data("BENCH", "1Day")
                revision = self.revision()
                self.assertTrue(revision)
                for tf in dashboard.SUPPORTED_TIMEFRAMES:
                    bars = self.repo.fetch_bars(self.sid, tf, self.end + timedelta(days=2))
                    self.assertAlmostEqual(bars.iloc[0]["open"], 100 / ratio)
                    self.assertAlmostEqual(bars.iloc[0]["high"], 102 / ratio)
                    self.assertAlmostEqual(bars.iloc[0]["low"], 98 / ratio)
                    self.assertAlmostEqual(bars.iloc[0]["close"], 101 / ratio)
                    self.assertEqual(bars.iloc[0]["volume"], int(10000 * ratio))
                state = self.service.get_symbol_state("BENCH", "1Day", self.end + timedelta(days=2), self.settings)
                self.assertEqual(state["data_quality"]["adjustment_basis"]["revision"], revision)
                # Independently calculate SMA and one-bar return from the unit-converted closes.
                bars = self.repo.fetch_bars(self.sid, "1Day", self.end + timedelta(days=2))
                expected_sma = sum((101 + n) / ratio for n in range(23, 43)) / 20
                self.assertAlmostEqual(state["trend"]["sma_20"], expected_sma)
                self.assertAlmostEqual(state["volume"]["avg_volume_20"], (17 * 10000 * ratio + 3 * 10000) / 20)
                self.assertAlmostEqual(state["returns"]["r_1"], bars.iloc[-1]["close"] / bars.iloc[-2]["close"] - 1)
                self.assertAlmostEqual(bars.iloc[40]["close"] / bars.iloc[39]["close"] - 1, 141 / 140 - 1)
                # Same historical cache keys/cutoffs must recompute for changed revisions.
                new = self.service.get_symbol_state("BENCH", "1Day", self.boundary, self.settings)
                self.assertNotEqual(old["price"], new["price"])
                self.service.get_events("BENCH", "1Day", self.boundary, self.settings)
                self.service.get_future_state("BENCH", "1Day", self.start + timedelta(days=35), 3)
                self.assertEqual(self.service._events_cache.diagnostics()["misses"], 2)
                self.assertEqual(self.service._future_cache.diagnostics()["misses"], 2)
                self.sync()
                self.assertEqual(self.revision(), revision)

    def test_configuration_and_coverage_revision(self):
        self.seed()
        self.ratio = 10
        self.sync()
        first = self.revision()
        self.assertTrue(dashboard._historical_price_coverage("BENCH", "1Day", self.boundary, self.end)[2])
        self.mode = "raw"
        self.assertFalse(dashboard._historical_price_coverage("BENCH", "1Day", self.boundary, self.end)[2])
        self.sync()
        self.assertNotEqual(self.revision(), first)
        for tf in dashboard.SUPPORTED_TIMEFRAMES:
            self.assertEqual(self.repo.fetch_bars(self.sid, tf).iloc[0]["close"], 101)
        with get_connection(self.dsn) as conn, conn.cursor() as cur:
            cur.execute("SELECT DISTINCT adjustment_revision, adjustment_mode FROM market_data_coverage")
            self.assertEqual(cur.fetchall(), [(self.revision(), "raw")])

    def test_partial_provider_response_failure_and_write_failure_preserve_entire_old_generation(self):
        self.seed()
        self.sync()
        before = self.revision()
        self.ratio = 10
        original = self.fetch
        original_write = dashboard._upsert_stock_bars
        for failure in ("missing", "provider", "write"):
            def fetch(symbol, tf, start, end):
                if tf == "5Min":
                    if failure == "provider":
                        raise RuntimeError("page two failed")
                    if failure == "missing":
                        return []
                return original(symbol, tf, start, end)
            def write(*args, **kwargs):
                original_write(*args, **kwargs)
                raise RuntimeError("write failed after first timeframe")
            with self.subTest(failure=failure), patch.object(dashboard, "_fetch_alpaca_bars", side_effect=fetch), ExitStack() as stack:
                if failure == "write":
                    stack.enter_context(patch.object(dashboard, "_upsert_stock_bars", side_effect=write))
                with self.assertRaises((ValueError, RuntimeError)):
                    self.sync()
                self.assertEqual(self.revision(), before)
                for tf in dashboard.SUPPORTED_TIMEFRAMES:
                    self.assertEqual(self.repo.fetch_bars(self.sid, tf).iloc[0]["close"], 101)

    def test_concurrent_preparations_serialize_and_reads_keep_old_or_new_snapshot(self):
        self.seed()
        self.sync()
        self.ratio = 10
        staged, release = threading.Event(), threading.Event()
        calls = []
        original = self.fetch
        def fetch(*args):
            calls.append(threading.get_ident())
            if len(calls) == 1:
                staged.set()
                self.assertTrue(release.wait(10))
            return original(*args)
        with patch.object(dashboard, "_fetch_alpaca_bars", side_effect=fetch), ThreadPoolExecutor(max_workers=2) as pool:
            first = pool.submit(self.sync)
            self.assertTrue(staged.wait(10))
            # Reads remain available while the refresh is staged.
            old = self.service.get_symbol_state("BENCH", "1Day", self.boundary, self.settings)
            second = pool.submit(dashboard._sync_market_data_window_once, "BENCH", "15Min", self.boundary, self.end)
            release.set()
            first.result(timeout=20)
            second.result(timeout=20)
        self.assertEqual(len(set(calls[:5])), 1)
        self.assertEqual(len(set(calls[5:])), 1)
        self.assertNotEqual(calls[0], calls[5])
        new = self.service.get_symbol_state("BENCH", "1Day", self.boundary, self.settings)
        self.assertNotEqual(old["price"], new["price"])
        # Force a writer commit between daily and intraday reads of one uncached state.
        entered, proceed = threading.Event(), threading.Event()
        original_read = self.repo.fetch_state_bars
        def read(*args, **kwargs):
            result = original_read(*args, **kwargs)
            if args[1] == "1Day":
                entered.set()
                self.assertTrue(proceed.wait(10))
            return result
        cutoff = self.end + timedelta(days=1, hours=20)
        expected = self.service.get_symbol_state("BENCH", "15Min", cutoff, self.settings)
        self.service = AnalyticsService(self.repo)
        with patch.object(self.repo, "fetch_state_bars", side_effect=read), ThreadPoolExecutor(max_workers=1) as pool:
            pending = pool.submit(self.service.get_symbol_state, "BENCH", "15Min", cutoff, self.settings)
            self.assertTrue(entered.wait(10))
            self.mode = "raw"
            self.sync()
            proceed.set()
            self.assertEqual(pending.result(timeout=15), expected)

    def test_correct_reference_split_boundaries_are_not_adjusted_twice(self):
        # Stored reference observations read-only sampled 2026-10-18 for VEC-825.
        # These are preservation fixtures, not certification of every provider field.
        references = {
            "NVDA": [("2024-06-07T04:00Z", 119.77, 121.69, 118.02, 120.89, 412385800),
                     ("2024-06-10T04:00Z", 120.37, 195.95, 117.01, 121.79, 314162666)],
            "AVGO": [("2024-07-12T04:00Z", 171.1, 172.59, 169.13, 170.07, 30453000),
                     ("2024-07-15T04:00Z", 170.0, 173.5099, 169.26, 171.42, 22999201)],
            "WMT": [("2024-02-23T05:00Z", 58.7, 59.22, 58.44, 58.52, 74365767),
                    ("2024-02-26T05:00Z", 59.12, 59.84, 58.18, 59.6, 32154765)],
        }
        for symbol, values in references.items():
            sid = dashboard._ensure_symbol_metadata(symbol)
            bars = [dict(zip(("t", "o", "h", "l", "c", "v"), row)) for row in values]
            dashboard._upsert_stock_bars(sid, "1Day", bars)
            with patch.object(dashboard, "_fetch_alpaca_bars", return_value=bars):
                for _ in range(2):
                    dashboard._sync_market_data_window(symbol, "1Day", pd.Timestamp(values[0][0]), pd.Timestamp(values[-1][0]))
            stored = self.repo.fetch_bars(sid, "1Day")
            self.assertEqual(stored["close"].tolist(), [row[4] for row in values])
            self.assertEqual(stored["volume"].tolist(), [row[5] for row in values])
