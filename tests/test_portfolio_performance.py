import os
import subprocess
import unittest
from unittest.mock import Mock, patch

import numpy as np
import pandas as pd
import psycopg2

from analytics import indicators
from analytics.config import AnalyticsSettings
from analytics.data_access import AnalyticsRepository
from analytics.io_benchmark import price_fixture
from analytics.service import AnalyticsService
from tests.test_analytics import _fixture_bars
from tests.quality_compat import legacy_quality_contract


BASELINE = "8cf31c7"


def baseline_class(path, name):
    source = subprocess.check_output(["git", "show", f"{BASELINE}:{path}"], text=True)
    namespace = {"__package__": "analytics"}
    exec(compile(source, "portfolio-baseline", "exec"), namespace)
    return namespace[name]


class StateSnapshotParityTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.baseline = baseline_class("analytics/service.py", "AnalyticsService")

    def test_full_state_parity_across_warmup_thresholds_and_price_anchors(self):
        for backend in ("python", "rust") if indicators._rust_feature_columns else ("python",):
            for count in (1, 19, 20, 21, 49, 50, 51, 199, 200, 201, 260):
                bars = _fixture_bars(count)
                bars["close"] = 100 + 10 * np.sin(np.arange(count) / 3)
                bars["open"] = bars["close"]
                bars["high"] = bars.close + 1
                bars["low"] = bars.close - 1
                bars["volume"] = np.where(np.arange(count) % 7 == 0, 10000, 1000)
                for confirmation in (False, True):
                    for timeframe in ("1Day", "15Min"):
                        with self.subTest(backend=backend, count=count, volume=confirmation, timeframe=timeframe), \
                             patch.dict(os.environ, ANALYTICS_BACKEND=backend):
                            repo = Mock()
                            repo.fetch_symbol_context.return_value = {"symbol": "TEST", "symbol_id": 1}
                            repo.fetch_bars.return_value = bars
                            anchor = bars.tail(1).copy()
                            anchor["close"] = 95.0
                            anchor["open"] = 95.0
                            anchor["high"] = 96.0
                            anchor["low"] = 94.0
                            repo.fetch_latest_bar.return_value = anchor
                            settings = AnalyticsSettings(volume_confirmation=confirmation)
                            expected = type(self).baseline(repo).get_symbol_state("TEST", timeframe, None, settings)
                            actual = AnalyticsService(repo).get_symbol_state("TEST", timeframe, None, settings)
                            self.assertEqual(legacy_quality_contract(actual), legacy_quality_contract(expected))


@unittest.skipUnless(os.environ.get("ANALYTICS_TEST_DATABASE_URL"), "requires PostgreSQL")
class TimestampTransportTests(unittest.TestCase):
    def test_microsecond_timezone_and_pre_epoch_parity_for_all_repository_reads(self):
        old_repository = baseline_class("analytics/data_access.py", "AnalyticsRepository")
        with price_fixture(os.environ["ANALYTICS_TEST_DATABASE_URL"], 0, 0) as (dsn, symbol_id):
            with psycopg2.connect(dsn) as conn, conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO stock_bars (symbol_id, timeframe, bar_time, open, high, low, close, volume)
                    SELECT %s, '1Day', ts::timestamptz, 100, 101, 99, 100, 195850526
                    FROM unnest(ARRAY['1969-12-31T23:59:59.123456Z',
                                      '2026-09-17T09:30:00.123456-04:00',
                                      '2026-09-18T13:30:00.999999Z']) AS ts
                """, (symbol_id,))
            before, after = old_repository(dsn), AnalyticsRepository(dsn)
            cutoff = pd.Timestamp("2026-09-17T13:30:00.123456Z")
            pairs = [
                (before.fetch_bars(symbol_id, "1Day"), after.fetch_bars(symbol_id, "1Day")),
                (before.fetch_latest_bar(symbol_id, "1Day", cutoff), after.fetch_latest_bar(symbol_id, "1Day", cutoff)),
                (before.fetch_future_bars(symbol_id, "1Day", cutoff, 1), after.fetch_future_bars(symbol_id, "1Day", cutoff, 1)),
                (before.fetch_bars_many([symbol_id], "1Day")[symbol_id], after.fetch_bars_many([symbol_id], "1Day")[symbol_id]),
                (before.fetch_bars_many([symbol_id], "1Day", cutoff, latest=True)[symbol_id],
                 after.fetch_bars_many([symbol_id], "1Day", cutoff, latest=True)[symbol_id]),
                (before.fetch_bars_many([symbol_id], "1Day", after={symbol_id: cutoff}, limit=1)[symbol_id],
                 after.fetch_bars_many([symbol_id], "1Day", after={symbol_id: cutoff}, limit=1)[symbol_id]),
            ]
            for expected, actual in pairs:
                pd.testing.assert_frame_equal(actual, expected)
