"""Independent event-time oracles and real SQL cutoff invariance (VEC-829)."""
import os
import unittest
from unittest.mock import patch

import pandas as pd
import psycopg2

from analytics.availability import available_at, start_bound
from analytics.config import AnalyticsSettings
from analytics.data_access import AnalyticsRepository
from analytics import indicators
from analytics.io_benchmark import price_fixture
from analytics.service import AnalyticsService
from analytics.utils import parse_asof


class CompletionClockTests(unittest.TestCase):
    def test_daily_dst_early_close_holiday_and_legacy_labels(self):
        # Literal UTC expectations: no fixed 24h assumption or regular-close release.
        cases = [("2024-03-08T05:00Z", "2024-03-09T05:00Z"),
                 ("2024-03-10T05:00Z", "2024-03-11T04:00Z"),
                 ("2024-11-03T04:00Z", "2024-11-04T05:00Z"),
                 ("2024-07-03T04:00Z", "2024-07-04T04:00Z"),
                 ("2024-07-03T00:00Z", "2024-07-04T04:00Z")]
        for bar, completion in cases:
            with self.subTest(bar=bar):
                self.assertEqual(available_at(bar, "1Day"), pd.Timestamp(completion))
                for cutoff, eligible in [(pd.Timestamp(completion), True),
                                         (pd.Timestamp(completion) - pd.Timedelta(microseconds=1), False)]:
                    self.assertEqual(pd.Timestamp(bar) <= start_bound(cutoff, "1Day"), eligible)
        self.assertEqual(start_bound("2024-07-04T18:00Z", "1Day"), pd.Timestamp("2024-07-03T23:59:59.999999Z"))

    def test_full_intraday_intervals_including_extended_hours_and_early_close(self):
        for tf, end in [("1Min", "13:01"), ("5Min", "13:05"), ("15Min", "13:15"), ("1Hour", "14:00")]:
            self.assertEqual(available_at("2024-07-03T13:00:00-04:00", tf),
                             pd.Timestamp(f"2024-07-03T{end}:00-04:00"))
        self.assertEqual(available_at("2024-07-03T19:45:00-04:00", "15Min"), pd.Timestamp("2024-07-04T00:00Z"))


@unittest.skipUnless(os.environ.get("ANALYTICS_TEST_DATABASE_URL"), "PostgreSQL fixture DSN required")
class StoredAvailabilityTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.fixture = price_fixture(os.environ["ANALYTICS_TEST_DATABASE_URL"], daily_count=0, intraday_count=0)
        cls.dsn, cls.sid = cls.fixture.__enter__()
        cls.addClassCleanup(cls.fixture.__exit__, None, None, None)

    def setUp(self):
        self.repo = AnalyticsRepository(self.dsn)
        self.settings = AnalyticsSettings()
        with psycopg2.connect(self.dsn) as conn, conn.cursor() as cur:
            cur.execute("DELETE FROM stock_bars")
            cur.execute("INSERT INTO symbol_metadata (symbol) VALUES ('SPY'), ('QQQ') ON CONFLICT DO NOTHING")
            cur.execute("SELECT id, symbol FROM symbol_metadata")
            self.ids = {symbol: sid for sid, symbol in cur.fetchall()}
            # Literal expected daily returns at Sep 16 10am NY: BENCH +10%,
            # SPY +20%, QQQ -10%. Today's final 900 close is unavailable.
            for symbol, closes in [("BENCH", [100, 110, 900]), ("SPY", [100, 120, 900]), ("QQQ", [100, 90, 900])]:
                sid = self.ids[symbol]
                for date, close in zip(["2026-09-14", "2026-09-15", "2026-09-16"], closes):
                    cur.execute("INSERT INTO stock_bars (symbol_id,timeframe,bar_time,open,high,low,close,volume) VALUES (%s,'1Day',%s,%s,%s,%s,%s,1000)",
                                (sid, date + "T04:00Z", close, close + 1, close - 1, close))
                for tf in ("1Min", "5Min", "15Min", "1Hour"):
                    for stamp, close in [("13:00", 130), ("13:45", 145), ("14:00", 900), ("14:15", 950)]:
                        cur.execute("INSERT INTO stock_bars (symbol_id,timeframe,bar_time,open,high,low,close,volume) VALUES (%s,%s,%s,%s,%s,%s,%s,1000)",
                                    (sid, tf, "2026-09-16T" + stamp + "Z", close, close + 1, close - 1, close))

    def test_sql_boundaries_all_transports_and_historical_anchor(self):
        sid = self.sid
        for tf, end in [("1Min", "14:01"), ("5Min", "14:05"), ("15Min", "14:15"), ("1Hour", "15:00")]:
            cutoff = pd.Timestamp(f"2026-09-16T{end}:00Z")
            for instant, expected_stamp in [(cutoff, "14:00"), (cutoff - pd.Timedelta(microseconds=1), "13:45")]:
                expected_time = pd.Timestamp(f"2026-09-16T{expected_stamp}Z")
                frames = [self.repo.fetch_latest_bar(sid, tf, instant),
                          self.repo.fetch_bars_many([sid], tf, instant, latest=True)[sid],
                          self.repo.fetch_state_bars(sid, tf, instant, latest=True).to_frame()]
                for frame in frames:
                    self.assertEqual(frame.iloc[-1].timestamp, expected_time)
        for cutoff, expected in [("2026-09-17T03:59:59.999999Z", "2026-09-15"), ("2026-09-17T04:00Z", "2026-09-16")]:
            bar = self.repo.fetch_latest_bar(sid, "1Day", pd.Timestamp(cutoff))
            self.assertEqual(bar.iloc[-1].timestamp, pd.Timestamp(expected + "T04:00Z"))
        anchors = self.repo.fetch_historical_price_anchors(sid, [pd.Timestamp("2026-09-16T14:00Z"), pd.Timestamp("2026-09-16T14:07Z")])
        self.assertEqual([p["close"] for p in anchors.values()], [145, 145])

    def _check_paths(self):
        cutoff = parse_asof("2026-09-16T14:00Z")
        results = []
        for backend in ["python"] + (["rust"] if indicators._rust_feature_columns else []):
            with patch.dict(os.environ, ANALYTICS_BACKEND=backend):
                service = AnalyticsService(self.repo)
                single = service.get_symbol_state("BENCH", "1Day", cutoff, self.settings)
                batch = service.get_batch_state(["BENCH"], "1Day", cutoff, self.settings)
                history = service.get_historical_portfolio(["BENCH"], [cutoff.isoformat()], self.settings, 1)["dates"][0]
                state = batch["states"][0]
                self.assertEqual(state, history["features"]["states"][0])
                self.assertEqual(single["returns"], state["returns"])
                self.assertAlmostEqual(state["returns"]["r_1"], .10)
                self.assertAlmostEqual(state["relative"]["rel_return_spy_1"], -.10)
                self.assertAlmostEqual(state["relative"]["rel_return_qqq_1"], .20)
                self.assertEqual(state["price"]["close"], 145)
                self.assertEqual(state["data_quality"]["indicator_bar_timestamp"], "2026-09-15T04:00:00Z")
                labels = service.get_batch_future_state(["BENCH"], "1Day", cutoff, 1)
                self.assertEqual(labels, history["labels"])
                self.assertEqual(labels["future_states"][0]["anchor_price"]["close"], 110)
                results.append(batch)
        return results

    def test_independent_state_benchmark_label_oracles_and_unavailable_data_invariance(self):
        before = self._check_paths()
        with psycopg2.connect(self.dsn) as conn, conn.cursor() as cur:
            cur.execute("UPDATE stock_bars SET open=open*7,high=high*7,low=low*7,close=close*7,volume=volume*7 WHERE (timeframe='1Day' AND bar_time >= '2026-09-16') OR (timeframe<>'1Day' AND bar_time >= '2026-09-16T14:00Z')")
            cur.execute("INSERT INTO stock_bars (symbol_id,timeframe,bar_time,open,high,low,close,volume) VALUES (%s,'1Day','2026-09-17T04:00Z',1,2,1,1,1)", (self.sid,))
        self.assertEqual(before, self._check_paths())

    def test_portfolio_http_uses_completed_anchors_and_benchmarks(self):
        import app as dashboard

        with patch.object(dashboard, "analytics_service", AnalyticsService(self.repo)), \
             patch.object(dashboard, "_portfolio_symbols", return_value=["BENCH"]), \
             dashboard.app.test_client() as client:
            response = client.get("/api/portfolio/state?asof=2026-09-16T10:07:00-04:00")
        self.assertEqual(response.status_code, 200, response.get_json())
        state = response.get_json()["states"][0]
        self.assertEqual(state["price"]["close"], 145)
        self.assertAlmostEqual(state["returns"]["r_1"], .10)
        self.assertAlmostEqual(state["relative"]["rel_return_spy_1"], -.10)
