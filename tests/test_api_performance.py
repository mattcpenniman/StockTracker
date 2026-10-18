from concurrent.futures import ThreadPoolExecutor
import os
import unittest
from unittest.mock import MagicMock, Mock, patch

import pandas as pd

import app as dashboard
from analytics.config import AnalyticsSettings
from analytics import indicators
from analytics.service import AnalyticsNotFoundError, AnalyticsService
from analytics.utils import parse_asof
from analytics.io_benchmark import price_fixture
from database import get_connection
from tests.test_analytics import _fixture_bars


class SchemaInitializationTests(unittest.TestCase):
    def test_concurrent_requests_initialize_once_and_configuration_changes_reinitialize(self):
        factory = MagicMock()
        with patch.object(dashboard, "_get_conn", factory), \
             patch.object(dashboard, "_DB_INITIALIZED_FOR", None):
            with ThreadPoolExecutor(max_workers=8) as pool:
                list(pool.map(lambda _: dashboard._init_db_if_needed(), range(32)))
            self.assertEqual(factory.call_count, 1)
            with patch.object(dashboard, "DATABASE_URL", "other database"):
                dashboard._init_db_if_needed()
            self.assertEqual(factory.call_count, 2)
            with patch.object(dashboard.os, "getpid", return_value=os.getpid() + 1):
                dashboard._init_db_if_needed()
            self.assertEqual(factory.call_count, 3)

    def test_failed_commit_does_not_mark_schema_initialized(self):
        factory = MagicMock()
        factory.return_value.__exit__.side_effect = [RuntimeError("commit failed"), False]
        with patch.object(dashboard, "_get_conn", factory), \
             patch.object(dashboard, "_DB_INITIALIZED_FOR", None):
            with self.assertRaisesRegex(RuntimeError, "commit failed"):
                dashboard._init_db_if_needed()
            dashboard._init_db_if_needed()
            dashboard._init_db_if_needed()
            self.assertEqual(factory.call_count, 2)


class EventAnchorTests(unittest.TestCase):
    def repository(self, missing_intraday=False, missing_daily=False):
        daily = _fixture_bars()
        intraday = daily.iloc[-4:].copy()
        intraday["timestamp"] = pd.date_range("2023-09-16T12:00:00Z", periods=4, freq="15min")
        frames = {"1Day": daily, "15Min": intraday, "1Hour": intraday}
        if missing_intraday:
            frames["15Min"] = intraday.iloc[:0]
        if missing_daily:
            frames["1Day"] = daily.iloc[:0]
        repo = Mock()
        repo.fetch_symbol_context.return_value = {"symbol": "NVDA", "symbol_id": 1}

        def read(sid, timeframe, asof=None):
            frame = frames[timeframe]
            return frame if asof is None else frame[frame.timestamp <= asof]

        repo.fetch_bars.side_effect = read
        repo.fetch_latest_bar.side_effect = lambda *args: read(*args).tail(1)
        return repo

    def test_event_asof_matches_state_for_all_anchor_policies_without_state_calculation(self):
        for backend in ("python", "rust") if indicators._rust_feature_columns is not None else ("python",):
            for tf, cutoff in (("1Day", None), ("1Day", "2023-09-17T00:00:00Z"),
                               ("1Day", "2023-09-16"), ("1Day", "2023-09-16T12:31:00Z"),
                               ("15Min", None), ("15Min", "2023-09-16T12:31:00Z"),
                               ("1Hour", "2023-09-16T12:31:00Z")):
                for missing in (False, True) if tf == "1Day" else (False,):
                    with self.subTest(backend=backend, timeframe=tf, cutoff=cutoff, missing=missing), \
                         patch.dict(os.environ, ANALYTICS_BACKEND=backend):
                        service = AnalyticsService(self.repository(missing_intraday=missing))
                        asof = parse_asof(cutoff)
                        expected = service.get_symbol_state("NVDA", tf, asof, AnalyticsSettings())["as_of"]
                        service.repository.reset_mock()
                        with patch.object(service, "get_symbol_state", side_effect=AssertionError("redundant state")), \
                             patch.object(dashboard, "analytics_service", service), \
                             patch.object(dashboard, "_sync_current_analytics_data_if_needed"), \
                             dashboard.app.test_client() as client:
                            query = {"timeframe": tf}
                            if cutoff:
                                query["asof"] = cutoff
                            response = client.get("/api/events/NVDA", query_string=query)
                            self.assertEqual(response.status_code, 200, response.get_json())
                            self.assertEqual(response.get_json()["as_of"], expected)
                            cached = client.get("/api/events/NVDA", query_string=query)
                            self.assertEqual(cached.get_json(), response.get_json())
                        service.repository.fetch_bars.assert_called_once()

    def test_intraday_events_still_require_daily_state_anchor(self):
        service = AnalyticsService(self.repository(missing_daily=True))
        with self.assertRaisesRegex(AnalyticsNotFoundError, "1Day"):
            service.get_events("NVDA", "15Min", None, AnalyticsSettings())


@unittest.skipUnless(os.environ.get("ANALYTICS_TEST_DATABASE_URL"), "requires PostgreSQL")
class MutationEndpointTests(unittest.TestCase):
    def test_stock_upsert_chart_sync_and_coverage_preparation_in_private_schema(self):
        with price_fixture(os.environ["ANALYTICS_TEST_DATABASE_URL"], 260, 0) as (dsn, _), \
             patch.object(dashboard, "_get_conn", lambda: get_connection(dsn)), \
             patch.object(dashboard, "_fetch_alpaca_bars") as provider, \
             patch.object(dashboard, "_fetch_alpaca_latest_quote", return_value=None), \
             patch.object(dashboard, "_fetch_alpaca_latest_bar", return_value=None), \
             dashboard.app.test_client() as client:
            # A full refresh must return retained observations as well as new ones.
            def fetch(symbol, timeframe, start, end):
                with get_connection(dsn) as conn, conn.cursor() as cur:
                    cur.execute("""SELECT bar_time, open, high, low, close, volume FROM stock_bars
                        WHERE timeframe = %s AND bar_time BETWEEN %s AND %s ORDER BY bar_time""",
                        (timeframe, start, end))
                    bars = [dict(zip(("t", "o", "h", "l", "c", "v"), row)) for row in cur.fetchall()]
                for bar in bars:
                    bar["t"] = bar["t"].isoformat()
                if not any(bar["t"].startswith("2020-06-01") for bar in bars):
                    bars.append({"t": "2020-06-01T04:00:00Z", "o": 100, "h": 105, "l": 98, "c": 103, "v": 1234})
                return bars
            provider.side_effect = fetch
            created = client.post("/api/stocks", json={"symbol": "bench", "forecast_price": 110})
            self.assertEqual(created.status_code, 200, created.get_json())
            updated = client.post("/api/stocks", json={"rows": [{"symbol": "BENCH", "forecast_price": 120}]})
            self.assertEqual(updated.status_code, 200, updated.get_json())
            stored = client.get("/api/stocks?symbol=BENCH").get_json()
            self.assertEqual(stored["count"], 1)
            self.assertEqual(stored["rows"][0]["forecast_price"], 120)
            synced = client.post("/api/chart/BENCH/sync", json={"timeframe": "1Day"})
            self.assertEqual(synced.status_code, 200, synced.get_json())
            chart = client.get("/api/chart/BENCH?timeframe=1Day&limit=1").get_json()
            self.assertEqual(chart["bars"][-1]["c"], 103)
            provider.reset_mock()
            body = {"symbols": ["BENCH"], "start": "2020-05-31", "end": "2020-06-02",
                    "include_benchmarks": False}
            for _ in range(2):
                prepared = client.post("/api/coverage/prepare", json=body)
                self.assertEqual(prepared.status_code, 200, prepared.get_json())
                self.assertEqual(prepared.get_json()["errors"], [])
            # The chart refresh already verified this entire historical envelope.
            provider.assert_not_called()
