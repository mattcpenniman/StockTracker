from __future__ import annotations

import os
import unittest
from datetime import datetime, timezone
from unittest.mock import MagicMock, Mock, patch

import numpy as np
import pandas as pd

from app import app as flask_app, _market_snapshot_needs_sync, _state_payload_has_desired_price_anchor
from analytics.config import AnalyticsSettings
from analytics.indicators import compute_feature_frame
from analytics import indicators
from analytics.regimes import (
    classify_momentum_regime,
    classify_position_in_range,
    classify_signal_bias,
    classify_trend_regime,
    classify_volatility_regime,
)
from analytics.serializers import serialize_events
from analytics.service import AnalyticsNotFoundError, AnalyticsService
from analytics.signals import compute_signal_columns, generate_events
from analytics.utils import parse_asof
import app as dashboard
from analytics.dashboard_benchmark import baseline_namespace, dashboard_fixture, variants


class DashboardSnapshotTests(unittest.TestCase):
    def test_snapshot_is_bounded_chronological_and_preserves_missing_symbols(self):
        conn = MagicMock()
        cur = conn.__enter__.return_value.cursor.return_value.__enter__.return_value
        cur.fetchall.return_value = [
            ("AAPL", datetime(2026, 10, 15, tzinfo=timezone.utc), 101),
            ("AAPL", datetime(2026, 10, 16, tzinfo=timezone.utc), 102),
        ]
        with patch.object(dashboard, "_init_db_if_needed"), patch.object(dashboard, "_get_conn", return_value=conn):
            closes, latest = dashboard._daily_bar_snapshot(["aapl", "MISSING"])
        self.assertEqual(closes, {"AAPL": [101.0, 102.0], "MISSING": []})
        self.assertEqual(latest, {"AAPL": "2026-10-16T00:00:00+00:00", "MISSING": None})
        sql, params = cur.execute.call_args.args
        self.assertIn("CROSS JOIN LATERAL", sql)
        self.assertIn("LIMIT 250", sql)
        self.assertEqual(params, (["AAPL", "MISSING"],))
        cur.execute.assert_called_once()

    def test_empty_snapshot_does_not_connect(self):
        with patch.object(dashboard, "_get_conn") as connect:
            self.assertEqual(dashboard._daily_bar_snapshot([]), ({}, {}))
        connect.assert_not_called()

    def test_shared_snapshot_helpers_do_not_read_again(self):
        closes = {"AAPL": [100.0] * 250}
        with patch.object(dashboard, "_get_conn") as connect:
            self.assertEqual(dashboard._trend_states(["AAPL"], closes), {"AAPL": "Neutral"})
            self.assertEqual(dashboard._rsi_14_values(["AAPL"], closes), {"AAPL": 100.0})
            self.assertEqual(dashboard._macd_signals(["AAPL"], closes), {"AAPL": "Neutral"})
            self.assertEqual(dashboard._ma_distance_pcts(["AAPL"], {"AAPL": None}, closes),
                             {"AAPL": {"dist_ma20_pct": 0.0, "dist_ma50_pct": 0.0}})
        connect.assert_not_called()


@unittest.skipUnless(os.environ.get("ANALYTICS_TEST_DATABASE_URL"), "Set ANALYTICS_TEST_DATABASE_URL for dashboard PostgreSQL parity")
class DashboardPostgreSQLParityTests(unittest.TestCase):
    def test_full_dashboard_parity_at_warmup_boundaries_and_hidden_rows(self):
        baseline = baseline_namespace()
        with dashboard_fixture(os.environ["ANALYTICS_TEST_DATABASE_URL"], count=22, mixed=True) as dsn, \
             variants(dsn, baseline) as implementations, dashboard.app.test_client() as client:
            # Intraday data must not affect the daily snapshot or latest daily time.
            import psycopg2
            with psycopg2.connect(dsn) as conn, conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO stock_bars (symbol_id, timeframe, bar_time, open, high, low, close, volume)
                    SELECT id, '15Min', '2030-01-01', 900, 900, 900, 900, 1000 FROM symbol_metadata
                """)
            for url in ("/data", "/data?include_hidden=1", "/data?include_hidden=true"):
                payloads = {}
                for name, handler in implementations:
                    with patch.dict(dashboard.app.view_functions, data=handler):
                        response = client.get(url)
                    self.assertEqual(response.status_code, 200)
                    payloads[name] = response.get_json()
                self.assertEqual(payloads["before"], payloads["after"], url)
                self.assertEqual(len(payloads["after"]["rows"]), 15 if url == "/data" else 23)
            with psycopg2.connect(dsn) as conn, conn.cursor() as cur:
                cur.execute("DELETE FROM stock_forecasts")
            for _, handler in implementations:
                with patch.dict(dashboard.app.view_functions, data=handler):
                    self.assertEqual(client.get("/data").get_json(), {"rows": []})


def _fixture_bars(count: int = 260) -> pd.DataFrame:
    timestamps = pd.date_range("2023-01-01", periods=count, freq="D", tz="UTC")
    closes = pd.Series([100.0 + idx for idx in range(count)], dtype=float)
    highs = closes + 1.0
    lows = closes - 1.0
    opens = closes - 0.5
    volume = pd.Series([1000.0] * count, dtype=float)
    return pd.DataFrame(
        {
            "timestamp": timestamps,
            "open": opens,
            "high": highs,
            "low": lows,
            "close": closes,
            "volume": volume,
        }
    )


class IndicatorTests(unittest.TestCase):
    def setUp(self) -> None:
        self.settings = AnalyticsSettings(volume_confirmation=False, buffer_pct=0.0)

    def test_sma_ema_rsi_macd_atr_basic_correctness(self) -> None:
        bars = _fixture_bars(260)
        features = compute_feature_frame(bars, "1Day", self.settings)
        latest = features.iloc[-1]

        expected_sma20 = bars["close"].tail(20).mean()
        expected_sma50 = bars["close"].tail(50).mean()
        expected_ema12 = bars["close"].ewm(span=12, adjust=False, min_periods=12).mean().iloc[-1]
        expected_ema26 = bars["close"].ewm(span=26, adjust=False, min_periods=26).mean().iloc[-1]

        self.assertAlmostEqual(latest["sma_20"], expected_sma20, places=6)
        self.assertAlmostEqual(latest["sma_50"], expected_sma50, places=6)
        self.assertAlmostEqual(latest["ema_12"], expected_ema12, places=6)
        self.assertAlmostEqual(latest["ema_26"], expected_ema26, places=6)
        self.assertGreater(latest["rsi_14"], 99.0)
        self.assertAlmostEqual(latest["atr_14"], 2.0, places=6)
        self.assertAlmostEqual(latest["atr_14_pct"], 2.0 / bars["close"].iloc[-1], places=6)
        self.assertAlmostEqual(latest["macd"], latest["ema_12"] - latest["ema_26"], places=6)
        self.assertAlmostEqual(latest["high_252"], bars["high"].tail(252).max(), places=6)
        self.assertAlmostEqual(latest["low_252"], bars["low"].tail(252).min(), places=6)
        self.assertAlmostEqual(latest["distance_from_high_252"], (bars["close"].iloc[-1] / bars["high"].tail(252).max()) - 1.0, places=6)
        self.assertIn("return_20_over_vol_20", latest)

    def test_short_history_keeps_nulls_for_long_lookbacks(self) -> None:
        bars = _fixture_bars(10)
        features = compute_feature_frame(bars, "1Day", self.settings)
        latest = features.iloc[-1]
        self.assertTrue(pd.isna(latest["sma_20"]))
        self.assertTrue(pd.isna(latest["return_20"]))
        self.assertTrue(pd.isna(latest["atr_14"]))


class RustIndicatorTests(unittest.TestCase):
    def test_auto_falls_back_when_extension_is_unavailable(self) -> None:
        bars = _fixture_bars(30)
        with patch.object(indicators, "_rust_feature_columns", None), patch.dict(os.environ, ANALYTICS_BACKEND="auto"):
            actual = compute_feature_frame(bars, "1Day", AnalyticsSettings())
        pd.testing.assert_frame_equal(actual, indicators._compute_feature_frame_python(bars, "1Day", AnalyticsSettings()))
        with patch.object(indicators, "_rust_feature_columns", None), patch.dict(os.environ, ANALYTICS_BACKEND="rust"):
            with self.assertRaisesRegex(RuntimeError, "pip install ./rust"):
                compute_feature_frame(bars, "1Day", AnalyticsSettings())

    @unittest.skipIf(indicators._rust_feature_columns is None, "Install ./rust for native parity tests")
    def test_native_features_and_signals_match_pandas(self) -> None:
        rng = np.random.default_rng(777)
        for count in (1, 14, 20, 26, 55, 200, 252, 1200):
            bars = _fixture_bars(count)
            bars["close"] = 100 * np.exp(np.cumsum(rng.normal(0, 0.015, count)))
            bars["high"] = bars["close"] + rng.uniform(0.1, 3, count)
            bars["low"] = bars["close"] - rng.uniform(0.1, 3, count)
            bars["volume"] = rng.integers(0, 10_000_000, count).astype(float)
            # Exercise sorting and year boundaries, not just rising daily prices.
            bars = bars.sample(frac=1, random_state=777)
            for timeframe in ("1Day", "15Min"):
                for lookback in (1, 55, 1500):
                    with self.subTest(count=count, timeframe=timeframe, lookback=lookback):
                        settings = AnalyticsSettings(breakout_lookback=lookback, buffer_pct=0.005, sma_slope_lookback=7)
                        expected = indicators._compute_feature_frame_python(bars, timeframe, settings)
                        with patch.dict(os.environ, ANALYTICS_BACKEND="rust"):
                            actual = compute_feature_frame(bars, timeframe, settings)
                        pd.testing.assert_frame_equal(actual, expected, check_like=True, rtol=1e-8, atol=1e-10)
                        pd.testing.assert_frame_equal(
                            compute_signal_columns(actual, settings), compute_signal_columns(expected, settings),
                            check_like=True, rtol=1e-8, atol=1e-10,
                        )

    @unittest.skipIf(indicators._rust_feature_columns is None, "Install ./rust for native parity tests")
    def test_flat_zero_volume_and_missing_data(self) -> None:
        for scenario in ("flat", "fractional_flat", "down", "zero_volume", "missing", "missing_timestamp", "zero_price", "empty"):
            with self.subTest(scenario=scenario):
                bars = _fixture_bars(300)
                if scenario in {"flat", "fractional_flat"}:
                    bars[["open", "high", "low", "close"]] = 100.1 if scenario == "fractional_flat" else 100.0
                elif scenario == "down":
                    bars["close"] = bars["close"][::-1].to_numpy()
                elif scenario == "zero_volume":
                    bars["volume"] = 0.0
                elif scenario == "missing":
                    bars.loc[100, "volume"] = np.nan
                elif scenario == "missing_timestamp":
                    bars.loc[100, "timestamp"] = pd.NaT
                elif scenario == "zero_price":
                    bars.loc[100, "close"] = 0.0
                else:
                    bars = bars.iloc[:0]
                expected = indicators._compute_feature_frame_python(bars, "1Day", AnalyticsSettings())
                with patch.dict(os.environ, ANALYTICS_BACKEND="rust"):
                    actual = compute_feature_frame(bars, "1Day", AnalyticsSettings())
                # Native NaN and pandas object/pd.NA both serialize to JSON null.
                pd.testing.assert_frame_equal(actual.isna(), expected.isna(), check_like=True)
                for frame in (actual, expected):
                    for column in frame.select_dtypes(include="object"):
                        frame[column] = frame[column].astype("Float64").astype(float)
                pd.testing.assert_frame_equal(actual, expected, check_like=True, rtol=1e-8, atol=1e-10)
                if not bars.empty:
                    pd.testing.assert_frame_equal(
                        compute_signal_columns(actual, AnalyticsSettings()),
                        compute_signal_columns(expected, AnalyticsSettings()),
                        check_like=True, rtol=1e-8, atol=1e-10,
                    )


class SignalTests(unittest.TestCase):
    def setUp(self) -> None:
        self.settings = AnalyticsSettings(volume_confirmation=False, buffer_pct=0.0)

    def test_breakout_and_event_generation(self) -> None:
        bars = _fixture_bars(30)
        bars.loc[:, "close"] = [100.0] * 29 + [120.0]
        bars.loc[:, "high"] = bars["close"] + 1.0
        bars.loc[:, "low"] = bars["close"] - 1.0
        bars.loc[:, "open"] = bars["close"] - 0.5
        bars.loc[:, "volume"] = [1000.0] * 29 + [4000.0]

        features = compute_feature_frame(bars, "1Day", self.settings)
        signals = compute_signal_columns(features, self.settings)
        latest = signals.iloc[-1]

        self.assertTrue(bool(latest["is_breakout"]))

        events = generate_events(features, self.settings, event_limit=10)
        event_types = [event["event_type"] for event in events]
        self.assertIn("breakout", event_types)
        self.assertIn("new_high_20", event_types)

    def test_breakdown_and_volume_spike_events(self) -> None:
        bars = _fixture_bars(30)
        bars.loc[:, "close"] = [100.0] * 29 + [80.0]
        bars.loc[:, "high"] = bars["close"] + 1.0
        bars.loc[:, "low"] = bars["close"] - 1.0
        bars.loc[:, "volume"] = [1000.0] * 29 + [5000.0]

        features = compute_feature_frame(bars, "1Day", self.settings)
        events = generate_events(features, self.settings, event_limit=10)
        event_types = [event["event_type"] for event in events]
        self.assertIn("breakdown", event_types)
        self.assertIn("volume_spike", event_types)


class RegimeTests(unittest.TestCase):
    def test_regime_classification(self) -> None:
        row = pd.Series(
            {
                "close": 110.0,
                "sma_50": 105.0,
                "sma_200": 100.0,
                "rsi_14": 60.0,
                "macd_hist": 0.5,
                "volatility_ratio": 1.8,
                "range_position_20": 0.1,
                "is_breakout": False,
                "is_breakdown": False,
                "trend_strength": -0.8,
                "atr_14": 2.0,
            }
        )
        settings = AnalyticsSettings()
        self.assertEqual(classify_trend_regime(row), "uptrend")
        self.assertEqual(classify_momentum_regime(row), "bullish")
        self.assertEqual(classify_volatility_regime(row, settings), "high")
        self.assertEqual(classify_position_in_range(row), "near_low")
        self.assertEqual(classify_signal_bias(row, settings), "bullish")


class SerializationTests(unittest.TestCase):
    def test_json_serialization_converts_nan_to_null(self) -> None:
        payload = serialize_events(
            "NVDA",
            "1Day",
            [{"event_type": "breakout", "timestamp": pd.Timestamp("2023-01-10", tz="UTC"), "strength": float("nan")}],
        )
        self.assertIsNone(payload["events"][0]["strength"])
        self.assertEqual(payload["events"][0]["timestamp"], "2023-01-10T00:00:00Z")


class ServiceTests(unittest.TestCase):
    def test_service_builds_state_payload(self) -> None:
        repo = Mock()
        repo.fetch_symbol_context.return_value = {
            "symbol_id": 1,
            "symbol": "NVDA",
            "exchange": "NASDAQ",
            "asset_class": "us_equity",
            "name": "NVIDIA Corp",
            "is_active": True,
            "created_at": pd.Timestamp("2023-01-01", tz="UTC").to_pydatetime(),
            "updated_at": pd.Timestamp("2023-01-01", tz="UTC").to_pydatetime(),
            "last_synced_at": pd.Timestamp("2023-09-20", tz="UTC").to_pydatetime(),
            "last_successful_sync_at": pd.Timestamp("2023-09-20", tz="UTC").to_pydatetime(),
            "last_quote_synced_at": None,
            "last_bar_synced_at": pd.Timestamp("2023-09-20", tz="UTC").to_pydatetime(),
            "latest_bar_time": pd.Timestamp("2023-09-20", tz="UTC").to_pydatetime(),
            "latest_quote_time": None,
            "sync_status": "idle",
            "sync_error": None,
        }
        repo.fetch_bars.return_value = _fixture_bars(260)
        service = AnalyticsService(repository=repo)

        payload = service.get_symbol_state("NVDA", "1Day", None, AnalyticsSettings(volume_confirmation=False, buffer_pct=0.0))
        self.assertEqual(payload["symbol"], "NVDA")
        self.assertEqual(payload["timeframe"], "1Day")
        self.assertEqual(payload["trend"]["regime"], "uptrend")
        self.assertEqual(payload["state"]["trend"], "uptrend")
        self.assertIn("signal_bias", payload["state"])
        self.assertIn("atr_14_pct", payload["volatility"])
        self.assertIn("return_20_over_vol_20", payload["volatility"])
        self.assertIn("distance_from_high_252_pct", payload["range"])
        self.assertTrue(payload["data_quality"]["has_sufficient_history_200"])

    def test_batch_state_adds_portfolio_ranks_and_benchmark_relative_returns(self) -> None:
        repo = Mock()

        def context(symbol: str, symbol_id: int) -> dict:
            return {
                "symbol_id": symbol_id,
                "symbol": symbol,
                "exchange": "NYSE",
                "asset_class": "us_equity",
                "name": symbol,
                "is_active": True,
                "created_at": pd.Timestamp("2023-01-01", tz="UTC").to_pydatetime(),
                "updated_at": pd.Timestamp("2023-01-01", tz="UTC").to_pydatetime(),
                "last_synced_at": pd.Timestamp("2023-09-20", tz="UTC").to_pydatetime(),
                "last_successful_sync_at": pd.Timestamp("2023-09-20", tz="UTC").to_pydatetime(),
                "last_quote_synced_at": None,
                "last_bar_synced_at": pd.Timestamp("2023-09-20", tz="UTC").to_pydatetime(),
                "latest_bar_time": pd.Timestamp("2023-09-20", tz="UTC").to_pydatetime(),
                "latest_quote_time": None,
                "sync_status": "idle",
                "sync_error": None,
            }

        contexts = {
            "AAA": context("AAA", 1),
            "BBB": context("BBB", 2),
            "SPY": context("SPY", 3),
            "QQQ": context("QQQ", 4),
        }

        def bars_with_step(step: float) -> pd.DataFrame:
            bars = _fixture_bars(260)
            closes = pd.Series([100.0 + (idx * step) for idx in range(260)], dtype=float)
            bars.loc[:, "close"] = closes
            bars.loc[:, "open"] = closes - 0.5
            bars.loc[:, "high"] = closes + 1.0
            bars.loc[:, "low"] = closes - 1.0
            return bars

        bars_by_id = {
            1: bars_with_step(1.0),
            2: bars_with_step(2.0),
            3: bars_with_step(0.5),
            4: bars_with_step(1.5),
        }
        repo.fetch_symbol_context.side_effect = lambda symbol: contexts.get(str(symbol).upper())
        repo.fetch_bars.side_effect = lambda symbol_id, timeframe, asof=None, start=None: bars_by_id[symbol_id]
        service = AnalyticsService(repository=repo)

        payload = service.get_batch_state(["AAA", "BBB"], "1Day", None, AnalyticsSettings(volume_confirmation=False, buffer_pct=0.0))
        states = {state["symbol"]: state for state in payload["states"]}

        self.assertEqual(payload["benchmarks"], ["SPY", "QQQ"])
        self.assertEqual(payload["benchmark_errors"], [])
        self.assertGreater(states["BBB"]["portfolio"]["rank_return_20"], states["AAA"]["portfolio"]["rank_return_20"])
        self.assertIn("rank_rsi_14", states["AAA"]["portfolio"])
        self.assertIn("rel_return_spy_20", states["AAA"]["relative"])
        self.assertAlmostEqual(
            states["AAA"]["relative"]["rel_return_spy_20"],
            states["AAA"]["returns"]["r_20"] - service.get_symbol_state("SPY", "1Day", None, AnalyticsSettings(volume_confirmation=False, buffer_pct=0.0))["returns"]["r_20"],
            places=6,
        )

    def test_intraday_asof_uses_daily_indicators_and_intraday_price(self) -> None:
        repo = Mock()
        repo.fetch_symbol_context.return_value = {
            "symbol_id": 1,
            "symbol": "NVDA",
            "exchange": "NASDAQ",
            "asset_class": "us_equity",
            "name": "NVIDIA Corp",
            "is_active": True,
            "created_at": pd.Timestamp("2023-01-01", tz="UTC").to_pydatetime(),
            "updated_at": pd.Timestamp("2023-01-01", tz="UTC").to_pydatetime(),
            "last_synced_at": pd.Timestamp("2023-09-20", tz="UTC").to_pydatetime(),
            "last_successful_sync_at": pd.Timestamp("2023-09-20", tz="UTC").to_pydatetime(),
            "last_quote_synced_at": None,
            "last_bar_synced_at": pd.Timestamp("2023-09-20", tz="UTC").to_pydatetime(),
            "latest_bar_time": pd.Timestamp("2023-09-20", tz="UTC").to_pydatetime(),
            "latest_quote_time": None,
            "sync_status": "idle",
            "sync_error": None,
        }
        daily_bars = _fixture_bars(260)
        intraday_bars = pd.DataFrame(
            {
                "timestamp": pd.to_datetime(["2023-09-17T15:45:00Z", "2023-09-17T16:00:00Z"], utc=True),
                "open": [360.0, 399.0],
                "high": [361.0, 401.0],
                "low": [359.0, 398.5],
                "close": [360.5, 400.0],
                "volume": [1000.0, 1200.0],
            }
        )
        repo.fetch_bars.return_value = daily_bars
        repo.fetch_latest_bar.return_value = intraday_bars.tail(1)
        service = AnalyticsService(repository=repo)

        payload = service.get_symbol_state("NVDA", "15Min", pd.Timestamp("2023-09-17T16:00:00Z"), AnalyticsSettings(volume_confirmation=False, buffer_pct=0.0))
        expected_sma20 = daily_bars["close"].tail(20).mean()

        self.assertEqual(payload["timeframe"], "15Min")
        self.assertEqual(payload["price"]["close"], 400.0)
        self.assertEqual(payload["price"]["open"], 399.0)
        self.assertEqual(payload["price"]["high"], 401.0)
        self.assertEqual(payload["price"]["low"], 398.5)
        self.assertEqual(payload["price"]["timestamp"], "2023-09-17T16:00:00Z")
        self.assertEqual(payload["price"]["source_timeframe"], "15Min")
        self.assertEqual(payload["as_of"], "2023-09-17T16:00:00Z")
        self.assertAlmostEqual(payload["trend"]["sma_20"], expected_sma20, places=6)
        self.assertAlmostEqual(payload["range"]["distance_from_sma_20_pct"], (400.0 / expected_sma20) - 1.0, places=6)
        self.assertEqual(payload["data_quality"]["indicator_source_timeframe"], "1Day")
        self.assertEqual(payload["data_quality"]["price_source_timeframe"], "15Min")
        self.assertEqual(payload["data_quality"]["indicator_bar_timestamp"], daily_bars.iloc[-1]["timestamp"].isoformat().replace("+00:00", "Z"))
        self.assertEqual(payload["data_quality"]["price_bar_timestamp"], "2023-09-17T16:00:00Z")

    def test_daily_request_with_intraday_asof_uses_15min_anchor_price(self) -> None:
        repo = Mock()
        repo.fetch_symbol_context.return_value = {
            "symbol_id": 1,
            "symbol": "NVDA",
            "exchange": "NASDAQ",
            "asset_class": "us_equity",
            "name": "NVIDIA Corp",
            "is_active": True,
            "created_at": pd.Timestamp("2023-01-01", tz="UTC").to_pydatetime(),
            "updated_at": pd.Timestamp("2023-01-01", tz="UTC").to_pydatetime(),
            "last_synced_at": pd.Timestamp("2023-09-20", tz="UTC").to_pydatetime(),
            "last_successful_sync_at": pd.Timestamp("2023-09-20", tz="UTC").to_pydatetime(),
            "last_quote_synced_at": None,
            "last_bar_synced_at": pd.Timestamp("2023-09-20", tz="UTC").to_pydatetime(),
            "latest_bar_time": pd.Timestamp("2023-09-20", tz="UTC").to_pydatetime(),
            "latest_quote_time": None,
            "sync_status": "idle",
            "sync_error": None,
        }
        daily_bars = _fixture_bars(260)
        intraday_bars = pd.DataFrame(
            {
                "timestamp": pd.to_datetime(["2023-09-17T12:00:00Z", "2023-09-17T12:15:00Z"], utc=True),
                "open": [360.0, 399.0],
                "high": [361.0, 401.0],
                "low": [359.0, 398.5],
                "close": [360.5, 400.0],
                "volume": [1000.0, 1200.0],
            }
        )
        repo.fetch_bars.return_value = daily_bars
        repo.fetch_latest_bar.return_value = intraday_bars.tail(1)
        service = AnalyticsService(repository=repo)

        payload = service.get_symbol_state("NVDA", "1Day", pd.Timestamp("2023-09-17T12:15:00Z"), AnalyticsSettings(volume_confirmation=False, buffer_pct=0.0))
        self.assertEqual(payload["timeframe"], "1Day")
        self.assertEqual(payload["price"]["close"], 400.0)
        self.assertEqual(payload["price"]["source_timeframe"], "15Min")
        self.assertEqual(payload["data_quality"]["indicator_source_timeframe"], "1Day")
        self.assertEqual(payload["data_quality"]["price_source_timeframe"], "15Min")
        self.assertEqual(payload["price"]["timestamp"], "2023-09-17T12:15:00Z")

    def test_service_builds_future_state_payload(self) -> None:
        repo = Mock()
        repo.fetch_symbol_context.return_value = {
            "symbol_id": 1,
            "symbol": "NVDA",
            "exchange": "NASDAQ",
            "asset_class": "us_equity",
            "name": "NVIDIA Corp",
            "is_active": True,
            "created_at": pd.Timestamp("2023-01-01", tz="UTC").to_pydatetime(),
            "updated_at": pd.Timestamp("2023-01-01", tz="UTC").to_pydatetime(),
            "last_synced_at": pd.Timestamp("2023-09-20", tz="UTC").to_pydatetime(),
            "last_successful_sync_at": pd.Timestamp("2023-09-20", tz="UTC").to_pydatetime(),
            "last_quote_synced_at": None,
            "last_bar_synced_at": pd.Timestamp("2023-09-20", tz="UTC").to_pydatetime(),
            "latest_bar_time": pd.Timestamp("2023-09-20", tz="UTC").to_pydatetime(),
            "latest_quote_time": None,
            "sync_status": "idle",
            "sync_error": None,
        }
        anchor_and_history = _fixture_bars(30)
        future = pd.DataFrame(
            {
                "timestamp": pd.to_datetime(
                    [
                        "2023-01-31T00:00:00Z",
                        "2023-02-01T00:00:00Z",
                        "2023-02-02T00:00:00Z",
                        "2023-02-03T00:00:00Z",
                        "2023-02-06T00:00:00Z",
                        "2023-02-07T00:00:00Z",
                    ],
                    utc=True,
                ),
                "open": [130.0, 131.0, 132.0, 133.0, 134.0, 135.0],
                "high": [132.0, 140.0, 136.0, 137.0, 138.0, 150.0],
                "low": [129.0, 128.0, 127.0, 130.0, 133.0, 132.0],
                "close": [131.0, 135.0, 133.0, 136.0, 137.0, 149.0],
                "volume": [1000.0] * 6,
            }
        )
        repo.fetch_latest_bar.return_value = anchor_and_history.tail(1)
        repo.fetch_future_bars.return_value = future.head(5)
        service = AnalyticsService(repository=repo)

        payload = service.get_future_state("NVDA", "1Day", pd.Timestamp("2023-01-30", tz="UTC"), 5)
        self.assertEqual(payload["symbol"], "NVDA")
        self.assertEqual(payload["horizon_days"], 5)
        self.assertEqual(payload["future_state"]["max_price"], 140.0)
        self.assertEqual(payload["future_state"]["min_price"], 127.0)
        self.assertEqual(payload["future_state"]["price_at_horizon"], 137.0)
        self.assertAlmostEqual(payload["future_state"]["max_return_pct"], (140.0 / 129.0) - 1.0, places=6)
        self.assertEqual(payload["window"]["future_bar_count"], 5)
        self.assertTrue(payload["window"]["complete_window"])
        self.assertEqual(payload["future_state"]["price_at_horizon_timestamp"], "2023-02-06T00:00:00Z")
        self.assertEqual(payload["window"]["target_end_timestamp"], "2023-02-06T00:00:00Z")

    def test_future_state_daily_request_with_intraday_asof_uses_15min_bars(self) -> None:
        repo = Mock()
        repo.fetch_symbol_context.return_value = {
            "symbol_id": 1,
            "symbol": "NVDA",
            "exchange": "NASDAQ",
            "asset_class": "us_equity",
            "name": "NVIDIA Corp",
            "is_active": True,
            "created_at": pd.Timestamp("2023-01-01", tz="UTC").to_pydatetime(),
            "updated_at": pd.Timestamp("2023-01-01", tz="UTC").to_pydatetime(),
            "last_synced_at": pd.Timestamp("2023-09-20", tz="UTC").to_pydatetime(),
            "last_successful_sync_at": pd.Timestamp("2023-09-20", tz="UTC").to_pydatetime(),
            "last_quote_synced_at": None,
            "last_bar_synced_at": pd.Timestamp("2023-09-20", tz="UTC").to_pydatetime(),
            "latest_bar_time": pd.Timestamp("2023-09-20", tz="UTC").to_pydatetime(),
            "latest_quote_time": None,
            "sync_status": "idle",
            "sync_error": None,
        }
        intraday_anchor_bars = pd.DataFrame(
            {
                "timestamp": pd.to_datetime(
                    ["2023-09-17T12:00:00Z", "2023-09-17T12:15:00Z"],
                    utc=True,
                ),
                "open": [360.0, 399.0],
                "high": [361.0, 401.0],
                "low": [359.0, 398.5],
                "close": [360.5, 400.0],
                "volume": [1000.0, 1200.0],
            }
        )
        intraday_future_bars = pd.DataFrame(
            {
                "timestamp": pd.to_datetime(
                    [
                        "2023-09-17T12:30:00Z",
                        "2023-09-17T12:45:00Z",
                        "2023-09-17T13:00:00Z",
                    ],
                    utc=True,
                ),
                "open": [400.0, 404.0, 402.0],
                "high": [405.0, 406.0, 407.0],
                "low": [399.0, 401.0, 400.0],
                "close": [404.0, 402.0, 406.0],
                "volume": [900.0, 800.0, 1000.0],
            }
        )
        repo.fetch_latest_bar.return_value = intraday_anchor_bars.tail(1)
        repo.fetch_future_bars.return_value = intraday_future_bars.head(2)
        service = AnalyticsService(repository=repo)

        payload = service.get_future_state(
            "NVDA",
            "1Day",
            pd.Timestamp("2023-09-17T12:15:00Z"),
            2,
        )

        self.assertEqual(payload["timeframe"], "1Day")
        self.assertEqual(payload["anchor_price"]["close"], 400.0)
        self.assertEqual(payload["anchor_price"]["timestamp"], "2023-09-17T12:15:00Z")
        self.assertEqual(payload["anchor_price"]["source_timeframe"], "15Min")
        self.assertEqual(payload["window"]["source_timeframe"], "15Min")
        self.assertEqual(payload["future_state"]["source_timeframe"], "15Min")
        self.assertEqual(payload["future_state"]["max_price"], 406.0)
        self.assertEqual(payload["future_state"]["max_price_timestamp"], "2023-09-17T12:45:00Z")
        self.assertEqual(payload["future_state"]["price_at_horizon"], 402.0)
        self.assertEqual(payload["future_state"]["price_at_horizon_timestamp"], "2023-09-17T12:45:00Z")

    def test_future_state_reports_stop_loss_and_take_profit_hit_timestamps(self) -> None:
        repo = Mock()
        repo.fetch_symbol_context.return_value = {
            "symbol_id": 1,
            "symbol": "NVDA",
            "exchange": "NASDAQ",
            "asset_class": "us_equity",
            "name": "NVIDIA Corp",
            "is_active": True,
            "created_at": pd.Timestamp("2023-01-01", tz="UTC").to_pydatetime(),
            "updated_at": pd.Timestamp("2023-01-01", tz="UTC").to_pydatetime(),
            "last_synced_at": pd.Timestamp("2023-09-20", tz="UTC").to_pydatetime(),
            "last_successful_sync_at": pd.Timestamp("2023-09-20", tz="UTC").to_pydatetime(),
            "last_quote_synced_at": None,
            "last_bar_synced_at": pd.Timestamp("2023-09-20", tz="UTC").to_pydatetime(),
            "latest_bar_time": pd.Timestamp("2023-09-20", tz="UTC").to_pydatetime(),
            "latest_quote_time": None,
            "sync_status": "idle",
            "sync_error": None,
        }
        intraday_anchor_bars = pd.DataFrame(
            {
                "timestamp": pd.to_datetime(["2023-09-17T12:15:00Z"], utc=True),
                "open": [399.0],
                "high": [401.0],
                "low": [398.5],
                "close": [400.0],
                "volume": [1200.0],
            }
        )
        intraday_future_bars = pd.DataFrame(
            {
                "timestamp": pd.to_datetime(
                    [
                        "2023-09-17T12:30:00Z",
                        "2023-09-17T12:45:00Z",
                        "2023-09-17T13:00:00Z",
                    ],
                    utc=True,
                ),
                "open": [400.0, 403.0, 381.0],
                "high": [404.0, 421.0, 382.0],
                "low": [399.0, 402.0, 379.0],
                "close": [403.0, 420.0, 380.0],
                "volume": [900.0, 800.0, 1000.0],
            }
        )
        repo.fetch_latest_bar.return_value = intraday_anchor_bars.tail(1)
        repo.fetch_future_bars.return_value = intraday_future_bars
        service = AnalyticsService(repository=repo)

        payload = service.get_future_state(
            "NVDA",
            "1Day",
            pd.Timestamp("2023-09-17T12:15:00Z"),
            3,
            stop_loss_pct=0.05,
            take_profit_pct=0.05,
        )

        self.assertEqual(payload["future_state"]["take_profit_price"], 420.0)
        self.assertEqual(payload["future_state"]["take_profit_hit_timestamp"], "2023-09-17T12:45:00Z")
        self.assertEqual(payload["future_state"]["stop_loss_price"], 380.0)
        self.assertEqual(payload["future_state"]["stop_loss_hit_timestamp"], "2023-09-17T13:00:00Z")
        self.assertEqual(payload["future_state"]["first_threshold_hit"], "take_profit")
        self.assertEqual(payload["future_state"]["first_threshold_hit_timestamp"], "2023-09-17T12:45:00Z")

    def test_future_state_short_side_flips_stop_loss_and_take_profit_logic(self) -> None:
        repo = Mock()
        repo.fetch_symbol_context.return_value = {
            "symbol_id": 1,
            "symbol": "NVDA",
            "exchange": "NASDAQ",
            "asset_class": "us_equity",
            "name": "NVIDIA Corp",
            "is_active": True,
            "created_at": pd.Timestamp("2023-01-01", tz="UTC").to_pydatetime(),
            "updated_at": pd.Timestamp("2023-01-01", tz="UTC").to_pydatetime(),
            "last_synced_at": pd.Timestamp("2023-09-20", tz="UTC").to_pydatetime(),
            "last_successful_sync_at": pd.Timestamp("2023-09-20", tz="UTC").to_pydatetime(),
            "last_quote_synced_at": None,
            "last_bar_synced_at": pd.Timestamp("2023-09-20", tz="UTC").to_pydatetime(),
            "latest_bar_time": pd.Timestamp("2023-09-20", tz="UTC").to_pydatetime(),
            "latest_quote_time": None,
            "sync_status": "idle",
            "sync_error": None,
        }
        intraday_anchor_bars = pd.DataFrame(
            {
                "timestamp": pd.to_datetime(["2023-09-17T12:15:00Z"], utc=True),
                "open": [399.0],
                "high": [401.0],
                "low": [398.5],
                "close": [400.0],
                "volume": [1200.0],
            }
        )
        intraday_future_bars = pd.DataFrame(
            {
                "timestamp": pd.to_datetime(
                    [
                        "2023-09-17T12:30:00Z",
                        "2023-09-17T12:45:00Z",
                        "2023-09-17T13:00:00Z",
                    ],
                    utc=True,
                ),
                "open": [400.0, 378.0, 418.0],
                "high": [402.0, 379.0, 421.0],
                "low": [399.0, 379.0, 417.0],
                "close": [401.0, 380.0, 420.0],
                "volume": [900.0, 800.0, 1000.0],
            }
        )
        repo.fetch_latest_bar.return_value = intraday_anchor_bars.tail(1)
        repo.fetch_future_bars.return_value = intraday_future_bars
        service = AnalyticsService(repository=repo)

        payload = service.get_future_state(
            "NVDA",
            "1Day",
            pd.Timestamp("2023-09-17T12:15:00Z"),
            3,
            stop_loss_pct=0.05,
            take_profit_pct=0.05,
            side="short",
        )

        self.assertEqual(payload["side"], "short")
        self.assertEqual(payload["future_state"]["side"], "short")
        self.assertEqual(payload["future_state"]["take_profit_price"], 380.0)
        self.assertEqual(payload["future_state"]["take_profit_hit_timestamp"], "2023-09-17T12:45:00Z")
        self.assertEqual(payload["future_state"]["stop_loss_price"], 420.0)
        self.assertEqual(payload["future_state"]["stop_loss_hit_timestamp"], "2023-09-17T13:00:00Z")
        self.assertEqual(payload["future_state"]["first_threshold_hit"], "take_profit")
        self.assertEqual(payload["future_state"]["first_threshold_hit_timestamp"], "2023-09-17T12:45:00Z")

    def test_service_builds_batch_future_state_payload(self) -> None:
        service = AnalyticsService(repository=Mock())
        service.get_future_state = Mock(
            side_effect=[
                {"symbol": "NVDA", "future_state": {"return_at_horizon_pct": 0.05}},
                AnalyticsNotFoundError("Symbol 'MSFT' was not found in symbol_metadata."),
            ]
        )

        payload = service.get_batch_future_state(
            ["NVDA", "MSFT"],
            "1Day",
            pd.Timestamp("2023-09-20", tz="UTC"),
            20,
        )

        self.assertEqual(payload["timeframe"], "1Day")
        self.assertEqual(payload["requested_asof"], "2023-09-20T00:00:00Z")
        self.assertEqual(payload["horizon_days"], 20)
        self.assertEqual(len(payload["future_states"]), 1)
        self.assertEqual(payload["future_states"][0]["symbol"], "NVDA")
        self.assertEqual(payload["errors"][0]["symbol"], "MSFT")


class StateEndpointHelperTests(unittest.TestCase):

    def test_market_snapshot_needs_sync_when_latest_bar_is_stale_despite_fresh_sync(self) -> None:
        snapshot = {
            "bars": [{"t": "2026-03-27T04:00:00Z"}],
            "sync_state": {"last_successful_sync_at": "2026-06-09T00:00:00Z"},
        }

        with patch("app._utcnow", return_value=datetime(2026, 6, 9, tzinfo=timezone.utc)):
            self.assertTrue(_market_snapshot_needs_sync(snapshot, 5))

    @patch("app.analytics_service")
    @patch("app._sync_current_analytics_data_if_needed")
    def test_current_state_endpoint_refreshes_before_reading_payload(self, mock_sync, mock_service) -> None:
        mock_service.get_symbol_state.return_value = {
            "symbol": "NVDA",
            "timeframe": "1Day",
            "as_of": "2026-06-08T04:00:00Z",
            "data_quality": {},
            "price": {},
        }

        with flask_app.test_client() as client:
            response = client.get("/api/state/NVDA")

        self.assertEqual(response.status_code, 200)
        mock_sync.assert_called_once_with("NVDA", "1Day")
        self.assertTrue(mock_service.get_symbol_state.called)

    @patch("app.analytics_service")
    @patch("app._read_df")
    def test_portfolio_state_endpoint_uses_visible_portfolio_symbols(self, mock_read_df, mock_service) -> None:
        mock_read_df.return_value = pd.DataFrame(
            {
                "symbol": ["NVDA", "MSFT", "TSLA"],
                "hidden": [False, True, False],
            }
        )
        mock_service.get_batch_state.return_value = {
            "timeframe": "1Day",
            "requested_asof": "2023-09-20T23:59:59.999999Z",
            "states": [{"symbol": "NVDA"}, {"symbol": "TSLA"}],
            "errors": [],
        }

        with flask_app.test_client() as client:
            response = client.get("/api/portfolio/state?timeframe=1D&asof=2023-09-20")

        payload = response.get_json()
        self.assertEqual(response.status_code, 200)
        self.assertTrue(payload["ok"])
        self.assertEqual(payload["portfolio_symbol_count"], 2)
        self.assertEqual(payload["count"], 2)
        mock_service.get_batch_state.assert_called_once()
        self.assertEqual(mock_service.get_batch_state.call_args.args[0], ["NVDA", "TSLA"])

    @patch("app.analytics_service")
    @patch("app._read_df")
    def test_portfolio_future_state_endpoint_requires_asof_and_days(self, mock_read_df, mock_service) -> None:
        mock_read_df.return_value = pd.DataFrame({"symbol": ["NVDA"], "hidden": [False]})

        with flask_app.test_client() as client:
            missing_days = client.get("/api/portfolio/futurestate?asof=2023-09-20")
            missing_asof = client.get("/api/portfolio/futurestate?days=20")

        self.assertEqual(missing_days.status_code, 400)
        self.assertEqual(missing_asof.status_code, 400)
        self.assertFalse(mock_service.get_batch_future_state.called)

    @patch("app.analytics_service")
    @patch("app._read_df")
    def test_portfolio_future_state_endpoint_uses_portfolio_symbols(self, mock_read_df, mock_service) -> None:
        mock_read_df.return_value = pd.DataFrame(
            {
                "symbol": ["NVDA", "MSFT"],
                "hidden": [False, True],
            }
        )
        mock_service.get_batch_future_state.return_value = {
            "timeframe": "1Day",
            "requested_asof": "2023-09-20T23:59:59.999999Z",
            "horizon_days": 20,
            "side": "long",
            "future_states": [{"symbol": "NVDA"}],
            "errors": [],
        }

        with flask_app.test_client() as client:
            response = client.get("/api/portfolio/futurestate?timeframe=1D&asof=2023-09-20&days=20")

        payload = response.get_json()
        self.assertEqual(response.status_code, 200)
        self.assertTrue(payload["ok"])
        self.assertEqual(payload["portfolio_symbol_count"], 1)
        self.assertEqual(payload["count"], 1)
        mock_service.get_batch_future_state.assert_called_once()
        self.assertEqual(mock_service.get_batch_future_state.call_args.args[:4], (["NVDA"], "1Day", parse_asof("2023-09-20"), 20))

    def test_intraday_anchor_accepts_same_day_bar(self) -> None:
        payload = {
            "price": {"timestamp": "2024-08-23T20:00:00Z"},
            "data_quality": {
                "price_source_timeframe": "15Min",
                "price_bar_timestamp": "2024-08-23T20:00:00Z",
            },
        }

        self.assertTrue(
            _state_payload_has_desired_price_anchor(
                payload,
                "15Min",
                pd.Timestamp("2024-08-23T21:03:00Z"),
            )
        )

    def test_intraday_anchor_rejects_stale_prior_day_bar(self) -> None:
        payload = {
            "price": {"timestamp": "2024-08-22T20:00:00Z"},
            "data_quality": {
                "price_source_timeframe": "15Min",
                "price_bar_timestamp": "2024-08-22T20:00:00Z",
            },
        }

        self.assertFalse(
            _state_payload_has_desired_price_anchor(
                payload,
                "15Min",
                pd.Timestamp("2024-08-23T21:03:00Z"),
            )
        )


if __name__ == "__main__":
    unittest.main()
