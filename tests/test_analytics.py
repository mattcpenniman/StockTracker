from __future__ import annotations

import unittest
from datetime import timezone
from unittest.mock import Mock

import pandas as pd

from app import _state_payload_has_desired_price_anchor
from analytics.config import AnalyticsSettings
from analytics.indicators import compute_feature_frame
from analytics.regimes import (
    classify_momentum_regime,
    classify_position_in_range,
    classify_signal_bias,
    classify_trend_regime,
    classify_volatility_regime,
)
from analytics.serializers import serialize_events
from analytics.service import AnalyticsService
from analytics.signals import compute_signal_columns, generate_events


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
        self.assertAlmostEqual(latest["macd"], latest["ema_12"] - latest["ema_26"], places=6)

    def test_short_history_keeps_nulls_for_long_lookbacks(self) -> None:
        bars = _fixture_bars(10)
        features = compute_feature_frame(bars, "1Day", self.settings)
        latest = features.iloc[-1]
        self.assertTrue(pd.isna(latest["sma_20"]))
        self.assertTrue(pd.isna(latest["return_20"]))
        self.assertTrue(pd.isna(latest["atr_14"]))


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
        self.assertTrue(payload["data_quality"]["has_sufficient_history_200"])

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
        repo.fetch_bars.side_effect = [daily_bars, intraday_bars]
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
        repo.fetch_bars.side_effect = [daily_bars, intraday_bars]
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
        repo.fetch_bars.side_effect = [anchor_and_history, future]
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
        repo.fetch_bars.side_effect = [intraday_anchor_bars, intraday_future_bars]
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


class StateEndpointHelperTests(unittest.TestCase):
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
