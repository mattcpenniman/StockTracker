from __future__ import annotations

import json
import os
import unittest
from concurrent.futures import Future, ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
from threading import Barrier, Event, Lock
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
from analytics.utils import clean_json_value, parse_asof
import app as dashboard
from analytics.dashboard_benchmark import baseline_namespace, dashboard_fixture, variants
from analytics.events_benchmark import baseline_generate_events, event_fixture


class FutureLabelHorizonTests(unittest.TestCase):
    def setUp(self):
        # Competing daily and intraday data: choosing the wrong series changes
        # both the label values and the realized end date.
        self.frames = {}
        for timeframe, timestamps in (
            ("1Day", pd.bdate_range("2023-09-19T04:00:00Z", periods=25, normalize=False)),
            ("15Min", pd.date_range("2023-09-20T12:00:00Z", periods=100, freq="15min")),
        ):
            closes = np.arange(len(timestamps), dtype=float) + (100 if timeframe == "1Day" else 400)
            self.frames[timeframe] = pd.DataFrame({
                "timestamp": timestamps, "open": closes, "high": closes + 2,
                "low": closes - 2, "close": closes, "volume": 1000.0,
            })
        self.repo = Mock()
        context = {"symbol": "NVDA", "symbol_id": 1}
        self.repo.fetch_symbol_context.return_value = context
        self.repo.fetch_symbol_contexts.return_value = {"NVDA": context}

        def read(timeframe, asof=None, after=None, limit=None, latest=False):
            bars = self.frames[timeframe]
            if asof is not None:
                bars = bars[bars.timestamp <= asof]
            if after is not None:
                bars = bars[bars.timestamp > after]
            return bars.tail(1) if latest else bars.head(limit)

        self.repo.fetch_latest_bar.side_effect = lambda sid, tf, asof: read(tf, asof, latest=True)
        self.repo.fetch_future_bars.side_effect = lambda sid, tf, after, limit: read(tf, after=after, limit=limit)
        self.repo.fetch_bars_many.side_effect = lambda ids, tf, asof=None, latest=False, after=None, limit=None: {
            sid: read(tf, asof, after[sid] if after else None, limit, latest) for sid in ids
        }
        self.service = AnalyticsService(self.repo)

    def test_daily_responses_count_twenty_daily_bars_for_date_and_timed_cutoffs(self):
        for asof, anchor, end, close in (
            ("2023-09-20", "2023-09-20T04:00:00Z", "2023-10-18T04:00:00Z", 121),
            ("2023-09-20T15:00:00Z", "2023-09-20T04:00:00Z", "2023-10-18T04:00:00Z", 121),
            ("2023-09-20T11:00:00-04:00", "2023-09-20T04:00:00Z", "2023-10-18T04:00:00Z", 121),
            ("2023-09-20T00:00:00Z", "2023-09-19T04:00:00Z", "2023-10-17T04:00:00Z", 120),
        ):
            for portfolio in (False, True):
                with self.subTest(asof=asof, portfolio=portfolio), \
                     patch.object(dashboard, "analytics_service", self.service), \
                     patch.object(dashboard, "_portfolio_symbols", return_value=["NVDA", "MISSING"]), \
                     patch.object(dashboard, "_sync_market_data_window") as sync, \
                     flask_app.test_client() as client:
                    path = "/api/portfolio/futurestate" if portfolio else "/api/futurestate/NVDA"
                    response = client.get(path, query_string={"timeframe": "1D", "asof": asof, "days": 20})
                    self.assertEqual(response.status_code, 200, response.get_json())
                    payload = response.get_json()
                    if portfolio:
                        self.assertEqual(payload["count"], 1)
                        self.assertEqual(payload["errors"][0]["symbol"], "MISSING")
                        payload = payload["future_states"][0]
                    self.assertEqual(payload["as_of"], anchor)
                    for section in ("anchor_price", "window", "future_state"):
                        self.assertEqual(payload[section]["source_timeframe"], "1Day")
                    self.assertEqual(payload["window"]["future_bar_count"], 20)
                    self.assertTrue(payload["window"]["complete_window"])
                    self.assertEqual(payload["window"]["realized_end_timestamp"], end)
                    self.assertEqual(payload["window"]["target_end_timestamp"], end)
                    self.assertEqual(payload["future_state"]["price_at_horizon"], close)
                    self.assertEqual(payload["future_state"]["max_price"], close + 2)
                    sync.assert_not_called()

    def test_daily_partial_window_and_no_future_bars(self):
        partial = self.service.get_future_state("NVDA", "1Day", parse_asof("2023-10-19"), 20)
        self.assertEqual(partial["window"]["future_bar_count"], 2)
        self.assertFalse(partial["window"]["complete_window"])
        self.assertIsNone(partial["window"]["target_end_timestamp"])
        self.assertEqual(partial["window"]["realized_end_timestamp"], "2023-10-23T04:00:00Z")
        with self.assertRaisesRegex(AnalyticsNotFoundError, "No future bars.*1Day"):
            self.service.get_future_state("NVDA", "1Day", parse_asof("2023-10-23"), 20)

    def test_explicit_intraday_response_keeps_intraday_anchor_window_and_sync(self):
        with patch.object(dashboard, "analytics_service", self.service), \
             patch.object(dashboard, "_sync_market_data_window") as sync, \
             flask_app.test_client() as client:
            response = client.get("/api/futurestate/NVDA/15Min?asof=2023-09-20T15:00:00Z&days=20")
        self.assertEqual(response.status_code, 200)
        payload = response.get_json()
        for section in ("anchor_price", "window", "future_state"):
            self.assertEqual(payload[section]["source_timeframe"], "15Min")
        self.assertEqual(payload["anchor_price"]["timestamp"], "2023-09-20T15:00:00Z")
        self.assertEqual(payload["window"]["future_bar_count"], 20)
        self.assertEqual(payload["window"]["realized_end_timestamp"], "2023-09-20T20:00:00Z")
        self.assertEqual(sync.call_args.args, ("NVDA", "15Min"))

    def test_daily_thresholds_keep_side_and_first_hit_order(self):
        for side, first, stop_date, profit_date in (
            ("long", "take_profit", None, "2023-09-26T04:00:00Z"),
            ("short", "stop_loss", "2023-09-26T04:00:00Z", None),
        ):
            with self.subTest(side=side):
                payload = self.service.get_future_state("NVDA", "1Day", parse_asof("2023-09-20"), 20,
                                                        stop_loss_pct=0.05, take_profit_pct=0.05, side=side)
                state = payload["future_state"]
                self.assertEqual(state["first_threshold_hit"], first)
                self.assertEqual(state["stop_loss_hit_timestamp"], stop_date)
                self.assertEqual(state["take_profit_hit_timestamp"], profit_date)


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

    def test_exact_serialized_event_parity(self) -> None:
        baseline = baseline_generate_events()
        rng = np.random.default_rng(786)
        frames = [event_fixture(1000, density) for density in ("sparse", "dense")]
        for count in (0, 1, 14, 20, 26, 55, 200, 252, 500):
            bars = _fixture_bars(count)
            bars["close"] = 100 * np.exp(np.cumsum(rng.normal(0, 0.03, count)))
            bars["high"] = bars["close"] + 1
            bars["low"] = bars["close"] - 1
            bars["volume"] = rng.integers(0, 10000, count)
            frames.append(compute_feature_frame(bars, "1Day", self.settings))
        irregular = frames[1].iloc[:30].copy()
        irregular.loc[0:2, "timestamp"] = pd.NaT
        irregular.loc[4:6, "timestamp"] = irregular.loc[3, "timestamp"]
        irregular.loc[10, "timestamp"] = irregular.loc[20, "timestamp"]
        irregular.loc[12, ["breakout_strength", "volume_zscore"]] = np.nan
        frames.append(irregular)
        missing_timestamps = frames[1].iloc[:10].copy()
        missing_timestamps["timestamp"] = pd.NaT
        frames.append(missing_timestamps)
        for frame_idx, features in enumerate(frames):
            for confirmation in (False, True):
                settings = AnalyticsSettings(volume_confirmation=confirmation, buffer_pct=0.0)
                expected = serialize_events("BENCH", "1Day", baseline(features, settings))
                for limit in (None, -5, 0, 1, 2, 7, 18, 100, 10000):
                    with self.subTest(frame=frame_idx, confirmation=confirmation, limit=limit):
                        limited = dict(expected)
                        if limit is not None:
                            limited["events"] = expected["events"][:max(1, limit)]
                        actual = serialize_events("BENCH", "1Day", generate_events(features, settings, limit))
                        self.assertEqual(json.dumps(actual, sort_keys=True), json.dumps(limited, sort_keys=True))

    def test_simultaneous_event_ties_keep_original_bar_and_event_order(self) -> None:
        features = event_fixture(3, "dense")
        features["timestamp"] = features.loc[0, "timestamp"]
        events = generate_events(features, self.settings)
        self.assertEqual([event["event_type"] for event in events[:5]], [
            "breakdown", "new_low_20", "new_low_55", "volume_spike", "volatility_spike",
        ])
        self.assertEqual([event["event_type"] for event in events[5:15]], [
            "breakout", "price_crosses_above_sma20", "price_crosses_above_sma50",
            "price_crosses_above_sma200", "macd_bullish_cross", "rsi_enters_overbought",
            "new_high_20", "new_high_55", "volume_spike", "volatility_spike",
        ])


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
    def test_nested_numpy_scalars_are_native_and_strict_json_safe(self) -> None:
        for dtype in (np.int8, np.int16, np.int32, np.int64, np.uint8, np.uint16, np.uint32, np.uint64):
            with self.subTest(dtype=dtype):
                value = dtype(np.iinfo(dtype).max)
                cleaned = clean_json_value({"events": [{"value": value}]})
                self.assertIs(type(cleaned["events"][0]["value"]), int)
                self.assertEqual(json.loads(json.dumps(cleaned, allow_nan=False))["events"][0]["value"], int(value))
        for dtype in (np.float16, np.float32, np.float64, np.longdouble):
            with self.subTest(dtype=dtype):
                cleaned = clean_json_value((dtype(1.25), dtype("nan"), dtype("inf"), dtype("-inf")))
                self.assertIs(type(cleaned[0]), float)
                self.assertEqual(json.loads(json.dumps(cleaned, allow_nan=False)), [1.25, None, None, None])
        cleaned = clean_json_value([np.bool_(True), np.bool_(False)])
        self.assertTrue(all(type(value) is bool for value in cleaned))
        self.assertEqual(json.dumps(cleaned, allow_nan=False), "[true, false]")

    def test_json_serialization_converts_nan_to_null(self) -> None:
        payload = serialize_events(
            "NVDA",
            "1Day",
            [{"event_type": "breakout", "timestamp": pd.Timestamp("2023-01-10", tz="UTC"), "strength": float("nan")}],
        )
        self.assertIsNone(payload["events"][0]["strength"])
        self.assertEqual(payload["events"][0]["timestamp"], "2023-01-10T00:00:00Z")


@unittest.skipUnless(os.environ.get("ANALYTICS_TEST_DATABASE_URL"), "Set ANALYTICS_TEST_DATABASE_URL for historical event PostgreSQL integration")
class HistoricalEventsPostgreSQLTests(unittest.TestCase):
    def test_integer_volume_historical_limits_cold_and_cached(self) -> None:
        import psycopg2
        from analytics.data_access import AnalyticsRepository
        from analytics.io_benchmark import price_fixture

        settings = AnalyticsSettings()
        baseline = baseline_generate_events()
        with price_fixture(os.environ["ANALYTICS_TEST_DATABASE_URL"], 260, 260) as (dsn, symbol_id):
            repo = AnalyticsRepository(dsn)
            for timeframe, alias, start, freq in (
                ("1Day", "1D", "2000-01-01", "D"),
                ("15Min", "15Min", "2020-01-01", "15min"),
            ):
                asof = pd.date_range(start, periods=259, freq=freq, tz="UTC")[-1].to_pydatetime()
                with psycopg2.connect(dsn) as conn, conn.cursor() as cur:
                    cur.execute("""
                        UPDATE stock_bars SET close = 120, high = 121, volume = 195850526
                        WHERE symbol_id = %s AND timeframe = %s AND bar_time = %s
                    """, (symbol_id, timeframe, asof))
                bars = repo.fetch_bars(symbol_id, timeframe, asof)
                self.assertEqual(bars["volume"].dtype, np.dtype("int64"))
                self.assertEqual(len(bars), 259)  # The later stored bar is excluded.
                for backend in ("python", "rust") if indicators._rust_feature_columns is not None else ("python",):
                    with patch.dict(os.environ, ANALYTICS_BACKEND=backend):
                        features = compute_feature_frame(bars, timeframe, settings)
                        expected = serialize_events("BENCH", timeframe, baseline(features, settings, 20))["events"]
                        self.assertEqual([event["value"] for event in expected if event["event_type"] == "volume_spike"], [195850526])
                        for limit in (1, 20):
                            with self.subTest(timeframe=timeframe, backend=backend, limit=limit):
                                service = AnalyticsService(repo)
                                url = f"/api/events/BENCH?timeframe={alias}&asof={asof.isoformat()}&event_limit={limit}"
                                # Encode '+' in the ISO offset for the query string.
                                url = url.replace("+00:00", "Z")
                                with patch.object(dashboard, "analytics_service", service), \
                                     patch.object(repo, "fetch_bars", wraps=repo.fetch_bars) as reads, \
                                     patch.object(dashboard, "_sync_current_analytics_data_if_needed") as sync, \
                                     patch.object(dashboard, "_backfill_analytics_symbol_if_needed") as backfill, \
                                     flask_app.test_client() as client:
                                    cold = client.get(url)
                                    self.assertEqual(cold.status_code, 200, cold.get_json())
                                    self.assertGreater(reads.call_count, 0)
                                    reads.reset_mock()
                                    cached = client.get(url)
                                    reads.assert_not_called()
                                    sync.assert_not_called()
                                    backfill.assert_not_called()
                                self.assertEqual(cached.status_code, 200, cached.get_json())
                                payload = cold.get_json()
                                self.assertTrue(payload["ok"])
                                self.assertEqual(payload, cached.get_json())
                                self.assertEqual(payload["events"], expected[:limit])
                                json.dumps(payload, allow_nan=False)


class HistoricalCoverageTests(unittest.TestCase):
    def setUp(self):
        self.bars = _fixture_bars(260)
        # A Friday before the Labor Day closure; no exact-date bar is required.
        self.bars["timestamp"] = pd.bdate_range(end="2026-09-04T04:00:00Z", periods=260, normalize=False)
        self.contexts = {s: {"symbol": s, "symbol_id": i} for i, s in enumerate(("NVDA", "SPY"), 1)}
        self.repo = Mock()
        self.repo.fetch_symbol_context.side_effect = self.contexts.get
        self.repo.fetch_symbol_contexts.return_value = self.contexts
        self.repo.fetch_bars.return_value = self.bars
        self.repo.fetch_bars_many.side_effect = lambda ids, *args, **kwargs: {i: self.bars for i in ids}
        self.repo.fetch_latest_bar.return_value = self.bars.tail(1)
        self.repo.fetch_future_bars.return_value = self.bars.tail(1).assign(timestamp=pd.Timestamp("2026-09-08T04:00:00Z"))
        self.service = AnalyticsService(self.repo)

    def test_covered_closure_and_months_old_history_have_distinct_quality(self):
        for cutoff, stale in (("2026-09-04T23:00:00Z", False), ("2026-09-06T00:00:00Z", False),
                              ("2026-09-07T23:00:00Z", False), ("2026-11-01T00:00:00Z", True)):
            with self.subTest(cutoff=cutoff):
                state = self.service.get_symbol_state("NVDA", "1Day", parse_asof(cutoff), AnalyticsSettings())
                coverage = state["data_quality"]["historical_coverage"]["indicators"]
                self.assertEqual(coverage["stale"], stale)
                self.assertEqual(coverage["requested_asof"], cutoff)
                self.assertEqual(coverage["actual_bar_timestamp"], "2026-09-04T04:00:00Z")
                self.assertFalse(coverage["completeness_verified"])
                self.assertEqual(state, self.service.get_symbol_state("NVDA", "1Day", parse_asof(cutoff), AnalyticsSettings()))

    def test_batch_reports_stale_symbols_and_missing_or_stale_benchmarks(self):
        batch = self.service.get_batch_state(["NVDA", "MISSING"], "1Day", parse_asof("2026-11-01T00:00:00Z"), AnalyticsSettings())
        self.assertEqual(batch["data_quality"]["stale_symbols"], ["NVDA"])
        self.assertEqual([e["symbol"] for e in batch["errors"]], ["MISSING"])
        self.assertEqual({e["symbol"] for e in batch["benchmark_errors"]}, {"SPY", "QQQ"})
        self.assertEqual(batch["states"][0]["relative"], {})
        self.assertEqual(batch["states"][0]["data_quality"]["benchmark_coverage"]["QQQ"]["status"], "missing")

    def test_unaligned_benchmark_does_not_generate_relative_returns(self):
        older = self.bars.iloc[:-1]
        self.repo.fetch_bars_many.side_effect = lambda ids, *args, **kwargs: {i: self.bars if i == 1 else older for i in ids}
        batch = self.service.get_batch_state(["NVDA"], "1Day", parse_asof("2026-09-07T00:00:00Z"), AnalyticsSettings())
        state = batch["states"][0]
        self.assertFalse(state["data_quality"]["benchmark_coverage"]["SPY"]["aligned"])
        self.assertEqual(state["relative"], {})

    def test_historical_feature_label_pairing_exposes_anchor_and_partial_window(self):
        cutoff = parse_asof("2026-09-07T00:00:00Z")
        state = self.service.get_symbol_state("NVDA", "1Day", cutoff, AnalyticsSettings())
        label = self.service.get_future_state("NVDA", "1Day", cutoff, 2)
        self.assertEqual(state["data_quality"]["indicator_bar_timestamp"], label["anchor_price"]["timestamp"])
        self.assertEqual(state["requested_asof"], label["requested_asof"])
        self.assertEqual(state["data_quality"]["historical_coverage"]["indicators"], label["data_quality"]["historical_coverage"])
        self.assertFalse(label["data_quality"]["complete_future_window"])
        stale = self.service.get_future_state("NVDA", "1Day", parse_asof("2026-11-01"), 1)
        self.assertTrue(stale["data_quality"]["historical_coverage"]["stale"])

    def test_recent_intraday_price_does_not_mask_stale_daily_indicators(self):
        cutoff = parse_asof("2026-10-01T18:00:00Z")
        self.repo.fetch_latest_bar.return_value = self.bars.tail(1).assign(timestamp=pd.Timestamp(cutoff))
        state = self.service.get_symbol_state("NVDA", "1Day", cutoff, AnalyticsSettings())
        coverage = state["data_quality"]["historical_coverage"]
        self.assertTrue(coverage["indicators"]["stale"])
        self.assertFalse(coverage["price"]["stale"])

    def test_prepare_bounds_and_per_symbol_errors(self):
        client = flask_app.test_client()
        payload = {"symbols": ["nvda", "NVDA"], "start": "2026-01-01T00:00:00Z", "end": "2026-09-08T00:00:00Z"}
        with patch.object(dashboard, "_prepare_daily_coverage", side_effect=[{"symbol": "NVDA"}, RuntimeError("provider unavailable"), {"symbol": "QQQ"}]) as prepare:
            response = client.post("/api/coverage/prepare", json=payload)
            self.assertEqual(response.status_code, 200, response.get_json())
            self.assertEqual([c.args[0] for c in prepare.call_args_list], ["NVDA", "SPY", "QQQ"])
            self.assertEqual(response.get_json()["errors"], [{"symbol": "SPY", "error": "provider unavailable"}])
            prepare.reset_mock()
            for change in ({"symbols": list(map(str, range(51)))}, {"start": "2020-01-01"},
                           {"end": "2099-01-01"}, {"end": payload["start"]}, {"include_benchmarks": "false"}):
                with self.subTest(change=change):
                    self.assertEqual(client.post("/api/coverage/prepare", json={**payload, **change}).status_code, 400)
            prepare.assert_not_called()

    def test_prepare_fetches_only_gaps_and_reuses_verified_empty_closure(self):
        start, middle, end = map(parse_asof, ("2026-09-04T00:00:00Z", "2026-09-05T00:00:00Z", "2026-09-08T00:00:00Z"))
        with patch.object(dashboard, "_historical_price_coverage", return_value=(None, None, [(middle, end)])), \
             patch.object(dashboard, "_sync_market_data_window") as sync:
            result = dashboard._prepare_daily_coverage("NVDA", start, end)
            sync.assert_called_once_with("NVDA", "1Day", start, middle)
            self.assertEqual(result["fetched_window_count"], 1)
        with patch.object(dashboard, "_historical_price_coverage", return_value=(None, None, [(start, middle), (middle, end)])), \
             patch.object(dashboard, "_sync_market_data_window") as sync:
            result = dashboard._prepare_daily_coverage("NVDA", start, end)
            sync.assert_not_called()
            self.assertEqual(result["fetched_window_count"], 0)


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
        repo.fetch_symbol_contexts.return_value = contexts
        repo.fetch_bars_many.side_effect = lambda ids, *args, **kwargs: {sid: bars_by_id[sid] for sid in ids}
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

    def test_future_state_explicit_intraday_request_uses_15min_bars(self) -> None:
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
            "15Min",
            pd.Timestamp("2023-09-17T12:15:00Z"),
            2,
        )

        self.assertEqual(payload["timeframe"], "15Min")
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
            "15Min",
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
            "15Min",
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
        service.repository.fetch_symbol_contexts.return_value = {}
        service.get_future_state = Mock(
            side_effect=[
                {"symbol": "NVDA", "future_state": {"return_at_horizon_pct": 0.05},
                 "data_quality": {"historical_coverage": {"stale": False}}, "window": {"complete_window": True}},
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


class HistoricalStateSyncTests(unittest.TestCase):
    asof = datetime(2023, 9, 20, 15, 45, tzinfo=timezone.utc)

    def payload(self, timestamp=None, timeframe="15Min"):
        return {
            "symbol": "NVDA", "timeframe": "1Day", "as_of": "2023-09-20T15:45:00Z",
            "price": {"timestamp": (timestamp or self.asof).isoformat()},
            "data_quality": {"price_source_timeframe": timeframe,
                             "price_bar_timestamp": (timestamp or self.asof).isoformat()},
        }

    def test_five_documented_url_forms_reuse_covered_data(self):
        urls = [
            "/api/state/NVDA",
            "/api/state/NVDA?timeframe=1D&asof=2023-09-20",
            "/api/state/NVDA?timeframe=15Min&asof=2023-09-20T15:45:00Z",
            "/api/state/NVDA?timeframe=1D&asof=2023-09-20&hidets=true",
            "/api/state/NVDA/1Day?breakout_lookback=55&buffer_pct=0.005",
        ]
        def coverage(symbol, timeframe, start, end):
            return end, end + timedelta(days=1), []

        with patch.object(dashboard, "_historical_price_coverage", side_effect=coverage) as covered, \
                patch.object(dashboard, "_get_market_snapshot", return_value={}), \
                patch.object(dashboard, "_market_snapshot_needs_sync", return_value=False), \
                patch.object(dashboard, "_sync_market_data") as current_sync, \
                patch.object(dashboard, "_sync_market_data_window") as window_sync, \
                patch.object(dashboard.analytics_service, "get_symbol_state") as state:
            for url in urls:
                with self.subTest(url=url):
                    asof = parse_asof("2023-09-20") if "asof=2023-09-20&" in url or url.endswith("2023-09-20") else self.asof
                    state.return_value = self.payload(asof)
                    with flask_app.test_client() as client:
                        for _ in range(2):
                            response = client.get(url)
                            self.assertEqual(response.status_code, 200, response.get_json())
                            if "hidets=true" in url:
                                self.assertNotIn("symbol", response.get_json())
                                self.assertNotIn("timestamp", response.get_json()["price"])
                    requested_timeframe = "15Min" if "timeframe=15Min" in url else "1Day"
                    self.assertEqual(state.call_args.args[1], requested_timeframe)
                    if "asof=" in url:
                        self.assertEqual(state.call_args.args[2], asof)
                        self.assertEqual(covered.call_args.args[1], "15Min")
                        self.assertEqual(covered.call_args.args[3], asof)
                    else:
                        self.assertIsNone(state.call_args.args[2])
            window_sync.assert_not_called()
            current_sync.assert_not_called()
            settings = state.call_args.args[3]
            self.assertEqual(settings.breakout_lookback, 55)
            self.assertEqual(settings.buffer_pct, 0.005)

    def test_stale_anchor_fetches_only_missing_tail(self):
        anchor = self.asof - timedelta(days=1)
        with patch.object(dashboard, "_historical_price_coverage", return_value=(anchor, anchor, [])), \
                patch.object(dashboard, "_sync_market_data_window") as sync:
            dashboard._prepare_historical_price_anchor("NVDA", "15Min", self.asof)
        sync.assert_called_once_with("NVDA", "15Min", anchor + timedelta(minutes=15), self.asof)

    def test_stale_same_day_anchor_is_not_covered_just_because_newer_days_exist(self):
        anchor = self.asof - timedelta(hours=3)
        with patch.object(dashboard, "_historical_price_coverage", return_value=(anchor, self.asof + timedelta(days=1), [])), \
                patch.object(dashboard, "_sync_market_data_window") as sync:
            dashboard._prepare_historical_price_anchor("NVDA", "15Min", self.asof)
        sync.assert_called_once_with("NVDA", "15Min", anchor + timedelta(minutes=15), self.asof)

    def test_unfinished_delayed_bar_is_refetched(self):
        anchor = self.asof - timedelta(minutes=5)
        with patch.object(dashboard, "_utcnow", return_value=self.asof + timedelta(minutes=20)), \
                patch.object(dashboard, "CHART_DELAY_MINUTES", 20), \
                patch.object(dashboard, "_historical_price_coverage", return_value=(anchor, anchor, [])), \
                patch.object(dashboard, "_sync_market_data_window") as sync:
            for _ in range(2):
                dashboard._prepare_historical_price_anchor("NVDA", "15Min", self.asof)
        self.assertEqual(sync.call_count, 2)
        sync.assert_called_with("NVDA", "15Min", anchor, self.asof)

    def test_partial_coverage_does_not_hide_unfinished_bar_at_exact_asof(self):
        windows = [(self.asof - timedelta(days=7), self.asof - timedelta(minutes=15))]
        with patch.object(dashboard, "_utcnow", return_value=self.asof + timedelta(minutes=20)), \
                patch.object(dashboard, "CHART_DELAY_MINUTES", 20), \
                patch.object(dashboard, "_historical_price_coverage", return_value=(self.asof, self.asof, windows)), \
                patch.object(dashboard, "_sync_market_data_window") as sync:
            dashboard._prepare_historical_price_anchor("NVDA", "15Min", self.asof)
        sync.assert_called_once_with("NVDA", "15Min", self.asof, self.asof)

    def test_current_state_checks_freshness_again_after_an_unsuccessful_refresh(self):
        stale = {"bars": [{"t": "2023-09-19T20:00:00Z"}], "sync_state": {}}
        with patch.object(dashboard, "_get_market_snapshot", return_value=stale) as snapshot, \
                patch.object(dashboard, "_sync_market_data") as sync:
            dashboard._sync_current_analytics_data_if_needed("NVDA", "15Min")
            dashboard._sync_current_analytics_data_if_needed("NVDA", "15Min")
        self.assertEqual(snapshot.call_count, 4)
        self.assertEqual(sync.call_count, 4)

    def test_missing_anchor_fetches_window_and_partial_coverage_fetches_only_gaps(self):
        start = self.asof - timedelta(days=7)
        windows = [(start, start + timedelta(days=2)),
                   (start + timedelta(days=3), self.asof)]
        with patch.object(dashboard, "_historical_price_coverage", return_value=(None, None, windows)), \
                patch.object(dashboard, "_sync_market_data_window") as sync:
            dashboard._prepare_historical_price_anchor("NVDA", "15Min", self.asof)
        sync.assert_called_once_with("NVDA", "15Min", start + timedelta(days=2), start + timedelta(days=3))
        with patch.object(dashboard, "_historical_price_coverage", return_value=(None, None, [])), \
                patch.object(dashboard, "_sync_market_data_window") as sync:
            dashboard._prepare_historical_price_anchor("NVDA", "15Min", self.asof)
        sync.assert_called_once_with("NVDA", "15Min", start, self.asof)

    def test_closures_reuse_successfully_verified_empty_tail(self):
        for end in [parse_asof("2023-09-23"), parse_asof("2023-12-25")]:
            anchor = end - timedelta(days=3)
            windows = [(anchor + timedelta(minutes=15), end)]
            with self.subTest(end=end), \
                    patch.object(dashboard, "_historical_price_coverage", return_value=(anchor, anchor, windows)), \
                    patch.object(dashboard, "_sync_market_data_window") as sync, \
                    patch.object(dashboard.analytics_service, "get_symbol_state", return_value=self.payload(anchor)):
                dashboard._read_analytics_state("NVDA", "1Day", end, AnalyticsSettings())
                dashboard._read_analytics_state("NVDA", "1Day", end, AnalyticsSettings())
                sync.assert_not_called()

    def test_failed_historical_fetch_returns_error_then_retries(self):
        with patch.object(dashboard, "_historical_price_coverage", return_value=(None, None, [])), \
                patch.object(dashboard, "_sync_market_data_window", side_effect=[RuntimeError("provider down"), None]) as sync, \
                patch.object(dashboard.analytics_service, "get_symbol_state", return_value=self.payload()):
            with flask_app.test_client() as client:
                failed = client.get("/api/state/NVDA?asof=2023-09-20T15:45:00Z")
                retried = client.get("/api/state/NVDA?asof=2023-09-20T15:45:00Z")
            self.assertEqual(failed.status_code, 500)
            self.assertIn("provider down", failed.get_json()["error"])
            self.assertEqual(retried.status_code, 200)
            self.assertEqual(sync.call_count, 2)

    def test_missing_intraday_anchor_does_not_silently_return_daily_fallback(self):
        with patch.object(dashboard, "_prepare_historical_price_anchor"), \
                patch.object(dashboard, "_backfill_analytics_symbol_if_needed") as backfill, \
                patch.object(dashboard.analytics_service, "get_symbol_state", return_value=self.payload(timeframe="1Day")):
            with flask_app.test_client() as client:
                response = client.get("/api/state/NVDA?asof=2023-09-20T15:45:00Z")
        self.assertEqual(response.status_code, 404)
        backfill.assert_called_once_with("NVDA")

    def test_daily_not_found_still_backfills_and_retries(self):
        with patch.object(dashboard, "_prepare_historical_price_anchor"), \
                patch.object(dashboard, "_backfill_analytics_symbol_if_needed") as backfill, \
                patch.object(dashboard.analytics_service, "get_symbol_state", side_effect=[AnalyticsNotFoundError("no daily bars"), self.payload()]):
            with flask_app.test_client() as client:
                response = client.get("/api/state/NVDA?asof=2023-09-20T15:45:00Z")
        self.assertEqual(response.status_code, 200)
        backfill.assert_called_once_with("NVDA")

    def test_concurrent_cold_requests_share_sync(self):
        self._concurrent_cold_requests()

    def test_concurrent_cold_requests_share_failure_then_retry(self):
        self._concurrent_cold_requests(fail=True)

    def _concurrent_cold_requests(self, fail=False):
        entered, release, joined = Event(), Event(), Event()
        barrier = Barrier(6)
        count_lock = Lock()
        waiters = 0
        class ObservedFuture(Future):
            def result(self, timeout=None):
                nonlocal waiters
                with count_lock:
                    waiters += 1
                    if waiters == 5:
                        joined.set()
                return super().result(timeout)
        def provider(*args):
            entered.set()
            self.assertTrue(release.wait(5))
            if fail:
                raise RuntimeError("provider down")
        def request_state():
            barrier.wait(timeout=5)
            with flask_app.test_client() as client:
                return client.get("/api/state/NVDA?asof=2023-09-20T15:45:00Z").status_code
        with patch.object(dashboard, "_historical_price_coverage", return_value=(None, None, [])), \
                patch.object(dashboard, "_sync_market_data_window", side_effect=provider) as sync, \
                patch.object(dashboard.analytics_service, "get_symbol_state", return_value=self.payload()), \
                patch("market_sync.Future", ObservedFuture), \
                ThreadPoolExecutor(max_workers=6) as pool:
            futures = [pool.submit(request_state) for _ in range(6)]
            self.assertTrue(entered.wait(5))
            self.assertTrue(joined.wait(5))
            release.set()
            self.assertEqual([f.result(timeout=5) for f in futures], [500 if fail else 200] * 6)
            sync.assert_called_once()
            if fail:
                sync.side_effect = None
                with flask_app.test_client() as client:
                    response = client.get("/api/state/NVDA?asof=2023-09-20T15:45:00Z")
                self.assertEqual(response.status_code, 200)
                self.assertEqual(sync.call_count, 2)

    def test_window_records_only_successful_delay_bounded_coverage(self):
        conn = MagicMock()
        cur = conn.__enter__.return_value.cursor.return_value.__enter__.return_value
        now = self.asof + timedelta(minutes=20)
        with patch.object(dashboard, "_ensure_symbol_metadata", return_value=1), \
                patch.object(dashboard, "_get_conn", return_value=conn), \
                patch.object(dashboard, "_utcnow", return_value=now), \
                patch.object(dashboard, "CHART_DELAY_MINUTES", 20), \
                patch.object(dashboard, "_fetch_alpaca_bars", return_value=[]) as fetch, \
                patch.object(dashboard, "_upsert_stock_bars", return_value=0), \
                patch.object(dashboard, "_get_market_snapshot", return_value={}):
            dashboard._sync_market_data_window("NVDA", "15Min", self.asof - timedelta(days=7), now)
            self.assertEqual(fetch.call_args.args[-1], self.asof)
            inserts = [c for c in cur.execute.call_args_list if "INSERT INTO market_data_coverage" in c.args[0]]
            self.assertEqual(len(inserts), 1)
            self.assertEqual(inserts[0].args[1][-1], self.asof - timedelta(minutes=15))
            cur.reset_mock()
            fetch.side_effect = RuntimeError("provider down")
            with self.assertRaisesRegex(RuntimeError, "provider down"):
                dashboard._sync_market_data_window("NVDA", "15Min", self.asof - timedelta(days=7), now)
            self.assertFalse(any("INSERT INTO market_data_coverage" in c.args[0] for c in cur.execute.call_args_list))


if __name__ == "__main__":
    unittest.main()
