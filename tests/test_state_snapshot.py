from collections import Counter
from dataclasses import replace
import os
import unittest
from unittest.mock import MagicMock, patch

import numpy as np
import pandas as pd

import app as dashboard
from analytics import indicators
from analytics.config import AnalyticsSettings
from analytics.data_access import AnalyticsRepository
from analytics.service import AnalyticsService
from analytics.snapshot_benchmark import ReferenceService
from analytics.state_bars import StateBars
from analytics.utils import clean_json_value, parse_asof
from tests.test_historical import HistoryRepository


def typed(frame):
    timestamps = frame.timestamp.astype("datetime64[us, UTC]").astype("int64")
    return StateBars.from_rows(list(zip(timestamps, *(frame[c] for c in ("open", "high", "low", "close", "volume")))))


class TypedHistoryRepository(HistoryRepository):
    def fetch_state_bars_many(self, ids, tf, asof=None, *, latest=False):
        return {sid: typed(frame) for sid, frame in self.fetch_bars_many(ids, tf, asof, latest=latest).items()}

    def fetch_state_bars(self, sid, tf, asof=None, *, latest=False):
        return self.fetch_state_bars_many([sid], tf, asof, latest=latest)[sid]


class SnapshotTests(unittest.TestCase):
    def test_typed_transport_timestamp_and_large_integer_volume_precision(self):
        repo = AnalyticsRepository()
        connection = MagicMock()
        cur = connection.__enter__.return_value.cursor.return_value.__enter__.return_value
        timestamp = pd.Timestamp("2024-12-31T23:59:59.999999Z")
        micros = int(timestamp.as_unit("us").asm8.astype(np.int64))
        row = (micros, 100., 102., 98., 101., 2 ** 60 + 1)
        cur.fetchall.return_value = [(7, *row)]
        with patch.object(repo, "get_conn", return_value=connection):
            bars = repo.fetch_state_bars(7, "1Day", timestamp)
        self.assertEqual(bars.latest()["timestamp"], timestamp)
        self.assertEqual(bars.latest()["volume"], 2 ** 60 + 1)
        self.assertEqual(bars.native_inputs()[-1].tolist(), [2024])
        pd.testing.assert_frame_equal(bars.to_frame(), repo._bar_frame([row], timestamp_unit="us"), check_dtype=False)

    def test_randomized_snapshot_matches_every_full_frame_column(self):
        rng = np.random.default_rng(809)
        settings_list = [AnalyticsSettings(), replace(AnalyticsSettings(), breakout_lookback=1, sma_slope_lookback=1),
                         replace(AnalyticsSettings(), breakout_lookback=1500, sma_slope_lookback=73, buffer_pct=.02,
                                 volume_multiple=.8, volume_confirmation=False, event_limit=77, rsi_overbought=65,
                                 rsi_oversold=35, volume_zscore_threshold=1.1, volatility_spike_multiple=1.2,
                                 volatility_regime_low_multiple=.6, volatility_regime_high_multiple=1.8)]
        for backend in ["python"] + (["rust"] if indicators._rust_feature_columns else []):
            with patch.dict(os.environ, ANALYTICS_BACKEND=backend):
                for count in (1, 14, 20, 26, 55, 60, 200, 252, 1200, 5000):
                    close = 100 * np.exp(np.cumsum(rng.normal(0, .015, count)))
                    frame = pd.DataFrame(dict(timestamp=pd.date_range("2023-12-20T23:59:59.123456Z", periods=count),
                        open=close - .5, high=close + rng.uniform(.1, 3, count), low=close - rng.uniform(.1, 3, count),
                        close=close, volume=rng.integers(0, 10_000_000, count)))
                    for settings in settings_list:
                        with self.subTest(backend=backend, count=count, settings=settings):
                            snapshot = indicators.compute_feature_snapshot(typed(frame), "1Day", settings)
                            expected = indicators.compute_feature_frame(frame, "1Day", settings).iloc[-1].to_dict()
                            # Same-backend transport/output parity is exact, including
                            # JSON nulls; cross-backend tolerance is tested separately.
                            self.assertEqual(clean_json_value(snapshot), clean_json_value(expected))
                            if backend == "rust":
                                oracle = indicators._compute_feature_frame_python(frame, "1Day", settings).iloc[-1].to_dict()
                                for key, value in snapshot.items():
                                    if key != "timestamp":
                                        left, right = clean_json_value(value), clean_json_value(oracle[key])
                                        if left is None or right is None:
                                            self.assertEqual(left, right, key)
                                        else:
                                            np.testing.assert_allclose(left, right, rtol=1e-8, atol=1e-10, err_msg=key)

    def test_flat_zero_missing_nonfinite_and_python_fallback_parity(self):
        base = HistoryRepository().frames[0, "1Day"].copy()
        cases = []
        flat = base.copy()
        flat[["open", "high", "low", "close"]] = 100.1
        cases.append(flat)
        for col, value in (("volume", 0), ("close", 0.), ("close", -1.), ("close", np.nan),
                           ("high", np.inf), ("low", -np.inf), ("volume", None)):
            frame = base.copy()
            frame.loc[20:, col] = value
            cases.append(frame)
        for backend in ["python"] + (["rust"] if indicators._rust_feature_columns else []):
            with patch.dict(os.environ, ANALYTICS_BACKEND=backend):
                for frame in cases:
                    expected = indicators.compute_feature_frame(frame, "1Day", AnalyticsSettings()).iloc[-1].to_dict()
                    actual = indicators.compute_feature_snapshot(typed(frame), "1Day", AnalyticsSettings())
                    self.assertEqual(clean_json_value(actual), clean_json_value(expected))
        missing_timestamp = base.copy()
        missing_timestamp.loc[20, "timestamp"] = pd.NaT
        actual = indicators.compute_feature_snapshot(missing_timestamp, "1Day", AnalyticsSettings())
        expected = indicators.compute_feature_frame(missing_timestamp, "1Day", AnalyticsSettings()).iloc[-1].to_dict()
        self.assertTrue(pd.isna(actual.pop("timestamp")))
        self.assertTrue(pd.isna(expected.pop("timestamp")))
        self.assertEqual(clean_json_value(actual), clean_json_value(expected))

    def test_extension_absence_old_abi_and_invalid_configuration(self):
        bars = typed(HistoryRepository().frames[0, "1Day"])
        with patch.dict(os.environ, ANALYTICS_BACKEND="auto"), patch.object(indicators, "_rust_feature_columns", None):
            actual = indicators.compute_feature_snapshot(bars, "1Day", AnalyticsSettings())
            expected = indicators._compute_feature_frame_python(bars.to_frame(), "1Day", AnalyticsSettings()).iloc[-1].to_dict()
            self.assertEqual(clean_json_value(actual), clean_json_value(expected))
        for backend, error in (("invalid", ValueError), ("rust", RuntimeError)):
            with patch.dict(os.environ, ANALYTICS_BACKEND=backend), patch.object(indicators, "_rust_feature_columns", None):
                with self.assertRaises(error):
                    indicators.compute_feature_snapshot(bars, "1Day", AnalyticsSettings())
        if indicators._rust_feature_columns:
            native = indicators._rust_feature_columns
            def old(*args, **kw):
                if kw:
                    raise TypeError("feature_columns() got an unexpected keyword argument 'snapshot'")
                return native(*args)
            with patch.dict(os.environ, ANALYTICS_BACKEND="rust"), patch.object(indicators, "_rust_feature_columns", old):
                actual = indicators.compute_feature_snapshot(bars, "1Day", AnalyticsSettings())
            with patch.dict(os.environ, ANALYTICS_BACKEND="rust"):
                expected = indicators.compute_feature_frame(bars.to_frame(), "1Day", AnalyticsSettings()).iloc[-1].to_dict()
            self.assertEqual(clean_json_value(actual), clean_json_value(expected))

    @unittest.skipUnless(indicators._rust_feature_columns, "native snapshot")
    def test_native_snapshot_avoids_feature_frame_and_records_one_dispatch(self):
        bars = typed(HistoryRepository().frames[0, "1Day"])
        counts = Counter()
        token = indicators._dispatch_trace.set(counts)
        try:
            with patch.dict(os.environ, ANALYTICS_BACKEND="rust"), \
                 patch.object(indicators, "compute_feature_frame", side_effect=AssertionError("full frame")), \
                 patch.object(StateBars, "to_frame", side_effect=AssertionError("bar frame")):
                indicators.compute_feature_snapshot(bars, "1Day", AnalyticsSettings())
            self.assertEqual(counts, {"native": 1})
        finally:
            indicators._dispatch_trace.reset(token)

    def test_endpoint_full_serialized_parity_cutoffs_settings_quality_and_benchmarks(self):
        repo = TypedHistoryRepository()
        repo.contexts["B"].update(sync_status="error", sync_error="stale fixture")
        del repo.contexts["QQQ"]
        repo.frames[2, "15Min"] = repo.frames[2, "15Min"].iloc[:0]
        settings = replace(AnalyticsSettings(), breakout_lookback=55, buffer_pct=.01, sma_slope_lookback=3,
                           volume_confirmation=False, volume_multiple=.9, rsi_overbought=65, rsi_oversold=35,
                           volatility_regime_low_multiple=.8, volatility_regime_high_multiple=1.3)
        for backend in ["python"] + (["rust"] if indicators._rust_feature_columns else []):
            for cutoff in ("2023-01-20", "2024-01-01", "2024-07-05T00:00:00Z", "2024-07-05",
                           "2024-07-05T16:00:00.123456-04:00", "2024-07-06"):
                for cls in (ReferenceService, AnalyticsService):
                    current = cls(repo)
                    with patch.dict(os.environ, ANALYTICS_BACKEND=backend), \
                         patch.object(dashboard, "analytics_service", current), \
                         patch.object(dashboard, "_init_db_if_needed"), \
                         patch.object(dashboard, "_portfolio_symbols", return_value=["A", "B", "SPY", "EMPTY", "MISSING"]), \
                         patch.object(dashboard, "_sync_market_data_window", side_effect=AssertionError("provider")), \
                         dashboard.app.test_client() as client:
                        payload = client.get("/api/portfolio/state", query_string=dict(asof=cutoff)).get_json()
                        custom = current.get_batch_state(["A", "B", "SPY", "EMPTY", "MISSING"], "1Day", parse_asof(cutoff), settings)
                        intraday = client.get("/api/portfolio/state", query_string=dict(asof=cutoff, timeframe="15Min")).get_json()
                        # QQQ missing metadata and SPY missing intraday anchor.
                        single = client.get("/api/state/SPY", query_string=dict(asof=cutoff)).get_json()
                    if cls is ReferenceService:
                        expected = payload, custom, single, intraday
                    else:
                        self.assertEqual((payload, custom, single, intraday), expected)


if __name__ == "__main__":
    unittest.main()
