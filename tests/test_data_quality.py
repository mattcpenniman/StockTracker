import unittest
from unittest.mock import patch

import numpy as np
import pandas as pd

import app as dashboard
from analytics.config import AnalyticsSettings
from analytics.coverage import historical_coverage, session_coverage
from analytics.indicators import compute_feature_frame, compute_feature_snapshot, sufficiency_flags
from analytics.quality import DataQualityError, corporate_action_diagnostics
from analytics.service import AnalyticsService
from tests.test_historical import HistoryRepository
from tests.test_state_snapshot import typed, TypedHistoryRepository


class DataQualityTests(unittest.TestCase):
    def test_ingestion_rejects_missing_nonfinite_and_impossible_bars_before_write(self):
        valid = dict(t="2026-03-17T13:30:00Z", o=100, h=102, l=99, c=101, v=0)
        self.assertEqual(dashboard._normalize_bar_row(1, "1Day", valid)[7], 0)
        cases = [{k: v for k, v in valid.items() if k != key} for key in ("o", "h", "l", "c", "v")]
        cases += [{**valid, key: value} for key, value in (
            ("o", None), ("c", 0), ("l", -1), ("h", float("inf")),
            ("c", float("nan")), ("c", "bad"), ("v", -1), ("v", 1.5),
            ("v", float("inf")), ("h", 100), ("l", 102))]
        for bar in cases:
            with self.subTest(bar=bar), patch.object(dashboard, "_get_conn") as conn:
                with self.assertRaises(DataQualityError):
                    dashboard._upsert_stock_bars(1, "1Day", [valid, bar])
                conn.assert_not_called()

    def test_python_and_typed_native_boundaries_reject_all_invalid_fields(self):
        base = HistoryRepository().frames[0, "1Day"].copy()
        for col, value in (("open", 0), ("close", np.nan), ("high", np.inf),
                           ("low", -np.inf), ("volume", -1), ("open", 10000)):
            frame = base.copy()
            frame.loc[3, col] = value
            for backend in ("python", "auto"):
                with self.subTest(col=col, backend=backend), patch.dict("os.environ", ANALYTICS_BACKEND=backend):
                    with self.assertRaises(DataQualityError):
                        compute_feature_frame(frame, "1Day", AnalyticsSettings())
                    with self.assertRaises(DataQualityError):
                        compute_feature_snapshot(typed(frame), "1Day", AnalyticsSettings())

    def test_one_invalid_symbol_does_not_abort_portfolio_or_benchmarks(self):
        repo = TypedHistoryRepository()
        repo.frames[0, "1Day"].loc[3, "volume"] = -1
        symbols = list(repo.contexts)
        result = AnalyticsService(repo).get_batch_state(symbols, "1Day", pd.Timestamp("2024-03-01", tz="UTC"), AnalyticsSettings())
        self.assertTrue(result["states"])
        self.assertIn("Invalid OHLCV", result["errors"][0]["error"])
        self.assertNotIn(result["errors"][0]["symbol"], [s["symbol"] for s in result["states"]])

    def test_weekend_holiday_early_close_and_current_requests(self):
        for cutoff in ("2024-07-04T20:00Z", "2024-07-06T20:00Z"):
            actual = "2024-07-03T04:00Z" if "07-04" in cutoff else "2024-07-05T04:00Z"
            coverage = historical_coverage(cutoff, actual)
            self.assertFalse(coverage["stale"])
            self.assertTrue(coverage["anchor_completed"])
        before = historical_coverage("2024-07-03T16:59Z", "2024-07-02T04:00Z")
        after = historical_coverage("2024-07-03T17:00Z", "2024-07-02T04:00Z")
        self.assertFalse(before["stale"])
        self.assertTrue(after["stale"])
        current = historical_coverage(None, "2024-07-02T04:00Z", now="2024-07-04T20:00Z")
        self.assertTrue(current["stale"])
        self.assertIn("Resync", current["action"])
        self.assertEqual(current["expected_latest_completed"], "2024-07-03")

    def test_intraday_early_close_and_dst(self):
        coverage = historical_coverage("2024-07-04T20:00Z", "2024-07-03T16:45Z", "15Min")
        self.assertFalse(coverage["stale"])
        self.assertTrue(coverage["anchor_completed"])
        for cutoff, anchor in (("2024-03-08T21:00Z", "2024-03-08T20:45Z"),
                               ("2024-03-11T20:00Z", "2024-03-11T19:45Z")):
            self.assertFalse(historical_coverage(cutoff, anchor, "15Min")["stale"])
        self.assertTrue(historical_coverage("2024-03-11T20:00Z", "2024-03-11T19:30Z", "15Min")["stale"])

    def test_interior_gap_independent_of_recent_anchor_and_holidays(self):
        result = session_coverage(pd.to_datetime(["2024-07-02", "2024-07-05"], utc=True))
        self.assertEqual(result["missing_sessions"], ["2024-07-03"])
        self.assertFalse(result["completeness_verified"])
        complete = session_coverage(pd.to_datetime(["2024-07-03", "2024-07-05"], utc=True))
        self.assertTrue(complete["session_presence_verified"])
        self.assertFalse(complete["completeness_verified"])

    def test_metric_sufficiency_uses_required_history_and_available_value(self):
        frame = HistoryRepository().frames[0, "1Day"].iloc[:20].copy()
        frame["volume"] = 0
        latest = compute_feature_snapshot(frame, "1Day", AnalyticsSettings())
        flags = sufficiency_flags(20, latest, AnalyticsSettings())
        self.assertTrue(flags["has_sufficient_history_20"])
        self.assertTrue(flags["metric_history"]["sma_20"]["usable"])
        self.assertFalse(flags["metric_history"]["return_20"]["usable"])
        self.assertFalse(flags["metric_history"]["volume_ratio_20"]["usable"])

    def test_large_move_is_review_warning_not_invalidity_or_adjustment(self):
        frame = HistoryRepository().frames[0, "1Day"].copy()
        frame.loc[1:, ["open", "high", "low", "close"]] *= 2
        features = compute_feature_frame(frame, "1Day", AnalyticsSettings())
        self.assertGreater(features["return_1"].iloc[1], .5)
        quality = corporate_action_diagnostics(frame)
        self.assertEqual(quality["status"], "warning")
        self.assertFalse(quality["verified"])
        self.assertEqual(quality["review_ticket"], "VEC-830")
        self.assertEqual(corporate_action_diagnostics(frame.iloc[2:].reset_index(drop=True))["status"], "unknown")
