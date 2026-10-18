"""Issuer-sourced conversion economics, provider basis checks and publication."""
from contextlib import ExitStack
import copy
import json
import os
from pathlib import Path
import unittest
from unittest.mock import patch

import pandas as pd

import app as dashboard
from analytics.config import AnalyticsSettings
from analytics.data_access import AnalyticsRepository
from analytics.io_benchmark import price_fixture
from analytics.quality import corporate_action_diagnostics
from analytics.service import AnalyticsService
from corporate_actions import normalize_actions
from database import get_connection

FIXTURE = json.loads((Path(__file__).parent / "fixtures/azn_conversion.json").read_text())
START = pd.Timestamp("2025-01-01T00:00Z").to_pydatetime()
END = pd.Timestamp("2026-02-03T23:00Z").to_pydatetime()


class CorporateActionTests(unittest.TestCase):
    def normalize(self, bars, raw=None, symbol="AZN", mode="split"):
        return normalize_actions(symbol, "1Day", bars, mode,
                                 fetch_raw=lambda *args: raw or FIXTURE["bars"], start=START, end=END)

    def test_issuer_ratio_removes_only_unit_gain_and_converts_all_ohlcv_fields(self):
        raw = copy.deepcopy(FIXTURE["bars"])
        result, evidence = self.normalize(raw)
        self.assertEqual(raw, FIXTURE["bars"])
        self.assertEqual(result[0]["c"], FIXTURE["expected_pre_conversion_close_in_ordinary_units"])
        self.assertEqual(result[0]["v"], FIXTURE["expected_pre_conversion_volume_in_ordinary_units"])
        for key in ("o", "h", "l", "c", "vw"):
            self.assertEqual(result[0][key], raw[0][key] * FIXTURE["old_ads_per_ordinary_share"])
        self.assertEqual(result[0]["n"], raw[0]["n"])
        self.assertEqual(result[1], raw[1])
        self.assertAlmostEqual(result[1]["c"] / result[0]["c"] - 1,
                               FIXTURE["expected_boundary_price_return"])
        self.assertTrue(evidence["factor_verified"])
        self.assertIn(FIXTURE["action_source"], evidence["sources"])

    def test_provider_corrected_history_is_not_adjusted_twice(self):
        corrected, _ = self.normalize(FIXTURE["bars"])
        repeated, evidence = self.normalize(corrected)
        self.assertEqual(repeated, corrected)
        self.assertEqual(evidence["provider_price_factors"], [2])

    def test_unrecognized_provider_basis_or_missing_reference_aborts(self):
        for raw in ([FIXTURE["bars"][1]], [{**FIXTURE["bars"][0], "h": 200}]):
            with self.subTest(raw=raw), self.assertRaisesRegex(ValueError, "Missing raw|Unresolved AZN"):
                self.normalize(FIXTURE["bars"], raw)
        with self.assertRaisesRegex(ValueError, "dividend/all"):
            self.normalize(FIXTURE["bars"], mode="all")

    def test_raw_mode_and_fractional_volume_policy(self):
        raw = [{**FIXTURE["bars"][0], "v": 101}]
        result, _ = self.normalize(raw, raw, mode="raw")
        self.assertEqual(result[0]["v"], 51)

    def test_genuine_large_moves_and_distribution_prices_are_preserved(self):
        for symbol in ("HOOD", "SNX", "UNKNOWN"):
            bars = [dict(t="2020-11-30T05:00Z", c=160.31), dict(t="2020-12-01T05:00Z", c=76.02)]
            result, _ = self.normalize(bars, symbol=symbol)
            self.assertEqual(result, bars)
        bars = FIXTURE["bars"] + [{**FIXTURE["bars"][1], "t": "2026-02-03T05:00Z", "c": 400}]
        result, _ = self.normalize(bars)
        self.assertEqual(result[-1]["c"], 400)
        frame = pd.DataFrame({"close": [100, 101], "timestamp": pd.to_datetime(["2026-01-01", "2026-01-02"], utc=True)})
        quality = corporate_action_diagnostics(frame, {"symbol": "SNX"})
        self.assertEqual(quality["status"], "warning")
        self.assertEqual(quality["large_move_count"], 0)
        self.assertEqual(quality["unresolved_actions"][0]["id"], "SNX-2020-12-01-Concentrix")


@unittest.skipUnless(os.environ.get("ANALYTICS_TEST_DATABASE_URL"), "requires PostgreSQL")
class CorporateActionPostgreSQLTests(unittest.TestCase):
    def setUp(self):
        self.stack = ExitStack()
        self.addCleanup(self.stack.close)
        self.dsn, _ = self.stack.enter_context(price_fixture(os.environ["ANALYTICS_TEST_DATABASE_URL"], 0, 0))
        self.stack.enter_context(patch.object(dashboard, "_get_conn", lambda: get_connection(self.dsn)))
        self.sid = dashboard._ensure_symbol_metadata("AZN")
        self.repo = AnalyticsRepository(self.dsn)
        self.service = AnalyticsService(self.repo)
        self.rows = [{**FIXTURE["bars"][0], "t": t.isoformat()}
                     for t in pd.bdate_range(end="2026-01-30T05:00Z", periods=300)] + [FIXTURE["bars"][1]]
        self.corrected_provider = False

        def fetch(symbol, tf, lo, hi, *, adjustment_override=None):
            bars = [dict(b) for b in self.rows if lo <= pd.Timestamp(b["t"]) <= hi]
            if self.corrected_provider and adjustment_override != "raw":
                bars, _ = normalize_actions("AZN", tf, bars, "raw", fetch_raw=None, start=lo, end=hi)
            return bars
        self.stack.enter_context(patch.object(dashboard, "_fetch_alpaca_bars", side_effect=fetch))
        self.stack.enter_context(patch.object(dashboard, "_alpaca_bar_adjustment_for_timeframe", return_value="split"))
        for tf in dashboard.SUPPORTED_TIMEFRAMES:
            dashboard._upsert_stock_bars(self.sid, tf, self.rows)

    def sync(self):
        return dashboard._sync_market_data_window("AZN", "1Day", START, END)

    def state(self):
        return self.service.get_symbol_state("AZN", "1Day", END, AnalyticsSettings())

    def test_stored_history_returns_indicators_labels_cache_and_idempotent_preparation(self):
        old = self.state()
        self.assertGreater(old["returns"]["r_1"], 1)
        self.assertEqual(old["data_quality"]["corporate_action_consistency"]["status"], "warning")
        self.sync()
        state = self.state()
        self.assertAlmostEqual(state["returns"]["r_1"], FIXTURE["expected_boundary_price_return"])
        self.assertAlmostEqual(state["returns"]["ytd"], FIXTURE["expected_boundary_price_return"])
        self.assertAlmostEqual(state["trend"]["sma_200"], (199 * 185.54 + 188.41) / 200)
        for tf in dashboard.SUPPORTED_TIMEFRAMES:
            bars = self.repo.fetch_bars(self.sid, tf)
            self.assertEqual(bars.iloc[-2]["close"], 185.54)
            self.assertEqual(bars.iloc[-2]["volume"], 2777512)
            self.assertEqual(bars.iloc[-1]["close"], 188.41)
        manifest = state["data_quality"]["adjustment_basis"]["corporate_actions"]
        self.assertEqual(len(manifest["reviewed_actions"]), 5)
        self.assertFalse(state["data_quality"]["corporate_action_consistency"]["unresolved_actions"])
        with patch.object(dashboard, "analytics_service", self.service), patch.object(dashboard, "_portfolio_symbols", return_value=["AZN"]):
            response = dashboard.app.test_client().get("/api/portfolio/state?asof=2026-02-03T23:00:00Z")
            self.assertEqual(response.status_code, 200, response.get_json())
            self.assertAlmostEqual(response.get_json()["states"][0]["returns"]["r_1"], FIXTURE["expected_boundary_price_return"])
        label = self.service.get_future_state("AZN", "1Day", pd.Timestamp("2026-02-01T23:00Z"), 1)
        self.assertAlmostEqual(label["future_state"]["return_at_horizon_pct"], FIXTURE["expected_boundary_price_return"])
        revision = self.repo.fetch_symbol_context("AZN")["adjustment_revision"]
        self.sync()
        self.assertEqual(self.repo.fetch_symbol_context("AZN")["adjustment_revision"], revision)
        self.assertEqual(self.state()["returns"], state["returns"])
        self.assertEqual(self.state()["trend"], state["trend"])
        self.corrected_provider = True
        self.sync()
        self.assertAlmostEqual(self.state()["returns"]["r_1"], FIXTURE["expected_boundary_price_return"])

    def test_missing_raw_bar_rolls_back_all_timeframes_and_provenance(self):
        self.sync()
        revision = self.repo.fetch_symbol_context("AZN")["adjustment_revision"]
        fetch = dashboard._fetch_alpaca_bars.side_effect
        def missing(*args, **kwargs):
            return [] if kwargs.get("adjustment_override") == "raw" and args[1] == "5Min" else fetch(*args, **kwargs)
        with patch.object(dashboard, "_fetch_alpaca_bars", side_effect=missing):
            with self.assertRaisesRegex(ValueError, "Missing raw"):
                self.sync()
        self.assertEqual(self.repo.fetch_symbol_context("AZN")["adjustment_revision"], revision)

    def test_legacy_coverage_is_invalidated_before_preparation(self):
        self.assertEqual(dashboard._historical_price_coverage("AZN", "1Day", START, END), (None, None, []))
        self.sync()
        self.assertTrue(dashboard._historical_price_coverage("AZN", "1Day", START, END)[2])

    def test_repair_entry_point_republishes_retained_history(self):
        from analytics.repair_corporate_actions import repair
        with patch.object(dashboard, "analytics_service", self.service):
            first, second = repair(), repair()
        self.assertEqual(first["adjustment_basis"]["revision"], second["adjustment_basis"]["revision"])
        self.assertAlmostEqual(second["states"]["2026-02-03T23:00:00Z"]["returns"]["r_1"],
                               FIXTURE["expected_boundary_price_return"])
