from dataclasses import replace
import os
import unittest
import weakref
from unittest.mock import MagicMock, patch

import numpy as np
import pandas as pd

import app as dashboard
from analytics import historical, indicators
from analytics.config import AnalyticsSettings
from analytics.data_access import AnalyticsRepository, LabelBar
from analytics.future_labels import scan_windows
from analytics.service import AnalyticsService
from analytics.utils import parse_asof


class HistoryRepository:
    """Identical in-memory reads behind the old and new service entrypoints."""
    def __init__(self):
        self.contexts = {s: dict(symbol=s, symbol_id=i) for i, s in enumerate(("A", "B", "SPY", "QQQ", "EMPTY"))}
        self.frames = {}
        dates = pd.bdate_range("2023-01-02T05:00:00Z", "2024-07-10T05:00:00Z")
        dates = dates[~dates.strftime("%m-%d").isin(["01-01", "07-04"])]
        for s, context in self.contexts.items():
            sid = context["symbol_id"]
            close = 100 + sid * np.sin(np.arange(len(dates)) / 11) + np.arange(len(dates)) / 20
            for tf in ("1Day", "15Min"):
                frame = pd.DataFrame(dict(timestamp=dates + pd.Timedelta(hours=15 if tf == "15Min" else 0),
                    open=close - 1, high=close + 2, low=close - 2, close=close, volume=1000 + np.arange(len(dates)) % 100))
                if s == "EMPTY" or s == "QQQ" and tf == "15Min":
                    frame = frame.iloc[:0]
                if s == "B":
                    frame = frame.iloc[:-15]  # stale/unaligned benchmark history
                self.frames[sid, tf] = frame

    def fetch_symbol_contexts(self, symbols):
        return {s: self.contexts[s] for s in symbols if s in self.contexts}

    def fetch_symbol_context(self, symbol):
        return self.contexts.get(symbol)

    def fetch_bars_many(self, ids, tf, asof=None, *, latest=False, after=None, limit=None):
        result = {}
        for sid in ids:
            frame = self.frames[sid, tf]
            if asof is not None:
                frame = frame[frame.timestamp <= asof]
            if after is not None:
                frame = frame[frame.timestamp > after[sid]]
            result[sid] = (frame.tail(1) if latest else frame.head(limit)).reset_index(drop=True)
        return result

    def fetch_bars(self, sid, tf, asof=None):
        return self.fetch_bars_many([sid], tf, asof)[sid]

    def fetch_latest_bar(self, sid, tf, asof=None):
        return self.fetch_bars_many([sid], tf, asof, latest=True)[sid]

    def fetch_label_bars_many(self, ids, tf, asof=None, **kw):
        return {sid: [LabelBar(r.timestamp.to_pydatetime(), r.high, r.low, r.close) for r in frame.itertuples()]
                for sid, frame in self.fetch_bars_many(ids, tf, asof, **kw).items()}

    def fetch_label_summary(self, sid, tf, asof, days, stop, take, side):
        anchors = self.fetch_label_bars_many([sid], tf, asof, latest=True)[sid]
        bars = self.fetch_label_bars_many([sid], tf, after={sid: anchors[-1].timestamp}, limit=days)[sid] if anchors else []
        return anchors, bars, scan_windows([bars], [anchors[-1].close], stop, take, side)[0] if anchors else None

    def fetch_historical_price_anchors(self, sid, dates):
        return {i: frame.iloc[-1].to_dict() for i, date in enumerate(dates)
                if not (frame := self.fetch_latest_bar(sid, "15Min", date)).empty}


class HistoricalTests(unittest.TestCase):
    def setUp(self):
        self.repo = HistoryRepository()
        self.service = AnalyticsService(self.repo)

    def test_complete_payload_parity_dates_settings_backends_sides_and_errors(self):
        cutoffs = ["2024-07-04", "2024-07-06", "2024-07-05T00:00:00Z", "2024-07-05T20:00:00Z",
                   "2024-07-05T16:00:00-04:00", "2023-01-01", "2023-01-20", "2024-07-10", "2024-07-04"]
        symbols = ["A", "B", "SPY", "EMPTY", "MISSING"]
        for backend in ["python"] + (["rust"] if indicators._rust_feature_columns else []):
            for settings in (AnalyticsSettings(), replace(AnalyticsSettings(), breakout_lookback=55, buffer_pct=.01,
                    volume_confirmation=False, sma_slope_lookback=3, volatility_regime_low_multiple=.8)):
                for side in ("long", "short"):
                    with self.subTest(backend=backend, settings=settings, side=side), patch.dict(os.environ, ANALYTICS_BACKEND=backend):
                        result = self.service.get_historical_portfolio(symbols, cutoffs, settings, 20, .01, .02, side)
                        for row in result["dates"]:
                            date = parse_asof(row["cutoff"])
                            expected = AnalyticsService(self.repo)
                            self.assertEqual(row["features"], expected.get_batch_state(symbols, "1Day", date, settings))
                            self.assertEqual(row["labels"], expected.get_batch_future_state(symbols, "1Day", date, 20, .01, .02, side))
                        self.assertEqual(result["execution"]["feature_computations"], 4)
                        self.assertEqual(result["sample_count"], 45)
                        self.assertEqual(result["dates"][0]["features"], result["dates"][-1]["features"])

    def test_every_feature_column_is_prefix_invariant_when_future_removed_or_changed(self):
        bars = self.repo.frames[0, "1Day"]
        for backend in ["python"] + (["rust"] if indicators._rust_feature_columns else []):
            with patch.dict(os.environ, ANALYTICS_BACKEND=backend):
                full = indicators.compute_feature_frame(bars, "1Day", AnalyticsSettings())
                for n in (1, 14, 26, 60, 200, 260, len(bars) - 1):
                    prefix = indicators.compute_feature_frame(bars.iloc[:n], "1Day", AnalyticsSettings())
                    changed = bars.copy()
                    changed.loc[n:, ["open", "high", "low", "close", "volume"]] *= 17
                    future = indicators.compute_feature_frame(changed, "1Day", AnalyticsSettings())
                    for candidate in (prefix, future.iloc[:n]):
                        pd.testing.assert_frame_equal(full.iloc[:n].isna(), candidate.isna())
                        pd.testing.assert_frame_equal(full.iloc[:n].fillna(0), candidate.fillna(0),
                                                      check_exact=True, check_dtype=False)

    def test_missing_benchmarks_and_per_date_ranks(self):
        del self.repo.contexts["QQQ"]
        self.repo.frames[2, "1Day"] = self.repo.frames[2, "1Day"].iloc[:25]
        dates = ["2023-01-20", "2024-07-05"]
        result = self.service.get_historical_portfolio(["A", "B"], dates, AnalyticsSettings(), 2)
        for date, row in zip(dates, result["dates"]):
            self.assertEqual(row["features"], AnalyticsService(self.repo).get_batch_state(["A", "B"], "1Day", parse_asof(date), AnalyticsSettings()))
        self.assertNotEqual(result["dates"][0]["features"]["states"][0]["portfolio"],
                            result["dates"][1]["features"]["states"][0]["portfolio"])

    def test_invalid_history_rejects_symbol_for_entire_shared_sweep(self):
        dates = ["2024-06-03", "2024-07-02"]
        for value in (0.0, float("nan")):
            self.repo.frames[1, "1Day"].loc[self.repo.frames[1, "1Day"].timestamp > parse_asof("2024-06-15"), "close"] = value
            result = self.service.get_historical_portfolio(["B"], dates, AnalyticsSettings(), 2)
            for row in result["dates"]:
                self.assertEqual(row["features"]["states"], [])
                self.assertEqual(row["features"]["errors"][0]["symbol"], "B")
                self.assertIn("Invalid OHLCV", row["features"]["errors"][0]["error"])

    def test_no_cache_pollution_and_reads_scale_with_symbols_not_dates(self):
        for n in (12, 60):
            with patch.object(self.repo, "fetch_bars_many", wraps=self.repo.fetch_bars_many) as reads:
                result = self.service.get_historical_portfolio([" a ", "A", "B"], ["2024-07-05"] * n, AnalyticsSettings(), 2)
            self.assertEqual(result["symbols"], ["A", "B"])
            self.assertEqual(result["execution"]["history_reads"], 4)
            self.assertEqual(result["execution"]["feature_computations"], 4)
            self.assertEqual(result["execution"]["tail_reads"], 2)
            self.assertEqual(sum(call.kwargs.get("limit") == 20001 for call in reads.call_args_list), 4)
            self.assertEqual(self.service._state_cache.diagnostics()["entries"], 0)

    def test_full_histories_and_features_are_released_between_symbols(self):
        history_refs, feature_refs = [], []
        read = self.repo.fetch_bars_many
        compute = historical.compute_feature_frame

        def tracked_read(*a, **kw):
            if kw.get("limit") == 20001:
                self.assertTrue(all(ref() is None for ref in history_refs + feature_refs))
            result = read(*a, **kw)
            if kw.get("limit") == 20001:
                history_refs.extend(weakref.ref(frame) for frame in result.values())
            return result

        def tracked_compute(*a, **kw):
            result = compute(*a, **kw)
            feature_refs.append(weakref.ref(result))
            return result

        with patch.object(self.repo, "fetch_bars_many", tracked_read), patch.object(historical, "compute_feature_frame", tracked_compute):
            # Include a no-future-bar error to detect retained traceback cycles.
            self.service.get_historical_portfolio(["A", "B"], ["2024-07-10"] * 60, AnalyticsSettings())
        self.assertTrue(all(ref() is None for ref in history_refs + feature_refs))

    def test_maximum_date_symbol_count_and_empty_portfolio(self):
        result = self.service.get_historical_portfolio(["MISSING" + str(n) for n in range(50)],
                    ["2024-07-05"] * 60, AnalyticsSettings())
        self.assertEqual(result["sample_count"], 3000)
        self.assertEqual(sum(len(row["features"]["errors"]) for row in result["dates"]), 3000)
        empty = self.service.get_historical_portfolio([], ["2024-07-05"] * 120, AnalyticsSettings())
        self.assertEqual(empty["sample_count"], 0)
        self.assertTrue(all(not row["features"]["states"] and not row["labels"]["future_states"] for row in empty["dates"]))

    def test_input_bounds_rejected_before_reads(self):
        cases = [(["A"] * 51, ["2024-07-05"], {}, historical.HistoricalLimitError),
                 (["A"], ["2024-07-05"] * 121, {}, historical.HistoricalLimitError),
                 ([str(n) for n in range(50)], ["2024-07-05"] * 61, {}, historical.HistoricalLimitError),
                 (["A"], ["2024-07-05"], {"days":253}, historical.HistoricalLimitError),
                 (["A"], [None], {}, ValueError), (["A"], [""], {}, ValueError),
                 (["A"], ["2024-07-05"], {"days":True}, ValueError),
                 (["A"], ["2024-07-05"], {"stop_loss_pct":float("nan")}, ValueError),
                 (["A"], ["2024-07-05"], {"take_profit_pct":10 ** 400}, ValueError)]
        with patch.object(self.repo, "fetch_symbol_contexts", side_effect=AssertionError("read")):
            for symbols, dates, kw, error in cases:
                with self.subTest(symbols=len(symbols), dates=len(dates), kw=kw), self.assertRaises(error):
                    self.service.get_historical_portfolio(symbols, dates, AnalyticsSettings(), **kw)

    def test_history_output_bounds_and_gate_released_on_failure(self):
        for limits in ({"max_history_bars":5}, {"max_output_bytes":100}, {"max_output_bytes":10000}):
            with patch.dict(historical.LIMITS, limits), self.assertRaises(historical.HistoricalLimitError):
                self.service.get_historical_portfolio(["A", "B"], ["2024-07-05"], AnalyticsSettings())
        self.assertTrue(self.service.get_historical_portfolio(["A"], ["2024-07-05"], AnalyticsSettings()))
        with historical._admission, self.assertRaises(historical.HistoricalBusyError):
            self.service.get_historical_portfolio(["A"], ["2024-07-05"], AnalyticsSettings())

    def test_http_membership_contract_validation_and_no_provider_sync(self):
        body = dict(cutoffs=["2024-07-05"], include_hidden=True, settings=dict(sma_slope_lookback=3))
        with patch.object(dashboard, "analytics_service", self.service), patch.object(dashboard, "_init_db_if_needed"), \
             patch.object(dashboard, "_portfolio_symbols", return_value=["A", "B"]) as membership, \
             patch.object(dashboard, "_sync_market_data_window", side_effect=AssertionError("sync")), \
             patch.object(dashboard, "_backfill_analytics_symbol_if_needed", side_effect=AssertionError("backfill")), \
             dashboard.app.test_client() as client:
            response = client.post("/api/portfolio/history", json=body)
            self.assertEqual(response.status_code, 200, response.get_json())
            membership.assert_called_once_with(include_hidden=True)
            self.assertTrue(response.get_json()["ok"])
            for extra in (dict(timeframe="15Min"), dict(settings={"unknown":1}), dict(settings={"buffer_pct":float("inf")}),
                          dict(settings={"sma_slope_lookback":0}), dict(side="none"), dict(include_hidden="yes")):
                self.assertEqual(client.post("/api/portfolio/history", json={**body, **extra}).status_code, 400)
            self.assertEqual(client.post("/api/portfolio/history", json=[]).status_code, 400)
            self.assertEqual(client.post("/api/portfolio/history", data="x" * 65537).status_code, 413)
            self.assertEqual(client.post("/api/portfolio/history", json={**body, "cutoffs":["2024-07-05"] * 121}).status_code, 413)
            with historical._admission:
                busy = client.post("/api/portfolio/history", json=body)
                self.assertEqual(busy.status_code, 503)
                self.assertEqual(busy.headers["Retry-After"], "1")

    def test_anchor_query_is_bounded_inclusive_and_preserves_cutoff_positions(self):
        repo = AnalyticsRepository()
        conn = MagicMock()
        cur = conn.__enter__.return_value.cursor.return_value.__enter__.return_value
        cur.fetchall.return_value = [(2, 1700000000000000, 100, 101, 99, 100, 1000)]
        dates = [parse_asof("2023-01-01"), parse_asof("2024-01-01")]
        with patch.object(repo, "get_conn", return_value=conn):
            result = repo.fetch_historical_price_anchors(7, dates)
        sql, args = cur.execute.call_args.args
        self.assertIn("WITH ORDINALITY", sql)
        self.assertIn("bar_time <= requested.cutoff", sql)
        self.assertIn("ORDER BY bar_time DESC LIMIT 1", sql)
        self.assertEqual(args, (dates, 7))
        self.assertEqual(list(result), [1])


if __name__ == "__main__":
    unittest.main()
