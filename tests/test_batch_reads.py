from __future__ import annotations

from datetime import timedelta
import os
import unittest
from unittest.mock import MagicMock, Mock, patch

import pandas as pd
import psycopg2

import app as dashboard
from analytics.batch_benchmark import baseline_implementations, batch_fixture, route_variant
from analytics.config import AnalyticsSettings
from analytics.data_access import AnalyticsRepository
from analytics.io_benchmark import measured_repository
from analytics.service import AnalyticsService


class BatchQueryTests(unittest.TestCase):
    def test_contexts_normalize_deduplicate_and_do_not_connect_when_empty(self):
        repo = AnalyticsRepository()
        conn = MagicMock()
        cur = conn.__enter__.return_value.cursor.return_value.__enter__.return_value
        cur.fetchall.return_value = []
        with patch.object(repo, "get_conn", return_value=conn) as connect:
            self.assertEqual(repo.fetch_symbol_contexts([]), {})
            connect.assert_not_called()
            self.assertEqual(repo.fetch_symbol_contexts([" spy ", "SPY", "missing"]), {})
        sql, params = cur.execute.call_args.args
        self.assertIn("sm.symbol = ANY(%s)", sql)
        self.assertEqual(params, (["SPY", "MISSING"],))
        cur.execute.assert_called_once()

    def test_bulk_limits_are_per_symbol_and_future_boundaries_are_strict(self):
        repo = AnalyticsRepository()
        conn = MagicMock()
        cur = conn.__enter__.return_value.cursor.return_value.__enter__.return_value
        cur.fetchall.return_value = []
        anchor = pd.Timestamp("2023-01-03T09:30:00-05:00")
        with patch.object(repo, "get_conn", return_value=conn):
            frames = repo.fetch_bars_many([2, 1, 2], "15Min", anchor, latest=True)
            sql, params = cur.execute.call_args.args
            self.assertIn("CROSS JOIN LATERAL", sql)
            self.assertIn("bar_time <= %s ORDER BY bar_time DESC LIMIT %s)", sql)
            self.assertEqual(params, [[2, 1], "15Min", anchor.tz_convert("UTC"), 1])
            self.assertEqual(list(frames), [2, 1])
            self.assertTrue(all(frame.empty for frame in frames.values()))
            repo.fetch_bars_many([1, 2], "15Min", after={1: anchor, 2: anchor + timedelta(minutes=15)}, limit=20)
            sql, params = cur.execute.call_args.args
            self.assertIn("bar_time > anchors.bar_time ORDER BY bar_time ASC LIMIT %s)", sql)
            self.assertEqual(params[1], [anchor.tz_convert("UTC"), anchor.tz_convert("UTC") + timedelta(minutes=15)])
            self.assertEqual(params[-1], 20)

    def test_bulk_freshness_preserves_policy_and_uses_post_sync_cache_key(self):
        now = pd.Timestamp("2026-10-16T20:00:00Z")
        contexts = {s: {"symbol_id": i, "symbol": s, "last_successful_sync_at": now,
                        "last_bar_synced_at": now - timedelta(days=1)}
                    for i, s in enumerate(("FRESH", "STALE", "SPY", "QQQ"), 1)}
        repo = Mock()
        repo.fetch_symbol_contexts.return_value = contexts
        repo.fetch_bars_many.side_effect = lambda ids, tf, **kw: {
            sid: pd.DataFrame({"timestamp": [now - timedelta(days=10 if sid == 2 and tf == "15Min" else 0)]})
            for sid in ids
        }
        result = {"sync_state": {"last_bar_synced_at": now.isoformat(), "last_successful_sync_at": now.isoformat(),
                                  "sync_status": "idle", "sync_error": None}}
        with patch.object(dashboard, "analytics_service", AnalyticsService(repo)), \
             patch.object(dashboard, "_utcnow", return_value=now), \
             patch.object(dashboard, "_sync_market_data", return_value=result) as sync:
            actual = dashboard._prepare_current_analytics_batch(["FRESH", "STALE", "FRESH"], "15Min")
        sync.assert_called_once_with("STALE", "15Min", force_full=False, snapshot_limit=1)
        repo.fetch_symbol_contexts.assert_called_once_with(["FRESH", "STALE", "FRESH", "SPY", "QQQ"])
        self.assertEqual(repo.fetch_bars_many.call_count, 2)
        self.assertEqual(actual["STALE"]["last_bar_synced_at"], now)
        self.assertEqual(actual["FRESH"]["last_bar_synced_at"], now - timedelta(days=1))

    def test_freshness_sync_failure_propagates(self):
        repo = Mock()
        repo.fetch_symbol_contexts.return_value = {"MISSING": {"symbol": "MISSING", "symbol_id": 1}}
        repo.fetch_bars_many.return_value = {}
        with patch.object(dashboard, "analytics_service", AnalyticsService(repo)), \
             patch.object(dashboard, "_sync_market_data", side_effect=RuntimeError("sync failed")):
            with self.assertRaisesRegex(RuntimeError, "sync failed"):
                dashboard._prepare_current_analytics_batch(["MISSING"], "1Day")

    def test_new_symbol_gets_context_once_after_creation_with_sync_disabled(self):
        repo = Mock()
        context = {"symbol": "NEW", "symbol_id": 7}
        repo.fetch_symbol_contexts.side_effect = [{}, {"NEW": context}]
        repo.fetch_bars_many.return_value = {}
        with patch.object(dashboard, "analytics_service", AnalyticsService(repo)), \
             patch.object(dashboard, "CHART_LATEST_AUTO_SYNC_MAX_AGE_DAYS", -1), \
             patch.object(dashboard, "_ensure_symbol_metadata", return_value=7) as ensure, \
             patch.object(dashboard, "_sync_market_data") as sync:
            actual = dashboard._prepare_current_analytics_batch(["NEW", "NEW"], "1Day")
        ensure.assert_called_once_with("NEW")
        sync.assert_not_called()
        self.assertEqual(actual, {"NEW": context})
        self.assertEqual(repo.fetch_symbol_contexts.call_count, 2)
        self.assertEqual(repo.fetch_symbol_contexts.call_args.args, (["NEW"],))

    def test_batch_larger_than_lru_handles_eviction_after_read_planning(self):
        repo = Mock()
        repo.fetch_symbol_contexts.return_value = {
            s: {"symbol": s, "symbol_id": i} for i, s in enumerate(("A", "B", "C"), 1)
        }
        frame = pd.DataFrame({"timestamp": [pd.Timestamp("2023-01-01T00:00:00Z")],
                              "open": [100.0], "high": [101.0], "low": [99.0], "close": [100.0], "volume": [1000.0]})
        repo.fetch_bars_many.side_effect = lambda ids, *args, **kw: {sid: frame for sid in ids}
        service = AnalyticsService(repo)
        service._state_cache.maxsize = 2
        service._future_cache.maxsize = 2
        state = service.get_batch_state(["A", "B", "C"], "1Day", None, AnalyticsSettings())
        self.assertEqual(service.get_batch_state(["A", "B", "C"], "1Day", None, AnalyticsSettings()), state)
        future = service.get_batch_future_state(["A", "B", "C"], "1Day", None, 1)
        self.assertEqual(service.get_batch_future_state(["A", "B", "C"], "1Day", None, 1), future)
        repo.fetch_symbol_context.assert_not_called()


@unittest.skipUnless(os.environ.get("ANALYTICS_TEST_DATABASE_URL"), "Set ANALYTICS_TEST_DATABASE_URL for batch PostgreSQL parity")
class BatchPostgreSQLTests(unittest.TestCase):
    def assertLegacyEqual(self, actual, expected):
        # VEC-798 adds quality reports and intentionally suppresses unaligned
        # relative returns. Independent coverage tests assert those semantics;
        # retain legacy parity for every other feature, label and error.
        def core(value):
            if isinstance(value, list):
                return [core(item) for item in value]
            if not isinstance(value, dict):
                return value
            result = {}
            for key, item in value.items():
                if key in {"historical_coverage", "benchmark_coverage", "requested_asof", "relative",
                           "stale_symbols", "incomplete_window_symbols", "completeness_verified", "complete_future_window"}:
                    continue
                if key == "benchmark_errors":
                    item = [error for error in item if error["error"] != "Stale trailing benchmark history at requested asof."]
                cleaned = core(item)
                if key == "data_quality" and not cleaned:
                    continue
                result[key] = cleaned
            return result
        self.assertEqual(core(actual), core(expected))

    @classmethod
    def setUpClass(cls):
        cls.fixture = batch_fixture(os.environ["ANALYTICS_TEST_DATABASE_URL"], count=22, mixed=True)
        cls.dsn, cls.end = cls.fixture.__enter__()
        cls.addClassCleanup(cls.fixture.__exit__, None, None, None)
        cls.before, cls.namespace = baseline_implementations()

    def test_mixed_batch_parity_order_duplicates_benchmarks_and_cache_isolation(self):
        symbols = ["D021", "MISSING", "D000", "SPY", "D020", "D021", "QQQ", "BENCH"]
        for timeframe, asof in (("1Day", None), ("1Day", self.end - timedelta(days=2)),
                                ("15Min", self.end - timedelta(days=2, hours=-4)), ("1Hour", self.end)):
            repo = measured_repository(AnalyticsRepository, self.dsn)
            after = AnalyticsService(repo)
            before = type(self).before(AnalyticsRepository(self.dsn))
            expected = before.get_batch_state(symbols, timeframe, asof, AnalyticsSettings())
            for _ in range(2):
                with patch.object(repo, "fetch_symbol_contexts", wraps=repo.fetch_symbol_contexts) as contexts, \
                     patch.object(repo, "fetch_symbol_context", side_effect=AssertionError("per-symbol metadata")):
                    actual = after.get_batch_state(symbols, timeframe, asof, AnalyticsSettings())
                self.assertLegacyEqual(actual, expected)
                contexts.assert_called_once()
                self.assertIs(after.repository, repo)
            single = after.get_symbol_state("D021", timeframe, asof, AnalyticsSettings())
            self.assertNotIn("portfolio", single)
            self.assertNotIn("relative", single)
            # Ranks must be recomputed for a different portfolio, without cache contamination.
            self.assertLegacyEqual(after.get_batch_state(["D021"], timeframe, asof, AnalyticsSettings()),
                             before.get_batch_state(["D021"], timeframe, asof, AnalyticsSettings()))

    def test_mixed_future_parity_partial_windows_thresholds_and_invalid_inputs(self):
        symbols = ["D021", "MISSING", "D000", "D020", "D021", "SPY", "BENCH"]
        for timeframe, asof in (("1Day", self.end - timedelta(days=2)),
                                ("15Min", self.end - timedelta(days=2, hours=-4)), ("1Hour", self.end)):
            for side in ("long", "short"):
                repo = measured_repository(AnalyticsRepository, self.dsn)
                after = AnalyticsService(repo)
                before = type(self).before(AnalyticsRepository(self.dsn))
                for _ in range(2):
                    with patch.object(repo, "fetch_symbol_context", side_effect=AssertionError("per-symbol metadata")):
                        actual = after.get_batch_future_state(symbols, timeframe, asof, 20, 0.02, 0.02, side)
                    self.assertLegacyEqual(actual, before.get_batch_future_state(symbols, timeframe, asof, 20, 0.02, 0.02, side))
                with self.assertRaises(ValueError):
                    after.get_batch_future_state(symbols, timeframe, asof, 0)

    def test_portfolio_routes_hidden_filter_hidets_and_mixed_errors(self):
        with dashboard.app.test_client() as client:
            for suffix in ("", "&include_hidden=1", "&include_hidden=true&hidets=1"):
                for path in ("state", "futurestate"):
                    # Legacy parity applies to midnight daily labels. Date-only
                    # future requests intentionally no longer use 15Min windows;
                    # independent horizon assertions cover those responses.
                    asof = f"{self.end.date()}T00:00:00Z" if path == "futurestate" else self.end.date()
                    url = f"/api/portfolio/{path}?asof={asof}&days=20{suffix}"
                    payloads = []
                    for label in ("before", "after"):
                        with route_variant(self.dsn, label, type(self).before, self.namespace):
                            response = client.get(url)
                        self.assertEqual(response.status_code, 200, response.get_json())
                        payloads.append(response.get_json())
                    self.assertLegacyEqual(*payloads)
                    self.assertTrue(payloads[0]["errors"])
                    self.assertEqual(payloads[0]["portfolio_symbol_count"], 23 if suffix else 15)

    def test_warm_counts_and_sync_timestamp_invalidates_state_and_future(self):
        repo = measured_repository(AnalyticsRepository, self.dsn)
        service = AnalyticsService(repo)
        asof = self.end - timedelta(days=2)
        args = (["D021", "D021"], "1Day", asof)
        old_state = service.get_batch_state(*args, AnalyticsSettings())
        old_future = service.get_batch_future_state(*args, 2)
        for method, extra in ((service.get_batch_state, AnalyticsSettings()), (service.get_batch_future_state, 2)):
            repo.queries = 0
            method(*args, extra)
            self.assertEqual(repo.queries, 1)
        try:
            with psycopg2.connect(self.dsn) as conn, conn.cursor() as cur:
                cur.execute("UPDATE stock_bars SET close=close+10 WHERE symbol_id=(SELECT id FROM symbol_metadata WHERE symbol='D021') AND bar_time=%s AND timeframe='1Day'", (asof,))
                cur.execute("UPDATE symbol_sync_state SET last_bar_synced_at=last_bar_synced_at+interval '1 second' WHERE symbol_id=(SELECT id FROM symbol_metadata WHERE symbol='D021')")
            new_state = service.get_batch_state(*args, AnalyticsSettings())
            new_future = service.get_batch_future_state(*args, 2)
            self.assertNotEqual(new_state["states"][0]["price"], old_state["states"][0]["price"])
            self.assertNotEqual(new_future["future_states"][0]["anchor_price"], old_future["future_states"][0]["anchor_price"])
        finally:
            with psycopg2.connect(self.dsn) as conn, conn.cursor() as cur:
                cur.execute("UPDATE stock_bars SET close=close-10 WHERE symbol_id=(SELECT id FROM symbol_metadata WHERE symbol='D021') AND bar_time=%s AND timeframe='1Day'", (asof,))

    def test_read_chunks_cross_100_symbols_and_warm_hits_skip_bars(self):
        with batch_fixture(os.environ["ANALYTICS_TEST_DATABASE_URL"], count=105, bars=30) as (dsn, end):
            repo = measured_repository(AnalyticsRepository, dsn)
            service = AnalyticsService(repo)
            symbols = [f"D{i:03d}" for i in range(105)] + ["D000", "D104"]
            asof = end - timedelta(days=2)
            future = service.get_batch_future_state(symbols, "1Day", asof, 20)
            self.assertEqual(repo.queries, 5)  # contexts + two anchors + two windows
            self.assertEqual(future["errors"], [])
            self.assertEqual([state["symbol"] for state in future["future_states"]], symbols)
            self.assertTrue(all(state["window"]["future_bar_count"] == 2 for state in future["future_states"]))
            repo.queries = 0
            states = service.get_batch_state(symbols, "1Day", asof, AnalyticsSettings())
            self.assertEqual(repo.queries, 12)  # contexts + eleven history chunks, including benchmarks
            self.assertEqual(states["errors"], [])
            for method, extra in ((service.get_batch_state, AnalyticsSettings()), (service.get_batch_future_state, 20)):
                repo.queries = 0
                method(symbols, "1Day", asof, extra)
                self.assertEqual(repo.queries, 1)


if __name__ == "__main__":
    unittest.main()
