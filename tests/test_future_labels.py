from datetime import datetime, timedelta, timezone
import json
import os
import random
import unittest
from unittest.mock import MagicMock, patch

import pandas as pd

from analytics.data_access import AnalyticsRepository, LabelBar
from analytics.future_labels import scan_windows
from analytics.io_benchmark import measured_repository, price_fixture
from analytics.service import AnalyticsService, AnalyticsNotFoundError
from analytics.utils import parse_asof


class ReferenceService(AnalyticsService):
    _compute_future_state = AnalyticsService._compute_future_state_reference


class MemoryLabels(AnalyticsRepository):
    def __init__(self, bars):
        self.bars = bars

    def fetch_symbol_contexts(self, symbols):
        return {s: {"symbol": s, "symbol_id": 1} for s in symbols if s == "A"}

    def fetch_symbol_context(self, symbol):
        return self.fetch_symbol_contexts([symbol]).get(symbol)

    def fetch_label_bars_many(self, ids, timeframe, asof=None, *, latest=False, after=None, limit=None):
        return {sid: ([b for b in self.bars if asof is None or b.timestamp <= asof][-1:] if latest else
                      [b for b in self.bars if b.timestamp > after[sid]][:limit]) for sid in ids}

    def fetch_bars_many(self, ids, timeframe, asof=None, **kwargs):
        records = self.fetch_label_bars_many(ids, timeframe, asof, **kwargs)
        return {sid: pd.DataFrame([dict(timestamp=b.timestamp, high=b.high, low=b.low, close=b.close) for b in bars],
                                 columns=["timestamp", "high", "low", "close"]) for sid, bars in records.items()}

    def fetch_latest_bar(self, sid, tf, asof=None):
        return self.fetch_bars_many([sid], tf, asof, latest=True)[sid]

    def fetch_future_bars(self, sid, tf, after, limit):
        return self.fetch_bars_many([sid], tf, after={sid: after}, limit=limit)[sid]


class TypedLabelTests(unittest.TestCase):
    def test_native_batch_candidate_matches_python_including_nonfinite_values(self):
        try:
            from apollo_indicators import future_window_indices
        except ImportError:
            self.skipTest("Experimental Rust comparator not installed")
        self.assertEqual(future_window_indices([], [], None, None, False), [])
        self.assertEqual(scan_windows([], [], None, None, "long"), [])
        rng = random.Random(810)
        ts = datetime(2026, 1, 1, tzinfo=timezone.utc)
        windows = [[LabelBar(ts, rng.choice([float("nan"), float("inf"), 100, 110]),
                             rng.choice([float("nan"), -float("inf"), 90, 100]), 100)
                    for _ in range(n)] for n in (0, 1, 20, 252)]
        for side in ("long", "short"):
            for stop, take in ((None, None), (0, 0), (0.05, 0.1), (None, .1), (.1, None)):
                for anchors in ([0] * 4, [100] * 4, [float("nan")] * 4):
                    self.assertEqual(scan_windows(windows, anchors, stop, take, side),
                        future_window_indices([[(b.high, b.low) for b in w] for w in windows], anchors,
                                              stop, take, side == "short"))

    def test_full_payload_differential_edges_and_random_windows(self):
        rng = random.Random(810)
        ts = parse_asof("2026-03-17T09:30:00.123456-04:00")
        for tf in ("1Day", "1Hour", "15Min", "5Min", "1Min"):
            for close in (0.0, 100.0, float("nan"), float("inf")):
                # Repeated extrema, a same-bar double hit and nonfinite close.
                fixed = [LabelBar(ts, 100, 100, close),
                         LabelBar(ts + timedelta(microseconds=1), 110, 90, float("inf")),
                         LabelBar(ts + timedelta(minutes=1), 110, 90, 102)]
                random_bars = [LabelBar(ts + timedelta(minutes=i + 2), rng.randrange(100, 120),
                                       rng.randrange(80, 100), rng.randrange(90, 110)) for i in range(20)]
                repo = MemoryLabels(fixed + random_bars)
                for side in ("long", "short"):
                    for stop, take in ((None, None), (0, 0), (0.05, 0.05), (None, 0.10), (0.10, None)):
                        for days in (1, 2, 20, 252):
                            with self.subTest(tf=tf, close=close, side=side, days=days, stop=stop, take=take):
                                args = (["A", "MISSING", "A"], tf, ts, days, stop, take, side)
                                expected = ReferenceService(repo).get_batch_future_state(*args)
                                with patch.object(repo, "_bar_frame", side_effect=AssertionError("pandas frame")):
                                    actual = AnalyticsService(repo).get_batch_future_state(*args)
                                self.assertEqual(actual, expected)
                                json.dumps(actual, allow_nan=False)
                                self.assertEqual(actual["future_states"][0]["as_of"], "2026-03-17T13:30:00.123456Z")
                                self.assertEqual(actual["future_states"][0]["future_state"]["max_price_timestamp"],
                                                 expected["future_states"][0]["future_state"]["max_price_timestamp"])

    def test_missing_anchor_and_empty_window_are_isolated(self):
        ts = datetime(2026, 1, 1, tzinfo=timezone.utc)
        repo = MemoryLabels([LabelBar(ts, 1, 1, 1)])
        for asof in (ts - timedelta(microseconds=1), ts):
            args = (["A", "MISSING"], "1Min", asof, 1)
            self.assertEqual(AnalyticsService(repo).get_batch_future_state(*args),
                             ReferenceService(repo).get_batch_future_state(*args))

    def test_typed_query_preserves_microseconds_and_bounded_sql(self):
        repo = AnalyticsRepository()
        conn = MagicMock()
        cur = conn.__enter__.return_value.cursor.return_value.__enter__.return_value
        cur.fetchall.return_value = [(1, 1773754200123456, 110, 90, None)]
        ts = parse_asof("2026-03-17T13:30:00.123456Z")
        with patch.object(repo, "get_conn", return_value=conn), \
             patch.object(repo, "_bar_frame", side_effect=AssertionError("frame")):
            bars = repo.fetch_label_bars_many([1], "1Min", ts, latest=True)
            sql, params = cur.execute.call_args.args
            self.assertIn("bar_time <= %s ORDER BY bar_time DESC LIMIT %s)", sql)
            self.assertEqual(bars[1][0].timestamp, ts)
            self.assertEqual(bars[1][0].high, 110.0)
            repo.fetch_label_bars_many([1], "1Min", after={1: ts}, limit=20)
            sql, params = cur.execute.call_args.args
            self.assertIn("bar_time > anchors.bar_time ORDER BY bar_time ASC LIMIT %s)", sql)
            self.assertEqual(params[1], [ts])
            self.assertEqual(params[-1], 20)
        with self.assertRaises(ValueError):
            repo.fetch_label_bars_many([1], "1Day")


@unittest.skipUnless(os.environ.get("ANALYTICS_TEST_DATABASE_URL"), "Set test PostgreSQL DSN")
class TypedLabelPostgreSQLTests(unittest.TestCase):
    def test_real_chunk_limits_and_inclusive_microsecond_anchors(self):
        import psycopg2
        with price_fixture(os.environ["ANALYTICS_TEST_DATABASE_URL"], daily_count=2, intraday_count=2) as (dsn, sid):
            ts = parse_asof("2026-03-17T09:30:00.123456-04:00")
            with psycopg2.connect(dsn) as conn, conn.cursor() as cur:
                cur.execute("INSERT INTO symbol_metadata (symbol) SELECT 'A' || n FROM generate_series(1, 205) n")
                cur.execute("""INSERT INTO stock_bars (symbol_id, timeframe, bar_time, open, high, low, close, volume)
                    SELECT id, tf, %s::timestamptz + n * interval '1 microsecond', 100, 110, 90, 100, 1
                    FROM symbol_metadata CROSS JOIN unnest(ARRAY['1Day','1Hour','15Min','5Min','1Min']) tf
                    CROSS JOIN generate_series(0, 2) n WHERE symbol != 'BENCH'""", (ts,))
            symbols = [f"A{i}" for i in range(1, 206)] + ["MISSING", "BENCH"]
            for tf in ("1Day", "1Hour", "15Min", "5Min", "1Min"):
                repo = measured_repository(AnalyticsRepository, dsn)
                args = (symbols, tf, ts, 1, 0, 0, "long")
                expected = ReferenceService(AnalyticsRepository(dsn)).get_batch_future_state(*args)
                with patch.object(repo, "_bar_frame", side_effect=AssertionError("frame")), \
                     patch("analytics.future_labels.scan_windows", wraps=scan_windows) as scan:
                    actual = AnalyticsService(repo).get_batch_future_state(*args)
                self.assertEqual(actual, expected)
                self.assertEqual(len(actual["future_states"]), 205)
                self.assertEqual(len(actual["errors"]), 2)
                self.assertEqual(repo.queries, 7)  # metadata + three anchor/window chunks
                self.assertLessEqual(repo.rows, 206 + 206 * 2)
                self.assertEqual(scan.call_count, 3)
                self.assertTrue(all(len(call.args[0]) <= 100 for call in scan.call_args_list))
                label = actual["future_states"][0]
                self.assertEqual(label["as_of"], "2026-03-17T13:30:00.123456Z")
                self.assertEqual(label["window"]["realized_end_timestamp"], "2026-03-17T13:30:00.123457Z")
                self.assertEqual(label["future_state"]["first_threshold_hit"], "both")

    def test_real_bounded_rows_and_no_per_symbol_frames(self):
        with price_fixture(os.environ["ANALYTICS_TEST_DATABASE_URL"], daily_count=8000, intraday_count=300) as (dsn, sid):
            repo = measured_repository(AnalyticsRepository, dsn)
            for tf in ("1Day", "15Min", "1Hour", "5Min", "1Min"):
                for horizon in (1, 20, 252):
                    args = (["BENCH", "MISSING", "BENCH"], tf, parse_asof("2020-01-01T00:00:00Z"), horizon, 0, 0, "short")
                    expected = ReferenceService(AnalyticsRepository(dsn)).get_batch_future_state(*args)
                    repo.queries = repo.rows = 0
                    with patch.object(repo, "_bar_frame", side_effect=AssertionError("per-symbol frame")):
                        service = AnalyticsService(repo)
                        actual = service.get_batch_future_state(*args)
                        self.assertEqual(actual, expected)
                        self.assertLessEqual(repo.queries, 3)
                        self.assertLessEqual(repo.rows, 2 + horizon)
                        repo.queries = repo.rows = 0
                        self.assertEqual(service.get_batch_future_state(*args), expected)
                        self.assertEqual(repo.queries, 1 if actual["future_states"] else 2)


if __name__ == "__main__":
    unittest.main()
