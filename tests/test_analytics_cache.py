from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace
import os
from threading import Barrier, Event
import time
import unittest
from unittest.mock import Mock, patch

import pandas as pd

from analytics.cache import CacheCapacityError, _StateCache
from analytics.config import AnalyticsSettings
from analytics.service import AnalyticsService


class CacheTests(unittest.TestCase):
    def await_count(self, cache, name, count):
        deadline = time.monotonic() + 5
        while cache.diagnostics()[name] < count and time.monotonic() < deadline:
            time.sleep(.001)
        self.assertGreaterEqual(cache.diagnostics()[name], count)

    def test_single_flight_and_independent_keys_and_immutable_results(self):
        cache = _StateCache()
        entered, release = Event(), Event()
        original = {"nested": [1]}

        def compute():
            entered.set()
            self.assertTrue(release.wait(5))
            return original

        with ThreadPoolExecutor(10) as pool:
            first = pool.submit(cache.get_or_compute, ("a",), compute)
            self.assertTrue(entered.wait(5))
            others = [pool.submit(cache.get_or_compute, ("a",), compute) for _ in range(8)]
            try:
                self.await_count(cache, "coalesced_waits", 8)
                self.assertEqual(pool.submit(cache.get_or_compute, ("b",), lambda: {"b": 2}).result(2), {"b": 2})
            finally:
                release.set()
            results = [f.result(5) for f in [first, *others]]
        results[0]["nested"].append(2)
        original["nested"].append(3)
        self.assertEqual(results[1], {"nested": [1]})
        self.assertEqual(cache.get_or_compute(("a",), lambda: self.fail("cache miss")), {"nested": [1]})
        self.assertEqual(cache.diagnostics()["computations"], 2)
        self.assertEqual(cache.diagnostics()["in_flight"], 0)

    def test_failure_and_baseexception_wake_all_waiters_and_retry(self):
        for error in (ValueError("producer failed"), BaseException("cancelled")):
            cache = _StateCache()
            entered, release = Event(), Event()

            def fail():
                entered.set()
                release.wait(5)
                raise error

            with ThreadPoolExecutor(5) as pool:
                first = pool.submit(cache.get_or_compute, (1,), fail)
                self.assertTrue(entered.wait(5))
                others = [pool.submit(cache.get_or_compute, (1,), fail) for _ in range(4)]
                try:
                    self.await_count(cache, "coalesced_waits", 4)
                finally:
                    release.set()
                for future in [first, *others]:
                    with self.assertRaises(type(error)):
                        future.result(5)
            self.assertEqual(cache.diagnostics()["in_flight"], 0)
            self.assertEqual(cache.get_or_compute((1,), lambda: {"retry": True}), {"retry": True})

    def test_bounded_flights_reject_admission_without_blocking_recursion(self):
        cache = _StateCache(max_inflight=1)
        entered, release = Event(), Event()
        with ThreadPoolExecutor(1) as pool:
            producer = pool.submit(cache.get_or_compute, (1,), lambda: (entered.set(), release.wait(5), {})[-1])
            self.assertTrue(entered.wait(5))
            try:
                for key in range(2, 1000):
                    with self.assertRaises(CacheCapacityError):
                        cache.get_or_compute((key,), lambda: {})
                self.assertEqual(cache.diagnostics()["in_flight"], 1)
            finally:
                release.set()
            producer.result(5)
        with self.assertRaisesRegex(RuntimeError, "Recursive"):
            cache.get_or_compute((2,), lambda: cache.get_or_compute((2,), lambda: {}))
        self.assertEqual(cache.diagnostics()["in_flight"], 0)
        self.assertEqual(cache.get_or_compute((2,), lambda: {}), {})

    def test_waiters_are_bounded_and_released(self):
        cache = _StateCache(max_waiters=1)
        entered, release = Event(), Event()
        with ThreadPoolExecutor(2) as pool:
            producer = pool.submit(cache.get_or_compute, (1,), lambda: (entered.set(), release.wait(5), {})[-1])
            self.assertTrue(entered.wait(5))
            waiter = pool.submit(cache.get_or_compute, (1,), lambda: self.fail("duplicate producer"))
            try:
                self.await_count(cache, "coalesced_waits", 1)
                with self.assertRaisesRegex(CacheCapacityError, "waiter budget"):
                    cache.get_or_compute((1,), lambda: {})
                self.assertEqual(cache.diagnostics()["waiters"], 1)
            finally:
                release.set()
            producer.result(5)
            waiter.result(5)
        self.assertEqual(cache.diagnostics()["waiters"], 0)

    def test_recursive_cross_key_producers_cannot_deadlock(self):
        cache = _StateCache()
        barrier = Barrier(2)

        def produce(key, other):
            def compute():
                barrier.wait(5)
                return cache.get_or_compute((other,), lambda: {})
            return cache.get_or_compute((key,), compute)

        with ThreadPoolExecutor(2) as pool:
            results = [pool.submit(produce, "a", "b"), pool.submit(produce, "b", "a")]
            failures = 0
            for result in results:
                try:
                    result.result(5)
                except RuntimeError:
                    failures += 1
        self.assertGreaterEqual(failures, 1)
        self.assertEqual(cache.diagnostics()["in_flight"], 0)

    def test_concurrent_eviction_broad_scan_and_oversized_entries(self):
        cache = _StateCache(maxsize=7, maxbytes=4096, max_inflight=16)
        start = Barrier(8)

        def scan(worker):
            start.wait(5)
            for day in range(200):
                value = {"day": day, "value": "x" * 300}
                self.assertEqual(cache.get_or_compute((worker, day), lambda: value), value)
                stats = cache.diagnostics()
                self.assertLessEqual(stats["current_bytes"], 4096)
                self.assertLessEqual(stats["entries"], 7)
                self.assertLessEqual(stats["in_flight"], 16)

        with ThreadPoolExecutor(8) as pool:
            list(pool.map(scan, range(8)))
        self.assertGreater(cache.diagnostics()["evictions"], 1500)
        before = cache.diagnostics()["entries"]
        for _ in range(2):
            self.assertEqual(len(cache.get_or_compute(("large",), lambda: {"v": "x" * 5000})["v"]), 5000)
        self.assertEqual(cache.diagnostics()["entries"], before)
        self.assertEqual(cache.diagnostics()["oversized"], 2)
        self.assertEqual(cache.diagnostics()["in_flight"], 0)
        from analytics.cache_benchmark import retained_size
        self.assertLessEqual(retained_size(cache._data), cache.maxbytes)

    def test_configuration_is_validated_per_cache(self):
        for raw in ("0", "-1", "banana", "1.5"):
            with patch.dict(os.environ, {"ANALYTICS_STATE_CACHE_BYTES": raw}):
                with self.assertRaisesRegex(ValueError, "ANALYTICS_STATE_CACHE_BYTES"):
                    AnalyticsService(Mock())
        with patch.dict(os.environ, {"ANALYTICS_STATE_CACHE_ENTRIES": "512", "ANALYTICS_FUTURE_CACHE_BYTES": "123456"}):
            stats = AnalyticsService(Mock()).cache_diagnostics()
            self.assertEqual(stats["state"]["max_entries"], 512)
            self.assertEqual(stats["future"]["max_bytes"], 123456)
            self.assertEqual(stats["event"]["max_entries"], 256)


class ServiceCacheTests(unittest.TestCase):
    def setUp(self):
        self.context = {"symbol": "A", "symbol_id": 1, "last_bar_synced_at": pd.Timestamp("2026-01-01T00:00:00Z")}
        self.repo = Mock()
        self.repo.fetch_symbol_context.return_value = self.context
        self.repo.fetch_symbol_contexts.return_value = {"A": self.context}
        self.service = AnalyticsService(self.repo)
        self.asof = pd.Timestamp("2025-12-01T00:00:00Z")
        self.settings = AnalyticsSettings()

    def test_all_key_dimensions_and_revision_invalidate(self):
        state_args = ("A", "1Day", self.asof, self.settings)
        future_args = ("A", "1Day", self.asof, 20, .05, .10, "long")
        with patch.object(self.service, "_compute_symbol_state", return_value={"state": 1}) as state, \
             patch.object(self.service, "_compute_future_state", return_value={"future": 1}) as future, \
             patch.object(self.service, "_compute_events", return_value={"events": []}) as events:
            for _ in range(2):
                self.service.get_symbol_state(*state_args)
                self.service.get_future_state(*future_args)
                self.service.get_events(*state_args)
            self.assertEqual((state.call_count, future.call_count, events.call_count), (1, 1, 1))
            for index, alternative in ((1, "15Min"), (2, self.asof + pd.Timedelta(days=1)),
                                       (3, replace(self.settings, buffer_pct=.01))):
                args = list(state_args)
                args[index] = alternative
                self.service.get_symbol_state(*args)
                self.service.get_events(*args)
            self.service.get_events(*state_args, event_limit=3)
            for index, alternative in ((1, "15Min"), (2, self.asof + pd.Timedelta(days=1)),
                                       (3, 10), (4, .02), (5, .20), (6, "short")):
                args = list(future_args)
                args[index] = alternative
                self.service.get_future_state(*args)
            self.assertEqual((state.call_count, future.call_count, events.call_count), (4, 7, 5))
            self.context["last_bar_synced_at"] += pd.Timedelta(seconds=1)
            self.service.get_symbol_state(*state_args)
            self.service.get_future_state(*future_args)
            self.service.get_events(*state_args)
            self.assertEqual((state.call_count, future.call_count, events.call_count), (5, 8, 6))

    def test_real_data_revision_and_returned_payload_isolation(self):
        frame = pd.DataFrame({"timestamp": [self.asof], "open": [100.], "high": [110.],
                              "low": [90.], "close": [100.], "volume": [1000.]})
        self.repo.fetch_bars.return_value = frame
        self.repo.fetch_latest_bar.return_value = frame
        self.repo.fetch_future_bars.return_value = frame
        for method, args in ((self.service.get_symbol_state, ("A", "1Day", self.asof, self.settings)),
                             (self.service.get_events, ("A", "1Day", self.asof, self.settings)),
                             (self.service.get_future_state, ("A", "1Day", self.asof, 1))):
            result = method(*args)
            expected = method(*args)
            result.clear()
            self.assertEqual(method(*args), expected)
        old = self.service.get_symbol_state("A", "1Day", self.asof, self.settings)
        frame.loc[0, "close"] = 105.
        self.context["last_bar_synced_at"] += pd.Timedelta(seconds=1)
        new = self.service.get_symbol_state("A", "1Day", self.asof, self.settings)
        self.assertNotEqual(old["price"]["close"], new["price"]["close"])

    def test_concurrent_batch_requests_share_producer_and_keep_reads_request_local(self):
        entered, release = Event(), Event()
        frame = pd.DataFrame({"timestamp": [self.asof], "open": [100.], "high": [110.],
                              "low": [90.], "close": [100.], "volume": [1000.]})

        def read(ids, *args, **kwargs):
            entered.set()
            self.assertTrue(release.wait(5))
            return {sid: frame for sid in ids}

        self.repo.fetch_bars_many.side_effect = read
        for name, cache, args in (("get_batch_state", self.service._state_cache, (["A"], "1Day", self.asof, self.settings)),
                                  ("get_batch_future_state", self.service._future_cache, (["A"], "1Day", self.asof, 1))):
            entered.clear()
            release.clear()
            with ThreadPoolExecutor(4) as pool:
                first = pool.submit(getattr(self.service, name), *args)
                self.assertTrue(entered.wait(5))
                others = [pool.submit(getattr(self.service, name), *args) for _ in range(3)]
                try:
                    CacheTests.await_count(self, cache, "coalesced_waits", 3)
                finally:
                    release.set()
                results = [f.result(5) for f in [first, *others]]
            self.assertTrue(all(result == results[0] for result in results))
            self.assertEqual(cache.diagnostics()["computations"], 1)
            self.assertIs(self.service.repository, self.repo)
        # One daily state history read, one future anchor and one future window.
        self.assertEqual(self.repo.fetch_bars_many.call_count, 3)

    def test_admission_failure_is_collected_per_symbol(self):
        for method, producer, args, result_key in (
            (self.service.get_batch_state, "_compute_symbol_state", (["A", "MISSING"], "1Day", self.asof, self.settings), "states"),
            (self.service.get_batch_future_state, "_compute_future_state", (["A", "MISSING"], "1Day", self.asof, 1), "future_states"),
        ):
            with patch.object(self.service, producer, side_effect=CacheCapacityError("retry later")):
                result = method(*args)
            self.assertEqual(result[result_key], [])
            self.assertEqual(result["errors"][0], {"symbol": "A", "error": "retry later"})
            self.assertEqual(result["errors"][1]["symbol"], "MISSING")
