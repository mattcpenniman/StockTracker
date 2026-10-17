from concurrent.futures import ThreadPoolExecutor
import threading
import time
import unittest
from unittest.mock import Mock, patch

import app
from live_prices import PriceCache, Quote, YahooQuotes, parse_quote


class PriceCacheTests(unittest.TestCase):
    def cache(self, provider, **kwargs):
        cache = PriceCache(provider, **kwargs)
        self.addCleanup(cache.pool.shutdown)
        return cache

    def test_fresh_expired_missing_and_failed_refresh(self):
        now = [100.0]
        provider = Mock()
        provider.fetch.side_effect = [{"A": Quote(10, 1)}, {"A": Quote(12, 2)}, RuntimeError("offline")]
        cache = self.cache(provider, ttl=15, retry_seconds=3, clock=lambda: now[0])
        self.assertEqual(cache.get_prices(["a", "B", "A"]), {"A": 10, "B": None})
        now[0] += 2
        self.assertEqual(cache.get_prices(["B", "A"]), {"A": 10, "B": None})
        self.assertEqual(provider.fetch.call_count, 1)
        now[0] += 13
        self.assertEqual(cache.get_prices(["A", "B"]), {"A": 12, "B": None})
        now[0] += 15
        self.assertEqual(cache.get_prices(["A", "B"]), {"A": None, "B": None})
        self.assertEqual(cache.get_prices(["A", "B"]), {"A": None, "B": None})
        self.assertEqual(provider.fetch.call_count, 3)
        self.assertEqual(cache.get_prices([]), {})

    def test_identical_and_overlapping_concurrency_coalesces(self):
        started, release = threading.Event(), threading.Event()

        def fetch(symbols, publish):
            started.set()
            self.assertTrue(release.wait(2))
            return {s: Quote(10, 1) for s in symbols}

        provider = Mock(fetch=Mock(side_effect=fetch))
        cache = self.cache(provider)
        with ThreadPoolExecutor(max_workers=20) as pool:
            first = pool.submit(cache.get_prices, ["A", "B"])
            self.assertTrue(started.wait(1))
            rest = [pool.submit(cache.get_prices, ["B", "A"] if i % 2 else ["A"]) for i in range(19)]
            release.set()
            self.assertEqual(first.result(), {"A": 10, "B": 10})
            for result in rest:
                self.assertEqual(result.result()["A"], 10)
        self.assertEqual(provider.fetch.call_count, 1)
        self.assertEqual(cache.get_prices(["A", "C"]), {"A": 10, "C": 10})
        self.assertEqual(provider.fetch.call_args.args[0], ["C"])

    def test_timeout_retains_healthy_partial_quotes_and_does_not_queue_refreshes(self):
        release = threading.Event()

        def fetch(symbols, publish):
            publish({"A": Quote(10, 1)})
            release.wait(2)
            return {"A": Quote(10, 1), "B": Quote(20, 2)}

        provider = Mock(fetch=Mock(side_effect=fetch))
        cache = self.cache(provider, wait_seconds=0.03)
        try:
            start = time.monotonic()
            for _ in range(3):
                self.assertEqual(cache.get_prices(["A", "B"]), {"A": 10, "B": None})
            self.assertLess(time.monotonic() - start, 0.5)
            self.assertEqual(provider.fetch.call_count, 1)
        finally:
            release.set()
        self.assertEqual(cache.get_prices(["A", "B"]), {"A": 10, "B": 20})

    def test_fresh_symbols_survive_another_symbols_failure_and_old_entries_are_pruned(self):
        now = [0.0]
        provider = Mock()
        provider.fetch.side_effect = [{"A": Quote(10, 1)}, RuntimeError(), {}]
        cache = self.cache(provider, clock=lambda: now[0])
        self.assertEqual(cache.get_prices(["A"]), {"A": 10})
        now[0] += 1
        self.assertEqual(cache.get_prices(["A", "B"]), {"A": 10, "B": None})
        now[0] += 20
        cache.get_prices(["C"])
        self.assertNotIn("A", cache.entries)

    def test_long_refresh_does_not_rejuvenate_early_quotes(self):
        now = [0.0]
        published, release = threading.Event(), threading.Event()

        def fetch(symbols, publish):
            publish({"A": Quote(10, 1)})
            published.set()
            release.wait(2)
            return {"A": Quote(10, 1)}

        cache = self.cache(Mock(fetch=fetch), clock=lambda: now[0], wait_seconds=0.03)
        try:
            self.assertEqual(cache.get_prices(["A", "B"]), {"A": 10, "B": None})
            self.assertTrue(published.is_set())
            now[0] += 16
        finally:
            release.set()
        with cache.condition:
            cache.condition.wait_for(lambda: cache.inflight is None, timeout=1)
        self.assertEqual(cache.get_prices(["A", "B"]), {"A": None, "B": None})


class ProviderTests(unittest.TestCase):
    def provider(self):
        provider = YahooQuotes(timeout=0.2, workers=2)
        self.addCleanup(provider.pool.shutdown)
        return provider

    @patch("yfinance.data.YfData")
    def test_batch_is_one_request_and_uses_regular_market_price(self, data):
        data.return_value.get_raw_json.return_value = {"quoteResponse": {"result": [
            {"symbol": s, "regularMarketPrice": 10, "regularMarketTime": 123} for s in ("A", "B")
        ]}}
        self.assertEqual(self.provider().fetch(["A", "B"]), {"A": Quote(10, 123), "B": Quote(10, 123)})
        call = data.return_value.get_raw_json
        self.assertEqual(call.call_count, 1)
        self.assertEqual(call.call_args.kwargs, {"params": {"symbols": "A,B"}, "timeout": 0.2})

    @patch("yfinance.data.YfData")
    def test_partial_batch_fallback_and_invalid_prices(self, data):
        def request(url, **kwargs):
            if "/v7/" in url:
                return {"quoteResponse": {"result": [{"symbol": "A", "regularMarketPrice": 10}]}}
            if url.endswith("/B"):
                return {"chart": {"result": [{"meta": {"regularMarketPrice": 20, "regularMarketTime": 456}}]}}
            raise TimeoutError()

        data.return_value.get_raw_json.side_effect = request
        self.assertEqual(self.provider().fetch(["A", "B", "C"]), {"A": Quote(10, None), "B": Quote(20, 456)})
        self.assertEqual(data.return_value.get_raw_json.call_count, 3)
        for value in (None, 0, -1, "nan", "inf", "bad"):
            self.assertIsNone(parse_quote(value))

    @patch("yfinance.data.YfData")
    def test_unsupported_batch_falls_back_concurrently(self, data):
        barrier = threading.Barrier(2)

        def request(url, **kwargs):
            if "/v7/" in url:
                raise RuntimeError("401")
            barrier.wait(timeout=1)
            return {"chart": {"result": [{"meta": {"regularMarketPrice": 10}}]}}

        data.return_value.get_raw_json.side_effect = request
        self.assertEqual(len(self.provider().fetch(["A", "B"])), 2)


class DashboardPriceTests(unittest.TestCase):
    def test_cached_live_price_preserves_price_dependent_calculations(self):
        import pandas as pd

        provider = Mock()
        provider.fetch.return_value = {"A": Quote(100, 1)}
        cache = PriceCache(provider)
        self.addCleanup(cache.pool.shutdown)
        df = pd.DataFrame([{"symbol": s, "forecast_price": 125, "hidden": False} for s in ("A", "B")])
        closes = {s: [80.0] * 250 for s in ("A", "B")}
        with patch.object(app, "_live_prices", cache), patch.object(app, "_read_df", return_value=df), \
             patch.object(app, "_load_next_earnings_map", return_value={}), \
             patch.object(app, "_daily_bar_snapshot", return_value=(closes, {})):
            with app.app.test_client() as client:
                before = client.get("/data").get_json()
                self.assertEqual(client.get("/data").get_json(), before)
        self.assertEqual(provider.fetch.call_count, 1)
        a, b = before["rows"]
        self.assertEqual((a["current_price"], a["diff_dollar"], a["diff_pct"], a["dist_ma20_pct"]), (100, 25, 25, 25))
        self.assertIsNone(b["current_price"])
        self.assertIsNone(b["diff_pct"])
        self.assertEqual(b["dist_ma20_pct"], 0)  # Existing MA-only historical-close fallback.


if __name__ == "__main__":
    unittest.main()
