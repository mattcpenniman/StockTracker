from concurrent.futures import ThreadPoolExecutor
from dataclasses import asdict, fields, replace
import json
from threading import Barrier
import unittest
from unittest.mock import Mock, patch

import numpy as np
import pandas as pd

import app
from analytics.cache import _StateCache
from analytics.config import AnalyticsSettings
from analytics.service import AnalyticsService
from analytics.utils import clean_json_value, parse_asof
from analytics.warm_response_benchmark import baseline_types


def repository():
    repo = Mock()
    contexts = {s: dict(symbol=s, symbol_id=i) for i, s in enumerate(("A", "B", "SPY", "QQQ"))}
    repo.fetch_symbol_contexts.side_effect = lambda symbols: {s: contexts[s] for s in symbols if s in contexts}
    repo.fetch_symbol_context.side_effect = contexts.get
    frames = {}
    for i in range(4):
        close = np.arange(300, dtype=float) * (i + 1) + 100
        frames[i] = pd.DataFrame(dict(timestamp=pd.date_range("2025-01-01", periods=300, tz="UTC"),
            open=close, close=close, high=close + 2, low=close - 2, volume=np.full(300, 1000.)))
    # QQQ has stale, misaligned coverage, not an HTTP/symbol failure.
    frames[3] = frames[3].iloc[:200]

    def read(sid, tf, asof=None, after=None, limit=None, latest=False):
        frame = frames[sid]
        if asof is not None:
            frame = frame[frame.timestamp <= asof]
        if after is not None:
            frame = frame[frame.timestamp > after]
        return frame.tail(1) if latest else frame.head(limit)

    repo.fetch_bars_many.side_effect = lambda ids, tf, asof=None, after=None, **kw: {
        sid: read(sid, tf, asof, after.get(sid) if after else None, **kw) for sid in ids}
    repo.fetch_bars.side_effect = read
    repo.fetch_latest_bar.side_effect = lambda sid, tf, asof: read(sid, tf, asof, latest=True)
    repo.fetch_future_bars.side_effect = lambda sid, tf, after, limit: read(sid, tf, after=after, limit=limit)
    return repo


def vandalize(value):
    """Mutate every returned container, including all nested quality mappings."""
    if isinstance(value, dict):
        for item in list(value.values()):
            vandalize(item)
        value["corrupt"] = True
    elif isinstance(value, list):
        for item in value:
            vandalize(item)
        value.append("corrupt")


class WarmResponseTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.before, cls.before_settings = baseline_types()

    def cases(self):
        for symbols in (["A", "B", "SPY", "A", "MISSING"], ["B"], []):
            for date, settings in (("2025-09-15", {}), ("2025-09-25T00:00:00Z", {"buffer_pct": .03, "volume_confirmation": False})):
                yield symbols, parse_asof(date), settings

    def test_complete_cold_warm_parity_and_sequential_mutation_isolation(self):
        after, before = AnalyticsService(repository()), type(self).before(repository())
        for symbols, date, values in self.cases():
            for endpoint in ("state", "future"):
                def request(service, settings_type):
                    if endpoint == "state":
                        return service.get_batch_state(symbols, "1Day", date, settings_type(**values))
                    return service.get_batch_future_state(symbols, "1Day", date, 20, .05, .1, "short")
                expected = request(before, type(self).before_settings)
                for _ in range(2):
                    actual = request(after, AnalyticsSettings)
                    self.assertEqual(actual, expected)
                    self.assertEqual(json.loads(json.dumps(actual, allow_nan=False)), actual)
                    vandalize(actual)
                self.assertEqual(request(after, AnalyticsSettings), expected)
        single = after.get_symbol_state("A", "1Day", parse_asof("2025-09-15"), AnalyticsSettings())
        self.assertNotIn("portfolio", single)
        self.assertNotIn("relative", single)
        self.assertNotIn("benchmark_coverage", single["data_quality"])

    def test_concurrent_different_portfolios_dates_settings_and_mutation(self):
        service = AnalyticsService(repository())
        cases = list(self.cases())

        def request(service, case):
            symbols, date, values = case
            return (service.get_batch_state(symbols, "1Day", date, AnalyticsSettings(**values)),
                    service.get_batch_future_state(symbols, "1Day", date, 20, .02, .04))

        expected = [request(AnalyticsService(repository()), case) for case in cases]
        start = Barrier(len(cases))

        def worker(index):
            start.wait(10)
            for _ in range(3):
                actual = request(service, cases[index])
                self.assertEqual(actual, expected[index])
                for payload in actual:
                    vandalize(payload)

        with ThreadPoolExecutor(len(cases)) as pool:
            list(pool.map(worker, range(len(cases))))
        for index, case in enumerate(cases):
            self.assertEqual(request(service, case), expected[index])

    def test_routes_full_json_parity_hidden_hidets_and_missing_benchmarks(self):
        for endpoint in ("state", "futurestate"):
            for suffix in ("", "&include_hidden=1", "&include_hidden=true&hidets=true"):
                payloads = []
                for service_type, settings_type in ((type(self).before, type(self).before_settings), (AnalyticsService, AnalyticsSettings)):
                    repo = repository()
                    # Missing benchmark and per-symbol errors coexist.
                    contexts = repo.fetch_symbol_contexts.side_effect
                    repo.fetch_symbol_contexts.side_effect = lambda symbols: {s: c for s, c in contexts(symbols).items() if s != "QQQ"}
                    with patch.object(app, "analytics_service", service_type(repo)), \
                         patch.object(app, "AnalyticsSettings", settings_type), \
                         patch.object(app, "_init_db_if_needed"), \
                         patch.object(app, "_portfolio_symbols", side_effect=lambda include_hidden=False: ["A", "B", "MISSING"] if include_hidden else ["A"]), \
                         app.app.test_client() as client:
                        path = f"/api/portfolio/{endpoint}?asof=2025-09-15&days=20&buffer_pct=.02{suffix}"
                        cold = client.get(path)
                        warm = client.get(path)
                    self.assertEqual(cold.status_code, 200, cold.get_json())
                    self.assertEqual(cold.get_json(), warm.get_json())
                    payloads.append(cold.get_json())
                self.assertEqual(*payloads)

    def test_settings_key_matches_old_format_once_and_covers_every_field(self):
        settings = AnalyticsSettings()
        expected = tuple(sorted(asdict(settings).items()))
        with patch("analytics.config.fields", wraps=fields) as build:
            service = AnalyticsService(repository())
            service.get_batch_state(["A", "B"], "1Day", parse_asof("2025-09-15"), settings)
            self.assertEqual(build.call_count, 1)
            self.assertIs(settings.cache_key(), settings.cache_key())
        self.assertEqual(settings.cache_key(), expected)
        for field in fields(settings):
            value = getattr(settings, field.name)
            changed = replace(settings, **{field.name: not value if isinstance(value, bool) else value + 1})
            self.assertNotEqual(changed.cache_key(), expected, field.name)
            self.assertEqual(changed.cache_key(), tuple(sorted(asdict(changed).items())))

    def test_normalized_cache_contract_and_new_relative_overflow(self):
        cache = _StateCache()
        raw = dict(integer=np.int64(7), boolean=np.bool_(True), values=[np.nan, np.inf, -np.inf, None],
                   timestamp=pd.Timestamp("2025-09-15T00:00:00Z"), empty={})
        expected = dict(integer=7, boolean=True, values=[None] * 4, timestamp="2025-09-15T00:00:00Z", empty={})
        self.assertEqual(cache.get_or_compute((1,), lambda: clean_json_value(raw)), expected)
        self.assertEqual(cache.get_or_compute((1,), lambda: self.fail("miss")), expected)
        for value in (np.int64(1), np.bool_(True), np.nan, pd.Timestamp("2025-01-01")):
            with self.assertRaises((TypeError, ValueError)):
                cache.get_or_compute((2,), lambda: {"invalid": value})
        service = AnalyticsService(repository())
        def state(context, *args):
            return dict(symbol=context["symbol"], returns={"r_20": -1e308 if context["symbol"] == "SPY" else 1e308},
                        data_quality={"indicator_bar_timestamp": "same", "historical_coverage": {}})
        with patch.object(service, "_compute_symbol_state", side_effect=state):
            payload = service.get_batch_state(["A", "SPY"], "1Day", None, AnalyticsSettings())
        self.assertIsNone(payload["states"][0]["relative"]["rel_return_spy_20"])
        json.dumps(payload, allow_nan=False)
