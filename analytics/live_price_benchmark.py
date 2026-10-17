"""Real PostgreSQL/Flask /data, controlled quote transport before/after VEC-782.

Provider delays are synthetic, not claims about Yahoo's live service. Each old
fast_info access models one chart request (a conservative baseline: real
yfinance may need additional metadata/history requests). Each new bulk call
models one upstream quote request, excluding authentication handshakes/retries.
"""
from __future__ import annotations

import argparse
import ast
import os
import statistics
import subprocess
import time
from types import SimpleNamespace
from unittest.mock import patch

import numpy as np
import psycopg2

import app
from live_prices import PriceCache, YahooQuotes
from .dashboard_benchmark import dashboard_fixture


class Transport:
    def __init__(self, delay, offset):
        self.delay = delay
        self.offset = offset
        self.calls = 0
        self.timestamps = {}

    def quote(self, symbol):
        self.timestamps[symbol] = time.time() + self.offset[0] - 60
        return {"symbol": symbol, "regularMarketPrice": 107.5,
                "regularMarketTime": self.timestamps[symbol]}

    def get_raw_json(self, url, params, timeout):
        self.calls += 1
        time.sleep(self.delay)
        return {"quoteResponse": {"result": [self.quote(s) for s in params["symbols"].split(",")]}}

    def tickers(self, symbols):
        transport = self

        class Ticker:
            def __init__(self, symbol):
                self.symbol = symbol

            @property
            def fast_info(self):
                transport.calls += 1
                time.sleep(transport.delay)
                return {"last_price": transport.quote(self.symbol)["regularMarketPrice"]}

        return SimpleNamespace(tickers={s: Ticker(s) for s in symbols.split()})


def old_fetch(ref):
    source = subprocess.check_output(["git", "show", f"{ref}:app.py"], text=True)
    functions = [n for n in ast.parse(source).body if isinstance(n, ast.FunctionDef) and n.name == "_fetch_current_prices"]
    namespace = dict(vars(app))
    exec(compile(ast.Module(body=functions, type_ignores=[]), "price-baseline", "exec"), namespace)
    return namespace


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repeats", type=int, default=20)
    parser.add_argument("--baseline-ref", default="f1816b4")
    args = parser.parse_args()
    dsn = os.environ.get("ANALYTICS_TEST_DATABASE_URL")
    if not dsn or args.repeats < 2:
        parser.error("Set ANALYTICS_TEST_DATABASE_URL; --repeats must be >= 2")
    baseline = old_fetch(args.baseline_ref)
    original_fetch = app._fetch_current_prices
    print("Real PostgreSQL (5000 daily bars/symbol), Flask /data; synthetic upstream transport.")
    print("Warm DB/OS caches. Cold = empty quote cache; warm = +5s simulated cache/market clock.")
    print("Baseline models >=1 request/symbol; auth/retries excluded. All quote values identical.")
    print("scenario symbols version read median_ms p95_ms requests/read max_quote_age_s")
    for size in (10, 50, 100):
        with dashboard_fixture(dsn, size) as private_dsn, \
             patch.object(app, "_get_conn", lambda: psycopg2.connect(private_dsn)):
            for scenario, delay in (("normal-1ms", 0.001), ("slow-10ms", 0.01)):
                samples = {(v, state): [] for v in ("before", "after") for state in ("cold", "warm")}
                counts = {key: [] for key in samples}
                ages = {key: [] for key in samples}
                for repeat in range(args.repeats + 1):
                    parity = None
                    versions = ("before", "after") if repeat % 2 else ("after", "before")
                    for version in versions:
                        offset = [0.0]
                        transport = Transport(delay, offset)
                        provider = YahooQuotes()
                        cache = PriceCache(provider, clock=lambda: time.monotonic() + offset[0])
                        fetch = baseline["_fetch_current_prices"] if version == "before" else original_fetch
                        try:
                            with patch.dict(baseline, yf=SimpleNamespace(Tickers=transport.tickers)), \
                                 patch("yfinance.data.YfData", return_value=transport), \
                                 patch.object(app, "_live_prices", cache), \
                                 patch.object(app, "_fetch_current_prices", fetch), app.app.test_client() as client:
                                for state in ("cold", "warm"):
                                    offset[0] = 0.0 if state == "cold" else 5.0
                                    calls = transport.calls
                                    start = time.perf_counter()
                                    response = client.get("/data")
                                    elapsed = (time.perf_counter() - start) * 1000
                                    assert response.status_code == 200
                                    payload = response.get_json()
                                    if parity is None:
                                        parity = payload
                                    assert payload == parity, "Dashboard JSON changed"
                                    expected_calls = size if version == "before" else (1 if state == "cold" else 0)
                                    assert transport.calls - calls == expected_calls, "Unexpected quote request count"
                                    if repeat:
                                        key = (version, state)
                                        samples[key].append(elapsed)
                                        counts[key].append(transport.calls - calls)
                                        ages[key].append(max(time.time() + offset[0] - ts for ts in transport.timestamps.values()))
                        finally:
                            cache.pool.shutdown()
                            provider.pool.shutdown()
                for key, values in samples.items():
                    print(scenario, size, *key, f"{statistics.median(values):.2f}",
                          f"{np.percentile(values, 95):.2f}", f"{statistics.mean(counts[key]):.0f}",
                          f"{max(ages[key]):.2f}", flush=True)


if __name__ == "__main__":
    main()
