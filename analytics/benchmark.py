"""Reproducible, uncached calculation benchmark: python -m analytics.benchmark."""
from __future__ import annotations

import argparse
import os
import statistics
import time
from unittest.mock import Mock, patch

import numpy as np
import pandas as pd

from .config import AnalyticsSettings
from .indicators import compute_feature_frame, _rust_feature_columns
from .signals import compute_signal_columns


def _assert_payload_equal(actual, expected) -> None:
    if isinstance(expected, dict):
        assert actual.keys() == expected.keys()
        for key in expected:
            _assert_payload_equal(actual[key], expected[key])
    elif isinstance(expected, float):
        np.testing.assert_allclose(actual, expected, rtol=1e-8, atol=1e-10)
    else:
        assert actual == expected, (actual, expected)


def benchmark_endpoints(bars: pd.DataFrame, repeats: int) -> None:
    """Exercise real routes/services with fixture data and cold service caches."""
    from app import app
    from .service import AnalyticsService

    urls = [
        "/api/state/NVDA",
        "/api/state/NVDA?timeframe=1D&asof=2023-09-20",
        "/api/state/NVDA?timeframe=15Min&asof=2023-09-20T15:45:00Z",
        "/api/state/NVDA?timeframe=1D&asof=2023-09-20&hidets=true",
        "/api/state/NVDA/1Day?breakout_lookback=55&buffer_pct=0.005",
    ]
    bars = bars.copy()
    bars["timestamp"] = pd.date_range(end="2023-09-20", periods=len(bars), freq="D", tz="UTC")
    intraday = bars.iloc[-1:].copy()
    intraday["timestamp"] = pd.Timestamp("2023-09-20T15:45:00Z")
    repo = Mock()
    repo.fetch_symbol_context.return_value = {"symbol_id": 1, "symbol": "NVDA", "sync_status": "idle"}
    repo.fetch_bars.side_effect = lambda symbol_id, timeframe, asof: bars if timeframe == "1Day" else intraday
    print(f"Cold Flask test-client requests ({len(bars)} fixture bars; database/network/sync mocked)")
    with app.test_client() as client, patch("app._sync_current_analytics_data_if_needed"), patch("app._sync_market_data_window"):
        for url in urls:
            timings, payloads = {}, {}
            for backend in ("python", "rust"):
                with patch.dict(os.environ, ANALYTICS_BACKEND=backend):
                    samples = []
                    for iteration in range(repeats + 1):
                        # Each request gets a fresh cache, including the warmup.
                        with patch("app.analytics_service", AnalyticsService(repository=repo)):
                            start = time.perf_counter()
                            response = client.get(url)
                            elapsed = time.perf_counter() - start
                        assert response.status_code == 200, response.get_json()
                        payloads[backend] = response.get_json()
                        if iteration:
                            samples.append(elapsed)
                    timings[backend] = statistics.median(samples) * 1000
            _assert_payload_equal(payloads["rust"], payloads["python"])
            print(f"{url}\n  pandas {timings['python']:.2f} ms; Rust {timings['rust']:.2f} ms; {timings['python'] / timings['rust']:.2f}x faster")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--bars", type=int, nargs="+", default=[260, 1000, 10000])
    parser.add_argument("--repeats", type=int, default=20)
    parser.add_argument("--http", action="store_true", help="Also compare all five state URL examples with a fixture repository")
    args = parser.parse_args()
    if _rust_feature_columns is None:
        parser.error("Install the release extension first: pip install ./rust")
    if args.repeats < 1 or any(count < 1 for count in args.bars):
        parser.error("Bar counts and repeats must be positive")
    rng = np.random.default_rng(777)
    settings = AnalyticsSettings(breakout_lookback=55, buffer_pct=0.005)
    original = os.environ.get("ANALYTICS_BACKEND")
    try:
        print("Uncached feature + signal calculation (median; excludes database, sync, HTTP)")
        for count in args.bars:
            close = 100 * np.exp(np.cumsum(rng.normal(0, 0.01, count)))
            bars = pd.DataFrame({
                "timestamp": pd.date_range("2000-01-01", periods=count, freq="D", tz="UTC"),
                "open": close - 0.5, "high": close + 1, "low": close - 1,
                "close": close, "volume": rng.integers(0, 10_000_000, count),
            })
            timings = {}
            results = {}
            for backend in ("python", "rust"):
                os.environ["ANALYTICS_BACKEND"] = backend
                compute_signal_columns(compute_feature_frame(bars, "1Day", settings), settings)
                samples = []
                for _ in range(args.repeats):
                    start = time.perf_counter()
                    results[backend] = compute_signal_columns(compute_feature_frame(bars, "1Day", settings), settings)
                    samples.append(time.perf_counter() - start)
                timings[backend] = statistics.median(samples) * 1000
            pd.testing.assert_frame_equal(results["rust"], results["python"], check_like=True, rtol=1e-8, atol=1e-10)
            print(f"{count:>6} bars: pandas {timings['python']:.2f} ms; Rust {timings['rust']:.2f} ms; {timings['python'] / timings['rust']:.2f}x faster")
        if args.http:
            benchmark_endpoints(bars, args.repeats)
    finally:
        if original is None:
            os.environ.pop("ANALYTICS_BACKEND", None)
        else:
            os.environ["ANALYTICS_BACKEND"] = original


if __name__ == "__main__":
    main()
