"""VEC-786 event assembly parity, profiling, memory, and PostgreSQL endpoint timing.

python -m analytics.events_benchmark --repeats 10
ANALYTICS_TEST_DATABASE_URL=... python -m analytics.events_benchmark --endpoint-only
Baseline source is loaded from the pinned pre-change commit, never the worktree.
"""
from __future__ import annotations

import argparse
from contextlib import contextmanager
import cProfile
import gc
import json
import os
import pstats
import statistics
import subprocess
import time
import tracemalloc
from typing import Any
from unittest.mock import patch

import numpy as np
import pandas as pd

from .config import AnalyticsSettings
from .indicators import compute_feature_frame
from .serializers import serialize_events
from .signals import generate_events


BASELINE = "b178518"


def baseline_generate_events():
    source = subprocess.check_output(["git", "show", f"{BASELINE}:analytics/signals.py"], text=True)
    namespace: dict[str, Any] = {"__package__": "analytics"}
    exec(compile(source, "events-baseline", "exec"), namespace)
    return namespace["generate_events"]


def event_fixture(count, density):
    """Controlled feature inputs: sparse pulses versus many simultaneous events."""
    close = np.full(count, 100.0)
    if density == "dense":
        close += np.where(np.arange(count) % 2, 10.0, -10.0)
    elif density == "sparse":
        close[249::250] += np.where(np.arange(len(close[249::250])) % 2, 10.0, -10.0)
    else:
        raise ValueError(density)
    bars = pd.DataFrame({
        "timestamp": pd.date_range("2020-01-01", periods=count, freq="min", tz="UTC"),
        "open": close, "high": close + 1, "low": close - 1,
        "close": close, "volume": np.full(count, 1000.0),
    })
    features = compute_feature_frame(bars, "1Min", AnalyticsSettings())
    # Deliberately independent of indicator/backend rounding and warmup. Tests
    # also exercise real feature frames at every warmup boundary.
    for column in ("sma_20", "sma_50", "sma_200"):
        features[column] = 100.0
    features["breakout_level"] = features["prior_high_20"] = features["prior_high_55"] = 101.0
    features["breakdown_level"] = features["prior_low_20"] = features["prior_low_55"] = 99.0
    features["macd"] = close - 100
    features["macd_signal"] = 0.0
    features["rsi_14"] = np.where(close > 100, 80.0, np.where(close < 100, 20.0, 50.0))
    features["volume_ratio_20"] = np.where(close != 100, 3.0, 1.0)
    features["volume_zscore"] = np.where(close != 100, 3.0, 0.0)
    features["volatility_ratio"] = np.where(close != 100, 3.0, 1.0)
    return features


def summary(samples):
    return {"median_ms": round(statistics.median(samples), 3),
            "p95_ms": round(float(np.percentile(samples, 95)), 3)}


def assembly_benchmark(repeats, sizes, limits):
    before = baseline_generate_events()
    settings = AnalyticsSettings(volume_confirmation=False, buffer_pct=0.0)
    for count in sizes:
        for density in ("sparse", "dense"):
            features = event_fixture(count, density)
            for limit in limits:
                samples = {name: [] for name in ("before", "after")}
                implementations = [("before", before), ("after", generate_events)]
                for iteration in range(repeats + 1):
                    payloads = {}
                    for label, implementation in implementations[::1 if iteration % 2 else -1]:
                        start = time.perf_counter()
                        events = implementation(features, settings, limit)
                        elapsed = (time.perf_counter() - start) * 1000
                        if iteration:
                            samples[label].append(elapsed)
                        if iteration == 0:
                            payloads[label] = serialize_events("BENCH", "1Min", events)
                        del events
                    if iteration == 0:
                        assert json.dumps(payloads["before"], sort_keys=True) == json.dumps(payloads["after"], sort_keys=True), (count, density, limit)
                        del payloads
                for label, implementation in implementations:
                    gc.collect()
                    tracemalloc.start()
                    events = implementation(features, settings, limit)
                    _, peak = tracemalloc.get_traced_memory()
                    tracemalloc.stop()
                    print(json.dumps({"bars": count, "density": density, "limit": limit,
                                      "variant": label, **summary(samples[label]),
                                      "peak_mib": round(peak / 1024**2, 3), "events": len(events)}), flush=True)
                    del events
    # Profile assembly separately from signal-column copying, feature calculation,
    # serialization, DB, and sync. Keep the real mask construction in the profile.
    features = event_fixture(10000, "sparse")
    from .signals import compute_signal_columns
    working = compute_signal_columns(features, settings)
    for label, implementation in (("before", before), ("after", generate_events)):
        print(f"ASSEMBLY PROFILE {label}: 10k sparse, limit=100; precomputed signal columns", flush=True)
        with patch.dict(implementation.__globals__, compute_signal_columns=lambda *_: working):
            profiler = cProfile.Profile()
            profiler.runcall(implementation, features, settings, 100)
            pstats.Stats(profiler).strip_dirs().sort_stats("cumulative").print_stats(15)


@contextmanager
def timed_stage(timings, name):
    start = time.perf_counter()
    try:
        yield
    finally:
        timings[name] = timings.get(name, 0) + (time.perf_counter() - start) * 1000


def endpoint_benchmark(repeats, sizes):
    import app as dashboard
    from database import close_pools
    from . import service as service_module
    from .data_access import AnalyticsRepository
    from .io_benchmark import price_fixture

    before = baseline_generate_events()
    for count in sizes:
        with price_fixture(os.environ["ANALYTICS_TEST_DATABASE_URL"], count, 0) as (dsn, _), \
             dashboard.app.test_client() as client:
            timings = {}

            class TimedRepository(AnalyticsRepository):
                def fetch_symbol_contexts(self, *args, **kwargs):
                    with timed_stage(timings, "db_metadata"):
                        return super().fetch_symbol_contexts(*args, **kwargs)

                def fetch_bars(self, *args, **kwargs):
                    with timed_stage(timings, "db_bars"):
                        return super().fetch_bars(*args, **kwargs)

            def timed_function(function, name):
                def wrapped(*args, **kwargs):
                    with timed_stage(timings, name):
                        return function(*args, **kwargs)
                return wrapped

            # Historical route bypasses sync by design. Count exact full-history
            # reads and separately time metadata/bars (including pool + conversion),
            # features (both events and the route's state/as_of call), and assembly.
            asof = (pd.Timestamp("2000-01-01", tz="UTC") + pd.Timedelta(days=count - 1)).isoformat()
            url = f"/api/events/BENCH?asof={asof.replace('+00:00', 'Z')}&event_limit=100"
            samples = {(label, mode): [] for label in ("before", "after") for mode in ("cold", "warm")}
            for iteration in range(repeats + 1):
                payloads = {}
                for label, implementation in [("before", before), ("after", generate_events)][::1 if iteration % 2 else -1]:
                    service = service_module.AnalyticsService(TimedRepository(dsn))
                    with patch.object(dashboard, "analytics_service", service), \
                         patch.object(dashboard, "_sync_current_analytics_data_if_needed", side_effect=AssertionError("Unexpected sync")), \
                         patch.object(dashboard, "_backfill_analytics_symbol_if_needed", side_effect=AssertionError("Unexpected backfill")), \
                         patch.object(service_module, "generate_events", timed_function(implementation, "events")), \
                         patch.object(service_module, "compute_feature_frame", timed_function(compute_feature_frame, "features")):
                        for mode in ("cold", "warm"):
                            timings.clear()
                            start = time.perf_counter()
                            response = client.get(url)
                            elapsed = (time.perf_counter() - start) * 1000
                            assert response.status_code == 200, response.get_json()
                            payloads[label, mode] = response.get_json()
                            if iteration:
                                samples[label, mode].append({"total": elapsed, **timings})
                assert all(value == payloads["before", "cold"] for value in payloads.values())
            for (label, mode), rows in samples.items():
                stages = {name: summary([row.get(name, 0) for row in rows])
                          for name in ("total", "db_metadata", "db_bars", "features", "events")}
                print(json.dumps({"endpoint_bars": count, "variant": label, "cache": mode,
                                  "sync": "historical request: bypassed, provider prohibited", "stages": stages}), flush=True)
            close_pools()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repeats", type=int, default=10)
    parser.add_argument("--sizes", type=int, nargs="+", default=[1000, 10000, 100000])
    parser.add_argument("--endpoint-only", action="store_true")
    parser.add_argument("--all-events", action="store_true", help="Also time unlimited history materialization")
    args = parser.parse_args()
    if min(args.repeats, *args.sizes) < 1:
        parser.error("Use positive counts")
    print(f"baseline={BASELINE}; repeats={args.repeats}; first alternating pair excluded; peak=tracemalloc separate run", flush=True)
    if args.endpoint_only:
        if not os.environ.get("ANALYTICS_TEST_DATABASE_URL"):
            parser.error("Set ANALYTICS_TEST_DATABASE_URL")
        endpoint_benchmark(args.repeats, args.sizes)
    else:
        assembly_benchmark(args.repeats, args.sizes, [100, None] if args.all_events else [100])


if __name__ == "__main__":
    main()
