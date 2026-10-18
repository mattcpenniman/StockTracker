"""Isolated serving and sequential profiling using existing PostgreSQL data.

Set DATABASE_URL to a read-only PostgreSQL connection and ANALYTICS_BACKEND to
rust or python. `serve` uses one threaded Werkzeug worker without debug/reload;
`profile` measures cold (fresh service) and warm (same service repeated) requests.
No provider sync is permitted. Never run this module in a production worker.
"""
from __future__ import annotations

import argparse
import cProfile
from contextlib import nullcontext
import io
import json
from pathlib import Path
import pstats
import time
from unittest.mock import patch

import app
from .data_access import AnalyticsRepository
from .indicators import backend_diagnostics
from .service import AnalyticsService


def prohibited(*args, **kwargs):
    raise AssertionError("Provider I/O is prohibited in historical load tests")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("mode", choices=["serve", "profile"])
    parser.add_argument("--port", type=int, default=5000)
    parser.add_argument("--dates", default="2026-03-17,2026-06-16,2026-09-16")
    parser.add_argument("--repeats", type=int, default=3)
    parser.add_argument("--output", type=Path)
    parser.add_argument("--timing-only", action="store_true", help="Disable cProfile for unbiased wall-clock comparisons")
    parser.add_argument("--membership-baseline-ref", help="Use only the membership helper from this git ref (requires git)")
    args = parser.parse_args()
    if args.repeats < 1 or (args.mode == "profile" and args.output is None):
        parser.error("Use positive repeats and --output for profiling")
    membership = nullcontext()
    if args.membership_baseline_ref:
        from .membership_benchmark import baseline_membership
        membership = patch.object(app, "_portfolio_symbols", baseline_membership(args.membership_baseline_ref))
    # DDL belongs to deployment, not this read-only benchmark process.
    with membership, patch.object(app, "_init_db_if_needed"), \
         patch.object(app, "_sync_market_data", prohibited), \
         patch.object(app, "_sync_market_data_window", prohibited), \
         patch.object(app, "_backfill_analytics_symbol_if_needed", prohibited):
        if args.mode == "serve":
            from werkzeug.serving import run_simple
            run_simple("0.0.0.0", args.port, app.app, threaded=True, use_reloader=False)
            return
        records = []
        with app.app.test_client() as client:
            for endpoint in ("state", "futurestate"):
                for date in args.dates.split(","):
                    profiles = {mode: cProfile.Profile() for mode in ("cold", "warm")}
                    timings = {mode: [] for mode in profiles}
                    count = 0
                    for _ in range(args.repeats):
                        app.analytics_service = AnalyticsService(AnalyticsRepository())
                        for mode in profiles:
                            path = f"/api/portfolio/{endpoint}?timeframe=1D&asof={date}"
                            if endpoint == "futurestate":
                                path += "&days=20&stoploss=0.05&takeprofit=0.10"
                            start = time.perf_counter()
                            with nullcontext() if args.timing_only else profiles[mode]:
                                response = client.get(path)
                            timings[mode].append((time.perf_counter() - start) * 1000)
                            payload = response.get_json()
                            assert response.status_code == 200 and payload["ok"] and payload["count"] > 0, str(payload.get("error"))
                            assert not payload["errors"] and payload["count"] == payload["portfolio_symbol_count"], str(payload["errors"])
                            count = payload["count"]
                    for mode, profile in profiles.items():
                        stream = io.StringIO()
                        functions = []
                        if not args.timing_only:
                            stats = pstats.Stats(profile, stream=stream).strip_dirs().sort_stats("cumulative")
                            stats.print_stats(35)
                            functions = [{"file": key[0], "line": key[1], "function": key[2],
                                          "primitive_calls": value[0], "calls": value[1],
                                          "self_seconds": value[2], "cumulative_seconds": value[3]}
                                         for key, value in stats.stats.items()]
                        records.append({"endpoint": endpoint, "date": date, "cache": mode,
                                        "latencies_ms": timings[mode], "count": count,
                                        "profile_top": stream.getvalue(), "functions": functions})
                        print(json.dumps({k: v for k, v in records[-1].items() if k not in {"functions", "profile_top"}}), flush=True)
        args.output.write_text(json.dumps({"backend": backend_diagnostics(), "profiled": not args.timing_only,
                                          "records": records}, indent=2) + "\n")


if __name__ == "__main__":
    main()
