"""VEC-814 alternating warm routes, allocations, profiles and isolated serving.

Uses the actual service/settings sources at --baseline-ref, keeping current
membership, repository, cache and routes. Requires a read-only DATABASE_URL.
"""
import argparse
import cProfile
from contextlib import ExitStack, contextmanager
import json
from pathlib import Path
import pstats
import subprocess
import sys
import time
import tracemalloc
from types import ModuleType
from unittest.mock import patch

import app
from .config import AnalyticsSettings
from .data_access import AnalyticsRepository
from .membership_benchmark import rss_kib
from .portfolio_load import percentile, request as http_request, run_stage
from .portfolio_load_local import prohibited
from .service import AnalyticsService


BASELINE = "5a88dae"


def baseline_types(ref=BASELINE):
    def load(path):
        source = subprocess.check_output(["git", "show", f"{ref}:{path}"], text=True)
        name = "analytics._warm_baseline_" + path.split("/")[-1].removesuffix(".py")
        module = ModuleType(name)
        sys.modules[name] = module
        namespace = module.__dict__
        namespace["__package__"] = "analytics"
        exec(compile(source, f"{ref}:{path}", "exec"), namespace)
        return namespace
    return load("analytics/service.py")["AnalyticsService"], load("analytics/config.py")["AnalyticsSettings"]


@contextmanager
def isolated():
    with ExitStack() as stack:
        stack.enter_context(patch.object(app, "_init_db_if_needed"))
        for name in ("_sync_market_data", "_sync_market_data_window", "_backfill_analytics_symbol_if_needed"):
            stack.enter_context(patch.object(app, name, prohibited))
        yield


def http_load(before_url, after_url, output):
    """DOC-228 hot stages, fully drained, with full-count and RSS checks."""
    from .cache_benchmark import control

    records = []
    for workload, endpoints, rates in (
        ("mixed", ("state", "futurestate"), (5, 25, 50, 100)),
        ("state", ("state",), (25, 50)),
        ("futurestate", ("futurestate",), (50, 100)),
    ):
        for variant, base in (("before", before_url), ("after", after_url)):
            cases = [(endpoint, date, 20) for date in ("2026-03-17", "2026-06-16", "2026-09-16") for endpoint in endpoints]
            assert all(http_request(base, *case, 120)["valid"] for case in cases)
            for rate in rates:
                before = control(base)
                stage = run_stage(base, cases, rate, 15, 32, 120)
                stage.update(variant=variant, workload=workload, memory_before=before, memory_after=control(base))
                records.append(stage)
                output.write_text(json.dumps(records, indent=2) + "\n")
                assert all(r["valid"] and r["count"] == 37 for r in stage["records"]), "Invalid/full-count HTTP response"
                print(json.dumps(dict(variant=variant, workload=workload, rate=rate,
                    rps=stage["completed_in_window_rps"], summary=stage["summary"])), flush=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--baseline-ref", default=BASELINE)
    parser.add_argument("--repeats", type=int, default=30)
    parser.add_argument("--output", type=Path)
    parser.add_argument("--serve", type=int)
    parser.add_argument("--variant", choices=("before", "after"), default="after")
    parser.add_argument("--load", action="store_true", help="Run DOC-228 hot HTTP loads against two isolated servers")
    parser.add_argument("--before-url", default="http://127.0.0.1:5014")
    parser.add_argument("--after-url", default="http://127.0.0.1:5015")
    args = parser.parse_args()
    if args.repeats < 1 or (not args.serve and not args.output):
        parser.error("Positive repeats and --output required for measurements")
    if args.load:
        http_load(args.before_url.rstrip("/"), args.after_url.rstrip("/"), args.output)
        return
    before, before_settings = baseline_types(args.baseline_ref)
    types = {"before": (before, before_settings), "after": (AnalyticsService, AnalyticsSettings)}
    repo = AnalyticsRepository()
    with repo.get_conn() as conn, conn.cursor() as cur:
        cur.execute("SHOW default_transaction_read_only")
        assert cur.fetchone()[0] == "on", "Use a read-only DATABASE_URL"
    with isolated():
        if args.serve:
            # Reuse the benchmark-only diagnostics route for current/peak RSS.
            from .cache_benchmark import serve
            service_type, settings_type = types[args.variant]
            app.analytics_service = service_type(repo)
            with patch.object(app, "AnalyticsSettings", settings_type):
                serve(args.serve)
            return
        services = {name: cls(repo) for name, (cls, _) in types.items()}
        records = []
        with app.app.test_client() as client:
            for endpoint in ("state", "futurestate"):
                for date in ("2026-03-17", "2026-06-16", "2026-09-16"):
                    path = f"/api/portfolio/{endpoint}?asof={date}&days=20&stoploss=.05&takeprofit=.10"

                    def request(name):
                        with patch.object(app, "analytics_service", services[name]), \
                             patch.object(app, "AnalyticsSettings", types[name][1]):
                            response = client.get(path)
                            payload = response.get_json()
                        assert response.status_code == 200 and payload["ok"], payload
                        assert payload["count"] == payload["portfolio_symbol_count"] == 37 and not payload["errors"], payload
                        return payload

                    assert request("before") == request("after"), path
                    samples = {name: {"ms": [], "peak_traced_bytes": []} for name in types}
                    rss_start = rss_kib()
                    for iteration in range(args.repeats):
                        payloads = {}
                        for name in (("before", "after") if iteration % 2 else ("after", "before")):
                            start = time.perf_counter()
                            payloads[name] = request(name)
                            samples[name]["ms"].append((time.perf_counter() - start) * 1000)
                        assert payloads["before"] == payloads["after"], path
                    for iteration in range(6):
                        for name in (("before", "after") if iteration % 2 else ("after", "before")):
                            tracemalloc.start()
                            request(name)
                            samples[name]["peak_traced_bytes"].append(tracemalloc.get_traced_memory()[1])
                            tracemalloc.stop()
                    for name in types:
                        profile = cProfile.Profile()
                        with profile:
                            for _ in range(5):
                                request(name)
                        samples[name]["functions"] = [dict(file=k[0], line=k[1], function=k[2],
                            calls=v[1], self_seconds=v[2], cumulative_seconds=v[3])
                            for k, v in pstats.Stats(profile).stats.items()]
                    summary = {name: dict(p50_ms=percentile(s["ms"], .5), p95_ms=percentile(s["ms"], .95),
                        peak_traced_bytes_p50=percentile(s["peak_traced_bytes"], .5)) for name, s in samples.items()}
                    record = dict(endpoint=endpoint, date=date, pairs=args.repeats, summary=summary,
                                  rss_start_kib=rss_start, rss_end_kib=rss_kib(), samples=samples)
                    records.append(record)
                    print(json.dumps({k: v for k, v in record.items() if k != "samples"}), flush=True)
        args.output.write_text(json.dumps(dict(baseline=args.baseline_ref, records=records), indent=2) + "\n")


if __name__ == "__main__":
    main()
