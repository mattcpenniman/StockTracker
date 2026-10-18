"""Alternating warm membership/route parity on stored, read-only PostgreSQL data.

DATABASE_URL=<read-only DSN> python -m analytics.membership_benchmark \
    --repeats 30 --output /tmp/membership.json
Requires git history for the baseline helper. No provider calls or schema DDL.
"""
from __future__ import annotations

import argparse
import ast
from contextlib import contextmanager
import json
from pathlib import Path
import subprocess
import time
import tracemalloc
from typing import Callable, Any
from unittest.mock import patch

from psycopg2.extensions import cursor

import app
from database import get_connection
from .portfolio_load import percentile
from .portfolio_load_local import prohibited


BASELINE = "9d391c3"


def baseline_membership(ref=BASELINE):
    source = subprocess.check_output(["git", "show", f"{ref}:app.py"], text=True)
    node = next(n for n in ast.parse(source).body
                if isinstance(n, ast.FunctionDef) and n.name == "_portfolio_symbols")
    namespace = {}
    exec(compile(ast.Module(body=[node], type_ignores=[]), "membership-baseline", "exec"),
         app.__dict__, namespace)
    return namespace["_portfolio_symbols"]


def rss_kib():
    return int(next(line.split()[1] for line in Path("/proc/self/status").read_text().splitlines()
                    if line.startswith("VmRSS:")))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--baseline-ref", default=BASELINE)
    parser.add_argument("--repeats", type=int, default=30)
    parser.add_argument("--dates", default="2026-03-17,2026-06-16,2026-09-16")
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    if args.repeats < 1:
        parser.error("Use positive repeats")
    before, after = baseline_membership(args.baseline_ref), app._portfolio_symbols
    queries = []

    class CountingCursor(cursor):
        def execute(self, sql, *args, **kwargs):
            queries.append(str(sql))
            return super().execute(sql, *args, **kwargs)

    @contextmanager
    def connection():
        with get_connection(app.DATABASE_URL) as conn:
            previous = conn.cursor_factory
            conn.cursor_factory = CountingCursor
            try:
                yield conn
            finally:
                conn.cursor_factory = previous

    records = []
    with patch.object(app, "_init_db_if_needed"), \
         patch.object(app, "_get_conn", connection), \
         patch.object(app.analytics_service.repository, "get_conn", connection), \
         patch.object(app, "_sync_market_data", prohibited), \
         patch.object(app, "_sync_market_data_window", prohibited), \
         patch.object(app, "_backfill_analytics_symbol_if_needed", prohibited), \
         app.app.test_client() as client:
        cases: list[tuple[str, Callable[[], Any]]] = [
            (f"helper/include_hidden={hidden}", lambda h=hidden: app._portfolio_symbols(h))
            for hidden in (False, True)]
        for endpoint in ("state", "futurestate"):
            for date in args.dates.split(","):
                path = f"/api/portfolio/{endpoint}?timeframe=1D&asof={date}"
                if endpoint == "futurestate":
                    path += "&days=20&stoploss=0.05&takeprofit=0.10"

                def request(path=path):
                    response = client.get(path)
                    payload = response.get_json()
                    assert response.status_code == 200 and payload["ok"], payload
                    assert payload["count"] > 0 and payload["count"] == payload["portfolio_symbol_count"], payload
                    assert not payload["errors"], payload["errors"]
                    return payload

                cases.append((path, request))
        for name, operation in cases:
            samples = {label: {"ms": [], "queries": [], "peak_traced_bytes": []}
                       for label in ("before", "after")}
            # Warm both variants before timings (same service/cache for both).
            for helper in (before, after):
                with patch.object(app, "_portfolio_symbols", helper):
                    operation()
            rss_start = rss_kib()
            for iteration in range(args.repeats):
                payloads = {}
                for label in (("before", "after") if iteration % 2 else ("after", "before")):
                    with patch.object(app, "_portfolio_symbols", before if label == "before" else after):
                        queries.clear()
                        start = time.perf_counter()
                        payloads[label] = operation()
                        samples[label]["ms"].append((time.perf_counter() - start) * 1000)
                        samples[label]["queries"].append(len(queries))
                        samples[label]["sql"] = list(dict.fromkeys(queries))
                assert payloads["before"] == payloads["after"], name
            # Allocation probes are separate from uninstrumented latency samples.
            for _ in range(5):
                for label, helper in (("before", before), ("after", after)):
                    with patch.object(app, "_portfolio_symbols", helper):
                        tracemalloc.start()
                        operation()
                        samples[label]["peak_traced_bytes"].append(tracemalloc.get_traced_memory()[1])
                        tracemalloc.stop()
            record = {"case": name, "parity": "exact full JSON/list", "pairs": args.repeats,
                      "rss_start_kib": rss_start, "rss_end_kib": rss_kib(),
                      "samples": samples,
                      "summary": {label: {"p50_ms": percentile(values["ms"], .5),
                                          "p95_ms": percentile(values["ms"], .95),
                                          "queries": sorted(set(values["queries"])),
                                          "peak_traced_bytes_p50": percentile(values["peak_traced_bytes"], .5)}
                                  for label, values in samples.items()}}
            records.append(record)
            print(json.dumps({k: v for k, v in record.items() if k != "samples"}), flush=True)
    args.output.write_text(json.dumps({"baseline": args.baseline_ref, "records": records}, indent=2) + "\n")


if __name__ == "__main__":
    main()
