"""Release evidence against real HTTP or an instrumented, isolated app process.

For an older image: docker exec -i CONTAINER python - --instrument < this_file
Instrumentation uses real PostgreSQL/provider paths and never patches serving workers.
"""
from __future__ import annotations

import argparse
from collections import Counter, defaultdict
import functools
import inspect
import json
import math
import os
import resource
import time
import urllib.error
import urllib.request


CASES = [
    ("daily_state", "/api/state/NVDA?timeframe=1D&asof=2026-09-01T00:00:00Z", None),
    ("intraday_events", "/api/events/NVDA?timeframe=15Min&asof=2026-09-01T15:45:00Z&event_limit=20", None),
    ("portfolio_state", "/api/portfolio/state?timeframe=1D&asof=2026-09-01T00:00:00Z", None),
    ("batch_state", "/api/state/batch", {"symbols": ["NVDA", "AAPL", "MSFT"], "timeframe": "1D", "asof": "2026-09-02T00:00:00Z"}),
    ("daily_events", "/api/events/NVDA?timeframe=1D&asof=2026-09-01T00:00:00Z&event_limit=20", None),
    ("daily_labels", "/api/futurestate/NVDA?timeframe=1D&asof=2026-09-01&days=20", None),
    ("current_portfolio", "/api/portfolio/state?timeframe=1D", None),
]


def assert_payload_parity(left, right, path="payload"):
    """Compare full JSON, tolerating only floating-point kernel rounding."""
    if isinstance(left, dict) and isinstance(right, dict):
        assert left.keys() == right.keys(), path + ": keys differ"
        for key in left:
            assert_payload_parity(left[key], right[key], path + "." + key)
    elif isinstance(left, list) and isinstance(right, list):
        assert len(left) == len(right), path + ": lengths differ"
        for i, (a, b) in enumerate(zip(left, right)):
            assert_payload_parity(a, b, f"{path}[{i}]")
    elif (isinstance(left, (int, float)) and isinstance(right, (int, float))
          and not isinstance(left, bool) and not isinstance(right, bool)):
        assert math.isclose(left, right, rel_tol=1e-7, abs_tol=1e-7), path + ": numeric mismatch"
    else:
        assert left == right, path + ": value mismatch"


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--base-url", default="http://192.168.1.80:5005")
    parser.add_argument("--instrument", action="store_true")
    parser.add_argument("--repeats", type=int, default=5)
    parser.add_argument("--case", action="append", choices=[case[0] for case in CASES])
    parser.add_argument("--cold-each", action="store_true", help="Fresh service caches for each isolated request")
    parser.add_argument("--require-success", action="store_true")
    parser.add_argument("--parity", action="store_true", help="Compare full Python/Rust JSON in an isolated process")
    args = parser.parse_args()
    if args.repeats < 1 or ((args.cold_each or args.parity) and not args.instrument):
        parser.error("Use positive repeats; --cold-each and --parity require --instrument")
    if args.parity and args.case and "current_portfolio" in args.case:
        parser.error("Parity requires fixed historical inputs; current_portfolio is excluded")
    counts: Counter[str] = Counter()
    phases: dict[str, float] = defaultdict(float)
    client = None
    app = service = indicators = None
    if args.instrument:
        import psycopg2
        from psycopg2.extensions import cursor

        class CountingCursor(cursor):
            def execute(self, *a, **kw):
                counts["sql"] += 1
                start = time.perf_counter()
                try:
                    return super().execute(*a, **kw)
                finally:
                    phases["sql_ms"] += (time.perf_counter() - start) * 1000

            def fetchall(self):
                start = time.perf_counter()
                rows = super().fetchall()
                phases["fetch_rows_ms"] += (time.perf_counter() - start) * 1000
                counts["rows"] += len(rows)
                return rows

            def fetchone(self):
                start = time.perf_counter()
                row = super().fetchone()
                phases["fetch_rows_ms"] += (time.perf_counter() - start) * 1000
                counts["rows"] += int(row is not None)
                return row

        connect = psycopg2.connect

        def counted_connect(*a, **kw):
            counts["connections"] += 1
            kw["cursor_factory"] = CountingCursor
            return connect(*a, **kw)

        psycopg2.connect = counted_connect
        import app
        import analytics.service as service
        from analytics import indicators

        def wrap(module, name, metric, phase):
            original = getattr(module, name, None)
            if original is None:
                return

            @functools.wraps(original)
            def measured(*a, **kw):
                counts[metric] += 1
                start = time.perf_counter()
                try:
                    return original(*a, **kw)
                finally:
                    phases[phase] += (time.perf_counter() - start) * 1000
            descriptor = inspect.getattr_static(module, name)
            setattr(module, name, staticmethod(measured) if isinstance(descriptor, staticmethod) else measured)

        for name in ("_fetch_alpaca_bars", "_fetch_alpaca_latest_quote", "_fetch_alpaca_latest_bar"):
            wrap(app, name, "provider_calls", "provider_ms")
        for name in ("compute_feature_frame", "compute_signal_columns", "generate_events"):
            wrap(service, name, name + "_calls", name + "_ms")
        wrap(indicators, "_rust_feature_columns", "native_kernel_calls", "native_kernel_ms")
        for name in ("fetch_bars", "fetch_bars_many", "fetch_latest_bar", "fetch_latest_bars_many"):
            wrap(service.AnalyticsRepository, name, name + "_calls", name + "_ms")
        wrap(service.AnalyticsRepository, "_bar_frame", "frame_conversion_calls", "frame_conversion_ms")
        wrap(service.AnalyticsService, "_add_portfolio_ranks", "ranking_calls", "ranking_ms")
        wrap(app.app.json, "dumps", "json_dumps_calls", "json_dumps_ms")
        # Production schema initialization is measured separately, not hidden in the
        # first endpoint. It is the supported additive coverage-schema path.
        start = time.perf_counter()
        app._init_db_if_needed()
        print(json.dumps({"schema_init_ms": (time.perf_counter() - start) * 1000,
                          "sql": counts["sql"]}), flush=True)
        client = app.app.test_client()

    if args.parity:
        assert app is not None and service is not None and client is not None and indicators is not None
        if getattr(indicators, "_rust_feature_columns", None) is None:
            raise RuntimeError("Parity requires the native extension and dispatch implementation")
        original_backend = os.environ.get("ANALYTICS_BACKEND")
        try:
            for name, path, body in CASES:
                if args.case and name not in args.case:
                    continue
                if name == "current_portfolio":
                    # Moving current/provider snapshots are not a fixed parity input.
                    continue
                payloads = []
                for backend in ("python", "rust"):
                    counts.clear()
                    os.environ["ANALYTICS_BACKEND"] = backend
                    app.analytics_service = service.AnalyticsService()
                    response = client.post(path, json=body) if body else client.get(path)
                    payload = response.get_json()
                    assert response.status_code == 200 and payload["ok"] and not payload.get("errors"), name
                    if backend == "rust" and name != "daily_labels":
                        assert counts["native_kernel_calls"] > 0, name + ": native execution missing"
                    payloads.append(payload)
                assert_payload_parity(*payloads)
                print(json.dumps({"case": name, "full_json_parity": True,
                                  "relative_tolerance": 1e-7, "absolute_tolerance": 1e-7}), flush=True)
        finally:
            if original_backend is None:
                os.environ.pop("ANALYTICS_BACKEND", None)
            else:
                os.environ["ANALYTICS_BACKEND"] = original_backend
        return 0

    failed = False
    for name, path, body in CASES:
        if args.case and name not in args.case:
            continue
        for repeat in range(args.repeats):
            if args.cold_each:
                assert app is not None and service is not None
                app.analytics_service = service.AnalyticsService()
            counts.clear()
            phases.clear()
            counts["provider_calls"] = 0
            start = time.perf_counter()
            if args.instrument:
                assert client is not None
                response = client.post(path, json=body) if body else client.get(path)
                status, payload = response.status_code, response.get_json()
            else:
                request = urllib.request.Request(args.base_url + path,
                    data=json.dumps(body).encode() if body else None,
                    headers={"Content-Type": "application/json"} if body else {})
                try:
                    response = urllib.request.urlopen(request, timeout=240)
                except urllib.error.HTTPError as exc:
                    response = exc
                with response:
                    status, payload = response.code, json.load(response)
            elapsed = (time.perf_counter() - start) * 1000
            summary = {key: payload[key] for key in (
                "ok", "error", "count", "portfolio_symbol_count", "errors", "window", "anchor_price",
            ) if key in payload}
            summary["event_count"] = len(payload.get("events", []))
            failed |= status != 200 or not payload.get("ok") or bool(payload.get("errors"))
            record = {"case": name, "repeat": repeat, "ms": round(elapsed, 3),
                      "status": status, "summary": summary}
            if args.instrument:
                record.update(counts=dict(counts), phases_ms={k: round(v, 3) for k, v in phases.items()},
                              process_peak_rss_kib=resource.getrusage(resource.RUSAGE_SELF).ru_maxrss)
                from analytics import indicators
                if hasattr(indicators, "backend_diagnostics"):
                    record["execution"] = indicators.backend_diagnostics()["execution"]
            print(json.dumps(record, sort_keys=True), flush=True)
    return int(args.require_success and failed)


if __name__ == "__main__":
    raise SystemExit(main())
