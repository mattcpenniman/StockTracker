"""Nonmutating, final-runtime analytics verification: python -m analytics.release_check."""
from __future__ import annotations

import argparse
from collections import Counter
import json

import numpy as np
import pandas as pd

from .config import AnalyticsSettings
from . import indicators
from .runtime import runtime_diagnostics


def verify_release(*, require_rust=False, expected_revision=None, expected_release=None,
                   allow_unversioned=False) -> dict:
    diagnostics = runtime_diagnostics()
    errors = []
    build = diagnostics["build"]
    if not allow_unversioned and any(build[key] in (None, "", "unknown") for key in ("revision", "release_id")):
        errors.append("artifact_build_identity_missing")
    if expected_revision is not None and build["revision"] != expected_revision:
        errors.append("revision_mismatch")
    if expected_release is not None and build["release_id"] != expected_release:
        errors.append("release_id_mismatch")
    if not diagnostics["ready"]:
        errors.append(diagnostics["reason"])
    # Always use the public dispatcher, the runtime configuration, and finite data.
    # No DB, providers, caches, environment mutation, or market-data writes.
    n = 320
    close = 100 + np.arange(n) * 0.1 + np.sin(np.arange(n) * 0.37)
    bars = pd.DataFrame({
        "timestamp": pd.date_range("2024-01-01", periods=n, freq="D", tz="UTC"),
        "open": close, "high": close + 2, "low": close - 2, "close": close,
        "volume": 1000 + np.arange(n) % 31,
    })
    trace = Counter()
    token = indicators._dispatch_trace.set(trace)
    finite = False
    try:
        frame = indicators.compute_feature_frame(bars, "1Day", AnalyticsSettings())
        finite = bool(np.isfinite(frame[["sma_200", "rsi_14", "atr_14", "realized_vol_20"]].iloc[-1].to_numpy(dtype=float)).all())
        if not finite:
            errors.append("nonfinite_feature_output")
    except Exception:
        # Exception messages can contain input/configuration details; expose only a code.
        errors.append("feature_calculation_failed")
    finally:
        indicators._dispatch_trace.reset(token)
    native_required = require_rust or diagnostics["configured_backend"] == "rust" or diagnostics["effective_backend"] == "rust"
    if native_required and trace["native"] != 1:
        errors.append("native_execution_not_observed")
    return {
        "ok": not errors, "runtime": diagnostics,
        "smoke": {"rows": n, "finite_output": finite, "native_calls": trace["native"],
                  "python_fallback_calls": sum(v for k, v in trace.items() if k != "native"),
                  "dispatch_reasons": dict(trace)},
        "errors": errors,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--require-rust", action="store_true")
    parser.add_argument("--expected-revision")
    parser.add_argument("--expected-release")
    parser.add_argument("--allow-unversioned", action="store_true", help="Only for local/dev images")
    args = parser.parse_args()
    result = verify_release(**vars(args))
    print(json.dumps(result, sort_keys=True))
    return 0 if result["ok"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
