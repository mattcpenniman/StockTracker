"""Shared fail-closed OHLCV validation and non-certifying diagnostics."""
from __future__ import annotations

import numpy as np
import pandas as pd

from .state_bars import StateBars


# PostgreSQL NaN sorts above infinity, so the strict upper bound excludes both.
OHLCV_SQL_CHECK = """open > 0 AND open < 'Infinity'::float8
    AND high > 0 AND high < 'Infinity'::float8
    AND low > 0 AND low < 'Infinity'::float8
    AND close > 0 AND close < 'Infinity'::float8
    AND low <= LEAST(open, close) AND high >= GREATEST(open, close)
    AND low <= high AND volume >= 0"""


class DataQualityError(ValueError):
    """Reject the entire symbol/window; never drop or fill malformed bars."""


def validate_bars(bars):
    if isinstance(bars, StateBars):
        columns = dict(zip(("open", "high", "low", "close", "volume"), bars.values))
        timestamps = bars.timestamps
    else:
        columns = bars
        if "timestamp" not in bars:
            raise DataQualityError("Invalid OHLCV: missing timestamp; resync the symbol.")
        timestamps = pd.to_datetime(bars["timestamp"], utc=True, errors="coerce")
    if not len(timestamps):
        return
    if (pd.isna(timestamps).any() or pd.Index(timestamps).has_duplicates or
            isinstance(bars, StateBars) and (timestamps == np.iinfo(np.int64).min).any()):
        raise DataQualityError("Invalid OHLCV: missing/duplicate timestamps; resync the symbol.")
    arrays = {}
    for name in ("open", "high", "low", "close", "volume"):
        if name not in columns:
            raise DataQualityError(f"Invalid OHLCV: missing {name}; resync the symbol.")
        values = np.asarray(pd.to_numeric(columns[name], errors="coerce"), dtype=float)
        invalid = ~np.isfinite(values) | (values < 0 if name == "volume" else values <= 0)
        if name == "volume":
            invalid |= values != np.floor(values)
        if invalid.any():
            index = int(np.flatnonzero(invalid)[0])
            raise DataQualityError(f"Invalid OHLCV: {name} at row {index} must be finite and {'nonnegative' if name == 'volume' else 'positive'}; resync the symbol.")
        arrays[name] = values
    invalid = ((arrays["low"] > np.minimum(arrays["open"], arrays["close"])) |
               (arrays["high"] < np.maximum(arrays["open"], arrays["close"])) |
               (arrays["low"] > arrays["high"]))
    if invalid.any():
        raise DataQualityError(f"Invalid OHLCV: impossible high/low range at row {int(np.flatnonzero(invalid)[0])}; resync the symbol.")


def corporate_action_diagnostics(bars):
    close = bars.values[3] if isinstance(bars, StateBars) else bars["close"].to_numpy()
    changes = np.asarray(close[1:], dtype=float) / np.asarray(close[:-1], dtype=float) - 1
    indices = np.flatnonzero(np.abs(changes) >= .5)
    timestamps = (pd.to_datetime(bars.timestamps, unit="us", utc=True) if isinstance(bars, StateBars)
                  else bars["timestamp"])
    return {
        "status": "warning" if len(indices) else "unknown",
        "verified": False,
        "reason": "large_moves_require_review" if len(indices) else "adjustment_provenance_not_verified",
        "review_ticket": "VEC-830",
        "adjustment_basis_ticket": "VEC-831",
        "large_move_threshold_pct": .5,
        "large_move_count": len(indices),
        "examples": [{"timestamp": timestamps[i + 1] if isinstance(bars, StateBars) else timestamps.iloc[i + 1],
                      "return_pct": float(changes[i])} for i in indices[-10:]],
        "action": "Verify corporate-action factors and adjustment basis; magnitude alone does not establish invalidity.",
    }
