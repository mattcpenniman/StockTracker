"""Calendar-independent trailing-age diagnostics, not a trading calendar audit."""
from __future__ import annotations

import pandas as pd

from .utils import clean_json_value


def historical_coverage(requested_asof, actual_timestamp):
    """Expose lag without assuming every calendar date is a trading session.

    Seven calendar days is a conservative warning threshold, not proof of
    completeness. In particular, recent anchors can still have missing bars.
    """
    requested = pd.Timestamp(requested_asof) if requested_asof is not None else None
    actual = pd.Timestamp(actual_timestamp)
    if actual.tzinfo is None:
        actual = actual.tz_localize("UTC")
    if requested is not None and requested.tzinfo is None:
        requested = requested.tz_localize("UTC")
    lag = (requested - actual).total_seconds() if requested is not None else None
    status = "not_evaluated" if lag is None else (
        "anchor_after_cutoff" if lag < 0 else "stale_trailing" if lag > 7 * 86400 else "within_tolerance"
    )
    return clean_json_value({
        "requested_asof": requested,
        "actual_bar_timestamp": actual,
        "lag_seconds": lag,
        "status": status,
        "stale": lag > 7 * 86400 if lag is not None else None,
        "max_lag_calendar_days": 7,
        "completeness_verified": False,
    })
