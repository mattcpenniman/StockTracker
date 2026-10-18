"""NYSE regular-session diagnostics, separate from input/economic validity."""
from functools import lru_cache

import numpy as np
import pandas as pd
import pandas_market_calendars as mcal

from .utils import clean_json_value
from .availability import available_at, start_bound


@lru_cache(maxsize=128)
def _schedule(start, end):
    return mcal.get_calendar("NYSE").schedule(start_date=start, end_date=end)


@lru_cache(maxsize=128)
def _year_end_date(year):
    return _schedule(f"{year}-12-01", f"{year}-12-31").index[-1].date()


def year_end_bars(timestamps):
    """Daily bars dated on the final NYSE session (UTC daily-bar labels).

    This certifies the anchor date only, not provider adjustment provenance.
    """
    stamps = pd.DatetimeIndex(timestamps)
    dates = stamps.date
    return np.asarray([date == _year_end_date(year) for date, year in zip(dates, stamps.year)], dtype=bool)


def _utc(value):
    stamp = pd.Timestamp(value)
    return stamp.tz_localize("UTC") if stamp.tzinfo is None else stamp.tz_convert("UTC")


def historical_coverage(requested_asof, actual_timestamp, timeframe="1Day", *, now=None):
    requested = _utc(requested_asof) if requested_asof is not None else None
    cutoff = requested if requested is not None else _utc(now or pd.Timestamp.now(tz="UTC").floor("min"))
    actual = _utc(actual_timestamp)
    lag = (cutoff - actual).total_seconds()
    schedule = _schedule((cutoff - pd.Timedelta(days=40)).date().isoformat(), cutoff.date().isoformat())
    completed = schedule[schedule.market_close <= cutoff]
    expected = None
    if timeframe == "1Day":
        completed = schedule[schedule.index.date <= start_bound(cutoff, timeframe).date()]
        expected = completed.index[-1].date() if len(completed) else None
        stale = actual.date() < expected if expected else None
        freshness = "missing_completed_session" if stale else "current_completed_session" if expected else "unknown"
        anchor_completed = bool(available_at(actual, timeframe) <= cutoff)
    else:
        minutes = {"1Hour": 60, "15Min": 15, "5Min": 5, "1Min": 1}.get(timeframe)
        candidates = []
        if minutes:
            for row in reversed(list(schedule.itertuples())):
                starts = pd.date_range(row.market_open, row.market_close, freq=f"{minutes}min", inclusive="left")
                candidates.extend(start for start in starts if start + pd.Timedelta(minutes=minutes) <= cutoff)
                if candidates:
                    break
        expected = max(candidates) if candidates else None
        stale = actual < expected if expected is not None else None
        freshness = "missing_completed_interval" if stale else "current_completed_interval" if expected is not None else "unknown"
        anchor_completed = None
        if minutes:
            anchor_completed = bool(available_at(actual, timeframe) <= cutoff)
    return clean_json_value({
        "requested_asof": requested, "evaluated_at": cutoff, "actual_bar_timestamp": actual,
        "lag_seconds": lag, "status": "anchor_after_cutoff" if lag < 0 else "stale_trailing" if stale else "within_tolerance" if stale is not None else "not_evaluated",
        "stale": stale, "freshness_status": freshness, "expected_latest_completed": str(expected) if expected is not None else None,
        "anchor_completed": anchor_completed,
        "calendar": "NYSE", "calendar_scope": "regular-session freshness diagnostic; eligibility uses full NY dates and full intraday intervals, including extended hours",
        "completeness_verified": False,
        "action": "Resync missing completed sessions/intervals; check provider availability or symbol trading status." if stale else None,
    })


def session_coverage(timestamps, timeframe="1Day"):
    stamps = pd.DatetimeIndex(pd.to_datetime(timestamps, utc=True))
    dates = stamps.normalize().tz_localize(None) if timeframe == "1Day" else stamps.tz_convert("America/New_York").normalize().tz_localize(None)
    schedule = _schedule(dates.min().date().isoformat(), dates.max().date().isoformat())
    missing = schedule.index.difference(dates.unique())
    unexpected = dates.unique().difference(schedule.index)
    return {
        "status": "warning" if len(missing) or len(unexpected) else "sessions_present",
        "calendar": "NYSE", "scope": "session presence within observed bounds; not listing-lifetime or intraday interval completeness",
        "missing_session_count": len(missing), "missing_sessions": [d.date().isoformat() for d in missing[:20]],
        "unexpected_session_count": len(unexpected), "unexpected_sessions": [d.date().isoformat() for d in unexpected[:20]],
        "session_presence_verified": not len(missing) and not len(unexpected),
        "completeness_verified": False,
        "action": "Backfill missing sessions; investigate halts/listing dates and out-of-calendar bars." if len(missing) or len(unexpected) else None,
    }
