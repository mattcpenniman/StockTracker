"""Conservative event-time eligibility for stored, start-labelled Alpaca bars.

Daily OHLCV is not certified regular-session-only. Treat it as a complete New
York calendar-day aggregate, including extended hours, rather than releasing
its final values at the regular close. This is not provider receipt-time or
revision history: the database does not retain either.
"""
import pandas as pd


MINUTES = {"1Hour": 60, "15Min": 15, "5Min": 5, "1Min": 1}


def utc(value):
    stamp = pd.Timestamp(value)
    return stamp.tz_localize("UTC") if stamp.tzinfo is None else stamp.tz_convert("UTC")


def available_at(timestamp, timeframe):
    stamp = utc(timestamp)
    if timeframe == "1Day":
        # Both legacy UTC-midnight and provider NY-midnight daily labels name
        # the UTC date of the trading session. Do not interpret 00Z as prior NY day.
        next_date = stamp.tz_localize(None).normalize() + pd.Timedelta(days=1)
        return next_date.tz_localize("America/New_York").tz_convert("UTC")
    return stamp + pd.Timedelta(minutes=MINUTES[timeframe])


def start_bound(cutoff, timeframe):
    """Inclusive, indexable upper bound on eligible bar starts (microseconds)."""
    cutoff = utc(cutoff if cutoff is not None else pd.Timestamp.now(tz="UTC").floor("min"))
    if timeframe == "1Day":
        date = cutoff.tz_convert("America/New_York").date()
        return pd.Timestamp(date, tz="UTC") - pd.Timedelta(microseconds=1)
    return cutoff - pd.Timedelta(minutes=MINUTES[timeframe])


def policy():
    return {
        "policy": "completed_stored_bars",
        "daily": "next_America/New_York_midnight",
        "intraday": "bar_start_plus_full_interval",
        "extended_hours": "included; daily aggregates conservatively held through entire NY date",
        "partial_bars": False,
        "provider_receipt_time_verified": False,
        "revision_history_verified": False,
    }
