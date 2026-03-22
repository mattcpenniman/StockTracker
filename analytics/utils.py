from __future__ import annotations

import math
from datetime import date, datetime, time, timezone
from typing import Any

import pandas as pd


def parse_asof(value: str | None) -> datetime | None:
    if not value:
        return None

    raw = str(value).strip()
    if not raw:
        return None

    try:
        if len(raw) == 10:
            return datetime.combine(date.fromisoformat(raw), time.max, tzinfo=timezone.utc)
        return datetime.fromisoformat(raw.replace("Z", "+00:00")).astimezone(timezone.utc)
    except ValueError as exc:
        raise ValueError("Invalid 'asof'; expected YYYY-MM-DD or ISO 8601 timestamp.") from exc


def isoformat_utc(value: datetime | pd.Timestamp | None) -> str | None:
    if value is None:
        return None
    if isinstance(value, pd.Timestamp):
        if value.tzinfo is None:
            value = value.tz_localize(timezone.utc)
        value = value.to_pydatetime()
    return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def _clean_float(value: Any) -> Any:
    if isinstance(value, bool):
        return value
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        if math.isnan(value) or math.isinf(value):
            return None
        return value
    return value


def clean_json_value(value: Any) -> Any:
    if value is None:
        return None
    if isinstance(value, (datetime, pd.Timestamp)):
        return isoformat_utc(value)
    if isinstance(value, dict):
        return {key: clean_json_value(val) for key, val in value.items()}
    if isinstance(value, list):
        return [clean_json_value(item) for item in value]
    if isinstance(value, tuple):
        return [clean_json_value(item) for item in value]
    return _clean_float(value)


def safe_ratio(numerator: float | None, denominator: float | None) -> float | None:
    if numerator is None or denominator is None:
        return None
    if denominator == 0:
        return None
    return numerator / denominator
