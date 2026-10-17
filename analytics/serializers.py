from __future__ import annotations

from typing import Any

import pandas as pd

from .utils import clean_json_value, isoformat_utc


def serialize_symbol_metadata(context: dict[str, Any]) -> dict[str, Any]:
    return clean_json_value(
        {
            "symbol": context["symbol"],
            "exchange": context.get("exchange"),
            "asset_class": context.get("asset_class"),
            "name": context.get("name"),
            "is_active": context.get("is_active"),
            "created_at": context.get("created_at"),
            "updated_at": context.get("updated_at"),
            "last_sync_timestamp": context.get("last_synced_at"),
            "last_successful_sync_timestamp": context.get("last_successful_sync_at"),
            "latest_bar_time": context.get("latest_bar_time"),
            "latest_quote_time": context.get("latest_quote_time"),
            "sync_status": context.get("sync_status"),
        }
    )


def serialize_events(symbol: str, timeframe: str, events: list[dict[str, Any]]) -> dict[str, Any]:
    return clean_json_value({"symbol": symbol, "timeframe": timeframe, "events": events})


def serialize_state_payload(
    symbol: str,
    timeframe: str,
    latest: pd.Series,
    data_quality: dict[str, Any],
    sections: dict[str, Any],
) -> dict[str, Any]:
    payload = {
        "symbol": symbol,
        "timeframe": timeframe,
        "as_of": isoformat_utc(latest["timestamp"]),
        **sections,
        "data_quality": data_quality,
    }
    return clean_json_value(payload)
