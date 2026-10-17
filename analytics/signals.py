from __future__ import annotations

from typing import Any

import numpy as np
import pandas as pd

from .config import AnalyticsSettings


def _crosses_above(series_a: pd.Series, series_b: pd.Series) -> pd.Series:
    return (
        series_a.shift(1).notna()
        & series_b.shift(1).notna()
        & (series_a.shift(1) <= series_b.shift(1))
        & (series_a > series_b)
    )


def _crosses_below(series_a: pd.Series, series_b: pd.Series) -> pd.Series:
    return (
        series_a.shift(1).notna()
        & series_b.shift(1).notna()
        & (series_a.shift(1) >= series_b.shift(1))
        & (series_a < series_b)
    )


def compute_signal_columns(df: pd.DataFrame, settings: AnalyticsSettings) -> pd.DataFrame:
    out = df.copy()

    volume_confirmed = out["volume_ratio_20"] >= settings.volume_multiple
    if settings.volume_confirmation:
        breakout_ok = volume_confirmed
        breakdown_ok = volume_confirmed
    else:
        breakout_ok = pd.Series(True, index=out.index)
        breakdown_ok = pd.Series(True, index=out.index)

    out["is_breakout"] = out["close"] > out["breakout_level"]
    out["is_breakout"] = out["is_breakout"] & breakout_ok & out["breakout_level"].notna()

    out["is_breakdown"] = out["close"] < out["breakdown_level"]
    out["is_breakdown"] = out["is_breakdown"] & breakdown_ok & out["breakdown_level"].notna()

    out["above_sma20"] = out["close"] > out["sma_20"]
    out["above_sma50"] = out["close"] > out["sma_50"]
    out["above_sma200"] = out["close"] > out["sma_200"]
    out["volume_spike"] = (out["volume_zscore"] >= settings.volume_zscore_threshold) | (out["volume_ratio_20"] >= settings.volume_multiple)
    out["volatility_spike"] = out["volatility_ratio"] > settings.volatility_spike_multiple
    return out


def generate_events(df: pd.DataFrame, settings: AnalyticsSettings, event_limit: int | None = None) -> list[dict[str, Any]]:
    if df.empty:
        return []

    working = compute_signal_columns(df, settings)
    close = working["close"]

    timestamp_col = working["timestamp"]

    price_crosses = {
        "price_crosses_above_sma20": _crosses_above(close, working["sma_20"]),
        "price_crosses_below_sma20": _crosses_below(close, working["sma_20"]),
        "price_crosses_above_sma50": _crosses_above(close, working["sma_50"]),
        "price_crosses_below_sma50": _crosses_below(close, working["sma_50"]),
        "price_crosses_above_sma200": _crosses_above(close, working["sma_200"]),
        "price_crosses_below_sma200": _crosses_below(close, working["sma_200"]),
    }

    macd_crosses = {
        "macd_bullish_cross": _crosses_above(working["macd"], working["macd_signal"]),
        "macd_bearish_cross": _crosses_below(working["macd"], working["macd_signal"]),
    }

    rsi_events = {
        "rsi_enters_overbought": (working["rsi_14"] > settings.rsi_overbought) & (working["rsi_14"].shift(1) <= settings.rsi_overbought),
        "rsi_enters_oversold": (working["rsi_14"] < settings.rsi_oversold) & (working["rsi_14"].shift(1) >= settings.rsi_oversold),
    }

    new_extrema = {
        "new_high_20": working["high"] > working["prior_high_20"],
        "new_low_20": working["low"] < working["prior_low_20"],
        "new_high_55": working["high"] > working["prior_high_55"],
        "new_low_55": working["low"] < working["prior_low_55"],
    }

    # Spec order is the historical append order, including ties within a bar.
    specs = [
        ("breakout", "close", "breakout_level", "breakout_strength"),
        ("breakdown", "close", "breakdown_level", "breakdown_strength"),
        *((name, "close", name.split("_")[-1].replace("sma", "sma_"), None) for name in price_crosses),
        *((name, "macd", "macd_signal", None) for name in macd_crosses),
        *((name, "rsi_14", None, None) for name in rsi_events),
        *((name, "high" if "high" in name else "low", name.replace("new_", "prior_"), None) for name in new_extrema),
        ("volume_spike", "volume", "avg_volume_20", "volume_zscore"),
        ("volatility_spike", "realized_vol_20", "volatility_baseline", "volatility_ratio"),
    ]
    masks = np.column_stack([
        working["is_breakout"], working["is_breakdown"],
        *price_crosses.values(), *macd_crosses.values(), *rsi_events.values(),
        *new_extrema.values(), working["volume_spike"], working["volatility_spike"],
    ])
    candidates = np.flatnonzero(masks.any(axis=1) & timestamp_col.notna().to_numpy())
    if not len(candidates):
        return []
    # Stable descending timestamp order also preserves original row order across
    # duplicate timestamps. Do not reverse rows: that would reverse these ties.
    order = timestamp_col.iloc[candidates].reset_index(drop=True).sort_values(
        ascending=False, kind="stable",
    ).index.to_numpy()
    columns = {key: working[key].to_numpy(copy=False) for spec in specs for key in spec[1:] if key is not None}
    timestamps = timestamp_col.array
    volume_confirmed = (working["volume_ratio_20"] >= settings.volume_multiple).to_numpy()
    limit = None if event_limit is None else max(1, event_limit)
    event_rows: list[dict[str, Any]] = []
    for idx in candidates[order]:
        timestamp = timestamps[idx]
        for event_idx in np.flatnonzero(masks[idx]):
            event_type, value, reference, strength = specs[event_idx]
            event = {"event_type": event_type, "timestamp": timestamp, "value": columns[value][idx]}
            if reference is not None:
                event["reference_level"] = columns[reference][idx]
            if strength is not None:
                event["strength"] = columns[strength][idx]
            if event_type in {"breakout", "breakdown"}:
                event["confirmed_by_volume"] = bool(volume_confirmed[idx])
            event_rows.append(event)
            # Limit before materialization: a partial final bar keeps spec order.
            if limit is not None and len(event_rows) >= limit:
                return event_rows
    return event_rows


def latest_signal_payload(row: pd.Series) -> dict[str, Any]:
    return {
        "is_breakout": bool(row["is_breakout"]) if pd.notna(row.get("is_breakout")) else False,
        "is_breakdown": bool(row["is_breakdown"]) if pd.notna(row.get("is_breakdown")) else False,
        "breakout_level": row.get("breakout_level"),
        "breakdown_level": row.get("breakdown_level"),
        "breakout_strength": row.get("breakout_strength"),
        "breakdown_strength": row.get("breakdown_strength"),
        "above_sma20": bool(row["above_sma20"]) if pd.notna(row.get("above_sma20")) else False,
        "above_sma50": bool(row["above_sma50"]) if pd.notna(row.get("above_sma50")) else False,
        "above_sma200": bool(row["above_sma200"]) if pd.notna(row.get("above_sma200")) else False,
        "extension_from_mean": row.get("extension_from_mean"),
        "volume_zscore": row.get("volume_zscore"),
        "return_zscore": row.get("return_zscore"),
    }
