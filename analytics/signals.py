from __future__ import annotations

from typing import Any

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

    event_rows: list[dict[str, Any]] = []
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

    for idx in working.index:
        timestamp = timestamp_col.iloc[idx]
        if pd.isna(timestamp):
            continue

        row = working.iloc[idx]
        if bool(row.get("is_breakout")):
            event_rows.append(
                {
                    "event_type": "breakout",
                    "timestamp": timestamp,
                    "value": row["close"],
                    "reference_level": row["breakout_level"],
                    "strength": row["breakout_strength"],
                    "confirmed_by_volume": bool(row["volume_ratio_20"] >= settings.volume_multiple),
                }
            )
        if bool(row.get("is_breakdown")):
            event_rows.append(
                {
                    "event_type": "breakdown",
                    "timestamp": timestamp,
                    "value": row["close"],
                    "reference_level": row["breakdown_level"],
                    "strength": row["breakdown_strength"],
                    "confirmed_by_volume": bool(row["volume_ratio_20"] >= settings.volume_multiple),
                }
            )

        for event_type, mask in price_crosses.items():
            if bool(mask.iloc[idx]):
                event_rows.append(
                    {
                        "event_type": event_type,
                        "timestamp": timestamp,
                        "value": row["close"],
                        "reference_level": row[event_type.split("_")[-1].replace("sma", "sma_")],
                    }
                )

        for event_type, mask in macd_crosses.items():
            if bool(mask.iloc[idx]):
                event_rows.append(
                    {
                        "event_type": event_type,
                        "timestamp": timestamp,
                        "value": row["macd"],
                        "reference_level": row["macd_signal"],
                    }
                )

        for event_type, mask in rsi_events.items():
            if bool(mask.iloc[idx]):
                event_rows.append(
                    {
                        "event_type": event_type,
                        "timestamp": timestamp,
                        "value": row["rsi_14"],
                    }
                )

        for event_type, mask in new_extrema.items():
            if bool(mask.iloc[idx]):
                ref_key = "prior_high_20"
                if event_type == "new_low_20":
                    ref_key = "prior_low_20"
                elif event_type == "new_high_55":
                    ref_key = "prior_high_55"
                elif event_type == "new_low_55":
                    ref_key = "prior_low_55"
                event_rows.append(
                    {
                        "event_type": event_type,
                        "timestamp": timestamp,
                        "value": row["high"] if "high" in event_type else row["low"],
                        "reference_level": row[ref_key],
                    }
                )

        if bool(row.get("volume_spike")):
            event_rows.append(
                {
                    "event_type": "volume_spike",
                    "timestamp": timestamp,
                    "value": row["volume"],
                    "reference_level": row["avg_volume_20"],
                    "strength": row["volume_zscore"],
                }
            )
        if bool(row.get("volatility_spike")):
            event_rows.append(
                {
                    "event_type": "volatility_spike",
                    "timestamp": timestamp,
                    "value": row["realized_vol_20"],
                    "reference_level": row["volatility_baseline"],
                    "strength": row["volatility_ratio"],
                }
            )

    event_rows.sort(key=lambda item: item["timestamp"], reverse=True)
    if event_limit is not None:
        event_rows = event_rows[: max(1, event_limit)]
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
