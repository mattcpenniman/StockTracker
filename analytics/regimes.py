from __future__ import annotations

import pandas as pd

from .config import AnalyticsSettings


def classify_trend_regime(row: pd.Series) -> str:
    if pd.notna(row.get("close")) and pd.notna(row.get("sma_50")) and pd.notna(row.get("sma_200")):
        if row["close"] > row["sma_50"] and row["sma_50"] > row["sma_200"]:
            return "uptrend"
        if row["close"] < row["sma_50"] and row["sma_50"] < row["sma_200"]:
            return "downtrend"
    return "sideways"


def classify_momentum_regime(row: pd.Series) -> str:
    if pd.notna(row.get("rsi_14")) and pd.notna(row.get("macd_hist")):
        if row["rsi_14"] > 55 and row["macd_hist"] > 0:
            return "bullish"
        if row["rsi_14"] < 45 and row["macd_hist"] < 0:
            return "bearish"
    return "neutral"


def classify_volatility_regime(row: pd.Series, settings: AnalyticsSettings) -> str:
    ratio = row.get("volatility_ratio")
    if pd.notna(ratio):
        if ratio < settings.volatility_regime_low_multiple:
            return "low"
        if ratio > settings.volatility_regime_high_multiple:
            return "high"
    return "moderate"


def classify_position_in_range(row: pd.Series) -> str:
    position = row.get("range_position_20")
    if pd.isna(position):
        return "unknown"
    if position <= 0.2:
        return "near_low"
    if position >= 0.8:
        return "near_high"
    return "mid_range"


def classify_signal_bias(row: pd.Series, settings: AnalyticsSettings) -> str:
    breakout = bool(row.get("is_breakout")) if pd.notna(row.get("is_breakout")) else False
    breakdown = bool(row.get("is_breakdown")) if pd.notna(row.get("is_breakdown")) else False
    trend = classify_trend_regime(row)
    momentum = classify_momentum_regime(row)

    if breakout and momentum == "bullish":
        return "strong_bullish"
    if breakdown and momentum == "bearish":
        return "strong_bearish"

    if trend == "uptrend" and momentum == "bullish":
        return "bullish"
    if trend == "downtrend" and momentum == "bearish":
        return "bearish"

    trend_strength = row.get("trend_strength")
    if pd.notna(trend_strength) and pd.notna(row.get("atr_14")):
        if trend_strength >= 0.5:
            return "weak_bullish"
        if trend_strength <= -0.5:
            return "weak_bearish"

    if momentum == "bullish":
        return "weak_bullish"
    if momentum == "bearish":
        return "weak_bearish"
    return "neutral"
