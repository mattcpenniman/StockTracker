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

