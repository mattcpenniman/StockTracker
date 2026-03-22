from __future__ import annotations

import math

import pandas as pd

from .config import AnalyticsSettings
from .utils import safe_ratio


TRADING_PERIODS_PER_YEAR = {
    "1Day": 252.0,
    "1Hour": 252.0 * 6.5,
    "15Min": 252.0 * 26.0,
    "5Min": 252.0 * 78.0,
    "1Min": 252.0 * 390.0,
}


def _wilder_rsi(close: pd.Series, period: int = 14) -> pd.Series:
    delta = close.diff()
    gain = delta.clip(lower=0.0)
    loss = (-delta).clip(lower=0.0)
    avg_gain = gain.ewm(alpha=1 / period, adjust=False, min_periods=period).mean()
    avg_loss = loss.ewm(alpha=1 / period, adjust=False, min_periods=period).mean()
    rs = avg_gain / avg_loss.replace(0.0, pd.NA)
    rsi = 100.0 - (100.0 / (1.0 + rs))
    rsi = rsi.mask((avg_loss == 0) & avg_gain.notna(), 100.0)
    rsi = rsi.mask((avg_gain == 0) & avg_loss.notna(), 0.0)
    return rsi.where(avg_gain.notna() & avg_loss.notna(), pd.NA)


def _atr(df: pd.DataFrame, period: int = 14) -> pd.Series:
    prev_close = df["close"].shift(1)
    true_range = pd.concat(
        [
            df["high"] - df["low"],
            (df["high"] - prev_close).abs(),
            (df["low"] - prev_close).abs(),
        ],
        axis=1,
    ).max(axis=1)
    return true_range.ewm(alpha=1 / period, adjust=False, min_periods=period).mean()


def _zscore(series: pd.Series, window: int) -> pd.Series:
    rolling_mean = series.rolling(window, min_periods=window).mean()
    rolling_std = series.rolling(window, min_periods=window).std(ddof=0)
    return (series - rolling_mean) / rolling_std.replace(0.0, pd.NA)


def compute_feature_frame(
    bars: pd.DataFrame,
    timeframe: str,
    settings: AnalyticsSettings,
) -> pd.DataFrame:
    df = bars.copy()
    if df.empty:
        return df

    df = df.sort_values("timestamp").reset_index(drop=True)
    close = df["close"]
    volume = df["volume"]

    returns = close.pct_change()
    df["return_1"] = close.pct_change(1)
    df["return_5"] = close.pct_change(5)
    df["return_20"] = close.pct_change(20)
    df["return_60"] = close.pct_change(60)

    year_start_close = close.groupby(df["timestamp"].dt.year).transform("first")
    df["ytd_return"] = (close / year_start_close) - 1.0

    df["sma_20"] = close.rolling(20, min_periods=20).mean()
    df["sma_50"] = close.rolling(50, min_periods=50).mean()
    df["sma_200"] = close.rolling(200, min_periods=200).mean()
    df["ema_12"] = close.ewm(span=12, adjust=False, min_periods=12).mean()
    df["ema_26"] = close.ewm(span=26, adjust=False, min_periods=26).mean()
    df["sma20_slope"] = (df["sma_20"] - df["sma_20"].shift(settings.sma_slope_lookback)) / settings.sma_slope_lookback
    df["sma50_slope"] = (df["sma_50"] - df["sma_50"].shift(settings.sma_slope_lookback)) / settings.sma_slope_lookback

    df["rsi_14"] = _wilder_rsi(close, 14)
    df["macd"] = df["ema_12"] - df["ema_26"]
    df["macd_signal"] = df["macd"].ewm(span=9, adjust=False, min_periods=9).mean()
    df["macd_hist"] = df["macd"] - df["macd_signal"]

    df["atr_14"] = _atr(df, 14)
    df["stddev_20"] = returns.rolling(20, min_periods=20).std(ddof=0)
    annualization = math.sqrt(TRADING_PERIODS_PER_YEAR.get(timeframe, 252.0))
    df["realized_vol_20"] = df["stddev_20"] * annualization

    df["high_20"] = df["high"].rolling(20, min_periods=20).max()
    df["low_20"] = df["low"].rolling(20, min_periods=20).min()
    df["high_55"] = df["high"].rolling(55, min_periods=55).max()
    df["low_55"] = df["low"].rolling(55, min_periods=55).min()

    df["prior_high_breakout"] = df["high"].shift(1).rolling(settings.breakout_lookback, min_periods=settings.breakout_lookback).max()
    df["prior_low_breakdown"] = df["low"].shift(1).rolling(settings.breakout_lookback, min_periods=settings.breakout_lookback).min()
    df["prior_high_20"] = df["high"].shift(1).rolling(20, min_periods=20).max()
    df["prior_low_20"] = df["low"].shift(1).rolling(20, min_periods=20).min()
    df["prior_high_55"] = df["high"].shift(1).rolling(55, min_periods=55).max()
    df["prior_low_55"] = df["low"].shift(1).rolling(55, min_periods=55).min()

    df["distance_from_high_20"] = (close / df["high_20"]) - 1.0
    df["distance_from_low_20"] = (close / df["low_20"]) - 1.0
    df["distance_from_sma_20"] = (close / df["sma_20"]) - 1.0
    df["distance_from_sma_50"] = (close / df["sma_50"]) - 1.0
    df["distance_from_sma_200"] = (close / df["sma_200"]) - 1.0

    range_span = (df["high_20"] - df["low_20"]).replace(0.0, pd.NA)
    df["range_position_20"] = (close - df["low_20"]) / range_span

    df["avg_volume_20"] = volume.rolling(20, min_periods=20).mean()
    df["volume_ratio_20"] = volume / df["avg_volume_20"].replace(0.0, pd.NA)
    df["volume_zscore"] = _zscore(volume.astype(float), 20)
    df["return_zscore"] = _zscore(returns, 20)

    df["breakout_level"] = df["prior_high_breakout"] * (1.0 + settings.buffer_pct)
    df["breakdown_level"] = df["prior_low_breakdown"] * (1.0 - settings.buffer_pct)

    df["breakout_strength"] = (close - df["breakout_level"]) / df["atr_14"].replace(0.0, pd.NA)
    df["breakdown_strength"] = (df["breakdown_level"] - close) / df["atr_14"].replace(0.0, pd.NA)
    df["trend_strength"] = (df["sma_20"] - df["sma_50"]) / df["atr_14"].replace(0.0, pd.NA)
    df["extension_from_mean"] = (close - df["sma_20"]) / df["atr_14"].replace(0.0, pd.NA)

    volatility_baseline = df["realized_vol_20"].rolling(60, min_periods=20).median()
    df["volatility_baseline"] = volatility_baseline
    df["volatility_ratio"] = df["realized_vol_20"] / volatility_baseline.replace(0.0, pd.NA)
    df["volume_anomaly"] = df["volume_ratio_20"] >= settings.volume_multiple

    return df


def latest_price_payload(row: pd.Series) -> dict:
    return {
        "timestamp": row.get("timestamp"),
        "open": row["open"],
        "high": row["high"],
        "low": row["low"],
        "close": row["close"],
        "volume": int(row["volume"]) if pd.notna(row["volume"]) else None,
    }


def sufficiency_flags(df: pd.DataFrame) -> dict:
    count = len(df)
    return {
        "bar_count": count,
        "has_sufficient_history_20": count >= 20,
        "has_sufficient_history_50": count >= 50,
        "has_sufficient_history_200": count >= 200,
    }
