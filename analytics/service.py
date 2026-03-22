from __future__ import annotations

from collections import OrderedDict
from typing import Any

import pandas as pd

from .config import AnalyticsSettings
from .data_access import AnalyticsRepository
from .indicators import compute_feature_frame, latest_price_payload, sufficiency_flags
from .regimes import (
    classify_momentum_regime,
    classify_position_in_range,
    classify_signal_bias,
    classify_trend_regime,
    classify_volatility_regime,
)
from .serializers import serialize_events, serialize_state_payload, serialize_symbol_metadata
from .signals import compute_signal_columns, generate_events, latest_signal_payload
from .utils import clean_json_value, isoformat_utc


class AnalyticsNotFoundError(RuntimeError):
    pass


class _StateCache:
    def __init__(self, maxsize: int = 256):
        self.maxsize = maxsize
        self._data: OrderedDict[tuple, dict[str, Any]] = OrderedDict()

    def get(self, key: tuple) -> dict[str, Any] | None:
        if key not in self._data:
            return None
        self._data.move_to_end(key)
        return self._data[key]

    def set(self, key: tuple, value: dict[str, Any]) -> None:
        self._data[key] = value
        self._data.move_to_end(key)
        while len(self._data) > self.maxsize:
            self._data.popitem(last=False)


class AnalyticsService:
    def __init__(self, repository: AnalyticsRepository | None = None):
        self.repository = repository or AnalyticsRepository()
        self._state_cache = _StateCache()
        self._events_cache = _StateCache()
        self._future_cache = _StateCache()

    def _context_for_symbol(self, symbol: str) -> dict[str, Any]:
        context = self.repository.fetch_symbol_context(symbol)
        if not context:
            raise AnalyticsNotFoundError(f"Symbol '{symbol.upper()}' was not found in symbol_metadata.")
        return context

    def get_symbol_metadata(self, symbol: str) -> dict[str, Any]:
        return serialize_symbol_metadata(self._context_for_symbol(symbol))

    def _state_cache_key(
        self,
        context: dict[str, Any],
        timeframe: str,
        asof: str | None,
        settings: AnalyticsSettings,
    ) -> tuple:
        return (
            context["symbol"],
            timeframe,
            asof,
            isoformat_utc(context.get("last_bar_synced_at")),
            settings.cache_key(),
        )

    def _preferred_price_timeframe(self, requested_timeframe: str, asof_dt) -> str:
        if requested_timeframe != "1Day" or asof_dt is None:
            return requested_timeframe

        timestamp = pd.Timestamp(asof_dt)
        if timestamp.tzinfo is None:
            timestamp = timestamp.tz_localize("UTC")

        if timestamp.hour == 0 and timestamp.minute == 0 and timestamp.second == 0 and timestamp.microsecond == 0:
            return requested_timeframe
        return "15Min"

    def _load_feature_frame(
        self,
        symbol: str,
        timeframe: str,
        asof_dt,
        settings: AnalyticsSettings,
    ) -> tuple[dict[str, Any], pd.DataFrame]:
        context = self._context_for_symbol(symbol)
        bars = self.repository.fetch_bars(context["symbol_id"], timeframe, asof_dt)
        if bars.empty:
            raise AnalyticsNotFoundError(
                f"No bars available for symbol '{context['symbol']}' on timeframe '{timeframe}'."
            )
        features = compute_feature_frame(bars, timeframe, settings)
        features = compute_signal_columns(features, settings)
        return context, features

    def get_symbol_state(
        self,
        symbol: str,
        timeframe: str,
        asof_dt,
        settings: AnalyticsSettings,
    ) -> dict[str, Any]:
        context = self._context_for_symbol(symbol)
        cache_key = self._state_cache_key(context, timeframe, isoformat_utc(asof_dt), settings)
        cached = self._state_cache.get(cache_key)
        if cached is not None:
            return cached

        indicator_timeframe = "1Day"
        price_timeframe = self._preferred_price_timeframe(timeframe, asof_dt)
        bars = self.repository.fetch_bars(context["symbol_id"], indicator_timeframe, asof_dt)
        if bars.empty:
            raise AnalyticsNotFoundError(
                f"No bars available for symbol '{context['symbol']}' on timeframe '{indicator_timeframe}'."
            )

        features = compute_signal_columns(compute_feature_frame(bars, indicator_timeframe, settings), settings)
        latest = features.iloc[-1]
        anchor_bar = latest
        if price_timeframe != indicator_timeframe:
            anchor_bars = self.repository.fetch_bars(context["symbol_id"], price_timeframe, asof_dt)
            if anchor_bars.empty:
                price_timeframe = indicator_timeframe
            else:
                anchor_bar = anchor_bars.iloc[-1]

        anchor_timestamp = anchor_bar["timestamp"]
        anchor_close = float(anchor_bar["close"])
        trend_regime = classify_trend_regime(latest)
        momentum_regime = classify_momentum_regime(latest)
        volatility_regime = classify_volatility_regime(latest, settings)
        quality = sufficiency_flags(features)
        quality.update(
            {
                "last_bar_timestamp": anchor_timestamp,
                "indicator_bar_timestamp": latest["timestamp"],
                "price_bar_timestamp": anchor_timestamp,
                "indicator_source_timeframe": indicator_timeframe,
                "price_source_timeframe": price_timeframe,
                "last_sync_timestamp": context.get("last_synced_at"),
                "sync_status": context.get("sync_status"),
                "sync_error": context.get("sync_error"),
            }
        )

        volume_confirmed = bool(latest.get("volume_ratio_20") >= settings.volume_multiple) if pd.notna(latest.get("volume_ratio_20")) else False
        breakout_level = latest.get("breakout_level")
        breakdown_level = latest.get("breakdown_level")
        atr_14 = latest.get("atr_14")
        high_20 = latest.get("high_20")
        low_20 = latest.get("low_20")
        sma_20 = latest.get("sma_20")
        sma_50 = latest.get("sma_50")
        sma_200 = latest.get("sma_200")
        range_span = (high_20 - low_20) if pd.notna(high_20) and pd.notna(low_20) else None

        breakout_strength = ((anchor_close - breakout_level) / atr_14) if pd.notna(breakout_level) and pd.notna(atr_14) and atr_14 not in (0, 0.0) else None
        breakdown_strength = ((breakdown_level - anchor_close) / atr_14) if pd.notna(breakdown_level) and pd.notna(atr_14) and atr_14 not in (0, 0.0) else None
        extension_from_mean = ((anchor_close - sma_20) / atr_14) if pd.notna(sma_20) and pd.notna(atr_14) and atr_14 not in (0, 0.0) else None
        distance_from_high_20_pct = ((anchor_close / high_20) - 1.0) if pd.notna(high_20) and high_20 not in (0, 0.0) else None
        distance_from_low_20_pct = ((anchor_close / low_20) - 1.0) if pd.notna(low_20) and low_20 not in (0, 0.0) else None
        distance_from_sma_20_pct = ((anchor_close / sma_20) - 1.0) if pd.notna(sma_20) and sma_20 not in (0, 0.0) else None
        distance_from_sma_50_pct = ((anchor_close / sma_50) - 1.0) if pd.notna(sma_50) and sma_50 not in (0, 0.0) else None
        distance_from_sma_200_pct = ((anchor_close / sma_200) - 1.0) if pd.notna(sma_200) and sma_200 not in (0, 0.0) else None
        range_position_20_pct = ((anchor_close - low_20) / range_span) if range_span not in (None, 0, 0.0) else None
        above_sma20 = bool(anchor_close > sma_20) if pd.notna(sma_20) else False
        above_sma50 = bool(anchor_close > sma_50) if pd.notna(sma_50) else False
        above_sma200 = bool(anchor_close > sma_200) if pd.notna(sma_200) else False
        is_breakout = bool(anchor_close > breakout_level) if pd.notna(breakout_level) else False
        is_breakdown = bool(anchor_close < breakdown_level) if pd.notna(breakdown_level) else False
        if settings.volume_confirmation:
            is_breakout = is_breakout and volume_confirmed
            is_breakdown = is_breakdown and volume_confirmed
        anchor_classification_row = latest.copy()
        anchor_classification_row["range_position_20"] = range_position_20_pct
        anchor_classification_row["is_breakout"] = is_breakout
        anchor_classification_row["is_breakdown"] = is_breakdown
        position_in_range = classify_position_in_range(anchor_classification_row)
        signal_bias = classify_signal_bias(anchor_classification_row, settings)

        sections = {
            "state": {
                "trend": trend_regime,
                "momentum": momentum_regime,
                "volatility": volatility_regime,
                "position_in_range": position_in_range,
                "signal_bias": signal_bias,
            },
            "price": {
                **latest_price_payload(anchor_bar),
                "source_timeframe": price_timeframe,
            },
            "returns": {
                "r_1": latest.get("return_1"),
                "r_5": latest.get("return_5"),
                "r_20": latest.get("return_20"),
                "r_60": latest.get("return_60"),
                "ytd": latest.get("ytd_return"),
            },
            "trend": {
                "sma_20": latest.get("sma_20"),
                "sma_50": latest.get("sma_50"),
                "sma_200": latest.get("sma_200"),
                "ema_12": latest.get("ema_12"),
                "ema_26": latest.get("ema_26"),
                "sma20_slope": latest.get("sma20_slope"),
                "sma50_slope": latest.get("sma50_slope"),
                "regime": trend_regime,
                "trend_strength": latest.get("trend_strength"),
            },
            "momentum": {
                "rsi_14": latest.get("rsi_14"),
                "macd": latest.get("macd"),
                "macd_signal": latest.get("macd_signal"),
                "macd_hist": latest.get("macd_hist"),
                "regime": momentum_regime,
            },
            "volatility": {
                "atr_14": latest.get("atr_14"),
                "stddev_20": latest.get("stddev_20"),
                "realized_vol_20": latest.get("realized_vol_20"),
                "volatility_ratio": latest.get("volatility_ratio"),
                "regime": volatility_regime,
            },
            "range": {
                "high_20": latest.get("high_20"),
                "low_20": latest.get("low_20"),
                "high_55": latest.get("high_55"),
                "low_55": latest.get("low_55"),
                "distance_from_high_20_pct": distance_from_high_20_pct,
                "distance_from_low_20_pct": distance_from_low_20_pct,
                "distance_from_sma_20_pct": distance_from_sma_20_pct,
                "distance_from_sma_50_pct": distance_from_sma_50_pct,
                "distance_from_sma_200_pct": distance_from_sma_200_pct,
                "range_position_20_pct": range_position_20_pct,
            },
            "volume": {
                "avg_volume_20": latest.get("avg_volume_20"),
                "volume_ratio_20": latest.get("volume_ratio_20"),
                "volume_anomaly": bool(latest.get("volume_anomaly")) if pd.notna(latest.get("volume_anomaly")) else False,
            },
            "signals": {
                **latest_signal_payload(latest),
                "is_breakout": is_breakout,
                "is_breakdown": is_breakdown,
                "breakout_strength": breakout_strength,
                "breakdown_strength": breakdown_strength,
                "above_sma20": above_sma20,
                "above_sma50": above_sma50,
                "above_sma200": above_sma200,
                "extension_from_mean": extension_from_mean,
            },
        }

        latest_for_serialization = latest.copy()
        latest_for_serialization["timestamp"] = anchor_timestamp
        payload = serialize_state_payload(context["symbol"], timeframe, latest_for_serialization, quality, sections)
        self._state_cache.set(cache_key, payload)
        return payload

    def get_events(
        self,
        symbol: str,
        timeframe: str,
        asof_dt,
        settings: AnalyticsSettings,
        event_limit: int | None = None,
    ) -> dict[str, Any]:
        context = self._context_for_symbol(symbol)
        cache_key = (
            context["symbol"],
            timeframe,
            isoformat_utc(asof_dt),
            isoformat_utc(context.get("last_bar_synced_at")),
            settings.cache_key(),
            event_limit,
        )
        cached = self._events_cache.get(cache_key)
        if cached is not None:
            return cached

        bars = self.repository.fetch_bars(context["symbol_id"], timeframe, asof_dt)
        if bars.empty:
            raise AnalyticsNotFoundError(
                f"No bars available for symbol '{context['symbol']}' on timeframe '{timeframe}'."
            )
        features = compute_feature_frame(bars, timeframe, settings)
        events = generate_events(features, settings, event_limit=event_limit or settings.event_limit)
        payload = serialize_events(context["symbol"], timeframe, events)
        self._events_cache.set(cache_key, payload)
        return payload

    def get_future_state(
        self,
        symbol: str,
        timeframe: str,
        asof_dt,
        days: int,
    ) -> dict[str, Any]:
        if days <= 0:
            raise ValueError("'days' must be greater than zero.")

        context = self._context_for_symbol(symbol)
        cache_key = (
            context["symbol"],
            timeframe,
            isoformat_utc(asof_dt),
            days,
            isoformat_utc(context.get("last_bar_synced_at")),
        )
        cached = self._future_cache.get(cache_key)
        if cached is not None:
            return cached

        anchor_bars = self.repository.fetch_bars(context["symbol_id"], timeframe, asof_dt)
        if anchor_bars.empty:
            raise AnalyticsNotFoundError(
                f"No anchor bar available for symbol '{context['symbol']}' on timeframe '{timeframe}' at the requested as-of."
            )

        anchor_bar = anchor_bars.iloc[-1]
        anchor_timestamp = anchor_bar["timestamp"]
        anchor_close = float(anchor_bar["close"])
        future_bars_all = self.repository.fetch_bars(
            context["symbol_id"],
            timeframe,
            start=anchor_timestamp + pd.Timedelta(microseconds=1),
        )
        if future_bars_all.empty:
            raise AnalyticsNotFoundError(
                f"No future bars available for symbol '{context['symbol']}' on timeframe '{timeframe}' for the requested window."
            )

        future_bars = future_bars_all.head(days).copy()
        if future_bars.empty:
            raise AnalyticsNotFoundError(
                f"No future bars available for symbol '{context['symbol']}' on timeframe '{timeframe}' for the requested window."
            )

        end_bar = future_bars.iloc[-1]
        high_idx = future_bars["high"].idxmax()
        low_idx = future_bars["low"].idxmin()
        max_bar = future_bars.loc[high_idx]
        min_bar = future_bars.loc[low_idx]
        complete_window = len(future_bars) == days

        payload = clean_json_value(
            {
                "symbol": context["symbol"],
                "timeframe": timeframe,
                "as_of": anchor_timestamp,
                "horizon_days": days,
                "anchor_price": {
                    "close": anchor_close,
                    "timestamp": anchor_timestamp,
                },
                "window": {
                    "requested_future_bar_count": days,
                    "future_bar_count": len(future_bars),
                    "complete_window": complete_window,
                    "target_end_timestamp": future_bars.iloc[days - 1]["timestamp"] if complete_window else None,
                    "realized_end_timestamp": end_bar["timestamp"],
                },
                "future_state": {
                    "max_price": float(max_bar["high"]),
                    "max_price_timestamp": max_bar["timestamp"],
                    "max_return_pct": (float(max_bar["high"]) / anchor_close) - 1.0 if anchor_close else None,
                    "min_price": float(min_bar["low"]),
                    "min_price_timestamp": min_bar["timestamp"],
                    "min_return_pct": (float(min_bar["low"]) / anchor_close) - 1.0 if anchor_close else None,
                    "price_at_horizon": float(end_bar["close"]),
                    "price_at_horizon_timestamp": end_bar["timestamp"],
                    "return_at_horizon_pct": (float(end_bar["close"]) / anchor_close) - 1.0 if anchor_close else None,
                },
            }
        )
        self._future_cache.set(cache_key, payload)
        return payload

    def get_batch_state(
        self,
        symbols: list[str],
        timeframe: str,
        asof_dt,
        settings: AnalyticsSettings,
    ) -> dict[str, Any]:
        states: list[dict[str, Any]] = []
        errors: list[dict[str, str]] = []

        for symbol in symbols:
            try:
                states.append(self.get_symbol_state(symbol, timeframe, asof_dt, settings))
            except AnalyticsNotFoundError as exc:
                errors.append({"symbol": str(symbol).upper(), "error": str(exc)})

        return clean_json_value(
            {
                "timeframe": timeframe,
                "requested_asof": isoformat_utc(asof_dt),
                "states": states,
                "errors": errors,
            }
        )

    def health(self) -> dict[str, Any]:
        ok = self.repository.ping()
        return {"ok": ok, "service": "analytics", "database": "connected" if ok else "disconnected"}
