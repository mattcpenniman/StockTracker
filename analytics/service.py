from __future__ import annotations

from copy import copy
from typing import Any

import pandas as pd

from .config import AnalyticsSettings
from .cache import CacheCapacityError, _StateCache
from .coverage import historical_coverage
from .data_access import AnalyticsRepository, BatchReads
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


class AnalyticsService:
    def __init__(self, repository: AnalyticsRepository | None = None):
        self.repository = repository or AnalyticsRepository()
        self._state_cache = _StateCache.configured("state")
        self._events_cache = _StateCache.configured("event")
        self._future_cache = _StateCache.configured("future")

    def _context_for_symbol(self, symbol: str) -> dict[str, Any]:
        context = self.repository.fetch_symbol_context(symbol)
        if not context:
            raise AnalyticsNotFoundError(f"Symbol '{symbol.upper()}' was not found in symbol_metadata.")
        return context

    def get_symbol_metadata(self, symbol: str) -> dict[str, Any]:
        return serialize_symbol_metadata(self._context_for_symbol(symbol))

    def _batch_service(self, symbols, contexts=None):
        scoped = copy(self)
        contexts = self.repository.fetch_symbol_contexts(symbols) if contexts is None else contexts
        scoped.repository = BatchReads(self.repository, contexts)
        return scoped

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

    def _future_cache_key(self, context, timeframe, asof_dt, days, stop_loss_pct, take_profit_pct, side):
        return (
            context["symbol"], timeframe,
            isoformat_utc(asof_dt), days, stop_loss_pct, take_profit_pct, side,
            isoformat_utc(context.get("last_bar_synced_at")),
        )

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
        return self._state_cache.get_or_compute(
            cache_key, lambda: self._compute_symbol_state(context, timeframe, asof_dt, settings),
        )

    def _compute_symbol_state(self, context, timeframe, asof_dt, settings):
        indicator_timeframe = "1Day"
        price_timeframe = self._preferred_price_timeframe(timeframe, asof_dt)
        # Keep full daily history: EMA/RSI initialization and YTD depend on it.
        bars = self.repository.fetch_bars(context["symbol_id"], indicator_timeframe, asof_dt)
        if bars.empty:
            raise AnalyticsNotFoundError(
                f"No bars available for symbol '{context['symbol']}' on timeframe '{indicator_timeframe}'."
            )

        features = compute_feature_frame(bars, indicator_timeframe, settings)
        # State returns one snapshot, not historical events. All five exposed
        # signal flags are calculated from the anchor below, so constructing
        # full-history signal columns here would only duplicate that work.
        latest = features.iloc[-1].to_dict()
        anchor_bar = latest
        if price_timeframe != indicator_timeframe:
            anchor_bars = self.repository.fetch_latest_bar(context["symbol_id"], price_timeframe, asof_dt)
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
                "historical_coverage": {
                    "indicators": historical_coverage(asof_dt, latest["timestamp"]),
                    "price": historical_coverage(asof_dt, anchor_timestamp),
                },
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
                "atr_14_pct": (atr_14 / anchor_close) if pd.notna(atr_14) and anchor_close else None,
                "stddev_20": latest.get("stddev_20"),
                "realized_vol_20": latest.get("realized_vol_20"),
                "return_20_over_vol_20": latest.get("return_20_over_vol_20"),
                "return_60_over_vol_20": latest.get("return_60_over_vol_20"),
                "volatility_ratio": latest.get("volatility_ratio"),
                "regime": volatility_regime,
            },
            "range": {
                "high_20": latest.get("high_20"),
                "low_20": latest.get("low_20"),
                "high_55": latest.get("high_55"),
                "low_55": latest.get("low_55"),
                "high_252": latest.get("high_252"),
                "low_252": latest.get("low_252"),
                "distance_from_high_20_pct": distance_from_high_20_pct,
                "distance_from_low_20_pct": distance_from_low_20_pct,
                "distance_from_high_252_pct": ((anchor_close / latest.get("high_252")) - 1.0) if pd.notna(latest.get("high_252")) and latest.get("high_252") not in (0, 0.0) else None,
                "distance_from_low_252_pct": ((anchor_close / latest.get("low_252")) - 1.0) if pd.notna(latest.get("low_252")) and latest.get("low_252") not in (0, 0.0) else None,
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
                **latest_signal_payload({
                    **latest, "is_breakout": is_breakout, "is_breakdown": is_breakdown,
                    "above_sma20": above_sma20, "above_sma50": above_sma50, "above_sma200": above_sma200,
                }),
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
        payload["requested_asof"] = isoformat_utc(asof_dt)
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
        return self._events_cache.get_or_compute(
            cache_key, lambda: self._compute_events(context, timeframe, asof_dt, settings, event_limit),
        )

    def _compute_events(self, context, timeframe, asof_dt, settings, event_limit):
        bars = self.repository.fetch_bars(context["symbol_id"], timeframe, asof_dt)
        if bars.empty:
            raise AnalyticsNotFoundError(
                f"No bars available for symbol '{context['symbol']}' on timeframe '{timeframe}'."
            )
        features = compute_feature_frame(bars, timeframe, settings)
        events = generate_events(features, settings, event_limit=event_limit or settings.event_limit)
        payload = serialize_events(context["symbol"], timeframe, events)
        # The route historically built an entire daily state only to obtain its
        # price timestamp. Preserve that exact anchor/fallback policy with bounded
        # reads, reusing the already computed event series where possible.
        daily_bars = features if timeframe == "1Day" else self.repository.fetch_latest_bar(
            context["symbol_id"], "1Day", asof_dt,
        )
        if daily_bars.empty:
            raise AnalyticsNotFoundError(
                f"No bars available for symbol '{context['symbol']}' on timeframe '1Day'."
            )
        anchor_timestamp = daily_bars.iloc[-1]["timestamp"]
        price_timeframe = self._preferred_price_timeframe(timeframe, asof_dt)
        if price_timeframe != "1Day":
            price_bars = features if price_timeframe == timeframe else self.repository.fetch_latest_bar(
                context["symbol_id"], price_timeframe, asof_dt,
            )
            if not price_bars.empty:
                anchor_timestamp = price_bars.iloc[-1]["timestamp"]
        payload["as_of"] = isoformat_utc(anchor_timestamp)
        return payload

    def get_future_state(
        self,
        symbol: str,
        timeframe: str,
        asof_dt,
        days: int,
        stop_loss_pct: float | None = None,
        take_profit_pct: float | None = None,
        side: str = "long",
    ) -> dict[str, Any]:
        if days <= 0:
            raise ValueError("'days' must be greater than zero.")
        if stop_loss_pct is not None and stop_loss_pct < 0:
            raise ValueError("'stop_loss_pct' must be greater than or equal to zero.")
        if take_profit_pct is not None and take_profit_pct < 0:
            raise ValueError("'take_profit_pct' must be greater than or equal to zero.")
        if side not in {"long", "short"}:
            raise ValueError("'side' must be either 'long' or 'short'.")

        context = self._context_for_symbol(symbol)
        # Labels use one consistent bar series for the anchor and horizon.
        # State price snapshots may prefer intraday resolution, but that must
        # never turn a requested daily label into N fifteen-minute bars.
        cache_key = self._future_cache_key(context, timeframe, asof_dt, days, stop_loss_pct, take_profit_pct, side)
        return self._future_cache.get_or_compute(
            cache_key, lambda: self._compute_future_state(context, timeframe, asof_dt, days, stop_loss_pct, take_profit_pct, side),
        )

    def _compute_future_state(self, context, timeframe, asof_dt, days, stop_loss_pct, take_profit_pct, side):
        repository = self.repository.repository if isinstance(self.repository, BatchReads) else self.repository
        if callable(getattr(type(repository), "fetch_label_summary", None)):
            return self._compute_typed_future_state(context, timeframe, asof_dt, days, stop_loss_pct, take_profit_pct, side)
        return self._compute_future_state_reference(context, timeframe, asof_dt, days, stop_loss_pct, take_profit_pct, side)

    def _compute_typed_future_state(self, context, timeframe, asof_dt, days, stop_loss_pct, take_profit_pct, side):
        anchors, bars, summary = self.repository.fetch_label_summary(
            context["symbol_id"], timeframe, asof_dt, days, stop_loss_pct, take_profit_pct, side)
        if not anchors:
            raise AnalyticsNotFoundError(
                f"No anchor bar available for symbol '{context['symbol']}' on timeframe '{timeframe}' at the requested as-of."
            )
        if not bars:
            raise AnalyticsNotFoundError(
                f"No future bars available for symbol '{context['symbol']}' on timeframe '{timeframe}' for the requested window."
            )
        anchor = anchors[-1]
        close = anchor.close
        direction = 1 if side == "long" else -1
        stop = close * (1.0 - direction * stop_loss_pct) if stop_loss_pct is not None else None
        take = close * (1.0 + direction * take_profit_pct) if take_profit_pct is not None else None
        high_idx, low_idx, stop_idx, take_idx = summary
        if high_idx < 0 or low_idx < 0:
            raise ValueError("Future window has no valid extrema.")
        high, low = bars[high_idx], bars[low_idx]
        stop_hit = bars[stop_idx] if stop_idx >= 0 else None
        take_hit = bars[take_idx] if take_idx >= 0 else None
        first, timestamp = None, None
        if stop_hit is not None and take_hit is not None:
            first = "both" if stop_hit.timestamp == take_hit.timestamp else "stop_loss" if stop_hit.timestamp < take_hit.timestamp else "take_profit"
            timestamp = min(stop_hit.timestamp, take_hit.timestamp)
        elif stop_hit is not None:
            first, timestamp = "stop_loss", stop_hit.timestamp
        elif take_hit is not None:
            first, timestamp = "take_profit", take_hit.timestamp
        return self._future_payload(context, timeframe, asof_dt, days, stop_loss_pct, take_profit_pct, side,
            anchor.timestamp, close, len(bars), bars[-1], high, low, stop, take, stop_hit, take_hit, first, timestamp)

    def _compute_future_state_reference(self, context, timeframe, asof_dt, days, stop_loss_pct, take_profit_pct, side):
        """Pandas oracle retained for differential tests and measurements."""
        price_timeframe = timeframe
        anchor_bars = self.repository.fetch_latest_bar(context["symbol_id"], price_timeframe, asof_dt)
        if anchor_bars.empty:
            raise AnalyticsNotFoundError(
                f"No anchor bar available for symbol '{context['symbol']}' on timeframe '{price_timeframe}' at the requested as-of."
            )

        anchor_bar = anchor_bars.iloc[-1]
        anchor_timestamp = anchor_bar["timestamp"]
        anchor_close = float(anchor_bar["close"])
        future_bars = self.repository.fetch_future_bars(
            context["symbol_id"],
            price_timeframe,
            after=anchor_timestamp,
            limit=days,
        )
        if future_bars.empty:
            raise AnalyticsNotFoundError(
                f"No future bars available for symbol '{context['symbol']}' on timeframe '{price_timeframe}' for the requested window."
            )

        end_bar = future_bars.iloc[-1]
        high_idx = future_bars["high"].idxmax()
        low_idx = future_bars["low"].idxmin()
        max_bar = future_bars.loc[high_idx]
        min_bar = future_bars.loc[low_idx]
        complete_window = len(future_bars) == days

        if side == "long":
            stop_loss_price = (anchor_close * (1.0 - stop_loss_pct)) if stop_loss_pct is not None else None
            take_profit_price = (anchor_close * (1.0 + take_profit_pct)) if take_profit_pct is not None else None
        else:
            stop_loss_price = (anchor_close * (1.0 + stop_loss_pct)) if stop_loss_pct is not None else None
            take_profit_price = (anchor_close * (1.0 - take_profit_pct)) if take_profit_pct is not None else None

        stop_loss_hit = None
        if stop_loss_price is not None:
            if side == "long":
                stop_loss_hits = future_bars[future_bars["low"] <= stop_loss_price]
            else:
                stop_loss_hits = future_bars[future_bars["high"] >= stop_loss_price]
            if not stop_loss_hits.empty:
                stop_loss_hit = stop_loss_hits.iloc[0]

        take_profit_hit = None
        if take_profit_price is not None:
            if side == "long":
                take_profit_hits = future_bars[future_bars["high"] >= take_profit_price]
            else:
                take_profit_hits = future_bars[future_bars["low"] <= take_profit_price]
            if not take_profit_hits.empty:
                take_profit_hit = take_profit_hits.iloc[0]

        first_threshold_hit = None
        first_threshold_hit_timestamp = None
        if stop_loss_hit is not None and take_profit_hit is not None:
            stop_loss_hit_ts = pd.Timestamp(stop_loss_hit["timestamp"])
            take_profit_hit_ts = pd.Timestamp(take_profit_hit["timestamp"])
            if stop_loss_hit_ts < take_profit_hit_ts:
                first_threshold_hit = "stop_loss"
                first_threshold_hit_timestamp = stop_loss_hit["timestamp"]
            elif take_profit_hit_ts < stop_loss_hit_ts:
                first_threshold_hit = "take_profit"
                first_threshold_hit_timestamp = take_profit_hit["timestamp"]
            else:
                first_threshold_hit = "both"
                first_threshold_hit_timestamp = stop_loss_hit["timestamp"]
        elif stop_loss_hit is not None:
            first_threshold_hit = "stop_loss"
            first_threshold_hit_timestamp = stop_loss_hit["timestamp"]
        elif take_profit_hit is not None:
            first_threshold_hit = "take_profit"
            first_threshold_hit_timestamp = take_profit_hit["timestamp"]

        return self._future_payload(context, timeframe, asof_dt, days, stop_loss_pct, take_profit_pct, side,
            anchor_timestamp, anchor_close, len(future_bars), end_bar, max_bar, min_bar,
            stop_loss_price, take_profit_price, stop_loss_hit, take_profit_hit,
            first_threshold_hit, first_threshold_hit_timestamp)

    def _future_payload(self, context, timeframe, asof_dt, days, stop_loss_pct, take_profit_pct, side,
                        anchor_timestamp, anchor_close, count, end_bar, max_bar, min_bar,
                        stop_loss_price, take_profit_price, stop_loss_hit, take_profit_hit,
                        first_threshold_hit, first_threshold_hit_timestamp):
        price_timeframe = timeframe
        complete_window = count == days
        payload = clean_json_value(
            {
                "symbol": context["symbol"],
                "timeframe": timeframe,
                "as_of": anchor_timestamp,
                "requested_asof": isoformat_utc(asof_dt),
                "data_quality": {
                    "historical_coverage": historical_coverage(asof_dt, anchor_timestamp),
                    "complete_future_window": complete_window,
                },
                "horizon_days": days,
                "side": side,
                "anchor_price": {
                    "close": anchor_close,
                    "timestamp": anchor_timestamp,
                    "source_timeframe": price_timeframe,
                },
                "window": {
                    "requested_future_bar_count": days,
                    "future_bar_count": count,
                    "complete_window": complete_window,
                    "target_end_timestamp": end_bar["timestamp"] if complete_window else None,
                    "realized_end_timestamp": end_bar["timestamp"],
                    "source_timeframe": price_timeframe,
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
                    "source_timeframe": price_timeframe,
                    "side": side,
                    "stop_loss_pct": stop_loss_pct,
                    "stop_loss_price": stop_loss_price,
                    "stop_loss_hit_timestamp": stop_loss_hit["timestamp"] if stop_loss_hit is not None else None,
                    "take_profit_pct": take_profit_pct,
                    "take_profit_price": take_profit_price,
                    "take_profit_hit_timestamp": take_profit_hit["timestamp"] if take_profit_hit is not None else None,
                    "first_threshold_hit": first_threshold_hit,
                    "first_threshold_hit_timestamp": first_threshold_hit_timestamp,
                },
            }
        )
        return payload

    def _state_metric(self, state: dict[str, Any], path: tuple[str, str]) -> float | None:
        section = state.get(path[0])
        if not isinstance(section, dict):
            return None
        value = section.get(path[1])
        if value is None or pd.isna(value):
            return None
        return float(value)

    def _add_portfolio_ranks(self, states: list[dict[str, Any]]) -> None:
        metric_paths = {
            "return_20": ("returns", "r_20"),
            "return_60": ("returns", "r_60"),
            "rsi_14": ("momentum", "rsi_14"),
            "trend_strength": ("trend", "trend_strength"),
            "realized_vol_20": ("volatility", "realized_vol_20"),
            "volume_ratio_20": ("volume", "volume_ratio_20"),
        }

        for state in states:
            state.setdefault("portfolio", {})

        for metric_name, path in metric_paths.items():
            values = pd.Series(
                {idx: self._state_metric(state, path) for idx, state in enumerate(states)},
                dtype="float64",
            ).dropna()
            if values.empty:
                continue
            ranks = values.rank(method="average", pct=True)
            for idx, rank in ranks.items():
                states[int(idx)]["portfolio"][f"rank_{metric_name}"] = float(rank)

    def _add_benchmark_relative_returns(
        self,
        states: list[dict[str, Any]],
        timeframe: str,
        asof_dt,
        settings: AnalyticsSettings,
    ) -> tuple[list[str], list[dict[str, str]]]:
        benchmarks = ("SPY", "QQQ")
        state_by_symbol = {str(state.get("symbol", "")).upper(): state for state in states}
        benchmark_states: dict[str, dict[str, Any]] = {}
        benchmark_errors: list[dict[str, str]] = []

        for benchmark in benchmarks:
            if benchmark in state_by_symbol:
                benchmark_states[benchmark] = state_by_symbol[benchmark]
                continue
            try:
                benchmark_states[benchmark] = self.get_symbol_state(benchmark, timeframe, asof_dt, settings)
            except (AnalyticsNotFoundError, CacheCapacityError) as exc:
                benchmark_errors.append({"symbol": benchmark, "error": str(exc)})

        for benchmark, state in benchmark_states.items():
            coverage = state.get("data_quality", {}).get("historical_coverage", {}).get("indicators", {})
            if coverage.get("stale"):
                benchmark_errors.append({"symbol": benchmark, "error": "Stale trailing benchmark history at requested asof."})

        return_fields = ("r_1", "r_5", "r_20", "r_60")
        for state in states:
            relative = state.setdefault("relative", {})
            relative_quality = state.setdefault("data_quality", {}).setdefault("benchmark_coverage", {})
            for benchmark, benchmark_state in benchmark_states.items():
                indicator_timestamp = state.get("data_quality", {}).get("indicator_bar_timestamp")
                benchmark_timestamp = benchmark_state.get("data_quality", {}).get("indicator_bar_timestamp")
                aligned = indicator_timestamp == benchmark_timestamp
                coverage = benchmark_state.get("data_quality", {}).get("historical_coverage", {}).get("indicators", {})
                relative_quality[benchmark] = {
                    "indicator_bar_timestamp": benchmark_timestamp,
                    "aligned": aligned,
                    "historical_coverage": coverage,
                }
                if not aligned or coverage.get("stale"):
                    continue
                benchmark_returns = benchmark_state.get("returns", {}) if isinstance(benchmark_state.get("returns"), dict) else {}
                state_returns = state.get("returns", {}) if isinstance(state.get("returns"), dict) else {}
                for field in return_fields:
                    state_return = state_returns.get(field)
                    benchmark_return = benchmark_returns.get(field)
                    if state_return is not None and benchmark_return is not None and pd.notna(state_return) and pd.notna(benchmark_return):
                        relative[f"rel_return_{benchmark.lower()}_{field.removeprefix('r_')}"] = float(state_return) - float(benchmark_return)

            for error in benchmark_errors:
                relative_quality.setdefault(error["symbol"], {"status": "missing", "error": error["error"]})

        return list(benchmark_states.keys()), benchmark_errors

    def get_batch_state(
        self,
        symbols: list[str],
        timeframe: str,
        asof_dt,
        settings: AnalyticsSettings,
        *,
        contexts=None,
    ) -> dict[str, Any]:
        scoped = self._batch_service([*symbols, "SPY", "QQQ"], contexts)
        scoped.repository.ids = list(dict.fromkeys(
            context["symbol_id"] for context in scoped.repository.contexts.values()
            if self._state_cache_key(context, timeframe, isoformat_utc(asof_dt), settings) not in self._state_cache
        ))
        states: list[dict[str, Any]] = []
        errors: list[dict[str, str]] = []

        for symbol in symbols:
            try:
                states.append(scoped.get_symbol_state(symbol, timeframe, asof_dt, settings))
            except (AnalyticsNotFoundError, CacheCapacityError) as exc:
                errors.append({"symbol": str(symbol).upper(), "error": str(exc)})

        self._add_portfolio_ranks(states)
        benchmarks, benchmark_errors = scoped._add_benchmark_relative_returns(states, timeframe, asof_dt, settings)

        return clean_json_value(
            {
                "timeframe": timeframe,
                "requested_asof": isoformat_utc(asof_dt),
                "benchmarks": benchmarks,
                "states": states,
                "errors": errors,
                "benchmark_errors": benchmark_errors,
                "data_quality": {
                    "stale_symbols": [state["symbol"] for state in states if any(
                        item.get("stale") for item in state.get("data_quality", {}).get("historical_coverage", {}).values()
                    )],
                    "completeness_verified": False,
                },
            }
        )

    def get_batch_future_state(
        self,
        symbols: list[str],
        timeframe: str,
        asof_dt,
        days: int,
        stop_loss_pct: float | None = None,
        take_profit_pct: float | None = None,
        side: str = "long",
    ) -> dict[str, Any]:
        scoped = self._batch_service(symbols)
        scoped.repository.ids = list(dict.fromkeys(
            context["symbol_id"] for context in scoped.repository.contexts.values()
            if self._future_cache_key(context, timeframe, asof_dt, days, stop_loss_pct, take_profit_pct, side) not in self._future_cache
        ))
        future_states: list[dict[str, Any]] = []
        errors: list[dict[str, str]] = []

        for symbol in symbols:
            try:
                future_states.append(
                    scoped.get_future_state(
                        symbol,
                        timeframe,
                        asof_dt,
                        days,
                        stop_loss_pct=stop_loss_pct,
                        take_profit_pct=take_profit_pct,
                        side=side,
                    )
                )
            except (AnalyticsNotFoundError, CacheCapacityError) as exc:
                errors.append({"symbol": str(symbol).upper(), "error": str(exc)})

        return clean_json_value(
            {
                "timeframe": timeframe,
                "requested_asof": isoformat_utc(asof_dt),
                "horizon_days": days,
                "side": side,
                "future_states": future_states,
                "errors": errors,
                "data_quality": {
                    "stale_symbols": [state["symbol"] for state in future_states if state["data_quality"]["historical_coverage"]["stale"]],
                    "incomplete_window_symbols": [state["symbol"] for state in future_states if not state["window"]["complete_window"]],
                    "completeness_verified": False,
                },
            }
        )

    def cache_diagnostics(self) -> dict[str, Any]:
        return {"scope": "per_worker", **{name: cache.diagnostics() for name, cache in (
            ("state", self._state_cache), ("event", self._events_cache), ("future", self._future_cache),
        )}}

    def health(self) -> dict[str, Any]:
        from .runtime import runtime_diagnostics

        runtime = runtime_diagnostics()
        ok = self.repository.ping()
        return {"ok": ok and runtime["ready"], "service": "analytics", "database": "connected" if ok else "disconnected", "runtime": runtime, "caches": self.cache_diagnostics()}
