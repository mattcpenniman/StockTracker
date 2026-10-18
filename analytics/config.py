from __future__ import annotations

from dataclasses import dataclass, fields
from functools import cached_property


@dataclass(frozen=True)
class AnalyticsSettings:
    breakout_lookback: int = 20
    buffer_pct: float = 0.0025
    volume_confirmation: bool = True
    volume_multiple: float = 1.5
    sma_slope_lookback: int = 5
    event_limit: int = 20
    rsi_overbought: float = 70.0
    rsi_oversold: float = 30.0
    volume_zscore_threshold: float = 2.0
    volatility_spike_multiple: float = 1.5
    volatility_regime_low_multiple: float = 0.75
    volatility_regime_high_multiple: float = 1.5

    @cached_property
    def _cache_key(self) -> tuple:
        # Settings are frozen scalar values. Build once per settings instance
        # (normally one request), shared by batch planning and actual lookups.
        # Keep the old sorted (name, value) representation and every field.
        return tuple(sorted((field.name, getattr(self, field.name)) for field in fields(self)))

    def cache_key(self) -> tuple:
        return self._cache_key
