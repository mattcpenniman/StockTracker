from __future__ import annotations

from dataclasses import asdict, dataclass


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

    def cache_key(self) -> tuple:
        return tuple(sorted(asdict(self).items()))

