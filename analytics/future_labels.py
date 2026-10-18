"""Compact future-window reduction, independent of pandas and transport.

The batch boundary is also used by the experimental native comparator. Results
are indices so UTC timestamps never cross the floating-point/native boundary.
"""
from math import isnan


def scan_windows(windows, anchors, stop_loss_pct, take_profit_pct, side):
    results = []
    for bars, close in zip(windows, anchors):
        direction = 1 if side == "long" else -1
        stop = close * (1.0 - direction * stop_loss_pct) if stop_loss_pct is not None else None
        take = close * (1.0 + direction * take_profit_pct) if take_profit_pct is not None else None
        high = low = stop_hit = take_hit = -1
        for i, bar in enumerate(bars):
            if not isnan(bar.high) and (high < 0 or bar.high > bars[high].high):
                high = i
            if not isnan(bar.low) and (low < 0 or bar.low < bars[low].low):
                low = i
            if stop_hit < 0 and stop is not None and (bar.low <= stop if side == "long" else bar.high >= stop):
                stop_hit = i
            if take_hit < 0 and take is not None and (bar.high >= take if side == "long" else bar.low <= take):
                take_hit = i
        results.append((high, low, stop_hit, take_hit))
    return results
