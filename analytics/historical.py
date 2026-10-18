"""Bounded point-in-time daily sweeps. Contract and causality audit: HISTORICAL.md."""
from dataclasses import fields
from datetime import datetime
import json
import math
from threading import BoundedSemaphore

import pandas as pd

from .config import AnalyticsSettings
from .data_access import LabelBar
from .future_labels import scan_windows
from .indicators import compute_feature_frame
from .quality import DataQualityError, validate_bars
from .service import AnalyticsNotFoundError
from .utils import isoformat_utc, parse_asof
from .availability import start_bound


LIMITS = dict(max_dates=120, max_symbols=50, max_samples=3000,
              max_horizon=252, max_history_bars=20000, max_output_bytes=32 * 1024 * 1024)
_admission = BoundedSemaphore(1)


class HistoricalLimitError(ValueError):
    pass


class HistoricalBusyError(RuntimeError):
    pass


def _finite_nonnegative(value):
    try:
        return type(value) in (int, float) and math.isfinite(value) and value >= 0
    except OverflowError:
        return False


def historical_settings(values):
    """Strict JSON settings, also checked for direct service callers."""
    if not isinstance(values, dict):
        raise ValueError("'settings' must be an object.")
    defaults = AnalyticsSettings()
    known = {field.name for field in fields(defaults)}
    if values.keys() - known:
        raise ValueError("Unknown settings: " + ", ".join(sorted(values.keys() - known)))
    for name, value in values.items():
        default = getattr(defaults, name)
        if isinstance(default, bool):
            valid = isinstance(value, bool)
        elif isinstance(default, int):
            valid = type(value) is int and 0 < value <= 20000
        else:
            valid = _finite_nonnegative(value)
        if not valid:
            raise ValueError(f"Invalid setting '{name}'.")
    return AnalyticsSettings(**values)


def validate_request(symbols, cutoffs, settings, days, stop, take, side):
    if not isinstance(settings, AnalyticsSettings):
        raise ValueError("'settings' must be AnalyticsSettings.")
    if not isinstance(symbols, list) or any(not isinstance(s, str) or not s.strip() for s in symbols):
        raise ValueError("'symbols' must be an array of non-empty strings.")
    # Bound raw arrays as well as unique work before allocating/normalizing them.
    if len(symbols) > LIMITS["max_symbols"]:
        raise HistoricalLimitError("At most 50 input symbols per request.")
    if any(len(s) > 32 for s in symbols):
        raise ValueError("Symbol strings must be at most 32 characters.")
    symbols = list(dict.fromkeys(s.strip().upper() for s in symbols))
    if not isinstance(cutoffs, list) or not cutoffs:
        raise ValueError("'cutoffs' must be a non-empty array.")
    if len(cutoffs) > LIMITS["max_dates"] or len(cutoffs) * len(symbols) > LIMITS["max_samples"]:
        raise HistoricalLimitError("At most 120 dates and 3000 date-symbol pairs per request.")
    if any(not isinstance(c, (str, datetime)) for c in cutoffs):
        raise ValueError("Cutoffs must be date strings or timestamps.")
    dates = [parse_asof(c) if isinstance(c, str) else c for c in cutoffs]
    if any(c is None or c.tzinfo is None for c in dates):
        raise ValueError("Every cutoff must specify a date or timezone-aware instant.")
    if type(days) is not int or days <= 0:
        raise ValueError("'days' must be a positive integer.")
    if days > LIMITS["max_horizon"]:
        raise HistoricalLimitError("At most 252 future daily bars per request.")
    if side not in ("long", "short"):
        raise ValueError("'side' must be 'long' or 'short'.")
    if any(v is not None and not _finite_nonnegative(v) for v in (stop, take)):
        raise ValueError("Thresholds must be finite nonnegative numbers or null.")
    historical_settings({f.name: getattr(settings, f.name) for f in fields(AnalyticsSettings)})
    return symbols, dates


def historical_portfolio(service, symbols, cutoffs, settings, days, stop, take, side):
    symbols, dates = validate_request(symbols, cutoffs, settings, days, stop, take, side)
    if not _admission.acquire(blocking=False):
        raise HistoricalBusyError("A historical sweep is already running in this worker; retry later.")
    try:
        return _run(service, symbols, cutoffs, dates, settings, days, stop, take, side)
    finally:
        _admission.release()


def _run(service, symbols, cutoffs, dates, settings, days, stop, take, side):
    requested = set(symbols)
    all_symbols = list(dict.fromkeys([*symbols, "SPY", "QQQ"]))
    contexts = service.repository.fetch_symbol_contexts(all_symbols)
    states = [{} for _ in dates]
    labels = [{} for _ in dates]
    metrics = dict(history_reads=0, price_reads=0, tail_reads=0, loaded_rows=0, feature_computations=0)
    # Account for payload bytes while building, so even unexpectedly large error
    # strings cannot grow the retained response without bound.
    retained_bytes = 0

    def retain(target, index, symbol, value):
        nonlocal retained_bytes
        retained_bytes += len(json.dumps(str(value) if isinstance(value, Exception) else value,
                                         separators=(",", ":"), allow_nan=False).encode())
        if retained_bytes > LIMITS["max_output_bytes"]:
            raise HistoricalLimitError("Historical result exceeds 32 MiB; split dates/symbols.")
        target[index][symbol] = value

    for symbol in all_symbols:
        context = contexts.get(symbol)
        if context is None:
            error = AnalyticsNotFoundError(f"Symbol '{symbol}' was not found in symbol_metadata.")
            for i in range(len(dates)):
                retain(states, i, symbol, error)
                if symbol in requested:
                    retain(labels, i, symbol, error)
            continue
        sid = context["symbol_id"]
        bars = service.repository.fetch_bars_many([sid], "1Day", max(dates),
                                                 limit=LIMITS["max_history_bars"] + 1)[sid]
        metrics["history_reads"] += 1
        metrics["loaded_rows"] += len(bars)
        if len(bars) > LIMITS["max_history_bars"]:
            raise HistoricalLimitError(f"Daily history for '{symbol}' exceeds 20000 bars; cannot truncate initialization.")
        history_error = None
        valid_count = len(bars)
        try:
            features = compute_feature_frame(bars, "1Day", settings) if not bars.empty else bars
        except DataQualityError as exc:
            history_error = exc.with_traceback(None)
            # A malformed later bar must not invalidate earlier cutoffs. Find
            # the longest valid prefix only on the exceptional path.
            low, high = 0, len(bars)
            while low < high:
                middle = (low + high + 1) // 2
                try:
                    validate_bars(bars.iloc[:middle])
                    low = middle
                except DataQualityError:
                    high = middle - 1
            valid_count = low
            features = compute_feature_frame(bars.iloc[:valid_count], "1Day", settings) if valid_count else bars.iloc[:0]
        metrics["feature_computations"] += int(not bars.empty)
        price_dates = [d for d in dates if service._preferred_price_timeframe("1Day", d) == "15Min"]
        prices = service.repository.fetch_historical_price_anchors(sid, price_dates) if price_dates else {}
        metrics["price_reads"] += int(bool(price_dates))
        metrics["loaded_rows"] += len(prices)
        price_by_date = {date: prices.get(i) for i, date in enumerate(price_dates)}
        label_bars = []
        if symbol in requested and not bars.empty:
            validity = [True] * len(bars)
            if history_error is not None:
                # Preserve per-window label validation, including open/volume
                # checks which cannot be reconstructed from HLC-only records.
                prices = bars[["open", "high", "low", "close"]]
                validity = (prices.gt(0).all(axis=1) & prices.lt(float("inf")).all(axis=1)
                    & bars.volume.ge(0) & bars.volume.lt(float("inf")) & bars.volume.mod(1).eq(0)
                    & bars.low.le(bars[["open", "close"]].min(axis=1))
                    & bars.high.ge(bars[["open", "close"]].max(axis=1)) & bars.low.le(bars.high)).tolist()
            label_bars = [LabelBar(row.timestamp.to_pydatetime(), float(row.high), float(row.low), float(row.close), valid)
                          for row, valid in zip(bars.itertuples(index=False), validity)]
            tail = service.repository.fetch_label_bars_many([sid], "1Day",
                    after={sid: label_bars[-1].timestamp}, limit=days)[sid]
            label_bars.extend(tail)
            metrics["tail_reads"] += 1
            metrics["loaded_rows"] += len(tail)
        # Prefixes use the same completion boundary as repository reads.
        bounds = [start_bound(date, "1Day") for date in dates]
        counts = bars.timestamp.searchsorted(pd.to_datetime(bounds, utc=True), side="right") if not bars.empty else [0] * len(dates)
        windows = [label_bars[n:n + days] if n else [] for n in counts]
        summaries = scan_windows(windows, [label_bars[n - 1].close if n and label_bars else 0.0 for n in counts],
                                 stop, take, side) if label_bars else [None] * len(dates)
        for i, (date, count) in enumerate(zip(dates, counts)):
            if not count:
                state = AnalyticsNotFoundError(f"No bars available for symbol '{symbol}' on timeframe '1Day'.")
            elif count > valid_count:
                state = history_error
            else:
                price = price_by_date.get(date)
                try:
                    state = service._state_from_features(context, "1Day", date, settings, features.iloc[:count],
                                                         "15Min" if price is not None else "1Day", price)
                except DataQualityError as exc:
                    state = exc.with_traceback(None)
            retain(states, i, symbol, state)
            if symbol in requested:
                try:
                    label = service._typed_future_from_window(context, "1Day", date, days, stop, take, side,
                        [label_bars[count - 1]] if count else [], windows[i], summaries[i])
                except (AnalyticsNotFoundError, DataQualityError) as exc:
                    # Retain only the error, not a traceback into this request's
                    # frame (which would keep every history/result alive in a cycle).
                    label = exc.with_traceback(None)
                retain(labels, i, symbol, label)
        # No full histories/feature frames retained across symbols.
        del bars, features, label_bars, windows, prices, price_by_date

    output = []
    for i, date in enumerate(dates):
        def split(results):
            values, errors = [], []
            for symbol in symbols:
                result = results[symbol]
                if isinstance(result, Exception):
                    errors.append(dict(symbol=symbol, error=str(result)))
                else:
                    values.append(result)
            return values, errors
        values, errors = split(states[i])
        service._add_portfolio_ranks(values)
        benchmarks, benchmark_errors = service._add_benchmark_relative_returns(values, "1Day", date, settings, states[i])
        feature_payload = service._state_batch_payload("1Day", date, values, errors, benchmarks, benchmark_errors)
        values, errors = split(labels[i])
        label_payload = service._future_batch_payload("1Day", date, days, side, values, errors)
        output.append(dict(date_index=i, cutoff=cutoffs[i] if isinstance(cutoffs[i], str) else isoformat_utc(cutoffs[i]),
                           requested_asof=isoformat_utc(date), features=feature_payload, labels=label_payload))
    result = dict(timeframe="1Day", symbols=symbols, date_count=len(dates), sample_count=len(dates) * len(symbols),
                  limits=dict(LIMITS), execution=metrics, dates=output)
    if len(json.dumps(result, separators=(",", ":"), allow_nan=False).encode()) > LIMITS["max_output_bytes"]:
        raise HistoricalLimitError("Historical result exceeds 32 MiB; split dates/symbols.")
    return result
