# Historical multi-date contract (VEC-811)

`POST /api/portfolio/history` is a synchronous, stored-data-only daily sweep.
The service entrypoint is `AnalyticsService.get_historical_portfolio(symbols,
cutoffs, settings, days=20, stop_loss_pct=None, take_profit_pct=None, side="long")`.

```json
{"symbols":["NVDA","AAPL"],"cutoffs":["2026-03-17","2026-06-16T00:00:00Z"],
 "timeframe":"1D","settings":{"breakout_lookback":55,"buffer_pct":0.005},
 "days":20,"stop_loss_pct":0.05,"take_profit_pct":0.10,"side":"long"}
```

Omit `symbols` to snapshot saved portfolio membership once; `include_hidden`
(JSON boolean, default false) then controls membership. Explicit symbols are
trimmed, uppercased and deduplicated in first-seen order. Membership is **current
saved membership**, not historical holdings. Settings are one `AnalyticsSettings`
object for the whole sweep; the HTTP settings object accepts its field names.
Only `1D`/`1Day` are supported. Cutoffs use the existing `parse_asof`: date-only
means 23:59:59.999999 UTC; explicit timestamps retain their instant. Cutoff order
and duplicates are retained with a zero-based `date_index`.

## Bar completion and availability (VEC-829)

All state, event and label-anchor reads use completed stored bars. Daily bars
are start-labelled aggregates without regular-session-only provenance; their
UTC label date identifies the session for both legacy 00Z and Alpaca 04Z/05Z
labels. Final daily OHLCV becomes eligible at **the next America/New_York
midnight**, including on early-close days. DST uses the IANA timezone, not a
fixed 24-hour delay. Daily bars can contain extended-hours trades, so neither
the regular close nor an early close releases them. Weekends and holidays do
not manufacture bars; freshness diagnostics use the NYSE calendar's previous
eligible session. At a date-only UTC end-of-day cutoff, that NY date's final
daily aggregate is still unavailable; request the next NY midnight to include it.

Intraday OHLCV is eligible when `bar_start + full timeframe interval <= cutoff`.
Premarket and postmarket bars are included, and intervals are never shortened
at a regular or early close. No partial aggregate is built. At 14:00Z the 13:45Z
15-minute bar is eligible, but the 14:00Z bar is not. Equality at completion is
inclusive; equality at a bar's start does not release its final values.

This same policy bounds Python/native inputs, single/batch/portfolio reads,
SPY/QQQ, historical prefixes, prices and price-dependent ratios/signals. Missing
intraday prices retain the service's completed-daily fallback. Quality includes
`bar_availability`, `indicator_available_at` and `price_available_at`; label
quality includes `anchor_available_at`. Label anchors use the latest eligible
bar of the requested timeframe; the realized window is the next N stored bars
strictly after that anchor (not N bars after the requested instant). Its first
bar can therefore overlap the cutoff. Label OHLCV is deliberately future data.

Eligibility models event-time completion conservatively, not the historical
instant the provider published or revised each observation. The database has
no receipt-time/version history; these guarantees do not certify historical
provider latency or adjustment/revision provenance. Current reads evaluate
completion against now, rounded down to the cache's minute boundary.

Response: `timeframe`, `symbols`, `date_count`, `sample_count` (requested
date-symbol pairs), `limits`, `execution`, and `dates`. Each date contains
`date_index`, original `cutoff`, normalized `requested_asof`, `features` and
`labels`. These last two objects have exactly the existing `get_batch_state`
and `get_batch_future_state` schemas, including complete per-symbol payloads,
errors, benchmark errors, timestamps, stale coverage and partial-window flags.
HTTP adds `ok: true`. Join features and realized labels by
`(date_index, symbol)`, **not** by actual anchor timestamp: daily state can use
a stored 15-minute price anchor while labels always use daily bars. Labels
contain realized future information and must never become historical features.
Ranks and SPY/QQQ alignment are evaluated separately for each date. Empty saved
portfolios are allowed. Missing metadata/history/future windows are associated
with the date and symbol in the appropriate section's `errors` list.

Hard bounds: 120 cutoffs, 50 input symbols, 3,000 date-symbol pairs,
1–252 future daily bars, 20,000 daily history bars per symbol, 32 MiB compact JSON
result, 64 KiB HTTP request. Invalid input returns 400; size/history/output limits
return 413 with no partial result. Split symbols/dates into smaller requests;
history over the cap cannot be truncated without changing EMA/RSI seeds. One
sweep is admitted per worker; another returns 503 / Retry-After: 1. Interactive
requests are not subject to that gate. No background job infrastructure is used.

Each symbol (including SPY/QQQ) is processed and released before the next:
one capped daily history read through the maximum cutoff, one feature series,
one bounded query for all required 15-minute anchors and (requested symbols
only) one typed daily tail up to the horizon. Earlier daily label windows reuse
that history and `future_labels.scan_windows`. Features never see the tail.
No per-date history reads, feature recomputation or shared cache population.
Execution counters report history/price/tail reads, loaded rows and feature
computations. Read-only PostgreSQL connections are supported.
If an unusual later bar makes native dispatch ineligible, one additional native
prefix calculation preserves the earlier single-date backend/numeric values;
this is still bounded per symbol, independent of the number of cutoffs.

No provider synchronization or metadata creation occurs. Use the existing
`POST /api/coverage/prepare` explicitly for missing daily/SPY/QQQ coverage and
15-minute price windows (respect its symbol/window limits), then rerun. Coverage
diagnostics use calendar-aware expected eligible sessions/regular intervals and
`completeness_verified: false`; a recent anchor is not proof of gap-free data.
No transaction-wide market-data snapshot is promised across symbol reads.

## Look-ahead audit

Both `analytics/indicators.py` and `rust/src/lib.rs` were audited. Returns use
positive lags; SMA, range extrema, standard deviation, z-scores, volume means and
volatility-baseline median use trailing windows; EMA, Wilder RSI/ATR and MACD use
forward recurrences with original history seeds; slopes and breakout/breakdown
levels use positive shifts. Ratios, distances, regimes and anomaly flags are
row-local. YTD divides the row's daily close by the final trading-session close
of the preceding calendar year, including the first trading day's gain/loss.
The prior-year anchor must be present on the final NYSE session date (UTC daily
bar labels); otherwise YTD is null, including IPOs, truncated histories, missing
year-end sessions and whole missing years. There is no first-in-year or older-year
fallback. Both prices use the stored adjustment basis: this is price return,
not dividend-reinvested total return. `data_quality.ytd` reports unavailable
anchors or qualifies available returns because adjustment provenance remains
unverified (see VEC-831). State YTD uses the daily indicator close, even when
`price` uses a 15-minute anchor. No centered window, negative shift or backward fill is
used. Thus a prefix of the computed series is point-in-time equivalent. Tests
compare every column after removing/changing later bars, plus full serialized
per-date payloads under multiple settings/backends. Quality bar counts use only
the prefix, never the full series length.
