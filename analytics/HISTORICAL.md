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
diagnostics retain the seven-calendar-day warning heuristic and
`completeness_verified: false`; a recent anchor is not proof of gap-free data.
No transaction-wide market-data snapshot is promised across symbol reads.

## Look-ahead audit

Both `analytics/indicators.py` and `rust/src/lib.rs` were audited. Returns use
positive lags; SMA, range extrema, standard deviation, z-scores, volume means and
volatility-baseline median use trailing windows; EMA, Wilder RSI/ATR and MACD use
forward recurrences with original history seeds; slopes and breakout/breakdown
levels use positive shifts. Ratios, distances, regimes and anomaly flags are
row-local. YTD uses the first stored close of the row's year (no backfill or
year-end statistic). No centered window, negative shift or backward fill is
used. Thus a prefix of the computed series is point-in-time equivalent. Tests
compare every column after removing/changing later bars, plus full serialized
per-date payloads under multiple settings/backends. Quality bar counts use only
the prefix, never the full series length.
