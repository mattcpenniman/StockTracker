# Stock Forecast Tracker

Single-file Flask app for tracking stock forecast prices with a PostgreSQL backend.

## Features
- Add or update stock forecasts from the web UI
- Persist stock data in PostgreSQL
- Click a symbol to open a chart page backed by PostgreSQL market data
- Auto-fetch Alpaca bars on first chart load when no cached data exists
- Show sync status, last sync timestamps, latest quote, and a manual sync button
- Track `reward_score`, `risk_score`, `confidence_score` (all 0-10)
- Track `classification` (`buy`, `hold/watch`, `sell`)
- Compute dashboard/API `opportunity_score` using:
  - `(Reward×4) + ((10-Risk)×4) + (Confidence×2)`
- Fetch live prices with `yfinance` for dashboard comparison
- Export current stock data to CSV
- Update earnings calendar data into PostgreSQL
- External API for reading/updating forecast data

## Requirements (Local)
- Python 3.10+
- PostgreSQL
- pip

## Install (Local)
```bash
pip install -r requirements.txt
```

## Run (Local)
```bash
export DATABASE_URL='postgresql://stock_user:stock_pass@localhost:5432/stock_tracker'
python app.py
```

## Run With Docker (Recommended)
```bash
docker compose up --build -d
```

Then open:
- `http://127.0.0.1:5000`
- Or your mapped port from `.env` (for example `5005`)

The containers are configured with `restart: unless-stopped`, so they will restart automatically unless you stop them explicitly.

## Migrate Existing CSV Data
If you have historical `stocks.csv`, import it into PostgreSQL:
```bash
python migrate_csv_to_postgres.py
```

Optional migration env vars:
- `STOCK_TRACKER_CSV` (default: `stocks.csv`)
- `DATABASE_URL`
- `STOCK_TRACKER_TABLE`

## Environment Variables
- `HOST` (default: `127.0.0.1`, use `0.0.0.0` in containers)
- `PORT` (default: `5000`)
- `DATABASE_URL` (default: `postgresql://stock_user:stock_pass@localhost:5432/stock_tracker`)
- `STOCK_TRACKER_TABLE` (default: `stock_forecasts`)
- `STOCK_TRACKER_EARNINGS_TABLE` (default: `earnings_calendar`)
- `FMP_API_KEY` (for earnings update route)
- `CHART_DELAY` or `CHART_DELAY_MINUTES` (default: `20`; shifts Alpaca historical bar queries back to avoid recent SIP data restrictions on free plans)

## Routes
- `GET /` - web UI
- `GET /api-docs` - API documentation page
- `POST /add` - add/update one forecast row from UI JSON
- `GET /data` - UI table data with live prices and earnings enrichment
- `GET /api/stocks` - stored rows from PostgreSQL
- `POST /api/stocks` - upsert one or many rows into PostgreSQL
- `GET /chart/<symbol>` - chart page for a symbol
- `GET /api/chart/<symbol>` - stored bars + sync state + latest quote/bar cache
- `POST /api/chart/<symbol>/sync` - fetch fresh market data from Alpaca and upsert it
- `GET /api/chart/<symbol>/latest` - latest quote/bar cache + sync state
- `GET /api/state/<symbol>` - analytics state JSON for one symbol
- `GET /api/state/<symbol>/<timeframe>` - analytics state JSON for an explicit timeframe
- `POST /api/state/batch` - analytics state JSON for many symbols
- `GET /api/events/<symbol>` - recent deterministic event JSON for one symbol
- `GET /api/events/<symbol>/<timeframe>` - event JSON for an explicit timeframe
- `GET /api/futurestate/<symbol>` - realized future max/min/end price window from an `asof` date using the next N future bars
- `GET /api/futurestate/<symbol>/<timeframe>` - realized future window for an explicit timeframe using the next N future bars
- `GET /api/metadata/<symbol>` - symbol metadata + sync freshness JSON
- `GET /api/health/analytics` - analytics health check
- `GET /export` - download current stock data as CSV
- `GET /health` - health check
- `POST /update-earnings` - refresh the earnings calendar table in PostgreSQL

## External API

### GET /api/stocks
```bash
curl -s http://127.0.0.1:5000/api/stocks
curl -s "http://127.0.0.1:5000/api/stocks?symbol=AAPL"
curl -s "http://127.0.0.1:5000/api/stocks?limit=10"
```

Example row:
```json
{
  "symbol": "AAPL",
  "forecast_price": 210.5,
  "updated_date": "2026-03-07",
  "reward_score": 8,
  "risk_score": 3,
  "confidence_score": 7,
  "classification": "buy",
  "opportunity_score": 74
}
```

### POST /api/stocks (single)
```bash
curl -X POST http://127.0.0.1:5000/api/stocks \
  -H "Content-Type: application/json" \
  -d '{"symbol":"AAPL","forecast_price":210.5,"updated_date":"2026-03-07","reward_score":8,"risk_score":3,"confidence_score":7,"classification":"buy"}'
```

### POST /api/stocks (batch)
```bash
curl -X POST http://127.0.0.1:5000/api/stocks \
  -H "Content-Type: application/json" \
  -d '{"rows":[{"symbol":"AAPL","forecast_price":210.5,"reward_score":8,"risk_score":3,"confidence_score":7,"classification":"buy"},{"symbol":"MSFT","forecast_price":480,"reward_score":6,"risk_score":4,"confidence_score":7,"classification":"hold/watch"}]}'
```

## Analytics Backend

The app now exposes a non-visual analytics layer over already-synced `stock_bars` data. It is designed for downstream agents and automation, not chart rendering.

### Analytics endpoints

`GET /api/state/<symbol>`
```bash
curl -s "http://127.0.0.1:5000/api/state/NVDA?timeframe=1D&asof=2023-09-20"
```

`GET /api/events/<symbol>`
```bash
curl -s "http://127.0.0.1:5000/api/events/NVDA?timeframe=1D&event_limit=10&breakout_lookback=20"
```

`GET /api/futurestate/<symbol>`
```bash
curl -s "http://127.0.0.1:5000/api/futurestate/NVDA?timeframe=1D&asof=2023-09-20&days=20"
```

`POST /api/state/batch`
```bash
curl -s http://127.0.0.1:5000/api/state/batch \
  -H "Content-Type: application/json" \
  -d '{"symbols":["NVDA","AAPL","MSFT"],"timeframe":"1D","asof":"2023-09-20"}'
```

`GET /api/metadata/<symbol>`
```bash
curl -s "http://127.0.0.1:5000/api/metadata/NVDA"
```

`GET /api/health/analytics`
```bash
curl -s http://127.0.0.1:5000/api/health/analytics
```

### Query parameters

- `timeframe`: supported values are `1D`/`1Day`, `1Hour`, `15Min`, `5Min`, `1Min`
- `asof`: backdated cutoff, accepts `YYYY-MM-DD` or ISO 8601
- `event_limit`: number of most recent events to return, default `20`
- `breakout_lookback`: prior-bar lookback window for breakout and breakdown rules, default `20`
- `buffer_pct`: confirmation buffer for breakout and breakdown rules, default `0.0025`
- `volume_confirmation`: require volume confirmation for breakouts and breakdowns, default `true`
- `volume_multiple`: minimum `volume / avg_volume_20` threshold for confirmation, default `1.5`

### Calculations

Returned state JSON includes:

- Returns: `r_1`, `r_5`, `r_20`, `r_60`, `ytd`
- Trend: `sma_20`, `sma_50`, `sma_200`, `ema_12`, `ema_26`, `sma20_slope`, `sma50_slope`
- Momentum: `rsi_14`, `macd`, `macd_signal`, `macd_hist`
- Volatility: `atr_14`, `stddev_20`, `realized_vol_20`, `volatility_ratio`
- Range and positioning: `high_20`, `low_20`, `high_55`, `low_55`, explicit percentage fields such as `distance_from_high_20_pct`, and `range_position_20_pct`
- Volume: `avg_volume_20`, `volume_ratio_20`, `volume_anomaly`
- Normalized metrics: `breakout_strength`, `breakdown_strength`, `trend_strength`, `extension_from_mean`, `volume_zscore`, `return_zscore`

### Regime labels

- Trend regime:
  - `uptrend` if `close > sma_50` and `sma_50 > sma_200`
  - `downtrend` if `close < sma_50` and `sma_50 < sma_200`
  - otherwise `sideways`
- Momentum regime:
  - `bullish` if `rsi_14 > 55` and `macd_hist > 0`
  - `bearish` if `rsi_14 < 45` and `macd_hist < 0`
  - otherwise `neutral`
- Volatility regime:
  - `low`, `moderate`, or `high` based on `realized_vol_20` relative to its rolling median baseline

### Event rules

The event feed returns timestamped, deterministic events such as:

- `breakout`, `breakdown`
- `price_crosses_above_sma20`, `price_crosses_below_sma20`
- `price_crosses_above_sma50`, `price_crosses_below_sma50`
- `price_crosses_above_sma200`, `price_crosses_below_sma200`
- `macd_bullish_cross`, `macd_bearish_cross`
- `rsi_enters_overbought`, `rsi_enters_oversold`
- `new_high_20`, `new_low_20`, `new_high_55`, `new_low_55`
- `volume_spike`, `volatility_spike`

### Example analytics state response

```json
{
  "ok": true,
  "symbol": "NVDA",
  "timeframe": "1Day",
  "as_of": "2023-09-20T00:00:00Z",
  "state": {
    "trend": "uptrend",
    "momentum": "bullish",
    "volatility": "moderate",
    "position_in_range": "near_high",
    "signal_bias": "bullish"
  },
  "price": {
    "open": 440.0,
    "high": 445.2,
    "low": 437.5,
    "close": 444.1,
    "volume": 51234000
  },
  "returns": {
    "r_1": 0.0121,
    "r_5": 0.0314,
    "r_20": 0.0882,
    "r_60": 0.1527,
    "ytd": 1.8411
  },
  "trend": {
    "sma_20": 430.8,
    "sma_50": 410.2,
    "sma_200": 276.4,
    "ema_12": 435.4,
    "ema_26": 426.8,
    "sma20_slope": 2.14,
    "sma50_slope": 1.36,
    "regime": "uptrend",
    "trend_strength": 2.48
  },
  "momentum": {
    "rsi_14": 63.2,
    "macd": 8.6,
    "macd_signal": 7.9,
    "macd_hist": 0.7,
    "regime": "bullish"
  },
  "volatility": {
    "atr_14": 8.3,
    "stddev_20": 0.021,
    "realized_vol_20": 0.333,
    "volatility_ratio": 1.08,
    "regime": "moderate"
  },
  "range": {
    "high_20": 446.0,
    "low_20": 395.5,
    "high_55": 481.9,
    "low_55": 352.3,
    "distance_from_high_20_pct": -0.0043,
    "distance_from_low_20_pct": 0.1229,
    "distance_from_sma_20_pct": 0.0309,
    "distance_from_sma_50_pct": 0.0826,
    "distance_from_sma_200_pct": 0.6067,
    "range_position_20_pct": 0.9624
  },
  "volume": {
    "avg_volume_20": 42150600.0,
    "volume_ratio_20": 1.2155,
    "volume_anomaly": false
  },
  "signals": {
    "is_breakout": false,
    "is_breakdown": false,
    "breakout_level": 447.115,
    "breakdown_level": 394.51125,
    "breakout_strength": -0.3633,
    "breakdown_strength": -5.9745,
    "above_sma20": true,
    "above_sma50": true,
    "above_sma200": true,
    "extension_from_mean": 1.6024,
    "volume_zscore": 1.02,
    "return_zscore": 0.84
  },
  "data_quality": {
    "bar_count": 252,
    "has_sufficient_history_20": true,
    "has_sufficient_history_50": true,
    "has_sufficient_history_200": true,
    "last_bar_timestamp": "2023-09-20T00:00:00Z",
    "last_sync_timestamp": "2023-09-21T12:10:58Z",
    "sync_status": "idle",
    "sync_error": null
  }
}
```

### Example events response

```json
{
  "ok": true,
  "symbol": "NVDA",
  "timeframe": "1Day",
  "as_of": "2023-09-20T00:00:00Z",
  "events": [
    {
      "event_type": "breakout",
      "timestamp": "2023-09-20T00:00:00Z",
      "value": 444.1,
      "reference_level": 442.99,
      "strength": 0.13,
      "confirmed_by_volume": true
    }
  ]
}
```

### Example future-state response

```json
{
  "ok": true,
  "symbol": "NVDA",
  "timeframe": "1Day",
  "as_of": "2023-09-20T00:00:00Z",
  "horizon_days": 20,
  "anchor_price": {
    "close": 444.1,
    "timestamp": "2023-09-20T00:00:00Z"
  },
  "window": {
    "target_end_timestamp": "2023-10-10T00:00:00Z",
    "realized_end_timestamp": "2023-10-10T00:00:00Z",
    "future_bar_count": 14
  },
  "future_state": {
    "max_price": 471.2,
    "max_price_timestamp": "2023-10-02T00:00:00Z",
    "max_return_pct": 0.061,
    "min_price": 430.4,
    "min_price_timestamp": "2023-09-25T00:00:00Z",
    "min_return_pct": -0.0308,
    "price_at_horizon": 468.7,
    "price_at_horizon_timestamp": "2023-10-10T00:00:00Z",
    "return_at_horizon_pct": 0.0554
  }
}
```

### Future-state rules

- `asof` is required and anchors the calculation on the latest stored bar at or before that timestamp
- `days` is required and defines the number of future bars to evaluate; for `1Day` this means trading days, not calendar days
- `max_price` uses the maximum future `high` inside the window
- `min_price` uses the minimum future `low` inside the window
- `price_at_horizon` uses the close of the Nth future bar in the realized window
- all `*_pct` values are relative to the anchor close at `asof`

### Assumptions

- Historical OHLCV bars already exist in PostgreSQL table `stock_bars`
- Symbol identity exists in `symbol_metadata`
- Sync freshness comes from `symbol_sync_state`
- The analytics layer reads stored bars only; it does not trigger new ingestion
- Timestamps are emitted as UTC ISO 8601 strings
- `distance_from_*_pct` and `range_position_20_pct` are percentage-style values, not raw dollar distances

### Known limits

- Initial tests cover analytics math and serialization, not live PostgreSQL integration
- Batch requests currently iterate symbols in-process; this is fine for a manageable watchlist-sized universe
- Cached analytics responses are in-memory per process and keyed by symbol, timeframe, `asof`, and last bar sync time

## Field Rules
- `symbol`: required
- `forecast_price`: required numeric
- `updated_date`: optional (`YYYY-MM-DD`, defaults to server date)
- `reward_score`: optional, 0 to 10 (default `5`)
- `risk_score`: optional, 0 to 10 (default `5`)
- `confidence_score`: optional, 0 to 10 (default `5`)
- `classification`: optional, one of `buy`, `hold/watch`, `sell` (default `hold/watch`)
- `opportunity_score`: computed output field only

## Notes
- `POST /api/stocks` upserts by `symbol` (case-insensitive, normalized to uppercase).
- `GET /api/stocks` returns only the latest stored row per stock, ordered by newest `updated_date` first.
- `GET /api/stocks?limit=N` limits the number of rows returned.
- `GET /chart/<symbol>` will try PostgreSQL first and only fetch from Alpaca automatically if no stored bars are available yet.
- Alpaca credentials are read from `APCA-API-KEY-ID` and `APCA-API-SECRET-KEY` in `.env`.
- If you are on Alpaca's free plan, set `CHART_DELAY=20` in `.env` so historical bar syncs stop 20 minutes behind real time and avoid recent SIP data 403 errors.
- In the UI, "Earnings calendar up to (date)" defaults to 4 calendar months in the future.
