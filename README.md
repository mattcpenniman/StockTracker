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
