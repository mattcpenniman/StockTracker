# Stock Forecast Tracker

Single-file Flask app for tracking stock forecast prices in a CSV-backed store.

## Features
- Add or update stock forecasts from the web UI
- Persist data in `stocks.csv`
- Fetch live prices with `yfinance` for dashboard comparison
- Export CSV data
- Update earnings data into `earnings.csv`
- External API for reading/updating the CSV "database"
- Built-in API documentation page in the web UI

## Requirements
- Python 3.10+
- pip

## Install
```bash
pip install flask yfinance pandas python-dotenv
```

## Run
```bash
python app.py
```

Default server URL:
- `http://127.0.0.1:5000`

Optional environment variables:
- `HOST` (default: `127.0.0.1`)
- `PORT` (default: `5000`)
- `STOCK_TRACKER_CSV` (default: `stocks.csv`)
- `EARNINGS_CSV` (default: `earnings.csv`)
- `FMP_API_KEY` (for earnings update route)

## Routes
- `GET /` - web UI
- `GET /api-docs` - API documentation page
- `POST /add` - add/update one forecast row from UI JSON
- `GET /data` - UI table data with live prices and earnings enrichment
- `GET /api/stocks` - raw stored rows from CSV (script-friendly)
- `POST /api/stocks` - upsert one or many rows into CSV
- `GET /export` - download CSV
- `GET /health` - health check
- `POST /update-earnings` - refresh `earnings.csv`

## External Script API

### Get raw stored rows
```bash
curl -s http://127.0.0.1:5000/api/stocks
```

### Get a single stock by symbol
```bash
curl -s "http://127.0.0.1:5000/api/stocks?symbol=AAPL"
```

Example response:
```json
{
  "ok": true,
  "rows": [
    {"symbol": "AAPL", "forecast_price": 210.5, "updated_date": "2026-03-07"}
  ],
  "count": 1
}
```

### Upsert one row
```bash
curl -X POST http://127.0.0.1:5000/api/stocks \
  -H "Content-Type: application/json" \
  -d '{"symbol":"AAPL","forecast_price":210.5,"updated_date":"2026-03-07"}'
```

### Upsert many rows
```bash
curl -X POST http://127.0.0.1:5000/api/stocks \
  -H "Content-Type: application/json" \
  -d '{"rows":[{"symbol":"AAPL","forecast_price":210.5},{"symbol":"MSFT","forecast_price":480}]}'
```

## Notes
- `POST /api/stocks` upserts by `symbol` (case-insensitive, normalized to uppercase).
- If `updated_date` is omitted, the server uses today’s date.
- `GET /api/stocks` returns only stored values, not live market values.
- `GET /api/stocks?symbol=...` filters to one symbol.
- In the UI, "Earnings calendar up to (date)" defaults to 4 calendar months in the future.
