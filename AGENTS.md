# AGENTS.md

Guidance for coding agents working in this repository.

## Project Overview

This is a single-file Flask stock forecast tracker with a PostgreSQL backend and an analytics package under `analytics/`.

Core files:

- `app.py`: Flask app, database initialization, web UI, API routes, chart sync, and API docs.
- `analytics/service.py`: service layer for symbol state, events, future-state labels, and batch analytics.
- `analytics/data_access.py`: PostgreSQL access for symbol metadata and OHLCV bars.
- `analytics/indicators.py`: technical indicator feature frame generation.
- `analytics/signals.py`: signal columns and event generation.
- `tests/test_analytics.py`: analytics and endpoint tests.

## Local Commands

Install dependencies in an isolated environment when the system Python is externally managed:

```bash
python -m venv /tmp/opencode/apollo-venv
/tmp/opencode/apollo-venv/bin/python -m pip install -r requirements.txt
```

Run the analytics tests:

```bash
/tmp/opencode/apollo-venv/bin/python -m unittest tests/test_analytics.py
```

Run the app locally:

```bash
python app.py
```

Run with Docker:

```bash
docker compose up --build -d
```

## Analytics API Notes

The analytics endpoints are designed for downstream automation and model-training workflows. They use already-synced OHLCV bars in PostgreSQL.

Single-symbol technical state:

```bash
curl -s "http://127.0.0.1:5000/api/state/NVDA?timeframe=1D&asof=2023-09-20"
```

Batch technical state for submitted symbols:

```bash
curl -s http://127.0.0.1:5000/api/state/batch \
  -H "Content-Type: application/json" \
  -d '{"symbols":["NVDA","AAPL","MSFT"],"timeframe":"1D","asof":"2023-09-20"}'
```

Portfolio technical state for saved forecast rows:

```bash
curl -s "http://127.0.0.1:5000/api/portfolio/state?timeframe=1D&asof=2023-09-20"
```

Single-symbol realized future labels:

```bash
curl -s "http://127.0.0.1:5000/api/futurestate/NVDA?timeframe=1D&asof=2023-09-20&days=20"
```

Portfolio realized future labels for saved forecast rows:

```bash
curl -s "http://127.0.0.1:5000/api/portfolio/futurestate?timeframe=1D&asof=2023-09-20&days=20"
```

Useful portfolio query parameters:

- `include_hidden=1`: include hidden portfolio rows. Defaults to visible rows only.
- `hidets=true`: remove symbol and timestamp-like fields from responses.
- `timeframe`: accepts `1D`, `1Day`, `1Hour`, `15Min`, `5Min`, or `1Min`.
- `asof`: accepts `YYYY-MM-DD` or ISO 8601 timestamps.
- `days`: required by future-state endpoints; number of future bars to evaluate.

## Portfolio Endpoint Behavior

`GET /api/portfolio/state` loads symbols from stored forecast rows via `_portfolio_symbols()` and returns:

- `states`: successful per-symbol technical payloads.
- `errors`: per-symbol failures, such as missing bars or missing metadata.
- `portfolio_symbol_count`: number of symbols requested from the portfolio.
- `count`: number of successful state payloads.

`GET /api/portfolio/futurestate` uses the same portfolio symbol source and returns:

- `future_states`: successful per-symbol realized future-window payloads.
- `errors`: per-symbol failures, such as insufficient future bars.
- `horizon_days`: requested future-bar horizon.
- `side`: `long` or `short` threshold interpretation.
- `portfolio_symbol_count`: number of symbols requested from the portfolio.
- `count`: number of successful future-state payloads.

For `1Day`, `days` means future trading bars, not calendar days.

## Model-Training Workflow

To produce a historical training sample for selecting the top stocks over a future horizon:

1. Call `/api/portfolio/state?asof=<date>&timeframe=1D` to collect features.
2. Call `/api/portfolio/futurestate?asof=<date>&timeframe=1D&days=<horizon>` to collect labels.
3. Join responses by `symbol`.
4. Use `future_state.return_at_horizon_pct`, `max_return_pct`, or risk-adjusted variants as labels depending on the model objective.

## Development Guidelines

- Keep API route behavior compatible with the documented `/api-docs` page in `app.py`.
- Prefer adding analytics business logic to `analytics/service.py` rather than duplicating it in routes.
- Keep route handlers responsible for parsing input, syncing data when needed, calling the service, and shaping HTTP errors.
- Preserve per-symbol error collection for batch and portfolio endpoints so one bad symbol does not fail the whole response.
- Add or update tests in `tests/test_analytics.py` for endpoint behavior and service-layer additions.
