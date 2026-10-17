# Alpaca Stock Charting Backend Design

This design assumes:

- We are building a stock-charting backend backed by PostgreSQL.
- Market data ingestion comes from Alpaca Market Data API.
- The frontend needs historical OHLCV chart data plus fast latest quote/latest bar reads.
- We prefer a shared-table design over one-table-per-symbol for operational simplicity and scale.

The current repository uses Flask in [`app.py`](/home/mpenniman/apollo/app.py), but the route sketch below is intentionally FastAPI-oriented because that is the better long-term fit for a typed service layer, async HTTP clients, and background workers.

## 1. Architecture Overview

### Core components

1. `api` service
   - FastAPI app serving symbol metadata, historical bars, latest quote, and latest bar endpoints.
   - Reads only from PostgreSQL.
   - Does not call Alpaca directly on hot user paths except as an optional cache-miss fallback.

2. `ingestion` worker
   - Pulls historical bars from Alpaca `GET https://data.alpaca.markets/v2/stocks/bars`.
   - Pulls latest quotes from `GET https://data.alpaca.markets/v2/stocks/quotes/latest`.
   - Pulls latest minute bars from `GET https://data.alpaca.markets/v2/stocks/bars/latest`.
   - Uses `APCA-API-KEY-ID` and `APCA-API-SECRET-KEY` headers for auth on Trading API market data requests.

3. PostgreSQL
   - Source of truth for symbols, bars, latest quote cache, latest bar cache, and sync state.
   - Shared-table design keyed by `symbol_id` + `timeframe` + `bar_time`.

4. Scheduler
   - Cron, Celery Beat, APScheduler, Temporal, or Kubernetes `CronJob`.
   - Runs backfills, incremental sync jobs, and stale-symbol repair jobs.

### Logical flow

1. Symbols are registered in `symbol_metadata`.
2. Worker chooses symbols needing sync based on `last_synced_at`, `latest_bar_time`, and backlog state.
3. Worker fetches Alpaca bars in batches by symbol list and timeframe window.
4. Bars are upserted into a shared `stock_bars` table.
5. Worker updates `symbol_sync_state` and optionally refreshes `latest_quotes` and `latest_bars`.
6. API serves chart queries from PostgreSQL with no vendor dependency on the request path.

### Why shared tables are preferred

- Better schema governance: one migration path, one query model, one retention policy.
- Better indexing and partitioning: PostgreSQL partitioning works naturally on time-based data.
- Easier fanout reads: multi-symbol screens, scanners, and watchlists are efficient.
- Lower operational overhead: no per-symbol DDL churn and no catalog bloat.

One-table-per-symbol is only justified if you need hard physical isolation per tenant or if your database technology specifically rewards that pattern. For a stock-charting site in PostgreSQL, shared tables are the right default.

## 2. PostgreSQL DDL

```sql
CREATE TABLE symbol_metadata (
    id BIGSERIAL PRIMARY KEY,
    symbol TEXT NOT NULL UNIQUE,
    exchange TEXT,
    asset_class TEXT NOT NULL DEFAULT 'us_equity',
    status TEXT NOT NULL DEFAULT 'active',
    name TEXT,
    currency TEXT NOT NULL DEFAULT 'USD',
    is_active BOOLEAN NOT NULL DEFAULT TRUE,
    first_seen_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CHECK (symbol = upper(symbol))
);

CREATE TABLE symbol_sync_state (
    symbol_id BIGINT PRIMARY KEY REFERENCES symbol_metadata(id) ON DELETE CASCADE,
    last_synced_at TIMESTAMPTZ,
    last_successful_sync_at TIMESTAMPTZ,
    last_quote_synced_at TIMESTAMPTZ,
    last_bar_synced_at TIMESTAMPTZ,
    latest_bar_time TIMESTAMPTZ,
    latest_quote_time TIMESTAMPTZ,
    sync_status TEXT NOT NULL DEFAULT 'idle',
    sync_error TEXT,
    sync_cursor JSONB,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CHECK (sync_status IN ('idle', 'running', 'error'))
);

CREATE TABLE stock_bars (
    symbol_id BIGINT NOT NULL REFERENCES symbol_metadata(id) ON DELETE CASCADE,
    timeframe TEXT NOT NULL,
    bar_time TIMESTAMPTZ NOT NULL,
    open NUMERIC(18, 8) NOT NULL,
    high NUMERIC(18, 8) NOT NULL,
    low NUMERIC(18, 8) NOT NULL,
    close NUMERIC(18, 8) NOT NULL,
    volume BIGINT NOT NULL,
    trade_count BIGINT,
    vwap NUMERIC(18, 8),
    source TEXT NOT NULL DEFAULT 'alpaca',
    ingested_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (symbol_id, timeframe, bar_time),
    CHECK (timeframe IN ('1Min', '5Min', '15Min', '1Hour', '1Day')),
    CHECK (high >= low),
    CHECK (open >= 0),
    CHECK (high >= 0),
    CHECK (low >= 0),
    CHECK (close >= 0),
    CHECK (volume >= 0)
) PARTITION BY RANGE (bar_time);

CREATE TABLE stock_bars_2026_01 PARTITION OF stock_bars
    FOR VALUES FROM ('2026-01-01 00:00:00+00') TO ('2026-02-01 00:00:00+00');

CREATE TABLE latest_quotes (
    symbol_id BIGINT PRIMARY KEY REFERENCES symbol_metadata(id) ON DELETE CASCADE,
    quote_time TIMESTAMPTZ NOT NULL,
    bid_price NUMERIC(18, 8),
    bid_size BIGINT,
    ask_price NUMERIC(18, 8),
    ask_size BIGINT,
    bid_exchange TEXT,
    ask_exchange TEXT,
    conditions JSONB,
    tape TEXT,
    source TEXT NOT NULL DEFAULT 'alpaca',
    ingested_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE latest_bars (
    symbol_id BIGINT PRIMARY KEY REFERENCES symbol_metadata(id) ON DELETE CASCADE,
    timeframe TEXT NOT NULL DEFAULT '1Min',
    bar_time TIMESTAMPTZ NOT NULL,
    open NUMERIC(18, 8) NOT NULL,
    high NUMERIC(18, 8) NOT NULL,
    low NUMERIC(18, 8) NOT NULL,
    close NUMERIC(18, 8) NOT NULL,
    volume BIGINT NOT NULL,
    trade_count BIGINT,
    vwap NUMERIC(18, 8),
    source TEXT NOT NULL DEFAULT 'alpaca',
    ingested_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CHECK (timeframe = '1Min')
);

CREATE INDEX idx_symbol_metadata_active_symbol
    ON symbol_metadata (is_active, symbol);

CREATE INDEX idx_stock_bars_symbol_tf_time_desc
    ON stock_bars (symbol_id, timeframe, bar_time DESC);

CREATE INDEX idx_stock_bars_tf_time_symbol
    ON stock_bars (timeframe, bar_time DESC, symbol_id);

CREATE INDEX idx_latest_quotes_quote_time
    ON latest_quotes (quote_time DESC);

CREATE INDEX idx_latest_bars_bar_time
    ON latest_bars (bar_time DESC);
```

### Notes on DDL choices

- `symbol_metadata` holds durable symbol identity and business metadata.
- `symbol_sync_state` holds `last_synced_at` and worker state without polluting metadata.
- `stock_bars` stores immutable historical bars in a shared fact table.
- `latest_quotes` and `latest_bars` are cache tables optimized for ultra-fast point reads.
- `NUMERIC(18,8)` is conservative for price precision; if throughput is more important than decimal exactness, `DOUBLE PRECISION` is acceptable.

### Partitioning guidance

- Partition `stock_bars` by month on `bar_time` for manageable index size and retention operations.
- Pre-create upcoming partitions with automation.
- If data volume becomes very large, consider subpartitioning by hash of `symbol_id` inside monthly partitions, but only after measurement.

## 3. Ingestion / Sync Design

### Historical bars sync

Use Alpaca historical bars endpoint:

- `GET https://data.alpaca.markets/v2/stocks/bars`
- Send `APCA-API-KEY-ID` and `APCA-API-SECRET-KEY` headers.
- Batch symbols where practical.
- Handle pagination via `next_page_token`.

Important Alpaca behavior:

- Results are sorted by symbol first, then by bar timestamp.
- With multi-symbol queries, one symbol can dominate the page until its records are exhausted.
- The worker must keep following `next_page_token` until completion.

### Suggested sync algorithm

1. Select eligible symbols from `symbol_metadata` joined to `symbol_sync_state`.
2. For each symbol, compute `start`:
   - `latest_bar_time + 1 interval unit` if previously synced.
   - Else configured backfill start date.
3. Compute `end` as current UTC time minus a small safety lag.
4. Request bars from Alpaca for a timeframe such as `1Min` or `1Day`.
5. Upsert bars into `stock_bars`.
6. Update `latest_bar_time`, `last_bar_synced_at`, `last_successful_sync_at`, and `last_synced_at`.
7. Store any pagination or recovery state in `sync_cursor` if the job is interrupted.

### Latest quote and latest bar refresh

Use separate lightweight jobs:

- Quotes: `GET https://data.alpaca.markets/v2/stocks/quotes/latest`
- Latest minute bars: `GET https://data.alpaca.markets/v2/stocks/bars/latest`

These jobs should:

1. Pull symbols in batches.
2. Upsert into `latest_quotes` and `latest_bars`.
3. Update `latest_quote_time`, `latest_bar_time`, and `last_synced_at` in `symbol_sync_state`.

### Failure handling

- Mark `sync_status='running'` at job start and clear on success.
- On failure, set `sync_status='error'` and store a compact `sync_error`.
- Retry transient HTTP 429/5xx errors with exponential backoff and jitter.
- Make sync idempotent through primary-key upserts.

### Concurrency model

- Parallelize by symbol batches, not by one request per row.
- Use advisory locks or a leasing column pattern so two workers do not sync the same symbol/timeframe window concurrently.
- Keep write transactions small, for example 500 to 5,000 bars per upsert batch.

### Minimal Python sync shape

```python
async def sync_symbol_bars(symbol_id: int, symbol: str, timeframe: str) -> None:
    state = await repo.get_sync_state(symbol_id)
    start = compute_next_start(state.latest_bar_time, timeframe)
    end = utcnow() - SAFETY_LAG

    page_token = None
    while True:
        response = await alpaca_client.get_stock_bars(
            symbols=[symbol],
            timeframe=timeframe,
            start=start,
            end=end,
            page_token=page_token,
        )
        bars = normalize_bars(response, symbol_id, timeframe)
        await repo.upsert_bars(bars)

        page_token = response.get("next_page_token")
        if not page_token:
            break

    await repo.mark_symbol_synced(symbol_id=symbol_id, timeframe=timeframe)
```

## 4. FastAPI Route Sketch

```python
from datetime import datetime
from fastapi import FastAPI, Depends, HTTPException, Query
from pydantic import BaseModel

app = FastAPI()


class BarOut(BaseModel):
    t: datetime
    o: float
    h: float
    l: float
    c: float
    v: int


class QuoteOut(BaseModel):
    t: datetime
    bid_price: float | None
    bid_size: int | None
    ask_price: float | None
    ask_size: int | None


@app.get("/v1/symbols")
async def list_symbols(search: str | None = None, active_only: bool = True):
    ...


@app.get("/v1/symbols/{symbol}")
async def get_symbol(symbol: str):
    ...


@app.get("/v1/symbols/{symbol}/bars", response_model=list[BarOut])
async def get_bars(
    symbol: str,
    timeframe: str = Query(..., pattern="^(1Min|5Min|15Min|1Hour|1Day)$"),
    start: datetime | None = None,
    end: datetime | None = None,
    limit: int = Query(500, ge=1, le=10000),
):
    # Resolve symbol_id, then read from stock_bars ordered by bar_time asc.
    ...


@app.get("/v1/symbols/{symbol}/bars/latest", response_model=BarOut)
async def get_latest_bar(symbol: str):
    # Read from latest_bars first, optionally fall back to max(bar_time) in stock_bars.
    ...


@app.get("/v1/symbols/{symbol}/quotes/latest", response_model=QuoteOut)
async def get_latest_quote(symbol: str):
    # Read from latest_quotes.
    ...


@app.post("/internal/sync/symbols/{symbol}/bars")
async def trigger_symbol_backfill(
    symbol: str,
    timeframe: str,
    start: datetime | None = None,
    end: datetime | None = None,
):
    # Internal/admin route or enqueue job only.
    ...
```

### API behavior guidance

- Public chart routes should read from PostgreSQL only.
- Internal sync routes should enqueue work, not perform long syncs inline.
- Return timestamps in UTC ISO 8601.
- Normalize symbols to uppercase at the API boundary.

## 5. Indexing and Upsert Strategy

### Bar upsert

Use PostgreSQL `ON CONFLICT` on `(symbol_id, timeframe, bar_time)`.

```sql
INSERT INTO stock_bars (
    symbol_id, timeframe, bar_time, open, high, low, close, volume, trade_count, vwap, source
)
VALUES
    ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, 'alpaca')
ON CONFLICT (symbol_id, timeframe, bar_time)
DO UPDATE SET
    open = EXCLUDED.open,
    high = EXCLUDED.high,
    low = EXCLUDED.low,
    close = EXCLUDED.close,
    volume = EXCLUDED.volume,
    trade_count = EXCLUDED.trade_count,
    vwap = EXCLUDED.vwap,
    source = EXCLUDED.source,
    ingested_at = now();
```

### Latest quote upsert

Use `symbol_id` as the primary key.

```sql
INSERT INTO latest_quotes (
    symbol_id, quote_time, bid_price, bid_size, ask_price, ask_size,
    bid_exchange, ask_exchange, conditions, tape, source
)
VALUES (...)
ON CONFLICT (symbol_id)
DO UPDATE SET
    quote_time = EXCLUDED.quote_time,
    bid_price = EXCLUDED.bid_price,
    bid_size = EXCLUDED.bid_size,
    ask_price = EXCLUDED.ask_price,
    ask_size = EXCLUDED.ask_size,
    bid_exchange = EXCLUDED.bid_exchange,
    ask_exchange = EXCLUDED.ask_exchange,
    conditions = EXCLUDED.conditions,
    tape = EXCLUDED.tape,
    source = EXCLUDED.source,
    ingested_at = now()
WHERE latest_quotes.quote_time <= EXCLUDED.quote_time;
```

### Latest bar upsert

Use the same pattern for `latest_bars` with a stale-write guard:

```sql
...
WHERE latest_bars.bar_time <= EXCLUDED.bar_time;
```

### Query patterns supported by indexes

- Single-symbol chart lookup:
  - `WHERE symbol_id = ? AND timeframe = ? AND bar_time BETWEEN ? AND ? ORDER BY bar_time`
- Latest available stored bar:
  - `WHERE symbol_id = ? AND timeframe = ? ORDER BY bar_time DESC LIMIT 1`
- Multi-symbol latest cache screens:
  - joins from `symbol_metadata` to `latest_quotes` or `latest_bars`

## 6. Recommendation With Tradeoffs

### Recommended design

Use:

- `symbol_metadata` for durable symbol definitions.
- `symbol_sync_state` for `last_synced_at` and worker bookkeeping.
- A shared partitioned `stock_bars` table for historical OHLCV.
- Dedicated `latest_quotes` and `latest_bars` tables for fast point reads.
- FastAPI for typed read APIs and a separate worker for ingestion.

This is the most balanced design for a charting site because it keeps reads simple, sync jobs idempotent, and future scaling options open.

### Tradeoffs

#### Shared bars table

Pros:

- Operationally simple.
- Easy to add symbols.
- Efficient for watchlists and scanners.
- Works well with partitioning and batch upserts.

Cons:

- Needs careful index management at large scale.
- Requires partition lifecycle automation once data grows.

#### Separate latest cache tables

Pros:

- Fast retrieval for homepage/watchlist/current quote views.
- Keeps hot reads off the large historical fact table.

Cons:

- Slightly more ingestion complexity because latest state is duplicated from historical facts.

#### Storing `last_synced_at` in separate sync table

Pros:

- Cleaner domain model.
- Easier to track worker state, retries, and errors.
- Avoids mixing operational state into reference metadata.

Cons:

- Slightly more joins.

If you want the absolute simplest schema, `last_synced_at` can live directly on `symbol_metadata`, but for a production ingestion pipeline I recommend the dedicated `symbol_sync_state` table.

### Final recommendation

For this project, I would ship:

1. A shared, partitioned `stock_bars` table keyed by `(symbol_id, timeframe, bar_time)`.
2. `symbol_metadata` plus `symbol_sync_state`.
3. `latest_quotes` and `latest_bars` cache tables.
4. Background incremental sync from Alpaca with idempotent upserts.
5. FastAPI read endpoints backed entirely by PostgreSQL.

This gives you a design that is simple enough to build now and strong enough to scale to thousands of symbols and large historical windows without revisiting the core data model.

## References

- Alpaca historical stock bars: https://docs.alpaca.markets/reference/stockbars
- Alpaca latest stock quotes: https://docs.alpaca.markets/reference/stocklatestquotes-1
- Alpaca latest stock bars: https://docs.alpaca.markets/reference/stocklatestbars-1
- Alpaca authentication: https://docs.alpaca.markets/docs/authentication
