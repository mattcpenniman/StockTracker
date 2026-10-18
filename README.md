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

### Rust analytics acceleration

The state and event APIs can calculate their indicator feature frame in a fused
Rust extension (PyO3 + NumPy). Install with Rust/Cargo 1.83+ and a C linker available:

```bash
pip install ./rust
```

Docker builds and installs the release extension automatically in a separate
build stage. The runtime image does not need the Rust toolchain.

`ANALYTICS_BACKEND=auto` (default) uses Rust when installed, otherwise pandas.
Use `ANALYTICS_BACKEND=python` to force the reference implementation or
`ANALYTICS_BACKEND=rust` to require the extension at calculation time. Histories
with missing/nonfinite OHLCV values or nonpositive closes use pandas to preserve
its existing missing-data behavior. Restart the app after changing backends to
clear its in-process state cache.

All `/api/state` URL forms and parameters retain their existing meaning, including
`asof`, `timeframe`, `hidets`, `breakout_lookback`, and `buffer_pct`. Native code
calculates returns, moving averages, RSI, MACD, ATR, volatility, ranges, volume
statistics, and breakout levels; signal classification still runs in Python.
The native calculation releases the GIL. Database queries, market-data syncing,
HTTP serialization, and cache hits are outside this acceleration.

Validate parity and measure uncached feature + signal calculation locally:

```bash
python -m unittest tests/test_analytics.py
python -m analytics.benchmark --bars 260 1000 10000 --repeats 20
python -m analytics.benchmark --bars 1000 --repeats 20 --http
```

The `--http` comparison exercises all five documented single-symbol state URL
examples through Flask with a fixture repository and a fresh cache per request.
It validates JSON parity and includes route/service/serialization overhead, while
mocking database and market-data network calls; it is not a live-server benchmark.

### PostgreSQL connection lifecycle (VEC-785)

App and analytics database blocks share a lazy connection pool per database URL
and worker process. `DB_POOL_SIZE=4` bounds open connections in each pool;
`DB_POOL_TIMEOUT_SECONDS=30` controls checkout waiting and connection establishment.
Budget worker count × pool size against PostgreSQL's connection limit, plus other
clients. Batch analytics reads remain sequential and do not reserve extra slots.

Successful blocks commit; failed blocks roll back before returning a connection.
Transport failures discard the connection. Reused connections are checked before
checkout; failed checks create a replacement without replaying application SQL.
Connections are not held during analytics computation or provider calls.

Pools are created on first use, including in preloaded worker deployments. Forked
children discard inherited socket descriptors and initialize fresh pools while
the parent keeps its connections. `database.close_pools()` is an explicit worker
shutdown hook and is also registered with `atexit`; idle connections close
immediately and checked-out connections close when returned. Do not attach pool
shutdown to Flask request teardown, which would defeat reuse.

Validate lifecycle/concurrency against real PostgreSQL and reproduce endpoint
measurements (private schemas; no market-data provider calls):

```bash
export ANALYTICS_TEST_DATABASE_URL='postgresql://user:password@localhost:5432/test_database'
python -m unittest tests.test_database
python -m analytics.pool_benchmark --repeats 10 --bars 5000 --load-requests 80
```

The benchmark compares the old direct-connect factory with pooling using identical
SQL, routes, and full JSON payloads. It reports connection creations, sampled
active/total sessions, post-workload transaction state, throughput and median/p95
latency, including warm caches and eight concurrent callers. Measurement evidence
is maintained in Vector under VEC-785.

### API request overhead (VEC-805)

Database schema initialization runs once per worker/database configuration, after
a successful commit. Concurrent requests share initialization; failures are retried.
Restart workers after externally replacing their schema. Forecast and market-data
reads still query PostgreSQL on each request as before.

Events reuse their feature-series timestamp or bounded latest-bar reads to retain
the state's price-anchor policy without calculating an entire second state.
Indicator history, Rust/Python dispatch and numerical formulas are preserved.

Reproduce live HTTP probes and exact-response before/after measurements:

```bash
python -m analytics.api_probe --base-url http://192.168.1.80:5005 --repeats 5
export ANALYTICS_TEST_DATABASE_URL='postgresql://user:password@host/database'
python -m unittest discover -s tests
ANALYTICS_BACKEND=rust python -m analytics.api_performance_benchmark --repeats 20
```

The live probe reads stored data and checks invalid mutation inputs; chart latest
can perform its normal freshness sync. Successful mutation tests use a private
schema and deterministic provider data. The performance benchmark also uses a
private schema, prohibits provider calls, and compares the pinned `1c65793` app
and service with exact full-JSON equality on each alternating pair. Event requests
are cold; other measured requests are warm. Full timings, query counts, methodology
and deployed-baseline results are maintained in Vector: `vector doc show DOC-223`.

### Portfolio snapshot overhead (VEC-806)

Portfolio/state snapshots calculate indicators over full history, but use only
the latest row and the existing scalar anchor-signal calculations for the output.
They do not build historical signal columns that are then discarded. Event
endpoints still calculate their complete event history.

Analytics bar queries transport timestamps as exact integer epoch microseconds,
then convert them to UTC in pandas. This avoids a Python datetime allocation per
database row without changing timestamp precision, query cutoffs, or ordering.

To compare actual portfolio HTTP latency between two single-process serving
instances backed by the same data:

```bash
python -m analytics.portfolio_endpoint_benchmark \
  --before-url http://127.0.0.1:5007 --after-url http://192.168.1.80:5005 \
  --repeats 20 --output /tmp/portfolio-results.json
```

Use `--asof current` to measure the current portfolio (normal freshness sync can
occur). The probe alternates instance order, compares exact full-response hashes,
and verifies Rust-call deltas for both cold and warm requests. One warmup pair is
excluded. Distinct `event_limit` settings force cold state-cache keys without
changing state math. This probe assumes exclusive benchmark traffic and one
serving process per instance so health counters refer to the measured worker.

### Bounded analytics price reads (VEC-780)

Price anchors use an inclusive latest-at-or-before query (`ORDER BY bar_time DESC
LIMIT 1`). Future labels read only the next requested trading bars, strictly
after the anchor, in ascending time order. State indicators retain their full
daily history for EMA/RSI initialization and YTD parity.

Run PostgreSQL parity tests and the database-backed benchmark with a database
account allowed to create schemas. Each run creates and removes a private schema
using the app's production DDL and indexes:

```bash
export ANALYTICS_TEST_DATABASE_URL='postgresql://user:password@localhost:5432/test_database'
python -m unittest tests/test_analytics.py tests/test_price_reads.py
ANALYTICS_BACKEND=python python -m analytics.io_benchmark --repeats 20
```

Local measurement on 2026-10-17: PostgreSQL 16 in Docker, Python 3.12/pandas
backend, 10,000 daily + 200,000 intraday bars for one synthetic symbol. Requests
use real Flask routes and PostgreSQL connections, fresh service caches, warm
database/OS caches, and mocked market-data sync. One warmup pair is excluded;
20 measured before/after pairs alternate order and assert identical JSON.
The before adapter reproduces the prior full-history reads and Python-side
window truncation. These are local measurements, not live-server results.

| Endpoint / anchor | Queries before → after | Rows transferred before → after | Median ms before → after | p95 ms before → after |
| --- | --- | --- | --- | --- |
| State, daily midnight | 2 → 2 | 8,348 → 8,348 | 214.13 → 219.61 | 228.99 → 267.50 |
| State, 15Min | 3 → 3 | 108,349 → 8,349 | 1,014.70 → 230.04 | 1,078.56 → 273.12 |
| Future state, daily, 20 bars | 3 → 3 | 10,001 → 22 | 172.77 → 84.21 | 203.65 → 90.13 |
| Future state, 15Min, 20 bars, short | 3 → 3 | 200,001 → 22 | 1,516.65 → 79.55 | 1,642.08 → 85.68 |

Rows include the single symbol-context row. Daily-only state retains the same
query workload; its timing variation is not evidence of an optimization.
Anchors are `2022-11-07T00:00:00Z` (daily) and `2022-11-07T16:00:00Z`
(intraday). Date-only API input still means end-of-day UTC and therefore selects
15-minute price anchors under the existing rules.

`EXPLAIN (ANALYZE, BUFFERS)` of the actual bounded repository queries showed:

- Latest: `Limit → Index Scan Backward using stock_bars_pkey`, one actual row,
  seven shared buffer hits, execution time 0.202 ms.
- Forward: `Limit → Index Scan using stock_bars_pkey`, 20 actual rows,
  five shared buffer hits, execution time 0.090 ms.

Both index conditions constrain `symbol_id`, `timeframe`, and `bar_time`, with
no sort or sequential scan. PostgreSQL chose the existing composite primary-key
index over the equivalent `idx_stock_bars_symbol_tf_time` index. The benchmark
prints complete plans, including planning times, on each run.

### Bulk batch and portfolio analytics (VEC-784)

Batch requests load symbol metadata and sync timestamps once, including SPY/QQQ
for state enrichment. A request-local repository view shares those contexts and
lazy bar reads without changing the shared service's repository. Cache hits skip
bar reads; cache misses group full daily histories by ten symbols and bounded
price anchors/future windows by up to 100 symbols. Daily indicator history is
not truncated. SQL limits and strict/inclusive time boundaries apply separately
to each symbol, including symbols with different anchor timestamps.

Current batch/portfolio state routes check freshness with bulk latest-bar reads
and the same freshness policy as single-symbol routes. Successful sync results
supply updated cache-invalidation timestamps before analytics are read. Newly
created symbols load their new metadata after sync. Benchmark auto-sync behavior,
requested ordering, duplicate entries, per-symbol errors, hidden-row filtering,
portfolio ranks, and benchmark-relative returns retain their existing semantics.
All database work remains sequential through `AnalyticsRepository.get_conn`;
connection pooling is tracked separately in VEC-785.

Reproduce PostgreSQL payload parity and 10/50/100-symbol cold/warm measurements:

```bash
export ANALYTICS_TEST_DATABASE_URL='postgresql://user:password@localhost:5432/test_database'
python -m unittest tests.test_batch_reads
python -m analytics.batch_benchmark --repeats 10 --bars 5000
```

The benchmark loads pre-change service/routes from git ref `7e9e230`, creates a
private schema using production DDL/indexes, and asserts exact full JSON parity
for every measured pair. It uses real Flask requests and PostgreSQL connections,
warm database/OS caches, and alternating before/after order with one excluded
warmup pair. Cold/warm refer to service caches; schema-init work and provider
network calls are excluded. Median/p95 timings, SQL counts, and transferred rows
are printed for historical batch state, current portfolio state, and daily and
intraday portfolio future labels. Missing intraday anchors remain independent
per-symbol failures and are retried on warm requests.

Measured 100-symbol medians on PostgreSQL 16.13 with the native indicator backend:
historical batch state **10,867 → 6,806 ms cold / 2,469 → 70 ms warm**;
current portfolio state **16,856 → 7,234 ms cold / 8,384 → 356 ms warm**;
daily portfolio future labels **7,587 → 677 ms cold / 2,435 → 61 ms warm**.
Full 10/50/100-symbol median/p95 timings, query/row counts, parity coverage and
measurement conditions are in Vector: `vector doc show DOC-215`
("VEC-784 bulk analytics query and latency evidence").

## Run (Local)
```bash
export DATABASE_URL='postgresql://stock_user:stock_pass@localhost:5432/stock_tracker'
python app.py
```

### Dashboard live-price freshness (VEC-782)

`/data` uses Yahoo's multi-symbol quote endpoint (up to 100 symbols/request)
through yfinance's authenticated transport. Missing symbols or an unsupported
batch endpoint fall back to at most eight concurrent Yahoo chart-metadata
requests. Both paths use `regularMarketPrice`, the latest regular-market price,
not a bid/ask midpoint, extended-hours price, or adjusted historical close.
The old historical-close fallback is no longer substituted into `current_price`.
Prices can represent the previous session on weekends/holidays or delayed feeds;
cache freshness means time since retrieval, not time since the market traded.

Configuration (seconds; finite positive values, restart to apply):

| Variable | Default | Meaning |
| --- | --- | --- |
| `LIVE_PRICE_CACHE_SECONDS` | 15 | Successful quote freshness window |
| `LIVE_PRICE_RETRY_SECONDS` | 3 | Negative-cache duration after missing/failed quotes |
| `LIVE_PRICE_PROVIDER_TIMEOUT_SECONDS` | 3 | Timeout passed to each yfinance transport call |
| `LIVE_PRICE_WAIT_SECONDS` | 5 | Maximum quote wait per dashboard read |

Fresh reads make no provider calls. Expired entries refresh on demand. One
refresh runs per process, and concurrent/overlapping reads join it. A slow
refresh may finish after a caller's wait deadline; subsequent callers still
join it, with no queued duplicate refresh. Healthy quotes are published as they
arrive, independently of missing/slow symbols. Transport authentication/retries
can exceed a single socket timeout, but do not extend the dashboard's quote wait.
After the deadline or refresh failure, unavailable/expired prices are `null`;
expired values are never served as current. No scheduled refresh runs when idle.

The existing per-symbol `diff_dollar`/`diff_pct` null behavior and MA-distance
historical-close fallback remain in place. No historical fallback is labeled
`current_price`. The cache is process-local (current Docker/local runtime runs
one Flask process); multiple processes/replicas each have their own cache and
refresh limit. Expired entries are pruned on refresh completion.

Validation and reproducible database-backed `/data` benchmark:

```bash
python -m unittest discover -s tests
ANALYTICS_TEST_DATABASE_URL='postgresql://user:password@host/db' python -m analytics.live_price_benchmark --repeats 20
```

The benchmark uses real PostgreSQL/Flask and controlled provider latency, counts
quote requests, checks full JSON parity, and reports market quote age separately
from retrieval freshness. It includes cold/warm reads and a slow-provider case.
Recorded results and live transport evidence are in Vector document **DOC-214**
(`vector doc show DOC-214`). At 100 symbols with controlled 10 ms/provider-call
latency, median `/data` time fell from 1,488 ms to 451 ms cold / 458 ms warm;
modeled quote requests fell from 100/read to one cold / zero warm.

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
curl -s "http://127.0.0.1:5000/api/state/NVDA?timeframe=15Min&asof=2023-09-20T15:45:00Z"
curl -s "http://127.0.0.1:5000/api/state/NVDA?timeframe=1D&asof=2023-09-20&hidets=true"
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

### Runtime identity and release verification

Both `/health` and `/api/health/analytics` add a `runtime` object while retaining
their existing database fields. Startup logs report the same runtime identity.
It includes artifact `build.revision`/`build.release_id`, `configured_backend`,
`extension_available`/`extension_version`, `effective_backend` for eligible finite
data, and `execution.observed_backend` (`not_yet_invoked`, `python`, `rust`, or
`mixed`). Installing a wheel alone does not count as invoking it.

`execution.native_calls`, `python_fallback_calls`, and `fallback_reasons` count
completed dispatches in **one process since startup**, with its PID. They exclude
cache hits and synthetic verification. Multiple workers have separate counters;
these are diagnostics rather than a durable metrics store. Fallback reasons are
`forced_python`, `extension_unavailable`, `empty_input`, and `ineligible_data`
(nonfinite input, missing timestamps, or nonpositive close). No symbols, prices,
credentials, or environment dumps are included.

`ANALYTICS_BACKEND=auto` uses Rust when available and otherwise supports Python.
`python` intentionally selects the reference implementation. `rust` requires an
available extension: health/readiness returns 503 if the DB is connected but Rust
is unavailable. Intentional unusual-data fallback remains supported even in Rust
mode. Invalid backend configuration also fails readiness.

Embed the exact clean source revision and release identifier when building:

```bash
BUILD_REVISION=$(git rev-parse HEAD) RELEASE_ID=release-2026-10-17 docker compose build app
```

Docker writes `/app/build-info.json` after copying source and attaches matching
OCI revision/version labels. Runtime identity reads that artifact only, never
host git or runtime `BUILD_REVISION`/`RELEASE_ID` overrides. Build inputs must name
the clean source used to build the image. Local images without supplied arguments
report `unknown`; default release verification rejects them.

The final Docker stage runs a Rust smoke check after installing the wheel. After
deployment, verify the **running container**, supplying the intended release values
from your release record (not from a possibly newer host checkout):

```bash
docker compose exec -T app python -m analytics.release_check \
  --require-rust --expected-revision FULL_COMMIT_SHA --expected-release release-2026-10-17
curl -s http://127.0.0.1:5000/api/health/analytics
```

The CLI emits JSON and exits nonzero for missing/mismatched artifact identity,
failed/nonfinite computation, or missing native invocation. It calculates 320
synthetic finite bars through the public dispatcher in that runtime, bypassing
state caches; it does not contact a provider or database, write market data, or
change runtime configuration. Its isolated `smoke.native_calls` proves execution
for that calculation; HTTP counters independently describe serving-process work.
Use `--allow-unversioned` only for development, and omit `--require-rust` to verify
intentional Python reference mode. An older deployment without these additions
will lack `runtime` and the CLI: treat that as **unverified/stale**, not Rust-ready.

Kernel coverage: single-symbol state, batch state, portfolio state, and events
calculate indicator features on cache misses. `futurestate` and portfolio
`futurestate` use price-window labels and do **not** require the indicator kernel.

### Validated Docker rollout (VEC-802)

Compose requires Rust by default (`ANALYTICS_BACKEND=rust`); override explicitly
for intentional Python deployments. `APP_IMAGE` selects a versioned local or
registry image, defaulting to `apollo-app`. Keep the release values with the
deployment record and supply the same `APP_IMAGE` to subsequent Compose commands.

Build from an exact tracked revision, excluding uncommitted host changes:

```bash
set -euo pipefail
export BUILD_REVISION=$(git rev-parse origin/master)
export RELEASE_ID=release-YYYYMMDD-N
export APP_IMAGE=apollo-app:$RELEASE_ID
export ANALYTICS_BACKEND=rust
git archive "$BUILD_REVISION" | docker build \
  --build-arg BUILD_REVISION="$BUILD_REVISION" --build-arg RELEASE_ID="$RELEASE_ID" \
  -t "$APP_IMAGE" -
docker compose run --rm --no-deps app python -m analytics.release_check \
  --require-rust --expected-revision "$BUILD_REVISION" --expected-release "$RELEASE_ID"
```

Record the old app image ID and PostgreSQL container/volume identity. Retain a
rollback image tag and a database backup (`pg_dump -Fc`) before rollout. Apply the
additive coverage schema using the app's supported initializer, then replace only
the app; PostgreSQL and its existing volume stay running:

```bash
docker compose run --rm --no-deps app python -c 'import app; app._init_db_if_needed()'
docker compose up -d --no-build --no-deps app
docker compose exec -T app python -m analytics.release_check \
  --require-rust --expected-revision "$BUILD_REVISION" --expected-release "$RELEASE_ID"
curl --fail http://127.0.0.1:5005/api/health/analytics
```

Use your configured host port. Check the actual container's OCI labels/image ID,
HTTP build identity/readiness, and serving-process native-call deltas around fresh
historical state, events, batch and portfolio requests. Repeat requests should hit
caches without new native calls. Check event JSON and daily future-label horizons
separately. For rollback, set `APP_IMAGE` to the retained old image and repeat the
app-only `up` command; the additive coverage table can remain.

Reproduce measurements with `analytics/deployment_probe.py` (real PostgreSQL and
provider paths, including provider writes if a request needs sync):

```bash
python analytics/deployment_probe.py --base-url http://127.0.0.1:5005 --require-success
docker exec -i stock-tracker-app python - --instrument --cold-each \
  --case daily_state --case intraday_events --case portfolio_state --require-success \
  < analytics/deployment_probe.py
docker exec -i stock-tracker-app python - --instrument --parity < analytics/deployment_probe.py
```

The instrumented probe runs a **separate process** in the actual image, with fresh
service caches when requested; it does not patch serving workers. It counts SQL
(including pool liveness checks), fetched rows, provider calls and native calls;
reports feature/kernel, signals, event assembly, repository/frame conversion,
ranking and JSON timing; and records cumulative process peak RSS on Linux. Nested
phase timings overlap. Parity compares full historical JSON in fresh Python/Rust
service instances with `1e-7` absolute/relative numeric tolerance and exact
structural/label equality; moving current snapshots are excluded. HTTP mode
measures serving-worker latency but cannot directly instrument its SQL/providers.
Historical cutoffs and symbols are explicit in `CASES`; run probes sequentially
for comparable evidence. VEC-802's live deployment measurements are in Vector:
`vector doc show DOC-222` (release `vec-802-20261018`).

### Query parameters

- `timeframe`: supported values are `1D`/`1Day`, `1Hour`, `15Min`, `5Min`, `1Min`
- `asof`: backdated cutoff, accepts `YYYY-MM-DD` or ISO 8601
- `hidets`: optional boolean, default `false`; when `true`, removes `symbol` and timestamp-like fields from the response
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

When `GET /api/state` is queried with an intraday timeframe such as `15Min`, the anchor price comes from that intraday bar at or before `asof`, while moving averages, regimes, RSI, MACD, ATR, and other indicator stacks remain based on daily bars. Price-sensitive fields like `distance_from_sma_*_pct`, `range_position_20_pct`, `above_sma*`, and `extension_from_mean` are recalculated from the intraday anchor price.

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
- The anchor and future window both use the requested timeframe. A date-only `asof` means end-of-day UTC; a timed `asof` is an exact cutoff. Daily requests select the latest stored daily bar at or before that cutoff and the next N daily bars, never fifteen-minute bars. A midnight cutoff can select an earlier session than a date-only cutoff.
- Request `timeframe=15Min` explicitly for fifteen-minute anchors and horizons. `anchor_price.source_timeframe`, `window.source_timeframe`, and `future_state.source_timeframe` report the requested bar series. Unlike technical-state price snapshots, daily future labels do not use intraday price anchors.

- Partial windows return the available bars with `complete_window=false` and `target_end_timestamp=null`; no future bars produces a per-symbol error (collected in `errors` for portfolio requests).
- `max_price` uses the maximum future `high` inside the window
- `min_price` uses the minimum future `low` inside the window
- `price_at_horizon` uses the close of the Nth future bar in the realized window
- all `*_pct` values are relative to the anchor close at `asof`

### Future-window preparation

Single-symbol intraday future-state reads check persisted successful provider
coverage before fetching, reuse completed windows (including empty market
closures), and fetch only missing ranges. Required provider failures are returned
as errors. Identical concurrent preparation shares one operation within a worker.
The search envelope remains seven calendar days before `asof` through `days`
calendar days after it, capped at the delayed present. Automatic single-symbol
preparation is limited to a 31-day envelope; larger uncovered envelopes require
explicit preparation in smaller windows. This search is not a guarantee of N trading bars;
inspect `window.complete_window` and `future_bar_count`.

Portfolio future-state reads perform **no provider I/O**. For explicit intraday
requests, symbols without verified coverage return per-symbol errors identifying
the required timeframe and bounds. Prepare these ranges explicitly, then retry:

```bash
curl -s http://127.0.0.1:5000/api/coverage/prepare \
  -H 'Content-Type: application/json' \
  -d '{"symbols":["NVDA"],"timeframe":"15Min","start":"2023-09-13T15:00:00Z","end":"2023-10-10T15:00:00Z"}'
```

Intraday preparation accepts at most **5 unique symbols and 31 calendar days**
per request. Split larger work into bounded requests; do not fan out automatic
preparation from a portfolio read. `include_benchmarks` defaults to false for
intraday preparation and true for daily preparation. Daily preparation retains
its 50-symbol/400-day limits. Daily future-state reads use stored daily bars;
missing daily data on the single-symbol route retains its daily backfill retry.
Coverage is recorded only after successful fetch/upsert and excludes unfinished
delayed bars, which must be rechecked. A portfolio requiring such a tail reports
preparation required until the requested tail is finalized; no stale result is
silently substituted. Existing bars alone do not certify gap-free coverage.

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

## Historical analytics coverage (VEC-798)

Historical state and future-label responses report `requested_asof` separately
from their actual anchors. Inspect `data_quality.historical_coverage`: state
reports daily indicators and price separately, while labels report their anchor.
Each includes `actual_bar_timestamp`, `lag_seconds`, `stale`, and `status`.
`stale_trailing` means more than seven **calendar** days behind the cutoff;
`within_tolerance` is an age check, not proof of complete history. No exact-date
bar is required for weekends or holidays. `completeness_verified` remains false:
this check does not audit interior gaps or use an exchange calendar. Requests
without an `asof` have `not_evaluated` historical coverage.

Batch quality summaries list `stale_symbols`; label batches also list
`incomplete_window_symbols`. Check these along with per-symbol `errors` and
`benchmark_errors` even when HTTP status is 200 and `ok=true`. Per-state
`benchmark_coverage` reports alignment and benchmark age. Stale or unaligned
benchmarks do not produce relative-return features.

Prepare a targeted daily window before generating a training dataset:

```bash
curl -s http://127.0.0.1:5000/api/coverage/prepare \
  -H 'Content-Type: application/json' \
  -d '{"symbols":["NVDA","AAPL"],"start":"2025-11-01T00:00:00Z","end":"2026-10-01T00:00:00Z"}'
```

This adds SPY and QQQ by default (`include_benchmarks: false` disables this),
deduplicates symbols, and fetches daily gaps in recorded provider coverage.
Limits are 50 unique symbols including benchmarks and 400 calendar days per
request, ending no later than the delayed present. Choose bounds that include
indicator warmup and the future-label horizon; split longer histories into
bounded requests. Verified empty closure windows are reused. Unfinalized recent
tails may be fetched again. The response includes `prepared`, `errors`, and
per-symbol `fetched_window_count`; provider failures do not abort other symbols.
Provider coverage records verify a completed response, not session completeness.

For daily feature/label pairing, preserve timestamps and join on **symbol and
daily indicator/label anchor timestamp**, checking age and complete future
windows. Timed daily states can have an intraday price anchor, so joining on
symbol or top-level `as_of` alone is insufficient. Full endpoint details are on
`/api-docs`.

## Dashboard daily snapshot benchmark (VEC-781)

`GET /data` shares one chronological daily-close snapshot across trend, RSI,
MACD, latest-bar timestamp, and MA distances. A per-symbol indexed `LATERAL`
query reads at most 250 bars. RSI/MACD retain the dashboard's existing 250-bar
seed window; MA/trend formulas and missing-history defaults are preserved.

Reproduce against a PostgreSQL database where the user can create schemas:

```bash
export ANALYTICS_TEST_DATABASE_URL='postgresql://user:password@host/database'
python -m unittest discover -s tests
python -m analytics.dashboard_benchmark --repeats 30 --bars 5000
```

The benchmark creates and removes a unique private schema with production DDL
and indexes. It loads the actual pre-change route/helpers from git commit
`1b62127` (`--baseline-ref` overrides this); retain that commit in the checkout's
history for the benchmark and PostgreSQL dashboard parity test. Each measured
request checks exact full-response parity. Price fetching is stubbed with a mix
of supplied and missing prices, so no live-price network calls enter these timings.
All other route work, including repeated schema-initialization SQL, is measured.
One warmup pair precedes 30 measured pairs, with before/after order alternating;
PostgreSQL and OS caches are warm. These are synthetic long-history portfolios,
not production traffic measurements.

Measured 2026-10-17, 5,000 daily bars per symbol, milliseconds:

| Symbols | Before median / p95 | After median / p95 | Daily bar scan rows before → after | Total rows transferred before → after |
| --- | ---: | ---: | ---: | ---: |
| 10 | 819.30 / 1063.18 | 198.14 / 477.61 | 250,000 → 2,500 | 7,720 → 2,510 |
| 50 | 2621.34 / 2855.65 | 301.14 / 339.69 | 1,250,000 → 12,500 | 38,600 → 12,550 |
| 100 | 4956.04 / 5146.97 | 441.44 / 639.38 | 2,500,000 → 25,000 | 77,200 → 25,100 |

At every size: bar SELECTs **5 → 1**, all SELECTs **7 → 3**, total SQL
statements including initialization **112 → 48**. Transferred rows include
forecast/earnings rows. Scan counts sum `Actual Rows + Rows Removed by Filter`
times `Actual Loops` at `stock_bars` relation scan nodes in `EXPLAIN ANALYZE`;
they count logical rows visited, not physical disk reads or intermediate nodes.
The baseline plans scan whole daily histories under window/aggregate nodes.
The new plan uses `Nested Loop → Limit → Index Scan Backward (stock_bars_pkey)`,
250 rows per symbol, followed by chronological sorting of the bounded result.
No additional index is required.

Full `EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON)` plans for all five old bar
queries and the new query at each portfolio size are retained in Vector:
`vector doc show DOC-213` ("VEC-781 dashboard snapshot benchmark and EXPLAIN evidence").
PostgreSQL parity tests also cover 0/1/14/15/19/20/21/25/26/33/34/35/49/50/
199/200/201/220/249/250/251/5000-bar histories, hidden rows, absent metadata,
empty portfolios, latest-close fallback, and unrelated newer intraday bars.

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
