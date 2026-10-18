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

### Bounded analytics caches (VEC-808)

State, event and future results use separate, concurrency-safe per-worker LRUs.
Configure each with positive integer environment variables (invalid values fail
service startup):

| Variable (`KIND` = `STATE`, `EVENT`, or `FUTURE`) | Default per cache / worker |
| --- | ---: |
| `ANALYTICS_KIND_CACHE_ENTRIES` | 256 |
| `ANALYTICS_KIND_CACHE_BYTES` | 8388608 (8 MiB) |
| `ANALYTICS_KIND_CACHE_INFLIGHT` | 64 producers |
| `ANALYTICS_KIND_CACHE_WAITERS` | 256 coalesced waiters |

The byte budget charges encoded immutable JSON bytes, recursively measured key
objects (`sys.getsizeof`, conservatively charging shared objects repeatedly),
and 256 bytes per entry for mapping/tuple overhead. Both entry and byte limits
apply. Oversized results are delivered to the producer and its current waiters
but are not retained; later requests compute again. JSON decoding gives every
caller an independent mutable response, so route flags and portfolio enrichment
cannot contaminate cached results.

The three caches together have at most 24 MiB of accounted retained entry storage
per worker by default, plus fixed container/lock overhead and bounded in-flight
bookkeeping (64 keys per cache, each limited to 16 KiB, and 256 waiting callers).
This is **not a whole-process RSS limit**: input bars/features, temporary encoding,
decoded responses, request threads, Python allocator high-water marks and DB
pools need additional memory. Multiply budgets by the production worker count.
The benchmark measures retained object graphs and worker RSS separately rather
than treating entry count or JSON wire length as memory consumption.

Per-key single-flight covers state, events and future computations. Unrelated
keys proceed independently; PostgreSQL, analytics, encoding and decoding run
outside the short cache lock. Failures (including producer cancellation via a
raised `BaseException`) wake all waiters and remove the flight, permitting retry.
A disconnected HTTP client does not cancel an active producer; it finishes for
other waiters. Same-key recursion and nested waits on another producer are
rejected to prevent dependency cycles within a cache. Exhausted producer/waiter
budgets reject new admissions rather than queueing indefinitely: single-symbol
routes return 503 / `Retry-After: 1`; batches collect per-symbol errors. Existing
producers continue, and cached hits still work.

`GET /api/health/analytics` includes `caches` with `scope: per_worker`, and fixed
state/event/future groups: hits, misses, evictions, computations, coalesced waits,
failures, oversized results, rejected admissions, entries, accounted current bytes,
active producers/waiters and configured limits. Counters reset on worker restart.
Planning membership probes do not count as hits/misses or alter recency. A miss
counts each caller without a retained result; computations count elected producers
and coalesced waits count admitted followers. There are no symbol/date labels.

Keys retain symbol, timeframe, asof and `last_bar_synced_at` distinctions; state
and event keys also include all analytics settings, events include the limit,
and future keys include horizon, thresholds and side. Each request reads current
metadata before planning. A changed stored sync revision makes old entries
unreachable (they expire by LRU eviction). The revision is the existing
symbol-wide sync timestamp, not a transactional content hash: direct SQL edits,
metadata/coverage changes or writes that do not advance `last_bar_synced_at`
are not detected, and a sync racing a request can mix metadata and bar snapshots.
There is no cross-worker invalidation broadcast or TTL; writers must advance the
revision, or workers must be restarted after out-of-band data edits.

For DOC-228's 37-symbol twelve-date working set (468 state entries including
SPY/QQQ, 444 future entries), this measured configuration fits within the
unchanged 8 MiB byte budgets:

```bash
ANALYTICS_STATE_CACHE_ENTRIES=512
ANALYTICS_FUTURE_CACHE_ENTRIES=512
```

That is capacity tuning for a finite working set, not a solution for arbitrary
historical sweeps. VEC-811 tracks multi-date computation reuse; VEC-814 tracks
further warm-response assembly work. Request-local `BatchReads` stay independent
and retain lazy, bounded group reads; planning is advisory because eviction can
happen before use. A coalesced follower does not execute its producer callback,
although independent leaders can still prefetch overlapping batch groups.

Reproduce unprofiled before/default/tuned HTTP tests and synchronized 16-client
cold bursts using the same read-only Docker isolation described below. Replace
the serving command with `python -m analytics.cache_benchmark serve`, then run:

```bash
python -m analytics.cache_benchmark run --base-url http://127.0.0.1:5008 \
  --label after-default --output /tmp/cache-default.json
python -m analytics.cache_benchmark parity --base-url http://127.0.0.1:5008 \
  --compare-url http://127.0.0.1:5009 --output /tmp/cache-parity.json
```

Repeat in a fresh container with the two 512-entry settings for the tuned case.
For the pre-change checkout, mount this checkout's `analytics/cache_benchmark.py`
at `/bench/cache_benchmark.py`, put the baseline checkout at `/work`, set
`PYTHONPATH=/work`, and serve with `python /bench/cache_benchmark.py serve`.
The adapter counts legacy get/set hits/misses/evictions without adding coalescing.
Benchmark-only `/_benchmark/cache` GET/DELETE routes sample counters, deep retained
object size, RSS and process-lifetime peak RSS, and reset caches between workloads;
they are installed only by this isolated entrypoint. Hot and sweep stages include
warmup; bursts deliberately reset to cold. Raw records include p50/p95/p99,
completed throughput, capacity/scheduler drops, failures and before/after counters.
Measured default/tuned comparisons, memory measurements and full-JSON parity are
recorded in Vector DOC-229 (`vector doc show DOC-229`).

### High-rate, multi-date portfolio load test (VEC-807)

The stdlib HTTP load generator exercises both historical portfolio endpoints with
fixed-clock arrival rates and bounded concurrency. It reports per-endpoint/date
p50/p95/p99, offered and completed throughput, client admission/scheduler drops,
HTTP/application errors, response sizes and data-quality observations. An HTTP
200 with missing symbols is a failure; stale coverage/benchmark warnings and
incomplete future windows are counted separately. JSON contains raw request
records as well as summaries; overloaded stages fully drain before the next.

```bash
python -m analytics.portfolio_load --base-url http://127.0.0.1:5008 \
  --dates 2026-03-17,2026-06-16,2026-09-16 --rates 5,25,50,100 \
  --duration 15 --concurrency 32 --warmup --output /tmp/portfolio-hot.json
```

Use `--endpoint state` or `--endpoint futurestate` for isolated endpoint load.
For a date sweep exceeding the current 256-entry per-symbol caches:

```bash
python -m analytics.portfolio_load --base-url http://127.0.0.1:5008 \
  --dates 2026-02-17,2026-02-24,2026-03-17,2026-03-24,2026-04-15,2026-04-22,2026-05-15,2026-05-22,2026-06-16,2026-07-15,2026-08-18,2026-09-16 \
  --rates 1,5,10 --duration 30 --concurrency 16 --warmup \
  --output /tmp/portfolio-sweep.json
```

Select dates covered by your own portfolio data with enough future trading bars.
Date-only cutoffs use the application's end-of-day semantics: state may use
15-minute price anchors while daily indicators and daily future labels retain
their existing timeframes. `--warmup` requests each case once and aborts on
partial portfolios; a working set exceeding the cache cannot remain warm.
Latency percentiles cover completed requests, including drain; client drops
must be considered alongside them when interpreting overload capacity.

An isolated serving/profiling entrypoint uses the same routes and stored data,
prohibits provider calls and bypasses deployment DDL. For the compose setup,
from the repository root, with the existing image containing the Rust extension:

```bash
IMAGE=$(docker inspect stock-tracker-app --format '{{.Config.Image}}')
docker run -d --rm --name apollo-portfolio-load --network apollo_default \
  -p 127.0.0.1:5008:5000 -v "$PWD:/work:ro" -v /tmp:/results -w /work \
  -e PYTHONDONTWRITEBYTECODE=1 -e ANALYTICS_BACKEND=rust \
  -e 'DATABASE_URL=postgresql://stock_user:stock_pass@postgres:5432/stock_tracker?options=-cdefault_transaction_read_only%3Don' \
  "$IMAGE" python -m analytics.portfolio_load_local serve

# Run profiles after HTTP load has finished, without competing benchmark traffic.
docker exec apollo-portfolio-load python -m analytics.portfolio_load_local profile \
  --repeats 3 --output /results/portfolio-profile-rust.json
docker exec -e ANALYTICS_BACKEND=python apollo-portfolio-load \
  python -m analytics.portfolio_load_local profile \
  --repeats 3 --output /results/portfolio-profile-python.json

# Use --timing-only for uninstrumented cold/warm wall-clock comparisons.
docker exec apollo-portfolio-load python -m analytics.portfolio_load_local profile \
  --timing-only --repeats 5 --output /results/portfolio-timing-rust.json
docker stop apollo-portfolio-load
python -m unittest tests.test_portfolio_load
```

Adapt image/network/credentials to your environment. This setup uses a separate
threaded Werkzeug process and a read-only database connection; the application
container and its caches are not the load target. Profiling uses sequential Flask
test-client requests with fresh services for cold samples and immediate warm
repeats. cProfile overhead is substantial: use it for hotspot attribution and
`--timing-only`/HTTP runs for latency. Each profile output records observed backend
dispatch counters, per-function call counts and cumulative/self times.

Measured 2026-10-18 on 37 actual portfolio symbols: the three-date mixed workload
handled 25 req/s below 50 ms p95, saturating around 37–38 req/s; twelve dates
exceeding the caches saturated near 3.3 req/s. Rust already reduced cold state
median from 2,378 to 838 ms, but cold future labels spent ~59% of profiled time
materializing tiny pandas frames. Warm requests were dominated by membership
DataFrame creation, copying and JSON cleanup. These are single-worker local
observations with existing coverage warnings, not production capacity guarantees.
Full tables, methodology, coverage findings and prioritized Rust/Python/cache
recommendations are maintained in Vector: `vector doc show DOC-228`.

### Focused portfolio membership reads (VEC-813)

Analytics portfolio routes read only ordered symbols from PostgreSQL, filtering
hidden rows in SQL. Python preserves the existing Unicode normalization and
first-occurrence deduplication rules. Each request reads current membership
through the shared pool, so forecast additions, deletions and visibility edits
take effect immediately.

The alternating benchmark loads the actual pre-change helper from git history,
asserts full response equality on both Flask routes, and records warm latency,
query counts, separate allocation probes and process RSS observations:

```bash
DATABASE_URL='<read-only DSN>' ANALYTICS_BACKEND=rust \
  python -m analytics.membership_benchmark --repeats 30 --output /tmp/membership.json
```

It uses stored data, prohibits provider calls and bypasses schema DDL. The
isolated HTTP server above also accepts `--membership-baseline-ref 9d391c3`
(requires git) to compare endpoint-only load with the old helper on current code.
Measured visible membership median fell from 6.57 to 0.97 ms; all 180 alternating
route pairs had identical full JSON. At 100 offered futurestate requests/s,
in-window throughput rose from 58.1 to 92.1 req/s in the short local load test.
Methodology, full latency/drop tables, data-quality observations and validation
are maintained in Vector: `vector doc show DOC-233`.

### Warm portfolio response ownership (VEC-814)

Symbol serializers normalize JSON before insertion into immutable byte caches.
Each cache caller receives an independently decoded object graph, so portfolio
ranks, relative returns and benchmark quality can be enriched in place without
copying the whole symbol again. Batch assembly normalizes only its small envelope
and newly calculated relative returns; it does not recursively rebuild clean
symbol subtrees. Frozen settings reuse one complete settings-key tuple per
instance across batch planning and symbol lookups.

The baseline-pinned benchmark compares complete cold/warm responses, alternates
warm latency pairs, and separately records traced allocation peaks and profiles:

```bash
DATABASE_URL='<read-only DSN>' ANALYTICS_BACKEND=rust \
  python -m analytics.warm_response_benchmark --repeats 30 --output /tmp/warm.json

# Separate terminals, isolated servers with benchmark-only RSS diagnostics:
DATABASE_URL='<read-only DSN>' ANALYTICS_BACKEND=rust \
  python -m analytics.warm_response_benchmark --serve 5014 --variant before
DATABASE_URL='<read-only DSN>' ANALYTICS_BACKEND=rust \
  python -m analytics.warm_response_benchmark --serve 5015 --variant after
python -m analytics.warm_response_benchmark --load --output /tmp/warm-load.json
```

Requires git history and the stored 37-symbol portfolio used by DOC-228. It
enforces read-only database transactions, bypasses DDL and prohibits providers.
Against the current SQL-membership baseline, warm state medians fell 25–26% and
futurestate 18–21%; peak traced memory fell about 2% / 4%. Mixed throughput at
100 offered req/s rose 47.2→61.9 req/s. Overloaded state-only p95/p99 worsened
despite higher throughput. Full gains, regressions, ownership tests and
reproduction details are in Vector: `vector doc show DOC-238`.

### Cold state snapshots (VEC-809)

State endpoints use request-local typed OHLCV columns and the Rust extension's
`feature_columns(..., snapshot=True)` mode (extension 0.1.1+). Only the latest
feature scalars cross the output boundary; there is no historical feature
DataFrame on the eligible native state path. The same native series kernel
retains all EMA/RSI/MACD seed history, trailing windows and YTD semantics.
Native working series are still allocated internally. Event generation and
multi-date historical reuse retain the full-frame API; both feed the same state
payload assembler. Missing/nonfinite or nonpositive-close histories retain the
Python oracle. Older extension wheels use the compatible full-frame path.
Integer UTC microsecond timestamps and original integer volume are preserved.

On the current post-VEC-811 implementation, alternating cold Flask requests on
stored data improved median latency from **81→55 / 284→182 / 929→579 ms** for
1/10/37 symbols (plus SPY/QQQ), with exact complete JSON parity and unchanged
SQL reads/rows. These gains are additional to upstream reuse/cache work. Full
methodology, 252/5,000-bar tests, peak RSS, boundary timings, actual SQL plans and
HTTP sweep results are in Vector: `vector doc show DOC-237`.

```bash
# Existing stored data: refuses a writable default transaction; no DDL/provider I/O.
DATABASE_URL='<read-only DSN>' ANALYTICS_BACKEND=rust \
  python -m analytics.snapshot_benchmark --repeats 5 --explain \
  --output /tmp/snapshot-comparison.json
# Repeat in fresh processes with --variant reference / --variant snapshot for RSS.
# Synthetic history lengths: creates/drops a private schema on a disposable DB;
# timed endpoint reads still enforce read-only transactions.
ANALYTICS_TEST_DATABASE_URL='<disposable DSN>' ANALYTICS_BACKEND=rust \
  python -m analytics.snapshot_benchmark --fixture-bars 5000 --repeats 5 \
  --output /tmp/snapshot-5000.json
```

`--serve PORT --variant reference|snapshot` runs an isolated local worker for
the VEC-807 `analytics.portfolio_load` cold/sweep workload. Internal native-versus-
pandas feature parity uses `rtol=1e-8, atol=1e-10`; transport/same-backend and
serialized state parity use exact equality, including quality/error fields.

### Typed future-label batches (VEC-810)

Future labels use UTC-microsecond `LabelBar` records containing only timestamp,
high, low and close. Request-local batches read at most 100 symbols per chunk,
with one anchor and at most the requested future bars per symbol. A single scan
finds extrema (earliest ties) and first threshold hits. The pandas oracle remains
available for differential validation; state/events keep their frame interface.

The measured default is the direct Python reducer. The experimental PyO3
`future_window_indices` comparator takes one chunk per call, releases the GIL,
and returns four indices per window. Its conversion-inclusive latency did not
justify selecting it over Python. Build the current extension before comparing:

```bash
VIRTUAL_ENV=/tmp/opencode/apollo-venv /tmp/opencode/apollo-venv/bin/maturin develop \
  --release --manifest-path rust/Cargo.toml
# Set DATABASE_URL to a read-only connection to your stored portfolio database.
python -m analytics.future_benchmark --repeats 15 \
  --dates 2026-03-17,2026-06-16,2026-09-16 --horizons 1,20,252 \
  --output /tmp/future-comparison.json
# Separate fresh-process RSS observations (repeat for python and rust).
python -m analytics.future_benchmark --rss-variant reference --repeats 5 \
  --output /tmp/future-reference-rss.json
# Isolated HTTP target for the portfolio_load commands above.
python -m analytics.future_benchmark --rss-variant python --serve 5011 \
  --output /tmp/unused.json
ANALYTICS_TEST_DATABASE_URL=<disposable-test-dsn> python -m unittest tests.test_future_labels
```

The comparison alternates reference/Python/Rust ordering, checks full JSON
equality, and reports unprofiled cold/warm p50/p95. A separate instrumented pass
reports bar conversion, conversion-inclusive batch reduction, and SQL/row counts
including portfolio membership. RSS uses Linux `VmHWM` per fresh process. On the
37-symbol portfolio, 20-bar cold median fell from 291 to 39 ms; warm median stayed
near 17 ms. A 252-bar request can be partial when stored future history is short;
inspect `complete_window`. Detailed evidence and load tables live in Vector:
`vector doc show DOC-230`.

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
- `POST /api/portfolio/history` - bounded stored-data daily features and realized labels for many cutoffs
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

### Historical daily portfolio sweeps (VEC-811)

```bash
curl -s http://127.0.0.1:5000/api/portfolio/history \
  -H 'Content-Type: application/json' \
  -d '{"symbols":["NVDA","AAPL"],"cutoffs":["2026-03-17","2026-06-16T00:00:00Z"],"timeframe":"1D","settings":{"breakout_lookback":55},"days":20,"stop_loss_pct":0.05,"take_profit_pct":0.10,"side":"long"}'
```

Omit `symbols` to snapshot current saved portfolio membership once (optionally
`"include_hidden": true`). Date-only cutoffs mean end-of-day UTC; explicit
timestamps retain their instant. One daily history/feature series is reused per
symbol, with bounded stored 15-minute price anchors and typed daily future windows.
The read never fetches provider data: use `/api/coverage/prepare` explicitly for
missing daily/benchmark and intraday coverage before rerunning.

Response `dates[]` entries contain `date_index`, original `cutoff`, normalized
`requested_asof`, `features` (existing batch-state schema) and `labels` (existing
batch-futurestate schema). Join by `(date_index, symbol)`; actual state price and
daily-label anchor timestamps may differ. Ranks, errors and SPY/QQQ alignment are
per date. Keep realized future labels separate from historical model inputs;
inspect coverage and partial-window flags. Current saved membership does not
represent historical holdings.

Limits: 120 dates, 50 input symbols, 3,000 date-symbol pairs, 1–252 future bars,
20,000 daily history bars per symbol, 32 MiB compact JSON result, 64 KiB request.
Oversized requests return 413 without partial output; invalid inputs return 400.
One sweep per worker (busy: 503 / Retry-After: 1); split larger datasets into
requests. Full-history initialization is preserved rather than truncated.
The complete contract, settings, service entrypoint and causality audit are in
[`analytics/HISTORICAL.md`](analytics/HISTORICAL.md) and `/api-docs`.

Read-only PostgreSQL measurements on 37 stored symbols, Rust indicators, horizon
20: median 12-date sweep **11.38 → 2.48 s (4.59x)** and 60-date sweep
**53.27 → 6.59 s (8.08x)**. Complete feature/label JSON matched on all 444/2,220
date-symbol samples, including coverage/errors/benchmarks. Bulk feature
computations stayed at 39 (including SPY/QQQ) rather than 468/2,340. At 60 dates,
reads/rows fell from 540/3,977,819 to 116/71,239; process peak RSS was 201.0 versus
183.8 MiB. The JSON API probe returned the same full result in 7.17 s / 11.86 MiB.
Five concurrent cached interactive requests/s increased p95 from 30 to 87 ms
during the 60-date bulk sweep (maximum 735 ms), so synchronous work still has
interactive cost. These are isolated service/test-client measurements, not
network or production-worker capacity measurements.

Reproduce each variant/count in a fresh process with a read-only `DATABASE_URL`
(`options=-cdefault_transaction_read_only%3Don`); the harness checks read-only
mode, disables DDL/provider I/O, reports independent peak RSS and full-result
fingerprints. Repeat with `--variant bulk`, `--date-count 12` and/or
`--impact-jobs 3`; `--probe-api` also checks the HTTP JSON entrypoint.

```bash
python -m analytics.historical_benchmark --variant sequential --date-count 60 \
  --repeats 3 --impact-jobs 1 --output /tmp/sequential60.json
python -m analytics.historical_benchmark --variant bulk --date-count 60 \
  --repeats 3 --impact-jobs 3 --probe-api --output /tmp/bulk60.json
# After also generating sequential12.json and bulk12.json:
python -m analytics.historical_benchmark --compare-files \
  /tmp/sequential12.json /tmp/bulk12.json /tmp/sequential60.json /tmp/bulk60.json
```

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

### Production WSGI processes (VEC-815)

The container starts `python -m production`, which performs the supported schema
initialization once, closes its DB pools, then **execs Gunicorn 23.0.0**. Gunicorn
is PID 1, with no Flask debugger, reloader, or extra launcher process. It does
**not preload**: each worker imports the application and creates its own caches,
locks, sync single-flights and lazy quote executors. The exec boundary removes
all schema-initializer state before any fork; the existing pool fork callbacks
remain a defense for other callers. Never enable `--preload` on this application.

| Setting | Default | Meaning |
| --- | ---: | --- |
| `WSGI_WORKERS` | 2 | Independent serving processes |
| `WSGI_THREADS` | 2 | `gthread` request threads per worker |
| `DB_POOL_SIZE` | 2 in production | Maximum connections **per worker per exact DSN**; lazy allocation |
| `DB_POOL_TIMEOUT_SECONDS` | 30 | Pool acquisition/connect waiting budget |
| `WSGI_TIMEOUT` | 120 | Gunicorn worker heartbeat timeout; **not a request deadline** for gthread |
| `WSGI_GRACEFUL_TIMEOUT` | 120 | Time allowed to drain on graceful shutdown before forced termination |
| `WSGI_STOP_GRACE_PERIOD` | 130s | Compose's container stop allowance; keep greater than graceful timeout |
| `WSGI_INIT_DB` | 1 | `0` skips launcher DDL when a deploy job already provisioned the schema |
| `HOST` / `PORT` | 0.0.0.0 / 5000 in Docker | Listening address inside the container |

Production defaults apply with `python -m production` or the checked-in Gunicorn
config. Other entrypoints retain the pool module's four-connection default.
Use `python app.py` deliberately for local development with Flask debug/reload.
Do not serve production with that development command. `WSGI_APP` is an advanced
launcher override; its default is `wsgi:application`. The benchmark-only
`analytics.wsgi_load:application` forbids providers and adds a shutdown-test route;
it is for disposable load containers only.

Each worker validates the required DB tables/columns in a completed read-only
transaction and executes the finite native/backend smoke calculation **before
serving requests**. Missing schema or invalid/required-but-unavailable Rust aborts
worker boot and causes Gunicorn startup to fail. Workers mark schema readiness so
normal requests do not repeat DDL. Schema changes require a new release/startup.
The smoke uses the selected backend: intentional `ANALYTICS_BACKEND=python` is
supported, while Compose defaults to required Rust. Startup smoke dispatches do
not inflate serving native counters or populate analytical result caches.

`runtime.process` reports the responding PID, parent PID, per-DSN pool limit and
worker-local startup checks. `X-Worker-Pid` identifies the responding process on
every WSGI response. Build identity is the same artifact across workers, while
native counters, caches, quote freshness and sync flights are **per process**.
One healthy response, including Compose's healthcheck, is not proof of aggregate
worker readiness or aggregate cache hit rates. Match the configured process count
with `docker top`, check every `WSGI worker ready` log, and observe each serving
PID via health (the benchmark's `all_workers` helper does this explicitly).
Gunicorn binds the listener before boot completes; connection availability alone
does not establish readiness. During rolling replacement, track the current PID
set; a cached health sample from a dead worker is not current evidence.

There is no periodic background market-data/live-price loop. Quote refreshes are
on demand, with at most one refresh coordinator and eight symbol-fetch threads
per worker by default. These executors start threads only when used. Additional
processes can duplicate provider refreshes and historical misses: single-flight
coalescing is not cross-process. Historical capacity probes prohibit provider
calls and use read-only DB transactions; do not substitute live-moving requests
when comparing process counts.

`WSGI_THREADS` limits request threads, not all OS threads. Native numerical
libraries can create additional threads on import: the measured image had 26 OS
threads per active worker with two request threads, even without quote refreshes.
Those native-library settings were held identical across all process-count runs.

**Resource budget:** default production DB maximum is `2 workers × 2 = 4`
connections for one DSN, plus a transient schema-initialization connection before
workers start. Add other applications, monitors/admin reserve, extra DSNs and
both old/new replicas during rolling overlap before comparing to PostgreSQL
`max_connections`. Reloads can temporarily overlap old/new worker generations;
include that too. Pool size bounds connections, not executing requests. Caches
default to at most 24 MiB of accounted entries **per worker** across three groups
(48 MiB for two workers), plus interpreter/imports, result frames, decoded JSON,
request/quote threads and allocator retention. RSS is substantially larger than
cache charges. Warming one process does not warm another; warm every desired
case/PID or include cold misses in the capacity estimate. Default 256 entries
cannot hold all twelve dates for 37 symbols in every worker.

For startup and rollback, use the versioned-image build/check procedure below.
If schema was applied separately, set `WSGI_INIT_DB=0` for the launch. Replace
only the app and allow drain:

```bash
export WSGI_WORKERS=2 WSGI_THREADS=2 DB_POOL_SIZE=2
export WSGI_TIMEOUT=120 WSGI_GRACEFUL_TIMEOUT=120 WSGI_STOP_GRACE_PERIOD=130s
docker compose up -d --no-build --no-deps app
docker inspect stock-tracker-app --format '{{json .Config.Cmd}} {{.Image}}'
docker top stock-tracker-app -eo pid,ppid,rss,args
docker compose logs app
curl --fail http://127.0.0.1:5005/api/health/analytics
# Roll back to the retained, previously validated image:
APP_IMAGE=apollo-app:YOUR_RETAINED_RELEASE docker compose up -d --no-build --no-deps app
```

SIGTERM stops admission and drains in-flight work up to the graceful timeout;
worker exit explicitly closes pools. Normal pooled transactions commit/rollback
and return connections, so idle sessions must not remain idle-in-transaction.
Forced termination can interrupt requests and roll back transactions. A client
disconnect does not cancel analytic computation. Gunicorn's default connection
backlog/worker connection queues and gthread heartbeat are not end-to-end load
shedding or request deadlines; size upstream admission/timeout limits separately.
Compose replacement can have downtime; it is not itself a zero-downtime rollout.

#### Reproducing process capacity and shutdown measurements

Build the intended clean revision into an image first. On the Docker host, use a
read-only DSN whose DB address is reachable from both the host and Docker network:

```bash
DATABASE_URL='postgresql://USER:PASSWORD@DB_IP/stock_tracker?options=-cdefault_transaction_read_only%3Don' \
  python -m analytics.wsgi_benchmark --image apollo-app:YOUR_RELEASE \
    --network apollo_default --workers 1,2,4 --threads 2 --connection-budget 8 \
    --duration 15 --soak-seconds 180 --output /tmp/wsgi-capacity.json
ANALYTICS_TEST_DATABASE_URL='postgresql://USER:PASSWORD@DB_IP/stock_tracker' \
  python -m unittest tests.test_wsgi tests.test_database
```

The host needs psycopg2 and local Docker socket access. Disposable containers use
the actual production launcher/config with only provider guards and a slow-request
test route added. Each worker verifies its own schema and Rust execution. The
probe requires read-only defaults, warms **every endpoint/date/PID**, then uses
the unchanged VEC-807 open-loop client with fixed 32 hot / 16 sweep in-flight
caps. The 1/2/4-worker pool limits are 8/4/2, keeping the total ceiling at eight;
two request threads per process remain fixed. It captures cold warmup timings,
per-worker cache/native health, complete-window throughput, p50/p95/p99, failed
and dropped arrivals, continuous one-second Docker CPU/memory/process RSS and
tagged PostgreSQL sessions, a three-minute overload per process count, recovery,
and SIGTERM while a DB request is observed in flight. The final assertions require
successful complete responses, no leaked transaction-idle sessions, exit 0 and
zero remaining app DB sessions. Stages drain fully; latency covers completed
requests including drain, while throughput counts only completions within the
offering window. Admission drops have no latency. Docker DB CPU includes other
clients sharing that PostgreSQL instance; process RSS double-counts shared pages.

#### Measured tradeoffs (2026-10-18)

Full results, endpoint p50/p95/p99, hot/sweep/drop tables, continuous resource
methodology and raw-artifact references are in **Vector DOC-239: VEC-815 Production
WSGI multi-process capacity and resource budgets**. The measured image is clean
revision `50dc359`, release `vec-815-capacity`, immutable image `d97ae978e5ac...`.
It includes previously merged analytics optimizations; only comparisons between
process counts in this same image isolate process-model gains.

| Workers (2 request threads each) | 180-second hot completion req/s at 250 offered | Combined p50 / p95 / p99 ms | Client capacity / scheduler drops | Mean app / DB CPU % | Peak app cgroup MiB | Actual peak DB sessions |
| ---: | ---: | --- | ---: | --- | ---: | ---: |
| 1 | 65.39 | 481.84 / 577.53 / 630.29 | 32,145 / 1,052 | 101.54 / 15.07 | 128.68 | 2 |
| 2 | 118.80 | 263.34 / 487.88 / 540.61 | 20,954 / 2,631 | 202.40 / 27.84 | 227.25 | 4 |
| 4 | 208.26 | 54.76 / 127.15 / 204.87 | 433 / 7,069 | 375.41 / 48.59 | 436.65 | 8 |

All **85,148 measured completed requests** across hot, twelve-date sweep, soak
and recovery returned HTTP 200, 37/37 symbols, zero symbol errors and no incomplete
future windows. Existing state stale/benchmark coverage warnings remain. Every
worker passed DB/native startup, every in-flight SIGTERM probe completed, exit
codes were zero and no app DB sessions remained after shutdown. All process
counts recovered to 25 req/s with no drops and p95 33.74 / 36.30 / 35.24 ms.

Two workers at 100 offered req/s completed 99.73 req/s with combined p95 47.19 ms,
zero capacity drops and two scheduler-missed arrivals. Twelve-date sweep at 10
offered req/s completed 6.00 / 9.93 / 9.93 req/s in-window; combined p95 was
3,363.19 / 975.89 / 721.04 ms. Four-worker hot arrivals were **client-scheduler
limited** (15.7% scheduler drops in the soak), so 208 req/s is observed throughput,
not a proven maximum server rate or a lossless 250 req/s claim. Tests ran on the
shared 24-CPU host/DB; the three-minute overload per process count is not an SLA
or hours-long memory-leak study.

The selected two-worker default balances the measured latency/capacity gain with
memory and connections. Observed worker RSS peaked around 148–150 MiB after
historical sweeps; this is larger than retained cache charges. Each worker
duplicates 117 state and 111 future computations and 902,002 accounted cache
bytes for the hot set. First-per-PID hot state medians were 566–610 ms, versus
roughly 25–33 ms when hot at moderate load. The default twelve-date caches evict
in every worker; process count does not create a shared working set. Four
processes improve capacity further at roughly double two-worker cgroup memory.

**Delivery state:** the new production command/image was verified in isolated
containers (including the default unmodified WSGI app), not rolled out to the
live service. At verification, port 5005 still served `vec-806-20261018` with
`python app.py`; PR-open/merged and deployed are separate states.

### Live WSGI rollout (VEC-824, 2026-10-18)

The VEC-815 WSGI implementation is now deployed at
**http://192.168.1.80:5005/**. This supersedes the earlier VEC-815 delivery-state
observation above. Full deployment and preservation evidence is **Vector DOC-240:
VEC-824 Live production WSGI rollout and serving-worker verification**.

- Merged application: `11491a0d33a694f29e52d9955dbd4c8c5c179b25` (PR #27).
- Release/image: `vec-824-20261018` / `apollo-app:vec-824-20261018`.
- Immutable image: `sha256:0ba179b2502807f5ec93aab7ec6796e87b69f06f28c44d440d9410d770df3041`.
- Started `2026-10-18T16:02:45Z`; actual Gunicorn master PID 1, two serving
  workers, two request threads each, pool limit two per worker. No debug/reloader.
- Both workers passed DB/schema and native startup; each executed **46 real
  serving native calls**, zero fallbacks. Warm repeats across both workers added
  zero native calls. State, 20-event intraday events, three-stock batch, 37-stock
  portfolio state and portfolio future-label requests all passed on each PID.
- Docker healthy, restart count zero; UI HTML and dashboard data passed (37
  visible rows, all with prices). PostgreSQL container/volume stayed running;
  1,839,280 bars, 38 stored forecasts and forecast checksum were unchanged.

The non-secret release selection is `deploy/vec-824.env`. Application/provider
credentials still come from the host's existing `.env`; its `APP_IMAGE` and
`WSGI_INIT_DB=0` also select this deployed release for plain Compose commands.
To repeat app-only startup with the already-built image:

```bash
docker compose --env-file deploy/vec-824.env up -d --no-build --no-deps app
docker compose --env-file deploy/vec-824.env exec -T app python -m analytics.release_check \
  --require-rust --expected-revision 11491a0d33a694f29e52d9955dbd4c8c5c179b25 \
  --expected-release vec-824-20261018
curl --fail http://192.168.1.80:5005/api/health/analytics
```

`WSGI_INIT_DB=0` assumes the schema was provisioned first: this rollout ran the
supported initializer in a separate release container before replacement. For a
fresh database, run that initializer or use `WSGI_INIT_DB=1` on first startup.
The release image was built from `git archive` of the exact merged revision;
the follow-up PR records configuration/evidence and is separate from live code.

Rollback image `apollo-app:pre-vec-824` retains the previous `vec-806-20261018`
image. A host-local pre-deploy backup is `/tmp/opencode/vec824-pre-deploy.dump`
(`pg_restore --list` verified). Roll back only the app with:

```bash
APP_IMAGE=apollo-app:pre-vec-824 docker compose --env-file deploy/vec-824.env \
  up -d --no-build --no-deps app
```

If keeping a rollback, update the host `.env`/release selection to that image as
well; the inline override lasts only for the command. Keep PostgreSQL and its
volume running. The rollback path was prepared, not exercised on the live app.

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

### Reproducible training-readiness audit (VEC-812)

```bash
python -m analytics.training_audit --base-url http://127.0.0.1:5005 \
  --dates 2026-03-17,2026-06-16,2026-09-16 --output /tmp/before.json
```

The default audit issues historical GETs only, for date-only, midnight UTC and
explicit 20:00 UTC cutoffs. It retains raw responses and a symbol/cutoff matrix
with daily indicator and price timestamps/source timeframes, benchmark alignment,
daily label anchors and realized horizon counts. The conservative screen requires
200 daily bars, recent anchors, expected price source, aligned recent SPY/QQQ,
matching indicator/label anchors and complete label horizons. Inspect
`response_complete` separately from `screen_pass`: full response counts alone do
not establish usable training coverage. Neither flag certifies interior gaps or
exchange-session completeness. Closure dates do not require exact-date bars.

Preparation is a separate explicit operation using the existing bounded API:

```bash
python -m analytics.training_audit --base-url http://127.0.0.1:5005 \
  --prepare-from /tmp/before.json --output /tmp/preparation.json
python -m analytics.training_audit --base-url http://127.0.0.1:5005 \
  --output /tmp/after.json
python -m analytics.training_audit --render-from /tmp/after.json \
  --before-file /tmp/before.json --receipts /tmp/preparation.json \
  --output /tmp/coverage-evidence.md
```

`--prepare-from` targets stale preferred 15-minute price anchors and stale daily
benchmarks in eight-day windows, with at most five symbols per request. Other
gaps, indicator warmup and interior benchmark history can be explicitly supplied
via `--prepare-plan plan.json`, a JSON array of `/api/coverage/prepare` request
bodies. API bounds still apply. Receipts retain provider errors and fetched-window
counts; rerun the audit to establish actual effects. A successful preparation
receipt alone is not evidence of available bars.

For a **separate daily-price benchmark dataset**, use `--variants midnight` with
the three original dates after preparation. Midnight selects the previous daily
session on these dates, avoiding mixed intraday-price/daily-label snapshots;
preserve actual anchors and join features/labels on symbol plus daily anchor.
Vector DOC-243 summarizes the findings, DOC-241 contains the durable before/after
matrix, DOC-242 stores the separate JSON dataset, and DOC-244 stores the closure
and partial-horizon controls. DOC-228/source-34d31ae timings remain the original baseline against its
original stored data; post-preparation quality results do not replace those
measurements. Historical saved membership also does not certify survivorship-free
portfolio selection.

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
