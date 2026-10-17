"""On-demand regular-market prices with bounded, process-local refreshes."""
from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
import logging
import math
import threading
import time
from urllib.parse import quote as quote_path


log = logging.getLogger(__name__)


@dataclass(frozen=True)
class Quote:
    price: float
    asof: float | None


def parse_quote(price, asof=None):
    try:
        value = float(price)
        timestamp = float(asof) if asof is not None else None
        if math.isfinite(value) and value > 0:
            return Quote(value, timestamp if timestamp is not None and math.isfinite(timestamp) else None)
    except (TypeError, ValueError, OverflowError):
        pass
    return None


class YahooQuotes:
    """Batch regularMarketPrice; chart metadata fallback has the same meaning.

    yfinance's authenticated transport handles Yahoo cookies/crumbs. Downloading
    daily bars is deliberately not used as a substitute for a current quote.
    """

    def __init__(self, timeout=3.0, workers=8):
        if not math.isfinite(timeout) or timeout <= 0 or workers < 1:
            raise ValueError("Quote timeout and worker count must be positive")
        self.timeout = timeout
        self.workers = workers
        self.pool = ThreadPoolExecutor(max_workers=workers, thread_name_prefix="quote-symbol")

    def fetch(self, symbols, publish=lambda quotes: None):
        from yfinance.data import YfData

        transport = YfData()
        quotes = {}
        # Yahoo accepts multiple symbols in one HTTP request. Limit URL size.
        missing = []
        for start in range(0, len(symbols), 100):
            chunk = symbols[start:start + 100]
            try:
                payload = transport.get_raw_json(
                    "https://query1.finance.yahoo.com/v7/finance/quote",
                    params={"symbols": ",".join(chunk)}, timeout=self.timeout,
                )
                for row in payload.get("quoteResponse", {}).get("result", []):
                    symbol = str(row.get("symbol", "")).upper()
                    quote = parse_quote(row.get("regularMarketPrice"), row.get("regularMarketTime"))
                    if symbol in chunk and quote is not None:
                        quotes[symbol] = quote
            except Exception as exc:
                log.warning("Batch quote request failed (%s)", type(exc).__name__)
            missing.extend(symbol for symbol in chunk if symbol not in quotes)
            publish({s: quotes[s] for s in chunk if s in quotes})

        def chart(symbol):
            try:
                payload = transport.get_raw_json(
                    "https://query1.finance.yahoo.com/v8/finance/chart/" + quote_path(symbol, safe=""),
                    params={"range": "1d", "interval": "1d"}, timeout=self.timeout,
                )
                meta = payload["chart"]["result"][0]["meta"]
                return parse_quote(meta.get("regularMarketPrice"), meta.get("regularMarketTime"))
            except Exception:
                return None

        # Submit at most one worker-sized wave, avoiding an unbounded task queue.
        for start in range(0, len(missing), self.workers):
            futures = {self.pool.submit(chart, symbol): symbol for symbol in missing[start:start + self.workers]}
            for future in as_completed(futures):
                symbol = futures[future]
                quote = future.result()
                if quote is not None:
                    quotes[symbol] = quote
                    publish({symbol: quote})
        return quotes


class PriceCache:
    """One in-flight refresh per process, including overlapping symbol sets.

    Callers wait at most wait_seconds. No expired value is returned on failure
    or timeout. Missing results are cached briefly to avoid outage stampedes.
    An outstanding refresh may finish after the caller's deadline; subsequent
    callers join it rather than creating more work.
    """

    def __init__(self, provider, ttl=15.0, retry_seconds=3.0, wait_seconds=5.0, clock=time.monotonic):
        if not all(math.isfinite(v) and v > 0 for v in (ttl, retry_seconds, wait_seconds)):
            raise ValueError("Live-price cache durations must be finite and positive")
        self.provider = provider
        self.ttl = ttl
        self.retry_seconds = retry_seconds
        self.wait_seconds = wait_seconds
        self.clock = clock
        self.entries = {}
        self.condition = threading.Condition()
        self.inflight = None
        self.pool = ThreadPoolExecutor(max_workers=1, thread_name_prefix="quote-refresh")

    def _refresh(self, symbols):
        quotes = {}
        published = set()

        def publish(values):
            published.update(values)
            self._publish(values)

        try:
            quotes = self.provider.fetch(symbols, publish)
        except Exception as exc:
            log.warning("Live-price refresh failed (%s)", type(exc).__name__)
            quotes = {}
        finally:
            with self.condition:
                now = self.clock()
                self.entries = {s: entry for s, entry in self.entries.items() if entry[1] > now}
                for symbol in symbols:
                    quote = quotes.get(symbol)
                    # Preserve the actual receipt time of incrementally published quotes.
                    if (quote is None or symbol in published) and symbol not in self.entries:
                        self.entries[symbol] = (None, now + self.retry_seconds)
                    elif quote is not None and symbol not in self.entries:
                        self.entries[symbol] = (quote, now + self.ttl)
                self.inflight = None
                self.condition.notify_all()

    def _publish(self, quotes):
        with self.condition:
            now = self.clock()
            for symbol, quote in quotes.items():
                self.entries[symbol] = (quote, now + self.ttl)
            self.condition.notify_all()

    def get_quotes(self, symbols):
        symbols = sorted({str(s).strip().upper() for s in symbols if str(s).strip()})
        deadline = time.monotonic() + self.wait_seconds
        with self.condition:
            while symbols:
                now = self.clock()
                missing = [s for s in symbols if s not in self.entries or self.entries[s][1] <= now]
                if not missing:
                    break
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    break
                if self.inflight is None:
                    self.inflight = tuple(missing)
                    self.pool.submit(self._refresh, missing)
                self.condition.wait(remaining)
            now = self.clock()
            return {s: self.entries[s][0] if s in self.entries and self.entries[s][1] > now else None for s in symbols}

    def get_prices(self, symbols):
        return {s: quote.price if quote else None for s, quote in self.get_quotes(symbols).items()}
