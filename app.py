from __future__ import annotations

import calendar
import csv
import io
import json
import os
import re
import threading
from datetime import date, datetime, timedelta, timezone
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen
from typing import Dict, List, Tuple

import pandas as pd
from flask import Flask, Response, jsonify, request
import matplotlib.pyplot as plt

# Load environment variables from a .env file in the same directory (if present)
try:
    from dotenv import load_dotenv

    # Try the script directory first, then CWD
    _DOTENV_LOADED = load_dotenv(os.path.join(os.path.dirname(__file__), ".env")) or load_dotenv()
except Exception:
    _DOTENV_LOADED = False

# Try to import yfinance gracefully, with a friendly error later if missing
try:
    import yfinance as yf
except Exception:  # pragma: no cover
    yf = None

# Try to import psycopg2 gracefully, with a friendly error later if missing
try:
    import psycopg2
    from psycopg2.extras import execute_values
except Exception:  # pragma: no cover
    psycopg2 = None
    execute_values = None

app = Flask(__name__)

_RAW_DB_TABLE = os.environ.get("STOCK_TRACKER_TABLE", "stock_forecasts")
if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", _RAW_DB_TABLE):
    raise RuntimeError("STOCK_TRACKER_TABLE must be a valid SQL identifier (letters, numbers, underscore).")
DB_TABLE = _RAW_DB_TABLE
_RAW_EARNINGS_TABLE = os.environ.get("STOCK_TRACKER_EARNINGS_TABLE", "earnings_calendar")
if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", _RAW_EARNINGS_TABLE):
    raise RuntimeError("STOCK_TRACKER_EARNINGS_TABLE must be a valid SQL identifier (letters, numbers, underscore).")
EARNINGS_TABLE = _RAW_EARNINGS_TABLE
DATABASE_URL = os.environ.get(
    "DATABASE_URL",
    "postgresql://stock_user:stock_pass@localhost:5432/stock_tracker",
)
CSV_HEADERS = ["symbol", "forecast_price", "updated_date"]
EXTENDED_HEADERS = CSV_HEADERS + ["reward_score", "risk_score", "confidence_score", "classification"]
_LOCK = threading.Lock()
ALLOWED_CLASSIFICATIONS = {"buy", "hold/watch", "sell"}
DEFAULT_SCORE = 5.0
DEFAULT_CLASSIFICATION = "hold/watch"
ALPACA_DATA_BASE_URL = os.environ.get("ALPACA_DATA_BASE_URL", "https://data.alpaca.markets").rstrip("/")
ALPACA_API_KEY_ID = os.environ.get("APCA-API-KEY-ID") or os.environ.get("ALPACA_API_KEY_ID")
ALPACA_API_SECRET_KEY = os.environ.get("APCA-API-SECRET-KEY") or os.environ.get("ALPACA_API_SECRET_KEY")
DEFAULT_CHART_TIMEFRAME = os.environ.get("CHART_DEFAULT_TIMEFRAME", "1Day")
DEFAULT_CHART_LOOKBACK_DAYS = int(os.environ.get("CHART_LOOKBACK_DAYS", "180"))
CHART_DELAY_MINUTES = int(os.environ.get("CHART_DELAY_MINUTES", os.environ.get("CHART_DELAY", "20")))
SUPPORTED_TIMEFRAMES = {"1Min", "5Min", "15Min", "1Hour", "1Day"}
TIMEFRAME_DELTAS = {
    "1Min": timedelta(minutes=1),
    "5Min": timedelta(minutes=5),
    "15Min": timedelta(minutes=15),
    "1Hour": timedelta(hours=1),
    "1Day": timedelta(days=1),
}


def _opportunity_score(reward_score: float, risk_score: float, confidence_score: float) -> float:
    return (reward_score * 4.0) + ((10.0 - risk_score) * 4.0) + (confidence_score * 2.0)


def _get_conn():
    if psycopg2 is None:
        raise RuntimeError("Missing dependency: psycopg2-binary is required for PostgreSQL access.")
    return psycopg2.connect(DATABASE_URL)


def _init_db_if_needed() -> None:
    with _LOCK:
        with _get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"""
                    CREATE TABLE IF NOT EXISTS {DB_TABLE} (
                        symbol TEXT PRIMARY KEY,
                        forecast_price DOUBLE PRECISION NOT NULL,
                        updated_date DATE NOT NULL,
                        reward_score DOUBLE PRECISION NOT NULL DEFAULT {DEFAULT_SCORE},
                        risk_score DOUBLE PRECISION NOT NULL DEFAULT {DEFAULT_SCORE},
                        confidence_score DOUBLE PRECISION NOT NULL DEFAULT {DEFAULT_SCORE},
                        classification TEXT NOT NULL DEFAULT '{DEFAULT_CLASSIFICATION}'
                    )
                    """
                )
                cur.execute(
                    f"ALTER TABLE {DB_TABLE} ADD COLUMN IF NOT EXISTS reward_score DOUBLE PRECISION NOT NULL DEFAULT {DEFAULT_SCORE}"
                )
                cur.execute(
                    f"ALTER TABLE {DB_TABLE} ADD COLUMN IF NOT EXISTS risk_score DOUBLE PRECISION NOT NULL DEFAULT {DEFAULT_SCORE}"
                )
                cur.execute(
                    f"ALTER TABLE {DB_TABLE} ADD COLUMN IF NOT EXISTS confidence_score DOUBLE PRECISION NOT NULL DEFAULT {DEFAULT_SCORE}"
                )
                cur.execute(
                    f"ALTER TABLE {DB_TABLE} ADD COLUMN IF NOT EXISTS classification TEXT NOT NULL DEFAULT '{DEFAULT_CLASSIFICATION}'"
                )
                cur.execute(
                    f"""
                    CREATE TABLE IF NOT EXISTS {EARNINGS_TABLE} (
                        symbol TEXT NOT NULL,
                        earnings_date DATE NOT NULL,
                        earnings_time TEXT,
                        eps_estimate DOUBLE PRECISION,
                        eps_actual DOUBLE PRECISION,
                        revenue_estimate DOUBLE PRECISION,
                        revenue_actual DOUBLE PRECISION,
                        PRIMARY KEY (symbol, earnings_date)
                    )
                    """
                )
                cur.execute(
                    """
                    CREATE TABLE IF NOT EXISTS symbol_metadata (
                        id BIGSERIAL PRIMARY KEY,
                        symbol TEXT NOT NULL UNIQUE,
                        exchange TEXT,
                        asset_class TEXT NOT NULL DEFAULT 'us_equity',
                        name TEXT,
                        is_active BOOLEAN NOT NULL DEFAULT TRUE,
                        created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                        updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                        CHECK (symbol = upper(symbol))
                    )
                    """
                )
                cur.execute(
                    """
                    CREATE TABLE IF NOT EXISTS symbol_sync_state (
                        symbol_id BIGINT PRIMARY KEY REFERENCES symbol_metadata(id) ON DELETE CASCADE,
                        last_synced_at TIMESTAMPTZ,
                        last_successful_sync_at TIMESTAMPTZ,
                        last_quote_synced_at TIMESTAMPTZ,
                        last_bar_synced_at TIMESTAMPTZ,
                        latest_bar_time TIMESTAMPTZ,
                        latest_quote_time TIMESTAMPTZ,
                        sync_status TEXT NOT NULL DEFAULT 'idle',
                        sync_error TEXT,
                        updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                        CHECK (sync_status IN ('idle', 'running', 'error'))
                    )
                    """
                )
                cur.execute(
                    """
                    CREATE TABLE IF NOT EXISTS stock_bars (
                        symbol_id BIGINT NOT NULL REFERENCES symbol_metadata(id) ON DELETE CASCADE,
                        timeframe TEXT NOT NULL,
                        bar_time TIMESTAMPTZ NOT NULL,
                        open DOUBLE PRECISION NOT NULL,
                        high DOUBLE PRECISION NOT NULL,
                        low DOUBLE PRECISION NOT NULL,
                        close DOUBLE PRECISION NOT NULL,
                        volume BIGINT NOT NULL,
                        trade_count BIGINT,
                        vwap DOUBLE PRECISION,
                        source TEXT NOT NULL DEFAULT 'alpaca',
                        ingested_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                        PRIMARY KEY (symbol_id, timeframe, bar_time)
                    )
                    """
                )
                cur.execute(
                    """
                    CREATE TABLE IF NOT EXISTS latest_quotes (
                        symbol_id BIGINT PRIMARY KEY REFERENCES symbol_metadata(id) ON DELETE CASCADE,
                        quote_time TIMESTAMPTZ NOT NULL,
                        bid_price DOUBLE PRECISION,
                        bid_size BIGINT,
                        ask_price DOUBLE PRECISION,
                        ask_size BIGINT,
                        bid_exchange TEXT,
                        ask_exchange TEXT,
                        conditions JSONB,
                        tape TEXT,
                        source TEXT NOT NULL DEFAULT 'alpaca',
                        ingested_at TIMESTAMPTZ NOT NULL DEFAULT now()
                    )
                    """
                )
                cur.execute(
                    """
                    CREATE TABLE IF NOT EXISTS latest_bars (
                        symbol_id BIGINT PRIMARY KEY REFERENCES symbol_metadata(id) ON DELETE CASCADE,
                        timeframe TEXT NOT NULL DEFAULT '1Min',
                        bar_time TIMESTAMPTZ NOT NULL,
                        open DOUBLE PRECISION NOT NULL,
                        high DOUBLE PRECISION NOT NULL,
                        low DOUBLE PRECISION NOT NULL,
                        close DOUBLE PRECISION NOT NULL,
                        volume BIGINT NOT NULL,
                        trade_count BIGINT,
                        vwap DOUBLE PRECISION,
                        source TEXT NOT NULL DEFAULT 'alpaca',
                        ingested_at TIMESTAMPTZ NOT NULL DEFAULT now()
                    )
                    """
                )
                cur.execute(
                    "CREATE INDEX IF NOT EXISTS idx_symbol_metadata_symbol ON symbol_metadata (symbol)"
                )
                cur.execute(
                    "CREATE INDEX IF NOT EXISTS idx_stock_bars_symbol_tf_time ON stock_bars (symbol_id, timeframe, bar_time DESC)"
                )
                cur.execute(
                    "CREATE INDEX IF NOT EXISTS idx_stock_bars_timeframe_time_symbol ON stock_bars (timeframe, bar_time DESC, symbol_id)"
                )


def _read_df() -> pd.DataFrame:
    _init_db_if_needed()
    with _get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"""
                SELECT symbol, forecast_price, updated_date::TEXT,
                       reward_score, risk_score, confidence_score, classification
                FROM {DB_TABLE}
                ORDER BY symbol ASC
                """
            )
            rows = cur.fetchall()

    if not rows:
        return pd.DataFrame(columns=EXTENDED_HEADERS)

    df = pd.DataFrame(rows, columns=EXTENDED_HEADERS)
    df["symbol"] = df["symbol"].astype(str).str.upper().str.strip()
    df["forecast_price"] = pd.to_numeric(df["forecast_price"], errors="coerce")
    df["updated_date"] = df["updated_date"].astype(str)
    df["reward_score"] = pd.to_numeric(df["reward_score"], errors="coerce")
    df["risk_score"] = pd.to_numeric(df["risk_score"], errors="coerce")
    df["confidence_score"] = pd.to_numeric(df["confidence_score"], errors="coerce")
    df["classification"] = df["classification"].astype(str).str.strip().str.lower()
    return df


def _score_field(row: Dict, key: str, symbol: str) -> float:
    raw = row.get(key, DEFAULT_SCORE)
    try:
        val = float(raw)
    except Exception as exc:
        raise ValueError(f"Invalid {key} for symbol {symbol}; expected a number between 0 and 10.") from exc
    if val < 0 or val > 10:
        raise ValueError(f"Invalid {key} for symbol {symbol}; expected a number between 0 and 10.")
    return val


def _normalize_row(row: Dict) -> Tuple[str, float, str, float, float, float, str]:
    symbol = str(row.get("symbol", "")).strip().upper()
    if not symbol:
        raise ValueError("Each row requires a symbol.")

    try:
        forecast_price = float(row.get("forecast_price"))
    except Exception as exc:
        raise ValueError(f"Invalid forecast price for symbol {symbol}.") from exc

    updated_date = str(row.get("updated_date") or date.today().isoformat()).strip()
    try:
        date.fromisoformat(updated_date)
    except Exception as exc:
        raise ValueError(f"Invalid updated_date for symbol {symbol}; expected YYYY-MM-DD.") from exc

    reward_score = _score_field(row, "reward_score", symbol)
    risk_score = _score_field(row, "risk_score", symbol)
    confidence_score = _score_field(row, "confidence_score", symbol)

    classification = str(row.get("classification", DEFAULT_CLASSIFICATION)).strip().lower()
    if classification not in ALLOWED_CLASSIFICATIONS:
        raise ValueError(f"Invalid classification for symbol {symbol}; expected one of: buy, hold/watch, sell.")

    return (symbol, forecast_price, updated_date, reward_score, risk_score, confidence_score, classification)


def _upsert_stocks(rows: List[Dict]) -> int:
    """Upsert one or more stock rows into PostgreSQL."""
    _init_db_if_needed()
    payload = [_normalize_row(r) for r in rows]

    with _get_conn() as conn:
        with conn.cursor() as cur:
            execute_values(
                cur,
                f"""
                INSERT INTO {DB_TABLE}
                    (symbol, forecast_price, updated_date, reward_score, risk_score, confidence_score, classification)
                VALUES %s
                ON CONFLICT (symbol)
                DO UPDATE SET
                    forecast_price = EXCLUDED.forecast_price,
                    updated_date = EXCLUDED.updated_date,
                    reward_score = EXCLUDED.reward_score,
                    risk_score = EXCLUDED.risk_score,
                    confidence_score = EXCLUDED.confidence_score,
                    classification = EXCLUDED.classification
                """,
                payload,
            )
    return len(payload)


def _parse_optional_float(value) -> float | None:
    if value in (None, "", "null"):
        return None
    try:
        return float(value)
    except Exception:
        return None


def _fetch_earnings_rows(to_date: str, api_key: str) -> List[Tuple[str, str, str, float | None, float | None, float | None, float | None]]:
    if not api_key:
        raise ValueError("FMP_API_KEY is required.")

    params = urlencode({"to": to_date, "apikey": api_key})
    url = f"https://financialmodelingprep.com/stable/earnings-calendar?{params}"
    req = Request(url, headers={"User-Agent": "stock-tracker/1.0"})
    with urlopen(req, timeout=30) as resp:
        payload = json.loads(resp.read().decode("utf-8"))

    if not isinstance(payload, list):
        raise RuntimeError("Unexpected earnings API response format.")

    rows = []
    for item in payload:
        if not isinstance(item, dict):
            continue
        symbol = str(item.get("symbol", "")).strip().upper()
        earnings_date = str(item.get("date", "")).strip()
        if not symbol or not earnings_date:
            continue
        date.fromisoformat(earnings_date)
        rows.append(
            (
                symbol,
                earnings_date,
                str(item.get("time", "") or "").strip(),
                _parse_optional_float(item.get("epsEstimated", item.get("epsEstimate"))),
                _parse_optional_float(item.get("epsActual")),
                _parse_optional_float(item.get("revenueEstimated", item.get("revenueEstimate"))),
                _parse_optional_float(item.get("revenueActual")),
            )
        )
    return rows


def _replace_earnings_rows(rows: List[Tuple[str, str, str, float | None, float | None, float | None, float | None]], to_date: str) -> int:
    _init_db_if_needed()
    with _get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(f"DELETE FROM {EARNINGS_TABLE} WHERE earnings_date <= %s", (to_date,))
            if rows:
                execute_values(
                    cur,
                    f"""
                    INSERT INTO {EARNINGS_TABLE}
                        (symbol, earnings_date, earnings_time, eps_estimate, eps_actual, revenue_estimate, revenue_actual)
                    VALUES %s
                    ON CONFLICT (symbol, earnings_date)
                    DO UPDATE SET
                        earnings_time = EXCLUDED.earnings_time,
                        eps_estimate = EXCLUDED.eps_estimate,
                        eps_actual = EXCLUDED.eps_actual,
                        revenue_estimate = EXCLUDED.revenue_estimate,
                        revenue_actual = EXCLUDED.revenue_actual
                    """,
                    rows,
                )
    return len(rows)


# -------- Price fetching (via yfinance) ---------

def _fetch_current_prices(symbols: List[str]) -> Dict[str, float | None]:
    """Return a mapping of symbol -> last price (float) or None if unavailable.
    Uses yfinance Tickers batch when possible for efficiency.
    """
    out: Dict[str, float | None] = {s: None for s in symbols}

    if not symbols:
        return out

    if yf is None:
        return out

    try:
        # Batch fetch via Tickers
        tickers = yf.Tickers(" ".join(symbols))
        for sym, t in tickers.tickers.items():
            sym_u = sym.upper()
            price = None
            try:
                fi = getattr(t, "fast_info", None)
                if fi is not None:
                    price = fi.get("last_price") or fi.get("last_price")
                if price is None:
                    hist = t.history(period="1d")
                    if not hist.empty:
                        price = float(hist["Close"].iloc[-1])
            except Exception:
                price = None
            out[sym_u] = float(price) if price is not None else None
    except Exception:
        for sym in symbols:
            price = None
            try:
                t = yf.Ticker(sym)
                fi = getattr(t, "fast_info", None)
                if fi is not None:
                    price = fi.get("last_price")
                if price is None:
                    hist = t.history(period="1d")
                    if not hist.empty:
                        price = float(hist["Close"].iloc[-1])
            except Exception:
                price = None
            out[sym] = float(price) if price is not None else None

    return out


# ----------------- Next earnings helper ----------------------

def _load_next_earnings_map() -> Dict[str, str]:
    """Return mapping SYMBOL -> next earnings date (YYYY-MM-DD) from PostgreSQL."""
    _init_db_if_needed()
    with _get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"""
                SELECT DISTINCT ON (symbol) symbol, earnings_date::TEXT
                FROM {EARNINGS_TABLE}
                WHERE eps_actual IS NULL
                ORDER BY symbol ASC, earnings_date ASC
                """
            )
            rows = cur.fetchall()
    return {str(symbol).upper(): str(earnings_date) for symbol, earnings_date in rows}


def _add_months(d: date, months: int) -> date:
    """Return a date shifted by N calendar months, clamping day to month end."""
    month_index = (d.month - 1) + months
    year = d.year + (month_index // 12)
    month = (month_index % 12) + 1
    day = min(d.day, calendar.monthrange(year, month)[1])
    return date(year, month, day)


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _to_alpaca_ts(value: datetime) -> str:
    return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def _parse_market_timestamp(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        return datetime.fromisoformat(str(value).replace("Z", "+00:00")).astimezone(timezone.utc)
    except Exception:
        return None


def _alpaca_headers() -> Dict[str, str]:
    if not ALPACA_API_KEY_ID or not ALPACA_API_SECRET_KEY:
        raise RuntimeError("Missing Alpaca credentials. Set APCA-API-KEY-ID and APCA-API-SECRET-KEY in .env.")
    return {
        "APCA-API-KEY-ID": ALPACA_API_KEY_ID,
        "APCA-API-SECRET-KEY": ALPACA_API_SECRET_KEY,
        "User-Agent": "stock-tracker/1.0",
    }


def _alpaca_get_json(path: str, params: Dict[str, str | int | None]) -> Dict:
    query = urlencode({k: v for k, v in params.items() if v not in (None, "")}, doseq=True)
    url = f"{ALPACA_DATA_BASE_URL}{path}"
    if query:
        url = f"{url}?{query}"

    req = Request(url, headers=_alpaca_headers())
    try:
        with urlopen(req, timeout=30) as resp:
            payload = json.loads(resp.read().decode("utf-8"))
    except HTTPError as exc:
        detail = exc.read().decode("utf-8", errors="replace")
        raise RuntimeError(f"Alpaca request failed ({exc.code}): {detail}") from exc
    except URLError as exc:
        raise RuntimeError(f"Unable to reach Alpaca: {exc}") from exc

    if not isinstance(payload, dict):
        raise RuntimeError("Unexpected Alpaca response format.")
    return payload


def _ensure_symbol_metadata(symbol: str) -> int:
    normalized_symbol = str(symbol).strip().upper()
    if not normalized_symbol:
        raise ValueError("Symbol is required.")

    _init_db_if_needed()
    with _get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO symbol_metadata (symbol)
                VALUES (%s)
                ON CONFLICT (symbol)
                DO UPDATE SET updated_at = now()
                RETURNING id
                """,
                (normalized_symbol,),
            )
            symbol_id = int(cur.fetchone()[0])
            cur.execute(
                """
                INSERT INTO symbol_sync_state (symbol_id)
                VALUES (%s)
                ON CONFLICT (symbol_id) DO NOTHING
                """,
                (symbol_id,),
            )
    return symbol_id


def _extract_symbol_bars(payload: Dict, symbol: str) -> List[Dict]:
    bars_container = payload.get("bars")
    if isinstance(bars_container, dict):
        symbol_bars = bars_container.get(symbol)
        if isinstance(symbol_bars, list):
            return symbol_bars
        if isinstance(symbol_bars, dict):
            return [symbol_bars]
    if isinstance(bars_container, list):
        return bars_container
    return []


def _fetch_alpaca_bars(symbol: str, timeframe: str, start: datetime, end: datetime) -> List[Dict]:
    if timeframe not in SUPPORTED_TIMEFRAMES:
        raise ValueError(f"Unsupported timeframe '{timeframe}'.")

    symbol = symbol.upper()
    page_token = None
    bars: List[Dict] = []

    while True:
        payload = _alpaca_get_json(
            "/v2/stocks/bars",
            {
                "symbols": symbol,
                "timeframe": timeframe,
                "start": _to_alpaca_ts(start),
                "end": _to_alpaca_ts(end),
                "adjustment": "raw",
                "limit": 10000,
                "page_token": page_token,
            },
        )
        bars.extend(_extract_symbol_bars(payload, symbol))
        page_token = payload.get("next_page_token")
        if not page_token:
            break

    return bars


def _fetch_alpaca_latest_quote(symbol: str) -> Dict | None:
    symbol = symbol.upper()
    payload = _alpaca_get_json("/v2/stocks/quotes/latest", {"symbols": symbol})
    quotes_container = payload.get("quotes")
    if isinstance(quotes_container, dict):
        quote = quotes_container.get(symbol)
        if isinstance(quote, dict):
            return quote
    if isinstance(payload.get("quote"), dict):
        return payload["quote"]
    return None


def _fetch_alpaca_latest_bar(symbol: str) -> Dict | None:
    symbol = symbol.upper()
    payload = _alpaca_get_json("/v2/stocks/bars/latest", {"symbols": symbol})
    bars_container = payload.get("bars")
    if isinstance(bars_container, dict):
        bar = bars_container.get(symbol)
        if isinstance(bar, dict):
            return bar
    if isinstance(payload.get("bar"), dict):
        return payload["bar"]
    return None


def _normalize_bar_row(symbol_id: int, timeframe: str, payload: Dict) -> Tuple:
    bar_time = _parse_market_timestamp(payload.get("t"))
    if bar_time is None:
        raise ValueError("Alpaca bar is missing a valid timestamp.")
    return (
        symbol_id,
        timeframe,
        bar_time,
        float(payload.get("o", 0.0)),
        float(payload.get("h", 0.0)),
        float(payload.get("l", 0.0)),
        float(payload.get("c", 0.0)),
        int(payload.get("v", 0) or 0),
        int(payload.get("n", 0) or 0) if payload.get("n") is not None else None,
        float(payload.get("vw")) if payload.get("vw") is not None else None,
    )


def _upsert_stock_bars(symbol_id: int, timeframe: str, bars: List[Dict]) -> int:
    if not bars:
        return 0

    payload = [_normalize_bar_row(symbol_id, timeframe, item) for item in bars]
    with _get_conn() as conn:
        with conn.cursor() as cur:
            execute_values(
                cur,
                """
                INSERT INTO stock_bars
                    (symbol_id, timeframe, bar_time, open, high, low, close, volume, trade_count, vwap)
                VALUES %s
                ON CONFLICT (symbol_id, timeframe, bar_time)
                DO UPDATE SET
                    open = EXCLUDED.open,
                    high = EXCLUDED.high,
                    low = EXCLUDED.low,
                    close = EXCLUDED.close,
                    volume = EXCLUDED.volume,
                    trade_count = EXCLUDED.trade_count,
                    vwap = EXCLUDED.vwap,
                    ingested_at = now()
                """,
                payload,
            )
    return len(payload)


def _upsert_latest_quote(symbol_id: int, payload: Dict | None) -> Dict | None:
    if not payload:
        return None
    quote_time = _parse_market_timestamp(payload.get("t"))
    if quote_time is None:
        return None

    normalized = {
        "t": quote_time.isoformat(),
        "bid_price": float(payload.get("bp")) if payload.get("bp") is not None else None,
        "bid_size": int(payload.get("bs", 0) or 0) if payload.get("bs") is not None else None,
        "ask_price": float(payload.get("ap")) if payload.get("ap") is not None else None,
        "ask_size": int(payload.get("as", 0) or 0) if payload.get("as") is not None else None,
        "bid_exchange": str(payload.get("bx")) if payload.get("bx") is not None else None,
        "ask_exchange": str(payload.get("ax")) if payload.get("ax") is not None else None,
        "conditions": payload.get("c"),
        "tape": str(payload.get("z")) if payload.get("z") is not None else None,
    }

    with _get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO latest_quotes
                    (symbol_id, quote_time, bid_price, bid_size, ask_price, ask_size, bid_exchange, ask_exchange, conditions, tape)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb, %s)
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
                    ingested_at = now()
                WHERE latest_quotes.quote_time <= EXCLUDED.quote_time
                """,
                (
                    symbol_id,
                    quote_time,
                    normalized["bid_price"],
                    normalized["bid_size"],
                    normalized["ask_price"],
                    normalized["ask_size"],
                    normalized["bid_exchange"],
                    normalized["ask_exchange"],
                    json.dumps(normalized["conditions"]) if normalized["conditions"] is not None else None,
                    normalized["tape"],
                ),
            )
    return normalized


def _upsert_latest_bar(symbol_id: int, payload: Dict | None) -> Dict | None:
    if not payload:
        return None
    bar_time = _parse_market_timestamp(payload.get("t"))
    if bar_time is None:
        return None

    normalized = {
        "t": bar_time.isoformat(),
        "o": float(payload.get("o", 0.0)),
        "h": float(payload.get("h", 0.0)),
        "l": float(payload.get("l", 0.0)),
        "c": float(payload.get("c", 0.0)),
        "v": int(payload.get("v", 0) or 0),
        "n": int(payload.get("n", 0) or 0) if payload.get("n") is not None else None,
        "vw": float(payload.get("vw")) if payload.get("vw") is not None else None,
    }

    with _get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO latest_bars
                    (symbol_id, timeframe, bar_time, open, high, low, close, volume, trade_count, vwap)
                VALUES (%s, '1Min', %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (symbol_id)
                DO UPDATE SET
                    timeframe = EXCLUDED.timeframe,
                    bar_time = EXCLUDED.bar_time,
                    open = EXCLUDED.open,
                    high = EXCLUDED.high,
                    low = EXCLUDED.low,
                    close = EXCLUDED.close,
                    volume = EXCLUDED.volume,
                    trade_count = EXCLUDED.trade_count,
                    vwap = EXCLUDED.vwap,
                    ingested_at = now()
                WHERE latest_bars.bar_time <= EXCLUDED.bar_time
                """,
                (
                    symbol_id,
                    bar_time,
                    normalized["o"],
                    normalized["h"],
                    normalized["l"],
                    normalized["c"],
                    normalized["v"],
                    normalized["n"],
                    normalized["vw"],
                ),
            )
    return normalized


def _get_market_snapshot(symbol: str, timeframe: str, limit: int) -> Dict:
    symbol = str(symbol).strip().upper()
    symbol_id = _ensure_symbol_metadata(symbol)
    limit = max(1, min(limit, 5000))

    _init_db_if_needed()
    with _get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT bar_time, open, high, low, close, volume
                FROM (
                    SELECT bar_time, open, high, low, close, volume
                    FROM stock_bars
                    WHERE symbol_id = %s AND timeframe = %s
                    ORDER BY bar_time DESC
                    LIMIT %s
                ) bars
                ORDER BY bar_time ASC
                """,
                (symbol_id, timeframe, limit),
            )
            bars = [
                {
                    "t": bar_time.astimezone(timezone.utc).isoformat(),
                    "o": float(open_),
                    "h": float(high),
                    "l": float(low),
                    "c": float(close),
                    "v": int(volume),
                }
                for bar_time, open_, high, low, close, volume in cur.fetchall()
            ]

            cur.execute(
                """
                SELECT last_synced_at, last_successful_sync_at, last_quote_synced_at, last_bar_synced_at,
                       latest_bar_time, latest_quote_time, sync_status, sync_error
                FROM symbol_sync_state
                WHERE symbol_id = %s
                """,
                (symbol_id,),
            )
            state_row = cur.fetchone()

            cur.execute(
                """
                SELECT quote_time, bid_price, bid_size, ask_price, ask_size
                FROM latest_quotes
                WHERE symbol_id = %s
                """,
                (symbol_id,),
            )
            quote_row = cur.fetchone()

            cur.execute(
                """
                SELECT bar_time, open, high, low, close, volume
                FROM latest_bars
                WHERE symbol_id = %s
                """,
                (symbol_id,),
            )
            latest_bar_row = cur.fetchone()

    sync_state = {
        "last_synced_at": state_row[0].astimezone(timezone.utc).isoformat() if state_row and state_row[0] else None,
        "last_successful_sync_at": state_row[1].astimezone(timezone.utc).isoformat() if state_row and state_row[1] else None,
        "last_quote_synced_at": state_row[2].astimezone(timezone.utc).isoformat() if state_row and state_row[2] else None,
        "last_bar_synced_at": state_row[3].astimezone(timezone.utc).isoformat() if state_row and state_row[3] else None,
        "latest_bar_time": state_row[4].astimezone(timezone.utc).isoformat() if state_row and state_row[4] else None,
        "latest_quote_time": state_row[5].astimezone(timezone.utc).isoformat() if state_row and state_row[5] else None,
        "sync_status": state_row[6] if state_row else "idle",
        "sync_error": state_row[7] if state_row else None,
    }

    latest_quote = None
    if quote_row:
        latest_quote = {
            "t": quote_row[0].astimezone(timezone.utc).isoformat(),
            "bid_price": float(quote_row[1]) if quote_row[1] is not None else None,
            "bid_size": int(quote_row[2]) if quote_row[2] is not None else None,
            "ask_price": float(quote_row[3]) if quote_row[3] is not None else None,
            "ask_size": int(quote_row[4]) if quote_row[4] is not None else None,
        }

    latest_bar = None
    if latest_bar_row:
        latest_bar = {
            "t": latest_bar_row[0].astimezone(timezone.utc).isoformat(),
            "o": float(latest_bar_row[1]),
            "h": float(latest_bar_row[2]),
            "l": float(latest_bar_row[3]),
            "c": float(latest_bar_row[4]),
            "v": int(latest_bar_row[5]),
        }

    return {
        "symbol": symbol,
        "timeframe": timeframe,
        "bars": bars,
        "bar_count": len(bars),
        "latest_quote": latest_quote,
        "latest_bar": latest_bar,
        "sync_state": sync_state,
    }


def _sync_market_data(symbol: str, timeframe: str, force_full: bool = False) -> Dict:
    symbol = str(symbol).strip().upper()
    if timeframe not in SUPPORTED_TIMEFRAMES:
        raise ValueError(f"Unsupported timeframe '{timeframe}'.")

    symbol_id = _ensure_symbol_metadata(symbol)
    now = _utcnow()
    delayed_now = now - timedelta(minutes=max(CHART_DELAY_MINUTES, 0))

    with _get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE symbol_sync_state
                SET sync_status = 'running', sync_error = NULL, updated_at = now()
                WHERE symbol_id = %s
                """,
                (symbol_id,),
            )
            cur.execute(
                """
                SELECT max(bar_time)
                FROM stock_bars
                WHERE symbol_id = %s AND timeframe = %s
                """,
                (symbol_id, timeframe),
            )
            row = cur.fetchone()
    latest_historical_bar_time = row[0] if row else None

    start = delayed_now - timedelta(days=DEFAULT_CHART_LOOKBACK_DAYS)
    if latest_historical_bar_time and not force_full:
        start = latest_historical_bar_time.astimezone(timezone.utc) + TIMEFRAME_DELTAS.get(timeframe, timedelta(days=1))
    end = delayed_now

    bars_inserted = 0
    try:
        if start <= end:
            historical_bars = _fetch_alpaca_bars(symbol, timeframe, start, end)
            bars_inserted = _upsert_stock_bars(symbol_id, timeframe, historical_bars)
        else:
            historical_bars = []

        latest_quote = _upsert_latest_quote(symbol_id, _fetch_alpaca_latest_quote(symbol))
        latest_bar = _upsert_latest_bar(symbol_id, _fetch_alpaca_latest_bar(symbol))

        with _get_conn() as conn:
            with conn.cursor() as cur:
                effective_latest_bar_time = _parse_market_timestamp(latest_bar.get("t")) if latest_bar else None
                if not effective_latest_bar_time and historical_bars:
                    effective_latest_bar_time = _parse_market_timestamp(historical_bars[-1].get("t"))
                effective_latest_quote_time = _parse_market_timestamp(latest_quote.get("t")) if latest_quote else None
                cur.execute(
                    """
                    UPDATE symbol_sync_state
                    SET last_synced_at = now(),
                        last_successful_sync_at = now(),
                        last_quote_synced_at = CASE WHEN %s IS NOT NULL THEN now() ELSE last_quote_synced_at END,
                        last_bar_synced_at = CASE WHEN %s IS NOT NULL OR %s > 0 THEN now() ELSE last_bar_synced_at END,
                        latest_bar_time = COALESCE(%s, latest_bar_time),
                        latest_quote_time = COALESCE(%s, latest_quote_time),
                        sync_status = 'idle',
                        sync_error = NULL,
                        updated_at = now()
                    WHERE symbol_id = %s
                    """,
                    (
                        latest_quote is not None,
                        latest_bar is not None,
                        bars_inserted,
                        effective_latest_bar_time,
                        effective_latest_quote_time,
                        symbol_id,
                    ),
                )
    except Exception as exc:
        with _get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE symbol_sync_state
                    SET sync_status = 'error',
                        sync_error = %s,
                        updated_at = now()
                    WHERE symbol_id = %s
                    """,
                    (str(exc)[:1000], symbol_id),
                )
        raise

    result = _get_market_snapshot(symbol, timeframe, 500)
    result["fetched_from_alpaca"] = True
    result["bars_inserted"] = bars_inserted
    return result


# ----------------- Routes ----------------------

@app.get("/")
def index() -> Response:
    earnings_default_date = _add_months(date.today(), 4).isoformat()
    html = f"""
<!doctype html>
<html lang=\"en\">
  <head>
    <meta charset=\"utf-8\" />
    <meta name=\"viewport\" content=\"width=device-width, initial-scale=1\" />
    <title>Stock Forecast Tracker</title>
    <script>
      let SORT_KEY = 'opportunity_score';
      let SORT_ASC = false;
      let ROWS = [];

      async function fetchData() {{
        const res = await fetch('/data');
        const payload = await res.json();
        ROWS = payload.rows || [];
        renderTable();
        document.getElementById('last-refreshed').textContent = new Date().toLocaleString();
      }}

      function sortBy(key) {{
        if (SORT_KEY === key) {{ SORT_ASC = !SORT_ASC; }} else {{ SORT_KEY = key; SORT_ASC = true; }}
        renderTable();
      }}

      function byKey(a, b, key) {{
        const av = a[key];
        const bv = b[key];
        if (av == null && bv == null) return 0;
        if (av == null) return 1;
        if (bv == null) return -1;
        if (typeof av === 'number' && typeof bv === 'number') return av - bv;
        return String(av).localeCompare(String(bv));
      }}

      function renderTable() {{
        const data = [...ROWS].sort((a,b) => byKey(a,b,SORT_KEY) * (SORT_ASC ? 1 : -1));
        const tbody = document.getElementById('tbody');
        tbody.innerHTML = '';
        for (const r of data) {{
          const diff_dollar = r.diff_dollar == null ? '—' : r.diff_dollar.toLocaleString(undefined, {{minimumFractionDigits: 2, maximumFractionDigits: 2}});
          const diff_pct = r.diff_pct == null ? '—' : (r.diff_pct).toFixed(2) + '%';
          const current_price = r.current_price == null ? '—' : r.current_price.toLocaleString(undefined, {{minimumFractionDigits: 2, maximumFractionDigits: 2}});
          const forecast_price = r.forecast_price == null ? '—' : r.forecast_price.toLocaleString(undefined, {{minimumFractionDigits: 2, maximumFractionDigits: 2}});
          const next_earnings = (r.next_earnings == null || r.next_earnings === '') ? '—' : r.next_earnings;
          const colorClass = r.diff_dollar == null ? '' : (r.diff_dollar >= 0 ? 'pos' : 'neg');

          const tr = document.createElement('tr');
          tr.innerHTML = `
            <td><a href=\"/chart/${{r.symbol}}\" class=\"symbol-link\"><code>${{r.symbol}}</code></a></td>
            <td class=\"num\">${{forecast_price}}</td>
            <td class=\"num\">${{Number(r.reward_score).toFixed(1)}}</td>
            <td class=\"num\">${{Number(r.risk_score).toFixed(1)}}</td>
            <td class=\"num\">${{Number(r.confidence_score).toFixed(1)}}</td>
            <td>${{r.classification}}</td>
            <td class=\"num\">${{Number(r.opportunity_score).toFixed(1)}}</td>
            <td>${{r.updated_date || ''}}</td>
            <td>${{next_earnings}}</td>
            <td class=\"num\">${{current_price}}</td>
            <td class=\"num ${{colorClass}}\">${{diff_dollar}}</td>
            <td class=\"num ${{colorClass}}\">${{diff_pct}}</td>
          `;
          tbody.appendChild(tr);
        }}
      }}

      async function addOrUpdate(e) {{
        e.preventDefault();
        const symbol = document.getElementById('symbol').value.trim().toUpperCase();
        const forecast_price = parseFloat(document.getElementById('forecast_price').value);
        const updated_date = document.getElementById('updated_date').value;
        const reward_score = parseFloat(document.getElementById('reward_score').value);
        const risk_score = parseFloat(document.getElementById('risk_score').value);
        const confidence_score = parseFloat(document.getElementById('confidence_score').value);
        const classification = document.getElementById('classification').value;

        const res = await fetch('/add', {{
          method: 'POST',
          headers: {{ 'Content-Type': 'application/json' }},
          body: JSON.stringify({{
            symbol, forecast_price, updated_date, reward_score, risk_score, confidence_score, classification
          }})
        }});

        const payload = await res.json();
        if (!payload.ok) {{
          alert(payload.error || 'Failed to save');
          return;
        }}

        document.getElementById('form').reset();
        document.getElementById('reward_score').value = '5';
        document.getElementById('risk_score').value = '5';
        document.getElementById('confidence_score').value = '5';
        document.getElementById('classification').value = 'hold/watch';
        await fetchData();
      }}

      async function updateEarnings(e) {{
        e.preventDefault();
        const to = document.getElementById('earnings_to').value;
        const btn = document.getElementById('earnings_btn');
        btn.disabled = true;
        const old = btn.textContent;
        btn.textContent = 'Updating...';
        try {{
          const form = new FormData();
          form.append('to', to);
          const res = await fetch('/update-earnings', {{ method: 'POST', body: form }});
          const text = await res.text();
          const win = window.open('', '_blank');
          if (win) {{ win.document.write(text); win.document.close(); }}
          await fetchData();
        }} finally {{
          btn.disabled = false;
          btn.textContent = old;
        }}
      }}

      window.addEventListener('DOMContentLoaded', () => {{
        document.getElementById('form').addEventListener('submit', addOrUpdate);
        document.getElementById('refresh').addEventListener('click', fetchData);
        document.getElementById('earnings-form').addEventListener('submit', updateEarnings);
        fetchData();
      }});
    </script>
    <style>
      :root {{
        --bg: #0b0f14;
        --card: #0f1520;
        --muted: #aab8c5;
        --text: #e6edf3;
        --accent: #2e90fa;
        --good: #17b26a;
        --bad: #f04438;
        --border: #1f2a3a;
      }}
      html, body {{
        margin: 0;
        padding: 0;
        background: var(--bg);
        color: var(--text);
        font-family: ui-sans-serif, system-ui, -apple-system, Segoe UI, Roboto, Helvetica Neue, Arial;
      }}
      .wrap {{
        max-width: 1300px;
        margin: 24px auto;
        padding: 0 16px;
      }}
      .grid {{
        display: grid;
        gap: 16px;
        grid-template-columns: 320px 1fr;
      }}
      @media (max-width: 900px) {{ .grid {{ grid-template-columns: 1fr; }} }}
      .card {{
        background: var(--card);
        border: 1px solid var(--border);
        border-radius: 16px;
        padding: 16px;
      }}
      h1 {{ margin: 0 0 8px; font-size: 24px; }}
      .muted {{ color: var(--muted); }}
      label {{ display: block; margin: 10px 0 6px; font-size: 13px; color: var(--muted); }}
      input, select {{
        width: 100%;
        box-sizing: border-box;
        padding: 10px 12px;
        border-radius: 10px;
        border: 1px solid var(--border);
        background: #0c121b;
        color: var(--text);
      }}
      button {{
        margin-top: 12px;
        padding: 10px 14px;
        border: 0;
        border-radius: 10px;
        background: var(--accent);
        color: white;
        cursor: pointer;
        font-weight: 600;
      }}
      button.secondary {{ background: #243447; }}
      .row {{ display: flex; gap: 8px; align-items: center; flex-wrap: wrap; }}
      table {{ width: 100%; border-collapse: collapse; margin-top: 10px; }}
      th, td {{ border-bottom: 1px solid var(--border); padding: 10px 8px; text-align: left; }}
      th {{ color: var(--muted); font-size: 13px; cursor: pointer; user-select: none; white-space: nowrap; }}
      td.num, th.num {{ text-align: right; font-variant-numeric: tabular-nums; }}
      .pos {{ color: var(--good); }}
      .neg {{ color: var(--bad); }}
      a {{ color: var(--accent); text-decoration: none; }}
      a:hover {{ text-decoration: underline; }}
      .symbol-link {{
        color: var(--accent);
        text-decoration: none;
      }}
      .symbol-link:hover {{
        text-decoration: underline;
      }}
      .footer {{ margin-top: 8px; color: var(--muted); font-size: 12px; }}
      .spacer {{ height: 8px; }}
      code {{ color: #d7f0ff; }}
      .table-wrap {{ overflow-x: auto; }}
    </style>
  </head>
  <body>
    <div class=\"wrap\">
      <div class=\"row\" style=\"justify-content:space-between; margin-bottom: 10px;\">
        <h1>Stock Forecast Tracker</h1>
        <div class=\"muted\">Last refreshed: <span id=\"last-refreshed\">—</span> · <a href=\"/api-docs\">API Docs</a></div>
      </div>

      <div class=\"grid\">
        <div class=\"card\">
          <h3 style=\"margin-top:0\">Add / Update Forecast</h3>
          <p class=\"muted\">Data is persisted in PostgreSQL. Scores are 0-10.</p>
          <form id=\"form\">
            <label>Symbol</label>
            <input id=\"symbol\" placeholder=\"AAPL\" required />

            <label>Forecast Price ($)</label>
            <input id=\"forecast_price\" type=\"number\" step=\"0.01\" placeholder=\"210.50\" required />

            <label>Updated Date</label>
            <input id=\"updated_date\" type=\"date\" value=\"{date.today().isoformat()}\" required />

            <label>Reward Score (0-10)</label>
            <input id=\"reward_score\" type=\"number\" min=\"0\" max=\"10\" step=\"0.1\" value=\"5\" required />

            <label>Risk Score (0-10)</label>
            <input id=\"risk_score\" type=\"number\" min=\"0\" max=\"10\" step=\"0.1\" value=\"5\" required />

            <label>Confidence Score (0-10)</label>
            <input id=\"confidence_score\" type=\"number\" min=\"0\" max=\"10\" step=\"0.1\" value=\"5\" required />

            <label>Classification</label>
            <select id=\"classification\" required>
              <option value=\"buy\">buy</option>
              <option value=\"hold/watch\" selected>hold/watch</option>
              <option value=\"sell\">sell</option>
            </select>

            <button type=\"submit\">Save</button>
            <button type=\"button\" id=\"refresh\" class=\"secondary\">Refresh Prices</button>
          </form>

          <div class=\"spacer\"></div>
          <h3 style=\"margin-bottom:8px;\">Earnings</h3>
          <form id=\"earnings-form\" class=\"row\">
            <label style=\"margin:0;\">Earnings calendar up to (date)</label>
            <input id=\"earnings_to\" type=\"date\" value=\"{earnings_default_date}\" style=\"max-width: 180px;\" />
            <button id=\"earnings_btn\" type=\"submit\">Update Earnings</button>
          </form>
          <div class=\"footer\">Stores earnings calendar data in PostgreSQL. Requires <code>FMP_API_KEY</code>.</div>
        </div>

        <div class=\"card\">
          <h3 style=\"margin-top:0\">Tracked Forecasts</h3>
          <div class=\"muted\">Opportunity = (Reward×4) + ((10-Risk)×4) + (Confidence×2). Click a symbol to open its chart and sync status.</div>
          <div class=\"table-wrap\">
            <table>
              <thead>
                <tr>
                  <th data-key=\"symbol\" onclick=\"sortBy('symbol')\">Symbol</th>
                  <th class=\"num\" data-key=\"forecast_price\" onclick=\"sortBy('forecast_price')\">Forecast ($)</th>
                  <th class=\"num\" data-key=\"reward_score\" onclick=\"sortBy('reward_score')\">Reward</th>
                  <th class=\"num\" data-key=\"risk_score\" onclick=\"sortBy('risk_score')\">Risk</th>
                  <th class=\"num\" data-key=\"confidence_score\" onclick=\"sortBy('confidence_score')\">Confidence</th>
                  <th data-key=\"classification\" onclick=\"sortBy('classification')\">Class</th>
                  <th class=\"num\" data-key=\"opportunity_score\" onclick=\"sortBy('opportunity_score')\">Opportunity</th>
                  <th data-key=\"updated_date\" onclick=\"sortBy('updated_date')\">Updated</th>
                  <th data-key=\"next_earnings\" onclick=\"sortBy('next_earnings')\">Next Earnings</th>
                  <th class=\"num\" data-key=\"current_price\" onclick=\"sortBy('current_price')\">Current ($)</th>
                  <th class=\"num\" data-key=\"diff_dollar\" onclick=\"sortBy('diff_dollar')\">Δ $</th>
                  <th class=\"num\" data-key=\"diff_pct\" onclick=\"sortBy('diff_pct')\">Δ %</th>
                </tr>
              </thead>
              <tbody id=\"tbody\"></tbody>
            </table>
          </div>
        </div>
      </div>
    </div>
  </body>
</html>
    """
    return Response(html, mimetype="text/html")


@app.post("/add")
def add_symbol() -> Response:
    payload = request.get_json(force=True, silent=True) or {}
    try:
        _upsert_stocks([payload])
    except ValueError as e:
        return jsonify({"ok": False, "error": str(e)}), 400
    except Exception as e:
        return jsonify({"ok": False, "error": f"Database error: {e}"}), 500
    return jsonify({"ok": True})


@app.get("/data")
def data() -> Response:
    df = _read_df()
    symbols = [] if df.empty else sorted(df["symbol"].dropna().astype(str).str.upper().unique().tolist())

    prices: Dict[str, float | None] = _fetch_current_prices(symbols)
    next_map: Dict[str, str] = _load_next_earnings_map()

    rows = []
    if not df.empty:
        for _, r in df.iterrows():
            sym = str(r["symbol"]).upper()
            fpx = None
            try:
                fpx = float(r["forecast_price"]) if r["forecast_price"] == r["forecast_price"] else None
            except Exception:
                fpx = None
            cur = prices.get(sym)

            reward_score = float(r.get("reward_score")) if pd.notna(r.get("reward_score")) else DEFAULT_SCORE
            risk_score = float(r.get("risk_score")) if pd.notna(r.get("risk_score")) else DEFAULT_SCORE
            confidence_score = (
                float(r.get("confidence_score")) if pd.notna(r.get("confidence_score")) else DEFAULT_SCORE
            )
            classification = str(r.get("classification", DEFAULT_CLASSIFICATION))

            diff_dollar = None
            diff_pct = None
            if cur is not None and fpx not in (None, 0):
                diff_dollar = fpx - cur
                try:
                    diff_pct = (diff_dollar / cur) * 100.0
                except Exception:
                    diff_pct = None

            rows.append(
                {
                    "symbol": sym,
                    "forecast_price": fpx,
                    "reward_score": reward_score,
                    "risk_score": risk_score,
                    "confidence_score": confidence_score,
                    "classification": classification,
                    "opportunity_score": _opportunity_score(reward_score, risk_score, confidence_score),
                    "updated_date": str(r.get("updated_date", "")),
                    "next_earnings": next_map.get(sym),
                    "current_price": cur,
                    "diff_dollar": diff_dollar,
                    "diff_pct": diff_pct,
                }
            )

    return jsonify({"rows": rows})


@app.get("/chart/<symbol>")
def chart_page(symbol: str) -> Response:
    symbol = str(symbol).strip().upper()
    html = f"""
<!doctype html>
<html lang="en">
  <head>
    <meta charset="utf-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1" />
    <title>{symbol} Chart</title>
    <style>
      :root {{
        --bg: #0b0f14;
        --card: #0f1520;
        --muted: #9fb0c0;
        --text: #e6edf3;
        --accent: #2e90fa;
        --good: #17b26a;
        --warn: #f79009;
        --bad: #f04438;
        --border: #1f2a3a;
      }}
      html, body {{
        margin: 0;
        background:
          radial-gradient(circle at top right, rgba(46, 144, 250, 0.18), transparent 30%),
          linear-gradient(180deg, #0b0f14 0%, #07111c 100%);
        color: var(--text);
        font-family: Georgia, 'Times New Roman', serif;
      }}
      .wrap {{
        max-width: 1200px;
        margin: 24px auto;
        padding: 0 16px 40px;
      }}
      .top {{
        display: flex;
        justify-content: space-between;
        gap: 16px;
        align-items: center;
        flex-wrap: wrap;
        margin-bottom: 18px;
      }}
      .headline {{
        margin: 0;
        font-size: clamp(28px, 5vw, 48px);
        letter-spacing: 0.02em;
      }}
      .muted {{ color: var(--muted); }}
      .grid {{
        display: grid;
        grid-template-columns: minmax(0, 2fr) minmax(280px, 1fr);
        gap: 16px;
      }}
      @media (max-width: 920px) {{
        .grid {{ grid-template-columns: 1fr; }}
      }}
      .card {{
        background: rgba(15, 21, 32, 0.92);
        border: 1px solid var(--border);
        border-radius: 18px;
        padding: 18px;
        box-shadow: 0 18px 40px rgba(0, 0, 0, 0.25);
      }}
      .canvas-wrap {{
        margin-top: 14px;
        border-radius: 14px;
        overflow: hidden;
        border: 1px solid var(--border);
        background:
          linear-gradient(180deg, rgba(46, 144, 250, 0.06), transparent 30%),
          #09111b;
      }}
      canvas {{
        display: block;
        width: 100%;
        height: 380px;
      }}
      .stats {{
        display: grid;
        grid-template-columns: repeat(2, minmax(0, 1fr));
        gap: 12px;
      }}
      .stat {{
        padding: 12px;
        border: 1px solid var(--border);
        border-radius: 14px;
        background: rgba(9, 17, 27, 0.8);
      }}
      .stat-label {{
        display: block;
        color: var(--muted);
        font-size: 12px;
        text-transform: uppercase;
        letter-spacing: 0.08em;
        margin-bottom: 6px;
      }}
      .stat-value {{
        font-size: 20px;
        font-variant-numeric: tabular-nums;
      }}
      button, select {{
        border-radius: 12px;
        border: 1px solid var(--border);
        background: #112033;
        color: var(--text);
        padding: 10px 14px;
      }}
      button {{
        background: var(--accent);
        border: 0;
        cursor: pointer;
        font-weight: 700;
      }}
      button.secondary {{
        background: #223349;
      }}
      button:disabled {{
        cursor: wait;
        opacity: 0.7;
      }}
      .toolbar {{
        display: flex;
        flex-wrap: wrap;
        gap: 10px;
        align-items: center;
        margin-top: 12px;
      }}
      .status {{
        margin-top: 14px;
        padding: 12px;
        border-radius: 12px;
        border: 1px solid var(--border);
        background: rgba(17, 32, 51, 0.55);
      }}
      .status.error {{
        border-color: rgba(240, 68, 56, 0.5);
        color: #ffd2ce;
      }}
      .meta {{
        display: flex;
        flex-direction: column;
        gap: 10px;
      }}
      a {{ color: var(--accent); text-decoration: none; }}
      a:hover {{ text-decoration: underline; }}
      .empty {{
        color: var(--muted);
        font-style: italic;
      }}
    </style>
  </head>
  <body>
    <div class="wrap">
      <div class="top">
        <div>
          <div class="muted"><a href="/">Back to dashboard</a></div>
          <h1 class="headline">{symbol}</h1>
          <div class="muted">Historical chart with PostgreSQL cache and Alpaca sync controls.</div>
        </div>
        <div class="toolbar">
          <label class="muted" for="timeframe">Timeframe</label>
          <select id="timeframe">
            <option value="1Day" selected>1 Day</option>
            <option value="1Hour">1 Hour</option>
            <option value="15Min">15 Min</option>
            <option value="5Min">5 Min</option>
            <option value="1Min">1 Min</option>
          </select>
          <button id="sync-btn">Sync Now</button>
          <button id="reload-btn" class="secondary">Reload DB Data</button>
        </div>
      </div>

      <div class="grid">
        <div class="card">
          <div id="chart-summary" class="muted">Loading chart data...</div>
          <div class="canvas-wrap">
            <canvas id="chart" width="960" height="380"></canvas>
          </div>
          <div id="empty-state" class="status empty" style="display:none;">No bars available yet.</div>
          <div id="status" class="status" style="display:none;"></div>
        </div>

        <div class="card meta">
          <div class="stats">
            <div class="stat">
              <span class="stat-label">Last Close</span>
              <div class="stat-value" id="last-close">-</div>
            </div>
            <div class="stat">
              <span class="stat-label">Bars Loaded</span>
              <div class="stat-value" id="bar-count">0</div>
            </div>
            <div class="stat">
              <span class="stat-label">Latest Quote</span>
              <div class="stat-value" id="latest-quote">-</div>
            </div>
            <div class="stat">
              <span class="stat-label">Latest Bar</span>
              <div class="stat-value" id="latest-bar-time">-</div>
            </div>
          </div>

          <div class="status">
            <div><strong>Last synced:</strong> <span id="last-synced">-</span></div>
            <div><strong>Last successful sync:</strong> <span id="last-successful-sync">-</span></div>
            <div><strong>Last bar sync:</strong> <span id="last-bar-sync">-</span></div>
            <div><strong>Last quote sync:</strong> <span id="last-quote-sync">-</span></div>
            <div><strong>Sync status:</strong> <span id="sync-state">idle</span></div>
            <div><strong>Sync error:</strong> <span id="sync-error">-</span></div>
          </div>
        </div>
      </div>
    </div>

    <script>
      const SYMBOL = {json.dumps(symbol)};
      const canvas = document.getElementById('chart');
      const ctx = canvas.getContext('2d');

      function fmtDate(value) {{
        if (!value) return '-';
        return new Date(value).toLocaleString();
      }}

      function fmtPrice(value) {{
        if (value == null || Number.isNaN(Number(value))) return '-';
        return Number(value).toLocaleString(undefined, {{ minimumFractionDigits: 2, maximumFractionDigits: 2 }});
      }}

      function setStatus(message, isError = false) {{
        const el = document.getElementById('status');
        el.style.display = message ? 'block' : 'none';
        el.textContent = message || '';
        el.classList.toggle('error', !!isError);
      }}

      function clearCanvas() {{
        ctx.clearRect(0, 0, canvas.width, canvas.height);
        ctx.fillStyle = '#9fb0c0';
        ctx.font = '16px Georgia';
        ctx.fillText('No chart data yet', 24, 40);
      }}

      function drawChart(bars) {{
        ctx.clearRect(0, 0, canvas.width, canvas.height);
        if (!bars.length) {{
          clearCanvas();
          return;
        }}

        const closes = bars.map((b) => Number(b.c));
        const highs = bars.map((b) => Number(b.h));
        const lows = bars.map((b) => Number(b.l));
        const max = Math.max(...highs);
        const min = Math.min(...lows);
        const padLeft = 52;
        const padRight = 22;
        const padTop = 24;
        const padBottom = 38;
        const w = canvas.width - padLeft - padRight;
        const h = canvas.height - padTop - padBottom;
        const range = Math.max(max - min, 0.0001);

        ctx.strokeStyle = 'rgba(159, 176, 192, 0.18)';
        ctx.lineWidth = 1;
        for (let i = 0; i < 4; i++) {{
          const y = padTop + (h * i / 3);
          ctx.beginPath();
          ctx.moveTo(padLeft, y);
          ctx.lineTo(canvas.width - padRight, y);
          ctx.stroke();
        }}

        ctx.fillStyle = '#9fb0c0';
        ctx.font = '12px Georgia';
        for (let i = 0; i < 4; i++) {{
          const ratio = 1 - (i / 3);
          const value = min + (range * ratio);
          const y = padTop + (h * i / 3);
          ctx.fillText(fmtPrice(value), 8, y + 4);
        }}

        ctx.beginPath();
        closes.forEach((value, idx) => {{
          const x = padLeft + ((bars.length === 1 ? 0.5 : idx / (bars.length - 1)) * w);
          const y = padTop + ((max - value) / range) * h;
          if (idx === 0) ctx.moveTo(x, y);
          else ctx.lineTo(x, y);
        }});
        const gradient = ctx.createLinearGradient(0, padTop, 0, canvas.height - padBottom);
        gradient.addColorStop(0, '#7cc4ff');
        gradient.addColorStop(1, '#2e90fa');
        ctx.strokeStyle = gradient;
        ctx.lineWidth = 3;
        ctx.stroke();

        ctx.fillStyle = '#d7f0ff';
        ctx.font = '12px Georgia';
        const firstLabel = new Date(bars[0].t).toLocaleDateString();
        const lastLabel = new Date(bars[bars.length - 1].t).toLocaleDateString();
        ctx.fillText(firstLabel, padLeft, canvas.height - 12);
        const lastWidth = ctx.measureText(lastLabel).width;
        ctx.fillText(lastLabel, canvas.width - padRight - lastWidth, canvas.height - 12);
      }}

      function applyPayload(payload) {{
        const bars = payload.bars || [];
        drawChart(bars);
        document.getElementById('empty-state').style.display = bars.length ? 'none' : 'block';
        document.getElementById('chart-summary').textContent = bars.length
          ? `Showing ${{bars.length}} ${{payload.timeframe}} bars from PostgreSQL for ${{payload.symbol}}.`
          : `No ${{payload.timeframe}} bars stored for ${{payload.symbol}}.`;

        const lastBar = bars.length ? bars[bars.length - 1] : null;
        document.getElementById('last-close').textContent = lastBar ? fmtPrice(lastBar.c) : '-';
        document.getElementById('bar-count').textContent = String(payload.bar_count || bars.length || 0);
        document.getElementById('latest-quote').textContent = payload.latest_quote
          ? `${{fmtPrice(payload.latest_quote.bid_price)}} / ${{fmtPrice(payload.latest_quote.ask_price)}}`
          : '-';
        document.getElementById('latest-bar-time').textContent = payload.latest_bar ? fmtDate(payload.latest_bar.t) : '-';

        const sync = payload.sync_state || {{}};
        document.getElementById('last-synced').textContent = fmtDate(sync.last_synced_at);
        document.getElementById('last-successful-sync').textContent = fmtDate(sync.last_successful_sync_at);
        document.getElementById('last-bar-sync').textContent = fmtDate(sync.last_bar_synced_at);
        document.getElementById('last-quote-sync').textContent = fmtDate(sync.last_quote_synced_at);
        document.getElementById('sync-state').textContent = sync.sync_status || 'idle';
        document.getElementById('sync-error').textContent = sync.sync_error || '-';
      }}

      async function loadChart(options = {{}}) {{
        const timeframe = document.getElementById('timeframe').value;
        const params = new URLSearchParams({{
          timeframe,
          limit: '180',
          auto_sync: options.autoSync ? '1' : '0',
        }});
        const res = await fetch(`/api/chart/${{encodeURIComponent(SYMBOL)}}?${{params.toString()}}`);
        const payload = await res.json();
        if (!res.ok || !payload.ok) {{
          throw new Error(payload.error || 'Failed to load chart data.');
        }}
        applyPayload(payload);
        if (payload.auto_synced) {{
          setStatus('No cached bars were available, so the server fetched market data from Alpaca.');
        }} else if (options.showReloadMessage) {{
          setStatus('Reloaded chart data from PostgreSQL.');
        }} else {{
          setStatus('');
        }}
      }}

      async function syncNow() {{
        const btn = document.getElementById('sync-btn');
        btn.disabled = true;
        setStatus('Syncing latest market data from Alpaca...');
        try {{
          const timeframe = document.getElementById('timeframe').value;
          const res = await fetch(`/api/chart/${{encodeURIComponent(SYMBOL)}}/sync`, {{
            method: 'POST',
            headers: {{ 'Content-Type': 'application/json' }},
            body: JSON.stringify({{ timeframe }}),
          }});
          const payload = await res.json();
          if (!res.ok || !payload.ok) {{
            throw new Error(payload.error || 'Sync failed.');
          }}
          applyPayload(payload);
          setStatus(`Sync complete. ${{payload.bars_inserted || 0}} bar(s) inserted or updated.`);
        }} catch (err) {{
          setStatus(err.message || String(err), true);
        }} finally {{
          btn.disabled = false;
        }}
      }}

      window.addEventListener('DOMContentLoaded', async () => {{
        document.getElementById('timeframe').addEventListener('change', () => loadChart({{ autoSync: true }}).catch((err) => setStatus(err.message, true)));
        document.getElementById('reload-btn').addEventListener('click', () => loadChart({{ autoSync: false, showReloadMessage: true }}).catch((err) => setStatus(err.message, true)));
        document.getElementById('sync-btn').addEventListener('click', syncNow);
        try {{
          await loadChart({{ autoSync: true }});
        }} catch (err) {{
          setStatus(err.message || String(err), true);
          clearCanvas();
        }}
      }});
    </script>
  </body>
</html>
    """
    return Response(html, mimetype="text/html")


@app.get("/api/chart/<symbol>")
def get_chart_data(symbol: str) -> Response:
    timeframe = (request.args.get("timeframe") or DEFAULT_CHART_TIMEFRAME).strip()
    if timeframe not in SUPPORTED_TIMEFRAMES:
        return jsonify({"ok": False, "error": f"Unsupported timeframe '{timeframe}'."}), 400

    limit_raw = (request.args.get("limit") or "180").strip()
    auto_sync = (request.args.get("auto_sync") or "").strip().lower() in {"1", "true", "yes"}
    try:
        limit = int(limit_raw)
    except ValueError:
        return jsonify({"ok": False, "error": "'limit' must be an integer."}), 400

    try:
        payload = _get_market_snapshot(symbol, timeframe, limit)
        auto_synced = False
        if auto_sync and not payload["bars"]:
            payload = _sync_market_data(symbol, timeframe, force_full=True)
            payload["bar_count"] = min(len(payload["bars"]), limit)
            payload["bars"] = payload["bars"][-limit:]
            auto_synced = True
        payload["ok"] = True
        payload["auto_synced"] = auto_synced
        return jsonify(payload)
    except ValueError as exc:
        return jsonify({"ok": False, "error": str(exc)}), 400
    except Exception as exc:
        return jsonify({"ok": False, "error": str(exc)}), 500


@app.post("/api/chart/<symbol>/sync")
def sync_chart_data(symbol: str) -> Response:
    payload = request.get_json(silent=True) or {}
    timeframe = str(payload.get("timeframe") or DEFAULT_CHART_TIMEFRAME).strip()
    if timeframe not in SUPPORTED_TIMEFRAMES:
        return jsonify({"ok": False, "error": f"Unsupported timeframe '{timeframe}'."}), 400
    try:
        snapshot = _sync_market_data(symbol, timeframe, force_full=False)
        snapshot["ok"] = True
        return jsonify(snapshot)
    except ValueError as exc:
        return jsonify({"ok": False, "error": str(exc)}), 400
    except Exception as exc:
        return jsonify({"ok": False, "error": str(exc)}), 500


@app.get("/api/chart/<symbol>/latest")
def get_latest_market_data(symbol: str) -> Response:
    timeframe = (request.args.get("timeframe") or DEFAULT_CHART_TIMEFRAME).strip()
    if timeframe not in SUPPORTED_TIMEFRAMES:
        return jsonify({"ok": False, "error": f"Unsupported timeframe '{timeframe}'."}), 400
    try:
        snapshot = _get_market_snapshot(symbol, timeframe, 1)
        return jsonify(
            {
                "ok": True,
                "symbol": snapshot["symbol"],
                "timeframe": snapshot["timeframe"],
                "latest_quote": snapshot["latest_quote"],
                "latest_bar": snapshot["latest_bar"],
                "sync_state": snapshot["sync_state"],
            }
        )
    except ValueError as exc:
        return jsonify({"ok": False, "error": str(exc)}), 400
    except Exception as exc:
        return jsonify({"ok": False, "error": str(exc)}), 500


@app.get("/api/stocks")
def get_stocks_api() -> Response:
    """Return the latest stored row per symbol for external scripts."""
    symbol_filter = (request.args.get("symbol") or "").strip().upper()
    limit_raw = (request.args.get("limit") or "").strip()
    limit = None
    if limit_raw:
        try:
            limit = int(limit_raw)
        except ValueError:
            return jsonify({"ok": False, "error": "'limit' must be a positive integer."}), 400
        if limit <= 0:
            return jsonify({"ok": False, "error": "'limit' must be a positive integer."}), 400

    df = _read_df()
    if symbol_filter and not df.empty:
        df = df[df["symbol"].astype(str).str.upper() == symbol_filter].copy()
    if not df.empty:
        df = df.sort_values(["updated_date", "symbol"], ascending=[False, True]).reset_index(drop=True)
    if limit is not None and not df.empty:
        df = df.head(limit).copy()

    rows = []
    if not df.empty:
        for _, r in df.iterrows():
            reward_score = float(r.get("reward_score")) if pd.notna(r.get("reward_score")) else DEFAULT_SCORE
            risk_score = float(r.get("risk_score")) if pd.notna(r.get("risk_score")) else DEFAULT_SCORE
            confidence_score = (
                float(r.get("confidence_score")) if pd.notna(r.get("confidence_score")) else DEFAULT_SCORE
            )
            rows.append(
                {
                    "symbol": str(r.get("symbol", "")).upper(),
                    "forecast_price": float(r["forecast_price"]) if pd.notna(r.get("forecast_price")) else None,
                    "reward_score": reward_score,
                    "risk_score": risk_score,
                    "confidence_score": confidence_score,
                    "classification": str(r.get("classification", DEFAULT_CLASSIFICATION)),
                    "opportunity_score": _opportunity_score(reward_score, risk_score, confidence_score),
                    "updated_date": str(r.get("updated_date", "")),
                }
            )
    return jsonify(
        {"ok": True, "rows": rows, "count": len(rows), "symbol": symbol_filter or None, "limit": limit}
    )


@app.get("/api-docs")
def api_docs() -> Response:
    html = """
<!doctype html>
<html lang="en">
  <head>
    <meta charset="utf-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1" />
    <title>Stock Tracker API Docs</title>
    <style>
      :root {
        --bg: #0b0f14; --card: #0f1520; --muted: #aab8c5; --text: #e6edf3; --accent: #2e90fa; --border: #1f2a3a;
      }
      html, body { background: var(--bg); color: var(--text); font-family: ui-sans-serif, system-ui, -apple-system, 'Segoe UI', Roboto, 'Helvetica Neue', Arial; margin: 0; }
      .wrap { max-width: 920px; margin: 24px auto; padding: 0 16px; }
      .card { background: var(--card); border: 1px solid var(--border); border-radius: 16px; padding: 18px; }
      h1 { margin-top: 0; font-size: 24px; }
      h2 { margin-top: 22px; font-size: 18px; }
      p, li { color: var(--text); line-height: 1.5; }
      .muted { color: var(--muted); }
      pre { background: #0c121b; border: 1px solid var(--border); border-radius: 10px; padding: 12px; overflow-x: auto; }
      code { color: #d7f0ff; }
      a { color: var(--accent); text-decoration: none; }
      a:hover { text-decoration: underline; }
      .symbol-link {{
        color: var(--accent);
        text-decoration: none;
      }}
      .symbol-link:hover {{
        text-decoration: underline;
      }}
      .top { display: flex; justify-content: space-between; align-items: baseline; margin-bottom: 8px; }
    </style>
  </head>
  <body>
    <div class="wrap">
      <div class="card">
        <div class="top">
          <h1>API Docs</h1>
          <a href="/">Back to app</a>
        </div>
        <p class="muted">Base URL: <code>http://127.0.0.1:5000</code></p>

        <h2>GET /api/stocks</h2>
        <p>Returns the latest stored row per stock from PostgreSQL. Optional query parameters: <code>symbol</code> and <code>limit</code>.</p>
<pre><code>curl -s "http://127.0.0.1:5000/api/stocks"
curl -s "http://127.0.0.1:5000/api/stocks?symbol=AAPL"
curl -s "http://127.0.0.1:5000/api/stocks?limit=10"</code></pre>

        <h2>POST /api/stocks</h2>
        <p>Upserts one or many rows by symbol (case-insensitive).</p>
<pre><code>curl -X POST http://127.0.0.1:5000/api/stocks \
  -H "Content-Type: application/json" \
  -d '{"symbol":"AAPL","forecast_price":210.5,"updated_date":"2026-03-07","reward_score":8,"risk_score":3,"confidence_score":7,"classification":"buy"}'</code></pre>

<pre><code>curl -X POST http://127.0.0.1:5000/api/stocks \
  -H "Content-Type: application/json" \
  -d '{"rows":[{"symbol":"AAPL","forecast_price":210.5,"reward_score":8,"risk_score":3,"confidence_score":7,"classification":"buy"},{"symbol":"MSFT","forecast_price":480,"reward_score":6,"risk_score":4,"confidence_score":7,"classification":"hold/watch"}]}'</code></pre>

        <h2>GET /chart/&lt;symbol&gt;</h2>
        <p>Interactive chart page. It loads bars from PostgreSQL and, if none are stored yet, automatically fetches them from Alpaca on first load.</p>
<pre><code>http://127.0.0.1:5000/chart/AAPL</code></pre>

        <h2>GET /api/chart/&lt;symbol&gt;</h2>
        <p>Returns stored chart bars, latest quote/bar cache, and sync metadata. Optional params: <code>timeframe</code>, <code>limit</code>, and <code>auto_sync=1</code>.</p>
<pre><code>curl -s "http://127.0.0.1:5000/api/chart/AAPL?timeframe=1Day&limit=180&auto_sync=1"</code></pre>

        <h2>POST /api/chart/&lt;symbol&gt;/sync</h2>
        <p>Fetches the latest market data from Alpaca, upserts bars into PostgreSQL, and returns the refreshed chart payload.</p>
<pre><code>curl -X POST http://127.0.0.1:5000/api/chart/AAPL/sync \
  -H "Content-Type: application/json" \
  -d '{"timeframe":"1Day"}'</code></pre>

        <h2>Fields</h2>
        <ul>
          <li><code>symbol</code>: required, stock ticker (stored uppercase)</li>
          <li><code>forecast_price</code>: required, numeric</li>
          <li><code>updated_date</code>: optional, defaults to server date (YYYY-MM-DD)</li>
          <li><code>reward_score</code>: optional, number from 0 to 10 (default 5)</li>
          <li><code>risk_score</code>: optional, number from 0 to 10 (default 5)</li>
          <li><code>confidence_score</code>: optional, number from 0 to 10 (default 5)</li>
          <li><code>classification</code>: optional, one of <code>buy</code>, <code>hold/watch</code>, <code>sell</code> (default hold/watch)</li>
          <li><code>opportunity_score</code>: computed output only using <code>(Reward×4)+((10-Risk)×4)+(Confidence×2)</code></li>
        </ul>
      </div>
    </div>
  </body>
</html>
    """
    return Response(html, mimetype="text/html")


@app.post("/api/stocks")
def post_stocks_api() -> Response:
    """Upsert one or more rows in the PostgreSQL database.
    Accepts either:
      - single row
      - batch: {"rows":[...]}
    """
    payload = request.get_json(force=True, silent=True) or {}
    if "rows" in payload:
        rows = payload.get("rows")
        if not isinstance(rows, list) or not rows:
            return jsonify({"ok": False, "error": "'rows' must be a non-empty list."}), 400
    else:
        rows = [payload]

    try:
        n = _upsert_stocks(rows)
    except ValueError as e:
        return jsonify({"ok": False, "error": str(e)}), 400
    except Exception as e:
        return jsonify({"ok": False, "error": f"Database error: {e}"}), 500

    return jsonify({"ok": True, "upserted": n})


@app.get("/export")
def export_csv() -> Response:
    df = _read_df()
    stream = io.StringIO()
    writer = csv.writer(stream)
    writer.writerow(EXTENDED_HEADERS + ["opportunity_score"])
    for _, r in df.iterrows():
        reward_score = float(r.get("reward_score")) if pd.notna(r.get("reward_score")) else DEFAULT_SCORE
        risk_score = float(r.get("risk_score")) if pd.notna(r.get("risk_score")) else DEFAULT_SCORE
        confidence_score = float(r.get("confidence_score")) if pd.notna(r.get("confidence_score")) else DEFAULT_SCORE
        writer.writerow(
            [
                r.get("symbol", ""),
                r.get("forecast_price", ""),
                r.get("updated_date", ""),
                reward_score,
                risk_score,
                confidence_score,
                r.get("classification", DEFAULT_CLASSIFICATION),
                _opportunity_score(reward_score, risk_score, confidence_score),
            ]
        )

    out = Response(stream.getvalue(), mimetype="text/csv")
    out.headers["Content-Disposition"] = "attachment; filename=stocks.csv"
    return out


@app.get("/health")
def health() -> Response:
    try:
        _init_db_if_needed()
        with _get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT 1")
                cur.fetchone()
        return jsonify({"ok": True, "database": "postgres", "table": DB_TABLE})
    except Exception as e:
        return jsonify({"ok": False, "database": "postgres", "error": str(e)}), 500


@app.post("/update-earnings")
def update_earnings() -> Response:
    to_date = (request.form.get("to") or (request.get_json(silent=True) or {}).get("to") or date.today().isoformat())
    api_key = os.environ.get("FMP_API_KEY") or os.environ.get("FMP_APIKEY") or os.environ.get("FMP_KEY")
    try:
        date.fromisoformat(to_date)
        rows = _fetch_earnings_rows(to_date, api_key)
        n = _replace_earnings_rows(rows, to_date)
    except ValueError as e:
        return Response(f"<pre>Update failed: {e}</pre>", mimetype="text/html", status=400)
    except Exception as e:
        return Response(f"<pre>Update failed: {e}</pre>", mimetype="text/html", status=500)
    return Response(
        f"<pre>Updated earnings table '{EARNINGS_TABLE}' with {n} row(s) for to={to_date}. <a href='/'>&larr; back</a></pre>",
        mimetype="text/html",
    )


if __name__ == "__main__":
    host = os.environ.get("HOST", "127.0.0.1")
    port_var = os.environ.get("PORT", "5000")
    try:
        port = int(port_var)
    except (TypeError, ValueError):
        port = 5000
    print(f"Running on http://{host}:{port} (dotenv={'on' if _DOTENV_LOADED else 'off'})")
    app.run(host=host, port=port, debug=True)
