from __future__ import annotations

import calendar
import csv
import io
import json
import os
import re
import threading
from contextlib import nullcontext
from datetime import date, datetime, timedelta, timezone
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen
from typing import Dict, List, Sequence, Tuple

import pandas as pd
from flask import Flask, Response, jsonify, request
import matplotlib.pyplot as plt

from analytics import AnalyticsNotFoundError, AnalyticsService, AnalyticsSettings
from analytics.quality import validate_bars, OHLCV_SQL_CHECK
from analytics.cache import CacheCapacityError
from analytics.utils import isoformat_utc, parse_asof
from analytics.runtime import runtime_diagnostics
from live_prices import PriceCache, YahooQuotes
from database import get_connection
from market_sync import SyncFlights
from adjustment_history import refresh_history

# Load environment variables from a .env file in the same directory (if present)
try:
    from dotenv import load_dotenv

    # Try the script directory first, then CWD
    _DOTENV_LOADED = load_dotenv(os.path.join(os.path.dirname(__file__), ".env")) or load_dotenv()
except Exception:
    _DOTENV_LOADED = False

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
EXTENDED_HEADERS = CSV_HEADERS + ["reward_score", "risk_score", "confidence_score", "classification", "hidden"]
_LOCK = threading.Lock()
_DB_INITIALIZED_FOR = None
ALLOWED_CLASSIFICATIONS = {"buy", "hold/watch", "sell"}
DEFAULT_SCORE = 5.0
DEFAULT_CLASSIFICATION = "hold/watch"
ALPACA_DATA_BASE_URL = os.environ.get("ALPACA_DATA_BASE_URL", "https://data.alpaca.markets").rstrip("/")
ALPACA_API_KEY_ID = os.environ.get("APCA-API-KEY-ID") or os.environ.get("ALPACA_API_KEY_ID")
ALPACA_API_SECRET_KEY = os.environ.get("APCA-API-SECRET-KEY") or os.environ.get("ALPACA_API_SECRET_KEY")
DEFAULT_CHART_TIMEFRAME = os.environ.get("CHART_DEFAULT_TIMEFRAME", "1Day")
DEFAULT_CHART_LOOKBACK_DAYS = int(os.environ.get("CHART_LOOKBACK_DAYS", "180"))
DAILY_CHART_LOOKBACK_DAYS = int(os.environ.get("CHART_DAILY_LOOKBACK_DAYS", "2555"))
INTRADAY_CHART_LOOKBACK_DAYS = int(os.environ.get("CHART_INTRADAY_LOOKBACK_DAYS", os.environ.get("CHART_LOOKBACK_DAYS", "180")))
CHART_DELAY_MINUTES = int(os.environ.get("CHART_DELAY_MINUTES", os.environ.get("CHART_DELAY", "20")))
CHART_15MIN_FETCH_LIMIT = int(os.environ.get("CHART_15MIN_FETCH_LIMIT", "1200"))
CHART_LATEST_AUTO_SYNC_MAX_AGE_DAYS = int(os.environ.get("CHART_LATEST_AUTO_SYNC_MAX_AGE_DAYS", "5"))
ALPACA_BAR_ADJUSTMENT = os.environ.get("ALPACA_BAR_ADJUSTMENT", "split").strip() or "split"
SUPPORTED_TIMEFRAMES = {"1Min", "5Min", "15Min", "1Hour", "1Day"}
ANALYTICS_TIMEFRAME_ALIASES = {
    "1D": "1Day",
    "1DAY": "1Day",
    "1DAYS": "1Day",
    "1H": "1Hour",
    "1HR": "1Hour",
    "60MIN": "1Hour",
    "15M": "15Min",
    "5M": "5Min",
    "1M": "1Min",
}
TIMEFRAME_DELTAS = {
    "1Min": timedelta(minutes=1),
    "5Min": timedelta(minutes=5),
    "15Min": timedelta(minutes=15),
    "1Hour": timedelta(hours=1),
    "1Day": timedelta(days=1),
}
analytics_service = AnalyticsService()
app.logger.warning("Analytics runtime: %s", json.dumps(runtime_diagnostics(), sort_keys=True))
_market_sync_flights = SyncFlights()


def _opportunity_score(reward_score: float, risk_score: float, confidence_score: float) -> float:
    return (reward_score * 4.0) + ((10.0 - risk_score) * 4.0) + (confidence_score * 2.0)


def _action_score(row: Dict) -> float:
    if (
        row.get("rsi_14") is None
        and row.get("dist_ma20_pct") is None
        and row.get("dist_ma50_pct") is None
        and row.get("trend_state") in (None, "Neutral")
        and row.get("macd_signal") in (None, "Neutral")
    ):
        return 0.0

    score = 0.0

    diff_pct = row.get("diff_pct")
    if diff_pct is not None:
        try:
            score += max(0.0, min(float(diff_pct), 30.0))
        except Exception:
            pass

    trend_state = row.get("trend_state")
    if trend_state == "Bullish":
        score += 20.0
    elif trend_state == "Neutral":
        score += 10.0

    ma_stack = str(row.get("ma_stack") or "")
    dist_ma20_pct = row.get("dist_ma20_pct")
    dist_ma50_pct = row.get("dist_ma50_pct")
    if ma_stack.startswith("Price > MA20 > MA50 > MA200"):
        score += 15.0
    elif dist_ma20_pct is not None and dist_ma50_pct is not None:
        try:
            if float(dist_ma20_pct) > 0 and float(dist_ma50_pct) > 0:
                score += 10.0
            elif float(dist_ma50_pct) > 0:
                score += 6.0
        except Exception:
            pass
    elif dist_ma50_pct is not None:
        try:
            if float(dist_ma50_pct) > 0:
                score += 6.0
        except Exception:
            pass

    rsi_14 = row.get("rsi_14")
    if rsi_14 is not None:
        try:
            rsi = float(rsi_14)
            if 50.0 <= rsi <= 65.0:
                score += 15.0
            elif 45.0 <= rsi < 50.0 or 65.0 < rsi <= 70.0:
                score += 10.0
            elif 30.0 <= rsi < 45.0:
                score += 6.0
            elif rsi > 70.0:
                score += 3.0
            elif rsi < 30.0:
                score += 5.0
        except Exception:
            pass

    macd_signal = row.get("macd_signal")
    if macd_signal == "Bull cross":
        score += 15.0
    elif macd_signal == "Positive":
        score += 10.0
    elif macd_signal == "Neutral":
        score += 5.0
    elif macd_signal == "Negative":
        score += 2.0

    if dist_ma20_pct is not None:
        try:
            dist_ma20 = float(dist_ma20_pct)
            if dist_ma20 > 12.0:
                score -= 10.0
            elif dist_ma20 > 8.0:
                score -= 5.0
        except Exception:
            pass

    return round(max(0.0, min(score, 100.0)), 1)


def _get_conn():
    return get_connection(DATABASE_URL)


def _init_db_if_needed() -> None:
    global _DB_INITIALIZED_FOR
    # Schema setup is process/configuration scoped, not request scoped. Include
    # the factory so isolated repositories/test schemas initialize independently.
    key = (os.getpid(), DATABASE_URL, DB_TABLE, EARNINGS_TABLE, _get_conn)
    if _DB_INITIALIZED_FOR == key:
        return
    with _LOCK:
        if _DB_INITIALIZED_FOR == key:
            return
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
                        classification TEXT NOT NULL DEFAULT '{DEFAULT_CLASSIFICATION}',
                        hidden BOOLEAN NOT NULL DEFAULT FALSE
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
                    f"ALTER TABLE {DB_TABLE} ADD COLUMN IF NOT EXISTS hidden BOOLEAN NOT NULL DEFAULT FALSE"
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
                    CREATE TABLE IF NOT EXISTS market_data_coverage (
                        symbol_id BIGINT NOT NULL REFERENCES symbol_metadata(id) ON DELETE CASCADE,
                        timeframe TEXT NOT NULL,
                        window_start TIMESTAMPTZ NOT NULL,
                        window_end TIMESTAMPTZ NOT NULL,
                        PRIMARY KEY (symbol_id, timeframe, window_start, window_end),
                        CHECK (window_start <= window_end)
                    )
                    """
                )
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS market_data_basis (
                        symbol_id BIGINT PRIMARY KEY REFERENCES symbol_metadata(id) ON DELETE CASCADE,
                        revision TEXT NOT NULL,
                        source TEXT NOT NULL,
                        adjustment_modes JSONB NOT NULL,
                        adjusted_through DATE NOT NULL,
                        checked_at TIMESTAMPTZ NOT NULL
                    );
                    ALTER TABLE market_data_coverage ADD COLUMN IF NOT EXISTS adjustment_revision TEXT;
                    ALTER TABLE market_data_coverage ADD COLUMN IF NOT EXISTS adjustment_mode TEXT;
                """)
                # Enforce new writes without claiming legacy rows were audited.
                cur.execute(f"""
                    DO $$ BEGIN
                        IF NOT EXISTS (SELECT 1 FROM pg_constraint
                            WHERE conrelid = 'stock_bars'::regclass AND conname = 'stock_bars_valid_ohlcv') THEN
                            ALTER TABLE stock_bars ADD CONSTRAINT stock_bars_valid_ohlcv
                                CHECK ({OHLCV_SQL_CHECK}) NOT VALID;
                        END IF;
                    END $$;
                """)
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
        # Publish only after the transaction commits; failures must be retried.
        _DB_INITIALIZED_FOR = key


def _read_df() -> pd.DataFrame:
    _init_db_if_needed()
    with _get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"""
                SELECT symbol, forecast_price, updated_date::TEXT,
                       reward_score, risk_score, confidence_score, classification, hidden
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
    df["hidden"] = df["hidden"].astype(bool)
    return df


def _daily_bar_snapshot(symbols: List[str]) -> Tuple[Dict[str, List[float]], Dict[str, str | None]]:
    """Read at most 250 daily bars per symbol, newest first via the bar index.

    Keep dashboard RSI/MACD's 250-bar seed window (not analytics' seed).
    Arrays are returned chronologically for all dashboard calculations to share.
    """
    closes: Dict[str, List[float]] = {str(symbol).upper(): [] for symbol in symbols}
    latest: Dict[str, str | None] = {symbol: None for symbol in closes}
    if not closes:
        return closes, latest
    _init_db_if_needed()
    with _get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT sm.symbol, bars.bar_time, bars.close
                FROM symbol_metadata sm
                CROSS JOIN LATERAL (
                    SELECT sb.bar_time, sb.close
                    FROM stock_bars sb
                    WHERE sb.symbol_id = sm.id AND sb.timeframe = '1Day'
                    ORDER BY sb.bar_time DESC
                    LIMIT 250
                ) bars
                WHERE sm.symbol = ANY(%s)
                ORDER BY sm.symbol ASC, bars.bar_time ASC
                """,
                (list(closes),),
            )
            for symbol, bar_time, close in cur.fetchall():
                key = str(symbol).upper()
                closes[key].append(float(close))
                latest[key] = bar_time.astimezone(timezone.utc).isoformat()
    return closes, latest


def _ma_distance_pcts(
    symbols: List[str], reference_prices: Dict[str, float | None],
    closes_by_symbol: Dict[str, List[float]] | None = None,
) -> Dict[str, Dict[str, float | None]]:
    out: Dict[str, Dict[str, float | None]] = {
        str(symbol).upper(): {"dist_ma20_pct": None, "dist_ma50_pct": None} for symbol in symbols
    }
    if not out:
        return out

    if closes_by_symbol is None:
        closes_by_symbol, _ = _daily_bar_snapshot(symbols)

    for symbol, closes in closes_by_symbol.items():
        reference_price = reference_prices.get(symbol)
        if reference_price is None and closes:
            reference_price = closes[-1]
        if reference_price is None:
            continue

        if len(closes) >= 20:
            ma20 = sum(closes[-20:]) / 20.0
            if ma20:
                out[symbol]["dist_ma20_pct"] = ((reference_price - ma20) / ma20) * 100.0
        if len(closes) >= 50:
            ma50 = sum(closes[-50:]) / 50.0
            if ma50:
                out[symbol]["dist_ma50_pct"] = ((reference_price - ma50) / ma50) * 100.0

    return out


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


def _set_stock_hidden(symbol: str, hidden: bool) -> bool:
    symbol = str(symbol).strip().upper()
    if not symbol:
        raise ValueError("Symbol is required.")

    _init_db_if_needed()
    with _get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"UPDATE {DB_TABLE} SET hidden = %s WHERE symbol = %s",
                (hidden, symbol),
            )
            return cur.rowcount > 0


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


# -------- Freshness-cached live regular-market prices ---------

_live_prices = PriceCache(
    YahooQuotes(timeout=float(os.environ.get("LIVE_PRICE_PROVIDER_TIMEOUT_SECONDS", "3"))),
    ttl=float(os.environ.get("LIVE_PRICE_CACHE_SECONDS", "15")),
    retry_seconds=float(os.environ.get("LIVE_PRICE_RETRY_SECONDS", "3")),
    wait_seconds=float(os.environ.get("LIVE_PRICE_WAIT_SECONDS", "5")),
)


def _fetch_current_prices(symbols: List[str]) -> Dict[str, float | None]:
    """Last regular-market quote, cached briefly; unavailable/expired = None."""
    return _live_prices.get_prices(symbols)


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


def _trend_states(symbols: List[str], closes_by_symbol: Dict[str, List[float]] | None = None) -> Dict[str, str]:
    out = {str(symbol).upper(): "Neutral" for symbol in symbols}
    if not out:
        return out

    if closes_by_symbol is None:
        closes_by_symbol, _ = _daily_bar_snapshot(symbols)

    for symbol, closes in closes_by_symbol.items():
        if len(closes) < 200:
            continue
        ma20 = sum(closes[-20:]) / 20.0
        ma50 = sum(closes[-50:]) / 50.0
        ma200 = sum(closes[-200:]) / 200.0
        prev_ma20 = sum(closes[-21:-1]) / 20.0
        latest_close = closes[-1]
        if latest_close > ma20 > ma50 > ma200 and ma20 > prev_ma20:
            out[symbol] = "Bullish"
        elif latest_close < ma20 < ma50 < ma200 and ma20 < prev_ma20:
            out[symbol] = "Bearish"
    return out


def _rsi_14_values(symbols: List[str], closes_by_symbol: Dict[str, List[float]] | None = None) -> Dict[str, float | None]:
    out: Dict[str, float | None] = {str(symbol).upper(): None for symbol in symbols}
    if not out:
        return out

    if closes_by_symbol is None:
        closes_by_symbol, _ = _daily_bar_snapshot(symbols)

    period = 14
    for symbol, closes in closes_by_symbol.items():
        if len(closes) <= period:
            continue

        gain_sum = 0.0
        loss_sum = 0.0
        for idx in range(1, period + 1):
            change = closes[idx] - closes[idx - 1]
            if change >= 0:
                gain_sum += change
            else:
                loss_sum += abs(change)

        avg_gain = gain_sum / period
        avg_loss = loss_sum / period
        rsi = 100.0 if avg_loss == 0 else 100.0 - (100.0 / (1.0 + (avg_gain / avg_loss)))

        for idx in range(period + 1, len(closes)):
            change = closes[idx] - closes[idx - 1]
            gain = max(change, 0.0)
            loss = max(-change, 0.0)
            avg_gain = ((avg_gain * (period - 1)) + gain) / period
            avg_loss = ((avg_loss * (period - 1)) + loss) / period
            rsi = 100.0 if avg_loss == 0 else 100.0 - (100.0 / (1.0 + (avg_gain / avg_loss)))

        out[symbol] = rsi

    return out


def _ema_number_values(values: Sequence[float | None], period: int) -> List[float | None]:
    out: List[float | None] = []
    multiplier = 2.0 / (period + 1.0)
    ema = None
    valid_count = 0
    for value in values:
        if value is None:
            out.append(None)
            continue
        valid_count += 1
        ema = value if ema is None else (value * multiplier) + (ema * (1.0 - multiplier))
        out.append(ema if valid_count >= period else None)
    return out


def _macd_signals(symbols: List[str], closes_by_symbol: Dict[str, List[float]] | None = None) -> Dict[str, str]:
    out = {str(symbol).upper(): "Neutral" for symbol in symbols}
    if not out:
        return out

    if closes_by_symbol is None:
        closes_by_symbol, _ = _daily_bar_snapshot(symbols)

    for symbol, closes in closes_by_symbol.items():
        if len(closes) < 35:
            continue

        ema12 = _ema_number_values(closes, 12)
        ema26 = _ema_number_values(closes, 26)
        macd_line = [
            (fast - slow) if fast is not None and slow is not None else None
            for fast, slow in zip(ema12, ema26)
        ]
        signal_line = _ema_number_values(macd_line, 9)
        hist = [
            (macd - signal) if macd is not None and signal is not None else None
            for macd, signal in zip(macd_line, signal_line)
        ]
        previous = hist[-2]
        latest = hist[-1]
        if previous is None or latest is None:
            continue
        if previous <= 0 and latest > 0:
            out[symbol] = "Bull cross"
        elif previous >= 0 and latest < 0:
            out[symbol] = "Bear cross"
        elif latest > 0:
            out[symbol] = "Positive"
        elif latest < 0:
            out[symbol] = "Negative"

    return out


def _add_months(d: date, months: int) -> date:
    """Return a date shifted by N calendar months, clamping day to month end."""
    month_index = (d.month - 1) + months
    year = d.year + (month_index // 12)
    month = (month_index % 12) + 1
    day = min(d.day, calendar.monthrange(year, month)[1])
    return date(year, month, day)


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _lookback_days_for_timeframe(timeframe: str) -> int:
    if timeframe == "1Day":
        return max(1, DAILY_CHART_LOOKBACK_DAYS)
    return max(1, INTRADAY_CHART_LOOKBACK_DAYS)


def _to_alpaca_ts(value: datetime) -> str:
    return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def _parse_market_timestamp(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        return datetime.fromisoformat(str(value).replace("Z", "+00:00")).astimezone(timezone.utc)
    except Exception:
        return None


def _market_snapshot_needs_sync(snapshot: Dict, max_age_days: int) -> bool:
    if max_age_days < 0:
        return False

    bars = snapshot.get("bars") or []
    if not bars:
        return True

    latest_bar_time = _parse_market_timestamp(bars[-1].get("t"))
    if latest_bar_time is None:
        return True

    delayed_now = _utcnow() - timedelta(minutes=max(CHART_DELAY_MINUTES, 0))
    if latest_bar_time <= delayed_now - timedelta(days=max_age_days):
        return True

    sync_state = snapshot.get("sync_state") or {}
    synced_at = _parse_market_timestamp(
        sync_state.get("last_successful_sync_at") or sync_state.get("last_synced_at")
    )
    if synced_at is None:
        return True

    return synced_at <= _utcnow() - timedelta(days=max_age_days)


def _alpaca_headers() -> Dict[str, str]:
    if not ALPACA_API_KEY_ID or not ALPACA_API_SECRET_KEY:
        raise RuntimeError("Missing Alpaca credentials. Set APCA-API-KEY-ID and APCA-API-SECRET-KEY in .env.")
    return {
        "APCA-API-KEY-ID": ALPACA_API_KEY_ID,
        "APCA-API-SECRET-KEY": ALPACA_API_SECRET_KEY,
        "User-Agent": "stock-tracker/1.0",
    }


def _alpaca_bar_adjustment_for_timeframe(timeframe: str) -> str:
    # Daily indicators and intraday anchors must have the same economic basis.
    return ALPACA_BAR_ADJUSTMENT


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
    adjustment = _alpaca_bar_adjustment_for_timeframe(timeframe)

    while True:
        payload = _alpaca_get_json(
            "/v2/stocks/bars",
            {
                "symbols": symbol,
                "timeframe": timeframe,
                "start": _to_alpaca_ts(start),
                "end": _to_alpaca_ts(end),
                "adjustment": adjustment,
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


def _normalize_bar_row(symbol_id: int, timeframe: str, payload: Dict, *, validated=False) -> Tuple:
    bar_time = _parse_market_timestamp(payload.get("t"))
    if bar_time is None:
        raise ValueError("Alpaca bar is missing a valid timestamp.")
    # Fail the entire provider page before opening a transaction. No filling,
    # dropping, or partial acceptance of malformed market observations.
    if not validated:
        validate_bars(pd.DataFrame([{
            "timestamp": bar_time, **{name: payload.get(key) for name, key in
            (("open", "o"), ("high", "h"), ("low", "l"), ("close", "c"), ("volume", "v"))}
        }]))
    return (
        symbol_id,
        timeframe,
        bar_time,
        float(payload["o"]),
        float(payload["h"]),
        float(payload["l"]),
        float(payload["c"]),
        int(payload["v"]),
        int(payload.get("n", 0) or 0) if payload.get("n") is not None else None,
        float(payload.get("vw")) if payload.get("vw") is not None else None,
    )


def _normalize_bar_rows(symbol_id: int, timeframe: str, bars: List[Dict]) -> List[Tuple]:
    if not bars:
        return []
    # Full-history refreshes must not allocate/validate a pandas frame per bar.
    validate_bars(pd.DataFrame([{
        "timestamp": _parse_market_timestamp(bar.get("t")),
        **{name: bar.get(key) for name, key in
           (("open", "o"), ("high", "h"), ("low", "l"), ("close", "c"), ("volume", "v"))}
    } for bar in bars]))
    return [_normalize_bar_row(symbol_id, timeframe, bar, validated=True) for bar in bars]


def _upsert_stock_bars(symbol_id: int, timeframe: str, bars: List[Dict], *, cursor=None) -> int:
    if not bars:
        return 0

    payload = _normalize_bar_rows(symbol_id, timeframe, bars)
    with (_get_conn() if cursor is None else nullcontext(None)) as conn:
        with (conn.cursor() if cursor is None else nullcontext(cursor)) as cur:
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
                    source = 'alpaca',
                    ingested_at = now()
                """,
                payload,
            )
    return len(payload)


def _refresh_adjusted_history(symbol_id, symbol, timeframe, start, end):
    with _get_conn() as conn:
        return refresh_history(conn, symbol_id, symbol, timeframe, start, end,
            fetch=_fetch_alpaca_bars, normalize=_normalize_bar_rows, write=_upsert_stock_bars,
            mode=_alpaca_bar_adjustment_for_timeframe, source=ALPACA_DATA_BASE_URL,
            now=_utcnow, deltas=TIMEFRAME_DELTAS,
            finalized_through=_utcnow() - timedelta(minutes=max(CHART_DELAY_MINUTES, 0)))


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


def _get_market_snapshot(symbol: str, timeframe: str, limit: int, before: datetime | None = None) -> Dict:
    symbol = str(symbol).strip().upper()
    symbol_id = _ensure_symbol_metadata(symbol)
    limit = max(1, min(limit, 5000))

    _init_db_if_needed()
    with _get_conn() as conn:
        with conn.cursor() as cur:
            before_filter_sql = ""
            cur.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
            params = [symbol_id, timeframe]
            if before is not None:
                before_filter_sql = " AND bar_time < %s"
                params.append(before.astimezone(timezone.utc))
            params.append(limit)
            cur.execute(
                f"""
                SELECT bar_time, open, high, low, close, volume
                FROM (
                    SELECT bar_time, open, high, low, close, volume
                    FROM stock_bars
                    WHERE symbol_id = %s AND timeframe = %s{before_filter_sql}
                    ORDER BY bar_time DESC
                    LIMIT %s
                ) bars
                ORDER BY bar_time ASC
                """,
                params,
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

            oldest_loaded_bar_time = bars[0]["t"] if bars else None
            has_older_bars = False
            if bars:
                cur.execute(
                    """
                    SELECT 1
                    FROM stock_bars
                    WHERE symbol_id = %s AND timeframe = %s AND bar_time < %s
                    LIMIT 1
                    """,
                    (
                        symbol_id,
                        timeframe,
                        datetime.fromisoformat(oldest_loaded_bar_time).astimezone(timezone.utc),
                    ),
                )
                has_older_bars = cur.fetchone() is not None

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

            cur.execute("""SELECT revision, source, adjustment_modes, adjusted_through, checked_at
                FROM market_data_basis WHERE symbol_id = %s""", (symbol_id,))
            basis = cur.fetchone()

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
        "has_older_bars": has_older_bars,
        "oldest_loaded_bar_time": oldest_loaded_bar_time,
        "latest_quote": latest_quote,
        "latest_bar": latest_bar,
        "sync_state": sync_state,
        "adjustment_basis": {
            "revision": basis[0], "source": basis[1], "modes": basis[2],
            "adjusted_through": basis[3].isoformat(), "checked_at": basis[4].isoformat(),
            "policy": "full_retained_history_refresh", "economic_actions_verified": False,
        } if basis else None,
    }


def _sync_market_data(symbol: str, timeframe: str, force_full: bool = False, snapshot_limit: int = 5000) -> Dict:
    return _market_sync_flights.run(
        ("sync", symbol.strip().upper(), timeframe, force_full, snapshot_limit),
        lambda: _sync_market_data_once(symbol, timeframe, force_full, snapshot_limit),
    )


def _sync_market_data_once(symbol: str, timeframe: str, force_full: bool = False, snapshot_limit: int = 5000) -> Dict:
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

    start = delayed_now - timedelta(days=_lookback_days_for_timeframe(timeframe))
    if latest_historical_bar_time and not force_full:
        start = latest_historical_bar_time.astimezone(timezone.utc) + TIMEFRAME_DELTAS.get(timeframe, timedelta(days=1))
    end = delayed_now

    bars_inserted = 0
    try:
        if start <= end:
            historical_bars = _refresh_adjusted_history(symbol_id, symbol, timeframe, start, end)
            bars_inserted = len(historical_bars)
        else:
            # Even without a new bar, configuration/provider corrections need a refresh.
            historical_bars = _refresh_adjusted_history(symbol_id, symbol, timeframe, end, end)
            bars_inserted = len(historical_bars)

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
                        last_quote_synced_at = CASE WHEN %s THEN now() ELSE last_quote_synced_at END,
                        last_bar_synced_at = CASE WHEN %s OR %s > 0 THEN now() ELSE last_bar_synced_at END,
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

    result = _get_market_snapshot(symbol, timeframe, snapshot_limit)
    result["fetched_from_alpaca"] = True
    result["bars_inserted"] = bars_inserted
    return result


def _sync_market_data_window(
    symbol: str,
    timeframe: str,
    start: datetime,
    end: datetime,
    snapshot_limit: int = 1,
) -> Dict:
    return _market_sync_flights.run(
        ("window", symbol.strip().upper(), timeframe, start, end, snapshot_limit),
        lambda: _sync_market_data_window_once(symbol, timeframe, start, end, snapshot_limit),
    )


def _sync_market_data_window_once(
    symbol: str,
    timeframe: str,
    start: datetime,
    end: datetime,
    snapshot_limit: int = 1,
) -> Dict:
    symbol = str(symbol).strip().upper()
    if timeframe not in SUPPORTED_TIMEFRAMES:
        raise ValueError(f"Unsupported timeframe '{timeframe}'.")

    symbol_id = _ensure_symbol_metadata(symbol)
    now = _utcnow()
    delayed_now = now - timedelta(minutes=max(CHART_DELAY_MINUTES, 0))
    bounded_end = min(end.astimezone(timezone.utc), delayed_now)
    bounded_start = start.astimezone(timezone.utc)

    if bounded_start > bounded_end:
        raise ValueError("Target sync window is in the future after delay adjustment.")

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

    bars_inserted = 0
    try:
        historical_bars = _refresh_adjusted_history(symbol_id, symbol, timeframe, bounded_start, bounded_end)
        bars_inserted = len(historical_bars)

        with _get_conn() as conn:
            with conn.cursor() as cur:
                effective_latest_bar_time = None
                if historical_bars:
                    effective_latest_bar_time = _parse_market_timestamp(historical_bars[-1].get("t"))
                cur.execute(
                    """
                    UPDATE symbol_sync_state
                    SET last_synced_at = now(),
                        last_successful_sync_at = now(),
                        last_bar_synced_at = CASE WHEN %s > 0 THEN now() ELSE last_bar_synced_at END,
                        latest_bar_time = COALESCE(%s, latest_bar_time),
                        sync_status = 'idle',
                        sync_error = NULL,
                        updated_at = now()
                    WHERE symbol_id = %s
                    """,
                    (
                        bars_inserted,
                        effective_latest_bar_time,
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

    result = _get_market_snapshot(symbol, timeframe, snapshot_limit)
    result["fetched_from_alpaca"] = True
    result["bars_inserted"] = bars_inserted
    result["requested_window_start"] = bounded_start.isoformat()
    result["requested_window_end"] = bounded_end.isoformat()
    return result


def _normalize_analytics_timeframe(value: str | None) -> str:
    raw = str(value or DEFAULT_CHART_TIMEFRAME).strip()
    if not raw:
        return DEFAULT_CHART_TIMEFRAME
    normalized = ANALYTICS_TIMEFRAME_ALIASES.get(raw.upper(), raw)
    if normalized not in SUPPORTED_TIMEFRAMES:
        raise ValueError(f"Unsupported timeframe '{raw}'.")
    return normalized


def _analytics_settings_from_request(source: Dict | None = None) -> AnalyticsSettings:
    source = source or {}

    def _get_value(name: str, default):
        if name in source and source.get(name) not in (None, ""):
            return source.get(name)
        if request.args.get(name) not in (None, ""):
            return request.args.get(name)
        return default

    def _to_int(name: str, default: int) -> int:
        raw = _get_value(name, default)
        try:
            value = int(raw)
        except (TypeError, ValueError) as exc:
            raise ValueError(f"'{name}' must be an integer.") from exc
        if value <= 0:
            raise ValueError(f"'{name}' must be greater than zero.")
        return value

    def _to_float(name: str, default: float) -> float:
        raw = _get_value(name, default)
        try:
            return float(raw)
        except (TypeError, ValueError) as exc:
            raise ValueError(f"'{name}' must be numeric.") from exc

    volume_confirmation_raw = _get_value("volume_confirmation", True)
    if isinstance(volume_confirmation_raw, str):
        volume_confirmation = volume_confirmation_raw.strip().lower() not in {"0", "false", "no"}
    else:
        volume_confirmation = bool(volume_confirmation_raw)

    return AnalyticsSettings(
        breakout_lookback=_to_int("breakout_lookback", 20),
        buffer_pct=_to_float("buffer_pct", 0.0025),
        volume_confirmation=volume_confirmation,
        volume_multiple=_to_float("volume_multiple", 1.5),
        event_limit=_to_int("event_limit", 20),
    )


def _analytics_price_timeframe_for_request(requested_timeframe: str, asof_dt: datetime | None) -> str:
    if requested_timeframe != "1Day" or asof_dt is None:
        return requested_timeframe

    ts = asof_dt.astimezone(timezone.utc)
    if ts.hour == 0 and ts.minute == 0 and ts.second == 0 and ts.microsecond == 0:
        return requested_timeframe
    return "15Min"


def _state_payload_has_desired_price_anchor(
    payload: Dict,
    desired_price_timeframe: str,
    asof_dt: datetime | None,
) -> bool:
    quality = payload.get("data_quality") or {}
    if quality.get("price_source_timeframe") != desired_price_timeframe:
        return False

    if desired_price_timeframe == "1Day" or asof_dt is None:
        return True

    raw_price_bar_timestamp = quality.get("price_bar_timestamp") or (payload.get("price") or {}).get("timestamp")
    if not raw_price_bar_timestamp:
        return False

    price_bar_timestamp = parse_asof(raw_price_bar_timestamp)
    if price_bar_timestamp is None:
        return False

    asof_ts = asof_dt.astimezone(timezone.utc)
    price_ts = price_bar_timestamp.astimezone(timezone.utc)
    if price_ts > asof_ts:
        return False

    # On weekdays we expect an intraday anchor from the same UTC trading date.
    # This avoids treating stale 15-minute rows from a prior day as a valid
    # backfill for a timed 1Day request.
    if asof_ts.weekday() < 5 and price_ts.date() != asof_ts.date():
        return False

    return True


def _parse_bool_query_param(name: str, default: bool = False) -> bool:
    raw = request.args.get(name)
    if raw is None:
        return default
    return str(raw).strip().lower() in {"1", "true", "yes", "on"}


def _portfolio_symbols(include_hidden: bool = False) -> List[str]:
    _init_db_if_needed()
    with _get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"SELECT symbol FROM {DB_TABLE}"
                + ("" if include_hidden else " WHERE hidden IS NOT TRUE")
                + " ORDER BY symbol ASC"
            )
            rows = cur.fetchall()
    # Preserve raw database ordering before normalization and first-occurrence
    # deduplication. Python handles Unicode case/whitespace just as pandas did;
    # SQL upper/trim/distinct would change those semantics or the ordering.
    symbols = [str(row[0]).upper().strip() for row in rows if row[0] is not None]
    return [symbol for symbol in dict.fromkeys(symbols) if symbol]


def _strip_symbol_and_timestamps(value):
    if isinstance(value, dict):
        out = {}
        for key, item in value.items():
            key_str = str(key)
            if key_str == "symbol":
                continue
            if key_str == "as_of":
                continue
            if key_str == "timestamp":
                continue
            if key_str.endswith("_timestamp") or key_str.endswith("_time") or key_str.endswith("_at"):
                continue
            out[key] = _strip_symbol_and_timestamps(item)
        return out
    if isinstance(value, list):
        return [_strip_symbol_and_timestamps(item) for item in value]
    return value


def _backfill_analytics_symbol_if_needed(symbol: str) -> None:
    _market_sync_flights.run(
        ("backfill", symbol.strip().upper()),
        lambda: _sync_market_data(symbol, "1Day", force_full=True, snapshot_limit=1),
    )


def _historical_price_coverage(symbol: str, timeframe: str, start: datetime, end: datetime):
    """Indexed bar bounds plus explicitly verified (possibly empty) windows."""
    symbol_id = _ensure_symbol_metadata(symbol.strip().upper())
    with _get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
            cur.execute(
                """
                SELECT
                    (SELECT max(bar_time) FROM stock_bars
                     WHERE symbol_id = %s AND timeframe = %s AND bar_time <= %s),
                    (SELECT max(bar_time) FROM stock_bars
                     WHERE symbol_id = %s AND timeframe = %s)
                """,
                (symbol_id, timeframe, end, symbol_id, timeframe),
            )
            anchor, latest = cur.fetchone()
            cur.execute(
                """
                SELECT window_start, window_end FROM market_data_coverage c
                JOIN market_data_basis b USING (symbol_id)
                WHERE symbol_id = %s AND timeframe = %s
                  AND c.adjustment_revision = b.revision AND c.adjustment_mode = %s
                  AND b.source = %s
                  AND window_end >= %s AND window_start <= %s
                ORDER BY window_start
                """,
                (symbol_id, timeframe, _alpaca_bar_adjustment_for_timeframe(timeframe), ALPACA_DATA_BASE_URL, start, end),
            )
            return anchor, latest, cur.fetchall()


def _prepare_historical_price_anchor(symbol: str, timeframe: str, asof_dt: datetime) -> None:
    def prepare():
        delayed_now = _utcnow() - timedelta(minutes=max(CHART_DELAY_MINUTES, 0))
        end = min(asof_dt, delayed_now)
        start = asof_dt - timedelta(days=7)
        if start > end:
            raise ValueError("Target sync window is in the future after delay adjustment.")
        anchor, latest, windows = _historical_price_coverage(symbol, timeframe, start, end)
        step = TIMEFRAME_DELTAS[timeframe]
        suitable = anchor is not None and anchor >= start
        payload = {"data_quality": {"price_source_timeframe": timeframe, "price_bar_timestamp": anchor}}
        if (suitable and latest >= end and latest > anchor
                and end - anchor < step and end + step <= delayed_now
                and _state_payload_has_desired_price_anchor(payload, timeframe, end)):
            return
        # A stored anchor supplies the left edge; never refetch its older history.
        cursor = max(start, anchor + step) if suitable else start
        # Refresh the last bar itself if asof falls inside its interval.
        if suitable and cursor > end:
            cursor = anchor
        tail_covered = False
        for covered_start, covered_end in windows:
            if covered_end < cursor:
                continue
            if covered_start > cursor:
                _sync_market_data_window(symbol, timeframe, cursor, min(covered_start, end))
            cursor = max(cursor, covered_end)
            if cursor >= end:
                tail_covered = True
                break
        if not tail_covered:
            _sync_market_data_window(symbol, timeframe, cursor, end)

    _market_sync_flights.run(("historical-anchor", symbol.strip().upper(), timeframe, asof_dt), prepare)


def _read_analytics_state(symbol, timeframe, asof_dt, settings):
    desired = _analytics_price_timeframe_for_request(timeframe, asof_dt)
    if desired != "1Day" and asof_dt is not None:
        _prepare_historical_price_anchor(symbol, desired, asof_dt)
    payload = analytics_service.get_symbol_state(symbol, timeframe, asof_dt, settings)
    if desired != "1Day":
        if asof_dt is not None:
            # A completed historical fetch may legitimately end on a closure,
            # but a daily fallback is not the requested intraday price anchor.
            quality = payload.get("data_quality") or {}
            timestamp = quality.get("price_bar_timestamp")
            if (quality.get("price_source_timeframe") != desired or not timestamp
                    or not asof_dt - timedelta(days=7) <= parse_asof(timestamp) <= asof_dt):
                raise AnalyticsNotFoundError(f"No {desired} price anchor available for '{symbol.upper()}' at or before asof.")
        elif not _state_payload_has_desired_price_anchor(payload, desired, asof_dt):
            _market_sync_flights.run(
                ("full", symbol.strip().upper(), desired),
                lambda: _sync_market_data(symbol, desired, force_full=True, snapshot_limit=1),
            )
            payload = analytics_service.get_symbol_state(symbol, timeframe, asof_dt, settings)
    return payload


def _sync_current_analytics_data_if_needed(symbol: str, requested_timeframe: str) -> None:
    return _market_sync_flights.run(
        ("current", symbol.strip().upper(), requested_timeframe),
        lambda: _sync_current_analytics_data_once(symbol, requested_timeframe),
    )


def _sync_current_analytics_data_once(symbol: str, requested_timeframe: str) -> None:
    timeframes = ["1Day"]
    if requested_timeframe != "1Day":
        timeframes.append(requested_timeframe)

    for timeframe in timeframes:
        snapshot = _get_market_snapshot(symbol, timeframe, 1)
        if _market_snapshot_needs_sync(snapshot, CHART_LATEST_AUTO_SYNC_MAX_AGE_DAYS):
            _sync_market_data(symbol, timeframe, force_full=False, snapshot_limit=1)


def _prepare_current_analytics_batch(symbols: List[str], requested_timeframe: str) -> Dict:
    """Bulk freshness checks, preserving the single-symbol freshness policy.

    Benchmarks share metadata but retain their existing no-auto-sync behavior.
    Sync results carry the new invalidation timestamps; do not reuse pre-sync
    cache keys or re-read metadata for symbols that already existed.
    """
    repository = analytics_service.repository
    contexts = repository.fetch_symbol_contexts([*symbols, "SPY", "QQQ"])
    symbols = list(dict.fromkeys(symbols))
    ids = [contexts[s]["symbol_id"] for s in symbols if s in contexts]
    timeframes = ["1Day"] if requested_timeframe == "1Day" else ["1Day", requested_timeframe]
    latest_by_timeframe = {
        tf: repository.fetch_bars_many(ids, tf, latest=True) for tf in timeframes
    }
    created = []
    for symbol in symbols:
        context = contexts.get(symbol)
        if context is None:
            # Match _get_market_snapshot's creation behavior even when automatic
            # freshness sync is disabled. Load the new context after any syncs.
            context = {"symbol": symbol, "symbol_id": _ensure_symbol_metadata(symbol)}
            contexts[symbol] = context
            created.append(symbol)
        for timeframe in timeframes:
            frame = latest_by_timeframe[timeframe].get(context["symbol_id"])
            snapshot = {
                "bars": [{"t": frame.iloc[-1]["timestamp"].isoformat()}] if frame is not None and not frame.empty else [],
                "sync_state": context or {},
            }
            if _market_snapshot_needs_sync(snapshot, CHART_LATEST_AUTO_SYNC_MAX_AGE_DAYS):
                result = _sync_market_data(symbol, timeframe, force_full=False, snapshot_limit=1)
                for key, value in result["sync_state"].items():
                    context[key] = _parse_market_timestamp(value) if key.endswith("_at") or key.endswith("_time") else value
    # Only newly created symbols need their new IDs, once sync has inserted them.
    if created:
        contexts.update(repository.fetch_symbol_contexts(created))
    return contexts


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
      let SORT_KEY = 'action_score';
      let SORT_ASC = false;
      let ROWS = [];
      let SHOW_HIDDEN = false;

      async function fetchData() {{
        const params = new URLSearchParams();
        if (SHOW_HIDDEN) params.set('include_hidden', '1');
        const res = await fetch(`/data?${{params.toString()}}`);
        const payload = await res.json();
        ROWS = payload.rows || [];
        renderTable();
        document.getElementById('last-refreshed').textContent = new Date().toLocaleString();
      }}

      function toggleHiddenRows() {{
        SHOW_HIDDEN = !SHOW_HIDDEN;
        const btn = document.getElementById('show-hidden');
        if (btn) btn.textContent = SHOW_HIDDEN ? 'Hide Hidden Rows' : 'Show Hidden Rows';
        fetchData();
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
        if (key === 'trend_state') {{
          const trendOrder = {{ Bearish: -1, Neutral: 0, Bullish: 1 }};
          return (trendOrder[av] ?? 0) - (trendOrder[bv] ?? 0);
        }}
        if (key === 'macd_signal') {{
          const macdOrder = {{ 'Bear cross': -2, Negative: -1, Neutral: 0, Positive: 1, 'Bull cross': 2 }};
          return (macdOrder[av] ?? 0) - (macdOrder[bv] ?? 0);
        }}
        if (typeof av === 'number' && typeof bv === 'number') return av - bv;
        return String(av).localeCompare(String(bv));
      }}

      function formatLastBarDate(value) {{
        if (!value) return '—';
        const parsed = new Date(value);
        if (Number.isNaN(parsed.getTime())) return String(value);
        return parsed.toLocaleDateString();
      }}

      function formatSignedPct(value) {{
        if (value == null) return '—';
        const num = Number(value);
        if (Number.isNaN(num)) return '—';
        return `${{num >= 0 ? '+' : ''}}${{num.toFixed(2)}}%`;
      }}

      async function updateBars(symbol, btn) {{
        if (!symbol || !btn) return;
        btn.disabled = true;
        const old = btn.textContent;
        btn.textContent = 'Updating...';
        try {{
          const res = await fetch(`/api/chart/${{encodeURIComponent(symbol)}}/sync`, {{
            method: 'POST',
            headers: {{ 'Content-Type': 'application/json' }},
            body: JSON.stringify({{ timeframe: '1Day', force_full: false, limit: 1 }}),
          }});
          const payload = await res.json();
          if (!res.ok || !payload.ok) {{
            throw new Error(payload.error || 'Failed to update bars.');
          }}
          await fetchData();
        }} catch (err) {{
          alert(err.message || String(err));
          btn.disabled = false;
          btn.textContent = old;
        }}
      }}

      async function setStockHidden(symbol, hidden, btn) {{
        if (!symbol || !btn) return;
        btn.disabled = true;
        const old = btn.textContent;
        btn.textContent = hidden ? 'Hiding...' : 'Restoring...';
        try {{
          const res = await fetch(`/api/stocks/${{encodeURIComponent(symbol)}}/hidden`, {{
            method: 'POST',
            headers: {{ 'Content-Type': 'application/json' }},
            body: JSON.stringify({{ hidden }}),
          }});
          const payload = await res.json();
          if (!res.ok || !payload.ok) {{
            throw new Error(payload.error || 'Failed to update hidden status.');
          }}
          await fetchData();
        }} catch (err) {{
          alert(err.message || String(err));
          btn.disabled = false;
          btn.textContent = old;
        }}
      }}

      async function updateAllBars() {{
        const btn = document.getElementById('update-all-bars');
        const symbols = [...new Set(ROWS.map((row) => row.symbol).filter(Boolean))];
        if (!btn || !symbols.length) return;
        btn.disabled = true;
        const old = btn.textContent;
        try {{
          for (let idx = 0; idx < symbols.length; idx += 1) {{
            btn.textContent = `Updating ${{idx + 1}}/${{symbols.length}}...`;
            const symbol = symbols[idx];
            const res = await fetch(`/api/chart/${{encodeURIComponent(symbol)}}/sync`, {{
              method: 'POST',
              headers: {{ 'Content-Type': 'application/json' }},
              body: JSON.stringify({{ timeframe: '1Day', force_full: false, limit: 1 }}),
            }});
            const payload = await res.json();
            if (!res.ok || !payload.ok) {{
              throw new Error(`${{symbol}}: ${{payload.error || 'Failed to update bars.'}}`);
            }}
          }}
          await fetchData();
        }} catch (err) {{
          alert(err.message || String(err));
        }} finally {{
          btn.disabled = false;
          btn.textContent = old;
        }}
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
          const actionScore = r.action_score == null ? '—' : Number(r.action_score).toFixed(1);
          const actionClass = r.action_score == null ? 'muted' : (r.action_score >= 75 ? 'pos' : (r.action_score < 40 ? 'neg' : ''));
          const rsi_14 = r.rsi_14 == null ? '—' : Number(r.rsi_14).toFixed(1);
          const rsiClass = r.rsi_14 == null ? 'muted' : (r.rsi_14 < 30 || (r.rsi_14 >= 50 && r.rsi_14 <= 70) ? 'pos' : (r.rsi_14 > 70 ? 'neg' : 'muted'));
          const macdSignal = r.macd_signal || 'Neutral';
          const macdClass = (macdSignal === 'Bull cross' || macdSignal === 'Positive') ? 'pos' : ((macdSignal === 'Bear cross' || macdSignal === 'Negative') ? 'neg' : 'muted');
          const distMa20 = formatSignedPct(r.dist_ma20_pct);
          const distMa50 = formatSignedPct(r.dist_ma50_pct);
          const distMa20Class = r.dist_ma20_pct == null ? 'muted' : (r.dist_ma20_pct >= 0 ? 'pos' : 'neg');
          const distMa50Class = r.dist_ma50_pct == null ? 'muted' : (r.dist_ma50_pct >= 0 ? 'pos' : 'neg');
          const next_earnings = (r.next_earnings == null || r.next_earnings === '') ? '—' : r.next_earnings;
          const last_bar = formatLastBarDate(r.latest_bar_time);
          const colorClass = r.diff_dollar == null ? '' : (r.diff_dollar >= 0 ? 'pos' : 'neg');
          const trendClass = r.trend_state === 'Bullish' ? 'pos' : (r.trend_state === 'Bearish' ? 'neg' : 'muted');
          const hiddenAction = r.hidden ? 'Restore' : 'Hide';
          const hiddenTarget = r.hidden ? 'false' : 'true';

          const tr = document.createElement('tr');
          if (r.hidden) tr.classList.add('hidden-row');
          tr.innerHTML = `
            <td><a href=\"/chart/${{r.symbol}}\" class=\"symbol-link\"><code>${{r.symbol}}</code></a></td>
            <td class=\"num\">${{forecast_price}}</td>
            <td class=\"num\">${{current_price}}</td>
            <td class=\"num ${{colorClass}}\">${{diff_dollar}}</td>
            <td class=\"num ${{colorClass}}\">${{diff_pct}}</td>
            <td class=\"num ${{rsiClass}}\">${{rsi_14}}</td>
            <td class=\"${{macdClass}}\">${{macdSignal}}</td>
            <td class=\"${{trendClass}}\">${{r.trend_state || 'Neutral'}}</td>
            <td class=\"num ${{distMa20Class}}\">${{distMa20}}</td>
            <td class=\"num ${{distMa50Class}}\">${{distMa50}}</td>
            <td class=\"num\">${{Number(r.reward_score).toFixed(1)}}</td>
            <td class=\"num\">${{Number(r.risk_score).toFixed(1)}}</td>
            <td class=\"num\">${{Number(r.confidence_score).toFixed(1)}}</td>
            <td>${{r.classification}}</td>
            <td class=\"num ${{actionClass}}\">${{actionScore}}</td>
            <td class=\"num\">${{Number(r.opportunity_score).toFixed(1)}}</td>
            <td class="date">${{r.updated_date || ''}}</td>
            <td class="date">${{next_earnings}}</td>
            <td class="date">${{last_bar}}</td>
            <td class="action"><button type="button" class="inline-btn secondary" onclick="updateBars('${{r.symbol}}', this)">Update Bars</button></td>
            <td class="action"><button type="button" class="inline-btn secondary" onclick="setStockHidden('${{r.symbol}}', ${{hiddenTarget}}, this)">${{hiddenAction}}</button></td>
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
        document.getElementById('update-all-bars').addEventListener('click', updateAllBars);
        document.getElementById('show-hidden').addEventListener('click', toggleHiddenRows);
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
      button.inline-btn {{ margin-top: 0; padding: 7px 10px; white-space: nowrap; }}
      tr.hidden-row {{ opacity: 0.55; }}
      .row {{ display: flex; gap: 8px; align-items: center; flex-wrap: wrap; }}
      table {{ width: 100%; border-collapse: collapse; margin-top: 10px; }}
      th, td {{ border-bottom: 1px solid var(--border); padding: 10px 8px; text-align: left; }}
      th {{ color: var(--muted); font-size: 13px; cursor: pointer; user-select: none; white-space: nowrap; }}
      td.num, th.num {{ text-align: right; font-variant-numeric: tabular-nums; }}
      td.date, th.date {{ white-space: nowrap; }}
      td.action, th.action {{ text-align: right; white-space: nowrap; }}
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
          <div class=\"row\" style=\"justify-content:space-between; align-items:flex-start;\">
            <div>
              <h3 style=\"margin-top:0\">Tracked Forecasts</h3>
              <div class=\"muted\">Opportunity = (Reward×4) + ((10-Risk)×4) + (Confidence×2). Click a symbol to open its chart and sync status.</div>
            </div>
            <div class=\"row\" style=\"justify-content:flex-end;\">
              <button type=\"button\" id=\"show-hidden\" class=\"secondary\">Show Hidden Rows</button>
              <button type=\"button\" id=\"update-all-bars\" class=\"secondary\">Update All Bars</button>
            </div>
          </div>
          <div class=\"table-wrap\">
            <table>
              <thead>
                <tr>
                  <th data-key=\"symbol\" onclick=\"sortBy('symbol')\">Symbol</th>
                  <th class=\"num\" data-key=\"forecast_price\" onclick=\"sortBy('forecast_price')\">Forecast ($)</th>
                  <th class=\"num\" data-key=\"current_price\" onclick=\"sortBy('current_price')\">Current ($)</th>
                  <th class=\"num\" data-key=\"diff_dollar\" onclick=\"sortBy('diff_dollar')\">Δ $</th>
                  <th class=\"num\" data-key=\"diff_pct\" onclick=\"sortBy('diff_pct')\">Δ %</th>
                  <th class=\"num\" data-key=\"rsi_14\" onclick=\"sortBy('rsi_14')\">RSI 14</th>
                  <th data-key=\"macd_signal\" onclick=\"sortBy('macd_signal')\">MACD</th>
                  <th data-key=\"trend_state\" onclick=\"sortBy('trend_state')\">Trend</th>
                  <th class=\"num\" data-key=\"dist_ma20_pct\" onclick=\"sortBy('dist_ma20_pct')\">vs MA20</th>
                  <th class=\"num\" data-key=\"dist_ma50_pct\" onclick=\"sortBy('dist_ma50_pct')\">vs MA50</th>
                  <th class=\"num\" data-key=\"reward_score\" onclick=\"sortBy('reward_score')\">Reward</th>
                  <th class=\"num\" data-key=\"risk_score\" onclick=\"sortBy('risk_score')\">Risk</th>
                  <th class=\"num\" data-key=\"confidence_score\" onclick=\"sortBy('confidence_score')\">Confidence</th>
                  <th data-key=\"classification\" onclick=\"sortBy('classification')\">Class</th>
                  <th class=\"num\" data-key=\"action_score\" onclick=\"sortBy('action_score')\">Action</th>
                  <th class=\"num\" data-key=\"opportunity_score\" onclick=\"sortBy('opportunity_score')\">Opportunity</th>
                  <th class=\"date\" data-key=\"updated_date\" onclick=\"sortBy('updated_date')\">Updated</th>
                  <th class=\"date\" data-key=\"next_earnings\" onclick=\"sortBy('next_earnings')\">Next Earnings</th>
                  <th class=\"date\" data-key=\"latest_bar_time\" onclick=\"sortBy('latest_bar_time')\">Last Bar</th>
                  <th class=\"action\">Bars</th>
                  <th class=\"action\">Tracker</th>
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


@app.post("/api/stocks/<symbol>/hidden")
def set_stock_hidden(symbol: str) -> Response:
    payload = request.get_json(force=True, silent=True) or {}
    raw_hidden = payload.get("hidden", True)
    hidden = raw_hidden if isinstance(raw_hidden, bool) else str(raw_hidden).strip().lower() in {"1", "true", "yes", "on"}
    try:
        found = _set_stock_hidden(symbol, hidden)
    except ValueError as e:
        return jsonify({"ok": False, "error": str(e)}), 400
    except Exception as e:
        return jsonify({"ok": False, "error": f"Database error: {e}"}), 500
    if not found:
        return jsonify({"ok": False, "error": f"Unknown symbol: {str(symbol).strip().upper()}"}), 404
    return jsonify({"ok": True, "symbol": str(symbol).strip().upper(), "hidden": hidden})


@app.get("/data")
def data() -> Response:
    df = _read_df()
    include_hidden = (request.args.get("include_hidden") or "").strip().lower() in {"1", "true", "yes", "on"}
    if not include_hidden and not df.empty:
        df = df[~df["hidden"].astype(bool)].copy()
    symbols = [] if df.empty else sorted(df["symbol"].dropna().astype(str).str.upper().unique().tolist())

    prices: Dict[str, float | None] = _fetch_current_prices(symbols)
    next_map: Dict[str, str] = _load_next_earnings_map()
    daily_closes, latest_bar_map = _daily_bar_snapshot(symbols)
    trend_map: Dict[str, str] = _trend_states(symbols, daily_closes)
    rsi_14_map: Dict[str, float | None] = _rsi_14_values(symbols, daily_closes)
    macd_signal_map: Dict[str, str] = _macd_signals(symbols, daily_closes)
    ma_distance_map = _ma_distance_pcts(symbols, prices, daily_closes)

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
            ma_distances = ma_distance_map.get(sym, {})

            diff_dollar = None
            diff_pct = None
            if cur is not None and fpx not in (None, 0):
                diff_dollar = fpx - cur
                try:
                    diff_pct = (diff_dollar / cur) * 100.0
                except Exception:
                    diff_pct = None

            row_data = {
                "symbol": sym,
                "forecast_price": fpx,
                "reward_score": reward_score,
                "risk_score": risk_score,
                "confidence_score": confidence_score,
                "classification": classification,
                "hidden": bool(r.get("hidden")),
                "opportunity_score": _opportunity_score(reward_score, risk_score, confidence_score),
                "updated_date": str(r.get("updated_date", "")),
                "next_earnings": next_map.get(sym),
                "current_price": cur,
                "diff_dollar": diff_dollar,
                "diff_pct": diff_pct,
                "rsi_14": rsi_14_map.get(sym),
                "macd_signal": macd_signal_map.get(sym, "Neutral"),
                "trend_state": trend_map.get(sym, "Neutral"),
                "dist_ma20_pct": ma_distances.get("dist_ma20_pct"),
                "dist_ma50_pct": ma_distances.get("dist_ma50_pct"),
                "latest_bar_time": latest_bar_map.get(sym),
            }
            row_data["action_score"] = _action_score(row_data)
            rows.append(row_data)

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
        max-width: min(1800px, calc(100vw - 24px));
        margin: 24px auto;
        padding: 0 12px 40px;
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
        grid-template-columns: minmax(0, 1fr) minmax(280px, 340px);
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
        height: clamp(520px, 68vh, 760px);
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
      button, select, input {{
        border-radius: 12px;
        border: 1px solid var(--border);
        background: #112033;
        color: var(--text);
        padding: 10px 14px;
      }}
      input[type="checkbox"] {{
        width: 16px;
        height: 16px;
        padding: 0;
        accent-color: var(--accent);
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
      .toolbar-group {{
        display: flex;
        flex-wrap: wrap;
        gap: 8px;
        align-items: center;
      }}
      .toggle {{
        display: inline-flex;
        align-items: center;
        gap: 6px;
        color: var(--muted);
        font-size: 14px;
        white-space: nowrap;
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
      .chart-help {{
        margin-top: 10px;
        color: var(--muted);
        font-size: 13px;
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
          <div class="toolbar-group">
            <label class="muted" for="timeframe">Timeframe</label>
            <select id="timeframe">
              <option value="1Day" selected>1 Day</option>
              <option value="1Hour">1 Hour</option>
              <option value="15Min">15 Min</option>
              <option value="5Min">5 Min</option>
              <option value="1Min">1 Min</option>
            </select>
          </div>
          <div class="toolbar-group">
            <label class="muted" for="chart-type">Chart</label>
            <select id="chart-type">
              <option value="line">Line</option>
              <option value="candles" selected>Candlesticks</option>
            </select>
          </div>
          <div class="toolbar-group" aria-label="Moving average overlays">
            <span class="muted">MA</span>
            <label class="toggle"><input id="ma-20" type="checkbox" checked /> 20</label>
            <label class="toggle"><input id="ma-50" type="checkbox" /> 50</label>
            <label class="toggle"><input id="ma-200" type="checkbox" /> 200</label>
            <label class="toggle"><input id="ema-20" type="checkbox" /> EMA 20</label>
            <label class="toggle"><input id="bollinger-20" type="checkbox" /> Bollinger 20</label>
            <label class="toggle"><input id="volume-bars" type="checkbox" checked /> Volume</label>
            <label class="toggle"><input id="rsi-14" type="checkbox" /> RSI 14</label>
            <label class="toggle"><input id="macd" type="checkbox" /> MACD</label>
            <label class="toggle"><input id="support-resistance" type="checkbox" /> Support/Resistance</label>
            <label class="toggle"><input id="trade-signals" type="checkbox" /> Signals</label>
          </div>
          <div class="toolbar-group">
            <button id="pan-left-btn" class="secondary" type="button">Earlier</button>
            <button id="pan-right-btn" class="secondary" type="button">Later</button>
            <button id="zoom-in-btn" class="secondary" type="button">Zoom In</button>
            <button id="zoom-out-btn" class="secondary" type="button">Zoom Out</button>
            <button id="reset-view-btn" class="secondary" type="button">Reset View</button>
          </div>
          <div class="toolbar-group">
            <button id="sync-btn" type="button">Sync Now</button>
            <button id="reload-btn" class="secondary" type="button">Reload DB Data</button>
          </div>
        </div>
      </div>

      <div class="grid">
        <div class="card">
          <div id="chart-summary" class="muted">Loading chart data...</div>
          <div id="range-summary" class="chart-help">Preparing chart viewport...</div>
          <div id="hover-summary" class="chart-help">Hover over the chart to inspect a bar.</div>
          <div class="canvas-wrap">
            <canvas id="chart" width="1120" height="560"></canvas>
          </div>
          <div class="chart-help">Mouse wheel zooms. Drag horizontally to pan. Use Earlier/Later to move through older bars.</div>
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
              <span class="stat-label">Chart Last Bar</span>
              <div class="stat-value" id="chart-last-bar-time">-</div>
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
      let ALL_BARS = [];
      let VIEW_END = 0;
      let WINDOW_SIZE = 0;
      let CURRENT_TIMEFRAME = '1Day';
      let DRAG_STATE = null;
      let HOVER_INDEX = null;
      let HOVER_CANVAS_X = null;
      let LAST_DRAW_STATE = null;
      let HAS_OLDER_BARS = false;
      let FETCHING_OLDER_BARS = false;
      let SR_CACHE = {{ signature: '', levels: [] }};
      const MOVING_AVERAGES = [
        {{ period: 20, id: 'ma-20', color: '#fdb022' }},
        {{ period: 50, id: 'ma-50', color: '#b692f6' }},
        {{ period: 200, id: 'ma-200', color: '#12b76a' }},
      ];
      const EMA_OVERLAYS = [
        {{ period: 20, id: 'ema-20', color: '#2dd4bf' }},
      ];
      const BOLLINGER_20 = {{ period: 20, id: 'bollinger-20', color: '#7cc4ff' }};

      function fmtDate(value) {{
        if (!value) return '-';
        return new Date(value).toLocaleString();
      }}

      function fmtPrice(value) {{
        if (value == null || Number.isNaN(Number(value))) return '-';
        return Number(value).toLocaleString(undefined, {{ minimumFractionDigits: 2, maximumFractionDigits: 2 }});
      }}

      function fmtVolume(value) {{
        if (value == null || Number.isNaN(Number(value))) return '-';
        return Number(value).toLocaleString(undefined, {{ maximumFractionDigits: 0 }});
      }}

      function fmtPct(value) {{
        if (value == null || Number.isNaN(Number(value))) return '-';
        const sign = Number(value) > 0 ? '+' : '';
        return `${{sign}}${{Number(value).toLocaleString(undefined, {{ minimumFractionDigits: 2, maximumFractionDigits: 2 }})}}%`;
      }}

      function setStatus(message, isError = false) {{
        const el = document.getElementById('status');
        el.style.display = message ? 'block' : 'none';
        el.textContent = message || '';
        el.classList.toggle('error', !!isError);
      }}

      function resizeCanvasToDisplay() {{
        const rect = canvas.getBoundingClientRect();
        const nextWidth = Math.max(320, Math.round(rect.width));
        const nextHeight = Math.max(360, Math.round(rect.height));
        if (canvas.width === nextWidth && canvas.height === nextHeight) return false;
        canvas.width = nextWidth;
        canvas.height = nextHeight;
        return true;
      }}

      function clearCanvas() {{
        ctx.clearRect(0, 0, canvas.width, canvas.height);
        LAST_DRAW_STATE = null;
        ctx.fillStyle = '#9fb0c0';
        ctx.font = '16px Georgia';
        ctx.fillText('No chart data yet', 24, 40);
      }}

      function formatAxisLabel(value) {{
        const date = new Date(value);
        if (CURRENT_TIMEFRAME === '1Day') return date.toLocaleDateString();
        return date.toLocaleString(undefined, {{
          month: 'short',
          day: 'numeric',
          hour: 'numeric',
          minute: '2-digit',
        }});
      }}

      function defaultWindowSize(timeframe, totalBars) {{
        const defaults = {{
          '1Min': 240,
          '5Min': 288,
          '15Min': 320,
          '1Hour': 360,
          '1Day': 260,
        }};
        return Math.max(20, Math.min(defaults[timeframe] || 180, totalBars || defaults[timeframe] || 180));
      }}

      function desiredFetchLimit(timeframe) {{
        const limits = {{
          '1Min': 1500,
          '5Min': 1500,
          '15Min': {CHART_15MIN_FETCH_LIMIT},
          '1Hour': 1200,
          '1Day': 3000,
        }};
        return String(limits[timeframe] || 1000);
      }}

      function clampViewport() {{
        if (!ALL_BARS.length) {{
          VIEW_END = 0;
          WINDOW_SIZE = 0;
          return;
        }}
        WINDOW_SIZE = Math.max(20, Math.min(WINDOW_SIZE || defaultWindowSize(CURRENT_TIMEFRAME, ALL_BARS.length), ALL_BARS.length));
        VIEW_END = Math.max(WINDOW_SIZE, Math.min(VIEW_END || ALL_BARS.length, ALL_BARS.length));
      }}

      function getVisibleBars() {{
        clampViewport();
        if (!ALL_BARS.length) return [];
        return ALL_BARS.slice(getVisibleStart(), VIEW_END);
      }}

      function getVisibleStart() {{
        clampViewport();
        return ALL_BARS.length ? Math.max(0, VIEW_END - WINDOW_SIZE) : 0;
      }}

      function clampHoverIndex(bars) {{
        if (!bars.length) {{
          HOVER_INDEX = null;
          return;
        }}
        if (HOVER_INDEX == null) return;
        HOVER_INDEX = Math.max(0, Math.min(HOVER_INDEX, bars.length - 1));
      }}

      function getChartType() {{
        return document.getElementById('chart-type')?.value || 'line';
      }}

      function getEnabledMovingAverages() {{
        return MOVING_AVERAGES.filter((ma) => document.getElementById(ma.id)?.checked);
      }}

      function getEnabledEmas() {{
        return EMA_OVERLAYS.filter((ema) => document.getElementById(ema.id)?.checked);
      }}

      function showVolumeBars() {{
        return !!document.getElementById('volume-bars')?.checked;
      }}

      function showRsi14() {{
        return !!document.getElementById('rsi-14')?.checked;
      }}

      function showMacd() {{
        return !!document.getElementById('macd')?.checked;
      }}

      function showBollinger20() {{
        return !!document.getElementById(BOLLINGER_20.id)?.checked;
      }}

      function showSupportResistance() {{
        return !!document.getElementById('support-resistance')?.checked;
      }}

      function showTradeSignals() {{
        return !!document.getElementById('trade-signals')?.checked;
      }}

      function signalSummary(signals) {{
        return (signals || []).map((signal) => signal.label).join(', ');
      }}

      function supportResistanceTolerance(bars) {{
        if (!bars.length) return 0;
        const highs = bars.map((bar) => Number(bar.h)).filter((value) => !Number.isNaN(value));
        const lows = bars.map((bar) => Number(bar.l)).filter((value) => !Number.isNaN(value));
        if (!highs.length || !lows.length) return 0;
        const priceRange = Math.max(...highs) - Math.min(...lows);
        const avgRange = bars.reduce((sum, bar) => sum + Math.max(Number(bar.h) - Number(bar.l), 0), 0) / bars.length;
        return Math.max(avgRange * 0.8, priceRange * 0.003, 0.01);
      }}

      function supportResistanceLevels() {{
        const bars = ALL_BARS;
        const signature = `${{CURRENT_TIMEFRAME}}|${{bars.length}}|${{bars[0]?.t || ''}}|${{bars[bars.length - 1]?.t || ''}}`;
        if (SR_CACHE.signature === signature) return SR_CACHE.levels;
        if (bars.length < 8) {{
          SR_CACHE = {{ signature, levels: [] }};
          return SR_CACHE.levels;
        }}
        const tolerance = supportResistanceTolerance(bars);
        if (!tolerance) {{
          SR_CACHE = {{ signature, levels: [] }};
          return SR_CACHE.levels;
        }}
        const pivotSpan = bars.length > 1000 ? 3 : 2;
        const pivots = [];

        for (let idx = pivotSpan; idx < bars.length - pivotSpan; idx++) {{
          const high = Number(bars[idx].h);
          const low = Number(bars[idx].l);
          if (Number.isNaN(high) || Number.isNaN(low)) continue;

          let isSwingHigh = true;
          let isSwingLow = true;
          for (let offset = 1; offset <= pivotSpan; offset++) {{
            if (high < Number(bars[idx - offset].h) || high < Number(bars[idx + offset].h)) isSwingHigh = false;
            if (low > Number(bars[idx - offset].l) || low > Number(bars[idx + offset].l)) isSwingLow = false;
          }}
          if (isSwingHigh) pivots.push({{ price: high, idx, side: 'resistance' }});
          if (isSwingLow) pivots.push({{ price: low, idx, side: 'support' }});
        }}
        if (!pivots.length) {{
          SR_CACHE = {{ signature, levels: [] }};
          return SR_CACHE.levels;
        }}

        const clusters = [];
        pivots
          .sort((a, b) => a.price - b.price)
          .forEach((pivot) => {{
            const cluster = clusters.find((item) => Math.abs(item.price - pivot.price) <= tolerance);
            const weight = 1 + (pivot.idx / Math.max(bars.length - 1, 1));
            if (!cluster) {{
              clusters.push({{
                price: pivot.price,
                weightTotal: weight,
                pivotCount: 1,
                highCount: pivot.side === 'resistance' ? 1 : 0,
                lowCount: pivot.side === 'support' ? 1 : 0,
                latestPivotIndex: pivot.idx,
              }});
              return;
            }}
            cluster.price = ((cluster.price * cluster.weightTotal) + (pivot.price * weight)) / (cluster.weightTotal + weight);
            cluster.weightTotal += weight;
            cluster.pivotCount += 1;
            cluster.highCount += pivot.side === 'resistance' ? 1 : 0;
            cluster.lowCount += pivot.side === 'support' ? 1 : 0;
            cluster.latestPivotIndex = Math.max(cluster.latestPivotIndex, pivot.idx);
          }});

        const referenceClose = Number(bars[bars.length - 1]?.c);
        const levels = clusters.map((cluster) => {{
          let touchCount = 0;
          let latestTouchIndex = cluster.latestPivotIndex;
          bars.forEach((bar, idx) => {{
            const high = Number(bar.h);
            const low = Number(bar.l);
            const close = Number(bar.c);
            const reacted = Math.abs(high - cluster.price) <= tolerance
              || Math.abs(low - cluster.price) <= tolerance
              || Math.abs(close - cluster.price) <= tolerance;
            if (reacted) {{
              touchCount += 1;
              latestTouchIndex = idx;
            }}
          }});
          const recentReaction = latestTouchIndex / Math.max(bars.length - 1, 1);
          const side = !Number.isNaN(referenceClose)
            ? (cluster.price <= referenceClose ? 'support' : 'resistance')
            : (cluster.highCount >= cluster.lowCount ? 'resistance' : 'support');
          return {{
            price: cluster.price,
            side,
            touches: touchCount,
            score: (touchCount * 10) + (cluster.pivotCount * 4) + (recentReaction * 8),
            latestTouchIndex,
          }};
        }}).sort((a, b) => b.score - a.score);
        SR_CACHE = {{ signature, levels }};
        return SR_CACHE.levels;
      }}

      function nearestSupportResistance(levels, referencePrice) {{
        const price = Number(referencePrice);
        if (!levels.length || Number.isNaN(price)) return {{ support: null, resistance: null }};
        const supports = levels.filter((level) => level.price <= price).sort((a, b) => price - a.price - (price - b.price));
        const resistances = levels.filter((level) => level.price >= price).sort((a, b) => a.price - price - (b.price - price));
        return {{ support: supports[0] || null, resistance: resistances[0] || null }};
      }}

      function formatSrDistance(label, level, referencePrice) {{
        const price = Number(referencePrice);
        if (!level || Number.isNaN(price)) return `${{label}} -`;
        const distance = Math.abs(price - level.price);
        const pct = price ? (distance / price) * 100 : 0;
        return `${{label}} $${{fmtPrice(level.price)}} (${{fmtPrice(distance)}}, ${{fmtPrice(pct)}}%)`;
      }}

      function bollingerStatus(close, band) {{
        if (!band) return `warming up; needs ${{BOLLINGER_20.period}} bars`;
        const price = Number(close);
        if (price > Number(band.upper)) return 'above upper band';
        if (price < Number(band.lower)) return 'below lower band';
        return 'inside channel';
      }}

      function movingAverageValues(bars, period) {{
        const values = [];
        let sum = 0;
        bars.forEach((bar, idx) => {{
          sum += Number(bar.c);
          if (idx >= period) sum -= Number(bars[idx - period].c);
          values.push(idx >= period - 1 ? sum / period : null);
        }});
        return values;
      }}

      function volumeAverageValues(bars, period = 20) {{
        const values = [];
        let sum = 0;
        bars.forEach((bar, idx) => {{
          sum += Number(bar.v || 0);
          if (idx >= period) sum -= Number(bars[idx - period].v || 0);
          values.push(idx >= period - 1 ? sum / period : null);
        }});
        return values;
      }}

      function drawHoverInfoBox(lines, padLeft, padTop, padRight) {{
        if (!lines.length) return;
        ctx.save();
        ctx.font = '12px Georgia';
        const lineHeight = 17;
        const paddingX = 10;
        const paddingY = 8;
        const maxTextWidth = Math.max(...lines.map((line) => ctx.measureText(line.text).width));
        const boxWidth = Math.min(canvas.width - padLeft - padRight - 16, Math.max(260, maxTextWidth + (paddingX * 2)));
        const boxHeight = (lines.length * lineHeight) + (paddingY * 2);
        const boxX = padLeft + 8;
        const boxY = padTop + 8;

        ctx.fillStyle = 'rgba(7, 17, 28, 0.94)';
        ctx.fillRect(boxX, boxY, boxWidth, boxHeight);
        ctx.strokeStyle = 'rgba(124, 196, 255, 0.55)';
        ctx.strokeRect(boxX, boxY, boxWidth, boxHeight);
        lines.forEach((line, idx) => {{
          ctx.fillStyle = line.color || '#d7f0ff';
          ctx.fillText(line.text, boxX + paddingX, boxY + paddingY + 12 + (idx * lineHeight), boxWidth - (paddingX * 2));
        }});
        ctx.restore();
      }}

      function emaValues(bars, period) {{
        const values = [];
        const multiplier = 2 / (period + 1);
        let ema = null;
        bars.forEach((bar, idx) => {{
          const close = Number(bar.c);
          if (idx === 0) ema = close;
          else ema = (close * multiplier) + (ema * (1 - multiplier));
          values.push(idx >= period - 1 ? ema : null);
        }});
        return values;
      }}

      function bollingerValues(bars, period = 20, deviations = 2) {{
        const values = [];
        let sum = 0;
        let sumSquares = 0;
        bars.forEach((bar, idx) => {{
          const close = Number(bar.c);
          sum += close;
          sumSquares += close * close;
          if (idx >= period) {{
            const outgoing = Number(bars[idx - period].c);
            sum -= outgoing;
            sumSquares -= outgoing * outgoing;
          }}
          if (idx < period - 1) {{
            values.push(null);
            return;
          }}
          const middle = sum / period;
          const variance = Math.max((sumSquares / period) - (middle * middle), 0);
          const stdDev = Math.sqrt(variance);
          values.push({{
            middle,
            upper: middle + (deviations * stdDev),
            lower: middle - (deviations * stdDev),
          }});
        }});
        return values;
      }}

      function rsiValues(bars, period = 14) {{
        const values = Array(bars.length).fill(null);
        if (bars.length <= period) return values;

        let gainSum = 0;
        let lossSum = 0;
        for (let idx = 1; idx <= period; idx++) {{
          const change = Number(bars[idx].c) - Number(bars[idx - 1].c);
          if (change >= 0) gainSum += change;
          else lossSum += Math.abs(change);
        }}

        let avgGain = gainSum / period;
        let avgLoss = lossSum / period;
        values[period] = avgLoss === 0 ? 100 : 100 - (100 / (1 + (avgGain / avgLoss)));

        for (let idx = period + 1; idx < bars.length; idx++) {{
          const change = Number(bars[idx].c) - Number(bars[idx - 1].c);
          const gain = Math.max(change, 0);
          const loss = Math.max(-change, 0);
          avgGain = ((avgGain * (period - 1)) + gain) / period;
          avgLoss = ((avgLoss * (period - 1)) + loss) / period;
          values[idx] = avgLoss === 0 ? 100 : 100 - (100 / (1 + (avgGain / avgLoss)));
        }}

        return values;
      }}

      function emaNumberValues(numbers, period) {{
        const values = [];
        const multiplier = 2 / (period + 1);
        let ema = null;
        let validCount = 0;
        numbers.forEach((raw) => {{
          const value = Number(raw);
          if (raw == null || Number.isNaN(value)) {{
            values.push(null);
            return;
          }}
          validCount += 1;
          ema = ema == null ? value : (value * multiplier) + (ema * (1 - multiplier));
          values.push(validCount >= period ? ema : null);
        }});
        return values;
      }}

      function macdValues(bars) {{
        const closes = bars.map((bar) => Number(bar.c));
        const ema12 = emaNumberValues(closes, 12);
        const ema26 = emaNumberValues(closes, 26);
        const macdLine = closes.map((_, idx) => (
          ema12[idx] != null && ema26[idx] != null ? ema12[idx] - ema26[idx] : null
        ));
        const signalLine = emaNumberValues(macdLine, 9);
        return macdLine.map((macd, idx) => {{
          const signal = signalLine[idx];
          return {{
            macd,
            signal,
            hist: macd != null && signal != null ? macd - signal : null,
          }};
        }});
      }}

      function tradeSignalValues(bars) {{
        const signals = Array.from({{ length: bars.length }}, () => []);
        if (!bars.length) return signals;

        const ma20 = movingAverageValues(bars, 20);
        const ma50 = movingAverageValues(bars, 50);
        const rsi14 = rsiValues(bars, 14);
        const macdSeries = macdValues(bars);
        const addSignal = (idx, signal) => {{
          signals[idx].push(signal);
        }};

        for (let idx = 1; idx < bars.length; idx++) {{
          const close = Number(bars[idx].c);
          const prevClose = Number(bars[idx - 1].c);
          if (Number.isNaN(close) || Number.isNaN(prevClose)) continue;

          if (idx >= 20) {{
            const recentBars = bars.slice(idx - 20, idx);
            const recentHigh = Math.max(...recentBars.map((bar) => Number(bar.h)).filter((value) => !Number.isNaN(value)));
            const recentLow = Math.min(...recentBars.map((bar) => Number(bar.l)).filter((value) => !Number.isNaN(value)));
            if (!Number.isNaN(recentHigh) && close > recentHigh) {{
              addSignal(idx, {{ type: 'breakout', label: 'BO', side: 'up', color: '#17b26a', detail: `Breakout: close $${{fmtPrice(close)}} above 20-bar high $${{fmtPrice(recentHigh)}}` }});
            }}
            if (!Number.isNaN(recentLow) && close < recentLow) {{
              addSignal(idx, {{ type: 'breakdown', label: 'BD', side: 'down', color: '#f04438', detail: `Breakdown: close $${{fmtPrice(close)}} below 20-bar low $${{fmtPrice(recentLow)}}` }});
            }}
          }}

          const currMa50 = ma50[idx];
          const prevMa50 = ma50[idx - 1];
          if (currMa50 != null && prevMa50 != null) {{
            if (prevClose <= prevMa50 && close > currMa50) {{
              addSignal(idx, {{ type: 'trend-up', label: 'T+', side: 'up', color: '#b692f6', detail: `Trend change: close crossed above MA50 $${{fmtPrice(currMa50)}}` }});
            }} else if (prevClose >= prevMa50 && close < currMa50) {{
              addSignal(idx, {{ type: 'trend-down', label: 'T-', side: 'down', color: '#b692f6', detail: `Trend change: close crossed below MA50 $${{fmtPrice(currMa50)}}` }});
            }}
          }}

          const currRsi = rsi14[idx];
          const prevRsi = rsi14[idx - 1];
          if (currRsi != null && prevRsi != null) {{
            if (prevRsi <= 70 && currRsi > 70) {{
              addSignal(idx, {{ type: 'overbought', label: 'OB', side: 'down', color: '#f97066', detail: `RSI14 crossed above 70: ${{fmtPrice(currRsi)}}` }});
            }} else if (prevRsi >= 30 && currRsi < 30) {{
              addSignal(idx, {{ type: 'oversold', label: 'OS', side: 'up', color: '#2dd4bf', detail: `RSI14 crossed below 30: ${{fmtPrice(currRsi)}}` }});
            }}
          }}

          const currMacd = macdSeries[idx];
          const prevMacd = macdSeries[idx - 1];
          if (currMacd?.macd != null && currMacd?.signal != null && prevMacd?.macd != null && prevMacd?.signal != null) {{
            const currDiff = Number(currMacd.macd) - Number(currMacd.signal);
            const prevDiff = Number(prevMacd.macd) - Number(prevMacd.signal);
            if (prevDiff <= 0 && currDiff > 0) {{
              addSignal(idx, {{ type: 'macd-up', label: 'M+', side: 'up', color: '#7cc4ff', detail: `MACD crossed above signal: ${{fmtPrice(currMacd.macd)}} / ${{fmtPrice(currMacd.signal)}}` }});
            }} else if (prevDiff >= 0 && currDiff < 0) {{
              addSignal(idx, {{ type: 'macd-down', label: 'M-', side: 'down', color: '#7cc4ff', detail: `MACD crossed below signal: ${{fmtPrice(currMacd.macd)}} / ${{fmtPrice(currMacd.signal)}}` }});
            }}
          }}

          const currMa20 = ma20[idx];
          const prevMa20 = ma20[idx - 1];
          if (currMa20 != null && currMa50 != null && prevMa20 != null && prevMa50 != null) {{
            const currDiff = Number(currMa20) - Number(currMa50);
            const prevDiff = Number(prevMa20) - Number(prevMa50);
            if (prevDiff <= 0 && currDiff > 0) {{
              addSignal(idx, {{ type: 'ma-up', label: 'X+', side: 'up', color: '#fdb022', detail: `MA20 crossed above MA50: $${{fmtPrice(currMa20)}} / $${{fmtPrice(currMa50)}}` }});
            }} else if (prevDiff >= 0 && currDiff < 0) {{
              addSignal(idx, {{ type: 'ma-down', label: 'X-', side: 'down', color: '#fdb022', detail: `MA20 crossed below MA50: $${{fmtPrice(currMa20)}} / $${{fmtPrice(currMa50)}}` }});
            }}
          }}
        }}

        return signals;
      }}

      function drawLineSeries(values, pointX, pointY, color, width = 2) {{
        ctx.beginPath();
        let started = false;
        values.forEach((value, idx) => {{
          if (value == null || Number.isNaN(Number(value))) return;
          const x = pointX(idx);
          const y = pointY(Number(value));
          if (!started) {{
            ctx.moveTo(x, y);
            started = true;
          }} else {{
            ctx.lineTo(x, y);
          }}
        }});
        if (!started) return;
        ctx.strokeStyle = color;
        ctx.lineWidth = width;
        ctx.stroke();
      }}

      function drawBollingerChannel(values, pointX, pointY) {{
        const points = values
          .map((band, idx) => band ? {{ idx, upper: Number(band.upper), lower: Number(band.lower) }} : null)
          .filter((point) => point && !Number.isNaN(point.upper) && !Number.isNaN(point.lower));
        if (!points.length) return;

        ctx.beginPath();
        points.forEach((point, idx) => {{
          const x = pointX(point.idx);
          const y = pointY(point.upper);
          if (idx === 0) ctx.moveTo(x, y);
          else ctx.lineTo(x, y);
        }});
        [...points].reverse().forEach((point) => {{
          ctx.lineTo(pointX(point.idx), pointY(point.lower));
        }});
        ctx.closePath();
        ctx.fillStyle = 'rgba(124, 196, 255, 0.12)';
        ctx.fill();

        drawLineSeries(values.map((band) => band ? band.upper : null), pointX, pointY, 'rgba(124, 196, 255, 0.78)', 1.5);
        drawLineSeries(values.map((band) => band ? band.lower : null), pointX, pointY, 'rgba(124, 196, 255, 0.78)', 1.5);
      }}

      function drawSupportResistanceLevels(levels, pointY, padLeft, padRight, min, max) {{
        const visibleLevels = levels
          .filter((level) => level.price >= min && level.price <= max)
          .sort((a, b) => b.score - a.score)
          .slice(0, 10)
          .sort((a, b) => b.price - a.price);
        if (!visibleLevels.length) return;

        ctx.save();
        ctx.font = '12px Georgia';
        visibleLevels.forEach((level) => {{
          const y = pointY(level.price);
          const color = level.side === 'support' ? '#17b26a' : '#f97066';
          ctx.strokeStyle = level.side === 'support' ? 'rgba(23, 178, 106, 0.72)' : 'rgba(249, 112, 102, 0.72)';
          ctx.lineWidth = 1.5;
          ctx.setLineDash([7, 5]);
          ctx.beginPath();
          ctx.moveTo(padLeft, y);
          ctx.lineTo(canvas.width - padRight, y);
          ctx.stroke();
          ctx.setLineDash([]);

          const label = `${{level.side === 'support' ? 'Support' : 'Resistance'}} $${{fmtPrice(level.price)}}`;
          const labelWidth = ctx.measureText(label).width + 12;
          const labelX = Math.max(padLeft + 6, canvas.width - padRight - labelWidth - 4);
          const labelY = Math.max(18, Math.min(y - 9, canvas.height - 56));
          ctx.fillStyle = 'rgba(7, 17, 28, 0.86)';
          ctx.fillRect(labelX, labelY, labelWidth, 20);
          ctx.strokeStyle = level.side === 'support' ? 'rgba(23, 178, 106, 0.45)' : 'rgba(249, 112, 102, 0.45)';
          ctx.strokeRect(labelX, labelY, labelWidth, 20);
          ctx.fillStyle = color;
          ctx.fillText(label, labelX + 6, labelY + 14);
        }});
        ctx.restore();
      }}

      function drawCandlesticks(bars, pointX, pointY, slotWidth) {{
        const bodyWidth = Math.max(3, Math.min(14, slotWidth * 0.62));
        bars.forEach((bar, idx) => {{
          const open = Number(bar.o);
          const close = Number(bar.c);
          const high = Number(bar.h);
          const low = Number(bar.l);
          const x = pointX(idx);
          const highY = pointY(high);
          const lowY = pointY(low);
          const openY = pointY(open);
          const closeY = pointY(close);
          const rising = close >= open;
          const color = rising ? '#17b26a' : '#f04438';
          const bodyTop = Math.min(openY, closeY);
          const bodyHeight = Math.max(2, Math.abs(closeY - openY));

          ctx.strokeStyle = color;
          ctx.lineWidth = 1;
          ctx.beginPath();
          ctx.moveTo(x, highY);
          ctx.lineTo(x, lowY);
          ctx.stroke();

          ctx.fillStyle = rising ? 'rgba(23, 178, 106, 0.8)' : 'rgba(240, 68, 56, 0.82)';
          ctx.strokeStyle = color;
          ctx.fillRect(x - (bodyWidth / 2), bodyTop, bodyWidth, bodyHeight);
          ctx.strokeRect(x - (bodyWidth / 2), bodyTop, bodyWidth, bodyHeight);
        }});
      }}

      function drawTradeSignalMarkers(signalSeries, bars, pointX, pointY, priceBottom) {{
        if (!signalSeries.length) return;
        ctx.save();
        ctx.font = '10px ui-sans-serif, system-ui, sans-serif';
        ctx.textAlign = 'center';
        ctx.textBaseline = 'middle';
        signalSeries.forEach((signals, idx) => {{
          if (!signals?.length || !bars[idx]) return;
          const x = pointX(idx);
          let aboveCount = 0;
          let belowCount = 0;
          signals.forEach((signal) => {{
            const label = signal.label || '?';
            const width = Math.max(18, ctx.measureText(label).width + 10);
            const isBelow = signal.side === 'up';
            const rawY = isBelow
              ? pointY(Number(bars[idx].l)) + 12 + (belowCount++ * 17)
              : pointY(Number(bars[idx].h)) - 12 - (aboveCount++ * 17);
            const y = isBelow
              ? Math.min(priceBottom - 10, rawY)
              : Math.max(12, rawY);
            ctx.fillStyle = 'rgba(7, 17, 28, 0.88)';
            ctx.fillRect(x - (width / 2), y - 8, width, 16);
            ctx.strokeStyle = signal.color || '#d7f0ff';
            ctx.lineWidth = 1.5;
            ctx.strokeRect(x - (width / 2), y - 8, width, 16);
            ctx.fillStyle = signal.color || '#d7f0ff';
            ctx.fillText(label, x, y + 0.5);
          }});
        }});
        ctx.restore();
      }}

      function drawVolumeBars(bars, pointX, padLeft, padRight, plotBottom, slotWidth) {{
        const volumes = bars.map((bar) => Number(bar.v || 0));
        const maxVolume = Math.max(...volumes, 1);
        const volumeHeight = 62;
        const volumeTop = plotBottom - volumeHeight;
        const barWidth = Math.max(2, Math.min(12, slotWidth * 0.58));

        ctx.strokeStyle = 'rgba(159, 176, 192, 0.16)';
        ctx.beginPath();
        ctx.moveTo(padLeft, volumeTop);
        ctx.lineTo(canvas.width - padRight, volumeTop);
        ctx.stroke();

        bars.forEach((bar, idx) => {{
          const volume = Number(bar.v || 0);
          const height = Math.max(1, (volume / maxVolume) * volumeHeight);
          const rising = Number(bar.c) >= Number(bar.o);
          const x = pointX(idx);
          ctx.fillStyle = rising ? 'rgba(23, 178, 106, 0.22)' : 'rgba(240, 68, 56, 0.22)';
          ctx.fillRect(x - (barWidth / 2), plotBottom - height, barWidth, height);
        }});
      }}

      function drawRsiPanel(rsiValuesForViewport, pointX, padLeft, padRight, top, height) {{
        const bottom = top + height;
        const pointRsiY = (value) => top + ((100 - value) / 100) * height;

        ctx.fillStyle = 'rgba(7, 17, 28, 0.62)';
        ctx.fillRect(padLeft, top, canvas.width - padLeft - padRight, height);
        ctx.strokeStyle = 'rgba(159, 176, 192, 0.22)';
        ctx.strokeRect(padLeft, top, canvas.width - padLeft - padRight, height);

        ctx.font = '12px Georgia';
        [70, 50, 30].forEach((level) => {{
          const y = pointRsiY(level);
          ctx.strokeStyle = level === 50 ? 'rgba(159, 176, 192, 0.28)' : 'rgba(253, 176, 34, 0.32)';
          ctx.setLineDash(level === 50 ? [] : [5, 5]);
          ctx.beginPath();
          ctx.moveTo(padLeft, y);
          ctx.lineTo(canvas.width - padRight, y);
          ctx.stroke();
          ctx.setLineDash([]);
          ctx.fillStyle = '#9fb0c0';
          ctx.fillText(String(level), 16, y + 4);
        }});

        drawLineSeries(rsiValuesForViewport, pointX, pointRsiY, '#f97066', 2);

        ctx.fillStyle = '#f97066';
        ctx.fillRect(padLeft, top - 14, 18, 3);
        ctx.fillStyle = '#d7f0ff';
        ctx.fillText('RSI 14', padLeft + 24, top - 9);

        return {{ top, bottom, pointRsiY }};
      }}

      function drawMacdPanel(macdSeries, pointX, padLeft, padRight, top, height, slotWidth) {{
        const bottom = top + height;
        const values = macdSeries.flatMap((item) => item ? [item.macd, item.signal, item.hist] : []).filter((value) => value != null && !Number.isNaN(Number(value)));
        const max = Math.max(0, ...values);
        const min = Math.min(0, ...values);
        const range = Math.max(max - min, 0.0001);
        const pointMacdY = (value) => top + ((max - value) / range) * height;
        const zeroY = pointMacdY(0);
        const barWidth = Math.max(2, Math.min(12, slotWidth * 0.58));

        ctx.fillStyle = 'rgba(7, 17, 28, 0.62)';
        ctx.fillRect(padLeft, top, canvas.width - padLeft - padRight, height);
        ctx.strokeStyle = 'rgba(159, 176, 192, 0.22)';
        ctx.strokeRect(padLeft, top, canvas.width - padLeft - padRight, height);

        ctx.strokeStyle = 'rgba(159, 176, 192, 0.38)';
        ctx.beginPath();
        ctx.moveTo(padLeft, zeroY);
        ctx.lineTo(canvas.width - padRight, zeroY);
        ctx.stroke();

        macdSeries.forEach((item, idx) => {{
          if (!item || item.hist == null || Number.isNaN(Number(item.hist))) return;
          const hist = Number(item.hist);
          const x = pointX(idx);
          const y = pointMacdY(hist);
          ctx.fillStyle = hist >= 0 ? 'rgba(23, 178, 106, 0.58)' : 'rgba(240, 68, 56, 0.58)';
          ctx.fillRect(x - (barWidth / 2), Math.min(y, zeroY), barWidth, Math.max(1, Math.abs(zeroY - y)));
        }});

        drawLineSeries(macdSeries.map((item) => item ? item.macd : null), pointX, pointMacdY, '#7cc4ff', 2);
        drawLineSeries(macdSeries.map((item) => item ? item.signal : null), pointX, pointMacdY, '#fdb022', 2);

        ctx.font = '12px Georgia';
        ctx.fillStyle = '#7cc4ff';
        ctx.fillRect(padLeft, top - 14, 18, 3);
        ctx.fillStyle = '#d7f0ff';
        ctx.fillText('MACD', padLeft + 24, top - 9);
        ctx.fillStyle = '#fdb022';
        ctx.fillRect(padLeft + 78, top - 14, 18, 3);
        ctx.fillStyle = '#d7f0ff';
        ctx.fillText('Signal', padLeft + 102, top - 9);
        ctx.fillStyle = '#9fb0c0';
        ctx.fillText('0', 16, zeroY + 4);

        return {{ top, bottom, pointMacdY }};
      }}

      function drawChart(bars, startIndex = 0) {{
        ctx.clearRect(0, 0, canvas.width, canvas.height);
        if (!bars.length) {{
          clearCanvas();
          return;
        }}

        const chartType = getChartType();
        const closes = bars.map((b) => Number(b.c));
        const highs = bars.map((b) => Number(b.h));
        const lows = bars.map((b) => Number(b.l));
        const averageSource = ALL_BARS.length ? ALL_BARS : bars;
        const showRsi = showRsi14();
        const showMacdPanel = showMacd();
        const showSignals = showTradeSignals();
        const rsiSeries = showRsi
          ? rsiValues(averageSource, 14).slice(startIndex, startIndex + bars.length)
          : [];
        const macdSeries = showMacdPanel
          ? macdValues(averageSource).slice(startIndex, startIndex + bars.length)
          : [];
        const signalSeries = showSignals
          ? tradeSignalValues(averageSource).slice(startIndex, startIndex + bars.length)
          : [];
        const enabledMovingAverages = getEnabledMovingAverages().map((ma) => ({{
          ...ma,
          values: movingAverageValues(averageSource, ma.period).slice(startIndex, startIndex + bars.length),
        }}));
        const enabledEmas = getEnabledEmas().map((ema) => ({{
          ...ema,
          values: emaValues(averageSource, ema.period).slice(startIndex, startIndex + bars.length),
        }}));
        const bollingerSeries = showBollinger20()
          ? bollingerValues(averageSource, BOLLINGER_20.period).slice(startIndex, startIndex + bars.length)
          : [];
        const volumeAverageSeries = showVolumeBars()
          ? volumeAverageValues(averageSource, 20).slice(startIndex, startIndex + bars.length)
          : [];
        const srLevels = showSupportResistance() ? supportResistanceLevels() : [];
        const overlays = enabledMovingAverages.concat(enabledEmas);
        const overlayValues = overlays.flatMap((overlay) => overlay.values.filter((value) => value != null));
        const bollingerRangeValues = bollingerSeries.flatMap((band) => band ? [band.upper, band.lower] : []);
        const max = Math.max(...highs, ...overlayValues, ...bollingerRangeValues);
        const min = Math.min(...lows, ...overlayValues, ...bollingerRangeValues);
        const padLeft = 52;
        const padRight = 22;
        const padTop = 24;
        const padBottom = 38;
        const lowerPanelCount = (showRsi ? 1 : 0) + (showMacdPanel ? 1 : 0);
        const lowerPanelGap = lowerPanelCount ? 30 : 0;
        const rsiHeight = showRsi ? Math.max(96, Math.min(150, Math.round(canvas.height * 0.22))) : 0;
        const macdHeight = showMacdPanel ? Math.max(110, Math.min(160, Math.round(canvas.height * 0.24))) : 0;
        const lowerPanelHeight = rsiHeight + macdHeight + Math.max(0, lowerPanelCount - 1) * lowerPanelGap;
        const priceBottom = canvas.height - padBottom - lowerPanelHeight - (lowerPanelCount ? lowerPanelGap : 0);
        const w = canvas.width - padLeft - padRight;
        const h = Math.max(120, priceBottom - padTop);
        const range = Math.max(max - min, 0.0001);
        const pointX = (idx) => padLeft + ((bars.length === 1 ? 0.5 : idx / (bars.length - 1)) * w);
        const pointY = (value) => padTop + ((max - value) / range) * h;

        LAST_DRAW_STATE = {{ bars, rsiSeries, macdSeries, signalSeries, bollingerSeries, srLevels, padLeft, padRight, padTop, padBottom, w, h, max, min, range, pointX, pointY }};
        if (HOVER_CANVAS_X != null) {{
          const relative = (HOVER_CANVAS_X - padLeft) / Math.max(w, 1);
          const idx = Math.round(relative * Math.max(bars.length - 1, 0));
          HOVER_INDEX = Math.max(0, Math.min(bars.length - 1, idx));
        }}
        clampHoverIndex(bars);

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

        if (bollingerSeries.length) {{
          drawBollingerChannel(bollingerSeries, pointX, pointY);
        }}

        if (srLevels.length) {{
          drawSupportResistanceLevels(srLevels, pointY, padLeft, padRight, min, max);
        }}

        if (showVolumeBars()) {{
          drawVolumeBars(bars, pointX, padLeft, padRight, priceBottom, bars.length > 1 ? w / (bars.length - 1) : w);
        }}

        if (chartType === 'candles') {{
          drawCandlesticks(bars, pointX, pointY, bars.length > 1 ? w / (bars.length - 1) : w);
        }} else {{
          const gradient = ctx.createLinearGradient(0, padTop, 0, canvas.height - padBottom);
          gradient.addColorStop(0, '#7cc4ff');
          gradient.addColorStop(1, '#2e90fa');
          drawLineSeries(closes, pointX, pointY, gradient, 3);
        }}

        overlays.forEach((overlay) => drawLineSeries(overlay.values, pointX, pointY, overlay.color, 2));

        if (showSignals) {{
          drawTradeSignalMarkers(signalSeries, bars, pointX, pointY, priceBottom);
        }}

        let lowerPanelTop = priceBottom + (lowerPanelCount ? lowerPanelGap : 0);
        if (showRsi) {{
          const rsiPanel = drawRsiPanel(rsiSeries, pointX, padLeft, padRight, lowerPanelTop, rsiHeight);
          LAST_DRAW_STATE.rsiPanel = rsiPanel;
          lowerPanelTop = rsiPanel.bottom + lowerPanelGap;
        }}
        if (showMacdPanel) {{
          const macdPanel = drawMacdPanel(macdSeries, pointX, padLeft, padRight, lowerPanelTop, macdHeight, bars.length > 1 ? w / (bars.length - 1) : w);
          LAST_DRAW_STATE.macdPanel = macdPanel;
        }}

        if (overlays.length || bollingerSeries.length) {{
          ctx.font = '12px Georgia';
          let legendX = padLeft;
          if (bollingerSeries.length) {{
            const label = 'Bollinger 20';
            ctx.fillStyle = BOLLINGER_20.color;
            ctx.fillRect(legendX, padTop - 14, 18, 3);
            ctx.fillStyle = '#d7f0ff';
            ctx.fillText(label, legendX + 24, padTop - 9);
            legendX += ctx.measureText(label).width + 58;
          }}
          overlays.forEach((overlay) => {{
            const label = `${{overlay.id.startsWith('ema') ? 'EMA' : 'MA'}} ${{overlay.period}}`;
            ctx.fillStyle = overlay.color;
            ctx.fillRect(legendX, padTop - 14, 18, 3);
            ctx.fillStyle = '#d7f0ff';
            ctx.fillText(label, legendX + 24, padTop - 9);
            legendX += ctx.measureText(label).width + 58;
          }});
        }}

        ctx.fillStyle = '#d7f0ff';
        ctx.font = '12px Georgia';
        const labelCount = Math.min(6, bars.length);
        for (let tick = 0; tick < labelCount; tick++) {{
          const idx = Math.min(
            bars.length - 1,
            Math.round((bars.length - 1) * (labelCount === 1 ? 0 : tick / (labelCount - 1)))
          );
          const x = pointX(idx);
          const label = formatAxisLabel(bars[idx].t);
          const labelWidth = ctx.measureText(label).width;
          ctx.fillText(
            label,
            Math.max(padLeft, Math.min(x - (labelWidth / 2), canvas.width - padRight - labelWidth)),
            canvas.height - 12
          );
        }}

        if (HOVER_INDEX != null && bars[HOVER_INDEX]) {{
          const hovered = bars[HOVER_INDEX];
          const hoverX = pointX(HOVER_INDEX);
          const hoverY = pointY(Number(hovered.c));
          const absoluteHoverIndex = startIndex + HOVER_INDEX;
          const prevBar = absoluteHoverIndex > 0 ? ALL_BARS[absoluteHoverIndex - 1] : null;
          const closeChangePct = prevBar && Number(prevBar.c)
            ? ((Number(hovered.c) - Number(prevBar.c)) / Number(prevBar.c)) * 100
            : null;
          const rsiValue = rsiSeries[HOVER_INDEX];
          const macdValue = macdSeries[HOVER_INDEX];
          const signalValues = signalSeries[HOVER_INDEX] || [];
          const bollingerValue = bollingerSeries[HOVER_INDEX];
          const volumeAverageValue = volumeAverageSeries[HOVER_INDEX];
          const hoverLineBottom = LAST_DRAW_STATE.macdPanel?.bottom || LAST_DRAW_STATE.rsiPanel?.bottom || priceBottom;

          ctx.strokeStyle = 'rgba(215, 240, 255, 0.55)';
          ctx.setLineDash([4, 4]);
          ctx.beginPath();
          ctx.moveTo(hoverX, padTop);
          ctx.lineTo(hoverX, hoverLineBottom);
          ctx.moveTo(padLeft, hoverY);
          ctx.lineTo(canvas.width - padRight, hoverY);
          ctx.stroke();
          ctx.setLineDash([]);

          if (showRsi && LAST_DRAW_STATE.rsiPanel && rsiValue != null) {{
            const rsiY = LAST_DRAW_STATE.rsiPanel.pointRsiY(Number(rsiValue));
            ctx.fillStyle = '#f97066';
            ctx.beginPath();
            ctx.arc(hoverX, rsiY, 4, 0, Math.PI * 2);
            ctx.fill();
          }}

          if (showMacdPanel && LAST_DRAW_STATE.macdPanel && macdValue?.macd != null) {{
            const macdY = LAST_DRAW_STATE.macdPanel.pointMacdY(Number(macdValue.macd));
            ctx.fillStyle = '#7cc4ff';
            ctx.beginPath();
            ctx.arc(hoverX, macdY, 4, 0, Math.PI * 2);
            ctx.fill();
          }}

          ctx.fillStyle = '#d7f0ff';
          ctx.beginPath();
          ctx.arc(hoverX, hoverY, 4, 0, Math.PI * 2);
          ctx.fill();

          const tooltipLines = [
            {{ text: `${{fmtDate(hovered.t)}}`, color: '#ffffff' }},
            {{ text: `O $${{fmtPrice(hovered.o)}}  H $${{fmtPrice(hovered.h)}}  L $${{fmtPrice(hovered.l)}}  C $${{fmtPrice(hovered.c)}}`, color: '#d7f0ff' }},
            {{ text: `Vol ${{fmtVolume(hovered.v)}}  Close chg ${{fmtPct(closeChangePct)}}`, color: closeChangePct == null ? '#9fb0c0' : (closeChangePct >= 0 ? '#17b26a' : '#f97066') }},
          ];
          overlays.forEach((overlay) => {{
            const value = overlay.values[HOVER_INDEX];
            const label = `${{overlay.id.startsWith('ema') ? 'EMA' : 'MA'}}${{overlay.period}}`;
            tooltipLines.push({{ text: `${{label}} $${{fmtPrice(value)}}`, color: overlay.color }});
          }});
          if (showVolumeBars()) {{
            tooltipLines.push({{ text: `Vol avg20 ${{fmtVolume(volumeAverageValue)}}`, color: '#9fb0c0' }});
          }}
          if (showBollinger20()) {{
            tooltipLines.push({{
              text: bollingerValue
                ? `BB20 U $${{fmtPrice(bollingerValue.upper)}}  M $${{fmtPrice(bollingerValue.middle)}}  L $${{fmtPrice(bollingerValue.lower)}} (${{bollingerStatus(hovered.c, bollingerValue)}})`
                : `BB20 ${{bollingerStatus(hovered.c, bollingerValue)}}`,
              color: BOLLINGER_20.color,
            }});
          }}
          if (showRsi && rsiValue != null) {{
            tooltipLines.push({{ text: `RSI14 ${{fmtPrice(rsiValue)}}`, color: '#f97066' }});
          }}
          if (showMacdPanel && macdValue?.macd != null) {{
            tooltipLines.push({{ text: `MACD ${{fmtPrice(macdValue.macd)}}  Sig ${{fmtPrice(macdValue.signal)}}  Hist ${{fmtPrice(macdValue.hist)}}`, color: '#7cc4ff' }});
          }}
          if (showSignals && signalValues.length) {{
            signalValues.forEach((signal) => {{
              tooltipLines.push({{ text: `Signal ${{signal.label}}: ${{signal.detail}}`, color: signal.color || '#d7f0ff' }});
            }});
          }}
          const nearestSr = showSupportResistance() ? nearestSupportResistance(srLevels, hovered.c) : {{ support: null, resistance: null }};
          if (showSupportResistance()) {{
            tooltipLines.push({{ text: `S/R ${{formatSrDistance('S', nearestSr.support, hovered.c)}}`, color: '#17b26a' }});
            tooltipLines.push({{ text: `S/R ${{formatSrDistance('R', nearestSr.resistance, hovered.c)}}`, color: '#f97066' }});
          }}
          drawHoverInfoBox(tooltipLines, padLeft, padTop, padRight);
        }}
      }}

      function renderViewport() {{
        const viewportStart = getVisibleStart();
        const visibleBars = getVisibleBars();
        drawChart(visibleBars, viewportStart);
        document.getElementById('empty-state').style.display = visibleBars.length ? 'none' : 'block';

        const lastBar = visibleBars.length ? visibleBars[visibleBars.length - 1] : null;
        const firstBar = visibleBars.length ? visibleBars[0] : null;
        const hoveredBar = (HOVER_INDEX != null && visibleBars[HOVER_INDEX]) ? visibleBars[HOVER_INDEX] : null;
        const visibleRsiValues = showRsi14()
          ? rsiValues(ALL_BARS, 14).slice(viewportStart, viewportStart + visibleBars.length)
          : [];
        const visibleMacdValues = showMacd()
          ? macdValues(ALL_BARS).slice(viewportStart, viewportStart + visibleBars.length)
          : [];
        const visibleSignalValues = showTradeSignals()
          ? tradeSignalValues(ALL_BARS).slice(viewportStart, viewportStart + visibleBars.length)
          : [];
        const visibleBollingerValues = showBollinger20()
          ? bollingerValues(ALL_BARS, BOLLINGER_20.period).slice(viewportStart, viewportStart + visibleBars.length)
          : [];
        const srLevels = showSupportResistance() ? supportResistanceLevels() : [];
        const latestRsi = visibleRsiValues.length ? visibleRsiValues[visibleRsiValues.length - 1] : null;
        const hoveredRsi = hoveredBar && HOVER_INDEX != null ? visibleRsiValues[HOVER_INDEX] : null;
        const latestMacd = visibleMacdValues.length ? visibleMacdValues[visibleMacdValues.length - 1] : null;
        const hoveredMacd = hoveredBar && HOVER_INDEX != null ? visibleMacdValues[HOVER_INDEX] : null;
        const hoveredSignals = hoveredBar && HOVER_INDEX != null ? (visibleSignalValues[HOVER_INDEX] || []) : [];
        const hoveredSignalText = signalSummary(hoveredSignals);
        const latestBollinger = visibleBollingerValues.length ? visibleBollingerValues[visibleBollingerValues.length - 1] : null;
        const hoveredBollinger = hoveredBar && HOVER_INDEX != null ? visibleBollingerValues[HOVER_INDEX] : null;
        const chartLabel = getChartType() === 'candles' ? 'candlestick' : 'line';
        const indicatorLabels = getEnabledMovingAverages()
          .map((ma) => `MA${{ma.period}}`)
          .concat(getEnabledEmas().map((ema) => `EMA${{ema.period}}`));
        if (showBollinger20()) indicatorLabels.push('BB20');
        if (showVolumeBars()) indicatorLabels.push('volume');
        if (showRsi14()) indicatorLabels.push('RSI14');
        if (showMacd()) indicatorLabels.push('MACD');
        if (showSupportResistance()) indicatorLabels.push('support/resistance');
        if (showTradeSignals()) indicatorLabels.push('signals');
        const indicatorLabel = indicatorLabels.join(', ') || 'no overlays';
        const rsiSummary = showRsi14() && latestRsi != null ? ` Latest RSI14: ${{fmtPrice(latestRsi)}}.` : '';
        const macdSummary = showMacd() && latestMacd?.macd != null
          ? ` Latest MACD: ${{fmtPrice(latestMacd.macd)}} signal ${{fmtPrice(latestMacd.signal)}} histogram ${{fmtPrice(latestMacd.hist)}}.`
          : '';
        const bollingerSummary = showBollinger20()
          ? ` Latest BB20: ${{bollingerStatus(lastBar?.c, latestBollinger)}}${{latestBollinger ? `; upper ${{fmtPrice(latestBollinger.upper)}}, middle ${{fmtPrice(latestBollinger.middle)}}, lower ${{fmtPrice(latestBollinger.lower)}}` : ''}}.`
          : '';
        const latestSr = showSupportResistance() ? nearestSupportResistance(srLevels, lastBar?.c) : {{ support: null, resistance: null }};
        const hoveredSr = showSupportResistance() ? nearestSupportResistance(srLevels, hoveredBar?.c) : {{ support: null, resistance: null }};
        const srSummary = showSupportResistance() && lastBar
          ? ` Nearest S/R: ${{formatSrDistance('support', latestSr.support, lastBar.c)}}; ${{formatSrDistance('resistance', latestSr.resistance, lastBar.c)}}.`
          : '';
        document.getElementById('chart-summary').textContent = visibleBars.length
          ? `Showing ${{visibleBars.length}} of ${{ALL_BARS.length}} loaded ${{CURRENT_TIMEFRAME}} bars for ${{SYMBOL}} as a ${{chartLabel}} chart with ${{indicatorLabel}}.${{rsiSummary}}${{macdSummary}}${{bollingerSummary}}${{srSummary}}`
          : `No ${{CURRENT_TIMEFRAME}} bars stored for ${{SYMBOL}}.`;
        document.getElementById('range-summary').textContent = visibleBars.length
          ? `Viewport: ${{formatAxisLabel(firstBar.t)}} to ${{formatAxisLabel(lastBar.t)}}`
          : 'No viewport available yet.';
        document.getElementById('hover-summary').textContent = hoveredBar
          ? `Hovered bar: ${{fmtDate(hoveredBar.t)}} | Close $${{fmtPrice(hoveredBar.c)}} | High $${{fmtPrice(hoveredBar.h)}} | Low $${{fmtPrice(hoveredBar.l)}}${{hoveredRsi != null ? ` | RSI14 ${{fmtPrice(hoveredRsi)}}` : ''}}${{showMacd() && hoveredMacd?.macd != null ? ` | MACD ${{fmtPrice(hoveredMacd.macd)}} Sig ${{fmtPrice(hoveredMacd.signal)}} Hist ${{fmtPrice(hoveredMacd.hist)}}` : ''}}${{showTradeSignals() && hoveredSignalText ? ` | Signals ${{hoveredSignalText}}` : ''}}${{showBollinger20() ? ` | BB20 ${{bollingerStatus(hoveredBar.c, hoveredBollinger)}}${{hoveredBollinger ? ` U ${{fmtPrice(hoveredBollinger.upper)}} M ${{fmtPrice(hoveredBollinger.middle)}} L ${{fmtPrice(hoveredBollinger.lower)}}` : ''}}` : ''}}${{showSupportResistance() ? ` | S/R ${{formatSrDistance('support', hoveredSr.support, hoveredBar.c)}}; ${{formatSrDistance('resistance', hoveredSr.resistance, hoveredBar.c)}}` : ''}}`
          : 'Hover over the chart to inspect a bar.';
        document.getElementById('last-close').textContent = lastBar ? fmtPrice(lastBar.c) : '-';
        document.getElementById('bar-count').textContent = String(ALL_BARS.length);
        document.getElementById('chart-last-bar-time').textContent = lastBar ? fmtDate(lastBar.t) : '-';
      }}

      function applyPayload(payload, options = {{}}) {{
        CURRENT_TIMEFRAME = payload.timeframe || CURRENT_TIMEFRAME;
        const incomingBars = payload.bars || [];
        if (options.prepend && incomingBars.length) {{
          const seen = new Set(ALL_BARS.map((bar) => bar.t));
          const olderBars = incomingBars.filter((bar) => !seen.has(bar.t));
          ALL_BARS = olderBars.concat(ALL_BARS);
          VIEW_END += olderBars.length;
        }} else {{
          ALL_BARS = incomingBars;
          VIEW_END = ALL_BARS.length;
          WINDOW_SIZE = defaultWindowSize(CURRENT_TIMEFRAME, ALL_BARS.length);
        }}
        HAS_OLDER_BARS = !!payload.has_older_bars;
        renderViewport();

        document.getElementById('latest-quote').textContent = payload.latest_quote
          ? `${{fmtPrice(payload.latest_quote.bid_price)}} / ${{fmtPrice(payload.latest_quote.ask_price)}}`
          : '-';

        const sync = payload.sync_state || {{}};
        document.getElementById('last-synced').textContent = fmtDate(sync.last_synced_at);
        document.getElementById('last-successful-sync').textContent = fmtDate(sync.last_successful_sync_at);
        document.getElementById('last-bar-sync').textContent = fmtDate(sync.last_bar_synced_at);
        document.getElementById('last-quote-sync').textContent = fmtDate(sync.last_quote_synced_at);
        document.getElementById('sync-state').textContent = sync.sync_status || 'idle';
        document.getElementById('sync-error').textContent = sync.sync_error || '-';
      }}

      async function loadOlderBarsIfNeeded() {{
        if (FETCHING_OLDER_BARS || !ALL_BARS.length || !HAS_OLDER_BARS) return false;
        FETCHING_OLDER_BARS = true;
        try {{
          const params = new URLSearchParams({{
            timeframe: CURRENT_TIMEFRAME,
            limit: desiredFetchLimit(CURRENT_TIMEFRAME),
            before: ALL_BARS[0].t,
          }});
          const res = await fetch(`/api/chart/${{encodeURIComponent(SYMBOL)}}?${{params.toString()}}`);
          const payload = await res.json();
          if (!res.ok || !payload.ok) {{
            throw new Error(payload.error || 'Failed to load older chart data.');
          }}
          if (!(payload.bars || []).length) {{
            HAS_OLDER_BARS = false;
            return false;
          }}
          applyPayload(payload, {{ prepend: true }});
          setStatus(`Loaded ${{payload.bars.length}} older ${{CURRENT_TIMEFRAME}} bars from PostgreSQL.`);
          return true;
        }} finally {{
          FETCHING_OLDER_BARS = false;
        }}
      }}

      function zoomViewport(direction) {{
        if (!ALL_BARS.length) return;
        const step = Math.max(10, Math.round(WINDOW_SIZE * 0.2));
        WINDOW_SIZE = direction > 0
          ? Math.min(ALL_BARS.length, WINDOW_SIZE + step)
          : Math.max(20, WINDOW_SIZE - step);
        clampViewport();
        renderViewport();
      }}

      async function panViewport(direction) {{
        if (!ALL_BARS.length) return;
        const step = Math.max(5, Math.round(WINDOW_SIZE * 0.25));
        if (direction < 0 && VIEW_END - WINDOW_SIZE <= step && HAS_OLDER_BARS) {{
          try {{
            await loadOlderBarsIfNeeded();
          }} catch (err) {{
            setStatus(err.message || String(err), true);
          }}
        }}
        VIEW_END = Math.max(WINDOW_SIZE, Math.min(ALL_BARS.length, VIEW_END + (direction * step)));
        renderViewport();
      }}

      function resetViewport() {{
        if (!ALL_BARS.length) return;
        WINDOW_SIZE = defaultWindowSize(CURRENT_TIMEFRAME, ALL_BARS.length);
        VIEW_END = ALL_BARS.length;
        renderViewport();
      }}

      async function loadChart(options = {{}}) {{
        const timeframe = document.getElementById('timeframe').value;
        const params = new URLSearchParams({{
          timeframe,
          limit: desiredFetchLimit(timeframe),
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
          const limit = Number(desiredFetchLimit(timeframe));
          const res = await fetch(`/api/chart/${{encodeURIComponent(SYMBOL)}}/sync`, {{
            method: 'POST',
            headers: {{ 'Content-Type': 'application/json' }},
            body: JSON.stringify({{ timeframe, force_full: timeframe === '1Day', limit }}),
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
        resizeCanvasToDisplay();
        window.addEventListener('resize', () => {{
          if (resizeCanvasToDisplay()) renderViewport();
        }});
        document.getElementById('timeframe').addEventListener('change', () => loadChart({{ autoSync: true }}).catch((err) => setStatus(err.message, true)));
        document.getElementById('chart-type').addEventListener('change', renderViewport);
        MOVING_AVERAGES.forEach((ma) => {{
          document.getElementById(ma.id).addEventListener('change', renderViewport);
        }});
        EMA_OVERLAYS.forEach((ema) => {{
          document.getElementById(ema.id).addEventListener('change', renderViewport);
        }});
        document.getElementById(BOLLINGER_20.id).addEventListener('change', renderViewport);
        document.getElementById('volume-bars').addEventListener('change', renderViewport);
        document.getElementById('rsi-14').addEventListener('change', renderViewport);
        document.getElementById('macd').addEventListener('change', renderViewport);
        document.getElementById('support-resistance').addEventListener('change', renderViewport);
        document.getElementById('trade-signals').addEventListener('change', renderViewport);
        document.getElementById('reload-btn').addEventListener('click', () => loadChart({{ autoSync: false, showReloadMessage: true }}).catch((err) => setStatus(err.message, true)));
        document.getElementById('sync-btn').addEventListener('click', syncNow);
        document.getElementById('pan-left-btn').addEventListener('click', () => panViewport(-1));
        document.getElementById('pan-right-btn').addEventListener('click', () => panViewport(1));
        document.getElementById('zoom-in-btn').addEventListener('click', () => zoomViewport(-1));
        document.getElementById('zoom-out-btn').addEventListener('click', () => zoomViewport(1));
        document.getElementById('reset-view-btn').addEventListener('click', resetViewport);
        canvas.addEventListener('wheel', (event) => {{
          event.preventDefault();
          zoomViewport(event.deltaY > 0 ? 1 : -1);
        }}, {{ passive: false }});
        canvas.addEventListener('mousedown', (event) => {{
          DRAG_STATE = {{ startX: event.clientX, startViewEnd: VIEW_END }};
        }});
        canvas.addEventListener('mouseleave', () => {{
          HOVER_INDEX = null;
          HOVER_CANVAS_X = null;
          renderViewport();
        }});
        window.addEventListener('mouseup', () => {{
          DRAG_STATE = null;
        }});
        canvas.addEventListener('mousemove', (event) => {{
          if (DRAG_STATE || !LAST_DRAW_STATE || !LAST_DRAW_STATE.bars.length) return;
          const rect = canvas.getBoundingClientRect();
          const x = (event.clientX - rect.left) * (canvas.width / Math.max(rect.width, 1));
          HOVER_CANVAS_X = x;
          const relative = (x - LAST_DRAW_STATE.padLeft) / Math.max(LAST_DRAW_STATE.w, 1);
          const idx = Math.round(relative * Math.max(LAST_DRAW_STATE.bars.length - 1, 0));
          HOVER_INDEX = Math.max(0, Math.min(LAST_DRAW_STATE.bars.length - 1, idx));
          renderViewport();
        }});
        window.addEventListener('mousemove', (event) => {{
          if (!DRAG_STATE || !ALL_BARS.length) return;
          const deltaX = event.clientX - DRAG_STATE.startX;
          const barsPerPixel = WINDOW_SIZE / Math.max(canvas.clientWidth, 1);
          const deltaBars = Math.round(deltaX * barsPerPixel);
          VIEW_END = Math.max(WINDOW_SIZE, Math.min(ALL_BARS.length, DRAG_STATE.startViewEnd - deltaBars));
          renderViewport();
        }});
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
        before_dt = parse_asof(request.args.get("before"))
    except ValueError:
        return jsonify({"ok": False, "error": "Invalid 'before'; expected YYYY-MM-DD or ISO 8601 timestamp."}), 400

    try:
        payload = _get_market_snapshot(symbol, timeframe, limit, before=before_dt)
        auto_synced = False
        if auto_sync and before_dt is None and not payload["bars"]:
            payload = _sync_market_data(symbol, timeframe, force_full=True, snapshot_limit=limit)
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
    force_full = bool(payload.get("force_full", timeframe == "1Day"))
    limit_raw = payload.get("limit", 5000)
    if timeframe not in SUPPORTED_TIMEFRAMES:
        return jsonify({"ok": False, "error": f"Unsupported timeframe '{timeframe}'."}), 400
    try:
        limit = int(limit_raw)
    except (TypeError, ValueError):
        return jsonify({"ok": False, "error": "'limit' must be an integer."}), 400
    try:
        snapshot = _sync_market_data(symbol, timeframe, force_full=force_full, snapshot_limit=limit)
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
        auto_synced = False
        if _market_snapshot_needs_sync(snapshot, CHART_LATEST_AUTO_SYNC_MAX_AGE_DAYS):
            snapshot = _sync_market_data(symbol, timeframe, force_full=False, snapshot_limit=1)
            auto_synced = True
        return jsonify(
            {
                "ok": True,
                "symbol": snapshot["symbol"],
                "timeframe": snapshot["timeframe"],
                "latest_quote": snapshot["latest_quote"],
                "latest_bar": snapshot["latest_bar"],
                "sync_state": snapshot["sync_state"],
                "auto_synced": auto_synced,
                "auto_sync_max_age_days": CHART_LATEST_AUTO_SYNC_MAX_AGE_DAYS,
            }
        )
    except ValueError as exc:
        return jsonify({"ok": False, "error": str(exc)}), 400
    except Exception as exc:
        return jsonify({"ok": False, "error": str(exc)}), 500


@app.get("/api/state/<symbol>")
@app.get("/api/state/<symbol>/<timeframe>")
def get_analytics_state(symbol: str, timeframe: str | None = None) -> Response:
    normalized_timeframe = None
    settings = None
    asof_dt = None
    hide_ts = _parse_bool_query_param("hidets", default=False)
    try:
        normalized_timeframe = _normalize_analytics_timeframe(timeframe or request.args.get("timeframe"))
        settings = _analytics_settings_from_request()
        asof_dt = parse_asof(request.args.get("asof"))
        if asof_dt is None:
            _sync_current_analytics_data_if_needed(symbol, normalized_timeframe)
        payload = _read_analytics_state(symbol, normalized_timeframe, asof_dt, settings)
        payload["ok"] = True
        if hide_ts:
            payload = _strip_symbol_and_timestamps(payload)
        return jsonify(payload)
    except AnalyticsNotFoundError as exc:
        try:
            _backfill_analytics_symbol_if_needed(symbol)
            payload = _read_analytics_state(symbol, normalized_timeframe or "1Day", asof_dt, settings or AnalyticsSettings())
            payload["ok"] = True
            if hide_ts:
                payload = _strip_symbol_and_timestamps(payload)
            return jsonify(payload)
        except CacheCapacityError as capacity_exc:
            return jsonify({"ok": False, "error": str(capacity_exc)}), 503, {"Retry-After": "1"}
        except Exception:
            pass
        return jsonify({"ok": False, "error": str(exc)}), 404
    except ValueError as exc:
        return jsonify({"ok": False, "error": str(exc)}), 400
    except CacheCapacityError as exc:
        return jsonify({"ok": False, "error": str(exc)}), 503, {"Retry-After": "1"}
    except Exception as exc:
        return jsonify({"ok": False, "error": str(exc)}), 500


@app.get("/api/events/<symbol>")
@app.get("/api/events/<symbol>/<timeframe>")
def get_analytics_events(symbol: str, timeframe: str | None = None) -> Response:
    try:
        hide_ts = _parse_bool_query_param("hidets", default=False)
        normalized_timeframe = _normalize_analytics_timeframe(timeframe or request.args.get("timeframe"))
        settings = _analytics_settings_from_request()
        asof_dt = parse_asof(request.args.get("asof"))
        event_limit_raw = request.args.get("event_limit")
        event_limit = int(event_limit_raw) if event_limit_raw else settings.event_limit
        if asof_dt is None:
            _sync_current_analytics_data_if_needed(symbol, normalized_timeframe)
        payload = analytics_service.get_events(symbol, normalized_timeframe, asof_dt, settings, event_limit=event_limit)
        payload["ok"] = True
        if hide_ts:
            payload = _strip_symbol_and_timestamps(payload)
        return jsonify(payload)
    except AnalyticsNotFoundError as exc:
        try:
            _backfill_analytics_symbol_if_needed(symbol)
            payload = analytics_service.get_events(symbol, normalized_timeframe, asof_dt, settings, event_limit=event_limit)
            payload["ok"] = True
            if hide_ts:
                payload = _strip_symbol_and_timestamps(payload)
            return jsonify(payload)
        except CacheCapacityError as capacity_exc:
            return jsonify({"ok": False, "error": str(capacity_exc)}), 503, {"Retry-After": "1"}
        except Exception:
            pass
        return jsonify({"ok": False, "error": str(exc)}), 404
    except ValueError as exc:
        return jsonify({"ok": False, "error": str(exc)}), 400
    except CacheCapacityError as exc:
        return jsonify({"ok": False, "error": str(exc)}), 503, {"Retry-After": "1"}
    except Exception as exc:
        return jsonify({"ok": False, "error": str(exc)}), 500


@app.get("/api/metadata/<symbol>")
def get_symbol_metadata_api(symbol: str) -> Response:
    try:
        hide_ts = _parse_bool_query_param("hidets", default=False)
        payload = analytics_service.get_symbol_metadata(symbol)
        payload["ok"] = True
        if hide_ts:
            payload = _strip_symbol_and_timestamps(payload)
        return jsonify(payload)
    except AnalyticsNotFoundError as exc:
        try:
            _backfill_analytics_symbol_if_needed(symbol)
            payload = analytics_service.get_symbol_metadata(symbol)
            payload["ok"] = True
            if hide_ts:
                payload = _strip_symbol_and_timestamps(payload)
            return jsonify(payload)
        except Exception:
            pass
        return jsonify({"ok": False, "error": str(exc)}), 404
    except Exception as exc:
        return jsonify({"ok": False, "error": str(exc)}), 500


@app.get("/api/futurestate/<symbol>")
@app.get("/api/futurestate/<symbol>/<timeframe>")
def get_analytics_future_state(symbol: str, timeframe: str | None = None) -> Response:
    hide_ts = _parse_bool_query_param("hidets", default=False)
    days_raw = (request.args.get("days") or request.args.get("horizon_days") or "").strip()
    if not days_raw:
        return jsonify({"ok": False, "error": "'days' is required."}), 400

    stop_loss_pct = None
    take_profit_pct = None
    side = (request.args.get("side") or "long").strip().lower() or "long"
    stop_loss_raw = (request.args.get("stoploss") or request.args.get("stop_loss") or "").strip()
    take_profit_raw = (request.args.get("takeprofit") or request.args.get("take_profit") or "").strip()

    try:
        days = int(days_raw)
    except ValueError:
        return jsonify({"ok": False, "error": "'days' must be an integer."}), 400

    if stop_loss_raw:
        try:
            stop_loss_pct = float(stop_loss_raw)
        except ValueError:
            return jsonify({"ok": False, "error": "'stoploss' must be a number."}), 400
    if take_profit_raw:
        try:
            take_profit_pct = float(take_profit_raw)
        except ValueError:
            return jsonify({"ok": False, "error": "'takeprofit' must be a number."}), 400
    if side not in {"long", "short"}:
        return jsonify({"ok": False, "error": "'side' must be either 'long' or 'short'."}), 400

    try:
        normalized_timeframe = _normalize_analytics_timeframe(timeframe or request.args.get("timeframe"))
        asof_dt = parse_asof(request.args.get("asof"))
        if asof_dt is None:
            return jsonify({"ok": False, "error": "'asof' is required."}), 400
        _validate_future_preparation(days, stop_loss_pct, take_profit_pct)
        if normalized_timeframe != "1Day":
            start, end = _future_preparation_window(asof_dt, days)
            _prepare_window_coverage(symbol, normalized_timeframe, start, end,
                                     allow_sync=end - start <= timedelta(days=31))
        payload = analytics_service.get_future_state(
            symbol,
            normalized_timeframe,
            asof_dt,
            days,
            stop_loss_pct=stop_loss_pct,
            take_profit_pct=take_profit_pct,
            side=side,
        )
        payload["ok"] = True
        if hide_ts:
            payload = _strip_symbol_and_timestamps(payload)
        return jsonify(payload)
    except AnalyticsNotFoundError as exc:
        try:
            if normalized_timeframe != "1Day":
                return jsonify({"ok": False, "error": str(exc)}), 404
            _backfill_analytics_symbol_if_needed(symbol)
            payload = analytics_service.get_future_state(
                symbol,
                normalized_timeframe,
                asof_dt,
                days,
                stop_loss_pct=stop_loss_pct,
                take_profit_pct=take_profit_pct,
                side=side,
            )
            payload["ok"] = True
            if hide_ts:
                payload = _strip_symbol_and_timestamps(payload)
            return jsonify(payload)
        except CacheCapacityError as capacity_exc:
            return jsonify({"ok": False, "error": str(capacity_exc)}), 503, {"Retry-After": "1"}
        except Exception:
            pass
        return jsonify({"ok": False, "error": str(exc)}), 404
    except ValueError as exc:
        return jsonify({"ok": False, "error": str(exc)}), 400
    except CacheCapacityError as exc:
        return jsonify({"ok": False, "error": str(exc)}), 503, {"Retry-After": "1"}
    except Exception as exc:
        return jsonify({"ok": False, "error": str(exc)}), 500


def _validate_future_preparation(days, stop_loss_pct, take_profit_pct):
    if days <= 0:
        raise ValueError("'days' must be greater than zero.")
    if any(value is not None and value < 0 for value in (stop_loss_pct, take_profit_pct)):
        raise ValueError("Stop-loss and take-profit percentages must be nonnegative.")


def _future_preparation_window(asof_dt, days):
    # Preserve the existing calendar search envelope, not a promise of N trading
    # sessions. The service reports partial horizons independently of coverage.
    start = asof_dt - timedelta(days=7)
    end = min(asof_dt + timedelta(days=days),
              _utcnow() - timedelta(minutes=max(CHART_DELAY_MINUTES, 0)))
    if start >= end:
        raise ValueError("Target sync window is in the future after delay adjustment.")
    return start, end


def _prepare_window_coverage(symbol: str, timeframe: str, start: datetime, end: datetime,
                             allow_sync: bool = True) -> Dict:
    """Fetch only unverified gaps; completed empty windows count as coverage."""
    symbol = symbol.strip().upper()
    def prepare():
        _, _, windows = _historical_price_coverage(symbol, timeframe, start, end)
        cursor = start
        gaps = []
        for covered_start, covered_end in windows:
            if covered_end < cursor:
                continue
            if covered_start > cursor:
                gaps.append((cursor, min(covered_start, end)))
            cursor = max(cursor, covered_end)
            if cursor >= end:
                break
        if cursor < end:
            gaps.append((cursor, end))
        if gaps and not allow_sync:
            raise AnalyticsNotFoundError(
                f"Verified {timeframe} coverage required for '{symbol}'; use POST /api/coverage/prepare "
                f"with timeframe={timeframe}, start={isoformat_utc(start)}, end={isoformat_utc(end)}."
            )
        for gap_start, gap_end in gaps:
            _sync_market_data_window(symbol, timeframe, gap_start, gap_end)
        return {"symbol": symbol, "fetched_window_count": len(gaps), "status": "prepared"}

    return _market_sync_flights.run(("coverage", symbol, timeframe, start, end, allow_sync), prepare)


def _prepare_daily_coverage(symbol: str, start: datetime, end: datetime) -> Dict:
    return _prepare_window_coverage(symbol, "1Day", start, end)


@app.post("/api/coverage/prepare")
def prepare_analytics_coverage() -> Response:
    """Explicit, bounded preparation for features, labels and benchmarks."""
    try:
        payload = request.get_json(silent=True)
        if not isinstance(payload, dict):
            raise ValueError("A JSON object is required.")
        symbols = payload.get("symbols")
        if not isinstance(symbols, list) or not symbols or any(not isinstance(s, str) or not s.strip() for s in symbols):
            raise ValueError("'symbols' must be a non-empty array of symbol strings.")
        symbols = list(dict.fromkeys(s.strip().upper() for s in symbols))
        timeframe = _normalize_analytics_timeframe(payload.get("timeframe"))
        include_benchmarks = payload.get("include_benchmarks", timeframe == "1Day")
        if not isinstance(include_benchmarks, bool):
            raise ValueError("'include_benchmarks' must be a boolean.")
        if include_benchmarks:
            symbols = list(dict.fromkeys([*symbols, "SPY", "QQQ"]))
        if len(symbols) > 50:
            raise ValueError("At most 50 symbols, including benchmarks, may be prepared per request.")
        if timeframe != "1Day" and len(symbols) > 5:
            raise ValueError("At most 5 intraday symbols may be prepared per request.")
        start = parse_asof(payload.get("start"))
        end = parse_asof(payload.get("end"))
        if start is None or end is None or not start < end:
            raise ValueError("'start' and 'end' are required and start must precede end.")
        if end - start > timedelta(days=400):
            raise ValueError("Coverage preparation is limited to 400 calendar days per request.")
        if timeframe != "1Day" and end - start > timedelta(days=31):
            raise ValueError("Intraday preparation is limited to 31 calendar days per request.")
        if end > _utcnow() - timedelta(minutes=max(CHART_DELAY_MINUTES, 0)):
            raise ValueError("'end' must not exceed the delayed present time.")
        prepared, errors = [], []
        for symbol in symbols:
            try:
                prepared.append(_prepare_daily_coverage(symbol, start, end) if timeframe == "1Day"
                                else _prepare_window_coverage(symbol, timeframe, start, end))
            except Exception as exc:
                errors.append({"symbol": symbol, "error": str(exc)})
        return jsonify({"ok": True, "timeframe": timeframe, "start": isoformat_utc(start),
                        "end": isoformat_utc(end), "prepared": prepared, "errors": errors,
                        "count": len(prepared), "requested_symbol_count": len(symbols)})
    except (ValueError, TypeError) as exc:
        return jsonify({"ok": False, "error": str(exc)}), 400


@app.post("/api/state/batch")
def get_analytics_batch_state() -> Response:
    payload = request.get_json(silent=True) or {}
    symbols = payload.get("symbols")
    if not isinstance(symbols, list) or not symbols:
        return jsonify({"ok": False, "error": "'symbols' must be a non-empty array."}), 400

    try:
        hide_ts = _parse_bool_query_param("hidets", default=False)
        normalized_timeframe = _normalize_analytics_timeframe(payload.get("timeframe") or request.args.get("timeframe"))
        settings = _analytics_settings_from_request(payload)
        asof_dt = parse_asof(str(payload.get("asof") or request.args.get("asof") or "").strip() or None)
        normalized_symbols = [str(item).strip().upper() for item in symbols]
        if asof_dt is None:
            contexts = _prepare_current_analytics_batch(normalized_symbols, normalized_timeframe)
            batch = analytics_service.get_batch_state(normalized_symbols, normalized_timeframe, asof_dt, settings, contexts=contexts)
        else:
            batch = analytics_service.get_batch_state(normalized_symbols, normalized_timeframe, asof_dt, settings)
        batch["ok"] = True
        if hide_ts:
            batch = _strip_symbol_and_timestamps(batch)
        return jsonify(batch)
    except ValueError as exc:
        return jsonify({"ok": False, "error": str(exc)}), 400
    except Exception as exc:
        return jsonify({"ok": False, "error": str(exc)}), 500


@app.post("/api/portfolio/history")
def get_portfolio_history() -> Response:
    from analytics.historical import HistoricalBusyError, HistoricalLimitError, historical_settings

    try:
        if request.content_length is not None and request.content_length > 65536:
            raise HistoricalLimitError("Historical request body exceeds 64 KiB.")
        raw = request.stream.read(65537)
        if len(raw) > 65536:
            raise HistoricalLimitError("Historical request body exceeds 64 KiB.")
        try:
            body = json.loads(raw)
        except (ValueError, UnicodeDecodeError) as exc:
            raise ValueError("A JSON object is required.") from exc
        if not isinstance(body, dict):
            raise ValueError("A JSON object is required.")
        allowed = {"symbols", "include_hidden", "cutoffs", "timeframe", "settings", "days",
                   "stop_loss_pct", "take_profit_pct", "side"}
        if body.keys() - allowed:
            raise ValueError("Unknown request fields: " + ", ".join(sorted(body.keys() - allowed)))
        if _normalize_analytics_timeframe(body.get("timeframe") or "1Day") != "1Day":
            raise ValueError("Historical sweeps support daily timeframe only (1D/1Day).")
        include_hidden = body.get("include_hidden", False)
        if not isinstance(include_hidden, bool):
            raise ValueError("'include_hidden' must be a boolean.")
        if "symbols" in body and "include_hidden" in body:
            raise ValueError("'include_hidden' only applies when symbols are omitted.")
        symbols = body["symbols"] if "symbols" in body else _portfolio_symbols(include_hidden=include_hidden)
        result = analytics_service.get_historical_portfolio(
            symbols, body.get("cutoffs"), historical_settings(body.get("settings", {})),
            body.get("days", 20), body.get("stop_loss_pct"), body.get("take_profit_pct"), body.get("side", "long"))
        result["ok"] = True
        return jsonify(result)
    except HistoricalLimitError as exc:
        return jsonify({"ok": False, "error": str(exc)}), 413
    except HistoricalBusyError as exc:
        return jsonify({"ok": False, "error": str(exc)}), 503, {"Retry-After": "1"}
    except (ValueError, TypeError) as exc:
        return jsonify({"ok": False, "error": str(exc)}), 400
    except Exception as exc:
        return jsonify({"ok": False, "error": str(exc)}), 500


@app.get("/api/portfolio/state")
def get_portfolio_analytics_state() -> Response:
    try:
        hide_ts = _parse_bool_query_param("hidets", default=False)
        include_hidden = _parse_bool_query_param("include_hidden", default=False)
        symbols = _portfolio_symbols(include_hidden=include_hidden)
        normalized_timeframe = _normalize_analytics_timeframe(request.args.get("timeframe"))
        settings = _analytics_settings_from_request()
        asof_dt = parse_asof(request.args.get("asof"))
        if asof_dt is None:
            contexts = _prepare_current_analytics_batch(symbols, normalized_timeframe)
            batch = analytics_service.get_batch_state(symbols, normalized_timeframe, asof_dt, settings, contexts=contexts)
        else:
            batch = analytics_service.get_batch_state(symbols, normalized_timeframe, asof_dt, settings)
        batch["ok"] = True
        batch["portfolio_symbol_count"] = len(symbols)
        batch["count"] = len(batch.get("states", []))
        if hide_ts:
            batch = _strip_symbol_and_timestamps(batch)
        return jsonify(batch)
    except ValueError as exc:
        return jsonify({"ok": False, "error": str(exc)}), 400
    except Exception as exc:
        return jsonify({"ok": False, "error": str(exc)}), 500


@app.get("/api/portfolio/futurestate")
def get_portfolio_analytics_future_state() -> Response:
    days_raw = (request.args.get("days") or request.args.get("horizon_days") or "").strip()
    if not days_raw:
        return jsonify({"ok": False, "error": "'days' is required."}), 400

    try:
        days = int(days_raw)
    except ValueError:
        return jsonify({"ok": False, "error": "'days' must be an integer."}), 400

    stop_loss_pct = None
    take_profit_pct = None
    side = (request.args.get("side") or "long").strip().lower() or "long"
    stop_loss_raw = (request.args.get("stoploss") or request.args.get("stop_loss") or "").strip()
    take_profit_raw = (request.args.get("takeprofit") or request.args.get("take_profit") or "").strip()
    if stop_loss_raw:
        try:
            stop_loss_pct = float(stop_loss_raw)
        except ValueError:
            return jsonify({"ok": False, "error": "'stoploss' must be a number."}), 400
    if take_profit_raw:
        try:
            take_profit_pct = float(take_profit_raw)
        except ValueError:
            return jsonify({"ok": False, "error": "'takeprofit' must be a number."}), 400
    if side not in {"long", "short"}:
        return jsonify({"ok": False, "error": "'side' must be either 'long' or 'short'."}), 400

    try:
        hide_ts = _parse_bool_query_param("hidets", default=False)
        include_hidden = _parse_bool_query_param("include_hidden", default=False)
        symbols = _portfolio_symbols(include_hidden=include_hidden)
        normalized_timeframe = _normalize_analytics_timeframe(request.args.get("timeframe"))
        asof_dt = parse_asof(request.args.get("asof"))
        if asof_dt is None:
            return jsonify({"ok": False, "error": "'asof' is required."}), 400
        _validate_future_preparation(days, stop_loss_pct, take_profit_pct)
        preparation_errors = []
        ready_symbols = symbols
        if normalized_timeframe != "1Day":
            start, end = _future_preparation_window(asof_dt, days)
            ready_symbols = []
            for symbol in symbols:
                try:
                    _prepare_window_coverage(symbol, normalized_timeframe, start, end, allow_sync=False)
                    ready_symbols.append(symbol)
                except Exception as exc:
                    preparation_errors.append({"symbol": symbol, "error": str(exc)})
        batch = analytics_service.get_batch_future_state(
            ready_symbols,
            normalized_timeframe,
            asof_dt,
            days,
            stop_loss_pct=stop_loss_pct,
            take_profit_pct=take_profit_pct,
            side=side,
        )
        batch["errors"].extend(preparation_errors)
        batch["ok"] = True
        batch["portfolio_symbol_count"] = len(symbols)
        batch["count"] = len(batch.get("future_states", []))
        if hide_ts:
            batch = _strip_symbol_and_timestamps(batch)
        return jsonify(batch)
    except ValueError as exc:
        return jsonify({"ok": False, "error": str(exc)}), 400
    except Exception as exc:
        return jsonify({"ok": False, "error": str(exc)}), 500


@app.get("/api/health/analytics")
def analytics_health() -> Response:
    try:
        payload = analytics_service.health()
        status = 200 if payload["ok"] else 503
        return jsonify(payload), status
    except Exception as exc:
        return jsonify({"ok": False, "service": "analytics", "error": str(exc), "runtime": runtime_diagnostics()}), 500


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

    next_earnings_map = _load_next_earnings_map()
    rows = []
    if not df.empty:
        for _, r in df.iterrows():
            symbol = str(r.get("symbol", "")).upper()
            reward_score = float(r.get("reward_score")) if pd.notna(r.get("reward_score")) else DEFAULT_SCORE
            risk_score = float(r.get("risk_score")) if pd.notna(r.get("risk_score")) else DEFAULT_SCORE
            confidence_score = (
                float(r.get("confidence_score")) if pd.notna(r.get("confidence_score")) else DEFAULT_SCORE
            )
            rows.append(
                {
                    "symbol": symbol,
                    "forecast_price": float(r["forecast_price"]) if pd.notna(r.get("forecast_price")) else None,
                    "reward_score": reward_score,
                    "risk_score": risk_score,
                    "confidence_score": confidence_score,
                    "classification": str(r.get("classification", DEFAULT_CLASSIFICATION)),
                    "hidden": bool(r.get("hidden")),
                    "opportunity_score": _opportunity_score(reward_score, risk_score, confidence_score),
                    "updated_date": str(r.get("updated_date", "")),
                    "next_earnings": next_earnings_map.get(symbol),
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

        <h2>Analytics Endpoints</h2>
        <p>These endpoints expose non-visual market state JSON built from already-synced OHLCV bars in PostgreSQL.</p>

        <h2>POST /api/portfolio/history</h2>
        <p>Bounded synchronous daily historical sweep, with one history read and feature computation per symbol.
        Omit <code>symbols</code> to use current saved portfolio membership; <code>include_hidden</code> is a JSON boolean (default false).
        Explicit symbols are normalized/deduplicated. Cutoff order and duplicates are retained; date-only means end-of-day UTC.
        Only <code>1D</code>/<code>1Day</code> is supported. <code>settings</code> accepts AnalyticsSettings fields for the entire sweep.
        Defaults: days=20, side=long, thresholds=null. Percentages are fractions.</p>
<pre><code>curl -s http://127.0.0.1:5000/api/portfolio/history -H 'Content-Type: application/json' \\
  -d '{"symbols":["NVDA","AAPL"],"cutoffs":["2026-03-17","2026-06-16T00:00:00Z"],"timeframe":"1D","settings":{"breakout_lookback":55},"days":20,"stop_loss_pct":0.05,"take_profit_pct":0.10,"side":"long"}'</code></pre>
        <p>Response: <code>ok, timeframe, symbols, date_count, sample_count, limits, execution, dates</code>.
        Each <code>dates[]</code> item has <code>date_index, cutoff, requested_asof, features, labels</code>.
        Features and labels use the existing batch-state and batch-futurestate schemas, including per-symbol errors,
        quality/coverage and actual timestamps. Each date has independent ranks and SPY/QQQ alignment.
        Join by <code>(date_index, symbol)</code>: features may use stored 15-minute price anchors; realized labels always use daily bars.
        Keep realized future labels separate from historical model inputs. Partial horizons retain their existing quality flags.</p>
        <p>Limits: 120 dates, 50 input symbols, 3,000 date-symbol pairs, 1–252 future bars, 20,000 daily history bars per symbol,
        32 MiB compact JSON output, 64 KiB request. Size limits return 413 without partial output; invalid inputs return 400.
        One sweep per worker; busy returns 503 with Retry-After: 1. Split larger requests into chunks.
        Full-history seeds are preserved; oversized histories are rejected rather than truncated.
        Execution counters report history/price/tail reads, rows and feature computations.</p>
        <p>This entrypoint never synchronizes providers or creates metadata. Prepare missing daily/benchmark and 15-minute coverage explicitly
        with <code>POST /api/coverage/prepare</code>, then rerun. Recent anchors are not proof of complete data;
        <code>completeness_verified</code> remains false. Saved membership is current, not historical holdings.</p>

        <h2>GET /api/state/&lt;symbol&gt;</h2>
        <p>Returns current or backdated analytics state for one symbol. Optional query parameters: <code>timeframe</code>, <code>asof</code>, <code>breakout_lookback</code>, <code>buffer_pct</code>, <code>volume_confirmation</code>, <code>volume_multiple</code>. If you request an intraday timeframe like <code>15Min</code>, the anchor price comes from the intraday bar at or before <code>asof</code> while moving averages and regimes still use daily bars.</p>
        <p>Historical reads reuse stored price anchors and successfully verified provider coverage, fetching only missing windows. Date-only <code>asof</code> values mean end-of-day and use a 15-minute price anchor for daily analytics. A completed fetch over a market closure may use an earlier intraday bar; inspect <code>data_quality.price_bar_timestamp</code>. Required sync failures return an error, and unavailable intraday anchors return 404 after backfill. Current reads retain freshness checks, and unfinished delayed bars are rechecked rather than permanently marked covered.</p>
<pre><code>curl -s "http://127.0.0.1:5000/api/state/NVDA"
curl -s "http://127.0.0.1:5000/api/state/NVDA?timeframe=1D&asof=2023-09-20"
curl -s "http://127.0.0.1:5000/api/state/NVDA?timeframe=15Min&asof=2023-09-20T15:45:00Z"
curl -s "http://127.0.0.1:5000/api/state/NVDA?timeframe=1D&asof=2023-09-20&hidets=true"
curl -s "http://127.0.0.1:5000/api/state/NVDA/1Day?breakout_lookback=55&buffer_pct=0.005"</code></pre>

<pre><code>{
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
  "trend": {
    "regime": "uptrend",
    "trend_strength": 2.48
  },
  "momentum": {
    "regime": "bullish"
  },
  "signals": {
    "is_breakout": false,
    "above_sma20": true
  },
  "data_quality": {
    "bar_count": 252,
    "sync_status": "idle"
  }
}</code></pre>

        <h2>POST /api/state/batch</h2>
        <p>Returns analytics state for multiple symbols in a single request. Useful for watchlists and downstream agents.</p>
<pre><code>curl -X POST http://127.0.0.1:5000/api/state/batch \
  -H "Content-Type: application/json" \
  -d '{"symbols":["NVDA","AAPL","MSFT"],"timeframe":"1D","asof":"2023-09-20"}'</code></pre>

        <h2>GET /api/portfolio/state</h2>
        <p>Returns analytics state for every visible stock in the saved portfolio, where the portfolio is sourced from the stored forecast rows in <code>stock_forecasts</code>. Optional query parameters match <code>/api/state/&lt;symbol&gt;</code>, including <code>timeframe</code>, <code>asof</code>, <code>hidets</code>, and <code>include_hidden=1</code>. Symbols that cannot produce analytics are returned in <code>errors</code> instead of failing the whole request. Each state also includes portfolio cross-sectional rank features and benchmark-relative returns against available <code>SPY</code> and <code>QQQ</code> data.</p>
<pre><code>curl -s "http://127.0.0.1:5000/api/portfolio/state?timeframe=1D&asof=2023-09-20"
curl -s "http://127.0.0.1:5000/api/portfolio/state?timeframe=1D&asof=2023-09-20&include_hidden=1"</code></pre>

<pre><code>{
  "ok": true,
  "timeframe": "1Day",
  "requested_asof": "2023-09-20T23:59:59.999999Z",
  "portfolio_symbol_count": 3,
  "count": 2,
  "benchmarks": ["SPY", "QQQ"],
  "states": [
    {
      "symbol": "NVDA",
      "as_of": "2023-09-20T00:00:00Z",
      "price": {},
      "returns": {},
      "trend": {},
      "momentum": {},
      "volatility": {
        "atr_14_pct": 0.024,
        "return_20_over_vol_20": 0.31
      },
      "range": {
        "high_252": 502.66,
        "distance_from_high_252_pct": -0.08
      },
      "volume": {},
      "signals": {},
      "portfolio": {
        "rank_return_20": 0.8,
        "rank_trend_strength": 0.7
      },
      "relative": {
        "rel_return_spy_20": 0.04,
        "rel_return_qqq_20": 0.02
      },
      "data_quality": {}
    }
  ],
  "benchmark_errors": [],
  "errors": [
    {
      "symbol": "MSFT",
      "error": "No bars available for symbol 'MSFT' on timeframe '1Day'."
    }
  ]
}</code></pre>

        <h2>GET /api/events/&lt;symbol&gt;</h2>
        <p>Returns recent deterministic events for one symbol. Optional query parameters: <code>timeframe</code>, <code>asof</code>, <code>event_limit</code>, <code>breakout_lookback</code>, <code>buffer_pct</code>, <code>volume_confirmation</code>, <code>volume_multiple</code>.</p>
<pre><code>curl -s "http://127.0.0.1:5000/api/events/NVDA?timeframe=1D&event_limit=10"
curl -s "http://127.0.0.1:5000/api/events/NVDA/1Day?asof=2023-09-20&volume_multiple=2.0"</code></pre>

<pre><code>{
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
}</code></pre>

        <h2>GET /api/futurestate/&lt;symbol&gt;</h2>
        <p>The anchor and future window both use the requested timeframe. A date-only <code>asof</code> means end-of-day UTC; a timestamp is an exact cutoff. Daily requests use the latest stored daily bar at or before that cutoff and the next <code>N</code> daily bars, even for timed cutoffs. They do not switch to fifteen-minute labels. Request <code>timeframe=15Min</code> explicitly for fifteen-minute anchors and horizons. <code>anchor_price.source_timeframe</code>, <code>window.source_timeframe</code>, and <code>future_state.source_timeframe</code> report that series. A partial window returns available bars with <code>complete_window=false</code> and a null <code>target_end_timestamp</code>; no future bars produces a per-symbol error.</p>
        <p>Returns realized forward max price, min price, and end-of-window price from an <code>asof</code> anchor using the next <code>N</code> future bars. For <code>1Day</code>, <code>days</code> means trading days rather than calendar days. Required query parameters: <code>asof</code> and <code>days</code>. Optional query parameter: <code>timeframe</code>.</p>
<pre><code>curl -s "http://127.0.0.1:5000/api/futurestate/NVDA?timeframe=1D&asof=2023-09-20&days=20"
curl -s "http://127.0.0.1:5000/api/futurestate/NVDA/1Day?asof=2023-09-20T00:00:00Z&days=5"</code></pre>

<pre><code>{
  "ok": true,
  "symbol": "NVDA",
  "timeframe": "1Day",
  "as_of": "2023-09-20T00:00:00Z",
  "horizon_days": 20,
  "anchor_price": {
    "close": 444.1,
    "timestamp": "2023-09-20T00:00:00Z"
  },
  "future_state": {
    "max_price": 471.2,
    "max_return_pct": 0.061,
    "min_price": 430.4,
    "min_return_pct": -0.0308,
    "price_at_horizon": 468.7,
    "return_at_horizon_pct": 0.0554
  }
}</code></pre>

        <h2>GET /api/portfolio/futurestate</h2>
        <p>Portfolio reads perform no provider I/O. Explicit intraday requests require verified provider coverage over the seven-day anchor lookback through <code>asof + days</code>, capped at the delayed present. Uncovered symbols return per-symbol errors with preparation bounds; prepare them using <code>POST /api/coverage/prepare</code> and retry. Single-symbol intraday reads fetch only missing coverage ranges for envelopes up to 31 calendar days and report required provider failures; larger uncovered envelopes require explicit preparation in smaller windows. Successful empty windows cover market closures; unfinished delayed tails are rechecked, not permanently covered. This calendar search does not guarantee N trading bars; inspect <code>complete_window</code>.</p>
        <p>Returns realized forward performance for every visible stock in the saved portfolio from the same <code>asof</code> anchor. This is intended for labeling historical training examples after fetching technicals from <code>/api/portfolio/state</code>. Required query parameters: <code>asof</code> and <code>days</code>. Optional query parameters include <code>timeframe</code>, <code>hidets</code>, <code>include_hidden=1</code>, <code>side</code>, <code>stoploss</code>, and <code>takeprofit</code>. For <code>1Day</code>, <code>days</code> means future trading bars rather than calendar days.</p>
        <h3>Historical coverage and daily preparation</h3>
        <p>Every actual provider ingestion refreshes the full retained history and verified-window envelopes for all stored timeframes of that symbol. Publication of bars, adjustment provenance, replacement coverage receipts and cache invalidation is atomic; concurrent preparations serialize per symbol, and analytics reads use one database snapshot. Daily and intraday requests share <code>ALPACA_BAR_ADJUSTMENT</code> (default <code>split</code>). Changed configurations invalidate coverage receipts. This conservative policy increases provider I/O and holds a database connection while fetching history. Missing retained timestamps or failed pages abort the refresh and preserve the preceding generation.</p>
        <p><code>adjustment_basis</code> (under state/label <code>data_quality</code>, and at top level for events/charts) reports the latest content/configuration revision, provider source, modes, adjusted-through date and check time. The revision fingerprints the provider history; it is not a corporate-action event ID or a historical data-vintage archive. Legacy rows have null provenance until successfully refreshed. <code>economic_actions_verified=false</code> remains explicit: provider split adjustment does not certify ADR conversions or other non-split actions (VEC-830), and no correction is inferred from move magnitude. Default split-adjusted returns are price returns without dividend reinvestment.</p>
        <p>HTTP success and <code>ok=true</code> report execution, not validated investment performance or complete history. Ingestion rejects an entire provider page before writing if required OHLCV is missing, nonnumeric, nonfinite, nonpositive OHLC, outside its high/low range, or has negative/fractional volume. Zero volume is valid. Analytics rejects the entire affected symbol/window, never fills or drops invalid bars, and keeps other portfolio symbols in <code>states</code> with failures in <code>errors</code>. Shared historical sweeps reject the symbol for all requested cutoffs if their loaded history is malformed. The database constraint enforces new writes; its NOT VALID migration does not certify pre-existing rows.</p>
        <p><strong>Completed-bar policy:</strong> All state paths (single, batch, portfolio and historical), benchmarks, events and future-label anchors consume only completed stored bars. Daily OHLCV may include extended hours and becomes eligible at the <strong>next America/New_York midnight</strong> after its UTC-labelled session date (both legacy 00Z and provider 04Z/05Z labels). DST follows that timezone. Neither a regular close nor an early close releases the full-day aggregate. Intraday bars, including premarket/postmarket, become eligible at <code>bar_start + full interval &lt;= cutoff</code>; intervals are not shortened at exchange closes. No partial bars are built. At 14:00Z a 13:45Z 15-minute bar is eligible, a 14:00Z bar is not. Date-only cutoffs remain 23:59:59.999999 UTC, so that New York date's daily aggregate is still unavailable; use the following NY midnight to include it. Holidays/weekends do not create synthetic bars. Price-dependent calculations use the eligible price anchor. Future labels use the next N stored bars strictly after their eligible anchor; the first label bar can overlap the requested instant and deliberately contains future data.</p>
        <p><code>data_quality.bar_availability</code>, <code>indicator_available_at</code>, <code>price_available_at</code> and label <code>anchor_available_at</code> expose this policy. Completion is conservative event-time eligibility, not proof of provider publication time: receipt times and revisions are not versioned. <code>ohlcv_validation</code> certifies numeric/range checks only. <code>historical_coverage</code> separates indicators and price; labels report anchor coverage and <code>complete_future_window</code> (bar count only). Current requests evaluate against now rounded down to the minute; historical requests use the cutoff. Daily freshness uses the NYSE calendar's latest eligible session (holidays, weekends, DST and early closes accounted for); intraday freshness compares regular-session completed intervals, not extended-hours completeness. <code>expected_latest_completed</code>, <code>freshness_status</code>, <code>anchor_completed</code> and remediation <code>action</code> explain results. Legacy <code>within_tolerance</code> means no missing expected eligible session/interval, not proof of complete history.</p>
        <p>State <code>session_coverage</code> reports missing and out-of-calendar session dates within observed history bounds, with counts and up to 20 examples. This assumes US-equity NYSE regular sessions, not symbol-specific halts, listing lifetime, extended hours, or full intraday interval coverage. <code>metric_history</code> reports metric-specific valid-bar warmup and finite-value availability; legacy 20/50/200 flags count validated observations, not completed sessions. Check these independently of session coverage. Corporate-action consistency is <code>unknown</code> without authoritative adjustment provenance; absolute one-bar moves of at least 50% produce a review warning, never automatic rejection or normalization. Inspect VEC-830/VEC-831 and the flagged timestamps. A recent anchor, enough rows or passing numeric checks does not verify the economic adjustment basis. Overall <code>completeness_verified</code> remains false. Batch summaries list stale, session-warning and corporate-action-warning symbols.</p>
        <p>Benchmark quality appears under each state's <code>data_quality.benchmark_coverage</code>. Missing or stale benchmarks report <code>benchmark_errors</code>; relative returns are omitted for stale benchmarks or unequal indicator timestamps. For daily training, join by symbol AND indicator/label anchor timestamp, require a complete future window, and inspect coverage. A timed daily state may use an intraday price while its indicators and daily labels use daily bars; joining by symbol alone is insufficient. Preserve timestamps rather than using <code>hidets</code> when pairing.</p>
        <p><code>POST /api/coverage/prepare</code> explicitly prepares daily bars using JSON <code>symbols</code>, <code>start</code>, <code>end</code>, and optional boolean <code>include_benchmarks</code> (default true, adds SPY and QQQ). Use ISO timestamps for precise bounds. Limits: 50 unique symbols including benchmarks and 400 calendar days per request; end must be at or before the delayed present. Select a window containing indicator warmup, the cutoff and future-label horizon; split longer histories into bounded requests. Only gaps in previously verified provider windows are fetched. Completed empty windows cover closures too; recent unfinalized tails may be fetched again. Provider failures are collected per symbol in <code>errors</code>; <code>ok=true</code> does not mean every symbol prepared. Preparation verifies provider responses, not that a provider supplied every trading session. Subsequent analytics reads expose actual anchors and realized horizon counts.</p>
        <pre>curl -s http://127.0.0.1:5000/api/coverage/prepare -H 'Content-Type: application/json' -d '{"symbols":["NVDA"],"start":"2025-11-01T00:00:00Z","end":"2026-10-01T00:00:00Z"}'</pre>
        <p>For intraday labels, supply <code>timeframe</code> (for example <code>15Min</code>). Intraday preparation is limited to 5 unique symbols including benchmarks and 31 calendar days per request; split larger work into bounded requests. Benchmarks default to excluded for intraday preparation. Daily limits and defaults above are unchanged. Identical preparation coalesces within each worker; separate processes may fetch concurrently.</p>
<pre><code>curl -s "http://127.0.0.1:5000/api/portfolio/futurestate?timeframe=1D&asof=2023-09-20&days=20"</code></pre>

<pre><code>{
  "ok": true,
  "timeframe": "1Day",
  "requested_asof": "2023-09-20T23:59:59.999999Z",
  "horizon_days": 20,
  "side": "long",
  "portfolio_symbol_count": 3,
  "count": 2,
  "future_states": [
    {
      "symbol": "NVDA",
      "as_of": "2023-09-20T00:00:00Z",
      "anchor_price": {
        "close": 444.1,
        "timestamp": "2023-09-20T00:00:00Z",
        "source_timeframe": "1Day"
      },
      "window": {
        "future_bar_count": 20,
        "complete_window": true,
        "source_timeframe": "1Day"
      },
      "future_state": {
        "max_return_pct": 0.061,
        "min_return_pct": -0.0308,
        "return_at_horizon_pct": 0.0554
      }
    }
  ],
  "errors": [
    {
      "symbol": "MSFT",
      "error": "No future bars available for symbol 'MSFT' on timeframe '1Day' for the requested window."
    }
  ]
}</code></pre>

        <h2>GET /api/metadata/&lt;symbol&gt;</h2>
        <p>Returns symbol metadata and data freshness fields from <code>symbol_metadata</code> and <code>symbol_sync_state</code>.</p>
<pre><code>curl -s "http://127.0.0.1:5000/api/metadata/NVDA"</code></pre>

        <h2>GET /api/health/analytics</h2>
        <p>Returns database connectivity and runtime diagnostics: artifact revision/release ID,
        configured/effective backend, extension availability/version, and process-local native/Python
        execution counts with fallback reasons. Cache hits and synthetic release checks are excluded.
        Returns 503 if Rust is explicitly configured but unavailable. State (including batch/portfolio)
        and events use the indicator kernel; futurestate price-window labels do not.
        Run <code>python -m analytics.release_check --require-rust --expected-revision REV --expected-release ID</code>
        inside the final runtime to prove native execution with synthetic data and verify artifact identity.</p>
        <p><code>caches</code> reports per-worker state/event/future hit, miss, eviction,
        computation, coalesced-wait, failure and admission-rejection counters, current entries/accounted
        bytes, in-flight producers/waiters and configured limits. Counters have no symbol/date labels.
        Single-symbol analytics return 503 with <code>Retry-After: 1</code> on cache admission exhaustion;
        batches retain per-symbol errors. Cache budgets and revision limitations are documented in README.</p>
<pre><code>curl -s "http://127.0.0.1:5000/api/health/analytics"</code></pre>

        <h2>Analytics Query Parameters</h2>
        <ul>
          <li><code>timeframe</code>: one of <code>1Day</code>, <code>1D</code>, <code>1Hour</code>, <code>15Min</code>, <code>5Min</code>, <code>1Min</code></li>
          <li><code>asof</code>: optional backdated cutoff; accepts <code>YYYY-MM-DD</code> or full ISO 8601 timestamps</li>
          <li><code>hidets</code>: optional boolean, default <code>false</code>; when true, removes <code>symbol</code> and timestamp-like fields from the response</li>
          <li><code>include_hidden</code>: optional boolean for portfolio endpoints, default <code>false</code>; when true, includes hidden portfolio rows</li>
          <li><code>days</code>: required by future-state endpoints; number of future bars to evaluate from the <code>asof</code> anchor</li>
          <li><code>event_limit</code>: optional max number of events returned, default <code>20</code></li>
          <li><code>breakout_lookback</code>: optional prior-bar lookback for breakout and breakdown rules, default <code>20</code></li>
          <li><code>buffer_pct</code>: optional breakout and breakdown buffer, default <code>0.0025</code></li>
          <li><code>volume_confirmation</code>: optional boolean, default <code>true</code></li>
          <li><code>volume_multiple</code>: optional volume confirmation threshold, default <code>1.5</code></li>
        </ul>

        <h2>Analytics Outputs</h2>
        <p>The state payload includes returns, moving averages, EMA, RSI, MACD, ATR, realized volatility, range positioning, volume metrics, normalized strengths, regime labels, signal flags, and data quality metadata. Percentage-style fields use a <code>_pct</code> suffix, and nulls are returned for values that cannot be computed yet from short history.</p>
        <p><code>returns.ytd</code> is daily-close price return from the preceding calendar year's final NYSE trading-session close, including the first trading day's gain/loss. Both prices use the stored adjustment basis; dividends are not reinvested. Missing final-session anchors (including new listings or incomplete histories) return null, with no first-in-year or older-year fallback. <code>data_quality.ytd</code> reports availability and qualifies available values because adjustment provenance is unverified. Python/Rust, snapshot and historical paths share this rule. YTD uses the daily indicator close even when <code>price</code> uses a newer intraday anchor.</p>

        <h2>Event Types</h2>
        <p>Possible event types include <code>breakout</code>, <code>breakdown</code>, price crosses vs. <code>SMA 20/50/200</code>, <code>macd_bullish_cross</code>, <code>macd_bearish_cross</code>, <code>rsi_enters_overbought</code>, <code>rsi_enters_oversold</code>, <code>new_high_20</code>, <code>new_low_20</code>, <code>new_high_55</code>, <code>new_low_55</code>, <code>volume_spike</code>, and <code>volatility_spike</code>.</p>

        <h2>GET /api/stocks</h2>
        <p>Returns the latest stored row per stock from PostgreSQL, including <code>updated_date</code> and <code>next_earnings</code>. Optional query parameters: <code>symbol</code> and <code>limit</code>.</p>
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

        <h2>GET /api/chart/&lt;symbol&gt;/latest</h2>
        <p>Returns the latest quote, latest cached bar, and sync metadata for a symbol.</p>
<pre><code>curl -s "http://127.0.0.1:5000/api/chart/AAPL/latest?timeframe=1Day"</code></pre>

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
                bool(r.get("hidden")),
                _opportunity_score(reward_score, risk_score, confidence_score),
            ]
        )

    out = Response(stream.getvalue(), mimetype="text/csv")
    out.headers["Content-Disposition"] = "attachment; filename=stocks.csv"
    return out


@app.get("/health")
def health() -> Response:
    runtime = runtime_diagnostics()
    try:
        _init_db_if_needed()
        with _get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT 1")
                cur.fetchone()
        return jsonify({"ok": runtime["ready"], "database": "postgres", "table": DB_TABLE, "runtime": runtime}), 200 if runtime["ready"] else 503
    except Exception as e:
        return jsonify({"ok": False, "database": "postgres", "error": str(e), "runtime": runtime}), 500


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
