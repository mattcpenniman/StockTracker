from __future__ import annotations

import calendar
import csv
import io
import json
import os
import re
import threading
from urllib.parse import urlencode
from urllib.request import Request, urlopen
from datetime import date
from typing import Dict, List, Tuple

import pandas as pd
from flask import Flask, Response, jsonify, request

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
            <td><code>${{r.symbol}}</code></td>
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
          <div class=\"muted\">Opportunity = (Reward×4) + ((10-Risk)×4) + (Confidence×2).</div>
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
