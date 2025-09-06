# Stock Forecast Tracker – single-file Flask app
# ------------------------------------------------
# Quick start:
#   1) pip install flask yfinance pandas python-dotenv
#   2) python app.py
#   3) Open http://127.0.0.1:5000
#
# Features:
#  - Add a stock symbol, forecasted price, and updated date.
#  - Data stored in a backend CSV (stocks.csv).
#  - Sortable table shows Current Price, $ Diff, and % Diff.
#  - CSV auto-creates on first run. Existing symbols are updated (not duplicated).
#  - "Refresh Prices" button to refetch latest prices.
#  - Download CSV via /export
#
# Notes:
#  - Current prices fetched via yfinance (no API key required).
#  - If a price can't be fetched, cells show "—" and diffs are blank.

from __future__ import annotations

import os
import threading
from datetime import date
from typing import Dict, List

import pandas as pd
from flask import Flask, jsonify, request, send_file, Response
from earnings_data import fetch_fmp_and_save_csv

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

app = Flask(__name__)

CSV_PATH = os.environ.get("STOCK_TRACKER_CSV", "stocks.csv")
CSV_HEADERS = ["symbol", "forecast_price", "updated_date"]
EARNINGS_CSV = os.environ.get("EARNINGS_CSV", "earnings.csv")
_LOCK = threading.Lock()


def _init_csv_if_needed() -> None:
    if not os.path.exists(CSV_PATH):
        with _LOCK:
            if not os.path.exists(CSV_PATH):
                pd.DataFrame(columns=CSV_HEADERS).to_csv(CSV_PATH, index=False)


def _read_df() -> pd.DataFrame:
    _init_csv_if_needed()
    try:
        df = pd.read_csv(CSV_PATH, dtype={"symbol": str}, keep_default_na=False)
    except Exception:
        # If file corrupt, recreate with headers
        df = pd.DataFrame(columns=CSV_HEADERS)
        df.to_csv(CSV_PATH, index=False)
    # Normalize
    if not df.empty:
        df["symbol"] = df["symbol"].str.upper().str.strip()
        # coerce forecast_price to float
        df["forecast_price"] = pd.to_numeric(df["forecast_price"], errors="coerce")
        df["updated_date"] = df["updated_date"].astype(str)
    return df


def _write_df(df: pd.DataFrame) -> None:
    with _LOCK:
        df.to_csv(CSV_PATH, index=False)


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
        # yfinance keys are exactly the input tickers uppercased
        tickers = yf.Tickers(" ".join(symbols))
        for sym, t in tickers.tickers.items():
            sym_u = sym.upper()
            price = None
            try:
                # Try fast_info first (available in newer yfinance)
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
        # Fallback: loop each symbol individually if batch failed
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
    """Return mapping SYMBOL -> next earnings date (YYYY-MM-DD) from earnings CSV.
    Logic mimics:
        df = pd.read_csv("earnings.csv"); df.sort_values("date");
        df = df[pd.isna(df["epsActual"])]  # upcoming only
        df.drop_duplicates("symbol")        # first future date per symbol
    """
    try:
        if not os.path.exists(EARNINGS_CSV):
            return {}
        edf = pd.read_csv(EARNINGS_CSV, dtype={"symbol": str}, keep_default_na=True)
    except Exception:
        return {}
    if edf.empty or "symbol" not in edf.columns or "date" not in edf.columns:
        return {}
    edf["symbol"] = edf["symbol"].astype(str).str.upper().str.strip()
    # Keep only entries without actual EPS (future)
    if "epsActual" in edf.columns:
        edf = edf[pd.isna(edf["epsActual"])].copy()
    try:
        edf["date"] = pd.to_datetime(edf["date"], errors="coerce").dt.date.astype(str)
    except Exception:
        pass
    edf = edf.sort_values("date").drop_duplicates("symbol", keep="first")
    return dict(zip(edf["symbol"], edf["date"]))


# ----------------- Routes ----------------------

@app.get("/")
def index() -> Response:
    html = f"""
<!doctype html>
<html lang=\"en\">
  <head>
    <meta charset=\"utf-8\" />
    <meta name=\"viewport\" content=\"width=device-width, initial-scale=1\" />
    <title>Stock Forecast Tracker</title>
    <script>
      // Simple sort state
      let SORT_KEY = 'symbol';
      let SORT_ASC = true;
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
        // Handle undefined/null gracefully
        if (av == null && bv == null) return 0;
        if (av == null) return 1;
        if (bv == null) return -1;
        if (typeof av === 'number' && typeof bv === 'number') {{
          return av - bv;
        }}
        return String(av).localeCompare(String(bv));
      }}

      function renderTable() {{
        const data = [...ROWS].sort((a,b) => byKey(a,b,SORT_KEY) * (SORT_ASC ? 1 : -1));
        const tbody = document.getElementById('tbody');
        tbody.innerHTML = '';
        for (const r of data) {{
          const diff_dollar = r.diff_dollar == null ? '—' : r.diff_dollar.toLocaleString(undefined, {{maximumFractionDigits: 2}});
          const diff_pct = r.diff_pct == null ? '—' : (r.diff_pct).toFixed(2) + '%';
          const current_price = r.current_price == null ? '—' : r.current_price.toLocaleString(undefined, {{maximumFractionDigits: 2}});
          const forecast_price = r.forecast_price == null ? '—' : r.forecast_price.toLocaleString(undefined, {{maximumFractionDigits: 2}});
          const next_earnings = (r.next_earnings == null || r.next_earnings === '') ? '—' : r.next_earnings;
          const colorClass = r.diff_dollar == null ? '' : (r.diff_dollar >= 0 ? 'pos' : 'neg');
          const yahooUrl = `https://finance.yahoo.com/quote/${{encodeURIComponent(r.symbol)}}`;
          const tr = document.createElement('tr');
          tr.innerHTML = `
            <td><a href="${{yahooUrl}}" target="_blank" rel="noopener">${{r.symbol}}</a></td>
            <td class="num">${{forecast_price}}</td>
            <td>${{r.updated_date}}</td>
            <td>${{next_earnings}}</td>
            <td class="num">${{current_price}}</td>
            <td class="num ${{colorClass}}">${{diff_dollar}}</td>
            <td class="num ${{colorClass}}">${{diff_pct}}</td>
          `;
          tbody.appendChild(tr);
        }}

        // Update sort UI
        for (const th of document.querySelectorAll('th')) {{ th.classList.remove('sorted'); th.classList.remove('desc'); }}
        const th = document.querySelector(`th[data-key="${{SORT_KEY}}"]`);
        if (th) {{ th.classList.add('sorted'); if (!SORT_ASC) th.classList.add('desc'); }}
      }}

      async function onSubmitForm(e) {{
        e.preventDefault();
        const sym = document.getElementById('symbol').value.trim();
        const forecast = document.getElementById('forecast').value.trim();
        const upd = document.getElementById('updated_date').value.trim();
        if (!sym || !forecast) {{ alert('Symbol and forecast are required.'); return; }}
        const res = await fetch('/add', {{
          method: 'POST',
          headers: {{ 'Content-Type': 'application/json' }},
          body: JSON.stringify({{ symbol: sym, forecast_price: forecast, updated_date: upd }})
        }});
        if (!res.ok) {{ alert('Failed to save.'); return; }}
        const formEl = document.getElementById('add-form'); if (formEl && typeof formEl.reset === 'function') formEl.reset();
        await fetchData();
      }}

      function onRefresh() {{ fetchData(); }}

      window.addEventListener('DOMContentLoaded', () => {{
        document.getElementById('add-form').addEventListener('submit', onSubmitForm);
        document.getElementById('refresh-btn').addEventListener('click', onRefresh);
        fetchData();
      }});
    </script>
    <style>
      :root {{
        --bg: #0b0f14; --card: #0f1520; --muted: #aab8c5; --text: #e6edf3; --accent: #2e90fa; --pos: #12b886; --neg: #f03e3e;
        --border: #1f2a3a;
      }}
      html, body {{ background: var(--bg); color: var(--text); font-family: ui-sans-serif, system-ui, -apple-system, 'Segoe UI', Roboto, 'Helvetica Neue', Arial; margin: 0; }}
      .wrap {{ max-width: 1100px; margin: 24px auto; padding: 0 16px; }}
      .card {{ background: var(--card); border: 1px solid var(--border); border-radius: 16px; padding: 16px; box-shadow: 0 4px 24px rgba(0,0,0,0.25); }}
      h1 {{ font-size: 22px; margin: 0 0 12px; letter-spacing: 0.3px; }}
      p.muted {{ color: var(--muted); margin-top: 6px; }}
      form {{ display: grid; grid-template-columns: repeat(6, 1fr); gap: 10px; align-items: end; margin-bottom: 16px; }}
      label {{ font-size: 12px; color: var(--muted); display: block; margin-bottom: 6px; }}
      input {{ width: 100%; padding: 10px 12px; border-radius: 10px; border: 1px solid var(--border); background: #0c121b; color: var(--text); }}
      button {{ padding: 10px 14px; border-radius: 10px; border: 1px solid var(--border); background: #142033; color: var(--text); cursor: pointer; transition: transform .06s ease, background .2s; }}
      button:hover {{ transform: translateY(-1px); background: #182843; }}
      .actions {{ display: flex; gap: 10px; align-items: center; }}
      table {{ width: 100%; border-collapse: collapse; }}
      th, td {{ padding: 10px 12px; border-bottom: 1px solid var(--border); }}
      th {{ text-align: left; font-weight: 600; color: var(--muted); user-select: none; cursor: pointer; }}
      th.sorted {{ color: var(--text); }}
      th.sorted::after {{ content: ' \25B2'; opacity: .7; }}
      th.sorted.desc::after {{ content: ' \25BC'; opacity: .7; }}
      td.num {{ text-align: right; }}
      td.pos {{ color: var(--pos); }}
      td.neg {{ color: var(--neg); }}
      .topbar {{ display: flex; justify-content: space-between; align-items: baseline; margin-bottom: 10px; }}
      .small {{ font-size: 12px; color: var(--muted); }}
      a {{ color: var(--accent); text-decoration: none; }}
      a:hover {{ text-decoration: underline; }}
      /* Smaller date field for earnings updater */
      #earnings_to {{ width: 160px; padding: 8px 10px; }}
      @media (max-width: 860px) {{
        form {{ grid-template-columns: 1fr 1fr; }}
      }}
    </style>
  </head>
  <body>
    <div class="wrap">
      <div class="card">
        <div class="topbar">
          <h1>📈 Stock Forecast Tracker</h1>
          <div class="actions">
            <button id="refresh-btn" title="Refetch current prices">Refresh Prices</button>
            <a href="/export"><button type="button" title="Download CSV">Download CSV</button></a>
          </div>
        </div>
        <p class="muted">Add a symbol with your forecast and last updated date. Data is persisted to <code>stocks.csv</code> on the server.</p>
        <form id="add-form">
          <div>
            <label for="symbol">Symbol</label>
            <input id="symbol" name="symbol" placeholder="AAPL" required />
          </div>
          <div>
            <label for="forecast">Forecasted Price</label>
            <input id="forecast" name="forecast" type="number" step="0.01" placeholder="200" required />
          </div>
          <div>
            <label for="updated_date">Updated Date</label>
            <input id="updated_date" name="updated_date" type="date" value="{date.today().isoformat()}" />
          </div>
          <div>
            <label>&nbsp;</label>
            <button type="submit">Add / Update</button>
          </div>
        </form>
         <!-- Earnings Calendar Updater -->
        <form action=\"/update-earnings\" method=\"post\" style=\"margin-top:14px; display:grid; grid-template-columns: 1fr auto; gap:10px; align-items:end;\">
          <div>
            <label for=\"earnings_to\">Earnings calendar up to (date)</label>
            <input id=\"earnings_to\" name=\"to\" type=\"date\" value=\"2025-12-16\" />
            <div class=\"small\">Saves to <code>earnings.csv</code>.</div>
          </div>
          <div>
            <label>&nbsp;</label>
            <button type=\"submit\">Update Earnings</button>
          </div>
        </form>
        <div class="small">Last refreshed: <span id="last-refreshed">—</span></div>
        <div style="overflow-x:auto; margin-top: 10px;">
          <table>
            <thead>
              <tr>
                <th data-key="symbol" onclick="sortBy('symbol')">Symbol</th>
                <th class="num" data-key="forecast_price" onclick="sortBy('forecast_price')">Forecast ($)</th>
                <th data-key="updated_date" onclick="sortBy('updated_date')">Updated</th>
                <th data-key="next_earnings" onclick="sortBy('next_earnings')">Next ER</th>
                <th class="num" data-key="current_price" onclick="sortBy('current_price')">Current ($)</th>
                <th class="num" data-key="diff_dollar" onclick="sortBy('diff_dollar')">Δ $</th>
                <th class="num" data-key="diff_pct" onclick="sortBy('diff_pct')">Δ %</th>
              </tr>
            </thead>
            <tbody id="tbody"></tbody>
          </table>
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
    symbol = (payload.get("symbol") or "").strip().upper()
    forecast_raw = payload.get("forecast_price")
    updated_date = (payload.get("updated_date") or "").strip() or date.today().isoformat()

    if not symbol:
        return jsonify({"ok": False, "error": "Symbol is required."}), 400

    try:
        forecast_price = float(forecast_raw)
    except Exception:
        return jsonify({"ok": False, "error": "Invalid forecast price."}), 400

    df = _read_df()

    if df.empty:
        df = pd.DataFrame([[symbol, forecast_price, updated_date]], columns=CSV_HEADERS)
    else:
        mask = df["symbol"].str.upper() == symbol
        if mask.any():
            df.loc[mask, ["symbol", "forecast_price", "updated_date"]] = [symbol, forecast_price, updated_date]
        else:
            df = pd.concat([df, pd.DataFrame([[symbol, forecast_price, updated_date]], columns=CSV_HEADERS)], ignore_index=True)

    _write_df(df)
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

            diff_dollar = None
            diff_pct = None
            if cur is not None and fpx not in (None, 0):
                diff_dollar = fpx - cur
                try:
                    diff_pct = (diff_dollar / fpx) * 100.0
                except Exception:
                    diff_pct = None

            rows.append({
                "symbol": sym,
                "forecast_price": fpx,
                "updated_date": str(r.get("updated_date", "")),
                "next_earnings": next_map.get(sym),
                "current_price": cur,
                "diff_dollar": diff_dollar,
                "diff_pct": diff_pct,
            })

    return jsonify({"rows": rows})


@app.get("/export")
def export_csv() -> Response:
    _init_csv_if_needed()
    return send_file(CSV_PATH, as_attachment=True, download_name=os.path.basename(CSV_PATH))


@app.get("/health")
def health() -> Response:
    ok = os.path.exists(CSV_PATH)
    return jsonify({"ok": ok, "csv": CSV_PATH})


@app.post("/update-earnings")
def update_earnings() -> Response:
    # Accept either form post or JSON
    to_date = (request.form.get("to") or (request.get_json(silent=True) or {}).get("to") or date.today().isoformat())
    api_key = os.environ.get("FMP_API_KEY") or os.environ.get("FMP_APIKEY") or os.environ.get("FMP_KEY")
    try:
        n = fetch_fmp_and_save_csv(to_date, api_key, EARNINGS_CSV)
    except Exception as e:
        return Response(f"<pre>Update failed: {e}</pre>", mimetype="text/html", status=500)
    return Response(f"<pre>Updated earnings file '{EARNINGS_CSV}' with {n} rows for to={to_date}. <a href='/'>&larr; back</a></pre>", mimetype="text/html")


if __name__ == "__main__":
    # You can override host/port via env vars: HOST, PORT
    host = os.environ.get("HOST", "127.0.0.1")
port_var = os.environ.get("PORT", "5000")
try:
    port = int(port_var)
except (TypeError, ValueError):
    port = 5000
print(f"Running on http://{host}:{port} (dotenv={'on' if _DOTENV_LOADED else 'off'})")
app.run(host=host, port=port, debug=True)

