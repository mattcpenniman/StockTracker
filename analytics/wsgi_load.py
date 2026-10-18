"""Isolated benchmark ONLY: production WSGI startup with provider I/O prohibited.

Use a read-only DATABASE_URL and WSGI_INIT_DB=0. No algorithm changes or cache
overrides. Patches live for the worker lifetime and turn accidental sync into
failed load validation rather than silently fetching fresh data.
"""
import app
from .portfolio_load_local import prohibited

app._sync_market_data = prohibited
app._sync_market_data_window = prohibited
app._backfill_analytics_symbol_if_needed = prohibited
app._live_prices.provider.fetch = prohibited

from wsgi import application  # noqa: E402,F401


@app.app.get("/__load/slow")
def slow():
    with app._get_conn() as conn, conn.cursor() as cur:
        cur.execute("SELECT pg_sleep(2)")
    return {"ok": True}
