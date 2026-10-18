"""Worker-local WSGI startup. A failed check aborts worker boot, before serving."""
import json
import os

import app
from analytics import runtime
from analytics.release_check import verify_release


def initialize_worker():
    # Schema migration belongs to the launcher/deploy job, not concurrent workers.
    # Validate required tables/columns without DDL, including on read-only replicas.
    with app._get_conn() as conn:
        with conn.cursor() as cur:
            for table, columns in (
                (app.DB_TABLE, "symbol, forecast_price, updated_date, reward_score, risk_score, confidence_score, classification, hidden"),
                (app.EARNINGS_TABLE, "symbol, earnings_date"),
                ("symbol_metadata", "id, symbol"),
                ("symbol_sync_state", "symbol_id, last_bar_synced_at"),
                ("stock_bars", "symbol_id, timeframe, bar_time, open, high, low, close, volume"),
                ("market_data_coverage", "symbol_id, timeframe, window_start, window_end"),
                ("latest_quotes", "symbol_id, quote_time"),
                ("latest_bars", "symbol_id, bar_time"),
            ):
                cur.execute(f"SELECT {columns} FROM {table} LIMIT 0")
    result = verify_release(allow_unversioned=True)
    if not result["ok"]:
        raise RuntimeError(f"Worker analytics startup failed: {result['errors']}")
    app._DB_INITIALIZED_FOR = (os.getpid(), app.DATABASE_URL, app.DB_TABLE, app.EARNINGS_TABLE, app._get_conn)
    runtime.WORKER_STARTUP = {"database": "verified", "analytics_smoke": result["smoke"]}
    app.app.debug = False
    app.app.logger.warning("WSGI worker ready: %s", json.dumps(runtime.runtime_diagnostics(), sort_keys=True))


initialize_worker()


def application(environ, start_response):
    def process_response(status, headers, exc_info=None):
        return start_response(status, headers + [("X-Worker-Pid", str(os.getpid()))], exc_info)
    return app.app(environ, process_response)
