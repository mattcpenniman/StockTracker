from __future__ import annotations

import os
from datetime import datetime, timezone
from typing import Any

import pandas as pd

try:
    import psycopg2
except Exception:  # pragma: no cover
    psycopg2 = None


DATABASE_URL = os.environ.get(
    "DATABASE_URL",
    "postgresql://stock_user:stock_pass@localhost:5432/stock_tracker",
)


class AnalyticsRepository:
    def __init__(self, database_url: str | None = None):
        self.database_url = database_url or DATABASE_URL

    def get_conn(self):
        if psycopg2 is None:
            raise RuntimeError("Missing dependency: psycopg2-binary is required for PostgreSQL access.")
        return psycopg2.connect(self.database_url)

    def ping(self) -> bool:
        with self.get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT 1")
                return bool(cur.fetchone())

    def fetch_symbol_context(self, symbol: str) -> dict[str, Any] | None:
        normalized_symbol = str(symbol).strip().upper()
        with self.get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT sm.id,
                           sm.symbol,
                           sm.exchange,
                           sm.asset_class,
                           sm.name,
                           sm.is_active,
                           sm.created_at,
                           sm.updated_at,
                           ss.last_synced_at,
                           ss.last_successful_sync_at,
                           ss.last_quote_synced_at,
                           ss.last_bar_synced_at,
                           ss.latest_bar_time,
                           ss.latest_quote_time,
                           ss.sync_status,
                           ss.sync_error
                    FROM symbol_metadata sm
                    LEFT JOIN symbol_sync_state ss ON ss.symbol_id = sm.id
                    WHERE sm.symbol = %s
                    """,
                    (normalized_symbol,),
                )
                row = cur.fetchone()

        if not row:
            return None

        return {
            "symbol_id": int(row[0]),
            "symbol": row[1],
            "exchange": row[2],
            "asset_class": row[3],
            "name": row[4],
            "is_active": bool(row[5]),
            "created_at": row[6],
            "updated_at": row[7],
            "last_synced_at": row[8],
            "last_successful_sync_at": row[9],
            "last_quote_synced_at": row[10],
            "last_bar_synced_at": row[11],
            "latest_bar_time": row[12],
            "latest_quote_time": row[13],
            "sync_status": row[14] or "idle",
            "sync_error": row[15],
        }

    def fetch_bars(
        self,
        symbol_id: int,
        timeframe: str,
        asof: datetime | None = None,
        start: datetime | None = None,
    ) -> pd.DataFrame:
        sql = """
            SELECT bar_time, open, high, low, close, volume
            FROM stock_bars
            WHERE symbol_id = %s
              AND timeframe = %s
        """
        params: list[Any] = [symbol_id, timeframe]
        if start is not None:
            sql += " AND bar_time >= %s"
            params.append(start.astimezone(timezone.utc))
        if asof is not None:
            sql += " AND bar_time <= %s"
            params.append(asof.astimezone(timezone.utc))
        sql += " ORDER BY bar_time ASC"

        with self.get_conn() as conn:
            df = pd.read_sql_query(
                sql,
                conn,
                params=params,
                parse_dates=["bar_time"],
            )

        if df.empty:
            return pd.DataFrame(columns=["timestamp", "open", "high", "low", "close", "volume"])

        timestamps = pd.to_datetime(df["bar_time"], utc=True)
        return pd.DataFrame(
            {
                "timestamp": timestamps,
                "open": pd.to_numeric(df["open"], errors="coerce"),
                "high": pd.to_numeric(df["high"], errors="coerce"),
                "low": pd.to_numeric(df["low"], errors="coerce"),
                "close": pd.to_numeric(df["close"], errors="coerce"),
                "volume": pd.to_numeric(df["volume"], errors="coerce"),
            }
        )
