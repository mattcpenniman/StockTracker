from __future__ import annotations

import os
from datetime import datetime, timezone
from typing import Any

import pandas as pd
from database import get_connection


DATABASE_URL = os.environ.get(
    "DATABASE_URL",
    "postgresql://stock_user:stock_pass@localhost:5432/stock_tracker",
)


class AnalyticsRepository:
    def __init__(self, database_url: str | None = None):
        self.database_url = database_url or DATABASE_URL

    def get_conn(self):
        return get_connection(self.database_url)

    def ping(self) -> bool:
        with self.get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT 1")
                return bool(cur.fetchone())

    def fetch_symbol_context(self, symbol: str) -> dict[str, Any] | None:
        normalized_symbol = str(symbol).strip().upper()
        return self.fetch_symbol_contexts([normalized_symbol]).get(normalized_symbol)

    def fetch_symbol_contexts(self, symbols: list[str]) -> dict[str, dict[str, Any]]:
        symbols = list(dict.fromkeys(str(s).strip().upper() for s in symbols))
        if not symbols:
            return {}
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
                    WHERE sm.symbol = ANY(%s)
                    """,
                    (symbols,),
                )
                rows = cur.fetchall()
        return {row[1]: self._context_row(row) for row in rows}

    @staticmethod
    def _context_row(row) -> dict[str, Any]:
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
        return self._fetch_bars(symbol_id, timeframe, asof=asof, start=start)

    def fetch_latest_bar(
        self, symbol_id: int, timeframe: str, asof: datetime | None = None,
    ) -> pd.DataFrame:
        """Return at most one bar, including a bar exactly at the as-of."""
        return self._fetch_bars(symbol_id, timeframe, asof=asof, descending=True, limit=1)

    def fetch_future_bars(
        self, symbol_id: int, timeframe: str, after: datetime, limit: int,
    ) -> pd.DataFrame:
        """Return the next trading bars, strictly after the anchor, oldest first."""
        if limit <= 0:
            raise ValueError("'limit' must be greater than zero.")
        return self._fetch_bars(symbol_id, timeframe, after=after, limit=limit)

    def _fetch_bars(
        self,
        symbol_id: int,
        timeframe: str,
        *,
        asof: datetime | None = None,
        start: datetime | None = None,
        after: datetime | None = None,
        descending: bool = False,
        limit: int | None = None,
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
        if after is not None:
            sql += " AND bar_time > %s"
            params.append(after.astimezone(timezone.utc))
        sql += " ORDER BY bar_time DESC" if descending else " ORDER BY bar_time ASC"
        if limit is not None:
            sql += " LIMIT %s"
            params.append(limit)

        with self.get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, params)
                rows = cur.fetchall()

        return self._bar_frame(rows)

    @staticmethod
    def _bar_frame(rows) -> pd.DataFrame:
        if not rows:
            return pd.DataFrame(columns=["timestamp", "open", "high", "low", "close", "volume"])

        df = pd.DataFrame(
            rows,
            columns=["bar_time", "open", "high", "low", "close", "volume"],
        )
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

    def fetch_bars_many(self, symbol_ids, timeframe, asof=None, *, latest=False, after=None, limit=None):
        """Compatible reads; LATERAL limits apply per symbol, never to the batch.

        Full daily history is intentionally retained for indicator initialization.
        Callers chunk those reads; anchors and future windows remain SQL-bounded.
        """
        ids = list(dict.fromkeys(symbol_ids))
        if not ids:
            return {}
        sql = """
            SELECT requested.symbol_id, bars.*
            FROM unnest(%s::bigint[]) WITH ORDINALITY AS requested(symbol_id, position)
        """
        params: list[Any] = [ids]
        if after is not None:
            if limit is None or limit <= 0:
                raise ValueError("'limit' must be greater than zero.")
            sql += " JOIN unnest(%s::timestamptz[]) WITH ORDINALITY AS anchors(bar_time, position) USING (position)"
            params.append([after[sid].astimezone(timezone.utc) for sid in ids])
        sql += " CROSS JOIN LATERAL (SELECT bar_time, open, high, low, close, volume FROM stock_bars WHERE symbol_id = requested.symbol_id AND timeframe = %s"
        params.append(timeframe)
        if asof is not None:
            sql += " AND bar_time <= %s"
            params.append(asof.astimezone(timezone.utc))
        if after is not None:
            sql += " AND bar_time > anchors.bar_time"
        sql += " ORDER BY bar_time DESC" if latest else " ORDER BY bar_time ASC"
        if latest or limit is not None:
            sql += " LIMIT %s"
            params.append(1 if latest else limit)
        sql += ") bars ORDER BY requested.position, bars.bar_time"
        with self.get_conn() as conn, conn.cursor() as cur:
            cur.execute(sql, params)
            rows = cur.fetchall()
        grouped = {sid: [] for sid in ids}
        for row in rows:
            grouped[row[0]].append(row[1:])
        return {sid: self._bar_frame(values) for sid, values in grouped.items()}


class BatchReads:
    """Request-local repository view. Never installed on the shared service.

    Read lazily so a fully warm request only loads contexts. History is fetched
    in groups of ten, while small, bounded anchor/window reads use groups of 100.
    Missing metadata and empty frames are memoized too.
    """

    def __init__(self, repository, contexts):
        self.repository = repository
        self.contexts = contexts
        self.ids = list(dict.fromkeys(c["symbol_id"] for c in contexts.values()))
        self.frames = {}
        self.anchor_asofs = {}

    def fetch_symbol_context(self, symbol):
        return self.contexts.get(str(symbol).strip().upper())

    def _read(self, symbol_id, timeframe, asof, *, latest=False, after=None, limit=None):
        key = (timeframe, asof, latest, limit)
        frames = self.frames.setdefault(key, {})
        if symbol_id not in frames:
            size = 100 if latest or after is not None else 10
            pending = [sid for sid in self.ids if sid not in frames and sid != symbol_id]
            # Start with the requested symbol even when some earlier symbols hit cache.
            # It may also have been evicted from the shared LRU since planning.
            ids = [symbol_id] + pending[:size - 1]
            anchors = None
            if after is not None:
                latest_frames = self.frames[(timeframe, asof, True, None)]
                anchors = {sid: frame.iloc[-1]["timestamp"] for sid, frame in latest_frames.items() if not frame.empty}
                ids = [sid for sid in ids if sid in anchors]
            frames.update(self.repository.fetch_bars_many(ids, timeframe, asof if after is None else None,
                                                        latest=latest, after=anchors, limit=limit))
        return frames[symbol_id]

    def fetch_bars(self, symbol_id, timeframe, asof=None):
        return self._read(symbol_id, timeframe, asof)

    def fetch_latest_bar(self, symbol_id, timeframe, asof=None):
        self.anchor_asofs[symbol_id, timeframe] = asof
        return self._read(symbol_id, timeframe, asof, latest=True)

    def fetch_future_bars(self, symbol_id, timeframe, after, limit):
        # The latest-bar read uses the request's original as-of, not the anchor.
        asof = self.anchor_asofs[symbol_id, timeframe]
        return self._read(symbol_id, timeframe, asof, after=after, limit=limit)
