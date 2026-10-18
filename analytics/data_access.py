from __future__ import annotations

import os
from contextlib import nullcontext
from datetime import datetime, timezone, timedelta
from typing import NamedTuple
from typing import Any

import pandas as pd
from database import get_connection
from . import future_labels
from .state_bars import StateBars
from .quality import OHLCV_SQL_CHECK
from .availability import start_bound
from .read_snapshot import active_read


DATABASE_URL = os.environ.get(
    "DATABASE_URL",
    "postgresql://stock_user:stock_pass@localhost:5432/stock_tracker",
)


class LabelBar(NamedTuple):
    timestamp: datetime
    high: float
    low: float
    close: float
    ohlcv_valid: bool = True

    def __getitem__(self, key):
        return getattr(self, key) if isinstance(key, str) else tuple.__getitem__(self, key)


_EPOCH = datetime(1970, 1, 1, tzinfo=timezone.utc)


class AnalyticsRepository:
    def __init__(self, database_url: str | None = None):
        self.database_url = database_url or DATABASE_URL

    def get_conn(self):
        active = active_read.get()
        if active is not None and active[0] == self.database_url:
            return nullcontext(active[1])
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
                           ss.sync_error,
                           b.revision,
                           b.adjustment_modes,
                           b.adjusted_through,
                           b.checked_at,
                           b.source,
                           b.corporate_actions
                    FROM symbol_metadata sm
                    LEFT JOIN symbol_sync_state ss ON ss.symbol_id = sm.id
                    LEFT JOIN market_data_basis b ON b.symbol_id = sm.id
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
            "adjustment_revision": row[16],
            "adjustment_basis": {"revision": row[16], "modes": row[17],
                                 "adjusted_through": row[18].isoformat(), "checked_at": row[19].isoformat(),
                                 "source": row[20], "policy": "full_retained_history_refresh",
                                 "economic_actions_verified": False,
                                 "corporate_actions": row[21]} if row[16] else None,
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
        """Return at most one completed bar available at the as-of."""
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
            SELECT (EXTRACT(EPOCH FROM bar_time) * 1000000)::bigint AS bar_time_us,
                   open, high, low, close, volume
            FROM stock_bars
            WHERE symbol_id = %s
              AND timeframe = %s
        """
        params: list[Any] = [symbol_id, timeframe]
        if start is not None:
            sql += " AND bar_time >= %s"
            params.append(start.astimezone(timezone.utc))
        if after is None or asof is not None:
            sql += " AND bar_time <= %s"
            params.append(start_bound(asof, timeframe).to_pydatetime())
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

        return self._bar_frame(rows, timestamp_unit="us")

    @staticmethod
    def _bar_frame(rows, timestamp_unit=None) -> pd.DataFrame:
        if not rows:
            return pd.DataFrame(columns=["timestamp", "open", "high", "low", "close", "volume"])

        df = pd.DataFrame(
            rows,
            columns=["bar_time", "open", "high", "low", "close", "volume"],
        )
        timestamps = pd.to_datetime(df["bar_time"], utc=True, cache=False, unit=timestamp_unit)
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

    def fetch_label_bars_many(self, symbol_ids, timeframe, asof=None, *, latest=False, after=None, limit=None):
        """Bounded label-only records; preserve integer UTC microseconds."""
        if not latest and (after is None or limit is None or limit <= 0):
            raise ValueError("Label windows require anchors and a positive limit.")
        return self.fetch_bars_many(symbol_ids, timeframe, asof, latest=latest,
                                    after=after, limit=limit, _labels=True)

    def fetch_historical_price_anchors(self, symbol_id, cutoffs):
        """At most one 15-minute anchor per cutoff; never materialize intraday history."""
        if not cutoffs:
            return {}
        with self.get_conn() as conn, conn.cursor() as cur:
            cur.execute("""
                SELECT requested.position, (EXTRACT(EPOCH FROM bars.bar_time) * 1000000)::bigint,
                       bars.open, bars.high, bars.low, bars.close, bars.volume
                FROM unnest(%s::timestamptz[]) WITH ORDINALITY AS requested(cutoff, position)
                CROSS JOIN LATERAL (
                    SELECT bar_time, open, high, low, close, volume FROM stock_bars
                    WHERE symbol_id = %s AND timeframe = '15Min' AND bar_time <= requested.cutoff
                    ORDER BY bar_time DESC LIMIT 1
                ) bars ORDER BY requested.position
            """, ([start_bound(c, "15Min").to_pydatetime() for c in cutoffs], symbol_id))
            rows = cur.fetchall()
        frame = self._bar_frame([row[1:] for row in rows], timestamp_unit="us")
        return {int(row[0]) - 1: frame.iloc[idx].to_dict() for idx, row in enumerate(rows)}

    def fetch_label_window(self, symbol_id, timeframe, asof, days):
        anchors = self.fetch_label_bars_many([symbol_id], timeframe, asof, latest=True)
        anchor = anchors[symbol_id]
        windows = self.fetch_label_bars_many([symbol_id], timeframe, after={symbol_id: anchor[-1].timestamp}, limit=days) if anchor else {}
        return anchor, windows.get(symbol_id, [])

    def fetch_label_summary(self, symbol_id, timeframe, asof, days, stop, take, side):
        anchors, bars = self.fetch_label_window(symbol_id, timeframe, asof, days)
        summary = future_labels.scan_windows([bars], [anchors[-1].close], stop, take, side)[0] if anchors else None
        return anchors, bars, summary

    def fetch_state_bars(self, symbol_id, timeframe, asof=None, *, latest=False):
        return self.fetch_state_bars_many([symbol_id], timeframe, asof, latest=latest)[symbol_id]

    def fetch_state_bars_many(self, symbol_ids, timeframe, asof=None, *, latest=False):
        return self.fetch_bars_many(symbol_ids, timeframe, asof, latest=latest, _state=True)

    def fetch_bars_many(self, symbol_ids, timeframe, asof=None, *, latest=False, after=None, limit=None, _labels=False, _state=False):
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
        columns = f"high, low, close, COALESCE(({OHLCV_SQL_CHECK}), false) AS ohlcv_valid" if _labels else "open, high, low, close, volume"
        sql += f" CROSS JOIN LATERAL (SELECT (EXTRACT(EPOCH FROM bar_time) * 1000000)::bigint AS bar_time_us, {columns} FROM stock_bars WHERE symbol_id = requested.symbol_id AND timeframe = %s"
        params.append(timeframe)
        if after is None or asof is not None:
            sql += " AND bar_time <= %s"
            params.append(start_bound(asof, timeframe).to_pydatetime())
        if after is not None:
            sql += " AND bar_time > anchors.bar_time"
        sql += " ORDER BY bar_time DESC" if latest else " ORDER BY bar_time ASC"
        if latest or limit is not None:
            sql += " LIMIT %s"
            params.append(1 if latest else limit)
        sql += ") bars ORDER BY requested.position, bars.bar_time_us"
        with self.get_conn() as conn, conn.cursor() as cur:
            cur.execute(sql, params)
            rows = cur.fetchall()
        grouped = {sid: [] for sid in ids}
        for row in rows:
            grouped[row[0]].append(row[1:])
        if _labels:
            return self._label_records(grouped)
        if _state:
            return {sid: StateBars.from_rows(values) for sid, values in grouped.items()}
        return {sid: self._bar_frame(values, timestamp_unit="us") for sid, values in grouped.items()}

    @staticmethod
    def _label_records(grouped):
        return {sid: [LabelBar(_EPOCH + timedelta(microseconds=int(row[0])),
                    *(float(value) if value is not None else float("nan") for value in row[1:4]),
                    bool(row[4]) if len(row) > 4 else True)
                      for row in rows] for sid, rows in grouped.items()}


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
        self.label_windows = {}
        self.label_summaries = {}

    def fetch_label_summary(self, symbol_id, timeframe, asof, days, stop, take, side):
        anchors, bars = self.fetch_label_window(symbol_id, timeframe, asof, days)
        summaries = self.label_summaries.setdefault((timeframe, asof, days, stop, take, side), {})
        if symbol_id not in summaries:
            pending = {sid: pair for sid, pair in self.label_windows[timeframe, asof, days].items() if sid not in summaries}
            valid = {sid: pair for sid, pair in pending.items() if pair[0]}
            values = future_labels.scan_windows([pair[1] for pair in valid.values()],
                [pair[0][-1].close for pair in valid.values()], stop, take, side)
            summaries.update(dict.fromkeys(pending))
            summaries.update(zip(valid, values))
        return anchors, bars, summaries[symbol_id]

    def fetch_label_window(self, symbol_id, timeframe, asof, days):
        windows = self.label_windows.setdefault((timeframe, asof, days), {})
        if symbol_id not in windows:
            ids = [symbol_id] + [sid for sid in self.ids if sid != symbol_id and sid not in windows][:99]
            anchors = self.repository.fetch_label_bars_many(ids, timeframe, asof, latest=True)
            after = {sid: bars[-1].timestamp for sid, bars in anchors.items() if bars}
            futures = self.repository.fetch_label_bars_many(list(after), timeframe, after=after, limit=days)
            windows.update({sid: (anchors[sid], futures.get(sid, [])) for sid in ids})
        return windows[symbol_id]

    def fetch_symbol_context(self, symbol):
        return self.contexts.get(str(symbol).strip().upper())

    def _read(self, symbol_id, timeframe, asof, *, latest=False, after=None, limit=None, state=False):
        key = (timeframe, asof, latest, limit)
        if state:
            key += ("state",)
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
            if state:
                frames.update(self.repository.fetch_state_bars_many(ids, timeframe, asof, latest=latest))
            else:
                frames.update(self.repository.fetch_bars_many(ids, timeframe, asof if after is None else None,
                                                            latest=latest, after=anchors, limit=limit))
        return frames[symbol_id]

    def fetch_state_bars(self, symbol_id, timeframe, asof=None, *, latest=False):
        if not callable(getattr(type(self.repository), "fetch_state_bars_many", None)):
            return self.fetch_latest_bar(symbol_id, timeframe, asof) if latest else self.fetch_bars(symbol_id, timeframe, asof)
        return self._read(symbol_id, timeframe, asof, latest=latest, state=True)

    def fetch_bars(self, symbol_id, timeframe, asof=None):
        return self._read(symbol_id, timeframe, asof)

    def fetch_latest_bar(self, symbol_id, timeframe, asof=None):
        self.anchor_asofs[symbol_id, timeframe] = asof
        return self._read(symbol_id, timeframe, asof, latest=True)

    def fetch_future_bars(self, symbol_id, timeframe, after, limit):
        # The latest-bar read uses the request's original as-of, not the anchor.
        asof = self.anchor_asofs[symbol_id, timeframe]
        return self._read(symbol_id, timeframe, asof, after=after, limit=limit)
