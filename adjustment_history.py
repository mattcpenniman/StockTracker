"""Conservative provider-basis refresh: stage every retained timeframe, publish once.

No factors are inferred from price moves. Revision fingerprints provider OHLCV,
configuration, source and reviewed issuer conversions, not all economic actions.
"""
import hashlib
import json
from datetime import datetime, timezone
from corporate_actions import action_manifest, normalize_actions


def refresh_history(conn, symbol_id, symbol, timeframe, start, end, *, fetch,
                    normalize, write, mode, source, now, deltas, finalized_through,
                    fetch_raw=None):
    with conn.cursor() as cur:
        # Serialize all preparations for this symbol across processes/timeframes.
        cur.execute("SELECT pg_advisory_xact_lock(%s)", (symbol_id,))
        cur.execute("""
            SELECT timeframe, min(t), max(t) FROM (
                SELECT timeframe, bar_time AS t FROM stock_bars WHERE symbol_id = %s
                UNION ALL SELECT timeframe, window_start FROM market_data_coverage WHERE symbol_id = %s
                UNION ALL SELECT timeframe, window_end FROM market_data_coverage WHERE symbol_id = %s
            ) bounds GROUP BY timeframe
        """, (symbol_id, symbol_id, symbol_id))
        bounds = {tf: (lo, hi) for tf, lo, hi in cur.fetchall()}
        lo, hi = bounds.get(timeframe, (start, end))
        bounds[timeframe] = (min(lo, start), max(hi, end))
        staged = {}
        checked_at = now()
        modes = {tf: mode(tf) for tf in bounds}
        actions = action_manifest(symbol)
        for tf, (lo, hi) in sorted(bounds.items()):
            bars = fetch(symbol, tf, lo, hi)
            # Validate provider input and raw reference before converting units.
            if symbol == "AZN":
                normalize(symbol_id, tf, bars)
            def raw_reference(sym, frame, left, right):
                if fetch_raw is None:
                    raise ValueError("AZN conversion requires raw provider reference data.")
                raw = fetch_raw(sym, frame, left, right)
                normalize(symbol_id, frame, raw)
                return raw
            bars, evidence = normalize_actions(symbol, tf, bars, modes[tf],
                fetch_raw=raw_reference, start=lo, end=hi)
            if evidence:
                actions["reviewed_actions"].append(evidence)
            rows = normalize(symbol_id, tf, bars)
            times = [row[2] for row in rows]
            if len(set(times)) != len(times) or any(t < lo or t > hi for t in times):
                raise ValueError("Provider refresh returned duplicate or out-of-window bars.")
            # An incomplete but successful response must not silently destroy history.
            cur.execute("SELECT bar_time FROM stock_bars WHERE symbol_id = %s AND timeframe = %s",
                        (symbol_id, tf))
            if not {row[0] for row in cur.fetchall()}.issubset(times):
                raise ValueError(f"Incomplete adjustment refresh for {symbol} {tf}; retained bars are missing.")
            staged[tf] = (bars, rows)
        if now().date() != checked_at.date() or any(mode(tf) != modes[tf] for tf in bounds):
            raise RuntimeError("Adjustment basis changed during preparation; retry.")
        digest = hashlib.sha256(json.dumps({
            "source": source, "modes": modes, "corporate_actions": actions,
            "bars": {tf: sorted(rows, key=lambda row: row[2]) for tf, (_, rows) in sorted(staged.items())},
        }, default=str, sort_keys=True).encode()).hexdigest()
        cur.execute("SELECT revision FROM market_data_basis WHERE symbol_id = %s", (symbol_id,))
        previous = cur.fetchone()
        for tf, (bars, _) in staged.items():
            write(symbol_id, tf, bars, cursor=cur)
        # Old receipts are replaced by receipts for the newly fetched envelopes,
        # including empty windows; no old-basis receipt survives publication.
        cur.execute("DELETE FROM market_data_coverage WHERE symbol_id = %s", (symbol_id,))
        for tf, (lo, hi) in bounds.items():
            step = deltas[tf].total_seconds()
            finalized = datetime.fromtimestamp((int(finalized_through.timestamp() // step) - 1) * step, timezone.utc)
            covered_end = min(hi, finalized)
            if lo <= covered_end:
                cur.execute("""INSERT INTO market_data_coverage
                    (symbol_id, timeframe, window_start, window_end, adjustment_revision, adjustment_mode)
                    VALUES (%s, %s, %s, %s, %s, %s)""",
                    (symbol_id, tf, lo, covered_end, digest, modes[tf]))
        cur.execute("""INSERT INTO market_data_basis
            (symbol_id, revision, source, adjustment_modes, adjusted_through, checked_at, corporate_actions)
            VALUES (%s, %s, %s, %s::jsonb, %s, %s, %s::jsonb)
            ON CONFLICT (symbol_id) DO UPDATE SET revision = EXCLUDED.revision,
                source = EXCLUDED.source, adjustment_modes = EXCLUDED.adjustment_modes,
                adjusted_through = EXCLUDED.adjusted_through, checked_at = EXCLUDED.checked_at,
                corporate_actions = EXCLUDED.corporate_actions""",
            (symbol_id, digest, source, json.dumps(modes), checked_at.date(), checked_at, json.dumps(actions)))
        if previous is None or previous[0] != digest:
            cur.execute("""UPDATE symbol_sync_state SET last_bar_synced_at = clock_timestamp(),
                updated_at = clock_timestamp() WHERE symbol_id = %s""", (symbol_id,))
        return staged[timeframe][0]
