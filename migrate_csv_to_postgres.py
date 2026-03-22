from __future__ import annotations

import csv
import os
import re
from datetime import date

import psycopg2
from psycopg2.extras import execute_values

DATABASE_URL = os.environ.get(
    "DATABASE_URL",
    "postgresql://stock_user:stock_pass@localhost:5432/stock_tracker",
)
DB_TABLE = os.environ.get("STOCK_TRACKER_TABLE", "stock_forecasts")
SOURCE_CSV = os.environ.get("STOCK_TRACKER_CSV", "stocks.csv")
DEFAULT_SCORE = 5.0
DEFAULT_CLASSIFICATION = "hold/watch"

if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", DB_TABLE):
    raise RuntimeError("STOCK_TRACKER_TABLE must be a valid SQL identifier (letters, numbers, underscore).")


def _fnum(value, default):
    if value is None or str(value).strip() == "":
        return float(default)
    try:
        return float(value)
    except Exception:
        return float(default)


def _load_rows(path: str):
    if not os.path.exists(path):
        return []

    rows = []
    with open(path, "r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        for r in reader:
            symbol = str(r.get("symbol", "")).strip().upper()
            if not symbol:
                continue
            try:
                forecast_price = float(r.get("forecast_price"))
            except Exception:
                continue

            updated_date = str(r.get("updated_date") or date.today().isoformat()).strip()
            try:
                date.fromisoformat(updated_date)
            except Exception:
                updated_date = date.today().isoformat()

            reward_score = max(0.0, min(10.0, _fnum(r.get("reward_score"), DEFAULT_SCORE)))
            risk_score = max(0.0, min(10.0, _fnum(r.get("risk_score"), DEFAULT_SCORE)))
            confidence_score = max(0.0, min(10.0, _fnum(r.get("confidence_score"), DEFAULT_SCORE)))

            classification = str(r.get("classification") or DEFAULT_CLASSIFICATION).strip().lower()
            if classification not in {"buy", "hold/watch", "sell"}:
                classification = DEFAULT_CLASSIFICATION

            rows.append(
                (symbol, forecast_price, updated_date, reward_score, risk_score, confidence_score, classification)
            )
    return rows


def main() -> int:
    rows = _load_rows(SOURCE_CSV)
    if not rows:
        print(f"No rows imported. Source CSV not found or empty: {SOURCE_CSV}")
        return 0

    with psycopg2.connect(DATABASE_URL) as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"""
                CREATE TABLE IF NOT EXISTS {DB_TABLE} (
                    symbol TEXT PRIMARY KEY,
                    forecast_price DOUBLE PRECISION NOT NULL,
                    updated_date DATE NOT NULL,
                    reward_score DOUBLE PRECISION NOT NULL DEFAULT 5.0,
                    risk_score DOUBLE PRECISION NOT NULL DEFAULT 5.0,
                    confidence_score DOUBLE PRECISION NOT NULL DEFAULT 5.0,
                    classification TEXT NOT NULL DEFAULT 'hold/watch'
                )
                """
            )
            cur.execute(f"ALTER TABLE {DB_TABLE} ADD COLUMN IF NOT EXISTS reward_score DOUBLE PRECISION NOT NULL DEFAULT 5.0")
            cur.execute(f"ALTER TABLE {DB_TABLE} ADD COLUMN IF NOT EXISTS risk_score DOUBLE PRECISION NOT NULL DEFAULT 5.0")
            cur.execute(
                f"ALTER TABLE {DB_TABLE} ADD COLUMN IF NOT EXISTS confidence_score DOUBLE PRECISION NOT NULL DEFAULT 5.0"
            )
            cur.execute(
                f"ALTER TABLE {DB_TABLE} ADD COLUMN IF NOT EXISTS classification TEXT NOT NULL DEFAULT 'hold/watch'"
            )

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
                rows,
            )

    print(f"Imported {len(rows)} row(s) from {SOURCE_CSV} into {DB_TABLE}.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
