"""Refresh retained AZN history through the normal atomic publication boundary.

Run after provisioning schema and deploying the conversion-aware ingestion code:
    python -m analytics.repair_corporate_actions

No stored value is multiplied in place. Every run starts with provider history;
source reconciliation failure leaves the previous generation intact.
"""
import json


def repair():
    import app
    from analytics.config import AnalyticsSettings
    from analytics.utils import parse_asof

    with app._get_conn() as conn, conn.cursor() as cur:
        cur.execute("""SELECT sm.id, min(bar_time), max(bar_time)
            FROM symbol_metadata sm JOIN stock_bars b ON b.symbol_id = sm.id
            WHERE sm.symbol = 'AZN' AND b.timeframe = '1Day' GROUP BY sm.id""")
        bounds = cur.fetchone()
    if not bounds:
        raise ValueError("No retained AZN daily history to repair.")
    symbol_id, start, end = bounds
    app._refresh_adjusted_history(symbol_id, "AZN", "1Day", start, end)
    context = app.analytics_service.repository.fetch_symbol_context("AZN")
    result = {"symbol": "AZN", "adjustment_basis": context["adjustment_basis"], "states": {}}
    for cutoff in ("2026-02-03T00:00:00Z", "2026-02-03T23:00:00Z", "2026-09-15T23:00:00Z"):
        state = app.analytics_service.get_symbol_state("AZN", "1Day", parse_asof(cutoff), AnalyticsSettings())
        result["states"][cutoff] = {"returns": state["returns"], "sma_200": state["trend"]["sma_200"],
                                    "data_quality": state["data_quality"]}
    return result


if __name__ == "__main__":
    print(json.dumps(repair(), default=str, sort_keys=True))
