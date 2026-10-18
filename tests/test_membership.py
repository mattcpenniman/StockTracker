import os
import unittest
from unittest.mock import MagicMock, patch

import psycopg2

import app
from analytics.io_benchmark import price_fixture
from analytics.membership_benchmark import baseline_membership


class MembershipQueryTests(unittest.TestCase):
    def test_one_focused_query_no_dataframe_and_shared_initialization(self):
        connection = MagicMock()
        cur = connection.__enter__.return_value.cursor.return_value.__enter__.return_value
        cur.fetchall.return_value = [(None,), ("",), ("\t",), (" aapl ",), ("AAPL",), ("ß\u00a0",)]
        with patch.object(app, "_init_db_if_needed") as init, \
             patch.object(app, "_get_conn", return_value=connection) as factory, \
             patch.object(app, "_read_df", side_effect=AssertionError("full forecast read")), \
             patch.object(app.pd, "DataFrame", side_effect=AssertionError("DataFrame allocation")):
            for hidden in (False, True):
                cur.execute.reset_mock()
                self.assertEqual(app._portfolio_symbols(hidden), ["AAPL", "SS"])
                cur.execute.assert_called_once()
                self.assertEqual(cur.execute.call_args.args[0],
                                 f"SELECT symbol FROM {app.DB_TABLE}"
                                 + ("" if hidden else " WHERE hidden IS NOT TRUE")
                                 + " ORDER BY symbol ASC")
            self.assertEqual(init.call_count, 2)
            self.assertEqual(factory.call_count, 2)


@unittest.skipUnless(os.environ.get("ANALYTICS_TEST_DATABASE_URL"), "requires PostgreSQL")
class MembershipPostgresTests(unittest.TestCase):
    def test_real_membership_parity_visibility_normalization_and_mutations(self):
        before = baseline_membership()
        with price_fixture(os.environ["ANALYTICS_TEST_DATABASE_URL"], 0, 0) as (dsn, _), \
             patch.object(app, "_get_conn", lambda: psycopg2.connect(dsn)):
            def parity():
                for hidden in (False, True):
                    self.assertEqual(app._portfolio_symbols(hidden), before(hidden))

            parity()  # Empty table.
            with psycopg2.connect(dsn) as conn, conn.cursor() as cur:
                for symbol, hidden in [(" aapl ", False), ("AAPL", True), ("aapl", False),
                                       ("\tMSFT\n", False), ("", False), (" \t", False),
                                       ("ß\u00a0", False), ("SS", False), ("TSLA", True)]:
                    cur.execute("INSERT INTO stock_forecasts (symbol, forecast_price, updated_date, hidden) "
                                "VALUES (%s, 123, '2026-10-18', %s)", (symbol, hidden))
            parity()
            self.assertEqual(set(app._portfolio_symbols()), {"AAPL", "MSFT", "SS"})
            self.assertIn("TSLA", app._portfolio_symbols(True))
            # PK forbids null symbols, but permits distinct raw spellings.
            with self.assertRaises(psycopg2.errors.NotNullViolation):
                with psycopg2.connect(dsn) as conn, conn.cursor() as cur:
                    cur.execute("INSERT INTO stock_forecasts (symbol, forecast_price, updated_date) "
                                "VALUES (NULL, 123, '2026-10-18')")

            # Both real routes must re-read membership after each committed edit.
            with patch.object(app, "analytics_service") as service, app.app.test_client() as client:
                def state(symbols, *args, **kwargs):
                    return {"states": [{"symbol": s} for s in symbols if s != "NEW"],
                            "errors": [{"symbol": "NEW", "error": "missing bars"}] if "NEW" in symbols else []}

                def future(symbols, *args, **kwargs):
                    result = state(symbols)
                    result["future_states"] = result.pop("states")
                    return result

                service.get_batch_state.side_effect = state
                service.get_batch_future_state.side_effect = future

                def check_routes():
                    parity()
                    for suffix in ("", "&include_hidden=1", "&include_hidden=true",
                                   "&include_hidden=yes", "&include_hidden=on", "&include_hidden=0"):
                        expected = before(suffix not in ("", "&include_hidden=0"))
                        for endpoint in ("state", "futurestate"):
                            response = client.get(f"/api/portfolio/{endpoint}?asof=2026-09-16&days=20{suffix}")
                            self.assertEqual(response.status_code, 200)
                            payload = response.get_json()
                            self.assertEqual(payload["portfolio_symbol_count"], len(expected))
                            self.assertEqual(payload["count"], len(expected) - ("NEW" in expected))
                            method = service.get_batch_state if endpoint == "state" else service.get_batch_future_state
                            self.assertEqual(method.call_args.args[0], expected)
                            self.assertEqual(len(payload["errors"]), int("NEW" in expected))

                check_routes()
                app._upsert_stocks([{"symbol": "new", "forecast_price": 150}])
                check_routes()
                app._set_stock_hidden("NEW", True)
                check_routes()
                app._set_stock_hidden("NEW", False)
                check_routes()
                with psycopg2.connect(dsn) as conn, conn.cursor() as cur:
                    cur.execute("DELETE FROM stock_forecasts WHERE symbol = 'NEW'")
                check_routes()
                with psycopg2.connect(dsn) as conn, conn.cursor() as cur:
                    cur.execute("UPDATE stock_forecasts SET hidden = TRUE")
                check_routes()  # All hidden: visible membership is empty.
                with psycopg2.connect(dsn) as conn, conn.cursor() as cur:
                    cur.execute("DELETE FROM stock_forecasts")
                check_routes()
