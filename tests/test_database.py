from concurrent.futures import ThreadPoolExecutor
import multiprocessing
import os
import unittest
from unittest.mock import MagicMock, patch

import psycopg2
from psycopg2.extensions import TRANSACTION_STATUS_IDLE

import database


def fork_probe(dsn, output):
    with database.get_connection(dsn) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT pg_backend_pid()")
            output.send(cur.fetchone()[0])
    database.close_pools()


class PoolTests(unittest.TestCase):
    def setUp(self):
        self.pool = database.ConnectionPool("test", size=1, timeout=0.02)
        self.addCleanup(self.pool.close)
        self.conn = MagicMock(closed=0)
        self.connect = patch.object(database.psycopg2, "connect", return_value=self.conn).start()
        self.addCleanup(patch.stopall)

    def test_reuse_commit_rollback_and_bounded_wait(self):
        with self.pool.connection() as conn:
            self.assertIs(conn, self.conn)
            with self.assertRaises(TimeoutError):
                self.pool.checkout()
        with self.assertRaisesRegex(ValueError, "caller"):
            with self.pool.connection():
                raise ValueError("caller")
        self.connect.assert_called_once()
        self.conn.commit.assert_called_once()
        self.assertEqual(self.conn.rollback.call_count, 2)  # health check + error
        self.assertEqual(self.pool.idle, [self.conn])

    def test_rollback_failure_discards_and_reconnects(self):
        self.conn.rollback.side_effect = psycopg2.OperationalError("broken")
        with self.assertRaisesRegex(ValueError, "original"):
            with self.pool.connection():
                raise ValueError("original")
        self.conn.close.assert_called_once()
        replacement = MagicMock(closed=0)
        self.connect.return_value = replacement
        with self.pool.connection() as conn:
            self.assertIs(conn, replacement)

    def test_stale_connection_is_replaced_before_caller_runs(self):
        with self.pool.connection():
            pass
        self.conn.cursor.side_effect = psycopg2.OperationalError("server restart")
        replacement = MagicMock(closed=0)
        self.connect.return_value = replacement
        with self.pool.connection() as conn:
            self.assertIs(conn, replacement)
        self.conn.close.assert_called_once()

    def test_connect_and_commit_failures_do_not_exhaust_pool(self):
        self.connect.side_effect = psycopg2.OperationalError("offline")
        with self.assertRaises(psycopg2.OperationalError):
            self.pool.checkout()
        self.assertFalse(self.pool.connections)
        self.connect.side_effect = None
        self.conn.commit.side_effect = psycopg2.OperationalError("commit")
        with self.assertRaises(psycopg2.OperationalError):
            with self.pool.connection():
                pass
        self.conn.rollback.assert_called_once()
        self.conn.close.assert_called_once()

    def test_shutdown_closes_idle_and_checked_out_on_return(self):
        with self.pool.connection():
            self.pool.close()
            self.conn.close.assert_not_called()
        self.conn.close.assert_called_once()
        with self.assertRaises(RuntimeError):
            self.pool.checkout()


@unittest.skipUnless(os.getenv("ANALYTICS_TEST_DATABASE_URL"), "Requires PostgreSQL")
class PostgreSQLPoolTests(unittest.TestCase):
    def setUp(self):
        self.dsn = os.environ["ANALYTICS_TEST_DATABASE_URL"]
        self.pool = database.ConnectionPool(self.dsn, size=2, timeout=5)
        self.addCleanup(self.pool.close)

    def test_concurrent_transactions_rollback_reconnect_and_shutdown(self):
        def request(n):
            try:
                with self.pool.connection() as conn, conn.cursor() as cur:
                    cur.execute("SELECT pg_sleep(0.005)")
                    if n % 3 == 0:
                        cur.execute("SELECT 1/0")
            except psycopg2.errors.DivisionByZero:
                pass
        with ThreadPoolExecutor(max_workers=8) as workers:
            list(workers.map(request, range(80)))
        self.assertEqual(len(self.pool.connections), 2)
        self.assertEqual(len(self.pool.idle), 2)
        self.assertTrue(all(c.get_transaction_status() == TRANSACTION_STATUS_IDLE for c in self.pool.idle))
        dead = self.pool.idle[-1]
        with psycopg2.connect(self.dsn) as admin, admin.cursor() as cur:
            cur.execute("SELECT pg_terminate_backend(%s)", (dead.get_backend_pid(),))
        with self.pool.connection() as conn:
            self.assertIsNot(conn, dead)
        self.assertTrue(dead.closed)
        connections = list(self.pool.connections)
        self.pool.close()
        self.assertTrue(all(c.closed for c in connections))

    def test_app_and_repository_share_pool_and_fork_preserves_parent(self):
        import app
        from analytics.data_access import AnalyticsRepository
        database.close_pools()
        self.addCleanup(database.close_pools)
        with patch.object(app, "DATABASE_URL", self.dsn):
            with app._get_conn() as conn:
                parent_pid = conn.get_backend_pid()
            with AnalyticsRepository(self.dsn).get_conn() as conn:
                self.assertEqual(conn.get_backend_pid(), parent_pid)
            # Exercise both idle and checked-out inherited descriptors. The
            # child must neither reuse nor terminate the parent's transaction.
            for checked_out in (False, True):
                held = database.get_connection(self.dsn) if checked_out else None
                if held:
                    active = held.__enter__()
                    with active.cursor() as cur:
                        cur.execute("SELECT 1")
                try:
                    ctx = multiprocessing.get_context("fork")
                    reader, writer = ctx.Pipe()
                    child = ctx.Process(target=fork_probe, args=(self.dsn, writer))
                    child.start()
                    self.assertTrue(reader.poll(10))
                    self.assertNotEqual(reader.recv(), parent_pid)
                    child.join(10)
                    self.assertEqual(child.exitcode, 0)
                    reader.close()
                    writer.close()
                finally:
                    if held:
                        held.__exit__(None, None, None)
            with app._get_conn() as conn, conn.cursor() as cur:
                cur.execute("SELECT pg_backend_pid()")
                self.assertEqual(cur.fetchone()[0], parent_pid)


if __name__ == "__main__":
    unittest.main()
