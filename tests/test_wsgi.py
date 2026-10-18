"""Real WSGI startup/failure/drain checks against a private PostgreSQL schema."""
from concurrent.futures import ThreadPoolExecutor
import json
import os
from pathlib import Path
import signal
import socket
import subprocess
import sys
import tempfile
import time
import unittest
from unittest.mock import patch
from urllib.request import urlopen
from uuid import uuid4

import psycopg2
from psycopg2.extensions import make_dsn

import production


class LauncherTests(unittest.TestCase):
    def test_schema_failure_closes_pools_without_launching(self):
        with patch.dict(os.environ, WSGI_INIT_DB="1"), \
             patch("app._init_db_if_needed", side_effect=RuntimeError("migration failed")), \
             patch("database.close_pools") as close, patch.object(os, "execvp") as execute:
            with self.assertRaisesRegex(RuntimeError, "migration failed"):
                production.main()
            close.assert_called_once()
            execute.assert_not_called()

    def test_preprovisioned_schema_execs_clean_server_process(self):
        with patch.dict(os.environ, WSGI_INIT_DB="0", WSGI_APP="wsgi:application"), \
             patch.object(os, "execvp") as execute:
            production.main()
        self.assertEqual(execute.call_args.args[1], [sys.executable, "-m", "gunicorn", "--config", "gunicorn.conf.py", "wsgi:application"])


@unittest.skipUnless(os.getenv("ANALYTICS_TEST_DATABASE_URL"), "Requires PostgreSQL")
class WSGILifecycleTests(unittest.TestCase):
    def setUp(self):
        self.admin = psycopg2.connect(os.environ["ANALYTICS_TEST_DATABASE_URL"])
        self.admin.autocommit = True
        self.schema = "wsgi_" + uuid4().hex
        with self.admin.cursor() as cur:
            cur.execute(f'CREATE SCHEMA "{self.schema}"')
        self.name = self.schema
        self.dsn = make_dsn(os.environ["ANALYTICS_TEST_DATABASE_URL"],
                            options=f"-csearch_path={self.schema}", application_name=self.name)
        with socket.socket() as sock:
            sock.bind(("127.0.0.1", 0))
            self.port = sock.getsockname()[1]
        self.log = tempfile.TemporaryFile(mode="w+")
        self.process = None
        self.env = {**os.environ, "DATABASE_URL": self.dsn, "HOST": "127.0.0.1", "PORT": str(self.port),
                    "WSGI_WORKERS": "2", "WSGI_THREADS": "2", "DB_POOL_SIZE": "1",
                    "WSGI_INIT_DB": "1", "WSGI_APP": "analytics.wsgi_load:application",
                    "ANALYTICS_BACKEND": "auto"}

    def tearDown(self):
        if self.process and self.process.poll() is None:
            self.process.terminate()
            self.process.wait(timeout=15)
        with self.admin.cursor() as cur:
            cur.execute(f'DROP SCHEMA "{self.schema}" CASCADE')
        self.admin.close()
        self.log.close()

    def launch(self, **env):
        self.process = subprocess.Popen([sys.executable, "-m", "production"], env={**self.env, **env},
                                        cwd=Path(__file__).resolve().parent.parent,
                                        stdout=self.log, stderr=self.log)

    def get(self, path):
        return json.load(urlopen(f"http://127.0.0.1:{self.port}{path}", timeout=10))

    def test_two_worker_startup_budget_and_inflight_sigterm(self):
        self.launch()
        seen = set()
        deadline = time.monotonic() + 30
        while len(seen) < 2 and time.monotonic() < deadline:
            try:
                payload = self.get("/api/health/analytics")
                process = payload["runtime"]["process"]
                self.assertEqual(process["parent_pid"], self.process.pid)
                self.assertEqual(process["startup"]["database"], "verified")
                self.assertTrue(process["startup"]["analytics_smoke"]["finite_output"])
                self.assertEqual(payload["runtime"]["execution"]["native_calls"], 0)
                seen.add(process["pid"])
            except OSError:
                time.sleep(.05)
        self.assertEqual(len(seen), 2)
        with self.admin.cursor() as cur:
            cur.execute("SELECT state, count(*) FROM pg_stat_activity WHERE application_name=%s GROUP BY state", (self.name,))
            self.assertEqual(cur.fetchall(), [("idle", 2)])
        with ThreadPoolExecutor(1) as pool:
            future = pool.submit(self.get, "/__load/slow")
            deadline = time.monotonic() + 10
            while time.monotonic() < deadline:
                with self.admin.cursor() as cur:
                    cur.execute("SELECT count(*) FROM pg_stat_activity WHERE application_name=%s AND wait_event='PgSleep'", (self.name,))
                    if cur.fetchone()[0]:
                        break
                time.sleep(.01)
            else:
                self.fail("No in-flight request observed")
            self.process.send_signal(signal.SIGTERM)
            self.assertEqual(future.result(timeout=10), {"ok": True})
        self.assertEqual(self.process.wait(timeout=15), 0)
        with self.admin.cursor() as cur:
            cur.execute("SELECT count(*) FROM pg_stat_activity WHERE application_name=%s", (self.name,))
            self.assertEqual(cur.fetchone()[0], 0)

    def test_missing_schema_aborts_worker_boot(self):
        self.launch(WSGI_INIT_DB="0")
        self.assertNotEqual(self.process.wait(timeout=30), 0)
        self.log.seek(0)
        self.assertIn("does not exist", self.log.read())

    def test_invalid_backend_aborts_worker_boot(self):
        self.launch(ANALYTICS_BACKEND="invalid")
        self.assertNotEqual(self.process.wait(timeout=30), 0)
        self.log.seek(0)
        self.assertIn("Worker analytics startup failed", self.log.read())
