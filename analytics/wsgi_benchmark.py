"""1/2/4-process production WSGI capacity and lifecycle study on stored data.

Requires Docker's local Unix socket, host psycopg2 and a READ-ONLY DATABASE_URL
reachable from the host and --network. Never targets the deployed app container.
Creates disposable containers; use an image built from the intended revision.
"""
from __future__ import annotations

import argparse
from concurrent.futures import ThreadPoolExecutor
from contextlib import closing
import http.client
import json
import os
from pathlib import Path
import socket
import subprocess
from threading import Event, Thread
import time
from urllib.request import urlopen

import psycopg2
from psycopg2.extensions import make_dsn

from .portfolio_load import request, run_stage


HOT_DATES = "2026-03-17,2026-06-16,2026-09-16".split(",")
SWEEP_DATES = "2026-02-17,2026-02-24,2026-03-17,2026-03-24,2026-04-15,2026-04-22,2026-05-15,2026-05-22,2026-06-16,2026-07-15,2026-08-18,2026-09-16".split(",")


def docker(*args):
    return subprocess.check_output(["docker", *args], text=True).strip()


class DockerHTTP(http.client.HTTPConnection):
    def __init__(self):
        super().__init__("localhost", timeout=10)

    def connect(self):
        self.sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        self.sock.settimeout(self.timeout)
        self.sock.connect("/var/run/docker.sock")


def api(path):
    with closing(DockerHTTP()) as client:
        client.request("GET", "/v1.41/" + path)
        response = client.getresponse()
        if response.status != 200:
            raise RuntimeError(f"Docker API {response.status}: {path}")
        return json.loads(response.read())


def health(base):
    with urlopen(base + "/api/health/analytics", timeout=10) as response:
        result = json.load(response)
    assert result["ok"] and result["runtime"]["process"]["startup"]["database"] == "verified", result
    return result


def all_workers(base, workers):
    seen = {}
    deadline = time.monotonic() + 60
    while len(seen) < workers and time.monotonic() < deadline:
        result = health(base)
        seen[str(result["runtime"]["process"]["pid"])] = result
    assert len(seen) == workers, f"Only observed {list(seen)} of {workers} workers"
    return seen


def warm(base, cases, pids):
    """Verify every case reached every serving PID; retain cold-repeat timings."""
    records = []
    for case in cases:
        seen = set()
        for _ in range(500):
            record = request(base, *case, 120)
            records.append(record)
            assert record["valid"], record
            seen.add(record["worker_pid"])
            if seen == set(pids):
                break
        assert seen == set(pids), f"Warmup missed a worker for {case}: {seen}"
    return records


def sessions(conn, name):
    with conn.cursor() as cur:
        # pg_stat_activity can reuse a transaction-scoped statistics snapshot.
        # This also makes repeated polling safe for callers inside `with conn`.
        cur.execute("SELECT pg_stat_clear_snapshot()")
        cur.execute("""SELECT state, count(*), max(extract(epoch FROM clock_timestamp()-xact_start))
                       FROM pg_stat_activity WHERE application_name=%s GROUP BY state""", (name,))
        return {state: {"count": count, "max_transaction_age_seconds": float(age or 0)}
                for state, count, age in cur.fetchall()}


class Monitor:
    def __init__(self, app_id, db_id, dsn, name):
        self.app_id, self.db_id, self.dsn, self.name = app_id, db_id, dsn, name
        self.stop = Event()
        self.samples = []
        self.errors = []
        self.phase = "startup"
        self.thread = Thread(target=self.run, daemon=True)

    def run(self):
        conn = None
        try:
            # Do not wrap this long-lived monitor in `with conn`: psycopg2 2.9
            # starts a transaction there even on an autocommit connection.
            conn = psycopg2.connect(self.dsn, application_name="vec815-monitor")
            conn.autocommit = True
            while not self.stop.is_set():
                sample = {"at": time.time(), "phase": self.phase, "sessions": sessions(conn, self.name)}
                for label, identity in (("app", self.app_id), ("db", self.db_id)):
                    stats = api(f"containers/{identity}/stats?stream=false&one-shot=true")
                    sample[label] = {"cpu_ns": stats["cpu_stats"]["cpu_usage"]["total_usage"],
                                     "memory_bytes": stats["memory_stats"].get("usage", 0)}
                # /top returns host PIDs. RSS includes shared pages in each PID;
                # container memory is retained separately, not summed as PSS.
                top = api(f"containers/{self.app_id}/top?ps_args=-eo%20pid,ppid,rss,args")
                sample["processes"] = top
                self.samples.append(sample)
                self.stop.wait(1)
        except Exception as exc:
            self.errors.append(f"{type(exc).__name__}: {exc}")
        finally:
            if conn is not None:
                conn.close()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--image", required=True)
    parser.add_argument("--network", default="apollo_default")
    parser.add_argument("--db-container", default="stock-tracker-postgres")
    parser.add_argument("--port", type=int, default=5016)
    parser.add_argument("--workers", default="1,2,4")
    parser.add_argument("--threads", type=int, default=2)
    parser.add_argument("--connection-budget", type=int, default=8)
    parser.add_argument("--duration", type=float, default=20)
    parser.add_argument("--soak-seconds", type=float, default=180)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    workers_list = [int(n) for n in args.workers.split(",")]
    if any(n < 1 or args.connection_budget % n for n in workers_list):
        parser.error("Positive worker counts must divide connection-budget")
    if min(args.threads, args.duration, args.soak_seconds) <= 0:
        parser.error("threads and durations must be positive")
    dsn = os.environ["DATABASE_URL"]
    with psycopg2.connect(dsn) as conn, conn.cursor() as cur:
        cur.execute("SHOW default_transaction_read_only")
        assert cur.fetchone()[0] == "on", "DATABASE_URL must enforce read-only transactions"
    base = f"http://127.0.0.1:{args.port}"
    result = {"configuration": {**vars(args), "output": str(args.output)},
              "image": api(f"images/{args.image}/json")["Id"], "runs": []}

    def save():
        args.output.write_text(json.dumps(result, indent=2) + "\n")

    for workers in workers_list:
        name = f"vec815-{workers}"
        run = {"workers": workers, "threads": args.threads,
               "pool_size": args.connection_budget // workers, "stages": []}
        result["runs"].append(run)
        identity = docker("run", "-d", "--name", name, "--network", args.network,
                          "-p", f"127.0.0.1:{args.port}:5000",
                          "-e", "DATABASE_URL=" + make_dsn(dsn, application_name=name),
                          "-e", "WSGI_INIT_DB=0", "-e", "WSGI_APP=analytics.wsgi_load:application",
                          "-e", "ANALYTICS_BACKEND=rust", "-e", f"WSGI_WORKERS={workers}",
                          "-e", f"WSGI_THREADS={args.threads}", "-e", f"DB_POOL_SIZE={run['pool_size']}",
                          args.image)
        monitor = Monitor(identity, args.db_container, dsn, name)
        run["resource_samples"] = monitor.samples
        run["monitor_errors"] = monitor.errors
        try:
            start = time.monotonic()
            deadline = start + 60
            while True:
                try:
                    run["startup_health"] = all_workers(base, workers)
                    break
                except (OSError, KeyError):
                    if time.monotonic() > deadline:
                        raise
                    time.sleep(.2)
            run["startup_seconds"] = time.monotonic() - start
            run["processes_at_start"] = api(f"containers/{identity}/top?ps_args=-eo%20pid,ppid,rss,args")
            monitor.thread.start()
            pids = run["startup_health"]
            hot = [(e, d, 20) for d in HOT_DATES for e in ("state", "futurestate")]
            sweep = [(e, d, 20) for d in SWEEP_DATES for e in ("state", "futurestate")]
            monitor.phase = "hot_warmup"
            run["hot_warmup"] = warm(base, hot, pids)
            run["hot_health"] = all_workers(base, workers)
            workloads = [("hot25", hot, 25, args.duration, 32),
                         ("hot100", hot, 100, args.duration, 32),
                         ("hot250", hot, 250, args.duration, 32),
                         ("recovery25", hot, 25, args.duration, 32)]
            for label, cases, rate, duration, concurrency in workloads:
                monitor.phase = label
                stage = {"label": label, **run_stage(base, cases, rate, duration, concurrency, 120)}
                run["stages"].append(stage)
                print(json.dumps({"workers": workers, "label": label,
                                  **{k: v for k, v in stage.items() if k not in {"records", "by_date", "by_endpoint"}}}), flush=True)
                save()
            monitor.phase = "sweep_warmup"
            run["sweep_warmup"] = warm(base, sweep, pids)
            run["sweep_health"] = all_workers(base, workers)
            for label, cases, rate, duration, concurrency in (
                ("sweep1", sweep, 1, max(30, args.duration), 16),
                ("sweep10", sweep, 10, max(30, args.duration), 16),
                ("soak250", hot, 250, args.soak_seconds, 32),
                ("post_soak25", hot, 25, args.duration, 32),
            ):
                monitor.phase = label
                stage = {"label": label, **run_stage(base, cases, rate, duration, concurrency, 120)}
                run["stages"].append(stage)
                print(json.dumps({"workers": workers, "label": label,
                                  **{k: v for k, v in stage.items() if k not in {"records", "by_date", "by_endpoint"}}}), flush=True)
                save()
            monitor.phase = "idle"
            run["final_health"] = all_workers(base, workers)
            time.sleep(3)
            with psycopg2.connect(dsn) as conn:
                conn.autocommit = True
                run["idle_sessions"] = sessions(conn, name)
                assert set(run["idle_sessions"]) <= {"idle"}, run["idle_sessions"]
            # Exercise TERM with an observed in-flight DB transaction, not a sleep
            # before an unverified request. This test route exists ONLY in wsgi_load.
            monitor.phase = "shutdown"
            with ThreadPoolExecutor(1) as executor:
                future = executor.submit(lambda: json.load(urlopen(base + "/__load/slow", timeout=15)))
                deadline = time.monotonic() + 10
                with psycopg2.connect(dsn) as conn:
                    conn.autocommit = True
                    while time.monotonic() < deadline:
                        with conn.cursor() as cur:
                            cur.execute("SELECT pg_stat_clear_snapshot()")
                            cur.execute("SELECT count(*) FROM pg_stat_activity WHERE application_name=%s AND wait_event='PgSleep'", (name,))
                            if cur.fetchone()[0]:
                                break
                        time.sleep(.01)
                    else:
                        raise AssertionError("In-flight shutdown request not observed")
                before = time.monotonic()
                monitor.stop.set()
                monitor.thread.join(15)
                docker("kill", "--signal=TERM", identity)
                run["inflight_shutdown_response"] = future.result()
                run["exit_code"] = docker("wait", identity)
                run["shutdown_seconds"] = time.monotonic() - before
            monitor.stop.set()
            monitor.thread.join(15)
            with psycopg2.connect(dsn) as conn:
                conn.autocommit = True
                run["sessions_after_shutdown"] = sessions(conn, name)
                assert not run["sessions_after_shutdown"]
            assert run["exit_code"] == "0"
            assert not any(s["summary"]["failed"] for s in run["stages"])
        finally:
            monitor.stop.set()
            if monitor.thread.ident:
                monitor.thread.join(15)
            run["resource_samples"] = monitor.samples
            run["monitor_errors"] = monitor.errors
            run["logs"] = subprocess.check_output(["docker", "logs", identity], text=True, stderr=subprocess.STDOUT)
            save()
            docker("rm", "-f", identity)
        assert not monitor.errors, monitor.errors


if __name__ == "__main__":
    main()
