"""Lazy, bounded PostgreSQL pools shared by app and analytics in each process.

DB_POOL_SIZE (default 4) is per database URL *per worker*, not per request.
Batch analytics reads are sequential. Budget workers * pool size against the
server connection limit, including other applications and administrative access.
"""
from __future__ import annotations

import atexit
from contextlib import contextmanager
import os
import threading
import time

import psycopg2


class ConnectionPool:
    def __init__(self, dsn, size=4, timeout=30.0):
        if size < 1 or timeout <= 0:
            raise ValueError("Pool size and checkout timeout must be positive")
        self.dsn, self.size, self.timeout = dsn, size, timeout
        self.condition = threading.Condition()
        self.connections = set()
        self.idle = []
        self.closed = False

    def _discard(self, conn):
        self.connections.discard(conn)
        conn.close()

    def checkout(self):
        deadline = time.monotonic() + self.timeout
        with self.condition:
            while True:
                if self.closed:
                    raise RuntimeError("Database pool is closed")
                if self.idle:
                    conn = self.idle.pop()
                    try:
                        # Detect server-side disconnects before handing a connection
                        # to a caller. Never replay application SQL or writes.
                        with conn.cursor() as cur:
                            cur.execute("SELECT 1")
                        conn.rollback()
                        return conn
                    except (psycopg2.Error, OSError):
                        self._discard(conn)
                elif len(self.connections) < self.size:
                    conn = psycopg2.connect(self.dsn, connect_timeout=max(1, int(self.timeout)))
                    self.connections.add(conn)
                    return conn
                else:
                    remaining = deadline - time.monotonic()
                    if remaining <= 0:
                        raise TimeoutError("Timed out waiting for a PostgreSQL connection")
                    self.condition.wait(remaining)

    def put(self, conn, broken=False):
        with self.condition:
            if self.closed or broken or conn.closed:
                self._discard(conn)
            else:
                self.idle.append(conn)
            self.condition.notify()

    @contextmanager
    def connection(self):
        conn = self.checkout()
        broken = False
        try:
            yield conn
            conn.commit()
        except BaseException as exc:
            broken = isinstance(exc, (psycopg2.OperationalError, psycopg2.InterfaceError))
            try:
                conn.rollback()
            except BaseException:
                broken = True
            raise
        finally:
            # A transport error can leave an apparently open libpq connection.
            broken = broken or bool(conn.closed)
            self.put(conn, broken)

    def close(self):
        """Close idle connections now; checked-out connections close on return."""
        with self.condition:
            self.closed = True
            for conn in self.idle:
                self._discard(conn)
            self.idle.clear()
            self.condition.notify_all()


_lock = threading.RLock()
_pools = {}


def get_connection(dsn):
    with _lock:
        pool = _pools.get(dsn)
        if pool is None:
            pool = ConnectionPool(dsn, int(os.getenv("DB_POOL_SIZE", "4")),
                                  float(os.getenv("DB_POOL_TIMEOUT_SECONDS", "30")))
            _pools[dsn] = pool
    return pool.connection()


def close_pools():
    """Worker shutdown hook; subsequent requests may create fresh pools."""
    with _lock:
        for pool in _pools.values():
            pool.close()
        _pools.clear()


def _before_fork():
    _lock.acquire()
    for pool in _pools.values():
        pool.condition.acquire()


def _after_fork_parent():
    for pool in _pools.values():
        pool.condition.release()
    _lock.release()


def _after_fork_child():
    global _lock, _pools
    for pool in _pools.values():
        # Do not send libpq termination/SSL shutdown over the parent's socket.
        # Close our duplicate descriptor first, then release the libpq object.
        for conn in pool.connections:
            if not conn.closed:
                try:
                    os.close(conn.fileno())
                except OSError:
                    pass
                conn.close()
        pool.connections.clear()
        pool.idle.clear()
        pool.closed = True
        pool.condition = threading.Condition()
    _pools = {}
    _lock = threading.RLock()


os.register_at_fork(before=_before_fork, after_in_parent=_after_fork_parent,
                    after_in_child=_after_fork_child)
atexit.register(close_pools)
