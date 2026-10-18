"""Per-worker byte-accounted LRU and bounded, per-key single-flight.

Only immutable JSON bytes are retained. Decoding outside the lock gives each
caller its own payload, including the producer. PostgreSQL and analytics never
run under the cache lock.
"""
from __future__ import annotations

from collections import OrderedDict
from concurrent.futures import Future
import json
import os
import sys
from threading import Lock, get_ident


def _size(value):
    # Conservative: shared key objects are charged on every occurrence.
    return sys.getsizeof(value) + (sum(_size(item) for item in value) if isinstance(value, tuple) else 0)


def _positive_env(name, default):
    raw = os.environ.get(name, str(default))
    try:
        value = int(raw)
    except ValueError:
        raise ValueError(f"{name} must be a positive integer") from None
    if value <= 0:
        raise ValueError(f"{name} must be a positive integer")
    return value


class CacheCapacityError(RuntimeError):
    """Admission rejected rather than growing the flight registry without bound."""


class _StateCache:
    def __init__(self, maxsize=256, maxbytes=8 * 1024 * 1024, max_inflight=64, max_waiters=256):
        if min(maxsize, maxbytes, max_inflight, max_waiters) <= 0:
            raise ValueError("Cache budgets must be positive")
        self.maxsize, self.maxbytes, self.max_inflight = maxsize, maxbytes, max_inflight
        self.max_waiters = max_waiters
        self._data = OrderedDict()
        self._flights = {}
        self._lock = Lock()
        self._bytes = 0
        self._waiters = 0
        self._counts = dict(hits=0, misses=0, evictions=0, coalesced_waits=0,
                            oversized=0, rejected=0, failures=0, computations=0)

    @classmethod
    def configured(cls, name):
        prefix = f"ANALYTICS_{name.upper()}_CACHE_"
        return cls(_positive_env(prefix + "ENTRIES", 256),
                   _positive_env(prefix + "BYTES", 8 * 1024 * 1024),
                   _positive_env(prefix + "INFLIGHT", 64),
                   _positive_env(prefix + "WAITERS", 256))

    def __contains__(self, key):
        # Planning probes neither change recency nor inflate hit/miss counters.
        with self._lock:
            return key in self._data

    def _store(self, key, value):
        # Include conservative mapping/tuple overhead, not just wire length.
        size = _size(key) + sys.getsizeof(value) + 256
        if size > self.maxbytes:
            self._counts["oversized"] += 1
            return
        previous = self._data.pop(key, None)
        if previous is not None:
            self._bytes -= previous[1]
        while self._data and (len(self._data) >= self.maxsize or self._bytes + size > self.maxbytes):
            _, (_, removed_size) = self._data.popitem(last=False)
            self._bytes -= removed_size
            self._counts["evictions"] += 1
        self._data[key] = (value, size)
        self._bytes += size

    def get_or_compute(self, key, compute):
        # Keys come from validated service inputs. Cap even unusually large
        # settings/asof keys before admitting them to the flight registry.
        if _size(key) > 16 * 1024:
            raise CacheCapacityError("Analytics cache key exceeds 16 KiB")
        owner = get_ident()
        value = None
        producer = None
        with self._lock:
            cached = self._data.get(key)
            if cached is not None:
                self._data.move_to_end(key)
                self._counts["hits"] += 1
                value = cached[0]
                flight = None
            else:
                self._counts["misses"] += 1
                existing = self._flights.get(key)
                if existing is not None:
                    flight, producer = existing
                    if producer == owner or any(thread == owner for _, thread in self._flights.values()):
                        # A producer may compute an independent nested key but
                        # may not wait on another producer (A->B/B->A cycle).
                        raise RuntimeError("Recursive analytics cache wait is not allowed")
                    if self._waiters >= self.max_waiters:
                        self._counts["rejected"] += 1
                        raise CacheCapacityError("Analytics cache waiter budget exhausted; retry later")
                    self._waiters += 1
                    self._counts["coalesced_waits"] += 1
                else:
                    if len(self._flights) >= self.max_inflight:
                        self._counts["rejected"] += 1
                        raise CacheCapacityError("Analytics cache in-flight budget exhausted; retry later")
                    flight = Future()
                    self._flights[key] = (flight, owner)
                    self._counts["computations"] += 1
                    producer = owner
        if cached is not None:
            assert value is not None
            return json.loads(value)
        assert flight is not None
        if producer != owner:
            try:
                value = flight.result()
            finally:
                with self._lock:
                    self._waiters -= 1
            return json.loads(value)
        try:
            value = json.dumps(compute(), allow_nan=False, separators=(",", ":")).encode("utf-8")
            with self._lock:
                self._store(key, value)
                del self._flights[key]
                flight.set_result(value)
        except BaseException as exc:
            # Includes cancellation/SystemExit: every waiter is released, and
            # no failed entry survives to poison a later retry.
            with self._lock:
                self._flights.pop(key, None)
                self._counts["failures"] += 1
                if not flight.done():
                    flight.set_exception(exc)
            raise
        return json.loads(value)

    def diagnostics(self):
        with self._lock:
            return {**self._counts, "entries": len(self._data), "current_bytes": self._bytes,
                    "in_flight": len(self._flights), "max_entries": self.maxsize,
                    "waiters": self._waiters, "max_waiters": self.max_waiters,
                    "max_bytes": self.maxbytes, "max_in_flight": self.max_inflight}
