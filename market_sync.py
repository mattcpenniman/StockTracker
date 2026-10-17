"""Process-local single-flight coordination for market-data work."""

from concurrent.futures import Future
from threading import Lock


class SyncFlights:
    def __init__(self):
        self._lock = Lock()
        self._pending = {}

    def run(self, key, work):
        with self._lock:
            future = self._pending.get(key)
            owner = future is None
            if owner:
                future = self._pending[key] = Future()
        if not owner:
            return future.result()
        try:
            result = work()
        except BaseException as exc:
            future.set_exception(exc)
            raise
        else:
            future.set_result(result)
            return result
        finally:
            with self._lock:
                del self._pending[key]
