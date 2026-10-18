"""Keep contexts, daily/intraday bars and label windows on one MVCC snapshot."""
from contextvars import ContextVar
from functools import wraps
from typing import Any


active_read: ContextVar[Any] = ContextVar("analytics_read_snapshot", default=None)


def coherent_read(method):
    @wraps(method)
    def read(self, *args, **kwargs):
        from .data_access import AnalyticsRepository, BatchReads
        repository = self.repository
        while isinstance(repository, BatchReads):
            repository = repository.repository
        if (active_read.get() is not None or not isinstance(repository, AnalyticsRepository)
                or not isinstance(getattr(repository, "database_url", None), str)):
            return method(self, *args, **kwargs)
        with repository.get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
            token = active_read.set((repository.database_url, conn))
            try:
                return method(self, *args, **kwargs)
            finally:
                active_read.reset(token)
    return read
