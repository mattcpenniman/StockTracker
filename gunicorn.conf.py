"""Conservative, explicit per-process budgets; never preload application state."""
import os

os.environ.setdefault("DB_POOL_SIZE", "2")


def positive(name: str, default: str):
    value = int(os.environ.get(name, default))
    if value < 1:
        raise ValueError(f"{name} must be positive")
    return value


bind = f"{os.environ.get('HOST', '0.0.0.0')}:{os.environ.get('PORT', '5000')}"
workers = positive("WSGI_WORKERS", "2")
threads = positive("WSGI_THREADS", "2")
worker_class = "gthread"
timeout = positive("WSGI_TIMEOUT", "120")
graceful_timeout = positive("WSGI_GRACEFUL_TIMEOUT", "120")
keepalive = 2
preload_app = False
reload = False
accesslog = None
errorlog = "-"
capture_output = True


def worker_exit(server, worker):
    from database import close_pools
    close_pools()
