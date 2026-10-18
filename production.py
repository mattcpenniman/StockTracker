"""Production launcher: initialize schema once, then replace this process entirely.

The exec boundary discards initialization-time imports, locks, executors and DB
sockets before Gunicorn forks. WSGI_INIT_DB=0 requires a pre-provisioned schema.
"""
import os
import sys


def main():
    mode = os.environ.get("WSGI_INIT_DB", "1")
    if mode not in {"0", "1"}:
        raise ValueError("WSGI_INIT_DB must be 0 or 1")
    if mode == "1":
        import app
        from database import close_pools
        try:
            app._init_db_if_needed()
        finally:
            close_pools()
    os.execvp(sys.executable, [sys.executable, "-m", "gunicorn", "--config",
                             "gunicorn.conf.py", os.environ.get("WSGI_APP", "wsgi:application")])


if __name__ == "__main__":
    main()
