"""Read artifact identity without consulting git, the host, or runtime env overrides."""
from __future__ import annotations

import json
import os
from pathlib import Path

from .indicators import backend_diagnostics


BUILD_INFO_PATH = Path(__file__).resolve().parent.parent / "build-info.json"
WORKER_STARTUP = None


def runtime_diagnostics() -> dict:
    try:
        metadata = json.loads(BUILD_INFO_PATH.read_text())
        build = {key: metadata.get(key, "unknown") for key in ("revision", "release_id")}
        build["source"] = "artifact"
    except (OSError, ValueError, AttributeError):
        build = {"revision": "unknown", "release_id": "unknown", "source": "unversioned"}
    return {"build": build, **backend_diagnostics(), "process": {
        "scope": "responding_worker_only; not aggregate readiness or counters",
        "pid": os.getpid(), "parent_pid": os.getppid(),
        "startup": WORKER_STARTUP,
        "db_pool_limit_per_dsn": int(os.environ.get("DB_POOL_SIZE", "4")),
    }}
