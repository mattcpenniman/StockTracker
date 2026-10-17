"""Read artifact identity without consulting git, the host, or runtime env overrides."""
from __future__ import annotations

import json
from pathlib import Path

from .indicators import backend_diagnostics


BUILD_INFO_PATH = Path(__file__).resolve().parent.parent / "build-info.json"


def runtime_diagnostics() -> dict:
    try:
        metadata = json.loads(BUILD_INFO_PATH.read_text())
        build = {key: metadata.get(key, "unknown") for key in ("revision", "release_id")}
        build["source"] = "artifact"
    except (OSError, ValueError, AttributeError):
        build = {"revision": "unknown", "release_id": "unknown", "source": "unversioned"}
    return {"build": build, **backend_diagnostics()}
