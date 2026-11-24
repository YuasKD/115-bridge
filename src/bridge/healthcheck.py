"""Lightweight container healthcheck: ``python -m bridge.healthcheck``.

Only the standard library plus PyYAML is imported, so it starts in a fraction of the
time needed to import the whole application (p115client, FastAPI, uvicorn, ...).
It must stay free of imports from the rest of the ``bridge`` package.
"""

from __future__ import annotations

import os
import sys
import urllib.request
from collections.abc import Mapping

DEFAULT_PORT = 8080
DEFAULT_CONFIG_PATH = "/config/config.yml"  # 与 bridge.config 保持一致
CONFIG_ENV = "BRIDGE_CONFIG"
TIMEOUT_SECONDS = 5


def read_port(env: Mapping[str, str] | None = None) -> int:
    """Read ``webui.port`` from the config file; fall back to 8080 on any problem."""
    environ = os.environ if env is None else env
    path = environ.get(CONFIG_ENV) or DEFAULT_CONFIG_PATH
    try:
        import yaml

        with open(path, encoding="utf-8") as fh:
            data = yaml.safe_load(fh)
        port = data["webui"]["port"]
    except Exception:  # noqa: BLE001
        return DEFAULT_PORT
    if isinstance(port, bool) or not isinstance(port, int) or not 0 < port < 65536:
        return DEFAULT_PORT
    return port


def check(env: Mapping[str, str] | None = None) -> int:
    """Return 0 if the local ``/healthz`` answers ``ok``, otherwise 1."""
    try:
        port = read_port(env)
        # 访问本机，绝不经过环境里的代理。
        opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
        with opener.open(f"http://127.0.0.1:{port}/healthz", timeout=TIMEOUT_SECONDS) as resp:
            ok = resp.status == 200 and resp.read(16).strip() == b"ok"
        return 0 if ok else 1
    except Exception:  # noqa: BLE001
        return 1


if __name__ == "__main__":
    sys.exit(check())
