"""Lightweight healthcheck module."""

from __future__ import annotations

import socket
import subprocess
import sys
import threading
from collections.abc import Iterator
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path
from typing import Any

import pytest

from bridge import healthcheck as hc


class _Handler(BaseHTTPRequestHandler):
    body = b"ok"

    def do_GET(self) -> None:  # noqa: N802
        self.send_response(200 if self.path == "/healthz" else 404)
        self.end_headers()
        self.wfile.write(self.body)

    def log_message(self, *args: Any) -> None:
        return None


@pytest.fixture
def server() -> Iterator[HTTPServer]:
    httpd = HTTPServer(("127.0.0.1", 0), _Handler)
    threading.Thread(target=httpd.serve_forever, daemon=True).start()
    yield httpd
    httpd.shutdown()
    httpd.server_close()


def _cfg(tmp_path: Path, port: int) -> dict[str, str]:
    cfg = tmp_path / "config.yml"
    cfg.write_text(f"webui:\n  port: {port}\n", encoding="utf-8")
    return {"BRIDGE_CONFIG": str(cfg)}


def test_no_heavy_imports() -> None:
    code = (
        "import sys, bridge.healthcheck;"
        "bad=[m for m in ('bridge.app','p115client','fastapi','uvicorn','pydantic','bridge.config')"
        " if m in sys.modules];"
        "sys.exit(1 if bad else 0)"
    )
    assert subprocess.run([sys.executable, "-c", code], check=False).returncode == 0


def test_port_from_config(tmp_path: Path) -> None:
    assert hc.read_port(_cfg(tmp_path, 12345)) == 12345


def test_missing_or_bad_config_falls_back(tmp_path: Path) -> None:
    assert hc.read_port({"BRIDGE_CONFIG": str(tmp_path / "absent.yml")}) == 8080
    bad = tmp_path / "bad.yml"
    bad.write_text("webui:\n  port: abc\n", encoding="utf-8")
    assert hc.read_port({"BRIDGE_CONFIG": str(bad)}) == 8080
    bad.write_text("::: not yaml [", encoding="utf-8")
    assert hc.read_port({"BRIDGE_CONFIG": str(bad)}) == 8080


def test_success_with_dead_proxy(
    tmp_path: Path, server: HTTPServer, monkeypatch: pytest.MonkeyPatch
) -> None:
    for var in ("HTTP_PROXY", "http_proxy", "ALL_PROXY", "all_proxy"):
        monkeypatch.setenv(var, "http://127.0.0.1:9")
    monkeypatch.delenv("NO_PROXY", raising=False)
    monkeypatch.delenv("no_proxy", raising=False)
    assert hc.check(_cfg(tmp_path, server.server_address[1])) == 0


def test_wrong_body_fails(
    tmp_path: Path, server: HTTPServer, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(_Handler, "body", b"nope")
    assert hc.check(_cfg(tmp_path, server.server_address[1])) == 1


def test_no_response_returns_1(tmp_path: Path) -> None:
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        port = sock.getsockname()[1]
    assert hc.check(_cfg(tmp_path, port)) == 1
