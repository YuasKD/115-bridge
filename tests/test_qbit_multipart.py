"""T07 返工：按 Radarr `HttpRequestBuilder` 的实际格式逐字节构造 multipart 请求体。

Radarr/Sonarr 不用 httpx 那套编码：boundary 为 `-----` 加 14 位十六进制，每个部分都带
`Content-Type: application/octet-stream`，文件名原样写入（UTF-8，不做 RFC 5987 编码）。
"""

from __future__ import annotations

import secrets
from collections.abc import AsyncIterator
from dataclasses import dataclass
from pathlib import Path

import httpx
import pytest

from bridge.api.qbit import create_qbit_app
from bridge.config import Secrets, Settings, WebUIConfig
from bridge.db.repo import Source, TaskState
from bridge.db.sqlite import SqliteTaskRepo
from bridge.testing.clock import FakeClock
from bridge.torrent import build_magnet, parse_torrent

FIXTURES = Path(__file__).parent / "fixtures" / "torrents"
PASSWORD = "s3cret-pass"
H1 = "a" * 40


class NullCleaner:
    async def cancel_and_cleanup_incomplete(self, info_hash: str) -> None:
        return None

    async def remove_completed(self, task: object) -> None:
        return None


@dataclass
class Env:
    client: httpx.AsyncClient
    repo: SqliteTaskRepo


@pytest.fixture
async def env() -> AsyncIterator[Env]:
    repo = await SqliteTaskRepo.open(":memory:")
    app = create_qbit_app(
        repo=repo,
        settings=Settings(webui=WebUIConfig(username="admin")),
        secrets=Secrets(webui_password=PASSWORD),
        clock=FakeClock(),
        cleaner=NullCleaner(),  # type: ignore[arg-type]
    )
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://t/api/v2"
    ) as client:
        r = await client.post("/auth/login", data={"username": "admin", "password": PASSWORD})
        assert r.text == "Ok."
        yield Env(client, repo)
    await repo.close()


def radarr_boundary() -> str:
    return "-----" + secrets.token_hex(7)  # 14 位十六进制


def radarr_body(boundary: str, parts: list[tuple[str, str | None, bytes]]) -> bytes:
    """parts: (字段名, 文件名或 None, 内容)。"""
    out = b""
    for name, filename, data in parts:
        out += f"--{boundary}\r\n".encode()
        disp = f'Content-Disposition: form-data; name="{name}"'
        if filename is not None:
            disp += f'; filename="{filename}"'
        out += disp.encode("utf-8") + b"\r\n"
        out += b"Content-Type: application/octet-stream\r\n\r\n"
        out += data + b"\r\n"
    out += f"--{boundary}--\r\n".encode()
    return out


async def post_add(
    env: Env, parts: list[tuple[str, str | None, bytes]], *, quoted: bool = False
) -> httpx.Response:
    b = radarr_boundary()
    assert len(b) == 5 + 14 and all(c in "0123456789abcdef" for c in b[5:])
    token = f'"{b}"' if quoted else b
    return await env.client.post(
        "/torrents/add",
        content=radarr_body(b, parts),
        headers={"Content-Type": f"multipart/form-data; boundary={token}"},
    )


@pytest.mark.parametrize("quoted", [False, True])
@pytest.mark.parametrize(
    "fname",
    [
        "big-buck-bunny.torrent",
        "sintel.torrent",
        "ubuntu-24.04.3-live-server-amd64.iso.torrent",
    ],
)
async def test_real_torrent_bytes_and_btih_exact(env: Env, fname: str, quoted: bool) -> None:
    data = (FIXTURES / fname).read_bytes()
    pt = parse_torrent(data)
    r = await post_add(
        env,
        [("torrents", "My.Movie.2024.torrent", data), ("category", None, b"radarr")],
        quoted=quoted,
    )
    assert r.text == "Ok."
    t = await env.repo.get_task(pt.info_hash)
    assert t is not None
    assert t.state is TaskState.QUEUED and t.source is Source.TORRENT
    assert t.category == "radarr"
    assert await env.repo.get_manifest(pt.info_hash) == list(pt.files)


@pytest.mark.parametrize(
    "filename",
    [
        "电影.2024.1080p.torrent",
        "a;b.torrent",
        "测试;文件; name.torrent",
        "x.torrent",
    ],
)
async def test_chinese_and_semicolon_filenames(env: Env, filename: str) -> None:
    data = (FIXTURES / "big-buck-bunny.torrent").read_bytes()
    pt = parse_torrent(data)
    r = await post_add(env, [("torrents", filename, data), ("category", None, b"radarr")])
    assert r.text == "Ok."
    assert await env.repo.get_task(pt.info_hash) is not None


async def test_long_magnet_in_field_without_filename(env: Env) -> None:
    trackers = "".join(
        f"&tr=udp%3A%2F%2Ftracker{i}.example.org%3A6969%2Fannounce" for i in range(30)
    )
    magnet = build_magnet(H1, "Long.Name") + trackers
    assert len(magnet.encode()) > 1024
    r = await post_add(env, [("urls", None, magnet.encode()), ("category", None, b"radarr")])
    assert r.text == "Ok."
    t = await env.repo.get_task(H1)
    assert t is not None and t.source is Source.MAGNET and t.name == "Long.Name"


async def test_magnet_with_chinese_name_and_other_fields(env: Env) -> None:
    magnet = build_magnet(H1, "星际穿越.Interstellar.2014")
    r = await post_add(
        env,
        [
            ("urls", None, magnet.encode()),
            ("category", None, b"radarr"),
            ("paused", None, b"false"),
            ("sequentialDownload", None, b"false"),
        ],
    )
    assert r.text == "Ok."
    t = await env.repo.get_task(H1)
    assert t is not None and t.name == "星际穿越.Interstellar.2014"


async def test_torrent_part_after_plain_field(env: Env) -> None:
    """文件部分在普通字段之后，二进制内容仍须逐字节一致。"""
    data = (FIXTURES / "sintel.torrent").read_bytes()
    pt = parse_torrent(data)
    r = await post_add(env, [("category", None, b"radarr"), ("torrents", "x.torrent", data)])
    assert r.text == "Ok."
    assert await env.repo.get_manifest(pt.info_hash) == list(pt.files)
