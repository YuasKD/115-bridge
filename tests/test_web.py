"""T15：Web 页面与扫码测试。内存仓储、假会话、ASGI 传输，不访问网络。"""

from __future__ import annotations

import json
import re
from collections.abc import AsyncIterator
from dataclasses import dataclass, field, replace
from datetime import UTC, datetime, timedelta
from typing import Any

import httpx
import pytest
from fastapi.routing import APIRoute
from pydantic import ValidationError

from bridge.api.qbit import FetchProgress
from bridge.api.web import create_app, create_web_router
from bridge.cloud.protocol import CloudSpace, QrToken, Quota
from bridge.cloud.session import SessionState, SessionStatus
from bridge.config import Secrets, Settings, WebUIConfig
from bridge.db.repo import (
    EVENTS_PER_TASK,
    Fallback,
    ManifestFile,
    Source,
    Stage,
    Task,
    TaskState,
    TaskUpdate,
)
from bridge.db.sqlite import SqliteTaskRepo
from bridge.kv_keys import KV_QUOTA, encode_quota
from bridge.testing.clock import FakeClock

PASSWORD = "s3cret-pass"
FAKE_COOKIE = "UID=FAKECOOKIE123; CID=FAKECID456"
ARR_KEY = "arr-key-XYZ-789"
TG_TOKEN = "123456:TG-TOKEN-ABC"
H1 = "a" * 40
PNG = b"\x89PNG\r\n\x1a\nFAKEQR"
SECRETS_IN_ENV = [PASSWORD, FAKE_COOKIE, "FAKECOOKIE123", ARR_KEY, TG_TOKEN]


@dataclass
class FakeSession:
    status_value: SessionStatus
    qr: QrToken | None = None
    regenerated: int = 0
    fail: bool = False
    qr_reads: int = 0
    _unused: list[int] = field(default_factory=list)

    def status(self) -> SessionStatus:
        return self.status_value

    def current_qr(self) -> QrToken | None:
        self.qr_reads += 1
        return self.qr

    async def regenerate_qr(self) -> QrToken:
        if self.fail:
            raise RuntimeError("boom")
        self.regenerated += 1
        self.qr = QrToken(uid="u2", image_png=PNG)
        return self.qr


class NoCleaner:
    async def cancel_and_cleanup_incomplete(self, info_hash: str) -> None: ...

    async def remove_completed(self, task: Task) -> None: ...


class Progress:
    def progress(self, info_hash: str) -> FetchProgress:
        return FetchProgress(dlspeed=2048, eta=90)


def _status() -> SessionStatus:
    t0 = datetime(2026, 1, 1, tzinfo=UTC)
    return SessionStatus(
        state=SessionState.CIRCUIT_OPEN,
        since=t0,
        last_success=t0 - timedelta(minutes=5),
        circuit_until=t0 + timedelta(minutes=10),
        circuit_level=2,
        login_app="tv",
        cookie_present=True,
        qr_pending=False,
        quota=Quota(5, 100),
        degraded_since=None,
        detail="risk control",
        space=CloudSpace(total=2 * 1024**4, remaining=1536 * 1024**3),
    )


@dataclass
class Env:
    client: httpx.AsyncClient
    anon: httpx.AsyncClient
    repo: SqliteTaskRepo
    session: FakeSession
    clock: FakeClock
    app_routes: list[APIRoute]


@pytest.fixture
async def env() -> AsyncIterator[Env]:
    repo = await SqliteTaskRepo.open(":memory:")
    clock = FakeClock()
    session = FakeSession(_status())
    app = create_app(
        repo=repo,
        settings=Settings(webui=WebUIConfig(username="admin")),
        secrets=Secrets(webui_password=PASSWORD),
        clock=clock,
        cleaner=NoCleaner(),
        session=session,
        progress_source=Progress(),
        free_space=lambda path: 7 * 1024**3,
    )
    transport = httpx.ASGITransport(app=app)
    async with (
        httpx.AsyncClient(transport=transport, base_url="http://t") as client,
        httpx.AsyncClient(transport=transport, base_url="http://t") as anon,
    ):
        r = await client.post(
            "/api/v2/auth/login", data={"username": "admin", "password": PASSWORD}
        )
        assert r.text == "Ok."
        # 新版 FastAPI 的 app.routes 把 include 进来的路由包成 _IncludedRouter，
        # 所以对单独构造的 web 路由器遍历路由
        web_router = create_web_router(
            repo=repo,
            settings=Settings(),
            clock=clock,
            session=session,
            sessions={},
        )
        routes = [r for r in web_router.routes if isinstance(r, APIRoute)]
        yield Env(client, anon, repo, session, clock, routes)
    await repo.close()


async def mk(env: Env, name: str = "Movie", state: TaskState = TaskState.QUEUED) -> None:
    await env.repo.create_task(H1, "radarr", name, Source.MAGNET, env.clock.now(), "")
    if state is TaskState.FAILED:
        await env.repo.transition(H1, TaskState.FAILED, "t", env.clock.now())


# ---------------------------------------------------------------- 认证


async def test_unauthenticated_rejected(env: Env) -> None:
    r = await env.anon.get("/")
    assert r.status_code == 200 and "<form" in r.text and "Movie" not in r.text
    assert env.session.qr_reads == 0
    assert (await env.anon.get("/web/status.json")).status_code == 403
    assert (await env.anon.get("/web/qr.png")).status_code == 403
    assert (await env.anon.post("/web/qr/regenerate")).status_code == 403
    assert env.session.regenerated == 0


async def test_wrong_password_not_authenticated(env: Env) -> None:
    r = await env.anon.post("/api/v2/auth/login", data={"username": "admin", "password": "bad"})
    assert r.text == "Fails."
    assert (await env.anon.get("/web/status.json")).status_code == 403


# ---------------------------------------------------------------- 内容


async def test_status_json_contents(env: Env) -> None:
    await mk(env)
    await env.repo.incr_retry(H1, Stage.FETCH)
    await env.repo.kv_set(KV_QUOTA, encode_quota(Quota(42, 100), env.clock.now()))
    d = (await env.client.get("/web/status.json")).json()
    s = d["session"]
    assert s["state"] == "circuit_open" and s["login_app"] == "tv"
    assert s["cookie_present"] is True and s["circuit_level"] == 2
    assert s["circuit_until"] and s["last_success"] and s["since"]
    assert d["quota"]["remaining"] == 42 and d["quota"]["total"] == 100
    assert d["quota"]["read_at"]
    assert d["local_free_bytes"] == 7 * 1024**3
    assert d["cloud_space"] == {"total": 2 * 1024**4, "remaining": 1536 * 1024**3}
    (t,) = d["tasks"]
    assert t["name"] == "Movie" and t["category"] == "radarr" and t["state"] == "queued"
    assert t["retries"] == {"fetch": 1} and t["fallback"] == "none"


async def test_quota_falls_back_to_session_status(env: Env) -> None:
    d = (await env.client.get("/web/status.json")).json()
    assert d["quota"]["remaining"] == 5 and d["quota"]["read_at"] is None


async def test_cloud_space_unknown_when_not_read(env: Env) -> None:
    env.session.status_value = replace(env.session.status_value, space=None)
    assert (await env.client.get("/web/status.json")).json()["cloud_space"] is None
    page = (await env.client.get("/")).text
    assert "115 网盘剩余空间" in page and "未知" in page


async def test_free_space_error_is_none() -> None:
    repo = await SqliteTaskRepo.open(":memory:")

    def boom(path: str) -> int:
        raise FileNotFoundError(path)

    app = create_app(
        repo=repo,
        settings=Settings(webui=WebUIConfig(username="admin")),
        secrets=Secrets(webui_password=PASSWORD),
        clock=FakeClock(),
        cleaner=NoCleaner(),
        session=FakeSession(_status()),
        free_space=boom,
    )
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://t") as c:
        await c.post("/api/v2/auth/login", data={"username": "admin", "password": PASSWORD})
        assert (await c.get("/web/status.json")).json()["local_free_bytes"] is None
        assert (await c.get("/")).status_code == 200
    await repo.close()


async def test_page_shows_everything(env: Env) -> None:
    await mk(env, state=TaskState.FAILED)
    await env.repo.kv_set(KV_QUOTA, encode_quota(Quota(42, 100), env.clock.now()))
    r = await env.client.get("/")
    assert r.status_code == 200
    for needle in (
        "circuit_open",
        "tv",
        "42 / 100",
        "7.00 GiB",
        "115 网盘剩余空间",
        "1.50 TiB",
        "2.00 TiB",
        "Movie",
        "radarr",
        "failed",
    ):
        assert needle in r.text
    assert "重新生成二维码" in r.text


async def test_qr_present_and_absent(env: Env) -> None:
    r = await env.client.get("/")
    assert "/web/qr.png" not in r.text
    assert (await env.client.get("/web/qr.png")).status_code == 404
    env.session.qr = QrToken(uid="u1", image_png=PNG)
    r = await env.client.get("/")
    assert '<img src="/web/qr.png"' in r.text
    png = await env.client.get("/web/qr.png")
    assert png.status_code == 200 and png.content == PNG
    assert png.headers["content-type"] == "image/png"


async def test_regenerate_calls_session(env: Env) -> None:
    r = await env.client.post("/web/qr/regenerate")
    assert r.status_code == 200 and r.json() == {"ok": True}
    assert env.session.regenerated == 1
    assert (await env.client.get("/web/qr.png")).content == PNG


async def test_regenerate_failure_does_not_leak(env: Env) -> None:
    env.session.fail = True
    r = await env.client.post("/web/qr/regenerate")
    assert r.status_code == 502 and "boom" not in r.text


# ---------------------------------------------------------------- 安全


async def test_no_secrets_in_responses(env: Env) -> None:
    await mk(env)
    await seed_failed(env.repo, env.clock, H2, "Failed Movie")
    await seed_completed_removed(env.repo, env.clock, H3, "Done Movie")
    env.session.qr = QrToken(uid="u1", image_png=PNG)
    bodies = [
        (await env.client.get("/")).text,
        (await env.client.get("/web/status.json")).text,
        (await env.anon.get("/")).text,
    ]
    for body in bodies:
        for secret in SECRETS_IN_ENV:
            assert secret not in body
    for k in ("cookie", "password", "token", "api_key"):
        assert k not in (await env.client.get("/web/status.json")).text.replace(
            "cookie_present", ""
        )


async def test_task_name_is_escaped(env: Env) -> None:
    evil = '<script>alert("x")</script>&<b>'
    await mk(env, name=evil)
    page = (await env.client.get("/")).text
    assert evil not in page
    assert "&lt;script&gt;alert(&quot;x&quot;)&lt;/script&gt;&amp;&lt;b&gt;" in page


async def test_no_task_mutating_routes(env: Env) -> None:
    routes = {(r.path, m) for r in env.app_routes for m in r.methods if m != "HEAD"}
    assert routes == {
        ("/", "GET"),
        ("/web/status.json", "GET"),
        ("/web/qr.png", "GET"),
        ("/web/qr/regenerate", "POST"),  # 唯一的写操作，作用于会话而非任务
    }
    for r in env.app_routes:
        assert "task" not in r.path and "torrent" not in r.path


async def test_qbit_api_still_works_on_same_app(env: Env) -> None:
    r = await env.client.get("/api/v2/app/version")
    assert r.status_code == 200
    assert (await env.anon.get("/api/v2/app/version")).status_code == 403


class FakeStall:
    def __init__(self, reasons: dict[str, str]) -> None:
        self.reasons = reasons

    def stall_reason(self, info_hash: str) -> str | None:
        return self.reasons.get(info_hash)


async def test_stall_reason_column_shown_and_escaped() -> None:
    repo = await SqliteTaskRepo.open(":memory:")
    clock = FakeClock()
    await repo.create_task(H1, "radarr", "Movie", Source.MAGNET, clock.now(), "")
    app = create_app(
        repo=repo,
        settings=Settings(webui=WebUIConfig(username="admin")),
        secrets=Secrets(webui_password=PASSWORD),
        clock=clock,
        cleaner=NoCleaner(),
        session=FakeSession(_status()),
        stall_view=FakeStall({H1: "target exists <b>x</b>"}),
    )
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://t") as c:
        await c.post("/api/v2/auth/login", data={"username": "admin", "password": PASSWORD})
        [t] = (await c.get("/web/status.json")).json()["tasks"]
        assert t["stall_reason"] == "target exists <b>x</b>"
        assert t["error"] is None  # 不写进 error
        page = (await c.get("/")).text
        assert "停住原因" in page
        assert "target exists &lt;b&gt;x&lt;/b&gt;" in page
        assert "<b>x</b>" not in page
    await repo.close()


async def test_stall_reason_absent_without_view(env: Env) -> None:
    await mk(env)
    [t] = (await env.client.get("/web/status.json")).json()["tasks"]
    assert t["stall_reason"] is None


# ---------------------------------------------------------------- missingFiles


class _Missing:
    def __init__(self, marks: dict[str, int | None]) -> None:
        self.marks = marks

    def might_be_missing(self, info_hash: str) -> bool:
        return info_hash in self.marks

    def is_missing(self, info_hash: str, completion_id: int | None) -> bool:
        return self.marks.get(info_hash, -1) == completion_id


async def _web_client(repo: SqliteTaskRepo, clock: FakeClock, view: _Missing) -> httpx.AsyncClient:
    app = create_app(
        repo=repo,
        settings=Settings(webui=WebUIConfig(username="admin")),
        secrets=Secrets(webui_password=PASSWORD),
        clock=clock,
        cleaner=NoCleaner(),
        session=FakeSession(_status()),
        free_space=lambda path: 1,
        missing_files=view,
    )
    client = httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://t")
    r = await client.post("/api/v2/auth/login", data={"username": "admin", "password": PASSWORD})
    assert r.text == "Ok."
    return client


async def test_web_shows_missing_files(env: Env) -> None:
    from bridge.missing_protocol import completion_id

    await mk(env)
    for s in (
        TaskState.SUBMITTING,
        TaskState.CLOUD_METADATA,
        TaskState.CLOUD_DOWNLOADING,
        TaskState.CLOUD_DONE,
        TaskState.LOCATING,
        TaskState.FETCHING,
        TaskState.VERIFYING,
        TaskState.COMPLETED,
    ):
        await env.repo.transition(H1, s, "t", env.clock.now())
    cid = await completion_id(env.repo, H1)
    assert cid is not None

    client = await _web_client(env.repo, env.clock, _Missing({H1: cid}))
    try:
        page = await client.get("/")
        data = (await client.get("/web/status.json")).json()
    finally:
        await client.aclose()
    assert "本地文件缺失" in page.text
    assert data["tasks"][0]["missing_files"] is True

    client = await _web_client(env.repo, env.clock, _Missing({H1: cid + 1}))  # 已不是同一次完成
    try:
        page = await client.get("/")
        data = (await client.get("/web/status.json")).json()
    finally:
        await client.aclose()
    assert "本地文件缺失" not in page.text
    assert data["tasks"][0]["missing_files"] is False


async def test_web_unmarked_completed_does_not_query_events(
    env: Env, monkeypatch: pytest.MonkeyPatch
) -> None:
    await mk(env)
    for s in (
        TaskState.SUBMITTING,
        TaskState.CLOUD_METADATA,
        TaskState.CLOUD_DOWNLOADING,
        TaskState.CLOUD_DONE,
        TaskState.LOCATING,
        TaskState.FETCHING,
        TaskState.VERIFYING,
        TaskState.COMPLETED,
    ):
        await env.repo.transition(H1, s, "t", env.clock.now())
    calls: list[str] = []
    real = env.repo.list_events

    async def spy(info_hash: str, limit: int = 50) -> Any:
        calls.append(info_hash)
        return await real(info_hash, limit)

    monkeypatch.setattr(env.repo, "list_events", spy)
    client = await _web_client(env.repo, env.clock, _Missing({}))
    try:
        data = (await client.get("/web/status.json")).json()
    finally:
        await client.aclose()
    assert data["tasks"][0]["missing_files"] is False
    assert calls == []


# ---------------------------------------------------------------- 重设计 UI


def _embedded(page: str, tag_id: str) -> Any:
    m = re.search(
        r'<script type="application/json" id="' + tag_id + r'">(.*?)</script>', page, re.S
    )
    assert m is not None
    return json.loads(m.group(1))


async def test_initial_embedded_data_matches_status_json(env: Env) -> None:
    await mk(env)
    page = (await env.client.get("/")).text
    live = (await env.client.get("/web/status.json")).json()
    assert _embedded(page, "ui-data") == live
    const = _embedded(page, "ui-const")
    assert const["poll_seconds"] == 5
    assert const["state_labels"]["fetching"] == ["取回中", "badge purple"]


async def test_page_polls_instead_of_meta_refresh(env: Env) -> None:
    page = (await env.client.get("/")).text
    assert 'http-equiv="refresh"' not in page
    assert 'id="row-tpl"' in page and 'data-field="task.name"' in page
    assert 'id="ui-const"' in page and 'id="ui-data"' in page
    assert "/web/status.json" in page and "bridge-theme" in page
    assert "prefers-color-scheme" in page and "localStorage" in page


async def test_row_skeleton_matches_server_rows(env: Env) -> None:
    """服务端首屏行与 JS 克隆骨架的 data-field 集合必须一致，防止两边漂移。"""
    await mk(env)
    await seed_failed(env.repo, env.clock, H2, "Failed Movie")
    await seed_completed_removed(env.repo, env.clock, H3, "Done Movie")
    page = (await env.client.get("/")).text
    tpl = page.split('<template id="row-tpl">')[1].split("</template>")[0]
    body = page.split('<tbody id="taskBody">')[1].split("</tbody>")[0]
    first_task = "</tr>".join(body.split("</tr>")[:2])

    def fields(s: str) -> set[str]:
        return set(re.findall(r'data-field="([^"]+)"', s))

    def class_fields(s: str) -> set[str]:
        return set(re.findall(r'data-class-field="([^"]+)"', s))

    def title_fields(s: str) -> set[str]:
        return set(re.findall(r'data-title-field="([^"]+)"', s))

    def raw_title_fields(s: str) -> set[str]:
        return set(re.findall(r'data-raw-title-field="([^"]+)"', s))

    assert fields(tpl) == fields(first_task)
    assert class_fields(tpl) == class_fields(first_task)
    assert title_fields(tpl) == title_fields(first_task)
    assert raw_title_fields(tpl) == raw_title_fields(first_task)

    # T19：历史行骨架同样纳入防漂移断言
    for tpl_id, body_id in (("failed-row-tpl", "failedBody"), ("done-row-tpl", "doneBody")):
        hist_tpl = page.split(f'<template id="{tpl_id}">')[1].split("</template>")[0]
        hist_body = page.split(f'<tbody id="{body_id}">')[1].split("</tbody>")[0]
        first_hist_row = hist_body.split("</tr>")[0]
        assert fields(hist_tpl) == fields(first_hist_row)
        assert class_fields(hist_tpl) == class_fields(first_hist_row)
        assert title_fields(hist_tpl) == title_fields(first_hist_row)
        assert raw_title_fields(hist_tpl) == raw_title_fields(first_hist_row)


async def test_qr_panel_hidden_attribute(env: Env) -> None:
    page = (await env.client.get("/")).text
    assert 'id="qrPanel" hidden' in page
    env.session.qr = QrToken(uid="u1", image_png=PNG)
    page = (await env.client.get("/")).text
    assert 'id="qrPanel" hidden' not in page
    assert 'id="qrPanel"' in page


async def test_qr_img_src_refreshed_with_cache_bust(env: Env) -> None:
    """T21：qr_pending 期间每次轮询都刷新 img.src（cache-bust），重新生成与自动换码都能显示新码。"""
    page = (await env.client.get("/")).text
    m = re.search(
        r"if\(d\.session\.qr_pending\)\{(.*?)\}else if\(img\)\{img\.remove\(\);\}", page, re.S
    )
    assert m is not None  # 非 pending 时移除 img 的分支仍在
    block = m.group(1)
    assert "createElement" in block and "panel.insertBefore(img,panel.firstChild)" in block
    # src 刷新在创建分支之外：每次 apply 都执行，而非仅创建时
    assert '}\nimg.src="/web/qr"+".png?t="+Date.now();' in block


async def test_embedded_json_escapes_script_close(env: Env) -> None:
    evil = '</script><script>alert("x")</script>'
    await mk(env, name=evil)
    page = (await env.client.get("/")).text
    assert evil not in page
    assert _embedded(page, "ui-data")["tasks"][0]["name"] == evil


async def test_login_page_redesigned(env: Env) -> None:
    page = (await env.anon.get("/")).text
    assert '<form class="login"' in page
    assert 'autocomplete="current-password"' in page
    assert 'name="username"' in page and 'type="password"' in page
    assert "bridge-theme" in page
    assert "Movie" not in page


async def test_absolute_times_local_in_js_utc_labelled_fallback(env: Env) -> None:
    page = (await env.client.get("/")).text
    # JS 用浏览器本地时区渲染绝对时间与刷新 stamp
    assert "toLocaleTimeString" in page and "toLocaleString" in page
    assert "toISOString().slice" not in page
    # 服务端首屏 fallback 显式标 UTC，不含糊
    assert "UTC</span>" in page


# ---------------------------------------------------------------- T19 任务历史

H2 = "b" * 40
H3 = "c" * 40
H4 = "d" * 40

COMPLETED_PATH = (
    TaskState.SUBMITTING,
    TaskState.CLOUD_METADATA,
    TaskState.CLOUD_DOWNLOADING,
    TaskState.CLOUD_DONE,
    TaskState.LOCATING,
    TaskState.FETCHING,
    TaskState.VERIFYING,
    TaskState.COMPLETED,
)


async def seed_failed(
    repo: SqliteTaskRepo,
    clock: FakeClock,
    info_hash: str,
    name: str,
    *,
    deleted: bool = True,
    error: str | None = "offline timeout",
) -> None:
    """failed（终态）+ 重试/回退/错误，默认已被 Arr 回调删除（client_deleted）。"""
    await repo.create_task(info_hash, "radarr", name, Source.MAGNET, clock.now(), "")
    await repo.transition(info_hash, TaskState.FAILED, "t", clock.now())
    if error is not None:
        await repo.set_error(info_hash, error)
    await repo.incr_retry(info_hash, Stage.FETCH)
    await repo.incr_retry(info_hash, Stage.FETCH)
    await repo.incr_retry(info_hash, Stage.VERIFY)
    await repo.set_fallback(info_hash, Fallback.DONE)
    if deleted:
        await repo.mark_client_deleted(info_hash)


async def seed_completed_removed(
    repo: SqliteTaskRepo,
    clock: FakeClock,
    info_hash: str,
    name: str,
    *,
    cloud_size: int | None = None,
    files: list[ManifestFile] | None = None,
) -> None:
    """走完整链路到 completed，再转 removed（Arr 导入后删除）。"""
    await repo.create_task(info_hash, "radarr", name, Source.MAGNET, clock.now(), "")
    for s in COMPLETED_PATH:
        await repo.transition(info_hash, s, "t", clock.now())
    if files is not None:
        await repo.put_manifest(info_hash, files)
    if cloud_size is not None:
        await repo.update(info_hash, TaskUpdate(cloud_size=cloud_size))
    await repo.transition(info_hash, TaskState.REMOVED, "t", clock.now())


async def test_history_visible_in_json_and_page(env: Env) -> None:
    await seed_failed(env.repo, env.clock, H1, "Failed Movie")
    await seed_completed_removed(env.repo, env.clock, H2, "Done Movie", cloud_size=5 * 1024**3)
    d = (await env.client.get("/web/status.json")).json()
    (f,) = d["failed_history"]
    assert f["name"] == "Failed Movie" and f["category"] == "radarr" and f["hash"] == H1
    assert f["failed_at"] == env.clock.now().isoformat()
    assert f["error"] == "offline timeout"
    assert f["retries"] == {"fetch": 2, "verify": 1} and f["fallback"] == "done"
    (c,) = d["completed_history"]
    assert c["name"] == "Done Movie" and c["category"] == "radarr"
    assert c["completed_at"] == env.clock.now().isoformat()
    assert c["size"] == 5 * 1024**3
    assert c["path"] == "/data/downloads/115/radarr/Done Movie"
    assert d["tasks"] == []  # 历史不进主表

    page = (await env.client.get("/")).text
    assert "失败历史" in page and "成功历史" in page
    assert 'id="failedHistory" hidden' not in page and 'id="doneHistory" hidden' not in page
    assert 'class="badge red" id="failedCount">1<' in page
    assert 'class="badge green" id="doneCount">1<' in page
    assert "保留 7 天 · 超期自动隐藏 · 仅本页可见" in page
    # 原生 details，默认折叠（无 open 属性）
    assert "<details" in page and re.search(r"<details[^>]*\bopen\b", page) is None
    # 移动端卡片变换沿用主表：每个 td 带 data-label
    for label in (
        "失败时间",
        "失败原因",
        "重试 f/v",
        "回退",
        "hash",
        "完成时间",
        "总大小",
        "落盘路径",
    ):
        assert f'data-label="{label}"' in page
    # 原因/路径/hash 截断 + title 全文
    assert ".clip" in page and "text-overflow:ellipsis" in page
    assert 'data-raw-title-field="failed.error_full"' in page
    assert 'data-raw-title-field="done.path"' in page


async def relogin(env: Env) -> None:
    """SID 滑动有效期 3600s：假时钟大幅推进后需要重新登录。"""
    r = await env.client.post(
        "/api/v2/auth/login", data={"username": "admin", "password": PASSWORD}
    )
    assert r.text == "Ok."


async def test_history_fallback_badge_colors(env: Env) -> None:
    """草图：失败历史里回退 done 为绿色徽章，pending 保持琥珀，none 为暗淡占位。"""
    await seed_failed(env.repo, env.clock, H1, "Done Fallback")  # seed 置 Fallback.DONE
    await seed_failed(env.repo, env.clock, H2, "Pending Fallback")
    await env.repo.set_fallback(H2, Fallback.PENDING)
    await env.repo.create_task(H3, "radarr", "No Fallback", Source.MAGNET, env.clock.now(), "")
    await env.repo.transition(H3, TaskState.FAILED, "t", env.clock.now())
    await env.repo.mark_client_deleted(H3)
    page = (await env.client.get("/")).text
    assert 'class="badge green" data-field="failed.fallback_label"' in page
    assert 'class="badge amber" data-field="failed.fallback_label"' in page
    assert 'class="dimtd" data-field="failed.fallback_label"' in page
    # JS 侧同步：done -> badge green
    assert 't.fallback==="done"?["done","badge green"]' in page


async def test_history_sorted_desc(env: Env) -> None:
    await seed_failed(env.repo, env.clock, H1, "Old Fail")
    await env.clock.advance(3600)
    await seed_failed(env.repo, env.clock, H2, "New Fail")
    await seed_completed_removed(env.repo, env.clock, H3, "Old Done")
    await env.clock.advance(3600)
    await seed_completed_removed(env.repo, env.clock, H4, "New Done")
    await relogin(env)
    d = (await env.client.get("/web/status.json")).json()
    assert [t["name"] for t in d["failed_history"]] == ["New Fail", "Old Fail"]
    assert [t["name"] for t in d["completed_history"]] == ["New Done", "Old Done"]


async def test_history_expiry_with_fake_clock(env: Env) -> None:
    await seed_failed(env.repo, env.clock, H1, "Failed Movie")
    await seed_completed_removed(env.repo, env.clock, H2, "Done Movie")
    tasks_before = await env.repo.list_tasks(include_client_deleted=True)
    events_before = await env.repo.list_events(H1, limit=200)

    await env.clock.advance(7 * 86400)  # 恰好到期：<= 保留期，仍可见
    await relogin(env)
    d = (await env.client.get("/web/status.json")).json()
    assert len(d["failed_history"]) == 1 and len(d["completed_history"]) == 1

    await env.clock.advance(1)  # 超期一秒：自动隐藏
    d = (await env.client.get("/web/status.json")).json()
    assert d["failed_history"] == [] and d["completed_history"] == []
    page = (await env.client.get("/")).text
    assert 'id="failedHistory" hidden' in page and 'id="doneHistory" hidden' in page
    assert "Failed Movie" not in page and "Done Movie" not in page

    # 只改可见性：tasks 行与 events 不被删除或修改
    assert await env.repo.list_tasks(include_client_deleted=True) == tasks_before
    assert await env.repo.list_events(H1, limit=200) == events_before


async def test_history_retention_zero_disables_sections() -> None:
    with pytest.raises(ValidationError):
        WebUIConfig(history_retention_days=-1)
    repo = await SqliteTaskRepo.open(":memory:")
    clock = FakeClock()
    await seed_failed(repo, clock, H1, "Failed Movie")
    await seed_completed_removed(repo, clock, H2, "Done Movie")
    app = create_app(
        repo=repo,
        settings=Settings(webui=WebUIConfig(username="admin", history_retention_days=0)),
        secrets=Secrets(webui_password=PASSWORD),
        clock=clock,
        cleaner=NoCleaner(),
        session=FakeSession(_status()),
    )
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://t") as c:
        await c.post("/api/v2/auth/login", data={"username": "admin", "password": PASSWORD})
        d = (await c.get("/web/status.json")).json()
        assert d["failed_history"] == [] and d["completed_history"] == []
        page = (await c.get("/")).text
        assert 'id="failedHistory" hidden' in page and 'id="doneHistory" hidden' in page
        assert "Failed Movie" not in page and "Done Movie" not in page
    await repo.close()


async def test_arr_info_excludes_history(env: Env) -> None:
    await seed_failed(env.repo, env.clock, H1, "Failed Movie")
    await seed_completed_removed(env.repo, env.clock, H2, "Done Movie")
    await env.repo.create_task(H3, "radarr", "Active", Source.MAGNET, env.clock.now(), "")
    info = (await env.client.get("/api/v2/torrents/info")).json()
    assert [t["name"] for t in info] == ["Active"]


async def test_status_json_history_keys_additive(env: Env) -> None:
    await mk(env)
    d = (await env.client.get("/web/status.json")).json()
    assert set(d) == {
        "session",
        "tasks",
        "quota",
        "cloud_space",
        "local_free_bytes",
        "failed_history",
        "completed_history",
    }
    assert d["failed_history"] == [] and d["completed_history"] == []


async def test_history_membership_rules(env: Env) -> None:
    # failed 但未被 Arr 删除：不进失败历史（仍在主表）
    await seed_failed(env.repo, env.clock, H1, "Not Deleted", deleted=False)
    # 在途被删（从未 completed）：不进成功历史
    await env.repo.create_task(H2, "radarr", "Inflight Removed", Source.MAGNET, env.clock.now(), "")
    await env.repo.transition(H2, TaskState.REMOVED, "t", env.clock.now())
    # completed 但未 removed：不进成功历史（仍在主表）
    await env.repo.create_task(H3, "radarr", "Still Completed", Source.MAGNET, env.clock.now(), "")
    for s in COMPLETED_PATH:
        await env.repo.transition(H3, s, "t", env.clock.now())
    d = (await env.client.get("/web/status.json")).json()
    assert d["failed_history"] == [] and d["completed_history"] == []
    assert [t["name"] for t in d["tasks"]] == ["Not Deleted", "Still Completed"]


async def test_completed_history_size_and_path(env: Env) -> None:
    files = [ManifestFile("Done Movie/a.mkv", 1000), ManifestFile("Done Movie/b.mkv", 2000)]
    await seed_completed_removed(env.repo, env.clock, H1, "Done Movie", cloud_size=999, files=files)
    await seed_completed_removed(
        env.repo, env.clock, H2, "Manifest Only", files=[ManifestFile("Manifest Only", 4096)]
    )
    d = (await env.client.get("/web/status.json")).json()
    by_name = {t["name"]: t for t in d["completed_history"]}
    assert by_name["Done Movie"]["size"] == 999  # cloud_size 优先于清单和
    assert by_name["Manifest Only"]["size"] == 4096  # 无 cloud_size 用清单和
    assert by_name["Manifest Only"]["path"] == "/data/downloads/115/radarr/Manifest Only"


async def test_completed_history_uses_latest_completed_event(env: Env) -> None:
    await seed_completed_removed(env.repo, env.clock, H1, "Twice")
    events = await env.repo.list_events(H1, limit=200)
    first_at = next(e.at for e in events if e.to_state is TaskState.COMPLETED)
    await env.clock.advance(3600)
    await env.repo.requeue_removed(H1, "re-add", env.clock.now())
    for s in COMPLETED_PATH:
        await env.repo.transition(H1, s, "t", env.clock.now())
    await env.repo.transition(H1, TaskState.REMOVED, "t", env.clock.now())
    await relogin(env)
    d = (await env.client.get("/web/status.json")).json()
    (c,) = d["completed_history"]
    assert c["completed_at"] == env.clock.now().isoformat()
    assert c["completed_at"] != first_at.isoformat()


async def test_completed_history_fast_path_pulls_two_events(
    env: Env, monkeypatch: pytest.MonkeyPatch
) -> None:
    """常见 completed→removed 直达：只拉 2 条事件，不做深扫。"""
    await seed_completed_removed(env.repo, env.clock, H1, "Done Movie")
    calls: list[tuple[str, int]] = []
    real = env.repo.list_events

    async def spy(info_hash: str, limit: int = 50) -> Any:
        calls.append((info_hash, limit))
        return await real(info_hash, limit)

    monkeypatch.setattr(env.repo, "list_events", spy)
    d = (await env.client.get("/web/status.json")).json()
    (c,) = d["completed_history"]
    assert c["name"] == "Done Movie"
    assert c["completed_at"] == env.clock.now().isoformat()
    assert calls == [(H1, 2)]


async def test_completed_history_deep_scan_after_requeue_inflight_remove(
    env: Env, monkeypatch: pytest.MonkeyPatch
) -> None:
    """requeue 后在途再删：快路径不成立，深扫仍取到最近一条 completed。"""
    await seed_completed_removed(env.repo, env.clock, H1, "Requeued")
    completed_at = env.clock.now().isoformat()
    await env.clock.advance(3600)
    await env.repo.requeue_removed(H1, "re-add", env.clock.now())
    await env.repo.transition(H1, TaskState.SUBMITTING, "t", env.clock.now())
    await env.repo.transition(H1, TaskState.REMOVED, "t", env.clock.now())
    calls: list[tuple[str, int]] = []
    real = env.repo.list_events

    async def spy(info_hash: str, limit: int = 50) -> Any:
        calls.append((info_hash, limit))
        return await real(info_hash, limit)

    monkeypatch.setattr(env.repo, "list_events", spy)
    await relogin(env)
    d = (await env.client.get("/web/status.json")).json()
    (c,) = d["completed_history"]
    assert c["completed_at"] == completed_at  # 仍是上一轮的完成时刻
    assert calls == [(H1, 2), (H1, EVENTS_PER_TASK)]


async def test_history_escaping(env: Env) -> None:
    evil = '<script>alert("x")</script>&<b>'
    await seed_failed(env.repo, env.clock, H1, evil, error=evil)
    await seed_completed_removed(env.repo, env.clock, H2, evil)
    page = (await env.client.get("/")).text
    assert evil not in page
    assert "&lt;script&gt;alert(&quot;x&quot;)&lt;/script&gt;&amp;&lt;b&gt;" in page
    d = _embedded(page, "ui-data")
    assert d["failed_history"][0]["name"] == evil and d["failed_history"][0]["error"] == evil
    assert d["completed_history"][0]["path"].endswith(evil)


async def test_history_abs_times_utc_fallback_local_js(env: Env) -> None:
    await seed_failed(env.repo, env.clock, H1, "Failed Movie")
    await seed_completed_removed(env.repo, env.clock, H2, "Done Movie")
    page = (await env.client.get("/")).text
    # 服务端首屏 fallback 显式标 UTC（FakeClock 起点 2026-01-01T00:00:00Z）
    assert "2026-01-01 00:00:00 UTC" in page
    # JS 接管后走浏览器本地时区（现有 abs()/toLocaleString 模式）
    assert "failed_abs:abs(t.failed_at)" in page
    assert "completed_abs:abs(t.completed_at)" in page
