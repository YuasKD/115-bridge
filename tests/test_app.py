"""T16：应用入口测试。假 Session、临时目录，不访问网络、不调用真实 p115client。"""

from __future__ import annotations

import asyncio
import logging
import os
import signal
import socket
import subprocess
import sys
import threading
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path
from typing import Any

import httpx
import pytest

from bridge import app as bridge_app
from bridge.api.web import create_app
from bridge.app import (
    EXIT_CONFIG,
    EXIT_LOCKED,
    EXIT_OK,
    EXIT_RUNTIME,
    Components,
    StartupError,
    _run_server,
    attach_uvicorn_redaction,
    build_components,
    check_same_filesystem,
    check_state_writable,
    healthcheck,
    main,
    make_server,
    serve,
    validate_umask,
)
from bridge.cloud.fake import FakeCloud
from bridge.cloud.protocol import QrToken
from bridge.cloud.session import AlreadyRunning, LeaseHeld, SessionState, SessionStatus
from bridge.config import (
    AppConfig,
    ArrConfig,
    PathsConfig,
    Runtime,
    Secrets,
    Settings,
    WebUIConfig,
)
from bridge.log import RedactingFilter
from bridge.notify import NoopNotifier, TelegramNotifier
from bridge.pipeline.reconcile import ReconcileReport
from bridge.testing.clock import FakeClock

REPO_ROOT = Path(__file__).resolve().parent.parent


@pytest.fixture(autouse=True)
def _isolate_process_state(monkeypatch: pytest.MonkeyPatch) -> Any:
    """main 会改 umask、信号处理和 root logger：测试结束后全部恢复，不污染其他用例。"""
    old_umask = os.umask(0)
    os.umask(old_umask)
    old_handlers = {sig: signal.getsignal(sig) for sig in (signal.SIGINT, signal.SIGTERM)}
    root = logging.getLogger()
    old_log = (list(root.handlers), root.level)
    # main 里的 setup_logging 会清掉 root 的 handler（包括 caplog 的），默认换成空操作。
    monkeypatch.setattr(bridge_app, "setup_logging", lambda *a, **k: None)
    try:
        yield
    finally:
        os.umask(old_umask)
        for sig, handler in old_handlers.items():
            if handler is not None:
                signal.signal(sig, handler)
        root.handlers[:] = old_log[0]
        root.setLevel(old_log[1])


class FakeSession(FakeCloud):
    """假 Session：FakeCloud 加生命周期与 Web 视图接口，记录事件顺序。"""

    def __init__(self, clock: FakeClock, events: list[str], notifier: Any = None) -> None:
        super().__init__(clock)
        self.events = events
        self.notifier = notifier
        self.start_error: Exception | None = None
        self.started = False

    async def start(self) -> None:
        if self.start_error is not None:
            raise self.start_error
        self.started = True
        self.events.append("session.start")

    async def close(self) -> None:
        self.events.append("session.close")

    def status(self) -> SessionStatus:
        return SessionStatus(
            state=SessionState.OK,
            since=self.clock.now(),
            last_success=None,
            circuit_until=None,
            circuit_level=0,
            login_app="tv",
            cookie_present=True,
            qr_pending=False,
            quota=None,
            degraded_since=None,
            detail="",
        )

    def current_qr(self) -> QrToken | None:
        return None

    async def regenerate_qr(self) -> QrToken:
        raise NotImplementedError


def make_config(tmp_path: Path, *, telegram: bool = False, umask: str = "000") -> AppConfig:
    settings = Settings(
        webui=WebUIConfig(port=18080, username="admin"),
        paths=PathsConfig(
            root=str(tmp_path / "data" / "root"),
            incomplete=str(tmp_path / "data" / "root" / ".incomplete"),
            state=str(tmp_path / "config"),
        ),
        arr={
            "radarr": ArrConfig(url="http://radarr:7878"),
            "sonarr": ArrConfig(url="http://sonarr:8989"),
        },
    )
    secrets = Secrets.model_validate(
        {
            "webui_password": "pw",
            "arr_api_keys": {"radarr": "rk", "sonarr": "sk"},
            "telegram_bot_token": "123456:TG-TOKEN-ABCDEFGHIJ" if telegram else None,
            "telegram_chat_id": "42" if telegram else None,
        }
    )
    return AppConfig(settings=settings, secrets=secrets, runtime=Runtime(umask=umask))


class Harness:
    def __init__(self) -> None:
        self.clock = FakeClock()
        self.events: list[str] = []
        self.session: FakeSession | None = None
        self.reconcile_kwargs: dict[str, Any] = {}

    def session_factory(self, settings: Any, repo: Any, notifier: Any, clock: Any) -> FakeSession:
        self.session = FakeSession(self.clock, self.events, notifier)
        self.repo = repo
        return self.session

    async def open_repo(self, path: Path) -> Any:
        from bridge.db.sqlite import SqliteTaskRepo

        path.parent.mkdir(parents=True, exist_ok=True)
        repo = await SqliteTaskRepo.open(path)
        original = repo.close
        events = self.events

        async def close() -> None:
            events.append("repo.close")
            await original()

        repo.close = close  # type: ignore[method-assign]
        return repo

    async def reconcile(self, **kwargs: Any) -> ReconcileReport:
        self.events.append("reconcile")
        self.reconcile_kwargs = kwargs
        return ReconcileReport()

    async def build(self, config: AppConfig) -> Components:
        return await build_components(
            config,
            clock=self.clock,
            session_factory=self.session_factory,
            open_repo=self.open_repo,
            reconcile_fn=self.reconcile,
        )


# ---------------------------------------------------------------- UMASK


@pytest.mark.parametrize(
    ("value", "expected"),
    [("000", 0), ("022", 0o22), ("0077", 0o77), ("777", 0o777), ("0000", 0)],
)
def test_umask_valid(value: str, expected: int) -> None:
    assert validate_umask(value) == expected


@pytest.mark.parametrize(
    "value", ["", "0", "08", "089", "abc", "22", "00000", "-22", " 022", "022 ", "0o22", "٠٠٠"]
)
def test_umask_invalid(value: str) -> None:
    with pytest.raises(StartupError):
        validate_umask(value)


# ---------------------------------------------------------------- 文件系统与状态目录


def test_same_filesystem_creates_missing_dirs(tmp_path: Path) -> None:
    settings = make_config(tmp_path).settings
    check_same_filesystem(settings)
    assert Path(settings.paths.root).is_dir()
    assert Path(settings.paths.incomplete).is_dir()


def test_different_filesystem_is_startup_error(tmp_path: Path) -> None:
    settings = make_config(tmp_path).settings
    incomplete = Path(settings.paths.incomplete)

    def stat_dev(path: Path) -> int:
        return 2 if path == incomplete else 1

    with pytest.raises(StartupError, match="same filesystem"):
        check_same_filesystem(settings, stat_dev)


def test_same_filesystem_checks_after_creating(tmp_path: Path) -> None:
    settings = make_config(tmp_path).settings
    seen: list[bool] = []

    def stat_dev(path: Path) -> int:
        seen.append(path.is_dir())
        return 7

    check_same_filesystem(settings, stat_dev)
    assert seen == [True, True]


def test_state_dir_not_writable(tmp_path: Path) -> None:
    blocker = tmp_path / "file"
    blocker.write_text("x")
    config = make_config(tmp_path)
    settings = config.settings.model_copy(
        update={"paths": PathsConfig(state=str(blocker / "sub"))},
    )
    with pytest.raises(StartupError, match="paths.state"):
        check_state_writable(settings)


def test_state_dir_created_and_clean(tmp_path: Path) -> None:
    settings = make_config(tmp_path).settings
    check_state_writable(settings)
    assert list(Path(settings.paths.state).iterdir()) == []


# ---------------------------------------------------------------- 装配接线


async def test_wiring(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    h = Harness()
    created: dict[str, Any] = {}

    def spy_create_app(**kwargs: Any) -> Any:
        created.update(kwargs)
        return create_app(**kwargs)

    monkeypatch.setattr(bridge_app.web, "create_app", spy_create_app)
    c = await h.build(make_config(tmp_path, telegram=True))
    try:
        # Notifier 只有一个实例，Telegram 配好就用 Telegram
        assert isinstance(c.notifier, TelegramNotifier)
        assert h.session is not None and h.session.notifier is c.notifier
        assert c.orchestrator._notifier is c.notifier
        assert h.reconcile_kwargs["notifier"] is c.notifier
        assert created["session"] is h.session

        # Arr 映射的键是 category，同一 Arr 的客户端实例对应其配置
        assert set(c.arrs) == {"radarr", "tv-sonarr"}
        assert c.orchestrator._arrs is c.arrs
        assert c.arrs["radarr"].name == "radarr"  # type: ignore[attr-defined]
        assert c.arrs["tv-sonarr"].name == "sonarr"  # type: ignore[attr-defined]

        # qBit 层 cleaner 是 orch.cleaner；同一 Fetcher 的进度与同一调度器的停住原因
        assert created["cleaner"] is c.orchestrator.cleaner
        assert created["progress_source"] is c.fetcher.progress_source
        assert created["stall_view"] is c.orchestrator
        assert created["missing_files"] is c.report.missing_files
        assert c.orchestrator._fetcher is c.fetcher
        assert c.orchestrator._cloud is h.session
        assert c.fetcher._cloud is h.session

        # Fetcher 回调指向调度器（后绑定）
        c.fetcher._on_cdn_403("a" * 40, "f1")  # type: ignore[misc]
        assert c.orchestrator._cdn_403_files == {("a" * 40, "f1")}
        c.fetcher._on_file_done("a" * 40, "f1")  # type: ignore[misc]
        assert c.orchestrator._cdn_403_files == set()

        # 两个 http 客户端都有有限超时
        for client in (c.arr_http, c.fetch_http):
            t = client.timeout
            assert t.connect and t.read and t.write and t.pool
        assert c.fetch_http is not c.arr_http
        assert c.fetcher._http is c.fetch_http
    finally:
        await c.aclose()
    assert c.arr_http.is_closed and c.fetch_http.is_closed


async def test_noop_notifier_without_telegram(tmp_path: Path) -> None:
    h = Harness()
    c = await h.build(make_config(tmp_path))
    try:
        assert isinstance(c.notifier, NoopNotifier)
    finally:
        await c.aclose()


async def test_healthz_unauthenticated(tmp_path: Path) -> None:
    h = Harness()
    c = await h.build(make_config(tmp_path))
    try:
        transport = httpx.ASGITransport(app=c.app)
        async with httpx.AsyncClient(transport=transport, base_url="http://t") as client:
            resp = await client.get("/healthz")
            assert resp.status_code == 200
            assert resp.text == "ok"
            # 其他路由仍然需要认证
            assert (await client.get("/api/v2/torrents/info")).status_code == 403
    finally:
        await c.aclose()


async def test_reconcile_runs_before_serving(tmp_path: Path) -> None:
    h = Harness()
    c = await h.build(make_config(tmp_path))
    try:
        # 构造完成时对账已经执行，调度器与 uvicorn 尚未启动
        assert h.events == ["session.start", "reconcile"]
    finally:
        await c.aclose()


async def test_lock_failure_cleans_up(tmp_path: Path) -> None:
    h = Harness()
    original = h.session_factory

    def factory(settings: Any, repo: Any, notifier: Any, clock: Any) -> FakeSession:
        s = original(settings, repo, notifier, clock)
        s.start_error = LeaseHeld("held")
        return s

    with pytest.raises(LeaseHeld):
        await build_components(
            make_config(tmp_path),
            clock=h.clock,
            session_factory=factory,
            open_repo=h.open_repo,
            reconcile_fn=h.reconcile,
        )
    assert h.events == ["repo.close"]  # 仓储被回收，没有走到对账


# ---------------------------------------------------------------- 运行与关停


class FakeServer:
    def __init__(self, events: list[str]) -> None:
        self.events = events
        self._exit = asyncio.Event()
        self.running = asyncio.Event()

    @property
    def should_exit(self) -> bool:
        return self._exit.is_set()

    @should_exit.setter
    def should_exit(self, value: bool) -> None:
        if value:
            self._exit.set()

    async def serve(self) -> None:
        self.running.set()
        await self._exit.wait()
        self.events.append("server.exit")


async def test_shutdown_order(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    h = Harness()
    c = await h.build(make_config(tmp_path))
    events = h.events

    async def fake_run() -> None:
        events.append("run.start")
        try:
            await asyncio.sleep(3600)
        finally:
            await asyncio.sleep(0)
            events.append("run.shutdown")

    monkeypatch.setattr(c.orchestrator, "run", fake_run)
    server = FakeServer(events)
    stop = asyncio.Event()
    task = asyncio.create_task(serve(c, server, stop))
    await server.running.wait()
    await asyncio.sleep(0.01)
    assert "run.start" in events and not task.done()
    stop.set()
    code = await asyncio.wait_for(task, 5)
    assert code == EXIT_OK
    tail = events[events.index("run.start") :]
    assert tail == [
        "run.start",
        "run.shutdown",
        "server.exit",
        "session.close",
        "repo.close",
    ]
    assert c.arr_http.is_closed and c.fetch_http.is_closed


async def test_unexpected_exit_shuts_down_with_error_code(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    h = Harness()
    c = await h.build(make_config(tmp_path))

    async def crashing_run() -> None:
        raise RuntimeError("boom")

    monkeypatch.setattr(c.orchestrator, "run", crashing_run)
    server = FakeServer(h.events)
    code = await asyncio.wait_for(serve(c, server, asyncio.Event()), 5)
    assert code == EXIT_RUNTIME
    assert server.should_exit
    assert h.events[-2:] == ["session.close", "repo.close"]


# ---------------------------------------------------------------- main 的退出码


def write_env(
    tmp_path: Path, *, umask: str = "000", extra_yaml: str = "", port: int = 18080
) -> dict[str, str]:
    cfg = tmp_path / "config.yml"
    cfg.write_text(
        f"webui:\n  username: admin\n  port: {port}\n"
        f"paths:\n  root: {(tmp_path / 'd').as_posix()}\n"
        f"  incomplete: {(tmp_path / 'd' / '.inc').as_posix()}\n"
        f"  state: {(tmp_path / 's').as_posix()}\n"
        "arr:\n  radarr:\n    url: http://r\n  sonarr:\n    url: http://s\n" + extra_yaml,
        encoding="utf-8",
    )
    return {
        "BRIDGE_CONFIG": str(cfg),
        "WEBUI_PASSWORD": "pw",
        "RADARR_API_KEY": "rk",
        "SONARR_API_KEY": "sk",
        "UMASK": umask,
    }


async def test_main_invalid_umask(tmp_path: Path) -> None:
    code = await main([], write_env(tmp_path, umask="089"))
    assert code == EXIT_CONFIG


async def test_main_missing_secret(tmp_path: Path) -> None:
    env = write_env(tmp_path)
    del env["WEBUI_PASSWORD"]
    assert await main([], env) == EXIT_CONFIG


async def test_main_lease_held_exits_nonzero(tmp_path: Path) -> None:
    h = Harness()
    original = h.session_factory

    def factory(settings: Any, repo: Any, notifier: Any, clock: Any) -> FakeSession:
        s = original(settings, repo, notifier, clock)
        s.start_error = LeaseHeld("held")
        return s

    assert await main([], write_env(tmp_path), session_factory=factory) == EXIT_LOCKED


async def test_main_flock_held_exits_nonzero(tmp_path: Path) -> None:
    h = Harness()
    original = h.session_factory

    def factory(settings: Any, repo: Any, notifier: Any, clock: Any) -> FakeSession:
        s = original(settings, repo, notifier, clock)
        s.start_error = AlreadyRunning("locked")
        return s

    assert await main([], write_env(tmp_path), session_factory=factory) == EXIT_LOCKED


class BlockingStartSession(FakeSession):
    """start() 卡住（模拟对账或 115 观测很慢），记录是否被取消。"""

    def __init__(self, clock: FakeClock, events: list[str], notifier: Any = None) -> None:
        super().__init__(clock, events, notifier)
        self.entered = asyncio.Event()
        self.cancelled = False

    async def start(self) -> None:
        self.entered.set()
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            self.cancelled = True
            raise


async def test_build_cancelled_mid_startup_cleans_up(tmp_path: Path) -> None:
    h = Harness()
    sessions: list[BlockingStartSession] = []
    created = asyncio.Event()

    def factory(settings: Any, repo: Any, notifier: Any, clock: Any) -> FakeSession:
        s = BlockingStartSession(h.clock, h.events, notifier)
        sessions.append(s)
        created.set()
        return s

    task = asyncio.create_task(
        build_components(
            make_config(tmp_path),
            clock=h.clock,
            session_factory=factory,
            open_repo=h.open_repo,
            reconcile_fn=h.reconcile,
        )
    )
    await created.wait()
    await sessions[0].entered.wait()
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert h.events == ["repo.close"]


async def test_main_installs_signals_first_and_stop_during_startup_exits_zero(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    stops: list[asyncio.Event] = []
    monkeypatch.setattr(bridge_app, "install_stop_signals", stops.append)
    h = Harness()
    sessions: list[BlockingStartSession] = []
    created = asyncio.Event()

    def factory(settings: Any, repo: Any, notifier: Any, clock: Any) -> FakeSession:
        s = BlockingStartSession(h.clock, h.events, notifier)
        sessions.append(s)
        created.set()
        return s

    task = asyncio.create_task(main([], write_env(tmp_path), session_factory=factory))
    await created.wait()
    await sessions[0].entered.wait()
    assert len(stops) == 1  # 构造开始之前信号处理已经装好
    stops[0].set()  # 启动阶段收到 SIGTERM
    code = await asyncio.wait_for(task, 5)
    assert code == EXIT_OK
    assert sessions[0].cancelled
    assert "session.close" not in h.events  # 还没登记的 Session 不会被 close


async def test_main_signals_installed_even_when_config_invalid(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    stops: list[asyncio.Event] = []
    monkeypatch.setattr(bridge_app, "install_stop_signals", stops.append)
    assert await main([], write_env(tmp_path, umask="089")) == EXIT_CONFIG
    assert len(stops) == 1


async def test_main_unexpected_exception_is_structured_and_redacted(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    fake_cookie = "UID=fakeuid123_abc; CID=fakecid456"
    fake_key = "apikey=FAKEKEY0123456789"
    long_tail = "x" * 1000

    def factory(settings: Any, repo: Any, notifier: Any, clock: Any) -> FakeSession:
        raise RuntimeError(f"boom {fake_cookie} {fake_key} {long_tail}")

    with caplog.at_level(logging.DEBUG):
        code = await main([], write_env(tmp_path), session_factory=factory)
    assert code == EXIT_RUNTIME
    rec = next(r for r in caplog.records if r.getMessage() == "unexpected error, exiting")
    assert rec.error_type == "RuntimeError"  # type: ignore[attr-defined]
    assert rec.stage == "build"  # type: ignore[attr-defined]
    msg = rec.error  # type: ignore[attr-defined]
    assert msg.startswith("boom ")
    assert len(msg) <= 300
    assert "x" * 100 in msg  # 消息本身有记录
    assert any(r.levelno == logging.DEBUG and "Traceback" in r.getMessage() for r in caplog.records)
    for secret in ("fakeuid123_abc", "fakecid456", "FAKEKEY0123456789"):
        assert secret not in caplog.text


async def test_main_unexpected_exception_no_traceback_above_debug(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    def factory(settings: Any, repo: Any, notifier: Any, clock: Any) -> FakeSession:
        raise RuntimeError("boom")

    with caplog.at_level(logging.INFO):
        await main([], write_env(tmp_path), session_factory=factory)
    assert "Traceback" not in caplog.text


async def test_main_make_server_failure_closes_components(tmp_path: Path) -> None:
    h = Harness()

    def bad_server(app: Any, port: int) -> Any:
        raise OSError("cannot create server")

    code = await main(
        [], write_env(tmp_path), session_factory=h.session_factory, server_factory=bad_server
    )
    assert code == EXIT_RUNTIME
    assert h.events[-1] == "session.close"  # 组件被回收
    with pytest.raises(Exception):  # noqa: B017, PT011
        await h.repo.list_tasks()  # 仓储已关闭


def test_real_setup_logging_is_restored_by_fixture(monkeypatch: pytest.MonkeyPatch) -> None:
    # 真实 setup_logging 会替换 root handler；本文件的 autouse 夹具负责恢复。
    from bridge.log import setup_logging

    before = list(logging.getLogger().handlers)
    setup_logging()
    assert logging.getLogger().handlers != before


async def test_missing_config_file_exits_config_code(tmp_path: Path) -> None:
    env = write_env(tmp_path)
    env["BRIDGE_CONFIG"] = str(tmp_path / "absent.yml")
    assert await main([], env) == EXIT_CONFIG


# ---------------------------------------------------------------- 健康检查与代理


class _OkHandler(BaseHTTPRequestHandler):
    def do_GET(self) -> None:  # noqa: N802
        self.send_response(200 if self.path == "/healthz" else 404)
        self.end_headers()
        self.wfile.write(b"ok")

    def log_message(self, *args: Any) -> None:
        return None


def test_healthcheck_bypasses_proxy_env(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    httpd = HTTPServer(("127.0.0.1", 0), _OkHandler)
    thread = threading.Thread(target=httpd.serve_forever, daemon=True)
    thread.start()
    try:
        for var in ("HTTP_PROXY", "http_proxy", "ALL_PROXY", "all_proxy"):
            monkeypatch.setenv(var, "http://127.0.0.1:9")  # 死代理：经过它就会失败
        monkeypatch.delenv("NO_PROXY", raising=False)
        monkeypatch.delenv("no_proxy", raising=False)
        assert healthcheck(write_env(tmp_path, port=httpd.server_address[1])) == EXIT_OK
    finally:
        httpd.shutdown()
        httpd.server_close()


def test_healthcheck_failure_returns_runtime(tmp_path: Path) -> None:
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        port = sock.getsockname()[1]
    assert healthcheck(write_env(tmp_path, port=port)) == EXIT_RUNTIME


async def test_http_clients_proxy_policy(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("HTTPS_PROXY", "http://127.0.0.1:9")
    h = Harness()
    c = await h.build(make_config(tmp_path))
    try:
        assert c.arr_http.trust_env is False  # Arr 在局域网
        assert c.fetch_http.trust_env is True  # CDN 在公网，可经环境代理
    finally:
        await c.aclose()


# ---------------------------------------------------------------- 真实调度器与真实 uvicorn


async def test_serve_with_real_orchestrator_run_shuts_down(tmp_path: Path) -> None:
    h = Harness()
    c = await h.build(make_config(tmp_path))
    server = FakeServer(h.events)
    stop = asyncio.Event()
    task = asyncio.create_task(serve(c, server, stop))
    await server.running.wait()
    # 真实 run()：跑完首个 tick 后在 FakeClock 上睡眠
    for _ in range(1000):  # tick 里有 aiosqlite 线程往返，按真实时间轮询
        if h.clock.pending_sleepers:
            break
        await asyncio.sleep(0.01)
    assert h.clock.pending_sleepers >= 1 and not task.done()
    stop.set()
    assert await asyncio.wait_for(task, 5) == EXIT_OK
    assert h.events[-3:] == ["server.exit", "session.close", "repo.close"]
    assert c.arr_http.is_closed and c.fetch_http.is_closed


def _free_port() -> int:
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


async def test_real_uvicorn_serves_healthz_and_stops(tmp_path: Path) -> None:
    h = Harness()
    c = await h.build(make_config(tmp_path))
    port = _free_port()
    server = make_server(c.app, port)
    task = asyncio.create_task(_run_server(server))
    try:
        for _ in range(500):
            if getattr(server, "started", False) or task.done():
                break
            await asyncio.sleep(0.01)
        assert getattr(server, "started", False)
        async with httpx.AsyncClient(trust_env=False) as client:
            resp = await client.get(f"http://127.0.0.1:{port}/healthz")
        assert resp.status_code == 200 and resp.text == "ok"
        server.should_exit = True
        await asyncio.wait_for(task, 10)
    finally:
        server.should_exit = True
        if not task.done():
            task.cancel()
        await c.aclose()


async def test_real_uvicorn_port_in_use_becomes_runtime_error_and_cleans_up(
    tmp_path: Path,
) -> None:
    h = Harness()
    c = await h.build(make_config(tmp_path))
    with socket.socket() as blocker:
        blocker.bind(("0.0.0.0", 0))  # noqa: S104
        blocker.listen()
        port = blocker.getsockname()[1]
        with pytest.raises(RuntimeError, match="uvicorn exited during startup"):
            await asyncio.wait_for(_run_server(make_server(c.app, port)), 10)
        # 经 serve：uvicorn 启动失败 -> 退出码 1，其余资源仍按序回收
        code = await asyncio.wait_for(
            serve(c, make_server(c.app, port), asyncio.Event()),
            10,
        )
    assert code == EXIT_RUNTIME
    assert h.events[-2:] == ["session.close", "repo.close"]
    assert c.arr_http.is_closed and c.fetch_http.is_closed


# ---------------------------------------------------------------- 日志与入口


def test_uvicorn_loggers_get_redaction_filter() -> None:
    attach_uvicorn_redaction()
    attach_uvicorn_redaction()  # 幂等
    for name in ("uvicorn", "uvicorn.error", "uvicorn.access"):
        filters = [f for f in logging.getLogger(name).filters if isinstance(f, RedactingFilter)]
        assert len(filters) == 1


def test_module_entry_help() -> None:
    env = {**os.environ, "PYTHONPATH": str(REPO_ROOT / "src")}
    proc = subprocess.run(
        [sys.executable, "-m", "bridge", "--help"],
        capture_output=True,
        text=True,
        env=env,
        timeout=60,
        check=False,
    )
    assert proc.returncode == 0
    assert "--healthcheck" in proc.stdout
