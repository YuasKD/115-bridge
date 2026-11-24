"""应用入口（T16）：加载配置、校验、装配各组件，并在同一个事件循环里并发运行 uvicorn 与调度器。

装配与运行拆开，便于测试：

- ``validate_umask`` / ``check_same_filesystem`` / ``check_state_writable``：启动前校验。
- ``build_components``：按依赖顺序构造所有组件（含启动对账），失败时回收已打开的资源。
- ``serve``：并发运行 uvicorn 与 ``Orchestrator.run()``，收到停止信号后按固定顺序关停。
- ``main``：串起以上各步并返回退出码。

退出码：0 正常停止，1 运行中异常退出，2 配置或启动校验失败，3 拿不到进程锁或 lease。

信号：``main`` 一开始就安装 SIGTERM/SIGINT 处理（容器里 Python 是 PID 1，没有默认的信号处理）。
启动阶段（含对账、115 观测）收到信号时中止构造，按已登记的顺序回收资源，以 0 退出。
"""

from __future__ import annotations

import argparse
import asyncio
import contextlib
import logging
import os
import re
import signal
import sys
import tempfile
import traceback
from collections.abc import Awaitable, Callable, Mapping
from contextlib import AbstractContextManager, AsyncExitStack, nullcontext
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Protocol

import httpx
import uvicorn
from fastapi import FastAPI

from bridge.api import web
from bridge.api.web import SessionView
from bridge.arr.client import HttpArrClient
from bridge.arr.protocol import ArrClient
from bridge.clock import Clock, SystemClock
from bridge.cloud.protocol import CloudClient
from bridge.cloud.session import AlreadyRunning, LeaseHeld, Session
from bridge.config import AppConfig, ConfigError, Settings, load_config
from bridge.db.repo import TaskRepo
from bridge.db.sqlite import SqliteTaskRepo
from bridge.log import RedactingFilter, log_event, redact, setup_logging
from bridge.notify import NoopNotifier, Notifier, TelegramNotifier
from bridge.pipeline.fetch import Fetcher
from bridge.pipeline.orchestrator import Orchestrator
from bridge.pipeline.reconcile import ReconcileReport, reconcile
from bridge.pipeline.verify import Verifier

logger = logging.getLogger(__name__)

DB_FILENAME = "bridge.db"
UVICORN_LOGGERS = ("uvicorn", "uvicorn.error", "uvicorn.access")

EXIT_OK = 0
EXIT_RUNTIME = 1
EXIT_CONFIG = 2
EXIT_LOCKED = 3

ERROR_MESSAGE_LIMIT = 300

# 有限超时：Arr 与 Telegram 是短请求；CDN 下载是长流，只限制连接与「两次数据之间」的读间隔。
ARR_TIMEOUT_SECONDS = 15.0
ARR_TIMEOUT = httpx.Timeout(ARR_TIMEOUT_SECONDS)
TELEGRAM_TIMEOUT = httpx.Timeout(15.0)
FETCH_TIMEOUT = httpx.Timeout(connect=15.0, read=60.0, write=60.0, pool=30.0)

_UMASK_RE = re.compile(r"[0-7]{3,4}")


class StartupError(Exception):
    """启动前校验失败（消息不含密钥）。"""


# ---------------------------------------------------------------- 启动校验


def validate_umask(value: str) -> int:
    """``UMASK`` 必须是 3 或 4 位八进制数字串（如 ``000``、``022``、``0077``），返回其整数值。"""
    if _UMASK_RE.fullmatch(value) is None:
        raise StartupError(f"UMASK must be 3 or 4 octal digits (0-7), got {value!r}")
    return int(value, 8)


def _default_stat_dev(path: Path) -> int:
    return os.stat(path).st_dev


def check_same_filesystem(
    settings: Settings,
    stat_dev: Callable[[Path], int] = _default_stat_dev,
) -> None:
    """``paths.incomplete`` 与 ``paths.root`` 必须在同一文件系统（否则落盘 rename 会 EXDEV）。

    目录不存在时先创建，再比较 ``st_dev``。"""
    root = Path(settings.paths.root)
    incomplete = Path(settings.paths.incomplete)
    for path in (root, incomplete):
        try:
            path.mkdir(parents=True, exist_ok=True)
        except OSError as exc:
            raise StartupError(f"cannot create directory {path}: {exc.strerror or exc}") from None
    if stat_dev(root) != stat_dev(incomplete):
        raise StartupError(
            f"paths.incomplete ({incomplete}) and paths.root ({root}) must be on the same "
            "filesystem"
        )


def check_state_writable(settings: Settings) -> None:
    """``paths.state`` 必须存在（不存在则创建）且当前用户可写。"""
    state = Path(settings.paths.state)
    try:
        state.mkdir(parents=True, exist_ok=True)
        with tempfile.NamedTemporaryFile(dir=state, prefix=".write-test-"):
            pass
    except OSError as exc:
        raise StartupError(
            f"paths.state ({state}) is not writable: {exc.strerror or exc}"
        ) from None


def validate_startup(
    config: AppConfig,
    stat_dev: Callable[[Path], int] = _default_stat_dev,
) -> int:
    """全部启动校验，返回 umask 整数值。"""
    umask = validate_umask(config.runtime.umask)
    check_same_filesystem(config.settings, stat_dev)
    check_state_writable(config.settings)
    return umask


def attach_uvicorn_redaction() -> None:
    """给 uvicorn 自己的 logger 也挂脱敏 Filter（访问日志里有 URL 查询串）。"""
    for name in UVICORN_LOGGERS:
        lg = logging.getLogger(name)
        if not any(isinstance(f, RedactingFilter) for f in lg.filters):
            lg.addFilter(RedactingFilter())


# ---------------------------------------------------------------- 装配


class SessionLike(CloudClient, SessionView, Protocol):
    """``cloud.session.Session`` 满足它；测试用假实现。"""

    async def start(self) -> None: ...

    async def close(self) -> None: ...


SessionFactory = Callable[[Settings, TaskRepo, Notifier, Clock], SessionLike]
ReconcileFn = Callable[..., Awaitable[ReconcileReport]]
RepoOpener = Callable[[Path], Awaitable[SqliteTaskRepo]]


def _default_session_factory(
    settings: Settings, repo: TaskRepo, notifier: Notifier, clock: Clock
) -> SessionLike:
    return Session.from_settings(settings, repo, notifier, clock)


async def _open_repo(path: Path) -> SqliteTaskRepo:
    return await SqliteTaskRepo.open(path)


@dataclass(slots=True)
class Components:
    settings: Settings
    clock: Clock
    repo: TaskRepo
    notifier: Notifier
    session: SessionLike
    arrs: dict[str, ArrClient]  # 键是 category
    arr_http: httpx.AsyncClient
    fetch_http: httpx.Client
    fetcher: Fetcher
    verifier: Verifier
    orchestrator: Orchestrator
    report: ReconcileReport
    app: FastAPI
    exit_stack: AsyncExitStack

    async def aclose(self) -> None:
        """关 Session、http 客户端、仓储（与打开顺序相反；Session 关闭时还要用仓储释放 lease）。"""
        await self.exit_stack.aclose()


def build_arrs(
    settings: Settings, api_keys: Mapping[str, str], http: httpx.AsyncClient
) -> dict[str, ArrClient]:
    """每个 Arr 一个客户端；返回 category -> ArrClient（同一 Arr 的多个 category 共用实例）。"""
    clients: dict[str, ArrClient] = {}
    for name, cfg in settings.arr.items():
        clients[name] = HttpArrClient(
            name,
            cfg.resolved_type(name),
            cfg.url,
            api_keys[name],
            client=http,
            timeout=ARR_TIMEOUT_SECONDS,
        )
    return {category: clients[arr_name] for category, arr_name in settings.categories.items()}


async def build_components(
    config: AppConfig,
    *,
    clock: Clock | None = None,
    session_factory: SessionFactory = _default_session_factory,
    open_repo: RepoOpener = _open_repo,
    reconcile_fn: ReconcileFn = reconcile,
) -> Components:
    """按依赖顺序构造并启动组件，在调度器和 uvicorn 之前完成启动对账。

    拿不到进程锁或 lease 时 ``Session.start()`` 抛 ``AlreadyRunning`` / ``LeaseHeld``，
    原样向上抛；此前打开的资源都会被回收。"""
    settings = config.settings
    secrets = config.secrets
    clk = clock or SystemClock()
    stack = AsyncExitStack()
    try:
        repo = await open_repo(Path(settings.paths.state) / DB_FILENAME)
        stack.push_async_callback(repo.close)

        notifier: Notifier
        if secrets.telegram_enabled:
            assert secrets.telegram_bot_token is not None
            assert secrets.telegram_chat_id is not None
            tg_http = httpx.AsyncClient(timeout=TELEGRAM_TIMEOUT)
            stack.push_async_callback(tg_http.aclose)
            notifier = TelegramNotifier(
                secrets.telegram_bot_token.get_secret_value(),
                secrets.telegram_chat_id.get_secret_value(),
                clk,
                client=tg_http,
            )
        else:
            notifier = NoopNotifier()

        # Arr 在局域网：不读环境代理变量（HTTP(S)_PROXY 多半是给 Telegram 设的）。
        # CDN 在公网：保留 trust_env，允许经环境代理访问。
        arr_http = httpx.AsyncClient(timeout=ARR_TIMEOUT, trust_env=False)
        fetch_http = httpx.Client(timeout=FETCH_TIMEOUT)  # 读线程里同步使用
        stack.push_async_callback(arr_http.aclose)
        stack.callback(fetch_http.close)

        session = session_factory(settings, repo, notifier, clk)
        await session.start()
        stack.push_async_callback(session.close)

        arrs = build_arrs(
            settings,
            {name: key.get_secret_value() for name, key in secrets.arr_api_keys.items()},
            arr_http,
        )

        # Fetcher 先于调度器创建：回调用 lambda 后绑定。
        orch_ref: list[Orchestrator] = []
        fetcher = Fetcher(
            repo=repo,
            cloud=session,
            http=fetch_http,
            clock=clk,
            config=settings.fetch,
            incomplete_dir=settings.paths.incomplete,
            on_cdn_403=lambda h, p: orch_ref[0].on_cdn_403(h, p),
            on_file_done=lambda h, p: orch_ref[0].on_file_done(h, p),
        )
        verifier = Verifier(
            repo,
            root=settings.paths.root,
            incomplete=settings.paths.incomplete,
            cfg=settings.verify,
        )
        orchestrator = Orchestrator(
            repo=repo,
            cloud=session,
            arrs=arrs,
            notifier=notifier,
            clock=clk,
            fetcher=fetcher,
            verifier=verifier,
            settings=settings,
        )
        orch_ref.append(orchestrator)

        report = await reconcile_fn(
            repo=repo, cloud=session, clock=clk, settings=settings, notifier=notifier
        )

        app = web.create_app(
            repo=repo,
            settings=settings,
            secrets=secrets,
            clock=clk,
            cleaner=orchestrator.cleaner,
            session=session,
            progress_source=fetcher.progress_source,
            stall_view=orchestrator,
            missing_files=report.missing_files,
        )
    except BaseException:
        await stack.aclose()
        raise
    return Components(
        settings=settings,
        clock=clk,
        repo=repo,
        notifier=notifier,
        session=session,
        arrs=arrs,
        arr_http=arr_http,
        fetch_http=fetch_http,
        fetcher=fetcher,
        verifier=verifier,
        orchestrator=orchestrator,
        report=report,
        app=app,
        exit_stack=stack,
    )


# ---------------------------------------------------------------- 运行


class ServerLike(Protocol):
    should_exit: bool

    async def serve(self) -> None: ...


class _Server(uvicorn.Server):
    """不让 uvicorn 接管信号：关停顺序由 ``serve`` 统一控制。"""

    def capture_signals(self) -> AbstractContextManager[None]:  # type: ignore[override]
        return nullcontext()


def make_server(app: FastAPI, port: int) -> ServerLike:
    config = uvicorn.Config(app, host="0.0.0.0", port=port, log_config=None)  # noqa: S104
    return _Server(config)


async def _run_server(server: ServerLike) -> None:
    try:
        await server.serve()
    except SystemExit as exc:  # uvicorn 启动失败（如端口被占）时 sys.exit(1)
        raise RuntimeError(f"uvicorn exited during startup (code {exc.code})") from None


def install_stop_signals(stop: asyncio.Event) -> None:
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        try:
            loop.add_signal_handler(sig, stop.set)
        except NotImplementedError:  # Windows
            signal.signal(sig, lambda *_: loop.call_soon_threadsafe(stop.set))


async def _await_quietly(task: asyncio.Task[Any], what: str) -> None:
    try:
        await task
    except asyncio.CancelledError:
        return
    except Exception as exc:  # noqa: BLE001
        log_event(logger, logging.ERROR, f"{what} ended with error", error_type=type(exc).__name__)


async def serve(components: Components, server: ServerLike, stop: asyncio.Event) -> int:
    """并发运行 uvicorn 与调度器，直到收到停止信号或任一方退出，然后按顺序关停：

    取消 ``run()``（它会 shutdown 并等后台协程）→ 停 uvicorn → 关 Session、http 客户端、仓储。
    返回退出码：因停止信号退出为 0，否则为 1。"""
    loop = asyncio.get_running_loop()
    orch_task = loop.create_task(components.orchestrator.run())
    server_task = loop.create_task(_run_server(server))
    stop_task = loop.create_task(stop.wait())
    code = EXIT_RUNTIME
    try:
        await asyncio.wait({orch_task, server_task, stop_task}, return_when=asyncio.FIRST_COMPLETED)
        if stop_task.done():
            code = EXIT_OK
        else:
            log_event(logger, logging.ERROR, "a main task exited unexpectedly, shutting down")
    finally:
        stop_task.cancel()
        orch_task.cancel()
        await _await_quietly(orch_task, "orchestrator")
        server.should_exit = True
        await _await_quietly(server_task, "uvicorn")
        await components.aclose()
    return code


# ---------------------------------------------------------------- 入口


def healthcheck(env: Mapping[str, str] | None = None) -> int:
    """容器健康检查：转调 ``bridge.healthcheck``（轻量，不导入整个应用）。"""
    from bridge.healthcheck import check

    return check(env)


def _parse_args(argv: list[str] | None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="python -m bridge",
        description="115 Bridge: qBittorrent-compatible facade backed by 115 offline downloads. "
        "Configuration comes from the YAML file (BRIDGE_CONFIG, default /config/config.yml) "
        "and environment variables.",
    )
    parser.add_argument(
        "--healthcheck",
        action="store_true",
        help="query the local /healthz endpoint and exit 0 if healthy (for container healthchecks)",
    )
    return parser.parse_args(argv)


async def main(
    argv: list[str] | None = None,
    env: Mapping[str, str] | None = None,
    *,
    session_factory: SessionFactory = _default_session_factory,
    server_factory: Callable[[FastAPI, int], ServerLike] = make_server,
) -> int:
    args = _parse_args(argv)
    if args.healthcheck:
        return healthcheck(env)

    setup_logging()
    attach_uvicorn_redaction()
    # 信号处理最先安装：启动阶段收到 SIGTERM 也能及时中止，而不是等 SIGKILL。
    stop = asyncio.Event()
    install_stop_signals(stop)
    stage = "config"
    try:
        config = load_config(env)
        stage = "validate"
        umask = validate_startup(config)
        os.umask(umask)
        stage = "build"
        components = await _build_unless_stopped(config, session_factory, stop)
        if components is None:
            log_event(logger, logging.INFO, "stop signal received during startup, exiting")
            return EXIT_OK
        stage = "serve"
        try:
            server = server_factory(components.app, config.settings.webui.port)
        except BaseException:
            # serve() 的 finally 还没接管：这里保证组件被回收。
            await components.aclose()
            raise
        log_event(logger, logging.INFO, "bridge started", port=config.settings.webui.port)
        return await serve(components, server, stop)
    except (ConfigError, StartupError) as exc:
        log_event(logger, logging.ERROR, "startup failed", reason=str(exc))
        return EXIT_CONFIG
    except AlreadyRunning as exc:
        log_event(
            logger, logging.ERROR, "another process holds the state lock, exiting", reason=str(exc)
        )
        return EXIT_LOCKED
    except LeaseHeld as exc:
        log_event(
            logger, logging.ERROR, "another instance holds the 115 lease, exiting", reason=str(exc)
        )
        return EXIT_LOCKED
    except Exception as exc:  # noqa: BLE001
        # 消息先脱敏再截断（截断可能切断密钥片段，所以顺序不能反）。
        log_event(
            logger,
            logging.ERROR,
            "unexpected error, exiting",
            error_type=type(exc).__name__,
            stage=stage,
            error=redact(str(exc))[:ERROR_MESSAGE_LIMIT],
        )
        if logger.isEnabledFor(logging.DEBUG):
            logger.debug(
                "unexpected error traceback: %s",
                redact("".join(traceback.format_exception(exc))),
            )
        return EXIT_RUNTIME


async def _build_unless_stopped(
    config: AppConfig, session_factory: SessionFactory, stop: asyncio.Event
) -> Components | None:
    """构造组件；构造期间收到停止信号则取消构造（``build_components`` 会回收已登记的资源）
    并返回 None。"""
    loop = asyncio.get_running_loop()
    build_task = loop.create_task(build_components(config, session_factory=session_factory))
    stop_task = loop.create_task(stop.wait())
    try:
        await asyncio.wait({build_task, stop_task}, return_when=asyncio.FIRST_COMPLETED)
        if build_task.done():
            return build_task.result()
        build_task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await build_task
        return None
    finally:
        stop_task.cancel()
        if not build_task.done():
            build_task.cancel()


def run() -> None:
    sys.exit(asyncio.run(main()))


__all__ = [
    "DB_FILENAME",
    "Components",
    "StartupError",
    "build_arrs",
    "build_components",
    "check_same_filesystem",
    "check_state_writable",
    "main",
    "run",
    "serve",
    "validate_startup",
    "validate_umask",
]
