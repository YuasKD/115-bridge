"""115 会话层（T08）：唯一 import p115client 的文件（不变量 1）。

职责：
- 登录：只用 TV 端扫码，cookie 存 0600 文件，只在登录成功时写，日志只记长度和登录端（不变量 6）。
- 唯一性：进程内文件锁加跨容器 lease。
- 限速与熔断：所有 115 API 请求过令牌桶；风控类错误全局熔断，1 分钟起翻倍，最长 1 小时。
- 错误分类：依据 ``docs/error-codes.md`` 第 5 节，顺序是网络、HTTP 状态、115 原始码、异常类兜底。
- 暂停期间（认证失效、需人工验证、熔断、lease 丢失）调用任何接口都**快速失败**：
  抛对应的全局异常，不发请求，不计任务重试（不变量 4）。
- 每次 115 请求都有超时（经注入的 Clock 计时），超时按网络问题处理。
- 扫码相关请求出错只抛异常，不改全局状态；换 client 或状态切换之前发出的请求，
  迟到的结果不得改变新状态（状态代号 epoch）。
- 永不调用任何删除云端内容的接口（不变量 2）。本文件没有删除类调用。
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import logging
import os
import re
import socket
import ssl
import struct
import sys
import urllib.error
import uuid
import zlib
from collections.abc import Awaitable, Callable, Mapping
from dataclasses import dataclass, replace
from datetime import datetime
from enum import Enum, StrEnum
from pathlib import Path
from typing import Any, NoReturn, Protocol, TypeVar

import httpx
import qrcode  # type: ignore[import-untyped]
from p115client import P115Client, check_response
from p115client.exception import (
    P115AccessTokenError,
    P115AuthenticationError,
    P115BusyOSError,
    P115DataError,
    P115NoSpaceError,
    P115OSError,
)
from p115client.util import unescape_115_charref

from bridge.clock import Clock
from bridge.cloud.protocol import (
    AuthExpired,
    CloudEntry,
    CloudError,
    CloudNotFound,
    CloudSpace,
    DownloadLink,
    NetworkError,
    OfflinePage,
    OfflineStat,
    OfflineTask,
    QrStatus,
    QrToken,
    Quota,
    QuotaExhausted,
    RiskControl,
    ServerError,
    SubmitRejected,
    VerifyRequired,
)
from bridge.cloud.ratelimit import CircuitBreaker, TokenBucket
from bridge.config import Settings
from bridge.db.repo import TaskRepo
from bridge.log import log_event, redact
from bridge.notify import Notifier
from bridge.torrent.errors import InvalidMagnetError
from bridge.torrent.magnet import normalize_btih, parse_magnet

logger = logging.getLogger(__name__)

_T = TypeVar("_T")

# ---------------------------------------------------------------- 常量

LOGIN_APP = "tv"
COOKIE_FILENAME = "115-tv.cookie"
LOCK_FILENAME = "session.lock"

CACHE_TTL_SECONDS = 20.0
HEARTBEAT_SECONDS = 300.0
# 网盘空间缓存时长；心跳每 5 分钟顺带刷新一次（与「5 分钟内有成功调用就跳过」独立计时）
SPACE_CACHE_SECONDS = 300.0
GIB = 1024**3
NETWORK_ALERT_SECONDS = 30 * 60.0
BUSY_BACKOFF_SECONDS: tuple[float, ...] = (1.0, 2.0, 4.0)
LEASE_TTL_SECONDS = 90
LEASE_RENEW_SECONDS = 30.0
QR_POLL_SECONDS = 3.0
QR_TTL_SECONDS = 300.0
QR_RETRY_SECONDS = 60.0
DIR_PAGE_SIZE = 1000
DOWNLOAD_USER_AGENT = "Mozilla/5.0"

# 每次 115 请求的超时（经注入的 Clock 计时）。超时按网络问题处理。
REQUEST_TIMEOUT_SECONDS = 30.0
# 扫码状态接口是长轮询（服务端会挂起到状态变化或自己超时），给更长的上限。
QR_POLL_TIMEOUT_SECONDS = 90.0
# 扫码轮询的活跃窗口：每次推送尝试或页面访问后轮询这么久，之后停止。
QR_ACTIVE_WINDOW_SECONDS = 600.0
# 窗口外两次「推送尝试」的间隔（与 Notifier 的 30 分钟去重同一节奏）。
QR_PUSH_INTERVAL_SECONDS = 1800.0
QR_IDLE_TICK_SECONDS = 5.0
# 同一 hash 连续得到未知错误码达到这个次数，按提交被拒处理。
SUBMIT_UNKNOWN_LIMIT = 3
# 后台协程异常退出后重启前的等待。
TASK_RESTART_SECONDS = 5.0

# ------------------------------------------------ 错误码表（error-codes.md 第 5 节）

AUTH_CODES = frozenset({99, 990001, 40101017, 40101032, 40101033, 40101035, 40101037, 40101038})
OPEN_AUTH_CODES = frozenset(
    {40140116, 40140119, 40140120, 40140121, 40140122, 40140123, 40140124, 40140125, 40140126}
)
RISK_CODES = frozenset({590075, 40140117})
VERIFY_CODES = frozenset({911})
BUSY_CODES = frozenset({990005, 990009, 990019, 51012})
QUOTA_CODES = frozenset({10010})
SPACE_CODES = frozenset({91005, 2})
REJECT_CODES = frozenset({10004, 50038})
EXISTS_CODES = frozenset({10008})
NOT_FOUND_CODES = frozenset({20018, 50015, 90008, 430004, 70005, 231011})
# 参数错误（1001、990002、40100000）与接口失效（980006）：多半是组件缺陷，立即告警
PARAM_CODES = frozenset({1001, 990002, 40100000, 980006})
# 取下载链接时视为「云端不存在」的码（人已拍板）：违规内容、文件过大
LINK_NOT_FOUND_CODES = frozenset({50038, 50028})

_CODE_KEYS = ("errno", "errNo", "errcode", "errCode", "code", "msg_code")
_MSG_KEYS = ("msg", "error_msg", "error", "message")


class ErrorKind(Enum):
    AUTH = "auth"
    VERIFY = "verify"
    RISK = "risk"
    NETWORK = "network"
    SERVER = "server"
    BUSY = "busy"
    QUOTA = "quota"
    REJECT = "reject"
    EXISTS = "exists"
    NOT_FOUND = "not_found"
    PARAM = "param"
    UNPARSEABLE = "unparseable"  # 响应不是 JSON，需要探测登录状态再定


@dataclass(frozen=True, slots=True)
class Classified:
    kind: ErrorKind
    code: int | None = None
    message: str = ""
    http_status: int | None = None


class CodedError(Exception):
    """会话层内部：把逐条结果里的 115 错误码交给同一套分类。"""

    def __init__(self, code: int | None, message: str) -> None:
        super().__init__(f"code={code} {message}")
        self.code = code
        self.message = message


def _to_int(value: Any) -> int | None:
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value
    if isinstance(value, str) and value.strip().lstrip("-").isdigit():
        return int(value.strip())
    return None


def extract_code(resp: Mapping[str, Any]) -> int | None:
    """从 115 原始响应里取错误码，键的顺序同 p115client.check_response。"""
    for key in _CODE_KEYS:
        if key in resp and resp[key] not in (None, "", 0):
            return _to_int(resp[key])
    return None


def extract_message(resp: Mapping[str, Any]) -> str:
    for key in _MSG_KEYS:
        value = resp.get(key)
        if isinstance(value, str) and value:
            return value
    return ""


def _code_of_exception(exc: BaseException) -> tuple[int | None, str]:
    """取 115 原始码：对 P115OSError 用 ``exc["errno"]``（``exc.errno`` 是 POSIX 码，不能用）。"""
    if isinstance(exc, CodedError):
        return exc.code, exc.message
    if isinstance(exc, P115OSError):
        message = exc.message
        if isinstance(message, Mapping):
            return extract_code(message), extract_message(message)
    return None, ""


def _is_network_error(exc: BaseException) -> bool:
    if isinstance(exc, urllib.error.HTTPError):
        return False
    if isinstance(
        exc, (TimeoutError, ConnectionError, ssl.SSLError, socket.gaierror, httpx.TransportError)
    ):
        return True
    # p115client 默认后端抛 urllib3_future.exceptions.*，既不是 OSError 也不是内置 TimeoutError
    for cls in type(exc).__mro__:
        if cls.__module__.startswith(("urllib3_future.exceptions", "urllib3.exceptions")):
            return True
    return False


def classify_exception(exc: BaseException) -> Classified:
    """按 docs/error-codes.md 第 5 节的顺序分类：网络、HTTP 状态、115 原始码、异常类兜底。

    ``P115DataError``（响应不是 JSON）返回 UNPARSEABLE，由调用方探测登录状态后再定。
    """
    # 1. 网络
    if _is_network_error(exc):
        return Classified(ErrorKind.NETWORK, message=type(exc).__name__)
    # 2. HTTP 状态
    if isinstance(exc, urllib.error.HTTPError):
        if exc.code == 405:
            return Classified(ErrorKind.RISK, http_status=405, message="HTTP 405")
        return Classified(ErrorKind.SERVER, http_status=exc.code, message=f"HTTP {exc.code}")
    # 3. 115 原始码（911 必须排在认证之前）
    code, message = _code_of_exception(exc)
    if code is not None:
        kind = classify_code(code)
        if kind is not None:
            return Classified(kind, code=code, message=message)
    # 4. 异常类兜底
    if isinstance(exc, (P115AuthenticationError, P115AccessTokenError)):
        return Classified(ErrorKind.AUTH, code=code, message=message)
    if isinstance(exc, P115NoSpaceError):
        return Classified(ErrorKind.QUOTA, code=code, message=message)
    if isinstance(exc, FileNotFoundError):
        return Classified(ErrorKind.NOT_FOUND, code=code, message=message)
    if isinstance(exc, P115BusyOSError):
        return Classified(ErrorKind.BUSY, code=code, message=message)
    if isinstance(exc, P115DataError):
        return Classified(ErrorKind.UNPARSEABLE, code=code, message=message)
    # 其余（含未知码的 P115OSError、解析失败等未知结构）一律按服务端异常
    return Classified(ErrorKind.SERVER, code=code, message=message or f"{type(exc).__name__}")


def classify_code(code: int) -> ErrorKind | None:
    """115 原始码到类别；表里没有返回 None。"""
    if code in VERIFY_CODES:
        return ErrorKind.VERIFY
    if code in AUTH_CODES or code in OPEN_AUTH_CODES:
        return ErrorKind.AUTH
    if code in RISK_CODES:
        return ErrorKind.RISK
    if code in BUSY_CODES:
        return ErrorKind.BUSY
    if code in EXISTS_CODES:
        return ErrorKind.EXISTS
    if code in QUOTA_CODES or code in SPACE_CODES:
        return ErrorKind.QUOTA
    if code in REJECT_CODES:
        return ErrorKind.REJECT
    if code in NOT_FOUND_CODES:
        return ErrorKind.NOT_FOUND
    if code in PARAM_CODES:
        return ErrorKind.PARAM
    return None


# ---------------------------------------------------------------- 状态快照


class SessionState(StrEnum):
    OK = "ok"
    AUTH_EXPIRED = "auth_expired"
    VERIFY_REQUIRED = "verify_required"
    CIRCUIT_OPEN = "circuit_open"
    LEASE_LOST = "lease_lost"  # 失去跨容器 lease，本进程不得再碰 115


@dataclass(frozen=True, slots=True)
class SessionStatus:
    """只读状态快照，供 Web 页面（T15）展示。不含任何凭据。"""

    state: SessionState
    since: datetime  # 当前状态的开始时间
    last_success: datetime | None  # 最近一次成功的 115 API 调用
    circuit_until: datetime | None  # 熔断解除时间；未熔断为 None
    circuit_level: int  # 熔断已触发次数（0 表示无）
    login_app: str  # 恒为 "tv"
    cookie_present: bool
    qr_pending: bool  # 是否有等待扫码的二维码
    quota: Quota | None  # 最近一次离线任务列表读到的配额
    degraded_since: datetime | None  # 网络或服务端连续失败的起点
    detail: str  # 进入当前暂停状态的原因，不含密钥
    space: CloudSpace | None = None  # 最近一次读到的 115 网盘空间；未读到为 None


# ---------------------------------------------------------------- 唯一性


class AlreadyRunning(Exception):
    """同一数据目录已被另一个进程持有 flock。"""


class LeaseHeld(Exception):
    """跨容器 lease 被别的 owner 持有。"""


class FileLockBackend(Protocol):
    def lock(self, fd: int) -> None:
        """加独占非阻塞锁；已被占用时抛 OSError。"""
        ...

    def unlock(self, fd: int) -> None: ...


class DefaultFileLockBackend:
    """POSIX 用 fcntl.flock，Windows 用 msvcrt.locking。"""

    def lock(self, fd: int) -> None:
        if sys.platform == "win32":
            import msvcrt

            os.lseek(fd, 0, os.SEEK_SET)
            msvcrt.locking(fd, msvcrt.LK_NBLCK, 1)
        else:
            import fcntl

            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)

    def unlock(self, fd: int) -> None:
        if sys.platform == "win32":
            import msvcrt

            os.lseek(fd, 0, os.SEEK_SET)
            msvcrt.locking(fd, msvcrt.LK_UNLCK, 1)
        else:
            import fcntl

            fcntl.flock(fd, fcntl.LOCK_UN)


class ProcessLock:
    """对 ``paths.state`` 下的锁文件加 flock，拿不到抛 :class:`AlreadyRunning`。"""

    def __init__(self, path: Path, backend: FileLockBackend | None = None) -> None:
        self._path = path
        self._backend: FileLockBackend = backend or DefaultFileLockBackend()
        self._fd: int | None = None

    def acquire(self) -> None:
        if self._fd is not None:
            return
        self._path.parent.mkdir(parents=True, exist_ok=True)
        fd = os.open(self._path, os.O_RDWR | os.O_CREAT, 0o600)
        try:
            self._backend.lock(fd)
        except OSError as exc:
            os.close(fd)
            raise AlreadyRunning(f"another process holds {self._path}") from exc
        self._fd = fd

    def release(self) -> None:
        if self._fd is None:
            return
        with contextlib.suppress(OSError):
            self._backend.unlock(self._fd)
        os.close(self._fd)
        self._fd = None


# ---------------------------------------------------------------- cookie 文件


def write_cookie_file(path: Path, cookie: str) -> None:
    """原子写入，权限 0600。只在登录成功时调用。"""
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(path.name + ".tmp")
    fd = os.open(tmp, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
    try:
        with os.fdopen(fd, "w", encoding="latin-1") as f:
            f.write(cookie)
            f.flush()
            os.fsync(f.fileno())
    except BaseException:
        with contextlib.suppress(OSError):
            tmp.unlink()
        raise
    os.replace(tmp, path)
    with contextlib.suppress(OSError):
        os.chmod(path, 0o600)
    _fsync_dir(path.parent)


def _fsync_dir(directory: Path) -> None:
    """POSIX 上把目录项落盘，使 rename 在断电后也不丢；Windows 不支持，跳过。"""
    if sys.platform == "win32":
        return
    with contextlib.suppress(OSError):
        fd = os.open(directory, os.O_RDONLY)
        try:
            os.fsync(fd)
        finally:
            os.close(fd)


_UID_RE = re.compile(r"(?:^|;)\s*UID=\S")


def has_uid(cookie: str) -> bool:
    """cookie 必须含 ``UID=``：缺了 p115client 会改走 ``/open/`` 路径并派生 Open token。"""
    return _UID_RE.search(cookie) is not None


def read_cookie_file(path: Path) -> str | None:
    try:
        text = path.read_text(encoding="latin-1").strip()
    except FileNotFoundError:
        return None
    return text or None


# ---------------------------------------------------------------- 会话


@dataclass(slots=True)
class _Qr:
    uid: str
    payload: dict[str, Any]
    created: float
    png: bytes


class Session:
    """真实 ``CloudClient`` 实现。"""

    def __init__(
        self,
        *,
        state_dir: Path,
        repo: TaskRepo,
        notifier: Notifier,
        clock: Clock,
        rate_per_second: float = 1,
        burst: int = 3,
        download_app: str = "android",
        p115_cls: Any = None,
        lock_backend: FileLockBackend | None = None,
        owner: str | None = None,
        space_min_gib: int = 100,
        auto_qr: bool = True,
        cookie_path: Path | None = None,
    ) -> None:
        self._state_dir = state_dir
        self._cookie_path = cookie_path or state_dir / COOKIE_FILENAME
        self._repo = repo
        self._notifier = notifier
        self._clock = clock
        self._download_app = download_app
        self._p115: Any = p115_cls if p115_cls is not None else P115Client
        self._lock = ProcessLock(state_dir / LOCK_FILENAME, lock_backend)
        # lease 持有者：每次进程启动生成新的随机 id，与 hostname、pid、/config 无关，
        # 保证任意两个进程互斥；被强杀后新进程要等旧 lease 过期（TTL）才能取得。
        self._owner = owner or uuid.uuid4().hex
        self._space_min_bytes = space_min_gib * GIB
        self._auto_qr = auto_qr

        self._bucket = TokenBucket(rate_per_second, burst, clock)
        self._breaker = CircuitBreaker(clock)

        self._client: Any = None
        self._cookie_len = 0

        self._lease_ok = True
        self._auth_expired = False
        self._verify_required = False
        self._detail = ""
        self._started_at = clock.now()
        self._since: dict[SessionState, datetime] = {}
        self._last_success_mono: float | None = None
        self._last_success_wall: datetime | None = None
        self._fail_since_mono: float | None = None
        self._fail_since_wall: datetime | None = None
        self._fail_alerted = False
        self._quota: Quota | None = None
        self._space: CloudSpace | None = None
        self._space_stamp: float | None = None  # 缓存时间（monotonic）
        self._space_lock = asyncio.Lock()
        self._space_low = False

        self._cache: dict[tuple[int, int], tuple[float, OfflinePage]] = {}
        self._cache_locks: dict[tuple[int, int], asyncio.Lock] = {}

        # 状态代号：每次进入或退出暂停类状态、更换 client 都加 1。请求发出前记下，
        # 返回后若已变化，则它的结果不得解除 911、熔断等状态。
        self._epoch = 0
        self._unknown_submit: dict[str, int] = {}  # hash -> 连续未知码次数（只在内存里）

        self._qr: _Qr | None = None
        self._qr_task: asyncio.Task[None] | None = None
        self._qr_active_until = 0.0  # 扫码轮询活跃窗口的结束（monotonic）
        self._last_push_mono: float | None = None  # 最近一次推送尝试
        self._finish_lock = asyncio.Lock()
        self._finished_uid: str | None = None
        self._tasks: list[asyncio.Task[None]] = []

    @classmethod
    def from_settings(
        cls,
        settings: Settings,
        repo: TaskRepo,
        notifier: Notifier,
        clock: Clock,
        **kwargs: Any,
    ) -> Session:
        return cls(
            state_dir=Path(settings.paths.state),
            repo=repo,
            notifier=notifier,
            clock=clock,
            rate_per_second=settings.api.rate_per_second,
            burst=settings.api.burst,
            download_app=settings.fetch.download_app,
            space_min_gib=settings.cloud.space_min_gib,
            **kwargs,
        )

    def __repr__(self) -> str:
        return f"Session(state={self.state.value}, login_app={LOGIN_APP!r})"

    # ------------------------------------------------------------ 状态

    @property
    def state(self) -> SessionState:
        if not self._lease_ok:
            return SessionState.LEASE_LOST
        if self._auth_expired:
            return SessionState.AUTH_EXPIRED
        if self._verify_required:
            return SessionState.VERIFY_REQUIRED
        if self._breaker.is_open():
            return SessionState.CIRCUIT_OPEN
        return SessionState.OK

    def status(self) -> SessionStatus:
        state = self.state
        return SessionStatus(
            state=state,
            since=self._since.get(state, self._started_at),
            last_success=self._last_success_wall,
            circuit_until=self._breaker.open_until,
            circuit_level=self._breaker.level,
            login_app=LOGIN_APP,
            cookie_present=self._client is not None,
            qr_pending=self._qr is not None,
            quota=self._quota,
            degraded_since=self._fail_since_wall,
            detail=self._detail if state is not SessionState.OK else "",
            space=self._space,
        )

    def current_qr(self) -> QrToken | None:
        """当前等待扫码的二维码，供 Web 页面展示。访问一次就把扫码轮询窗口续 10 分钟；
        已过本地有效期的旧码不返回（后台循环会马上换新码）。"""
        self._touch_qr_window()
        qr = self._qr
        if qr is None or self._clock.monotonic() - qr.created >= QR_TTL_SECONDS:
            return None
        return QrToken(uid=qr.uid, image_png=qr.png)

    def _touch_qr_window(self) -> None:
        self._qr_active_until = self._clock.monotonic() + QR_ACTIVE_WINDOW_SECONDS

    def _mark(self, state: SessionState) -> None:
        self._since[state] = self._clock.now()

    # ------------------------------------------------------------ 生命周期

    async def open(self) -> None:
        """取进程锁与 lease，读 cookie。拿不到锁或 lease 抛异常，调用方应退出。"""
        self._lock.acquire()
        try:
            ok = await self._repo.acquire_lease(self._owner, self._clock.now(), LEASE_TTL_SECONDS)
        except BaseException:
            self._lock.release()
            raise
        if not ok:
            self._lock.release()
            raise LeaseHeld("another bridge instance holds the 115 lease")
        self._lease_ok = True
        log_event(logger, logging.INFO, "lease acquired", owner=self._owner[:8])
        try:
            cookie = read_cookie_file(self._cookie_path)
            if cookie and not has_uid(cookie):
                log_event(logger, logging.ERROR, "cookie has no UID", cookie_len=len(cookie))
                await self._enter_auth_expired("cookie file has no UID")
            elif cookie:
                self._client = self._make_client(cookie)
                self._epoch += 1
                log_event(
                    logger,
                    logging.INFO,
                    "cookie loaded",
                    cookie_len=len(cookie),
                    login_app=LOGIN_APP,
                )
            else:
                await self._enter_auth_expired("no cookie file")
        except BaseException:
            # 半途失败（如 cookie 文件无权限读取）：释放 lease 与 flock 后再抛出。
            self._lease_ok = False
            with contextlib.suppress(Exception):
                await self._repo.release_lease(self._owner)
            self._lock.release()
            raise

    async def start(self) -> None:
        """open 加启动心跳与 lease 续约协程。"""
        await self.open()
        loop = asyncio.get_running_loop()
        self._tasks = [
            loop.create_task(self._supervised("heartbeat", self.heartbeat_loop)),
            loop.create_task(self._supervised("lease", self.lease_loop)),
        ]

    async def close(self) -> None:
        for task in [*self._tasks, self._qr_task]:
            if task is not None and not task.done():
                task.cancel()
        for task in [*self._tasks, self._qr_task]:
            if task is not None:
                with contextlib.suppress(asyncio.CancelledError, Exception):
                    await task
        self._tasks = []
        self._qr_task = None
        if self._lease_ok:
            with contextlib.suppress(Exception):
                await self._repo.release_lease(self._owner)
        self._lock.release()

    async def _supervised(self, name: str, factory: Callable[[], Awaitable[None]]) -> None:
        """后台循环意外抛异常时记日志、告警并在等待后重启，不让它静默死掉。"""
        while True:
            try:
                await factory()
                return
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # noqa: BLE001
                log_event(
                    logger,
                    logging.ERROR,
                    "background task crashed, restarting",
                    task=name,
                    error_type=type(exc).__name__,
                )
                await self._alert(
                    f"task-crash-{name}",
                    f"115 Bridge: 后台任务 {name} 异常退出（{type(exc).__name__}），已自动重启。",
                )
                await self._clock.sleep(TASK_RESTART_SECONDS)

    def _make_client(self, cookie: str) -> Any:
        return self._p115(cookies=cookie)

    # ------------------------------------------------------------ lease

    async def lease_loop(self) -> None:
        """每 30 秒续约；可由调度器启动。"""
        while True:
            await self._clock.sleep(LEASE_RENEW_SECONDS)
            await self.renew_lease_once()

    async def renew_lease_once(self) -> bool:
        """续约一次。失败即进入暂停（lease_lost）并告警；之后每次再尝试重新取得。"""
        now = self._clock.now()
        try:
            if self._lease_ok:
                ok = await self._repo.renew_lease(self._owner, now, LEASE_TTL_SECONDS)
            else:
                ok = await self._repo.acquire_lease(self._owner, now, LEASE_TTL_SECONDS)
        except Exception as exc:  # noqa: BLE001
            log_event(logger, logging.WARNING, "lease call failed", error_type=type(exc).__name__)
            ok = False
        if ok and not self._lease_ok:
            self._lease_ok = True
            self._epoch += 1
            log_event(logger, logging.INFO, "lease regained")
            await self._recovered("lease", "115 Bridge: 已重新取得实例 lease，恢复访问 115。")
        elif not ok and self._lease_ok:
            self._lease_ok = False
            self._epoch += 1
            self._detail = "lease lost"
            self._mark(SessionState.LEASE_LOST)
            log_event(logger, logging.ERROR, "lease lost, pausing all 115 access")
            await self._alert("lease", "115 Bridge: 失去实例 lease，已暂停所有 115 访问。")
        return ok

    # ------------------------------------------------------------ 通知

    async def _alert(self, kind: str, text: str) -> None:
        try:
            await self._notifier.alert(kind, text)
        except Exception as exc:  # noqa: BLE001
            log_event(logger, logging.WARNING, "alert failed", error_type=type(exc).__name__)

    async def _recovered(self, kind: str, text: str) -> None:
        try:
            await self._notifier.recovered(kind, text)
        except Exception as exc:  # noqa: BLE001
            log_event(logger, logging.WARNING, "notice failed", error_type=type(exc).__name__)

    # ------------------------------------------------------------ 暂停与恢复

    def _gate(self, probe: bool = False) -> None:
        """暂停期间快速失败：不发请求，抛全局异常（不变量 4）。"""
        if not self._lease_ok:
            raise ServerError("session paused: lease lost")
        if self._auth_expired:
            raise AuthExpired("session paused: authentication expired, waiting for QR scan")
        if self._verify_required and not probe:
            raise VerifyRequired("session paused: 115 requires manual account verification")
        if self._breaker.is_open():
            raise RiskControl(
                f"session paused: circuit open, retry in {self._breaker.remaining():.0f}s"
            )

    async def _enter_auth_expired(self, reason: str) -> None:
        if self._auth_expired:
            return
        self._auth_expired = True
        self._epoch += 1
        self._detail = reason
        self._mark(SessionState.AUTH_EXPIRED)
        self._cache.clear()
        log_event(logger, logging.ERROR, "auth expired, waiting for QR scan", reason=reason)
        await self._alert(
            "auth", "115 Bridge: 115 登录失效，已暂停所有 115 操作，请扫码登录（TV 端）。"
        )
        self._ensure_qr_task()

    async def _enter_verify_required(self, message: str) -> None:
        if self._verify_required:
            return
        self._verify_required = True
        self._epoch += 1
        self._detail = "115 requires account verification (code 911)"
        self._mark(SessionState.VERIFY_REQUIRED)
        log_event(logger, logging.ERROR, "verification required", detail=redact(message)[:200])
        await self._alert(
            "verify", "115 Bridge: 115 要求验证账号（911），已暂停。请到 115 网页完成验证。"
        )

    async def _trip_circuit(self, c: Classified) -> None:
        was_open = self._breaker.is_open()
        seconds = self._breaker.trip()
        if was_open:
            return
        self._epoch += 1
        self._detail = f"risk control (code={c.code}, http={c.http_status})"
        self._mark(SessionState.CIRCUIT_OPEN)
        log_event(
            logger,
            logging.ERROR,
            "risk control, circuit open",
            code=c.code,
            http_status=c.http_status,
            backoff_seconds=seconds,
            circuit_level=self._breaker.level,
        )
        await self._alert(
            "risk",
            f"115 Bridge: 115 风控或限流，已熔断 {seconds / 60:.0f} 分钟"
            f"（第 {self._breaker.level} 次）。",
        )

    async def _note_failure(self, kind: str) -> None:
        """网络或服务端连续失败满 30 分钟才告警（Notifier 再做 30 分钟去重）。"""
        now = self._clock.monotonic()
        if self._fail_since_mono is None:
            self._fail_since_mono = now
            self._fail_since_wall = self._clock.now()
        if now - self._fail_since_mono >= NETWORK_ALERT_SECONDS:
            self._fail_alerted = True
            await self._alert(
                "network", f"115 Bridge: 115 连续 30 分钟以上无法访问（{kind}），保持原状重试中。"
            )

    async def _on_success(self, *, clear_states: bool = True) -> None:
        """一次请求成功。``clear_states`` 为假（请求发出后状态已切换）时，
        只记录成功，不解除 911 与熔断。"""
        self._last_success_mono = self._clock.monotonic()
        self._last_success_wall = self._clock.now()
        self._fail_since_mono = None
        self._fail_since_wall = None
        if self._fail_alerted:
            self._fail_alerted = False
            await self._recovered("network", "115 Bridge: 115 访问已恢复。")
        if not clear_states:
            return
        if self._verify_required:
            self._verify_required = False
            self._epoch += 1
            log_event(logger, logging.INFO, "verification cleared")
            await self._recovered("verify", "115 Bridge: 账号验证已通过，已恢复访问。")
        if self._breaker.note_success():
            log_event(logger, logging.INFO, "circuit level reset")
            await self._recovered("risk", "115 Bridge: 风控熔断已解除，已恢复访问。")

    # ------------------------------------------------------------ 调用封装

    async def _bounded(self, aw: Awaitable[_T], seconds: float) -> _T:
        """给一次请求加超时。计时走注入的 Clock（测试里用假时钟），超时抛 TimeoutError，
        由分类按网络问题处理。"""
        work: asyncio.Future[_T] = asyncio.ensure_future(aw)
        timer: asyncio.Future[None] = asyncio.ensure_future(self._clock.sleep(seconds))
        try:
            waiting: set[asyncio.Future[Any]] = {work, timer}
            await asyncio.wait(waiting, return_when=asyncio.FIRST_COMPLETED)
        except BaseException:
            work.cancel()
            timer.cancel()
            await asyncio.gather(work, timer, return_exceptions=True)
            raise
        if work.done():
            timer.cancel()
            await asyncio.gather(timer, return_exceptions=True)
            return work.result()
        work.cancel()
        await asyncio.gather(work, return_exceptions=True)
        raise TimeoutError(f"request timed out after {seconds:g}s")

    async def _probe_login(self, client: Any) -> ErrorKind:
        """响应不是 JSON 时探测：JSON 且 state 为假才算认证失效，非 JSON 按风控。"""

        def parse(_: Any, content: bytes, /) -> str:
            try:
                data = json.loads(content)
            except Exception:  # noqa: BLE001
                return "nonjson"
            if not isinstance(data, dict):
                return "nonjson"
            return "ok" if data.get("state") else "unauth"

        await self._bucket.acquire()
        try:
            result = await self._bounded(
                client.login_status(parse=parse, async_=True), REQUEST_TIMEOUT_SECONDS
            )
        except Exception as exc:  # noqa: BLE001
            return classify_exception(exc).kind
        if result == "unauth":
            return ErrorKind.AUTH
        if result == "nonjson":
            return ErrorKind.RISK
        return ErrorKind.SERVER

    async def _call(
        self,
        op: str,
        fn: Callable[[Any], Awaitable[_T]],
        *,
        qr: bool = False,
        probe: bool = False,
        submit_key: str | None = None,
        on_exists: Callable[[], _T] | None = None,
        not_found: Callable[[], _T] | None = None,
        extra_not_found: frozenset[int] = frozenset(),
        request_timeout: float | None = REQUEST_TIMEOUT_SECONDS,
    ) -> _T:
        """所有 115 API 请求的唯一入口：快速失败、令牌桶、超时、分类、熔断。

        - ``qr=True``：扫码登录相关请求。不受认证类暂停限制，不需要 cookie；出错只抛异常给
          调用方，不改任何全局状态（不进认证失效、不熔断、不进需人工验证）；成功也不算
          「成功调用」。但 lease 丢失时不得发请求。
        - ``probe=True``：需人工验证状态下的探测，允许穿过 verify 暂停。
        - ``submit_key``：离线提交上下文（值为 hash），REJECT 类码映射为 SubmitRejected，
          同一 hash 连续未知码计数。
        - ``on_exists``：任务已存在（10008）时的返回值（视为成功）。
        - ``not_found``：云端不存在时的返回值（否则抛 CloudNotFound）。
        - ``extra_not_found``：该接口上额外视为「云端不存在」的码。
        - ``request_timeout``：单次请求超时；为 None 表示 ``fn`` 内部自己对每个请求加超时。
        """
        busy_attempt = 0
        while True:
            client: Any = None
            if qr:
                if not self._lease_ok:
                    raise ServerError("session paused: lease lost")
            else:
                self._gate(probe)
            await self._bucket.acquire()
            if not qr:
                self._gate(probe)  # 排队期间状态可能已变
                client = self._client
                if client is None:
                    raise AuthExpired("no 115 cookie, login required")
            epoch0 = self._epoch
            try:
                if request_timeout is None:
                    result = await fn(client)
                else:
                    result = await self._bounded(fn(client), request_timeout)
            except asyncio.CancelledError:
                raise
            except CloudNotFound:
                await self._succeeded(qr, epoch0)
                raise
            except (SubmitRejected, QuotaExhausted):
                raise
            except CloudError as exc:
                if isinstance(exc, ServerError) and self._owns(client, qr):
                    await self._note_failure("server")
                raise
            except Exception as exc:  # noqa: BLE001
                c = classify_exception(exc)
                if c.kind is ErrorKind.UNPARSEABLE and client is not None:
                    c = Classified(await self._probe_login(client), message="non-JSON response")
                if c.code is not None and c.code in extra_not_found:
                    c = replace(c, kind=ErrorKind.NOT_FOUND)
                log_event(
                    logger,
                    logging.WARNING,
                    "115 call failed",
                    op=op,
                    kind=c.kind.value,
                    code=c.code,
                    http_status=c.http_status,
                    error_type=type(exc).__name__,
                    detail=redact(c.message)[:200],
                )
                known = c.code is not None and (
                    classify_code(c.code) is not None or c.code in extra_not_found
                )
                if submit_key is not None and known:
                    self._unknown_submit.pop(submit_key, None)  # 得到已知码，计数清零
                if c.kind is ErrorKind.BUSY:
                    if busy_attempt < len(BUSY_BACKOFF_SECONDS):
                        await self._clock.sleep(BUSY_BACKOFF_SECONDS[busy_attempt])
                        busy_attempt += 1
                        continue
                    if self._owns(client, qr):
                        await self._note_failure("busy")
                    raise ServerError(f"{op}: 115 busy, retries exhausted") from None
                if c.kind is ErrorKind.EXISTS and on_exists is not None:
                    await self._succeeded(qr, epoch0)
                    return on_exists()
                if c.kind is ErrorKind.NOT_FOUND:
                    await self._succeeded(qr, epoch0)
                    if not_found is not None:
                        return not_found()
                await self._raise_classified(
                    op,
                    c,
                    submit_key=submit_key,
                    apply_state=self._owns(client, qr),
                )
            else:
                if submit_key is not None:
                    self._unknown_submit.pop(submit_key, None)
                await self._succeeded(qr, epoch0)
                return result

    def _owns(self, client: Any, qr: bool) -> bool:
        """这次请求的结果是否有资格改全局状态：扫码请求不行；换过 client 后旧请求也不行。"""
        return not qr and client is self._client

    async def _succeeded(self, qr: bool, epoch0: int) -> None:
        if not qr:
            await self._on_success(clear_states=epoch0 == self._epoch)

    async def _raise_classified(
        self, op: str, c: Classified, *, submit_key: str | None, apply_state: bool
    ) -> NoReturn:
        """把分类结果转成异常。``apply_state`` 为假（扫码请求或旧 client 的迟到结果）时
        只抛异常，不进入暂停、不熔断、不计网络失败。"""
        match c.kind:
            case ErrorKind.AUTH:
                if apply_state:
                    await self._enter_auth_expired(f"{op}: code={c.code}")
                raise AuthExpired(f"{op}: authentication expired (code={c.code})") from None
            case ErrorKind.VERIFY:
                if apply_state:
                    await self._enter_verify_required(c.message)
                raise VerifyRequired(f"{op}: account verification required") from None
            case ErrorKind.RISK:
                if apply_state:
                    await self._trip_circuit(c)
                raise RiskControl(
                    f"{op}: risk control (code={c.code}, http={c.http_status})"
                ) from None
            case ErrorKind.NETWORK:
                if apply_state:
                    await self._note_failure("network")
                raise NetworkError(f"{op}: {c.message}") from None
            case ErrorKind.QUOTA:
                raise QuotaExhausted(f"{op}: quota or space exhausted (code={c.code})") from None
            case ErrorKind.REJECT if submit_key is not None:
                raise SubmitRejected(c.code, c.message) from None
            case ErrorKind.NOT_FOUND:
                raise CloudNotFound(f"{op}: not found (code={c.code})") from None
            case ErrorKind.PARAM:
                await self._alert(
                    "api-param",
                    f"115 Bridge: 115 接口返回参数错误或接口失效（code={c.code}，{op}），"
                    "多半是组件缺陷，请检查。",
                )
                if apply_state:
                    await self._note_failure("server")
                raise ServerError(f"{op}: parameter error (code={c.code})") from None
            case _:
                if (
                    submit_key is not None
                    and c.code is not None
                    and c.http_status is None
                    and classify_code(c.code) is None
                ):
                    await self._submit_unknown_code(submit_key, c)
                if c.http_status is not None and 400 <= c.http_status < 500:
                    await self._alert(
                        "api-4xx", f"115 Bridge: 115 接口返回 HTTP {c.http_status}，含义未知。"
                    )
                if apply_state:
                    await self._note_failure("server")
                raise ServerError(
                    f"{op}: server error (code={c.code}, http={c.http_status})"
                ) from None

    async def _submit_unknown_code(self, key: str, c: Classified) -> None:
        """提交时的未知错误码：同一 hash 连续第 3 次抛 SubmitRejected 并告警；
        前两次由调用方按服务端异常抛出。计数只在内存里，重启后重新数。"""
        n = self._unknown_submit.get(key, 0) + 1
        if n < SUBMIT_UNKNOWN_LIMIT:
            self._unknown_submit[key] = n
            return
        self._unknown_submit.pop(key, None)
        log_event(logger, logging.ERROR, "submit rejected by repeated unknown code", code=c.code)
        await self._alert(
            f"submit-unknown-{key[:8]}",
            f"115 Bridge: 提交 {key[:8]} 连续 {SUBMIT_UNKNOWN_LIMIT} 次得到未知错误码 "
            f"{c.code}，已按提交被拒处理。",
        )
        raise SubmitRejected(c.code, c.message or "unknown error code") from None

    # ------------------------------------------------------------ 离线任务

    async def list_offline(self, stat: OfflineStat, page: int = 1) -> OfflinePage:
        """通道选 web（``type="web"``）：数据在响应顶层，实验中已用 TV cookie 验证；
        解析同时兼容 ssp 的 ``data`` 嵌套结构。结果缓存 20 秒，按 (stat, page) 区分。"""
        return await self._list_offline(stat, page)

    async def _list_offline(
        self, stat: OfflineStat, page: int, *, probe: bool = False, use_cache: bool = True
    ) -> OfflinePage:
        """轮询与心跳共用的路径：同一把按 (stat, page) 的锁，同一个缓存。
        心跳不读缓存（要真发请求），但同样排队等锁，所以轮询卡住时心跳也会卡住。"""
        self._gate(probe)
        key = (int(stat), page)
        if use_cache:
            hit = self._cache_hit(key)
            if hit is not None:
                return hit
        lock = self._cache_locks.setdefault(key, asyncio.Lock())
        async with lock:
            if use_cache:
                hit = self._cache_hit(key)
                if hit is not None:
                    return hit
            return await self._fetch_offline(stat, page, probe=probe)

    def _cache_hit(self, key: tuple[int, int]) -> OfflinePage | None:
        entry = self._cache.get(key)
        if entry is None:
            return None
        stamp, cached = entry
        if self._clock.monotonic() - stamp >= CACHE_TTL_SECONDS:
            del self._cache[key]
            return None
        return replace(cached, tasks=list(cached.tasks))

    async def _fetch_offline(
        self, stat: OfflineStat, page: int, *, probe: bool = False
    ) -> OfflinePage:
        async def fn(client: Any) -> OfflinePage:
            resp = await client.clouddownload_task_list(
                {"page": page, "stat": int(stat)}, type="web", async_=True
            )
            check_response(resp)
            return self._parse_offline(resp, page)

        result = await self._call("list_offline", fn, probe=probe)
        self._cache[(int(stat), page)] = (self._clock.monotonic(), result)
        self._quota = result.quota
        return replace(result, tasks=list(result.tasks))

    @staticmethod
    def _parse_offline(resp: Mapping[str, Any], page: int) -> OfflinePage:
        data = resp.get("data")
        body: Mapping[str, Any] = data if isinstance(data, Mapping) else resp
        raw_tasks = body.get("tasks")
        if raw_tasks is None and isinstance(data, list):
            raw_tasks = data
        if raw_tasks is None:
            raw_tasks = resp.get("tasks")
        if raw_tasks is None:
            raw_tasks = []  # 115 在没有任务时可能省略 tasks
        if not isinstance(raw_tasks, list):
            raise ServerError("list_offline: unexpected structure (tasks)")

        def pick(key: str) -> Any:
            for src in (body, resp):
                if key in src and src[key] is not None:
                    return src[key]
            return None

        remaining = _to_int(pick("quota"))
        total = _to_int(pick("total"))
        if remaining is None or total is None:
            raise ServerError("list_offline: unexpected structure (quota/total missing)")
        page_count = _to_int(pick("page_count")) or 1
        tasks: list[OfflineTask] = []
        for item in raw_tasks:
            if not isinstance(item, Mapping):
                raise ServerError("list_offline: unexpected structure (task item)")
            if not item.get("info_hash"):
                log_event(logger, logging.WARNING, "offline task without info_hash, skipped")
                continue
            file_id = item.get("file_id")
            wp = item.get("wp_path_id")
            try:
                percent = float(item.get("percentDone") or 0)
            except (TypeError, ValueError):
                percent = 0.0
            tasks.append(
                OfflineTask(
                    hash=str(item["info_hash"]).lower(),
                    name=str(item.get("name") or ""),
                    status=_to_int(item.get("status")) or 0,
                    percent_done=percent,
                    file_id=str(file_id) if file_id not in (None, "", 0, "0") else None,
                    wp_path_id=str(wp) if wp not in (None, "") else None,
                    del_path=str(item.get("del_path") or ""),
                    size=_to_int(item.get("size")) or 0,
                )
            )
        return OfflinePage(
            tasks=tasks,
            quota=Quota(remaining=remaining, total=total),
            page=_to_int(pick("page")) or page,
            page_count=page_count,
        )

    async def add_magnet(self, magnet: str, wp_path_id: str) -> str:
        try:
            hint = parse_magnet(magnet).info_hash
        except InvalidMagnetError as exc:
            raise SubmitRejected(None, str(exc)) from None

        async def fn(client: Any) -> str:
            resp = await client.clouddownload_task_add_urls(
                {"url[0]": magnet, "wp_path_id": str(wp_path_id)}, type="ssp", async_=True
            )
            check_response(resp)
            item = self._first_result(resp)
            if item is None:
                raise ServerError("add_magnet: unexpected structure (result)")
            if item.get("state"):
                reported = item.get("info_hash")
                if isinstance(reported, str):
                    try:
                        return normalize_btih(reported)
                    except InvalidMagnetError:
                        pass
                return hint
            raise CodedError(extract_code(item), extract_message(item))

        result = await self._call("add_magnet", fn, submit_key=hint, on_exists=lambda: hint)
        self._cache.clear()
        return result

    @staticmethod
    def _first_result(resp: Mapping[str, Any]) -> Mapping[str, Any] | None:
        data = resp.get("data")
        results = data.get("result") if isinstance(data, Mapping) else resp.get("result")
        if isinstance(results, list) and results and isinstance(results[0], Mapping):
            return results[0]
        return None

    # ------------------------------------------------------------ 目录

    async def makedirs(self, path: str) -> str:
        parts = [p for p in path.split("/") if p]
        if not parts:
            return "0"
        rel = "/".join(parts)

        async def fn(client: Any) -> str:
            try:
                resp = await client.fs_makedirs_app({"path": rel, "parent_id": 0}, async_=True)
                check_response(resp)
            except FileExistsError:
                resp = await client.fs_dir_getid({"path": "/" + rel}, async_=True)
                check_response(resp)
            data = resp.get("data")
            cid = resp.get("cid") or resp.get("id")
            if not cid and isinstance(data, Mapping):
                cid = data.get("cid") or data.get("id")
            # fs_dir_getid 对不存在的路径返回 id=0（同 p115client tool/attr.py），
            # 不能把根目录当成分类目录
            if cid is None or _to_int(cid) in (None, 0):
                raise ServerError("makedirs: unexpected structure (no usable id)")
            return str(cid)

        return await self._call("makedirs", fn)

    async def get_entry(self, entry_id: str) -> CloudEntry | None:
        entry_id = str(entry_id)
        if entry_id == "0":
            return CloudEntry("0", "0", "", True, 0, None, None)

        async def fn(client: Any) -> CloudEntry | None:
            resp = await client.fs_file_skim(entry_id, async_=True)
            check_response(resp)
            data = resp.get("data")
            if not isinstance(data, list) or not data:
                return None
            info = data[0]
            sha1 = info.get("sha1") or None
            is_dir = not sha1
            return CloudEntry(
                id=str(info.get("file_id") or entry_id),
                parent_id=str(info.get("parent_id") or info.get("cid") or ""),
                name=unescape_115_charref(str(info.get("file_name") or "")),
                is_dir=is_dir,
                size=0 if is_dir else (_to_int(info.get("file_size")) or 0),
                sha1=str(sha1) if sha1 else None,
                pickcode=str(info["pick_code"]) if info.get("pick_code") else None,
            )

        return await self._call("get_entry", fn, not_found=lambda: None)

    async def list_dir(self, dir_id: str) -> list[CloudEntry]:
        """列一层目录。115 对不存在或已删除的 cid 不报错而是返回根目录列表，
        所以每页都核对响应里的 cid（同 p115client 的 fs_files 与 iterdir）。"""
        dir_id = str(dir_id)
        want = _to_int(dir_id)

        async def fn(client: Any) -> list[CloudEntry]:
            entries: list[CloudEntry] = []
            offset = 0
            while True:
                resp = await self._bounded(
                    client.fs_files_app(
                        {
                            "cid": dir_id,
                            "limit": DIR_PAGE_SIZE,
                            "offset": offset,
                            "show_dir": 1,
                            "cur": 1,
                            "record_open_time": 0,
                        },
                        async_=True,
                    ),
                    REQUEST_TIMEOUT_SECONDS,
                )
                check_response(resp)
                if want != 0:  # 根目录不会不存在，p115client 同样跳过
                    got = _to_int(resp.get("cid"))
                    if got is None:
                        raise ServerError("list_dir: unexpected structure (cid)")
                    if got != want:
                        raise CloudNotFound(f"list_dir: directory {dir_id} not found")
                got_offset = _to_int(resp.get("offset"))
                if got_offset is not None and got_offset != offset:
                    raise ServerError("list_dir: offset mismatch")
                rows = resp.get("data")
                count = _to_int(resp.get("count"))
                if not isinstance(rows, list) or count is None:
                    raise ServerError("list_dir: unexpected structure (data/count)")
                for row in rows:
                    entries.append(self._parse_entry(row, dir_id))
                offset += len(rows)
                if offset >= count:
                    return entries
                if not rows:
                    raise ServerError("list_dir: short listing (empty page before count)")
                # 翻页之间也要过令牌桶，且要先过暂停检查
                self._gate()
                await self._bucket.acquire()

        return await self._call("list_dir", fn, request_timeout=None)

    @staticmethod
    def _parse_entry(row: Mapping[str, Any], dir_id: str) -> CloudEntry:
        try:
            is_dir = str(row["fc"]) == "0"
            sha1 = row.get("sha1") or None
            return CloudEntry(
                id=str(row["fid"]),
                parent_id=str(row.get("pid", dir_id)),
                name=unescape_115_charref(str(row["fn"])),
                is_dir=is_dir,
                size=0 if is_dir else (_to_int(row.get("fs")) or 0),
                sha1=str(sha1) if sha1 and not is_dir else None,
                pickcode=str(row["pc"]) if row.get("pc") else None,
            )
        except KeyError as exc:
            raise ServerError("list_dir: unexpected structure (entry)") from exc

    # ------------------------------------------------------------ 下载链接

    async def download_link(self, pickcode: str) -> DownloadLink:
        """取下载链接（API 请求，过令牌桶）。CDN 字节下载不在本模块。

        显式传 ``user_agent``：链接可能要求请求 UA 与取链接时一致，
        返回的 headers 带上这个 UA（不变量 8）。"""

        async def fn(client: Any) -> DownloadLink:
            link = await client.download_url(
                pickcode, app=self._download_app, user_agent=DOWNLOAD_USER_AGENT, async_=True
            )
            url = str(link)
            if not url or not url.startswith("http"):
                raise ServerError("download_link: empty url")
            raw = getattr(link, "headers", None)
            headers = {str(k): str(v) for k, v in dict(raw or {}).items()}
            if not any(k.lower() == "user-agent" and v for k, v in headers.items()):
                headers = {k: v for k, v in headers.items() if k.lower() != "user-agent"}
                headers["user-agent"] = DOWNLOAD_USER_AGENT
            size = getattr(link, "size", None)
            return DownloadLink(url=url, headers=headers, size=_to_int(size))

        return await self._call("download_link", fn, extra_not_found=LINK_NOT_FOUND_CODES)

    # ------------------------------------------------------------ 网盘空间

    async def get_space(self) -> CloudSpace:
        """只读查询网盘总空间与剩余空间（``fs_index_info``，GET ``/files/index_info``）。
        结果缓存 5 分钟；过了缓存期才发请求，走与其他接口相同的暂停检查、令牌桶、超时与分类。"""
        self._gate()
        if self._space is not None and not self._space_stale():
            return self._space
        async with self._space_lock:
            if self._space is not None and not self._space_stale():
                return self._space
            return await self._fetch_space()

    def _space_stale(self) -> bool:
        stamp = self._space_stamp
        return stamp is None or self._clock.monotonic() - stamp >= SPACE_CACHE_SECONDS

    async def _fetch_space(self) -> CloudSpace:
        async def fn(client: Any) -> CloudSpace:
            resp = await client.fs_index_info(0, async_=True)
            check_response(resp)
            return self._parse_space(resp)

        space = await self._call("get_space", fn)
        self._space = space
        self._space_stamp = self._clock.monotonic()
        await self._check_space_alert(space)
        return space

    @staticmethod
    def _parse_space(resp: Mapping[str, Any]) -> CloudSpace:
        """``data.space_info.all_total.size`` 与 ``all_remain.size``（字节）；
        没有 ``all_remain`` 时用 ``all_total.size - all_use.size``。"""
        data = resp.get("data")
        info = data.get("space_info") if isinstance(data, Mapping) else None
        if not isinstance(info, Mapping):
            raise ServerError("get_space: unexpected structure (space_info)")

        def size(key: str) -> int | None:
            item = info.get(key)
            raw = item.get("size") if isinstance(item, Mapping) else None
            if isinstance(raw, bool):
                return None
            if isinstance(raw, (int, float)):
                return int(raw)
            return _to_int(raw)

        total = size("all_total")
        remaining = size("all_remain")
        if remaining is None:
            used = size("all_use")
            if total is not None and used is not None:
                remaining = total - used
        if total is None or remaining is None or total < 0 or remaining < 0:
            raise ServerError("get_space: unexpected structure (size fields)")
        return CloudSpace(total=total, remaining=remaining)

    async def _check_space_alert(self, space: CloudSpace) -> None:
        """剩余空间低于阈值时告警，恢复后发恢复通知。只告警，不暂停任何动作。"""
        if space.remaining < self._space_min_bytes:
            self._space_low = True
            await self._alert(
                "cloud-space",
                f"115 Bridge: 115 网盘剩余空间 {space.remaining / GIB:.1f} GiB，"
                f"低于阈值 {self._space_min_bytes // GIB} GiB"
                f"（总量 {space.total / GIB:.1f} GiB）。",
            )
        elif self._space_low:
            self._space_low = False
            await self._recovered(
                "cloud-space",
                f"115 Bridge: 115 网盘剩余空间已恢复到 {space.remaining / GIB:.1f} GiB。",
            )

    # ------------------------------------------------------------ 心跳

    async def heartbeat_loop(self) -> None:
        """每 5 分钟一次；可由调度器作为协程启动。"""
        while True:
            await self._clock.sleep(HEARTBEAT_SECONDS)
            await self.heartbeat_once()

    async def heartbeat_once(self) -> bool:
        """5 分钟内已有成功调用则跳过；否则调一次离线任务列表。返回是否真的发了请求。

        认证失效与 lease 丢失时不发；熔断期内不发；需人工验证时发探测，
        一旦成功就自动恢复。失败按同一套分类处理，不向外抛。"""
        if self._auth_expired or not self._lease_ok or self._breaker.is_open():
            return False
        sent = False
        if (
            self._last_success_mono is None
            or self._clock.monotonic() - self._last_success_mono >= HEARTBEAT_SECONDS
        ):
            sent = True
            try:
                await self._list_offline(
                    OfflineStat.RUNNING, 1, probe=self._verify_required, use_cache=False
                )
            except CloudError as exc:
                log_event(logger, logging.INFO, "heartbeat failed", error_type=type(exc).__name__)
        await self._refresh_space()
        return sent

    async def _refresh_space(self) -> None:
        """心跳顺带刷新网盘空间：独立计时（缓存满 5 分钟才真发请求），
        不受「5 分钟内已有成功调用」的跳过规则影响；失败只记日志。"""
        if self._auth_expired or not self._lease_ok or self._breaker.is_open():
            return
        if self._verify_required:
            return  # 探测只用离线列表，不拿空间请求去撞 911
        try:
            await self.get_space()
        except CloudError as exc:
            log_event(logger, logging.INFO, "space refresh failed", error_type=type(exc).__name__)

    # ------------------------------------------------------------ 扫码登录（TV 端）

    async def qr_start(self) -> QrToken:
        async def fn(_: Any) -> _Qr:
            resp = await self._p115.login_qrcode_token(async_=True)
            check_response(resp)
            data = resp.get("data")
            if not isinstance(data, Mapping) or not data.get("uid") or not data.get("qrcode"):
                raise ServerError("qr_start: unexpected structure")
            payload = {k: data[k] for k in ("uid", "time", "sign") if k in data}
            return _Qr(
                uid=str(data["uid"]),
                payload=payload,
                created=self._clock.monotonic(),
                png=_render_qr(str(data["qrcode"])),
            )

        qr = await self._call("qr_start", fn, qr=True)
        self._qr = qr
        return QrToken(uid=qr.uid, image_png=qr.png)

    async def qr_poll(self, uid: str) -> QrStatus:
        qr = self._qr
        if qr is None or qr.uid != uid:
            return QrStatus.EXPIRED

        async def fn(_: Any) -> QrStatus:
            resp = await self._p115.login_qrcode_scan_status(dict(qr.payload), async_=True)
            data = resp.get("data") if isinstance(resp, Mapping) else None
            if isinstance(data, Mapping) and "status" in data:
                mapping = {
                    0: QrStatus.WAITING,
                    1: QrStatus.SCANNED,
                    2: QrStatus.CONFIRMED,
                    -1: QrStatus.EXPIRED,
                    -2: QrStatus.CANCELED,
                }
                status = mapping.get(_to_int(data["status"]) or 0)
                if status is None:
                    raise ServerError("qr_poll: unknown status")
                return status
            if isinstance(resp, Mapping) and not resp.get("state"):
                return QrStatus.EXPIRED
            raise ServerError("qr_poll: unexpected structure")

        return await self._call("qr_poll", fn, qr=True, request_timeout=QR_POLL_TIMEOUT_SECONDS)

    async def qr_finish(self, uid: str) -> None:
        """换取 TV 端 cookie。失败只抛异常：会话原状态、原 cookie 与原 client 都不动
        （会话正常时手动扫码失败不能把它拖进暂停）。"""
        async with self._finish_lock:
            if self._finished_uid == uid:
                return

            async def fn(_: Any) -> Any:
                resp = await self._p115.login_qrcode_scan_result(uid, app=LOGIN_APP, async_=True)
                check_response(resp)
                data = resp.get("data")
                cookie = data.get("cookie") if isinstance(data, Mapping) else None
                if not cookie:
                    raise ServerError("qr_finish: no cookie in result")
                return self._p115(cookies=cookie)  # str 或 dict 都可以

            client = await self._call("qr_finish", fn, qr=True)
            cookie_str = str(client.cookies_str)
            if not has_uid(cookie_str):
                # 缺 UID 时 p115client 会改走 /open/ 路径并派生 Open token，按认证失效处理
                log_event(
                    logger, logging.ERROR, "login result has no UID", cookie_len=len(cookie_str)
                )
                raise AuthExpired("qr_finish: login result has no UID")
            if not self._lease_ok:
                raise ServerError("session paused: lease lost")
            # 只在登录成功时写 cookie 文件
            write_cookie_file(self._cookie_path, cookie_str)
            self._client = client
            self._epoch += 1
            self._cookie_len = len(cookie_str)
            self._finished_uid = uid
            self._qr = None
            log_event(
                logger,
                logging.INFO,
                "login succeeded",
                cookie_len=len(cookie_str),
                login_app=LOGIN_APP,
            )
            if self._auth_expired:
                self._auth_expired = False
                self._detail = ""
                await self._recovered("auth", "115 Bridge: 扫码登录成功，已恢复 115 访问。")

    async def regenerate_qr(self) -> QrToken:
        """手动重新生成二维码（Web 页面入口）并推送；会被后台轮询自动接管。
        算一次页面访问，也算一次推送尝试，扫码轮询窗口续 10 分钟。"""
        return await self._regenerate(extend_window=True)

    async def _regenerate(self, *, extend_window: bool) -> QrToken:
        """生成新码并尝试推送。``extend_window`` 为真时它是一次「推送尝试」：记下时间并把
        轮询窗口续 10 分钟。窗口内因旧码过期而自动换新码时为假，只是再推一次
        （Notifier 会去重），不续窗口，否则轮询永远不会停。"""
        token = await self.qr_start()
        if extend_window:
            self._last_push_mono = self._clock.monotonic()
            self._touch_qr_window()
        try:
            await self._notifier.send_qr(token.image_png, _QR_CAPTION)
        except Exception as exc:  # noqa: BLE001
            log_event(logger, logging.WARNING, "send_qr failed", error_type=type(exc).__name__)
        return token

    def _ensure_qr_task(self) -> None:
        if not self._auto_qr:
            return
        if self._qr_task is not None and not self._qr_task.done():
            return
        self._qr_task = asyncio.get_running_loop().create_task(self.qr_login_loop())

    async def qr_login_loop(self) -> None:
        """认证失效后：推送二维码并只在活跃窗口内轮询扫码状态，直到登录成功。

        窗口外不轮询、不自动重生成，等下一次推送尝试（30 分钟一次）或页面访问；
        lease 丢失期间不请求 115。"""
        while self._auth_expired:
            try:
                await self._qr_cycle()
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # noqa: BLE001
                log_event(logger, logging.WARNING, "qr cycle failed", error_type=type(exc).__name__)
                await self._clock.sleep(QR_RETRY_SECONDS)

    async def _qr_cycle(self) -> None:
        if not self._lease_ok:
            await self._clock.sleep(QR_POLL_SECONDS)
            return
        now = self._clock.monotonic()
        if now >= self._qr_active_until:
            # 窗口外：只在到了推送节奏时才生成并推送一次
            last = self._last_push_mono
            if last is None or now - last >= QR_PUSH_INTERVAL_SECONDS:
                await self._regenerate(extend_window=True)
            else:
                await self._clock.sleep(QR_IDLE_TICK_SECONDS)
            return
        qr = self._qr
        if qr is None or now - qr.created >= QR_TTL_SECONDS:
            await self._regenerate(extend_window=False)
            return
        status = await self.qr_poll(qr.uid)
        if status in (QrStatus.WAITING, QrStatus.SCANNED):
            await self._clock.sleep(QR_POLL_SECONDS)
        elif status is QrStatus.CONFIRMED:
            await self.qr_finish(qr.uid)
        elif self._qr is not None and self._qr.uid == qr.uid:
            # 过期或取消：只清掉被轮询的这个码，不能清掉期间手动生成的新码
            self._qr = None


_QR_CAPTION = "115 Bridge: 请用 115 手机端扫码登录（TV 端），5 分钟内有效。"


def _render_qr(url: str, box_size: int = 8, border: int = 3) -> bytes:
    """把二维码渲染成 PNG 字节。

    只用 qrcode 的矩阵加标准库 zlib 自己编码 PNG：锁定的依赖里没有 Pillow 或 pypng，
    ``make_image()`` 在容器里会因缺少图像后端而失败。"""
    code = qrcode.QRCode(box_size=box_size, border=border)
    code.add_data(url)
    code.make(fit=True)
    matrix: list[list[bool]] = code.get_matrix()  # 已含 border
    size = len(matrix) * box_size
    black, white = bytes([0]), bytes([255])
    rows = bytearray()
    for row in matrix:
        line = bytearray([0])  # 滤波类型 None
        for cell in row:
            line += (black if cell else white) * box_size
        rows += bytes(line) * box_size
    ihdr = struct.pack(">IIBBBBB", size, size, 8, 0, 0, 0, 0)  # 8 位灰度

    def chunk(tag: bytes, data: bytes) -> bytes:
        body = tag + data
        return struct.pack(">I", len(data)) + body + struct.pack(">I", zlib.crc32(body))

    signature = bytes([0x89]) + b"PNG" + bytes([0x0D, 0x0A, 0x1A, 0x0A])
    return (
        signature
        + chunk(b"IHDR", ihdr)
        + chunk(b"IDAT", zlib.compress(bytes(rows)))
        + chunk(b"IEND", b"")
    )
