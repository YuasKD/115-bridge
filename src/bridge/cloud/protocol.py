"""115 访问契约（T01）。

除 ``cloud/session.py`` 外，所有模块只依赖本文件，不得 import p115client。
改动本文件需主会话批准。

约定：
- 115 的 id 一律是 ``str``（不变量 5）。
- hash 一律小写 40 位 hex（不变量 11）。
- 全部方法都是协程；令牌桶、熔断、缓存由实现方（session.py）负责，调用方不感知。
- 全局类异常（AuthExpired、RiskControl、NetworkError、ServerError）由实现方抛出，
  调用方据此暂停，不计任务重试（不变量 4）。
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum, IntEnum
from typing import Protocol, runtime_checkable

# ---------------------------------------------------------------- 异常


class CloudError(Exception):
    """115 访问异常基类。"""


class AuthExpired(CloudError):
    """认证失效：cookie 过期、被挤下线、4014xxxx。全局暂停并推送二维码。"""


class VerifyRequired(CloudError):
    """115 要求人工验证账号（错误码 911）。全局暂停并告警，不推二维码（人已拍板）。"""


class RiskControl(CloudError):
    """风控/限流：频率异常类错误码。全局熔断退避。"""


class NetworkError(CloudError):
    """超时、连接错误、SSL 错误。保持原状，下个周期重试。"""


class ServerError(CloudError):
    """115 服务端 5xx 或返回未知结构。处理同 NetworkError。"""


class SubmitRejected(CloudError):
    """115 拒绝该 magnet。任务级失败，直接 Arr 回退，不重试。"""

    def __init__(self, errcode: int | None, message: str) -> None:
        super().__init__(f"submit rejected: errcode={errcode} {message}")
        self.errcode = errcode
        self.message = message


class CloudNotFound(CloudError):
    """云端文件或目录已不存在（例如在 115 网页上被删）。

    任务级：标 failed 并做 Arr 回退（人已拍板），不是全局暂停。
    """


class QuotaExhausted(CloudError):
    """离线配额或网盘空间不足。暂停新提交并告警，不算任务失败。"""


GLOBAL_ERRORS: tuple[type[CloudError], ...] = (
    AuthExpired,
    VerifyRequired,
    RiskControl,
    NetworkError,
    ServerError,
)
"""全局性错误：调用方遇到这些只暂停，不改任务状态、不计重试。"""


# ---------------------------------------------------------------- 数据类


class OfflineStat(IntEnum):
    """离线任务列表的筛选参数 ``stat``。"""

    RUNNING = 12
    DONE = 11
    FAILED = 9


OFFLINE_DONE_STATUS = 2
OFFLINE_FAILED_STATUSES = frozenset({-1, 3, 4, 5})


@dataclass(frozen=True, slots=True)
class OfflineTask:
    """115 离线任务列表中的一条。"""

    hash: str  # 小写 hex
    name: str
    status: int
    percent_done: float  # 0 到 100
    file_id: str | None  # 任务根目录 id，完成前可能为空
    wp_path_id: str | None  # 保存目录 id
    del_path: str  # 根目录名
    size: int

    @property
    def is_done(self) -> bool:
        """完成判定（不变量 7）：status == 2 且 file_id 非空。"""
        return self.status == OFFLINE_DONE_STATUS and bool(self.file_id)

    @property
    def is_failed(self) -> bool:
        return self.status in OFFLINE_FAILED_STATUSES


@dataclass(frozen=True, slots=True)
class Quota:
    remaining: int  # 响应里的 quota
    total: int


@dataclass(frozen=True, slots=True)
class CloudSpace:
    """115 网盘空间（字节）。"""

    total: int
    remaining: int


@dataclass(frozen=True, slots=True)
class OfflinePage:
    tasks: list[OfflineTask]
    quota: Quota
    page: int
    page_count: int


@dataclass(frozen=True, slots=True)
class CloudEntry:
    """115 目录树中的一项。"""

    id: str
    parent_id: str
    name: str
    is_dir: bool
    size: int  # 目录为 0
    sha1: str | None  # 文件才有，大写或小写都可能，比较时忽略大小写
    pickcode: str | None


@dataclass(frozen=True, slots=True)
class DownloadLink:
    """一次性下载链接。不得存储（不变量 8），请求时必须带上 headers。"""

    url: str
    headers: dict[str, str]  # 至少含 user-agent
    size: int | None = None


class QrStatus(Enum):
    WAITING = "waiting"
    SCANNED = "scanned"  # 已扫码，待手机确认
    CONFIRMED = "confirmed"
    EXPIRED = "expired"
    CANCELED = "canceled"


@dataclass(frozen=True, slots=True)
class QrToken:
    uid: str
    image_png: bytes  # 二维码图片，供 Telegram 和 Web 页面展示


# ---------------------------------------------------------------- 接口


@runtime_checkable
class CloudClient(Protocol):
    """115 访问接口。实现：``session.py``（真实）、``fake.py``（测试）。"""

    # -- 离线任务 --

    async def list_offline(self, stat: OfflineStat, page: int = 1) -> OfflinePage:
        """按 stat 列一页离线任务，同时返回配额。实现方缓存 20 秒。"""
        ...

    async def get_space(self) -> CloudSpace:
        """只读查询网盘总空间和剩余空间（人已拍板，2026-11-18）。实现方可缓存。"""
        ...

    async def add_magnet(self, magnet: str, wp_path_id: str) -> str:
        """提交一个 magnet 到指定目录，返回 115 回报的 hash（小写）。

        已存在于 115 的同一 hash 视为成功。
        Raises: SubmitRejected, QuotaExhausted, 以及全局类异常。
        """
        ...

    # -- 目录 --

    async def makedirs(self, path: str) -> str:
        """按绝对路径建目录（已存在则直接返回），返回目录 id。"""
        ...

    async def get_entry(self, entry_id: str) -> CloudEntry | None:
        """取单个文件或目录的属性；不存在返回 None（用于秒取回前确认云端仍在）。"""
        ...

    async def list_dir(self, dir_id: str) -> list[CloudEntry]:
        """列一层目录（实现方负责翻页），不递归。"""
        ...

    # -- 下载 --

    async def download_link(self, pickcode: str) -> DownloadLink:
        """取下载链接（默认 app="android"，由配置决定）。每次请求前都要重新调用。"""
        ...

    # -- 扫码登录（TV 端） --

    async def qr_start(self) -> QrToken: ...

    async def qr_poll(self, uid: str) -> QrStatus: ...

    async def qr_finish(self, uid: str) -> None:
        """确认后换取 TV 端 cookie 并持久化（0600），之后会话恢复可用。"""
        ...
