"""任务仓储契约（T01）：数据类、状态机、仓储接口。

实现（schema、WAL、转换校验）在 T02。改动本文件或 schema 需主会话批准。
时间一律由调用方传入（注入时钟），仓储内部不读系统时间。
"""

from __future__ import annotations

from collections.abc import Collection, Mapping
from dataclasses import dataclass, field
from datetime import datetime
from enum import StrEnum
from typing import Protocol, runtime_checkable

# ---------------------------------------------------------------- 状态机


class TaskState(StrEnum):
    QUEUED = "queued"
    SUBMITTING = "submitting"
    CLOUD_METADATA = "cloud_metadata"
    CLOUD_DOWNLOADING = "cloud_downloading"
    CLOUD_DONE = "cloud_done"
    LOCATING = "locating"
    FETCHING = "fetching"
    VERIFYING = "verifying"
    COMPLETED = "completed"
    REMOVED = "removed"
    FAILED = "failed"


TERMINAL_STATES: frozenset[TaskState] = frozenset(
    {TaskState.COMPLETED, TaskState.REMOVED, TaskState.FAILED}
)

ACTIVE_STATES: frozenset[TaskState] = frozenset(TaskState) - TERMINAL_STATES

_S = TaskState

ALLOWED_TRANSITIONS: Mapping[TaskState, frozenset[TaskState]] = {
    # 正常路径；云端阶段即使一次轮询就看到完成，也由调度器逐级推进，不跳级
    _S.QUEUED: frozenset({_S.SUBMITTING, _S.LOCATING}),  # LOCATING = 秒取回
    _S.SUBMITTING: frozenset({_S.CLOUD_METADATA}),
    _S.CLOUD_METADATA: frozenset({_S.CLOUD_DOWNLOADING}),
    _S.CLOUD_DOWNLOADING: frozenset({_S.CLOUD_DONE}),
    _S.CLOUD_DONE: frozenset({_S.LOCATING}),
    _S.LOCATING: frozenset({_S.FETCHING}),
    _S.FETCHING: frozenset({_S.VERIFYING}),
    _S.VERIFYING: frozenset({_S.COMPLETED, _S.FETCHING}),  # 校验失败重下
    _S.COMPLETED: frozenset({_S.REMOVED}),
    _S.REMOVED: frozenset(),
    _S.FAILED: frozenset(),
}
"""合法转换。另外，任何 ACTIVE 状态都可以转到 FAILED 和 REMOVED（见 can_transition）。

- 任意非终态 → FAILED：触发 Arr 回退。
- 任意非终态 → REMOVED：用户在 Arr 里手动删除在途任务（已拍板）。
- FAILED 收到 torrents/delete 只清本地，不转 REMOVED（已拍板）。
"""


def can_transition(src: TaskState, dst: TaskState) -> bool:
    if src in ACTIVE_STATES and dst in (TaskState.FAILED, TaskState.REMOVED):
        return True
    return dst in ALLOWED_TRANSITIONS[src]


class InvalidTransition(Exception):
    def __init__(self, info_hash: str, src: TaskState, dst: TaskState) -> None:
        super().__init__(f"{info_hash}: {src.value} -> {dst.value} not allowed")
        self.info_hash = info_hash
        self.src = src
        self.dst = dst


class TaskNotFound(Exception):
    pass


class StateMismatch(Exception):
    """transition(expected=...) 时当前状态与期望不符。"""

    def __init__(self, info_hash: str, expected: TaskState, actual: TaskState) -> None:
        super().__init__(f"{info_hash}: expected {expected.value}, found {actual.value}")
        self.info_hash = info_hash
        self.expected = expected
        self.actual = actual


class Stage(StrEnum):
    """重试计数的阶段键。计数只增不减（不变量 3）。"""

    FETCH = "fetch"
    VERIFY = "verify"


class Source(StrEnum):
    MAGNET = "magnet"
    TORRENT = "torrent"


class Fallback(StrEnum):
    """Arr 回退进度（待人确认：设计文档的 tasks 表未列此字段）。"""

    NONE = "none"  # 未失败或无需回退
    PENDING = "pending"  # 已标 failed，Arr 调用未成功，每 10 分钟重试
    DONE = "done"  # Arr 已接受 DELETE


# ---------------------------------------------------------------- 数据类


@dataclass(frozen=True, slots=True)
class Task:
    hash: str  # 小写 hex，主键
    category: str
    name: str
    state: TaskState
    source: Source
    added_at: datetime
    magnet: str = ""  # 提交 115 用的 magnet（带 tracker 和 dn），添加时写入
    submitted_at: datetime | None = None
    cloud_file_id: str | None = None
    cloud_size: int | None = None
    cloud_percent: float = 0.0
    local_bytes: int = 0
    error: str | None = None
    retries: Mapping[Stage, int] = field(default_factory=dict)
    fallback: Fallback = Fallback.NONE  # 待确认的新增字段
    client_deleted: bool = False  # 待确认：Arr 已调 torrents/delete，torrents/info 不再列出


@dataclass(frozen=True, slots=True)
class TaskUpdate:
    """进度类字段的部分更新；None 表示不改。不能改 state、retries。"""

    submitted_at: datetime | None = None
    cloud_file_id: str | None = None
    cloud_size: int | None = None
    cloud_percent: float | None = None
    local_bytes: int | None = None
    name: str | None = None


@dataclass(frozen=True, slots=True)
class ManifestFile:
    """torrent_meta：本地解析出的种子清单中的一个文件。"""

    rel_path: str  # 相对任务根目录，用 / 分隔
    size: int


@dataclass(frozen=True, slots=True)
class TaskFile:
    """task_files：定位结果，每个云端文件一行。"""

    rel_path: str
    size: int
    sha1: str
    pickcode: str
    local_bytes: int = 0
    verified: bool = False


@dataclass(frozen=True, slots=True)
class CloudIndexEntry:
    """cloud_index：hash 到 115 目录，永久保留，用于秒取回。"""

    hash: str
    cloud_file_id: str
    root_name: str
    total_size: int
    completed_at: datetime


@dataclass(frozen=True, slots=True)
class Event:
    id: int
    hash: str
    at: datetime
    from_state: TaskState | None  # 新建任务时为 None
    to_state: TaskState
    reason: str


EVENTS_PER_TASK = 200


# ---------------------------------------------------------------- 仓储


@runtime_checkable
class TaskRepo(Protocol):
    # -- 任务 --

    async def create_task(
        self,
        info_hash: str,
        category: str,
        name: str,
        source: Source,
        at: datetime,
        magnet: str,
    ) -> tuple[Task, bool]:
        """新建 QUEUED 任务并写一条事件。已存在则原样返回，第二项为 False（幂等）。

        magnet：.torrent 用 TorrentInfo.to_magnet()（带种子里的 tracker 和 dn）；
        magnet 链接原样保存。提交 115 时只用它，原始种子字节不保存。
        """
        ...

    async def get_task(self, info_hash: str) -> Task | None: ...

    async def list_tasks(
        self,
        *,
        category: str | None = None,
        states: Collection[TaskState] | None = None,
        include_client_deleted: bool = False,
    ) -> list[Task]: ...

    async def transition(
        self,
        info_hash: str,
        dst: TaskState,
        reason: str,
        at: datetime,
        *,
        expected: TaskState | None = None,
    ) -> Task:
        """校验并转换状态，同一事务内写事件并裁剪到 EVENTS_PER_TASK。

        expected 不为 None 时做比较并转换：当前状态不等于 expected 就抛
        StateMismatch，不做任何改动（人已拍板，用于并发场景）。
        Raises: TaskNotFound, InvalidTransition, StateMismatch。
        """
        ...

    async def requeue_removed(self, info_hash: str, reason: str, at: datetime) -> Task:
        """removed → queued，仅供 torrents/add 重新添加已 removed 的 hash 使用（人已拍板）。

        不经过 can_transition；同一事务内写事件、清除 client_deleted 和 error、
        fallback 重置为 NONE，并清掉上一轮的进度：submitted_at、cloud_file_id、
        cloud_size 置空，cloud_percent、local_bytes 置 0，删除该任务的 task_files
        （人已拍板）。重试计数、cloud_index、清单（torrent_meta）、magnet 保留不动。
        Raises: TaskNotFound；任务不是 removed 时抛 InvalidTransition(src, QUEUED)。
        """
        ...

    async def update(self, info_hash: str, change: TaskUpdate) -> Task: ...

    async def set_error(self, info_hash: str, error: str | None) -> Task: ...

    async def incr_retry(self, info_hash: str, stage: Stage) -> int:
        """该阶段计数加一并返回新值。没有任何接口能减少或清零计数。"""
        ...

    async def set_fallback(self, info_hash: str, fallback: Fallback) -> Task: ...

    async def mark_client_deleted(self, info_hash: str) -> Task: ...

    # -- 种子清单 --

    async def put_manifest(self, info_hash: str, files: list[ManifestFile]) -> None:
        """整体替换。"""
        ...

    async def get_manifest(self, info_hash: str) -> list[ManifestFile]:
        """没有清单（纯 magnet 且元数据未知）时返回空列表。"""
        ...

    # -- 定位结果与取回进度 --

    async def put_task_files(self, info_hash: str, files: list[TaskFile]) -> None:
        """整体替换。"""
        ...

    async def list_task_files(self, info_hash: str) -> list[TaskFile]: ...

    async def update_task_file(
        self,
        info_hash: str,
        rel_path: str,
        *,
        local_bytes: int | None = None,
        verified: bool | None = None,
    ) -> None: ...

    # -- 秒取回索引 --

    async def get_cloud_index(self, info_hash: str) -> CloudIndexEntry | None: ...

    async def put_cloud_index(self, entry: CloudIndexEntry) -> None:
        """插入或覆盖。没有删除接口：索引永久保留。"""
        ...

    # -- 事件 --

    async def list_events(self, info_hash: str, limit: int = 50) -> list[Event]:
        """按时间倒序。"""
        ...

    # -- kv 与 lease --

    async def kv_get(self, key: str) -> str | None: ...

    async def kv_set(self, key: str, value: str) -> None: ...

    async def acquire_lease(self, owner: str, now: datetime, ttl_seconds: int) -> bool:
        """无人持有、已过期或本就由 owner 持有时获取成功；否则 False。"""
        ...

    async def renew_lease(self, owner: str, now: datetime, ttl_seconds: int) -> bool:
        """仅当仍由 owner 持有且未过期时续约。"""
        ...

    async def release_lease(self, owner: str) -> None: ...
