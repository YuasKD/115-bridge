"""missingFiles 契约：对账（pipeline）提供，qBit 与 Web（api）读取。

放在中立模块，避免 api 依赖 pipeline 实现。改动需主会话批准。

对账只标记「启动时就已 completed 且本地文件缺失」的那一次完成。一次完成用该任务最近一次
转入 COMPLETED 的事件 id（``events.id`` 单调递增）标识，任务之后经 removed、requeue、
重新完成会得到新的事件 id，旧标记自然失效。
"""

from __future__ import annotations

from typing import Protocol, runtime_checkable

from bridge.db.repo import TaskRepo, TaskState


@runtime_checkable
class MissingFilesView(Protocol):
    def might_be_missing(self, info_hash: str) -> bool:
        """只看 hash 是否在快照里（廉价、不查库）；为 False 时调用方无需再查 ``completion_id``。"""
        ...

    def is_missing(self, info_hash: str, completion_id: int | None) -> bool:
        """``completion_id`` 为该任务当前这次完成的标识（见 ``completion_id``），未知传 None。"""
        ...


async def completion_id(repo: TaskRepo, info_hash: str) -> int | None:
    """该任务最近一次转入 COMPLETED 的事件 id；没有返回 None。

    只取最新一条事件：events 只在状态转换时写入，当前处于 COMPLETED 的任务，其最新事件必定是
    转入 COMPLETED 的那一条。最新事件不是 COMPLETED（任务不在 COMPLETED）时返回 None。"""
    for ev in await repo.list_events(info_hash, limit=1):
        if ev.to_state is TaskState.COMPLETED:
            return ev.id
    return None


__all__ = ["MissingFilesView", "completion_id"]
