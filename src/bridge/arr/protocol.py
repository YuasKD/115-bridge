"""Arr 回退契约（T01）。

只覆盖设计文档「Arr 回退流程」需要的两个调用。改动本文件需主会话批准。
"""

from __future__ import annotations

from typing import Protocol, runtime_checkable


class ArrError(Exception):
    """Arr 调用异常基类。任何 ArrError 都让任务保持 error，每 10 分钟重试并告警。"""


class ArrUnreachable(ArrError):
    """连接失败、超时、5xx。"""


class ArrAuthError(ArrError):
    """401/403：API Key 错误。"""


class QueueItemNotFound(ArrError):
    """队列里没有该 downloadId。"""


@runtime_checkable
class ArrClient(Protocol):
    """一个 Arr 实例（Radarr 或 Sonarr）。category 到实例的对应由调用方按配置决定。"""

    name: str  # 配置里的名字，例如 "radarr"

    async def find_queue_item(self, info_hash: str) -> int:
        """按 downloadId 找队列项 id。

        info_hash 传小写 hex，实现方转大写后比较（不变量 11）。
        Raises: ArrUnreachable, ArrAuthError, QueueItemNotFound。
        """
        ...

    async def remove_and_blocklist(self, queue_id: int) -> None:
        """DELETE /api/v3/queue/{id}?removeFromClient=true&blocklist=true&skipRedownload=false。

        404 视为 QueueItemNotFound。
        Raises: ArrUnreachable, ArrAuthError, QueueItemNotFound。
        """
        ...
