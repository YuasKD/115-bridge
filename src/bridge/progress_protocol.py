"""取回进度契约：取回模块（pipeline）提供，qBit 兼容层（api）读取。

放在中立模块，避免 pipeline 反向依赖 api 层。改动需主会话批准。
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Protocol, runtime_checkable

ETA_INFINITE = 8640000  # qBittorrent 用 8640000 表示无穷


@dataclass(frozen=True, slots=True)
class FetchProgress:
    """取回模块（T11）提供的运行时信息。"""

    dlspeed: int = 0  # bytes/s
    eta: int = ETA_INFINITE  # 秒
    waiting_for_space: bool = False  # 本地空间不足而等待；为真时 fetching 报 queuedDL


@runtime_checkable
class ProgressSource(Protocol):
    def progress(self, info_hash: str) -> FetchProgress: ...


class ZeroProgressSource:
    """默认实现：全部零值。"""

    def progress(self, info_hash: str) -> FetchProgress:
        return FetchProgress()
