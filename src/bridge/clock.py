"""时钟契约（T01）。

所有时间相关逻辑（超时、退避、令牌桶、告警去重、lease）都注入 Clock，
测试用假时钟（T04 的 FakeClock），不用真实 sleep。
"""

from __future__ import annotations

import asyncio
import time
from datetime import UTC, datetime
from typing import Protocol, runtime_checkable


@runtime_checkable
class Clock(Protocol):
    def now(self) -> datetime:
        """当前时间，带 UTC 时区。"""
        ...

    def monotonic(self) -> float:
        """单调秒数，用于间隔计算。"""
        ...

    async def sleep(self, seconds: float) -> None: ...


class SystemClock:
    def now(self) -> datetime:
        return datetime.now(UTC)

    def monotonic(self) -> float:
        return time.monotonic()

    async def sleep(self, seconds: float) -> None:
        await asyncio.sleep(seconds)
