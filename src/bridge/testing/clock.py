"""假时钟：实现 ``bridge.clock.Clock``，``advance`` 推进时间并唤醒到期的 sleep。"""

from __future__ import annotations

import asyncio
import heapq
import itertools
from datetime import UTC, datetime, timedelta


class FakeClock:
    def __init__(self, start: datetime | None = None, monotonic_start: float = 1000.0) -> None:
        self._start = start or datetime(2026, 1, 1, tzinfo=UTC)
        self._mono_start = monotonic_start
        self._mono = monotonic_start
        self._seq = itertools.count()
        self._waiters: list[tuple[float, int, asyncio.Future[None]]] = []

    def now(self) -> datetime:
        return self._start + timedelta(seconds=self._mono - self._mono_start)

    def monotonic(self) -> float:
        return self._mono

    @property
    def pending_sleepers(self) -> int:
        return sum(1 for _, _, f in self._waiters if not f.done())

    async def sleep(self, seconds: float) -> None:
        if seconds <= 0:
            await asyncio.sleep(0)
            return
        fut: asyncio.Future[None] = asyncio.get_running_loop().create_future()
        heapq.heappush(self._waiters, (self._mono + seconds, next(self._seq), fut))
        await fut

    async def settle(self, rounds: int = 10) -> None:
        """让出事件循环若干次，使已被唤醒的协程跑完。"""
        for _ in range(rounds):
            await asyncio.sleep(0)

    async def advance(self, seconds: float) -> None:
        """推进 ``seconds`` 秒，按到期顺序唤醒 sleep，每次唤醒后让出事件循环。"""
        if seconds < 0:
            raise ValueError("cannot go back in time")
        target = self._mono + seconds
        await self.settle()
        while self._waiters and self._waiters[0][0] <= target:
            deadline, _, fut = heapq.heappop(self._waiters)
            if fut.done():  # 已取消
                continue
            self._mono = max(self._mono, deadline)
            fut.set_result(None)
            await self.settle()
        self._mono = target
        await self.settle()
