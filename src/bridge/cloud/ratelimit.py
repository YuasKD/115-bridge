"""令牌桶与熔断器（T08）。

只用于 115 API 请求，CDN 字节下载不经过这里。时间全部来自注入的 Clock。
"""

from __future__ import annotations

from datetime import datetime, timedelta

from bridge.clock import Clock


class TokenBucket:
    """令牌桶：每秒补 ``rate`` 个，最多攒 ``burst`` 个。

    采用「预约」方式：先扣令牌（可以扣成负数），再按欠账睡眠，
    因此多个等待者按到达顺序排队，不需要锁。
    """

    def __init__(self, rate: float, burst: int, clock: Clock) -> None:
        if rate <= 0:
            raise ValueError("rate must be positive")
        if burst < 1:
            raise ValueError("burst must be at least 1")
        self._rate = float(rate)
        self._burst = float(burst)
        self._clock = clock
        self._tokens = float(burst)
        self._stamp = clock.monotonic()

    def _refill(self) -> None:
        now = self._clock.monotonic()
        elapsed = max(0.0, now - self._stamp)
        self._stamp = now
        self._tokens = min(self._burst, self._tokens + elapsed * self._rate)

    @property
    def available(self) -> float:
        """当前可用令牌数（可能为负，表示已有等待者）。"""
        self._refill()
        return self._tokens

    def try_acquire(self) -> bool:
        """有令牌就取走并返回 True，不等待。"""
        self._refill()
        if self._tokens >= 1.0:
            self._tokens -= 1.0
            return True
        return False

    async def acquire(self) -> None:
        """取一个令牌，不够就等到够。"""
        self._refill()
        self._tokens -= 1.0
        if self._tokens < 0.0:
            await self._clock.sleep(-self._tokens / self._rate)


class CircuitBreaker:
    """熔断计时：1 分钟起，每次再触发翻倍，最长 1 小时。

    ``trip`` 在熔断期内重复调用无效（并发的在途请求同时失败不会连续翻倍）。

    复位规则（人已拍板）：熔断期过后恢复请求，但等级要「连续 ``clean_seconds``（默认 10 分钟）
    没再触发」才复位到 1 分钟。计时从熔断期结束那一刻起算；其间再触发则等级继续翻倍。
    ``note_success`` 在每次成功调用后调用，满足条件时复位并返回 True（调用方此时发恢复通知）。
    """

    def __init__(
        self,
        clock: Clock,
        base_seconds: float = 60.0,
        cap_seconds: float = 3600.0,
        clean_seconds: float = 600.0,
    ):
        self._clock = clock
        self._base = base_seconds
        self._cap = cap_seconds
        self._clean = clean_seconds
        self._level = 0
        self._open_until_mono: float | None = None
        self._open_until_wall: datetime | None = None

    @property
    def level(self) -> int:
        """已触发的次数（0 表示从未触发或已复位）。"""
        return self._level

    def is_open(self) -> bool:
        return self._open_until_mono is not None and self._clock.monotonic() < self._open_until_mono

    def remaining(self) -> float:
        if self._open_until_mono is None:
            return 0.0
        return max(0.0, self._open_until_mono - self._clock.monotonic())

    @property
    def open_until(self) -> datetime | None:
        """熔断解除时间（墙钟）；未熔断时为 None。"""
        return self._open_until_wall if self.is_open() else None

    def trip(self) -> float:
        """触发熔断，返回本次的退避秒数。已在熔断期内则返回剩余秒数，不翻倍。"""
        if self.is_open():
            return self.remaining()
        duration = float(min(self._base * (2**self._level), self._cap))
        self._level += 1
        self._open_until_mono = self._clock.monotonic() + duration
        self._open_until_wall = self._clock.now() + timedelta(seconds=duration)
        return duration

    def note_success(self) -> bool:
        """熔断期已过且自那以后连续 ``clean_seconds`` 没再触发时复位，返回是否发生了复位。"""
        if self._level == 0 or self._open_until_mono is None or self.is_open():
            return False
        if self._clock.monotonic() - self._open_until_mono < self._clean:
            return False
        self.reset()
        return True

    def reset(self) -> None:
        self._level = 0
        self._open_until_mono = None
        self._open_until_wall = None
