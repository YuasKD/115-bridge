"""取回进度：滑动窗口速度、ETA、等待空间标志。实现 ``api.qbit.ProgressSource``。

时间一律来自注入的 Clock（monotonic），不读系统时间。

内存与计算量有固定上界：样本按整秒合并成桶，``record()`` 自己就清理窗口外的桶，
所以桶数不超过 ``window_seconds + 2``，与写入速度、也与 ``progress()`` 的轮询频率无关
（Arr 长时间不轮询时内存不会增长）。
"""

from __future__ import annotations

import math
from collections import deque
from dataclasses import dataclass, field

from bridge.clock import Clock
from bridge.progress_protocol import ETA_INFINITE, FetchProgress

DEFAULT_WINDOW_SECONDS = 10.0


@dataclass(slots=True)
class _Track:
    buckets: deque[list[int]] = field(default_factory=deque)  # [整秒, 该秒写入的字节数]
    first_t: float | None = None
    remaining: int = 0
    waiting_for_space: bool = False


class ProgressTracker:
    """按 hash 记录写入的字节，给出 dlspeed、eta、waiting_for_space。"""

    def __init__(self, clock: Clock, window_seconds: float = DEFAULT_WINDOW_SECONDS) -> None:
        self._clock = clock
        self._window = window_seconds
        self._tracks: dict[str, _Track] = {}

    def _track(self, info_hash: str) -> _Track:
        t = self._tracks.get(info_hash)
        if t is None:
            t = _Track()
            self._tracks[info_hash] = t
        return t

    def begin(self, info_hash: str, remaining: int) -> None:
        """开始（或重新开始）取回：设置剩余字节，清空速度样本。"""
        t = self._track(info_hash)
        t.buckets.clear()
        t.first_t = None
        t.remaining = max(0, remaining)

    def record(self, info_hash: str, nbytes: int) -> None:
        t = self._track(info_hash)
        now = self._clock.monotonic()
        if t.first_t is None:
            t.first_t = now
        sec = math.floor(now)
        if t.buckets and t.buckets[-1][0] == sec:
            t.buckets[-1][1] += nbytes
        else:
            t.buckets.append([sec, nbytes])
        self._prune(t, now)
        t.remaining = max(0, t.remaining - nbytes)

    def set_waiting(self, info_hash: str, waiting: bool) -> None:
        self._track(info_hash).waiting_for_space = waiting

    def clear(self, info_hash: str) -> None:
        self._tracks.pop(info_hash, None)

    def _prune(self, t: _Track, now: float) -> None:
        # 桶 [s, s+1) 完全落在窗口之外才丢弃
        cutoff = now - self._window
        while t.buckets and t.buckets[0][0] + 1 <= cutoff:
            t.buckets.popleft()

    def progress(self, info_hash: str) -> FetchProgress:
        t = self._tracks.get(info_hash)
        if t is None:
            return FetchProgress()
        now = self._clock.monotonic()
        self._prune(t, now)
        speed = 0
        eta = ETA_INFINITE
        if t.buckets and t.first_t is not None:
            total = sum(b[1] for b in t.buckets)
            elapsed = min(max(now - t.first_t, 1.0), self._window)
            speed = int(total / elapsed)
            if speed > 0:
                eta = min(ETA_INFINITE, -(-t.remaining // speed))
        return FetchProgress(dlspeed=speed, eta=eta, waiting_for_space=t.waiting_for_space)
