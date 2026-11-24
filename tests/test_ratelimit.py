from __future__ import annotations

import asyncio

import pytest

from bridge.cloud.ratelimit import CircuitBreaker, TokenBucket
from bridge.testing.clock import FakeClock


async def test_burst_then_rate() -> None:
    clock = FakeClock()
    bucket = TokenBucket(rate=1, burst=3, clock=clock)
    for _ in range(3):  # 突发 3 个不等待：假时钟不推进，任务也能完成
        burst_task = asyncio.create_task(bucket.acquire())
        await clock.settle()
        assert burst_task.done()
    task = asyncio.create_task(bucket.acquire())
    await clock.settle()
    assert not task.done()  # 第 4 个要等 1 秒
    await clock.advance(0.9)
    assert not task.done()
    await clock.advance(0.2)
    assert task.done()


async def test_waiters_are_spaced_by_rate() -> None:
    clock = FakeClock()
    bucket = TokenBucket(rate=2, burst=1, clock=clock)
    await bucket.acquire()
    done: list[float] = []

    async def worker() -> None:
        await bucket.acquire()
        done.append(clock.monotonic())

    tasks = [asyncio.create_task(worker()) for _ in range(3)]
    await clock.settle()
    start = clock.monotonic()
    await clock.advance(2.0)
    assert len(done) == 3
    # rate=2/s：三个等待者依次相隔 0.5 秒
    assert [round(t - start, 3) for t in done] == [0.5, 1.0, 1.5]
    await asyncio.gather(*tasks)


async def test_refill_capped_at_burst() -> None:
    clock = FakeClock()
    bucket = TokenBucket(rate=1, burst=3, clock=clock)
    await clock.advance(1000)
    assert bucket.available == pytest.approx(3.0)
    assert bucket.try_acquire() and bucket.try_acquire() and bucket.try_acquire()
    assert not bucket.try_acquire()


def test_bucket_rejects_bad_params() -> None:
    with pytest.raises(ValueError):
        TokenBucket(rate=0, burst=3, clock=FakeClock())
    with pytest.raises(ValueError):
        TokenBucket(rate=1, burst=0, clock=FakeClock())


async def test_circuit_doubles_and_caps_at_one_hour() -> None:
    clock = FakeClock()
    breaker = CircuitBreaker(clock)
    durations = []
    for _ in range(9):
        durations.append(breaker.trip())
        assert breaker.is_open()
        await clock.advance(durations[-1])
        assert not breaker.is_open()
    assert durations == [60, 120, 240, 480, 960, 1920, 3600, 3600, 3600]


async def test_trip_inside_open_window_does_not_double() -> None:
    clock = FakeClock()
    breaker = CircuitBreaker(clock)
    assert breaker.trip() == 60
    await clock.advance(10)
    assert breaker.trip() == pytest.approx(50)  # 并发的在途请求同时失败，不再翻倍
    assert breaker.level == 1


async def test_reset_restarts_from_one_minute() -> None:
    clock = FakeClock()
    breaker = CircuitBreaker(clock)
    breaker.trip()
    await clock.advance(61)
    breaker.trip()
    breaker.reset()
    assert breaker.level == 0
    assert not breaker.is_open()
    assert breaker.trip() == 60
    assert breaker.open_until is not None


async def test_level_resets_only_after_ten_clean_minutes() -> None:
    clock = FakeClock()
    breaker = CircuitBreaker(clock)
    assert breaker.trip() == 60
    assert not breaker.note_success()  # 熔断期内
    await clock.advance(61)
    assert not breaker.is_open()
    assert not breaker.note_success()  # 熔断期过了，但等级要连续 10 分钟没再触发
    assert breaker.level == 1
    await clock.advance(598)  # 自熔断期结束起 599 秒
    assert not breaker.note_success() and breaker.level == 1
    await clock.advance(1)
    assert breaker.note_success()  # 满 10 分钟，复位并报告一次
    assert breaker.level == 0
    assert not breaker.note_success()  # 之后不会重复报告
    assert breaker.trip() == 60


async def test_intermittent_trips_keep_doubling_until_cap_then_reset() -> None:
    clock = FakeClock()
    breaker = CircuitBreaker(clock)
    durations = []
    for _ in range(9):
        durations.append(breaker.trip())
        await clock.advance(durations[-1] + 599)  # 每次恢复后不到 10 分钟又触发
        assert not breaker.note_success()
        await clock.advance(0.5)
        assert not breaker.note_success()
        await clock.advance(0.5)
    assert durations == [60, 120, 240, 480, 960, 1920, 3600, 3600, 3600]
    assert breaker.level == 9
    assert breaker.note_success()
    assert breaker.level == 0
    assert breaker.trip() == 60


async def test_trip_during_clean_window_restarts_the_clock() -> None:
    clock = FakeClock()
    breaker = CircuitBreaker(clock)
    breaker.trip()
    await clock.advance(61 + 590)
    assert breaker.trip() == 120  # 差 10 秒满 10 分钟时再次触发：继续翻倍
    await clock.advance(121 + 300)
    assert not breaker.note_success()  # 计时从新一轮熔断结束重新算
    await clock.advance(300)
    assert breaker.note_success()
