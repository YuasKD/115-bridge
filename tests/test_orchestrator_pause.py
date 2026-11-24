"""T13：全局暂停、退避、并发与基础设施故障。

全局暂停期间任务状态和重试计数不变，不回退（不变量 3、4）。
"""

from __future__ import annotations

import asyncio
import sqlite3
from collections.abc import AsyncIterator
from pathlib import Path

import pytest

import test_orchestrator_support as sup
from bridge.cloud.fake import OfflineScenario
from bridge.cloud.protocol import (
    AuthExpired,
    CloudError,
    NetworkError,
    QuotaExhausted,
    RiskControl,
    ServerError,
    VerifyRequired,
)
from bridge.db.repo import Fallback, TaskState
from bridge.pipeline.fetch import FetchResult, FetchStatus
from bridge.pipeline.orchestrator import Backoff, JobKind
from bridge.pipeline.verify import TargetExists, VerifyResult, VerifyStatus
from bridge.testing.clock import FakeClock
from test_orchestrator_support import (
    Env,
    H,
    StubFetcher,
    StubVerifier,
    ff,
    put_at,
)


@pytest.fixture
async def env(tmp_path: Path) -> AsyncIterator[Env]:
    async with sup.make_env(tmp_path) as e:
        yield e


# ------------------------------------------------------------ 115 全局错误


# 115 忙（990005 等）由会话层做短退避，漏到调度器时表现为 ServerError，处理同其他全局错误
GLOBALS: list[type[CloudError]] = [
    AuthExpired,
    VerifyRequired,
    RiskControl,
    ServerError,
    NetworkError,
]


async def setup_phase(env: Env, phase: str, exc: CloudError) -> TaskState:
    """让任务停在某个阶段的入口，并在该阶段的 115 接口上注入全局错误。返回期望保持的状态。"""
    await env.add_task(OfflineScenario("Show", [ff("a.mkv", 4000), ff("b.mkv", 50)]))
    if phase == "submit":
        env.cloud.inject("add_magnet", exc, times=None)
        return TaskState.SUBMITTING
    await env.orch.tick()  # 提交成功
    assert await env.state() is TaskState.CLOUD_METADATA
    if phase == "track":
        env.cloud.inject("list_offline", exc, times=None)
        await env.clock.advance(61)
        return TaskState.CLOUD_METADATA
    await env.clock.advance(61)  # 离线完成
    if phase == "locate":
        env.cloud.inject("list_dir", exc, times=None)
        return TaskState.LOCATING
    assert phase == "fetch"
    env.cloud.inject("download_link", exc, times=None)
    return TaskState.FETCHING


@pytest.mark.parametrize("exc_type", GLOBALS)
@pytest.mark.parametrize("phase", ["submit", "track", "locate", "fetch"])
async def test_global_error_pauses_without_touching_task(
    env: Env, phase: str, exc_type: type[CloudError]
) -> None:
    expected = await setup_phase(env, phase, exc_type("boom"))
    before = None
    for _ in range(4):
        await env.orch.tick()
        await env.settle_jobs()
        await env.clock.advance(61)
        task = await env.task()
        assert task.state is expected
        assert task.retries == {}  # 不计重试
        assert task.fallback is Fallback.NONE and task.error is None
        if before is not None:
            assert task.local_bytes == before.local_bytes
        before = task
    assert env.radarr.calls == []  # 不回退
    assert env.notifier.alerts == []  # 告警由会话层负责，调度器不重复
    assert env.orch.pauses == {phase: exc_type.__name__}

    # 恢复后继续走完
    env.cloud.clear_faults()
    task = await env.drive_to(TaskState.COMPLETED)
    assert task.retries == {}
    assert env.orch.pauses == {}
    assert env.radarr.calls == []


# ------------------------------------------------------------ 配额与保存目录


async def test_quota_low_pauses_submit_and_resumes(env: Env) -> None:
    env.cloud.quota_remaining = 5  # 低于 quota_min=20
    await env.add_task(OfflineScenario("Show", [ff("a.mkv", 100)]))
    for _ in range(3):
        await env.orch.tick()
        await env.clock.advance(61)
    task = await env.task()
    assert task.state is TaskState.SUBMITTING  # 已在 submitting，没有 submitted_at
    assert task.submitted_at is None
    assert task.retries == {} and task.fallback is Fallback.NONE
    assert env.cloud.call_count("add_magnet") == 0
    assert "quota" in env.notifier.kinds
    assert env.radarr.calls == []
    assert env.orch.pauses == {"submit": "quota low"}

    env.cloud.quota_remaining = 1000  # 配额恢复后，对停在 submitting 的任务重新 submit
    task = await env.drive_to(TaskState.COMPLETED)
    assert env.cloud.call_count("add_magnet") == 1
    assert [k for k, _ in env.notifier.recoveries] == ["quota"]
    assert task.retries == {}


async def test_quota_exhausted_pauses_submit_with_backoff(tmp_path: Path) -> None:
    async with sup.make_env(tmp_path, quota_min=0) as env:
        env.cloud.quota_remaining = 0
        await env.add_task(OfflineScenario("Show", [ff("a.mkv", 100)]))
        calls: list[float] = []
        for _ in range(400):  # 每 10 秒一轮
            await env.orch.tick()
            n = env.cloud.call_count("add_magnet")
            if n > len(calls):
                calls.append(env.clock.monotonic())
            await env.clock.advance(10)
            if len(calls) >= 5:
                break
        gaps = [b - a for a, b in zip(calls, calls[1:], strict=False)]
        assert gaps == [30, 60, 120, 240]  # 30 秒起翻倍
        task = await env.task()
        assert task.state is TaskState.SUBMITTING and task.retries == {}
        assert "quota" in env.notifier.kinds
        assert env.radarr.calls == []

        env.cloud.quota_remaining = 100
        await env.drive_to(TaskState.COMPLETED, step=500)


async def test_save_dir_unavailable_pauses_submit_not_fail(env: Env) -> None:
    dir_id = await env.cloud.makedirs("/115-bridge/radarr")
    env.cloud.hide_entry(dir_id)  # 重建后仍核对不到目录
    await env.add_task(OfflineScenario("Show", [ff("a.mkv", 100)]))
    for _ in range(4):
        await env.orch.tick()
        await env.clock.advance(61)
    task = await env.task()
    assert task.state is TaskState.SUBMITTING
    assert task.retries == {} and task.fallback is Fallback.NONE
    assert env.cloud.call_count("add_magnet") == 0
    assert "save-dir" in env.notifier.kinds
    assert env.radarr.calls == []  # 问题在保存目录，不是片源：不回退

    env.cloud._hidden.discard(dir_id)
    await env.drive_to(TaskState.COMPLETED, step=500)
    assert [k for k, _ in env.notifier.recoveries] == ["save-dir"]


# ------------------------------------------------------------ 本地空间与 CDN 断网


async def test_local_space_low_pauses_fetch_and_resumes(env: Env) -> None:
    env.free_bytes = 10 * 1024**3  # 低于 50 GiB 余量
    await env.add_task(OfflineScenario("Show", [ff("a.mkv", 4000)]))
    await env.drive_to(TaskState.FETCHING)
    for _ in range(4):
        await env.orch.tick()
        await env.settle_jobs()
        await env.clock.advance(500)
        task = await env.task()
        assert task.state is TaskState.FETCHING
        assert task.retries == {} and task.fallback is Fallback.NONE
    assert env.cdn.requests == []  # 一个字节都没下
    assert "local-space" in env.notifier.kinds
    assert env.radarr.calls == []

    env.free_bytes = 10_000 * 1024**3
    await env.drive_to(TaskState.COMPLETED, step=500)
    assert [k for k, _ in env.notifier.recoveries] == ["local-space"]


async def test_cdn_down_pauses_fetch_alerts_after_30_minutes(env: Env) -> None:
    env.transport.down = True
    await env.add_task(OfflineScenario("Show", [ff("a.mkv", 4000)]))
    await env.drive_to(TaskState.FETCHING)
    await env.clock.advance(1)
    await env.orch.tick()
    await env.settle_jobs()
    assert env.orch.pauses.get("fetch") == "cdn network"
    assert "fetch-network" not in env.notifier.kinds  # 刚断，不告警

    for _ in range(80):
        await env.orch.tick()
        await env.settle_jobs()
        task = await env.task()
        assert task.state is TaskState.FETCHING
        assert task.retries == {}  # CDN 连接层错误不计重试
        await env.clock.advance(60)
    assert "fetch-network" in env.notifier.kinds  # 持续 30 分钟以上才告警
    assert env.radarr.calls == []

    env.transport.down = False
    await env.drive_to(TaskState.COMPLETED, step=1000)
    assert "fetch-network" in [k for k, _ in env.notifier.recoveries]
    assert env.orch.pauses == {}


# ------------------------------------------------------------ 退避


def test_backoff_sequence_doubles_from_30_seconds_to_one_hour() -> None:
    clock = FakeClock()
    b = Backoff(clock, 30.0, 3600.0)
    delays = [b.fail("k") for _ in range(10)]
    assert delays == [30, 60, 120, 240, 480, 960, 1920, 3600, 3600, 3600]
    assert b.failures("k") == 10
    assert not b.ready("k")
    b.reset("k")
    assert b.ready("k") and b.failures("k") == 0
    assert b.fail("k") == 30
    assert b.fail("other") == 30  # 按 key 独立


async def test_backoff_spacing_for_stuck_verify(env: Env) -> None:
    """verifying 中 TargetExists：按 30 秒起翻倍、最长 1 小时的间隔重试，不改状态。"""
    stub = StubVerifier(env.clock)

    async def exists(h: str) -> VerifyResult:
        raise TargetExists(h, "/data/x")

    stub.behavior = exists
    orch = env.make_orch(verifier=stub)
    await put_at(env, H, TaskState.VERIFYING)
    for _ in range(1500):
        await orch.tick()
        await orch.wait_jobs(1)
        await env.clock.advance(10)
        if len(stub.call_times) >= 10:
            break
    gaps = [b - a for a, b in zip(stub.call_times, stub.call_times[1:], strict=False)]
    assert gaps == [30, 60, 120, 240, 480, 960, 1920, 3600, 3600]
    task = await env.task()
    assert task.state is TaskState.VERIFYING and task.retries == {}
    assert task.fallback is Fallback.NONE and env.radarr.calls == []
    await orch.shutdown()


# ------------------------------------------------------------ 并发与基础设施


async def test_same_hash_never_fetched_twice_concurrently(env: Env) -> None:
    stub = StubFetcher(env.incomplete)
    release = asyncio.Event()

    async def slow(_h: str) -> FetchResult:
        await release.wait()
        return FetchResult(FetchStatus.DONE, 1, 1)

    stub.behavior = slow
    orch = env.make_orch(fetcher=stub)
    await put_at(env, H, TaskState.FETCHING)
    for _ in range(6):
        await orch.tick()
        await env.clock.advance(61)
        await asyncio.sleep(0)
    assert stub.calls == [H]  # 只有一个取回
    assert orch.active_jobs == {H: JobKind.FETCH}
    assert stub.peak == 1

    release.set()
    await orch.wait_jobs(5)
    assert await env.state() is TaskState.VERIFYING
    assert stub.forgotten == [H]  # 取回协程结束后才 forget
    await orch.shutdown()


async def test_fetch_infrastructure_failure_keeps_task_and_backs_off(env: Env) -> None:
    stub = StubFetcher(env.incomplete)
    times: list[float] = []

    async def broken(_h: str) -> FetchResult:
        times.append(env.clock.monotonic())
        raise sqlite3.OperationalError("database is locked")

    stub.behavior = broken
    orch = env.make_orch(fetcher=stub)
    await put_at(env, H, TaskState.FETCHING)
    for _ in range(300):
        await orch.tick()
        await orch.wait_jobs(1)
        await env.clock.advance(10)
        if len(times) >= 5:
            break
    gaps = [b - a for a, b in zip(times, times[1:], strict=False)]
    assert gaps == [30, 60, 120, 240]
    task = await env.task()
    assert task.state is TaskState.FETCHING  # 基础设施故障不是任务失败
    assert task.retries == {} and task.fallback is Fallback.NONE
    assert "infra-fetch" in env.notifier.kinds  # 出错位置带进 kind
    assert env.radarr.calls == []
    await orch.shutdown()


async def test_long_fetch_alerts_but_does_not_fail(env: Env) -> None:
    stub = StubFetcher(env.incomplete)
    never = asyncio.Event()

    async def forever(_h: str) -> FetchResult:
        await never.wait()
        return FetchResult(FetchStatus.DONE, 1, 1)

    stub.behavior = forever
    orch = env.make_orch(fetcher=stub)
    await put_at(env, H, TaskState.FETCHING)
    await orch.tick()
    await env.clock.advance(23 * 3600)
    await orch.tick()
    assert not any(k.startswith("fetch-slow") for k in env.notifier.kinds)
    await env.clock.advance(2 * 3600)
    await orch.tick()
    assert [k for k in env.notifier.kinds if k.startswith("fetch-slow")]
    assert await env.state() is TaskState.FETCHING  # 只告警，不判失败、不回退
    assert env.radarr.calls == []
    await orch.shutdown()


async def test_missing_cloud_root_alerts_without_fail(env: Env) -> None:
    await put_at(env, H, TaskState.LOCATING)  # 没有 cloud_file_id：调度器自身缺陷
    for _ in range(3):
        await env.orch.tick()
        await env.clock.advance(100)
    task = await env.task()
    assert task.state is TaskState.LOCATING
    assert task.fallback is Fallback.NONE and env.radarr.calls == []
    assert [k for k in env.notifier.kinds if k.startswith("locate-bug")]


async def test_state_changed_under_us_abandons_step(env: Env) -> None:
    """add_magnet 期间 torrents/delete 把任务转成 removed：调度器的 expected 转换放弃，不报错。"""
    await env.add_task(OfflineScenario("Show", [ff("a.mkv", 100)]))
    original = env.cloud.add_magnet

    async def racing(magnet: str, wp_path_id: str) -> str:
        result = await original(magnet, wp_path_id)
        await env.repo.transition(H, TaskState.REMOVED, "torrents/delete", env.clock.now())
        return result

    env.cloud.add_magnet = racing  # type: ignore[method-assign]
    for _ in range(3):
        await env.orch.tick()
        await env.clock.advance(61)
    assert await env.state() is TaskState.REMOVED  # 没有被覆盖成 cloud_metadata
    events = await env.repo.list_events(H)
    assert events[0].to_state is TaskState.REMOVED  # 最新一条仍是删除
    assert env.radarr.calls == []
    assert env.notifier.alerts == []


async def test_quota_exhausted_class_is_not_task_failure(env: Env) -> None:
    env.cloud.inject("add_magnet", QuotaExhausted("full"), times=None)
    await env.add_task(OfflineScenario("Show", [ff("a.mkv", 100)]))
    for _ in range(3):
        await env.orch.tick()
        await env.clock.advance(500)
    task = await env.task()
    assert task.state is TaskState.SUBMITTING and task.retries == {}
    assert env.radarr.calls == []


# ------------------------------------------------------------ run()


async def test_run_loops_every_poll_interval_and_wakes_on_job_end(env: Env) -> None:
    stub_f = StubFetcher(env.incomplete)
    stub_v = StubVerifier(env.clock)

    async def renamed(_h: str) -> VerifyResult:
        return VerifyResult(VerifyStatus.RENAMED, final_path=str(env.root / "radarr" / "Show"))

    stub_v.behavior = renamed
    orch = env.make_orch(fetcher=stub_f, verifier=stub_v)
    ticks = 0
    real_tick = orch.tick

    async def counting_tick() -> None:
        nonlocal ticks
        ticks += 1
        await real_tick()

    orch.tick = counting_tick  # type: ignore[method-assign]
    await put_at(env, H, TaskState.FETCHING)
    runner = asyncio.create_task(orch.run())
    # 取回、校验两个后台协程结束时各唤醒一轮，不必等满 60 秒；数据库在线程里，这里等真实的短时间
    for _ in range(1000):
        if await env.state() is TaskState.COMPLETED:
            break
        await asyncio.sleep(0.005)
    await env.clock.settle(20)
    assert await env.state() is TaskState.COMPLETED
    base = ticks

    async def wait_sleeping() -> None:
        for _ in range(1000):
            if env.clock.pending_sleepers:
                return
            await asyncio.sleep(0.005)
        raise AssertionError("run loop never went to sleep")

    async def wait_ticks(n: int) -> None:
        for _ in range(1000):
            if ticks >= n:
                return
            await asyncio.sleep(0.005)
        raise AssertionError("tick did not run")

    assert base >= 3
    await wait_sleeping()
    base = ticks
    await env.clock.advance(59)
    await asyncio.sleep(0.1)
    assert ticks == base  # 间隔没到
    await env.clock.advance(2)
    await wait_ticks(base + 1)  # poll_seconds = 60
    await asyncio.sleep(0.1)
    assert ticks == base + 1
    await wait_sleeping()
    await env.clock.advance(60)
    await wait_ticks(base + 2)
    runner.cancel()
    await asyncio.gather(runner, return_exceptions=True)


async def test_global_error_during_instant_check_leaves_task_queued(env: Env) -> None:
    await env.add_task(OfflineScenario("Show", [ff("a.mkv", 100)]))
    await env.drive_terminal()
    await env.repo.transition(H, TaskState.REMOVED, "d", env.clock.now())
    await env.repo.requeue_removed(H, "again", env.clock.now())
    env.cloud.inject("get_entry", RiskControl("slow down"), times=None)
    for _ in range(3):
        await env.orch.tick()
        await env.clock.advance(61)
    task = await env.task()
    assert task.state is TaskState.QUEUED  # 不转 submitting，也不回退
    assert task.retries == {} and env.radarr.calls == []
    assert env.orch.pauses == {"queued": "RiskControl"}
