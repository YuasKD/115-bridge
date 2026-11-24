"""T13 返工：审查发现的缺陷与人拍板的行为。

- 落盘后写 completed 失败不会重新校验
- 单个任务的意外异常不挡住同阶段其他任务
- Fetcher 事件回调驱动的连续 CDN 403 告警
- 停住原因、内存集合与暂停标记的清理、CDN 退避只升一级
- 取回、校验协程的 ``_fail`` 与 Arr 回调竞争
- verify 落盘与取消的竞态、LocalCleaner 的 realpath 防护
"""

from __future__ import annotations

import asyncio
import os
import sqlite3
import sys
import threading
from collections.abc import AsyncIterator, Callable
from pathlib import Path
from typing import Any

import pytest

import test_orchestrator_support as sup
from bridge.arr.fake import FakeArr
from bridge.cloud.fake import CdnFault, OfflineScenario
from bridge.cloud.protocol import CloudError, NetworkError
from bridge.config import FetchConfig
from bridge.db.repo import Fallback, Stage, TaskState
from bridge.pipeline import orchestrator as orch_mod
from bridge.pipeline import verify as verify_mod
from bridge.pipeline.fetch import Fetcher, FetchFailed, FetchResult
from bridge.pipeline.orchestrator import UnsafeLocalPath
from bridge.pipeline.verify import VerifyFailed, VerifyStatus
from test_orchestrator_support import (
    H2,
    H3,
    Env,
    H,
    StubFetcher,
    StubVerifier,
    ff,
    put_at,
)

H4 = "12" * 20
H5 = "34" * 20


@pytest.fixture
async def env(tmp_path: Path) -> AsyncIterator[Env]:
    async with sup.make_env(tmp_path) as e:
        yield e


async def wait_until(pred: Callable[[], bool], limit: int = 2000) -> None:
    for _ in range(limit):
        if pred():
            return
        await asyncio.sleep(0.005)
    raise AssertionError("condition not reached")


# ------------------------------------------------------------ 1. 落盘后写 completed 失败


async def test_completed_write_failure_after_landing_never_reverifies(
    env: Env, monkeypatch: pytest.MonkeyPatch
) -> None:
    f = ff("a.mkv", 3000)
    await env.add_task(OfflineScenario("Show", [f]))
    verify_calls: list[str] = []
    real_verify = env.verifier.verify

    async def counting(info_hash: str) -> Any:
        verify_calls.append(info_hash)
        return await real_verify(info_hash)

    monkeypatch.setattr(env.verifier, "verify", counting)
    real_transition = env.repo.transition
    failures: list[int] = []

    async def flaky(info_hash: str, dst: TaskState, *a: Any, **kw: Any) -> Any:
        if dst is TaskState.COMPLETED and not failures:
            failures.append(1)
            raise sqlite3.OperationalError("database is locked")
        return await real_transition(info_hash, dst, *a, **kw)

    monkeypatch.setattr(env.repo, "transition", flaky)

    task = await env.drive_terminal()
    assert failures == [1]
    assert task.state is TaskState.COMPLETED  # 之后转 completed 成功
    assert len(verify_calls) == 1  # 绝不再调用 verify
    assert Stage.VERIFY not in task.retries  # VERIFY 计数不变
    assert task.fallback is Fallback.NONE and env.radarr.calls == []  # 没有回退
    assert (env.root / "radarr" / "Show" / "a.mkv").read_bytes() == f.content  # 文件仍在正式目录
    assert "infra-verify" in env.notifier.kinds
    assert env.orch._landed == {}


async def test_completed_write_failure_is_retried_with_per_task_backoff(
    env: Env, monkeypatch: pytest.MonkeyPatch
) -> None:
    await env.add_task(OfflineScenario("Show", [ff("a.mkv", 3000)]))
    real_transition = env.repo.transition
    times: list[float] = []

    async def always_fail(info_hash: str, dst: TaskState, *a: Any, **kw: Any) -> Any:
        if dst is TaskState.COMPLETED:
            times.append(env.clock.monotonic())
            raise sqlite3.OperationalError("database is locked")
        return await real_transition(info_hash, dst, *a, **kw)

    monkeypatch.setattr(env.repo, "transition", always_fail)
    await env.drive_to(TaskState.VERIFYING)
    for _ in range(80):
        await env.orch.tick()
        await env.settle_jobs()
        await env.clock.advance(10)
        if len(times) >= 4:
            break
    gaps = [b - a for a, b in zip(times, times[1:], strict=False)]
    assert gaps == [30, 60, 120]
    assert (await env.task()).state is TaskState.VERIFYING
    assert (env.root / "radarr" / "Show" / "a.mkv").exists()


# ------------------------------------------------------------ 2. 单个任务的异常不挡住别人


@pytest.mark.parametrize("stage", ["queued", "submit", "locate"])
@pytest.mark.parametrize("kind", ["runtime", "cloud"])
async def test_one_tasks_error_does_not_block_the_stage(
    env: Env, monkeypatch: pytest.MonkeyPatch, stage: str, kind: str
) -> None:
    await env.add_task(OfflineScenario("A", [ff("a.mkv", 2000)]), info_hash=H)
    await env.add_task(OfflineScenario("B", [ff("b.mkv", 2000)]), info_hash=H2, arr_item=8)
    target = {"queued": "check_instant", "submit": "submit", "locate": "locate"}[stage]
    real = getattr(orch_mod, target)
    times: list[float] = []

    async def flaky(task: Any, *a: Any, **kw: Any) -> Any:
        if task.hash == H:
            times.append(env.clock.monotonic())
            raise RuntimeError("boom") if kind == "runtime" else CloudError("boom")
        return await real(task, *a, **kw)

    monkeypatch.setattr(orch_mod, target, flaky)

    done = await env.drive_to(TaskState.COMPLETED, H2)
    assert done.state is TaskState.COMPLETED  # 后面的任务照常推进
    stuck = await env.task(H)
    expected_state = {
        "queued": TaskState.QUEUED,
        "submit": TaskState.SUBMITTING,
        "locate": TaskState.LOCATING,
    }[stage]
    assert stuck.state is expected_state
    assert stuck.retries == {} and stuck.fallback is Fallback.NONE
    assert env.radarr.calls == []
    expected_kind = f"infra-{stage}" if kind == "runtime" else "cloud-unexpected"
    assert expected_kind in env.notifier.kinds
    assert env.orch.stall_reason(H)  # 停住原因可见

    # 出错的任务按退避重试：每次间隔翻倍
    seq = [times[-1]]
    for _ in range(300):
        before = len(times)
        await env.orch.tick()
        if len(times) > before:
            seq.append(times[-1])
        await env.clock.advance(10)
        if len(seq) >= 4:
            break
    gaps = [b - a for a, b in zip(seq, seq[1:], strict=False)]
    assert len(gaps) >= 3
    assert gaps[1] == 2 * gaps[0] and gaps[2] == 2 * gaps[1]
    assert gaps[0] >= 30


async def test_error_in_job_spawn_does_not_block_other_tasks(
    env: Env, monkeypatch: pytest.MonkeyPatch
) -> None:
    await env.add_task(OfflineScenario("A", [ff("a.mkv", 2000)]), info_hash=H)
    await env.add_task(OfflineScenario("B", [ff("b.mkv", 2000)]), info_hash=H2, arr_item=8)
    real = env.orch._check_fetch_age

    async def flaky(task: Any) -> None:
        if task.hash == H:
            raise RuntimeError("boom")
        await real(task)

    monkeypatch.setattr(env.orch, "_check_fetch_age", flaky)
    done = await env.drive_to(TaskState.COMPLETED, H2)
    assert done.state is TaskState.COMPLETED
    assert (await env.task(H)).state is TaskState.FETCHING
    assert "infra-fetch" in env.notifier.kinds


async def test_error_applying_one_observation_does_not_block_others(
    env: Env, monkeypatch: pytest.MonkeyPatch
) -> None:
    await env.add_task(OfflineScenario("A", [ff("a.mkv", 2000)]), info_hash=H)
    await env.add_task(OfflineScenario("B", [ff("b.mkv", 2000)]), info_hash=H2, arr_item=8)
    real = env.orch._apply_observation

    async def flaky(info_hash: str, obs: Any) -> None:
        if info_hash == H:
            raise RuntimeError("boom")
        await real(info_hash, obs)

    monkeypatch.setattr(env.orch, "_apply_observation", flaky)
    done = await env.drive_to(TaskState.COMPLETED, H2)
    assert done.state is TaskState.COMPLETED
    assert (await env.task(H)).state in (TaskState.CLOUD_METADATA, TaskState.CLOUD_DOWNLOADING)
    assert "infra-track" in env.notifier.kinds


async def test_error_in_one_fallback_does_not_block_others(
    env: Env, monkeypatch: pytest.MonkeyPatch
) -> None:
    dead = OfflineScenario("Dead", [ff("a.mkv", 100)], outcome=-1)
    await env.add_task(dead, info_hash=H)
    await env.add_task(dead, info_hash=H2, arr_item=8)
    real = env.orch._fallback_one

    async def flaky(info_hash: str) -> None:
        if info_hash == H:
            raise RuntimeError("boom")
        await real(info_hash)

    monkeypatch.setattr(env.orch, "_fallback_one", flaky)

    async def until() -> bool:
        return (await env.task(H2)).fallback is Fallback.DONE

    await env.drive(until)
    assert (await env.task(H)).fallback is Fallback.PENDING
    assert "infra-fallback" in env.notifier.kinds


# ------------------------------------------------------------ 3. 连续 CDN 403（真 Fetcher）


async def add_with_403(env: Env, specs: list[tuple[str, str]], faults: bool = True) -> None:
    """建任务、让它提交一次以便知道 pickcode，再给这些文件排上很多个 403。"""
    for h, name in specs:
        await env.add_task(OfflineScenario(name, [ff(f"{name}.mkv", 3000)]), info_hash=h)
    await env.orch.tick()
    if not faults:
        return
    for _h, name in specs:
        [entry] = [e for e in env.cloud.entries_by_name(f"{name}.mkv") if not e.is_dir]
        assert entry.pickcode is not None
        env.cdn.queue_fault(entry.pickcode, *[CdnFault("status", status=403)] * 400)


async def drive_all_terminal(env: Env, hashes: list[str]) -> None:
    async def until() -> bool:
        states = [await env.state(h) for h in hashes]
        return all(s in (TaskState.COMPLETED, TaskState.FAILED, TaskState.REMOVED) for s in states)

    await env.drive(until, step=400)
    await env.orch.tick()


async def test_three_consecutive_403_files_alert_once_before_any_task_is_failed(
    env: Env,
) -> None:
    await add_with_403(env, [(H, "A"), (H2, "B"), (H3, "C")])

    async def alerted() -> bool:
        return "cdn-403" in env.notifier.kinds

    await env.drive(alerted, step=61)
    for h in (H, H2, H3):
        assert await env.state(h) is TaskState.FETCHING  # 告警时还没有任何任务被拉黑
    assert env.radarr.calls == []
    assert {(e[1], e[2]) for e in env.cdn_events if e[0] == "403"} == {
        (H, "A.mkv"),
        (H2, "B.mkv"),
        (H3, "C.mkv"),
    }

    await drive_all_terminal(env, [H, H2, H3])
    assert env.notifier.kinds.count("cdn-403") == 1  # 同一连串只告警一次
    for h in (H, H2, H3):
        assert await env.state(h) is TaskState.FAILED


async def test_403_streak_is_by_distinct_files_not_attempts(env: Env) -> None:
    await add_with_403(env, [(H, "A"), (H2, "B")])
    await drive_all_terminal(env, [H, H2])  # 每个文件 403 很多次，但只有两个文件
    assert len([e for e in env.cdn_events if e[0] == "403"]) > 2
    assert "cdn-403" not in env.notifier.kinds


async def test_successful_file_resets_403_streak(env: Env) -> None:
    await add_with_403(env, [(H, "A"), (H2, "B")])
    await drive_all_terminal(env, [H, H2])
    assert len(env.orch._cdn_403_files) == 2

    await env.add_task(OfflineScenario("C", [ff("C.mkv", 3000)]), info_hash=H3, arr_item=9)
    task = await env.drive_to(TaskState.COMPLETED, H3)
    assert task.state is TaskState.COMPLETED
    assert env.orch._cdn_403_files == set()  # 有文件成功，清零

    await add_with_403(env, [(H4, "D"), (H5, "E")])
    await drive_all_terminal(env, [H4, H5])
    assert "cdn-403" not in env.notifier.kinds  # 4 个 403 文件，但中间有成功，不算连续


async def test_dropping_connections_is_not_reported_as_403(env: Env) -> None:
    env.cdn.max_connections = 1  # 第 2 个连接被 403，降到 1 连接后成功
    await env.add_task(OfflineScenario("A", [ff("a.mkv", 9000)]))
    task = await env.drive_terminal()
    assert task.state is TaskState.COMPLETED
    assert any(r.status == 403 for r in env.cdn.requests)
    assert [e for e in env.cdn_events if e[0] == "403"] == []
    assert [e for e in env.cdn_events if e[0] == "done"] == [("done", H, "a.mkv")]
    assert env.notifier.alerts == []


async def test_callback_errors_never_break_fetch(env: Env) -> None:
    def boom(_h: str, _p: str) -> None:
        raise RuntimeError("callback bug")

    fetcher = Fetcher(
        repo=env.repo,
        cloud=env.cloud,
        http=env._http,
        clock=env.clock,
        config=FetchConfig(),
        incomplete_dir=env.incomplete,
        free_space=lambda _p: env.free_bytes,
        read_chunk_bytes=1,
        on_cdn_403=boom,
        on_file_done=boom,
    )
    env.orch = env.make_orch(fetcher=fetcher)
    f = ff("a.mkv", 5000)
    await env.add_task(OfflineScenario("Show", [f]))
    task = await env.drive_terminal()
    assert task.state is TaskState.COMPLETED
    assert (env.root / "radarr" / "Show" / "a.mkv").read_bytes() == f.content


async def test_file_done_clears_cdn_failure_clock(env: Env) -> None:
    env.orch._cdn_fail_since = 5.0
    env.orch.on_file_done(H, "a.mkv")
    assert env.orch._cdn_fail_since is None


# ------------------------------------------------------------ 6. 停住原因


async def test_stall_reason_target_exists_then_cleared(env: Env) -> None:
    await env.add_task(OfflineScenario("Show", [ff("a.mkv", 3000)]))
    existing = env.root / "radarr" / "Show"
    existing.mkdir(parents=True)
    await env.drive_to(TaskState.VERIFYING)
    assert env.orch.stall_reason(H) is None
    await env.orch.tick()
    await env.settle_jobs()
    reason = env.orch.stall_reason(H)
    assert reason is not None and "目标路径已存在" in reason and str(existing) in reason
    assert (await env.task()).error is None  # 不写进 tasks.error

    existing.rmdir()
    await env.drive_to(TaskState.COMPLETED)
    assert env.orch.stall_reason(H) is None  # 恢复推进后清除


async def test_stall_reason_land_os_error(env: Env) -> None:
    await env.add_task(OfflineScenario("Show", [ff("a.mkv", 3000)]))
    env.root.mkdir(parents=True)
    (env.root / "radarr").write_text("a file where the category dir should be")
    await env.drive_to(TaskState.VERIFYING)
    await env.orch.tick()
    await env.settle_jobs()
    reason = env.orch.stall_reason(H)
    assert reason is not None and "落盘磁盘错误" in reason
    assert (await env.task()).error is None


async def test_stall_reason_waiting_for_space_then_cleared(env: Env) -> None:
    env.free_bytes = 10 * 1024**3
    await env.add_task(OfflineScenario("Show", [ff("a.mkv", 3000)]))
    await env.drive_to(TaskState.FETCHING)
    await env.orch.tick()
    await env.settle_jobs()
    assert env.orch.stall_reason(H) == "等待本地空间"
    env.free_bytes = 10_000 * 1024**3
    await env.drive_to(TaskState.COMPLETED, step=500)
    assert env.orch.stall_reason(H) is None


async def test_stall_reason_infrastructure_failure(env: Env) -> None:
    stub = StubFetcher(env.incomplete)

    async def broken(_h: str) -> FetchResult:
        raise sqlite3.OperationalError("database is locked")

    stub.behavior = broken
    orch = env.make_orch(fetcher=stub)
    await put_at(env, H, TaskState.FETCHING)
    await orch.tick()
    await orch.wait_jobs(5)
    reason = orch.stall_reason(H)
    assert reason is not None and "基础设施故障" in reason
    assert (await env.task()).error is None
    await orch.shutdown()


async def test_stall_reason_dropped_when_task_is_removed(env: Env) -> None:
    await env.add_task(OfflineScenario("Show", [ff("a.mkv", 3000)]))
    (env.root / "radarr" / "Show").mkdir(parents=True)
    await env.drive_to(TaskState.VERIFYING)
    await env.orch.tick()
    await env.settle_jobs()
    assert env.orch.stall_reason(H) is not None
    await env.repo.transition(H, TaskState.REMOVED, "torrents/delete", env.clock.now())
    await env.orch.tick()
    assert env.orch.stall_reason(H) is None
    assert env.orch.pauses == {}  # 没有 verifying 任务了，verify 暂停标记一并清除


# ------------------------------------------------------------ 7. _fail 与 Arr 回调竞争


class CallbackArr(FakeArr):
    """Arr 的 DELETE queue 同步回调本进程的 torrents/delete。"""

    def __init__(self, client: Any) -> None:
        super().__init__(name="radarr")
        self.client = client

    async def remove_and_blocklist(self, queue_id: int) -> None:
        await super().remove_and_blocklist(queue_id)
        r = await self.client.post("/torrents/delete", data={"hashes": H, "deleteFiles": "true"})
        assert r.status_code == 200


@pytest.mark.parametrize("stage", ["fetch", "verify"])
async def test_job_failure_races_with_fallback_and_arr_callback(
    env: Env, monkeypatch: pytest.MonkeyPatch, stage: str
) -> None:
    arr = CallbackArr(None)
    arr.add_queue_item(H, 5)
    fetch_stub = StubFetcher(env.incomplete)
    verify_stub = StubVerifier(env.clock)

    async def fetch_fails(h: str) -> FetchResult:
        raise FetchFailed(h, 6, "boom")

    async def verify_fails(h: str) -> Any:
        raise VerifyFailed(h, 3, ("a.mkv",))

    fetch_stub.behavior = fetch_fails
    verify_stub.behavior = verify_fails
    orch = env.make_orch(fetcher=fetch_stub, verifier=verify_stub, arrs={"radarr": arr})
    env.orch = orch
    arr.client = await env.qbit()  # 真实 qBit ASGI 应用 + LocalCleaner
    await put_at(env, H, TaskState.FETCHING if stage == "fetch" else TaskState.VERIFYING)

    # 模拟「_fail 已转 failed、还没写 error 与回退」的瞬间，别处的 tick 抢先跑了回退阶段
    real_set_error = env.repo.set_error
    seen: list[int] = []

    async def racing(info_hash: str, message: str) -> Any:
        if not seen:
            await orch._run_fallbacks()
            seen.append(len(arr.calls))
        return await real_set_error(info_hash, message)

    monkeypatch.setattr(env.repo, "set_error", racing)
    await orch.tick()
    await orch.wait_jobs(5)
    assert seen == [0]  # 协程还在收尾时，回退阶段不碰这个任务

    await orch.tick()
    task = await env.task()
    assert task.state is TaskState.FAILED
    assert task.client_deleted is True  # Arr 的回调 torrents/delete 到达
    assert task.fallback is Fallback.DONE
    assert task.error and ("fetch failed" if stage == "fetch" else "verify failed") in task.error
    assert arr.removed == [5]
    assert len(arr.calls_to("find_queue_item")) == 1
    await orch.shutdown()


async def test_fail_does_not_overwrite_done_fallback(
    env: Env, monkeypatch: pytest.MonkeyPatch
) -> None:
    await put_at(env, H, TaskState.CLOUD_DOWNLOADING)
    real_set_error = env.repo.set_error

    async def finish_in_between(info_hash: str, message: str) -> Any:
        await env.repo.mark_client_deleted(info_hash)
        await env.repo.set_fallback(info_hash, Fallback.DONE)
        return await real_set_error(info_hash, message)

    monkeypatch.setattr(env.repo, "set_error", finish_in_between)
    await env.orch._fail(H, "offline task failed (status -1)")
    task = await env.task()
    assert task.fallback is Fallback.DONE  # 没有被覆盖回 PENDING
    assert task.error and "status -1" in task.error


# ------------------------------------------------------------ 8. verify 落盘与取消


@pytest.fixture
def slow_land(monkeypatch: pytest.MonkeyPatch) -> tuple[threading.Event, threading.Event]:
    entered = threading.Event()
    release = threading.Event()
    real_land = verify_mod._land

    def land(*a: Any, **kw: Any) -> bool:
        entered.set()
        assert release.wait(20)
        return real_land(*a, **kw)

    monkeypatch.setattr(verify_mod, "_land", land)
    return entered, release


async def test_cancel_during_landing_waits_and_reports_result(
    env: Env, slow_land: tuple[threading.Event, threading.Event]
) -> None:
    entered, release = slow_land
    f = ff("a.mkv", 3000)
    await env.add_task(OfflineScenario("Show", [f]))
    await env.drive_to(TaskState.VERIFYING)
    job = asyncio.ensure_future(env.verifier.verify(H))
    await wait_until(entered.is_set)
    job.cancel()
    await asyncio.sleep(0.05)
    assert not job.done()  # 取消没有让调用方先走：它在等线程落盘完成
    release.set()
    result = await job
    assert result.status is VerifyStatus.RENAMED  # 调用方知道已落盘
    assert result.final_path == str(env.root / "radarr" / "Show")
    assert (env.root / "radarr" / "Show" / "a.mkv").read_bytes() == f.content


async def test_cancel_during_landing_that_fails_stays_cancelled(
    env: Env, monkeypatch: pytest.MonkeyPatch
) -> None:
    entered = threading.Event()
    release = threading.Event()

    def land(*_a: Any, **_kw: Any) -> bool:
        entered.set()
        assert release.wait(20)
        raise PermissionError("denied")

    monkeypatch.setattr(verify_mod, "_land", land)
    await env.add_task(OfflineScenario("Show", [ff("a.mkv", 3000)]))
    await env.drive_to(TaskState.VERIFYING)
    job = asyncio.ensure_future(env.verifier.verify(H))
    await wait_until(entered.is_set)
    job.cancel()
    release.set()
    with pytest.raises(asyncio.CancelledError):
        await job


async def test_delete_during_landing_cleans_landed_content(
    env: Env, slow_land: tuple[threading.Event, threading.Event]
) -> None:
    entered, release = slow_land
    await env.add_task(OfflineScenario("Show", [ff("a.mkv", 3000)]))
    await env.drive_to(TaskState.VERIFYING)
    await env.orch.tick()  # 启动校验协程，卡在落盘线程里
    await wait_until(entered.is_set)
    await env.repo.transition(H, TaskState.REMOVED, "torrents/delete", env.clock.now())
    cleanup = asyncio.ensure_future(env.orch.cleaner.cancel_and_cleanup_incomplete(H))
    await asyncio.sleep(0.05)
    assert not cleanup.done()  # 等校验协程（含落盘）彻底结束
    release.set()
    await cleanup
    assert await env.state() is TaskState.REMOVED
    assert not (env.root / "radarr" / "Show").exists()  # 落盘的那份被清掉
    assert env.orch._landed == {}


async def test_cancel_during_landing_still_records_completion(
    env: Env, slow_land: tuple[threading.Event, threading.Event]
) -> None:
    entered, release = slow_land
    f = ff("a.mkv", 3000)
    await env.add_task(OfflineScenario("Show", [f]))
    await env.drive_to(TaskState.VERIFYING)
    await env.orch.tick()
    await wait_until(entered.is_set)
    cancel = asyncio.ensure_future(env.orch.cancel_job(H))  # 例如关停
    await asyncio.sleep(0.05)
    release.set()
    await cancel
    # 文件已落盘，任务不能留在 verifying 里靠重新校验（那会把好片源判坏）
    assert await env.state() is TaskState.COMPLETED
    assert (env.root / "radarr" / "Show" / "a.mkv").read_bytes() == f.content
    assert env.orch._landed == {}


# ------------------------------------------------------------ 10. 内存集合与暂停标记


async def test_space_waiting_task_removed_sends_recovery_notice(env: Env) -> None:
    env.free_bytes = 10 * 1024**3
    await env.add_task(OfflineScenario("Show", [ff("a.mkv", 3000)]))
    await env.drive_to(TaskState.FETCHING)
    await env.orch.tick()
    await env.settle_jobs()
    assert "local-space" in env.notifier.kinds
    assert env.orch.pauses.get("fetch") == "waiting for local space"
    assert env.orch._space_waiting == {H}

    await env.repo.transition(H, TaskState.REMOVED, "torrents/delete", env.clock.now())
    await env.orch.tick()
    assert env.orch._space_waiting == set()
    assert [k for k, _ in env.notifier.recoveries] == ["local-space"]
    assert env.orch.pauses == {}


async def test_pauses_without_matching_tasks_are_cleared(env: Env) -> None:
    for phase in ("queued", "submit", "locate", "fetch", "verify"):
        env.orch._pauses[phase] = "stale"
    await env.orch.tick()
    assert env.orch.pauses == {}


async def test_pauses_with_matching_tasks_are_kept(env: Env) -> None:
    await put_at(env, H, TaskState.VERIFYING)
    env.orch._pauses["verify"] = "target exists"
    env.orch._pauses["fetch"] = "stale"  # 没有 fetching 任务
    await env.orch._prune()
    assert env.orch.pauses == {"verify": "target exists"}


# ------------------------------------------------------------ 11. CDN 退避只升一级


async def test_concurrent_cdn_errors_raise_global_backoff_one_level(env: Env) -> None:
    for h in (H, H2, H3):
        await put_at(env, h, TaskState.FETCHING)
    # 三个任务的协程在同一假时刻撞上 CDN 连接错误
    stub = StubFetcher(env.incomplete)
    gate = asyncio.Event()

    async def down(_h: str) -> FetchResult:
        await gate.wait()
        raise NetworkError("cdn connection failed: ConnectError")

    stub.behavior = down
    orch = env.make_orch(fetcher=stub)
    await orch.tick()
    await wait_until(lambda: len(stub.calls) == 3)  # 三个协程都在跑
    gate.set()
    await orch.wait_jobs(5)
    assert orch._backoff.failures(("fetch-net",)) == 1  # 只升一级
    assert orch.pauses.get("fetch") == "cdn network"
    for h in (H, H2, H3):
        reason = orch.stall_reason(h)
        assert reason is not None and "CDN" in reason
    await orch.shutdown()


# ------------------------------------------------------------ 12. LocalCleaner 的 realpath 防护


@pytest.mark.skipif(sys.platform == "win32", reason="needs POSIX symlinks")
async def test_remove_completed_refuses_symlinked_category_dir(env: Env, tmp_path: Path) -> None:
    media = tmp_path / "media"
    (media / "Show").mkdir(parents=True)
    (media / "Show" / "a.mkv").write_bytes(b"precious")
    env.root.mkdir(parents=True)
    (env.root / "radarr").symlink_to(media, target_is_directory=True)
    task = await put_at(env, H, TaskState.COMPLETED)
    with pytest.raises(UnsafeLocalPath):
        await env.orch.cleaner.remove_completed(task)
    assert (media / "Show" / "a.mkv").read_bytes() == b"precious"


@pytest.mark.skipif(sys.platform == "win32", reason="needs POSIX symlinks")
async def test_remove_completed_allows_symlinked_root(env: Env, tmp_path: Path) -> None:
    real_root = tmp_path / "real_lib"
    (real_root / "radarr" / "Show").mkdir(parents=True)
    (real_root / "radarr" / "Show" / "a.mkv").write_bytes(b"x")
    env.root.symlink_to(real_root, target_is_directory=True)
    task = await put_at(env, H, TaskState.COMPLETED)
    await env.orch.cleaner.remove_completed(task)
    assert not (real_root / "radarr" / "Show").exists()


async def test_content_target_compares_realpaths(
    env: Env, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """realpath 与 abspath 不一致（分类目录是符号链接）时拒绝；与平台无关的模拟。"""
    media = tmp_path / "media"
    category = os.fspath(env.root / "radarr")
    real_realpath = os.path.realpath

    def fake_realpath(p: Any, *a: Any, **kw: Any) -> str:
        if os.fspath(p) == category:
            return str(media)
        return real_realpath(p, *a, **kw)

    task = await put_at(env, H, TaskState.COMPLETED)
    monkeypatch.setattr(os.path, "realpath", fake_realpath)
    with pytest.raises(UnsafeLocalPath):
        env.orch.cleaner._content_target(task)
    monkeypatch.undo()
    assert env.orch.cleaner._content_target(task) == env.root / "radarr" / "Show"
