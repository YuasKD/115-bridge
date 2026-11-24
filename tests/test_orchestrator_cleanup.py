"""T13：torrents/delete 与流水线并发，以及 LocalCleaner 的路径安全。

用 qBit 的真实删除接口（ASGI）加调度器的 LocalCleaner 实现。
"""

from __future__ import annotations

import asyncio
import os
from collections.abc import AsyncIterator
from pathlib import Path

import httpx
import pytest

import test_orchestrator_support as sup
from bridge.cloud.fake import OfflineScenario
from bridge.db.repo import Fallback, Source, TaskState
from bridge.pipeline.orchestrator import UnsafeLocalPath
from bridge.pipeline.verify import VerifyResult, VerifyStatus
from test_orchestrator_support import Env, H, StubVerifier, ff, put_at

READ_ONLY_CLOUD_CALLS = {
    "list_offline",
    "add_magnet",
    "makedirs",
    "get_entry",
    "list_dir",
    "download_link",
}


@pytest.fixture
async def env(tmp_path: Path) -> AsyncIterator[Env]:
    async with sup.make_env(tmp_path) as e:
        yield e


async def delete(client: httpx.AsyncClient, info_hash: str = H) -> httpx.Response:
    return await client.post("/torrents/delete", data={"hashes": info_hash, "deleteFiles": "true"})


async def wait_for(pred: object, limit: int = 1000) -> None:
    assert callable(pred)
    for _ in range(limit):
        if pred():
            return
        await asyncio.sleep(0.005)
    raise AssertionError("condition not reached")


def assert_cloud_untouched(env: Env) -> None:
    """不变量 2：只可能有读取和提交类调用，FakeCloud 本来也没有任何删除方法。"""
    assert {c.method for c in env.cloud.calls} <= READ_ONLY_CLOUD_CALLS
    assert not any("delete" in name or "remove" in name for name in dir(env.cloud))


# ------------------------------------------------------------ 删除与取回并发


async def test_delete_during_fetch_cancels_job_and_cleans_incomplete(env: Env) -> None:
    client = await env.qbit()
    await env.add_task(OfflineScenario("Show", [ff("a.mkv", 6000), ff("b.mkv", 3000)]))
    env.transport.gate = asyncio.Event()  # CDN 卡住：取回协程停在请求上
    await env.drive_to(TaskState.FETCHING)
    await wait_for(lambda: env.transport.requests > 0)
    assert env.orch.active_jobs  # 取回在跑
    assert (env.incomplete / H).exists()

    r = await delete(client)
    assert r.status_code == 200

    task = await env.task()
    assert task.state is TaskState.REMOVED  # 在途任务被删：removed，不回退
    assert task.fallback is Fallback.NONE
    assert env.orch.active_jobs == {}  # 协程已取消并等它结束
    assert not (env.incomplete / H).exists()  # .incomplete/<hash> 与 .state/<hash> 都清了
    assert not env.fetcher.state_dir(H).exists()
    requests_after_delete = env.transport.requests
    for _ in range(3):
        await env.orch.tick()
        await env.clock.advance(61)
    assert env.transport.requests == requests_after_delete  # 不会重新取回
    assert env.orch.active_jobs == {}
    assert env.radarr.calls == [] and env.notifier.alerts == []
    assert_cloud_untouched(env)
    await client.aclose()


async def test_delete_then_readd_restarts_from_instant_restore(env: Env) -> None:
    client = await env.qbit()
    files = [ff("a.mkv", 6000)]
    await env.add_task(OfflineScenario("Show", files))
    env.transport.gate = asyncio.Event()
    await env.drive_to(TaskState.FETCHING)
    await wait_for(lambda: env.transport.requests > 0)
    assert (await delete(client)).status_code == 200
    assert await env.state() is TaskState.REMOVED

    env.transport.gate = None
    magnet = f"magnet:?xt=urn:btih:{H}&dn=Show"
    r = await client.post("/torrents/add", data={"urls": magnet, "category": "radarr"})
    assert r.text == "Ok."
    assert await env.state() is TaskState.QUEUED  # removed -> queued，只能由 add 触发
    task = await env.drive_terminal()
    assert task.state is TaskState.COMPLETED
    assert env.cloud.call_count("add_magnet") == 1  # 115 上还在：秒取回，不再提交
    assert (env.root / "radarr" / "Show" / "a.mkv").read_bytes() == files[0].content
    await client.aclose()


async def test_delete_in_cloud_stage_only_stops_tracking(env: Env) -> None:
    client = await env.qbit()
    await env.add_task(OfflineScenario("Show", [ff("a.mkv", 100)], never_finishes=True))
    await env.drive_to(TaskState.CLOUD_DOWNLOADING)
    assert (await delete(client)).status_code == 200
    assert await env.state() is TaskState.REMOVED
    calls = len(env.cloud.calls)
    for _ in range(3):
        await env.orch.tick()
        await env.clock.advance(61)
    assert len(env.cloud.calls) == calls  # 停止跟踪，云端不再被查询
    assert env.cloud.call_count("add_magnet") == 1  # 云端任务不动
    assert env.radarr.calls == []
    assert_cloud_untouched(env)
    await client.aclose()


async def test_delete_completed_removes_only_that_content(env: Env) -> None:
    client = await env.qbit()
    files = [ff("a.mkv", 3000), ff("Sub/b.srt", 20)]
    await env.add_task(OfflineScenario("Show", files))
    await env.drive_terminal()
    other = env.root / "radarr" / "Other"
    other.mkdir()
    (other / "keep.mkv").write_bytes(b"keep")
    sibling_category = env.root / "tv-sonarr" / "Show"
    sibling_category.mkdir(parents=True)
    (sibling_category / "keep.mkv").write_bytes(b"keep")
    (env.incomplete / "someone-else").mkdir(parents=True)

    assert (await delete(client)).status_code == 200
    assert await env.state() is TaskState.REMOVED
    assert not (env.root / "radarr" / "Show").exists()
    assert (other / "keep.mkv").read_bytes() == b"keep"
    assert (sibling_category / "keep.mkv").read_bytes() == b"keep"
    assert (env.root / "radarr").is_dir()  # 分类目录本身不动
    assert (env.incomplete / "someone-else").is_dir()
    assert_cloud_untouched(env)
    await client.aclose()


async def test_delete_landing_race_removes_landed_content(env: Env) -> None:
    """校验协程已 rename 到正式目录、写 completed 之前 torrents/delete 把任务转成 removed。"""
    stub = StubVerifier(env.clock)
    final = env.root / "radarr" / "Show"

    async def land_then_deleted(_h: str) -> VerifyResult:
        final.mkdir(parents=True)
        (final / "a.mkv").write_bytes(b"landed")
        await env.repo.transition(H, TaskState.REMOVED, "torrents/delete", env.clock.now())
        return VerifyResult(VerifyStatus.RENAMED, final_path=str(final))

    stub.behavior = land_then_deleted
    orch = env.make_orch(verifier=stub)
    await put_at(env, H, TaskState.VERIFYING)
    await orch.tick()
    await orch.wait_jobs(5)
    assert await env.state() is TaskState.REMOVED
    assert not final.exists()  # 落盘的那份被清掉，不留孤儿
    await orch.shutdown()


async def test_cleaner_cancels_running_job_before_forget(env: Env) -> None:
    """forget 必须在取回协程完全结束之后调用。"""
    order: list[str] = []
    stub = sup.StubFetcher(env.incomplete)

    async def hang(_h: str) -> sup.FetchResult:
        try:
            await asyncio.Event().wait()
        finally:
            order.append("fetch-ended")
        raise AssertionError("unreachable")

    stub.behavior = hang
    real_forget = stub.forget

    def forget(h: str) -> None:
        order.append("forget")
        real_forget(h)

    stub.forget = forget  # type: ignore[method-assign]
    orch = env.make_orch(fetcher=stub)
    await put_at(env, H, TaskState.FETCHING)
    await orch.tick()
    await wait_for(lambda: stub.active == 1)
    (env.incomplete / H).mkdir(parents=True)
    await orch.cleaner.cancel_and_cleanup_incomplete(H)
    assert order == ["fetch-ended", "forget"]
    assert not (env.incomplete / H).exists()
    await orch.shutdown()


# ------------------------------------------------------------ 路径安全


async def make_completed(env: Env, name: str, category: str = "radarr") -> object:
    await env.repo.create_task(
        H, category, name, Source.MAGNET, env.clock.now(), "magnet:?xt=urn:btih:" + H
    )
    task = await env.repo.get_task(H)
    assert task is not None
    return task


@pytest.mark.parametrize("name", ["..", ".", "", "a/b", "../x", "a\\b", "x\x00y"])
async def test_remove_completed_refuses_unsafe_names(env: Env, name: str) -> None:
    victim = env.root / "radarr" / "victim"
    victim.mkdir(parents=True)
    (env.root / "important.txt").write_text("x")
    task = await make_completed(env, name)
    with pytest.raises(UnsafeLocalPath):
        await env.orch.cleaner.remove_completed(task)  # type: ignore[arg-type]
    assert victim.is_dir() and (env.root / "important.txt").exists()


@pytest.mark.parametrize("category", [".incomplete", "other", "..", "a/b"])
async def test_remove_completed_refuses_unknown_or_unsafe_category(env: Env, category: str) -> None:
    (env.incomplete / "x").mkdir(parents=True)
    task = await make_completed(env, "Show", category)
    with pytest.raises(UnsafeLocalPath):
        await env.orch.cleaner.remove_completed(task)  # type: ignore[arg-type]
    assert (env.incomplete / "x").is_dir()


async def test_remove_completed_handles_missing_and_file_content(env: Env) -> None:
    task = await make_completed(env, "movie.mkv")
    await env.orch.cleaner.remove_completed(task)  # type: ignore[arg-type]  # 不存在：成功
    path = env.root / "radarr" / "movie.mkv"
    path.parent.mkdir(parents=True)
    path.write_bytes(b"x")
    await env.orch.cleaner.remove_completed(task)  # type: ignore[arg-type]
    assert not path.exists() and path.parent.is_dir()


async def test_remove_completed_does_not_follow_symlink(env: Env, tmp_path: Path) -> None:
    outside = tmp_path / "outside"
    outside.mkdir()
    (outside / "precious").write_text("x")
    link = env.root / "radarr" / "Show"
    link.parent.mkdir(parents=True)
    try:
        os.symlink(outside, link, target_is_directory=True)
    except OSError:
        pytest.skip("symlinks not permitted")
    task = await make_completed(env, "Show")
    await env.orch.cleaner.remove_completed(task)  # type: ignore[arg-type]
    assert not link.exists() and (outside / "precious").read_text() == "x"


@pytest.mark.parametrize("bad", ["../etc", "x" * 39, "A" * 40, ""])
async def test_cleanup_incomplete_refuses_bad_hash(env: Env, bad: str) -> None:
    (env.root / "keep").mkdir(parents=True)
    with pytest.raises(UnsafeLocalPath):
        await env.orch.cleaner.cancel_and_cleanup_incomplete(bad)
    assert (env.root / "keep").is_dir()


async def test_cleanup_incomplete_is_idempotent_and_scoped(env: Env) -> None:
    mine = env.incomplete / H
    state = env.fetcher.state_dir(H)
    other = env.incomplete / ("cd" * 20)
    for d in (mine, state, other):
        d.mkdir(parents=True)
        (d / "f").write_bytes(b"x")
    await env.orch.cleaner.cancel_and_cleanup_incomplete(H)
    await env.orch.cleaner.cancel_and_cleanup_incomplete(H)  # 再来一次也成功
    assert not mine.exists() and not state.exists()
    assert (other / "f").exists()  # 别的任务不受影响


async def test_failed_task_delete_only_cleans_local(env: Env) -> None:
    client = await env.qbit()
    await env.add_task(OfflineScenario("Dead", [ff("a.mkv", 100)], outcome=-1))
    task = await env.drive_terminal()
    assert task.fallback is Fallback.DONE
    leftover = env.incomplete / H
    leftover.mkdir(parents=True)
    assert (await delete(client)).status_code == 200
    task = await env.task()
    assert task.state is TaskState.FAILED and task.client_deleted
    assert not leftover.exists()
    await client.aclose()


async def test_shutdown_cancels_background_alerts(env: Env) -> None:
    started = asyncio.Event()

    async def hang(kind: str, text: str) -> bool:
        started.set()
        await asyncio.Event().wait()
        return True

    env.notifier.alert = hang  # type: ignore[method-assign]
    env.orch._background_alert("cdn-403", "x")
    await started.wait()
    pending = list(env.orch._bg_alerts)
    assert len(pending) == 1

    await env.orch.shutdown()

    assert pending[0].done() and pending[0].cancelled()
    assert not env.orch._bg_alerts
