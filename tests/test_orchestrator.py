"""T13：调度器端到端。FakeCloud + FakeCdn + FakeArr + FakeClock + 内存仓储 + 临时目录。

从 ``create_task``（torrents/add 的等价物）开始，只通过多次 ``tick()`` 推进。
本文件覆盖正常路径和 design.md「失败处理与 Arr 回退」表里的任务级失败行；
暂停类的行在 test_orchestrator_pause.py，Arr 回退细节在 test_orchestrator_fallback.py。
"""

from __future__ import annotations

from collections.abc import AsyncIterator
from pathlib import Path

import pytest

import test_orchestrator_support as sup
from bridge.cloud.fake import CdnFault, OfflineScenario, make_content
from bridge.cloud.protocol import CloudNotFound
from bridge.db.repo import Fallback, ManifestFile, Stage, TaskState
from test_orchestrator_support import Env, H, ff


@pytest.fixture
async def env(tmp_path: Path) -> AsyncIterator[Env]:
    async with sup.make_env(tmp_path) as e:
        yield e


PATH_STATES = [
    TaskState.QUEUED,
    TaskState.SUBMITTING,
    TaskState.CLOUD_METADATA,
    TaskState.CLOUD_DOWNLOADING,
    TaskState.CLOUD_DONE,
    TaskState.LOCATING,
    TaskState.FETCHING,
    TaskState.VERIFYING,
    TaskState.COMPLETED,
]


async def visited(env: Env, info_hash: str = H) -> list[TaskState]:
    events = await env.repo.list_events(info_hash, limit=200)
    return [e.to_state for e in reversed(events)]


# ------------------------------------------------------------ 正常路径


async def test_multi_file_end_to_end(env: Env) -> None:
    files = [ff("a.mkv", 9000), ff("Sub/b.srt", 40), ff("Sub/Deep/c.nfo", 7)]
    await env.add_task(OfflineScenario("Show", files))
    task = await env.drive_terminal()

    assert task.state is TaskState.COMPLETED
    assert await visited(env) == PATH_STATES  # 严格逐级，不跳级
    final = env.root / "radarr" / "Show"
    for f in files:
        assert (final / f.path).read_bytes() == f.content
    assert not (env.incomplete / H).exists()
    assert task.retries == {}
    assert task.fallback is Fallback.NONE
    assert env.radarr.calls == []
    assert env.notifier.alerts == []
    assert env.cloud.call_count("add_magnet") == 1


async def test_single_file_end_to_end(env: Env) -> None:
    f = ff("movie.mkv", 5000)
    sc = OfflineScenario("movie.mkv", [f], single_file_at_root=True)
    await env.add_task(sc)
    task = await env.drive_terminal()

    assert task.state is TaskState.COMPLETED
    assert await visited(env) == PATH_STATES
    assert (env.root / "radarr" / "movie.mkv").read_bytes() == f.content
    # track 把保存目录解析成保存目录里的那个文件，而不是把保存目录当任务根
    [entry] = [e for e in env.cloud.entries_by_name("movie.mkv") if not e.is_dir]
    assert task.cloud_file_id == entry.id
    assert env.notifier.alerts == []


async def test_instant_restore_end_to_end(env: Env) -> None:
    files = [ff("a.mkv", 6000), ff("b.mkv", 100)]
    await env.add_task(OfflineScenario("Pack", files))
    first = await env.drive_terminal()
    final = env.root / "radarr" / "Pack"
    assert first.state is TaskState.COMPLETED

    # Arr 删除：removed，本地清理；同一个 hash 再次添加
    await env.repo.transition(H, TaskState.REMOVED, "torrents/delete", env.clock.now())
    await env.orch.cleaner.remove_completed(first)
    assert not final.exists()
    await env.repo.requeue_removed(H, "torrents/add", env.clock.now())

    task = await env.drive_terminal()
    assert task.state is TaskState.COMPLETED
    assert env.cloud.call_count("add_magnet") == 1  # 秒取回：没有再次提交，不消耗配额
    assert (await visited(env))[-5:] == [
        TaskState.QUEUED,
        TaskState.LOCATING,  # 秒取回：queued 直接到 locating
        TaskState.FETCHING,
        TaskState.VERIFYING,
        TaskState.COMPLETED,
    ]
    for f in files:
        assert (final / f.path).read_bytes() == f.content


# ------------------------------------------------------------ 任务级失败：直接回退


async def assert_failed_and_fell_back(env: Env, *, error_contains: str) -> None:
    task = await env.task()
    assert task.state is TaskState.FAILED
    assert task.fallback is Fallback.DONE
    assert error_contains in (task.error or "")
    assert env.radarr.removed == [7]  # find_queue_item + remove_and_blocklist 各调用一次
    assert [c.method for c in env.radarr.calls] == ["find_queue_item", "remove_and_blocklist"]
    assert env.radarr.calls[0].args == (H,)  # 传小写，由客户端转大写


async def test_submit_rejected(env: Env) -> None:
    sc = OfflineScenario("Bad", [ff("a.mkv", 100)], reject=(11000, "invalid magnet"))
    await env.add_task(sc)
    task = await env.drive_terminal()
    assert task.state is TaskState.FAILED
    await assert_failed_and_fell_back(env, error_contains="submit rejected")
    assert task.retries == {}  # 不重试，直接回退
    assert env.cloud.call_count("add_magnet") == 1


@pytest.mark.parametrize("status", [-1, 3, 4, 5])
async def test_offline_failed(env: Env, status: int) -> None:
    sc = OfflineScenario("Dead", [ff("a.mkv", 100)], outcome=status)
    await env.add_task(sc)
    task = await env.drive_terminal()
    assert task.state is TaskState.FAILED
    await assert_failed_and_fell_back(env, error_contains=f"status {status}")
    assert task.retries == {}


async def test_offline_timeout_after_12_hours(env: Env) -> None:
    sc = OfflineScenario("Stuck", [ff("a.mkv", 100)], never_finishes=True)
    await env.add_task(sc)
    await env.drive_to(TaskState.CLOUD_DOWNLOADING)
    # 99% 停滞不判失败，Arr 里继续显示排队中
    await env.clock.advance(11 * 3600)
    await env.orch.tick()
    assert await env.state() is TaskState.CLOUD_DOWNLOADING
    assert env.radarr.calls == []
    await env.clock.advance(2 * 3600)
    await env.orch.tick()
    task = await env.task()
    assert task.state is TaskState.FAILED
    await assert_failed_and_fell_back(env, error_contains="timeout")
    assert task.retries == {}


async def test_match_failed(env: Env) -> None:
    sc = OfflineScenario("Show", [ff("a.mkv", 5000), ff("b.mkv", 300)])
    bad_manifest = [ManifestFile("a.mkv", 5000), ManifestFile("b.mkv", 301)]
    await env.add_task(sc, manifest=bad_manifest)
    task = await env.drive_terminal()
    assert task.state is TaskState.FAILED
    await assert_failed_and_fell_back(env, error_contains="MatchFailed")
    assert task.retries == {}
    assert env.cdn.requests == []  # 没有下载任何内容


async def test_root_unresolved_for_single_file(env: Env) -> None:
    f = ff("movie.mkv", 5000)
    sc = OfflineScenario("movie.mkv", [f], single_file_at_root=True)
    await env.add_task(sc)
    await env.orch.tick()  # 提交
    [entry] = [e for e in env.cloud.entries_by_name("movie.mkv") if not e.is_dir]
    env.cloud.hide_entry(entry.id)  # 保存目录里找不到名字和大小都对的文件
    task = await env.drive_terminal()
    assert task.state is TaskState.FAILED
    await assert_failed_and_fell_back(env, error_contains="cannot resolve single-file root")


async def test_cloud_not_found_at_locate(env: Env) -> None:
    await env.add_task(OfflineScenario("Show", [ff("a.mkv", 5000)]))
    await env.orch.tick()
    env.cloud.hide_entry(env.cloud.task_root_id(H) or "")
    task = await env.drive_terminal()
    assert task.state is TaskState.FAILED
    await assert_failed_and_fell_back(env, error_contains="CloudNotFound")
    assert task.retries == {}


async def test_cloud_not_found_at_fetch(env: Env) -> None:
    await env.add_task(OfflineScenario("Show", [ff("a.mkv", 5000)]))
    env.cloud.inject("download_link", CloudNotFound("gone"), times=None)
    task = await env.drive_terminal()
    assert task.state is TaskState.FAILED
    await assert_failed_and_fell_back(env, error_contains="cloud file not found")
    assert task.retries == {}  # 不计重试


async def test_unsafe_task_name_fails_at_verify(env: Env) -> None:
    f = ff("a.mkv", 2000)
    await env.add_task(OfflineScenario("Show", [f]), name="evil/../name")
    task = await env.drive_terminal()
    assert task.state is TaskState.FAILED
    await assert_failed_and_fell_back(env, error_contains="UnsafeName")
    assert not (env.root / "radarr").exists()  # 什么都没落盘


# ------------------------------------------------------------ 任务级失败：计入重试


async def test_fetch_failed_after_retry_budget(env: Env) -> None:
    f = ff("a.mkv", 5000)
    await env.add_task(OfflineScenario("Show", [f]))
    # 每轮两个分段都会吃到一个 500；一个字节都没写进去，每轮计一次重试
    await env.orch.tick()
    [entry] = [e for e in env.cloud.entries_by_name("a.mkv") if not e.is_dir]
    assert entry.pickcode is not None
    env.cdn.queue_fault(entry.pickcode, *[CdnFault("status", status=500)] * 200)
    task = await env.drive_terminal(step=4000)
    assert task.state is TaskState.FAILED
    await assert_failed_and_fell_back(env, error_contains="fetch failed")
    assert task.retries == {Stage.FETCH: 6}  # 超过上限 5 次，只计在取回阶段，调度器不重复计
    assert not (env.root / "radarr" / "Show").exists()


async def test_verify_failed_after_retry_budget(env: Env) -> None:
    f = ff("a.mkv", 5000)
    await env.add_task(OfflineScenario("Show", [f]))
    await env.orch.tick()
    [entry] = [e for e in env.cloud.entries_by_name("a.mkv") if not e.is_dir]
    assert entry.pickcode is not None
    # CDN 给出的内容与 115 属性里的 sha1 不符（大小相同）
    env.cdn.add_file(entry.pickcode, make_content("tampered", 5000))
    task = await env.drive_terminal()
    assert task.state is TaskState.FAILED
    await assert_failed_and_fell_back(env, error_contains="verify failed")
    assert task.retries == {Stage.VERIFY: 3}  # 删掉重下，超过 2 次后回退
    assert not (env.root / "radarr" / "Show").exists()  # 校验没通过，绝不进正式目录
    states = await visited(env)
    assert states.count(TaskState.FETCHING) == 3  # 初次加两次重下
    assert states.count(TaskState.VERIFYING) == 3


async def test_verify_mismatch_once_then_recovers(env: Env) -> None:
    f = ff("a.mkv", 5000)
    await env.add_task(OfflineScenario("Show", [f]))
    await env.orch.tick()
    [entry] = [e for e in env.cloud.entries_by_name("a.mkv") if not e.is_dir]
    assert entry.pickcode is not None
    good = f.content
    env.cdn.add_file(entry.pickcode, make_content("tampered", 5000))

    async def fix_after_first_verify() -> bool:
        t = await env.task()
        if t.retries.get(Stage.VERIFY, 0) >= 1:
            env.cdn.add_file(entry.pickcode or "", good)
        return t.state in (TaskState.COMPLETED, TaskState.FAILED)

    await env.drive(fix_after_first_verify)
    task = await env.task()
    assert task.state is TaskState.COMPLETED
    assert task.retries == {Stage.VERIFY: 1}
    assert (env.root / "radarr" / "Show" / "a.mkv").read_bytes() == good
    assert env.radarr.calls == []


# ------------------------------------------------------------ 校验阶段的停住（不回退）


async def test_target_exists_stops_and_alerts_without_fallback(env: Env) -> None:
    f = ff("a.mkv", 3000)
    await env.add_task(OfflineScenario("Show", [f]))
    existing = env.root / "radarr" / "Show"
    existing.mkdir(parents=True)
    (existing / "other.txt").write_text("keep me")

    await env.drive_to(TaskState.VERIFYING)
    for _ in range(3):
        await env.orch.tick()
        await env.settle_jobs()
        await env.clock.advance(3000)
    task = await env.task()
    assert task.state is TaskState.VERIFYING  # 不转 failed
    assert task.fallback is Fallback.NONE
    assert env.radarr.calls == []  # 不触发 Arr 回退
    assert any(k.startswith("target-exists-") for k in env.notifier.kinds)
    assert (existing / "other.txt").read_text() == "keep me"  # 不覆盖、不删除
    assert env.orch.pauses.get("verify") == "target exists"

    # 人处理掉之后恢复：已校验的文件跳过，直接改名
    (existing / "other.txt").unlink()
    existing.rmdir()
    task = await env.drive_to(TaskState.COMPLETED)
    assert (env.root / "radarr" / "Show" / "a.mkv").read_bytes() == f.content


async def test_land_oserror_stops_and_alerts_without_fallback(env: Env) -> None:
    f = ff("a.mkv", 3000)
    await env.add_task(OfflineScenario("Show", [f]))
    env.root.mkdir(parents=True)
    (env.root / "radarr").write_text("a file where the category dir should be")

    await env.drive_to(TaskState.VERIFYING)
    for _ in range(3):
        await env.orch.tick()
        await env.settle_jobs()
        await env.clock.advance(3000)
    task = await env.task()
    assert task.state is TaskState.VERIFYING
    assert task.retries == {}  # 原始 OSError 不计重试
    assert env.radarr.calls == []
    assert any(k.startswith("verify-os-") for k in env.notifier.kinds)

    (env.root / "radarr").unlink()
    task = await env.drive_to(TaskState.COMPLETED)
    assert (env.root / "radarr" / "Show" / "a.mkv").read_bytes() == f.content


async def test_requeued_task_with_spent_fetch_budget_fails_with_clear_reason(env: Env) -> None:
    """重新入队后重试计数保留；预算已用完时 fetch 直接抛 FetchFailed，回退文案要说明原因。"""
    await env.add_task(OfflineScenario("Show", [ff("a.mkv", 100)]))
    for _ in range(6):
        await env.repo.incr_retry(H, Stage.FETCH)
    task = await env.drive_terminal()
    assert task.state is TaskState.FAILED
    await assert_failed_and_fell_back(env, error_contains="retry budget already spent")
    assert task.retries == {Stage.FETCH: 6}  # 调度器不重复计数
    assert env.cdn.requests == []
