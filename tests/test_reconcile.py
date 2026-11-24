"""T14：启动对账。FakeCloud、FakeClock、内存仓储、临时目录。"""

from __future__ import annotations

import hashlib
import os
from collections.abc import AsyncIterator
from pathlib import Path

import httpx
import pytest

import test_orchestrator_support as sup
from bridge.api.qbit import create_qbit_app
from bridge.cloud.fake import OfflineScenario
from bridge.cloud.protocol import AuthExpired
from bridge.config import Secrets
from bridge.db.repo import Fallback, Stage, TaskFile, TaskState, TaskUpdate
from bridge.missing_protocol import completion_id
from bridge.pipeline.fetch import _write_state
from bridge.pipeline.reconcile import MissingFiles, ReconcileReport, reconcile
from test_orchestrator_support import H2, H3, Env, H, ff, put_at

HOUR = 3600.0
OLD = "1" * 40
YOUNG = "2" * 40


@pytest.fixture
async def env(tmp_path: Path) -> AsyncIterator[Env]:
    async with sup.make_env(tmp_path) as e:
        yield e


async def run(env: Env, **kw: object) -> ReconcileReport:
    return await reconcile(
        repo=env.repo,
        cloud=env.cloud,
        clock=env.clock,
        settings=env.settings,
        notifier=env.notifier,
        **kw,  # type: ignore[arg-type]
    )


def mk_incomplete(env: Env, name: str, content: bytes = b"x") -> Path:
    d = env.incomplete / name
    d.mkdir(parents=True)
    (d / "f.bin").write_bytes(content)
    return d


def land(env: Env, category: str = "radarr", name: str = "Show") -> Path:
    p = env.root / category / name
    p.mkdir(parents=True)
    (p / "a.mkv").write_bytes(b"data")
    return p


# ------------------------------------------------------------ 规则 1：离线中的任务


async def reach_downloading(env: Env) -> None:
    """用小步长推进，让任务停在 CLOUD_DOWNLOADING（不被 drive 的大步长一路带到完成）。"""
    for _ in range(50):
        await env.orch.tick()
        if await env.state() is TaskState.CLOUD_DOWNLOADING:
            return
        await env.clock.advance(1.0)
    raise AssertionError("did not reach CLOUD_DOWNLOADING")


async def test_offline_task_is_observed_and_progress_written_not_advanced(env: Env) -> None:
    files = [ff("a.mkv", 5000)]
    await env.add_task(OfflineScenario("Show", files, ramp_seconds=1000.0, stall_percent=50.0))
    await reach_downloading(env)
    await env.clock.advance(300.0)

    report = await run(env)

    assert report.offline_skipped is None
    assert report.offline_observed == {H: "downloading"}
    task = await env.task()
    assert task.state is TaskState.CLOUD_DOWNLOADING  # 只观测，不推进
    assert task.cloud_percent > 0


async def test_offline_done_is_left_for_scheduler_first_tick(env: Env) -> None:
    await env.add_task(OfflineScenario("Show", [ff("a.mkv", 5000)], ramp_seconds=1000.0))
    await reach_downloading(env)
    await env.clock.advance(5000.0)

    report = await run(env)

    assert report.offline_observed == {H: "done"}
    task = await env.task()
    assert task.state is TaskState.CLOUD_DOWNLOADING
    assert task.cloud_file_id  # 进度字段已写库
    await env.orch.tick()  # 调度器第一次 tick 接手推进
    assert (await env.task()).state is not TaskState.CLOUD_DOWNLOADING


async def test_auth_expired_does_not_raise_and_marks_skipped(env: Env) -> None:
    await env.add_task(OfflineScenario("Show", [ff("a.mkv", 5000)], ramp_seconds=1000.0))
    await reach_downloading(env)
    env.cloud.inject("list_offline", AuthExpired("cookie expired"), times=None)

    report = await run(env)

    assert report.offline_skipped is not None
    assert "AuthExpired" in report.offline_skipped
    assert report.offline_observed == {}
    task = await env.task()
    assert task.state is TaskState.CLOUD_DOWNLOADING
    assert task.retries == {}  # 不计重试


# ------------------------------------------------------------ 规则 2：fetching 校正字节


def state_path(env: Env, h: str, rel: str) -> Path:
    return env.incomplete / ".state" / h / (hashlib.sha1(rel.encode()).hexdigest() + ".json")


async def setup_fetching(env: Env, files: list[TaskFile]) -> None:
    await put_at(env, H, TaskState.FETCHING)
    await env.repo.put_task_files(H, files)
    await env.repo.update(H, TaskUpdate(local_bytes=sum(f.local_bytes for f in files)))


def tf(rel: str, size: int, local: int = 0, verified: bool = False) -> TaskFile:
    return TaskFile(rel, size, "S" * 40, "pc", local_bytes=local, verified=verified)


async def test_fetching_local_bytes_follow_progress_record(env: Env) -> None:
    await setup_fetching(
        env, [tf("a.bin", 1000, local=900), tf("b.bin", 500, local=500, verified=True)]
    )
    d = env.incomplete / H
    d.mkdir(parents=True)
    (d / "a.bin").write_bytes(b"\0" * 1000)  # 预分配的稀疏文件，实际写到 400
    _write_state(state_path(env, H, "a.bin"), 1000, [(0, 500, 400), (500, 1000, 0)])

    report = await run(env)

    files = {f.rel_path: f for f in await env.repo.list_task_files(H)}
    assert files["a.bin"].local_bytes == 400  # 不超过记录
    assert files["b.bin"].local_bytes == 500
    task = await env.task()
    assert task.local_bytes == 900
    assert report.fetch_adjusted == {H: (1400, 900)}
    assert task.state is TaskState.FETCHING
    assert task.retries.get(Stage.FETCH, 0) == 0


async def test_fetching_corrupt_or_ahead_record_resets_to_zero(env: Env) -> None:
    await setup_fetching(env, [tf("a.bin", 1000, local=700), tf("b.bin", 1000, local=700)])
    d = env.incomplete / H
    d.mkdir(parents=True)
    (d / "a.bin").write_bytes(b"\0" * 1000)
    state_path(env, H, "a.bin").parent.mkdir(parents=True)
    state_path(env, H, "a.bin").write_text("{not json", encoding="utf-8")  # 记录损坏
    (d / "b.bin").write_bytes(b"\0" * 100)  # 记录超前于磁盘上的数据
    _write_state(state_path(env, H, "b.bin"), 1000, [(0, 1000, 700)])

    await run(env)

    files = {f.rel_path: f.local_bytes for f in await env.repo.list_task_files(H)}
    assert files == {"a.bin": 0, "b.bin": 0}
    assert (await env.task()).local_bytes == 0
    assert (d / "a.bin").exists() and (d / "b.bin").exists()  # 对账不动磁盘，续传交给 fetch


async def test_fetching_complete_file_without_record_kept(env: Env) -> None:
    await setup_fetching(env, [tf("a.bin", 300, local=300)])
    d = env.incomplete / H
    d.mkdir(parents=True)
    (d / "a.bin").write_bytes(b"\1" * 300)

    report = await run(env)

    assert (await env.task()).local_bytes == 300
    assert report.fetch_adjusted == {}


# ------------------------------------------------------------ 规则 3：孤儿目录


async def test_orphan_boundary_23h59_kept_24h_removed(env: Env) -> None:
    now = env.clock.now().timestamp()
    mtimes = {OLD: now - 24 * HOUR, YOUNG: now - (24 * HOUR - 60)}
    mk_incomplete(env, OLD)
    mk_incomplete(env, YOUNG)
    (env.incomplete / ".state" / OLD).mkdir(parents=True)
    (env.incomplete / ".state" / OLD / "x.json").write_text("{}")
    (env.incomplete / ".state" / YOUNG).mkdir(parents=True)

    report = await run(env, mtime_of=lambda p: mtimes[p.name])

    assert report.orphans_removed == [OLD]
    assert report.orphans_young == [YOUNG]
    assert not (env.incomplete / OLD).exists()
    assert not (env.incomplete / ".state" / OLD).exists()  # .state/<hash> 一并清
    assert (env.incomplete / YOUNG).exists()
    assert (env.incomplete / ".state" / YOUNG).exists()


async def test_orphan_uses_real_mtime_by_default(env: Env) -> None:
    d = mk_incomplete(env, OLD)
    old = env.clock.now().timestamp() - 30 * HOUR
    os.utime(d, (old, old))
    report = await run(env)
    assert report.orphans_removed == [OLD]


async def test_db_known_hashes_are_never_orphans(env: Env) -> None:
    await put_at(env, H, TaskState.FETCHING)
    await put_at(env, H2, TaskState.QUEUED)
    await env.repo.transition(H2, TaskState.FAILED, "x", env.clock.now())
    await put_at(env, H3, TaskState.QUEUED)
    await env.repo.transition(H3, TaskState.REMOVED, "x", env.clock.now())
    for h in (H, H2, H3):
        mk_incomplete(env, h)

    report = await run(env, mtime_of=lambda _p: 0.0)  # 全部远超 24 小时

    assert report.orphans_removed == []
    for h in (H, H2, H3):
        assert (env.incomplete / h / "f.bin").exists()


async def test_state_dir_and_non_hash_entries_are_not_task_dirs(env: Env) -> None:
    (env.incomplete / ".state" / "notes").mkdir(parents=True)  # .state 下的非 hash 名
    (env.incomplete / "notes").mkdir()
    (env.incomplete / ("A" * 40)).mkdir()  # 大写 hex 不匹配
    (env.incomplete / ("1" * 39)).mkdir()
    (env.incomplete / ("1" * 41)).mkdir()
    (env.incomplete / ("2" * 40 + ".txt")).write_text("x")
    (env.incomplete / ("3" * 40)).write_text("a file, not a dir")

    report = await run(env, mtime_of=lambda _p: 0.0)

    assert report.orphans_removed == []
    assert (env.incomplete / ".state" / "notes").exists()
    assert (env.incomplete / "notes").exists()
    assert (env.incomplete / ("A" * 40)).exists()
    assert (env.incomplete / ("3" * 40)).exists()


# ---------------------------------------- 规则 3 补充：孤立的 .state/<hash>


def mk_state(env: Env, name: str) -> Path:
    d = env.incomplete / ".state" / name
    d.mkdir(parents=True)
    (d / "x.json").write_text("{}")
    return d


async def test_state_orphan_boundary_24h_removed_23h59_kept(env: Env) -> None:
    now = env.clock.now().timestamp()
    mtimes = {OLD: now - 24 * HOUR, YOUNG: now - (24 * HOUR - 60)}
    mk_state(env, OLD)
    mk_state(env, YOUNG)

    report = await run(env, mtime_of=lambda p: mtimes[p.name])

    assert report.state_orphans_removed == [OLD]
    assert report.state_orphans_young == [YOUNG]
    assert report.orphans_removed == []
    assert not (env.incomplete / ".state" / OLD).exists()
    assert (env.incomplete / ".state" / YOUNG).exists()
    assert (env.incomplete / ".state").exists()


async def test_state_orphan_kept_when_db_has_hash(env: Env) -> None:
    await put_at(env, H, TaskState.FETCHING)
    mk_state(env, H)
    report = await run(env, mtime_of=lambda _p: 0.0)
    assert report.state_orphans_removed == []
    assert (env.incomplete / ".state" / H / "x.json").exists()


async def test_state_orphan_skipped_when_incomplete_dir_still_exists(env: Env) -> None:
    # <hash> 仍在：不走这条路径。孤儿主规则在 mtime 不够老时两者都留。
    mk_incomplete(env, YOUNG)
    mk_state(env, YOUNG)
    now = env.clock.now().timestamp()
    mtimes = {YOUNG: now - HOUR}

    report = await run(env, mtime_of=lambda p: mtimes[p.name])

    assert report.state_orphans_removed == []
    assert report.state_orphans_young == []
    assert report.orphans_young == [YOUNG]
    assert (env.incomplete / ".state" / YOUNG).exists()


async def test_state_orphan_with_old_incomplete_dir_handled_by_main_rule(env: Env) -> None:
    mk_incomplete(env, OLD)
    mk_state(env, OLD)

    report = await run(env, mtime_of=lambda _p: 0.0)

    assert report.orphans_removed == [OLD]
    assert report.state_orphans_removed == []
    assert not (env.incomplete / ".state" / OLD).exists()


async def test_state_non_hash_names_files_and_symlinks_untouched(env: Env) -> None:
    state = env.incomplete / ".state"
    state.mkdir(parents=True)
    (state / "notes").mkdir()
    (state / ("A" * 40)).mkdir()
    (state / ("1" * 39)).mkdir()
    (state / ("3" * 40)).write_text("a file")
    outside = env.incomplete.parent / "outside"
    outside.mkdir()
    (outside / "keep.txt").write_text("k")
    try:
        (state / OLD).symlink_to(outside, target_is_directory=True)
    except (OSError, NotImplementedError):
        pytest.skip("symlinks not available")

    report = await run(env, mtime_of=lambda _p: 0.0)

    assert report.state_orphans_removed == []
    assert report.state_orphan_errors == {}
    assert (state / "notes").exists()
    assert (state / ("A" * 40)).exists()
    assert (state / ("1" * 39)).exists()
    assert (state / ("3" * 40)).exists()
    assert (state / OLD).is_symlink()
    assert (outside / "keep.txt").exists()


async def test_state_orphan_delete_failure_recorded_and_continues(
    env: Env, monkeypatch: pytest.MonkeyPatch
) -> None:
    import bridge.pipeline.reconcile as rec

    mk_state(env, OLD)
    mk_state(env, YOUNG)
    real = rec.shutil.rmtree

    def flaky(path: object, *a: object, **k: object) -> None:
        if Path(str(path)).name == OLD:
            raise PermissionError("denied")
        real(path, *a, **k)  # type: ignore[arg-type]

    monkeypatch.setattr(rec.shutil, "rmtree", flaky)

    report = await run(env, mtime_of=lambda _p: 0.0)

    assert OLD in report.state_orphan_errors
    assert report.state_orphans_removed == [YOUNG]
    assert not (env.incomplete / ".state" / YOUNG).exists()


async def test_missing_incomplete_dir_is_fine(env: Env) -> None:
    report = await run(env)
    assert report.orphans_removed == []


# ------------------------------------------------------------ 规则 4：completed 缺文件


async def test_completed_missing_files_reported_and_alerted(env: Env) -> None:
    await put_at(env, H, TaskState.COMPLETED)  # 名字 Show，没有文件
    await put_at(env, H2, TaskState.COMPLETED)
    p = env.root / "radarr" / "Show"
    assert not p.exists()

    report = await run(env)

    assert isinstance(report.missing_files, MissingFiles)
    assert report.missing_files.hashes == {H, H2}
    cid = await completion_id(env.repo, H)
    assert cid is not None
    assert report.missing_files.is_missing(H, cid)
    assert not report.missing_files.is_missing("f" * 40, cid)
    assert report.missing_files.might_be_missing(H)
    assert report.missing_files.might_be_missing(H.upper())
    assert not report.missing_files.might_be_missing("f" * 40)
    assert env.notifier.kinds == ["missing-files"]
    assert report.missing_alerted
    assert (await env.task()).state is TaskState.COMPLETED  # 状态不变


async def test_completed_with_files_present_not_missing(env: Env) -> None:
    await put_at(env, H, TaskState.COMPLETED)
    land(env)

    report = await run(env)

    assert report.missing_files.hashes == frozenset()
    assert env.notifier.alerts == []


async def test_missing_files_wired_into_qbit(env: Env) -> None:
    await put_at(env, H, TaskState.COMPLETED)
    await put_at(env, H2, TaskState.COMPLETED)
    land(env, name="Show")
    await env.repo.update(H2, TaskUpdate(name="Gone"))
    report = await run(env)
    app = create_qbit_app(
        repo=env.repo,
        settings=env.settings,
        secrets=Secrets(webui_password=sup.PASSWORD),
        clock=env.clock,
        cleaner=env.orch.cleaner,
        missing_files=report.missing_files,
    )
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://t/api/v2"
    ) as c:
        await c.post("/auth/login", data={"username": "admin", "password": sup.PASSWORD})
        states = {t["hash"]: t["state"] for t in (await c.get("/torrents/info")).json()}
    assert states == {H: "stoppedUP", H2: "missingFiles"}


async def _qbit_states(env: Env, report: ReconcileReport) -> dict[str, str]:
    app = create_qbit_app(
        repo=env.repo,
        settings=env.settings,
        secrets=Secrets(webui_password=sup.PASSWORD),
        clock=env.clock,
        cleaner=env.orch.cleaner,
        missing_files=report.missing_files,
    )
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://t/api/v2"
    ) as c:
        await c.post("/auth/login", data={"username": "admin", "password": sup.PASSWORD})
        return {t["hash"]: t["state"] for t in (await c.get("/torrents/info")).json()}


async def test_missing_mark_expires_after_removed_requeue_recomplete(env: Env) -> None:
    await put_at(env, H, TaskState.COMPLETED)
    report = await run(env)
    assert (await _qbit_states(env, report)) == {H: "missingFiles"}

    # 同一 hash 被删除、重新添加、重新完成：已经不是启动时那一次 completed
    await env.repo.transition(H, TaskState.REMOVED, "arr delete", env.clock.now())
    await env.repo.requeue_removed(H, "re-add", env.clock.now())
    for s in (
        TaskState.SUBMITTING,
        TaskState.CLOUD_METADATA,
        TaskState.CLOUD_DOWNLOADING,
        TaskState.CLOUD_DONE,
        TaskState.LOCATING,
        TaskState.FETCHING,
        TaskState.VERIFYING,
        TaskState.COMPLETED,
    ):
        await env.repo.transition(H, s, "again", env.clock.now())

    assert (await _qbit_states(env, report)) == {H: "stoppedUP"}


async def test_missing_mark_stays_until_restart_when_files_restored(env: Env) -> None:
    """按启动对账语义：文件事后被恢复，标记保留到下次重启（重启时重新计算）。"""
    await put_at(env, H, TaskState.COMPLETED)
    report = await run(env)
    land(env)
    assert (await _qbit_states(env, report)) == {H: "missingFiles"}
    assert (await _qbit_states(env, await run(env))) == {H: "stoppedUP"}


# ------------------------------------------------------------ 补充：VERIFYING 已落盘


async def test_verifying_empty_incomplete_dir_with_content_is_landed(env: Env) -> None:
    """单文件落盘崩在「改名文件」与「删空目录」之间。"""
    await put_at(env, H, TaskState.VERIFYING)
    (env.incomplete / H).mkdir(parents=True)
    land(env)

    report = await run(env)

    assert report.landed_completed == [H]
    assert (await env.task()).state is TaskState.COMPLETED
    assert not (env.incomplete / H).exists()


async def test_verifying_empty_incomplete_dir_without_content_is_lost(env: Env) -> None:
    await put_at(env, H, TaskState.VERIFYING)
    (env.incomplete / H).mkdir(parents=True)

    report = await run(env)

    assert report.landed_completed == []
    assert (await env.task()).state is TaskState.VERIFYING


async def test_verifying_already_landed_goes_straight_to_completed(env: Env) -> None:
    await put_at(env, H, TaskState.VERIFYING)
    land(env)  # incomplete/<H> 不在，content_path 在

    report = await run(env)

    assert report.landed_completed == [H]
    task = await env.task()
    assert task.state is TaskState.COMPLETED
    assert task.retries == {}
    events = await env.repo.list_events(H, limit=1)
    assert events[0].to_state is TaskState.COMPLETED
    assert report.missing_files.hashes == frozenset()  # 随后不会被当成缺失


async def test_verifying_with_incomplete_dir_left_for_scheduler(env: Env) -> None:
    await put_at(env, H, TaskState.VERIFYING)
    mk_incomplete(env, H)
    land(env)  # 即使 content_path 同名已存在（例如另一个任务），也不转

    report = await run(env)

    assert report.landed_completed == []
    assert (await env.task()).state is TaskState.VERIFYING


async def test_verifying_with_neither_is_reported_not_changed(env: Env) -> None:
    await put_at(env, H, TaskState.VERIFYING)

    report = await run(env)

    assert report.verifying_lost == [H]
    assert (await env.task()).state is TaskState.VERIFYING


async def test_failed_fallback_none_left_to_scheduler(env: Env) -> None:
    await put_at(env, H, TaskState.QUEUED)
    await env.repo.transition(H, TaskState.FAILED, "x", env.clock.now())

    await run(env)

    task = await env.task()
    assert task.state is TaskState.FAILED
    assert task.fallback is Fallback.NONE  # 对账不动；由调度器 tick 补成 PENDING
    await env.orch.tick()
    assert (await env.task()).fallback is not Fallback.NONE


async def test_reconcile_never_calls_cloud_delete(env: Env) -> None:
    await put_at(env, H, TaskState.COMPLETED)
    await run(env)
    assert not any("delete" in c.method or "remove" in c.method for c in env.cloud.calls)
