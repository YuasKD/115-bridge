from __future__ import annotations

from collections.abc import AsyncIterator
from dataclasses import replace
from pathlib import Path

import pytest

from bridge.cloud.fake import FakeCloud, FakeFile, OfflineScenario
from bridge.cloud.protocol import (
    AuthExpired,
    CloudError,
    CloudNotFound,
    NetworkError,
    OfflinePage,
    OfflineStat,
    RiskControl,
    ServerError,
    VerifyRequired,
)
from bridge.db.repo import Source, Task, TaskState, TaskUpdate
from bridge.db.sqlite import SqliteTaskRepo
from bridge.kv_keys import KV_QUOTA, decode_quota
from bridge.pipeline.submit import QuotaLow, submit
from bridge.pipeline.track import ObservationKind as K
from bridge.pipeline.track import TrackState, track, unescape_115
from bridge.testing.clock import FakeClock
from bridge.torrent.magnet import build_magnet

ROOT = "/115-bridge"
HOUR = 3600


def h(n: int) -> str:
    return f"{n:02x}" * 20


@pytest.fixture
async def repo(tmp_path: Path) -> AsyncIterator[SqliteTaskRepo]:
    r = await SqliteTaskRepo.open(tmp_path / "t.db")
    yield r
    await r.close()


@pytest.fixture
def clock() -> FakeClock:
    return FakeClock()


@pytest.fixture
def cloud(clock: FakeClock) -> FakeCloud:
    return FakeCloud(clock)


def scenario(name: str = "Movie", **kw: object) -> OfflineScenario:
    return OfflineScenario(name=name, files=[FakeFile("a.mkv", b"x" * 1000)], **kw)  # type: ignore[arg-type]


async def start(
    repo: SqliteTaskRepo,
    cloud: FakeCloud,
    clock: FakeClock,
    n: int,
    *,
    state: TaskState = TaskState.CLOUD_METADATA,
    sc: OfflineScenario | None = None,
) -> Task:
    """建任务、提交到 FakeCloud，并推进到指定云端状态。"""
    info_hash = h(n)
    if sc is not None:
        cloud.script(info_hash, sc)
    task, _ = await repo.create_task(
        info_hash,
        "radarr",
        f"t{n}",
        Source.MAGNET,
        clock.now(),
        build_magnet(info_hash, f"t{n}"),
    )
    await submit(task, cloud=cloud, repo=repo, clock=clock, root_dir=ROOT, quota_min=0)
    for st in (TaskState.SUBMITTING, TaskState.CLOUD_METADATA, TaskState.CLOUD_DOWNLOADING):
        await repo.transition(info_hash, st, "test", clock.now())
        if st is state:
            break
    got = await repo.get_task(info_hash)
    assert got is not None
    return got


async def do_track(
    cloud: FakeCloud, repo: SqliteTaskRepo, clock: FakeClock, hours: float = 12
) -> dict[str, object]:
    return dict(await track(cloud=cloud, repo=repo, clock=clock, timeout_hours=hours))


def stats_called(cloud: FakeCloud) -> list[int]:
    return [int(c.args[0]) for c in cloud.calls_to("list_offline")]


# ---------------------------------------------------------------- 查询次数


async def test_many_tasks_share_one_query(
    repo: SqliteTaskRepo, cloud: FakeCloud, clock: FakeClock
) -> None:
    for n in range(1, 6):
        await start(repo, cloud, clock, n)
    cloud.calls.clear()
    await clock.advance(30)
    obs = await track(cloud=cloud, repo=repo, clock=clock, timeout_hours=12)
    assert set(obs) == {h(n) for n in range(1, 6)}
    assert cloud.call_count("list_offline") == 1
    assert stats_called(cloud) == [OfflineStat.RUNNING]


async def test_no_tracked_tasks_makes_no_calls(
    repo: SqliteTaskRepo, cloud: FakeCloud, clock: FakeClock
) -> None:
    assert await track(cloud=cloud, repo=repo, clock=clock, timeout_hours=12) == {}
    assert cloud.calls == []


async def test_done_task_triggers_stat_11_only(
    repo: SqliteTaskRepo, cloud: FakeCloud, clock: FakeClock
) -> None:
    await start(repo, cloud, clock, 1)
    await start(repo, cloud, clock, 2)
    cloud.complete(h(1))
    cloud.calls.clear()
    obs = await track(cloud=cloud, repo=repo, clock=clock, timeout_hours=12)
    assert stats_called(cloud) == [12, 11]
    assert obs[h(1)].kind is K.DONE
    assert obs[h(2)].kind in (K.METADATA, K.DOWNLOADING)


async def test_failed_task_triggers_stat_11_then_9(
    repo: SqliteTaskRepo, cloud: FakeCloud, clock: FakeClock
) -> None:
    await start(repo, cloud, clock, 1)
    cloud.fail(h(1), status=4)
    cloud.calls.clear()
    obs = await track(cloud=cloud, repo=repo, clock=clock, timeout_hours=12)
    assert stats_called(cloud) == [12, 11, 9]
    assert obs[h(1)].kind is K.FAILED
    assert obs[h(1)].status == 4


async def test_not_found_everywhere_queries_all_three(
    repo: SqliteTaskRepo, cloud: FakeCloud, clock: FakeClock
) -> None:
    await start(repo, cloud, clock, 1)
    cloud._tasks.clear()  # 云端查不到这个任务
    cloud.calls.clear()
    obs = await track(cloud=cloud, repo=repo, clock=clock, timeout_hours=12)
    assert stats_called(cloud) == [12, 11, 9]
    assert obs[h(1)].kind is K.NOT_FOUND


async def test_paging_finds_target_on_later_page_and_stops(
    repo: SqliteTaskRepo, cloud: FakeCloud, clock: FakeClock
) -> None:
    cloud.page_size = 2
    # 云端有 4 个无关任务排在前面，目标在第 3 页
    for n in range(100, 104):
        await cloud.add_magnet(build_magnet(h(n)), "0")
    await start(repo, cloud, clock, 1)
    await start(repo, cloud, clock, 2)
    cloud.calls.clear()
    obs = await track(cloud=cloud, repo=repo, clock=clock, timeout_hours=12)
    pages = [(int(c.args[0]), c.args[1]) for c in cloud.calls_to("list_offline")]
    assert pages == [(12, 1), (12, 2), (12, 3)]
    assert set(obs) == {h(1), h(2)}
    assert all(o.kind is not K.NOT_FOUND for o in obs.values())


async def test_paging_stops_once_all_found(
    repo: SqliteTaskRepo, cloud: FakeCloud, clock: FakeClock
) -> None:
    cloud.page_size = 2
    await start(repo, cloud, clock, 1)
    await start(repo, cloud, clock, 2)
    for n in range(100, 106):
        await cloud.add_magnet(build_magnet(h(n)), "0")
    cloud.calls.clear()
    await track(cloud=cloud, repo=repo, clock=clock, timeout_hours=12)
    assert cloud.call_count("list_offline") == 1


async def test_hash_compare_is_lowercase(
    repo: SqliteTaskRepo, cloud: FakeCloud, clock: FakeClock
) -> None:
    await start(repo, cloud, clock, 1)
    orig = cloud.list_offline

    async def upper(stat: OfflineStat, page: int = 1) -> OfflinePage:
        p = await orig(stat, page)
        return replace(p, tasks=[replace(t, hash=t.hash.upper()) for t in p.tasks])

    cloud.list_offline = upper  # type: ignore[method-assign]
    obs = await track(cloud=cloud, repo=repo, clock=clock, timeout_hours=12)
    assert obs[h(1)].kind is not K.NOT_FOUND


# ---------------------------------------------------------------- 观测结果


async def test_metadata_phase_when_percent_zero(
    repo: SqliteTaskRepo, cloud: FakeCloud, clock: FakeClock
) -> None:
    await start(repo, cloud, clock, 1)
    obs = await track(cloud=cloud, repo=repo, clock=clock, timeout_hours=12)
    assert obs[h(1)].kind is K.METADATA
    assert obs[h(1)].percent == 0.0


async def test_downloading_reports_percent_and_writes_progress(
    repo: SqliteTaskRepo, cloud: FakeCloud, clock: FakeClock
) -> None:
    await start(repo, cloud, clock, 1)
    await clock.advance(30)  # ramp 60 秒，一半
    obs = (await track(cloud=cloud, repo=repo, clock=clock, timeout_hours=12))[h(1)]
    assert obs.kind is K.DOWNLOADING
    assert obs.percent == pytest.approx(49.5)
    assert obs.size == 4096
    saved = await repo.get_task(h(1))
    assert saved is not None
    assert saved.cloud_percent == pytest.approx(49.5)
    assert saved.cloud_size == 4096
    assert saved.cloud_file_id is None


async def test_done_writes_file_id_and_size(
    repo: SqliteTaskRepo, cloud: FakeCloud, clock: FakeClock
) -> None:
    await start(repo, cloud, clock, 1, sc=scenario())
    await clock.advance(61)
    obs = (await track(cloud=cloud, repo=repo, clock=clock, timeout_hours=12))[h(1)]
    root = cloud.task_root_id(h(1))
    assert root is not None
    assert obs.kind is K.DONE
    assert obs.file_id == root
    assert obs.size == 1000
    saved = await repo.get_task(h(1))
    assert saved is not None
    assert saved.cloud_file_id == root
    assert saved.cloud_percent == 100.0


async def test_percent_100_without_done_is_still_downloading(
    repo: SqliteTaskRepo, cloud: FakeCloud, clock: FakeClock
) -> None:
    await start(repo, cloud, clock, 1, sc=scenario(transient_seconds=600))
    await clock.advance(61)  # ramp 结束，进入 percent100 过渡态（status 1）
    o = (await track(cloud=cloud, repo=repo, clock=clock, timeout_hours=12))[h(1)]
    assert o.kind is K.DOWNLOADING
    assert o.percent == 100.0
    assert o.file_id is None


async def test_status_2_without_file_id_is_not_done(
    repo: SqliteTaskRepo, cloud: FakeCloud, clock: FakeClock
) -> None:
    await start(
        repo, cloud, clock, 1, sc=scenario(transient_seconds=600, transient_kind="no_file_id")
    )
    await clock.advance(61)
    o = (await track(cloud=cloud, repo=repo, clock=clock, timeout_hours=12))[h(1)]
    assert o.kind is K.DOWNLOADING  # 出现在 stat=11 列表里，但 file_id 为空
    assert o.file_id is None
    await clock.advance(600)
    o = (await track(cloud=cloud, repo=repo, clock=clock, timeout_hours=12))[h(1)]
    assert o.kind is K.DONE


@pytest.mark.parametrize("status", [-1, 3, 4, 5])
async def test_failed_statuses(
    repo: SqliteTaskRepo, cloud: FakeCloud, clock: FakeClock, status: int
) -> None:
    await start(repo, cloud, clock, 1, sc=scenario(outcome=status))
    await clock.advance(61)
    o = (await track(cloud=cloud, repo=repo, clock=clock, timeout_hours=12))[h(1)]
    assert o.kind is K.FAILED
    assert o.status == status


# ---------------------------------------------------------------- 99% 与超时


async def test_stall_at_99_is_not_failure_until_12_hours(
    repo: SqliteTaskRepo, cloud: FakeCloud, clock: FakeClock
) -> None:
    await start(repo, cloud, clock, 1, sc=scenario(never_finishes=True))
    await clock.advance(11 * HOUR)
    o = (await track(cloud=cloud, repo=repo, clock=clock, timeout_hours=12))[h(1)]
    assert o.kind is K.DOWNLOADING
    assert o.percent == 99.0
    await clock.advance(HOUR - 1)
    o = (await track(cloud=cloud, repo=repo, clock=clock, timeout_hours=12))[h(1)]
    assert o.kind is K.DOWNLOADING
    await clock.advance(1)  # 恰好 12 小时
    o = (await track(cloud=cloud, repo=repo, clock=clock, timeout_hours=12))[h(1)]
    assert o.kind is K.TIMEOUT


async def test_timeout_counts_from_submitted_at_and_honors_config(
    repo: SqliteTaskRepo, cloud: FakeCloud, clock: FakeClock
) -> None:
    await clock.advance(5 * HOUR)  # 任务排队很久后才提交，起点是 submitted_at
    await start(repo, cloud, clock, 1, sc=scenario(never_finishes=True))
    await clock.advance(2 * HOUR)
    assert (await track(cloud=cloud, repo=repo, clock=clock, timeout_hours=12))[
        h(1)
    ].kind is K.DOWNLOADING
    assert (await track(cloud=cloud, repo=repo, clock=clock, timeout_hours=2))[
        h(1)
    ].kind is K.TIMEOUT


async def test_metadata_phase_also_times_out(
    repo: SqliteTaskRepo, cloud: FakeCloud, clock: FakeClock
) -> None:
    await start(repo, cloud, clock, 1, sc=scenario(ramp_seconds=10**9, never_finishes=True))
    await clock.advance(12 * HOUR)
    o = (await track(cloud=cloud, repo=repo, clock=clock, timeout_hours=12))[h(1)]
    assert o.kind is K.TIMEOUT


async def test_done_beats_timeout(repo: SqliteTaskRepo, cloud: FakeCloud, clock: FakeClock) -> None:
    await start(repo, cloud, clock, 1, sc=scenario())
    await clock.advance(13 * HOUR)
    o = (await track(cloud=cloud, repo=repo, clock=clock, timeout_hours=12))[h(1)]
    assert o.kind is K.DONE


async def test_not_found_is_in_progress_until_timeout(
    repo: SqliteTaskRepo, cloud: FakeCloud, clock: FakeClock
) -> None:
    await start(repo, cloud, clock, 1)
    cloud._tasks.clear()
    await clock.advance(11 * HOUR)
    assert (await track(cloud=cloud, repo=repo, clock=clock, timeout_hours=12))[
        h(1)
    ].kind is K.NOT_FOUND
    await clock.advance(HOUR)
    assert (await track(cloud=cloud, repo=repo, clock=clock, timeout_hours=12))[
        h(1)
    ].kind is K.TIMEOUT


# ---------------------------------------------------------------- 任务选取


async def test_only_cloud_stage_tasks_with_submitted_at_are_tracked(
    repo: SqliteTaskRepo, cloud: FakeCloud, clock: FakeClock
) -> None:
    await start(repo, cloud, clock, 1, state=TaskState.SUBMITTING)
    await start(repo, cloud, clock, 2, state=TaskState.CLOUD_METADATA)
    await start(repo, cloud, clock, 3, state=TaskState.CLOUD_DOWNLOADING)
    t4 = await start(repo, cloud, clock, 4, state=TaskState.CLOUD_DOWNLOADING)
    await repo.transition(t4.hash, TaskState.CLOUD_DONE, "t", clock.now())
    # 排队中，未提交
    await repo.create_task(h(5), "radarr", "t5", Source.MAGNET, clock.now(), build_magnet(h(5)))
    # 提交阶段但还没写 submitted_at
    await repo.create_task(h(6), "radarr", "t6", Source.MAGNET, clock.now(), build_magnet(h(6)))
    await repo.transition(h(6), TaskState.SUBMITTING, "t", clock.now())
    obs = await track(cloud=cloud, repo=repo, clock=clock, timeout_hours=12)
    assert set(obs) == {h(1), h(2), h(3)}


# ---------------------------------------------------------------- 配额与异常


async def test_quota_saved_and_used_by_submit(
    repo: SqliteTaskRepo, cloud: FakeCloud, clock: FakeClock
) -> None:
    await start(repo, cloud, clock, 1)
    cloud.quota_remaining = 7
    cloud.quota_total = 1570
    await track(cloud=cloud, repo=repo, clock=clock, timeout_hours=12)
    assert (await repo.kv_get(KV_QUOTA) or "").startswith("7/1570@")
    cloud.calls.clear()
    t2, _ = await repo.create_task(h(2), "radarr", "t2", Source.MAGNET, clock.now(), "")
    with pytest.raises(QuotaLow):
        await submit(t2, cloud=cloud, repo=repo, clock=clock, root_dir=ROOT, quota_min=20)
    assert cloud.call_count("list_offline") == 1  # QuotaLow 前强制刷新一次，仍低才抛
    assert not cloud.was_called("add_magnet")
    # 已有任务不受影响
    saved = await repo.get_task(h(1))
    assert saved is not None
    assert saved.state is TaskState.CLOUD_METADATA


@pytest.mark.parametrize(
    "exc",
    [AuthExpired(), VerifyRequired(), RiskControl(), NetworkError(), ServerError()],
)
@pytest.mark.parametrize("on_call", [1, 2])
async def test_global_errors_propagate_and_nothing_changes(
    repo: SqliteTaskRepo,
    cloud: FakeCloud,
    clock: FakeClock,
    exc: CloudError,
    on_call: int,
) -> None:
    await start(repo, cloud, clock, 1)
    await start(repo, cloud, clock, 2)
    cloud.fail(h(2))  # 让第二次调用（stat=11）也会发生
    await clock.advance(30)
    before = [await repo.get_task(h(1)), await repo.get_task(h(2))]
    kv_before = await repo.kv_get(KV_QUOTA)
    cloud.calls.clear()
    cloud.inject("list_offline", exc, on_call=on_call, times=None)
    with pytest.raises(type(exc)):
        await track(cloud=cloud, repo=repo, clock=clock, timeout_hours=12)
    assert [await repo.get_task(h(1)), await repo.get_task(h(2))] == before
    assert await repo.kv_get(KV_QUOTA) == kv_before


# ---------------------------------------------------------------- 配额带读取时间


async def test_quota_saved_with_read_time(
    repo: SqliteTaskRepo, cloud: FakeCloud, clock: FakeClock
) -> None:
    await start(repo, cloud, clock, 1)
    await clock.advance(5)
    await track(cloud=cloud, repo=repo, clock=clock, timeout_hours=12)
    decoded = decode_quota(await repo.kv_get(KV_QUOTA))
    assert decoded is not None
    quota, at = decoded
    assert (quota.remaining, quota.total) == (cloud.quota_remaining, 1570)
    assert at == clock.now()


# ---------------------------------------------------------------- 单文件种子的根


SINGLE_NAME = "Movie.2024.mkv"


async def single_file_task(
    repo: SqliteTaskRepo,
    cloud: FakeCloud,
    clock: FakeClock,
    n: int = 1,
    *,
    name: str = SINGLE_NAME,
) -> tuple[str, str]:
    """构造 115 上的单文件种子形态（``single_file_at_root``）并推进到离线完成。

    file_id 等于保存目录 id，文件直接在保存目录里。返回 (保存目录 id, 保存目录里目标文件 id)。
    """
    sc = OfflineScenario(
        name=name, files=[FakeFile("a.mkv", b"x" * 1000)], single_file_at_root=True
    )
    await start(repo, cloud, clock, n, sc=sc)
    wp = await cloud.makedirs(f"{ROOT}/radarr")
    [entry] = [e for e in cloud.entries_by_name(name) if e.parent_id == wp and not e.is_dir]
    await clock.advance(61)
    return wp, entry.id


def put_in_save_dir(cloud: FakeCloud, wp: str, n: int, sc: OfflineScenario) -> None:
    """用公开的 preload_tree 在保存目录里放干扰项。"""
    cloud.preload_tree(h(n), sc, wp)


async def test_single_file_root_resolved_to_file_in_save_dir(
    repo: SqliteTaskRepo, cloud: FakeCloud, clock: FakeClock
) -> None:
    wp, fid = await single_file_task(repo, cloud, clock)
    # 保存目录里还有同名但大小不同的文件、同大小但不同名的文件，以及同名目录
    put_in_save_dir(
        cloud,
        wp,
        90,
        OfflineScenario(SINGLE_NAME, [FakeFile("x", b"y" * 999)], single_file_at_root=True),
    )
    put_in_save_dir(
        cloud,
        wp,
        91,
        OfflineScenario("Other.mkv", [FakeFile("x", b"z" * 1000)], single_file_at_root=True),
    )
    put_in_save_dir(
        cloud, wp, 92, OfflineScenario(SINGLE_NAME, [FakeFile("x", b"w" * 1000)])
    )  # 同名目录
    assert len(cloud.entries_by_name(SINGLE_NAME)) == 3
    o = (await track(cloud=cloud, repo=repo, clock=clock, timeout_hours=12))[h(1)]
    assert o.kind is K.DONE
    assert o.file_id == fid
    assert o.file_id != wp
    saved = await repo.get_task(h(1))
    assert saved is not None
    assert saved.cloud_file_id == fid
    assert saved.cloud_size == 1000
    assert [c.args for c in cloud.calls_to("list_dir")] == [(wp,)]


async def test_single_file_names_compared_after_unescape_on_both_sides(
    repo: SqliteTaskRepo, cloud: FakeCloud, clock: FakeClock
) -> None:
    raw = "A[128074]B.mkv"  # 115 字符引用；会话层列目录时已转成 "A" + 表情 + "B.mkv"
    wp, fid = await single_file_task(repo, cloud, clock, name=raw)
    # fake 没有「列目录名与 del_path 转义形式不同」的正式接口，这里保留一处私有访问
    entry = cloud._entries[fid]
    cloud._entries[fid] = replace(entry, name="A👊B.mkv")
    o = (await track(cloud=cloud, repo=repo, clock=clock, timeout_hours=12))[h(1)]
    assert o.kind is K.DONE
    assert o.file_id == fid
    assert unescape_115(raw) == "A👊B.mkv"


@pytest.mark.parametrize("case", ["file_missing", "only_dir_with_same_name"])
async def test_single_file_unresolved_never_writes_wp_path_id(
    repo: SqliteTaskRepo, cloud: FakeCloud, clock: FakeClock, case: str
) -> None:
    wp, fid = await single_file_task(repo, cloud, clock)
    cloud.hide_entry(fid)  # 保存目录里已找不到这个文件
    if case == "only_dir_with_same_name":
        put_in_save_dir(cloud, wp, 90, OfflineScenario(SINGLE_NAME, [FakeFile("x", b"y" * 1000)]))
    o = (await track(cloud=cloud, repo=repo, clock=clock, timeout_hours=12))[h(1)]
    assert o.kind is K.ROOT_UNRESOLVED
    assert o.reason
    assert o.file_id is None
    saved = await repo.get_task(h(1))
    assert saved is not None
    assert saved.cloud_file_id is None
    assert saved.cloud_file_id != wp


async def test_single_file_duplicates_pick_first_by_id_and_warn(
    repo: SqliteTaskRepo,
    cloud: FakeCloud,
    clock: FakeClock,
    caplog: pytest.LogCaptureFixture,
) -> None:
    wp, fid = await single_file_task(repo, cloud, clock)
    put_in_save_dir(
        cloud,
        wp,
        90,
        OfflineScenario(SINGLE_NAME, [FakeFile("x", b"y" * 1000)], single_file_at_root=True),
    )
    ids = sorted(
        e.id for e in cloud.entries_by_name(SINGLE_NAME) if e.parent_id == wp and not e.is_dir
    )
    assert len(ids) == 2
    with caplog.at_level("WARNING", logger="bridge.pipeline.track"):
        o = (await track(cloud=cloud, repo=repo, clock=clock, timeout_hours=12))[h(1)]
    assert o.kind is K.DONE
    assert o.file_id == ids[0]
    saved = await repo.get_task(h(1))
    assert saved is not None
    assert saved.cloud_file_id == ids[0]
    [rec] = [r for r in caplog.records if r.levelname == "WARNING"]
    assert rec.__dict__["hash"] == h(1)
    assert rec.__dict__["candidates"] == 2


async def test_single_file_stale_cloud_file_id_is_not_reused(
    repo: SqliteTaskRepo, cloud: FakeCloud, clock: FakeClock
) -> None:
    wp, fid = await single_file_task(repo, cloud, clock)
    # 上一轮遗留的过期 id（既不是保存目录，也不是现在保存目录里的文件）
    await repo.update(h(1), TaskUpdate(cloud_file_id="stale-from-last-round"))
    o = (await track(cloud=cloud, repo=repo, clock=clock, timeout_hours=12))[h(1)]
    assert o.kind is K.DONE
    assert o.file_id == fid
    saved = await repo.get_task(h(1))
    assert saved is not None
    assert saved.cloud_file_id == fid
    assert [c.args for c in cloud.calls_to("list_dir")] == [(wp,)]


async def test_single_file_resolves_again_on_every_done_observation(
    repo: SqliteTaskRepo, cloud: FakeCloud, clock: FakeClock
) -> None:
    wp, fid = await single_file_task(repo, cloud, clock)
    await track(cloud=cloud, repo=repo, clock=clock, timeout_hours=12)
    await track(cloud=cloud, repo=repo, clock=clock, timeout_hours=12)
    assert cloud.call_count("list_dir") == 2


async def test_single_file_save_dir_not_found_is_unresolved(
    repo: SqliteTaskRepo, cloud: FakeCloud, clock: FakeClock
) -> None:
    await single_file_task(repo, cloud, clock)
    cloud.inject("list_dir", CloudNotFound("gone"), times=None)
    o = (await track(cloud=cloud, repo=repo, clock=clock, timeout_hours=12))[h(1)]
    assert o.kind is K.ROOT_UNRESOLVED


async def test_single_file_global_error_changes_nothing(
    repo: SqliteTaskRepo, cloud: FakeCloud, clock: FakeClock
) -> None:
    await single_file_task(repo, cloud, clock)
    before = await repo.get_task(h(1))
    kv_before = await repo.kv_get(KV_QUOTA)
    cloud.inject("list_dir", NetworkError(), times=None)
    with pytest.raises(NetworkError):
        await track(cloud=cloud, repo=repo, clock=clock, timeout_hours=12)
    assert await repo.get_task(h(1)) == before
    assert await repo.kv_get(KV_QUOTA) == kv_before


async def test_multi_file_root_does_not_list_save_dir(
    repo: SqliteTaskRepo, cloud: FakeCloud, clock: FakeClock
) -> None:
    await start(repo, cloud, clock, 1, sc=scenario())
    await clock.advance(61)
    o = (await track(cloud=cloud, repo=repo, clock=clock, timeout_hours=12))[h(1)]
    assert o.kind is K.DONE
    assert o.file_id == cloud.task_root_id(h(1))
    assert not cloud.was_called("list_dir")


# ---------------------------------------------------------------- 翻页限制与深查限频


async def deep_setup(repo: SqliteTaskRepo, cloud: FakeCloud, clock: FakeClock) -> None:
    """云端已完成列表有 6 页（每页 2 条），被跟踪的 h(1) 在云端查不到。"""
    cloud.page_size = 2
    for n in range(100, 112):
        await cloud.add_magnet(build_magnet(h(n)), "0")
    await start(repo, cloud, clock, 1)
    await clock.advance(61)
    cloud._tasks.pop(h(1))
    cloud.calls.clear()


def pages_of(cloud: FakeCloud, stat: int) -> list[int]:
    return [c.args[1] for c in cloud.calls_to("list_offline") if int(c.args[0]) == stat]


async def test_stat_11_and_9_limited_to_max_pages(
    repo: SqliteTaskRepo, cloud: FakeCloud, clock: FakeClock
) -> None:
    await deep_setup(repo, cloud, clock)
    obs = await track(cloud=cloud, repo=repo, clock=clock, timeout_hours=12, max_pages=3)
    assert obs[h(1)].kind is K.NOT_FOUND
    assert pages_of(cloud, 11) == [1, 2, 3]
    assert pages_of(cloud, 9) == [1]


async def test_default_max_pages_is_5(
    repo: SqliteTaskRepo, cloud: FakeCloud, clock: FakeClock
) -> None:
    await deep_setup(repo, cloud, clock)
    await track(cloud=cloud, repo=repo, clock=clock, timeout_hours=12)
    assert pages_of(cloud, 11) == [1, 2, 3, 4, 5]


async def test_running_list_is_not_page_limited(
    repo: SqliteTaskRepo, cloud: FakeCloud, clock: FakeClock
) -> None:
    cloud.page_size = 1
    for n in range(100, 108):
        await cloud.add_magnet(build_magnet(h(n)), "0")
    await start(repo, cloud, clock, 1)
    cloud.calls.clear()
    obs = await track(cloud=cloud, repo=repo, clock=clock, timeout_hours=12, max_pages=2)
    assert obs[h(1)].kind is not K.NOT_FOUND
    assert pages_of(cloud, 12) == list(range(1, 10))


async def test_deep_lookup_throttled_to_every_10_minutes(
    repo: SqliteTaskRepo, cloud: FakeCloud, clock: FakeClock
) -> None:
    await deep_setup(repo, cloud, clock)
    state = TrackState()

    async def run() -> None:
        cloud.calls.clear()
        o = await track(
            cloud=cloud, repo=repo, clock=clock, timeout_hours=12, max_pages=3, state=state
        )
        assert o[h(1)].kind is K.NOT_FOUND

    await run()
    assert pages_of(cloud, 11) == [1, 2, 3]  # 首次深查
    await clock.advance(60)
    await run()
    assert pages_of(cloud, 11) == [1]  # 限频内只查第 1 页
    assert pages_of(cloud, 9) == [1]
    await clock.advance(539)
    await run()
    assert pages_of(cloud, 11) == [1]
    await clock.advance(1)  # 距上次深查恰好 600 秒
    await run()
    assert pages_of(cloud, 11) == [1, 2, 3]
    await clock.advance(60)
    await run()
    assert pages_of(cloud, 11) == [1]


async def test_found_while_throttled_clears_state(
    repo: SqliteTaskRepo, cloud: FakeCloud, clock: FakeClock
) -> None:
    await deep_setup(repo, cloud, clock)
    state = TrackState()
    await track(cloud=cloud, repo=repo, clock=clock, timeout_hours=12, max_pages=3, state=state)
    assert h(1) in state.last_deep
    # 任务之后在 115 上出现并完成，排在 stat=11 的第 1 页
    await cloud.add_magnet(build_magnet(h(1)), "0")
    mine = cloud._tasks.pop(h(1))
    cloud._tasks = {h(1): mine, **cloud._tasks}
    cloud.complete(h(1))
    await clock.advance(60)
    o = await track(cloud=cloud, repo=repo, clock=clock, timeout_hours=12, max_pages=3, state=state)
    assert o[h(1)].kind is not K.NOT_FOUND
    assert h(1) not in state.last_deep


async def test_throttle_does_not_delay_timeout(
    repo: SqliteTaskRepo, cloud: FakeCloud, clock: FakeClock
) -> None:
    await deep_setup(repo, cloud, clock)
    state = TrackState()
    await track(cloud=cloud, repo=repo, clock=clock, timeout_hours=12, max_pages=3, state=state)
    await clock.advance(12 * HOUR)
    o = await track(cloud=cloud, repo=repo, clock=clock, timeout_hours=12, max_pages=3, state=state)
    assert o[h(1)].kind is K.TIMEOUT
