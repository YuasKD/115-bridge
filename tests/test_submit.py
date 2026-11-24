from __future__ import annotations

from collections.abc import AsyncIterator
from pathlib import Path

import pytest

from bridge.cloud.fake import FakeCloud, FakeFile, OfflineScenario
from bridge.cloud.protocol import (
    AuthExpired,
    CloudError,
    CloudNotFound,
    NetworkError,
    Quota,
    QuotaExhausted,
    RiskControl,
    ServerError,
    SubmitRejected,
    VerifyRequired,
)
from bridge.db.repo import CloudIndexEntry, Source, Task, TaskState, TaskUpdate
from bridge.db.sqlite import SqliteTaskRepo
from bridge.kv_keys import KV_QUOTA, encode_quota
from bridge.pipeline.submit import (
    QuotaLow,
    SaveDirUnavailable,
    SubmitKind,
    check_instant,
    submit,
)
from bridge.testing.clock import FakeClock
from bridge.torrent.magnet import build_magnet, parse_magnet

H = "ab" * 20
H2 = "cd" * 20
TRACKERS = ("udp://tracker.example:6969/announce", "http://t2.example/announce")
ROOT = "/115-bridge"


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


async def add(
    repo: SqliteTaskRepo,
    clock: FakeClock,
    h: str = H,
    *,
    category: str = "radarr",
    magnet: str | None = None,
    source: Source = Source.TORRENT,
) -> Task:
    if magnet is None:
        magnet = build_magnet(h, "My Movie", TRACKERS)
    task, _ = await repo.create_task(h, category, "My Movie", source, clock.now(), magnet)
    return task


async def do_submit(
    task: Task,
    cloud: FakeCloud,
    repo: SqliteTaskRepo,
    clock: FakeClock,
    *,
    quota_min: int = 20,
) -> SubmitKind:
    res = await submit(
        task, cloud=cloud, repo=repo, clock=clock, root_dir=ROOT, quota_min=quota_min
    )
    return res.kind


async def stored(repo: SqliteTaskRepo, h: str = H) -> Task:
    t = await repo.get_task(h)
    assert t is not None
    return t


# ---------------------------------------------------------------- 基本提交


async def test_torrent_task_submits_magnet_with_trackers(
    repo: SqliteTaskRepo, cloud: FakeCloud, clock: FakeClock
) -> None:
    task = await add(repo, clock)
    res = await submit(task, cloud=cloud, repo=repo, clock=clock, root_dir=ROOT, quota_min=20)
    assert res.kind is SubmitKind.SUBMITTED
    (call,) = cloud.calls_to("add_magnet")
    sent = parse_magnet(call.args[0])
    assert sent.info_hash == H
    assert sent.trackers == TRACKERS
    assert call.args[0] == task.magnet


async def test_empty_magnet_falls_back_to_build_magnet(
    repo: SqliteTaskRepo, cloud: FakeCloud, clock: FakeClock
) -> None:
    task = await add(repo, clock, magnet="", source=Source.MAGNET)
    await do_submit(task, cloud, repo, clock)
    (call,) = cloud.calls_to("add_magnet")
    assert call.args[0] == build_magnet(H, "My Movie")


async def test_submitted_at_written_and_saved_to_category_dir(
    repo: SqliteTaskRepo, cloud: FakeCloud, clock: FakeClock
) -> None:
    task = await add(repo, clock)
    await clock.advance(5)
    res = await submit(task, cloud=cloud, repo=repo, clock=clock, root_dir=ROOT + "/", quota_min=20)
    assert res.task.submitted_at == clock.now()
    assert (await stored(repo)).submitted_at == clock.now()
    (mk,) = cloud.calls_to("makedirs")
    assert mk.args == ("/115-bridge/radarr",)
    (call,) = cloud.calls_to("add_magnet")
    assert call.args[1] == await cloud.makedirs("/115-bridge/radarr")


# ---------------------------------------------------------------- 幂等


async def test_resubmit_same_hash_does_not_call_115_again(
    repo: SqliteTaskRepo, cloud: FakeCloud, clock: FakeClock
) -> None:
    stale = await add(repo, clock)
    assert await do_submit(stale, cloud, repo, clock) is SubmitKind.SUBMITTED
    first_at = (await stored(repo)).submitted_at
    await clock.advance(30)
    # 即使传入的是旧的 Task 对象（submitted_at 仍为 None）也不重复提交
    assert await do_submit(stale, cloud, repo, clock) is SubmitKind.ALREADY_SUBMITTED
    assert cloud.call_count("add_magnet") == 1
    assert (await stored(repo)).submitted_at == first_at


async def test_task_with_submitted_at_never_calls_115(
    repo: SqliteTaskRepo, cloud: FakeCloud, clock: FakeClock
) -> None:
    task = await add(repo, clock)
    await repo.update(H, TaskUpdate(submitted_at=clock.now()))
    assert await do_submit(task, cloud, repo, clock) is SubmitKind.ALREADY_SUBMITTED
    assert cloud.calls == []


# ---------------------------------------------------------------- 目录缓存


async def test_category_dir_id_cached_in_kv(
    repo: SqliteTaskRepo, cloud: FakeCloud, clock: FakeClock
) -> None:
    t1 = await add(repo, clock, H)
    t2 = await add(repo, clock, H2)
    await do_submit(t1, cloud, repo, clock)
    await do_submit(t2, cloud, repo, clock)
    assert cloud.call_count("makedirs") == 1
    dir_ids = {c.args[1] for c in cloud.calls_to("add_magnet")}
    assert len(dir_ids) == 1
    assert await repo.kv_get("cloud.dir:/115-bridge/radarr") == dir_ids.pop()


async def test_each_category_gets_own_dir(
    repo: SqliteTaskRepo, cloud: FakeCloud, clock: FakeClock
) -> None:
    t1 = await add(repo, clock, H, category="radarr")
    t2 = await add(repo, clock, H2, category="tv-sonarr")
    await do_submit(t1, cloud, repo, clock)
    await do_submit(t2, cloud, repo, clock)
    assert [c.args[0] for c in cloud.calls_to("makedirs")] == [
        "/115-bridge/radarr",
        "/115-bridge/tv-sonarr",
    ]
    ids = [c.args[1] for c in cloud.calls_to("add_magnet")]
    assert ids[0] != ids[1]


# ---------------------------------------------------------------- 秒取回


def _scenario() -> OfflineScenario:
    return OfflineScenario(name="Old Movie", files=[FakeFile("a.mkv", b"x" * 100)])


def _scenario_single() -> OfflineScenario:
    return OfflineScenario(
        name="one.mkv", files=[FakeFile("a.mkv", b"x" * 10)], single_file_at_root=True
    )


async def _put_index(repo: SqliteTaskRepo, clock: FakeClock, root_id: str) -> None:
    await repo.put_cloud_index(CloudIndexEntry(H, root_id, "Old Movie", 100, clock.now()))


async def test_instant_restore_dir_root_hit(
    repo: SqliteTaskRepo, cloud: FakeCloud, clock: FakeClock
) -> None:
    root = cloud.preload_tree(H, _scenario(), "0")
    await _put_index(repo, clock, root)
    task = await add(repo, clock)
    cloud.quota_remaining = 0  # 即使配额为 0 也不受影响
    hit = await check_instant(task, cloud=cloud, repo=repo)
    assert hit is not None
    assert hit.cloud_file_id == root
    assert cloud.was_called("list_dir")  # 目录根用 list_dir 确认
    assert not cloud.was_called("add_magnet")
    assert not cloud.was_called("list_offline")
    assert not cloud.was_called("makedirs")
    saved = await stored(repo)
    assert saved.cloud_file_id == root
    assert saved.cloud_size == 100
    assert saved.submitted_at is None
    assert saved.state is TaskState.QUEUED  # 状态转换归调度器


async def test_instant_restore_file_root_hit_uses_get_entry(
    repo: SqliteTaskRepo, cloud: FakeCloud, clock: FakeClock
) -> None:
    cloud.preload_tree(H, _scenario(), "0")
    (fid,) = [e.id for e in cloud.entries_by_name("a.mkv")]
    await _put_index(repo, clock, fid)
    task = await add(repo, clock)
    hit = await check_instant(task, cloud=cloud, repo=repo)
    assert hit is not None
    assert hit.cloud_file_id == fid
    assert not cloud.was_called("list_dir")
    assert (await stored(repo)).cloud_file_id == fid


async def test_instant_restore_ignores_submitted_at(
    repo: SqliteTaskRepo, cloud: FakeCloud, clock: FakeClock
) -> None:
    root = cloud.preload_tree(H, _scenario(), "0")
    await _put_index(repo, clock, root)
    task = await add(repo, clock)
    await repo.update(H, TaskUpdate(submitted_at=clock.now()))
    assert await check_instant(task, cloud=cloud, repo=repo) is not None


async def test_index_hit_but_cloud_entry_gone_is_miss(
    repo: SqliteTaskRepo, cloud: FakeCloud, clock: FakeClock
) -> None:
    root = cloud.preload_tree(H, _scenario(), "0")
    await _put_index(repo, clock, root)
    cloud.hide_entry(root)
    task = await add(repo, clock)
    assert await check_instant(task, cloud=cloud, repo=repo) is None
    assert (await stored(repo)).cloud_file_id is None
    assert await do_submit(task, cloud, repo, clock) is SubmitKind.SUBMITTED
    assert cloud.call_count("add_magnet") == 1


async def test_list_dir_not_found_is_miss(
    repo: SqliteTaskRepo, cloud: FakeCloud, clock: FakeClock
) -> None:
    root = cloud.preload_tree(H, _scenario(), "0")
    await _put_index(repo, clock, root)
    cloud.inject("list_dir", CloudNotFound("gone"), times=None)
    task = await add(repo, clock)
    assert await check_instant(task, cloud=cloud, repo=repo) is None
    assert (await stored(repo)).cloud_file_id is None


async def test_no_index_is_miss_without_calls(
    repo: SqliteTaskRepo, cloud: FakeCloud, clock: FakeClock
) -> None:
    task = await add(repo, clock)
    assert await check_instant(task, cloud=cloud, repo=repo) is None
    assert cloud.calls == []


def test_submit_kind_has_no_instant_restore() -> None:
    assert {k.name for k in SubmitKind} == {"SUBMITTED", "ALREADY_SUBMITTED"}


# ---------------------------------------------------------------- 配额


async def test_quota_below_min_blocks_submit_and_changes_nothing(
    repo: SqliteTaskRepo, cloud: FakeCloud, clock: FakeClock
) -> None:
    cloud.quota_remaining = 19
    task = await add(repo, clock)
    before = await stored(repo)
    with pytest.raises(QuotaLow) as ei:
        await do_submit(task, cloud, repo, clock, quota_min=20)
    assert ei.value.remaining == 19
    assert ei.value.minimum == 20
    assert not cloud.was_called("add_magnet")
    assert not cloud.was_called("makedirs")
    assert await stored(repo) == before


async def test_quota_equal_to_min_still_submits(
    repo: SqliteTaskRepo, cloud: FakeCloud, clock: FakeClock
) -> None:
    cloud.quota_remaining = 20
    task = await add(repo, clock)
    assert await do_submit(task, cloud, repo, clock, quota_min=20) is SubmitKind.SUBMITTED


async def test_quota_read_from_kv_without_listing(
    repo: SqliteTaskRepo, cloud: FakeCloud, clock: FakeClock
) -> None:
    await repo.kv_set(KV_QUOTA, encode_quota(Quota(500, 1570), clock.now()))
    task = await add(repo, clock)
    await do_submit(task, cloud, repo, clock)
    assert not cloud.was_called("list_offline")


async def test_quota_fetched_once_then_decremented_locally(
    repo: SqliteTaskRepo, cloud: FakeCloud, clock: FakeClock
) -> None:
    cloud.quota_remaining = 21
    t1 = await add(repo, clock, H)
    t2 = await add(repo, clock, H2)
    await do_submit(t1, cloud, repo, clock)
    assert cloud.call_count("list_offline") == 1
    assert (await repo.kv_get(KV_QUOTA) or "").startswith("20/1570@")
    await do_submit(t2, cloud, repo, clock)  # 20 不低于 20，仍可提交
    assert cloud.call_count("list_offline") == 1
    assert (await repo.kv_get(KV_QUOTA) or "").startswith("19/1570@")
    t3 = await add(repo, clock, "ef" * 20)
    with pytest.raises(QuotaLow):
        await do_submit(t3, cloud, repo, clock)


async def test_quota_exhausted_from_115_propagates(
    repo: SqliteTaskRepo, cloud: FakeCloud, clock: FakeClock
) -> None:
    cloud.quota_remaining = 0
    task = await add(repo, clock)
    with pytest.raises(QuotaExhausted):
        await do_submit(task, cloud, repo, clock, quota_min=0)
    assert (await stored(repo)).submitted_at is None


# ---------------------------------------------------------------- 异常


async def test_submit_rejected_propagates_and_task_unchanged(
    repo: SqliteTaskRepo, cloud: FakeCloud, clock: FakeClock
) -> None:
    cloud.script(H, OfflineScenario(name="x", files=[], reject=(10008, "bad")))
    task = await add(repo, clock)
    before = await stored(repo)
    with pytest.raises(SubmitRejected) as ei:
        await do_submit(task, cloud, repo, clock)
    assert ei.value.errcode == 10008
    assert await stored(repo) == before


@pytest.mark.parametrize(
    "exc",
    [AuthExpired(), VerifyRequired(), RiskControl(), NetworkError(), ServerError()],
)
@pytest.mark.parametrize("method", ["list_offline", "makedirs", "add_magnet"])
async def test_global_errors_propagate_and_task_unchanged(
    repo: SqliteTaskRepo,
    cloud: FakeCloud,
    clock: FakeClock,
    method: str,
    exc: CloudError,
) -> None:
    cloud.inject(method, exc, times=None)
    task = await add(repo, clock)
    before = await stored(repo)
    with pytest.raises(type(exc)):
        await do_submit(task, cloud, repo, clock)
    assert await stored(repo) == before


@pytest.mark.parametrize("method", ["get_entry", "list_dir"])
async def test_global_error_on_instant_restore_check_propagates(
    repo: SqliteTaskRepo, cloud: FakeCloud, clock: FakeClock, method: str
) -> None:
    root = cloud.preload_tree(H, _scenario(), "0")
    await _put_index(repo, clock, root)
    cloud.inject(method, AuthExpired(), times=None)
    task = await add(repo, clock)
    before = await stored(repo)
    with pytest.raises(AuthExpired):
        await check_instant(task, cloud=cloud, repo=repo)
    assert await stored(repo) == before
    assert not cloud.was_called("add_magnet")


# ---------------------------------------------------------------- 配额过期与强制刷新


async def test_stale_quota_is_refreshed_after_poll_seconds(
    repo: SqliteTaskRepo, cloud: FakeCloud, clock: FakeClock
) -> None:
    await repo.kv_set(KV_QUOTA, encode_quota(Quota(5, 1570), clock.now()))
    await clock.advance(61)
    cloud.quota_remaining = 500  # 月初恢复
    task = await add(repo, clock)
    assert await do_submit(task, cloud, repo, clock) is SubmitKind.SUBMITTED
    assert cloud.call_count("list_offline") == 1


async def test_fresh_quota_within_poll_seconds_not_refreshed(
    repo: SqliteTaskRepo, cloud: FakeCloud, clock: FakeClock
) -> None:
    await repo.kv_set(KV_QUOTA, encode_quota(Quota(500, 1570), clock.now()))
    await clock.advance(60)
    task = await add(repo, clock)
    await do_submit(task, cloud, repo, clock)
    assert not cloud.was_called("list_offline")


async def test_poll_seconds_is_a_parameter(
    repo: SqliteTaskRepo, cloud: FakeCloud, clock: FakeClock
) -> None:
    await repo.kv_set(KV_QUOTA, encode_quota(Quota(500, 1570), clock.now()))
    await clock.advance(11)
    task = await add(repo, clock)
    await submit(
        task, cloud=cloud, repo=repo, clock=clock, root_dir=ROOT, quota_min=20, poll_seconds=10
    )
    assert cloud.call_count("list_offline") == 1


async def test_quota_low_forces_one_refresh_then_recovers(
    repo: SqliteTaskRepo, cloud: FakeCloud, clock: FakeClock
) -> None:
    await repo.kv_set(KV_QUOTA, encode_quota(Quota(5, 1570), clock.now()))
    cloud.quota_remaining = 500  # 缓存还新鲜，但实际已恢复
    task = await add(repo, clock)
    assert await do_submit(task, cloud, repo, clock) is SubmitKind.SUBMITTED
    assert cloud.call_count("list_offline") == 1


async def test_quota_low_after_refresh_raises_once(
    repo: SqliteTaskRepo, cloud: FakeCloud, clock: FakeClock
) -> None:
    await repo.kv_set(KV_QUOTA, encode_quota(Quota(5, 1570), clock.now()))
    cloud.quota_remaining = 6
    task = await add(repo, clock)
    with pytest.raises(QuotaLow) as ei:
        await do_submit(task, cloud, repo, clock)
    assert ei.value.remaining == 6
    assert cloud.call_count("list_offline") == 1  # 只强制刷新一次


async def test_quota_without_timestamp_is_treated_as_stale(
    repo: SqliteTaskRepo, cloud: FakeCloud, clock: FakeClock
) -> None:
    await repo.kv_set(KV_QUOTA, "5/1570")
    task = await add(repo, clock)
    await do_submit(task, cloud, repo, clock)
    assert cloud.call_count("list_offline") == 1


# ---------------------------------------------------------------- 保存目录缓存失效


async def test_stale_cached_dir_is_rebuilt_before_add(
    repo: SqliteTaskRepo, cloud: FakeCloud, clock: FakeClock
) -> None:
    await repo.kv_set("cloud.dir:/115-bridge/radarr", "stale-id")
    task = await add(repo, clock)
    assert await do_submit(task, cloud, repo, clock) is SubmitKind.SUBMITTED
    assert cloud.call_count("add_magnet") == 1  # 提交前就核对出失效，add_magnet 只调一次
    new_id = await cloud.makedirs("/115-bridge/radarr")
    assert new_id != "stale-id"
    assert cloud.calls_to("add_magnet")[0].args[1] == new_id
    assert await repo.kv_get("cloud.dir:/115-bridge/radarr") == new_id
    assert (await stored(repo)).submitted_at is not None


async def test_cached_id_that_is_a_file_is_rebuilt(
    repo: SqliteTaskRepo, cloud: FakeCloud, clock: FakeClock
) -> None:
    root = cloud.preload_tree(H2, _scenario_single(), await cloud.makedirs("/elsewhere"))
    [f] = [e for e in await cloud.list_dir(root)]
    assert not f.is_dir
    await repo.kv_set("cloud.dir:/115-bridge/radarr", f.id)
    task = await add(repo, clock)
    await do_submit(task, cloud, repo, clock)
    new_id = await repo.kv_get("cloud.dir:/115-bridge/radarr")
    assert new_id and new_id != f.id
    assert cloud.calls_to("add_magnet")[0].args[1] == new_id


async def test_valid_cached_dir_is_checked_with_get_entry_and_reused(
    repo: SqliteTaskRepo, cloud: FakeCloud, clock: FakeClock
) -> None:
    dir_id = await cloud.makedirs("/115-bridge/radarr")
    await repo.kv_set("cloud.dir:/115-bridge/radarr", dir_id)
    task = await add(repo, clock)
    await do_submit(task, cloud, repo, clock)
    assert [c.args for c in cloud.calls_to("get_entry")] == [(dir_id,)]
    assert cloud.call_count("makedirs") == 1  # 只有上面测试自己的那次
    assert cloud.calls_to("add_magnet")[0].args[1] == dir_id


async def test_bad_cache_between_submits_is_corrected_on_next_submit(
    repo: SqliteTaskRepo, cloud: FakeCloud, clock: FakeClock
) -> None:
    await do_submit(await add(repo, clock), cloud, repo, clock)
    first = await repo.kv_get("cloud.dir:/115-bridge/radarr")
    assert first
    await repo.kv_set("cloud.dir:/115-bridge/radarr", "gone-id")
    await do_submit(await add(repo, clock, h=H2), cloud, repo, clock)
    second = await repo.kv_get("cloud.dir:/115-bridge/radarr")
    assert second == first  # 同路径目录仍在，重建后拿回同一个 id
    assert cloud.calls_to("add_magnet")[1].args[1] == first


async def test_cloud_not_found_on_add_rebuilds_dir_and_retries_once(
    repo: SqliteTaskRepo, cloud: FakeCloud, clock: FakeClock
) -> None:
    old_id = await cloud.makedirs("/115-bridge/radarr")
    await repo.kv_set("cloud.dir:/115-bridge/radarr", old_id)
    cloud.inject("add_magnet", CloudNotFound("dir gone"), times=1)
    task = await add(repo, clock)
    assert await do_submit(task, cloud, repo, clock) is SubmitKind.SUBMITTED
    assert cloud.call_count("add_magnet") == 2
    assert (await stored(repo)).submitted_at is not None


async def test_cloud_not_found_twice_raises_save_dir_unavailable(
    repo: SqliteTaskRepo, cloud: FakeCloud, clock: FakeClock
) -> None:
    cloud.inject("add_magnet", CloudNotFound("dir gone"), times=None)
    task = await add(repo, clock)
    before = await stored(repo)
    with pytest.raises(SaveDirUnavailable) as ei:
        await do_submit(task, cloud, repo, clock)
    assert not isinstance(ei.value, CloudError)
    assert cloud.call_count("add_magnet") == 2
    assert await stored(repo) == before


async def test_rebuilt_dir_still_unusable_raises_save_dir_unavailable(
    repo: SqliteTaskRepo, cloud: FakeCloud, clock: FakeClock
) -> None:
    # 目录对 get_entry 不可见：缓存核对失败，makedirs 返回同一个 id，重建后核对也失败
    dir_id = await cloud.makedirs("/115-bridge/radarr")
    cloud.hide_entry(dir_id)
    await repo.kv_set("cloud.dir:/115-bridge/radarr", dir_id)
    task = await add(repo, clock)
    before = await stored(repo)
    with pytest.raises(SaveDirUnavailable):
        await do_submit(task, cloud, repo, clock)
    assert cloud.call_count("add_magnet") == 0
    assert await stored(repo) == before


async def test_only_failing_category_cache_is_cleared(
    repo: SqliteTaskRepo, cloud: FakeCloud, clock: FakeClock
) -> None:
    await repo.kv_set("cloud.dir:/115-bridge/tv-sonarr", "keep-me")
    cloud.inject("add_magnet", CloudNotFound("x"), times=1)
    task = await add(repo, clock, category="radarr")
    await do_submit(task, cloud, repo, clock)
    assert await repo.kv_get("cloud.dir:/115-bridge/tv-sonarr") == "keep-me"
