from __future__ import annotations

import asyncio
import contextlib
import itertools
from collections.abc import AsyncIterator
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest
from hypothesis import given, settings
from hypothesis import strategies as st

from bridge.db.repo import (
    ACTIVE_STATES,
    ALLOWED_TRANSITIONS,
    EVENTS_PER_TASK,
    CloudIndexEntry,
    Fallback,
    InvalidTransition,
    ManifestFile,
    Source,
    Stage,
    StateMismatch,
    TaskFile,
    TaskNotFound,
    TaskRepo,
    TaskState,
    TaskUpdate,
    can_transition,
)
from bridge.db.sqlite import SqliteTaskRepo

S = TaskState
T0 = datetime(2026, 1, 1, tzinfo=UTC)
H = "a" * 40

# 到达各状态的最短合法路径（从 QUEUED 出发）
PATHS: dict[TaskState, list[TaskState]] = {
    S.QUEUED: [],
    S.SUBMITTING: [S.SUBMITTING],
    S.CLOUD_METADATA: [S.SUBMITTING, S.CLOUD_METADATA],
    S.CLOUD_DOWNLOADING: [S.SUBMITTING, S.CLOUD_METADATA, S.CLOUD_DOWNLOADING],
    S.CLOUD_DONE: [S.SUBMITTING, S.CLOUD_METADATA, S.CLOUD_DOWNLOADING, S.CLOUD_DONE],
    S.LOCATING: [S.LOCATING],
    S.FETCHING: [S.LOCATING, S.FETCHING],
    S.VERIFYING: [S.LOCATING, S.FETCHING, S.VERIFYING],
    S.COMPLETED: [S.LOCATING, S.FETCHING, S.VERIFYING, S.COMPLETED],
    S.REMOVED: [S.REMOVED],
    S.FAILED: [S.FAILED],
}


@pytest.fixture
async def repo(tmp_path: Path) -> AsyncIterator[SqliteTaskRepo]:
    r = await SqliteTaskRepo.open(tmp_path / "bridge.db")
    yield r
    await r.close()


async def make_task_in(repo: SqliteTaskRepo, state: TaskState, h: str = H) -> None:
    await repo.create_task(h, "radarr", "n", Source.MAGNET, T0, "")
    for s in PATHS[state]:
        await repo.transition(h, s, "walk", T0)


async def test_implements_protocol(repo: SqliteTaskRepo) -> None:
    assert isinstance(repo, TaskRepo)


async def test_wal_enabled(repo: SqliteTaskRepo) -> None:
    async with repo._conn.execute("PRAGMA journal_mode") as cur:
        row = await cur.fetchone()
    assert row is not None and row[0] == "wal"


async def test_six_tables(repo: SqliteTaskRepo) -> None:
    async with repo._conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'"
    ) as cur:
        names = {r[0] for r in await cur.fetchall()}
    assert names == {"tasks", "task_files", "torrent_meta", "cloud_index", "events", "kv"}


# ------------------------------------------------------------ 状态机


@pytest.mark.parametrize(("src", "dst"), list(itertools.product(S, S)))
async def test_every_edge(repo: SqliteTaskRepo, src: TaskState, dst: TaskState) -> None:
    await make_task_in(repo, src)
    before = len(await repo.list_events(H, 1000))
    if can_transition(src, dst):
        t = await repo.transition(H, dst, "r", T0)
        assert t.state == dst
        events = await repo.list_events(H, 1000)
        assert len(events) == before + 1
        assert (events[0].from_state, events[0].to_state, events[0].reason) == (src, dst, "r")
    else:
        with pytest.raises(InvalidTransition) as ei:
            await repo.transition(H, dst, "r", T0)
        assert (ei.value.src, ei.value.dst) == (src, dst)
        t2 = await repo.get_task(H)
        assert t2 is not None and t2.state == src
        assert len(await repo.list_events(H, 1000)) == before


def test_contract_rules() -> None:
    assert can_transition(S.QUEUED, S.LOCATING)  # 秒取回
    assert can_transition(S.VERIFYING, S.FETCHING)
    assert can_transition(S.COMPLETED, S.REMOVED)
    for dst in S:
        assert not can_transition(S.FAILED, dst)
        assert not can_transition(S.REMOVED, dst)
    for src in ACTIVE_STATES:
        assert can_transition(src, S.FAILED) and can_transition(src, S.REMOVED)
    assert not can_transition(S.COMPLETED, S.FAILED)
    # 云端阶段不跳级
    assert S.CLOUD_DONE not in ALLOWED_TRANSITIONS[S.SUBMITTING]


async def test_some_illegal_edges(repo: SqliteTaskRepo) -> None:
    await make_task_in(repo, S.QUEUED)
    for dst in (S.FETCHING, S.COMPLETED, S.CLOUD_DONE, S.QUEUED):
        with pytest.raises(InvalidTransition):
            await repo.transition(H, dst, "x", T0)


async def test_transition_missing_task(repo: SqliteTaskRepo) -> None:
    with pytest.raises(TaskNotFound):
        await repo.transition(H, S.SUBMITTING, "x", T0)


async def test_expected_match(repo: SqliteTaskRepo) -> None:
    await make_task_in(repo, S.QUEUED)
    t = await repo.transition(H, S.SUBMITTING, "r", T0, expected=S.QUEUED)
    assert t.state == S.SUBMITTING


async def test_expected_mismatch_changes_nothing(repo: SqliteTaskRepo) -> None:
    await make_task_in(repo, S.SUBMITTING)
    before = len(await repo.list_events(H, 1000))
    with pytest.raises(StateMismatch) as ei:
        await repo.transition(H, S.CLOUD_METADATA, "r", T0, expected=S.QUEUED)
    assert (ei.value.info_hash, ei.value.expected, ei.value.actual) == (
        H,
        S.QUEUED,
        S.SUBMITTING,
    )
    t = await repo.get_task(H)
    assert t is not None and t.state == S.SUBMITTING
    assert len(await repo.list_events(H, 1000)) == before


async def test_expected_checked_before_illegal_transition(repo: SqliteTaskRepo) -> None:
    await make_task_in(repo, S.QUEUED)
    # QUEUED -> COMPLETED 非法，但 expected 不符时先报 StateMismatch
    with pytest.raises(StateMismatch):
        await repo.transition(H, S.COMPLETED, "r", T0, expected=S.FETCHING)
    # expected 相符时才报 InvalidTransition
    with pytest.raises(InvalidTransition):
        await repo.transition(H, S.COMPLETED, "r", T0, expected=S.QUEUED)


async def test_expected_missing_task(repo: SqliteTaskRepo) -> None:
    with pytest.raises(TaskNotFound):
        await repo.transition(H, S.SUBMITTING, "x", T0, expected=S.QUEUED)


# ------------------------------------------------------------ requeue_removed


async def test_requeue_removed_success(repo: SqliteTaskRepo) -> None:
    await make_task_in(repo, S.FETCHING)
    await repo.incr_retry(H, Stage.FETCH)
    await repo.incr_retry(H, Stage.FETCH)
    await repo.incr_retry(H, Stage.VERIFY)
    await repo.put_manifest(H, [ManifestFile("a/b.mkv", 5)])
    await repo.put_task_files(H, [TaskFile("a/b.mkv", 5, "s", "p")])
    await repo.put_cloud_index(CloudIndexEntry(H, "123", "root", 5, T0))
    await repo.set_error(H, "boom")
    await repo.set_fallback(H, Fallback.DONE)
    await repo.update(
        H,
        TaskUpdate(
            submitted_at=T0, cloud_file_id="cf", cloud_size=99, cloud_percent=50, local_bytes=7
        ),
    )
    await repo.transition(H, S.REMOVED, "user", T0)
    await repo.mark_client_deleted(H)
    before = len(await repo.list_events(H, 1000))
    old = await repo.get_task(H)
    assert old is not None
    assert old.submitted_at == T0 and old.cloud_file_id == "cf" and old.cloud_size == 99
    assert old.cloud_percent == 50 and old.local_bytes == 7

    t = await repo.requeue_removed(H, "re-add", T0 + timedelta(seconds=1))

    assert t.state == S.QUEUED and t.client_deleted is False
    assert dict(t.retries) == {Stage.FETCH: 2, Stage.VERIFY: 1}
    assert t.error is None and t.fallback == Fallback.NONE  # 重新入队即重新开始
    assert await repo.get_manifest(H) == [ManifestFile("a/b.mkv", 5)]
    # 清掉上一轮进度
    assert t.submitted_at is None and t.cloud_file_id is None and t.cloud_size is None
    assert t.cloud_percent == 0 and t.local_bytes == 0
    assert await repo.list_task_files(H) == []
    # 保留：重试计数（上面）、cloud_index、torrent_meta、magnet、name
    assert t.magnet == old.magnet and t.name == old.name
    ci = await repo.get_cloud_index(H)
    assert ci is not None and ci.cloud_file_id == "123"
    events = await repo.list_events(H, 1000)
    assert len(events) == before + 1
    assert (events[0].from_state, events[0].to_state, events[0].reason) == (
        S.REMOVED,
        S.QUEUED,
        "re-add",
    )
    # 重新入队后可正常推进
    assert (await repo.transition(H, S.SUBMITTING, "r", T0)).state == S.SUBMITTING


@pytest.mark.parametrize("state", [s for s in S if s != S.REMOVED])
async def test_requeue_rejected_unless_removed(repo: SqliteTaskRepo, state: TaskState) -> None:
    await make_task_in(repo, state)
    before = len(await repo.list_events(H, 1000))
    with pytest.raises(InvalidTransition) as ei:
        await repo.requeue_removed(H, "x", T0)
    assert (ei.value.src, ei.value.dst) == (state, S.QUEUED)
    t = await repo.get_task(H)
    assert t is not None and t.state == state
    assert len(await repo.list_events(H, 1000)) == before


async def test_requeue_missing_task(repo: SqliteTaskRepo) -> None:
    with pytest.raises(TaskNotFound):
        await repo.requeue_removed(H, "x", T0)


async def test_plain_transition_cannot_leave_removed(repo: SqliteTaskRepo) -> None:
    await make_task_in(repo, S.REMOVED)
    with pytest.raises(InvalidTransition):
        await repo.transition(H, S.QUEUED, "x", T0)


# ------------------------------------------------------------ 任务 CRUD


async def test_create_idempotent(repo: SqliteTaskRepo) -> None:
    t, created = await repo.create_task(H, "radarr", "n", Source.TORRENT, T0, "")
    assert created and t.state == S.QUEUED and t.added_at == T0
    assert t.fallback == Fallback.NONE and not t.client_deleted and dict(t.retries) == {}
    t2, created2 = await repo.create_task(H, "other", "other", Source.MAGNET, T0, "")
    assert not created2 and t2 == t
    events = await repo.list_events(H)
    assert len(events) == 1 and events[0].from_state is None and events[0].to_state == S.QUEUED


async def test_uppercase_hash_rejected(repo: SqliteTaskRepo) -> None:
    with pytest.raises(ValueError):
        await repo.create_task("A" * 40, "radarr", "n", Source.MAGNET, T0, "")
    with pytest.raises(ValueError):
        await repo.put_cloud_index(CloudIndexEntry("A" * 40, "1", "r", 1, T0))


async def test_naive_datetime_rejected(repo: SqliteTaskRepo) -> None:
    with pytest.raises(ValueError):
        await repo.create_task(H, "radarr", "n", Source.MAGNET, datetime(2026, 1, 1), "")


async def test_update_and_error(repo: SqliteTaskRepo) -> None:
    await make_task_in(repo, S.QUEUED)
    t = await repo.update(
        H,
        TaskUpdate(submitted_at=T0, cloud_file_id="0123456789012345678", cloud_size=5, name="x"),
    )
    assert t.cloud_file_id == "0123456789012345678"  # 字符串，不丢精度
    assert t.cloud_size == 5 and t.name == "x" and t.submitted_at == T0
    t = await repo.update(H, TaskUpdate(cloud_percent=42.5, local_bytes=7))
    assert t.cloud_percent == 42.5 and t.local_bytes == 7 and t.cloud_size == 5
    t = await repo.set_error(H, "boom")
    assert t.error == "boom"
    t = await repo.set_error(H, None)
    assert t.error is None
    with pytest.raises(TaskNotFound):
        await repo.update("b" * 40, TaskUpdate(name="x"))


async def test_list_tasks_filters(repo: SqliteTaskRepo) -> None:
    await make_task_in(repo, S.QUEUED, "a1")
    await repo.create_task("a2", "tv-sonarr", "n", Source.MAGNET, T0 + timedelta(seconds=1), "")
    await repo.transition("a2", S.SUBMITTING, "r", T0)
    await make_task_in(repo, S.FAILED, "a3")
    assert [t.hash for t in await repo.list_tasks()] == ["a1", "a3", "a2"]
    assert [t.hash for t in await repo.list_tasks(category="tv-sonarr")] == ["a2"]
    assert [t.hash for t in await repo.list_tasks(states={S.QUEUED, S.FAILED})] == ["a1", "a3"]
    assert await repo.list_tasks(states=set()) == []
    await repo.mark_client_deleted("a3")
    assert [t.hash for t in await repo.list_tasks()] == ["a1", "a2"]
    assert len(await repo.list_tasks(include_client_deleted=True)) == 3
    t = await repo.get_task("a3")
    assert t is not None and t.client_deleted


async def test_fallback(repo: SqliteTaskRepo) -> None:
    await make_task_in(repo, S.FAILED)
    assert (await repo.set_fallback(H, Fallback.PENDING)).fallback == Fallback.PENDING
    assert (await repo.set_fallback(H, Fallback.DONE)).fallback == Fallback.DONE


async def test_retry_counts_per_stage_and_survive_transitions(repo: SqliteTaskRepo) -> None:
    await make_task_in(repo, S.FETCHING)
    assert await repo.incr_retry(H, Stage.FETCH) == 1
    assert await repo.incr_retry(H, Stage.FETCH) == 2
    assert await repo.incr_retry(H, Stage.VERIFY) == 1
    await repo.transition(H, S.VERIFYING, "r", T0)
    await repo.transition(H, S.FETCHING, "r", T0)
    await repo.transition(H, S.FAILED, "r", T0)
    t = await repo.get_task(H)
    assert t is not None and dict(t.retries) == {Stage.FETCH: 2, Stage.VERIFY: 1}
    with pytest.raises(TaskNotFound):
        await repo.incr_retry("b" * 40, Stage.FETCH)


# ------------------------------------------------------------ 属性测试

OPS = st.one_of(
    st.tuples(st.just("t"), st.sampled_from(list(S))),
    st.tuples(st.just("r"), st.sampled_from(list(Stage))),
    st.tuples(st.just("q"), st.none()),
)


@settings(max_examples=60, deadline=None)
@given(ops=st.lists(OPS, max_size=40))
def test_retries_never_decrease(ops: list[tuple[str, object]]) -> None:
    async def run() -> None:
        repo = await SqliteTaskRepo.open(":memory:")
        try:
            await repo.create_task(H, "radarr", "n", Source.MAGNET, T0, "")
            prev: dict[Stage, int] = {}
            for kind, arg in ops:
                if kind == "t":
                    assert isinstance(arg, TaskState)
                    with contextlib.suppress(InvalidTransition):
                        await repo.transition(H, arg, "p", T0)
                elif kind == "q":
                    with contextlib.suppress(InvalidTransition):
                        await repo.requeue_removed(H, "p", T0)
                else:
                    assert isinstance(arg, Stage)
                    new = await repo.incr_retry(H, arg)
                    assert new == prev.get(arg, 0) + 1
                t = await repo.get_task(H)
                assert t is not None
                cur = dict(t.retries)
                for stage, n in prev.items():
                    assert cur.get(stage, 0) >= n
                prev = cur
        finally:
            await repo.close()

    asyncio.run(run())


# ------------------------------------------------------------ events 裁剪


async def test_events_trimmed_to_200_oldest_first(repo: SqliteTaskRepo) -> None:
    await make_task_in(repo, S.FETCHING)
    await make_task_in(repo, S.QUEUED, "b" * 40)
    for i in range(250):
        dst = S.VERIFYING if i % 2 == 0 else S.FETCHING
        await repo.transition(H, dst, f"n{i}", T0 + timedelta(seconds=i))
    events = await repo.list_events(H, 1000)
    assert len(events) == EVENTS_PER_TASK
    assert events[0].reason == "n249"  # 最新在前
    assert events[-1].reason == "n50"  # 最旧的 53 条（含建立时的事件）已被裁剪
    assert len(await repo.list_events("b" * 40)) == 1  # 其他任务不受影响
    assert len(await repo.list_events(H, 5)) == 5


# ------------------------------------------------------------ 子表与 kv


async def test_manifest_and_task_files(repo: SqliteTaskRepo) -> None:
    await make_task_in(repo, S.QUEUED)
    assert await repo.get_manifest(H) == []
    await repo.put_manifest(H, [ManifestFile("b/2.mkv", 2), ManifestFile("a.mkv", 1)])
    assert await repo.get_manifest(H) == [ManifestFile("a.mkv", 1), ManifestFile("b/2.mkv", 2)]
    await repo.put_manifest(H, [ManifestFile("c.mkv", 3)])
    assert await repo.get_manifest(H) == [ManifestFile("c.mkv", 3)]
    with pytest.raises(TaskNotFound):
        await repo.put_manifest("b" * 40, [])

    f1 = TaskFile("a.mkv", 10, "s1", "pc1")
    await repo.put_task_files(H, [f1, TaskFile("b.mkv", 20, "s2", "pc2")])
    await repo.update_task_file(H, "a.mkv", local_bytes=5)
    await repo.update_task_file(H, "a.mkv", verified=True)
    files = await repo.list_task_files(H)
    assert files[0] == TaskFile("a.mkv", 10, "s1", "pc1", 5, True)
    assert files[1] == TaskFile("b.mkv", 20, "s2", "pc2", 0, False)
    with pytest.raises(TaskNotFound):
        await repo.update_task_file(H, "zzz", local_bytes=1)
    await repo.put_task_files(H, [])
    assert await repo.list_task_files(H) == []


async def test_cloud_index_upsert_and_persists_without_task(repo: SqliteTaskRepo) -> None:
    assert await repo.get_cloud_index(H) is None
    e = CloudIndexEntry(H, "3000000000000000001", "root", 100, T0)
    await repo.put_cloud_index(e)
    assert await repo.get_cloud_index(H) == e
    e2 = CloudIndexEntry(H, "9", "root2", 200, T0 + timedelta(days=1))
    await repo.put_cloud_index(e2)
    assert await repo.get_cloud_index(H) == e2


async def test_kv(repo: SqliteTaskRepo) -> None:
    assert await repo.kv_get("k") is None
    await repo.kv_set("k", "v1")
    await repo.kv_set("k", "v2")
    assert await repo.kv_get("k") == "v2"


async def test_persistence_across_reopen(tmp_path: Path) -> None:
    p = tmp_path / "x.db"
    r1 = await SqliteTaskRepo.open(p)
    await r1.create_task(H, "radarr", "n", Source.MAGNET, T0, "")
    await r1.incr_retry(H, Stage.FETCH)
    await r1.close()
    r2 = await SqliteTaskRepo.open(p)
    t = await r2.get_task(H)
    assert t is not None and t.retries[Stage.FETCH] == 1
    await r2.close()


# ------------------------------------------------------------ lease


async def test_lease_acquire_renew_expire(repo: SqliteTaskRepo) -> None:
    now = T0
    assert not await repo.renew_lease("A", now, 30)  # 没有持有
    assert await repo.acquire_lease("A", now, 30)
    assert await repo.acquire_lease("A", now + timedelta(seconds=5), 30)  # 自己重入
    assert not await repo.acquire_lease("B", now + timedelta(seconds=10), 30)
    assert not await repo.renew_lease("B", now + timedelta(seconds=10), 30)
    # A 在 t=30 续约（上次获取在 t=5，过期 t=35），过期时间推到 t=60
    assert await repo.renew_lease("A", now + timedelta(seconds=30), 30)
    assert not await repo.acquire_lease("B", now + timedelta(seconds=59), 30)
    # 恰好到期即视为过期，B 接管
    assert await repo.acquire_lease("B", now + timedelta(seconds=60), 30)
    # A 已被接管，不能续约；B 持有时 A 拿不到
    assert not await repo.renew_lease("A", now + timedelta(seconds=61), 30)
    assert not await repo.acquire_lease("A", now + timedelta(seconds=61), 30)


async def test_lease_renew_after_expiry_fails(repo: SqliteTaskRepo) -> None:
    assert await repo.acquire_lease("A", T0, 10)
    assert not await repo.renew_lease("A", T0 + timedelta(seconds=10), 10)
    assert await repo.acquire_lease("A", T0 + timedelta(seconds=11), 10)  # 过期后可重新获取


async def test_lease_release(repo: SqliteTaskRepo) -> None:
    assert await repo.acquire_lease("A", T0, 30)
    await repo.release_lease("B")  # 非持有者释放无效
    assert not await repo.acquire_lease("B", T0, 30)
    await repo.release_lease("A")
    assert await repo.acquire_lease("B", T0, 30)


async def test_magnet_stored_read_back_and_not_overwritten(repo: SqliteTaskRepo) -> None:
    m1 = f"magnet:?xt=urn:btih:{H}&dn=a&tr=http%3A%2F%2Ft%2Fa"
    t, created = await repo.create_task(H, "radarr", "n", Source.MAGNET, T0, m1)
    assert created and t.magnet == m1
    t2, created2 = await repo.create_task(H, "radarr", "n", Source.MAGNET, T0, "magnet:?other")
    assert not created2 and t2.magnet == m1
    got = await repo.get_task(H)
    assert got is not None and got.magnet == m1
    assert [x.magnet for x in await repo.list_tasks()] == [m1]
