from __future__ import annotations

from collections.abc import AsyncIterator
from dataclasses import replace
from pathlib import Path

import pytest

from bridge.cloud.fake import FakeCloud, FakeFile, OfflineScenario
from bridge.cloud.protocol import AuthExpired, CloudNotFound
from bridge.db.repo import ManifestFile, Source, Task, TaskFile, TaskUpdate
from bridge.db.sqlite import SqliteTaskRepo
from bridge.kv_keys import KV_DIR_PREFIX
from bridge.pipeline.locate import (
    LocateError,
    MatchFailed,
    MissingCloudRoot,
    locate,
    sanitize_name,
)
from bridge.testing.clock import FakeClock

H = "ab" * 20
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


def ff(path: str, size: int, cloud_name: str | None = None) -> FakeFile:
    return FakeFile(path, (path.encode() * size)[:size], cloud_name)


async def setup(
    repo: SqliteTaskRepo,
    cloud: FakeCloud,
    scenario: OfflineScenario,
    manifest: list[ManifestFile] | None,
    *,
    cloud_size: int | None = -1,
    name: str = "dn-name",
    file_form: bool = False,
) -> Task:
    root = cloud.preload_tree(H, scenario, "0")
    file_id = root
    if file_form:
        file_id = (await cloud.list_dir(root))[0].id
    t0 = FakeClock().now()
    await repo.create_task(H, "radarr", name, Source.MAGNET, t0, "magnet:?xt=urn:btih:" + H)
    size = scenario.size if cloud_size == -1 else cloud_size
    await repo.update(H, TaskUpdate(cloud_file_id=file_id, cloud_size=size))
    if manifest is not None:
        await repo.put_manifest(H, manifest)
    t = await repo.get_task(H)
    assert t is not None
    return t


def sc(*files: FakeFile, name: str = "Root") -> OfflineScenario:
    return OfflineScenario(name=name, files=list(files))


async def test_multi_file_exact(repo: SqliteTaskRepo, cloud: FakeCloud, clock: FakeClock) -> None:
    s = sc(ff("a.mkv", 10), ff("Sub/b.srt", 4), ff("Sub/Deep/c.nfo", 3))
    m = [ManifestFile("a.mkv", 10), ManifestFile("Sub/b.srt", 4), ManifestFile("Sub/Deep/c.nfo", 3)]
    t = await setup(repo, cloud, s, m)
    res = await locate(t, repo=repo, cloud=cloud, clock=clock, root_dir=ROOT)
    assert [f.rel_path for f in res.files] == ["Sub/Deep/c.nfo", "Sub/b.srt", "a.mkv"]
    assert all(f.sha1 == f.sha1.lower() and len(f.sha1) == 40 for f in res.files)
    assert all(f.pickcode.startswith("pc") for f in res.files)
    stored = await repo.list_task_files(H)
    assert {f.rel_path for f in stored} == {f.rel_path for f in res.files}
    assert cloud.call_count("get_entry") == 1
    # 有清单不改名
    got = await repo.get_task(H)
    assert got is not None and got.name == "dn-name"


async def test_cloud_index_written(
    repo: SqliteTaskRepo, cloud: FakeCloud, clock: FakeClock
) -> None:
    s = sc(ff("a.mkv", 10), ff("b.mkv", 5))
    t = await setup(repo, cloud, s, [ManifestFile("a.mkv", 10), ManifestFile("b.mkv", 5)])
    await clock.advance(30)
    await locate(t, repo=repo, cloud=cloud, clock=clock, root_dir=ROOT)
    idx = await repo.get_cloud_index(H)
    assert idx is not None
    assert idx.cloud_file_id == t.cloud_file_id
    assert idx.root_name == "Root"
    assert idx.total_size == 15
    assert idx.completed_at == clock.now()


async def test_renamed_file_falls_back_to_unique_size(
    repo: SqliteTaskRepo, cloud: FakeCloud, clock: FakeClock
) -> None:
    s = sc(ff("a.mkv", 10), ff("Sub/b:x.srt", 4, cloud_name="b_x.srt"))
    m = [ManifestFile("a.mkv", 10), ManifestFile("Sub/b:x.srt", 4)]
    t = await setup(repo, cloud, s, m)
    res = await locate(t, repo=repo, cloud=cloud, clock=clock, root_dir=ROOT)
    assert "Sub/b_x.srt" in [f.rel_path for f in res.files]  # 取 115 上的实际名字


async def test_same_dir_same_size_ambiguous_fails(
    repo: SqliteTaskRepo, cloud: FakeCloud, clock: FakeClock
) -> None:
    s = sc(ff("x?.mkv", 5, "x_.mkv"), ff("y?.mkv", 5, "y_.mkv"))
    m = [ManifestFile("x?.mkv", 5), ManifestFile("y?.mkv", 5)]
    t = await setup(repo, cloud, s, m)
    with pytest.raises(MatchFailed):
        await locate(t, repo=repo, cloud=cloud, clock=clock, root_dir=ROOT)
    assert await repo.list_task_files(H) == []
    assert await repo.get_cloud_index(H) is None


async def test_partial_rename_same_size_matches(
    repo: SqliteTaskRepo, cloud: FakeCloud, clock: FakeClock
) -> None:
    s = sc(ff("a.mkv", 5), ff("b?.mkv", 5, "b_.mkv"))
    m = [ManifestFile("a.mkv", 5), ManifestFile("b?.mkv", 5)]
    t = await setup(repo, cloud, s, m)
    res = await locate(t, repo=repo, cloud=cloud, clock=clock, root_dir=ROOT)
    assert [f.rel_path for f in res.files] == ["a.mkv", "b_.mkv"]


async def test_extra_file_on_cloud_fails(
    repo: SqliteTaskRepo, cloud: FakeCloud, clock: FakeClock
) -> None:
    s = sc(ff("a.mkv", 10), ff("extra.txt", 2))
    t = await setup(repo, cloud, s, [ManifestFile("a.mkv", 10)])
    with pytest.raises(MatchFailed, match="extra.txt"):
        await locate(t, repo=repo, cloud=cloud, clock=clock, root_dir=ROOT)


async def test_missing_file_fails(repo: SqliteTaskRepo, cloud: FakeCloud, clock: FakeClock) -> None:
    s = sc(ff("a.mkv", 10))
    t = await setup(repo, cloud, s, [ManifestFile("a.mkv", 10), ManifestFile("b.mkv", 7)])
    with pytest.raises(MatchFailed, match="b.mkv"):
        await locate(t, repo=repo, cloud=cloud, clock=clock, root_dir=ROOT)


async def test_size_mismatch_same_path_fails(
    repo: SqliteTaskRepo, cloud: FakeCloud, clock: FakeClock
) -> None:
    s = sc(ff("a.mkv", 10))
    t = await setup(repo, cloud, s, [ManifestFile("a.mkv", 11)])
    with pytest.raises(MatchFailed):
        await locate(t, repo=repo, cloud=cloud, clock=clock, root_dir=ROOT)


async def test_pad_file_missing_is_skipped(
    repo: SqliteTaskRepo, cloud: FakeCloud, clock: FakeClock
) -> None:
    s = sc(ff("a.mkv", 10), ff("b.mkv", 6))
    m = [ManifestFile("a.mkv", 10), ManifestFile(".pad/3", 3), ManifestFile("b.mkv", 6)]
    t = await setup(repo, cloud, s, m)
    res = await locate(t, repo=repo, cloud=cloud, clock=clock, root_dir=ROOT)
    assert res.skipped_pads == [".pad/3"]
    assert len(res.files) == 2


async def test_pad_file_present_is_matched(
    repo: SqliteTaskRepo, cloud: FakeCloud, clock: FakeClock
) -> None:
    s = sc(ff("a.mkv", 10), ff(".pad/3", 3))
    m = [ManifestFile("a.mkv", 10), ManifestFile(".pad/3", 3)]
    t = await setup(repo, cloud, s, m)
    res = await locate(t, repo=repo, cloud=cloud, clock=clock, root_dir=ROOT)
    assert [f.rel_path for f in res.files] == [".pad/3", "a.mkv"]
    assert res.skipped_pads == []


async def test_single_file_dir_form(
    repo: SqliteTaskRepo, cloud: FakeCloud, clock: FakeClock
) -> None:
    s = sc(ff("movie.mkv", 20), name="movie")
    t = await setup(repo, cloud, s, [ManifestFile("movie.mkv", 20)])
    res = await locate(t, repo=repo, cloud=cloud, clock=clock, root_dir=ROOT)
    assert [f.rel_path for f in res.files] == ["movie.mkv"]
    assert res.root_name == "movie"


async def test_single_file_file_form(
    repo: SqliteTaskRepo, cloud: FakeCloud, clock: FakeClock
) -> None:
    s = sc(ff("movie.mkv", 20), name="movie")
    t = await setup(repo, cloud, s, [ManifestFile("movie.mkv", 20)], file_form=True)
    n = cloud.call_count("list_dir")
    res = await locate(t, repo=repo, cloud=cloud, clock=clock, root_dir=ROOT)
    assert [f.rel_path for f in res.files] == ["movie.mkv"]
    assert res.root_name == "movie.mkv"
    assert cloud.call_count("list_dir") == n
    idx = await repo.get_cloud_index(H)
    assert idx is not None and idx.root_name == "movie.mkv"


async def test_no_manifest_size_sum_ok_and_renames(
    repo: SqliteTaskRepo, cloud: FakeCloud, clock: FakeClock
) -> None:
    s = sc(ff("a.mkv", 10), ff("d/b.mkv", 5), name="Real.Name")
    t = await setup(repo, cloud, s, None, name="evil/../dn")
    res = await locate(t, repo=repo, cloud=cloud, clock=clock, root_dir=ROOT)
    assert res.total_size == 15
    got = await repo.get_task(H)
    assert got is not None and got.name == "Real.Name"
    assert res.task_name == "Real.Name"


async def test_no_manifest_size_sum_wrong_fails(
    repo: SqliteTaskRepo, cloud: FakeCloud, clock: FakeClock
) -> None:
    s = sc(ff("a.mkv", 10))
    t = await setup(repo, cloud, s, None, cloud_size=99)
    with pytest.raises(MatchFailed, match="99"):
        await locate(t, repo=repo, cloud=cloud, clock=clock, root_dir=ROOT)
    assert await repo.list_task_files(H) == []


async def test_no_manifest_cloud_size_none_fails(
    repo: SqliteTaskRepo, cloud: FakeCloud, clock: FakeClock
) -> None:
    s = sc(ff("a.mkv", 10))
    t = await setup(repo, cloud, s, None, cloud_size=None)
    with pytest.raises(MatchFailed, match="size unknown"):
        await locate(t, repo=repo, cloud=cloud, clock=clock, root_dir=ROOT)
    assert await repo.list_task_files(H) == []


async def test_root_is_category_save_dir_fails(
    repo: SqliteTaskRepo, cloud: FakeCloud, clock: FakeClock
) -> None:
    save = await cloud.makedirs("/115-bridge/radarr")
    s = sc(ff("movie.mkv", 20), name="movie")
    root = cloud.preload_tree(H, s, save)
    await repo.create_task(H, "radarr", "movie", Source.MAGNET, FakeClock().now(), "m")
    await repo.update(H, TaskUpdate(cloud_file_id=save, cloud_size=20))
    await repo.kv_set(KV_DIR_PREFIX + "/115-bridge/radarr", save)
    t = await repo.get_task(H)
    assert t is not None and root != save
    n = cloud.call_count("list_dir")
    with pytest.raises(MatchFailed, match="save directory"):
        await locate(t, repo=repo, cloud=cloud, clock=clock, root_dir=ROOT)
    assert cloud.call_count("list_dir") == n
    assert await repo.list_task_files(H) == []


async def test_list_dir_cloud_not_found_propagates(
    repo: SqliteTaskRepo, cloud: FakeCloud, clock: FakeClock
) -> None:
    s = sc(ff("a.mkv", 10), ff("d/b.mkv", 5))
    t = await setup(repo, cloud, s, None)
    cloud.inject("list_dir", CloudNotFound("gone"), on_call=2)
    with pytest.raises(CloudNotFound):
        await locate(t, repo=repo, cloud=cloud, clock=clock, root_dir=ROOT)


async def test_relocate_identical_keeps_progress(
    repo: SqliteTaskRepo, cloud: FakeCloud, clock: FakeClock
) -> None:
    s = sc(ff("a.mkv", 10), ff("b.mkv", 5))
    t = await setup(repo, cloud, s, [ManifestFile("a.mkv", 10), ManifestFile("b.mkv", 5)])
    await locate(t, repo=repo, cloud=cloud, clock=clock, root_dir=ROOT)
    await repo.update_task_file(H, "a.mkv", local_bytes=10, verified=True)
    await repo.update_task_file(H, "b.mkv", local_bytes=2)
    res = await locate(t, repo=repo, cloud=cloud, clock=clock, root_dir=ROOT)
    stored = {f.rel_path: f for f in await repo.list_task_files(H)}
    assert (stored["a.mkv"].local_bytes, stored["a.mkv"].verified) == (10, True)
    assert (stored["b.mkv"].local_bytes, stored["b.mkv"].verified) == (2, False)
    assert {f.rel_path: f.local_bytes for f in res.files} == {"a.mkv": 10, "b.mkv": 2}


async def test_relocate_different_replaces(
    repo: SqliteTaskRepo, cloud: FakeCloud, clock: FakeClock
) -> None:
    s = sc(ff("a.mkv", 10), ff("b.mkv", 5))
    t = await setup(repo, cloud, s, [ManifestFile("a.mkv", 10), ManifestFile("b.mkv", 5)])
    await locate(t, repo=repo, cloud=cloud, clock=clock, root_dir=ROOT)
    await repo.put_task_files(
        H,
        [
            TaskFile("a.mkv", 10, "f" * 40, "stale", local_bytes=10, verified=True),
            TaskFile("b.mkv", 5, "f" * 40, "stale", local_bytes=5),
        ],
    )
    await locate(t, repo=repo, cloud=cloud, clock=clock, root_dir=ROOT)
    stored = await repo.list_task_files(H)
    assert all(f.local_bytes == 0 and not f.verified and f.pickcode != "stale" for f in stored)


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        ("a/b", "ab"),
        ("a\\b", "ab"),
        ("a\x00b", "ab"),
        ("..", H),
        (".", H),
        ("", H),
        ("/", H),
        ("/..", H),
        ("ok name", "ok name"),
    ],
)
def test_sanitize_name(raw: str, expected: str) -> None:
    assert sanitize_name(raw, H) == expected


async def test_no_manifest_name_sanitized_to_hash(
    repo: SqliteTaskRepo, cloud: FakeCloud, clock: FakeClock
) -> None:
    s = sc(ff("a.mkv", 10), name="..")
    t = await setup(repo, cloud, s, None)
    res = await locate(t, repo=repo, cloud=cloud, clock=clock, root_dir=ROOT)
    assert res.task_name == H
    got = await repo.get_task(H)
    assert got is not None and got.name == H


async def test_get_entry_none_raises(
    repo: SqliteTaskRepo, cloud: FakeCloud, clock: FakeClock
) -> None:
    s = sc(ff("a.mkv", 10))
    t = await setup(repo, cloud, s, None)
    assert t.cloud_file_id is not None
    cloud.hide_entry(t.cloud_file_id)
    with pytest.raises(CloudNotFound):
        await locate(t, repo=repo, cloud=cloud, clock=clock, root_dir=ROOT)


async def test_cloud_file_id_none_raises(
    repo: SqliteTaskRepo, cloud: FakeCloud, clock: FakeClock
) -> None:
    s = sc(ff("a.mkv", 10))
    t = await setup(repo, cloud, s, None)
    with pytest.raises(MissingCloudRoot) as ei:
        await locate(
            replace(t, cloud_file_id=None), repo=repo, cloud=cloud, clock=clock, root_dir=ROOT
        )
    assert not isinstance(ei.value, CloudNotFound)  # 不能触发回退
    assert isinstance(ei.value, LocateError)


async def test_auth_expired_propagates(
    repo: SqliteTaskRepo, cloud: FakeCloud, clock: FakeClock
) -> None:
    s = sc(ff("a.mkv", 10), ff("d/b.mkv", 5))
    t = await setup(repo, cloud, s, None)
    cloud.inject("list_dir", AuthExpired("expired"), on_call=2)
    with pytest.raises(AuthExpired):
        await locate(t, repo=repo, cloud=cloud, clock=clock, root_dir=ROOT)
    assert await repo.list_task_files(H) == []


async def test_only_list_dir_and_get_entry_used(
    repo: SqliteTaskRepo, cloud: FakeCloud, clock: FakeClock
) -> None:
    s = sc(ff("a.mkv", 10), ff("d/b.mkv", 5))
    t = await setup(repo, cloud, s, None)
    n = len(cloud.calls)
    await locate(t, repo=repo, cloud=cloud, clock=clock, root_dir=ROOT)
    assert {c.method for c in cloud.calls[n:]} <= {"get_entry", "list_dir"}
