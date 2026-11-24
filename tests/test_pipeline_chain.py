"""locate -> fetch -> verify 链路测试（不经调度器，T13 尚未实现）。

每个场景都断言：最终文件落在 ``layout.content_path``，并且 qBit 的 ``content_path`` 与
``torrents/files`` 的名字与磁盘上的实际文件一致。
"""

from __future__ import annotations

from collections.abc import AsyncIterator
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import httpx
import pytest

from bridge import layout
from bridge.api.qbit import create_qbit_app
from bridge.cloud.fake import FakeCdn, FakeCloud, FakeFile, OfflineScenario, make_content
from bridge.cloud.protocol import OfflineStat
from bridge.config import FetchConfig, PathsConfig, Secrets, Settings, WebUIConfig
from bridge.db.repo import ManifestFile, Source, TaskState, TaskUpdate
from bridge.db.sqlite import SqliteTaskRepo
from bridge.kv_keys import KV_DIR_PREFIX
from bridge.pipeline.fetch import GIB, Fetcher, FetchStatus
from bridge.pipeline.locate import LocateResult, MatchFailed, locate
from bridge.pipeline.submit import category_path, check_instant, submit
from bridge.pipeline.track import Observation, ObservationKind, track
from bridge.pipeline.verify import Verifier, VerifyStatus
from bridge.testing.clock import FakeClock

H = "ab" * 20
CATEGORY = "radarr"
CLOUD_ROOT = "/115-bridge"
PASSWORD = "pw"

_PATH = [
    TaskState.SUBMITTING,
    TaskState.CLOUD_METADATA,
    TaskState.CLOUD_DOWNLOADING,
    TaskState.CLOUD_DONE,
    TaskState.LOCATING,
    TaskState.FETCHING,
    TaskState.VERIFYING,
    TaskState.COMPLETED,
]


def ff(path: str, size: int) -> FakeFile:
    return FakeFile(path, make_content(path, size))


@dataclass
class Chain:
    repo: SqliteTaskRepo
    clock: FakeClock
    cloud: FakeCloud
    cdn: FakeCdn
    root: Path
    save_dir_id: str

    @property
    def incomplete(self) -> Path:
        return self.root / ".incomplete"

    async def offline(
        self,
        scenario: OfflineScenario,
        *,
        task_name: str,
        manifest: list[ManifestFile] | None,
        cloud_size: int | None = -1,
    ) -> str:
        """让 115 离线完成并建好任务，返回 115 报告的 file_id。"""
        self.cloud.script(H, scenario)
        await self.cloud.add_magnet("magnet:?xt=urn:btih:" + H, self.save_dir_id)
        await self.clock.advance(scenario.ramp_seconds + 1)
        [done] = (await self.cloud.list_offline(OfflineStat.DONE)).tasks
        assert done.is_done and done.file_id is not None
        await self.repo.create_task(
            H, CATEGORY, task_name, Source.MAGNET, self.clock.now(), "magnet:?xt=urn:btih:" + H
        )
        size = scenario.size if cloud_size == -1 else cloud_size
        await self.repo.update(H, TaskUpdate(cloud_file_id=done.file_id, cloud_size=size))
        if manifest is not None:
            await self.repo.put_manifest(H, manifest)
        return done.file_id

    async def locate(self) -> LocateResult:
        task = await self.repo.get_task(H)
        assert task is not None
        return await locate(
            task, repo=self.repo, cloud=self.cloud, clock=self.clock, root_dir=CLOUD_ROOT
        )

    async def submit_and_track(
        self, scenario: OfflineScenario, *, task_name: str, manifest: list[ManifestFile]
    ) -> Observation:
        """按调度器的状态转换：QUEUED -> SUBMITTING -> 云端阶段 -> track 得到 DONE -> LOCATING。"""
        self.cloud.script(H, scenario)
        magnet = "magnet:?xt=urn:btih:" + H
        task, _ = await self.repo.create_task(
            H, CATEGORY, task_name, Source.MAGNET, self.clock.now(), magnet
        )
        await self.repo.put_manifest(H, manifest)
        for st in (TaskState.SUBMITTING,):
            await self.repo.transition(H, st, "t", self.clock.now())
        await submit(
            task,
            cloud=self.cloud,
            repo=self.repo,
            clock=self.clock,
            root_dir=CLOUD_ROOT,
            quota_min=0,
        )
        for st in (TaskState.CLOUD_METADATA, TaskState.CLOUD_DOWNLOADING):
            await self.repo.transition(H, st, "t", self.clock.now())
        await self.clock.advance(scenario.ramp_seconds + 1)
        obs = (await track(cloud=self.cloud, repo=self.repo, clock=self.clock, timeout_hours=12))[H]
        assert obs.kind is ObservationKind.DONE
        for st in (TaskState.CLOUD_DONE, TaskState.LOCATING):
            await self.repo.transition(H, st, "t", self.clock.now())
        return obs

    async def fetch_and_verify(self, path: list[TaskState] = _PATH) -> str:
        fetcher = Fetcher(
            repo=self.repo,
            cloud=self.cloud,
            http=httpx.Client(transport=self.cdn.sync_transport),
            clock=self.clock,
            config=FetchConfig(),
            incomplete_dir=self.incomplete,
            free_space=lambda _p: 10_000 * GIB,
        )
        assert (await fetcher.fetch(H)).status is FetchStatus.DONE
        verifier = Verifier(self.repo, root=self.root, incomplete=self.incomplete)
        res = await verifier.verify(H)
        assert res.status is VerifyStatus.RENAMED and res.final_path is not None
        for s in path:
            await self.repo.transition(H, s, "t", self.clock.now())
        return res.final_path

    async def check_qbit(self, final_path: str) -> list[str]:
        """断言落盘位置与 qBit 报告一致，返回 torrents/files 的名字。"""
        task = await self.repo.get_task(H)
        assert task is not None
        manifest = await self.repo.get_manifest(H)
        expected = layout.content_path(str(self.root), task, manifest)
        assert Path(final_path) == Path(expected)
        settings = Settings(
            webui=WebUIConfig(username="admin"),
            paths=PathsConfig(root=str(self.root), incomplete=str(self.incomplete)),
        )
        app = create_qbit_app(
            repo=self.repo,
            settings=settings,
            secrets=Secrets(webui_password=PASSWORD),
            clock=self.clock,
            cleaner=None,  # type: ignore[arg-type]
        )
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), base_url="http://t/api/v2"
        ) as c:
            await c.post("/auth/login", data={"username": "admin", "password": PASSWORD})
            [row] = (await c.get("/torrents/info")).json()
            files = (await c.get("/torrents/files", params={"hash": H})).json()
        assert row["content_path"] == expected
        assert row["save_path"] == layout.save_path(str(self.root), CATEGORY)
        single = layout.is_single_file(task, manifest)
        check_disk(Path(expected), task.name, single, files)
        assert not incomplete_exists(self.incomplete / H)
        return [f["name"] for f in files]


def incomplete_exists(p: Path) -> bool:
    return p.exists()


def read(p: Path) -> bytes:
    return p.read_bytes()


def remove(p: Path) -> None:
    p.unlink()


def exists(p: Path) -> bool:
    return p.exists()


def check_disk(content: Path, task_name: str, single: bool, files: list[dict[str, Any]]) -> None:
    for f in files:
        if single:
            assert f["name"] == task_name
            target = content
        else:
            prefix = task_name + "/"
            assert f["name"].startswith(prefix)
            target = content / f["name"][len(prefix) :]
        assert target.is_file(), f["name"]
        assert target.stat().st_size == f["size"]
    if not single:
        on_disk = {p.relative_to(content).as_posix() for p in content.rglob("*") if p.is_file()}
        assert on_disk == {f["name"][len(task_name) + 1 :] for f in files}


@pytest.fixture
async def chain(tmp_path: Path) -> AsyncIterator[Chain]:
    repo = await SqliteTaskRepo.open(":memory:")
    clock = FakeClock()
    cdn = FakeCdn(clock, chunk_size=4096, link_ttl=1e12)
    cloud = FakeCloud(clock, cdn=cdn)
    path = category_path(CLOUD_ROOT, CATEGORY)
    save = await cloud.makedirs(path)
    await repo.kv_set(KV_DIR_PREFIX + path, save)
    yield Chain(repo, clock, cloud, cdn, tmp_path / "lib", save)
    await repo.close()


def manifest_of(*files: FakeFile) -> list[ManifestFile]:
    return [ManifestFile(f.path, len(f.content)) for f in files]


async def test_multi_file(chain: Chain) -> None:
    files = [ff("a.mkv", 9000), ff("Sub/b.srt", 40), ff("Sub/Deep/c.nfo", 7)]
    await chain.offline(
        OfflineScenario("Show", files), task_name="Show", manifest=manifest_of(*files)
    )
    res = await chain.locate()
    assert len(res.files) == 3
    final = await chain.fetch_and_verify()
    names = await chain.check_qbit(final)
    assert names == ["Show/Sub/Deep/c.nfo", "Show/Sub/b.srt", "Show/a.mkv"]
    for f in files:
        assert read(Path(final) / f.path) == f.content


async def test_single_file_when_root_is_file_and_equals_save_dir(chain: Chain) -> None:
    f = ff("movie.mkv", 5000)
    sc = OfflineScenario("movie.mkv", [f], single_file_at_root=True)
    file_id = await chain.offline(sc, task_name="movie.mkv", manifest=manifest_of(f))
    # 115 报告的 file_id 就是保存目录：locate 绝不能把保存目录当任务目录
    assert file_id == chain.save_dir_id
    with pytest.raises(MatchFailed, match="save directory"):
        await chain.locate()
    assert await chain.repo.list_task_files(H) == []
    # 实际由 track 按 del_path 与大小在保存目录里解析出文件 id（见 test_track.py），
    # 这里只测 locate 与后续链路，所以手动模拟这一步
    [entry] = [e for e in await chain.cloud.list_dir(file_id) if e.name == "movie.mkv"]
    assert not entry.is_dir
    await chain.repo.update(H, TaskUpdate(cloud_file_id=entry.id))
    res = await chain.locate()
    assert res.root_name == "movie.mkv"
    final = await chain.fetch_and_verify()
    assert (await chain.check_qbit(final)) == ["movie.mkv"]
    assert read(Path(final)) == f.content
    assert Path(final).name == "movie.mkv"


@pytest.mark.parametrize("cloud_name", ["Real.Name", ".."])
async def test_pure_magnet_name_is_sanitized(chain: Chain, cloud_name: str) -> None:
    files = [ff("a.mkv", 6000), ff("d/b.srt", 30)]
    await chain.offline(OfflineScenario(cloud_name, files), task_name="evil/../dn", manifest=None)
    res = await chain.locate()
    expected = cloud_name if cloud_name != ".." else H
    assert res.task_name == expected
    final = await chain.fetch_and_verify()
    names = await chain.check_qbit(final)
    assert names == [f"{expected}/a.mkv", f"{expected}/d/b.srt"]
    assert Path(final).name == expected


@pytest.mark.parametrize("pad_on_cloud", [False, True])
async def test_pad_files(chain: Chain, pad_on_cloud: bool) -> None:
    real = [ff("a.mkv", 7000), ff("b.mkv", 3000)]
    pad = ff(".pad/1096", 1096)
    manifest = [ManifestFile("a.mkv", 7000), ManifestFile(".pad/1096", 1096)]
    manifest.append(ManifestFile("b.mkv", 3000))
    cloud_files = [*real, pad] if pad_on_cloud else real
    await chain.offline(OfflineScenario("Pack", cloud_files), task_name="Pack", manifest=manifest)
    res = await chain.locate()
    assert res.skipped_pads == ([] if pad_on_cloud else [".pad/1096"])
    final = await chain.fetch_and_verify()
    names = await chain.check_qbit(final)
    # 实际存在的文件才出现在 torrents/files 里，且与磁盘一致
    assert ("Pack/.pad/1096" in names) is pad_on_cloud
    assert exists(Path(final) / ".pad" / "1096") is pad_on_cloud


async def test_renamed_on_115_fallback_match(chain: Chain) -> None:
    a = ff("a.mkv", 8000)
    b = FakeFile("Sub/b:x.srt", make_content("bx", 50), cloud_name="b_x.srt")
    await chain.offline(
        OfflineScenario("Show", [a, b]), task_name="Show", manifest=manifest_of(a, b)
    )
    res = await chain.locate()
    assert [f.rel_path for f in res.files] == ["Sub/b_x.srt", "a.mkv"]
    final = await chain.fetch_and_verify()
    names = await chain.check_qbit(final)
    assert names == ["Show/Sub/b_x.srt", "Show/a.mkv"]
    assert read(Path(final) / "Sub" / "b_x.srt") == b.content


_TAIL = [TaskState.FETCHING, TaskState.VERIFYING, TaskState.COMPLETED]


async def test_single_file_from_track_to_qbit(chain: Chain) -> None:
    f = ff("movie.mkv", 5000)
    sc = OfflineScenario("movie.mkv", [f], single_file_at_root=True)
    obs = await chain.submit_and_track(sc, task_name="movie.mkv", manifest=manifest_of(f))
    # track 解析出保存目录里的那个文件，而不是保存目录
    [entry] = [e for e in chain.cloud.entries_by_name("movie.mkv") if not e.is_dir]
    assert obs.file_id == entry.id
    assert obs.file_id != chain.save_dir_id
    task = await chain.repo.get_task(H)
    assert task is not None and task.cloud_file_id == entry.id
    res = await chain.locate()
    assert res.root_name == "movie.mkv"
    final = await chain.fetch_and_verify(_TAIL)
    assert (await chain.check_qbit(final)) == ["movie.mkv"]
    assert read(Path(final)) == f.content


async def test_single_file_instant_restore_after_removed(chain: Chain) -> None:
    f = ff("movie.mkv", 5000)
    sc = OfflineScenario("movie.mkv", [f], single_file_at_root=True)
    await chain.submit_and_track(sc, task_name="movie.mkv", manifest=manifest_of(f))
    await chain.locate()
    first = await chain.fetch_and_verify(_TAIL)
    await chain.check_qbit(first)
    [entry] = [e for e in chain.cloud.entries_by_name("movie.mkv") if not e.is_dir]

    # 用户在 Arr 里删除：任务 removed，本地文件被清理；之后同一个种子重新入队
    await chain.repo.transition(H, TaskState.REMOVED, "t", chain.clock.now())
    remove(Path(first))
    await chain.repo.requeue_removed(H, "re-add", chain.clock.now())
    task = await chain.repo.get_task(H)
    assert task is not None and task.state is TaskState.QUEUED and task.cloud_file_id is None

    index = await check_instant(task, cloud=chain.cloud, repo=chain.repo)
    assert index is not None and index.cloud_file_id == entry.id
    await chain.repo.transition(H, TaskState.LOCATING, "instant", chain.clock.now())
    task = await chain.repo.get_task(H)
    assert task is not None and task.cloud_file_id == entry.id
    assert chain.cloud.call_count("add_magnet") == 1  # 没有再次提交
    res = await chain.locate()
    assert res.root_name == "movie.mkv"
    final = await chain.fetch_and_verify(_TAIL)
    assert final == first
    assert (await chain.check_qbit(final)) == ["movie.mkv"]
    assert read(Path(final)) == f.content
