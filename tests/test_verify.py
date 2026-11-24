from __future__ import annotations

import hashlib
import os
import threading
from collections.abc import AsyncIterator
from pathlib import Path

import httpx
import pytest

from bridge import layout
from bridge.api.qbit import create_qbit_app
from bridge.config import Secrets, Settings, WebUIConfig
from bridge.db.repo import (
    ManifestFile,
    Source,
    Stage,
    TaskFile,
    TaskNotFound,
    TaskState,
    TaskUpdate,
)
from bridge.db.sqlite import SqliteTaskRepo
from bridge.pipeline import verify as verify_mod
from bridge.pipeline.verify import (
    NoFilesToVerify,
    TargetExists,
    UnsafeName,
    Verifier,
    VerifyFailed,
    VerifyStatus,
)
from bridge.testing.clock import FakeClock

H = "cd" * 20
DATA = {"a.mkv": b"A" * 5000, "sub/b.srt": b"hello subtitle", "c.nfo": b""}


def sha(b: bytes) -> str:
    return hashlib.sha1(b).hexdigest()


class Env:
    def __init__(self, repo: SqliteTaskRepo, tmp: Path) -> None:
        self.repo = repo
        self.root = tmp / "lib"
        self.incomplete = self.root / ".incomplete"
        self.verifier = Verifier(repo, root=self.root, incomplete=self.incomplete)

    def p(self, rel: str) -> Path:
        return self.incomplete / H / rel

    async def setup(
        self, name: str = "Movie", data: dict[str, bytes] | None = None, *, upper: bool = False
    ) -> None:
        data = DATA if data is None else data
        await self.repo.create_task(H, "radarr", name, Source.MAGNET, FakeClock().now(), "")
        await self.repo.put_manifest(H, [ManifestFile(k, len(v)) for k, v in data.items()])
        tfs = []
        for k, v in data.items():
            s = sha(v).upper() if upper else sha(v)
            tfs.append(TaskFile(k, len(v), s, "pc", local_bytes=len(v)))
            self.p(k).parent.mkdir(parents=True, exist_ok=True)
            self.p(k).write_bytes(v)
        await self.repo.put_task_files(H, tfs)

    async def retries(self) -> int:
        t = await self.repo.get_task(H)
        assert t is not None
        return t.retries.get(Stage.VERIFY, 0)

    def library_files(self) -> list[str]:
        if not self.root.exists():
            return []
        return sorted(
            str(p.relative_to(self.root))
            for p in self.root.rglob("*")
            if ".incomplete" not in p.parts and not p.is_dir()
        )


@pytest.fixture
async def env(tmp_path: Path) -> AsyncIterator[Env]:
    r = await SqliteTaskRepo.open(tmp_path / "t.db")
    yield Env(r, tmp_path)
    await r.close()


async def test_multi_file_renamed(env: Env) -> None:
    await env.setup()
    res = await env.verifier.verify(H)
    assert res.status is VerifyStatus.RENAMED
    task = await env.repo.get_task(H)
    assert task is not None
    expected = layout.content_path(str(env.root), task, await env.repo.get_manifest(H))
    assert Path(res.final_path or "") == Path(expected)
    for k, v in DATA.items():
        assert (Path(expected) / k).read_bytes() == v
    assert not (env.incomplete / H).exists()
    assert all(f.verified for f in await env.repo.list_task_files(H))
    assert await env.retries() == 0


async def test_single_file_renamed(env: Env) -> None:
    await env.setup("one.mkv", {"one.mkv": b"xyz" * 100})
    res = await env.verifier.verify(H)
    assert res.status is VerifyStatus.RENAMED
    task = await env.repo.get_task(H)
    assert task is not None
    expected = Path(layout.content_path(str(env.root), task, await env.repo.get_manifest(H)))
    assert Path(res.final_path or "") == expected
    assert [Path(f).as_posix() for f in env.library_files()] == ["radarr/one.mkv"]
    assert not (env.incomplete / H).exists()


async def test_sha1_case_insensitive(env: Env) -> None:
    await env.setup(upper=True)
    assert (await env.verifier.verify(H)).status is VerifyStatus.RENAMED


async def test_size_mismatch_deletes_only_that_file(env: Env) -> None:
    await env.setup()
    env.p("a.mkv").write_bytes(b"A" * 4999)
    res = await env.verifier.verify(H)
    assert res.status is VerifyStatus.REFETCH
    assert res.bad_files == ("a.mkv",) and res.retries == 1
    assert not env.p("a.mkv").exists()
    assert env.p("sub/b.srt").read_bytes() == DATA["sub/b.srt"]
    assert env.p("c.nfo").exists()
    files = {f.rel_path: f for f in await env.repo.list_task_files(H)}
    assert not files["a.mkv"].verified and files["a.mkv"].local_bytes == 0
    assert files["sub/b.srt"].verified
    assert env.library_files() == []


async def test_refetch_reduces_task_local_bytes(env: Env) -> None:
    await env.setup()
    total = sum(len(v) for v in DATA.values())
    await env.repo.update(H, TaskUpdate(local_bytes=total))
    env.p("a.mkv").write_bytes(b"A" * 4999)
    res = await env.verifier.verify(H)
    assert res.status is VerifyStatus.REFETCH
    t = await env.repo.get_task(H)
    assert t is not None and t.local_bytes == total - len(DATA["a.mkv"])


async def test_unknown_task_raises_repo_task_not_found(env: Env) -> None:
    with pytest.raises(TaskNotFound):
        await env.verifier.verify(H)


async def test_sha1_mismatch_deletes_only_that_file(env: Env) -> None:
    await env.setup()
    env.p("sub/b.srt").write_bytes(b"HELLO SUBTITLE")  # same size
    res = await env.verifier.verify(H)
    assert res.bad_files == ("sub/b.srt",)
    assert not env.p("sub/b.srt").exists()
    assert env.p("a.mkv").exists()
    assert env.library_files() == []


async def test_missing_file_is_mismatch(env: Env) -> None:
    await env.setup()
    env.p("a.mkv").unlink()
    res = await env.verifier.verify(H)
    assert res.status is VerifyStatus.REFETCH and res.bad_files == ("a.mkv",)


async def test_third_mismatch_raises_and_count_is_three(env: Env) -> None:
    await env.setup()
    for n in (1, 2):
        env.p("a.mkv").write_bytes(b"bad")
        res = await env.verifier.verify(H)
        assert res.retries == n
        env.p("a.mkv").write_bytes(DATA["a.mkv"])  # 模拟重下
    env.p("a.mkv").write_bytes(b"bad")
    with pytest.raises(VerifyFailed) as ei:
        await env.verifier.verify(H)
    assert ei.value.retries == 3
    assert await env.retries() == 3
    assert env.library_files() == []


async def test_verified_and_same_size_skips_rehash(env: Env) -> None:
    await env.setup()
    await env.repo.update_task_file(H, "a.mkv", verified=True)
    env.p("a.mkv").write_bytes(b"B" * 5000)  # 大小不变，内容变了：按约定跳过
    res = await env.verifier.verify(H)
    assert res.status is VerifyStatus.RENAMED and res.skipped == ("a.mkv",)


async def test_verified_but_size_changed_is_rechecked(env: Env) -> None:
    await env.setup()
    await env.repo.update_task_file(H, "a.mkv", verified=True)
    env.p("a.mkv").write_bytes(b"A" * 10)
    res = await env.verifier.verify(H)
    assert res.status is VerifyStatus.REFETCH
    files = {f.rel_path: f for f in await env.repo.list_task_files(H)}
    assert not files["a.mkv"].verified


async def test_target_exists_touches_nothing(env: Env) -> None:
    await env.setup()
    target = env.root / "radarr" / "Movie"
    target.mkdir(parents=True)
    (target / "keep.txt").write_text("mine")
    with pytest.raises(TargetExists):
        await env.verifier.verify(H)
    assert [p.name for p in target.iterdir()] == ["keep.txt"]
    for k, v in DATA.items():
        assert env.p(k).read_bytes() == v
    assert await env.retries() == 0
    assert [p.name for p in (env.root / "radarr").iterdir()] == ["Movie"]


async def test_target_exists_single_file(env: Env) -> None:
    await env.setup("one.mkv", {"one.mkv": b"zz"})
    target = env.root / "radarr" / "one.mkv"
    target.parent.mkdir(parents=True)
    target.write_bytes(b"old")
    with pytest.raises(TargetExists):
        await env.verifier.verify(H)
    assert target.read_bytes() == b"old"
    assert env.p("one.mkv").read_bytes() == b"zz"


@pytest.mark.parametrize("name", ["a/b", "a\\b", "..", ".", "x\x00y"])
async def test_unsafe_name_rejected(env: Env, name: str) -> None:
    await env.setup(name)
    with pytest.raises(UnsafeName):
        await env.verifier.verify(H)
    assert await env.retries() == 0
    assert env.library_files() == []
    assert env.p("a.mkv").exists()


async def test_no_files(env: Env) -> None:
    await env.repo.create_task(H, "radarr", "Movie", Source.MAGNET, FakeClock().now(), "")
    with pytest.raises(NoFilesToVerify):
        await env.verifier.verify(H)


async def test_nothing_in_library_before_rename(env: Env, monkeypatch: pytest.MonkeyPatch) -> None:
    await env.setup()
    seen: list[list[str]] = []
    real = os.rename

    def spy(src: str | os.PathLike[str], dst: str | os.PathLike[str]) -> None:
        seen.append(env.library_files())
        real(src, dst)

    monkeypatch.setattr(os, "rename", spy)
    await env.verifier.verify(H)
    assert seen == [[]]


async def test_hash_runs_in_thread(env: Env, monkeypatch: pytest.MonkeyPatch) -> None:
    await env.setup()
    threads: set[int] = set()
    real = verify_mod._sha1_file

    def spy(p: Path) -> str:
        threads.add(threading.get_ident())
        return real(p)

    monkeypatch.setattr(verify_mod, "_sha1_file", spy)
    await env.verifier.verify(H)
    assert threads and threading.get_ident() not in threads


@pytest.mark.parametrize("single", [False, True])
async def test_qbit_content_path_matches_layout(env: Env, single: bool) -> None:
    if single:
        await env.setup("one.mkv", {"one.mkv": b"zz"})
    else:
        await env.setup()
    t = await env.repo.get_task(H)
    assert t is not None
    settings = Settings(webui=WebUIConfig(username="admin"))
    app = create_qbit_app(
        repo=env.repo,
        settings=settings,
        secrets=Secrets(webui_password="pw"),
        clock=FakeClock(),
        cleaner=None,  # type: ignore[arg-type]
    )
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://t/api/v2"
    ) as c:
        await c.post("/auth/login", data={"username": "admin", "password": "pw"})
        [row] = (await c.get("/torrents/info")).json()
    manifest = await env.repo.get_manifest(H)
    assert row["content_path"] == layout.content_path(settings.paths.root, t, manifest)
    assert row["save_path"] == layout.save_path(settings.paths.root, t.category)
    assert TaskState.QUEUED is t.state
