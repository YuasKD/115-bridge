"""T07：qBittorrent WebAPI 兼容层测试。只用内存仓储和 ASGI 传输，不访问网络。"""

from __future__ import annotations

import posixpath
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import httpx
import pytest

from bridge import layout
from bridge.api.qbit import (
    ETA_INFINITE,
    FetchProgress,
    ZeroProgressSource,
    create_qbit_app,
    qbit_state,
)
from bridge.config import Secrets, Settings, WebUIConfig
from bridge.db.repo import (
    Fallback,
    ManifestFile,
    Source,
    Stage,
    StateMismatch,
    Task,
    TaskFile,
    TaskState,
    TaskUpdate,
)
from bridge.db.sqlite import SqliteTaskRepo
from bridge.testing.clock import FakeClock
from bridge.torrent import build_magnet, parse_torrent

FIXTURES = Path(__file__).parent / "fixtures" / "torrents"
H1 = "a" * 40
H2 = "b" * 40
H3 = "c" * 40
PASSWORD = "s3cret-pass"


@dataclass
class RecordingCleaner:
    calls: list[tuple[str, str]] = field(default_factory=list)

    async def cancel_and_cleanup_incomplete(self, info_hash: str) -> None:
        self.calls.append(("cancel", info_hash))

    async def remove_completed(self, task: Task) -> None:
        self.calls.append(("remove_completed", task.hash))


class MapProgress:
    def __init__(self) -> None:
        self.data: dict[str, FetchProgress] = {}

    def progress(self, info_hash: str) -> FetchProgress:
        return self.data.get(info_hash, FetchProgress())


@dataclass
class Env:
    client: httpx.AsyncClient
    repo: SqliteTaskRepo
    clock: FakeClock
    cleaner: RecordingCleaner
    progress: MapProgress
    settings: Settings


@pytest.fixture
async def env() -> AsyncIterator[Env]:
    repo = await SqliteTaskRepo.open(":memory:")
    clock = FakeClock()
    cleaner = RecordingCleaner()
    progress = MapProgress()
    settings = Settings(webui=WebUIConfig(username="admin"))
    secrets = Secrets(webui_password=PASSWORD)
    app = create_qbit_app(
        repo=repo,
        settings=settings,
        secrets=secrets,
        clock=clock,
        cleaner=cleaner,
        progress_source=progress,
    )
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://t/api/v2") as client:
        r = await client.post("/auth/login", data={"username": "admin", "password": PASSWORD})
        assert r.text == "Ok."
        yield Env(client, repo, clock, cleaner, progress, settings)
    await repo.close()


async def mk(
    env: Env,
    h: str = H1,
    *,
    state: TaskState = TaskState.QUEUED,
    category: str = "radarr",
    name: str = "Movie",
    source: Source = Source.MAGNET,
    manifest: list[ManifestFile] | None = None,
) -> Task:
    await env.repo.create_task(h, category, name, source, env.clock.now(), "")
    if manifest is not None:
        await env.repo.put_manifest(h, manifest)
    path = [
        TaskState.SUBMITTING,
        TaskState.CLOUD_METADATA,
        TaskState.CLOUD_DOWNLOADING,
        TaskState.CLOUD_DONE,
        TaskState.LOCATING,
        TaskState.FETCHING,
        TaskState.VERIFYING,
        TaskState.COMPLETED,
    ]
    if state is TaskState.FAILED:
        await env.repo.transition(h, TaskState.FAILED, "t", env.clock.now())
    elif state is not TaskState.QUEUED:
        for s in path:
            await env.repo.transition(h, s, "t", env.clock.now())
            if s is state:
                break
    task = await env.repo.get_task(h)
    assert task is not None
    return task


async def info(env: Env, **params: str) -> list[dict[str, Any]]:
    r = await env.client.get("/torrents/info", params=params)
    assert r.status_code == 200
    return r.json()  # type: ignore[no-any-return]


# ---------------------------------------------------------------- 认证


async def test_login_sets_sid_and_returns_ok(env: Env) -> None:
    async with httpx.AsyncClient(transport=env.client._transport, base_url="http://t/api/v2") as c:
        r = await c.post("/auth/login", data={"username": "admin", "password": PASSWORD})
        assert r.status_code == 200 and r.text == "Ok."
        assert "SID" in r.cookies
        assert (await c.get("/app/version")).status_code == 200


@pytest.mark.parametrize(
    ("user", "pw"), [("admin", "wrong"), ("root", PASSWORD), ("", ""), ("admin", PASSWORD + "x")]
)
async def test_login_failure(env: Env, user: str, pw: str) -> None:
    async with httpx.AsyncClient(transport=env.client._transport, base_url="http://t/api/v2") as c:
        r = await c.post("/auth/login", data={"username": user, "password": pw})
        assert r.text == "Fails."
        assert "SID" not in r.cookies
        assert (await c.get("/app/version")).status_code == 403


async def test_unauthenticated_is_403_on_every_endpoint(env: Env) -> None:
    async with httpx.AsyncClient(transport=env.client._transport, base_url="http://t/api/v2") as c:
        for path in (
            "/app/version",
            "/app/webapiVersion",
            "/app/preferences",
            "/torrents/info",
            "/torrents/properties?hash=x",
            "/torrents/files?hash=x",
            "/torrents/categories",
        ):
            assert (await c.get(path)).status_code == 403, path
        for path in (
            "/auth/logout",
            "/torrents/add",
            "/torrents/delete",
            "/torrents/createCategory",
            "/torrents/setCategory",
            "/torrents/stop",
            "/torrents/start",
            "/torrents/setShareLimits",
            "/torrents/topPrio",
            "/torrents/setForceStart",
            "/torrents/addTags",
        ):
            assert (await c.post(path)).status_code == 403, path
        c.cookies.set("SID", "forged")
        assert (await c.get("/app/version")).status_code == 403


async def test_logout_invalidates_session(env: Env) -> None:
    r = await env.client.post("/auth/logout")
    assert r.text == "Ok."
    assert (await env.client.get("/app/version")).status_code == 403


async def test_session_expires_and_slides(env: Env) -> None:
    await env.clock.advance(3000)
    assert (await env.client.get("/app/version")).status_code == 200  # 续期
    await env.clock.advance(3000)
    assert (await env.client.get("/app/version")).status_code == 200
    await env.clock.advance(3601)
    assert (await env.client.get("/app/version")).status_code == 403


async def test_wrong_password_not_logged(env: Env, caplog: pytest.LogCaptureFixture) -> None:
    caplog.set_level("DEBUG")
    async with httpx.AsyncClient(transport=env.client._transport, base_url="http://t/api/v2") as c:
        await c.post("/auth/login", data={"username": "admin", "password": "hunter2-wrong"})
        await c.post("/auth/login", data={"username": "admin", "password": PASSWORD})
    text = "\n".join(r.getMessage() + str(r.__dict__) for r in caplog.records)
    assert "hunter2-wrong" not in text
    assert PASSWORD not in text


# ---------------------------------------------------------------- app


async def test_version_endpoints(env: Env) -> None:
    v = (await env.client.get("/app/version")).text
    assert v.startswith("v5.")
    w = (await env.client.get("/app/webapiVersion")).text
    parts = tuple(int(x) for x in w.split("."))
    assert parts >= (2, 11, 2)


async def test_preferences(env: Env) -> None:
    p = (await env.client.get("/app/preferences")).json()
    assert p["dht"] is True
    assert p["max_ratio_enabled"] is True
    assert p["max_ratio"] == 0
    assert p["max_ratio_act"] == 0
    assert p["save_path"] == env.settings.paths.root


# ---------------------------------------------------------------- 状态映射


@pytest.mark.parametrize(
    ("state", "expected"),
    [
        (TaskState.QUEUED, "metaDL"),
        (TaskState.SUBMITTING, "metaDL"),
        (TaskState.CLOUD_METADATA, "metaDL"),
        (TaskState.CLOUD_DOWNLOADING, "queuedDL"),
        (TaskState.CLOUD_DONE, "queuedDL"),
        (TaskState.LOCATING, "queuedDL"),
        (TaskState.FETCHING, "downloading"),
        (TaskState.VERIFYING, "checkingDL"),
        (TaskState.COMPLETED, "stoppedUP"),
        (TaskState.FAILED, "error"),
    ],
)
async def test_state_mapping_rows(env: Env, state: TaskState, expected: str) -> None:
    await mk(env, state=state)
    [t] = await info(env)
    assert t["state"] == expected
    assert t["state"] != "stalledDL"


async def test_stalled_never_reported_for_any_state(env: Env) -> None:
    for s in TaskState:
        task = Task(
            hash=H1,
            category="radarr",
            name="n",
            state=s,
            source=Source.MAGNET,
            added_at=env.clock.now(),
        )
        for waiting in (False, True):
            assert qbit_state(task, FetchProgress(waiting_for_space=waiting)) != "stalledDL"


async def test_fetching_waiting_for_space_is_queued(env: Env) -> None:
    await mk(env, state=TaskState.FETCHING)
    env.progress.data[H1] = FetchProgress(dlspeed=999, eta=5, waiting_for_space=True)
    [t] = await info(env)
    assert t["state"] == "queuedDL"
    assert t["dlspeed"] == 0
    assert t["eta"] == ETA_INFINITE


async def test_fetching_reports_speed_eta_and_progress(env: Env) -> None:
    await mk(env, state=TaskState.FETCHING)
    await env.repo.update(H1, TaskUpdate(cloud_size=1000, local_bytes=250))
    env.progress.data[H1] = FetchProgress(dlspeed=5_000_000, eta=42)
    [t] = await info(env)
    assert t["state"] == "downloading"
    assert t["dlspeed"] == 5_000_000
    assert t["eta"] == 42
    assert t["progress"] == pytest.approx(0.25)
    assert t["size"] == 1000
    assert t["amount_left"] == 750


async def test_default_progress_source_is_zero() -> None:
    p = ZeroProgressSource().progress(H1)
    assert p.dlspeed == 0 and p.eta == ETA_INFINITE and p.waiting_for_space is False


async def test_cloud_percent_is_progress(env: Env) -> None:
    await mk(env, state=TaskState.CLOUD_DOWNLOADING)
    await env.repo.update(H1, TaskUpdate(cloud_percent=37.5, cloud_size=2000))
    [t] = await info(env)
    assert t["state"] == "queuedDL"
    assert t["progress"] == pytest.approx(0.375)
    assert t["dlspeed"] == 0


async def test_metadata_phase_progress_zero(env: Env) -> None:
    await mk(env, state=TaskState.CLOUD_METADATA)
    [t] = await info(env)
    assert t["progress"] == 0


async def test_completed_fields(env: Env) -> None:
    await mk(env, state=TaskState.COMPLETED)
    await env.repo.update(H1, TaskUpdate(cloud_size=1234))
    [t] = await info(env)
    assert t["state"] == "stoppedUP"
    assert t["ratio"] == 0
    assert t["ratio_limit"] == -2
    assert t["progress"] == 1
    assert t["amount_left"] == 0
    assert t["completed"] == 1234
    assert t["completion_on"] > 0


async def test_not_completed_never_reports_stopped_up(env: Env) -> None:
    for i, s in enumerate(
        [TaskState.QUEUED, TaskState.FETCHING, TaskState.VERIFYING, TaskState.FAILED]
    ):
        await mk(env, f"{i:040x}", state=s)
    assert all(t["state"] != "stoppedUP" for t in await info(env))


async def test_removed_and_client_deleted_hidden(env: Env) -> None:
    await mk(env, H1)
    await mk(env, H2)
    await mk(env, H3, state=TaskState.FAILED)
    await env.repo.transition(H2, TaskState.REMOVED, "t", env.clock.now())
    await env.repo.mark_client_deleted(H3)
    assert [t["hash"] for t in await info(env)] == [H1]
    assert (await env.client.get("/torrents/properties", params={"hash": H2})).status_code == 404
    assert (await env.client.get("/torrents/files", params={"hash": H3})).status_code == 404


# ---------------------------------------------------------------- torrents/info


async def test_info_field_names_and_types(env: Env) -> None:
    await mk(env, state=TaskState.FETCHING)
    [t] = await info(env)
    ints = [
        "added_on",
        "amount_left",
        "completed",
        "completion_on",
        "dl_limit",
        "dlspeed",
        "downloaded",
        "downloaded_session",
        "eta",
        "last_activity",
        "max_seeding_time",
        "num_complete",
        "num_incomplete",
        "num_leechs",
        "num_seeds",
        "priority",
        "reannounce",
        "seeding_time",
        "seeding_time_limit",
        "seen_complete",
        "size",
        "time_active",
        "total_size",
        "trackers_count",
        "up_limit",
        "uploaded",
        "uploaded_session",
        "upspeed",
    ]
    for k in ints:
        assert type(t[k]) is int, k
    for k in ("progress", "ratio", "ratio_limit", "availability", "popularity"):
        assert isinstance(t[k], int | float) and not isinstance(t[k], bool), k
    strs = [
        "category",
        "content_path",
        "hash",
        "infohash_v1",
        "infohash_v2",
        "magnet_uri",
        "name",
        "root_path",
        "save_path",
        "state",
        "tags",
        "tracker",
        "comment",
        "download_path",
    ]
    for k in strs:
        assert isinstance(t[k], str), k
    for k in ("auto_tmm", "force_start", "seq_dl", "super_seeding", "private", "f_l_piece_prio"):
        assert type(t[k]) is bool, k
    assert t["hash"] == H1
    assert t["category"] == "radarr"
    assert t["name"] == "Movie"
    assert t["magnet_uri"].startswith("magnet:?xt=urn:btih:" + H1)


async def test_info_filters_category_hashes(env: Env) -> None:
    await mk(env, H1, category="radarr")
    await mk(env, H2, category="tv-sonarr")
    await mk(env, H3, category="radarr")
    assert {t["hash"] for t in await info(env)} == {H1, H2, H3}
    assert {t["hash"] for t in await info(env, category="radarr")} == {H1, H3}
    assert await info(env, category="nope") == []
    assert [t["hash"] for t in await info(env, hashes=f"{H2.upper()}|{H3}")] == [H2, H3]
    assert len(await info(env, hashes="all")) == 3
    assert len(await info(env, limit="2")) == 2
    assert [t["hash"] for t in await info(env, offset="2")] == [H3]


async def test_info_post_form(env: Env) -> None:
    await mk(env, H1)
    r = await env.client.post("/torrents/info", data={"hashes": H1})
    assert [t["hash"] for t in r.json()] == [H1]


# ---------------------------------------------------------------- 路径字段


async def test_paths_multi_file(env: Env) -> None:
    await mk(
        env,
        state=TaskState.COMPLETED,
        name="Show.S01",
        source=Source.TORRENT,
        manifest=[ManifestFile("a.mkv", 10), ManifestFile("sub/b.mkv", 20)],
    )
    [t] = await info(env)
    root = env.settings.paths.root
    assert t["save_path"] == f"{root}/radarr"
    assert t["content_path"] == f"{root}/radarr/Show.S01"
    assert t["root_path"] == f"{root}/radarr/Show.S01"
    assert t["content_path"] != t["save_path"]


async def test_paths_single_file(env: Env) -> None:
    await mk(
        env,
        state=TaskState.COMPLETED,
        name="movie.mkv",
        source=Source.TORRENT,
        manifest=[ManifestFile("movie.mkv", 10)],
    )
    [t] = await info(env)
    root = env.settings.paths.root
    assert t["content_path"] == f"{root}/radarr/movie.mkv"
    assert t["root_path"] == t["content_path"]
    assert t["content_path"] != t["save_path"]


async def test_paths_multi_file_with_one_file(env: Env) -> None:
    await mk(
        env,
        name="Dir",
        source=Source.TORRENT,
        manifest=[ManifestFile("movie.mkv", 10)],
    )
    [t] = await info(env)
    assert t["content_path"] == f"{env.settings.paths.root}/radarr/Dir"


async def test_paths_without_manifest_use_name(env: Env) -> None:
    await mk(env, name="Some.Name")
    [t] = await info(env)
    assert t["content_path"] == f"{env.settings.paths.root}/radarr/Some.Name"
    assert t["content_path"] != t["save_path"]


async def test_paths_from_real_torrents(env: Env) -> None:
    for fname, h in (
        ("big-buck-bunny.torrent", None),
        ("ubuntu-24.04.3-live-server-amd64.iso.torrent", None),
    ):
        data = (FIXTURES / fname).read_bytes()
        pt = parse_torrent(data)
        r = await env.client.post(
            "/torrents/add", files={"torrents": (fname, data)}, data={"category": "radarr"}
        )
        assert r.text == "Ok."
        h = pt.info_hash
        [t] = await info(env, hashes=h)
        assert t["content_path"].endswith("/" + pt.name)
        assert t["content_path"] != t["save_path"]
        assert t["size"] == pt.total_size


# ---------------------------------------------------------------- properties / files


async def test_properties(env: Env) -> None:
    await mk(env, state=TaskState.COMPLETED, name="X", manifest=[ManifestFile("a", 5)])
    r = await env.client.get("/torrents/properties", params={"hash": H1})
    p = r.json()
    assert p["save_path"] == f"{env.settings.paths.root}/radarr"
    assert p["hash"] == H1
    assert p["share_ratio"] == 0
    assert p["total_size"] == 5
    for k in ("addition_date", "completion_date", "creation_date", "dl_speed", "eta", "seeds"):
        assert type(p[k]) is int, k
    assert (await env.client.get("/torrents/properties", params={"hash": H2})).status_code == 404


async def test_files_multi_file_names_include_root(env: Env) -> None:
    await mk(
        env,
        name="Root",
        source=Source.TORRENT,
        manifest=[ManifestFile("a.mkv", 10), ManifestFile("s/b.srt", 20)],
    )
    files = (await env.client.get("/torrents/files", params={"hash": H1})).json()
    assert [f["name"] for f in files] == ["Root/a.mkv", "Root/s/b.srt"]
    assert [f["index"] for f in files] == [0, 1]
    assert [f["size"] for f in files] == [10, 20]
    for f in files:
        assert isinstance(f["progress"], float)
        assert f["priority"] == 1
        assert isinstance(f["is_seed"], bool)
        assert isinstance(f["piece_range"], list)


async def test_files_single_and_completed(env: Env) -> None:
    await mk(
        env,
        state=TaskState.COMPLETED,
        name="m.mkv",
        manifest=[ManifestFile("m.mkv", 10)],
    )
    [f] = (await env.client.get("/torrents/files", params={"hash": H1})).json()
    assert f["name"] == "m.mkv"
    assert f["progress"] == 1
    assert f["is_seed"] is True


async def test_files_use_task_files_names_when_present(env: Env) -> None:
    await mk(
        env,
        name="Root",
        source=Source.TORRENT,
        state=TaskState.FETCHING,
        manifest=[ManifestFile("a.mkv", 10), ManifestFile("s/b:x.srt", 20)],
    )
    await env.repo.put_task_files(
        H1,
        [
            TaskFile("a.mkv", 10, "0" * 40, "pc1", local_bytes=5),
            TaskFile("s/b_x.srt", 20, "1" * 40, "pc2", local_bytes=20),
        ],
    )
    files = (await env.client.get("/torrents/files", params={"hash": H1})).json()
    assert [f["name"] for f in files] == ["Root/a.mkv", "Root/s/b_x.srt"]
    assert [f["progress"] for f in files] == [0.5, 1.0]
    assert [f["size"] for f in files] == [10, 20]


async def test_files_empty_without_manifest_and_unknown_hash(env: Env) -> None:
    await mk(env)
    assert (await env.client.get("/torrents/files", params={"hash": H1})).json() == []
    assert (await env.client.get("/torrents/files", params={"hash": H2})).status_code == 404


async def test_files_single_file_name_is_task_name_not_115_name(env: Env) -> None:
    # 清单 a:b.mkv，115 改名为 a_b.mkv；单文件落盘名恒为 task.name（layout.content_path）
    await mk(
        env,
        name="a:b.mkv",
        state=TaskState.FETCHING,
        manifest=[ManifestFile("a:b.mkv", 10)],
    )
    await env.repo.put_task_files(H1, [TaskFile("a_b.mkv", 10, "0" * 40, "pc1", local_bytes=4)])
    [f] = (await env.client.get("/torrents/files", params={"hash": H1})).json()
    assert f["name"] == "a:b.mkv"
    assert f["size"] == 10
    assert f["progress"] == 0.4
    task = await env.repo.get_task(H1)
    assert task is not None
    manifest = await env.repo.get_manifest(H1)
    content = layout.content_path(env.settings.paths.root, task, manifest)
    assert posixpath.basename(content) == f["name"]


# ---------------------------------------------------------------- add


async def test_add_magnet_queues_only(env: Env) -> None:
    magnet = build_magnet(H1, "My.Movie.2024")
    r = await env.client.post("/torrents/add", data={"urls": magnet, "category": "radarr"})
    assert r.text == "Ok."
    t = await env.repo.get_task(H1)
    assert t is not None
    assert t.state is TaskState.QUEUED
    assert t.source is Source.MAGNET
    assert t.name == "My.Movie.2024"
    assert t.category == "radarr"
    assert await env.repo.get_manifest(H1) == []


async def test_add_magnet_without_dn_uses_hash_and_info_shows_it(env: Env) -> None:
    r = await env.client.post(
        "/torrents/add", data={"urls": f"magnet:?xt=urn:btih:{H1.upper()}", "category": "radarr"}
    )
    assert r.text == "Ok."
    [t] = await info(env)
    assert t["name"] == H1
    assert t["state"] == "metaDL"


async def test_add_multiple_urls_newline_separated_multipart(env: Env) -> None:
    urls = f"{build_magnet(H1, 'a')}\n{build_magnet(H2, 'b')}\r\n\n"
    r = await env.client.post(
        "/torrents/add",
        files={"urls": (None, urls), "category": (None, "tv-sonarr"), "paused": (None, "false")},
    )
    assert r.text == "Ok."
    assert {t["hash"] for t in await info(env, category="tv-sonarr")} == {H1, H2}


async def test_add_torrent_file_stores_manifest(env: Env) -> None:
    data = (FIXTURES / "big-buck-bunny.torrent").read_bytes()
    pt = parse_torrent(data)
    r = await env.client.post(
        "/torrents/add",
        files={"torrents": ("x.torrent", data, "application/x-bittorrent")},
        data={
            "category": "radarr",
            "savepath": "/ignored",
            "paused": "true",
            "tags": "a,b",
            "autoTMM": "false",
        },
    )
    assert r.text == "Ok."
    t = await env.repo.get_task(pt.info_hash)
    assert t is not None
    assert t.source is Source.TORRENT
    assert t.name == pt.name
    assert t.state is TaskState.QUEUED
    assert await env.repo.get_manifest(pt.info_hash) == list(pt.files)


async def test_add_is_idempotent(env: Env) -> None:
    await mk(env, H1, state=TaskState.FETCHING, name="orig")
    r = await env.client.post(
        "/torrents/add", data={"urls": build_magnet(H1, "other"), "category": "radarr"}
    )
    assert r.text == "Ok."
    t = await env.repo.get_task(H1)
    assert t is not None
    assert t.state is TaskState.FETCHING
    assert t.name == "orig"


async def test_add_removed_requeues_keeping_retries(env: Env) -> None:
    await mk(env, H1, state=TaskState.FETCHING)
    await env.repo.incr_retry(H1, Stage.FETCH)
    await env.repo.set_error(H1, "boom")
    await env.repo.set_fallback(H1, Fallback.PENDING)
    await env.repo.transition(H1, TaskState.REMOVED, "t", env.clock.now())
    await env.repo.mark_client_deleted(H1)
    r = await env.client.post(
        "/torrents/add", data={"urls": build_magnet(H1, "n"), "category": "radarr"}
    )
    assert r.text == "Ok."
    t = await env.repo.get_task(H1)
    assert t is not None
    assert t.state is TaskState.QUEUED
    assert t.retries[Stage.FETCH] == 1
    assert t.error is None and t.fallback is Fallback.NONE and t.client_deleted is False
    assert [x["hash"] for x in await info(env)] == [H1]


async def test_add_removed_torrent_backfills_missing_manifest(env: Env) -> None:
    data = (FIXTURES / "big-buck-bunny.torrent").read_bytes()
    pt = parse_torrent(data)
    await mk(env, pt.info_hash, name="old")  # magnet 来源，没有清单
    await env.repo.transition(pt.info_hash, TaskState.REMOVED, "t", env.clock.now())
    r = await env.client.post(
        "/torrents/add", files={"torrents": ("x.torrent", data)}, data={"category": "radarr"}
    )
    assert r.text == "Ok."
    t = await env.repo.get_task(pt.info_hash)
    assert t is not None and t.state is TaskState.QUEUED
    assert await env.repo.get_manifest(pt.info_hash) == list(pt.files)


@pytest.mark.parametrize("client_deleted", [False, True])
async def test_add_failed_hash_fails(env: Env, client_deleted: bool) -> None:
    await mk(env, H1, state=TaskState.FAILED)
    if client_deleted:
        await env.repo.mark_client_deleted(H1)
    r = await env.client.post(
        "/torrents/add", data={"urls": build_magnet(H1, "n"), "category": "radarr"}
    )
    assert r.text == "Fails."
    t = await env.repo.get_task(H1)
    assert t is not None and t.state is TaskState.FAILED


async def test_add_failed_among_successes_is_ok(env: Env) -> None:
    await mk(env, H1, state=TaskState.FAILED)
    urls = f"{build_magnet(H1, 'a')}\n{build_magnet(H2, 'b')}"
    r = await env.client.post("/torrents/add", data={"urls": urls, "category": "radarr"})
    assert r.text == "Ok."
    assert await env.repo.get_task(H2) is not None
    r = await env.client.post(
        "/torrents/add", data={"urls": build_magnet(H1, "a"), "category": "radarr"}
    )
    assert r.text == "Fails."


async def test_add_rejects_unknown_or_missing_category(env: Env) -> None:
    for data in ({"urls": build_magnet(H1, "n"), "category": "bogus"}, {"urls": build_magnet(H1)}):
        r = await env.client.post("/torrents/add", data=data)
        assert r.status_code == 200 and r.text == "Fails."
    assert await env.repo.get_task(H1) is None


async def test_add_garbage_fails(env: Env) -> None:
    r = await env.client.post("/torrents/add", data={"urls": "http://x/y", "category": "radarr"})
    assert r.text == "Fails."
    r = await env.client.post(
        "/torrents/add",
        files={"torrents": ("x.torrent", b"not bencode")},
        data={"category": "radarr"},
    )
    assert r.text == "Fails."
    r = await env.client.post("/torrents/add", data={"category": "radarr"})
    assert r.text == "Fails."
    assert await env.repo.list_tasks() == []


async def test_add_partial_success_is_ok(env: Env) -> None:
    r = await env.client.post(
        "/torrents/add",
        data={"urls": f"junk\n{build_magnet(H1, 'a')}", "category": "radarr"},
    )
    assert r.text == "Ok."
    assert await env.repo.get_task(H1) is not None


# ---------------------------------------------------------------- delete


@pytest.mark.parametrize("delete_files", ["true", "false", ""])
async def test_delete_in_progress(env: Env, delete_files: str) -> None:
    await mk(env, H1, state=TaskState.FETCHING)
    r = await env.client.post("/torrents/delete", data={"hashes": H1, "deleteFiles": delete_files})
    assert r.text == "Ok."
    t = await env.repo.get_task(H1)
    assert t is not None and t.state is TaskState.REMOVED
    assert env.cleaner.calls == [("cancel", H1)]
    assert await info(env) == []


@pytest.mark.parametrize("delete_files", ["true", "false"])
async def test_delete_completed(env: Env, delete_files: str) -> None:
    await mk(env, H1, state=TaskState.COMPLETED)
    r = await env.client.post(
        "/torrents/delete", data={"hashes": H1.upper(), "deleteFiles": delete_files}
    )
    assert r.text == "Ok."
    t = await env.repo.get_task(H1)
    assert t is not None and t.state is TaskState.REMOVED
    assert env.cleaner.calls == [("remove_completed", H1)]


@pytest.mark.parametrize("delete_files", ["true", "false"])
async def test_delete_failed_keeps_state(env: Env, delete_files: str) -> None:
    await mk(env, H1, state=TaskState.FAILED)
    r = await env.client.post("/torrents/delete", data={"hashes": H1, "deleteFiles": delete_files})
    assert r.text == "Ok."
    t = await env.repo.get_task(H1)
    assert t is not None
    assert t.state is TaskState.FAILED
    assert t.client_deleted is True
    assert env.cleaner.calls == [("cancel", H1)]
    assert await info(env) == []


async def test_delete_every_active_state(env: Env) -> None:
    active = [
        s for s in TaskState if s not in (TaskState.COMPLETED, TaskState.REMOVED, TaskState.FAILED)
    ]
    hashes = []
    for i, s in enumerate(active):
        h = f"{i + 1:040x}"
        hashes.append(h)
        await mk(env, h, state=s)
    await env.client.post("/torrents/delete", data={"hashes": "|".join(hashes)})
    for h in hashes:
        t = await env.repo.get_task(h)
        assert t is not None and t.state is TaskState.REMOVED
    assert [c[0] for c in env.cleaner.calls] == ["cancel"] * len(hashes)


async def test_delete_multiple_unknown_and_all(env: Env) -> None:
    await mk(env, H1)
    await mk(env, H2, state=TaskState.COMPLETED)
    r = await env.client.post("/torrents/delete", data={"hashes": f"{H1}|{H3}|"})
    assert r.text == "Ok."
    assert (await env.repo.get_task(H1)).state is TaskState.REMOVED  # type: ignore[union-attr]
    assert (await env.repo.get_task(H2)).state is TaskState.COMPLETED  # type: ignore[union-attr]
    await env.client.post("/torrents/delete", data={"hashes": "all"})
    assert (await env.repo.get_task(H2)).state is TaskState.REMOVED  # type: ignore[union-attr]


async def test_delete_is_idempotent(env: Env) -> None:
    await mk(env, H1, state=TaskState.FAILED)
    await env.client.post("/torrents/delete", data={"hashes": H1})
    await env.client.post("/torrents/delete", data={"hashes": H1})
    assert env.cleaner.calls == [("cancel", H1)]
    await mk(env, H2)
    await env.client.post("/torrents/delete", data={"hashes": H2})
    await env.client.post("/torrents/delete", data={"hashes": H2})
    assert env.cleaner.calls == [("cancel", H1), ("cancel", H2)]


@asynccontextmanager
async def _app_client(env: Env, repo: Any, cleaner: Any) -> AsyncIterator[httpx.AsyncClient]:
    app = create_qbit_app(
        repo=repo,
        settings=env.settings,
        secrets=Secrets(webui_password=PASSWORD),
        clock=env.clock,
        cleaner=cleaner,
    )
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://t/api/v2") as c:
        await c.post("/auth/login", data={"username": "admin", "password": PASSWORD})
        yield c


class RacingRepo:
    """包装仓储：前 times 次 transition 之前，先让流水线把任务推进到 `to`。"""

    def __init__(self, inner: SqliteTaskRepo, env: Env, to: TaskState, times: int = 1) -> None:
        self._inner = inner
        self._env = env
        self._to = to
        self.times = times
        self.mismatches = 0

    def __getattr__(self, name: str) -> Any:
        return getattr(self._inner, name)

    async def transition(self, info_hash: str, dst: TaskState, *a: Any, **kw: Any) -> Task:
        if self.times > 0:
            self.times -= 1
            await self._inner.transition(info_hash, self._to, "race", self._env.clock.now())
        try:
            return await self._inner.transition(info_hash, dst, *a, **kw)
        except StateMismatch:
            self.mismatches += 1
            raise


async def test_delete_race_pipeline_fails_task_first(env: Env) -> None:
    """读状态之后、转换之前流水线把任务标为 failed：重读后按 failed 处理，不清错。"""
    await mk(env, H1, state=TaskState.FETCHING)
    repo = RacingRepo(env.repo, env, TaskState.FAILED)
    async with _app_client(env, repo, env.cleaner) as c:
        r = await c.post("/torrents/delete", data={"hashes": H1})
    assert r.text == "Ok."
    assert repo.mismatches == 1
    t = await env.repo.get_task(H1)
    assert t is not None
    assert t.state is TaskState.FAILED
    assert t.client_deleted is True
    assert env.cleaner.calls == [("cancel", H1)]


async def test_delete_race_pipeline_completes_task_first(env: Env) -> None:
    """流水线在删除转换前把 verifying 推到 completed：必须清成品，不能只清半成品。"""
    await mk(env, H1, state=TaskState.VERIFYING)
    repo = RacingRepo(env.repo, env, TaskState.COMPLETED)
    async with _app_client(env, repo, env.cleaner) as c:
        r = await c.post("/torrents/delete", data={"hashes": H1})
    assert r.text == "Ok."
    assert repo.mismatches == 1
    t = await env.repo.get_task(H1)
    assert t is not None and t.state is TaskState.REMOVED
    assert env.cleaner.calls == [("remove_completed", H1)]


async def test_delete_conflict_limit_returns_500(env: Env) -> None:
    """一直冲突：1 次初始加 3 次重试后放弃，返回 500，且不做任何清理。"""

    class AlwaysMismatch(RacingRepo):
        async def transition(self, info_hash: str, dst: TaskState, *a: Any, **kw: Any) -> Task:
            self.mismatches += 1
            raise StateMismatch(info_hash, TaskState.FETCHING, TaskState.VERIFYING)

    await mk(env, H1, state=TaskState.FETCHING)
    repo = AlwaysMismatch(env.repo, env, TaskState.FAILED, times=0)
    async with _app_client(env, repo, env.cleaner) as c:
        r = await c.post("/torrents/delete", data={"hashes": H1})
    assert r.status_code == 500
    assert repo.mismatches == 4
    assert env.cleaner.calls == []


class FlakyCleaner(RecordingCleaner):
    def __init__(self, fail_for: set[str]) -> None:
        super().__init__()
        self.fail_for = fail_for

    async def cancel_and_cleanup_incomplete(self, info_hash: str) -> None:
        await super().cancel_and_cleanup_incomplete(info_hash)
        if info_hash in self.fail_for:
            raise OSError("disk on fire")

    async def remove_completed(self, task: Task) -> None:
        await super().remove_completed(task)
        if task.hash in self.fail_for:
            raise OSError("disk on fire")


@pytest.mark.parametrize("state", [TaskState.FETCHING, TaskState.COMPLETED, TaskState.FAILED])
async def test_delete_cleanup_error_returns_500_and_retry_cleans_again(
    env: Env, state: TaskState
) -> None:
    await mk(env, H1, state=state)
    cleaner = FlakyCleaner({H1})
    expected_call = "remove_completed" if state is TaskState.COMPLETED else "cancel"
    async with _app_client(env, env.repo, cleaner) as c:
        r = await c.post("/torrents/delete", data={"hashes": H1})
        assert r.status_code == 500
        t = await env.repo.get_task(H1)
        assert t is not None
        # 状态保持在转换后的值，并记录清理失败
        assert t.state is (TaskState.FAILED if state is TaskState.FAILED else TaskState.REMOVED)
        assert t.error is not None and "cleanup failed" in t.error
        assert cleaner.calls == [(expected_call, H1)]
        # Arr 重试：再次尝试清理，不是空操作
        cleaner.fail_for.clear()
        r = await c.post("/torrents/delete", data={"hashes": H1})
        assert r.text == "Ok."
        assert cleaner.calls == [(expected_call, H1)] * 2
        t = await env.repo.get_task(H1)
        assert t is not None and t.error is None
        # 清理成功后再删是空操作
        assert (await c.post("/torrents/delete", data={"hashes": H1})).text == "Ok."
        assert cleaner.calls == [(expected_call, H1)] * 2


async def test_delete_one_hash_failing_does_not_block_others(env: Env) -> None:
    await mk(env, H1, state=TaskState.FETCHING)
    await mk(env, H2, state=TaskState.FETCHING)
    await mk(env, H3, state=TaskState.COMPLETED)
    cleaner = FlakyCleaner({H1})
    async with _app_client(env, env.repo, cleaner) as c:
        r = await c.post("/torrents/delete", data={"hashes": f"{H1}|{H2}|{H3}"})
    assert r.status_code == 500
    assert cleaner.calls == [("cancel", H1), ("cancel", H2), ("remove_completed", H3)]
    for h in (H1, H2, H3):
        t = await env.repo.get_task(h)
        assert t is not None and t.state is TaskState.REMOVED
    t2 = await env.repo.get_task(H2)
    assert t2 is not None and t2.error is None


async def test_qbit_module_never_references_cloud() -> None:
    """qbit 模块及其传递导入的 bridge 模块都不引用 bridge.cloud。"""
    import ast
    import importlib.util

    seen: set[str] = set()
    todo = ["bridge.api.qbit"]
    while todo:
        mod = todo.pop()
        if mod in seen:
            continue
        seen.add(mod)
        try:
            spec = importlib.util.find_spec(mod)
        except ModuleNotFoundError:  # `from pkg.mod import name` 中的 name 不是模块
            continue
        if spec is None or spec.origin is None or not spec.origin.endswith(".py"):
            continue
        with open(spec.origin, encoding="utf-8") as fh:  # noqa: ASYNC230
            tree = ast.parse(fh.read())
        for node in ast.walk(tree):
            names: list[str] = []
            if isinstance(node, ast.Import):
                names = [a.name for a in node.names]
            elif isinstance(node, ast.ImportFrom) and node.level == 0 and node.module:
                names = [node.module] + [f"{node.module}.{a.name}" for a in node.names]
            for n in names:
                if n == "bridge" or n.startswith("bridge."):
                    todo.append(n)
    assert "bridge.db.repo" in seen  # 确认遍历确实走到了依赖
    cloud = sorted(m for m in seen if m == "bridge.cloud" or m.startswith("bridge.cloud."))
    assert cloud == []


# ---------------------------------------------------------------- category 与「接受但不改变行为」


async def test_categories(env: Env) -> None:
    cats = (await env.client.get("/torrents/categories")).json()
    assert set(cats) == set(env.settings.categories)
    assert cats["radarr"] == {
        "name": "radarr",
        "savePath": f"{env.settings.paths.root}/radarr",
    }


async def test_accepted_noop_endpoints_return_success_and_change_nothing(env: Env) -> None:
    await mk(env, H1, state=TaskState.FETCHING)
    before = await env.repo.get_task(H1)
    for name, data in (
        ("createCategory", {"category": "x", "savePath": "/y"}),
        ("setCategory", {"hashes": H1, "category": "tv-sonarr"}),
        ("setCategory", {"hashes": H1, "category": "radarr-imported"}),
        ("stop", {"hashes": H1}),
        ("start", {"hashes": H1}),
        ("setShareLimits", {"hashes": H1, "ratioLimit": -2, "seedingTimeLimit": -2}),
        ("topPrio", {"hashes": H1}),
        ("setForceStart", {"hashes": H1, "value": "true"}),
        ("addTags", {"hashes": H1, "tags": "a,b"}),
    ):
        r = await env.client.post(f"/torrents/{name}", data=data)
        assert r.status_code == 200, name
        assert r.text == "Ok.", name
    assert await env.repo.get_task(H1) == before
    assert env.cleaner.calls == []
    assert set((await env.client.get("/torrents/categories")).json()) == set(
        env.settings.categories
    )


async def test_add_torrent_stores_magnet_with_btih_dn_and_trackers(env: Env) -> None:
    from bridge.torrent import parse_magnet

    data = (FIXTURES / "big-buck-bunny.torrent").read_bytes()
    pt = parse_torrent(data)
    assert pt.trackers
    r = await env.client.post(
        "/torrents/add",
        files={"torrents": ("x.torrent", data, "application/x-bittorrent")},
        data={"category": "radarr"},
    )
    assert r.text == "Ok."
    t = await env.repo.get_task(pt.info_hash)
    assert t is not None
    assert t.magnet == pt.to_magnet()
    mg = parse_magnet(t.magnet)
    assert mg.info_hash == pt.info_hash
    assert mg.display_name == pt.name
    assert set(mg.trackers) == set(pt.trackers)


async def test_add_magnet_saved_verbatim_stripped(env: Env) -> None:
    raw = f"magnet:?xt=urn:btih:{H1.upper()}&dn=Foo%20Bar&tr=udp%3A%2F%2Fx%3A1%2Fannounce&x=1"
    r = await env.client.post("/torrents/add", data={"urls": f"  {raw}  \n", "category": "radarr"})
    assert r.text == "Ok."
    t = await env.repo.get_task(H1)
    assert t is not None and t.magnet == raw


async def test_add_again_does_not_overwrite_magnet(env: Env) -> None:
    first = build_magnet(H1, "first", ["udp://a:1/announce"])
    await env.client.post("/torrents/add", data={"urls": first, "category": "radarr"})
    await env.client.post(
        "/torrents/add", data={"urls": build_magnet(H1, "second"), "category": "radarr"}
    )
    t = await env.repo.get_task(H1)
    assert t is not None and t.magnet == first


# ---------------------------------------------------------------- T14：missingFiles


class _MissingView:
    def __init__(self, hashes: set[str]) -> None:
        self.hashes = hashes

    def might_be_missing(self, info_hash: str) -> bool:
        return info_hash in self.hashes

    def is_missing(self, info_hash: str, completion_id: int | None) -> bool:
        return info_hash in self.hashes


@asynccontextmanager
async def _missing_client(env: Env, view: _MissingView | None) -> AsyncIterator[httpx.AsyncClient]:
    app = create_qbit_app(
        repo=env.repo,
        settings=env.settings,
        secrets=Secrets(webui_password=PASSWORD),
        clock=env.clock,
        cleaner=env.cleaner,
        progress_source=env.progress,
        missing_files=view,
    )
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://t/api/v2") as client:
        r = await client.post("/auth/login", data={"username": "admin", "password": PASSWORD})
        assert r.text == "Ok."
        yield client


async def test_completed_missing_files_reports_missing_files(env: Env) -> None:
    await mk(env, H1, state=TaskState.COMPLETED)
    await mk(env, H2, state=TaskState.COMPLETED)
    await mk(env, H3, state=TaskState.FETCHING)
    async with _missing_client(env, _MissingView({H1, H3})) as client:
        r = await client.get("/torrents/info")
    by_hash = {t["hash"]: t for t in r.json()}
    assert by_hash[H1]["state"] == "missingFiles"
    assert by_hash[H1]["progress"] == 1.0  # 其余字段不变
    assert by_hash[H1]["ratio"] == 0
    assert by_hash[H2]["state"] == "stoppedUP"  # 文件在
    assert by_hash[H3]["state"] == "downloading"  # 只影响 completed


async def test_unmarked_completed_does_not_query_events(
    env: Env, monkeypatch: pytest.MonkeyPatch
) -> None:
    await mk(env, H1, state=TaskState.COMPLETED)
    await mk(env, H2, state=TaskState.COMPLETED)
    calls: list[str] = []
    real = env.repo.list_events

    async def spy(info_hash: str, limit: int = 50) -> Any:
        if limit == 1:  # completion_id 的查询（info 里另有 limit=20 的无关查询）
            calls.append(info_hash)
        return await real(info_hash, limit)

    monkeypatch.setattr(env.repo, "list_events", spy)
    async with _missing_client(env, _MissingView({H1})) as client:
        r = await client.get("/torrents/info")
    assert len(r.json()) == 2
    assert calls == [H1]  # 未被标记的 H2 不查事件


async def test_no_missing_view_keeps_stopped_up(env: Env) -> None:
    await mk(env, H1, state=TaskState.COMPLETED)
    async with _missing_client(env, None) as client:
        r = await client.get("/torrents/info")
    assert r.json()[0]["state"] == "stoppedUP"
