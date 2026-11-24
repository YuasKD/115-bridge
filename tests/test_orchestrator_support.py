"""T13 测试的公用装置（本文件自身没有测试）：FakeCloud、FakeCdn、FakeArr、FakeClock、内存仓储、
临时目录组成的完整环境，以及推进调度器的 ``drive``。"""

from __future__ import annotations

import asyncio
import contextlib
from collections.abc import AsyncIterator, Awaitable, Callable
from dataclasses import dataclass, field
from pathlib import Path

import httpx

from bridge.api.qbit import create_qbit_app
from bridge.arr.fake import FakeArr
from bridge.cloud.fake import FakeCdn, FakeCloud, FakeFile, OfflineScenario, make_content
from bridge.config import (
    CloudConfig,
    FetchConfig,
    PathsConfig,
    Secrets,
    Settings,
    VerifyConfig,
    WebUIConfig,
)
from bridge.db.repo import ManifestFile, Source, Task, TaskState
from bridge.db.sqlite import SqliteTaskRepo
from bridge.pipeline.fetch import GIB, Fetcher, FetchResult, FetchStatus
from bridge.pipeline.orchestrator import Orchestrator, Tuning
from bridge.pipeline.verify import Verifier, VerifyResult
from bridge.testing.clock import FakeClock
from cdn_sync import sync_client

H = "ab" * 20
H2 = "cd" * 20
H3 = "ef" * 20
PASSWORD = "pw"

TERMINAL = (TaskState.COMPLETED, TaskState.FAILED, TaskState.REMOVED)


def ff(path: str, size: int) -> FakeFile:
    return FakeFile(path, make_content(path, size))


def manifest_of(*files: FakeFile) -> list[ManifestFile]:
    return [ManifestFile(f.path, len(f.content)) for f in files]


@dataclass
class RecordingNotifier:
    alerts: list[tuple[str, str]] = field(default_factory=list)
    recoveries: list[tuple[str, str]] = field(default_factory=list)

    async def alert(self, kind: str, text: str) -> bool:
        self.alerts.append((kind, text))
        return True

    async def send_qr(self, png: bytes, caption: str) -> bool:
        return True

    async def recovered(self, kind: str, text: str) -> bool:
        self.recoveries.append((kind, text))
        return True

    @property
    def kinds(self) -> list[str]:
        return [k for k, _ in self.alerts]


class CdnTransport(httpx.AsyncBaseTransport):
    """包住 FakeCdn：可模拟断网（连接失败）和卡住（门）。"""

    def __init__(self, cdn: FakeCdn) -> None:
        self._inner = httpx.MockTransport(cdn.handler)
        self.down = False
        self.gate: asyncio.Event | None = None
        self.requests = 0

    async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
        self.requests += 1
        if self.down:
            raise httpx.ConnectError("network down", request=request)
        if self.gate is not None:
            await self.gate.wait()
        return await self._inner.handle_async_request(request)


class Env:
    def __init__(self, tmp_path: Path, repo: SqliteTaskRepo, quota_min: int = 20) -> None:
        self.repo = repo
        self.clock = FakeClock()
        self.cdn = FakeCdn(self.clock, chunk_size=4096, link_ttl=1e12)
        self.cloud = FakeCloud(self.clock, cdn=self.cdn)
        self.transport = CdnTransport(self.cdn)
        self.notifier = RecordingNotifier()
        self.radarr = FakeArr(name="radarr")
        self.sonarr = FakeArr(name="sonarr")
        self.root = tmp_path / "lib"
        self.incomplete = self.root / ".incomplete"
        self.free_bytes = 10_000 * GIB
        self.cdn_events: list[tuple[str, str, str]] = []  # Fetcher 回调的记录
        self.settings = Settings(
            webui=WebUIConfig(username="admin"),
            paths=PathsConfig(root=str(self.root), incomplete=str(self.incomplete)),
            categories={"radarr": "radarr", "tv-sonarr": "sonarr"},
            cloud=CloudConfig(quota_min=quota_min),
        )
        self._http = sync_client(self.transport)
        self.fetcher = Fetcher(
            repo=repo,
            cloud=self.cloud,
            http=self._http,
            clock=self.clock,
            config=FetchConfig(),
            incomplete_dir=self.incomplete,
            free_space=lambda _p: self.free_bytes,
            read_chunk_bytes=1,
            on_cdn_403=self._on_403,
            on_file_done=self._on_done,
        )
        self.verifier = Verifier(
            repo, root=self.root, incomplete=self.incomplete, cfg=VerifyConfig()
        )
        self.orch = self.make_orch()

    # Fetcher 的事件回调：记录下来，并转给当前的调度器（测试里可能换掉 self.orch）
    def _on_403(self, info_hash: str, rel_path: str) -> None:
        self.cdn_events.append(("403", info_hash, rel_path))
        self.orch.on_cdn_403(info_hash, rel_path)

    def _on_done(self, info_hash: str, rel_path: str) -> None:
        self.cdn_events.append(("done", info_hash, rel_path))
        self.orch.on_file_done(info_hash, rel_path)

    def make_orch(
        self,
        *,
        fetcher: object | None = None,
        verifier: object | None = None,
        arrs: dict[str, FakeArr] | None = None,
        tuning: Tuning | None = None,
    ) -> Orchestrator:
        return Orchestrator(
            repo=self.repo,
            cloud=self.cloud,
            arrs=arrs if arrs is not None else {"radarr": self.radarr, "tv-sonarr": self.sonarr},
            notifier=self.notifier,
            clock=self.clock,
            fetcher=fetcher if fetcher is not None else self.fetcher,  # type: ignore[arg-type]
            verifier=verifier if verifier is not None else self.verifier,  # type: ignore[arg-type]
            settings=self.settings,
            tuning=tuning,
        )

    # -- 任务 --

    async def add_task(
        self,
        scenario: OfflineScenario,
        *,
        info_hash: str = H,
        category: str = "radarr",
        name: str | None = None,
        manifest: bool | list[ManifestFile] = True,
        arr_item: int | None = 7,
    ) -> Task:
        """相当于 torrents/add 入队：脚本化 115 的场景，建 QUEUED 任务，Arr 队列里放一项。"""
        self.cloud.script(info_hash, scenario)
        task, _ = await self.repo.create_task(
            info_hash,
            category,
            name or scenario.name,
            Source.MAGNET,
            self.clock.now(),
            "magnet:?xt=urn:btih:" + info_hash,
        )
        if manifest is True:
            await self.repo.put_manifest(info_hash, manifest_of(*scenario.files))
        elif manifest:
            await self.repo.put_manifest(info_hash, manifest)
        if arr_item is not None:
            arr = self.sonarr if category == "tv-sonarr" else self.radarr
            arr.add_queue_item(info_hash, arr_item)
        return task

    async def state(self, info_hash: str = H) -> TaskState:
        task = await self.repo.get_task(info_hash)
        assert task is not None
        return task.state

    async def task(self, info_hash: str = H) -> Task:
        task = await self.repo.get_task(info_hash)
        assert task is not None
        return task

    async def settle_jobs(self) -> None:
        """等后台协程结束，或者全部卡在假时钟的 sleep 上（等时间推进）。

        取回和校验的文件操作在线程里，需要真实的短暂等待；这里不用 sleep 实现任何时间逻辑。
        """
        for _ in range(2000):
            if not self.orch.active_jobs:
                return
            gate = self.transport.gate
            if gate is not None and not gate.is_set() and self.transport.requests > 0:
                return  # 卡在 CDN 的门上，由测试自己放行或删除
            await asyncio.sleep(0.005)
            if self.orch.active_jobs and self.clock.pending_sleepers:
                await asyncio.sleep(0.02)
                if self.orch.active_jobs and self.clock.pending_sleepers:
                    return
        raise AssertionError("jobs did not settle")

    async def drive(
        self,
        until: Callable[[], Awaitable[bool]],
        *,
        rounds: int = 400,
        step: float = 61.0,
        orch: Orchestrator | None = None,
    ) -> None:
        """tick、等后台协程、推进假时钟，直到条件成立。"""
        o = orch or self.orch
        for _ in range(rounds):
            await o.tick()
            await self.settle_jobs()
            if await until():
                return
            await self.clock.advance(step)
        raise AssertionError("condition not reached")

    async def drive_to(
        self, states: TaskState | tuple[TaskState, ...], info_hash: str = H, **kw: float
    ) -> Task:
        wanted = (states,) if isinstance(states, TaskState) else states

        async def until() -> bool:
            return (await self.state(info_hash)) in wanted

        await self.drive(until, **kw)  # type: ignore[arg-type]
        return await self.task(info_hash)

    async def drive_terminal(self, info_hash: str = H, **kw: float) -> Task:
        task = await self.drive_to(TERMINAL, info_hash, **kw)
        if task.state is TaskState.FAILED:
            # 后台协程里失败的任务，Arr 回退由随后（run() 里立即被唤醒）的那一轮 tick 执行
            await self.orch.tick()
            task = await self.task(info_hash)
        return task

    # -- qBit --

    async def qbit(self) -> httpx.AsyncClient:
        app = create_qbit_app(
            repo=self.repo,
            settings=self.settings,
            secrets=Secrets(webui_password=PASSWORD),
            clock=self.clock,
            cleaner=self.orch.cleaner,
        )
        client = httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), base_url="http://t/api/v2"
        )
        r = await client.post("/auth/login", data={"username": "admin", "password": PASSWORD})
        assert r.text == "Ok."
        return client


@contextlib.asynccontextmanager
async def make_env(tmp_path: Path, *, quota_min: int = 20) -> AsyncIterator[Env]:
    repo = await SqliteTaskRepo.open(":memory:")
    e = Env(tmp_path, repo, quota_min)
    try:
        yield e
    finally:
        await e.orch.shutdown()
        e._http.close()
        await repo.close()


# ------------------------------------------------------------ 替身：用来制造真实模块不易触发的异常


class StubFetcher:
    """按脚本返回或抛出；记录调用，用来断言同一 hash 不会并发取回。"""

    def __init__(self, incomplete: Path) -> None:
        self._incomplete = incomplete
        self.calls: list[str] = []
        self.forgotten: list[str] = []
        self.active = 0
        self.peak = 0
        self.behavior: Callable[[str], Awaitable[FetchResult]] = self._done

    @staticmethod
    async def _done(_h: str) -> FetchResult:
        return FetchResult(FetchStatus.DONE, 1, 1)

    async def fetch(self, info_hash: str) -> FetchResult:
        self.calls.append(info_hash)
        self.active += 1
        self.peak = max(self.peak, self.active)
        try:
            return await self.behavior(info_hash)
        finally:
            self.active -= 1

    def forget(self, info_hash: str) -> None:
        self.forgotten.append(info_hash)

    def state_dir(self, info_hash: str) -> Path:
        return self._incomplete / ".state" / info_hash


class StubVerifier:
    def __init__(self, clock: FakeClock) -> None:
        self._clock = clock
        self.call_times: list[float] = []
        self.behavior: Callable[[str], Awaitable[VerifyResult]] | None = None

    async def verify(self, info_hash: str) -> VerifyResult:
        self.call_times.append(self._clock.monotonic())
        assert self.behavior is not None
        return await self.behavior(info_hash)


async def put_at(env: Env, info_hash: str, state: TaskState, category: str = "radarr") -> Task:
    """不经过调度器，直接把任务按合法路径推到某个状态（用于构造场景）。"""
    await env.repo.create_task(
        info_hash,
        category,
        "Show",
        Source.MAGNET,
        env.clock.now(),
        "magnet:?xt=urn:btih:" + info_hash,
    )
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
        if state is TaskState.QUEUED:
            break
        await env.repo.transition(info_hash, s, "setup", env.clock.now())
        if s is state:
            break
    return await env.task(info_hash)
