from __future__ import annotations

import asyncio
import hashlib
import json
from collections.abc import AsyncIterator, Awaitable, Callable
from pathlib import Path
from typing import Any

import httpx
import pytest

from bridge.api.qbit import ETA_INFINITE, ProgressSource
from bridge.cloud.fake import (
    CdnFault,
    FakeCdn,
    FakeCloud,
    FakeFile,
    OfflineScenario,
    make_content,
)
from bridge.cloud.protocol import RiskControl
from bridge.config import FetchConfig
from bridge.db.repo import Source, Stage, TaskFile
from bridge.db.sqlite import SqliteTaskRepo
from bridge.pipeline.fetch import (
    GIB,
    Fetcher,
    FetchFailed,
    FetchStatus,
    NoFilesToFetch,
    UnsafePath,
    safe_target,
)
from bridge.pipeline.progress import ProgressTracker
from bridge.testing.clock import FakeClock
from cdn_sync import sync_client

H = "ab" * 20
SIZE_A = 100_000
SIZE_B = 60_001


class RecClock(FakeClock):
    def __init__(self) -> None:
        super().__init__()
        self.slept: list[float] = []
        # 登记等待的那一刻同步调用（早于任何 advance 能唤醒它），用于确定性地改变测试状态
        self.on_sleep: Callable[[float], None] | None = None

    async def sleep(self, seconds: float) -> None:
        self.slept.append(seconds)
        if self.on_sleep is not None:
            self.on_sleep(seconds)
        await super().sleep(seconds)


@pytest.fixture
async def repo(tmp_path: Path) -> AsyncIterator[SqliteTaskRepo]:
    r = await SqliteTaskRepo.open(tmp_path / "t.db")
    yield r
    await r.close()


@pytest.fixture
def clock() -> RecClock:
    return RecClock()


@pytest.fixture
def cdn(clock: RecClock) -> FakeCdn:
    return FakeCdn(clock, chunk_size=4096, link_ttl=1e12)


@pytest.fixture
def cloud(clock: RecClock, cdn: FakeCdn) -> FakeCloud:
    return FakeCloud(clock, cdn=cdn)


@pytest.fixture
def incomplete(tmp_path: Path) -> Path:
    return tmp_path / "inc"


def sha1_of(p: Path) -> str:
    return hashlib.sha1(p.read_bytes()).hexdigest()


class Env:
    def __init__(
        self,
        repo: SqliteTaskRepo,
        cloud: FakeCloud,
        clock: RecClock,
        incomplete: Path,
        http_wrapper: Callable[[httpx.AsyncBaseTransport], httpx.AsyncBaseTransport] | None = None,
    ) -> None:
        self.repo = repo
        self.cloud = cloud
        self.clock = clock
        self.incomplete = incomplete
        self.http_wrapper = http_wrapper
        self.files: list[TaskFile] = []
        self.contents: dict[str, bytes] = {}

    async def setup(self, specs: list[tuple[str, int]], h: str = H) -> list[TaskFile]:
        ffs = [FakeFile(p, make_content(p, n)) for p, n in specs]
        sc = OfflineScenario(name="task", files=ffs)
        root = self.cloud.preload_tree(h, sc, "0")
        by_name: dict[str, Any] = {}

        async def walk(dir_id: str, prefix: str) -> None:
            for e in await self.cloud.list_dir(dir_id):
                if e.is_dir:
                    await walk(e.id, prefix + e.name + "/")
                else:
                    by_name[prefix + e.name] = e

        await walk(root, "")
        await self.repo.create_task(
            h, "radarr", "task", Source.MAGNET, self.clock.now(), "magnet:?xt=urn:btih:" + h
        )
        files = []
        for f in ffs:
            e = by_name[f.path]
            assert e.pickcode and e.sha1
            files.append(TaskFile(f.path, e.size, e.sha1.lower(), e.pickcode))
        if h == H:
            self.files = files
            self.contents = {f.path: f.content for f in ffs}
        await self.repo.put_task_files(h, files)
        return files

    def fetcher(
        self,
        *,
        free: int = 10_000 * GIB,
        max_files: int = 1,
        conns: int = 2,
        max_retries: int = 5,
        checkpoint: int = 8192,
        write_block: int = 4096,
    ) -> Fetcher:
        transport: httpx.AsyncBaseTransport = self.cloud.cdn.transport
        if self.http_wrapper is not None:
            transport = self.http_wrapper(transport)
        return Fetcher(
            repo=self.repo,
            cloud=self.cloud,
            http=sync_client(transport),
            clock=self.clock,
            config=FetchConfig(
                connections_per_file=conns, max_files=max_files, max_retries=max_retries
            ),
            incomplete_dir=self.incomplete,
            free_space=lambda _p: free,
            checkpoint_bytes=checkpoint,
            write_block_bytes=write_block,
            read_chunk_bytes=1,  # 读线程不合并块：保持旧测试按网络块推进的节奏
        )

    def path(self, rel: str) -> Path:
        return self.incomplete / H / rel

    def assert_content(self) -> None:
        for rel, data in self.contents.items():
            assert self.path(rel).read_bytes() == data


async def wait_until(pred: Callable[[], bool], limit: int = 20_000) -> None:
    """等一个条件成立。数据库在线程里跑，所以这里让出事件循环并短暂等待真实线程。"""
    for _ in range(limit):
        if pred():
            return
        await asyncio.sleep(0.001)
    raise AssertionError("condition not reached")


async def drive(clock: FakeClock, coro: Awaitable[Any]) -> Any:
    t = asyncio.ensure_future(coro)
    for _ in range(100_000):
        if t.done():
            break
        await clock.advance(4000)
    return await t


@pytest.fixture
def env(repo: SqliteTaskRepo, cloud: FakeCloud, clock: RecClock, incomplete: Path) -> Env:
    return Env(repo, cloud, clock, incomplete)


async def fetch_retries(repo: SqliteTaskRepo) -> int:
    t = await repo.get_task(H)
    assert t is not None
    return t.retries.get(Stage.FETCH, 0)


# ---------------------------------------------------------------- 基本


async def test_two_connections_fresh_link_and_ua(env: Env, cloud: FakeCloud, cdn: FakeCdn) -> None:
    await env.setup([("a.mkv", SIZE_A), ("Season 1/b.mkv", SIZE_B)])
    f = env.fetcher()
    res = await drive(env.clock, f.fetch(H))
    assert res.status is FetchStatus.DONE
    env.assert_content()
    for tf in env.files:
        assert sha1_of(env.path(tf.rel_path)) == tf.sha1
    # 每个请求都取了新链接，带链接的 UA
    assert len(cdn.requests) == cloud.call_count("download_link") == 4
    assert len({r.token for r in cdn.requests}) == 4
    assert all(r.status == 206 for r in cdn.requests)
    assert all(r.user_agent and r.user_agent.startswith("FakeCloud-UA/") for r in cdn.requests)
    # 每个文件 2 个分段，且从未触发第 3 连接 403
    ranges = sorted(r.range or "" for r in cdn.requests if r.pickcode == env.files[0].pickcode)
    assert ranges == ["bytes=0-49999", "bytes=50000-99999"]
    # 进度写库
    t = await env.repo.get_task(H)
    assert t is not None and t.local_bytes == SIZE_A + SIZE_B
    assert {x.rel_path: x.local_bytes for x in await env.repo.list_task_files(H)} == {
        "a.mkv": SIZE_A,
        "Season 1/b.mkv": SIZE_B,
    }
    assert await fetch_retries(env.repo) == 0
    assert not (env.incomplete / ".state" / H).exists()


async def test_never_more_than_two_connections(env: Env, cdn: FakeCdn) -> None:
    await env.setup([("a.mkv", SIZE_A)])
    peak = 0

    class Spy(httpx.AsyncBaseTransport):
        def __init__(self, inner: httpx.AsyncBaseTransport) -> None:
            self.inner = inner

        async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
            nonlocal peak
            resp = await self.inner.handle_async_request(request)
            peak = max(peak, cdn.active_connections(env.files[0].pickcode))
            return resp

    env.http_wrapper = Spy
    await env.fetcher(max_files=3).fetch(H)
    assert peak == 2
    assert all(r.status == 206 for r in cdn.requests)  # 没有出现第 3 个连接的 403
    env.assert_content()


@pytest.mark.parametrize(("max_files", "peak"), [(1, 1), (2, 2), (3, 3)])
async def test_max_files_limit(env: Env, max_files: int, peak: int) -> None:
    await env.setup([(f"f{i}.bin", 40_000) for i in range(3)])
    f = env.fetcher(max_files=max_files)
    await drive(env.clock, f.fetch(H))
    assert f.peak_active_files == peak
    env.assert_content()


async def test_zero_size_file_and_nested(env: Env) -> None:
    await env.setup([("d/e/empty.txt", 0), ("d/x.bin", 10)])
    await drive(env.clock, env.fetcher().fetch(H))
    assert env.path("d/e/empty.txt").read_bytes() == b""
    env.assert_content()


async def test_progress_source_protocol(env: Env) -> None:
    f = env.fetcher()
    assert isinstance(f.progress_source, ProgressSource)


# ---------------------------------------------------------------- 严格校验


class SizeWatch(httpx.AsyncBaseTransport):
    """每次响应后检查文件大小从不超过预期。"""

    def __init__(self, inner: httpx.AsyncBaseTransport, env: Env) -> None:
        self.inner = inner
        self.env = env
        self.max_seen = 0

    async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
        resp = await self.inner.handle_async_request(request)
        for tf in self.env.files:
            p = self.env.path(tf.rel_path)
            if p.exists():
                assert p.stat().st_size <= tf.size
                self.max_seen = max(self.max_seen, p.stat().st_size)
        return resp


@pytest.mark.parametrize(
    "fault",
    [
        CdnFault("full_200"),
        CdnFault("bad_range", delta=1),
        CdnFault("bad_range", delta=-1),
        CdnFault("bad_length", delta=1),
        CdnFault("bad_length", delta=-5),
        CdnFault("status", status=500),
    ],
    ids=["200", "range+1", "range-1", "len+1", "len-5", "500"],
)
async def test_bad_response_discarded(env: Env, cdn: FakeCdn, fault: CdnFault) -> None:
    await env.setup([("a.mkv", SIZE_A)])
    cdn.queue_fault(env.files[0].pickcode, fault)
    env.http_wrapper = lambda inner: SizeWatch(inner, env)
    # 单连接：双连接时另一条连接被取消前缓冲的数据会先写盘，算作进展（取决于时机）
    await drive(env.clock, env.fetcher(conns=1).fetch(H))
    env.assert_content()
    assert await fetch_retries(env.repo) == 1
    assert env.clock.slept == [30.0]
    assert env.path("a.mkv").stat().st_size == SIZE_A


async def test_stream_cut_resumes_from_break(env: Env, cdn: FakeCdn) -> None:
    await env.setup([("a.mkv", SIZE_A)])
    cdn.queue_fault(env.files[0].pickcode, CdnFault("cut", after_bytes=12_000))
    await drive(env.clock, env.fetcher(conns=1).fetch(H))
    env.assert_content()
    assert await fetch_retries(env.repo) == 0  # 断流前写入过数据：有进展，不计重试
    assert env.clock.slept == [5.0]
    starts = [int((r.range or "")[6:].split("-")[0]) for r in cdn.requests if r.status == 206]
    assert starts[0] == 0
    assert starts[1] == 12_000  # 从断点续，不重下已写的部分
    # 续传的请求也取了新链接
    assert len({r.token for r in cdn.requests}) == len(cdn.requests)


# ---------------------------------------------------------------- 重启续传


async def test_restart_resumes_from_progress_record(
    env: Env, repo: SqliteTaskRepo, cdn: FakeCdn
) -> None:
    await env.setup([("a.mkv", SIZE_A)])
    pc = env.files[0].pickcode
    cdn.queue_fault(pc, CdnFault("cut", after_bytes=20_000))
    first = env.fetcher(conns=1, checkpoint=4096)
    task = asyncio.ensure_future(first.fetch(H))
    # 断流后进入 5 秒退避（有进展），此时「进程被杀」
    await wait_until(lambda: env.clock.pending_sleepers > 0)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    part = env.path("a.mkv")
    assert 0 < part.stat().st_size < SIZE_A
    state = json.loads(next((env.incomplete / ".state" / H).glob("*.json")).read_text())
    assert state["size"] == SIZE_A
    written = sum(s[2] for s in state["segments"])
    assert written == 20_000
    tf = (await repo.list_task_files(H))[0]
    assert tf.local_bytes == written

    # 「重启」：全新实例，新 HTTP 客户端，重新开始计请求
    cdn.requests.clear()
    second = env.fetcher(max_retries=5)
    await drive(env.clock, second.fetch(H))
    env.assert_content()
    assert sha1_of(part) == env.files[0].sha1
    downloaded = sum(
        int(r.range.split("=")[1].split("-")[1]) - int(r.range.split("=")[1].split("-")[0]) + 1
        for r in cdn.requests
        if r.range
    )
    assert downloaded == SIZE_A - written  # 只下了缺的部分
    assert not (env.incomplete / ".state" / H).exists()


async def test_record_ahead_of_data_is_discarded(env: Env, repo: SqliteTaskRepo) -> None:
    """记录说已写但数据不在（不应发生）：整文件重下，结果仍然正确。"""
    await env.setup([("a.mkv", SIZE_A)])
    f = env.fetcher()
    sd = env.incomplete / ".state" / H
    sd.mkdir(parents=True)
    key = hashlib.sha1(b"a.mkv").hexdigest()
    (sd / f"{key}.json").write_text(
        json.dumps({"v": 1, "size": SIZE_A, "segments": [[0, 50000, 50000], [50000, SIZE_A, 0]]})
    )
    await drive(env.clock, f.fetch(H))
    env.assert_content()


async def test_completed_file_not_redownloaded(env: Env, cdn: FakeCdn) -> None:
    await env.setup([("a.mkv", SIZE_A), ("b.mkv", 5000)])
    await drive(env.clock, env.fetcher().fetch(H))
    cdn.requests.clear()
    res = await drive(env.clock, env.fetcher().fetch(H))
    assert res.status is FetchStatus.DONE
    assert cdn.requests == []


async def test_verified_files_skipped(env: Env, cdn: FakeCdn) -> None:
    await env.setup([("a.mkv", SIZE_A), ("b.mkv", 5000)])
    await env.repo.update_task_file(H, "a.mkv", verified=True)
    await drive(env.clock, env.fetcher().fetch(H))
    assert {r.pickcode for r in cdn.requests} == {env.files[1].pickcode}


# ---------------------------------------------------------------- 403


async def test_403_drops_to_one_connection_without_retry(
    env: Env, cdn: FakeCdn, cloud: FakeCloud
) -> None:
    cdn.max_connections = 1
    await env.setup([("a.mkv", SIZE_A)])
    await drive(env.clock, env.fetcher().fetch(H))
    env.assert_content()
    assert any(r.status == 403 for r in cdn.requests)
    assert await fetch_retries(env.repo) == 0
    assert env.clock.slept == []
    # 403 之后仍是每次请求先重取链接
    assert len({r.token for r in cdn.requests}) == len(cdn.requests)
    assert cloud.call_count("download_link") == len(cdn.requests)
    # 降级后不再并发：最后的 206 请求之间没有 403
    last403 = max(i for i, r in enumerate(cdn.requests) if r.status == 403)
    assert all(r.status == 206 for r in cdn.requests[last403 + 1 :])


async def test_persistent_403_counts_as_fetch_failure(
    env: Env, cdn: FakeCdn, cloud: FakeCloud
) -> None:
    cdn.max_connections = 0
    await env.setup([("a.mkv", SIZE_A)])
    with pytest.raises(FetchFailed):
        await drive(env.clock, env.fetcher().fetch(H))
    assert await fetch_retries(env.repo) == 6
    assert cloud.call_count("download_link") >= 6  # API 正常（链接取得成功），按下载侧失败计数


async def test_403_then_risk_control_propagates_without_retry(
    env: Env, cdn: FakeCdn, cloud: FakeCloud
) -> None:
    cdn.max_connections = 1
    await env.setup([("a.mkv", SIZE_A)])
    cloud.inject("download_link", RiskControl("boom"), on_call=3, times=None)
    with pytest.raises(RiskControl):
        await drive(env.clock, env.fetcher().fetch(H))
    assert await fetch_retries(env.repo) == 0
    assert env.clock.slept == []


async def test_global_error_from_download_link_not_counted(env: Env, cloud: FakeCloud) -> None:
    await env.setup([("a.mkv", SIZE_A)])
    cloud.inject("download_link", RiskControl("boom"), on_call=1, times=None)
    with pytest.raises(RiskControl):
        await drive(env.clock, env.fetcher().fetch(H))
    assert await fetch_retries(env.repo) == 0
    t = await env.repo.get_task(H)
    assert t is not None  # 不改任务状态由调度器负责；此处也未被动过
    assert t.state.value == "queued"


# ---------------------------------------------------------------- 空间


async def test_waiting_for_space(env: Env, cdn: FakeCdn) -> None:
    await env.setup([("a.mkv", SIZE_A)])
    f = env.fetcher(free=10 * GIB)
    res = await f.fetch(H)
    assert res.status is FetchStatus.WAITING_FOR_SPACE
    assert f.progress_source.progress(H).waiting_for_space is True
    assert cdn.requests == []
    assert not env.path("a.mkv").exists()
    assert await fetch_retries(env.repo) == 0

    # 空间恢复：换一个新实例（模拟重启或下一轮调度）再调用即继续
    free = {"v": 10_000 * GIB}
    f2 = env.fetcher()
    f2._free_space = lambda _p: free["v"]
    f2.tracker.set_waiting(H, True)
    res = await drive(env.clock, f2.fetch(H))
    assert res.status is FetchStatus.DONE
    assert f2.progress_source.progress(H).waiting_for_space is False


async def test_space_threshold_accounts_for_remaining_bytes(env: Env) -> None:
    await env.setup([("a.mkv", SIZE_A)])
    margin = 50 * GIB
    below = env.fetcher(free=margin + SIZE_A - 1)
    assert (await below.fetch(H)).status is FetchStatus.WAITING_FOR_SPACE
    exact = env.fetcher(free=margin + SIZE_A)
    assert (await drive(env.clock, exact.fetch(H))).status is FetchStatus.DONE


# ---------------------------------------------------------------- 重试


async def test_retry_budget_and_backoff(env: Env, cdn: FakeCdn) -> None:
    await env.setup([("a.mkv", SIZE_A)])
    cdn.queue_fault(env.files[0].pickcode, *[CdnFault("status", status=500)] * 40)
    f = env.fetcher(conns=1)
    with pytest.raises(FetchFailed) as ei:
        await drive(env.clock, f.fetch(H))
    assert ei.value.retries == 6
    assert env.clock.slept == [30, 60, 120, 240, 480]
    assert await fetch_retries(env.repo) == 6
    # 再次调用：预算已用尽，直接失败，计数不变（只增不减）
    with pytest.raises(FetchFailed):
        await drive(env.clock, env.fetcher().fetch(H))
    assert await fetch_retries(env.repo) == 6


async def test_backoff_capped_at_one_hour(env: Env, cdn: FakeCdn) -> None:
    await env.setup([("a.mkv", SIZE_A)])
    cdn.queue_fault(env.files[0].pickcode, *[CdnFault("status", status=500)] * 40)
    with pytest.raises(FetchFailed):
        await drive(env.clock, env.fetcher(conns=1, max_retries=9).fetch(H))
    assert env.clock.slept[-1] == 3600
    assert max(env.clock.slept) == 3600


# ---------------------------------------------------------------- 路径


@pytest.mark.parametrize(
    "rel",
    ["../x", "a/../../x", "/etc/passwd", "a//b", "", ".", "a/./b", "C:/x", "a\\b", "x\x00y", "a/"],
)
async def test_unsafe_paths_rejected(env: Env, cdn: FakeCdn, rel: str) -> None:
    await env.setup([("a.mkv", 100)])
    await env.repo.put_task_files(H, [TaskFile(rel, 10, "0" * 40, env.files[0].pickcode)])
    with pytest.raises(UnsafePath):
        await env.fetcher().fetch(H)
    assert cdn.requests == []
    assert await fetch_retries(env.repo) == 0


def test_safe_target_stays_inside(tmp_path: Path) -> None:
    p = safe_target(tmp_path, H, "Season 1/a.mkv")
    assert p == (tmp_path / H / "Season 1" / "a.mkv").resolve()
    with pytest.raises(UnsafePath):
        safe_target(tmp_path, "../evil", "a")


async def test_no_files(env: Env) -> None:
    await env.setup([("a.mkv", 100)])
    await env.repo.put_task_files(H, [])
    with pytest.raises(NoFilesToFetch):
        await env.fetcher().fetch(H)


# ---------------------------------------------------------------- 速度与 ETA


def test_speed_and_eta_sliding_window() -> None:
    clock = FakeClock()
    tr = ProgressTracker(clock, window_seconds=10)
    assert tr.progress(H).dlspeed == 0 and tr.progress(H).eta == ETA_INFINITE
    tr.begin(H, 10_000)
    tr.record(H, 1000)
    # 刚开始：分母至少 1 秒
    assert tr.progress(H).dlspeed == 1000
    clock._mono += 4  # type: ignore[attr-defined]
    tr.record(H, 1000)
    p = tr.progress(H)
    assert p.dlspeed == 500  # 2000 字节 / 4 秒
    assert p.eta == 16  # 剩 8000 / 500
    clock._mono += 20  # type: ignore[attr-defined]
    p = tr.progress(H)  # 窗口内无样本
    assert p.dlspeed == 0 and p.eta == ETA_INFINITE
    tr.record(H, 8000)
    assert tr.progress(H).eta == 0
    assert tr.progress(H).waiting_for_space is False
    tr.set_waiting(H, True)
    assert tr.progress(H).waiting_for_space is True
    tr.clear(H)
    assert tr.progress(H).dlspeed == 0


async def test_speed_reported_during_fetch(env: Env) -> None:
    await env.setup([("a.mkv", SIZE_A)])
    f = env.fetcher()
    await f.fetch(H)
    p = f.progress_source.progress(H)
    assert p.dlspeed == SIZE_A  # 时钟未推进，分母取 1 秒下限
    assert p.eta == 0
