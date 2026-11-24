# ruff: noqa: F811
"""T11 返工补充测试：重试计数规则、空间预留、ENOSPC、本地异常、取消、恢复路径、进度内存上界。

夹具和辅助类复用 ``test_fetch.py``。
"""

from __future__ import annotations

import asyncio
import errno
import hashlib
import json
import sqlite3
from collections.abc import AsyncIterator, Callable
from pathlib import Path
from typing import Any

import httpx
import pytest

import bridge.pipeline.fetch as fetch_mod
from bridge.cloud.fake import CdnFault, FakeCdn, FakeCloud
from bridge.cloud.protocol import CloudNotFound, RiskControl
from bridge.db.repo import TaskUpdate
from bridge.db.sqlite import SqliteTaskRepo
from bridge.pipeline.fetch import (
    GIB,
    Fetcher,
    FetchFailed,
    FetchStatus,
    _load_segments,
)
from bridge.pipeline.progress import ProgressTracker
from bridge.testing.clock import FakeClock
from cdn_sync import join_readers
from test_fetch import (  # noqa: F401  (夹具)
    SIZE_A,
    SIZE_B,
    Env,
    H,
    cdn,
    clock,
    cloud,
    drive,
    env,
    fetch_retries,
    incomplete,
    repo,
    wait_until,
)

H2 = "cd" * 20


# ---------------------------------------------------------------- 进度内存上界


def test_tracker_memory_bounded_without_polling() -> None:
    """Arr 长时间不轮询（只 record、不 progress）时，样本数有固定上界。"""
    clock_ = FakeClock()
    tr = ProgressTracker(clock_, window_seconds=10)
    tr.begin(H, 10**12)
    for _ in range(200_000):  # 200 秒、每毫秒一块
        clock_._mono += 0.001  # type: ignore[attr-defined]
        tr.record(H, 4096)
        assert len(tr._tracks[H].buckets) <= 12  # type: ignore[attr-defined]
    p = tr.progress(H)
    assert 0.85 * 4096 * 1000 <= p.dlspeed <= 1.15 * 4096 * 1000


def test_tracker_same_second_merged() -> None:
    tr = ProgressTracker(FakeClock(), window_seconds=10)
    tr.begin(H, 10**9)
    for _ in range(100_000):
        tr.record(H, 10)
    assert len(tr._tracks[H].buckets) == 1  # type: ignore[attr-defined]
    assert tr.progress(H).dlspeed == 1_000_000


# ---------------------------------------------------------------- 测试用传输层


class _WrapStream(httpx.AsyncByteStream):
    def __init__(self, inner: httpx.AsyncByteStream) -> None:
        self.inner = inner

    async def __aiter__(self) -> AsyncIterator[bytes]:
        async for c in self.inner:
            yield c

    async def aclose(self) -> None:
        await self.inner.aclose()


class ExtraStream(_WrapStream):
    """正文比声明的长：把多余字节拼到最后一块。"""

    def __init__(self, inner: httpx.AsyncByteStream, extra: bytes) -> None:
        super().__init__(inner)
        self.extra = extra

    async def __aiter__(self) -> AsyncIterator[bytes]:
        prev: bytes | None = None
        async for c in self.inner:
            if prev is not None:
                yield prev
            prev = c
        if prev is not None:
            yield prev + self.extra


class StallStream(_WrapStream):
    """发出 after 字节后永远挂起，用来在下载中途取消。"""

    def __init__(
        self,
        inner: httpx.AsyncByteStream,
        after: int,
        stalled: asyncio.Event,
        release: asyncio.Event | None = None,
    ) -> None:
        super().__init__(inner)
        self.after = after
        self.stalled = stalled
        # 读线程卡在这里时，事件循环一侧无法替它关闭连接（真实场景下由读超时兜底）；
        # 测试可以设置 release 来模拟「读返回了」，线程随即发现停止标志并退出
        self.release = release or asyncio.Event()

    async def __aiter__(self) -> AsyncIterator[bytes]:
        sent = 0
        async for c in self.inner:
            yield c
            sent += len(c)
            if sent >= self.after:
                self.stalled.set()
                await self.release.wait()


class TickStream(_WrapStream):
    """每发一块就把假时钟推进若干秒。"""

    def __init__(self, inner: httpx.AsyncByteStream, clock_: FakeClock, step: float) -> None:
        super().__init__(inner)
        self.clock = clock_
        self.step = step

    async def __aiter__(self) -> AsyncIterator[bytes]:
        async for c in self.inner:
            self.clock._mono += self.step  # type: ignore[attr-defined]
            yield c


class StreamHook(httpx.AsyncBaseTransport):
    def __init__(
        self,
        inner: httpx.AsyncBaseTransport,
        make: Callable[[httpx.AsyncByteStream], httpx.AsyncByteStream | None],
    ) -> None:
        self.inner = inner
        self.make = make

    async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
        resp = await self.inner.handle_async_request(request)
        assert isinstance(resp.stream, httpx.AsyncByteStream)
        new = self.make(resp.stream)
        if new is None:
            return resp
        return httpx.Response(resp.status_code, headers=resp.headers, stream=new)


class GateTransport(httpx.AsyncBaseTransport):
    """第一个请求在放行前挂起，并通知测试「已到达」。"""

    def __init__(
        self, inner: httpx.AsyncBaseTransport, gate: asyncio.Event, reached: asyncio.Event
    ) -> None:
        self.inner = inner
        self.gate = gate
        self.reached = reached
        self.first = True

    async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
        if self.first:
            self.first = False
            self.reached.set()
            await self.gate.wait()
        return await self.inner.handle_async_request(request)


def assert_idle(f: Fetcher, max_files: int = 1) -> None:
    """名额、活动计数、空间预留全部释放。"""
    assert f._active_files == 0  # type: ignore[attr-defined]
    assert f._file_slots._value == max_files  # type: ignore[attr-defined]
    assert f._remaining == {}  # type: ignore[attr-defined]


def downloaded(cdn_: FakeCdn) -> int:
    total = 0
    for r in cdn_.requests:
        if r.range and r.status == 206:
            a, b = r.range.split("=")[1].split("-")
            total += int(b) - int(a) + 1
    return total


# ---------------------------------------------------------------- 重试计数规则


async def test_progress_failures_not_counted_and_complete(env: Env, cdn: FakeCdn) -> None:
    """反复断流但每次都有进展：不计重试，固定 5 秒退避，最终完成。"""
    await env.setup([("a.mkv", SIZE_A)])
    cdn.queue_fault(env.files[0].pickcode, *[CdnFault("cut", after_bytes=10_000)] * 8)
    res = await drive(env.clock, env.fetcher(conns=1).fetch(H))
    assert res.status is FetchStatus.DONE
    env.assert_content()
    assert await fetch_retries(env.repo) == 0
    assert env.clock.slept == [5.0] * 8


async def test_six_consecutive_no_progress_failures_fail(env: Env, cdn: FakeCdn) -> None:
    await env.setup([("a.mkv", SIZE_A)])
    cdn.queue_fault(env.files[0].pickcode, *[CdnFault("cut", after_bytes=0)] * 40)
    f = env.fetcher(conns=1)
    with pytest.raises(FetchFailed) as ei:
        await drive(env.clock, f.fetch(H))
    assert ei.value.retries == 6
    assert env.clock.slept == [30, 60, 120, 240, 480]
    assert await fetch_retries(env.repo) == 6
    assert_idle(f)


async def test_mixed_progress_and_no_progress(env: Env, cdn: FakeCdn) -> None:
    await env.setup([("a.mkv", SIZE_A)])
    s500 = CdnFault("status", status=500)
    cut = CdnFault("cut", after_bytes=10_000)
    cdn.queue_fault(env.files[0].pickcode, s500, cut, s500, cut, cut)
    await drive(env.clock, env.fetcher(conns=1).fetch(H))
    env.assert_content()
    assert await fetch_retries(env.repo) == 2  # 只数没进展的，且只增不减
    assert env.clock.slept == [30, 5, 60, 5, 5]


# ---------------------------------------------------------------- 云端文件不存在


async def test_cloud_not_found_propagates_without_retry(env: Env, cloud: FakeCloud) -> None:
    await env.setup([("a.mkv", SIZE_A)])
    cloud.inject("download_link", CloudNotFound("gone"), on_call=1, times=None)
    f = env.fetcher(max_files=2)
    with pytest.raises(CloudNotFound):
        await drive(env.clock, f.fetch(H))
    assert await fetch_retries(env.repo) == 0
    assert env.clock.slept == []
    assert_idle(f, 2)  # 异常后名额释放


# ---------------------------------------------------------------- 空间预留与 ENOSPC


async def test_space_reserved_for_other_active_tasks(env: Env, cdn: FakeCdn) -> None:
    """两个任务同时开始，空间只够一个：第二个进入等待，第一个完成后可继续。"""
    await env.setup([("a.mkv", SIZE_A)])
    files2 = await env.setup([("x.bin", 40_000)], h=H2)
    gate, reached = asyncio.Event(), asyncio.Event()
    env.http_wrapper = lambda inner: GateTransport(inner, gate, reached)
    f = env.fetcher(free=50 * GIB + SIZE_A + 10_000)
    t1 = asyncio.ensure_future(f.fetch(H))
    await reached.wait()  # 任务 1 已登记预留，卡在第一个请求
    res2 = await f.fetch(H2)
    assert res2.status is FetchStatus.WAITING_FOR_SPACE
    assert f.progress_source.progress(H2).waiting_for_space is True
    assert not any(r.pickcode == files2[0].pickcode for r in cdn.requests)
    assert await fetch_retries(env.repo) == 0
    gate.set()
    assert (await drive(env.clock, t1)).status is FetchStatus.DONE
    assert (await drive(env.clock, f.fetch(H2))).status is FetchStatus.DONE  # 预留已释放
    assert_idle(f)


def fail_writes(monkeypatch: pytest.MonkeyPatch, after: int, err: int) -> dict[str, Any]:
    real = fetch_mod._write_chunk
    st: dict[str, Any] = {"n": 0, "on": True}

    def fake(fs: Any, offset: int, data: bytes) -> None:
        st["n"] += 1
        if st["on"] and st["n"] > after:
            raise OSError(err, "injected")
        real(fs, offset, data)

    monkeypatch.setattr(fetch_mod, "_write_chunk", fake)
    return st


async def test_enospc_mid_download_saves_checkpoint_and_waits(
    env: Env, cdn: FakeCdn, monkeypatch: pytest.MonkeyPatch
) -> None:
    await env.setup([("a.mkv", SIZE_A)])
    st = fail_writes(monkeypatch, after=5, err=errno.ENOSPC)  # 写了 5 块（20480 字节）后写满
    f = env.fetcher(conns=1)
    res = await f.fetch(H)  # 不会抛，也没有退避
    assert res.status is FetchStatus.WAITING_FOR_SPACE
    assert res.downloaded_bytes == 20_480 and res.needed_bytes == SIZE_A - 20_480
    assert f.progress_source.progress(H).waiting_for_space is True
    assert await fetch_retries(env.repo) == 0
    assert env.clock.slept == []
    sd = env.incomplete / ".state" / H
    assert list(sd.glob("*.tmp")) == []
    segs = _load_segments(next(sd.glob("*.json")), SIZE_A)
    assert segs is not None and sum(s.done for s in segs) == 20_480
    assert (await env.repo.list_task_files(H))[0].local_bytes == 20_480
    assert_idle(f)

    # 空间恢复后续传，从检查点位置继续
    st["on"] = False
    cdn.requests.clear()
    assert (await drive(env.clock, f.fetch(H))).status is FetchStatus.DONE
    env.assert_content()
    assert cdn.requests[0].range == f"bytes=20480-{SIZE_A - 1}"
    assert f.progress_source.progress(H).waiting_for_space is False


async def test_enospc_during_checkpoint_fails_safely(
    env: Env, monkeypatch: pytest.MonkeyPatch
) -> None:
    """检查点本身也写满：不抛、不留临时文件；记录只会落后不会超前，之后仍能正确完成。"""
    await env.setup([("a.mkv", SIZE_A)])

    def boom(src: Any, dst: Any) -> None:
        raise OSError(errno.ENOSPC, "No space left on device")

    f = env.fetcher(conns=1)
    with monkeypatch.context() as m:
        m.setattr(fetch_mod.os, "replace", boom)
        res = await f.fetch(H)
    assert res.status is FetchStatus.WAITING_FOR_SPACE
    assert await fetch_retries(env.repo) == 0
    sd = env.incomplete / ".state" / H
    assert list(sd.glob("*.tmp")) == []
    assert list(sd.glob("*.json")) == []
    assert (await drive(env.clock, f.fetch(H))).status is FetchStatus.DONE
    env.assert_content()


async def test_windows_disk_full_winerror_is_enospc(
    env: Env, monkeypatch: pytest.MonkeyPatch
) -> None:
    await env.setup([("a.mkv", SIZE_A)])
    real = fetch_mod._write_chunk
    calls = {"n": 0}

    def fake(fs: Any, offset: int, data: bytes) -> None:
        calls["n"] += 1
        if calls["n"] == 2:
            e = OSError(errno.EINVAL, "disk full")
            e.winerror = 112  # type: ignore[attr-defined]
            raise e
        real(fs, offset, data)

    monkeypatch.setattr(fetch_mod, "_write_chunk", fake)
    res = await env.fetcher(conns=1).fetch(H)
    assert res.status is FetchStatus.WAITING_FOR_SPACE


# ---------------------------------------------------------------- 本地 IO 与数据库异常


def heal_on_backoff(env: Env, st: dict[str, Any]) -> None:
    """进入退避（clock.sleep 被调用）的那一刻同步关掉注入错误，不依赖线程调度。"""
    env.clock.on_sleep = lambda _s: st.__setitem__("on", False)


async def test_local_oserror_without_progress_counts_once(
    env: Env, monkeypatch: pytest.MonkeyPatch
) -> None:
    await env.setup([("a.mkv", SIZE_A)])
    st = fail_writes(monkeypatch, after=0, err=errno.EIO)
    heal_on_backoff(env, st)
    await drive(env.clock, env.fetcher(conns=1).fetch(H))
    env.assert_content()
    assert await fetch_retries(env.repo) == 1
    assert env.clock.slept == [30.0]


async def test_local_oserror_with_progress_not_counted(
    env: Env, monkeypatch: pytest.MonkeyPatch
) -> None:
    await env.setup([("a.mkv", SIZE_A)])
    st = fail_writes(monkeypatch, after=4, err=errno.EIO)
    heal_on_backoff(env, st)
    await drive(env.clock, env.fetcher(conns=1).fetch(H))
    env.assert_content()
    assert await fetch_retries(env.repo) == 0
    assert env.clock.slept == [5.0]


async def test_persistent_local_oserror_ends_in_fetch_failed(
    env: Env, monkeypatch: pytest.MonkeyPatch
) -> None:
    await env.setup([("a.mkv", SIZE_A)])
    fail_writes(monkeypatch, after=0, err=errno.EIO)
    f = env.fetcher(conns=1)
    with pytest.raises(FetchFailed):  # 不是原始 OSError
        await drive(env.clock, f.fetch(H))
    assert await fetch_retries(env.repo) == 6
    assert_idle(f)


async def test_db_error_during_checkpoint_is_attempt_failure(
    env: Env, repo: SqliteTaskRepo, monkeypatch: pytest.MonkeyPatch
) -> None:
    await env.setup([("a.mkv", SIZE_A)])
    real = repo.update_task_file
    calls = {"n": 0}

    async def flaky(*a: Any, **kw: Any) -> None:
        calls["n"] += 1
        if calls["n"] == 2:
            raise sqlite3.OperationalError("database is locked")
        await real(*a, **kw)

    monkeypatch.setattr(repo, "update_task_file", flaky)
    await drive(env.clock, env.fetcher(conns=1).fetch(H))  # 不抛 sqlite3 异常
    env.assert_content()
    assert await fetch_retries(env.repo) == 0  # 失败前已有进展
    assert env.clock.slept == [5.0]


async def test_mkdir_error_is_attempt_failure(env: Env, monkeypatch: pytest.MonkeyPatch) -> None:
    await env.setup([("a.mkv", SIZE_A)])
    real = fetch_mod._prepare_file
    calls = {"n": 0}

    def flaky(target: Path, size: int) -> None:
        calls["n"] += 1
        if calls["n"] == 1:
            raise PermissionError(errno.EACCES, "denied")
        real(target, size)

    monkeypatch.setattr(fetch_mod, "_prepare_file", flaky)
    await drive(env.clock, env.fetcher(conns=1).fetch(H))
    env.assert_content()
    assert await fetch_retries(env.repo) == 1
    assert env.clock.slept == [30.0]


async def test_external_truncation_before_final_check_counts_failure(
    env: Env, monkeypatch: pytest.MonkeyPatch
) -> None:
    """完成时的大小检查在重试循环内：不符时计一次失败、整文件重下，不抛私有异常。"""
    await env.setup([("a.mkv", SIZE_A)])
    real = fetch_mod._final_size
    calls = {"n": 0}

    def truncating(target: Path) -> int:
        calls["n"] += 1
        if calls["n"] == 1:
            with open(target, "r+b") as fh:
                fh.truncate(SIZE_A - 1)
        return real(target)

    monkeypatch.setattr(fetch_mod, "_final_size", truncating)
    await drive(env.clock, env.fetcher(conns=1).fetch(H))
    env.assert_content()
    assert await fetch_retries(env.repo) == 1  # 已写的数据作废，不算进展
    assert env.clock.slept == [30.0]


async def test_unexpected_exception_not_leaked(env: Env, monkeypatch: pytest.MonkeyPatch) -> None:
    await env.setup([("a.mkv", SIZE_A)])

    def bad(fs: Any, snapshot: Any) -> None:
        raise ValueError("I/O operation on closed file")

    monkeypatch.setattr(fetch_mod, "_persist", bad)
    # 每轮都写入过数据（有进展），不计重试；没有 ValueError 逃出
    res = await drive(env.clock, env.fetcher(conns=1).fetch(H))
    assert res.status is FetchStatus.DONE
    env.assert_content()
    assert await fetch_retries(env.repo) == 0


# ---------------------------------------------------------------- CDN 正文过长


async def test_cdn_body_longer_than_declared_is_truncated(env: Env) -> None:
    await env.setup([("a.mkv", SIZE_A)])
    once = {"done": False}

    def make(inner: httpx.AsyncByteStream) -> httpx.AsyncByteStream | None:
        if once["done"]:
            return None
        once["done"] = True
        return ExtraStream(inner, b"X" * 777)

    sizes: list[int] = []

    class Spy(httpx.AsyncBaseTransport):
        def __init__(self, inner: httpx.AsyncBaseTransport) -> None:
            self.inner = inner

        async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
            p = env.path("a.mkv")
            if p.exists():
                sizes.append(p.stat().st_size)
            return await self.inner.handle_async_request(request)

    env.http_wrapper = lambda inner: Spy(StreamHook(inner, make))
    await drive(env.clock, env.fetcher(conns=1).fetch(H))
    env.assert_content()
    assert env.path("a.mkv").stat().st_size == SIZE_A  # 一个字节也没多写
    assert max(sizes, default=0) <= SIZE_A
    assert await fetch_retries(env.repo) == 0  # 数据都在，有进展
    assert env.clock.slept == [5.0]


# ---------------------------------------------------------------- 取消与异常后的清理


async def test_cancel_saves_record_closes_connections_releases_slot(env: Env, cdn: FakeCdn) -> None:
    await env.setup([("a.mkv", SIZE_A)])
    stalled = asyncio.Event()
    release = asyncio.Event()
    env.http_wrapper = lambda inner: StreamHook(
        inner, lambda s: StallStream(s, 20_480, stalled, release)
    )
    f = env.fetcher(conns=2, checkpoint=8192)
    task = asyncio.ensure_future(f.fetch(H))
    await stalled.wait()
    # 让另一个连接也写完它那 20480 字节（读线程让两个连接不再同步推进，不能靠 sleep 估计）
    await wait_until(lambda: f._remaining.get(H) == SIZE_A - 2 * 20_480)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    sd = env.incomplete / ".state" / H
    assert list(sd.glob("*.tmp")) == []  # 没有残留临时文件
    segs = _load_segments(next(sd.glob("*.json")), SIZE_A)  # 记录可读
    assert segs is not None
    written = sum(s.done for s in segs)
    assert written >= 20_480
    assert env.path("a.mkv").stat().st_size >= max(s.start + s.done for s in segs if s.done)
    assert (await env.repo.list_task_files(H))[0].local_bytes == written
    release.set()  # 读线程阻塞的那次读返回
    await join_readers()  # 线程发现停止标志后退出
    assert cdn.active_connections(env.files[0].pickcode) == 0  # 连接已关闭
    assert (await env.repo.list_task_files(H))[0].local_bytes == written  # 遗留线程没有再写
    assert_idle(f)  # max_files 名额释放

    # 之后能从记录续传并得到正确结果
    env.http_wrapper = None
    await drive(env.clock, env.fetcher().fetch(H))
    env.assert_content()


async def test_cancel_not_masked_by_checkpoint_failure(
    env: Env, monkeypatch: pytest.MonkeyPatch
) -> None:
    """收尾检查点抛出非 OSError 的异常时，不能顶掉 CancelledError。"""
    await env.setup([("a.mkv", SIZE_A)])
    stalled = asyncio.Event()
    real = fetch_mod._persist

    def persist(fs: Any, snapshot: Any) -> None:
        if stalled.is_set():
            raise RuntimeError("checkpoint exploded")
        real(fs, snapshot)

    monkeypatch.setattr(fetch_mod, "_persist", persist)
    env.http_wrapper = lambda inner: StreamHook(inner, lambda s: StallStream(s, 20_480, stalled))
    f = env.fetcher(conns=2, checkpoint=8192)
    task = asyncio.ensure_future(f.fetch(H))
    await stalled.wait()
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert_idle(f)


async def test_exception_releases_slot_and_closes_connections(
    env: Env, cdn: FakeCdn, cloud: FakeCloud
) -> None:
    await env.setup([("a.mkv", SIZE_A), ("b.mkv", SIZE_B)])
    cloud.inject("download_link", RiskControl("boom"), on_call=3, times=None)
    f = env.fetcher(max_files=2)
    with pytest.raises(RiskControl):
        await drive(env.clock, f.fetch(H))
    for tf in env.files:
        assert cdn.active_connections(tf.pickcode) == 0
    assert_idle(f, 2)
    assert await fetch_retries(env.repo) == 0


# ---------------------------------------------------------------- 恢复路径


def write_record(env: Env, segments: list[list[int]], raw: str | None = None) -> None:
    sd = env.incomplete / ".state" / H
    sd.mkdir(parents=True, exist_ok=True)
    key = hashlib.sha1(b"a.mkv").hexdigest()
    text = raw if raw is not None else json.dumps({"v": 1, "size": SIZE_A, "segments": segments})
    (sd / f"{key}.json").write_text(text)


def write_partial(env: Env, data: bytes) -> None:
    env.path("a.mkv").parent.mkdir(parents=True, exist_ok=True)
    env.path("a.mkv").write_bytes(data)


@pytest.mark.parametrize(
    "raw",
    [
        "not json {",
        "[]",
        "null",
        json.dumps({"size": SIZE_A}),
        json.dumps({"size": SIZE_A, "segments": [[0, 50000, 60000], [50000, SIZE_A, 0]]}),
        json.dumps({"size": SIZE_A, "segments": [[0, 10, 0]]}),
        json.dumps({"size": SIZE_A, "segments": [[0, "x", 0]]}),
        json.dumps({"size": SIZE_A + 1, "segments": [[0, SIZE_A + 1, 5]]}),
    ],
    ids=["garbage", "list", "null", "no-segments", "done>len", "gap", "bad-type", "wrong-size"],
)
async def test_corrupt_record_redownloads_everything(env: Env, cdn: FakeCdn, raw: str) -> None:
    await env.setup([("a.mkv", SIZE_A)])
    write_record(env, [], raw=raw)
    write_partial(env, env.contents["a.mkv"][:30_000])
    await drive(env.clock, env.fetcher().fetch(H))
    env.assert_content()
    assert downloaded(cdn) == SIZE_A


async def test_discarded_record_zeroes_db_display(env: Env, repo: SqliteTaskRepo) -> None:
    await env.setup([("a.mkv", SIZE_A)])
    write_record(env, [], raw="garbage")
    write_partial(env, b"x" * 30_000)
    await repo.update_task_file(H, "a.mkv", local_bytes=30_000)
    await repo.update(H, TaskUpdate(local_bytes=30_000))
    res = await env.fetcher(free=10 * GIB).fetch(H)  # 空间不足，只做准备
    assert res.status is FetchStatus.WAITING_FOR_SPACE
    assert (await repo.list_task_files(H))[0].local_bytes == 0
    t = await repo.get_task(H)
    assert t is not None and t.local_bytes == 0
    assert not env.path("a.mkv").exists()


async def test_file_larger_than_expected_redownloaded(env: Env, cdn: FakeCdn) -> None:
    await env.setup([("a.mkv", SIZE_A)])
    write_record(env, [[0, 50000, 50000], [50000, SIZE_A, 0]])
    write_partial(env, env.contents["a.mkv"] + b"extra" * 2)
    await drive(env.clock, env.fetcher().fetch(H))
    env.assert_content()
    assert env.path("a.mkv").stat().st_size == SIZE_A
    assert downloaded(cdn) == SIZE_A


async def test_file_truncated_below_record_redownloaded(env: Env, cdn: FakeCdn) -> None:
    await env.setup([("a.mkv", SIZE_A)])
    write_record(env, [[0, 50000, 50000], [50000, SIZE_A, 0]])
    write_partial(env, env.contents["a.mkv"][:10_000])  # 比记录里的 50000 少
    await drive(env.clock, env.fetcher().fetch(H))
    env.assert_content()
    assert downloaded(cdn) == SIZE_A


# ---------------------------------------------------------------- 检查点频率


def count_persists(monkeypatch: pytest.MonkeyPatch) -> dict[str, int]:
    real = fetch_mod._persist
    st = {"n": 0}

    def counting(fs: Any, snapshot: Any) -> None:
        st["n"] += 1
        real(fs, snapshot)

    monkeypatch.setattr(fetch_mod, "_persist", counting)
    return st


async def test_checkpoint_not_taken_before_threshold(
    env: Env, monkeypatch: pytest.MonkeyPatch
) -> None:
    """字节数没到、时间没到：整个文件只在轮末存一次检查点。"""
    await env.setup([("a.mkv", SIZE_A)])
    st = count_persists(monkeypatch)
    await env.fetcher(conns=1, checkpoint=10**9).fetch(H)
    assert st["n"] == 1
    env.assert_content()


async def test_checkpoint_by_bytes(env: Env, monkeypatch: pytest.MonkeyPatch) -> None:
    await env.setup([("a.mkv", SIZE_A)])
    st = count_persists(monkeypatch)
    await env.fetcher(conns=1, checkpoint=8192).fetch(H)
    assert st["n"] == SIZE_A // 8192 + 1  # 每 8192 字节一次，加轮末一次


async def test_checkpoint_by_seconds(env: Env, monkeypatch: pytest.MonkeyPatch) -> None:
    await env.setup([("a.mkv", SIZE_A)])
    st = count_persists(monkeypatch)
    env.http_wrapper = lambda inner: StreamHook(inner, lambda s: TickStream(s, env.clock, 11.0))
    await env.fetcher(conns=1, checkpoint=10**9).fetch(H)
    assert st["n"] > 10  # 每块都超过 10 秒
    env.assert_content()


# ---------------------------------------------------------------- CDN 连接层错误（不计重试）


class ConnectFail(httpx.AsyncBaseTransport):
    """前 ``times`` 个请求在收到任何响应前抛指定的连接层异常，之后放行。"""

    def __init__(
        self, inner: httpx.AsyncBaseTransport, exc: Callable[[], Exception], times: int
    ) -> None:
        self.inner = inner
        self.exc = exc
        self.left = times

    async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
        if self.left > 0:
            self.left -= 1
            raise self.exc()
        return await self.inner.handle_async_request(request)


@pytest.mark.parametrize(
    "make",
    [
        lambda: httpx.ConnectError("refused"),
        lambda: httpx.ConnectTimeout("timed out"),
    ],
)
async def test_cdn_connect_error_is_network_error_not_counted(
    env: Env, repo: SqliteTaskRepo, make: Callable[[], Exception]
) -> None:
    from bridge.cloud.protocol import NetworkError

    await env.setup([("a.mkv", SIZE_A)])
    holder: list[ConnectFail] = []

    def wrap(t: httpx.AsyncBaseTransport) -> httpx.AsyncBaseTransport:
        holder.append(ConnectFail(t, make, 1))
        return holder[0]

    env.http_wrapper = wrap
    f = env.fetcher(conns=1)
    with pytest.raises(NetworkError):
        await drive(env.clock, f.fetch(H))
    assert await fetch_retries(repo) == 0
    assert env.clock.slept == []  # 没有退避
    assert_idle(f)
    # 恢复连接后再调用，能完成
    res = await drive(env.clock, f.fetch(H))
    assert res.status == FetchStatus.DONE
    env.assert_content()
    assert await fetch_retries(repo) == 0


async def test_cdn_connect_error_after_progress_resumes_from_checkpoint(
    env: Env, repo: SqliteTaskRepo, cdn: FakeCdn
) -> None:
    """先下一部分后断流（有进展，不计重试），续传时连接失败转 NetworkError，检查点已存。"""
    from bridge.cloud.protocol import NetworkError

    await env.setup([("a.mkv", SIZE_A)])
    cdn.queue_fault(env.files[0].pickcode, CdnFault("cut", after_bytes=12_000))

    class SecondRequestFails(httpx.AsyncBaseTransport):
        def __init__(self, inner: httpx.AsyncBaseTransport) -> None:
            self.inner = inner
            self.n = 0
            self.fail = True

        async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
            self.n += 1
            if self.n == 2 and self.fail:
                raise httpx.ConnectError("down")
            return await self.inner.handle_async_request(request)

    trans: list[SecondRequestFails] = []

    def wrap(t: httpx.AsyncBaseTransport) -> httpx.AsyncBaseTransport:
        trans.append(SecondRequestFails(t))
        return trans[0]

    env.http_wrapper = wrap
    f = env.fetcher(conns=1)
    with pytest.raises(NetworkError):
        await drive(env.clock, f.fetch(H))
    assert await fetch_retries(repo) == 0
    assert_idle(f)
    rec = list(f.state_dir(H).glob("*.json"))
    assert len(rec) == 1
    segs = json.loads(rec[0].read_text())["segments"]
    assert sum(s[2] for s in segs) == 12_000  # 检查点记着已写的部分
    res = await drive(env.clock, f.fetch(H))
    assert res.status == FetchStatus.DONE
    env.assert_content()
    starts = [int((r.range or "")[6:].split("-")[0]) for r in cdn.requests if r.status == 206]
    assert starts == [0, 12_000]  # 续传不重下已写部分
    assert await fetch_retries(repo) == 0


async def test_cdn_connect_error_releases_slots_with_many_files(
    env: Env, repo: SqliteTaskRepo
) -> None:
    from bridge.cloud.protocol import NetworkError

    await env.setup([("a.mkv", SIZE_A), ("b.mkv", SIZE_B)])
    env.http_wrapper = lambda t: ConnectFail(t, lambda: httpx.ConnectError("x"), 10**6)
    f = env.fetcher(conns=2, max_files=2)
    with pytest.raises(NetworkError):
        await drive(env.clock, f.fetch(H))
    assert await fetch_retries(repo) == 0
    assert_idle(f, max_files=2)


async def test_cdn_read_error_before_data_still_counted(env: Env, repo: SqliteTaskRepo) -> None:
    """读超时不属于连接层错误：没有进展时照常计重试。"""
    await env.setup([("a.mkv", SIZE_A)])
    env.http_wrapper = lambda t: ConnectFail(t, lambda: httpx.ReadTimeout("slow"), 1)
    f = env.fetcher(conns=1)
    await drive(env.clock, f.fetch(H))
    assert await fetch_retries(repo) == 1
    env.assert_content()
