"""T17：取回的响应体在专用读线程里用同步 httpx 读取。

本文件的测试直接用 ``FakeCdn.sync_transport``（真正的同步路径，不经过异步适配器），读线程是
真实线程；线程同步只用 ``threading.Event``，不靠 sleep 等待。
"""

from __future__ import annotations

import asyncio
import threading
from collections.abc import Iterator

import httpx
import pytest

import test_fetch as _tf
from bridge.cloud.fake import CdnFault, FakeCdn
from bridge.cloud.protocol import NetworkError
from bridge.config import FetchConfig
from bridge.pipeline.fetch import GIB, Fetcher, FetchStatus, _BodyReader
from cdn_sync import join_readers
from test_fetch import SIZE_A, Env, H, drive, fetch_retries, wait_until

# 夹具：赋值再导出（直接 import 会让参数同名触发 F811）
cdn = _tf.cdn
clock = _tf.clock
cloud = _tf.cloud
env = _tf.env
incomplete = _tf.incomplete
repo = _tf.repo

# ---------------------------------------------------------------- 测试用同步传输层


class _SyncWrap(httpx.SyncByteStream):
    def __init__(self, inner: httpx.SyncByteStream) -> None:
        self.inner = inner

    def __iter__(self) -> Iterator[bytes]:
        yield from self.inner

    def close(self) -> None:
        self.inner.close()


class _Extra(_SyncWrap):
    """正文比声明的长：把多余字节拼到最后一块。"""

    def __init__(self, inner: httpx.SyncByteStream, extra: bytes) -> None:
        super().__init__(inner)
        self.extra = extra

    def __iter__(self) -> Iterator[bytes]:
        prev: bytes | None = None
        for c in self.inner:
            if prev is not None:
                yield prev
            prev = c
        if prev is not None:
            yield prev + self.extra


class _Block(_SyncWrap):
    """发出 ``after`` 字节后卡在 ``gate`` 上（模拟阻塞中的 socket 读），放行后继续。"""

    def __init__(
        self,
        inner: httpx.SyncByteStream,
        after: int,
        reached: threading.Event,
        gate: threading.Event,
    ) -> None:
        super().__init__(inner)
        self.after = after
        self.reached = reached
        self.gate = gate

    def __iter__(self) -> Iterator[bytes]:
        sent = 0
        for c in self.inner:
            yield c
            sent += len(c)
            if sent >= self.after:
                self.reached.set()
                self.gate.wait()


class _Counting(_SyncWrap):
    def __init__(self, inner: httpx.SyncByteStream, on_pull: threading.Event, limit: int) -> None:
        super().__init__(inner)
        self.pulled = 0
        self.on_pull = on_pull
        self.limit = limit

    def __iter__(self) -> Iterator[bytes]:
        for c in self.inner:
            self.pulled += 1
            if self.pulled >= self.limit:
                self.on_pull.set()
            yield c


class _Hook(httpx.BaseTransport):
    def __init__(self, inner: httpx.BaseTransport, make: object) -> None:
        self.inner = inner
        self.make = make  # (stream) -> stream | None，只对第一个请求生效
        self.used = False

    def handle_request(self, request: httpx.Request) -> httpx.Response:
        resp = self.inner.handle_request(request)
        assert isinstance(resp.stream, httpx.SyncByteStream)
        if self.used:
            return resp
        self.used = True
        new = self.make(resp.stream)  # type: ignore[operator]
        return httpx.Response(resp.status_code, headers=resp.headers, stream=new)


class _ConnectFail(httpx.BaseTransport):
    def __init__(self, exc: Exception) -> None:
        self.exc = exc

    def handle_request(self, request: httpx.Request) -> httpx.Response:
        raise self.exc


def make_fetcher(
    env: Env,
    transport: httpx.BaseTransport,
    *,
    conns: int = 1,
    read_chunk: int = 1024 * 1024,
    depth: int = 4,
    write_block: int = 4096,
    checkpoint: int = 8192,
) -> Fetcher:
    return Fetcher(
        repo=env.repo,
        cloud=env.cloud,
        http=httpx.Client(transport=transport),
        clock=env.clock,
        config=FetchConfig(connections_per_file=conns, max_files=1),
        incomplete_dir=env.incomplete,
        free_space=lambda _p: 10_000 * GIB,
        checkpoint_bytes=checkpoint,
        write_block_bytes=write_block,
        read_chunk_bytes=read_chunk,
        queue_depth=depth,
    )


# ---------------------------------------------------------------- 测试


async def test_plain_download_via_sync_path(env: Env, cdn: FakeCdn) -> None:
    await env.setup([("a.mkv", SIZE_A)])
    res = await drive(env.clock, make_fetcher(env, cdn.sync_transport, conns=2).fetch(H))
    assert res.status is FetchStatus.DONE
    env.assert_content()
    assert all(r.status == 206 for r in cdn.requests)
    assert len({r.token for r in cdn.requests}) == len(cdn.requests)  # 每次都取新链接
    await join_readers()
    assert cdn.active_connections(env.files[0].pickcode) == 0


@pytest.mark.parametrize("read_chunk", [1, 1024 * 1024])
async def test_cut_resumes_with_threaded_reader(env: Env, cdn: FakeCdn, read_chunk: int) -> None:
    """断流后续传，字节正确。read_chunk 为 1 MiB 时断流前不足一块的数据也不能丢。"""
    await env.setup([("a.mkv", SIZE_A)])
    cdn.queue_fault(env.files[0].pickcode, CdnFault("cut", after_bytes=12_000))
    f = make_fetcher(env, cdn.sync_transport, read_chunk=read_chunk)
    await drive(env.clock, f.fetch(H))
    env.assert_content()
    assert await fetch_retries(env.repo) == 0  # 断流前收到过数据：有进展，不计重试
    assert env.clock.slept == [5.0]
    starts = [int((r.range or "")[6:].split("-")[0]) for r in cdn.requests if r.status == 206]
    assert starts[0] == 0
    assert starts[1] == 12_000  # 从断点续，已收到的字节没有因断流丢失
    await join_readers()


async def test_extra_bytes_from_cdn_are_truncated(env: Env, cdn: FakeCdn) -> None:
    await env.setup([("a.mkv", SIZE_A)])
    transport = _Hook(cdn.sync_transport, lambda s: _Extra(s, b"X" * 777))
    sizes: list[int] = []

    class Spy(httpx.BaseTransport):
        def handle_request(self, request: httpx.Request) -> httpx.Response:
            p = env.path("a.mkv")
            if p.exists():
                sizes.append(p.stat().st_size)
            return transport.handle_request(request)

    await drive(env.clock, make_fetcher(env, Spy(), read_chunk=1).fetch(H))
    env.assert_content()  # 多发的 777 字节没有写进文件
    assert max(sizes, default=0) <= SIZE_A
    assert env.path("a.mkv").stat().st_size == SIZE_A
    assert await fetch_retries(env.repo) == 0  # 数据都在，有进展
    assert env.clock.slept == [5.0]  # 多发按单次失败处理，退避后重取
    await join_readers()


@pytest.mark.parametrize("exc", [httpx.ConnectError("refused"), httpx.ConnectTimeout("slow")])
async def test_connect_failure_is_network_error(env: Env, exc: Exception) -> None:
    await env.setup([("a.mkv", SIZE_A)])
    f = make_fetcher(env, _ConnectFail(exc))
    with pytest.raises(NetworkError):
        await f.fetch(H)
    assert await fetch_retries(env.repo) == 0  # 全局网络问题，不计取回重试
    await join_readers()


async def test_cancel_stops_reader_thread_without_further_writes(env: Env, cdn: FakeCdn) -> None:
    await env.setup([("a.mkv", SIZE_A)])
    reached, gate = threading.Event(), threading.Event()
    transport = _Hook(cdn.sync_transport, lambda s: _Block(s, 8192, reached, gate))
    f = make_fetcher(env, transport, read_chunk=1)
    task = asyncio.ensure_future(f.fetch(H))
    assert await asyncio.to_thread(reached.wait, 5)  # 读线程停在「阻塞的读」上
    await wait_until(lambda: f._remaining.get(H) == SIZE_A - 8192)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task  # 取消不等读线程
    size_before = env.path("a.mkv").stat().st_size
    local_before = (await env.repo.list_task_files(H))[0].local_bytes
    gate.set()  # 阻塞的读返回
    await join_readers()  # 线程看到停止标志，在有限时间内退出
    assert cdn.active_connections(env.files[0].pickcode) == 0  # 连接已由线程自己关闭
    assert env.path("a.mkv").stat().st_size == size_before  # 没有遗留写入
    assert (await env.repo.list_task_files(H))[0].local_bytes == local_before
    assert f._remaining == {} and f._active_files == 0

    # 之后能续传并得到正确结果
    await drive(env.clock, make_fetcher(env, cdn.sync_transport).fetch(H))
    env.assert_content()


async def test_reader_queue_is_bounded(env: Env, cdn: FakeCdn) -> None:
    """消费者不取时，读线程最多多读 depth + 1 块（队列里 depth 块，手里 1 块）就被背压挡住。"""
    await env.setup([("a.mkv", SIZE_A)])
    depth = 2
    on_pull = threading.Event()
    streams: list[_Counting] = []

    def make(s: httpx.SyncByteStream) -> httpx.SyncByteStream:
        c = _Counting(s, on_pull, depth + 1)
        streams.append(c)
        return c

    client = httpx.Client(transport=_Hook(cdn.sync_transport, make))
    link = await env.cloud.download_link(env.files[0].pickcode)
    headers = {**link.headers, "Range": f"bytes=0-{SIZE_A - 1}"}
    reader = _BodyReader(
        client, link.url, headers, asyncio.get_running_loop(), chunk_bytes=1, depth=depth
    )
    reader.start()
    head = await reader.head()
    assert head.status_code == 206
    reader.proceed()
    assert await asyncio.to_thread(on_pull.wait, 5)
    await wait_until(lambda: reader._events.qsize() == depth)
    # SIZE_A / 4096 约 25 块：若无背压，线程早已读完；有背压则一直活着，卡在信用上
    await asyncio.to_thread(reader._thread.join, 0.2)
    assert reader._thread.is_alive()
    assert streams[0].pulled == depth + 1

    # 取走一块，还一份信用，线程再往前读一块
    assert await reader.next_chunk() is not None
    await wait_until(lambda: streams[0].pulled == depth + 2)

    reader.stop()
    await reader.wait_closed()
    await asyncio.to_thread(reader._thread.join, 5)
    assert not reader._thread.is_alive()
    await asyncio.to_thread(client.close)
    await join_readers()
