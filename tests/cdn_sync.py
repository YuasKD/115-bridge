"""测试装置（本文件自身没有测试）：把异步传输层包成同步传输层，供取回的读线程使用。

取回的读线程用同步 ``httpx.Client``，而一部分旧测试的传输层是异步的（门、停住的流、拨假时钟的流
都用事件循环上的 ``asyncio.Event``）。这里的适配器让读线程把每次调用交给测试所在的事件循环执行
并阻塞等待结果，测试里的这些异步传输层不用改写，行为也与原来一致。
"""

from __future__ import annotations

import asyncio
import contextlib
import threading
from collections.abc import Iterator

import httpx


class _SyncStream(httpx.SyncByteStream):
    def __init__(self, inner: httpx.AsyncByteStream, loop: asyncio.AbstractEventLoop) -> None:
        self._inner = inner
        self._loop = loop

    def __iter__(self) -> Iterator[bytes]:
        ait = self._inner.__aiter__()

        async def step() -> bytes | None:
            try:
                return await ait.__anext__()
            except StopAsyncIteration:
                return None

        while True:
            chunk = asyncio.run_coroutine_threadsafe(step(), self._loop).result()
            if chunk is None:
                return
            yield chunk

    def close(self) -> None:
        with contextlib.suppress(RuntimeError):
            asyncio.run_coroutine_threadsafe(self._inner.aclose(), self._loop).result()


class AsyncToSyncTransport(httpx.BaseTransport):
    def __init__(self, inner: httpx.AsyncBaseTransport, loop: asyncio.AbstractEventLoop) -> None:
        self._inner = inner
        self._loop = loop

    def handle_request(self, request: httpx.Request) -> httpx.Response:
        resp = asyncio.run_coroutine_threadsafe(
            self._inner.handle_async_request(request), self._loop
        ).result()
        assert isinstance(resp.stream, httpx.AsyncByteStream)
        return httpx.Response(
            resp.status_code,
            headers=resp.headers,
            stream=_SyncStream(resp.stream, self._loop),
            extensions=resp.extensions,
        )


def sync_client(inner: httpx.AsyncBaseTransport) -> httpx.Client:
    """在运行中的事件循环里调用：返回经由该事件循环执行 ``inner`` 的同步客户端。"""
    return httpx.Client(transport=AsyncToSyncTransport(inner, asyncio.get_running_loop()))


async def join_readers(limit: float = 5.0) -> None:
    """等所有取回读线程退出（真实线程，超时即视为泄漏）。"""
    for t in [t for t in threading.enumerate() if t.name == "cdn-reader"]:
        await asyncio.to_thread(t.join, limit)
        assert not t.is_alive(), "cdn-reader thread did not exit"
