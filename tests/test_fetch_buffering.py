# ruff: noqa: F811
"""T11 性能修复：取回攒块写盘。夹具复用 ``test_fetch.py``。"""

from __future__ import annotations

import asyncio
import errno
from typing import Any

import pytest

import bridge.pipeline.fetch as fetch_mod
from bridge.cloud.fake import CdnFault, FakeCdn
from bridge.pipeline.fetch import FetchStatus, _load_segments
from test_fetch import (  # noqa: F401  (夹具)
    SIZE_A,
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
)
from test_fetch_rework import StallStream, StreamHook, assert_idle

BLOCK = 32_768
HUGE = 1 << 30


def spy_writes(monkeypatch: pytest.MonkeyPatch) -> dict[str, Any]:
    real = fetch_mod._write_chunk
    st: dict[str, Any] = {"calls": 0, "bytes": 0}

    def fake(fs: Any, offset: int, data: bytes) -> None:
        real(fs, offset, data)
        st["calls"] += 1
        st["bytes"] += len(data)

    monkeypatch.setattr(fetch_mod, "_write_chunk", fake)
    return st


async def test_write_count_matches_block_size(env: Env, monkeypatch: pytest.MonkeyPatch) -> None:
    await env.setup([("a.mkv", SIZE_A)])
    st = spy_writes(monkeypatch)
    f = env.fetcher(conns=1, checkpoint=HUGE, write_block=BLOCK)
    res = await f.fetch(H)
    assert res.status is FetchStatus.DONE
    env.assert_content()
    chunks = -(-SIZE_A // 4096)  # 收到的块数 25
    assert st["bytes"] == SIZE_A
    assert st["calls"] == -(-SIZE_A // BLOCK) == 4
    assert st["calls"] < chunks / 4


async def test_record_never_ahead_across_checkpoint_threshold(
    env: Env, monkeypatch: pytest.MonkeyPatch
) -> None:
    await env.setup([("a.mkv", SIZE_A)])
    st = spy_writes(monkeypatch)
    seen: list[int] = []
    real = fetch_mod._persist

    def persist(fs: Any, snapshot: Any) -> None:
        total = sum(s[2] for s in snapshot)
        assert total <= st["bytes"]  # 记录不超前于已写数据
        seen.append(total)
        real(fs, snapshot)

    monkeypatch.setattr(fetch_mod, "_persist", persist)
    # 检查点阈值 20000 落在写块 32768 之内：缓冲达到阈值就先写盘再存检查点
    f = env.fetcher(conns=1, checkpoint=20_000, write_block=BLOCK)
    await f.fetch(H)
    env.assert_content()
    assert seen and seen[0] >= 20_000
    assert all(x <= SIZE_A for x in seen)


async def test_stream_cut_flushes_buffer_and_counts_as_progress(env: Env, cdn: FakeCdn) -> None:
    await env.setup([("a.mkv", SIZE_A)])
    cdn.queue_fault(env.files[0].pickcode, CdnFault("cut", after_bytes=10_000))
    f = env.fetcher(conns=1, checkpoint=HUGE, write_block=HUGE)
    res = await drive(env.clock, f.fetch(H))
    assert res.status is FetchStatus.DONE
    env.assert_content()
    assert await fetch_retries(env.repo) == 0  # 缓冲数据写盘后算进展
    assert env.clock.slept == [5.0]
    assert cdn.requests[1].range is not None
    assert not cdn.requests[1].range.startswith("bytes=0-")  # 从已写位置续传


async def test_cancel_flushes_buffer_and_keeps_cancelled_error(env: Env, cdn: FakeCdn) -> None:
    await env.setup([("a.mkv", SIZE_A)])
    stalled = asyncio.Event()
    env.http_wrapper = lambda inner: StreamHook(inner, lambda s: StallStream(s, 20_480, stalled))
    f = env.fetcher(conns=1, checkpoint=HUGE, write_block=HUGE)
    task = asyncio.ensure_future(f.fetch(H))
    await stalled.wait()
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    sd = env.incomplete / ".state" / H
    segs = _load_segments(next(sd.glob("*.json")), SIZE_A)
    assert segs is not None and sum(s.done for s in segs) == 20_480
    assert env.path("a.mkv").read_bytes()[:20_480] == env.contents["a.mkv"][:20_480]
    assert_idle(f)
    env.http_wrapper = None
    await drive(env.clock, env.fetcher().fetch(H))
    env.assert_content()


async def test_enospc_on_buffer_write_waits_for_space(
    env: Env, cdn: FakeCdn, monkeypatch: pytest.MonkeyPatch
) -> None:
    await env.setup([("a.mkv", SIZE_A)])
    real = fetch_mod._write_chunk
    st: dict[str, Any] = {"n": 0, "on": True}

    def fake(fs: Any, offset: int, data: bytes) -> None:
        st["n"] += 1
        if st["on"] and st["n"] > 1:  # 第一块写成功，之后的缓冲写满
            raise OSError(errno.ENOSPC, "injected")
        real(fs, offset, data)

    monkeypatch.setattr(fetch_mod, "_write_chunk", fake)
    f = env.fetcher(conns=1, checkpoint=HUGE, write_block=BLOCK)
    res = await f.fetch(H)
    assert res.status is FetchStatus.WAITING_FOR_SPACE
    assert res.downloaded_bytes == BLOCK
    assert await fetch_retries(env.repo) == 0
    assert env.clock.slept == []
    assert_idle(f)

    st["on"] = False
    cdn.requests.clear()
    assert (await drive(env.clock, f.fetch(H))).status is FetchStatus.DONE
    env.assert_content()
    assert cdn.requests[0].range == f"bytes={BLOCK}-{SIZE_A - 1}"
