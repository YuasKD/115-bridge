"""T18：`_tx` 在取消与 COMMIT 失败下不把事务留在连接上。

做法：替换连接的 `execute`，在指定语句排进 aiosqlite 线程的那一刻取消外层任务，
取消就落在这条语句的 await 上（与生产中取消正在写库的取回同一窗口）。不用 sleep。
"""

from __future__ import annotations

import asyncio
import sqlite3
from collections.abc import AsyncIterator, Callable
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pytest

from bridge.db.repo import Source
from bridge.db.sqlite import SqliteTaskRepo

T0 = datetime(2026, 1, 1, tzinfo=UTC)
H1 = "1" * 40
H2 = "2" * 40
EVENT_INSERT = "INSERT INTO events"


@pytest.fixture
async def repo(tmp_path: Path) -> AsyncIterator[SqliteTaskRepo]:
    r = await SqliteTaskRepo.open(tmp_path / "bridge.db")
    yield r
    await r.close()


class Injector:
    """在 sql 以某前缀开头的语句发出时执行动作（每个前缀只触发一次）。"""

    def __init__(self, repo: SqliteTaskRepo) -> None:
        self.conn = repo._conn
        self.real = self.conn.execute
        self.actions: dict[str, Callable[[], None]] = {}
        self.seen: list[str] = []

    def on(self, prefix: str, action: Callable[[], None]) -> None:
        self.actions[prefix] = action

    def execute(self, sql: str, *args: Any, **kwargs: Any) -> Any:
        self.seen.append(sql)
        for prefix in list(self.actions):
            if sql.startswith(prefix):
                self.actions.pop(prefix)()
        return self.real(sql, *args, **kwargs)

    def __enter__(self) -> Injector:
        self.conn.execute = self.execute  # type: ignore[method-assign]
        return self

    def __exit__(self, *exc: object) -> None:
        del self.conn.execute


async def create(repo: SqliteTaskRepo, h: str) -> None:
    await repo.create_task(h, "radarr", "n", Source.TORRENT, T0, "magnet:?")


async def run_cancelled(
    repo: SqliteTaskRepo, setup: Callable[[Injector, asyncio.Task[None]], None]
) -> Injector:
    with Injector(repo) as inj:
        t = asyncio.create_task(create(repo, H1))
        setup(inj, t)
        with pytest.raises(asyncio.CancelledError):
            await t
    return inj


async def assert_healthy(repo: SqliteTaskRepo) -> None:
    assert not repo._conn.in_transaction
    await create(repo, H2)
    assert await repo.get_task(H2) is not None


async def assert_atomic(repo: SqliteTaskRepo, *, committed: bool) -> None:
    task = await repo.get_task(H1)
    events = await repo.list_events(H1)
    if committed:
        assert task is not None and len(events) == 1
    else:
        assert task is None and events == []


async def test_cancel_on_begin(repo: SqliteTaskRepo) -> None:
    await run_cancelled(repo, lambda inj, t: inj.on("BEGIN", t.cancel))
    await assert_healthy(repo)
    await assert_atomic(repo, committed=False)


async def test_cancel_in_body(repo: SqliteTaskRepo) -> None:
    inj = await run_cancelled(repo, lambda inj, t: inj.on(EVENT_INSERT, t.cancel))
    assert "ROLLBACK" in inj.seen
    await assert_healthy(repo)
    await assert_atomic(repo, committed=False)


async def test_cancel_on_commit(repo: SqliteTaskRepo) -> None:
    await run_cancelled(repo, lambda inj, t: inj.on("COMMIT", t.cancel))
    await assert_healthy(repo)
    # COMMIT 已排进线程，等它执行完：整体提交，取消照常向上抛
    await assert_atomic(repo, committed=True)


async def test_cancel_again_during_rollback(repo: SqliteTaskRepo) -> None:
    def setup(inj: Injector, t: asyncio.Task[None]) -> None:
        inj.on(EVENT_INSERT, t.cancel)
        inj.on("ROLLBACK", t.cancel)

    await run_cancelled(repo, setup)
    await assert_healthy(repo)
    await assert_atomic(repo, committed=False)


async def test_cancel_twice_on_begin(repo: SqliteTaskRepo) -> None:
    def setup(inj: Injector, t: asyncio.Task[None]) -> None:
        def twice() -> None:
            t.cancel()
            t.cancel()

        inj.on("BEGIN", twice)

    await run_cancelled(repo, setup)
    await assert_healthy(repo)
    await assert_atomic(repo, committed=False)


async def test_lock_held_until_begin_finished(repo: SqliteTaskRepo) -> None:
    """取消后锁要等 BEGIN 真正执行完才释放，下一笔写不会撞上半开的事务。"""
    with Injector(repo) as inj:
        t = asyncio.create_task(create(repo, H1))
        inj.on("BEGIN", t.cancel)
        nxt = asyncio.create_task(create(repo, H2))
        with pytest.raises(asyncio.CancelledError):
            await t
        await nxt
    assert await repo.get_task(H2) is not None
    assert await repo.get_task(H1) is None
    assert not repo._conn.in_transaction


async def test_commit_failure_rolls_back(repo: SqliteTaskRepo) -> None:
    real = repo._conn.execute

    def execute(sql: str, *args: Any, **kwargs: Any) -> Any:
        if sql == "COMMIT":
            raise sqlite3.OperationalError("injected commit failure")
        return real(sql, *args, **kwargs)

    repo._conn.execute = execute  # type: ignore[method-assign]
    try:
        with pytest.raises(sqlite3.OperationalError, match="injected"):
            await create(repo, H1)
    finally:
        del repo._conn.execute
    await assert_healthy(repo)
    await assert_atomic(repo, committed=False)


async def test_body_exception_still_rolls_back(repo: SqliteTaskRepo) -> None:
    def boom() -> None:
        raise RuntimeError("boom")

    with Injector(repo) as inj:
        inj.on(EVENT_INSERT, boom)
        with pytest.raises(RuntimeError, match="boom"):
            await create(repo, H1)
    await assert_healthy(repo)
    await assert_atomic(repo, committed=False)
