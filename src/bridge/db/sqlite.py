"""TaskRepo 的 aiosqlite 实现。

- WAL 模式；单连接，操作用 asyncio.Lock 串行，写事务显式 BEGIN IMMEDIATE。
- 时间以 UTC ISO 字符串存储，由调用方传入；仓储内部不读系统时间。要求带时区。
- hash 必须是小写 hex，大写会被拒绝（ValueError），不做静默规范化。
- 115 的 id 一律 TEXT。
"""

from __future__ import annotations

import asyncio
import json
import re
import sqlite3
from collections.abc import AsyncIterator, Collection
from contextlib import asynccontextmanager
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any, Self

import aiosqlite

from bridge.db.repo import (
    EVENTS_PER_TASK,
    CloudIndexEntry,
    Event,
    Fallback,
    InvalidTransition,
    ManifestFile,
    Source,
    Stage,
    StateMismatch,
    Task,
    TaskFile,
    TaskNotFound,
    TaskState,
    TaskUpdate,
    can_transition,
)
from bridge.db.schema import SCHEMA_SQL

_HASH_RE = re.compile(r"[0-9a-f]+")
_LEASE_KEY = "lease"


def _check_hash(info_hash: str) -> str:
    if not _HASH_RE.fullmatch(info_hash):
        raise ValueError(f"hash must be lowercase hex: {info_hash!r}")
    return info_hash


def _enc(dt: datetime) -> str:
    if dt.tzinfo is None:
        raise ValueError("datetime must be timezone-aware")
    return dt.astimezone(UTC).isoformat()


def _dec(s: str) -> datetime:
    return datetime.fromisoformat(s)


def _dec_opt(s: str | None) -> datetime | None:
    return None if s is None else _dec(s)


def _row_to_task(r: sqlite3.Row) -> Task:
    raw: dict[str, int] = json.loads(r["retries"])
    return Task(
        hash=r["hash"],
        category=r["category"],
        name=r["name"],
        state=TaskState(r["state"]),
        source=Source(r["source"]),
        added_at=_dec(r["added_at"]),
        submitted_at=_dec_opt(r["submitted_at"]),
        cloud_file_id=r["cloud_file_id"],
        cloud_size=r["cloud_size"],
        cloud_percent=r["cloud_percent"],
        local_bytes=r["local_bytes"],
        error=r["error"],
        retries={Stage(k): v for k, v in raw.items()},
        fallback=Fallback(r["fallback"]),
        client_deleted=bool(r["client_deleted"]),
        magnet=r["magnet"],
    )


class SqliteTaskRepo:
    def __init__(self, conn: aiosqlite.Connection) -> None:
        self._conn = conn
        self._lock = asyncio.Lock()

    @classmethod
    async def open(cls, path: str | Path) -> Self:
        conn = await aiosqlite.connect(str(path), isolation_level=None)
        conn.row_factory = sqlite3.Row
        await conn.execute("PRAGMA journal_mode=WAL")
        await conn.execute("PRAGMA synchronous=NORMAL")
        await conn.execute("PRAGMA foreign_keys=ON")
        await conn.executescript(SCHEMA_SQL)
        return cls(conn)

    async def close(self) -> None:
        await self._conn.close()

    # ------------------------------------------------------------ 内部

    @asynccontextmanager
    async def _tx(self) -> AsyncIterator[aiosqlite.Connection]:
        # aiosqlite 先把语句排进工作线程再 await：取消落在 await 上时语句照样执行。
        # 所以事务控制语句都等到真正执行完，任何异常（含取消）都按连接的实际
        # 事务状态回滚，保证离开这里时连接不在事务中。
        async with self._lock:
            try:
                await self._run_to_end("BEGIN IMMEDIATE")
                try:
                    yield self._conn
                except BaseException:
                    await self._rollback_if_open()
                    raise
                await self._run_to_end("COMMIT")
            except BaseException:
                await self._rollback_if_open()
                raise

    async def _run_to_end(self, sql: str) -> None:
        """执行一条事务控制语句，调用方被取消也等它执行完，再把取消向上抛。"""

        async def run() -> None:
            await self._conn.execute(sql)

        fut = asyncio.ensure_future(run())
        cancel: asyncio.CancelledError | None = None
        while not fut.done():
            try:
                await asyncio.shield(fut)
            except asyncio.CancelledError as e:
                if fut.done():
                    break
                cancel = e
        if cancel is not None:
            if not fut.cancelled():
                fut.exception()  # 取走结果，避免 "exception was never retrieved"
            raise cancel
        fut.result()

    async def _rollback_if_open(self) -> None:
        if self._conn.in_transaction:
            await self._run_to_end("ROLLBACK")

    @staticmethod
    async def _fetch_task(conn: aiosqlite.Connection, info_hash: str) -> Task | None:
        async with conn.execute("SELECT * FROM tasks WHERE hash=?", (info_hash,)) as cur:
            row = await cur.fetchone()
        return None if row is None else _row_to_task(row)

    async def _require(self, conn: aiosqlite.Connection, info_hash: str) -> Task:
        task = await self._fetch_task(conn, info_hash)
        if task is None:
            raise TaskNotFound(info_hash)
        return task

    @staticmethod
    async def _add_event(
        conn: aiosqlite.Connection,
        info_hash: str,
        at: datetime,
        src: TaskState | None,
        dst: TaskState,
        reason: str,
    ) -> None:
        await conn.execute(
            "INSERT INTO events(hash, at, from_state, to_state, reason) VALUES (?,?,?,?,?)",
            (info_hash, _enc(at), src.value if src else None, dst.value, reason),
        )
        await conn.execute(
            "DELETE FROM events WHERE hash=? AND id NOT IN "
            "(SELECT id FROM events WHERE hash=? ORDER BY id DESC LIMIT ?)",
            (info_hash, info_hash, EVENTS_PER_TASK),
        )

    # ------------------------------------------------------------ 任务

    async def create_task(
        self,
        info_hash: str,
        category: str,
        name: str,
        source: Source,
        at: datetime,
        magnet: str,
    ) -> tuple[Task, bool]:
        _check_hash(info_hash)
        async with self._tx() as conn:
            cur = await conn.execute(
                "INSERT OR IGNORE INTO tasks(hash, category, name, state, source, added_at, "
                "magnet) "
                "VALUES (?,?,?,?,?,?,?)",
                (
                    info_hash,
                    category,
                    name,
                    TaskState.QUEUED.value,
                    source.value,
                    _enc(at),
                    magnet,
                ),
            )
            created = cur.rowcount == 1
            if created:
                await self._add_event(conn, info_hash, at, None, TaskState.QUEUED, "created")
            return await self._require(conn, info_hash), created

    async def get_task(self, info_hash: str) -> Task | None:
        async with self._lock:
            return await self._fetch_task(self._conn, info_hash)

    async def list_tasks(
        self,
        *,
        category: str | None = None,
        states: Collection[TaskState] | None = None,
        include_client_deleted: bool = False,
    ) -> list[Task]:
        where: list[str] = []
        args: list[Any] = []
        if category is not None:
            where.append("category=?")
            args.append(category)
        if states is not None:
            if not states:
                return []
            where.append(f"state IN ({','.join('?' * len(states))})")
            args.extend(s.value for s in states)
        if not include_client_deleted:
            where.append("client_deleted=0")
        sql = "SELECT * FROM tasks"
        if where:
            sql += " WHERE " + " AND ".join(where)
        sql += " ORDER BY added_at, hash"
        async with self._lock, self._conn.execute(sql, args) as cur:
            return [_row_to_task(r) for r in await cur.fetchall()]

    async def transition(
        self,
        info_hash: str,
        dst: TaskState,
        reason: str,
        at: datetime,
        *,
        expected: TaskState | None = None,
    ) -> Task:
        async with self._tx() as conn:
            task = await self._require(conn, info_hash)
            if expected is not None and task.state != expected:
                raise StateMismatch(info_hash, expected, task.state)
            if not can_transition(task.state, dst):
                raise InvalidTransition(info_hash, task.state, dst)
            await conn.execute("UPDATE tasks SET state=? WHERE hash=?", (dst.value, info_hash))
            await self._add_event(conn, info_hash, at, task.state, dst, reason)
            return await self._require(conn, info_hash)

    async def requeue_removed(self, info_hash: str, reason: str, at: datetime) -> Task:
        async with self._tx() as conn:
            task = await self._require(conn, info_hash)
            if task.state != TaskState.REMOVED:
                raise InvalidTransition(info_hash, task.state, TaskState.QUEUED)
            await conn.execute(
                "UPDATE tasks SET state=?, client_deleted=0, error=NULL, fallback=?, "
                "submitted_at=NULL, cloud_file_id=NULL, cloud_size=NULL, "
                "cloud_percent=0, local_bytes=0 WHERE hash=?",
                (TaskState.QUEUED.value, Fallback.NONE.value, info_hash),
            )
            await conn.execute("DELETE FROM task_files WHERE hash=?", (info_hash,))
            await self._add_event(conn, info_hash, at, TaskState.REMOVED, TaskState.QUEUED, reason)
            return await self._require(conn, info_hash)

    async def update(self, info_hash: str, change: TaskUpdate) -> Task:
        async with self._tx() as conn:
            await self._require(conn, info_hash)
            await conn.execute(
                "UPDATE tasks SET "
                "submitted_at=COALESCE(?, submitted_at), "
                "cloud_file_id=COALESCE(?, cloud_file_id), "
                "cloud_size=COALESCE(?, cloud_size), "
                "cloud_percent=COALESCE(?, cloud_percent), "
                "local_bytes=COALESCE(?, local_bytes), "
                "name=COALESCE(?, name) WHERE hash=?",
                (
                    None if change.submitted_at is None else _enc(change.submitted_at),
                    change.cloud_file_id,
                    change.cloud_size,
                    change.cloud_percent,
                    change.local_bytes,
                    change.name,
                    info_hash,
                ),
            )
            return await self._require(conn, info_hash)

    async def set_error(self, info_hash: str, error: str | None) -> Task:
        async with self._tx() as conn:
            await self._require(conn, info_hash)
            await conn.execute("UPDATE tasks SET error=? WHERE hash=?", (error, info_hash))
            return await self._require(conn, info_hash)

    async def incr_retry(self, info_hash: str, stage: Stage) -> int:
        async with self._tx() as conn:
            task = await self._require(conn, info_hash)
            new = task.retries.get(stage, 0) + 1
            counts = {s.value: n for s, n in task.retries.items()}
            counts[stage.value] = new
            await conn.execute(
                "UPDATE tasks SET retries=? WHERE hash=?",
                (json.dumps(counts, sort_keys=True), info_hash),
            )
            return new

    async def set_fallback(self, info_hash: str, fallback: Fallback) -> Task:
        async with self._tx() as conn:
            await self._require(conn, info_hash)
            await conn.execute(
                "UPDATE tasks SET fallback=? WHERE hash=?", (fallback.value, info_hash)
            )
            return await self._require(conn, info_hash)

    async def mark_client_deleted(self, info_hash: str) -> Task:
        async with self._tx() as conn:
            await self._require(conn, info_hash)
            await conn.execute("UPDATE tasks SET client_deleted=1 WHERE hash=?", (info_hash,))
            return await self._require(conn, info_hash)

    # ------------------------------------------------------------ 清单

    async def put_manifest(self, info_hash: str, files: list[ManifestFile]) -> None:
        async with self._tx() as conn:
            await self._require(conn, info_hash)
            await conn.execute("DELETE FROM torrent_meta WHERE hash=?", (info_hash,))
            await conn.executemany(
                "INSERT INTO torrent_meta(hash, rel_path, size) VALUES (?,?,?)",
                [(info_hash, f.rel_path, f.size) for f in files],
            )

    async def get_manifest(self, info_hash: str) -> list[ManifestFile]:
        async with (
            self._lock,
            self._conn.execute(
                "SELECT rel_path, size FROM torrent_meta WHERE hash=? ORDER BY rel_path",
                (info_hash,),
            ) as cur,
        ):
            return [ManifestFile(r["rel_path"], r["size"]) for r in await cur.fetchall()]

    # ------------------------------------------------------------ task_files

    async def put_task_files(self, info_hash: str, files: list[TaskFile]) -> None:
        async with self._tx() as conn:
            await self._require(conn, info_hash)
            await conn.execute("DELETE FROM task_files WHERE hash=?", (info_hash,))
            await conn.executemany(
                "INSERT INTO task_files(hash, rel_path, size, sha1, pickcode, local_bytes, "
                "verified) VALUES (?,?,?,?,?,?,?)",
                [
                    (
                        info_hash,
                        f.rel_path,
                        f.size,
                        f.sha1,
                        f.pickcode,
                        f.local_bytes,
                        int(f.verified),
                    )
                    for f in files
                ],
            )

    async def list_task_files(self, info_hash: str) -> list[TaskFile]:
        async with (
            self._lock,
            self._conn.execute(
                "SELECT * FROM task_files WHERE hash=? ORDER BY rel_path", (info_hash,)
            ) as cur,
        ):
            return [
                TaskFile(
                    rel_path=r["rel_path"],
                    size=r["size"],
                    sha1=r["sha1"],
                    pickcode=r["pickcode"],
                    local_bytes=r["local_bytes"],
                    verified=bool(r["verified"]),
                )
                for r in await cur.fetchall()
            ]

    async def update_task_file(
        self,
        info_hash: str,
        rel_path: str,
        *,
        local_bytes: int | None = None,
        verified: bool | None = None,
    ) -> None:
        async with self._tx() as conn:
            cur = await conn.execute(
                "UPDATE task_files SET local_bytes=COALESCE(?, local_bytes), "
                "verified=COALESCE(?, verified) WHERE hash=? AND rel_path=?",
                (local_bytes, None if verified is None else int(verified), info_hash, rel_path),
            )
            if cur.rowcount == 0:
                raise TaskNotFound(f"{info_hash}:{rel_path}")

    # ------------------------------------------------------------ cloud_index

    async def get_cloud_index(self, info_hash: str) -> CloudIndexEntry | None:
        async with (
            self._lock,
            self._conn.execute("SELECT * FROM cloud_index WHERE hash=?", (info_hash,)) as cur,
        ):
            r = await cur.fetchone()
        if r is None:
            return None
        return CloudIndexEntry(
            hash=r["hash"],
            cloud_file_id=r["cloud_file_id"],
            root_name=r["root_name"],
            total_size=r["total_size"],
            completed_at=_dec(r["completed_at"]),
        )

    async def put_cloud_index(self, entry: CloudIndexEntry) -> None:
        _check_hash(entry.hash)
        async with self._tx() as conn:
            await conn.execute(
                "INSERT OR REPLACE INTO cloud_index(hash, cloud_file_id, root_name, total_size, "
                "completed_at) VALUES (?,?,?,?,?)",
                (
                    entry.hash,
                    entry.cloud_file_id,
                    entry.root_name,
                    entry.total_size,
                    _enc(entry.completed_at),
                ),
            )

    # ------------------------------------------------------------ 事件

    async def list_events(self, info_hash: str, limit: int = 50) -> list[Event]:
        async with (
            self._lock,
            self._conn.execute(
                "SELECT * FROM events WHERE hash=? ORDER BY id DESC LIMIT ?",
                (info_hash, limit),
            ) as cur,
        ):
            return [
                Event(
                    id=r["id"],
                    hash=r["hash"],
                    at=_dec(r["at"]),
                    from_state=TaskState(r["from_state"]) if r["from_state"] else None,
                    to_state=TaskState(r["to_state"]),
                    reason=r["reason"],
                )
                for r in await cur.fetchall()
            ]

    # ------------------------------------------------------------ kv 与 lease

    async def kv_get(self, key: str) -> str | None:
        async with (
            self._lock,
            self._conn.execute("SELECT value FROM kv WHERE key=?", (key,)) as cur,
        ):
            r = await cur.fetchone()
        return None if r is None else str(r["value"])

    async def kv_set(self, key: str, value: str) -> None:
        async with self._tx() as conn:
            await conn.execute("INSERT OR REPLACE INTO kv(key, value) VALUES (?,?)", (key, value))

    @staticmethod
    async def _read_lease(conn: aiosqlite.Connection) -> tuple[str, datetime] | None:
        async with conn.execute("SELECT value FROM kv WHERE key=?", (_LEASE_KEY,)) as cur:
            r = await cur.fetchone()
        if r is None:
            return None
        data = json.loads(r["value"])
        return str(data["owner"]), _dec(data["expires_at"])

    @staticmethod
    async def _write_lease(
        conn: aiosqlite.Connection, owner: str, now: datetime, ttl_seconds: int
    ) -> None:
        value = json.dumps(
            {"owner": owner, "expires_at": _enc(now + timedelta(seconds=ttl_seconds))}
        )
        await conn.execute(
            "INSERT OR REPLACE INTO kv(key, value) VALUES (?,?)", (_LEASE_KEY, value)
        )

    async def acquire_lease(self, owner: str, now: datetime, ttl_seconds: int) -> bool:
        _enc(now)
        async with self._tx() as conn:
            cur = await self._read_lease(conn)
            if cur is not None and cur[0] != owner and cur[1] > now:
                return False
            await self._write_lease(conn, owner, now, ttl_seconds)
            return True

    async def renew_lease(self, owner: str, now: datetime, ttl_seconds: int) -> bool:
        _enc(now)
        async with self._tx() as conn:
            cur = await self._read_lease(conn)
            if cur is None or cur[0] != owner or cur[1] <= now:
                return False
            await self._write_lease(conn, owner, now, ttl_seconds)
            return True

    async def release_lease(self, owner: str) -> None:
        async with self._tx() as conn:
            cur = await self._read_lease(conn)
            if cur is not None and cur[0] == owner:
                await conn.execute("DELETE FROM kv WHERE key=?", (_LEASE_KEY,))
