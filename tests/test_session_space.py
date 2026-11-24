# ruff: noqa: F811
"""会话层：115 网盘空间（get_space）与 lease 每次启动新 id。夹具复用 ``test_session.py``。"""

from __future__ import annotations

import asyncio
import os
import re
import socket
from pathlib import Path
from typing import Any, cast

import pytest

from bridge.cloud.protocol import NetworkError, OfflineStat, RiskControl, ServerError
from bridge.cloud.session import COOKIE_FILENAME, LeaseHeld, Session, SessionState
from bridge.config import CloudConfig, PathsConfig, Settings
from bridge.db.repo import TaskRepo
from bridge.db.sqlite import SqliteTaskRepo
from bridge.testing.clock import FakeClock
from test_session import (  # noqa: F401  (夹具)
    GIB,
    SESSION_SRC,
    Backend,
    Env,
    FakeNotifier,
    NoopLock,
    env,
    index_info,
    make_p115_cls,
    perr,
    task_page,
)

# ---------------------------------------------------------------- 115 网盘空间


async def test_get_space_parses_and_caches_five_minutes(env: Env) -> None:
    s = await env.make()
    env.backend.script("fs_index_info", index_info(2048, 1500))
    space = await s.get_space()
    assert (space.total, space.remaining) == (2048 * GIB, 1500 * GIB)
    assert env.backend.calls[-1][1] == (0,)  # count_space_nums=0
    await env.clock.advance(299)
    assert await s.get_space() == space
    assert env.backend.count("fs_index_info") == 1
    await env.clock.advance(2)
    await s.get_space()
    assert env.backend.count("fs_index_info") == 2
    assert s.status().space == space


async def test_status_space_is_none_before_first_read(env: Env) -> None:
    s = await env.make()
    assert s.status().space is None


async def test_get_space_falls_back_to_total_minus_used(env: Env) -> None:
    s = await env.make()
    resp = index_info(100, 40)
    del resp["data"]["space_info"]["all_remain"]
    env.backend.script("fs_index_info", resp)
    assert (await s.get_space()).remaining == 40 * GIB


async def test_get_space_accepts_float_sizes(env: Env) -> None:
    s = await env.make()
    resp = index_info()
    resp["data"]["space_info"]["all_total"]["size"] = 2.0 * 1024**4
    resp["data"]["space_info"]["all_remain"]["size"] = 1.5 * 1024**4
    env.backend.script("fs_index_info", resp)
    space = await s.get_space()
    assert (space.total, space.remaining) == (2 * 1024**4, int(1.5 * 1024**4))


@pytest.mark.parametrize(
    "resp",
    [
        {"state": True, "data": {}},
        {"state": True},
        {"state": True, "data": {"space_info": {"all_total": {"size": 5}}}},
        {"state": True, "data": {"space_info": {"all_total": {}, "all_remain": {}}}},
    ],
)
async def test_get_space_unexpected_structure_is_server_error(env: Env, resp: Any) -> None:
    s = await env.make()
    env.backend.script("fs_index_info", resp)
    with pytest.raises(ServerError):
        await s.get_space()
    assert s.status().space is None


async def test_get_space_uses_common_classification_and_pause(env: Env) -> None:
    s = await env.make()
    env.backend.script("fs_index_info", perr(590075))
    with pytest.raises(RiskControl):
        await s.get_space()
    assert s.state is SessionState.CIRCUIT_OPEN
    before = env.backend.count("fs_index_info")
    with pytest.raises(RiskControl):  # 熔断期快速失败，不发请求
        await s.get_space()
    assert env.backend.count("fs_index_info") == before


async def test_get_space_times_out_as_network_error(env: Env) -> None:
    s = await env.make()

    async def hang(*a: Any, **k: Any) -> Any:
        await asyncio.sleep(3600)

    env.backend.script("fs_index_info", hang)
    task = asyncio.create_task(s.get_space())
    await env.clock.settle()
    await env.clock.advance(31)
    with pytest.raises(NetworkError):
        await task


async def test_get_space_goes_through_token_bucket(env: Env) -> None:
    s = await env.make(rate_per_second=1, burst=1)
    env.backend.script("fs_index_info", index_info())
    await s.get_space()  # 用掉唯一的令牌
    await env.clock.advance(300)  # 缓存刚好过期，令牌已补满
    await s.get_space()
    assert env.backend.count("fs_index_info") == 2


async def test_space_low_alerts_and_recovery_notice(env: Env) -> None:
    s = await env.make()  # 默认阈值 100 GiB
    env.backend.script("fs_index_info", index_info(2048, 50))
    await s.get_space()
    assert env.notifier.alert_kinds() == ["cloud-space"]
    assert s.state is SessionState.OK  # 只告警，不暂停
    await env.clock.advance(301)
    env.backend.script("fs_index_info", index_info(2048, 500))
    await s.get_space()
    assert [k for k, _ in env.notifier.recoveries] == ["cloud-space"]
    await env.clock.advance(301)
    await s.get_space()
    assert [k for k, _ in env.notifier.recoveries] == ["cloud-space"]  # 不重复发恢复


async def test_space_equal_to_threshold_does_not_alert(env: Env) -> None:
    s = await env.make(space_min_gib=100)
    env.backend.script("fs_index_info", index_info(2048, 100))
    await s.get_space()
    assert env.notifier.alerts == []


async def test_space_threshold_comes_from_settings(env: Env) -> None:
    settings = Settings(
        cloud=CloudConfig(space_min_gib=1500), paths=PathsConfig(state=str(env.tmp))
    )
    s = Session.from_settings(
        settings,
        cast(TaskRepo, env.repo),
        env.notifier,
        env.clock,
        p115_cls=make_p115_cls(env.backend),
        lock_backend=NoopLock(),
        auto_qr=False,
    )
    env.sessions.append(s)
    env.cookie_path.write_text("UID=x; CID=c")
    await s.open()
    env.backend.script("fs_index_info", index_info(2048, 1400))
    await s.get_space()
    assert env.notifier.alert_kinds() == ["cloud-space"]


async def test_heartbeat_refreshes_space_independently(env: Env) -> None:
    s = await env.make()
    env.backend.script("clouddownload_task_list", task_page())
    env.backend.script("fs_index_info", index_info(2048, 1000))
    await env.clock.advance(301)
    assert await s.heartbeat_once() is True
    assert env.backend.count("fs_index_info") == 1
    assert s.status().space is not None
    # 之后离线列表在 60 秒前刚成功（心跳跳过它），空间缓存已满 5 分钟，仍要刷新
    await env.clock.advance(240)
    await s.list_offline(OfflineStat.DONE)
    await env.clock.advance(60)
    assert await s.heartbeat_once() is False
    assert env.backend.count("fs_index_info") == 2


async def test_heartbeat_space_failure_does_not_raise(env: Env) -> None:
    s = await env.make()
    env.backend.script("clouddownload_task_list", task_page())
    env.backend.script("fs_index_info", ServerError("boom"))
    await env.clock.advance(301)
    assert await s.heartbeat_once() is True
    assert s.status().space is None


async def test_heartbeat_skips_space_while_paused(env: Env) -> None:
    s = await env.make()
    env.backend.script("clouddownload_task_list", perr(590075))
    with pytest.raises(RiskControl):
        await s.list_offline(OfflineStat.RUNNING)
    await env.clock.advance(30)
    await s.heartbeat_once()
    assert env.backend.count("fs_index_info") == 0


def test_space_query_uses_only_read_endpoint() -> None:
    assert "fs_index_info" in SESSION_SRC
    assert re.search(r"client\.\w*(delete|remove)\w*\(", SESSION_SRC) is None


# ---------------------------------------------------------------- lease：每次启动新 id


async def test_default_owner_is_random_per_instance(env: Env) -> None:
    def make() -> Session:
        return Session(
            state_dir=env.tmp,
            repo=cast(TaskRepo, env.repo),
            notifier=env.notifier,
            clock=env.clock,
        )

    a, b = make(), make()
    assert a._owner != b._owner
    assert re.fullmatch(r"[0-9a-f]{32}", a._owner)
    assert socket.gethostname() not in a._owner
    assert str(os.getpid()) not in a._owner.split(":")


def _sqlite_session(
    repo: SqliteTaskRepo, tmp_path: Path, name: str, clock: FakeClock, backend: Backend
) -> Session:
    d = tmp_path / name
    d.mkdir(exist_ok=True)
    (d / COOKIE_FILENAME).write_text("UID=x; CID=c; SEID=s")
    return Session(
        state_dir=d,
        repo=repo,
        notifier=FakeNotifier(),
        clock=clock,
        p115_cls=make_p115_cls(backend),
        lock_backend=NoopLock(),
        auto_qr=False,
    )


async def test_two_sessions_same_repo_mutually_exclusive(tmp_path: Path) -> None:
    """同 hostname、同 pid 的两个 Session 共用仓储：后一个拿不到 lease，
    前一个正常关闭后后一个立即取得。"""
    repo = await SqliteTaskRepo.open(":memory:")
    try:
        clock, backend = FakeClock(), Backend()
        first = _sqlite_session(repo, tmp_path, "a", clock, backend)
        second = _sqlite_session(repo, tmp_path, "b", clock, backend)
        await first.open()
        with pytest.raises(LeaseHeld):
            await second.open()
        await first.close()
        await second.open()  # 正常关闭已释放，立即取得
        await second.close()
    finally:
        await repo.close()


async def test_killed_owner_lease_expires_after_ttl(tmp_path: Path) -> None:
    """被强杀（没走 close）后，新进程要等 90 秒 TTL 过期才能取得。"""
    repo = await SqliteTaskRepo.open(":memory:")
    try:
        clock, backend = FakeClock(), Backend()
        dead = _sqlite_session(repo, tmp_path, "a", clock, backend)
        await dead.open()  # 不 close，模拟被强杀
        survivor = _sqlite_session(repo, tmp_path, "b", clock, backend)
        await clock.advance(89)
        with pytest.raises(LeaseHeld):
            await survivor.open()
        await clock.advance(2)
        await survivor.open()
        await survivor.close()
    finally:
        await repo.close()
