"""会话层测试：mock p115client，不访问网络，用 FakeClock。"""

from __future__ import annotations

import asyncio
import inspect
import logging
import os
import re
import ssl
import sys
import urllib.error
from datetime import datetime
from pathlib import Path
from typing import Any, cast

import pytest
from p115client import P115Client, check_response
from p115client.exception import P115DataError

from bridge.cloud import session as session_mod
from bridge.cloud.protocol import (
    AuthExpired,
    CloudClient,
    CloudError,
    CloudNotFound,
    NetworkError,
    OfflineStat,
    QrStatus,
    QuotaExhausted,
    RiskControl,
    ServerError,
    SubmitRejected,
    VerifyRequired,
)
from bridge.cloud.session import (
    COOKIE_FILENAME,
    AlreadyRunning,
    ErrorKind,
    LeaseHeld,
    ProcessLock,
    Session,
    SessionState,
    classify_exception,
)
from bridge.db.repo import TaskRepo
from bridge.testing.clock import FakeClock

SESSION_SRC = Path(session_mod.__file__).read_text(encoding="utf-8")
SECRET = "SECRETCOOKIEVALUE"
HASH = "dd8255ecdc7ca55fb0bbf81323d87062db1f6d1c"
MAGNET = f"magnet:?xt=urn:btih:{HASH}&dn=Big+Buck+Bunny"

# ---------------------------------------------------------------- 假件


def perr(code: int, msg: str = "m") -> Exception:
    """用真实的 check_response 造出 p115client 抛的异常。"""
    try:
        check_response({"state": False, "errno": code, "error": msg})
    except Exception as exc:  # noqa: BLE001
        return exc
    raise AssertionError("check_response did not raise")


def http_error(code: int) -> urllib.error.HTTPError:
    return urllib.error.HTTPError("https://115.test/x", code, "msg", {}, None)  # type: ignore[arg-type]


def task_page(
    tasks: list[dict[str, Any]] | None = None, quota: int = 1250, total: int = 1570
) -> dict[str, Any]:
    return {
        "state": True,
        "tasks": tasks if tasks is not None else [],
        "count": len(tasks or []),
        "page_count": 1,
        "page": 1,
        "quota": quota,
        "total": total,
    }


def raw_task(**over: Any) -> dict[str, Any]:
    base: dict[str, Any] = {
        "info_hash": HASH.upper(),
        "name": "Big Buck Bunny",
        "status": 2,
        "percentDone": 100,
        "file_id": "3333",
        "wp_path_id": 4444,
        "del_path": "Big Buck Bunny",
        "size": 1000,
    }
    base.update(over)
    return base


GIB = 1024**3


def index_info(total_gib: float = 2048, remain_gib: float = 1500, **over: Any) -> dict[str, Any]:
    """``/files/index_info`` 响应里与空间有关的部分。"""
    info: dict[str, Any] = {
        "all_total": {"size": int(total_gib * GIB), "size_format": "2TB"},
        "all_remain": {"size": int(remain_gib * GIB), "size_format": "1.5TB"},
        "all_use": {"size": int((total_gib - remain_gib) * GIB), "size_format": "0.5TB"},
    }
    info.update(over)
    return {"state": True, "data": {"space_info": info}}


class Backend:
    """按方法名脚本化响应；列表里最后一项会一直重复。项可以是 dict、异常或可调用对象。"""

    def __init__(self) -> None:
        self.scripts: dict[str, list[Any]] = {}
        self.calls: list[tuple[str, tuple[Any, ...], dict[str, Any]]] = []
        self.scripts["fs_index_info"] = [index_info()]  # 心跳顺带读网盘空间

    def script(self, name: str, *items: Any) -> None:
        self.scripts[name] = list(items)

    def count(self, name: str) -> int:
        return sum(1 for n, _, _ in self.calls if n == name)

    @property
    def api_calls(self) -> int:
        return sum(1 for n, _, _ in self.calls if not n.startswith(("login_qrcode", "__")))

    def respond(self, name: str, *args: Any, **kwargs: Any) -> Any:
        self.calls.append((name, args, kwargs))
        queue = self.scripts.get(name)
        if not queue:
            raise AssertionError(f"unscripted call: {name}")
        item = queue.pop(0) if len(queue) > 1 else queue[0]
        if callable(item):
            item = item(*args, **kwargs)
        if isinstance(item, BaseException):
            raise item
        return item


def make_p115_cls(backend: Backend) -> Any:
    """假的 P115Client：每次调用都用真实方法的签名校验参数（bind），防止传错参数名。"""

    async def call(name: str, has_self: bool, *a: Any, **k: Any) -> Any:
        sig = inspect.signature(getattr(P115Client, name))
        if has_self:
            sig.bind(None, *a, **k)
        else:
            sig.bind(*a, **k)
        result = backend.respond(name, *a, **k)
        if inspect.isawaitable(result):
            result = await result
        return result

    class FakeP115:
        def __init__(self, cookies: Any = None) -> None:
            if isinstance(cookies, dict):
                self.cookies_str = "; ".join(f"{k}={v}" for k, v in cookies.items())
            else:
                self.cookies_str = str(cookies or "")
            backend.calls.append(("__init__", (), {}))

        def __getattr__(self, name: str) -> Any:
            if name.startswith("login_another") or "delete" in name or name.endswith("_del"):
                raise AssertionError(f"forbidden call: {name}")
            raise AttributeError(name)

        async def clouddownload_task_list(self, *a: Any, **k: Any) -> Any:
            return await call("clouddownload_task_list", True, *a, **k)

        async def clouddownload_task_add_urls(self, *a: Any, **k: Any) -> Any:
            return await call("clouddownload_task_add_urls", True, *a, **k)

        async def fs_makedirs_app(self, *a: Any, **k: Any) -> Any:
            return await call("fs_makedirs_app", True, *a, **k)

        async def fs_dir_getid(self, *a: Any, **k: Any) -> Any:
            return await call("fs_dir_getid", True, *a, **k)

        async def fs_file_skim(self, *a: Any, **k: Any) -> Any:
            return await call("fs_file_skim", True, *a, **k)

        async def fs_index_info(self, *a: Any, **k: Any) -> Any:
            return await call("fs_index_info", True, *a, **k)

        async def fs_files_app(self, *a: Any, **k: Any) -> Any:
            return await call("fs_files_app", True, *a, **k)

        async def download_url(self, *a: Any, **k: Any) -> Any:
            return await call("download_url", True, *a, **k)

        async def login_status(self, *a: Any, **k: Any) -> Any:
            return await call("login_status", True, *a, **k)

        @staticmethod
        async def login_qrcode_token(*a: Any, **k: Any) -> Any:
            return await call("login_qrcode_token", False, *a, **k)

        @staticmethod
        async def login_qrcode_scan_status(*a: Any, **k: Any) -> Any:
            return await call("login_qrcode_scan_status", False, *a, **k)

        @staticmethod
        async def login_qrcode_scan_result(*a: Any, **k: Any) -> Any:
            return await call("login_qrcode_scan_result", False, *a, **k)

    return FakeP115


class FakeNotifier:
    def __init__(self) -> None:
        self.alerts: list[tuple[str, str]] = []
        self.qrs: list[tuple[bytes, str]] = []
        self.recoveries: list[tuple[str, str]] = []

    async def alert(self, kind: str, text: str) -> bool:
        self.alerts.append((kind, text))
        return True

    async def send_qr(self, png: bytes, caption: str) -> bool:
        self.qrs.append((png, caption))
        return True

    async def recovered(self, kind: str, text: str) -> bool:
        self.recoveries.append((kind, text))
        return True

    def alert_kinds(self) -> list[str]:
        return [k for k, _ in self.alerts]


class FakeRepo:
    """只实现 lease 与 kv 的那部分仓储。"""

    def __init__(self) -> None:
        self.holder: str | None = None
        self.acquire_ok = True
        self.renew_ok = True
        self.released: list[str] = []

    async def acquire_lease(self, owner: str, now: datetime, ttl_seconds: int) -> bool:
        if not self.acquire_ok:
            return False
        self.holder = owner
        return True

    async def renew_lease(self, owner: str, now: datetime, ttl_seconds: int) -> bool:
        return self.renew_ok and self.holder == owner

    async def release_lease(self, owner: str) -> None:
        self.released.append(owner)
        self.holder = None

    async def kv_get(self, key: str) -> str | None:
        return None

    async def kv_set(self, key: str, value: str) -> None:
        return None


class NoopLock:
    def lock(self, fd: int) -> None:
        return None

    def unlock(self, fd: int) -> None:
        return None


class Env:
    def __init__(self, tmp_path: Path) -> None:
        self.tmp = tmp_path
        self.clock = FakeClock()
        self.backend = Backend()
        self.notifier = FakeNotifier()
        self.repo = FakeRepo()
        self.sessions: list[Session] = []

    @property
    def cookie_path(self) -> Path:
        return self.tmp / COOKIE_FILENAME

    async def make(
        self,
        *,
        cookie: bool = True,
        auto_qr: bool = False,
        open_: bool = True,
        **kwargs: Any,
    ) -> Session:
        kwargs.setdefault("rate_per_second", 1000)
        kwargs.setdefault("burst", 1000)
        if cookie:
            self.cookie_path.write_text(f"UID={SECRET}; CID=c; SEID=s; KID=k")
        session = Session(
            state_dir=self.tmp,
            repo=cast(TaskRepo, self.repo),
            notifier=self.notifier,
            clock=self.clock,
            p115_cls=make_p115_cls(self.backend),
            lock_backend=NoopLock(),
            owner="test-owner",
            auto_qr=auto_qr,
            **kwargs,
        )
        self.sessions.append(session)
        if open_:
            await session.open()
        return session


@pytest.fixture
async def env(tmp_path: Path) -> Any:
    e = Env(tmp_path)
    yield e
    for s in e.sessions:
        await s.close()


# ---------------------------------------------------------------- 分类表


@pytest.mark.parametrize(
    ("code", "kind"),
    [
        (911, ErrorKind.VERIFY),
        (99, ErrorKind.AUTH),
        (990001, ErrorKind.AUTH),
        (40101017, ErrorKind.AUTH),
        (40101032, ErrorKind.AUTH),
        (40101035, ErrorKind.AUTH),  # 不在 p115client 的 match 里，只能靠码
        (40140125, ErrorKind.AUTH),
        (590075, ErrorKind.RISK),
        (40140117, ErrorKind.RISK),
        (990005, ErrorKind.BUSY),
        (990009, ErrorKind.BUSY),
        (990019, ErrorKind.BUSY),
        (51012, ErrorKind.BUSY),
        (10008, ErrorKind.EXISTS),
        (10010, ErrorKind.QUOTA),
        (91005, ErrorKind.QUOTA),
        (2, ErrorKind.QUOTA),
        (10004, ErrorKind.REJECT),
        (50038, ErrorKind.REJECT),
        (20018, ErrorKind.NOT_FOUND),
        (1001, ErrorKind.PARAM),
        (990002, ErrorKind.PARAM),
        (40100000, ErrorKind.PARAM),
        (980006, ErrorKind.PARAM),
        (987654321, ErrorKind.SERVER),  # 未知码
        (990003, ErrorKind.SERVER),
    ],
)
def test_classify_by_code(code: int, kind: ErrorKind) -> None:
    c = classify_exception(perr(code))
    assert c.kind is kind
    assert c.code == code


def test_911_is_not_auth_even_though_class_is_authentication_error() -> None:
    from p115client.exception import P115AuthenticationError

    exc = perr(911)
    assert isinstance(exc, P115AuthenticationError)  # 前提：p115client 把 911 归为认证异常
    assert classify_exception(exc).kind is ErrorKind.VERIFY


def test_http_status_classification() -> None:
    assert classify_exception(http_error(405)).kind is ErrorKind.RISK
    assert classify_exception(http_error(502)).kind is ErrorKind.SERVER
    c = classify_exception(http_error(404))
    assert c.kind is ErrorKind.SERVER
    assert c.http_status == 404


def test_http_error_is_not_mistaken_for_network_oserror() -> None:
    # HTTPError 是 OSError 的子类，必须先于通用 OSError 判定
    assert isinstance(http_error(405), OSError)
    assert classify_exception(http_error(405)).kind is ErrorKind.RISK


def test_network_errors() -> None:
    from urllib3_future.exceptions import ProtocolError

    for exc in (
        TimeoutError("t"),
        ConnectionResetError("c"),
        ssl.SSLError("s"),
        ProtocolError("p"),
    ):
        assert classify_exception(exc).kind is ErrorKind.NETWORK, exc


def test_class_fallback_without_code() -> None:
    from p115client.exception import P115AuthenticationError, P115BusyOSError

    assert classify_exception(P115AuthenticationError(137, "no code")).kind is ErrorKind.AUTH
    assert classify_exception(P115BusyOSError(16, "no code")).kind is ErrorKind.BUSY
    assert classify_exception(P115DataError(61, b"<html>")).kind is ErrorKind.UNPARSEABLE
    assert classify_exception(ValueError("weird")).kind is ErrorKind.SERVER
    assert classify_exception(KeyError("x")).kind is ErrorKind.SERVER


def test_code_wins_over_class() -> None:
    # 590075 是 P115BusyOSError，但必须判为风控而不是「忙」
    from p115client.exception import P115BusyOSError

    exc = perr(590075)
    assert isinstance(exc, P115BusyOSError)
    assert classify_exception(exc).kind is ErrorKind.RISK
    assert classify_exception(perr(990005)).kind is ErrorKind.BUSY


def test_posix_errno_is_not_used_as_115_code() -> None:
    # 异常的 .errno 是 POSIX 码（EAUTH=137 等），不能当 115 码
    exc = perr(987654321)
    assert classify_exception(exc).code == 987654321


# ---------------------------------------------------------------- 接口形状


def test_session_satisfies_cloud_client_protocol(env: Env) -> None:
    assert issubclass(Session, CloudClient)
    for name in (
        "list_offline",
        "add_magnet",
        "makedirs",
        "get_entry",
        "list_dir",
        "download_link",
        "qr_start",
        "qr_poll",
        "qr_finish",
    ):
        assert callable(getattr(Session, name))


# ---------------------------------------------------------------- list_offline


async def test_list_offline_parses_web_layout(env: Env) -> None:
    s = await env.make()
    env.backend.script("clouddownload_task_list", task_page([raw_task()]))
    page = await s.list_offline(OfflineStat.RUNNING)
    assert page.quota.remaining == 1250 and page.quota.total == 1570
    t = page.tasks[0]
    assert t.hash == HASH  # 小写
    assert t.file_id == "3333" and t.wp_path_id == "4444"  # id 一律 str
    assert t.is_done
    _, args, kwargs = env.backend.calls[-1]
    assert args[0] == {"page": 1, "stat": 12}
    assert kwargs["type"] == "web"
    assert s.status().quota == page.quota


async def test_list_offline_parses_ssp_layout(env: Env) -> None:
    s = await env.make()
    body = {"state": True, "data": task_page([raw_task(file_id="")])}
    env.backend.script("clouddownload_task_list", body)
    page = await s.list_offline(OfflineStat.RUNNING)
    assert page.tasks[0].file_id is None
    assert not page.tasks[0].is_done
    assert page.quota.total == 1570


async def test_list_offline_unexpected_structure_is_server_error(env: Env) -> None:
    s = await env.make()
    env.backend.script("clouddownload_task_list", {"state": True, "tasks": ["not-a-dict"]})
    with pytest.raises(ServerError):
        await s.list_offline(OfflineStat.RUNNING)
    assert s.state is SessionState.OK


async def test_list_offline_cache_20_seconds_per_stat_and_page(env: Env) -> None:
    s = await env.make()
    env.backend.script("clouddownload_task_list", task_page([raw_task()]))
    await s.list_offline(OfflineStat.RUNNING)
    await s.list_offline(OfflineStat.RUNNING)
    assert env.backend.count("clouddownload_task_list") == 1
    await s.list_offline(OfflineStat.DONE)  # 不同 stat 不共用
    await s.list_offline(OfflineStat.RUNNING, page=2)  # 不同 page 不共用
    assert env.backend.count("clouddownload_task_list") == 3
    await env.clock.advance(19)
    await s.list_offline(OfflineStat.RUNNING)
    assert env.backend.count("clouddownload_task_list") == 3
    await env.clock.advance(1.5)
    await s.list_offline(OfflineStat.RUNNING)
    assert env.backend.count("clouddownload_task_list") == 4


async def test_cache_returns_independent_copy(env: Env) -> None:
    s = await env.make()
    env.backend.script("clouddownload_task_list", task_page([raw_task()]))
    first = await s.list_offline(OfflineStat.RUNNING)
    first.tasks.clear()
    second = await s.list_offline(OfflineStat.RUNNING)
    assert len(second.tasks) == 1


async def test_errors_are_not_cached(env: Env) -> None:
    s = await env.make()
    env.backend.script("clouddownload_task_list", TimeoutError("t"), task_page())
    with pytest.raises(NetworkError):
        await s.list_offline(OfflineStat.RUNNING)
    await s.list_offline(OfflineStat.RUNNING)
    assert env.backend.count("clouddownload_task_list") == 2


# ---------------------------------------------------------------- 令牌桶接入


async def test_api_calls_go_through_token_bucket(env: Env) -> None:
    s = await env.make(rate_per_second=1, burst=3)
    env.backend.script("clouddownload_task_list", task_page())
    tasks = [asyncio.create_task(s.list_offline(OfflineStat.RUNNING, page=p)) for p in range(1, 5)]
    await env.clock.settle()
    assert sum(t.done() for t in tasks) == 3  # 突发 3 个
    await env.clock.advance(1.1)
    assert all(t.done() for t in tasks)


async def test_download_link_goes_through_token_bucket(env: Env) -> None:
    s = await env.make(rate_per_second=1, burst=3)
    env.backend.script("clouddownload_task_list", task_page())
    env.backend.script("download_url", _link("https://cdn.test/f?t=1"))
    for p in range(1, 4):
        await s.list_offline(OfflineStat.RUNNING, page=p)
    t = asyncio.create_task(s.download_link("pc1"))
    await env.clock.settle()
    assert not t.done()
    await env.clock.advance(1.1)
    assert t.done()


# ---------------------------------------------------------------- 熔断与暂停


async def test_risk_trips_circuit_and_fast_fails_without_requests(env: Env) -> None:
    s = await env.make()
    env.backend.script("clouddownload_task_list", perr(590075))
    with pytest.raises(RiskControl):
        await s.list_offline(OfflineStat.RUNNING)
    assert s.state is SessionState.CIRCUIT_OPEN
    assert "risk" in env.notifier.alert_kinds()
    before = len(env.backend.calls)
    # 熔断期间所有方法都快速失败，不发请求
    with pytest.raises(RiskControl):
        await s.list_offline(OfflineStat.DONE)
    with pytest.raises(RiskControl):
        await s.add_magnet(MAGNET, "1")
    with pytest.raises(RiskControl):
        await s.makedirs("/a")
    with pytest.raises(RiskControl):
        await s.get_entry("1")
    with pytest.raises(RiskControl):
        await s.list_dir("1")
    with pytest.raises(RiskControl):
        await s.download_link("pc")
    assert len(env.backend.calls) == before


async def test_http_405_trips_circuit_and_backoff_doubles(env: Env) -> None:
    s = await env.make(burst=100, rate_per_second=100)
    env.backend.script("clouddownload_task_list", http_error(405))
    with pytest.raises(RiskControl):
        await s.list_offline(OfflineStat.RUNNING)
    st = s.status()
    assert st.circuit_level == 1 and st.circuit_until is not None
    assert (st.circuit_until - env.clock.now()).total_seconds() == pytest.approx(60)
    await env.clock.advance(61)
    assert s.state is SessionState.OK  # 熔断期过，下一次调用就是探测
    with pytest.raises(RiskControl):
        await s.list_offline(OfflineStat.RUNNING)  # 探测再失败
    st = s.status()
    assert st.circuit_level == 2
    assert (st.circuit_until - env.clock.now()).total_seconds() == pytest.approx(120)  # type: ignore[operator]


async def test_circuit_level_resets_only_after_ten_clean_minutes(env: Env) -> None:
    s = await env.make()
    env.backend.script("clouddownload_task_list", perr(590075), task_page())
    with pytest.raises(RiskControl):
        await s.list_offline(OfflineStat.RUNNING)
    await env.clock.advance(61)
    await s.list_offline(OfflineStat.RUNNING)  # 熔断期过后立即恢复请求
    assert s.state is SessionState.OK
    assert s.status().circuit_level == 1  # 但等级没有复位
    assert env.notifier.recoveries == []  # 恢复通知只在复位时发
    await env.clock.advance(500)
    await s.list_offline(OfflineStat.RUNNING)
    assert s.status().circuit_level == 1 and env.notifier.recoveries == []
    await env.clock.advance(100)  # 自熔断期结束起满 10 分钟
    await s.list_offline(OfflineStat.RUNNING)
    assert s.status().circuit_level == 0
    assert [k for k, _ in env.notifier.recoveries] == ["risk"]
    # 复位后再触发又从 1 分钟起
    env.backend.script("clouddownload_task_list", perr(590075))
    await env.clock.advance(21)
    with pytest.raises(RiskControl):
        await s.list_offline(OfflineStat.RUNNING)
    assert (s.status().circuit_until - env.clock.now()).total_seconds() == pytest.approx(60)  # type: ignore[operator]


async def test_intermittent_trips_keep_doubling_then_reset_with_one_notice(env: Env) -> None:
    s = await env.make()
    risk, ok = perr(590075), task_page()
    durations: list[float] = []
    for _ in range(3):  # 每次熔断期过后成功一次，但不足 10 分钟又触发
        env.backend.script("clouddownload_task_list", risk)
        with pytest.raises(RiskControl):
            await s.list_offline(OfflineStat.RUNNING)
        until = s.status().circuit_until
        assert until is not None
        durations.append((until - env.clock.now()).total_seconds())
        await env.clock.advance(durations[-1] + 1)
        env.backend.script("clouddownload_task_list", ok)
        await s.list_offline(OfflineStat.RUNNING)
        await env.clock.advance(21)  # 越过 20 秒缓存
    assert durations == [60, 120, 240]
    assert s.status().circuit_level == 3 and env.notifier.recoveries == []
    await env.clock.advance(600)
    await s.list_offline(OfflineStat.RUNNING)
    assert s.status().circuit_level == 0
    assert [k for k, _ in env.notifier.recoveries] == ["risk"]  # 只发一次


async def test_circuit_trip_does_not_touch_task_state_exceptions_are_global(env: Env) -> None:
    from bridge.cloud.protocol import GLOBAL_ERRORS

    s = await env.make()
    env.backend.script("clouddownload_task_list", perr(590075))
    with pytest.raises(RiskControl) as ei:
        await s.list_offline(OfflineStat.RUNNING)
    assert isinstance(ei.value, GLOBAL_ERRORS)


# ---------------------------------------------------------------- 认证失效与需人工验证


async def test_auth_expired_pauses_and_fast_fails(env: Env) -> None:
    s = await env.make()
    env.backend.script("clouddownload_task_list", perr(990001))
    with pytest.raises(AuthExpired):
        await s.list_offline(OfflineStat.RUNNING)
    assert s.state is SessionState.AUTH_EXPIRED
    assert env.notifier.alert_kinds() == ["auth"]
    before = len(env.backend.calls)
    with pytest.raises(AuthExpired):
        await s.add_magnet(MAGNET, "1")
    with pytest.raises(AuthExpired):
        await s.download_link("pc")
    assert len(env.backend.calls) == before


async def test_verify_required_pauses_without_qr(env: Env) -> None:
    s = await env.make(auto_qr=True)
    env.backend.script("clouddownload_task_list", perr(911))
    with pytest.raises(VerifyRequired):
        await s.list_offline(OfflineStat.RUNNING)
    await env.clock.advance(60)
    assert s.state is SessionState.VERIFY_REQUIRED
    assert env.notifier.alert_kinds() == ["verify"]
    assert env.notifier.qrs == []
    assert env.backend.count("login_qrcode_token") == 0  # 不进入等待扫码
    before = len(env.backend.calls)
    with pytest.raises(VerifyRequired):
        await s.list_offline(OfflineStat.DONE)
    assert len(env.backend.calls) == before


async def test_verify_required_probe_by_heartbeat_recovers(env: Env) -> None:
    s = await env.make()
    env.backend.script("clouddownload_task_list", perr(911), task_page())
    with pytest.raises(VerifyRequired):
        await s.list_offline(OfflineStat.RUNNING)
    assert await s.heartbeat_once() is True  # 探测穿过 verify 暂停
    assert s.state is SessionState.OK
    assert [k for k, _ in env.notifier.recoveries] == ["verify"]


async def test_unparseable_body_probes_login_status(env: Env) -> None:
    s = await env.make()
    env.backend.script("clouddownload_task_list", P115DataError(61, b"<html>"))
    env.backend.script("login_status", lambda **kw: kw["parse"](None, b'{"state": false}'))
    with pytest.raises(AuthExpired):
        await s.list_offline(OfflineStat.RUNNING)

    env2 = Env(env.tmp / "b")
    env2.tmp.mkdir()
    s2 = await env2.make()
    env2.backend.script("clouddownload_task_list", P115DataError(61, b"<html>"))
    env2.backend.script("login_status", lambda **kw: kw["parse"](None, b"<html>oops</html>"))
    with pytest.raises(RiskControl):  # 非 JSON 按风控
        await s2.list_offline(OfflineStat.RUNNING)
    await s2.close()


# ---------------------------------------------------------------- 网络、服务端、忙


async def test_network_error_keeps_state_and_alerts_only_after_30_minutes(env: Env) -> None:
    s = await env.make(burst=100, rate_per_second=100)
    env.backend.script("clouddownload_task_list", TimeoutError("t"))
    for _ in range(3):
        with pytest.raises(NetworkError):
            await s.list_offline(OfflineStat.RUNNING)
        await env.clock.advance(60)
    assert s.state is SessionState.OK
    assert env.notifier.alerts == []
    await env.clock.advance(30 * 60)
    with pytest.raises(NetworkError):
        await s.list_offline(OfflineStat.RUNNING)
    assert env.notifier.alert_kinds() == ["network"]
    # 恢复
    env.backend.script("clouddownload_task_list", task_page())
    await s.list_offline(OfflineStat.RUNNING)
    assert [k for k, _ in env.notifier.recoveries] == ["network"]
    assert s.status().degraded_since is None


async def test_server_error_5xx(env: Env) -> None:
    s = await env.make()
    env.backend.script("clouddownload_task_list", http_error(502))
    with pytest.raises(ServerError):
        await s.list_offline(OfflineStat.RUNNING)
    assert s.state is SessionState.OK
    assert env.notifier.alerts == []


async def test_other_4xx_is_server_error_with_alert(env: Env) -> None:
    s = await env.make()
    env.backend.script("clouddownload_task_list", http_error(403))
    with pytest.raises(ServerError):
        await s.list_offline(OfflineStat.RUNNING)
    assert env.notifier.alert_kinds() == ["api-4xx"]


async def test_unknown_code_is_server_error_no_pause(env: Env) -> None:
    s = await env.make()
    env.backend.script("clouddownload_task_list", perr(123456789))
    with pytest.raises(ServerError):
        await s.list_offline(OfflineStat.RUNNING)
    assert s.state is SessionState.OK


async def test_busy_retries_with_backoff_then_succeeds(env: Env) -> None:
    s = await env.make()
    env.backend.script("clouddownload_task_list", perr(990005), perr(990005), task_page())
    task = asyncio.create_task(s.list_offline(OfflineStat.RUNNING))
    await env.clock.settle()
    assert env.backend.count("clouddownload_task_list") == 1
    await env.clock.advance(1.0)  # 第一次退避 1 秒
    assert env.backend.count("clouddownload_task_list") == 2
    await env.clock.advance(2.0)  # 第二次退避 2 秒
    await task
    assert env.backend.count("clouddownload_task_list") == 3
    assert s.state is SessionState.OK


async def test_busy_exhausted_is_server_error_not_circuit(env: Env) -> None:
    s = await env.make()
    env.backend.script("clouddownload_task_list", perr(990005))
    task = asyncio.create_task(s.list_offline(OfflineStat.RUNNING))
    await env.clock.advance(1 + 2 + 4 + 1)
    with pytest.raises(ServerError):
        await task
    assert env.backend.count("clouddownload_task_list") == 4  # 1 次加 3 次重试
    assert s.state is SessionState.OK
    assert s.status().circuit_level == 0


# ---------------------------------------------------------------- add_magnet


def _add_ok(**item: Any) -> dict[str, Any]:
    base = {"state": True, "info_hash": HASH.upper()}
    base.update(item)
    return {"state": True, "data": {"result": [base]}}


async def test_add_magnet_success_uses_ssp_and_wp_path_id(env: Env) -> None:
    s = await env.make()
    env.backend.script("clouddownload_task_add_urls", _add_ok())
    assert await s.add_magnet(MAGNET, "4444") == HASH
    _, args, kwargs = env.backend.calls[-1]
    assert args[0] == {"url[0]": MAGNET, "wp_path_id": "4444"}
    assert kwargs["type"] == "ssp"


async def test_add_magnet_existing_task_is_success(env: Env) -> None:
    s = await env.make()
    item = {"state": False, "errcode": 10008, "error_msg": "任务已存在，请勿输入重复的链接地址。"}
    env.backend.script("clouddownload_task_add_urls", {"state": True, "data": {"result": [item]}})
    assert await s.add_magnet(MAGNET, "1") == HASH
    # 顶层 state 为假的形式也一样
    env.backend.script(
        "clouddownload_task_add_urls", {"state": False, "errno": 10008, "error": "任务已存在"}
    )
    assert await s.add_magnet(MAGNET, "1") == HASH


@pytest.mark.parametrize(
    ("code", "exc"),
    [
        (10004, SubmitRejected),
        (10010, QuotaExhausted),
        (91005, QuotaExhausted),
        (911, VerifyRequired),
        (990001, AuthExpired),
        (590075, RiskControl),
        (424242, ServerError),  # 未知码：不当作任务失败
    ],
)
async def test_add_magnet_error_codes(env: Env, code: int, exc: type[CloudError]) -> None:
    s = await env.make()
    item = {"state": False, "errcode": code, "error_msg": "x"}
    env.backend.script("clouddownload_task_add_urls", {"state": True, "data": {"result": [item]}})
    with pytest.raises(exc) as ei:
        await s.add_magnet(MAGNET, "1")
    if exc is SubmitRejected:
        assert ei.value.errcode == code  # type: ignore[attr-defined]


async def test_add_magnet_reject_code_is_server_error_outside_submit(env: Env) -> None:
    s = await env.make()
    env.backend.script("fs_makedirs_app", perr(10004))
    with pytest.raises(ServerError):
        await s.makedirs("/a")


async def test_add_magnet_invalid_magnet(env: Env) -> None:
    s = await env.make()
    with pytest.raises(SubmitRejected):
        await s.add_magnet("magnet:?dn=nohash", "1")
    assert env.backend.api_calls == 0


async def test_add_magnet_invalidates_list_cache(env: Env) -> None:
    s = await env.make()
    env.backend.script("clouddownload_task_list", task_page())
    env.backend.script("clouddownload_task_add_urls", _add_ok())
    await s.list_offline(OfflineStat.RUNNING)
    await s.add_magnet(MAGNET, "1")
    await s.list_offline(OfflineStat.RUNNING)
    assert env.backend.count("clouddownload_task_list") == 2


# ---------------------------------------------------------------- 目录与下载


async def test_makedirs_returns_str_id_and_strips_slash(env: Env) -> None:
    s = await env.make()
    env.backend.script("fs_makedirs_app", {"state": True, "cid": 12345})
    assert await s.makedirs("/115-bridge/radarr") == "12345"
    assert env.backend.calls[-1][1][0] == {"path": "115-bridge/radarr", "parent_id": 0}
    assert await s.makedirs("/") == "0"


async def test_makedirs_existing_dir_falls_back_to_getid(env: Env) -> None:
    s = await env.make()
    env.backend.script("fs_makedirs_app", perr_exists())
    env.backend.script("fs_dir_getid", {"state": True, "id": 777})
    assert await s.makedirs("/a/b") == "777"


def perr_exists() -> Exception:
    try:
        check_response({"state": False, "error": "目录名称已存在"})
    except Exception as exc:  # noqa: BLE001
        return exc
    raise AssertionError


def dir_page(
    cid: Any, rows: list[dict[str, Any]], offset: int = 0, count: int | None = None
) -> Any:
    return {
        "state": True,
        "cid": cid,
        "offset": offset,
        "count": len(rows) if count is None else count,
        "data": rows,
    }


async def test_list_dir_paginates_and_maps_entries(env: Env) -> None:
    s = await env.make()
    page1 = dir_page(
        1,
        [
            {"fc": "0", "fid": 10, "pid": 1, "fn": "Season 1"},
            {"fc": "1", "fid": 11, "pid": 1, "fn": "a.mkv", "fs": "100", "sha1": "AB", "pc": "pcA"},
        ],
    )
    env.backend.script("fs_files_app", page1)
    entries = await s.list_dir("1")
    assert [e.id for e in entries] == ["10", "11"]
    d, f = entries
    assert d.is_dir and d.size == 0 and d.sha1 is None and d.pickcode is None
    assert not f.is_dir and f.size == 100 and f.sha1 == "AB" and f.pickcode == "pcA"
    assert f.parent_id == "1"
    payload = env.backend.calls[-1][1][0]
    assert payload["cid"] == "1" and payload["record_open_time"] == 0


async def test_list_dir_multi_page_follows_count(env: Env, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(session_mod, "DIR_PAGE_SIZE", 2)
    s = await env.make()
    rows = [{"fc": "1", "fid": i, "pid": 1, "fn": f"{i}", "fs": 1, "sha1": "x"} for i in range(5)]
    env.backend.script(
        "fs_files_app",
        lambda payload, **k: dir_page(
            1, rows[payload["offset"] :][:2], offset=payload["offset"], count=5
        ),
    )
    entries = await s.list_dir("1")
    assert [e.id for e in entries] == ["0", "1", "2", "3", "4"]
    assert env.backend.count("fs_files_app") == 3


async def test_list_dir_short_page_does_not_end_listing(
    env: Env, monkeypatch: pytest.MonkeyPatch
) -> None:
    """翻页以响应的 count 为准：本页少于 limit 条也不能当作结束。"""
    monkeypatch.setattr(session_mod, "DIR_PAGE_SIZE", 10)
    s = await env.make()
    rows = [{"fc": "1", "fid": i, "pid": 1, "fn": f"{i}", "fs": 1, "sha1": "x"} for i in range(5)]
    env.backend.script(
        "fs_files_app",
        lambda payload, **k: dir_page(
            1, rows[payload["offset"] :][:3], offset=payload["offset"], count=5
        ),
    )
    assert len(await s.list_dir("1")) == 5
    assert env.backend.count("fs_files_app") == 2


async def test_list_dir_nonexistent_cid_returns_root_listing_is_not_found(env: Env) -> None:
    """115 对不存在或已删除的 cid 不报错而是返回根目录列表，必须靠核对 cid 识别。"""
    s = await env.make()
    root = dir_page(0, [{"fc": "0", "fid": 99, "pid": 0, "fn": "root-dir"}])
    env.backend.script("fs_files_app", root)
    with pytest.raises(CloudNotFound):
        await s.list_dir("123456")
    assert s.state is SessionState.OK
    # 根目录自己不做这个核对
    assert [e.id for e in await s.list_dir("0")] == ["99"]


async def test_list_dir_cid_checked_on_every_page(
    env: Env, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(session_mod, "DIR_PAGE_SIZE", 1)
    s = await env.make()
    row = {"fc": "1", "fid": 1, "pid": 5, "fn": "a", "fs": 1, "sha1": "x"}
    env.backend.script(
        "fs_files_app",
        dir_page("5", [row], offset=0, count=2),
        dir_page(0, [row], offset=1, count=2),  # 第二页中途目录被删，返回根目录
    )
    with pytest.raises(CloudNotFound):
        await s.list_dir("5")


async def test_list_dir_offset_mismatch_is_server_error(env: Env) -> None:
    s = await env.make()
    row = {"fc": "1", "fid": 1, "pid": 5, "fn": "a", "fs": 1, "sha1": "x"}
    env.backend.script("fs_files_app", dir_page(5, [row], offset=7, count=1))
    with pytest.raises(ServerError):
        await s.list_dir("5")


async def test_list_dir_not_found_code_is_cloud_not_found(env: Env) -> None:
    s = await env.make()
    env.backend.script("fs_files_app", perr(20018))
    with pytest.raises(CloudNotFound):
        await s.list_dir("5")
    assert s.state is SessionState.OK


async def test_list_dir_names_are_unescaped(env: Env) -> None:
    s = await env.make()
    env.backend.script(
        "fs_files_app", dir_page(1, [{"fc": "0", "fid": 3, "pid": 1, "fn": "[\x02128074]dir"}])
    )
    assert [e.name for e in await s.list_dir("1")] == ["\U0001f44adir"]


async def test_get_entry(env: Env) -> None:
    s = await env.make()
    env.backend.script(
        "fs_file_skim",
        {
            "state": True,
            "data": [
                {
                    "file_id": 5,
                    "file_name": "x.mkv",
                    "sha1": "AA",
                    "file_size": "9",
                    "pick_code": "p",
                }
            ],
        },
    )
    e = await s.get_entry("5")
    assert e is not None and e.id == "5" and not e.is_dir and e.size == 9 and e.sha1 == "AA"
    env.backend.script("fs_file_skim", {"state": False, "error": "文件不存在"})
    assert await s.get_entry("6") is None
    env.backend.script(
        "fs_file_skim",
        {"state": True, "data": [{"file_id": 8, "file_name": "[\x02128074]x", "sha1": "A"}]},
    )
    named = await s.get_entry("8")
    assert named is not None and named.name == "\U0001f44ax"
    assert (await s.get_entry("0")) is not None  # 根目录不发请求


class _Link(str):
    headers: dict[str, str]
    size: int | None


def _link(url: str, headers: dict[str, str] | None = None, size: int | None = None) -> _Link:
    link = _Link(url)
    link.headers = {"user-agent": "UA-from-115"} if headers is None else headers
    link.size = size
    return link


async def test_download_link_carries_user_agent(env: Env) -> None:
    s = await env.make(download_app="android")
    env.backend.script("download_url", _link("https://cdn.test/f?t=1", size=42))
    link = await s.download_link("pc1")
    assert link.url == "https://cdn.test/f?t=1"
    assert link.headers["user-agent"] == "UA-from-115"
    assert link.size == 42
    _, args, kwargs = env.backend.calls[-1]
    assert args == ("pc1",)
    assert kwargs["app"] == "android"
    assert kwargs["user_agent"]  # 显式传 UA，使取链接与下载请求的 UA 一致


async def test_download_link_without_ua_gets_the_one_used_for_request(env: Env) -> None:
    s = await env.make()
    env.backend.script("download_url", _link("https://cdn.test/f", headers={"user-agent": ""}))
    link = await s.download_link("pc1")
    assert link.headers["user-agent"] == session_mod.DOWNLOAD_USER_AGENT


async def test_download_link_never_cached(env: Env) -> None:
    s = await env.make()
    env.backend.script("download_url", _link("https://cdn.test/1"), _link("https://cdn.test/2"))
    a = await s.download_link("pc")
    b = await s.download_link("pc")
    assert a.url != b.url


@pytest.mark.parametrize("code", [20018, 50038, 50028])
async def test_download_link_missing_file_raises_cloud_not_found(env: Env, code: int) -> None:
    s = await env.make()
    env.backend.script("download_url", perr(code))
    with pytest.raises(CloudNotFound):
        await s.download_link("pc")
    assert s.state is SessionState.OK  # 任务级，不是全局暂停


async def test_50038_is_only_cloud_not_found_on_download_link(env: Env) -> None:
    s = await env.make()
    env.backend.script("fs_makedirs_app", perr(50038))
    with pytest.raises(ServerError):
        await s.makedirs("/a")


# ---------------------------------------------------------------- 心跳


async def test_heartbeat_skips_when_recent_success(env: Env) -> None:
    s = await env.make()
    env.backend.script("clouddownload_task_list", task_page())
    await s.list_offline(OfflineStat.RUNNING)
    await s.get_space()  # 空间缓存是新的，t=100 的心跳不会另发空间请求
    await env.clock.advance(100)
    assert await s.heartbeat_once() is False
    assert env.backend.count("clouddownload_task_list") == 1
    await env.clock.advance(201)
    assert await s.heartbeat_once() is True  # 距上次成功已超 5 分钟
    assert env.backend.count("clouddownload_task_list") == 2


async def test_heartbeat_loop_runs_every_five_minutes(env: Env) -> None:
    s = await env.make()
    env.backend.script("clouddownload_task_list", task_page())
    loop_task = asyncio.create_task(s.heartbeat_loop())
    await env.clock.settle()
    await env.clock.advance(299)
    assert env.backend.count("clouddownload_task_list") == 0
    await env.clock.advance(2)
    assert env.backend.count("clouddownload_task_list") == 1
    # 平时轮询在 5 分钟内成功过，下一次心跳跳过
    await env.clock.advance(100)
    await s.list_offline(OfflineStat.DONE)
    assert env.backend.count("clouddownload_task_list") == 2
    await env.clock.advance(200)
    assert env.backend.count("clouddownload_task_list") == 2
    await env.clock.advance(300)
    assert env.backend.count("clouddownload_task_list") == 3
    loop_task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await loop_task


async def test_heartbeat_failure_goes_through_same_classification(env: Env) -> None:
    s = await env.make()
    env.backend.script("clouddownload_task_list", perr(990001))
    await env.clock.advance(301)
    assert await s.heartbeat_once() is True  # 不向外抛
    assert s.state is SessionState.AUTH_EXPIRED


async def test_heartbeat_does_not_poll_while_paused(env: Env) -> None:
    s = await env.make()
    env.backend.script("clouddownload_task_list", perr(590075))
    with pytest.raises(RiskControl):
        await s.list_offline(OfflineStat.RUNNING)
    await env.clock.advance(30)
    assert await s.heartbeat_once() is False  # 熔断期内不发


# ---------------------------------------------------------------- 扫码登录（TV 端）


def _qr_token_script(env: Env) -> None:
    counter = {"n": 0}

    def token() -> dict[str, Any]:
        counter["n"] += 1
        n = counter["n"]
        return {
            "state": 1,
            "data": {
                "uid": f"U{n}",
                "time": n,
                "sign": f"S{n}",
                "qrcode": f"https://115.test/q{n}",
            },
        }

    env.backend.script("login_qrcode_token", lambda *a, **k: token())


def _status(code: int) -> dict[str, Any]:
    return {"state": 1, "data": {"status": code}}


def _result_ok() -> dict[str, Any]:
    return {
        "state": 1,
        "data": {"cookie": {"UID": SECRET, "CID": "cid1", "SEID": "seid1", "KID": "kid1"}},
    }


async def test_qr_start_poll_finish_manual_flow(env: Env) -> None:
    s = await env.make(cookie=False)
    _qr_token_script(env)
    env.backend.script("login_qrcode_scan_status", _status(0), _status(1), _status(2), _status(-1))
    env.backend.script("login_qrcode_scan_result", _result_ok())
    token = await s.qr_start()
    assert token.uid == "U1" and token.image_png.startswith(b"\x89PNG")
    assert await s.qr_poll("U1") is QrStatus.WAITING
    assert await s.qr_poll("U1") is QrStatus.SCANNED
    assert await s.qr_poll("U1") is QrStatus.CONFIRMED
    assert await s.qr_poll("U1") is QrStatus.EXPIRED
    assert await s.qr_poll("unknown-uid") is QrStatus.EXPIRED
    # 扫码前还没有 cookie 文件：只在登录成功时写
    assert not env.cookie_path.exists()
    await s.qr_finish("U1")
    assert env.cookie_path.exists()
    _, args, kwargs = [c for c in env.backend.calls if c[0] == "login_qrcode_scan_result"][0]
    assert args == ("U1",) and kwargs["app"] == "tv"
    # 状态请求带的是 token 里的 uid、time、sign
    status_call = [c for c in env.backend.calls if c[0] == "login_qrcode_scan_status"][0]
    assert status_call[1][0] == {"uid": "U1", "time": 1, "sign": "S1"}


async def test_qr_finish_is_idempotent(env: Env) -> None:
    s = await env.make(cookie=False)
    _qr_token_script(env)
    env.backend.script("login_qrcode_scan_result", _result_ok())
    await s.qr_start()
    await s.qr_finish("U1")
    await s.qr_finish("U1")
    assert env.backend.count("login_qrcode_scan_result") == 1


async def test_auth_expired_runs_qr_loop_to_recovery(env: Env) -> None:
    s = await env.make(auto_qr=True)
    _qr_token_script(env)
    env.backend.script("clouddownload_task_list", perr(990001), task_page())
    env.backend.script("login_qrcode_scan_status", _status(0), _status(1), _status(2))
    env.backend.script("login_qrcode_scan_result", _result_ok())
    with pytest.raises(AuthExpired):
        await s.list_offline(OfflineStat.RUNNING)
    await env.clock.settle()
    assert len(env.notifier.qrs) == 1  # 二维码已推送
    assert env.notifier.qrs[0][0].startswith(b"\x89PNG")
    assert s.state is SessionState.AUTH_EXPIRED and s.status().qr_pending
    assert s.current_qr() is not None
    await env.clock.advance(3)  # 第二次轮询读到 SCANNED
    assert s.state is SessionState.AUTH_EXPIRED
    await env.clock.advance(3)  # 第三次读到 CONFIRMED，换取 cookie
    assert s.state is SessionState.OK
    assert [k for k, _ in env.notifier.recoveries] == ["auth"]
    assert env.cookie_path.exists()
    await s.list_offline(OfflineStat.RUNNING)  # 会话恢复可用
    assert not s.status().qr_pending
    assert env.backend.count("login_qrcode_scan_result") == 1


async def test_qr_expired_regenerates(env: Env) -> None:
    s = await env.make(cookie=False, auto_qr=True)
    _qr_token_script(env)
    # 第一个码轮询到 -1（过期），随后的码一直等待
    env.backend.script("login_qrcode_scan_status", _status(-1), _status(0))
    await env.clock.settle(100)  # 无 cookie：open() 进入 auth_expired 并启动二维码循环
    assert s.state is SessionState.AUTH_EXPIRED
    assert env.backend.count("login_qrcode_token") == 2  # 过期后立刻重新生成
    assert len(env.notifier.qrs) == 2  # 新码也推送（去重由 Notifier 负责）
    current = s.current_qr()
    assert current is not None and current.uid == "U2"
    assert await s.qr_poll("U1") is QrStatus.EXPIRED


async def test_qr_local_ttl_regenerates_even_if_still_waiting(env: Env) -> None:
    s = await env.make(cookie=False, auto_qr=True)
    _qr_token_script(env)
    env.backend.script("login_qrcode_scan_status", _status(0))
    await env.clock.settle()
    await env.clock.advance(session_mod.QR_TTL_SECONDS + 10)
    assert env.backend.count("login_qrcode_token") >= 2
    assert s.state is SessionState.AUTH_EXPIRED


async def test_manual_regenerate_replaces_current_qr_and_pushes(env: Env) -> None:
    s = await env.make(cookie=False, auto_qr=False)
    _qr_token_script(env)
    t1 = await s.regenerate_qr()
    t2 = await s.regenerate_qr()
    assert t1.uid != t2.uid
    assert (s.current_qr() or t1).uid == t2.uid
    assert len(env.notifier.qrs) == 2
    assert await s.qr_poll(t1.uid) is QrStatus.EXPIRED  # 旧码作废


async def test_failed_finish_does_not_write_cookie(env: Env) -> None:
    s = await env.make(cookie=False)
    _qr_token_script(env)
    env.backend.script("login_qrcode_scan_result", perr(40101004))
    await s.qr_start()
    with pytest.raises(AuthExpired):
        await s.qr_finish("U1")
    assert not env.cookie_path.exists()
    env.backend.script("login_qrcode_scan_result", {"state": 1, "data": {}})
    with pytest.raises(ServerError):
        await s.qr_finish("U1")
    assert not env.cookie_path.exists()


async def test_never_calls_login_another_open(env: Env) -> None:
    s = await env.make(cookie=False)
    _qr_token_script(env)
    env.backend.script("login_qrcode_scan_result", _result_ok())
    await s.qr_start()
    await s.qr_finish("U1")
    assert not any("another" in n or "open" in n for n, _, _ in env.backend.calls)
    assert "login_another_open" not in SESSION_SRC


async def test_qr_calls_work_while_paused(env: Env) -> None:
    s = await env.make()
    env.backend.script("clouddownload_task_list", perr(990001))
    with pytest.raises(AuthExpired):
        await s.list_offline(OfflineStat.RUNNING)
    _qr_token_script(env)
    token = await s.qr_start()  # 认证失效期间二维码接口不受暂停限制
    assert token.uid == "U1"


# ---------------------------------------------------------------- cookie 文件与日志


@pytest.mark.skipif(sys.platform == "win32", reason="POSIX 权限位")
async def test_cookie_file_is_0600(env: Env) -> None:
    old_umask = os.umask(0o000)
    try:
        s = await env.make(cookie=False)
        _qr_token_script(env)
        env.backend.script("login_qrcode_scan_result", _result_ok())
        await s.qr_start()
        await s.qr_finish("U1")
    finally:
        os.umask(old_umask)
    assert (env.cookie_path.stat().st_mode & 0o777) == 0o600
    assert not list(env.tmp.glob("*.tmp"))


async def test_cookie_written_is_the_client_cookie_string(env: Env) -> None:
    s = await env.make(cookie=False)
    _qr_token_script(env)
    env.backend.script("login_qrcode_scan_result", _result_ok())
    await s.qr_start()
    await s.qr_finish("U1")
    text = env.cookie_path.read_text()
    assert f"UID={SECRET}" in text and "CID=cid1" in text


async def test_cookie_never_in_logs_repr_or_status(
    env: Env, caplog: pytest.LogCaptureFixture
) -> None:
    caplog.set_level(logging.DEBUG)
    s = await env.make(cookie=True)
    env.backend.script("clouddownload_task_list", perr(990001, f"UID={SECRET} bad"))
    with pytest.raises(AuthExpired) as ei:
        await s.list_offline(OfflineStat.RUNNING)
    _qr_token_script(env)
    env.backend.script("login_qrcode_scan_result", _result_ok())
    await s.qr_start()
    await s.qr_finish("U1")
    blob = "\n".join(
        r.getMessage() + " " + " ".join(f"{k}={v}" for k, v in r.__dict__.items())
        for r in caplog.records
    )
    blob += repr(s) + str(s.status()) + str(ei.value) + repr(ei.value)
    assert SECRET not in blob
    # 日志里只有长度和登录端
    login_logs = [r for r in caplog.records if r.getMessage() == "login succeeded"]
    assert login_logs and login_logs[0].cookie_len > 0 and login_logs[0].login_app == "tv"  # type: ignore[attr-defined]
    loaded = [r for r in caplog.records if r.getMessage() == "cookie loaded"]
    assert loaded and loaded[0].cookie_len > 0  # type: ignore[attr-defined]


async def test_no_cookie_file_enters_auth_expired_on_open(env: Env) -> None:
    s = await env.make(cookie=False)
    assert s.state is SessionState.AUTH_EXPIRED
    assert env.notifier.alert_kinds() == ["auth"]
    with pytest.raises(AuthExpired):
        await s.list_offline(OfflineStat.RUNNING)
    assert env.backend.api_calls == 0


# ---------------------------------------------------------------- 唯一性：flock 与 lease


def test_process_lock_second_holder_raises(tmp_path: Path) -> None:
    a = ProcessLock(tmp_path / "x.lock")
    b = ProcessLock(tmp_path / "x.lock")
    a.acquire()
    try:
        with pytest.raises(AlreadyRunning):
            b.acquire()
    finally:
        a.release()
    b.acquire()  # 释放后可再取
    b.release()


def test_process_lock_with_injected_backend(tmp_path: Path) -> None:
    class Busy:
        def lock(self, fd: int) -> None:
            raise BlockingIOError("held")

        def unlock(self, fd: int) -> None:  # pragma: no cover
            return None

    with pytest.raises(AlreadyRunning):
        ProcessLock(tmp_path / "y.lock", Busy()).acquire()


async def test_open_fails_when_lease_held_and_releases_flock(env: Env) -> None:
    env.repo.acquire_ok = False
    s = await env.make(open_=False)
    with pytest.raises(LeaseHeld):
        await s.open()
    assert env.backend.api_calls == 0


async def test_open_failure_after_lease_releases_lease_and_flock(
    env: Env, monkeypatch: pytest.MonkeyPatch
) -> None:
    def boom(path: Path) -> str | None:
        raise PermissionError("denied")

    monkeypatch.setattr("bridge.cloud.session.read_cookie_file", boom)
    s = await env.make(open_=False)
    with pytest.raises(PermissionError):
        await s.open()
    assert env.repo.released == ["test-owner"]
    assert env.repo.holder is None
    assert s._lock._fd is None  # flock 已释放


async def test_open_takes_flock_via_state_dir(tmp_path: Path) -> None:
    e1, e2 = Env(tmp_path), Env(tmp_path)
    (tmp_path / COOKIE_FILENAME).write_text("UID=x")
    s1 = Session(
        state_dir=tmp_path,
        repo=cast(TaskRepo, e1.repo),
        notifier=e1.notifier,
        clock=e1.clock,
        p115_cls=make_p115_cls(e1.backend),
    )
    s2 = Session(
        state_dir=tmp_path,
        repo=cast(TaskRepo, e2.repo),
        notifier=e2.notifier,
        clock=e2.clock,
        p115_cls=make_p115_cls(e2.backend),
    )
    await s1.open()
    try:
        with pytest.raises(AlreadyRunning):
            await s2.open()
    finally:
        await s1.close()
    assert e1.repo.released  # 关闭时释放 lease


async def test_lease_renew_failure_pauses_alerts_and_recovers(env: Env) -> None:
    s = await env.make()
    assert await s.renew_lease_once() is True
    env.repo.renew_ok = False
    env.repo.acquire_ok = False
    assert await s.renew_lease_once() is False
    assert s.state is SessionState.LEASE_LOST
    assert env.notifier.alert_kinds() == ["lease"]
    before = len(env.backend.calls)
    with pytest.raises(ServerError):  # 全局类异常，调用方不计任务重试
        await s.list_offline(OfflineStat.RUNNING)
    assert len(env.backend.calls) == before
    assert await s.heartbeat_once() is False
    # 重新取得后恢复
    env.repo.acquire_ok = True
    assert await s.renew_lease_once() is True
    assert s.state is SessionState.OK
    assert [k for k, _ in env.notifier.recoveries] == ["lease"]


async def test_lease_loop_renews_every_interval(env: Env) -> None:
    s = await env.make()
    loop_task = asyncio.create_task(s.lease_loop())
    await env.clock.settle()
    env.repo.renew_ok = False
    env.repo.acquire_ok = False
    await env.clock.advance(session_mod.LEASE_RENEW_SECONDS + 1)
    assert s.state is SessionState.LEASE_LOST
    loop_task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await loop_task


async def test_renew_exception_counts_as_failure(env: Env) -> None:
    s = await env.make()

    async def boom(*a: Any, **k: Any) -> bool:
        raise RuntimeError("db down")

    env.repo.renew_lease = boom  # type: ignore[method-assign]
    env.repo.acquire_lease = boom  # type: ignore[method-assign]
    assert await s.renew_lease_once() is False
    assert s.state is SessionState.LEASE_LOST


# ---------------------------------------------------------------- 状态快照与不变量


async def test_status_snapshot(env: Env) -> None:
    s = await env.make()
    st = s.status()
    assert st.state is SessionState.OK and st.login_app == "tv" and st.cookie_present
    assert st.last_success is None
    env.backend.script("clouddownload_task_list", task_page())
    await s.list_offline(OfflineStat.RUNNING)
    assert s.status().last_success == env.clock.now()
    with pytest.raises(Exception):  # noqa: B017,PT011  frozen dataclass
        st.state = SessionState.AUTH_EXPIRED  # type: ignore[misc]
    env.backend.script("clouddownload_task_list", perr(911))
    await env.clock.advance(21)
    with pytest.raises(VerifyRequired):
        await s.list_offline(OfflineStat.RUNNING)
    st = s.status()
    assert st.state is SessionState.VERIFY_REQUIRED and st.since == env.clock.now()
    assert st.detail


def test_no_delete_calls_in_session_source() -> None:
    """不变量 2：会话层不含任何删除云端内容的调用。"""
    src = Path(session_mod.__file__).read_text(encoding="utf-8")
    called = set(re.findall(r"client\.(\w+)\(|self\._p115\.(\w+)\(", src))
    names = {n for pair in called for n in pair if n}
    assert names, "expected to find p115 calls"
    for name in names:
        assert not re.search(r"del|remove|clear|rb_|trash|recycle", name, re.IGNORECASE), name


def test_only_session_imports_p115client() -> None:
    """不变量 1。"""
    root = Path(session_mod.__file__).parents[1]
    offenders = [
        str(p.relative_to(root))
        for p in root.rglob("*.py")
        if p.name != "session.py"
        and re.search(r"^\s*(import|from)\s+p115client", p.read_text(encoding="utf-8"), re.M)
    ]
    assert offenders == []


def test_ids_are_strings_in_signature_helpers() -> None:
    # 不变量 5：id 只按字符串处理；解析函数对数字 id 输出 str
    entry = Session._parse_entry({"fc": "1", "fid": 7, "pid": 3, "fn": "n", "fs": 1}, "3")
    assert entry.id == "7" and entry.parent_id == "3"


# ================================================================ 返工：新增测试


def slow(env: Env, result: Any, seconds: float) -> Any:
    """脚本项：用假时钟挂起 ``seconds`` 秒后返回（或抛出）result。"""

    async def run(*a: Any, **k: Any) -> Any:
        await env.clock.sleep(seconds)
        if isinstance(result, BaseException):
            raise result
        return result

    return run


# ---------------------------------------------------------------- 请求超时


async def test_hung_request_times_out_as_network_error_and_releases_lock(env: Env) -> None:
    s = await env.make()
    env.backend.script("clouddownload_task_list", slow(env, task_page(), 10**9), task_page())
    task = asyncio.create_task(s.list_offline(OfflineStat.RUNNING))
    await env.clock.settle(50)
    assert not task.done()
    await env.clock.advance(session_mod.REQUEST_TIMEOUT_SECONDS + 1)
    with pytest.raises(NetworkError):
        await task
    assert s.state is SessionState.OK
    # 持锁的 list_offline 超时后锁已释放，下一次请求能正常发出
    page = await s.list_offline(OfflineStat.RUNNING)
    assert page.quota.total == 1570


async def test_heartbeat_shares_lock_with_polling(env: Env) -> None:
    """轮询卡在持锁的请求里时，心跳也得排队，不能「心跳成功、轮询卡死」。"""
    s = await env.make()
    env.backend.script("clouddownload_task_list", slow(env, task_page(), 10**9), task_page())
    await env.clock.advance(session_mod.HEARTBEAT_SECONDS + 1)
    poll = asyncio.create_task(s.list_offline(OfflineStat.RUNNING))
    await env.clock.settle(50)
    beat = asyncio.create_task(s.heartbeat_once())
    await env.clock.settle(50)
    assert env.backend.count("clouddownload_task_list") == 1  # 心跳在等同一把锁
    assert not beat.done()
    await env.clock.advance(session_mod.REQUEST_TIMEOUT_SECONDS + 1)
    with pytest.raises(NetworkError):
        await poll
    assert await beat is True
    assert env.backend.count("clouddownload_task_list") == 2


async def test_concurrent_list_offline_same_key_sends_one_request(env: Env) -> None:
    s = await env.make()
    env.backend.script("clouddownload_task_list", slow(env, task_page([raw_task()]), 5))
    tasks = [asyncio.create_task(s.list_offline(OfflineStat.RUNNING)) for _ in range(5)]
    await env.clock.settle(50)
    await env.clock.advance(6)
    pages = await asyncio.gather(*tasks)
    assert env.backend.count("clouddownload_task_list") == 1
    assert all(len(p.tasks) == 1 for p in pages)


async def test_list_dir_page_requests_each_have_timeout(env: Env) -> None:
    s = await env.make()
    env.backend.script("fs_files_app", slow(env, dir_page(1, []), 10**9))
    task = asyncio.create_task(s.list_dir("1"))
    await env.clock.settle(50)
    await env.clock.advance(session_mod.REQUEST_TIMEOUT_SECONDS + 1)
    with pytest.raises(NetworkError):
        await task


async def test_qr_poll_has_a_longer_timeout_than_normal_requests(env: Env) -> None:
    assert session_mod.QR_POLL_TIMEOUT_SECONDS > session_mod.REQUEST_TIMEOUT_SECONDS
    s = await env.make(cookie=False)
    _qr_token_script(env)
    await s.qr_start()
    env.backend.script("login_qrcode_scan_status", slow(env, _status(0), 60))
    task = asyncio.create_task(s.qr_poll("U1"))
    await env.clock.settle(50)
    await env.clock.advance(61)  # 长轮询挂 60 秒仍然正常
    assert await task is QrStatus.WAITING
    env.backend.script("login_qrcode_scan_status", slow(env, _status(0), 10**9))
    task = asyncio.create_task(s.qr_poll("U1"))
    await env.clock.settle(50)
    await env.clock.advance(session_mod.QR_POLL_TIMEOUT_SECONDS + 1)
    with pytest.raises(NetworkError):
        await task


# ---------------------------------------------------------------- 扫码请求不改全局状态


@pytest.mark.parametrize(
    ("failure", "exc"),
    [
        (lambda: perr(40101004), AuthExpired),
        (lambda: http_error(405), RiskControl),
        (lambda: perr(911), VerifyRequired),
        (lambda: perr(590075), RiskControl),
        (lambda: TimeoutError("t"), NetworkError),
    ],
)
async def test_qr_failures_never_change_global_state(
    env: Env, failure: Any, exc: type[CloudError]
) -> None:
    s = await env.make()  # 会话正常，带着一个有效 cookie
    original = env.cookie_path.read_text()
    _qr_token_script(env)
    env.backend.script("login_qrcode_scan_result", failure())
    await s.qr_start()
    with pytest.raises(exc):
        await s.qr_finish("U1")
    env.backend.script("login_qrcode_token", failure())
    with pytest.raises(exc):
        await s.regenerate_qr()
    env.backend.script("login_qrcode_scan_status", failure())
    with pytest.raises(exc):
        await s.qr_poll("U1")
    assert s.state is SessionState.OK
    assert s.status().circuit_level == 0 and s.status().cookie_present
    assert env.notifier.alerts == []
    assert env.cookie_path.read_text() == original  # 原 cookie 保留
    env.backend.script("clouddownload_task_list", task_page())
    await s.list_offline(OfflineStat.RUNNING)  # 会话照常可用


async def test_qr_failure_while_auth_expired_does_not_trip_circuit(env: Env) -> None:
    s = await env.make(cookie=False)
    env.backend.script("login_qrcode_token", http_error(405))
    with pytest.raises(RiskControl):
        await s.qr_start()
    assert s.state is SessionState.AUTH_EXPIRED  # 仍是等待扫码，没有变成熔断
    assert s.status().circuit_level == 0


# ---------------------------------------------------------------- 解析与目录兜底


async def test_offline_task_without_info_hash_is_skipped(
    env: Env, caplog: pytest.LogCaptureFixture
) -> None:
    caplog.set_level(logging.WARNING)
    s = await env.make()
    env.backend.script("clouddownload_task_list", task_page([raw_task(info_hash=""), raw_task()]))
    page = await s.list_offline(OfflineStat.RUNNING)
    assert [t.hash for t in page.tasks] == [HASH]
    assert any("without info_hash" in r.getMessage() for r in caplog.records)


async def test_makedirs_getid_zero_is_not_treated_as_root(env: Env) -> None:
    s = await env.make()
    env.backend.script("fs_makedirs_app", perr_exists())
    for zero in (0, "0"):
        env.backend.script("fs_dir_getid", {"state": True, "id": zero})
        with pytest.raises(ServerError):
            await s.makedirs("/a/b")
    # 主路径返回 0 也一样
    env.backend.script("fs_makedirs_app", {"state": True, "cid": 0})
    with pytest.raises(ServerError):
        await s.makedirs("/a/b")


# ---------------------------------------------------------------- 提交时的未知 errcode


def _add_fail(code: int) -> dict[str, Any]:
    item = {"state": False, "errcode": code, "error_msg": "x"}
    return {"state": True, "data": {"result": [item]}}


async def test_unknown_submit_code_third_time_is_submit_rejected_with_alert(env: Env) -> None:
    s = await env.make()
    env.backend.script("clouddownload_task_add_urls", _add_fail(424242))
    for _ in range(2):
        with pytest.raises(ServerError):
            await s.add_magnet(MAGNET, "1")
    assert not any(k.startswith("submit-unknown") for k in env.notifier.alert_kinds())
    with pytest.raises(SubmitRejected) as ei:
        await s.add_magnet(MAGNET, "1")
    assert ei.value.errcode == 424242
    kinds = [k for k in env.notifier.alert_kinds() if k.startswith("submit-unknown")]
    assert len(kinds) == 1
    assert any("424242" in text for _, text in env.notifier.alerts)
    assert s.state is SessionState.OK


async def test_unknown_submit_counter_is_per_hash_and_reset_by_success_or_known_code(
    env: Env,
) -> None:
    s = await env.make()
    other = "magnet:?xt=urn:btih:" + "ab" * 20
    env.backend.script("clouddownload_task_add_urls", _add_fail(424242))
    for _ in range(2):
        with pytest.raises(ServerError):
            await s.add_magnet(MAGNET, "1")
    with pytest.raises(ServerError):  # 另一个 hash 单独计数
        await s.add_magnet(other, "1")
    env.backend.script("clouddownload_task_add_urls", _add_ok())
    await s.add_magnet(MAGNET, "1")  # 成功后清零
    env.backend.script("clouddownload_task_add_urls", _add_fail(424242))
    for _ in range(2):
        with pytest.raises(ServerError):
            await s.add_magnet(MAGNET, "1")
    env.backend.script("clouddownload_task_add_urls", _add_fail(10010))
    with pytest.raises(QuotaExhausted):  # 已知码清零
        await s.add_magnet(MAGNET, "1")
    env.backend.script("clouddownload_task_add_urls", _add_fail(424242))
    for _ in range(2):
        with pytest.raises(ServerError):
            await s.add_magnet(MAGNET, "1")


# ---------------------------------------------------------------- 参数错误码立即告警


@pytest.mark.parametrize("code", [1001, 990002, 40100000, 980006])
async def test_param_error_codes_alert_immediately_but_stay_server_error(
    env: Env, code: int
) -> None:
    s = await env.make()
    env.backend.script("clouddownload_task_list", perr(code))
    with pytest.raises(ServerError):
        await s.list_offline(OfflineStat.RUNNING)
    assert env.notifier.alert_kinds() == ["api-param"]  # 立即，且不是 network
    assert str(code) in env.notifier.alerts[0][1]
    assert s.state is SessionState.OK


# ---------------------------------------------------------------- 状态代号与 client 切换


async def test_stale_success_does_not_clear_verify_required(env: Env) -> None:
    s = await env.make()

    async def dispatch(payload: dict[str, Any], **k: Any) -> Any:
        if payload["stat"] == int(OfflineStat.RUNNING):
            await env.clock.sleep(10)  # 慢请求：发出时会话还正常
            return task_page()
        raise perr(911)

    env.backend.script("clouddownload_task_list", dispatch)
    slow_call = asyncio.create_task(s.list_offline(OfflineStat.RUNNING))
    await env.clock.settle(50)
    with pytest.raises(VerifyRequired):
        await s.list_offline(OfflineStat.DONE)
    assert s.state is SessionState.VERIFY_REQUIRED
    await env.clock.advance(11)
    await slow_call  # 迟到的成功
    assert s.state is SessionState.VERIFY_REQUIRED
    assert env.notifier.recoveries == []


async def test_stale_success_does_not_reset_circuit_level(env: Env) -> None:
    s = await env.make()

    async def dispatch(payload: dict[str, Any], **k: Any) -> Any:
        if payload["stat"] == int(OfflineStat.RUNNING):
            await env.clock.sleep(1000)
            return task_page()
        raise perr(590075)

    env.backend.script("clouddownload_task_list", dispatch)
    slow_call = asyncio.create_task(
        s._call("list_offline", s_fn(s, OfflineStat.RUNNING), request_timeout=None)  # noqa: SLF001
    )
    await env.clock.settle(50)
    with pytest.raises(RiskControl):
        await s.list_offline(OfflineStat.DONE)
    await env.clock.advance(1001)  # 熔断期已过且距触发超过 10 分钟，但这个成功发出于熔断前
    await slow_call
    assert s.status().circuit_level == 1
    assert env.notifier.recoveries == []


def s_fn(s: Session, stat: OfflineStat) -> Any:
    """绕过 30 秒超时，直接构造一个会拖很久的 list_offline 请求。"""

    async def fn(client: Any) -> Any:
        return await client.clouddownload_task_list({"page": 1, "stat": int(stat)}, async_=True)

    return fn


async def test_old_client_auth_failure_does_not_pause_a_freshly_logged_in_session(
    env: Env,
) -> None:
    s = await env.make()
    # 旧 cookie 发出的请求，晚些时候报认证失效
    env.backend.script("clouddownload_task_list", slow(env, perr(990001), 10))
    old_call = asyncio.create_task(s.list_offline(OfflineStat.RUNNING))
    await env.clock.settle(50)
    _qr_token_script(env)
    env.backend.script("login_qrcode_scan_result", _result_ok())
    await s.qr_start()
    await s.qr_finish("U1")  # 换了新 client
    await env.clock.advance(11)
    with pytest.raises(AuthExpired):
        await old_call
    assert s.state is SessionState.OK  # 迟到的旧失败不得让新会话再次暂停
    assert env.notifier.alerts == []


# ---------------------------------------------------------------- cookie 必须含 UID


async def test_cookie_file_without_uid_is_auth_expired(env: Env) -> None:
    env.cookie_path.write_text("CID=c; SEID=s; KID=k")
    s = await env.make(cookie=False)
    assert s.state is SessionState.AUTH_EXPIRED
    assert not s.status().cookie_present
    assert env.backend.count("__init__") == 0  # 没有用残缺 cookie 建 client
    with pytest.raises(AuthExpired):
        await s.list_offline(OfflineStat.RUNNING)
    assert env.backend.api_calls == 0


def test_has_uid() -> None:
    assert session_mod.has_uid("UID=1_A1_2; CID=c")
    assert session_mod.has_uid("CID=c; UID=1_A1_2")
    assert not session_mod.has_uid("CID=c; SEID=s")
    assert not session_mod.has_uid("XUID=1; CID=c")
    assert not session_mod.has_uid("UID=")


async def test_qr_finish_without_uid_is_auth_failure_and_writes_nothing(env: Env) -> None:
    s = await env.make(cookie=False)
    _qr_token_script(env)
    env.backend.script(
        "login_qrcode_scan_result",
        {"state": 1, "data": {"cookie": {"CID": "c", "SEID": "s", "KID": "k"}}},
    )
    await s.qr_start()
    with pytest.raises(AuthExpired):
        await s.qr_finish("U1")
    assert not env.cookie_path.exists()
    assert not s.status().cookie_present


# ---------------------------------------------------------------- 扫码轮询活跃窗口


async def _qr_env(env: Env) -> Session:
    s = await env.make(cookie=False, auto_qr=True)
    _qr_token_script(env)
    env.backend.script("login_qrcode_scan_status", _status(0))
    await env.clock.settle(100)
    return s


def _polls(env: Env) -> int:
    return env.backend.count("login_qrcode_scan_status")


async def test_qr_polling_stops_after_window_and_resumes_on_next_push(env: Env) -> None:
    s = await _qr_env(env)
    assert env.backend.count("login_qrcode_token") == 1 and len(env.notifier.qrs) == 1
    await env.clock.advance(100)
    assert _polls(env) > 0  # 窗口内在轮询
    await env.clock.advance(520)  # t≈620，窗口（600 秒）已过
    frozen_polls, frozen_tokens = _polls(env), env.backend.count("login_qrcode_token")
    await env.clock.advance(1000)  # t≈1620：窗口外，不轮询、不自动重生成
    assert _polls(env) == frozen_polls
    assert env.backend.count("login_qrcode_token") == frozen_tokens
    assert s.state is SessionState.AUTH_EXPIRED
    await env.clock.advance(300)  # t≈1920，越过 30 分钟节奏：重新生成并推送
    assert env.backend.count("login_qrcode_token") == frozen_tokens + 1
    assert len(env.notifier.qrs) == frozen_tokens + 1
    await env.clock.advance(60)
    assert _polls(env) > frozen_polls  # 新窗口内恢复轮询
    await env.clock.advance(700)
    again = _polls(env)
    await env.clock.advance(500)
    assert _polls(env) == again  # 新窗口过后又停


async def test_deduped_push_still_counts_as_push_attempt(env: Env) -> None:
    async def deduped(png: bytes, caption: str) -> bool:
        env.notifier.qrs.append((png, caption))
        return False  # Notifier 去重，没有真正发出

    env.notifier.send_qr = deduped  # type: ignore[method-assign]
    await _qr_env(env)
    await env.clock.advance(700)
    frozen = _polls(env)
    tokens = env.backend.count("login_qrcode_token")
    await env.clock.advance(1000)
    assert _polls(env) == frozen
    await env.clock.advance(400)  # 30 分钟节奏照常到来
    assert env.backend.count("login_qrcode_token") == tokens + 1
    await env.clock.advance(30)
    assert _polls(env) > frozen


async def test_page_access_reopens_polling_window(env: Env) -> None:
    s = await _qr_env(env)
    await env.clock.advance(700)
    frozen = _polls(env)
    await env.clock.advance(100)
    assert _polls(env) == frozen
    s.current_qr()  # 有人打开 Web 页面
    await env.clock.advance(20)
    assert _polls(env) > frozen
    await env.clock.advance(700)
    frozen2 = _polls(env)
    await env.clock.advance(100)
    assert _polls(env) == frozen2  # 只续 10 分钟


async def test_manual_regenerate_reopens_window_and_is_a_push_attempt(env: Env) -> None:
    s = await _qr_env(env)
    await env.clock.advance(700)
    frozen = _polls(env)
    await s.regenerate_qr()
    await env.clock.advance(20)
    assert _polls(env) > frozen
    await env.clock.advance(700)  # 新窗口（10 分钟）已过
    pushes = len(env.notifier.qrs)
    await env.clock.advance(900)  # 距这次推送尝试不到 30 分钟，窗口外不自动重生成
    assert len(env.notifier.qrs) == pushes


async def test_manual_regenerate_not_cleared_by_stale_poll_result(env: Env) -> None:
    """后台循环轮询旧码时人手动生成了新码：旧码轮询返回过期，不得清掉新码。"""
    s = await env.make(cookie=False, auto_qr=True)
    _qr_token_script(env)
    env.backend.script("login_qrcode_scan_status", slow(env, _status(-1), 5), _status(0))
    await env.clock.settle(100)  # U1 已推送，正在轮询（挂起中）
    token = await s.regenerate_qr()  # 手动换码 U2
    assert token.uid == "U2"
    await env.clock.advance(6)  # 旧码的轮询返回「过期」
    current = s.current_qr()
    assert current is not None and current.uid == "U2"
    assert env.backend.count("login_qrcode_token") == 2  # 没有因为误清而再生成


async def test_qr_loop_idles_without_requests_while_lease_lost(env: Env) -> None:
    s = await _qr_env(env)
    env.repo.renew_ok = False
    env.repo.acquire_ok = False
    await s.renew_lease_once()
    assert s.state is SessionState.LEASE_LOST
    calls = len(env.backend.calls)
    await env.clock.advance(3000)  # 即使到了推送节奏也不得请求 115
    assert len(env.backend.calls) == calls
    # 手动入口同样不得请求或写 cookie
    with pytest.raises(ServerError):
        await s.regenerate_qr()
    with pytest.raises(ServerError):
        await s.qr_finish("U1")
    assert len(env.backend.calls) == calls
    assert not env.cookie_path.exists()
    # lease 取回后循环继续
    env.repo.acquire_ok = True
    await s.renew_lease_once()
    await env.clock.advance(10)
    assert len(env.backend.calls) > calls


async def test_lease_lost_after_confirm_does_not_write_cookie(env: Env) -> None:
    s = await env.make(cookie=False)
    _qr_token_script(env)
    await s.qr_start()

    async def result_then_lease_lost(*a: Any, **k: Any) -> Any:
        env.repo.renew_ok = False
        env.repo.acquire_ok = False
        await s.renew_lease_once()
        return _result_ok()

    env.backend.script("login_qrcode_scan_result", result_then_lease_lost)
    with pytest.raises(ServerError):
        await s.qr_finish("U1")
    assert not env.cookie_path.exists()


# ---------------------------------------------------------------- 后台任务不静默死掉


async def test_background_task_crash_is_logged_alerted_and_restarted(
    env: Env, caplog: pytest.LogCaptureFixture
) -> None:
    caplog.set_level(logging.ERROR)
    s = await env.make(open_=False)
    calls: list[int] = []

    async def flaky() -> bool:
        calls.append(1)
        if len(calls) == 1:
            raise RuntimeError("boom")
        return False

    s.heartbeat_once = flaky  # type: ignore[method-assign]
    await s.start()
    await env.clock.advance(session_mod.HEARTBEAT_SECONDS + 1)
    assert len(calls) == 1  # 第一次心跳抛了非 CloudError 的异常
    assert any("background task crashed" in r.getMessage() for r in caplog.records)
    assert "task-crash-heartbeat" in env.notifier.alert_kinds()
    await env.clock.advance(session_mod.TASK_RESTART_SECONDS + session_mod.HEARTBEAT_SECONDS + 1)
    assert len(calls) == 2  # 已重启，继续按周期心跳
    assert all(not t.done() for t in s._tasks)  # noqa: SLF001


# ---------------------------------------------------------------- cookie 原子写


def test_cookie_write_fsyncs_directory_after_replace(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    order: list[str] = []
    real_replace = os.replace
    monkeypatch.setattr(os, "replace", lambda a, b: (order.append("replace"), real_replace(a, b)))
    monkeypatch.setattr(session_mod, "_fsync_dir", lambda d: order.append(f"fsync:{d.name}"))
    session_mod.write_cookie_file(tmp_path / "sub" / "c.cookie", "UID=1")
    assert order == ["replace", "fsync:sub"]


@pytest.mark.skipif(sys.platform == "win32", reason="POSIX 才对目录 fsync")
def test_fsync_dir_calls_os_fsync_on_posix(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    seen: list[int] = []
    monkeypatch.setattr(os, "fsync", lambda fd: seen.append(fd))
    session_mod._fsync_dir(tmp_path)  # noqa: SLF001
    assert len(seen) == 1


def test_fsync_dir_is_skipped_on_windows(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(session_mod.sys, "platform", "win32")
    monkeypatch.setattr(os, "fsync", lambda fd: pytest.fail("must not fsync"))
    session_mod._fsync_dir(tmp_path)  # noqa: SLF001
