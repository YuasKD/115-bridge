from __future__ import annotations

import logging
from collections.abc import AsyncIterator
from datetime import UTC, datetime

import httpx
import pytest
import respx

from bridge.notify import NoopNotifier, TelegramNotifier

TOKEN = "123456789:FAKEtokenFAKEtokenFAKEtoken_abc"
BASE = f"https://api.telegram.org/bot{TOKEN}"


class StubClock:
    def __init__(self) -> None:
        self.t = 1000.0

    def now(self) -> datetime:
        return datetime.fromtimestamp(self.t, UTC)

    def monotonic(self) -> float:
        return self.t

    async def sleep(self, seconds: float) -> None:
        self.t += seconds


@pytest.fixture
def clock() -> StubClock:
    return StubClock()


@pytest.fixture
async def notifier(clock: StubClock) -> AsyncIterator[TelegramNotifier]:
    async with httpx.AsyncClient() as c:
        yield TelegramNotifier(TOKEN, "42", clock, c)


async def test_alert_dedup_30min(notifier: TelegramNotifier, clock: StubClock) -> None:
    with respx.mock:
        route = respx.post(f"{BASE}/sendMessage").respond(200, json={"ok": True})
        assert await notifier.alert("quota", "low") is True
        assert await notifier.alert("quota", "low") is False
        assert await notifier.alert("space", "other kind") is True
        clock.t += 29 * 60
        assert await notifier.alert("quota", "low") is False
        clock.t += 61
        assert await notifier.alert("quota", "low") is True
        assert route.call_count == 3


async def test_qr_window(notifier: TelegramNotifier, clock: StubClock) -> None:
    with respx.mock:
        route = respx.post(f"{BASE}/sendPhoto").respond(200, json={"ok": True})
        assert await notifier.send_qr(b"\x89PNG", "scan") is True
        clock.t += 10 * 60
        assert await notifier.send_qr(b"\x89PNG", "scan") is False
        clock.t += 20 * 60
        assert await notifier.send_qr(b"\x89PNG", "scan") is True
        assert route.call_count == 2
        assert b"\x89PNG" in route.calls[0].request.content


async def test_failure_does_not_consume_window_and_does_not_raise(
    notifier: TelegramNotifier,
) -> None:
    with respx.mock:
        route = respx.post(f"{BASE}/sendMessage").mock(
            side_effect=[httpx.ConnectError("boom"), httpx.Response(500), httpx.Response(200)]
        )
        assert await notifier.alert("k", "t") is False
        assert await notifier.alert("k", "t") is False
        assert await notifier.alert("k", "t") is True
        assert route.call_count == 3


async def test_recovered_clears_windows(notifier: TelegramNotifier) -> None:
    with respx.mock:
        respx.post(f"{BASE}/sendMessage").respond(200, json={"ok": True})
        photo = respx.post(f"{BASE}/sendPhoto").respond(200, json={"ok": True})
        await notifier.alert("auth", "down")
        await notifier.send_qr(b"x", "qr")
        assert await notifier.recovered("auth", "back") is True
        assert await notifier.alert("auth", "down again") is True
        assert await notifier.send_qr(b"x", "qr") is True
        assert photo.call_count == 2


async def test_token_not_in_logs(
    notifier: TelegramNotifier, caplog: pytest.LogCaptureFixture
) -> None:
    caplog.set_level(logging.DEBUG)
    with respx.mock:
        req_url = f"{BASE}/sendMessage"
        respx.post(req_url).mock(side_effect=httpx.ConnectError(f"failed {req_url}"))
        await notifier.alert("k", "t")
        respx.post(f"{BASE}/sendPhoto").respond(500)
        await notifier.send_qr(b"x", "c")
    assert caplog.records
    for r in caplog.records:
        assert TOKEN not in r.getMessage()
        assert TOKEN not in str(r.__dict__)
    assert TOKEN not in caplog.text


async def test_only_send_endpoints(notifier: TelegramNotifier) -> None:
    with respx.mock:
        respx.post(f"{BASE}/sendMessage").respond(200)
        respx.post(f"{BASE}/sendPhoto").respond(200)
        await notifier.alert("a", "t")
        await notifier.send_qr(b"x", "c")
        await notifier.recovered("a", "ok")
        paths = [c.request.url.path.rsplit("/", 1)[1] for c in respx.calls]
    assert paths
    assert set(paths) <= {"sendMessage", "sendPhoto"}


async def test_noop() -> None:
    n = NoopNotifier()
    assert await n.alert("k", "t") is False
    assert await n.send_qr(b"x", "c") is False
    assert await n.recovered("k", "t") is False
