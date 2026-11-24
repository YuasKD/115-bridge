from __future__ import annotations

import httpx
import pytest

from bridge.cloud.fake import CdnFault, FakeCdn, FakeCloud
from bridge.cloud.protocol import DownloadLink
from bridge.testing.clock import FakeClock

DATA = bytes(range(256)) * 40  # 10240 字节
PC = "pc1"


@pytest.fixture
def clock() -> FakeClock:
    return FakeClock()


@pytest.fixture
def cdn(clock: FakeClock) -> FakeCdn:
    c = FakeCdn(clock, chunk_size=1000)
    c.add_file(PC, DATA)
    return c


@pytest.fixture
def client(cdn: FakeCdn) -> httpx.AsyncClient:
    return httpx.AsyncClient(transport=cdn.transport)


def hdr(link: DownloadLink, **extra: str) -> dict[str, str]:
    return {**link.headers, **extra}


async def test_range_206(cdn: FakeCdn, client: httpx.AsyncClient) -> None:
    link = cdn.issue_link(PC)
    r = await client.get(link.url, headers=hdr(link, range="bytes=100-199"))
    assert r.status_code == 206
    assert r.content == DATA[100:200]
    assert r.headers["content-range"] == f"bytes 100-199/{len(DATA)}"
    assert r.headers["content-length"] == "100"
    r = await client.get(link.url, headers=hdr(link, range="bytes=10000-"))
    assert r.status_code == 206 and r.content == DATA[10000:]
    assert r.headers["content-range"] == f"bytes 10000-10239/{len(DATA)}"
    r = await client.get(link.url, headers=hdr(link, range="bytes=99999-"))
    assert r.status_code == 416


async def test_no_range_is_200_full(cdn: FakeCdn, client: httpx.AsyncClient) -> None:
    link = cdn.issue_link(PC)
    r = await client.get(link.url, headers=link.headers)
    assert r.status_code == 200 and r.content == DATA


async def test_third_connection_403_even_with_new_link(
    cdn: FakeCdn, client: httpx.AsyncClient
) -> None:
    l1, l2, l3 = cdn.issue_link(PC), cdn.issue_link(PC), cdn.issue_link(PC)
    async with (
        client.stream("GET", l1.url, headers=hdr(l1, range="bytes=0-99")) as r1,
        client.stream("GET", l2.url, headers=hdr(l2, range="bytes=100-199")) as r2,
    ):
        assert (r1.status_code, r2.status_code) == (206, 206)
        assert cdn.active_connections(PC) == 2
        r3 = await client.get(l3.url, headers=hdr(l3, range="bytes=200-299"))
        assert r3.status_code == 403
        l4 = cdn.issue_link(PC)
        r4 = await client.get(l4.url, headers=hdr(l4, range="bytes=200-299"))
        assert r4.status_code == 403
        await r1.aread()
        await r1.aclose()
        assert cdn.active_connections(PC) == 1  # r1 已关闭，名额释放
    r5 = await client.get(l3.url, headers=hdr(l3, range="bytes=200-299"))
    assert r5.status_code == 206
    assert cdn.active_connections(PC) == 0


async def test_user_agent_required(cdn: FakeCdn, client: httpx.AsyncClient) -> None:
    link = cdn.issue_link(PC)
    assert (await client.get(link.url)).status_code == 403
    assert (await client.get(link.url, headers={"user-agent": "other"})).status_code == 403
    other = cdn.issue_link(PC)
    # 用别的链接的 UA 也不行
    assert (await client.get(link.url, headers=other.headers)).status_code == 403
    assert (await client.get(link.url, headers=link.headers)).status_code == 200


async def test_unknown_or_expired_token_403(
    cdn: FakeCdn, client: httpx.AsyncClient, clock: FakeClock
) -> None:
    link = cdn.issue_link(PC)
    bad = link.url.replace("token=tok", "token=zzz")
    assert (await client.get(bad, headers=link.headers)).status_code == 403
    await clock.advance(cdn.link_ttl + 1)
    assert (await client.get(link.url, headers=link.headers)).status_code == 403
    fresh = cdn.issue_link(PC)
    assert (await client.get(fresh.url, headers=fresh.headers)).status_code == 200
    cdn.expire_all_links()
    assert (await client.get(fresh.url, headers=fresh.headers)).status_code == 403


async def test_mid_stream_cut(cdn: FakeCdn, client: httpx.AsyncClient) -> None:
    cdn.queue_fault(PC, CdnFault("cut", after_bytes=2500))
    link = cdn.issue_link(PC)
    got = b""
    with pytest.raises(httpx.ReadError):
        async with client.stream("GET", link.url, headers=hdr(link, range="bytes=0-9999")) as r:
            async for chunk in r.aiter_raw():
                got += chunk
    assert got == DATA[:2500]
    assert cdn.active_connections(PC) == 0
    # 故障只消费一次，重试（新链接）正常
    link2 = cdn.issue_link(PC)
    r2 = await client.get(link2.url, headers=hdr(link2, range="bytes=2500-"))
    assert r2.status_code == 206 and r2.content == DATA[2500:]


async def test_wrong_content_length(cdn: FakeCdn, client: httpx.AsyncClient) -> None:
    cdn.queue_fault(PC, CdnFault("bad_length", delta=5))
    link = cdn.issue_link(PC)
    async with client.stream("GET", link.url, headers=hdr(link, range="bytes=0-99")) as r:
        assert r.status_code == 206
        assert r.headers["content-length"] == "105"  # 与真实 100 不符
        assert len(await r.aread()) == 100


async def test_wrong_content_range(cdn: FakeCdn, client: httpx.AsyncClient) -> None:
    cdn.queue_fault(PC, CdnFault("bad_range", delta=7))
    link = cdn.issue_link(PC)
    r = await client.get(link.url, headers=hdr(link, range="bytes=0-99"))
    assert r.status_code == 206
    assert r.headers["content-range"] == f"bytes 0-106/{len(DATA)}"


async def test_full_200_ignores_range_and_status_fault(
    cdn: FakeCdn, client: httpx.AsyncClient
) -> None:
    cdn.queue_fault(PC, CdnFault("full_200"), CdnFault("status", status=503))
    link = cdn.issue_link(PC)
    r = await client.get(link.url, headers=hdr(link, range="bytes=100-199"))
    assert r.status_code == 200 and len(r.content) == len(DATA)
    assert "content-range" not in r.headers
    r = await client.get(link.url, headers=hdr(link, range="bytes=100-199"))
    assert r.status_code == 503


async def test_request_log(cdn: FakeCdn, client: httpx.AsyncClient) -> None:
    link = cdn.issue_link(PC)
    await client.get(link.url, headers=hdr(link, range="bytes=0-9"))
    await client.get(link.url)
    assert [(q.status, q.range) for q in cdn.requests] == [(206, "bytes=0-9"), (403, None)]


async def test_cloud_links_are_served_by_its_cdn(clock: FakeClock) -> None:
    cloud = FakeCloud(clock)
    cloud.cdn.add_file("pcx", b"abc")
    link = cloud.cdn.issue_link("pcx")
    async with httpx.AsyncClient(transport=cloud.cdn.transport) as c:
        assert (await c.get(link.url, headers=link.headers)).content == b"abc"
