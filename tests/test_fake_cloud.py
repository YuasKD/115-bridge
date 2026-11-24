from __future__ import annotations

import asyncio
import hashlib

import httpx
import pytest

from bridge.clock import Clock
from bridge.cloud.fake import (
    FakeCloud,
    FakeFile,
    OfflineScenario,
    make_content,
    rename_illegal,
    truncate_name,
)
from bridge.cloud.protocol import (
    AuthExpired,
    CloudClient,
    CloudNotFound,
    NetworkError,
    OfflineStat,
    QrStatus,
    QuotaExhausted,
    RiskControl,
    ServerError,
    SubmitRejected,
)
from bridge.testing.clock import FakeClock

H = "a" * 40


def magnet(h: str = H) -> str:
    return f"magnet:?xt=urn:btih:{h}&dn=x"


def scenario(**kw: object) -> OfflineScenario:
    files = [FakeFile("a.mkv", make_content("a", 100)), FakeFile("sub/b.srt", b"hello")]
    return OfflineScenario(name="Movie", files=files, **kw)  # type: ignore[arg-type]


@pytest.fixture
def clock() -> FakeClock:
    return FakeClock()


@pytest.fixture
def cloud(clock: FakeClock) -> FakeCloud:
    return FakeCloud(clock)


async def test_protocol_conformance(cloud: FakeCloud, clock: FakeClock) -> None:
    assert isinstance(cloud, CloudClient)
    assert isinstance(clock, Clock)
    assert not any("del" in n or "remove" in n for n in dir(cloud) if not n.startswith("_"))


async def test_progress_follows_clock_then_completes(cloud: FakeCloud, clock: FakeClock) -> None:
    cloud.script(H, scenario(ramp_seconds=100, stall_percent=99, stall_seconds=50))
    wp = await cloud.makedirs("/115-bridge/radarr")
    assert await cloud.add_magnet(magnet(), wp) == H
    t = (await cloud.list_offline(OfflineStat.RUNNING)).tasks[0]
    assert t.percent_done == 0 and not t.is_done
    await clock.advance(50)
    t = (await cloud.list_offline(OfflineStat.RUNNING)).tasks[0]
    assert t.percent_done == pytest.approx(49.5)
    await clock.advance(50)  # 卡在 99%
    await clock.advance(30)
    t = (await cloud.list_offline(OfflineStat.RUNNING)).tasks[0]
    assert t.percent_done == 99 and not t.is_done
    await clock.advance(30)  # 完成
    assert (await cloud.list_offline(OfflineStat.RUNNING)).tasks == []
    done = (await cloud.list_offline(OfflineStat.DONE)).tasks[0]
    assert done.is_done and done.file_id is not None and done.wp_path_id == wp
    assert done.del_path == "Movie" and done.size == 105


async def test_list_offline_quota_and_paging(clock: FakeClock) -> None:
    cloud = FakeCloud(clock, quota_total=10, quota_remaining=5, page_size=2)
    for i in range(3):
        await cloud.add_magnet(magnet(f"{i:040x}"), "0")
    page = await cloud.list_offline(OfflineStat.RUNNING, 2)
    assert (page.quota.remaining, page.quota.total) == (2, 10)
    assert page.page_count == 2 and len(page.tasks) == 1 and page.page == 2


async def test_transitional_states_are_not_done(cloud: FakeCloud, clock: FakeClock) -> None:
    for h, kind in (("b" * 40, "percent100"), ("c" * 40, "no_file_id")):
        cloud.script(
            h,
            OfflineScenario(
                "T",
                [FakeFile("f", b"x")],
                ramp_seconds=10,
                transient_seconds=30,
                transient_kind=kind,  # type: ignore[arg-type]
            ),
        )
        await cloud.add_magnet(magnet(h), "0")
    await clock.advance(20)
    tasks = {
        t.hash: t
        for stat in (OfflineStat.RUNNING, OfflineStat.DONE)
        for t in (await cloud.list_offline(stat)).tasks
    }
    for t in tasks.values():
        assert t.percent_done == 100 and t.file_id is None and not t.is_done
    assert tasks["b" * 40].status == 1 and tasks["c" * 40].status == 2
    await clock.advance(30)
    assert all(t.is_done for t in (await cloud.list_offline(OfflineStat.DONE)).tasks)


async def test_manual_state(cloud: FakeCloud) -> None:
    await cloud.add_magnet(magnet(), "0")
    cloud.set_task_state(H, status=1, percent_done=100)
    assert not (await cloud.list_offline(OfflineStat.RUNNING)).tasks[0].is_done
    cloud.complete(H)
    assert (await cloud.list_offline(OfflineStat.DONE)).tasks[0].is_done


@pytest.mark.parametrize("status", [-1, 3, 4, 5])
async def test_offline_failure(cloud: FakeCloud, clock: FakeClock, status: int) -> None:
    cloud.script(H, scenario(ramp_seconds=10, outcome=status))
    await cloud.add_magnet(magnet(), "0")
    await clock.advance(20)
    assert (await cloud.list_offline(OfflineStat.RUNNING)).tasks == []
    failed = (await cloud.list_offline(OfflineStat.FAILED)).tasks
    assert len(failed) == 1 and failed[0].is_failed and failed[0].status == status
    assert not failed[0].is_done


async def test_manual_fail_and_never_finishes(cloud: FakeCloud, clock: FakeClock) -> None:
    cloud.script(H, scenario(ramp_seconds=1, never_finishes=True))
    await cloud.add_magnet(magnet(), "0")
    await clock.advance(10**6)
    t = (await cloud.list_offline(OfflineStat.RUNNING)).tasks[0]
    assert t.percent_done == 99 and not t.is_done
    cloud.fail(H, 4)
    assert (await cloud.list_offline(OfflineStat.FAILED)).tasks[0].status == 4


async def test_add_magnet_idempotent_reject_quota(cloud: FakeCloud) -> None:
    await cloud.add_magnet(magnet(), "0")
    before = cloud.quota_remaining
    assert await cloud.add_magnet(magnet(H.upper()), "0") == H
    assert cloud.quota_remaining == before
    assert len((await cloud.list_offline(OfflineStat.RUNNING)).tasks) == 1

    cloud.script("d" * 40, OfflineScenario("R", [], reject=(911, "bad")))
    with pytest.raises(SubmitRejected) as ei:
        await cloud.add_magnet(magnet("d" * 40), "0")
    assert ei.value.errcode == 911

    cloud.quota_remaining = 0
    with pytest.raises(QuotaExhausted):
        await cloud.add_magnet(magnet("e" * 40), "0")


async def test_tree_with_renamed_files(cloud: FakeCloud, clock: FakeClock) -> None:
    long_name = "x" * 300 + ".mkv"
    sc = OfflineScenario(
        "Show",
        [
            FakeFile("Season 1/a:b?.mkv", b"1", cloud_name=rename_illegal("a:b?.mkv")),
            FakeFile("Season 1/" + long_name, b"22", cloud_name=truncate_name(long_name, 255)),
            FakeFile("c.nfo", b"333"),
        ],
        ramp_seconds=1,
    )
    cloud.script(H, sc)
    wp = await cloud.makedirs("/115-bridge/tv")
    await cloud.add_magnet(magnet(), wp)
    await clock.advance(5)
    root_id = (await cloud.list_offline(OfflineStat.DONE)).tasks[0].file_id
    assert root_id is not None
    root = await cloud.get_entry(root_id)
    assert root is not None and root.is_dir and root.name == "Show" and root.parent_id == wp
    top = {e.name: e for e in await cloud.list_dir(root_id)}
    assert set(top) == {"Season 1", "c.nfo"}
    inner = {e.name: e for e in await cloud.list_dir(top["Season 1"].id)}
    assert "a_b_.mkv" in inner
    trunc = truncate_name(long_name, 255)
    assert len(trunc) == 255 and trunc.endswith(".mkv") and trunc in inner
    f = inner["a_b_.mkv"]
    assert isinstance(f.id, str) and f.sha1 == hashlib.sha1(b"1").hexdigest().upper()
    assert f.pickcode and f.size == 1 and not f.is_dir


async def test_makedirs_idempotent_and_hide(cloud: FakeCloud) -> None:
    a = await cloud.makedirs("/115-bridge/radarr")
    assert await cloud.makedirs("/115-bridge/radarr") == a
    assert isinstance(a, str)
    cloud.hide_entry(a)
    assert await cloud.get_entry(a) is None


async def test_preload_tree(cloud: FakeCloud) -> None:
    root = cloud.preload_tree(H, scenario(), "0")
    assert len(await cloud.list_dir(root)) == 2


async def test_download_link_fresh_each_time(cloud: FakeCloud, clock: FakeClock) -> None:
    cloud.script(H, scenario(ramp_seconds=1))
    await cloud.add_magnet(magnet(), "0")
    await clock.advance(5)
    root = (await cloud.list_offline(OfflineStat.DONE)).tasks[0].file_id
    assert root
    pc = next(e.pickcode for e in await cloud.list_dir(root) if e.pickcode)
    assert pc
    l1, l2 = await cloud.download_link(pc), await cloud.download_link(pc)
    assert l1.url != l2.url and l1.headers["user-agent"] != l2.headers["user-agent"]
    assert l1.size == 100
    with pytest.raises(CloudNotFound):
        await cloud.download_link("nope")
    fid = next(e.id for e in await cloud.list_dir(root) if e.pickcode == pc)
    cloud.hide_entry(fid)
    with pytest.raises(CloudNotFound):
        await cloud.download_link(pc)


async def test_list_dir_on_file_id_raises(cloud: FakeCloud) -> None:
    wp = await cloud.makedirs("/x")
    sc = OfflineScenario("one.mkv", [FakeFile("a", b"x" * 10)], single_file_at_root=True)
    cloud.preload_tree("ab" * 20, sc, wp)
    [f] = await cloud.list_dir(wp)
    assert not f.is_dir
    with pytest.raises(CloudNotFound):
        await cloud.list_dir(f.id)


async def test_list_dir_missing_or_hidden_raises(cloud: FakeCloud) -> None:
    with pytest.raises(CloudNotFound):
        await cloud.list_dir("999999")
    d = await cloud.makedirs("/x/y")
    assert await cloud.list_dir(d) == []
    cloud.hide_entry(d)
    with pytest.raises(CloudNotFound):
        await cloud.list_dir(d)


async def test_add_magnet_invalid_wp_path_id_raises(cloud: FakeCloud) -> None:
    quota = cloud.quota_remaining
    with pytest.raises(CloudNotFound):
        await cloud.add_magnet(magnet(), "999999")
    d = await cloud.makedirs("/x")
    cloud.hide_entry(d)
    with pytest.raises(CloudNotFound):
        await cloud.add_magnet(magnet(), d)
    assert cloud.quota_remaining == quota


async def test_single_file_at_root_shape(cloud: FakeCloud, clock: FakeClock) -> None:
    sc = OfflineScenario(
        "movie.mkv",
        [FakeFile("movie.mkv", make_content("m", 50))],
        ramp_seconds=1,
        single_file_at_root=True,
    )
    cloud.script(H, sc)
    wp = await cloud.makedirs("/115-bridge/radarr")
    await cloud.add_magnet(magnet(), wp)
    await clock.advance(5)
    [t] = (await cloud.list_offline(OfflineStat.DONE)).tasks
    assert t.file_id == wp == t.wp_path_id and t.del_path == "movie.mkv"
    [e] = await cloud.list_dir(wp)
    assert not e.is_dir and e.name == "movie.mkv" and e.size == 50 and e.parent_id == wp


@pytest.mark.parametrize(
    "exc",
    [AuthExpired("a"), RiskControl("r"), NetworkError("n"), ServerError("s")],
)
async def test_inject_on_nth_call(cloud: FakeCloud, exc: Exception) -> None:
    cloud.inject("list_offline", exc, on_call=3)  # type: ignore[arg-type]
    await cloud.list_offline(OfflineStat.RUNNING)
    await cloud.list_offline(OfflineStat.RUNNING)
    with pytest.raises(type(exc)):
        await cloud.list_offline(OfflineStat.RUNNING)
    await cloud.list_offline(OfflineStat.RUNNING)  # times=1，之后恢复
    assert cloud.call_count("list_offline") == 4


async def test_inject_forever_and_other_methods(cloud: FakeCloud) -> None:
    cloud.inject("add_magnet", SubmitRejected(1, "no"), times=None)
    cloud.inject("get_entry", AuthExpired())
    for _ in range(2):
        with pytest.raises(SubmitRejected):
            await cloud.add_magnet(magnet(), "0")
    with pytest.raises(AuthExpired):
        await cloud.get_entry("1")
    assert await cloud.get_entry("1") is None


async def test_call_recording(cloud: FakeCloud) -> None:
    await cloud.add_magnet(magnet(), "0")
    await cloud.list_offline(OfflineStat.DONE, 1)
    assert cloud.calls_to("add_magnet")[0].args == (magnet(), "0")
    assert cloud.calls_to("list_offline")[0].args == (OfflineStat.DONE, 1)
    assert not cloud.was_called("download_link")
    assert [c.method for c in cloud.calls] == ["add_magnet", "list_offline"]


async def test_qr_flow(cloud: FakeCloud) -> None:
    tok = await cloud.qr_start()
    assert tok.image_png
    seen = [await cloud.qr_poll(tok.uid) for _ in range(4)]
    assert seen == [QrStatus.WAITING, QrStatus.SCANNED, QrStatus.CONFIRMED, QrStatus.CONFIRMED]
    await cloud.qr_finish(tok.uid)
    assert cloud.logged_in


async def test_fake_clock_wakes_sleepers_in_order(clock: FakeClock) -> None:
    order: list[str] = []

    async def w(name: str, s: float) -> None:
        await clock.sleep(s)
        order.append(name)

    tasks = [asyncio.create_task(w("b", 20)), asyncio.create_task(w("a", 10))]
    await clock.settle()
    assert clock.pending_sleepers == 2
    t0, n0 = clock.monotonic(), clock.now()
    await clock.advance(15)
    assert order == ["a"]
    await clock.advance(10)
    assert order == ["a", "b"]
    assert clock.monotonic() == t0 + 25 and (clock.now() - n0).total_seconds() == 25
    await asyncio.gather(*tasks)


async def test_fake_clock_chained_sleeps(clock: FakeClock) -> None:
    ticks = 0

    async def loop() -> None:
        nonlocal ticks
        for _ in range(5):
            await clock.sleep(10)
            ticks += 1

    t = asyncio.create_task(loop())
    await clock.advance(35)
    assert ticks == 3
    await clock.advance(100)
    await t
    assert ticks == 5


async def test_download_via_cdn_ok(cloud: FakeCloud, clock: FakeClock) -> None:
    cloud.script(H, scenario(ramp_seconds=1))
    await cloud.add_magnet(magnet(), "0")
    await clock.advance(2)
    root = (await cloud.list_offline(OfflineStat.DONE)).tasks[0].file_id
    assert root
    entry = next(e for e in await cloud.list_dir(root) if e.name == "a.mkv")
    assert entry.pickcode
    link = await cloud.download_link(entry.pickcode)
    async with httpx.AsyncClient(transport=cloud.cdn.transport) as c:
        r = await c.get(link.url, headers=link.headers)
    assert r.status_code == 200 and r.content == make_content("a", 100)


async def test_get_space_configurable_and_injectable(clock: FakeClock) -> None:
    from bridge.cloud.protocol import ServerError

    cloud = FakeCloud(clock, space_total=1000, space_remaining=400)
    space = await cloud.get_space()
    assert (space.total, space.remaining) == (1000, 400)
    cloud.space_remaining = 10
    assert (await cloud.get_space()).remaining == 10
    cloud.inject("get_space", ServerError("boom"))
    with pytest.raises(ServerError):
        await cloud.get_space()
    assert (await cloud.get_space()).remaining == 10  # 只抛一次
