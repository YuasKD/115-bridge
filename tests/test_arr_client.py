import httpx
import pytest
import respx

from bridge.arr.client import ArrType, HttpArrClient
from bridge.arr.protocol import ArrAuthError, ArrClient, ArrUnreachable, QueueItemNotFound

KEY = "sekret-key-123"
HASH = "ab" * 20
BASE = "http://radarr:7878"


def make(
    arr_type: ArrType = "radarr", base: str = BASE, client: httpx.AsyncClient | None = None
) -> HttpArrClient:
    return HttpArrClient("x", arr_type, base, KEY, client=client)


def page(records: list[dict[str, object]], total: int, n: int = 1) -> httpx.Response:
    return httpx.Response(
        200, json={"page": n, "pageSize": 2, "totalRecords": total, "records": records}
    )


def test_protocol_conformance() -> None:
    assert isinstance(make(), ArrClient)


@respx.mock
async def test_radarr_params_header_and_match() -> None:
    route = respx.get(f"{BASE}/api/v3/queue").mock(
        return_value=page([{"id": 7, "downloadId": HASH.upper()}], 1)
    )
    assert await make().find_queue_item(HASH) == 7
    req = route.calls.last.request
    assert req.url.params["includeUnknownMovieItems"] == "false"
    assert "includeUnknownSeriesItems" not in req.url.params
    assert req.headers["X-Api-Key"] == KEY


@respx.mock
async def test_sonarr_params() -> None:
    route = respx.get("http://sonarr:8989/api/v3/queue").mock(
        return_value=page([{"id": 3, "downloadId": HASH.upper()}], 1)
    )
    c = make("sonarr", "http://sonarr:8989/")
    assert await c.find_queue_item(HASH) == 3
    params = route.calls.last.request.url.params
    assert params["includeUnknownSeriesItems"] == "false"
    assert "includeUnknownMovieItems" not in params


@respx.mock
async def test_pagination_finds_item_on_later_page() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        n = int(request.url.params["page"])
        if n == 1:
            return page([{"id": 1, "downloadId": "A"}, {"id": 2, "downloadId": "B"}], 5, 1)
        if n == 2:
            return page([{"id": 3, "downloadId": "C"}, {"id": 4}], 5, 2)
        return page([{"id": 9, "downloadId": HASH.upper()}], 5, 3)

    route = respx.get(f"{BASE}/api/v3/queue").mock(side_effect=handler)
    assert await make().find_queue_item(HASH) == 9
    assert route.call_count == 3


@respx.mock
async def test_not_found_stops_after_all_pages() -> None:
    route = respx.get(f"{BASE}/api/v3/queue").mock(
        return_value=page([{"id": 1, "downloadId": "OTHER"}], 1)
    )
    with pytest.raises(QueueItemNotFound):
        await make().find_queue_item(HASH)
    assert route.call_count == 1


@respx.mock
async def test_empty_queue_not_found() -> None:
    respx.get(f"{BASE}/api/v3/queue").mock(return_value=page([], 0))
    with pytest.raises(QueueItemNotFound):
        await make().find_queue_item(HASH)


@respx.mock
async def test_remove_params() -> None:
    route = respx.delete(f"{BASE}/api/v3/queue/7").mock(return_value=httpx.Response(200))
    await make().remove_and_blocklist(7)
    req = route.calls.last.request
    assert dict(req.url.params) == {
        "removeFromClient": "true",
        "blocklist": "true",
        "skipRedownload": "false",
    }
    assert req.headers["X-Api-Key"] == KEY


@respx.mock
async def test_remove_404_is_not_found() -> None:
    respx.delete(f"{BASE}/api/v3/queue/7").mock(return_value=httpx.Response(404))
    with pytest.raises(QueueItemNotFound):
        await make().remove_and_blocklist(7)


@respx.mock
@pytest.mark.parametrize("status", [401, 403])
async def test_auth_error(status: int) -> None:
    respx.get(f"{BASE}/api/v3/queue").mock(return_value=httpx.Response(status))
    respx.delete(f"{BASE}/api/v3/queue/1").mock(return_value=httpx.Response(status))
    with pytest.raises(ArrAuthError) as ei:
        await make().find_queue_item(HASH)
    assert KEY not in str(ei.value)
    with pytest.raises(ArrAuthError):
        await make().remove_and_blocklist(1)


@respx.mock
@pytest.mark.parametrize(
    "effect",
    [
        httpx.ConnectError("boom"),
        httpx.ReadTimeout("slow"),
        httpx.Response(503),
        httpx.Response(500),
    ],
)
async def test_unreachable(effect: object) -> None:
    respx.get(f"{BASE}/api/v3/queue").mock(side_effect=[effect])
    respx.delete(f"{BASE}/api/v3/queue/1").mock(side_effect=[effect])
    with pytest.raises(ArrUnreachable):
        await make().find_queue_item(HASH)
    with pytest.raises(ArrUnreachable):
        await make().remove_and_blocklist(1)


@respx.mock
async def test_malformed_response_is_unreachable() -> None:
    respx.get(f"{BASE}/api/v3/queue").mock(return_value=httpx.Response(200, text="<html>"))
    with pytest.raises(ArrUnreachable):
        await make().find_queue_item(HASH)


@pytest.mark.parametrize(
    "rec",
    [
        {"downloadId": HASH.upper()},
        {"id": "abc", "downloadId": HASH.upper()},
        {"id": None, "downloadId": HASH.upper()},
        {"id": [1], "downloadId": HASH.upper()},
    ],
)
@respx.mock
async def test_malformed_record_id_is_unreachable(rec: dict[str, object]) -> None:
    respx.get(f"{BASE}/api/v3/queue").mock(return_value=page([rec], 1))
    with pytest.raises(ArrUnreachable) as ei:
        await make().find_queue_item(HASH)
    assert KEY not in str(ei.value)


@respx.mock
async def test_non_dict_record_is_unreachable() -> None:
    respx.get(f"{BASE}/api/v3/queue").mock(return_value=page(["oops"], 1))  # type: ignore[list-item]
    with pytest.raises(ArrUnreachable):
        await make().find_queue_item(HASH)


@respx.mock
async def test_injected_client_used_and_key_not_in_repr() -> None:
    respx.get(f"{BASE}/api/v3/queue").mock(
        return_value=page([{"id": 5, "downloadId": HASH.upper()}], 1)
    )
    async with httpx.AsyncClient() as hc:
        c = make(client=hc)
        assert await c.find_queue_item(HASH) == 5
    assert KEY not in repr(c)
