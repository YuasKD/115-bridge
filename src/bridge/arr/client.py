"""Radarr / Sonarr 队列客户端（T05）。

实现 ``ArrClient``：按 downloadId 找队列项，再 remove + blocklist。
API Key 只通过请求头 ``X-Api-Key`` 发送，不进日志、异常消息或 repr。
"""

from __future__ import annotations

import logging
from typing import Any, Literal

import httpx

from bridge.arr.protocol import ArrAuthError, ArrUnreachable, QueueItemNotFound
from bridge.log import log_event

logger = logging.getLogger(__name__)

ArrType = Literal["radarr", "sonarr"]

# Radarr 用 includeUnknownMovieItems，Sonarr 用 includeUnknownSeriesItems。
_UNKNOWN_PARAM: dict[str, str] = {
    "radarr": "includeUnknownMovieItems",
    "sonarr": "includeUnknownSeriesItems",
}

DEFAULT_TIMEOUT = 15.0
DEFAULT_PAGE_SIZE = 200
_MAX_PAGES = 1000  # 防御：服务端异常时避免死循环


class HttpArrClient:
    def __init__(
        self,
        name: str,
        arr_type: ArrType,
        base_url: str,
        api_key: str,
        *,
        client: httpx.AsyncClient | None = None,
        timeout: float = DEFAULT_TIMEOUT,
        page_size: int = DEFAULT_PAGE_SIZE,
    ) -> None:
        if arr_type not in _UNKNOWN_PARAM:
            raise ValueError(f"unsupported arr type: {arr_type!r}")
        self.name = name
        self._type: ArrType = arr_type
        self._base_url = base_url.rstrip("/")
        self._api_key = api_key
        self._client = client
        self._timeout = timeout
        self._page_size = page_size

    def __repr__(self) -> str:
        return (
            f"HttpArrClient(name={self.name!r}, type={self._type!r}, base_url={self._base_url!r})"
        )

    async def _request(self, method: str, path: str, params: dict[str, Any]) -> httpx.Response:
        url = f"{self._base_url}/api/v3{path}"
        headers = {"X-Api-Key": self._api_key}
        try:
            if self._client is not None:
                resp = await self._client.request(
                    method, url, params=params, headers=headers, timeout=self._timeout
                )
            else:
                async with httpx.AsyncClient(timeout=self._timeout) as c:
                    resp = await c.request(method, url, params=params, headers=headers)
        except httpx.HTTPError as exc:
            # 只记异常类型，避免 URL 或头信息外泄。
            raise ArrUnreachable(f"{self.name}: {type(exc).__name__}") from None
        if resp.status_code in (401, 403):
            raise ArrAuthError(f"{self.name}: HTTP {resp.status_code}")
        if resp.status_code >= 500:
            raise ArrUnreachable(f"{self.name}: HTTP {resp.status_code}")
        return resp

    async def find_queue_item(self, info_hash: str) -> int:
        target = info_hash.upper()
        params: dict[str, Any] = {
            _UNKNOWN_PARAM[self._type]: "false",
            "pageSize": self._page_size,
        }
        seen = 0
        for page in range(1, _MAX_PAGES + 1):
            params["page"] = page
            resp = await self._request("GET", "/queue", params)
            if resp.status_code != 200:
                raise ArrUnreachable(f"{self.name}: unexpected HTTP {resp.status_code}")
            try:
                data = resp.json()
                records = data["records"]
                total = int(data.get("totalRecords", 0))
            except (ValueError, KeyError, TypeError):
                raise ArrUnreachable(f"{self.name}: malformed queue response") from None
            try:
                for rec in records:
                    download_id = rec.get("downloadId")
                    if isinstance(download_id, str) and download_id.upper() == target:
                        return int(rec["id"])
            except (ValueError, KeyError, TypeError, AttributeError):
                raise ArrUnreachable(f"{self.name}: malformed queue response") from None
            seen += len(records)
            if not records or seen >= total:
                break
        log_event(logger, logging.INFO, "arr queue item not found", hash=info_hash, stage="arr")
        raise QueueItemNotFound(f"{self.name}: no queue item for {target}")

    async def remove_and_blocklist(self, queue_id: int) -> None:
        params = {
            "removeFromClient": "true",
            "blocklist": "true",
            "skipRedownload": "false",
        }
        resp = await self._request("DELETE", f"/queue/{queue_id}", params)
        if resp.status_code == 404:
            raise QueueItemNotFound(f"{self.name}: queue id {queue_id} not found")
        if resp.status_code >= 300:
            raise ArrUnreachable(f"{self.name}: unexpected HTTP {resp.status_code}")
