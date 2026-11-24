"""Telegram push-only notifier.

Only ``sendMessage`` and ``sendPhoto`` are used; the bot never receives messages.
Failures are logged (with the token scrubbed) and never raised to the caller.

Dedup semantics: the window starts only after a *successful* send, so a failed
push does not suppress the next attempt. ``recovered`` clears the window of the
given kind and the QR window, so the next outage alerts immediately.
"""

from __future__ import annotations

import logging
from typing import Protocol

import httpx

from bridge.clock import Clock
from bridge.log import log_event, redact

logger = logging.getLogger(__name__)

DEDUP_WINDOW_SECONDS = 30 * 60
_API = "https://api.telegram.org"


class _TokenFilter(logging.Filter):
    """httpx logs full request URLs at INFO; mask the token there too."""

    def __init__(self, token: str) -> None:
        super().__init__()
        self._token = token

    def filter(self, record: logging.LogRecord) -> bool:
        record.msg = record.getMessage().replace(self._token, "[REDACTED]")
        record.args = None
        return True


class Notifier(Protocol):
    async def alert(self, kind: str, text: str) -> bool:
        """Push an alert, at most once per ``kind`` per window. True if sent."""
        ...

    async def send_qr(self, png: bytes, caption: str) -> bool:
        """Push a QR image, at most once per window. True if sent."""
        ...

    async def recovered(self, kind: str, text: str) -> bool:
        """Push a recovery notice and reset dedup windows for ``kind`` and QR."""
        ...


class NoopNotifier:
    """Used when Telegram is not configured."""

    async def alert(self, kind: str, text: str) -> bool:
        return False

    async def send_qr(self, png: bytes, caption: str) -> bool:
        return False

    async def recovered(self, kind: str, text: str) -> bool:
        return False


class TelegramNotifier:
    def __init__(
        self,
        token: str,
        chat_id: str,
        clock: Clock,
        client: httpx.AsyncClient | None = None,
        window_seconds: float = DEDUP_WINDOW_SECONDS,
    ) -> None:
        self._token = token
        self._chat_id = chat_id
        self._clock = clock
        self._client = client or httpx.AsyncClient(timeout=15.0)
        self._window = window_seconds
        self._last_alert: dict[str, float] = {}
        self._last_qr: float | None = None
        self._inflight: set[str] = set()
        logging.getLogger("httpx").addFilter(_TokenFilter(token))

    def _due(self, last: float | None) -> bool:
        return last is None or self._clock.monotonic() - last >= self._window

    async def alert(self, kind: str, text: str) -> bool:
        key = f"alert:{kind}"
        if key in self._inflight or not self._due(self._last_alert.get(kind)):
            return False
        self._inflight.add(key)
        try:
            ok = await self._call("sendMessage", {"chat_id": self._chat_id, "text": text})
            if ok:
                self._last_alert[kind] = self._clock.monotonic()
            return ok
        finally:
            self._inflight.discard(key)

    async def send_qr(self, png: bytes, caption: str) -> bool:
        if "qr" in self._inflight or not self._due(self._last_qr):
            return False
        self._inflight.add("qr")
        try:
            ok = await self._call(
                "sendPhoto",
                {"chat_id": self._chat_id, "caption": caption},
                files={"photo": ("qr.png", png, "image/png")},
            )
            if ok:
                self._last_qr = self._clock.monotonic()
            return ok
        finally:
            self._inflight.discard("qr")

    async def recovered(self, kind: str, text: str) -> bool:
        # Clear first: even if the push fails, the next outage must alert at once.
        self._last_alert.pop(kind, None)
        self._last_qr = None
        return await self._call("sendMessage", {"chat_id": self._chat_id, "text": text})

    def _scrub(self, text: str) -> str:
        return redact(text.replace(self._token, "[REDACTED]"))

    async def _call(
        self,
        method: str,
        data: dict[str, str],
        files: dict[str, tuple[str, bytes, str]] | None = None,
    ) -> bool:
        url = f"{_API}/bot{self._token}/{method}"
        try:
            resp = await self._client.post(url, data=data, files=files)
            if resp.status_code != 200:
                log_event(
                    logger,
                    logging.WARNING,
                    "telegram push failed",
                    method=method,
                    status=resp.status_code,
                )
                return False
            return True
        except Exception as exc:  # noqa: BLE001 - notifier must never raise
            log_event(
                logger,
                logging.WARNING,
                "telegram push error",
                method=method,
                error_type=type(exc).__name__,
                error=self._scrub(str(exc)),
            )
            return False
