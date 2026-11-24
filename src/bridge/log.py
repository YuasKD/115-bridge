"""Structured logging helpers and secret redaction."""

from __future__ import annotations

import json
import logging
import re
from typing import Any

REDACTED = "[REDACTED]"
NOISY_LOGGERS = ("httpx", "httpcore")

# Order matters: more specific patterns first.
_PATTERNS: list[tuple[re.Pattern[str], str]] = [
    # Telegram bot token in URLs or free text: bot<digits>:<token>
    (re.compile(r"bot\d{5,}:[A-Za-z0-9_-]{10,}"), f"bot{REDACTED}"),
    # Authorization: Bearer xxx
    (re.compile(r"(?i)(bearer\s+)[A-Za-z0-9._~+/=-]+"), rf"\1{REDACTED}"),
    # Header style: X-Api-Key: xxx
    (re.compile(r"(?i)(x-api-key\s*[:=]\s*)[^\s,;&\"']+"), rf"\1{REDACTED}"),
    # key=value (URL query etc.) for apikey, api_key, token, access_token
    (re.compile(r"(?i)\b((?:api[_-]?key|access[_-]?token|token)=)[^\s&\"']+"), rf"\1{REDACTED}"),
    # JSON-ish "apikey": "xxx"
    (
        re.compile(
            r"(?i)([\"'](?:api[_-]?key|x-api-key|token|access_token)[\"']\s*:\s*[\"'])[^\"']*"
        ),
        rf"\1{REDACTED}",
    ),
    # 115 cookie key=value pairs
    (re.compile(r"\b(UID|CID|SEID|KID)=[^\s;,\"']+"), rf"\1={REDACTED}"),
]


def redact(text: str) -> str:
    """Return ``text`` with cookies, tokens and API keys masked."""
    for pattern, repl in _PATTERNS:
        text = pattern.sub(repl, text)
    return text


def redact_value(value: Any) -> Any:
    if isinstance(value, str):
        return redact(value)
    if isinstance(value, dict):
        return {k: redact_value(v) for k, v in value.items()}
    if isinstance(value, list):
        return [redact_value(v) for v in value]
    if isinstance(value, tuple):
        return tuple(redact_value(v) for v in value)
    return value


_STD_ATTRS = frozenset(logging.makeLogRecord({}).__dict__) | {"message", "asctime"}


class RedactingFilter(logging.Filter):
    """Mask secrets in the message, its args and extra structured fields."""

    def filter(self, record: logging.LogRecord) -> bool:
        record.msg = redact(record.getMessage())
        record.args = None
        for key, value in list(record.__dict__.items()):
            if key not in _STD_ATTRS:
                record.__dict__[key] = redact_value(value)
        return True


class JsonFormatter(logging.Formatter):
    """One JSON object per line; includes ``hash`` and ``stage`` when provided."""

    def format(self, record: logging.LogRecord) -> str:
        payload: dict[str, Any] = {
            "ts": self.formatTime(record),
            "level": record.levelname,
            "logger": record.name,
            "msg": record.getMessage(),
        }
        for key, value in record.__dict__.items():
            if key not in _STD_ATTRS:
                payload[key] = value
        if record.exc_info:
            payload["exc"] = redact(self.formatException(record.exc_info))
        return json.dumps(payload, ensure_ascii=False, default=str)


def setup_logging(level: int | str = logging.INFO) -> None:
    handler = logging.StreamHandler()
    handler.setFormatter(JsonFormatter())
    handler.addFilter(RedactingFilter())
    root = logging.getLogger()
    root.handlers[:] = [handler]
    root.setLevel(level)
    # httpx 在 INFO 会把带签名的完整 CDN 链接写进日志，压到 WARNING
    for noisy in NOISY_LOGGERS:
        logging.getLogger(noisy).setLevel(logging.WARNING)


def log_event(
    logger: logging.Logger,
    level: int,
    msg: str,
    *,
    hash: str | None = None,  # noqa: A002
    stage: str | None = None,
    **fields: Any,
) -> None:
    """Log with structured ``hash`` and ``stage`` fields."""
    extra: dict[str, Any] = dict(fields)
    if hash is not None:
        extra["hash"] = hash
    if stage is not None:
        extra["stage"] = stage
    logger.log(level, msg, extra=extra)
