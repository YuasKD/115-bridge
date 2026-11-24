import io
import json
import logging

import pytest

from bridge.log import JsonFormatter, RedactingFilter, log_event, redact

FAKE_COOKIE = "UID=1234_fake_uid; CID=fakecid0000; SEID=fakeseid1111; KID=fakekid2222"


def test_redact_cookie() -> None:
    out = redact(f"cookie: {FAKE_COOKIE}")
    for v in ("1234_fake_uid", "fakecid0000", "fakeseid1111", "fakekid2222"):
        assert v not in out
    assert "UID=[REDACTED]" in out


@pytest.mark.parametrize(
    ("text", "secret"),
    [
        ("Authorization: Bearer fake.token-value_123", "fake.token-value_123"),
        ("X-Api-Key: fakeapikey0123456789", "fakeapikey0123456789"),
        ("GET http://r:7878/api/v3/queue?apikey=fakekey999&page=1", "fakekey999"),
        ("url=http://x/?token=faketoken777&a=b", "faketoken777"),
        (
            "https://api.telegram.org/bot123456789:FAKE-token_abcdefghij/sendMessage",
            "FAKE-token_abcdefghij",
        ),
        ('{"apikey": "fakejsonkey"}', "fakejsonkey"),
    ],
)
def test_redact_secrets(text: str, secret: str) -> None:
    out = redact(text)
    assert secret not in out
    assert "[REDACTED]" in out


def test_redact_keeps_normal_text() -> None:
    assert redact("task abc finished stage=fetch") == "task abc finished stage=fetch"


def test_filter_and_json_output() -> None:
    buf = io.StringIO()
    h = logging.StreamHandler(buf)
    h.setFormatter(JsonFormatter())
    h.addFilter(RedactingFilter())
    lg = logging.getLogger("t.redact")
    lg.handlers[:] = [h]
    lg.setLevel(logging.INFO)
    lg.propagate = False
    log_event(lg, logging.INFO, "login", hash="abc", stage="submit", cookie_hdr=FAKE_COOKIE)
    lg.info("args %s", "apikey=fakekey999")
    lines = [json.loads(x) for x in buf.getvalue().splitlines()]
    assert lines[0]["hash"] == "abc"
    assert lines[0]["stage"] == "submit"
    assert "fakecid0000" not in buf.getvalue()
    assert "fakekey999" not in buf.getvalue()


def test_setup_logging_quiets_httpx() -> None:
    from bridge.log import setup_logging

    root = logging.getLogger()
    saved = (root.handlers[:], root.level)
    try:
        for name in ("httpx", "httpcore"):
            logging.getLogger(name).setLevel(logging.NOTSET)
        setup_logging(logging.INFO)
        for name in ("httpx", "httpcore"):
            assert logging.getLogger(name).getEffectiveLevel() == logging.WARNING
        assert root.level == logging.INFO
    finally:
        root.handlers[:], root.level = saved[0], saved[1]
