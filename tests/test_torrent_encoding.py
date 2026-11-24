from __future__ import annotations

from typing import Any

import pytest

from bridge.torrent import (
    BencodeError,
    InvalidTorrentError,
    UnsafePathError,
    parse_torrent,
)
from bridge.torrent.bencode import decode

GBK_NAME = "中文名".encode("gbk")


def enc(o: Any) -> bytes:
    if isinstance(o, int):
        return b"i%de" % o
    if isinstance(o, bytes):
        return b"%d:%s" % (len(o), o)
    if isinstance(o, str):
        return enc(o.encode())
    if isinstance(o, list):
        return b"l" + b"".join(enc(x) for x in o) + b"e"
    assert isinstance(o, dict)
    return b"d" + b"".join(enc(k) + enc(v) for k, v in sorted(o.items())) + b"e"


def mk(info: dict[str, Any], **top: Any) -> bytes:
    return enc({"info": info, **top})


def single(name: str | bytes = "a.bin", **extra: Any) -> dict[str, Any]:
    return {"name": name, "length": 5, "piece length": 16384, "pieces": b"x" * 20, **extra}


def multi(paths: list[list[bytes]], name: str = "root") -> dict[str, Any]:
    return {
        "name": name,
        "piece length": 16384,
        "pieces": b"x" * 20,
        "files": [{"length": 1, "path": p} for p in paths],
    }


def test_name_utf8_field_preferred() -> None:
    t = parse_torrent(mk(single(GBK_NAME, **{"name.utf-8": "中文名"})))
    assert t.name == "中文名"


def test_name_gbk_with_encoding() -> None:
    t = parse_torrent(mk(single(GBK_NAME), encoding="GBK"))
    assert t.name == "中文名"
    assert t.files[0].rel_path == "中文名"


def test_name_gbk_without_hints_rejected() -> None:
    with pytest.raises(InvalidTorrentError):
        parse_torrent(mk(single(GBK_NAME)))


def test_name_unknown_encoding_rejected() -> None:
    with pytest.raises(InvalidTorrentError):
        parse_torrent(mk(single(GBK_NAME), encoding="nope-9"))


def test_path_utf8_field_in_multi() -> None:
    info: dict[str, Any] = multi([[GBK_NAME, "文件.mkv".encode("gbk")]], name="r")
    info["files"][0]["path.utf-8"] = ["中文名", "文件.mkv"]
    t = parse_torrent(mk(info))
    assert t.files[0].rel_path == "中文名/文件.mkv"


def test_path_gbk_with_encoding_in_multi() -> None:
    t = parse_torrent(mk(multi([[GBK_NAME, b"a.mkv"]]), encoding="GBK"))
    assert t.files[0].rel_path == "中文名/a.mkv"


def test_invalid_utf8_field_falls_back() -> None:
    t = parse_torrent(mk(single("ok.bin", **{"name.utf-8": b"\xff\xfe"})))
    assert t.name == "ok.bin"


def test_utf8_field_dangerous_still_rejected() -> None:
    with pytest.raises(UnsafePathError):
        parse_torrent(mk(single("safe", **{"name.utf-8": ".."})))
    info: dict[str, Any] = multi([[b"ok", b"ok2"]])
    info["files"][0]["path.utf-8"] = ["a", "../x"]
    with pytest.raises(UnsafePathError):
        parse_torrent(mk(info))


def test_trailing_bytes_tolerated() -> None:
    base = mk(single())
    for tail in (b"\n", b"\r\n\x00\xff garbage", b"d1:a"):
        assert parse_torrent(base + tail).info_hash == parse_torrent(base).info_hash


def test_bencode_decode_strict_by_default() -> None:
    with pytest.raises(BencodeError):
        decode(b"i1e\n")
