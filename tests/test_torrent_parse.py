from __future__ import annotations

import base64
from pathlib import Path
from typing import Any

import pytest

from bridge.torrent import (
    BencodeError,
    InvalidMagnetError,
    InvalidTorrentError,
    TorrentError,
    UnsafePathError,
    UnsupportedTorrentError,
    build_magnet,
    magnet_display_name,
    normalize_btih,
    parse_magnet,
    parse_torrent,
)
from bridge.torrent.bencode import decode

FIX = Path(__file__).parent / "fixtures" / "torrents"
H = "dd8255ecdc7ca55fb0bbf81323d87062db1f6d1c"


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


def single(name: str = "a.bin", **extra: Any) -> dict[str, Any]:
    return {"name": name, "length": 5, "piece length": 16384, "pieces": b"x" * 20, **extra}


def multi(paths: list[list[bytes]], name: str = "root") -> dict[str, Any]:
    return {
        "name": name,
        "piece length": 16384,
        "pieces": b"x" * 20,
        "files": [{"length": 1, "path": p} for p in paths],
    }


# bencode
def test_bencode_basic() -> None:
    assert decode(b"d1:ai1e1:bl1:xee") == {b"a": 1, b"b": [b"x"]}
    assert decode(b"i-5e") == -5


@pytest.mark.parametrize(
    "bad",
    [
        b"",
        b"i1",
        b"ie",
        b"i-0e",
        b"i01e",
        b"5:abc",
        b"l",
        b"d1:a",
        b"di1e1:ae",
        b"x",
        b"i1ei2e",
        b"d1:ai1e1:ai2ee",
        b"01:a",
        b"l" * 100 + b"e" * 100,
    ],
)
def test_bencode_invalid(bad: bytes) -> None:
    with pytest.raises(BencodeError):
        decode(bad)


# fixtures
def test_bbb() -> None:
    t = parse_torrent((FIX / "big-buck-bunny.torrent").read_bytes())
    assert t.info_hash == H
    assert t.is_multi_file and len(t.files) == 3
    assert t.root_name == t.name
    assert all(not f.rel_path.startswith(t.name + "/") for f in t.files)
    assert t.trackers


def test_sintel() -> None:
    t = parse_torrent((FIX / "sintel.torrent").read_bytes())
    assert t.info_hash == "08ada5a7a6183aae1e09d831df6748d566095a10"
    assert len(t.files) == 11
    assert t.total_size == sum(f.size for f in t.files)


def test_ubuntu_single() -> None:
    t = parse_torrent((FIX / "ubuntu-24.04.3-live-server-amd64.iso.torrent").read_bytes())
    assert t.info_hash == "a1dfefec1a9dd7fa8a041ebeeea271db55126d2f"
    assert not t.is_multi_file
    assert len(t.files) == 1
    assert t.files[0].rel_path == t.name == "ubuntu-24.04.3-live-server-amd64.iso"
    assert t.files[0].size == 3303444480


def test_to_magnet_roundtrip() -> None:
    t = parse_torrent((FIX / "big-buck-bunny.torrent").read_bytes())
    m = parse_magnet(t.to_magnet())
    assert m.info_hash == H
    assert m.display_name == t.name
    assert m.trackers == t.trackers
    assert len(set(m.trackers)) == len(m.trackers)


def test_trackers_dedup() -> None:
    data = mk(
        single(),
        announce="http://a/x",
        **{"announce-list": [["http://a/x", "http://b/y"], ["http://b/y"], ["udp://c:1"]]},
    )
    assert parse_torrent(data).trackers == ("http://a/x", "http://b/y", "udp://c:1")


# magnet
def test_magnet_hex_and_base32() -> None:
    b32 = base64.b32encode(bytes.fromhex(H)).decode()
    assert len(b32) == 32
    assert parse_magnet(f"magnet:?xt=urn:btih:{H.upper()}").info_hash == H
    assert parse_magnet(f"magnet:?xt=urn:btih:{b32}").info_hash == H
    assert parse_magnet(f"magnet:?xt=urn:btih:{b32.lower()}").info_hash == H
    assert normalize_btih(H.upper()) == H


def test_magnet_params_and_display_name() -> None:
    uri = f"magnet:?xt=urn:btih:{H}&dn=My%20Movie&tr=http%3A%2F%2Fa%2Fann&tr=http%3A%2F%2Fa%2Fann"
    m = parse_magnet(uri)
    assert m.display_name == "My Movie"
    assert m.trackers == ("http://a/ann",)
    assert magnet_display_name(m) == "My Movie"
    assert magnet_display_name(parse_magnet(f"magnet:?xt=urn:btih:{H}")) == H


def test_build_magnet_roundtrip() -> None:
    uri = build_magnet(H.upper(), "a b&c", ["udp://t:1/announce?x=1", "udp://t:1/announce?x=1"])
    m = parse_magnet(uri)
    assert (m.info_hash, m.display_name, m.trackers) == (H, "a b&c", ("udp://t:1/announce?x=1",))


@pytest.mark.parametrize(
    "bad",
    [
        "",
        "http://x",
        "magnet:?dn=x",
        "magnet:?xt=urn:btih:zz",
        "magnet:?xt=urn:btmh:" + H,
        "magnet:?xt=urn:btih:" + H[:-1],
        f"magnet:?xt=urn:btih:{H}&xt=urn:btih:{H}",
    ],
)
def test_magnet_invalid(bad: str) -> None:
    with pytest.raises(InvalidMagnetError):
        parse_magnet(bad)


# invalid torrents
def test_not_bencode() -> None:
    with pytest.raises(TorrentError):
        parse_torrent(b"garbage")


@pytest.mark.parametrize(
    "data",
    [
        enc([1]),
        enc({"a": 1}),
        mk({"name": "a"}),
        mk({"name": "a", "length": 1, "pieces": b"x" * 19}),
        mk({"length": 1, "pieces": b"x" * 20}),
        mk(single(length=-1)),
        mk({"name": "a", "pieces": b"x" * 20, "files": []}),
        mk(single(files=[])),
        mk(multi([[b"a"], [b"a"]])),
        mk({"name": "r", "pieces": b"x" * 20, "files": [{"length": "x", "path": [b"a"]}]}),
    ],
)
def test_invalid_structure(data: bytes) -> None:
    with pytest.raises(InvalidTorrentError):
        parse_torrent(data)


def test_v2_only_unsupported() -> None:
    data = mk({"name": "a", "meta version": 2, "piece length": 16384, "file tree": {}})
    with pytest.raises(UnsupportedTorrentError):
        parse_torrent(data)


@pytest.mark.parametrize(
    "path",
    [
        [b".."],
        [b"a", b"..", b"b"],
        [b""],
        [b"a/b"],
        [b"/etc"],
        [b"C:"],
        [b"a\\b"],
        [],
        [b"a\x00"],
    ],
)
def test_path_traversal(path: list[bytes]) -> None:
    with pytest.raises(UnsafePathError):
        parse_torrent(mk(multi([path])))


@pytest.mark.parametrize("name", ["..", ".", "", "a/b", "/abs"])
def test_unsafe_name(name: str) -> None:
    with pytest.raises(UnsafePathError):
        parse_torrent(mk(single(name)))


def test_multi_rel_path_excludes_root() -> None:
    t = parse_torrent(mk(multi([[b"d", b"x.mkv"], [b"y.txt"]], name="Root")))
    assert t.root_name == "Root"
    assert [f.rel_path for f in t.files] == ["d/x.mkv", "y.txt"]
