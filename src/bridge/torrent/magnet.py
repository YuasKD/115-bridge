"""magnet 解析与生成。BTIH 统一小写 hex。"""

from __future__ import annotations

import base64
import re
from collections.abc import Iterable
from dataclasses import dataclass
from urllib.parse import parse_qsl, quote, urlsplit

from bridge.torrent.errors import InvalidMagnetError

_HEX40 = re.compile(r"^[0-9a-fA-F]{40}$")
_BASE32 = re.compile(r"^[A-Za-z2-7]{32}$")


@dataclass(frozen=True, slots=True)
class Magnet:
    info_hash: str  # 小写 40 位 hex
    display_name: str | None = None
    trackers: tuple[str, ...] = ()


def normalize_btih(value: str) -> str:
    """40 位 hex 或 32 位 base32 转为小写 hex，其他抛 InvalidMagnetError。"""
    v = value.strip()
    if _HEX40.match(v):
        return v.lower()
    if _BASE32.match(v):
        return base64.b32decode(v.upper()).hex()
    raise InvalidMagnetError(f"BTIH 必须是 40 位 hex 或 32 位 base32: {value!r}")


def parse_magnet(uri: str) -> Magnet:
    parts = urlsplit(uri.strip())
    if parts.scheme.lower() != "magnet":
        raise InvalidMagnetError("不是 magnet 链接")
    params = parse_qsl(parts.query, keep_blank_values=False)
    btih: str | None = None
    prefix = "urn:btih:"
    for key, val in params:
        if key.lower() == "xt" and val.lower().startswith(prefix):
            if btih is not None:
                raise InvalidMagnetError("magnet 含多个 btih")
            btih = normalize_btih(val[len(prefix) :])
    if btih is None:
        raise InvalidMagnetError("magnet 缺少 xt=urn:btih:")
    dn = next((v for k, v in params if k.lower() == "dn" and v), None)
    trackers = _dedup(v for k, v in params if k.lower() == "tr" and v)
    return Magnet(info_hash=btih, display_name=dn, trackers=tuple(trackers))


def build_magnet(
    info_hash: str, display_name: str | None = None, trackers: Iterable[str] = ()
) -> str:
    """生成 magnet：xt、可选 dn、去重后的 tr。"""
    out = f"magnet:?xt=urn:btih:{normalize_btih(info_hash)}"
    if display_name:
        out += "&dn=" + quote(display_name, safe="")
    for tr in _dedup(trackers):
        out += "&tr=" + quote(tr, safe="")
    return out


def magnet_display_name(magnet: Magnet) -> str:
    """元数据未知时的显示名：dn，没有则 hash。"""
    return magnet.display_name or magnet.info_hash


def _dedup(items: Iterable[str]) -> list[str]:
    seen: set[str] = set()
    out: list[str] = []
    for it in items:
        if it not in seen:
            seen.add(it)
            out.append(it)
    return out
