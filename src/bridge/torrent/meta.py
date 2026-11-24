"""种子（.torrent）解析：BTIH、清单、tracker。

清单约定：`files[].rel_path` 相对 115 任务根目录（任务根目录名就是 `root_name`），
因此多文件种子的 rel_path 不含 info.name；单文件种子的 rel_path 就是文件名。
"""

from __future__ import annotations

import hashlib
from dataclasses import dataclass
from typing import Any

from bridge.db.repo import ManifestFile
from bridge.torrent.bencode import decode_with_info_span
from bridge.torrent.errors import (
    InvalidTorrentError,
    UnsafePathError,
    UnsupportedTorrentError,
)
from bridge.torrent.magnet import build_magnet


@dataclass(frozen=True, slots=True)
class TorrentInfo:
    info_hash: str  # 小写 40 位 hex
    name: str  # info.name
    root_name: str  # 任务根目录名，等于 info.name
    is_multi_file: bool
    files: tuple[ManifestFile, ...]  # rel_path 相对任务根目录，用 / 分隔
    trackers: tuple[str, ...]  # announce 与 announce-list 去重后

    @property
    def total_size(self) -> int:
        return sum(f.size for f in self.files)

    def to_magnet(self) -> str:
        return build_magnet(self.info_hash, self.name, self.trackers)


def _check_segment(seg: str, what: str) -> str:
    if seg in ("", ".", ".."):
        raise UnsafePathError(f"{what} 含非法路径段: {seg!r}")
    if "/" in seg or "\\" in seg or chr(0) in seg:
        raise UnsafePathError(f"{what} 路径段含分隔符或空字符: {seg!r}")
    if len(seg) >= 2 and seg[1] == ":" and seg[0].isalpha():
        raise UnsafePathError(f"{what} 像盘符绝对路径: {seg!r}")
    return seg


def _decode_text(raw: bytes, utf8_raw: object, encoding: str | None, what: str) -> str:
    """优先 *.utf-8 字段，其次原字段按 UTF-8，最后按顶层 encoding 解码。"""
    if isinstance(utf8_raw, bytes):
        try:
            return utf8_raw.decode("utf-8")
        except UnicodeDecodeError:
            pass
    try:
        return raw.decode("utf-8")
    except UnicodeDecodeError:
        pass
    if encoding:
        try:
            return raw.decode(encoding)
        except (UnicodeDecodeError, LookupError):
            pass
    raise InvalidTorrentError(f"{what} 无法解码（非 UTF-8 且无可用 encoding）")


def _safe_segment(
    raw: bytes, what: str, utf8_raw: object = None, encoding: str | None = None
) -> str:
    return _check_segment(_decode_text(raw, utf8_raw, encoding, what), what)


def _top_encoding(top: dict[bytes, Any]) -> str | None:
    enc = top.get(b"encoding")
    if isinstance(enc, bytes):
        return enc.decode("ascii", errors="ignore").strip() or None
    return None


def _get(d: dict[bytes, Any], key: bytes, typ: type, what: str) -> Any:
    if key not in d:
        raise InvalidTorrentError(f"缺少字段 {what}")
    v = d[key]
    if not isinstance(v, typ) or isinstance(v, bool):
        raise InvalidTorrentError(f"字段 {what} 类型错误")
    return v


def _trackers(top: dict[bytes, Any]) -> tuple[str, ...]:
    urls: list[str] = []
    ann = top.get(b"announce")
    if isinstance(ann, bytes):
        urls.append(ann.decode("utf-8", errors="ignore"))
    al = top.get(b"announce-list")
    if isinstance(al, list):
        for tier in al:
            for u in tier if isinstance(tier, list) else [tier]:
                if isinstance(u, bytes):
                    urls.append(u.decode("utf-8", errors="ignore"))
    seen: set[str] = set()
    out: list[str] = []
    for u in urls:
        u = u.strip()
        if u and u not in seen:
            seen.add(u)
            out.append(u)
    return tuple(out)


def parse_torrent(data: bytes) -> TorrentInfo:
    """解析 .torrent 字节。非法输入抛 TorrentError 的子类。"""
    top, span = decode_with_info_span(data, strict=False)
    if not isinstance(top, dict):
        raise InvalidTorrentError("顶层不是字典")
    if span is None:
        raise InvalidTorrentError("缺少 info 字典")
    info = _get(top, b"info", dict, "info")
    info_hash = hashlib.sha1(data[span[0] : span[1]]).hexdigest()

    if not (b"pieces" in info and (b"length" in info or b"files" in info)):
        if info.get(b"meta version") == 2 or b"file tree" in info:
            raise UnsupportedTorrentError("不支持纯 BTv2 种子")
        raise InvalidTorrentError("info 缺少 pieces 与 length/files")
    pieces = _get(info, b"pieces", bytes, "info.pieces")
    if len(pieces) == 0 or len(pieces) % 20 != 0:
        raise InvalidTorrentError("info.pieces 长度必须是 20 的非零倍数")

    encoding = _top_encoding(top)
    name = _safe_segment(
        _get(info, b"name", bytes, "info.name"),
        "info.name",
        info.get(b"name.utf-8"),
        encoding,
    )

    files: list[ManifestFile] = []
    if b"files" in info:
        if b"length" in info:
            raise InvalidTorrentError("info 同时含 length 与 files")
        raw_files = _get(info, b"files", list, "info.files")
        if not raw_files:
            raise InvalidTorrentError("info.files 为空")
        seen: set[str] = set()
        for i, f in enumerate(raw_files):
            if not isinstance(f, dict):
                raise InvalidTorrentError(f"files[{i}] 不是字典")
            length = _get(f, b"length", int, f"files[{i}].length")
            if length < 0:
                raise InvalidTorrentError(f"files[{i}].length 为负")
            path = _get(f, b"path", list, f"files[{i}].path")
            if not path:
                raise UnsafePathError(f"files[{i}].path 为空")
            path_u8 = f.get(b"path.utf-8")
            if not isinstance(path_u8, list) or len(path_u8) != len(path):
                path_u8 = [None] * len(path)
            segs: list[str] = []
            for s, u in zip(path, path_u8, strict=True):
                if not isinstance(s, bytes):
                    raise InvalidTorrentError(f"files[{i}].path 含非字符串")
                segs.append(_safe_segment(s, f"files[{i}].path", u, encoding))
            rel = "/".join(segs)
            if rel in seen:
                raise InvalidTorrentError(f"文件路径重复: {rel}")
            seen.add(rel)
            files.append(ManifestFile(rel_path=rel, size=length))
        multi = True
    else:
        length = _get(info, b"length", int, "info.length")
        if length < 0:
            raise InvalidTorrentError("info.length 为负")
        files.append(ManifestFile(rel_path=name, size=length))
        multi = False

    return TorrentInfo(
        info_hash=info_hash,
        name=name,
        root_name=name,
        is_multi_file=multi,
        files=tuple(files),
        trackers=_trackers(top),
    )
