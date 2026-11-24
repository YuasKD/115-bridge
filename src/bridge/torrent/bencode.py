"""最小 bencode 解码器，只用标准库。

解码结果：int、bytes、list、dict[bytes, ...]。
`decode_with_info_span` 额外返回顶层 `info` 值的原始字节区间，用于计算 BTIH。
"""

from __future__ import annotations

from typing import Any

from bridge.torrent.errors import BencodeError

_MAX_DEPTH = 64
_MAX_INT_DIGITS = 40


class _Decoder:
    def __init__(self, data: bytes) -> None:
        self.data = data
        self.pos = 0
        self.info_span: tuple[int, int] | None = None

    def _peek(self) -> bytes:
        return self.data[self.pos : self.pos + 1]

    def decode(self, depth: int = 0) -> Any:
        if depth > _MAX_DEPTH:
            raise BencodeError("嵌套过深")
        c = self._peek()
        if not c:
            raise BencodeError("数据意外结束")
        if c == b"i":
            return self._int()
        if c == b"l":
            self.pos += 1
            items: list[Any] = []
            while True:
                nxt = self._peek()
                if not nxt:
                    raise BencodeError("列表未结束")
                if nxt == b"e":
                    self.pos += 1
                    return items
                items.append(self.decode(depth + 1))
        if c == b"d":
            return self._dict(depth)
        if c.isdigit():
            return self._bytes()
        raise BencodeError(f"位置 {self.pos} 处出现非法字节 {c!r}")

    def _int(self) -> int:
        end = self.data.find(b"e", self.pos)
        if end == -1:
            raise BencodeError("整数未结束")
        raw = self.data[self.pos + 1 : end]
        if not raw or len(raw) > _MAX_INT_DIGITS:
            raise BencodeError("整数非法")
        body = raw[1:] if raw.startswith(b"-") else raw
        if not body.isdigit():
            raise BencodeError(f"整数非法: {raw!r}")
        if (body.startswith(b"0") and len(body) > 1) or raw == b"-0":
            raise BencodeError(f"整数含前导零或为负零: {raw!r}")
        self.pos = end + 1
        return int(raw)

    def _bytes(self) -> bytes:
        colon = self.data.find(b":", self.pos)
        if colon == -1:
            raise BencodeError("字符串长度后缺少冒号")
        raw = self.data[self.pos : colon]
        if not raw.isdigit() or len(raw) > 12 or (raw.startswith(b"0") and len(raw) > 1):
            raise BencodeError(f"字符串长度非法: {raw!r}")
        start = colon + 1
        end = start + int(raw)
        if end > len(self.data):
            raise BencodeError("字符串长度超出数据")
        self.pos = end
        return self.data[start:end]

    def _dict(self, depth: int) -> dict[bytes, Any]:
        self.pos += 1
        result: dict[bytes, Any] = {}
        while True:
            nxt = self._peek()
            if not nxt:
                raise BencodeError("字典未结束")
            if nxt == b"e":
                self.pos += 1
                return result
            if not nxt.isdigit():
                raise BencodeError("字典键必须是字符串")
            key = self._bytes()
            if key in result:
                raise BencodeError(f"字典键重复: {key!r}")
            start = self.pos
            value = self.decode(depth + 1)
            if depth == 0 and key == b"info":
                self.info_span = (start, self.pos)
            result[key] = value


def decode(data: bytes) -> Any:
    """解码完整的 bencode 数据；尾部多余字节视为非法。"""
    value, _ = decode_with_info_span(data)
    return value


def decode_with_info_span(
    data: bytes, *, strict: bool = True
) -> tuple[Any, tuple[int, int] | None]:
    """解码并返回顶层 `info` 值在原始数据中的 [start, end) 区间（没有则 None）。

    字典键顺序不强制（真实种子不一定严格排序），BTIH 按原始字节计算，不受影响。
    strict=False 时忽略顶层值之后的多余字节（.torrent 解析用）。
    """
    if not isinstance(data, (bytes, bytearray)):
        raise BencodeError("输入必须是 bytes")
    dec = _Decoder(bytes(data))
    value = dec.decode()
    if strict and dec.pos != len(dec.data):
        raise BencodeError("数据末尾有多余字节")
    return value, dec.info_span
