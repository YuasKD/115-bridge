"""种子解析相关异常。"""

from __future__ import annotations


class TorrentError(ValueError):
    """种子或 magnet 解析失败的基类。"""


class BencodeError(TorrentError):
    """bencode 数据非法。"""


class InvalidTorrentError(TorrentError):
    """种子结构非法（缺字段、类型不对等）。"""


class UnsupportedTorrentError(TorrentError):
    """不支持的种子类型（例如纯 BTv2）。"""


class UnsafePathError(TorrentError):
    """种子内路径含穿越、绝对路径或空段等危险内容。"""


class InvalidMagnetError(TorrentError):
    """magnet 链接非法。"""
