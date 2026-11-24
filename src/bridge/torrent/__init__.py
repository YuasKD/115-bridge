"""种子解析：bencode、BTIH、清单、magnet。"""

from bridge.torrent.errors import (
    BencodeError,
    InvalidMagnetError,
    InvalidTorrentError,
    TorrentError,
    UnsafePathError,
    UnsupportedTorrentError,
)
from bridge.torrent.magnet import (
    Magnet,
    build_magnet,
    magnet_display_name,
    normalize_btih,
    parse_magnet,
)
from bridge.torrent.meta import TorrentInfo, parse_torrent

__all__ = [
    "BencodeError",
    "InvalidMagnetError",
    "InvalidTorrentError",
    "Magnet",
    "TorrentError",
    "TorrentInfo",
    "UnsafePathError",
    "UnsupportedTorrentError",
    "build_magnet",
    "magnet_display_name",
    "normalize_btih",
    "parse_magnet",
    "parse_torrent",
]
