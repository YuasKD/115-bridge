"""kv 表的键与值编解码（中立模块，pipeline 与 api 共用，避免 api 依赖 pipeline 实现）。"""

from __future__ import annotations

from datetime import UTC, datetime

from bridge.cloud.protocol import Quota
from bridge.db.repo import TaskRepo

KV_QUOTA = "cloud.quota"
"""最近一次读到的配额，格式 ``<remaining>/<total>@<读取时间 unix 秒>``。track 与 submit 写入。"""

KV_DIR_PREFIX = "cloud.dir:"
"""保存目录 id 缓存，键为 ``cloud.dir:<绝对路径>``。清除时写空串（视为未缓存）。"""


def encode_quota(quota: Quota, read_at: datetime) -> str:
    return f"{quota.remaining}/{quota.total}@{read_at.timestamp():.3f}"


def decode_quota(raw: str | None) -> tuple[Quota, datetime] | None:
    """解析 kv 里的配额。格式不对或没有读取时间返回 None（视为需要刷新）。"""
    if raw is None:
        return None
    body, at_sep, at_raw = raw.partition("@")
    left, sep, right = body.partition("/")
    if not sep or not at_sep:
        return None
    try:
        return Quota(int(left), int(right)), datetime.fromtimestamp(float(at_raw), UTC)
    except ValueError:
        return None


async def store_quota(repo: TaskRepo, quota: Quota, read_at: datetime) -> None:
    await repo.kv_set(KV_QUOTA, encode_quota(quota, read_at))


__all__ = [
    "KV_DIR_PREFIX",
    "KV_QUOTA",
    "decode_quota",
    "encode_quota",
    "store_quota",
]
