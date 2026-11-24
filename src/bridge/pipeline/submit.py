"""提交（T09）：把任务的 magnet 提交成 115 离线任务。

- 本模块不做状态转换、不发告警（由调度器负责）；只调用 115、写 ``submitted_at`` 等进度字段，
  返回 ``SubmitResult`` 或抛出明确的异常。
- 秒取回拆在 ``check_instant``：调度器在任务还是 QUEUED 时先调用它，命中就把 QUEUED 转到
  LOCATING；不命中才转 SUBMITTING 再调 ``submit``。``submit`` 不会返回秒取回结果。
- 配额：kv 里的配额带读取时间。超过 ``poll_seconds`` 就重新 ``list_offline(RUNNING)`` 读一次；
  剩余低于 ``quota_min`` 时也强制刷新一次再判断，仍低才抛 ``QuotaLow``，不改任何任务。
- 保存目录缓存：每次提交前用 ``get_entry`` 核对缓存的目录 id 仍在且是目录；不在或不是目录就清
  缓存并 ``makedirs`` 重建（重建后同样核对）。``add_magnet`` 抛 ``CloudNotFound`` 时再清缓存、
  重建、重试一次。重建后仍拿不到可用目录，或重试后 ``add_magnet`` 仍抛 ``CloudNotFound``，抛
  ``SaveDirUnavailable``：问题在保存目录而不是片源，不是任务级失败。调度器应全局暂停提交并告警，
  不标 failed、不回退。
- ``SubmitRejected``、``QuotaExhausted`` 和全局类异常原样上抛；``QuotaLow``、
  ``SaveDirUnavailable`` 是本模块定义的暂停类异常（都不是 ``CloudError`` 子类）。
- 永不删除云端内容（不变量 2）：本模块只建目录和添加任务。
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime
from enum import StrEnum

from bridge.clock import Clock
from bridge.cloud.protocol import CloudClient, CloudNotFound, OfflineStat, Quota
from bridge.db.repo import CloudIndexEntry, Task, TaskNotFound, TaskRepo, TaskUpdate
from bridge.kv_keys import KV_DIR_PREFIX, KV_QUOTA, decode_quota, store_quota
from bridge.torrent.magnet import build_magnet

log = logging.getLogger(__name__)


class SubmitError(Exception):
    pass


class QuotaLow(SubmitError):
    """剩余离线次数低于 ``quota_min``（已强制刷新确认）。调度器据此暂停新提交并告警。"""

    def __init__(self, remaining: int, minimum: int) -> None:
        super().__init__(f"offline quota low: remaining={remaining} min={minimum}")
        self.remaining = remaining
        self.minimum = minimum


class SaveDirUnavailable(SubmitError):
    """分类保存目录重建后仍不可用。全局暂停提交并告警，不是任务级失败，不回退。"""

    def __init__(self, path: str, reason: str) -> None:
        super().__init__(f"save dir {path} unavailable: {reason}")
        self.path = path
        self.reason = reason


class SubmitKind(StrEnum):
    SUBMITTED = "submitted"  # 本次调用了 add_magnet 并写入 submitted_at
    ALREADY_SUBMITTED = "already_submitted"  # 任务已有 submitted_at，没有调用 115


@dataclass(frozen=True, slots=True)
class SubmitResult:
    kind: SubmitKind
    task: Task  # 写库之后的最新任务


def category_path(root_dir: str, category: str) -> str:
    return f"{root_dir.rstrip('/')}/{category}"


async def _read_quota(cloud: CloudClient, repo: TaskRepo, clock: Clock) -> tuple[Quota, datetime]:
    page = await cloud.list_offline(OfflineStat.RUNNING, 1)
    now = clock.now()
    await store_quota(repo, page.quota, now)
    return page.quota, now


async def _rebuild_dir(cloud: CloudClient, repo: TaskRepo, path: str) -> str:
    key = KV_DIR_PREFIX + path
    await repo.kv_set(key, "")
    dir_id = await cloud.makedirs(path)
    entry = await cloud.get_entry(dir_id)
    if entry is None or not entry.is_dir:
        raise SaveDirUnavailable(path, f"rebuilt directory {dir_id} is missing or not a directory")
    await repo.kv_set(key, dir_id)
    return dir_id


async def _category_dir_id(cloud: CloudClient, repo: TaskRepo, path: str) -> str:
    cached = await repo.kv_get(KV_DIR_PREFIX + path)
    if cached:
        entry = await cloud.get_entry(cached)
        if entry is not None and entry.is_dir:
            return cached
        log.warning("cached save dir invalid, rebuilding", extra={"stage": "submit", "path": path})
    return await _rebuild_dir(cloud, repo, path)


async def check_instant(
    task: Task, *, cloud: CloudClient, repo: TaskRepo
) -> CloudIndexEntry | None:
    """秒取回判定：``cloud_index`` 有该 hash 且云端条目仍在。

    调度器在任务还是 QUEUED 时先调用。命中时写好 ``cloud_file_id``、``cloud_size`` 并返回索引
    条目，调度器把 QUEUED 转到 LOCATING；不命中返回 None。不调用 ``add_magnet``、不读配额、
    不受 ``submitted_at`` 影响。

    云端是否还在：先 ``get_entry`` 区分目录和文件；目录再用 ``list_dir`` 确认（会话层核对
    cid，比 ``get_entry`` 可靠），抛 ``CloudNotFound`` 视为不命中；文件以 ``get_entry`` 为准。
    全局类异常原样上抛，此时不改任务。
    """
    index = await repo.get_cloud_index(task.hash)
    if index is None:
        return None
    entry = await cloud.get_entry(index.cloud_file_id)
    if entry is None:
        return None
    if entry.is_dir:
        try:
            await cloud.list_dir(index.cloud_file_id)
        except CloudNotFound:
            return None
    await repo.update(
        task.hash,
        TaskUpdate(cloud_file_id=index.cloud_file_id, cloud_size=index.total_size),
    )
    log.info("instant restore", extra={"hash": task.hash, "stage": "submit"})
    return index


async def _add_with_dir_retry(cloud: CloudClient, repo: TaskRepo, path: str, magnet: str) -> None:
    dir_id = await _category_dir_id(cloud, repo, path)
    try:
        await cloud.add_magnet(magnet, dir_id)
        return
    except CloudNotFound:
        log.warning("save dir invalid, rebuilding", extra={"stage": "submit", "path": path})
    dir_id = await _rebuild_dir(cloud, repo, path)
    try:
        await cloud.add_magnet(magnet, dir_id)
    except CloudNotFound as exc:
        raise SaveDirUnavailable(path, f"add_magnet still not found after rebuild: {exc}") from exc


async def submit(
    task: Task,
    *,
    cloud: CloudClient,
    repo: TaskRepo,
    clock: Clock,
    root_dir: str,
    quota_min: int,
    poll_seconds: float = 60.0,
) -> SubmitResult:
    """提交一个任务（调度器已把它转到 SUBMITTING）。异常见模块说明；异常下任务字段都不被改动。"""
    fresh = await repo.get_task(task.hash)
    if fresh is None:
        raise TaskNotFound(task.hash)
    task = fresh

    # 幂等：已提交过就不再调用 115
    if task.submitted_at is not None:
        return SubmitResult(SubmitKind.ALREADY_SUBMITTED, task)

    cached = decode_quota(await repo.kv_get(KV_QUOTA))
    if cached is None or (clock.now() - cached[1]).total_seconds() > poll_seconds:
        cached = await _read_quota(cloud, repo, clock)
        refreshed = True
    else:
        refreshed = False
    quota, read_at = cached
    if quota.remaining < quota_min and not refreshed:
        quota, read_at = await _read_quota(cloud, repo, clock)
    if quota.remaining < quota_min:
        raise QuotaLow(quota.remaining, quota_min)

    magnet = task.magnet or build_magnet(task.hash, task.name)
    await _add_with_dir_retry(cloud, repo, category_path(root_dir, task.category), magnet)

    updated = await repo.update(task.hash, TaskUpdate(submitted_at=clock.now()))
    # 本地减一，读取时间不变，过期后会重新读
    await store_quota(repo, Quota(max(quota.remaining - 1, 0), quota.total), read_at)
    log.info("submitted", extra={"hash": task.hash, "stage": "submit"})
    return SubmitResult(SubmitKind.SUBMITTED, updated)
