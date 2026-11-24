"""离线跟踪（T09）：一次调用处理所有云端阶段的任务，共用查询结果。

- 只处理状态为 SUBMITTING、CLOUD_METADATA、CLOUD_DOWNLOADING 且已有 ``submitted_at`` 的任务。
- 先翻 ``stat=12`` 全部页；没找到的 hash 再查 ``stat=11``，仍没找到的再查 ``stat=9``；
  目标全部找到就停止翻页。hash 比较一律小写。
- 11 和 9 每个最多翻 ``max_pages`` 页（新完成、新失败的任务在前几页）。第 1 页每次都查；
  超过第 1 页的深查对「连续 NOT_FOUND」的 hash 限频为每 10 分钟一次（``TrackState`` 用
  Clock 记录）。限频不影响超时规则：仍找不到的任务在 ``timeout_hours`` 到期后照常判 TIMEOUT。
- 完成判定用 ``OfflineTask.is_done``（不变量 7）。percent 到 100 但不满足 is_done 的仍是下载中。
  99% 停滞不判失败，只按总时长判超时。
- 单文件种子：完成时 ``file_id == wp_path_id``（文件直接在保存目录里，名字是 ``del_path``）。
  此时在保存目录里找「名字等于 del_path 且大小等于任务大小」的文件，把它的 id 作为
  ``cloud_file_id``；每次观测到 DONE 都重新解析，不沿用库里已有的 ``cloud_file_id``
  （可能是上一轮遗留的过期 id）。名字和大小都相同的文件不止一个时按 id 字符串排序取第一个，
  并记 WARNING（取回后仍按 115 的 sha1 逐文件校验）；找不到才返回 ``ROOT_UNRESOLVED``
  （调度器据此标 failed 并回退）。绝不把 ``wp_path_id`` 写成 ``cloud_file_id``。
- 本模块不做状态转换、不发告警。只写进度字段（cloud_percent、cloud_size、cloud_file_id），
  把本次读到的配额写进 kv（带读取时间，``KV_QUOTA``，供 submit 使用），返回每个 hash 的观测结果。
- 全局类异常原样上抛，此时不改任何任务、不写配额。
"""

from __future__ import annotations

import logging
import re
from dataclasses import dataclass, field, replace
from datetime import timedelta
from enum import StrEnum

from bridge.clock import Clock
from bridge.cloud.protocol import CloudClient, CloudNotFound, OfflineStat, OfflineTask, Quota
from bridge.db.repo import Task, TaskRepo, TaskState, TaskUpdate
from bridge.kv_keys import store_quota

log = logging.getLogger(__name__)

TRACKED_STATES: frozenset[TaskState] = frozenset(
    {TaskState.SUBMITTING, TaskState.CLOUD_METADATA, TaskState.CLOUD_DOWNLOADING}
)

_STAT_ORDER = (OfflineStat.RUNNING, OfflineStat.DONE, OfflineStat.FAILED)


class ObservationKind(StrEnum):
    METADATA = "metadata"  # 元数据阶段：尚无 size 或 percent 为 0
    DOWNLOADING = "downloading"  # 下载中（含 percent 100 但未 is_done 的过渡态、99% 停滞）
    DONE = "done"  # status == 2 且 file_id 非空
    FAILED = "failed"  # status 为 -1、3、4、5 之一
    TIMEOUT = "timeout"  # 从 submitted_at 起超过 timeout_hours 仍未完成
    NOT_FOUND = "not_found"  # 查过的列表里都没有，且未超时；视为仍在进行
    ROOT_UNRESOLVED = "root_unresolved"  # 已完成但解析不出任务根（单文件种子）；调度器标 failed


@dataclass(frozen=True, slots=True)
class Observation:
    hash: str
    kind: ObservationKind
    percent: float = 0.0
    size: int | None = None
    file_id: str | None = None  # 仅 DONE
    status: int | None = None  # 115 的 status；列表里没找到时为 None
    reason: str | None = None  # 仅 ROOT_UNRESOLVED


def _classify(
    task: Task, found: OfflineTask | None, *, elapsed: timedelta, timeout: timedelta
) -> Observation:
    if found is None:
        kind = ObservationKind.TIMEOUT if elapsed >= timeout else ObservationKind.NOT_FOUND
        return Observation(task.hash, kind, percent=task.cloud_percent, size=task.cloud_size)
    size = found.size if found.size > 0 else None
    if found.is_done:
        return Observation(
            task.hash,
            ObservationKind.DONE,
            percent=found.percent_done,
            size=size,
            file_id=found.file_id,
            status=found.status,
        )
    if found.is_failed:
        return Observation(
            task.hash,
            ObservationKind.FAILED,
            percent=found.percent_done,
            size=size,
            status=found.status,
        )
    if elapsed >= timeout:
        kind = ObservationKind.TIMEOUT
    elif size is None or found.percent_done <= 0:
        kind = ObservationKind.METADATA
    else:
        kind = ObservationKind.DOWNLOADING
    return Observation(task.hash, kind, percent=found.percent_done, size=size, status=found.status)


DEEP_INTERVAL_SECONDS = 600.0

_CHARREF = re.compile(r"\[\x02([0-9]+)\]")


def unescape_115(name: str) -> str:
    """115 字符引用反转义（形如方括号包 0x02 与十进制码点）。

    session.py 对 list_dir 的名字已做过；这里对两边再做一次以保证比较一致
    （本层不能 import p115client）。
    """
    return _CHARREF.sub(lambda m: chr(int(m.group(1))), name)


@dataclass(slots=True)
class TrackState:
    """深查限频的内存状态，由调度器持有一个实例并每次传入。重启后丢失只会多做一次深查。"""

    last_deep: dict[str, float] = field(default_factory=dict)


async def _lookup(
    cloud: CloudClient,
    wanted: set[str],
    *,
    max_pages: int,
    clock: Clock,
    state: TrackState | None,
) -> tuple[dict[str, OfflineTask], Quota | None]:
    pending = set(wanted)
    found: dict[str, OfflineTask] = {}
    quota: Quota | None = None
    now = clock.monotonic()

    def eligible(h: str) -> bool:
        if state is None:
            return True
        last = state.last_deep.get(h)
        return last is None or now - last >= DEEP_INTERVAL_SECONDS

    deep_done: set[str] = set()
    for stat in _STAT_ORDER:
        if not pending:
            break
        limit = None if stat is OfflineStat.RUNNING else max_pages
        page_no = 1
        while pending:
            page = await cloud.list_offline(stat, page_no)
            quota = page.quota
            for t in page.tasks:
                h = t.hash.lower()
                if h in pending:
                    pending.discard(h)
                    found[h] = t
            if page_no >= page.page_count or (limit is not None and page_no >= limit):
                break
            if limit is not None:
                if not any(eligible(h) for h in pending):
                    break  # 剩下的都在限频内，不深查
                deep_done |= {h for h in pending if eligible(h)}
            page_no += 1
    if state is not None:
        for h in found:
            state.last_deep.pop(h, None)
        for h in deep_done & pending:
            state.last_deep[h] = now
    return found, quota


async def _resolve_single_file(cloud: CloudClient, task: Task, found: OfflineTask) -> Observation:
    """file_id 等于保存目录 id：在保存目录里按 del_path 和大小找唯一文件。"""
    size = found.size if found.size > 0 else task.cloud_size
    base = Observation(
        task.hash,
        ObservationKind.ROOT_UNRESOLVED,
        percent=found.percent_done,
        size=size,
        status=found.status,
    )
    assert found.wp_path_id is not None
    if not size:
        return replace(base, reason="single-file task has no size to match")
    try:
        entries = await cloud.list_dir(found.wp_path_id)
    except CloudNotFound:
        return replace(base, reason="save directory not found")
    want = unescape_115(found.del_path)
    matches = [
        e for e in entries if not e.is_dir and e.size == size and unescape_115(e.name) == want
    ]
    if not matches:
        return replace(base, reason="single-file root: no file matches name and size in save dir")
    matches.sort(key=lambda e: str(e.id))
    if len(matches) > 1:
        log.warning(
            "single-file root ambiguous, picking first by id",
            extra={
                "hash": task.hash,
                "stage": "track",
                "candidates": len(matches),
                "picked": matches[0].id,
            },
        )
    return Observation(
        task.hash,
        ObservationKind.DONE,
        percent=found.percent_done,
        size=size,
        file_id=matches[0].id,
        status=found.status,
    )


async def track(
    *,
    cloud: CloudClient,
    repo: TaskRepo,
    clock: Clock,
    timeout_hours: float,
    max_pages: int = 5,
    state: TrackState | None = None,
) -> dict[str, Observation]:
    """返回 ``{hash: Observation}``，键是本次被跟踪的任务的 hash。

    ``state`` 为 None 时不做深查限频（每次都按 ``max_pages`` 查）。
    """
    tasks = [t for t in await repo.list_tasks(states=TRACKED_STATES) if t.submitted_at is not None]
    if not tasks:
        return {}

    found, quota = await _lookup(
        cloud, {t.hash for t in tasks}, max_pages=max_pages, clock=clock, state=state
    )
    now = clock.now()
    timeout = timedelta(hours=timeout_hours)

    out: dict[str, Observation] = {}
    for t in tasks:
        assert t.submitted_at is not None
        f = found.get(t.hash)
        obs = _classify(t, f, elapsed=now - t.submitted_at, timeout=timeout)
        if (
            f is not None
            and obs.kind is ObservationKind.DONE
            and f.wp_path_id is not None
            and f.file_id == f.wp_path_id
        ):
            obs = await _resolve_single_file(cloud, t, f)
        out[t.hash] = obs

    # 解析阶段可能抛全局异常，所有写库放在最后
    for t in tasks:
        obs = out[t.hash]
        if t.hash in found:
            change = TaskUpdate(
                cloud_percent=obs.percent if obs.percent != t.cloud_percent else None,
                cloud_size=obs.size if obs.size is not None and obs.size != t.cloud_size else None,
                cloud_file_id=obs.file_id if obs.file_id != t.cloud_file_id else None,
            )
            if (
                change.cloud_percent is not None
                or change.cloud_size is not None
                or change.cloud_file_id is not None
            ):
                await repo.update(t.hash, change)
        log.info("tracked", extra={"hash": t.hash, "stage": "track", "kind": obs.kind.value})

    if quota is not None:
        await store_quota(repo, quota, clock.now())
    return out
