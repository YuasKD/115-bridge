"""启动对账（T14）：在 ``Orchestrator.run()`` 之前调用一次。

调度器的 ``_landed``、退避、``_fallback_next``、``_space_waiting`` 都只在内存里，重启即丢，
所以对账只用数据库、本地磁盘和一次 115 观测，不依赖这些内存状态。

规则（design.md「启动对账」）：

1. 离线中的任务（``track.TRACKED_STATES`` 且已有 ``submitted_at``）：复用 ``track.track`` 做一次
   观测，进度字段（cloud_percent、cloud_size、cloud_file_id）由 track 写库。**只观测，不推进
   状态**：观测结果（完成、失败、超时等）留给调度器第一次 tick 的跟踪阶段处理，这样不必在
   这里重复一套转换逻辑。遇到 115 的任何 ``CloudError``（含 ``GLOBAL_ERRORS``）不抛出，
   跳过这一步并在报告里标记，由调度器之后按全局暂停正常处理。
2. ``fetching`` 的任务：只把 ``tasks.local_bytes`` 与 ``task_files.local_bytes`` 校正为与
   ``.incomplete`` 里的文件和 ``.state`` 进度记录一致。判定规则与 ``fetch.Fetcher._prepare`` 相同
   （记录有效且数据在盘上则取记录的已完成字节，否则无记录且文件完整、库里也记着完整则取全部，
   其余一律 0）。只读磁盘，不删不改文件；真正的续传仍由 fetch 完成。
3. ``<paths.incomplete>`` 下名字为 40 位小写 hex 的目录，数据库里没有该 hash 的任务（任何状态，
   含 failed、removed、client_deleted）且目录 mtime 距今不少于 24 小时，才清理，并同时清
   ``<incomplete>/.state/<hash>``。其他条目（``.state`` 等）一律不碰。
   另外扫描 ``<incomplete>/.state/`` 下一层 40 位小写 hex 的真实目录：数据库没有该 hash、
   ``<incomplete>/<hash>`` 也不存在、mtime 同样不少于 24 小时，就单独删除（结果记入
   ``state_orphans_*``，与 ``orphans_*`` 分开，便于区分）。
4. ``completed`` 但本地内容（``layout.content_path``）已不在：记入 ``MissingFiles``，
   qBit 层据此报 ``missingFiles``，并推送一条 ``missing-files`` 告警。

补充：``verifying`` 且 ``.incomplete/<hash>`` 已不在（或只剩空目录：单文件落盘崩在改名与删目录
之间）、``content_path`` 已存在，说明落盘改名已完成而写 completed 之前崩溃，直接
VERIFYING -> COMPLETED（``expected=VERIFYING``）。

「FAILED 且 fallback=NONE」由调度器 ``_run_fallbacks`` 每轮补成 PENDING，对账不处理。

不变量：不调用任何删除云端内容的接口（2）；不计重试（3）；所有转换都带 ``expected``。
"""

from __future__ import annotations

import asyncio
import hashlib
import logging
import os
import re
import shutil
from collections.abc import Callable, Mapping
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path

from bridge import layout
from bridge.clock import Clock
from bridge.cloud.protocol import CloudClient, CloudError
from bridge.config import Settings
from bridge.db.repo import (
    InvalidTransition,
    StateMismatch,
    Task,
    TaskFile,
    TaskNotFound,
    TaskRepo,
    TaskState,
    TaskUpdate,
)
from bridge.log import log_event
from bridge.missing_protocol import completion_id
from bridge.notify.telegram import Notifier
from bridge.pipeline.fetch import (
    STATE_DIRNAME,
    UnsafePath,
    _load_segments,
    _stat_size,
    safe_target,
)
from bridge.pipeline.track import TrackState, track

logger = logging.getLogger(__name__)

ORPHAN_MIN_AGE_SECONDS = 24 * 3600.0
MISSING_FILES_ALERT_KIND = "missing-files"
ALERT_LIST_LIMIT = 10

_HASH_RE = re.compile(r"[0-9a-f]{40}")


class MissingFiles:
    """``missing_protocol.MissingFilesView`` 的实现（只读快照）。

    记录启动时 completed 且缺文件的任务，及其那次完成的事件 id（``completion_id``）。只有当前
    任务仍是同一次完成才报缺失；任务经 removed、requeue、重新完成后事件 id 变了，标记失效。
    文件事后被恢复不会自动清除，保持启动对账语义，下次重启重新计算。
    """

    def __init__(self, marks: Mapping[str, int | None] | None = None) -> None:
        self._marks: dict[str, int | None] = dict(marks or {})

    def might_be_missing(self, info_hash: str) -> bool:
        return info_hash.lower() in self._marks

    def is_missing(self, info_hash: str, completion_id: int | None) -> bool:
        key = info_hash.lower()
        return key in self._marks and self._marks[key] == completion_id

    @property
    def hashes(self) -> frozenset[str]:
        return frozenset(self._marks)


@dataclass(slots=True)
class ReconcileReport:
    # 规则 1：hash -> 观测结果（ObservationKind 的值）；跳过时为空，原因在 offline_skipped
    offline_observed: dict[str, str] = field(default_factory=dict)
    offline_skipped: str | None = None  # 遇到 115 错误时的异常类名与消息
    # 规则 2：hash -> (校正前, 校正后) 的 tasks.local_bytes，仅列出有变化的任务
    fetch_adjusted: dict[str, tuple[int, int]] = field(default_factory=dict)
    # 补充规则：落盘已完成、直接转 completed 的任务
    landed_completed: list[str] = field(default_factory=list)
    # verifying 但 .incomplete 与 content_path 都不在（不处理，留给调度器）
    verifying_lost: list[str] = field(default_factory=list)
    # 规则 3
    orphans_removed: list[str] = field(default_factory=list)
    orphans_young: list[str] = field(default_factory=list)  # 孤儿但不满 24 小时，保留
    orphan_errors: dict[str, str] = field(default_factory=dict)
    # 规则 3 补充：只剩 .state/<hash>（.incomplete/<hash> 已不在）的残留，单独记录
    state_orphans_removed: list[str] = field(default_factory=list)
    state_orphans_young: list[str] = field(default_factory=list)
    state_orphan_errors: dict[str, str] = field(default_factory=dict)
    # 规则 4
    missing_files: MissingFiles = field(default_factory=MissingFiles)
    missing_alerted: bool = False


# ---------------------------------------------------------------- 纯本地的辅助


def _default_mtime(path: Path) -> float:
    return path.stat().st_mtime


def _file_done_bytes(incomplete: Path, info_hash: str, tf: TaskFile) -> int:
    """与 ``Fetcher._prepare`` 同样的判定，但只读：返回续传时将从哪里开始算已完成字节。"""
    if tf.verified:
        return tf.size
    try:
        target = safe_target(incomplete, info_hash, tf.rel_path)
    except UnsafePath:
        return tf.local_bytes  # 不安全的路径 fetch 会拒绝，这里不动
    state_path = (
        incomplete
        / STATE_DIRNAME
        / info_hash
        / (hashlib.sha1(tf.rel_path.encode()).hexdigest() + ".json")
    )
    has_record = state_path.exists()
    segs = _load_segments(state_path, tf.size) if has_record else None
    if segs is not None:
        on_disk = _stat_size(target)
        extent = max((s.start + s.done for s in segs if s.done > 0), default=0)
        if on_disk < extent or on_disk > tf.size:
            return 0
        return sum(s.done for s in segs)
    if (
        not has_record
        and tf.size > 0
        and tf.local_bytes == tf.size
        and _stat_size(target) == tf.size
    ):
        return tf.size
    return 0


def _compute_fetch_progress(
    incomplete: Path, info_hash: str, files: list[TaskFile]
) -> dict[str, int]:
    return {tf.rel_path: _file_done_bytes(incomplete, info_hash, tf) for tf in files}


def _orphan_target(incomplete: Path, name: str) -> Path | None:
    """返回可安全删除的 ``<incomplete>/<name>``，不安全时返回 None（路径检查参照 LocalCleaner）。"""
    if not _HASH_RE.fullmatch(name):
        return None
    base = Path(os.path.abspath(incomplete))
    target = Path(os.path.abspath(incomplete / name))
    if target.parent != base or target == base:
        return None
    if target.is_symlink() or not target.is_dir():
        return None
    # incomplete 自身若经符号链接指到别处，真实父目录必须一致
    if Path(os.path.realpath(target.parent)) != Path(os.path.realpath(base)):
        return None
    return target


def _remove_orphan(incomplete: Path, name: str) -> None:
    target = _orphan_target(incomplete, name)
    if target is None:
        raise OSError(f"refusing to delete orphan {name!r}")
    shutil.rmtree(target)
    state_dir = Path(os.path.abspath(incomplete / STATE_DIRNAME / name))
    state_root = Path(os.path.abspath(incomplete / STATE_DIRNAME))
    if state_dir.parent == state_root and not state_dir.is_symlink():
        shutil.rmtree(state_dir, ignore_errors=True)


def _state_orphan_target(incomplete: Path, name: str) -> Path | None:
    """返回可安全删除的 ``<incomplete>/.state/<name>``，不安全时返回 None。"""
    if not _HASH_RE.fullmatch(name):
        return None
    base = Path(os.path.abspath(incomplete / STATE_DIRNAME))
    target = Path(os.path.abspath(incomplete / STATE_DIRNAME / name))
    if target.parent != base or target == base:
        return None
    if target.is_symlink() or not target.is_dir():
        return None
    if Path(os.path.realpath(target.parent)) != Path(os.path.realpath(base)):
        return None
    return target


def _remove_state_orphan(incomplete: Path, name: str) -> None:
    target = _state_orphan_target(incomplete, name)
    if target is None:
        raise OSError(f"refusing to delete state orphan {name!r}")
    shutil.rmtree(target)


def _scan_state_candidates(incomplete: Path) -> list[str]:
    """``<incomplete>/.state`` 下名字为 40 位小写 hex 的真实目录（不含符号链接）。"""
    try:
        with os.scandir(incomplete / STATE_DIRNAME) as it:
            return sorted(
                e.name for e in it if _HASH_RE.fullmatch(e.name) and e.is_dir(follow_symlinks=False)
            )
    except (FileNotFoundError, NotADirectoryError):
        return []


def _scan_candidates(incomplete: Path) -> list[str]:
    """``<incomplete>`` 下名字为 40 位小写 hex 的真实目录（不含符号链接）。"""
    try:
        with os.scandir(incomplete) as it:
            return sorted(
                e.name for e in it if _HASH_RE.fullmatch(e.name) and e.is_dir(follow_symlinks=False)
            )
    except FileNotFoundError:
        return []


def _content_exists(root: str, task: Task) -> bool:
    return os.path.lexists(layout.content_path(root, task, []))


def _landing_done(incomplete_dir: Path) -> bool:
    """``.incomplete/<hash>`` 已不在，或只剩空目录就算落盘完成。

    单文件种子落盘是先改名文件、再删空目录，崩溃在两步之间会留下空目录。空目录顺手删掉
    （删除失败不影响判断）；其中仍有任何条目就不是落盘完成。
    """
    if not os.path.lexists(incomplete_dir):
        return True
    if incomplete_dir.is_symlink() or not incomplete_dir.is_dir():
        return False
    try:
        os.rmdir(incomplete_dir)  # 只有空目录才会成功
    except OSError:
        return False
    return True


# ---------------------------------------------------------------- 对账


async def reconcile(
    *,
    repo: TaskRepo,
    cloud: CloudClient,
    clock: Clock,
    settings: Settings,
    notifier: Notifier | None = None,
    mtime_of: Callable[[Path], float] = _default_mtime,
    orphan_min_age_seconds: float = ORPHAN_MIN_AGE_SECONDS,
) -> ReconcileReport:
    report = ReconcileReport()
    incomplete = Path(settings.paths.incomplete)
    root = settings.paths.root

    await _rule_offline(repo, cloud, clock, settings, report)
    await _rule_fetching(repo, incomplete, report)
    await _rule_verifying_landed(repo, clock, incomplete, root, report)
    await _rule_orphans(repo, clock, incomplete, mtime_of, orphan_min_age_seconds, report)
    await _rule_state_orphans(repo, clock, incomplete, mtime_of, orphan_min_age_seconds, report)
    await _rule_missing(repo, root, notifier, report)
    log_event(
        logger,
        logging.INFO,
        "startup reconcile done",
        stage="reconcile",
        offline=len(report.offline_observed),
        offline_skipped=report.offline_skipped,
        fetch_adjusted=len(report.fetch_adjusted),
        landed=len(report.landed_completed),
        orphans_removed=len(report.orphans_removed),
        state_orphans_removed=len(report.state_orphans_removed),
        missing=len(report.missing_files.hashes),
    )
    return report


async def _rule_offline(
    repo: TaskRepo, cloud: CloudClient, clock: Clock, settings: Settings, report: ReconcileReport
) -> None:
    try:
        observations = await track(
            cloud=cloud,
            repo=repo,
            clock=clock,
            timeout_hours=float(settings.cloud.timeout_hours),
            state=TrackState(),
        )
    except CloudError as exc:  # 含 GLOBAL_ERRORS：不让启动失败，调度器之后正常处理
        report.offline_skipped = f"{type(exc).__name__}: {exc}"
        log_event(
            logger,
            logging.WARNING,
            "reconcile skipped offline tracking",
            stage="reconcile",
            error=repr(exc),
        )
        return
    report.offline_observed = {h: o.kind.value for h, o in observations.items()}


async def _rule_fetching(repo: TaskRepo, incomplete: Path, report: ReconcileReport) -> None:
    for task in await repo.list_tasks(states={TaskState.FETCHING}, include_client_deleted=True):
        files = await repo.list_task_files(task.hash)
        if not files:
            continue
        progress = await asyncio.to_thread(_compute_fetch_progress, incomplete, task.hash, files)
        for tf in files:
            new = progress[tf.rel_path]
            if new != tf.local_bytes:
                await repo.update_task_file(task.hash, tf.rel_path, local_bytes=new)
        total = sum(progress.values())
        if total != task.local_bytes:
            await repo.update(task.hash, TaskUpdate(local_bytes=total))
            report.fetch_adjusted[task.hash] = (task.local_bytes, total)


async def _rule_verifying_landed(
    repo: TaskRepo, clock: Clock, incomplete: Path, root: str, report: ReconcileReport
) -> None:
    for task in await repo.list_tasks(states={TaskState.VERIFYING}, include_client_deleted=True):
        if not await asyncio.to_thread(_landing_done, incomplete / task.hash):
            continue  # 还在 .incomplete：调度器会重新校验
        if not await asyncio.to_thread(_content_exists, root, task):
            report.verifying_lost.append(task.hash)
            log_event(
                logger,
                logging.WARNING,
                "verifying task has neither incomplete dir nor content",
                hash=task.hash,
                stage="reconcile",
            )
            continue
        try:
            await repo.transition(
                task.hash,
                TaskState.COMPLETED,
                "reconcile: already landed before restart",
                clock.now(),
                expected=TaskState.VERIFYING,
            )
        except (StateMismatch, TaskNotFound, InvalidTransition) as exc:
            log_event(
                logger,
                logging.INFO,
                "landed transition abandoned",
                hash=task.hash,
                stage="reconcile",
                error=repr(exc),
            )
            continue
        report.landed_completed.append(task.hash)


async def _rule_orphans(
    repo: TaskRepo,
    clock: Clock,
    incomplete: Path,
    mtime_of: Callable[[Path], float],
    min_age: float,
    report: ReconcileReport,
) -> None:
    now: datetime = clock.now()
    for name in await asyncio.to_thread(_scan_candidates, incomplete):
        if await repo.get_task(name) is not None:
            continue  # 数据库里有（任何状态）就不算孤儿
        try:
            mtime = await asyncio.to_thread(mtime_of, incomplete / name)
        except OSError as exc:
            report.orphan_errors[name] = repr(exc)
            continue
        if now.timestamp() - mtime < min_age:
            report.orphans_young.append(name)
            continue
        try:
            await asyncio.to_thread(_remove_orphan, incomplete, name)
        except OSError as exc:
            report.orphan_errors[name] = repr(exc)
            log_event(
                logger,
                logging.ERROR,
                "orphan cleanup failed",
                hash=name,
                stage="reconcile",
                error=repr(exc),
            )
            continue
        report.orphans_removed.append(name)
        log_event(
            logger, logging.INFO, "orphan incomplete dir removed", hash=name, stage="reconcile"
        )


async def _rule_state_orphans(
    repo: TaskRepo,
    clock: Clock,
    incomplete: Path,
    mtime_of: Callable[[Path], float],
    min_age: float,
    report: ReconcileReport,
) -> None:
    now: datetime = clock.now()
    for name in await asyncio.to_thread(_scan_state_candidates, incomplete):
        if await repo.get_task(name) is not None:
            continue
        if await asyncio.to_thread(os.path.lexists, incomplete / name):
            continue  # <incomplete>/<hash> 仍在：由规则 3 主体管
        state_dir = incomplete / STATE_DIRNAME / name
        try:
            mtime = await asyncio.to_thread(mtime_of, state_dir)
        except OSError as exc:
            report.state_orphan_errors[name] = repr(exc)
            continue
        if now.timestamp() - mtime < min_age:
            report.state_orphans_young.append(name)
            continue
        try:
            await asyncio.to_thread(_remove_state_orphan, incomplete, name)
        except OSError as exc:
            report.state_orphan_errors[name] = repr(exc)
            log_event(
                logger,
                logging.ERROR,
                "state orphan cleanup failed",
                hash=name,
                stage="reconcile",
                error=repr(exc),
            )
            continue
        report.state_orphans_removed.append(name)
        log_event(logger, logging.INFO, "orphan state dir removed", hash=name, stage="reconcile")


async def _rule_missing(
    repo: TaskRepo, root: str, notifier: Notifier | None, report: ReconcileReport
) -> None:
    missing: list[Task] = []
    for task in await repo.list_tasks(states={TaskState.COMPLETED}):
        if not await asyncio.to_thread(_content_exists, root, task):
            missing.append(task)
    report.missing_files = MissingFiles(
        {t.hash: await completion_id(repo, t.hash) for t in missing}
    )
    if not missing:
        return
    for t in missing:
        log_event(
            logger,
            logging.WARNING,
            "completed task has no local files",
            hash=t.hash,
            stage="reconcile",
        )
    if notifier is None:
        return
    shown = ", ".join(f"{t.name} ({t.hash[:8]})" for t in missing[:ALERT_LIST_LIMIT])
    more = f" 等共 {len(missing)} 个" if len(missing) > ALERT_LIST_LIMIT else ""
    try:
        report.missing_alerted = await notifier.alert(
            MISSING_FILES_ALERT_KIND,
            f"115 Bridge: 以下已完成任务的本地文件已不在（qBit 将报 missingFiles）：{shown}{more}",
        )
    except Exception as exc:  # noqa: BLE001
        log_event(
            logger, logging.ERROR, "missing-files alert failed", stage="reconcile", error=repr(exc)
        )


__all__ = [
    "MISSING_FILES_ALERT_KIND",
    "ORPHAN_MIN_AGE_SECONDS",
    "MissingFiles",
    "ReconcileReport",
    "reconcile",
]
