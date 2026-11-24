"""调度器（T13）：把 submit、track、locate、fetch、verify 串成状态机，处理失败与 Arr 回退。

入口
- ``Orchestrator.tick()``：推进一轮。顺序：QUEUED（秒取回或转 SUBMITTING）、SUBMITTING
  （提交）、云端跟踪、CLOUD_DONE、定位、启动取回或校验的后台协程、Arr 回退。
  测试只用 tick 加 FakeClock。
- ``Orchestrator.run()``：长期运行，按 ``cloud.poll_seconds`` 循环 tick，用 Clock 等待；
  后台协程结束时会唤醒下一轮，不必等满一个间隔。
- ``Orchestrator.cleaner``：实现 ``api.qbit.LocalCleaner``（见 ``PipelineCleaner``）。

状态推进：严格按 ``ALLOWED_TRANSITIONS`` 逐级推进（云端阶段不跳级）；每次转换都带
``expected=`` 当前状态，遇到 ``StateMismatch`` 说明别处（例如 torrents/delete）抢先改了
状态，放弃这一步，下一轮重新读任务再决定。调用任何模块前都重新读任务。

全局暂停（不变量 4）：``GLOBAL_ERRORS``、``SaveDirUnavailable``、``QuotaLow``、
``QuotaExhausted``、fetch 抛出的 ``NetworkError``、本地空间等待，只暂停对应的动作
（提交、跟踪、定位、取回），不改任务状态、不计重试、不回退。

告警分工：会话层（session.py）已经对 115 的全局状态告警（kind 为 auth、verify、risk、
network、lease、api-param 等），所以调度器遇到 ``AuthExpired``、``VerifyRequired``、
``RiskControl``、``ServerError`` 以及 115 API 侧的 ``NetworkError`` 时只记日志、不告警。
调度器自己告警的只有会话层不知道的事：配额低或用尽（quota）、保存目录不可用（save-dir）、
本地空间不足（local-space）、CDN 连接持续失败（fetch-network；fetch 的 ``NetworkError``
消息以 ``cdn connection failed`` 开头才算 CDN 侧）、连续 CDN 403（cdn-403）、取回时间过长
（fetch-slow）、``TargetExists`` 与 verify 的原始 OSError、``MissingCloudRoot``、基础设施
故障（``infra-<出错位置>``）、Arr 回退失败（Arr 不可达 ``arr-unreachable-<arr名>``、Key 错误
``arr-auth-<arr名>``、其他 ``arr-error-<arr名>``；队列一直找不到的最终告警
``arr-notfound-<hash>``）、
category 没有对应 Arr（arr-missing）。同一 kind 的去重由 Notifier 负责（30 分钟）。

单个任务的意外异常只影响它自己：按任务记退避（键如 ``("locate", hash)``），告警后继续处理
同阶段的其他任务，不会中止整个阶段。

停住原因：调度器为每个任务记录当前的停住原因（``stall_reason(hash)``，只读，不写进
``tasks.error``），任务状态变化或恢复推进时清除，供 Web 页面显示。

重试与退避：fetch、verify 模块内部已计数的不重复计数（调度器从不调用 ``incr_retry``）。
调度器自己需要稍后再试的动作用 ``Backoff``：按任务、按阶段，30 秒起翻倍、最长 1 小时，
用 Clock。

Arr 回退（design.md 5 步）：先把任务转为 failed（``expected=``），写 error 原因，
``set_fallback(PENDING)``，再按 category 找 Arr 调 ``find_queue_item`` 和
``remove_and_blocklist``，成功置 DONE；失败保持 PENDING，每 10 分钟重试并告警。
任务已 ``client_deleted``（Arr 已调过 torrents/delete）视为回退已完成。
队列里找不到对应项（``QueueItemNotFound``）：第一次不告警，从第一次找不到起持续
``Tuning.queue_not_found_seconds``（1 小时）仍找不到就停止重试，置 DONE 并推一条最终告警，
请到 Arr 手动处理；Arr 不可达、Key 错误一直每 10 分钟重试并告警。

``tasks.error`` 里的两种内容共存：failed 的失败原因，和 qBit 层写的
``CLEANUP_ERROR_PREFIX`` 开头的清理失败标记。写失败原因前先读，已有清理标记就不覆盖
（原因仍在 events 与日志里）；Arr 回退本身的失败不写进 ``tasks.error``（只进日志与告警），
所以不会与清理标记冲突，也不会被当成清理错误。

永不删除 115 上的内容（不变量 2）：本模块不调用任何云端删除，``LocalCleaner`` 只删本地。
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
import os
import re
import shutil
from collections.abc import Awaitable, Callable, Hashable, Mapping
from dataclasses import dataclass
from enum import StrEnum
from pathlib import Path
from typing import Protocol

from bridge import layout
from bridge.arr.protocol import ArrAuthError, ArrClient, ArrUnreachable, QueueItemNotFound
from bridge.clock import Clock
from bridge.cloud.protocol import (
    GLOBAL_ERRORS,
    CloudClient,
    CloudError,
    CloudNotFound,
    NetworkError,
    QuotaExhausted,
    SubmitRejected,
)
from bridge.config import Settings
from bridge.db.repo import (
    ACTIVE_STATES,
    EVENTS_PER_TASK,
    Fallback,
    InvalidTransition,
    StateMismatch,
    Task,
    TaskNotFound,
    TaskRepo,
    TaskState,
)
from bridge.log import log_event
from bridge.markers import CLEANUP_ERROR_PREFIX
from bridge.notify import Notifier
from bridge.pipeline.fetch import (
    FetchError,
    FetchFailed,
    FetchResult,
    FetchStatus,
)
from bridge.pipeline.locate import LocateError, MatchFailed, MissingCloudRoot, locate
from bridge.pipeline.submit import (
    QuotaLow,
    SaveDirUnavailable,
    SubmitError,
    check_instant,
    submit,
)
from bridge.pipeline.track import TRACKED_STATES, Observation, ObservationKind, TrackState, track
from bridge.pipeline.verify import (
    TargetExists,
    VerifyError,
    VerifyResult,
    VerifyStatus,
)

logger = logging.getLogger(__name__)

BACKOFF_BASE_SECONDS = 30.0
BACKOFF_MAX_SECONDS = 3600.0
FALLBACK_RETRY_SECONDS = 600.0
QUEUE_NOT_FOUND_GIVEUP_SECONDS = 3600.0
FETCH_ALERT_HOURS = 24.0
CDN_403_THRESHOLD = 3
CDN_NETWORK_ALERT_SECONDS = 30 * 60.0
CDN_CONNECT_FAILED_PREFIX = "cdn connection failed"
ERROR_MAX_LEN = 500
GIB = 1024**3

_HASH_RE = re.compile(r"[0-9a-f]{40}")

# 云端阶段的逐级路径；任务从其中某个状态推进到目标状态时，每一级都走一次转换
_CLOUD_ORDER: tuple[TaskState, ...] = (
    TaskState.SUBMITTING,
    TaskState.CLOUD_METADATA,
    TaskState.CLOUD_DOWNLOADING,
    TaskState.CLOUD_DONE,
    TaskState.LOCATING,
)


# ---------------------------------------------------------------- 依赖的窄接口


class FetcherLike(Protocol):
    """``pipeline.fetch.Fetcher`` 中调度器用到的部分（测试可替换）。"""

    async def fetch(self, info_hash: str) -> FetchResult: ...

    def forget(self, info_hash: str) -> None: ...

    def state_dir(self, info_hash: str) -> Path: ...


class VerifierLike(Protocol):
    async def verify(self, info_hash: str) -> VerifyResult: ...


@dataclass(frozen=True, slots=True)
class Tuning:
    """调度器自己的参数（设计文档配置表之外的实现细节，默认值见各常量）。"""

    backoff_base: float = BACKOFF_BASE_SECONDS
    backoff_max: float = BACKOFF_MAX_SECONDS
    fallback_retry_seconds: float = FALLBACK_RETRY_SECONDS
    fetch_alert_hours: float = FETCH_ALERT_HOURS
    cdn_403_threshold: int = CDN_403_THRESHOLD
    cdn_network_alert_seconds: float = CDN_NETWORK_ALERT_SECONDS
    queue_not_found_seconds: float = QUEUE_NOT_FOUND_GIVEUP_SECONDS


# ---------------------------------------------------------------- 退避


class Backoff:
    """按 key 的指数退避：第 n 次连续失败后等 ``base * 2**(n-1)`` 秒，最长 ``cap`` 秒。"""

    def __init__(self, clock: Clock, base: float = BACKOFF_BASE_SECONDS, cap: float = 3600.0):
        self._clock = clock
        self._base = base
        self._cap = cap
        self._failures: dict[Hashable, int] = {}
        self._next: dict[Hashable, float] = {}

    def ready(self, key: Hashable) -> bool:
        return self._clock.monotonic() >= self._next.get(key, 0.0)

    def fail(self, key: Hashable) -> float:
        """记一次失败，返回这次要等的秒数。"""
        n = self._failures.get(key, 0) + 1
        self._failures[key] = n
        delay = float(min(self._base * 2 ** (n - 1), self._cap))
        self._next[key] = self._clock.monotonic() + delay
        return delay

    def reset(self, key: Hashable) -> None:
        self._failures.pop(key, None)
        self._next.pop(key, None)

    def failures(self, key: Hashable) -> int:
        return self._failures.get(key, 0)


# ---------------------------------------------------------------- 后台协程


class JobKind(StrEnum):
    FETCH = "fetch"
    VERIFY = "verify"


@dataclass(slots=True)
class _Job:
    kind: JobKind
    task: asyncio.Task[None]


class UnsafeLocalPath(Exception):
    """本地清理的目标路径不在允许的范围内。绝不删除。"""


def _safe_component(value: str, what: str) -> None:
    if value in ("", ".", "..") or any(c in value for c in ("/", "\\", "\x00")):
        raise UnsafeLocalPath(f"unsafe {what}: {value!r}")


def _rmtree_if_exists(path: Path) -> None:
    """删除目录树；不存在视为成功；其他错误抛出（由 qBit 层转成 500）。"""
    try:
        shutil.rmtree(path)
    except FileNotFoundError:
        return


def _remove_content(target: Path) -> None:
    if not os.path.lexists(target):
        return
    if target.is_symlink() or not target.is_dir():
        target.unlink()
    else:
        shutil.rmtree(target)


class PipelineCleaner:
    """``api.qbit.LocalCleaner`` 的实现。只删本地，绝不碰云端。"""

    def __init__(self, orchestrator: Orchestrator) -> None:
        self._o = orchestrator

    async def cancel_and_cleanup_incomplete(self, info_hash: str) -> None:
        """取消该任务的取回或校验并等其结束，再清理 ``.incomplete/<hash>`` 与 ``.state/<hash>``。

        ``Fetcher.forget`` 在协程完全结束之后才调用。若校验协程已经 rename 到正式目录而
        任务又被删除（落盘与 torrents/delete 竞态），连同落盘的那份一起删除。
        """
        o = self._o
        if not _HASH_RE.fullmatch(info_hash):
            raise UnsafeLocalPath(f"bad hash {info_hash!r}")
        await o.cancel_job(info_hash)
        o._fetcher.forget(info_hash)
        await asyncio.to_thread(_rmtree_if_exists, o._incomplete_dir / info_hash)
        await asyncio.to_thread(_rmtree_if_exists, o._fetcher.state_dir(info_hash))
        if o._landed.pop(info_hash, None) is not None:
            task = await o._repo.get_task(info_hash)
            if task is not None:
                await asyncio.to_thread(_remove_content, self._content_target(task))

    async def remove_completed(self, task: Task) -> None:
        """删除 ``layout.content_path`` 对应的本地文件或目录，必须在 ``<root>/<category>`` 之下。"""
        await asyncio.to_thread(_remove_content, self._content_target(task))

    def _content_target(self, task: Task) -> Path:
        o = self._o
        _safe_component(task.category, "category")
        _safe_component(task.name, "task name")
        if o._categories and task.category not in o._categories:
            raise UnsafeLocalPath(f"unknown category {task.category!r}")
        save = Path(os.path.abspath(layout.save_path(str(o._root_dir), task.category)))
        target = Path(os.path.abspath(layout.content_path(str(o._root_dir), task, [])))
        incomplete = Path(os.path.abspath(o._incomplete_dir))
        if target.parent != save or target == incomplete or incomplete in target.parents:
            raise UnsafeLocalPath(f"refusing to delete {target}")
        # 分类目录若是符号链接（例如指向媒体库），realpath 与「真实根 / category」不一致：拒绝
        real_save = Path(os.path.realpath(o._root_dir)) / task.category
        if Path(os.path.realpath(target.parent)) != real_save:
            raise UnsafeLocalPath(f"refusing to delete {target}: save dir is not a plain directory")
        return target


# ---------------------------------------------------------------- 调度器


class Orchestrator:
    def __init__(
        self,
        *,
        repo: TaskRepo,
        cloud: CloudClient,
        arrs: Mapping[str, ArrClient],
        notifier: Notifier,
        clock: Clock,
        fetcher: FetcherLike,
        verifier: VerifierLike,
        settings: Settings,
        track_state: TrackState | None = None,
        tuning: Tuning | None = None,
    ) -> None:
        self._repo = repo
        self._cloud = cloud
        self._arrs = arrs  # category -> ArrClient
        self._notifier = notifier
        self._clock = clock
        self._fetcher = fetcher
        self._verifier = verifier
        self._settings = settings
        self._tune = tuning or Tuning()
        self._track_state = track_state or TrackState()
        self._root_dir = Path(settings.paths.root)
        self._incomplete_dir = Path(settings.paths.incomplete)
        self._categories = frozenset(settings.categories)
        self._backoff = Backoff(clock, self._tune.backoff_base, self._tune.backoff_max)
        self._jobs: dict[str, _Job] = {}
        self._landed: dict[str, str] = {}  # verify 已 rename、尚未写 completed 的落盘路径
        self._pauses: dict[str, str] = {}
        self._stalls: dict[str, tuple[TaskState, str]] = {}  # hash -> (记录时的状态, 原因)
        self._alerted: set[str] = set()  # 发过告警、等待恢复通知的 kind
        self._space_waiting: set[str] = set()
        self._fallback_next: dict[str, float] = {}
        self._queue_missing_since: dict[str, float] = {}  # Arr 队列第一次找不到的时刻
        self._cdn_403_files: set[tuple[str, str]] = set()  # 自上次成功以来 403 的文件
        self._cdn_403_alerted = False
        self._bg_alerts: set[asyncio.Task[None]] = set()
        self._cdn_fail_since: float | None = None
        self._tick_lock = asyncio.Lock()
        self._wake = asyncio.Event()
        self.cleaner = PipelineCleaner(self)

    # ------------------------------------------------------------ 对外

    @property
    def pauses(self) -> Mapping[str, str]:
        """当前暂停中的动作（提交、跟踪、定位、取回）及原因，没有暂停时为空。"""
        return dict(self._pauses)

    def stall_reason(self, info_hash: str) -> str | None:
        """该任务当前的停住原因（只读，供 Web 页面）；没有停住返回 None。"""
        entry = self._stalls.get(info_hash)
        return entry[1] if entry is not None else None

    def on_cdn_403(self, info_hash: str, rel_path: str) -> None:
        """``Fetcher`` 的 ``on_cdn_403`` 回调：某文件降到 1 连接后仍 403。

        按文件计数：连续 ``cdn_403_threshold`` 个不同文件 403（中间没有文件成功完成）告警一次。
        """
        self._cdn_403_files.add((info_hash, rel_path))
        if len(self._cdn_403_files) >= self._tune.cdn_403_threshold and not self._cdn_403_alerted:
            self._cdn_403_alerted = True
            self._background_alert(
                "cdn-403",
                f"115 Bridge: 连续 {self._tune.cdn_403_threshold} 个文件遇到 CDN 403，"
                "可能是风控或链接问题。",
            )

    def on_file_done(self, info_hash: str, rel_path: str) -> None:
        """``Fetcher`` 的 ``on_file_done`` 回调：有文件经 CDN 成功下完，说明 CDN 可用。"""
        self._cdn_403_files.clear()
        self._cdn_403_alerted = False
        self._cdn_fail_since = None

    def _background_alert(self, kind: str, text: str) -> None:
        task = asyncio.ensure_future(self._alert(kind, text))
        self._bg_alerts.add(task)
        task.add_done_callback(self._bg_alerts.discard)

    @property
    def active_jobs(self) -> Mapping[str, JobKind]:
        """正在运行的后台协程（hash 到取回或校验）。"""
        return {h: j.kind for h, j in self._jobs.items() if not j.task.done()}

    async def tick(self) -> None:
        async with self._tick_lock:
            await self._phase("reap", self._reap_jobs)
            await self._phase("prune", self._prune)
            await self._phase("queued", self._advance_queued)
            await self._phase("submit", self._advance_submitting)
            await self._phase("track", self._run_track)
            await self._phase("cloud_done", self._advance_cloud_done)
            await self._phase("locate", self._run_locate)
            await self._phase("jobs", self._spawn_jobs)
            await self._phase("fallback", self._run_fallbacks)

    async def run(self) -> None:
        """长期运行：按 ``cloud.poll_seconds`` 循环；后台协程结束时提前唤醒。"""
        try:
            while True:
                self._wake.clear()
                try:
                    await self.tick()
                except asyncio.CancelledError:
                    raise
                except Exception as exc:  # noqa: BLE001
                    log_event(logger, logging.ERROR, "tick crashed", error=repr(exc))
                    await self._alert(
                        "orchestrator", f"115 Bridge: 调度器异常（{type(exc).__name__}）。"
                    )
                await self._sleep_or_wake(float(self._settings.cloud.poll_seconds))
        finally:
            await self.shutdown()

    async def shutdown(self) -> None:
        for h in list(self._jobs):
            await self.cancel_job(h)
        # 后台告警协程也要在 http 客户端关闭之前收尾
        alerts = list(self._bg_alerts)
        for t in alerts:
            t.cancel()
        if alerts:
            await asyncio.gather(*alerts, return_exceptions=True)

    async def wait_jobs(self, real_seconds: float | None = None) -> bool:
        """等当前所有后台协程结束（真实时间的超时）；都结束返回 True。"""
        tasks = [j.task for j in self._jobs.values()]
        if not tasks:
            return True
        _, pending = await asyncio.wait(tasks, timeout=real_seconds)
        return not pending

    async def cancel_job(self, info_hash: str) -> None:
        """取消该 hash 的后台协程并等它完全结束。没有则立即返回。"""
        job = self._jobs.get(info_hash)
        if job is None or job.task.done():
            return
        job.task.cancel()
        await asyncio.wait({job.task})

    # ------------------------------------------------------------ 通用辅助

    async def _sleep_or_wake(self, seconds: float) -> None:
        sleeper = asyncio.ensure_future(self._clock.sleep(seconds))
        waker = asyncio.ensure_future(self._wake.wait())
        try:
            await asyncio.wait({sleeper, waker}, return_when=asyncio.FIRST_COMPLETED)
        finally:
            sleeper.cancel()
            waker.cancel()
            await asyncio.gather(sleeper, waker, return_exceptions=True)

    async def _phase(self, name: str, fn: Callable[[], Awaitable[None]]) -> None:
        """一个阶段里意外的异常（多半是仓储故障）不让整轮失败，也不改任何任务。"""
        try:
            await fn()
        except asyncio.CancelledError:
            raise
        except Exception as exc:  # noqa: BLE001
            await self._infra(name, None, exc)

    async def _infra(self, where: str, info_hash: str | None, exc: BaseException) -> None:
        """基础设施故障（仓储、磁盘之外的意外错误）：记日志、告警，不计任务失败、不改状态。"""
        log_event(
            logger,
            logging.ERROR,
            "infrastructure failure",
            hash=info_hash,
            stage=where,
            error=repr(exc),
        )
        await self._alert(
            f"infra-{where}",
            f"115 Bridge: 内部故障（{where}：{type(exc).__name__}），任务保持原状。",
        )

    def _stall(self, info_hash: str, state: TaskState, reason: str) -> None:
        self._stalls[info_hash] = (state, reason)

    async def _task_failure(
        self, stage: str, state: TaskState, info_hash: str, exc: BaseException
    ) -> None:
        """单个任务在某阶段的意外异常：按任务退避，记停住原因，告警。调用方随后 ``continue``。"""
        delay = self._backoff.fail((stage, info_hash))
        self._stall(info_hash, state, f"调度器内部问题（{stage}：{type(exc).__name__}）")
        log_event(
            logger,
            logging.ERROR,
            "task step crashed, backing off",
            hash=info_hash,
            stage=stage,
            error=repr(exc),
            retry_in=delay,
        )
        await self._infra(stage, info_hash, exc)

    async def _prune(self) -> None:
        """清理内存里已经过期的记录：停住原因、等待空间的任务、没有对应任务的暂停标记。"""
        for h, (state, _reason) in list(self._stalls.items()):
            task = await self._repo.get_task(h)
            if task is None or task.state is not state:
                self._stalls.pop(h, None)
        present = {
            t.state
            for t in await self._repo.list_tasks(
                states={
                    TaskState.QUEUED,
                    TaskState.SUBMITTING,
                    TaskState.LOCATING,
                    TaskState.FETCHING,
                    TaskState.VERIFYING,
                }
            )
        }
        if self._space_waiting:
            fetching = {t.hash for t in await self._repo.list_tasks(states={TaskState.FETCHING})}
            self._space_waiting &= fetching
            if not self._space_waiting and self._pauses.get("fetch") == "waiting for local space":
                self._clear_pause("fetch")
                await self._recover("local-space", "115 Bridge: 本地空间已恢复，继续取回。")
        for phase, st in (
            ("queued", TaskState.QUEUED),
            ("submit", TaskState.SUBMITTING),
            ("locate", TaskState.LOCATING),
            ("fetch", TaskState.FETCHING),
            ("verify", TaskState.VERIFYING),
        ):
            if st not in present:
                self._clear_pause(phase)

    async def _alert(self, kind: str, text: str, *, recover: bool = False) -> None:
        if recover:
            self._alerted.add(kind)
        try:
            await self._notifier.alert(kind, text)
        except Exception as exc:  # noqa: BLE001
            log_event(logger, logging.WARNING, "alert failed", error_type=type(exc).__name__)

    async def _recover(self, kind: str, text: str) -> None:
        if kind not in self._alerted:
            return
        self._alerted.discard(kind)
        try:
            await self._notifier.recovered(kind, text)
        except Exception as exc:  # noqa: BLE001
            log_event(logger, logging.WARNING, "notice failed", error_type=type(exc).__name__)

    def _global(self, phase: str, exc: BaseException) -> None:
        """115 全局错误：只暂停该动作。告警由会话层负责，这里不重复。"""
        self._pauses[phase] = type(exc).__name__
        log_event(
            logger,
            logging.WARNING,
            "paused by global error",
            stage=phase,
            error=type(exc).__name__,
            detail=str(exc)[:200],
        )

    def _clear_pause(self, phase: str) -> None:
        self._pauses.pop(phase, None)

    async def _move(
        self, info_hash: str, dst: TaskState, reason: str, expected: TaskState
    ) -> Task | None:
        """带 expected 的转换。别处抢先改了状态（或任务没了）返回 None，由调用方放弃这一步。"""
        try:
            moved = await self._repo.transition(
                info_hash, dst, reason, self._clock.now(), expected=expected
            )
            self._stalls.pop(info_hash, None)
            return moved
        except StateMismatch as exc:
            log_event(
                logger,
                logging.INFO,
                "state changed under us, step abandoned",
                hash=info_hash,
                expected=expected.value,
                actual=exc.actual.value,
                dst=dst.value,
            )
        except TaskNotFound:
            pass
        except InvalidTransition as exc:
            log_event(logger, logging.ERROR, "invalid transition", hash=info_hash, error=str(exc))
        return None

    async def _step_to(self, info_hash: str, target: TaskState, reason: str) -> bool:
        """沿云端阶段逐级推进到 target。返回是否到达。"""
        task = await self._repo.get_task(info_hash)
        if task is None or task.state not in _CLOUD_ORDER:
            return False
        i = _CLOUD_ORDER.index(task.state)
        j = _CLOUD_ORDER.index(target)
        for k in range(i + 1, j + 1):
            if await self._move(info_hash, _CLOUD_ORDER[k], reason, _CLOUD_ORDER[k - 1]) is None:
                return False
        return True

    # ------------------------------------------------------------ 失败与 Arr 回退

    async def _fail(
        self, info_hash: str, reason: str, *, expected: TaskState | None = None
    ) -> None:
        """任务级失败：转 failed（expected）、写原因、回退置 PENDING。回退调用在 tick 末尾执行。"""
        reason = reason[:ERROR_MAX_LEN]
        for _ in range(3):
            task = await self._repo.get_task(info_hash)
            if task is None or task.state not in ACTIVE_STATES:
                return
            if expected is not None and task.state is not expected:
                return
            try:
                await self._repo.transition(
                    info_hash, TaskState.FAILED, reason, self._clock.now(), expected=task.state
                )
            except StateMismatch:
                continue
            except (TaskNotFound, InvalidTransition):
                return
            break
        else:
            return
        log_event(logger, logging.WARNING, "task failed", hash=info_hash, reason=reason)
        task = await self._repo.get_task(info_hash)
        if task is not None and not (task.error or "").startswith(CLEANUP_ERROR_PREFIX):
            await self._repo.set_error(info_hash, reason)
        # 只在回退仍是 NONE 时才写 PENDING：别处（Arr 回调 torrents/delete 后的回退完成）可能
        # 已经写了 DONE，不能被覆盖回去
        task = await self._repo.get_task(info_hash)
        if task is not None and task.fallback is Fallback.NONE:
            await self._repo.set_fallback(info_hash, Fallback.PENDING)
        self._wake.set()

    async def _run_fallbacks(self) -> None:
        now = self._clock.monotonic()
        failed = await self._repo.list_tasks(states={TaskState.FAILED}, include_client_deleted=True)
        for t in failed:
            if t.fallback is Fallback.DONE:
                continue
            job = self._jobs.get(t.hash)
            if job is not None and not job.task.done():
                continue  # 协程里的 _fail 还没收尾（写 error、PENDING），不和它抢
            try:
                if t.client_deleted:
                    # Arr 已调过 torrents/delete：回退已完成，不再重试、不告警
                    await self._finish_fallback(t.hash)
                    continue
                if t.fallback is Fallback.NONE:
                    # 转 failed 之后、写 PENDING 之前崩溃留下的孤儿
                    await self._repo.set_fallback(t.hash, Fallback.PENDING)
                if self._fallback_next.get(t.hash, 0.0) > now:
                    continue
                await self._fallback_one(t.hash)
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # noqa: BLE001
                self._fallback_next[t.hash] = now + self._tune.fallback_retry_seconds
                await self._task_failure("fallback", TaskState.FAILED, t.hash, exc)

    async def _finish_fallback(self, info_hash: str) -> None:
        self._fallback_next.pop(info_hash, None)
        self._queue_missing_since.pop(info_hash, None)
        with contextlib.suppress(TaskNotFound):
            await self._repo.set_fallback(info_hash, Fallback.DONE)

    async def _fallback_one(self, info_hash: str) -> None:
        task = await self._repo.get_task(info_hash)
        if task is None or task.state is not TaskState.FAILED or task.fallback is Fallback.DONE:
            return
        if task.client_deleted:
            await self._finish_fallback(info_hash)
            return
        retry_at = self._clock.monotonic() + self._tune.fallback_retry_seconds
        arr = self._arrs.get(task.category)
        if arr is None:
            self._fallback_next[info_hash] = retry_at
            await self._alert(
                f"arr-missing-{task.category}",
                f"115 Bridge: category {task.category} 没有对应的 Arr，任务 {info_hash[:8]} "
                "无法回退，保持 error。",
            )
            return
        try:
            queue_id = await arr.find_queue_item(info_hash)
            await arr.remove_and_blocklist(queue_id)
        except Exception as exc:  # noqa: BLE001 - ArrError 及任何意外错误都按回退失败处理
            if isinstance(exc, QueueItemNotFound):
                await self._queue_item_missing(info_hash, arr.name, retry_at)
                return
            self._fallback_next[info_hash] = retry_at
            log_event(
                logger,
                logging.WARNING,
                "arr fallback failed",
                hash=info_hash,
                stage="fallback",
                error=type(exc).__name__,
            )
            if isinstance(exc, ArrUnreachable):
                kind = f"arr-unreachable-{arr.name}"
            elif isinstance(exc, ArrAuthError):
                kind = f"arr-auth-{arr.name}"
            else:
                kind = f"arr-error-{arr.name}"
            await self._alert(
                kind,
                f"115 Bridge: 任务 {info_hash[:8]} 的 Arr 回退失败（{arr.name}："
                f"{type(exc).__name__}），保持 error，每 "
                f"{self._tune.fallback_retry_seconds / 60:.0f} 分钟重试。",
            )
            return
        await self._finish_fallback(info_hash)
        log_event(logger, logging.INFO, "arr fallback done", hash=info_hash, stage="fallback")

    async def _queue_item_missing(self, info_hash: str, arr_name: str, retry_at: float) -> None:
        """Arr 队列里没有该项。第一次不告警；持续超过时限就放弃，置 DONE 并推最终告警。"""
        fresh = await self._repo.get_task(info_hash)
        if fresh is not None and fresh.client_deleted:
            # Arr 已经处理并回调了 torrents/delete，队列项自然已不在
            await self._finish_fallback(info_hash)
            return
        now = self._clock.monotonic()
        since = self._queue_missing_since.setdefault(info_hash, now)
        waited = now - since
        log_event(
            logger,
            logging.WARNING,
            "arr queue item not found",
            hash=info_hash,
            stage="fallback",
            waited=waited,
        )
        if waited < self._tune.queue_not_found_seconds:
            self._fallback_next[info_hash] = retry_at
            return
        await self._finish_fallback(info_hash)
        await self._alert(
            f"arr-notfound-{info_hash[:8]}",
            f"115 Bridge: 任务 {info_hash[:8]} 在 {arr_name} 的队列里持续 "
            f"{self._tune.queue_not_found_seconds / 3600:.0f} 小时找不到，已停止自动回退，"
            f"请到 {arr_name} 手动处理（该任务保持 error）。",
        )

    # ------------------------------------------------------------ QUEUED 与 SUBMITTING

    def _submit_blocked(self) -> bool:
        return not self._backoff.ready(("submit",))

    async def _advance_queued(self) -> None:
        for t in await self._repo.list_tasks(states={TaskState.QUEUED}):
            key = ("queued", t.hash)
            if not self._backoff.ready(key):
                continue
            try:
                if await self._queued_one(t.hash):
                    return
            except asyncio.CancelledError:
                raise
            except CloudError as exc:  # GLOBAL_ERRORS 已在 _queued_one 里处理
                self._backoff.fail(key)
                self._stall(t.hash, TaskState.QUEUED, f"115 返回意外错误（{type(exc).__name__}）")
                log_event(
                    logger,
                    logging.ERROR,
                    "unexpected cloud error at instant check",
                    hash=t.hash,
                    stage="queued",
                    error=repr(exc),
                )
                await self._alert(
                    "cloud-unexpected", f"115 Bridge: 秒取回检查时遇到意外的 115 错误：{exc}"
                )
            except Exception as exc:  # noqa: BLE001
                await self._task_failure("queued", TaskState.QUEUED, t.hash, exc)

    async def _queued_one(self, info_hash: str) -> bool:
        """处理一个 QUEUED 任务。返回 True 表示全局暂停，本阶段其余任务也别试了。"""
        fresh = await self._repo.get_task(info_hash)
        if fresh is None or fresh.state is not TaskState.QUEUED:
            return False
        try:
            index = await check_instant(fresh, cloud=self._cloud, repo=self._repo)
        except GLOBAL_ERRORS as exc:
            self._global("queued", exc)
            return True
        except TaskNotFound:
            return False
        self._backoff.reset(("queued", info_hash))
        if index is not None:
            self._clear_pause("queued")
            await self._move(fresh.hash, TaskState.LOCATING, "instant restore", TaskState.QUEUED)
            return False
        self._clear_pause("queued")
        if self._submit_blocked():
            return False
        await self._move(fresh.hash, TaskState.SUBMITTING, "submit", TaskState.QUEUED)
        return False

    async def _advance_submitting(self) -> None:
        tasks = await self._repo.list_tasks(states={TaskState.SUBMITTING})
        for t in tasks:
            if self._submit_blocked():
                self._stall_all_submitting(tasks, "等待提交恢复（退避中）")
                return
            key = ("submit", t.hash)
            if not self._backoff.ready(key):
                continue
            try:
                if await self._submit_one(t.hash, tasks):
                    return
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # noqa: BLE001
                await self._task_failure("submit", TaskState.SUBMITTING, t.hash, exc)

    def _stall_all_submitting(self, tasks: list[Task], reason: str) -> None:
        for t in tasks:
            self._stall(t.hash, TaskState.SUBMITTING, reason)

    async def _submit_one(self, info_hash: str, tasks: list[Task]) -> bool:
        """提交一个 SUBMITTING 任务。返回 True 表示提交被全局暂停，本阶段其余任务也别试了。"""
        fresh = await self._repo.get_task(info_hash)
        if fresh is None or fresh.state is not TaskState.SUBMITTING:
            return False
        try:
            await submit(
                fresh,
                cloud=self._cloud,
                repo=self._repo,
                clock=self._clock,
                root_dir=self._settings.cloud.root_dir,
                quota_min=self._settings.cloud.quota_min,
                poll_seconds=float(self._settings.cloud.poll_seconds),
            )
        except SaveDirUnavailable as exc:
            self._backoff.fail(("submit",))
            self._pauses["submit"] = "save dir unavailable"
            self._stall_all_submitting(tasks, "分类保存目录不可用，已暂停提交")
            await self._alert(
                "save-dir",
                f"115 Bridge: 分类保存目录 {exc.path} 不可用（{exc.reason}），已暂停提交。",
                recover=True,
            )
            return True
        except QuotaLow as exc:
            self._pauses["submit"] = "quota low"
            self._stall_all_submitting(tasks, "离线配额低，已暂停提交")
            await self._alert(
                "quota",
                f"115 Bridge: 离线配额剩余 {exc.remaining}，低于 {exc.minimum}，已暂停新提交。",
                recover=True,
            )
            return True
        except QuotaExhausted as exc:
            self._backoff.fail(("submit",))
            self._pauses["submit"] = "quota exhausted"
            self._stall_all_submitting(tasks, "离线配额或网盘空间不足，已暂停提交")
            await self._alert(
                "quota",
                f"115 Bridge: 离线配额或网盘空间不足（{exc}），已暂停新提交。",
                recover=True,
            )
            return True
        except SubmitRejected as exc:
            await self._fail(fresh.hash, f"submit rejected: errcode={exc.errcode} {exc.message}")
            return False
        except SubmitError as exc:
            await self._fail(fresh.hash, f"submit failed: {exc}")
            return False
        except GLOBAL_ERRORS as exc:
            self._global("submit", exc)
            return True
        except CloudError as exc:
            self._backoff.fail(("submit", fresh.hash))
            self._stall(
                fresh.hash, TaskState.SUBMITTING, f"115 返回意外错误（{type(exc).__name__}）"
            )
            log_event(
                logger,
                logging.ERROR,
                "unexpected cloud error while submitting",
                hash=fresh.hash,
                stage="submit",
                error=repr(exc),
            )
            await self._alert("cloud-unexpected", f"115 Bridge: 提交时遇到意外的 115 错误：{exc}")
            return False
        except TaskNotFound:
            return False
        self._backoff.reset(("submit",))
        self._backoff.reset(("submit", fresh.hash))
        self._clear_pause("submit")
        await self._recover("quota", "115 Bridge: 离线配额已恢复，继续提交。")
        await self._recover("save-dir", "115 Bridge: 分类保存目录已恢复，继续提交。")
        await self._move(fresh.hash, TaskState.CLOUD_METADATA, "submitted", TaskState.SUBMITTING)
        return False

    # ------------------------------------------------------------ 云端跟踪

    async def _run_track(self) -> None:
        try:
            observations = await track(
                cloud=self._cloud,
                repo=self._repo,
                clock=self._clock,
                timeout_hours=float(self._settings.cloud.timeout_hours),
                state=self._track_state,
            )
        except GLOBAL_ERRORS as exc:
            self._global("track", exc)
            return
        except CloudError as exc:
            log_event(
                logger, logging.ERROR, "unexpected cloud error while tracking", error=repr(exc)
            )
            await self._alert("cloud-unexpected", f"115 Bridge: 跟踪时遇到意外的 115 错误：{exc}")
            return
        self._clear_pause("track")
        for h, obs in observations.items():
            key = ("track", h)
            if not self._backoff.ready(key):
                continue
            try:
                await self._apply_observation(h, obs)
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # noqa: BLE001
                task = await self._safe_get(h)
                await self._task_failure(
                    "track", task.state if task is not None else TaskState.CLOUD_DOWNLOADING, h, exc
                )
                continue
            self._backoff.reset(key)

    async def _safe_get(self, info_hash: str) -> Task | None:
        try:
            return await self._repo.get_task(info_hash)
        except Exception:  # noqa: BLE001
            return None

    async def _apply_observation(self, info_hash: str, obs: Observation) -> None:
        task = await self._repo.get_task(info_hash)
        if task is None or task.state not in TRACKED_STATES:
            return
        match obs.kind:
            case ObservationKind.FAILED:
                await self._fail(info_hash, f"offline task failed (status {obs.status})")
            case ObservationKind.TIMEOUT:
                await self._fail(
                    info_hash,
                    f"offline timeout after {self._settings.cloud.timeout_hours} hours",
                )
            case ObservationKind.ROOT_UNRESOLVED:
                await self._fail(info_hash, f"cannot resolve single-file root: {obs.reason}")
            case ObservationKind.DONE:
                await self._step_to(info_hash, TaskState.LOCATING, "offline done")
            case ObservationKind.METADATA:
                await self._step_to(info_hash, TaskState.CLOUD_METADATA, "offline metadata")
            case ObservationKind.DOWNLOADING:
                await self._step_to(info_hash, TaskState.CLOUD_DOWNLOADING, "offline downloading")
            case ObservationKind.NOT_FOUND:
                pass

    async def _advance_cloud_done(self) -> None:
        for t in await self._repo.list_tasks(states={TaskState.CLOUD_DONE}):
            key = ("cloud_done", t.hash)
            if not self._backoff.ready(key):
                continue
            try:
                await self._move(t.hash, TaskState.LOCATING, "cloud done", TaskState.CLOUD_DONE)
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # noqa: BLE001
                await self._task_failure("cloud_done", TaskState.CLOUD_DONE, t.hash, exc)
                continue
            self._backoff.reset(key)

    # ------------------------------------------------------------ 定位

    async def _run_locate(self) -> None:
        for t in await self._repo.list_tasks(states={TaskState.LOCATING}):
            key = ("locate", t.hash)
            if not self._backoff.ready(key):
                continue
            try:
                if await self._locate_one(t.hash, key):
                    return
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # noqa: BLE001
                await self._task_failure("locate", TaskState.LOCATING, t.hash, exc)

    async def _locate_one(self, info_hash: str, key: tuple[str, str]) -> bool:
        """定位一个任务。返回 True 表示全局暂停，本阶段其余任务也别试了。"""
        fresh = await self._repo.get_task(info_hash)
        if fresh is None or fresh.state is not TaskState.LOCATING:
            return False
        try:
            await locate(
                fresh,
                repo=self._repo,
                cloud=self._cloud,
                clock=self._clock,
                root_dir=self._settings.cloud.root_dir,
            )
        except (MatchFailed, CloudNotFound) as exc:
            await self._fail(fresh.hash, f"locate failed: {type(exc).__name__}: {exc}")
            return False
        except MissingCloudRoot as exc:
            # 调度器自身的程序缺陷：告警，不标 failed、不回退（以免拉黑好片源）
            self._backoff.fail(key)
            self._stall(fresh.hash, TaskState.LOCATING, "调度器内部问题（定位时没有云端根 id）")
            log_event(
                logger,
                logging.ERROR,
                "locate without cloud root",
                hash=fresh.hash,
                error=str(exc),
            )
            await self._alert(
                f"locate-bug-{fresh.hash[:8]}",
                f"115 Bridge: 任务 {fresh.hash[:8]} 定位时没有云端根 id（程序缺陷），已停住。",
            )
            return False
        except LocateError as exc:
            self._backoff.fail(key)
            self._stall(
                fresh.hash, TaskState.LOCATING, f"调度器内部问题（定位：{type(exc).__name__}）"
            )
            await self._alert(
                f"locate-bug-{fresh.hash[:8]}",
                f"115 Bridge: 任务 {fresh.hash[:8]} 定位出错（{type(exc).__name__}），已停住。",
            )
            return False
        except GLOBAL_ERRORS as exc:
            self._global("locate", exc)
            return True
        except CloudError as exc:
            self._backoff.fail(key)
            self._stall(fresh.hash, TaskState.LOCATING, f"115 返回意外错误（{type(exc).__name__}）")
            await self._alert("cloud-unexpected", f"115 Bridge: 定位时遇到意外的 115 错误：{exc}")
            return False
        except TaskNotFound:
            return False
        self._backoff.reset(key)
        self._clear_pause("locate")
        await self._move(fresh.hash, TaskState.FETCHING, "located", TaskState.LOCATING)
        return False

    # ------------------------------------------------------------ 后台协程：取回与校验

    async def _reap_jobs(self) -> None:
        """任务被删除（REMOVED 或记录没了）而协程还在跑时取消它。清理由 LocalCleaner 负责。"""
        for h, job in list(self._jobs.items()):
            if job.task.done():
                continue
            task = await self._repo.get_task(h)
            if task is None or task.state is TaskState.REMOVED:
                job.task.cancel()

    async def _spawn_jobs(self) -> None:
        for t in await self._repo.list_tasks(states={TaskState.FETCHING, TaskState.VERIFYING}):
            try:
                await self._spawn_one(t.hash)
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # noqa: BLE001
                kind = JobKind.FETCH if t.state is TaskState.FETCHING else JobKind.VERIFY
                self._backoff.fail((kind.value, t.hash))
                self._stall(
                    t.hash, t.state, f"调度器内部问题（{kind.value}：{type(exc).__name__}）"
                )
                await self._infra(kind.value, t.hash, exc)

    async def _spawn_one(self, info_hash: str) -> None:
        fresh = await self._repo.get_task(info_hash)
        if fresh is None:
            return
        if fresh.state is TaskState.FETCHING:
            await self._check_fetch_age(fresh)
        running = self._jobs.get(fresh.hash)
        if running is not None and not running.task.done():
            return  # 同一 hash 同时只能有一个取回或校验
        if fresh.state is TaskState.FETCHING:
            kind = JobKind.FETCH
            if not self._backoff.ready(("fetch-net",)):
                return
        elif fresh.state is TaskState.VERIFYING:
            kind = JobKind.VERIFY
        else:
            return
        if not self._backoff.ready((kind.value, fresh.hash)):
            return
        self._stalls.pop(fresh.hash, None)  # 开始新一次尝试；再停住时由协程重新记录
        self._spawn(kind, fresh.hash)

    def _spawn(self, kind: JobKind, info_hash: str) -> None:
        task = asyncio.create_task(self._run_job(kind, info_hash), name=f"{kind.value}-{info_hash}")
        job = _Job(kind, task)
        self._jobs[info_hash] = job

        def done(_t: asyncio.Task[None]) -> None:
            if self._jobs.get(info_hash) is job:
                del self._jobs[info_hash]
            self._wake.set()

        task.add_done_callback(done)

    async def _run_job(self, kind: JobKind, info_hash: str) -> None:
        try:
            if kind is JobKind.FETCH:
                await self._do_fetch(info_hash)
            else:
                await self._do_verify(info_hash)
        except asyncio.CancelledError:
            raise
        except Exception as exc:  # noqa: BLE001
            # fetch 开头读库失败等仓储异常、意外错误：基础设施故障，任务保持原状，退避后再试
            self._backoff.fail((kind.value, info_hash))
            state = TaskState.FETCHING if kind is JobKind.FETCH else TaskState.VERIFYING
            self._stall(info_hash, state, f"基础设施故障（{kind.value}：{type(exc).__name__}）")
            await self._infra(kind.value, info_hash, exc)

    async def _check_fetch_age(self, task: Task) -> None:
        """取回阶段整体时长告警：有进展不计重试，没有上限就会无限续传，所以超过阈值时告警。"""
        events = await self._repo.list_events(task.hash, limit=EVENTS_PER_TASK)
        since = next((e.at for e in events if e.to_state is TaskState.FETCHING), None)
        if since is None:
            return
        hours = (self._clock.now() - since).total_seconds() / 3600
        if hours >= self._tune.fetch_alert_hours:
            await self._alert(
                f"fetch-slow-{task.hash[:8]}",
                f"115 Bridge: 任务 {task.hash[:8]} 已在取回阶段 {hours:.0f} 小时仍未完成。",
            )

    async def _do_fetch(self, info_hash: str) -> None:
        key = (JobKind.FETCH.value, info_hash)
        try:
            result = await self._fetcher.fetch(info_hash)
        except FetchFailed as exc:
            # 连续 403 的告警由 Fetcher 的 on_cdn_403 回调驱动，这里不解析错误字符串
            await self._fail(info_hash, f"fetch failed: {exc}", expected=TaskState.FETCHING)
            return
        except FetchError as exc:  # UnsafePath、NoFilesToFetch：任务级，不计重试
            await self._fail(
                info_hash, f"fetch failed: {type(exc).__name__}: {exc}", expected=TaskState.FETCHING
            )
            return
        except CloudNotFound as exc:
            await self._fail(info_hash, f"cloud file not found: {exc}", expected=TaskState.FETCHING)
            return
        except NetworkError as exc:
            await self._fetch_network_error(info_hash, exc)
            return
        except GLOBAL_ERRORS as exc:
            self._global("fetch", exc)
            return
        except CloudError as exc:
            self._backoff.fail(key)
            self._stall(info_hash, TaskState.FETCHING, f"115 返回意外错误（{type(exc).__name__}）")
            await self._alert("cloud-unexpected", f"115 Bridge: 取回时遇到意外的 115 错误：{exc}")
            return
        except TaskNotFound:
            return
        if result.status is FetchStatus.WAITING_FOR_SPACE:
            self._backoff.fail(key)
            self._space_waiting.add(info_hash)
            self._pauses["fetch"] = "waiting for local space"
            self._stall(info_hash, TaskState.FETCHING, "等待本地空间")
            free = "unknown" if result.free_bytes is None else f"{result.free_bytes / GIB:.1f} GiB"
            need = result.needed_bytes or 0
            await self._alert(
                "local-space",
                f"115 Bridge: 本地空间不足，取回已暂停（剩余 {free}，还需 {need / GIB:.1f} GiB）。",
                recover=True,
            )
            return
        self._backoff.reset(key)
        self._backoff.reset(("fetch-net",))
        self._cdn_fail_since = None
        self._clear_pause("fetch")
        self._space_waiting.discard(info_hash)
        if not self._space_waiting:
            await self._recover("local-space", "115 Bridge: 本地空间已恢复，继续取回。")
        await self._recover("fetch-network", "115 Bridge: CDN 连接已恢复，继续取回。")
        if await self._move(info_hash, TaskState.VERIFYING, "fetched", TaskState.FETCHING):
            self._fetcher.forget(info_hash)  # 取回协程已完全结束

    async def _fetch_network_error(self, info_hash: str, exc: NetworkError) -> None:
        if not str(exc).startswith(CDN_CONNECT_FAILED_PREFIX):
            # 115 API 侧的网络问题，会话层自己感知并告警
            self._global("fetch", exc)
            return
        now = self._clock.monotonic()
        if self._cdn_fail_since is None:
            self._cdn_fail_since = now
        self._stall(info_hash, TaskState.FETCHING, "CDN 连接失败，退避后重试")
        # 同一轮里多个协程同时撞上 CDN 错误：全局退避只升一级（退避窗口内的后到者不再升级）
        if self._backoff.ready(("fetch-net",)):
            delay = self._backoff.fail(("fetch-net",))
            log_event(
                logger,
                logging.WARNING,
                "cdn unreachable, fetch paused",
                stage="fetch",
                retry_in=delay,
            )
        self._pauses["fetch"] = "cdn network"
        if now - self._cdn_fail_since >= self._tune.cdn_network_alert_seconds:
            await self._alert(
                "fetch-network",
                "115 Bridge: 连续 30 分钟以上无法连接 115 CDN，取回已暂停，退避重试中。",
                recover=True,
            )

    async def _do_verify(self, info_hash: str) -> None:
        key = (JobKind.VERIFY.value, info_hash)
        if info_hash in self._landed:
            # 已经落盘、只差写 completed：绝不再调用 verify（.incomplete/<hash> 已空，
            # 会把好片源全部判坏）。只重试 VERIFYING -> COMPLETED，退避由 _run_job 按任务记。
            await self._finish_landed(info_hash, key)
            return
        try:
            result = await self._verifier.verify(info_hash)
        except TargetExists as exc:
            # 不转 failed、不回退：停在 verifying，告警等人处理；每次重试前由退避控制
            self._backoff.fail(key)
            self._pauses["verify"] = "target exists"
            self._stall(info_hash, TaskState.VERIFYING, f"目标路径已存在：{exc.path}")
            await self._alert(
                f"target-exists-{info_hash[:8]}",
                f"115 Bridge: 任务 {info_hash[:8]} 的目标路径已存在（{exc.path}），"
                "已停在校验阶段，请人工处理。",
            )
            return
        except VerifyError as exc:  # VerifyFailed、UnsafeName、NoFilesToVerify
            await self._fail(
                info_hash,
                f"verify failed: {type(exc).__name__}: {exc}",
                expected=TaskState.VERIFYING,
            )
            return
        except FetchError as exc:  # verify 里 safe_target 的 UnsafePath
            await self._fail(
                info_hash,
                f"verify failed: {type(exc).__name__}: {exc}",
                expected=TaskState.VERIFYING,
            )
            return
        except TaskNotFound:
            return
        except OSError as exc:
            # _land 的原始错误（EXDEV、EACCES、ENAMETOOLONG、EROFS 等）：不计重试，留在 verifying
            self._backoff.fail(key)
            self._pauses["verify"] = f"os error {exc.errno}"
            self._stall(
                info_hash, TaskState.VERIFYING, f"落盘磁盘错误：{type(exc).__name__}: {exc}"
            )
            await self._alert(
                f"verify-os-{info_hash[:8]}",
                f"115 Bridge: 任务 {info_hash[:8]} 落盘失败（{type(exc).__name__}: {exc}），"
                "已停在校验阶段。",
            )
            return
        self._backoff.reset(key)
        self._clear_pause("verify")
        if result.status is VerifyStatus.REFETCH:
            await self._move(
                info_hash,
                TaskState.FETCHING,
                f"verify mismatch, refetch {len(result.bad_files)} files",
                TaskState.VERIFYING,
            )
            return
        # RENAMED：先记下落盘路径，之后只重试写 completed，绝不重新校验
        assert result.final_path is not None
        self._landed[info_hash] = result.final_path
        await self._finish_landed(info_hash, key)

    async def _finish_landed(self, info_hash: str, key: tuple[str, str]) -> None:
        """已落盘的任务：VERIFYING -> COMPLETED（带 expected）。其他异常抛给 _run_job 退避重试。"""
        if await self._move(info_hash, TaskState.COMPLETED, "verified", TaskState.VERIFYING):
            self._landed.pop(info_hash, None)
            self._backoff.reset(key)
            self._clear_pause("verify")
            self._fetcher.forget(info_hash)
            return
        # 转换没有发生：任务已不在 VERIFYING。落盘与 torrents/delete 竞态时，cleaner 可能已经先
        # 跑完，这里补删落盘的那份；其他情况下落盘记录已无意义，丢弃，避免日后误当作已落盘。
        self._backoff.reset(key)
        task = await self._repo.get_task(info_hash)
        had = self._landed.pop(info_hash, None) is not None
        if task is not None and task.state is TaskState.REMOVED and had:
            await asyncio.to_thread(_remove_content, self.cleaner._content_target(task))


__all__ = [
    "Backoff",
    "FetcherLike",
    "JobKind",
    "Orchestrator",
    "PipelineCleaner",
    "Tuning",
    "UnsafeLocalPath",
    "VerifierLike",
]
