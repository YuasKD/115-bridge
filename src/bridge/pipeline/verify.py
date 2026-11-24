"""校验与落盘（T12）：逐文件校验大小和 sha1，全部通过后整树 rename 到正式目录。

分工：本模块不做状态转换、不发告警（调度器 T13 负责）。它只校验、计重试、改名、
写 ``task_files.verified``，并返回结果或抛明确异常：

- 返回 ``VerifyResult(RENAMED)``：全部通过，已原子 rename 到 ``layout.content_path``。
  调度器此后才可转 completed（不变量 10：在此之前对 Arr 不报 ``stoppedUP``）。
- 返回 ``VerifyResult(REFETCH)``：有文件大小或 sha1 不符（含缺失），已删掉这些文件
  （不动其他文件）并把 ``Stage.VERIFY`` 计数加一。调度器把 verifying 转回 fetching 再调
  ``Fetcher.fetch``，它只会重下被删的文件。
- 抛 ``VerifyFailed``：不符且计数已超过 ``verify.max_retries``，调度器做 Arr 回退。
- 抛 ``TargetExists``：目标路径已存在。不覆盖、不删除、不加后缀，什么都不动。
- 抛 ``UnsafeName``：任务名或分类不适合做路径分量。检查在任何操作之前，不计重试。
- 抛 ``NoFilesToVerify`` / ``bridge.db.repo.TaskNotFound`` / ``UnsafePath``（来自 fetch）：
  任务级问题。
- 原始 ``OSError``：``_land``（建父目录、``os.rename``）的失败不被包装，直接逃出，调度器需要
  处理：跨卷 ``EXDEV``（incomplete 与正式目录不在同一文件系统，属部署配置错误）、权限不足
  ``EACCES`` / ``EPERM``、文件名过长 ``ENAMETOOLONG``、只读文件系统 ``EROFS`` 等。此时
  不计重试、不动 task_files 的 verified（已写为 True），源文件原样保留。

REFETCH 时同步把 ``tasks.local_bytes`` 减去被删文件的字节（按剩余 task_files 重算），界面进度
不会偏高。

重试计数只增不减（不变量 3）。正式目录在 rename 之前绝不出现任何内容（不变量 10），
rename 用同文件系统的 ``os.rename``，不会退化成复制。
"""

from __future__ import annotations

import asyncio
import contextlib
import hashlib
import logging
import os
from dataclasses import dataclass, field
from enum import StrEnum
from pathlib import Path

from bridge import layout
from bridge.config import VerifyConfig
from bridge.db.repo import Stage, TaskFile, TaskNotFound, TaskRepo, TaskUpdate
from bridge.log import log_event
from bridge.pipeline.fetch import safe_target

logger = logging.getLogger(__name__)

CHUNK_BYTES = 1024 * 1024


class VerifyStatus(StrEnum):
    RENAMED = "renamed"
    REFETCH = "refetch"


@dataclass(frozen=True, slots=True)
class VerifyResult:
    status: VerifyStatus
    final_path: str | None = None  # RENAMED：落盘后的 content_path
    bad_files: tuple[str, ...] = ()  # REFETCH：被删除的 rel_path
    retries: int = 0  # REFETCH：Stage.VERIFY 的新计数
    skipped: tuple[str, ...] = field(default=())  # 已 verified 且大小未变而跳过重算的文件


class VerifyError(Exception):
    pass


class VerifyFailed(VerifyError):
    def __init__(self, info_hash: str, retries: int, bad_files: tuple[str, ...]) -> None:
        super().__init__(f"{info_hash}: verify failed {retries} times, bad files: {bad_files}")
        self.info_hash = info_hash
        self.retries = retries
        self.bad_files = bad_files


class TargetExists(VerifyError):
    def __init__(self, info_hash: str, path: str) -> None:
        super().__init__(f"{info_hash}: target already exists: {path}")
        self.info_hash = info_hash
        self.path = path


class UnsafeName(VerifyError):
    pass


class NoFilesToVerify(VerifyError):
    pass


def _check_component(value: str, what: str) -> None:
    if value in ("", ".", "..") or any(c in value for c in ("/", "\\", "\x00")):
        raise UnsafeName(f"unsafe {what}: {value!r}")


def _sha1_file(path: Path) -> str:
    h = hashlib.sha1()
    with path.open("rb") as f:
        while chunk := f.read(CHUNK_BYTES):
            h.update(chunk)
    return h.hexdigest()


def _size_matches(path: Path, size: int) -> bool:
    try:
        return path.is_file() and path.stat().st_size == size
    except OSError:
        return False


def _land(source: Path, final: Path, *, single_root: Path | None) -> bool:
    """rename 到正式位置。目标已存在返回 True 且什么都不动。

    lexists：悬空符号链接也算存在。不覆盖、不删除、不加后缀。
    """
    if os.path.lexists(final):
        return True
    final.parent.mkdir(parents=True, exist_ok=True)
    os.rename(source, final)
    if single_root is not None:
        with contextlib.suppress(OSError):
            single_root.rmdir()
    return False


async def _land_shielded(source: Path, final: Path, *, single_root: Path | None) -> bool:
    """在线程里落盘，且不因取消而半途丢失结果。

    ``to_thread`` 的线程无法被取消：等待者被取消后线程仍会把 rename 做完，调用方却不知道。
    所以用 ``shield`` 保护，收到取消时继续等线程结束：

    - 落盘成功（返回 False）：吞掉这次取消，正常返回，调用方据此记录落盘路径、转 completed
      或清理（协程被取消的原因，例如任务被删除，由调用方转换时的 ``expected`` 检查发现）。
    - 目标已存在或 rename 失败：重新抛出取消（落盘没有发生，没有什么要通知的）。
    """
    fut = asyncio.ensure_future(asyncio.to_thread(_land, source, final, single_root=single_root))
    cancelled = False
    while not fut.done():
        try:
            await asyncio.shield(fut)
        except asyncio.CancelledError:
            cancelled = True  # 线程还在跑：继续等它结束
        except Exception:  # noqa: BLE001, S110 - 线程自己的异常由下面的 result() 抛出
            pass
    try:
        exists = fut.result()
    except Exception:
        if cancelled:
            raise asyncio.CancelledError from None
        raise
    if cancelled:
        if exists:
            raise asyncio.CancelledError
        log_event(
            logger,
            logging.INFO,
            "cancelled during landing, landing completed",
            stage="verify",
            path=str(final),
        )
    return exists


class Verifier:
    def __init__(
        self,
        repo: TaskRepo,
        *,
        root: str | Path,
        incomplete: str | Path,
        cfg: VerifyConfig | None = None,
    ) -> None:
        self._repo = repo
        self._root = str(root)
        self._incomplete = Path(incomplete)
        self._cfg = cfg or VerifyConfig()

    async def verify(self, info_hash: str) -> VerifyResult:
        task = await self._repo.get_task(info_hash)
        if task is None:
            raise TaskNotFound(info_hash)
        _check_component(task.name, "task name")
        _check_component(task.category, "category")
        files = await self._repo.list_task_files(info_hash)
        if not files:
            raise NoFilesToVerify(info_hash)
        manifest = await self._repo.get_manifest(info_hash)
        single = layout.is_single_file(task, manifest)
        if single and len(files) != 1:
            raise NoFilesToVerify(f"{info_hash}: single-file manifest but {len(files)} files")

        bad: list[str] = []
        skipped: list[str] = []
        for tf in files:
            path = safe_target(self._incomplete, info_hash, tf.rel_path)
            outcome = await self._check_file(tf, path)
            if outcome == "skipped":
                skipped.append(tf.rel_path)
            if outcome == "bad":
                bad.append(tf.rel_path)
                with contextlib.suppress(OSError):
                    path.unlink()
                await self._repo.update_task_file(
                    info_hash, tf.rel_path, local_bytes=0, verified=False
                )
            else:
                await self._repo.update_task_file(info_hash, tf.rel_path, verified=True)

        if bad:
            remaining = sum(tf.local_bytes for tf in await self._repo.list_task_files(info_hash))
            await self._repo.update(info_hash, TaskUpdate(local_bytes=remaining))
            retries = await self._repo.incr_retry(info_hash, Stage.VERIFY)
            log_event(
                logger,
                logging.WARNING,
                "verify mismatch",
                hash=info_hash,
                stage="verify",
                bad=len(bad),
                retries=retries,
            )
            if retries > self._cfg.max_retries:
                raise VerifyFailed(info_hash, retries, tuple(bad))
            return VerifyResult(VerifyStatus.REFETCH, bad_files=tuple(bad), retries=retries)

        final = Path(layout.content_path(self._root, task, manifest))
        source_root = self._incomplete / info_hash
        if single:
            source = safe_target(self._incomplete, info_hash, files[0].rel_path)
        else:
            source = source_root
        if await _land_shielded(source, final, single_root=source_root if single else None):
            raise TargetExists(info_hash, str(final))
        log_event(
            logger,
            logging.INFO,
            "verified and moved",
            hash=info_hash,
            stage="verify",
            files=len(files),
        )
        return VerifyResult(VerifyStatus.RENAMED, final_path=str(final), skipped=tuple(skipped))

    async def _check_file(self, tf: TaskFile, path: Path) -> str:
        """返回 ``ok`` / ``skipped``（已 verified 且大小没变）/ ``bad``。"""
        if not await asyncio.to_thread(_size_matches, path, tf.size):
            return "bad"
        if tf.verified:
            return "skipped"
        if not tf.sha1:
            # 115 没给 sha1（设计 193 行：纯 magnet 只校验大小），大小已符合
            return "ok"
        try:
            digest = await asyncio.to_thread(_sha1_file, path)
        except OSError:
            return "bad"
        return "ok" if digest.lower() == tf.sha1.lower() else "bad"
