"""取回（T11）：把 task_files 里的文件从 115 CDN 下载到 ``<paths.incomplete>/<hash>/<rel_path>``。

分工：本模块不做状态转换、不发告警、不整理到正式目录（T12）。它负责下载、记录进度、
计取回重试，并返回结果或抛明确异常，由调度器（T13）处理。

``fetch()`` 的返回值：

- ``FetchResult(DONE)``：全部文件已完整落盘（尚未校验）。
- ``FetchResult(WAITING_FOR_SPACE)``：本地空间不足（下载前检查不通过，或下载中途磁盘写满
  ``ENOSPC``）。后者会先尽力存检查点（检查点本身也写满时安全失败，记录只会落后不会超前）。
  不计重试（不变量 4），调度器稍后再调用 ``fetch()`` 即继续。

``fetch()`` 可能抛出的全部异常（除此之外没有别的）：

- ``FetchFailed``：重试预算用尽，调度器做 Arr 回退。
- ``UnsafePath`` / ``NoFilesToFetch``：任务级问题，不计重试，调度器按失败处理。
- ``CloudNotFound``：``download_link`` 报云端文件已不存在。任务级，不计重试，调度器标 failed
  并做 Arr 回退。
- ``NetworkError``：向 CDN 发请求时在收到任何响应之前就失败（``httpx.ConnectError``、
  ``httpx.ConnectTimeout``：连接被拒、DNS、连接超时、TLS 握手等），属全局网络问题，不计取回
  重试（不变量 4），调度器全局暂停取回、稍后重试。抛出前已存检查点、释放 max_files 名额，
  再次调用 ``fetch()`` 从检查点续传。读超时、收到部分数据后的断流、HTTP 层错误（403、416、
  长度或范围不符、5xx）不属于此类，照常按下面的重试计数规则处理。
- 其余 ``CloudError``（``AuthExpired``、``RiskControl`` 等）：``download_link`` 的全局异常
  原样上抛，不计重试（不变量 4）。
- ``asyncio.CancelledError``：取消。已写数据先存检查点，连接与文件句柄关闭，名额释放。
- 仓储（数据库）异常：只有 ``fetch()`` 开头读 ``task_files`` / ``tasks``，以及 ``incr_retry``
  本身失败时才会原样逃出（此时无法计数，没有别的合理处理）。下载过程中的写库失败按单次
  失败处理，不会逃出。

下载过程中的本地异常（写入、fsync、写记录、mkdir、open、写库、文件被外部截断）：
``ENOSPC`` 转 ``WAITING_FOR_SPACE``；其他一律当作单次可重试失败，不以原始异常逃出。

重试计数规则（人已拍板）：这一轮失败前该文件写入过新数据（有进展）不计重试，固定 5 秒
退避后续传；一个字节都没写进去才 ``incr_retry``，按 30 秒起翻倍（上限 1 小时）退避。
计数只增不减（不变量 3）。

空间预留：检查空间时减去同一 ``Fetcher`` 内所有正在取回（已开始未完成）任务的剩余字节。

进度持久化：每个文件一份 JSON 旁路记录 ``<incomplete>/.state/<hash>/<sha1(rel_path)>.json``，
内容是各分段 ``[start, end, done]``。检查点顺序：先 flush 并 fsync 数据，再原子写（临时文件、
fsync、replace）记录，最后更新数据库。崩溃后记录永远不超前于已落盘的数据；最多重下检查点
之后的少量数据。检查点频率：累计写入 64 MiB 或距上次检查点 10 秒，先到者。数据块写入、
fsync、写记录都在线程里做，同一文件的这些操作由一把线程锁串行（取消后遗留的线程操作不会
与收尾的检查点争用同一个临时文件）。文件下载完成后先把 ``task_files.local_bytes`` 写满，
再删记录。

写盘按块攒：读到的数据先攒在内存，达到写块大小（默认 1 MiB）才用一次线程写；失败、断流、
ENOSPC、取消时已通过校验的缓冲数据先尽力写盘。

读响应体在线程里做（T17）：每个分段请求起一个专用读线程（``threading.Thread``，不占默认线程
池），用同步 ``httpx.Client`` 发请求、按约 1 MiB 的块 ``iter_raw``，经事件循环一侧的队列交回。
线程先把响应头交回，事件循环一侧做 403、206、Content-Range、Content-Length 检查，通过后才放线
程读正文。在途块数由信用量限制（每连接最多 ``QUEUE_DEPTH`` 块），事件循环取走一块才还一份
信用，所以慢消费者会让读线程停下（背压）。读线程不写盘、不碰 ``_FileState``、不碰进度。
停止：事件循环一侧置停止标志并还一份信用，线程在下一块边界退出并自己关闭响应（不从别的线程
关闭 httpx 同步响应，那不安全）。线程若正阻塞在 socket 读上，最迟在读超时（``FETCH_TIMEOUT``
的 read，60 秒）后抛出并退出，退出后不会再投递任何东西。非取消的失败会等线程退出再返回，保证
CDN 连接已释放再重试；取消时不等（关停不被拖住），遗留线程读完当前块后自行退出。

不变量 8：链接不存储，每次请求前重新取。不变量 9：写入永远不超过分段结束位置。
"""

from __future__ import annotations

import asyncio
import contextlib
import errno
import hashlib
import json
import logging
import os
import re
import shutil
import threading
from collections.abc import AsyncIterator, Callable, Sequence
from dataclasses import dataclass, field
from enum import StrEnum
from pathlib import Path
from typing import IO, Any

import httpx

from bridge.clock import Clock
from bridge.cloud.protocol import CloudClient, CloudError, NetworkError
from bridge.config import FetchConfig
from bridge.db.repo import Stage, TaskFile, TaskRepo, TaskUpdate
from bridge.log import log_event
from bridge.pipeline.progress import ProgressTracker

logger = logging.getLogger(__name__)

GIB = 1024**3
BACKOFF_BASE_SECONDS = 30.0
BACKOFF_MAX_SECONDS = 3600.0
PROGRESS_BACKOFF_SECONDS = 5.0  # 本轮有进展的失败：固定短退避，不计重试
DEFAULT_CHECKPOINT_BYTES = 64 * 1024 * 1024
DEFAULT_CHECKPOINT_SECONDS = 10.0
DEFAULT_WRITE_BLOCK_BYTES = 1024 * 1024  # 攒够这么多才切一次线程写盘
READ_CHUNK_BYTES = 1024 * 1024  # 读线程每块大小
QUEUE_DEPTH = 4  # 每个连接最多在途（已读出、尚未被事件循环取走）的块数
STATE_DIRNAME = ".state"
_HASH_RE = re.compile(r"[0-9a-f]{40}")
_CONTENT_RANGE_RE = re.compile(r"bytes (\d+)-(\d+)/(\d+)")
_WIN_DISK_FULL = (39, 112)  # ERROR_HANDLE_DISK_FULL, ERROR_DISK_FULL

FreeSpaceFn = Callable[[Path], int]
FileEventFn = Callable[[str, str], None]  # (info_hash, rel_path)


def default_free_space(path: Path) -> int:
    return shutil.disk_usage(path).free


def _is_enospc(e: OSError) -> bool:
    return e.errno == errno.ENOSPC or getattr(e, "winerror", None) in _WIN_DISK_FULL


# ---------------------------------------------------------------- 结果与异常


class FetchStatus(StrEnum):
    DONE = "done"
    WAITING_FOR_SPACE = "waiting_for_space"


@dataclass(frozen=True, slots=True)
class FetchResult:
    status: FetchStatus
    total_bytes: int = 0  # 任务全部文件的预期大小之和
    downloaded_bytes: int = 0  # 当前已在本地的字节数
    free_bytes: int | None = None  # WAITING_FOR_SPACE 时的剩余空间
    needed_bytes: int | None = None  # WAITING_FOR_SPACE 时尚未下载的字节数


class FetchError(Exception):
    pass


class FetchFailed(FetchError):
    """取回重试超过上限。调度器做 Arr 回退。"""

    def __init__(self, info_hash: str, retries: int, last_error: str) -> None:
        super().__init__(f"{info_hash}: fetch failed after {retries} retries: {last_error}")
        self.info_hash = info_hash
        self.retries = retries
        self.last_error = last_error


class UnsafePath(FetchError):
    """task_files 的 rel_path 不安全（绝对路径、``..``、空段、越界）。任务级失败。"""


class NoFilesToFetch(FetchError):
    """任务没有 task_files（尚未定位）。"""


class _AttemptError(Exception):
    """一次下载尝试失败（可重试）。

    ``force_count`` 为真时，即使本轮写入过数据也计一次重试（例如文件被外部截断，已写的
    数据作废，不算进展）。
    """

    def __init__(self, message: str, *, force_count: bool = False) -> None:
        super().__init__(message)
        self.force_count = force_count


class _Forbidden(_AttemptError):
    """CDN 返回 403。"""


class _NoSpace(Exception):
    """本地磁盘写满（ENOSPC）。内部使用，fetch() 转成 WAITING_FOR_SPACE。"""


# ---------------------------------------------------------------- 路径


def safe_target(incomplete: Path, info_hash: str, rel_path: str) -> Path:
    """返回 ``<incomplete>/<hash>/<rel_path>``，不安全时抛 ``UnsafePath``。"""
    if not _HASH_RE.fullmatch(info_hash):
        raise UnsafePath(f"bad hash {info_hash!r}")
    if not rel_path or "\x00" in rel_path or "\\" in rel_path:
        raise UnsafePath(f"bad rel_path {rel_path!r}")
    if rel_path.startswith("/") or re.match(r"^[A-Za-z]:", rel_path):
        raise UnsafePath(f"absolute rel_path {rel_path!r}")
    parts = rel_path.split("/")
    if any(p in ("", ".", "..") for p in parts):
        raise UnsafePath(f"bad segment in rel_path {rel_path!r}")
    base = (incomplete / info_hash).resolve()
    target = (base / Path(*parts)).resolve()
    if not target.is_relative_to(base) or target == base:
        raise UnsafePath(f"rel_path escapes task dir {rel_path!r}")
    return target


# ---------------------------------------------------------------- 文件状态


@dataclass(slots=True)
class _Segment:
    start: int
    end: int  # 不含
    done: int = 0

    @property
    def length(self) -> int:
        return self.end - self.start

    @property
    def complete(self) -> bool:
        return self.done >= self.length


@dataclass(slots=True)
class _FileState:
    tf: TaskFile
    target: Path
    state_path: Path
    segments: list[_Segment]
    conns: int
    last_checkpoint: float
    discarded: bool = False  # 准备时丢弃了旧数据或旧记录
    handle: IO[bytes] | None = None
    lock: asyncio.Lock = field(default_factory=asyncio.Lock)  # 串行化检查点
    io: threading.Lock = field(default_factory=threading.Lock)  # 串行化线程里的文件操作
    unsynced: int = 0
    written: int = 0  # 本实例累计写入字节，用来判断一轮有没有进展

    @property
    def done_bytes(self) -> int:
        return sum(s.done for s in self.segments)

    @property
    def complete(self) -> bool:
        return all(s.complete for s in self.segments)


def _split(size: int, parts: int) -> list[_Segment]:
    if size <= 0:
        return []
    n = max(1, min(parts, size))
    base = size // n
    segs: list[_Segment] = []
    pos = 0
    for i in range(n):
        end = size if i == n - 1 else pos + base
        segs.append(_Segment(pos, end))
        pos = end
    return segs


def _load_segments(path: Path, size: int) -> list[_Segment] | None:
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
        if raw.get("size") != size:
            return None
        segs = [_Segment(int(a), int(b), int(c)) for a, b, c in raw["segments"]]
    except (OSError, ValueError, KeyError, TypeError, AttributeError):
        return None
    pos = 0
    for s in segs:
        if s.start != pos or s.end <= s.start or not (0 <= s.done <= s.length):
            return None
        pos = s.end
    if pos != size:
        return None
    return segs


def _stat_size(path: Path) -> int:
    try:
        return path.stat().st_size
    except OSError:
        return -1


def _write_state(path: Path, size: int, snapshot: list[tuple[int, int, int]]) -> None:
    """原子写旁路记录：临时文件、fsync、replace。失败时不留临时文件。"""
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(".tmp")
    payload = json.dumps({"v": 1, "size": size, "segments": [list(s) for s in snapshot]})
    try:
        with open(tmp, "wb") as f:
            f.write(payload.encode("utf-8"))
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp, path)
    except BaseException:
        with contextlib.suppress(OSError):
            tmp.unlink()
        raise


def _sync_file(handle: IO[bytes]) -> None:
    handle.flush()
    os.fsync(handle.fileno())


def _prepare_file(target: Path, size: int) -> None:
    target.parent.mkdir(parents=True, exist_ok=True)
    if size == 0:
        target.write_bytes(b"")


def _open_file(target: Path) -> IO[bytes]:
    return open(target, "r+b" if target.exists() else "w+b", buffering=0)  # noqa: SIM115


def _write_chunk(fs: _FileState, offset: int, data: bytes) -> None:
    """在线程里写一块数据。整个 seek+write 在锁里，两个分段共用句柄时不会交错。"""
    with fs.io:
        h = fs.handle
        if h is None:
            raise OSError("file handle closed")
        h.seek(offset)
        view = memoryview(data)
        while view:
            n = h.write(view)
            if not n:
                raise OSError(errno.ENOSPC, "short write")
            view = view[n:]


def _persist(fs: _FileState, snapshot: list[tuple[int, int, int]]) -> None:
    """在线程里：先 fsync 数据，再原子写记录。"""
    with fs.io:
        if fs.handle is not None:
            _sync_file(fs.handle)
        _write_state(fs.state_path, fs.tf.size, snapshot)


def _close_handle(fs: _FileState, *, sync: bool) -> None:
    with fs.io:
        h = fs.handle
        fs.handle = None
        if h is not None:
            try:
                if sync:
                    _sync_file(h)
            finally:
                h.close()


def _final_size(target: Path) -> int:
    target.parent.mkdir(parents=True, exist_ok=True)
    if not target.exists():
        target.touch()
    return target.stat().st_size


# ---------------------------------------------------------------- 读线程


@dataclass(frozen=True, slots=True)
class _Head:
    status_code: int
    headers: httpx.Headers


class _BodyReader:
    """一个分段请求的读线程。线程只做网络读，通过 ``_post`` 把结果交给事件循环。"""

    def __init__(
        self,
        client: httpx.Client,
        url: str,
        headers: dict[str, str],
        loop: asyncio.AbstractEventLoop,
        *,
        chunk_bytes: int,
        depth: int,
    ) -> None:
        self._client = client
        self._url = url
        self._headers = headers
        self._loop = loop
        self._chunk_bytes = chunk_bytes
        self._events: asyncio.Queue[tuple[str, Any]] = asyncio.Queue()
        self._credits = threading.Semaphore(max(1, depth))
        self._stop = threading.Event()
        self._proceed = threading.Event()
        self._closed: asyncio.Future[None] = loop.create_future()
        self._thread = threading.Thread(target=self._run, name="cdn-reader", daemon=True)

    # -- 线程一侧 --

    def _post(self, kind: str, payload: Any) -> None:
        with contextlib.suppress(RuntimeError):  # 事件循环已关闭：没有人再需要结果
            self._loop.call_soon_threadsafe(self._events.put_nowait, (kind, payload))

    def _set_closed(self) -> None:
        if not self._closed.done():
            self._closed.set_result(None)

    def _emit(self, parts: list[bytes]) -> bool:
        """把攒着的数据作为一块交出去；已被要求停止则返回 False（数据丢弃）。"""
        if self._stop.is_set():
            return False
        self._credits.acquire()  # 背压：事件循环没取走前不再往前读
        if self._stop.is_set():
            return False
        data = parts[0] if len(parts) == 1 else b"".join(parts)
        parts.clear()
        self._post("data", data)
        return True

    def _run(self) -> None:
        # 不用 ``iter_raw(chunk_size)``：httpx 的重分块器在底层流出错时会丢掉没凑满的缓冲，
        # 断流前已收到的字节就白白丢了。这里自己攒块，出错时先把已收到的交出去再报错。
        parts: list[bytes] = []
        size = 0
        try:
            with self._client.stream("GET", self._url, headers=self._headers) as resp:
                self._post("head", _Head(resp.status_code, resp.headers))
                self._proceed.wait()
                if self._stop.is_set():
                    return
                try:
                    for piece in resp.iter_raw():
                        if not piece:
                            continue
                        parts.append(piece)
                        size += len(piece)
                        if size >= self._chunk_bytes:
                            size = 0
                            if not self._emit(parts):
                                return
                finally:
                    if parts and not self._stop.is_set():
                        self._emit(parts)
                self._post("end", None)
        except BaseException as e:  # noqa: BLE001 - 原样带回事件循环一侧分类
            if not self._stop.is_set():
                self._post("error", e)
        finally:
            with contextlib.suppress(RuntimeError):
                self._loop.call_soon_threadsafe(self._set_closed)

    # -- 事件循环一侧 --

    def start(self) -> None:
        self._thread.start()

    async def head(self) -> _Head:
        kind, payload = await self._events.get()
        if kind == "error":
            if isinstance(payload, httpx.ConnectError | httpx.ConnectTimeout):
                raise NetworkError(f"cdn connection failed: {payload!r}") from payload
            raise payload
        head: _Head = payload
        return head

    def proceed(self) -> None:
        self._proceed.set()

    async def next_chunk(self) -> bytes | None:
        kind, payload = await self._events.get()
        if kind == "data":
            self._credits.release()
            data: bytes = payload
            return data
        if kind == "end":
            return None
        raise payload

    def stop(self) -> None:
        self._stop.set()
        self._proceed.set()
        self._credits.release()  # 唤醒可能正等信用的线程

    async def wait_closed(self) -> None:
        await self._closed


class _Chunks:
    def __init__(self, reader: _BodyReader) -> None:
        self._reader = reader

    def __aiter__(self) -> _Chunks:
        return self

    async def __anext__(self) -> bytes:
        chunk = await self._reader.next_chunk()
        if chunk is None:
            raise StopAsyncIteration
        return chunk


# ---------------------------------------------------------------- 取回器


class Fetcher:
    def __init__(
        self,
        *,
        repo: TaskRepo,
        cloud: CloudClient,
        http: httpx.Client,
        clock: Clock,
        config: FetchConfig,
        incomplete_dir: str | Path,
        free_space: FreeSpaceFn = default_free_space,
        tracker: ProgressTracker | None = None,
        checkpoint_bytes: int = DEFAULT_CHECKPOINT_BYTES,
        checkpoint_seconds: float = DEFAULT_CHECKPOINT_SECONDS,
        write_block_bytes: int = DEFAULT_WRITE_BLOCK_BYTES,
        read_chunk_bytes: int = READ_CHUNK_BYTES,
        queue_depth: int = QUEUE_DEPTH,
        on_cdn_403: FileEventFn | None = None,
        on_file_done: FileEventFn | None = None,
    ) -> None:
        # 可选事件回调（同步、不抛异常；抛了也会被吞掉）：某文件降到 1 连接后仍 403 时调用
        # on_cdn_403，文件经 CDN 下载完成时调用 on_file_done。供调度器统计连续 403。
        self._on_cdn_403 = on_cdn_403
        self._on_file_done = on_file_done
        self._repo = repo
        self._cloud = cloud
        self._http = http
        self._clock = clock
        self._cfg = config
        self._incomplete = Path(incomplete_dir)
        self._free_space = free_space
        self._checkpoint_bytes = max(1, checkpoint_bytes)
        self._checkpoint_seconds = checkpoint_seconds
        self._write_block = max(1, write_block_bytes)
        self._read_chunk = max(1, read_chunk_bytes)
        self._queue_depth = max(1, queue_depth)
        self.tracker = tracker or ProgressTracker(clock)
        self._file_slots = asyncio.Semaphore(max(1, config.max_files))
        self._active_files = 0
        self.peak_active_files = 0
        self._task_bytes: dict[str, dict[str, int]] = {}
        self._remaining: dict[str, int] = {}  # 正在取回的任务还没下的字节数（空间预留）

    @property
    def progress_source(self) -> ProgressTracker:
        """给 qBit 层的 ``ProgressSource``。"""
        return self.tracker

    def state_dir(self, info_hash: str) -> Path:
        return self._incomplete / STATE_DIRNAME / info_hash

    def forget(self, info_hash: str) -> None:
        """丢弃该 hash 的进度记录和运行时信息（任务被删除或终结时由调度器调用）。"""
        shutil.rmtree(self.state_dir(info_hash), ignore_errors=True)
        self.tracker.clear(info_hash)
        self._task_bytes.pop(info_hash, None)

    # -- 入口 --

    async def fetch(self, info_hash: str) -> FetchResult:
        files = await self._repo.list_task_files(info_hash)
        if not files:
            raise NoFilesToFetch(info_hash)
        targets = {
            tf.rel_path: safe_target(self._incomplete, info_hash, tf.rel_path) for tf in files
        }
        total = sum(tf.size for tf in files)

        states: dict[str, _FileState] = {}
        for tf in files:
            if tf.verified:
                continue
            states[tf.rel_path] = self._prepare(info_hash, tf, targets[tf.rel_path])
        verified_bytes = sum(tf.size for tf in files if tf.verified)
        have = verified_bytes + sum(s.done_bytes for s in states.values())
        needed = total - have
        await self._zero_discarded(info_hash, states, have)

        task = await self._repo.get_task(info_hash)
        if task is not None and task.retries.get(Stage.FETCH, 0) > self._cfg.max_retries:
            raise FetchFailed(info_hash, task.retries[Stage.FETCH], "retry budget already spent")

        while True:
            try:
                self._incomplete.mkdir(parents=True, exist_ok=True)
                free = self._free_space(self._incomplete)
                break
            except OSError as e:
                if _is_enospc(e):
                    free = 0
                    break
                await self._count_failure(info_hash, "", _AttemptError(f"local io error: {e!r}"))
        # 从这里到登记预留之间没有 await，两个任务不会同时通过检查
        reserved_elsewhere = sum(v for k, v in self._remaining.items() if k != info_hash)
        if free - reserved_elsewhere - needed < self._cfg.min_free_gib * GIB:
            return self._waiting(info_hash, total, have, free, needed)
        self._remaining[info_hash] = needed

        self.tracker.begin(info_hash, needed)
        self.tracker.set_waiting(info_hash, False)
        self._task_bytes[info_hash] = {
            tf.rel_path: (tf.size if tf.verified else states[tf.rel_path].done_bytes)
            for tf in files
        }
        jobs = [asyncio.ensure_future(self._download_file(info_hash, s)) for s in states.values()]
        no_space = False
        try:
            try:
                await self._gather(jobs)
            except _NoSpace:
                no_space = True
        finally:
            for s in states.values():
                with contextlib.suppress(Exception):
                    await asyncio.to_thread(_close_handle, s, sync=False)
            self._remaining.pop(info_hash, None)
        if no_space:
            have_now = verified_bytes + sum(s.done_bytes for s in states.values())
            try:
                free_now: int | None = self._free_space(self._incomplete)
            except OSError:
                free_now = None
            return self._waiting(info_hash, total, have_now, free_now, total - have_now)
        shutil.rmtree(self.state_dir(info_hash), ignore_errors=True)
        self._task_bytes.pop(info_hash, None)
        return FetchResult(FetchStatus.DONE, total, total)

    def _waiting(
        self, info_hash: str, total: int, have: int, free: int | None, needed: int
    ) -> FetchResult:
        self.tracker.begin(info_hash, needed)
        self.tracker.set_waiting(info_hash, True)
        log_event(
            logger,
            logging.WARNING,
            "fetch waiting for space",
            hash=info_hash,
            stage="fetch",
            free=free,
            needed=needed,
        )
        return FetchResult(
            FetchStatus.WAITING_FOR_SPACE, total, have, free_bytes=free, needed_bytes=needed
        )

    async def _zero_discarded(
        self, info_hash: str, states: dict[str, _FileState], have: int
    ) -> None:
        """准备时丢弃了旧数据的文件，把库里的 local_bytes 同步归零（只影响显示）。"""
        dropped = [rel for rel, s in states.items() if s.discarded]
        if not dropped:
            return
        try:
            for rel in dropped:
                await self._repo.update_task_file(info_hash, rel, local_bytes=0)
            await self._repo.update(info_hash, TaskUpdate(local_bytes=have))
        except Exception as e:  # 纯显示用，失败不影响取回
            log_event(
                logger,
                logging.WARNING,
                "reset local_bytes failed",
                hash=info_hash,
                stage="fetch",
                error=repr(e),
            )

    @staticmethod
    async def _gather(jobs: Sequence[asyncio.Future[None]]) -> None:
        """全部完成；任一失败则取消其余并抛出第一个异常。"""
        try:
            done, pending = await asyncio.wait(jobs, return_when=asyncio.FIRST_EXCEPTION)
            for j in done:
                exc = j.exception()
                if exc is not None:
                    raise exc
            while pending:
                done, pending = await asyncio.wait(pending, return_when=asyncio.FIRST_EXCEPTION)
                for j in done:
                    exc = j.exception()
                    if exc is not None:
                        raise exc
        finally:
            for j in jobs:
                if not j.done():
                    j.cancel()
            await asyncio.gather(*jobs, return_exceptions=True)

    # -- 文件准备 --

    def _prepare(self, info_hash: str, tf: TaskFile, target: Path) -> _FileState:
        state_path = self.state_dir(info_hash) / (
            hashlib.sha1(tf.rel_path.encode("utf-8")).hexdigest() + ".json"
        )
        conns = max(1, self._cfg.connections_per_file)
        has_record = state_path.exists()
        segs = _load_segments(state_path, tf.size) if has_record else None
        if segs is not None:
            # 记录里的数据必须真的在盘上，否则重来
            on_disk = _stat_size(target)
            extent = max((s.start + s.done for s in segs if s.done > 0), default=0)
            if on_disk < extent or on_disk > tf.size:
                segs = None
        elif (
            not has_record
            and tf.size > 0
            and tf.local_bytes == tf.size
            and _stat_size(target) == tf.size
        ):
            # 无记录：仅当文件完整且库里也记着完整时视为已下完
            segs = [_Segment(0, tf.size, tf.size)]
        discarded = False
        if segs is None:
            discarded = has_record or target.exists() or tf.local_bytes > 0
            with contextlib.suppress(OSError):
                target.unlink()
            with contextlib.suppress(OSError):
                state_path.unlink()
            segs = _split(tf.size, conns)
        return _FileState(
            tf=tf,
            target=target,
            state_path=state_path,
            segments=segs,
            conns=conns,
            last_checkpoint=self._clock.monotonic(),
            discarded=discarded,
        )

    # -- 单文件 --

    def _emit(self, fn: FileEventFn | None, info_hash: str, rel_path: str) -> None:
        if fn is None:
            return
        try:
            fn(info_hash, rel_path)
        except Exception as e:  # noqa: BLE001 - 回调的错误不能影响取回
            log_event(
                logger,
                logging.WARNING,
                "fetch event callback failed",
                hash=info_hash,
                stage="fetch",
                error=repr(e),
            )

    async def _download_file(self, info_hash: str, fs: _FileState) -> None:
        tf = fs.tf
        already_complete = fs.complete and tf.size > 0  # 续传时早已完成的文件没有碰 CDN
        while True:
            err: _AttemptError | None = None
            written_before = fs.written
            async with self._file_slots:
                self._active_files += 1
                self.peak_active_files = max(self.peak_active_files, self._active_files)
                try:
                    await self._attempt(info_hash, fs)
                except _Forbidden as e:
                    if fs.conns > 1:
                        # 先降到 1 个连接并重取链接，不计重试
                        fs.conns = 1
                        log_event(
                            logger,
                            logging.INFO,
                            "cdn 403, dropping to 1 connection",
                            hash=info_hash,
                            stage="fetch",
                            file=tf.rel_path,
                        )
                        continue
                    self._emit(self._on_cdn_403, info_hash, tf.rel_path)
                    err = e
                except _AttemptError as e:
                    err = e
                except (httpx.HTTPError, httpx.InvalidURL) as e:
                    err = _AttemptError(repr(e))
                except OSError as e:
                    if _is_enospc(e):
                        raise _NoSpace(str(e)) from e
                    err = _AttemptError(f"local io error: {e!r}")
                except (CloudError, FetchError):
                    raise
                except Exception as e:
                    # 兜底：没预料到的异常（如关闭的句柄抛 ValueError）也当作单次失败，
                    # 不以原始异常逃出 fetch()
                    log_event(
                        logger,
                        logging.ERROR,
                        "unexpected error while fetching",
                        hash=info_hash,
                        stage="fetch",
                        file=tf.rel_path,
                        error=repr(e),
                    )
                    err = _AttemptError(f"unexpected error: {e!r}")
                finally:
                    self._active_files -= 1
            if err is None:
                if not already_complete:
                    self._emit(self._on_file_done, info_hash, tf.rel_path)
                return
            if fs.written > written_before and not err.force_count:
                log_event(
                    logger,
                    logging.INFO,
                    "fetch attempt failed after progress, resuming",
                    hash=info_hash,
                    stage="fetch",
                    file=tf.rel_path,
                    error=str(err),
                )
                await self._clock.sleep(PROGRESS_BACKOFF_SECONDS)
            else:
                await self._count_failure(info_hash, tf.rel_path, err)

    async def _count_failure(self, info_hash: str, rel_path: str, err: _AttemptError) -> None:
        """没有进展的失败：计一次重试；超限抛 FetchFailed，否则指数退避。"""
        n = await self._repo.incr_retry(info_hash, Stage.FETCH)
        log_event(
            logger,
            logging.WARNING,
            "fetch attempt failed",
            hash=info_hash,
            stage="fetch",
            file=rel_path,
            retries=n,
            error=str(err),
        )
        if n > self._cfg.max_retries:
            raise FetchFailed(info_hash, n, str(err))
        await self._clock.sleep(min(BACKOFF_BASE_SECONDS * 2 ** (n - 1), BACKOFF_MAX_SECONDS))

    async def _attempt(self, info_hash: str, fs: _FileState) -> None:
        """一次尝试：补齐剩余分段，然后做完成检查。任何失败以异常表示。"""
        if not fs.complete or fs.tf.size == 0:
            await asyncio.to_thread(_prepare_file, fs.target, fs.tf.size)
        if not fs.complete:
            await self._round(info_hash, fs)
        if fs.complete:
            await self._finish_file(info_hash, fs)

    async def _finish_file(self, info_hash: str, fs: _FileState) -> None:
        tf = fs.tf
        await asyncio.to_thread(_close_handle, fs, sync=True)
        if await asyncio.to_thread(_final_size, fs.target) != tf.size:
            # 例如文件被外部截断；整文件重来，计一次失败
            with contextlib.suppress(OSError):
                fs.target.unlink()
            with contextlib.suppress(OSError):
                fs.state_path.unlink()
            fs.segments = _split(tf.size, fs.conns)
            fs.unsynced = 0
            with contextlib.suppress(Exception):
                await self._update_db(info_hash, tf.rel_path, 0)
            raise _AttemptError("final size mismatch", force_count=True)
        async with fs.lock:
            await self._update_db(info_hash, tf.rel_path, tf.size)
        with contextlib.suppress(OSError):
            fs.state_path.unlink()

    async def _round(self, info_hash: str, fs: _FileState) -> None:
        """跑一轮：所有未完成分段并发（受 fs.conns 限制）。任一失败则取消其余并抛出。"""
        if fs.handle is None:
            fs.handle = await asyncio.to_thread(_open_file, fs.target)
        gate = asyncio.Semaphore(fs.conns)

        async def run(seg: _Segment) -> None:
            async with gate:
                if not seg.complete:
                    await self._request(info_hash, fs, seg)

        jobs = [asyncio.ensure_future(run(s)) for s in fs.segments if not s.complete]
        try:
            await self._gather(jobs)
        except BaseException:
            # 失败或取消：尽力把已写入的部分落到记录里；检查点自己的任何错误都不得顶掉原异常
            with contextlib.suppress(Exception):
                await self._checkpoint(info_hash, fs)
            raise
        await self._checkpoint(info_hash, fs)

    async def _request(self, info_hash: str, fs: _FileState, seg: _Segment) -> None:
        tf = fs.tf
        link = await self._cloud.download_link(tf.pickcode)  # CloudError 原样上抛
        start = seg.start + seg.done
        last = seg.end - 1
        headers = {**link.headers, "Range": f"bytes={start}-{last}", "Accept-Encoding": "identity"}
        reader = _BodyReader(
            self._http,
            link.url,
            headers,
            asyncio.get_running_loop(),
            chunk_bytes=self._read_chunk,
            depth=self._queue_depth,
        )
        cancelled = False
        reader.start()
        try:
            # 收到响应前的 ConnectError / ConnectTimeout 在这里转成 NetworkError
            head = await reader.head()
            if head.status_code == 403:
                raise _Forbidden("cdn 403")
            if head.status_code != 206:
                raise _AttemptError(f"unexpected status {head.status_code}")
            m = _CONTENT_RANGE_RE.fullmatch(head.headers.get("content-range", "").strip())
            if m is None or (int(m[1]), int(m[2]), int(m[3])) != (start, last, tf.size):
                raise _AttemptError(f"bad content-range {head.headers.get('content-range')!r}")
            cl = head.headers.get("content-length")
            if cl is None or not cl.isdigit() or int(cl) != last - start + 1:
                raise _AttemptError(f"bad content-length {cl!r}")
            reader.proceed()
            await self._stream_body(info_hash, fs, seg, _Chunks(reader))
        except asyncio.CancelledError:
            cancelled = True
            raise
        finally:
            reader.stop()
            if not cancelled:
                await reader.wait_closed()  # 连接释放后才重试，CDN 的连接计数才准

    async def _stream_body(
        self, info_hash: str, fs: _FileState, seg: _Segment, chunks: AsyncIterator[bytes]
    ) -> None:
        """读响应正文，攒到写块大小再一次性写盘。

        ``seg.done``、``fs.unsynced``、``fs.written`` 只在数据真正写入文件后推进，记录永不超前
        于落盘数据。不变量 9 的判断基于「已写加缓冲」。任何异常（断流、取消、超长等）退出前，
        先尽力把缓冲区里已通过校验的数据写盘，写盘自己的错误不会顶掉原异常。
        """
        buf: list[bytes] = []
        buffered = 0

        async def flush() -> None:
            nonlocal buffered
            if not buffered:
                return
            data = buf[0] if len(buf) == 1 else b"".join(buf)
            n = len(data)
            # 同一偏移重复写是幂等的：第一次被取消后再来一次不会出错
            await asyncio.to_thread(_write_chunk, fs, seg.start + seg.done, data)
            buf.clear()
            buffered = 0
            seg.done += n
            fs.written += n
            fs.unsynced += n
            if info_hash in self._remaining:
                self._remaining[info_hash] = max(0, self._remaining[info_hash] - n)

        overflow = False
        try:
            async for chunk in chunks:
                room = seg.end - (seg.start + seg.done + buffered)
                if len(chunk) > room:
                    chunk = chunk[:room]
                    overflow = True
                if chunk:
                    buf.append(chunk)
                    buffered += len(chunk)
                    self.tracker.record(info_hash, len(chunk))
                    elapsed = self._clock.monotonic() - fs.last_checkpoint
                    if (
                        buffered >= self._write_block
                        or fs.unsynced + buffered >= self._checkpoint_bytes
                        or elapsed >= self._checkpoint_seconds
                    ):
                        await flush()
                        if (
                            fs.unsynced >= self._checkpoint_bytes
                            or self._clock.monotonic() - fs.last_checkpoint
                            >= self._checkpoint_seconds
                        ):
                            await self._checkpoint(info_hash, fs)
                if overflow:
                    raise _AttemptError("cdn sent more bytes than requested")
            await flush()
        except BaseException:
            with contextlib.suppress(Exception):
                await flush()
            raise
        if not seg.complete:
            raise _AttemptError("stream ended early")

    # -- 检查点 --

    async def _checkpoint(self, info_hash: str, fs: _FileState) -> None:
        async with fs.lock:
            snapshot = [(s.start, s.end, s.done) for s in fs.segments]
            done = sum(s[2] for s in snapshot)
            await asyncio.to_thread(_persist, fs, snapshot)  # 先落数据，再写记录
            fs.unsynced = 0
            fs.last_checkpoint = self._clock.monotonic()
            await self._update_db(info_hash, fs.tf.rel_path, done)

    async def _update_db(self, info_hash: str, rel_path: str, done: int) -> None:
        per_file = self._task_bytes.setdefault(info_hash, {})
        per_file[rel_path] = done
        try:
            await self._repo.update_task_file(info_hash, rel_path, local_bytes=done)
            await self._repo.update(info_hash, TaskUpdate(local_bytes=sum(per_file.values())))
        except Exception as e:
            raise _AttemptError(f"database error: {e!r}") from e


__all__ = [
    "Fetcher",
    "FetchFailed",
    "FetchError",
    "FetchResult",
    "FetchStatus",
    "FreeSpaceFn",
    "NoFilesToFetch",
    "UnsafePath",
    "safe_target",
]
