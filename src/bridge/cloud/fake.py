"""内存版 115，实现 ``CloudClient``，附带假 CDN。仅供测试，不访问网络。

- ``FakeCloud``：按场景脚本推进离线任务，完成后生成目录树；记录所有调用；
  可在第 N 次调用某方法时抛指定异常。**不提供任何删除方法**（不变量 2）。
- ``FakeCdn``：``httpx.MockTransport`` 的处理器（``cdn.handler`` / ``cdn.transport``），
  模拟 Range/206、同文件 2 连接上限、断流、错误长度、user-agent 与 token 校验。
"""

from __future__ import annotations

import asyncio
import base64
import hashlib
import itertools
import math
import re
import threading
from collections.abc import AsyncIterator, Iterator
from dataclasses import dataclass
from typing import Any, Literal
from urllib.parse import quote

import httpx

from bridge.clock import Clock
from bridge.cloud.protocol import (
    OFFLINE_DONE_STATUS,
    CloudEntry,
    CloudError,
    CloudNotFound,
    CloudSpace,
    DownloadLink,
    OfflinePage,
    OfflineStat,
    OfflineTask,
    QrStatus,
    QrToken,
    Quota,
    QuotaExhausted,
    SubmitRejected,
)

CDN_HOST = "cdn.fake115.test"
ROOT_ID = "0"

_ILLEGAL_CHARS = '<>:"/\\|?*'

# ---------------------------------------------------------------- 辅助


def make_content(seed: str, size: int) -> bytes:
    """确定性的伪随机内容。"""
    out = bytearray()
    counter = 0
    while len(out) < size:
        out += hashlib.sha256(f"{seed}:{counter}".encode()).digest()
        counter += 1
    return bytes(out[:size])


def rename_illegal(name: str) -> str:
    """模拟 115 把非法字符替换成下划线。"""
    return "".join("_" if c in _ILLEGAL_CHARS else c for c in name)


def truncate_name(name: str, max_len: int) -> str:
    """模拟 115 截断过长文件名，保留扩展名。"""
    if len(name) <= max_len:
        return name
    stem, dot, ext = name.rpartition(".")
    if not dot or len(ext) >= max_len:
        return name[:max_len]
    return stem[: max_len - len(ext) - 1] + "." + ext


def parse_btih(magnet: str) -> str:
    m = re.search(r"xt=urn:btih:([0-9A-Za-z]+)", magnet)
    if m is None:
        raise SubmitRejected(None, "not a btih magnet")
    h = m.group(1)
    if len(h) == 40:
        return h.lower()
    if len(h) == 32:
        return base64.b32decode(h.upper()).hex()
    raise SubmitRejected(None, "bad btih length")


# ---------------------------------------------------------------- 场景


@dataclass(slots=True)
class FakeFile:
    path: str  # 种子内相对路径，如 "Season 1/a.mkv"
    content: bytes
    cloud_name: str | None = None  # 115 改名后的文件名（只改最后一段）


@dataclass(slots=True)
class OfflineScenario:
    """一个 hash 的离线进程脚本，随假时钟推进。

    时间线（自提交起）：
    ``ramp_seconds`` 内从 0 升到 ``stall_percent``；再停留 ``stall_seconds``；
    然后（``never_finishes`` 为假时）可选地经过 ``transient_seconds`` 的过渡态；
    最后进入 ``outcome``：2 完成，或 -1/3/4/5 失败。
    过渡态 ``transient_kind``：
    - ``"percent100"``：percent 100，status 仍为 1，file_id 为空；
    - ``"no_file_id"``：status 2，percent 100，file_id 仍为空。
    """

    name: str
    files: list[FakeFile]
    ramp_seconds: float = 60.0
    stall_percent: float = 99.0
    stall_seconds: float = 0.0
    transient_seconds: float = 0.0
    transient_kind: Literal["percent100", "no_file_id"] = "percent100"
    outcome: int = OFFLINE_DONE_STATUS
    never_finishes: bool = False
    reject: tuple[int | None, str] | None = None  # 设置后 add_magnet 抛 SubmitRejected
    single_file_at_root: bool = False
    """单文件种子的 115 真实形态：离线完成时 ``file_id`` 等于 ``wp_path_id``，
    唯一的文件直接放在保存目录里，文件名为 ``name``（即 ``del_path``），不建任务目录。
    要求 ``files`` 恰好一个；``cloud_name`` 被忽略。"""

    @property
    def size(self) -> int:
        return sum(len(f.content) for f in self.files)


@dataclass(slots=True)
class _Override:
    status: int
    percent_done: float
    with_file_id: bool


@dataclass(slots=True)
class _Task:
    hash: str
    scenario: OfflineScenario
    added_at: float
    wp_path_id: str
    root_id: str | None
    override: _Override | None = None


@dataclass(frozen=True, slots=True)
class Call:
    method: str
    args: tuple[Any, ...]
    kwargs: dict[str, Any]


@dataclass(slots=True)
class _Fault:
    method: str
    exc: CloudError
    on_call: int
    times: int | None  # None 表示此后每次都抛


# ---------------------------------------------------------------- 假 CDN


@dataclass(frozen=True, slots=True)
class CdnFault:
    """脚本化的 CDN 异常，按顺序被同一文件通过校验的请求消费。

    kind:
    - ``"cut"``：只发 ``after_bytes`` 字节后断流（抛 ``httpx.ReadError``）；
    - ``"bad_length"``：Content-Length 比真实长度多 ``delta``；
    - ``"bad_range"``：Content-Range 的结束位置偏移 ``delta``；
    - ``"full_200"``：忽略 Range，返回 200 全量；
    - ``"status"``：直接返回 ``status``。
    """

    kind: Literal["cut", "bad_length", "bad_range", "full_200", "status"]
    after_bytes: int = 0
    delta: int = 1
    status: int = 500


@dataclass(frozen=True, slots=True)
class CdnRequest:
    pickcode: str
    range: str | None
    status: int
    token: str | None
    user_agent: str | None


@dataclass(slots=True)
class _LinkInfo:
    pickcode: str
    user_agent: str
    expires_at: float


class _Stream(httpx.SyncByteStream, httpx.AsyncByteStream):
    """同时支持同步和异步迭代，供 ``httpx.Client`` 和 ``httpx.AsyncClient`` 使用。"""

    def __init__(self, data: bytes, chunk_size: int, cut_after: int | None, release: Any) -> None:
        self._data = data
        self._chunk = chunk_size
        self._cut_after = cut_after
        self._release = release
        self._released = False

    def _limit(self) -> int:
        return len(self._data) if self._cut_after is None else min(self._cut_after, len(self._data))

    def __iter__(self) -> Iterator[bytes]:
        limit = self._limit()
        pos = 0
        while pos < limit:
            yield self._data[pos : min(pos + self._chunk, limit)]
            pos += self._chunk
        if self._cut_after is not None:
            self._do_release()
            raise httpx.ReadError("fake cdn: connection reset")

    async def __aiter__(self) -> AsyncIterator[bytes]:
        limit = self._limit()
        pos = 0
        while pos < limit:
            yield self._data[pos : min(pos + self._chunk, limit)]
            pos += self._chunk
            await asyncio.sleep(0)
        if self._cut_after is not None:
            self._do_release()
            raise httpx.ReadError("fake cdn: connection reset")

    def close(self) -> None:
        self._do_release()

    async def aclose(self) -> None:
        self._do_release()

    def _do_release(self) -> None:
        if not self._released:
            self._released = True
            self._release()


class FakeCdn:
    def __init__(
        self,
        clock: Clock,
        *,
        max_connections: int = 2,
        link_ttl: float = 900.0,
        chunk_size: int = 64 * 1024,
    ) -> None:
        self._clock = clock
        self.max_connections = max_connections
        self.link_ttl = link_ttl
        self.chunk_size = chunk_size
        self._files: dict[str, bytes] = {}
        self._links: dict[str, _LinkInfo] = {}
        self._active: dict[str, int] = {}
        self._faults: dict[str, list[CdnFault]] = {}
        self._tokens = itertools.count(1)
        self.requests: list[CdnRequest] = []
        self._lock = threading.Lock()  # 同步路径在读线程里被调用

    # -- 配置 --

    def add_file(self, pickcode: str, content: bytes) -> None:
        self._files[pickcode] = content

    def queue_fault(self, pickcode: str, *faults: CdnFault) -> None:
        self._faults.setdefault(pickcode, []).extend(faults)

    def expire_all_links(self) -> None:
        for info in self._links.values():
            info.expires_at = -1.0

    def active_connections(self, pickcode: str) -> int:
        return self._active.get(pickcode, 0)

    def issue_link(self, pickcode: str) -> DownloadLink:
        n = next(self._tokens)
        token = f"tok{n}"
        ua = f"FakeCloud-UA/{n}"
        self._links[token] = _LinkInfo(pickcode, ua, self._clock.monotonic() + self.link_ttl)
        url = f"https://{CDN_HOST}/dl/{quote(pickcode)}?token={token}"
        return DownloadLink(url=url, headers={"user-agent": ua}, size=len(self._files[pickcode]))

    @property
    def transport(self) -> httpx.MockTransport:
        """给 ``httpx.AsyncClient`` 用。"""
        return httpx.MockTransport(self.handler)

    @property
    def sync_transport(self) -> httpx.MockTransport:
        """给 ``httpx.Client`` 用（取回的读线程用同步客户端）。"""
        return httpx.MockTransport(self.sync_handler)

    # -- 处理器 --

    async def handler(self, request: httpx.Request) -> httpx.Response:
        return self.sync_handler(request)

    def sync_handler(self, request: httpx.Request) -> httpx.Response:
        with self._lock:
            return self._handle(request)

    def _handle(self, request: httpx.Request) -> httpx.Response:
        parts = request.url.path.split("/")
        pickcode = parts[2] if len(parts) >= 3 and parts[1] == "dl" else ""
        token = request.url.params.get("token")
        ua = request.headers.get("user-agent")
        rng = request.headers.get("range")
        status, response = self._respond(pickcode, token, ua, rng)
        self.requests.append(CdnRequest(pickcode, rng, status, token, ua))
        return response

    def _respond(
        self, pickcode: str, token: str | None, ua: str | None, rng: str | None
    ) -> tuple[int, httpx.Response]:
        def deny(code: int) -> tuple[int, httpx.Response]:
            return code, httpx.Response(code)

        info = self._links.get(token) if token else None
        if (
            info is None
            or info.pickcode != pickcode
            or pickcode not in self._files
            or info.expires_at < self._clock.monotonic()
            or ua != info.user_agent
        ):
            return deny(403)
        if self._active.get(pickcode, 0) >= self.max_connections:
            return deny(403)

        queue = self._faults.get(pickcode)
        fault = queue.pop(0) if queue else None
        if fault is not None and fault.kind == "status":
            return deny(fault.status)

        data = self._files[pickcode]
        total = len(data)
        headers: dict[str, str] = {"accept-ranges": "bytes"}
        code = 200
        body = data
        if rng is not None and not (fault is not None and fault.kind == "full_200"):
            m = re.fullmatch(r"bytes=(\d+)-(\d*)", rng.strip())
            if m is None:
                return deny(416)
            start = int(m.group(1))
            end = int(m.group(2)) if m.group(2) else total - 1
            end = min(end, total - 1)
            if start >= total or start > end:
                return 416, httpx.Response(416, headers={"content-range": f"bytes */{total}"})
            code = 206
            body = data[start : end + 1]
            shown_end = end
            if fault is not None and fault.kind == "bad_range":
                shown_end = end + fault.delta
            headers["content-range"] = f"bytes {start}-{shown_end}/{total}"
        length = len(body)
        if fault is not None and fault.kind == "bad_length":
            length += fault.delta
        headers["content-length"] = str(length)

        cut_after = fault.after_bytes if fault is not None and fault.kind == "cut" else None
        self._active[pickcode] = self._active.get(pickcode, 0) + 1

        def release() -> None:
            with self._lock:
                self._active[pickcode] -= 1

        stream = _Stream(body, self.chunk_size, cut_after, release)
        return code, httpx.Response(code, headers=headers, stream=stream)


# ---------------------------------------------------------------- FakeCloud


class FakeCloud:
    """实现 ``CloudClient``。没有任何删除方法。"""

    def __init__(
        self,
        clock: Clock,
        *,
        quota_total: int = 1570,
        quota_remaining: int = 1250,
        page_size: int = 30,
        sha1_uppercase: bool = True,
        cdn: FakeCdn | None = None,
        space_total: int = 2 * 1024**4,
        space_remaining: int = 1536 * 1024**3,
    ) -> None:
        self.clock = clock
        self.cdn = cdn or FakeCdn(clock)
        self.quota_total = quota_total
        self.quota_remaining = quota_remaining
        self.space_total = space_total
        self.space_remaining = space_remaining
        self.page_size = page_size
        self.sha1_uppercase = sha1_uppercase

        self.calls: list[Call] = []
        self._counts: dict[str, int] = {}
        self._faults: list[_Fault] = []

        self._ids = itertools.count(1000)
        self._entries: dict[str, CloudEntry] = {}
        self._hidden: set[str] = set()
        self._pickcode_to_file: dict[str, str] = {}
        self._scenarios: dict[str, OfflineScenario] = {}
        self._tasks: dict[str, _Task] = {}

        self._qr_script: list[QrStatus] = [QrStatus.WAITING, QrStatus.SCANNED, QrStatus.CONFIRMED]
        self._qr_polls = 0
        self.logged_in = False

    # ------------------------------------------------ 测试配置

    def script(self, info_hash: str, scenario: OfflineScenario) -> None:
        """为某个 hash 登记场景。未登记的 hash 在提交时使用默认场景。"""
        self._scenarios[info_hash.lower()] = scenario

    def inject(
        self, method: str, exc: CloudError, *, on_call: int = 1, times: int | None = 1
    ) -> None:
        """第 ``on_call`` 次（从 1 起，含本次）调用 ``method`` 起抛 ``exc``，共 ``times`` 次。

        ``times=None`` 表示此后每次调用都抛。
        """
        self._faults.append(_Fault(method, exc, on_call, times))

    def clear_faults(self) -> None:
        self._faults.clear()

    def set_qr_script(self, statuses: list[QrStatus]) -> None:
        self._qr_script = list(statuses)
        self._qr_polls = 0

    def set_task_state(
        self, info_hash: str, *, status: int, percent_done: float, with_file_id: bool = False
    ) -> None:
        """手动固定任务状态，不再随时钟推进。可构造过渡态。"""
        task = self._tasks[info_hash.lower()]
        task.override = _Override(status, percent_done, with_file_id)

    def complete(self, info_hash: str) -> None:
        self.set_task_state(
            info_hash, status=OFFLINE_DONE_STATUS, percent_done=100.0, with_file_id=True
        )

    def fail(self, info_hash: str, status: int = -1) -> None:
        task = self._tasks[info_hash.lower()]
        self.set_task_state(info_hash, status=status, percent_done=task.scenario.stall_percent)

    def preload_tree(self, info_hash: str, scenario: OfflineScenario, wp_path_id: str) -> str:
        """直接在云端放好一棵任务目录树（模拟此前已离线完成），返回根目录 id。"""
        self._scenarios[info_hash.lower()] = scenario
        return self._build_tree(scenario, wp_path_id)

    def hide_entry(self, entry_id: str) -> None:
        """测试用：让某条目对 ``get_entry`` 不可见（模拟云端文件已不在）。"""
        self._hidden.add(entry_id)

    def calls_to(self, method: str) -> list[Call]:
        return [c for c in self.calls if c.method == method]

    def call_count(self, method: str) -> int:
        return len(self.calls_to(method))

    def was_called(self, method: str) -> bool:
        return self.call_count(method) > 0

    def task_root_id(self, info_hash: str) -> str | None:
        return self._tasks[info_hash.lower()].root_id

    def entries_by_name(self, name: str) -> list[CloudEntry]:
        return [e for e in self._entries.values() if e.name == name]

    # ------------------------------------------------ 内部

    def _enter(self, method: str, *args: Any, **kwargs: Any) -> None:
        self.calls.append(Call(method, args, kwargs))
        n = self._counts.get(method, 0) + 1
        self._counts[method] = n
        for f in self._faults:
            if f.method == method and n >= f.on_call and (f.times is None or f.times > 0):
                if f.times is not None:
                    f.times -= 1
                raise f.exc

    def _new_id(self) -> str:
        return str(next(self._ids))

    def _find_child(self, parent_id: str, name: str, *, is_dir: bool) -> CloudEntry | None:
        for e in self._entries.values():
            if e.parent_id == parent_id and e.name == name and e.is_dir == is_dir:
                return e
        return None

    def _ensure_dir(self, parent_id: str, name: str) -> str:
        found = self._find_child(parent_id, name, is_dir=True)
        if found is not None:
            return found.id
        eid = self._new_id()
        self._entries[eid] = CloudEntry(eid, parent_id, name, True, 0, None, None)
        return eid

    def _build_tree(self, scenario: OfflineScenario, wp_path_id: str) -> str:
        self._check_save_dir(wp_path_id)
        if scenario.single_file_at_root:
            if len(scenario.files) != 1:
                raise ValueError("single_file_at_root requires exactly one file")
            root = wp_path_id
        else:
            root = self._ensure_dir(wp_path_id, scenario.name)
        for f in scenario.files:
            *dirs, base = f.path.split("/")
            parent = root
            if scenario.single_file_at_root:
                dirs, base = [], scenario.name
            for d in dirs:
                parent = self._ensure_dir(parent, d)
            fid = self._new_id()
            pickcode = f"pc{fid}"
            sha1 = hashlib.sha1(f.content).hexdigest()
            sha1 = sha1.upper() if self.sha1_uppercase else sha1
            name = f.cloud_name if f.cloud_name is not None else base
            if scenario.single_file_at_root:
                name = scenario.name
            self._entries[fid] = CloudEntry(
                fid, parent, name, False, len(f.content), sha1, pickcode
            )
            self._pickcode_to_file[pickcode] = fid
            self.cdn.add_file(pickcode, f.content)
        return root

    def _check_save_dir(self, dir_id: str) -> None:
        if dir_id == ROOT_ID:
            return
        e = self._entries.get(dir_id)
        if e is None or not e.is_dir or dir_id in self._hidden:
            raise CloudNotFound(f"unknown directory {dir_id}")

    def _default_scenario(self, info_hash: str) -> OfflineScenario:
        return OfflineScenario(
            name=f"task-{info_hash[:8]}",
            files=[FakeFile("video.mkv", make_content(info_hash, 4096))],
        )

    def _view(self, task: _Task) -> OfflineTask:
        sc = task.scenario
        done_id = task.root_id
        if task.override is not None:
            o = task.override
            return self._task(task, o.status, o.percent_done, done_id if o.with_file_id else None)
        t = self.clock.monotonic() - task.added_at
        if t < sc.ramp_seconds:
            pct = sc.stall_percent * t / sc.ramp_seconds if sc.ramp_seconds > 0 else 0.0
            return self._task(task, 1, pct, None)
        if t < sc.ramp_seconds + sc.stall_seconds or sc.never_finishes:
            return self._task(task, 1, sc.stall_percent, None)
        if sc.outcome != OFFLINE_DONE_STATUS:
            return self._task(task, sc.outcome, sc.stall_percent, None)
        if t < sc.ramp_seconds + sc.stall_seconds + sc.transient_seconds:
            status = 1 if sc.transient_kind == "percent100" else OFFLINE_DONE_STATUS
            return self._task(task, status, 100.0, None)
        return self._task(task, OFFLINE_DONE_STATUS, 100.0, done_id)

    def _task(self, task: _Task, status: int, pct: float, file_id: str | None) -> OfflineTask:
        sc = task.scenario
        return OfflineTask(
            hash=task.hash,
            name=sc.name,
            status=status,
            percent_done=pct,
            file_id=file_id,
            wp_path_id=task.wp_path_id,
            del_path=sc.name,
            size=sc.size,
        )

    # ------------------------------------------------ CloudClient

    async def list_offline(self, stat: OfflineStat, page: int = 1) -> OfflinePage:
        self._enter("list_offline", stat, page)
        views = [self._view(t) for t in self._tasks.values()]
        if stat == OfflineStat.RUNNING:
            views = [v for v in views if v.status not in (OFFLINE_DONE_STATUS,) and not v.is_failed]
        elif stat == OfflineStat.DONE:
            views = [v for v in views if v.status == OFFLINE_DONE_STATUS]
        else:
            views = [v for v in views if v.is_failed]
        page_count = max(1, math.ceil(len(views) / self.page_size))
        start = (page - 1) * self.page_size
        return OfflinePage(
            tasks=views[start : start + self.page_size],
            quota=Quota(self.quota_remaining, self.quota_total),
            page=page,
            page_count=page_count,
        )

    async def get_space(self) -> CloudSpace:
        self._enter("get_space")
        return CloudSpace(total=self.space_total, remaining=self.space_remaining)

    async def add_magnet(self, magnet: str, wp_path_id: str) -> str:
        self._enter("add_magnet", magnet, wp_path_id)
        info_hash = parse_btih(magnet)
        if info_hash in self._tasks:
            return info_hash
        self._check_save_dir(wp_path_id)
        scenario = self._scenarios.get(info_hash) or self._default_scenario(info_hash)
        if scenario.reject is not None:
            raise SubmitRejected(*scenario.reject)
        if self.quota_remaining <= 0:
            raise QuotaExhausted("offline quota exhausted")
        root_id = self._build_tree(scenario, wp_path_id) if scenario.outcome == 2 else None
        self.quota_remaining -= 1
        self._tasks[info_hash] = _Task(
            info_hash, scenario, self.clock.monotonic(), wp_path_id, root_id
        )
        return info_hash

    async def makedirs(self, path: str) -> str:
        self._enter("makedirs", path)
        parent = ROOT_ID
        for name in [p for p in path.split("/") if p]:
            parent = self._ensure_dir(parent, name)
        return parent

    async def get_entry(self, entry_id: str) -> CloudEntry | None:
        self._enter("get_entry", entry_id)
        if entry_id in self._hidden:
            return None
        return self._entries.get(entry_id)

    async def list_dir(self, dir_id: str) -> list[CloudEntry]:
        self._enter("list_dir", dir_id)
        if dir_id in self._hidden or (dir_id != ROOT_ID and dir_id not in self._entries):
            raise CloudNotFound(f"directory {dir_id} not found")
        if dir_id != ROOT_ID and not self._entries[dir_id].is_dir:
            # 与真实会话层核对 cid 的行为一致：传入文件 id 视为目录不存在
            raise CloudNotFound(f"{dir_id} is a file, not a directory")
        return [
            e for e in self._entries.values() if e.parent_id == dir_id and e.id not in self._hidden
        ]

    async def download_link(self, pickcode: str) -> DownloadLink:
        self._enter("download_link", pickcode)
        fid = self._pickcode_to_file.get(pickcode)
        if fid is None or fid in self._hidden:
            raise CloudNotFound(f"unknown pickcode {pickcode}")
        return self.cdn.issue_link(pickcode)

    async def qr_start(self) -> QrToken:
        self._enter("qr_start")
        self._qr_polls = 0
        return QrToken(uid="fake-uid", image_png=b"\x89PNG\r\n\x1a\nfake")

    async def qr_poll(self, uid: str) -> QrStatus:
        self._enter("qr_poll", uid)
        idx = min(self._qr_polls, len(self._qr_script) - 1)
        self._qr_polls += 1
        return self._qr_script[idx]

    async def qr_finish(self, uid: str) -> None:
        self._enter("qr_finish", uid)
        self.logged_in = True
