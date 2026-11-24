"""qBittorrent 5.x WebAPI 兼容层（T07）。

只实现 Radarr/Sonarr 实际调用的接口子集，见设计文档「qBittorrent WebAPI 兼容层」。

- 不 import p115client，不依赖 CloudClient：本模块永不调用 115（不变量 2）。
- `torrents/add` 只解析并入队；`torrents/delete` 只通过 `LocalCleaner` 清理本地。
- 速度、ETA、等待空间由取回模块通过 `ProgressSource` 提供；删除动作由流水线通过
  `LocalCleaner` 执行。
- 不报 `stalledDL`；`stoppedUP` 只给已通过校验的 completed 任务（不变量 10）。
- hash 统一小写 hex（不变量 11）。密码与 SID 不进日志（不变量 6）。
"""

from __future__ import annotations

import contextlib
import email
import email.policy
import hmac
import logging
import secrets as _secrets
from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Protocol, runtime_checkable
from urllib.parse import parse_qsl

from fastapi import APIRouter, Depends, FastAPI, HTTPException, Request
from fastapi.responses import JSONResponse, PlainTextResponse, Response

from bridge.clock import Clock
from bridge.config import Secrets, Settings
from bridge.db.repo import (
    ACTIVE_STATES,
    InvalidTransition,
    ManifestFile,
    Source,
    StateMismatch,
    Task,
    TaskNotFound,
    TaskRepo,
    TaskState,
)
from bridge.layout import content_path, is_single_file, save_path
from bridge.log import log_event
from bridge.markers import CLEANUP_ERROR_PREFIX
from bridge.missing_protocol import MissingFilesView, completion_id
from bridge.progress_protocol import (
    ETA_INFINITE,
    FetchProgress,
    ProgressSource,
    ZeroProgressSource,
)
from bridge.torrent import (
    TorrentError,
    build_magnet,
    magnet_display_name,
    parse_magnet,
    parse_torrent,
)

logger = logging.getLogger(__name__)

APP_VERSION = "v5.0.0"
WEBAPI_VERSION = "2.11.2"
SID_COOKIE = "SID"
SESSION_TTL_SECONDS = 3600.0
DELETE_MAX_RETRIES = 3  # torrents/delete 遇到 StateMismatch 的最多重试次数


# ---------------------------------------------------------------- 外部协议


@runtime_checkable
class LocalCleaner(Protocol):
    """本地清理，由流水线实现。本模块只调用，不直接删文件，更不碰云端。"""

    async def cancel_and_cleanup_incomplete(self, info_hash: str) -> None:
        """停止跟踪与取回，清理 `.incomplete/<hash>`。"""
        ...

    async def remove_completed(self, task: Task) -> None:
        """删除本地 `<category>` 下已完成的那份（媒体库硬链接不受影响）。"""
        ...


# ---------------------------------------------------------------- 状态与路径


def qbit_state(task: Task, progress: FetchProgress) -> str:
    """TaskState 到 qBittorrent state。永远不返回 stalledDL。"""
    s = task.state
    if s in (TaskState.QUEUED, TaskState.SUBMITTING, TaskState.CLOUD_METADATA):
        return "metaDL"
    if s in (TaskState.CLOUD_DOWNLOADING, TaskState.CLOUD_DONE, TaskState.LOCATING):
        return "queuedDL"
    if s is TaskState.FETCHING:
        return "queuedDL" if progress.waiting_for_space else "downloading"
    if s is TaskState.VERIFYING:
        return "checkingDL"
    if s is TaskState.COMPLETED:
        return "stoppedUP"
    if s is TaskState.FAILED:
        return "error"
    # REMOVED 不应出现在列表中；防御性处理
    return "error"


def _epoch(dt: datetime) -> int:
    return int(dt.timestamp())


def _total_size(task: Task, manifest: list[ManifestFile]) -> int:
    if task.cloud_size:
        return task.cloud_size
    return sum(f.size for f in manifest)


def _progress_value(task: Task, total: int) -> float:
    s = task.state
    if s is TaskState.COMPLETED:
        return 1.0
    if s in (TaskState.CLOUD_DOWNLOADING, TaskState.CLOUD_DONE, TaskState.LOCATING):
        return max(0.0, min(1.0, task.cloud_percent / 100.0))
    if s in (TaskState.FETCHING, TaskState.VERIFYING, TaskState.FAILED):
        if total <= 0:
            return 0.0
        return max(0.0, min(1.0, task.local_bytes / total))
    return 0.0


# ---------------------------------------------------------------- 请求解析


@dataclass(slots=True)
class _Params:
    fields: dict[str, str]
    files: list[tuple[str, bytes]]  # (字段名, 内容)

    def get(self, key: str, default: str = "") -> str:
        return self.fields.get(key, default)


def _parse_multipart(content_type: str, body: bytes) -> _Params:
    raw = b"Content-Type: " + content_type.encode("latin-1") + b"\r\n\r\n" + body
    msg = email.message_from_bytes(raw, policy=email.policy.HTTP)
    fields: dict[str, str] = {}
    files: list[tuple[str, bytes]] = []
    if not msg.is_multipart():
        return _Params(fields, files)
    for part in msg.iter_parts():
        name = part.get_param("name", header="content-disposition")
        if not isinstance(name, str):
            continue
        payload = part.get_payload(decode=True)
        if not isinstance(payload, bytes):
            continue
        if part.get_filename() is not None:
            files.append((name, payload))
        else:
            fields[name] = payload.decode("utf-8", errors="replace")
    return _Params(fields, files)


async def _read_params(request: Request) -> _Params:
    """合并 query 与 body（urlencoded 或 multipart）；body 优先。"""
    fields: dict[str, str] = dict(request.query_params)
    files: list[tuple[str, bytes]] = []
    body = await request.body()
    ctype = request.headers.get("content-type", "")
    main = ctype.split(";", 1)[0].strip().lower()
    if body and main == "application/x-www-form-urlencoded":
        fields.update(parse_qsl(body.decode("utf-8", errors="replace"), keep_blank_values=True))
    elif body and main == "multipart/form-data":
        parsed = _parse_multipart(ctype, body)
        fields.update(parsed.fields)
        files = parsed.files
    return _Params(fields, files)


def _split_hashes(value: str) -> list[str] | None:
    """`hashes` 参数：`|` 分隔；`all` 返回 None 表示全部。"""
    v = value.strip()
    if v.lower() == "all":
        return None
    return [h.strip().lower() for h in v.split("|") if h.strip()]


# ---------------------------------------------------------------- 路由工厂


def make_require_auth(sessions: dict[str, float], clock: Clock) -> Callable[[Request], None]:
    """SID cookie 认证依赖。Web 页面（T15）与 qBit 路由共用同一份 `sessions`。"""

    def require_auth(request: Request) -> None:
        sid = request.cookies.get(SID_COOKIE)
        now = clock.monotonic()
        if sid is not None:
            expiry = sessions.get(sid)
            if expiry is not None:
                if expiry > now:
                    sessions[sid] = now + SESSION_TTL_SECONDS
                    return
                sessions.pop(sid, None)
        raise HTTPException(status_code=403, detail="Forbidden")

    return require_auth


def create_qbit_router(
    *,
    repo: TaskRepo,
    settings: Settings,
    secrets: Secrets,
    clock: Clock,
    cleaner: LocalCleaner,
    progress_source: ProgressSource | None = None,
    sessions: dict[str, float] | None = None,
    missing_files: MissingFilesView | None = None,
) -> APIRouter:
    progress_src: ProgressSource = progress_source or ZeroProgressSource()
    router = APIRouter(prefix="/api/v2")
    if sessions is None:
        sessions = {}  # SID -> 过期的 monotonic 时刻（滑动）
    username_b = (settings.webui.username or "").encode("utf-8")
    password_b = secrets.webui_password.get_secret_value().encode("utf-8")

    # -- 认证 --

    require_auth = make_require_auth(sessions, clock)

    auth = [Depends(require_auth)]

    def ok() -> Response:
        return PlainTextResponse("Ok.")

    @router.post("/auth/login")
    async def login(request: Request) -> Response:
        params = await _read_params(request)
        u = params.get("username").encode("utf-8")
        p = params.get("password").encode("utf-8")
        # 两项都比较，避免按短路时间泄露哪一项错了
        user_ok = hmac.compare_digest(u, username_b)
        pass_ok = hmac.compare_digest(p, password_b)
        if not (user_ok and pass_ok):
            log_event(logger, logging.WARNING, "qbit login failed", stage="qbit")
            return PlainTextResponse("Fails.")
        sid = _secrets.token_urlsafe(24)
        sessions[sid] = clock.monotonic() + SESSION_TTL_SECONDS
        resp = PlainTextResponse("Ok.")
        resp.set_cookie(SID_COOKIE, sid, httponly=True, samesite="strict", path="/")
        log_event(logger, logging.INFO, "qbit login ok", stage="qbit")
        return resp

    @router.post("/auth/logout", dependencies=auth)
    async def logout(request: Request) -> Response:
        sid = request.cookies.get(SID_COOKIE)
        if sid is not None:
            sessions.pop(sid, None)
        resp = PlainTextResponse("Ok.")
        resp.delete_cookie(SID_COOKIE, path="/")
        return resp

    # -- app --

    @router.get("/app/version", dependencies=auth)
    @router.post("/app/version", dependencies=auth)
    async def app_version() -> Response:
        return PlainTextResponse(APP_VERSION)

    @router.get("/app/webapiVersion", dependencies=auth)
    @router.post("/app/webapiVersion", dependencies=auth)
    async def app_webapi_version() -> Response:
        return PlainTextResponse(WEBAPI_VERSION)

    @router.get("/app/preferences", dependencies=auth)
    @router.post("/app/preferences", dependencies=auth)
    async def app_preferences() -> Response:
        return JSONResponse(
            {
                "save_path": settings.paths.root,
                "temp_path_enabled": False,
                "temp_path": settings.paths.incomplete,
                "auto_tmm_enabled": False,
                "start_paused_enabled": False,
                "queueing_enabled": False,
                "torrent_content_layout": "Original",
                "web_ui_port": settings.webui.port,
                # Remove Completed 生效所需（设计文档「让 Remove Completed 生效」）
                "dht": True,
                "max_ratio_enabled": True,
                "max_ratio": 0,
                "max_ratio_act": 0,  # 暂停；报 1 或 3 会让 Arr 的「测试」失败
                "max_seeding_time_enabled": False,
                "max_seeding_time": -1,
                "max_inactive_seeding_time_enabled": False,
                "max_inactive_seeding_time": -1,
            }
        )

    # -- torrents 视图 --

    async def _visible_tasks() -> list[Task]:
        tasks = await repo.list_tasks(include_client_deleted=False)
        return [t for t in tasks if t.state is not TaskState.REMOVED]

    async def _visible_task(info_hash: str) -> Task | None:
        t = await repo.get_task(info_hash.strip().lower())
        if t is None or t.state is TaskState.REMOVED or t.client_deleted:
            return None
        return t

    async def _completion_on(task: Task) -> int:
        if task.state is not TaskState.COMPLETED:
            return -1
        for ev in await repo.list_events(task.hash, limit=20):
            if ev.to_state is TaskState.COMPLETED:
                return _epoch(ev.at)
        return -1

    async def _info_entry(task: Task) -> dict[str, Any]:
        manifest = await repo.get_manifest(task.hash)
        prog = progress_src.progress(task.hash)
        state = qbit_state(task, prog)
        if (
            missing_files is not None
            and task.state is TaskState.COMPLETED
            and missing_files.might_be_missing(task.hash)
            and missing_files.is_missing(task.hash, await completion_id(repo, task.hash))
        ):
            state = "missingFiles"
        total = _total_size(task, manifest)
        p = _progress_value(task, total)
        completed = total if task.state is TaskState.COMPLETED else int(total * p)
        downloading = state == "downloading"
        content = content_path(settings.paths.root, task, manifest)
        save = save_path(settings.paths.root, task.category)
        added = _epoch(task.added_at)
        done = task.state is TaskState.COMPLETED
        return {
            "added_on": added,
            "amount_left": max(total - completed, 0),
            "auto_tmm": False,
            "availability": -1,
            "category": task.category,
            "comment": "",
            "completed": completed,
            "completion_on": await _completion_on(task),
            "content_path": content,
            "dl_limit": -1,
            "dlspeed": prog.dlspeed if downloading else 0,
            "download_path": "",
            "downloaded": completed,
            "downloaded_session": completed,
            "eta": prog.eta if downloading else ETA_INFINITE,
            "f_l_piece_prio": False,
            "force_start": False,
            "has_metadata": bool(manifest) or task.cloud_size is not None,
            "hash": task.hash,
            "inactive_seeding_time_limit": -2,
            "infohash_v1": task.hash,
            "infohash_v2": "",
            "last_activity": added,
            "magnet_uri": build_magnet(task.hash, task.name),
            "max_inactive_seeding_time": -2,
            "max_ratio": -1,
            "max_seeding_time": -2,
            "name": task.name,
            "num_complete": 0,
            "num_incomplete": 0,
            "num_leechs": 0,
            "num_seeds": 0,
            "popularity": 0,
            "priority": 0,
            "private": False,
            "progress": p,
            "ratio": 0,
            "ratio_limit": -2,  # 使用全局上限，配合 max_ratio=0 即「已达到做种目标」
            "reannounce": 0,
            "root_path": content,
            "save_path": save,
            "seeding_time": 0,
            "seeding_time_limit": -2,
            "seen_complete": -1 if not done else added,
            "seq_dl": False,
            "size": total,
            "state": state,
            "super_seeding": False,
            "tags": "",
            "time_active": 0,
            "total_size": total,
            "tracker": "",
            "trackers_count": 0,
            "up_limit": -1,
            "uploaded": 0,
            "uploaded_session": 0,
            "upspeed": 0,
        }

    @router.get("/torrents/info", dependencies=auth)
    @router.post("/torrents/info", dependencies=auth)
    async def torrents_info(request: Request) -> Response:
        params = await _read_params(request)
        tasks = await _visible_tasks()
        if "category" in params.fields:
            tasks = [t for t in tasks if t.category == params.fields["category"]]
        if "hashes" in params.fields:
            wanted = _split_hashes(params.fields["hashes"])
            if wanted is not None:
                tasks = [t for t in tasks if t.hash in set(wanted)]
        tasks.sort(key=lambda t: (t.added_at, t.hash))
        offset = _int(params.get("offset"), 0)
        limit = _int(params.get("limit"), 0)
        if offset:
            tasks = tasks[offset:]
        if limit > 0:
            tasks = tasks[:limit]
        return JSONResponse([await _info_entry(t) for t in tasks])

    @router.get("/torrents/properties", dependencies=auth)
    @router.post("/torrents/properties", dependencies=auth)
    async def torrents_properties(request: Request) -> Response:
        params = await _read_params(request)
        task = await _visible_task(params.get("hash"))
        if task is None:
            return PlainTextResponse("Not Found", status_code=404)
        entry = await _info_entry(task)
        added = entry["added_on"]
        return JSONResponse(
            {
                "addition_date": added,
                "comment": "",
                "completion_date": entry["completion_on"],
                "created_by": "",
                "creation_date": -1,
                "dl_limit": -1,
                "dl_speed": entry["dlspeed"],
                "dl_speed_avg": 0,
                "download_path": "",
                "eta": entry["eta"],
                "hash": task.hash,
                "infohash_v1": task.hash,
                "infohash_v2": "",
                "is_private": False,
                "last_seen": added,
                "name": task.name,
                "nb_connections": 0,
                "nb_connections_limit": -1,
                "peers": 0,
                "peers_total": 0,
                "piece_size": 0,
                "pieces_have": 0,
                "pieces_num": 0,
                "reannounce": 0,
                "save_path": entry["save_path"],
                "seeding_time": 0,
                "seeds": 0,
                "seeds_total": 0,
                "share_ratio": 0,
                "time_elapsed": 0,
                "total_downloaded": entry["downloaded"],
                "total_downloaded_session": entry["downloaded_session"],
                "total_size": entry["total_size"],
                "total_uploaded": 0,
                "total_uploaded_session": 0,
                "total_wasted": 0,
                "up_limit": -1,
                "up_speed": 0,
                "up_speed_avg": 0,
            }
        )

    @router.get("/torrents/files", dependencies=auth)
    @router.post("/torrents/files", dependencies=auth)
    async def torrents_files(request: Request) -> Response:
        params = await _read_params(request)
        task = await _visible_task(params.get("hash"))
        if task is None:
            return PlainTextResponse("Not Found", status_code=404)
        manifest = await repo.get_manifest(task.hash)
        single = is_single_file(task, manifest)
        task_files = await repo.list_task_files(task.hash)
        # 有 task_files 时用 115 实际名字（即本地落盘的名字），否则用种子清单；
        # 单文件任务落盘的名字恒为 task.name（layout.content_path），不用 115 改过的名字
        rows: list[tuple[str, int, int]] = (
            [(tf.rel_path, tf.size, tf.local_bytes) for tf in task_files]
            if task_files
            else [(mf.rel_path, mf.size, 0) for mf in manifest]
        )
        if single:
            rows = [(task.name, size, done) for _, size, done in rows[:1]]
        out: list[dict[str, Any]] = []
        for i, (rel_path, size, local_bytes) in enumerate(rows):
            if task.state is TaskState.COMPLETED:
                prog = 1.0
            else:
                prog = min(local_bytes / size, 1.0) if size > 0 else 0.0
            out.append(
                {
                    "index": i,
                    "name": rel_path if single else f"{task.name}/{rel_path}",
                    "size": size,
                    "progress": prog,
                    "priority": 1,
                    "is_seed": task.state is TaskState.COMPLETED,
                    "piece_range": [0, 0],
                    "availability": -1,
                }
            )
        return JSONResponse(out)

    # -- add --

    async def _enqueue(
        category: str,
        source: Source,
        info_hash: str,
        name: str,
        files: list[ManifestFile] | None,
        magnet: str,
    ) -> bool:
        """入队。返回该 hash 是否算添加成功（failed 的 hash 视为失败）。"""
        task, created = await repo.create_task(
            info_hash, category, name, source, clock.now(), magnet
        )
        if not created:
            if task.state is TaskState.FAILED:
                log_event(
                    logger, logging.INFO, "qbit add rejected, task failed",
                    hash=info_hash, stage="qbit",
                )  # fmt: skip
                return False
            if task.state is TaskState.REMOVED:
                try:
                    task = await repo.requeue_removed(info_hash, "torrents/add", clock.now())
                except InvalidTransition:
                    return False
                log_event(logger, logging.INFO, "qbit add requeued", hash=info_hash, stage="qbit")
        if files is not None and (
            created or (task.state in ACTIVE_STATES and not await repo.get_manifest(info_hash))
        ):
            await repo.put_manifest(info_hash, files)
        log_event(
            logger,
            logging.INFO,
            "qbit add accepted" if created else "qbit add already known",
            hash=info_hash,
            stage="qbit",
            state=task.state.value,
        )
        return True

    @router.post("/torrents/add", dependencies=auth)
    async def torrents_add(request: Request) -> Response:
        params = await _read_params(request)
        category = params.get("category")
        if category not in settings.categories:
            return PlainTextResponse("Fails.")
        accepted = 0
        for line in params.get("urls").replace("\r", "\n").split("\n"):
            url = line.strip()
            if not url:
                continue
            try:
                mg = parse_magnet(url)
            except TorrentError:
                continue
            if await _enqueue(
                category, Source.MAGNET, mg.info_hash, magnet_display_name(mg), None, url
            ):
                accepted += 1
        for field_name, data in params.files:
            if field_name != "torrents":
                continue
            try:
                info = parse_torrent(data)
            except TorrentError:
                continue
            if await _enqueue(
                category,
                Source.TORRENT,
                info.info_hash,
                info.name,
                list(info.files),
                info.to_magnet(),
            ):
                accepted += 1
        return PlainTextResponse("Ok." if accepted else "Fails.")

    # -- delete --

    async def _cleanup(task: Task, was: TaskState) -> None:
        """按转换前的状态清理本地；绝不碰云端。"""
        if was is TaskState.COMPLETED:
            await cleaner.remove_completed(task)
        else:
            await cleaner.cancel_and_cleanup_incomplete(task.hash)

    async def _removed_from(info_hash: str) -> TaskState:
        for ev in await repo.list_events(info_hash, limit=20):
            if ev.to_state is TaskState.REMOVED and ev.from_state is not None:
                return ev.from_state
        return TaskState.FETCHING  # 查不到来源：按未完成处理（只清 .incomplete）

    async def _delete_one(info_hash: str) -> bool:
        """返回是否成功；失败（清理出错或并发冲突不止）返回 False。"""
        for _ in range(1 + DELETE_MAX_RETRIES):
            task = await repo.get_task(info_hash)
            if task is None:
                return True
            was = task.state
            try:
                if was is TaskState.REMOVED or (was is TaskState.FAILED and task.client_deleted):
                    # 已处理过；仅当上次本地清理出错（带标记）时再试一次清理
                    if task.error is None or not task.error.startswith(CLEANUP_ERROR_PREFIX):
                        return True
                    was = await _removed_from(info_hash) if was is TaskState.REMOVED else was
                elif was is TaskState.FAILED:
                    # 失败任务不转换，只标记 Arr 已删除
                    await repo.mark_client_deleted(info_hash)
                else:
                    await repo.transition(
                        info_hash, TaskState.REMOVED, "torrents/delete", clock.now(), expected=was
                    )
            except StateMismatch:
                continue  # 流水线抢先转换了：按最新状态重来
            except InvalidTransition:
                continue
            except TaskNotFound:
                return True
            try:
                await _cleanup(task, was)
            except Exception as exc:
                # 状态已转换，保持不变；记录错误，由 Arr 重试时再清理
                log_event(
                    logger, logging.ERROR, "qbit delete cleanup failed",
                    hash=info_hash, stage="qbit", from_state=was.value, error=type(exc).__name__,
                )  # fmt: skip
                with contextlib.suppress(TaskNotFound):
                    await repo.set_error(
                        info_hash, f"{CLEANUP_ERROR_PREFIX}: {type(exc).__name__}: {exc}"[:500]
                    )
                return False
            if task.error is not None and task.error.startswith(CLEANUP_ERROR_PREFIX):
                await repo.set_error(info_hash, None)
            log_event(
                logger, logging.INFO, "qbit delete handled", hash=info_hash, stage="qbit",
                from_state=was.value,
            )  # fmt: skip
            return True
        log_event(logger, logging.ERROR, "qbit delete state conflict", hash=info_hash, stage="qbit")
        return False

    @router.post("/torrents/delete", dependencies=auth)
    async def torrents_delete(request: Request) -> Response:
        # deleteFiles 无论取值都不影响行为：只清本地，不碰云端
        params = await _read_params(request)
        wanted = _split_hashes(params.get("hashes"))
        if wanted is None:
            wanted = [t.hash for t in await _visible_tasks()]
        failed = 0
        for h in wanted:  # 某个 hash 出错不阻止其他 hash
            if not await _delete_one(h):
                failed += 1
        if failed:
            return PlainTextResponse("Internal Server Error", status_code=500)
        return ok()

    # -- category --

    @router.get("/torrents/categories", dependencies=auth)
    @router.post("/torrents/categories", dependencies=auth)
    async def torrents_categories() -> Response:
        return JSONResponse(
            {
                c: {"name": c, "savePath": save_path(settings.paths.root, c)}
                for c in settings.categories
            }
        )

    async def _accept() -> Response:
        return ok()

    # category 集合由配置固定：以下两个接口接受但不改变行为
    router.add_api_route(
        "/torrents/createCategory", _accept, methods=["GET", "POST"], dependencies=auth
    )
    router.add_api_route(
        "/torrents/setCategory", _accept, methods=["GET", "POST"], dependencies=auth
    )
    for name in ("stop", "start", "setShareLimits", "topPrio", "setForceStart", "addTags"):
        router.add_api_route(
            f"/torrents/{name}", _accept, methods=["GET", "POST"], dependencies=auth
        )

    return router


def _int(value: str, default: int) -> int:
    try:
        return int(value)
    except ValueError:
        return default


def create_qbit_app(
    *,
    repo: TaskRepo,
    settings: Settings,
    secrets: Secrets,
    clock: Clock,
    cleaner: LocalCleaner,
    progress_source: ProgressSource | None = None,
    missing_files: MissingFilesView | None = None,
) -> FastAPI:
    app = FastAPI(title="115 Bridge qBit API", docs_url=None, redoc_url=None, openapi_url=None)
    app.include_router(
        create_qbit_router(
            repo=repo,
            settings=settings,
            secrets=secrets,
            clock=clock,
            cleaner=cleaner,
            progress_source=progress_source,
            missing_files=missing_files,
        )
    )
    return app


__all__ = [
    "APP_VERSION",
    "ETA_INFINITE",
    "WEBAPI_VERSION",
    "FetchProgress",
    "LocalCleaner",
    "MissingFilesView",
    "ProgressSource",
    "ZeroProgressSource",
    "create_qbit_app",
    "create_qbit_router",
    "make_require_auth",
    "qbit_state",
]
