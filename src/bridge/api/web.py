"""Web 页面与扫码（T15）。

与 qBit API 同端口、同一份 SID 认证（共用 `sessions`）。只读展示会话、任务、配额、空间
与任务历史（T19），唯一的写操作是「重新生成二维码」，它作用于会话而非任务；
没有任何修改任务的路由。

- 不 import p115client：会话层通过窄接口 `SessionView` 注入（不变量 1）。
- 不展示、不返回 cookie、密码、Arr Key、Telegram token（不变量 6）；`SessionStatus` 只含
  `cookie_present` 布尔值。
- 所有动态内容做 HTML 转义。
- 页面模板与前端交互在 `web_ui.py`（纯呈现模块）：服务端渲染首屏快照，
  JS 每 `web_ui.POLL_SECONDS` 秒轮询 `/web/status.json` 原地更新，不整页刷新。
"""

from __future__ import annotations

import logging
import shutil
from collections.abc import Callable
from datetime import datetime, timedelta
from typing import TYPE_CHECKING, Any, Protocol

from fastapi import APIRouter, Depends, FastAPI, Request
from fastapi.responses import HTMLResponse, JSONResponse, PlainTextResponse, Response

from bridge.api import web_ui
from bridge.api.qbit import (
    LocalCleaner,
    MissingFilesView,
    create_qbit_router,
    make_require_auth,
)
from bridge.clock import Clock
from bridge.cloud.protocol import QrToken, Quota
from bridge.config import Secrets, Settings
from bridge.db.repo import EVENTS_PER_TASK, Event, Task, TaskRepo, TaskState
from bridge.kv_keys import KV_QUOTA, decode_quota
from bridge.layout import content_path
from bridge.log import log_event
from bridge.missing_protocol import completion_id
from bridge.progress_protocol import ProgressSource, ZeroProgressSource

if TYPE_CHECKING:  # 只用于类型标注，运行时不碰 session 模块（它 import p115client）
    from bridge.cloud.session import SessionStatus

logger = logging.getLogger(__name__)

MISSING_FILES_TEXT = "本地文件缺失"  # 与 qBit 层的 missingFiles 一致


class SessionView(Protocol):
    """Web 页面需要的会话接口，`cloud.session.Session` 满足它，测试用假实现。"""

    def status(self) -> SessionStatus: ...

    def current_qr(self) -> QrToken | None: ...

    async def regenerate_qr(self) -> QrToken: ...


class StallView(Protocol):
    """调度器记录的任务停住原因（只读）。``pipeline.orchestrator.Orchestrator`` 满足它。"""

    def stall_reason(self, info_hash: str) -> str | None: ...


FreeSpaceFn = Callable[[str], int]


def disk_free_bytes(path: str) -> int:
    """`path` 所在文件系统的剩余字节数。"""
    return shutil.disk_usage(path).free


# ---------------------------------------------------------------- 数据组装


def _iso(dt: datetime | None) -> str | None:
    return dt.isoformat() if dt is not None else None


def _session_json(st: SessionStatus) -> dict[str, Any]:
    return {
        "state": str(st.state),
        "since": _iso(st.since),
        "last_success": _iso(st.last_success),
        "circuit_until": _iso(st.circuit_until),
        "circuit_level": st.circuit_level,
        "login_app": st.login_app,
        "cookie_present": st.cookie_present,
        "qr_pending": st.qr_pending,
        "degraded_since": _iso(st.degraded_since),
        "detail": st.detail,
    }


def _space_json(st: SessionStatus) -> dict[str, int] | None:
    """115 网盘空间（字节）；会话层还没读到时为 None（页面显示「未知」）。"""
    if st.space is None:
        return None
    return {"total": st.space.total, "remaining": st.space.remaining}


def _total(task: Task, manifest_total: int) -> int:
    return task.cloud_size or manifest_total


def _local_percent(task: Task, total: int) -> float:
    if task.state is TaskState.COMPLETED:
        return 100.0
    if total <= 0:
        return 0.0
    return max(0.0, min(100.0, task.local_bytes * 100.0 / total))


# ---------------------------------------------------------------- 路由


def create_web_router(
    *,
    repo: TaskRepo,
    settings: Settings,
    clock: Clock,
    session: SessionView,
    sessions: dict[str, float],
    progress_source: ProgressSource | None = None,
    free_space: FreeSpaceFn = disk_free_bytes,
    stall_view: StallView | None = None,
    missing_files: MissingFilesView | None = None,
) -> APIRouter:
    progress_src: ProgressSource = progress_source or ZeroProgressSource()
    router = APIRouter()
    require_auth = make_require_auth(sessions, clock)
    auth = [Depends(require_auth)]

    def _authed(request: Request) -> bool:
        try:
            require_auth(request)
        except Exception:
            return False
        return True

    def _local_free() -> int | None:
        try:
            return free_space(settings.paths.incomplete)
        except OSError:
            return None

    async def _quota() -> dict[str, Any] | None:
        decoded = decode_quota(await repo.kv_get(KV_QUOTA))
        quota: Quota | None
        read_at: datetime | None
        if decoded is not None:
            quota, read_at = decoded
        else:
            quota, read_at = session.status().quota, None
        if quota is None:
            return None
        return {"remaining": quota.remaining, "total": quota.total, "read_at": _iso(read_at)}

    async def _task_json(t: Task) -> dict[str, Any]:
        manifest = await repo.get_manifest(t.hash)
        total = _total(t, sum(f.size for f in manifest))
        prog = progress_src.progress(t.hash)
        fetching = t.state is TaskState.FETCHING
        missing = (
            missing_files is not None
            and t.state is TaskState.COMPLETED
            and missing_files.might_be_missing(t.hash)
            and missing_files.is_missing(t.hash, await completion_id(repo, t.hash))
        )
        stall = stall_view.stall_reason(t.hash) if stall_view is not None else None
        return {
            "hash": t.hash,
            "name": t.name,
            "category": t.category,
            "state": str(t.state),
            "cloud_percent": t.cloud_percent,
            "local_percent": _local_percent(t, total),
            "size": total,
            "dlspeed": prog.dlspeed if fetching else 0,
            "eta": prog.eta if fetching else None,
            "waiting_for_space": prog.waiting_for_space if fetching else False,
            "error": t.error,
            "missing_files": missing,
            "stall_reason": MISSING_FILES_TEXT if missing else stall,
            "retries": {str(k): v for k, v in t.retries.items()},
            "fallback": str(t.fallback),
        }

    async def _history() -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
        """任务历史（T19）：只影响 Web 可见性，不删除、不修改任何 tasks 行与 events。

        失败历史：failed 且 client_deleted，距转入 failed 的事件时刻不超过保留期；
        成功历史：存在 completed 事件且当前 removed，距最近一条 completed 事件不超过保留期。
        保留期用注入时钟计算；0 关闭两区（返回空列表）。均按时间倒序。
        """
        days = settings.webui.history_retention_days
        if days <= 0:
            return [], []
        cutoff = clock.now() - timedelta(days=days)
        failed: list[tuple[datetime, dict[str, Any]]] = []
        for t in await repo.list_tasks(states=[TaskState.FAILED], include_client_deleted=True):
            if not t.client_deleted:
                continue
            # failed 是终态，最后一条事件就是转入 failed
            events = await repo.list_events(t.hash, limit=1)
            if not events or events[0].to_state is not TaskState.FAILED or events[0].at < cutoff:
                continue
            failed.append(
                (
                    events[0].at,
                    {
                        "name": t.name,
                        "category": t.category,
                        "failed_at": _iso(events[0].at),
                        "error": t.error,
                        "retries": {str(k): v for k, v in t.retries.items()},
                        "fallback": str(t.fallback),
                        "hash": t.hash,
                    },
                )
            )
        completed: list[tuple[datetime, dict[str, Any]]] = []
        for t in await repo.list_tasks(states=[TaskState.REMOVED], include_client_deleted=True):
            # 快路径：常见的 completed→removed 直达只拉 2 条事件；
            # 否则（requeue 后再删等罕见情形）深扫取最近一条 completed。
            events = await repo.list_events(t.hash, limit=2)
            ev: Event | None
            if (
                len(events) >= 2
                and events[0].to_state is TaskState.REMOVED
                and events[0].from_state is TaskState.COMPLETED
                and events[1].to_state is TaskState.COMPLETED
            ):
                ev = events[1]
            else:
                deep = await repo.list_events(t.hash, limit=EVENTS_PER_TASK)
                ev = next((e for e in deep if e.to_state is TaskState.COMPLETED), None)
            if ev is None or ev.at < cutoff:
                continue  # 在途被删（从未 completed）不进成功历史
            manifest = await repo.get_manifest(t.hash)
            completed.append(
                (
                    ev.at,
                    {
                        "name": t.name,
                        "category": t.category,
                        "completed_at": _iso(ev.at),
                        "size": _total(t, sum(f.size for f in manifest)),
                        "path": content_path(settings.paths.root, t, manifest),
                    },
                )
            )
        failed.sort(key=lambda item: item[0], reverse=True)
        completed.sort(key=lambda item: item[0], reverse=True)
        return [entry for _, entry in failed], [entry for _, entry in completed]

    async def _data() -> dict[str, Any]:
        tasks = await repo.list_tasks(include_client_deleted=False)
        visible = [t for t in tasks if t.state is not TaskState.REMOVED]
        status = session.status()
        failed_history, completed_history = await _history()
        return {
            "session": _session_json(status),
            "tasks": [await _task_json(t) for t in visible],
            "quota": await _quota(),
            "cloud_space": _space_json(status),
            "local_free_bytes": _local_free(),
            "failed_history": failed_history,
            "completed_history": completed_history,
        }

    @router.get("/", response_class=HTMLResponse)
    async def index(request: Request) -> Response:
        if not _authed(request):
            return HTMLResponse(web_ui.render_login_page())
        data = await _data()
        has_qr = session.current_qr() is not None  # 访问页面会续扫码轮询窗口
        return HTMLResponse(
            web_ui.render_status_page(
                data,
                has_qr=has_qr,
                min_free_gib=settings.fetch.min_free_gib,
                incomplete=settings.paths.incomplete,
                now=clock.now(),
                history_retention_days=settings.webui.history_retention_days,
            )
        )

    @router.get("/web/status.json", dependencies=auth)
    async def status_json() -> Response:
        return JSONResponse(await _data())

    @router.get("/web/qr.png", dependencies=auth)
    async def qr_png() -> Response:
        qr = session.current_qr()
        if qr is None:
            return Response(status_code=404)
        return Response(qr.image_png, media_type="image/png", headers={"Cache-Control": "no-store"})

    @router.post("/web/qr/regenerate", dependencies=auth)
    async def qr_regenerate() -> Response:
        try:
            await session.regenerate_qr()
        except Exception as exc:
            log_event(
                logger,
                logging.WARNING,
                "qr regenerate failed",
                stage="web",
                error=type(exc).__name__,
            )
            return JSONResponse({"ok": False, "error": type(exc).__name__}, status_code=502)
        return JSONResponse({"ok": True})

    return router


def create_app(
    *,
    repo: TaskRepo,
    settings: Settings,
    secrets: Secrets,
    clock: Clock,
    cleaner: LocalCleaner,
    session: SessionView,
    progress_source: ProgressSource | None = None,
    free_space: FreeSpaceFn = disk_free_bytes,
    stall_view: StallView | None = None,
    missing_files: MissingFilesView | None = None,
) -> FastAPI:
    """qBit API 与 Web 页面挂在同一个 app，共用同一份登录会话。"""
    app = FastAPI(title="115 Bridge", docs_url=None, redoc_url=None, openapi_url=None)
    sessions: dict[str, float] = {}

    @app.get("/healthz", response_class=PlainTextResponse)
    async def healthz() -> str:
        """容器健康检查：无需认证，只表示进程在响应，不暴露任何状态。"""
        return "ok"

    app.include_router(
        create_qbit_router(
            repo=repo,
            settings=settings,
            secrets=secrets,
            clock=clock,
            cleaner=cleaner,
            progress_source=progress_source,
            sessions=sessions,
            missing_files=missing_files,
        )
    )
    app.include_router(
        create_web_router(
            repo=repo,
            settings=settings,
            clock=clock,
            session=session,
            sessions=sessions,
            progress_source=progress_source,
            free_space=free_space,
            stall_view=stall_view,
            missing_files=missing_files,
        )
    )
    return app


__all__ = [
    "SessionView",
    "StallView",
    "create_app",
    "create_web_router",
    "disk_free_bytes",
]
