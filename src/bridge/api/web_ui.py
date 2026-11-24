"""Web 页面模板与渲染（T15 重设计）：纯呈现模块，不 import 任何后端模块。

结构约定（保证服务端首屏与 JS 轮询更新不漂移）：

- 每块 HTML 骨架只定义一次（``string.Template``），同时带 ``$占位符`` 与 ``data-field``：
  服务端用 ``substitute`` 填首屏值；JS 轮询时克隆 ``<template>`` 里的同一骨架，
  用通用 binder 按 ``data-field`` 填新值。
- 动态文本服务端经 ``html.escape``、JS 经 ``textContent`` 插入，不拼 innerHTML。
- 中文状态文案只出现在服务端渲染与内嵌常量里，JS 不硬编码，
  以便「本地文件缺失」这类文案只在该出现时出现。
- 初始数据与常量以 ``<script type="application/json">`` 内嵌（``<`` 转义防闭合）。
- 深浅双主题：CSS 变量 + ``html[data-theme]``，默认跟随系统，选择存 localStorage。
- 绝对时间：JS 接管后显示浏览器本地时区；服务端首屏 fallback 用 UTC 墙钟并显式标 ``UTC``。
"""

from __future__ import annotations

import html
import json
from datetime import datetime
from string import Template
from typing import Any

# ---------------------------------------------------------------- 文案与配色

# 状态 -> (中文标签, badge class)。等待空间与文件缺失是覆盖项，见 task_view。
STATE_LABELS: dict[str, tuple[str, str]] = {
    "queued": ("排队中", "badge gray"),
    "submitting": ("排队中", "badge gray"),
    "cloud_metadata": ("排队中", "badge gray"),
    "cloud_downloading": ("离线中", "badge blue"),
    "cloud_done": ("离线中", "badge blue"),
    "locating": ("离线中", "badge blue"),
    "fetching": ("取回中", "badge purple"),
    "verifying": ("校验中", "badge amber"),
    "completed": ("已完成", "badge green"),
    "failed": ("失败", "badge red"),
    "removed": ("已移除", "badge gray"),
}
SESSION_LABELS: dict[str, tuple[str, str]] = {
    "ok": ("正常", "ok-text"),
    "auth_expired": ("认证失效", "bad-text"),
    "verify_required": ("需人工验证", "warn-text"),
    "circuit_open": ("风控熔断", "warn-text"),
    "lease_lost": ("实例失锁", "bad-text"),
}
MISSING_LABEL = ("文件缺失", "badge red")
WAITING_LABEL = ("等空间", "badge amber")
POLL_SECONDS = 5
THEME_STORAGE_KEY = "bridge-theme"


def _e(value: object) -> str:
    return html.escape(str(value), quote=True)


def _safe_json(obj: Any) -> str:
    """内嵌 JSON：转义 < 防止提前闭合 script 标签。"""
    return json.dumps(obj, ensure_ascii=False, separators=(",", ":")).replace("<", "\\u003c")


# ---------------------------------------------------------------- 格式化


def fmt_bytes(n: int | None) -> str:
    if n is None:
        return "未知"
    size = float(n)
    for unit in ("B", "KiB", "MiB", "GiB", "TiB"):
        if size < 1024 or unit == "TiB":
            return f"{size:.0f} {unit}" if unit == "B" else f"{size:.2f} {unit}"
        size /= 1024
    return f"{n} B"  # pragma: no cover


def fmt_eta(seconds: int | None) -> str:
    if seconds is None or seconds >= 8640000:
        return "-"
    m, s = divmod(seconds, 60)
    h, m = divmod(m, 60)
    return f"{h}h{m:02d}m" if h else f"{m}m{s:02d}s"


def fmt_abs(iso: str | None) -> str:
    """服务端首屏 fallback 用 UTC 墙钟并显式标注；JS 接管后显示浏览器本地时区。"""
    return f"{iso[:19].replace('T', ' ')} UTC" if iso else "-"


def fmt_rel(iso: str | None, now: datetime) -> str:
    if not iso:
        return "-"
    delta = (now - datetime.fromisoformat(iso)).total_seconds()
    if delta < 0:
        return fmt_abs(iso)
    if delta < 60:
        return "刚刚"
    if delta < 3600:
        return f"{int(delta // 60)} 分钟前"
    if delta < 86400:
        return f"{int(delta // 3600)} 小时前"
    return f"{int(delta // 86400)} 天前"


def fmt_pct(value: float) -> str:
    return f"{value:.0f}%"


# ---------------------------------------------------------------- 视图模型


def session_view(s: dict[str, Any], now: datetime) -> dict[str, Any]:
    label, tone = SESSION_LABELS.get(s["state"], ("未知", "warn-text"))
    ok = s["state"] == "ok"
    return {
        "label": label,
        "tone": f"big {tone}",
        "pill": "pill" if ok else "pill bad",
        "raw": s["state"],
        "login_app": s["login_app"],
        "cookie": "存在" if s["cookie_present"] else "不存在",
        "last_rel": fmt_rel(s["last_success"], now),
        "last_abs": s["last_success"] or "",
        "has_circuit": bool(s["circuit_until"]),
        "circuit_hidden": "" if s["circuit_until"] else "hidden",
        "circuit_rel": fmt_abs(s["circuit_until"]),
        "circuit_level": s["circuit_level"],
        "has_degraded": bool(s["degraded_since"]),
        "degraded_hidden": "" if s["degraded_since"] else "hidden",
        "degraded_rel": fmt_rel(s["degraded_since"], now),
        "detail": s["detail"] or "",
        "has_detail": bool(s["detail"]) and not ok,
        "detail_hidden": "" if (s["detail"] and not ok) else "hidden",
    }


def quota_view(q: dict[str, Any] | None, now: datetime) -> dict[str, Any]:
    if q is None:
        return {
            "text": "未知",
            "width": 0,
            "has": False,
            "bar_hidden": "hidden",
            "read_rel": "-",
            "read_abs": "",
        }
    total = q["total"] or 0
    width = round(q["remaining"] * 100.0 / total, 1) if total else 0
    return {
        "text": f"{q['remaining']} / {q['total']}",
        "width": width,
        "has": True,
        "bar_hidden": "",
        "read_rel": fmt_rel(q["read_at"], now),
        "read_abs": q["read_at"] or "",
    }


def space_view(sp: dict[str, Any] | None) -> dict[str, Any]:
    if sp is None:
        return {
            "text": "未知",
            "total": "-",
            "width": 0,
            "has": False,
            "bar_hidden": "hidden",
            "used": "-",
        }
    total = sp["total"] or 0
    width = round(sp["remaining"] * 100.0 / total, 1) if total else 0
    used = round((total - sp["remaining"]) * 100.0 / total, 1) if total else 0
    return {
        "text": fmt_bytes(sp["remaining"]),
        "total": fmt_bytes(sp["total"]),
        "width": width,
        "has": True,
        "bar_hidden": "",
        "used": f"{used:.1f}%",
    }


def task_view(t: dict[str, Any]) -> dict[str, Any]:
    if t["missing_files"]:
        label, cls = MISSING_LABEL
    elif t["state"] == "fetching" and t["waiting_for_space"]:
        label, cls = WAITING_LABEL
    else:
        label, cls = STATE_LABELS.get(t["state"], ("未知", "badge gray"))
    fb = t["fallback"]
    fb_label, fb_cls = ("-", "dimtd") if fb == "none" else (fb, "badge amber")
    retries = t["retries"]
    return {
        "name": t["name"],
        "category": t["category"],
        "badge_label": label,
        "badge_cls": cls,
        "cloud_pct": fmt_pct(t["cloud_percent"]),
        "cloud_w": round(t["cloud_percent"], 1),
        "local_pct": fmt_pct(t["local_percent"]),
        "local_w": round(t["local_percent"], 1),
        "speed": fmt_bytes(t["dlspeed"]) + "/s" if t["dlspeed"] else "-",
        "eta": fmt_eta(t["eta"]) if t["state"] == "fetching" else "-",
        "retries": f"{retries.get('fetch', 0)}/{retries.get('verify', 0)}",
        "fallback_label": fb_label,
        "fallback_cls": fb_cls,
        "error": t["error"] or "-",
        "stall": t["stall_reason"] or "-",
        "state": t["state"],
        "hash": t["hash"],
    }


def failed_history_view(t: dict[str, Any]) -> dict[str, Any]:
    """失败历史行（T19）。绝对时间首屏用 UTC fallback，JS 接管后走本地时区。"""
    fb = t["fallback"]
    if fb == "none":
        fb_label, fb_cls = "-", "dimtd"
    elif fb == "done":
        fb_label, fb_cls = "done", "badge green"  # 回退已完成：草图定为绿色
    else:
        fb_label, fb_cls = fb, "badge amber"
    retries = t["retries"]
    return {
        "name": t["name"],
        "category": t["category"],
        "failed_abs": fmt_abs(t["failed_at"]),
        "error": t["error"] or "-",
        "error_full": t["error"] or "",
        "retries": f"{retries.get('fetch', 0)}/{retries.get('verify', 0)}",
        "fallback_label": fb_label,
        "fallback_cls": fb_cls,
        "hash": t["hash"],
    }


def completed_history_view(t: dict[str, Any]) -> dict[str, Any]:
    """成功历史行（T19）。"""
    return {
        "name": t["name"],
        "category": t["category"],
        "completed_abs": fmt_abs(t["completed_at"]),
        "size": fmt_bytes(t["size"]),
        "path": t["path"],
    }


# ---------------------------------------------------------------- CSS

CSS = """\
html[data-theme="dark"]{--bg:#0e1016;--card:#161a24;--line:#252b3b;--text:#e9ecf5;
--muted:#8f97ad;--accent:#8b5cf6;--accent2:#a78bfa;--track:#232939;--green:#34d399;
--amber:#fbbf24;--red:#f87171;--blue:#60a5fa;--purple:#a78bfa;color-scheme:dark}
html[data-theme="light"]{--bg:#f5f6fa;--card:#ffffff;--line:#e4e7f0;--text:#1b2130;
--muted:#68708a;--accent:#7c3aed;--accent2:#8b5cf6;--track:#eceef5;--green:#059669;
--amber:#b45309;--red:#dc2626;--blue:#2563eb;--purple:#7c3aed;color-scheme:light}
*{box-sizing:border-box}
[hidden]{display:none !important}
body{background:var(--bg);color:var(--text);margin:0;
font:14px/1.5 system-ui,-apple-system,"Segoe UI","PingFang SC","Microsoft YaHei",sans-serif}
.mono{font-family:ui-monospace,SFMono-Regular,Consolas,monospace}
.wrap{max-width:1180px;margin:0 auto;padding:22px 20px 30px}
.top{display:flex;justify-content:space-between;align-items:center;gap:12px;
margin-bottom:18px;flex-wrap:wrap}
.brand{display:flex;align-items:center;gap:10px}
.logo{width:28px;height:28px;border-radius:9px;
background:linear-gradient(135deg,var(--accent),#c084fc);
box-shadow:0 2px 10px rgba(139,92,246,.45)}
h1{font-size:18px;margin:0;letter-spacing:.2px}
.tools{display:flex;gap:8px;align-items:center}
.pill{display:inline-flex;align-items:center;gap:7px;padding:5px 12px;
border-radius:999px;font-size:12.5px;border:1px solid var(--line);background:var(--card)}
.dot{width:8px;height:8px;border-radius:50%;background:var(--green);
box-shadow:0 0 8px var(--green)}
.pill.bad .dot{background:var(--red);box-shadow:0 0 8px var(--red)}
.iconbtn{width:32px;height:32px;border-radius:9px;border:1px solid var(--line);
background:var(--card);color:var(--muted);cursor:pointer;font-size:15px}
.iconbtn:hover{color:var(--text);border-color:var(--accent)}
.banner{background:rgba(248,113,113,.12);border:1px solid var(--red);color:var(--red);
border-radius:10px;padding:8px 12px;font-size:12.5px;margin-bottom:14px}
.cards{display:grid;grid-template-columns:repeat(4,1fr);gap:14px}
.card{background:var(--card);border:1px solid var(--line);border-radius:14px;
padding:14px 16px}
.card h3{margin:0 0 8px;font-size:12px;font-weight:600;color:var(--muted)}
.big{font-size:24px;font-weight:650;margin-bottom:8px}
.ok-text{color:var(--green)}
.bad-text{color:var(--red)}
.warn-text{color:var(--amber)}
.bar{height:6px;border-radius:99px;background:var(--track);overflow:hidden;
margin:6px 0 8px}
.bar i{display:block;height:100%;border-radius:99px;
background:linear-gradient(90deg,var(--accent),var(--accent2))}
.kv{display:flex;justify-content:space-between;gap:8px;font-size:12.5px;
color:var(--muted);padding:2px 0}
.kv span{flex:none}
.kv b{color:var(--text);font-weight:550;min-width:0;word-break:break-all;
text-align:right}
.qrpanel{display:flex;gap:18px;align-items:center;background:var(--card);
border:1px solid var(--red);border-radius:14px;padding:16px;margin-top:14px}
.qrpanel img{border-radius:8px;background:#fff;padding:6px}
.qrpanel h3{margin:0 0 6px;font-size:14px}
.qrpanel p{margin:0 0 10px;color:var(--muted);font-size:12.5px}
.btn{background:var(--accent);color:#fff;border:0;border-radius:9px;padding:8px 14px;
font-size:13px;cursor:pointer}
.btn:hover{filter:brightness(1.1)}
.tasks{margin-top:20px}
.shead{display:flex;align-items:center;gap:10px;margin-bottom:10px}
.shead h2{font-size:15px;margin:0}
.count{background:var(--track);color:var(--muted);border-radius:99px;padding:1px 9px;
font-size:12px}
.hint{margin-left:auto;color:var(--muted);font-size:12px}
.twrap{border:1px solid var(--line);border-radius:14px;overflow:hidden;
background:var(--card)}
table{width:100%;border-collapse:collapse}
th{font-size:12px;color:var(--muted);text-align:left;font-weight:600;padding:10px 12px;
border-bottom:1px solid var(--line);white-space:nowrap}
td{padding:11px 12px;border-bottom:1px solid var(--line);font-size:13px;
vertical-align:middle}
tbody tr:last-child td{border-bottom:0}
tr.row{cursor:pointer}
tr.row:hover td{background:rgba(139,92,246,.06)}
.tname{font-weight:550;max-width:340px;overflow:hidden;text-overflow:ellipsis;
white-space:nowrap}
.cat{display:inline-block;margin-top:3px;font-size:11px;color:var(--muted);
border:1px solid var(--line);border-radius:6px;padding:0 6px}
.badge{display:inline-block;padding:2px 9px;border-radius:999px;font-size:12px;
font-weight:550;white-space:nowrap}
.badge.purple{color:var(--purple);background:rgba(139,92,246,.16)}
.badge.blue{color:var(--blue);background:rgba(96,165,250,.15)}
.badge.green{color:var(--green);background:rgba(52,211,153,.15)}
.badge.red{color:var(--red);background:rgba(248,113,113,.15)}
.badge.amber{color:var(--amber);background:rgba(251,191,36,.15)}
.badge.gray{color:var(--muted);background:var(--track)}
.prog{display:flex;align-items:center;gap:8px;min-width:110px}
.prog .bar{flex:1;margin:0}
.prog span{font-size:12px;color:var(--muted);width:38px;text-align:right}
td.num{font-family:ui-monospace,SFMono-Regular,Consolas,monospace;white-space:nowrap}
.dimtd{color:var(--muted)}
tr.detail td{background:rgba(139,92,246,.05);font-size:12.5px;color:var(--muted);
padding:10px 14px}
tr.detail b{color:var(--text)}
.empty{color:var(--muted);text-align:center;padding:26px 12px}
.history{margin-top:14px}
.history summary{display:flex;align-items:center;gap:10px;cursor:pointer;
list-style:none;padding:4px 2px}
.history summary::-webkit-details-marker{display:none}
.history summary::before{content:"▸";color:var(--muted);font-size:12px}
.history[open] summary::before{content:"▾"}
.history .htitle{font-size:15px;font-weight:600}
.history .twrap{margin-top:10px}
.clip{display:inline-block;max-width:280px;overflow:hidden;text-overflow:ellipsis;
white-space:nowrap;vertical-align:bottom}
footer{margin-top:16px;color:var(--muted);font-size:12px}
noscript p{color:var(--amber);font-size:12.5px}
@media(max-width:980px){.cards{grid-template-columns:repeat(2,1fr)}}
@media(max-width:768px){
.cards{grid-template-columns:1fr}
thead{display:none}
table,tbody,tr,td{display:block;width:100%}
tr.row,tr.hrow{border-bottom:1px solid var(--line);padding:6px 0}
td{border:0;padding:6px 14px;display:flex;justify-content:space-between;
align-items:center;gap:10px}
td::before{content:attr(data-label);color:var(--muted);font-size:12px;flex:none}
td.cell-name{display:block}
td.cell-name::before{display:block;margin-bottom:2px}
.tname{max-width:none;white-space:normal}
.prog{min-width:0;flex:1}
tr.detail td{display:block}
tr.detail td::before{content:none}
.clip{max-width:60vw}
.hint{display:none}
}
"""

# ---------------------------------------------------------------- 骨架

CARD_SESSION = Template(
    '<div class="card"><h3>会话</h3>'
    '<div class="$tone" data-field="session.label"'
    ' data-class-field="session.tone">$label</div>'
    '<div class="kv"><span>状态码</span>'
    '<b class="mono" data-field="session.raw">$raw</b></div>'
    '<div class="kv"><span>登录端</span><b data-field="session.login_app">$login_app</b></div>'
    '<div class="kv"><span>Cookie</span><b data-field="session.cookie">$cookie</b></div>'
    '<div class="kv"><span>最近成功</span>'
    '<b data-field="session.last_rel" data-title-field="session.last_abs"'
    ' title="$last_abs">$last_rel</b></div>'
    '<div class="kv" $circuit_hidden data-field="session.has_circuit" data-kind="hidden">'
    '<span>熔断解除</span><b data-field="session.circuit_rel">$circuit_rel</b></div>'
    '<div class="kv" $degraded_hidden data-field="session.has_degraded" data-kind="hidden">'
    '<span>网络异常起点</span><b data-field="session.degraded_rel">$degraded_rel</b></div>'
    '<div class="kv" $detail_hidden data-field="session.has_detail" data-kind="hidden">'
    '<span>说明</span><b data-field="session.detail">$detail</b></div>'
    "</div>"
)

CARD_QUOTA = Template(
    '<div class="card"><h3>离线配额</h3>'
    '<div class="big mono" data-field="quota.text">$text</div>'
    '<div class="bar" $bar_hidden data-field="quota.has" data-kind="hidden">'
    '<i data-field="quota.width" data-kind="width" style="width:$width%"></i></div>'
    '<div class="kv"><span>读取于</span>'
    '<b data-field="quota.read_rel" data-title-field="quota.read_abs"'
    ' title="$read_abs">$read_rel</b></div>'
    "</div>"
)

CARD_SPACE = Template(
    '<div class="card"><h3>115 网盘剩余空间</h3>'
    '<div class="big mono" data-field="space.text">$text</div>'
    '<div class="bar" $bar_hidden data-field="space.has" data-kind="hidden">'
    '<i data-field="space.width" data-kind="width" style="width:$width%"></i></div>'
    '<div class="kv"><span>总量</span><b data-field="space.total">$total</b></div>'
    '<div class="kv"><span>已用</span><b data-field="space.used">$used</b></div>'
    "</div>"
)

CARD_LOCAL = Template(
    '<div class="card"><h3>本地剩余空间</h3>'
    '<div class="big mono" data-field="local.text">$text</div>'
    '<div class="kv"><span>下载卷</span><b>$incomplete</b></div>'
    '<div class="kv"><span>保留余量</span><b>$min_free</b></div>'
    "</div>"
)

ROW_TEMPLATE_KEYS = (
    "name",
    "category",
    "badge_cls",
    "badge_label",
    "cloud_w",
    "cloud_pct",
    "local_w",
    "local_pct",
    "speed",
    "eta",
    "retries",
    "fallback_cls",
    "fallback_label",
    "error",
    "stall",
    "state",
    "hash",
)

ROW = Template(
    '<tr class="row"><td class="cell-name" data-label="名称">'
    '<div class="tname" data-field="task.name">$name</div>'
    '<span class="cat" data-field="task.category">$category</span></td>'
    '<td data-label="状态"><span class="$badge_cls" data-field="task.badge_label"'
    ' data-class-field="task.badge_cls">$badge_label</span></td>'
    '<td data-label="115 进度"><div class="prog"><div class="bar">'
    '<i data-field="task.cloud_w" data-kind="width" style="width:$cloud_w%"></i></div>'
    '<span data-field="task.cloud_pct">$cloud_pct</span></div></td>'
    '<td data-label="本地进度"><div class="prog"><div class="bar">'
    '<i data-field="task.local_w" data-kind="width" style="width:$local_w%"></i></div>'
    '<span data-field="task.local_pct">$local_pct</span></div></td>'
    '<td class="num" data-label="速度" data-field="task.speed">$speed</td>'
    '<td class="num" data-label="ETA" data-field="task.eta">$eta</td>'
    '<td class="num" data-label="重试 f/v" data-field="task.retries">$retries</td>'
    '<td data-label="回退"><span class="$fallback_cls" data-field="task.fallback_label"'
    ' data-class-field="task.fallback_cls">$fallback_label</span></td></tr>'
    '<tr class="detail" hidden><td colspan="8"><b>失败原因</b> '
    '<span data-field="task.error">$error</span> · <b>停住原因</b> '
    '<span data-field="task.stall">$stall</span> · <b>状态码</b> '
    '<span class="mono" data-field="task.state">$state</span> · <b>hash</b> '
    '<span class="mono" data-field="task.hash">$hash</span></td></tr>'
)

# T19 历史行：与主表同一骨架模式（$占位符 + data-field），只读、无展开交互。
# 原因/路径/hash 用 .clip 截断，title 挂全文（data-raw-title-field 不做时间格式化）。
FAILED_ROW_TEMPLATE_KEYS = (
    "name",
    "category",
    "failed_abs",
    "error",
    "error_full",
    "retries",
    "fallback_cls",
    "fallback_label",
    "hash",
)

FAILED_ROW = Template(
    '<tr class="hrow"><td data-label="名称">'
    '<div class="tname" data-field="failed.name">$name</div></td>'
    '<td data-label="category"><span class="cat" data-field="failed.category">$category</span></td>'
    '<td class="num" data-label="失败时间" data-field="failed.failed_abs">$failed_abs</td>'
    '<td data-label="失败原因"><span class="clip" data-field="failed.error"'
    ' data-raw-title-field="failed.error_full" title="$error_full">$error</span></td>'
    '<td class="num" data-label="重试 f/v" data-field="failed.retries">$retries</td>'
    '<td data-label="回退"><span class="$fallback_cls" data-field="failed.fallback_label"'
    ' data-class-field="failed.fallback_cls">$fallback_label</span></td>'
    '<td data-label="hash"><span class="mono clip" data-field="failed.hash"'
    ' data-raw-title-field="failed.hash" title="$hash">$hash</span></td></tr>'
)

DONE_ROW_TEMPLATE_KEYS = (
    "name",
    "category",
    "completed_abs",
    "size",
    "path",
)

DONE_ROW = Template(
    '<tr class="hrow"><td data-label="名称">'
    '<div class="tname" data-field="done.name">$name</div></td>'
    '<td data-label="category"><span class="cat" data-field="done.category">$category</span></td>'
    '<td class="num" data-label="完成时间" data-field="done.completed_abs">$completed_abs</td>'
    '<td class="num" data-label="总大小" data-field="done.size">$size</td>'
    '<td data-label="落盘路径"><span class="mono clip" data-field="done.path"'
    ' data-raw-title-field="done.path" title="$path">$path</span></td></tr>'
)

LOGIN = Template(
    '<!doctype html><html lang="zh" data-theme="dark"><head><meta charset="utf-8">'
    '<meta name="viewport" content="width=device-width,initial-scale=1">'
    "<title>115 Bridge 登录</title><style>$css</style></head><body>"
    '<form class="login" id="f">'
    '<div class="brand"><span class="logo"></span><h1>115 Bridge</h1></div>'
    '<p class="sub">登录以查看会话状态与任务</p>'
    '<label for="u">用户名</label>'
    '<input id="u" name="username" autocomplete="username">'
    '<label for="p">密码</label>'
    '<input id="p" name="password" type="password" autocomplete="current-password">'
    '<button class="btn" type="submit">登 录</button>'
    '<p class="err" id="m"></p>'
    '<p class="foot">仅限局域网访问 · 凭据不会出现在页面与日志中</p>'
    "</form><script>$js</script></body></html>"
)

LOGIN_CSS_EXTRA = """\
body{min-height:100vh;display:flex;align-items:center;justify-content:center;
background-image:radial-gradient(600px 300px at 50% -80px,rgba(139,92,246,.22),transparent)}
.login{width:360px;max-width:calc(100vw - 40px);background:var(--card);
border:1px solid var(--line);border-radius:18px;padding:30px 28px;
box-shadow:0 12px 40px rgba(0,0,0,.35)}
.login .brand{margin-bottom:6px}
.login .logo{width:34px;height:34px;border-radius:10px}
.sub{color:var(--muted);font-size:12.5px;margin:0 0 22px}
label{display:block;font-size:12.5px;color:var(--muted);margin:0 0 6px}
input{width:100%;background:var(--bg);border:1px solid var(--line);border-radius:10px;
color:var(--text);padding:10px 12px;font-size:14px;margin-bottom:16px;outline:none}
input:focus{border-color:var(--accent);box-shadow:0 0 0 3px rgba(139,92,246,.25)}
.login .btn{width:100%;padding:11px;font-weight:600;
background:linear-gradient(135deg,var(--accent),var(--accent2))}
.err{color:var(--red);font-size:12.5px;margin:12px 0 0;min-height:18px}
.foot{color:var(--muted);font-size:11.5px;margin:18px 0 0;text-align:center}
"""

LOGIN_JS = """\
(function(){
"use strict";
var key="bridge-theme";
var saved=null;
try{saved=localStorage.getItem(key);}catch(e){}
var light=window.matchMedia&&matchMedia("(prefers-color-scheme: light)").matches;
document.documentElement.dataset.theme=saved||(light?"light":"dark");
document.getElementById("f").addEventListener("submit",function(ev){
ev.preventDefault();
var body=new URLSearchParams(new FormData(ev.target));
fetch("/api/v2/auth/login",{method:"POST",body:body}).then(function(r){return r.text();})
.then(function(t){
if(t.trim()==="Ok."){location.reload();}
else{document.getElementById("m").textContent="登录失败";}
}).catch(function(){document.getElementById("m").textContent="网络错误";});
});
})();
"""

PAGE = Template(
    '<!doctype html><html lang="zh" data-theme="dark"><head><meta charset="utf-8">'
    '<meta name="viewport" content="width=device-width,initial-scale=1">'
    '<title>115 Bridge</title><style>$css</style></head><body><div class="wrap">'
    '<header class="top"><div class="brand"><span class="logo"></span>'
    '<h1>115 Bridge</h1></div><div class="tools">'
    '<span class="$pill" id="sessPill" data-field="session.pill" data-kind="class">'
    '<i class="dot"></i><span data-field="session.label">$session_label</span></span>'
    '<button class="iconbtn" id="themeBtn" title="切换主题">&#9680;</button>'
    '<button class="iconbtn" id="refreshBtn" title="刷新">&#10227;</button>'
    '<button class="iconbtn" id="logoutBtn" title="退出登录">&#9211;</button>'
    "</div></header>"
    '<div class="banner" id="connBanner" hidden>连接中断，正在重试…</div>'
    "<noscript><p>当前显示加载时快照；启用 JavaScript 后每 $poll 秒自动刷新。</p></noscript>"
    '<section class="cards" id="cards">$cards</section>'
    '<section class="qrpanel" id="qrPanel" $qr_hidden>'
    "$qr_img"
    "<div><h3>等待扫码登录（TV 端）</h3>"
    "<p>请用 115 手机 App 扫描二维码，5 分钟内有效。失效后自动换新码。</p>"
    '<button class="btn" id="regenBtn">重新生成二维码</button></div></section>'
    '<section class="tasks"><div class="shead"><h2>任务</h2>'
    '<span class="count" id="taskCount">$count</span>'
    '<span class="hint">点击行展开详情</span></div>'
    '<div class="twrap"><table><thead><tr><th>名称</th><th>状态</th><th>115 进度</th>'
    "<th>本地进度</th><th>速度</th><th>ETA</th><th>重试 f/v</th><th>回退</th></tr></thead>"
    '<tbody id="taskBody">$rows</tbody></table></div></section>'
    '<details class="history" id="failedHistory" $failed_hidden>'
    '<summary><span class="htitle">失败历史</span>'
    '<span class="badge red" id="failedCount">$failed_count</span>'
    '<span class="hint">保留 $history_days 天 · 超期自动隐藏 · 仅本页可见</span></summary>'
    '<div class="twrap"><table><thead><tr><th>名称</th><th>category</th><th>失败时间</th>'
    "<th>失败原因</th><th>重试 f/v</th><th>回退</th><th>hash</th></tr></thead>"
    '<tbody id="failedBody">$failed_rows</tbody></table></div></details>'
    '<details class="history" id="doneHistory" $done_hidden>'
    '<summary><span class="htitle">成功历史</span>'
    '<span class="badge green" id="doneCount">$done_count</span>'
    '<span class="hint">保留 $history_days 天 · 超期自动隐藏 · 仅本页可见</span></summary>'
    '<div class="twrap"><table><thead><tr><th>名称</th><th>category</th><th>完成时间</th>'
    "<th>总大小</th><th>落盘路径</th></tr></thead>"
    '<tbody id="doneBody">$done_rows</tbody></table></div></details>'
    '<footer>每 $poll 秒自动刷新 · 上次更新 <span id="updatedAt">$updated</span>'
    " · 只读页面，任务操作请在 Arr 中进行</footer>"
    "</div>"
    '<template id="row-tpl">$row_skeleton</template>'
    '<template id="failed-row-tpl">$failed_row_skeleton</template>'
    '<template id="done-row-tpl">$done_row_skeleton</template>'
    '<script type="application/json" id="ui-const">$constants</script>'
    '<script type="application/json" id="ui-data">$initial</script>'
    "<script>$js</script></body></html>"
)

# ---------------------------------------------------------------- 页面 JS

PAGE_JS = """\
(function(){
"use strict";
var CONST=JSON.parse(document.getElementById("ui-const").textContent);
var DATA=JSON.parse(document.getElementById("ui-data").textContent);
var expanded={};
var timer=null;
function bytes(n){
if(n==null)return "未知";
var u=["B","KiB","MiB","GiB","TiB"],v=n,i=0;
while(v>=1024&&i<u.length-1){v/=1024;i++;}
return (i===0?v.toFixed(0):v.toFixed(2))+" "+u[i];
}
function eta(s){
if(s==null||s>=8640000)return "-";
var m=Math.floor(s/60),h=Math.floor(m/60),ss=s%60;m=m%60;
function p(x){return String(x).padStart(2,"0");}
return h?h+"h"+p(m)+"m":m+"m"+p(ss)+"s";
}
function abs(iso){
if(!iso)return "-";
return new Date(iso).toLocaleString("zh-CN",{hour12:false});
}
function rel(iso){
if(!iso)return "-";
var d=(Date.now()-new Date(iso).getTime())/1000;
if(d<0)return abs(iso);
if(d<60)return "刚刚";
if(d<3600)return Math.floor(d/60)+" 分钟前";
if(d<86400)return Math.floor(d/3600)+" 小时前";
return Math.floor(d/86400)+" 天前";
}
function pct(x){return Math.round(x)+"%";}
function badge(t){
if(t.missing_files)return CONST.missing_label;
if(t.state==="fetching"&&t.waiting_for_space)return CONST.waiting_label;
return CONST.state_labels[t.state]||["未知","badge gray"];
}
function taskVM(t){
var b=badge(t),r=t.retries||{};
var fb=t.fallback==="none"?["-","dimtd"]:[t.fallback,"badge amber"];
return {
name:t.name,category:t.category,badge_label:b[0],badge_cls:b[1],
cloud_pct:pct(t.cloud_percent),cloud_w:t.cloud_percent,
local_pct:pct(t.local_percent),local_w:t.local_percent,
speed:t.dlspeed?bytes(t.dlspeed)+"/s":"-",
eta:t.state==="fetching"?eta(t.eta):"-",
retries:(r.fetch||0)+"/"+(r.verify||0),
fallback_label:fb[0],fallback_cls:fb[1],
error:t.error||"-",stall:t.stall_reason||"-",state:t.state,hash:t.hash
};
}
function failedHistVM(t){
var r=t.retries||{};
var fb=t.fallback==="none"?["-","dimtd"]
:t.fallback==="done"?["done","badge green"]:[t.fallback,"badge amber"];
return {
name:t.name,category:t.category,failed_abs:abs(t.failed_at),
error:t.error||"-",error_full:t.error||"",
retries:(r.fetch||0)+"/"+(r.verify||0),
fallback_label:fb[0],fallback_cls:fb[1],hash:t.hash
};
}
function doneHistVM(t){
return {
name:t.name,category:t.category,completed_abs:abs(t.completed_at),
size:bytes(t.size),path:t.path
};
}
function sessionVM(s){
var l=CONST.session_labels[s.state]||["未知","warn-text"];
var ok=s.state==="ok";
return {
label:l[0],tone:"big "+l[1],pill:ok?"pill":"pill bad",raw:s.state,
login_app:s.login_app,cookie:s.cookie_present?"存在":"不存在",
last_rel:rel(s.last_success),last_abs:s.last_success||"",
has_circuit:!!s.circuit_until,circuit_rel:abs(s.circuit_until),
has_degraded:!!s.degraded_since,degraded_rel:rel(s.degraded_since),
detail:s.detail||"",has_detail:!!s.detail&&!ok
};
}
function quotaVM(q){
if(!q)return {text:"未知",width:0,has:false,read_rel:"-",read_abs:""};
var w=q.total?Math.round(q.remaining*1000/q.total)/10:0;
return {text:q.remaining+" / "+q.total,width:w,has:true,
read_rel:rel(q.read_at),read_abs:q.read_at||""};
}
function spaceVM(sp){
if(!sp)return {text:"未知",total:"-",width:0,has:false,used:"-"};
var w=sp.total?Math.round(sp.remaining*1000/sp.total)/10:0;
var u=sp.total?Math.round((sp.total-sp.remaining)*1000/sp.total)/10:0;
return {text:bytes(sp.remaining),total:bytes(sp.total),width:w,has:true,used:u+"%"};
}
function resolve(ctx,path){
var v=ctx,p=path.split(".");
for(var i=0;i<p.length;i++){v=v==null?null:v[p[i]];}
return v;
}
function bind(root,ctx){
var els=Array.prototype.slice.call(root.querySelectorAll("[data-field]"));
if(root.getAttribute&&root.getAttribute("data-field"))els.unshift(root);
els.forEach(function(el){
var v=resolve(ctx,el.dataset.field);
var k=el.dataset.kind||"text";
if(k==="text"){el.textContent=v==null?"-":String(v);}
else if(k==="width"){el.style.width=(v||0)+"%";}
else if(k==="hidden"){el.hidden=!v;}
else if(k==="class"){el.className=v==null?"":String(v);}
if(el.dataset.classField){
el.className=String(resolve(ctx,el.dataset.classField)||"");
}
if(el.dataset.titleField){
var tv=resolve(ctx,el.dataset.titleField);
el.title=tv?abs(String(tv)):"";
}
if(el.dataset.rawTitleField){
var rv=resolve(ctx,el.dataset.rawTitleField);
el.title=rv==null?"":String(rv);
}
});
}
function renderRows(tasks){
var body=document.getElementById("taskBody");
body.textContent="";
document.getElementById("taskCount").textContent=String(tasks.length);
if(!tasks.length){
var tr=document.createElement("tr");
var td=document.createElement("td");
td.colSpan=8;td.className="empty";td.textContent="暂无任务";
tr.appendChild(td);body.appendChild(tr);return;
}
tasks.forEach(function(t){
var frag=document.getElementById("row-tpl").content.cloneNode(true);
bind(frag,{task:taskVM(t)});
var row=frag.querySelector("tr.row"),det=frag.querySelector("tr.detail");
if(expanded[t.hash])det.hidden=false;
row.addEventListener("click",function(){
det.hidden=!det.hidden;expanded[t.hash]=!det.hidden;
});
body.appendChild(frag);
});
}
function renderHistory(secId,countId,bodyId,tplId,entries,vmfn,ns){
document.getElementById(secId).hidden=!entries.length;
document.getElementById(countId).textContent=String(entries.length);
var body=document.getElementById(bodyId);
body.textContent="";
entries.forEach(function(t){
var frag=document.getElementById(tplId).content.cloneNode(true);
var ctx={};ctx[ns]=vmfn(t);
bind(frag,ctx);
body.appendChild(frag);
});
}
function apply(d){
var vm={
session:sessionVM(d.session),quota:quotaVM(d.quota),
space:spaceVM(d.cloud_space),local:{text:bytes(d.local_free_bytes)}
};
bind(document.getElementById("cards"),vm);
bind(document.getElementById("sessPill"),vm);
renderRows(d.tasks);
renderHistory("failedHistory","failedCount","failedBody","failed-row-tpl",
d.failed_history||[],failedHistVM,"failed");
renderHistory("doneHistory","doneCount","doneBody","done-row-tpl",
d.completed_history||[],doneHistVM,"done");
var panel=document.getElementById("qrPanel");
panel.hidden=!d.session.qr_pending;
var img=panel.querySelector("img");
if(d.session.qr_pending){
if(!img){
img=document.createElement("img");
img.alt="qr";img.width=240;
panel.insertBefore(img,panel.firstChild);
}
img.src="/web/qr"+".png?t="+Date.now();
}else if(img){img.remove();}
document.getElementById("updatedAt").textContent=
new Date().toLocaleTimeString("zh-CN",{hour12:false});
document.getElementById("connBanner").hidden=true;
}
function poll(){
fetch("/web/status.json",{cache:"no-store"}).then(function(r){
if(!r.ok)throw new Error("http "+r.status);
return r.json();
}).then(apply).catch(function(){
document.getElementById("connBanner").hidden=false;
});
}
function schedule(){
if(timer)clearTimeout(timer);
timer=setTimeout(function(){
if(!document.hidden)poll();
schedule();
},CONST.poll_seconds*1000);
}
var key="bridge-theme",saved=null;
try{saved=localStorage.getItem(key);}catch(e){}
var light=window.matchMedia&&matchMedia("(prefers-color-scheme: light)").matches;
document.documentElement.dataset.theme=saved||(light?"light":"dark");
document.getElementById("themeBtn").addEventListener("click",function(){
var h=document.documentElement;
var t=h.dataset.theme==="dark"?"light":"dark";
h.dataset.theme=t;
try{localStorage.setItem(key,t);}catch(e){}
});
document.getElementById("refreshBtn").addEventListener("click",poll);
document.getElementById("logoutBtn").addEventListener("click",function(){
fetch("/api/v2/auth/logout",{method:"POST"}).then(function(){location.reload();});
});
document.getElementById("regenBtn").addEventListener("click",function(){
fetch("/web/qr/regenerate",{method:"POST"}).then(poll).catch(poll);
});
apply(DATA);
schedule();
})();
"""


# ---------------------------------------------------------------- 渲染入口


def render_login_page() -> str:
    return LOGIN.substitute(css=CSS + LOGIN_CSS_EXTRA, js=LOGIN_JS)


def render_status_page(
    data: dict[str, Any],
    *,
    has_qr: bool,
    min_free_gib: int,
    incomplete: str,
    now: datetime,
    history_retention_days: int,
) -> str:
    sv = session_view(data["session"], now)
    qv = quota_view(data["quota"], now)
    spv = space_view(data["cloud_space"])
    cards = (
        CARD_SESSION.substitute(**{_e(k): _e(v) for k, v in sv.items()})
        + CARD_QUOTA.substitute(**{_e(k): _e(v) for k, v in qv.items()})
        + CARD_SPACE.substitute(**{_e(k): _e(v) for k, v in spv.items()})
        + CARD_LOCAL.substitute(
            text=_e(fmt_bytes(data["local_free_bytes"])),
            incomplete=_e(incomplete),
            min_free=_e(f"{min_free_gib} GiB"),
        )
    )
    views = [task_view(t) for t in data["tasks"]]
    rows = "".join(ROW.substitute(**{_e(k): _e(v) for k, v in vm.items()}) for vm in views)
    failed_views = [failed_history_view(t) for t in data["failed_history"]]
    failed_rows = "".join(
        FAILED_ROW.substitute(**{_e(k): _e(v) for k, v in vm.items()}) for vm in failed_views
    )
    done_views = [completed_history_view(t) for t in data["completed_history"]]
    done_rows = "".join(
        DONE_ROW.substitute(**{_e(k): _e(v) for k, v in vm.items()}) for vm in done_views
    )
    constants = {
        "poll_seconds": POLL_SECONDS,
        "state_labels": {k: list(v) for k, v in STATE_LABELS.items()},
        "session_labels": {k: list(v) for k, v in SESSION_LABELS.items()},
        "missing_label": list(MISSING_LABEL),
        "waiting_label": list(WAITING_LABEL),
    }
    qr_img = '<img src="/web/qr.png" alt="qr" width="240">' if has_qr else ""
    return PAGE.substitute(
        css=CSS,
        js=PAGE_JS,
        cards=cards,
        rows=rows,
        count=len(views),
        pill=_e(sv["pill"]),
        session_label=_e(sv["label"]),
        qr_hidden="" if has_qr else "hidden",
        qr_img=qr_img,
        poll=POLL_SECONDS,
        updated=f"{now.strftime('%H:%M:%S')} UTC",
        row_skeleton=ROW.substitute(**{k: "" for k in ROW_TEMPLATE_KEYS}),
        failed_hidden="" if failed_views else "hidden",
        failed_count=len(failed_views),
        failed_rows=failed_rows,
        done_hidden="" if done_views else "hidden",
        done_count=len(done_views),
        done_rows=done_rows,
        history_days=history_retention_days,
        failed_row_skeleton=FAILED_ROW.substitute(**{k: "" for k in FAILED_ROW_TEMPLATE_KEYS}),
        done_row_skeleton=DONE_ROW.substitute(**{k: "" for k in DONE_ROW_TEMPLATE_KEYS}),
        constants=_safe_json(constants),
        initial=_safe_json(data),
    )


__all__ = [
    "POLL_SECONDS",
    "STATE_LABELS",
    "SESSION_LABELS",
    "THEME_STORAGE_KEY",
    "fmt_bytes",
    "fmt_eta",
    "fmt_rel",
    "render_login_page",
    "render_status_page",
]
