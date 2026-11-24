"""文件定位（T10）：把 115 任务目录树对照种子清单，结果写入 task_files 和 cloud_index。

- 只用 ``get_entry`` 和 ``list_dir`` 从 ``task.cloud_file_id`` 递归遍历，不用任何搜索接口。
- 115 的 id 一律按字符串处理（不变量 5）。
- 本模块不做状态转换（由调度器负责）。匹配失败抛 ``MatchFailed``；
  云端条目已不在（``get_entry`` 返回 None，或递归中 ``list_dir`` 抛出）
  统一抛 ``CloudNotFound``：任务级，调度器把任务标 failed 并做 Arr 回退（不再回退到重新提交）。
- ``task.cloud_file_id`` 为空是调度器的程序缺陷（不该在没有根 id 时调用 locate），抛
  ``MissingCloudRoot``（``LocateError`` 子类，不是 ``CloudNotFound``）：调度器应告警，
  不标 failed、不回退，以免把好片源拉黑。
  全局类异常（AuthExpired 等）原样上抛。
- 防护：根条目 id 等于该分类保存目录 id（kv ``cloud.dir:<root_dir>/<category>``）时判
  ``MatchFailed``，不会把保存目录当作任务目录；无清单且 ``task.cloud_size`` 为 None 时同样
  判 ``MatchFailed``（无从校验大小）。
- 重入：任务已有 task_files 且新结果（rel_path、size、sha1、pickcode）完全一致时保留原有
  ``local_bytes`` 与 ``verified``，不整体替换；不一致才替换。
"""

from __future__ import annotations

from collections import Counter
from dataclasses import dataclass, field

from bridge.clock import Clock
from bridge.cloud.protocol import CloudClient, CloudEntry, CloudNotFound
from bridge.db.repo import (
    CloudIndexEntry,
    ManifestFile,
    Task,
    TaskFile,
    TaskRepo,
    TaskUpdate,
)
from bridge.kv_keys import KV_DIR_PREFIX
from bridge.pipeline.submit import category_path

PAD_PREFIX = ".pad/"


class LocateError(Exception):
    pass


class MissingCloudRoot(LocateError):
    """``task.cloud_file_id`` 为空。调度器的程序缺陷，不是任务问题，也不是云端问题。"""


class MatchFailed(LocateError):
    """清单与 115 目录树对不上，或大小之和不符。任务级失败。"""

    def __init__(self, reason: str) -> None:
        super().__init__(reason)
        self.reason = reason


@dataclass(frozen=True, slots=True)
class LocateResult:
    root_name: str  # 115 上的根目录名或单文件文件名
    task_name: str  # 定位后的任务名（无清单时为净化后的 root_name）
    total_size: int
    files: list[TaskFile]
    skipped_pads: list[str] = field(default_factory=list)


@dataclass(frozen=True, slots=True)
class _CloudFile:
    rel_path: str
    entry: CloudEntry

    @property
    def dir(self) -> str:
        return self.rel_path.rpartition("/")[0]


def sanitize_name(name: str, info_hash: str) -> str:
    """去掉 ``/``、``\\``、NUL；结果为空、``.``、``..`` 时改用 hash。"""
    cleaned = name.replace("/", "").replace("\\", "").replace("\x00", "")
    if cleaned in ("", ".", ".."):
        return info_hash
    return cleaned


def _dir_of(rel_path: str) -> str:
    return rel_path.rpartition("/")[0]


async def _walk(cloud: CloudClient, root: CloudEntry) -> list[_CloudFile]:
    if not root.is_dir:
        return [_CloudFile(root.name, root)]
    out: list[_CloudFile] = []
    seen: set[str] = {root.id}
    stack: list[tuple[str, str]] = [(root.id, "")]
    while stack:
        dir_id, prefix = stack.pop()
        for e in await cloud.list_dir(dir_id):
            rel = f"{prefix}{e.name}"
            if e.is_dir:
                if e.id in seen:
                    continue
                seen.add(e.id)
                stack.append((e.id, rel + "/"))
            else:
                out.append(_CloudFile(rel, e))
    out.sort(key=lambda f: f.rel_path)
    return out


def _preview(items: list[str], limit: int = 5) -> str:
    shown = ", ".join(items[:limit])
    return shown if len(items) <= limit else f"{shown} (+{len(items) - limit} more)"


def _match_manifest(
    manifest: list[ManifestFile], cloud_files: list[_CloudFile]
) -> list[_CloudFile]:
    """返回匹配上的云端文件；对不上抛 MatchFailed。"""
    claimed: dict[str, _CloudFile] = {}  # 云端 entry.id -> 文件
    by_path: dict[tuple[str, int], _CloudFile] = {}
    for cf in cloud_files:
        by_path.setdefault((cf.rel_path, cf.entry.size), cf)

    # 第一轮：相对路径加大小精确匹配
    pending: list[ManifestFile] = []
    for m in manifest:
        hit = by_path.get((m.rel_path, m.size))
        if hit is not None and hit.entry.id not in claimed:
            claimed[hit.entry.id] = hit
        else:
            pending.append(m)

    # 第二轮：同目录内大小唯一。只在「尚未精确匹配」的文件里计数：
    # 清单侧数该目录里精确匹配失败的条目，115 侧数该目录里没被精确匹配占用的文件。
    free_cloud = [cf for cf in cloud_files if cf.entry.id not in claimed]
    cloud_dir_sizes = Counter((cf.dir, cf.entry.size) for cf in free_cloud)
    manifest_dir_sizes = Counter(
        (_dir_of(m.rel_path), m.size) for m in pending if not m.rel_path.startswith(PAD_PREFIX)
    )
    missing: list[str] = []
    for m in pending:
        # BEP 47 填充文件：115 上精确匹配不到就跳过，不算失败，也不做退化匹配
        # （主会话决定）。115 上有的填充文件在第一轮已照常匹配。
        if m.rel_path.startswith(PAD_PREFIX):
            continue
        key = (_dir_of(m.rel_path), m.size)
        if cloud_dir_sizes[key] == 1 and manifest_dir_sizes[key] == 1:
            cand = next(cf for cf in free_cloud if (cf.dir, cf.entry.size) == key)
            claimed[cand.entry.id] = cand
            continue
        missing.append(m.rel_path)

    if missing:
        raise MatchFailed(f"manifest files not matched on 115: {_preview(missing)}")
    extra = [cf.rel_path for cf in cloud_files if cf.entry.id not in claimed]
    if extra:
        raise MatchFailed(f"files on 115 not in manifest: {_preview(extra)}")
    return [cf for cf in cloud_files if cf.entry.id in claimed]


def _same_files(a: list[TaskFile], b: list[TaskFile]) -> bool:
    def key(files: list[TaskFile]) -> list[tuple[str, int, str, str]]:
        return sorted((f.rel_path, f.size, f.sha1.lower(), f.pickcode) for f in files)

    return key(a) == key(b)


async def locate(
    task: Task,
    *,
    repo: TaskRepo,
    cloud: CloudClient,
    clock: Clock,
    root_dir: str,
) -> LocateResult:
    if not task.cloud_file_id:
        raise MissingCloudRoot(f"{task.hash}: cloud_file_id is empty")
    root = await cloud.get_entry(task.cloud_file_id)
    if root is None:
        raise CloudNotFound(f"{task.hash}: cloud entry {task.cloud_file_id} not found")
    save_dir_id = await repo.kv_get(KV_DIR_PREFIX + category_path(root_dir, task.category))
    if save_dir_id is not None and str(root.id) == str(save_dir_id):
        raise MatchFailed(
            f"task root {root.id} is the category save directory, not a task directory"
        )

    cloud_files = await _walk(cloud, root)
    if not cloud_files:
        raise MatchFailed("no files under 115 task root")

    manifest = await repo.get_manifest(task.hash)
    skipped_pads: list[str] = []
    if manifest:
        matched = _match_manifest(manifest, cloud_files)
        matched_paths = {cf.rel_path for cf in matched}
        skipped_pads = [
            m.rel_path
            for m in manifest
            if m.rel_path.startswith(PAD_PREFIX) and m.rel_path not in matched_paths
        ]
    else:
        matched = cloud_files
        total = sum(cf.entry.size for cf in matched)
        if task.cloud_size is None:
            raise MatchFailed("no manifest and task size unknown, cannot verify size")
        if total != task.cloud_size:
            raise MatchFailed(f"size sum {total} != task size {task.cloud_size}")

    files: list[TaskFile] = []
    for cf in matched:
        if not cf.entry.pickcode or not cf.entry.sha1:
            raise MatchFailed(f"{cf.rel_path}: missing pickcode or sha1 on 115")
        files.append(
            TaskFile(
                rel_path=cf.rel_path,
                size=cf.entry.size,
                sha1=cf.entry.sha1.lower(),
                pickcode=cf.entry.pickcode,
            )
        )
    total_size = sum(f.size for f in files)

    task_name = task.name
    if not manifest:
        # 纯 magnet 的任务名来自不可信的 dn，本地根目录名要用它，必须净化
        task_name = sanitize_name(root.name, task.hash)
        if task_name != task.name:
            await repo.update(task.hash, TaskUpdate(name=task_name))

    existing = await repo.list_task_files(task.hash)
    if existing and _same_files(existing, files):
        files = sorted(existing, key=lambda f: f.rel_path)  # 保留 local_bytes 与 verified
    else:
        await repo.put_task_files(task.hash, files)
    await repo.put_cloud_index(
        CloudIndexEntry(
            hash=task.hash,
            cloud_file_id=task.cloud_file_id,
            root_name=root.name,
            total_size=total_size,
            completed_at=clock.now(),
        )
    )
    return LocateResult(
        root_name=root.name,
        task_name=task_name,
        total_size=total_size,
        files=files,
        skipped_pads=skipped_pads,
    )
