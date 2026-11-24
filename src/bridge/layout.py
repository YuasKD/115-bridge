"""本地落盘布局的纯函数（T12）。

qBit 层报给 Arr 的 ``save_path`` / ``content_path`` 与校验后的 rename 目标必须一致，
否则 Arr 找不到文件，所以两边都从这里取规则。本模块不 import api 或 pipeline。

规则：本地根目录名固定用任务名，``content_path`` 恒为 ``<root>/<category>/<task.name>``。
多文件时它是目录，单文件时它是文件本身。
"""

from __future__ import annotations

import posixpath
from collections.abc import Sequence

from bridge.db.repo import ManifestFile, Task


def is_single_file(task: Task, manifest: Sequence[ManifestFile]) -> bool:
    """清单恰好一个文件且 rel_path 等于任务名；没有清单按多文件处理。"""
    return len(manifest) == 1 and manifest[0].rel_path == task.name


def save_path(root: str, category: str) -> str:
    return posixpath.join(root.rstrip("/") or "/", category)


def content_path(root: str, task: Task, manifest: Sequence[ManifestFile]) -> str:
    # 多文件和单文件都是 save_path/<task.name>，始终不等于 save_path。
    del manifest
    return posixpath.join(save_path(root, task.category), task.name)
