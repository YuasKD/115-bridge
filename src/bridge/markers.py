"""跨层共享的字符串标记（中立模块，pipeline 与 api 都从这里导入，互不依赖）。"""

from __future__ import annotations

CLEANUP_ERROR_PREFIX = "local cleanup failed"
"""``tasks.error`` 标记：本地清理出错，等 Arr 重试。"""
