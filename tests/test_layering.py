"""模块分层：pipeline 不依赖 api，api 不依赖 pipeline 的实现模块。"""

from __future__ import annotations

import ast
from pathlib import Path

import bridge

SRC = Path(bridge.__file__).parent


def _imports(path: Path) -> set[str]:
    out: set[str] = set()
    for node in ast.walk(ast.parse(path.read_text(encoding="utf-8"))):
        if isinstance(node, ast.Import):
            out.update(a.name for a in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module and node.level == 0:
            out.add(node.module)
            out.update(f"{node.module}.{a.name}" for a in node.names)
    return out


def _under(names: set[str], pkg: str) -> set[str]:
    return {n for n in names if n == pkg or n.startswith(pkg + ".")}


def test_pipeline_does_not_import_api() -> None:
    files = sorted((SRC / "pipeline").glob("*.py"))
    assert files
    bad = {f.name: _under(_imports(f), "bridge.api") for f in files}
    assert {k: v for k, v in bad.items() if v} == {}


def test_api_does_not_import_pipeline() -> None:
    files = sorted((SRC / "api").glob("*.py"))
    assert files
    bad = {f.name: _under(_imports(f), "bridge.pipeline") for f in files}
    assert {k: v for k, v in bad.items() if v} == {}


def test_no_relative_imports_in_src() -> None:
    """选择「禁止相对导入」而不是解析：上面的 ``_imports`` 只认绝对导入，禁掉相对导入即可保证
    它不会漏检（项目本来也统一用绝对导入）。"""
    offenders: list[str] = []
    for path in sorted(SRC.rglob("*.py")):
        for node in ast.walk(ast.parse(path.read_text(encoding="utf-8"))):
            if isinstance(node, ast.ImportFrom) and node.level > 0:
                offenders.append(f"{path.relative_to(SRC)}:{node.lineno}")
    assert offenders == []


NEUTRAL_MODULES = ("markers", "kv_keys", "missing_protocol", "progress_protocol")


def test_neutral_modules_do_not_import_api_or_pipeline() -> None:
    for name in NEUTRAL_MODULES:
        path = SRC / f"{name}.py"
        assert path.is_file(), path
        names = _imports(path)
        assert _under(names, "bridge.api") == set(), name
        assert _under(names, "bridge.pipeline") == set(), name
