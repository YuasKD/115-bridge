from __future__ import annotations

from datetime import UTC, datetime

from bridge import layout
from bridge.db.repo import ManifestFile, Source, Task, TaskState

NOW = datetime(2026, 1, 1, tzinfo=UTC)


def mk(name: str = "Movie", category: str = "radarr") -> Task:
    return Task(
        hash="ab" * 20,
        category=category,
        name=name,
        state=TaskState.QUEUED,
        source=Source.MAGNET,
        added_at=NOW,
    )


def test_is_single_file() -> None:
    t = mk("a.mkv")
    assert layout.is_single_file(t, [ManifestFile("a.mkv", 1)])
    assert not layout.is_single_file(t, [ManifestFile("other.mkv", 1)])
    assert not layout.is_single_file(t, [ManifestFile("a.mkv", 1), ManifestFile("b", 1)])
    assert not layout.is_single_file(t, [])


def test_paths() -> None:
    t = mk("Movie")
    assert layout.save_path("/data/115/", "radarr") == "/data/115/radarr"
    assert layout.content_path("/data/115", t, []) == "/data/115/radarr/Movie"
    one = mk("a.mkv")
    assert layout.content_path("/data/115", one, [ManifestFile("a.mkv", 1)]) == (
        "/data/115/radarr/a.mkv"
    )
