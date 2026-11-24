"""SQLite schema（6 张表）。lease 存在 kv 表，不另建表。"""

from __future__ import annotations

SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS tasks (
    hash           TEXT PRIMARY KEY,
    category       TEXT NOT NULL,
    name           TEXT NOT NULL,
    state          TEXT NOT NULL,
    source         TEXT NOT NULL,
    added_at       TEXT NOT NULL,
    submitted_at   TEXT,
    cloud_file_id  TEXT,
    cloud_size     INTEGER,
    cloud_percent  REAL NOT NULL DEFAULT 0,
    local_bytes    INTEGER NOT NULL DEFAULT 0,
    error          TEXT,
    retries        TEXT NOT NULL DEFAULT '{}',
    fallback       TEXT NOT NULL DEFAULT 'none',
    client_deleted INTEGER NOT NULL DEFAULT 0,
    magnet         TEXT NOT NULL DEFAULT ''
);
CREATE INDEX IF NOT EXISTS idx_tasks_state ON tasks(state);
CREATE INDEX IF NOT EXISTS idx_tasks_category ON tasks(category);

CREATE TABLE IF NOT EXISTS task_files (
    hash        TEXT NOT NULL REFERENCES tasks(hash),
    rel_path    TEXT NOT NULL,
    size        INTEGER NOT NULL,
    sha1        TEXT NOT NULL,
    pickcode    TEXT NOT NULL,
    local_bytes INTEGER NOT NULL DEFAULT 0,
    verified    INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (hash, rel_path)
);

CREATE TABLE IF NOT EXISTS torrent_meta (
    hash     TEXT NOT NULL REFERENCES tasks(hash),
    rel_path TEXT NOT NULL,
    size     INTEGER NOT NULL,
    PRIMARY KEY (hash, rel_path)
);

CREATE TABLE IF NOT EXISTS cloud_index (
    hash          TEXT PRIMARY KEY,
    cloud_file_id TEXT NOT NULL,
    root_name     TEXT NOT NULL,
    total_size    INTEGER NOT NULL,
    completed_at  TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS events (
    id         INTEGER PRIMARY KEY AUTOINCREMENT,
    hash       TEXT NOT NULL REFERENCES tasks(hash),
    at         TEXT NOT NULL,
    from_state TEXT,
    to_state   TEXT NOT NULL,
    reason     TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_events_hash ON events(hash, id);

CREATE TABLE IF NOT EXISTS kv (
    key   TEXT PRIMARY KEY,
    value TEXT NOT NULL
);
"""
