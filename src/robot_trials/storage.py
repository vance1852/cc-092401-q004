"""统计准入服务的 SQLite 模式与事务辅助。"""

from __future__ import annotations

import contextlib
import json
import sqlite3
from collections.abc import Iterator
from pathlib import Path


SCHEMA_VERSION = 3

SCHEMA_SQL = """
PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS schema_meta (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS protocol_catalog (
    protocol_id TEXT NOT NULL,
    version INTEGER NOT NULL CHECK (version > 0),
    title TEXT NOT NULL,
    task_family TEXT NOT NULL,
    canonical_json TEXT NOT NULL,
    content_sha256 TEXT NOT NULL CHECK (length(content_sha256) = 64),
    created_at TEXT NOT NULL,
    PRIMARY KEY (protocol_id, version),
    UNIQUE (content_sha256)
);

CREATE TABLE IF NOT EXISTS users (
    user_id TEXT PRIMARY KEY,
    display_name TEXT NOT NULL,
    role TEXT NOT NULL CHECK (role IN ('operator', 'statistician', 'approver', 'auditor')),
    active INTEGER NOT NULL DEFAULT 1 CHECK (active IN (0, 1))
);

CREATE TABLE IF NOT EXISTS robots (
    robot_id TEXT PRIMARY KEY,
    model_name TEXT NOT NULL,
    vendor TEXT NOT NULL,
    created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS builds (
    build_id TEXT PRIMARY KEY,
    robot_id TEXT NOT NULL REFERENCES robots(robot_id),
    version TEXT NOT NULL,
    content_sha256 TEXT NOT NULL CHECK (length(content_sha256) = 64),
    created_at TEXT NOT NULL,
    UNIQUE (robot_id, version),
    UNIQUE (content_sha256)
);

CREATE TABLE IF NOT EXISTS batches (
    batch_id TEXT PRIMARY KEY,
    protocol_id TEXT NOT NULL,
    protocol_version INTEGER NOT NULL,
    build_id TEXT NOT NULL REFERENCES builds(build_id),
    state TEXT NOT NULL CHECK (state IN ('draft', 'running', 'sealed', 'analyzing', 'analyzed', 'decided')),
    revision INTEGER NOT NULL DEFAULT 1 CHECK (revision > 0),
    created_by TEXT NOT NULL REFERENCES users(user_id),
    created_at TEXT NOT NULL,
    started_at TEXT,
    sealed_at TEXT,
    FOREIGN KEY (protocol_id, protocol_version) REFERENCES protocol_catalog(protocol_id, version)
);

CREATE TABLE IF NOT EXISTS observations (
    observation_id INTEGER PRIMARY KEY AUTOINCREMENT,
    batch_id TEXT NOT NULL REFERENCES batches(batch_id),
    source_batch TEXT NOT NULL,
    source_row TEXT NOT NULL,
    robot_id TEXT NOT NULL REFERENCES robots(robot_id),
    stratum_key TEXT NOT NULL,
    observed_at TEXT NOT NULL,
    metrics_json TEXT NOT NULL,
    content_sha256 TEXT NOT NULL CHECK (length(content_sha256) = 64),
    imported_by TEXT NOT NULL REFERENCES users(user_id),
    imported_at TEXT NOT NULL,
    UNIQUE (batch_id, source_batch, source_row)
);

CREATE TABLE IF NOT EXISTS idempotency_keys (
    scope TEXT NOT NULL,
    key TEXT NOT NULL,
    request_sha256 TEXT NOT NULL CHECK (length(request_sha256) = 64),
    response_json TEXT NOT NULL,
    created_at TEXT NOT NULL,
    PRIMARY KEY (scope, key)
);

CREATE TABLE IF NOT EXISTS exclusion_requests (
    exclusion_id INTEGER PRIMARY KEY AUTOINCREMENT,
    observation_id INTEGER NOT NULL REFERENCES observations(observation_id),
    status TEXT NOT NULL CHECK (status IN ('pending', 'approved', 'rejected', 'revoked')),
    reason TEXT NOT NULL,
    requested_by TEXT NOT NULL REFERENCES users(user_id),
    requested_at TEXT NOT NULL,
    reviewed_by TEXT REFERENCES users(user_id),
    reviewed_at TEXT,
    review_note TEXT,
    revoked_by TEXT REFERENCES users(user_id),
    revoked_at TEXT,
    revoke_reason TEXT,
    CHECK ((status = 'revoked') = (revoked_at IS NOT NULL))
);

CREATE UNIQUE INDEX IF NOT EXISTS one_open_exclusion_per_observation
ON exclusion_requests(observation_id)
WHERE status IN ('pending', 'approved');

CREATE TABLE IF NOT EXISTS analysis_jobs (
    job_id INTEGER PRIMARY KEY AUTOINCREMENT,
    batch_id TEXT NOT NULL REFERENCES batches(batch_id),
    batch_revision INTEGER NOT NULL,
    state TEXT NOT NULL CHECK (state IN ('queued', 'leased', 'succeeded', 'failed')),
    attempts INTEGER NOT NULL DEFAULT 0 CHECK (attempts >= 0),
    available_at TEXT NOT NULL,
    lease_owner TEXT,
    lease_expires_at TEXT,
    last_error TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    UNIQUE (batch_id, batch_revision)
);

CREATE TABLE IF NOT EXISTS analyses (
    analysis_id INTEGER PRIMARY KEY AUTOINCREMENT,
    batch_id TEXT NOT NULL REFERENCES batches(batch_id),
    batch_revision INTEGER NOT NULL,
    protocol_sha256 TEXT NOT NULL CHECK (length(protocol_sha256) = 64),
    input_sha256 TEXT NOT NULL CHECK (length(input_sha256) = 64),
    algorithm_version TEXT NOT NULL,
    seed INTEGER NOT NULL,
    result_json TEXT NOT NULL,
    created_by TEXT NOT NULL,
    created_at TEXT NOT NULL,
    UNIQUE (batch_id, batch_revision, input_sha256)
);

CREATE TABLE IF NOT EXISTS decisions (
    decision_id INTEGER PRIMARY KEY AUTOINCREMENT,
    batch_id TEXT NOT NULL REFERENCES batches(batch_id),
    analysis_id INTEGER NOT NULL REFERENCES analyses(analysis_id),
    decision TEXT NOT NULL CHECK (decision IN ('needs_more_data', 'approved', 'rejected')),
    reason TEXT NOT NULL,
    decided_by TEXT NOT NULL REFERENCES users(user_id),
    decided_at TEXT NOT NULL,
    UNIQUE (batch_id, analysis_id)
);

CREATE TABLE IF NOT EXISTS audit_events (
    event_id INTEGER PRIMARY KEY AUTOINCREMENT,
    entity_type TEXT NOT NULL,
    entity_id TEXT NOT NULL,
    event_type TEXT NOT NULL,
    actor_id TEXT NOT NULL,
    payload_json TEXT NOT NULL,
    created_at TEXT NOT NULL
);
"""

# 复核事实一旦写入即不可变：撤销只能写 revoked_* 列，任何改写或删除
# reviewed_by/reviewed_at/review_note 的尝试都在数据库层被拒绝。
TRIGGER_STATEMENTS = (
    """
    CREATE TRIGGER IF NOT EXISTS exclusion_review_facts_immutable
    BEFORE UPDATE ON exclusion_requests
    WHEN OLD.reviewed_by IS NOT NULL
       AND (NEW.reviewed_by IS NOT OLD.reviewed_by
            OR NEW.reviewed_at IS NOT OLD.reviewed_at
            OR NEW.review_note IS NOT OLD.review_note)
    BEGIN
        SELECT RAISE(ABORT, '复核事实不可改写');
    END
    """,
    """
    CREATE TRIGGER IF NOT EXISTS exclusion_review_facts_no_delete
    BEFORE DELETE ON exclusion_requests
    WHEN OLD.reviewed_by IS NOT NULL
    BEGIN
        SELECT RAISE(ABORT, '复核事实不可删除');
    END
    """,
)

REQUIRED_TABLES = frozenset({
    "schema_meta", "protocol_catalog", "users", "robots", "builds", "batches",
    "observations", "idempotency_keys", "exclusion_requests", "analysis_jobs",
    "analyses", "decisions", "audit_events",
})


def connect(path: str | Path) -> sqlite3.Connection:
    """打开连接并启用严格的事务与外键设置。"""

    connection = sqlite3.connect(str(path), isolation_level=None)
    connection.row_factory = sqlite3.Row
    connection.execute("PRAGMA foreign_keys = ON")
    connection.execute("PRAGMA busy_timeout = 5000")
    return connection


@contextlib.contextmanager
def transaction(connection: sqlite3.Connection, *, immediate: bool = False) -> Iterator[None]:
    """显式事务；异常时保证回滚。"""

    connection.execute("BEGIN IMMEDIATE" if immediate else "BEGIN")
    try:
        yield
    except BaseException:
        connection.rollback()
        raise
    else:
        connection.commit()


def _migrate_to_v3(connection: sqlite3.Connection) -> None:
    """把 v2 的排除记录升级到带独立撤销事实的结构。

    只处理缺少撤销列的旧表，可重复执行。旧代码撤销时会把撤销理由和撤销时间
    覆盖到 review_note/reviewed_at，因此对升级前已撤销的记录做确定性归位：
    这两列里的值移入 revoke_reason/revoked_at 并清空，恢复“原复核时间与意见
    已不可考”的真实状态；reviewed_by 记录的是真实复核人，予以保留。撤销人
    从未写入行内，但追加式审计链中的 exclusion.revoked 事件记录了操作人与
    时间，用它恢复 revoked_by/revoked_at；找不到对应事件的保持 NULL，不伪造。
    """

    columns = {
        row["name"]
        for row in connection.execute("PRAGMA table_info(exclusion_requests)").fetchall()
    }
    new_columns = {
        "revoked_by": "ALTER TABLE exclusion_requests ADD COLUMN revoked_by TEXT REFERENCES users(user_id)",
        "revoked_at": "ALTER TABLE exclusion_requests ADD COLUMN revoked_at TEXT",
        "revoke_reason": "ALTER TABLE exclusion_requests ADD COLUMN revoke_reason TEXT",
    }
    missing = [name for name in new_columns if name not in columns]
    if not missing:
        return
    for name in missing:
        connection.execute(new_columns[name])
    connection.execute(
        "UPDATE exclusion_requests SET "
        "revoked_at=reviewed_at, revoke_reason=review_note, "
        "reviewed_at=NULL, review_note=NULL "
        "WHERE status='revoked' AND revoked_at IS NULL"
    )
    events = connection.execute(
        "SELECT event_id,actor_id,payload_json,created_at FROM audit_events "
        "WHERE event_type='exclusion.revoked' ORDER BY event_id"
    ).fetchall()
    revoke_events: dict[int, sqlite3.Row] = {}
    for event in events:
        payload = json.loads(event["payload_json"])
        if not isinstance(payload, dict):
            continue
        exclusion_id = payload.get("exclusion_id")
        if isinstance(exclusion_id, int):
            revoke_events[exclusion_id] = event
    for exclusion_id, event in revoke_events.items():
        connection.execute(
            "UPDATE exclusion_requests SET revoked_by=?,revoked_at=? "
            "WHERE exclusion_id=? AND status='revoked'",
            (event["actor_id"], event["created_at"], exclusion_id),
        )


def initialize(connection: sqlite3.Connection) -> None:
    """初始化或升级数据库结构，重复执行不改变已有数据。"""

    connection.executescript(SCHEMA_SQL)
    with transaction(connection, immediate=True):
        _migrate_to_v3(connection)
        for statement in TRIGGER_STATEMENTS:
            connection.execute(statement)
        connection.execute(
            "INSERT INTO schema_meta(key, value) VALUES('schema_version', ?) "
            "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
            (str(SCHEMA_VERSION),),
        )


def inspect_schema(connection: sqlite3.Connection) -> dict[str, object]:
    """返回适合机器检查的数据库结构摘要。"""

    table_rows = connection.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' ORDER BY name"
    ).fetchall()
    tables = tuple(row["name"] for row in table_rows)
    version_row = connection.execute(
        "SELECT value FROM schema_meta WHERE key='schema_version'"
    ).fetchone()
    missing = sorted(REQUIRED_TABLES - set(tables))
    foreign_keys = connection.execute("PRAGMA foreign_keys").fetchone()[0]
    return {
        "tables": tables,
        "missing_tables": missing,
        "schema_version": None if version_row is None else version_row["value"],
        "foreign_keys": bool(foreign_keys),
    }
