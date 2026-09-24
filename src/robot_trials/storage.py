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
    revoke_reason TEXT
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

REQUIRED_TABLES = frozenset({
    "schema_meta", "protocol_catalog", "users", "robots", "builds", "batches",
    "observations", "idempotency_keys", "exclusion_requests", "analysis_jobs",
    "analyses", "decisions", "audit_events",
})

# 排除事实的保护触发器单独维护：必须在 v2->v3 旧数据修复完成之后再创建，
# 否则修复过程中移动旧撤销数据会被“复核事实不可变”触发器拦截。
PROTECTION_TRIGGERS = (
    "CREATE TRIGGER IF NOT EXISTS exclusion_review_immutable_update "
    "BEFORE UPDATE OF reviewed_by, reviewed_at, review_note ON exclusion_requests "
    "WHEN old.reviewed_at IS NOT NULL AND ("
    "new.reviewed_by IS NOT old.reviewed_by "
    "OR new.reviewed_at IS NOT old.reviewed_at "
    "OR new.review_note IS NOT old.review_note) "
    "BEGIN "
    "SELECT RAISE(ABORT, 'review facts are immutable and must not be overwritten'); "
    "END",
    "CREATE TRIGGER IF NOT EXISTS exclusion_no_delete "
    "BEFORE DELETE ON exclusion_requests "
    "BEGIN SELECT RAISE(ABORT, 'exclusion facts must never be deleted'); END",
)

# ALTER TABLE ADD COLUMN 在部分 SQLite 构建下不接受 REFERENCES，
# 外键仅在新建库的规范模式中声明；迁移列保持纯类型即可，写入方始终校验用户。
_MIGRATION_COLUMNS = {
    "revoked_by": "TEXT",
    "revoked_at": "TEXT",
    "revoke_reason": "TEXT",
}


def _table_columns(connection: sqlite3.Connection, table: str) -> set[str]:
    return {row["name"] for row in connection.execute(f"PRAGMA table_info({table})")}


def _table_exists(connection: sqlite3.Connection, table: str) -> bool:
    return connection.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (table,)
    ).fetchone() is not None


def _legacy_revocation_events(connection: sqlite3.Connection) -> dict[int, sqlite3.Row]:
    """旧实现把撤销事件挂在观测实体下，payload 里带有 exclusion_id。"""

    events: dict[int, sqlite3.Row] = {}
    rows = connection.execute(
        "SELECT entity_id,payload_json,actor_id,created_at FROM audit_events "
        "WHERE entity_type='observation' AND event_type='exclusion.revoked'"
    ).fetchall()
    for row in rows:
        try:
            payload = json.loads(row["payload_json"])
            events[int(payload["exclusion_id"])] = row
        except (ValueError, KeyError, TypeError):
            continue
    return events


def _repair_legacy_revocations(connection: sqlite3.Connection) -> None:
    """把升级前被撤销覆盖的痕迹恢复为“批准/撤销”两个独立事实。

    优先依据不可变的审计事件重建原批准证据与撤销人；审计缺失时不伪造，
    仅把残留在复核列中的撤销痕迹搬到撤销列（reviewed_by 在旧实现中未被
    覆盖，仍是真实的批准人，予以保留）。
    """

    has_audit = _table_exists(connection, "audit_events")
    revocation_events = _legacy_revocation_events(connection) if has_audit else {}
    legacy_rows = connection.execute(
        "SELECT exclusion_id,reviewed_by,reviewed_at,review_note FROM exclusion_requests "
        "WHERE status='revoked' AND revoked_at IS NULL AND reviewed_at IS NOT NULL"
    ).fetchall()
    for legacy in legacy_rows:
        exclusion_id = legacy["exclusion_id"]
        approval = None
        revocation = revocation_events.get(exclusion_id) if has_audit else None
        if has_audit and revocation is not None:
            approval = connection.execute(
                "SELECT actor_id,created_at,payload_json FROM audit_events "
                "WHERE entity_type='exclusion' AND entity_id=? AND event_type='exclusion.approved' "
                "ORDER BY event_id LIMIT 1",
                (str(exclusion_id),),
            ).fetchone()
        if approval is not None and revocation is not None:
            try:
                note = json.loads(approval["payload_json"]).get("note")
                reason = json.loads(revocation["payload_json"]).get("reason", legacy["review_note"])
            except (ValueError, AttributeError):
                note, reason = None, legacy["review_note"]
            connection.execute(
                "UPDATE exclusion_requests SET reviewed_by=?,reviewed_at=?,review_note=?,"
                "revoked_by=?,revoked_at=?,revoke_reason=? WHERE exclusion_id=?",
                (
                    approval["actor_id"], approval["created_at"], note,
                    revocation["actor_id"], revocation["created_at"], reason, exclusion_id,
                ),
            )
        else:
            # 无审计证据：撤销时间/理由来自残留列，批准时间与意见已不可考，置空。
            connection.execute(
                "UPDATE exclusion_requests SET revoke_reason=review_note,revoked_at=reviewed_at,"
                "review_note=NULL,reviewed_at=NULL WHERE exclusion_id=?",
                (exclusion_id,),
            )


def _migrate(connection: sqlite3.Connection) -> None:
    """把既有数据库安全升级到当前模式，不删除或改写任何已记录事实。"""

    with transaction(connection, immediate=True):
        columns = _table_columns(connection, "exclusion_requests")
        for column, declaration in _MIGRATION_COLUMNS.items():
            if column not in columns:
                connection.execute(
                    f"ALTER TABLE exclusion_requests ADD COLUMN {column} {declaration}"
                )
        # 升级前的旧实现曾把撤销理由/时间覆盖到 review_note/reviewed_at，
        # 先依据审计链修复旧数据，再安装触发器，避免修复被“复核事实不可变”拦截。
        _repair_legacy_revocations(connection)
        # 旧库可能缺少保护触发器（例如手工建库），幂等补齐。
        for statement in PROTECTION_TRIGGERS:
            connection.execute(statement)
        connection.execute(
            "INSERT INTO schema_meta(key, value) VALUES('schema_version', ?) "
            "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
            (str(SCHEMA_VERSION),),
        )


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


def initialize(connection: sqlite3.Connection) -> None:
    """初始化基础资料表，重复执行不改变已有数据。

    建表脚本对新库直接给出当前结构；随后的升级步骤全部以“列是否存在、
    数据是否仍是旧形态”为前提，在已是当前版本的库上是空操作，因此新库、
    旧库和重复初始化共用同一条幂等路径。
    """

    connection.executescript(SCHEMA_SQL)
    _migrate(connection)


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
