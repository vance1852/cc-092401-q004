from __future__ import annotations

import sqlite3
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path

from robot_trials.clock import FrozenClock
from robot_trials.service import TrialService
from robot_trials.storage import connect, initialize, inspect_schema, transaction


# 升级前的 v2 排除表：没有 revoked_* 列，旧代码撤销时会把撤销信息
# 覆盖进 reviewed_at/review_note。
V2_EXCLUSION_DDL = """
CREATE TABLE schema_meta (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL
);
CREATE TABLE users (
    user_id TEXT PRIMARY KEY,
    display_name TEXT NOT NULL,
    role TEXT NOT NULL,
    active INTEGER NOT NULL DEFAULT 1
);
CREATE TABLE batches (
    batch_id TEXT PRIMARY KEY,
    state TEXT NOT NULL
);
CREATE TABLE observations (
    observation_id INTEGER PRIMARY KEY,
    batch_id TEXT NOT NULL
);
CREATE TABLE audit_events (
    event_id INTEGER PRIMARY KEY AUTOINCREMENT,
    entity_type TEXT NOT NULL,
    entity_id TEXT NOT NULL,
    event_type TEXT NOT NULL,
    actor_id TEXT NOT NULL,
    payload_json TEXT NOT NULL,
    created_at TEXT NOT NULL
);
CREATE TABLE exclusion_requests (
    exclusion_id INTEGER PRIMARY KEY AUTOINCREMENT,
    observation_id INTEGER NOT NULL,
    status TEXT NOT NULL CHECK (status IN ('pending', 'approved', 'rejected', 'revoked')),
    reason TEXT NOT NULL,
    requested_by TEXT NOT NULL,
    requested_at TEXT NOT NULL,
    reviewed_by TEXT,
    reviewed_at TEXT,
    review_note TEXT
);
"""


class StorageTests(unittest.TestCase):
    def test_initialize_is_repeatable(self) -> None:
        connection = sqlite3.connect(":memory:", isolation_level=None)
        connection.row_factory = sqlite3.Row
        try:
            initialize(connection)
            initialize(connection)
            summary = inspect_schema(connection)
        finally:
            connection.close()
        self.assertEqual(summary["missing_tables"], [])
        self.assertEqual(summary["schema_version"], "3")

    def test_transaction_rolls_back_on_error(self) -> None:
        connection = sqlite3.connect(":memory:", isolation_level=None)
        connection.execute("CREATE TABLE items(value TEXT NOT NULL)")
        with self.assertRaises(RuntimeError):
            with transaction(connection):
                connection.execute("INSERT INTO items(value) VALUES('x')")
                raise RuntimeError("stop")
        count = connection.execute("SELECT count(*) FROM items").fetchone()[0]
        connection.close()
        self.assertEqual(count, 0)

    def test_connect_enables_foreign_keys(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            connection = connect(Path(directory) / "test.sqlite3")
            try:
                self.assertEqual(connection.execute("PRAGMA foreign_keys").fetchone()[0], 1)
            finally:
                connection.close()


class MigrationTests(unittest.TestCase):
    def _build_v2_database(self, connection: sqlite3.Connection) -> None:
        connection.executescript(V2_EXCLUSION_DDL)
        connection.execute(
            "INSERT INTO schema_meta(key,value) VALUES('schema_version','2')"
        )
        for user_id, role in (
            ("op-1", "operator"), ("op-2", "operator"), ("op-3", "operator"),
            ("op-4", "operator"), ("stat-1", "statistician"), ("stat-2", "statistician"),
        ):
            connection.execute(
                "INSERT INTO users(user_id,display_name,role) VALUES(?,?,?)",
                (user_id, user_id, role),
            )
        connection.execute("INSERT INTO batches(batch_id,state) VALUES('batch-1','running')")
        for observation_id in (1, 2, 3, 4):
            connection.execute(
                "INSERT INTO observations(observation_id,batch_id) VALUES(?,'batch-1')",
                (observation_id,),
            )
        connection.execute(
            "INSERT INTO exclusion_requests(observation_id,status,reason,requested_by,requested_at,"
            "reviewed_by,reviewed_at,review_note) VALUES(1,'approved','记录失效','op-1','2026-09-20T08:00:00Z',"
            "'stat-1','2026-09-20T09:00:00Z','证据充分')"
        )
        # 旧版撤销：撤销时间与理由覆盖了复核时间与意见，复核人无法区分但仍被保留。
        connection.execute(
            "INSERT INTO exclusion_requests(observation_id,status,reason,requested_by,requested_at,"
            "reviewed_by,reviewed_at,review_note) VALUES(2,'revoked','记录失效','op-2','2026-09-20T08:00:00Z',"
            "'stat-2','2026-09-21T10:30:00Z','原始记录已找回')"
        )
        connection.execute(
            "INSERT INTO exclusion_requests(observation_id,status,reason,requested_by,requested_at) "
            "VALUES(3,'pending','待复核','op-3','2026-09-22T08:00:00Z')"
        )
        connection.execute(
            "INSERT INTO exclusion_requests(observation_id,status,reason,requested_by,requested_at,"
            "reviewed_by,reviewed_at,review_note) VALUES(4,'rejected','证据不足','op-4','2026-09-22T09:00:00Z',"
            "'stat-1','2026-09-22T10:00:00Z','不满足排除条件')"
        )
        # 旧版撤销写入的审计事件：实体是观测，载荷带 exclusion_id 与撤销理由。
        connection.execute(
            "INSERT INTO audit_events(entity_type,entity_id,event_type,actor_id,payload_json,created_at) "
            "VALUES('observation','2','exclusion.revoked','op-2',"
            "'{\"exclusion_id\":2,\"reason\":\"原始记录已找回\"}','2026-09-21T10:30:00Z')"
        )

    def test_v2_database_is_upgraded_and_legacy_facts_are_rehomed(self) -> None:
        connection = sqlite3.connect(":memory:", isolation_level=None)
        connection.row_factory = sqlite3.Row
        try:
            self._build_v2_database(connection)
            initialize(connection)
            summary = inspect_schema(connection)
            self.assertEqual(summary["schema_version"], "3")
            columns = {
                row["name"]
                for row in connection.execute("PRAGMA table_info(exclusion_requests)").fetchall()
            }
            self.assertGreaterEqual(columns, {"revoked_by", "revoked_at", "revoke_reason"})

            approved = connection.execute(
                "SELECT * FROM exclusion_requests WHERE observation_id=1"
            ).fetchone()
            self.assertEqual(approved["status"], "approved")
            self.assertEqual(approved["reviewed_by"], "stat-1")
            self.assertEqual(approved["reviewed_at"], "2026-09-20T09:00:00Z")
            self.assertEqual(approved["review_note"], "证据充分")
            self.assertIsNone(approved["revoked_at"])

            revoked = connection.execute(
                "SELECT * FROM exclusion_requests WHERE observation_id=2"
            ).fetchone()
            self.assertEqual(revoked["status"], "revoked")
            # 被旧代码覆盖的撤销信息确定性归位到撤销列。
            self.assertEqual(revoked["revoked_at"], "2026-09-21T10:30:00Z")
            self.assertEqual(revoked["revoke_reason"], "原始记录已找回")
            # 真实复核人保留；丢失的复核时间与意见不伪造，明确置空。
            self.assertEqual(revoked["reviewed_by"], "stat-2")
            self.assertIsNone(revoked["reviewed_at"])
            self.assertIsNone(revoked["review_note"])
            # 撤销人从追加式审计链恢复。
            self.assertEqual(revoked["revoked_by"], "op-2")

            pending = connection.execute(
                "SELECT status,reviewed_at,revoked_at FROM exclusion_requests WHERE observation_id=3"
            ).fetchone()
            self.assertEqual(pending["status"], "pending")
            self.assertIsNone(pending["reviewed_at"])
            self.assertIsNone(pending["revoked_at"])

            rejected = connection.execute(
                "SELECT reviewed_at,review_note,revoked_at FROM exclusion_requests WHERE observation_id=4"
            ).fetchone()
            self.assertEqual(rejected["review_note"], "不满足排除条件")
            self.assertIsNone(rejected["revoked_at"])
        finally:
            connection.close()

    def test_migration_is_idempotent(self) -> None:
        connection = sqlite3.connect(":memory:", isolation_level=None)
        connection.row_factory = sqlite3.Row
        try:
            self._build_v2_database(connection)
            initialize(connection)
            initialize(connection)
            self.assertEqual(inspect_schema(connection)["schema_version"], "3")
            revoked = connection.execute(
                "SELECT revoked_at,revoke_reason,reviewed_at,review_note "
                "FROM exclusion_requests WHERE status='revoked'"
            ).fetchone()
            self.assertEqual(revoked["revoke_reason"], "原始记录已找回")
            self.assertIsNone(revoked["reviewed_at"])
        finally:
            connection.close()

    def test_migration_persists_on_disk(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "v2.sqlite3"
            first = sqlite3.connect(path, isolation_level=None)
            try:
                self._build_v2_database(first)
            finally:
                first.close()
            upgraded = connect(path)
            try:
                initialize(upgraded)
                self.assertEqual(inspect_schema(upgraded)["schema_version"], "3")
            finally:
                upgraded.close()

    def test_revoke_on_upgraded_database_keeps_legacy_review_facts(self) -> None:
        connection = sqlite3.connect(":memory:", isolation_level=None)
        connection.row_factory = sqlite3.Row
        try:
            self._build_v2_database(connection)
            clock = FrozenClock(datetime(2026, 9, 24, 12, 0, tzinfo=timezone.utc))
            service = TrialService(connection, clock)
            result = service.revoke_exclusion("op-1", 1, "原始记录已找回")
            self.assertEqual(result["status"], "revoked")
            row = connection.execute(
                "SELECT reviewed_by,reviewed_at,review_note,revoked_by,revoked_at,revoke_reason "
                "FROM exclusion_requests WHERE exclusion_id=1"
            ).fetchone()
            # 升级前写入的复核事实在撤销后原样保留。
            self.assertEqual(row["reviewed_by"], "stat-1")
            self.assertEqual(row["reviewed_at"], "2026-09-20T09:00:00Z")
            self.assertEqual(row["review_note"], "证据充分")
            self.assertEqual(row["revoked_by"], "op-1")
            self.assertEqual(row["revoked_at"], "2026-09-24T12:00:00Z")
            self.assertEqual(row["revoke_reason"], "原始记录已找回")
        finally:
            connection.close()


class ReviewFactImmutabilityTests(unittest.TestCase):
    def setUp(self) -> None:
        self.connection = sqlite3.connect(":memory:", isolation_level=None)
        self.connection.row_factory = sqlite3.Row
        initialize(self.connection)
        # 本测试类只验证复核事实触发器，不构造 observations/users 外键链。
        self.connection.execute("PRAGMA foreign_keys = OFF")
        self.connection.execute(
            "INSERT INTO exclusion_requests(observation_id,status,reason,requested_by,requested_at,"
            "reviewed_by,reviewed_at,review_note) VALUES(1,'approved','记录失效','op-1','2026-09-20T08:00:00Z',"
            "'stat-1','2026-09-20T09:00:00Z','证据充分')"
        )

    def tearDown(self) -> None:
        self.connection.close()

    def test_review_facts_cannot_be_rewritten(self) -> None:
        for statement, params in (
            ("UPDATE exclusion_requests SET reviewed_by='stat-2' WHERE exclusion_id=1", ()),
            ("UPDATE exclusion_requests SET reviewed_at='2026-09-21T10:30:00Z' WHERE exclusion_id=1", ()),
            ("UPDATE exclusion_requests SET review_note='被篡改' WHERE exclusion_id=1", ()),
            (
                "UPDATE exclusion_requests SET review_note=?,reviewed_at=? "
                "WHERE exclusion_id=1 AND status='approved'",
                ("撤销理由", "2026-09-21T10:30:00Z"),
            ),
        ):
            with self.subTest(statement=statement):
                with self.assertRaises(sqlite3.IntegrityError):
                    self.connection.execute(statement, params)
        row = self.connection.execute(
            "SELECT reviewed_by,reviewed_at,review_note FROM exclusion_requests WHERE exclusion_id=1"
        ).fetchone()
        self.assertEqual(tuple(row), ("stat-1", "2026-09-20T09:00:00Z", "证据充分"))

    def test_reviewed_rows_cannot_be_deleted(self) -> None:
        with self.assertRaises(sqlite3.IntegrityError):
            self.connection.execute("DELETE FROM exclusion_requests WHERE exclusion_id=1")
        self.assertEqual(
            self.connection.execute("SELECT count(*) FROM exclusion_requests").fetchone()[0], 1
        )

    def test_revoke_columns_can_be_written_without_touching_review(self) -> None:
        self.connection.execute(
            "UPDATE exclusion_requests SET status='revoked',revoked_by='op-1',"
            "revoked_at='2026-09-21T10:30:00Z',revoke_reason='原始记录已找回' WHERE exclusion_id=1"
        )
        row = self.connection.execute(
            "SELECT status,reviewed_by,reviewed_at,review_note,revoked_by,revoked_at,revoke_reason "
            "FROM exclusion_requests WHERE exclusion_id=1"
        ).fetchone()
        self.assertEqual(row["status"], "revoked")
        self.assertEqual(row["reviewed_by"], "stat-1")
        self.assertEqual(row["reviewed_at"], "2026-09-20T09:00:00Z")
        self.assertEqual(row["review_note"], "证据充分")
        self.assertEqual(row["revoked_by"], "op-1")
        self.assertEqual(row["revoke_reason"], "原始记录已找回")

    def test_unreviewed_row_still_updatable(self) -> None:
        self.connection.execute(
            "INSERT INTO exclusion_requests(observation_id,status,reason,requested_by,requested_at) "
            "VALUES(2,'pending','待复核','op-2','2026-09-22T08:00:00Z')"
        )
        self.connection.execute(
            "UPDATE exclusion_requests SET status='rejected',reviewed_by='stat-1',"
            "reviewed_at='2026-09-22T09:00:00Z',review_note='驳回' WHERE exclusion_id=2"
        )
        row = self.connection.execute(
            "SELECT status,review_note FROM exclusion_requests WHERE exclusion_id=2"
        ).fetchone()
        self.assertEqual(tuple(row), ("rejected", "驳回"))


if __name__ == "__main__":
    unittest.main()
