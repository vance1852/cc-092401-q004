from __future__ import annotations

import sqlite3
import tempfile
import unittest
from pathlib import Path

from robot_trials.storage import connect, initialize, inspect_schema, transaction


# v2 时代的 exclusion_requests 结构：没有独立的撤销列，旧实现的撤销
# 会把 reviewed_at/review_note 覆盖成撤销时间与撤销理由。
LEGACY_V2_SCHEMA = """
CREATE TABLE schema_meta (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL
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
INSERT INTO schema_meta(key,value) VALUES('schema_version','2');
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

    def _legacy_database(self, directory: str) -> Path:
        """构造一个 v2 时代的库：一条被旧撤销覆盖过的记录和一条正常批准记录。"""

        path = Path(directory) / "legacy.sqlite3"
        connection = sqlite3.connect(path, isolation_level=None)
        connection.executescript(LEGACY_V2_SCHEMA)
        connection.executescript("""
            CREATE TABLE audit_events (
                event_id INTEGER PRIMARY KEY AUTOINCREMENT,
                entity_type TEXT NOT NULL,
                entity_id TEXT NOT NULL,
                event_type TEXT NOT NULL,
                actor_id TEXT NOT NULL,
                payload_json TEXT NOT NULL,
                created_at TEXT NOT NULL
            );
            INSERT INTO exclusion_requests
                (exclusion_id,observation_id,status,reason,requested_by,requested_at,
                 reviewed_by,reviewed_at,review_note)
            VALUES
                (1,101,'revoked','现场记录失效','operator','2026-09-20T08:00:00Z',
                 'stat','2026-09-21T09:30:00Z','已找回原始记录'),
                (2,102,'approved','传感器离线','operator','2026-09-20T08:05:00Z',
                 'stat','2026-09-20T09:00:00Z','证据充分');
            INSERT INTO audit_events(entity_type,entity_id,event_type,actor_id,payload_json,created_at)
            VALUES
                ('exclusion','1','exclusion.approved','stat',
                 '{"note":"原始批准意见"}','2026-09-20T09:00:00Z'),
                ('observation','101','exclusion.revoked','operator',
                 '{"exclusion_id":1,"reason":"已找回原始记录"}','2026-09-21T09:30:00Z');
        """)
        connection.close()
        return path

    def test_migration_restores_legacy_revocation_from_audit_chain(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = self._legacy_database(directory)
            connection = connect(path)
            try:
                initialize(connection)
                summary = inspect_schema(connection)
                self.assertEqual(summary["schema_version"], "3")
                self.assertEqual(summary["missing_tables"], [])
                columns = {
                    row["name"]
                    for row in connection.execute("PRAGMA table_info(exclusion_requests)")
                }
                self.assertTrue({"revoked_by", "revoked_at", "revoke_reason"} <= columns)
                restored = connection.execute(
                    "SELECT * FROM exclusion_requests WHERE exclusion_id=1"
                ).fetchone()
                # 批准事实依据审计链重建，撤销事实独立成列，两者可区分。
                self.assertEqual(restored["status"], "revoked")
                self.assertEqual(restored["reviewed_by"], "stat")
                self.assertEqual(restored["reviewed_at"], "2026-09-20T09:00:00Z")
                self.assertEqual(restored["review_note"], "原始批准意见")
                self.assertEqual(restored["revoked_by"], "operator")
                self.assertEqual(restored["revoked_at"], "2026-09-21T09:30:00Z")
                self.assertEqual(restored["revoke_reason"], "已找回原始记录")
                # 正常批准记录不受迁移影响。
                untouched = connection.execute(
                    "SELECT * FROM exclusion_requests WHERE exclusion_id=2"
                ).fetchone()
                self.assertEqual(untouched["reviewed_at"], "2026-09-20T09:00:00Z")
                self.assertEqual(untouched["review_note"], "证据充分")
                self.assertIsNone(untouched["revoked_at"])
                # 迁移后保护触发器立即生效。
                with self.assertRaises(sqlite3.IntegrityError):
                    connection.execute(
                        "UPDATE exclusion_requests SET review_note='伪造' WHERE exclusion_id=2"
                    )
                # 重复初始化幂等，不再改动已修复的数据。
                initialize(connection)
                again = connection.execute(
                    "SELECT * FROM exclusion_requests WHERE exclusion_id=1"
                ).fetchone()
                self.assertEqual(again["reviewed_at"], "2026-09-20T09:00:00Z")
                self.assertEqual(again["revoked_at"], "2026-09-21T09:30:00Z")
            finally:
                connection.close()

    def test_migration_without_audit_evidence_keeps_facts_honest(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "noaudit.sqlite3"
            connection = sqlite3.connect(path, isolation_level=None)
            connection.executescript(LEGACY_V2_SCHEMA)
            connection.execute(
                "INSERT INTO exclusion_requests"
                "(observation_id,status,reason,requested_by,requested_at,"
                " reviewed_by,reviewed_at,review_note) "
                "VALUES(101,'revoked','现场记录失效','operator','2026-09-20T08:00:00Z',"
                "'stat','2026-09-21T09:30:00Z','已找回原始记录')"
            )
            connection.close()
            connection = connect(path)
            try:
                initialize(connection)
                row = connection.execute("SELECT * FROM exclusion_requests").fetchone()
                # 无审计证据时不伪造批准时间与意见；撤销痕迹完整保留在撤销列。
                self.assertEqual(row["status"], "revoked")
                self.assertEqual(row["reviewed_by"], "stat")
                self.assertIsNone(row["reviewed_at"])
                self.assertIsNone(row["review_note"])
                self.assertEqual(row["revoked_at"], "2026-09-21T09:30:00Z")
                self.assertEqual(row["revoke_reason"], "已找回原始记录")
            finally:
                connection.close()


if __name__ == "__main__":
    unittest.main()
