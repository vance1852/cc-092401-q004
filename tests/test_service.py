from __future__ import annotations

import json
import sqlite3
import unittest
from datetime import datetime, timezone
from pathlib import Path

from robot_trials.clock import FrozenClock
from robot_trials.errors import Conflict, Forbidden, InvalidState, ValidationFailed
from robot_trials.jsonio import load_json
from robot_trials.service import TrialService


ROOT = Path(__file__).resolve().parents[1]


class ServiceTests(unittest.TestCase):
    def setUp(self) -> None:
        self.connection = sqlite3.connect(":memory:", isolation_level=None)
        self.connection.row_factory = sqlite3.Row
        self.clock = FrozenClock(datetime(2026, 9, 24, 8, 0, tzinfo=timezone.utc))
        self.service = TrialService(self.connection, self.clock)
        for user_id, role in (
            ("operator", "operator"),
            ("stat", "statistician"),
            ("approver", "approver"),
            ("auditor", "auditor"),
        ):
            self.service.create_user(user_id, user_id, role)
        self.protocol = load_json(ROOT / "fixtures" / "demo_protocol.json")
        self.rows = [
            json.loads(line)
            for line in (ROOT / "fixtures" / "demo_observations.jsonl").read_text(encoding="utf-8").splitlines()
            if line.strip()
        ]
        self.service.register_robot("operator", "robot-a", "A 型", "厂商")
        self.service.register_build("operator", "build-a", "robot-a", "1.0", "b" * 64)
        self.service.publish_protocol("stat", self.protocol)
        self.service.create_batch("operator", "batch-a", "demo-delivery-v1", 1, "build-a")
        self.service.start_batch("operator", "batch-a", 1)

    def tearDown(self) -> None:
        self.connection.close()

    def test_complete_workflow(self) -> None:
        imported = self.service.import_observations("operator", "batch-a", "key-1", self.rows)
        self.assertEqual(imported["inserted"], 6)
        self.service.seal_batch("stat", "batch-a", 2)
        job = self.service.claim_job("worker", 30)
        analysis = self.service.complete_job("worker", job["job_id"], "stat")
        self.service.decide("approver", "batch-a", analysis["analysis_id"], "approved", "满足规则")
        report = self.service.report("auditor", "batch-a")
        self.assertEqual(report["batch"]["state"], "decided")
        self.assertEqual(report["analysis"]["result"]["conclusion"], "pass")

    def test_idempotent_replay_and_conflict(self) -> None:
        first = self.service.import_observations("operator", "batch-a", "key-1", self.rows)
        second = self.service.import_observations("operator", "batch-a", "key-1", self.rows)
        self.assertEqual(first, second)
        changed = [dict(item) for item in self.rows]
        changed[0] = dict(changed[0])
        changed[0]["metrics"] = dict(changed[0]["metrics"])
        changed[0]["metrics"]["completion_seconds"] = "99"
        with self.assertRaises(Conflict):
            self.service.import_observations("operator", "batch-a", "key-1", changed)
        count = self.connection.execute("SELECT count(*) FROM observations").fetchone()[0]
        self.assertEqual(count, 6)

    def test_import_rolls_back_when_one_source_row_duplicates(self) -> None:
        self.service.import_observations("operator", "batch-a", "key-1", self.rows[:1])
        with self.assertRaises(Conflict):
            self.service.import_observations("operator", "batch-a", "key-2", self.rows[:2])
        count = self.connection.execute("SELECT count(*) FROM observations").fetchone()[0]
        self.assertEqual(count, 1)

    def test_role_separation(self) -> None:
        with self.assertRaises(Forbidden):
            self.service.seal_batch("operator", "batch-a", 2)
        with self.assertRaises(Forbidden):
            self.service.report("operator", "batch-a")

    def _import_and_request(self, reason: str = "现场记录失效") -> tuple[int, int]:
        self.service.import_observations("operator", "batch-a", "key-1", self.rows)
        observation_id = self.connection.execute(
            "SELECT observation_id FROM observations ORDER BY observation_id LIMIT 1"
        ).fetchone()[0]
        requested = self.service.request_exclusion("operator", observation_id, reason)
        return observation_id, requested["exclusion_id"]

    def _approve_exclusion(self, exclusion_id: int, note: str = "证据充分") -> dict:
        self.clock.advance(seconds=10)
        return self.service.review_exclusion("stat", exclusion_id, True, note)

    def _review_snapshot(self, exclusion_id: int) -> tuple:
        return self.connection.execute(
            "SELECT reviewed_by,reviewed_at,review_note,revoked_by,revoked_at,revoke_reason,status "
            "FROM exclusion_requests WHERE exclusion_id=?",
            (exclusion_id,),
        ).fetchone()

    def test_revoke_records_separate_facts_and_preserves_review(self) -> None:
        observation_id, exclusion_id = self._import_and_request()
        self._approve_exclusion(exclusion_id, note="批准依据：编号 12 的物证照片")
        before = self._review_snapshot(exclusion_id)
        self.clock.advance(seconds=30)
        revoked = self.service.revoke_exclusion("operator", exclusion_id, "已找回原始记录")
        self.assertEqual(revoked["status"], "revoked")
        after = self._review_snapshot(exclusion_id)
        # 最初的 reviewed_by/reviewed_at/review_note 必须逐字节保留。
        self.assertEqual(after["reviewed_by"], "stat")
        self.assertEqual(after["reviewed_by"], before["reviewed_by"])
        self.assertEqual(after["reviewed_at"], before["reviewed_at"])
        self.assertEqual(after["review_note"], "批准依据：编号 12 的物证照片")
        self.assertEqual(after["review_note"], before["review_note"])
        # 撤销作为独立事实单独记录，撤销时间严格晚于批准时间。
        self.assertEqual(after["revoked_by"], "operator")
        self.assertIsNotNone(after["revoked_at"])
        self.assertGreater(after["revoked_at"], after["reviewed_at"])
        self.assertEqual(after["revoke_reason"], "已找回原始记录")

    def test_report_shows_review_and_revocation_as_distinguishable_facts(self) -> None:
        observation_id, exclusion_id = self._import_and_request()
        self._approve_exclusion(exclusion_id)
        self.clock.advance(seconds=30)
        self.service.revoke_exclusion("operator", exclusion_id, "已找回原始记录")
        report = self.service.report("auditor", "batch-a")
        (entry,) = report["exclusions"]
        self.assertEqual(entry["status"], "revoked")
        self.assertEqual(entry["review"], {
            "reviewed_by": "stat",
            "reviewed_at": entry["review"]["reviewed_at"],
            "review_note": "证据充分",
        })
        self.assertEqual(entry["revocation"], {
            "revoked_by": "operator",
            "revoked_at": entry["revocation"]["revoked_at"],
            "revoke_reason": "已找回原始记录",
        })
        self.assertGreater(entry["revocation"]["revoked_at"], entry["review"]["reviewed_at"])
        # 事件链同时包含批准与撤销两个可区分事件，撤销事件内固化原批准证据。
        chain = [event for event in report["events"] if event["entity_id"] == str(exclusion_id)]
        types = [event["event_type"] for event in chain]
        self.assertEqual(types, ["exclusion.requested", "exclusion.approved", "exclusion.revoked"])
        revoked_event = next(event for event in chain if event["event_type"] == "exclusion.revoked")
        self.assertEqual(revoked_event["payload"]["original_review"]["reviewed_by"], "stat")
        self.assertEqual(revoked_event["payload"]["original_review"]["review_note"], "证据充分")

    def test_duplicate_revoke_is_rejected_and_cannot_tamper_review(self) -> None:
        _, exclusion_id = self._import_and_request()
        self._approve_exclusion(exclusion_id)
        before = self._review_snapshot(exclusion_id)
        self.service.revoke_exclusion("operator", exclusion_id, "第一次撤销")
        with self.assertRaises(InvalidState):
            self.service.revoke_exclusion("operator", exclusion_id, "重复撤销")
        after = self._review_snapshot(exclusion_id)
        self.assertEqual(after["status"], "revoked")
        self.assertEqual(after["revoke_reason"], "第一次撤销")
        self.assertEqual(after["reviewed_at"], before["reviewed_at"])
        self.assertEqual(after["review_note"], before["review_note"])

    def test_revoke_after_seal_is_rejected_and_cannot_tamper_review(self) -> None:
        _, exclusion_id = self._import_and_request()
        self._approve_exclusion(exclusion_id)
        before = self._review_snapshot(exclusion_id)
        self.service.seal_batch("stat", "batch-a", 2)
        with self.assertRaises(InvalidState):
            self.service.revoke_exclusion("operator", exclusion_id, "封存后撤销")
        after = self._review_snapshot(exclusion_id)
        self.assertEqual(after["status"], "approved")
        self.assertIsNone(after["revoked_at"])
        self.assertEqual(after["reviewed_by"], before["reviewed_by"])
        self.assertEqual(after["reviewed_at"], before["reviewed_at"])
        self.assertEqual(after["review_note"], before["review_note"])

    def test_revoke_by_non_applicant_is_rejected_and_cannot_tamper_review(self) -> None:
        self.service.create_user("operator-2", "另一名操作员", "operator")
        _, exclusion_id = self._import_and_request()
        self._approve_exclusion(exclusion_id)
        before = self._review_snapshot(exclusion_id)
        with self.assertRaises(Forbidden):
            self.service.revoke_exclusion("operator-2", exclusion_id, "非申请人撤销")
        # 无该权限的角色同样被拒。
        with self.assertRaises(Forbidden):
            self.service.revoke_exclusion("stat", exclusion_id, "复核人撤销")
        after = self._review_snapshot(exclusion_id)
        self.assertEqual(after["status"], "approved")
        self.assertIsNone(after["revoked_at"])
        self.assertEqual(after["reviewed_by"], before["reviewed_by"])
        self.assertEqual(after["reviewed_at"], before["reviewed_at"])
        self.assertEqual(after["review_note"], before["review_note"])

    def test_revoke_with_blank_reason_is_rejected(self) -> None:
        _, exclusion_id = self._import_and_request()
        self._approve_exclusion(exclusion_id)
        with self.assertRaises(ValidationFailed):
            self.service.revoke_exclusion("operator", exclusion_id, "   ")
        after = self._review_snapshot(exclusion_id)
        self.assertEqual(after["status"], "approved")
        self.assertIsNone(after["revoked_at"])

    def test_revoked_exclusion_is_efficiently_excluded_from_active_set(self) -> None:
        _, exclusion_id = self._import_and_request()
        self._approve_exclusion(exclusion_id)
        self.service.revoke_exclusion("operator", exclusion_id, "已找回原始记录")
        # 当前生效集合走部分唯一索引同一谓词，撤销后观测重新回到分析输入。
        active = self.connection.execute(
            "SELECT count(*) FROM exclusion_requests WHERE status IN ('pending','approved') "
            "AND exclusion_id=?",
            (exclusion_id,),
        ).fetchone()[0]
        self.assertEqual(active, 0)
        self.service.seal_batch("stat", "batch-a", 2)
        job = self.service.claim_job("worker", 30)
        analysis = self.service.complete_job("worker", job["job_id"], "stat")
        self.assertEqual(analysis["result"]["included_count"], 6)
        self.assertEqual(analysis["result"]["excluded_count"], 0)

    def test_database_trigger_blocks_direct_tampering_and_deletion(self) -> None:
        _, exclusion_id = self._import_and_request()
        self._approve_exclusion(exclusion_id)
        # 即便绕过服务层直接执行 SQL，也不能改写复核事实或删除排除记录。
        with self.assertRaises(sqlite3.IntegrityError):
            self.connection.execute(
                "UPDATE exclusion_requests SET review_note='被伪造的意见' WHERE exclusion_id=?",
                (exclusion_id,),
            )
        with self.assertRaises(sqlite3.IntegrityError):
            self.connection.execute(
                "UPDATE exclusion_requests SET reviewed_by='operator' WHERE exclusion_id=?",
                (exclusion_id,),
            )
        with self.assertRaises(sqlite3.IntegrityError):
            self.connection.execute(
                "DELETE FROM exclusion_requests WHERE exclusion_id=?", (exclusion_id,)
            )
        row = self._review_snapshot(exclusion_id)
        self.assertEqual(row["reviewed_by"], "stat")
        self.assertEqual(row["review_note"], "证据充分")
        self.assertEqual(row["status"], "approved")

    def test_failed_job_returns_to_queue_after_delay(self) -> None:
        self.service.import_observations("operator", "batch-a", "key-1", self.rows)
        self.service.seal_batch("stat", "batch-a", 2)
        job = self.service.claim_job("worker-a", 10)
        failed = self.service.fail_job("worker-a", job["job_id"], "临时计算失败", retry_seconds=5)
        self.assertEqual(failed["state"], "queued")
        self.assertIsNone(self.service.claim_job("worker-b", 10))
        self.clock.advance(seconds=5)
        retried = self.service.claim_job("worker-b", 10)
        self.assertEqual(retried["job_id"], job["job_id"])
        self.assertEqual(retried["attempts"], 2)

    def test_lease_can_be_reclaimed_after_expiry(self) -> None:
        self.service.import_observations("operator", "batch-a", "key-1", self.rows)
        self.service.seal_batch("stat", "batch-a", 2)
        first = self.service.claim_job("worker-a", 10)
        self.clock.advance(seconds=11)
        second = self.service.claim_job("worker-b", 10)
        self.assertEqual(first["job_id"], second["job_id"])
        self.assertEqual(second["lease_owner"], "worker-b")
        with self.assertRaises(InvalidState):
            self.service.complete_job("worker-a", first["job_id"], "stat")


if __name__ == "__main__":
    unittest.main()
