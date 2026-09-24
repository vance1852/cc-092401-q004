from __future__ import annotations

import json
import sqlite3
import unittest
from datetime import datetime, timezone
from pathlib import Path

from robot_trials.clock import FrozenClock
from robot_trials.errors import Conflict, Forbidden, InvalidState
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
            ("operator-2", "operator"),
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

    def _create_approved_exclusion(self, note: str = "证据充分") -> tuple[int, int, dict]:
        imported = self.service.import_observations("operator", "batch-a", "key-1", self.rows)
        self.assertEqual(imported["inserted"], 6)
        observation_id = self.connection.execute(
            "SELECT observation_id FROM observations ORDER BY observation_id LIMIT 1"
        ).fetchone()[0]
        requested = self.service.request_exclusion("operator", observation_id, "现场记录失效")
        exclusion_id = requested["exclusion_id"]
        self.clock.advance(seconds=10)
        reviewed = self.service.review_exclusion("stat", exclusion_id, True, note)
        self.assertEqual(reviewed["status"], "approved")
        before = dict(
            self.connection.execute(
                "SELECT reviewed_by,reviewed_at,review_note FROM exclusion_requests "
                "WHERE exclusion_id=?",
                (exclusion_id,),
            ).fetchone()
        )
        self.clock.advance(seconds=20)
        return exclusion_id, observation_id, before

    def test_revoke_records_separate_facts_without_touching_review(self) -> None:
        exclusion_id, observation_id, before = self._create_approved_exclusion()
        revoked = self.service.revoke_exclusion("operator", exclusion_id, "已找回原始记录")
        self.assertEqual(revoked["status"], "revoked")
        row = dict(
            self.connection.execute(
                "SELECT * FROM exclusion_requests WHERE exclusion_id=?", (exclusion_id,)
            ).fetchone()
        )
        # 最初复核事实必须原样保留。
        self.assertEqual(row["reviewed_by"], before["reviewed_by"])
        self.assertEqual(row["reviewed_at"], before["reviewed_at"])
        self.assertEqual(row["review_note"], before["review_note"])
        self.assertEqual(row["reviewed_by"], "stat")
        self.assertEqual(row["review_note"], "证据充分")
        self.assertLess(row["reviewed_at"], row["revoked_at"])
        # 撤销事实独立记录。
        self.assertEqual(row["revoked_by"], "operator")
        self.assertEqual(row["revoke_reason"], "已找回原始记录")
        self.assertIsNotNone(row["revoked_at"])

    def test_review_and_revocation_are_two_distinguishable_facts_in_event_chain(self) -> None:
        exclusion_id, _, _ = self._create_approved_exclusion()
        self.service.revoke_exclusion("operator", exclusion_id, "已找回原始记录")
        events = self.connection.execute(
            "SELECT event_type,actor_id FROM audit_events WHERE entity_type='exclusion' "
            "AND entity_id=? ORDER BY event_id",
            (str(exclusion_id),),
        ).fetchall()
        self.assertEqual(
            [(row[0], row[1]) for row in events],
            [
                ("exclusion.requested", "operator"),
                ("exclusion.approved", "stat"),
                ("exclusion.revoked", "operator"),
            ],
        )

    def test_report_shows_review_and_revocation_as_separate_facts(self) -> None:
        exclusion_id, _, _ = self._create_approved_exclusion()
        self.service.revoke_exclusion("operator", exclusion_id, "已找回原始记录")
        report = self.service.report("auditor", "batch-a")
        matches = [item for item in report["exclusions"] if item["exclusion_id"] == exclusion_id]
        self.assertEqual(len(matches), 1)
        entry = matches[0]
        self.assertEqual(entry["status"], "revoked")
        self.assertEqual(entry["reviewed_by"], "stat")
        self.assertEqual(entry["review_note"], "证据充分")
        self.assertEqual(entry["revoked_by"], "operator")
        self.assertEqual(entry["revoke_reason"], "已找回原始记录")
        chain = [event["event_type"] for event in report["events"] if event["entity_type"] == "exclusion"]
        self.assertEqual(chain, ["exclusion.requested", "exclusion.approved", "exclusion.revoked"])

    def test_duplicate_revoke_is_rejected_and_leaves_review_intact(self) -> None:
        exclusion_id, _, before = self._create_approved_exclusion()
        self.service.revoke_exclusion("operator", exclusion_id, "第一次撤销")
        with self.assertRaises(InvalidState):
            self.service.revoke_exclusion("operator", exclusion_id, "重复撤销")
        row = self.connection.execute(
            "SELECT reviewed_by,reviewed_at,review_note,revoked_by,revoke_reason "
            "FROM exclusion_requests WHERE exclusion_id=?",
            (exclusion_id,),
        ).fetchone()
        self.assertEqual(row["review_note"], before["review_note"])
        self.assertEqual(row["reviewed_at"], before["reviewed_at"])
        self.assertEqual(row["revoke_reason"], "第一次撤销")

    def test_revoke_after_seal_is_rejected(self) -> None:
        exclusion_id, _, before = self._create_approved_exclusion()
        self.service.seal_batch("stat", "batch-a", 2)
        with self.assertRaises(InvalidState):
            self.service.revoke_exclusion("operator", exclusion_id, "封存后撤销")
        row = self.connection.execute(
            "SELECT status,reviewed_by,reviewed_at,review_note,revoked_at "
            "FROM exclusion_requests WHERE exclusion_id=?",
            (exclusion_id,),
        ).fetchone()
        self.assertEqual(row["status"], "approved")
        self.assertEqual(row["review_note"], before["review_note"])
        self.assertEqual(row["reviewed_at"], before["reviewed_at"])
        self.assertIsNone(row["revoked_at"])

    def test_non_requester_cannot_revoke(self) -> None:
        exclusion_id, _, before = self._create_approved_exclusion()
        with self.assertRaises(Forbidden):
            self.service.revoke_exclusion("operator-2", exclusion_id, "非申请人撤销")
        with self.assertRaises(Forbidden):
            self.service.revoke_exclusion("stat", exclusion_id, "复核人撤销")
        row = self.connection.execute(
            "SELECT status,reviewed_by,reviewed_at,review_note,revoked_at "
            "FROM exclusion_requests WHERE exclusion_id=?",
            (exclusion_id,),
        ).fetchone()
        self.assertEqual(row["status"], "approved")
        self.assertEqual(row["review_note"], before["review_note"])
        self.assertEqual(row["reviewed_at"], before["reviewed_at"])
        self.assertIsNone(row["revoked_at"])

    def test_revoked_exclusion_is_no_longer_applied_to_analysis(self) -> None:
        exclusion_id, _, _ = self._create_approved_exclusion()
        applied = self.connection.execute(
            "SELECT count(*) FROM observations o JOIN exclusion_requests e "
            "ON e.observation_id=o.observation_id AND e.status='approved' "
            "WHERE o.batch_id='batch-a'"
        ).fetchone()[0]
        self.assertEqual(applied, 1)
        self.service.revoke_exclusion("operator", exclusion_id, "已找回原始记录")
        applied_after = self.connection.execute(
            "SELECT count(*) FROM observations o JOIN exclusion_requests e "
            "ON e.observation_id=o.observation_id AND e.status='approved' "
            "WHERE o.batch_id='batch-a'"
        ).fetchone()[0]
        self.assertEqual(applied_after, 0)

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
