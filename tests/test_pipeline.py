"""End-to-end and reliability tests for the pipeline (scenario ids from FAILURE_MODEL.md)."""

from __future__ import annotations

import sqlite3
import unittest

from opsflow.domain.errors import ConcurrencyError, InvalidTransition, TransientError
from opsflow.pipeline import rerun_batch, resolve_review
from opsflow.pipeline.config import Hooks
from opsflow.storage.sqlite import RecordRow
from tests.helpers import ACTOR, PipelineTestCase, SimulatedCrash


def snapshot(case: PipelineTestCase, repo=None) -> dict:  # noqa: ANN001
    """Business outcome of every batch, independent of timing and random event ids."""
    out = {}
    with (repo or case.repo).transaction() as tx:
        for b in tx.list_batches():
            recon = tx.latest_reconciliation(b.id) or {}
            out[b.source_name] = {
                "status": b.status,
                "reconciliation": b.reconciliation_status,
                "processed_debit": recon.get("processed_debit_minor"),
                "processed_credit": recon.get("processed_credit_minor"),
                "records": [(r.line_no, r.status, r.category_code, r.duplicate_of) for r in tx.list_records(b.id)],
            }
    return out


class ManifestTests(PipelineTestCase):
    """The generator's manifest is the oracle: every expected outcome must be met exactly."""

    def test_all_scenarios_reach_their_expected_outcome(self) -> None:
        files = self.files()
        ids = {f.name: self.ingest(f) for f in files}
        self.worker().run_until_idle()

        for f in files:
            with self.subTest(file=f.name, scenario=f.scenario):
                batch = self.batch(ids[f.name])
                self.assertEqual(batch.status, f.expected_batch_status)
                self.assertEqual(batch.reconciliation_status, f.expected_reconciliation)
                records = {r.line_no: r for r in self.records(batch.id)}
                self.assertEqual(sorted(records), [e.line_no for e in f.records])
                with self.repo.transaction() as tx:
                    issues = tx.list_issues(batch.id)
                for expected in f.records:
                    rec = records[expected.line_no]
                    self.assertEqual(rec.status, expected.status, f"line {expected.line_no}")
                    self.assertEqual(rec.category_code, expected.category, f"line {expected.line_no}")
                    if expected.status == "VALIDATION_FAILED":
                        codes = {i["code"] for i in issues if i["line_no"] == expected.line_no}
                        self.assertIn(expected.reason, codes, f"line {expected.line_no}")
                    if expected.status == "NEEDS_REVIEW":
                        with self.repo.transaction() as tx:
                            reasons = {t["reason"] for t in tx.list_reviews(batch_id=batch.id)
                                       if t["record_id"] == rec.id}  # fmt: skip
                        self.assertEqual(reasons, {expected.reason})

    def test_generator_is_deterministic(self) -> None:
        first = [(f.name, f.content) for f in self.files()]
        second = [(f.name, f.content) for f in self.files()]
        self.assertEqual(first, second)

    def test_duplicate_records_are_never_double_counted(self) -> None:
        for f in self.files():
            self.ingest(f)
        self.worker().run_until_idle()
        with self.repo.transaction() as tx:
            rows = tx.execute(
                """SELECT source_ref, COUNT(*) AS n FROM record
                   WHERE status = 'PROCESSED' GROUP BY source_ref HAVING COUNT(*) > 1"""
            )
        self.assertEqual(rows, [], "a business key was processed more than once")


class IdempotencyTests(PipelineTestCase):
    def test_f2_same_file_twice_creates_no_new_work(self) -> None:
        f = self.files()[0]
        first = self.ingest(f)
        self.worker().run_until_idle()
        before = snapshot(self)
        second = self.ingest(f)
        self.assertEqual(first, second)
        self.assertEqual(self.worker().run_until_idle(), [])
        self.assertEqual(snapshot(self), before)
        with self.repo.transaction() as tx:
            actions = [e["action"] for e in tx.list_audit(first)]
        self.assertIn("DUPLICATE_SUBMISSION", actions)

    def test_ids_are_deterministic_across_databases(self) -> None:
        other = self.new_repo("other")
        f = self.files()[0]
        a, b = self.ingest(f), self.ingest(f, repo=other)
        self.worker().run_until_idle()
        self.worker(other).run_until_idle()
        self.assertEqual(a, b)
        self.assertEqual([r.id for r in self.records(a)], [r.id for r in self.records(b, repo=other)])

    def test_running_the_worker_again_changes_nothing(self) -> None:
        for f in self.files():
            self.ingest(f)
        self.worker().run_until_idle()
        before = snapshot(self)
        with self.repo.transaction() as tx:
            audit_count = len(tx.list_audit())
        self.assertEqual(self.worker().run_until_idle(), [])
        self.assertEqual(snapshot(self), before)
        with self.repo.transaction() as tx:
            self.assertEqual(len(tx.list_audit()), audit_count)


class CrashAndRetryTests(PipelineTestCase):
    def reference(self) -> dict:
        ref = self.new_repo("reference")
        for f in self.files():
            self.ingest(f, repo=ref)
        self.worker(ref).run_until_idle()
        return snapshot(self, ref)

    def test_f21_crash_mid_batch_resumes_to_the_identical_result(self) -> None:
        expected = self.reference()
        files = self.files()
        for f in files:
            self.ingest(f)

        def crash(rec: RecordRow, stage: str) -> None:
            if rec.line_no == 6 and stage == "classify":
                raise SimulatedCrash

        with self.assertRaises(SimulatedCrash):
            self.worker(hooks=Hooks(before_record=crash)).run_until_idle()

        first_id = self.ingest(files[0])  # resubmission returns the existing id
        self.assertEqual(self.batch(first_id).status, "PROCESSING")
        self.assertEqual(self.worker(worker_id="worker-2").run_until_idle(), [], "lease still valid")

        self.clock.advance(seconds=301)  # lease expires
        self.worker(worker_id="worker-2").run_until_idle()
        self.assertEqual(snapshot(self), expected)
        with self.repo.transaction() as tx:
            runs = tx.list_runs(first_id)
        self.assertEqual([r["outcome"] for r in runs], ["ABANDONED", "COMPLETED"])

    def test_f16_transient_error_retries_with_backoff_then_succeeds(self) -> None:
        f = self.files()[0]
        batch_id = self.ingest(f)
        failures = {"left": 2}

        def flaky(rec: RecordRow, stage: str) -> None:
            if rec.line_no == 5 and stage == "classify" and failures["left"]:
                failures["left"] -= 1
                raise TransientError("PROVIDER_UNAVAILABLE", "simulated outage")

        worker = self.worker(hooks=Hooks(before_record=flaky))
        worker.run_until_idle()
        batch = self.batch(batch_id)
        self.assertEqual(batch.status, "RETRY_PENDING")
        self.assertEqual(batch.reconciliation_status, "PENDING")
        self.assertEqual(worker.run_until_idle(), [], "not due yet: backoff is respected")

        self.clock.advance(seconds=60)
        worker.run_until_idle()
        self.assertEqual(self.batch(batch_id).status, "RETRY_PENDING")  # second failure
        self.clock.advance(seconds=120)
        worker.run_until_idle()
        batch = self.batch(batch_id)
        self.assertEqual((batch.status, batch.reconciliation_status), ("PROCESSED", "RECONCILED"))
        with self.repo.transaction() as tx:
            outcomes = [r["outcome"] for r in tx.list_runs(batch_id)]
        self.assertEqual(outcomes, ["RETRY_SCHEDULED", "RETRY_SCHEDULED", "COMPLETED"])

    def test_retry_budget_exhaustion_fails_then_operator_rerun_succeeds(self) -> None:
        batch_id = self.ingest(self.files()[0], max_attempts=2)
        outage = {"on": True}

        def down(rec: RecordRow, stage: str) -> None:
            if outage["on"] and stage == "classify":
                raise TransientError("PROVIDER_UNAVAILABLE", "simulated outage")

        worker = self.worker(hooks=Hooks(before_record=down))
        worker.run_until_idle()
        self.clock.advance(minutes=20)
        worker.run_until_idle()
        batch = self.batch(batch_id)
        self.assertEqual(batch.status, "FAILED")
        self.assertEqual(batch.reconciliation_status, "PENDING", "unfinished work is not reported as success")

        with self.assertRaises(ValueError):
            rerun_batch(self.repo, self.clock, batch_id, actor=ACTOR, reason=" ")
        outage["on"] = False
        rerun_batch(self.repo, self.clock, batch_id, actor=ACTOR, reason="provider restored")
        worker.run_until_idle()
        batch = self.batch(batch_id)
        self.assertEqual((batch.status, batch.reconciliation_status), ("PROCESSED", "RECONCILED"))
        with self.repo.transaction() as tx:
            reruns = [e for e in tx.list_audit(batch_id) if e["action"] == "OPERATOR_RERUN"]
        self.assertEqual(reruns[0]["actor"], ACTOR)
        self.assertEqual(reruns[0]["details"]["reason"], "provider restored")

    def test_rerun_only_allowed_for_failed_batches(self) -> None:
        batch_id = self.ingest(self.files()[0])
        self.worker().run_until_idle()
        with self.assertRaises(InvalidTransition):
            rerun_batch(self.repo, self.clock, batch_id, actor=ACTOR, reason="why not")

    def test_unexpected_error_fails_batch_with_record_context(self) -> None:
        batch_id = self.ingest(self.files()[0])

        def bug(rec: RecordRow, stage: str) -> None:
            if rec.line_no == 4:
                raise KeyError("boom")

        self.worker(hooks=Hooks(before_record=bug)).run_until_idle()
        with self.repo.transaction() as tx:
            run = tx.list_runs(batch_id)[0]
        self.assertEqual(self.batch(batch_id).status, "FAILED")
        self.assertEqual(run["error_code"], "UNEXPECTED_ERROR")
        self.assertIn("record", run["error_detail"])
        statuses = [r.status for r in self.records(batch_id)]
        self.assertEqual(statuses[0], "PROCESSED", "work committed before the error is kept")

    def test_crash_loop_is_stopped_by_the_retry_budget(self) -> None:
        batch_id = self.ingest(self.files()[0])

        def always_crash(rec: RecordRow, stage: str) -> None:
            raise SimulatedCrash

        for _ in range(3):
            with self.assertRaises(SimulatedCrash):
                self.worker(hooks=Hooks(before_record=always_crash)).run_until_idle()
            self.clock.advance(seconds=301)
        self.worker().run_until_idle()
        self.assertEqual(self.batch(batch_id).status, "FAILED")


class OrderingTests(PipelineTestCase):
    def test_f25_later_delivery_waits_behind_failed_earlier_delivery(self) -> None:
        files = self.files()
        day1, day3 = files[0], files[2]  # day 3 repeats three of day 1's transactions
        id1, id3 = self.ingest(day1), self.ingest(day3)

        def bug(rec: RecordRow, stage: str) -> None:
            if rec.batch_id == id1 and rec.line_no == 3:
                raise RuntimeError("boom")

        self.worker(hooks=Hooks(before_record=bug)).run_until_idle()
        self.assertEqual(self.batch(id1).status, "FAILED")
        self.assertEqual(self.batch(id3).status, "RECEIVED", "must not overtake the failed delivery")
        later = self.batch(id3)
        with self.repo.transaction() as tx:
            blocker = tx.blocking_predecessor(later)
        self.assertEqual(blocker.id if blocker else None, id1)

        rerun_batch(self.repo, self.clock, id1, actor=ACTOR, reason="fixed")
        self.worker().run_until_idle()
        overlap = [r.status for r in self.records(id3)][:3]
        self.assertEqual(overlap, ["DUPLICATE"] * 3, "day 1 keeps ownership of its own transactions")

    def test_structurally_invalid_delivery_does_not_block(self) -> None:
        files = self.files()
        bad, good = files[6], files[3]
        self.ingest(bad)
        good_id = self.ingest(good)
        self.worker().run_until_idle()
        self.assertEqual(self.batch(good_id).status, "PROCESSED")


class ReviewTests(PipelineTestCase):
    def setUp(self) -> None:
        super().setUp()
        self.messy_id = self.ingest(self.files()[1])
        self.worker().run_until_idle()
        with self.repo.transaction() as tx:
            self.tasks = {t["reason"]: t for t in tx.list_reviews(batch_id=self.messy_id)}

    def test_resolving_all_reviews_completes_and_reconciles_the_batch(self) -> None:
        self.assertEqual(self.batch(self.messy_id).status, "NEEDS_REVIEW")
        resolve_review(self.repo, self.clock, self.tasks["RULE_CONFLICT"]["id"], decision="CORRECT",
                       category="FUEL", note="receipt shows fuel", actor=ACTOR)  # fmt: skip
        self.assertEqual(self.batch(self.messy_id).reconciliation_status, "PENDING")
        resolve_review(self.repo, self.clock, self.tasks["NO_RULE_AI_DISABLED"]["id"], decision="REJECT",
                       note="not business spend", actor=ACTOR)  # fmt: skip
        resolve_review(self.repo, self.clock, self.tasks["DUPLICATE_CONFLICT"]["id"], decision="REJECT",
                       note="resent with wrong amount", actor=ACTOR)  # fmt: skip
        batch = self.batch(self.messy_id)
        self.assertEqual((batch.status, batch.reconciliation_status), ("PROCESSED", "RECONCILED"))
        with self.repo.transaction() as tx:
            rec = tx.get_record(self.tasks["RULE_CONFLICT"]["record_id"])
        self.assertEqual((rec.status, rec.category_code, rec.category_source), ("PROCESSED", "FUEL", "HUMAN"))

    def test_decisions_require_reasons_and_valid_categories(self) -> None:
        task_id = self.tasks["RULE_CONFLICT"]["id"]
        with self.assertRaises(ValueError):
            resolve_review(self.repo, self.clock, task_id, decision="CORRECT", category="FUEL", actor=ACTOR)
        with self.assertRaises(ValueError):  # FREIGHT_REVENUE is a CREDIT category; this is a DEBIT
            resolve_review(self.repo, self.clock, task_id, decision="CORRECT", category="FREIGHT_REVENUE",
                           note="x", actor=ACTOR)  # fmt: skip
        with self.assertRaises(ValueError):  # nothing was suggested, so nothing to approve
            resolve_review(self.repo, self.clock, task_id, decision="APPROVE", actor=ACTOR)

    def test_f23_stale_and_repeated_decisions_are_rejected(self) -> None:
        task = self.tasks["NO_RULE_AI_DISABLED"]
        resolve_review(self.repo, self.clock, task["id"], decision="REJECT", note="no", actor=ACTOR,
                       expected_version=task["version"])  # fmt: skip
        with self.assertRaises(InvalidTransition):
            resolve_review(self.repo, self.clock, task["id"], decision="REJECT", note="again", actor=ACTOR)
        other = self.tasks["RULE_CONFLICT"]
        with self.assertRaises(ConcurrencyError):
            resolve_review(self.repo, self.clock, other["id"], decision="REJECT", note="x", actor=ACTOR,
                           expected_version=other["version"] + 1)  # fmt: skip

    def test_f22_rejection_is_inherited_by_later_identical_copies(self) -> None:
        """Outcomes must not depend on *when* a reviewer decides (deterministic replay)."""
        task = self.tasks["NO_RULE_AI_DISABLED"]
        resolve_review(self.repo, self.clock, task["id"], decision="REJECT", note="wrong row", actor=ACTOR)
        with self.repo.transaction() as tx:
            rejected = tx.get_record(task["record_id"])
            claims = tx.execute("SELECT record_id FROM record_claim WHERE source_ref = ?", (rejected.source_ref,))
        self.assertEqual([c["record_id"] for c in claims], [rejected.id], "claim is kept")

        # A later delivery repeats the rejected row byte-for-byte: it is a DUPLICATE, not new spend.
        lines = self.files()[1].content.decode().splitlines()
        repeated = next(line for line in lines if line.startswith(rejected.source_ref))
        later = (
            "#KFX-BANK-EXPORT v1,business_date=2026-03-09\n"
            "txn_id,posted_on,payee,memo,amount,currency,dr_cr\n"
            f"{repeated}\n#TRAILER,count=1,debits=86.40,credits=0.00\n"
        ).encode()
        from opsflow.pipeline import ingest_bytes

        later_id = ingest_bytes(self.repo, self.clock, source_type="BANK_CSV", source_name="later.csv",
                                content=later, actor=ACTOR).batch_id  # fmt: skip
        self.worker().run_until_idle()
        copy = self.records(later_id)[0]
        self.assertEqual((copy.status, copy.duplicate_of), ("DUPLICATE", rejected.id))
        self.assertEqual(self.batch(later_id).reconciliation_status, "RECONCILED")


class DatabaseInvariantTests(PipelineTestCase):
    def setUp(self) -> None:
        super().setUp()
        self.batch_id = self.ingest(self.files()[0])
        self.worker().run_until_idle()

    def test_audit_trail_is_append_only(self) -> None:
        for sql in ("UPDATE audit_event SET actor = 'x'", "DELETE FROM audit_event"):
            with self.subTest(sql=sql), self.assertRaises(sqlite3.IntegrityError), self.repo.transaction() as tx:
                tx.execute(sql)

    def test_raw_input_is_immutable(self) -> None:
        with self.assertRaises(sqlite3.IntegrityError), self.repo.transaction() as tx:
            tx.execute("UPDATE record SET raw_payload = '{}'")

    def test_processed_record_must_have_a_category_and_a_decision_source(self) -> None:
        rec = self.records(self.batch_id)[0]
        for sql in (
            "UPDATE record SET category_code = NULL, category_source = NULL, rule_id = NULL, rule_version = NULL "
            "WHERE id = ?",
            "UPDATE record SET category_source = 'AI' WHERE id = ?",  # AI decision without an attempt id
        ):
            with self.subTest(sql=sql), self.assertRaises(sqlite3.IntegrityError), self.repo.transaction() as tx:
                tx.execute(sql, (rec.id,))

    def test_working_batch_cannot_exist_without_a_lease(self) -> None:
        """Guards against an orphan the lease query could never reclaim (plan review R20)."""
        with self.assertRaises(sqlite3.IntegrityError), self.repo.transaction() as tx:
            tx.execute("UPDATE source_batch SET status = 'PROCESSING' WHERE id = ?", (self.batch_id,))

    def test_every_state_change_is_audited(self) -> None:
        with self.repo.transaction() as tx:
            events = tx.list_audit(self.batch_id)
        record_events = [e for e in events if e["entity_type"] == "record"]
        self.assertEqual(len(record_events), 2 * len(self.records(self.batch_id)))  # VALIDATED + PROCESSED
        actions = [e["action"] for e in events if e["entity_type"] == "batch"]
        self.assertEqual(actions[:3], ["BATCH_RECEIVED", "BATCH_LEASED", "STRUCTURE_VALIDATED"])
        self.assertEqual(actions[-2:], ["BATCH_PROCESSED", "RECONCILIATION_COMPUTED"])


if __name__ == "__main__":
    unittest.main()
