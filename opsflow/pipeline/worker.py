"""The batch worker.

Life of one attempt::

    acquire lease (compare-and-set)            -> VALIDATING or PROCESSING
    VALIDATING: parse structure, register rows -> PROCESSING   (checkpoint)
    PROCESSING: per record, two short transactions
        A: normalise + validate + dedupe claim -> VALIDATED | VALIDATION_FAILED | DUPLICATE | NEEDS_REVIEW
        B: rules (AI in Phase 6)              -> PROCESSED | NEEDS_REVIEW
    finalise                                   -> PROCESSED | NEEDS_REVIEW
    transient error                            -> RETRY_PENDING (backoff) | FAILED (budget spent)
    unexpected error                           -> FAILED (committed work is kept)

Every write re-checks that this run still holds the lease, so a worker that
stalled past its lease can never overwrite the work of the worker that took over.
"""

from __future__ import annotations

import json
import random
from dataclasses import dataclass
from datetime import timedelta

from opsflow.clock import Clock
from opsflow.domain.errors import ConcurrencyError, LeaseLost, StructureError, TransientError
from opsflow.domain.ids import fingerprint, new_event_id, record_id
from opsflow.domain.models import Direction, NormalisedRecord, RawRecord
from opsflow.domain.rules import RULESET_VERSION, RuleOutcome, classify_by_rules
from opsflow.domain.states import (
    MACHINE_PENDING_RECORD_STATES,
    BatchStatus,
    CategorySource,
    RecordStatus,
    ReviewReason,
    RunOutcome,
    ensure_batch_transition,
    ensure_record_transition,
)
from opsflow.domain.validation import validate_canonical
from opsflow.obs import log
from opsflow.pipeline.config import Hooks, PipelineConfig
from opsflow.pipeline.recon import reconcile_batch
from opsflow.sources import SourceAdapter, default_adapters
from opsflow.storage.sqlite import BatchRow, RecordRow, SqliteRepository, SqliteTx

_NORMALISED_FIELDS = ("source_ref", "business_date", "counterparty", "description", "amount_minor", "currency", "direction")


@dataclass(frozen=True, slots=True)
class Lease:
    batch_id: str
    run_id: str
    attempt_no: int


@dataclass(frozen=True, slots=True)
class RunSummary:
    batch_id: str
    run_id: str
    final_status: str


class Worker:
    def __init__(
        self,
        repo: SqliteRepository,
        clock: Clock,
        config: PipelineConfig | None = None,
        *,
        adapters: dict[str, SourceAdapter] | None = None,
        hooks: Hooks | None = None,
    ) -> None:
        self.repo = repo
        self.clock = clock
        self.config = config or PipelineConfig()
        self.adapters = adapters or default_adapters()
        self.hooks = hooks or Hooks()
        self.actor = f"system:{self.config.worker_id}"
        self._rng = random.Random(self.config.jitter_seed)
        self._current_record: str | None = None

    # ------------------------------------------------------------- public API
    def run_once(self) -> RunSummary | None:
        """Lease and process at most one batch. Returns None when there is nothing due."""
        lease = self._acquire()
        if lease is None:
            return None
        self._process(lease)
        with self.repo.transaction() as tx:
            status = tx.get_batch(lease.batch_id).status
        return RunSummary(lease.batch_id, lease.run_id, status)

    def run_until_idle(self, max_runs: int = 1000) -> list[RunSummary]:
        summaries = []
        for _ in range(max_runs):
            summary = self.run_once()
            if summary is None:
                break
            summaries.append(summary)
        return summaries

    # ---------------------------------------------------------------- leasing
    def _acquire(self) -> Lease | None:
        now = self.clock.now()
        try:
            with self.repo.transaction() as tx:
                for batch in tx.leasable_batches(now):
                    lease = self._try_lease(tx, batch)
                    if lease is not None:
                        return lease
        except ConcurrencyError:
            log("lease_race_lost", worker=self.config.worker_id)
        return None

    def _try_lease(self, tx: SqliteTx, batch: BatchRow) -> Lease | None:
        now = self.clock.now()
        current = BatchStatus(batch.status)
        if current in (BatchStatus.VALIDATING, BatchStatus.PROCESSING):
            # The previous holder's lease expired: it crashed or stalled. We do not
            # know how far it got, so its run is ABANDONED (neither failed nor succeeded).
            tx.finish_run(
                batch.lease_run_id, outcome=RunOutcome.ABANDONED, finished_at=now,
                error_code="LEASE_EXPIRED", error_detail="lease expired before the run finished",
            )  # fmt: skip
            tx.audit(
                now=now, actor=self.actor, entity_type="batch", entity_id=batch.id, batch_id=batch.id,
                action="LEASE_EXPIRED", from_state=current, to_state=current, run_id=batch.lease_run_id,
            )  # fmt: skip

        if batch.retry_budget_used >= batch.max_attempts:
            # Only reachable after crashes: a batch that keeps killing workers must stop.
            ensure_batch_transition(current, BatchStatus.FAILED)
            tx.update_batch(
                batch.id, now=now, expected_version=batch.version, status=BatchStatus.FAILED,
                lease_run_id=None, lease_expires_at=None, next_attempt_at=None,
            )  # fmt: skip
            tx.audit(
                now=now, actor=self.actor, entity_type="batch", entity_id=batch.id, batch_id=batch.id,
                action="BATCH_FAILED", from_state=current, to_state=BatchStatus.FAILED,
                details={"error_code": "RETRY_BUDGET_EXHAUSTED", "attempts": batch.retry_budget_used},
            )  # fmt: skip
            reconcile_batch(tx, batch.id, now=now, actor=self.actor)
            return None

        target = BatchStatus.PROCESSING if batch.structure_validated_at else BatchStatus.VALIDATING
        ensure_batch_transition(current, target)
        run_id = new_event_id()
        attempt_no = tx.insert_run(run_id=run_id, batch_id=batch.id, worker_id=self.config.worker_id, now=now)
        tx.update_batch(
            batch.id, now=now, expected_version=batch.version, require_lease_free_or_expired=True,
            status=target, lease_run_id=run_id, lease_expires_at=now + self._lease_length(),
            next_attempt_at=None, retry_budget_used=batch.retry_budget_used + 1,
        )  # fmt: skip
        tx.audit(
            now=now, actor=self.actor, entity_type="batch", entity_id=batch.id, batch_id=batch.id,
            action="BATCH_LEASED", from_state=current, to_state=target, run_id=run_id,
            details={"attempt_no": attempt_no, "budget_used": batch.retry_budget_used + 1},
        )  # fmt: skip
        log("batch_leased", batch_id=batch.id, run_id=run_id, attempt_no=attempt_no, status=str(target))
        return Lease(batch.id, run_id, attempt_no)

    def _lease_length(self) -> timedelta:
        return timedelta(seconds=self.config.lease_seconds)

    def _renew(self, tx: SqliteTx, lease: Lease) -> None:
        now = self.clock.now()
        tx.renew_lease(lease.batch_id, lease.run_id, now, now + self._lease_length())

    # ------------------------------------------------------------- processing
    def _process(self, lease: Lease) -> None:
        self._current_record = None
        try:
            with self.repo.transaction() as tx:
                status = tx.get_batch(lease.batch_id).status
            if status == BatchStatus.VALIDATING and not self._validate_structure(lease):
                return
            self._process_records(lease)
            self._finalise(lease)
        except LeaseLost as exc:
            log("lease_lost", batch_id=lease.batch_id, run_id=lease.run_id, detail=str(exc))
        except TransientError as exc:
            self._end_attempt(lease, exc.code, str(exc), transient=True)
        except Exception as exc:  # noqa: BLE001 - deliberate: any bug must fail the batch visibly
            detail = f"{type(exc).__name__}: {exc}"
            if self._current_record:
                detail += f" (record {self._current_record})"
            self._end_attempt(lease, "UNEXPECTED_ERROR", detail, transient=False)

    def _validate_structure(self, lease: Lease) -> bool:
        with self.repo.transaction() as tx:
            batch = tx.get_batch(lease.batch_id)
            content = tx.get_blob(batch.id)
        adapter = self.adapters[batch.source_type]
        now = self.clock.now()
        try:
            parsed = adapter.parse(content)
        except StructureError as exc:
            with self.repo.transaction() as tx:
                tx.insert_issue(
                    batch_id=batch.id, record_id=None, code=exc.code, field=None,
                    message=exc.message, severity="ERROR", now=now,
                )  # fmt: skip
                ensure_batch_transition(BatchStatus.VALIDATING, BatchStatus.VALIDATION_FAILED)
                tx.update_batch(
                    batch.id, now=now, held_by_run=lease.run_id, status=BatchStatus.VALIDATION_FAILED,
                    lease_run_id=None, lease_expires_at=None,
                )  # fmt: skip
                tx.finish_run(lease.run_id, outcome=RunOutcome.COMPLETED, finished_at=now)
                tx.audit(
                    now=now, actor=self.actor, entity_type="batch", entity_id=batch.id, batch_id=batch.id,
                    action="STRUCTURE_INVALID", from_state=BatchStatus.VALIDATING,
                    to_state=BatchStatus.VALIDATION_FAILED, run_id=lease.run_id,
                    details={"code": exc.code, "message": exc.message},
                )  # fmt: skip
                reconcile_batch(tx, batch.id, now=now, actor=self.actor, run_id=lease.run_id)
            log("structure_invalid", batch_id=batch.id, code=exc.code)
            return False

        rows = [
            {
                "id": record_id(batch.id, raw.line_no),
                "batch_id": batch.id,
                "line_no": raw.line_no,
                "raw_payload": raw.fields,
                "raw_row_error": raw.row_error,
            }
            for raw in parsed.records
        ]
        control = parsed.control
        with self.repo.transaction() as tx:
            inserted = tx.insert_records(rows, now)
            ensure_batch_transition(BatchStatus.VALIDATING, BatchStatus.PROCESSING)
            tx.update_batch(
                batch.id, now=now, held_by_run=lease.run_id, status=BatchStatus.PROCESSING,
                business_date=parsed.business_date, structure_validated_at=now,
                control_record_count=control.record_count if control else None,
                control_debit_minor=control.debit_minor if control else None,
                control_credit_minor=control.credit_minor if control else None,
            )  # fmt: skip
            tx.audit(
                now=now, actor=self.actor, entity_type="batch", entity_id=batch.id, batch_id=batch.id,
                action="STRUCTURE_VALIDATED", from_state=BatchStatus.VALIDATING,
                to_state=BatchStatus.PROCESSING, run_id=lease.run_id,
                details={"records": len(rows), "newly_registered": inserted},
            )  # fmt: skip
        return True

    def _process_records(self, lease: Lease) -> None:
        with self.repo.transaction() as tx:
            batch = tx.get_batch(lease.batch_id)
            pending = tx.list_records(batch.id, [str(s) for s in MACHINE_PENDING_RECORD_STATES])
        adapter = self.adapters[batch.source_type]
        for rec in pending:
            self._current_record = rec.id
            if rec.status == RecordStatus.RECEIVED:
                self._hook(rec, "validate")
                rec = self._validate_record(lease, batch, adapter, rec.id)
            if rec.status == RecordStatus.VALIDATED:
                self._hook(rec, "classify")
                self._classify_record(lease, rec.id)
        self._current_record = None

    def _hook(self, rec: RecordRow, stage: str) -> None:
        if self.hooks.before_record is not None:
            self.hooks.before_record(rec, stage)

    # Step A ------------------------------------------------------------------
    def _validate_record(self, lease: Lease, batch: BatchRow, adapter: SourceAdapter, rid: str) -> RecordRow:
        now = self.clock.now()
        with self.repo.transaction() as tx:
            self._renew(tx, lease)
            rec = tx.get_record(rid)
            if rec.status != RecordStatus.RECEIVED:
                return rec  # already done by an earlier attempt
            raw = RawRecord(rec.line_no, rec.raw_payload, rec.raw_row_error)
            result = validate_canonical(adapter.to_canonical(raw), received_on=batch.received_at.date())
            fields = {name: result.parsed.get(name) for name in _NORMALISED_FIELDS}
            details: dict[str, object] = {"line_no": rec.line_no}

            if result.record is None:
                target = RecordStatus.VALIDATION_FAILED
                for issue in result.issues:
                    tx.insert_issue(
                        batch_id=batch.id, record_id=rid, code=issue.code, field=issue.field,
                        message=issue.message, severity=str(issue.severity), now=now,
                    )  # fmt: skip
                details["issues"] = [i.code for i in result.issues]
                extra: dict[str, object] = {}
            else:
                fp = fingerprint(result.record.business_fields())
                claim = tx.claim(batch.source_type, result.record.source_ref, rid, fp, now)
                extra = {"content_fingerprint": fp}
                if claim.acquired:
                    target = RecordStatus.VALIDATED
                elif claim.owner_fingerprint == fp:
                    target = RecordStatus.DUPLICATE
                    extra["duplicate_of"] = claim.owner_record_id
                    details["duplicate_of"] = claim.owner_record_id
                else:
                    target = RecordStatus.NEEDS_REVIEW
                    review_id = new_event_id()
                    tx.insert_review(
                        id=review_id, record_id=rid, batch_id=batch.id, reason=ReviewReason.DUPLICATE_CONFLICT,
                        conflicting_record_id=claim.owner_record_id, status="OPEN", created_at=now,
                    )  # fmt: skip
                    details.update(reason=str(ReviewReason.DUPLICATE_CONFLICT), review_id=review_id,
                                   conflicts_with=claim.owner_record_id)  # fmt: skip

            ensure_record_transition(RecordStatus.RECEIVED, target)
            tx.update_record(rid, expected_version=rec.version, now=now, status=target, **fields, **extra)
            tx.audit(
                now=now, actor=self.actor, entity_type="record", entity_id=rid, batch_id=batch.id,
                action=f"RECORD_{target}", from_state=RecordStatus.RECEIVED, to_state=target,
                run_id=lease.run_id, details=details,
            )  # fmt: skip
            return tx.get_record(rid)

    # Step B ------------------------------------------------------------------
    def _classify_record(self, lease: Lease, rid: str) -> None:
        now = self.clock.now()
        with self.repo.transaction() as tx:
            self._renew(tx, lease)
            rec = tx.get_record(rid)
            if rec.status != RecordStatus.VALIDATED:
                return
            normalised = NormalisedRecord(
                source_ref=rec.source_ref or "",
                business_date=rec.business_date,  # type: ignore[arg-type]
                counterparty=rec.counterparty or "",
                description=rec.description,
                amount_minor=rec.amount_minor or 0,
                currency=rec.currency or "",
                direction=Direction(rec.direction),
            )
            result = classify_by_rules(normalised)
            tx.insert_classification_attempt(
                id=new_event_id(), record_id=rid, method="RULE", outcome=result.outcome,
                rule_id=result.rule_id, rule_version=result.rule_version,
                matched_rule_ids=json.dumps(list(result.matched_rule_ids)), ruleset_version=RULESET_VERSION,
                category_code=result.category, created_at=now,
            )  # fmt: skip
            details: dict[str, object] = {"line_no": rec.line_no, "rule_outcome": str(result.outcome),
                                          "matched_rules": list(result.matched_rule_ids)}  # fmt: skip
            if result.outcome is RuleOutcome.MATCHED:
                target = RecordStatus.PROCESSED
                ensure_record_transition(RecordStatus.VALIDATED, target)
                tx.update_record(
                    rid, expected_version=rec.version, now=now, status=target, category_code=result.category,
                    category_source=CategorySource.RULE, rule_id=result.rule_id, rule_version=result.rule_version,
                )  # fmt: skip
                details["category"] = result.category
            else:
                # Phase 6 inserts the AI classifier here for NO_MATCH. Until then (and
                # whenever AI is disabled) an unmatched record goes to a human: never "Other".
                reason = (
                    ReviewReason.RULE_CONFLICT
                    if result.outcome is RuleOutcome.CONFLICT
                    else ReviewReason.NO_RULE_AI_DISABLED
                )
                target = RecordStatus.NEEDS_REVIEW
                ensure_record_transition(RecordStatus.VALIDATED, target)
                review_id = new_event_id()
                tx.insert_review(
                    id=review_id, record_id=rid, batch_id=rec.batch_id, reason=reason, status="OPEN",
                    created_at=now,
                )  # fmt: skip
                tx.update_record(rid, expected_version=rec.version, now=now, status=target)
                details.update(reason=str(reason), review_id=review_id)
            tx.audit(
                now=now, actor=self.actor, entity_type="record", entity_id=rid, batch_id=rec.batch_id,
                action=f"RECORD_{target}", from_state=RecordStatus.VALIDATED, to_state=target,
                run_id=lease.run_id, details=details,
            )  # fmt: skip

    # ------------------------------------------------------------- finishing
    def _finalise(self, lease: Lease) -> None:
        now = self.clock.now()
        with self.repo.transaction() as tx:
            batch = tx.get_batch(lease.batch_id)
            leftover = tx.list_records(batch.id, [str(s) for s in MACHINE_PENDING_RECORD_STATES])
            if leftover:
                raise RuntimeError(f"invariant broken: {len(leftover)} record(s) still need machine work")
            target = BatchStatus.NEEDS_REVIEW if tx.count_open_reviews(batch.id) else BatchStatus.PROCESSED
            ensure_batch_transition(BatchStatus(batch.status), target)
            tx.update_batch(
                batch.id, now=now, held_by_run=lease.run_id, status=target,
                lease_run_id=None, lease_expires_at=None,
            )  # fmt: skip
            tx.finish_run(lease.run_id, outcome=RunOutcome.COMPLETED, finished_at=now)
            tx.audit(
                now=now, actor=self.actor, entity_type="batch", entity_id=batch.id, batch_id=batch.id,
                action=f"BATCH_{target}", from_state=batch.status, to_state=target, run_id=lease.run_id,
            )  # fmt: skip
            report = reconcile_batch(tx, batch.id, now=now, actor=self.actor, run_id=lease.run_id)
        log("batch_finished", batch_id=lease.batch_id, run_id=lease.run_id, status=str(target),
            reconciliation=str(report.status))  # fmt: skip

    def _end_attempt(self, lease: Lease, code: str, detail: str, *, transient: bool) -> None:
        now = self.clock.now()
        try:
            with self.repo.transaction() as tx:
                batch = tx.get_batch(lease.batch_id)
                current = BatchStatus(batch.status)
                if transient and batch.retry_budget_used < batch.max_attempts:
                    delay = self._backoff(batch.retry_budget_used)
                    target, outcome = BatchStatus.RETRY_PENDING, RunOutcome.RETRY_SCHEDULED
                    changes: dict[str, object] = {"next_attempt_at": now + delay}
                else:
                    target, outcome = BatchStatus.FAILED, RunOutcome.FAILED
                    changes = {}
                ensure_batch_transition(current, target)
                tx.update_batch(
                    batch.id, now=now, held_by_run=lease.run_id, status=target,
                    lease_run_id=None, lease_expires_at=None, **changes,
                )  # fmt: skip
                tx.finish_run(lease.run_id, outcome=outcome, finished_at=now, error_code=code, error_detail=detail)
                tx.audit(
                    now=now, actor=self.actor, entity_type="batch", entity_id=batch.id, batch_id=batch.id,
                    action=f"BATCH_{target}", from_state=current, to_state=target, run_id=lease.run_id,
                    details={"error_code": code, "error_detail": detail, **{k: str(v) for k, v in changes.items()}},
                )  # fmt: skip
                reconcile_batch(tx, batch.id, now=now, actor=self.actor, run_id=lease.run_id)
            log("attempt_ended", batch_id=lease.batch_id, run_id=lease.run_id, status=str(target), error_code=code)
        except LeaseLost:
            log("lease_lost_while_failing", batch_id=lease.batch_id, run_id=lease.run_id, error_code=code)

    def _backoff(self, attempts_used: int) -> timedelta:
        c = self.config
        base = min(c.backoff_base_seconds * 2 ** max(attempts_used - 1, 0), c.backoff_cap_seconds)
        return timedelta(seconds=base * self._rng.uniform(0.8, 1.2))
