"""Human review decisions (approve / correct / reject)."""

from __future__ import annotations

from opsflow.clock import Clock
from opsflow.domain.errors import ConcurrencyError, InvalidTransition
from opsflow.domain.states import (
    BatchStatus,
    CategorySource,
    RecordStatus,
    ReviewDecision,
    ensure_batch_transition,
    ensure_record_transition,
)
from opsflow.pipeline.recon import reconcile_batch
from opsflow.storage.sqlite import SqliteRepository


def resolve_review(
    repo: SqliteRepository,
    clock: Clock,
    review_id: str,
    *,
    decision: str,
    actor: str,
    category: str | None = None,
    note: str | None = None,
    expected_version: int | None = None,
) -> None:
    decision_ = ReviewDecision(decision)
    note = (note or "").strip() or None
    now = clock.now()
    with repo.transaction() as tx:
        task = tx.get_review(review_id)
        if task["status"] != "OPEN":
            raise InvalidTransition("review", task["status"], "RESOLVED")
        if expected_version is not None and expected_version != task["version"]:
            raise ConcurrencyError(f"review {review_id} was changed by someone else; refresh and retry")
        record = tx.get_record(task["record_id"])

        if decision_ is ReviewDecision.APPROVE:
            decided = task["suggested_category"]
            if decided is None:
                raise ValueError("nothing to approve: this review has no suggestion; use CORRECT with a category")
        elif decision_ is ReviewDecision.CORRECT:
            if not category:
                raise ValueError("CORRECT needs a category")
            if note is None:
                raise ValueError("CORRECT needs a note explaining the decision")
            decided = category
        else:
            if note is None:
                raise ValueError("REJECT needs a note explaining the decision")
            decided = None

        if decided is not None:
            direction = tx.category_direction(decided)
            if direction is None:
                raise ValueError(f"unknown category {decided!r}")
            if direction != record.direction:
                raise ValueError(f"category {decided} is for {direction} records; this record is {record.direction}")

        target = RecordStatus.REJECTED if decision_ is ReviewDecision.REJECT else RecordStatus.PROCESSED
        ensure_record_transition(RecordStatus(record.status), target)

        tx.update_review(
            review_id, expected_version=task["version"], status="RESOLVED", decision=decision_,
            decided_category=decided, decided_by=actor, decided_at=now, note=note,
        )  # fmt: skip
        if target is RecordStatus.PROCESSED:
            tx.update_record(
                record.id, expected_version=record.version, now=now, status=target,
                category_code=decided, category_source=CategorySource.HUMAN, decided_review_id=review_id,
            )  # fmt: skip
        else:
            # The business-key claim is deliberately KEPT: an identical copy arriving
            # later inherits this rejection (DUPLICATE), and a changed copy goes to
            # review as DUPLICATE_CONFLICT. Releasing it would make later outcomes
            # depend on *when* the reviewer decided.
            tx.update_record(record.id, expected_version=record.version, now=now, status=target)

        batch_id = record.batch_id
        tx.audit(
            now=now, actor=actor, entity_type="review", entity_id=review_id, batch_id=batch_id,
            action="REVIEW_RESOLVED", from_state="OPEN", to_state="RESOLVED",
            details={"decision": str(decision_), "category": decided, "note": note, "reason": task["reason"]},
        )  # fmt: skip
        tx.audit(
            now=now, actor=actor, entity_type="record", entity_id=record.id, batch_id=batch_id,
            action=f"RECORD_{target}", from_state=record.status, to_state=target,
            details={"line_no": record.line_no, "review_id": review_id},
        )  # fmt: skip

        batch = tx.get_batch(batch_id)
        if batch.status == BatchStatus.NEEDS_REVIEW and tx.count_open_reviews(batch_id) == 0:
            ensure_batch_transition(BatchStatus.NEEDS_REVIEW, BatchStatus.PROCESSED)
            tx.update_batch(batch_id, now=now, expected_version=batch.version, status=BatchStatus.PROCESSED)
            tx.audit(
                now=now, actor=actor, entity_type="batch", entity_id=batch_id, batch_id=batch_id,
                action="BATCH_REVIEWS_COMPLETE", from_state=BatchStatus.NEEDS_REVIEW,
                to_state=BatchStatus.PROCESSED,
            )  # fmt: skip
        reconcile_batch(tx, batch_id, now=now, actor=actor)
