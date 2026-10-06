"""Operator actions."""

from __future__ import annotations

from opsflow.clock import Clock
from opsflow.domain.errors import InvalidTransition
from opsflow.domain.states import BatchStatus, ensure_batch_transition
from opsflow.storage.sqlite import SqliteRepository


def rerun_batch(repo: SqliteRepository, clock: Clock, batch_id: str, *, actor: str, reason: str) -> None:
    """Queue a FAILED batch for another attempt with a fresh retry budget.

    Safe by construction: terminal records are skipped, business keys are claimed
    atomically, and reconciliation is recomputed from current state.
    """
    if not reason.strip():
        raise ValueError("a rerun needs a reason (it is recorded in the audit trail)")
    now = clock.now()
    with repo.transaction() as tx:
        batch = tx.get_batch(batch_id)
        current = BatchStatus(batch.status)
        if current is not BatchStatus.FAILED:
            raise InvalidTransition("batch", current, BatchStatus.RETRY_PENDING)
        ensure_batch_transition(current, BatchStatus.RETRY_PENDING)
        tx.update_batch(
            batch_id, now=now, expected_version=batch.version,
            status=BatchStatus.RETRY_PENDING, next_attempt_at=now, retry_budget_used=0,
        )  # fmt: skip
        tx.audit(
            now=now, actor=actor, entity_type="batch", entity_id=batch_id, batch_id=batch_id,
            action="OPERATOR_RERUN", from_state=current, to_state=BatchStatus.RETRY_PENDING,
            details={"reason": reason, "previous_budget_used": batch.retry_budget_used},
        )  # fmt: skip
