"""Explicit state machines for batches and records.

Every status change in OpsFlow goes through ``ensure_*_transition`` so an illegal
move (for example approving a record that is already PROCESSED) fails loudly
instead of silently corrupting state.
"""

from __future__ import annotations

from enum import StrEnum

from opsflow.domain.errors import InvalidTransition


class BatchStatus(StrEnum):
    """Execution status of a delivery. Says nothing about correctness."""

    RECEIVED = "RECEIVED"
    VALIDATING = "VALIDATING"
    VALIDATION_FAILED = "VALIDATION_FAILED"
    PROCESSING = "PROCESSING"
    NEEDS_REVIEW = "NEEDS_REVIEW"
    RETRY_PENDING = "RETRY_PENDING"
    FAILED = "FAILED"
    PROCESSED = "PROCESSED"


class RecordStatus(StrEnum):
    RECEIVED = "RECEIVED"
    VALIDATED = "VALIDATED"
    VALIDATION_FAILED = "VALIDATION_FAILED"
    DUPLICATE = "DUPLICATE"
    NEEDS_REVIEW = "NEEDS_REVIEW"
    PROCESSED = "PROCESSED"
    REJECTED = "REJECTED"


class ReconciliationStatus(StrEnum):
    """Correctness of a delivery, kept separate from execution status."""

    NOT_STARTED = "NOT_STARTED"
    PENDING = "PENDING"
    RECONCILED = "RECONCILED"
    MISMATCH = "MISMATCH"
    UNKNOWN = "UNKNOWN"


class RunOutcome(StrEnum):
    RUNNING = "RUNNING"
    COMPLETED = "COMPLETED"  # finished without a technical error (batch status says how)
    RETRY_SCHEDULED = "RETRY_SCHEDULED"
    FAILED = "FAILED"
    ABANDONED = "ABANDONED"  # lease expired while RUNNING: we do not know what happened


class CategorySource(StrEnum):
    RULE = "RULE"
    AI = "AI"
    HUMAN = "HUMAN"


class ReviewReason(StrEnum):
    LOW_CONFIDENCE = "LOW_CONFIDENCE"
    AI_INVALID_RESPONSE = "AI_INVALID_RESPONSE"
    NO_RULE_AI_DISABLED = "NO_RULE_AI_DISABLED"
    RULE_CONFLICT = "RULE_CONFLICT"
    DUPLICATE_CONFLICT = "DUPLICATE_CONFLICT"


class ReviewDecision(StrEnum):
    APPROVE = "APPROVE"
    CORRECT = "CORRECT"
    REJECT = "REJECT"


_B = BatchStatus
BATCH_TRANSITIONS: dict[BatchStatus, frozenset[BatchStatus]] = {
    _B.RECEIVED: frozenset({_B.VALIDATING}),
    # Self-loops are lease reclaims after a crashed worker.
    _B.VALIDATING: frozenset({_B.VALIDATING, _B.VALIDATION_FAILED, _B.PROCESSING, _B.FAILED}),
    _B.PROCESSING: frozenset(
        {_B.PROCESSING, _B.PROCESSED, _B.NEEDS_REVIEW, _B.RETRY_PENDING, _B.FAILED}
    ),
    _B.RETRY_PENDING: frozenset({_B.VALIDATING, _B.PROCESSING, _B.FAILED}),
    _B.FAILED: frozenset({_B.RETRY_PENDING}),  # operator rerun only
    _B.NEEDS_REVIEW: frozenset({_B.PROCESSED}),
    _B.VALIDATION_FAILED: frozenset(),
    _B.PROCESSED: frozenset(),
}

_R = RecordStatus
RECORD_TRANSITIONS: dict[RecordStatus, frozenset[RecordStatus]] = {
    _R.RECEIVED: frozenset({_R.VALIDATION_FAILED, _R.DUPLICATE, _R.NEEDS_REVIEW, _R.VALIDATED}),
    _R.VALIDATED: frozenset({_R.PROCESSED, _R.NEEDS_REVIEW}),
    _R.NEEDS_REVIEW: frozenset({_R.PROCESSED, _R.REJECTED}),
    _R.VALIDATION_FAILED: frozenset(),
    _R.DUPLICATE: frozenset(),
    _R.PROCESSED: frozenset(),
    _R.REJECTED: frozenset(),
}

# Records in these states still need work (machine or human) before totals are final.
PENDING_RECORD_STATES = frozenset({_R.RECEIVED, _R.VALIDATED, _R.NEEDS_REVIEW})
# Records the worker still has to touch.
MACHINE_PENDING_RECORD_STATES = frozenset({_R.RECEIVED, _R.VALIDATED})
# Batches a worker may pick up (subject to lease/backoff timing).
LEASABLE_BATCH_STATES = frozenset({_B.RECEIVED, _B.RETRY_PENDING, _B.VALIDATING, _B.PROCESSING})


def ensure_batch_transition(current: BatchStatus, target: BatchStatus) -> None:
    if target not in BATCH_TRANSITIONS[current]:
        raise InvalidTransition("batch", str(current), str(target))


def ensure_record_transition(current: RecordStatus, target: RecordStatus) -> None:
    if target not in RECORD_TRANSITIONS[current]:
        raise InvalidTransition("record", str(current), str(target))
