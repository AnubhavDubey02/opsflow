"""Reconciliation maths (pure).

The repository gathers facts with one aggregate query over *current* record
states; this module turns them into checks and an overall status. Totals are
always recomputed, never incremented, so reruns and reviews cannot double count.
See DATA_MODEL.md section 4.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import StrEnum

from opsflow.domain.models import ControlTotals
from opsflow.domain.states import PENDING_RECORD_STATES, ReconciliationStatus, RecordStatus


class CheckResult(StrEnum):
    PASS = "PASS"
    FAIL = "FAIL"
    UNKNOWN = "UNKNOWN"
    NOT_APPLICABLE = "NOT_APPLICABLE"


@dataclass(frozen=True, slots=True)
class Check:
    name: str
    result: CheckResult
    expected: object = None
    actual: object = None
    note: str | None = None


@dataclass(frozen=True, slots=True)
class ReconciliationFacts:
    received: int
    status_counts: dict[str, int]
    control: ControlTotals | None
    # Sums over every record whose amount AND direction could be parsed, whatever its status.
    source_debit_minor: int
    source_credit_minor: int
    # Records whose amount or direction is not known (unparseable, or not yet normalised).
    unknown_amount_count: int
    # What flows on to reporting: PROCESSED records only.
    processed_debit_minor: int
    processed_credit_minor: int
    structure_invalid: bool = False


@dataclass(frozen=True, slots=True)
class ReconciliationReport:
    status: ReconciliationStatus
    checks: tuple[Check, ...]
    facts: ReconciliationFacts
    pending: int = field(default=0)


def _total_check(name: str, control_value: int | None, known_sum: int, unknown: int) -> Check:
    if control_value is None:
        return Check(name, CheckResult.NOT_APPLICABLE, note="sender provided no control total")
    if unknown == 0:
        result = CheckResult.PASS if known_sum == control_value else CheckResult.FAIL
        return Check(name, result, expected=control_value, actual=known_sum)
    if known_sum > control_value:
        # Every amount is > 0, so the unknown ones can only make the gap bigger.
        return Check(name, CheckResult.FAIL, control_value, known_sum, note=f"{unknown} amount(s) unknown")
    return Check(name, CheckResult.UNKNOWN, control_value, known_sum, note=f"{unknown} amount(s) unknown")


def reconcile(facts: ReconciliationFacts) -> ReconciliationReport:
    if facts.structure_invalid:
        check = Check("structure", CheckResult.UNKNOWN, note="delivery could not be parsed")
        return ReconciliationReport(ReconciliationStatus.UNKNOWN, (check,), facts)

    counts = facts.status_counts
    known_states = {str(s) for s in RecordStatus}
    accounted = sum(n for status, n in counts.items() if status in known_states)
    pending = sum(counts.get(str(s), 0) for s in PENDING_RECORD_STATES)

    checks = [
        Check(
            "identity",
            CheckResult.PASS if accounted == facts.received else CheckResult.FAIL,
            expected=facts.received,
            actual=accounted,
            note="received = processed + rejected + validation_failed + duplicate + pending",
        )
    ]
    control = facts.control or ControlTotals(None, None, None)
    if control.record_count is None:
        checks.append(Check("completeness", CheckResult.NOT_APPLICABLE, note="no control count"))
    else:
        checks.append(
            Check(
                "completeness",
                CheckResult.PASS if facts.received == control.record_count else CheckResult.FAIL,
                expected=control.record_count,
                actual=facts.received,
            )
        )
    checks.append(
        _total_check("source_debit_total", control.debit_minor, facts.source_debit_minor, facts.unknown_amount_count)
    )
    checks.append(
        _total_check("source_credit_total", control.credit_minor, facts.source_credit_minor, facts.unknown_amount_count)
    )
    checks.append(
        Check(
            "no_pending",
            CheckResult.PASS if pending == 0 else CheckResult.UNKNOWN,
            expected=0,
            actual=pending,
        )
    )

    results = {c.result for c in checks}
    if CheckResult.FAIL in results:
        status = ReconciliationStatus.MISMATCH
    elif pending > 0:
        status = ReconciliationStatus.PENDING
    elif CheckResult.UNKNOWN in results:
        status = ReconciliationStatus.UNKNOWN
    else:
        status = ReconciliationStatus.RECONCILED
    return ReconciliationReport(status, tuple(checks), facts, pending)
