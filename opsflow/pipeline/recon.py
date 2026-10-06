"""Batch reconciliation inside an existing transaction."""

from __future__ import annotations

from datetime import datetime

from opsflow.domain.reconcile import ReconciliationReport, reconcile
from opsflow.storage.sqlite import SqliteTx


def reconcile_batch(
    tx: SqliteTx, batch_id: str, *, now: datetime, actor: str, run_id: str | None = None
) -> ReconciliationReport:
    batch = tx.get_batch(batch_id)  # fresh read: status may have just changed in this transaction
    report = reconcile(tx.reconciliation_facts(batch))
    previous = batch.reconciliation_status
    tx.set_reconciliation(batch_id, report, now)
    tx.audit(
        now=now, actor=actor, entity_type="batch", entity_id=batch_id, batch_id=batch_id,
        action="RECONCILIATION_COMPUTED", from_state=previous, to_state=str(report.status), run_id=run_id,
        details={
            "pending": report.pending,
            "failed_checks": [c.name for c in report.checks if c.result == "FAIL"],
            "unknown_checks": [c.name for c in report.checks if c.result == "UNKNOWN"],
        },
    )  # fmt: skip
    return report
