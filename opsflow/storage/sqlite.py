"""SQLite repository adapter.

Real SQL, real transactions, real constraints. Used for development and the test
suite; the PostgreSQL adapter implements the same methods.

Transactions: every unit of work runs inside ``with repo.transaction() as tx:``.
``BEGIN IMMEDIATE`` takes the write lock up front, so check-then-write sequences
inside one transaction cannot interleave with another writer.
"""

from __future__ import annotations

import json
import sqlite3
from collections.abc import Iterator, Sequence
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import date, datetime
from enum import Enum
from importlib import resources
from pathlib import Path
from typing import Any

from opsflow.clock import from_ts, to_ts
from opsflow.domain.errors import ConcurrencyError, LeaseLost, NotFound
from opsflow.domain.models import ControlTotals
from opsflow.domain.reconcile import ReconciliationFacts, ReconciliationReport


@dataclass(frozen=True, slots=True)
class BatchRow:
    id: str
    received_seq: int
    source_type: str
    source_name: str
    business_date: date | None
    content_sha256: str
    status: str
    reconciliation_status: str
    control: ControlTotals | None
    retry_budget_used: int
    max_attempts: int
    next_attempt_at: datetime | None
    lease_run_id: str | None
    lease_expires_at: datetime | None
    structure_validated_at: datetime | None
    received_at: datetime
    updated_at: datetime
    version: int


@dataclass(frozen=True, slots=True)
class RecordRow:
    id: str
    batch_id: str
    line_no: int
    raw_payload: dict[str, str]
    raw_row_error: str | None
    content_fingerprint: str | None
    status: str
    source_ref: str | None
    business_date: date | None
    counterparty: str | None
    description: str | None
    amount_minor: int | None
    currency: str | None
    direction: str | None
    category_code: str | None
    category_source: str | None
    rule_id: str | None
    duplicate_of: str | None
    version: int


@dataclass(frozen=True, slots=True)
class ClaimResult:
    acquired: bool
    owner_record_id: str
    owner_fingerprint: str


_BATCH_COLUMNS = frozenset(
    {
        "status", "reconciliation_status", "business_date", "control_record_count",
        "control_debit_minor", "control_credit_minor", "retry_budget_used", "next_attempt_at",
        "lease_run_id", "lease_expires_at", "structure_validated_at",
    }
)  # fmt: skip
_RECORD_COLUMNS = frozenset(
    {
        "status", "content_fingerprint", "source_ref", "business_date", "counterparty",
        "description", "amount_minor", "currency", "direction", "category_code",
        "category_source", "rule_id", "rule_version", "decided_attempt_id",
        "decided_review_id", "duplicate_of",
    }
)  # fmt: skip
_REVIEW_COLUMNS = frozenset({"status", "decision", "decided_category", "decided_by", "decided_at", "note"})
_RUN_COLUMNS = frozenset({"finished_at", "outcome", "error_code", "error_detail"})

# Earlier, still-unfinished delivery of the same source (correlated on alias "b").
_UNFINISHED = "('RECEIVED', 'VALIDATING', 'PROCESSING', 'RETRY_PENDING', 'FAILED')"
_PREDECESSOR_SQL = f"""SELECT 1 FROM source_batch p
                       WHERE p.source_type = b.source_type AND p.received_seq < b.received_seq
                         AND p.status IN {_UNFINISHED}"""
_PREDECESSOR_ID_SQL = f"""SELECT p2.id FROM source_batch p2
                          WHERE p2.source_type = b.source_type AND p2.received_seq < b.received_seq
                            AND p2.status IN {_UNFINISHED}
                          ORDER BY p2.received_seq LIMIT 1"""


def _sql_value(value: Any) -> Any:
    if isinstance(value, datetime):
        return to_ts(value)
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, Enum):
        return value.value
    if isinstance(value, (dict, list, tuple)):
        return json.dumps(value, default=str, sort_keys=True)
    return value


def _opt_ts(value: str | None) -> datetime | None:
    return from_ts(value) if value else None


def _opt_date(value: str | None) -> date | None:
    return date.fromisoformat(value) if value else None


def _batch_from_row(r: sqlite3.Row) -> BatchRow:
    has_control = r["control_record_count"] is not None or r["control_debit_minor"] is not None
    control = (
        ControlTotals(r["control_record_count"], r["control_debit_minor"], r["control_credit_minor"])
        if has_control
        else None
    )
    return BatchRow(
        id=r["id"],
        received_seq=r["received_seq"],
        source_type=r["source_type"],
        source_name=r["source_name"],
        business_date=_opt_date(r["business_date"]),
        content_sha256=r["content_sha256"],
        status=r["status"],
        reconciliation_status=r["reconciliation_status"],
        control=control,
        retry_budget_used=r["retry_budget_used"],
        max_attempts=r["max_attempts"],
        next_attempt_at=_opt_ts(r["next_attempt_at"]),
        lease_run_id=r["lease_run_id"],
        lease_expires_at=_opt_ts(r["lease_expires_at"]),
        structure_validated_at=_opt_ts(r["structure_validated_at"]),
        received_at=from_ts(r["received_at"]),
        updated_at=from_ts(r["updated_at"]),
        version=r["version"],
    )


def _record_from_row(r: sqlite3.Row) -> RecordRow:
    return RecordRow(
        id=r["id"],
        batch_id=r["batch_id"],
        line_no=r["line_no"],
        raw_payload=json.loads(r["raw_payload"]),
        raw_row_error=r["raw_row_error"],
        content_fingerprint=r["content_fingerprint"],
        status=r["status"],
        source_ref=r["source_ref"],
        business_date=_opt_date(r["business_date"]),
        counterparty=r["counterparty"],
        description=r["description"],
        amount_minor=r["amount_minor"],
        currency=r["currency"],
        direction=r["direction"],
        category_code=r["category_code"],
        category_source=r["category_source"],
        rule_id=r["rule_id"],
        duplicate_of=r["duplicate_of"],
        version=r["version"],
    )


def _set_clause(changes: dict[str, Any], allowed: frozenset[str]) -> tuple[str, list[Any]]:
    unknown = set(changes) - allowed
    if unknown:
        raise ValueError(f"cannot update columns {sorted(unknown)}")
    # Column names come only from the allow-list above, never from user input.
    parts = [f"{name} = ?" for name in changes]
    return ", ".join(parts), [_sql_value(v) for v in changes.values()]


class SqliteTx:
    """All repository operations available inside one transaction."""

    def __init__(self, conn: sqlite3.Connection) -> None:
        self._c = conn

    # ----------------------------------------------------------------- batches
    def insert_batch_if_absent(
        self,
        *,
        batch_id: str,
        source_type: str,
        source_name: str,
        content_sha256: str,
        content: bytes,
        max_attempts: int,
        now: datetime,
    ) -> bool:
        cur = self._c.execute(
            """INSERT INTO source_batch (id, received_seq, source_type, source_name, content_sha256, status,
                   max_attempts, received_at, updated_at)
               VALUES (?, (SELECT COALESCE(MAX(received_seq), 0) + 1 FROM source_batch),
                       ?, ?, ?, 'RECEIVED', ?, ?, ?)
               ON CONFLICT (source_type, content_sha256) DO NOTHING""",
            (batch_id, source_type, source_name, content_sha256, max_attempts, to_ts(now), to_ts(now)),
        )
        if cur.rowcount == 0:
            return False
        self._c.execute("INSERT INTO source_blob (batch_id, content) VALUES (?, ?)", (batch_id, content))
        return True

    def find_batch_by_hash(self, source_type: str, content_sha256: str) -> BatchRow | None:
        row = self._c.execute(
            "SELECT * FROM source_batch WHERE source_type = ? AND content_sha256 = ?",
            (source_type, content_sha256),
        ).fetchone()
        return _batch_from_row(row) if row else None

    def get_batch(self, batch_id: str) -> BatchRow:
        row = self._c.execute("SELECT * FROM source_batch WHERE id = ?", (batch_id,)).fetchone()
        if row is None:
            raise NotFound(f"batch {batch_id}")
        return _batch_from_row(row)

    def find_batch_by_prefix(self, prefix: str) -> BatchRow:
        rows = self._c.execute(
            "SELECT * FROM source_batch WHERE id LIKE ? || '%' ORDER BY id LIMIT 2", (prefix,)
        ).fetchall()
        if len(rows) != 1:
            raise NotFound(f"no unique batch matching {prefix!r}")
        return _batch_from_row(rows[0])

    def get_blob(self, batch_id: str) -> bytes:
        row = self._c.execute("SELECT content FROM source_blob WHERE batch_id = ?", (batch_id,)).fetchone()
        if row is None:
            raise NotFound(f"blob for batch {batch_id}")
        return bytes(row["content"])

    def list_batches(self) -> list[BatchRow]:
        rows = self._c.execute("SELECT * FROM source_batch ORDER BY received_seq").fetchall()
        return [_batch_from_row(r) for r in rows]

    def leasable_batches(self, now: datetime, limit: int = 10) -> list[BatchRow]:
        """Batches due for work, oldest first.

        Per-source ordering: a delivery never overtakes an earlier delivery of the
        same source that is still unfinished (including FAILED). Duplicate
        detection is first-come-first-served, so without this rule the outcome of a
        replay would depend on *when* a failure happened.
        """
        rows = self._c.execute(
            f"""SELECT * FROM source_batch b
               WHERE (b.status = 'RECEIVED'
                      OR (b.status = 'RETRY_PENDING' AND b.next_attempt_at <= ?)
                      OR (b.status IN ('VALIDATING', 'PROCESSING') AND b.lease_expires_at < ?))
                 AND NOT EXISTS ({_PREDECESSOR_SQL})
               ORDER BY b.received_seq
               LIMIT ?""",
            (to_ts(now), to_ts(now), limit),
        ).fetchall()
        return [_batch_from_row(r) for r in rows]

    def blocking_predecessor(self, batch: BatchRow) -> BatchRow | None:
        row = self._c.execute(
            f"SELECT p.* FROM source_batch b, source_batch p WHERE b.id = ? AND p.id = ({_PREDECESSOR_ID_SQL})",
            (batch.id,),
        ).fetchone()
        return _batch_from_row(row) if row else None

    def update_batch(
        self,
        batch_id: str,
        *,
        now: datetime,
        expected_version: int | None = None,
        held_by_run: str | None = None,
        require_lease_free_or_expired: bool = False,
        **changes: Any,
    ) -> None:
        """Compare-and-set update. At least one guard must be given.

        - ``expected_version``: optimistic lock (nobody changed the row since we read it)
        - ``held_by_run``: the caller's lease must still be valid
        - ``require_lease_free_or_expired``: used when taking a lease
        """
        if expected_version is None and held_by_run is None:
            raise ValueError("update_batch needs expected_version and/or held_by_run")
        set_sql, params = _set_clause(changes, _BATCH_COLUMNS)
        where = ["id = ?"]
        where_params: list[Any] = [batch_id]
        if expected_version is not None:
            where.append("version = ?")
            where_params.append(expected_version)
        if held_by_run is not None:
            where.append("lease_run_id = ? AND lease_expires_at >= ?")
            where_params += [held_by_run, to_ts(now)]
        if require_lease_free_or_expired:
            where.append("(lease_run_id IS NULL OR lease_expires_at < ?)")
            where_params.append(to_ts(now))
        cur = self._c.execute(
            f"UPDATE source_batch SET {set_sql}, updated_at = ?, version = version + 1 WHERE {' AND '.join(where)}",
            [*params, to_ts(now), *where_params],
        )
        if cur.rowcount == 0:
            if held_by_run is not None:
                raise LeaseLost(f"lease on batch {batch_id} is no longer held by run {held_by_run}")
            raise ConcurrencyError(f"batch {batch_id} changed concurrently")

    def renew_lease(self, batch_id: str, run_id: str, now: datetime, expires_at: datetime) -> None:
        cur = self._c.execute(
            """UPDATE source_batch SET lease_expires_at = ?
               WHERE id = ? AND lease_run_id = ? AND lease_expires_at >= ?""",
            (to_ts(expires_at), batch_id, run_id, to_ts(now)),
        )
        if cur.rowcount == 0:
            raise LeaseLost(f"lease on batch {batch_id} is no longer held by run {run_id}")

    def set_reconciliation(self, batch_id: str, report: ReconciliationReport, now: datetime) -> None:
        """Reconciliation is derived data, so it does not bump the batch version."""
        f = report.facts
        counts = f.status_counts
        self._c.execute(
            """INSERT INTO reconciliation_result (batch_id, computed_at, status, received, processed,
                   rejected, validation_failed, duplicate, pending, source_debit_minor,
                   source_credit_minor, processed_debit_minor, processed_credit_minor,
                   unknown_amount_count, checks)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                batch_id, to_ts(now), str(report.status), f.received, counts.get("PROCESSED", 0),
                counts.get("REJECTED", 0), counts.get("VALIDATION_FAILED", 0), counts.get("DUPLICATE", 0),
                report.pending, f.source_debit_minor, f.source_credit_minor, f.processed_debit_minor,
                f.processed_credit_minor, f.unknown_amount_count,
                json.dumps([
                    {"name": c.name, "result": str(c.result), "expected": c.expected,
                     "actual": c.actual, "note": c.note}
                    for c in report.checks
                ]),
            ),
        )  # fmt: skip
        self._c.execute(
            "UPDATE source_batch SET reconciliation_status = ? WHERE id = ?", (str(report.status), batch_id)
        )

    def latest_reconciliation(self, batch_id: str) -> dict[str, Any] | None:
        row = self._c.execute(
            "SELECT * FROM reconciliation_result WHERE batch_id = ? ORDER BY id DESC LIMIT 1", (batch_id,)
        ).fetchone()
        if row is None:
            return None
        result = dict(row)
        result["checks"] = json.loads(result["checks"])
        return result

    def reconciliation_facts(self, batch: BatchRow) -> ReconciliationFacts:
        """One pass of conditional aggregation over the batch's current record states."""
        counts = {
            r["status"]: r["n"]
            for r in self._c.execute(
                "SELECT status, COUNT(*) AS n FROM record WHERE batch_id = ? GROUP BY status", (batch.id,)
            )
        }
        sums = self._c.execute(
            """SELECT
                 COUNT(*) AS received,
                 COALESCE(SUM(CASE WHEN direction = 'DEBIT'  THEN amount_minor END), 0) AS source_debit,
                 COALESCE(SUM(CASE WHEN direction = 'CREDIT' THEN amount_minor END), 0) AS source_credit,
                 COALESCE(SUM(CASE WHEN amount_minor IS NULL OR direction IS NULL THEN 1 ELSE 0 END), 0)
                     AS unknown_amounts,
                 COALESCE(SUM(CASE WHEN status = 'PROCESSED' AND direction = 'DEBIT'
                                   THEN amount_minor END), 0) AS processed_debit,
                 COALESCE(SUM(CASE WHEN status = 'PROCESSED' AND direction = 'CREDIT'
                                   THEN amount_minor END), 0) AS processed_credit
               FROM record WHERE batch_id = ?""",
            (batch.id,),
        ).fetchone()
        return ReconciliationFacts(
            received=sums["received"],
            status_counts=counts,
            control=batch.control,
            source_debit_minor=sums["source_debit"],
            source_credit_minor=sums["source_credit"],
            unknown_amount_count=sums["unknown_amounts"],
            processed_debit_minor=sums["processed_debit"],
            processed_credit_minor=sums["processed_credit"],
            structure_invalid=batch.status == "VALIDATION_FAILED",
        )

    # ----------------------------------------------------------------- records
    def insert_records(self, rows: Sequence[dict[str, Any]], now: datetime) -> int:
        """Idempotent: re-registering an already-registered line is a no-op."""
        inserted = 0
        for row in rows:
            cur = self._c.execute(
                """INSERT INTO record (id, batch_id, line_no, raw_payload, raw_row_error, status,
                       created_at, updated_at)
                   VALUES (?, ?, ?, ?, ?, 'RECEIVED', ?, ?)
                   ON CONFLICT (batch_id, line_no) DO NOTHING""",
                (
                    row["id"], row["batch_id"], row["line_no"], json.dumps(row["raw_payload"], sort_keys=True),
                    row["raw_row_error"], to_ts(now), to_ts(now),
                ),
            )  # fmt: skip
            inserted += cur.rowcount
        return inserted

    def list_records(self, batch_id: str, statuses: Sequence[str] | None = None) -> list[RecordRow]:
        sql = "SELECT * FROM record WHERE batch_id = ?"
        params: list[Any] = [batch_id]
        if statuses:
            sql += f" AND status IN ({', '.join('?' for _ in statuses)})"
            params += [str(s) for s in statuses]
        rows = self._c.execute(sql + " ORDER BY line_no", params).fetchall()
        return [_record_from_row(r) for r in rows]

    def get_record(self, record_id: str) -> RecordRow:
        row = self._c.execute("SELECT * FROM record WHERE id = ?", (record_id,)).fetchone()
        if row is None:
            raise NotFound(f"record {record_id}")
        return _record_from_row(row)

    def update_record(self, record_id: str, *, expected_version: int, now: datetime, **changes: Any) -> None:
        set_sql, params = _set_clause(changes, _RECORD_COLUMNS)
        cur = self._c.execute(
            f"UPDATE record SET {set_sql}, updated_at = ?, version = version + 1 WHERE id = ? AND version = ?",
            [*params, to_ts(now), record_id, expected_version],
        )
        if cur.rowcount == 0:
            raise ConcurrencyError(f"record {record_id} changed concurrently")

    def claim(self, source_type: str, source_ref: str, record_id: str, fingerprint: str, now: datetime) -> ClaimResult:
        cur = self._c.execute(
            """INSERT INTO record_claim (source_type, source_ref, record_id, content_fingerprint, claimed_at)
               VALUES (?, ?, ?, ?, ?) ON CONFLICT (source_type, source_ref) DO NOTHING""",
            (source_type, source_ref, record_id, fingerprint, to_ts(now)),
        )
        if cur.rowcount == 1:
            return ClaimResult(True, record_id, fingerprint)
        row = self._c.execute(
            "SELECT record_id, content_fingerprint FROM record_claim WHERE source_type = ? AND source_ref = ?",
            (source_type, source_ref),
        ).fetchone()
        return ClaimResult(row["record_id"] == record_id, row["record_id"], row["content_fingerprint"])

    # ------------------------------------------------- issues / classification
    def insert_issue(
        self, *, batch_id: str, record_id: str | None, code: str, field: str | None,
        message: str, severity: str, now: datetime,
    ) -> None:  # fmt: skip
        self._c.execute(
            """INSERT INTO validation_issue (batch_id, record_id, code, field, message, severity, created_at)
               VALUES (?, ?, ?, ?, ?, ?, ?)""",
            (batch_id, record_id, code, field, message, severity, to_ts(now)),
        )

    def list_issues(self, batch_id: str) -> list[dict[str, Any]]:
        rows = self._c.execute(
            """SELECT i.*, r.line_no FROM validation_issue i
               LEFT JOIN record r ON r.id = i.record_id
               WHERE i.batch_id = ? ORDER BY r.line_no, i.id""",
            (batch_id,),
        ).fetchall()
        return [dict(r) for r in rows]

    def insert_classification_attempt(self, **values: Any) -> None:
        columns = ", ".join(values)
        placeholders = ", ".join("?" for _ in values)
        self._c.execute(
            f"INSERT INTO classification_attempt ({columns}) VALUES ({placeholders})",
            [_sql_value(v) for v in values.values()],
        )

    def list_classification_attempts(self, record_id: str) -> list[dict[str, Any]]:
        rows = self._c.execute(
            "SELECT * FROM classification_attempt WHERE record_id = ? ORDER BY created_at, id", (record_id,)
        ).fetchall()
        return [dict(r) for r in rows]

    # ----------------------------------------------------------------- reviews
    def insert_review(self, **values: Any) -> None:
        columns = ", ".join(values)
        placeholders = ", ".join("?" for _ in values)
        self._c.execute(
            f"INSERT INTO review_task ({columns}) VALUES ({placeholders})",
            [_sql_value(v) for v in values.values()],
        )

    def get_review(self, review_id: str) -> dict[str, Any]:
        row = self._c.execute("SELECT * FROM review_task WHERE id = ?", (review_id,)).fetchone()
        if row is None:
            raise NotFound(f"review {review_id}")
        return dict(row)

    def find_review_by_prefix(self, prefix: str) -> dict[str, Any]:
        rows = self._c.execute(
            "SELECT * FROM review_task WHERE id LIKE ? || '%' ORDER BY id LIMIT 2", (prefix,)
        ).fetchall()
        if len(rows) != 1:
            raise NotFound(f"no unique review matching {prefix!r}")
        return dict(rows[0])

    def list_reviews(self, status: str | None = "OPEN", batch_id: str | None = None) -> list[dict[str, Any]]:
        sql = """SELECT t.*, r.line_no, r.source_ref, r.counterparty, r.description, r.amount_minor,
                        r.direction, b.source_name
                 FROM review_task t
                 JOIN record r ON r.id = t.record_id
                 JOIN source_batch b ON b.id = t.batch_id
                 WHERE 1 = 1"""
        params: list[Any] = []
        if status:
            sql += " AND t.status = ?"
            params.append(status)
        if batch_id:
            sql += " AND t.batch_id = ?"
            params.append(batch_id)
        rows = self._c.execute(sql + " ORDER BY t.created_at, b.received_at, r.line_no", params).fetchall()
        return [dict(r) for r in rows]

    def count_open_reviews(self, batch_id: str) -> int:
        row = self._c.execute(
            "SELECT COUNT(*) AS n FROM review_task WHERE batch_id = ? AND status = 'OPEN'", (batch_id,)
        ).fetchone()
        return int(row["n"])

    def update_review(self, review_id: str, *, expected_version: int, **changes: Any) -> None:
        set_sql, params = _set_clause(changes, _REVIEW_COLUMNS)
        cur = self._c.execute(
            f"UPDATE review_task SET {set_sql}, version = version + 1 WHERE id = ? AND version = ?",
            [*params, review_id, expected_version],
        )
        if cur.rowcount == 0:
            raise ConcurrencyError(f"review {review_id} changed concurrently")

    def category_direction(self, code: str) -> str | None:
        row = self._c.execute("SELECT direction FROM category WHERE code = ?", (code,)).fetchone()
        return row["direction"] if row else None

    # -------------------------------------------------------------------- runs
    def insert_run(self, *, run_id: str, batch_id: str, worker_id: str, now: datetime) -> int:
        row = self._c.execute(
            "SELECT COALESCE(MAX(attempt_no), 0) + 1 AS n FROM processing_run WHERE batch_id = ?", (batch_id,)
        ).fetchone()
        attempt_no = int(row["n"])
        self._c.execute(
            """INSERT INTO processing_run (id, batch_id, attempt_no, worker_id, started_at, outcome)
               VALUES (?, ?, ?, ?, ?, 'RUNNING')""",
            (run_id, batch_id, attempt_no, worker_id, to_ts(now)),
        )
        return attempt_no

    def finish_run(self, run_id: str, **changes: Any) -> None:
        set_sql, params = _set_clause(changes, _RUN_COLUMNS)
        self._c.execute(
            f"UPDATE processing_run SET {set_sql} WHERE id = ? AND outcome = 'RUNNING'", [*params, run_id]
        )

    def list_runs(self, batch_id: str) -> list[dict[str, Any]]:
        rows = self._c.execute(
            "SELECT * FROM processing_run WHERE batch_id = ? ORDER BY attempt_no", (batch_id,)
        ).fetchall()
        return [dict(r) for r in rows]

    # ------------------------------------------------------------------- audit
    def audit(
        self,
        *,
        now: datetime,
        actor: str,
        entity_type: str,
        entity_id: str,
        action: str,
        batch_id: str | None = None,
        from_state: str | None = None,
        to_state: str | None = None,
        run_id: str | None = None,
        details: dict[str, Any] | None = None,
    ) -> None:
        self._c.execute(
            """INSERT INTO audit_event (occurred_at, actor, entity_type, entity_id, batch_id, action,
                   from_state, to_state, run_id, details)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                to_ts(now), actor, entity_type, entity_id, batch_id, action,
                _sql_value(from_state), _sql_value(to_state), run_id,
                json.dumps(details or {}, default=str, sort_keys=True),
            ),
        )  # fmt: skip

    def list_audit(self, batch_id: str | None = None) -> list[dict[str, Any]]:
        if batch_id is None:
            rows = self._c.execute("SELECT * FROM audit_event ORDER BY id").fetchall()
        else:
            rows = self._c.execute("SELECT * FROM audit_event WHERE batch_id = ? ORDER BY id", (batch_id,)).fetchall()
        return [{**dict(r), "details": json.loads(r["details"])} for r in rows]

    def execute(self, sql: str, params: Sequence[Any] = ()) -> list[sqlite3.Row]:
        """Escape hatch for tests and ad-hoc reporting (read-only use recommended)."""
        return self._c.execute(sql, params).fetchall()


class SqliteRepository:
    def __init__(self, path: str | Path) -> None:
        self._path = str(path)
        # isolation_level=None: we issue BEGIN/COMMIT ourselves (explicit transactions).
        self._conn = sqlite3.connect(self._path, isolation_level=None, timeout=30)
        self._conn.row_factory = sqlite3.Row
        self._conn.execute("PRAGMA foreign_keys = ON")
        self._conn.execute("PRAGMA journal_mode = WAL")
        self._conn.execute("PRAGMA busy_timeout = 30000")

    def migrate(self) -> None:
        schema = resources.files("opsflow.storage").joinpath("schema_sqlite.sql").read_text(encoding="utf-8")
        self._conn.executescript(schema)

    @contextmanager
    def transaction(self) -> Iterator[SqliteTx]:
        self._conn.execute("BEGIN IMMEDIATE")
        try:
            yield SqliteTx(self._conn)
        except BaseException:
            self._conn.execute("ROLLBACK")
            raise
        else:
            self._conn.execute("COMMIT")

    def close(self) -> None:
        self._conn.close()
