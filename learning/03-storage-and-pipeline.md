# 03 — Storage and the reliable pipeline (`opsflow/storage/`, `opsflow/pipeline/`)

## What was built

- `storage/schema_sqlite.sql`: tables, CHECK constraints, a partial unique index, and triggers that make the
  audit trail and raw input immutable.
- `storage/sqlite.py`: the repository. Every unit of work is `with repo.transaction() as tx:`.
- `pipeline/ingest.py`: accepts bytes and deduplicates whole deliveries by SHA-256.
- `pipeline/worker.py`: leases a batch, validates structure (checkpoint), processes each record in two short
  transactions, then finalises and reconciles. Transient errors back off and retry; anything unexpected fails
  the batch visibly.
- `pipeline/review.py`: approve, correct or reject, with mandatory reasons and optimistic locking.
- `pipeline/operations.py`: operator rerun of a `FAILED` batch, which is audited.

## Why it exists

This is where reliability is won or lost. A crash, a retry, a second worker or a human decision must never
double count, lose work or rewrite history.

## Key decisions

1. **State change and audit in the same transaction.** The trail can't disagree with the data.
2. **Compare-and-set updates** (`WHERE version = ?`, `WHERE lease_run_id = ? AND lease_expires_at >= ?`). If
   someone else changed the row, `rowcount` is 0, and we raise instead of overwriting.
3. **Leases belong to a run, not a worker.** A restarted worker with the same name can't mistake an old lease for
   its own.
4. **Two transactions per record.** `VALIDATED` is a durable checkpoint: an outage between validation and
   classification resumes cleanly.
5. **Business-key claims** (`INSERT … ON CONFLICT DO NOTHING`) make duplicate detection atomic. Claims are never
   released, so a review decision can't change how a *later* file is processed. The outcome doesn't depend on
   when the human clicked.
6. **Per-source arrival order.** A delivery waits behind an earlier unfinished or `FAILED` delivery from the same
   source. Found by running the demo: without it, which file "owned" an overlapping transaction depended on when
   a failure happened.
7. **Reconciliation is recomputed** with one conditional-aggregation query over current states, and stored as an
   append-only history.

## SQL concepts

`BEGIN IMMEDIATE` / `COMMIT` / `ROLLBACK`, `INSERT … ON CONFLICT DO NOTHING`, optimistic locking with a
`version` column, `CHECK` constraints spanning several columns, partial unique indexes
(`… WHERE status = 'OPEN'`), triggers with `RAISE(ABORT)`, `NOT EXISTS` correlated subqueries (the ordering
rule), `GROUP BY` and `SUM(CASE WHEN …)`, `COALESCE`, `LEFT JOIN`, fixed-width UTC text timestamps (why they
must be fixed-width in SQLite).

## Python concepts

`contextlib.contextmanager`, `BaseException` vs `Exception` (simulated crashes can't be swallowed),
dependency injection (clock, hooks, adapters), keyword-only arguments, `sqlite3.Row`, the `typing.Protocol`
adapter interface.

## Failure modes covered (tests)

F2 duplicate submission · F16 transient outage with backoff · retry budget exhaustion followed by an operator
rerun · F21 crash mid-batch, resumed by another worker to the *identical* result · crash loop stopped by the budget
· unexpected bug (batch FAILED, committed work kept) · F22 rejection inherited by later identical copies · F23 stale or repeated decisions ·
F25 ordering · append-only audit, immutable raw input, and CHECK invariants enforced by the database.

## Questions you should be able to answer

1. A worker is killed after committing line 6's validation but before classifying it. Walk through exactly what
   the next worker does.
2. Why does `renew_lease` refuse to renew an *expired* lease, even if nobody has taken it yet?
3. Why is the simulated crash a `BaseException` subclass?
4. How would two workers racing for the same batch be resolved, and which line of SQL decides it?
5. Why does a `FAILED` batch block later deliveries of its source, but a `NEEDS_REVIEW` batch doesn't?

## Changes to make yourself

1. Add `opsflow stats`: one SQL query returning, per category, the processed count and total debits/credits
   across all batches. Prove duplicates aren't counted.
2. Add `opsflow abandon <batch> --reason`: an operator marks a `FAILED` batch as permanently abandoned so later
   deliveries stop waiting. You'll need a new terminal state, transition-table entries, a CHECK update,
   reconciliation behaviour and an audit event. Update DATA_MODEL.md first.
