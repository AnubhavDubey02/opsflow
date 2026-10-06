# OpsFlow

**Turn messy, manual back-office data handling into a reliable, auditable workflow.**

OpsFlow ingests business money data (bank exports today; invoices and an API in
later phases). It normalises and validates every record and categorises it with
explainable rules. Anything uncertain goes to a human. Each delivery is then
reconciled against the sender's own control totals. Every step is idempotent,
resumable after a crash, and written to an append-only audit trail.

> All data in this repository is **synthetic**, generated from a seed for the
> fictional company *Kestrel Freight Co.* No real company, person or system is
> represented.

## Why it exists

Small operations teams copy CSVs into spreadsheets and hope nothing was counted
twice. OpsFlow answers the questions that process can't:

- *Did we receive everything we were sent?* (completeness against control totals)
- *Was anything counted twice?* (deterministic IDs, business-key claims, duplicate detection)
- *Why was this record rejected or categorised this way?* (validation issues, rule evidence, audit)
- *What happens when something fails halfway?* (leases, checkpoints, retries, safe reruns)

## Quick start (Python 3.11+, no dependencies)

```bash
python -m opsflow demo              # full story in a temporary database (~2 s)
python -m unittest discover -s tests -t .   # 56 tests
```

Step by step, against a local database file:

```bash
python -m opsflow generate --out data/inbox        # 7 synthetic files + manifest.json
python -m opsflow ingest data/inbox/*.csv          # idempotent: re-ingesting does nothing
python -m opsflow process                          # worker: validate, dedupe, classify, reconcile
python -m opsflow report                           # operations overview
python -m opsflow show <batch-id-prefix>           # checks, validation issues, records, runs
python -m opsflow reviews                          # human review queue
python -m opsflow review <id> correct --category FUEL --note "receipt shows fuel"
python -m opsflow review <id> reject --note "not company spend"
python -m opsflow rerun <batch> --reason "provider restored"   # FAILED batches only
python -m opsflow audit <batch>                    # append-only audit trail
```

PowerShell users: `python -m opsflow ingest (Get-ChildItem data\inbox\*.csv)`.

## What the demo shows

| File | Scenario | Expected outcome |
|---|---|---|
| day 1 | clean file; also resubmitted, and a fault is injected mid-run | duplicate submission ignored; batch `FAILED`, later files **wait**; rerun resumes → `PROCESSED` / `RECONCILED` |
| day 2 | in-file duplicate, missing field, impossible date, EUR, future date, rule conflict, no rule, same id with a different amount | `NEEDS_REVIEW` / `PENDING` → after three human decisions `PROCESSED` / `RECONCILED` |
| day 3 | export overlaps day 1 by three rows | 3 × `DUPLICATE`, never double counted → `RECONCILED` |
| day 4 | unparseable and negative amounts, short row | `PROCESSED` / **`UNKNOWN`**: the totals cannot be proven, so they aren't claimed |
| day 5 | trailer count says one more row than sent | `MISMATCH` (completeness) |
| day 6 | trailer debit total 10.00 too high | `MISMATCH` (total) |
| day 7 | file truncated before its trailer | `VALIDATION_FAILED`, zero records created |

## Design highlights

- **Two separate status fields.** Execution status (`PROCESSED`) and correctness (`RECONCILED` / `MISMATCH` /
  `UNKNOWN`) are kept apart. A batch can finish running and still not balance.
- **Never "unknown → success".** No "Other" category. Unparseable totals give `UNKNOWN`. A crashed run is
  `ABANDONED`, not failed or succeeded.
- **Idempotent everywhere.** Content-hash delivery IDs, deterministic record IDs (UUIDv5), atomic
  business-key claims, compare-and-set state transitions.
- **Totals are recomputed from current state, never incremented.** Reruns and reviews cannot double count.
- **Per-source arrival order.** A delivery never overtakes an earlier failed one, so the results are the same
  whenever a failure happens (replay determinism).
- **The database enforces the rules.** CHECK constraints, a partial unique index (one open review per
  record), and triggers that make the audit trail and the raw input immutable.
- **Testable time.** An injected clock gives deterministic backoff, lease-expiry and crash tests.

Read more: [ARCHITECTURE.md](ARCHITECTURE.md) · [DATA_MODEL.md](DATA_MODEL.md) ·
[FAILURE_MODEL.md](FAILURE_MODEL.md) · [IMPLEMENTATION_PLAN.md](IMPLEMENTATION_PLAN.md) ·
[LEARNING_PLAN.md](LEARNING_PLAN.md) · [learning/](learning/)

## Project layout

```
opsflow/
  domain/      pure logic: states, money, validation, rules, reconciliation, ids
  sources/     source adapters (bank CSV)
  synth/       seeded synthetic data + expected-outcome manifest (test oracle)
  storage/     SQLite adapter + schema (PostgreSQL adapter: phase 3b)
  pipeline/    ingest, worker (leases/retries), review, rerun, reconciliation
  cli.py       command-line interface
tests/         unit, end-to-end, crash/retry and DB-invariant tests
scripts/       confidentiality scan
learning/      study notes per module
```

## Status

The vertical slice is complete. See `IMPLEMENTATION_PLAN.md` for the remaining phases: PostgreSQL, FastAPI,
AI classification (with a mock provider), invoice and API sources, React dashboard, Docker.

## Before publishing

```bash
OPSFLOW_DENYLIST=/path/outside/repo/denylist.txt python scripts/confidentiality_scan.py
```
