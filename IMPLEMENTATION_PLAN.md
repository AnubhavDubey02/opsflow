# OpsFlow — Implementation Plan

## 0. Dependency strategy

The vertical slice deliberately uses **only the Python standard library**, so it
runs anywhere with Python 3.11+ and nothing to install. Therefore:

- The **core** (`domain/`, `sources/`, `synth/`, `pipeline/`, `ai/` mock) is written
  in dependency-free Python and runs anywhere.
- Storage is behind a `Repository` port. The vertical slice ships a **SQLite
  adapter** (stdlib `sqlite3`): real SQL, real transactions, real constraints and
  triggers. It remains afterwards as the fast test adapter.
- The **PostgreSQL adapter**, FastAPI, React and Docker phases start once the
  dependencies are available. No untested Postgres code is shipped.
- Tests are `unittest.TestCase` classes: they run today with
  `python -m unittest` and run unchanged under `pytest` later.

## 1. Plan review (critical) — findings and changes

| # | Lens | Finding in the first draft | Change made |
|---|---|---|---|
| R1 | Multiple meanings | `record.decision_ref` was a polymorphic string (rule id *or* attempt id *or* review id). Not joinable, not constrainable. | Replaced by `rule_id`/`rule_version`, `decided_attempt_id` FK, `decided_review_id` FK, with a CHECK tying them to `category_source`. |
| R2 | Multiple meanings | `attempt_count` meant both "retry budget used" and "how many times we ran". | Renamed `retry_budget_used`; history = `COUNT(processing_run)`. |
| R3 | Missing UNKNOWN | A delivery that arrived but was structurally invalid would show as `LATE/MISSING` — false. | Added derived status `ARRIVED_INVALID`. |
| R4 | Missing context | Conflicting duplicate review did not say *what* it conflicts with. | `review_task.conflicting_record_id`. |
| R5 | Partial failure | Reconciliation was only computed on success, so a `FAILED` batch showed `NOT_STARTED` — hiding committed work. | Reconcile on **every** batch terminal/parking transition (`PROCESSED`, `NEEDS_REVIEW`, `FAILED`, `VALIDATION_FAILED`). |
| R6 | Retries / cost | AI cache could have cached `PROVIDER_ERROR`, making outages permanent. | Only `OK`, `LOW_CONFIDENCE`, `INVALID_RESPONSE` are cacheable. |
| R7 | Double counting | Unclear whether source totals include in-file duplicates. | Source totals = everything the sender sent (matches their control); processed totals exclude duplicates. Both reported. |
| R8 | Test evidence | Tests would hand-assert a few cases. | Generator emits a **scenario manifest** (expected final status + reason per record); the end-to-end test asserts the pipeline matches it exactly. |
| R9 | Unnecessary complexity | `expected_delivery`, AI, review, retries all in the first slice would be a big-bang build. | Slice = bank CSV + rules + SQLite + CLI + manifest tests. Everything else is layered on in later phases. |
| R10 | Unnecessary complexity | `version` column on every table. | Only on `source_batch`, `record`, `review_task` (the rows that are concurrently updated). |
| R11 | Auditability | Audit immutability relied on convention. | DB trigger rejects `UPDATE`/`DELETE` on `audit_event` (SQLite and Postgres). |
| R13 | Retries | A lease keyed by worker id lets a restarted worker with the same id mistake an old lease for its own. | Lease is held by `lease_run_id` (unique per attempt). |
| R14 | Partial failure | A retry could not tell whether the file had already been parsed. | `structure_validated_at` checkpoint; `RETRY_PENDING → VALIDATING` or `→ PROCESSING` accordingly. |
| R15 | Missing UNKNOWN | When two rules matched, rule order would silently pick one. | Rule outcome `CONFLICT` ⇒ review reason `RULE_CONFLICT`. |
| R16 | Missing PENDING | A batch that failed midway reported totals `UNKNOWN` because unprocessed rows had no amounts — really it is *not finished*. | Precedence FAIL > PENDING > UNKNOWN > RECONCILED; unprocessed rows count as pending. |
| R17 | Retries (crash loops) | A batch that crashes the worker every time would be reclaimed forever. | Reclaim consumes retry budget; exhausted ⇒ `FAILED` with run `ABANDONED`. |
| R18 | Deterministic replay (found by running the slice) | A FAILED batch let a *later* overlapping file claim its transactions first: no double counting, but totals depended on *when* the failure happened. | Per-source FIFO: a delivery waits behind an earlier unfinished/FAILED delivery of the same source (F25); waiting is shown, never silent. `NEEDS_REVIEW` and `VALIDATION_FAILED` do not block. |
| R19 | Deterministic replay (found by SQL review) | Rejecting a record released its business key, so an identical later copy was PROCESSED or DUPLICATE depending on whether the reviewer had decided yet. | Claims are never released; copies inherit the rejection, changed copies go to review. This is also what makes it safe for `NEEDS_REVIEW` not to block. |
| R20 | Orphaned state (found by SQL review) | Schema allowed `VALIDATING`/`PROCESSING` without a lease — invisible to the lease query forever. | CHECK made biconditional: lease ⇔ working status. |
| R12 | Human override | Overriding an already-`PROCESSED` record was undefined. | Explicitly out of scope for v1 (needs adjustment entries); documented in FAILURE_MODEL §5 rather than half-built. |

## 2. Phases

| Phase | Deliverable | Exit evidence |
|---|---|---|
| 1 | Architecture, data model, failure model, plans (this set) | reviewed docs |
| 2 | `synth/` seeded generator for bank CSV (+ invoices, API later) with scenario manifest | same seed ⇒ byte-identical files; manifest covers F1–F12 |
| 3a | `domain/` + `sources/bank_csv` + SQLite repository + `pipeline` + CLI — **vertical slice** | `python -m opsflow demo` runs end to end; manifest test green |
| 3b | PostgreSQL adapter + migrations (needs deps) | same test suite green against Postgres |
| 4 | Reconciliation engine + `expected_delivery` lateness | F9–F13 tests |
| 5 | FastAPI: ingest upload, batches, records, issues, audit, rerun | API tests via TestClient |
| 6 | AI port: mock (fault-injectable) + provider adapters; caching; thresholds | F16, F18–F20 tests |
| 7 | Review workflow: queue, approve/correct/reject, optimistic locking | F5, F22, F23 tests |
| 8 | Retries/backoff, leases, abandoned runs, operator rerun; invoice + API sources | F14–F17, F21 tests |
| 9 | React dashboard: batches, reconciliation, review queue, audit timeline, deliveries | manual demo script + screenshots of **synthetic** data |
| 10 | Test hardening: property tests for parsers, coverage report | coverage ≥ 85% on core |
| 11 | Docker compose (postgres, api, worker, web) | `docker compose up` + demo |
| 12 | README, diagrams, demo walkthrough, confidentiality scan script | a new user completes the Definition of Done unaided |

## 3. Vertical slice scope (Phase 2 + 3a)

```
python -m opsflow generate --seed 7 --out data/inbox     # synthetic files + manifest
python -m opsflow ingest data/inbox/*.csv                 # idempotent
python -m opsflow process                                 # run worker once
python -m opsflow report                                  # batches, reconciliation, issues
python -m opsflow audit <batch-id>                        # audit trail
python -m opsflow demo                                    # all of the above in a temp DB
python -m unittest discover -s tests                      # tests
```

Covered scenarios in the slice: F1, F2, F3, F4, F5 (parked as `NEEDS_REVIEW`),
F6, F7, F9, F10, F11, F12, F17, F20 (rules-only mode ⇒ review), F21 (simulated crash).

## 4. Open decisions

1. **Dependencies.** Phases 3b, 5, 9, 10 (pytest), 11 need FastAPI, SQLAlchemy/psycopg,
   pytest, Node and Docker.
2. **AI provider** for Phase 6 real adapters (needs an API key and spending) —
   the mock provider keeps the project fully functional without one.

## 5. Confidentiality controls built into the repo

- `scripts/confidentiality_scan.py` (a minimal version ships with the slice)
  fails on hostnames, internal-looking URLs, absolute local paths, email
  addresses, private keys and secret-like strings. **Organisation-specific terms
  are read from a denylist file kept outside the repository** (path via
  `OPSFLOW_DENYLIST`), so the public repo never names who must not be named.
- All sample data is produced by `synth/` from a seed; no hand-copied data files.
- No git remote is configured by tooling, ever.
