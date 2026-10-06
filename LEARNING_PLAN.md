# OpsFlow — Learning Plan

You learn by **predicting, then reading, then changing** the code. Each module
gets a note in `learning/` with: what was built, why, key decisions, SQL concepts,
Python concepts, failure modes, 5 questions, 2 changes for you to implement.

## Working rules

1. **Predict before you run.** Before each command, write down what you expect
   (state, counts, error). Compare. Mismatches are the lesson.
2. **Explain before you ask.** When something breaks: "I think it is X because Y;
   this would prove me wrong: Z." Then investigate.
3. **Change it yourself.** Every note ends with two changes. Write the test first.
4. **No copying from work.** Every example, rule and dataset here is invented. Keep
   it that way.

## Skill map

| Module | Python you will learn | SQL / data you will learn | Reliability concepts |
|---|---|---|---|
| `domain/states` | `enum.StrEnum`, frozensets, custom exceptions | state as a constrained column | explicit state machines, illegal transitions |
| `domain/money`, `normalise`, `validate` | `decimal.Decimal`, `re`, dataclasses (`frozen=True`, `slots=True`), pure functions | why `BIGINT` cents beat `FLOAT` | fail the smallest unit, error codes vs messages |
| `domain/dedupe`, ids | `hashlib`, `uuid.uuid5`, canonical JSON | natural keys vs surrogate keys, `UNIQUE`, `INSERT … ON CONFLICT` | idempotency, deterministic replay |
| `synth/` | `random.Random(seed)`, generators, `csv` | control totals, trailer records | test oracles (the manifest) |
| `storage/sqlite` | context managers, `sqlite3`, protocols (`typing.Protocol`) | DDL, CHECK constraints, partial indexes, triggers, transactions, aggregates with `FILTER`/`CASE` | atomic state + audit, append-only logs |
| `pipeline/` | dependency injection, `Clock` abstraction, structured logging | compare-and-set `UPDATE … WHERE status = ?`, leases | partial failure, resumable checkpoints, retries |
| `reconcile` | pure aggregation logic | `GROUP BY`, conditional aggregation, reconciling sums | never derive totals from counters |
| Phase 5+ (API) | FastAPI, Pydantic, dependency overrides | pagination, filtering | 409 conflicts, idempotency keys |
| Phase 6 (AI) | Protocols, strict JSON parsing, timeouts | caching by key | confidence thresholds, never trusting model output |
| Phase 7–8 | optimistic locking | `version` columns, `SKIP LOCKED` | human-in-the-loop, backoff + jitter |

## Milestone checks (you, unaided)

| After | You can… |
|---|---|
| Slice | draw both state machines from memory; explain why a rerun cannot double count; write the reconciliation SQL from scratch |
| Phase 5 | add an endpoint with validation and a test |
| Phase 6 | add a new AI failure mode to the mock and handle it |
| Phase 8 | explain what happens if the worker is killed at any line of `process_batch` |
| Phase 12 | demo the project in 5 minutes to a non-technical client and in 15 to an engineer |
