# OpsFlow — Failure Model

Every failure has (1) a detection point, (2) an explicit resulting state, (3) an
operator-visible signal and (4) a recovery path. "Expected behaviour" below is
what the automated tests assert.

## 1. Principles

1. **Never convert unknown into success.** Missing evidence ⇒ `UNKNOWN`,
   `PENDING` or `NEEDS_REVIEW`.
2. **Fail the smallest unit.** A bad row fails the row, not the file; a bad file
   fails the batch, not the system.
3. **Separate transient from permanent.** Transient (timeouts, provider outage)
   ⇒ retry with backoff. Permanent (bad data) ⇒ terminal state + explanation.
   Retrying permanent errors only wastes time and money.
4. **Every retry is safe.** Re-executing any step yields the same result and the
   same IDs, and never double counts.
5. **The record of what happened is written with what happened** (same transaction).

## 2. Error taxonomy

| Class | Examples | Handling |
|---|---|---|
| `DATA` (permanent, per record) | missing field, malformed amount/date, total mismatch, unsupported currency | record `VALIDATION_FAILED` + `validation_issue` rows |
| `STRUCTURE` (permanent, per batch) | wrong header, missing trailer, truncated document, unparseable JSON | batch `VALIDATION_FAILED` + file-level issue |
| `TRANSIENT` | AI timeout/5xx/rate-limit, booking API 503, DB serialisation failure | batch `RETRY_PENDING` with exponential backoff + jitter; `FAILED` when exhausted |
| `AMBIGUOUS` | low AI confidence, invalid AI output, conflicting duplicate | record `NEEDS_REVIEW` + review task |
| `UNEXPECTED` | programming error | batch `FAILED`, run `FAILED` with error, committed work kept |
| `CRASH` | process killed mid-run | lease expires ⇒ run `ABANDONED`; next worker resumes |

## 3. Scenario catalogue

| # | Scenario (synthetic) | Detection | Expected system behaviour | Recovery |
|---|---|---|---|---|
| F1 | **Successful file** | — | all records `PROCESSED`; batch `PROCESSED`; reconciliation `RECONCILED` | — |
| F2 | **Same file submitted twice** | sha256 matches existing batch | no new batch, no new records; existing batch id returned; audit `DUPLICATE_SUBMISSION` | none needed |
| F3 | **Duplicate record within a file** (identical row twice) | claim on `(source_type, source_ref)` with same fingerprint | first `PROCESSED`, second `DUPLICATE` (`duplicate_of` set); excluded from processed totals; source totals still include it so control totals match | none |
| F4 | **Duplicate across files** (overlapping exports) | existing claim, same fingerprint | `DUPLICATE`; not double counted | none |
| F5 | **Conflicting duplicate** (same id, different amount) | existing claim, different fingerprint | `NEEDS_REVIEW` reason `DUPLICATE_CONFLICT`; batch `NEEDS_REVIEW`; reconciliation `PENDING` | reviewer rejects one / approves |
| F6 | **Missing required field** | validator | record `VALIDATION_FAILED`, issue `REQUIRED_FIELD_MISSING(field)`; rest of file continues; batch `PROCESSED`; reconciliation reports the failure (not hidden) | resubmit corrected file → only fixed rows become new work (failed rows never claimed their key), others `DUPLICATE` |
| F7 | **Malformed values** (`12,3a.00`, `2026-13-40`, negative amount, `EUR`) | validator | `VALIDATION_FAILED` with code per field (`MALFORMED_AMOUNT`, `MALFORMED_DATE`, `NON_POSITIVE_AMOUNT`, `UNSUPPORTED_CURRENCY`) | as F6 |
| F8 | **Incorrect invoice total** (lines ≠ stated total) | validator | `VALIDATION_FAILED` `INVOICE_TOTAL_MISMATCH` with expected/actual | vendor reissues invoice |
| F9 | **Trailer count ≠ rows** (truncated/extra rows) | reconciliation `completeness` | batch executes normally; reconciliation `MISMATCH` with expected/actual counts | investigate source; resubmit |
| F10 | **Trailer total ≠ sum of rows** | reconciliation total check | `MISMATCH` | investigate |
| F11 | **Unparseable amount + control total present** | reconciliation | total check `UNKNOWN` (cannot prove the sum) ⇒ overall `UNKNOWN`, never `RECONCILED` | fix source |
| F12 | **Missing trailer / wrong header** | structural parse | batch `VALIDATION_FAILED`; zero records created | resubmit fixed file |
| F13 | **Delayed file** | `expected_delivery` past `due_at` | derived `LATE`, then `MISSING` after grace; dashboard alert; when it finally arrives → `ARRIVED_LATE` | chase sender |
| F14 | **Booking API failure** (503 / timeout) | fetcher | retries with backoff; on exhaustion no batch is created, an audit `FETCH_FAILED` event is written and the expectation stays `LATE`/`MISSING` | rerun the fetch |
| F15 | **API partial pages** (`total_count` > items fetched) | reconciliation `completeness` | `MISMATCH`, not success | re-pull window |
| F16 | **AI provider outage during classification** | provider raises transient | records stay `VALIDATED`; batch `RETRY_PENDING` (`next_attempt_at` = backoff); after `max_attempts` → `FAILED` | operator rerun (optionally with AI disabled ⇒ records go to review) |
| F17 | **Duplicate retry** (rerun after partial success, or two workers) | compare-and-set lease; terminal records skipped; claims; AI cache | no record processed twice; no duplicate audit transitions; same reconciliation result | none |
| F18 | **AI low confidence** (< threshold) | confidence check | `NEEDS_REVIEW` `LOW_CONFIDENCE`; suggestion shown; attempt stored with confidence + threshold | reviewer approves/corrects |
| F19 | **AI invalid structured response** (not JSON, unknown category, confidence out of range) | strict parser | attempt `INVALID_RESPONSE` with `raw_response`; **no retry** (deterministic garbage); `NEEDS_REVIEW` `AI_INVALID_RESPONSE` | reviewer decides |
| F20 | **AI disabled and no rule matches** | pipeline | `NEEDS_REVIEW` `NO_RULE_AI_DISABLED` | reviewer decides |
| F21 | **Partial processing** (crash after N of M records) | lease expiry | run `ABANDONED`; next lease resumes at first non-terminal record; final state identical to an uninterrupted run | automatic |
| F22 | **Reviewer rejects a record** | review API | record `REJECTED`, batch re-reconciled; totals recomputed (not decremented). Its business-key claim is kept, so a later identical copy is `DUPLICATE` (inherits the rejection) whenever it arrives | — |
| F23 | **Concurrent review of same task** | version check | second decision rejected `409 CONFLICT`; one audit event only | refresh |
| F24 | **Illegal transition requested** (e.g. approve a PROCESSED record) | state machine | rejected with `InvalidTransition`; no change; no audit | — |
| F25 | **Earlier delivery of the same source is unfinished or FAILED** | lease query | later deliveries of that source **wait** (not leased) and are shown as `WAITING: earlier delivery … is FAILED`; other sources continue. Ensures duplicate ownership — and therefore every total — is identical however and whenever failures happen | fix / rerun the earlier batch; waiting batches then run in arrival order |

## 4. Retry policy

```
delay(attempt) = min(base * 2^(attempt-1), cap) * jitter(0.8..1.2)
base = 30s, cap = 15min, max_attempts = 3 (configurable)
```

- Jitter uses a seeded RNG in tests (deterministic).
- `attempt_count` counts automatic attempts since the last operator rerun;
  every attempt is also a `processing_run` row, so history is never lost.
- An operator rerun of a `FAILED` batch resets the budget and is audited with the
  operator's name.

## 5. What is deliberately *not* handled in v1

| Gap | Why acceptable now | Planned |
|---|---|---|
| Cross-source matching (invoice ↔ bank payment) | Avoids silent double counting by *not* summing sources together | v2 matching engine |
| Reversing an already-`PROCESSED` record | Requires adjustment entries, not edits | v2 adjustments |
| Multi-currency | Rejected explicitly (`UNSUPPORTED_CURRENCY`), not mis-converted | v2 |
| Poison-pill single record crashing the worker repeatedly | Caught: unexpected errors fail the batch with the record id in `error_detail` | v2 per-record quarantine |
