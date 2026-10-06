# 01 — Domain layer (`opsflow/domain/`)

## What was built

Pure business logic with no I/O:

| File | Purpose |
|---|---|
| `states.py` | Enums for every status, plus legal-transition tables and `ensure_*_transition` |
| `money.py` | Strict amount parsing into integer cents; stable error codes |
| `models.py` | Frozen dataclasses: `RawRecord`, `ControlTotals`, `NormalisedRecord`, `ValidationResult` |
| `validation.py` | Normalise + validate one canonical record; collect *all* issues |
| `rules.py` | Deterministic categorisation; detects `CONFLICT` instead of "first match wins" |
| `reconcile.py` | Turns aggregate facts into checks and an overall status |
| `ids.py` | Deterministic UUIDv5 IDs and content fingerprints |
| `errors.py` | Exception hierarchy; the *class* of an error decides the pipeline's reaction |

## Why it exists

Business rules that sit inside SQL or HTTP handlers can't be tested in isolation.
Keeping them pure means about 30 of the tests run with no database at all, in milliseconds.

## Key decisions

1. **Money is `int` cents.** `0.1 + 0.2 != 0.3` in floats. `Decimal` is used only to *parse*.
2. **Sign never carries meaning.** Amounts are always > 0; `direction` says in or out. A negative amount is
   rejected (`NON_POSITIVE_AMOUNT`), never quietly treated as a credit.
3. **Validation collects every issue** and also returns what *could* be parsed. A row with a bad date still
   contributes its amount to the source totals, so reconciliation stays exact.
4. **Rules evaluate all matches.** If two rules disagree, a human decides. Changing rule order can never
   silently change results.
5. **Reconciliation precedence:** FAIL > PENDING > UNKNOWN > RECONCILED. A known sum already above the control
   total is a definite FAIL, even with unknown amounts, because amounts are positive.

## SQL concepts it prepares for

- Constraining a status column to a fixed set (`CHECK (status IN (...))`).
- Integer money columns (`BIGINT` cents).
- Natural keys (`source_ref`) vs surrogate keys (`id`).

## Python concepts

`enum.StrEnum`, `@dataclass(frozen=True, slots=True)`, `frozenset`, `re` (anchored patterns),
`decimal.Decimal`, `uuid.uuid5`, `hashlib`, custom exception hierarchies, the walrus operator `:=`.

## Failure modes handled

Malformed or negative amounts, impossible dates (2026-02-30), future dates, unsupported currency, malformed
direction, missing fields, short rows, rule conflicts, unknown statuses (the identity check fails).

## Questions you should be able to answer

1. Why does `parse_amount("12,34.00")` fail instead of returning 1234.00?
2. Why does a `VALIDATION_FAILED` record still have `amount_minor` set?
3. What would go wrong if rules were "first match wins"?
4. Why is `PENDING` ranked above `UNKNOWN` when choosing the overall status?
5. Why are record IDs derived from `(batch_id, line_no)` rather than random?

## Changes to make yourself (write the test first)

1. Add a WARNING-level issue `LARGE_AMOUNT` for amounts above 25,000.00. The record must still pass, and the
   warning must not block it.
2. Add a rule `R-PARK-002` for "car park" that maps to `TOLLS_PARKING`, then prove with a test that a record
   matching both toll rules is still `MATCHED`, not `CONFLICT`.
