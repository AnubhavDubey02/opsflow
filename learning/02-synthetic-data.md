# 02 — Synthetic data and the manifest oracle (`opsflow/synth/`, `opsflow/sources/`)

## What was built

- `sources/bank_csv.py`: parses an invented export format (header, column line, rows, `#TRAILER` with control
  count and totals).
- `synth/bank_csv.py`: a seeded generator producing seven daily files, each built to trigger specific
  scenarios from FAILURE_MODEL.md, plus `manifest.json` with the **expected outcome of every record and batch**.

## Why it exists

You can't test reliability on happy-path data. You also must never borrow real data, even "anonymised".
Generating it from a seed means the data is invented, reproducible and shareable.
The manifest turns the generator into a *test oracle*: the end-to-end test asserts every record reaches exactly
the status the generator intended.

## Key decisions

1. **Structure errors fail the file; row errors fail the row.** A missing trailer means we can't know what the
   sender intended, so nothing is registered. A short row is still a row: it is registered and fails alone.
2. **The trailer reflects the sender's view.** It includes amounts we can't parse. That is why day 4 reconciles
   to `UNKNOWN` rather than pretending the totals match.
3. **Same seed ⇒ byte-identical files.** Tested.

## SQL concepts

Control totals and trailer records are the classic batch-file integrity mechanism. The reconciliation query
compares them with `SUM(CASE WHEN … THEN amount_minor END)`.

## Python concepts

`random.Random(seed)` (an isolated RNG, not the global one), `csv.reader` / `csv.writer` (quoting, embedded
commas), `dataclasses.replace`, `io.StringIO`, `utf-8-sig` (BOM handling).

## Failure modes covered

F1, F3–F7, F9–F12 and F20, plus a duplicate submission (F2) driven by resubmitting the day-1 file.

## Questions you should be able to answer

1. Why is a file without a trailer rejected entirely, while a 3-column row is not?
2. Day 2 contains the same transaction twice. Why do the trailer totals still match?
3. What does the manifest let a test prove that hand-written assertions usually don't?
4. Why does the generator use `random.Random(seed)` rather than `random.seed(seed)`?
5. Which scenario would break if the generator rounded amounts with floats?

## Changes to make yourself

1. Add a day-8 file whose trailer *credit* total is wrong. Add it to the manifest and make the end-to-end test
   cover it.
2. Make the parser reject a file whose header `business_date` differs from every row's `posted_on` by more than
   7 days. Decide first: is that a structure error or a row warning? Write down why.
