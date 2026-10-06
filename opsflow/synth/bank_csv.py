"""Synthetic bank-export generator for the fictional Kestrel Freight Co.

Produces a set of daily files, each exercising specific failure scenarios
(FAILURE_MODEL.md), plus a **manifest**: the expected final status of every
record and batch. The manifest is the test oracle: the end-to-end test asserts
the pipeline reaches exactly these outcomes.

Same seed => byte-identical output.
"""

from __future__ import annotations

import csv
import io
import json
import random
from dataclasses import asdict, dataclass, field, replace
from datetime import date, timedelta
from pathlib import Path

from opsflow.sources.bank_csv import COLUMNS, FIRST_DATA_LINE

# (payee, memo, category, dr_cr, min, max). Invented counterparties.
_TEMPLATES = (
    ("Petroway Fuels #{n}", "Diesel fill bay {n}", "FUEL", "D", 80, 650),
    ("Coastline Tollway Authority", "E-Pass top-up", "TOLLS_PARKING", "D", 20, 120),
    ("Ridgeback Tyre & Brake", "Brake pad replacement unit {n}", "VEHICLE_MAINTENANCE", "D", 150, 1400),
    ("Meridian Haulage Ltd", "Subcontract leg {n}", "SUBCONTRACT_HAULAGE", "D", 400, 2600),
    ("Copperleaf Software", "Monthly subscription", "OFFICE_SOFTWARE", "D", 29, 240),
    ("First Harbor Bank", "Monthly service charge", "BANK_FEES", "D", 5, 45),
    ("Alder & Finch Retail", "Remittance INV-{n}", "FREIGHT_REVENUE", "C", 900, 7800),
    ("Norrow Foods", "Pallet storage week {n}", "STORAGE_REVENUE", "C", 300, 2400),
)


@dataclass(frozen=True)
class Row:
    txn_id: str
    posted_on: str
    payee: str
    memo: str
    amount: str
    currency: str
    dr_cr: str
    # What the *sender* believes this row is worth (feeds their trailer totals).
    intended_minor: int | None
    expect_status: str
    expect_reason: str | None = None
    expect_category: str | None = None
    columns_override: tuple[str, ...] | None = None  # emit a malformed row

    def cells(self) -> tuple[str, ...]:
        if self.columns_override is not None:
            return self.columns_override
        return (self.txn_id, self.posted_on, self.payee, self.memo, self.amount, self.currency, self.dr_cr)


@dataclass
class ExpectedRecord:
    line_no: int
    status: str
    reason: str | None
    category: str | None


@dataclass
class GeneratedFile:
    name: str
    scenario: str
    business_date: str
    expected_batch_status: str
    expected_reconciliation: str
    content: bytes = field(repr=False)
    records: list[ExpectedRecord] = field(default_factory=list)


def _money_text(minor: int, rng: random.Random) -> str:
    whole, frac = divmod(minor, 100)
    text = f"{whole:,}.{frac:02d}" if whole >= 1000 else f"{whole}.{frac:02d}"
    return f"${text}" if rng.random() < 0.25 else text


class _DayBuilder:
    def __init__(self, rng: random.Random, day: date) -> None:
        self.rng = rng
        self.day = day
        self.rows: list[Row] = []
        self._seq = 0

    def next_ref(self) -> str:
        self._seq += 1
        return f"TX-{self.day:%y%m%d}-{self._seq:04d}"

    def clean(self, template_index: int | None = None) -> Row:
        idx = self.rng.randrange(len(_TEMPLATES)) if template_index is None else template_index
        payee, memo, category, dr_cr, lo, hi = _TEMPLATES[idx]
        n = self.rng.randint(2, 98)
        minor = self.rng.randint(lo * 100, hi * 100)
        row = Row(
            txn_id=self.next_ref(), posted_on=self.day.isoformat(), payee=payee.format(n=n),
            memo=memo.format(n=n), amount=_money_text(minor, self.rng), currency="USD", dr_cr=dr_cr,
            intended_minor=minor, expect_status="PROCESSED", expect_category=category,
        )  # fmt: skip
        self.rows.append(row)
        return row

    def add(self, row: Row) -> Row:
        self.rows.append(row)
        return row

    def variant(self, **changes: object) -> Row:
        """A row based on a fresh clean template, then deliberately broken."""
        base = self.clean()
        self.rows.pop()
        return self.add(replace(base, expect_category=None, **changes))  # type: ignore[arg-type]

    def render(
        self,
        *,
        name: str,
        scenario: str,
        expected_batch: str,
        expected_recon: str,
        count_delta: int = 0,
        debit_delta_minor: int = 0,
        include_trailer: bool = True,
    ) -> GeneratedFile:
        buf = io.StringIO()
        buf.write(f"#KFX-BANK-EXPORT v1,business_date={self.day.isoformat()}\n")
        writer = csv.writer(buf, lineterminator="\n")
        writer.writerow(COLUMNS)
        for row in self.rows:
            writer.writerow(row.cells())
        debits = sum(r.intended_minor or 0 for r in self.rows if r.dr_cr == "D") + debit_delta_minor
        credits = sum(r.intended_minor or 0 for r in self.rows if r.dr_cr == "C")
        if include_trailer:
            buf.write(
                f"#TRAILER,count={len(self.rows) + count_delta},"
                f"debits={debits // 100}.{debits % 100:02d},credits={credits // 100}.{credits % 100:02d}\n"
            )
        records = (
            [
                ExpectedRecord(FIRST_DATA_LINE + i, r.expect_status, r.expect_reason, r.expect_category)
                for i, r in enumerate(self.rows)
            ]
            if include_trailer
            else []  # structurally invalid: no records are ever registered
        )
        return GeneratedFile(
            name=name, scenario=scenario, business_date=self.day.isoformat(),
            expected_batch_status=expected_batch, expected_reconciliation=expected_recon,
            content=buf.getvalue().encode("utf-8"), records=records,
        )  # fmt: skip


def generate_bank_files(seed: int = 7, start: date = date(2026, 3, 2)) -> list[GeneratedFile]:
    rng = random.Random(seed)
    files: list[GeneratedFile] = []

    # Day 1 - F1: clean file, every template represented.
    d1 = _DayBuilder(rng, start)
    for i in range(len(_TEMPLATES)):
        d1.clean(i)
    for _ in range(4):
        d1.clean()
    files.append(d1.render(name=f"kfx_bank_{d1.day}.csv", scenario="F1 clean file",
                           expected_batch="PROCESSED", expected_recon="RECONCILED"))  # fmt: skip

    # Day 2 - messy rows; trailer is correct.
    d2 = _DayBuilder(rng, start + timedelta(days=1))
    clean_rows = [d2.clean() for _ in range(6)]
    d2.add(replace(clean_rows[1], expect_status="DUPLICATE", expect_category=None))  # F3 exact in-file duplicate
    d2.variant(payee="", expect_status="VALIDATION_FAILED", expect_reason="REQUIRED_FIELD_MISSING")  # F6
    d2.variant(posted_on="2026-02-30", expect_status="VALIDATION_FAILED", expect_reason="MALFORMED_DATE")  # F7
    d2.variant(currency="EUR", expect_status="VALIDATION_FAILED", expect_reason="UNSUPPORTED_CURRENCY")  # F7
    d2.variant(posted_on="2030-01-15", expect_status="VALIDATION_FAILED", expect_reason="DATE_IN_FUTURE")  # F7
    d2.add(Row(d2.next_ref(), d2.day.isoformat(), "Quickstop Fuel & Tyre", "Fuel and tyre rotation", "214.80",
               "USD", "D", 21480, "NEEDS_REVIEW", "RULE_CONFLICT"))  # two rules disagree  # fmt: skip
    d2.add(Row(d2.next_ref(), d2.day.isoformat(), "Hartwell Catering", "Team lunch - depot", "86.40",
               "USD", "D", 8640, "NEEDS_REVIEW", "NO_RULE_AI_DISABLED"))  # F20  # fmt: skip
    original = clean_rows[2]
    changed_minor = (original.intended_minor or 0) + 1000
    d2.add(replace(original, amount=f"{changed_minor // 100}.{changed_minor % 100:02d}",
                   intended_minor=changed_minor, expect_status="NEEDS_REVIEW",
                   expect_reason="DUPLICATE_CONFLICT", expect_category=None))  # F5  # fmt: skip
    files.append(d2.render(name=f"kfx_bank_{d2.day}.csv", scenario="F3 F5 F6 F7 F20 messy records",
                           expected_batch="NEEDS_REVIEW", expected_recon="PENDING"))  # fmt: skip

    # Day 3 - F4: export overlaps the previous day's file by three identical rows.
    d3 = _DayBuilder(rng, start + timedelta(days=2))
    for row in d1.rows[:3]:
        d3.add(replace(row, expect_status="DUPLICATE", expect_category=None))
    for _ in range(8):
        d3.clean()
    files.append(d3.render(name=f"kfx_bank_{d3.day}.csv", scenario="F4 cross-file duplicates",
                           expected_batch="PROCESSED", expected_recon="RECONCILED"))  # fmt: skip

    # Day 4 - F11: unparseable amounts; the sender's trailer includes their true values.
    d4 = _DayBuilder(rng, start + timedelta(days=3))
    for _ in range(5):
        d4.clean()
    d4.variant(amount="12,3a.00", intended_minor=1230, expect_status="VALIDATION_FAILED",
               expect_reason="MALFORMED_AMOUNT")  # fmt: skip
    d4.variant(amount="-45.00", intended_minor=4500, expect_status="VALIDATION_FAILED",
               expect_reason="NON_POSITIVE_AMOUNT")  # fmt: skip
    short = d4.clean()
    d4.rows.pop()
    d4.add(replace(short, columns_override=(short.txn_id, short.posted_on, short.payee),
                   expect_status="VALIDATION_FAILED", expect_reason="MALFORMED_ROW",
                   expect_category=None))  # fmt: skip
    files.append(d4.render(name=f"kfx_bank_{d4.day}.csv", scenario="F7 F11 unparseable amounts",
                           expected_batch="PROCESSED", expected_recon="UNKNOWN"))  # fmt: skip

    # Day 5 - F9: trailer claims one more row than the file contains.
    d5 = _DayBuilder(rng, start + timedelta(days=4))
    for _ in range(8):
        d5.clean()
    files.append(d5.render(name=f"kfx_bank_{d5.day}.csv", scenario="F9 trailer count mismatch",
                           expected_batch="PROCESSED", expected_recon="MISMATCH", count_delta=1))  # fmt: skip

    # Day 6 - F10: trailer debit total is 10.00 higher than the rows.
    d6 = _DayBuilder(rng, start + timedelta(days=5))
    for _ in range(8):
        d6.clean()
    files.append(d6.render(name=f"kfx_bank_{d6.day}.csv", scenario="F10 trailer total mismatch",
                           expected_batch="PROCESSED", expected_recon="MISMATCH",
                           debit_delta_minor=1000))  # fmt: skip

    # Day 7 - F12: file truncated before the trailer.
    d7 = _DayBuilder(rng, start + timedelta(days=6))
    for _ in range(5):
        d7.clean()
    files.append(d7.render(name=f"kfx_bank_{d7.day}.csv", scenario="F12 missing trailer",
                           expected_batch="VALIDATION_FAILED", expected_recon="UNKNOWN",
                           include_trailer=False))  # fmt: skip
    return files


def manifest(files: list[GeneratedFile], seed: int) -> dict[str, object]:
    return {
        "generator": "opsflow.synth.bank_csv",
        "seed": seed,
        "note": "Synthetic data for a fictional company. Expected outcomes are the test oracle.",
        "resubmit_to_test_idempotency": [files[0].name],
        "files": [
            {k: v for k, v in asdict(f).items() if k != "content"} for f in files
        ],
    }


def write_bank_files(out_dir: Path, seed: int = 7) -> list[Path]:
    out_dir.mkdir(parents=True, exist_ok=True)
    files = generate_bank_files(seed)
    paths = []
    for f in files:
        path = out_dir / f.name
        path.write_bytes(f.content)
        paths.append(path)
    (out_dir / "manifest.json").write_text(json.dumps(manifest(files, seed), indent=2) + "\n", encoding="utf-8")
    return paths
