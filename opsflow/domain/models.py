"""Value objects shared across modules. Frozen so they cannot be mutated by accident."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date
from enum import StrEnum


class SourceType(StrEnum):
    BANK_CSV = "BANK_CSV"
    INVOICE_DOC = "INVOICE_DOC"
    BOOKING_API = "BOOKING_API"


class Direction(StrEnum):
    DEBIT = "DEBIT"  # money out
    CREDIT = "CREDIT"  # money in


class Severity(StrEnum):
    ERROR = "ERROR"  # blocks the record
    WARNING = "WARNING"  # recorded, does not block


@dataclass(frozen=True, slots=True)
class RawRecord:
    """One row exactly as the source delivered it (stored verbatim, never edited)."""

    line_no: int
    fields: dict[str, str]
    row_error: str | None = None  # e.g. wrong column count: the row exists but is unreadable


@dataclass(frozen=True, slots=True)
class ControlTotals:
    """What the *sender* says they sent. ``None`` means the sender did not say."""

    record_count: int | None
    debit_minor: int | None
    credit_minor: int | None


@dataclass(frozen=True, slots=True)
class ParsedDelivery:
    source_type: SourceType
    business_date: date | None
    records: tuple[RawRecord, ...]
    control: ControlTotals | None


@dataclass(frozen=True, slots=True)
class NormalisedRecord:
    source_ref: str
    business_date: date
    counterparty: str
    description: str | None
    amount_minor: int
    currency: str
    direction: Direction

    def business_fields(self) -> dict[str, object]:
        """Fields that define *what happened*; used for duplicate fingerprints."""
        return {
            "source_ref": self.source_ref,
            "business_date": self.business_date.isoformat(),
            "counterparty": self.counterparty.casefold(),
            "description": (self.description or "").casefold(),
            "amount_minor": self.amount_minor,
            "currency": self.currency,
            "direction": str(self.direction),
        }


@dataclass(frozen=True, slots=True)
class ValidationIssue:
    code: str
    field: str | None
    message: str
    severity: Severity = Severity.ERROR


@dataclass(frozen=True, slots=True)
class ValidationResult:
    """``parsed`` holds every field that *could* be parsed, even when the record failed,
    so reconciliation can still count a valid amount on an otherwise invalid row."""

    parsed: dict[str, object]
    issues: tuple[ValidationIssue, ...] = field(default=())
    record: NormalisedRecord | None = None

    @property
    def ok(self) -> bool:
        return self.record is not None
