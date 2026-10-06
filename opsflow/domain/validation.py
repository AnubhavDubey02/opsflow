"""Normalisation + validation of a canonical record.

Input is the *canonical* dict a source adapter produces (string values or None).
All issues are collected (not just the first) so an operator sees everything
wrong with a row in one go.
"""

from __future__ import annotations

import re
from collections.abc import Mapping
from datetime import date

from opsflow.domain.models import (
    Direction,
    NormalisedRecord,
    Severity,
    ValidationIssue,
    ValidationResult,
)
from opsflow.domain.money import AmountError, parse_amount

SUPPORTED_CURRENCIES = frozenset({"USD"})
REQUIRED_FIELDS = ("source_ref", "business_date", "counterparty", "amount", "currency", "direction")
MAX_REF_LENGTH = 64

_ISO_DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")
_CURRENCY_RE = re.compile(r"^[A-Za-z]{3}$")
_WS_RE = re.compile(r"\s+")
_DIRECTIONS = {
    "D": Direction.DEBIT,
    "DR": Direction.DEBIT,
    "DEBIT": Direction.DEBIT,
    "C": Direction.CREDIT,
    "CR": Direction.CREDIT,
    "CREDIT": Direction.CREDIT,
}


def clean_text(value: str | None) -> str | None:
    """Trim, collapse internal whitespace (incl. tabs / NBSP), map empty to None."""
    if value is None:
        return None
    collapsed = _WS_RE.sub(" ", value.replace("\u00a0", " ")).strip()
    return collapsed or None


def validate_canonical(canonical: Mapping[str, str | None], *, received_on: date) -> ValidationResult:
    issues: list[ValidationIssue] = []
    parsed: dict[str, object] = {}
    values = {key: clean_text(canonical.get(key)) for key in (*REQUIRED_FIELDS, "description")}

    row_error = canonical.get("_row_error")
    if row_error:
        issues.append(ValidationIssue("MALFORMED_ROW", None, row_error))

    for name in REQUIRED_FIELDS:
        if values[name] is None:
            issues.append(ValidationIssue("REQUIRED_FIELD_MISSING", name, f"{name} is required"))

    if (ref := values["source_ref"]) is not None:
        if len(ref) > MAX_REF_LENGTH:
            issues.append(ValidationIssue("REF_TOO_LONG", "source_ref", f"longer than {MAX_REF_LENGTH}"))
        else:
            parsed["source_ref"] = ref.upper()

    if (raw_date := values["business_date"]) is not None:
        try:
            if not _ISO_DATE_RE.match(raw_date):
                raise ValueError
            business_date = date.fromisoformat(raw_date)
        except ValueError:
            issues.append(
                ValidationIssue("MALFORMED_DATE", "business_date", f"{raw_date!r} is not a valid YYYY-MM-DD date")
            )
        else:
            if business_date > received_on:
                issues.append(
                    ValidationIssue(
                        "DATE_IN_FUTURE", "business_date", f"{business_date} is after receipt date {received_on}"
                    )
                )
            else:
                parsed["business_date"] = business_date

    if (counterparty := values["counterparty"]) is not None:
        parsed["counterparty"] = counterparty
    parsed["description"] = values["description"]

    if (raw_amount := values["amount"]) is not None:
        try:
            parsed["amount_minor"] = parse_amount(raw_amount)
        except AmountError as exc:
            issues.append(ValidationIssue(exc.code, "amount", f"{raw_amount!r} is not a positive amount"))

    if (raw_currency := values["currency"]) is not None:
        currency = raw_currency.upper()
        if not _CURRENCY_RE.match(currency):
            issues.append(ValidationIssue("MALFORMED_CURRENCY", "currency", f"{raw_currency!r}"))
        elif currency not in SUPPORTED_CURRENCIES:
            issues.append(ValidationIssue("UNSUPPORTED_CURRENCY", "currency", f"{currency} is not supported"))
        else:
            parsed["currency"] = currency

    if (raw_direction := values["direction"]) is not None:
        direction = _DIRECTIONS.get(raw_direction.upper())
        if direction is None:
            issues.append(ValidationIssue("MALFORMED_DIRECTION", "direction", f"{raw_direction!r}"))
        else:
            parsed["direction"] = direction

    if any(issue.severity is Severity.ERROR for issue in issues):
        return ValidationResult(parsed=parsed, issues=tuple(issues))

    record = NormalisedRecord(
        source_ref=parsed["source_ref"],  # type: ignore[arg-type]
        business_date=parsed["business_date"],  # type: ignore[arg-type]
        counterparty=parsed["counterparty"],  # type: ignore[arg-type]
        description=parsed["description"],  # type: ignore[arg-type]
        amount_minor=parsed["amount_minor"],  # type: ignore[arg-type]
        currency=parsed["currency"],  # type: ignore[arg-type]
        direction=parsed["direction"],  # type: ignore[arg-type]
    )
    return ValidationResult(parsed=parsed, issues=tuple(issues), record=record)
