"""Strict money parsing.

Money is held as integer minor units (cents). Floats are never used: 0.1 + 0.2
is not 0.3 in binary floating point, and reconciliation must be exact.
"""

from __future__ import annotations

import re
from decimal import Decimal

# 1234 | 1,234 | 1234.5 | 1,234.56 | optional leading "$". Thousands groups must be
# well formed ("12,34.00" is rejected rather than guessed).
_AMOUNT_RE = re.compile(r"^\$?(?:\d{1,3}(?:,\d{3})+|\d+)(?:\.\d{1,2})?$")
_NEGATIVE_RE = re.compile(r"^(?:-\$?|\$?-|\().*")


class AmountError(ValueError):
    def __init__(self, code: str, text: str) -> None:
        super().__init__(f"{code}: {text!r}")
        self.code = code
        self.text = text


def parse_amount(text: str) -> int:
    """Parse a positive amount into cents. Raise AmountError with a stable code."""
    value = text.strip()
    if _NEGATIVE_RE.match(value):
        raise AmountError("NON_POSITIVE_AMOUNT", text)
    if not _AMOUNT_RE.match(value):
        raise AmountError("MALFORMED_AMOUNT", text)
    cents = int(Decimal(value.lstrip("$").replace(",", "")) * 100)
    if cents <= 0:
        raise AmountError("NON_POSITIVE_AMOUNT", text)
    return cents


def parse_control_amount(text: str) -> int:
    """Control totals may legitimately be zero (e.g. no credits that day)."""
    value = text.strip()
    if value in {"0", "0.0", "0.00"}:
        return 0
    return parse_amount(value)


def format_minor(cents: int | None) -> str:
    if cents is None:
        return "-"
    sign = "-" if cents < 0 else ""
    whole, frac = divmod(abs(cents), 100)
    return f"{sign}{whole:,}.{frac:02d}"
