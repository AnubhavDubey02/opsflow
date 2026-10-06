"""Deterministic categorisation rules (first line of classification).

All rules are evaluated, never "first match wins": if rules disagree the outcome
is CONFLICT and a human decides. Rule order must never silently pick a category.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from enum import StrEnum

from opsflow.domain.models import Direction, NormalisedRecord

RULESET_VERSION = "2026.1"


class RuleOutcome(StrEnum):
    MATCHED = "MATCHED"
    NO_MATCH = "NO_MATCH"
    CONFLICT = "CONFLICT"


@dataclass(frozen=True, slots=True)
class Rule:
    rule_id: str
    version: int
    category: str
    direction: Direction
    pattern: re.Pattern[str]

    def matches(self, record: NormalisedRecord) -> bool:
        if record.direction is not self.direction:
            return False
        haystack = f"{record.counterparty} {record.description or ''}".casefold()
        return self.pattern.search(haystack) is not None


@dataclass(frozen=True, slots=True)
class RuleResult:
    outcome: RuleOutcome
    category: str | None
    rule_id: str | None
    rule_version: int | None
    matched_rule_ids: tuple[str, ...]


def _rule(rule_id: str, category: str, direction: Direction, pattern: str, version: int = 1) -> Rule:
    return Rule(rule_id, version, category, direction, re.compile(pattern, re.IGNORECASE))


DEFAULT_RULES: tuple[Rule, ...] = (
    _rule("R-FUEL-001", "FUEL", Direction.DEBIT, r"\b(fuel|diesel|petrol|truckstop|ev charging)\b"),
    _rule("R-TOLL-001", "TOLLS_PARKING", Direction.DEBIT, r"\b(toll|tollway|parking|e-?pass)\b"),
    _rule("R-MAINT-001", "VEHICLE_MAINTENANCE", Direction.DEBIT, r"\b(tyres?|tires?|brakes?|mechanic|lube)\b"),
    _rule("R-HAUL-001", "SUBCONTRACT_HAULAGE", Direction.DEBIT, r"\b(haulage|subcontract(ed)?)\b"),
    _rule("R-SOFT-001", "OFFICE_SOFTWARE", Direction.DEBIT, r"\b(software|subscription|stationery)\b"),
    _rule("R-FEE-001", "BANK_FEES", Direction.DEBIT, r"\b(service charge|card fee|wire fee)\b"),
    _rule("R-FRT-001", "FREIGHT_REVENUE", Direction.CREDIT, r"\b(remittance|freight charges)\b"),
    _rule("R-STOR-001", "STORAGE_REVENUE", Direction.CREDIT, r"\b(pallet storage|warehousing)\b"),
)


def classify_by_rules(record: NormalisedRecord, rules: tuple[Rule, ...] = DEFAULT_RULES) -> RuleResult:
    matched = sorted((r for r in rules if r.matches(record)), key=lambda r: r.rule_id)
    categories = {r.category for r in matched}
    ids = tuple(r.rule_id for r in matched)
    if not matched:
        return RuleResult(RuleOutcome.NO_MATCH, None, None, None, ids)
    if len(categories) > 1:
        return RuleResult(RuleOutcome.CONFLICT, None, None, None, ids)
    first = matched[0]  # several rules agreeing on one category: lowest id is cited
    return RuleResult(RuleOutcome.MATCHED, first.category, first.rule_id, first.version, ids)
