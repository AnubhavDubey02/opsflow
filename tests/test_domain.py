from __future__ import annotations

import unittest
from datetime import date

from opsflow.domain.errors import InvalidTransition
from opsflow.domain.models import ControlTotals, Direction, NormalisedRecord
from opsflow.domain.money import AmountError, format_minor, parse_amount, parse_control_amount
from opsflow.domain.reconcile import CheckResult, ReconciliationFacts, reconcile
from opsflow.domain.rules import RuleOutcome, classify_by_rules
from opsflow.domain.states import (
    BatchStatus,
    ReconciliationStatus,
    RecordStatus,
    ensure_batch_transition,
    ensure_record_transition,
)
from opsflow.domain.validation import clean_text, validate_canonical

RECEIVED_ON = date(2026, 3, 10)


def canonical(**overrides: str | None) -> dict[str, str | None]:
    base: dict[str, str | None] = {
        "source_ref": "tx-1", "business_date": "2026-03-02", "counterparty": "  Petroway   Fuels ",
        "description": "Diesel", "amount": "1,234.50", "currency": "usd", "direction": "D",
    }  # fmt: skip
    base.update(overrides)
    return base


def record(counterparty: str, description: str, direction: Direction = Direction.DEBIT) -> NormalisedRecord:
    return NormalisedRecord("TX-1", date(2026, 3, 2), counterparty, description, 1000, "USD", direction)


class MoneyTests(unittest.TestCase):
    def test_valid_amounts(self) -> None:
        cases = {"1": 100, "1.5": 150, "1,234.56": 123456, "$12.00": 1200, " 0.01 ": 1, "1234567.89": 123456789}
        for text, cents in cases.items():
            with self.subTest(text=text):
                self.assertEqual(parse_amount(text), cents)

    def test_malformed_amounts_are_rejected_not_guessed(self) -> None:
        for text in ["12,3a.00", "12,34.00", "1.234", "", "abc", "1,2345.00", "1.2.3"]:
            with self.subTest(text=text), self.assertRaises(AmountError) as ctx:
                parse_amount(text)
            self.assertEqual(ctx.exception.code, "MALFORMED_AMOUNT")

    def test_non_positive_amounts(self) -> None:
        for text in ["-45.00", "(12.00)", "0.00", "0", "$-3"]:
            with self.subTest(text=text), self.assertRaises(AmountError) as ctx:
                parse_amount(text)
            self.assertEqual(ctx.exception.code, "NON_POSITIVE_AMOUNT")

    def test_control_amount_allows_zero(self) -> None:
        self.assertEqual(parse_control_amount("0.00"), 0)
        self.assertEqual(parse_control_amount("10.00"), 1000)

    def test_format(self) -> None:
        self.assertEqual(format_minor(123456), "1,234.56")
        self.assertEqual(format_minor(5), "0.05")
        self.assertEqual(format_minor(None), "-")


class ValidationTests(unittest.TestCase):
    def test_valid_record_is_normalised(self) -> None:
        result = validate_canonical(canonical(), received_on=RECEIVED_ON)
        self.assertTrue(result.ok)
        rec = result.record
        assert rec is not None
        self.assertEqual(rec.source_ref, "TX-1")
        self.assertEqual(rec.counterparty, "Petroway Fuels")
        self.assertEqual(rec.amount_minor, 123450)
        self.assertEqual(rec.currency, "USD")
        self.assertIs(rec.direction, Direction.DEBIT)

    def test_all_issues_are_reported_at_once(self) -> None:
        result = validate_canonical(
            canonical(counterparty="", business_date="02/03/2026", amount="x", currency="EUR"),
            received_on=RECEIVED_ON,
        )
        codes = {i.code for i in result.issues}
        self.assertFalse(result.ok)
        self.assertEqual(codes, {"REQUIRED_FIELD_MISSING", "MALFORMED_DATE", "MALFORMED_AMOUNT", "UNSUPPORTED_CURRENCY"})

    def test_failed_record_keeps_parseable_amount_for_reconciliation(self) -> None:
        result = validate_canonical(canonical(counterparty=None), received_on=RECEIVED_ON)
        self.assertFalse(result.ok)
        self.assertEqual(result.parsed["amount_minor"], 123450)
        self.assertIs(result.parsed["direction"], Direction.DEBIT)

    def test_impossible_and_future_dates(self) -> None:
        self.assertEqual(
            [i.code for i in validate_canonical(canonical(business_date="2026-02-30"), received_on=RECEIVED_ON).issues],
            ["MALFORMED_DATE"],
        )
        self.assertEqual(
            [i.code for i in validate_canonical(canonical(business_date="2026-03-11"), received_on=RECEIVED_ON).issues],
            ["DATE_IN_FUTURE"],
        )

    def test_direction_and_currency_formats(self) -> None:
        self.assertEqual(
            [i.code for i in validate_canonical(canonical(direction="X"), received_on=RECEIVED_ON).issues],
            ["MALFORMED_DIRECTION"],
        )
        self.assertEqual(
            [i.code for i in validate_canonical(canonical(currency="US$"), received_on=RECEIVED_ON).issues],
            ["MALFORMED_CURRENCY"],
        )

    def test_row_error_is_reported(self) -> None:
        result = validate_canonical({"_row_error": "expected 7 columns, found 3"}, received_on=RECEIVED_ON)
        self.assertIn("MALFORMED_ROW", {i.code for i in result.issues})

    def test_clean_text(self) -> None:
        self.assertEqual(clean_text("  a\t\u00a0 b  "), "a b")
        self.assertIsNone(clean_text("   "))


class RuleTests(unittest.TestCase):
    def test_match(self) -> None:
        result = classify_by_rules(record("Petroway", "Diesel fill"))
        self.assertEqual((result.outcome, result.category, result.rule_id), (RuleOutcome.MATCHED, "FUEL", "R-FUEL-001"))

    def test_conflict_is_never_resolved_by_rule_order(self) -> None:
        result = classify_by_rules(record("Quickstop Fuel & Tyre", "Fuel and tyre rotation"))
        self.assertEqual(result.outcome, RuleOutcome.CONFLICT)
        self.assertIsNone(result.category)
        self.assertEqual(set(result.matched_rule_ids), {"R-FUEL-001", "R-MAINT-001"})

    def test_no_match(self) -> None:
        self.assertEqual(classify_by_rules(record("Hartwell Catering", "Team lunch")).outcome, RuleOutcome.NO_MATCH)

    def test_direction_matters(self) -> None:
        # Money *in* from a fuel station is a refund, not a fuel expense: do not guess.
        result = classify_by_rules(record("Petroway", "Diesel refund", Direction.CREDIT))
        self.assertEqual(result.outcome, RuleOutcome.NO_MATCH)


class StateMachineTests(unittest.TestCase):
    def test_legal_and_illegal_batch_transitions(self) -> None:
        ensure_batch_transition(BatchStatus.RECEIVED, BatchStatus.VALIDATING)
        ensure_batch_transition(BatchStatus.FAILED, BatchStatus.RETRY_PENDING)
        for current, target in [
            (BatchStatus.PROCESSED, BatchStatus.PROCESSING),
            (BatchStatus.VALIDATION_FAILED, BatchStatus.RETRY_PENDING),
            (BatchStatus.RECEIVED, BatchStatus.PROCESSED),
            (BatchStatus.FAILED, BatchStatus.PROCESSED),
        ]:
            with self.subTest(current=current, target=target), self.assertRaises(InvalidTransition):
                ensure_batch_transition(current, target)

    def test_terminal_records_cannot_move(self) -> None:
        for terminal in (RecordStatus.PROCESSED, RecordStatus.DUPLICATE, RecordStatus.REJECTED,
                         RecordStatus.VALIDATION_FAILED):  # fmt: skip
            for target in RecordStatus:
                with self.subTest(terminal=terminal, target=target), self.assertRaises(InvalidTransition):
                    ensure_record_transition(terminal, target)


def facts(**overrides: object) -> ReconciliationFacts:
    base: dict[str, object] = {
        "received": 3, "status_counts": {"PROCESSED": 2, "DUPLICATE": 1},
        "control": ControlTotals(3, 3000, 0), "source_debit_minor": 3000, "source_credit_minor": 0,
        "unknown_amount_count": 0, "processed_debit_minor": 2000, "processed_credit_minor": 0,
    }  # fmt: skip
    base.update(overrides)
    return ReconciliationFacts(**base)  # type: ignore[arg-type]


class ReconcileTests(unittest.TestCase):
    def check(self, report, name):  # noqa: ANN001, ANN201
        return next(c for c in report.checks if c.name == name)

    def test_reconciled_with_duplicates_excluded_from_processed(self) -> None:
        report = reconcile(facts())
        self.assertEqual(report.status, ReconciliationStatus.RECONCILED)
        self.assertEqual(report.facts.processed_debit_minor, 2000)  # duplicate not double counted

    def test_count_mismatch(self) -> None:
        report = reconcile(facts(control=ControlTotals(4, 3000, 0)))
        self.assertEqual(report.status, ReconciliationStatus.MISMATCH)
        self.assertEqual(self.check(report, "completeness").result, CheckResult.FAIL)

    def test_unknown_amount_gives_unknown_not_success(self) -> None:
        report = reconcile(facts(status_counts={"PROCESSED": 2, "VALIDATION_FAILED": 1},
                                 source_debit_minor=2000, unknown_amount_count=1))  # fmt: skip
        self.assertEqual(report.status, ReconciliationStatus.UNKNOWN)

    def test_known_sum_already_over_control_is_a_definite_mismatch(self) -> None:
        report = reconcile(facts(source_debit_minor=3500, unknown_amount_count=1,
                                 status_counts={"PROCESSED": 2, "VALIDATION_FAILED": 1}))  # fmt: skip
        self.assertEqual(report.status, ReconciliationStatus.MISMATCH)

    def test_pending_beats_unknown(self) -> None:
        report = reconcile(facts(status_counts={"PROCESSED": 1, "RECEIVED": 2}, unknown_amount_count=2,
                                 source_debit_minor=1000))  # fmt: skip
        self.assertEqual(report.status, ReconciliationStatus.PENDING)

    def test_mismatch_beats_pending(self) -> None:
        report = reconcile(facts(status_counts={"PROCESSED": 1, "NEEDS_REVIEW": 2}, control=ControlTotals(5, 3000, 0)))
        self.assertEqual(report.status, ReconciliationStatus.MISMATCH)

    def test_no_control_totals_is_not_applicable(self) -> None:
        report = reconcile(facts(control=None))
        self.assertEqual(report.status, ReconciliationStatus.RECONCILED)
        self.assertEqual(self.check(report, "completeness").result, CheckResult.NOT_APPLICABLE)

    def test_identity_detects_unknown_status_values(self) -> None:
        report = reconcile(facts(status_counts={"PROCESSED": 2, "SOMETHING_NEW": 1}))
        self.assertEqual(self.check(report, "identity").result, CheckResult.FAIL)

    def test_structure_invalid_is_unknown(self) -> None:
        report = reconcile(facts(received=0, status_counts={}, structure_invalid=True))
        self.assertEqual(report.status, ReconciliationStatus.UNKNOWN)


if __name__ == "__main__":
    unittest.main()
