from __future__ import annotations

import unittest

from opsflow.domain.errors import StructureError
from opsflow.sources.bank_csv import BankCsvAdapter

HEADER = "#KFX-BANK-EXPORT v1,business_date=2026-03-02\ntxn_id,posted_on,payee,memo,amount,currency,dr_cr\n"
ROW = "TX-1,2026-03-02,Petroway,Diesel,10.00,USD,D\n"
TRAILER = "#TRAILER,count=1,debits=10.00,credits=0.00\n"


class BankCsvTests(unittest.TestCase):
    def setUp(self) -> None:
        self.adapter = BankCsvAdapter()

    def parse(self, text: str):  # noqa: ANN201
        return self.adapter.parse(text.encode("utf-8"))

    def test_valid_file(self) -> None:
        parsed = self.parse(HEADER + ROW + TRAILER)
        self.assertEqual(len(parsed.records), 1)
        self.assertEqual(parsed.records[0].line_no, 3)
        self.assertEqual(parsed.control.record_count, 1)
        self.assertEqual(parsed.control.debit_minor, 1000)
        self.assertEqual(parsed.control.credit_minor, 0)

    def test_structural_errors(self) -> None:
        cases = {
            "BAD_HEADER": "#SOMETHING ELSE\n" + HEADER.split("\n", 1)[1] + ROW + TRAILER,
            "BAD_COLUMNS": HEADER.split("\n", 1)[0] + "\ntxn,date\n" + ROW + TRAILER,
            "MISSING_TRAILER": HEADER + ROW,
            "TRUNCATED_FILE": HEADER,
            "BAD_TRAILER": HEADER + ROW + "#TRAILER,count=1,debits=1.0.0,credits=0.00\n",
        }
        for code, text in cases.items():
            with self.subTest(code=code), self.assertRaises(StructureError) as ctx:
                self.parse(text)
            self.assertEqual(ctx.exception.code, code)

    def test_bad_encoding(self) -> None:
        with self.assertRaises(StructureError) as ctx:
            self.adapter.parse(HEADER.encode() + b"\xff\xfe\n" + TRAILER.encode())
        self.assertEqual(ctx.exception.code, "BAD_ENCODING")

    def test_bad_row_fails_only_that_row(self) -> None:
        parsed = self.parse(HEADER + "TX-1,2026-03-02,Petroway\n" + ROW + TRAILER)
        self.assertEqual(len(parsed.records), 2)
        self.assertIsNotNone(parsed.records[0].row_error)
        self.assertIsNone(parsed.records[1].row_error)

    def test_bom_crlf_and_blank_lines_are_tolerated(self) -> None:
        text = "\ufeff" + (HEADER + ROW + "\n" + TRAILER).replace("\n", "\r\n") + "\r\n\r\n"
        parsed = self.parse(text)
        self.assertEqual(len(parsed.records), 1)

    def test_quoted_commas(self) -> None:
        parsed = self.parse(HEADER + 'TX-1,2026-03-02,"Alder, Finch & Co",Remittance,"1,200.00",USD,C\n' + TRAILER)
        self.assertEqual(parsed.records[0].fields["payee"], "Alder, Finch & Co")
        self.assertEqual(parsed.records[0].fields["amount"], "1,200.00")


if __name__ == "__main__":
    unittest.main()
