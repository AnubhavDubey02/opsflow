"""Bank/card CSV export adapter (invented format, version 1).

```
#KFX-BANK-EXPORT v1,business_date=2026-03-02
txn_id,posted_on,payee,memo,amount,currency,dr_cr
TX-0001,2026-03-02,Petroway Fuels 14,Diesel fill,412.50,USD,D
#TRAILER,count=1,debits=412.50,credits=0.00
```

Structural problems (bad header, missing trailer, bad encoding) fail the whole
delivery. Problems inside one row fail only that row.
"""

from __future__ import annotations

import csv
import io
import re
from datetime import date

from opsflow.domain.errors import StructureError
from opsflow.domain.models import ControlTotals, ParsedDelivery, RawRecord, SourceType
from opsflow.domain.money import AmountError, parse_control_amount

COLUMNS = ("txn_id", "posted_on", "payee", "memo", "amount", "currency", "dr_cr")
_HEADER_RE = re.compile(r"^#KFX-BANK-EXPORT v1,business_date=(\d{4}-\d{2}-\d{2})$")
_TRAILER_RE = re.compile(r"^#TRAILER,count=(\d+),debits=([0-9.,]+),credits=([0-9.,]+)$")

# Header line, column line, first data row: data row N lives on file line N + 2.
FIRST_DATA_LINE = 3


class BankCsvAdapter:
    source_type = SourceType.BANK_CSV

    def parse(self, content: bytes) -> ParsedDelivery:
        try:
            text = content.decode("utf-8-sig")
        except UnicodeDecodeError as exc:
            raise StructureError("BAD_ENCODING", f"not valid UTF-8 at byte {exc.start}") from None

        lines = text.splitlines()
        while lines and not lines[-1].strip():
            lines.pop()
        if len(lines) < 3:
            raise StructureError("TRUNCATED_FILE", "expected header, column line and trailer")

        header = _HEADER_RE.match(lines[0].strip())
        if not header:
            raise StructureError("BAD_HEADER", "first line is not a KFX-BANK-EXPORT v1 header")
        try:
            business_date = date.fromisoformat(header.group(1))
        except ValueError:
            raise StructureError("BAD_HEADER", f"invalid business_date {header.group(1)!r}") from None

        if tuple(c.strip() for c in lines[1].split(",")) != COLUMNS:
            raise StructureError("BAD_COLUMNS", f"column line must be {','.join(COLUMNS)}")

        trailer = _TRAILER_RE.match(lines[-1].strip())
        if not trailer:
            raise StructureError("MISSING_TRAILER", "last line is not a #TRAILER control record")
        try:
            control = ControlTotals(
                record_count=int(trailer.group(1)),
                debit_minor=parse_control_amount(trailer.group(2)),
                credit_minor=parse_control_amount(trailer.group(3)),
            )
        except AmountError as exc:
            raise StructureError("BAD_TRAILER", f"unparseable control amount {exc.text!r}") from None

        records = []
        body = lines[2:-1]
        for offset, row in enumerate(csv.reader(io.StringIO("\n".join(body)))):
            line_no = FIRST_DATA_LINE + offset
            if not row or all(not cell.strip() for cell in row):
                continue  # blank line: not a record
            if len(row) != len(COLUMNS):
                records.append(
                    RawRecord(
                        line_no=line_no,
                        fields={f"col_{i}": cell for i, cell in enumerate(row)},
                        row_error=f"expected {len(COLUMNS)} columns, found {len(row)}",
                    )
                )
                continue
            records.append(RawRecord(line_no=line_no, fields=dict(zip(COLUMNS, row, strict=True))))

        return ParsedDelivery(SourceType.BANK_CSV, business_date, tuple(records), control)

    def to_canonical(self, raw: RawRecord) -> dict[str, str | None]:
        f = raw.fields
        return {
            "source_ref": f.get("txn_id"),
            "business_date": f.get("posted_on"),
            "counterparty": f.get("payee"),
            "description": f.get("memo"),
            "amount": f.get("amount"),
            "currency": f.get("currency"),
            "direction": f.get("dr_cr"),
            "_row_error": raw.row_error,
        }
