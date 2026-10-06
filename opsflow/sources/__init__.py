"""Source adapter registry. Each adapter turns delivered bytes into raw records."""

from __future__ import annotations

from typing import Protocol

from opsflow.domain.models import ParsedDelivery, RawRecord, SourceType
from opsflow.sources.bank_csv import BankCsvAdapter


class SourceAdapter(Protocol):
    source_type: SourceType

    def parse(self, content: bytes) -> ParsedDelivery: ...

    def to_canonical(self, raw: RawRecord) -> dict[str, str | None]: ...


def default_adapters() -> dict[str, SourceAdapter]:
    adapters: list[SourceAdapter] = [BankCsvAdapter()]
    return {str(a.source_type): a for a in adapters}
