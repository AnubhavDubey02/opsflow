"""Injectable clock (ARCHITECTURE D10): tests control time, production uses UTC now."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import Protocol

_TS_FORMAT = "%Y-%m-%dT%H:%M:%S.%fZ"


class Clock(Protocol):
    def now(self) -> datetime: ...


class SystemClock:
    def now(self) -> datetime:
        return datetime.now(UTC)


class FixedClock:
    """A clock that only moves when told to."""

    def __init__(self, start: datetime) -> None:
        if start.tzinfo is None:
            raise ValueError("FixedClock needs a timezone-aware datetime")
        self._now = start.astimezone(UTC)

    def now(self) -> datetime:
        return self._now

    def advance(self, **kwargs: float) -> datetime:
        self._now += timedelta(**kwargs)
        return self._now


def to_ts(value: datetime) -> str:
    """Fixed-width UTC text: sorts correctly as a string (needed in SQLite)."""
    if value.tzinfo is None:
        raise ValueError("naive datetimes are not allowed")
    return value.astimezone(UTC).strftime(_TS_FORMAT)


def from_ts(value: str) -> datetime:
    return datetime.strptime(value, _TS_FORMAT).replace(tzinfo=UTC)
