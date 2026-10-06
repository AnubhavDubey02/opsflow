"""Structured (JSON-lines) logging with correlation fields."""

from __future__ import annotations

import json
import logging
import sys
from typing import Any

_LOGGER_NAME = "opsflow"


class JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        payload: dict[str, Any] = {
            "ts": self.formatTime(record, "%Y-%m-%dT%H:%M:%S"),
            "level": record.levelname,
            "event": record.getMessage(),
        }
        payload.update(getattr(record, "fields", {}))
        if record.exc_info:
            payload["exc"] = self.formatException(record.exc_info)
        return json.dumps(payload, default=str)


def configure_logging(level: str = "INFO") -> None:
    logger = logging.getLogger(_LOGGER_NAME)
    if not logger.handlers:
        handler = logging.StreamHandler(sys.stderr)
        handler.setFormatter(JsonFormatter())
        logger.addHandler(handler)
    logger.setLevel(level)


def log(event: str, level: int = logging.INFO, **fields: Any) -> None:
    logging.getLogger(_LOGGER_NAME).log(level, event, extra={"fields": fields})
