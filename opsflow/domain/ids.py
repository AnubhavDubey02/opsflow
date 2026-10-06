"""Deterministic identifiers and fingerprints.

Batch and record IDs are UUIDv5 values derived from their content/position, so
replaying the same input always yields the same IDs (ARCHITECTURE.md, D2).
"""

from __future__ import annotations

import hashlib
import json
import uuid
from collections.abc import Mapping
from typing import Any

# Arbitrary, fixed namespace owned by this project. Never change it: doing so
# would change every ID derived from it.
OPSFLOW_NAMESPACE = uuid.UUID("6f1c9a52-3b7e-4d1a-9c55-0a8e2f4b7d31")


def sha256_hex(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def batch_id(source_type: str, content_sha256: str) -> str:
    return str(uuid.uuid5(OPSFLOW_NAMESPACE, f"batch:{source_type}:{content_sha256}"))


def record_id(batch_id_: str, line_no: int) -> str:
    return str(uuid.uuid5(OPSFLOW_NAMESPACE, f"record:{batch_id_}:{line_no}"))


def new_event_id() -> str:
    """Random id for events (runs, reviews, attempts): they are facts, not replayable data."""
    return str(uuid.uuid4())


def canonical_json(value: Mapping[str, Any]) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), default=str)


def fingerprint(value: Mapping[str, Any]) -> str:
    return sha256_hex(canonical_json(value).encode("utf-8"))
