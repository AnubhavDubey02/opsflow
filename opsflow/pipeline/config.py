from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass

from opsflow.storage.sqlite import RecordRow


@dataclass(frozen=True, slots=True)
class PipelineConfig:
    worker_id: str = "worker-1"
    lease_seconds: int = 300
    backoff_base_seconds: float = 30.0
    backoff_cap_seconds: float = 900.0
    jitter_seed: int | None = None  # set in tests for deterministic backoff


@dataclass(frozen=True, slots=True)
class Hooks:
    """Fault-injection points used by tests to simulate crashes and outages.

    ``before_record(record, stage)`` runs before each record step; ``stage`` is
    ``"validate"`` or ``"classify"``. Raising from it behaves exactly like the
    real step failing at that moment.
    """

    before_record: Callable[[RecordRow, str], None] | None = None
