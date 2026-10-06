"""Exception hierarchy.

The class of an error decides how the pipeline reacts, so the hierarchy is part
of the failure model (see FAILURE_MODEL.md section 2).
"""

from __future__ import annotations


class OpsFlowError(Exception):
    """Base class for all expected OpsFlow errors."""


class InvalidTransition(OpsFlowError):
    def __init__(self, entity: str, current: str, target: str) -> None:
        super().__init__(f"illegal {entity} transition {current} -> {target}")
        self.entity = entity
        self.current = current
        self.target = target


class StructureError(OpsFlowError):
    """The delivery as a whole cannot be parsed (permanent; batch VALIDATION_FAILED)."""

    def __init__(self, code: str, message: str) -> None:
        super().__init__(f"{code}: {message}")
        self.code = code
        self.message = message


class TransientError(OpsFlowError):
    """Something outside our control failed and may succeed later (retry)."""

    def __init__(self, code: str, message: str) -> None:
        super().__init__(f"{code}: {message}")
        self.code = code
        self.message = message


class ConcurrencyError(OpsFlowError):
    """Optimistic-lock check failed: someone else changed the row first."""


class LeaseLost(OpsFlowError):
    """This worker no longer owns the batch lease; it must stop touching the batch."""


class NotFound(OpsFlowError):
    pass
