"""Idempotent ingestion: accept bytes fast, validate later in the worker.

Accepting is deliberately dumb (hash + store). Parsing happens in the worker so
that a malformed file never blocks the sender and is always kept for replay.
"""

from __future__ import annotations

from dataclasses import dataclass

from opsflow.clock import Clock
from opsflow.domain.ids import batch_id as make_batch_id
from opsflow.domain.ids import sha256_hex
from opsflow.domain.models import SourceType
from opsflow.domain.states import BatchStatus
from opsflow.obs import log
from opsflow.storage.sqlite import SqliteRepository


@dataclass(frozen=True, slots=True)
class IngestResult:
    batch_id: str
    created: bool  # False = this exact content was already received (no new work)


def ingest_bytes(
    repo: SqliteRepository,
    clock: Clock,
    *,
    source_type: str,
    source_name: str,
    content: bytes,
    actor: str,
    max_attempts: int = 3,
) -> IngestResult:
    source_type = str(SourceType(source_type))  # rejects unknown source types
    digest = sha256_hex(content)
    new_id = make_batch_id(source_type, digest)
    now = clock.now()
    with repo.transaction() as tx:
        created = tx.insert_batch_if_absent(
            batch_id=new_id,
            source_type=source_type,
            source_name=source_name,
            content_sha256=digest,
            content=content,
            max_attempts=max_attempts,
            now=now,
        )
        if created:
            tx.audit(
                now=now, actor=actor, entity_type="batch", entity_id=new_id, batch_id=new_id,
                action="BATCH_RECEIVED", to_state=BatchStatus.RECEIVED,
                details={"source_name": source_name, "bytes": len(content), "sha256": digest},
            )  # fmt: skip
            batch_id = new_id
        else:
            existing = tx.find_batch_by_hash(source_type, digest)
            assert existing is not None
            batch_id = existing.id
            tx.audit(
                now=now, actor=actor, entity_type="batch", entity_id=batch_id, batch_id=batch_id,
                action="DUPLICATE_SUBMISSION",
                details={"source_name": source_name, "existing_status": existing.status},
            )  # fmt: skip
    log("ingest", batch_id=batch_id, created=created, source_name=source_name)
    return IngestResult(batch_id, created)
