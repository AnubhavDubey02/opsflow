"""Shared test fixtures (unittest-style so they run with or without pytest)."""

from __future__ import annotations

import shutil
import tempfile
import unittest
from datetime import UTC, datetime
from pathlib import Path

from opsflow.clock import FixedClock
from opsflow.pipeline import PipelineConfig, Worker, ingest_bytes
from opsflow.pipeline.config import Hooks
from opsflow.storage.sqlite import SqliteRepository
from opsflow.synth.bank_csv import GeneratedFile, generate_bank_files

T0 = datetime(2026, 3, 10, 8, 0, tzinfo=UTC)
ACTOR = "user:test-operator"


class SimulatedCrash(BaseException):
    """Like a killed process: not an Exception, so the worker cannot catch it."""


class PipelineTestCase(unittest.TestCase):
    def setUp(self) -> None:
        self._dir = Path(tempfile.mkdtemp(prefix="opsflow-test-"))
        self._repos: list[SqliteRepository] = []
        self.repo = self.new_repo("main")
        self.clock = FixedClock(T0)

    def tearDown(self) -> None:
        for repo in self._repos:
            repo.close()
        shutil.rmtree(self._dir, ignore_errors=True)

    def new_repo(self, name: str) -> SqliteRepository:
        repo = SqliteRepository(self._dir / f"{name}.db")
        repo.migrate()
        self._repos.append(repo)
        return repo

    def worker(self, repo: SqliteRepository | None = None, *, hooks: Hooks | None = None,
               worker_id: str = "worker-1", **config: object) -> Worker:  # fmt: skip
        cfg = PipelineConfig(worker_id=worker_id, jitter_seed=1, **config)  # type: ignore[arg-type]
        return Worker(repo or self.repo, self.clock, cfg, hooks=hooks)

    def ingest(self, f: GeneratedFile, repo: SqliteRepository | None = None, *, max_attempts: int = 3) -> str:
        result = ingest_bytes(
            repo or self.repo, self.clock, source_type="BANK_CSV", source_name=f.name,
            content=f.content, actor=ACTOR, max_attempts=max_attempts,
        )  # fmt: skip
        self.clock.advance(seconds=1)
        return result.batch_id

    def files(self) -> list[GeneratedFile]:
        return generate_bank_files(seed=7)

    def batch(self, batch_id: str, repo: SqliteRepository | None = None):  # noqa: ANN201
        with (repo or self.repo).transaction() as tx:
            return tx.get_batch(batch_id)

    def records(self, batch_id: str, repo: SqliteRepository | None = None):  # noqa: ANN201
        with (repo or self.repo).transaction() as tx:
            return tx.list_records(batch_id)
