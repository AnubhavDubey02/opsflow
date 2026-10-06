"""Command-line interface: ``python -m opsflow <command>``.

The operator identity defaults to ``user:operator`` (never the OS login name),
so demo databases and screenshots carry no personal identifiers.
"""

from __future__ import annotations

import argparse
import json
import os
import shutil
import sys
import tempfile
from collections.abc import Sequence
from pathlib import Path
from typing import Any

from opsflow.clock import SystemClock, to_ts
from opsflow.domain.errors import OpsFlowError
from opsflow.domain.money import format_minor
from opsflow.obs import configure_logging
from opsflow.pipeline import Hooks, PipelineConfig, Worker, ingest_bytes, rerun_batch, resolve_review
from opsflow.storage.sqlite import BatchRow, RecordRow, SqliteRepository, SqliteTx
from opsflow.synth.bank_csv import write_bank_files

DEFAULT_ACTOR = "user:operator"


def _table(rows: Sequence[Sequence[Any]], headers: Sequence[str]) -> str:
    cells = [[("" if v is None else str(v)) for v in row] for row in rows]
    widths = [max(len(h), *(len(r[i]) for r in cells)) if cells else len(h) for i, h in enumerate(headers)]
    line = "  ".join(h.ljust(w) for h, w in zip(headers, widths, strict=True))
    out = [line, "  ".join("-" * w for w in widths)]
    out += ["  ".join(c.ljust(w) for c, w in zip(r, widths, strict=True)) for r in cells]
    return "\n".join(out)


def _open(args: argparse.Namespace) -> SqliteRepository:
    repo = SqliteRepository(args.db)
    repo.migrate()
    args._repos.append(repo)  # closed by main(); open handles would lock the file on Windows
    return repo


# --------------------------------------------------------------------- commands
def cmd_generate(args: argparse.Namespace) -> int:
    paths = write_bank_files(Path(args.out), seed=args.seed)
    print(f"wrote {len(paths)} synthetic files + manifest.json to {args.out}")
    for p in paths:
        print(f"  {p.name}")
    return 0


def cmd_ingest(args: argparse.Namespace) -> int:
    repo = _open(args)
    clock = SystemClock()
    for name in args.paths:
        path = Path(name)
        result = ingest_bytes(
            repo, clock, source_type=args.source_type, source_name=path.name,
            content=path.read_bytes(), actor=args.actor,
        )  # fmt: skip
        verdict = "accepted" if result.created else "already received (no new work)"
        print(f"{path.name}: {verdict}  batch={result.batch_id[:8]}")
    return 0


def _fault_hook(line_no: int | None) -> Hooks:
    if line_no is None:
        return Hooks()
    fired = {"done": False}

    def hook(rec: RecordRow, stage: str) -> None:
        if rec.line_no == line_no and stage == "validate" and not fired["done"]:
            fired["done"] = True
            raise RuntimeError(f"injected fault at line {line_no} (demo)")

    return Hooks(before_record=hook)


def cmd_process(args: argparse.Namespace) -> int:
    repo = _open(args)
    worker = Worker(repo, SystemClock(), PipelineConfig(worker_id=args.worker_id), hooks=_fault_hook(args.inject_fault_at_line))
    summaries = worker.run_until_idle(max_runs=args.max_runs)
    if not summaries:
        print("nothing to process")
    with repo.transaction() as tx:
        for s in summaries:
            b = tx.get_batch(s.batch_id)
            print(f"{b.source_name}: {b.status}  reconciliation={b.reconciliation_status}")
        for b in tx.list_batches():
            note = _waiting_note(tx, b)
            if note:
                print(f"{b.source_name}: {b.status}  {note}")
    return 0


def _waiting_note(tx: SqliteTx, batch: BatchRow) -> str:
    if batch.status not in ("RECEIVED", "RETRY_PENDING"):
        return ""
    blocker = tx.blocking_predecessor(batch)
    if blocker is not None:
        return f"WAITING: earlier delivery {blocker.id[:8]} ({blocker.source_name}) is {blocker.status}"
    if batch.status == "RETRY_PENDING" and batch.next_attempt_at is not None:
        return f"retry due {to_ts(batch.next_attempt_at)[:19]}"
    return ""


def cmd_report(args: argparse.Namespace) -> int:
    repo = _open(args)
    rows = []
    with repo.transaction() as tx:
        for b in tx.list_batches():
            r = tx.latest_reconciliation(b.id) or {}
            rows.append([
                b.id[:8], b.source_name, b.status, b.reconciliation_status, r.get("received", 0),
                r.get("processed", 0), r.get("validation_failed", 0), r.get("duplicate", 0),
                r.get("rejected", 0), r.get("pending", 0), format_minor(r.get("processed_debit_minor")),
                format_minor(r.get("processed_credit_minor")), _waiting_note(tx, b),
            ])  # fmt: skip
    print(_table(rows, ["batch", "source", "status", "reconciliation", "recv", "ok", "invalid",
                        "dup", "rej", "pend", "debits", "credits", "note"]))  # fmt: skip
    return 0


def cmd_show(args: argparse.Namespace) -> int:
    repo = _open(args)
    with repo.transaction() as tx:
        b = tx.find_batch_by_prefix(args.batch)
        print(f"batch {b.id}\n  source  {b.source_name} ({b.source_type})\n  status  {b.status}"
              f"\n  recon   {b.reconciliation_status}\n  budget  {b.retry_budget_used}/{b.max_attempts}")  # fmt: skip
        recon = tx.latest_reconciliation(b.id)
        if recon:
            def shown(check: dict[str, Any], key: str) -> Any:
                value = check[key]
                return format_minor(value) if "total" in check["name"] and isinstance(value, int) else value

            print("\nreconciliation checks")
            print(_table([[c["name"], c["result"], shown(c, "expected"), shown(c, "actual"), c["note"]]
                          for c in recon["checks"]], ["check", "result", "expected", "actual", "note"]))  # fmt: skip
        issues = tx.list_issues(b.id)
        if issues:
            print("\nvalidation issues")
            print(_table([[i["line_no"] or "file", i["code"], i["field"], i["message"]] for i in issues],
                         ["line", "code", "field", "message"]))  # fmt: skip
        records = tx.list_records(b.id)
        if records:
            print("\nrecords")
            print(_table(
                [[r.line_no, r.status, r.source_ref, r.counterparty, format_minor(r.amount_minor),
                  r.direction, r.category_code, r.category_source] for r in records],
                ["line", "status", "ref", "counterparty", "amount", "dir", "category", "by"],
            ))  # fmt: skip
        runs = tx.list_runs(b.id)
        if runs:
            print("\nprocessing runs")
            print(_table([[r["attempt_no"], r["worker_id"], r["outcome"], r["error_code"], r["error_detail"]]
                          for r in runs], ["attempt", "worker", "outcome", "error", "detail"]))  # fmt: skip
    return 0


def cmd_reviews(args: argparse.Namespace) -> int:
    repo = _open(args)
    with repo.transaction() as tx:
        tasks = tx.list_reviews(status=None if args.all else "OPEN")
    print(_table(
        [[t["id"][:8], t["source_name"], t["line_no"], t["reason"], t["counterparty"], t["description"],
          format_minor(t["amount_minor"]), t["direction"], t["suggested_category"], t["status"], t["decision"]]
         for t in tasks],
        ["review", "source", "line", "reason", "counterparty", "description", "amount", "dir",
         "suggested", "status", "decision"],
    ))  # fmt: skip
    return 0


def cmd_review(args: argparse.Namespace) -> int:
    repo = _open(args)
    with repo.transaction() as tx:
        task = tx.find_review_by_prefix(args.review)
    resolve_review(
        repo, SystemClock(), task["id"], decision=args.decision.upper(), actor=args.actor,
        category=args.category, note=args.note,
    )  # fmt: skip
    print(f"review {task['id'][:8]}: {args.decision.upper()} recorded")
    return 0


def cmd_rerun(args: argparse.Namespace) -> int:
    repo = _open(args)
    with repo.transaction() as tx:
        batch = tx.find_batch_by_prefix(args.batch)
    rerun_batch(repo, SystemClock(), batch.id, actor=args.actor, reason=args.reason)
    print(f"batch {batch.id[:8]} queued for rerun (run `process` to execute)")
    return 0


def cmd_audit(args: argparse.Namespace) -> int:
    repo = _open(args)
    with repo.transaction() as tx:
        batch_id = tx.find_batch_by_prefix(args.batch).id if args.batch else None
        events = tx.list_audit(batch_id)
    print(_table(
        [[e["id"], e["occurred_at"][:19], e["actor"], e["entity_type"], e["entity_id"][:8], e["action"],
          e["from_state"], e["to_state"], json.dumps(e["details"])[:70]] for e in events],
        ["#", "at", "actor", "entity", "id", "action", "from", "to", "details"],
    ))  # fmt: skip
    return 0


def cmd_demo(args: argparse.Namespace) -> int:
    workdir = Path(tempfile.mkdtemp(prefix="opsflow-demo-"))
    db = workdir / "demo.db"
    inbox = workdir / "inbox"
    base = ["--db", str(db)]

    def step(title: str, argv: list[str]) -> None:
        print(f"\n=== {title}\n$ opsflow {' '.join(argv)}")
        main([*base, *argv])

    try:
        step("1. Generate synthetic input (fictional company, seeded)", ["generate", "--out", str(inbox)])
        files = sorted(p for p in inbox.glob("*.csv"))
        step("2. Ingest every file", ["ingest", *map(str, files)])
        step("3. Submit the first file again (idempotency)", ["ingest", str(files[0])])
        step("4. Process, with a fault injected at line 5 of the first batch", ["process", "--inject-fault-at-line", "5"])
        step("5. Operations report: later files wait behind the failed one", ["report"])
        repo = SqliteRepository(db)
        with repo.transaction() as tx:
            failed = next(b for b in tx.list_batches() if b.status == "FAILED")
        step("6. Inspect the failed batch", ["show", failed.id[:8]])
        step("7. Rerun it safely", ["rerun", failed.id[:8], "--reason", "demo: transient fault cleared"])
        step("8. Process again (the rerun resumes; waiting files follow in order)", ["process"])
        step("9. Review queue", ["reviews"])
        with repo.transaction() as tx:
            messy = next(b for b in tx.list_batches() if b.status == "NEEDS_REVIEW")
            tasks = tx.list_reviews(batch_id=messy.id)
        for t in tasks:
            if t["reason"] == "RULE_CONFLICT":
                step("10a. Correct a rule conflict", ["review", t["id"][:8], "correct", "--category", "FUEL",
                                                      "--note", "mostly fuel per receipt"])  # fmt: skip
            elif t["reason"] == "NO_RULE_AI_DISABLED":
                step("10b. Reject a non-business expense", ["review", t["id"][:8], "reject",
                                                            "--note", "personal expense, not company spend"])  # fmt: skip
            else:
                step("10c. Reject the conflicting duplicate", ["review", t["id"][:8], "reject",
                                                               "--note", "sender re-sent with wrong amount"])  # fmt: skip
        repo.close()
        step("11. Final report", ["report"])
        step("12. Reconciliation detail of the messy batch", ["show", messy.id[:8]])
        step("13. Audit trail of the rerun batch", ["audit", failed.id[:8]])
        print(f"\nDemo database kept at: {db}" if args.keep else "")
    finally:
        if not args.keep:
            shutil.rmtree(workdir, ignore_errors=True)
    return 0


# ------------------------------------------------------------------------ parser
def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(prog="opsflow", description="Reliable data operations pipeline (synthetic demo).")
    p.add_argument("--db", default=os.environ.get("OPSFLOW_DB", "opsflow.db"), help="SQLite database path")
    p.add_argument("--log-level", default=os.environ.get("OPSFLOW_LOG_LEVEL", "WARNING"))
    sub = p.add_subparsers(dest="command", required=True)

    g = sub.add_parser("generate", help="write synthetic input files + manifest")
    g.add_argument("--out", default="data/inbox")
    g.add_argument("--seed", type=int, default=7)
    g.set_defaults(func=cmd_generate)

    i = sub.add_parser("ingest", help="submit files (idempotent)")
    i.add_argument("paths", nargs="+")
    i.add_argument("--source-type", default="BANK_CSV")
    i.add_argument("--actor", default=DEFAULT_ACTOR)
    i.set_defaults(func=cmd_ingest)

    pr = sub.add_parser("process", help="run the worker until no batch is due")
    pr.add_argument("--worker-id", default="worker-1")
    pr.add_argument("--max-runs", type=int, default=100)
    pr.add_argument("--inject-fault-at-line", type=int, default=None, help="demo: fail once at this line")
    pr.set_defaults(func=cmd_process)

    sub.add_parser("report", help="batch status + reconciliation overview").set_defaults(func=cmd_report)

    s = sub.add_parser("show", help="batch detail: checks, issues, records, runs")
    s.add_argument("batch", help="batch id or unique prefix")
    s.set_defaults(func=cmd_show)

    rv = sub.add_parser("reviews", help="list the review queue")
    rv.add_argument("--all", action="store_true", help="include resolved reviews")
    rv.set_defaults(func=cmd_reviews)

    r = sub.add_parser("review", help="resolve a review")
    r.add_argument("review", help="review id or unique prefix")
    r.add_argument("decision", choices=["approve", "correct", "reject"])
    r.add_argument("--category")
    r.add_argument("--note")
    r.add_argument("--actor", default=DEFAULT_ACTOR)
    r.set_defaults(func=cmd_review)

    rr = sub.add_parser("rerun", help="queue a FAILED batch for another attempt")
    rr.add_argument("batch")
    rr.add_argument("--reason", required=True)
    rr.add_argument("--actor", default=DEFAULT_ACTOR)
    rr.set_defaults(func=cmd_rerun)

    a = sub.add_parser("audit", help="audit trail (all, or one batch)")
    a.add_argument("batch", nargs="?")
    a.set_defaults(func=cmd_audit)

    d = sub.add_parser("demo", help="run the full story in a temporary database")
    d.add_argument("--keep", action="store_true", help="keep the demo database afterwards")
    d.set_defaults(func=cmd_demo)
    return p


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    args._repos = []
    configure_logging(args.log_level)
    try:
        return int(args.func(args))
    except (OpsFlowError, ValueError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2
    finally:
        for repo in args._repos:
            repo.close()
