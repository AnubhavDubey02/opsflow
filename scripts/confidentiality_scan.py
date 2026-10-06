"""Repository confidentiality scan. Run before any commit or push.

    python scripts/confidentiality_scan.py            # generic checks
    OPSFLOW_DENYLIST=/path/outside/repo/denylist.txt python scripts/confidentiality_scan.py

The denylist (one case-insensitive term per line) is kept OUTSIDE the repository,
so the public code never names the organisations or systems it must not mention.
Exit code 1 if anything is found.
"""

from __future__ import annotations

import os
import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SKIP_DIRS = {".git", "__pycache__", ".venv", "node_modules", "data", "htmlcov"}
BINARY_SUFFIXES = {".db", ".png", ".jpg", ".gif", ".ico", ".pdf", ".zip", ".pyc"}

GENERIC_PATTERNS: dict[str, re.Pattern[str]] = {
    "windows user path": re.compile(r"[A-Za-z]:\\Users\\[^\\\s]+", re.IGNORECASE),
    "unix home path": re.compile(r"/(?:home|Users)/[a-z0-9._-]+/", re.IGNORECASE),
    "email address": re.compile(r"\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}\b"),
    "internal-looking host": re.compile(r"\b[a-z0-9-]+\.(?:corp|internal|intranet|local|lan)\b", re.IGNORECASE),
    "ipv4 address": re.compile(r"\b(?:\d{1,3}\.){3}\d{1,3}\b"),
    "private key": re.compile(r"-----BEGIN [A-Z ]*PRIVATE KEY-----"),
    "ssh public key": re.compile(r"\bssh-(?:rsa|ed25519) AAAA"),
    "aws access key": re.compile(r"\bAKIA[0-9A-Z]{16}\b"),
    "secret assignment": re.compile(
        r"(?i)\b(?:api[_-]?key|secret|password|passwd|token)\b\s*[:=]\s*['\"][^'\"\s]{8,}['\"]"
    ),
    "bearer token": re.compile(r"(?i)bearer\s+[a-z0-9._-]{20,}"),
}
# Version-like dotted numbers that are not addresses.
_ALLOWED_IPS = {"0.0.0.0", "127.0.0.1"}


def iter_files() -> list[Path]:
    files = []
    for path in ROOT.rglob("*"):
        if path.is_dir() or any(part in SKIP_DIRS for part in path.relative_to(ROOT).parts):
            continue
        if path.suffix.lower() in BINARY_SUFFIXES:
            continue
        files.append(path)
    return sorted(files)


def load_denylist() -> list[str]:
    location = os.environ.get("OPSFLOW_DENYLIST")
    if not location:
        return []
    path = Path(location).resolve()
    if ROOT in path.parents:
        sys.exit("refusing to use a denylist stored inside the repository")
    return [t.strip().lower() for t in path.read_text(encoding="utf-8").splitlines() if t.strip()]


def scan() -> list[str]:
    findings = []
    terms = load_denylist()
    for path in iter_files():
        rel = path.relative_to(ROOT)
        try:
            text = path.read_text(encoding="utf-8")
        except UnicodeDecodeError:
            findings.append(f"{rel}: not UTF-8 text (review manually)")
            continue
        for lineno, line in enumerate(text.splitlines(), start=1):
            for name, pattern in GENERIC_PATTERNS.items():
                for match in pattern.finditer(line):
                    if name == "ipv4 address" and match.group(0) in _ALLOWED_IPS:
                        continue
                    findings.append(f"{rel}:{lineno}: {name}")
            lowered = line.lower()
            findings += [f"{rel}:{lineno}: denylisted term #{i + 1}" for i, t in enumerate(terms) if t in lowered]
    return findings


def main() -> int:
    findings = scan()
    files = len(iter_files())
    denylist = "with" if os.environ.get("OPSFLOW_DENYLIST") else "WITHOUT"
    if findings:
        print(f"confidentiality scan FAILED ({files} files, {denylist} denylist):")
        print("\n".join(f"  {f}" for f in findings))
        return 1
    print(f"confidentiality scan passed ({files} files, {denylist} denylist)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
