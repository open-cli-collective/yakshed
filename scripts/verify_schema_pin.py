#!/usr/bin/env python3
"""Verify that pins/codex-app-server-schema/ matches pins/codex-lock.json.

Recomputes the aggregate digest over the committed schema tree and compares it
to `stable_schema_sha256` in the lock record. The digest is locale-independent:
files are ordered by their POSIX relative path using Python's default string
sort, and the aggregate is the SHA-256 of newline-joined `<sha256>  <relpath>`
lines (trailing newline included).

Exit codes: 0 match, 1 mismatch, 2 usage/IO error.
"""

from __future__ import annotations

import hashlib
import json
import sys
from pathlib import Path


def tree_digest(schema_root: Path) -> tuple[str, int]:
    files = sorted(
        (p for p in schema_root.rglob("*") if p.is_file()),
        key=lambda p: p.relative_to(schema_root).as_posix(),
    )
    lines = []
    for p in files:
        file_hash = hashlib.sha256(p.read_bytes()).hexdigest()
        lines.append(f"{file_hash}  {p.relative_to(schema_root).as_posix()}")
    aggregate = hashlib.sha256(("\n".join(lines) + "\n").encode("utf-8")).hexdigest()
    return aggregate, len(files)


def main(argv: list[str]) -> int:
    repo_root = Path(argv[1]) if len(argv) > 1 else Path(__file__).resolve().parent.parent
    lock_path = repo_root / "pins" / "codex-lock.json"
    schema_root = repo_root / "pins" / "codex-app-server-schema"

    if not lock_path.is_file() or not schema_root.is_dir():
        print(f"error: missing {lock_path} or {schema_root}", file=sys.stderr)
        return 2

    lock = json.loads(lock_path.read_text(encoding="utf-8"))
    expected = lock.get("stable_schema_sha256")
    if not expected:
        print("error: codex-lock.json has no stable_schema_sha256", file=sys.stderr)
        return 2

    actual, file_count = tree_digest(schema_root)
    if actual != expected:
        print(f"MISMATCH: lock={expected} actual={actual} files={file_count}", file=sys.stderr)
        print(
            "The committed schema tree does not match the lock record. Regenerate the "
            "schema from the pinned binary and update codex-lock.json in the same commit.",
            file=sys.stderr,
        )
        return 1

    print(f"ok: schema pin verified ({file_count} files, sha256 {actual})")
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
