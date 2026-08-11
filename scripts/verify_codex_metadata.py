#!/usr/bin/env python3
"""Verify that last-validated Codex version mirrors are synchronized.

Exit codes: 0 in sync, 1 mismatch, 2 usage/IO/parse issue.
"""

from __future__ import annotations

import json
import re
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
LOCK_PATH = REPO_ROOT / "pins/codex-lock.json"
LIB_RS = REPO_ROOT / "crates/provider-codex/src/lib.rs"
FAKE_CODEX = REPO_ROOT / "crates/provider-codex/tests/fake_codex.py"

RELEASE_NOTE_RE = re.compile(r'LAST_VALIDATED_CODEX_VERSION\s*:\s*&str\s*=\s*"(?P<value>\d+\.\d+\.\d+)"')
CLI_VERSION_RE = re.compile(r'"cliVersion"\s*:\s*"(?P<value>\d+\.\d+\.\d+)"')


def read_versions() -> tuple[str, str, str]:
    lock = json.loads(LOCK_PATH.read_text(encoding="utf-8"))
    lock_version = lock["codex_version"]
    lib = LIB_RS.read_text(encoding="utf-8")
    fake = FAKE_CODEX.read_text(encoding="utf-8")

    lib_match = RELEASE_NOTE_RE.search(lib)
    if not lib_match:
        raise ValueError("unable to find LAST_VALIDATED_CODEX_VERSION in src/lib.rs")
    fake_match = CLI_VERSION_RE.search(fake)
    if not fake_match:
        raise ValueError("unable to find cliVersion in tests/fake_codex.py")

    return lock_version, lib_match["value"], fake_match["value"]


def main() -> int:
    try:
        lock_version, lib_version, fake_version = read_versions()
    except (OSError, ValueError, KeyError, json.JSONDecodeError) as error:
        print(f"ERROR: {error}", file=sys.stderr)
        return 2

    if len({lock_version, lib_version, fake_version}) != 1:
        print("VERSION MISMATCH: last-validated metadata is not synchronized", file=sys.stderr)
        print(f"  codex-lock.json:    {lock_version}")
        print(f"  src/lib.rs:         {lib_version}")
        print(f"  fake_codex.py:      {fake_version}")
        return 1

    print(f"ok: Codex metadata versions are aligned ({lock_version})")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
