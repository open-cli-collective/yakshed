#!/usr/bin/env python3
"""Minimal child process used by the YakShed backend contract test.

It never prints the credential itself. It reports only whether the requested
variable was present, its SHA-256 digest, and whether forbidden variables were
inherited. The contract host consumes this output and must not forward the
digest to production-facing surfaces.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import sys
from typing import Sequence


def parse_args(argv: Sequence[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--credential-var", required=True)
    parser.add_argument("--forbid", action="append", default=[])
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(sys.argv[1:] if argv is None else argv)
    value = os.environ.get(args.credential_var)
    forbidden_present = sorted(name for name in args.forbid if name in os.environ)

    result = {
        "protocol_version": 1,
        "credential_variable": args.credential_var,
        "present": value is not None,
        "sha256": hashlib.sha256(value.encode("utf-8")).hexdigest() if value is not None else None,
        "forbidden_present": forbidden_present,
    }
    sys.stdout.write(json.dumps(result, separators=(",", ":")) + "\n")
    sys.stdout.flush()

    if value is None:
        return 3
    if forbidden_present:
        return 4
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
