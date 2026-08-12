#!/usr/bin/env python3
"""Hermetic protocol fake for the YakShed 1Password backend tests."""

import json
import os
from pathlib import Path
import subprocess
import sys
import time


root = Path(__file__).resolve().parent
(root / "fake_op.invocation.json").write_text(
    json.dumps({"argv": sys.argv[1:], "environment": sorted(os.environ)}),
    encoding="utf-8",
)

if "--account" in sys.argv and sys.argv[sys.argv.index("--account") + 1] == "locked":
    print("[ERROR] not signed in", file=sys.stderr)
    raise SystemExit(1)
if "--account" in sys.argv and sys.argv[sys.argv.index("--account") + 1] == "signed-out-work":
    print("[ERROR] account signed-out-work is not currently signed in", file=sys.stderr)
    raise SystemExit(1)

if sys.argv[1:3] == ["account", "get"]:
    print("{}", end="")
    raise SystemExit(0)

reference = next((arg for arg in sys.argv[1:] if arg.startswith("op://")), "")
field = reference.rsplit("/", 1)[-1]
if field == "missing":
    print("[ERROR] secret not found", file=sys.stderr)
    raise SystemExit(4)
if field == "malformed":
    os.write(sys.stdout.fileno(), b"\xff")
    raise SystemExit(0)
if field == "failure":
    print("[ERROR] onepassword-secret-canary-731", file=sys.stderr)
    raise SystemExit(1)
if field == "forbidden":
    print(f"[ERROR] permission denied for {reference} in account work", file=sys.stderr)
    raise SystemExit(1)
if field == "hang":
    descendant = subprocess.Popen(["/bin/sleep", "60"])
    (root / "fake_op.pids").write_text(
        f"{os.getpid()} {descendant.pid}", encoding="utf-8"
    )
    time.sleep(60)

print("onepassword-secret-canary-731", end="")
