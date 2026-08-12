#!/usr/bin/env python3
"""Local-only read-path check for a user-provided 1Password reference."""

import argparse
import os
import signal
import subprocess


parser = argparse.ArgumentParser()
parser.add_argument("--reference", required=True)
parser.add_argument("--executable", default="op")
parser.add_argument("--account", required=True)
args = parser.parse_args()

parts = args.reference.removeprefix("op://").split("/")
if not args.reference.startswith("op://") or len(parts) != 3 or not all(parts):
    parser.error("reference must be op://vault/item/field")

command = [args.executable, "read", "--no-newline", "--force", args.reference]
command.extend(["--account", args.account])
allowed = {
    key: value
    for key, value in os.environ.items()
    if key
    in {
        "HOME",
        "PATH",
        "XDG_CONFIG_HOME",
        "OP_CONFIG_DIR",
        "OP_SERVICE_ACCOUNT_TOKEN",
        "OP_CONNECT_HOST",
        "OP_CONNECT_TOKEN",
    }
}
allowed["NO_COLOR"] = "1"
process = subprocess.Popen(
    command,
    stdin=subprocess.DEVNULL,
    stdout=subprocess.PIPE,
    stderr=subprocess.PIPE,
    env=allowed,
    start_new_session=True,
)
stdout = b""
stderr = b""
timed_out = False
try:
    stdout, stderr = process.communicate(timeout=10)
except subprocess.TimeoutExpired:
    timed_out = True
finally:
    try:
        os.killpg(process.pid, signal.SIGKILL)
    except ProcessLookupError:
        pass
    process.wait()

secret = bytearray(stdout)
diagnostic = bytearray(stderr)
try:
    if timed_out:
        raise SystemExit("FAIL: op read timed out")
    if process.returncode != 0:
        raise SystemExit("FAIL: op read was unavailable or authentication is required")
    if not secret or len(secret) > 65536:
        raise SystemExit("FAIL: op read returned an invalid payload")
    print("PASS: op read resolved the reference without printing its value")
finally:
    secret[:] = b"\0" * len(secret)
    diagnostic[:] = b"\0" * len(diagnostic)
