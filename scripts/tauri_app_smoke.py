#!/usr/bin/env python3
"""Launch the packaged macOS app with isolated platform dirs, then verify clean quit."""

import argparse
import os
from pathlib import Path
import signal
import subprocess
import tempfile
import time


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--app",
        type=Path,
        default=Path("target/release/bundle/macos/YakShed.app"),
    )
    args = parser.parse_args()
    executable = args.app / "Contents/MacOS/yakshed-desktop"
    if not executable.is_file():
        parser.error(f"packaged executable not found: {executable}")

    with tempfile.TemporaryDirectory(prefix="yakshed-app-smoke-") as root:
        env = os.environ.copy()
        env["HOME"] = root
        env["PATH"] = "/usr/bin:/bin"
        process = subprocess.Popen(
            [executable],
            env=env,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            start_new_session=True,
        )
        pgid = os.getpgid(process.pid)
        try:
            deadline = time.monotonic() + 20
            database = None
            while time.monotonic() < deadline:
                if process.poll() is not None:
                    raise RuntimeError(f"app exited during startup: {process.returncode}")
                database = next(Path(root).rglob("yakshed.sqlite3"), None)
                if database:
                    break
                time.sleep(0.1)
            if database is None:
                raise RuntimeError("healthy startup database was not created")

            subprocess.run(
                ["osascript", "-e", 'tell application id "dev.yakshed.desktop" to quit'],
                check=True,
                capture_output=True,
                text=True,
            )
            process.wait(timeout=10)
            if process.returncode != 0:
                raise RuntimeError(f"app quit with status {process.returncode}")
            try:
                os.killpg(pgid, 0)
            except ProcessLookupError:
                pass
            else:
                raise RuntimeError(f"process group {pgid} survived app quit")
            print(f"PASS packaged app healthy; clean quit; process group {pgid} gone")
            return 0
        finally:
            if process.poll() is None:
                os.killpg(pgid, signal.SIGTERM)
                process.wait(timeout=5)


if __name__ == "__main__":
    raise SystemExit(main())
