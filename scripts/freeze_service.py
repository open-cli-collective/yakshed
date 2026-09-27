"""Build and smoke-test the frozen YakShed Python service.

The Codex CLI is a native executable shipped by ``openai-codex-cli-bin``. It
must be collected together with the SDK package metadata so the SDK's normal
``_installed_codex_path`` resolver keeps working inside the PyInstaller
bundle. macOS packaging uses Python 3.12; Python 3.13 is also accepted.
"""

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
import time


ROOT = Path(__file__).resolve().parents[1]
ENTRY = ROOT / "scripts" / "freeze_entry.py"
PYINSTALLER_VERSION = "6.16.0"


def codex_version() -> str:
    for line in (ROOT / "backend" / "requirements.txt").read_text().splitlines():
        if line.startswith("openai-codex=="):
            return line.split("==", 1)[1].strip()
    raise SystemExit("backend/requirements.txt is missing its pinned openai-codex dependency")


CODEX_VERSION = codex_version()


def supported_python() -> bool:
    return sys.version_info[:2] in {(3, 12), (3, 13)}


def reexecute_with_uv(argv: list[str]) -> int | None:
    if supported_python() or os.environ.get("YAKSHED_FREEZE_REEXEC") == "1":
        return None
    requested = os.environ.get("YAKSHED_FREEZE_PYTHON")
    if requested:
        interpreter = Path(requested).expanduser()
        if not interpreter.is_file():
            raise SystemExit(f"YAKSHED_FREEZE_PYTHON does not point to an interpreter: {interpreter}")
        env = os.environ.copy()
        env["YAKSHED_FREEZE_REEXEC"] = "1"
        completed = subprocess.run([str(interpreter), str(Path(__file__).resolve()), *argv], cwd=ROOT, env=env)
        return completed.returncode
    uv = shutil.which("uv")
    if not uv:
        raise SystemExit("freeze_service.py requires Python 3.12 or 3.13 (install uv or set YAKSHED_FREEZE_PYTHON)")
    env = os.environ.copy()
    env["YAKSHED_FREEZE_REEXEC"] = "1"
    command = [
        uv,
        "run",
        "--python",
        "3.12",
        "--with",
        f"PyInstaller=={PYINSTALLER_VERSION}",
        "--with",
        f"openai-codex=={CODEX_VERSION}",
        str(Path(__file__).resolve()),
        *argv,
    ]
    completed = subprocess.run(command, cwd=ROOT, env=env)
    return completed.returncode


def require_imports() -> None:
    missing = []
    for module in ("PyInstaller", "openai_codex", "codex_cli_bin"):
        try:
            __import__(module)
        except ImportError:
            missing.append(module)
    if missing:
        raise SystemExit(
            "missing freeze dependencies: "
            + ", ".join(missing)
            + f"; install PyInstaller=={PYINSTALLER_VERSION} and openai-codex=={CODEX_VERSION} in the pinned Python"
        )


def run_service_smoke(binary: Path) -> None:
    with tempfile.TemporaryDirectory(prefix="yakshed-frozen-smoke-") as temporary:
        data_dir = Path(temporary) / "data"
        codex_home = Path(temporary) / "codex-home"
        codex_home.mkdir()
        process = subprocess.Popen(
            [str(binary), "--data-dir", str(data_dir), "--demo"],
            cwd=ROOT,
            env={**os.environ, "CODEX_HOME": str(codex_home), "YAKSHED_DESKTOP": "1"},
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.DEVNULL,
            text=True,
        )
        import selectors

        selector = selectors.DefaultSelector()
        assert process.stdout is not None
        selector.register(process.stdout, selectors.EVENT_READ)

        def read_response(deadline: float) -> dict:
            while time.monotonic() < deadline:
                if process.poll() is not None:
                    break
                ready = selector.select(timeout=0.2)
                if ready:
                    line = process.stdout.readline()
                    if line:
                        return json.loads(line)
            raise RuntimeError("frozen service did not answer its request")

        try:
            assert process.stdin is not None and process.stdout is not None
            process.stdin.write(json.dumps({"id": "freeze", "method": "snapshot", "params": {}}) + "\n")
            process.stdin.flush()
            response = read_response(time.monotonic() + 20)
            if response.get("id") != "freeze" or not isinstance(response.get("result"), dict):
                raise RuntimeError("frozen service returned an invalid snapshot")
            if not any(adapter.get("id") == "demo" for adapter in response["result"].get("adapters", [])):
                raise RuntimeError("frozen service did not enable its explicit demo adapter")
            process.stdin.write(json.dumps({"id": "status", "method": "adapter.status", "params": {"adapter": "codex"}}) + "\n")
            process.stdin.flush()
            status = read_response(time.monotonic() + 30)
            status_result = status.get("result")
            if (
                status.get("id") != "status"
                or not isinstance(status_result, dict)
                or status_result.get("installed") is not True
                or status_result.get("version") != CODEX_VERSION
                or not isinstance(status_result.get("authenticated"), bool)
                or status_result.get("error") is not None
            ):
                raise RuntimeError("frozen service could not initialize the bundled Codex runtime")
        finally:
            selector.close()
            if process.stdin and not process.stdin.closed:
                process.stdin.close()
            try:
                process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait(timeout=5)


def build(output: Path) -> None:
    require_imports()
    output = output.expanduser().resolve()
    output.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix="yakshed-pyinstaller-") as temporary:
        temp_root = Path(temporary)
        dist = temp_root / "dist"
        work = temp_root / "work"
        spec = temp_root / "spec"
        command = [
            sys.executable,
            "-m",
            "PyInstaller",
            "--noconfirm",
            "--clean",
            "--onefile",
            "--name",
            "yakshed-service",
            "--distpath",
            str(dist),
            "--workpath",
            str(work),
            "--specpath",
            str(spec),
            "--paths",
            str(ROOT),
            "--hidden-import",
            "backend.yakshed",
            "--collect-all",
            "openai_codex",
            "--collect-all",
            "codex_cli_bin",
            "--copy-metadata",
            "openai-codex",
            "--copy-metadata",
            "openai-codex-cli-bin",
            str(ENTRY),
        ]
        subprocess.run(command, cwd=ROOT, check=True)
        built = dist / "yakshed-service"
        if not built.is_file():
            raise RuntimeError("PyInstaller did not produce yakshed-service")
        shutil.copy2(built, output)
    output.chmod(output.stat().st_mode | 0o111)
    run_service_smoke(output)
    print(f"frozen service ready: {output}")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Freeze and smoke-test the YakShed service")
    parser.add_argument("--output", type=Path, default=ROOT / "backend" / "dist" / "yakshed-service")
    args = parser.parse_args(argv)
    result = reexecute_with_uv(sys.argv[1:] if argv is None else argv)
    if result is not None:
        return result
    if not supported_python():
        raise SystemExit("freeze_service.py requires Python 3.12 or 3.13")
    build(args.output)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
