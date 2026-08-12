#!/usr/bin/env python3
"""Launch the real macOS Tauri app against the deterministic Codex fake."""

import argparse
import hashlib
import json
import os
import socket
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path
from tempfile import TemporaryDirectory


SCENARIOS = ("approval", "user_input", "chunked")
DEV_ROOT_ENV = "YAKSHED_DEV_APP_ROOT"
DEV_CODEX_ENV = "YAKSHED_DEV_APP_CODEX_SCRIPT"
DEV_SCENARIO_ENV = "YAKSHED_DEV_APP_SCENARIO"


def validate_scenario(value):
    if value not in SCENARIOS:
        choices = ", ".join(SCENARIOS)
        raise ValueError(f"scenario must be one of: {choices}")
    return value


def canonical_worktree(path):
    return Path(path).resolve()


def derive_state_root(worktree, temp_root=None):
    canonical = canonical_worktree(worktree)
    digest = hashlib.sha256(str(canonical).encode("utf-8")).hexdigest()
    base = Path(temp_root) if temp_root is not None else Path(tempfile.gettempdir())
    return base.resolve() / "yakshed-dev" / digest


def choose_port():
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


def tauri_config_override(port):
    if not 1 <= port <= 65535:
        raise ValueError(f"port out of range: {port}")
    before_dev = (
        "npm --prefix yakshed-tauri run dev -- "
        f"--host 127.0.0.1 --port {port} --strictPort"
    )
    return json.dumps(
        {
            "build": {
                "devUrl": f"http://127.0.0.1:{port}",
                "beforeDevCommand": before_dev,
            }
        },
        separators=(",", ":"),
    )


def tauri_command(tauri_root, port):
    return [
        "npm",
        "--prefix",
        str(tauri_root),
        "run",
        "tauri",
        "--",
        "dev",
        "--config",
        tauri_config_override(port),
    ]


def launch_environment(state_root, fake_codex, scenario):
    environment = os.environ.copy()
    environment.update(
        {
            DEV_ROOT_ENV: str(state_root),
            DEV_CODEX_ENV: str(fake_codex),
            DEV_SCENARIO_ENV: validate_scenario(scenario),
        }
    )
    return environment


def _usable_cargo(path):
    return path.is_file() and os.access(path, os.X_OK)


def _rustup_cargo(rustup, worktree, environment):
    result = subprocess.run(
        [rustup, "which", "cargo"],
        cwd=worktree,
        env=environment,
        capture_output=True,
        check=False,
        text=True,
    )
    if result.returncode != 0:
        return None
    output = result.stdout.strip().splitlines()
    if not output:
        return None
    cargo = Path(output[-1].strip()).expanduser()
    return cargo if _usable_cargo(cargo) else None


def _toolchain_cargo(rustup_home):
    toolchains = rustup_home / "toolchains"
    if not toolchains.is_dir():
        return None
    for toolchain in sorted(toolchains.iterdir()):
        cargo = toolchain / "bin" / "cargo"
        if _usable_cargo(cargo):
            return cargo.resolve()
    return None


def ensure_cargo_environment(environment, worktree):
    result = environment.copy()
    path = result.get("PATH", "")
    if shutil.which("cargo", path=path):
        return result

    cargo = None
    rustup = shutil.which("rustup", path=path)
    if rustup:
        cargo = _rustup_cargo(rustup, worktree, result)
    if cargo is None:
        rustup_home = Path(
            result.get("RUSTUP_HOME", Path.home() / ".rustup")
        ).expanduser()
        cargo = _toolchain_cargo(rustup_home)
    if cargo is None:
        raise RuntimeError(
            "cargo is not on PATH and no usable rustup toolchain was found; "
            "install Rust with rustup or add cargo to PATH"
        )

    result["PATH"] = os.pathsep.join(filter(None, (str(cargo.parent), path)))
    return result


def check_ui_dependencies(tauri_root):
    bin_root = Path(tauri_root) / "node_modules" / ".bin"
    missing = [name for name in ("tauri", "vite") if not (bin_root / name).is_file()]
    if missing:
        names = ", ".join(missing)
        raise RuntimeError(
            "locked UI dependencies are not installed "
            f"(missing {names}); run `cd crates/yakshed-tauri && npm ci`"
        )


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--scenario",
        choices=SCENARIOS,
        default="approval",
        help="fake Codex journey to exercise (default: approval)",
    )
    parser.add_argument(
        "--self-test",
        action="store_true",
        help="run hermetic launcher construction checks without starting Tauri",
    )
    return parser.parse_args(argv)


def self_test():
    with TemporaryDirectory(prefix="yakshed-dev-app-test-") as temporary:
        fixture = Path(temporary)
        worktree = fixture / "worktree"
        worktree.mkdir()
        tauri_root = worktree / "crates" / "yakshed-tauri"
        tauri_root.mkdir(parents=True)
        fake = worktree / "fake_codex.py"
        fake.write_text("#!/usr/bin/env python3\n", encoding="utf-8")
        system_temp = fixture / "system-temp"
        system_temp.mkdir()

        assert validate_scenario("approval") == "approval"
        assert validate_scenario("user_input") == "user_input"
        assert validate_scenario("chunked") == "chunked"
        try:
            validate_scenario("production")
        except ValueError:
            pass
        else:
            raise AssertionError("invalid scenario was accepted")

        first = derive_state_root(worktree, system_temp)
        second = derive_state_root(worktree, system_temp)
        other = derive_state_root(fixture / "other-worktree", system_temp)
        assert first == second
        assert first.parent == system_temp.resolve() / "yakshed-dev"
        assert first != other

        override = json.loads(tauri_config_override(43127))
        assert override["build"]["devUrl"] == "http://127.0.0.1:43127"
        before_dev = override["build"]["beforeDevCommand"]
        assert "npm --prefix yakshed-tauri run dev" in before_dev
        assert "--strictPort" in before_dev
        command = tauri_command(tauri_root, 43127)
        assert command[:7] == [
            "npm",
            "--prefix",
            str(tauri_root),
            "run",
            "tauri",
            "--",
            "dev",
        ]
        assert command[7] == "--config"
        assert json.loads(command[8]) == override
        assert launch_environment(first, fake, "approval")[DEV_CODEX_ENV] == str(fake)

        normal_bin = fixture / "normal-bin"
        normal_bin.mkdir()
        normal_cargo = normal_bin / "cargo"
        normal_cargo.write_text("#!/bin/sh\n", encoding="utf-8")
        normal_cargo.chmod(0o700)
        normal_environment = {"PATH": str(normal_bin)}
        assert ensure_cargo_environment(normal_environment, worktree) == normal_environment

        rustup_home = fixture / "rustup"
        fallback_bin = rustup_home / "toolchains" / "fixture-toolchain" / "bin"
        fallback_bin.mkdir(parents=True)
        fallback_cargo = fallback_bin / "cargo"
        fallback_cargo.write_text("#!/bin/sh\n", encoding="utf-8")
        fallback_cargo.chmod(0o700)
        fallback_environment = ensure_cargo_environment(
            {"PATH": str(fixture / "empty-bin"), "RUSTUP_HOME": str(rustup_home)},
            worktree,
        )
        assert fallback_environment["PATH"].split(os.pathsep)[0] == str(fallback_bin.resolve())
        assert shutil.which("cargo", path=fallback_environment["PATH"]) == str(
            fallback_cargo.resolve()
        )

    print("PASS dev_app self-test")


def main(argv=None):
    args = parse_args(argv)
    if args.self_test:
        self_test()
        return 0
    if sys.platform != "darwin":
        print("dev_app.py launches the Tauri desktop app on macOS only", file=sys.stderr)
        return 2

    repository = canonical_worktree(Path(__file__).parent.parent)
    tauri_root = repository / "crates" / "yakshed-tauri"
    fake_codex = repository / "crates" / "provider-codex" / "tests" / "fake_codex.py"
    try:
        check_ui_dependencies(tauri_root)
        if not fake_codex.is_file():
            raise RuntimeError(f"shared fake Codex script is missing: {fake_codex}")
        state_root = derive_state_root(repository)
        state_root.mkdir(mode=0o700, parents=True, exist_ok=True)
        os.chmod(state_root, 0o700)
        port = choose_port()
        command = tauri_command(tauri_root, port)
        environment = launch_environment(state_root, fake_codex, args.scenario)
        environment = ensure_cargo_environment(environment, repository)
    except (OSError, RuntimeError, ValueError) as error:
        print(f"dev_app.py: {error}", file=sys.stderr)
        return 2

    print(f"worktree: {repository}")
    print(f"state root: {state_root}")
    print(f"port: {port}")
    print(f"scenario: {args.scenario}")
    print(f"cleanup: rm -rf -- {state_root}")
    print("state is preserved for later launches in this worktree")
    try:
        return subprocess.run(command, cwd=repository, env=environment).returncode
    except OSError as error:
        print(f"dev_app.py: could not start npm: {error}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
