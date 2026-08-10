#!/usr/bin/env python3
"""External acceptance test for the composed YakShed Rust backend.

The script launches the test-only `yakshed-contract-host`, communicates using
backend-contract-v1 JSONL, and verifies high-value state/secret boundaries.
It uses only the Python standard library and never requires a real keyring,
provider credential, network connection, or Tauri runtime.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path
import queue
import secrets
import shutil
import subprocess
import sys
import tempfile
import threading
import time
from typing import Any, Iterable, Mapping, Sequence

PROTOCOL_VERSION = 1
MAX_RESPONSE_BYTES = 4 * 1024 * 1024
MAX_SCAN_FILE_BYTES = 64 * 1024 * 1024

HOME_ID = "0193f26e-7a72-7d42-bf77-0de14c4cc111"
WORK_ID = "0193f26e-7a72-7d42-bf77-0de14c4cc222"
LAB_ID = "0193f26e-7a72-7d42-bf77-0de14c4cc333"
PROJECT_ID = "0193f26e-7a72-7d42-bf77-0de14c4dd000"
WORK_ITEM_ID = "0193f26e-7a72-7d42-bf77-0de14c4dd001"
WORK_LOCATOR = "connection/work/anthropic_api_key"
LAB_LOCATOR = "connection/lab/fireworks_api_key"


class ContractFailure(RuntimeError):
    pass


def parse_args(argv: Sequence[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--host",
        required=True,
        help="Path to the compiled yakshed-contract-host executable.",
    )
    parser.add_argument(
        "--host-arg",
        action="append",
        default=[],
        help="Additional argument passed to the contract host; repeatable.",
    )
    parser.add_argument(
        "--fake-harness",
        required=True,
        help="Path to scripts/fake_harness.py.",
    )
    parser.add_argument(
        "--root",
        help="Use an explicit test root instead of a new temporary directory.",
    )
    parser.add_argument(
        "--keep-temp",
        action="store_true",
        help="Keep the temporary root after the test for inspection.",
    )
    parser.add_argument(
        "--timeout",
        type=float,
        default=15.0,
        help="Per-request timeout in seconds (default: 15).",
    )
    return parser.parse_args(argv)


class CanarySet:
    def __init__(self) -> None:
        nonce = secrets.token_hex(12)
        self.values: dict[str, str] = {
            "work credential": f"yakshed-contract-work-{nonce}",
            "lab credential": f"yakshed-contract-lab-{nonce}",
        }

    def digest(self, label: str) -> str:
        return hashlib.sha256(self.values[label].encode("utf-8")).hexdigest()

    def assert_absent(self, text: str, context: str) -> None:
        for label, value in self.values.items():
            if value in text:
                raise ContractFailure(f"{label} leaked into {context}")

    def redact(self, text: str) -> str:
        for label, value in self.values.items():
            text = text.replace(value, f"<redacted:{label}>")
        return text


class HostClient:
    def __init__(
        self,
        executable: Path,
        root: Path,
        timeout: float,
        canaries: CanarySet,
        extra_args: Sequence[str],
    ) -> None:
        self.executable = executable
        self.root = root
        self.timeout = timeout
        self.canaries = canaries
        self.extra_args = list(extra_args)
        self.process: subprocess.Popen[str] | None = None
        self._next_id = 1
        self._stderr_lines: list[str] = []
        self._stderr_queue: queue.Queue[str] = queue.Queue()
        self._stderr_thread: threading.Thread | None = None

    def start(self) -> None:
        if self.process is not None:
            raise ContractFailure("contract host is already running")

        cmd = [
            str(self.executable),
            "--protocol-version",
            str(PROTOCOL_VERSION),
            "--root",
            str(self.root),
            "--secret-backend",
            "memory",
            "--harness",
            "mock",
            *self.extra_args,
        ]
        self.process = subprocess.Popen(
            cmd,
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            encoding="utf-8",
            errors="strict",
            bufsize=1,
        )
        assert self.process.stderr is not None
        self._stderr_thread = threading.Thread(
            target=self._drain_stderr,
            args=(self.process.stderr,),
            name="yakshed-contract-host-stderr",
            daemon=True,
        )
        self._stderr_thread.start()

    def _drain_stderr(self, stream: Any) -> None:
        try:
            for line in stream:
                self._stderr_lines.append(line)
                self._stderr_queue.put(line)
        finally:
            stream.close()

    def request(self, op: str, params: Mapping[str, Any] | None = None) -> Mapping[str, Any]:
        response = self._request_raw(op, params)
        if not response.get("ok"):
            error = response.get("error") or {}
            code = error.get("code", "unknown")
            message = self.canaries.redact(str(error.get("message", "")))
            raise ContractFailure(f"{op} failed with {code}: {message}")
        result = response.get("result")
        if not isinstance(result, dict):
            raise ContractFailure(f"{op} returned a non-object result")
        return result

    def request_error(
        self,
        op: str,
        expected_code: str,
        params: Mapping[str, Any] | None = None,
    ) -> Mapping[str, Any]:
        response = self._request_raw(op, params)
        if response.get("ok"):
            raise ContractFailure(f"{op} unexpectedly succeeded; expected {expected_code}")
        error = response.get("error")
        if not isinstance(error, dict):
            raise ContractFailure(f"{op} returned a malformed error")
        actual = error.get("code")
        if actual != expected_code:
            raise ContractFailure(f"{op} returned {actual!r}; expected {expected_code!r}")
        return error

    def _request_raw(self, op: str, params: Mapping[str, Any] | None) -> Mapping[str, Any]:
        process = self.process
        if process is None or process.stdin is None or process.stdout is None:
            raise ContractFailure("contract host is not running")
        if process.poll() is not None:
            raise ContractFailure(self._premature_exit_message(op))

        request_id = self._next_id
        self._next_id += 1
        payload = {"id": request_id, "op": op, "params": dict(params or {})}
        process.stdin.write(json.dumps(payload, separators=(",", ":")) + "\n")
        process.stdin.flush()

        line_box: list[str] = []
        error_box: list[BaseException] = []

        def read_line() -> None:
            try:
                line_box.append(process.stdout.readline())
            except BaseException as exc:  # surfaced in caller thread
                error_box.append(exc)

        thread = threading.Thread(target=read_line, daemon=True)
        thread.start()
        thread.join(self.timeout)
        if thread.is_alive():
            self.terminate()
            raise ContractFailure(f"{op} timed out after {self.timeout:.1f}s")
        if error_box:
            raise ContractFailure(f"failed reading response for {op}: {error_box[0]}")
        line = line_box[0] if line_box else ""
        if not line:
            raise ContractFailure(self._premature_exit_message(op))
        if len(line.encode("utf-8")) > MAX_RESPONSE_BYTES:
            raise ContractFailure(f"{op} response exceeded {MAX_RESPONSE_BYTES} bytes")

        self.canaries.assert_absent(line, f"{op} response")
        try:
            response = json.loads(line)
        except json.JSONDecodeError as exc:
            raise ContractFailure(f"{op} returned invalid JSON: {exc}") from exc
        if not isinstance(response, dict):
            raise ContractFailure(f"{op} returned a non-object response")
        if response.get("id") != request_id:
            raise ContractFailure(
                f"{op} response id {response.get('id')!r} did not match {request_id}"
            )
        return response

    def _premature_exit_message(self, op: str) -> str:
        process = self.process
        code = process.poll() if process is not None else None
        tail = "".join(self._stderr_lines[-20:])
        tail = self.canaries.redact(tail).strip()
        suffix = f"; stderr tail: {tail}" if tail else ""
        return f"contract host exited during {op} with code {code}{suffix}"

    def shutdown(self) -> None:
        process = self.process
        if process is None:
            return
        if process.poll() is None:
            try:
                result = self.request("shutdown")
                if result.get("shutting_down") is not True:
                    raise ContractFailure("shutdown did not acknowledge shutting_down=true")
            except (BrokenPipeError, ContractFailure):
                self.terminate()
                raise
        try:
            code = process.wait(timeout=self.timeout)
        except subprocess.TimeoutExpired as exc:
            self.terminate()
            raise ContractFailure("contract host did not exit after shutdown") from exc
        if code != 0:
            raise ContractFailure(self._premature_exit_message("shutdown"))
        self.process = None
        if self._stderr_thread is not None:
            self._stderr_thread.join(timeout=1.0)
        self._assert_stderr_clean()

    def terminate(self) -> None:
        process = self.process
        if process is None:
            return
        if process.poll() is None:
            process.kill()
        try:
            process.wait(timeout=5)
        except subprocess.TimeoutExpired:
            pass
        self.process = None

    def _assert_stderr_clean(self) -> None:
        text = "".join(self._stderr_lines)
        self.canaries.assert_absent(text, "contract host stderr")



def require(condition: bool, message: str) -> None:
    if not condition:
        raise ContractFailure(message)



def assert_paths_under_root(paths: Mapping[str, Any], root: Path) -> None:
    seen: set[Path] = set()
    root_resolved = root.resolve()
    for key in ("config_root", "cache_root", "data_root", "runtime_root"):
        raw = paths.get(key)
        require(isinstance(raw, str) and raw, f"paths.read omitted {key}")
        candidate = Path(raw).resolve()
        try:
            candidate.relative_to(root_resolved)
        except ValueError as exc:
            raise ContractFailure(f"{key} escaped the test root") from exc
        require(candidate not in seen, f"{key} is not distinct")
        seen.add(candidate)



def scan_tree_for_canaries(root: Path, canaries: CanarySet) -> None:
    for path in root.rglob("*"):
        try:
            if not path.is_file() or path.is_symlink():
                continue
            size = path.stat().st_size
            if size > MAX_SCAN_FILE_BYTES:
                raise ContractFailure(
                    f"refusing to skip oversized contract artifact {path} ({size} bytes)"
                )
            data = path.read_bytes()
        except FileNotFoundError:
            continue
        except OSError as exc:
            raise ContractFailure(f"could not inspect contract file {path}: {exc}") from exc
        for label, value in canaries.values.items():
            if value.encode("utf-8") in data:
                raise ContractFailure(f"{label} leaked to disk at {path}")



def put_connection(
    host: HostClient,
    revision: int,
    connection: Mapping[str, Any],
) -> int:
    result = host.request(
        "connection.put",
        {"expected_config_revision": revision, "connection": connection},
    )
    new_revision = result.get("config_revision")
    require(isinstance(new_revision, int), "connection.put omitted config_revision")
    require(new_revision > revision, "config revision did not increase")
    return new_revision



def delegated_connection() -> Mapping[str, Any]:
    return {
        "id": HOME_ID,
        "name": "Home",
        "harness": "mock",
        "model_provider": "openai",
        "provider_state": "home-test",
        "credentials": [
            {
                "slot": "codex.account",
                "source": "delegated",
                "authority": "mock-delegated",
                "delivery": {"kind": "harness_managed"},
            }
        ],
    }



def work_connection() -> Mapping[str, Any]:
    return {
        "id": WORK_ID,
        "name": "Work",
        "harness": "mock",
        "model_provider": "anthropic",
        "provider_state": "work-test",
        "credentials": [
            {
                "slot": "anthropic.api_key",
                "source": "secret",
                "backend": "memory",
                "locator": WORK_LOCATOR,
                "delivery": {
                    "kind": "process_environment",
                    "variable": "ANTHROPIC_API_KEY",
                },
            }
        ],
    }



def lab_connection() -> Mapping[str, Any]:
    return {
        "id": LAB_ID,
        "name": "Lab",
        "harness": "mock",
        "model_provider": "fireworks",
        "provider_state": "lab-test",
        "credentials": [
            {
                "slot": "fireworks.api_key",
                "source": "secret",
                "backend": "memory",
                "locator": LAB_LOCATOR,
                "delivery": {
                    "kind": "process_environment",
                    "variable": "FIREWORKS_API_KEY",
                },
            }
        ],
    }



def expect_status(host: HostClient, locator: str, expected: str) -> None:
    result = host.request(
        "secret.status", {"backend": "memory", "locator": locator}
    )
    require(result.get("status") == expected, f"{locator} status was not {expected}")



def run_contract(args: argparse.Namespace, root: Path, canaries: CanarySet) -> None:
    host_path = Path(args.host).resolve()
    fake_harness = Path(args.fake_harness).resolve()
    require(host_path.is_file(), f"contract host not found: {host_path}")
    require(fake_harness.is_file(), f"fake harness not found: {fake_harness}")

    host = HostClient(host_path, root, args.timeout, canaries, args.host_arg)
    try:
        host.start()
        hello = host.request("hello", {"protocol_version": PROTOCOL_VERSION})
        require(hello.get("protocol_version") == PROTOCOL_VERSION, "protocol mismatch")
        paths = host.request("paths.read")
        assert_paths_under_root(paths, root)

        revision = 0
        revision = put_connection(host, revision, delegated_connection())
        revision = put_connection(host, revision, work_connection())
        revision = put_connection(host, revision, lab_connection())

        listed = host.request("connection.list")
        connections = listed.get("connections")
        require(isinstance(connections, list) and len(connections) == 3, "expected 3 connections")
        delegated = host.request("connection.get", {"connection_id": HOME_ID})
        delegated_credentials = delegated.get("connection", {}).get("credentials", [])
        require(
            delegated_credentials
            and delegated_credentials[0].get("status") == "delegated"
            and delegated_credentials[0].get("delivery") == {"kind": "harness_managed"},
            "delegated harness-managed delivery was not preserved",
        )

        host.request(
            "secret.put",
            {
                "backend": "memory",
                "locator": WORK_LOCATOR,
                "value": canaries.values["work credential"],
                "overwrite": False,
            },
        )
        host.request_error(
            "secret.put",
            "already_exists",
            {
                "backend": "memory",
                "locator": WORK_LOCATOR,
                "value": canaries.values["work credential"],
                "overwrite": False,
            },
        )
        host.request(
            "secret.put",
            {
                "backend": "memory",
                "locator": LAB_LOCATOR,
                "value": canaries.values["lab credential"],
                "overwrite": False,
            },
        )
        expect_status(host, WORK_LOCATOR, "present")
        expect_status(host, LAB_LOCATOR, "present")
        expect_status(host, "connection/other/missing", "missing")

        host.request(
            "work.create",
            {
                "id": WORK_ITEM_ID,
                "project_id": PROJECT_ID,
                "title": "Contract test work item",
            },
        )
        work = host.request("work.get", {"work_item_id": WORK_ITEM_ID})
        require(work.get("work_item", {}).get("title") == "Contract test work item", "work item did not round-trip")

        host.request(
            "cache.put",
            {
                "namespace": "contract",
                "key": "model-catalog",
                "value": {"models": ["mock-model"]},
            },
        )
        require(
            host.request(
                "cache.exists", {"namespace": "contract", "key": "model-catalog"}
            ).get("exists")
            is True,
            "cache entry was not written",
        )
        host.request("cache.clear")
        require(
            host.request(
                "cache.exists", {"namespace": "contract", "key": "model-catalog"}
            ).get("exists")
            is False,
            "cache.clear did not remove cache entry",
        )
        host.request("connection.get", {"connection_id": WORK_ID})
        host.request("work.get", {"work_item_id": WORK_ITEM_ID})
        expect_status(host, WORK_LOCATOR, "present")

        for connection_id, slot, expected_label, forbidden in (
            (
                WORK_ID,
                "anthropic.api_key",
                "work credential",
                ["OPENAI_API_KEY", "FIREWORKS_API_KEY"],
            ),
            (
                LAB_ID,
                "fireworks.api_key",
                "lab credential",
                ["OPENAI_API_KEY", "ANTHROPIC_API_KEY"],
            ),
        ):
            probe = host.request(
                "runtime.credential_probe",
                {
                    "connection_id": connection_id,
                    "slot": slot,
                    "probe_program": sys.executable,
                    "probe_args": [str(fake_harness)],
                    "expected_sha256": canaries.digest(expected_label),
                    "forbidden_variables": forbidden,
                },
            )
            require(probe.get("matched") is True, f"credential probe failed for {slot}")
            require(probe.get("forbidden_present") == [], f"forbidden variables reached {slot} probe")

        sleeping_probe = root / "sleeping_probe.py"
        sleeping_pid = root / "sleeping_probe.pid"
        sleeping_probe.write_text(
            "import pathlib,subprocess,sys\n"
            "child=subprocess.Popen([sys.executable,'-c','import time;time.sleep(60)'])\n"
            "pathlib.Path(sys.argv[1]).write_text(str(child.pid))\n",
            encoding="utf-8",
        )
        host.request_error(
            "runtime.credential_probe",
            "timeout",
            {
                "connection_id": WORK_ID,
                "slot": "anthropic.api_key",
                "probe_program": sys.executable,
                "probe_args": [str(sleeping_probe), str(sleeping_pid)],
                "expected_sha256": canaries.digest("work credential"),
                "forbidden_variables": [],
            },
        )
        probe_pid = int(sleeping_pid.read_text(encoding="utf-8"))
        reap_deadline = time.monotonic() + 2
        while True:
            try:
                os.kill(probe_pid, 0)
            except ProcessLookupError:
                break
            if time.monotonic() >= reap_deadline:
                raise ContractFailure("timed-out credential probe group remained alive")
            time.sleep(0.01)
        expect_status(host, WORK_LOCATOR, "present")

        config_text = (root / "config" / "config.toml").read_text(encoding="utf-8")
        require(
            "delivery" not in config_text,
            "adapter-owned credential delivery leaked into canonical config",
        )

        scan_tree_for_canaries(root, canaries)
        host.shutdown()

        # Restart with the same persistent roots. Memory-backed values must disappear.
        host = HostClient(host_path, root, args.timeout, canaries, args.host_arg)
        host.start()
        host.request("hello", {"protocol_version": PROTOCOL_VERSION})
        connections = host.request("connection.list").get("connections")
        require(isinstance(connections, list) and len(connections) == 3, "connections did not persist")
        host.request("work.get", {"work_item_id": WORK_ITEM_ID})
        expect_status(host, WORK_LOCATOR, "missing")
        expect_status(host, LAB_LOCATOR, "missing")

        host.request(
            "secret.put",
            {
                "backend": "memory",
                "locator": WORK_LOCATOR,
                "value": canaries.values["work credential"],
                "overwrite": False,
            },
        )
        persisted = host.request("connection.get", {"connection_id": WORK_ID})
        credentials = persisted.get("connection", {}).get("credentials", [])
        require(
            credentials
            and credentials[0].get("delivery")
            == {"kind": "process_environment", "variable": "ANTHROPIC_API_KEY"},
            "credential delivery metadata did not survive restart",
        )
        restarted_probe = host.request(
            "runtime.credential_probe",
            {
                "connection_id": WORK_ID,
                "slot": "anthropic.api_key",
                "probe_program": sys.executable,
                "probe_args": [str(fake_harness)],
                "expected_sha256": canaries.digest("work credential"),
                "forbidden_variables": ["OPENAI_API_KEY", "FIREWORKS_API_KEY"],
            },
        )
        require(restarted_probe.get("matched") is True, "credential probe failed after restart")
        reset = host.request("config.reset")
        reset_revision = reset.get("config_revision")
        require(isinstance(reset_revision, int), "config.reset omitted revision")
        host.request("work.get", {"work_item_id": WORK_ITEM_ID})
        expect_status(host, WORK_LOCATOR, "present")
        listed = host.request("connection.list")
        require(listed.get("connections") == [], "config.reset did not remove connections")

        revision = put_connection(host, reset_revision, work_connection())
        host.request("data.purge")
        host.request("connection.get", {"connection_id": WORK_ID})
        expect_status(host, WORK_LOCATOR, "present")
        host.request_error("work.get", "not_found", {"work_item_id": WORK_ITEM_ID})

        summary = host.request("state.summary")
        require(summary.get("connections") == 1, "data.purge changed config")
        require(summary.get("work_items") == 0, "data.purge did not clear work data")
        scan_tree_for_canaries(root, canaries)
        host.shutdown()
    finally:
        host.terminate()



def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(sys.argv[1:] if argv is None else argv)
    canaries = CanarySet()

    explicit_root = Path(args.root).expanduser().resolve() if args.root else None
    temp_dir: tempfile.TemporaryDirectory[str] | None = None
    if explicit_root is None:
        temp_dir = tempfile.TemporaryDirectory(prefix="yakshed-contract-")
        root = Path(temp_dir.name).resolve()
    else:
        root = explicit_root
        root.mkdir(parents=True, exist_ok=True)

    try:
        run_contract(args, root, canaries)
    except (ContractFailure, OSError, subprocess.SubprocessError) as exc:
        message = canaries.redact(str(exc))
        print(f"FAIL: {message}", file=sys.stderr)
        if explicit_root is not None:
            print(f"Test root retained at: {root}", file=sys.stderr)
        return 1
    else:
        print("PASS: YakShed backend contract v1")
        if explicit_root is not None:
            print(f"Test root retained at: {root}")
        return 0
    finally:
        if temp_dir is not None:
            if args.keep_temp:
                # TemporaryDirectory has no detach API; copy to a stable sibling.
                kept = Path(tempfile.gettempdir()) / f"yakshed-contract-kept-{int(time.time())}"
                shutil.copytree(root, kept, dirs_exist_ok=False)
                print(f"Copied retained test root to: {kept}", file=sys.stderr)
            temp_dir.cleanup()


if __name__ == "__main__":
    raise SystemExit(main())
