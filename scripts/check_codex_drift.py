#!/usr/bin/env python3
"""Check the latest stable Codex release against YakShed's validated schema.

Exit codes: 0 PASS, 10 DRIFT (newer version, identical schema), 11
SCHEMA-DRIFT (newer version, changed schema), 2 retryable network/API failure,
3 local platform, asset-integrity, or schema-generation failure.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import platform
import re
import subprocess
import sys
import tarfile
import tempfile
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path

sys.dont_write_bytecode = True
from verify_schema_pin import tree_digest

PASS = 0
NETWORK_ERROR = 2
LOCAL_ERROR = 3
DRIFT = 10
SCHEMA_DRIFT = 11
API_ROOT = "https://api.github.com"


class NetworkError(RuntimeError):
    pass


def request_json(url: str) -> dict:
    headers = {
        "Accept": "application/vnd.github+json",
        "User-Agent": "yakshed-codex-drift-detector",
        "X-GitHub-Api-Version": "2022-11-28",
    }
    if token := os.environ.get("GITHUB_TOKEN"):
        headers["Authorization"] = f"Bearer {token}"
    try:
        with urllib.request.urlopen(
            urllib.request.Request(url, headers=headers), timeout=30
        ) as response:
            return json.load(response)
    except (OSError, urllib.error.URLError, json.JSONDecodeError) as error:
        raise NetworkError(str(error)) from error


def release(repo: str, tag: str | None = None) -> dict:
    suffix = f"/releases/tags/{urllib.parse.quote(tag, safe='')}" if tag else "/releases/latest"
    return request_json(f"{API_ROOT}/repos/{repo}{suffix}")


def release_version(value: dict) -> str:
    tag = value.get("tag_name", "")
    match = re.fullmatch(r"rust-v(\d+\.\d+\.\d+)", tag)
    if not match:
        raise ValueError(f"latest release tag is not a stable rust-v release: {tag!r}")
    return match.group(1)


def version_tuple(value: str) -> tuple[int, int, int]:
    match = re.fullmatch(r"(\d+)\.(\d+)\.(\d+)", value)
    if not match:
        raise ValueError(f"invalid Codex version: {value!r}")
    return tuple(map(int, match.groups()))


def current_target() -> str:
    machines = {
        "arm64": "aarch64",
        "aarch64": "aarch64",
        "x86_64": "x86_64",
        "amd64": "x86_64",
    }
    machine = machines.get(platform.machine().lower())
    systems = {"darwin": "apple-darwin", "linux": "unknown-linux-musl", "win32": "pc-windows-msvc"}
    system = systems.get(sys.platform)
    if not machine or not system:
        raise ValueError(f"unsupported platform: {platform.machine()}-{sys.platform}")
    return f"{machine}-{system}"


def asset_for(value: dict, name: str) -> dict:
    try:
        asset = next(asset for asset in value["assets"] if asset["name"] == name)
    except (KeyError, StopIteration) as error:
        raise ValueError(f"release {value.get('tag_name')} has no asset {name}") from error
    digest = asset.get("digest", "")
    if not re.fullmatch(r"sha256:[0-9a-f]{64}", digest):
        raise ValueError(f"release metadata has no valid SHA-256 for {name}")
    return asset


def download_asset(asset: dict, destination: Path) -> None:
    headers = {"User-Agent": "yakshed-codex-drift-detector"}
    if token := os.environ.get("GITHUB_TOKEN"):
        headers["Authorization"] = f"Bearer {token}"
    try:
        with urllib.request.urlopen(
            urllib.request.Request(asset["browser_download_url"], headers=headers), timeout=120
        ) as response, destination.open("wb") as output:
            while chunk := response.read(1024 * 1024):
                output.write(chunk)
    except (OSError, urllib.error.URLError) as error:
        raise NetworkError(str(error)) from error
    expected = asset["digest"].removeprefix("sha256:")
    actual = hashlib.sha256(destination.read_bytes()).hexdigest()
    if actual != expected:
        raise ValueError(f"asset digest mismatch for {asset['name']}: expected {expected}, got {actual}")


def extract_codex(archive: Path, destination: Path) -> None:
    expected = archive.name.removesuffix(".tar.gz")
    with tarfile.open(archive, "r:gz") as bundle:
        members = [
            member
            for member in bundle.getmembers()
            if member.isfile() and Path(member.name).name == expected
        ]
        if len(members) != 1:
            raise ValueError(f"expected one {expected} binary in {archive.name}, found {len(members)}")
        source = bundle.extractfile(members[0])
        if source is None:
            raise ValueError(f"could not extract {expected}")
        with destination.open("wb") as output:
            while chunk := source.read(1024 * 1024):
                output.write(chunk)
    destination.chmod(0o755)


def generate_schema(value: dict, target: str, output: Path) -> dict:
    asset_name = f"codex-{target}{'.exe' if target.endswith('windows-msvc') else ''}.tar.gz"
    asset = asset_for(value, asset_name)
    archive = output.parent / asset_name
    binary = output.parent / asset_name.removesuffix(".tar.gz")
    download_asset(asset, archive)
    extract_codex(archive, binary)
    subprocess.run(
        [str(binary), "app-server", "generate-json-schema", "--out", str(output)],
        check=True,
        timeout=120,
    )
    if not output.is_dir() or not any(output.rglob("*.json")):
        raise ValueError("Codex schema generation produced no JSON files")
    return asset


def file_hashes(root: Path) -> dict[str, str]:
    return {
        path.relative_to(root).as_posix(): hashlib.sha256(path.read_bytes()).hexdigest()
        for path in root.rglob("*")
        if path.is_file()
    }


def run_drift(
    lock: dict,
    *,
    repo_root: Path | None = None,
    release_lookup=release,
    generate_schema_fn=generate_schema,
    target_fn=current_target,
) -> tuple[int, str]:
    repo_root = repo_root or Path(__file__).resolve().parent.parent
    try:
        latest = release_lookup(lock["release_repo"])
        latest_version = release_version(latest)
        validated_version = lock["codex_version"]
        if version_tuple(latest_version) <= version_tuple(validated_version):
            return (
                PASS,
                f"PASS: latest Codex {latest_version}; last validated {validated_version}",
            )

        target = target_fn()
        with tempfile.TemporaryDirectory(prefix="yakshed-codex-drift-") as temp:
            generated = Path(temp) / "schema"
            generate_schema_fn(latest, target, generated)
            actual_digest, actual_count = tree_digest(generated)
            expected_root = repo_root / "pins/codex-app-server-schema"
            expected_digest, expected_count = tree_digest(expected_root)
            if actual_digest == expected_digest:
                return (
                    DRIFT,
                    f"DRIFT: Codex {latest_version} is newer than last validated {validated_version}; "
                    f"schema unchanged ({actual_count} files, sha256 {actual_digest})",
                )

            old, new = file_hashes(expected_root), file_hashes(generated)
            changed = sorted(
                path for path in old.keys() | new.keys() if old.get(path) != new.get(path)
            )
            lines = [
                f"SCHEMA-DRIFT: Codex {latest_version} is newer than last validated "
                f"{validated_version}",
                f"schema: {expected_digest} ({expected_count} files) -> "
                f"{actual_digest} ({actual_count} files)",
                "changed files:",
            ]
            for path in changed:
                status = "added" if path not in old else "removed" if path not in new else "modified"
                lines.append(f"  {status}: {path}")
            return SCHEMA_DRIFT, "\n".join(lines)
    except NetworkError as error:
        return NETWORK_ERROR, f"NETWORK-ERROR (retryable): {error}"
    except (KeyError, OSError, ValueError, subprocess.SubprocessError, tarfile.TarError) as error:
        return LOCAL_ERROR, f"ERROR: {error}"


def _write_schema_tree(root: Path, files: dict[str, str]) -> None:
    for path, value in files.items():
        target = root / path
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(value, encoding="utf-8")


def _fake_release(version: str, asset_name: str) -> dict:
    return {
        "tag_name": f"rust-v{version}",
        "assets": [{"name": asset_name, "digest": f"sha256:{'b'*64}"}],
    }


def _schema_generator(files: dict[str, str]):
    def _generate(_value: dict, _target: str, output: Path) -> dict:
        output.mkdir(parents=True, exist_ok=True)
        _write_schema_tree(output, files)
        return {"name": "codex.tar.gz", "digest": "sha256:" + "c"*64}

    return _generate


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--self-test", action="store_true", help="run offline behavior test suite")
    return parser.parse_args()


def self_test() -> None:
    target = "x86_64-unknown-linux-musl"
    asset_name = f"codex-{target}.tar.gz"
    base_schema = {"schema.json": '{"kind":"base"}\n', "nested/example.json": '{"v":"1"}\n'}
    changed_schema = {"schema.json": '{"kind":"next"}\n', "nested/example.json": '{"v":"1"}\n', "extra.json": '{"v":"added"}\n'}
    with tempfile.TemporaryDirectory(prefix="yakshed-codex-drift-selftest-") as temp:
        repo_root = Path(temp) / "repo"
        expected_root = repo_root / "pins/codex-app-server-schema"
        expected_root.mkdir(parents=True)
        _write_schema_tree(expected_root, base_schema)

        code, message = run_drift(
            {"release_repo": "openai/codex", "codex_version": "9.10.0"},
            repo_root=repo_root,
            release_lookup=lambda *_args: _fake_release("9.9.8", asset_name),
            generate_schema_fn=_schema_generator(base_schema),
            target_fn=lambda: target,
        )
        assert code == PASS
        assert "PASS: latest Codex 9.9.8" in message

        code, message = run_drift(
            {"release_repo": "openai/codex", "codex_version": "9.9.8"},
            repo_root=repo_root,
            release_lookup=lambda *_args: _fake_release("9.10.0", asset_name),
            generate_schema_fn=_schema_generator(base_schema),
            target_fn=lambda: target,
        )
        assert code == DRIFT
        assert "DRIFT: Codex 9.10.0 is newer" in message

        code, message = run_drift(
            {"release_repo": "openai/codex", "codex_version": "9.9.8"},
            repo_root=repo_root,
            release_lookup=lambda *_args: _fake_release("9.10.0", asset_name),
            generate_schema_fn=_schema_generator(changed_schema),
            target_fn=lambda: target,
        )
        assert code == SCHEMA_DRIFT
        assert "SCHEMA-DRIFT: Codex 9.10.0 is newer than last validated 9.9.8" in message
        assert "changed files:" in message
        assert "  added: extra.json" in message

        code, message = run_drift(
            {"release_repo": "openai/codex", "codex_version": "9.9.8"},
            repo_root=repo_root,
            release_lookup=lambda *_args: (_ for _ in ()).throw(NetworkError("network down")),
            generate_schema_fn=_schema_generator(base_schema),
            target_fn=lambda: target,
        )
        assert code == NETWORK_ERROR

        code, message = run_drift(
            {"release_repo": "openai/codex", "codex_version": "9.9.8"},
            repo_root=repo_root,
            release_lookup=lambda *_args: _fake_release("9.10.0", asset_name),
            generate_schema_fn=lambda *_args: (_ for _ in ()).throw(tarfile.TarError("bad archive")),
            target_fn=lambda: target,
        )
        assert code == LOCAL_ERROR
        assert "bad archive" in message
    print("ok: check_codex_drift hermetic self-test passed")


def main() -> int:
    args = parse_args()
    if args.self_test:
        self_test()
        return 0

    repo_root = Path(__file__).resolve().parent.parent
    lock = json.loads((repo_root / "pins/codex-lock.json").read_text(encoding="utf-8"))
    code, message = run_drift(lock, repo_root=repo_root)
    if code in (NETWORK_ERROR, LOCAL_ERROR):
        print(message, file=sys.stderr)
    else:
        print(message)
    return code


if __name__ == "__main__":
    raise SystemExit(main())
