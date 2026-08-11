#!/usr/bin/env python3
"""Check the latest stable Codex release against YakShed's validated schema.

Exit codes: 0 PASS, 10 DRIFT (newer version, identical schema), 11
SCHEMA-DRIFT (newer version, changed schema), 2 retryable network/API failure,
3 local platform, asset-integrity, or schema-generation failure.
"""

from __future__ import annotations

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


def main() -> int:
    repo_root = Path(__file__).resolve().parent.parent
    lock = json.loads((repo_root / "pins/codex-lock.json").read_text(encoding="utf-8"))
    try:
        latest = release(lock["release_repo"])
        latest_version = release_version(latest)
        validated_version = lock["codex_version"]
        if version_tuple(latest_version) <= version_tuple(validated_version):
            print(f"PASS: latest Codex {latest_version}; last validated {validated_version}")
            return PASS

        target = current_target()
        with tempfile.TemporaryDirectory(prefix="yakshed-codex-drift-") as temp:
            generated = Path(temp) / "schema"
            generate_schema(latest, target, generated)
            actual_digest, actual_count = tree_digest(generated)
            expected_root = repo_root / "pins/codex-app-server-schema"
            expected_digest, expected_count = tree_digest(expected_root)
            if actual_digest == expected_digest:
                print(
                    f"DRIFT: Codex {latest_version} is newer than last validated {validated_version}; "
                    f"schema unchanged ({actual_count} files, sha256 {actual_digest})"
                )
                return DRIFT

            old, new = file_hashes(expected_root), file_hashes(generated)
            changed = sorted(
                path for path in old.keys() | new.keys() if old.get(path) != new.get(path)
            )
            print(
                f"SCHEMA-DRIFT: Codex {latest_version} is newer than last validated "
                f"{validated_version}"
            )
            print(
                f"schema: {expected_digest} ({expected_count} files) -> "
                f"{actual_digest} ({actual_count} files)"
            )
            print("changed files:")
            for path in changed:
                status = "added" if path not in old else "removed" if path not in new else "modified"
                print(f"  {status}: {path}")
            return SCHEMA_DRIFT
    except NetworkError as error:
        print(f"NETWORK-ERROR (retryable): {error}", file=sys.stderr)
        return NETWORK_ERROR
    except (KeyError, OSError, ValueError, subprocess.SubprocessError, tarfile.TarError) as error:
        print(f"ERROR: {error}", file=sys.stderr)
        return LOCAL_ERROR


if __name__ == "__main__":
    raise SystemExit(main())
