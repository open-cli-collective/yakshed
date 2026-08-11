#!/usr/bin/env python3
"""Advance YakShed's last-validated Codex release and committed schema.

Usage: repin_codex.py [rust-vX.Y.Z] [--dry-run] | --self-test
Exit codes: 0 success, 2 retryable network/API failure, 3 invalid release,
asset-integrity, schema-generation, or filesystem failure.
"""

from __future__ import annotations

import argparse
import json
import shutil
import subprocess
import sys
import tempfile
from datetime import UTC, datetime
from pathlib import Path

sys.dont_write_bytecode = True
from check_codex_drift import (
    NetworkError,
    asset_for,
    current_target,
    generate_schema,
    release,
    release_version,
)
from verify_schema_pin import tree_digest


def updated_lock(lock: dict, value: dict, schema_digest: str, validated_at: str) -> dict:
    version = release_version(value)
    tag = value["tag_name"]
    result = dict(lock)
    result["codex_version"] = version
    result["release_tag"] = tag
    result["targets"] = {
        target: {
            "asset": details["asset"],
            "sha256": asset_for(value, details["asset"])["digest"].removeprefix("sha256:"),
        }
        for target, details in lock["targets"].items()
    }
    result["stable_schema_sha256"] = schema_digest
    result["schema_provenance"] = (
        f"Last validated from the {result['targets'][current_target()]['asset']} release asset for "
        f"{tag}, downloaded and digest-checked against GitHub release metadata on {validated_at}."
    )
    result["adapter_revision"] = int(lock["adapter_revision"]) + 1
    result["pinned_at"] = validated_at
    result["notes"] = (
        "YakShed tracks the latest stable Codex release at runtime; this record is the last version "
        "validated against the adapter and committed schema. Release-asset digests support reproducible "
        "validation and drift detection, not a runtime version gate."
    )
    return result


def self_test() -> None:
    target = current_target()
    name = f"codex-{target}{'.exe' if target.endswith('windows-msvc') else ''}.tar.gz"
    lock = {"targets": {target: {"asset": name, "sha256": "old"}}, "adapter_revision": 4}
    value = {
        "tag_name": "rust-v9.8.7",
        "assets": [{"name": name, "digest": f"sha256:{'a' * 64}"}],
    }
    actual = updated_lock(lock, value, "schema", "2030-01-02")
    assert actual["codex_version"] == "9.8.7"
    assert actual["targets"][target]["sha256"] == "a" * 64
    assert actual["stable_schema_sha256"] == "schema"
    assert actual["adapter_revision"] == 5
    assert actual["pinned_at"] == "2030-01-02"
    print("ok: lock rewrite self-test passed")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("tag", nargs="?", help="release tag (default: latest stable release)")
    parser.add_argument(
        "--dry-run", action="store_true", help="validate and print the new lock without writing"
    )
    parser.add_argument(
        "--self-test", action="store_true", help="test lock rewriting without network access"
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    if args.self_test:
        self_test()
        return 0

    repo_root = Path(__file__).resolve().parent.parent
    pins = repo_root / "pins"
    lock_path = pins / "codex-lock.json"
    schema_root = pins / "codex-app-server-schema"
    try:
        lock = json.loads(lock_path.read_text(encoding="utf-8"))
        value = release(lock["release_repo"], args.tag)
        target = current_target()
        with tempfile.TemporaryDirectory(prefix=".codex-repin-", dir=pins) as temp:
            generated = Path(temp) / "schema"
            generate_schema(value, target, generated)
            digest, count = tree_digest(generated)
            replacement = updated_lock(lock, value, digest, datetime.now(UTC).date().isoformat())
            rendered = json.dumps(replacement, indent=2) + "\n"
            if args.dry_run:
                print(rendered, end="")
                print(f"dry-run: generated schema has {count} files, sha256 {digest}", file=sys.stderr)
                return 0

            backup = Path(temp) / "previous-schema"
            new_lock = Path(temp) / "codex-lock.json"
            new_lock.write_text(rendered, encoding="utf-8")
            schema_root.rename(backup)
            try:
                generated.rename(schema_root)
                new_lock.replace(lock_path)
            except Exception:
                if schema_root.exists():
                    shutil.rmtree(schema_root)
                backup.rename(schema_root)
                raise
        print(f"repinned Codex to {replacement['release_tag']} ({count} schema files, sha256 {digest})")
        return 0
    except NetworkError as error:
        print(f"NETWORK-ERROR (retryable): {error}", file=sys.stderr)
        return 2
    except (KeyError, OSError, ValueError, subprocess.SubprocessError) as error:
        print(f"ERROR: {error}", file=sys.stderr)
        return 3


if __name__ == "__main__":
    raise SystemExit(main())
