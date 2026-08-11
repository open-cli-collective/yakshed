#!/usr/bin/env python3
"""Advance YakShed's last-validated Codex release and committed schema.

Usage: repin_codex.py [rust-vX.Y.Z] [--dry-run] | --self-test
Exit codes: 0 success, 2 retryable network/API failure, 3 invalid release,
asset-integrity, schema-generation, or filesystem failure.
"""

from __future__ import annotations

import argparse
import json
import re
import shutil
import subprocess
import sys
import tempfile
import tarfile
from datetime import UTC, datetime
from pathlib import Path

sys.dont_write_bytecode = True
import check_codex_drift as drift
from check_codex_drift import (
    NetworkError,
    asset_for,
    release,
    release_version,
)
from verify_schema_pin import tree_digest

REPO_ROOT = Path(__file__).resolve().parent.parent
LIB_RS = Path("crates/provider-codex/src/lib.rs")
FAKE_CODEX = Path("crates/provider-codex/tests/fake_codex.py")

CODEX_VERSION_RE = re.compile(
    r'^(\s*const LAST_VALIDATED_CODEX_VERSION: &str = ")\d+\.\d+\.\d+(";)',
    re.MULTILINE,
)
FAKE_CLI_VERSION_RE = re.compile(r'("cliVersion":\s*")\d+\.\d+\.\d+(")', re.MULTILINE)


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
        f"Last validated from the {result['targets'][drift.current_target()]['asset']} release asset for "
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


def update_last_validated_metadata(
    root_version: str,
    repo_root: Path = REPO_ROOT,
    replace_fn=Path.replace,
) -> list[tuple[Path, str]]:
    paths = [
        (repo_root / LIB_RS, CODEX_VERSION_RE, f'\\g<1>{root_version}\\g<2>'),
        (repo_root / FAKE_CODEX, FAKE_CLI_VERSION_RE, f'\\g<1>{root_version}\\g<2>'),
    ]
    staged_updates: list[tuple[Path, str, Path]] = []
    for path, pattern, replacement in paths:
        text = path.read_text(encoding="utf-8")
        rewritten, count = pattern.subn(replacement, text)
        if count != 1:
            raise ValueError(f"unable to update last-validated version in {path}")
        temporary = path.with_suffix(path.suffix + ".codex-repin")
        temporary.write_text(rewritten, encoding="utf-8")
        staged_updates.append((path, text, temporary))

    committed: list[tuple[Path, str]] = []
    try:
        for path, original_text, temporary in staged_updates:
            replace_fn(temporary, path)
            committed.append((path, original_text))
    except Exception as error:
        for path, original_text in reversed(committed):
            path.write_text(original_text, encoding="utf-8")
        for _, _original_text, temporary in reversed(staged_updates):
            if temporary.exists():
                temporary.unlink()
        raise
    for _, _original_text, temporary in staged_updates:
        if temporary.exists():
            temporary.unlink()
    return [(path, original_text) for path, original_text, _ in staged_updates]


def run_repin(
    lock: dict,
    *,
    tag: str | None,
    dry_run: bool,
    update_metadata: bool,
    repo_root: Path = REPO_ROOT,
    release_fn=release,
    generate_schema_fn=drift.generate_schema,
    current_target_fn=drift.current_target,
) -> int:
    pins = repo_root / "pins"
    lock_path = pins / "codex-lock.json"
    schema_root = pins / "codex-app-server-schema"

    value = release_fn(lock["release_repo"], tag)
    target = current_target_fn()
    with tempfile.TemporaryDirectory(prefix=".codex-repin-", dir=pins) as temp:
        generated = Path(temp) / "schema"
        generate_schema_fn(value, target, generated)
        digest, count = tree_digest(generated)
        replacement = updated_lock(lock, value, digest, datetime.now(UTC).date().isoformat())
        rendered = json.dumps(replacement, indent=2) + "\n"
        if dry_run:
            print(rendered, end="")
            print(f"dry-run: generated schema has {count} files, sha256 {digest}", file=sys.stderr)
            return 0

        backup = Path(temp) / "previous-schema"
        new_lock = Path(temp) / "codex-lock.json"
        new_lock.write_text(rendered, encoding="utf-8")

        metadata_backups: list[tuple[Path, str]] = []
        if update_metadata:
            metadata_backups = update_last_validated_metadata(
                replacement["codex_version"], repo_root
            )
        try:
            schema_root.rename(backup)
            generated.rename(schema_root)
            new_lock.replace(lock_path)
        except Exception:
            if metadata_backups:
                for path, original_text in reversed(metadata_backups):
                    path.write_text(original_text, encoding="utf-8")
            if schema_root.exists():
                shutil.rmtree(schema_root)
            if backup.exists():
                backup.rename(schema_root)
            if new_lock.exists():
                new_lock.unlink()
            lock_path.write_text(json.dumps(lock, indent=2) + "\n", encoding="utf-8")
            raise

        print(
            f"repinned Codex to {replacement['release_tag']} ({count} schema files, sha256 {digest})"
        )
        return 0


def self_test() -> None:
    target = drift.current_target()
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
    with tempfile.TemporaryDirectory(prefix=".codex-repin-selftest-", dir=Path(__file__).resolve().parent) as temp:
        root = Path(temp) / "repo"
        pins = root / "pins"
        (pins / "codex-app-server-schema").mkdir(parents=True, exist_ok=True)
        (pins / "codex-lock.json").write_text(
            json.dumps({"release_repo": "openai/codex", "codex_version": "9.8.6", "targets": {target: {"asset": name, "sha256": "a"*64}}, "adapter_revision": 0}, indent=2) + "\n",
            encoding="utf-8",
        )
        (root / "crates/provider-codex/src").mkdir(parents=True, exist_ok=True)
        (root / "crates/provider-codex/tests").mkdir(parents=True, exist_ok=True)
        (root / LIB_RS).write_text(
            'const LAST_VALIDATED_CODEX_VERSION: &str = "9.8.6";\n',
            encoding="utf-8",
        )
        (root / FAKE_CODEX).write_text(
            '{"cliVersion": "9.8.6", "model": "fake-model"}\n',
            encoding="utf-8",
        )
        real_lib = (REPO_ROOT / LIB_RS).read_text(encoding="utf-8")
        real_fake = (REPO_ROOT / FAKE_CODEX).read_text(encoding="utf-8")
        assert CODEX_VERSION_RE.search(real_lib), "unable to match LAST_VALIDATED_CODEX_VERSION pattern in repo/src/lib.rs"
        assert FAKE_CLI_VERSION_RE.search(real_fake), "unable to match cliVersion pattern in repo/tests/fake_codex.py"
        code = None
        try:
            run_repin(
                json.loads((pins / "codex-lock.json").read_text(encoding="utf-8")),
                tag="rust-v9.8.7",
                dry_run=False,
                update_metadata=True,
                repo_root=root,
                release_fn=lambda *_args: {"tag_name": "rust-v9.8.7", "assets": [{"name": name, "digest": "sha256:" + "a" * 64}]},
                generate_schema_fn=lambda *_args: (_ for _ in ()).throw(
                    tarfile.TarError("malformed archive")
                ),
            )
            raise AssertionError("expected TarError from malformed schema archive")
        except tarfile.TarError:
            code = 3
        except Exception as error:
            raise AssertionError(f"expected TarError, got {type(error).__name__}: {error}")
        assert code == 3

    with tempfile.TemporaryDirectory(prefix=".codex-repin-selftest-", dir=Path(__file__).resolve().parent) as temp:
        root = Path(temp) / "repo"
        (root / "crates/provider-codex/src").mkdir(parents=True, exist_ok=True)
        (root / "crates/provider-codex/tests").mkdir(parents=True, exist_ok=True)
        lib_path = root / LIB_RS
        fake_path = root / FAKE_CODEX
        lib_path.write_text('const LAST_VALIDATED_CODEX_VERSION: &str = "9.8.6";\n', encoding="utf-8")
        fake_path.write_text('{"cliVersion": "9.8.6", "model": "fake-model"}\n', encoding="utf-8")

        call_count = 0

        def flaky_replace(source: Path, destination: Path) -> None:
            nonlocal call_count
            call_count += 1
            if call_count == 2:
                raise OSError("simulated metadata write failure")
            return source.replace(destination)

        try:
            update_last_validated_metadata(
                "9.9.0",
                repo_root=root,
                replace_fn=flaky_replace,
            )
            raise AssertionError("expected simulated metadata write failure")
        except OSError as error:
            assert str(error) == "simulated metadata write failure"

        assert call_count == 2
        assert lib_path.read_text(encoding="utf-8").strip() == 'const LAST_VALIDATED_CODEX_VERSION: &str = "9.8.6";'
        assert fake_path.read_text(encoding="utf-8").strip() == '{"cliVersion": "9.8.6", "model": "fake-model"}'

    with tempfile.TemporaryDirectory(prefix=".codex-repin-selftest-", dir=Path(__file__).resolve().parent) as temp:
        root = Path(temp) / "repo"
        pins = root / "pins"
        pins.mkdir(parents=True)
        schema_root = pins / "codex-app-server-schema"
        schema_root.mkdir()
        new_lock = pins / "codex-lock.json"
        (root / "crates/provider-codex/src").mkdir(parents=True, exist_ok=True)
        (root / "crates/provider-codex/tests").mkdir(parents=True, exist_ok=True)
        lock_contents = {
            "release_repo": "openai/codex",
            "codex_version": "9.8.6",
            "targets": {target: {"asset": name, "sha256": "a"*64}},
            "adapter_revision": 4,
        }
        new_lock.write_text(json.dumps(lock_contents, indent=2) + "\n", encoding="utf-8")
        lib_path = root / LIB_RS
        fake_path = root / FAKE_CODEX
        lib_path.write_text('const LAST_VALIDATED_CODEX_VERSION: &str = "9.8.6";\n', encoding="utf-8")
        fake_path.write_text('{"cliVersion": "9.8.6", "model": "fake-model"}\n', encoding="utf-8")

        rewrites = update_last_validated_metadata("9.9.0", repo_root=root)
        assert len(rewrites) == 2
        updated_lib = lib_path.read_text(encoding="utf-8")
        updated_fake = fake_path.read_text(encoding="utf-8")
        assert 'const LAST_VALIDATED_CODEX_VERSION: &str = "9.9.0";' in updated_lib
        assert '"cliVersion": "9.9.0"' in updated_fake

        lock_json = json.loads(new_lock.read_text(encoding="utf-8"))
        def write_fake_schema(_value: dict, _target: str, output: Path) -> dict:
            output.mkdir(parents=True, exist_ok=True)
            (output / "schema.json").write_text("{}", encoding="utf-8")
            return {"name": name, "digest": f"sha256:{'a' * 64}"}

        run_repin(
            lock_json,
            tag="rust-v9.8.7",
            dry_run=False,
            update_metadata=True,
            repo_root=root,
            release_fn=lambda *_args: {"tag_name": "rust-v9.8.7", "assets": [{"name": name, "digest": "sha256:" + "a" * 64}]},
            generate_schema_fn=write_fake_schema,
        )
        final_lock = json.loads((pins / "codex-lock.json").read_text(encoding="utf-8"))
        final_lib = (root / LIB_RS).read_text(encoding="utf-8")
        final_fake = (root / FAKE_CODEX).read_text(encoding="utf-8")
        assert final_lock["codex_version"] == "9.8.7"
        assert 'const LAST_VALIDATED_CODEX_VERSION: &str = "9.8.7";' in final_lib
        assert '"cliVersion": "9.8.7"' in final_fake
    print("ok: lock rewrite self-test passed")
    print("ok: malformed archive path raises TarError")
    print("ok: metadata rewrite self-test passed")


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

    try:
        repo_root = Path(__file__).resolve().parent.parent
        lock_path = repo_root / "pins" / "codex-lock.json"
        lock = json.loads(lock_path.read_text(encoding="utf-8"))
        return run_repin(
            lock,
            tag=args.tag,
            dry_run=args.dry_run,
            update_metadata=True,
        )
    except NetworkError as error:
        print(f"NETWORK-ERROR (retryable): {error}", file=sys.stderr)
        return 2
    except (KeyError, OSError, ValueError, subprocess.SubprocessError, tarfile.TarError) as error:
        print(f"ERROR: {error}", file=sys.stderr)
        return 3


if __name__ == "__main__":
    raise SystemExit(main())
