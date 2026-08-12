#!/usr/bin/env python3
"""Verify YakShed's small agent front door and sensitive reviewer routing."""

from __future__ import annotations

import argparse
from fnmatch import fnmatchcase
from pathlib import Path
import re
import sys
import tempfile
from urllib.parse import unquote, urlsplit


MAX_AGENTS_LINES = 150
LINK_FILES = ("AGENTS.md", "README.md")
LINK_PATTERN = re.compile(r"\[[^\]]+\]\(([^)\n]+)\)")

# These are deliberate, repo-specific contracts, not a general YAML/glob linter.
# Broad ownership reviews cover their actual crates; secret review stays limited
# to files that currently compose credentials or cross the auth/IPC boundary.
REVIEWER_CONTRACTS = {
    "architecture/seams": {
        "index": ".codereview/agents/architecture/seams/index.yaml",
        "required_globs": (
            "crates/yakshed-desktop/**",
            "crates/yakshed-tauri/**",
        ),
        "sentinels": (
            "crates/yakshed-desktop/src/macos.rs",
            "crates/yakshed-tauri/src/lib.rs",
        ),
    },
    "tauri/config-ipc": {
        "index": ".codereview/agents/tauri/config-ipc/index.yaml",
        "required_globs": (
            "crates/yakshed-desktop/tauri.conf.json",
            "crates/yakshed-tauri/Cargo.toml",
            "crates/yakshed-tauri/build.rs",
            "crates/yakshed-tauri/src/lib.rs",
            "crates/yakshed-tauri/src/macos.rs",
            "crates/yakshed-tauri/src/roster.rs",
            "crates/yakshed-tauri/src/frontend/client.ts",
            "crates/yakshed-tauri/tests/**",
            "crates/yakshed-desktop-api/**",
        ),
        "sentinels": (
            "crates/yakshed-desktop/tauri.conf.json",
            "crates/yakshed-tauri/src/lib.rs",
            "crates/yakshed-desktop-api/src/lib.rs",
        ),
    },
    "security/secret-boundary": {
        "index": ".codereview/agents/security/secret-boundary/index.yaml",
        "required_globs": (
            "crates/yakshed-desktop/src/macos.rs",
            "crates/yakshed-tauri/src/macos.rs",
            "crates/yakshed-tauri/src/frontend/client.ts",
            "crates/yakshed-tauri/tests/ipc.rs",
        ),
        "sentinels": (
            "crates/yakshed-desktop/src/macos.rs",
            "crates/yakshed-tauri/src/macos.rs",
            "crates/yakshed-tauri/src/frontend/client.ts",
            "crates/yakshed-tauri/tests/ipc.rs",
        ),
    },
}


class GuidanceError(Exception):
    """Actionable repository-guidance verification failures."""


def _read_text(root: Path, relative: str, errors: list[str]) -> str | None:
    path = root / relative
    try:
        return path.read_text(encoding="utf-8")
    except FileNotFoundError:
        errors.append(f"missing {relative}; restore it before running an agent")
    except UnicodeDecodeError as error:
        errors.append(f"{relative} is not UTF-8 text: {error}")
    except OSError as error:
        errors.append(f"cannot read {relative}: {error}")
    return None


def _link_target(raw: str) -> str:
    raw = raw.strip()
    if raw.startswith("<") and ">" in raw:
        return raw[1 : raw.index(">")]
    return raw.split()[0] if raw else ""


def _validate_markdown_links(root: Path, errors: list[str]) -> None:
    for relative in LINK_FILES:
        text = _read_text(root, relative, errors)
        if text is None:
            continue
        source = root / relative
        for match in LINK_PATTERN.finditer(text):
            target = _link_target(match.group(1))
            line = text.count("\n", 0, match.start()) + 1
            if not target or target.startswith("#"):
                continue
            parsed = urlsplit(target)
            if parsed.scheme or parsed.netloc:
                continue
            local = unquote(target.split("#", 1)[0].split("?", 1)[0])
            if not local:
                continue
            if Path(local).is_absolute():
                errors.append(f"{relative}:{line} uses an absolute link {target!r}; use a repo-relative path")
                continue
            candidate = (source.parent / local).resolve()
            try:
                candidate.relative_to(root.resolve())
            except ValueError:
                errors.append(
                    f"{relative}:{line} points outside the repository with {target!r}; "
                    "use a path inside the repository"
                )
                continue
            if not candidate.exists():
                errors.append(
                    f"{relative}:{line} points to missing local path {target!r}; "
                    "fix the link or remove it"
                )
            elif local.endswith("/") and not candidate.is_dir():
                errors.append(
                    f"{relative}:{line} marks {target!r} as a directory, but it is not one"
                )


def _parse_file_globs(text: str) -> list[str]:
    patterns: list[str] = []
    in_file_globs = False
    for line in text.splitlines():
        if line.strip() == "file_globs:":
            in_file_globs = True
            continue
        if not in_file_globs:
            continue
        if line.startswith("  - "):
            value = line[4:].strip()
            if len(value) >= 2 and value[0] == value[-1] and value[0] in "'\"":
                value = value[1:-1]
            patterns.append(value)
            continue
        if line and not line.startswith(" "):
            in_file_globs = False
    return patterns


def _validate_reviewer_coverage(root: Path, errors: list[str]) -> None:
    for reviewer, contract in REVIEWER_CONTRACTS.items():
        index = contract["index"]
        text = _read_text(root, index, errors)
        if text is None:
            continue
        patterns = _parse_file_globs(text)
        if not patterns:
            errors.append(f"{index} has no parseable file_globs for {reviewer}")
            continue
        for pattern in patterns:
            if "src-tauri" in pattern:
                errors.append(
                    f"{index} contains obsolete src-tauri pattern {pattern!r}; "
                    "use the actual crates/yakshed-* paths"
                )
        for required in contract["required_globs"]:
            if required not in patterns:
                errors.append(
                    f"{reviewer} index is missing required reviewer glob {required!r}; "
                    f"add it to {index}"
                )
        for sentinel in contract["sentinels"]:
            if not (root / sentinel).is_file():
                errors.append(
                    f"reviewer sentinel {sentinel} is missing; update the fixed coverage contract "
                    "if the repository layout intentionally changes"
                )
                continue
            if not any(fnmatchcase(sentinel, pattern) for pattern in patterns):
                errors.append(
                    f"{reviewer} does not cover sentinel {sentinel}; "
                    f"add a matching glob to {index}"
                )


def verify(root: Path) -> None:
    """Raise GuidanceError when the repository guidance contract is broken."""

    errors: list[str] = []
    claude = _read_text(root, "CLAUDE.md", errors)
    if claude is not None and claude != "@AGENTS.md\n":
        errors.append("CLAUDE.md must contain exactly '@AGENTS.md' followed by one newline")

    agents = _read_text(root, "AGENTS.md", errors)
    if agents is not None:
        line_count = len(agents.splitlines())
        if line_count > MAX_AGENTS_LINES:
            errors.append(
                f"AGENTS.md has {line_count} lines; keep the index at {MAX_AGENTS_LINES} lines or fewer"
            )

    _validate_markdown_links(root, errors)
    _validate_reviewer_coverage(root, errors)
    if errors:
        raise GuidanceError("\n".join(f"- {error}" for error in errors))


def _write_fixture(root: Path) -> None:
    root.mkdir(parents=True, exist_ok=True)
    (root / "docs").mkdir(exist_ok=True)
    (root / "docs/guide.md").write_text("# Guide\n", encoding="utf-8")
    (root / "AGENTS.md").write_text(
        "# Agent index\n\n[docs](docs/) [guide](docs/guide.md) "
        "[external](https://example.com) [fragment](#local)\n",
        encoding="utf-8",
    )
    (root / "README.md").write_text(
        "[guide](docs/guide.md) [external](https://example.com) [fragment](#local)\n",
        encoding="utf-8",
    )
    (root / "CLAUDE.md").write_text("@AGENTS.md\n", encoding="utf-8")
    for contract in REVIEWER_CONTRACTS.values():
        for sentinel in contract["sentinels"]:
            path = root / sentinel
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text("fixture\n", encoding="utf-8")
    for contract in REVIEWER_CONTRACTS.values():
        index = root / contract["index"]
        index.parent.mkdir(parents=True, exist_ok=True)
        lines = ["name: fixture", "file_globs:"]
        lines.extend(f'  - "{pattern}"' for pattern in contract["required_globs"])
        index.write_text("\n".join(lines) + "\n", encoding="utf-8")


def _expect_pass(root: Path, label: str) -> None:
    try:
        verify(root)
    except GuidanceError as error:
        raise AssertionError(f"self-test {label} should pass:\n{error}") from error


def _expect_failure(root: Path, label: str, needle: str, mutate) -> None:
    mutate(root)
    try:
        verify(root)
    except GuidanceError as error:
        if needle not in str(error):
            raise AssertionError(
                f"self-test {label} failed for the wrong reason:\n{error}"
            ) from error
    else:
        raise AssertionError(f"self-test {label} unexpectedly passed")


def self_test() -> None:
    """Exercise every nontrivial rule against isolated temporary fixtures."""

    cases = (
        (
            "exact CLAUDE pointer",
            "CLAUDE.md must contain exactly",
            lambda root: (root / "CLAUDE.md").write_text("wrong\n", encoding="utf-8"),
        ),
        (
            "AGENTS line limit",
            "AGENTS.md has",
            lambda root: (root / "AGENTS.md").write_text("line\n" * 151, encoding="utf-8"),
        ),
        (
            "Markdown link resolution",
            "README.md:1 points to missing local path",
            lambda root: (root / "README.md").write_text("[missing](missing/)\n", encoding="utf-8"),
        ),
        (
            "reviewer sentinel coverage",
            "architecture/seams index is missing required reviewer glob",
            lambda root: _remove_pattern(
                root,
                REVIEWER_CONTRACTS["architecture/seams"]["index"],
                "crates/yakshed-desktop/**",
            ),
        ),
        (
            "obsolete reviewer path",
            "contains obsolete src-tauri pattern",
            lambda root: _append_pattern(
                root,
                REVIEWER_CONTRACTS["tauri/config-ipc"]["index"],
                "src-tauri/**",
            ),
        ),
    )
    with tempfile.TemporaryDirectory(prefix="yakshed-guidance-self-test-") as temporary:
        base = Path(temporary)
        valid = base / "valid"
        _write_fixture(valid)
        _expect_pass(valid, "valid fixture")
        for name, needle, mutate in cases:
            case = base / name.replace(" ", "-")
            _write_fixture(case)
            _expect_failure(case, name, needle, mutate)
    print("PASS agent-guidance verifier self-test")


def _remove_pattern(root: Path, relative: str, pattern: str) -> None:
    path = root / relative
    lines = [line for line in path.read_text(encoding="utf-8").splitlines() if pattern not in line]
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def _append_pattern(root: Path, relative: str, pattern: str) -> None:
    path = root / relative
    with path.open("a", encoding="utf-8") as stream:
        stream.write(f'  - "{pattern}"\n')


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, help="repository root (default: parent of scripts/)")
    parser.add_argument("--self-test", action="store_true", help="run hermetic pass/fail fixtures")
    args = parser.parse_args(argv)
    if args.self_test:
        try:
            self_test()
        except AssertionError as error:
            print(f"FAIL agent-guidance verifier self-test: {error}", file=sys.stderr)
            return 1
        return 0
    root = (args.root or Path(__file__).resolve().parents[1]).resolve()
    try:
        verify(root)
    except GuidanceError as error:
        print(f"FAIL agent guidance in {root}:\n{error}", file=sys.stderr)
        return 1
    print(f"PASS agent guidance: {root}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
