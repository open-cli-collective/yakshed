# YakShed

YakShed is a local-first macOS desktop workbench for organizing, supervising,
and resuming software work performed with coding-agent harnesses. It is built
with Tauri, Svelte, and Rust. Codex App Server is the first harness boundary;
the product model is the work item, not the provider conversation.

## Current state

The repository contains a working vertical slice and its supporting contracts:

- Rust workspace layers for domain state, application use cases, persistence,
  secrets, harnesses, the Codex adapter, the desktop API, and the Tauri shell.
- Durable config, SQLite state, cache/artifact storage, revisioned snapshots,
  and restart-safe state boundaries under injected `AppPaths`.
- Explicit delegated or secret-backed credential bindings, a macOS Keychain
  backend, development/test stores, narrow write-only credential ingress, and
  canary-based redaction tests.
- Codex JSONL transport/reduction with approvals, user input, steering,
  interruption, process cleanup, unknown-event handling, and a generated schema
  pin. The last validated Codex release is 0.147.0; it is metadata, not a
  runtime version gate.
- A Svelte/Tauri surface for work-item creation, Codex connection setup and
  sign-in, run controls, timeline updates, approvals, user input, and outcome
  reconciliation.
- A deterministic mock harness, fake Codex process, test-only JSONL contract
  host, Rust integration tests, and Playwright UI coverage.

This is an implementation workbench, not yet the complete product described by
the north star. The current shell does not expose every planned work-graph,
working-copy, Reader, multi-harness, or remote-runtime workflow.

## Product direction

The product is heading toward a durable work graph in which yaks can branch,
pause, depend on one another, move between harnesses, and be resumed without
reconstructing context from terminal scrollback. Planned direction includes
first-class notes, todos, labels, worktrees, artifacts and Reader views,
provider-neutral second-harness support, richer permission/runtime controls,
and longer-lived background supervision. See the
[product gestalt](docs/product/gestalt.md) and
[overall architecture](docs/architecture/overall.md) for the intended scope.

## Supported environment and prerequisites

- macOS is the supported desktop target, including native Keychain use and
  packaged-app smoke checks. The Rust backend, contract host, and web checks
  can run without opening the desktop shell.
- Rust 1.96.0 is pinned in [`rust-toolchain.toml`](rust-toolchain.toml); use
  the committed [`Cargo.lock`](Cargo.lock).
- Node.js 22 and npm are required for the Svelte/Tauri toolchain;
  [`package.json`](crates/yakshed-tauri/package.json) records npm 11.19.0 as
  the package-manager reference. `npm ci` installs the locked UI dependencies.
- Python 3 is required for the standard-library contract and packaging helper
  scripts.
- Live Codex runs require a `codex` executable on `PATH` and a Codex App Server
  login. Contract, mock, Rust, and Playwright checks do not require a real
  provider credential.

## Quick start

Run the deterministic backend checks from the repository root:

```sh
cargo test --workspace --locked
cargo build -p yakshed-contract-host --locked
python3 scripts/backend_contract_test.py \
  --host target/debug/yakshed-contract-host \
  --fake-harness scripts/fake_harness.py
```

Build and exercise the web surface:

```sh
cd crates/yakshed-tauri
npm ci
npm run typecheck
npm run build
npm run test:e2e
```

On macOS, start the desktop shell after installing the UI dependencies:

```sh
npm run tauri -- dev
```

For a packaged app and clean launch/quit check, use
[`docs/runbooks/tauri-packaging.md`](docs/runbooks/tauri-packaging.md).

## Verification

The practical full workspace lane is:

```sh
cargo fmt --all -- --check
cargo clippy --workspace --all-targets --locked -- -D warnings
cargo test --workspace --locked
python3 scripts/verify_schema_pin.py
python3 scripts/verify_codex_metadata.py
```

Run the frontend checks when the UI changes, and the macOS package smoke when
packaging, startup, or process lifecycle changes. Codex drift is checked by
the scheduled/manual procedure in
[`codex-tracking.md`](docs/standards/codex-tracking.md) rather than by ordinary
hermetic checks.

## Repository knowledge store

Start with [`AGENTS.md`](AGENTS.md), then use the source-of-truth documents:

- [Harness-engineering standard](docs/standards/harness-engineering.md)
- [Product gestalt](docs/product/gestalt.md)
- [Overall architecture](docs/architecture/overall.md)
- [Sandboxing and approvals](docs/architecture/sandboxing.md)
- [Backend composition and testing](docs/standards/backend-composition-and-testing.md)
- [Working with secrets](docs/standards/working-with-secrets.md)
- [Working with state](docs/standards/working-with-state.md)
- [Backend contract v1](docs/contracts/backend-contract-v1.md)
- [Codex phase-0 verification](pins/phase0-verification.md) and [lock record](pins/codex-lock.json)
- [Tauri success criteria](docs/standards/tauri-success-criteria.md)
- [Credentials and packaging criteria](docs/standards/credentials-packaging-criteria.md)
- [Codex authentication](docs/runbooks/codex-auth.md), [packaging](docs/runbooks/tauri-packaging.md), and [release signing](docs/runbooks/release-signing.md)
- [Repo-local review agents](.codereview/agents/README.md)
