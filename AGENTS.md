# YakShed agent index

This is the short entry point for work in YakShed. Use it as a map, not as a
second architecture manual. Read the task-relevant source of truth before
editing, inspect the existing implementation and tests, and keep changes
proportionate to this local-first macOS desktop application.

## Source of truth and precedence

The repository knowledge store is authoritative. When implementation documents
conflict, use this order:

1. [`working-with-secrets.md`](docs/standards/working-with-secrets.md)
2. [`working-with-state.md`](docs/standards/working-with-state.md)
3. [`sandboxing.md`](docs/architecture/sandboxing.md)
4. [`overall.md`](docs/architecture/overall.md)
5. [`backend-composition-and-testing.md`](docs/standards/backend-composition-and-testing.md)
6. [`backend-contract-v1.md`](docs/contracts/backend-contract-v1.md)

[`harness-engineering.md`](docs/standards/harness-engineering.md) governs the
agent-first working method and chapter gates; it does not override those
technical standards. Phase criteria and runbooks apply within their scope.

## Task map

| Task | Start with |
| --- | --- |
| Product intent or UI behavior | [`gestalt.md`](docs/product/gestalt.md) |
| Ownership, layers, harness adapters | [`overall.md`](docs/architecture/overall.md) |
| Permissions, approvals, execution safety | [`sandboxing.md`](docs/architecture/sandboxing.md) |
| Config, SQLite, artifacts, paths, reset | [`working-with-state.md`](docs/standards/working-with-state.md) |
| Credential ingress, storage, delivery | [`working-with-secrets.md`](docs/standards/working-with-secrets.md) |
| Rust composition and tests | [`backend-composition-and-testing.md`](docs/standards/backend-composition-and-testing.md) |
| JSONL contract or fake host | [`backend-contract-v1.md`](docs/contracts/backend-contract-v1.md) |
| Codex pins or drift | [`codex-tracking.md`](docs/standards/codex-tracking.md) and [`phase0-verification.md`](pins/phase0-verification.md) |
| Tauri, packaging, or release | [`tauri-success-criteria.md`](docs/standards/tauri-success-criteria.md) and [`tauri-packaging.md`](docs/runbooks/tauri-packaging.md) |
| Credential or release packaging | [`credentials-packaging-criteria.md`](docs/standards/credentials-packaging-criteria.md) and [`release-signing.md`](docs/runbooks/release-signing.md) |

## Essential invariants

- A YakShed work item is not a provider session. YakShed owns work, state,
  artifacts, process supervision, and product IPC; harnesses own loops,
  provider sessions, delegated auth, tools, and their sandbox.
- Every durable concept has one owner and one source of truth. Secrets,
  config, cache, data, provider state, runtime files, and working-copy state
  have separate lifecycles.
- Secret values never enter config, SQLite, cache, artifacts, frontend state,
  events, logs, URLs, or argv. Delegated provider credentials stay delegated.
- Tauri is an outer adapter. Expose product operations with validated IDs and
  DTOs, never generic shell, filesystem, SQL, process, secret, or provider-RPC
  commands.
- The WebView is a projection. Events are revisioned hints; snapshots recover
  missed events and reloads. Provider readers do not block on UI decisions.
- Validate at boundaries, use injected temporary paths in tests, preserve
  uncertain outcomes, and fail closed when required isolation is unavailable.

## Checks

For a docs-only change, run `git diff --check` and verify every new relative
Markdown link resolves. For implementation changes, use the smallest relevant
fast checks first:

```text
cargo fmt --all -- --check
cargo test -p <changed-crate> --locked
python3 scripts/verify_schema_pin.py       # only when pins/schema are touched
python3 scripts/verify_codex_metadata.py   # only when Codex metadata is touched
```

The full cheap lane is:

```text
cargo clippy --workspace --all-targets --locked -- -D warnings
cargo test --workspace --locked
cargo build -p yakshed-contract-host --locked
python3 scripts/backend_contract_test.py \
  --host target/debug/yakshed-contract-host \
  --fake-harness scripts/fake_harness.py
```

For frontend changes also run `npm ci`, `npm run typecheck`, `npm run build`,
and `npm run test:e2e` from `crates/yakshed-tauri`. For packaged macOS changes,
run `npm run package` there, then `python3 scripts/tauri_app_smoke.py` from the
repository root as documented in the packaging runbook.

## UI validation and review routing

Use [`gestalt.md`](docs/product/gestalt.md) and the files under
`design/YakShed-UI/` for intent and visual reference; validate behavior against
the built Svelte surface, then use the packaged macOS smoke when the desktop
shell or lifecycle is in scope. Route changed boundaries to the repo-local
reviewers: Rust behavior/tests to `rust/implementation-tests`, ownership or
standards to `architecture/seams`, Tauri/IPC/CSP/frontend DTOs to
`tauri/config-ipc`, and credential paths to `security/secret-boundary`.

Those reviewers are scoped by globs, not omniscient. Invoke the secret reviewer
explicitly when a credential path changes without a matching filename, and do
not treat a passing unit test as a substitute for the applicable boundary or
UI check.
