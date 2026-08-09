# YakShed backend specification bundle

> **Status:** implementation-ready architecture baseline  
> **Research snapshot:** 2026-08-09  
> **Primary runtime:** Rust backend in a Tauri desktop application  
> **First harness:** Codex App Server over JSONL stdio

This bundle turns the YakShed architecture discussion into versionable implementation contracts.
It is intended to be handed to an implementation agent and then kept in the repository as the
source of truth for boundaries that should not drift silently.

## Contents

```text
docs/
├── product/
│   └── gestalt.md
├── architecture/
│   ├── overall.md
│   └── sandboxing.md
├── standards/
│   ├── working-with-secrets.md
│   ├── working-with-state.md
│   └── backend-composition-and-testing.md
└── contracts/
    └── backend-contract-v1.md

.codereview/agents/
├── architecture/seams/
├── rust/implementation-tests/
├── security/secret-boundary/
└── tauri/config-ipc/

scripts/
├── backend_contract_test.py
└── fake_harness.py
```

## Reading order

If you are the orchestrating implementation agent, read `HANDOFF.md` first — it contains
phase-0 pinning work, hard gates, decisions already made, and known traps.

Start with `docs/product/gestalt.md`. It explains the product idea and should frame every more detailed decision below. It is a north-star brief rather than a wire-level contract.

When normative documents overlap, interpret them in this order:

1. `working-with-secrets.md` for access-secret ownership, storage, ingress, resolution, and delivery.
2. `working-with-state.md` for config, cache, durable data, artifacts, provider-owned state, and paths.
3. `sandboxing.md` for execution permissions and approval mediation.
4. `overall.md` for the larger product and harness architecture.
5. `backend-composition-and-testing.md` for workspace structure, dependency direction, and Definition of Done.
6. `backend-contract-v1.md` for the test-only JSONL acceptance harness.

The code-review agents encode selected invariants from those documents. The documents remain the
source of truth; prompts are enforcement aids rather than a second architecture specification.

## Core decisions

- A YakShed work item is not a provider session.
- Harnesses retain ownership of their agent loops, native sessions, delegated authentication,
  tool semantics, and sandbox implementation.
- YakShed owns the work graph, connection boundaries, working copies, notes, todos, artifacts,
  application persistence, process supervision, and Tauri surface.
- Credential authority is selected per credential requirement. A connection may be delegated,
  secret-backed, or hybrid.
- Secret values never become config, SQLite state, frontend state, event payloads, logs, or command-line arguments.
- Config, cache, durable data, provider-owned state, runtime files, and secrets have distinct owners and deletion lifecycles.
- Tauri is an outer adapter. Domain and infrastructure modules do not gain IPC merely to make them testable.
- Every module is delivered with unit tests and at least one boundary-level integration test.
- A dedicated test-only contract host composes the real Rust modules with memory secrets and a fake harness;
  the Python runner drives that host over JSONL without creating a production API.

## Suggested implementation order

1. Establish the Rust workspace, `AppPaths`, config loader, SQLite actor, and migration test harness.
2. Implement `yakshed-secrets` with memory and local OS backends, then the broker and redaction tests.
3. Implement the provider-neutral harness interface and deterministic mock harness.
4. Implement the test-only contract host and make `scripts/backend_contract_test.py` pass.
5. Add the Codex App Server transport and protocol reducer behind the harness interface.
6. Add the plain Rust desktop facade, then thin Tauri commands/events around that facade.
7. Add native keyring, 1Password, packaging, and platform-specific CI lanes.

A module is not complete because its happy path compiles. Its error taxonomy, lifecycle behavior,
recovery behavior, and tests are part of the deliverable.
