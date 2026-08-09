# YakShed: Backend Composition and Testing

> **Status:** normative implementation handoff  
> **Research snapshot:** 2026-08-09  
> **Purpose:** turn the architectural boundaries into independently testable Rust deliverables without making Tauri IPC the internal module system

## 1. Decision

YakShed's backend should be composed from ordinary Rust modules and crates with explicit constructor wiring.
Tauri is the outer desktop adapter. It is not the dependency-injection framework, state owner, secret broker,
process supervisor, or test harness.

```text
Svelte / TypeScript
        │
        │ Tauri commands + revisioned events
        ▼
Thin Tauri adapter
        │
        ▼
Plain Rust desktop facade
        │
        ▼
Application use cases
        │
   ┌────┼────────────┬──────────────┐
   ▼    ▼            ▼              ▼
Store  Credentials  Harnesses      Git/workspaces
   │       │            │              │
SQLite  keyrings/op   Codex/mock      system Git
```

A module is independently testable because its dependencies are explicit and replaceable at legitimate seams,
not because every function is remotely callable.

---

## 2. Architectural rules

### 2.1 Tauri is an adapter

Only the Tauri layer may depend on Tauri types, macros, application handles, event APIs, capability files,
window identities, or plugin APIs.

Core crates MUST NOT accept:

- `tauri::AppHandle`;
- `tauri::State`;
- `tauri::Window` / `WebviewWindow`;
- frontend event names;
- JSON values merely because IPC uses JSON;
- raw filesystem paths supplied by JavaScript when an app ID should be used.

### 2.2 Interfaces exist at real seams

Create a trait or port when at least one of these is true:

- an external implementation already varies: Codex versus Claude, OS keyring versus 1Password;
- the boundary is process/network/disk I/O and needs a deterministic fake;
- ownership must be inverted to preserve dependency direction;
- substituting the dependency is materially cheaper than running it in most tests;
- the contract is independently valuable to more than one consumer.

Do not create a trait merely because a type has methods or because mocking is fashionable.
A private concrete type plus a focused fake constructor is often enough.

### 2.3 Consumer-owned contracts

The layer that needs a behavior should define the smallest behavior it needs. Provider- or library-specific
APIs are translated at the adapter boundary.

```text
Application needs: start run, interrupt run, resolve approval
Codex exposes:     turn/start, turn/interrupt, JSON-RPC response
Adapter translates; application never imports Codex DTOs
```

### 2.4 One owner per lifecycle

Each process, connection, provider session, work item, approval, database connection, config file, and secret reference
has one owning component. Ownership includes startup, mutation, recovery, and shutdown.

### 2.5 Modules may be independently testable without being crates

Use a crate boundary when it enforces dependency direction, permits reuse by a future daemon/CLI, or isolates a major
external dependency. Use modules for internal organization when another crate would add ceremony without protection.

---

## 3. Initial workspace shape

This is a starting point, not a requirement to create every crate before code exists.

```text
Cargo.toml
crates/
├── yakshed-domain/
│   ├── ids, work graph, connection model, artifacts, approvals
│   └── no I/O and no Tauri
│
├── yakshed-application/
│   ├── use cases and orchestration
│   ├── snapshots/revisions
│   └── application-level ports
│
├── yakshed-store/
│   ├── AppPaths
│   ├── config implementation
│   ├── SQLite actor + migrations
│   └── artifact store
│
├── yakshed-secrets/
│   ├── broker and public contracts
│   ├── memory backend
│   ├── local OS backend
│   ├── 1Password backend
│   └── helper/environment adapters
│
├── yakshed-harness/
│   ├── provider-neutral harness contract
│   ├── runtime/process abstractions
│   └── normalized events/capabilities
│
├── provider-codex/
│   ├── App Server transport
│   ├── generated protocol types
│   ├── reducer
│   └── Codex adapter
│
├── provider-mock/
│   ├── deterministic scripted harness
│   └── fault injection
│
└── yakshed-desktop-api/
    ├── frontend-safe DTOs
    ├── plain Rust command facade
    └── event/snapshot mapping

src-tauri/
├── Tauri application assembly
├── #[tauri::command] wrappers
├── capabilities and permissions
└── sidecar packaging

tools/
└── yakshed-contract-host/
    └── test-only JSONL composition host

scripts/
├── backend_contract_test.py
└── fake_harness.py
```

Potential consolidation is acceptable:

- `yakshed-domain` and `yakshed-application` may begin as one crate if dependency direction remains clear.
- runtime supervision may begin inside `yakshed-harness`.
- `yakshed-desktop-api` may initially be a plain Rust module under `src-tauri`, provided it does not import Tauri.

Do not split an implementation into `*-api`, `*-core`, `*-impl`, and `*-types` crates until a concrete dependency problem exists.

---

## 4. Dependency direction

```text
yakshed-domain
      ▲
      │
yakshed-application
      ▲
      ├──────── yakshed-store
      ├──────── yakshed-secrets
      ├──────── yakshed-harness ◄──── provider-codex / provider-mock
      └──────── git/workspace adapter
      ▲
      │
yakshed-desktop-api
      ▲
      │
src-tauri
```

Infrastructure crates may depend on domain types where useful. The domain does not import infrastructure types.
The application does not import Tauri or provider wire DTOs.

Avoid a generic service locator. Construct a typed service graph at startup.

```rust
pub struct AppServices {
    pub app: std::sync::Arc<ApplicationService>,
    pub store: std::sync::Arc<dyn AppStore>,
    pub credentials: std::sync::Arc<CredentialBroker>,
    pub runtimes: std::sync::Arc<RuntimeBroker>,
}
```

The exact fields may differ, but dependency construction should be visible and testable.

---

## 5. Domain and application boundary

### Domain

The domain contains values and invariants that do not perform I/O:

- IDs and opaque provider IDs;
- work items and graph edges;
- connection and credential-binding models;
- run, approval, artifact, and lifecycle states;
- capability and policy decisions;
- pure state transitions.

Illegal states should be difficult to construct, but avoid type-level machinery that makes routine evolution painful.
Closed enums are preferable when the set is product-owned; opaque strings are preferable for provider-owned identifiers.

### Application

The application layer owns use cases:

```text
create_work_item
spawn_child_work
start_run
send_message
steer_run
interrupt_run
resolve_approval
publish_artifact
archive_work_subtree
clear_connection_credential
```

Use cases validate authorization/scope, coordinate dependencies, perform transactions, and emit application events.
They do not know Tauri command names or Codex RPC method names.

---

## 6. Infrastructure module contracts

Each infrastructure module should expose:

1. a small public API;
2. a typed error model;
3. one production implementation;
4. one deterministic test implementation or fixture where replacement is useful;
5. lifecycle methods where resources need startup/shutdown;
6. unit tests for pure policy and edge cases;
7. integration tests for its real boundary.

### 6.1 Store module

Public behavior is application-shaped. Tests use real SQLite and temporary files.
The application never sees a `rusqlite::Connection`.

### 6.2 Secrets module

The broker and resolver contracts are public; backend-library types are private.
Tests use the YakShed memory store. Native keyring tests are a separate platform lane.

### 6.3 Harness module

The provider-neutral contract exposes capabilities and normalized events. The mock harness can:

- stream messages in arbitrary chunks;
- request approvals;
- emit file/command events;
- crash mid-run;
- delay or reorder legal events;
- return unknown native items;
- simulate overloaded or disconnected runtimes.

### 6.4 Codex adapter

The JSONL transport, RPC correlation, schema types, reducer, and application adapter are separable modules.
Transport tests use a fake `codex` executable or in-memory duplex streams; reducer tests replay golden protocol traces.

### 6.5 Desktop facade

The facade accepts frontend-safe DTOs, calls application use cases, and returns snapshots/errors.
It has no Tauri dependency, so command behavior is unit/integration tested directly.
Tauri wrappers should be nearly mechanical.

---

## 7. Tauri IPC

IPC is required only where the WebView needs to invoke or observe an application use case.
It is not a generic internal bus.

### Good commands

```text
create_work_item
get_work_item_snapshot
start_run
send_message
resolve_approval
set_connection_credential
open_artifact
clear_cache
```

### Bad commands

```text
run_shell(command)
read_file(path)
write_file(path, bytes)
query_sql(sql)
resolve_secret(reference)
call_codex(method, params)
spawn_process(path, args)
```

Rules:

- commands accept app IDs and validated operation DTOs;
- raw paths are accepted only by a narrow, explicit file-picker/import flow and canonicalized in Rust;
- secret ingress is write-only;
- commands never return implementation handles;
- errors are mapped to stable frontend codes with redacted messages;
- events are revisioned and recoverable through snapshot fetches;
- large data is paginated or streamed through a bounded artifact API;
- no module gains Tauri macros to simplify a test.

### Testing command wrappers

Most behavior is tested against the plain Rust desktop facade. Add focused Tauri tests for:

- command registration and capability assignment;
- DTO deserialization and input rejection;
- window-specific permission boundaries;
- event mapping;
- CSP/updater configuration;
- absence of prohibited commands.

---

## 8. Test strategy

### 8.1 Unit tests

Unit tests prove local policy and transformations:

- domain state transitions;
- config migration functions;
- secret locator validation and error mapping;
- protocol envelope classification;
- event normalization;
- path policy;
- artifact digest/path mapping;
- capability selection;
- retry eligibility.

A unit test should fail if the relevant implementation is removed or reversed. Tests that merely restate constructors are not enough.

### 8.2 Crate integration tests

Integration tests exercise real boundaries under temporary roots:

- config atomic write/read/migrate;
- SQLite migrations and transactions;
- artifact staging/publish/recovery;
- memory secret broker and delivery;
- mock harness orchestration;
- application use cases across store + credentials + harness;
- desktop facade DTO mapping.

### 8.3 Provider contract tests

The Codex adapter gets deterministic protocol tests with a fake executable that can:

- split one JSON message across writes;
- write multiple frames quickly;
- flood stderr;
- issue server-initiated requests;
- produce unknown events;
- return overload errors;
- exit before/after mutating request acknowledgement;
- emit malformed or oversized frames.

Golden traces test reducers independently from process I/O.

### 8.4 External composition smoke test

A dedicated Rust binary, `yakshed-contract-host`, composes:

- production `AppPaths` behavior redirected to a temporary root;
- production config implementation;
- production SQLite implementation and migrations;
- production artifact implementation where covered;
- production credential broker with memory backend;
- production application/desktop facade;
- mock harness and real process spawner for a fake child harness.

It exposes a **test-only JSONL protocol over stdio**. This is not linked into the shipping desktop application and is not Tauri IPC.

`scripts/backend_contract_test.py` drives it with Python's standard library. This gives CI and local development one cheap,
language-external acceptance test that catches composition mistakes Rust unit tests can accidentally share.

See `../contracts/backend-contract-v1.md`.

### 8.5 Native/platform tests

Run on native OS runners:

- Keychain/Credential Manager/Secret Service;
- Tauri packaging and sidecar execution;
- process-group and Windows Job Object cleanup;
- path encoding and non-ASCII paths;
- file permissions/ACL expectations;
- updater/signing configuration where credentials are available.

### 8.6 Live provider tests

Credential-gated and non-blocking for ordinary pull requests:

- Codex initialize/account/model/thread smoke;
- one small turn and interruption;
- provider login/status compatibility;
- optional 1Password/account integration.

Most correctness must not depend on live model calls.

---

## 9. Cheap CI lane

The default pull-request lane should be deterministic and require no desktop keyring, network, or model credential.

```text
cargo fmt --check
cargo clippy --workspace --all-targets --locked -- -D warnings
cargo test --workspace --locked
cargo run -p yakshed-contract-host -- --version   # build/probe
python3 scripts/backend_contract_test.py \
  --host target/debug/yakshed-contract-host \
  --fake-harness scripts/fake_harness.py
```

The exact commands may evolve. The invariant is that the cheap lane exercises the composed backend with memory secrets and mock providers.

---

## 10. Test-only contract host

The host exists because external acceptance testing is valuable; it must not become a second production API.

Requirements:

- compiled as a separate binary/package;
- clearly named test/contract host;
- no listener socket or network transport;
- JSONL stdin/stdout only;
- temporary root required unless an explicit test flag is supplied;
- memory secret backend required by default;
- mock harness required by default;
- operations limited to the contract document;
- responses never contain secret values;
- stderr is diagnostic only and bounded by the Python runner;
- protocol version negotiated with `hello`;
- no code path enables this protocol in the Tauri binary.

The host should use the same constructors the desktop uses wherever possible. A separate hand-built fake composition would miss wiring regressions.

---

## 11. Deterministic test dependencies

Inject only dependencies whose nondeterminism or external effects matter:

```rust
pub trait Clock {
    fn now(&self) -> time::OffsetDateTime;
}

pub trait IdGenerator {
    fn next_work_item_id(&self) -> WorkItemId;
}
```

Likely explicit seams:

- `AppPaths`;
- clock and ID generator;
- `AppStore` / database actor handle;
- `CredentialBroker` / memory backend;
- `HarnessAdapter` / mock harness;
- process spawner for provider and helper tests;
- filesystem watcher abstraction if needed.

Do not create traits around pure helper functions or stable value types solely for dependency injection.

---

## 12. Fault injection

The mock/fake implementations should make failures first-class rather than relying on sleeps and race luck.

Examples:

```rust
pub enum MockHarnessFault {
    ExitAfterRunAccepted,
    ExitAfterFileMutation,
    DelayApproval(std::time::Duration),
    EmitUnknownEvent,
    EmitMalformedNativePayload,
    NeverComplete,
}

pub enum MemorySecretFault {
    NotFound,
    LockedOrDenied,
    Timeout,
    FailNextWrite,
    UncertainWrite,
}
```

Fault plans are scoped to a test instance and consumed deterministically.

---

## 13. Definition of Done for a module/deliverable

A deliverable is complete only when all applicable items are true:

### Contract

- responsibility and owner are documented;
- public API is narrow and named in domain/application language;
- dependency direction is correct;
- errors are typed and actionable;
- lifecycle and cancellation behavior are explicit;
- no unbounded input/output path exists.

### Unit tests

- success behavior is proven;
- meaningful error paths are proven;
- edge cases and state transitions are proven;
- tests are deterministic and hermetic;
- canary secrets do not appear in captured logs/output where applicable.

### Integration tests

- the real external boundary is exercised when cheap and deterministic;
- otherwise a faithful fake plus a separate native/live lane exists;
- temporary `AppPaths` isolate disk state;
- cleanup is verified;
- restart/recovery behavior is covered for durable modules.

### Composition

- the module is wired through the same constructor graph used by the app;
- the contract host covers its critical path when appropriate;
- no production IPC was added solely to make the module testable;
- Tauri exposure exists only for actual user-facing use cases.

### Review

- Rust implementation reviewer passes;
- Tauri reviewer passes when IPC/config is touched;
- secret reviewer passes when credential paths are touched;
- architecture reviewer passes when ownership, persistence, or abstraction changes.

### Documentation

- source-of-truth standard is updated when behavior changes;
- migration/compatibility implications are recorded;
- unsupported platform/backend behavior is explicit.

---

## 14. YAGNI controls

The following require a demonstrated need before implementation:

- dynamic Rust plugin ABI;
- generic event bus spanning all modules;
- distributed/multi-user state;
- recursive secret-backend dependency graphs;
- arbitrary provider-method passthrough;
- generic repository abstraction per SQLite table;
- multiple database connections/pools;
- background daemon before task survival requires it;
- encrypted-file secret backend before explicit demand and unlock UX;
- direct SDK integrations for every cloud secret manager;
- a test RPC exposed in production.

Prefer a reversible local implementation over an anticipatory framework. Conversely, do not postpone real external seams such as
harness adapters, secret backends, process supervision, or persistence boundaries; those variations already exist.

---

## 15. Handoff checklist for an implementation agent

Before coding:

1. Read `overall.md`, `sandboxing.md`, `working-with-secrets.md`, and `working-with-state.md`.
2. Produce a workspace dependency graph and confirm no Tauri/provider types enter domain/application crates.
3. Implement the mock/memory path before native providers.
4. Establish temporary-path integration tests before production path resolution is used broadly.
5. Implement the contract host and make the Python test pass before adding the Codex adapter.
6. Add Tauri only after the plain desktop facade is testable.
7. Run the architecture and secret reviewers on each boundary-changing pull request.

The implementation should optimize for clear ownership and cheap verification, not for the fewest files or the greatest number of abstractions.
