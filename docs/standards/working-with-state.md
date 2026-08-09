# YakShed: Working with State

> **Status:** normative backend standard  
> **Research snapshot:** 2026-08-09  
> **Applies to:** configuration, SQLite, artifacts, provider-owned state roots, caches, runtime files, migrations, reset/purge behavior, and test isolation

The words **MUST**, **MUST NOT**, **SHOULD**, and **MAY** describe requirements for the YakShed implementation.

## 1. Decision

YakShed has four persistent state pillars and one transient runtime area:

| Pillar | Meaning | Owner | Loss semantics |
|---|---|---|---|
| **Secrets** | Access credentials | delegated harness or selected secret backend | Must not be copied into ordinary state |
| **Config** | Durable, user/admin-controlled, non-secret settings | YakShed | Must survive restart; may be reset intentionally |
| **Cache** | Derived, rebuildable acceleration data | YakShed | Safe to delete at any time |
| **Data** | Program-managed work state whose loss is not acceptable | YakShed or explicitly identified provider | Must survive restart and ordinary reset |
| **Runtime** | Sockets, locks, PIDs, transient coordination | YakShed process/runtime broker | Expected to disappear after session/process exit |

These categories MUST NOT share a source of truth or deletion lifecycle merely because they are all files.

The defining test is:

```text
Did a person or administrator author it?            → Config
Can YakShed recompute it without semantic loss?     → Cache
Would deletion lose work, history, or identity?     → Data
Does possession alone grant access?                 → Secret
Should it disappear with the process/session?       → Runtime
```

When cache versus data is genuinely ambiguous, default to cache only when loss is demonstrably tolerable.

---

## 2. Goals

- One owner and one source of truth for every durable concept.
- Cross-platform paths are resolved in one injected component.
- Tests never read or write the developer's actual YakShed directories.
- Config writes are atomic and schema-versioned.
- SQLite transactions preserve application invariants.
- Artifacts are immutable, addressable, and recoverable from partial writes.
- Provider-owned state remains opaque to YakShed.
- Clear/reset/purge operations have narrow, predictable scopes.
- The WebView can reload and reconstruct projections from Rust snapshots.
- Each state module is independently testable without Tauri.

## 3. Non-goals

- Treating every provider transcript as YakShed's canonical data.
- Making config a second database.
- Storing secrets in SQLite because the database file has restrictive permissions.
- Using the Tauri Store plugin as the canonical application store.
- Building a distributed database or multi-device synchronization system in v1.
- Guaranteeing durability on unsupported network filesystems.
- Exposing generic filesystem operations to the WebView.

---

## 4. Sources of truth

| Concept | Canonical authority |
|---|---|
| Work items, graph edges, todos, notes, labels | YakShed SQLite |
| Connection definitions and non-secret backend configuration | YakShed config |
| Provider conversation context | Provider/harness state |
| Provider thread/turn/item projection | Provider, with rebuildable YakShed cache/projection |
| Working-copy filesystem | Git/filesystem in the selected execution runtime |
| Current Git diff | Git/filesystem, not a stale provider message |
| Provider-reported change | Timeline provenance item |
| Artifacts created/imported by YakShed | YakShed artifact store + SQLite metadata |
| Layout and UI selection | Config or lightweight UI preferences, depending on durability |
| Model catalog | Cache unless the user explicitly pins a model in config |
| Secret value | Delegated harness or selected secret backend |

Duplicating provider state into YakShed is acceptable only as a cache or search projection with a documented rebuild path.

---

## 5. AppPaths

Every path originates from one injected value object.

```rust
#[derive(Debug, Clone)]
pub struct AppPaths {
    pub config_root: std::path::PathBuf,
    pub cache_root: std::path::PathBuf,
    pub data_root: std::path::PathBuf,
    pub runtime_root: std::path::PathBuf,
}

impl AppPaths {
    pub fn production() -> Result<Self, PathError>;
    pub fn for_test(root: &std::path::Path) -> Self;
}
```

No domain, application, store, secret, harness, or Tauri command module may independently call home-directory or XDG helpers.
Legacy-source migration code may compute old paths, but target paths always come from `AppPaths`.

Use the `directories` crate behind this wrapper. The wrapper owns the final policy because `ProjectDirs::state_dir()` and
`runtime_dir()` are not present on every platform.

### 5.1 Platform policy

| Platform | Config | Cache | Data | Runtime |
|---|---|---|---|---|
| Linux | `ProjectDirs::config_dir()` | `cache_dir()` | `state_dir()` | `runtime_dir()` when available, otherwise data-local `runtime/` |
| macOS | Application Support project root + `config/` | `cache_dir()` | Application Support project root + `data/` | data root + `runtime/` |
| Windows | `config_dir()` under Roaming AppData | `cache_dir()` under Local AppData | `data_local_dir()` | data root + `runtime/` |

The exact mapping is implemented and tested in one place. Documentation and diagnostics should print the resolved paths.

### 5.2 Directory layout

```text
<config-root>/
└── config.toml

<cache-root>/
├── models/
├── git/
├── search/
├── readers/
└── protocol-projections/

<data-root>/
├── yakshed.sqlite3
├── artifacts/
│   ├── sha256/
│   └── staging/
├── provider-state/
│   └── <connection-id>/
├── audit/
├── backups/
└── recovery/

<runtime-root>/
├── yakshed.sock
├── yakshed.pid
├── locks/
└── tmp/
```

On POSIX, YakShed-owned state directories SHOULD be `0700` and sensitive files SHOULD be `0600`.
Windows uses inherited ACLs and native APIs rather than pretending POSIX modes provide enforcement.

---

## 6. Config

Config is durable, non-secret, human/admin-facing state.

### 6.1 Format

Use one `config.toml` with an explicit schema version.

```toml
schema_version = 1

[ui]
theme = "system"

[[connections]]
id = "0193f26e-7a72-7d42-bf77-0de14c4cc222"
name = "Work"
harness = "claude-code"
model_provider = "anthropic"
```

Config contains:

- connections and non-secret provider settings;
- secret backend definitions and references;
- project registrations and connection-routing policy;
- user-visible defaults and UI preferences;
- provider binary selection metadata;
- explicit experimental feature choices.

Config does not contain:

- API keys or OAuth tokens;
- provider transcripts;
- mutable run status;
- derived model catalogs;
- command output;
- work graph records;
- artifacts;
- cache contents;
- arbitrary provider JSON.

### 6.2 Atomic writes

Use an atomic replacement implementation such as `atomic-write-file` behind a YakShed helper.

```text
serialize to bytes
    ↓
write temporary file in destination directory
    ↓
flush/sync according to platform helper
    ↓
commit atomic replacement
    ↓
optionally sync parent directory where supported
```

The previous valid config or the new valid config should survive; a partial file must not become canonical.

Every save performs:

1. in-memory validation;
2. schema serialization;
3. atomic write;
4. restrictive permissions;
5. re-read/parse in tests;
6. a revision increment for observers.

### 6.3 Configuration service

Do not pass a mutable config struct around the process.

```rust
pub trait ConfigStore: Send + Sync {
    fn snapshot(&self) -> ConfigSnapshot;
    async fn update(
        &self,
        expected_revision: ConfigRevision,
        change: ConfigChange,
    ) -> Result<ConfigSnapshot, ConfigError>;
}
```

Updates are operation-shaped rather than “replace arbitrary TOML.” This permits validation, optimistic concurrency, and precise audit events.

### 6.4 Migrations

- Parse the schema version before normal construction.
- Reject a newer unsupported schema without rewriting it.
- Migrations are explicit, ordered, and deterministic.
- Pure data transformations are unit tested independently from disk I/O.
- A backup is created before any migration that could lose information.
- Migration writes use the same atomic writer.
- Migration is idempotent across crash/restart.
- Divergent legacy sources fail loudly rather than silently choosing precedence for durable concepts.
- Secrets found in legacy config follow `working-with-secrets.md` and are never copied through ordinary config DTOs.

### 6.5 Config reset

Resetting config removes or recreates `config.toml` and optionally UI preferences. It MUST NOT silently delete:

- secret values at external backends;
- provider-owned state;
- work items or artifacts;
- SQLite data;
- working copies.

The UI must state what will become unreachable after references are removed.

---

## 7. Cache

Cache is disposable and rebuildable.

Examples:

- model catalogs;
- syntax/highlighting products;
- Git status snapshots;
- provider history projections;
- search indexes that can be rebuilt from data/provider state;
- remote metadata with a known source;
- rendered Markdown or diff fragments.

Rules:

- deleting the entire cache root while YakShed is stopped is safe;
- a cache miss changes performance, not semantics;
- cache records include a format/version envelope;
- stale or corrupt cache entries are deleted and recomputed;
- cache writes are atomic where partial files could be misread;
- cache keys incorporate all inputs required for correctness;
- cache invalidation does not mutate canonical data;
- user-configurable TTLs are introduced only when they represent a real product behavior, not because an implementation needs a knob.

A cache should not have complex migrations. Change its version and rebuild it.

### Cache envelope

```rust
pub struct CacheEnvelope<T> {
    pub format_version: u32,
    pub created_at: time::OffsetDateTime,
    pub source_revision: Option<String>,
    pub value: T,
}
```

### Cache clear

`clear_cache` removes only cache roots. Connections, provider state, SQLite, artifacts, config, and secrets remain intact.

---

## 8. Durable application data

YakShed's canonical work state lives in SQLite.

### 8.1 Initial tables

```text
projects
working_copies
work_items
work_edges
connections_runtime_state
session_bindings
runs
timeline_items
artifacts
approval_requests
todos
notes
labels
work_item_labels
projection_cursors
audit_events
```

Not every table must exist in the first migration. The conceptual ownership must remain stable.

### 8.2 SQLite implementation

Use `rusqlite` with bundled SQLite. Own the connection in one dedicated database actor or serialized worker.
Do not hold a `rusqlite::Connection` in arbitrary application services and do not block Tokio worker threads with database I/O.

A reasonable initial stack:

```text
rusqlite + bundled SQLite
rusqlite_migration
one database actor / worker thread
```

`tokio-rusqlite` is acceptable if its lifecycle and error behavior fit the actor design. A small custom worker is also acceptable.
The architectural invariant is serialized connection ownership, not a specific wrapper crate.

### 8.3 Connection initialization

On open:

```sql
PRAGMA foreign_keys = ON;
PRAGMA journal_mode = WAL;
PRAGMA busy_timeout = 5000;
```

Start with a conservative durability setting and change it only after explicit performance testing. Schema migrations complete before the application reports the store as ready.

### 8.4 Transactions

Every operation that changes a domain invariant is one transaction.

Examples:

- create work item + initial status + parent edge;
- archive a work subtree;
- create session binding + initial run;
- resolve approval + append audit event;
- publish artifact metadata + attach it to a run;
- move work item between sections/manual ordering.

Repository methods should be application-shaped, not one generic CRUD interface per table.

```rust
#[async_trait::async_trait]
pub trait AppStore: Send + Sync {
    async fn create_work_item(&self, command: CreateWorkItem) -> Result<WorkItemSnapshot, StoreError>;
    async fn append_timeline_batch(&self, batch: TimelineBatch) -> Result<ProjectionRevision, StoreError>;
    async fn record_pending_approval(&self, approval: PendingApprovalRecord) -> Result<(), StoreError>;
    async fn resolve_approval(&self, resolution: ApprovalResolutionRecord) -> Result<(), StoreError>;
}
```

Do not expose raw SQL, table repositories, or connection handles to Tauri.

### 8.5 IDs and time

- Generate app-owned UUIDv7 IDs outside SQLite.
- Provider IDs remain opaque strings in namespaced columns.
- Store timestamps in a documented UTC representation.
- Inject `Clock` and `IdGenerator` into tests where determinism matters.
- Do not invent provider IDs or use them as YakShed primary keys.

### 8.6 Migrations and backups

- Migrations are append-only and run transactionally where SQLite permits.
- Every migration has an upgrade test from the previous schema.
- Destructive migrations create a database backup using SQLite-supported mechanisms, not a raw copy of an active WAL database.
- A failed migration leaves the previous database usable or produces a recoverable backup with clear instructions.
- Downgrades are not automatic. A newer schema is rejected by an older binary.

### 8.7 Corruption and recovery

At startup:

- classify open, migration, and integrity failures;
- never silently create a new empty database over an unreadable existing one;
- retain the failed database and WAL files for recovery;
- offer explicit recovery/export tooling;
- keep redacted diagnostics with schema/app versions and path metadata.

---

## 9. Artifacts

Large immutable bodies live in a content-addressed artifact store; metadata lives in SQLite.

```text
<data-root>/artifacts/sha256/ab/abcdef...
```

```rust
pub struct ArtifactRecord {
    pub id: ArtifactId,
    pub work_item_id: WorkItemId,
    pub run_id: Option<RunId>,
    pub kind: ArtifactKind,
    pub digest: ContentDigest,
    pub byte_len: u64,
    pub media_type: String,
    pub provenance: ArtifactProvenance,
}
```

Artifacts may include plans, diffs, files, images, command logs, browser captures, and provider-native payloads.
They are not secrets merely because they are sensitive; they are durable work data and receive restrictive filesystem permissions.

### 9.1 Publish sequence

1. stream content into a staging file while computing its digest;
2. enforce a configured maximum size;
3. flush and atomically move it to the digest path;
4. transactionally insert metadata and references;
5. periodically garbage-collect unreferenced blobs older than a safe grace period.

Write the blob before committing metadata. A crash may leave an orphan blob, which is recoverable. Metadata pointing at a missing blob is harder to repair.

### 9.2 Mutability

Artifact bodies are immutable. A changed document receives a new digest and normally a new artifact record or version relation.
Do not edit content-addressed files in place.

### 9.3 Reader safety

The Reader opens artifacts by `ArtifactId`, not arbitrary WebView-provided filesystem paths. Rust resolves the ID, validates access, and streams bounded content.

---

## 10. Provider-owned state

Provider state is durable data with a different owner.

```text
<data-root>/provider-state/<connection-id>/
```

For Codex, this may be used as the connection's `CODEX_HOME`.

YakShed owns:

- the directory allocation;
- connection association;
- process environment pointing the provider at it;
- high-level lifecycle policy;
- backup/delete confirmation.

The provider owns:

- internal file formats;
- auth records;
- native session persistence;
- caches and indexes under that root;
- migration of its own formats.

YakShed MUST interact through provider APIs instead of parsing or mutating provider files.
Provider-state deletion is an explicit operation and may log out accounts or destroy native session history.

Shared-provider-state mode, such as using an existing `~/.codex`, is an explicit advanced connection setting. YakShed must label it as externally shared and avoid claiming exclusive lifecycle control.

---

## 11. Runtime state

Runtime files include:

- local daemon sockets or named-pipe metadata;
- PID/lock files;
- temporary protocol traces;
- process-control files;
- short-lived staging directories.

Rules:

- runtime data is not canonical;
- stale locks are validated against process identity, not trusted blindly;
- startup may clean stale runtime files after ownership/path checks;
- secrets are not written to runtime files;
- runtime files are scoped to the current OS user;
- a future daemon owns its runtime root independently from the WebView.

---

## 12. Working copies and Git state

Working copies are app-owned domain resources, but their filesystem contents are external state.

```rust
pub struct WorkingCopy {
    pub id: WorkingCopyId,
    pub project_id: ProjectId,
    pub runtime_id: ExecutionRuntimeId,
    pub native_path: RuntimePath,
    pub branch: Option<String>,
    pub base_revision: Option<String>,
    pub kind: WorkingCopyKind,
    pub state: WorkingCopyState,
}
```

SQLite records identity and lifecycle. Git and the filesystem remain authoritative for current content.

Before destructive worktree operations, verify active runs, approvals, dirty state, child dependencies, and commit reachability.
App-owned Git operations are not protected by the harness sandbox and require their own narrow validation.

---

## 13. Projections and frontend revisions

The WebView is not a durable queue. Every frontend-facing aggregate has a monotonically increasing revision.

```text
get_work_item_snapshot(id) → revision 194
WorkItemPatched            → revision 195
TimelineBatchAppended      → revision 196
```

If the frontend observes a gap, it fetches a new snapshot.

Rules:

- Rust owns canonical projection revisions;
- events are hints/patches, not the sole copy of state;
- WebView reload does not lose work;
- deltas are batched before IPC to avoid one event per token;
- large timelines are paginated and virtualized;
- incomplete streaming items are transient until provider completion finalizes them.

---

## 14. Lifecycle and deletion matrix

| Operation | Config | Cache | YakShed SQLite/data | Artifacts | Provider state | Secrets | Working copies |
|---|---:|---:|---:|---:|---:|---:|---:|
| Clear cache | keep | delete | keep | keep | keep | keep | keep |
| Reset UI preferences | partial reset | keep/delete UI cache | keep | keep | keep | keep | keep |
| Reset connection config | remove selected config | selected cache | keep | keep | keep unless explicit | detach refs; do not auto-delete | keep |
| Clear connection credential | keep ref/status update | keep | keep | keep | delegated logout only when requested | selected value/binding | keep |
| Delete work item | keep | related cache may clear | archive/delete by product semantics | retain or delete by explicit policy | provider session archive/delete separately | keep | separate decision |
| Purge YakShed data | keep unless separately selected | delete | delete | delete | explicit separate confirmation | keep | explicit separate confirmation |
| Delete provider state | keep/update connection | provider cache under root deleted | keep YakShed data | keep | delete selected root | may remove delegated auth | keep |
| Factory uninstall | external installer policy | external installer policy | explicit user choice | explicit user choice | explicit user choice | external stores require explicit choice | never assume |

There is no ambiguous “clear everything” command in the core API. Destructive composition belongs in a clearly labelled maintenance flow.

---

## 15. Concurrency

- One database actor owns the SQLite connection.
- Config updates use optimistic revision checks and one writer.
- Artifact digest paths are safe for concurrent identical writes; staging files are unique.
- Worktree mutations serialize per repository.
- Provider state roots are not mutated by more than one incompatible provider process unless the provider explicitly supports it.
- Cache writes tolerate duplicate computation.
- No `.await` occurs while holding a synchronous mutex.
- Long filesystem/database operations do not block Tauri command threads or Tokio worker threads.

---

## 16. Test isolation

Tests MUST use injected paths.

```rust
let temp = tempfile::tempdir()?;
let paths = AppPaths::for_test(temp.path());
```

Do not rely only on changing `HOME` or XDG environment variables. Platform APIs may ignore them or use other known-folder sources.

Test fixtures receive:

- temporary `AppPaths`;
- memory secret backend;
- deterministic clock and ID generator when necessary;
- real atomic config writer;
- real SQLite and migrations;
- test artifact store;
- mock/fake provider adapter.

No unit or integration test may touch:

- the developer's OS keyring;
- normal YakShed config/data paths;
- `~/.codex`;
- real repositories unless explicitly marked as a native/system test;
- network services.

---

## 17. Testing requirements

### 17.1 Path tests

On each target OS, prove:

- production roots are absolute and distinct according to policy;
- `for_test` is entirely contained under the supplied root;
- directory creation applies expected permissions;
- no module bypasses `AppPaths` in architecture-sensitive code review.

### 17.2 Config tests

- valid round trip;
- unknown/newer schema rejection;
- every migration path;
- atomic failure leaves previous file intact;
- concurrent revision conflict;
- no access-secret fields serialize;
- reset scope.

### 17.3 Database tests

- migrations from every supported prior schema;
- foreign keys and transactions;
- crash/rollback behavior for multi-record use cases;
- provider IDs remain opaque and namespaced;
- archive/delete lifecycle;
- pagination and revision monotonicity;
- integrity/recovery classification.

### 17.4 Artifact tests

- digest and deduplication;
- maximum size enforcement;
- interrupted staging write;
- orphan collection grace period;
- metadata never points at an uncommitted blob;
- path traversal impossible through `ArtifactId`;
- content streaming bounds.

### 17.5 Separation tests

- clearing cache does not affect config/data/secrets;
- resetting config does not affect data/provider state/secrets;
- purging data does not delete external secrets;
- memory secret restart loses values while config references and SQLite data persist;
- provider-owned state is never parsed by YakShed tests.

The Python contract test exercises these high-value separations through the composed backend.

---

## 18. Definition of Done

A state module is complete only when:

- its state classification and source of truth are documented;
- its public API is operation-shaped and does not expose implementation handles;
- paths are injected;
- writes are atomic or transactional as appropriate;
- migrations and recovery behavior are specified;
- unit tests cover pure transformations and errors;
- integration tests use real storage under temporary paths;
- lifecycle clear/reset/purge behavior is tested;
- no Tauri dependency exists unless the module is specifically the desktop adapter;
- no secret value can be serialized into its state;
- the architecture reviewer has no unresolved blocking or major findings.

---

## 19. Research references

- Open CLI Collective state standard: `https://github.com/open-cli-collective/cli-common/blob/main/docs/working-with-state.md`
- `directories::ProjectDirs`: `https://docs.rs/directories/latest/directories/struct.ProjectDirs.html`
- Atomic file replacement: `https://docs.rs/atomic-write-file/latest/atomic_write_file/`
- `rusqlite`: `https://github.com/rusqlite/rusqlite`
- `rusqlite_migration`: `https://docs.rs/rusqlite_migration/latest/rusqlite_migration/`
