# YakShed: Overall Architecture

> **Status:** Directional architecture and implementation plan  
> **Research snapshot:** 2026-08-09  
> **Primary integration target:** Codex App Server, with additional harnesses added behind the same product boundary

## 1. Architectural thesis

YakShed should be a **work-centric desktop control plane over provider-owned agent harnesses**.

It should not reimplement the Codex agent loop, tool system, session persistence, approvals, authentication, or operating-system sandbox. For Codex, YakShed should launch a pinned `codex` executable, connect to `codex app-server`, translate its protocol into YakShed’s domain model, and supervise the resulting process. Codex App Server is explicitly intended for rich clients that need authentication, conversation history, approvals, and streamed agent events.[^codex-app-server]

The differentiated product is not “a prettier terminal.” It is the structure around the work:

```text
Project
└── Work item / yak
    ├── one or more provider sessions
    ├── parent, child, dependency, and blocker relationships
    ├── todos
    ├── notes
    ├── labels
    ├── working copy
    ├── runs
    └── artifacts
        ├── plan
        ├── diff
        ├── file
        ├── command output
        ├── image
        └── provider-native result
```

The primary navigation unit is **the work**, not the file. Files, plans, diffs, terminal output, and model messages are artifacts encountered while doing that work.

---

## 2. Product and harness ownership

### YakShed owns

- Projects and repository registrations.
- Work items and the “yak” graph.
- Labels, todos, notes, statuses, and archive semantics.
- Working-copy and worktree allocation.
- Reader layout and artifact navigation.
- Cross-provider search and indexing.
- Provider-neutral identifiers.
- Process supervision and runtime health.
- Connection/profile selection.
- Secret references for credentials that are not provider-managed.
- Application persistence in SQLite.
- Tauri IPC and every app-owned filesystem or Git operation.

### A harness owns

For Codex, this includes:

- The agent loop and model interaction.
- Provider thread, turn, and item semantics.
- Context management and compaction.
- Tool selection and invocation.
- Command and file-edit execution.
- Codex sandbox enforcement.
- Codex approval semantics.
- Codex session persistence.
- ChatGPT/OpenAI authentication when delegated to App Server.
- Codex configuration, skills, and MCP behavior.

The wrapper boundary must remain real. YakShed may project and cache provider state, but it should not quietly become the canonical implementation of the provider’s harness.

---

## 3. Terminology

Several axes need separate names because collapsing them into a single “provider” field creates architectural confusion.

| Concept | Meaning | Examples |
|---|---|---|
| **Harness** | The agent runtime and its session/tool semantics | Codex, Claude Code |
| **Model provider** | The service serving model requests | OpenAI, Anthropic, Fireworks, Bedrock |
| **Execution runtime** | Where the harness and commands run | Local macOS, WSL, SSH host, container, VM |
| **Connection** | A named trust, billing, state, and credential boundary in YakShed | Home Codex, Work Claude, Fireworks Lab |
| **Preset** | Changeable run behavior | Model, reasoning effort, sandbox, approval mode |
| **Provider session** | A harness-native conversation/session | Codex thread, Claude session |
| **Work item** | YakShed’s provider-neutral unit of work | “Retire the Q2 devserver shim” |

A useful relationship is:

```text
Connection
├── harness
├── model provider
├── auth binding
├── isolated state root
├── execution runtime
└── trust/data-routing policy

Work item
├── session binding → connection + provider session
├── runs
└── artifacts
```

A provider session should remain bound to the connection that created it. Changing from Home Codex to Work Claude or Fireworks should create a new session binding, optionally seeded with a handoff summary, rather than mutating the identity of an existing session.

---

## 4. System architecture

```text
┌──────────────────────────────────────────────────────────────────────┐
│                         Tauri WebView                                │
│                                                                      │
│  Svelte + TypeScript                                                 │
│  ├── project/work tree                                               │
│  ├── conversation timeline                                           │
│  ├── todo and notes inspector                                        │
│  ├── artifact reader                                                 │
│  ├── approval surfaces                                               │
│  ├── connection and permission selectors                             │
│  └── command palette, shortcuts, and layout                          │
└──────────────────────────────┬───────────────────────────────────────┘
                               │ product commands
                               │ revisioned snapshots and event patches
┌──────────────────────────────▼───────────────────────────────────────┐
│                         Rust application core                        │
│                                                                      │
│  Work graph            SQLite             Artifact store             │
│  Run orchestration     Search/index       Git/worktrees              │
│  Approval broker       Connection store   Security/redaction         │
│  Credential resolver   Runtime registry   Projection reducers        │
│                                                                      │
│  ┌──────────────────────── RuntimeBroker ──────────────────────────┐ │
│  │                                                                 │ │
│  │  CodexAdapter                                                   │ │
│  │    └── CodexRpcClient                                           │ │
│  │          └── pinned `codex app-server` process                  │ │
│  │                                                                 │ │
│  │  ClaudeAdapter                                                  │ │
│  │    └── Claude SDK/CLI process                                   │ │
│  │                                                                 │ │
│  │  MockAdapter                                                    │ │
│  │    └── deterministic scripted provider                          │ │
│  └─────────────────────────────────────────────────────────────────┘ │
└──────────────────────────────────────────────────────────────────────┘
```

### Frontend rule

The WebView is a projection of application state. It must never be the canonical owner of:

- full transcripts;
- subprocesses;
- approval requests;
- filesystem state;
- secrets;
- Git state;
- provider request correlation.

It should be possible to reload the WebView, request fresh snapshots, and continue without reconstructing state from JavaScript memory.

---

## 5. Codex integration boundary

### Use App Server over stdio

The primary Codex integration should be:

```text
Rust process
   stdin  ─────────────► codex app-server
   stdout ◄───────────── JSONL protocol messages
   stderr ◄───────────── logs and diagnostics
```

App Server uses a JSON-RPC-like bidirectional protocol and can generate TypeScript or JSON Schema artifacts matching the exact Codex version that generated them.[^codex-schema] Use newline-delimited stdio for the first production implementation. Do not scrape the TUI and do not build product semantics around terminal text.

### Do not link against Codex internals

Treat the executable and generated protocol schema as the supported boundary. Avoid a Git dependency on internal Codex Rust crates unless OpenAI publishes an independently versioned, supported client crate. Linking against workspace internals would couple YakShed’s build graph to implementation details while the shipped sidecar could still be a different version.

### Bundle a pinned binary

Tauri supports external sidecar binaries through `bundle.externalBin`.[^tauri-sidecar] The normal release channel should bundle a known Codex version and test the exact app/binary/schema combination.

Maintain a lock record such as:

```json
{
  "codex_version": "<pinned-version>",
  "targets": {
    "aarch64-apple-darwin": {
      "asset": "...",
      "sha256": "..."
    }
  },
  "stable_schema_sha256": "...",
  "adapter_revision": 3
}
```

Offer a system-installed binary only as an explicit advanced mode. Probe and display its version, and warn or refuse when it falls outside YakShed’s tested compatibility range.

### Process topology

Do not launch one Codex process per work item. A single App Server can serve multiple Codex threads. A practical runtime identity is:

```rust
struct CodexRuntimeKey {
    connection_id: ConnectionId,
    binary_digest: BinaryDigest,
    codex_home: PathBuf,
    execution_runtime: ExecutionRuntimeId,
}
```

Use one process per runtime key. Separate processes are appropriate when connections differ by account, `CODEX_HOME`, binary version, host, container, or required crash/security isolation.

---

## 6. Codex protocol client

### Runtime actor

Each harness runtime should be managed by one actor that owns the child process, outbound writer, request correlation, subscriptions, and health state.

```rust
struct CodexRuntime {
    child: ChildHandle,
    outbound: mpsc::Sender<OutboundCommand>,
    pending_client_requests: HashMap<ClientRequestId, PendingRequest>,
    pending_server_requests: HashMap<ServerRequestId, PendingProviderRequest>,
    loaded_sessions: HashSet<ProviderSessionId>,
    health: RuntimeHealth,
}
```

A single actor avoids a web of locks around request IDs, process state, and event ordering.

### Framing and streams

The reader must:

- frame by newline rather than assuming one OS read equals one message;
- tolerate partial reads and rapid batches;
- enforce a configurable maximum frame size;
- preserve malformed frames in a bounded diagnostic buffer;
- keep stdout and stderr strictly separate;
- continuously drain stderr so it cannot block the child;
- redact secrets before persisting diagnostics.

### Bidirectional requests

App Server can initiate approval and input requests. The protocol reader must register those requests and continue reading. It must never wait synchronously for the frontend inside the stdout loop.

```text
receive provider request
    ↓
register in ApprovalBroker
    ↓
persist pending projection
    ↓
emit frontend event
    ↓
continue reading protocol
```

A later Tauri command resolves the pending request and sends the correlated response.

### Streaming reduction

Provider deltas are transient. Accumulate and batch them for the UI; do not write one SQLite row per token. The terminal provider item or run event should finalize the normalized state.

Keep the provider-native raw payload available for compatibility and diagnostics:

```rust
enum ProviderItem {
    AgentMessage(AgentMessage),
    CommandExecution(CommandExecution),
    FileChange(FileChange),
    ToolCall(ToolCall),
    Unknown {
        item_type: String,
        raw: Box<serde_json::value::RawValue>,
    },
}
```

Unknown optional fields or new item types should degrade visibly rather than crashing the runtime.

### Mutation retry policy

Read-only requests can usually be retried after explicit overload responses or reconnection. Mutating requests require outcome reconciliation. A disconnect after sending `turn/start` does not prove that the operation failed, so blind retries may duplicate work.

Use an explicit uncertain state:

```rust
enum RunStatus {
    Starting,
    Running,
    AwaitingApproval,
    AwaitingUserInput,
    Completed,
    Interrupted,
    Failed,
    Disconnected,
    OutcomeUnknown,
}
```

---

## 7. Product-level API

The Tauri command surface should express YakShed operations, not raw Codex RPC methods.

```text
create_work_item
send_message
steer_run
interrupt_run
resolve_approval
spawn_child_work
create_working_copy
open_artifact
archive_work_subtree
change_session_preset
```

Avoid exposing commands named like `codex_thread_start` or `codex_turn_start`. Otherwise Codex semantics will leak through the frontend and provider neutrality will become nominal.

Events should also be product-oriented and revisioned:

```rust
enum AppEvent {
    WorkItemPatched {
        work_item_id: WorkItemId,
        revision: u64,
        patch: WorkItemPatch,
    },
    TimelineBatchAppended {
        work_item_id: WorkItemId,
        revision: u64,
        items: Vec<TimelineItem>,
    },
    ApprovalOpened {
        work_item_id: WorkItemId,
        approval: ApprovalView,
    },
    ArtifactPublished {
        work_item_id: WorkItemId,
        artifact: ArtifactSummary,
    },
    ProviderHealthChanged {
        runtime_id: RuntimeId,
        state: ProviderHealth,
    },
}
```

Tauri events are hints, not a durable queue. If the frontend observes a revision gap, it should refetch a snapshot.

---

## 8. Domain model and sources of truth

An app work item is not a provider thread.

```rust
struct WorkItem {
    id: WorkItemId,
    project_id: ProjectId,
    title: String,
    status: WorkStatus,
    primary_working_copy: Option<WorkingCopyId>,
    created_at: Timestamp,
    archived_at: Option<Timestamp>,
}

struct SessionBinding {
    id: SessionBindingId,
    work_item_id: WorkItemId,
    connection_id: ConnectionId,
    harness: HarnessKind,
    runtime_id: RuntimeId,
    provider_session_id: String,
    role: SessionRole,
    provider_metadata: serde_json::Value,
}

struct Run {
    id: RunId,
    session_binding_id: SessionBindingId,
    provider_run_id: String,
    status: RunStatus,
    started_at: Timestamp,
    completed_at: Option<Timestamp>,
}

struct WorkEdge {
    from: WorkItemId,
    to: WorkItemId,
    kind: WorkEdgeKind, // SpawnedFrom, Blocks, DependsOn, Related
}
```

| State | Canonical authority |
|---|---|
| Provider conversation context | Harness |
| Provider thread/turn/item history | Harness, with rebuildable YakShed cache |
| Work graph | YakShed SQLite |
| Todos, notes, labels | YakShed SQLite |
| Working-copy allocation | YakShed |
| Current repository state | Git/filesystem |
| Live approval | Provider request plus YakShed projection |
| Layout and current selection | YakShed/UI |
| Large immutable artifact body | YakShed artifact store or source file |

Use YakShed-generated IDs, such as UUIDv7, and retain provider IDs as opaque namespaced strings.

---

## 9. Artifact model

The right-hand Reader should be driven by `ArtifactId`, not by arbitrary paths supplied by JavaScript.

```rust
struct Artifact {
    id: ArtifactId,
    work_item_id: WorkItemId,
    run_id: Option<RunId>,
    kind: ArtifactKind,
    content_ref: ContentRef,
    provenance: ArtifactProvenance,
}
```

Store artifact metadata in SQLite. Large immutable content can live in a content-addressed filesystem store:

```text
artifacts/
└── sha256/
    └── ab/
        └── abcdef...
```

Keep two distinct diff concepts:

- **Provider diff:** what an agent reported changing during a run.
- **Working-copy diff:** what Git says exists now.

The first is provenance. The second is authoritative for the current Reader view.

---

## 10. Working copies and worktrees

YakShed should own working-copy allocation because working copies belong to the provider-neutral work graph.

```text
create WorkItem
    ↓
allocate or reuse WorkingCopy
    ↓
start provider session with that cwd
    ↓
harness operates inside the YakShed-selected working copy
```

Do not let YakShed and a harness independently create or remove the same worktree.

A working-copy record should be runtime-aware:

```rust
struct WorkingCopy {
    id: WorkingCopyId,
    project_id: ProjectId,
    runtime_id: ExecutionRuntimeId,
    native_path: String,
    branch: Option<String>,
    base_revision: Option<String>,
    kind: WorkingCopyKind,
    state: WorkingCopyState,
}
```

Using a runtime-native string rather than assuming a local `PathBuf` leaves room for WSL, SSH, containers, and remote workers.

For the initial implementation, invoking the system `git` executable with argv arrays is likely preferable to rebuilding Git behavior through a library. Serialize worktree mutations per repository and explicitly handle hooks, submodules, LFS, dirty trees, and process crashes.

---

## 11. Yaks and session branching

“Spawn a yak” creates an app-owned child work item first. Provider-session creation is a strategy selected afterward.

```text
Fresh
    New provider session with no inherited conversation

Native fork
    Provider-native session fork where supported

Handoff
    New session seeded with a summary and selected artifacts

Same session
    Separate app work item represented within the parent provider session
```

For Codex, a native thread fork may be available. That should remain one implementation of a yak, not the definition of a yak. Cross-provider children necessarily use a handoff rather than a native fork.

Provider-internal subagents should not automatically become durable user-visible yaks. Some are short-lived implementation details; others may be promoted when they carry meaningful independent work.

---

## 12. Harness adapter boundary

Normalize product semantics while retaining provider-native capability differences.

```rust
#[async_trait::async_trait]
pub trait HarnessAdapter: Send + Sync {
    fn descriptor(&self) -> HarnessDescriptor;

    async fn capabilities(
        &self,
        runtime: &RuntimeHandle,
    ) -> Result<HarnessCapabilities>;

    async fn list_sessions(
        &self,
        runtime: &RuntimeHandle,
        query: SessionQuery,
    ) -> Result<Page<ProviderSessionSummary>>;

    async fn start_session(
        &self,
        runtime: &RuntimeHandle,
        spec: StartSessionSpec,
    ) -> Result<ProviderSession>;

    async fn resume_session(
        &self,
        runtime: &RuntimeHandle,
        id: &ProviderSessionId,
    ) -> Result<ProviderSession>;

    async fn start_run(
        &self,
        session: &ProviderSession,
        input: HarnessInput,
        options: RunOptions,
    ) -> Result<ProviderRunId>;

    async fn steer(&self, run: &ProviderRunId, input: HarnessInput) -> Result<()>;
    async fn interrupt(&self, run: &ProviderRunId) -> Result<()>;

    async fn respond_to_request(
        &self,
        request: ProviderRequestId,
        response: ProviderResponse,
    ) -> Result<()>;

    fn subscribe(&self) -> ProviderEventStream;
}
```

Capabilities should be queried, not inferred solely from the harness name:

```rust
struct HarnessCapabilities {
    persistent_sessions: bool,
    session_listing: bool,
    native_fork: bool,
    session_archive: bool,
    mid_run_steering: bool,
    client_approvals: bool,
    user_input_requests: bool,
    structured_file_changes: bool,
    command_output_streaming: bool,
    native_subagent_lineage: bool,
    images: bool,
    skills: bool,
    mcp: bool,
    account_ui: bool,
    model_discovery: bool,
}
```

Capabilities may vary by binary version, account, runtime, and enabled experimental features.

Implement `MockHarness` first, Codex second, and a genuinely different harness such as Claude Code third. Do not design a dynamic Rust plugin ABI before two real adapters reveal the actual points of divergence.

---

## 13. Connection credentials: deliberately short version

Credential management deserves a separate design, but the architecture needs a stable seam now.

There are two primary profile/connection modes.

### Mode A: delegated authentication

The harness owns login, secure storage, token refresh, and logout. YakShed invokes the harness’s account APIs and stores only non-secret connection metadata.

```rust
enum ProfileAuth {
    Delegated {
        authority: DelegatedAuthAuthority,
    },
    SecretBacked(SecretBinding),
}
```

For a normal Codex ChatGPT connection:

```text
YakShed → account/login/start
Codex App Server → browser/device flow
Codex → token storage and refresh
YakShed → account status only
```

A separate `CODEX_HOME` per isolated Codex connection provides a clean state and identity boundary. Codex can store its credentials in the operating-system keyring when configured to do so.[^codex-auth]

### Mode B: secret-backed authentication

The harness expects YakShed to supply a credential, such as an Anthropic or Fireworks API key. YakShed stores a **reference**, resolves it through a credential backend, and delivers it narrowly to the relevant runtime.

```rust
struct SecretBinding {
    backend_id: CredentialBackendId,
    selector: SecretSelector,
    delivery: SecretDelivery,
}

enum SecretDelivery {
    ProcessEnvironment { variable: String },
    ProviderRequestHeader { header: String },
    CredentialHelper,
    AdapterNative,
}
```

The backend abstraction should support many implementations without changing connection records:

```rust
#[async_trait::async_trait]
trait CredentialBackend: Send + Sync {
    fn descriptor(&self) -> CredentialBackendDescriptor;

    async fn resolve(
        &self,
        selector: &SecretSelector,
        context: &SecretContext,
    ) -> Result<SecretLease>;

    async fn put(
        &self,
        selector: &SecretSelector,
        value: SecretValue,
    ) -> Result<()>;

    async fn delete(&self, selector: &SecretSelector) -> Result<()>;
}

struct SecretLease {
    value: SecretValue,
    expires_at: Option<Timestamp>,
    renewable: bool,
    provenance: SecretProvenance,
}
```

Likely backend variations include:

- OS keychain;
- environment-only lookup;
- command/credential helper;
- encrypted local file;
- 1Password or similar desktop vault;
- HashiCorp Vault;
- AWS, GCP, or Azure secret managers;
- an enterprise identity broker issuing short-lived credentials.

The invariant is more important than any backend:

> SQLite contains a secret reference, never the secret value.

Secrets must not be returned to the WebView, placed in command-line arguments, included in debug output, or inherited by unrelated harness processes. When a secret is delivered through the harness process environment, configure the harness’s child-command environment policy so agent-run commands do not inherit it.[^codex-shell-env]

Detailed backend semantics, lease renewal, interactive unlock, and enterprise policy are intentionally deferred to a separate credential-manager design.

---

## 14. Security boundary

The security model has at least three layers:

```text
Untrusted rendered content
    ↓
Tauri WebView
    ↓ narrow application IPC
Rust application
    ↓ supervised provider protocol
Harness process
    ↓ harness sandbox and approvals
Spawned commands and filesystem
```

Codex owns the sandbox for Codex-generated commands. YakShed still owns:

- Tauri IPC authorization;
- path canonicalization;
- app-owned Git and filesystem actions;
- Markdown/HTML sanitization;
- secret handling;
- process launch policy;
- connection/repository routing rules;
- optional outer container or VM isolation.

See [`sandboxing.md`](sandboxing.md) for the detailed policy and approval design.

---

## 15. Persistence

A useful initial schema is:

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

Connection definitions, secret-backend definitions, credential bindings, and routing policy are
canonical in YakShed config, not SQLite (see `../standards/working-with-state.md` and
`../standards/working-with-secrets.md`). SQLite holds only derived runtime state for connections,
keyed by the config-owned connection ID.

Store normalized final timeline items for search and fast rendering, but keep the cache rebuildable from provider history where the harness supports it.

An optional raw protocol trace is valuable for diagnostics and replay tests, but it should be bounded, redacted, short-lived, and disabled by default outside explicit diagnostics.

---

## 16. Crash recovery and background ownership

When a provider process exits:

1. Mark the runtime unavailable.
2. Stop accepting new work for it.
3. Fail known-safe pending reads.
4. Mark ambiguous mutations as `OutcomeUnknown`.
5. Retain the last rendered state.
6. Capture bounded, redacted diagnostics.
7. Restart with backoff when policy allows.
8. Initialize the new connection.
9. List/read relevant provider sessions.
10. Reconcile provider state with YakShed bindings.
11. Publish corrected snapshots.

Initially, the Tauri process may own all child runtimes. Shape the process layer as a broker so it can later move to a local daemon:

```rust
#[async_trait::async_trait]
trait RuntimeBroker {
    async fn start_runtime(&self, spec: RuntimeSpec) -> Result<RuntimeHandle>;
    async fn connect_runtime(&self, id: RuntimeId) -> Result<RuntimeHandle>;
    async fn runtime_status(&self, id: RuntimeId) -> Result<RuntimeStatus>;
}
```

A future daemon can keep runs alive across UI restarts and provide a natural boundary for SSH, WSL, containers, VMs, and remote workers.

---

## 17. Suggested Rust workspace

The authoritative workspace layout, crate naming, and consolidation guidance live in
`../standards/backend-composition-and-testing.md`. The repo-local code-review agents
match their file globs against these crate names; do not rename crates without updating both.

Summary shape:

```text
crates/
├── yakshed-domain/
│   ├── ids, work graph, runs, artifacts, approvals
│   └── no I/O, no Tauri
├── yakshed-application/
│   └── use cases, snapshots/revisions, application ports
├── yakshed-store/
│   ├── AppPaths, config, SQLite actor + migrations
│   └── artifact store
├── yakshed-secrets/
│   ├── broker, resolver contracts, redaction
│   └── memory / local OS / 1Password / helper / environment backends
├── yakshed-harness/
│   ├── provider-neutral adapter contract, capabilities, events
│   └── runtime/process supervision (broker-shaped)
├── provider-codex/
│   ├── installation, supervisor, framing, rpc
│   └── generated schema, reducer, adapter
├── provider-mock/
│   └── scripted runtime, fault fixtures
└── yakshed-desktop-api/
    └── frontend-safe DTOs, plain Rust facade, event/snapshot mapping

src-tauri/
└── application assembly and Tauri integration

tools/
└── yakshed-contract-host/

web/
└── Svelte frontend
```

Git/worktree operations may begin as a module inside `yakshed-application` or a small adapter crate;
promote it when dependency direction requires. Keep provider, harness, and secret crates free of Tauri
dependencies so they can be tested independently and later moved into a daemon.

---

## 18. Testing strategy

### Deterministic fake provider

Build a fake `codex` executable that can:

- verify initialization;
- emit scripted deltas and completions;
- request approvals;
- split JSON across arbitrary writes;
- flood stderr;
- emit unknown event types;
- return overload errors;
- exit mid-run;
- produce malformed or oversized frames.

Most protocol tests should not require a live account.

### Golden trace replay

Maintain sanitized traces for:

- a simple answer;
- command execution;
- file changes;
- approval accepted and declined;
- user-input requests;
- steering and interruption;
- resume and fork;
- crash recovery;
- unknown events;
- large diffs and command output.

Replay traces into provider reducers and compare normalized snapshots.

### Bundled-binary contract tests

For every supported target, test the exact packaged binary and generated schema. Include initialization, account state, model discovery, thread listing, turn start, interruption, history read, and graceful shutdown.

### Process tests

Test process groups or Windows Job Objects, child and grandchild cleanup, non-ASCII paths, app exit during approval, stderr pressure, startup timeout, and executable permissions.

### Frontend load tests

Exercise dozens of dormant work items, several concurrent active runs, rapid text deltas, very large command outputs, long timelines, and repeated Reader switching. Virtualize the tree and timeline and batch backend deltas.

---

## 19. Implementation sequence

| Phase | Scope | Exit condition |
|---|---|---|
| **1. Protocol spike** | Bundled binary, stdio client, initialize, one thread/turn, streaming, approval, interrupt | Unknown events and process crashes do not take down YakShed |
| **2. Codex foundation** | Auth, model list, list/read/resume, normalized timeline, diagnostics | Existing Codex sessions can be opened and continued |
| **3. Product state** | Work items, graph, todos, notes, labels, artifacts, revisioned snapshots | UI no longer treats provider threads as product records |
| **4. Working copies** | Repository registry, worktrees, current diff, cleanup safeguards | Multiple work items can safely use one repository |
| **5. Yak workflows** | Fresh/fork/handoff, subtree archive, blockers | Work graph semantics are provider-independent |
| **6. Credential seam** | Delegated auth plus one secret backend and command helper | Connections can isolate home/work/lab credentials |
| **7. Second harness** | Claude or another materially different adapter | Provider API survives real semantic divergence |
| **8. Background broker** | Local daemon and reconnect | Runs survive UI restarts |
| **9. Remote runtimes** | WSL/SSH/container/VM adapters | Paths and process ownership no longer assume local execution |

The protocol spike is not complete until server-initiated requests work. Streaming assistant text without a nonblocking approval path does not validate the architecture.

---

## 20. Decisions to lock in

1. YakShed is a work-management and supervision product, not an agent-harness reimplementation.
2. Codex App Server over stdio is the primary Codex integration.
3. The Codex executable is a pinned, supervised sidecar rather than a linked library.
4. App `WorkItem` and provider `Session` remain separate entities.
5. The work graph, notes, todos, labels, and working-copy allocation are YakShed-owned.
6. Provider context, native session history, sandbox, approvals, and delegated auth remain harness-owned.
7. Tauri exposes product operations, never generic shell/filesystem primitives or raw provider RPC.
8. Provider events are normalized without discarding their native payloads.
9. Unknown protocol additions degrade gracefully.
10. Frontend events are revisioned hints; Rust snapshots are queryable at all times.
11. Connections isolate account, state root, runtime, credential path, billing, and data-routing policy.
12. Authentication is either delegated to the harness or resolved through a pluggable credential backend—never ad hoc in individual adapters.
13. The runtime layer is broker-shaped so local child ownership can later move into a daemon or remote environment.

---

## References

[^codex-app-server]: OpenAI, [Codex App Server](https://learn.chatgpt.com/docs/app-server). The documentation describes App Server as the integration surface for rich clients needing authentication, history, approvals, and streamed agent events.

[^codex-schema]: OpenAI, [Codex App Server — generated schemas](https://learn.chatgpt.com/docs/app-server). `codex app-server generate-ts` and `generate-json-schema` produce artifacts matching the invoked Codex version.

[^tauri-sidecar]: Tauri, [Embedding External Binaries](https://v2.tauri.app/develop/sidecar/).

[^codex-auth]: OpenAI, [Codex Authentication](https://learn.chatgpt.com/docs/auth). Codex supports file, keyring, and automatic credential-storage modes through `cli_auth_credentials_store`.

[^codex-shell-env]: OpenAI, [Codex Advanced Configuration](https://learn.chatgpt.com/docs/config-file/config-advanced). `shell_environment_policy` controls which environment variables are inherited by commands spawned through Codex.

Additional protocol detail: OpenAI, [openai/codex — app-server README](https://github.com/openai/codex/blob/main/codex-rs/app-server/README.md).
