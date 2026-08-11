# YakShed: Sandboxing, Permissions, and Approval Architecture

> **Status:** Directional security design  
> **Research snapshot:** 2026-08-09  
> **Scope:** Local and externally isolated harness execution, beginning with Codex App Server

## 1. Decision

YakShed must care deeply about sandboxing, but it should **not reimplement Codex’s operating-system sandbox**.

For the Codex adapter:

```text
Codex
└── implements and enforces the command sandbox

YakShed
└── selects policy, displays it, mediates approvals, supervises failures,
    and secures every YakShed-owned operation outside that sandbox
```

Codex applies platform-native enforcement to commands it spawns, including tools such as Git, package managers, test runners, and scripts. Current Codex documentation describes macOS Seatbelt, a native Windows sandbox, and Bubblewrap or a helper on Linux/WSL.[^codex-sandbox]

YakShed’s job is the control plane:

- choose the intended sandbox and approval policy;
- pass it to the harness;
- display the effective policy and its scope;
- receive provider-initiated approval requests;
- collect and return a decision;
- persist pending approval projections;
- detect and surface unavailable or degraded enforcement;
- prevent YakShed’s own Rust/Tauri operations from bypassing the intended boundary;
- optionally run the entire harness inside a stronger outer runtime such as a container or VM.

---

## 2. Threat model

The design should address at least these threats:

1. **Agent error:** a well-intentioned agent runs a destructive or overly broad command.
2. **Prompt injection from repository content:** instructions in code, documentation, logs, or tool output try to induce unsafe behavior.
3. **Malicious dependency or build script:** a package install, test, or build process attempts host access or data exfiltration.
4. **Accidental policy selection:** the user thinks a run is workspace-scoped when it actually has full access.
5. **Credential leakage:** provider keys inherited by agent-run commands appear in logs or leave the machine.
6. **App-level bypass:** a generic Tauri command or app-owned Git operation acts outside the harness sandbox.
7. **Protocol deadlock:** a provider waits for approval while YakShed’s reader is blocked and cannot process more messages.
8. **Sandbox degradation:** a platform prerequisite is missing and execution silently becomes less restricted.
9. **Wrong trust domain:** a work repository is run through the wrong home/work/experimental connection.
10. **Provider compromise or bug:** the harness process itself performs host operations outside the sandboxed command path.

The built-in Codex sandbox primarily addresses commands spawned through Codex. It does not, by itself, solve every threat above.

---

## 3. Three independent controls

A friendly permission picker usually compresses three different dimensions.

### 3.1 Sandbox or permission policy

This is the technical boundary that limits what a spawned command can read, write, or reach over the network.

Typical Codex modes include:

- `read-only`;
- `workspace-write`;
- `danger-full-access`;
- named/custom permission profiles;
- `externalSandbox` when an outer runtime supplies enforcement.

### 3.2 Approval policy

This determines when Codex is allowed to stop and request an exception or escalation.

It is distinct from the sandbox. A command can be denied by policy, allowed inside the current boundary, or paused pending approval.

### 3.3 Approval reviewer

This determines who resolves eligible approval requests:

- the user;
- an automatic reviewer;
- a future enterprise/policy service;
- no reviewer because escalation is disabled.

The current Codex UI presets map approximately as follows:[^codex-sandbox]

| YakShed-facing preset | Sandbox | Approval behavior | Reviewer |
|---|---|---|---|
| **Read only** | read-only | ask or fail on mutation, according to policy | user by default |
| **Ask for approval** | workspace-write | request escalation when necessary | user |
| **Approve for me** | workspace-write | request escalation when necessary | automatic reviewer |
| **Full access** | danger-full-access | no approval boundary | none |

“Ask for approval” does not mean “ask before every command.” Routine commands inside the current boundary can proceed autonomously.

“Approve for me” should not be described as unrestricted access. The sandbox boundary remains; the reviewer changes.

“Full access” should be treated as a materially different trust mode, not merely a convenience preference.

---

## 4. Execution and approval flow

```text
User selects a YakShed permission preset
                   │
                   ▼
YakShed resolves provider-specific policy
                   │
                   ▼
Rust starts or updates the provider session/turn
                   │
                   ▼
Model proposes a command or file operation
                   │
                   ▼
Harness evaluates current sandbox and approval policy
          ┌────────┴─────────┐
          │                  │
Allowed inside          Requires escalation
current boundary             │
          │                  ▼
          │          Provider sends a request
          │                  │
          │                  ▼
          │          YakShed ApprovalBroker
          │                  │
          │          user / auto-review / policy
          │                  │
          └──────────┬───────┘
                     ▼
            harness executes or denies
                     │
                     ▼
           authoritative completion event
```

App Server accepts sandbox policy and related settings on operations such as `turn/start`; the last-validated generated schema is the review baseline, while the scheduled drift check detects upstream changes.[^codex-app-server-policy]

The final provider completion event—not merely the approval response—should finalize YakShed’s command or file-change item.

---

## 5. Application-level policy model

YakShed should expose provider-neutral intent and let each adapter translate it.

```rust
struct ExecutionPolicy {
    filesystem: FilesystemPolicy,
    network: NetworkPolicy,
    approvals: ApprovalPolicy,
    reviewer: ReviewerPolicy,
    authority: IsolationAuthority,
}

enum IsolationAuthority {
    HarnessManaged,
    ExternalRuntime,
    Unsupported,
}

enum ApprovalPolicy {
    NeverEscalate,
    OnRequest,
    ProviderDefault,
}

enum ReviewerPolicy {
    User,
    Automatic,
    ExternalPolicyService,
    None,
}
```

For the first release, present a small set of presets and retain an advanced inspector showing the effective translated policy. Do not expose raw Codex enum spellings throughout the frontend.

A connection can also declare minimum requirements:

```rust
struct ConnectionSecurityPolicy {
    minimum_isolation: IsolationRequirement,
    allow_full_access: bool,
    require_user_review_for_network: bool,
    allowed_workspace_roots: Vec<RuntimePath>,
    allowed_git_remotes: Vec<RemotePattern>,
}
```

This allows a Work connection to refuse full access or experimental providers even when a harness technically supports them.

---

## 6. ApprovalBroker

Provider-initiated requests need a first-class Rust subsystem.

```rust
struct ApprovalBroker {
    pending: HashMap<ProviderRequestId, PendingApproval>,
}

struct PendingApproval {
    id: ApprovalId,
    work_item_id: WorkItemId,
    session_binding_id: SessionBindingId,
    runtime_id: RuntimeId,
    provider_request_id: ProviderRequestId,
    operation: ApprovalOperation,
    available_decisions: Vec<ApprovalDecision>,
    created_at: Timestamp,
    status: ApprovalStatus,
}
```

### Nonblocking rule

The protocol reader must not await UI input.

```text
receive server request
    ↓
validate and normalize
    ↓
register pending approval
    ↓
persist projection
    ↓
emit ApprovalOpened
    ↓
return to reading provider messages
```

When the user decides:

```text
resolve_approval(approval_id, decision)
    ↓
confirm request is still pending
    ↓
confirm decision was offered
    ↓
send correlated provider response
    ↓
mark response-sent
    ↓
wait for provider resolution/completion
    ↓
finalize timeline item
```

### State machine

```rust
enum ApprovalStatus {
    Pending,
    Responding,
    Accepted,
    Declined,
    Cancelled,
    Expired,
    ProviderDisconnected,
    OutcomeUnknown,
}
```

Persist enough state to recover the UI after a WebView reload, but do not assume a provider request can always be resumed after the provider process exits. On reconnect, reconcile with provider session state and label uncertain outcomes explicitly.

### Approval UI contents

Display, where available:

- exact command or file operation;
- friendly parsed command actions;
- working directory;
- execution runtime and environment;
- filesystem roots involved;
- requested network access or destinations;
- provider-supplied reason;
- whether the approval is once-only or session-persistent;
- the connection handling the work;
- the repository and work item.

A summary is useful, but the exact operation must remain inspectable.

---

## 7. The three security boundaries

### 7.1 Harness command sandbox

```text
Model
  ↓
Harness tools
  ↓
Codex sandbox / provider equivalent
  ↓
spawned command
```

For Codex, this is where OS-level command enforcement happens. YakShed should pass policy and consume approvals rather than recreate Seatbelt, Linux namespaces, seccomp, Windows tokens, or network mediation.

### 7.2 YakShed application boundary

```text
Svelte WebView
  ↓ narrow Tauri commands
Rust core
  ↓
filesystem, Git, sidecars, credential backends
```

Codex’s sandbox does not protect a generic Rust command such as:

```rust
run_arbitrary_shell_command(command: String)
```

Do not expose such primitives. Use capability-shaped operations:

```text
create_worktree(project_id, work_item_id)
delete_worktree(working_copy_id)
read_artifact(artifact_id)
read_working_copy_file(working_copy_id, relative_path)
resolve_approval(approval_id, decision)
```

For every app-owned path operation:

- canonicalize in Rust;
- verify it remains under the intended root;
- reject symlink/path traversal escapes where relevant;
- authorize against app-domain IDs rather than trusting a frontend path;
- log a redacted audit event for destructive operations.

### 7.3 Outer execution-runtime isolation

```text
Host machine
└── container / VM / remote worker
    └── harness process
        └── harness-sandboxed commands
```

Outer isolation is useful for:

- untrusted repositories;
- unattended automation;
- stronger confidentiality boundaries;
- multi-user or hosted deployment;
- malicious build-script testing;
- remote execution;
- limiting the effect of a provider-process vulnerability.

When a genuine external boundary exists, Codex supports an `externalSandbox` policy so it can avoid duplicating command sandbox enforcement.[^codex-app-server-policy] Do not select that mode merely because the process is supervised; the outer runtime must actually supply the intended filesystem and network restrictions.

For a normal personal desktop v1, use Codex’s built-in sandbox. Make containers, VMs, WSL, and SSH separate execution-runtime options rather than a prerequisite.

---

## 8. Important non-obvious caveats

### 8.1 Workspace write is not automatically workspace-only read

Current App Server policy shapes allow explicit restricted read access, but `workspaceWrite` can otherwise use broad read access by default.[^codex-app-server-policy] Therefore, do not describe the preset as:

> “The agent can only see this repository.”

A more accurate user-facing summary is:

```text
Write: current workspace only
Read: broader host access unless restricted
Network: blocked or approval-gated
```

Distinguish:

- **integrity boundary:** prevent writes outside selected roots;
- **confidentiality boundary:** prevent reads of unrelated host files and credentials.

For strong confidentiality, use restricted read rules or an outer container/VM that mounts only the required data.

### 8.2 Legacy sandbox settings and permission profiles do not compose

Current Codex documentation says named permission profiles and older `sandbox_mode` settings are alternative systems rather than additive layers.[^codex-permissions] YakShed should choose one translation path per supported Codex version and connection configuration. Do not emit both and assume the stricter combination wins.

Because the protocol and policy system evolve, generate schemas from the exact bundled binary and version-gate newer profile features.

### 8.3 Local command policy is not universal tool policy

Codex permission profiles govern local sandboxed command execution. MCP servers, connectors, browser/computer-use surfaces, cloud environments, and approved escalations have separate controls.[^codex-permissions-scope]

YakShed’s eventual permissions UI should avoid one misleading universal toggle:

```text
Permissions
├── Local files and commands
├── Shell network access
├── MCP and apps
├── Browser and computer use
└── Remote/cloud runtime policy
```

### 8.4 Some App Server process APIs are intentionally unsandboxed

App Server exposes `thread/shellCommand` as a user-initiated command path that the last-validated 0.147.0 schema explicitly documents as running unsandboxed with full access.[^codex-unsandboxed] Earlier documentation described a `process/spawn` method; it does not exist in that schema (see `pins/phase0-verification.md`). Two adjacent surfaces need their own classifications rather than inheriting that one:

- The `command/exec` family (`command/exec`, `command/exec/write`, `command/exec/resize`, `command/exec/terminate`) runs standalone commands **in the server sandbox**, accepting a per-request `sandboxPolicy` or `permissionProfile` and defaulting to the configured policy. It is sandboxed execution whose effective risk follows the supplied or default policy — which still means a permissive default policy makes it dangerous, and it bypasses thread/turn approval semantics.
- The `fs/*` RPCs (`fs/readFile`, `fs/writeFile`, `fs/remove`, and related) are direct host filesystem operations and are classified host-privileged.

Treat RPCs by risk class:

```rust
enum ProviderRpcRisk {
    PureRead,
    SandboxedExecution,
    ProviderManagedMutation,
    ExplicitUserUnsandboxed,
    HostPrivileged,
}
```

For v1:

- use normal turn APIs for agent work;
- do not expose `command/exec` or `fs/*` through a generic frontend command;
- expose `thread/shellCommand` only from a clearly user-initiated terminal or `!` workflow;
- visually distinguish user shell commands from agent commands;
- never let model-generated content invoke an unsandboxed path indirectly.

### 8.5 Full access does not need approval mediation

Once the user deliberately selects full access, Codex runs without the normal filesystem/network sandbox boundary. An approval popup cannot retroactively make that equivalent to sandboxed execution. The UI should present full access as an enduring state, not rely on occasional warning dialogs.

### 8.6 Same-user process isolation is weak for secrets

Even when command writes are constrained, a secret placed in the harness process environment can become reachable through process inspection or a sufficiently permissive command path. Prefer short-lived credentials, minimal environments, and outer isolation for sensitive work.

---

## 9. Credential interaction

A sandbox design must include secret egress controls.

A provider runtime may need an API key in its own environment while agent-run commands must not inherit that key. Codex provides `shell_environment_policy` to control environment inheritance for spawned commands.[^codex-shell-env]

A connection-specific Codex configuration should use a minimal environment and explicit exclusions, conceptually:

```toml
[shell_environment_policy]
inherit = "core"
ignore_default_excludes = false

[shell_environment_policy.filters]
"OPENAI_API_KEY" = "exclude"
"ANTHROPIC_API_KEY" = "exclude"
"FIREWORKS_API_KEY" = "exclude"
"AWS_*" = "exclude"
```

YakShed should also construct a deliberate environment for each harness process rather than inheriting every variable from the desktop application.

Rules:

- never place secrets in process arguments;
- never expose an existing secret to the WebView;
- never log or serialize secret values;
- inject only the credential needed by that connection;
- exclude it from agent-spawned child commands;
- stop or restart the relevant runtime after credential rotation when required;
- prefer credential helpers or short-lived leases for work connections.

Sandboxing limits command behavior; it is not a substitute for disciplined secret delivery.

---

## 10. Policy persistence and scope

The user must know whether a permission change applies to:

- only the next run;
- the loaded provider session;
- the YakShed work item;
- the project;
- the connection;
- the global default.

Recommended v1 behavior:

- permission selection is stored on the session binding as the next-run default;
- one-turn overrides are explicit and visually temporary;
- full access never silently becomes the application default;
- connection policy may impose a maximum, such as “Work never allows full access”;
- every effective-policy change produces an app audit event.

```rust
struct SessionSecurityState {
    requested: ExecutionPolicy,
    effective: EffectiveExecutionPolicy,
    source: PolicySource,
    changed_at: Timestamp,
}
```

Store both requested and effective policy. Enterprise configuration, runtime limitations, or provider capabilities may make the effective policy narrower than requested.

---

## 11. Effective-policy UI

The compact control can show a friendly preset, but an inspector should present reality:

```text
Ask for approval

Runtime: local macOS
Isolation: Codex Seatbelt sandbox
Write roots: /Users/rian/code/yakshed
Read scope: full host read
Network: restricted; escalation available
Reviewer: you
Connection: Home Codex
```

For full access, keep a persistent high-salience indicator in the composer and thread header. Do not rely on a one-time confirmation that disappears from view.

For degraded enforcement:

```text
Sandbox unavailable
Linux Bubblewrap/helper could not create the required namespace.
This run has not started.
```

Fail closed rather than silently changing to full access.

---

## 12. Platform behavior and diagnostics

Codex currently documents:

- macOS: built-in Seatbelt;
- native Windows: native Windows sandbox;
- WSL2/Linux: Bubblewrap where available, with a helper fallback and platform prerequisites.[^codex-sandbox]

YakShed should not infer successful enforcement solely from the requested setting. Surface provider startup warnings and retain structured runtime diagnostics:

```rust
struct IsolationDiagnostics {
    requested_authority: IsolationAuthority,
    effective_authority: IsolationAuthority,
    platform_backend: Option<String>,
    healthy: bool,
    warnings: Vec<DiagnosticMessage>,
    checked_at: Timestamp,
}
```

The “run” button should be disabled when a connection requires isolation and the runtime reports that it cannot provide it.

---

## 13. Repository and connection routing

Sandboxing does not prevent sending company code to the wrong model provider. YakShed connections should enforce repository-routing policy independently.

```rust
struct ConnectionPolicy {
    allowed_local_roots: Vec<RuntimePath>,
    forbidden_local_roots: Vec<RuntimePath>,
    allowed_git_remotes: Vec<RemotePattern>,
    allow_unclassified_repositories: bool,
    allow_full_access: bool,
    mismatch_behavior: MismatchBehavior,
}
```

Example:

```text
Home Codex
    allowed: ~/personal/**
    blocked: ~/work/**

Work Claude
    allowed: ~/work/**
    allowed remotes: github.com/company/**

Fireworks Lab
    allowed: ~/experiments/**
    blocked: ~/work/**
```

A mismatch should block the run or require a high-friction override. A subtle badge is not enough for a data-boundary violation.

---

## 14. App-owned operations

Not every operation should pass through the harness. YakShed will own worktree creation, artifact storage, indexing, and application persistence. Those operations need their own authorization model.

Classify them:

```rust
enum AppOperationRisk {
    ReadProjection,
    ReadWorkspace,
    WriteWorkspace,
    DestructiveRepositoryMutation,
    ProcessLaunch,
    CredentialAccess,
}
```

Examples:

| Operation | Required checks |
|---|---|
| Read artifact | artifact belongs to caller-visible work item |
| Read workspace file | canonical path remains under working-copy root |
| Create worktree | repository registered; branch/path conflict checks |
| Delete worktree | no active runs; dirty-tree and reachability checks; explicit confirmation |
| Spawn harness | approved binary, connection, runtime, environment, and credential binding |
| Resolve credential | backend policy, connection scope, no WebView exposure |

Do not claim that Codex sandboxing covers these paths. It does not.

---

## 15. Shutdown and interruption

On application exit or runtime shutdown:

1. Stop accepting new provider operations.
2. Resolve or cancel pending frontend interactions where the provider supports it.
3. Interrupt active turns according to user choice.
4. Terminate child process trees, not only the direct child.
5. Mark unresolved approvals and ambiguous operations as disconnected or outcome-unknown.
6. Reconcile provider state on restart.

On Unix, use a process group/session. On Windows, use a Job Object or equivalent process-tree ownership mechanism. A simple child-handle drop may leave grandchildren created by shells or build tools running.

---

## 16. Security testing

### Protocol tests

Test:

- approval request while deltas continue;
- two simultaneous approvals on different sessions;
- provider request with unknown fields;
- unsupported server request rejection;
- disconnect before and after sending an approval response;
- stale or duplicate frontend decision;
- WebView reload while approval is pending.

### Sandbox contract tests

For each platform and supported Codex version, verify expected behavior for:

- write inside workspace;
- write outside workspace;
- read inside workspace;
- read outside workspace under default and restricted-read modes;
- network blocked, allowed, and approval-gated;
- environment-secret exclusion;
- symlink escape attempts;
- subprocesses and grandchildren;
- missing sandbox prerequisites;
- full-access mode;
- external-sandbox mode inside a controlled container.

Use harmless sentinel files and local test servers rather than real sensitive data.

### Tauri boundary tests

Attempt:

- path traversal;
- symlink traversal;
- arbitrary command injection;
- unauthorized artifact IDs;
- cross-work-item access;
- direct frontend access to credential values;
- invocation of privileged commands from rendered Markdown or links.

### Rendering tests

Treat model output and repository text as untrusted. Sanitize Markdown and HTML, block script execution and inline event handlers, and route external links through an explicit system-browser action.

---

## 17. Recommended v1 behavior

Ship four user-facing presets:

```rust
enum PermissionPreset {
    ReadOnly,
    AskForApproval,
    ApproveForMe,
    FullAccess,
}
```

### Read only

- no autonomous writes;
- no autonomous network;
- suitable for unfamiliar repositories;
- clearly distinguish inability to mutate from inability to read unrelated host paths.

### Ask for approval — default

- workspace write;
- network restricted or approval-gated;
- user-reviewed escalation;
- visible workspace roots and read scope.

### Approve for me

- same sandbox boundary as Ask for approval;
- eligible escalation decisions delegated to the provider’s reviewer;
- visible indication that review is delegated.

### Full access

- no harness command sandbox;
- no ordinary approval boundary;
- explicit confirmation when first selected;
- persistent high-salience state;
- connection policy may prohibit it;
- scoped to the session unless the user deliberately changes a broader setting.

Start with the stable policy mechanism in the last-validated Codex schema. Review newer permission-profile capabilities through schema drift and contract tests; do not reject newer runtimes by version.

---

## 18. Security invariants

1. Agent-generated commands use a harness-managed or explicit external sandbox; they never fall through to a generic YakShed shell primitive.
2. YakShed does not implement Codex’s OS sandbox, but it verifies and surfaces its effective availability.
3. The protocol reader never blocks while waiting for approval.
4. Full access is an explicit, persistent, connection-policy-governed state.
5. Workspace write is not presented as a confidentiality guarantee unless read scope is also restricted.
6. Provider permission profiles, MCP/apps, browser tools, remote execution, and YakShed-owned operations are treated as separate policy surfaces.
7. Unsandboxed App Server APIs are available only from explicit user-initiated flows, or not exposed at all.
8. Secrets needed by a harness are filtered from agent-spawned commands.
9. Frontend code receives narrow domain capabilities rather than shell, arbitrary path, or credential primitives.
10. Missing or degraded sandbox enforcement fails closed when the connection requires isolation.
11. Repository-to-connection routing is enforced independently of local command sandboxing.
12. Every security decision records requested policy, effective policy, scope, and source without recording secret material.

---

## References

[^codex-sandbox]: OpenAI, [Sandbox](https://learn.chatgpt.com/docs/sandboxing). Describes current permission presets, platform enforcement, and Linux/WSL prerequisites.

[^codex-app-server-policy]: OpenAI, [Codex App Server](https://learn.chatgpt.com/docs/app-server). Documents turn-level `sandboxPolicy`, explicit read-access controls, `command/exec`, and `externalSandbox`.

[^codex-permissions]: OpenAI, [Permissions](https://learn.chatgpt.com/docs/permissions). Documents named permission profiles and their non-composition with legacy sandbox settings.

[^codex-permissions-scope]: OpenAI, [Permissions — scope and enforcement](https://learn.chatgpt.com/docs/permissions). Local permission profiles govern sandboxed commands; MCP, connectors, browser/computer use, cloud environments, and approved escalations have separate controls.

[^codex-unsandboxed]: OpenAI, [Codex App Server](https://learn.chatgpt.com/docs/app-server), and [openai/codex app-server README](https://github.com/openai/codex/blob/main/codex-rs/app-server/README.md). Current documentation describes `process/spawn` and `thread/shellCommand` as unsandboxed/full-access paths.

[^codex-shell-env]: OpenAI, [Advanced Configuration](https://learn.chatgpt.com/docs/config-file/config-advanced). `shell_environment_policy` controls environment variables inherited by commands spawned through Codex.
