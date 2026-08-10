# YakShed: Working with Secrets

> **Status:** normative backend standard  
> **Research snapshot:** 2026-08-09  
> **Applies to:** every YakShed connection, harness adapter, model-provider adapter, runtime, MCP integration, Git-host integration, and Tauri command that can cause credential material to enter memory

The words **MUST**, **MUST NOT**, **SHOULD**, and **MAY** describe requirements for the YakShed implementation.

## 1. Decision

YakShed is not a universal authentication service and should not become one.
It needs a narrow credential-control layer that supports two fundamentally different authority models:

1. **Delegated credentials** — the harness owns sign-in, storage, refresh, logout, and token use.
   YakShed starts and observes the flow but never receives the underlying credential. Codex App Server
   subscription authentication is the first example.
2. **Secret-backed credentials** — YakShed resolves an access secret from an explicitly configured
   backend and delivers it only to the component that requires it. Anthropic and Fireworks API keys
   are initial examples.

Credential authority is selected **per credential requirement**, not once for the whole connection.
A connection may therefore be delegated, secret-backed, or hybrid.

```text
Connection: Home Codex
└── codex.account
    └── delegated → Codex App Server

Connection: Work Claude
└── anthropic.api_key
    └── secret reference → company helper / OS store / 1Password

Connection: Fireworks Lab
├── codex.harness_state
│   └── provider-owned state, no YakShed secret
└── fireworks.api_key
    └── secret reference → selected secret backend
```

YakShed MUST store references and non-secret metadata. It MUST NOT store delegated OAuth tokens or
secret-backed credential values in application config, SQLite, cache, artifacts, frontend state, or logs.

---

## 2. Goals

- Runtime access secrets are resolved from an explicit authority and are never hidden in ordinary config.
- Delegated harness authentication remains delegated; YakShed does not copy provider-owned tokens.
- A user can maintain multiple home, work, and experimental identities without collisions.
- A single connection can use more than one credential source when the runtime genuinely requires it.
- OS keyrings, 1Password, command helpers, environment-backed development credentials, and test memory stores
  fit behind one small YakShed contract.
- Read-only secret sources remain read-only; the abstraction does not pretend every backend supports writes.
- Locked, denied, unavailable, and unauthenticated backends remain distinguishable so the UI can offer the correct remedy.
- Secret delivery is narrow, auditable, cancellable, and short-lived.
- Tests can exercise the entire credential contract without touching a developer's real keyring.

## 3. Non-goals

- Protecting against root, Administrator, or a malicious process already running as the same OS user.
- Making API keys safe in a fully unrestricted child process. A child that receives a key can use it.
- Inventing a new encrypted vault format in the first implementation.
- Supporting every commercial secret manager directly in-process.
- Exposing a generic secret-read API to the WebView.
- Mirroring every capability of every backend behind one lowest-common-denominator trait.
- Treating an OS keyring as a process sandbox. It is safer storage, not execution isolation.

---

## 4. Threat model

This standard is primarily intended to prevent:

- accidental disclosure through committed config or SQLite files;
- secrets appearing in logs, crash reports, screenshots, diagnostics, shell history, or process arguments;
- one YakShed connection reading, clearing, or overwriting another connection's credentials;
- ambient environment variables silently selecting the wrong account;
- a locked or denied Linux keyring silently falling back to a weaker store;
- the frontend or an untrusted rendered document obtaining a secret-read capability;
- agent-run commands inheriting credentials that only the harness process needs;
- credential-helper command injection or unbounded helper output;
- long-lived, application-wide in-memory secret caches;
- silent conflict resolution between two different values that both claim to be authoritative.

It does not defend against:

- an attacker with control of the OS account;
- a compromised provider binary to which YakShed intentionally delivers a credential;
- a provider or model service mishandling data after it leaves the machine;
- a user explicitly choosing full access and running untrusted code with that authority.

---

## 5. Terminology

### 5.1 Access secret

A value that grants access when possessed: API key, bearer token, OAuth refresh token, private key,
service-account token, or equivalent. Shared organization-wide tokens are still access secrets.

### 5.2 Deployment material

Stable non-secret material distributed to configure authentication but insufficient to grant access by itself:
provider URLs, tenant IDs, OAuth client identifiers intended for desktop distribution, account selectors,
vault names, and helper executable paths. Deployment material belongs in config if it is safe for internal
version control and machine management.

### 5.3 Credential requirement

A stable requirement declared by a harness or provider adapter.

```rust
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct CredentialSlot(pub String);

pub struct CredentialRequirement {
    pub slot: CredentialSlot,
    pub display_name: String,
    pub required: bool,
    pub delivery: CredentialDelivery,
}
```

Examples:

```text
codex.account
anthropic.api_key
fireworks.api_key
github.token
mcp.linear.oauth
runtime.ssh.private_key
```

The slot identifies a logical capability, not a physical keyring entry.

### 5.4 Credential binding

The connection-specific decision about who owns a requirement.

```rust
pub enum CredentialBinding {
    Delegated {
        authority: DelegatedAuthority,
    },
    Secret {
        reference: SecretReference,
    },
    Disabled,
}
```

### 5.5 Secret backend

A configured resolver such as `local-os`, `onepassword-work`, `company-helper`, or `environment-dev`.
The backend has a stable YakShed `SecretBackendId` and non-secret configuration.

### 5.6 Secret reference

A backend ID plus an opaque locator interpreted only by that backend.

```rust
pub struct SecretReference {
    pub backend_id: SecretBackendId,
    pub locator: SecretLocator,
}

pub struct SecretLocator(String);
```

The rest of YakShed MUST NOT parse a locator to infer account, vault, service, or key semantics.
The backend validates its own locator grammar.

### 5.7 Resolved secret / secret lease

A short-lived value returned by a resolver for one stated purpose. It is not serializable application state.

---

## 6. What goes where

| Value | Location | Examples |
|---|---|---|
| Access secret | delegated harness store or selected secret backend | API keys, refresh tokens, private keys |
| Secret reference | YakShed config | backend ID + locator |
| Connection metadata | YakShed config | display name, harness, provider URL, account label |
| Provider-owned auth state | provider-owned state root | Codex auth and session state |
| Derived auth status | memory / rebuildable projection | connected, expired, unlock required |
| Deployment material | YakShed config or managed file | tenant, base URL, vault, helper path |
| Short-lived resolved value | Rust memory only | `SecretString` / `SecretLease` |
| Presence/status | frontend-safe DTO | present, missing, delegated, locked |

A serializable config type MUST NOT contain `SecretString`, raw token fields, or fields named as a
legacy plaintext credential. Secret-bearing ingress DTOs MUST be distinct from persisted config DTOs.

---

## 7. Connection model

A connection binds trust, billing, runtime, provider state, and credential decisions.

```rust
pub struct Connection {
    pub id: ConnectionId,
    pub name: String,
    pub harness: HarnessKind,
    pub model_provider: ModelProviderKind,
    pub execution_runtime: ExecutionRuntimeId,
    pub provider_state_root: ProviderStateRootId,
    pub credential_bindings: Vec<CredentialBindingRecord>,
    pub policy: ConnectionPolicy,
}

pub struct CredentialBindingRecord {
    pub slot: CredentialSlot,
    pub binding: CredentialBinding,
}
```

A provider session MUST remain associated with the connection that created it. Switching connections
creates another provider session binding rather than silently changing the credentials behind an existing session.

### Example non-secret config

```toml
schema_version = 1

[[secret_backends]]
id = "local-os"
kind = "local-os"

[[secret_backends]]
id = "onepassword-work"
kind = "onepassword-cli"
account = "work"

[[connections]]
id = "0193f26e-7a72-7d42-bf77-0de14c4cc111"
name = "Home"
harness = "codex"
model_provider = "openai"
provider_state = "home-codex"

[[connections.credentials]]
slot = "codex.account"
source = "delegated"
authority = "codex-app-server"

[[connections]]
id = "0193f26e-7a72-7d42-bf77-0de14c4cc222"
name = "Work"
harness = "claude-code"
model_provider = "anthropic"
provider_state = "work-claude"

[[connections.credentials]]
slot = "anthropic.api_key"
source = "secret"
backend = "onepassword-work"
locator = "op://Engineering/YakShed Work/Anthropic API Key"

[[connections]]
id = "0193f26e-7a72-7d42-bf77-0de14c4cc333"
name = "Lab"
harness = "codex"
model_provider = "fireworks"
provider_state = "lab-codex"

[[connections.credentials]]
slot = "fireworks.api_key"
source = "secret"
backend = "local-os"
locator = "connection/0193f26e-7a72-7d42-bf77-0de14c4cc333/fireworks_api_key"
```

The locator is safe to show. The referenced value is not.

---

## 8. Public Rust interfaces

The runtime path needs less authority than the settings/admin path. Keep those interfaces separate.

```rust
#[async_trait::async_trait]
pub trait SecretResolver: Send + Sync {
    fn descriptor(&self) -> SecretBackendDescriptor;

    async fn probe(&self) -> Result<SecretBackendStatus, SecretError>;

    async fn resolve(
        &self,
        locator: &SecretLocator,
        context: &SecretAccessContext,
    ) -> Result<ResolvedSecret, SecretError>;
}

#[async_trait::async_trait]
pub trait SecretAdministrator: Send + Sync {
    async fn put(
        &self,
        locator: &SecretLocator,
        value: &secrecy::SecretString,
        options: PutSecretOptions,
    ) -> Result<PutSecretOutcome, SecretError>;

    async fn delete(
        &self,
        locator: &SecretLocator,
    ) -> Result<DeleteSecretOutcome, SecretError>;
}

pub struct SecretBackendHandle {
    pub resolver: std::sync::Arc<dyn SecretResolver>,
    pub administrator: Option<std::sync::Arc<dyn SecretAdministrator>>,
}
```

Do not force environment, 1Password, or company-helper backends to implement writes they cannot or should not perform.

### 8.1 CredentialBroker

```rust
pub struct CredentialBroker {
    backends: std::collections::HashMap<SecretBackendId, SecretBackendHandle>,
    locks: KeyedSecretLocks,
    audit: std::sync::Arc<dyn SecretAuditSink>,
}
```

The broker owns:

- backend lookup;
- reference and slot validation;
- delegated-versus-secret-backed dispatch;
- just-in-time resolution;
- timeout and cancellation policy;
- same-reference operation serialization;
- error normalization;
- secret-safe audit records;
- delivery-specific shaping.

The broker MUST NOT own provider login flows. A delegated authority has a separate provider adapter.

### 8.2 ResolvedSecret

```rust
pub struct ResolvedSecret {
    value: secrecy::SecretString,
    pub source: ResolvedSecretSource,
    pub expires_at: Option<time::OffsetDateTime>,
}
```

`ResolvedSecret` MUST NOT implement `Serialize`, ordinary `Display`, or ordinary `Debug`.
It SHOULD NOT implement `Clone`. Exposure must be explicit and local.

```rust
impl ResolvedSecret {
    pub fn expose<R>(&self, use_value: impl FnOnce(&str) -> R) -> R {
        use secrecy::ExposeSecret;
        use_value(self.value.expose_secret())
    }
}
```

The `secrecy` crate limits accidental formatting/serialization and attempts to zeroize wrapped values on drop.
This is defense in depth; it cannot remove copies made by the OS, a provider SDK, or a child process environment.

### 8.3 Secret access context

Every resolution must name its purpose.

```rust
pub struct SecretAccessContext {
    pub connection_id: ConnectionId,
    pub slot: CredentialSlot,
    pub purpose: SecretAccessPurpose,
    pub request_id: OperationId,
}

pub enum SecretAccessPurpose {
    StartHarness,
    RefreshHarness,
    ProviderHttpRequest,
    GitHostRequest,
    McpConnection,
    ValidateCredential,
}
```

A secret access audit event records metadata and outcome, never the value or a reversible encoding of it.

---

## 9. Initial backend set

### 9.1 `local-os`

Use the Rust keyring ecosystem behind a YakShed adapter:

```text
keyring-core
├── apple-native-keyring-store
├── windows-native-keyring-store
└── dbus-secret-service-keyring-store
```

YakShed should construct and retain explicit stores rather than rely on a mutable process-global default.
`keyring-core` specifically supports direct store construction for applications that need precise control over multiple stores.

Initial platform policy:

| Platform | Store |
|---|---|
| macOS | Keychain Services (`apple-native-keyring-store`, `keychain`) |
| Windows | Windows Credential Manager |
| Linux | Secret Service over D-Bus with Rust crypto |

Use a stable service and per-connection account mapping:

```text
service: dev.yakshed.YakShed
account: connection/<connection-id>/<credential-slot>
```

The config locator can be the account portion. The backend owns the physical mapping and user-facing label.

#### Linux failure behavior

- Secret Service available and unlocked: use it.
- Secret Service locked or access denied: fail closed with actionable remediation.
- Secret Service response ambiguous: fail closed.
- Secret Service absent: report unavailable and offer an explicitly configured alternative.

YakShed v1 MUST NOT silently fall back to memory, plaintext, a sample store, or an encrypted file.
An encrypted-file backend can be added later as an explicit store with an explicit unlock lifecycle.

### 9.2 `onepassword-cli`

Use the official `op` CLI in the first implementation.

- Read with `op read <secret-reference>`.
- Use `tokio::process::Command`, never a shell.
- Pass the reference as one argument.
- Bound stdout and stderr.
- Enforce a timeout and terminate the process tree on cancellation.
- Treat stdout as secret material immediately.
- Keep stderr only after redaction.
- Support an explicit account selector.
- Remove stale `OP_SESSION_*` variables from the spawned helper environment so desktop integration can be used.
- Prefer desktop integration for interactive local use and service accounts for least-privilege automation.

The initial backend SHOULD be read-only. Writing arbitrary 1Password items creates product and policy decisions
about vault structure, item ownership, and overwrite semantics that are not required for initial runtime resolution.

A service-account token used to authenticate `op` is itself an access secret. In v1 it may be supplied by a managed
launch environment or a separate explicitly configured bootstrap source. Do not build a recursive, arbitrary secret-backend
dependency graph until a real deployment requires it.

### 9.3 `command-helper`

This is the extensibility escape hatch for Vault, cloud secret managers, corporate identity brokers, rotating credentials,
or organization-specific policy.

Non-secret config:

```toml
[[secret_backends]]
id = "company-helper"
kind = "command-helper"
program = "/usr/local/bin/yakshed-credential-helper"
timeout_seconds = 15
```

YakShed sends one JSON request on stdin:

```json
{
  "version": 1,
  "operation": "resolve",
  "locator": "llm/anthropic/work",
  "context": {
    "connection_id": "0193f26e-7a72-7d42-bf77-0de14c4cc222",
    "slot": "anthropic.api_key",
    "purpose": "start_harness"
  }
}
```

The helper returns one bounded JSON object on stdout:

```json
{
  "version": 1,
  "value": "secret-value",
  "expires_at": "2026-08-09T20:00:00Z"
}
```

The value is secret. It MUST NOT be logged, echoed, persisted, or included in errors.

Requirements:

- absolute or trusted managed executable path;
- no shell evaluation;
- controlled environment inheritance;
- bounded request, stdout, and stderr sizes;
- strict protocol version and schema validation;
- timeout, cancellation, and process-tree cleanup;
- no helper configuration from an untrusted repository;
- executable and locator visible in settings, but not secret output;
- helper failures normalized to YakShed error categories.

### 9.4 `environment`

An explicit, read-only backend for development, CI, Nix shells, and managed launch wrappers.

```toml
[[secret_backends]]
id = "ci-env"
kind = "environment"

[[connections.credentials]]
slot = "fireworks.api_key"
source = "secret"
backend = "ci-env"
locator = "FIREWORKS_API_KEY"
```

Environment variables MUST NOT form a hidden precedence chain. The connection explicitly selects the backend and variable.
The UI must report that the credential is process-scoped and non-persistent.

### 9.5 `memory`

A deterministic in-memory backend for unit, integration, and contract tests.

- no persistence;
- safe parallel test isolation;
- programmable error responses;
- never auto-selected;
- refused in ordinary release configuration unless a dedicated development feature is enabled.

The `keyring-core` mock store can help test the native-keyring adapter, but YakShed should also provide its own memory backend
so the broker contract and application composition do not depend on keyring implementation details.

### 9.6 Optional `secretspec`

SecretSpec is useful prior art and may become an optional adapter for long-tail providers. It supports a broad set of
secret managers and uses Rust-native provider abstractions. YakShed MUST keep its own domain contract in front of it.

Before adoption, a spike must establish:

- providers can be instantiated without adopting SecretSpec as YakShed's config model;
- default features can be disabled to avoid unnecessary cloud SDKs and binary growth;
- all provider credentials can themselves be supplied without plaintext config;
- errors map cleanly to YakShed categories;
- timeout, cancellation, and GUI-process behavior are acceptable;
- no secret-bearing type crosses into application serialization.

SecretSpec is an implementation option, not the YakShed product model.

### 9.7 `local-file`

An explicit plaintext developer-convenience store for long-running local development without repeated OS-keychain prompts.

```toml
[[secret_backends]]
id = "dev-local"
kind = "local-file"
path = "/Users/example/.local/share/yakshed/dev-secrets.json"
```

- compiled only with the non-default `dev-secrets` Cargo feature;
- supported only on Unix, where YakShed enforces private directory and file permission bits;
- configured with an absolute path so behavior never depends on the process working directory;
- allowed only for deliberate development builds and never auto-selected or used as a fallback;
- refused with a typed configuration error naming the missing feature when ordinary builds reference it;
- refused with a distinct typed unsupported-platform error when the feature is compiled on non-Unix targets;
- stores plaintext secrets in a private local file and is not suitable for release configuration.

Backend instances targeting the same canonical file share a process-global lock and an exclusive Unix `flock`, held for
each complete read or read-modify-write operation. Dropping a queued mutation prevents it from starting; dropping one after
its write begins leaves an uncertain outcome that callers must reconcile before retrying. Reads wait for a contended flock;
mutations poll the flock non-blockingly so abandonment cancels the wait promptly.

Private mode bits are necessary but not sufficient: the store file and its direct parent must also have no extended ACL
entries. YakShed inspects native extended ACLs on macOS and the POSIX ACL xattrs on Linux before operations and re-checks the
store after atomic replacement. It refuses ACL-bearing paths with remediation rather than silently removing user-managed
filesystem security state. The direct parent and store file must be owned by the effective user; opened store descriptors
are revalidated for type, mode, owner, and path identity before reads.

Removing or resetting a backend's config intentionally retains its plaintext file. Re-adding the same backend ID and path
restores access to those values; manually delete the file to purge them.

The future development launch script must pass `--features dev-secrets`, mirroring Retune's `build-install.sh` pattern.

---

## 10. Backend registration and selection

Secret backends are configured by stable IDs. Connections select a backend explicitly.

```rust
pub struct SecretBackendConfig {
    pub id: SecretBackendId,
    pub kind: SecretBackendKind,
    pub settings: SecretBackendSettings,
}
```

Selection rules:

1. The credential binding names the backend ID.
2. Unknown backend IDs are configuration errors.
3. Unavailable backends fail visibly; no implicit store substitution occurs.
4. Memory is never an automatic fallback.
5. Discovery may report available backends, but discovery does not mutate config.
6. A backend may be read-only, write-only for ingress, or read/write; the UI reflects actual capabilities.
7. Backend authentication state is not equivalent to target credential presence.

The settings UI may offer discovery levels:

```text
Off   — do not probe external programs or accounts
Safe  — check executable/store availability without enumerating vaults or accounts
Full  — user-requested inventory such as 1Password accounts and vaults
```

Full discovery must be an explicit user action because it can prompt, unlock, or reveal organizational metadata.

---

## 11. Delegated authentication

A delegated binding means the provider process owns credential state.

For Codex App Server:

```text
YakShed starts App Server with the connection's isolated CODEX_HOME
    ↓
account/read
    ↓
account/login/start when requested
    ↓
open returned browser URL
    ↓
observe account/login/completed and account/updated
```

YakShed stores:

- connection ID;
- provider state-root ID;
- account display metadata reported by the provider;
- plan/auth mode reported by the provider;
- last successful status check;
- health and reauthentication-required state.

YakShed does not store:

- access tokens;
- refresh tokens;
- the provider's auth file contents;
- copies of provider keyring entries.

Separate delegated identities use separate provider state roots and normally separate provider processes.
A native provider `--profile` or config overlay is not assumed to be an authentication-isolation boundary.

Logout is sent to the provider adapter. YakShed does not delete provider auth files directly.

---

## 12. Secret ingress

The user may need to paste an API key into YakShed. This creates one intentionally narrow secret-bearing IPC path.

```rust
#[tauri::command]
async fn set_connection_credential(
    connection_id: ConnectionIdDto,
    slot: CredentialSlotDto,
    input: SecretIngressDto,
) -> Result<CredentialStatusDto, DesktopErrorDto>;
```

Rules:

- the command is write-only; no corresponding `get_secret` command exists;
- the connection and slot determine which backends and locators are valid;
- the secret is converted to `SecretString` immediately in Rust;
- it is written directly to the selected administrator;
- the frontend clears its input after completion;
- the response reports status, reference, and backend, never the value;
- no event payload contains the value;
- validation errors never echo the submitted string;
- Tauri tracing must skip the secret argument;
- clipboard clearing may be offered but must not be promised as complete protection.

Installer or CLI ingress should accept stdin or an explicitly named environment variable, never a secret-valued command-line flag.

### Overwrite semantics

Writing an existing logical secret without explicit overwrite MUST fail with `AlreadyExists`.
A replacement operation must be visibly intentional.

### Conflicts

If a legacy plaintext value and an existing backend value differ, YakShed MUST NOT pick a winner by precedence.
It reports both source locations without either value and requires an explicit keep/replace decision.

---

## 13. Resolution and delivery

### 13.1 Resolve just in time

The runtime supervisor requests the credential immediately before it is needed.

```text
Runtime needs anthropic.api_key
    ↓
CredentialBroker validates connection + slot + binding
    ↓
selected resolver returns ResolvedSecret
    ↓
value is delivered to exactly one consumer
    ↓
local lease drops
```

YakShed SHOULD NOT maintain a global secret cache. A backend may internally cache an authenticated session.
A short-lived resolved lease is allowed only when a concrete provider flow requires it and must honor expiration.

### 13.2 Child-process environment delivery

When a harness requires an environment variable:

- start from a deliberate environment, not blind inheritance;
- copy only runtime essentials such as `PATH`, home, locale, and temporary-directory variables;
- add only credentials required by that connection;
- never place credentials in argv;
- ensure agent-run child commands use the harness's environment-filter policy so provider API keys are not inherited unnecessarily;
- restart the harness after rotation if it only reads the value at startup.

```rust
pub enum CredentialDelivery {
    HarnessManaged,
    ProcessEnvironment { variable: String },
    HttpBearer,
    ProviderNative,
}
```

The adapter owns the final delivery translation. The domain does not know that Anthropic uses `ANTHROPIC_API_KEY`.

### 13.3 In-process HTTP delivery

For future direct API adapters, construct authorization headers in Rust immediately before sending the request.
Do not put bearer values into URL query strings, reusable config structs, debug request dumps, or telemetry.

---

## 14. Concurrency and lifetime

`keyring-core` is thread-safe at its own layer but warns that underlying stores may not reliably sequence simultaneous operations
on the same credential. YakShed must serialize operations by normalized secret reference.

```text
same backend + same locator → one keyed lock
unrelated references         → may proceed concurrently
```

Do not hold the keyed lock while waiting for unrelated user interaction.

Resolved values should live for the smallest practical scope. Dropping a `SecretString` is not a guarantee that all copies are gone;
it is still preferable to storing values in ordinary `String`s throughout the application.

---

## 15. Error model

Do not flatten secret failures into generic strings.

```rust
pub enum SecretError {
    NotFound { reference: SecretReferenceSummary },
    AlreadyExists { reference: SecretReferenceSummary },
    BackendUnavailable { backend: SecretBackendId, remediation: Option<String> },
    LockedOrDenied { backend: SecretBackendId, remediation: Option<String> },
    AuthenticationRequired { backend: SecretBackendId, action: AuthenticationAction },
    InvalidLocator { backend: SecretBackendId, reason: String },
    UnsupportedOperation { backend: SecretBackendId, operation: SecretOperation },
    TimedOut { backend: SecretBackendId },
    Cancelled { backend: SecretBackendId },
    ProtocolViolation { backend: SecretBackendId, reason: String },
    BackendFailure { backend: SecretBackendId, redacted_message: String },
}
```

The UI implications differ:

| Error | UI response |
|---|---|
| Not found | Offer set/select credential |
| Authentication required | Offer sign in or unlock |
| Locked or denied | Explain how to unlock; do not downgrade |
| Backend unavailable | Offer explicit alternative configuration |
| Invalid locator | Edit reference |
| Unsupported operation | Explain read-only/write-only limitation |
| Timeout/cancelled | Retry without assuming mutation outcome |
| Protocol violation | Mark helper/backend incompatible |

A mutating operation that times out after dispatch may have an uncertain outcome. Re-probe before retrying blindly.

---

## 16. Display, logging, telemetry, and support bundles

YakShed may display:

- backend ID and kind;
- secret reference/locator;
- delegated authority;
- presence state;
- expiration time if supplied;
- last successful access time;
- remediation guidance;
- connection and credential slot.

YakShed must never display or emit:

- full values;
- masked prefixes or suffixes;
- hashes intended to help identify real credentials;
- values in panic messages;
- helper stdout;
- authorization headers;
- secret-bearing environment dumps;
- serialized `SecretIngressDto` values.

Synthetic test values may be hashed inside the test-only contract host. Production diagnostics must not expose credential fingerprints.

Logging rules:

- mark secret-bearing function arguments with tracing skip annotations;
- use structured fields containing only IDs and status;
- cap and redact helper/provider stderr;
- strip secrets from crash-report attachments;
- require an explicit diagnostic mode for raw protocol traces and still run redaction;
- never upload source code, prompts, diffs, command output, or credentials by default.

---

## 17. Clear, rotate, and delete semantics

Operations are scoped to one connection and credential slot unless an explicitly broader administrative operation says otherwise.

| Operation | Effect |
|---|---|
| Disconnect delegated account | Provider adapter logs out that provider state root |
| Clear credential slot | Delete selected secret reference if backend supports delete |
| Rotate credential | Explicit overwrite or new reference, then runtime restart/refresh |
| Delete connection | Requires a plan for config, provider state, secret references, sessions, and data; no silent cascading |
| Reset YakShed config | Does not implicitly delete secrets or durable work data |
| Purge YakShed data | Does not implicitly delete external secrets |

A read-only external secret reference can be detached from the connection, but YakShed cannot promise to delete the external value.
The UI must distinguish “remove binding” from “delete secret at source.”

---

## 18. Migration rules

When migrating a legacy plaintext credential:

1. collect every candidate source;
2. compare values in memory without logging them;
3. if distinct candidates exist, fail as a conflict;
4. if a backend value already exists and differs, fail as a conflict;
5. on explicit user choice, write the selected value to the target backend;
6. atomically rewrite config without plaintext fields;
7. report the migration using source and destination references only;
8. ensure the plaintext source is removed before ordinary runtime resolution continues.

Migration must be idempotent. A crash after backend write but before config rewrite must be detected and reconciled on the next start.

---

## 19. Tauri boundary

The WebView may:

- list configured backends;
- request safe/full discovery;
- configure non-secret backend settings;
- show credential status;
- initiate delegated login/logout;
- submit a new secret through a narrow write-only command;
- detach or delete a binding with explicit scope.

The WebView may not:

- resolve or retrieve a stored secret;
- choose an arbitrary executable and ask it to resolve a secret;
- receive provider OAuth tokens;
- inspect child-process environments;
- enumerate an entire keychain or vault by default;
- invoke generic keyring operations;
- receive raw backend errors that could contain secret output.

Tauri commands call application-level use cases. Secret backends do not depend on Tauri.

---

## 20. Testing requirements

### Unit tests

Every backend and broker component must test:

- locator validation;
- not-found, exists, unsupported, locked/denied, unavailable, timeout, and cancellation mapping;
- no-overwrite behavior;
- same-reference serialization;
- redacted `Debug`/error output;
- delegated bindings never invoke secret resolution;
- disabled bindings fail before backend access;
- invalid connection/slot combinations fail closed;
- secret-bearing types cannot be serialized through ordinary app DTOs.

### Integration tests

Use the YakShed memory backend to test:

- independent connection/slot namespaces;
- write, status, resolve, rotate, and delete flows;
- broker-to-runtime delivery through a fake harness;
- no secret in config, SQLite, cache, artifacts, events, or captured logs;
- restart behavior: references persist, memory values do not;
- cancellation and uncertain mutation reconciliation;
- Tauri facade exposes write/status operations but no read operation.

### Native backend tests

Run separately on native CI runners:

- macOS Keychain set/get/delete using a test service namespace;
- Windows Credential Manager set/get/delete;
- Linux Secret Service when a test session is available;
- locked/denied Linux classification where practical;
- no test touches the user's production YakShed namespace.

These tests are not part of the cheapest universal lane and may require platform setup.

### Contract test

The test-only `yakshed-contract-host` and `scripts/backend_contract_test.py` provide a cross-module acceptance test using:

- temporary `AppPaths`;
- memory secrets;
- real config and SQLite implementations;
- a fake child harness;
- no Tauri and no network.

See `../contracts/backend-contract-v1.md`.

---

## 21. Definition of Done

A credential-related module is complete only when:

- its public contract and error taxonomy are documented;
- secret-bearing types are non-serializable and redacted;
- unit tests cover success and failure paths;
- a memory-backed integration test proves the behavior;
- no production IPC was added merely for testing;
- the secret-boundary reviewer has no unresolved blocking or major findings;
- logs and support bundles have been inspected with known canary secret values;
- cleanup/rotation behavior is explicit;
- platform-specific behavior is either tested or clearly gated as unsupported.

---

## 22. Research references

- Open CLI Collective secret-handling standard: `https://github.com/open-cli-collective/cli-common/blob/main/docs/working-with-secrets.md`
- Rust keyring core: `https://docs.rs/keyring-core/latest/keyring_core/`
- macOS/iOS keyring adapter: `https://docs.rs/apple-native-keyring-store/latest/apple_native_keyring_store/`
- Windows keyring adapter: `https://docs.rs/windows-native-keyring-store/latest/windows_native_keyring_store/`
- Linux Secret Service adapter: `https://docs.rs/dbus-secret-service-keyring-store/latest/dbus_secret_service_keyring_store/`
- `secrecy`: `https://docs.rs/secrecy/latest/secrecy/`
- 1Password CLI secret references: `https://developer.1password.com/docs/cli/secrets-scripts`
- SecretSpec: `https://github.com/cachix/secretspec`
