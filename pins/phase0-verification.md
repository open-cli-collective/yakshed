# Phase 0 verification: Codex 0.147.0 facts vs. spec claims

> **Date:** 2026-08-09
> **Binary:** codex-cli 0.147.0 (Homebrew install, aarch64-apple-darwin)
> **Method:** JSON-schema artifacts generated from the exact binary
> (`codex app-server generate-json-schema`), plus `--strict-config` probes.
> Where the binary disagrees with the architecture docs, the binary wins.

## Claims verified against the generated schema

| Spec claim | Verdict | Evidence |
|---|---|---|
| App Server is a JSON-RPC-like bidirectional protocol over stdio | **Confirmed** | `JSONRPCRequest/Response/Notification/Error` definitions; `ServerRequest.json` defines server-initiated requests |
| Server-initiated approval requests exist | **Confirmed** | `ExecCommandApproval`, `ApplyPatchApproval`, `CommandExecutionRequestApproval`, `FileChangeRequestApproval`, `PermissionsRequestApproval`, `ToolRequestUserInput`, `McpServerElicitationRequest` in `ServerRequest.json` |
| `externalSandbox` policy exists | **Confirmed** | `SandboxPolicy` oneOf: `dangerFullAccess`, `readOnly`, `externalSandbox`, `workspaceWrite` (v2 schema) |
| `workspace-write` is broad-read by default with explicit restricted-read available | **Confirmed** | `WorkspaceWriteSandboxPolicy` has `writableRoots`/`networkAccess` but no read fields; restricted read is expressed via `AdditionalFileSystemPermissions.entries[]` (`FileSystemSandboxEntry { path, access: FileSystemAccessMode }`); legacy `read`/`write` arrays are marked "will be removed in favor of `entries`" |
| `thread/shellCommand` is a user-initiated full-access command path | **Confirmed** | Method present in `ClientRequest.json` |
| `process/spawn` is an unsandboxed host process API | **CORRECTED** | No `process/spawn` method exists in 0.147.0. `thread/shellCommand` is the surface the schema explicitly documents as unsandboxed/full-access. The `command/exec` family runs "in the server sandbox" with per-request `sandboxPolicy`/`permissionProfile` (defaulting to configured policy) — sandboxed execution whose risk follows the effective policy, not host-privileged by construction. Direct host-privileged surfaces are the `fs/*` RPCs. `sandboxing.md` §8.4 updated in this commit |
| Permission profiles exist alongside sandbox modes | **Confirmed (schema level)** | `permissionProfile/list` method; `RequestPermissionProfile` / `GrantedPermissionProfile` / `PermissionGrantScope` (`turn`\|`session`) definitions. The non-composition claim (profiles vs. legacy `sandbox_mode`) is a config-system behavior not expressible in the schema; keep version-gated per `sandboxing.md` §8.2 |
| `shell_environment_policy` controls child-command env inheritance | **Confirmed (key exists)** | `codex exec --strict-config -c shell_environment_policy.inherit="core"` passes config validation (strict mode rejects unknown keys). Behavioral verification (exclusion actually applied to spawned commands) belongs to the phase-5 sandbox contract tests |
| Native thread fork available | **Confirmed** | `thread/fork` method present |
| Mid-run steering available | **Confirmed** | `turn/steer` method present alongside `turn/start` / `turn/interrupt` |
| Delegated auth via App Server | **Confirmed** | `account/login/start`, `account/login/cancel`, `account/logout`, `account/read` methods |
| Model discovery | **Confirmed** | `model/list`, `modelProvider/capabilities/read` methods |

## Client method surface (0.147.0)

95 client-request methods. Groups relevant to YakShed v1: `account/*`,
`thread/*` (start/resume/read/list/fork/archive/delete/rollback/inject_items/
shellCommand), `turn/*` (start/steer/interrupt), `model/list`,
`permissionProfile/list`, `config/*`, `command/exec*` (sandboxed execution
under the supplied or default policy; bypasses thread/turn approval semantics —
see `sandboxing.md` §8.4), `fs/*` (direct host filesystem RPCs —
host-privileged; YakShed must not expose these), `review/start`,
`fuzzyFileSearch`, `windowsSandbox/*`.

Methods that exist but are outside v1 scope and MUST NOT leak into the Tauri
surface: `plugin/*`, `marketplace/*`, `app/*`, `hooks/*`, `skills/*`,
`externalAgentConfig/*`, `feedback/upload`, `attestation`.

## Schema artifact

- `pins/codex-app-server-schema/` is the generated output, committed verbatim
  (285 files, including combined `codex_app_server_protocol.schemas.json` and
  `.v2.schemas.json`).
- Provenance: the tree was verified byte-identical between the local Homebrew
  0.147.0 binary and the `codex-aarch64-apple-darwin` asset from
  `rust-v0.147.0`, downloaded and digest-checked against the lock record.
- Aggregate digest recorded in `pins/codex-lock.json` (`stable_schema_sha256`);
  `scripts/verify_schema_pin.py` is the canonical, locale-independent
  recompute-and-compare check (wire it into CI's cheap lane when the workflow
  lands).
- Regenerating with a different codex version MUST update `codex-lock.json`
  and this document in the same commit.

## Notes for the Codex adapter workstream (phase 5)

- Two schema generations exist (`v1/`, `v2/` plus combined files). The adapter
  targets the v2 shapes; confirm the wire experiment during the protocol spike.
- `PermissionsRequestApprovalResponse.scope` (`turn`|`session`) maps cleanly to
  the "once-only or session-persistent" approval display requirement
  (`sandboxing.md` §6).
- `NetworkAccess` is `restricted`|`enabled` — not a boolean — on
  `externalSandbox`; `workspaceWrite.networkAccess` is a boolean. The reducer
  must not normalize these into one shape blindly.
