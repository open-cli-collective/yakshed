# Tauri phase — testable success criteria (Fable-authored, pre-phase gate)

Draft for the HANDOFF requirement: "Tauri arrives only after the facade is
testable without it" + Fable owes authored testable success criteria before
the phase starts. Each criterion states its empirical check. The phase is done
when ALL checks pass; nothing here is aspirational.

## Scope of the phase

Thin Tauri shell around `yakshed-desktop-api::DesktopApi` + the Svelte UI
shell wired to it. No new backend behavior. provider-codex, contract host,
and the Python gate are untouched.

## S1. Mechanical-wrapper property (the phase's validating criterion)

Every Tauri command is a 1:1 delegation to exactly one `DesktopApi` method:
deserialize DTO in, call, serialize DTO/error out. No branching business
logic, no state, no direct store/harness/secret access.

**Check (structural)**: the Tauri crate's `Cargo.toml` depends on
`yakshed-desktop-api` (+ tauri/serde runtime) ONLY among workspace crates —
extend the existing dependency-direction test to assert no dependency on
yakshed-application/store/harness/secrets/domain/provider-*. DTO types used in
command signatures come exclusively from desktop-api.
**Check (behavioral)**: command handlers are plain async fns testable without
a display; a headless test invokes each handler against a real DesktopApi
over temp roots (mock harness) and gets the same envelope the facade returns.

## S2. Complete command coverage

All shipped facade operations are exposed as Tauri commands with
provider-neutral names: create_project, create_work_item, list_work_items,
get_work_item_snapshot, get_work_item_snapshot_page, get_work_item_timeline_page
(+ at_revision), get_run_approval_page, get_pending_user_input_page, start_run,
steer_run, interrupt_run, reconcile_run, resolve_approval, respond_user_input,
connection_put/connection_get/list_connections, set_connection_credential,
list_artifacts, open_artifact, clear_cache, subscribe/events bridge.

**Check**: a test enumerates the registered command list and diffs it against
this roster — additions and omissions both fail.

## S3. Event bridge preserves the revision contract

FrontendEvents are forwarded to the WebView as Tauri events carrying their
revision unchanged; drop-oldest overflow remains the documented policy and a
gap is recoverable by snapshot refetch THROUGH THE TAURI LAYER.

**Check**: headless test — subscribe via the bridge, force an overflow,
verify the UI-side recovery procedure (refetch snapshot, reconcile by
revision) converges using only Tauri-exposed surfaces.

## S4. Readiness gating

No command is invocable (and no window renders actionable UI) before
`DesktopApi::new`'s startup reconciliation completes. Startup failure yields
a typed, renderable error state — not a hang or a default screen.

**Check**: test drives the app factory with (a) a healthy fixture — first
snapshot is post-reconciliation; (b) a poisoned store — factory surfaces the
typed startup error.

## S5. Secret boundary holds through IPC

set_connection_credential remains write-only end-to-end. The canary value,
ingested through the Tauri command path, is absent from: every queryable
command response, every emitted event payload, serialized error output, and
all fixture-backed persistence (reusing the facade canary scan).

**Check**: extend the existing canary test to drive ingress through the Tauri
command handler instead of the facade directly.

## S6. Hardened shell configuration

CSP locked down (no remote content, no eval), IPC restricted to the defined
command roster, no filesystem/shell/http Tauri plugin capabilities enabled
beyond what the roster needs (which is none — everything goes through
DesktopApi), single-window, no devtools in release.

**Check**: a test (or CI assertion) parses tauri.conf.json and fails on any
capability/allowlist drift from a committed expected config.

## S7. UI shell renders the product loop

The Svelte shell (per design/YakShed-UI mock direction, minimum viable set)
supports: work-item list + creation, timeline view fed by batched events,
approval prompt with resolve, user-input prompt with respond, run
start/steer/interrupt controls, connection setup incl. credential entry,
uncertainty states rendered distinctly (OutcomeUnknown is visible, not
disguised as failure).

**Check**: Playwright (or tauri-driver/WebdriverIO) smoke suite against the
built app with the mock-harness-backed backend: one full scripted run
lifecycle with an approval and a user input, assertions on rendered timeline
batching and the uncertainty state. Failures-only output via rtk.

## S8. Whole-workspace health unchanged

**Check**: existing full verification matrix still green (fmt, clippy -D
warnings, workspace tests, contract-host build, Python gate PASS,
git diff --check); no Tauri dependency leaks into any pre-existing crate
(dependency-direction test extended, not weakened).

## S9. Packaged app launches

`cargo tauri build` produces a macOS .app that launches, passes S4's healthy
path against a fresh real data dir, and quits cleanly (no orphaned
processes — verify the process group after quit).

**Check**: scripted launch/quit smoke in CI-on-macOS (or documented local
gate if CI runners can't run the GUI; then the local gate is part of the
merge checklist).

## Explicit non-goals this phase

Native keyring/1Password backends (next phase), packaging/signing beyond a
local .app (retune-style release comes later), multi-window, auto-update,
yaks/worktrees/labels product features.
