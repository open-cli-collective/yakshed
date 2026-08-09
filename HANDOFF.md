# YakShed implementation handoff

> **Audience:** the orchestrating agent (Fable) that will build YakShed from this bundle by
> delegating to coding agents
> **Bundle reviewed and reconciled:** 2026-08-09
> **Human owner:** Rian Stockbower

## What this bundle is

An implementation-ready backend specification for YakShed: a Tauri/Svelte/Rust desktop
application for organizing, supervising, and resuming work performed with coding-agent
harnesses (Codex first via App Server, Claude Code later). The spec was written and then
adversarially reviewed; the inconsistencies found in that review have already been fixed.
You are not being asked to re-litigate the architecture. You are being asked to build it.

Read `README.md` first — it defines the reading order and, critically, the precedence order
when normative documents overlap. The documents are the source of truth; the `.codereview/`
prompts are enforcement aids, not a second spec.

## Your role

You orchestrate; coding agents implement. Expected shape:

- Decompose along the crate boundaries in `docs/standards/backend-composition-and-testing.md` §3.
  Those boundaries were designed to be independently deliverable — one agent per module or
  module-cluster, with the dependency graph (§4) determining sequencing.
- Every boundary-touching deliverable gets reviewed by the applicable `.codereview/agents/`
  reviewer before you accept it. The Definition-of-Done sections in the standards docs make
  reviewer sign-off part of "complete" — treat that literally.
- The `.codereview/` content is consumed by `~/dev/codereview-cli`. The agent `file_globs`
  are written against the `crates/yakshed-*` layout; if any agent proposes renaming crates,
  the globs must change in the same PR (this is called out in `overall.md` §17).
- A module is not done because its happy path compiles. Error taxonomy, lifecycle behavior,
  recovery behavior, and tests are part of every deliverable (README, closing paragraph).

## Repository setup (first action)

This bundle is not a git repo. Create the implementation repo and import `docs/`,
`.codereview/`, and `scripts/` verbatim at the root — the README states the bundle is meant
to live in the repository as the drift-resistant source of truth, and the review agents are
meant to live on the base branch so a PR cannot weaken the reviewers applied to itself.
`YakShed-UI/` may come along as design reference (see below) but is not code.

## Phase 0 — pinning and verification (before any implementation)

Nothing in the bundle is version-pinned. Before delegating implementation work:

1. **Pin the Codex binary.** Fill in the lock record shape from `overall.md` §5 with a real
   version, per-target assets, SHA-256 digests, and the generated-schema digest.
2. **Verify the Codex facts against the pinned binary, not the docs' footnotes.** The spec's
   claims about App Server (`externalSandbox`, `shell_environment_policy`,
   `process/spawn`, `thread/shellCommand`, workspace-write-with-broad-read, permission
   profiles vs. legacy sandbox settings) are a research snapshot dated 2026-08-09. Generate
   TS/JSON-schema artifacts from the exact pinned binary (`codex app-server generate-ts` /
   `generate-json-schema`) and treat those as the wire contract. Where the binary disagrees
   with the docs, the binary wins; update the docs in the same commit.
3. **Pin the Rust toolchain, Tauri version, and key crates** (`rusqlite`, `keyring-core`
   family, `secrecy`, `directories`, `atomic-write-file`, `rusqlite_migration`). The docs
   name the crates; you choose and lock the versions.

## Hard gates

The README's "Suggested implementation order" is the plan, and the gates are hard. Do not
let an agent (or yourself) skip ahead to the Codex adapter because it is the interesting
part:

1. Workspace, `AppPaths`, config loader, SQLite actor, migration test harness.
2. `yakshed-secrets`: memory + local OS backends, broker, redaction tests.
3. Provider-neutral harness interface + deterministic mock harness.
4. **Gate: `yakshed-contract-host` built and `scripts/backend_contract_test.py` passes.**
5. Codex App Server transport + protocol reducer behind the harness interface.
   **Gate (from `overall.md` §19): the protocol spike is not complete until
   server-initiated requests (approvals) work nonblockingly.** Streaming text alone does
   not validate the architecture.
6. Plain Rust desktop facade, then thin Tauri commands/events around it. Tauri arrives
   only after the facade is testable without it.
7. Native keyring, 1Password, packaging, platform CI lanes.

## What the contract test does and does not prove

`backend_contract_test.py` + the contract host validate composition of paths, config,
SQLite, cache, memory secrets, and broker-to-child-process credential delivery, including
canary-leak scanning of every response, stderr, and raw disk bytes, and restart semantics
(config/data persist, memory secrets vanish). It is the cheapest high-value gate you have.

It deliberately does **not** cover artifacts, approvals, harness event streaming, or crash
recovery (contract §9 lists these as future extensions). Quality in those areas rests on
Rust unit/integration tests, golden-trace replay, the fake-`codex`-executable protocol
tests (`overall.md` §18, composition doc §8.3), and the review agents. Do not report
"contract test green" as "backend works."

The probe executable contract (argv shape, JSON response, exit codes 0/3/4) is specified in
`backend-contract-v1.md` §5.14; `scripts/fake_harness.py` is the reference implementation.

## Decisions already made — do not reopen

- Connection, secret-backend, and credential-binding **definitions are config-canonical**
  (`config.toml`); SQLite holds only derived runtime state keyed by connection ID. The
  contract host's `connection.put` bumping `config_revision` reflects this.
- Workspace layout and crate names are the `yakshed-*` set in the composition doc §3.
- The caller-supplied ID in the contract protocol's `work.create` is a test affordance;
  production `create_work_item` generates UUIDv7 and accepts no caller IDs.
- Codex is a pinned supervised sidecar, never a linked library; App Server over JSONL
  stdio; no TUI scraping.
- Tauri exposes product operations only — the "bad commands" list in composition doc §7 is
  a prohibition, not a style preference.
- Secret values never become config, SQLite, frontend state, events, logs, or argv.
  `SecretString`/`ResolvedSecret` are non-serializable, non-Debug, effectively non-Clone.
- Mock harness first, Codex second, a genuinely different harness third; no plugin ABI
  before two real adapters exist.
- The full "Decisions to lock in" list is `overall.md` §20; the YAGNI controls are
  composition doc §14. Both bind you and every agent you delegate to.

## Known traps and residual risks

- **Scope.** Even the narrowed first release is large. Resist letting agents gold-plate
  early modules; the YAGNI controls exist precisely because agents over-abstract.
- **Secret-boundary reviewer glob gap.** `security/secret-boundary` is `required_on_match`,
  but several globs match filename substrings (`**/*credential*`, `**/*secret*`). A
  secret-touching change in a file not named that way (e.g., child-env construction in a
  supervisor file) may not auto-trigger it. `crates/yakshed-harness/**` is globbed, which
  narrows the gap — but when you know a change touches credential delivery, invoke the
  reviewer explicitly rather than trusting glob selection.
- **Codex drift.** The protocol and permission systems evolve. Version-gate newer profile
  features; contract-test the exact bundled binary per target (`sandboxing.md` §8.2, §17).
- **Test hermeticity.** No test may touch the developer's real keyring, `~/.codex`, or
  production YakShed paths. Temporary `AppPaths` injection is mandatory; changing `HOME` is
  not sufficient (`working-with-state.md` §16). Native keyring tests are a separate lane.
- **Nonblocking protocol reader.** The single most architecture-validating behavior:
  server-initiated approval requests must never block the stdout read loop
  (`sandboxing.md` §6). Test approval-while-streaming early.

## The UI mock

`YakShed-UI/` ("Harness Client" HTML + `support.js`) is a claude-design artifact using a
template DSL (`sc-for`, `{{ }}`). It is **visual reference only** — layout, density, theme
tokens, interaction rhythm. Do not port it literally to Svelte, do not treat its "harness"
branding as the product name, and note it loads Google Fonts remotely, which the real app
should not. The backend spec (`gestalt.md` "The experience") describes the product rhythm
the real frontend must support; the mock shows one visual expression of it.

## Suggested first delegation wave

After Phase 0, these are parallelizable with no shared dependencies beyond
`yakshed-domain` types:

1. `AppPaths` + config store (atomic writes, migrations, revisions) — composition §6.1,
   state doc §5–6.
2. SQLite actor + initial migrations + `AppStore` trait — state doc §8.
3. `yakshed-secrets` memory backend + broker + redaction — secrets doc §8–9.5.
4. Artifact store (staging → digest → atomic move → metadata) — state doc §9.

Then serially: mock harness + harness contract → contract host + Python gate → Codex
transport/reducer → facade → Tauri. Run `architecture/seams` review on the workspace
skeleton PR before wave 1 merges; dependency-direction mistakes are cheapest to catch there.
