# Provider-neutral workbench rebuild

Status: active implementation authority (2026-09-27)

YakShed is being rebuilt as a local-first macOS workbench with one durable
provider-neutral domain and a thin Electron shell. The product term is
`task`; a task may own multiple provider sessions, including child tasks and
native provider subagents. Provider sessions, events, usage, approvals, and
artifacts are persisted in SQLite so reloads and restarts recover the same
work graph.

## Implementation boundary

- `backend/yakshed` owns domain types, SQLite persistence, JSONL product RPC,
  supervision, redaction, and provider adapters.
- The Codex adapter is the only module that imports `openai_codex`. It uses the
  pinned Python SDK `0.157.1` and app-server protocol. Its approval dispatch
  must fail closed and must not block event delivery while the UI is deciding.
- Electron main/preload supervise the backend and expose validated product
  operations through context isolation. The renderer has no direct shell,
  filesystem, SQL, or provider access.
- `frontend` is Svelte/Vite. It renders the task tree, timeline, composer,
  todos, notes, Reader, settings, themes, and usage rollups from RPC
  snapshots/events.
- The provider interface is small enough for a deterministic adapter and
  future Anthropic/OSS adapters without changing the domain or UI.

The product RPC is JSONL: requests are `{id,method,params}`, responses are
`{id,result}` or `{id,error:{message}}`, and writes emit a `{event:"changed",
revision}` hint for re-fetching. The backend entry point is
`python -m backend.yakshed --data-dir PATH`; `--demo` is the only switch that
enables deterministic fixture data and the demo adapter. The shipped Codex
adapter is the only real provider; Anthropic and OSS adapters remain future
registry implementations until they satisfy this contract.

## Required behavior

The first runnable slice must support root/child/grandchild tasks, inherited
labels and permissions with overrides, sessions and child sessions, archive
and undo, search including archived tasks, notes/todos, structured timeline,
plans/diffs/text Reader, dense themed layout, keyboard shortcuts, own and
subtree duration/token rollups, settings, and real provider controls. Normal
startup is empty; sample content exists only behind an explicit isolated demo
mode.

Usage records retain only values actually reported by a provider. Input,
output, reasoning, cache-read, cache-write, total, context-window, rate-limit,
cost, and currency stay nullable when absent. Cumulative and per-run values
carry an explicit basis and are deduplicated. Descendant totals are computed
without adding child usage twice.

Secrets never enter frontend state, SQLite, events, logs, URLs, or argv.
Unknown provider events are retained as sanitized extension envelopes. On
restart an active run becomes interrupted/unknown and never completed by
inference. A resume uses the opaque native session identifier.

## Verification gates

Run the standard-library backend tests, TypeScript typecheck/build, renderer
tests, and the macOS package smoke. The acceptance journey must exercise a
real rendered UI, a temporary SQLite root, duplicate/cumulative telemetry,
unknown events and redaction, provider registry replacement, actual packaged
Codex initialization and isolated run/resume/interrupt when delegated login is
available, and packaged Electron launch/close. If live auth is unavailable,
report that exact gap rather than fabricating telemetry.

The previous Rust/Tauri architecture remains in Git history for reference. It
is historical context only; new product work follows this document.

## Acceptance evidence (2026-09-27)

- Renderer and shell checks: `npm run typecheck` (0 errors/warnings), `npm run build:frontend`, `npm run test:e2e` (5 passed), `npm run test:shell` (5 passed), and `git diff --check`.
- Rendered browser review covered empty startup, root/child task creation with inherited labels and permissions, note/todo persistence after reload, archive/undo, palette and light/dark themes, Reader artifacts, approval response, and per-task workspace/connection restoration across switching and reload.
- Live SDK evidence: an isolated read-only start accepted prompt `YAKSHED_OK`; two-turn resume retained `cedar` in one native session while producing distinct native run IDs; an actual interrupt reached `interrupted`; and a native child requested `calculate 7*6`, persisted a completed native run with reported 2s duration, and hydrated usage. The measured subtree total was 87,851 (parent 66,135 + child 21,716).
- Anthropic and OSS adapters are future work and are not shipped. Child usage remains `null`/`partial` when unavailable rather than being inferred. The final PyInstaller freeze, Electron DMG/ZIP package, and `npm run package:smoke` passed locally on the final tree. CI evidence is available in the [branch's GitHub Actions runs](https://github.com/open-cli-collective/yakshed/actions?query=branch%3Arebuild%2Fprovider-neutral-workbench).
