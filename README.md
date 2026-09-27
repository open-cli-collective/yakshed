# YakShed

YakShed is a local-first desktop workbench for organizing, supervising, and
resuming software work performed by coding-agent harnesses. The active build
is an Electron shell around a Python service and Svelte renderer. Codex is the
first real provider; the product model remains provider-neutral.

## Active layout

- `backend/yakshed` owns SQLite state, the JSONL product service, supervision,
  redaction, and provider adapters.
- `desktop` owns the Electron main process, preload bridge, service lifecycle,
  IPC validation, packaging, and shell tests.
- `frontend` owns the Svelte workbench. The Vite build writes to
  `desktop/frontend` for packaged startup.
- `docs/plans/provider-neutral-workbench.md` is the active implementation
  authority. The files under `design/YakShed-UI` remain visual references.

The previous Rust/Tauri implementation was removed from this branch. Git
history retains it for archaeology; new work follows the Electron/Python
boundary described in [the architecture guide](docs/architecture/overall.md).

## Prerequisites

- macOS for the desktop target and package smoke; Linux is supported for CI
  checks and browser preview.
- Node.js 22.14 or later and npm 11.19 or later within the ranges in
  `package.json`.
- Python 3.11 or later for the service. Python 3.12 or 3.13 is used for the
  frozen service because the Codex SDK and PyInstaller wheels must match.
- A delegated Codex login is required only for live Codex operations. Demo and
  backend tests do not read or require provider credentials.

## Quick start

```sh
uv venv --python 3.12 .venv
uv pip install --python .venv/bin/python -r backend/requirements.txt
export YAKSHED_PYTHON="$PWD/.venv/bin/python"
npm ci
npm run dev
```

The normal app starts with an empty local data directory. To use the isolated
deterministic adapter and fixture workspace, run:

```sh
npm run dev:demo
```

For rendered browser checks backed by the real demo JSONL service:

```sh
npm run browser-preview
```

The preview is test-only, binds to loopback, rejects live provider login and
connections, uses a per-process token, and deletes its temporary data on exit.

## Checks and packaging

```sh
npm run typecheck
npm run build:frontend
npm run test:shell
npm run test:syntax
npm run backend:test
npm run test:e2e
```

The backend freeze step supplies `backend/dist/yakshed-service`, including the
Codex native runtime and package metadata. After that artifact exists, build
and exercise the real app bundle:

```sh
npm run package
npm run package:smoke
```

`python scripts/freeze_service.py --output backend/dist/yakshed-service` uses
the pinned Python environment and PyInstaller; when invoked from an unsupported
system Python it can bootstrap the same build through `uv`.

See the [Electron runbook](docs/runbooks/electron-desktop.md) for lifecycle,
freeze, release, and cleanup details. CI runs the Linux checks and macOS
package lane. Tag and manual release runs create draft artifacts; public
publishing requires verified signing and notarization.

## Data, authentication, and extension boundaries

Normal data lives under the OS application-data directory selected by Electron.
The demo directory is separate and opt-in. The renderer receives product
snapshots and revision hints through a narrow preload API; it never gets Node,
filesystem, SQL, provider, or generic process access. Production startup does
not expose a localhost HTTP service.

Authentication is delegated to the provider harness. YakShed does not ask the
renderer to handle tokens and never stores credentials in SQLite, events,
URLs, argv, logs, or frontend state. The Codex adapter is the only backend
module that imports the SDK. A future adapter must implement the same product
seam and sanitized event model rather than adding a provider RPC to the shell.

Read [the product gestalt](docs/product/gestalt.md), [the active contract](docs/contracts/backend-contract-v1.md), and [the sandboxing guide](docs/architecture/sandboxing.md) before changing a boundary.
