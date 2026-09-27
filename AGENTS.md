# YakShed agent index

The active authority is [`docs/plans/provider-neutral-workbench.md`](docs/plans/provider-neutral-workbench.md). The implementation is Electron + Python + Svelte; Rust/Tauri terminology and commands are historical and do not describe this branch.

Read the smallest relevant guide before changing a boundary:

- [Overall architecture](docs/architecture/overall.md) for ownership and
  process lifecycle.
- [Sandboxing](docs/architecture/sandboxing.md) for renderer, IPC, navigation,
  workspace, and credential rules.
- [Product JSONL contract](docs/contracts/backend-contract-v1.md) for allowed
  operations and revision recovery.
- [Electron runbook](docs/runbooks/electron-desktop.md) for development,
  preview, package, and smoke commands.
- [Product gestalt](docs/product/gestalt.md) and `design/YakShed-UI` for
  behavior, density, and visual intent.

## Invariants

- Tasks and provider sessions are separate durable records. The provider is a
  worker behind an adapter, not the product model.
- The Python service is the authority for SQLite state, run lifecycle,
  provider events, approvals, redaction, and revisioned snapshots.
- Electron main owns the child process and native capabilities. Preload
  exposes only the typed product methods; the renderer has no Node, shell,
  filesystem, SQL, or provider access.
- Events are hints. A renderer reload or missed event must recover by fetching
  a snapshot and comparing its revision.
- Secrets stay delegated to the provider or an approved backend boundary. They
  never enter frontend state, SQLite, events, URLs, argv, or logs.
- Demo data is explicit and isolated. Normal startup must not seed fixtures.
- Validate operation names, parameter shapes, IDs, paths, origins, and size at
  every boundary. Do not add generic shell, filesystem, SQL, or provider-RPC
  escape hatches.

## Development checks

Use the repository's `rtk` command prefix for shell commands. The normal cheap
lane is:

```sh
npm ci
npm run typecheck
npm run build:frontend
npm run test:shell
npm run test:syntax
npm run backend:test
npm run test:e2e
```

When packaging or lifecycle code changes, stage the backend-owned
`backend/dist/yakshed-service` first, then run `npm run package` and
`npm run package:smoke`. Keep credentials and token values out of command
output. Root controls final commit, push, and release delivery after the
integrated checks pass.
