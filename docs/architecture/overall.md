# YakShed active architecture

YakShed is a local-first desktop application. The active runtime has four
small boundaries:

```text
Svelte renderer
      │ typed preload methods/events
Electron main process ── native dialogs, tray, lifecycle, power policy
      │ private JSONL stdio
Python product service ── SQLite, runs, approvals, provider adapters
      │ provider seam
Codex SDK / deterministic demo adapter
```

Electron loads the renderer from a local file in packaged builds. It does not
run a production localhost API. The service is a child process supervised by
Electron and receives `--data-dir PATH`; packaged builds execute the frozen
`yakshed-service` binary from `resources/bin`.

## Ownership

The Python service owns task, session, run, note, todo, connection, approval,
event, usage, and settings state. SQLite is its source of truth. The service
normalizes provider meaning while retaining sanitized provider-native details
as metadata. Codex is isolated in `backend/yakshed/providers/codex.py`; the
demo adapter is enabled only by an explicit `--demo` process flag.

Electron owns process supervision, app data path selection, window/tray
lifecycle, native workspace selection, power-save policy, and the narrow IPC
facade. It validates all renderer calls against the fixed product allowlist.
The renderer owns presentation and transient interaction state only. It
requests snapshots after startup and after revisioned `changed` hints.

## Lifecycle and recovery

Startup resolves a new app-data root, starts the service, and loads the
renderer; the first snapshot request is the readiness check. A request has a
bounded timeout and a child exit rejects every in-flight request. Shutdown
closes the service stdin, waits for its exit, and terminates its process group
if needed.
The window may hide on close only when the persisted menubar setting is true;
quit always tears down the child.

Provider events update SQLite before the service emits a revision hint. A
renderer treats hints as lossy notifications: it fetches `snapshot`, compares
the revision, and reconciles the visible work graph. On service restart an
active run is marked interrupted/unknown rather than inferred complete.

## Extending the system

Add a provider in the backend adapter seam and map its capabilities and events
to existing product records. Keep authentication delegated to that provider.
Update the service contract and tests before adding an operation. A desktop
feature should cross the preload bridge as one named product operation with
bounded data, never as a generic command, SQL, filesystem, or provider-RPC
channel. The UI should consume the same snapshot/event shape regardless of
which adapter is active.

Packaging and release checks are defined in the [Electron runbook](../runbooks/electron-desktop.md); security constraints are in [sandboxing.md](sandboxing.md).
