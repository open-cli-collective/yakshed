# YakShed desktop security boundary

The shell is a capability boundary around a local service. The renderer is
untrusted application content even though it is shipped with the app.

## Renderer and navigation

Packaged startup loads the exact bundled `desktop/frontend/index.html` from
the app. Development startup accepts only an explicitly configured loopback
HTTP URL. `contextIsolation`, `sandbox`, `webSecurity`, and a disabled
`nodeIntegration` are required; `webviewTag` is disabled. A restrictive CSP,
permission denial, and exact sender-frame checks apply to every product IPC
handler.

Navigation stays on the configured renderer origin and path. The only external
navigation is an HTTPS provider login URL returned by the validated
`adapter.login` operation, opened through the native browser. Arbitrary
window-open requests and remote renderer URLs are blocked.

## IPC and service

Preload exposes `window.yakshed.request`, `onEvent`, and `chooseWorkspace`.
Main validates the method against the product allowlist, requires plain-object
parameters, limits depth/keys/arrays/strings and total JSON size, and bounds
responses and request time. There is no generic shell, filesystem, SQL,
provider-RPC, or arbitrary URL method. A backend error is reduced to a safe
message before it reaches the renderer.

The child service uses JSONL over private stdio. Electron owns its lifetime,
rejects pending calls on exit, and kills the process group on a stuck shutdown.
The packaged path executes only the frozen service shipped under
`resources/bin`; development may use the explicitly selected project Python.

## Paths, credentials, and preview

The native workspace chooser returns an existing non-root directory after
realpath validation. The backend applies its own workspace boundary checks.
Normal and demo data roots are separate OS application paths; demo fixtures
are never seeded during normal startup.

Codex authentication is delegated to the provider SDK. Token values are not
accepted as product parameters and must not enter SQLite, events, logs, URLs,
argv, or frontend state. The browser preview is an explicit demo-only test
server. It binds to loopback, uses a per-process token plus same-origin checks,
rejects live connections and login, and removes its temporary data root.
