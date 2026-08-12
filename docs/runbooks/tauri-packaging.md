# Tauri packaging gate

The WebKit app launch is a local macOS gate because the Linux CI runner has no Wry GUI session.

```sh
cd crates/yakshed-tauri
npm ci
npm run package
cd ../..
python3 scripts/tauri_app_smoke.py
```

The smoke launches the real `yakshed-desktop` binary with an isolated home, waits for its real
SQLite database, requests a normal application quit, and fails if its process group survives.
The production secret backend is the existing plaintext local-file backend until the keyring phase.

Playwright remains the CI S7 gate: it drives the built Svelte surface through the mock invoke/event
factory because Playwright cannot attach to the platform Wry webview on headless Linux.
