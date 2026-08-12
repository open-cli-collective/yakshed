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
For the hardened-runtime signing, notarization, and signed-app launch gate, see
[macOS release signing](release-signing.md).
New secret-backed connections use the macOS Keychain (`local-os`). On first launch after upgrading,
YakShed copies every entry from the interim `data_root/secrets.json`, verifies each Keychain read,
rewrites all affected connection bindings in one config revision, then overwrites the file with
zeroes and removes it. The overwrite is explicitly best-effort: APFS copy-on-write snapshots and
flash wear levelling prevent any application from guaranteeing physical erasure. A locked, denied,
or unavailable Keychain leaves the plaintext store and bindings intact, reports migration pending
in the UI, and retries on the next launch. `local-file` remains available only when explicitly
configured for development.

Explicit `onepassword` backends are resolve-only and use `op read --no-newline --force`
with `op://vault/item/field` locators. Validate a real CLI installation manually
without printing the secret:

```sh
python3 scripts/validate_onepassword.py --account work --reference 'op://vault/item/field'
```

This gate is intentionally local-only and must use a user-provided reference.

Playwright remains the CI S7 gate: it drives the built Svelte surface through the mock invoke/event
factory because Playwright cannot attach to the platform Wry webview on headless Linux.
