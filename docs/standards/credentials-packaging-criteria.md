# Credentials and packaging phase — testable success criteria

This phase replaces interim credential storage with production backends,
connects Codex's delegated authentication model, and makes the macOS package
release-ready. Each criterion has one empirical check. The phase is complete
when every check passes; later criteria do not weaken earlier secret-boundary
or workspace-health guarantees.

## Scope of the phase

Native macOS Keychain and 1Password CLI secret backends, production credential
composition and migration, delegated Codex authentication, macOS signing and
notarization, and platform-specific CI coverage.

## K1. Native macOS Keychain backend

`local-os` implements the existing secret backend contract with stable,
collision-free service/account names derived from the configured backend ID and
secret locator. Unsupported platforms report a typed unavailable capability.
Default tests never access a user's real keychain.

**Check**: a feature-gated macOS integration suite creates a temporary
keychain, exercises put, resolve, overwrite, delete, and delete-missing, checks
that canary values do not appear in errors or debug output, and destroys the
temporary keychain even after test failure.

## K2. Keychain production default and plaintext migration

Production composition defaults to `local-os`; `local-file` is available only
through explicit development configuration. On first launch, an interim
`data_root/secrets.json` is migrated transactionally into the Keychain and then
securely removed without exposing secret values.

**Check**: a packaged-app test starts from a fresh data root containing an
interim secret file, verifies each credential resolves through `local-os`, the
plaintext file no longer exists, and a second launch is an idempotent no-op.

## K3. 1Password CLI backend

`onepassword-cli` uses the `op` executable through the existing secret backend
contract. Absence or unusability is a typed unavailable capability and never a
desktop startup error.

**Check**: hermetic fake-`op` tests cover put, resolve, delete, typed absence,
malformed output, and canary redaction without requiring 1Password or network
access.

## K4. Delegated Codex authentication

As specified by `docs/architecture/overall.md` section 13, `codex.account` uses
Mode A: App Server account APIs operate against the connection's isolated
`CODEX_HOME`, while YakShed stores account status only. Secret-backed slots use
Mode B and receive broker-resolved credentials just in time.

**Check**: a fake App Server test signs in and signs out a connection, persists
only account status, and scans config, SQLite, events, logs, argv, and debug
output for the credential canary; a separate broker test proves Mode B delivery
occurs only at provider invocation.

## K5. Packaging and signing

The macOS app uses hardened-runtime code signing with committed minimal
entitlements and scripted notarization and stapling. Local development validates
an ad-hoc signature; Developer ID credentials remain an explicit user-provided
release step.

**Check**: the packaging gate builds the app, validates its signature and
entitlements with `codesign`, and either validates a stapled notarization ticket
or reports the documented missing user credentials before notarization begins.

## K6. Platform CI lanes

The unchanged Linux cheap lane continues to guard portable code. A macOS lane
runs workspace tests, the Keychain integration suite against a temporary
keychain, and the packaged build; GUI launch smoke runs there only when the
runner can support it reliably.

**Check**: CI configuration contains both lanes, and the macOS job proves its
temporary keychain is deleted after the integration test while producing a
macOS application bundle.

## K7. Whole-workspace health unchanged

Credential and packaging dependencies remain behind their intended adapters;
pre-existing dependency direction and committed Tauri configuration guarantees
stay intact.

**Check**: formatting, warning-denying clippy, workspace tests, Linux-target
checks, contract-host build and Python gate, schema and Codex metadata pins,
dependency-direction and configuration-drift tests, and `git diff --check` all
pass with the guards extended rather than weakened.

## Explicit non-goals this phase

Vault or other cloud secret backends, lease renewal, interactive unlock,
automatic updates, and Windows or Linux packaging.
