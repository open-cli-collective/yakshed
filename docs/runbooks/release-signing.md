# macOS release signing

YakShed ships with hardened runtime and an intentionally empty entitlement set.
The app is not sandboxed because it launches the user-installed Codex and `op`
executables. Without App Sandbox, outbound network access, Keychain access, and
the platform data directory require no entitlement. YakShed grants no hardened-
runtime exceptions such as JIT, unsigned executable memory, or DYLD overrides.

## Local ad-hoc gate

Build, sign, verify, and launch the exact signed bundle:

```sh
cd crates/yakshed-tauri
npm ci
npm run package
cd ../..
scripts/sign_and_notarize.sh --smoke
```

The script applies an ad-hoc signature by default with `--options runtime` and
the committed `Entitlements.plist`. `codesign --verify` must pass. `spctl` must
reject this bundle because ad-hoc signatures have no Apple trust chain; the
script reports that rejection as expected. The launch gate must still reach a
healthy database, quit normally, and leave no process group behind.

## One-time release setup

Install a `Developer ID Application` certificate and private key in the signing
Keychain. The certificate must belong to the Apple Developer team used for
notarization. Store notarization credentials in Keychain once; do not put an
Apple ID password or App Store Connect key in this repository or an environment
variable:

```sh
xcrun notarytool store-credentials yakshed-notary \
  --apple-id USER@example.com \
  --team-id TEAMID
```

Omit `--password`: `notarytool` prompts for the app-specific password
interactively, keeping it out of argv and shell history.

`notarytool` also supports App Store Connect API keys; use its documented
`store-credentials` flags if the release account uses that authentication.

## Developer ID release gate

Build the app, then supply the certificate identity and Keychain profile:

```sh
scripts/sign_and_notarize.sh \
  --identity 'Developer ID Application: Example Company (TEAMID)' \
  --notary-profile yakshed-notary \
  --smoke
```

The equivalent non-secret environment inputs are
`YAKSHED_SIGNING_IDENTITY` and `YAKSHED_NOTARY_PROFILE`. The script signs with a
secure timestamp, verifies the signature and entitlements, submits a temporary
zip with `notarytool --wait`, staples and validates the ticket, requires
Gatekeeper acceptance, and optionally runs the launch/quit smoke.

The Developer ID certificate, Apple account/team membership, and one real
notarization are user-provided release operations. This is the single open K5
item; CI deliberately exercises only ad-hoc hardened-runtime signing and never
receives signing secrets.
