#!/bin/sh
set -eu

repo_root=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
app="$repo_root/target/release/bundle/macos/YakShed.app"
entitlements="$repo_root/crates/yakshed-desktop/Entitlements.plist"
identity=${YAKSHED_SIGNING_IDENTITY:--}
notary_profile=${YAKSHED_NOTARY_PROFILE:-}
smoke=false

usage() {
  echo "usage: $0 [--app PATH] [--identity NAME] [--notary-profile PROFILE] [--smoke]" >&2
  exit 2
}

while [ "$#" -gt 0 ]; do
  case "$1" in
    --app) [ "$#" -ge 2 ] || usage; app=$2; shift 2 ;;
    --identity) [ "$#" -ge 2 ] || usage; identity=$2; shift 2 ;;
    --notary-profile) [ "$#" -ge 2 ] || usage; notary_profile=$2; shift 2 ;;
    --smoke) smoke=true; shift ;;
    -h|--help) usage ;;
    *) usage ;;
  esac
done

[ -d "$app" ] || { echo "app bundle not found: $app" >&2; exit 1; }
[ -f "$entitlements" ] || { echo "entitlements not found: $entitlements" >&2; exit 1; }

if [ "$identity" = "-" ]; then
  [ -z "$notary_profile" ] || {
    echo "notarization requires a Developer ID identity" >&2
    exit 1
  }
fi

if [ "$identity" = "-" ]; then
  codesign --force --options runtime --timestamp=none \
    --entitlements "$entitlements" --sign "$identity" "$app"
else
  codesign --force --options runtime --timestamp \
    --entitlements "$entitlements" --sign "$identity" "$app"
fi

codesign --verify --deep --strict --verbose=2 "$app"
codesign -dv --verbose=4 --entitlements - "$app"

run_smoke() {
  [ "$smoke" = false ] || python3 "$repo_root/scripts/tauri_app_smoke.py" --app "$app"
}

if [ -z "$notary_profile" ]; then
  if [ "$identity" = "-" ]; then
    if spctl --assess --type execute --verbose=4 "$app"; then
      echo "unexpected: Gatekeeper accepted an ad-hoc signature" >&2
      exit 1
    fi
    echo "PASS ad-hoc hardened-runtime signature valid; Gatekeeper rejection expected"
  else
    echo "USER-BLOCKED notarization: provide --notary-profile or YAKSHED_NOTARY_PROFILE"
  fi
  run_smoke
  exit 0
fi

archive=$(mktemp -t yakshed-notary.XXXXXX.zip)
trap 'rm -f "$archive"' EXIT HUP INT TERM
ditto -c -k --keepParent "$app" "$archive"
xcrun notarytool submit "$archive" --keychain-profile "$notary_profile" --wait
xcrun stapler staple "$app"
xcrun stapler validate "$app"
spctl --assess --type execute --verbose=4 "$app"
run_smoke
echo "PASS Developer ID signature, notarization, staple, and Gatekeeper assessment"
