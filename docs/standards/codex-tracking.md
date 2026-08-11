# Codex release tracking

YakShed runs newer stable Codex releases without a version gate. The committed
`pins/codex-lock.json` and `pins/codex-app-server-schema/` record the last
validated release. Weekly and manually dispatched CI runs
`scripts/check_codex_drift.py`; regular push and pull-request CI stays hermetic
and only verifies committed schema integrity.

The drift checker exits `0` for PASS, `10` for a newer release with the same
schema (DRIFT), `11` for a newer release with changed schema (SCHEMA-DRIFT), `2`
for a retryable network/API failure, and `3` for a local platform, integrity, or
schema-generation error. DRIFT and SCHEMA-DRIFT intentionally fail the scheduled
job and appear in its step summary.

## Responding to drift

1. Run `python3 scripts/repin_codex.py` (or pass an exact `rust-vX.Y.Z` tag).
2. Set `LAST_VALIDATED_CODEX_VERSION` and the fake's `cliVersion` to the new
   lock version, then review the schema diff and adapter DTO validation.
3. Run the workspace tests and backend contract suite.
4. If protocol fixtures changed, run
   `YAKSHED_UPDATE_GOLDEN=1 cargo test -p provider-codex --tests -- --test-threads=1`.
   This explicitly re-records deterministic JSONL from fake/adapter interactions.
5. Re-run the tests normally, without `YAKSHED_UPDATE_GOLDEN`, to verify the
   recorded traces.

`scripts/repin_codex.py --dry-run` downloads, verifies, and generates without
writing. `scripts/repin_codex.py --self-test` checks lock rewriting without
network access. A real re-pin updates all target asset digests, regenerates the
schema, records the validation date, and increments `adapter_revision`.
