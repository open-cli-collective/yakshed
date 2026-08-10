# YakShed backend contract test protocol v1

> **Status:** normative test contract  
> **Transport:** newline-delimited JSON over stdin/stdout  
> **Implementation:** separate Rust binary `yakshed-contract-host`  
> **Consumer:** `scripts/backend_contract_test.py`  
> **Production availability:** forbidden

## 1. Purpose

This protocol provides one cheap, language-external acceptance test for the composed Rust backend.
It is deliberately not Tauri IPC and not a production maintenance API.

The contract host must compose the same production constructors used by the desktop app, with these substitutions:

- all paths rooted beneath a caller-supplied temporary directory;
- memory-backed secret store;
- mock harness adapter;
- real config writer;
- real SQLite store and migrations;
- real artifact store where exercised;
- real process-spawn path for the included fake harness probe;
- deterministic clock and IDs when supplied by the test mode.

The test catches wiring and lifecycle regressions that can be missed when all tests are written in Rust against the same assumptions.

---

## 2. Launch contract

Example:

```bash
yakshed-contract-host \
  --protocol-version 1 \
  --root /tmp/yakshed-contract-abc123 \
  --secret-backend memory \
  --harness mock
```

Requirements:

- `--root` is required unless `--allow-auto-temp-root` is explicitly used.
- The root must be absolute.
- The host creates/uses config, cache, data, and runtime roots beneath it.
- Memory secrets and mock harness are the default and only required v1 modes.
- The host writes protocol responses to stdout and diagnostics to stderr.
- No non-protocol text appears on stdout.
- The host exits nonzero on startup failure.
- `--version` prints the binary version without opening state.

The Python runner may restart the host using the same root to verify persistence boundaries.

---

## 3. Framing and envelopes

One complete JSON object per line, UTF-8 encoded.

### Request

```json
{
  "id": 1,
  "op": "hello",
  "params": {}
}
```

### Success response

```json
{
  "id": 1,
  "ok": true,
  "result": {}
}
```

### Error response

```json
{
  "id": 1,
  "ok": false,
  "error": {
    "code": "not_found",
    "message": "credential is not present",
    "details": {}
  }
}
```

Rules:

- IDs are positive integers selected by the client.
- Responses echo the request ID.
- The host may emit no unsolicited stdout messages in v1.
- Unknown operations return `unknown_operation`.
- Malformed input returns a protocol error when an ID can be recovered; otherwise the host may terminate.
- Error messages and details never contain secret values.
- Response objects never contain secret values.

---

## 4. Error codes

Required stable codes:

```text
invalid_request
unknown_operation
conflict
not_found
already_exists
unsupported
backend_unavailable
locked_or_denied
authentication_required
timeout
cancelled
persistence_error
protocol_error
internal_error
```

The test runner relies mainly on `not_found`, `already_exists`, and `conflict`; the remaining codes reserve the shape for fault-injection extensions.

---

## 5. Operations

## 5.1 `hello`

Request:

```json
{"id":1,"op":"hello","params":{"protocol_version":1}}
```

Result:

```json
{
  "protocol_version": 1,
  "host_version": "0.1.0",
  "root": "/absolute/test/root",
  "modules": {
    "config": "production",
    "data": "sqlite",
    "secrets": "memory",
    "harness": "mock"
  }
}
```

The root is not secret. It must match the supplied test root after canonicalization where possible.

## 5.2 `paths.read`

Result:

```json
{
  "config_root": "/.../config",
  "cache_root": "/.../cache",
  "data_root": "/.../data",
  "runtime_root": "/.../runtime"
}
```

All paths must remain beneath the launch root and be distinct according to the state standard.

## 5.3 `connection.put`

Creates or replaces a non-secret connection configuration with optimistic revision checking.

Request:

```json
{
  "id": 3,
  "op": "connection.put",
  "params": {
    "expected_config_revision": 0,
    "connection": {
      "id": "0193f26e-7a72-7d42-bf77-0de14c4cc222",
      "name": "Work",
      "harness": "mock",
      "model_provider": "anthropic",
      "provider_state": "work-test",
      "credentials": [
        {
          "slot": "anthropic.api_key",
          "source": "secret",
          "backend": "memory",
          "locator": "connection/work/anthropic_api_key",
          "delivery": {
            "kind": "process_environment",
            "variable": "ANTHROPIC_API_KEY"
          }
        }
      ]
    }
  }
}
```

Result:

```json
{"config_revision":1,"connection_id":"0193f26e-7a72-7d42-bf77-0de14c4cc222"}
```

The host validates that no secret-valued field exists in the connection object.

## 5.4 `connection.get`

Request:

```json
{"id":4,"op":"connection.get","params":{"connection_id":"0193..."}}
```

Returns the persisted non-secret connection plus current credential status.
Credential status may be `present`, `missing`, `delegated`, `disabled`, or `unknown`; it never includes a value.

## 5.5 `connection.list`

Returns all configured connection summaries in deterministic order.

## 5.6 `secret.put`

Test-only secret ingress into the memory backend.

Request:

```json
{
  "id": 6,
  "op": "secret.put",
  "params": {
    "backend": "memory",
    "locator": "connection/work/anthropic_api_key",
    "value": "synthetic-contract-secret",
    "overwrite": false
  }
}
```

Result:

```json
{
  "backend": "memory",
  "locator": "connection/work/anthropic_api_key",
  "written": true
}
```

The request contains a synthetic secret because the protocol is test-only. The response, stderr, on-disk state, and subsequent snapshots must not contain it.

## 5.7 `secret.status`

Request:

```json
{"id":7,"op":"secret.status","params":{"backend":"memory","locator":"connection/work/anthropic_api_key"}}
```

Result:

```json
{"status":"present","backend":"memory","locator":"connection/work/anthropic_api_key"}
```

A valid locator with no value returns `status: "missing"`; absence is not an exceptional response.

## 5.8 `secret.delete`

Result reports `deleted: true|false`; it never returns the previous value.

## 5.9 `work.create`

Request:

```json
{
  "id": 9,
  "op": "work.create",
  "params": {
    "id": "0193f26e-7a72-7d42-bf77-0de14c4dd001",
    "project_id": "0193f26e-7a72-7d42-bf77-0de14c4dd000",
    "title": "Contract test work item"
  }
}
```

Result includes the work item and a monotonic data revision.

The caller-supplied ID is a determinism affordance of this test protocol only. The production
`create_work_item` use case generates its own UUIDv7 and does not accept caller-provided IDs
through Tauri or any other production surface.

## 5.10 `work.get`

Returns the persisted work-item snapshot and revision.

## 5.11 `cache.put`

Stores a synthetic non-secret cache entry.

```json
{
  "id": 11,
  "op": "cache.put",
  "params": {
    "namespace": "contract",
    "key": "model-catalog",
    "value": {"models":["mock-model"]}
  }
}
```

## 5.12 `cache.exists`

Returns `{"exists":true|false}`.

## 5.13 `cache.clear`

Deletes the cache root contents and recreates required directories. It must not alter config, data, artifacts, provider state, or secrets.

## 5.14 `runtime.credential_probe`

Exercises the real broker-to-process delivery path using `scripts/fake_harness.py` or another compatible probe executable.

Request:

```json
{
  "id": 14,
  "op": "runtime.credential_probe",
  "params": {
    "connection_id": "0193f26e-7a72-7d42-bf77-0de14c4cc222",
    "slot": "anthropic.api_key",
    "probe_program": "/absolute/path/to/python-or-probe",
    "probe_args": ["/absolute/path/to/fake_harness.py"],
    "expected_sha256": "<hex digest of synthetic secret>",
    "forbidden_variables": ["OPENAI_API_KEY", "FIREWORKS_API_KEY"]
  }
}
```

The host:

1. resolves the connection and credential binding;
2. resolves the secret through the broker;
3. constructs the controlled process environment;
4. starts `probe_program` with `probe_args` without a shell;
5. appends the fake-harness contract flags and passes the declared credential variable;
6. parses one bounded JSON response from the probe;
7. compares the returned digest to `expected_sha256`;
8. verifies forbidden variables were absent;
9. drops its local resolved lease.

Result:

```json
{
  "matched": true,
  "credential_variable": "ANTHROPIC_API_KEY",
  "forbidden_present": [],
  "exit_code": 0
}
```

The digest returned by the probe is not exposed by the host response. This operation exists only for synthetic contract secrets.

> The v1 contract host enables this operation only on Unix, where it can isolate and reap the
> complete probe process group. Non-Unix hosts return `unsupported`; add Windows Job Object
> support only when a Windows contract-gate lane is introduced.

### Probe executable contract

The host invokes the probe without a shell as:

```text
<probe_program> <probe_args...> --credential-var <VARIABLE> [--forbid <VARIABLE>]...
```

- `--credential-var` names the environment variable declared by the connection's delivery binding.
- `--forbid` is appended once per entry in `forbidden_variables`.

The probe writes exactly one JSON object to stdout and nothing else:

```json
{
  "protocol_version": 1,
  "credential_variable": "ANTHROPIC_API_KEY",
  "present": true,
  "sha256": "<hex digest of the variable's value, or null when absent>",
  "forbidden_present": []
}
```

Exit codes: `0` success, `3` credential variable absent, `4` one or more forbidden variables present.
The probe never prints the credential value itself. `scripts/fake_harness.py` is the reference implementation;
the host must treat probe stdout as bounded and must not forward the digest beyond this operation's comparison.

## 5.15 `config.reset`

Resets YakShed config to an empty schema-v1 config. It must not delete SQLite data, artifacts, provider-state roots, or external secret values.

Result includes the new config revision.

## 5.16 `data.purge`

Deletes and reinitializes YakShed-owned SQLite/data and artifact state beneath the test root.
It must not delete config or secret-backend values. Provider-owned state deletion is outside this v1 operation.

## 5.17 `state.summary`

Returns only non-secret counts/status:

```json
{
  "config_revision": 2,
  "connections": 1,
  "work_items": 1,
  "cache_entries": 0,
  "artifacts": 0,
  "secret_statuses": [
    {"backend":"memory","locator":"connection/work/anthropic_api_key","status":"present"}
  ]
}
```

## 5.18 `shutdown`

Returns `{"shutting_down":true}` and exits zero after flushing owned state and removing transient runtime files where applicable.

---

## 6. Restart semantics

When the host is restarted using the same root:

- config persists;
- SQLite work data persists;
- artifacts persist;
- cache persists unless explicitly cleared, but remains disposable;
- memory secret values do not persist;
- secret references in config persist and report `missing`;
- runtime files are recreated and stale files are handled safely.

The Python runner verifies this distinction.

---

## 7. Secret-leak assertions

The Python runner uses unique synthetic canary values and asserts they are absent from:

- every protocol response;
- captured host stderr;
- every regular file beneath the test root;
- fake-harness diagnostic output retained by the host;
- config and SQLite files inspected as raw bytes.

The only allowed locations are:

- the request line carrying `secret.put` from the test runner to the host;
- transient memory inside the host and probe process;
- the environment of the probe process during `runtime.credential_probe`.

The runner does not print the canary on failure; it identifies it by logical slot.

---

## 8. Required acceptance sequence

The standard Python test performs at least this sequence:

1. start a fresh host and negotiate v1;
2. verify all paths are under the temporary root;
3. create one delegated connection and two secret-backed connections;
4. put two distinct synthetic secrets into memory;
5. verify connection/credential isolation;
6. create and retrieve a work item;
7. write and clear a cache entry; verify config/data/secrets survive;
8. run credential probes and verify only the intended variable is delivered;
9. scan responses, stderr, and disk for canary leakage;
10. stop and restart using the same root;
11. verify config/data persist and memory secrets are missing;
12. reinsert one secret;
13. reset config and verify work data survives;
14. recreate one connection, purge data, and verify config + memory secret survive;
15. clean shutdown.

---

## 9. Extensibility

Protocol v1 is intentionally small. Future versions may add:

- artifact publish/read;
- approval lifecycle;
- revision-gap recovery;
- provider-event replay;
- migration fixtures;
- fault injection;
- process crash/recovery.

New operations require a protocol version change or backward-compatible optional extension declared by `hello`.
Do not turn the host into an unrestricted generic RPC surface.
