# YakShed product JSONL contract

The desktop service speaks newline-delimited JSON over stdin/stdout. Every
request is an object with a client id:

```json
{"id":"desktop_1","method":"snapshot","params":{}}
```

A successful response is `{"id":"desktop_1","result":...}`. A failure is
`{"id":"desktop_1","error":{"message":"safe explanation"}}`. The
service may emit a revision hint at any time:

```json
{"event":"changed","revision":12,"active_count":1}
```

The hint is lossy. Clients fetch `snapshot` after startup and after a hint;
`revision` is the recovery cursor and `active_count` is the authoritative
number used by the native stay-awake policy.

## Product operations

The shell allowlist is deliberately closed:

```text
snapshot                  task.detail
stats                     task.create
task.update               task.archive
note.save                 todo.create
todo.update               todo.delete
run.start                 run.interrupt
approval.respond          settings.update
connection.create         adapter.status
adapter.login
```

Each operation receives a bounded object with named IDs and fields. The
service validates required values, ownership, workspace paths, adapter
capabilities, and run state. Provider-native messages are handled inside the
adapter; they do not cross this boundary as arbitrary RPC.

Snapshots contain the task graph, sessions/runs, notes, todos, connections,
settings, adapters, timeline records, usage values, and current revision.
Writes persist before emitting a hint. An active run that cannot be reconciled
after restart is represented as interrupted or unknown.

## Boundary rules

Requests and responses have finite size limits and plain JSON values. Error
messages are sanitized and capped. Secret values are scrubbed before durable
state, events, or UI delivery. Adding a method requires a product use case,
backend validation, shell/preload wiring, and behavioral tests; do not expose
generic process, filesystem, SQL, credential, or provider transport calls.
