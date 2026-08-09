# YakShed code-review agents

These reviewers are repo-local enforcement for the YakShed backend standards.
They are intended to live on the base branch so a pull request cannot weaken the reviewers applied to itself.

| Agent | Scope |
|---|---|
| `rust/implementation-tests` | Rust correctness, idioms, concurrency, and behavioral tests |
| `tauri/config-ipc` | Tauri configuration, IPC surface, capabilities, CSP, and updater boundary |
| `architecture/seams` | Ownership, dependency direction, state classification, provider isolation, and YAGNI |
| `security/secret-boundary` | Credential ingress, resolution, storage, delivery, redaction, and cleanup |

The Rust and Tauri agents are adapted from the Open CLI Collective codereview-cli catalog at commit
`9f573a9294a5d03f704890feb777af618a06235d`. The architecture and secret-boundary reviewers are YakShed-specific.

Prompts deliberately avoid overlapping scopes. A reviewer should return no findings when its own invariants are satisfied.
