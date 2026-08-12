# YakShed: Product Gestalt

> **Purpose:** the 30,000-foot product brief to read before the detailed architecture and frontend sketch  
> **Audience:** implementation agents, maintainers, and reviewers who need to understand what YakShed is trying to become  
> **Status:** product north star; descriptive rather than a wire-level specification

## In one paragraph

YakShed is a desktop environment for **organizing, supervising, and resuming software work performed with coding-agent harnesses**. Codex is the first harness, not the product boundary. The application treats a durable unit of work—a **yak**—as the primary object, then attaches conversations, agent runs, child work, notes, todos, working copies, plans, diffs, files, command output, and other artifacts to it. The interface should feel less like an IDE with an AI sidebar and less like a prettier terminal, and more like a calm, high-information workbench for directing several pieces of software work without losing the reason, state, or provenance of any of them.

## The problem YakShed is solving

Coding agents are increasingly capable, but the surrounding experience is still organized around the wrong primitives:

- a terminal session that disappears into scrollback;
- a chat thread that becomes the accidental source of truth;
- an IDE organized around open files rather than active work;
- a provider-specific client that owns the user’s entire workflow;
- a pile of branches, worktrees, plans, diffs, and notes that must be mentally reassembled later.

That arrangement works for one short interaction. It becomes brittle when work branches, pauses, depends on other work, moves between providers, or needs to be resumed days later.

YakShed should reduce that coordination tax. It should answer, at a glance:

- What am I trying to accomplish?
- Which pieces of work are active, blocked, or complete?
- Which agent or harness is working on each piece?
- Which repository and working copy is it touching?
- What did it change, produce, discover, or ask permission to do?
- What is the next human decision?
- Can I close the application and later continue without reconstructing all of this from memory?

## The central product insight

> **The unit of navigation is the work, not the file and not the chat.**

A file is something the work touches. A diff is something the work produces. A conversation is one way the work progresses. A terminal command is an event in the work. None of those should become the container for the whole activity.

A **yak** is a durable YakShed-owned work item. It may be a feature, bug, investigation, migration, review, cleanup, prerequisite, or unexpected subproblem. It can have parent, child, dependency, blocker, and related-work relationships.

```text
Project
└── Yak / work item
    ├── objective and status
    ├── parent, child, dependency, and blocker relationships
    ├── one or more harness sessions
    ├── runs and approvals
    ├── notes, todos, and labels
    ├── working copy or worktree
    └── artifacts
        ├── plan
        ├── diff
        ├── file
        ├── command output
        ├── image
        └── provider-native result
```

The name is deliberately useful: when a task exposes another task, the user can **spawn a yak** rather than burying the new concern in a long transcript or letting it derail the parent. The child gets its own state and can later be merged, related, blocked, archived, or handed to another harness.

## The experience

The detailed frontend sketch defines the visual layout. The product model behind it should support this basic rhythm:

1. The user opens a project and sees its current work tree, not merely its files.
2. Selecting a yak restores the work’s timeline, current status, connection, permissions, notes, todos, working copy, and relevant artifacts.
3. The user directs an agent through a conversation-like surface, but structured commands, file edits, approvals, tool calls, and results retain their own identities rather than becoming undifferentiated text.
4. Plans, diffs, files, logs, and other outputs open in a common Reader surface. They are first-class artifacts with provenance, not temporary attachments to a message.
5. When a subproblem appears, the user can spawn child work, choose whether it starts clean, forks native provider context, or receives a curated handoff, and optionally allocate a separate worktree.
6. Several yaks may be active concurrently. YakShed shows where attention is needed without keeping a separate browser-like application instance alive for every dormant item.
7. Closing and reopening the application restores the work model. Provider sessions may reconnect or reconcile, but the user should not need to remember what existed only in frontend memory.

YakShed is therefore both conversational and operational. The conversation is important, but it is one projection of a larger work record.

## Harnesses are workers, not the product model

Codex, Claude Code, and future agent systems have their own strengths and their own native concepts. YakShed should consume those harnesses rather than rebuilding their agent loops.

For Codex, the intended shape is a latest-tracking `codex app-server` process supervised by Rust. Codex owns its thread semantics, model interaction, tool execution, sandbox, approvals, context management, and delegated login. YakShed translates those events into its own product projections and attaches them to work items.

The same principle applies to other harnesses:

```text
YakShed work model
        ↑
provider-neutral application use cases
        ↑
Harness adapter seam
   ├── Codex
   ├── Claude Code
   └── deterministic mock harness
```

YakShed should normalize **product meaning**, not erase real provider differences. “A run needs attention,” “an artifact was produced,” and “this work item has a child” are useful cross-provider concepts. Pretending every provider has identical fork, approval, reasoning, tool, and session semantics is not.

A provider-native session is attached to a yak; it is not the yak’s identity. Switching harnesses creates another session binding or an explicit handoff. It does not silently mutate a Codex thread into a Claude session.

## Connections are visible trust boundaries

A named YakShed connection answers more than “which model?” It captures where work is going, who pays, which credentials and provider state are involved, where execution occurs, and what repositories are allowed.

Examples:

```text
Home · Codex · ChatGPT subscription
Work · Claude Code · Anthropic API
Lab · Codex · Fireworks
```

The active connection should remain visible wherever the user can send work. Accidentally sending company code through a personal or experimental provider is a more serious failure than selecting a suboptimal model.

A connection may delegate authentication to its harness, resolve one or more secret references through YakShed, or combine both. Those details belong behind the credential broker and provider adapter. The product-level promise is simpler:

- the user can tell which connection owns a session;
- credentials do not leak into app state or unrelated runtimes;
- repository-routing policy can block an inappropriate connection;
- changing connections is explicit and auditable.

## Safety should be legible, not magical

The agent’s effective permissions should be visible and understandable. YakShed does not implement Codex’s operating-system sandbox, but it selects the requested policy, surfaces the effective policy, mediates approval requests, and ensures that its own Rust and Tauri operations do not bypass that boundary accidentally.

The user should be able to distinguish:

- read-only work;
- workspace writes with user approval for escalation;
- delegated or automatic approval where supported;
- unrestricted access;
- a stricter outer runtime such as a container, VM, WSL environment, or remote host.

A security-sensitive state should never exist only as a hidden config value. Connection, workspace, execution runtime, and permission mode belong in the visible context of the work.

## The product character

YakShed should be:

- **work-centric:** active objectives and relationships outrank open files;
- **calm:** dense information without constant visual alarm or terminal noise;
- **inspectable:** raw commands, diffs, approvals, provider identity, and provenance remain available;
- **keyboard-friendly:** navigating work, artifacts, approvals, and history should not require pointer-heavy UI choreography;
- **durable:** important state survives frontend reloads, process restarts, and the user’s attention moving elsewhere;
- **provider-aware but not provider-captured:** harness-specific power remains available behind a stable product model;
- **local-first:** the initial application supervises local processes and local repositories without requiring a hosted YakShed service;
- **selectively opinionated:** it should have a strong work model while avoiding an attempt to become every developer tool at once.

The Reader is a particularly important expression of that character. A plan, diff, file, log, image, or provider result should be able to occupy the same inspection surface without turning every artifact into an editor tab.

## What YakShed is not

YakShed is **not**:

- a VS Code clone;
- a full source-code editor in the first instance;
- a terminal emulator with nicer chrome;
- a chat client whose transcript is the database;
- a new universal agent harness;
- a lowest-common-denominator wrapper that hides useful provider features;
- a generic workflow automation platform;
- a credential manager product, even though it must resolve credentials safely;
- a cloud multi-user orchestration service in the first release;
- a plugin ABI designed before two or three real harness adapters prove what extension actually requires.

It may grow toward some neighboring capabilities, but those are not the organizing idea. When a proposed feature competes with the work-centric model, preserve the model.

## The backend gestalt

The backend should feel like a composable Rust application that happens to have a Tauri desktop adapter.

```text
Domain concepts
    ↑
Application use cases
    ↑
Ports and infrastructure adapters
    ↑
Desktop facade
    ↑
Thin Tauri commands and revisioned events
```

The Rust core owns canonical application state, process supervision, provider adapters, Git/worktree operations, credentials, artifacts, and persistence. SQLite and the artifact store hold durable application data. Provider-owned state remains under provider-owned formats in isolated roots. The WebView renders snapshots and patches; it does not own subprocesses, secrets, approvals, or complete transcripts.

Modules should be independently testable through ordinary Rust APIs and explicit dependency construction. Tauri IPC is reserved for actual frontend-facing use cases. A module does not acquire an IPC endpoint merely so a test can call it. Cheap end-to-end confidence comes from a test-only Rust contract host, memory-backed secrets, a deterministic fake harness, and a Python JSONL runner—not from widening the production attack surface.

The architecture should prefer direct, reversible choices:

- a module before a crate when dependency direction does not require a crate;
- a closed enum before a plugin registry when the variants are known;
- a focused trait at a genuine external seam rather than traits everywhere for mocking;
- provider-native semantics preserved behind adapters rather than duplicated in the domain;
- one clear owner for every durable concept and lifecycle transition.

“Composable” means independently constructible, replaceable at established seams, and testable. It does not mean abstracting every function.

## First-release shape

A credible first release is deliberately narrower than the long-term vision:

- local desktop application built with Tauri, Svelte, and Rust;
- Codex as the first production harness through App Server;
- named connections with delegated and secret-backed authentication;
- projects, work items, parent/child relationships, statuses, notes, todos, and labels;
- provider-session bindings and streamed structured timelines;
- sandbox/approval controls and visible runtime context;
- working-copy and worktree support;
- plans, diffs, files, logs, and other artifacts in the Reader;
- durable SQLite state and restart recovery;
- mock harness and memory secret store for deterministic implementation tests.

A second real harness should arrive before the abstraction is declared finished. That implementation is the proof that the product model is genuinely harness-neutral rather than Codex terminology with generic names.

## What good looks like

YakShed is succeeding when a user can begin a piece of work, let it branch, move attention elsewhere, and return later without reconstructing context from terminal scrollback, browser tabs, Git commands, and memory.

More concretely:

- a yak remains understandable after the original conversation has gone cold;
- the user can tell which harness, model provider, account, runtime, repository, worktree, and permission mode are involved;
- child work is explicit instead of being buried as a tangent;
- artifacts are easy to inspect and retain their origin;
- dormant work is cheap, while active work is observable and interruptible;
- provider crashes or application restarts degrade into reconciliation rather than invisible state loss;
- multiple connections can coexist without credential or data-routing confusion;
- the frontend can be replaced without rewriting the product core;
- adding a second harness requires an adapter and capability mapping, not a rewrite of work items, artifacts, or persistence.

## North-star sentence

> **YakShed is the place where software work is organized and supervised; agent harnesses are the workers, conversations are part of the record, and code-related outputs are artifacts of the work rather than the structure of the application.**

The detailed architecture documents define how to preserve that idea safely. The frontend sketch defines how it is expressed visually. When implementation choices are ambiguous, choose the option that makes the user’s work easier to understand, direct, split, inspect, and resume without making YakShed itself responsible for rebuilding the harness beneath it.

## Design reference

The files under [`design/YakShed-UI/`](../../design/YakShed-UI/) are visual
references for layout, density, theme tokens, and interaction rhythm. They use
a template DSL and include remote-font references, so they are not application
code or a literal Svelte implementation. Preserve the product behavior and
local-first boundary described here when validating or replacing the mock.
