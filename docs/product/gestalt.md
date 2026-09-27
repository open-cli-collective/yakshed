# YakShed product gestalt

YakShed is where software work is organized and supervised. A task is the
durable unit of navigation; a provider conversation, run, diff, plan, command
result, note, and child task are records attached to that work.

## Product character

The workbench should make it easy to answer:

- What is active, blocked, waiting, or complete?
- Which provider connection and workspace are involved?
- What changed, what was produced, and what needs a human decision?
- Can the task be closed and resumed without reconstructing context from
  terminal scrollback?

The layout is dense and calm. The task tree, timeline, composer, Reader,
notes, todos, settings, and usage rollups are projections of the same durable
work graph. Structured approvals, artifacts, and provider events retain their
identity instead of becoming undifferentiated chat text.

## Provider boundary

Codex is the first worker, not the product model. A connection and provider
session are visible parts of a task, while task identity, relationships,
artifacts, and local state belong to YakShed. A future adapter must map its
capabilities into the same product records and may retain provider-native
details where meanings differ.

## Safety and durability

The service is the authority for state and event order. The renderer can be
reloaded at any time and reconstructs from a snapshot plus revision hints.
Authentication stays delegated to the harness; secrets do not become UI or
database data. Demo fixtures are explicit and isolated from normal startup.

The visual files under [`design/YakShed-UI`](../../design/YakShed-UI/) are
references for layout, density, theme tokens, and interaction rhythm. They are
not application code. Preserve the local-first process and security boundary
when translating the reference into Svelte.
