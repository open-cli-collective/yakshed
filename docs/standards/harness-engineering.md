# YakShed: Harness Engineering

> **Status:** durable repository standard
> **Applies to:** agent-assisted design, implementation, review, and validation in YakShed

This standard adapts harness-engineering lessons to a small, local-first Tauri
desktop application. It is about making intent legible and feedback cheap; it
does not import the scale assumptions or infrastructure of a high-throughput
web service.

## Stable intent

Humans set product intent, boundaries, and acceptance evidence. Agents perform
the repository work inside those boundaries. The repository is the shared
memory: a short map points to detailed source-of-truth documents, and repeated
failure modes become tests, schemas, review rules, or focused documentation.

YakShed optimizes for a future agent being able to find the owner, understand
the invariant, make a small change, and prove the result. More automation is
useful only when it reduces real human attention or makes a real boundary more
reliable.

## Current strengths

- A link-rich product, architecture, state, secrets, sandbox, composition, and
  contract knowledge store is already checked into the repository.
- Rust modules and crates have explicit construction seams, a provider-neutral
  harness contract, a deterministic mock, and a test-only contract host.
- Persistence, credential delivery, process supervision, revisioned frontend
  events, and provider protocol reduction have concrete tests and failure
  states rather than relying on prose alone.
- Codex schema and release metadata are pinned for the last validated release,
  while runtime compatibility remains intentionally latest-tracking.
- The Svelte surface has a mock-backed Playwright path, and the macOS package
  has a launch/quit smoke path for the checks that cannot be proven on Linux.

## Enforced principles

1. **Map before manual.** Keep `AGENTS.md` short and route work to the owning
   document. Do not turn the entry point into a duplicate specification.
2. **Encode load-bearing rules.** Prefer types, schemas, migrations, boundary
   validation, tests, contract fixtures, revision checks, and CI over reminders.
3. **Keep ownership real.** YakShed owns product state and desktop operations;
   harnesses own provider loops, sessions, auth, tools, and sandbox behavior.
4. **Preserve boundaries.** Secrets stay out of ordinary state and IPC;
   Tauri stays at the edge; provider wire types are translated at adapters;
   app-owned paths and operations are authorized by Rust-owned IDs.
5. **Make failure legible.** Unknown provider events, disconnected processes,
   uncertain mutations, pending approvals, unavailable isolation, and frontend
   revision gaps have explicit states and recovery paths.
6. **Prefer deterministic evidence.** Use temporary paths, memory/mock
   backends, fake processes, real storage boundaries, and bounded fixtures.
   Live provider calls and native stores are supplemental lanes.
7. **Validate the UI as a user surface.** A UI change needs type/build and
   behavior evidence; a packaged desktop or lifecycle change also needs the
   macOS launch/quit gate when available.
8. **Use the smallest reversible design.** Add an abstraction when current
   variation, an external contract, a trust boundary, or reversal cost justifies
   it. Do not build plugin ABIs, generic buses, scorecards, or service layers
   for hypothetical future consumers.

## Acceptance gates

Every chapter or change must leave the repository more discoverable and keep
the applicable technical standards true. The minimum evidence is:

- **Repository front door:** `AGENTS.md` is a short index, `CLAUDE.md` is only
  its pointer, README describes current capability separately from direction,
  obsolete guidance is removed or moved to its owner, and both
  `scripts/verify_agent_guidance.py --self-test` and
  `scripts/verify_agent_guidance.py` pass.
- **Backend changes:** relevant formatting/tests run; the full cheap lane
  remains available; boundary changes have the appropriate contract,
  hermeticity, restart, redaction, or recovery evidence.
- **Frontend changes:** `npm run typecheck`, `npm run build`, and the
  mock-backed Playwright path pass; interaction, loading, error, approval, and
  uncertainty states remain inspectable.
- **Desktop/package changes:** Tauri configuration stays narrow and the
  packaged macOS launch/quit smoke passes or its documented local limitation is
  recorded.
- **Review routing:** changed boundaries receive the applicable repo-local
  reviewer. Tauri and credential matches are required; a known secret path
  that misses a filename glob is reviewed explicitly.

## Pragmatic deferrals and triggers

- **No local observability stack.** Add one only when bounded app diagnostics
  cannot answer a repeated, actionable failure and the smallest useful local
  fixture is understood.
- **No recurring documentation gardener.** Add one only after measurable doc
  drift causes repeated work, with an owning source and a narrow proposed fix.
- **No arbitrary file-size lint, scorecard, or broad merge-gate change.** Add or
  change enforcement only for a demonstrated failure mode, and keep security,
  durability, and boundary checks blocking.
- **No second-harness plugin ABI.** Add an extension seam after a second real
  adapter exposes stable variation that direct composition cannot express.
- **No daemon, remote runtime, or hosted control plane.** Add one when local
  process ownership or UI lifetime is a demonstrated product limitation.

Deferral is not deletion of the invariant. The trigger must name the observed
failure and the smallest next capability that would close it.

## Retirement and evolution

This standard records durable intent, not an archaeological checklist. When
implementation changes, update the owning detailed document, tests, and this
index only as needed. Add a new rule only when an observed failure, security
boundary, or repeated review pattern justifies it; name the owner and the check.
Delete or rewrite guidance when its behavior, tool, or acceptance gate is
retired. Detailed normative documents remain authoritative over this workflow
standard, and every chapter should leave fewer contradictory instructions than
it found.
