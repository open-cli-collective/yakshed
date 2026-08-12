<script lang="ts">
  import { onMount } from "svelte";
  import {
    client,
    type Approval,
    type Connection,
    type DesktopError,
    type FrontendEvent,
    type PendingUserInput,
    type Run,
    type TimelineItem,
    type WorkItem,
    type WorkItemSnapshot,
  } from "./client";
  import { reconcileSnapshot } from "./reconcile";

  const PROJECT_ID = "0197741e-f379-7000-8000-000000000001";
  let items: WorkItem[] = [];
  let selected: WorkItemSnapshot | null = null;
  let run: Run | null = null;
  let timeline: TimelineItem[] = [];
  let approvals: Approval[] = [];
  let inputs: PendingUserInput[] = [];
  let connections: Connection[] = [];
  let configRevision = 0;
  let loading = true;
  let error: DesktopError | null = null;
  let uncertain: string | null = null;
  let batchNotice: string | null = null;
  let settingsOpen = false;
  let theme: "light" | "dark" = "light";
  let eventQueue = Promise.resolve();

  function desktopError(cause: unknown): DesktopError {
    if (cause && typeof cause === "object" && "code" in cause && "message" in cause) {
      const value = cause as { code: DesktopError["code"]; message: string; detail?: string | null };
      return { code: value.code, message: value.message, detail: value.detail ?? null };
    }
    return { code: "internal_error", message: "Unexpected desktop error", detail: null };
  }

  async function bootstrap(): Promise<void> {
    try {
      const listedConnections = await client.listConnections();
      connections = listedConnections.connections;
      configRevision = listedConnections.config_revision;
      try {
        await client.createProject(PROJECT_ID, "YakShed");
      } catch (cause) {
        if (desktopError(cause).code !== "conflict") throw cause;
      }
      await refreshItems();
      if (items[0]) await openItem(items[0]);
    } catch (cause) {
      error = desktopError(cause);
    } finally {
      loading = false;
    }
  }

  async function refreshItems(): Promise<void> {
    const page = await client.listWorkItems(PROJECT_ID);
    items = page.items.map(({ work_item }) => work_item);
  }

  async function openItem(item: WorkItem): Promise<void> {
    selected = await client.getWorkItemSnapshot(item.id);
    await refreshRunData();
  }

  async function refreshRunData(): Promise<void> {
    if (!selected) return;
    run = selected.runs.at(-1) ?? null;
    if (!run) {
      timeline = [];
      approvals = [];
      inputs = [];
      return;
    }
    const revision = selected.revision;
    const [timelinePage, approvalPage, inputPage] = await Promise.all([
      client.getTimelineAtRevision(selected.work_item.id, run.id, null, 50, revision),
      client.getApprovals(selected.work_item.id, run.id, null, 50, revision),
      client.getPendingInput(selected.work_item.id, run.id, null, 50, revision),
    ]);
    timeline = timelinePage.items;
    approvals = approvalPage.approvals;
    inputs = inputPage.inputs;
  }

  async function handleEvent(event: FrontendEvent): Promise<void> {
    if (!selected || event.work_item_id !== selected.work_item.id) return;
    if (event.kind.type === "timeline_batch_appended") {
      batchNotice = `Batch received · ${event.kind.item_count} updates`;
    }
    if (event.kind.type === "run_outcome_unknown") {
      uncertain = `${event.kind.operation} may have completed. Reconcile before retrying.`;
    }
    const before = selected;
    selected = await reconcileSnapshot(before, event, () => client.getWorkItemSnapshot(before.work_item.id));
    await refreshRunData();
    await refreshItems();
  }

  async function createWorkItem(event: SubmitEvent): Promise<void> {
    event.preventDefault();
    const form = event.currentTarget as HTMLFormElement;
    const title = String(new FormData(form).get("title") ?? "").trim();
    if (!title) return;
    const created = await client.createWorkItem(PROJECT_ID, title);
    form.reset();
    await refreshItems();
    selected = created;
    await refreshRunData();
  }

  async function runAction(event: SubmitEvent): Promise<void> {
    event.preventDefault();
    if (!selected || !connections[0]) return;
    const form = event.currentTarget as HTMLFormElement;
    const prompt = String(new FormData(form).get("prompt") ?? "").trim();
    if (!prompt) return;
    uncertain = null;
    try {
      await client.startRun(selected.work_item.id, connections[0].id, prompt);
      form.reset();
    } catch (cause) {
      const problem = desktopError(cause);
      if (problem.code === "outcome_unknown") uncertain = "Run start may have succeeded. Reconcile before retrying.";
      else error = problem;
    }
  }

  async function steer(event: SubmitEvent): Promise<void> {
    event.preventDefault();
    if (!run) return;
    const form = event.currentTarget as HTMLFormElement;
    const message = String(new FormData(form).get("message") ?? "").trim();
    if (!message) return;
    await client.steerRun(run.id, message);
    form.reset();
  }

  async function interrupt(): Promise<void> {
    if (!run) return;
    try {
      await client.interruptRun(run.id);
    } catch (cause) {
      const problem = desktopError(cause);
      if (problem.code === "outcome_unknown") uncertain = "Interrupt outcome is unknown. The run may still be active.";
      else error = problem;
    }
  }

  async function reconcileCurrent(): Promise<void> {
    if (!run || !selected) return;
    await client.reconcileRun(run.id);
    selected = await client.getWorkItemSnapshot(selected.work_item.id);
    uncertain = null;
    await refreshRunData();
  }

  async function resolve(approvalId: string, decision: "approved" | "denied"): Promise<void> {
    await client.resolveApproval(approvalId, decision);
  }

  async function respond(event: SubmitEvent, requestId: string): Promise<void> {
    event.preventDefault();
    const form = event.currentTarget as HTMLFormElement;
    const response = String(new FormData(form).get("response") ?? "").trim();
    if (!response) return;
    await client.respondUserInput(requestId, response);
    form.reset();
  }

  async function addConnection(event: SubmitEvent): Promise<void> {
    event.preventDefault();
    const form = event.currentTarget as HTMLFormElement;
    const data = new FormData(form);
    const id = crypto.randomUUID();
    const saved = await client.putConnection(
      configRevision,
      {
        id,
        name: String(data.get("name")),
        harness: "codex",
        model_provider: String(data.get("provider")),
        provider_state: `connection-${id}`,
        credentials: [{ slot: "codex.account", source: "secret", backend: "local-file", locator: `${id}-account` }],
      },
      false,
    );
    connections = [...connections, saved.connection];
    configRevision = saved.config_revision;
    form.reset();
  }

  async function writeCredential(event: SubmitEvent): Promise<void> {
    event.preventDefault();
    if (!connections[0]) return;
    const form = event.currentTarget as HTMLFormElement;
    const value = String(new FormData(form).get("credential") ?? "");
    if (!value) return;
    await client.setCredential(connections[0].id, "codex.account", value, true);
    form.reset();
  }

  function toggleTheme(): void {
    theme = theme === "light" ? "dark" : "light";
  }

  onMount(() => {
    theme = matchMedia("(prefers-color-scheme: dark)").matches ? "dark" : "light";
    void bootstrap();
    let unlisten: (() => void) | undefined;
    void client.onEvent((event) => {
      eventQueue = eventQueue.then(() => handleEvent(event)).catch((cause) => {
        error = desktopError(cause);
      });
    }).then((stop) => { unlisten = stop; }).catch((cause) => { error = desktopError(cause); loading = false; });
    return () => unlisten?.();
  });
</script>

<svelte:head><title>{selected ? `${selected.work_item.title} · YakShed` : "YakShed"}</title></svelte:head>

<div class="app" data-theme={theme}>
  {#if loading}
    <main class="startup" aria-live="polite"><span class="spinner"></span>Opening the shed…</main>
  {:else if error}
    <main class="startup error-state">
      <p class="eyebrow">STARTUP FAILED · {error.code}</p>
      <h1>YakShed could not open.</h1>
      <p>{error.message}</p>
      {#if error.detail}<code>{error.detail}</code>{/if}
    </main>
  {:else}
    <aside class="rail" aria-label="Work items">
      <header class="brand">
        <span class="mark"></span><strong>yakshed</strong><small>local harness</small>
        <button class="icon-button" type="button" title="Toggle theme" aria-label="Toggle theme" onclick={toggleTheme}>{theme === "light" ? "◐" : "◑"}</button>
      </header>
      <form class="new-task" onsubmit={(event) => void createWorkItem(event)}>
        <label for="new-title">New work item</label>
        <div><input id="new-title" name="title" placeholder="What needs doing?" required /><button>Create</button></div>
      </form>
      <nav>
        <p class="section-title">Work items <span>{items.length}</span></p>
        {#each items as item (item.id)}
          <button class:active={selected?.work_item.id === item.id} aria-current={selected?.work_item.id === item.id ? "page" : undefined} onclick={() => void openItem(item)}>
            <span class:live={item.status === "active"} class="status-dot"></span>
            <span><strong>{item.title}</strong><small>rev {item.revision} · {item.status}</small></span>
          </button>
        {:else}
          <p class="empty">No work yet. Make the first item above.</p>
        {/each}
      </nav>
      <footer><button type="button" onclick={() => settingsOpen = !settingsOpen}>Connections <span>{connections.length}</span></button></footer>
    </aside>

    <main class="workspace">
      {#if selected}
        <header class="toolbar">
          <div><span>YakShed</span><b>›</b><strong>{selected.work_item.title}</strong></div>
          {#if run}<span class:unknown={run.status === "outcome_unknown"} class="run-status">{run.status.replace("_", " ")}</span>{/if}
        </header>
        <section class="timeline" aria-label="Run timeline" aria-live="polite">
          <div class="thread">
            {#if uncertain}
              <div class="uncertain" role="status"><strong>OUTCOME UNCERTAIN</strong><span>{uncertain}</span>{#if run}<button onclick={() => void reconcileCurrent()}>Reconcile</button>{/if}</div>
            {/if}
            {#if batchNotice}<p class="batch-notice">{batchNotice}</p>{/if}
            {#each timeline as entry (entry.id)}
              <article class:command={entry.kind.includes("command")}>
                <header><span>{entry.kind.replaceAll("_", " ")}</span><time>{new Date(entry.created_at_ms).toLocaleTimeString([], { hour: "2-digit", minute: "2-digit" })}</time></header>
                <p>{entry.body}</p>
              </article>
            {:else}
              <div class="welcome"><p class="eyebrow">READY</p><h1>{selected.work_item.title}</h1><p>Start a run to open the timeline.</p></div>
            {/each}
            {#each approvals.filter((approval) => approval.status === "pending") as approval (approval.id)}
              <section class="prompt-card approval">
                <p class="eyebrow">APPROVAL REQUIRED</p><h2>{approval.summary}</h2>
                <div><button onclick={() => void resolve(approval.id, "approved")}>Approve</button><button class="secondary" onclick={() => void resolve(approval.id, "denied")}>Deny</button></div>
              </section>
            {/each}
            {#each inputs as input (input.id)}
              <form class="prompt-card" onsubmit={(event) => void respond(event, input.id)}>
                <p class="eyebrow">WAITING ON YOU</p><h2>{input.prompt}</h2>
                <label for={`response-${input.id}`}>Response</label><textarea id={`response-${input.id}`} name="response" required></textarea><button>Send response</button>
              </form>
            {/each}
          </div>
        </section>
        <section class="composer" aria-label="Run controls">
          {#if run && ["starting", "running"].includes(run.status)}
            <form onsubmit={(event) => void steer(event)}><label for="message">Steer this run</label><textarea id="message" name="message" placeholder="Add direction…" required></textarea><div><span>Follow-up instruction</span><button>Steer</button><button class="secondary" type="button" onclick={() => void interrupt()}>Interrupt</button></div></form>
          {:else}
            <form onsubmit={(event) => void runAction(event)}><label for="prompt">Start a run</label><textarea id="prompt" name="prompt" placeholder={connections.length ? "Describe the work…" : "Add a connection first"} disabled={!connections.length} required></textarea><div><span>{connections[0]?.name ?? "No connection configured"}</span><button disabled={!connections.length}>Start run</button></div></form>
          {/if}
        </section>
      {:else}
        <section class="welcome centered"><p class="eyebrow">YAKSHED</p><h1>Pick a work item.</h1><p>Or create one from the rail.</p></section>
      {/if}
    </main>

    {#if settingsOpen}
      <aside class="settings" aria-label="Connection setup">
        <header><div><p class="eyebrow">CONNECTIONS</p><h2>Harness setup</h2></div><button class="icon-button" aria-label="Close connections" onclick={() => settingsOpen = false}>×</button></header>
        <form onsubmit={(event) => void addConnection(event)}>
          <label for="connection-name">Name</label><input id="connection-name" name="name" placeholder="Local Codex" required />
          <label for="provider">Model provider</label><input id="provider" name="provider" placeholder="codex" required />
          <button>Add connection</button>
        </form>
        {#if connections[0]}
          <form onsubmit={(event) => void writeCredential(event)}>
            <p><strong>{connections[0].name}</strong></p>
            <label for="credential">API credential <small>write-only</small></label>
            <input id="credential" name="credential" type="password" autocomplete="off" required />
            <button>Store credential</button>
          </form>
        {/if}
      </aside>
    {/if}
  {/if}
</div>
