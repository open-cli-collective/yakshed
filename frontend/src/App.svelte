<script lang="ts">
  import { onMount } from "svelte";
  import {
    chooseWorkspace,
    eventKind,
    eventPayload,
    eventText,
    normalizeDetail,
    normalizeSnapshot,
    latestTaskContext,
    projectTimeline,
    request,
    subscribe,
    type AdapterDescriptor,
    type Artifact,
    type Connection,
    type Detail,
    type PermissionMode,
    type Task,
    type TaskContext,
    type TaskResumeState,
    type Todo,
    taskResumeState,
  } from "./client";
  import SettingToggle from "./SettingToggle.svelte";
  import TaskRow from "./TaskRow.svelte";

  const palettes = [
    { id: "og", name: "Original", darkName: "Original", description: "Warm grey with an indigo signal." },
    { id: "ledger", name: "Ledger", darkName: "Ledger", description: "Paper and ink with amber edges." },
    { id: "basalt", name: "Basalt", darkName: "Basalt", description: "Cool near monochrome, teal signal." },
    { id: "mithril", name: "Mithril", darkName: "Ithildin", description: "Terminal blue with quiet surfaces." },
  ];
  const permissionModes: Array<{ id: PermissionMode; name: string; description: string }> = [
    { id: "read_only", name: "Read only", description: "Inspect files and answer without writes." },
    { id: "workspace_write", name: "Workspace write", description: "Write inside the selected workspace." },
    { id: "full_access", name: "Full access", description: "The adapter may use its full capability set." },
  ];
  const keyHelp = [
    ["c / ⇧c", "new task / child task"], ["j k · ↑ ↓", "move through tasks"], ["← →", "fold or unfold children"],
    ["x", "select task or todo"], ["e", "archive task / finish todo"], ["z", "undo archive"], ["l", "edit labels"],
    ["/", "search, including archived"], ["]", "todo rail"], ["\\", "Reader"], ["d · t", "mode · palette"],
    ["p", "permission for this task"], [",", "settings"], ["?", "keyboard help"],
  ];

  let tasks: Task[] = [];
  let connections: Connection[] = [];
  let adapters: AdapterDescriptor[] = [];
  let settings: Record<string, unknown> = {};
  let selectedId: string | null = null;
  let detail: Detail | null = null;
  let loading = true;
  let detailLoading = false;
  let bridgeError = "";
  let detailError = "";
  let search = "";
  let searchInput = "";
  let includeArchived = false;
  let searchOpen = false;
  let sidebarWidth = 306;
  let resizing = false;
  let railOpen = true;
  let readerOpen = false;
  let readerArtifact: Artifact | null = null;
  let settingsOpen = false;
  let settingsSection = "general";
  let palette = "og";
  let mode: "light" | "dark" = "dark";
  let defaultPermission: PermissionMode = "read_only";
  let focusZone: "threads" | "todos" = "threads";
  let selectedConnectionId = "";
  let workspacePath = "";
  let taskContextById: Record<string, TaskContext> = {};
  let dirtyTaskContext: Record<string, Partial<Record<keyof TaskContext, boolean>>> = {};
  let draftByTask: Record<string, string> = {};
  let todoDraft = "";
  let todoFocus = 0;
  let todoPicked: Record<string, boolean> = {};
  let editingTodoId: string | null = null;
  let todoTextDraft = "";
  let todoNoteDraft = "";
  let noteDraft = "";
  let noteDirty = false;
  let noteDrafts: Record<string, string> = {};
  let noteDirtyTask: string | null = null;
  let notesEditing = false;
  let labelsOpen = false;
  let labelDraft = "";
  let permissionsOpen = false;
  let createOpen = false;
  let createParentId: string | null = null;
  let createTitle = "";
  let createLabels = "";
  let createPermission: "inherit" | PermissionMode = "inherit";
  let createBusy = false;
  let connectionFormOpen = false;
  let connectionName = "";
  let connectionAdapter = "";
  let connectionModel = "";
  let connectionBusy = false;
  let adapterStatus: Record<string, string> = {};
  let toast = "";
  let undoIds: string[] = [];
  let runBusy = false;
  let helpOpen = false;
  let clockOpen = false;
  let usageOpen = false;
  let reloadSerial = 0;
  let layoutPreferencesLoaded = false;
  let stats: Record<string, unknown> | null = null;
  let resumeState: TaskResumeState = { resumable: true };
  let stopEvents: (() => void) | undefined;
  let toastTimer: ReturnType<typeof setTimeout> | undefined;
  let detailSerial = 0;

  $: selectedTask = selectedId ? tasks.find((task) => task.id === selectedId) ?? detail?.task ?? null : null;
  $: currentDraft = selectedId ? draftByTask[selectedId] ?? "" : "";
  $: currentRun = detail?.runs.slice().reverse().find((run) => ["queued", "starting", "running", "waiting", "cancelling"].includes(String(run.state ?? run.status))) ?? null;
  $: selectedConnection = connections.find((connection) => connection.id === selectedConnectionId) ?? null;
  $: currentCapabilities = selectedConnection ? adapterFor(selectedConnection)?.capabilities ?? [] : [];
  $: resumeState = detail ? taskResumeState(detail) : { resumable: true };
  $: effectivePermission = selectedTask ? effectivePermissionMode(selectedTask) : "read_only";
  $: allLabels = collectLabels(tasks);
  $: activeRows = treeRows("active", tasks, includeArchived, folded);
  $: waitingRows = treeRows("waiting", tasks, includeArchived, folded);
  $: inboxRows = treeRows("inbox", tasks, includeArchived, folded);
  $: archivedRows = tasks.filter((task) => task.archived).map((task) => ({ task, depth: 0 }));
  $: taskCount = tasks.filter((task) => !task.archived).length;
  $: pendingTodoCount = detail?.todos.filter((todo) => !todo.done).length ?? 0;
  $: themeName = palettes.find((item) => item.id === palette)?.[mode === "dark" ? "darkName" : "name"] ?? "Original";
  $: statsTaskCounts = object(stats?.tasks);
  $: timelineEvents = projectTimeline(detail?.events ?? []);

  function object(value: unknown): Record<string, unknown> {
    return value && typeof value === "object" ? value as Record<string, unknown> : {};
  }

  function text(value: unknown, fallback = ""): string {
    return typeof value === "string" ? value : value == null ? fallback : String(value);
  }

  function errorText(error: unknown): string {
    if (error instanceof Error) return error.message;
    const row = object(error);
    return text(row.message, "The desktop request could not be completed.");
  }

  function applyTheme(value: unknown): void {
    if (typeof value !== "string") return;
    const parts = value.toLowerCase().split("-");
    const nextPalette = palettes.some((item) => item.id === parts[0]) ? parts[0] : palette;
    const nextMode = parts[1] === "light" || parts[1] === "dark" ? parts[1] : mode;
    palette = nextPalette;
    mode = nextMode;
  }

  function loadLocalPreferences(): void {
    if (typeof localStorage === "undefined") return;
    const storedMode = localStorage.getItem("yakshed.mode");
    const storedPalette = localStorage.getItem("yakshed.palette");
    if (storedMode === "light" || storedMode === "dark") mode = storedMode;
    if (palettes.some((item) => item.id === storedPalette)) palette = storedPalette ?? palette;
  }

  function updateTaskContext(taskId: string, changes: TaskContext, markDirty = true): void {
    taskContextById = { ...taskContextById, [taskId]: { ...(taskContextById[taskId] ?? {}), ...changes } };
    if (!markDirty) return;
    const dirty = dirtyTaskContext[taskId] ?? {};
    const nextDirty = { ...dirty };
    if (changes.workspace !== undefined) nextDirty.workspace = true;
    if (changes.connectionId !== undefined) nextDirty.connectionId = true;
    dirtyTaskContext = { ...dirtyTaskContext, [taskId]: nextDirty };
  }

  function activateTaskContext(taskId: string): void {
    const context = taskContextById[taskId];
    selectedConnectionId = context?.connectionId ?? "";
    workspacePath = context?.workspace ?? "";
  }

  function restoreTaskContext(taskId: string, nextDetail: Detail): void {
    const persisted = latestTaskContext(nextDetail);
    const local = taskContextById[taskId] ?? {};
    const dirty = dirtyTaskContext[taskId] ?? {};
    const workspace = dirty.workspace ? local.workspace : persisted.workspace ?? local.workspace;
    const connectionId = dirty.connectionId ? local.connectionId : persisted.connectionId ?? local.connectionId;
    const restored: TaskContext = {
      ...(workspace ? { workspace } : {}),
      ...(connectionId ? { connectionId } : {}),
    };
    if (!restored.connectionId && !nextDetail.runs.length && !nextDetail.sessions.length) {
      const firstConnection = connections[0]?.id;
      if (firstConnection) restored.connectionId = firstConnection;
    }
    taskContextById = { ...taskContextById, [taskId]: restored };
    activateTaskContext(taskId);
  }

  async function loadSnapshot(selectFirst = true): Promise<void> {
    const serial = ++reloadSerial;
    try {
      let raw: unknown;
      try {
        raw = await request("snapshot", { search, include_archived: includeArchived });
      } catch (error) {
        if (!search && !includeArchived) throw error;
        raw = await request("snapshot", {});
      }
      if (serial !== reloadSerial) return;
      const snapshot = normalizeSnapshot(raw);
      tasks = snapshot.tasks;
      connections = snapshot.connections;
      adapters = snapshot.adapters;
      settings = snapshot.settings;
      if (settings.default_permission === "read_only" || settings.default_permission === "workspace_write" || settings.default_permission === "full_access") defaultPermission = settings.default_permission;
      if (!layoutPreferencesLoaded) {
        if (typeof settings.sidebar_width === "number" && settings.sidebar_width >= 230 && settings.sidebar_width <= 480) sidebarWidth = settings.sidebar_width;
        if (typeof settings.rail_open === "boolean") railOpen = settings.rail_open;
        if (typeof settings.reader_open === "boolean") readerOpen = settings.reader_open;
        layoutPreferencesLoaded = true;
      }
      applyTheme(settings.theme ?? (typeof settings.palette === "string" ? `${settings.palette}-${settings.mode ?? mode}` : undefined));
      loadLocalPreferences();
      const available = tasks.filter((task) => includeArchived || !task.archived);
      if (selectFirst && (!selectedId || !available.some((task) => task.id === selectedId))) selectedId = available[0]?.id ?? null;
      if (selectedId && available.some((task) => task.id === selectedId)) await loadDetail(selectedId);
      else detail = null;
      bridgeError = "";
    } catch (error) {
      bridgeError = errorText(error);
      tasks = [];
      detail = null;
    } finally {
      if (serial === reloadSerial) loading = false;
    }
  }

  async function loadDetail(taskId: string): Promise<void> {
    const serial = ++detailSerial;
    detailLoading = true;
    try {
      const raw = await request("task.detail", { task_id: taskId });
      if (serial !== detailSerial || selectedId !== taskId) return;
      const nextDetail = normalizeDetail(raw, taskId);
      detail = nextDetail;
      restoreTaskContext(taskId, nextDetail);
      const liveRun = nextDetail.runs.slice().reverse().find((run) => ["queued", "starting", "running", "waiting", "cancelling"].includes(String(run.state ?? run.status)));
      const projectedStatus = liveRun && String(liveRun.state ?? liveRun.status) === "waiting" ? "waiting" : liveRun ? "active" : null;
      if (projectedStatus) {
        const current = tasks.find((task) => task.id === taskId);
        if (current && current.status !== projectedStatus) tasks = tasks.map((task) => task.id === taskId ? { ...task, status: projectedStatus } : task);
      }
      if (noteDirtyTask !== taskId) {
        noteDraft = text(detail.notes[0]?.body, "");
        noteDrafts = { ...noteDrafts, [taskId]: noteDraft };
        noteDirty = false;
      } else {
        noteDraft = noteDrafts[taskId] ?? noteDraft;
        noteDirty = true;
      }
      if (!readerArtifact || !detail.artifacts.some((artifact) => artifact.id === readerArtifact?.id)) readerArtifact = firstReadableArtifact(detail.artifacts);
      detailError = "";
    } catch (error) {
      if (serial === detailSerial && selectedId === taskId) {
        detailError = errorText(error);
        detail = null;
      }
    } finally {
      if (serial === detailSerial) detailLoading = false;
    }
  }

  function adapterFor(connection: Connection): AdapterDescriptor | undefined {
    return adapters.find((adapter) => adapter.id === connection.adapter);
  }

  function supports(name: string): boolean {
    if (!currentCapabilities.length) return true;
    return currentCapabilities.some((capability) => capability === name || capability.endsWith(`.${name}`));
  }

  function adapterSupports(adapterId: string, ...names: string[]): boolean {
    const capabilities = adapters.find((adapter) => adapter.id === adapterId)?.capabilities ?? [];
    return !capabilities.length || capabilities.some((capability) => names.some((name) => capability === name || capability.endsWith(`.${name}`)));
  }

  function taskById(id: string | null): Task | undefined {
    return id ? tasks.find((task) => task.id === id) : undefined;
  }

  function childrenOf(parentId: string | null, source: Task[] = tasks, include = includeArchived): Task[] {
    return source.filter((task) => task.parent_task_id === parentId && (!task.archived || include));
  }

  function descendants(id: string): string[] {
    const result = [id];
    let cursor = 0;
    while (cursor < result.length) {
      const parent = result[cursor++];
      childrenOf(parent).forEach((child) => { if (!result.includes(child.id)) result.push(child.id); });
    }
    return result;
  }

  function treeRows(section: "active" | "waiting" | "inbox", source: Task[] = tasks, include = includeArchived, collapsed = folded): Array<{ task: Task; depth: number }> {
    if (section !== "inbox") return source.filter((task) => !task.archived && task.status === section).map((task) => ({ task, depth: ancestors(task).length }));
    const roots = source.filter((task) => !task.archived && (!task.parent_task_id || !source.some((parent) => parent.id === task.parent_task_id)));
    const rows: Array<{ task: Task; depth: number }> = [];
    const walk = (task: Task, depth: number) => {
      rows.push({ task, depth });
      if (collapsed.has(task.id)) return;
      childrenOf(task.id, source, include).forEach((child) => walk(child, depth + 1));
    };
    roots.forEach((root) => walk(root, 0));
    return rows;
  }

  let folded = new Set<string>();
  let picked = new Set<string>();

  function ancestors(task: Task): Task[] {
    const result: Task[] = [];
    let parent = task.parent_task_id;
    while (parent) {
      const item = taskById(parent);
      if (!item) break;
      result.unshift(item);
      parent = item.parent_task_id;
    }
    return result;
  }

  function labelEntries(task: Task | null): Array<{ name: string; color: string; own: boolean }> {
    if (!task) return [];
    const own = object(task.labels);
    const effective = Object.keys(object(task.effective_labels)).length ? object(task.effective_labels) : own;
    return Object.keys(effective).map((name) => ({ name, color: labelColor(name, effective[name]), own: Object.prototype.hasOwnProperty.call(own, name) }));
  }

  function collectLabels(source: Task[] = tasks): string[] {
    const names = new Set<string>();
    source.forEach((task) => [object(task.labels), object(task.effective_labels)].forEach((labels) => Object.keys(labels).forEach((name) => names.add(name))));
    return [...names].sort();
  }

  function labelColor(name: string, value: unknown): string {
    if (typeof value === "string" && value.trim()) return value;
    let hash = 0;
    for (const character of name) hash = (hash * 31 + character.charCodeAt(0)) % 360;
    return `oklch(0.67 0.13 ${hash})`;
  }

  function statusName(task: Task): string {
    if (task.archived) return "archived";
    if (task.status === "active") return "working";
    if (task.status === "waiting") return "waiting";
    if (task.status === "done") return "done";
    return "inbox";
  }

  function effectivePermissionMode(task: Task): PermissionMode {
    const value = task.effective_permissions?.mode ?? task.permissions?.mode;
    return value === "workspace_write" || value === "full_access" ? value : "read_only";
  }

  function permissionName(value: string): string {
    return permissionModes.find((item) => item.id === value)?.name ?? "Read only";
  }

  function formatDuration(ms: unknown): string {
    const total = Math.max(0, Math.floor(Number(ms) / 1000));
    const hours = Math.floor(total / 3600);
    const minutes = Math.floor((total % 3600) / 60);
    const seconds = total % 60;
    if (hours) return `${hours}h ${String(minutes).padStart(2, "0")}m`;
    if (minutes) return `${minutes}m ${String(seconds).padStart(2, "0")}s`;
    return `${seconds}s`;
  }

  function formatNumber(value: unknown): string {
    return typeof value === "number" ? value.toLocaleString() : "—";
  }

  function usageCompleteness(value: Record<string, unknown> | null | undefined): string {
    if (!value) return "unavailable";
    return value.incomplete === true ? "partial" : "reported";
  }

  function currentRunState(): string {
    return currentRun ? text(currentRun.state ?? currentRun.status, "working") : statusName(selectedTask ?? ({ status: "inbox", archived: false } as Task));
  }

  function firstReadableArtifact(list: Artifact[]): Artifact | null {
    return list.find((artifact) => artifact.content || artifact.path || ["plan", "diff", "text"].includes(String(artifact.kind))) ?? null;
  }

  function artifactText(artifact: Artifact | null): string {
    if (!artifact) return "";
    const metadata = object(artifact.metadata);
    return text(artifact.content ?? metadata.content ?? metadata.text, artifact.path ? `Open in workspace: ${artifact.path}` : "");
  }

  function eventView(event: Record<string, unknown>): { kind: string; body: string; approvalId: string; artifactId: string; meta: string } {
    const normalized = event as Parameters<typeof eventKind>[0];
    const payload = eventPayload(normalized);
    const kind = eventKind(normalized);
    const approvalId = text(payload.approval_id ?? event.approval_id);
    const requestedArtifactId = text(payload.artifact_id ?? event.artifact_id);
    const artifact = detail?.artifacts.find((candidate) => candidate.id === requestedArtifactId
      || (!requestedArtifactId && text(payload.name) && candidate.name === text(payload.name)));
    return {
      kind,
      body: eventText(normalized),
      approvalId,
      artifactId: artifact?.id ?? requestedArtifactId,
      meta: text(event.created_at ?? event.created_at_ms, ""),
    };
  }

  function approvalEventId(event: Record<string, unknown>): string {
    const payload = eventPayload(event as Parameters<typeof eventPayload>[0]);
    return text(payload.approval_id ?? event.approval_id);
  }

  function isApprovalResolution(event: Record<string, unknown>): boolean {
    const payload = eventPayload(event as Parameters<typeof eventPayload>[0]);
    return text(event.type ?? event.kind ?? payload.kind).toLowerCase() === "approval.resolved";
  }

  function approvalPending(approvalId: string): boolean {
    let pending = false;
    for (const event of detail?.events ?? []) {
      if (approvalEventId(event) !== approvalId) continue;
      const payload = eventPayload(event);
      const kind = text(event.type ?? event.kind ?? payload.kind).toLowerCase();
      if (kind === "approval") pending = true;
      if (kind === "approval.resolved") pending = false;
    }
    return pending;
  }

  function approvalDecision(approvalId: string): string {
    let decision = "";
    for (const event of detail?.events ?? []) {
      if (approvalEventId(event) !== approvalId) continue;
      const payload = eventPayload(event);
      const kind = text(event.type ?? event.kind ?? payload.kind).toLowerCase();
      if (kind === "approval.resolved") decision = text(payload.decision, "resolved");
    }
    return decision;
  }

  function approvalDecisionLabel(approvalId: string): string {
    const decision = approvalDecision(approvalId).toLowerCase();
    if (decision === "approve" || decision === "approved" || decision === "accept" || decision === "accepted") return "Approved";
    if (decision === "deny" || decision === "denied" || decision === "decline" || decision === "declined") return "Denied";
    return "Approval resolved";
  }

  function artifactById(id: string): Artifact | null {
    return detail?.artifacts.find((artifact) => artifact.id === id) ?? null;
  }

  function openArtifact(artifact: Artifact | null): void {
    if (!artifact) return;
    readerArtifact = artifact;
    void setLayout("reader_open", true);
  }

  function openTask(id: string): void {
    selectedId = id;
    activateTaskContext(id);
    focusZone = "threads";
    labelsOpen = false;
    permissionsOpen = false;
    notesEditing = false;
    todoPicked = {};
    void loadDetail(id);
  }

  function toggleFold(id: string): void {
    const next = new Set(folded);
    if (next.has(id)) next.delete(id); else next.add(id);
    folded = next;
  }

  function togglePicked(id: string): void {
    const next = new Set(picked);
    if (next.has(id)) next.delete(id); else next.add(id);
    picked = next;
  }

  function moveTask(direction: number): void {
    const rows = [...activeRows, ...waitingRows, ...inboxRows];
    if (!rows.length) return;
    const index = Math.max(0, rows.findIndex((row) => row.task.id === selectedId));
    const next = rows[Math.max(0, Math.min(rows.length - 1, index + direction))];
    if (next) openTask(next.task.id);
  }

  function moveTodo(direction: number): void {
    const total = detail?.todos.length ?? 0;
    if (!total) return;
    todoFocus = Math.max(0, Math.min(total - 1, todoFocus + direction));
  }

  function selectedTodoIds(): string[] {
    const todos = detail?.todos ?? [];
    const pickedIds = Object.keys(todoPicked);
    return pickedIds.length ? pickedIds : todos[todoFocus] ? [todos[todoFocus].id] : [];
  }

  function showToast(message: string): void {
    toast = message;
    if (toastTimer) clearTimeout(toastTimer);
    toastTimer = setTimeout(() => { toast = ""; }, 6000);
  }

  function updateDraft(value: string): void {
    if (!selectedId) return;
    draftByTask = { ...draftByTask, [selectedId]: value };
  }

  function selectConnection(value: string): void {
    selectedConnectionId = value;
    if (selectedId) updateTaskContext(selectedId, { connectionId: value });
  }

  function openCreate(parentId: string | null = null, initialTitle = ""): void {
    createParentId = parentId;
    createTitle = initialTitle;
    createLabels = "";
    createPermission = "inherit";
    createOpen = true;
    labelsOpen = false;
    setTimeout(() => document.getElementById("create-title")?.focus(), 0);
  }

  function parseLabels(raw: string): Record<string, string> {
    return Object.fromEntries(raw.split(",").map((value) => value.trim()).filter(Boolean).map((name) => [name, labelColor(name, "")]));
  }

  async function createTask(): Promise<void> {
    if (!createTitle.trim() || createBusy) return;
    createBusy = true;
    try {
      const permissions = createPermission === "inherit" ? {} : { mode: createPermission };
      const created = await request<unknown>("task.create", {
        title: createTitle.trim(),
        ...(createParentId ? { parent_task_id: createParentId } : {}),
        labels: parseLabels(createLabels),
        permissions,
      });
      const task = object(created).task ? object(created).task : created;
      const createdId = text(object(task).id ?? object(task).task_id);
      createOpen = false;
      await loadSnapshot();
      if (createdId) openTask(createdId);
      showToast(createParentId ? "Child task created." : "Task created.");
    } catch (error) {
      showToast(errorText(error));
    } finally {
      createBusy = false;
    }
  }

  async function updateTask(taskId: string, changes: Record<string, unknown>): Promise<void> {
    try {
      await request("task.update", { task_id: taskId, changes });
      await loadSnapshot(false);
      await loadDetail(taskId);
    } catch (error) {
      showToast(errorText(error));
    }
  }

  async function archiveTask(taskId: string, archived = true): Promise<void> {
    const ids = archived ? descendants(taskId) : [taskId];
    try {
      for (const id of ids) await request("task.archive", { task_id: id, archived });
      if (archived) undoIds = ids;
      await loadSnapshot(false);
      if (archived) showToast(ids.length > 1 ? `Archived ${ids.length} tasks.` : "Task archived.");
    } catch (error) {
      showToast(errorText(error));
    }
  }

  async function undoArchive(): Promise<void> {
    const ids = undoIds;
    undoIds = [];
    for (const id of ids) {
      try { await request("task.archive", { task_id: id, archived: false }); } catch { /* authoritative reload reports any failure */ }
    }
    await loadSnapshot(false);
    showToast("Archive undone.");
  }

  async function toggleLabel(name: string): Promise<void> {
    const targets = picked.size ? [...picked] : selectedId ? [selectedId] : [];
    for (const id of targets) {
      const task = taskById(id);
      if (!task) continue;
      const labels = { ...object(task.labels) };
      if (Object.prototype.hasOwnProperty.call(labels, name)) delete labels[name]; else labels[name] = labelColor(name, "");
      await updateTask(id, { labels });
    }
  }

  async function addLabel(): Promise<void> {
    const name = labelDraft.trim();
    if (!name) return;
    labelDraft = "";
    await toggleLabel(name);
  }

  async function setPermission(modeValue: PermissionMode | null): Promise<void> {
    if (!selectedId) return;
    if (modeValue === "full_access" && !window.confirm("Allow this task full access? Its adapter may use files and network outside the workspace.")) return;
    await updateTask(selectedId, { permissions: modeValue ? { mode: modeValue } : {} });
    permissionsOpen = false;
  }

  async function setSetting(key: string, value: unknown): Promise<void> {
    settings = { ...settings, [key]: value };
    try { await request("settings.update", { changes: { [key]: value } }); } catch (error) { showToast(errorText(error)); }
  }

  async function setLayout(key: "rail_open" | "reader_open", value: boolean): Promise<void> {
    if (key === "rail_open") railOpen = value;
    else readerOpen = value;
    await setSetting(key, value);
  }

  async function loadStats(): Promise<void> {
    try { stats = object(await request("stats")); } catch { stats = null; }
  }

  async function setDefaultPermission(value: PermissionMode): Promise<void> {
    if (value === "full_access" && !window.confirm("Use full access for new top-level tasks? You can override each task later.")) return;
    defaultPermission = value;
    await setSetting("default_permission", value);
  }

  async function setTheme(nextMode = mode, nextPalette = palette): Promise<void> {
    mode = nextMode;
    palette = nextPalette;
    localStorage.setItem("yakshed.mode", nextMode);
    localStorage.setItem("yakshed.palette", nextPalette);
    await setSetting("theme", nextPalette);
  }

  async function chooseWorkspacePath(targetTaskId = selectedId): Promise<string | null> {
    const path = await chooseWorkspace();
    if (!path || selectedId !== targetTaskId) return null;
    workspacePath = path;
    if (targetTaskId) updateTaskContext(targetTaskId, { workspace: path });
    return path;
  }

  async function submitPrompt(): Promise<void> {
    if (!selectedId || !currentDraft.trim() || !selectedConnectionId || runBusy || !resumeState.resumable) return;
    const taskId = selectedId;
    const connectionId = selectedConnectionId;
    const prompt = currentDraft.trim();
    let workspace = taskContextById[taskId]?.workspace ?? workspacePath;
    runBusy = true;
    try {
      if (!workspace) {
        const chosen = await chooseWorkspacePath(taskId);
        if (selectedId !== taskId) return;
        workspace = chosen ?? taskContextById[taskId]?.workspace ?? workspacePath;
      }
      if (!workspace) { showToast("Choose a workspace before starting a run."); return; }
      const session = detail?.sessions.slice().reverse().find((item) => item.connection_id === connectionId);
      const sessionId = text(session?.id, "");
      await request("run.start", {
        task_id: taskId,
        connection_id: connectionId,
        prompt,
        workspace,
        ...(sessionId ? { session_id: sessionId } : {}),
      });
      updateTaskContext(taskId, { workspace, connectionId }, false);
      const nextDirty = { ...dirtyTaskContext };
      delete nextDirty[taskId];
      dirtyTaskContext = nextDirty;
      if (selectedId === taskId && draftByTask[taskId] === prompt) draftByTask = { ...draftByTask, [taskId]: "" };
      await loadDetail(taskId);
    } catch (error) {
      showToast(errorText(error));
    } finally {
      runBusy = false;
    }
  }

  async function interruptRun(): Promise<void> {
    const runId = text(currentRun?.id ?? currentRun?.run_id);
    const taskId = selectedId;
    if (!runId) return;
    try {
      await request("run.interrupt", { run_id: runId });
      if (taskId && selectedId === taskId) await loadDetail(taskId);
    } catch (error) { showToast(errorText(error)); }
  }

  async function respondApproval(approvalId: string, decision: "approve" | "deny"): Promise<void> {
    const taskId = selectedId;
    try {
      await request("approval.respond", { approval_id: approvalId, decision, answers: {} });
      if (taskId && selectedId === taskId) await loadDetail(taskId);
    } catch (error) { showToast(errorText(error)); }
  }

  async function saveNote(): Promise<void> {
    const taskId = selectedId;
    if (!taskId || noteDirtyTask !== taskId || !noteDirty) { notesEditing = false; return; }
    try {
      await request("note.save", { task_id: taskId, body: noteDraft });
      noteDirty = false;
      noteDirtyTask = null;
      if (selectedId === taskId) {
        notesEditing = false;
        await loadDetail(taskId);
      }
    } catch (error) { showToast(errorText(error)); }
  }

  async function addTodo(): Promise<void> {
    const taskId = selectedId;
    if (!taskId || !todoDraft.trim()) return;
    try {
      await request("todo.create", { task_id: taskId, text: todoDraft.trim() });
      if (selectedId === taskId) {
        todoDraft = "";
        await loadDetail(taskId);
      }
    } catch (error) { showToast(errorText(error)); }
  }

  async function toggleTodo(todo: Todo): Promise<void> {
    const taskId = selectedId;
    try {
      await request("todo.update", { todo_id: todo.id, done: !todo.done });
      if (taskId && selectedId === taskId) await loadDetail(taskId);
    } catch (error) { showToast(errorText(error)); }
  }

  function beginTodoEdit(todo: Todo): void {
    editingTodoId = todo.id;
    todoTextDraft = todo.text;
    todoNoteDraft = todo.note ?? "";
  }

  async function saveTodo(todo: Todo): Promise<void> {
    if (!todoTextDraft.trim()) return;
    const taskId = selectedId;
    try {
      await request("todo.update", { todo_id: todo.id, text: todoTextDraft.trim(), note: todoNoteDraft });
      if (selectedId === taskId) {
        editingTodoId = null;
        await loadDetail(taskId ?? "");
      }
    } catch (error) { showToast(errorText(error)); }
  }

  async function deleteTodo(todo: Todo): Promise<void> {
    const taskId = selectedId;
    try {
      await request("todo.delete", { todo_id: todo.id });
      if (taskId && selectedId === taskId) await loadDetail(taskId);
    } catch (error) { showToast(errorText(error)); }
  }

  function sendTodoList(): void {
    const lines = (detail?.todos ?? []).filter((todo) => !todo.done).map((todo) => `- ${todo.text}${todo.note ? `\n  ${todo.note.replaceAll("\n", "\n  ")}` : ""}`);
    if (todoDraft.trim()) lines.push(`- ${todoDraft.trim()}`);
    if (!lines.length) return;
    updateDraft(`Next, in this order:\n${lines.join("\n")}`);
    focusZone = "threads";
    showToast("Todo list added to the composer.");
  }

  async function createConnection(): Promise<void> {
    if (!connectionName.trim() || !connectionAdapter || connectionBusy) return;
    connectionBusy = true;
    try {
      await request("connection.create", { name: connectionName.trim(), adapter: connectionAdapter, ...(connectionModel.trim() ? { model: connectionModel.trim() } : {}), options: {} });
      connectionName = "";
      connectionModel = "";
      connectionFormOpen = false;
      await loadSnapshot(false);
      showToast("Connection added.");
    } catch (error) { showToast(errorText(error)); }
    finally { connectionBusy = false; }
  }

  async function loginAdapter(adapter: string): Promise<void> {
    try {
      const result = await request<unknown>("adapter.login", { adapter });
      const row = object(result);
      const message = row.supported === false ? `unavailable · ${text(row.reason, "login is not supported")}` : text(row.message ?? row.status ?? row.auth_url ?? row.login_url, "login started");
      adapterStatus = { ...adapterStatus, [adapter]: message };
    } catch (error) { adapterStatus = { ...adapterStatus, [adapter]: errorText(error) }; }
  }

  async function inspectAdapter(adapter: string): Promise<void> {
    try {
      const result = await request<unknown>("adapter.status", { adapter });
      const row = object(result);
      const state = row.authenticated === true ? "authenticated" : row.authenticated === false ? "not authenticated" : text(row.status ?? row.state, "status available");
      const suffix = row.error ? ` · ${text(row.error)}` : "";
      adapterStatus = { ...adapterStatus, [adapter]: `${state}${suffix}` };
    } catch (error) { adapterStatus = { ...adapterStatus, [adapter]: errorText(error) }; }
  }

  function openSearch(): void {
    searchOpen = true;
    setTimeout(() => document.getElementById("search-input")?.focus(), 0);
  }

  async function submitSearch(): Promise<void> {
    search = searchInput.trim();
    if (search) includeArchived = true;
    await loadSnapshot();
  }

  function formatEventDate(value: string): string {
    if (!value) return "";
    const date = /^\d+$/.test(value) ? new Date(Number(value)) : new Date(value);
    return Number.isNaN(date.getTime()) ? value : date.toLocaleTimeString([], { hour: "numeric", minute: "2-digit" });
  }

  function startResize(event: PointerEvent): void {
    resizing = true;
    const start = event.clientX;
    const initial = sidebarWidth;
    const move = (current: PointerEvent) => { sidebarWidth = Math.max(230, Math.min(480, initial + current.clientX - start)); };
    const done = () => { resizing = false; window.removeEventListener("pointermove", move); window.removeEventListener("pointerup", done); void setSetting("sidebar_width", sidebarWidth); };
    window.addEventListener("pointermove", move);
    window.addEventListener("pointerup", done, { once: true });
  }

  function onComposerKey(event: KeyboardEvent): void {
    const commandSend = text(settings.send_shortcut, "enter") === "command_enter";
    const shouldSend = event.key === "Enter" && !event.shiftKey && (commandSend ? (event.metaKey || event.ctrlKey) : !(event.metaKey || event.ctrlKey));
    if (shouldSend) {
      event.preventDefault();
      void submitPrompt();
    }
  }

  function handleKeydown(event: KeyboardEvent): void {
    const target = event.target as HTMLElement | null;
    const key = event.key.toLowerCase();
    const typing = target?.tagName === "INPUT" || target?.tagName === "TEXTAREA" || target?.isContentEditable;
    if ((event.metaKey || event.ctrlKey) && event.key === ",") { event.preventDefault(); settingsOpen = !settingsOpen; return; }
    if (event.key === "Escape") {
      if (createOpen || settingsOpen || helpOpen) { createOpen = false; settingsOpen = false; helpOpen = false; return; }
      labelsOpen = false; permissionsOpen = false; clockOpen = false; usageOpen = false; searchOpen = false; return;
    }
    if (typing) {
      if (event.key === "/" && target?.id === "search-input") return;
      return;
    }
    if (event.key === "Tab" && (target === document.body || Boolean(target?.closest(".task-nav, .todo-rail")))) { event.preventDefault(); focusZone = focusZone === "todos" ? "threads" : "todos"; return; }
    if (createOpen || settingsOpen || helpOpen) return;
    if (key === "?") { helpOpen = !helpOpen; return; }
    if (key === "/") { event.preventDefault(); openSearch(); return; }
    if (key === "c") { openCreate(event.shiftKey ? selectedId : null); return; }
    if (key === "]") { void setLayout("rail_open", !railOpen); focusZone = "todos"; return; }
    if (event.key === "\\") { void setLayout("reader_open", !readerOpen); return; }
    if (key === "d") { void setTheme(mode === "dark" ? "light" : "dark"); return; }
    if (key === "t") {
      const index = palettes.findIndex((item) => item.id === palette);
      const next = palettes[(index + (event.shiftKey ? palettes.length - 1 : 1)) % palettes.length];
      void setTheme(mode, next.id);
      return;
    }
    if (event.key === ",") { settingsOpen = !settingsOpen; return; }
    if (key === "p") { permissionsOpen = !permissionsOpen; return; }
    if (key === "z") { if (undoIds.length) void undoArchive(); return; }
    if (focusZone === "todos") {
      if (key === "j" || event.key === "ArrowDown") moveTodo(1);
      else if (key === "k" || event.key === "ArrowUp") moveTodo(-1);
      else if (key === "x") { const todo = detail?.todos[todoFocus]; if (todo) todoPicked = { ...todoPicked, [todo.id]: !todoPicked[todo.id] }; }
      else if (key === "e") { const todo = detail?.todos[todoFocus]; if (todo) void toggleTodo(todo); }
      else if (event.key === "Delete" || event.key === "Backspace") { const todo = detail?.todos[todoFocus]; if (todo) void deleteTodo(todo); }
      return;
    }
    if (key === "j" || event.key === "ArrowDown") moveTask(1);
    else if (key === "k" || event.key === "ArrowUp") moveTask(-1);
    else if (event.key === "ArrowRight" && selectedId) folded = new Set([...folded].filter((id) => id !== selectedId));
    else if (event.key === "ArrowLeft" && selectedId) { const task = taskById(selectedId); if (task && childrenOf(task.id).length && !folded.has(task.id)) toggleFold(task.id); else if (task?.parent_task_id) openTask(task.parent_task_id); }
    else if (key === "x" && selectedId) togglePicked(selectedId);
    else if (key === "l") labelsOpen = !labelsOpen;
    else if (key === "e" && selectedId) void archiveTask(selectedId);
  }

  onMount(() => {
    const systemDark = window.matchMedia?.("(prefers-color-scheme: dark)").matches ?? true;
    mode = systemDark ? "dark" : "light";
    void loadSnapshot();
    void subscribe((event) => {
      if (event.event === "changed" || typeof event.revision === "number") void loadSnapshot(false);
    }).then((unsubscribe) => { stopEvents = unsubscribe; });
    window.addEventListener("keydown", handleKeydown);
    return () => { stopEvents?.(); window.removeEventListener("keydown", handleKeydown); };
  });
</script>

<svelte:head><title>{selectedTask ? `${selectedTask.title} · YakShed` : "YakShed"}</title></svelte:head>

<div class="app" data-theme={`${palette}-${mode}`} class:resizing>
  <aside class="sidebar" style={`width:${sidebarWidth}px;flex-basis:${sidebarWidth}px`} aria-label="Task list">
    <div class="brand-row">
      <div class="brand-mark" aria-hidden="true"></div>
      <strong>YakShed</strong>
      <button class="palette-button" type="button" aria-label="Open appearance palette" title="Palette · t to cycle" onclick={() => settingsOpen = true}>{themeName} {mode === "dark" ? "●" : "○"}</button>
    </div>

    <div class="sidebar-actions">
      {#if searchOpen}
        <form class="search-form" onsubmit={(event) => { event.preventDefault(); void submitSearch(); }}>
          <span aria-hidden="true">/</span>
          <input id="search-input" bind:value={searchInput} placeholder="Search tasks and archived" aria-label="Search tasks" />
          <button type="submit" aria-label="Submit search">↵</button>
        </form>
      {:else}
        <button class="search-button" type="button" onclick={openSearch}><span>/</span><span>Search everything</span><kbd>/</kbd></button>
      {/if}
      <button class="new-task-button" type="button" onclick={() => openCreate()}><kbd>c</kbd><span>New task</span><span class="grow"></span><small>⇧c child</small></button>
      {#if search || includeArchived}
        <div class="search-state"><span>{search ? `Results for “${search}”` : "Showing archived"}</span><button type="button" onclick={() => { search = ""; searchInput = ""; includeArchived = false; void loadSnapshot(); }}>clear</button></div>
      {/if}
    </div>

    <nav class="task-nav" aria-label="Tasks">
      {#if !tasks.length && !loading}
        <div class="sidebar-empty">No tasks yet.<br /><button type="button" onclick={() => openCreate()}>Make the first one →</button></div>
      {:else}
        {#each [{ title: "Active", rows: activeRows, count: tasks.filter((task) => !task.archived && task.status === "active").length }, { title: "Waiting", rows: waitingRows, count: tasks.filter((task) => !task.archived && task.status === "waiting").length }, { title: "Inbox", rows: inboxRows, count: tasks.filter((task) => !task.archived && !["active", "waiting"].includes(task.status)).length }] as section}
          {#if section.rows.length || section.count}
            <div class="task-section">
              <div class="section-label"><span>{section.title}</span><span class="mono">{section.count}</span><i></i></div>
              {#each section.rows as row (row.task.id)}
                <TaskRow {row} selectedId={selectedId} focusZone={focusZone} picked={picked.has(row.task.id)} labels={labelEntries(row.task)} hasChildren={childrenOf(row.task.id).length > 0} folded={folded.has(row.task.id)} onopen={() => openTask(row.task.id)} ontoggle={() => togglePicked(row.task.id)} onfold={() => toggleFold(row.task.id)} />
              {/each}
            </div>
          {/if}
        {/each}
        {#if includeArchived && archivedRows.length}
          <div class="task-section archived-section">
            <div class="section-label"><span>Archived</span><span class="mono">{archivedRows.length}</span><i></i></div>
            {#each archivedRows as row (row.task.id)}
              <TaskRow {row} selectedId={selectedId} focusZone={focusZone} picked={picked.has(row.task.id)} labels={labelEntries(row.task)} hasChildren={false} folded={false} onopen={() => openTask(row.task.id)} ontoggle={() => togglePicked(row.task.id)} onfold={() => undefined} />
            {/each}
          </div>
        {/if}
      {/if}
      <div class="task-section label-section">
        <div class="section-label"><span>Labels</span><i></i></div>
        {#each allLabels as label}
          <button class="label-filter" type="button" onclick={() => { searchInput = label; search = label; void loadSnapshot(); }}><span class="label-dot" style={`background:${labelColor(label, "")}`}></span><span>{label}</span><span class="grow"></span><span class="mono">{tasks.filter((task) => labelEntries(task).some((item) => item.name === label)).length}</span></button>
        {/each}
        <button class="archive-link" type="button" onclick={() => { includeArchived = !includeArchived; void loadSnapshot(); }}><span>▤</span><span>{includeArchived ? "Hide archived" : "Archived"}</span><span class="grow"></span><span class="mono">{tasks.filter((task) => task.archived).length}</span></button>
      </div>
    </nav>

    <div class="sidebar-footer">
      <button type="button" onclick={() => { settingsOpen = true; settingsSection = "stats"; void loadStats(); }}><span class="avatar">YS</span><span class="account-label">Local workspace</span><span class="grow"></span><kbd>?</kbd></button>
    </div>
    <button class="resize-handle" type="button" aria-label="Resize task sidebar" onpointerdown={startResize}></button>
  </aside>

  <main class="center-pane">
    {#if !selectedTask}
      <section class="welcome-surface">
        <div class="welcome-mark" aria-hidden="true">✦</div>
        <p class="eyebrow">LOCAL WORKBENCH</p>
        <h1>Make a task. Keep the context.</h1>
        <p class="welcome-copy">YakShed keeps work in a durable graph so every run, note, decision, and child task is ready when you return.</p>
        <form class="welcome-form" onsubmit={(event) => { event.preventDefault(); openCreate(null, createTitle); }}>
          <input value={createTitle} oninput={(event) => createTitle = event.currentTarget.value} placeholder="What are you working on?" aria-label="New task title" />
          <button type="submit">Create task <kbd>c</kbd></button>
        </form>
        {#if bridgeError}<p class="bridge-message">{bridgeError} The desktop shell will provide the task store.</p>{/if}
      </section>
    {:else}
      <header class="thread-header">
        <div class="breadcrumbs">
          {#each ancestors(selectedTask) as parent (parent.id)}<button type="button" onclick={() => openTask(parent.id)}>{parent.title}</button><span>›</span>{/each}
          <strong>{selectedTask.title}</strong>
          {#each labelEntries(selectedTask).slice(0, 3) as label (label.name)}<span class:inherited={!label.own} class="chip" style={`--chip-color:${label.color}`}>{label.name}</span>{/each}
          <button class="label-add" type="button" aria-label="Edit labels" onclick={() => labelsOpen = !labelsOpen}>+ label <kbd>l</kbd></button>
        </div>
        <div class="header-actions">
          <span class={`status-pill ${selectedTask.status}`}>{statusName(selectedTask)}</span>
          <button type="button" onclick={() => void archiveTask(selectedTask.id, !selectedTask.archived)}>{selectedTask.archived ? "Restore" : "Archive"} <kbd>e</kbd></button>
          <button type="button" class:active={railOpen} onclick={() => void setLayout("rail_open", !railOpen)}>Todos <span class="mono">]</span></button>
          <button type="button" class:active={readerOpen} onclick={() => void setLayout("reader_open", !readerOpen)}>Reader <span class="mono">\</span></button>
        </div>
      </header>

      <div class="timeline-scroll">
        <div class="timeline">
          {#if detailLoading}<p class="loading-line">Loading task…</p>{/if}
          {#if detailError}<p class="error-line">{detailError}</p>{/if}
          {#if detail && !detail.events.length}
            <div class="empty-timeline"><span class="eyebrow">READY</span><h2>This task is ready to run.</h2><p>Choose a connection and workspace below, then send the first prompt.</p></div>
          {/if}
          {#each timelineEvents as event, index (text(event.id, String(index)))}
            {@const view = eventView(event)}
            {#if !isApprovalResolution(event) && (view.body || view.kind !== "activity")}
              <article class={`timeline-event ${view.kind}`}>
                <div class="event-meta"><span>{view.kind}</span>{#if view.meta}<time>{formatEventDate(view.meta)}</time>{/if}<i></i></div>
                {#if view.body && view.kind !== "tool" && view.kind !== "approval"}<p>{view.body}</p>{/if}
                {#if view.kind === "tool"}<div class="tool-card"><span class="tool-glyph">⌁</span><span>{view.body || "Workspace activity"}</span></div>{/if}
                {#if view.kind === "artifact" && artifactById(view.artifactId)}<button class="artifact-link" type="button" onclick={() => openArtifact(artifactById(view.artifactId))}>OPEN IN READER · {artifactById(view.artifactId)?.name}</button>{/if}
                {#if view.kind === "approval" && view.approvalId}
                  {#if approvalPending(view.approvalId)}
                    <div class="approval-card"><div><strong>Waiting for your approval</strong><p>{view.body || "This run is paused until you decide."}</p></div><div class="approval-actions"><button type="button" onclick={() => void respondApproval(view.approvalId, "approve")}>Approve</button><button class="secondary" type="button" onclick={() => void respondApproval(view.approvalId, "deny")}>Deny</button></div></div>
                  {:else}<div class="approval-card approval-complete"><div><strong>{approvalDecisionLabel(view.approvalId)}</strong><p>{view.body || "The approval was resolved."}</p></div></div>{/if}
                {/if}
              </article>
            {/if}
          {/each}
          {#if selectedTask.status === "waiting" && !(detail?.events ?? []).some((event) => eventKind(event) === "approval")}
            <div class="waiting-callout"><strong>WAITING ON YOU</strong><span>The task is paused until the next decision arrives.</span></div>
          {/if}
        </div>
      </div>

      <footer class="composer-area">
        <div class="composer-meta">
          <button class="clock-button" type="button" onclick={() => usageOpen = !usageOpen}><span class:live={currentRun} class="clock-dot"></span><span class="mono">{formatDuration(detail?.duration.own_ms ?? 0)}</span><span>{currentRun ? "working" : "this task"}</span>{#if detail?.duration.subtree_ms && detail.duration.subtree_ms !== detail.duration.own_ms}<span class="separator"></span><span class="mono accent-text">{formatDuration(detail.duration.subtree_ms)}</span><span>branch</span>{/if}</button>
          <span class="grow"></span>
          <div class="permission-wrap"><button class="permission-chip" type="button" onclick={() => permissionsOpen = !permissionsOpen}><span class={`permission-dot ${effectivePermission}`}></span>{permissionName(effectivePermission)}{#if !selectedTask.permissions?.mode}<small>inherited</small>{/if}<span class="mono">p</span></button>
            {#if permissionsOpen}<div class="popover permission-popover"><p>This task’s permission applies to its runs and children unless they override it.</p>{#each permissionModes as permission}<button type="button" class:selected={effectivePermission === permission.id && Boolean(selectedTask.permissions?.mode)} onclick={() => void setPermission(permission.id)}><span class={`permission-dot ${permission.id}`}></span><span><strong>{permission.name}</strong><small>{permission.description}</small></span>{#if effectivePermission === permission.id && Boolean(selectedTask.permissions?.mode)}<span>✓</span>{/if}</button>{/each}<button class="clear-override" type="button" onclick={() => void setPermission(null)}>Clear override · inherit</button></div>{/if}
          </div>
          <button class="expand-button" type="button" aria-label="Open settings" onclick={() => { settingsOpen = true; settingsSection = "general"; }}>⌘,</button>
        </div>
        {#if usageOpen}<div class="usage-popover"><div class="section-label"><span>Where the time and usage went</span><i></i></div><div class="usage-grid"><span>This task</span><strong>{formatDuration(detail?.duration.own_ms ?? 0)}</strong><span>Branch total</span><strong>{formatDuration(detail?.duration.subtree_ms ?? 0)}</strong>{#if detail?.usage?.own}<span>Tokens, this task <em class="usage-quality">{usageCompleteness(detail.usage.own)}</em></span><strong>{formatNumber(detail.usage.own.total_tokens)}</strong>{/if}{#if detail?.usage?.subtree}<span>Tokens, branch <em class="usage-quality">{usageCompleteness(detail.usage.subtree)}</em></span><strong>{formatNumber(detail.usage.subtree.total_tokens)}</strong>{/if}</div></div>{/if}
        {#if !resumeState.resumable}<p id="resume-hint" class="resume-hint" role="status">{resumeState.reason}</p>{/if}
        <form class="composer" onsubmit={(event) => { event.preventDefault(); void submitPrompt(); }}>
          <textarea value={currentDraft} oninput={(event) => updateDraft(event.currentTarget.value)} onkeydown={onComposerKey} placeholder={selectedConnectionId ? "Reply, or start the next run…" : "Add a connection to start a run…"} aria-label="Task prompt" aria-describedby={resumeState.resumable ? undefined : "resume-hint"}></textarea>
          <div class="composer-footer"><span class="send-hint">↵ send</span><select value={selectedConnectionId} onchange={(event) => selectConnection(event.currentTarget.value)} aria-label="Connection"><option value="">No connection</option>{#each connections as connection (connection.id)}<option value={connection.id}>{connection.name}{connection.model ? ` · ${connection.model}` : ""}</option>{/each}</select><span class="grow"></span><button class="workspace-button" type="button" onclick={() => void chooseWorkspacePath()} title={workspacePath || "Choose workspace"}>⌂ {workspacePath ? workspacePath.split("/").at(-1) : "Choose workspace"}</button>{#if currentRun && supports("interrupt")}<button class="stop-button" type="button" onclick={() => void interruptRun()}>Stop</button>{:else}<button class="send-button" type="submit" disabled={!resumeState.resumable || !currentDraft.trim() || !selectedConnectionId || runBusy} title={resumeState.reason}>{runBusy ? "Starting…" : "Run"}</button>{/if}</div>
        </form>
      </footer>
    {/if}
  </main>

  {#if selectedTask && railOpen && !readerOpen}
    <aside class="todo-rail" aria-label="Todos and notes">
      <div class="rail-heading"><span class="eyebrow">TODO</span><span class="mono">{pendingTodoCount}/{detail?.todos.length ?? 0}</span><span class="grow"></span><button type="button" aria-label="Close todo rail" onclick={() => void setLayout("rail_open", false)}>×</button></div>
      <div class="todo-list">
        {#each detail?.todos ?? [] as todo, index (todo.id)}
          <div class:focused={focusZone === "todos" && index === todoFocus} class:todo-picked={todoPicked[todo.id]} class="todo-row">
            <button class="todo-pick" type="button" aria-label="Select todo" onclick={() => todoPicked = { ...todoPicked, [todo.id]: !todoPicked[todo.id] }}>×</button>
            <button class:done={todo.done} class="todo-check" type="button" aria-label={todo.done ? "Mark todo open" : "Mark todo done"} onclick={() => void toggleTodo(todo)}>{todo.done ? "✓" : ""}</button>
            {#if editingTodoId === todo.id}<div class="todo-edit"><input value={todoTextDraft} oninput={(event) => todoTextDraft = event.currentTarget.value} onkeydown={(event) => { if (event.key === "Enter") void saveTodo(todo); }} /><textarea value={todoNoteDraft} oninput={(event) => todoNoteDraft = event.currentTarget.value} placeholder="Todo note"></textarea><div><button type="button" onclick={() => void saveTodo(todo)}>save</button><button class="secondary" type="button" onclick={() => editingTodoId = null}>cancel</button></div></div>{:else}<button class:completed={todo.done} class="todo-copy" type="button" onclick={() => { focusZone = "todos"; todoFocus = index; }} ondblclick={() => beginTodoEdit(todo)}><span>{todo.text}</span>{#if todo.note}<small>{todo.note}</small>{/if}</button><button class="todo-more" type="button" aria-label="Edit todo" onclick={() => beginTodoEdit(todo)}>•••</button>{/if}
          </div>
        {:else}<p class="rail-empty">Nothing here yet. Add the next small thing.</p>{/each}
      </div>
      <div class="todo-add"><textarea value={todoDraft} oninput={(event) => todoDraft = event.currentTarget.value} onkeydown={(event) => { if (event.key === "Enter" && !event.shiftKey) { event.preventDefault(); void addTodo(); } }} placeholder="Add a todo · ↵ saves" aria-label="New todo"></textarea><div><span>↵ next item</span><button type="button" onclick={sendTodoList} disabled={!pendingTodoCount && !todoDraft.trim()}>send list</button></div></div>
      <div class="notes-heading"><span class="eyebrow">NOTES</span><span class="grow"></span><button type="button" onclick={() => notesEditing ? void saveNote() : notesEditing = true}>{notesEditing ? "done" : "edit"}</button></div>
      <div class="notes-body">{#if notesEditing}<textarea value={noteDraft} oninput={(event) => { noteDraft = event.currentTarget.value; noteDirty = true; noteDirtyTask = selectedId; noteDrafts = { ...noteDrafts, [selectedId ?? ""]: noteDraft }; }} placeholder="Markdown notes for this task"></textarea>{:else}{#if noteDraft}<button class="note-render" type="button" onclick={() => notesEditing = true}>{noteDraft}</button>{:else}<button class="note-empty" type="button" onclick={() => notesEditing = true}>No notes on this task. Click to write.</button>{/if}{/if}</div>
    </aside>
  {/if}

  {#if selectedTask && readerOpen}
    <aside class="reader-pane" aria-label="Reader">
      <div class="reader-tabs"><span class="eyebrow">READER</span><span class="grow"></span><button type="button" onclick={() => void setLayout("reader_open", false)}>close <span class="mono">\</span></button></div>
      <div class="artifact-tabs">{#each detail?.artifacts ?? [] as artifact (artifact.id)}<button class:active={readerArtifact?.id === artifact.id} type="button" onclick={() => readerArtifact = artifact}><span class="mono">{text(artifact.kind, "TEXT").toUpperCase()}</span>{artifact.name}</button>{/each}</div>
      {#if readerArtifact}<article class="reader-card"><div class="reader-title"><span class="eyebrow">{text(readerArtifact.kind, "TEXT").toUpperCase()}</span><h2>{readerArtifact.name}</h2></div><pre>{artifactText(readerArtifact)}</pre></article>{:else}<div class="reader-empty"><p>No plan, diff, or text artifact has been recorded for this task yet.</p></div>{/if}
    </aside>
  {/if}

  {#if labelsOpen && selectedTask}<div class="popover labels-popover"><p>Labels on <strong>{selectedTask.title}</strong>. Children inherit them.</p>{#each allLabels as label}<button class:checked={Object.prototype.hasOwnProperty.call(selectedTask.labels, label)} class:inherited={!Object.prototype.hasOwnProperty.call(selectedTask.labels, label) && labelEntries(selectedTask).some((item) => item.name === label)} type="button" onclick={() => void toggleLabel(label)}><span class="label-dot" style={`background:${labelColor(label, "")}`}></span><span>{label}</span><span class="grow"></span>{#if Object.prototype.hasOwnProperty.call(selectedTask.labels, label)}✓{:else if labelEntries(selectedTask).some((item) => item.name === label)}inherited{/if}</button>{/each}<form onsubmit={(event) => { event.preventDefault(); void addLabel(); }}><input bind:value={labelDraft} placeholder="new label" aria-label="New label" /><button type="submit">add</button></form></div>{/if}

  {#if settingsOpen}
    <div class="modal-backdrop" role="presentation" onclick={(event) => { if (event.target === event.currentTarget) settingsOpen = false; }}>
      <div class="settings-modal" role="dialog" aria-modal="true" aria-label="YakShed settings">
        <nav class="settings-nav"><div class="eyebrow">SETTINGS</div>{#each [["general", "General"], ["appearance", "Appearance"], ["permissions", "Permissions"], ["connections", "Connections"], ["keyboard", "Keyboard"], ["stats", "Stats"]] as item}<button class:current={settingsSection === item[0]} type="button" onclick={() => { settingsSection = item[0]; if (item[0] === "stats") void loadStats(); }}>{item[1]}</button>{/each}<span class="grow"></span><p>Settings are a layer over the workbench.<br /><kbd>esc</kbd> closes it.</p></nav>
        <div class="settings-content"><header><h2>{settingsSection[0].toUpperCase() + settingsSection.slice(1)}</h2><button type="button" onclick={() => settingsOpen = false}>Close <kbd>esc</kbd></button></header>
          {#if settingsSection === "general"}<div class="settings-rows"><SettingToggle label="Stay awake while a task runs" description="Keep the machine available for active work." value={Boolean(settings.stay_awake)} ontoggle={(value) => void setSetting("stay_awake", value)} /><SettingToggle label="Keep a menu-bar presence" description="Show active task count while the window is closed." value={Boolean(settings.menubar)} ontoggle={(value) => void setSetting("menubar", value)} /><div class="setting-row"><div><strong>Send with</strong><small>Enter sends; Shift+Enter inserts a newline.</small></div><select value={text(settings.send_shortcut, "enter")} onchange={(event) => void setSetting("send_shortcut", event.currentTarget.value)}><option value="enter">Enter</option><option value="command_enter">Command+Enter</option></select></div></div>
          {:else if settingsSection === "appearance"}<div class="appearance-settings"><div class="setting-row"><div><strong>Mode</strong><small>Use the system at launch, then change it with <kbd>d</kbd>.</small></div><div class="segmented"><button class:current={mode === "light"} type="button" onclick={() => void setTheme("light")}>Light</button><button class:current={mode === "dark"} type="button" onclick={() => void setTheme("dark")}>Dark</button></div></div><div class="setting-label">Palette</div><div class="palette-grid">{#each palettes as item}<button class:current={palette === item.id} type="button" onclick={() => void setTheme(mode, item.id)}><span class="palette-swatch" data-palette={item.id} data-mode={mode}></span><strong>{item[mode === "dark" ? "darkName" : "name"]}</strong><small>{item.description}</small></button>{/each}</div></div>
          {:else if settingsSection === "permissions"}<div class="permission-settings"><p class="settings-intro">New top-level tasks begin here. Children inherit the nearest task override.</p><div class="permission-cards">{#each permissionModes as permission}<button class:current={defaultPermission === permission.id} type="button" onclick={() => void setDefaultPermission(permission.id)}><span class={`permission-dot ${permission.id}`}></span><span><strong>{permission.name}</strong><small>{permission.description}</small></span>{#if defaultPermission === permission.id}✓{/if}</button>{/each}</div>{#if selectedTask}<div class="override-row"><span>Current task</span><strong>{selectedTask.title}</strong><span class="grow"></span><span>{permissionName(effectivePermission)}{selectedTask.permissions?.mode ? " · override" : " · inherited"}</span><button type="button" onclick={() => { settingsOpen = false; permissionsOpen = true; }}>edit</button></div>{/if}</div>
          {:else if settingsSection === "connections"}<div class="connections-settings"><p class="settings-intro">Connections choose where work runs and which model to use. Available abilities come from the selected connection.</p>{#each connections as connection}<div class="connection-row"><div><strong>{connection.name}</strong><small>{adapterFor(connection)?.name ?? connection.adapter}{connection.model ? ` · ${connection.model}` : ""}</small></div><span class="grow"></span><span class="connection-status">{adapterStatus[connection.adapter] ?? "status unknown"}</span><button type="button" onclick={() => void inspectAdapter(connection.adapter)}>status</button>{#if adapterSupports(connection.adapter, "login", "auth")}<button type="button" onclick={() => void loginAdapter(connection.adapter)}>login</button>{/if}</div>{/each}{#if !connections.length}<p class="empty-setting">No connections yet. Add one when a work connection is available.</p>{/if}{#if connectionFormOpen}<form class="connection-form" onsubmit={(event) => { event.preventDefault(); void createConnection(); }}><input bind:value={connectionName} placeholder="Connection name" aria-label="Connection name" /><select bind:value={connectionAdapter} aria-label="Adapter"><option value="">Choose adapter</option>{#each adapters as adapter}<option value={adapter.id}>{adapter.name}</option>{/each}</select><input bind:value={connectionModel} placeholder="Model (optional)" aria-label="Model" /><button type="submit" disabled={connectionBusy}>Add connection</button></form>{:else}<button class="add-connection" type="button" onclick={() => { connectionFormOpen = true; connectionAdapter = adapters[0]?.id ?? ""; }}>+ Add connection</button>{/if}</div>
          {:else if settingsSection === "keyboard"}<div class="key-grid">{#each keyHelp as item}<div><kbd>{item[0]}</kbd><span>{item[1]}</span></div>{/each}</div>
          {:else}<div class="stats-settings"><div class="stats-grid"><div><strong>{formatNumber(statsTaskCounts.active ?? taskCount)}</strong><span>open tasks</span></div><div><strong>{formatNumber(statsTaskCounts.archived ?? tasks.filter((task) => task.archived).length)}</strong><span>archived tasks</span></div><div><strong>{connections.length}</strong><span>connections</span></div><div><strong>{detail?.usage?.subtree && detail.usage.subtree.total_tokens != null ? formatNumber(detail.usage.subtree.total_tokens) : "—"}</strong><span>reported tokens, selected branch <em class="usage-quality">{usageCompleteness(detail?.usage?.subtree)}</em></span></div></div><p class="settings-intro">Stats are computed from persisted tasks and reported usage. A dash means no value was reported.</p></div>{/if}
        </div>
      </div>
    </div>
  {/if}

  {#if createOpen}<div class="modal-backdrop" role="presentation" onclick={(event) => { if (event.target === event.currentTarget) createOpen = false; }}><div class="create-modal" role="dialog" aria-modal="true" aria-label="Create task"><header><div><p class="eyebrow">{createParentId ? "CHILD TASK" : "NEW TASK"}</p><h2>{createParentId ? `Spawn under ${taskById(createParentId)?.title ?? "this task"}` : "Make a task"}</h2></div><button type="button" onclick={() => createOpen = false}>×</button></header><form onsubmit={(event) => { event.preventDefault(); void createTask(); }}><label for="create-title">Title</label><input id="create-title" bind:value={createTitle} placeholder="A clear piece of work" /><label for="create-labels">Labels <small>comma separated</small></label><input id="create-labels" bind:value={createLabels} placeholder="research, today" /><label for="create-permission">Permission</label><select id="create-permission" bind:value={createPermission}><option value="inherit">Inherit from parent</option>{#each permissionModes as permission}<option value={permission.id}>{permission.name}</option>{/each}</select><div class="create-footer"><span class="grow">{createParentId ? "Labels and permissions flow down to children." : "You can change both from the task header."}</span><button class="secondary" type="button" onclick={() => createOpen = false}>Cancel</button><button type="submit" disabled={createBusy || !createTitle.trim()}>{createBusy ? "Creating…" : "Create task"}</button></div></form></div></div>{/if}

  {#if helpOpen}<div class="modal-backdrop help-backdrop" role="presentation" onclick={(event) => { if (event.target === event.currentTarget) helpOpen = false; }}><div class="help-modal" role="dialog" aria-modal="true" aria-label="Keyboard shortcuts"><header><h2>Keyboard</h2><button type="button" onclick={() => helpOpen = false}>Close <kbd>esc</kbd></button></header><div class="key-grid">{#each keyHelp as item}<div><kbd>{item[0]}</kbd><span>{item[1]}</span></div>{/each}</div></div></div>{/if}

  {#if toast}<div class="toast" role="status"><span>{toast}</span>{#if undoIds.length}<button type="button" onclick={() => void undoArchive()}>Undo · z</button>{/if}</div>{/if}
</div>
