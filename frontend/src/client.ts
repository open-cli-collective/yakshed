export type PermissionMode = "read_only" | "workspace_write" | "full_access";

export interface Task {
  id: string;
  parent_task_id: string | null;
  title: string;
  status: string;
  archived: boolean;
  labels: Record<string, string | unknown>;
  permissions: { mode?: PermissionMode; [key: string]: unknown };
  effective_labels?: Record<string, string | unknown>;
  effective_permissions?: { mode?: PermissionMode; [key: string]: unknown };
  created_at?: string;
  updated_at?: string;
  [key: string]: unknown;
}

export interface Connection {
  id: string;
  name: string;
  adapter: string;
  provider?: string;
  model?: string | null;
  options?: Record<string, unknown>;
  [key: string]: unknown;
}

export interface AdapterDescriptor {
  id: string;
  name: string;
  capabilities: string[];
  [key: string]: unknown;
}

export interface Snapshot {
  tasks: unknown[];
  connections: unknown[];
  adapters?: unknown[];
  settings?: Record<string, unknown>;
  revision?: number;
  [key: string]: unknown;
}

export interface TimelineEvent {
  id?: string | number;
  kind?: string;
  type?: string;
  body?: string;
  payload?: Record<string, unknown> | string | null;
  created_at?: string;
  created_at_ms?: number;
  source?: string;
  [key: string]: unknown;
}

export interface Artifact {
  id: string;
  name?: string;
  kind?: string;
  mime?: string | null;
  content?: string | null;
  path?: string | null;
  metadata?: Record<string, unknown>;
  [key: string]: unknown;
}

export interface Todo {
  id: string;
  task_id?: string;
  text: string;
  note?: string;
  done: boolean;
  position?: number;
  [key: string]: unknown;
}

export interface Detail {
  task_id: string;
  task: Task;
  children: Task[];
  sessions: Record<string, unknown>[];
  runs: Record<string, unknown>[];
  events: TimelineEvent[];
  notes: Record<string, unknown>[];
  todos: Todo[];
  artifacts: Artifact[];
  usage: { own?: Record<string, unknown> | null; subtree?: Record<string, unknown> | null };
  duration: { own_ms?: number; subtree_ms?: number };
  [key: string]: unknown;
}

export interface TaskContext {
  workspace?: string;
  connectionId?: string;
}

export interface TaskResumeState {
  resumable: boolean;
  reason?: string;
}

export interface BridgeEvent {
  event?: string;
  revision?: number;
  [key: string]: unknown;
}

export interface YakshedBridge {
  request<T = unknown>(method: string, params?: Record<string, unknown>): Promise<T>;
  onEvent(callback: (event: BridgeEvent) => void): (() => void) | Promise<() => void>;
  chooseWorkspace(): Promise<string | null>;
}

declare global {
  interface Window {
    yakshed?: YakshedBridge;
  }
}

export function bridge(): YakshedBridge | null {
  return typeof window !== "undefined" ? window.yakshed ?? null : null;
}

export async function request<T = unknown>(method: string, params: Record<string, unknown> = {}): Promise<T> {
  const api = bridge();
  if (!api) throw new Error("The YakShed desktop bridge is unavailable.");
  return api.request<T>(method, params);
}

export async function subscribe(callback: (event: BridgeEvent) => void): Promise<() => void> {
  const api = bridge();
  if (!api) return () => undefined;
  return (await api.onEvent(callback)) ?? (() => undefined);
}

export async function chooseWorkspace(): Promise<string | null> {
  return bridge()?.chooseWorkspace() ?? null;
}

function object(value: unknown): Record<string, unknown> {
  return value && typeof value === "object" ? value as Record<string, unknown> : {};
}

function text(value: unknown, fallback = ""): string {
  return typeof value === "string" ? value : value == null ? fallback : String(value);
}

export function normalizeTask(value: unknown): Task {
  const row = object(value);
  const labels = object(row.labels);
  const permissions = object(row.permissions) as Task["permissions"];
  return {
    ...row,
    id: text(row.id ?? row.task_id),
    parent_task_id: (row.parent_task_id ?? row.parent_id ?? null) as string | null,
    title: text(row.title, "Untitled task"),
    status: text(row.status, "inbox"),
    archived: Boolean(row.archived),
    labels,
    permissions,
    effective_labels: object(row.effective_labels) as Task["effective_labels"],
    effective_permissions: object(row.effective_permissions) as Task["effective_permissions"],
  };
}

export function normalizeSnapshot(value: unknown): { tasks: Task[]; connections: Connection[]; adapters: AdapterDescriptor[]; settings: Record<string, unknown>; revision?: number } {
  const row = object(value) as Snapshot;
  const tasks = Array.isArray(row.tasks) ? row.tasks.map(normalizeTask).filter((task) => task.id) : [];
  const connections = Array.isArray(row.connections)
    ? row.connections.map((item) => {
        const connection = object(item);
        return { ...connection, id: text(connection.id), name: text(connection.name, "Connection"), adapter: text(connection.adapter) } as Connection;
      }).filter((item) => item.id)
    : [];
  const adapters = Array.isArray(row.adapters)
    ? row.adapters.map((item) => {
        const adapter = object(item);
        return { ...adapter, id: text(adapter.id), name: text(adapter.name, text(adapter.id, "Adapter")), capabilities: Array.isArray(adapter.capabilities) ? adapter.capabilities.map(String) : [] } as AdapterDescriptor;
      }).filter((item) => item.id)
    : [];
  return { tasks, connections, adapters, settings: object(row.settings), revision: typeof row.revision === "number" ? row.revision : undefined };
}

export function normalizeDetail(value: unknown, taskId: string): Detail {
  const row = object(value);
  const task = normalizeTask(row.task ?? row);
  const list = <T>(key: string): T[] => Array.isArray(row[key]) ? row[key] as T[] : [];
  const notes = list<Record<string, unknown>>("notes");
  const todos = list<unknown>("todos").map((item) => {
    const todo = object(item);
    return { ...todo, id: text(todo.id ?? todo.todo_id), text: text(todo.text), note: text(todo.note), done: Boolean(todo.done) } as Todo;
  }).filter((todo) => todo.id);
  return {
    ...row,
    task_id: text(row.task_id, taskId),
    task,
    children: list<unknown>("children").map(normalizeTask),
    sessions: list<Record<string, unknown>>("sessions"),
    runs: list<Record<string, unknown>>("runs"),
    events: list<TimelineEvent>("events"),
    notes,
    todos,
    artifacts: list<unknown>("artifacts").map((item) => {
      const artifact = object(item);
      return { ...artifact, id: text(artifact.id ?? artifact.artifact_id), name: text(artifact.name ?? artifact.path, "Artifact") } as Artifact;
    }).filter((artifact) => artifact.id),
    usage: object(row.usage) as Detail["usage"],
    duration: object(row.duration) as Detail["duration"],
  };
}

/** Restore only durable, product-level run context; provider metadata is ignored. */
export function latestTaskContext(detail: Pick<Detail, "runs" | "sessions">): TaskContext {
  const latestRun = detail.runs.slice().reverse()[0];
  const runOptions = object(latestRun?.requested_options);
  const sessionId = text(latestRun?.session_id);
  const session = detail.sessions.slice().reverse().find((candidate) => text(candidate.id) === sessionId)
    ?? detail.sessions.slice().reverse()[0];
  const effectiveOptions = object(session?.effective_options);
  const sessionOptions = object(session?.requested_options);
  const workspace = text(runOptions.workspace ?? effectiveOptions.workspace ?? sessionOptions.workspace);
  const connectionId = text(session?.connection_id);
  return {
    ...(workspace ? { workspace } : {}),
    ...(connectionId ? { connectionId } : {}),
  };
}

/** Read the neutral session capability for the connection that will receive a prompt. */
export function taskResumeState(detail: Pick<Detail, "runs" | "sessions">, connectionId: string): TaskResumeState {
  const session = detail.sessions.slice().reverse().find((candidate) => text(candidate.connection_id) === connectionId);
  if (!session) return { resumable: true };
  const metadata = object(session?.metadata);
  const capabilities = object(metadata.capabilities);
  if (capabilities.resumable === false) {
    return { resumable: false, reason: "This session is managed by its parent task and cannot accept direct prompts." };
  }
  return { resumable: true };
}

export function eventPayload(event: TimelineEvent): Record<string, unknown> {
  return typeof event.payload === "string" ? { text: event.payload } : object(event.payload);
}

export function eventText(event: TimelineEvent): string {
  const payload = eventPayload(event);
  const kind = text(event.kind ?? event.type ?? payload.kind).toLowerCase();
  if (kind === "approval.resolved") {
    const decision = text(payload.decision).toLowerCase();
    if (decision === "approve" || decision === "approved" || decision === "accept" || decision === "accepted") return "Approved.";
    if (decision === "deny" || decision === "denied" || decision === "decline" || decision === "declined") return "Denied.";
    return "Approval resolved.";
  }
  if (kind === "run.failed") {
    const error = payload.error;
    const reason = typeof error === "string" ? error.trim() : text(object(error).message).trim();
    return reason ? `Run failed: ${reason}` : "Run failed.";
  }
  const lifecycle = ({
    "run.started": "Run started.",
    "run.completed": "Run completed.",
    "run.interrupted": "Run interrupted.",
    "run.unknown": "Run ended with an unknown result.",
  } as Record<string, string>)[kind];
  if (lifecycle) return lifecycle;
  const value = text(
    event.body
      ?? payload.text
      ?? payload.delta
      ?? payload.body
      ?? payload.content
      ?? payload.output
      ?? payload.description
      ?? payload.message
      ?? payload.summary
      ?? payload.prompt
      ?? payload.plan
      ?? payload.model
      ?? payload.name
      ?? payload.status
      ?? payload.reason,
    "",
  );
  if (value) return value;
  return ({
    "subagent.started": "Child worker started.",
    "subagent.completed": "Child worker completed.",
  } as Record<string, string>)[kind] ?? "";
}

export function eventKind(event: TimelineEvent): "assistant" | "user" | "tool" | "approval" | "artifact" | "activity" {
  const payload = eventPayload(event);
  const value = text(event.kind ?? event.type ?? payload.kind).toLowerCase();
  if (value === "assistant" || value === "user" || value === "tool" || value === "approval" || value === "artifact") return value;
  return "activity";
}

/** Collapse normalized assistant deltas without crossing run boundaries. */
export function projectTimeline(events: TimelineEvent[]): TimelineEvent[] {
  const projected: TimelineEvent[] = [];
  const assistantByMessage = new Map<string, number>();
  for (const event of events) {
    if (eventKind(event) !== "assistant") {
      projected.push(event);
      continue;
    }
    const payload = eventPayload(event);
    const runId = text(payload.run_id ?? event.run_id);
    const messageId = text(payload.message_id ?? event.message_id);
    if (!runId || !messageId) {
      projected.push(event);
      continue;
    }
    const key = `${runId}\u0000${messageId}`;
    const existingIndex = assistantByMessage.get(key);
    const phase = text(payload.phase).toLowerCase();
    const finalText = text(payload.text ?? payload.content ?? event.body);
    const delta = text(payload.delta);
    if (existingIndex == null) {
      const body = finalText || delta;
      if (!body && (phase === "started" || phase === "delta")) continue;
      assistantByMessage.set(key, projected.length);
      projected.push({ ...event, body });
      continue;
    }
    const existing = projected[existingIndex];
    const current = eventText(existing);
    const next = phase === "completed" || phase === "complete" || phase === "final" || payload.final === true
      ? finalText || current
      : current + (delta || finalText);
    projected[existingIndex] = { ...event, ...existing, body: next, payload: { ...eventPayload(existing), ...payload, text: next, delta: "" } };
  }
  return projected;
}
