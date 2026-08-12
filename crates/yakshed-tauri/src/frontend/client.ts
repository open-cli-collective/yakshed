export type RunStatus =
  | "starting"
  | "running"
  | "completed"
  | "failed"
  | "interrupted"
  | "disconnected"
  | "outcome_unknown";

export interface DesktopError {
  code:
    | "invalid_request"
    | "conflict"
    | "not_found"
    | "unsupported"
    | "backend_unavailable"
    | "not_authenticated"
    | "persistence_error"
    | "outcome_unknown"
    | "internal_error";
  message: string;
  detail: string | null;
}

export interface StartupError {
  code: "persistence_error" | "internal_error";
  message: string;
}

export interface WorkItem {
  id: string;
  project_id: string;
  title: string;
  status: string;
  parent_id: string | null;
  revision: number;
  created_at_ms: number;
  updated_at_ms: number;
}

export interface Run {
  id: string;
  connection_id: string;
  work_item_id: string;
  status: RunStatus;
  created_at_ms: number;
  ended_at_ms: number | null;
}

export interface WorkItemSnapshot {
  revision: number;
  work_item: WorkItem;
  runs: Run[];
  next_run_after: string | null;
}

export interface WorkItemList {
  items: Array<{ work_item: WorkItem; revision: number }>;
  next_after: string | null;
}

export interface TimelineItem {
  id: string;
  connection_id: string;
  run_id: string;
  revision: number;
  kind: string;
  body: string;
  created_at_ms: number;
}

export interface TimelinePage {
  run_id: string;
  work_item_revision: number;
  items: TimelineItem[];
  next_after: number | null;
}

export interface Approval {
  id: string;
  connection_id: string;
  run_id: string;
  kind: string;
  summary: string;
  status: string;
  decision: string | null;
  requested_at_ms: number;
  response_started_at_ms: number | null;
  resolved_at_ms: number | null;
  voided_at_ms: number | null;
}

export interface ApprovalPage {
  work_item_revision: number;
  approvals: Approval[];
  next_after: string | null;
}

export interface PendingUserInput {
  id: string;
  run_id: string;
  prompt: string;
}

export interface UserInputPage {
  work_item_revision: number;
  inputs: PendingUserInput[];
  next_after: string | null;
}

export type CredentialBindingInput =
  | { slot: string; source: "delegated"; authority: string }
  | { slot: string; source: "secret"; backend: string; locator: string }
  | { slot: string; source: "disabled" };

export interface ConnectionInput {
  id: string;
  name: string;
  harness: string;
  model_provider: string;
  provider_state: string;
  credentials: CredentialBindingInput[];
}

export interface Connection extends ConnectionInput {}
export interface ConnectionEnvelope { config_revision: number; connection: Connection }
export type CredentialMigrationStatus =
  | { status: "ready" }
  | { status: "pending"; reason: "locked" | "denied" | "unavailable" | "collision" | "missing_source" | "source_in_use" | "target_in_use" | "failed" | "cleanup_required" };
export interface ConnectionList {
  config_revision: number;
  connections: Connection[];
  credential_migration: CredentialMigrationStatus;
}
export interface SecretWrite { overwritten: boolean }
export type AccountStatus =
  | { state: "not_authenticated" }
  | { state: "login_in_progress"; login_id: string; auth_url: string }
  | { state: "authenticated"; email: string | null; plan: string }
  | { state: "unknown" };
export interface Artifact { id: string; kind: string; byte_len: number; work_item_id: string }
export interface ArtifactList { revision: number; artifacts: Artifact[] }
export interface OpenArtifact { artifact: Artifact; bytes: number[]; media_type: string }

export type FrontendEventKind =
  | { type: "work_item_patched" }
  | { type: "timeline_batch_appended"; run_id: string; item_count: number }
  | { type: "approval_opened" | "approval_resolved"; run_id: string; approval_id: string }
  | { type: "user_input_opened"; run_id: string; request_id: string; prompt: string }
  | { type: "user_input_responded"; run_id: string; request_id: string }
  | { type: "run_status_changed"; run_id: string; status: RunStatus }
  | { type: "run_outcome_unknown"; run_id: string; operation: string };

export interface FrontendEvent {
  work_item_id: string;
  revision: number;
  kind: FrontendEventKind;
}

type Invoke = <T>(command: string, args?: Record<string, unknown>) => Promise<T>;
type Listen = <T>(event: string, handler: (event: { payload: T }) => void) => Promise<() => void>;

declare global {
  interface Window {
    __TAURI__?: { core: { invoke: Invoke }; event: { listen: Listen } };
    __YAKSHED_MOCK__?: { invoke: Invoke; listen: Listen };
  }
}

function transport(): { invoke: Invoke; listen: Listen } {
  if (window.__YAKSHED_MOCK__) return window.__YAKSHED_MOCK__;
  if (window.__TAURI__) return { invoke: window.__TAURI__.core.invoke, listen: window.__TAURI__.event.listen };
  throw { code: "internal_error", message: "YakShed desktop bridge is unavailable", detail: null } satisfies DesktopError;
}

const invoke = <T>(command: string, args?: Record<string, unknown>) => transport().invoke<T>(command, args);

export const client = {
  createProject: (id: string, name: string) => invoke<void>("create_project", { id, name }),
  createWorkItem: (projectId: string, title: string, parentId: string | null = null) =>
    invoke<WorkItemSnapshot>("create_work_item", { projectId, title, parentId }),
  listWorkItems: (projectId: string, after: string | null = null, limit = 50) =>
    invoke<WorkItemList>("list_work_items", { projectId, after, limit }),
  getWorkItemSnapshot: (id: string) => invoke<WorkItemSnapshot>("get_work_item_snapshot", { id }),
  getWorkItemSnapshotPage: (id: string, after: string | null, limit: number, expectedRevision: number | null) =>
    invoke<WorkItemSnapshot>("get_work_item_snapshot_page", { id, after, limit, expectedRevision }),
  getTimeline: (workItemId: string, runId: string, after: number | null = null, limit = 50) =>
    invoke<TimelinePage>("get_work_item_timeline_page", { workItemId, runId, after, limit }),
  getTimelineAtRevision: (workItemId: string, runId: string, after: number | null, limit: number, expectedRevision: number | null) =>
    invoke<TimelinePage>("get_work_item_timeline_page_at_revision", { workItemId, runId, after, limit, expectedRevision }),
  getApprovals: (workItemId: string, runId: string, after: string | null, limit: number, expectedRevision: number | null) =>
    invoke<ApprovalPage>("get_run_approval_page", { workItemId, runId, after, limit, expectedRevision }),
  getPendingInput: (workItemId: string, runId: string, after: string | null, limit: number, expectedRevision: number | null) =>
    invoke<UserInputPage>("get_pending_user_input_page", { workItemId, runId, after, limit, expectedRevision }),
  startRun: (workItemId: string, connectionId: string, input: string) => invoke<string>("start_run", { workItemId, connectionId, input }),
  steerRun: (runId: string, message: string) => invoke<void>("steer_run", { runId, message }),
  interruptRun: (runId: string) => invoke<void>("interrupt_run", { runId }),
  reconcileRun: (runId: string) => invoke<Run>("reconcile_run", { runId }),
  resolveApproval: (approvalId: string, decision: "approved" | "denied") => invoke<void>("resolve_approval", { approvalId, decision }),
  respondUserInput: (requestId: string, response: string) => invoke<void>("respond_user_input", { requestId, response }),
  putConnection: (expectedConfigRevision: number, connection: ConnectionInput) =>
    invoke<ConnectionEnvelope>("connection_put", { expectedConfigRevision, connection }),
  getConnection: (id: string) => invoke<ConnectionEnvelope>("connection_get", { id }),
  listConnections: () => invoke<ConnectionList>("list_connections"),
  setCredential: (connectionId: string, slot: string, value: string, overwrite: boolean) =>
    invoke<SecretWrite>("set_connection_credential", { connectionId, slot, value, overwrite }),
  accountStatus: (connectionId: string) => invoke<AccountStatus>("account_status", { connectionId }),
  accountLoginStart: (connectionId: string) => invoke<AccountStatus>("account_login_start", { connectionId }),
  accountLogout: (connectionId: string) => invoke<void>("account_logout", { connectionId }),
  listArtifacts: (workItemId: string) => invoke<ArtifactList>("list_artifacts", { workItemId }),
  openArtifact: (workItemId: string, artifactId: string, maxBytes: number) =>
    invoke<OpenArtifact>("open_artifact", { workItemId, artifactId, maxBytes }),
  clearCache: () => invoke<void>("clear_cache"),
  onEvent: (handler: (event: FrontendEvent) => void) =>
    transport().listen<FrontendEvent>("yakshed:frontend-event", ({ payload }) => handler(payload)),
};
