//! Frontend-safe command facade over application use-cases and infrastructure ports.

use std::collections::VecDeque;
use std::sync::Arc;

use tokio::sync::broadcast;
use yakshed_application::{
    AppEvent, AppEventKind, AppStore, ArtifactPort, ArtifactPortError, CachePort, CachePortError,
    ConfigPort, ConfigPortError, ConfigRevision, CreateProject, CreateWorkItem, ListWorkItems,
    OpenArtifactCommand, OpenArtifactPayload, PublicConnection, PublicCredentialBinding,
    PublicCredentialSource, PutConnectionCommand, RunOrchestrationError, RunSupervisor, SecretPort,
    SecretPortError, SetConnectionCredentialCommand, StoreError,
};
use yakshed_domain::{
    ApprovalRequestId, ApprovalSnapshot, ApprovalStatus, ArtifactId, ArtifactKind, Connection,
    ConnectionId, CredentialBinding, CredentialBindingRecord, CredentialSlot, ProjectId, RunId,
    RunSnapshot, RunStatus, SecretBackendId, SecretLocator, SecretReference, TimelineItemId,
    TimelineItemSnapshot, TimelineRevision, UtcTimestamp, WorkItemId, WorkItemSnapshot,
};

pub const APP_EVENT_CAPACITY: usize = 32;
const MAX_OPEN_ARTIFACT_BYTES: u64 = 8 * 1024 * 1024;
const SNAPSHOT_RETRIES: usize = 256;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DesktopErrorCode {
    InvalidRequest,
    Conflict,
    NotFound,
    Unsupported,
    BackendUnavailable,
    PersistenceError,
    OutcomeUnknown,
    InternalError,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DesktopError {
    pub code: DesktopErrorCode,
    pub message: &'static str,
    pub detail: Option<String>,
}

pub type Result<T> = std::result::Result<T, DesktopError>;

impl DesktopError {
    const fn new(code: DesktopErrorCode, message: &'static str) -> Self {
        Self {
            code,
            message,
            detail: None,
        }
    }

    const fn invalid_request(message: &'static str) -> Self {
        Self::new(DesktopErrorCode::InvalidRequest, message)
    }

    const fn not_found(message: &'static str) -> Self {
        Self::new(DesktopErrorCode::NotFound, message)
    }

    const fn conflict(message: &'static str) -> Self {
        Self::new(DesktopErrorCode::Conflict, message)
    }

    const fn unsupported(message: &'static str) -> Self {
        Self::new(DesktopErrorCode::Unsupported, message)
    }

    fn outcome_unknown(operation: &'static str) -> Self {
        Self {
            code: DesktopErrorCode::OutcomeUnknown,
            message: "operation outcome is uncertain",
            detail: Some(operation.to_owned()),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum FrontendRunStatus {
    Starting,
    Running,
    Completed,
    Failed,
    Interrupted,
    Disconnected,
    OutcomeUnknown,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum FrontendEventKind {
    WorkItemPatched,
    TimelineBatchAppended {
        run_id: RunId,
        item_count: usize,
    },
    ApprovalOpened {
        run_id: RunId,
        approval_id: ApprovalRequestId,
    },
    ApprovalResolved {
        run_id: RunId,
        approval_id: ApprovalRequestId,
    },
    UserInputOpened {
        run_id: RunId,
        request_id: yakshed_application::UserInputRequestId,
        prompt: String,
    },
    UserInputResponded {
        run_id: RunId,
        request_id: yakshed_application::UserInputRequestId,
    },
    RunStatusChanged {
        run_id: RunId,
        status: FrontendRunStatus,
    },
    RunOutcomeUnknown {
        run_id: RunId,
        operation: String,
    },
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FrontendEvent {
    /// Work-item id used by frontend for per-item state reconciliation.
    pub work_item_id: WorkItemId,
    /// Monotonic per-work-item revision published by the application.
    pub revision: u64,
    pub kind: FrontendEventKind,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct WorkItemSnapshotEnvelope {
    /// Durable revision of this work item used for event-gap recovery.
    pub revision: u64,
    pub work_item: WorkItemSnapshot,
    pub runs: Vec<FrontendRunSnapshot>,
    pub pending_approvals: Vec<FrontendApprovalSnapshot>,
    pub pending_user_inputs: Vec<WorkItemPendingUserInput>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FrontendRunSnapshot {
    pub id: RunId,
    pub connection_id: ConnectionId,
    pub work_item_id: WorkItemId,
    pub status: RunStatus,
    pub created_at: UtcTimestamp,
    pub ended_at: Option<UtcTimestamp>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FrontendApprovalSnapshot {
    pub id: ApprovalRequestId,
    pub connection_id: ConnectionId,
    pub run_id: RunId,
    pub kind: String,
    pub summary: String,
    pub status: ApprovalStatus,
    pub requested_at: UtcTimestamp,
    pub response_started_at: Option<UtcTimestamp>,
    pub resolved_at: Option<UtcTimestamp>,
    pub voided_at: Option<UtcTimestamp>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct WorkItemPendingUserInput {
    pub run_id: RunId,
    pub request_id: TimelineItemId,
    pub prompt: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct WorkItemListItem {
    pub work_item: WorkItemSnapshot,
    pub revision: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct WorkItemListEnvelope {
    pub items: Vec<WorkItemListItem>,
    pub next_after: Option<WorkItemId>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct WorkItemTimelineEnvelope {
    pub run_id: RunId,
    pub work_item_revision: u64,
    pub items: Vec<FrontendTimelineItemSnapshot>,
    pub next_after: Option<TimelineRevision>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FrontendTimelineItemSnapshot {
    pub id: TimelineItemId,
    pub connection_id: ConnectionId,
    pub run_id: RunId,
    pub revision: TimelineRevision,
    pub kind: String,
    pub body: String,
    pub created_at: UtcTimestamp,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ConnectionEnvelope {
    pub config_revision: ConfigRevision,
    pub connection: PublicConnection,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ConnectionListEnvelope {
    pub config_revision: ConfigRevision,
    pub connections: Vec<PublicConnection>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SecretWriteEnvelope {
    pub overwritten: bool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ArtifactListItem {
    pub id: ArtifactId,
    pub kind: ArtifactKind,
    pub byte_len: u64,
    pub work_item_id: WorkItemId,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ArtifactListEnvelope {
    /// Snapshot revision for the owning work item to support event-gap recovery.
    pub revision: u64,
    pub artifacts: Vec<ArtifactListItem>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OpenArtifactEnvelope {
    pub artifact: ArtifactListItem,
    pub bytes: Vec<u8>,
    pub media_type: String,
}

#[derive(Clone)]
pub struct ApiPorts {
    pub store: Arc<dyn AppStore>,
    pub run_supervisor: Arc<RunSupervisor>,
    pub config: Arc<dyn ConfigPort>,
    pub secrets: Arc<dyn SecretPort>,
    pub cache: Arc<dyn CachePort>,
    pub artifacts: Arc<dyn ArtifactPort>,
}

#[derive(Clone)]
pub struct DesktopApi {
    store: Arc<dyn AppStore>,
    run_supervisor: Arc<RunSupervisor>,
    config: Arc<dyn ConfigPort>,
    secrets: Arc<dyn SecretPort>,
    cache: Arc<dyn CachePort>,
    artifacts: Arc<dyn ArtifactPort>,
    events: broadcast::Sender<FrontendEvent>,
}

impl DesktopApi {
    /// Drops oldest events on overflow; consumers recovering missed revisions must call snapshot APIs.
    pub fn new(ports: ApiPorts) -> Self {
        let (events, _) = broadcast::channel(APP_EVENT_CAPACITY);
        let mut source = ports.run_supervisor.subscribe();
        let relay = events.clone();
        tokio::spawn(async move {
            loop {
                match source.recv().await {
                    Ok(event) => {
                        let _ = relay.send(map_frontend_event(event));
                    }
                    Err(broadcast::error::RecvError::Lagged(_)) => {
                        continue;
                    }
                    Err(_) => break,
                }
            }
        });

        Self {
            store: ports.store,
            run_supervisor: ports.run_supervisor,
            config: ports.config,
            secrets: ports.secrets,
            cache: ports.cache,
            artifacts: ports.artifacts,
            events,
        }
    }

    pub fn subscribe_events(&self) -> broadcast::Receiver<FrontendEvent> {
        self.events.subscribe()
    }

    pub async fn create_project(&self, id: ProjectId, name: String) -> Result<()> {
        self.store
            .create_project(CreateProject { id, name })
            .await
            .map_err(map_store_error)
            .map(|_| ())
    }

    pub async fn create_work_item(
        &self,
        project_id: ProjectId,
        title: impl Into<String>,
        parent_id: Option<WorkItemId>,
    ) -> Result<WorkItemSnapshotEnvelope> {
        let work_item = self
            .store
            .create_work_item(CreateWorkItem {
                id: yakshed_domain::WorkItemId::new_v7(),
                project_id,
                title: title.into(),
                parent_id,
            })
            .await
            .map_err(map_store_error)?;
        self.get_work_item_snapshot(work_item.id).await
    }

    pub async fn get_work_item_snapshot(&self, id: WorkItemId) -> Result<WorkItemSnapshotEnvelope> {
        for _ in 0..SNAPSHOT_RETRIES {
            let work_item = self
                .store
                .get_work_item(id)
                .await
                .map_err(map_store_error)?;
            let revision = work_item.revision.get();
            let mut runs = Vec::new();
            let mut after = None;
            loop {
                let page = self
                    .store
                    .list_runs_for_work_item(id, after, 200)
                    .await
                    .map_err(map_store_error)?;
                runs.extend(page.items);
                after = page.next_after;
                if after.is_none() {
                    break;
                }
            }
            let mut pending_approvals = Vec::new();
            let mut pending_user_inputs = Vec::new();
            for run in &runs {
                let mut approval_after = None;
                loop {
                    let page = self
                        .store
                        .list_approvals_for_run(run.id, approval_after, 200)
                        .await
                        .map_err(map_store_error)?;
                    pending_approvals.extend(page.items.into_iter().filter(|approval| {
                        matches!(
                            approval.status,
                            ApprovalStatus::Pending | ApprovalStatus::Responding { .. }
                        )
                    }));
                    approval_after = page.next_after;
                    if approval_after.is_none() {
                        break;
                    }
                }

                let mut timeline_after = None;
                let mut requested: VecDeque<(TimelineItemId, String)> = VecDeque::new();
                loop {
                    let page = self
                        .store
                        .list_timeline_page(yakshed_application::ListTimeline {
                            run_id: run.id,
                            after: timeline_after,
                            limit: 200,
                        })
                        .await
                        .map_err(map_store_error)?;
                    for item in page.items {
                        match item.kind.as_str() {
                            "user_input_requested" => requested.push_back((item.id, item.body)),
                            "user_input_responded" => {
                                if let Ok(request_id) = item.body.parse()
                                    && let Some(index) = requested
                                        .iter()
                                        .position(|(pending_id, _)| *pending_id == request_id)
                                {
                                    requested.remove(index);
                                }
                            }
                            _ => {}
                        }
                    }
                    timeline_after = page.next_after;
                    if timeline_after.is_none() {
                        break;
                    }
                }
                pending_user_inputs.extend(requested.into_iter().map(|(request_id, prompt)| {
                    WorkItemPendingUserInput {
                        run_id: run.id,
                        request_id,
                        prompt,
                    }
                }));
            }
            let current = self
                .store
                .get_work_item(id)
                .await
                .map_err(map_store_error)?;
            if current.revision.get() == revision {
                return Ok(WorkItemSnapshotEnvelope {
                    revision,
                    work_item,
                    runs: runs.into_iter().map(frontend_run).collect(),
                    pending_approvals: pending_approvals
                        .into_iter()
                        .map(frontend_approval)
                        .collect(),
                    pending_user_inputs,
                });
            }
        }
        Err(DesktopError::conflict("work item changed during snapshot"))
    }

    pub async fn list_work_items(
        &self,
        project_id: ProjectId,
        after: Option<WorkItemId>,
        limit: u32,
    ) -> Result<WorkItemListEnvelope> {
        let page = self
            .store
            .list_work_items(ListWorkItems {
                project_id,
                after,
                limit,
                include_archived: false,
            })
            .await
            .map_err(map_store_error)?;
        Ok(WorkItemListEnvelope {
            items: page
                .items
                .into_iter()
                .map(|work_item| {
                    let revision = work_item.revision.get();
                    WorkItemListItem {
                        work_item,
                        revision,
                    }
                })
                .collect(),
            next_after: page.next_after,
        })
    }

    pub async fn connection_put(
        &self,
        expected_config_revision: ConfigRevision,
        connection: PublicConnection,
        ensure_memory_secret_backend: bool,
    ) -> Result<ConnectionEnvelope> {
        let connection_id = connection.id;
        let needs_memory_secret_backend = ensure_memory_secret_backend
            || connection
                .credentials
                .iter()
                .any(|binding| matches!(binding.source, PublicCredentialSource::Secret { .. }));
        let connection = to_domain_connection(connection)?;
        let snapshot = self
            .config
            .put_connection(PutConnectionCommand {
                expected_config_revision,
                connection,
                ensure_memory_secret_backend: needs_memory_secret_backend,
            })
            .await
            .map_err(map_config_error)?;
        let connection = to_public_connection(
            snapshot
                .config
                .connections
                .into_iter()
                .find(|connection| connection.id == connection_id)
                .ok_or_else(|| DesktopError::invalid_request("stored connection missing"))?,
        );
        Ok(ConnectionEnvelope {
            config_revision: snapshot.revision,
            connection,
        })
    }

    pub async fn connection_get(&self, id: ConnectionId) -> Result<ConnectionEnvelope> {
        let snapshot = self
            .config
            .get_connection(id)
            .await
            .map_err(map_config_error)?;
        Ok(ConnectionEnvelope {
            config_revision: snapshot.config_revision,
            connection: snapshot.connection,
        })
    }

    pub async fn list_connections(&self) -> Result<ConnectionListEnvelope> {
        let list = self
            .config
            .list_connections()
            .await
            .map_err(map_config_error)?;
        Ok(ConnectionListEnvelope {
            config_revision: list.config_revision,
            connections: list.connections,
        })
    }

    pub async fn start_run(
        &self,
        work_item_id: WorkItemId,
        connection_id: ConnectionId,
        input: impl Into<String>,
    ) -> Result<RunId> {
        Ok(self
            .run_supervisor
            .start_run(work_item_id, connection_id, input.into())
            .await
            .map_err(map_run_error)?
            .id)
    }

    pub async fn steer_run(&self, run_id: RunId, message: impl Into<String>) -> Result<()> {
        self.run_supervisor
            .steer(run_id, message.into())
            .await
            .map_err(map_run_error)
    }

    pub async fn interrupt_run(&self, run_id: RunId) -> Result<()> {
        self.run_supervisor
            .interrupt(run_id)
            .await
            .map_err(map_run_error)
    }

    pub async fn reconcile_run(&self, run_id: RunId) -> Result<FrontendRunSnapshot> {
        self.run_supervisor
            .reconcile_run(run_id)
            .await
            .map(frontend_run)
            .map_err(map_run_error)
    }

    pub async fn resolve_approval(
        &self,
        approval_id: ApprovalRequestId,
        decision: yakshed_domain::ApprovalDecision,
    ) -> Result<()> {
        self.run_supervisor
            .resolve_approval(approval_id, decision)
            .await
            .map_err(map_run_error)
    }

    pub async fn respond_user_input(
        &self,
        request_id: yakshed_application::UserInputRequestId,
        response: impl Into<String>,
    ) -> Result<()> {
        self.run_supervisor
            .respond_user_input(request_id, response.into())
            .await
            .map_err(map_run_error)
    }

    pub async fn set_connection_credential(
        &self,
        connection_id: ConnectionId,
        slot: CredentialSlot,
        value: impl Into<String>,
        overwrite: bool,
    ) -> Result<SecretWriteEnvelope> {
        let outcome = self
            .secrets
            .set_connection_credential(SetConnectionCredentialCommand {
                connection_id,
                slot,
                value: yakshed_application::SecretValue::new(value.into()),
                overwrite,
            })
            .await
            .map_err(map_secret_error)?;
        Ok(SecretWriteEnvelope {
            overwritten: outcome.overwritten,
        })
    }

    pub async fn clear_cache(&self) -> Result<()> {
        self.cache.clear().await.map_err(map_cache_error)
    }

    pub async fn list_artifacts(&self, work_item_id: WorkItemId) -> Result<ArtifactListEnvelope> {
        let artifacts = self
            .artifacts
            .list_artifacts_for_work_item(work_item_id)
            .await
            .map_err(map_artifact_error)?;
        let revision = self
            .store
            .get_work_item(work_item_id)
            .await
            .map_err(map_store_error)?
            .revision
            .get();
        Ok(ArtifactListEnvelope {
            revision,
            artifacts: artifacts
                .into_iter()
                .map(|artifact| ArtifactListItem {
                    id: artifact.id,
                    kind: artifact.kind,
                    byte_len: artifact.byte_len,
                    work_item_id: artifact.work_item_id,
                })
                .collect(),
        })
    }

    pub async fn open_artifact(
        &self,
        work_item_id: WorkItemId,
        artifact_id: ArtifactId,
        max_bytes: u64,
    ) -> Result<OpenArtifactEnvelope> {
        if max_bytes == 0 || max_bytes > MAX_OPEN_ARTIFACT_BYTES {
            return Err(DesktopError::invalid_request(
                "artifact byte limit must be 1..8MiB",
            ));
        }
        let OpenArtifactPayload { artifact, bytes } = self
            .artifacts
            .open_artifact(OpenArtifactCommand {
                artifact_id,
                max_bytes,
            })
            .await
            .map_err(map_artifact_error)?;
        if artifact.work_item_id != work_item_id {
            return Err(DesktopError::not_found(
                "artifact not on requested work item",
            ));
        }
        Ok(OpenArtifactEnvelope {
            media_type: artifact.media_type,
            bytes,
            artifact: ArtifactListItem {
                id: artifact.id,
                kind: artifact.kind,
                byte_len: artifact.byte_len,
                work_item_id: artifact.work_item_id,
            },
        })
    }

    pub async fn get_work_item_timeline_page(
        &self,
        work_item_id: WorkItemId,
        run_id: RunId,
        after: Option<TimelineRevision>,
        limit: u32,
    ) -> Result<WorkItemTimelineEnvelope> {
        let run = self.store.get_run(run_id).await.map_err(map_store_error)?;
        if run.work_item_id != work_item_id {
            return Err(DesktopError::invalid_request(
                "run does not belong to work item",
            ));
        }
        for _ in 0..SNAPSHOT_RETRIES {
            let work_item_revision = self
                .store
                .get_work_item(work_item_id)
                .await
                .map_err(map_store_error)?
                .revision
                .get();
            let page = self
                .store
                .list_timeline_page(yakshed_application::ListTimeline {
                    run_id,
                    after,
                    limit,
                })
                .await
                .map_err(map_store_error)?;
            let current_revision = self
                .store
                .get_work_item(work_item_id)
                .await
                .map_err(map_store_error)?
                .revision
                .get();
            if current_revision == work_item_revision {
                return Ok(WorkItemTimelineEnvelope {
                    run_id,
                    work_item_revision,
                    items: page.items.into_iter().map(frontend_timeline_item).collect(),
                    next_after: page.next_after,
                });
            }
        }
        Err(DesktopError::conflict("work item changed during snapshot"))
    }
}

fn frontend_run(run: RunSnapshot) -> FrontendRunSnapshot {
    FrontendRunSnapshot {
        id: run.id,
        connection_id: run.connection_id,
        work_item_id: run.work_item_id,
        status: run.status,
        created_at: run.created_at,
        ended_at: run.ended_at,
    }
}

fn frontend_approval(approval: ApprovalSnapshot) -> FrontendApprovalSnapshot {
    FrontendApprovalSnapshot {
        id: approval.id,
        connection_id: approval.connection_id,
        run_id: approval.run_id,
        kind: approval.kind,
        summary: approval.summary,
        status: approval.status,
        requested_at: approval.requested_at,
        response_started_at: approval.response_started_at,
        resolved_at: approval.resolved_at,
        voided_at: approval.voided_at,
    }
}

fn frontend_timeline_item(item: TimelineItemSnapshot) -> FrontendTimelineItemSnapshot {
    FrontendTimelineItemSnapshot {
        id: item.id,
        connection_id: item.connection_id,
        run_id: item.run_id,
        revision: item.revision,
        kind: item.kind,
        body: item.body,
        created_at: item.created_at,
    }
}

fn map_frontend_event(event: AppEvent) -> FrontendEvent {
    FrontendEvent {
        work_item_id: event.work_item_id,
        revision: event.revision,
        kind: match event.kind {
            AppEventKind::WorkItemPatched => FrontendEventKind::WorkItemPatched,
            AppEventKind::TimelineBatchAppended { run_id, item_count } => {
                FrontendEventKind::TimelineBatchAppended { run_id, item_count }
            }
            AppEventKind::ApprovalOpened {
                run_id,
                approval_id,
            } => FrontendEventKind::ApprovalOpened {
                run_id,
                approval_id,
            },
            AppEventKind::ApprovalResolved {
                run_id,
                approval_id,
            } => FrontendEventKind::ApprovalResolved {
                run_id,
                approval_id,
            },
            AppEventKind::UserInputOpened {
                run_id,
                request_id,
                prompt,
            } => FrontendEventKind::UserInputOpened {
                run_id,
                request_id,
                prompt,
            },
            AppEventKind::UserInputResponded { run_id, request_id } => {
                FrontendEventKind::UserInputResponded { run_id, request_id }
            }
            AppEventKind::RunStatusChanged { run_id, status } => {
                FrontendEventKind::RunStatusChanged {
                    run_id,
                    status: map_run_status(status),
                }
            }
            AppEventKind::RunOutcomeUnknown { run_id, operation } => {
                FrontendEventKind::RunOutcomeUnknown {
                    run_id,
                    operation: operation.to_string(),
                }
            }
        },
    }
}

fn map_run_status(status: RunStatus) -> FrontendRunStatus {
    match status {
        RunStatus::Starting => FrontendRunStatus::Starting,
        RunStatus::Running => FrontendRunStatus::Running,
        RunStatus::Completed => FrontendRunStatus::Completed,
        RunStatus::Failed => FrontendRunStatus::Failed,
        RunStatus::Interrupted => FrontendRunStatus::Interrupted,
        RunStatus::Disconnected => FrontendRunStatus::Disconnected,
        RunStatus::OutcomeUnknown => FrontendRunStatus::OutcomeUnknown,
    }
}

fn map_store_error(error: StoreError) -> DesktopError {
    match error {
        StoreError::NotFound { .. } => DesktopError::not_found("record not found"),
        StoreError::Conflict(_) => DesktopError::conflict("state conflict"),
        StoreError::UnsupportedNewerSchema { .. } => {
            DesktopError::unsupported("unsupported persistence schema")
        }
        StoreError::Closed => DesktopError::unsupported("persistence backend closed"),
        StoreError::Open(_) | StoreError::Migration(_) | StoreError::Integrity(_) => {
            DesktopError::new(DesktopErrorCode::PersistenceError, "persistence failure")
        }
        StoreError::Backend(_) => {
            DesktopError::new(DesktopErrorCode::InternalError, "persistence failed")
        }
    }
}

fn map_run_error(error: RunOrchestrationError) -> DesktopError {
    match error {
        RunOrchestrationError::Harness(error) => match error {
            yakshed_application::HarnessPortError::OutcomeUnknown { operation } => {
                DesktopError::outcome_unknown(operation)
            }
            yakshed_application::HarnessPortError::NotFound(_) => {
                DesktopError::not_found("run artifact not found")
            }
            yakshed_application::HarnessPortError::Conflict(_) => {
                DesktopError::conflict("run action conflicts with current state")
            }
            yakshed_application::HarnessPortError::Unsupported(_) => {
                DesktopError::unsupported("operation unsupported")
            }
            yakshed_application::HarnessPortError::Disconnected => {
                DesktopError::unsupported("harness disconnected")
            }
            yakshed_application::HarnessPortError::Overloaded => {
                DesktopError::unsupported("harness overloaded")
            }
            yakshed_application::HarnessPortError::InvalidInput(_)
            | yakshed_application::HarnessPortError::Protocol(_)
            | yakshed_application::HarnessPortError::Transport(_)
            | yakshed_application::HarnessPortError::Runtime(_) => {
                DesktopError::invalid_request("request is invalid")
            }
            yakshed_application::HarnessPortError::Closed => {
                DesktopError::unsupported("harness closed")
            }
        },
        RunOrchestrationError::Store(error) => map_store_error(error),
        RunOrchestrationError::RunNotActive(_) => DesktopError::not_found("run is not active"),
        RunOrchestrationError::RunNeedsReconciliation(_) => {
            DesktopError::outcome_unknown("reconcile_run")
        }
        RunOrchestrationError::ApprovalNotPending(_) => {
            DesktopError::invalid_request("approval is not pending")
        }
        RunOrchestrationError::ApprovalDecisionConflict(_) => {
            DesktopError::conflict("approval decision conflicts with in-flight response")
        }
        RunOrchestrationError::UserInputNotPending(_) => {
            DesktopError::invalid_request("user input is not pending")
        }
        RunOrchestrationError::InvalidProviderId(_) => {
            DesktopError::invalid_request("provider identifier is invalid")
        }
    }
}

fn map_config_error(error: ConfigPortError) -> DesktopError {
    match error {
        ConfigPortError::Conflict { .. } => {
            DesktopError::conflict("configuration revision conflict")
        }
        ConfigPortError::Validation => DesktopError::invalid_request("configuration is invalid"),
        ConfigPortError::NotFound => DesktopError::not_found("configuration entry not found"),
        ConfigPortError::Unsupported | ConfigPortError::Unavailable => {
            DesktopError::unsupported("configuration unavailable")
        }
    }
}

fn map_secret_error(error: SecretPortError) -> DesktopError {
    match error {
        SecretPortError::ConnectionNotFound | SecretPortError::BindingNotFound => {
            DesktopError::not_found("connection or binding not found")
        }
        SecretPortError::BackendUnavailable => DesktopError::new(
            DesktopErrorCode::BackendUnavailable,
            "secret backend unavailable",
        ),
        SecretPortError::Locked => DesktopError::invalid_request("secret backend is locked"),
        SecretPortError::Denied => DesktopError::unsupported("secret backend denied access"),
        SecretPortError::AuthenticationRequired => {
            DesktopError::unsupported("secret backend authentication required")
        }
        SecretPortError::AlreadyExists => DesktopError::conflict("credential already exists"),
        SecretPortError::UncertainWrite => DesktopError::outcome_unknown("secret write uncertain"),
        SecretPortError::NotSecretBacked => {
            DesktopError::unsupported("credential binding is not secret-backed")
        }
        SecretPortError::Failed => {
            DesktopError::new(DesktopErrorCode::PersistenceError, "secret write failed")
        }
    }
}

fn map_cache_error(error: CachePortError) -> DesktopError {
    match error {
        CachePortError::Failed => {
            DesktopError::new(DesktopErrorCode::PersistenceError, "cache write failed")
        }
    }
}

fn map_artifact_error(error: ArtifactPortError) -> DesktopError {
    match error {
        ArtifactPortError::NotFound => DesktopError::not_found("artifact not found"),
        ArtifactPortError::TooLarge => DesktopError::unsupported("artifact exceeds requested size"),
        ArtifactPortError::Failed => DesktopError::new(
            DesktopErrorCode::PersistenceError,
            "artifact operation failed",
        ),
    }
}

fn to_domain_connection(
    connection: PublicConnection,
) -> std::result::Result<Connection, DesktopError> {
    let mut credentials = Vec::with_capacity(connection.credentials.len());
    for binding in connection.credentials {
        let source = match binding.source {
            PublicCredentialSource::Delegated { authority } => {
                CredentialBinding::Delegated { authority }
            }
            PublicCredentialSource::Secret { backend, locator } => CredentialBinding::Secret {
                reference: SecretReference {
                    backend_id: SecretBackendId::new(backend.as_str()).map_err(|_| {
                        DesktopError::invalid_request("credential binding source is invalid")
                    })?,
                    locator: SecretLocator::new(locator.as_str()).map_err(|_| {
                        DesktopError::invalid_request("credential binding source is invalid")
                    })?,
                },
            },
            PublicCredentialSource::Disabled => CredentialBinding::Disabled,
        };
        credentials.push(CredentialBindingRecord {
            slot: binding.slot,
            binding: source,
        });
    }
    let connection = Connection {
        id: connection.id,
        name: connection.name,
        harness: connection.harness,
        model_provider: connection.model_provider,
        provider_state: connection.provider_state,
        credentials,
    };
    connection
        .validate()
        .map_err(|_| DesktopError::invalid_request("connection is invalid"))?;
    Ok(connection)
}

fn to_public_connection(connection: Connection) -> PublicConnection {
    PublicConnection {
        id: connection.id,
        name: connection.name,
        harness: connection.harness,
        model_provider: connection.model_provider,
        provider_state: connection.provider_state,
        credentials: connection
            .credentials
            .into_iter()
            .map(|binding| {
                let source = match binding.binding {
                    CredentialBinding::Delegated { authority } => {
                        PublicCredentialSource::Delegated { authority }
                    }
                    CredentialBinding::Secret { reference } => PublicCredentialSource::Secret {
                        backend: reference.backend_id,
                        locator: reference.locator,
                    },
                    CredentialBinding::Disabled => PublicCredentialSource::Disabled,
                };
                PublicCredentialBinding {
                    slot: binding.slot,
                    source,
                }
            })
            .collect(),
    }
}
