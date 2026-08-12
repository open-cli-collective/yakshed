//! Frontend-safe command facade over application use-cases and infrastructure ports.

use std::sync::Arc;

use serde::{Deserialize, Serialize};
use tokio::sync::broadcast;
pub use yakshed_application::RunOrchestrationError;
use yakshed_application::{
    AppEvent, AppEventKind, AppStore, ArtifactPort, ArtifactPortError, CachePort, CachePortError,
    Clock, ConfigPort, ConfigPortError, CreateProject, CreateWorkItem,
    CredentialMigrationPendingReason, CredentialMigrationStatus, IdGenerator, ListWorkItems,
    OpenArtifactCommand, OpenArtifactPayload, PublicCredentialBinding, PublicCredentialSource,
    PutConnectionCommand, RunHarness, RunSupervisor, SecretPort, SecretPortError,
    SetConnectionCredentialCommand, StoreError,
};
pub use yakshed_application::{ConfigRevision, PublicConnection, UserInputRequestId};
pub use yakshed_domain::{
    ApprovalRequestId, ArtifactId, ConnectionId, CredentialSlot, ProjectId, RunId, TimelineItemId,
    TimelineRevision, WorkItemId,
};
use yakshed_domain::{
    ApprovalSnapshot, ApprovalStatus, ArtifactKind, Connection, CredentialBinding,
    CredentialBindingRecord, RunSnapshot, RunStatus, SecretBackendId, SecretLocator,
    SecretReference, TimelineItemSnapshot, UtcTimestamp, WorkItemSnapshot,
};

mod ipc;
pub use ipc::{ApprovalDecisionInput, ConnectionInput};

pub const APP_EVENT_CAPACITY: usize = 32;
const MAX_OPEN_ARTIFACT_BYTES: u64 = 8 * 1024 * 1024;
/// Desktop message ceiling; protects the application and provider boundary from WebView payloads.
pub const MAX_RUN_INPUT_BYTES: usize = 256 * 1024;
/// Follow-up steering is intentionally smaller than an initial context-bearing prompt.
pub const MAX_STEER_INPUT_BYTES: usize = 64 * 1024;
/// Server-request responses are bounded independently from provider-native limits.
pub const MAX_USER_INPUT_RESPONSE_BYTES: usize = 64 * 1024;
const SNAPSHOT_RETRIES: usize = 256;
const RECOVERY_PAGE_SIZE: u32 = 50;

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
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

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum StartupErrorCode {
    PersistenceError,
    InternalError,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct StartupError {
    pub code: StartupErrorCode,
    pub message: &'static str,
}

impl StartupError {
    const fn new(code: StartupErrorCode, message: &'static str) -> Self {
        Self { code, message }
    }

    pub fn persistence() -> Self {
        Self::new(
            StartupErrorCode::PersistenceError,
            "persistence startup failed",
        )
    }

    pub fn internal() -> Self {
        Self::new(StartupErrorCode::InternalError, "desktop startup failed")
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
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

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum FrontendRunStatus {
    Starting,
    Running,
    Completed,
    Failed,
    Interrupted,
    Disconnected,
    OutcomeUnknown,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum FrontendEventKind {
    WorkItemPatched,
    TimelineBatchAppended {
        run_id: String,
        item_count: usize,
    },
    ApprovalOpened {
        run_id: String,
        approval_id: String,
    },
    ApprovalResolved {
        run_id: String,
        approval_id: String,
    },
    UserInputOpened {
        run_id: String,
        request_id: String,
        prompt: String,
    },
    UserInputResponded {
        run_id: String,
        request_id: String,
    },
    RunStatusChanged {
        run_id: String,
        status: FrontendRunStatus,
    },
    RunOutcomeUnknown {
        run_id: String,
        operation: String,
    },
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct FrontendEvent {
    /// Work-item id used by frontend for per-item state reconciliation.
    pub work_item_id: String,
    /// Monotonic per-work-item revision published by the application.
    pub revision: u64,
    pub kind: FrontendEventKind,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct WorkItemSnapshotEnvelope {
    /// Durable revision of this work item used for event-gap recovery.
    pub revision: u64,
    pub work_item: FrontendWorkItemSnapshot,
    pub runs: Vec<FrontendRunSnapshot>,
    pub next_run_after: Option<String>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct FrontendWorkItemSnapshot {
    pub id: String,
    pub project_id: String,
    pub title: String,
    pub status: String,
    pub parent_id: Option<String>,
    pub revision: u64,
    pub created_at_ms: i64,
    pub updated_at_ms: i64,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct FrontendRunSnapshot {
    pub id: String,
    pub connection_id: String,
    pub work_item_id: String,
    pub status: FrontendRunStatus,
    pub created_at_ms: i64,
    pub ended_at_ms: Option<i64>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct FrontendApprovalSnapshot {
    pub id: String,
    pub connection_id: String,
    pub run_id: String,
    pub kind: String,
    pub summary: String,
    pub status: String,
    pub decision: Option<String>,
    pub requested_at_ms: i64,
    pub response_started_at_ms: Option<i64>,
    pub resolved_at_ms: Option<i64>,
    pub voided_at_ms: Option<i64>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct RunApprovalPageEnvelope {
    pub work_item_revision: u64,
    pub approvals: Vec<FrontendApprovalSnapshot>,
    pub next_after: Option<String>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct FrontendPendingUserInput {
    pub id: String,
    pub run_id: String,
    pub prompt: String,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct PendingUserInputPageEnvelope {
    pub work_item_revision: u64,
    pub inputs: Vec<FrontendPendingUserInput>,
    pub next_after: Option<String>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct WorkItemListItem {
    pub work_item: FrontendWorkItemSnapshot,
    pub revision: u64,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct WorkItemListEnvelope {
    pub items: Vec<WorkItemListItem>,
    pub next_after: Option<String>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct WorkItemTimelineEnvelope {
    pub run_id: String,
    pub work_item_revision: u64,
    pub items: Vec<FrontendTimelineItemSnapshot>,
    pub next_after: Option<u64>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct FrontendTimelineItemSnapshot {
    pub id: String,
    pub connection_id: String,
    pub run_id: String,
    pub revision: u64,
    pub kind: String,
    pub body: String,
    pub created_at_ms: i64,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct FrontendCredentialBinding {
    pub slot: String,
    pub source: String,
    pub authority: Option<String>,
    pub backend: Option<String>,
    pub locator: Option<String>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct FrontendConnection {
    pub id: String,
    pub name: String,
    pub harness: String,
    pub model_provider: String,
    pub provider_state: String,
    pub credentials: Vec<FrontendCredentialBinding>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ConnectionEnvelope {
    pub config_revision: u64,
    pub connection: FrontendConnection,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ConnectionListEnvelope {
    pub config_revision: u64,
    pub connections: Vec<FrontendConnection>,
    pub credential_migration: FrontendCredentialMigrationStatus,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "status", rename_all = "snake_case")]
pub enum FrontendCredentialMigrationStatus {
    Ready,
    Pending {
        reason: FrontendCredentialMigrationPendingReason,
    },
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum FrontendCredentialMigrationPendingReason {
    Locked,
    Denied,
    Unavailable,
    Collision,
    MissingSource,
    SourceInUse,
    Failed,
    CleanupRequired,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct SecretWriteEnvelope {
    pub overwritten: bool,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ArtifactListItem {
    pub id: String,
    pub kind: String,
    pub byte_len: u64,
    pub work_item_id: String,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ArtifactListEnvelope {
    /// Snapshot revision for the owning work item to support event-gap recovery.
    pub revision: u64,
    pub artifacts: Vec<ArtifactListItem>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct OpenArtifactEnvelope {
    pub artifact: ArtifactListItem,
    pub bytes: Vec<u8>,
    pub media_type: String,
}

#[derive(Clone)]
pub struct ApiPorts {
    pub store: Arc<dyn AppStore>,
    pub harness: Arc<dyn RunHarness>,
    pub clock: Arc<dyn Clock>,
    pub ids: Arc<dyn IdGenerator>,
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
    pub async fn new(ports: ApiPorts) -> std::result::Result<Self, StartupError> {
        let (events, _) = broadcast::channel(APP_EVENT_CAPACITY);
        let run_supervisor = Arc::new(RunSupervisor::new(
            ports.store.clone(),
            ports.harness,
            ports.clock,
            ports.ids,
        ));
        run_supervisor
            .ready()
            .await
            .map_err(Self::map_startup_error)?;
        let mut source = run_supervisor.subscribe();
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

        Ok(Self {
            store: ports.store,
            run_supervisor,
            config: ports.config,
            secrets: ports.secrets,
            cache: ports.cache,
            artifacts: ports.artifacts,
            events,
        })
    }

    fn map_startup_error(error: RunOrchestrationError) -> StartupError {
        match error {
            RunOrchestrationError::Store(_) => StartupError::persistence(),
            _ => StartupError::internal(),
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
        self.get_work_item_snapshot_page(id, None, RECOVERY_PAGE_SIZE, None)
            .await
    }

    pub async fn get_work_item_snapshot_page(
        &self,
        id: WorkItemId,
        after: Option<RunId>,
        limit: u32,
        expected_revision: Option<u64>,
    ) -> Result<WorkItemSnapshotEnvelope> {
        for _ in 0..SNAPSHOT_RETRIES {
            let work_item = self
                .store
                .get_work_item(id)
                .await
                .map_err(map_store_error)?;
            let revision = work_item.revision.get();
            if expected_revision.is_some_and(|expected| expected != revision) {
                return Err(DesktopError::conflict("work item revision changed"));
            }
            let runs = self
                .store
                .list_runs_for_work_item(id, after, limit.min(RECOVERY_PAGE_SIZE))
                .await
                .map_err(map_store_error)?;
            let current = self
                .store
                .get_work_item(id)
                .await
                .map_err(map_store_error)?;
            if current.revision.get() == revision {
                return Ok(WorkItemSnapshotEnvelope {
                    revision,
                    work_item: frontend_work_item(work_item),
                    runs: runs.items.into_iter().map(frontend_run).collect(),
                    next_run_after: runs.next_after.map(|id| id.to_string()),
                });
            }
        }
        Err(DesktopError::conflict("work item changed during snapshot"))
    }

    pub async fn get_run_approval_page(
        &self,
        work_item_id: WorkItemId,
        run_id: RunId,
        after: Option<ApprovalRequestId>,
        limit: u32,
        expected_revision: Option<u64>,
    ) -> Result<RunApprovalPageEnvelope> {
        let run = self.store.get_run(run_id).await.map_err(map_store_error)?;
        if run.work_item_id != work_item_id {
            return Err(DesktopError::invalid_request(
                "run does not belong to work item",
            ));
        }
        for _ in 0..SNAPSHOT_RETRIES {
            let revision = self
                .store
                .get_work_item(work_item_id)
                .await
                .map_err(map_store_error)?
                .revision
                .get();
            if expected_revision.is_some_and(|expected| expected != revision) {
                return Err(DesktopError::conflict("work item revision changed"));
            }
            let page = self
                .store
                .list_approvals_for_run(run_id, after, limit.min(RECOVERY_PAGE_SIZE))
                .await
                .map_err(map_store_error)?;
            let current = self
                .store
                .get_work_item(work_item_id)
                .await
                .map_err(map_store_error)?;
            if current.revision.get() == revision {
                return Ok(RunApprovalPageEnvelope {
                    work_item_revision: revision,
                    approvals: page.items.into_iter().map(frontend_approval).collect(),
                    next_after: page.next_after.map(|id| id.to_string()),
                });
            }
        }
        Err(DesktopError::conflict("work item changed during snapshot"))
    }

    pub async fn get_pending_user_input_page(
        &self,
        work_item_id: WorkItemId,
        run_id: RunId,
        after: Option<TimelineItemId>,
        limit: u32,
        expected_revision: Option<u64>,
    ) -> Result<PendingUserInputPageEnvelope> {
        let run = self.store.get_run(run_id).await.map_err(map_store_error)?;
        if run.work_item_id != work_item_id {
            return Err(DesktopError::invalid_request(
                "run does not belong to work item",
            ));
        }
        for _ in 0..SNAPSHOT_RETRIES {
            let revision = self
                .store
                .get_work_item(work_item_id)
                .await
                .map_err(map_store_error)?
                .revision
                .get();
            if expected_revision.is_some_and(|expected| expected != revision) {
                return Err(DesktopError::conflict("work item revision changed"));
            }
            let page = self
                .store
                .list_pending_user_inputs_for_run(run_id, after, limit.min(RECOVERY_PAGE_SIZE))
                .await
                .map_err(map_store_error)?;
            let current = self
                .store
                .get_work_item(work_item_id)
                .await
                .map_err(map_store_error)?;
            if current.revision.get() == revision {
                return Ok(PendingUserInputPageEnvelope {
                    work_item_revision: revision,
                    inputs: page
                        .items
                        .into_iter()
                        .map(|input| FrontendPendingUserInput {
                            id: input.id.to_string(),
                            run_id: input.run_id.to_string(),
                            prompt: input.prompt,
                        })
                        .collect(),
                    next_after: page.next_after.map(|id| id.to_string()),
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
                        work_item: frontend_work_item(work_item),
                        revision,
                    }
                })
                .collect(),
            next_after: page.next_after.map(|id| id.to_string()),
        })
    }

    pub async fn connection_put(
        &self,
        expected_config_revision: ConfigRevision,
        connection: PublicConnection,
    ) -> Result<ConnectionEnvelope> {
        let connection_id = connection.id;
        let connection = to_domain_connection(connection)?;
        let snapshot = self
            .config
            .put_connection(PutConnectionCommand {
                expected_config_revision,
                connection,
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
            config_revision: snapshot.revision.get(),
            connection: frontend_connection(connection),
        })
    }

    pub async fn connection_get(&self, id: ConnectionId) -> Result<ConnectionEnvelope> {
        let snapshot = self
            .config
            .get_connection(id)
            .await
            .map_err(map_config_error)?;
        Ok(ConnectionEnvelope {
            config_revision: snapshot.config_revision.get(),
            connection: frontend_connection(snapshot.connection),
        })
    }

    pub async fn list_connections(&self) -> Result<ConnectionListEnvelope> {
        let list = self
            .config
            .list_connections()
            .await
            .map_err(map_config_error)?;
        Ok(ConnectionListEnvelope {
            config_revision: list.config_revision.get(),
            credential_migration: match list.credential_migration {
                CredentialMigrationStatus::Ready => FrontendCredentialMigrationStatus::Ready,
                CredentialMigrationStatus::Pending(reason) => {
                    FrontendCredentialMigrationStatus::Pending {
                        reason: match reason {
                            CredentialMigrationPendingReason::Locked => {
                                FrontendCredentialMigrationPendingReason::Locked
                            }
                            CredentialMigrationPendingReason::Denied => {
                                FrontendCredentialMigrationPendingReason::Denied
                            }
                            CredentialMigrationPendingReason::Unavailable => {
                                FrontendCredentialMigrationPendingReason::Unavailable
                            }
                            CredentialMigrationPendingReason::Collision => {
                                FrontendCredentialMigrationPendingReason::Collision
                            }
                            CredentialMigrationPendingReason::MissingSource => {
                                FrontendCredentialMigrationPendingReason::MissingSource
                            }
                            CredentialMigrationPendingReason::SourceInUse => {
                                FrontendCredentialMigrationPendingReason::SourceInUse
                            }
                            CredentialMigrationPendingReason::Failed => {
                                FrontendCredentialMigrationPendingReason::Failed
                            }
                            CredentialMigrationPendingReason::CleanupRequired => {
                                FrontendCredentialMigrationPendingReason::CleanupRequired
                            }
                        },
                    }
                }
            },
            connections: list
                .connections
                .into_iter()
                .map(frontend_connection)
                .collect(),
        })
    }

    pub async fn start_run(
        &self,
        work_item_id: WorkItemId,
        connection_id: ConnectionId,
        input: impl Into<String>,
    ) -> Result<RunId> {
        let input = input.into();
        validate_text_limit(&input, MAX_RUN_INPUT_BYTES, "run input is too large")?;
        Ok(self
            .run_supervisor
            .start_run(work_item_id, connection_id, input)
            .await
            .map_err(map_run_error)?
            .id)
    }

    pub async fn steer_run(&self, run_id: RunId, message: impl Into<String>) -> Result<()> {
        let message = message.into();
        validate_text_limit(
            &message,
            MAX_STEER_INPUT_BYTES,
            "steering input is too large",
        )?;
        self.run_supervisor
            .steer(run_id, message)
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
        let response = response.into();
        validate_text_limit(
            &response,
            MAX_USER_INPUT_RESPONSE_BYTES,
            "user-input response is too large",
        )?;
        self.run_supervisor
            .respond_user_input(request_id, response)
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
                    id: artifact.id.to_string(),
                    kind: artifact_kind(artifact.kind),
                    byte_len: artifact.byte_len,
                    work_item_id: artifact.work_item_id.to_string(),
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
                id: artifact.id.to_string(),
                kind: artifact_kind(artifact.kind),
                byte_len: artifact.byte_len,
                work_item_id: artifact.work_item_id.to_string(),
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
        self.get_work_item_timeline_page_at_revision(work_item_id, run_id, after, limit, None)
            .await
    }

    pub async fn get_work_item_timeline_page_at_revision(
        &self,
        work_item_id: WorkItemId,
        run_id: RunId,
        after: Option<TimelineRevision>,
        limit: u32,
        expected_revision: Option<u64>,
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
            if expected_revision.is_some_and(|expected| expected != work_item_revision) {
                return Err(DesktopError::conflict("work item revision changed"));
            }
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
                    run_id: run_id.to_string(),
                    work_item_revision,
                    items: page.items.into_iter().map(frontend_timeline_item).collect(),
                    next_after: page.next_after.map(TimelineRevision::get),
                });
            }
        }
        Err(DesktopError::conflict("work item changed during snapshot"))
    }
}

fn validate_text_limit(value: &str, max_bytes: usize, message: &'static str) -> Result<()> {
    if value.len() > max_bytes {
        Err(DesktopError::invalid_request(message))
    } else {
        Ok(())
    }
}

fn frontend_run(run: RunSnapshot) -> FrontendRunSnapshot {
    FrontendRunSnapshot {
        id: run.id.to_string(),
        connection_id: run.connection_id.to_string(),
        work_item_id: run.work_item_id.to_string(),
        status: map_run_status(run.status),
        created_at_ms: run.created_at.unix_millis(),
        ended_at_ms: run.ended_at.map(UtcTimestamp::unix_millis),
    }
}

fn frontend_approval(approval: ApprovalSnapshot) -> FrontendApprovalSnapshot {
    let (status, decision) = match approval.status {
        ApprovalStatus::Pending => ("pending", None),
        ApprovalStatus::Responding { decision } => {
            ("responding", Some(approval_decision(decision)))
        }
        ApprovalStatus::Resolved { decision } => ("resolved", Some(approval_decision(decision))),
        ApprovalStatus::Voided { decision } => ("voided", decision.map(approval_decision)),
    };
    FrontendApprovalSnapshot {
        id: approval.id.to_string(),
        connection_id: approval.connection_id.to_string(),
        run_id: approval.run_id.to_string(),
        kind: approval.kind,
        summary: approval.summary,
        status: status.to_owned(),
        decision: decision.map(str::to_owned),
        requested_at_ms: approval.requested_at.unix_millis(),
        response_started_at_ms: approval.response_started_at.map(UtcTimestamp::unix_millis),
        resolved_at_ms: approval.resolved_at.map(UtcTimestamp::unix_millis),
        voided_at_ms: approval.voided_at.map(UtcTimestamp::unix_millis),
    }
}

fn frontend_timeline_item(item: TimelineItemSnapshot) -> FrontendTimelineItemSnapshot {
    FrontendTimelineItemSnapshot {
        id: item.id.to_string(),
        connection_id: item.connection_id.to_string(),
        run_id: item.run_id.to_string(),
        revision: item.revision.get(),
        kind: item.kind,
        body: item.body,
        created_at_ms: item.created_at.unix_millis(),
    }
}

fn frontend_work_item(item: WorkItemSnapshot) -> FrontendWorkItemSnapshot {
    FrontendWorkItemSnapshot {
        id: item.id.to_string(),
        project_id: item.project_id.to_string(),
        title: item.title,
        status: match item.status {
            yakshed_domain::WorkItemStatus::Ready => "ready",
            yakshed_domain::WorkItemStatus::Archived => "archived",
        }
        .to_owned(),
        parent_id: item.parent_id.map(|id| id.to_string()),
        revision: item.revision.get(),
        created_at_ms: item.created_at.unix_millis(),
        updated_at_ms: item.updated_at.unix_millis(),
    }
}

fn frontend_connection(connection: PublicConnection) -> FrontendConnection {
    FrontendConnection {
        id: connection.id.to_string(),
        name: connection.name,
        harness: connection.harness,
        model_provider: connection.model_provider,
        provider_state: connection.provider_state.to_string(),
        credentials: connection
            .credentials
            .into_iter()
            .map(|binding| {
                let (source, authority, backend, locator) = match binding.source {
                    PublicCredentialSource::Delegated { authority } => {
                        ("delegated", Some(authority), None, None)
                    }
                    PublicCredentialSource::Secret { backend, locator } => (
                        "secret",
                        None,
                        Some(backend.to_string()),
                        Some(locator.to_string()),
                    ),
                    PublicCredentialSource::Disabled => ("disabled", None, None, None),
                };
                FrontendCredentialBinding {
                    slot: binding.slot.to_string(),
                    source: source.to_owned(),
                    authority,
                    backend,
                    locator,
                }
            })
            .collect(),
    }
}

fn approval_decision(decision: yakshed_domain::ApprovalDecision) -> &'static str {
    match decision {
        yakshed_domain::ApprovalDecision::Approved => "approved",
        yakshed_domain::ApprovalDecision::Denied => "denied",
    }
}

fn artifact_kind(kind: ArtifactKind) -> String {
    match kind {
        ArtifactKind::Plan => "plan",
        ArtifactKind::Diff => "diff",
        ArtifactKind::File => "file",
        ArtifactKind::Image => "image",
        ArtifactKind::CommandLog => "command_log",
        ArtifactKind::BrowserCapture => "browser_capture",
        ArtifactKind::ProviderPayload => "provider_payload",
    }
    .to_owned()
}

fn map_frontend_event(event: AppEvent) -> FrontendEvent {
    FrontendEvent {
        work_item_id: event.work_item_id.to_string(),
        revision: event.revision,
        kind: match event.kind {
            AppEventKind::WorkItemPatched => FrontendEventKind::WorkItemPatched,
            AppEventKind::TimelineBatchAppended { run_id, item_count } => {
                FrontendEventKind::TimelineBatchAppended {
                    run_id: run_id.to_string(),
                    item_count,
                }
            }
            AppEventKind::ApprovalOpened {
                run_id,
                approval_id,
            } => FrontendEventKind::ApprovalOpened {
                run_id: run_id.to_string(),
                approval_id: approval_id.to_string(),
            },
            AppEventKind::ApprovalResolved {
                run_id,
                approval_id,
            } => FrontendEventKind::ApprovalResolved {
                run_id: run_id.to_string(),
                approval_id: approval_id.to_string(),
            },
            AppEventKind::UserInputOpened {
                run_id,
                request_id,
                prompt,
            } => FrontendEventKind::UserInputOpened {
                run_id: run_id.to_string(),
                request_id: request_id.to_string(),
                prompt,
            },
            AppEventKind::UserInputResponded { run_id, request_id } => {
                FrontendEventKind::UserInputResponded {
                    run_id: run_id.to_string(),
                    request_id: request_id.to_string(),
                }
            }
            AppEventKind::RunStatusChanged { run_id, status } => {
                FrontendEventKind::RunStatusChanged {
                    run_id: run_id.to_string(),
                    status: map_run_status(status),
                }
            }
            AppEventKind::RunOutcomeUnknown { run_id, operation } => {
                FrontendEventKind::RunOutcomeUnknown {
                    run_id: run_id.to_string(),
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
