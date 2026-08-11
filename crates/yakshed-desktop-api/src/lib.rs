//! Frontend-safe command facade over application use-cases and infrastructure ports.

use std::collections::HashMap;
use std::sync::Arc;

use tokio::sync::{Mutex, broadcast};
use yakshed_application::{
    AppEvent, AppEventKind, AppStore, ArtifactPort, ArtifactPortError, CachePort, CachePortError,
    ConfigPort, ConfigPortError, ConfigRevision, CreateProject, CreateWorkItem, ListWorkItems,
    OpenArtifactCommand, OpenArtifactPayload, PublicConnection, PublicCredentialBinding,
    PublicCredentialSource, PutConnectionCommand, RunOrchestrationError, RunSupervisor, SecretPort,
    SecretPortError, SetConnectionCredentialCommand, StoreError,
};
use yakshed_domain::{
    ApprovalRequestId, ArtifactId, ArtifactKind, Connection, ConnectionId, CredentialBinding,
    CredentialBindingRecord, CredentialSlot, ProjectId, RunId, RunStatus, SecretBackendId,
    SecretLocator, SecretReference, WorkItemId, WorkItemSnapshot,
};

pub const APP_EVENT_CAPACITY: usize = 128;

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
    Running,
    Completed,
    Failed,
    Interrupted,
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
    pub work_item: WorkItemSnapshot,
    pub revision: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct WorkItemListEnvelope {
    pub items: Vec<WorkItemSnapshot>,
    pub next_after: Option<WorkItemId>,
    pub revision: u64,
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
    event_revisions: Arc<Mutex<HashMap<WorkItemId, u64>>>,
}

impl DesktopApi {
    /// Drops oldest events on overflow; consumers recovering missed revisions must call snapshot APIs.
    pub fn new(ports: ApiPorts) -> Self {
        let (events, _) = broadcast::channel(APP_EVENT_CAPACITY);
        let mut source = ports.run_supervisor.subscribe();
        let relay = events.clone();
        let event_revisions = Arc::new(Mutex::new(HashMap::new()));
        let revisions = event_revisions.clone();
        tokio::spawn(async move {
            loop {
                match source.recv().await {
                    Ok(event) => {
                        let mut revisions = revisions.lock().await;
                        revisions.insert(event.work_item_id, event.revision);
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
            event_revisions,
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
        Ok(WorkItemSnapshotEnvelope {
            revision: self
                .snapshot_revision(work_item.id, work_item.revision.get())
                .await,
            work_item,
        })
    }

    pub async fn get_work_item_snapshot(&self, id: WorkItemId) -> Result<WorkItemSnapshotEnvelope> {
        let work_item = self
            .store
            .get_work_item(id)
            .await
            .map_err(map_store_error)?;
        Ok(WorkItemSnapshotEnvelope {
            revision: self.snapshot_revision(id, work_item.revision.get()).await,
            work_item,
        })
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
        let mut revision = page
            .items
            .iter()
            .map(|item| item.revision.get())
            .max()
            .unwrap_or(0);
        for item in &page.items {
            revision = revision.max(self.snapshot_revision(item.id, item.revision.get()).await);
        }
        Ok(WorkItemListEnvelope {
            items: page.items,
            next_after: page.next_after,
            revision,
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
        let connection = self
            .config
            .get_connection(id)
            .await
            .map_err(map_config_error)?;
        let config_revision = self
            .config
            .list_connections()
            .await
            .map_err(map_config_error)?
            .config_revision;
        Ok(ConnectionEnvelope {
            config_revision,
            connection,
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
                value: value.into(),
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
        let revision = self.snapshot_revision(work_item_id, revision).await;
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

    async fn snapshot_revision(&self, work_item_id: WorkItemId, fallback: u64) -> u64 {
        let current = *self
            .event_revisions
            .lock()
            .await
            .get(&work_item_id)
            .unwrap_or(&fallback);
        current.max(fallback)
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
        RunStatus::Running => FrontendRunStatus::Running,
        RunStatus::Completed => FrontendRunStatus::Completed,
        RunStatus::Failed => FrontendRunStatus::Failed,
        RunStatus::Interrupted => FrontendRunStatus::Interrupted,
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
        RunOrchestrationError::ApprovalNotPending(_) => {
            DesktopError::invalid_request("approval is not pending")
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
