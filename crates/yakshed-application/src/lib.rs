//! Application use cases, orchestration, snapshots, revisions, and application-owned ports, independent of Tauri commands and provider wire protocols.

use std::path::Path;
use std::{collections::HashSet, error::Error, fmt};

use async_trait::async_trait;
use thiserror::Error as ThisError;
use yakshed_domain::{
    ApprovalDecision, ApprovalRequestId, ApprovalSnapshot, AuditEventId, Connection, ConnectionId,
    CredentialBinding, NamespacedProviderId, ProjectId, ProjectSnapshot, RunId, RunSnapshot,
    RunStatus, SecretBackend, SecretBackendId, SecretBackendSettings, StreamCursor,
    TimelineBatchId, TimelineItemId, TimelineItemSnapshot, TimelineRevision, UtcTimestamp,
    WorkItemId, WorkItemSnapshot,
};

/// Canonical non-secret application configuration.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct AppConfig {
    pub connections: Vec<Connection>,
    pub secret_backends: Vec<SecretBackend>,
    pub ui: UiConfig,
}

impl AppConfig {
    pub fn validate(
        &self,
        backend_capabilities: &[SecretBackendCapability],
    ) -> Result<(), ConfigValidationError> {
        if self.ui.theme.trim().is_empty() {
            return Err(ConfigValidationError::invalid("ui.theme cannot be empty"));
        }

        let mut backend_ids = HashSet::new();
        let mut local_file_paths = HashSet::new();
        for backend in &self.secret_backends {
            backend
                .validate()
                .map_err(|error| ConfigValidationError::invalid(error.to_string()))?;
            validate_backend_configuration(backend, backend_capabilities)?;
            if let SecretBackendSettings::LocalFile { path } = &backend.settings
                && !local_file_paths.insert(path)
            {
                return Err(SecretBackendConfigurationError::DuplicateLocalFilePath {
                    backend: backend.id.clone(),
                }
                .into());
            }
            if !backend_ids.insert(&backend.id) {
                return Err(ConfigValidationError::invalid(format!(
                    "duplicate secret backend id: {}",
                    backend.id
                )));
            }
        }

        let mut connection_ids = HashSet::new();
        let mut provider_state_roots = HashSet::new();
        for connection in &self.connections {
            connection
                .validate()
                .map_err(|error| ConfigValidationError::invalid(error.to_string()))?;
            if !connection_ids.insert(connection.id) {
                return Err(ConfigValidationError::invalid(format!(
                    "duplicate connection id: {}",
                    connection.id
                )));
            }
            if !provider_state_roots.insert(&connection.provider_state) {
                return Err(ConfigValidationError::invalid(format!(
                    "duplicate provider state root: {}",
                    connection.provider_state
                )));
            }
            for credential in &connection.credentials {
                if let CredentialBinding::Secret { reference } = &credential.binding
                    && !backend_ids.contains(&reference.backend_id)
                {
                    return Err(ConfigValidationError::invalid(format!(
                        "credential references unknown secret backend: {}",
                        reference.backend_id
                    )));
                }
            }
        }
        Ok(())
    }
}

/// User-interface preferences stored in config.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct UiConfig {
    pub theme: String,
}

impl Default for UiConfig {
    fn default() -> Self {
        Self {
            theme: "system".to_owned(),
        }
    }
}

/// Monotonic in-process config revision used for optimistic concurrency.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ConfigRevision(u64);

impl ConfigRevision {
    pub const INITIAL: Self = Self(0);

    pub const fn new(value: u64) -> Self {
        Self(value)
    }

    pub const fn get(self) -> u64 {
        self.0
    }
}

/// Config plus the revision at which it was observed.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ConfigSnapshot {
    pub revision: ConfigRevision,
    pub config: AppConfig,
}

/// Validated configuration mutations available to application callers.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ConfigChange {
    PutConnection(Connection),
    /// Atomically registers the backends required by a connection and stores the connection.
    PutConnectionWithSecretBackends {
        connection: Connection,
        secret_backends: Vec<SecretBackend>,
    },
    RemoveConnection(ConnectionId),
    PutSecretBackend(SecretBackend),
    RemoveSecretBackend(SecretBackendId),
    SetUiTheme(String),
    Reset,
}

/// A violated invariant spanning canonical configuration values.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ConfigValidationError {
    Invalid(String),
    SecretBackend(SecretBackendConfigurationError),
}

impl ConfigValidationError {
    fn invalid(message: impl Into<String>) -> Self {
        Self::Invalid(message.into())
    }
}

impl fmt::Display for ConfigValidationError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Invalid(message) => message.fmt(formatter),
            Self::SecretBackend(error) => error.fmt(formatter),
        }
    }
}

impl Error for ConfigValidationError {}

impl From<SecretBackendConfigurationError> for ConfigValidationError {
    fn from(error: SecretBackendConfigurationError) -> Self {
        Self::SecretBackend(error)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum SecretBackendConfigurationError {
    UnsupportedKind {
        backend: SecretBackendId,
        kind: &'static str,
    },
    MissingFeature {
        backend: SecretBackendId,
        kind: &'static str,
        feature: &'static str,
    },
    UnsupportedPlatform {
        backend: SecretBackendId,
        kind: &'static str,
    },
    WrongKind {
        backend: SecretBackendId,
    },
    DuplicateLocalFilePath {
        backend: SecretBackendId,
    },
    AbsolutePathRequired {
        backend: SecretBackendId,
    },
}

impl fmt::Display for SecretBackendConfigurationError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::UnsupportedKind { backend, kind } => {
                write!(
                    formatter,
                    "secret backend {backend} uses unsupported kind {kind}"
                )
            }
            Self::MissingFeature {
                backend,
                kind,
                feature,
            } => write!(
                formatter,
                "secret backend {backend} kind {kind} requires Cargo feature {feature}"
            ),
            Self::UnsupportedPlatform { backend, kind } => write!(
                formatter,
                "secret backend {backend} kind {kind} is unsupported on this platform"
            ),
            Self::WrongKind { backend } => {
                write!(formatter, "secret backend {backend} is not local-file")
            }
            Self::DuplicateLocalFilePath { backend } => write!(
                formatter,
                "secret backend {backend} duplicates another local-file path"
            ),
            Self::AbsolutePathRequired { backend } => {
                write!(
                    formatter,
                    "secret backend {backend} requires an absolute path"
                )
            }
        }
    }
}

impl Error for SecretBackendConfigurationError {}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SecretBackendCapability {
    pub kind: &'static str,
    pub availability: SecretBackendAvailability,
}

impl SecretBackendCapability {
    pub const fn available(kind: &'static str) -> Self {
        Self {
            kind,
            availability: SecretBackendAvailability::Available,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SecretBackendAvailability {
    Available,
    MissingFeature { feature: &'static str },
    UnsupportedPlatform,
}

pub fn validate_backend_configuration(
    backend: &SecretBackend,
    backend_capabilities: &[SecretBackendCapability],
) -> Result<(), SecretBackendConfigurationError> {
    if let SecretBackendSettings::LocalFile { path } = &backend.settings
        && !Path::new(path).is_absolute()
    {
        return Err(SecretBackendConfigurationError::AbsolutePathRequired {
            backend: backend.id.clone(),
        });
    }
    let kind = backend.kind();
    let Some(capability) = backend_capabilities
        .iter()
        .find(|capability| capability.kind == kind)
    else {
        return Err(SecretBackendConfigurationError::UnsupportedKind {
            backend: backend.id.clone(),
            kind,
        });
    };
    match capability.availability {
        SecretBackendAvailability::Available => Ok(()),
        SecretBackendAvailability::MissingFeature { feature } => {
            Err(SecretBackendConfigurationError::MissingFeature {
                backend: backend.id.clone(),
                kind,
                feature,
            })
        }
        SecretBackendAvailability::UnsupportedPlatform => {
            Err(SecretBackendConfigurationError::UnsupportedPlatform {
                backend: backend.id.clone(),
                kind,
            })
        }
    }
}

pub trait Clock: Send + Sync {
    fn now(&self) -> UtcTimestamp;
}

pub struct SystemClock;

impl Clock for SystemClock {
    fn now(&self) -> UtcTimestamp {
        let now = time::OffsetDateTime::now_utc();
        UtcTimestamp::from_unix_millis(now.unix_timestamp() * 1_000 + i64::from(now.millisecond()))
    }
}

pub trait IdGenerator: Send + Sync {
    fn next_project_id(&self) -> ProjectId;
    fn next_work_item_id(&self) -> WorkItemId;
    fn next_run_id(&self) -> RunId;
    fn next_timeline_batch_id(&self) -> TimelineBatchId;
    fn next_timeline_item_id(&self) -> TimelineItemId;
    fn next_approval_request_id(&self) -> ApprovalRequestId;
    fn next_audit_event_id(&self) -> AuditEventId;
}

pub struct SystemIdGenerator;

impl IdGenerator for SystemIdGenerator {
    fn next_project_id(&self) -> ProjectId {
        ProjectId::new_v7()
    }

    fn next_work_item_id(&self) -> WorkItemId {
        WorkItemId::new_v7()
    }

    fn next_run_id(&self) -> RunId {
        RunId::new_v7()
    }

    fn next_timeline_batch_id(&self) -> TimelineBatchId {
        TimelineBatchId::new_v7()
    }

    fn next_timeline_item_id(&self) -> TimelineItemId {
        TimelineItemId::new_v7()
    }

    fn next_approval_request_id(&self) -> ApprovalRequestId {
        ApprovalRequestId::new_v7()
    }

    fn next_audit_event_id(&self) -> AuditEventId {
        AuditEventId::new_v7()
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CreateProject {
    pub id: ProjectId,
    pub name: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ProjectPage {
    pub items: Vec<ProjectSnapshot>,
    pub next_after: Option<ProjectId>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CreateWorkItem {
    pub id: WorkItemId,
    pub project_id: ProjectId,
    pub title: String,
    pub parent_id: Option<WorkItemId>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ListWorkItems {
    pub project_id: ProjectId,
    pub after: Option<WorkItemId>,
    pub limit: u32,
    pub include_archived: bool,
}

impl ListWorkItems {
    pub fn for_project(project_id: ProjectId, limit: u32) -> Self {
        Self {
            project_id,
            after: None,
            limit,
            include_archived: false,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct WorkItemPage {
    pub items: Vec<WorkItemSnapshot>,
    pub next_after: Option<WorkItemId>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CreateRun {
    pub id: RunId,
    pub connection_id: ConnectionId,
    pub work_item_id: WorkItemId,
    pub provider_run: Option<NamespacedProviderId>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct TransitionRun {
    pub run_id: RunId,
    pub expected_current: RunStatus,
    pub target: RunStatus,
    pub occurred_at: UtcTimestamp,
    pub audit_event_id: AuditEventId,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RunPage {
    pub items: Vec<RunSnapshot>,
    pub next_after: Option<RunId>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct NewTimelineItem {
    pub id: TimelineItemId,
    pub kind: String,
    pub body: String,
    pub provider_id: Option<NamespacedProviderId>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TimelineBatch {
    pub batch_id: TimelineBatchId,
    pub connection_id: ConnectionId,
    pub run_id: RunId,
    pub source_namespace: String,
    pub stream_id: String,
    pub expected_stream_revision: StreamCursor,
    pub items: Vec<NewTimelineItem>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ListTimeline {
    pub run_id: RunId,
    pub after: Option<TimelineRevision>,
    pub limit: u32,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TimelinePage {
    pub items: Vec<TimelineItemSnapshot>,
    pub next_after: Option<TimelineRevision>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct GetStreamCursor {
    pub connection_id: ConnectionId,
    pub run_id: RunId,
    pub source_namespace: String,
    pub stream_id: String,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct StreamCursorState {
    pub cursor: StreamCursor,
    pub last_batch_id: TimelineBatchId,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PendingApproval {
    pub id: ApprovalRequestId,
    pub run_id: RunId,
    pub provider_id: NamespacedProviderId,
    pub kind: String,
    pub summary: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BeginApprovalResponse {
    pub approval_id: ApprovalRequestId,
    pub decision: ApprovalDecision,
    pub audit_event_id: AuditEventId,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ConfirmApprovalResponse {
    pub approval_id: ApprovalRequestId,
    pub audit_event_id: AuditEventId,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ApprovalPage {
    pub items: Vec<ApprovalSnapshot>,
    pub next_after: Option<ApprovalRequestId>,
}

#[derive(Debug, ThisError)]
pub enum StoreError {
    #[error("{entity} not found: {id}")]
    NotFound { entity: &'static str, id: String },
    #[error("store invariant conflict: {0}")]
    Conflict(String),
    #[error("database integrity failure: {0}")]
    Integrity(String),
    #[error("database migration failure: {0}")]
    Migration(String),
    #[error("database schema {found} is newer than supported schema {supported}")]
    UnsupportedNewerSchema { found: u32, supported: u32 },
    #[error("database open failure: {0}")]
    Open(String),
    #[error("database backend failure: {0}")]
    Backend(String),
    #[error("database store is shut down")]
    Closed,
}

/// Application-shaped durable state operations; implementations expose no storage handles.
/// Create operations are idempotent by supplied ID when immutable command content matches.
#[async_trait]
pub trait AppStore: Send + Sync {
    async fn create_project(&self, command: CreateProject) -> Result<ProjectSnapshot, StoreError>;
    async fn list_projects(
        &self,
        after: Option<ProjectId>,
        limit: u32,
    ) -> Result<ProjectPage, StoreError>;
    async fn create_work_item(
        &self,
        command: CreateWorkItem,
    ) -> Result<WorkItemSnapshot, StoreError>;
    async fn get_work_item(&self, id: WorkItemId) -> Result<WorkItemSnapshot, StoreError>;
    async fn list_work_items(&self, query: ListWorkItems) -> Result<WorkItemPage, StoreError>;
    async fn archive_work_subtree(&self, root: WorkItemId) -> Result<u64, StoreError>;
    async fn create_run(&self, command: CreateRun) -> Result<RunSnapshot, StoreError>;
    async fn get_run(&self, id: RunId) -> Result<RunSnapshot, StoreError>;
    async fn transition_run(&self, command: TransitionRun) -> Result<RunSnapshot, StoreError>;
    async fn list_runs_for_work_item(
        &self,
        work_item_id: WorkItemId,
        after: Option<RunId>,
        limit: u32,
    ) -> Result<RunPage, StoreError>;
    async fn list_active_runs(
        &self,
        after: Option<RunId>,
        limit: u32,
    ) -> Result<RunPage, StoreError>;
    async fn append_timeline_batch(&self, batch: TimelineBatch)
    -> Result<StreamCursor, StoreError>;
    async fn get_stream_cursor(
        &self,
        query: GetStreamCursor,
    ) -> Result<Option<StreamCursorState>, StoreError>;
    async fn list_timeline_page(&self, query: ListTimeline) -> Result<TimelinePage, StoreError>;
    async fn record_pending_approval(
        &self,
        approval: PendingApproval,
    ) -> Result<ApprovalSnapshot, StoreError>;
    async fn list_pending_approvals(
        &self,
        after: Option<ApprovalRequestId>,
        limit: u32,
    ) -> Result<ApprovalPage, StoreError>;
    async fn list_approvals_for_run(
        &self,
        run_id: RunId,
        after: Option<ApprovalRequestId>,
        limit: u32,
    ) -> Result<ApprovalPage, StoreError>;
    async fn begin_approval_response(
        &self,
        response: BeginApprovalResponse,
    ) -> Result<ApprovalSnapshot, StoreError>;
    async fn confirm_approval_response(
        &self,
        response: ConfirmApprovalResponse,
    ) -> Result<ApprovalSnapshot, StoreError>;
    async fn list_unconfirmed_approval_responses(
        &self,
        after: Option<ApprovalRequestId>,
        limit: u32,
    ) -> Result<ApprovalPage, StoreError>;
    async fn shutdown(&self) -> Result<(), StoreError>;
}

#[cfg(test)]
mod tests {
    use super::*;
    use yakshed_domain::ProviderStateRootId;

    fn connection(id: &str) -> Connection {
        Connection {
            id: id.parse().unwrap(),
            name: id.to_owned(),
            harness: "codex".to_owned(),
            model_provider: "openai".to_owned(),
            provider_state: ProviderStateRootId::new("shared-codex").unwrap(),
            credentials: Vec::new(),
        }
    }

    #[test]
    fn config_rejects_connections_sharing_a_provider_state_root() {
        let config = AppConfig {
            connections: vec![
                connection("0193f26e-7a72-7d42-bf77-0de14c4cc111"),
                connection("0193f26e-7a72-7d42-bf77-0de14c4cc222"),
            ],
            ..AppConfig::default()
        };

        assert!(config.validate(&[]).is_err());
    }
}
