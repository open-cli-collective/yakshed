//! Application use cases, orchestration, snapshots, revisions, and application-owned ports, independent of Tauri commands and provider wire protocols.

use std::{collections::HashSet, error::Error, fmt};

use async_trait::async_trait;
use thiserror::Error as ThisError;
use yakshed_domain::{
    ApprovalDecision, ApprovalRequestId, ApprovalSnapshot, AuditEventId, Connection, ConnectionId,
    CredentialBinding, NamespacedProviderId, ProjectId, ProjectSnapshot, ProjectionRevision, RunId,
    RunSnapshot, SecretBackend, SecretBackendId, TimelineItemId, TimelineItemSnapshot,
    UtcTimestamp, WorkItemId, WorkItemSnapshot,
};

/// Canonical non-secret application configuration.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct AppConfig {
    pub connections: Vec<Connection>,
    pub secret_backends: Vec<SecretBackend>,
    pub ui: UiConfig,
}

impl AppConfig {
    pub fn validate(&self) -> Result<(), ConfigValidationError> {
        if self.ui.theme.trim().is_empty() {
            return Err(ConfigValidationError("ui.theme cannot be empty".to_owned()));
        }

        let mut backend_ids = HashSet::new();
        for backend in &self.secret_backends {
            backend
                .validate()
                .map_err(|error| ConfigValidationError(error.to_string()))?;
            if !backend_ids.insert(&backend.id) {
                return Err(ConfigValidationError(format!(
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
                .map_err(|error| ConfigValidationError(error.to_string()))?;
            if !connection_ids.insert(connection.id) {
                return Err(ConfigValidationError(format!(
                    "duplicate connection id: {}",
                    connection.id
                )));
            }
            if !provider_state_roots.insert(&connection.provider_state) {
                return Err(ConfigValidationError(format!(
                    "duplicate provider state root: {}",
                    connection.provider_state
                )));
            }
            for credential in &connection.credentials {
                if let CredentialBinding::Secret { reference } = &credential.binding
                    && !backend_ids.contains(&reference.backend_id)
                {
                    return Err(ConfigValidationError(format!(
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
    RemoveConnection(ConnectionId),
    PutSecretBackend(SecretBackend),
    RemoveSecretBackend(SecretBackendId),
    SetUiTheme(String),
    Reset,
}

/// A violated invariant spanning canonical configuration values.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ConfigValidationError(String);

impl fmt::Display for ConfigValidationError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(formatter)
    }
}

impl Error for ConfigValidationError {}

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
    pub name: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CreateWorkItem {
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
    pub work_item_id: WorkItemId,
    pub provider_run: Option<NamespacedProviderId>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct NewTimelineItem {
    pub kind: String,
    pub body: String,
    pub provider_id: Option<NamespacedProviderId>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TimelineBatch {
    pub run_id: RunId,
    pub source_namespace: String,
    pub stream_id: String,
    pub items: Vec<NewTimelineItem>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ListTimeline {
    pub run_id: RunId,
    pub after: Option<ProjectionRevision>,
    pub limit: u32,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TimelinePage {
    pub items: Vec<TimelineItemSnapshot>,
    pub next_after: Option<ProjectionRevision>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PendingApproval {
    pub run_id: RunId,
    pub provider_id: NamespacedProviderId,
    pub kind: String,
    pub summary: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ApprovalResolution {
    pub approval_id: ApprovalRequestId,
    pub decision: ApprovalDecision,
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
#[async_trait]
pub trait AppStore: Send + Sync {
    async fn create_project(&self, command: CreateProject) -> Result<ProjectSnapshot, StoreError>;
    async fn create_work_item(
        &self,
        command: CreateWorkItem,
    ) -> Result<WorkItemSnapshot, StoreError>;
    async fn get_work_item(&self, id: WorkItemId) -> Result<WorkItemSnapshot, StoreError>;
    async fn list_work_items(&self, query: ListWorkItems) -> Result<WorkItemPage, StoreError>;
    async fn archive_work_subtree(&self, root: WorkItemId) -> Result<u64, StoreError>;
    async fn create_run(&self, command: CreateRun) -> Result<RunSnapshot, StoreError>;
    async fn append_timeline_batch(
        &self,
        batch: TimelineBatch,
    ) -> Result<ProjectionRevision, StoreError>;
    async fn list_timeline_page(&self, query: ListTimeline) -> Result<TimelinePage, StoreError>;
    async fn record_pending_approval(
        &self,
        approval: PendingApproval,
    ) -> Result<ApprovalSnapshot, StoreError>;
    async fn resolve_approval(&self, resolution: ApprovalResolution) -> Result<(), StoreError>;
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

        assert!(config.validate().is_err());
    }
}
