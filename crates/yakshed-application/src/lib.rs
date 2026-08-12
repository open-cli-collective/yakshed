//! Application use cases, orchestration, snapshots, revisions, and application-owned ports, independent of Tauri commands and provider wire protocols.

use std::path::Path;
use std::{collections::HashSet, error::Error, fmt};

use async_trait::async_trait;
use secrecy::SecretString;
use thiserror::Error as ThisError;
use yakshed_domain::{
    ApprovalDecision, ApprovalRequestId, ApprovalSnapshot, ArtifactId, ArtifactRecord,
    AuditEventId, Connection, ConnectionId, CredentialBinding, CredentialSlot,
    NamespacedProviderId, ProjectId, ProjectSnapshot, ProviderRunIdentity, ProviderStateRootId,
    RunId, RunSnapshot, RunStatus, SecretBackend, SecretBackendId, SecretBackendSettings,
    SecretLocator, StreamCursor, TimelineBatchId, TimelineItemId, TimelineItemSnapshot,
    TimelineRevision, UtcTimestamp, WorkItemId, WorkItemSnapshot,
};

mod run_supervisor;

pub use run_supervisor::*;

/// Canonical non-secret application configuration.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct AppConfig {
    pub connections: Vec<Connection>,
    pub secret_backends: Vec<SecretBackend>,
    pub credential_migration: Option<CredentialMigrationRecord>,
    pub ui: UiConfig,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CredentialMigrationRecord {
    pub source: SecretBackend,
    pub target: SecretBackend,
    pub phase: CredentialMigrationPhase,
    pub locators: Vec<SecretLocator>,
    pub receipts: Vec<CredentialCopyReceipt>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CredentialMigrationPhase {
    Copying,
    CleanupPending,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CredentialCopyReceipt {
    pub locator: SecretLocator,
    pub state: CredentialCopyState,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CredentialCopyState {
    Copying,
    Copied,
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
                if let CredentialBinding::Secret { reference } = &credential.binding
                    && let Some(backend) = self
                        .secret_backends
                        .iter()
                        .find(|backend| backend.id == reference.backend_id)
                    && let Some(validate_locator) = backend_capabilities
                        .iter()
                        .find(|capability| capability.kind == backend.kind())
                        .and_then(|capability| capability.validate_locator)
                {
                    validate_locator(&reference.locator).map_err(ConfigValidationError::invalid)?;
                }
            }
        }
        if let Some(migration) = &self.credential_migration {
            if !matches!(
                migration.source.settings,
                SecretBackendSettings::LocalFile { .. }
            ) || migration.target.settings != SecretBackendSettings::LocalOs
                || migration.source.id == migration.target.id
            {
                return Err(ConfigValidationError::invalid(
                    "credential migration backend pair is invalid",
                ));
            }
            let mut receipt_locators = HashSet::new();
            if migration
                .receipts
                .iter()
                .any(|receipt| !receipt_locators.insert(&receipt.locator))
            {
                return Err(ConfigValidationError::invalid(
                    "credential migration contains duplicate receipts",
                ));
            }
            let manifest = migration.locators.iter().collect::<HashSet<_>>();
            if manifest.len() != migration.locators.len()
                || receipt_locators
                    .iter()
                    .any(|locator| !manifest.contains(locator))
            {
                return Err(ConfigValidationError::invalid(
                    "credential migration manifest is invalid",
                ));
            }
            let source_configured = self
                .secret_backends
                .iter()
                .any(|backend| backend == &migration.source);
            let target_configured = self
                .secret_backends
                .iter()
                .any(|backend| backend == &migration.target);
            if self
                .secret_backends
                .iter()
                .any(|backend| backend.id == migration.target.id && backend != &migration.target)
            {
                return Err(ConfigValidationError::invalid(
                    "credential migration target id is already in use",
                ));
            }
            let all_copied = migration.locators.iter().all(|locator| {
                migration.receipts.iter().any(|receipt| {
                    receipt.locator == *locator && receipt.state == CredentialCopyState::Copied
                })
            });
            match migration.phase {
                CredentialMigrationPhase::Copying if !source_configured => {
                    return Err(ConfigValidationError::invalid(
                        "copying credential migration requires its source backend",
                    ));
                }
                CredentialMigrationPhase::CleanupPending
                    if source_configured || !target_configured || !all_copied =>
                {
                    return Err(ConfigValidationError::invalid(
                        "cleanup credential migration requires its target and a fully copied manifest",
                    ));
                }
                _ => {}
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

impl fmt::Display for ConfigRevision {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(formatter)
    }
}

/// Config plus the revision at which it was observed.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ConfigSnapshot {
    pub revision: ConfigRevision,
    pub config: AppConfig,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ConfigConnectionSnapshot {
    pub config_revision: ConfigRevision,
    pub connection: PublicConnection,
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
    BeginCredentialMigration(CredentialMigrationRecord),
    RecordCredentialCopy {
        locator: SecretLocator,
        state: CredentialCopyState,
    },
    CheckpointCredentialMigration,
    FinishCredentialMigration,
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
        expected: &'static str,
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
            Self::WrongKind { backend, expected } => {
                write!(formatter, "secret backend {backend} is not {expected}")
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

#[derive(Clone, Copy, Debug)]
pub struct SecretBackendCapability {
    pub kind: &'static str,
    pub availability: SecretBackendAvailability,
    pub access: SecretBackendAccess,
    pub validate_locator: Option<SecretLocatorValidator>,
}

pub type SecretLocatorValidator = fn(&SecretLocator) -> Result<(), &'static str>;

impl SecretBackendCapability {
    pub const fn available(kind: &'static str) -> Self {
        Self {
            kind,
            availability: SecretBackendAvailability::Available,
            access: SecretBackendAccess::ReadWrite,
            validate_locator: None,
        }
    }

    pub const fn resolve_only(kind: &'static str) -> Self {
        Self {
            kind,
            availability: SecretBackendAvailability::Available,
            access: SecretBackendAccess::ResolveOnly,
            validate_locator: None,
        }
    }

    pub const fn with_locator_validator(mut self, validator: SecretLocatorValidator) -> Self {
        self.validate_locator = Some(validator);
        self
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SecretBackendAccess {
    ReadWrite,
    ResolveOnly,
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

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PublicCredentialBinding {
    pub slot: CredentialSlot,
    pub source: PublicCredentialSource,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum PublicCredentialSource {
    Delegated {
        authority: String,
    },
    Secret {
        backend: SecretBackendId,
        locator: SecretLocator,
    },
    Disabled,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PublicConnection {
    pub id: ConnectionId,
    pub name: String,
    pub harness: String,
    pub model_provider: String,
    pub provider_state: ProviderStateRootId,
    pub credentials: Vec<PublicCredentialBinding>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PublicConnectionList {
    pub config_revision: ConfigRevision,
    pub connections: Vec<PublicConnection>,
    pub credential_migration: CredentialMigrationStatus,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CredentialMigrationStatus {
    Ready,
    Pending(CredentialMigrationPendingReason),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CredentialMigrationPendingReason {
    Locked,
    Denied,
    Unavailable,
    Collision,
    MissingSource,
    SourceInUse,
    TargetInUse,
    Failed,
    CleanupRequired,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PutConnectionCommand {
    pub expected_config_revision: ConfigRevision,
    pub connection: Connection,
}

pub struct SetConnectionCredentialCommand {
    pub connection_id: ConnectionId,
    pub slot: CredentialSlot,
    pub value: SecretValue,
    pub overwrite: bool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SecretWriteOutcome {
    pub overwritten: bool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OpenArtifactCommand {
    pub artifact_id: ArtifactId,
    pub max_bytes: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OpenArtifactPayload {
    pub artifact: ArtifactRecord,
    pub bytes: Vec<u8>,
}

#[derive(Debug, ThisError)]
pub enum ConfigPortError {
    #[error("configuration is out of date: expected {expected}, actual {actual}")]
    Conflict {
        expected: ConfigRevision,
        actual: ConfigRevision,
    },
    #[error("configuration value is invalid")]
    Validation,
    #[error("credential requirement is not satisfied: {0}")]
    CredentialRequirement(String),
    #[error("not found")]
    NotFound,
    #[error("unsupported operation")]
    Unsupported,
    #[error("credential migration is pending")]
    MigrationPending,
    #[error("configuration service unavailable")]
    Unavailable,
}

#[derive(Debug, ThisError)]
pub enum SecretPortError {
    #[error("connection not found")]
    ConnectionNotFound,
    #[error("credential binding not found")]
    BindingNotFound,
    #[error("credential binding is not writeable")]
    NotSecretBacked,
    #[error("secret backend is resolve-only")]
    ResolveOnly,
    #[error("secret backend unavailable")]
    BackendUnavailable,
    #[error("secret backend returned locked")]
    Locked,
    #[error("secret backend denied access")]
    Denied,
    #[error("secret backend requires authentication")]
    AuthenticationRequired,
    #[error("secret already exists and overwrite is disabled")]
    AlreadyExists,
    #[error("secret write outcome is uncertain")]
    UncertainWrite,
    #[error("credential migration is pending")]
    MigrationPending,
    #[error("credential operation failed")]
    Failed,
}

pub struct SecretValue {
    value: SecretString,
}

impl SecretValue {
    pub fn new(value: impl Into<String>) -> Self {
        Self {
            value: SecretString::new(value.into().into()),
        }
    }

    pub fn as_secret(&self) -> &SecretString {
        &self.value
    }
}

#[derive(Debug, ThisError)]
pub enum CachePortError {
    #[error("cache operation failed")]
    Failed,
}

#[derive(Debug, ThisError)]
pub enum ArtifactPortError {
    #[error("artifact not found")]
    NotFound,
    #[error("artifact exceeds requested size")]
    TooLarge,
    #[error("artifact operation failed")]
    Failed,
}

#[async_trait]
pub trait ConfigPort: Send + Sync {
    async fn put_connection(
        &self,
        command: PutConnectionCommand,
    ) -> Result<ConfigSnapshot, ConfigPortError>;

    async fn get_connection(
        &self,
        connection_id: ConnectionId,
    ) -> Result<ConfigConnectionSnapshot, ConfigPortError>;

    async fn list_connections(&self) -> Result<PublicConnectionList, ConfigPortError>;
}

#[async_trait]
pub trait SecretPort: Send + Sync {
    async fn set_connection_credential(
        &self,
        command: SetConnectionCredentialCommand,
    ) -> Result<SecretWriteOutcome, SecretPortError>;
}

#[async_trait]
pub trait CachePort: Send + Sync {
    async fn clear(&self) -> Result<(), CachePortError>;
}

#[async_trait]
pub trait ArtifactPort: Send + Sync {
    async fn list_artifacts_for_work_item(
        &self,
        work_item_id: WorkItemId,
    ) -> Result<Vec<ArtifactRecord>, ArtifactPortError>;

    async fn open_artifact(
        &self,
        command: OpenArtifactCommand,
    ) -> Result<OpenArtifactPayload, ArtifactPortError>;
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
    pub provider_run: Option<ProviderRunIdentity>,
}

#[derive(Debug, Eq, PartialEq)]
pub struct TransitionRun {
    pub run_id: RunId,
    pub expected_current: RunStatus,
    pub target: RunStatus,
    pub provider_id: Option<ProviderRunIdentity>,
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
pub struct PendingUserInputSnapshot {
    pub id: TimelineItemId,
    pub run_id: RunId,
    pub prompt: String,
    pub provider_id: NamespacedProviderId,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PendingUserInputPage {
    pub items: Vec<PendingUserInputSnapshot>,
    pub next_after: Option<TimelineItemId>,
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

#[derive(Clone, Debug, ThisError)]
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
    async fn list_runs_needing_reconciliation(
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
    async fn list_pending_user_inputs_for_run(
        &self,
        run_id: RunId,
        after: Option<TimelineItemId>,
        limit: u32,
    ) -> Result<PendingUserInputPage, StoreError>;
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
