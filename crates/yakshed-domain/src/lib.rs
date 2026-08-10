//! Domain values and invariants for IDs, work graphs, connections, artifacts, approvals, and lifecycle state, with no I/O or Tauri coupling.

use std::{collections::HashSet, error::Error, fmt, str::FromStr};

use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// Stable identity of a configured connection.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub struct ConnectionId(Uuid);

impl FromStr for ConnectionId {
    type Err = ValidationError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        parse_uuid_v7("connection id", value).map(Self)
    }
}

impl TryFrom<String> for ConnectionId {
    type Error = ValidationError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        value.parse()
    }
}

impl From<ConnectionId> for String {
    fn from(value: ConnectionId) -> Self {
        value.to_string()
    }
}

impl fmt::Display for ConnectionId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(formatter)
    }
}

/// Stable identity of one artifact metadata record.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub struct ArtifactId(Uuid);

impl FromStr for ArtifactId {
    type Err = ValidationError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        parse_uuid_v7("artifact id", value).map(Self)
    }
}

impl TryFrom<String> for ArtifactId {
    type Error = ValidationError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        value.parse()
    }
}

impl From<ArtifactId> for String {
    fn from(value: ArtifactId) -> Self {
        value.to_string()
    }
}

impl fmt::Display for ArtifactId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(formatter)
    }
}

/// Stable identity of one durable work item.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub struct WorkItemId(Uuid);

impl WorkItemId {
    pub fn new_v7() -> Self {
        Self(Uuid::now_v7())
    }

    pub fn is_v7(self) -> bool {
        self.0.get_version_num() == 7
    }
}

impl TryFrom<Uuid> for WorkItemId {
    type Error = ValidationError;

    fn try_from(value: Uuid) -> Result<Self, Self::Error> {
        validate_uuid_v7("work item id", value).map(Self)
    }
}

impl FromStr for WorkItemId {
    type Err = ValidationError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        parse_uuid_v7("work item id", value).map(Self)
    }
}

impl TryFrom<String> for WorkItemId {
    type Error = ValidationError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        value.parse()
    }
}

impl From<WorkItemId> for String {
    fn from(value: WorkItemId) -> Self {
        value.to_string()
    }
}

impl fmt::Display for WorkItemId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(formatter)
    }
}

/// Stable identity of one harness run.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub struct RunId(Uuid);

impl RunId {
    pub fn new_v7() -> Self {
        Self(Uuid::now_v7())
    }

    pub fn is_v7(self) -> bool {
        self.0.get_version_num() == 7
    }
}

impl TryFrom<Uuid> for RunId {
    type Error = ValidationError;

    fn try_from(value: Uuid) -> Result<Self, Self::Error> {
        validate_uuid_v7("run id", value).map(Self)
    }
}

impl FromStr for RunId {
    type Err = ValidationError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        parse_uuid_v7("run id", value).map(Self)
    }
}

impl TryFrom<String> for RunId {
    type Error = ValidationError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        value.parse()
    }
}

impl From<RunId> for String {
    fn from(value: RunId) -> Self {
        value.to_string()
    }
}

impl fmt::Display for RunId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(formatter)
    }
}

/// Validated lowercase hexadecimal SHA-256 digest.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub struct ContentDigest(String);

impl ContentDigest {
    pub fn new(value: impl Into<String>) -> Result<Self, ValidationError> {
        let value = value.into();
        if value.len() != 64
            || !value
                .bytes()
                .all(|byte| byte.is_ascii_digit() || matches!(byte, b'a'..=b'f'))
        {
            return Err(ValidationError(
                "content digest must be 64 lowercase hexadecimal characters".to_owned(),
            ));
        }
        Ok(Self(value))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl FromStr for ContentDigest {
    type Err = ValidationError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        Self::new(value)
    }
}

impl TryFrom<String> for ContentDigest {
    type Error = ValidationError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        Self::new(value)
    }
}

impl From<ContentDigest> for String {
    fn from(value: ContentDigest) -> Self {
        value.0
    }
}

impl fmt::Display for ContentDigest {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(formatter)
    }
}

/// Product-owned artifact body categories.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ArtifactKind {
    Plan,
    Diff,
    File,
    Image,
    CommandLog,
    BrowserCapture,
    ProviderPayload,
}

/// Validated opaque description of where an artifact originated.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub struct ArtifactProvenance(String);

impl ArtifactProvenance {
    pub fn new(value: impl Into<String>) -> Result<Self, ValidationError> {
        let value = value.into();
        require_nonempty("artifact provenance", &value)?;
        Ok(Self(value))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl TryFrom<String> for ArtifactProvenance {
    type Error = ValidationError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        Self::new(value)
    }
}

impl From<ArtifactProvenance> for String {
    fn from(value: ArtifactProvenance) -> Self {
        value.0
    }
}

impl fmt::Display for ArtifactProvenance {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(formatter)
    }
}

/// Durable metadata for one immutable artifact body.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ArtifactRecord {
    pub id: ArtifactId,
    pub work_item_id: WorkItemId,
    pub run_id: Option<RunId>,
    pub kind: ArtifactKind,
    pub digest: ContentDigest,
    pub byte_len: u64,
    pub media_type: String,
    pub provenance: ArtifactProvenance,
}

macro_rules! uuid_id {
    ($name:ident, $doc:literal) => {
        #[doc = $doc]
        #[derive(
            Clone, Copy, Debug, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize,
        )]
        #[serde(try_from = "String", into = "String")]
        pub struct $name(Uuid);

        impl $name {
            pub fn new_v7() -> Self {
                Self(Uuid::now_v7())
            }

            pub fn is_v7(self) -> bool {
                self.0.get_version_num() == 7
            }
        }

        impl TryFrom<Uuid> for $name {
            type Error = ValidationError;

            fn try_from(value: Uuid) -> Result<Self, Self::Error> {
                validate_uuid_v7(stringify!($name), value).map(Self)
            }
        }

        impl FromStr for $name {
            type Err = ValidationError;

            fn from_str(value: &str) -> Result<Self, Self::Err> {
                parse_uuid_v7(stringify!($name), value).map(Self)
            }
        }

        impl TryFrom<String> for $name {
            type Error = ValidationError;

            fn try_from(value: String) -> Result<Self, Self::Error> {
                value.parse()
            }
        }

        impl From<$name> for String {
            fn from(value: $name) -> Self {
                value.to_string()
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                self.0.fmt(formatter)
            }
        }
    };
}

uuid_id!(ProjectId, "Stable identity of a YakShed project.");
uuid_id!(
    TimelineBatchId,
    "Stable identity of a timeline ingestion batch."
);
uuid_id!(TimelineItemId, "Stable identity of a timeline item.");
uuid_id!(ApprovalRequestId, "Stable identity of an approval request.");
uuid_id!(AuditEventId, "Stable identity of an audit event.");

/// UTC instant represented durably as Unix epoch milliseconds.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct UtcTimestamp(i64);

impl UtcTimestamp {
    pub const fn from_unix_millis(value: i64) -> Self {
        Self(value)
    }

    pub const fn unix_millis(self) -> i64 {
        self.0
    }
}

/// Monotonic cursor for one provider-owned source stream.
#[derive(Clone, Copy, Debug, Default, Eq, Ord, PartialEq, PartialOrd)]
pub struct StreamCursor(u64);

impl StreamCursor {
    pub const INITIAL: Self = Self(0);

    pub const fn new(value: u64) -> Self {
        Self(value)
    }

    pub const fn get(self) -> u64 {
        self.0
    }
}

/// Monotonic display ordinal within one run timeline.
#[derive(Clone, Copy, Debug, Default, Eq, Ord, PartialEq, PartialOrd)]
pub struct TimelineRevision(u64);

impl TimelineRevision {
    pub const INITIAL: Self = Self(0);

    pub const fn new(value: u64) -> Self {
        Self(value)
    }

    pub const fn get(self) -> u64 {
        self.0
    }
}

/// Monotonic revision of one durable application aggregate.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct DataRevision(u64);

impl DataRevision {
    pub const fn new(value: u64) -> Self {
        Self(value)
    }

    pub const fn get(self) -> u64 {
        self.0
    }
}

/// Opaque provider-owned identifier paired with its namespace.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct NamespacedProviderId {
    namespace: String,
    value: String,
}

impl NamespacedProviderId {
    pub fn new(
        namespace: impl Into<String>,
        value: impl Into<String>,
    ) -> Result<Self, ValidationError> {
        let namespace = namespace.into();
        let value = value.into();
        require_nonempty("provider namespace", &namespace)?;
        require_nonempty("provider id", &value)?;
        if namespace.len() > 128 || namespace.chars().any(char::is_control) {
            return Err(ValidationError("invalid provider namespace".to_owned()));
        }
        if value.len() > 4096 || value.chars().any(char::is_control) {
            return Err(ValidationError("invalid provider id".to_owned()));
        }
        Ok(Self { namespace, value })
    }

    pub fn namespace(&self) -> &str {
        &self.namespace
    }

    pub fn value(&self) -> &str {
        &self.value
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ProjectSnapshot {
    pub id: ProjectId,
    pub name: String,
    pub created_at: UtcTimestamp,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum WorkItemStatus {
    Ready,
    Archived,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct WorkItemSnapshot {
    pub id: WorkItemId,
    pub project_id: ProjectId,
    pub title: String,
    pub status: WorkItemStatus,
    pub parent_id: Option<WorkItemId>,
    pub revision: DataRevision,
    pub created_at: UtcTimestamp,
    pub updated_at: UtcTimestamp,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RunStatus {
    Running,
    Completed,
    Failed,
    Interrupted,
}

impl RunStatus {
    pub const fn can_transition_to(self, target: Self) -> bool {
        matches!(
            (self, target),
            (
                Self::Running,
                Self::Completed | Self::Failed | Self::Interrupted
            )
        )
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RunSnapshot {
    pub id: RunId,
    pub connection_id: ConnectionId,
    pub work_item_id: WorkItemId,
    pub status: RunStatus,
    pub provider_id: Option<NamespacedProviderId>,
    pub created_at: UtcTimestamp,
    pub ended_at: Option<UtcTimestamp>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TimelineItemSnapshot {
    pub id: TimelineItemId,
    pub connection_id: ConnectionId,
    pub run_id: RunId,
    pub revision: TimelineRevision,
    pub kind: String,
    pub body: String,
    pub provider_id: Option<NamespacedProviderId>,
    pub created_at: UtcTimestamp,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ApprovalDecision {
    Approved,
    Denied,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ApprovalStatus {
    Pending,
    Responding { decision: ApprovalDecision },
    Resolved { decision: ApprovalDecision },
    Voided { decision: Option<ApprovalDecision> },
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ApprovalSnapshot {
    pub id: ApprovalRequestId,
    pub connection_id: ConnectionId,
    pub run_id: RunId,
    pub provider_id: NamespacedProviderId,
    pub kind: String,
    pub summary: String,
    pub status: ApprovalStatus,
    pub requested_at: UtcTimestamp,
    pub response_started_at: Option<UtcTimestamp>,
    pub resolved_at: Option<UtcTimestamp>,
    pub voided_at: Option<UtcTimestamp>,
}

/// A configured harness/model-provider trust boundary.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Connection {
    pub id: ConnectionId,
    pub name: String,
    pub harness: String,
    pub model_provider: String,
    pub provider_state: ProviderStateRootId,
    pub credentials: Vec<CredentialBindingRecord>,
}

impl Connection {
    pub fn validate(&self) -> Result<(), ValidationError> {
        require_nonempty("connection name", &self.name)?;
        require_nonempty("connection harness", &self.harness)?;
        require_nonempty("connection model_provider", &self.model_provider)?;
        let mut slots = HashSet::new();
        for credential in &self.credentials {
            credential.validate()?;
            if !slots.insert(&credential.slot) {
                return Err(ValidationError(format!(
                    "duplicate credential slot: {}",
                    credential.slot
                )));
            }
        }
        Ok(())
    }
}

/// One credential slot and its non-secret binding.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CredentialBindingRecord {
    pub slot: CredentialSlot,
    pub binding: CredentialBinding,
    /// Adapter delivery metadata. `None` remains valid for schema-v1 entries written before
    /// delivery became canonical; new connection inputs should always supply it.
    pub delivery: Option<CredentialDelivery>,
}

impl CredentialBindingRecord {
    pub fn validate(&self) -> Result<(), ValidationError> {
        self.binding.validate()?;
        match (&self.binding, &self.delivery) {
            (_, None)
            | (CredentialBinding::Delegated { .. }, Some(CredentialDelivery::HarnessManaged))
            | (
                CredentialBinding::Secret { .. },
                Some(CredentialDelivery::ProcessEnvironment { .. }),
            ) => {}
            (CredentialBinding::Disabled, Some(_)) => {
                return Err(ValidationError(
                    "disabled credential cannot have delivery metadata".to_owned(),
                ));
            }
            _ => {
                return Err(ValidationError(
                    "credential source and delivery kind are incompatible".to_owned(),
                ));
            }
        }
        if let Some(delivery) = &self.delivery {
            delivery.validate()?;
        }
        Ok(())
    }
}

/// Closed non-secret mechanisms for delivering one resolved credential to a harness.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum CredentialDelivery {
    HarnessManaged,
    ProcessEnvironment { variable: String },
}

impl CredentialDelivery {
    pub fn process_environment(variable: impl Into<String>) -> Result<Self, ValidationError> {
        let delivery = Self::ProcessEnvironment {
            variable: variable.into(),
        };
        delivery.validate()?;
        Ok(delivery)
    }

    pub const fn kind(&self) -> &'static str {
        match self {
            Self::HarnessManaged => "harness_managed",
            Self::ProcessEnvironment { .. } => "process_environment",
        }
    }

    pub fn variable(&self) -> Option<&str> {
        match self {
            Self::HarnessManaged => None,
            Self::ProcessEnvironment { variable } => Some(variable),
        }
    }

    pub fn validate(&self) -> Result<(), ValidationError> {
        let Self::ProcessEnvironment { variable } = self else {
            return Ok(());
        };
        let mut bytes = variable.bytes();
        if !bytes
            .next()
            .is_some_and(|byte| byte == b'_' || byte.is_ascii_alphabetic())
            || !bytes.all(|byte| byte == b'_' || byte.is_ascii_alphanumeric())
        {
            return Err(ValidationError(
                "invalid process environment variable".to_owned(),
            ));
        }
        Ok(())
    }
}

/// Closed set of canonical credential-reference forms.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum CredentialBinding {
    Delegated { authority: String },
    Secret { reference: SecretReference },
    Disabled,
}

impl CredentialBinding {
    pub fn validate(&self) -> Result<(), ValidationError> {
        match self {
            Self::Delegated { authority } => {
                require_nonempty("delegated credential authority", authority)
            }
            Self::Secret { .. } | Self::Disabled => Ok(()),
        }
    }
}

/// Validated opaque credential-requirement identifier.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub struct CredentialSlot(String);

impl CredentialSlot {
    pub fn new(value: impl Into<String>) -> Result<Self, ValidationError> {
        let value = value.into();
        require_identifier("credential slot", &value)?;
        Ok(Self(value))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl TryFrom<String> for CredentialSlot {
    type Error = ValidationError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        Self::new(value)
    }
}

impl From<CredentialSlot> for String {
    fn from(value: CredentialSlot) -> Self {
        value.0
    }
}

impl fmt::Display for CredentialSlot {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(formatter)
    }
}

/// Correlates one application operation without exposing provider wire IDs.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct OperationId(String);

impl OperationId {
    pub fn new(value: impl Into<String>) -> Result<Self, ValidationError> {
        let value = value.into();
        require_identifier("operation id", &value)?;
        Ok(Self(value))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for OperationId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(formatter)
    }
}

/// Validated opaque identifier for one provider-owned state root.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub struct ProviderStateRootId(String);

impl ProviderStateRootId {
    pub fn new(value: impl Into<String>) -> Result<Self, ValidationError> {
        let value = value.into();
        require_identifier("provider state root id", &value)?;
        if value != value.to_ascii_lowercase() {
            return Err(ValidationError(
                "provider state root id must already be lowercase".to_owned(),
            ));
        }
        if matches!(value.as_str(), "." | "..") || value.ends_with(['.', ' ']) {
            return Err(ValidationError(
                "provider state root id must be one safe path component".to_owned(),
            ));
        }
        if is_windows_reserved_name(&value) {
            return Err(ValidationError(
                "provider state root id cannot be a Windows reserved device name".to_owned(),
            ));
        }
        Ok(Self(value))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl TryFrom<String> for ProviderStateRootId {
    type Error = ValidationError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        Self::new(value)
    }
}

impl From<ProviderStateRootId> for String {
    fn from(value: ProviderStateRootId) -> Self {
        value.0
    }
}

impl fmt::Display for ProviderStateRootId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(formatter)
    }
}

/// Stable identity of a configured secret backend.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub struct SecretBackendId(String);

impl SecretBackendId {
    pub fn new(value: impl Into<String>) -> Result<Self, ValidationError> {
        let value = value.into();
        require_identifier("secret backend id", &value)?;
        Ok(Self(value))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl TryFrom<String> for SecretBackendId {
    type Error = ValidationError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        Self::new(value)
    }
}

impl From<SecretBackendId> for String {
    fn from(value: SecretBackendId) -> Self {
        value.0
    }
}

impl fmt::Display for SecretBackendId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(formatter)
    }
}

/// Opaque backend-owned locator with only universal safety validation.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub struct SecretLocator(String);

impl SecretLocator {
    pub fn new(value: impl Into<String>) -> Result<Self, ValidationError> {
        let value = value.into();
        require_nonempty("secret locator", &value)?;
        if value.len() > 4096 {
            return Err(ValidationError(
                "secret locator exceeds 4096 bytes".to_owned(),
            ));
        }
        if value.chars().any(char::is_control) {
            return Err(ValidationError(
                "secret locator cannot contain control characters".to_owned(),
            ));
        }
        Ok(Self(value))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl TryFrom<String> for SecretLocator {
    type Error = ValidationError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        Self::new(value)
    }
}

impl From<SecretLocator> for String {
    fn from(value: SecretLocator) -> Self {
        value.0
    }
}

impl fmt::Display for SecretLocator {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(formatter)
    }
}

/// One normalized non-secret reference to backend-owned secret material.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct SecretReference {
    pub backend_id: SecretBackendId,
    pub locator: SecretLocator,
}

/// Configuration for a secret-reference backend, never a secret value.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SecretBackend {
    pub id: SecretBackendId,
    pub settings: SecretBackendSettings,
}

impl SecretBackend {
    pub fn validate(&self) -> Result<(), ValidationError> {
        self.settings.validate()
    }

    pub const fn kind(&self) -> &'static str {
        self.settings.kind()
    }
}

/// Closed, kind-correlated settings for configured secret backends.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum SecretBackendSettings {
    LocalOs,
    OnePasswordCli { account: Option<String> },
    Environment,
    Memory,
    LocalFile { path: String },
}

impl SecretBackendSettings {
    pub fn from_config_parts(
        kind: &str,
        account: Option<String>,
        path: Option<String>,
    ) -> Result<Self, ValidationError> {
        let settings = match (kind, account, path) {
            ("local-os", None, None) => Self::LocalOs,
            ("onepassword-cli", account, None) => Self::OnePasswordCli { account },
            ("environment", None, None) => Self::Environment,
            ("memory", None, None) => Self::Memory,
            ("local-file", None, Some(path)) => Self::LocalFile { path },
            ("local-file", None, None) => {
                return Err(ValidationError(
                    "local-file secret backend requires a path".to_owned(),
                ));
            }
            _ => {
                return Err(ValidationError(format!(
                    "unsupported or inconsistent secret backend settings for kind: {kind}"
                )));
            }
        };
        settings.validate()?;
        Ok(settings)
    }

    pub const fn kind(&self) -> &'static str {
        match self {
            Self::LocalOs => "local-os",
            Self::OnePasswordCli { .. } => "onepassword-cli",
            Self::Environment => "environment",
            Self::Memory => "memory",
            Self::LocalFile { .. } => "local-file",
        }
    }

    fn validate(&self) -> Result<(), ValidationError> {
        match self {
            Self::OnePasswordCli {
                account: Some(account),
            } => require_nonempty("secret backend account", account),
            Self::LocalFile { path } => require_nonempty("secret backend path", path),
            _ => Ok(()),
        }
    }
}

fn require_nonempty(field: &'static str, value: &str) -> Result<(), ValidationError> {
    if value.trim().is_empty() {
        Err(ValidationError(format!("{field} cannot be empty")))
    } else {
        Ok(())
    }
}

fn parse_uuid_v7(field: &'static str, value: &str) -> Result<Uuid, ValidationError> {
    let uuid = Uuid::parse_str(value)
        .map_err(|error| ValidationError(format!("invalid {field}: {error}")))?;
    validate_uuid_v7(field, uuid)
}

fn validate_uuid_v7(field: &'static str, uuid: Uuid) -> Result<Uuid, ValidationError> {
    if uuid.get_version_num() != 7 || uuid.get_variant() != uuid::Variant::RFC4122 {
        return Err(ValidationError(format!("{field} must be a UUIDv7")));
    }
    Ok(uuid)
}

fn require_identifier(field: &'static str, value: &str) -> Result<(), ValidationError> {
    require_nonempty(field, value)?;
    if value
        .chars()
        .all(|character| character.is_ascii_alphanumeric() || matches!(character, '-' | '_' | '.'))
    {
        Ok(())
    } else {
        Err(ValidationError(format!(
            "{field} may contain only ASCII letters, digits, '.', '-' and '_'"
        )))
    }
}

fn is_windows_reserved_name(value: &str) -> bool {
    let stem = value.split('.').next().unwrap_or(value);
    matches!(stem, "con" | "prn" | "aux" | "nul")
        || ((stem.starts_with("com") || stem.starts_with("lpt"))
            && stem.len() == 4
            && matches!(stem.as_bytes()[3], b'1'..=b'9'))
}

/// A violated domain value invariant.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ValidationError(String);

impl fmt::Display for ValidationError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(formatter)
    }
}

impl Error for ValidationError {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn app_owned_ids_are_uuid_v7() {
        let parsed = Uuid::parse_str(&WorkItemId::new_v7().to_string()).unwrap();
        assert_eq!(parsed.get_version_num(), 7);
    }

    #[test]
    fn app_owned_id_deserialization_rejects_non_v7_uuid() {
        use serde::Deserialize as _;
        use serde::de::value::{Error, StrDeserializer};

        let value = "550e8400-e29b-41d4-a716-446655440000";
        assert!(ConnectionId::deserialize(StrDeserializer::<Error>::new(value)).is_err());
        assert!(ProjectId::deserialize(StrDeserializer::<Error>::new(value)).is_err());
        assert!(TimelineBatchId::deserialize(StrDeserializer::<Error>::new(value)).is_err());
        assert!(ApprovalRequestId::deserialize(StrDeserializer::<Error>::new(value)).is_err());
    }

    #[test]
    fn run_state_transition_matrix_is_closed() {
        for terminal in [
            RunStatus::Completed,
            RunStatus::Failed,
            RunStatus::Interrupted,
        ] {
            assert!(RunStatus::Running.can_transition_to(terminal));
            assert!(!terminal.can_transition_to(RunStatus::Running));
            assert!(!terminal.can_transition_to(terminal));
        }
        assert!(!RunStatus::Running.can_transition_to(RunStatus::Running));
    }

    #[test]
    fn credential_binding_variants_enforce_their_reference_fields() {
        assert!(CredentialBinding::Disabled.validate().is_ok());
        assert!(
            CredentialBinding::Delegated {
                authority: String::new()
            }
            .validate()
            .is_err()
        );
        assert!(SecretLocator::new(String::new()).is_err());
    }

    #[test]
    fn credential_delivery_is_closed_and_validates_environment_variables() {
        assert!(CredentialDelivery::process_environment("ANTHROPIC_API_KEY").is_ok());
        assert!(CredentialDelivery::process_environment("NOT-AN-ENV-VAR").is_err());
        assert_eq!(CredentialDelivery::HarnessManaged.kind(), "harness_managed");
    }

    #[test]
    fn connection_rejects_conflicting_bindings_for_the_same_slot() {
        let connection = Connection {
            id: "0193f26e-7a72-7d42-bf77-0de14c4cc222".parse().unwrap(),
            name: "Work".to_owned(),
            harness: "codex".to_owned(),
            model_provider: "openai".to_owned(),
            provider_state: ProviderStateRootId::new("work-codex").unwrap(),
            credentials: vec![
                CredentialBindingRecord {
                    slot: CredentialSlot::new("codex.account").unwrap(),
                    binding: CredentialBinding::Delegated {
                        authority: "codex-app-server".to_owned(),
                    },
                    delivery: Some(CredentialDelivery::HarnessManaged),
                },
                CredentialBindingRecord {
                    slot: CredentialSlot::new("codex.account").unwrap(),
                    binding: CredentialBinding::Secret {
                        reference: SecretReference {
                            backend_id: SecretBackendId::new("local-os").unwrap(),
                            locator: SecretLocator::new("connection/work/codex_account").unwrap(),
                        },
                    },
                    delivery: Some(
                        CredentialDelivery::process_environment("CODEX_API_KEY").unwrap(),
                    ),
                },
            ],
        };

        assert!(connection.validate().is_err());
    }

    #[test]
    fn provider_state_root_id_rejects_empty_and_path_values() {
        assert!(ProviderStateRootId::new("").is_err());
        assert!(ProviderStateRootId::new("../shared-codex").is_err());
        assert!(ProviderStateRootId::new("work-codex").is_ok());
        assert!(ProviderStateRootId::new("WORK-CODEX").is_err());
        assert!(ProviderStateRootId::new("work.").is_err());
        assert!(ProviderStateRootId::new("work ").is_err());
        for reserved in [
            "CON", "con.txt", "prn", "aux.log", "nul", "com1", "com9.txt", "lpt1", "lpt9.log",
        ] {
            assert!(ProviderStateRootId::new(reserved).is_err(), "{reserved}");
        }
        assert!(ProviderStateRootId::new("com0").is_ok());
        assert!(ProviderStateRootId::new("com10").is_ok());
    }

    #[test]
    fn artifact_values_enforce_uuid_and_digest_shapes() {
        assert!(
            "0193f26e-7a72-7d42-bf77-0de14c4cc220"
                .parse::<ArtifactId>()
                .is_ok()
        );
        assert!(
            "550e8400-e29b-41d4-a716-446655440000"
                .parse::<ArtifactId>()
                .is_err()
        );
        assert!(
            "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
                .parse::<ContentDigest>()
                .is_ok()
        );
        assert!(ArtifactProvenance::new(" ").is_err());
    }
}
