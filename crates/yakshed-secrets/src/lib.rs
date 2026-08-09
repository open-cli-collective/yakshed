//! Secret contracts, an isolated memory backend, and credential brokering.

mod broker;
mod memory;

use std::{error::Error, fmt, sync::Arc};

use async_trait::async_trait;
use secrecy::SecretString;
use time::OffsetDateTime;
use yakshed_domain::{ConnectionId, CredentialSlot, OperationId};

pub use broker::{
    BrokerCancellation, ChildProcessEnvironment, CredentialBroker, CredentialResolution,
    CredentialStatus, shape_process_environment,
};
pub use memory::{MemorySecretBackend, MemorySecretFault};

#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct SecretBackendId(String);

impl SecretBackendId {
    pub fn new(value: impl Into<String>) -> Result<Self, SecretError> {
        let value = value.into();
        if value.is_empty()
            || !value
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.'))
        {
            return Err(SecretError::ProtocolViolation {
                backend: Self("invalid".into()),
                reason: "backend IDs use letters, digits, '.', '-', and '_'".into(),
            });
        }
        Ok(Self(value))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for SecretBackendId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct SecretLocator(String);

impl SecretLocator {
    pub fn new(value: impl Into<String>) -> Result<Self, &'static str> {
        let value = value.into();
        if value.is_empty() {
            return Err("locator must not be empty");
        }
        if value.len() > 4096 {
            return Err("locator exceeds 4096 bytes");
        }
        if value.chars().any(char::is_control) {
            return Err("locator must not contain control characters");
        }
        Ok(Self(value))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for SecretLocator {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct SecretReference {
    pub backend_id: SecretBackendId,
    pub locator: SecretLocator,
}

impl SecretReference {
    pub fn summary(&self) -> SecretReferenceSummary {
        SecretReferenceSummary {
            backend: self.backend_id.clone(),
            locator: self.locator.clone(),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SecretReferenceSummary {
    pub backend: SecretBackendId,
    pub locator: SecretLocator,
}

impl fmt::Display for SecretReferenceSummary {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{}:{}", self.backend, self.locator)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SecretBackendDescriptor {
    pub id: SecretBackendId,
    pub kind: String,
    pub writable: bool,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SecretBackendStatus {
    Available,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PutSecretOutcome {
    Written,
    Replaced,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct PutSecretOptions {
    pub overwrite: bool,
}

impl PutSecretOptions {
    pub const NO_OVERWRITE: Self = Self { overwrite: false };
    pub const OVERWRITE: Self = Self { overwrite: true };
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DeleteSecretOutcome {
    Deleted,
    NotFound,
}

#[async_trait]
pub trait SecretResolver: Send + Sync {
    fn descriptor(&self) -> SecretBackendDescriptor;

    async fn probe(&self) -> Result<SecretBackendStatus, SecretError>;

    async fn resolve(
        &self,
        locator: &SecretLocator,
        context: &SecretAccessContext,
    ) -> Result<ResolvedSecret, SecretError>;
}

#[async_trait]
pub trait SecretAdministrator: Send + Sync {
    async fn put(
        &self,
        locator: &SecretLocator,
        value: &SecretString,
        options: PutSecretOptions,
    ) -> Result<PutSecretOutcome, SecretError>;

    async fn delete(&self, locator: &SecretLocator) -> Result<DeleteSecretOutcome, SecretError>;
}

pub struct SecretBackendHandle {
    pub resolver: Arc<dyn SecretResolver>,
    pub administrator: Option<Arc<dyn SecretAdministrator>>,
}

/// A deliberately non-`Clone`, non-`Debug`, non-serializable secret lease.
///
/// ```compile_fail
/// # use secrecy::SecretString;
/// # use yakshed_secrets::{ResolvedSecret, ResolvedSecretSource, SecretBackendId};
/// # let source = ResolvedSecretSource { backend: SecretBackendId::new("memory").unwrap() };
/// let lease = ResolvedSecret::new(SecretString::from("secret".to_owned()), source, None);
/// let _ = lease.clone();
/// ```
///
/// ```compile_fail
/// # use secrecy::SecretString;
/// # use yakshed_secrets::{ResolvedSecret, ResolvedSecretSource, SecretBackendId};
/// # let source = ResolvedSecretSource { backend: SecretBackendId::new("memory").unwrap() };
/// let lease = ResolvedSecret::new(SecretString::from("secret".to_owned()), source, None);
/// let _ = format!("{lease:?}");
/// ```
///
/// ```compile_fail
/// # use secrecy::SecretString;
/// # use yakshed_secrets::{ResolvedSecret, ResolvedSecretSource, SecretBackendId};
/// # let source = ResolvedSecretSource { backend: SecretBackendId::new("memory").unwrap() };
/// let lease = ResolvedSecret::new(SecretString::from("secret".to_owned()), source, None);
/// let _ = serde_json::to_string(&lease);
/// ```
pub struct ResolvedSecret {
    value: SecretString,
    pub source: ResolvedSecretSource,
    pub expires_at: Option<OffsetDateTime>,
}

impl ResolvedSecret {
    pub fn new(
        value: SecretString,
        source: ResolvedSecretSource,
        expires_at: Option<OffsetDateTime>,
    ) -> Self {
        Self {
            value,
            source,
            expires_at,
        }
    }

    pub fn expose<R>(&self, use_value: impl FnOnce(&str) -> R) -> R {
        use secrecy::ExposeSecret;
        use_value(self.value.expose_secret())
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ResolvedSecretSource {
    pub backend: SecretBackendId,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SecretAccessContext {
    pub connection_id: ConnectionId,
    pub slot: CredentialSlot,
    pub purpose: SecretAccessPurpose,
    pub request_id: OperationId,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SecretAccessPurpose {
    StartHarness,
    RefreshHarness,
    ProviderHttpRequest,
    GitHostRequest,
    McpConnection,
    ValidateCredential,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum CredentialBinding {
    Delegated { authority: DelegatedAuthority },
    Secret { reference: SecretReference },
    Disabled,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DelegatedAuthority(pub String);

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CredentialBindingRecord {
    pub connection_id: ConnectionId,
    pub slot: CredentialSlot,
    pub binding: CredentialBinding,
    pub delivery: CredentialDelivery,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum CredentialDelivery {
    HarnessManaged,
    ProcessEnvironment { variable: String },
    HttpBearer,
    ProviderNative,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AuthenticationAction {
    SignIn,
    Unlock,
    Reauthenticate,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SecretOperation {
    Probe,
    Resolve,
    Put,
    Delete,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum InvalidBindingReason {
    UnknownConnection,
    UnknownSlot,
    Disabled,
    NotSecretBacked,
}

pub enum SecretError {
    NotFound {
        reference: SecretReferenceSummary,
    },
    AlreadyExists {
        reference: SecretReferenceSummary,
    },
    BackendUnavailable {
        backend: SecretBackendId,
        remediation: Option<String>,
    },
    LockedOrDenied {
        backend: SecretBackendId,
        remediation: Option<String>,
    },
    AuthenticationRequired {
        backend: SecretBackendId,
        action: AuthenticationAction,
    },
    InvalidLocator {
        backend: SecretBackendId,
        reason: String,
    },
    UnsupportedOperation {
        backend: SecretBackendId,
        operation: SecretOperation,
    },
    TimedOut {
        backend: SecretBackendId,
    },
    Cancelled {
        backend: SecretBackendId,
    },
    ProtocolViolation {
        backend: SecretBackendId,
        reason: String,
    },
    BackendFailure {
        backend: SecretBackendId,
        redacted_message: String,
    },
    InvalidBinding {
        connection_id: ConnectionId,
        slot: CredentialSlot,
        reason: InvalidBindingReason,
    },
}

impl fmt::Debug for SecretError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(self, formatter)
    }
}

impl fmt::Display for SecretError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NotFound { reference } => write!(formatter, "secret not found: {reference}"),
            Self::AlreadyExists { reference } => {
                write!(formatter, "secret already exists: {reference}")
            }
            Self::BackendUnavailable { backend, .. } => {
                write!(formatter, "secret backend unavailable: {backend}")
            }
            Self::LockedOrDenied { backend, .. } => {
                write!(formatter, "secret backend locked or denied: {backend}")
            }
            Self::AuthenticationRequired { backend, .. } => {
                write!(
                    formatter,
                    "secret backend authentication required: {backend}"
                )
            }
            Self::InvalidLocator { backend, .. } => {
                write!(formatter, "invalid secret locator for backend: {backend}")
            }
            Self::UnsupportedOperation { backend, operation } => {
                write!(
                    formatter,
                    "unsupported secret operation {operation:?}: {backend}"
                )
            }
            Self::TimedOut { backend } => {
                write!(formatter, "secret operation timed out: {backend}")
            }
            Self::Cancelled { backend } => {
                write!(formatter, "secret operation cancelled: {backend}")
            }
            Self::ProtocolViolation { backend, .. } => {
                write!(formatter, "secret backend protocol violation: {backend}")
            }
            Self::BackendFailure { backend, .. } => {
                write!(formatter, "secret backend failure: {backend}")
            }
            Self::InvalidBinding {
                connection_id,
                slot,
                reason,
            } => write!(
                formatter,
                "invalid credential binding {connection_id}/{slot}: {reason:?}"
            ),
        }
    }
}

impl Error for SecretError {}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SecretAuditEvent {
    pub connection_id: ConnectionId,
    pub slot: CredentialSlot,
    pub purpose: SecretAccessPurpose,
    pub request_id: OperationId,
    pub backend: Option<SecretBackendId>,
    pub operation: SecretOperation,
    pub outcome: SecretAuditOutcome,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SecretAuditOutcome {
    Succeeded,
    Delegated,
    NotFound,
    Rejected,
    Failed,
    TimedOut,
    Cancelled,
}

pub trait SecretAuditSink: Send + Sync {
    fn record(&self, event: SecretAuditEvent);
}

#[derive(Default)]
pub struct NoopSecretAuditSink;

impl SecretAuditSink for NoopSecretAuditSink {
    fn record(&self, _event: SecretAuditEvent) {}
}
