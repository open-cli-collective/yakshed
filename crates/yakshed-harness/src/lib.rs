//! Provider-neutral harness contract and normalized event model.
//!
//! Adapters retain provider-owned session/run identities and a redacted copy of every native
//! event payload. Event delivery uses a fixed bounded channel; producers await capacity rather
//! than dropping events or growing memory without bound.
//!
//! The architecture sketch's `subscribe` is fallible here because the bounded receiver has one
//! owner. `ProviderEventStream::recv` is intentionally smaller than a `futures::Stream` dependency;
//! the future Codex transport can feed the same bounded sender.

use std::{fmt, str::FromStr};

use async_trait::async_trait;
use thiserror::Error;
use tokio::sync::mpsc;
use yakshed_domain::{ApprovalDecision, ConnectionId, CredentialSlot};

pub const EVENT_BUFFER_CAPACITY: usize = 64;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct HarnessDescriptor {
    pub id: String,
    pub name: String,
    pub version: String,
}

/// Adapter-owned delivery mechanism for one canonical credential slot.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum HarnessCredentialDelivery {
    HarnessManaged,
    Delegated,
    ProcessEnvironment { variable: String },
}

/// One credential slot and the delivery mechanism fixed by its harness adapter.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct HarnessCredentialRequirement {
    pub slot: CredentialSlot,
    pub delivery: HarnessCredentialDelivery,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum HarnessAccountStatus {
    NotAuthenticated,
    LoginInProgress { login_id: String, auth_url: String },
    Authenticated { email: Option<String>, plan: String },
    Unknown,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct HarnessCapabilities {
    pub persistent_sessions: bool,
    pub session_listing: bool,
    pub native_fork: bool,
    pub session_archive: bool,
    pub mid_run_steering: bool,
    pub client_approvals: bool,
    pub user_input_requests: bool,
    pub structured_file_changes: bool,
    pub command_output_streaming: bool,
    pub native_subagent_lineage: bool,
    pub images: bool,
    pub skills: bool,
    pub mcp: bool,
    pub account_ui: bool,
    pub model_discovery: bool,
}

#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct RuntimeHandle(String);

impl RuntimeHandle {
    pub fn new(value: impl Into<String>) -> Result<Self, HarnessError> {
        validate_opaque_id("runtime", value.into()).map(Self)
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for RuntimeHandle {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(formatter)
    }
}

macro_rules! opaque_provider_id {
    ($name:ident, $label:literal) => {
        #[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
        pub struct $name(String);

        impl $name {
            pub fn new(value: impl Into<String>) -> Result<Self, HarnessError> {
                validate_opaque_id($label, value.into()).map(Self)
            }

            pub fn as_str(&self) -> &str {
                &self.0
            }
        }

        impl FromStr for $name {
            type Err = HarnessError;

            fn from_str(value: &str) -> Result<Self, Self::Err> {
                Self::new(value)
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                self.0.fmt(formatter)
            }
        }
    };
}

opaque_provider_id!(ProviderSessionId, "provider session id");
opaque_provider_id!(ProviderRunId, "provider run id");
opaque_provider_id!(ProviderRequestId, "provider request id");
opaque_provider_id!(ProviderCommandId, "provider command id");
opaque_provider_id!(SessionPageCursor, "session page cursor");

fn validate_opaque_id(label: &str, value: String) -> Result<String, HarnessError> {
    if value.trim().is_empty() || value.len() > 4096 || value.chars().any(char::is_control) {
        Err(HarnessError::InvalidInput(format!("invalid {label}")))
    } else {
        Ok(value)
    }
}

/// Runtime- and session-scoped provider run identity.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct ProviderRunHandle {
    runtime: RuntimeHandle,
    session_id: ProviderSessionId,
    native_id: ProviderRunId,
}

impl ProviderRunHandle {
    pub fn new(
        runtime: RuntimeHandle,
        session_id: ProviderSessionId,
        native_id: ProviderRunId,
    ) -> Self {
        Self {
            runtime,
            session_id,
            native_id,
        }
    }

    pub fn runtime(&self) -> &RuntimeHandle {
        &self.runtime
    }

    pub fn session_id(&self) -> &ProviderSessionId {
        &self.session_id
    }

    pub fn native_id(&self) -> &ProviderRunId {
        &self.native_id
    }
}

impl fmt::Display for ProviderRunHandle {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "{}/{}/{}",
            self.runtime, self.session_id, self.native_id
        )
    }
}

/// Provider request identity scoped through its owning runtime/session/run.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct ProviderRequestHandle {
    run: ProviderRunHandle,
    native_id: ProviderRequestId,
}

#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct ProviderCommandHandle {
    run: ProviderRunHandle,
    native_id: ProviderCommandId,
}

impl ProviderCommandHandle {
    pub fn new(run: ProviderRunHandle, native_id: ProviderCommandId) -> Self {
        Self { run, native_id }
    }

    pub fn run(&self) -> &ProviderRunHandle {
        &self.run
    }

    pub fn native_id(&self) -> &ProviderCommandId {
        &self.native_id
    }
}

impl fmt::Display for ProviderCommandHandle {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{}/{}", self.run, self.native_id)
    }
}

impl ProviderRequestHandle {
    pub fn new(run: ProviderRunHandle, native_id: ProviderRequestId) -> Self {
        Self { run, native_id }
    }

    pub fn run(&self) -> &ProviderRunHandle {
        &self.run
    }

    pub fn native_id(&self) -> &ProviderRequestId {
        &self.native_id
    }
}

impl fmt::Display for ProviderRequestHandle {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{}/{}", self.run, self.native_id)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
/// Session settings that are independent of connection ownership.
///
/// The runtime handle is the sole authority for the session's connection binding.
pub struct StartSessionSpec {
    pub working_directory: RuntimePath,
    pub title: String,
}

/// Provider/runtime-native path. Interpretation belongs to the selected runtime adapter.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct RuntimePath(String);

impl RuntimePath {
    pub fn new(value: impl Into<String>) -> Result<Self, HarnessError> {
        let value = value.into();
        if value.is_empty() || value.len() > 32_768 || value.contains('\0') {
            Err(HarnessError::InvalidInput(
                "invalid runtime path".to_owned(),
            ))
        } else {
            Ok(Self(value))
        }
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SessionQuery {
    pub after: Option<SessionPageCursor>,
    pub limit: u32,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Page<T> {
    pub items: Vec<T>,
    pub next: Option<SessionPageCursor>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ProviderSessionSummary {
    pub id: ProviderSessionId,
    pub title: String,
    pub connection_id: ConnectionId,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ProviderSession {
    pub id: ProviderSessionId,
    pub runtime: RuntimeHandle,
    pub connection_id: ConnectionId,
    pub working_directory: RuntimePath,
    pub title: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct HarnessInput(String);

impl HarnessInput {
    pub fn new(value: impl Into<String>) -> Result<Self, HarnessError> {
        let value = value.into();
        if value.trim().is_empty() {
            Err(HarnessError::InvalidInput(
                "harness input cannot be empty".to_owned(),
            ))
        } else {
            Ok(Self(value))
        }
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct RunOptions {
    pub model: Option<String>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ProviderResponse {
    Approval(ApprovalDecision),
    UserInput(String),
}

/// Exact provider-native payload after adapter-level credential redaction.
#[derive(Clone, Eq, PartialEq)]
pub struct NativePayload(String);

impl NativePayload {
    /// Constructs a payload only after the adapter has removed credential material.
    pub fn sanitized(value: impl Into<String>) -> Self {
        Self(value.into())
    }

    /// Returns the retained post-redaction bytes for rebuildable provider projections.
    pub fn sanitized_raw(&self) -> &str {
        &self.0
    }
}

impl fmt::Debug for NativePayload {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("NativePayload([redacted])")
    }
}

impl fmt::Display for NativePayload {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("[native payload redacted]")
    }
}

/// Provider diagnostic after adapter-level credential redaction.
#[derive(Clone, Eq, PartialEq)]
pub struct SanitizedDiagnostic(String);

impl SanitizedDiagnostic {
    /// Constructs a diagnostic only after the adapter has removed credential material.
    pub fn sanitized(value: impl Into<String>) -> Self {
        Self(value.into())
    }

    pub fn sanitized_text(&self) -> &str {
        &self.0
    }
}

impl fmt::Debug for SanitizedDiagnostic {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("SanitizedDiagnostic([redacted])")
    }
}

impl fmt::Display for SanitizedDiagnostic {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(formatter)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum HarnessRunTerminal {
    Completed,
    Failed { diagnostic: SanitizedDiagnostic },
    Interrupted,
    Crashed { diagnostic: SanitizedDiagnostic },
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum HarnessEvent {
    RunAccepted {
        run: ProviderRunHandle,
        native: NativePayload,
    },
    /// Transient streaming text; reducers batch these rather than persisting each chunk.
    MessageDelta {
        run: ProviderRunHandle,
        chunk: String,
        native: NativePayload,
    },
    /// Authoritative completed message suitable for finalizing normalized state.
    MessageCompleted {
        run: ProviderRunHandle,
        text: String,
        native: NativePayload,
    },
    ApprovalRequested {
        request: ProviderRequestHandle,
        summary: String,
        native: NativePayload,
    },
    UserInputRequested {
        request: ProviderRequestHandle,
        prompt: String,
        native: NativePayload,
    },
    FileMutation {
        run: ProviderRunHandle,
        path: String,
        summary: String,
        native: NativePayload,
    },
    /// Transient command output chunk; consumers append but do not finalize from this event.
    CommandOutputDelta {
        run: ProviderRunHandle,
        command: ProviderCommandHandle,
        command_text: String,
        chunk: String,
        native: NativePayload,
    },
    /// Authoritative completed command output; consumers replace/finalize from this event.
    CommandOutputCompleted {
        run: ProviderRunHandle,
        command: ProviderCommandHandle,
        command_text: String,
        output: String,
        native: NativePayload,
    },
    RunTerminal {
        run: ProviderRunHandle,
        state: HarnessRunTerminal,
        native: NativePayload,
    },
    Unknown {
        run: Option<ProviderRunHandle>,
        item_type: String,
        native: NativePayload,
    },
    MalformedNativePayload {
        run: Option<ProviderRunHandle>,
        item_type: String,
        native: NativePayload,
    },
}

impl HarnessEvent {
    pub fn native_payload(&self) -> &NativePayload {
        match self {
            Self::RunAccepted { native, .. }
            | Self::MessageDelta { native, .. }
            | Self::MessageCompleted { native, .. }
            | Self::ApprovalRequested { native, .. }
            | Self::UserInputRequested { native, .. }
            | Self::FileMutation { native, .. }
            | Self::CommandOutputDelta { native, .. }
            | Self::CommandOutputCompleted { native, .. }
            | Self::RunTerminal { native, .. }
            | Self::Unknown { native, .. }
            | Self::MalformedNativePayload { native, .. } => native,
        }
    }

    fn kind(&self) -> &'static str {
        match self {
            Self::RunAccepted { .. } => "run_accepted",
            Self::MessageDelta { .. } => "message_delta",
            Self::MessageCompleted { .. } => "message_completed",
            Self::ApprovalRequested { .. } => "approval_requested",
            Self::UserInputRequested { .. } => "user_input_requested",
            Self::FileMutation { .. } => "file_mutation",
            Self::CommandOutputDelta { .. } => "command_output_delta",
            Self::CommandOutputCompleted { .. } => "command_output_completed",
            Self::RunTerminal { .. } => "run_terminal",
            Self::Unknown { .. } => "unknown",
            Self::MalformedNativePayload { .. } => "malformed_native_payload",
        }
    }
}

impl fmt::Display for HarnessEvent {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.kind())
    }
}

#[derive(Debug, Error, Eq, PartialEq)]
pub enum HarnessError {
    #[error("invalid input: {0}")]
    InvalidInput(String),
    #[error("{entity} not found: {id}")]
    NotFound { entity: &'static str, id: String },
    #[error("conflict: {0}")]
    Conflict(String),
    #[error("operation is unsupported: {0}")]
    Unsupported(&'static str),
    #[error("runtime is overloaded")]
    Overloaded,
    #[error("runtime is disconnected")]
    Disconnected,
    #[error("provider account is not authenticated")]
    NotAuthenticated,
    #[error("outcome is unknown for mutating operation: {operation}")]
    OutcomeUnknown { operation: &'static str },
    #[error("harness event stream is closed")]
    Closed,
    #[error("provider protocol failure: {diagnostic}")]
    Protocol { diagnostic: SanitizedDiagnostic },
    #[error("provider transport failure: {diagnostic}")]
    Transport { diagnostic: SanitizedDiagnostic },
    #[error("provider runtime failure: {diagnostic}")]
    Runtime { diagnostic: SanitizedDiagnostic },
}

#[derive(Clone)]
pub struct HarnessEventSender(mpsc::Sender<HarnessEvent>);

pub struct HarnessEventPermit(mpsc::OwnedPermit<HarnessEvent>);

impl HarnessEventSender {
    pub async fn send(&self, event: HarnessEvent) -> Result<(), HarnessError> {
        self.0.send(event).await.map_err(|_| HarnessError::Closed)
    }

    pub async fn reserve(&self) -> Result<HarnessEventPermit, HarnessError> {
        self.0
            .clone()
            .reserve_owned()
            .await
            .map(HarnessEventPermit)
            .map_err(|_| HarnessError::Closed)
    }
}

impl HarnessEventPermit {
    pub fn send(self, event: HarnessEvent) {
        self.0.send(event);
    }
}

pub struct ProviderEventStream(mpsc::Receiver<HarnessEvent>);

impl ProviderEventStream {
    pub async fn recv(&mut self) -> Option<HarnessEvent> {
        self.0.recv().await
    }
}

pub fn event_channel() -> (HarnessEventSender, ProviderEventStream) {
    let (sender, receiver) = mpsc::channel(EVENT_BUFFER_CAPACITY);
    (HarnessEventSender(sender), ProviderEventStream(receiver))
}

#[async_trait]
pub trait HarnessAdapter: Send + Sync {
    fn descriptor(&self) -> HarnessDescriptor;

    fn credential_requirements(&self) -> Vec<HarnessCredentialRequirement>;

    async fn account_status(
        &self,
        _runtime: &RuntimeHandle,
    ) -> Result<HarnessAccountStatus, HarnessError> {
        Err(HarnessError::Unsupported("account status"))
    }

    async fn account_login_start(
        &self,
        _runtime: &RuntimeHandle,
    ) -> Result<HarnessAccountStatus, HarnessError> {
        Err(HarnessError::Unsupported("account login"))
    }

    async fn account_logout(&self, _runtime: &RuntimeHandle) -> Result<(), HarnessError> {
        Err(HarnessError::Unsupported("account logout"))
    }

    async fn capabilities(
        &self,
        runtime: &RuntimeHandle,
    ) -> Result<HarnessCapabilities, HarnessError>;

    async fn list_sessions(
        &self,
        runtime: &RuntimeHandle,
        query: SessionQuery,
    ) -> Result<Page<ProviderSessionSummary>, HarnessError>;

    async fn start_session(
        &self,
        runtime: &RuntimeHandle,
        spec: StartSessionSpec,
    ) -> Result<ProviderSession, HarnessError>;

    async fn resume_session(
        &self,
        runtime: &RuntimeHandle,
        id: &ProviderSessionId,
    ) -> Result<ProviderSession, HarnessError>;

    async fn start_run(
        &self,
        session: &ProviderSession,
        input: HarnessInput,
        options: RunOptions,
    ) -> Result<ProviderRunHandle, HarnessError>;

    async fn steer(&self, run: &ProviderRunHandle, input: HarnessInput)
    -> Result<(), HarnessError>;

    async fn interrupt(&self, run: &ProviderRunHandle) -> Result<(), HarnessError>;

    async fn respond_to_request(
        &self,
        request: ProviderRequestHandle,
        response: ProviderResponse,
    ) -> Result<(), HarnessError>;

    fn subscribe(&self) -> Result<ProviderEventStream, HarnessError>;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn native_payload_debug_output_is_redacted() {
        let retained = "sanitized-native-detail";
        let event = HarnessEvent::Unknown {
            run: None,
            item_type: "future".to_owned(),
            native: NativePayload::sanitized(retained),
        };

        assert!(!format!("{event:?}").contains(retained));
        assert!(!format!("{event}").contains(retained));
    }

    #[test]
    fn provider_ids_are_opaque_but_bounded() {
        assert!(ProviderRunId::new("provider/native:id").is_ok());
        assert!(ProviderRunId::new("").is_err());
        assert!(ProviderRunId::new("bad\nvalue").is_err());
    }

    #[test]
    fn runtime_paths_do_not_assume_host_path_semantics() {
        assert!(RuntimePath::new(r"C:\workspace\project").is_ok());
        assert!(RuntimePath::new("ssh://host/workspace").is_ok());
        assert!(RuntimePath::new("").is_err());
    }
}
