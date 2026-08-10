//! Provider-neutral harness contract and normalized event model.
//!
//! Adapters retain provider-owned session/run identities and a redacted copy of every native
//! event payload. Event delivery uses a fixed bounded channel; producers await capacity rather
//! than dropping events or growing memory without bound.
//!
//! The architecture sketch's `subscribe` is fallible here because the bounded receiver has one
//! owner. `ProviderEventStream::recv` is intentionally smaller than a `futures::Stream` dependency;
//! the future Codex transport can feed the same bounded sender.

use std::{fmt, path::PathBuf, str::FromStr};

use async_trait::async_trait;
use thiserror::Error;
use tokio::sync::mpsc;
use yakshed_domain::{ApprovalDecision, ConnectionId};

pub const EVENT_BUFFER_CAPACITY: usize = 64;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct HarnessDescriptor {
    pub id: String,
    pub name: String,
    pub version: String,
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

fn validate_opaque_id(label: &str, value: String) -> Result<String, HarnessError> {
    if value.trim().is_empty() || value.len() > 4096 || value.chars().any(char::is_control) {
        Err(HarnessError::InvalidInput(format!("invalid {label}")))
    } else {
        Ok(value)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StartSessionSpec {
    pub connection_id: ConnectionId,
    pub working_directory: PathBuf,
    pub title: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SessionQuery {
    pub after: Option<ProviderSessionId>,
    pub limit: u32,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Page<T> {
    pub items: Vec<T>,
    pub next_after: Option<ProviderSessionId>,
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
    pub working_directory: PathBuf,
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
    pub fn new(value: impl Into<String>) -> Self {
        Self(value.into())
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Debug for NativePayload {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("NativePayload([redacted])")
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum HarnessRunTerminal {
    Completed,
    Failed { message: String },
    Interrupted,
    Crashed { message: String },
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum HarnessEvent {
    RunAccepted {
        run_id: ProviderRunId,
        native: NativePayload,
    },
    /// Transient streaming text; reducers batch these rather than persisting each chunk.
    MessageDelta {
        run_id: ProviderRunId,
        chunk: String,
        native: NativePayload,
    },
    /// Authoritative completed message suitable for finalizing normalized state.
    MessageCompleted {
        run_id: ProviderRunId,
        text: String,
        native: NativePayload,
    },
    ApprovalRequested {
        run_id: ProviderRunId,
        request_id: ProviderRequestId,
        summary: String,
        native: NativePayload,
    },
    FileMutation {
        run_id: ProviderRunId,
        path: String,
        summary: String,
        native: NativePayload,
    },
    CommandOutput {
        run_id: ProviderRunId,
        command: String,
        chunk: String,
        native: NativePayload,
    },
    RunTerminal {
        run_id: ProviderRunId,
        state: HarnessRunTerminal,
        native: NativePayload,
    },
    Unknown {
        run_id: Option<ProviderRunId>,
        item_type: String,
        native: NativePayload,
    },
    MalformedNativePayload {
        run_id: Option<ProviderRunId>,
        item_type: String,
        native: NativePayload,
    },
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
    #[error("harness event stream is closed")]
    Closed,
    #[error("provider backend failure: {0}")]
    Backend(String),
}

#[derive(Clone)]
pub struct HarnessEventSender(mpsc::Sender<HarnessEvent>);

impl HarnessEventSender {
    pub async fn send(&self, event: HarnessEvent) -> Result<(), HarnessError> {
        self.0.send(event).await.map_err(|_| HarnessError::Closed)
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
    ) -> Result<ProviderRunId, HarnessError>;

    async fn steer(&self, run: &ProviderRunId, input: HarnessInput) -> Result<(), HarnessError>;

    async fn interrupt(&self, run: &ProviderRunId) -> Result<(), HarnessError>;

    async fn respond_to_request(
        &self,
        request: ProviderRequestId,
        response: ProviderResponse,
    ) -> Result<(), HarnessError>;

    fn subscribe(&self) -> Result<ProviderEventStream, HarnessError>;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn native_payload_debug_output_is_redacted() {
        let canary = "YAKSHED_SECRET_CANARY";
        let event = HarnessEvent::Unknown {
            run_id: None,
            item_type: "future".to_owned(),
            native: NativePayload::new(canary),
        };

        assert!(!format!("{event:?}").contains(canary));
    }

    #[test]
    fn provider_ids_are_opaque_but_bounded() {
        assert!(ProviderRunId::new("provider/native:id").is_ok());
        assert!(ProviderRunId::new("").is_err());
        assert!(ProviderRunId::new("bad\nvalue").is_err());
    }
}
