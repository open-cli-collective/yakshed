//! Codex App Server adapter over its last-validated JSONL stdio protocol.

mod actor;
pub mod reducer;

use std::{
    path::PathBuf,
    sync::{Arc, Mutex},
    time::Duration,
};

use actor::{RequestKind, RuntimeClient};
use async_trait::async_trait;
use serde_json::{Value, json};
use tokio::sync::OnceCell;
use yakshed_domain::{ConnectionId, CredentialSlot};
use yakshed_harness::{
    HarnessAccountStatus, HarnessAdapter, HarnessCapabilities, HarnessCredentialDelivery,
    HarnessCredentialRequirement, HarnessDescriptor, HarnessError, HarnessInput, Page,
    ProviderEventStream, ProviderRequestHandle, ProviderResponse, ProviderRunHandle, ProviderRunId,
    ProviderSession, ProviderSessionId, ProviderSessionSummary, RunOptions, RuntimeHandle,
    RuntimePath, SessionPageCursor, SessionQuery, StartSessionSpec, event_channel,
};

// Metadata only; runtime compatibility is intentionally not gated. See pins/codex-lock.json.
const LAST_VALIDATED_CODEX_VERSION: &str = "0.147.0";
const DEFAULT_MAX_FRAME_SIZE: usize = 1024 * 1024;

/// Local runtime identity. Each distinct key owns one App Server process.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CodexRuntimeKey {
    pub connection_id: ConnectionId,
    pub binary_digest: String,
    pub codex_home: PathBuf,
    pub execution_runtime: String,
}

/// Process configuration for one local Codex runtime.
#[derive(Clone, Debug)]
pub struct CodexRuntimeSpec {
    pub handle: RuntimeHandle,
    pub key: CodexRuntimeKey,
    pub program: PathBuf,
    pub args: Vec<String>,
    pub max_frame_size: usize,
    pub startup_timeout: Duration,
    /// Exact test/runtime canaries removed before payloads or diagnostics cross the adapter seam.
    pub redactions: Vec<String>,
}

impl CodexRuntimeSpec {
    pub fn local(
        handle: RuntimeHandle,
        key: CodexRuntimeKey,
        program: PathBuf,
        args: Vec<String>,
    ) -> Self {
        Self {
            handle,
            key,
            program,
            args,
            max_frame_size: DEFAULT_MAX_FRAME_SIZE,
            startup_timeout: Duration::from_secs(5),
            redactions: Vec::new(),
        }
    }

    fn validate(&self) -> Result<(), HarnessError> {
        if !self.key.codex_home.is_absolute() {
            return Err(HarnessError::InvalidInput(
                "Codex home must be absolute".to_owned(),
            ));
        }
        if self.key.binary_digest.trim().is_empty()
            || self.key.execution_runtime.trim().is_empty()
            || self.key.binary_digest.chars().any(char::is_control)
            || self.key.execution_runtime.chars().any(char::is_control)
        {
            return Err(HarnessError::InvalidInput(
                "invalid Codex runtime identity".to_owned(),
            ));
        }
        if self.max_frame_size == 0 || self.max_frame_size > 16 * 1024 * 1024 {
            return Err(HarnessError::InvalidInput(
                "invalid Codex maximum frame size".to_owned(),
            ));
        }
        if self
            .redactions
            .iter()
            .any(|value| value.is_empty() || value.len() > self.max_frame_size)
        {
            return Err(HarnessError::InvalidInput(
                "invalid Codex redaction value".to_owned(),
            ));
        }
        Ok(())
    }
}

/// One configured local runtime backed by one lazily started actor.
pub struct CodexAdapter {
    spec: CodexRuntimeSpec,
    runtime: OnceCell<RuntimeClient>,
    events: Mutex<Option<ProviderEventStream>>,
    event_sender: yakshed_harness::HarnessEventSender,
    process_group: Arc<std::sync::atomic::AtomicU32>,
}

impl CodexAdapter {
    pub fn new(spec: CodexRuntimeSpec) -> Result<Self, HarnessError> {
        spec.validate()?;
        let (event_sender, events) = event_channel();
        Ok(Self {
            spec,
            runtime: OnceCell::new(),
            events: Mutex::new(Some(events)),
            event_sender,
            process_group: Arc::new(std::sync::atomic::AtomicU32::new(0)),
        })
    }

    async fn runtime(&self, handle: &RuntimeHandle) -> Result<&RuntimeClient, HarnessError> {
        if handle != &self.spec.handle {
            return Err(HarnessError::NotFound {
                entity: "runtime",
                id: handle.to_string(),
            });
        }
        self.runtime
            .get_or_try_init(|| {
                actor::start_runtime(
                    self.spec.clone(),
                    self.event_sender.clone(),
                    Arc::clone(&self.process_group),
                )
            })
            .await
    }

    pub async fn diagnostics(&self) -> Result<Vec<String>, HarnessError> {
        self.runtime(&self.spec.handle).await?.diagnostics().await
    }

    pub async fn shutdown(&self) -> Result<(), HarnessError> {
        if let Some(runtime) = self.runtime.get() {
            runtime.shutdown().await?;
        }
        Ok(())
    }

    fn capabilities_value() -> HarnessCapabilities {
        HarnessCapabilities {
            persistent_sessions: true,
            session_listing: true,
            native_fork: false,
            session_archive: true,
            mid_run_steering: true,
            client_approvals: true,
            user_input_requests: true,
            structured_file_changes: true,
            command_output_streaming: true,
            native_subagent_lineage: true,
            images: true,
            skills: true,
            mcp: true,
            account_ui: true,
            model_discovery: true,
        }
    }

    fn parse_session(
        &self,
        value: &Value,
        fallback_title: &str,
    ) -> Result<ProviderSession, HarnessError> {
        let thread = value.get("thread").unwrap_or(value);
        let id = required_string(thread, "id")?;
        let cwd = required_string(thread, "cwd")?;
        let title = thread
            .get("name")
            .and_then(Value::as_str)
            .filter(|value| !value.is_empty())
            .or_else(|| thread.get("preview").and_then(Value::as_str))
            .unwrap_or(fallback_title)
            .to_owned();
        Ok(ProviderSession {
            id: ProviderSessionId::new(id)?,
            runtime: self.spec.handle.clone(),
            connection_id: self.spec.key.connection_id,
            working_directory: RuntimePath::new(cwd)?,
            title,
        })
    }
}

impl Drop for CodexAdapter {
    fn drop(&mut self) {
        actor::terminate_process_group(
            self.process_group
                .load(std::sync::atomic::Ordering::Acquire),
        );
    }
}

#[async_trait]
impl HarnessAdapter for CodexAdapter {
    fn descriptor(&self) -> HarnessDescriptor {
        HarnessDescriptor {
            id: "codex".to_owned(),
            name: "Codex App Server".to_owned(),
            version: LAST_VALIDATED_CODEX_VERSION.to_owned(),
        }
    }

    fn credential_requirements(&self) -> Vec<HarnessCredentialRequirement> {
        vec![HarnessCredentialRequirement {
            slot: CredentialSlot::new("codex.account").expect("constant slot is valid"),
            delivery: HarnessCredentialDelivery::Delegated,
        }]
    }

    async fn account_status(
        &self,
        runtime: &RuntimeHandle,
    ) -> Result<HarnessAccountStatus, HarnessError> {
        let result = self
            .runtime(runtime)
            .await?
            .request(
                "account/read",
                json!({"refreshToken": false}),
                false,
                RequestKind::AccountRead,
            )
            .await?;
        let Some(account) = result.get("account").filter(|account| !account.is_null()) else {
            return Ok(HarnessAccountStatus::NotAuthenticated);
        };
        if account.get("type").and_then(Value::as_str) != Some("chatgpt") {
            return Ok(HarnessAccountStatus::Unknown);
        }
        Ok(HarnessAccountStatus::Authenticated {
            email: account
                .get("email")
                .and_then(Value::as_str)
                .map(str::to_owned),
            plan: account
                .get("planType")
                .and_then(Value::as_str)
                .unwrap_or("unknown")
                .to_owned(),
        })
    }

    async fn account_login_start(
        &self,
        runtime: &RuntimeHandle,
    ) -> Result<HarnessAccountStatus, HarnessError> {
        let result = self
            .runtime(runtime)
            .await?
            .request(
                "account/login/start",
                json!({"type": "chatgpt", "useHostedLoginSuccessPage": true}),
                true,
                RequestKind::AccountLoginStart,
            )
            .await?;
        Ok(HarnessAccountStatus::LoginInProgress {
            login_id: required_string(&result, "loginId")?.to_owned(),
            auth_url: required_string(&result, "authUrl")?.to_owned(),
        })
    }

    async fn account_logout(&self, runtime: &RuntimeHandle) -> Result<(), HarnessError> {
        self.runtime(runtime)
            .await?
            .request("account/logout", json!({}), true, RequestKind::EmptyObject)
            .await?;
        Ok(())
    }

    async fn capabilities(
        &self,
        runtime: &RuntimeHandle,
    ) -> Result<HarnessCapabilities, HarnessError> {
        self.runtime(runtime).await?.health().await?;
        Ok(Self::capabilities_value())
    }

    async fn list_sessions(
        &self,
        runtime: &RuntimeHandle,
        query: SessionQuery,
    ) -> Result<Page<ProviderSessionSummary>, HarnessError> {
        let result = self
            .runtime(runtime)
            .await?
            .request(
                "thread/list",
                json!({"cursor": query.after.map(|value| value.to_string()), "limit": query.limit, "sortDirection": "asc"}),
                false,
                RequestKind::ThreadList,
            )
            .await?;
        let data = result
            .get("data")
            .and_then(Value::as_array)
            .ok_or_else(|| protocol_error("thread/list omitted data"))?;
        let mut items = Vec::with_capacity(data.len());
        for thread in data {
            items.push(ProviderSessionSummary {
                id: ProviderSessionId::new(required_string(thread, "id")?)?,
                title: thread
                    .get("name")
                    .and_then(Value::as_str)
                    .or_else(|| thread.get("preview").and_then(Value::as_str))
                    .unwrap_or("Codex thread")
                    .to_owned(),
                connection_id: self.spec.key.connection_id,
            });
        }
        let next = result
            .get("nextCursor")
            .and_then(Value::as_str)
            .map(SessionPageCursor::new)
            .transpose()?;
        Ok(Page { items, next })
    }

    async fn start_session(
        &self,
        runtime: &RuntimeHandle,
        spec: StartSessionSpec,
    ) -> Result<ProviderSession, HarnessError> {
        let cwd = PathBuf::from(spec.working_directory.as_str());
        if !cwd.is_absolute() {
            return Err(HarnessError::InvalidInput(
                "local Codex working directory must be absolute".to_owned(),
            ));
        }
        let client = self.runtime(runtime).await?;
        let result = client
            .request(
                "thread/start",
                json!({"cwd": spec.working_directory.as_str()}),
                true,
                RequestKind::Session {
                    expected_thread_id: None,
                },
            )
            .await?;
        let mut session = self.parse_session(&result, &spec.title)?;
        if let Err(error) = client
            .request(
                "thread/name/set",
                json!({"threadId": session.id.as_str(), "name": spec.title}),
                true,
                RequestKind::EmptyObject,
            )
            .await
        {
            client.record_diagnostic(error.to_string()).await;
            return Ok(session);
        }
        session.title = spec.title;
        Ok(session)
    }

    async fn resume_session(
        &self,
        runtime: &RuntimeHandle,
        id: &ProviderSessionId,
    ) -> Result<ProviderSession, HarnessError> {
        let result = self
            .runtime(runtime)
            .await?
            .request(
                "thread/resume",
                json!({"threadId": id.as_str()}),
                false,
                RequestKind::Session {
                    expected_thread_id: Some(id.as_str().to_owned()),
                },
            )
            .await?;
        self.parse_session(&result, "Codex thread")
    }

    async fn start_run(
        &self,
        session: &ProviderSession,
        input: HarnessInput,
        options: RunOptions,
    ) -> Result<ProviderRunHandle, HarnessError> {
        if session.runtime != self.spec.handle
            || session.connection_id != self.spec.key.connection_id
        {
            return Err(HarnessError::InvalidInput(
                "session does not belong to this Codex runtime".to_owned(),
            ));
        }
        if !matches!(
            self.account_status(&session.runtime).await?,
            HarnessAccountStatus::Authenticated { .. }
        ) {
            return Err(HarnessError::NotAuthenticated);
        }
        let result = self
            .runtime(&session.runtime)
            .await?
            .request(
                "turn/start",
                json!({
                    "threadId": session.id.as_str(),
                    "input": [{"type": "text", "text": input.as_str()}],
                    "model": options.model,
                }),
                true,
                RequestKind::StartRun {
                    session_id: session.id.clone(),
                },
            )
            .await?;
        let turn = result
            .get("turn")
            .ok_or_else(|| protocol_error("turn/start omitted turn"))?;
        Ok(ProviderRunHandle::new(
            self.spec.handle.clone(),
            session.id.clone(),
            ProviderRunId::new(required_string(turn, "id")?)?,
        ))
    }

    async fn steer(
        &self,
        run: &ProviderRunHandle,
        input: HarnessInput,
    ) -> Result<(), HarnessError> {
        self.runtime(run.runtime())
            .await?
            .request(
                "turn/steer",
                json!({
                    "threadId": run.session_id().as_str(),
                    "expectedTurnId": run.native_id().as_str(),
                    "input": [{"type": "text", "text": input.as_str()}],
                }),
                true,
                RequestKind::TurnSteer {
                    expected_turn_id: run.native_id().clone(),
                },
            )
            .await?;
        Ok(())
    }

    async fn interrupt(&self, run: &ProviderRunHandle) -> Result<(), HarnessError> {
        self.runtime(run.runtime())
            .await?
            .request(
                "turn/interrupt",
                json!({"threadId": run.session_id().as_str(), "turnId": run.native_id().as_str()}),
                true,
                RequestKind::EmptyObject,
            )
            .await?;
        Ok(())
    }

    async fn respond_to_request(
        &self,
        request: ProviderRequestHandle,
        response: ProviderResponse,
    ) -> Result<(), HarnessError> {
        self.runtime(request.run().runtime())
            .await?
            .respond(request, response)
            .await
    }

    fn subscribe(&self) -> Result<ProviderEventStream, HarnessError> {
        self.events
            .lock()
            .map_err(|_| HarnessError::Closed)?
            .take()
            .ok_or(HarnessError::Closed)
    }
}

fn required_string<'a>(value: &'a Value, field: &str) -> Result<&'a str, HarnessError> {
    value
        .get(field)
        .and_then(Value::as_str)
        .ok_or_else(|| protocol_error("Codex response omitted a required string"))
}

fn protocol_error(message: &str) -> HarnessError {
    HarnessError::Protocol {
        diagnostic: yakshed_harness::SanitizedDiagnostic::sanitized(message),
    }
}
