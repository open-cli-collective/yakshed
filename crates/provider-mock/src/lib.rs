//! Deterministic scripted implementation of the provider-neutral harness contract.

use std::{collections::HashMap, collections::VecDeque, sync::Mutex};

use async_trait::async_trait;
use tokio::sync::Mutex as AsyncMutex;
#[cfg(test)]
use tokio::sync::oneshot;
use yakshed_harness::{
    HarnessAdapter, HarnessCapabilities, HarnessDescriptor, HarnessError, HarnessEvent,
    HarnessEventSender, HarnessInput, HarnessRunTerminal, NativePayload, Page, ProviderEventStream,
    ProviderRequestHandle, ProviderRequestId, ProviderResponse, ProviderRunHandle, ProviderRunId,
    ProviderSession, ProviderSessionId, ProviderSessionSummary, RunOptions, RuntimeHandle,
    SanitizedDiagnostic, SessionPageCursor, SessionQuery, StartSessionSpec, event_channel,
};

/// Deterministic run/runtime faults. `DelayApproval` is released manually rather than by sleep.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum MockHarnessFault {
    ExitAfterRunAccepted,
    ExitAfterFileMutation,
    DelayApproval,
    EmitUnknownEvent,
    EmitMalformedNativePayload,
    NeverComplete,
    Overloaded,
    Disconnected,
    ProtocolFailure,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MockRunPlan {
    steps: VecDeque<MockScriptStep>,
    fault: Option<MockHarnessFault>,
}

impl MockRunPlan {
    pub fn new(steps: Vec<MockScriptStep>) -> Self {
        Self {
            steps: steps.into(),
            fault: None,
        }
    }

    pub fn with_fault(mut self, fault: MockHarnessFault) -> Self {
        self.fault = Some(fault);
        self
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum MockScriptStep {
    Message {
        chunk: String,
        native: String,
    },
    MessageCompleted {
        text: String,
        native: String,
    },
    Approval {
        request_id: ProviderRequestId,
        summary: String,
        native: String,
    },
    UserInput {
        request_id: ProviderRequestId,
        prompt: String,
        native: String,
    },
    AwaitResponse(ProviderRequestId),
    FileMutation {
        path: String,
        summary: String,
        native: String,
    },
    CommandOutput {
        command: String,
        chunk: String,
        native: String,
    },
    Complete {
        native: String,
    },
    Unknown {
        item_type: String,
        native: String,
    },
}

impl MockScriptStep {
    pub fn message(chunk: impl Into<String>) -> Self {
        let chunk = chunk.into();
        Self::Message {
            native: format!(r#"{{"type":"message.delta","delta":{chunk:?}}}"#),
            chunk,
        }
    }

    pub fn approval(request_id: ProviderRequestId, summary: impl Into<String>) -> Self {
        let summary = summary.into();
        Self::Approval {
            native: format!(r#"{{"type":"approval.requested","summary":{summary:?}}}"#),
            request_id,
            summary,
        }
    }

    pub fn message_completed(text: impl Into<String>) -> Self {
        let text = text.into();
        Self::MessageCompleted {
            native: format!(r#"{{"type":"message.completed","text":{text:?}}}"#),
            text,
        }
    }

    pub fn await_response(request_id: ProviderRequestId) -> Self {
        Self::AwaitResponse(request_id)
    }

    pub fn user_input(request_id: ProviderRequestId, prompt: impl Into<String>) -> Self {
        let prompt = prompt.into();
        Self::UserInput {
            native: format!(r#"{{"type":"user_input.requested","prompt":{prompt:?}}}"#),
            request_id,
            prompt,
        }
    }

    pub fn file_mutation(path: impl Into<String>, summary: impl Into<String>) -> Self {
        let path = path.into();
        let summary = summary.into();
        Self::FileMutation {
            native: format!(r#"{{"type":"file.mutation","path":{path:?},"summary":{summary:?}}}"#),
            path,
            summary,
        }
    }

    pub fn command_output(command: impl Into<String>, chunk: impl Into<String>) -> Self {
        let command = command.into();
        let chunk = chunk.into();
        Self::CommandOutput {
            native: format!(
                r#"{{"type":"command.output","command":{command:?},"chunk":{chunk:?}}}"#
            ),
            command,
            chunk,
        }
    }

    pub fn complete() -> Self {
        Self::Complete {
            native: r#"{"type":"run.completed"}"#.to_owned(),
        }
    }

    pub fn unknown(item_type: impl Into<String>, native: impl Into<String>) -> Self {
        Self::Unknown {
            item_type: item_type.into(),
            native: native.into(),
        }
    }
}

struct RunRecord {
    active: bool,
    delayed: bool,
    steps: VecDeque<MockScriptStep>,
    fault: Option<MockHarnessFault>,
}

struct RequestRecord {
    run: ProviderRunHandle,
    kind: RequestKind,
    response: Option<ProviderResponse>,
}

#[derive(Clone, Copy, Eq, PartialEq)]
enum RequestKind {
    Approval,
    UserInput,
}

struct SessionRecord {
    cursor: SessionPageCursor,
    session: ProviderSession,
}

struct State {
    next_session: HashMap<RuntimeHandle, u64>,
    next_run: HashMap<(RuntimeHandle, ProviderSessionId), u64>,
    next_session_cursor: u64,
    sessions: Vec<SessionRecord>,
    plans: VecDeque<MockRunPlan>,
    runs: HashMap<ProviderRunHandle, RunRecord>,
    requests: HashMap<ProviderRequestHandle, RequestRecord>,
    runtime_fault: Option<MockHarnessFault>,
}

pub struct MockHarness {
    capabilities: HarnessCapabilities,
    // ponytail: one mock-wide lock guarantees event/terminal order; split per run only if
    // deterministic concurrent-throughput scenarios require it.
    state: AsyncMutex<State>,
    events: HarnessEventSender,
    subscription: Mutex<Option<ProviderEventStream>>,
    native_redactions: Vec<String>,
    #[cfg(test)]
    steer_pause: Mutex<Option<SteerPause>>,
}

#[cfg(test)]
struct SteerPause {
    checked: oneshot::Sender<()>,
    resume: oneshot::Receiver<()>,
}

impl MockHarness {
    pub fn new(
        capabilities: HarnessCapabilities,
        plans: Vec<MockRunPlan>,
        runtime_fault: Option<MockHarnessFault>,
    ) -> Self {
        let (events, subscription) = event_channel();
        Self {
            capabilities,
            state: AsyncMutex::new(State {
                next_session: HashMap::new(),
                next_run: HashMap::new(),
                next_session_cursor: 1,
                sessions: Vec::new(),
                plans: plans.into(),
                runs: HashMap::new(),
                requests: HashMap::new(),
                runtime_fault,
            }),
            events,
            subscription: Mutex::new(Some(subscription)),
            native_redactions: Vec::new(),
            #[cfg(test)]
            steer_pause: Mutex::new(None),
        }
    }

    #[cfg(test)]
    fn pause_next_steer(&self) -> (oneshot::Receiver<()>, oneshot::Sender<()>) {
        let (checked_sender, checked_receiver) = oneshot::channel();
        let (resume_sender, resume_receiver) = oneshot::channel();
        *self
            .steer_pause
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(SteerPause {
            checked: checked_sender,
            resume: resume_receiver,
        });
        (checked_receiver, resume_sender)
    }

    pub fn with_native_redaction(mut self, value: impl Into<String>) -> Self {
        self.native_redactions.push(value.into());
        self
    }

    fn redact(&self, value: impl Into<String>) -> String {
        self.native_redactions
            .iter()
            .fold(value.into(), |text, secret| {
                text.replace(secret, "[redacted]")
            })
    }

    fn native(&self, value: impl Into<String>) -> NativePayload {
        NativePayload::sanitized(self.redact(value))
    }

    async fn check_runtime(&self) -> Result<(), HarnessError> {
        let fault = self.state.lock().await.runtime_fault.take();
        match fault {
            Some(MockHarnessFault::Overloaded) => Err(HarnessError::Overloaded),
            Some(MockHarnessFault::Disconnected) => Err(HarnessError::Disconnected),
            Some(MockHarnessFault::ProtocolFailure) => Err(HarnessError::Protocol {
                diagnostic: SanitizedDiagnostic::sanitized(self.redact(format!(
                    "native protocol failure: {}",
                    self.native_redactions
                        .first()
                        .map_or("unknown", String::as_str)
                ))),
            }),
            _ => Ok(()),
        }
    }

    async fn emit_terminal(
        &self,
        run: ProviderRunHandle,
        state: HarnessRunTerminal,
        native: impl Into<String>,
    ) -> Result<(), HarnessError> {
        self.events
            .send(HarnessEvent::RunTerminal {
                run,
                state,
                native: self.native(native),
            })
            .await
    }

    async fn process_run(&self, run_handle: &ProviderRunHandle) -> Result<(), HarnessError> {
        loop {
            let mut state = self.state.lock().await;
            let step = {
                let run = state
                    .runs
                    .get_mut(run_handle)
                    .ok_or(HarnessError::NotFound {
                        entity: "run",
                        id: run_handle.to_string(),
                    })?;
                if !run.active {
                    return Ok(());
                }
                if run.fault == Some(MockHarnessFault::DelayApproval)
                    && matches!(run.steps.front(), Some(MockScriptStep::Approval { .. }))
                {
                    run.delayed = true;
                    return Ok(());
                }
                run.steps.pop_front()
            };
            let Some(step) = step else {
                return Ok(());
            };
            match step {
                MockScriptStep::Message { chunk, native } => {
                    self.events
                        .send(HarnessEvent::MessageDelta {
                            run: run_handle.clone(),
                            chunk,
                            native: self.native(native),
                        })
                        .await?;
                }
                MockScriptStep::MessageCompleted { text, native } => {
                    self.events
                        .send(HarnessEvent::MessageCompleted {
                            run: run_handle.clone(),
                            text,
                            native: self.native(native),
                        })
                        .await?;
                }
                MockScriptStep::Approval {
                    request_id,
                    summary,
                    native,
                } => {
                    let request = ProviderRequestHandle::new(run_handle.clone(), request_id);
                    let previous = state.requests.insert(
                        request.clone(),
                        RequestRecord {
                            run: run_handle.clone(),
                            kind: RequestKind::Approval,
                            response: None,
                        },
                    );
                    if previous.is_some() {
                        return Err(HarnessError::Conflict(format!(
                            "duplicate provider request id: {request}"
                        )));
                    }
                    self.events
                        .send(HarnessEvent::ApprovalRequested {
                            request,
                            summary,
                            native: self.native(native),
                        })
                        .await?;
                }
                MockScriptStep::UserInput {
                    request_id,
                    prompt,
                    native,
                } => {
                    let request = ProviderRequestHandle::new(run_handle.clone(), request_id);
                    let previous = state.requests.insert(
                        request.clone(),
                        RequestRecord {
                            run: run_handle.clone(),
                            kind: RequestKind::UserInput,
                            response: None,
                        },
                    );
                    if previous.is_some() {
                        return Err(HarnessError::Conflict(format!(
                            "duplicate provider request id: {request}"
                        )));
                    }
                    self.events
                        .send(HarnessEvent::UserInputRequested {
                            request,
                            prompt,
                            native: self.native(native),
                        })
                        .await?;
                }
                MockScriptStep::AwaitResponse(request_id) => {
                    let request =
                        ProviderRequestHandle::new(run_handle.clone(), request_id.clone());
                    let answered = state
                        .requests
                        .get(&request)
                        .is_some_and(|request| request.response.is_some());
                    if !answered {
                        state
                            .runs
                            .get_mut(run_handle)
                            .expect("run exists while processing")
                            .steps
                            .push_front(MockScriptStep::AwaitResponse(request_id));
                        return Ok(());
                    }
                }
                MockScriptStep::FileMutation {
                    path,
                    summary,
                    native,
                } => {
                    self.events
                        .send(HarnessEvent::FileMutation {
                            run: run_handle.clone(),
                            path,
                            summary,
                            native: self.native(native),
                        })
                        .await?;
                    let should_exit = {
                        let run = state
                            .runs
                            .get_mut(run_handle)
                            .expect("run exists while processing");
                        if run.fault == Some(MockHarnessFault::ExitAfterFileMutation) {
                            run.active = false;
                            run.fault = None;
                            true
                        } else {
                            false
                        }
                    };
                    if should_exit {
                        return self
                            .emit_terminal(
                                run_handle.clone(),
                                HarnessRunTerminal::Crashed {
                                    diagnostic: SanitizedDiagnostic::sanitized(
                                        "mock runtime exited after file mutation",
                                    ),
                                },
                                r#"{"type":"runtime.exit"}"#,
                            )
                            .await;
                    }
                }
                MockScriptStep::CommandOutput {
                    command,
                    chunk,
                    native,
                } => {
                    self.events
                        .send(HarnessEvent::CommandOutput {
                            run: run_handle.clone(),
                            command,
                            chunk,
                            native: self.native(native),
                        })
                        .await?;
                }
                MockScriptStep::Complete { native } => {
                    state
                        .runs
                        .get_mut(run_handle)
                        .expect("run exists while processing")
                        .active = false;
                    return self
                        .emit_terminal(run_handle.clone(), HarnessRunTerminal::Completed, native)
                        .await;
                }
                MockScriptStep::Unknown { item_type, native } => {
                    self.events
                        .send(HarnessEvent::Unknown {
                            run: Some(run_handle.clone()),
                            item_type,
                            native: self.native(native),
                        })
                        .await?;
                }
            }
        }
    }

    pub async fn release_delayed_approval(&self) -> Result<(), HarnessError> {
        let run_id = {
            let mut state = self.state.lock().await;
            let (run_id, run) = state
                .runs
                .iter_mut()
                .find(|(_, run)| run.delayed)
                .ok_or_else(|| HarnessError::Conflict("no delayed approval".to_owned()))?;
            run.delayed = false;
            run.fault = None;
            run_id.clone()
        };
        self.process_run(&run_id).await
    }
}

#[async_trait]
impl HarnessAdapter for MockHarness {
    fn descriptor(&self) -> HarnessDescriptor {
        HarnessDescriptor {
            id: "mock".to_owned(),
            name: "Deterministic Mock Harness".to_owned(),
            version: "1".to_owned(),
        }
    }

    async fn capabilities(
        &self,
        _runtime: &RuntimeHandle,
    ) -> Result<HarnessCapabilities, HarnessError> {
        self.check_runtime().await?;
        Ok(self.capabilities)
    }

    async fn list_sessions(
        &self,
        runtime: &RuntimeHandle,
        query: SessionQuery,
    ) -> Result<Page<ProviderSessionSummary>, HarnessError> {
        self.check_runtime().await?;
        if query.limit == 0 || query.limit > 200 {
            return Err(HarnessError::InvalidInput(
                "session page size must be between 1 and 200".to_owned(),
            ));
        }
        let state = self.state.lock().await;
        let start = match query.after {
            Some(cursor) => state
                .sessions
                .iter()
                .position(|record| record.session.runtime == *runtime && record.cursor == cursor)
                .map(|index| index + 1)
                .ok_or_else(|| HarnessError::InvalidInput("unknown session cursor".to_owned()))?,
            None => 0,
        };
        let mut records = state
            .sessions
            .iter()
            .skip(start)
            .filter(|record| record.session.runtime == *runtime)
            .take(query.limit as usize + 1)
            .collect::<Vec<_>>();
        let has_more = records.len() > query.limit as usize;
        records.truncate(query.limit as usize);
        let next = has_more.then(|| records.last().expect("nonempty page").cursor.clone());
        let items = records
            .into_iter()
            .map(|record| ProviderSessionSummary {
                id: record.session.id.clone(),
                title: record.session.title.clone(),
                connection_id: record.session.connection_id,
            })
            .collect();
        Ok(Page { items, next })
    }

    async fn start_session(
        &self,
        runtime: &RuntimeHandle,
        spec: StartSessionSpec,
    ) -> Result<ProviderSession, HarnessError> {
        self.check_runtime().await?;
        if spec.title.trim().is_empty() {
            return Err(HarnessError::InvalidInput(
                "session requires a title".to_owned(),
            ));
        }
        let mut state = self.state.lock().await;
        let sequence = state.next_session.entry(runtime.clone()).or_insert(1);
        let id = ProviderSessionId::new(format!("session-{sequence:04}"))?;
        *sequence += 1;
        let session = ProviderSession {
            id,
            runtime: runtime.clone(),
            connection_id: spec.connection_id,
            working_directory: spec.working_directory,
            title: spec.title,
        };
        let cursor =
            SessionPageCursor::new(format!("session-position-{}", state.next_session_cursor))?;
        state.next_session_cursor += 1;
        state.sessions.push(SessionRecord {
            cursor,
            session: session.clone(),
        });
        Ok(session)
    }

    async fn resume_session(
        &self,
        runtime: &RuntimeHandle,
        id: &ProviderSessionId,
    ) -> Result<ProviderSession, HarnessError> {
        self.check_runtime().await?;
        self.state
            .lock()
            .await
            .sessions
            .iter()
            .find(|record| record.session.id == *id && record.session.runtime == *runtime)
            .map(|record| record.session.clone())
            .ok_or_else(|| HarnessError::NotFound {
                entity: "session",
                id: id.to_string(),
            })
    }

    async fn start_run(
        &self,
        session: &ProviderSession,
        _input: HarnessInput,
        _options: RunOptions,
    ) -> Result<ProviderRunHandle, HarnessError> {
        self.check_runtime().await?;
        let (run_id, fault) = {
            let mut state = self.state.lock().await;
            if !state
                .sessions
                .iter()
                .any(|record| record.session == *session)
            {
                return Err(HarnessError::NotFound {
                    entity: "session",
                    id: session.id.to_string(),
                });
            }
            let plan = state
                .plans
                .pop_front()
                .unwrap_or_else(|| MockRunPlan::new(vec![MockScriptStep::complete()]));
            match plan.fault {
                Some(MockHarnessFault::Overloaded) => return Err(HarnessError::Overloaded),
                Some(MockHarnessFault::Disconnected) => return Err(HarnessError::Disconnected),
                Some(MockHarnessFault::ProtocolFailure) => {
                    return Err(HarnessError::Protocol {
                        diagnostic: SanitizedDiagnostic::sanitized(
                            self.redact("mock run protocol failure"),
                        ),
                    });
                }
                _ => {}
            }
            let key = (session.runtime.clone(), session.id.clone());
            let sequence = state.next_run.entry(key).or_insert(1);
            let run_id = ProviderRunId::new(format!("run-{sequence:04}"))?;
            *sequence += 1;
            let run_handle =
                ProviderRunHandle::new(session.runtime.clone(), session.id.clone(), run_id);
            let fault = plan.fault;
            state.runs.insert(
                run_handle.clone(),
                RunRecord {
                    active: true,
                    delayed: false,
                    steps: plan.steps,
                    fault,
                },
            );
            (run_handle, fault)
        };
        self.events
            .send(HarnessEvent::RunAccepted {
                run: run_id.clone(),
                native: self.native(r#"{"type":"run.accepted"}"#),
            })
            .await?;
        match fault {
            Some(MockHarnessFault::ExitAfterRunAccepted) => {
                self.state
                    .lock()
                    .await
                    .runs
                    .get_mut(&run_id)
                    .expect("new run exists")
                    .active = false;
                self.emit_terminal(
                    run_id.clone(),
                    HarnessRunTerminal::Crashed {
                        diagnostic: SanitizedDiagnostic::sanitized(
                            "mock runtime exited after accepting run",
                        ),
                    },
                    r#"{"type":"runtime.exit"}"#,
                )
                .await?;
            }
            Some(MockHarnessFault::EmitUnknownEvent) => {
                self.events
                    .send(HarnessEvent::Unknown {
                        run: Some(run_id.clone()),
                        item_type: "mock.future-item".to_owned(),
                        native: self.native(r#"{"type":"mock.future-item","answer":42}"#),
                    })
                    .await?;
                self.process_run(&run_id).await?;
            }
            Some(MockHarnessFault::EmitMalformedNativePayload) => {
                self.events
                    .send(HarnessEvent::MalformedNativePayload {
                        run: Some(run_id.clone()),
                        item_type: "mock.malformed".to_owned(),
                        native: self.native("{not-json"),
                    })
                    .await?;
                self.process_run(&run_id).await?;
            }
            Some(MockHarnessFault::NeverComplete) => {}
            _ => self.process_run(&run_id).await?,
        }
        Ok(run_id)
    }

    async fn steer(
        &self,
        run: &ProviderRunHandle,
        input: HarnessInput,
    ) -> Result<(), HarnessError> {
        let state = self.state.lock().await;
        if !state.runs.get(run).is_some_and(|run| run.active) {
            return Err(HarnessError::Conflict(format!("run is not active: {run}")));
        }
        #[cfg(test)]
        let pause = self
            .steer_pause
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take();
        #[cfg(test)]
        if let Some(pause) = pause {
            let _ = pause.checked.send(());
            let _ = pause.resume.await;
        }
        let result = self
            .events
            .send(HarnessEvent::MessageDelta {
                run: run.clone(),
                chunk: input.as_str().to_owned(),
                native: self.native(r#"{"type":"run.steer"}"#),
            })
            .await;
        drop(state);
        result
    }

    async fn interrupt(&self, run: &ProviderRunHandle) -> Result<(), HarnessError> {
        let mut state = self.state.lock().await;
        let record = state
            .runs
            .get_mut(run)
            .ok_or_else(|| HarnessError::NotFound {
                entity: "run",
                id: run.to_string(),
            })?;
        if !record.active {
            return Err(HarnessError::Conflict(format!("run is not active: {run}")));
        }
        record.active = false;
        let result = self
            .emit_terminal(
                run.clone(),
                HarnessRunTerminal::Interrupted,
                r#"{"type":"run.interrupted"}"#,
            )
            .await;
        drop(state);
        result
    }

    async fn respond_to_request(
        &self,
        request: ProviderRequestHandle,
        response: ProviderResponse,
    ) -> Result<(), HarnessError> {
        let run = {
            let mut state = self.state.lock().await;
            let record =
                state
                    .requests
                    .get_mut(&request)
                    .ok_or_else(|| HarnessError::NotFound {
                        entity: "provider request",
                        id: request.to_string(),
                    })?;
            if record.response.is_some() {
                return Err(HarnessError::Conflict(format!(
                    "provider request already answered: {request}"
                )));
            }
            if !matches!(
                (&record.kind, &response),
                (RequestKind::Approval, ProviderResponse::Approval(_))
                    | (RequestKind::UserInput, ProviderResponse::UserInput(_))
            ) {
                return Err(HarnessError::Conflict(format!(
                    "response kind does not match provider request: {request}"
                )));
            }
            record.response = Some(response);
            record.run.clone()
        };
        self.process_run(&run).await
    }

    fn subscribe(&self) -> Result<ProviderEventStream, HarnessError> {
        self.subscription
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take()
            .ok_or_else(|| HarnessError::Conflict("event stream already subscribed".to_owned()))
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use yakshed_harness::RuntimePath;

    #[tokio::test]
    async fn steer_checked_before_interrupt_cannot_emit_after_terminal() {
        let mock = Arc::new(MockHarness::new(
            HarnessCapabilities::default(),
            vec![MockRunPlan::new(Vec::new()).with_fault(MockHarnessFault::NeverComplete)],
            None,
        ));
        let runtime = RuntimeHandle::new("runtime-a").unwrap();
        let session = mock
            .start_session(
                &runtime,
                StartSessionSpec {
                    connection_id: "0193f26e-7a72-7000-8000-00000000aaa1".parse().unwrap(),
                    working_directory: RuntimePath::new("runtime-a://workspace").unwrap(),
                    title: "race".to_owned(),
                },
            )
            .await
            .unwrap();
        let mut stream = mock.subscribe().unwrap();
        let run = mock
            .start_run(
                &session,
                HarnessInput::new("start").unwrap(),
                RunOptions::default(),
            )
            .await
            .unwrap();
        assert!(matches!(
            stream.recv().await,
            Some(HarnessEvent::RunAccepted { .. })
        ));

        let (checked, resume) = mock.pause_next_steer();
        let steer_mock = Arc::clone(&mock);
        let steer_run = run.clone();
        let steer = tokio::spawn(async move {
            steer_mock
                .steer(&steer_run, HarnessInput::new("steered").unwrap())
                .await
        });
        checked.await.unwrap();
        let mut interrupt = Box::pin(mock.interrupt(&run));
        tokio::select! {
            biased;
            result = &mut interrupt => panic!("interrupt passed an in-flight steer: {result:?}"),
            () = std::future::ready(()) => {}
        }
        resume.send(()).unwrap();
        let (steer_result, interrupt_result) = tokio::join!(steer, interrupt);
        steer_result.unwrap().unwrap();
        interrupt_result.unwrap();

        assert!(matches!(
            stream.recv().await,
            Some(HarnessEvent::MessageDelta { chunk, .. }) if chunk == "steered"
        ));
        assert!(matches!(
            stream.recv().await,
            Some(HarnessEvent::RunTerminal {
                state: HarnessRunTerminal::Interrupted,
                ..
            })
        ));
        assert!(matches!(
            mock.steer(&run, HarnessInput::new("too late").unwrap())
                .await,
            Err(HarnessError::Conflict(_))
        ));
    }
}
