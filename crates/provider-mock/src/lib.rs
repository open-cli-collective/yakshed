//! Deterministic scripted implementation of the provider-neutral harness contract.

use std::{collections::HashMap, collections::VecDeque, sync::Mutex};

use async_trait::async_trait;
use tokio::sync::Mutex as AsyncMutex;
use yakshed_harness::{
    HarnessAdapter, HarnessCapabilities, HarnessDescriptor, HarnessError, HarnessEvent,
    HarnessEventSender, HarnessInput, HarnessRunTerminal, NativePayload, Page, ProviderEventStream,
    ProviderRequestId, ProviderResponse, ProviderRunId, ProviderSession, ProviderSessionId,
    ProviderSessionSummary, RunOptions, RuntimeHandle, SessionQuery, StartSessionSpec,
    event_channel,
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
        native: NativePayload,
    },
    MessageCompleted {
        text: String,
        native: NativePayload,
    },
    Approval {
        request_id: ProviderRequestId,
        summary: String,
        native: NativePayload,
    },
    AwaitResponse(ProviderRequestId),
    FileMutation {
        path: String,
        summary: String,
        native: NativePayload,
    },
    CommandOutput {
        command: String,
        chunk: String,
        native: NativePayload,
    },
    Complete {
        native: NativePayload,
    },
}

impl MockScriptStep {
    pub fn message(chunk: impl Into<String>) -> Self {
        let chunk = chunk.into();
        Self::Message {
            native: NativePayload::new(format!(r#"{{"type":"message.delta","delta":{chunk:?}}}"#)),
            chunk,
        }
    }

    pub fn approval(request_id: ProviderRequestId, summary: impl Into<String>) -> Self {
        let summary = summary.into();
        Self::Approval {
            native: NativePayload::new(format!(
                r#"{{"type":"approval.requested","summary":{summary:?}}}"#
            )),
            request_id,
            summary,
        }
    }

    pub fn message_completed(text: impl Into<String>) -> Self {
        let text = text.into();
        Self::MessageCompleted {
            native: NativePayload::new(format!(
                r#"{{"type":"message.completed","text":{text:?}}}"#
            )),
            text,
        }
    }

    pub fn await_response(request_id: ProviderRequestId) -> Self {
        Self::AwaitResponse(request_id)
    }

    pub fn file_mutation(path: impl Into<String>, summary: impl Into<String>) -> Self {
        let path = path.into();
        let summary = summary.into();
        Self::FileMutation {
            native: NativePayload::new(format!(
                r#"{{"type":"file.mutation","path":{path:?},"summary":{summary:?}}}"#
            )),
            path,
            summary,
        }
    }

    pub fn command_output(command: impl Into<String>, chunk: impl Into<String>) -> Self {
        let command = command.into();
        let chunk = chunk.into();
        Self::CommandOutput {
            native: NativePayload::new(format!(
                r#"{{"type":"command.output","command":{command:?},"chunk":{chunk:?}}}"#
            )),
            command,
            chunk,
        }
    }

    pub fn complete() -> Self {
        Self::Complete {
            native: NativePayload::new(r#"{"type":"run.completed"}"#),
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
    run_id: ProviderRunId,
    response: Option<ProviderResponse>,
}

struct State {
    next_session: u64,
    next_run: u64,
    sessions: Vec<ProviderSession>,
    plans: VecDeque<MockRunPlan>,
    runs: HashMap<ProviderRunId, RunRecord>,
    requests: HashMap<ProviderRequestId, RequestRecord>,
    runtime_fault: Option<MockHarnessFault>,
}

pub struct MockHarness {
    capabilities: HarnessCapabilities,
    state: AsyncMutex<State>,
    events: HarnessEventSender,
    subscription: Mutex<Option<ProviderEventStream>>,
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
                next_session: 1,
                next_run: 1,
                sessions: Vec::new(),
                plans: plans.into(),
                runs: HashMap::new(),
                requests: HashMap::new(),
                runtime_fault,
            }),
            events,
            subscription: Mutex::new(Some(subscription)),
        }
    }

    async fn check_runtime(&self) -> Result<(), HarnessError> {
        let fault = self.state.lock().await.runtime_fault.take();
        match fault {
            Some(MockHarnessFault::Overloaded) => Err(HarnessError::Overloaded),
            Some(MockHarnessFault::Disconnected) => Err(HarnessError::Disconnected),
            _ => Ok(()),
        }
    }

    async fn emit_terminal(
        &self,
        run_id: ProviderRunId,
        state: HarnessRunTerminal,
        native: NativePayload,
    ) -> Result<(), HarnessError> {
        self.events
            .send(HarnessEvent::RunTerminal {
                run_id,
                state,
                native,
            })
            .await
    }

    async fn process_run(&self, run_id: &ProviderRunId) -> Result<(), HarnessError> {
        loop {
            let step = {
                let mut state = self.state.lock().await;
                let run = state.runs.get_mut(run_id).ok_or(HarnessError::NotFound {
                    entity: "run",
                    id: run_id.to_string(),
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
                            run_id: run_id.clone(),
                            chunk,
                            native,
                        })
                        .await?;
                }
                MockScriptStep::MessageCompleted { text, native } => {
                    self.events
                        .send(HarnessEvent::MessageCompleted {
                            run_id: run_id.clone(),
                            text,
                            native,
                        })
                        .await?;
                }
                MockScriptStep::Approval {
                    request_id,
                    summary,
                    native,
                } => {
                    let previous = self.state.lock().await.requests.insert(
                        request_id.clone(),
                        RequestRecord {
                            run_id: run_id.clone(),
                            response: None,
                        },
                    );
                    if previous.is_some() {
                        return Err(HarnessError::Conflict(format!(
                            "duplicate provider request id: {request_id}"
                        )));
                    }
                    self.events
                        .send(HarnessEvent::ApprovalRequested {
                            run_id: run_id.clone(),
                            request_id,
                            summary,
                            native,
                        })
                        .await?;
                }
                MockScriptStep::AwaitResponse(request_id) => {
                    let mut state = self.state.lock().await;
                    let answered = state
                        .requests
                        .get(&request_id)
                        .is_some_and(|request| request.response.is_some());
                    if !answered {
                        state
                            .runs
                            .get_mut(run_id)
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
                            run_id: run_id.clone(),
                            path,
                            summary,
                            native,
                        })
                        .await?;
                    let should_exit = {
                        let mut state = self.state.lock().await;
                        let run = state
                            .runs
                            .get_mut(run_id)
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
                                run_id.clone(),
                                HarnessRunTerminal::Crashed {
                                    message: "mock runtime exited after file mutation".to_owned(),
                                },
                                NativePayload::new(r#"{"type":"runtime.exit"}"#),
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
                            run_id: run_id.clone(),
                            command,
                            chunk,
                            native,
                        })
                        .await?;
                }
                MockScriptStep::Complete { native } => {
                    self.state
                        .lock()
                        .await
                        .runs
                        .get_mut(run_id)
                        .expect("run exists while processing")
                        .active = false;
                    return self
                        .emit_terminal(run_id.clone(), HarnessRunTerminal::Completed, native)
                        .await;
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
        let mut items = state
            .sessions
            .iter()
            .filter(|session| {
                session.runtime == *runtime
                    && query.after.as_ref().is_none_or(|after| session.id > *after)
            })
            .map(|session| ProviderSessionSummary {
                id: session.id.clone(),
                title: session.title.clone(),
                connection_id: session.connection_id,
            })
            .take(query.limit as usize + 1)
            .collect::<Vec<_>>();
        let has_more = items.len() > query.limit as usize;
        items.truncate(query.limit as usize);
        let next_after = has_more.then(|| items.last().expect("nonempty page").id.clone());
        Ok(Page { items, next_after })
    }

    async fn start_session(
        &self,
        runtime: &RuntimeHandle,
        spec: StartSessionSpec,
    ) -> Result<ProviderSession, HarnessError> {
        self.check_runtime().await?;
        if spec.title.trim().is_empty() || !spec.working_directory.is_absolute() {
            return Err(HarnessError::InvalidInput(
                "session requires a title and absolute working directory".to_owned(),
            ));
        }
        let mut state = self.state.lock().await;
        let id = ProviderSessionId::new(format!("session-{:04}", state.next_session))?;
        state.next_session += 1;
        let session = ProviderSession {
            id,
            runtime: runtime.clone(),
            connection_id: spec.connection_id,
            working_directory: spec.working_directory,
            title: spec.title,
        };
        state.sessions.push(session.clone());
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
            .find(|session| session.id == *id && session.runtime == *runtime)
            .cloned()
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
    ) -> Result<ProviderRunId, HarnessError> {
        self.check_runtime().await?;
        let (run_id, fault) = {
            let mut state = self.state.lock().await;
            if !state.sessions.contains(session) {
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
                _ => {}
            }
            let run_id = ProviderRunId::new(format!("run-{:04}", state.next_run))?;
            state.next_run += 1;
            let fault = plan.fault;
            state.runs.insert(
                run_id.clone(),
                RunRecord {
                    active: true,
                    delayed: false,
                    steps: plan.steps,
                    fault,
                },
            );
            (run_id, fault)
        };
        self.events
            .send(HarnessEvent::RunAccepted {
                run_id: run_id.clone(),
                native: NativePayload::new(r#"{"type":"run.accepted"}"#),
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
                        message: "mock runtime exited after accepting run".to_owned(),
                    },
                    NativePayload::new(r#"{"type":"runtime.exit"}"#),
                )
                .await?;
            }
            Some(MockHarnessFault::EmitUnknownEvent) => {
                self.events
                    .send(HarnessEvent::Unknown {
                        run_id: Some(run_id.clone()),
                        item_type: "mock.future-item".to_owned(),
                        native: NativePayload::new(r#"{"type":"mock.future-item","answer":42}"#),
                    })
                    .await?;
                self.process_run(&run_id).await?;
            }
            Some(MockHarnessFault::EmitMalformedNativePayload) => {
                self.events
                    .send(HarnessEvent::MalformedNativePayload {
                        run_id: Some(run_id.clone()),
                        item_type: "mock.malformed".to_owned(),
                        native: NativePayload::new("{not-json"),
                    })
                    .await?;
                self.process_run(&run_id).await?;
            }
            Some(MockHarnessFault::NeverComplete) => {}
            _ => self.process_run(&run_id).await?,
        }
        Ok(run_id)
    }

    async fn steer(&self, run: &ProviderRunId, input: HarnessInput) -> Result<(), HarnessError> {
        let state = self.state.lock().await;
        if !state.runs.get(run).is_some_and(|run| run.active) {
            return Err(HarnessError::Conflict(format!("run is not active: {run}")));
        }
        drop(state);
        self.events
            .send(HarnessEvent::MessageDelta {
                run_id: run.clone(),
                chunk: input.as_str().to_owned(),
                native: NativePayload::new(r#"{"type":"run.steer"}"#),
            })
            .await
    }

    async fn interrupt(&self, run: &ProviderRunId) -> Result<(), HarnessError> {
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
        drop(state);
        self.emit_terminal(
            run.clone(),
            HarnessRunTerminal::Interrupted,
            NativePayload::new(r#"{"type":"run.interrupted"}"#),
        )
        .await
    }

    async fn respond_to_request(
        &self,
        request: ProviderRequestId,
        response: ProviderResponse,
    ) -> Result<(), HarnessError> {
        let run_id = {
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
            record.response = Some(response);
            record.run_id.clone()
        };
        self.process_run(&run_id).await
    }

    fn subscribe(&self) -> Result<ProviderEventStream, HarnessError> {
        self.subscription
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take()
            .ok_or_else(|| HarnessError::Conflict("event stream already subscribed".to_owned()))
    }
}
