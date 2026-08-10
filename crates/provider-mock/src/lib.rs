//! Deterministic scripted implementation of the provider-neutral harness contract.

use std::{collections::HashMap, collections::VecDeque, sync::Mutex};

use async_trait::async_trait;
use tokio::sync::Mutex as AsyncMutex;
#[cfg(test)]
use tokio::sync::oneshot;
use yakshed_domain::ConnectionId;
use yakshed_harness::{
    HarnessAdapter, HarnessCapabilities, HarnessDescriptor, HarnessError, HarnessEvent,
    HarnessEventPermit, HarnessEventSender, HarnessInput, HarnessRunTerminal, NativePayload, Page,
    ProviderEventStream, ProviderRequestHandle, ProviderRequestId, ProviderResponse,
    ProviderRunHandle, ProviderRunId, ProviderSession, ProviderSessionId, ProviderSessionSummary,
    RunOptions, RuntimeHandle, SanitizedDiagnostic, SessionPageCursor, SessionQuery,
    StartSessionSpec, event_channel,
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
    Malformed {
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

    pub fn malformed(item_type: impl Into<String>, native: impl Into<String>) -> Self {
        Self::Malformed {
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
    pending: Option<PendingDelivery>,
}

#[derive(Clone, Eq, PartialEq)]
struct PendingDelivery {
    event: HarnessEvent,
    commit: DeliveryCommit,
}

#[derive(Clone, Eq, PartialEq)]
enum DeliveryCommit {
    Step {
        terminal_after: Option<PendingTerminal>,
    },
    Standalone,
    Terminal {
        consume_step: bool,
    },
}

#[derive(Clone, Eq, PartialEq)]
struct PendingTerminal {
    state: HarnessRunTerminal,
    native: String,
}

#[derive(Clone, Copy, Eq, PartialEq)]
enum FlushResult {
    None,
    Changed,
    Delivered,
    Terminal,
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
    runtime_connections: HashMap<RuntimeHandle, ConnectionId>,
    runtime_capabilities: HashMap<RuntimeHandle, HarnessCapabilities>,
    runtime_faults: HashMap<RuntimeHandle, VecDeque<MockHarnessFault>>,
    default_runtime_faults: VecDeque<MockHarnessFault>,
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
    #[cfg(test)]
    delivery_probe: Mutex<Option<DeliveryProbe>>,
    #[cfg(test)]
    acceptance_probe: Mutex<Option<oneshot::Sender<()>>>,
}

#[cfg(test)]
struct SteerPause {
    checked: oneshot::Sender<()>,
    resume: oneshot::Receiver<()>,
}

#[cfg(test)]
struct DeliveryProbe {
    remaining: usize,
    reached: oneshot::Sender<()>,
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
                runtime_connections: HashMap::new(),
                runtime_capabilities: HashMap::new(),
                runtime_faults: HashMap::new(),
                default_runtime_faults: runtime_fault.into_iter().collect(),
            }),
            events,
            subscription: Mutex::new(Some(subscription)),
            native_redactions: Vec::new(),
            #[cfg(test)]
            steer_pause: Mutex::new(None),
            #[cfg(test)]
            delivery_probe: Mutex::new(None),
            #[cfg(test)]
            acceptance_probe: Mutex::new(None),
        }
    }

    pub fn with_runtime(
        mut self,
        runtime: RuntimeHandle,
        connection_id: ConnectionId,
        capabilities: Option<HarnessCapabilities>,
        faults: Vec<MockHarnessFault>,
    ) -> Self {
        let state = self.state.get_mut();
        state
            .runtime_connections
            .insert(runtime.clone(), connection_id);
        if let Some(capabilities) = capabilities {
            state
                .runtime_capabilities
                .insert(runtime.clone(), capabilities);
        }
        if !faults.is_empty() {
            state.runtime_faults.insert(runtime, faults.into());
        }
        self
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

    #[cfg(test)]
    fn probe_delivery(&self, delivery_number: usize) -> oneshot::Receiver<()> {
        let (sender, receiver) = oneshot::channel();
        *self
            .delivery_probe
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(DeliveryProbe {
            remaining: delivery_number,
            reached: sender,
        });
        receiver
    }

    #[cfg(test)]
    fn note_delivery_attempt(&self) {
        let mut probe = self
            .delivery_probe
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let Some(current) = probe.as_mut() else {
            return;
        };
        current.remaining -= 1;
        if current.remaining == 0 {
            let reached = probe.take().expect("delivery probe exists").reached;
            let _ = reached.send(());
        }
    }

    #[cfg(test)]
    fn probe_acceptance_reserve(&self) -> oneshot::Receiver<()> {
        let (sender, receiver) = oneshot::channel();
        *self
            .acceptance_probe
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(sender);
        receiver
    }

    #[cfg(test)]
    fn note_acceptance_reserve(&self) {
        if let Some(sender) = self
            .acceptance_probe
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take()
        {
            let _ = sender.send(());
        }
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

    async fn check_runtime(
        &self,
        runtime: &RuntimeHandle,
    ) -> Result<HarnessCapabilities, HarnessError> {
        let mut state = self.state.lock().await;
        if !state.runtime_connections.contains_key(runtime) {
            return Err(HarnessError::NotFound {
                entity: "runtime",
                id: runtime.to_string(),
            });
        }
        let fault = state
            .runtime_faults
            .get_mut(runtime)
            .and_then(VecDeque::pop_front)
            .or_else(|| state.default_runtime_faults.pop_front());
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
            _ => Ok(state
                .runtime_capabilities
                .get(runtime)
                .copied()
                .unwrap_or(self.capabilities)),
        }
    }

    fn terminal_delivery(
        &self,
        run: ProviderRunHandle,
        state: HarnessRunTerminal,
        native: impl Into<String>,
        consume_step: bool,
    ) -> PendingDelivery {
        PendingDelivery {
            event: HarnessEvent::RunTerminal {
                run,
                state,
                native: self.native(native),
            },
            commit: DeliveryCommit::Terminal { consume_step },
        }
    }

    fn reject_immediate_run_fault(
        &self,
        fault: Option<MockHarnessFault>,
    ) -> Result<(), HarnessError> {
        match fault {
            Some(MockHarnessFault::Overloaded) => Err(HarnessError::Overloaded),
            Some(MockHarnessFault::Disconnected) => Err(HarnessError::Disconnected),
            Some(MockHarnessFault::ProtocolFailure) => Err(HarnessError::Protocol {
                diagnostic: SanitizedDiagnostic::sanitized(
                    self.redact("mock run protocol failure"),
                ),
            }),
            _ => Ok(()),
        }
    }

    // Every event path validates, reserves without the state lock, then revalidates and
    // commits with a synchronous permit send under the lock.
    async fn flush_pending(
        &self,
        run_handle: &ProviderRunHandle,
    ) -> Result<FlushResult, HarnessError> {
        let pending = {
            let state = self.state.lock().await;
            let run = state
                .runs
                .get(run_handle)
                .ok_or_else(|| HarnessError::NotFound {
                    entity: "run",
                    id: run_handle.to_string(),
                })?;
            run.pending.clone()
        };
        let Some(pending) = pending else {
            return Ok(FlushResult::None);
        };

        #[cfg(test)]
        self.note_delivery_attempt();
        let permit = self.events.reserve().await?;
        let mut state = self.state.lock().await;
        if state
            .runs
            .get(run_handle)
            .and_then(|run| run.pending.as_ref())
            != Some(&pending)
        {
            return Ok(FlushResult::Changed);
        }
        Ok(self.commit_pending(&mut state, run_handle, pending, permit))
    }

    fn commit_pending(
        &self,
        state: &mut State,
        run_handle: &ProviderRunHandle,
        pending: PendingDelivery,
        permit: HarnessEventPermit,
    ) -> FlushResult {
        permit.send(pending.event);
        let run = state
            .runs
            .get_mut(run_handle)
            .expect("pending delivery belongs to an existing run");
        match pending.commit {
            DeliveryCommit::Step { terminal_after } => {
                run.steps.pop_front();
                run.pending = terminal_after.map(|terminal| {
                    self.terminal_delivery(
                        run_handle.clone(),
                        terminal.state,
                        terminal.native,
                        false,
                    )
                });
                FlushResult::Delivered
            }
            DeliveryCommit::Standalone => {
                run.pending = None;
                FlushResult::Delivered
            }
            DeliveryCommit::Terminal { consume_step } => {
                if consume_step {
                    run.steps.pop_front();
                }
                run.pending = None;
                run.active = false;
                FlushResult::Terminal
            }
        }
    }

    async fn process_run(&self, run_handle: &ProviderRunHandle) -> Result<(), HarnessError> {
        loop {
            match self.flush_pending(run_handle).await? {
                FlushResult::Terminal => return Ok(()),
                FlushResult::Changed | FlushResult::Delivered => continue,
                FlushResult::None => {}
            }
            let step = {
                let mut state = self.state.lock().await;
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
                if run.pending.is_some() {
                    continue;
                }
                if run.fault == Some(MockHarnessFault::DelayApproval)
                    && matches!(run.steps.front(), Some(MockScriptStep::Approval { .. }))
                {
                    run.delayed = true;
                    return Ok(());
                }
                let step = run.steps.front().cloned();
                match step.as_ref() {
                    Some(MockScriptStep::AwaitResponse(request_id)) => {
                        let request =
                            ProviderRequestHandle::new(run_handle.clone(), request_id.clone());
                        if state
                            .requests
                            .get(&request)
                            .is_none_or(|request| request.response.is_none())
                        {
                            return Ok(());
                        }
                        state
                            .runs
                            .get_mut(run_handle)
                            .expect("run exists while processing")
                            .steps
                            .pop_front();
                        continue;
                    }
                    Some(
                        MockScriptStep::Approval { request_id, .. }
                        | MockScriptStep::UserInput { request_id, .. },
                    ) => {
                        let request =
                            ProviderRequestHandle::new(run_handle.clone(), request_id.clone());
                        if state.requests.contains_key(&request) {
                            return Err(HarnessError::Conflict(format!(
                                "duplicate provider request id: {request}"
                            )));
                        }
                    }
                    _ => {}
                }
                step
            };
            let Some(step) = step else {
                return Ok(());
            };
            #[cfg(test)]
            self.note_delivery_attempt();
            let permit = self.events.reserve().await?;
            let mut state = self.state.lock().await;
            let run = state.runs.get(run_handle).ok_or(HarnessError::NotFound {
                entity: "run",
                id: run_handle.to_string(),
            })?;
            if !run.active {
                return Ok(());
            }
            if run.pending.is_some() || run.steps.front() != Some(&step) {
                continue;
            }
            if let MockScriptStep::Approval { request_id, .. }
            | MockScriptStep::UserInput { request_id, .. } = &step
            {
                let request = ProviderRequestHandle::new(run_handle.clone(), request_id.clone());
                if state.requests.contains_key(&request) {
                    return Err(HarnessError::Conflict(format!(
                        "duplicate provider request id: {request}"
                    )));
                }
            }
            let pending = match step {
                MockScriptStep::Message { chunk, native } => PendingDelivery {
                    event: HarnessEvent::MessageDelta {
                        run: run_handle.clone(),
                        chunk,
                        native: self.native(native),
                    },
                    commit: DeliveryCommit::Step {
                        terminal_after: None,
                    },
                },
                MockScriptStep::MessageCompleted { text, native } => PendingDelivery {
                    event: HarnessEvent::MessageCompleted {
                        run: run_handle.clone(),
                        text,
                        native: self.native(native),
                    },
                    commit: DeliveryCommit::Step {
                        terminal_after: None,
                    },
                },
                MockScriptStep::Approval {
                    request_id,
                    summary,
                    native,
                } => {
                    let request = ProviderRequestHandle::new(run_handle.clone(), request_id);
                    state.requests.insert(
                        request.clone(),
                        RequestRecord {
                            run: run_handle.clone(),
                            kind: RequestKind::Approval,
                            response: None,
                        },
                    );
                    PendingDelivery {
                        event: HarnessEvent::ApprovalRequested {
                            request,
                            summary,
                            native: self.native(native),
                        },
                        commit: DeliveryCommit::Step {
                            terminal_after: None,
                        },
                    }
                }
                MockScriptStep::UserInput {
                    request_id,
                    prompt,
                    native,
                } => {
                    let request = ProviderRequestHandle::new(run_handle.clone(), request_id);
                    state.requests.insert(
                        request.clone(),
                        RequestRecord {
                            run: run_handle.clone(),
                            kind: RequestKind::UserInput,
                            response: None,
                        },
                    );
                    PendingDelivery {
                        event: HarnessEvent::UserInputRequested {
                            request,
                            prompt,
                            native: self.native(native),
                        },
                        commit: DeliveryCommit::Step {
                            terminal_after: None,
                        },
                    }
                }
                MockScriptStep::AwaitResponse(_) => unreachable!("await steps do not emit"),
                MockScriptStep::FileMutation {
                    path,
                    summary,
                    native,
                } => {
                    let terminal_after = if state.runs.get(run_handle).is_some_and(|run| {
                        run.fault == Some(MockHarnessFault::ExitAfterFileMutation)
                    }) {
                        state
                            .runs
                            .get_mut(run_handle)
                            .expect("run exists while processing")
                            .fault = None;
                        Some(PendingTerminal {
                            state: HarnessRunTerminal::Crashed {
                                diagnostic: SanitizedDiagnostic::sanitized(
                                    "mock runtime exited after file mutation",
                                ),
                            },
                            native: r#"{"type":"runtime.exit"}"#.to_owned(),
                        })
                    } else {
                        None
                    };
                    PendingDelivery {
                        event: HarnessEvent::FileMutation {
                            run: run_handle.clone(),
                            path,
                            summary,
                            native: self.native(native),
                        },
                        commit: DeliveryCommit::Step { terminal_after },
                    }
                }
                MockScriptStep::CommandOutput {
                    command,
                    chunk,
                    native,
                } => PendingDelivery {
                    event: HarnessEvent::CommandOutput {
                        run: run_handle.clone(),
                        command,
                        chunk,
                        native: self.native(native),
                    },
                    commit: DeliveryCommit::Step {
                        terminal_after: None,
                    },
                },
                MockScriptStep::Complete { native } => self.terminal_delivery(
                    run_handle.clone(),
                    HarnessRunTerminal::Completed,
                    native,
                    true,
                ),
                MockScriptStep::Unknown { item_type, native } => PendingDelivery {
                    event: HarnessEvent::Unknown {
                        run: Some(run_handle.clone()),
                        item_type,
                        native: self.native(native),
                    },
                    commit: DeliveryCommit::Step {
                        terminal_after: None,
                    },
                },
                MockScriptStep::Malformed { item_type, native } => PendingDelivery {
                    event: HarnessEvent::MalformedNativePayload {
                        run: Some(run_handle.clone()),
                        item_type,
                        native: self.native(native),
                    },
                    commit: DeliveryCommit::Step {
                        terminal_after: None,
                    },
                },
            };
            if self.commit_pending(&mut state, run_handle, pending, permit) == FlushResult::Terminal
            {
                return Ok(());
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
        runtime: &RuntimeHandle,
    ) -> Result<HarnessCapabilities, HarnessError> {
        self.check_runtime(runtime).await
    }

    async fn list_sessions(
        &self,
        runtime: &RuntimeHandle,
        query: SessionQuery,
    ) -> Result<Page<ProviderSessionSummary>, HarnessError> {
        self.check_runtime(runtime).await?;
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
        self.check_runtime(runtime).await?;
        if spec.title.trim().is_empty() {
            return Err(HarnessError::InvalidInput(
                "session requires a title".to_owned(),
            ));
        }
        let mut state = self.state.lock().await;
        let connection_id = *state
            .runtime_connections
            .get(runtime)
            .expect("checked runtime remains registered");
        let sequence = state.next_session.entry(runtime.clone()).or_insert(1);
        let id = ProviderSessionId::new(format!("session-{sequence:04}"))?;
        *sequence += 1;
        let session = ProviderSession {
            id,
            runtime: runtime.clone(),
            connection_id,
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
        self.check_runtime(runtime).await?;
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
        self.check_runtime(&session.runtime).await?;
        {
            let state = self.state.lock().await;
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
            self.reject_immediate_run_fault(state.plans.front().and_then(|plan| plan.fault))?;
        }
        #[cfg(test)]
        self.note_acceptance_reserve();
        let acceptance = self.events.reserve().await?;
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
            self.reject_immediate_run_fault(state.plans.front().and_then(|plan| plan.fault))?;
            let plan = state
                .plans
                .pop_front()
                .unwrap_or_else(|| MockRunPlan::new(vec![MockScriptStep::complete()]));
            let key = (session.runtime.clone(), session.id.clone());
            let sequence = state.next_run.entry(key).or_insert(1);
            let run_id = ProviderRunId::new(format!("run-{sequence:04}"))?;
            *sequence += 1;
            let run_handle =
                ProviderRunHandle::new(session.runtime.clone(), session.id.clone(), run_id);
            let fault = plan.fault;
            let pending = match fault {
                Some(MockHarnessFault::ExitAfterRunAccepted) => Some(self.terminal_delivery(
                    run_handle.clone(),
                    HarnessRunTerminal::Crashed {
                        diagnostic: SanitizedDiagnostic::sanitized(
                            "mock runtime exited after accepting run",
                        ),
                    },
                    r#"{"type":"runtime.exit"}"#,
                    false,
                )),
                Some(MockHarnessFault::EmitUnknownEvent) => Some(PendingDelivery {
                    event: HarnessEvent::Unknown {
                        run: Some(run_handle.clone()),
                        item_type: "mock.future-item".to_owned(),
                        native: self.native(r#"{"type":"mock.future-item","answer":42}"#),
                    },
                    commit: DeliveryCommit::Standalone,
                }),
                Some(MockHarnessFault::EmitMalformedNativePayload) => Some(PendingDelivery {
                    event: HarnessEvent::MalformedNativePayload {
                        run: Some(run_handle.clone()),
                        item_type: "mock.malformed".to_owned(),
                        native: self.native("{not-json"),
                    },
                    commit: DeliveryCommit::Standalone,
                }),
                _ => None,
            };
            state.runs.insert(
                run_handle.clone(),
                RunRecord {
                    active: true,
                    delayed: false,
                    steps: plan.steps,
                    fault,
                    pending,
                },
            );
            acceptance.send(HarnessEvent::RunAccepted {
                run: run_handle.clone(),
                native: self.native(r#"{"type":"run.accepted"}"#),
            });
            (run_handle, fault)
        };
        match fault {
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
        loop {
            match self.flush_pending(run).await? {
                FlushResult::Terminal => {
                    return Err(HarnessError::Conflict(format!("run is not active: {run}")));
                }
                FlushResult::Changed | FlushResult::Delivered => continue,
                FlushResult::None => {}
            }
            {
                let state = self.state.lock().await;
                if !state.runs.get(run).is_some_and(|record| record.active) {
                    return Err(HarnessError::Conflict(format!("run is not active: {run}")));
                }
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
            let permit = self.events.reserve().await?;
            let state = self.state.lock().await;
            let Some(record) = state.runs.get(run) else {
                return Err(HarnessError::NotFound {
                    entity: "run",
                    id: run.to_string(),
                });
            };
            if !record.active {
                return Err(HarnessError::Conflict(format!("run is not active: {run}")));
            }
            if record.pending.is_some() {
                continue;
            }
            permit.send(HarnessEvent::MessageDelta {
                run: run.clone(),
                chunk: input.as_str().to_owned(),
                native: self.native(r#"{"type":"run.steer"}"#),
            });
            return Ok(());
        }
    }

    async fn interrupt(&self, run: &ProviderRunHandle) -> Result<(), HarnessError> {
        loop {
            match self.flush_pending(run).await? {
                FlushResult::Terminal => return Ok(()),
                FlushResult::Changed | FlushResult::Delivered => continue,
                FlushResult::None => {}
            }
            {
                let state = self.state.lock().await;
                let record = state.runs.get(run).ok_or_else(|| HarnessError::NotFound {
                    entity: "run",
                    id: run.to_string(),
                })?;
                if !record.active {
                    return Err(HarnessError::Conflict(format!("run is not active: {run}")));
                }
            }
            #[cfg(test)]
            self.note_delivery_attempt();
            let permit = self.events.reserve().await?;
            let mut state = self.state.lock().await;
            let record = state.runs.get(run).ok_or_else(|| HarnessError::NotFound {
                entity: "run",
                id: run.to_string(),
            })?;
            if !record.active {
                return Err(HarnessError::Conflict(format!("run is not active: {run}")));
            }
            if record.pending.is_some() {
                continue;
            }
            let terminal = self.terminal_delivery(
                run.clone(),
                HarnessRunTerminal::Interrupted,
                r#"{"type":"run.interrupted"}"#,
                false,
            );
            self.commit_pending(&mut state, run, terminal, permit);
            return Ok(());
        }
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
            if !matches!(
                (&record.kind, &response),
                (RequestKind::Approval, ProviderResponse::Approval(_))
                    | (RequestKind::UserInput, ProviderResponse::UserInput(_))
            ) {
                return Err(HarnessError::Conflict(format!(
                    "response kind does not match provider request: {request}"
                )));
            }
            if let Some(existing) = &record.response {
                if existing != &response {
                    return Err(HarnessError::Conflict(format!(
                        "provider request already answered differently: {request}"
                    )));
                }
            } else {
                record.response = Some(response);
            }
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

    fn configured_mock(plans: Vec<MockRunPlan>) -> MockHarness {
        MockHarness::new(HarnessCapabilities::default(), plans, None).with_runtime(
            RuntimeHandle::new("runtime-a").unwrap(),
            "0193f26e-7a72-7000-8000-00000000aaa1".parse().unwrap(),
            None,
            Vec::new(),
        )
    }

    async fn test_session(mock: &MockHarness) -> ProviderSession {
        mock.start_session(
            &RuntimeHandle::new("runtime-a").unwrap(),
            StartSessionSpec {
                working_directory: RuntimePath::new("runtime-a://workspace").unwrap(),
                title: "test".to_owned(),
            },
        )
        .await
        .unwrap()
    }

    async fn saturated_mock(next_plan: MockRunPlan) -> (MockHarness, ProviderSession) {
        let mock = configured_mock(vec![
            MockRunPlan::new(Vec::new()).with_fault(MockHarnessFault::NeverComplete),
            next_plan,
        ]);
        let session = test_session(&mock).await;
        let run = mock
            .start_run(
                &session,
                HarnessInput::new("fill").unwrap(),
                RunOptions::default(),
            )
            .await
            .unwrap();
        for index in 1..yakshed_harness::EVENT_BUFFER_CAPACITY {
            mock.steer(&run, HarnessInput::new(format!("fill-{index}")).unwrap())
                .await
                .unwrap();
        }
        (mock, session)
    }

    #[tokio::test]
    async fn saturated_stream_stale_session_fails_before_reserving_capacity() {
        let (mock, mut stale_session) =
            saturated_mock(MockRunPlan::new(vec![MockScriptStep::complete()])).await;
        stale_session.id = ProviderSessionId::new("missing-session").unwrap();
        let result = tokio::time::timeout(
            std::time::Duration::from_millis(250),
            mock.start_run(
                &stale_session,
                HarnessInput::new("stale").unwrap(),
                RunOptions::default(),
            ),
        )
        .await
        .expect("stale session validation waited for event capacity");
        assert!(matches!(result, Err(HarnessError::NotFound { .. })));
    }

    #[tokio::test]
    async fn saturated_stream_rejecting_plan_fails_before_reserving_capacity() {
        let (mock, session) =
            saturated_mock(MockRunPlan::new(Vec::new()).with_fault(MockHarnessFault::Overloaded))
                .await;
        let result = tokio::time::timeout(
            std::time::Duration::from_millis(250),
            mock.start_run(
                &session,
                HarnessInput::new("reject").unwrap(),
                RunOptions::default(),
            ),
        )
        .await
        .expect("rejecting plan waited for event capacity");
        assert_eq!(result, Err(HarnessError::Overloaded));
        assert_eq!(mock.state.lock().await.plans.len(), 1);
    }

    #[tokio::test]
    async fn acceptance_and_staged_delivery_share_capacity_without_lock_inversion() {
        let mock = configured_mock(vec![
            MockRunPlan::new(Vec::new()).with_fault(MockHarnessFault::NeverComplete),
            MockRunPlan::new(Vec::new()).with_fault(MockHarnessFault::ExitAfterRunAccepted),
            MockRunPlan::new(Vec::new()).with_fault(MockHarnessFault::NeverComplete),
        ]);
        let session = test_session(&mock).await;
        let mut stream = mock.subscribe().unwrap();
        let filler = mock
            .start_run(
                &session,
                HarnessInput::new("filler").unwrap(),
                RunOptions::default(),
            )
            .await
            .unwrap();
        for index in 1..yakshed_harness::EVENT_BUFFER_CAPACITY - 1 {
            mock.steer(&filler, HarnessInput::new(format!("fill-{index}")).unwrap())
                .await
                .unwrap();
        }

        let terminal_reserve = mock.probe_delivery(1);
        let mut crashing = Box::pin(mock.start_run(
            &session,
            HarnessInput::new("crash").unwrap(),
            RunOptions::default(),
        ));
        tokio::select! {
            biased;
            result = &mut crashing => panic!("crash terminal did not backpressure: {result:?}"),
            result = terminal_reserve => result.unwrap(),
        }
        drop(crashing);
        let crash_run = mock
            .state
            .lock()
            .await
            .runs
            .keys()
            .find(|run| **run != filler)
            .expect("accepted crash run exists")
            .clone();

        let acceptance_reserve = mock.probe_acceptance_reserve();
        let mut accepting = Box::pin(mock.start_run(
            &session,
            HarnessInput::new("accepted").unwrap(),
            RunOptions::default(),
        ));
        tokio::select! {
            biased;
            result = &mut accepting => panic!("acceptance did not backpressure: {result:?}"),
            result = acceptance_reserve => result.unwrap(),
        }
        let delivery_reserve = mock.probe_delivery(1);
        let mut flushing = Box::pin(mock.process_run(&crash_run));
        tokio::select! {
            biased;
            result = &mut flushing => panic!("staged delivery did not backpressure: {result:?}"),
            result = delivery_reserve => result.unwrap(),
        }

        stream.recv().await.unwrap();
        tokio::time::timeout(std::time::Duration::from_millis(250), &mut accepting)
            .await
            .expect("acceptance waited on the state lock after capacity was freed")
            .unwrap();
        stream.recv().await.unwrap();
        tokio::time::timeout(std::time::Duration::from_millis(250), &mut flushing)
            .await
            .expect("staged delivery did not progress after acceptance released the lock")
            .unwrap();
        assert!(!mock.state.lock().await.runs[&crash_run].active);
    }

    #[tokio::test]
    async fn cancelled_backpressured_drive_replays_staged_step_without_loss_or_duplication() {
        let steps = (0..yakshed_harness::EVENT_BUFFER_CAPACITY)
            .map(|index| MockScriptStep::message(format!("chunk-{index}")))
            .chain(std::iter::once(MockScriptStep::complete()))
            .collect();
        let mock = configured_mock(vec![MockRunPlan::new(steps)]);
        let session = test_session(&mock).await;
        let mut stream = mock.subscribe().unwrap();
        let reached_backpressure = mock.probe_delivery(yakshed_harness::EVENT_BUFFER_CAPACITY);
        let mut start = Box::pin(mock.start_run(
            &session,
            HarnessInput::new("fill").unwrap(),
            RunOptions::default(),
        ));
        tokio::select! {
            biased;
            result = &mut start => panic!("drive completed before cancellation: {result:?}"),
            result = reached_backpressure => result.unwrap(),
        }
        drop(start);

        let run = ProviderRunHandle::new(
            session.runtime.clone(),
            session.id.clone(),
            ProviderRunId::new("run-0001").unwrap(),
        );
        let mut chunks = Vec::new();
        for _ in 0..yakshed_harness::EVENT_BUFFER_CAPACITY {
            if let Some(HarnessEvent::MessageDelta { chunk, .. }) = stream.recv().await {
                chunks.push(chunk);
            }
        }
        mock.process_run(&run).await.unwrap();
        loop {
            match stream.recv().await.unwrap() {
                HarnessEvent::MessageDelta { chunk, .. } => chunks.push(chunk),
                HarnessEvent::RunTerminal {
                    state: HarnessRunTerminal::Completed,
                    ..
                } => break,
                event => panic!("unexpected event after re-drive: {event:?}"),
            }
        }
        assert_eq!(
            chunks,
            (0..yakshed_harness::EVENT_BUFFER_CAPACITY)
                .map(|index| format!("chunk-{index}"))
                .collect::<Vec<_>>()
        );
    }

    #[tokio::test]
    async fn cancelled_backpressured_acceptance_leaves_no_orphaned_run() {
        let mock = configured_mock(vec![
            MockRunPlan::new(Vec::new()).with_fault(MockHarnessFault::NeverComplete),
            MockRunPlan::new(vec![MockScriptStep::complete()]),
        ]);
        let session = test_session(&mock).await;
        let mut stream = mock.subscribe().unwrap();
        let first_run = mock
            .start_run(
                &session,
                HarnessInput::new("first").unwrap(),
                RunOptions::default(),
            )
            .await
            .unwrap();
        for index in 1..yakshed_harness::EVENT_BUFFER_CAPACITY {
            mock.steer(
                &first_run,
                HarnessInput::new(format!("fill-{index}")).unwrap(),
            )
            .await
            .unwrap();
        }

        let reached_reserve = mock.probe_acceptance_reserve();
        let mut cancelled = Box::pin(mock.start_run(
            &session,
            HarnessInput::new("cancelled").unwrap(),
            RunOptions::default(),
        ));
        tokio::select! {
            biased;
            result = &mut cancelled => panic!("acceptance completed before cancellation: {result:?}"),
            result = reached_reserve => result.unwrap(),
        }
        drop(cancelled);
        {
            let state = mock.state.lock().await;
            assert_eq!(state.runs.len(), 1);
            assert_eq!(state.plans.len(), 1);
        }
        for _ in 0..yakshed_harness::EVENT_BUFFER_CAPACITY {
            stream.recv().await.unwrap();
        }

        let new_run = mock
            .start_run(
                &session,
                HarnessInput::new("retry").unwrap(),
                RunOptions::default(),
            )
            .await
            .unwrap();
        assert!(matches!(
            stream.recv().await,
            Some(HarnessEvent::RunAccepted { run, .. }) if run == new_run
        ));
        assert!(matches!(
            stream.recv().await,
            Some(HarnessEvent::RunTerminal { run, .. }) if run == new_run
        ));
        assert_eq!(mock.state.lock().await.runs.len(), 2);
    }

    #[tokio::test]
    async fn cancelled_response_drive_resumes_when_the_same_response_is_retried() {
        let request_id = ProviderRequestId::new("request-0001").unwrap();
        let steps = std::iter::once(MockScriptStep::approval(request_id.clone(), "approve"))
            .chain(std::iter::once(MockScriptStep::await_response(request_id)))
            .chain(
                (0..yakshed_harness::EVENT_BUFFER_CAPACITY)
                    .map(|index| MockScriptStep::message(format!("chunk-{index}"))),
            )
            .chain(std::iter::once(MockScriptStep::complete()))
            .collect();
        let mock = configured_mock(vec![MockRunPlan::new(steps)]);
        let session = test_session(&mock).await;
        let mut stream = mock.subscribe().unwrap();
        mock.start_run(
            &session,
            HarnessInput::new("start").unwrap(),
            RunOptions::default(),
        )
        .await
        .unwrap();
        stream.recv().await.unwrap();
        let request = match stream.recv().await.unwrap() {
            HarnessEvent::ApprovalRequested { request, .. } => request,
            event => panic!("expected approval, got {event:?}"),
        };

        let response = ProviderResponse::Approval(yakshed_domain::ApprovalDecision::Approved);
        let reached_backpressure = mock.probe_delivery(yakshed_harness::EVENT_BUFFER_CAPACITY + 1);
        let mut respond = Box::pin(mock.respond_to_request(request.clone(), response.clone()));
        tokio::select! {
            biased;
            result = &mut respond => panic!("response drive completed before cancellation: {result:?}"),
            result = reached_backpressure => result.unwrap(),
        }
        drop(respond);
        for _ in 0..yakshed_harness::EVENT_BUFFER_CAPACITY {
            stream.recv().await.unwrap();
        }

        mock.respond_to_request(request, response).await.unwrap();
        assert!(matches!(
            stream.recv().await,
            Some(HarnessEvent::RunTerminal {
                state: HarnessRunTerminal::Completed,
                ..
            })
        ));
    }

    #[tokio::test]
    async fn cancelled_backpressured_interrupt_retries_one_terminal_event() {
        let mock = configured_mock(vec![
            MockRunPlan::new(Vec::new()).with_fault(MockHarnessFault::NeverComplete),
        ]);
        let session = test_session(&mock).await;
        let mut stream = mock.subscribe().unwrap();
        let run = mock
            .start_run(
                &session,
                HarnessInput::new("start").unwrap(),
                RunOptions::default(),
            )
            .await
            .unwrap();
        for index in 1..yakshed_harness::EVENT_BUFFER_CAPACITY {
            mock.steer(&run, HarnessInput::new(format!("steer-{index}")).unwrap())
                .await
                .unwrap();
        }

        let reached_backpressure = mock.probe_delivery(1);
        let mut interrupt = Box::pin(mock.interrupt(&run));
        tokio::select! {
            biased;
            result = &mut interrupt => panic!("interrupt completed before cancellation: {result:?}"),
            result = reached_backpressure => result.unwrap(),
        }
        drop(interrupt);
        for _ in 0..yakshed_harness::EVENT_BUFFER_CAPACITY {
            stream.recv().await.unwrap();
        }

        mock.interrupt(&run).await.unwrap();
        assert!(matches!(
            stream.recv().await,
            Some(HarnessEvent::RunTerminal {
                state: HarnessRunTerminal::Interrupted,
                ..
            })
        ));
        assert!(matches!(
            mock.interrupt(&run).await,
            Err(HarnessError::Conflict(_))
        ));
    }

    #[tokio::test]
    async fn steer_revalidates_after_concurrent_interrupt_before_emitting() {
        let mock = Arc::new(
            MockHarness::new(
                HarnessCapabilities::default(),
                vec![MockRunPlan::new(Vec::new()).with_fault(MockHarnessFault::NeverComplete)],
                None,
            )
            .with_runtime(
                RuntimeHandle::new("runtime-a").unwrap(),
                "0193f26e-7a72-7000-8000-00000000aaa1".parse().unwrap(),
                None,
                Vec::new(),
            ),
        );
        let runtime = RuntimeHandle::new("runtime-a").unwrap();
        let session = mock
            .start_session(
                &runtime,
                StartSessionSpec {
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
        mock.interrupt(&run).await.unwrap();
        resume.send(()).unwrap();
        assert!(matches!(
            steer.await.unwrap(),
            Err(HarnessError::Conflict(_))
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
