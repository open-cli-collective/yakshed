use std::{
    collections::HashMap,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
};

use async_trait::async_trait;
use thiserror::Error;
use tokio::sync::{broadcast, mpsc};
use yakshed_domain::{
    ApprovalDecision, ApprovalRequestId, ConnectionId, NamespacedProviderId, RunId, RunSnapshot,
    RunStatus, StreamCursor, TimelineItemId, WorkItemId,
};

use crate::{
    AppStore, BeginApprovalResponse, Clock, ConfirmApprovalResponse, CreateRun, IdGenerator,
    NewTimelineItem, PendingApproval, StoreError, TimelineBatch, TransitionRun,
};

pub const APP_EVENT_CAPACITY: usize = 128;
pub const DELTA_BATCH_CHUNKS: usize = 32;

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct ProviderRunRef {
    pub namespace: String,
    pub native_id: String,
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct ProviderRequestRef {
    pub run: ProviderRunRef,
    pub native_id: String,
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct ProviderCommandRef {
    pub run: ProviderRunRef,
    pub native_id: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum RunTerminal {
    Completed,
    Failed { diagnostic: String },
    Interrupted,
    Crashed { diagnostic: String },
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum RunHarnessEvent {
    RunAccepted {
        run: ProviderRunRef,
    },
    MessageDelta {
        run: ProviderRunRef,
        chunk: String,
    },
    MessageCompleted {
        run: ProviderRunRef,
        text: String,
    },
    ApprovalRequested {
        request: ProviderRequestRef,
        summary: String,
    },
    UserInputRequested {
        request: ProviderRequestRef,
        prompt: String,
    },
    FileMutation {
        run: ProviderRunRef,
        path: String,
        summary: String,
    },
    CommandOutputDelta {
        run: ProviderRunRef,
        command: ProviderCommandRef,
        command_text: String,
        chunk: String,
    },
    CommandOutputCompleted {
        run: ProviderRunRef,
        command: ProviderCommandRef,
        command_text: String,
        output: String,
    },
    RunTerminal {
        run: ProviderRunRef,
        state: RunTerminal,
    },
    Unknown {
        run: Option<ProviderRunRef>,
        item_type: String,
        native: String,
    },
    Malformed {
        run: Option<ProviderRunRef>,
        item_type: String,
        native: String,
    },
}

impl RunHarnessEvent {
    fn run(&self) -> Option<&ProviderRunRef> {
        match self {
            Self::RunAccepted { run }
            | Self::MessageDelta { run, .. }
            | Self::MessageCompleted { run, .. }
            | Self::FileMutation { run, .. }
            | Self::CommandOutputDelta { run, .. }
            | Self::CommandOutputCompleted { run, .. }
            | Self::RunTerminal { run, .. } => Some(run),
            Self::ApprovalRequested { request, .. } | Self::UserInputRequested { request, .. } => {
                Some(&request.run)
            }
            Self::Unknown { run, .. } | Self::Malformed { run, .. } => run.as_ref(),
        }
    }
}

#[derive(Clone, Debug, Error, Eq, PartialEq)]
pub enum HarnessPortError {
    #[error("invalid input: {0}")]
    InvalidInput(String),
    #[error("not found: {0}")]
    NotFound(String),
    #[error("conflict: {0}")]
    Conflict(String),
    #[error("unsupported: {0}")]
    Unsupported(String),
    #[error("runtime overloaded")]
    Overloaded,
    #[error("runtime disconnected")]
    Disconnected,
    #[error("outcome unknown for {operation}")]
    OutcomeUnknown { operation: &'static str },
    #[error("event stream closed")]
    Closed,
    #[error("protocol failure: {0}")]
    Protocol(String),
    #[error("transport failure: {0}")]
    Transport(String),
    #[error("runtime failure: {0}")]
    Runtime(String),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum HarnessResponse {
    Approval(ApprovalDecision),
    UserInput(String),
}

#[async_trait]
pub trait RunHarness: Send + Sync {
    async fn start_run(
        &self,
        connection_id: ConnectionId,
        input: String,
    ) -> Result<ProviderRunRef, HarnessPortError>;
    async fn steer(&self, run: &ProviderRunRef, input: String) -> Result<(), HarnessPortError>;
    async fn interrupt(&self, run: &ProviderRunRef) -> Result<(), HarnessPortError>;
    async fn respond(
        &self,
        request: ProviderRequestRef,
        response: HarnessResponse,
    ) -> Result<(), HarnessPortError>;
    async fn next_event(&self) -> Result<Option<RunHarnessEvent>, HarnessPortError>;
}

pub type UserInputRequestId = TimelineItemId;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AppEvent {
    pub work_item_id: WorkItemId,
    pub revision: u64,
    pub kind: AppEventKind,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum AppEventKind {
    WorkItemPatched,
    TimelineBatchAppended {
        run_id: RunId,
        item_count: usize,
    },
    ApprovalOpened {
        run_id: RunId,
        approval_id: ApprovalRequestId,
    },
    ApprovalResolved {
        run_id: RunId,
        approval_id: ApprovalRequestId,
    },
    UserInputOpened {
        run_id: RunId,
        request_id: UserInputRequestId,
        prompt: String,
    },
    UserInputResponded {
        run_id: RunId,
        request_id: UserInputRequestId,
    },
    RunStatusChanged {
        run_id: RunId,
        status: RunStatus,
    },
    RunOutcomeUnknown {
        run_id: RunId,
        operation: &'static str,
    },
}

#[derive(Debug, Error)]
pub enum RunOrchestrationError {
    #[error(transparent)]
    Store(#[from] StoreError),
    #[error(transparent)]
    Harness(#[from] HarnessPortError),
    #[error("run is not active: {0}")]
    RunNotActive(RunId),
    #[error("approval is not pending: {0}")]
    ApprovalNotPending(ApprovalRequestId),
    #[error("user input is not pending: {0}")]
    UserInputNotPending(UserInputRequestId),
    #[error("invalid provider identifier: {0}")]
    InvalidProviderId(String),
}

#[derive(Clone)]
pub struct RunSupervisor(Arc<Inner>);

struct Inner {
    store: Arc<dyn AppStore>,
    harness: Arc<dyn RunHarness>,
    clock: Arc<dyn Clock>,
    ids: Arc<dyn IdGenerator>,
    events: broadcast::Sender<AppEvent>,
    pump_started: AtomicBool,
    state: Mutex<State>,
}

#[derive(Default)]
struct State {
    routes: HashMap<ProviderRunRef, mpsc::UnboundedSender<RunHarnessEvent>>,
    pending_events: HashMap<ProviderRunRef, Vec<RunHarnessEvent>>,
    runs: HashMap<RunId, ProviderRunRef>,
    work_items: HashMap<RunId, WorkItemId>,
    approvals: HashMap<ApprovalRequestId, (RunId, ProviderRequestRef)>,
    user_inputs: HashMap<UserInputRequestId, (RunId, ProviderRequestRef)>,
    revisions: HashMap<WorkItemId, u64>,
}

impl RunSupervisor {
    pub fn new(
        store: Arc<dyn AppStore>,
        harness: Arc<dyn RunHarness>,
        clock: Arc<dyn Clock>,
        ids: Arc<dyn IdGenerator>,
    ) -> Self {
        let (events, _) = broadcast::channel(APP_EVENT_CAPACITY);
        Self(Arc::new(Inner {
            store,
            harness,
            clock,
            ids,
            events,
            pump_started: AtomicBool::new(false),
            state: Mutex::new(State::default()),
        }))
    }

    /// Slow subscribers receive `Lagged`; the bounded channel drops oldest notifications.
    /// Callers recover from the durable snapshots rather than replaying application events.
    pub fn subscribe(&self) -> broadcast::Receiver<AppEvent> {
        self.0.events.subscribe()
    }

    pub async fn start_run(
        &self,
        work_item_id: WorkItemId,
        connection_id: ConnectionId,
        input: String,
    ) -> Result<RunSnapshot, RunOrchestrationError> {
        let run_id = self.0.ids.next_run_id();
        let run = self
            .0
            .store
            .create_run(CreateRun {
                id: run_id,
                connection_id,
                work_item_id,
                provider_run: None,
            })
            .await?;
        self.0
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .work_items
            .insert(run_id, work_item_id);
        self.0.publish(work_item_id, AppEventKind::WorkItemPatched);
        self.0.publish(
            work_item_id,
            AppEventKind::RunStatusChanged {
                run_id,
                status: RunStatus::Running,
            },
        );
        self.0.ensure_event_pump();

        let provider_run = match self.0.harness.start_run(connection_id, input).await {
            Ok(handle) => handle,
            Err(error @ HarnessPortError::OutcomeUnknown { operation }) => {
                self.0.publish(
                    work_item_id,
                    AppEventKind::RunOutcomeUnknown { run_id, operation },
                );
                return Err(error.into());
            }
            Err(error) => {
                self.0
                    .transition(run_id, work_item_id, RunStatus::Failed)
                    .await?;
                return Err(error.into());
            }
        };
        let (sender, receiver) = mpsc::unbounded_channel();
        {
            let mut state = self
                .0
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            state.runs.insert(run_id, provider_run.clone());
            state.routes.insert(provider_run.clone(), sender.clone());
            for event in state
                .pending_events
                .remove(&provider_run)
                .unwrap_or_default()
            {
                let _ = sender.send(event);
            }
        }
        let inner = Arc::clone(&self.0);
        tokio::spawn(async move {
            inner
                .consume_run(run_id, work_item_id, connection_id, provider_run, receiver)
                .await
        });
        Ok(run)
    }

    pub async fn steer(&self, run_id: RunId, input: String) -> Result<(), RunOrchestrationError> {
        let (run, work_item_id) = self.0.active_run(run_id)?;
        self.0
            .call_harness(run_id, work_item_id, self.0.harness.steer(&run, input))
            .await
    }

    pub async fn interrupt(&self, run_id: RunId) -> Result<(), RunOrchestrationError> {
        let (run, work_item_id) = self.0.active_run(run_id)?;
        self.0
            .call_harness(run_id, work_item_id, self.0.harness.interrupt(&run))
            .await
    }

    pub async fn resolve_approval(
        &self,
        approval_id: ApprovalRequestId,
        decision: ApprovalDecision,
    ) -> Result<(), RunOrchestrationError> {
        let (run_id, request) = self
            .0
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .approvals
            .get(&approval_id)
            .cloned()
            .ok_or(RunOrchestrationError::ApprovalNotPending(approval_id))?;
        let (_, work_item_id) = self.0.active_run(run_id)?;
        self.0
            .store
            .begin_approval_response(BeginApprovalResponse {
                approval_id,
                decision,
                audit_event_id: self.0.ids.next_audit_event_id(),
            })
            .await?;
        self.0
            .call_harness(
                run_id,
                work_item_id,
                self.0
                    .harness
                    .respond(request, HarnessResponse::Approval(decision)),
            )
            .await?;
        self.0
            .store
            .confirm_approval_response(ConfirmApprovalResponse {
                approval_id,
                audit_event_id: self.0.ids.next_audit_event_id(),
            })
            .await?;
        self.0
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .approvals
            .remove(&approval_id);
        self.0.publish(
            work_item_id,
            AppEventKind::ApprovalResolved {
                run_id,
                approval_id,
            },
        );
        Ok(())
    }

    pub async fn respond_user_input(
        &self,
        request_id: UserInputRequestId,
        response: String,
    ) -> Result<(), RunOrchestrationError> {
        let (run_id, request) = self
            .0
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .user_inputs
            .get(&request_id)
            .cloned()
            .ok_or(RunOrchestrationError::UserInputNotPending(request_id))?;
        let (_, work_item_id) = self.0.active_run(run_id)?;
        self.0
            .call_harness(
                run_id,
                work_item_id,
                self.0
                    .harness
                    .respond(request, HarnessResponse::UserInput(response)),
            )
            .await?;
        self.0
            .append_items(
                run_id,
                work_item_id,
                vec![NewTimelineItem {
                    id: self.0.ids.next_timeline_item_id(),
                    kind: "user_input_responded".to_owned(),
                    body: String::new(),
                    provider_id: None,
                }],
                None,
            )
            .await?;
        self.0
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .user_inputs
            .remove(&request_id);
        self.0.publish(
            work_item_id,
            AppEventKind::UserInputResponded { run_id, request_id },
        );
        Ok(())
    }
}

impl Inner {
    fn ensure_event_pump(self: &Arc<Self>) {
        if self.pump_started.swap(true, Ordering::AcqRel) {
            return;
        }
        let inner = Arc::clone(self);
        tokio::spawn(async move {
            loop {
                match inner.harness.next_event().await {
                    Ok(Some(event)) => inner.route_event(event),
                    Ok(None) | Err(HarnessPortError::Closed) => break,
                    Err(_) => break,
                }
            }
        });
    }

    fn route_event(&self, event: RunHarnessEvent) {
        let Some(run) = event.run().cloned() else {
            return;
        };
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if let Some(route) = state.routes.get(&run) {
            let _ = route.send(event);
        } else {
            state.pending_events.entry(run).or_default().push(event);
        }
    }

    fn active_run(
        &self,
        run_id: RunId,
    ) -> Result<(ProviderRunRef, WorkItemId), RunOrchestrationError> {
        let state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        Ok((
            state
                .runs
                .get(&run_id)
                .cloned()
                .ok_or(RunOrchestrationError::RunNotActive(run_id))?,
            *state
                .work_items
                .get(&run_id)
                .ok_or(RunOrchestrationError::RunNotActive(run_id))?,
        ))
    }

    async fn call_harness<T>(
        &self,
        run_id: RunId,
        work_item_id: WorkItemId,
        call: impl std::future::Future<Output = Result<T, HarnessPortError>>,
    ) -> Result<T, RunOrchestrationError> {
        match call.await {
            Ok(value) => Ok(value),
            Err(error @ HarnessPortError::OutcomeUnknown { operation }) => {
                self.publish(
                    work_item_id,
                    AppEventKind::RunOutcomeUnknown { run_id, operation },
                );
                Err(error.into())
            }
            Err(error) => Err(error.into()),
        }
    }

    fn publish(&self, work_item_id: WorkItemId, kind: AppEventKind) {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let revision = state.revisions.entry(work_item_id).or_default();
        *revision += 1;
        let _ = self.events.send(AppEvent {
            work_item_id,
            revision: *revision,
            kind,
        });
    }

    async fn transition(
        &self,
        run_id: RunId,
        work_item_id: WorkItemId,
        status: RunStatus,
    ) -> Result<(), StoreError> {
        self.store
            .transition_run(TransitionRun {
                run_id,
                expected_current: RunStatus::Running,
                target: status,
                occurred_at: self.clock.now(),
                audit_event_id: self.ids.next_audit_event_id(),
            })
            .await?;
        self.publish(
            work_item_id,
            AppEventKind::RunStatusChanged { run_id, status },
        );
        Ok(())
    }

    async fn append_items(
        &self,
        run_id: RunId,
        work_item_id: WorkItemId,
        items: Vec<NewTimelineItem>,
        stream: Option<&ProviderRunRef>,
    ) -> Result<(), StoreError> {
        if items.is_empty() {
            return Ok(());
        }
        let (source_namespace, stream_id) = stream
            .map(|run| (run.namespace.clone(), run.native_id.clone()))
            .unwrap_or_else(|| ("application".to_owned(), run_id.to_string()));
        let cursor = self
            .store
            .get_stream_cursor(crate::GetStreamCursor {
                connection_id: self.store.get_run(run_id).await?.connection_id,
                run_id,
                source_namespace: source_namespace.clone(),
                stream_id: stream_id.clone(),
            })
            .await?
            .map_or(StreamCursor::INITIAL, |state| state.cursor);
        let item_count = items.len();
        self.store
            .append_timeline_batch(TimelineBatch {
                batch_id: self.ids.next_timeline_batch_id(),
                connection_id: self.store.get_run(run_id).await?.connection_id,
                run_id,
                source_namespace,
                stream_id,
                expected_stream_revision: cursor,
                items,
            })
            .await?;
        self.publish(
            work_item_id,
            AppEventKind::TimelineBatchAppended { run_id, item_count },
        );
        Ok(())
    }

    async fn consume_run(
        self: Arc<Self>,
        run_id: RunId,
        work_item_id: WorkItemId,
        _connection_id: ConnectionId,
        provider_run: ProviderRunRef,
        mut events: mpsc::UnboundedReceiver<RunHarnessEvent>,
    ) {
        let mut deltas = DeltaBuffer::default();
        while let Some(event) = events.recv().await {
            let terminal = matches!(event, RunHarnessEvent::RunTerminal { .. });
            if let Err(error) = self
                .apply_event(run_id, work_item_id, &provider_run, &mut deltas, event)
                .await
            {
                let _ = error;
                break;
            }
            if terminal {
                break;
            }
        }
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        state.routes.remove(&provider_run);
        state.runs.remove(&run_id);
    }

    async fn apply_event(
        &self,
        run_id: RunId,
        work_item_id: WorkItemId,
        provider_run: &ProviderRunRef,
        deltas: &mut DeltaBuffer,
        event: RunHarnessEvent,
    ) -> Result<(), RunOrchestrationError> {
        match event {
            RunHarnessEvent::RunAccepted { .. } => return Ok(()),
            RunHarnessEvent::MessageDelta { chunk, .. } => deltas.push_message(chunk),
            RunHarnessEvent::CommandOutputDelta {
                command,
                command_text,
                chunk,
                ..
            } => deltas.push_command(command, command_text, chunk),
            event => {
                let mut items = deltas.take_items(self.ids.as_ref())?;
                match event {
                    RunHarnessEvent::MessageCompleted { text, .. } => {
                        items.push(self.item("message_completed", text, None));
                    }
                    RunHarnessEvent::CommandOutputCompleted {
                        command,
                        command_text,
                        output,
                        ..
                    } => {
                        items.push(self.item(
                            "command_output_completed",
                            format!("{command_text}\n{output}"),
                            Some(command_provider_id(&command)?),
                        ));
                    }
                    RunHarnessEvent::FileMutation { path, summary, .. } => {
                        items.push(self.item("file_mutation", format!("{path}\n{summary}"), None));
                    }
                    RunHarnessEvent::ApprovalRequested { request, summary } => {
                        if !items.is_empty() {
                            self.append_items(run_id, work_item_id, items, Some(provider_run))
                                .await?;
                        }
                        let approval_id = self.ids.next_approval_request_id();
                        self.store
                            .record_pending_approval(PendingApproval {
                                id: approval_id,
                                run_id,
                                provider_id: request_provider_id(&request)?,
                                kind: "approval".to_owned(),
                                summary,
                            })
                            .await?;
                        self.state
                            .lock()
                            .unwrap_or_else(std::sync::PoisonError::into_inner)
                            .approvals
                            .insert(approval_id, (run_id, request));
                        self.publish(
                            work_item_id,
                            AppEventKind::ApprovalOpened {
                                run_id,
                                approval_id,
                            },
                        );
                        return Ok(());
                    }
                    RunHarnessEvent::UserInputRequested { request, prompt } => {
                        let request_id = self.ids.next_timeline_item_id();
                        items.push(NewTimelineItem {
                            id: request_id,
                            kind: "user_input_requested".to_owned(),
                            body: prompt.clone(),
                            provider_id: Some(request_provider_id(&request)?),
                        });
                        self.state
                            .lock()
                            .unwrap_or_else(std::sync::PoisonError::into_inner)
                            .user_inputs
                            .insert(request_id, (run_id, request));
                        self.append_items(run_id, work_item_id, items, Some(provider_run))
                            .await?;
                        self.publish(
                            work_item_id,
                            AppEventKind::UserInputOpened {
                                run_id,
                                request_id,
                                prompt,
                            },
                        );
                        return Ok(());
                    }
                    RunHarnessEvent::Unknown {
                        item_type, native, ..
                    } => {
                        items.push(self.item(&format!("unknown:{item_type}"), native, None));
                    }
                    RunHarnessEvent::Malformed {
                        item_type, native, ..
                    } => {
                        items.push(self.item(&format!("malformed:{item_type}"), native, None));
                    }
                    RunHarnessEvent::RunTerminal { state, .. } => {
                        if !items.is_empty() {
                            self.append_items(run_id, work_item_id, items, Some(provider_run))
                                .await?;
                        }
                        let status = match state {
                            RunTerminal::Completed => RunStatus::Completed,
                            RunTerminal::Failed { .. } | RunTerminal::Crashed { .. } => {
                                RunStatus::Failed
                            }
                            RunTerminal::Interrupted => RunStatus::Interrupted,
                        };
                        self.transition(run_id, work_item_id, status).await?;
                        return Ok(());
                    }
                    RunHarnessEvent::RunAccepted { .. }
                    | RunHarnessEvent::MessageDelta { .. }
                    | RunHarnessEvent::CommandOutputDelta { .. } => unreachable!(),
                }
                self.append_items(run_id, work_item_id, items, Some(provider_run))
                    .await?;
                return Ok(());
            }
        }
        if deltas.chunks >= DELTA_BATCH_CHUNKS {
            let items = deltas.take_items(self.ids.as_ref())?;
            self.append_items(run_id, work_item_id, items, Some(provider_run))
                .await?;
        }
        Ok(())
    }

    fn item(
        &self,
        kind: &str,
        body: String,
        provider_id: Option<NamespacedProviderId>,
    ) -> NewTimelineItem {
        NewTimelineItem {
            id: self.ids.next_timeline_item_id(),
            kind: kind.to_owned(),
            body,
            provider_id,
        }
    }
}

#[derive(Default)]
struct DeltaBuffer {
    chunks: usize,
    message: String,
    commands: HashMap<ProviderCommandRef, (String, String)>,
}

impl DeltaBuffer {
    fn push_message(&mut self, chunk: String) {
        self.chunks += 1;
        self.message.push_str(&chunk);
    }

    fn push_command(&mut self, command: ProviderCommandRef, command_text: String, chunk: String) {
        self.chunks += 1;
        let entry = self
            .commands
            .entry(command)
            .or_insert((command_text, String::new()));
        entry.1.push_str(&chunk);
    }

    /// Deterministic policy: flush all accumulated deltas at every authoritative event or after
    /// 32 chunks, producing at most one message row and one row per command in a store batch.
    fn take_items(
        &mut self,
        ids: &dyn IdGenerator,
    ) -> Result<Vec<NewTimelineItem>, RunOrchestrationError> {
        self.chunks = 0;
        let mut items = Vec::new();
        if !self.message.is_empty() {
            items.push(NewTimelineItem {
                id: ids.next_timeline_item_id(),
                kind: "message_delta_batch".to_owned(),
                body: std::mem::take(&mut self.message),
                provider_id: None,
            });
        }
        let mut commands: Vec<_> = self.commands.drain().collect();
        commands.sort_by(|(left, _), (right, _)| left.native_id.cmp(&right.native_id));
        for (command, (command_text, output)) in commands {
            items.push(NewTimelineItem {
                id: ids.next_timeline_item_id(),
                kind: "command_output_delta_batch".to_owned(),
                body: format!("{command_text}\n{output}"),
                provider_id: Some(command_provider_id(&command)?),
            });
        }
        Ok(items)
    }
}

fn request_provider_id(
    request: &ProviderRequestRef,
) -> Result<NamespacedProviderId, RunOrchestrationError> {
    NamespacedProviderId::new(
        request.run.namespace.clone(),
        format!("{}/{}", request.run.native_id, request.native_id),
    )
    .map_err(|error| RunOrchestrationError::InvalidProviderId(error.to_string()))
}

fn command_provider_id(
    command: &ProviderCommandRef,
) -> Result<NamespacedProviderId, RunOrchestrationError> {
    NamespacedProviderId::new(
        command.run.namespace.clone(),
        format!("{}/{}", command.run.native_id, command.native_id),
    )
    .map_err(|error| RunOrchestrationError::InvalidProviderId(error.to_string()))
}
