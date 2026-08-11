use std::{
    collections::{HashMap, HashSet, VecDeque},
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
};

use async_trait::async_trait;
use thiserror::Error;
use tokio::sync::{Mutex as AsyncMutex, Notify, broadcast, mpsc};
use yakshed_domain::{
    ApprovalDecision, ApprovalRequestId, ApprovalStatus, ConnectionId, NamespacedProviderId, RunId,
    RunSnapshot, RunStatus, StreamCursor, TimelineItemId, WorkItemId,
};

use crate::{
    AppStore, BeginApprovalResponse, Clock, ConfirmApprovalResponse, CreateRun, IdGenerator,
    NewTimelineItem, PendingApproval, StoreError, TimelineBatch, TransitionRun,
};

pub const APP_EVENT_CAPACITY: usize = 128;
pub const DELTA_BATCH_CHUNKS: usize = 32;
const ROUTE_CHANNEL_CAPACITY: usize = 64;
const MAX_UNCERTAIN_STARTS: usize = 64;
const STREAM_FAILURE_OPERATION: &str = "stream_disconnected";

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct ProviderRunRef(NamespacedProviderId);

impl ProviderRunRef {
    pub fn new(
        namespace: impl Into<String>,
        native_id: impl Into<String>,
    ) -> Result<Self, RunOrchestrationError> {
        NamespacedProviderId::new(namespace, native_id)
            .map(Self)
            .map_err(|error| RunOrchestrationError::InvalidProviderId(error.to_string()))
    }

    pub fn namespace(&self) -> &str {
        self.0.namespace()
    }

    pub fn native_id(&self) -> &str {
        self.0.value()
    }

    fn provider_id(&self) -> NamespacedProviderId {
        self.0.clone()
    }
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
        correlation_id: RunId,
        input: String,
    ) -> Result<ProviderRunRef, HarnessPortError>;
    async fn lookup_run(
        &self,
        _connection_id: ConnectionId,
        _correlation_id: RunId,
    ) -> Result<Option<ProviderRunRef>, HarnessPortError> {
        Ok(None)
    }
    async fn steer(&self, run: &ProviderRunRef, input: String) -> Result<(), HarnessPortError>;
    async fn interrupt(&self, run: &ProviderRunRef) -> Result<(), HarnessPortError>;
    async fn respond(
        &self,
        request: ProviderRequestRef,
        response: HarnessResponse,
    ) -> Result<(), HarnessPortError>;
    async fn next_event(&self) -> Result<Option<RunHarnessEvent>, HarnessPortError>;

    /// Checks whether a persisted provider run can be safely reattached after process restart.
    async fn reconnect(&self, _run: &ProviderRunRef) -> Result<bool, HarnessPortError> {
        Ok(false)
    }
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

#[derive(Clone, Debug, Error)]
pub enum RunOrchestrationError {
    #[error(transparent)]
    Store(#[from] StoreError),
    #[error(transparent)]
    Harness(#[from] HarnessPortError),
    #[error("run is not active: {0}")]
    RunNotActive(RunId),
    #[error("run requires reconciliation: {0}")]
    RunNeedsReconciliation(RunId),
    #[error("approval is not pending: {0}")]
    ApprovalNotPending(ApprovalRequestId),
    #[error("approval decision conflicts with the in-flight response: {0}")]
    ApprovalDecisionConflict(ApprovalRequestId),
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
    pump: Mutex<Option<tokio::task::JoinHandle<()>>>,
    startup_done: AtomicBool,
    startup_error: Mutex<Option<RunOrchestrationError>>,
    startup_notify: Notify,
    start_lock: AsyncMutex<()>,
    state: Mutex<State>,
}

#[derive(Default)]
struct State {
    routes: HashMap<ProviderRunRef, mpsc::Sender<RunHarnessEvent>>,
    pending_events: HashMap<ProviderRunRef, Vec<RunHarnessEvent>>,
    pending_overflow: bool,
    start_in_progress: Option<RunId>,
    uncertain_starts: HashMap<RunId, PendingStart>,
    uncertain_start_order: VecDeque<RunId>,
    runs: HashMap<RunId, ProviderRunRef>,
    runs_by_provider: HashMap<ProviderRunRef, RunId>,
    work_items: HashMap<RunId, WorkItemId>,
    approvals: HashMap<ApprovalRequestId, (RunId, ProviderRequestRef)>,
    user_inputs: HashMap<UserInputRequestId, (RunId, ProviderRequestRef)>,
    approval_responses: HashMap<ApprovalRequestId, ApprovalDecision>,
    uncertain_runs: HashSet<RunId>,
}

#[derive(Default)]
struct PendingStart {
    events: HashMap<ProviderRunRef, Vec<RunHarnessEvent>>,
    overflow: bool,
}

#[derive(Default)]
struct RestoredRequests {
    approvals: HashMap<ApprovalRequestId, (RunId, ProviderRequestRef)>,
    user_inputs: HashMap<UserInputRequestId, (RunId, ProviderRequestRef)>,
    approval_responses: HashMap<ApprovalRequestId, ApprovalDecision>,
}

impl State {
    fn buffer_handshake_event(&mut self, run: ProviderRunRef, event: RunHarnessEvent) {
        if self.start_in_progress.is_some() {
            let pending_count = self.pending_events.values().map(Vec::len).sum::<usize>();
            if pending_count < ROUTE_CHANNEL_CAPACITY {
                self.pending_events.entry(run).or_default().push(event);
            } else {
                self.pending_overflow = true;
            }
            return;
        }
        if let Some(pending) = self
            .uncertain_starts
            .values_mut()
            .find(|pending| pending.events.contains_key(&run))
        {
            let count = pending.events.values().map(Vec::len).sum::<usize>();
            if count < ROUTE_CHANNEL_CAPACITY {
                pending.events.entry(run).or_default().push(event);
            } else {
                pending.overflow = true;
            }
        } else if let Some(correlation_id) = self
            .uncertain_start_order
            .iter()
            .find(|id| {
                self.uncertain_starts
                    .get(id)
                    .is_some_and(|pending| pending.events.is_empty())
            })
            .copied()
            && let Some(pending) = self.uncertain_starts.get_mut(&correlation_id)
        {
            pending.events.insert(run, vec![event]);
        }
    }
}

impl RunSupervisor {
    pub fn new(
        store: Arc<dyn AppStore>,
        harness: Arc<dyn RunHarness>,
        clock: Arc<dyn Clock>,
        ids: Arc<dyn IdGenerator>,
    ) -> Self {
        let (events, _) = broadcast::channel(APP_EVENT_CAPACITY);
        let supervisor = Self(Arc::new(Inner {
            store,
            harness,
            clock,
            ids,
            events,
            pump: Mutex::new(None),
            startup_done: AtomicBool::new(false),
            startup_error: Mutex::new(None),
            startup_notify: Notify::new(),
            start_lock: AsyncMutex::new(()),
            state: Mutex::new(State::default()),
        }));
        let inner = Arc::clone(&supervisor.0);
        tokio::spawn(async move {
            let error = inner.reconcile_startup().await.err();
            if let Some(error) = error {
                *inner
                    .startup_error
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(error);
            }
            inner.startup_done.store(true, Ordering::Release);
            inner.startup_notify.notify_waiters();
        });
        supervisor
    }

    /// Slow subscribers receive `Lagged`; the bounded channel drops oldest notifications.
    /// Callers recover from the durable snapshots rather than replaying application events.
    pub fn subscribe(&self) -> broadcast::Receiver<AppEvent> {
        self.0.events.subscribe()
    }

    pub async fn ready(&self) -> Result<(), RunOrchestrationError> {
        self.0.await_startup().await
    }

    pub async fn start_run(
        &self,
        work_item_id: WorkItemId,
        connection_id: ConnectionId,
        input: String,
    ) -> Result<RunSnapshot, RunOrchestrationError> {
        let supervisor = self.clone();
        tokio::spawn(async move {
            supervisor
                .start_run_owned(work_item_id, connection_id, input)
                .await
        })
        .await
        .map_err(|_| HarnessPortError::Runtime("start task stopped".to_owned()))?
    }

    async fn start_run_owned(
        &self,
        work_item_id: WorkItemId,
        connection_id: ConnectionId,
        input: String,
    ) -> Result<RunSnapshot, RunOrchestrationError> {
        self.0.await_startup().await?;
        let _start = self.0.start_lock.lock().await;
        let run_id = self.0.ids.next_run_id();
        self.0
            .store
            .create_run(CreateRun {
                id: run_id,
                connection_id,
                work_item_id,
                provider_run: None,
            })
            .await?;
        self.0
            .publish(
                work_item_id,
                AppEventKind::RunStatusChanged {
                    run_id,
                    status: RunStatus::Starting,
                },
            )
            .await;
        self.0
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .work_items
            .insert(run_id, work_item_id);
        {
            let mut state = self
                .0
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            state.start_in_progress = Some(run_id);
            state.pending_events.clear();
            state.pending_overflow = false;
        }
        self.0.ensure_event_pump();
        let provider_run = match self.0.harness.start_run(connection_id, run_id, input).await {
            Ok(handle) => handle,
            Err(error @ HarnessPortError::OutcomeUnknown { operation }) => {
                self.0.retain_unknown_handshake(run_id);
                self.0
                    .transition(run_id, RunStatus::Starting, RunStatus::OutcomeUnknown, None)
                    .await?;
                self.0
                    .publish(
                        work_item_id,
                        AppEventKind::RunOutcomeUnknown { run_id, operation },
                    )
                    .await;
                return Err(error.into());
            }
            Err(error) => {
                self.0.finish_start_handshake();
                self.0.close_run(run_id).await;
                self.0
                    .transition(run_id, RunStatus::Starting, RunStatus::Failed, None)
                    .await?;
                return Err(error.into());
            }
        };
        let (sender, receiver) = mpsc::channel(ROUTE_CHANNEL_CAPACITY);
        let provider_id = provider_run.provider_id();
        let run = match self
            .0
            .transition(
                run_id,
                RunStatus::Starting,
                RunStatus::Running,
                Some(provider_id),
            )
            .await
        {
            Ok(run) => run,
            Err(error) => {
                self.0
                    .retain_uncertain_start(run_id, work_item_id, &provider_run)
                    .await;
                return Err(error.into());
            }
        };
        let (pending_events, pending_overflow) = {
            let mut state = self
                .0
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            state.runs.insert(run_id, provider_run.clone());
            state.runs_by_provider.insert(provider_run.clone(), run_id);
            state.routes.insert(provider_run.clone(), sender.clone());
            state.start_in_progress = None;
            let pending = state
                .pending_events
                .remove(&provider_run)
                .unwrap_or_default();
            state.pending_events.clear();
            let overflow = std::mem::take(&mut state.pending_overflow);
            (pending, overflow)
        };
        let inner = Arc::clone(&self.0);
        tokio::spawn(async move {
            inner
                .consume_run(run_id, work_item_id, connection_id, provider_run, receiver)
                .await
        });
        self.0.ensure_event_pump();
        if pending_overflow {
            self.0.handle_route_overflow(run_id, work_item_id).await;
        } else {
            for event in pending_events {
                if sender.try_send(event).is_err() {
                    self.0.handle_route_overflow(run_id, work_item_id).await;
                    break;
                }
            }
        }
        Ok(run)
    }

    pub async fn steer(&self, run_id: RunId, input: String) -> Result<(), RunOrchestrationError> {
        self.0.await_startup().await?;
        let (run, work_item_id) = self.0.active_run(run_id)?;
        self.0
            .call_harness(run_id, work_item_id, self.0.harness.steer(&run, input))
            .await
    }

    pub async fn interrupt(&self, run_id: RunId) -> Result<(), RunOrchestrationError> {
        self.0.await_startup().await?;
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
        self.0.await_startup().await?;
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
        let responding = self
            .0
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .approval_responses
            .get(&approval_id)
            .copied();
        if let Some(existing) = responding {
            if existing != decision {
                return Err(RunOrchestrationError::ApprovalDecisionConflict(approval_id));
            }
        } else {
            self.0
                .store
                .begin_approval_response(BeginApprovalResponse {
                    approval_id,
                    decision,
                    audit_event_id: self.0.ids.next_audit_event_id(),
                })
                .await?;
            self.0
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .approval_responses
                .insert(approval_id, decision);
        }
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
        {
            let mut state = self
                .0
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            state.approvals.remove(&approval_id);
            state.approval_responses.remove(&approval_id);
        }
        self.0
            .publish(
                work_item_id,
                AppEventKind::ApprovalResolved {
                    run_id,
                    approval_id,
                },
            )
            .await;
        Ok(())
    }

    pub async fn respond_user_input(
        &self,
        request_id: UserInputRequestId,
        response: String,
    ) -> Result<(), RunOrchestrationError> {
        self.0.await_startup().await?;
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
                    body: request_id.to_string(),
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
        self.0
            .publish(
                work_item_id,
                AppEventKind::UserInputResponded { run_id, request_id },
            )
            .await;
        Ok(())
    }

    pub async fn reconcile_run(&self, run_id: RunId) -> Result<RunSnapshot, RunOrchestrationError> {
        self.0.await_startup().await?;
        let snapshot = self.0.store.get_run(run_id).await?;
        let provider_run = match snapshot.provider_id.clone() {
            Some(provider_id) => ProviderRunRef(provider_id),
            None => self
                .0
                .harness
                .lookup_run(snapshot.connection_id, run_id)
                .await?
                .ok_or(RunOrchestrationError::RunNeedsReconciliation(run_id))?,
        };
        if !self.0.harness.reconnect(&provider_run).await? {
            return Err(RunOrchestrationError::RunNeedsReconciliation(run_id));
        }
        self.0.reattach_run(snapshot, provider_run).await
    }
}

impl Inner {
    async fn await_startup(&self) -> Result<(), RunOrchestrationError> {
        loop {
            let notified = self.startup_notify.notified();
            if self.startup_done.load(Ordering::Acquire) {
                return self
                    .startup_error
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .clone()
                    .map_or(Ok(()), Err);
            }
            notified.await;
        }
    }

    fn finish_start_handshake(&self) {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        state.start_in_progress = None;
        state.pending_events.clear();
        state.pending_overflow = false;
    }

    fn retain_unknown_handshake(&self, run_id: RunId) {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        state.start_in_progress = None;
        let events = std::mem::take(&mut state.pending_events);
        let overflow = std::mem::take(&mut state.pending_overflow);
        // Bound unresolved correlations globally; evicted runs remain durably OutcomeUnknown.
        if state.uncertain_starts.len() >= MAX_UNCERTAIN_STARTS
            && let Some(evicted) = state.uncertain_start_order.pop_front()
        {
            state.uncertain_starts.remove(&evicted);
        }
        state
            .uncertain_starts
            .insert(run_id, PendingStart { events, overflow });
        state.uncertain_start_order.push_back(run_id);
        state.uncertain_runs.insert(run_id);
    }

    async fn retain_uncertain_start(
        self: &Arc<Self>,
        run_id: RunId,
        work_item_id: WorkItemId,
        provider_run: &ProviderRunRef,
    ) {
        let (pending, overflow) = {
            let mut state = self
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            state.start_in_progress = None;
            let pending = state
                .pending_events
                .remove(provider_run)
                .unwrap_or_default();
            state.pending_events.clear();
            let overflow = std::mem::take(&mut state.pending_overflow);
            (pending, overflow)
        };
        let _ = self.harness.interrupt(provider_run).await;
        let _ = self
            .transition(
                run_id,
                RunStatus::Starting,
                RunStatus::OutcomeUnknown,
                Some(provider_run.provider_id()),
            )
            .await;
        let _ = self
            .attach_route(run_id, work_item_id, provider_run.clone())
            .await;
        if let Some(route) = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .routes
            .get(provider_run)
            .cloned()
        {
            for event in pending {
                if route.try_send(event).is_err() {
                    break;
                }
            }
        }
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .uncertain_runs
            .insert(run_id);
        self.publish(
            work_item_id,
            AppEventKind::RunOutcomeUnknown {
                run_id,
                operation: "start_binding_failed",
            },
        )
        .await;
        if overflow {
            self.handle_route_overflow(run_id, work_item_id).await;
        }
    }

    async fn reconcile_startup(self: &Arc<Self>) -> Result<(), RunOrchestrationError> {
        let mut after = None;
        loop {
            let page = match self
                .store
                .list_runs_needing_reconciliation(after, 200)
                .await
            {
                Ok(page) => page,
                Err(error) => return Err(error.into()),
            };
            for run in page.items {
                let Some(provider_id) = run.provider_id.clone() else {
                    if matches!(run.status, RunStatus::Starting | RunStatus::Running) {
                        let target = if run.status == RunStatus::Starting {
                            RunStatus::OutcomeUnknown
                        } else {
                            RunStatus::Disconnected
                        };
                        self.transition(run.id, run.status, target, None).await?;
                    }
                    continue;
                };
                let provider_run = ProviderRunRef(provider_id);
                if self.harness.reconnect(&provider_run).await? {
                    self.reattach_run(run, provider_run).await?;
                } else if run.status == RunStatus::Starting {
                    self.transition(run.id, RunStatus::Starting, RunStatus::OutcomeUnknown, None)
                        .await?;
                } else if run.status == RunStatus::Running {
                    self.transition(run.id, RunStatus::Running, RunStatus::Disconnected, None)
                        .await?;
                }
            }
            after = page.next_after;
            if after.is_none() {
                break;
            }
        }
        Ok(())
    }

    async fn attach_route(
        self: &Arc<Self>,
        run_id: RunId,
        work_item_id: WorkItemId,
        provider_run: ProviderRunRef,
    ) -> Result<(), StoreError> {
        let connection_id = self.store.get_run(run_id).await?.connection_id;
        let receiver = self.prepare_route(
            run_id,
            work_item_id,
            provider_run.clone(),
            RestoredRequests::default(),
        );
        self.activate_route(run_id, work_item_id, connection_id, provider_run, receiver);
        Ok(())
    }

    fn prepare_route(
        self: &Arc<Self>,
        run_id: RunId,
        work_item_id: WorkItemId,
        provider_run: ProviderRunRef,
        restored: RestoredRequests,
    ) -> mpsc::Receiver<RunHarnessEvent> {
        let (sender, receiver) = mpsc::channel(ROUTE_CHANNEL_CAPACITY);
        {
            let mut state = self
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            state.runs.insert(run_id, provider_run.clone());
            state.runs_by_provider.insert(provider_run.clone(), run_id);
            state.work_items.insert(run_id, work_item_id);
            state.routes.insert(provider_run.clone(), sender);
            state.approvals.extend(restored.approvals);
            state.user_inputs.extend(restored.user_inputs);
            state.approval_responses.extend(restored.approval_responses);
            state.uncertain_runs.insert(run_id);
        }
        receiver
    }

    fn activate_route(
        self: &Arc<Self>,
        run_id: RunId,
        work_item_id: WorkItemId,
        connection_id: ConnectionId,
        provider_run: ProviderRunRef,
        receiver: mpsc::Receiver<RunHarnessEvent>,
    ) {
        let inner = Arc::clone(self);
        tokio::spawn(async move {
            inner
                .consume_run(run_id, work_item_id, connection_id, provider_run, receiver)
                .await;
        });
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .uncertain_runs
            .remove(&run_id);
        self.ensure_event_pump();
    }

    async fn reattach_run(
        self: &Arc<Self>,
        run: RunSnapshot,
        provider_run: ProviderRunRef,
    ) -> Result<RunSnapshot, RunOrchestrationError> {
        let attached = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .runs
            .contains_key(&run.id);
        if !attached {
            let restored = match self.load_requests(&run, &provider_run).await {
                Ok(restored) => restored,
                Err(error) => {
                    if run.status == RunStatus::Running {
                        let _ = self
                            .transition(run.id, RunStatus::Running, RunStatus::Disconnected, None)
                            .await;
                    }
                    return Err(error.into());
                }
            };
            let receiver =
                self.prepare_route(run.id, run.work_item_id, provider_run.clone(), restored);
            let transitioned = run.status != RunStatus::Running || run.provider_id.is_none();
            let run = if transitioned {
                match self
                    .store
                    .transition_run(TransitionRun {
                        run_id: run.id,
                        expected_current: run.status,
                        target: RunStatus::Running,
                        provider_id: Some(provider_run.provider_id()),
                        occurred_at: self.clock.now(),
                        audit_event_id: self.ids.next_audit_event_id(),
                    })
                    .await
                {
                    Ok(run) => run,
                    Err(error) => {
                        drop(receiver);
                        self.remove_route_state(run.id, &provider_run);
                        return Err(error.into());
                    }
                }
            } else {
                run
            };
            self.activate_route(
                run.id,
                run.work_item_id,
                run.connection_id,
                provider_run.clone(),
                receiver,
            );
            if transitioned {
                self.publish(
                    run.work_item_id,
                    AppEventKind::RunStatusChanged {
                        run_id: run.id,
                        status: RunStatus::Running,
                    },
                )
                .await;
            }
            self.deliver_uncertain_start_events(&run, &provider_run)
                .await;
            return Ok(run);
        }
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .uncertain_runs
            .remove(&run.id);
        Ok(run)
    }

    fn remove_route_state(&self, run_id: RunId, provider_run: &ProviderRunRef) {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        state.routes.remove(provider_run);
        state.runs_by_provider.remove(provider_run);
        state.runs.remove(&run_id);
        state.work_items.remove(&run_id);
        state
            .approvals
            .retain(|_, (pending_run, _)| *pending_run != run_id);
        state
            .user_inputs
            .retain(|_, (pending_run, _)| *pending_run != run_id);
        let active_approvals: HashSet<_> = state.approvals.keys().copied().collect();
        state
            .approval_responses
            .retain(|approval_id, _| active_approvals.contains(approval_id));
    }

    async fn load_requests(
        &self,
        run: &RunSnapshot,
        provider_run: &ProviderRunRef,
    ) -> Result<RestoredRequests, StoreError> {
        let mut restored = RestoredRequests::default();
        let mut after = None;
        loop {
            let page = self
                .store
                .list_approvals_for_run(run.id, after, 200)
                .await?;
            for approval in page.items {
                if matches!(
                    approval.status,
                    ApprovalStatus::Pending | ApprovalStatus::Responding { .. }
                ) && let Some(native_id) = approval
                    .provider_id
                    .value()
                    .strip_prefix(&format!("{}/", provider_run.native_id()))
                {
                    restored.approvals.insert(
                        approval.id,
                        (
                            run.id,
                            ProviderRequestRef {
                                run: provider_run.clone(),
                                native_id: native_id.to_owned(),
                            },
                        ),
                    );
                    if let ApprovalStatus::Responding { decision } = approval.status {
                        restored.approval_responses.insert(approval.id, decision);
                    }
                }
            }
            after = page.next_after;
            if after.is_none() {
                break;
            }
        }

        let mut after = None;
        loop {
            let page = self
                .store
                .list_pending_user_inputs_for_run(run.id, after, 200)
                .await?;
            for item in page.items {
                if let Some(native_id) = item
                    .provider_id
                    .value()
                    .strip_prefix(&format!("{}/", provider_run.native_id()))
                {
                    restored.user_inputs.insert(
                        item.id,
                        (
                            run.id,
                            ProviderRequestRef {
                                run: provider_run.clone(),
                                native_id: native_id.to_owned(),
                            },
                        ),
                    );
                }
            }
            after = page.next_after;
            if after.is_none() {
                break;
            }
        }
        Ok(restored)
    }

    async fn deliver_uncertain_start_events(
        &self,
        run: &RunSnapshot,
        provider_run: &ProviderRunRef,
    ) {
        let pending = {
            let mut state = self
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            state.uncertain_start_order.retain(|id| *id != run.id);
            state.uncertain_starts.remove(&run.id)
        };
        let Some(mut pending) = pending else {
            return;
        };
        let events = pending.events.remove(provider_run).unwrap_or_default();
        let route = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .routes
            .get(provider_run)
            .cloned();
        let Some(route) = route else {
            self.handle_route_overflow(run.id, run.work_item_id).await;
            return;
        };
        if pending.overflow {
            self.handle_route_overflow(run.id, run.work_item_id).await;
            return;
        }
        for event in events {
            if route.try_send(event).is_err() {
                self.handle_route_overflow(run.id, run.work_item_id).await;
                return;
            }
        }
    }

    fn ensure_event_pump(self: &Arc<Self>) {
        let mut handle = self
            .pump
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if handle.is_some() {
            return;
        }
        let inner = Arc::clone(self);
        *handle = Some(tokio::spawn(async move {
            inner.run_event_pump().await;
        }));
    }

    async fn run_event_pump(self: Arc<Self>) {
        loop {
            let operation = match self.harness.next_event().await {
                Ok(Some(event)) => {
                    self.route_event(event).await;
                    continue;
                }
                Ok(None) => STREAM_FAILURE_OPERATION,
                Err(HarnessPortError::Closed) => STREAM_FAILURE_OPERATION,
                Err(HarnessPortError::OutcomeUnknown { operation }) => operation,
                Err(_) => STREAM_FAILURE_OPERATION,
            };
            self.handle_pump_failure(operation).await;
            break;
        }
    }

    async fn route_event(&self, event: RunHarnessEvent) {
        let Some(run) = event.run().cloned() else {
            return;
        };
        let (result, run_id, work_item_id) = {
            let mut state = self
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            let run_id = state.runs_by_provider.get(&run).copied();
            let work_item_id = run_id.and_then(|id| state.work_items.get(&id).copied());
            let Some(route) = state.routes.get(&run) else {
                state.buffer_handshake_event(run, event);
                return;
            };
            (route.try_send(event), run_id, work_item_id)
        };
        let (run_id, work_item_id) = match (run_id, work_item_id) {
            (Some(run_id), Some(work_item_id)) => (run_id, work_item_id),
            _ => return,
        };
        match result {
            Ok(()) => {}
            Err(tokio::sync::mpsc::error::TrySendError::Full(_)) => {
                self.handle_route_overflow(run_id, work_item_id).await;
            }
            Err(tokio::sync::mpsc::error::TrySendError::Closed(_)) => {
                self.transition_to_disconnected(run_id).await;
            }
        }
    }

    async fn transition_to_disconnected(&self, run_id: RunId) {
        if let Ok(snapshot) = self.store.get_run(run_id).await
            && matches!(snapshot.status, RunStatus::Starting | RunStatus::Running)
        {
            let _ = self
                .transition(run_id, snapshot.status, RunStatus::Disconnected, None)
                .await;
        }
    }

    async fn handle_route_overflow(&self, run_id: RunId, work_item_id: WorkItemId) {
        let _ = self.close_run(run_id).await;
        if let Ok(current) = self.store.get_run(run_id).await {
            let target = match current.status {
                RunStatus::Starting => Some(RunStatus::OutcomeUnknown),
                RunStatus::Running => Some(RunStatus::Disconnected),
                _ => None,
            };
            if let Some(target) = target {
                let _ = self.transition(run_id, current.status, target, None).await;
            }
            self.publish(
                work_item_id,
                AppEventKind::RunOutcomeUnknown {
                    run_id,
                    operation: "event_channel_full",
                },
            )
            .await;
        }
    }

    async fn handle_pump_failure(&self, operation: &'static str) {
        let mut affected = Vec::new();
        {
            let mut state = self
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            for (run_id, run) in state.runs.iter() {
                if let Some(work_item_id) = state.work_items.get(run_id) {
                    affected.push((*run_id, run.clone(), *work_item_id));
                }
            }
            let provider_runs: Vec<ProviderRunRef> = state.runs.values().cloned().collect();
            for run in provider_runs {
                state.routes.remove(&run);
                state.runs_by_provider.remove(&run);
            }
            state.runs.clear();
            state.work_items.clear();
            state.runs_by_provider.clear();
            state.routes.clear();
            state.pending_events.clear();
            state.pending_overflow = false;
            state.start_in_progress = None;
            state.uncertain_starts.clear();
            state.uncertain_start_order.clear();
            state.approvals.clear();
            state.user_inputs.clear();
            state.approval_responses.clear();
            state.uncertain_runs.clear();
        }
        self.pump
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take();
        for (run_id, _provider, work_item_id) in affected {
            if let Ok(current) = self.store.get_run(run_id).await {
                let target = match current.status {
                    RunStatus::Starting => Some(RunStatus::OutcomeUnknown),
                    RunStatus::Running => Some(RunStatus::Disconnected),
                    _ => None,
                };
                if let Some(target) = target {
                    let _ = self.transition(run_id, current.status, target, None).await;
                }
            }
            self.publish(
                work_item_id,
                AppEventKind::RunOutcomeUnknown { run_id, operation },
            )
            .await;
        }
    }

    async fn close_run(&self, run_id: RunId) {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let provider = state.runs.get(&run_id).cloned();
        if let Some(provider) = &provider {
            state.routes.remove(provider);
            state.runs_by_provider.remove(provider);
        }
        state.runs.remove(&run_id);
        state.work_items.remove(&run_id);
        state
            .approvals
            .retain(|_, (pending_run_id, _)| *pending_run_id != run_id);
        state
            .user_inputs
            .retain(|_, (pending_run_id, _)| *pending_run_id != run_id);
        let active_approvals: HashSet<_> = state.approvals.keys().copied().collect();
        state
            .approval_responses
            .retain(|approval_id, _| active_approvals.contains(approval_id));
        state.uncertain_runs.remove(&run_id);
        state.uncertain_starts.remove(&run_id);
        state.uncertain_start_order.retain(|id| *id != run_id);
        if let Some(provider) = provider {
            state.pending_events.remove(&provider);
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
        if state.uncertain_runs.contains(&run_id) {
            return Err(RunOrchestrationError::RunNeedsReconciliation(run_id));
        }
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
                if let Ok(current) = self.store.get_run(run_id).await
                    && current.status != RunStatus::OutcomeUnknown
                    && current.status.can_transition_to(RunStatus::OutcomeUnknown)
                {
                    let _ = self
                        .transition(run_id, current.status, RunStatus::OutcomeUnknown, None)
                        .await;
                }
                self.state
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .uncertain_runs
                    .insert(run_id);
                self.publish(
                    work_item_id,
                    AppEventKind::RunOutcomeUnknown { run_id, operation },
                )
                .await;
                Err(error.into())
            }
            Err(error) => Err(error.into()),
        }
    }

    async fn publish(&self, work_item_id: WorkItemId, kind: AppEventKind) {
        let revision = self
            .store
            .get_work_item(work_item_id)
            .await
            .map(|work_item| work_item.revision.get())
            .unwrap_or(0);
        let _ = self.events.send(AppEvent {
            work_item_id,
            revision,
            kind,
        });
    }

    async fn transition(
        &self,
        run_id: RunId,
        expected_current: RunStatus,
        status: RunStatus,
        provider_id: Option<NamespacedProviderId>,
    ) -> Result<RunSnapshot, StoreError> {
        let snapshot = self
            .store
            .transition_run(TransitionRun {
                run_id,
                expected_current,
                target: status,
                provider_id,
                occurred_at: self.clock.now(),
                audit_event_id: self.ids.next_audit_event_id(),
            })
            .await?;
        self.publish(
            snapshot.work_item_id,
            AppEventKind::RunStatusChanged { run_id, status },
        )
        .await;
        Ok(snapshot)
    }

    async fn transition_to_terminal(
        &self,
        run_id: RunId,
        terminal_status: RunStatus,
    ) -> Result<(), StoreError> {
        let snapshot = self.store.get_run(run_id).await?;
        self.transition(run_id, snapshot.status, terminal_status, None)
            .await
            .map(|_| ())
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
            .map(|run| (run.namespace().to_owned(), run.native_id().to_owned()))
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
        )
        .await;
        Ok(())
    }

    async fn consume_run(
        self: Arc<Self>,
        run_id: RunId,
        work_item_id: WorkItemId,
        _connection_id: ConnectionId,
        provider_run: ProviderRunRef,
        mut events: mpsc::Receiver<RunHarnessEvent>,
    ) {
        let mut deltas = DeltaBuffer::default();
        while let Some(event) = events.recv().await {
            let terminal = matches!(event, RunHarnessEvent::RunTerminal { .. });
            if self
                .apply_event(run_id, work_item_id, &provider_run, &mut deltas, event)
                .await
                .is_err()
            {
                if let Ok(current) = self.store.get_run(run_id).await
                    && matches!(current.status, RunStatus::Starting | RunStatus::Running)
                {
                    let _ = self
                        .transition(run_id, current.status, RunStatus::OutcomeUnknown, None)
                        .await;
                    self.publish(
                        work_item_id,
                        AppEventKind::RunOutcomeUnknown {
                            run_id,
                            operation: "run_consumption_failed",
                        },
                    )
                    .await;
                }
                break;
            }
            if terminal {
                break;
            }
        }
        self.close_run(run_id).await;
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
                        )
                        .await;
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
                        )
                        .await;
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
                        self.transition_to_terminal(run_id, status).await?;
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
        request.run.namespace().to_owned(),
        format!("{}/{}", request.run.native_id(), request.native_id),
    )
    .map_err(|error| RunOrchestrationError::InvalidProviderId(error.to_string()))
}

fn command_provider_id(
    command: &ProviderCommandRef,
) -> Result<NamespacedProviderId, RunOrchestrationError> {
    NamespacedProviderId::new(
        command.run.namespace().to_owned(),
        format!("{}/{}", command.run.native_id(), command.native_id),
    )
    .map_err(|error| RunOrchestrationError::InvalidProviderId(error.to_string()))
}

#[cfg(test)]
mod routing_tests {
    use super::*;

    #[test]
    fn unknown_provider_runs_are_handshake_only_and_globally_bounded() {
        let mut state = State::default();
        for index in 0..100 {
            let run = ProviderRunRef::new("mock", format!("unknown-{index}")).unwrap();
            state.buffer_handshake_event(run.clone(), RunHarnessEvent::RunAccepted { run });
        }
        assert!(state.pending_events.is_empty());

        state.start_in_progress = Some(RunId::new_v7());
        for index in 0..100 {
            let run = ProviderRunRef::new("mock", format!("unknown-{index}")).unwrap();
            state.buffer_handshake_event(run.clone(), RunHarnessEvent::RunAccepted { run });
        }
        assert_eq!(
            state.pending_events.values().map(Vec::len).sum::<usize>(),
            ROUTE_CHANNEL_CAPACITY
        );
        assert!(state.pending_overflow);
    }
}
