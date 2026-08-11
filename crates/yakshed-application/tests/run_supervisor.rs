use std::{
    collections::HashMap,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

use async_trait::async_trait;
use provider_mock::{MockHarness, MockHarnessFault, MockRunPlan, MockScriptStep};
use tokio::sync::{Mutex, Notify};
use yakshed_application::{
    AppEvent, AppEventKind, AppStore, Clock, CreateProject, CreateWorkItem, HarnessPortError,
    HarnessResponse, IdGenerator, ListTimeline, ProviderCommandRef, ProviderRequestRef,
    ProviderRunRef, RunHarness, RunHarnessEvent, RunOrchestrationError, RunSupervisor, RunTerminal,
    SystemIdGenerator, TransitionRun,
};
use yakshed_domain::{
    ApprovalDecision, ApprovalStatus, ConnectionId, RunId, RunStatus, UtcTimestamp, WorkItemId,
};
use yakshed_harness::{
    HarnessAdapter, HarnessCapabilities, HarnessError, HarnessEvent, HarnessInput,
    HarnessRunTerminal, ProviderEventStream, ProviderRequestHandle, ProviderRequestId,
    ProviderResponse, ProviderRunHandle, ProviderSession, RunOptions, RuntimeHandle, RuntimePath,
    StartSessionSpec,
};
use yakshed_store::{AppPaths, SqliteStore};

const CONNECTION: &str = "0193f26e-7a72-7000-8000-00000000aaa1";

struct FixedClock;

impl Clock for FixedClock {
    fn now(&self) -> UtcTimestamp {
        UtcTimestamp::from_unix_millis(1_735_689_600_123)
    }
}

struct MockPort {
    harness: Arc<MockHarness>,
    session: ProviderSession,
    stream: Mutex<ProviderEventStream>,
    runs: Mutex<HashMap<ProviderRunRef, ProviderRunHandle>>,
    correlations: Mutex<HashMap<RunId, ProviderRunRef>>,
    unknown_interrupt: bool,
    unknown_start: bool,
    fail_stream: AtomicBool,
    fail_response: AtomicBool,
    fail_reconnect: AtomicBool,
    fail_lookup: AtomicBool,
    block_start: AtomicBool,
    start_release: Notify,
    stream_failure: Notify,
}

impl MockPort {
    async fn new_with_options(
        plans: Vec<MockRunPlan>,
        unknown_interrupt: bool,
        unknown_start: bool,
    ) -> Arc<Self> {
        let runtime = RuntimeHandle::new("mock-runtime").unwrap();
        let connection_id = connection_id();
        let harness = Arc::new(
            MockHarness::new(HarnessCapabilities::default(), plans, None).with_runtime(
                runtime.clone(),
                connection_id,
                None,
                Vec::new(),
            ),
        );
        let stream = harness.subscribe().unwrap();
        let session = harness
            .start_session(
                &runtime,
                StartSessionSpec {
                    working_directory: RuntimePath::new("mock-runtime://workspace").unwrap(),
                    title: "application test".to_owned(),
                },
            )
            .await
            .unwrap();
        Arc::new(Self {
            harness,
            session,
            stream: Mutex::new(stream),
            runs: Mutex::new(HashMap::new()),
            correlations: Mutex::new(HashMap::new()),
            unknown_interrupt,
            unknown_start,
            fail_stream: AtomicBool::new(false),
            fail_response: AtomicBool::new(false),
            fail_reconnect: AtomicBool::new(false),
            fail_lookup: AtomicBool::new(false),
            block_start: AtomicBool::new(false),
            start_release: Notify::new(),
            stream_failure: Notify::new(),
        })
    }

    fn fail_stream(&self) {
        self.fail_stream.store(true, Ordering::SeqCst);
        self.stream_failure.notify_waiters();
    }

    fn fail_next_response(&self) {
        self.fail_response.store(true, Ordering::SeqCst);
    }

    fn fail_next_reconnect(&self) {
        self.fail_reconnect.store(true, Ordering::SeqCst);
    }

    fn fail_next_lookup(&self) {
        self.fail_lookup.store(true, Ordering::SeqCst);
    }

    fn block_next_start(&self) {
        self.block_start.store(true, Ordering::SeqCst);
    }

    fn release_start(&self) {
        self.start_release.notify_one();
    }

    fn run_ref(run: &ProviderRunHandle) -> ProviderRunRef {
        ProviderRunRef::new("mock", run.to_string()).unwrap()
    }

    async fn native_run(
        &self,
        run: &ProviderRunRef,
    ) -> Result<ProviderRunHandle, HarnessPortError> {
        self.runs
            .lock()
            .await
            .get(run)
            .cloned()
            .ok_or_else(|| HarnessPortError::NotFound(run.native_id().to_owned()))
    }
}

#[async_trait]
impl RunHarness for MockPort {
    async fn start_run(
        &self,
        connection_id: ConnectionId,
        correlation_id: RunId,
        input: String,
    ) -> Result<ProviderRunRef, HarnessPortError> {
        if self.block_start.swap(false, Ordering::SeqCst) {
            self.start_release.notified().await;
        }
        if let Some(run) = self.correlations.lock().await.get(&correlation_id).cloned() {
            return Ok(run);
        }
        assert_eq!(connection_id, self.session.connection_id);
        let run = self
            .harness
            .start_run(
                &self.session,
                HarnessInput::new(input).map_err(map_error)?,
                RunOptions::default(),
            )
            .await
            .map_err(map_error)?;
        let run_ref = Self::run_ref(&run);
        self.runs.lock().await.insert(run_ref.clone(), run);
        self.correlations
            .lock()
            .await
            .insert(correlation_id, run_ref.clone());
        if self.unknown_start {
            return Err(HarnessPortError::OutcomeUnknown {
                operation: "start_run",
            });
        }
        Ok(run_ref)
    }

    async fn lookup_run(
        &self,
        connection_id: ConnectionId,
        correlation_id: RunId,
    ) -> Result<Option<ProviderRunRef>, HarnessPortError> {
        assert_eq!(connection_id, self.session.connection_id);
        if self.fail_lookup.swap(false, Ordering::SeqCst) {
            return Ok(None);
        }
        Ok(self.correlations.lock().await.get(&correlation_id).cloned())
    }

    async fn steer(&self, run: &ProviderRunRef, input: String) -> Result<(), HarnessPortError> {
        self.harness
            .steer(
                &self.native_run(run).await?,
                HarnessInput::new(input).map_err(map_error)?,
            )
            .await
            .map_err(map_error)
    }

    async fn interrupt(&self, run: &ProviderRunRef) -> Result<(), HarnessPortError> {
        if self.unknown_interrupt {
            return Err(HarnessPortError::OutcomeUnknown {
                operation: "interrupt",
            });
        }
        self.harness
            .interrupt(&self.native_run(run).await?)
            .await
            .map_err(map_error)
    }

    async fn respond(
        &self,
        request: ProviderRequestRef,
        response: HarnessResponse,
    ) -> Result<(), HarnessPortError> {
        if self.fail_response.swap(false, Ordering::SeqCst) {
            return Err(HarnessPortError::OutcomeUnknown {
                operation: "respond_approval",
            });
        }
        let run = self.native_run(&request.run).await?;
        let request =
            ProviderRequestHandle::new(run, request.native_id.parse().map_err(map_error)?);
        let response = match response {
            HarnessResponse::Approval(decision) => ProviderResponse::Approval(decision),
            HarnessResponse::UserInput(input) => ProviderResponse::UserInput(input),
        };
        self.harness
            .respond_to_request(request, response)
            .await
            .map_err(map_error)
    }

    async fn next_event(&self) -> Result<Option<RunHarnessEvent>, HarnessPortError> {
        if self.fail_stream.swap(false, Ordering::SeqCst) {
            return Err(HarnessPortError::Closed);
        }
        tokio::select! {
            event = async { self.stream.lock().await.recv().await } => Ok(event.map(convert_event)),
            () = self.stream_failure.notified() => {
                self.fail_stream.store(false, Ordering::SeqCst);
                Err(HarnessPortError::Closed)
            }
        }
    }

    async fn reconnect(&self, run: &ProviderRunRef) -> Result<bool, HarnessPortError> {
        if self.fail_reconnect.swap(false, Ordering::SeqCst) {
            return Ok(false);
        }
        Ok(self.runs.lock().await.contains_key(run))
    }
}

fn convert_event(event: HarnessEvent) -> RunHarnessEvent {
    match event {
        HarnessEvent::RunAccepted { run, .. } => RunHarnessEvent::RunAccepted {
            run: MockPort::run_ref(&run),
        },
        HarnessEvent::MessageDelta { run, chunk, .. } => RunHarnessEvent::MessageDelta {
            run: MockPort::run_ref(&run),
            chunk,
        },
        HarnessEvent::MessageCompleted { run, text, .. } => RunHarnessEvent::MessageCompleted {
            run: MockPort::run_ref(&run),
            text,
        },
        HarnessEvent::ApprovalRequested {
            request, summary, ..
        } => RunHarnessEvent::ApprovalRequested {
            request: request_ref(&request),
            summary,
        },
        HarnessEvent::UserInputRequested {
            request, prompt, ..
        } => RunHarnessEvent::UserInputRequested {
            request: request_ref(&request),
            prompt,
        },
        HarnessEvent::FileMutation {
            run, path, summary, ..
        } => RunHarnessEvent::FileMutation {
            run: MockPort::run_ref(&run),
            path,
            summary,
        },
        HarnessEvent::CommandOutputDelta {
            run,
            command,
            command_text,
            chunk,
            ..
        } => RunHarnessEvent::CommandOutputDelta {
            run: MockPort::run_ref(&run),
            command: ProviderCommandRef {
                run: MockPort::run_ref(command.run()),
                native_id: command.native_id().to_string(),
            },
            command_text,
            chunk,
        },
        HarnessEvent::CommandOutputCompleted {
            run,
            command,
            command_text,
            output,
            ..
        } => RunHarnessEvent::CommandOutputCompleted {
            run: MockPort::run_ref(&run),
            command: ProviderCommandRef {
                run: MockPort::run_ref(command.run()),
                native_id: command.native_id().to_string(),
            },
            command_text,
            output,
        },
        HarnessEvent::RunTerminal { run, state, .. } => RunHarnessEvent::RunTerminal {
            run: MockPort::run_ref(&run),
            state: match state {
                HarnessRunTerminal::Completed => RunTerminal::Completed,
                HarnessRunTerminal::Failed { diagnostic } => RunTerminal::Failed {
                    diagnostic: diagnostic.sanitized_text().to_owned(),
                },
                HarnessRunTerminal::Interrupted => RunTerminal::Interrupted,
                HarnessRunTerminal::Crashed { diagnostic } => RunTerminal::Crashed {
                    diagnostic: diagnostic.sanitized_text().to_owned(),
                },
            },
        },
        HarnessEvent::Unknown {
            run,
            item_type,
            native,
        } => RunHarnessEvent::Unknown {
            run: run.as_ref().map(MockPort::run_ref),
            item_type,
            native: native.sanitized_raw().to_owned(),
        },
        HarnessEvent::MalformedNativePayload {
            run,
            item_type,
            native,
        } => RunHarnessEvent::Malformed {
            run: run.as_ref().map(MockPort::run_ref),
            item_type,
            native: native.sanitized_raw().to_owned(),
        },
    }
}

fn request_ref(request: &ProviderRequestHandle) -> ProviderRequestRef {
    ProviderRequestRef {
        run: MockPort::run_ref(request.run()),
        native_id: request.native_id().to_string(),
    }
}

fn map_error(error: HarnessError) -> HarnessPortError {
    match error {
        HarnessError::InvalidInput(message) => HarnessPortError::InvalidInput(message),
        HarnessError::NotFound { entity, id } => {
            HarnessPortError::NotFound(format!("{entity}: {id}"))
        }
        HarnessError::Conflict(message) => HarnessPortError::Conflict(message),
        HarnessError::Unsupported(message) => HarnessPortError::Unsupported(message.to_owned()),
        HarnessError::Overloaded => HarnessPortError::Overloaded,
        HarnessError::Disconnected => HarnessPortError::Disconnected,
        HarnessError::OutcomeUnknown { operation } => {
            HarnessPortError::OutcomeUnknown { operation }
        }
        HarnessError::Closed => HarnessPortError::Closed,
        HarnessError::Protocol { diagnostic } => {
            HarnessPortError::Protocol(diagnostic.sanitized_text().to_owned())
        }
        HarnessError::Transport { diagnostic } => {
            HarnessPortError::Transport(diagnostic.sanitized_text().to_owned())
        }
        HarnessError::Runtime { diagnostic } => {
            HarnessPortError::Runtime(diagnostic.sanitized_text().to_owned())
        }
    }
}

struct TestContext {
    _temp: tempfile::TempDir,
    store: Arc<SqliteStore>,
    supervisor: RunSupervisor,
    harness: Arc<MockPort>,
    work_item_id: WorkItemId,
}

impl TestContext {
    async fn new(plan: MockRunPlan) -> Self {
        Self::with_unknown_interrupt(plan, false).await
    }

    async fn with_unknown_interrupt(plan: MockRunPlan, unknown_interrupt: bool) -> Self {
        Self::with_options(vec![plan], unknown_interrupt, false).await
    }

    async fn with_options(
        plans: Vec<MockRunPlan>,
        unknown_interrupt: bool,
        unknown_start: bool,
    ) -> Self {
        let temp = tempfile::tempdir().unwrap();
        let ids: Arc<dyn IdGenerator> = Arc::new(SystemIdGenerator);
        let clock: Arc<dyn Clock> = Arc::new(FixedClock);
        let store = Arc::new(
            SqliteStore::open(AppPaths::for_test(temp.path()), clock.clone(), ids.clone())
                .await
                .unwrap(),
        );
        let project_id = ids.next_project_id();
        store
            .create_project(CreateProject {
                id: project_id,
                name: "test".to_owned(),
            })
            .await
            .unwrap();
        let work_item_id = ids.next_work_item_id();
        store
            .create_work_item(CreateWorkItem {
                id: work_item_id,
                project_id,
                title: "test run".to_owned(),
                parent_id: None,
            })
            .await
            .unwrap();
        let harness = MockPort::new_with_options(plans, unknown_interrupt, unknown_start).await;
        let supervisor = RunSupervisor::new(store.clone(), harness.clone(), clock, ids);
        Self {
            _temp: temp,
            store,
            supervisor,
            harness,
            work_item_id,
        }
    }

    async fn start(&self) -> RunId {
        self.supervisor
            .start_run(self.work_item_id, connection_id(), "start".to_owned())
            .await
            .unwrap()
            .id
    }

    async fn wait_for_status(&self, run_id: RunId, expected: RunStatus) {
        let status = self
            .store
            .get_run(run_id)
            .await
            .map(|snapshot| snapshot.status)
            .ok();
        if status == Some(expected) {
            return;
        }

        let mut events = self.supervisor.subscribe();
        let result = tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                if let Ok(snapshot) = self.store.get_run(run_id).await
                    && snapshot.status == expected
                {
                    break;
                }
                let event = events.recv().await.unwrap();
                if let AppEventKind::RunStatusChanged {
                    run_id: changed_run_id,
                    status,
                } = event.kind
                    && changed_run_id == run_id
                    && status == expected
                {
                    break;
                }
                if let AppEventKind::RunOutcomeUnknown {
                    run_id: outcome_run_id,
                    operation,
                } = event.kind
                    && outcome_run_id == run_id
                {
                    panic!("run {run_id} became outcome-unknown during wait: {operation}");
                }
            }
        })
        .await;
        if result.is_err() {
            let snapshot = self.store.get_run(run_id).await;
            panic!(
                "timed out waiting for run {run_id} to become {expected:?}; final_status={:?}",
                snapshot.ok().map(|snapshot| snapshot.status)
            );
        }
    }

    async fn wait_for_status_via_events(
        &self,
        events: &mut tokio::sync::broadcast::Receiver<AppEvent>,
        run_id: RunId,
        expected: RunStatus,
    ) -> Vec<u64> {
        let mut revisions = Vec::new();
        tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                let event = events.recv().await.unwrap();
                revisions.push(event.revision);
                if let AppEventKind::RunStatusChanged {
                    run_id: changed_run_id,
                    status,
                } = event.kind
                    && changed_run_id == run_id
                    && status == expected
                {
                    break;
                }
            }
        })
        .await
        .unwrap();
        revisions
    }
}

fn connection_id() -> ConnectionId {
    CONNECTION.parse().unwrap()
}

async fn next_matching(
    events: &mut tokio::sync::broadcast::Receiver<AppEvent>,
    predicate: impl Fn(&AppEventKind) -> bool,
) -> AppEvent {
    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            let event = events.recv().await.unwrap();
            if predicate(&event.kind) {
                break event;
            }
        }
    })
    .await
    .unwrap()
}

#[tokio::test]
async fn full_lifecycle_batches_deltas_and_preserves_native_failures() {
    let context = TestContext::new(MockRunPlan::new(vec![
        MockScriptStep::message("hel"),
        MockScriptStep::message("lo"),
        MockScriptStep::message_completed("hello"),
        MockScriptStep::file_mutation("src/main.rs", "updated"),
        MockScriptStep::command_output("cargo test", "ok"),
        MockScriptStep::unknown("future", "opaque"),
        MockScriptStep::malformed("broken", "{bad"),
        MockScriptStep::complete(),
    ]))
    .await;
    let run_id = context.start().await;
    context.wait_for_status(run_id, RunStatus::Completed).await;
    assert!(
        context
            .store
            .get_run(run_id)
            .await
            .unwrap()
            .provider_id
            .is_some()
    );

    let timeline = context
        .store
        .list_timeline_page(ListTimeline {
            run_id,
            after: None,
            limit: 100,
        })
        .await
        .unwrap()
        .items;
    assert_eq!(
        timeline
            .iter()
            .filter(|item| item.kind == "message_delta_batch")
            .map(|item| item.body.as_str())
            .collect::<Vec<_>>(),
        ["hello"]
    );
    assert!(timeline.iter().any(|item| item.kind == "message_completed"));
    assert!(
        timeline
            .iter()
            .any(|item| item.kind == "unknown:future" && item.body == "opaque")
    );
    assert!(
        timeline
            .iter()
            .any(|item| item.kind == "malformed:broken" && item.body == "{bad")
    );
}

#[tokio::test]
async fn approval_opened_resolved_and_stream_continues() {
    let request = "request-0001".parse::<ProviderRequestId>().unwrap();
    let context = TestContext::new(MockRunPlan::new(vec![
        MockScriptStep::approval(request.clone(), "run command"),
        MockScriptStep::await_response(request),
        MockScriptStep::message_completed("continued"),
        MockScriptStep::complete(),
    ]))
    .await;
    let mut events = context.supervisor.subscribe();
    let run_id = context.start().await;
    let opened = next_matching(&mut events, |kind| {
        matches!(kind, AppEventKind::ApprovalOpened { .. })
    })
    .await;
    let AppEventKind::ApprovalOpened { approval_id, .. } = opened.kind else {
        unreachable!()
    };

    context
        .supervisor
        .resolve_approval(approval_id, ApprovalDecision::Approved)
        .await
        .unwrap();
    context.wait_for_status(run_id, RunStatus::Completed).await;
    let approval = context
        .store
        .list_approvals_for_run(run_id, None, 10)
        .await
        .unwrap()
        .items
        .pop()
        .unwrap();
    assert_eq!(
        approval.status,
        ApprovalStatus::Resolved {
            decision: ApprovalDecision::Approved
        }
    );
}

#[tokio::test]
async fn user_input_round_trip_continues_run() {
    let request = "request-0001".parse::<ProviderRequestId>().unwrap();
    let context = TestContext::new(MockRunPlan::new(vec![
        MockScriptStep::user_input(request.clone(), "favorite color?"),
        MockScriptStep::await_response(request),
        MockScriptStep::complete(),
    ]))
    .await;
    let mut events = context.supervisor.subscribe();
    let run_id = context.start().await;
    let opened = next_matching(&mut events, |kind| {
        matches!(kind, AppEventKind::UserInputOpened { .. })
    })
    .await;
    let AppEventKind::UserInputOpened { request_id, .. } = opened.kind else {
        unreachable!()
    };

    context
        .supervisor
        .respond_user_input(request_id, "blue".to_owned())
        .await
        .unwrap();
    context
        .wait_for_status_via_events(&mut events, run_id, RunStatus::Completed)
        .await;
}

#[tokio::test]
async fn interrupt_maps_to_interrupted_terminal() {
    let context =
        TestContext::new(MockRunPlan::new(Vec::new()).with_fault(MockHarnessFault::NeverComplete))
            .await;
    let run_id = context.start().await;
    context.supervisor.interrupt(run_id).await.unwrap();
    context
        .wait_for_status(run_id, RunStatus::Interrupted)
        .await;
}

#[tokio::test]
async fn steer_uses_the_application_run_id() {
    let context =
        TestContext::new(MockRunPlan::new(Vec::new()).with_fault(MockHarnessFault::NeverComplete))
            .await;
    let run_id = context.start().await;
    context
        .supervisor
        .steer(run_id, "new direction".to_owned())
        .await
        .unwrap();
    context.supervisor.interrupt(run_id).await.unwrap();
    context
        .wait_for_status(run_id, RunStatus::Interrupted)
        .await;
    let timeline = context
        .store
        .list_timeline_page(ListTimeline {
            run_id,
            after: None,
            limit: 10,
        })
        .await
        .unwrap();
    assert!(
        timeline
            .items
            .iter()
            .any(|item| { item.kind == "message_delta_batch" && item.body == "new direction" })
    );
}

#[tokio::test]
async fn crash_maps_to_failed_terminal() {
    let context = TestContext::new(
        MockRunPlan::new(Vec::new()).with_fault(MockHarnessFault::ExitAfterRunAccepted),
    )
    .await;
    let run_id = context.start().await;
    context.wait_for_status(run_id, RunStatus::Failed).await;
}

#[tokio::test]
async fn outcome_unknown_is_distinct_and_does_not_guess_terminal_status() {
    let context = TestContext::with_unknown_interrupt(
        MockRunPlan::new(Vec::new()).with_fault(MockHarnessFault::NeverComplete),
        true,
    )
    .await;
    let mut events = context.supervisor.subscribe();
    let run_id = context.start().await;
    let error = context.supervisor.interrupt(run_id).await.unwrap_err();
    assert!(matches!(
        error,
        RunOrchestrationError::Harness(HarnessPortError::OutcomeUnknown {
            operation: "interrupt"
        })
    ));
    let event = next_matching(&mut events, |kind| {
        matches!(kind, AppEventKind::RunOutcomeUnknown { .. })
    })
    .await;
    assert!(matches!(
        event.kind,
        AppEventKind::RunOutcomeUnknown {
            operation: "interrupt",
            ..
        }
    ));
    assert_eq!(
        context.store.get_run(run_id).await.unwrap().status,
        RunStatus::OutcomeUnknown
    );
}

#[tokio::test]
async fn application_event_revisions_are_monotonic_per_work_item() {
    let context = TestContext::new(MockRunPlan::new(vec![
        MockScriptStep::message("a"),
        MockScriptStep::message_completed("a"),
        MockScriptStep::complete(),
    ]))
    .await;
    let baseline_revision = context
        .store
        .get_work_item(context.work_item_id)
        .await
        .unwrap()
        .revision
        .get();
    let mut events = context.supervisor.subscribe();
    let run_id = context.start().await;
    let revisions = context
        .wait_for_status_via_events(&mut events, run_id, RunStatus::Completed)
        .await;
    assert!(revisions.len() >= 4);
    assert!(revisions.windows(2).all(|pair| pair[1] == pair[0] + 1));
    assert_eq!(revisions[0], baseline_revision + 1);
}

#[tokio::test]
async fn event_stream_failure_terminalizes_routes_and_next_run_restarts_pump() {
    let context = TestContext::with_options(
        vec![
            MockRunPlan::new(Vec::new()).with_fault(MockHarnessFault::NeverComplete),
            MockRunPlan::new(Vec::new()).with_fault(MockHarnessFault::NeverComplete),
        ],
        false,
        false,
    )
    .await;
    let mut events = context.supervisor.subscribe();
    let first = context.start().await;
    context.harness.fail_stream();
    let failed = next_matching(&mut events, |kind| {
        matches!(
            kind,
            AppEventKind::RunOutcomeUnknown {
                run_id,
                operation: "stream_disconnected"
            } if *run_id == first
        )
    })
    .await;
    assert_eq!(failed.work_item_id, context.work_item_id);
    assert_eq!(
        context.store.get_run(first).await.unwrap().status,
        RunStatus::Disconnected
    );

    let second = context.start().await;
    assert_eq!(
        context.store.get_run(second).await.unwrap().status,
        RunStatus::Running
    );
    context.supervisor.interrupt(second).await.unwrap();
    context
        .wait_for_status(second, RunStatus::Interrupted)
        .await;
}

#[tokio::test]
async fn uncertain_start_reconciles_by_correlation_without_duplicate_run() {
    let context = TestContext::with_options(
        vec![
            MockRunPlan::new(vec![
                MockScriptStep::message_completed("before reconciliation"),
                MockScriptStep::approval(
                    "hold-after-message".parse().unwrap(),
                    "hold after message",
                ),
            ])
            .with_fault(MockHarnessFault::DelayApproval),
        ],
        false,
        true,
    )
    .await;
    let mut events = context.supervisor.subscribe();
    assert!(matches!(
        context
            .supervisor
            .start_run(context.work_item_id, connection_id(), "start".to_owned())
            .await,
        Err(RunOrchestrationError::Harness(
            HarnessPortError::OutcomeUnknown {
                operation: "start_run"
            }
        ))
    ));
    let event = next_matching(&mut events, |kind| {
        matches!(
            kind,
            AppEventKind::RunOutcomeUnknown {
                operation: "start_run",
                ..
            }
        )
    })
    .await;
    let AppEventKind::RunOutcomeUnknown { run_id, .. } = event.kind else {
        unreachable!()
    };
    let run = context.store.get_run(run_id).await.unwrap();
    assert_eq!(run.status, RunStatus::OutcomeUnknown);
    assert!(run.provider_id.is_none());
    tokio::time::sleep(Duration::from_millis(20)).await;
    context.harness.fail_next_lookup();
    assert!(matches!(
        context.supervisor.reconcile_run(run_id).await,
        Err(RunOrchestrationError::RunNeedsReconciliation(id)) if id == run_id
    ));
    let reconciled = context.supervisor.reconcile_run(run_id).await.unwrap();
    assert_eq!(reconciled.status, RunStatus::Running);
    assert!(reconciled.provider_id.is_some());
    let timeline = tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            let page = context
                .store
                .list_timeline_page(ListTimeline {
                    run_id,
                    after: None,
                    limit: 20,
                })
                .await
                .unwrap();
            if page
                .items
                .iter()
                .any(|item| item.body == "before reconciliation")
            {
                break page;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert!(
        timeline
            .items
            .iter()
            .any(|item| item.body == "before reconciliation")
    );
    context
        .supervisor
        .steer(run_id, "after reconcile".to_owned())
        .await
        .unwrap();
    assert_eq!(context.harness.correlations.lock().await.len(), 1);
}

#[tokio::test]
async fn cancelled_start_caller_leaves_owned_operation_recoverable() {
    let context =
        TestContext::new(MockRunPlan::new(Vec::new()).with_fault(MockHarnessFault::NeverComplete))
            .await;
    context.harness.block_next_start();
    let supervisor = context.supervisor.clone();
    let work_item_id = context.work_item_id;
    let caller = tokio::spawn(async move {
        supervisor
            .start_run(work_item_id, connection_id(), "blocked".to_owned())
            .await
    });
    let run_id = tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            let page = context
                .store
                .list_runs_for_work_item(context.work_item_id, None, 10)
                .await
                .unwrap();
            if let Some(run) = page
                .items
                .into_iter()
                .find(|run| run.status == RunStatus::Starting)
            {
                break run.id;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    caller.abort();
    context.harness.release_start();
    context.wait_for_status(run_id, RunStatus::Running).await;
    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            if context
                .supervisor
                .steer(run_id, "still owned".to_owned())
                .await
                .is_ok()
            {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn fast_provider_saturating_route_is_durably_disconnected() {
    let request = "route-saturation".parse::<ProviderRequestId>().unwrap();
    let mut steps = vec![MockScriptStep::approval(request, "hold persistence")];
    steps.extend(
        (0..512).map(|index| MockScriptStep::message_completed(format!("message-{index}"))),
    );
    let context =
        TestContext::new(MockRunPlan::new(steps).with_fault(MockHarnessFault::DelayApproval)).await;
    let mut events = context.supervisor.subscribe();
    let run_id = context.start().await;
    let release_store = context.store.stall_worker().await.unwrap();
    let harness = context.harness.harness.clone();
    let producer = tokio::spawn(async move { harness.release_delayed_approval().await });
    for _ in 0..512 {
        tokio::task::yield_now().await;
    }
    release_store.send(()).unwrap();
    producer.await.unwrap().unwrap();
    next_matching(&mut events, |kind| {
        matches!(
            kind,
            AppEventKind::RunOutcomeUnknown {
                run_id: changed_run_id,
                operation: "event_channel_full"
            } if *changed_run_id == run_id
        )
    })
    .await;
    assert_eq!(
        context.store.get_run(run_id).await.unwrap().status,
        RunStatus::Disconnected
    );
}

#[tokio::test]
async fn persistence_failure_terminalizes_run_as_outcome_unknown() {
    let request = "store-failure".parse::<ProviderRequestId>().unwrap();
    let context = TestContext::new(
        MockRunPlan::new(vec![
            MockScriptStep::approval(request, "gate producer"),
            MockScriptStep::message_completed("cannot persist"),
        ])
        .with_fault(MockHarnessFault::DelayApproval),
    )
    .await;
    let mut events = context.supervisor.subscribe();
    let run_id = context.start().await;
    context.store.fail_next_append().await.unwrap();
    context
        .harness
        .harness
        .release_delayed_approval()
        .await
        .unwrap();
    next_matching(&mut events, |kind| {
        matches!(
            kind,
            AppEventKind::RunOutcomeUnknown {
                run_id: changed_run_id,
                operation: "run_consumption_failed"
            } if *changed_run_id == run_id
        )
    })
    .await;
    assert_eq!(
        context.store.get_run(run_id).await.unwrap().status,
        RunStatus::OutcomeUnknown
    );
}

#[tokio::test]
async fn post_start_binding_failure_retains_provider_identity_and_compensates() {
    let context =
        TestContext::new(MockRunPlan::new(Vec::new()).with_fault(MockHarnessFault::NeverComplete))
            .await;
    context
        .store
        .fail_next_transition_after_update()
        .await
        .unwrap();
    let mut events = context.supervisor.subscribe();
    assert!(matches!(
        context
            .supervisor
            .start_run(context.work_item_id, connection_id(), "start".to_owned())
            .await,
        Err(RunOrchestrationError::Store(_))
    ));
    let event = next_matching(&mut events, |kind| {
        matches!(
            kind,
            AppEventKind::RunOutcomeUnknown {
                operation: "start_binding_failed",
                ..
            }
        )
    })
    .await;
    let AppEventKind::RunOutcomeUnknown { run_id, .. } = event.kind else {
        unreachable!()
    };
    let run = context.store.get_run(run_id).await.unwrap();
    assert!(run.provider_id.is_some());
    assert!(matches!(
        run.status,
        RunStatus::OutcomeUnknown | RunStatus::Interrupted
    ));
}

#[tokio::test]
async fn uncertain_approval_response_recovers_after_supervisor_restart() {
    let request = "uncertain-approval".parse::<ProviderRequestId>().unwrap();
    let context = TestContext::new(MockRunPlan::new(vec![
        MockScriptStep::approval(request.clone(), "approve"),
        MockScriptStep::await_response(request),
        MockScriptStep::complete(),
    ]))
    .await;
    let mut events = context.supervisor.subscribe();
    let run_id = context.start().await;
    let opened = next_matching(&mut events, |kind| {
        matches!(kind, AppEventKind::ApprovalOpened { .. })
    })
    .await;
    let AppEventKind::ApprovalOpened { approval_id, .. } = opened.kind else {
        unreachable!()
    };
    context.harness.fail_next_response();
    assert!(matches!(
        context
            .supervisor
            .resolve_approval(approval_id, ApprovalDecision::Approved)
            .await,
        Err(RunOrchestrationError::Harness(
            HarnessPortError::OutcomeUnknown {
                operation: "respond_approval"
            }
        ))
    ));
    assert!(matches!(
        context
            .store
            .list_approvals_for_run(run_id, None, 10)
            .await
            .unwrap()
            .items[0]
            .status,
        ApprovalStatus::Responding {
            decision: ApprovalDecision::Approved
        }
    ));
    assert_eq!(
        context.store.get_run(run_id).await.unwrap().status,
        RunStatus::OutcomeUnknown
    );

    let restarted = RunSupervisor::new(
        context.store.clone(),
        context.harness.clone(),
        Arc::new(FixedClock),
        Arc::new(SystemIdGenerator),
    );
    restarted.reconcile_run(run_id).await.unwrap();
    restarted
        .resolve_approval(approval_id, ApprovalDecision::Approved)
        .await
        .unwrap();
    assert!(matches!(
        context
            .store
            .list_approvals_for_run(run_id, None, 10)
            .await
            .unwrap()
            .items[0]
            .status,
        ApprovalStatus::Resolved { .. }
    ));
}

#[tokio::test]
async fn manual_reconcile_after_failed_startup_reconnect_restores_controls() {
    let context =
        TestContext::new(MockRunPlan::new(Vec::new()).with_fault(MockHarnessFault::NeverComplete))
            .await;
    let run_id = context.start().await;
    context
        .store
        .transition_run(TransitionRun {
            run_id,
            expected_current: RunStatus::Running,
            target: RunStatus::Disconnected,
            provider_id: None,
            occurred_at: FixedClock.now(),
            audit_event_id: SystemIdGenerator.next_audit_event_id(),
        })
        .await
        .unwrap();
    context.harness.fail_next_reconnect();
    let restarted = RunSupervisor::new(
        context.store.clone(),
        context.harness.clone(),
        Arc::new(FixedClock),
        Arc::new(SystemIdGenerator),
    );
    restarted.reconcile_run(run_id).await.unwrap();
    restarted
        .steer(run_id, "reattached".to_owned())
        .await
        .unwrap();
}

#[tokio::test]
async fn failed_request_restore_keeps_run_recoverable() {
    let context =
        TestContext::new(MockRunPlan::new(Vec::new()).with_fault(MockHarnessFault::NeverComplete))
            .await;
    let run_id = context.start().await;
    context
        .store
        .transition_run(TransitionRun {
            run_id,
            expected_current: RunStatus::Running,
            target: RunStatus::Disconnected,
            provider_id: None,
            occurred_at: FixedClock.now(),
            audit_event_id: SystemIdGenerator.next_audit_event_id(),
        })
        .await
        .unwrap();
    context.store.fail_next_pending_input_read().await.unwrap();
    let restarted = RunSupervisor::new(
        context.store.clone(),
        context.harness.clone(),
        Arc::new(FixedClock),
        Arc::new(SystemIdGenerator),
    );
    assert!(matches!(
        restarted.ready().await,
        Err(RunOrchestrationError::Store(_))
    ));
    assert_eq!(
        context.store.get_run(run_id).await.unwrap().status,
        RunStatus::Disconnected
    );
}

#[tokio::test]
async fn user_input_is_answerable_after_supervisor_restart() {
    let request = "restart-input".parse::<ProviderRequestId>().unwrap();
    let context = TestContext::new(MockRunPlan::new(vec![
        MockScriptStep::user_input(request.clone(), "answer?"),
        MockScriptStep::await_response(request),
        MockScriptStep::complete(),
    ]))
    .await;
    let mut events = context.supervisor.subscribe();
    let run_id = context.start().await;
    let opened = next_matching(&mut events, |kind| {
        matches!(kind, AppEventKind::UserInputOpened { .. })
    })
    .await;
    let AppEventKind::UserInputOpened { request_id, .. } = opened.kind else {
        unreachable!()
    };
    let restarted = RunSupervisor::new(
        context.store.clone(),
        context.harness.clone(),
        Arc::new(FixedClock),
        Arc::new(SystemIdGenerator),
    );
    restarted.reconcile_run(run_id).await.unwrap();
    restarted
        .respond_user_input(request_id, "yes".to_owned())
        .await
        .unwrap();
}

#[tokio::test]
async fn supervisor_startup_reconciles_dangling_start_not_store_open() {
    let context = TestContext::new(MockRunPlan::new(Vec::new())).await;
    let dangling = context
        .store
        .create_run(yakshed_application::CreateRun {
            id: SystemIdGenerator.next_run_id(),
            connection_id: connection_id(),
            work_item_id: context.work_item_id,
            provider_run: None,
        })
        .await
        .unwrap();
    let restarted = RunSupervisor::new(
        context.store.clone(),
        context.harness.clone(),
        Arc::new(FixedClock),
        Arc::new(SystemIdGenerator),
    );
    let _ = restarted.steer(dangling.id, "reconcile".to_owned()).await;
    assert_eq!(
        context.store.get_run(dangling.id).await.unwrap().status,
        RunStatus::OutcomeUnknown
    );
}
