use std::{collections::HashMap, sync::Arc, time::Duration};

use async_trait::async_trait;
use provider_mock::{MockHarness, MockHarnessFault, MockRunPlan, MockScriptStep};
use tokio::sync::Mutex;
use yakshed_application::{
    AppEvent, AppEventKind, AppStore, Clock, CreateProject, CreateWorkItem, HarnessPortError,
    HarnessResponse, IdGenerator, ListTimeline, ProviderCommandRef, ProviderRequestRef,
    ProviderRunRef, RunHarness, RunHarnessEvent, RunOrchestrationError, RunSupervisor, RunTerminal,
    SystemIdGenerator,
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
    unknown_interrupt: bool,
}

impl MockPort {
    async fn new(plan: MockRunPlan, unknown_interrupt: bool) -> Arc<Self> {
        let runtime = RuntimeHandle::new("mock-runtime").unwrap();
        let connection_id = connection_id();
        let harness = Arc::new(
            MockHarness::new(HarnessCapabilities::default(), vec![plan], None).with_runtime(
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
            unknown_interrupt,
        })
    }

    fn run_ref(run: &ProviderRunHandle) -> ProviderRunRef {
        ProviderRunRef {
            namespace: "mock".to_owned(),
            native_id: run.to_string(),
        }
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
            .ok_or_else(|| HarnessPortError::NotFound(run.native_id.clone()))
    }
}

#[async_trait]
impl RunHarness for MockPort {
    async fn start_run(
        &self,
        connection_id: ConnectionId,
        input: String,
    ) -> Result<ProviderRunRef, HarnessPortError> {
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
        Ok(run_ref)
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
        Ok(self.stream.lock().await.recv().await.map(convert_event))
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
    work_item_id: WorkItemId,
}

impl TestContext {
    async fn new(plan: MockRunPlan) -> Self {
        Self::with_unknown_interrupt(plan, false).await
    }

    async fn with_unknown_interrupt(plan: MockRunPlan, unknown_interrupt: bool) -> Self {
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
        let port = MockPort::new(plan, unknown_interrupt).await;
        let supervisor = RunSupervisor::new(store.clone(), port, clock, ids);
        Self {
            _temp: temp,
            store,
            supervisor,
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
        let mut events = self.supervisor.subscribe();
        tokio::time::timeout(Duration::from_secs(2), async {
            loop {
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
            }
        })
        .await
        .unwrap();
    }

    async fn wait_for_status_via_events(
        &self,
        events: &mut tokio::sync::broadcast::Receiver<AppEvent>,
        run_id: RunId,
        expected: RunStatus,
    ) -> Vec<u64> {
        let mut revisions = Vec::new();
        tokio::time::timeout(Duration::from_secs(2), async {
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
        RunStatus::Running
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
    let mut events = context.supervisor.subscribe();
    let run_id = context.start().await;
    let revisions = context
        .wait_for_status_via_events(&mut events, run_id, RunStatus::Completed)
        .await;
    assert!(revisions.len() >= 4);
    assert!(revisions.windows(2).all(|pair| pair[1] == pair[0] + 1));
    assert_eq!(revisions[0], 1);
}
