#![cfg(target_os = "macos")]

use std::{
    collections::{BTreeSet, HashSet},
    sync::{
        Arc, Mutex as StdMutex,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

use async_trait::async_trait;
use provider_mock::{MockHarness, MockRunPlan, MockScriptStep};
use serde_json::{Value, json};
use tauri::{Emitter, Listener};
use tokio::sync::Mutex;
use yakshed_application::{
    AppConfig, AppStore, ArtifactPort, ArtifactPortError, CachePort, CachePortError, ConfigPort,
    ConfigPortError, ConfigRevision, ConfigSnapshot, CreateProject, CreateWorkItem,
    HarnessPortError, HarnessResponse, IdGenerator, OpenArtifactCommand, OpenArtifactPayload,
    ProviderRequestRef, ProviderRunRef, PublicConnection, PublicConnectionList,
    PublicCredentialBinding, PublicCredentialSource, PutConnectionCommand, RunHarness,
    RunHarnessEvent, RunTerminal, SecretPort, SecretPortError, SecretWriteOutcome,
    SetConnectionCredentialCommand, SystemClock, SystemIdGenerator,
};
use yakshed_desktop_api::{ApiPorts, ConnectionInput};
use yakshed_domain::{
    ArtifactRecord, Connection, ConnectionId, CredentialBinding, CredentialSlot, ProjectId,
    ProviderStateRootId, RunId, WorkItemId,
};
use yakshed_harness::{
    HarnessAdapter, HarnessCapabilities, HarnessError, HarnessEvent, HarnessInput,
    HarnessRunTerminal, ProviderEventStream, ProviderRunHandle, ProviderSession, RunOptions,
    RuntimeHandle, RuntimePath, StartSessionSpec,
};
use yakshed_store::{AppPaths, SqliteStore};
use yakshed_tauri::{
    COMMANDS, FRONTEND_EVENT_NAME, StartupError, configure, forward_events, initialize,
};

struct Fixture {
    temp: tempfile::TempDir,
    store: Arc<SqliteStore>,
    ids: Arc<dyn IdGenerator>,
    work_item_id: WorkItemId,
    connection_id: ConnectionId,
    config: Arc<TestConfig>,
    secrets: Arc<TestSecrets>,
}

impl Fixture {
    async fn new() -> Self {
        let temp = tempfile::tempdir().unwrap();
        let paths = AppPaths::for_test(temp.path());
        let ids: Arc<dyn IdGenerator> = Arc::new(SystemIdGenerator);
        let clock = Arc::new(SystemClock);
        let store = Arc::new(
            SqliteStore::open(paths.clone(), clock, ids.clone())
                .await
                .unwrap(),
        );
        let project_id = ids.next_project_id();
        store
            .create_project(CreateProject {
                id: project_id,
                name: "desktop".to_owned(),
            })
            .await
            .unwrap();
        let work_item_id = ids.next_work_item_id();
        store
            .create_work_item(CreateWorkItem {
                id: work_item_id,
                project_id,
                title: "base".to_owned(),
                parent_id: None,
            })
            .await
            .unwrap();
        let connection_id = "0193f26e-7a72-7d42-bf77-0de14c4cc001".parse().unwrap();
        let config = Arc::new(TestConfig::new(test_connection(connection_id)));
        Self {
            temp,
            store,
            ids,
            work_item_id,
            connection_id,
            config,
            secrets: Arc::new(TestSecrets::default()),
        }
    }

    async fn ports(&self, event_count: usize) -> ApiPorts {
        let plan = MockRunPlan::new(
            (0..event_count)
                .map(|index| MockScriptStep::file_mutation(format!("file-{index}"), "updated"))
                .chain(std::iter::once(MockScriptStep::complete()))
                .collect(),
        );
        ApiPorts {
            store: self.store.clone(),
            harness: MockPort::new(plan, self.connection_id).await,
            clock: Arc::new(SystemClock),
            ids: self.ids.clone(),
            config: self.config.clone(),
            secrets: self.secrets.clone(),
            cache: Arc::new(TestCache),
            artifacts: Arc::new(TestArtifacts),
        }
    }
}

struct TestConfig {
    state: StdMutex<(ConfigRevision, Vec<Connection>)>,
}

impl TestConfig {
    fn new(connection: Connection) -> Self {
        Self {
            state: StdMutex::new((ConfigRevision::INITIAL, vec![connection])),
        }
    }
}

#[async_trait]
impl ConfigPort for TestConfig {
    async fn put_connection(
        &self,
        command: PutConnectionCommand,
    ) -> Result<ConfigSnapshot, ConfigPortError> {
        let mut state = self.state.lock().unwrap();
        if command.expected_config_revision != state.0 {
            return Err(ConfigPortError::Conflict {
                expected: command.expected_config_revision,
                actual: state.0,
            });
        }
        state
            .1
            .retain(|connection| connection.id != command.connection.id);
        state.1.push(command.connection);
        state.0 = ConfigRevision::new(state.0.get() + 1);
        Ok(ConfigSnapshot {
            revision: state.0,
            config: AppConfig {
                connections: state.1.clone(),
                ..AppConfig::default()
            },
        })
    }

    async fn get_connection(
        &self,
        connection_id: ConnectionId,
    ) -> Result<yakshed_application::ConfigConnectionSnapshot, ConfigPortError> {
        let state = self.state.lock().unwrap();
        let connection = state
            .1
            .iter()
            .find(|connection| connection.id == connection_id)
            .cloned()
            .ok_or(ConfigPortError::NotFound)?;
        Ok(yakshed_application::ConfigConnectionSnapshot {
            config_revision: state.0,
            connection: to_public(connection),
        })
    }

    async fn list_connections(&self) -> Result<PublicConnectionList, ConfigPortError> {
        let state = self.state.lock().unwrap();
        Ok(PublicConnectionList {
            config_revision: state.0,
            connections: state.1.iter().cloned().map(to_public).collect(),
        })
    }
}

#[derive(Default)]
struct TestSecrets(AtomicBool);

#[async_trait]
impl SecretPort for TestSecrets {
    async fn set_connection_credential(
        &self,
        _command: SetConnectionCredentialCommand,
    ) -> Result<SecretWriteOutcome, SecretPortError> {
        if self.0.swap(true, Ordering::Relaxed) {
            Err(SecretPortError::AlreadyExists)
        } else {
            Ok(SecretWriteOutcome { overwritten: false })
        }
    }
}

struct TestCache;

#[async_trait]
impl CachePort for TestCache {
    async fn clear(&self) -> Result<(), CachePortError> {
        Ok(())
    }
}

struct TestArtifacts;

#[async_trait]
impl ArtifactPort for TestArtifacts {
    async fn list_artifacts_for_work_item(
        &self,
        _work_item_id: WorkItemId,
    ) -> Result<Vec<ArtifactRecord>, ArtifactPortError> {
        Ok(Vec::new())
    }

    async fn open_artifact(
        &self,
        _command: OpenArtifactCommand,
    ) -> Result<OpenArtifactPayload, ArtifactPortError> {
        Err(ArtifactPortError::NotFound)
    }
}

struct MockPort {
    harness: Arc<MockHarness>,
    _runtime: RuntimeHandle,
    _session: ProviderSession,
    stream: Mutex<ProviderEventStream>,
    runs: Mutex<HashSet<ProviderRunRef>>,
}

impl MockPort {
    async fn new(plan: MockRunPlan, connection_id: ConnectionId) -> Arc<Self> {
        let runtime = RuntimeHandle::new("mock-runtime").unwrap();
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
                    title: "tauri-test".to_owned(),
                },
            )
            .await
            .unwrap();
        Arc::new(Self {
            harness,
            _runtime: runtime,
            _session: session,
            stream: Mutex::new(stream),
            runs: Mutex::new(HashSet::new()),
        })
    }

    fn run_ref(run: &ProviderRunHandle) -> ProviderRunRef {
        ProviderRunRef::new("mock", run.to_string()).unwrap()
    }
}

#[async_trait]
impl RunHarness for MockPort {
    async fn start_run(
        &self,
        connection_id: ConnectionId,
        _correlation_id: RunId,
        input: String,
    ) -> Result<ProviderRunRef, HarnessPortError> {
        let run = self
            .harness
            .start_run(
                &self._session,
                HarnessInput::new(input).map_err(map_harness_error)?,
                RunOptions::default(),
            )
            .await
            .map_err(map_harness_error)?;
        let run = Self::run_ref(&run);
        self.runs.lock().await.insert(run.clone());
        assert_eq!(connection_id, self._session.connection_id);
        Ok(run)
    }

    async fn steer(&self, _run: &ProviderRunRef, _input: String) -> Result<(), HarnessPortError> {
        Ok(())
    }

    async fn interrupt(&self, _run: &ProviderRunRef) -> Result<(), HarnessPortError> {
        Ok(())
    }

    async fn respond(
        &self,
        _request: ProviderRequestRef,
        _response: HarnessResponse,
    ) -> Result<(), HarnessPortError> {
        Ok(())
    }

    async fn next_event(&self) -> Result<Option<RunHarnessEvent>, HarnessPortError> {
        Ok(self.stream.lock().await.recv().await.map(convert_event))
    }

    async fn reconnect(&self, run: &ProviderRunRef) -> Result<bool, HarnessPortError> {
        Ok(self.runs.lock().await.contains(run))
    }
}

fn convert_event(event: HarnessEvent) -> RunHarnessEvent {
    match event {
        HarnessEvent::RunAccepted { run, .. } => RunHarnessEvent::RunAccepted {
            run: MockPort::run_ref(&run),
        },
        HarnessEvent::FileMutation {
            run, path, summary, ..
        } => RunHarnessEvent::FileMutation {
            run: MockPort::run_ref(&run),
            path,
            summary,
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
        _ => panic!("unexpected event from scripted mock harness"),
    }
}

fn map_harness_error(error: HarnessError) -> HarnessPortError {
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

fn test_connection(id: ConnectionId) -> Connection {
    Connection {
        id,
        name: "mock".to_owned(),
        harness: "scripted".to_owned(),
        model_provider: "mock".to_owned(),
        provider_state: ProviderStateRootId::new("mock-provider").unwrap(),
        credentials: Vec::new(),
    }
}

fn to_public(connection: Connection) -> PublicConnection {
    PublicConnection {
        id: connection.id,
        name: connection.name,
        harness: connection.harness,
        model_provider: connection.model_provider,
        provider_state: connection.provider_state,
        credentials: connection
            .credentials
            .into_iter()
            .map(|binding| PublicCredentialBinding {
                slot: binding.slot,
                source: match binding.binding {
                    CredentialBinding::Delegated { authority } => {
                        PublicCredentialSource::Delegated { authority }
                    }
                    CredentialBinding::Secret { reference } => PublicCredentialSource::Secret {
                        backend: reference.backend_id,
                        locator: reference.locator,
                    },
                    CredentialBinding::Disabled => PublicCredentialSource::Disabled,
                },
            })
            .collect(),
    }
}

fn build_app(
    state: yakshed_tauri::ShellState,
) -> (
    tauri::App<tauri::test::MockRuntime>,
    tauri::WebviewWindow<tauri::test::MockRuntime>,
) {
    let app = configure(tauri::test::mock_builder(), state)
        .build(tauri::generate_context!())
        .unwrap();
    let webview = tauri::WebviewWindowBuilder::new(&app, "main", Default::default())
        .build()
        .unwrap();
    (app, webview)
}

fn invoke(
    webview: &tauri::WebviewWindow<tauri::test::MockRuntime>,
    cmd: &str,
    body: Value,
) -> Result<Value, Value> {
    tauri::test::get_ipc_response(
        webview,
        tauri::webview::InvokeRequest {
            cmd: cmd.into(),
            callback: tauri::ipc::CallbackFn(0),
            error: tauri::ipc::CallbackFn(1),
            url: "tauri://localhost".parse().unwrap(),
            body: tauri::ipc::InvokeBody::Json(body),
            headers: Default::default(),
            invoke_key: tauri::test::INVOKE_KEY.to_owned(),
        },
    )
    .map(|body| body.deserialize().unwrap())
}

async fn wait_for_completion(
    webview: &tauri::WebviewWindow<tauri::test::MockRuntime>,
    work_item_id: WorkItemId,
) -> Value {
    tokio::time::timeout(Duration::from_secs(4), async {
        loop {
            let snapshot = invoke(
                webview,
                "get_work_item_snapshot",
                json!({ "id": work_item_id }),
            )
            .unwrap();
            if snapshot["runs"]
                .as_array()
                .unwrap()
                .iter()
                .any(|run| run["status"] == "completed")
            {
                break snapshot;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap()
}

#[tokio::test]
async fn healthy_factory_gates_first_command_on_ready_api() {
    let fixture = Fixture::new().await;
    let state = initialize(async { Ok(fixture.ports(0).await) })
        .await
        .unwrap();
    let (_app, webview) = build_app(state);
    let snapshot = invoke(
        &webview,
        "get_work_item_snapshot",
        json!({ "id": fixture.work_item_id }),
    )
    .unwrap();
    assert_eq!(
        snapshot["work_item"]["id"],
        fixture.work_item_id.to_string()
    );
}

#[tokio::test]
async fn poisoned_store_factory_returns_serializable_startup_error() {
    let temp = tempfile::tempdir().unwrap();
    let paths = AppPaths::for_test(temp.path());
    std::fs::create_dir_all(&paths.data_root).unwrap();
    std::fs::write(paths.data_root.join("yakshed.sqlite3"), b"not sqlite").unwrap();
    let error =
        match SqliteStore::open(paths, Arc::new(SystemClock), Arc::new(SystemIdGenerator)).await {
            Ok(_) => panic!("poisoned store unexpectedly opened"),
            Err(error) => error,
        };
    let startup = match initialize(async {
        Err::<ApiPorts, _>(StartupError::persistence(error.to_string()))
    })
    .await
    {
        Ok(_) => panic!("poisoned startup unexpectedly succeeded"),
        Err(error) => error,
    };
    assert_eq!(
        serde_json::to_value(startup).unwrap()["code"],
        "persistence_error"
    );
}

#[tokio::test]
async fn every_registered_handler_is_invocable_against_a_real_desktop_api() {
    let fixture = Fixture::new().await;
    let state = initialize(async { Ok(fixture.ports(1).await) })
        .await
        .unwrap();
    let (_app, webview) = build_app(state);
    let new_project = ProjectId::new_v7();
    let second_connection = "0193f26e-7a72-7d42-bf77-0de14c4cc002".parse().unwrap();
    let mut invoked = BTreeSet::new();
    macro_rules! call {
        ($name:literal, $body:expr) => {{
            invoked.insert($name);
            invoke(&webview, $name, $body)
        }};
    }

    call!(
        "create_project",
        json!({ "id": new_project, "name": "new" })
    )
    .unwrap();
    let created = call!(
        "create_work_item",
        json!({ "projectId": new_project, "title": "task", "parentId": null })
    )
    .unwrap();
    let created_id = created["work_item"]["id"].as_str().unwrap();
    call!(
        "list_work_items",
        json!({ "projectId": new_project, "after": null, "limit": 10 })
    )
    .unwrap();
    call!("get_work_item_snapshot", json!({ "id": created_id })).unwrap();
    call!(
        "get_work_item_snapshot_page",
        json!({ "id": created_id, "after": null, "limit": 10, "expectedRevision": null })
    )
    .unwrap();
    let run_id = call!("start_run", json!({ "workItemId": fixture.work_item_id, "connectionId": fixture.connection_id, "input": "go" })).unwrap();
    wait_for_completion(&webview, fixture.work_item_id).await;
    call!(
        "get_work_item_timeline_page",
        json!({ "workItemId": fixture.work_item_id, "runId": run_id, "after": null, "limit": 10 })
    )
    .unwrap();
    call!("get_work_item_timeline_page_at_revision", json!({ "workItemId": fixture.work_item_id, "runId": run_id, "after": null, "limit": 10, "expectedRevision": null })).unwrap();
    call!("get_run_approval_page", json!({ "workItemId": fixture.work_item_id, "runId": run_id, "after": null, "limit": 10, "expectedRevision": null })).unwrap();
    call!("get_pending_user_input_page", json!({ "workItemId": fixture.work_item_id, "runId": run_id, "after": null, "limit": 10, "expectedRevision": null })).unwrap();
    let _ = call!("steer_run", json!({ "runId": run_id, "message": "later" }));
    let _ = call!("interrupt_run", json!({ "runId": run_id }));
    let _ = call!("reconcile_run", json!({ "runId": run_id }));
    let _ = call!(
        "resolve_approval",
        json!({ "approvalId": yakshed_domain::ApprovalRequestId::new_v7(), "decision": "approved" })
    );
    let _ = call!(
        "respond_user_input",
        json!({ "requestId": yakshed_domain::TimelineItemId::new_v7(), "response": "answer" })
    );
    call!(
        "connection_put",
        json!({
            "expectedConfigRevision": 0,
            "connection": ConnectionInput {
                id: second_connection,
                name: "second".to_owned(),
                harness: "scripted".to_owned(),
                model_provider: "mock".to_owned(),
                provider_state: "second-provider".to_owned(),
                credentials: Vec::new(),
            },
            "ensureMemorySecretBackend": false
        })
    )
    .unwrap();
    call!("connection_get", json!({ "id": second_connection })).unwrap();
    call!("list_connections", json!({})).unwrap();
    call!("set_connection_credential", json!({ "connectionId": fixture.connection_id, "slot": CredentialSlot::new("mock.api_key").unwrap(), "value": "secret", "overwrite": false })).unwrap();
    call!(
        "list_artifacts",
        json!({ "workItemId": fixture.work_item_id })
    )
    .unwrap();
    let _ = call!(
        "open_artifact",
        json!({ "workItemId": fixture.work_item_id, "artifactId": "0193f26e-7a72-7d42-bf77-0de14c4cc003", "maxBytes": 10 })
    );
    call!("clear_cache", json!({})).unwrap();

    assert_eq!(invoked, COMMANDS.iter().copied().collect());
}

#[tokio::test]
async fn event_overflow_recovers_through_tauri_snapshot_surfaces() {
    let fixture = Fixture::new().await;
    let state = initialize(async { Ok(fixture.ports(48).await) })
        .await
        .unwrap();
    let mut lagged = state.subscribe_events();
    let bridge = state.subscribe_events();
    let (app, webview) = build_app(state);
    let emitted = Arc::new(StdMutex::new(Vec::<Value>::new()));
    let capture = emitted.clone();
    app.listen(FRONTEND_EVENT_NAME, move |event| {
        capture
            .lock()
            .unwrap()
            .push(serde_json::from_str(event.payload()).unwrap());
    });
    let handle = app.handle().clone();
    tokio::spawn(forward_events(bridge, move |event| {
        handle.emit(FRONTEND_EVENT_NAME, event).is_ok()
    }));
    let run_id = invoke(
        &webview,
        "start_run",
        json!({ "workItemId": fixture.work_item_id, "connectionId": fixture.connection_id, "input": "overflow" }),
    )
    .unwrap();
    let snapshot = wait_for_completion(&webview, fixture.work_item_id).await;
    for _ in 0..16 {
        tokio::task::yield_now().await;
    }
    assert!(matches!(
        lagged.recv().await,
        Err(tokio::sync::broadcast::error::RecvError::Lagged(_))
    ));
    let timeline = invoke(
        &webview,
        "get_work_item_timeline_page",
        json!({ "workItemId": fixture.work_item_id, "runId": run_id, "after": null, "limit": 50 }),
    )
    .unwrap();
    assert_eq!(timeline["items"].as_array().unwrap().len(), 48);
    tokio::time::timeout(Duration::from_secs(1), async {
        while emitted.lock().unwrap().is_empty() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    let max_emitted = emitted
        .lock()
        .unwrap()
        .iter()
        .map(|event| event["revision"].as_u64().unwrap())
        .max()
        .unwrap();
    assert!(snapshot["revision"].as_u64().unwrap() >= max_emitted);
}

#[tokio::test]
async fn credential_canary_never_crosses_the_tauri_boundary() {
    let fixture = Fixture::new().await;
    let state = initialize(async { Ok(fixture.ports(0).await) })
        .await
        .unwrap();
    let (app, webview) = build_app(state);
    let events = Arc::new(StdMutex::new(Vec::<String>::new()));
    let capture = events.clone();
    app.listen(FRONTEND_EVENT_NAME, move |event| {
        capture.lock().unwrap().push(event.payload().to_owned());
    });
    let canary = "tauri-canary-secret-value";
    let wrote = invoke(
        &webview,
        "set_connection_credential",
        json!({ "connectionId": fixture.connection_id, "slot": "mock.api_key", "value": canary, "overwrite": false }),
    );
    let error = invoke(
        &webview,
        "set_connection_credential",
        json!({ "connectionId": fixture.connection_id, "slot": "mock.api_key", "value": canary, "overwrite": false }),
    );
    let single = invoke(
        &webview,
        "connection_get",
        json!({ "id": fixture.connection_id }),
    );
    let listed = invoke(&webview, "list_connections", json!({}));
    let serialized = format!(
        "{wrote:?}{error:?}{single:?}{listed:?}{:?}",
        events.lock().unwrap()
    );
    assert!(!serialized.contains(canary));
    assert!(!tree_contains(fixture.temp.path(), canary.as_bytes()));
}

fn tree_contains(path: &std::path::Path, needle: &[u8]) -> bool {
    std::fs::read_dir(path).unwrap().any(|entry| {
        let path = entry.unwrap().path();
        if path.is_dir() {
            tree_contains(&path, needle)
        } else {
            std::fs::read(path)
                .map(|bytes| bytes.windows(needle.len()).any(|window| window == needle))
                .unwrap_or(false)
        }
    })
}
