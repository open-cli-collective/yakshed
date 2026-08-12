use std::{
    collections::{HashMap, VecDeque},
    fs,
    io::Read,
    path::PathBuf,
    sync::{Arc, Mutex as StdMutex},
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use async_trait::async_trait;
use provider_codex::{CodexAdapter, CodexRuntimeKey, CodexRuntimeSpec};
use tokio::sync::{Mutex, Notify, mpsc};
use yakshed_application::{
    ArtifactPort, ArtifactPortError, CachePort, CachePortError, ConfigChange,
    ConfigConnectionSnapshot, ConfigPort, ConfigPortError, ConfigSnapshot, HarnessPortError,
    HarnessResponse, OpenArtifactCommand, OpenArtifactPayload, ProviderCommandRef,
    ProviderRequestRef, ProviderRunRef, PublicConnection, PublicConnectionList,
    PublicCredentialBinding, PublicCredentialSource, PutConnectionCommand, RunHarness,
    RunHarnessEvent, RunTerminal, SecretPort, SecretPortError, SecretWriteOutcome,
    SetConnectionCredentialCommand, SystemClock, SystemIdGenerator,
};
use yakshed_desktop_api::{ApiPorts, StartupError};
use yakshed_domain::{
    ArtifactRecord, Connection, ConnectionId, CredentialBinding, OperationId, RunId, SecretBackend,
    SecretBackendId, SecretBackendSettings, WorkItemId,
};
use yakshed_harness::{
    HarnessAdapter, HarnessError, HarnessEvent, HarnessInput, HarnessRunTerminal,
    ProviderEventStream, ProviderRequestHandle, ProviderResponse, ProviderRunHandle, RunOptions,
    RuntimeHandle, RuntimePath, StartSessionSpec,
};
use yakshed_secrets::{
    BrokerCancellation, CredentialBroker, LocalFileBackend, NoopSecretAuditSink, PutSecretOptions,
    PutSecretOutcome, SecretAccessContext, SecretAccessPurpose, SecretBackendHandle,
    backend_capabilities,
};
use yakshed_store::{
    AppPaths, ArtifactError, ArtifactStore, CacheStore, ConfigError, ConfigStore, SqliteStore,
};

const LOCAL_BACKEND_ID: &str = "local-file";
const MAX_ARTIFACT_BYTES: u64 = 64 * 1024 * 1024;
const PENDING_EVENT_CAPACITY: usize = 128;

pub fn run() {
    let state = match tauri::async_runtime::block_on(compose()) {
        Ok(state) => state,
        Err(error) => {
            eprintln!("{}", error.message);
            return;
        }
    };
    yakshed_tauri::app_builder(state)
        .run(tauri::generate_context!())
        .expect("YakShed desktop runtime failed");
}

async fn compose() -> Result<yakshed_tauri::ShellState, StartupError> {
    let paths = AppPaths::production().map_err(|_| StartupError::persistence())?;
    paths
        .create_data_root()
        .and_then(|()| paths.create_runtime_root())
        .map_err(|_| StartupError::persistence())?;

    let clock = Arc::new(SystemClock);
    let ids = Arc::new(SystemIdGenerator);
    let store = Arc::new(
        SqliteStore::open(paths.clone(), clock.clone(), ids.clone())
            .await
            .map_err(|_| StartupError::persistence())?,
    );
    let config_store = Arc::new(
        ConfigStore::open(paths.clone(), backend_capabilities())
            .map_err(|_| StartupError::persistence())?,
    );
    let local_backend = local_backend(&paths);
    let broker = Arc::new(build_broker(&config_store.snapshot(), &local_backend)?);
    let harness = Arc::new(CodexHarness::new(paths.clone(), config_store.clone()));
    let cache = Arc::new(CacheStore::open(&paths).map_err(|_| StartupError::persistence())?);
    let artifacts = Arc::new(
        ArtifactStore::new(&paths, MAX_ARTIFACT_BYTES).map_err(|_| StartupError::persistence())?,
    );
    let artifact_port = Arc::new(ProductionArtifacts {
        metadata: store.clone(),
        blobs: artifacts,
    });

    yakshed_tauri::initialize(async move {
        Ok(ApiPorts {
            store,
            harness,
            clock,
            ids,
            config: Arc::new(ProductionConfig {
                store: config_store.clone(),
                local_backend,
            }),
            secrets: Arc::new(ProductionSecrets {
                store: config_store,
                broker,
            }),
            cache: Arc::new(ProductionCache(cache)),
            artifacts: artifact_port,
        })
    })
    .await
}

fn local_backend(paths: &AppPaths) -> SecretBackend {
    SecretBackend {
        id: SecretBackendId::new(LOCAL_BACKEND_ID).expect("constant backend id is valid"),
        settings: SecretBackendSettings::LocalFile {
            path: paths
                .data_root
                .join("secrets.json")
                .to_string_lossy()
                .into_owned(),
        },
    }
}

fn build_broker(
    snapshot: &ConfigSnapshot,
    fallback: &SecretBackend,
) -> Result<CredentialBroker, StartupError> {
    let mut configured = snapshot.config.secret_backends.clone();
    if !configured.iter().any(|backend| backend.id == fallback.id) {
        configured.push(fallback.clone());
    }
    let mut handles = Vec::new();
    for config in configured {
        if !matches!(config.settings, SecretBackendSettings::LocalFile { .. }) {
            continue;
        }
        let id = config.id.clone();
        let backend =
            Arc::new(LocalFileBackend::from_config(&config).map_err(|_| StartupError::internal())?);
        handles.push((
            id,
            SecretBackendHandle {
                resolver: backend.clone(),
                administrator: Some(backend),
            },
        ));
    }
    CredentialBroker::new(
        handles,
        &snapshot.config.connections,
        Arc::new(NoopSecretAuditSink),
        Duration::from_secs(5),
    )
    .map_err(|_| StartupError::internal())
}

struct ProductionConfig {
    store: Arc<ConfigStore>,
    local_backend: SecretBackend,
}

#[async_trait]
impl ConfigPort for ProductionConfig {
    async fn put_connection(
        &self,
        command: PutConnectionCommand,
    ) -> Result<ConfigSnapshot, ConfigPortError> {
        let needs_local = command.connection.credentials.iter().any(|binding| {
            matches!(
                &binding.binding,
                CredentialBinding::Secret { reference }
                    if reference.backend_id == self.local_backend.id
            )
        });
        let change = if needs_local {
            ConfigChange::PutConnectionWithSecretBackends {
                connection: command.connection,
                secret_backends: vec![self.local_backend.clone()],
            }
        } else {
            ConfigChange::PutConnection(command.connection)
        };
        self.store
            .update(command.expected_config_revision, change)
            .await
            .map_err(map_config_error)
    }

    async fn get_connection(
        &self,
        connection_id: ConnectionId,
    ) -> Result<ConfigConnectionSnapshot, ConfigPortError> {
        let snapshot = self.store.snapshot();
        let connection = snapshot
            .config
            .connections
            .into_iter()
            .find(|connection| connection.id == connection_id)
            .ok_or(ConfigPortError::NotFound)?;
        Ok(ConfigConnectionSnapshot {
            config_revision: snapshot.revision,
            connection: public_connection(connection),
        })
    }

    async fn list_connections(&self) -> Result<PublicConnectionList, ConfigPortError> {
        let snapshot = self.store.snapshot();
        Ok(PublicConnectionList {
            config_revision: snapshot.revision,
            connections: snapshot
                .config
                .connections
                .into_iter()
                .map(public_connection)
                .collect(),
        })
    }
}

struct ProductionSecrets {
    store: Arc<ConfigStore>,
    broker: Arc<CredentialBroker>,
}

#[async_trait]
impl SecretPort for ProductionSecrets {
    async fn set_connection_credential(
        &self,
        command: SetConnectionCredentialCommand,
    ) -> Result<SecretWriteOutcome, SecretPortError> {
        let snapshot = self.store.snapshot();
        let connection = snapshot
            .config
            .connections
            .iter()
            .find(|connection| connection.id == command.connection_id)
            .ok_or(SecretPortError::ConnectionNotFound)?;
        let binding = connection
            .credentials
            .iter()
            .find(|binding| binding.slot == command.slot)
            .ok_or(SecretPortError::BindingNotFound)?;
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos();
        let context = SecretAccessContext {
            connection_id: connection.id,
            slot: command.slot,
            purpose: SecretAccessPurpose::ValidateCredential,
            request_id: OperationId::new(format!("desktop-{nonce}"))
                .map_err(|_| SecretPortError::Failed)?,
        };
        let outcome = self
            .broker
            .put(
                std::slice::from_ref(connection),
                binding,
                &context,
                command.value.as_secret(),
                PutSecretOptions {
                    overwrite: command.overwrite,
                },
                &BrokerCancellation::default(),
            )
            .await
            .map_err(SecretPortError::from)?;
        Ok(SecretWriteOutcome {
            overwritten: outcome == PutSecretOutcome::Replaced,
        })
    }
}

struct ProductionCache(Arc<CacheStore>);

#[async_trait]
impl CachePort for ProductionCache {
    async fn clear(&self) -> Result<(), CachePortError> {
        self.0.clear().map_err(|_| CachePortError::Failed)
    }
}

struct ProductionArtifacts {
    metadata: Arc<SqliteStore>,
    blobs: Arc<ArtifactStore>,
}

#[async_trait]
impl ArtifactPort for ProductionArtifacts {
    async fn list_artifacts_for_work_item(
        &self,
        work_item_id: WorkItemId,
    ) -> Result<Vec<ArtifactRecord>, ArtifactPortError> {
        self.metadata
            .list_artifact_metadata(work_item_id)
            .await
            .map_err(|_| ArtifactPortError::Failed)
    }

    async fn open_artifact(
        &self,
        command: OpenArtifactCommand,
    ) -> Result<OpenArtifactPayload, ArtifactPortError> {
        let artifact = self
            .metadata
            .get_artifact_metadata(command.artifact_id)
            .await
            .map_err(|error| match error {
                yakshed_application::StoreError::NotFound { .. } => ArtifactPortError::NotFound,
                _ => ArtifactPortError::Failed,
            })?;
        let blobs = self.blobs.clone();
        let digest = artifact.digest.clone();
        let bytes = tokio::task::spawn_blocking(move || {
            let mut reader = blobs
                .open(&digest, command.max_bytes)
                .map_err(map_artifact_error)?;
            let mut bytes = Vec::new();
            reader
                .read_to_end(&mut bytes)
                .map_err(|_| ArtifactPortError::Failed)?;
            Ok(bytes)
        })
        .await
        .map_err(|_| ArtifactPortError::Failed)??;
        Ok(OpenArtifactPayload { artifact, bytes })
    }
}

struct CodexHarness {
    paths: AppPaths,
    config: Arc<ConfigStore>,
    run_state: StdMutex<RunState>,
    event_sender: mpsc::Sender<HarnessEvent>,
    events: Mutex<mpsc::Receiver<HarnessEvent>>,
    event_ready: Notify,
}

#[derive(Default)]
struct RunState {
    providers: HashMap<ConnectionId, ProviderState>,
    runs: HashMap<ProviderRunRef, (ConnectionId, RunId, ProviderRunHandle)>,
    native_refs: HashMap<ProviderRunHandle, ProviderRunRef>,
    correlations: HashMap<RunId, ProviderRunRef>,
    pending_events: HashMap<ProviderRunHandle, VecDeque<HarnessEvent>>,
    replay_events: VecDeque<HarnessEvent>,
}

struct ProviderState {
    provider_state: String,
    adapter: Arc<CodexAdapter>,
}

impl CodexHarness {
    fn new(paths: AppPaths, config: Arc<ConfigStore>) -> Self {
        let (event_sender, events) = mpsc::channel(128);
        Self {
            paths,
            config,
            run_state: StdMutex::new(RunState::default()),
            event_sender,
            events: Mutex::new(events),
            event_ready: Notify::new(),
        }
    }

    fn provider(
        paths: &AppPaths,
        connection: &Connection,
    ) -> Result<(Arc<CodexAdapter>, ProviderEventStream), HarnessPortError> {
        let connection_id = connection.id;
        let runtime =
            RuntimeHandle::new(format!("codex-{connection_id}")).map_err(map_harness_error)?;
        let spec = CodexRuntimeSpec::local(
            runtime.clone(),
            CodexRuntimeKey {
                connection_id,
                binary_digest: "runtime-path:codex".to_owned(),
                codex_home: paths
                    .data_root
                    .join("codex")
                    .join(connection.provider_state.as_str()),
                execution_runtime: "local".to_owned(),
            },
            PathBuf::from("codex"),
            vec!["app-server".to_owned()],
        );
        let adapter = Arc::new(CodexAdapter::new(spec).map_err(map_harness_error)?);
        let stream = adapter.subscribe().map_err(map_harness_error)?;
        Ok((adapter, stream))
    }

    fn forward(mut stream: ProviderEventStream, sender: mpsc::Sender<HarnessEvent>) {
        tokio::spawn(async move {
            while let Some(event) = stream.recv().await {
                if sender.send(event).await.is_err() {
                    break;
                }
            }
        });
    }

    fn adapter(&self, connection: &Connection) -> Result<Arc<CodexAdapter>, HarnessPortError> {
        let mut state = self
            .run_state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if let Some(provider) = state.providers.get(&connection.id) {
            if provider.provider_state == connection.provider_state.as_str() {
                return Ok(provider.adapter.clone());
            }
            if state
                .runs
                .values()
                .any(|(connection_id, _, _)| *connection_id == connection.id)
            {
                return Err(HarnessPortError::Conflict(format!(
                    "connection {} changed provider state while active",
                    connection.id
                )));
            }
            state.providers.remove(&connection.id);
        }
        let (adapter, stream) = Self::provider(&self.paths, connection)?;
        Self::forward(stream, self.event_sender.clone());
        state.providers.insert(
            connection.id,
            ProviderState {
                provider_state: connection.provider_state.to_string(),
                adapter: adapter.clone(),
            },
        );
        Ok(adapter)
    }

    fn register_run(
        &self,
        connection_id: ConnectionId,
        correlation_id: RunId,
        native: ProviderRunHandle,
    ) -> Result<ProviderRunRef, HarnessPortError> {
        let run = ProviderRunRef::from_parts(
            "codex",
            native.runtime().as_str(),
            native.session_id().as_str(),
            native.native_id().as_str(),
        )
        .map_err(|error| HarnessPortError::InvalidInput(error.to_string()))?;
        let mut state = self
            .run_state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        state.native_refs.insert(native.clone(), run.clone());
        state
            .runs
            .insert(run.clone(), (connection_id, correlation_id, native.clone()));
        state.correlations.insert(correlation_id, run.clone());
        if let Some(events) = state.pending_events.remove(&native) {
            state.replay_events.extend(events);
            self.event_ready.notify_one();
        }
        Ok(run)
    }

    fn native_run(
        &self,
        run: &ProviderRunRef,
    ) -> Result<(Arc<CodexAdapter>, ProviderRunHandle), HarnessPortError> {
        let state = self
            .run_state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let (connection_id, _, native) = state
            .runs
            .get(run)
            .cloned()
            .ok_or_else(|| HarnessPortError::NotFound(run.native_id().to_owned()))?;
        let adapter = state
            .providers
            .get(&connection_id)
            .map(|provider| provider.adapter.clone())
            .ok_or_else(|| HarnessPortError::NotFound(connection_id.to_string()))?;
        Ok((adapter, native))
    }

    fn run_ref(&self, run: &ProviderRunHandle) -> Result<ProviderRunRef, HarnessPortError> {
        self.run_state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .native_refs
            .get(run)
            .cloned()
            .ok_or_else(|| HarnessPortError::NotFound(run.to_string()))
    }

    fn retire_run(&self, native: &ProviderRunHandle) {
        let mut state = self
            .run_state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let Some(run) = state.native_refs.remove(native) else {
            return;
        };
        if let Some((_, correlation_id, _)) = state.runs.remove(&run) {
            state.correlations.remove(&correlation_id);
        }
    }
}

#[async_trait]
impl RunHarness for CodexHarness {
    async fn start_run(
        &self,
        connection_id: ConnectionId,
        correlation_id: RunId,
        input: String,
    ) -> Result<ProviderRunRef, HarnessPortError> {
        if let Some(run) = self
            .run_state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .correlations
            .get(&correlation_id)
            .cloned()
        {
            return Ok(run);
        }
        let connection = self
            .config
            .snapshot()
            .config
            .connections
            .into_iter()
            .find(|connection| connection.id == connection_id)
            .ok_or_else(|| HarnessPortError::NotFound(connection_id.to_string()))?;
        if connection.harness != "codex" {
            return Err(HarnessPortError::Unsupported(format!(
                "connection {} uses harness {}",
                connection.id, connection.harness
            )));
        }
        let adapter = self.adapter(&connection)?;
        let session = adapter
            .start_session(
                &RuntimeHandle::new(format!("codex-{connection_id}")).map_err(map_harness_error)?,
                StartSessionSpec {
                    working_directory: run_working_directory(self.paths.clone(), correlation_id)
                        .await?,
                    title: format!("YakShed run {correlation_id}"),
                },
            )
            .await
            .map_err(map_harness_error)?;
        let native = adapter
            .start_run(
                &session,
                HarnessInput::new(input).map_err(map_harness_error)?,
                RunOptions::default(),
            )
            .await
            .map_err(map_harness_error)?;
        self.register_run(connection_id, correlation_id, native)
    }

    async fn lookup_run(
        &self,
        connection_id: ConnectionId,
        correlation_id: RunId,
    ) -> Result<Option<ProviderRunRef>, HarnessPortError> {
        let state = self
            .run_state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let run = state
            .correlations
            .get(&correlation_id)
            .filter(|run| {
                state
                    .runs
                    .get(*run)
                    .is_some_and(|(owner, _, _)| *owner == connection_id)
            })
            .cloned();
        Ok(run)
    }

    async fn steer(&self, run: &ProviderRunRef, input: String) -> Result<(), HarnessPortError> {
        let (adapter, native) = self.native_run(run)?;
        adapter
            .steer(
                &native,
                HarnessInput::new(input).map_err(map_harness_error)?,
            )
            .await
            .map_err(map_harness_error)
    }

    async fn interrupt(&self, run: &ProviderRunRef) -> Result<(), HarnessPortError> {
        let (adapter, native) = self.native_run(run)?;
        adapter.interrupt(&native).await.map_err(map_harness_error)
    }

    async fn respond(
        &self,
        request: ProviderRequestRef,
        response: HarnessResponse,
    ) -> Result<(), HarnessPortError> {
        let (adapter, native) = self.native_run(&request.run)?;
        let request = ProviderRequestHandle::new(
            native,
            request.native_id.parse().map_err(map_harness_error)?,
        );
        let response = match response {
            HarnessResponse::Approval(decision) => ProviderResponse::Approval(decision),
            HarnessResponse::UserInput(input) => ProviderResponse::UserInput(input),
        };
        adapter
            .respond_to_request(request, response)
            .await
            .map_err(map_harness_error)
    }

    async fn next_event(&self) -> Result<Option<RunHarnessEvent>, HarnessPortError> {
        loop {
            let replay = {
                self.run_state
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .replay_events
                    .pop_front()
            };
            if let Some(event) = replay {
                return self.convert_event(event).map(Some);
            }
            let notified = self.event_ready.notified();
            let mut events = self.events.lock().await;
            let event = tokio::select! {
                biased;
                () = notified => continue,
                event = events.recv() => event,
            };
            drop(events);
            let Some(event) = event else {
                return Ok(None);
            };
            let mut state = self
                .run_state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if let Some(native) = event_run(&event)
                && !state.native_refs.contains_key(native)
            {
                if state
                    .pending_events
                    .values()
                    .map(VecDeque::len)
                    .sum::<usize>()
                    >= PENDING_EVENT_CAPACITY
                {
                    return Err(HarnessPortError::Overloaded);
                }
                state
                    .pending_events
                    .entry(native.clone())
                    .or_default()
                    .push_back(event);
                continue;
            }
            drop(state);
            return self.convert_event(event).map(Some);
        }
    }

    async fn reconnect(&self, run: &ProviderRunRef) -> Result<bool, HarnessPortError> {
        Ok(self
            .run_state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .runs
            .contains_key(run))
    }
}

impl CodexHarness {
    fn convert_event(&self, event: HarnessEvent) -> Result<RunHarnessEvent, HarnessPortError> {
        let terminal = match &event {
            HarnessEvent::RunTerminal { run, .. } => Some(run.clone()),
            _ => None,
        };
        let converted = match event {
            HarnessEvent::RunAccepted { run, .. } => RunHarnessEvent::RunAccepted {
                run: self.run_ref(&run)?,
            },
            HarnessEvent::MessageDelta { run, chunk, .. } => RunHarnessEvent::MessageDelta {
                run: self.run_ref(&run)?,
                chunk,
            },
            HarnessEvent::MessageCompleted { run, text, .. } => RunHarnessEvent::MessageCompleted {
                run: self.run_ref(&run)?,
                text,
            },
            HarnessEvent::ApprovalRequested {
                request, summary, ..
            } => RunHarnessEvent::ApprovalRequested {
                request: self.request_ref(&request)?,
                summary,
            },
            HarnessEvent::UserInputRequested {
                request, prompt, ..
            } => RunHarnessEvent::UserInputRequested {
                request: self.request_ref(&request)?,
                prompt,
            },
            HarnessEvent::FileMutation {
                run, path, summary, ..
            } => RunHarnessEvent::FileMutation {
                run: self.run_ref(&run)?,
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
                run: self.run_ref(&run)?,
                command: ProviderCommandRef {
                    run: self.run_ref(command.run())?,
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
                run: self.run_ref(&run)?,
                command: ProviderCommandRef {
                    run: self.run_ref(command.run())?,
                    native_id: command.native_id().to_string(),
                },
                command_text,
                output,
            },
            HarnessEvent::RunTerminal { run, state, .. } => RunHarnessEvent::RunTerminal {
                run: self.run_ref(&run)?,
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
                run: match run.as_ref() {
                    Some(run) => Some(self.run_ref(run)?),
                    None => None,
                },
                item_type,
                native: native.sanitized_raw().to_owned(),
            },
            HarnessEvent::MalformedNativePayload {
                run,
                item_type,
                native,
            } => RunHarnessEvent::Malformed {
                run: match run.as_ref() {
                    Some(run) => Some(self.run_ref(run)?),
                    None => None,
                },
                item_type,
                native: native.sanitized_raw().to_owned(),
            },
        };
        if let Some(native) = terminal {
            self.retire_run(&native);
        }
        Ok(converted)
    }

    fn request_ref(
        &self,
        request: &ProviderRequestHandle,
    ) -> Result<ProviderRequestRef, HarnessPortError> {
        Ok(ProviderRequestRef {
            run: self.run_ref(request.run())?,
            native_id: request.native_id().to_string(),
        })
    }
}

fn event_run(event: &HarnessEvent) -> Option<&ProviderRunHandle> {
    match event {
        HarnessEvent::RunAccepted { run, .. }
        | HarnessEvent::MessageDelta { run, .. }
        | HarnessEvent::MessageCompleted { run, .. }
        | HarnessEvent::FileMutation { run, .. }
        | HarnessEvent::CommandOutputDelta { run, .. }
        | HarnessEvent::CommandOutputCompleted { run, .. }
        | HarnessEvent::RunTerminal { run, .. } => Some(run),
        HarnessEvent::ApprovalRequested { request, .. }
        | HarnessEvent::UserInputRequested { request, .. } => Some(request.run()),
        HarnessEvent::Unknown { run, .. } | HarnessEvent::MalformedNativePayload { run, .. } => {
            run.as_ref()
        }
    }
}

async fn run_working_directory(
    paths: AppPaths,
    correlation_id: RunId,
) -> Result<RuntimePath, HarnessPortError> {
    tokio::task::spawn_blocking(move || {
        use std::os::unix::fs::PermissionsExt;

        let path = paths
            .data_root
            .join("runs")
            .join(correlation_id.to_string());
        fs::create_dir_all(&path).map_err(|error| HarnessPortError::Runtime(error.to_string()))?;
        fs::set_permissions(&path, fs::Permissions::from_mode(0o700))
            .map_err(|error| HarnessPortError::Runtime(error.to_string()))?;
        RuntimePath::new(path.to_string_lossy().into_owned()).map_err(map_harness_error)
    })
    .await
    .map_err(|error| HarnessPortError::Runtime(error.to_string()))?
}

fn public_connection(connection: Connection) -> PublicConnection {
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

fn map_config_error(error: ConfigError) -> ConfigPortError {
    match error {
        ConfigError::Conflict { expected, actual } => {
            ConfigPortError::Conflict { expected, actual }
        }
        ConfigError::Validation(_) | ConfigError::SecretBackendConfiguration(_) => {
            ConfigPortError::Validation
        }
        ConfigError::UnsupportedSchema { .. }
        | ConfigError::Parse(_)
        | ConfigError::Serialize(_)
        | ConfigError::Worker(_)
        | ConfigError::Io { .. } => ConfigPortError::Unavailable,
    }
}

fn map_artifact_error(error: ArtifactError) -> ArtifactPortError {
    match error {
        ArtifactError::NotFound(_) => ArtifactPortError::NotFound,
        ArtifactError::BoundExceeded { .. } | ArtifactError::TooLarge { .. } => {
            ArtifactPortError::TooLarge
        }
        _ => ArtifactPortError::Failed,
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

#[cfg(test)]
mod tests {
    use super::*;
    use yakshed_application::{
        AppStore, ConfigRevision, CreateProject, CreateWorkItem, IdGenerator,
    };
    use yakshed_domain::{
        ArtifactId, ArtifactKind, ArtifactProvenance, CredentialBindingRecord, CredentialSlot,
        ProviderStateRootId, SecretLocator, SecretReference,
    };
    use yakshed_harness::{NativePayload, ProviderRunId, ProviderSessionId};
    use yakshed_store::ArtifactMetadata;

    fn test_config(paths: &AppPaths) -> Arc<ConfigStore> {
        Arc::new(ConfigStore::open(paths.clone(), backend_capabilities()).unwrap())
    }

    fn connection(id: &str, harness: &str, provider_state: &str) -> Connection {
        Connection {
            id: id.parse().unwrap(),
            name: "test".to_owned(),
            harness: harness.to_owned(),
            model_provider: "test".to_owned(),
            provider_state: ProviderStateRootId::new(provider_state).unwrap(),
            credentials: vec![],
        }
    }

    #[tokio::test]
    async fn production_config_accepts_a_secret_backed_connection() {
        let temp = tempfile::tempdir().unwrap();
        let paths = AppPaths::for_test(temp.path());
        let backend = local_backend(&paths);
        let store = Arc::new(ConfigStore::open(paths, backend_capabilities()).unwrap());
        let port = ProductionConfig {
            store,
            local_backend: backend.clone(),
        };
        let connection = Connection {
            id: "0193f26e-7a72-7000-8000-00000000d001".parse().unwrap(),
            name: "Codex".to_owned(),
            harness: "codex".to_owned(),
            model_provider: "openai".to_owned(),
            provider_state: ProviderStateRootId::new("codex-test").unwrap(),
            credentials: vec![CredentialBindingRecord {
                slot: CredentialSlot::new("codex.account").unwrap(),
                binding: CredentialBinding::Secret {
                    reference: SecretReference {
                        backend_id: backend.id.clone(),
                        locator: SecretLocator::new("codex/account").unwrap(),
                    },
                },
            }],
        };

        let snapshot = port
            .put_connection(PutConnectionCommand {
                expected_config_revision: yakshed_application::ConfigRevision::INITIAL,
                connection,
            })
            .await
            .unwrap();

        assert_eq!(snapshot.config.secret_backends, vec![backend]);
        assert_eq!(snapshot.config.connections.len(), 1);
    }

    #[tokio::test]
    async fn production_artifacts_join_persisted_metadata_to_blob_reads() {
        let temp = tempfile::tempdir().unwrap();
        let paths = AppPaths::for_test(temp.path());
        paths.create_data_root().unwrap();
        let clock = Arc::new(SystemClock);
        let ids: Arc<dyn IdGenerator> = Arc::new(SystemIdGenerator);
        let metadata = Arc::new(
            SqliteStore::open(paths.clone(), clock, ids.clone())
                .await
                .unwrap(),
        );
        let project_id = ids.next_project_id();
        metadata
            .create_project(CreateProject {
                id: project_id,
                name: "test".to_owned(),
            })
            .await
            .unwrap();
        let work_item_id = ids.next_work_item_id();
        metadata
            .create_work_item(CreateWorkItem {
                id: work_item_id,
                project_id,
                title: "artifact".to_owned(),
                parent_id: None,
            })
            .await
            .unwrap();
        let blobs = Arc::new(ArtifactStore::new(&paths, 1024).unwrap());
        let record = blobs
            .publish(
                b"persisted body".as_slice(),
                ArtifactMetadata {
                    id: ArtifactId::new_v7(),
                    work_item_id,
                    run_id: None,
                    kind: ArtifactKind::Plan,
                    media_type: "text/plain".to_owned(),
                    provenance: ArtifactProvenance::new("test").unwrap(),
                },
            )
            .unwrap();
        metadata
            .put_artifact_metadata(record.clone())
            .await
            .unwrap();
        metadata.shutdown().await.unwrap();
        drop(metadata);
        let metadata = Arc::new(
            SqliteStore::open(paths, Arc::new(SystemClock), ids)
                .await
                .unwrap(),
        );
        let port = ProductionArtifacts { metadata, blobs };

        assert_eq!(
            port.list_artifacts_for_work_item(work_item_id)
                .await
                .unwrap(),
            vec![record.clone()]
        );
        let opened = port
            .open_artifact(OpenArtifactCommand {
                artifact_id: record.id,
                max_bytes: 1024,
            })
            .await
            .unwrap();
        assert_eq!(opened.artifact, record);
        assert_eq!(opened.bytes, b"persisted body");
    }

    #[tokio::test]
    async fn unknown_and_wrong_harness_connections_are_rejected() {
        let temp = tempfile::tempdir().unwrap();
        let paths = AppPaths::for_test(temp.path());
        let config = test_config(&paths);
        let wrong = connection("0193f26e-7a72-7000-8000-00000000d002", "mock", "mock-test");
        config
            .update(
                ConfigRevision::INITIAL,
                ConfigChange::PutConnection(wrong.clone()),
            )
            .await
            .unwrap();
        let harness = CodexHarness::new(paths, config);

        assert!(matches!(
            harness
                .start_run(
                    "0193f26e-7a72-7000-8000-00000000ffff".parse().unwrap(),
                    SystemIdGenerator.next_run_id(),
                    "test".to_owned(),
                )
                .await,
            Err(HarnessPortError::NotFound(_))
        ));
        assert!(matches!(
            harness
                .start_run(wrong.id, SystemIdGenerator.next_run_id(), "test".to_owned(),)
                .await,
            Err(HarnessPortError::Unsupported(_))
        ));
    }

    #[tokio::test]
    async fn early_event_replays_after_lossless_registration_and_terminal_retires_run() {
        let temp = tempfile::tempdir().unwrap();
        let paths = AppPaths::for_test(temp.path());
        let config = test_config(&paths);
        let harness = Arc::new(CodexHarness::new(paths, config));
        let codex = connection(
            "0193f26e-7a72-7000-8000-00000000d003",
            "codex",
            "codex-long-run",
        );
        let adapter = harness.adapter(&codex).unwrap();
        assert!(Arc::ptr_eq(&adapter, &harness.adapter(&codex).unwrap()));
        let component = "x".repeat(4096);
        let native = ProviderRunHandle::new(
            RuntimeHandle::new(component.clone()).unwrap(),
            ProviderSessionId::new(component.clone()).unwrap(),
            ProviderRunId::new(component.clone()).unwrap(),
        );
        assert!(ProviderRunRef::new("codex", native.to_string()).is_err());

        harness
            .event_sender
            .send(HarnessEvent::RunAccepted {
                run: native.clone(),
                native: NativePayload::sanitized("{}"),
            })
            .await
            .unwrap();
        let waiting = tokio::spawn({
            let harness = harness.clone();
            async move { harness.next_event().await }
        });
        for _ in 0..100 {
            let buffered = harness
                .run_state
                .lock()
                .unwrap()
                .pending_events
                .contains_key(&native);
            if buffered {
                break;
            }
            tokio::task::yield_now().await;
        }
        assert!(
            harness
                .run_state
                .lock()
                .unwrap()
                .pending_events
                .contains_key(&native),
            "early event was not buffered"
        );
        let correlation_id = SystemIdGenerator.next_run_id();
        let run = harness
            .register_run(codex.id, correlation_id, native.clone())
            .unwrap();

        assert_eq!(
            waiting.await.unwrap().unwrap(),
            Some(RunHarnessEvent::RunAccepted { run })
        );
        let run = harness
            .lookup_run(codex.id, correlation_id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(run.runtime_id(), component);
        assert_eq!(run.session_id(), component);
        assert_eq!(run.native_id(), component);
        assert_eq!(harness.native_run(&run).unwrap().1, native);

        harness
            .event_sender
            .send(HarnessEvent::RunTerminal {
                run: native,
                state: HarnessRunTerminal::Completed,
                native: NativePayload::sanitized("{}"),
            })
            .await
            .unwrap();
        assert!(matches!(
            harness.next_event().await.unwrap(),
            Some(RunHarnessEvent::RunTerminal { .. })
        ));
        let state = harness.run_state.lock().unwrap();
        assert_eq!(state.providers.len(), 1);
        assert!(state.runs.is_empty());
        assert!(state.native_refs.is_empty());
        assert!(state.correlations.is_empty());
        drop(state);

        let changed = connection(
            "0193f26e-7a72-7000-8000-00000000d003",
            "codex",
            "codex-reconfigured",
        );
        let replacement = harness.adapter(&changed).unwrap();
        assert!(!Arc::ptr_eq(&adapter, &replacement));
        assert_eq!(harness.run_state.lock().unwrap().providers.len(), 1);
    }

    #[tokio::test]
    async fn each_run_gets_its_deterministic_private_working_directory() {
        use std::os::unix::fs::PermissionsExt;

        let temp = tempfile::tempdir().unwrap();
        let paths = AppPaths::for_test(temp.path());
        let run_id = SystemIdGenerator.next_run_id();
        let other_run_id = SystemIdGenerator.next_run_id();
        let directory = run_working_directory(paths.clone(), run_id).await.unwrap();
        let other_directory = run_working_directory(paths.clone(), other_run_id)
            .await
            .unwrap();
        let expected = paths.data_root.join("runs").join(run_id.to_string());

        assert_eq!(directory.as_str(), expected.to_string_lossy());
        assert_ne!(directory, other_directory);
        assert_eq!(
            fs::metadata(expected).unwrap().permissions().mode() & 0o777,
            0o700
        );
    }
}
