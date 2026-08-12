use std::{
    collections::{HashMap, hash_map::Entry},
    path::PathBuf,
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use async_trait::async_trait;
use provider_codex::{CodexAdapter, CodexRuntimeKey, CodexRuntimeSpec};
use secrecy::SecretString;
use tokio::sync::{Mutex, mpsc};
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
    ProviderEventStream, ProviderRequestHandle, ProviderResponse, ProviderRunHandle,
    ProviderSession, RunOptions, RuntimeHandle, RuntimePath, StartSessionSpec,
};
use yakshed_secrets::{
    BrokerCancellation, CredentialBroker, LocalFileBackend, NoopSecretAuditSink, PutSecretOptions,
    PutSecretOutcome, SecretAccessContext, SecretAccessPurpose, SecretBackendHandle,
    backend_capabilities,
};
use yakshed_store::{AppPaths, ArtifactStore, CacheStore, ConfigError, ConfigStore, SqliteStore};

const LOCAL_BACKEND_ID: &str = "local-file";
const MAX_ARTIFACT_BYTES: u64 = 64 * 1024 * 1024;

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
    let harness = Arc::new(CodexHarness::new(
        paths.clone(),
        &config_store.snapshot().config.connections,
    )?);
    let cache = Arc::new(CacheStore::open(&paths).map_err(|_| StartupError::persistence())?);
    let artifacts = Arc::new(
        ArtifactStore::new(&paths, MAX_ARTIFACT_BYTES).map_err(|_| StartupError::persistence())?,
    );

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
            artifacts: Arc::new(ProductionArtifacts { _store: artifacts }),
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
        if command.ensure_memory_secret_backend {
            return Err(ConfigPortError::Unsupported);
        }
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
                &SecretString::from(command.value.expose()),
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
    _store: Arc<ArtifactStore>,
}

#[async_trait]
impl ArtifactPort for ProductionArtifacts {
    async fn list_artifacts_for_work_item(
        &self,
        work_item_id: WorkItemId,
    ) -> Result<Vec<ArtifactRecord>, ArtifactPortError> {
        let _ = work_item_id;
        Ok(Vec::new())
    }

    async fn open_artifact(
        &self,
        command: OpenArtifactCommand,
    ) -> Result<OpenArtifactPayload, ArtifactPortError> {
        let _ = command;
        Err(ArtifactPortError::NotFound)
    }
}

struct ProviderState {
    adapter: Arc<CodexAdapter>,
    session: Option<ProviderSession>,
}

struct CodexHarness {
    paths: AppPaths,
    providers: Mutex<HashMap<ConnectionId, ProviderState>>,
    runs: Mutex<HashMap<ProviderRunRef, (ConnectionId, ProviderRunHandle)>>,
    correlations: Mutex<HashMap<RunId, ProviderRunRef>>,
    event_sender: mpsc::Sender<HarnessEvent>,
    events: Mutex<mpsc::Receiver<HarnessEvent>>,
}

impl CodexHarness {
    fn new(paths: AppPaths, connections: &[Connection]) -> Result<Self, StartupError> {
        let (event_sender, events) = mpsc::channel(128);
        let mut providers = HashMap::new();
        for connection in connections {
            if connection.harness != "codex" {
                continue;
            }
            let (state, stream) =
                Self::provider(&paths, connection.id).map_err(|_| StartupError::internal())?;
            Self::forward(stream, event_sender.clone());
            providers.insert(connection.id, state);
        }
        Ok(Self {
            paths,
            providers: Mutex::new(providers),
            runs: Mutex::new(HashMap::new()),
            correlations: Mutex::new(HashMap::new()),
            event_sender,
            events: Mutex::new(events),
        })
    }

    fn provider(
        paths: &AppPaths,
        connection_id: ConnectionId,
    ) -> Result<(ProviderState, ProviderEventStream), HarnessPortError> {
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
                    .join(connection_id.to_string()),
                execution_runtime: "local".to_owned(),
            },
            PathBuf::from("codex"),
            vec!["app-server".to_owned()],
        );
        let adapter = Arc::new(CodexAdapter::new(spec).map_err(map_harness_error)?);
        let stream = adapter.subscribe().map_err(map_harness_error)?;
        Ok((
            ProviderState {
                adapter,
                session: None,
            },
            stream,
        ))
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

    fn run_ref(run: &ProviderRunHandle) -> Result<ProviderRunRef, HarnessPortError> {
        ProviderRunRef::new("codex", run.to_string())
            .map_err(|error| HarnessPortError::InvalidInput(error.to_string()))
    }

    async fn native_run(
        &self,
        run: &ProviderRunRef,
    ) -> Result<(Arc<CodexAdapter>, ProviderRunHandle), HarnessPortError> {
        let (connection_id, native) = self
            .runs
            .lock()
            .await
            .get(run)
            .cloned()
            .ok_or_else(|| HarnessPortError::NotFound(run.native_id().to_owned()))?;
        let providers = self.providers.lock().await;
        let adapter = providers
            .get(&connection_id)
            .map(|state| state.adapter.clone())
            .ok_or_else(|| HarnessPortError::NotFound(connection_id.to_string()))?;
        Ok((adapter, native))
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
        if let Some(run) = self.correlations.lock().await.get(&correlation_id).cloned() {
            return Ok(run);
        }
        // ponytail: one lock serializes starts; split per connection if startup contention appears.
        let mut providers = self.providers.lock().await;
        if let Entry::Vacant(entry) = providers.entry(connection_id) {
            let (state, stream) = Self::provider(&self.paths, connection_id)?;
            Self::forward(stream, self.event_sender.clone());
            entry.insert(state);
        }
        let state = providers
            .get_mut(&connection_id)
            .expect("provider inserted");
        let session = match &state.session {
            Some(session) => session.clone(),
            None => {
                let working_directory = std::env::current_dir()
                    .map_err(|error| HarnessPortError::Runtime(error.to_string()))?;
                let session = state
                    .adapter
                    .start_session(
                        &RuntimeHandle::new(format!("codex-{connection_id}"))
                            .map_err(map_harness_error)?,
                        StartSessionSpec {
                            working_directory: RuntimePath::new(
                                working_directory.to_string_lossy().into_owned(),
                            )
                            .map_err(map_harness_error)?,
                            title: "YakShed run".to_owned(),
                        },
                    )
                    .await
                    .map_err(map_harness_error)?;
                state.session = Some(session.clone());
                session
            }
        };
        let native = state
            .adapter
            .start_run(
                &session,
                HarnessInput::new(input).map_err(map_harness_error)?,
                RunOptions::default(),
            )
            .await
            .map_err(map_harness_error)?;
        let run = Self::run_ref(&native)?;
        drop(providers);
        self.runs
            .lock()
            .await
            .insert(run.clone(), (connection_id, native));
        self.correlations
            .lock()
            .await
            .insert(correlation_id, run.clone());
        Ok(run)
    }

    async fn lookup_run(
        &self,
        _connection_id: ConnectionId,
        correlation_id: RunId,
    ) -> Result<Option<ProviderRunRef>, HarnessPortError> {
        Ok(self.correlations.lock().await.get(&correlation_id).cloned())
    }

    async fn steer(&self, run: &ProviderRunRef, input: String) -> Result<(), HarnessPortError> {
        let (adapter, native) = self.native_run(run).await?;
        adapter
            .steer(
                &native,
                HarnessInput::new(input).map_err(map_harness_error)?,
            )
            .await
            .map_err(map_harness_error)
    }

    async fn interrupt(&self, run: &ProviderRunRef) -> Result<(), HarnessPortError> {
        let (adapter, native) = self.native_run(run).await?;
        adapter.interrupt(&native).await.map_err(map_harness_error)
    }

    async fn respond(
        &self,
        request: ProviderRequestRef,
        response: HarnessResponse,
    ) -> Result<(), HarnessPortError> {
        let (adapter, native) = self.native_run(&request.run).await?;
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
        Ok(self.events.lock().await.recv().await.map(convert_event))
    }

    async fn reconnect(&self, run: &ProviderRunRef) -> Result<bool, HarnessPortError> {
        Ok(self.runs.lock().await.contains_key(run))
    }
}

fn convert_event(event: HarnessEvent) -> RunHarnessEvent {
    match event {
        HarnessEvent::RunAccepted { run, .. } => RunHarnessEvent::RunAccepted {
            run: CodexHarness::run_ref(&run).expect("provider emitted a valid run"),
        },
        HarnessEvent::MessageDelta { run, chunk, .. } => RunHarnessEvent::MessageDelta {
            run: CodexHarness::run_ref(&run).expect("provider emitted a valid run"),
            chunk,
        },
        HarnessEvent::MessageCompleted { run, text, .. } => RunHarnessEvent::MessageCompleted {
            run: CodexHarness::run_ref(&run).expect("provider emitted a valid run"),
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
            run: CodexHarness::run_ref(&run).expect("provider emitted a valid run"),
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
            run: CodexHarness::run_ref(&run).expect("provider emitted a valid run"),
            command: ProviderCommandRef {
                run: CodexHarness::run_ref(command.run()).expect("provider emitted a valid run"),
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
            run: CodexHarness::run_ref(&run).expect("provider emitted a valid run"),
            command: ProviderCommandRef {
                run: CodexHarness::run_ref(command.run()).expect("provider emitted a valid run"),
                native_id: command.native_id().to_string(),
            },
            command_text,
            output,
        },
        HarnessEvent::RunTerminal { run, state, .. } => RunHarnessEvent::RunTerminal {
            run: CodexHarness::run_ref(&run).expect("provider emitted a valid run"),
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
            run: run
                .as_ref()
                .map(|run| CodexHarness::run_ref(run).expect("provider emitted a valid run")),
            item_type,
            native: native.sanitized_raw().to_owned(),
        },
        HarnessEvent::MalformedNativePayload {
            run,
            item_type,
            native,
        } => RunHarnessEvent::Malformed {
            run: run
                .as_ref()
                .map(|run| CodexHarness::run_ref(run).expect("provider emitted a valid run")),
            item_type,
            native: native.sanitized_raw().to_owned(),
        },
    }
}

fn request_ref(request: &ProviderRequestHandle) -> ProviderRequestRef {
    ProviderRequestRef {
        run: CodexHarness::run_ref(request.run()).expect("provider emitted a valid request"),
        native_id: request.native_id().to_string(),
    }
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
