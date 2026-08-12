use std::{
    collections::{HashMap, hash_map::Entry},
    fs,
    io::Read,
    path::PathBuf,
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use async_trait::async_trait;
use provider_codex::{CodexAdapter, CodexRuntimeKey, CodexRuntimeSpec};
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
        let mut reader = self
            .blobs
            .open(&artifact.digest, command.max_bytes)
            .map_err(map_artifact_error)?;
        let mut bytes = Vec::new();
        reader
            .read_to_end(&mut bytes)
            .map_err(|_| ArtifactPortError::Failed)?;
        Ok(OpenArtifactPayload { artifact, bytes })
    }
}

struct ProviderState {
    adapter: Arc<CodexAdapter>,
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
        Ok((ProviderState { adapter }, stream))
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
        let mut providers = self.providers.lock().await;
        if let Entry::Vacant(entry) = providers.entry(connection_id) {
            let (state, stream) = Self::provider(&self.paths, connection_id)?;
            Self::forward(stream, self.event_sender.clone());
            entry.insert(state);
        }
        let adapter = providers
            .get_mut(&connection_id)
            .expect("provider inserted")
            .adapter
            .clone();
        drop(providers);
        let session = adapter
            .start_session(
                &RuntimeHandle::new(format!("codex-{connection_id}")).map_err(map_harness_error)?,
                StartSessionSpec {
                    working_directory: run_working_directory(&self.paths, correlation_id)?,
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
        let run = Self::run_ref(&native)?;
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
        self.events
            .lock()
            .await
            .recv()
            .await
            .map(convert_event)
            .transpose()
    }

    async fn reconnect(&self, run: &ProviderRunRef) -> Result<bool, HarnessPortError> {
        Ok(self.runs.lock().await.contains_key(run))
    }
}

fn convert_event(event: HarnessEvent) -> Result<RunHarnessEvent, HarnessPortError> {
    Ok(match event {
        HarnessEvent::RunAccepted { run, .. } => RunHarnessEvent::RunAccepted {
            run: CodexHarness::run_ref(&run)?,
        },
        HarnessEvent::MessageDelta { run, chunk, .. } => RunHarnessEvent::MessageDelta {
            run: CodexHarness::run_ref(&run)?,
            chunk,
        },
        HarnessEvent::MessageCompleted { run, text, .. } => RunHarnessEvent::MessageCompleted {
            run: CodexHarness::run_ref(&run)?,
            text,
        },
        HarnessEvent::ApprovalRequested {
            request, summary, ..
        } => RunHarnessEvent::ApprovalRequested {
            request: request_ref(&request)?,
            summary,
        },
        HarnessEvent::UserInputRequested {
            request, prompt, ..
        } => RunHarnessEvent::UserInputRequested {
            request: request_ref(&request)?,
            prompt,
        },
        HarnessEvent::FileMutation {
            run, path, summary, ..
        } => RunHarnessEvent::FileMutation {
            run: CodexHarness::run_ref(&run)?,
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
            run: CodexHarness::run_ref(&run)?,
            command: ProviderCommandRef {
                run: CodexHarness::run_ref(command.run())?,
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
            run: CodexHarness::run_ref(&run)?,
            command: ProviderCommandRef {
                run: CodexHarness::run_ref(command.run())?,
                native_id: command.native_id().to_string(),
            },
            command_text,
            output,
        },
        HarnessEvent::RunTerminal { run, state, .. } => RunHarnessEvent::RunTerminal {
            run: CodexHarness::run_ref(&run)?,
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
            run: run.as_ref().map(CodexHarness::run_ref).transpose()?,
            item_type,
            native: native.sanitized_raw().to_owned(),
        },
        HarnessEvent::MalformedNativePayload {
            run,
            item_type,
            native,
        } => RunHarnessEvent::Malformed {
            run: run.as_ref().map(CodexHarness::run_ref).transpose()?,
            item_type,
            native: native.sanitized_raw().to_owned(),
        },
    })
}

fn request_ref(request: &ProviderRequestHandle) -> Result<ProviderRequestRef, HarnessPortError> {
    Ok(ProviderRequestRef {
        run: CodexHarness::run_ref(request.run())?,
        native_id: request.native_id().to_string(),
    })
}

fn run_working_directory(
    paths: &AppPaths,
    correlation_id: RunId,
) -> Result<RuntimePath, HarnessPortError> {
    use std::os::unix::fs::PermissionsExt;

    let path = paths
        .data_root
        .join("runs")
        .join(correlation_id.to_string());
    fs::create_dir_all(&path).map_err(|error| HarnessPortError::Runtime(error.to_string()))?;
    fs::set_permissions(&path, fs::Permissions::from_mode(0o700))
        .map_err(|error| HarnessPortError::Runtime(error.to_string()))?;
    RuntimePath::new(path.to_string_lossy().into_owned()).map_err(map_harness_error)
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
    use yakshed_application::{AppStore, CreateProject, CreateWorkItem, IdGenerator};
    use yakshed_domain::{
        ArtifactId, ArtifactKind, ArtifactProvenance, CredentialBindingRecord, CredentialSlot,
        ProviderStateRootId, SecretLocator, SecretReference,
    };
    use yakshed_harness::{NativePayload, ProviderRunId, ProviderSessionId};
    use yakshed_store::ArtifactMetadata;

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
    async fn long_provider_run_components_return_a_typed_event_error() {
        let temp = tempfile::tempdir().unwrap();
        let harness = CodexHarness::new(AppPaths::for_test(temp.path()), &[]).unwrap();
        let component = "x".repeat(4096);
        let run = ProviderRunHandle::new(
            RuntimeHandle::new(component.clone()).unwrap(),
            ProviderSessionId::new(component.clone()).unwrap(),
            ProviderRunId::new(component).unwrap(),
        );

        harness
            .event_sender
            .send(HarnessEvent::RunAccepted {
                run,
                native: NativePayload::sanitized("{}"),
            })
            .await
            .unwrap();

        assert!(matches!(
            harness.next_event().await,
            Err(HarnessPortError::InvalidInput(_))
        ));
    }

    #[test]
    fn each_run_gets_its_deterministic_private_working_directory() {
        use std::os::unix::fs::PermissionsExt;

        let temp = tempfile::tempdir().unwrap();
        let paths = AppPaths::for_test(temp.path());
        let run_id = SystemIdGenerator.next_run_id();
        let other_run_id = SystemIdGenerator.next_run_id();
        let directory = run_working_directory(&paths, run_id).unwrap();
        let other_directory = run_working_directory(&paths, other_run_id).unwrap();
        let expected = paths.data_root.join("runs").join(run_id.to_string());

        assert_eq!(directory.as_str(), expected.to_string_lossy());
        assert_ne!(directory, other_directory);
        assert_eq!(
            fs::metadata(expected).unwrap().permissions().mode() & 0o777,
            0o700
        );
    }
}
