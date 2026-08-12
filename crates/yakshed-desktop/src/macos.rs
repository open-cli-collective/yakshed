use std::{
    collections::{HashMap, VecDeque},
    fs,
    io::Read,
    path::{Path, PathBuf},
    sync::{Arc, Mutex as StdMutex},
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use async_trait::async_trait;
use provider_codex::{CodexAdapter, CodexRuntimeKey, CodexRuntimeSpec};
use tokio::sync::{Mutex, Notify, mpsc};
use yakshed_application::{
    ArtifactPort, ArtifactPortError, CachePort, CachePortError, ConfigChange,
    ConfigConnectionSnapshot, ConfigPort, ConfigPortError, ConfigSnapshot, CredentialCopyState,
    CredentialMigrationPendingReason, CredentialMigrationPhase, CredentialMigrationRecord,
    CredentialMigrationStatus, HarnessPortError, HarnessResponse, OpenArtifactCommand,
    OpenArtifactPayload, ProviderCommandRef, ProviderRequestRef, ProviderRunRef, PublicConnection,
    PublicConnectionList, PublicCredentialBinding, PublicCredentialSource, PutConnectionCommand,
    RunHarness, RunHarnessEvent, RunTerminal, SecretPort, SecretPortError, SecretWriteOutcome,
    SetConnectionCredentialCommand, SystemClock, SystemIdGenerator,
};
use yakshed_desktop_api::{ApiPorts, StartupError};
use yakshed_domain::{
    ArtifactRecord, Connection, ConnectionId, CredentialBinding, CredentialSlot, OperationId,
    RunId, SecretBackend, SecretBackendId, SecretBackendSettings, WorkItemId,
};
use yakshed_harness::{
    HarnessAdapter, HarnessError, HarnessEvent, HarnessInput, HarnessRunTerminal,
    ProviderEventStream, ProviderRequestHandle, ProviderResponse, ProviderRunHandle, RunOptions,
    RuntimeHandle, RuntimePath, StartSessionSpec,
};
use yakshed_secrets::{
    BrokerCancellation, CredentialBroker, LocalFileBackend, LocalOsBackend, NoopSecretAuditSink,
    PutSecretOptions, PutSecretOutcome, SecretAccessContext, SecretAccessPurpose,
    SecretAdministrator, SecretBackendHandle, SecretError, SecretResolver, backend_capabilities,
};
use yakshed_store::{
    AppPaths, ArtifactError, ArtifactStore, CacheStore, ConfigError, ConfigStore, SqliteStore,
};

const LOCAL_BACKEND_ID: &str = "local-os";
const LEGACY_BACKEND_ID: &str = "local-file";
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
    let local_backend = local_backend();
    let legacy_backend = legacy_backend(&paths);
    let local_os =
        LocalOsBackend::from_config(&local_backend).map_err(|_| StartupError::internal())?;
    let credential_migration = migrate_interim_secrets(
        &paths,
        &config_store,
        &legacy_backend,
        &local_backend,
        &local_os,
    )
    .await;
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
                credential_migration,
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

fn local_backend() -> SecretBackend {
    SecretBackend {
        id: SecretBackendId::new(LOCAL_BACKEND_ID).expect("constant backend id is valid"),
        settings: SecretBackendSettings::LocalOs,
    }
}

fn legacy_backend(paths: &AppPaths) -> SecretBackend {
    SecretBackend {
        id: SecretBackendId::new(LEGACY_BACKEND_ID).expect("constant backend id is valid"),
        settings: SecretBackendSettings::LocalFile {
            path: paths
                .data_root
                .join("secrets.json")
                .to_string_lossy()
                .into_owned(),
        },
    }
}

async fn migrate_interim_secrets<T>(
    paths: &AppPaths,
    store: &ConfigStore,
    legacy_config: &SecretBackend,
    target_config: &SecretBackend,
    target: &T,
) -> CredentialMigrationStatus
where
    T: SecretResolver + SecretAdministrator,
{
    let source_path = paths.data_root.join("secrets.json");
    let exists = match tokio::task::spawn_blocking({
        let source_path = source_path.clone();
        move || source_path.try_exists()
    })
    .await
    {
        Ok(Ok(exists)) => exists,
        _ => return pending(SecretErrorClass::Failed),
    };
    let mut snapshot = store.snapshot();
    if snapshot.config.credential_migration.is_none() {
        let legacy_configured = snapshot
            .config
            .secret_backends
            .iter()
            .any(|backend| backend == legacy_config);
        if !legacy_configured {
            if exists
                && configured_local_file_owns_path(
                    &snapshot.config.secret_backends,
                    &source_path,
                    None,
                )
                .await
            {
                return CredentialMigrationStatus::Pending(
                    CredentialMigrationPendingReason::SourceInUse,
                );
            }
            return if exists {
                pending(SecretErrorClass::Failed)
            } else {
                CredentialMigrationStatus::Ready
            };
        }
        if configured_local_file_owns_path(
            &snapshot.config.secret_backends,
            &source_path,
            Some(&legacy_config.id),
        )
        .await
        {
            return CredentialMigrationStatus::Pending(
                CredentialMigrationPendingReason::SourceInUse,
            );
        }
        snapshot = match store
            .update(
                snapshot.revision,
                ConfigChange::BeginCredentialMigration(CredentialMigrationRecord {
                    source: legacy_config.clone(),
                    target: target_config.clone(),
                    phase: CredentialMigrationPhase::Copying,
                    receipts: Vec::new(),
                }),
            )
            .await
        {
            Ok(snapshot) => snapshot,
            Err(_) => return pending(SecretErrorClass::Failed),
        };
    }
    let record = snapshot
        .config
        .credential_migration
        .clone()
        .expect("migration was initialized");
    if record.source != *legacy_config || record.target != *target_config {
        return pending(SecretErrorClass::Failed);
    }
    let source = match LocalFileBackend::from_config(&record.source) {
        Ok(source) => source,
        Err(_) => return pending(SecretErrorClass::Failed),
    };
    if record.phase == CredentialMigrationPhase::CleanupPending {
        if configured_local_file_owns_path(&snapshot.config.secret_backends, &source_path, None)
            .await
        {
            return CredentialMigrationStatus::Pending(
                CredentialMigrationPendingReason::SourceInUse,
            );
        }
        if exists && source.purge_after_migration().await.is_err() {
            return CredentialMigrationStatus::Pending(
                CredentialMigrationPendingReason::CleanupRequired,
            );
        }
        return match store
            .update(snapshot.revision, ConfigChange::FinishCredentialMigration)
            .await
        {
            Ok(_) => CredentialMigrationStatus::Ready,
            Err(_) => pending(SecretErrorClass::Failed),
        };
    }
    if !exists {
        return CredentialMigrationStatus::Pending(CredentialMigrationPendingReason::MissingSource);
    }
    let mut locators = match source.locators().await {
        Ok(locators) => locators,
        Err(error) => return pending(classify_secret_error(&error)),
    };
    locators.extend(snapshot.config.connections.iter().flat_map(|connection| {
        connection
            .credentials
            .iter()
            .filter_map(|credential| match &credential.binding {
                CredentialBinding::Secret { reference }
                    if reference.backend_id == record.source.id =>
                {
                    Some(reference.locator.clone())
                }
                _ => None,
            })
    }));
    locators.sort();
    locators.dedup();
    let context = SecretAccessContext {
        connection_id: "0193f26e-7a72-7000-8000-00000000f001"
            .parse()
            .expect("constant migration connection id is valid"),
        slot: CredentialSlot::new("migration").expect("constant migration slot is valid"),
        purpose: SecretAccessPurpose::ValidateCredential,
        request_id: OperationId::new("plaintext-migration")
            .expect("constant migration operation id is valid"),
    };
    let mut receipts = record
        .receipts
        .iter()
        .map(|receipt| (receipt.locator.clone(), receipt.state))
        .collect::<HashMap<_, _>>();
    for locator in locators {
        let secret = match source.resolve(&locator, &context).await {
            Ok(secret) => secret,
            Err(SecretError::NotFound { .. }) => {
                return CredentialMigrationStatus::Pending(
                    CredentialMigrationPendingReason::MissingSource,
                );
            }
            Err(error) => return pending(classify_secret_error(&error)),
        };
        let receipt = receipts.get(&locator).copied();
        let existing = match target.resolve(&locator, &context).await {
            Ok(existing) => Some(existing),
            Err(SecretError::NotFound { .. }) => None,
            Err(error) => return pending(classify_secret_error(&error)),
        };
        if let Some(existing) = existing {
            if receipt.is_none()
                || !secret.expose(|source| existing.expose(|target| source == target))
            {
                return CredentialMigrationStatus::Pending(
                    CredentialMigrationPendingReason::Collision,
                );
            }
        } else {
            if receipt == Some(CredentialCopyState::Copied) {
                return CredentialMigrationStatus::Pending(
                    CredentialMigrationPendingReason::Collision,
                );
            }
            if receipt.is_none() {
                snapshot = match store
                    .update(
                        snapshot.revision,
                        ConfigChange::RecordCredentialCopy {
                            locator: locator.clone(),
                            state: CredentialCopyState::Copying,
                        },
                    )
                    .await
                {
                    Ok(snapshot) => snapshot,
                    Err(_) => return pending(SecretErrorClass::Failed),
                };
                receipts.insert(locator.clone(), CredentialCopyState::Copying);
            }
            if let Err(error) = target
                .put(&locator, secret.as_secret(), PutSecretOptions::NO_OVERWRITE)
                .await
            {
                if matches!(error, SecretError::AlreadyExists { .. }) {
                    return CredentialMigrationStatus::Pending(
                        CredentialMigrationPendingReason::Collision,
                    );
                }
                return pending(classify_secret_error(&error));
            }
        }
        let migrated = match target.resolve(&locator, &context).await {
            Ok(migrated) => migrated,
            Err(error) => return pending(classify_secret_error(&error)),
        };
        if !secret.expose(|source| migrated.expose(|target| source == target)) {
            return pending(SecretErrorClass::Failed);
        }
        if receipt != Some(CredentialCopyState::Copied) {
            snapshot = match store
                .update(
                    snapshot.revision,
                    ConfigChange::RecordCredentialCopy {
                        locator: locator.clone(),
                        state: CredentialCopyState::Copied,
                    },
                )
                .await
            {
                Ok(snapshot) => snapshot,
                Err(_) => return pending(SecretErrorClass::Failed),
            };
            receipts.insert(locator, CredentialCopyState::Copied);
        }
    }
    snapshot = match store
        .update(
            snapshot.revision,
            ConfigChange::CheckpointCredentialMigration,
        )
        .await
    {
        Ok(snapshot) => snapshot,
        Err(_) => return pending(SecretErrorClass::Failed),
    };
    if configured_local_file_owns_path(&snapshot.config.secret_backends, &source_path, None).await {
        return CredentialMigrationStatus::Pending(CredentialMigrationPendingReason::SourceInUse);
    }
    if source.purge_after_migration().await.is_err() {
        return CredentialMigrationStatus::Pending(
            CredentialMigrationPendingReason::CleanupRequired,
        );
    }
    match store
        .update(snapshot.revision, ConfigChange::FinishCredentialMigration)
        .await
    {
        Ok(_) => CredentialMigrationStatus::Ready,
        Err(_) => pending(SecretErrorClass::Failed),
    }
}

async fn configured_local_file_owns_path(
    backends: &[SecretBackend],
    source_path: &Path,
    except: Option<&SecretBackendId>,
) -> bool {
    let paths = backends
        .iter()
        .filter(|backend| except != Some(&backend.id))
        .filter_map(|backend| match &backend.settings {
            SecretBackendSettings::LocalFile { path } => Some(PathBuf::from(path)),
            _ => None,
        })
        .collect::<Vec<_>>();
    let source_path = source_path.to_owned();
    tokio::task::spawn_blocking(move || {
        let source = fs::canonicalize(source_path).ok();
        source.is_some_and(|source| {
            paths
                .iter()
                .any(|path| fs::canonicalize(path).is_ok_and(|path| path == source))
        })
    })
    .await
    .unwrap_or(true)
}

#[derive(Clone, Copy)]
enum SecretErrorClass {
    Locked,
    Denied,
    Unavailable,
    Failed,
}

fn classify_secret_error(error: &SecretError) -> SecretErrorClass {
    match error {
        SecretError::Locked { .. } => SecretErrorClass::Locked,
        SecretError::Denied { .. } => SecretErrorClass::Denied,
        SecretError::BackendUnavailable { .. } => SecretErrorClass::Unavailable,
        _ => SecretErrorClass::Failed,
    }
}

fn pending(reason: SecretErrorClass) -> CredentialMigrationStatus {
    CredentialMigrationStatus::Pending(match reason {
        SecretErrorClass::Locked => CredentialMigrationPendingReason::Locked,
        SecretErrorClass::Denied => CredentialMigrationPendingReason::Denied,
        SecretErrorClass::Unavailable => CredentialMigrationPendingReason::Unavailable,
        SecretErrorClass::Failed => CredentialMigrationPendingReason::Failed,
    })
}

fn build_broker(
    snapshot: &ConfigSnapshot,
    fallback: &SecretBackend,
) -> Result<CredentialBroker, StartupError> {
    let mut configured = snapshot.config.secret_backends.clone();
    if !configured.iter().any(|backend| backend.id == fallback.id) {
        configured.push(fallback.clone());
    }
    let handles = build_backend_handles(configured)?;
    CredentialBroker::new(
        handles,
        &snapshot.config.connections,
        Arc::new(NoopSecretAuditSink),
        Duration::from_secs(5),
    )
    .map_err(|_| StartupError::internal())
}

fn build_backend_handles(
    configured: Vec<SecretBackend>,
) -> Result<Vec<(SecretBackendId, SecretBackendHandle)>, StartupError> {
    configured
        .into_iter()
        .filter_map(|config| {
            let id = config.id.clone();
            let handle = match config.settings {
                SecretBackendSettings::LocalFile { .. } => LocalFileBackend::from_config(&config)
                    .map(|backend| {
                        let backend = Arc::new(backend);
                        SecretBackendHandle {
                            resolver: backend.clone(),
                            administrator: Some(backend),
                        }
                    }),
                SecretBackendSettings::LocalOs => {
                    LocalOsBackend::from_config(&config).map(|backend| {
                        let backend = Arc::new(backend);
                        SecretBackendHandle {
                            resolver: backend.clone(),
                            administrator: Some(backend),
                        }
                    })
                }
                _ => return None,
            };
            Some(handle.map(|handle| (id, handle)))
        })
        .collect::<Result<_, _>>()
        .map_err(|_| StartupError::internal())
}

struct ProductionConfig {
    store: Arc<ConfigStore>,
    local_backend: SecretBackend,
    credential_migration: CredentialMigrationStatus,
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
            credential_migration: self.credential_migration,
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

    fn evict_idle_adapter(&self, connection_id: ConnectionId, adapter: &Arc<CodexAdapter>) -> bool {
        let mut state = self
            .run_state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let idle = !state
            .runs
            .values()
            .any(|(owner, _, _)| *owner == connection_id);
        let current = state
            .providers
            .get(&connection_id)
            .is_some_and(|provider| Arc::ptr_eq(&provider.adapter, adapter));
        if idle && current {
            state.providers.remove(&connection_id);
            true
        } else {
            false
        }
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

    fn retire_run(&self, native: &ProviderRunHandle, runtime_failed: bool) {
        let mut state = self
            .run_state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let Some(run) = state.native_refs.remove(native) else {
            return;
        };
        if let Some((connection_id, correlation_id, _)) = state.runs.remove(&run) {
            state.correlations.remove(&correlation_id);
            if runtime_failed
                && !state
                    .runs
                    .values()
                    .any(|(owner, _, _)| *owner == connection_id)
            {
                state.providers.remove(&connection_id);
            }
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
        let runtime =
            RuntimeHandle::new(format!("codex-{connection_id}")).map_err(map_harness_error)?;
        let session_spec = StartSessionSpec {
            working_directory: run_working_directory(self.paths.clone(), correlation_id).await?,
            title: format!("YakShed run {correlation_id}"),
        };
        let mut adapter = self.adapter(&connection)?;
        let session = match adapter.start_session(&runtime, session_spec.clone()).await {
            Err(HarnessError::Disconnected) if self.evict_idle_adapter(connection_id, &adapter) => {
                adapter = self.adapter(&connection)?;
                adapter
                    .start_session(&runtime, session_spec)
                    .await
                    .map_err(map_harness_error)?
            }
            result => result.map_err(map_harness_error)?,
        };
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
            let pending_full = self
                .run_state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .pending_events
                .values()
                .map(VecDeque::len)
                .sum::<usize>()
                >= PENDING_EVENT_CAPACITY;
            if pending_full {
                notified.await;
                continue;
            }
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
            HarnessEvent::RunTerminal { run, state, .. } => Some((
                run.clone(),
                matches!(state, HarnessRunTerminal::Crashed { .. }),
            )),
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
        if let Some((native, runtime_failed)) = terminal {
            self.retire_run(&native, runtime_failed);
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
        AppStore, ConfigRevision, CreateProject, CreateWorkItem, IdGenerator, SecretValue,
    };
    use yakshed_domain::{
        ArtifactId, ArtifactKind, ArtifactProvenance, CredentialBindingRecord, CredentialSlot,
        ProviderStateRootId, SecretLocator, SecretReference,
    };
    use yakshed_harness::{NativePayload, ProviderRunId, ProviderSessionId, SanitizedDiagnostic};
    use yakshed_secrets::{MemorySecretBackend, MemorySecretFault};
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
        let backend = local_backend();
        let store = Arc::new(ConfigStore::open(paths, backend_capabilities()).unwrap());
        let port = ProductionConfig {
            store,
            local_backend: backend.clone(),
            credential_migration: CredentialMigrationStatus::Ready,
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
        assert_eq!(snapshot.config.secret_backends[0].kind(), "local-os");
        assert_eq!(snapshot.config.connections.len(), 1);
    }

    async fn plaintext_fixture(
        paths: &AppPaths,
    ) -> (Arc<ConfigStore>, SecretBackend, Vec<SecretLocator>) {
        paths.create_data_root().unwrap();
        let legacy = legacy_backend(paths);
        let locators = vec![
            SecretLocator::new("connection/one/api-key").unwrap(),
            SecretLocator::new("connection/two/api-key").unwrap(),
        ];
        let connection = Connection {
            id: "0193f26e-7a72-7000-8000-00000000d101".parse().unwrap(),
            name: "Legacy".to_owned(),
            harness: "codex".to_owned(),
            model_provider: "openai".to_owned(),
            provider_state: ProviderStateRootId::new("legacy-codex").unwrap(),
            credentials: locators
                .iter()
                .enumerate()
                .map(|(index, locator)| CredentialBindingRecord {
                    slot: CredentialSlot::new(format!("provider.key-{index}")).unwrap(),
                    binding: CredentialBinding::Secret {
                        reference: SecretReference {
                            backend_id: legacy.id.clone(),
                            locator: locator.clone(),
                        },
                    },
                })
                .collect(),
        };
        let store = Arc::new(ConfigStore::open(paths.clone(), backend_capabilities()).unwrap());
        store
            .update(
                ConfigRevision::INITIAL,
                ConfigChange::PutConnectionWithSecretBackends {
                    connection,
                    secret_backends: vec![legacy.clone()],
                },
            )
            .await
            .unwrap();
        let source = LocalFileBackend::from_config(&legacy).unwrap();
        for (index, locator) in locators.iter().enumerate() {
            source
                .put(
                    locator,
                    SecretValue::new(format!("migration-canary-{index}")).as_secret(),
                    PutSecretOptions::NO_OVERWRITE,
                )
                .await
                .unwrap();
        }
        (store, legacy, locators)
    }

    fn migration_context() -> SecretAccessContext {
        SecretAccessContext {
            connection_id: "0193f26e-7a72-7000-8000-00000000d101".parse().unwrap(),
            slot: CredentialSlot::new("provider.key-0").unwrap(),
            purpose: SecretAccessPurpose::ValidateCredential,
            request_id: OperationId::new("migration-test").unwrap(),
        }
    }

    async fn checkpoint_migration(
        store: &ConfigStore,
        legacy: &SecretBackend,
        target: &SecretBackend,
    ) {
        let snapshot = store.snapshot();
        let snapshot = store
            .update(
                snapshot.revision,
                ConfigChange::BeginCredentialMigration(CredentialMigrationRecord {
                    source: legacy.clone(),
                    target: target.clone(),
                    phase: CredentialMigrationPhase::Copying,
                    receipts: Vec::new(),
                }),
            )
            .await
            .unwrap();
        store
            .update(
                snapshot.revision,
                ConfigChange::CheckpointCredentialMigration,
            )
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn plaintext_migration_rewrites_once_verifies_and_removes_source() {
        let temp = tempfile::tempdir().unwrap();
        let paths = AppPaths::for_test(temp.path());
        let (store, legacy, locators) = plaintext_fixture(&paths).await;
        let target_config = local_backend();
        let target = MemorySecretBackend::new(target_config.id.clone());

        assert_eq!(
            migrate_interim_secrets(&paths, &store, &legacy, &target_config, &target).await,
            CredentialMigrationStatus::Ready
        );

        let snapshot = store.snapshot();
        let completed_revision = snapshot.revision;
        assert_eq!(snapshot.config.secret_backends, vec![target_config.clone()]);
        assert!(!paths.data_root.join("secrets.json").exists());
        for (index, locator) in locators.iter().enumerate() {
            assert!(
                target
                    .resolve(locator, &migration_context())
                    .await
                    .unwrap()
                    .expose(|value| value == format!("migration-canary-{index}"))
            );
        }
        let rendered = format!("{:?}", snapshot.config);
        assert!(!rendered.contains("migration-canary"));
        assert_eq!(
            migrate_interim_secrets(&paths, &store, &legacy, &target_config, &target).await,
            CredentialMigrationStatus::Ready
        );
        assert_eq!(store.snapshot().revision, completed_revision);
    }

    #[tokio::test]
    async fn plaintext_migration_resumes_after_a_partial_copy() {
        let temp = tempfile::tempdir().unwrap();
        let paths = AppPaths::for_test(temp.path());
        let (store, legacy, locators) = plaintext_fixture(&paths).await;
        let target_config = local_backend();
        let target = MemorySecretBackend::new(target_config.id.clone());
        target
            .put(
                &locators[0],
                SecretValue::new("migration-canary-0").as_secret(),
                PutSecretOptions::NO_OVERWRITE,
            )
            .await
            .unwrap();

        let snapshot = store.snapshot();
        store
            .update(
                snapshot.revision,
                ConfigChange::BeginCredentialMigration(CredentialMigrationRecord {
                    source: legacy.clone(),
                    target: target_config.clone(),
                    phase: CredentialMigrationPhase::Copying,
                    receipts: Vec::new(),
                }),
            )
            .await
            .unwrap();
        let snapshot = store.snapshot();
        store
            .update(
                snapshot.revision,
                ConfigChange::RecordCredentialCopy {
                    locator: locators[0].clone(),
                    state: CredentialCopyState::Copying,
                },
            )
            .await
            .unwrap();
        let persisted_receipt = fs::read_to_string(paths.config_root.join("config.toml")).unwrap();
        assert!(persisted_receipt.contains(locators[0].as_str()));
        assert!(!persisted_receipt.contains("migration-canary"));
        drop(store);
        let store = Arc::new(ConfigStore::open(paths.clone(), backend_capabilities()).unwrap());

        assert_eq!(
            migrate_interim_secrets(&paths, &store, &legacy, &target_config, &target).await,
            CredentialMigrationStatus::Ready
        );
        assert!(!paths.data_root.join("secrets.json").exists());
        assert!(
            target
                .resolve(&locators[1], &migration_context())
                .await
                .is_ok()
        );
    }

    #[tokio::test]
    async fn locked_keychain_defers_migration_without_losing_plaintext() {
        let temp = tempfile::tempdir().unwrap();
        let paths = AppPaths::for_test(temp.path());
        let (store, legacy, locators) = plaintext_fixture(&paths).await;
        let target_config = local_backend();
        let target = MemorySecretBackend::new(target_config.id.clone());
        target.plan_faults([MemorySecretFault::Locked]);

        assert_eq!(
            migrate_interim_secrets(&paths, &store, &legacy, &target_config, &target).await,
            CredentialMigrationStatus::Pending(CredentialMigrationPendingReason::Locked)
        );
        assert!(paths.data_root.join("secrets.json").exists());
        let snapshot = store.snapshot();
        assert_eq!(
            snapshot.config.credential_migration.unwrap().phase,
            CredentialMigrationPhase::Copying
        );
        assert!(
            LocalFileBackend::from_config(&legacy)
                .unwrap()
                .resolve(&locators[0], &migration_context())
                .await
                .unwrap()
                .expose(|value| value == "migration-canary-0")
        );
    }

    #[tokio::test]
    async fn zeroed_source_after_unlink_failure_is_cleaned_on_relaunch() {
        use std::os::unix::fs::PermissionsExt;

        let temp = tempfile::tempdir().unwrap();
        let paths = AppPaths::for_test(temp.path());
        let (store, legacy, locators) = plaintext_fixture(&paths).await;
        let target_config = local_backend();
        let target = MemorySecretBackend::new(target_config.id.clone());
        for (index, locator) in locators.iter().enumerate() {
            target
                .put(
                    locator,
                    SecretValue::new(format!("migration-canary-{index}")).as_secret(),
                    PutSecretOptions::NO_OVERWRITE,
                )
                .await
                .unwrap();
        }
        checkpoint_migration(&store, &legacy, &target_config).await;
        fs::set_permissions(&paths.data_root, fs::Permissions::from_mode(0o500)).unwrap();

        assert_eq!(
            migrate_interim_secrets(&paths, &store, &legacy, &target_config, &target).await,
            CredentialMigrationStatus::Pending(CredentialMigrationPendingReason::CleanupRequired)
        );
        fs::set_permissions(&paths.data_root, fs::Permissions::from_mode(0o700)).unwrap();
        let plaintext_path = paths.data_root.join("secrets.json");
        assert!(plaintext_path.exists());
        assert!(
            fs::read(&plaintext_path)
                .unwrap()
                .iter()
                .all(|byte| *byte == 0)
        );
        assert_eq!(
            migrate_interim_secrets(&paths, &store, &legacy, &target_config, &target).await,
            CredentialMigrationStatus::Ready
        );
        assert!(!paths.data_root.join("secrets.json").exists());
    }

    #[tokio::test]
    async fn phase_b_cleanup_preserves_a_newer_keychain_value() {
        let temp = tempfile::tempdir().unwrap();
        let paths = AppPaths::for_test(temp.path());
        let (store, legacy, locators) = plaintext_fixture(&paths).await;
        let target_config = local_backend();
        let target = MemorySecretBackend::new(target_config.id.clone());
        target
            .put(
                &locators[0],
                SecretValue::new("newer-keychain-canary").as_secret(),
                PutSecretOptions::NO_OVERWRITE,
            )
            .await
            .unwrap();
        checkpoint_migration(&store, &legacy, &target_config).await;

        assert_eq!(
            migrate_interim_secrets(&paths, &store, &legacy, &target_config, &target).await,
            CredentialMigrationStatus::Ready
        );
        assert!(
            target
                .resolve(&locators[0], &migration_context())
                .await
                .unwrap()
                .expose(|value| value == "newer-keychain-canary")
        );
    }

    #[tokio::test]
    async fn configured_legacy_backend_migrates_entries_without_bindings() {
        let temp = tempfile::tempdir().unwrap();
        let paths = AppPaths::for_test(temp.path());
        let (store, legacy, locators) = plaintext_fixture(&paths).await;
        let snapshot = store.snapshot();
        store
            .update(
                snapshot.revision,
                ConfigChange::RemoveConnection(snapshot.config.connections[0].id),
            )
            .await
            .unwrap();
        let target_config = local_backend();
        let target = MemorySecretBackend::new(target_config.id.clone());

        assert_eq!(
            migrate_interim_secrets(&paths, &store, &legacy, &target_config, &target).await,
            CredentialMigrationStatus::Ready
        );
        assert!(
            target
                .resolve(&locators[1], &migration_context())
                .await
                .is_ok()
        );
        assert!(!paths.data_root.join("secrets.json").exists());
    }

    #[tokio::test]
    async fn configured_target_collision_preserves_both_values() {
        let temp = tempfile::tempdir().unwrap();
        let paths = AppPaths::for_test(temp.path());
        let (store, legacy, locators) = plaintext_fixture(&paths).await;
        let target_config = local_backend();
        let snapshot = store.snapshot();
        store
            .update(
                snapshot.revision,
                ConfigChange::PutSecretBackend(target_config.clone()),
            )
            .await
            .unwrap();
        let target = MemorySecretBackend::new(target_config.id.clone());
        target
            .put(
                &locators[0],
                SecretValue::new("separately-owned-canary").as_secret(),
                PutSecretOptions::NO_OVERWRITE,
            )
            .await
            .unwrap();

        assert_eq!(
            migrate_interim_secrets(&paths, &store, &legacy, &target_config, &target).await,
            CredentialMigrationStatus::Pending(CredentialMigrationPendingReason::Collision)
        );
        assert!(paths.data_root.join("secrets.json").exists());
        assert_eq!(store.snapshot().config.secret_backends.len(), 2);
        assert!(
            target
                .resolve(&locators[0], &migration_context())
                .await
                .unwrap()
                .expose(|value| value == "separately-owned-canary")
        );
        assert!(
            LocalFileBackend::from_config(&legacy)
                .unwrap()
                .resolve(&locators[0], &migration_context())
                .await
                .unwrap()
                .expose(|value| value == "migration-canary-0")
        );
    }

    #[tokio::test]
    async fn configured_dev_backend_owning_plaintext_path_blocks_cleanup() {
        let temp = tempfile::tempdir().unwrap();
        let paths = AppPaths::for_test(temp.path());
        paths.create_data_root().unwrap();
        let target_config = local_backend();
        let dev_backend = SecretBackend {
            id: SecretBackendId::new("dev-local-file").unwrap(),
            settings: SecretBackendSettings::LocalFile {
                path: format!("{}/./secrets.json", paths.data_root.display()),
            },
        };
        let dev_store = LocalFileBackend::from_config(&dev_backend).unwrap();
        let locator = SecretLocator::new("dev/credential").unwrap();
        dev_store
            .put(
                &locator,
                SecretValue::new("dev-owner-canary").as_secret(),
                PutSecretOptions::NO_OVERWRITE,
            )
            .await
            .unwrap();
        let store = test_config(&paths);
        let snapshot = store
            .update(
                ConfigRevision::INITIAL,
                ConfigChange::PutSecretBackend(target_config.clone()),
            )
            .await
            .unwrap();
        store
            .update(
                snapshot.revision,
                ConfigChange::PutSecretBackend(dev_backend.clone()),
            )
            .await
            .unwrap();
        let target = MemorySecretBackend::new(target_config.id.clone());

        assert_eq!(
            migrate_interim_secrets(
                &paths,
                &store,
                &legacy_backend(&paths),
                &target_config,
                &target,
            )
            .await,
            CredentialMigrationStatus::Pending(CredentialMigrationPendingReason::SourceInUse)
        );
        assert!(store.snapshot().config.credential_migration.is_none());
        assert!(paths.data_root.join("secrets.json").exists());
        assert!(
            dev_store
                .resolve(&locator, &migration_context())
                .await
                .unwrap()
                .expose(|value| value == "dev-owner-canary")
        );
    }

    #[tokio::test]
    async fn bound_locator_missing_from_plaintext_defers_before_rewrite() {
        let temp = tempfile::tempdir().unwrap();
        let paths = AppPaths::for_test(temp.path());
        let (store, legacy, locators) = plaintext_fixture(&paths).await;
        LocalFileBackend::from_config(&legacy)
            .unwrap()
            .delete(&locators[1])
            .await
            .unwrap();
        let target_config = local_backend();
        let target = MemorySecretBackend::new(target_config.id.clone());

        assert_eq!(
            migrate_interim_secrets(&paths, &store, &legacy, &target_config, &target).await,
            CredentialMigrationStatus::Pending(CredentialMigrationPendingReason::MissingSource)
        );
        let snapshot = store.snapshot();
        assert_eq!(
            snapshot.config.credential_migration.unwrap().phase,
            CredentialMigrationPhase::Copying
        );
        assert!(snapshot.config.secret_backends.contains(&legacy));
        assert!(
            snapshot.config.connections[0]
                .credentials
                .iter()
                .all(|binding| matches!(
                    &binding.binding,
                    CredentialBinding::Secret { reference } if reference.backend_id == legacy.id
                ))
        );
        assert!(paths.data_root.join("secrets.json").exists());
    }

    #[test]
    fn every_production_configured_backend_kind_builds_a_handle() {
        let temp = tempfile::tempdir().unwrap();
        let local_file = legacy_backend(&AppPaths::for_test(temp.path()));
        let local_os = local_backend();
        let handles = build_backend_handles(vec![local_file.clone(), local_os.clone()]).unwrap();

        assert_eq!(handles.len(), 2);
        for backend in [local_file, local_os] {
            let (_, handle) = handles
                .iter()
                .find(|(id, _)| id == &backend.id)
                .expect("available production backend must have a handle");
            assert_eq!(handle.resolver.descriptor().kind, backend.kind());
            assert!(handle.administrator.is_some());
        }
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
    async fn crash_evicts_idle_adapter_for_the_next_start() {
        let temp = tempfile::tempdir().unwrap();
        let paths = AppPaths::for_test(temp.path());
        let harness = CodexHarness::new(paths.clone(), test_config(&paths));
        let codex = connection(
            "0193f26e-7a72-7000-8000-00000000d004",
            "codex",
            "codex-crash",
        );
        let adapter = harness.adapter(&codex).unwrap();
        let native = ProviderRunHandle::new(
            RuntimeHandle::new("runtime-crash").unwrap(),
            ProviderSessionId::new("session-crash").unwrap(),
            ProviderRunId::new("run-crash").unwrap(),
        );
        harness
            .register_run(codex.id, SystemIdGenerator.next_run_id(), native.clone())
            .unwrap();
        harness
            .event_sender
            .send(HarnessEvent::RunTerminal {
                run: native,
                state: HarnessRunTerminal::Crashed {
                    diagnostic: SanitizedDiagnostic::sanitized("runtime exited"),
                },
                native: NativePayload::sanitized("{}"),
            })
            .await
            .unwrap();

        assert!(matches!(
            harness.next_event().await.unwrap(),
            Some(RunHarnessEvent::RunTerminal {
                state: RunTerminal::Crashed { .. },
                ..
            })
        ));
        let replacement = harness.adapter(&codex).unwrap();
        assert!(!Arc::ptr_eq(&adapter, &replacement));
    }

    #[tokio::test]
    async fn pending_capacity_backpressures_without_losing_terminal() {
        let temp = tempfile::tempdir().unwrap();
        let paths = AppPaths::for_test(temp.path());
        let harness = Arc::new(CodexHarness::new(paths.clone(), test_config(&paths)));
        let codex = connection(
            "0193f26e-7a72-7000-8000-00000000d005",
            "codex",
            "codex-overflow",
        );
        let _ = harness.adapter(&codex).unwrap();
        let native = ProviderRunHandle::new(
            RuntimeHandle::new("runtime-overflow").unwrap(),
            ProviderSessionId::new("session-overflow").unwrap(),
            ProviderRunId::new("run-overflow").unwrap(),
        );
        let producer = tokio::spawn({
            let sender = harness.event_sender.clone();
            let native = native.clone();
            async move {
                for _ in 0..PENDING_EVENT_CAPACITY {
                    sender
                        .send(HarnessEvent::MessageDelta {
                            run: native.clone(),
                            chunk: "x".to_owned(),
                            native: NativePayload::sanitized("{}"),
                        })
                        .await
                        .unwrap();
                }
                sender
                    .send(HarnessEvent::RunTerminal {
                        run: native,
                        state: HarnessRunTerminal::Completed,
                        native: NativePayload::sanitized("{}"),
                    })
                    .await
                    .unwrap();
            }
        });
        let first = tokio::spawn({
            let harness = harness.clone();
            async move { harness.next_event().await }
        });
        loop {
            let count = harness
                .run_state
                .lock()
                .unwrap()
                .pending_events
                .values()
                .map(VecDeque::len)
                .sum::<usize>();
            if count == PENDING_EVENT_CAPACITY {
                break;
            }
            tokio::task::yield_now().await;
        }
        producer.await.unwrap();
        let run = harness
            .register_run(codex.id, SystemIdGenerator.next_run_id(), native)
            .unwrap();
        assert!(matches!(
            first.await.unwrap().unwrap(),
            Some(RunHarnessEvent::MessageDelta { .. })
        ));
        for _ in 1..PENDING_EVENT_CAPACITY {
            assert!(matches!(
                harness.next_event().await.unwrap(),
                Some(RunHarnessEvent::MessageDelta { .. })
            ));
        }
        assert_eq!(
            harness.next_event().await.unwrap(),
            Some(RunHarnessEvent::RunTerminal {
                run,
                state: RunTerminal::Completed,
            })
        );
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
