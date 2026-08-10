//! JSONL-only external acceptance host. This binary is a workspace test tool, not a shipping API.

use std::{
    collections::{HashMap, HashSet},
    ffi::OsString,
    io::{self, BufRead, Write},
    path::{Path, PathBuf},
    process::{ExitCode, Stdio},
    str::FromStr,
    sync::Arc,
    time::Duration,
};

use provider_mock::MockHarness;
use secrecy::SecretString;
use serde::Deserialize;
use serde_json::{Value, json};
use tokio::{
    io::{AsyncRead, AsyncReadExt},
    process::{Child, Command},
};
use yakshed_application::{
    AppStore, ConfigChange, ConfigRevision, CreateProject, CreateWorkItem, ListWorkItems,
    SystemClock, SystemIdGenerator,
};
use yakshed_domain::{
    Connection, ConnectionId, CredentialBinding, CredentialBindingRecord, CredentialDelivery,
    CredentialSlot, OperationId, ProjectId, ProviderStateRootId, SecretBackend, SecretBackendId,
    SecretBackendSettings, SecretLocator, SecretReference, WorkItemId, WorkItemSnapshot,
    WorkItemStatus,
};
use yakshed_harness::HarnessCapabilities;
use yakshed_secrets::{
    BrokerCancellation, CredentialBroker, CredentialResolution, CredentialStatus,
    DeleteSecretOutcome, MemorySecretBackend, NoopSecretAuditSink, PutSecretOptions,
    SecretAccessContext, SecretAccessPurpose, SecretAdministrator, SecretBackendHandle,
    SecretError, SecretResolver, backend_capabilities, shape_process_environment,
};
use yakshed_store::{
    AppPaths, ArtifactStore, CacheError, CacheStore, ConfigError, ConfigStore, SqliteStore,
};

const PROTOCOL_VERSION: u64 = 1;
const PROBE_OUTPUT_LIMIT: u64 = 64 * 1024;
/// Contract probes are local test helpers; five seconds covers interpreter startup with margin.
const PROBE_TIMEOUT: Duration = Duration::from_secs(5);
const ARTIFACT_MAX_BYTES: u64 = 64 * 1024 * 1024;

#[tokio::main(flavor = "current_thread")]
async fn main() -> ExitCode {
    match Args::parse() {
        Ok(Args::Version) => {
            println!("{} {}", env!("CARGO_PKG_NAME"), env!("CARGO_PKG_VERSION"));
            ExitCode::SUCCESS
        }
        Ok(Args::Run(options)) => match Host::open(options).await {
            Ok(mut host) => match host.serve().await {
                Ok(()) => ExitCode::SUCCESS,
                Err(error) => {
                    eprintln!("contract host failed: {}", error.safe_message());
                    ExitCode::FAILURE
                }
            },
            Err(error) => {
                eprintln!("contract host startup failed: {}", error.safe_message());
                ExitCode::FAILURE
            }
        },
        Err(message) => {
            eprintln!("contract host argument error: {message}");
            ExitCode::FAILURE
        }
    }
}

enum Args {
    Version,
    Run(LaunchOptions),
}

struct LaunchOptions {
    root: PathBuf,
    _temporary_root: Option<tempfile::TempDir>,
}

impl Args {
    fn parse() -> Result<Self, String> {
        let values = std::env::args().skip(1).collect::<Vec<_>>();
        if values == ["--version"] {
            return Ok(Self::Version);
        }
        let mut root = None;
        let mut allow_temp = false;
        let mut protocol = None;
        let mut secret_backend = "memory".to_owned();
        let mut harness = "mock".to_owned();
        let mut index = 0;
        while index < values.len() {
            match values[index].as_str() {
                "--allow-auto-temp-root" => allow_temp = true,
                "--root" | "--protocol-version" | "--secret-backend" | "--harness" => {
                    let flag = &values[index];
                    index += 1;
                    let value = values
                        .get(index)
                        .ok_or_else(|| format!("{flag} requires a value"))?;
                    match flag.as_str() {
                        "--root" => root = Some(PathBuf::from(value)),
                        "--protocol-version" => protocol = Some(value.clone()),
                        "--secret-backend" => secret_backend = value.clone(),
                        "--harness" => harness = value.clone(),
                        _ => unreachable!(),
                    }
                }
                unknown => return Err(format!("unknown argument: {unknown}")),
            }
            index += 1;
        }
        if protocol.as_deref() != Some("1") {
            return Err("--protocol-version must be 1".to_owned());
        }
        if secret_backend != "memory" {
            return Err("only --secret-backend memory is supported in v1".to_owned());
        }
        if harness != "mock" {
            return Err("only --harness mock is supported in v1".to_owned());
        }
        let (root, temporary_root) = match root {
            Some(root) => (root, None),
            None if allow_temp => {
                let temporary = tempfile::tempdir().map_err(|error| error.to_string())?;
                (temporary.path().to_owned(), Some(temporary))
            }
            None => return Err("--root is required unless --allow-auto-temp-root is set".into()),
        };
        if !root.is_absolute() {
            return Err("--root must be absolute".to_owned());
        }
        Ok(Self::Run(LaunchOptions {
            root,
            _temporary_root: temporary_root,
        }))
    }
}

struct Host {
    root: PathBuf,
    paths: AppPaths,
    config: ConfigStore,
    data: Option<SqliteStore>,
    cache: CacheStore,
    artifacts: Option<ArtifactStore>,
    memory: Arc<MemorySecretBackend>,
    broker: CredentialBroker,
    _harness: MockHarness,
    negotiated: bool,
    shutting_down: bool,
    _temporary_root: Option<tempfile::TempDir>,
}

impl Host {
    async fn open(options: LaunchOptions) -> Result<Self, HostError> {
        std::fs::create_dir_all(&options.root).map_err(HostError::persistence)?;
        let root = std::fs::canonicalize(&options.root).map_err(HostError::persistence)?;
        let paths = AppPaths::for_test(&root);
        paths
            .create_runtime_root()
            .map_err(HostError::persistence)?;
        clear_directory(&paths.runtime_root).map_err(HostError::persistence)?;
        let config = ConfigStore::open(paths.clone(), backend_capabilities())?;
        let cache = CacheStore::open(&paths)?;
        let data = SqliteStore::open(
            paths.clone(),
            Arc::new(SystemClock),
            Arc::new(SystemIdGenerator),
        )
        .await?;
        let artifacts = ArtifactStore::new(&paths, ARTIFACT_MAX_BYTES)?;
        let backend_id = SecretBackendId::new("memory")?;
        let memory = Arc::new(MemorySecretBackend::new(backend_id.clone()));
        let resolver: Arc<dyn SecretResolver> = memory.clone();
        let administrator: Arc<dyn SecretAdministrator> = memory.clone();
        let broker = CredentialBroker::new(
            [(
                backend_id,
                SecretBackendHandle {
                    resolver,
                    administrator: Some(administrator),
                },
            )],
            &[],
            Arc::new(NoopSecretAuditSink),
            Duration::from_secs(5),
        )?;
        Ok(Self {
            root,
            paths,
            config,
            data: Some(data),
            cache,
            artifacts: Some(artifacts),
            memory,
            broker,
            _harness: MockHarness::new(HarnessCapabilities::default(), Vec::new(), None),
            negotiated: false,
            shutting_down: false,
            _temporary_root: options._temporary_root,
        })
    }

    async fn serve(&mut self) -> Result<(), HostError> {
        let stdin = io::stdin();
        let mut stdout = io::BufWriter::new(io::stdout().lock());
        for line in stdin.lock().lines() {
            let line = line.map_err(HostError::protocol)?;
            let response = match serde_json::from_str::<Request>(&line) {
                Ok(request) if request.id > 0 => {
                    let id = request.id;
                    match self.dispatch(request).await {
                        Ok(result) => success(id, result),
                        Err(error) => failure(id, error),
                    }
                }
                Ok(request) => failure(
                    request.id,
                    HostError::invalid("request id must be positive"),
                ),
                Err(error) => {
                    let id = recover_id(&line).ok_or_else(|| HostError::protocol(&error))?;
                    failure(id, HostError::invalid(error.to_string()))
                }
            };
            serde_json::to_writer(&mut stdout, &response).map_err(HostError::protocol)?;
            stdout.write_all(b"\n").map_err(HostError::protocol)?;
            stdout.flush().map_err(HostError::protocol)?;
            if self.shutting_down {
                break;
            }
        }
        self.shutdown().await
    }

    async fn dispatch(&mut self, request: Request) -> Result<Value, HostError> {
        if request.op != "hello" && !self.negotiated {
            return Err(HostError::new(
                "protocol_error",
                "hello must negotiate protocol v1 first",
            ));
        }
        match request.op.as_str() {
            "hello" => self.hello(request.params),
            "paths.read" => Ok(json!({
                "config_root": self.paths.config_root,
                "cache_root": self.paths.cache_root,
                "data_root": self.paths.data_root,
                "runtime_root": self.paths.runtime_root,
            })),
            "connection.put" => self.connection_put(request.params).await,
            "connection.get" => self.connection_get(request.params).await,
            "connection.list" => self.connection_list(),
            "secret.put" => self.secret_put(request.params).await,
            "secret.status" => self.secret_status(request.params).await,
            "secret.delete" => self.secret_delete(request.params).await,
            "work.create" => self.work_create(request.params).await,
            "work.get" => self.work_get(request.params).await,
            "cache.put" => self.cache_put(request.params),
            "cache.exists" => self.cache_exists(request.params),
            "cache.clear" => {
                self.cache.clear()?;
                Ok(json!({"cleared": true}))
            }
            "runtime.credential_probe" => self.credential_probe(request.params).await,
            "config.reset" => self.config_reset().await,
            "data.purge" => self.data_purge().await,
            "state.summary" => self.state_summary().await,
            "shutdown" => {
                self.shutting_down = true;
                Ok(json!({"shutting_down": true}))
            }
            _ => Err(HostError::new(
                "unknown_operation",
                "operation is not part of protocol v1",
            )),
        }
    }

    fn hello(&mut self, params: Value) -> Result<Value, HostError> {
        let params: HelloParams = parse_params(params)?;
        if params.protocol_version != PROTOCOL_VERSION {
            return Err(HostError::new(
                "protocol_error",
                "only protocol version 1 is supported",
            ));
        }
        self.negotiated = true;
        Ok(json!({
            "protocol_version": PROTOCOL_VERSION,
            "host_version": env!("CARGO_PKG_VERSION"),
            "root": self.root,
            "modules": {
                "config": "production",
                "data": "sqlite",
                "secrets": "memory",
                "harness": "mock"
            }
        }))
    }

    async fn connection_put(&mut self, params: Value) -> Result<Value, HostError> {
        let params: PutConnectionParams = parse_params(params)?;
        let ConvertedConnection {
            connection,
            needs_memory,
        } = params.connection.into_domain()?;
        let id = connection.id;
        let secret_backends = needs_memory
            .then(|| SecretBackend {
                id: SecretBackendId::new("memory").expect("constant backend id is valid"),
                settings: SecretBackendSettings::Memory,
            })
            .into_iter()
            .collect();
        let snapshot = self
            .config
            .update(
                ConfigRevision::new(params.expected_config_revision),
                ConfigChange::PutConnectionWithSecretBackends {
                    connection,
                    secret_backends,
                },
            )
            .await?;
        Ok(json!({"config_revision": snapshot.revision.get(), "connection_id": id}))
    }

    async fn connection_get(&self, params: Value) -> Result<Value, HostError> {
        let params: ConnectionIdParams = parse_params(params)?;
        let id = ConnectionId::from_str(&params.connection_id)?;
        let snapshot = self.config.snapshot();
        let connection = snapshot
            .config
            .connections
            .iter()
            .find(|connection| connection.id == id)
            .ok_or_else(|| HostError::not_found("connection not found"))?;
        Ok(json!({
            "config_revision": snapshot.revision.get(),
            "connection": self.connection_json(connection).await?,
        }))
    }

    fn connection_list(&self) -> Result<Value, HostError> {
        let snapshot = self.config.snapshot();
        let connections = snapshot
            .config
            .connections
            .iter()
            .map(|connection| {
                json!({
                    "id": connection.id,
                    "name": connection.name,
                    "harness": connection.harness,
                    "model_provider": connection.model_provider,
                    "provider_state": connection.provider_state,
                })
            })
            .collect::<Vec<_>>();
        Ok(json!({
            "config_revision": snapshot.revision.get(),
            "connections": connections,
        }))
    }

    async fn connection_json(&self, connection: &Connection) -> Result<Value, HostError> {
        let snapshot = self.config.snapshot();
        let mut credentials = Vec::with_capacity(connection.credentials.len());
        for binding in &connection.credentials {
            let status = self
                .binding_status(&snapshot.config.connections, connection, binding)
                .await?;
            let mut value = match &binding.binding {
                CredentialBinding::Delegated { authority } => json!({
                    "slot": binding.slot,
                    "source": "delegated",
                    "authority": authority,
                    "status": status,
                }),
                CredentialBinding::Secret { reference } => json!({
                    "slot": binding.slot,
                    "source": "secret",
                    "backend": reference.backend_id,
                    "locator": reference.locator,
                    "status": status,
                }),
                CredentialBinding::Disabled => json!({
                    "slot": binding.slot,
                    "source": "disabled",
                    "status": status,
                }),
            };
            if let Some(delivery) = &binding.delivery {
                value["delivery"] = delivery_json(delivery);
            }
            credentials.push(value);
        }
        Ok(json!({
            "id": connection.id,
            "name": connection.name,
            "harness": connection.harness,
            "model_provider": connection.model_provider,
            "provider_state": connection.provider_state,
            "credentials": credentials,
        }))
    }

    async fn binding_status(
        &self,
        connections: &[Connection],
        connection: &Connection,
        binding: &CredentialBindingRecord,
    ) -> Result<&'static str, HostError> {
        let context = secret_context(connection.id, binding.slot.clone(), "status")?;
        let status = self
            .broker
            .status(
                connections,
                binding,
                &context,
                &BrokerCancellation::default(),
            )
            .await?;
        Ok(match status {
            CredentialStatus::Present => "present",
            CredentialStatus::Missing => "missing",
            CredentialStatus::Delegated(_) => "delegated",
            CredentialStatus::Disabled => "disabled",
        })
    }

    async fn secret_put(&self, params: Value) -> Result<Value, HostError> {
        let params: PutSecretParams = parse_params(params)?;
        require_memory(&params.backend)?;
        let locator = SecretLocator::new(params.locator)?;
        self.memory
            .put(
                &locator,
                &SecretString::from(params.value),
                PutSecretOptions {
                    overwrite: params.overwrite,
                },
            )
            .await?;
        Ok(json!({"backend": "memory", "locator": locator, "written": true}))
    }

    async fn secret_status(&self, params: Value) -> Result<Value, HostError> {
        let params: SecretRefParams = parse_params(params)?;
        require_memory(&params.backend)?;
        let locator = SecretLocator::new(params.locator)?;
        let status = match self
            .memory
            .resolve(&locator, &dummy_secret_context("status")?)
            .await
        {
            Ok(secret) => {
                drop(secret);
                "present"
            }
            Err(SecretError::NotFound { .. }) => "missing",
            Err(error) => return Err(error.into()),
        };
        Ok(json!({"status": status, "backend": "memory", "locator": locator}))
    }

    async fn secret_delete(&self, params: Value) -> Result<Value, HostError> {
        let params: SecretRefParams = parse_params(params)?;
        require_memory(&params.backend)?;
        let locator = SecretLocator::new(params.locator)?;
        let deleted = self.memory.delete(&locator).await? == DeleteSecretOutcome::Deleted;
        Ok(json!({"backend": "memory", "locator": locator, "deleted": deleted}))
    }

    async fn work_create(&self, params: Value) -> Result<Value, HostError> {
        let params: CreateWorkParams = parse_params(params)?;
        let project_id = ProjectId::from_str(&params.project_id)?;
        let id = WorkItemId::from_str(&params.id)?;
        let store = self.data()?;
        store
            .create_project(CreateProject {
                id: project_id,
                name: "Contract project".to_owned(),
            })
            .await?;
        let item = store
            .create_work_item(CreateWorkItem {
                id,
                project_id,
                title: params.title,
                parent_id: None,
            })
            .await?;
        Ok(work_json(&item))
    }

    async fn work_get(&self, params: Value) -> Result<Value, HostError> {
        let params: WorkIdParams = parse_params(params)?;
        let item = self
            .data()?
            .get_work_item(WorkItemId::from_str(&params.work_item_id)?)
            .await?;
        Ok(work_json(&item))
    }

    fn cache_put(&self, params: Value) -> Result<Value, HostError> {
        let params: CachePutParams = parse_params(params)?;
        self.cache
            .put(&params.namespace, &params.key, &params.value)?;
        Ok(json!({"stored": true}))
    }

    fn cache_exists(&self, params: Value) -> Result<Value, HostError> {
        let params: CacheKeyParams = parse_params(params)?;
        Ok(json!({"exists": self.cache.exists(&params.namespace, &params.key)?}))
    }

    async fn credential_probe(&self, params: Value) -> Result<Value, HostError> {
        let params: ProbeParams = parse_params(params)?;
        validate_probe(&params)?;
        let connection_id = ConnectionId::from_str(&params.connection_id)?;
        let slot = CredentialSlot::new(params.slot.clone())?;
        let snapshot = self.config.snapshot();
        let connection = snapshot
            .config
            .connections
            .iter()
            .find(|connection| connection.id == connection_id)
            .ok_or_else(|| HostError::not_found("connection not found"))?;
        let binding = connection
            .credentials
            .iter()
            .find(|binding| binding.slot == slot)
            .ok_or_else(|| HostError::not_found("credential binding not found"))?;
        let variable = binding
            .delivery
            .as_ref()
            .and_then(CredentialDelivery::variable)
            .ok_or_else(|| HostError::new("unsupported", "binding has no process delivery"))?;
        let context = secret_context(connection_id, slot, "credential-probe")?;
        let resolution = self
            .broker
            .resolve(
                &snapshot.config.connections,
                binding,
                &context,
                &BrokerCancellation::default(),
            )
            .await?;
        let CredentialResolution::Secret(secret) = resolution else {
            return Err(HostError::new(
                "unsupported",
                "delegated credentials cannot use the process probe",
            ));
        };
        let ambient = std::env::vars_os().collect::<HashMap<OsString, OsString>>();
        let environment =
            shape_process_environment(&ambient, variable, &secret).map_err(HostError::invalid)?;
        let output = run_probe(&params, variable, environment).await?;
        drop(secret);
        let matched = output.protocol_version == PROTOCOL_VERSION
            && output.present
            && output.credential_variable == *variable
            && output.sha256.as_deref() == Some(params.expected_sha256.as_str())
            && output.forbidden_present.is_empty();
        Ok(json!({
            "matched": matched,
            "credential_variable": variable,
            "forbidden_present": output.forbidden_present,
            "exit_code": output.exit_code,
        }))
    }

    async fn config_reset(&mut self) -> Result<Value, HostError> {
        let revision = self.config.snapshot().revision;
        let snapshot = self.config.update(revision, ConfigChange::Reset).await?;
        Ok(json!({"config_revision": snapshot.revision.get()}))
    }

    async fn data_purge(&mut self) -> Result<Value, HostError> {
        self.artifacts.take();
        if let Some(data) = self.data.take() {
            data.shutdown().await?;
        }
        if self
            .paths
            .data_root
            .try_exists()
            .map_err(HostError::persistence)?
        {
            std::fs::remove_dir_all(&self.paths.data_root).map_err(HostError::persistence)?;
        }
        self.paths
            .create_data_root()
            .map_err(HostError::persistence)?;
        self.data = Some(
            SqliteStore::open(
                self.paths.clone(),
                Arc::new(SystemClock),
                Arc::new(SystemIdGenerator),
            )
            .await?,
        );
        self.artifacts = Some(ArtifactStore::new(&self.paths, ARTIFACT_MAX_BYTES)?);
        Ok(json!({"purged": true}))
    }

    async fn state_summary(&self) -> Result<Value, HostError> {
        let snapshot = self.config.snapshot();
        let mut work_items = 0_u64;
        let mut after_project = None;
        loop {
            let projects = self.data()?.list_projects(after_project, 100).await?;
            for project in &projects.items {
                let mut after = None;
                loop {
                    let page = self
                        .data()?
                        .list_work_items(ListWorkItems {
                            project_id: project.id,
                            after,
                            limit: 100,
                            include_archived: true,
                        })
                        .await?;
                    work_items += u64::try_from(page.items.len())
                        .map_err(|_| HostError::internal("work item count overflow"))?;
                    after = page.next_after;
                    if after.is_none() {
                        break;
                    }
                }
            }
            after_project = projects.next_after;
            if after_project.is_none() {
                break;
            }
        }
        let mut seen = HashSet::new();
        let mut secret_statuses = Vec::new();
        for connection in &snapshot.config.connections {
            for binding in &connection.credentials {
                if let CredentialBinding::Secret { reference } = &binding.binding
                    && seen.insert(reference.clone())
                {
                    let status = match self
                        .memory
                        .resolve(&reference.locator, &dummy_secret_context("summary")?)
                        .await
                    {
                        Ok(secret) => {
                            drop(secret);
                            "present"
                        }
                        Err(SecretError::NotFound { .. }) => "missing",
                        Err(error) => return Err(error.into()),
                    };
                    secret_statuses.push(json!({
                        "backend": reference.backend_id,
                        "locator": reference.locator,
                        "status": status,
                    }));
                }
            }
        }
        secret_statuses.sort_by_key(Value::to_string);
        Ok(json!({
            "config_revision": snapshot.revision.get(),
            "connections": snapshot.config.connections.len(),
            "work_items": work_items,
            "cache_entries": self.cache.count()?,
            "artifacts": 0,
            "secret_statuses": secret_statuses,
        }))
    }

    fn data(&self) -> Result<&SqliteStore, HostError> {
        self.data
            .as_ref()
            .ok_or_else(|| HostError::internal("data store is unavailable"))
    }

    async fn shutdown(&mut self) -> Result<(), HostError> {
        self.artifacts.take();
        if let Some(data) = self.data.take() {
            data.shutdown().await?;
        }
        clear_directory(&self.paths.runtime_root).map_err(HostError::persistence)
    }
}

async fn run_probe(
    params: &ProbeParams,
    variable: &str,
    environment: yakshed_secrets::ChildProcessEnvironment,
) -> Result<ProbeOutput, HostError> {
    run_probe_with_timeout(params, variable, environment, PROBE_TIMEOUT).await
}

async fn run_probe_with_timeout(
    params: &ProbeParams,
    variable: &str,
    environment: yakshed_secrets::ChildProcessEnvironment,
    timeout: Duration,
) -> Result<ProbeOutput, HostError> {
    let mut command = Command::new(&params.probe_program);
    command
        .args(&params.probe_args)
        .arg("--credential-var")
        .arg(variable);
    for forbidden in &params.forbidden_variables {
        command.arg("--forbid").arg(forbidden);
    }
    environment.apply_to(command.as_std_mut());
    #[cfg(unix)]
    command.process_group(0);
    command.kill_on_drop(true);
    let mut child = command
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .map_err(|_| HostError::new("backend_unavailable", "credential probe could not start"))?;
    let process_group = child
        .id()
        .ok_or_else(|| HostError::internal("credential probe process ID unavailable"))?;
    let stdout = child
        .stdout
        .take()
        .ok_or_else(|| HostError::internal("credential probe stdout unavailable"))?;
    let stderr = child
        .stderr
        .take()
        .ok_or_else(|| HostError::internal("credential probe stderr unavailable"))?;
    let stdout_reader = tokio::spawn(read_bounded(stdout));
    let stderr_reader = tokio::spawn(read_bounded(stderr));
    let mut child = ProbeChild::new(child, process_group);
    let status = match tokio::time::timeout(timeout, child.wait()).await {
        Ok(Ok(status)) => {
            child.disarm();
            status
        }
        Ok(Err(_)) => {
            child.kill_and_reap().await;
            let _ = stdout_reader.await;
            let _ = stderr_reader.await;
            return Err(HostError::new(
                "backend_unavailable",
                "credential probe failed",
            ));
        }
        Err(_) => {
            child.kill_and_reap().await;
            let _ = stdout_reader.await;
            let _ = stderr_reader.await;
            return Err(HostError::new("timeout", "credential probe timed out"));
        }
    };
    let stdout = stdout_reader
        .await
        .map_err(|_| HostError::internal("credential probe reader failed"))??;
    let _stderr = stderr_reader
        .await
        .map_err(|_| HostError::internal("credential probe reader failed"))??;
    let mut output: ProbeOutput = serde_json::from_slice(&stdout)
        .map_err(|_| HostError::new("protocol_error", "credential probe returned invalid JSON"))?;
    output.exit_code = status.code().unwrap_or(-1);
    Ok(output)
}

struct ProbeChild {
    child: Option<Child>,
    process_group: u32,
}

impl ProbeChild {
    fn new(child: Child, process_group: u32) -> Self {
        Self {
            child: Some(child),
            process_group,
        }
    }

    async fn wait(&mut self) -> io::Result<std::process::ExitStatus> {
        self.child
            .as_mut()
            .expect("probe child is armed")
            .wait()
            .await
    }

    fn disarm(&mut self) {
        self.child.take();
    }

    async fn kill_and_reap(&mut self) {
        kill_process_group(self.process_group);
        if let Some(mut child) = self.child.take() {
            let _ = child.start_kill();
            let _ = child.wait().await;
        }
    }
}

impl Drop for ProbeChild {
    fn drop(&mut self) {
        let Some(mut child) = self.child.take() else {
            return;
        };
        kill_process_group(self.process_group);
        let _ = child.start_kill();
        if let Ok(runtime) = tokio::runtime::Handle::try_current() {
            runtime.spawn(async move {
                let _ = child.wait().await;
            });
        }
    }
}

#[cfg(unix)]
fn kill_process_group(process_group: u32) {
    if let Ok(process_group) = i32::try_from(process_group) {
        // SAFETY: negative PID targets the child-created process group; SIGKILL needs no handler.
        let _ = unsafe { libc::kill(-process_group, libc::SIGKILL) };
    }
}

#[cfg(not(unix))]
fn kill_process_group(_process_group: u32) {}

async fn read_bounded(reader: impl AsyncRead + Unpin) -> Result<Vec<u8>, HostError> {
    let mut bytes = Vec::new();
    reader
        .take(PROBE_OUTPUT_LIMIT + 1)
        .read_to_end(&mut bytes)
        .await
        .map_err(|_| HostError::new("protocol_error", "credential probe output read failed"))?;
    if bytes.len() as u64 > PROBE_OUTPUT_LIMIT {
        return Err(HostError::new(
            "protocol_error",
            "credential probe output exceeded the limit",
        ));
    }
    Ok(bytes)
}

fn validate_probe(params: &ProbeParams) -> Result<(), HostError> {
    if !Path::new(&params.probe_program).is_absolute() {
        return Err(HostError::invalid("probe_program must be absolute"));
    }
    if params.expected_sha256.len() != 64
        || !params
            .expected_sha256
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        return Err(HostError::invalid("expected_sha256 must be lowercase hex"));
    }
    for variable in &params.forbidden_variables {
        validate_environment_variable(variable)?;
    }
    Ok(())
}

fn validate_environment_variable(value: &str) -> Result<(), HostError> {
    let mut bytes = value.bytes();
    if !bytes
        .next()
        .is_some_and(|byte| byte == b'_' || byte.is_ascii_alphabetic())
        || !bytes.all(|byte| byte == b'_' || byte.is_ascii_alphanumeric())
    {
        return Err(HostError::invalid("invalid process environment variable"));
    }
    Ok(())
}

fn clear_directory(path: &Path) -> io::Result<()> {
    if path.try_exists()? {
        for entry in std::fs::read_dir(path)? {
            let entry = entry?;
            let file_type = entry.file_type()?;
            if file_type.is_dir() && !file_type.is_symlink() {
                std::fs::remove_dir_all(entry.path())?;
            } else {
                std::fs::remove_file(entry.path())?;
            }
        }
    } else {
        std::fs::create_dir_all(path)?;
    }
    Ok(())
}

fn work_json(item: &WorkItemSnapshot) -> Value {
    json!({
        "work_item": {
            "id": item.id,
            "project_id": item.project_id,
            "title": item.title,
            "status": match item.status { WorkItemStatus::Ready => "ready", WorkItemStatus::Archived => "archived" },
            "parent_id": item.parent_id,
            "revision": item.revision.get(),
            "created_at_unix_ms": item.created_at.unix_millis(),
            "updated_at_unix_ms": item.updated_at.unix_millis(),
        },
        "data_revision": item.revision.get(),
    })
}

fn delivery_json(delivery: &CredentialDelivery) -> Value {
    match delivery {
        CredentialDelivery::HarnessManaged => json!({"kind": "harness_managed"}),
        CredentialDelivery::ProcessEnvironment { variable } => {
            json!({"kind": "process_environment", "variable": variable})
        }
    }
}

fn secret_context(
    connection_id: ConnectionId,
    slot: CredentialSlot,
    operation: &str,
) -> Result<SecretAccessContext, HostError> {
    Ok(SecretAccessContext {
        connection_id,
        slot,
        purpose: SecretAccessPurpose::ValidateCredential,
        request_id: OperationId::new(operation)?,
    })
}

fn dummy_secret_context(operation: &str) -> Result<SecretAccessContext, HostError> {
    secret_context(
        ConnectionId::from_str("0193f26e-7a72-7d42-bf77-0de14c4cc999")?,
        CredentialSlot::new("contract.direct")?,
        operation,
    )
}

fn require_memory(backend: &str) -> Result<(), HostError> {
    if backend == "memory" {
        Ok(())
    } else {
        Err(HostError::new(
            "unsupported",
            "only the memory secret backend is available",
        ))
    }
}

fn recover_id(line: &str) -> Option<u64> {
    serde_json::from_str::<Value>(line)
        .ok()?
        .get("id")?
        .as_u64()
}

fn parse_params<T: for<'de> Deserialize<'de>>(value: Value) -> Result<T, HostError> {
    serde_json::from_value(value).map_err(|error| HostError::invalid(error.to_string()))
}

fn success(id: u64, result: Value) -> Value {
    json!({"id": id, "ok": true, "result": result})
}

fn failure(id: u64, error: HostError) -> Value {
    json!({
        "id": id,
        "ok": false,
        "error": {
            "code": error.code,
            "message": error.message,
            "details": error.details,
        }
    })
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Request {
    id: u64,
    op: String,
    #[serde(default = "empty_object")]
    params: Value,
}

fn empty_object() -> Value {
    json!({})
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct HelloParams {
    protocol_version: u64,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct PutConnectionParams {
    expected_config_revision: u64,
    connection: ConnectionInput,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ConnectionInput {
    id: String,
    name: String,
    harness: String,
    model_provider: String,
    provider_state: String,
    #[serde(default)]
    credentials: Vec<CredentialInput>,
}

impl ConnectionInput {
    fn into_domain(self) -> Result<ConvertedConnection, HostError> {
        let mut needs_memory = false;
        let mut credentials = Vec::with_capacity(self.credentials.len());
        for credential in self.credentials {
            match credential {
                CredentialInput::Delegated {
                    slot,
                    authority,
                    delivery,
                } => {
                    if !matches!(delivery, DeliveryInput::HarnessManaged) {
                        return Err(HostError::invalid(
                            "delegated credentials require harness_managed delivery",
                        ));
                    }
                    credentials.push(CredentialBindingRecord {
                        slot: CredentialSlot::new(slot)?,
                        binding: CredentialBinding::Delegated { authority },
                        delivery: Some(CredentialDelivery::HarnessManaged),
                    });
                }
                CredentialInput::Secret {
                    slot,
                    backend,
                    locator,
                    delivery,
                } => {
                    require_memory(&backend)?;
                    let slot = CredentialSlot::new(slot)?;
                    let DeliveryInput::ProcessEnvironment { variable } = delivery else {
                        return Err(HostError::invalid(
                            "secret credentials require process_environment delivery",
                        ));
                    };
                    needs_memory = true;
                    credentials.push(CredentialBindingRecord {
                        slot,
                        binding: CredentialBinding::Secret {
                            reference: SecretReference {
                                backend_id: SecretBackendId::new(backend)?,
                                locator: SecretLocator::new(locator)?,
                            },
                        },
                        delivery: Some(CredentialDelivery::process_environment(variable)?),
                    });
                }
                CredentialInput::Disabled { slot } => {
                    credentials.push(CredentialBindingRecord {
                        slot: CredentialSlot::new(slot)?,
                        binding: CredentialBinding::Disabled,
                        delivery: None,
                    });
                }
            }
        }
        let connection = Connection {
            id: ConnectionId::from_str(&self.id)?,
            name: self.name,
            harness: self.harness,
            model_provider: self.model_provider,
            provider_state: ProviderStateRootId::new(self.provider_state)?,
            credentials,
        };
        connection.validate()?;
        Ok(ConvertedConnection {
            connection,
            needs_memory,
        })
    }
}

struct ConvertedConnection {
    connection: Connection,
    needs_memory: bool,
}

#[derive(Deserialize)]
#[serde(tag = "source", rename_all = "snake_case", deny_unknown_fields)]
enum CredentialInput {
    Delegated {
        slot: String,
        authority: String,
        delivery: DeliveryInput,
    },
    Secret {
        slot: String,
        backend: String,
        locator: String,
        delivery: DeliveryInput,
    },
    Disabled {
        slot: String,
    },
}

#[derive(Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
enum DeliveryInput {
    ProcessEnvironment { variable: String },
    HarnessManaged,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ConnectionIdParams {
    connection_id: String,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct PutSecretParams {
    backend: String,
    locator: String,
    value: String,
    #[serde(default)]
    overwrite: bool,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct SecretRefParams {
    backend: String,
    locator: String,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct CreateWorkParams {
    id: String,
    project_id: String,
    title: String,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct WorkIdParams {
    work_item_id: String,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct CachePutParams {
    namespace: String,
    key: String,
    value: Value,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct CacheKeyParams {
    namespace: String,
    key: String,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ProbeParams {
    connection_id: String,
    slot: String,
    probe_program: String,
    #[serde(default)]
    probe_args: Vec<String>,
    expected_sha256: String,
    #[serde(default)]
    forbidden_variables: Vec<String>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ProbeOutput {
    protocol_version: u64,
    credential_variable: String,
    present: bool,
    sha256: Option<String>,
    forbidden_present: Vec<String>,
    #[serde(skip)]
    exit_code: i32,
}

struct HostError {
    code: &'static str,
    message: String,
    details: Value,
}

impl HostError {
    fn new(code: &'static str, message: impl Into<String>) -> Self {
        Self {
            code,
            message: message.into(),
            details: json!({}),
        }
    }

    fn invalid(message: impl Into<String>) -> Self {
        Self::new("invalid_request", message)
    }

    fn not_found(message: impl Into<String>) -> Self {
        Self::new("not_found", message)
    }

    fn internal(message: impl Into<String>) -> Self {
        Self::new("internal_error", message)
    }

    fn persistence(error: impl std::fmt::Display) -> Self {
        let _ = error;
        Self::new("persistence_error", "persistent state operation failed")
    }

    fn protocol(error: impl std::fmt::Display) -> Self {
        let _ = error;
        Self::new("protocol_error", "protocol I/O failed")
    }

    fn safe_message(&self) -> &str {
        &self.message
    }
}

impl From<yakshed_domain::ValidationError> for HostError {
    fn from(error: yakshed_domain::ValidationError) -> Self {
        Self::invalid(error.to_string())
    }
}

impl From<ConfigError> for HostError {
    fn from(error: ConfigError) -> Self {
        match error {
            ConfigError::Conflict { expected, actual } => Self {
                code: "conflict",
                message: "configuration revision conflict".to_owned(),
                details: json!({"expected": expected.get(), "actual": actual.get()}),
            },
            ConfigError::Validation(_) | ConfigError::SecretBackendConfiguration(_) => {
                Self::invalid("connection configuration is invalid")
            }
            _ => Self::persistence(error),
        }
    }
}

impl From<yakshed_application::StoreError> for HostError {
    fn from(error: yakshed_application::StoreError) -> Self {
        use yakshed_application::StoreError;
        match error {
            StoreError::NotFound { .. } => Self::not_found("durable record not found"),
            StoreError::Conflict(_) => Self::new("conflict", "durable state conflict"),
            _ => Self::persistence(error),
        }
    }
}

impl From<SecretError> for HostError {
    fn from(error: SecretError) -> Self {
        let code = match error {
            SecretError::NotFound { .. } => "not_found",
            SecretError::AlreadyExists { .. } => "already_exists",
            SecretError::BackendUnavailable { .. } => "backend_unavailable",
            SecretError::LockedOrDenied { .. } => "locked_or_denied",
            SecretError::AuthenticationRequired { .. } => "authentication_required",
            SecretError::TimedOut { .. } | SecretError::UncertainWrite { .. } => "timeout",
            SecretError::Cancelled { .. } => "cancelled",
            SecretError::UnsupportedOperation { .. } => "unsupported",
            SecretError::InvalidLocator { .. } | SecretError::InvalidBinding { .. } => {
                "invalid_request"
            }
            SecretError::ProtocolViolation { .. } => "protocol_error",
            SecretError::BackendFailure { .. } => "internal_error",
        };
        Self::new(code, "secret operation failed")
    }
}

impl From<CacheError> for HostError {
    fn from(error: CacheError) -> Self {
        match error {
            CacheError::InvalidKey => Self::invalid("invalid cache namespace or key"),
            _ => Self::persistence(error),
        }
    }
}

impl From<yakshed_store::ArtifactError> for HostError {
    fn from(error: yakshed_store::ArtifactError) -> Self {
        Self::persistence(error)
    }
}

impl From<yakshed_store::PathError> for HostError {
    fn from(error: yakshed_store::PathError) -> Self {
        Self::persistence(error)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn connection_input_rejects_secret_valued_fields() {
        let value = json!({
            "id": "0193f26e-7a72-7d42-bf77-0de14c4cc222",
            "name": "Work",
            "harness": "mock",
            "model_provider": "anthropic",
            "provider_state": "work-test",
            "credentials": [{
                "slot": "anthropic.api_key",
                "source": "secret",
                "backend": "memory",
                "locator": "connection/work/key",
                "delivery": {"kind": "process_environment", "variable": "API_KEY"},
                "token": "must-not-fit-the-schema"
            }]
        });
        assert!(serde_json::from_value::<ConnectionInput>(value).is_err());
    }

    #[test]
    fn connection_input_rejects_unknown_delivery_kinds() {
        let value = json!({
            "id": "0193f26e-7a72-7d42-bf77-0de14c4cc222",
            "name": "Work",
            "harness": "mock",
            "model_provider": "anthropic",
            "provider_state": "work-test",
            "credentials": [{
                "slot": "anthropic.api_key",
                "source": "secret",
                "backend": "memory",
                "locator": "connection/work/key",
                "delivery": {"kind": "shell_fragment"}
            }]
        });
        assert!(serde_json::from_value::<ConnectionInput>(value).is_err());
    }

    #[test]
    fn credential_probe_rejects_relative_executables() {
        let params = ProbeParams {
            connection_id: "ignored".to_owned(),
            slot: "ignored".to_owned(),
            probe_program: "python3".to_owned(),
            probe_args: Vec::new(),
            expected_sha256: "0".repeat(64),
            forbidden_variables: Vec::new(),
        };
        assert!(validate_probe(&params).is_err());
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn timed_out_probe_is_killed_and_reaped() {
        use yakshed_secrets::{ResolvedSecret, ResolvedSecretSource};

        let temp = tempfile::tempdir().unwrap();
        let pid_file = temp.path().join("probe.pid");
        let params = ProbeParams {
            connection_id: "ignored".to_owned(),
            slot: "ignored".to_owned(),
            probe_program: "/bin/sh".to_owned(),
            probe_args: vec![
                "-c".to_owned(),
                "echo $$ > \"$1\"; sleep 60".to_owned(),
                "probe".to_owned(),
                pid_file.display().to_string(),
            ],
            expected_sha256: "0".repeat(64),
            forbidden_variables: Vec::new(),
        };
        let secret = ResolvedSecret::new(
            SecretString::from("synthetic".to_owned()),
            ResolvedSecretSource {
                backend: SecretBackendId::new("memory").unwrap(),
            },
            None,
        );
        let environment =
            shape_process_environment(&HashMap::new(), "TEST_SECRET", &secret).unwrap();

        let error = match run_probe_with_timeout(
            &params,
            "TEST_SECRET",
            environment,
            Duration::from_millis(500),
        )
        .await
        {
            Ok(_) => panic!("sleeping probe unexpectedly completed"),
            Err(error) => error,
        };

        assert_eq!(error.code, "timeout");
        let pid = std::fs::read_to_string(pid_file)
            .unwrap()
            .trim()
            .parse::<libc::pid_t>()
            .unwrap();
        assert_eq!(unsafe { libc::kill(pid, 0) }, -1);
        assert_eq!(io::Error::last_os_error().raw_os_error(), Some(libc::ESRCH));
    }
}
