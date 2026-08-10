//! Plaintext local-development secret storage.
//!
//! Backends targeting the same canonical path share a process-global mutex and an exclusive Unix
//! `flock` for each operation. Config removal retains the file; delete it manually to purge.
//! Dropped mutations are suppressed before writing starts. A drop after writing starts has an
//! uncertain outcome and must be reconciled before retrying. Reads wait for a contended file lock;
//! abandoned mutations stop waiting promptly.

#[cfg(unix)]
use std::{
    collections::HashMap,
    fs::{self, File},
    io,
    io::Write,
    path::{Path, PathBuf},
    sync::{
        Arc, Mutex, OnceLock, Weak,
        atomic::{AtomicBool, Ordering},
    },
};

#[cfg(unix)]
use async_trait::async_trait;
#[cfg(unix)]
use atomic_write_file::{AtomicWriteFile, OpenOptions as AtomicOpenOptions};
#[cfg(unix)]
use secrecy::{ExposeSecret, SecretString};
#[cfg(unix)]
use serde::{Deserialize, Serialize};

#[cfg(unix)]
use crate::{
    DeleteSecretOutcome, LOCAL_FILE_BACKEND_KIND, PutSecretOptions, PutSecretOutcome,
    ResolvedSecret, ResolvedSecretSource, SecretAccessContext, SecretAdministrator,
    SecretBackendDescriptor, SecretBackendStatus, SecretError, SecretLocator,
    SecretReferenceSummary, SecretResolver,
};
use crate::{
    SecretBackend, SecretBackendConfigurationError, SecretBackendId, SecretBackendSettings,
    validate_backend_configuration,
};

#[cfg(unix)]
const FORMAT_VERSION: u32 = 1;

#[cfg(unix)]
static LOCAL_FILE_LOCKS: OnceLock<Mutex<HashMap<PathBuf, Weak<Mutex<()>>>>> = OnceLock::new();

pub struct LocalFileBackend {
    #[cfg(unix)]
    state: Arc<LocalFileState>,
}

#[cfg(unix)]
struct LocalFileState {
    id: SecretBackendId,
    configured_path: PathBuf,
    initialized: Mutex<Option<Arc<InitializedLocalFile>>>,
}

#[cfg(unix)]
struct InitializedLocalFile {
    path: PathBuf,
    lock: Arc<Mutex<()>>,
}

#[cfg(unix)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum LocalFileSecurityProblem {
    StoreNotRegular,
    StorePermissions,
    ParentNotDirectory,
    ParentWritable,
}

#[cfg(unix)]
#[derive(Deserialize, Serialize)]
struct LocalFileStore {
    format_version: u32,
    backend_id: SecretBackendId,
    secrets: HashMap<String, String>,
}

impl LocalFileBackend {
    pub fn from_config(config: &SecretBackend) -> Result<Self, SecretBackendConfigurationError> {
        let SecretBackendSettings::LocalFile { path } = &config.settings else {
            return Err(SecretBackendConfigurationError::WrongKind {
                backend: config.id.clone(),
            });
        };
        validate_backend_configuration(config, crate::backend_capabilities())?;
        #[cfg(not(unix))]
        let _ = path;

        #[cfg(unix)]
        {
            Ok(Self {
                state: Arc::new(LocalFileState {
                    id: config.id.clone(),
                    configured_path: PathBuf::from(path),
                    initialized: Mutex::new(None),
                }),
            })
        }
        #[cfg(not(unix))]
        unreachable!("platform validation rejects local-file on non-Unix targets")
    }

    #[cfg(unix)]
    async fn run_blocking<T>(
        &self,
        action: impl FnOnce(&LocalFileState) -> Result<T, SecretError> + Send + 'static,
    ) -> Result<T, SecretError>
    where
        T: Send + 'static,
    {
        let state = Arc::clone(&self.state);
        let backend = state.id.clone();
        tokio::task::spawn_blocking(move || action(&state))
            .await
            .unwrap_or_else(|_| {
                Err(SecretError::BackendFailure {
                    backend,
                    redacted_message: "local secret store worker failed".to_owned(),
                })
            })
    }

    #[cfg(unix)]
    async fn run_mutation<T>(
        &self,
        action: impl FnOnce(&LocalFileState, &AtomicBool) -> Result<T, SecretError> + Send + 'static,
    ) -> Result<T, SecretError>
    where
        T: Send + 'static,
    {
        let state = Arc::clone(&self.state);
        let backend = state.id.clone();
        let abandoned = Arc::new(AtomicBool::new(false));
        let worker_abandoned = Arc::clone(&abandoned);
        let mut guard = AbandonmentGuard {
            abandoned,
            completed: false,
        };
        let result = tokio::task::spawn_blocking(move || {
            if worker_abandoned.load(Ordering::Acquire) {
                return Err(SecretError::Cancelled { backend });
            }
            action(&state, &worker_abandoned)
        })
        .await
        .unwrap_or_else(|_| {
            Err(SecretError::BackendFailure {
                backend: self.state.id.clone(),
                redacted_message: "local secret store worker failed".to_owned(),
            })
        });
        guard.completed = true;
        result
    }
}

#[cfg(unix)]
struct AbandonmentGuard {
    abandoned: Arc<AtomicBool>,
    completed: bool,
}

#[cfg(unix)]
impl Drop for AbandonmentGuard {
    fn drop(&mut self) {
        if !self.completed {
            self.abandoned.store(true, Ordering::Release);
        }
    }
}

#[cfg(unix)]
impl LocalFileState {
    fn summary(&self, locator: &SecretLocator) -> SecretReferenceSummary {
        SecretReferenceSummary {
            backend: self.id.clone(),
            locator: locator.clone(),
        }
    }

    fn initialized(&self) -> Result<Arc<InitializedLocalFile>, SecretError> {
        let mut initialized = self
            .initialized
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if let Some(initialized) = initialized.as_ref() {
            return Ok(Arc::clone(initialized));
        }
        let path = canonical_store_path(&self.configured_path).map_err(|error| {
            map_io_error(&self.id, "failed to initialize local secret store", error)
        })?;
        let value = Arc::new(InitializedLocalFile {
            lock: shared_file_lock(&path),
            path,
        });
        *initialized = Some(Arc::clone(&value));
        Ok(value)
    }

    fn load(&self, path: &Path) -> Result<LocalFileStore, SecretError> {
        read_store(path, &self.id)
    }

    fn save(&self, path: &Path, store: &LocalFileStore) -> Result<(), SecretError> {
        write_store_with_hooks(path, store, || Ok(()), sync_directory)
            .map_err(|error| map_write_error(&self.id, error))
    }

    fn with_store_lock<T>(
        &self,
        abandoned: Option<&AtomicBool>,
        action: impl FnOnce(&Path) -> Result<T, SecretError>,
    ) -> Result<T, SecretError> {
        let initialized = self.initialized()?;
        let _process_guard = initialized
            .lock
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        validate_parent(&initialized.path, &self.id)?;
        let _file_guard = lock_exclusive(&lock_path(&initialized.path), &self.id, abandoned)?;
        validate_parent(&initialized.path, &self.id)?;
        validate_store_if_present(&initialized.path, &self.id)?;
        if abandoned.is_some_and(|flag| flag.load(Ordering::Acquire)) {
            return Err(SecretError::Cancelled {
                backend: self.id.clone(),
            });
        }
        claim_or_validate(&initialized.path, &self.id)?;
        validate_store_if_present(&initialized.path, &self.id)?;
        action(&initialized.path)
    }
}

#[cfg(unix)]
#[async_trait]
impl SecretResolver for LocalFileBackend {
    fn descriptor(&self) -> SecretBackendDescriptor {
        SecretBackendDescriptor {
            id: self.state.id.clone(),
            kind: LOCAL_FILE_BACKEND_KIND.to_owned(),
        }
    }

    async fn probe(&self) -> Result<SecretBackendStatus, SecretError> {
        self.run_blocking(|state| {
            state.with_store_lock(None, |path| {
                state.load(path)?;
                Ok(SecretBackendStatus::Available)
            })
        })
        .await
    }

    async fn resolve(
        &self,
        locator: &SecretLocator,
        _context: &SecretAccessContext,
    ) -> Result<ResolvedSecret, SecretError> {
        let locator = locator.clone();
        self.run_blocking(move |state| {
            state.with_store_lock(None, |path| {
                let store = state.load(path)?;
                let value =
                    store
                        .secrets
                        .get(locator.as_str())
                        .ok_or_else(|| SecretError::NotFound {
                            reference: state.summary(&locator),
                        })?;
                Ok(ResolvedSecret::new(
                    SecretString::from(value.clone()),
                    ResolvedSecretSource {
                        backend: state.id.clone(),
                    },
                    None,
                ))
            })
        })
        .await
    }
}

#[cfg(unix)]
#[async_trait]
impl SecretAdministrator for LocalFileBackend {
    fn backend_id(&self) -> SecretBackendId {
        self.state.id.clone()
    }

    async fn put(
        &self,
        locator: &SecretLocator,
        value: &SecretString,
        options: PutSecretOptions,
    ) -> Result<PutSecretOutcome, SecretError> {
        let locator = locator.clone();
        let value = value.expose_secret().to_owned();
        self.run_mutation(move |state, abandoned| {
            state.with_store_lock(Some(abandoned), |path| {
                let mut store = state.load(path)?;
                let existed = store.secrets.contains_key(locator.as_str());
                if existed && !options.overwrite {
                    return Err(SecretError::AlreadyExists {
                        reference: state.summary(&locator),
                    });
                }
                if abandoned.load(Ordering::Acquire) {
                    return Err(SecretError::Cancelled {
                        backend: state.id.clone(),
                    });
                }
                store.secrets.insert(locator.as_str().to_owned(), value);
                state.save(path, &store)?;
                Ok(if existed {
                    PutSecretOutcome::Replaced
                } else {
                    PutSecretOutcome::Written
                })
            })
        })
        .await
    }

    async fn delete(&self, locator: &SecretLocator) -> Result<DeleteSecretOutcome, SecretError> {
        let locator = locator.clone();
        self.run_mutation(move |state, abandoned| {
            state.with_store_lock(Some(abandoned), |path| {
                let mut store = state.load(path)?;
                if !store.secrets.contains_key(locator.as_str()) {
                    return Ok(DeleteSecretOutcome::NotFound);
                }
                if abandoned.load(Ordering::Acquire) {
                    return Err(SecretError::Cancelled {
                        backend: state.id.clone(),
                    });
                }
                store.secrets.remove(locator.as_str());
                state.save(path, &store)?;
                Ok(DeleteSecretOutcome::Deleted)
            })
        })
        .await
    }
}

#[cfg(unix)]
fn shared_file_lock(path: &Path) -> Arc<Mutex<()>> {
    let registry = LOCAL_FILE_LOCKS.get_or_init(|| Mutex::new(HashMap::new()));
    let mut registry = registry
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    registry.retain(|_, lock| lock.strong_count() > 0);
    if let Some(lock) = registry.get(path).and_then(Weak::upgrade) {
        return lock;
    }
    let lock = Arc::new(Mutex::new(()));
    registry.insert(path.to_owned(), Arc::downgrade(&lock));
    lock
}

#[cfg(unix)]
fn canonical_store_path(path: &Path) -> io::Result<PathBuf> {
    create_private_ancestors(path)?;
    let file_name = path.file_name().ok_or_else(|| {
        io::Error::new(io::ErrorKind::InvalidInput, "store path has no file name")
    })?;
    Ok(
        fs::canonicalize(path.parent().expect("validated store path has a parent"))?
            .join(file_name),
    )
}

#[cfg(unix)]
fn lock_path(path: &Path) -> PathBuf {
    let mut lock = path.as_os_str().to_os_string();
    lock.push(".lock");
    PathBuf::from(lock)
}

#[cfg(unix)]
struct FlockGuard(File);

#[cfg(unix)]
fn lock_exclusive(
    path: &Path,
    backend: &SecretBackendId,
    abandoned: Option<&AtomicBool>,
) -> Result<FlockGuard, SecretError> {
    use std::{os::fd::AsRawFd, os::unix::fs::OpenOptionsExt};

    let file = fs::OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .mode(0o600)
        .custom_flags(libc::O_NOFOLLOW)
        .open(path)
        .map_err(|error| map_io_error(backend, "failed to open local secret store lock", error))?;
    loop {
        if abandoned.is_some_and(|flag| flag.load(Ordering::Acquire)) {
            return Err(SecretError::Cancelled {
                backend: backend.clone(),
            });
        }
        // SAFETY: `file` owns a valid descriptor for the lifetime of the returned guard.
        if unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) } == 0 {
            return Ok(FlockGuard(file));
        }
        let error = io::Error::last_os_error();
        match error.kind() {
            io::ErrorKind::Interrupted => {}
            io::ErrorKind::WouldBlock => std::thread::sleep(std::time::Duration::from_millis(10)),
            _ => {
                return Err(map_io_error(
                    backend,
                    "failed to lock local secret store",
                    error,
                ));
            }
        }
    }
}

#[cfg(unix)]
impl Drop for FlockGuard {
    fn drop(&mut self) {
        use std::os::fd::AsRawFd;

        // SAFETY: the guard still owns the descriptor; unlock errors cannot be recovered in Drop.
        unsafe {
            libc::flock(self.0.as_raw_fd(), libc::LOCK_UN);
        }
    }
}

#[cfg(unix)]
fn validate_parent(path: &Path, backend: &SecretBackendId) -> Result<(), SecretError> {
    use std::os::unix::fs::PermissionsExt;

    let parent = path
        .parent()
        .ok_or_else(|| insecure(backend, LocalFileSecurityProblem::ParentNotDirectory))?;
    let metadata = fs::metadata(parent).map_err(|error| {
        map_io_error(
            backend,
            "failed to inspect local secret store parent",
            error,
        )
    })?;
    if !metadata.is_dir() {
        return Err(insecure(
            backend,
            LocalFileSecurityProblem::ParentNotDirectory,
        ));
    }
    if metadata.permissions().mode() & 0o022 != 0 {
        return Err(insecure(backend, LocalFileSecurityProblem::ParentWritable));
    }
    Ok(())
}

#[cfg(unix)]
fn validate_store_if_present(path: &Path, backend: &SecretBackendId) -> Result<(), SecretError> {
    use std::os::unix::fs::PermissionsExt;

    let metadata = match fs::symlink_metadata(path) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(()),
        Err(error) => {
            return Err(map_io_error(
                backend,
                "failed to inspect local secret store",
                error,
            ));
        }
    };
    if !metadata.file_type().is_file() {
        return Err(insecure(backend, LocalFileSecurityProblem::StoreNotRegular));
    }
    if metadata.permissions().mode() & 0o077 != 0 {
        return Err(insecure(
            backend,
            LocalFileSecurityProblem::StorePermissions,
        ));
    }
    Ok(())
}

#[cfg(unix)]
fn insecure(backend: &SecretBackendId, problem: LocalFileSecurityProblem) -> SecretError {
    let remediation = match problem {
        LocalFileSecurityProblem::StoreNotRegular => {
            "replace the local secret store with a private regular file"
        }
        LocalFileSecurityProblem::StorePermissions => {
            "remove group and other permissions from the local secret store"
        }
        LocalFileSecurityProblem::ParentNotDirectory => {
            "use a private directory for the local secret store"
        }
        LocalFileSecurityProblem::ParentWritable => {
            "remove group and other write permissions from the local secret store directory"
        }
    };
    SecretError::LockedOrDenied {
        backend: backend.clone(),
        remediation: Some(remediation.to_owned()),
    }
}

#[cfg(unix)]
fn claim_or_validate(path: &Path, backend: &SecretBackendId) -> Result<(), SecretError> {
    claim_or_validate_with_hook(path, backend, || Ok(()))
}

#[cfg(unix)]
fn claim_or_validate_with_hook(
    path: &Path,
    backend: &SecretBackendId,
    before_commit: impl FnOnce() -> io::Result<()>,
) -> Result<(), SecretError> {
    match fs::symlink_metadata(path) {
        Ok(_) => {
            read_store(path, backend)?;
            Ok(())
        }
        Err(error) if error.kind() == io::ErrorKind::NotFound => {
            let store = LocalFileStore {
                format_version: FORMAT_VERSION,
                backend_id: backend.clone(),
                secrets: HashMap::new(),
            };
            write_store_with_hooks(path, &store, before_commit, sync_directory)
                .map_err(|error| map_write_error(backend, error))
        }
        Err(error) => Err(map_io_error(
            backend,
            "failed to inspect local secret store",
            error,
        )),
    }
}

#[cfg(unix)]
fn read_store(path: &Path, backend: &SecretBackendId) -> Result<LocalFileStore, SecretError> {
    let bytes = match fs::read(path) {
        Ok(bytes) => bytes,
        Err(error) => {
            return Err(map_io_error(
                backend,
                "failed to read local secret store",
                error,
            ));
        }
    };
    let store = serde_json::from_slice::<LocalFileStore>(&bytes)
        .map_err(|_| backend_failure(backend, "invalid local secret store"))?;
    if store.format_version != FORMAT_VERSION {
        return Err(SecretError::ProtocolViolation {
            backend: backend.clone(),
            reason: "unsupported local secret store format".to_owned(),
        });
    }
    if store.backend_id != *backend {
        return Err(SecretError::BackendIdentityMismatch {
            backend: backend.clone(),
            file_backend: store.backend_id,
        });
    }
    Ok(store)
}

#[cfg(unix)]
enum WriteStoreError {
    BeforeCommit,
    AfterCommit,
}

#[cfg(unix)]
fn write_store_with_hooks(
    path: &Path,
    store: &LocalFileStore,
    before_commit: impl FnOnce() -> io::Result<()>,
    after_commit: impl FnOnce(&Path) -> io::Result<()>,
) -> Result<(), WriteStoreError> {
    let mut file = private_atomic_file(path).map_err(|_| WriteStoreError::BeforeCommit)?;
    serde_json::to_writer(&mut file, store).map_err(|_| WriteStoreError::BeforeCommit)?;
    file.write_all(b"\n")
        .map_err(|_| WriteStoreError::BeforeCommit)?;
    before_commit().map_err(|_| WriteStoreError::BeforeCommit)?;
    file.commit().map_err(|_| WriteStoreError::BeforeCommit)?;
    after_commit(path.parent().expect("store path has a parent"))
        .map_err(|_| WriteStoreError::AfterCommit)
}

#[cfg(unix)]
fn map_write_error(backend: &SecretBackendId, error: WriteStoreError) -> SecretError {
    match error {
        WriteStoreError::BeforeCommit => {
            backend_failure(backend, "failed to write local secret store")
        }
        WriteStoreError::AfterCommit => SecretError::UncertainWrite {
            backend: backend.clone(),
        },
    }
}

#[cfg(unix)]
fn backend_failure(backend: &SecretBackendId, message: &'static str) -> SecretError {
    SecretError::BackendFailure {
        backend: backend.clone(),
        redacted_message: message.to_owned(),
    }
}

#[cfg(unix)]
fn map_io_error(backend: &SecretBackendId, message: &'static str, error: io::Error) -> SecretError {
    if error.kind() == io::ErrorKind::PermissionDenied {
        SecretError::LockedOrDenied {
            backend: backend.clone(),
            remediation: Some("check local secret store ownership and permissions".to_owned()),
        }
    } else {
        backend_failure(backend, message)
    }
}

#[cfg(unix)]
fn create_private_ancestors(path: &Path) -> io::Result<()> {
    use std::os::unix::fs::PermissionsExt;

    let parent = path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "store path has no parent"))?;
    let mut missing = Vec::new();
    let mut current = parent;
    while !current.exists() {
        missing.push(current.to_owned());
        current = current.parent().ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                "store path has no existing ancestor",
            )
        })?;
    }
    for directory in missing.into_iter().rev() {
        match fs::create_dir(&directory) {
            Ok(()) => fs::set_permissions(&directory, fs::Permissions::from_mode(0o700))?,
            Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {}
            Err(error) => return Err(error),
        }
    }
    Ok(())
}

#[cfg(unix)]
fn private_atomic_file(path: &Path) -> io::Result<AtomicWriteFile> {
    use atomic_write_file::unix::OpenOptionsExt;
    use std::os::unix::fs::OpenOptionsExt as _;

    let mut options = AtomicOpenOptions::new();
    options.preserve_mode(false).mode(0o600);
    options.open(path)
}

#[cfg(unix)]
fn sync_directory(path: &Path) -> io::Result<()> {
    File::open(path)?.sync_all()
}

#[cfg(all(test, unix))]
mod tests {
    use super::*;
    use tempfile::tempdir;
    use yakshed_domain::{ConnectionId, CredentialSlot, OperationId};

    const CANARY: &str = "local-file-canary-41c49f1a";

    fn config(id: &str, path: &Path) -> SecretBackend {
        SecretBackend {
            id: SecretBackendId::new(id).unwrap(),
            settings: SecretBackendSettings::LocalFile {
                path: path.to_string_lossy().into_owned(),
            },
        }
    }

    fn context() -> SecretAccessContext {
        SecretAccessContext {
            connection_id: "0193f26e-7a72-7d42-bf77-0de14c4cc111"
                .parse::<ConnectionId>()
                .unwrap(),
            slot: CredentialSlot::new("provider.api_key").unwrap(),
            purpose: crate::SecretAccessPurpose::StartHarness,
            request_id: OperationId::new("local-file-test").unwrap(),
        }
    }

    #[tokio::test]
    async fn round_trip_set_read_delete() {
        let temp = tempdir().unwrap();
        let backend = LocalFileBackend::from_config(&config(
            "dev-local",
            &temp.path().join("store/secrets.json"),
        ))
        .unwrap();
        let locator = SecretLocator::new("connection/a/key").unwrap();

        assert_eq!(
            backend
                .put(
                    &locator,
                    &SecretString::from(CANARY.to_owned()),
                    PutSecretOptions::NO_OVERWRITE,
                )
                .await
                .unwrap(),
            PutSecretOutcome::Written
        );
        assert!(
            backend
                .resolve(&locator, &context())
                .await
                .unwrap()
                .expose(|value| value == CANARY)
        );
        assert_eq!(
            backend.delete(&locator).await.unwrap(),
            DeleteSecretOutcome::Deleted
        );
        assert!(matches!(
            backend.resolve(&locator, &context()).await,
            Err(SecretError::NotFound { .. })
        ));
    }

    #[tokio::test]
    async fn values_persist_across_backend_reinstantiation() {
        let temp = tempdir().unwrap();
        let config = config("dev-local", &temp.path().join("store/secrets.json"));
        let locator = SecretLocator::new("connection/a/key").unwrap();
        LocalFileBackend::from_config(&config)
            .unwrap()
            .put(
                &locator,
                &SecretString::from(CANARY.to_owned()),
                PutSecretOptions::NO_OVERWRITE,
            )
            .await
            .unwrap();

        assert!(
            LocalFileBackend::from_config(&config)
                .unwrap()
                .resolve(&locator, &context())
                .await
                .unwrap()
                .expose(|value| value == CANARY)
        );
    }

    #[tokio::test]
    async fn removing_and_readding_config_retains_file_values() {
        let temp = tempdir().unwrap();
        let config = config("dev-local", &temp.path().join("store/secrets.json"));
        let mut configured_backends = vec![config.clone()];
        let locator = SecretLocator::new("retained").unwrap();
        let backend = LocalFileBackend::from_config(&configured_backends[0]).unwrap();
        backend
            .put(
                &locator,
                &SecretString::from(CANARY.to_owned()),
                PutSecretOptions::NO_OVERWRITE,
            )
            .await
            .unwrap();
        configured_backends.clear();
        drop(backend);
        configured_backends.push(config);

        assert!(
            LocalFileBackend::from_config(&configured_backends[0])
                .unwrap()
                .resolve(&locator, &context())
                .await
                .unwrap()
                .expose(|value| value == CANARY)
        );
    }

    #[tokio::test]
    async fn separate_instances_share_path_lock_for_rmw_and_no_overwrite() {
        let temp = tempdir().unwrap();
        let path = temp.path().join("store/secrets.json");
        let settings = config("dev-local", &path);
        let first = Arc::new(LocalFileBackend::from_config(&settings).unwrap());
        let aliased = config("dev-local", &temp.path().join("store/./secrets.json"));
        let second = Arc::new(LocalFileBackend::from_config(&aliased).unwrap());
        let put = |backend: Arc<LocalFileBackend>, locator: &'static str| async move {
            backend
                .put(
                    &SecretLocator::new(locator).unwrap(),
                    &SecretString::from(format!("{locator}-canary")),
                    PutSecretOptions::NO_OVERWRITE,
                )
                .await
        };
        let (left, right) = tokio::join!(put(first.clone(), "left"), put(second.clone(), "right"));
        left.unwrap();
        right.unwrap();
        let first_initialized = first.state.initialized().unwrap();
        let second_initialized = second.state.initialized().unwrap();
        assert_eq!(first_initialized.path, second_initialized.path);
        assert!(Arc::ptr_eq(
            &first_initialized.lock,
            &second_initialized.lock
        ));
        first
            .resolve(&SecretLocator::new("left").unwrap(), &context())
            .await
            .unwrap();
        second
            .resolve(&SecretLocator::new("right").unwrap(), &context())
            .await
            .unwrap();

        let (first_result, second_result) =
            tokio::join!(put(first, "shared"), put(second, "shared"));
        assert_eq!(
            usize::from(first_result.is_ok()) + usize::from(second_result.is_ok()),
            1
        );
        assert_eq!(
            usize::from(matches!(
                first_result,
                Err(SecretError::AlreadyExists { .. })
            )) + usize::from(matches!(
                second_result,
                Err(SecretError::AlreadyExists { .. })
            )),
            1
        );
    }

    #[tokio::test]
    async fn aliased_path_second_backend_is_rejected_before_write() {
        use std::os::unix::fs::{PermissionsExt, symlink};

        let temp = tempdir().unwrap();
        let real_parent = temp.path().join("real");
        fs::create_dir(&real_parent).unwrap();
        fs::set_permissions(&real_parent, fs::Permissions::from_mode(0o700)).unwrap();
        let alias_parent = temp.path().join("alias");
        symlink(&real_parent, &alias_parent).unwrap();
        let path = real_parent.join("secrets.json");
        LocalFileBackend::from_config(&config("backend-a", &path))
            .unwrap()
            .probe()
            .await
            .unwrap();

        assert!(matches!(
            LocalFileBackend::from_config(&config("backend-b", &alias_parent.join("secrets.json")))
                .unwrap()
                .probe()
                .await,
            Err(SecretError::BackendIdentityMismatch { .. })
        ));
    }

    #[test]
    fn separate_open_file_descriptions_contend_on_flock() {
        use std::{sync::mpsc, time::Duration};

        let temp = tempdir().unwrap();
        let path = temp.path().join("store.lock");
        let backend = SecretBackendId::new("dev-local").unwrap();
        let first = lock_exclusive(&path, &backend, None).unwrap();
        let (acquired_tx, acquired_rx) = mpsc::channel();
        let contender = std::thread::spawn(move || {
            let _second = lock_exclusive(&path, &backend, None).unwrap();
            acquired_tx.send(()).unwrap();
        });

        assert!(acquired_rx.recv_timeout(Duration::from_millis(50)).is_err());
        drop(first);
        acquired_rx.recv_timeout(Duration::from_secs(1)).unwrap();
        contender.join().unwrap();
    }

    #[tokio::test]
    async fn insecure_preexisting_file_is_rejected() {
        use std::os::unix::fs::PermissionsExt;

        let temp = tempdir().unwrap();
        let path = temp.path().join("store/secrets.json");
        fs::create_dir(path.parent().unwrap()).unwrap();
        fs::set_permissions(path.parent().unwrap(), fs::Permissions::from_mode(0o700)).unwrap();
        fs::write(&path, b"{}").unwrap();
        fs::set_permissions(&path, fs::Permissions::from_mode(0o644)).unwrap();
        let backend = LocalFileBackend::from_config(&config("dev-local", &path)).unwrap();

        assert!(matches!(
            backend.probe().await,
            Err(SecretError::LockedOrDenied { .. })
        ));
    }

    #[tokio::test]
    async fn symlinked_store_is_rejected() {
        use std::os::unix::fs::symlink;

        let temp = tempdir().unwrap();
        let path = temp.path().join("store/secrets.json");
        fs::create_dir(path.parent().unwrap()).unwrap();
        let target = temp.path().join("target.json");
        fs::write(&target, b"{}").unwrap();
        symlink(target, &path).unwrap();
        let backend = LocalFileBackend::from_config(&config("dev-local", &path)).unwrap();

        assert!(matches!(
            backend.probe().await,
            Err(SecretError::LockedOrDenied { .. })
        ));
    }

    #[tokio::test]
    async fn group_writable_parent_is_rejected() {
        use std::os::unix::fs::PermissionsExt;

        let temp = tempdir().unwrap();
        let parent = temp.path().join("store");
        fs::create_dir(&parent).unwrap();
        fs::set_permissions(&parent, fs::Permissions::from_mode(0o770)).unwrap();
        let backend =
            LocalFileBackend::from_config(&config("dev-local", &parent.join("secrets.json")))
                .unwrap();

        assert!(matches!(
            backend.probe().await,
            Err(SecretError::LockedOrDenied { .. })
        ));
    }

    #[tokio::test]
    async fn permission_denied_read_maps_to_locked_or_denied_without_leaking_values() {
        use std::os::unix::fs::PermissionsExt;

        let temp = tempdir().unwrap();
        let path = temp.path().join("store/secrets.json");
        let backend = LocalFileBackend::from_config(&config("dev-local", &path)).unwrap();
        let locator = SecretLocator::new("mode-zero").unwrap();
        backend
            .put(
                &locator,
                &SecretString::from(CANARY.to_owned()),
                PutSecretOptions::NO_OVERWRITE,
            )
            .await
            .unwrap();
        fs::set_permissions(&path, fs::Permissions::from_mode(0o000)).unwrap();

        let Err(error) = backend.resolve(&locator, &context()).await else {
            panic!("mode-000 store must be rejected")
        };
        assert!(matches!(error, SecretError::LockedOrDenied { .. }));
        assert!(!format!("{error}").contains(CANARY));
        assert!(!format!("{error:?}").contains(CANARY));
    }

    #[tokio::test]
    async fn every_new_ancestor_is_private() {
        use std::os::unix::fs::PermissionsExt;

        let temp = tempdir().unwrap();
        let ancestors = [
            temp.path().join("a"),
            temp.path().join("a/b"),
            temp.path().join("a/b/c"),
        ];
        LocalFileBackend::from_config(&config("dev-local", &ancestors[2].join("store.json")))
            .unwrap()
            .probe()
            .await
            .unwrap();

        for ancestor in ancestors {
            assert_eq!(
                fs::metadata(ancestor).unwrap().permissions().mode() & 0o777,
                0o700
            );
        }
    }

    #[tokio::test]
    async fn construction_is_pure_and_first_use_initializes_storage() {
        let temp = tempdir().unwrap();
        let parent = temp.path().join("not-created-at-construction");
        let backend =
            LocalFileBackend::from_config(&config("dev-local", &parent.join("secrets.json")))
                .unwrap();

        assert!(!parent.exists());
        backend.probe().await.unwrap();
        assert!(parent.is_dir());
        assert!(parent.join("secrets.json").is_file());
    }

    #[tokio::test]
    async fn failed_initial_claim_leaves_no_file_and_next_access_recovers() {
        let temp = tempdir().unwrap();
        let backend = LocalFileBackend::from_config(&config(
            "dev-local",
            &temp.path().join("store/secrets.json"),
        ))
        .unwrap();
        let initialized = backend.state.initialized().unwrap();
        let _process_guard = initialized.lock.lock().unwrap();
        let file_guard =
            lock_exclusive(&lock_path(&initialized.path), &backend.state.id, None).unwrap();

        assert!(matches!(
            claim_or_validate_with_hook(&initialized.path, &backend.state.id, || Err(
                io::Error::other("initial claim fault")
            )),
            Err(SecretError::BackendFailure { .. })
        ));
        assert!(!initialized.path.exists());
        drop(file_guard);
        drop(_process_guard);

        backend.probe().await.unwrap();
        assert!(initialized.path.is_file());
    }

    #[tokio::test]
    async fn store_file_and_parent_have_private_permissions() {
        use std::os::unix::fs::PermissionsExt;

        let temp = tempdir().unwrap();
        let parent = temp.path().join("store");
        let path = parent.join("secrets.json");
        LocalFileBackend::from_config(&config("dev-local", &path))
            .unwrap()
            .put(
                &SecretLocator::new("key").unwrap(),
                &SecretString::from(CANARY.to_owned()),
                PutSecretOptions::NO_OVERWRITE,
            )
            .await
            .unwrap();

        assert_eq!(
            fs::metadata(parent).unwrap().permissions().mode() & 0o777,
            0o700
        );
        assert_eq!(
            fs::metadata(path).unwrap().permissions().mode() & 0o777,
            0o600
        );
    }

    #[test]
    fn dropped_queued_mutation_performs_no_write() {
        use std::sync::mpsc;

        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .max_blocking_threads(1)
            .build()
            .unwrap()
            .block_on(async {
                let temp = tempdir().unwrap();
                let backend = Arc::new(
                    LocalFileBackend::from_config(&config(
                        "dev-local",
                        &temp.path().join("store/secrets.json"),
                    ))
                    .unwrap(),
                );
                backend.probe().await.unwrap();
                let (started_tx, started_rx) = mpsc::channel();
                let (release_tx, release_rx) = mpsc::channel();
                let blocker = tokio::task::spawn_blocking(move || {
                    started_tx.send(()).unwrap();
                    release_rx.recv().unwrap();
                });
                started_rx.recv().unwrap();
                let mutation_backend = Arc::clone(&backend);
                let mutation = tokio::spawn(async move {
                    mutation_backend
                        .put(
                            &SecretLocator::new("abandoned").unwrap(),
                            &SecretString::from(CANARY.to_owned()),
                            PutSecretOptions::NO_OVERWRITE,
                        )
                        .await
                });
                tokio::task::yield_now().await;
                mutation.abort();
                let _ = mutation.await;
                release_tx.send(()).unwrap();
                blocker.await.unwrap();

                assert!(matches!(
                    backend
                        .resolve(&SecretLocator::new("abandoned").unwrap(), &context())
                        .await,
                    Err(SecretError::NotFound { .. })
                ));
            });
    }

    #[test]
    fn abandonment_returns_cancelled_while_flock_remains_contended() {
        use std::{sync::mpsc, time::Duration};

        let temp = tempdir().unwrap();
        let path = temp.path().join("store.lock");
        let backend = SecretBackendId::new("dev-local").unwrap();
        let held = lock_exclusive(&path, &backend, None).unwrap();
        let abandoned = Arc::new(AtomicBool::new(false));
        let worker_abandoned = Arc::clone(&abandoned);
        let (result_tx, result_rx) = mpsc::channel();
        let contender = std::thread::spawn(move || {
            result_tx
                .send(lock_exclusive(&path, &backend, Some(&worker_abandoned)))
                .unwrap();
        });

        std::thread::sleep(Duration::from_millis(30));
        abandoned.store(true, Ordering::Release);
        assert!(matches!(
            result_rx.recv_timeout(Duration::from_millis(250)).unwrap(),
            Err(SecretError::Cancelled { .. })
        ));
        drop(held);
        contender.join().unwrap();
    }

    #[tokio::test]
    async fn dropped_mutation_stops_waiting_for_flock_and_store_remains_operational() {
        use std::time::Duration;

        let temp = tempdir().unwrap();
        let path = temp.path().join("store/secrets.json");
        let backend = LocalFileBackend::from_config(&config("dev-local", &path)).unwrap();
        backend.probe().await.unwrap();
        let initialized = backend.state.initialized().unwrap();
        let held = lock_exclusive(&lock_path(&path), &backend.state.id, None).unwrap();
        let locator = SecretLocator::new("contended").unwrap();
        let value = SecretString::from(CANARY.to_owned());
        let mut mutation = Box::pin(backend.put(&locator, &value, PutSecretOptions::NO_OVERWRITE));

        assert!(
            tokio::time::timeout(Duration::from_millis(50), &mut mutation)
                .await
                .is_err()
        );
        assert!(initialized.lock.try_lock().is_err());
        drop(mutation);
        tokio::time::timeout(Duration::from_millis(250), async {
            loop {
                if initialized.lock.try_lock().is_ok() {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .expect("abandoned mutation must stop before the flock is released");
        drop(held);

        assert!(matches!(
            backend.resolve(&locator, &context()).await,
            Err(SecretError::NotFound { .. })
        ));
        backend
            .put(
                &locator,
                &SecretString::from(CANARY.to_owned()),
                PutSecretOptions::NO_OVERWRITE,
            )
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn failed_atomic_write_preserves_previous_contents() {
        let temp = tempdir().unwrap();
        let path = temp.path().join("store/secrets.json");
        let backend = LocalFileBackend::from_config(&config("dev-local", &path)).unwrap();
        let locator = SecretLocator::new("key").unwrap();
        backend
            .put(
                &locator,
                &SecretString::from(CANARY.to_owned()),
                PutSecretOptions::NO_OVERWRITE,
            )
            .await
            .unwrap();
        let previous = fs::read(&path).unwrap();
        let mut replacement = read_store(&path, &backend.state.id).unwrap();
        replacement
            .secrets
            .insert("key".to_owned(), "replacement-canary".to_owned());

        assert!(matches!(
            write_store_with_hooks(
                &path,
                &replacement,
                || Err(io::Error::other("fault")),
                |_| Ok(())
            ),
            Err(WriteStoreError::BeforeCommit)
        ));
        assert_eq!(fs::read(path).unwrap(), previous);
    }

    #[test]
    fn post_commit_failure_is_classified_as_uncertain_write() {
        let temp = tempdir().unwrap();
        let path = temp.path().join("store/secrets.json");
        let config = config("dev-local", &path);
        let backend = LocalFileBackend::from_config(&config).unwrap();
        let initialized = backend.state.initialized().unwrap();
        let store = LocalFileStore {
            format_version: FORMAT_VERSION,
            backend_id: backend.state.id.clone(),
            secrets: HashMap::new(),
        };
        let error = write_store_with_hooks(
            &initialized.path,
            &store,
            || Ok(()),
            |_| Err(io::Error::other("post-commit fault")),
        )
        .unwrap_err();

        assert!(matches!(
            map_write_error(&backend.state.id, error),
            SecretError::UncertainWrite { .. }
        ));
        assert!(initialized.path.exists());
    }

    #[test]
    fn errors_never_expose_stored_values() {
        let temp = tempdir().unwrap();
        let path = temp.path().join("store/secrets.json");
        let backend = LocalFileBackend::from_config(&config("dev-local", &path)).unwrap();
        let initialized = backend.state.initialized().unwrap();
        fs::write(&initialized.path, format!("not-json-{CANARY}")).unwrap();
        let Err(error) = backend.state.load(&initialized.path) else {
            panic!("invalid JSON must fail closed")
        };

        assert!(!format!("{error}").contains(CANARY));
        assert!(!format!("{error:?}").contains(CANARY));
    }
}
