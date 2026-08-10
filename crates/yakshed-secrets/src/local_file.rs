//! Plaintext local-development secret storage.
//!
//! On macOS and Linux, backends targeting the same canonical path share a process-global mutex and
//! an exclusive `flock` for each operation. Purge unlinks only the store; the zero-length `.lock`
//! sidecar is persistent coordination state and survives purge. Delete both files manually only
//! after every backend instance has stopped.
//! Dropped mutations are suppressed before writing starts. A drop after writing starts has an
//! uncertain outcome and must be reconciled before retrying. Reads wait for a contended file lock;
//! abandoned mutations stop waiting promptly.
//! Extended ACLs are rejected via native macOS ACL inspection or Linux POSIX ACL xattrs; YakShed
//! never removes filesystem ACLs on the user's behalf.

#[cfg(any(target_os = "macos", target_os = "linux"))]
use std::{
    collections::HashMap,
    fs::{self, File},
    io::{self, Read, Write},
    path::{Path, PathBuf},
    sync::{
        Arc, Mutex, OnceLock, Weak,
        atomic::{AtomicBool, Ordering},
    },
};

#[cfg(any(target_os = "linux", target_os = "macos"))]
use std::ffi::CString;

#[cfg(any(target_os = "macos", target_os = "linux"))]
use async_trait::async_trait;
#[cfg(any(target_os = "macos", target_os = "linux"))]
use atomic_write_file::{AtomicWriteFile, OpenOptions as AtomicOpenOptions};
#[cfg(any(target_os = "macos", target_os = "linux"))]
use secrecy::{ExposeSecret, SecretString};
#[cfg(any(target_os = "macos", target_os = "linux"))]
use serde::{Deserialize, Serialize};

#[cfg(any(target_os = "macos", target_os = "linux"))]
use crate::{
    DeleteSecretOutcome, LOCAL_FILE_BACKEND_KIND, PutSecretOptions, PutSecretOutcome,
    ResolvedSecret, ResolvedSecretSource, SecretAccessContext, SecretAdministrator,
    SecretBackendDescriptor, SecretBackendId, SecretBackendStatus, SecretError, SecretLocator,
    SecretReferenceSummary, SecretResolver,
};
use crate::{
    SecretBackend, SecretBackendConfigurationError, SecretBackendSettings,
    validate_backend_configuration,
};

#[cfg(any(target_os = "macos", target_os = "linux"))]
const FORMAT_VERSION: u32 = 1;

#[cfg(any(target_os = "macos", target_os = "linux"))]
static LOCAL_FILE_LOCKS: OnceLock<Mutex<HashMap<PathBuf, Weak<Mutex<()>>>>> = OnceLock::new();

pub struct LocalFileBackend {
    #[cfg(any(target_os = "macos", target_os = "linux"))]
    state: Arc<LocalFileState>,
}

#[cfg(any(target_os = "macos", target_os = "linux"))]
struct LocalFileState {
    id: SecretBackendId,
    configured_path: PathBuf,
    initialized: Mutex<Option<Arc<InitializedLocalFile>>>,
    #[cfg(test)]
    fail_next_post_commit_validation: AtomicBool,
}

#[cfg(any(target_os = "macos", target_os = "linux"))]
struct InitializedLocalFile {
    path: PathBuf,
    lock: Arc<Mutex<()>>,
}

#[cfg(any(target_os = "macos", target_os = "linux"))]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum LocalFileSecurityProblem {
    NotRegular,
    Permissions,
    Owner,
    Changed,
}

#[cfg(any(target_os = "macos", target_os = "linux"))]
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
        #[cfg(not(any(target_os = "macos", target_os = "linux")))]
        let _ = path;

        #[cfg(any(target_os = "macos", target_os = "linux"))]
        {
            Ok(Self {
                state: Arc::new(LocalFileState {
                    id: config.id.clone(),
                    configured_path: PathBuf::from(path),
                    initialized: Mutex::new(None),
                    #[cfg(test)]
                    fail_next_post_commit_validation: AtomicBool::new(false),
                }),
            })
        }
        #[cfg(not(any(target_os = "macos", target_os = "linux")))]
        unreachable!("platform validation rejects local-file outside macOS and Linux")
    }

    /// Removes the local store while holding both coordination locks.
    ///
    /// The `.lock` sidecar is intentionally retained so queued contenders keep using the same
    /// inode across purge. Manual deletion of both files is safe only with all instances stopped.
    #[cfg(any(target_os = "macos", target_os = "linux"))]
    pub async fn purge(&self) -> Result<(), SecretError> {
        self.run_abandonable(|state, abandoned| state.purge(abandoned))
            .await
    }

    #[cfg(any(target_os = "macos", target_os = "linux"))]
    async fn run_abandonable<T>(
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

#[cfg(any(target_os = "macos", target_os = "linux"))]
struct AbandonmentGuard {
    abandoned: Arc<AtomicBool>,
    completed: bool,
}

#[cfg(any(target_os = "macos", target_os = "linux"))]
impl Drop for AbandonmentGuard {
    fn drop(&mut self) {
        if !self.completed {
            self.abandoned.store(true, Ordering::Release);
        }
    }
}

#[cfg(any(target_os = "macos", target_os = "linux"))]
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
        validate_parent(&path, &self.id)?;
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
        write_store(
            path,
            store,
            &self.id,
            || Ok(()),
            AtomicWriteFile::commit,
            || {
                #[cfg(test)]
                if self
                    .fail_next_post_commit_validation
                    .swap(false, Ordering::AcqRel)
                {
                    return Err(backend_failure(
                        &self.id,
                        "injected post-commit validation failure",
                    ));
                }
                validate_store_if_present(path, &self.id)
            },
        )
    }

    fn purge(&self, abandoned: &AtomicBool) -> Result<(), SecretError> {
        let initialized = self.initialized()?;
        self.with_process_lock(&initialized, Some(abandoned), || {
            validate_parent(&initialized.path, &self.id)?;
            let _file_guard =
                lock_exclusive(&lock_path(&initialized.path), &self.id, Some(abandoned))?;
            validate_parent(&initialized.path, &self.id)?;
            if abandoned.load(Ordering::Acquire) {
                return Err(SecretError::Cancelled {
                    backend: self.id.clone(),
                });
            }
            if initialized.path.exists() {
                self.load(&initialized.path)?;
            }

            let store_removed = match fs::remove_file(&initialized.path) {
                Ok(()) => true,
                Err(error) if error.kind() == io::ErrorKind::NotFound => false,
                Err(error) => {
                    return Err(map_io_error(
                        &self.id,
                        "failed to purge local secret store",
                        error,
                    ));
                }
            };
            if abandoned.load(Ordering::Acquire) {
                return Err(SecretError::UncertainWrite {
                    backend: self.id.clone(),
                });
            }
            sync_directory(initialized.path.parent().expect("store path has a parent")).map_err(
                |error| {
                    if store_removed {
                        SecretError::UncertainWrite {
                            backend: self.id.clone(),
                        }
                    } else {
                        map_io_error(&self.id, "failed to sync purged local secret store", error)
                    }
                },
            )
        })
    }

    fn with_store_lock<T>(
        &self,
        abandoned: Option<&AtomicBool>,
        action: impl FnOnce(&Path) -> Result<T, SecretError>,
    ) -> Result<T, SecretError> {
        let initialized = self.initialized()?;
        self.with_process_lock(&initialized, abandoned, || {
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
        })
    }

    fn with_process_lock<T>(
        &self,
        initialized: &InitializedLocalFile,
        abandoned: Option<&AtomicBool>,
        action: impl FnOnce() -> Result<T, SecretError>,
    ) -> Result<T, SecretError> {
        let guard = loop {
            match initialized.lock.try_lock() {
                Ok(guard) => break guard,
                Err(std::sync::TryLockError::Poisoned(poisoned)) => break poisoned.into_inner(),
                Err(std::sync::TryLockError::WouldBlock) => {
                    if abandoned.is_some_and(|flag| flag.load(Ordering::Acquire)) {
                        return Err(SecretError::Cancelled {
                            backend: self.id.clone(),
                        });
                    }
                    std::thread::sleep(std::time::Duration::from_millis(10));
                }
            }
        };
        let result = action();
        drop(guard);
        result
    }
}

#[cfg(any(target_os = "macos", target_os = "linux"))]
#[async_trait]
impl SecretResolver for LocalFileBackend {
    fn descriptor(&self) -> SecretBackendDescriptor {
        SecretBackendDescriptor {
            id: self.state.id.clone(),
            kind: LOCAL_FILE_BACKEND_KIND.to_owned(),
        }
    }

    async fn probe(&self) -> Result<SecretBackendStatus, SecretError> {
        self.run_abandonable(|state, abandoned| {
            state.with_store_lock(Some(abandoned), |path| {
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
        self.run_abandonable(move |state, abandoned| {
            state.with_store_lock(Some(abandoned), |path| {
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

#[cfg(any(target_os = "macos", target_os = "linux"))]
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
        self.run_abandonable(move |state, abandoned| {
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
        self.run_abandonable(move |state, abandoned| {
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

#[cfg(any(target_os = "macos", target_os = "linux"))]
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

#[cfg(any(target_os = "macos", target_os = "linux"))]
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

#[cfg(any(target_os = "macos", target_os = "linux"))]
fn lock_path(path: &Path) -> PathBuf {
    let mut lock = path.as_os_str().to_os_string();
    lock.push(".lock");
    PathBuf::from(lock)
}

#[cfg(any(target_os = "macos", target_os = "linux"))]
struct FlockGuard(File);

#[cfg(any(target_os = "macos", target_os = "linux"))]
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

#[cfg(any(target_os = "macos", target_os = "linux"))]
impl Drop for FlockGuard {
    fn drop(&mut self) {
        use std::os::fd::AsRawFd;

        // SAFETY: the guard still owns the descriptor; unlock errors cannot be recovered in Drop.
        unsafe {
            libc::flock(self.0.as_raw_fd(), libc::LOCK_UN);
        }
    }
}

#[cfg(any(target_os = "macos", target_os = "linux"))]
fn validate_parent(path: &Path, backend: &SecretBackendId) -> Result<(), SecretError> {
    use std::os::unix::fs::{MetadataExt, PermissionsExt};

    let parent = path
        .parent()
        .ok_or_else(|| untrusted_component(backend, "store parent", path, "missing"))?;
    let metadata = match fs::symlink_metadata(parent) {
        Ok(metadata) => metadata,
        Err(error)
            if matches!(
                error.kind(),
                io::ErrorKind::NotFound | io::ErrorKind::NotADirectory
            ) =>
        {
            return Err(untrusted_component(
                backend,
                "store parent",
                parent,
                "not a directory",
            ));
        }
        Err(error) => {
            return Err(map_io_error(
                backend,
                "failed to inspect local secret store parent",
                error,
            ));
        }
    };
    if !metadata.is_dir() {
        return Err(untrusted_component(
            backend,
            "store parent",
            parent,
            "not a directory",
        ));
    }
    validate_no_extended_acl(parent, backend, true, "store parent")?;
    if !owned_by_effective_uid(metadata.uid(), effective_uid()) {
        return Err(untrusted_component(
            backend,
            "store parent",
            parent,
            "foreign owner",
        ));
    }
    if metadata.permissions().mode() & 0o022 != 0 {
        return Err(untrusted_component(
            backend,
            "store parent",
            parent,
            "group- or other-writable",
        ));
    }
    validate_ancestor_chain(parent, backend)?;
    Ok(())
}

#[cfg(any(target_os = "macos", target_os = "linux"))]
fn validate_ancestor_chain(parent: &Path, backend: &SecretBackendId) -> Result<(), SecretError> {
    use std::os::unix::fs::{MetadataExt, PermissionsExt};

    for ancestor in parent
        .ancestors()
        .skip(1)
        .collect::<Vec<_>>()
        .into_iter()
        .rev()
    {
        let metadata = match fs::symlink_metadata(ancestor) {
            Ok(metadata) => metadata,
            Err(error)
                if matches!(
                    error.kind(),
                    io::ErrorKind::NotFound | io::ErrorKind::NotADirectory
                ) =>
            {
                return Err(untrusted_component(
                    backend,
                    "ancestor",
                    ancestor,
                    "not a directory",
                ));
            }
            Err(error) => {
                return Err(map_io_error(
                    backend,
                    "failed to inspect local secret store ancestor",
                    error,
                ));
            }
        };
        if !metadata.is_dir() {
            return Err(untrusted_component(
                backend,
                "ancestor",
                ancestor,
                "not a directory",
            ));
        }
        validate_no_extended_acl(ancestor, backend, false, "ancestor")?;
        if !ancestor_owner_is_trusted(metadata.uid(), effective_uid()) {
            return Err(untrusted_component(
                backend,
                "ancestor",
                ancestor,
                "foreign owner",
            ));
        }
        if metadata.permissions().mode() & 0o022 != 0 {
            return Err(untrusted_component(
                backend,
                "ancestor",
                ancestor,
                "group- or other-writable",
            ));
        }
    }
    Ok(())
}

#[cfg(any(target_os = "macos", target_os = "linux"))]
fn validate_store_if_present(path: &Path, backend: &SecretBackendId) -> Result<(), SecretError> {
    open_validated_store_if_present(path, backend).map(|_| ())
}

#[cfg(any(target_os = "macos", target_os = "linux"))]
fn open_validated_store_if_present(
    path: &Path,
    backend: &SecretBackendId,
) -> Result<Option<File>, SecretError> {
    use std::os::unix::fs::{MetadataExt, OpenOptionsExt, PermissionsExt};

    let path_metadata = match fs::symlink_metadata(path) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(error) => {
            return Err(map_io_error(
                backend,
                "failed to inspect local secret store",
                error,
            ));
        }
    };
    if !path_metadata.file_type().is_file() {
        return Err(insecure(backend, LocalFileSecurityProblem::NotRegular));
    }
    validate_no_extended_acl(path, backend, true, "store")?;
    if path_metadata.permissions().mode() & 0o077 != 0 {
        return Err(insecure(backend, LocalFileSecurityProblem::Permissions));
    }
    if !owned_by_effective_uid(path_metadata.uid(), effective_uid()) {
        return Err(insecure(backend, LocalFileSecurityProblem::Owner));
    }
    let file = fs::OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_NOFOLLOW)
        .open(path)
        .map_err(|error| map_io_error(backend, "failed to open local secret store", error))?;
    let descriptor_metadata = file.metadata().map_err(|error| {
        map_io_error(backend, "failed to inspect open local secret store", error)
    })?;
    if !descriptor_metadata.file_type().is_file() {
        return Err(insecure(backend, LocalFileSecurityProblem::NotRegular));
    }
    if descriptor_metadata.permissions().mode() & 0o077 != 0 {
        return Err(insecure(backend, LocalFileSecurityProblem::Permissions));
    }
    if !owned_by_effective_uid(descriptor_metadata.uid(), effective_uid()) {
        return Err(insecure(backend, LocalFileSecurityProblem::Owner));
    }
    if descriptor_metadata.dev() != path_metadata.dev()
        || descriptor_metadata.ino() != path_metadata.ino()
    {
        return Err(insecure(backend, LocalFileSecurityProblem::Changed));
    }
    Ok(Some(file))
}

#[cfg(any(target_os = "macos", target_os = "linux"))]
const fn owned_by_effective_uid(owner: u32, effective_uid: u32) -> bool {
    owner == effective_uid
}

#[cfg(any(target_os = "macos", target_os = "linux"))]
const fn ancestor_owner_is_trusted(owner: u32, effective_uid: u32) -> bool {
    owner == 0 || owner == effective_uid
}

#[cfg(any(target_os = "macos", target_os = "linux"))]
fn effective_uid() -> u32 {
    // SAFETY: `geteuid` takes no arguments and has no preconditions.
    unsafe { libc::geteuid() }
}

#[cfg(any(target_os = "macos", target_os = "linux"))]
fn validate_no_extended_acl(
    path: &Path,
    backend: &SecretBackendId,
    reject_default_acl: bool,
    component: &'static str,
) -> Result<(), SecretError> {
    if let Some(check) = extended_acl_problem(path, reject_default_acl).map_err(|error| {
        map_io_error(
            backend,
            "failed to inspect local secret store access controls",
            error,
        )
    })? {
        return Err(untrusted_component(backend, component, path, check));
    }
    Ok(())
}

#[cfg(target_os = "macos")]
fn extended_acl_problem(
    path: &Path,
    _reject_default_acl: bool,
) -> io::Result<Option<&'static str>> {
    use std::{os::raw::c_void, os::unix::ffi::OsStrExt, ptr};

    unsafe extern "C" {
        fn acl_get_file(path: *const libc::c_char, acl_type: libc::c_int) -> *mut c_void;
        fn acl_get_entry(
            acl: *mut c_void,
            entry_id: libc::c_int,
            entry: *mut *mut c_void,
        ) -> libc::c_int;
        fn acl_free(object: *mut c_void) -> libc::c_int;
    }

    const ACL_TYPE_EXTENDED: libc::c_int = 0x0000_0100;
    const ACL_FIRST_ENTRY: libc::c_int = 0;

    let path = CString::new(path.as_os_str().as_bytes())
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "path contains NUL"))?;
    // SAFETY: the path is NUL-terminated, and the returned ACL is released before returning.
    let acl = unsafe { acl_get_file(path.as_ptr(), ACL_TYPE_EXTENDED) };
    if acl.is_null() {
        let error = io::Error::last_os_error();
        return if error.raw_os_error() == Some(libc::ENOENT) {
            Ok(None)
        } else {
            Err(error)
        };
    }
    let mut entry = ptr::null_mut();
    // SAFETY: `acl` is valid and `entry` points to writable storage for the borrowed entry.
    let result = unsafe { acl_get_entry(acl, ACL_FIRST_ENTRY, &mut entry) };
    // SAFETY: `acl` was returned by `acl_get_file` and has not yet been freed.
    unsafe { acl_free(acl) };
    match result {
        0 => Ok(Some("extended ACL")),
        _ => Err(io::Error::last_os_error()),
    }
}

#[cfg(target_os = "linux")]
fn extended_acl_problem(path: &Path, reject_default_acl: bool) -> io::Result<Option<&'static str>> {
    let (access_acl, default_acl) = linux_acl_xattrs(path)?;
    Ok(if access_acl {
        Some("extended access ACL")
    } else if linux_acl_is_untrusted(access_acl, default_acl, reject_default_acl) {
        Some("default ACL")
    } else {
        None
    })
}

#[cfg(any(test, target_os = "linux"))]
const fn linux_acl_is_untrusted(
    access_acl: bool,
    default_acl: bool,
    is_direct_parent: bool,
) -> bool {
    access_acl || (is_direct_parent && default_acl)
}

#[cfg(target_os = "linux")]
fn linux_acl_xattrs(path: &Path) -> io::Result<(bool, bool)> {
    use std::{os::unix::ffi::OsStrExt, ptr};

    let path = CString::new(path.as_os_str().as_bytes())
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "path contains NUL"))?;
    // SAFETY: the path is NUL-terminated; a null output buffer requests the required size.
    let size = unsafe { libc::listxattr(path.as_ptr(), ptr::null_mut(), 0) };
    if size < 0 {
        return Err(io::Error::last_os_error());
    }
    if size == 0 {
        return Ok((false, false));
    }
    let mut names = vec![0_u8; size as usize];
    // SAFETY: `names` exposes `size` writable bytes for the NUL-separated attribute names.
    let written = unsafe {
        libc::listxattr(
            path.as_ptr(),
            names.as_mut_ptr().cast::<libc::c_char>(),
            names.len(),
        )
    };
    if written < 0 {
        return Err(io::Error::last_os_error());
    }
    let mut access_acl = false;
    let mut default_acl = false;
    for name in names[..written as usize].split(|byte| *byte == 0) {
        access_acl |= name == b"system.posix_acl_access";
        default_acl |= name == b"system.posix_acl_default";
    }
    Ok((access_acl, default_acl))
}

#[cfg(any(target_os = "macos", target_os = "linux"))]
fn untrusted_component(
    backend: &SecretBackendId,
    component: &str,
    path: &Path,
    check: &str,
) -> SecretError {
    SecretError::LockedOrDenied {
        backend: backend.clone(),
        remediation: Some(format!("untrusted {component} {}: {check}", path.display())),
    }
}

#[cfg(any(target_os = "macos", target_os = "linux"))]
fn insecure(backend: &SecretBackendId, problem: LocalFileSecurityProblem) -> SecretError {
    let remediation = match problem {
        LocalFileSecurityProblem::NotRegular => {
            "replace the local secret store with a private regular file"
        }
        LocalFileSecurityProblem::Permissions => {
            "remove group and other permissions from the local secret store"
        }
        LocalFileSecurityProblem::Owner => {
            "make the effective user the owner of the local secret store"
        }
        LocalFileSecurityProblem::Changed => "retry after securing the replaced local secret store",
    };
    SecretError::LockedOrDenied {
        backend: backend.clone(),
        remediation: Some(remediation.to_owned()),
    }
}

#[cfg(any(target_os = "macos", target_os = "linux"))]
fn claim_or_validate(path: &Path, backend: &SecretBackendId) -> Result<(), SecretError> {
    claim_or_validate_with_hook(path, backend, || Ok(()))
}

#[cfg(any(target_os = "macos", target_os = "linux"))]
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
            write_store(
                path,
                &store,
                backend,
                before_commit,
                AtomicWriteFile::commit,
                || validate_store_if_present(path, backend),
            )
        }
        Err(error) => Err(map_io_error(
            backend,
            "failed to inspect local secret store",
            error,
        )),
    }
}

#[cfg(any(target_os = "macos", target_os = "linux"))]
fn write_store(
    path: &Path,
    store: &LocalFileStore,
    backend: &SecretBackendId,
    before_commit: impl FnOnce() -> io::Result<()>,
    commit: impl FnOnce(AtomicWriteFile) -> io::Result<()>,
    post_commit_validation: impl FnOnce() -> Result<(), SecretError>,
) -> Result<(), SecretError> {
    write_store_with_hooks(path, store, before_commit, commit, sync_directory)
        .map_err(|error| map_write_error(backend, error))?;
    post_commit_validation().map_err(|_| SecretError::UncertainWrite {
        backend: backend.clone(),
    })
}

#[cfg(any(target_os = "macos", target_os = "linux"))]
fn read_store(path: &Path, backend: &SecretBackendId) -> Result<LocalFileStore, SecretError> {
    let mut file = open_validated_store_if_present(path, backend)?
        .ok_or_else(|| backend_failure(backend, "local secret store disappeared before opening"))?;
    let mut bytes = Vec::new();
    file.read_to_end(&mut bytes)
        .map_err(|error| map_io_error(backend, "failed to read local secret store", error))?;
    let store = serde_json::from_slice::<LocalFileStore>(&bytes)
        .map_err(|_| backend_failure(backend, "invalid local secret store"))?;
    if store.format_version != FORMAT_VERSION {
        return Err(SecretError::ProtocolViolation {
            backend: backend.clone(),
            reason: "unsupported local secret store format".to_owned(),
        });
    }
    if store.backend_id != *backend {
        return Err(SecretError::ProtocolViolation {
            backend: backend.clone(),
            reason: "local secret store belongs to another backend".to_owned(),
        });
    }
    Ok(store)
}

#[cfg(any(target_os = "macos", target_os = "linux"))]
enum WriteStoreError {
    BeforeCommit,
    Uncertain,
}

#[cfg(any(target_os = "macos", target_os = "linux"))]
fn write_store_with_hooks(
    path: &Path,
    store: &LocalFileStore,
    before_commit: impl FnOnce() -> io::Result<()>,
    commit: impl FnOnce(AtomicWriteFile) -> io::Result<()>,
    after_commit: impl FnOnce(&Path) -> io::Result<()>,
) -> Result<(), WriteStoreError> {
    let mut file = private_atomic_file(path).map_err(|_| WriteStoreError::BeforeCommit)?;
    serde_json::to_writer(&mut file, store).map_err(|_| WriteStoreError::BeforeCommit)?;
    file.write_all(b"\n")
        .map_err(|_| WriteStoreError::BeforeCommit)?;
    before_commit().map_err(|_| WriteStoreError::BeforeCommit)?;
    commit(file).map_err(|_| WriteStoreError::Uncertain)?;
    after_commit(path.parent().expect("store path has a parent"))
        .map_err(|_| WriteStoreError::Uncertain)
}

#[cfg(any(target_os = "macos", target_os = "linux"))]
fn map_write_error(backend: &SecretBackendId, error: WriteStoreError) -> SecretError {
    match error {
        WriteStoreError::BeforeCommit => {
            backend_failure(backend, "failed to write local secret store")
        }
        WriteStoreError::Uncertain => SecretError::UncertainWrite {
            backend: backend.clone(),
        },
    }
}

#[cfg(any(target_os = "macos", target_os = "linux"))]
fn backend_failure(backend: &SecretBackendId, message: &'static str) -> SecretError {
    SecretError::BackendFailure {
        backend: backend.clone(),
        redacted_message: message.to_owned(),
    }
}

#[cfg(any(target_os = "macos", target_os = "linux"))]
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

#[cfg(any(target_os = "macos", target_os = "linux"))]
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

#[cfg(any(target_os = "macos", target_os = "linux"))]
fn private_atomic_file(path: &Path) -> io::Result<AtomicWriteFile> {
    use atomic_write_file::unix::OpenOptionsExt;
    use std::os::unix::fs::OpenOptionsExt as _;

    let mut options = AtomicOpenOptions::new();
    options.preserve_mode(false).mode(0o600);
    options.open(path)
}

#[cfg(any(target_os = "macos", target_os = "linux"))]
fn sync_directory(path: &Path) -> io::Result<()> {
    File::open(path)?.sync_all()
}

#[cfg(all(test, any(target_os = "macos", target_os = "linux")))]
mod tests {
    use super::*;
    use yakshed_domain::{ConnectionId, CredentialSlot, OperationId};

    const CANARY: &str = "local-file-canary-41c49f1a";

    #[cfg(target_os = "macos")]
    fn tempdir() -> io::Result<tempfile::TempDir> {
        tempfile::tempdir()
    }

    #[cfg(target_os = "linux")]
    fn tempdir() -> io::Result<tempfile::TempDir> {
        let home = std::env::var_os("HOME")
            .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "test home is unavailable"))?;
        tempfile::Builder::new()
            .prefix(".yakshed-secrets-test-")
            .tempdir_in(home)
    }

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

    async fn worker_releases_process_lock(initialized: &InitializedLocalFile) {
        tokio::time::timeout(std::time::Duration::from_millis(250), async {
            loop {
                if initialized.lock.try_lock().is_ok() {
                    break;
                }
                tokio::time::sleep(std::time::Duration::from_millis(5)).await;
            }
        })
        .await
        .expect("abandoned operation must release the process lock promptly");
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
    async fn isolated_tempdir_ancestor_chain_is_trusted() {
        let temp = tempdir().unwrap();
        let backend = LocalFileBackend::from_config(&config(
            "dev-local",
            &temp.path().join("store/secrets.json"),
        ))
        .unwrap();

        backend.probe().await.unwrap();
        let initialized = backend.state.initialized().unwrap();
        validate_parent(&initialized.path, &backend.state.id).unwrap();
        assert!(
            initialized
                .path
                .starts_with(fs::canonicalize(temp.path()).unwrap())
        );
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
    async fn purge_serializes_with_mutation_and_reinitializes_empty_store() {
        use std::os::unix::fs::MetadataExt;

        let temp = tempdir().unwrap();
        let path = temp.path().join("store/secrets.json");
        let config = config("dev-local", &path);
        let backend = Arc::new(LocalFileBackend::from_config(&config).unwrap());
        let existing = SecretLocator::new("existing").unwrap();
        backend
            .put(
                &existing,
                &SecretString::from(CANARY.to_owned()),
                PutSecretOptions::NO_OVERWRITE,
            )
            .await
            .unwrap();

        let sidecar = lock_path(&backend.state.initialized().unwrap().path);
        let sidecar_before = fs::metadata(&sidecar).unwrap();
        let concurrent_locator = SecretLocator::new("concurrent").unwrap();
        let concurrent_value = SecretString::from("concurrent-canary");
        let mutation = backend.put(
            &concurrent_locator,
            &concurrent_value,
            PutSecretOptions::NO_OVERWRITE,
        );
        let purge = backend.purge();
        let _ = tokio::join!(mutation, purge);

        backend.purge().await.unwrap();
        assert!(!path.exists());
        let sidecar_after = fs::metadata(&sidecar).unwrap();
        assert_eq!(sidecar_before.dev(), sidecar_after.dev());
        assert_eq!(sidecar_before.ino(), sidecar_after.ino());
        assert!(matches!(
            backend.resolve(&existing, &context()).await,
            Err(SecretError::NotFound { .. })
        ));
        assert!(path.exists());
    }

    #[tokio::test]
    async fn aliased_backend_cannot_purge_foreign_owned_store() {
        use std::os::unix::fs::{PermissionsExt, symlink};

        let temp = tempdir().unwrap();
        let real_parent = temp.path().join("real");
        fs::create_dir(&real_parent).unwrap();
        fs::set_permissions(&real_parent, fs::Permissions::from_mode(0o700)).unwrap();
        let alias_parent = temp.path().join("alias");
        symlink(&real_parent, &alias_parent).unwrap();
        let path = real_parent.join("secrets.json");
        let first = LocalFileBackend::from_config(&config("backend-a", &path)).unwrap();
        let locator = SecretLocator::new("purge-identity").unwrap();
        first
            .put(
                &locator,
                &SecretString::from(CANARY.to_owned()),
                PutSecretOptions::NO_OVERWRITE,
            )
            .await
            .unwrap();
        let second =
            LocalFileBackend::from_config(&config("backend-b", &alias_parent.join("secrets.json")))
                .unwrap();

        assert!(matches!(
            second.purge().await,
            Err(SecretError::ProtocolViolation { .. })
        ));
        assert!(
            first
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
            Err(SecretError::ProtocolViolation { .. })
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
    async fn group_writable_intermediate_ancestor_is_rejected() {
        assert_writable_intermediate_ancestor_is_rejected(0o720).await;
    }

    #[tokio::test]
    async fn other_writable_intermediate_ancestor_is_rejected() {
        assert_writable_intermediate_ancestor_is_rejected(0o702).await;
    }

    async fn assert_writable_intermediate_ancestor_is_rejected(mode: u32) {
        use std::os::unix::fs::PermissionsExt;

        let temp = tempdir().unwrap();
        let intermediate = temp.path().join("mutable");
        let parent = intermediate.join("private");
        fs::create_dir_all(&parent).unwrap();
        fs::set_permissions(&intermediate, fs::Permissions::from_mode(mode)).unwrap();
        fs::set_permissions(&parent, fs::Permissions::from_mode(0o700)).unwrap();
        let backend =
            LocalFileBackend::from_config(&config("dev-local", &parent.join("secrets.json")))
                .unwrap();

        let Err(error) = backend.probe().await else {
            panic!("mutable intermediate ancestor must be rejected")
        };
        let rendered = format!("{error} {error:?}");
        let intermediate = fs::canonicalize(intermediate).unwrap();
        assert!(rendered.contains(&intermediate.display().to_string()));
        assert!(rendered.contains("group- or other-writable"));
        assert!(!rendered.contains(CANARY));
        let SecretError::LockedOrDenied {
            remediation: Some(remediation),
            ..
        } = error
        else {
            panic!("mutable intermediate ancestor must map to LockedOrDenied")
        };
        assert!(remediation.contains("ancestor"), "{remediation}");
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

    fn assert_acl_rejected(error: SecretError, path: &Path) {
        assert!(!format!("{error}").contains(CANARY));
        assert!(!format!("{error:?}").contains(CANARY));
        let SecretError::LockedOrDenied {
            remediation: Some(remediation),
            ..
        } = error
        else {
            panic!("extended ACL must map to LockedOrDenied")
        };
        let path = fs::canonicalize(path).unwrap();
        assert!(remediation.contains(&path.display().to_string()));
        assert!(remediation.contains("ACL"));
    }

    #[cfg(target_os = "macos")]
    fn add_macos_acl(path: &Path) {
        let status = std::process::Command::new("chmod")
            .args(["+a", "everyone allow read"])
            .arg(path)
            .status()
            .unwrap();
        assert!(status.success());
    }

    #[cfg(target_os = "macos")]
    #[tokio::test]
    async fn macos_store_and_parent_acls_are_rejected_without_exposing_secrets() {
        for target_parent in [false, true] {
            let temp = tempdir().unwrap();
            let path = temp.path().join("store/secrets.json");
            let backend = LocalFileBackend::from_config(&config("dev-local", &path)).unwrap();
            let locator = SecretLocator::new("acl-protected").unwrap();
            backend
                .put(
                    &locator,
                    &SecretString::from(CANARY.to_owned()),
                    PutSecretOptions::NO_OVERWRITE,
                )
                .await
                .unwrap();
            let acl_path = if target_parent {
                path.parent().unwrap()
            } else {
                &path
            };
            add_macos_acl(acl_path);

            let Err(error) = backend.resolve(&locator, &context()).await else {
                panic!("ACL-protected store must be rejected")
            };
            assert_acl_rejected(error, acl_path);
        }
    }

    #[cfg(target_os = "linux")]
    fn add_linux_posix_acl(path: &Path) -> bool {
        use std::os::unix::ffi::OsStrExt;

        fn push_entry(bytes: &mut Vec<u8>, tag: u16, permissions: u16, id: u32) {
            bytes.extend_from_slice(&tag.to_le_bytes());
            bytes.extend_from_slice(&permissions.to_le_bytes());
            bytes.extend_from_slice(&id.to_le_bytes());
        }

        let mut acl = 2_u32.to_le_bytes().to_vec();
        push_entry(&mut acl, 0x01, 0o7, u32::MAX);
        // SAFETY: `geteuid` takes no arguments and has no preconditions.
        let other_id = unsafe { libc::geteuid() }.wrapping_add(1);
        push_entry(&mut acl, 0x02, 0o4, other_id);
        push_entry(&mut acl, 0x04, 0, u32::MAX);
        push_entry(&mut acl, 0x10, 0o4, u32::MAX);
        push_entry(&mut acl, 0x20, 0, u32::MAX);
        let path = CString::new(path.as_os_str().as_bytes()).unwrap();
        // SAFETY: all pointers reference live, correctly sized byte buffers for this call.
        let result = unsafe {
            libc::setxattr(
                path.as_ptr(),
                c"system.posix_acl_access".as_ptr(),
                acl.as_ptr().cast(),
                acl.len(),
                0,
            )
        };
        if result != 0 {
            eprintln!(
                "skipping POSIX ACL assertion: {}",
                io::Error::last_os_error()
            );
            return false;
        }
        true
    }

    #[cfg(target_os = "macos")]
    fn add_test_acl(path: &Path) -> bool {
        add_macos_acl(path);
        true
    }

    #[cfg(target_os = "linux")]
    fn add_test_acl(path: &Path) -> bool {
        add_linux_posix_acl(path)
    }

    #[tokio::test]
    async fn intermediate_ancestor_acl_is_rejected_without_exposing_secrets() {
        use std::os::unix::fs::PermissionsExt;

        let temp = tempdir().unwrap();
        let intermediate = temp.path().join("intermediate");
        let parent = intermediate.join("private");
        fs::create_dir_all(&parent).unwrap();
        fs::set_permissions(&intermediate, fs::Permissions::from_mode(0o700)).unwrap();
        fs::set_permissions(&parent, fs::Permissions::from_mode(0o700)).unwrap();
        let path = parent.join("secrets.json");
        let backend = LocalFileBackend::from_config(&config("dev-local", &path)).unwrap();
        let locator = SecretLocator::new("acl-ancestor").unwrap();
        backend
            .put(
                &locator,
                &SecretString::from(CANARY.to_owned()),
                PutSecretOptions::NO_OVERWRITE,
            )
            .await
            .unwrap();
        if !add_test_acl(&intermediate) {
            return;
        }

        let Err(error) = backend.resolve(&locator, &context()).await else {
            panic!("ACL-bearing intermediate ancestor must be rejected")
        };
        assert_acl_rejected(error, &intermediate);
    }

    #[cfg(target_os = "linux")]
    #[tokio::test]
    async fn linux_store_and_parent_acls_are_rejected_without_exposing_secrets() {
        for target_parent in [false, true] {
            let temp = tempdir().unwrap();
            let path = temp.path().join("store/secrets.json");
            let backend = LocalFileBackend::from_config(&config("dev-local", &path)).unwrap();
            let locator = SecretLocator::new("acl-protected").unwrap();
            backend
                .put(
                    &locator,
                    &SecretString::from(CANARY.to_owned()),
                    PutSecretOptions::NO_OVERWRITE,
                )
                .await
                .unwrap();
            let acl_path = if target_parent {
                path.parent().unwrap()
            } else {
                &path
            };
            if !add_linux_posix_acl(acl_path) {
                continue;
            }

            let Err(error) = backend.resolve(&locator, &context()).await else {
                panic!("ACL-protected store must be rejected")
            };
            assert_acl_rejected(error, acl_path);
        }
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
        let backend =
            LocalFileBackend::from_config(&config("dev-local", &ancestors[2].join("store.json")))
                .unwrap();
        backend.probe().await.unwrap();
        let initialized = backend.state.initialized().unwrap();
        validate_parent(&initialized.path, &backend.state.id).unwrap();

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
    fn ownership_predicate_rejects_a_foreign_uid() {
        assert!(owned_by_effective_uid(501, 501));
        assert!(!owned_by_effective_uid(502, 501));
    }

    #[test]
    fn ancestor_ownership_predicate_accepts_only_root_or_effective_uid() {
        assert!(ancestor_owner_is_trusted(0, 501));
        assert!(ancestor_owner_is_trusted(501, 501));
        assert!(!ancestor_owner_is_trusted(502, 501));
    }

    #[test]
    fn linux_acl_classification_distinguishes_ancestor_defaults_from_parent_defaults() {
        assert!(!linux_acl_is_untrusted(false, false, false));
        assert!(!linux_acl_is_untrusted(false, true, false));
        assert!(linux_acl_is_untrusted(true, false, false));
        assert!(linux_acl_is_untrusted(true, true, false));
        assert!(!linux_acl_is_untrusted(false, false, true));
        assert!(linux_acl_is_untrusted(false, true, true));
        assert!(linux_acl_is_untrusted(true, false, true));
        assert!(linux_acl_is_untrusted(true, true, true));
    }

    #[tokio::test]
    async fn opened_store_descriptor_matches_validated_path() {
        use std::os::unix::fs::{MetadataExt, PermissionsExt};

        let temp = tempdir().unwrap();
        let path = temp.path().join("store/secrets.json");
        let backend = LocalFileBackend::from_config(&config("dev-local", &path)).unwrap();
        backend.probe().await.unwrap();
        let path = backend.state.initialized().unwrap().path.clone();
        let path_metadata = fs::symlink_metadata(&path).unwrap();
        let file = open_validated_store_if_present(&path, &backend.state.id)
            .unwrap()
            .unwrap();
        let descriptor_metadata = file.metadata().unwrap();

        assert!(descriptor_metadata.is_file());
        assert_eq!(descriptor_metadata.permissions().mode() & 0o077, 0);
        assert_eq!(descriptor_metadata.uid(), effective_uid());
        assert_eq!(descriptor_metadata.dev(), path_metadata.dev());
        assert_eq!(descriptor_metadata.ino(), path_metadata.ino());
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
        worker_releases_process_lock(&initialized).await;
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
    async fn dropped_resolve_stops_waiting_for_flock_promptly() {
        use std::time::Duration;

        let temp = tempdir().unwrap();
        let path = temp.path().join("store/secrets.json");
        let backend = LocalFileBackend::from_config(&config("dev-local", &path)).unwrap();
        let locator = SecretLocator::new("contended-read").unwrap();
        backend
            .put(
                &locator,
                &SecretString::from(CANARY.to_owned()),
                PutSecretOptions::NO_OVERWRITE,
            )
            .await
            .unwrap();
        let initialized = backend.state.initialized().unwrap();
        let held = lock_exclusive(&lock_path(&path), &backend.state.id, None).unwrap();
        let access = context();
        let mut resolution = Box::pin(backend.resolve(&locator, &access));

        assert!(
            tokio::time::timeout(Duration::from_millis(50), &mut resolution)
                .await
                .is_err()
        );
        assert!(initialized.lock.try_lock().is_err());
        drop(resolution);
        worker_releases_process_lock(&initialized).await;
        drop(held);

        assert!(
            backend
                .resolve(&locator, &access)
                .await
                .unwrap()
                .expose(|value| value == CANARY)
        );
    }

    #[test]
    fn abandoned_process_mutex_wait_returns_without_acquiring_mutex() {
        let temp = tempdir().unwrap();
        let backend = Arc::new(
            LocalFileBackend::from_config(&config(
                "dev-local",
                &temp.path().join("store/secrets.json"),
            ))
            .unwrap(),
        );
        let initialized = backend.state.initialized().unwrap();
        let held = initialized.lock.lock().unwrap();
        let abandoned = Arc::new(AtomicBool::new(true));
        let state = Arc::clone(&backend.state);
        let worker_initialized = Arc::clone(&initialized);
        let worker_abandoned = Arc::clone(&abandoned);
        let (done_tx, done_rx) = std::sync::mpsc::channel();
        let worker = std::thread::spawn(move || {
            done_tx
                .send(
                    state.with_process_lock(&worker_initialized, Some(&worker_abandoned), || {
                        Ok::<_, SecretError>(())
                    }),
                )
                .unwrap();
        });

        assert!(matches!(
            done_rx
                .recv_timeout(std::time::Duration::from_millis(250))
                .unwrap(),
            Err(SecretError::Cancelled { .. })
        ));
        drop(held);
        worker.join().unwrap();
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
                AtomicWriteFile::commit,
                |_| Ok(())
            ),
            Err(WriteStoreError::BeforeCommit)
        ));
        assert_eq!(fs::read(path).unwrap(), previous);
    }

    #[test]
    fn commit_stage_failure_is_classified_as_uncertain_write() {
        let temp = tempdir().unwrap();
        let path = temp.path().join("store/secrets.json");
        let backend = LocalFileBackend::from_config(&config("dev-local", &path)).unwrap();
        let initialized = backend.state.initialized().unwrap();
        let store = LocalFileStore {
            format_version: FORMAT_VERSION,
            backend_id: backend.state.id.clone(),
            secrets: HashMap::from([("key".to_owned(), CANARY.to_owned())]),
        };

        assert!(matches!(
            write_store(
                &initialized.path,
                &store,
                &backend.state.id,
                || Ok(()),
                |_| Err(io::Error::other("commit-stage fault")),
                || Ok(())
            ),
            Err(SecretError::UncertainWrite { .. })
        ));
        assert!(!initialized.path.exists());
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
            AtomicWriteFile::commit,
            |_| Err(io::Error::other("post-commit fault")),
        )
        .unwrap_err();

        assert!(matches!(
            map_write_error(&backend.state.id, error),
            SecretError::UncertainWrite { .. }
        ));
        assert!(initialized.path.exists());
    }

    #[tokio::test]
    async fn post_commit_validation_failure_returns_uncertain_write_with_durable_value() {
        let temp = tempdir().unwrap();
        let path = temp.path().join("store/secrets.json");
        let backend = LocalFileBackend::from_config(&config("dev-local", &path)).unwrap();
        backend.probe().await.unwrap();
        let locator = SecretLocator::new("post-commit-validation").unwrap();
        backend
            .state
            .fail_next_post_commit_validation
            .store(true, Ordering::Release);

        assert!(matches!(
            backend
                .put(
                    &locator,
                    &SecretString::from(CANARY.to_owned()),
                    PutSecretOptions::NO_OVERWRITE,
                )
                .await,
            Err(SecretError::UncertainWrite { .. })
        ));
        assert_eq!(
            read_store(&path, &backend.state.id)
                .unwrap()
                .secrets
                .get(locator.as_str())
                .map(String::as_str),
            Some(CANARY)
        );
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
