//! Plaintext local-development secret storage.
//!
//! Backends targeting the same canonical path share a process-global lock. Cross-process locking
//! is intentionally out of scope because YakShed's store-layer single-instance lease owns it.

#[cfg(unix)]
use std::{
    collections::HashMap,
    fs::{self, File},
    io,
    io::Write,
    path::{Path, PathBuf},
    sync::{Arc, Mutex, OnceLock, Weak},
};

#[cfg(unix)]
use async_trait::async_trait;
#[cfg(unix)]
use atomic_write_file::{AtomicWriteFile, OpenOptions};
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
    path: PathBuf,
    lock: Arc<Mutex<()>>,
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
        validate_backend_configuration(config)?;
        #[cfg(not(unix))]
        let _ = path;

        #[cfg(unix)]
        {
            let path = canonical_store_path(Path::new(path)).map_err(|_| {
                SecretBackendConfigurationError::InvalidPath {
                    backend: config.id.clone(),
                }
            })?;
            let lock = shared_file_lock(&path);
            Ok(Self {
                state: Arc::new(LocalFileState {
                    id: config.id.clone(),
                    path,
                    lock,
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
}

#[cfg(unix)]
impl LocalFileState {
    fn summary(&self, locator: &SecretLocator) -> SecretReferenceSummary {
        SecretReferenceSummary {
            backend: self.id.clone(),
            locator: locator.clone(),
        }
    }

    fn load(&self) -> Result<LocalFileStore, SecretError> {
        read_store(&self.path, &self.id)
    }

    fn save(&self, store: &LocalFileStore) -> Result<(), SecretError> {
        write_store_with_hooks(&self.path, store, || Ok(()), sync_directory)
            .map_err(|error| map_write_error(&self.id, error))
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
            let _guard = state
                .lock
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            state.load()?;
            Ok(SecretBackendStatus::Available)
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
            let _guard = state
                .lock
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            let store = state.load()?;
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
        self.run_blocking(move |state| {
            let _guard = state
                .lock
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            let mut store = state.load()?;
            let existed = store.secrets.contains_key(locator.as_str());
            if existed && !options.overwrite {
                return Err(SecretError::AlreadyExists {
                    reference: state.summary(&locator),
                });
            }
            store.secrets.insert(locator.as_str().to_owned(), value);
            state.save(&store)?;
            Ok(if existed {
                PutSecretOutcome::Replaced
            } else {
                PutSecretOutcome::Written
            })
        })
        .await
    }

    async fn delete(&self, locator: &SecretLocator) -> Result<DeleteSecretOutcome, SecretError> {
        let locator = locator.clone();
        self.run_blocking(move |state| {
            let _guard = state
                .lock
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            let mut store = state.load()?;
            if store.secrets.remove(locator.as_str()).is_none() {
                return Ok(DeleteSecretOutcome::NotFound);
            }
            state.save(&store)?;
            Ok(DeleteSecretOutcome::Deleted)
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
    create_private_parent(path)?;
    if path.exists() {
        return fs::canonicalize(path);
    }
    let file_name = path.file_name().ok_or_else(|| {
        io::Error::new(io::ErrorKind::InvalidInput, "store path has no file name")
    })?;
    Ok(
        fs::canonicalize(path.parent().expect("validated store path has a parent"))?
            .join(file_name),
    )
}

#[cfg(unix)]
fn read_store(path: &Path, backend: &SecretBackendId) -> Result<LocalFileStore, SecretError> {
    let bytes = match fs::read(path) {
        Ok(bytes) => bytes,
        Err(error) if error.kind() == io::ErrorKind::NotFound => {
            return Ok(LocalFileStore {
                format_version: FORMAT_VERSION,
                backend_id: backend.clone(),
                secrets: HashMap::new(),
            });
        }
        Err(_) => {
            return Err(backend_failure(
                backend,
                "failed to read local secret store",
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
fn create_private_parent(path: &Path) -> io::Result<()> {
    use std::os::unix::fs::PermissionsExt;

    let parent = path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "store path has no parent"))?;
    let set_permissions = !parent.exists();
    fs::create_dir_all(parent)?;
    if set_permissions {
        fs::set_permissions(parent, fs::Permissions::from_mode(0o700))?;
    }
    Ok(())
}

#[cfg(unix)]
fn private_atomic_file(path: &Path) -> io::Result<AtomicWriteFile> {
    use atomic_write_file::unix::OpenOptionsExt;
    use std::os::unix::fs::OpenOptionsExt as _;

    let mut options = OpenOptions::new();
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
    async fn separate_instances_share_path_lock_for_rmw_and_no_overwrite() {
        let temp = tempdir().unwrap();
        let path = temp.path().join("store/secrets.json");
        let settings = config("dev-local", &path);
        let first = Arc::new(LocalFileBackend::from_config(&settings).unwrap());
        let aliased = config("dev-local", &temp.path().join("store/./secrets.json"));
        let second = Arc::new(LocalFileBackend::from_config(&aliased).unwrap());
        assert_eq!(first.state.path, second.state.path);
        assert!(Arc::ptr_eq(&first.state.lock, &second.state.lock));
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
    async fn mismatched_backend_id_file_is_rejected() {
        let temp = tempdir().unwrap();
        let path = temp.path().join("store/secrets.json");
        let locator = SecretLocator::new("key").unwrap();
        LocalFileBackend::from_config(&config("backend-a", &path))
            .unwrap()
            .put(
                &locator,
                &SecretString::from(CANARY.to_owned()),
                PutSecretOptions::NO_OVERWRITE,
            )
            .await
            .unwrap();

        assert!(matches!(
            LocalFileBackend::from_config(&config("backend-b", &path))
                .unwrap()
                .probe()
                .await,
            Err(SecretError::BackendIdentityMismatch { .. })
        ));
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
        let store = LocalFileStore {
            format_version: FORMAT_VERSION,
            backend_id: backend.state.id.clone(),
            secrets: HashMap::new(),
        };
        let error = write_store_with_hooks(
            &backend.state.path,
            &store,
            || Ok(()),
            |_| Err(io::Error::other("post-commit fault")),
        )
        .unwrap_err();

        assert!(matches!(
            map_write_error(&backend.state.id, error),
            SecretError::UncertainWrite { .. }
        ));
        assert!(backend.state.path.exists());
    }

    #[test]
    fn errors_never_expose_stored_values() {
        let temp = tempdir().unwrap();
        let path = temp.path().join("store/secrets.json");
        let backend = LocalFileBackend::from_config(&config("dev-local", &path)).unwrap();
        fs::write(&backend.state.path, format!("not-json-{CANARY}")).unwrap();
        let Err(error) = backend.state.load() else {
            panic!("invalid JSON must fail closed")
        };

        assert!(!format!("{error}").contains(CANARY));
        assert!(!format!("{error:?}").contains(CANARY));
    }
}
