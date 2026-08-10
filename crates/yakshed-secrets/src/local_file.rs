use std::{
    collections::HashMap,
    fs, io,
    io::Write,
    path::{Path, PathBuf},
    sync::Mutex,
};

use async_trait::async_trait;
use atomic_write_file::{AtomicWriteFile, OpenOptions};
use secrecy::{ExposeSecret, SecretString};

use crate::{
    DeleteSecretOutcome, LOCAL_FILE_BACKEND_KIND, PutSecretOptions, PutSecretOutcome,
    ResolvedSecret, ResolvedSecretSource, SecretAccessContext, SecretAdministrator, SecretBackend,
    SecretBackendConfigurationError, SecretBackendDescriptor, SecretBackendId, SecretBackendStatus,
    SecretError, SecretLocator, SecretReferenceSummary, SecretResolver,
    validate_backend_configuration,
};

/// Plaintext, explicitly configured secret storage for local development.
pub struct LocalFileBackend {
    id: SecretBackendId,
    path: PathBuf,
    io: Mutex<()>,
}

impl LocalFileBackend {
    pub fn from_config(config: &SecretBackend) -> Result<Self, SecretBackendConfigurationError> {
        if config.kind != LOCAL_FILE_BACKEND_KIND {
            return Err(SecretBackendConfigurationError::WrongKind {
                backend: config.id.clone(),
            });
        }
        validate_backend_configuration(config)?;
        Ok(Self {
            id: config.id.clone(),
            path: PathBuf::from(config.path.as_ref().expect("validated local-file path")),
            io: Mutex::new(()),
        })
    }

    fn summary(&self, locator: &SecretLocator) -> SecretReferenceSummary {
        SecretReferenceSummary {
            backend: self.id.clone(),
            locator: locator.clone(),
        }
    }

    fn load(&self) -> Result<HashMap<String, String>, SecretError> {
        read_store(&self.path).map_err(|_| self.failure("failed to read local secret store"))
    }

    fn save(&self, values: &HashMap<String, String>) -> Result<(), SecretError> {
        write_store_with_hook(&self.path, values, || Ok(()))
            .map_err(|_| self.failure("failed to write local secret store"))
    }

    fn failure(&self, message: &'static str) -> SecretError {
        SecretError::BackendFailure {
            backend: self.id.clone(),
            redacted_message: message.to_owned(),
        }
    }
}

#[async_trait]
impl SecretResolver for LocalFileBackend {
    fn descriptor(&self) -> SecretBackendDescriptor {
        SecretBackendDescriptor {
            id: self.id.clone(),
            kind: LOCAL_FILE_BACKEND_KIND.to_owned(),
        }
    }

    async fn probe(&self) -> Result<SecretBackendStatus, SecretError> {
        let _guard = self
            .io
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        self.load()?;
        Ok(SecretBackendStatus::Available)
    }

    async fn resolve(
        &self,
        locator: &SecretLocator,
        _context: &SecretAccessContext,
    ) -> Result<ResolvedSecret, SecretError> {
        let _guard = self
            .io
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let values = self.load()?;
        let value = values
            .get(locator.as_str())
            .ok_or_else(|| SecretError::NotFound {
                reference: self.summary(locator),
            })?;
        Ok(ResolvedSecret::new(
            SecretString::from(value.clone()),
            ResolvedSecretSource {
                backend: self.id.clone(),
            },
            None,
        ))
    }
}

#[async_trait]
impl SecretAdministrator for LocalFileBackend {
    fn backend_id(&self) -> SecretBackendId {
        self.id.clone()
    }

    async fn put(
        &self,
        locator: &SecretLocator,
        value: &SecretString,
        options: PutSecretOptions,
    ) -> Result<PutSecretOutcome, SecretError> {
        let _guard = self
            .io
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let mut values = self.load()?;
        let existed = values.contains_key(locator.as_str());
        if existed && !options.overwrite {
            return Err(SecretError::AlreadyExists {
                reference: self.summary(locator),
            });
        }
        values.insert(
            locator.as_str().to_owned(),
            value.expose_secret().to_owned(),
        );
        self.save(&values)?;
        Ok(if existed {
            PutSecretOutcome::Replaced
        } else {
            PutSecretOutcome::Written
        })
    }

    async fn delete(&self, locator: &SecretLocator) -> Result<DeleteSecretOutcome, SecretError> {
        let _guard = self
            .io
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let mut values = self.load()?;
        if values.remove(locator.as_str()).is_none() {
            return Ok(DeleteSecretOutcome::NotFound);
        }
        self.save(&values)?;
        Ok(DeleteSecretOutcome::Deleted)
    }
}

fn read_store(path: &Path) -> io::Result<HashMap<String, String>> {
    let bytes = match fs::read(path) {
        Ok(bytes) => bytes,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(HashMap::new()),
        Err(error) => return Err(error),
    };
    serde_json::from_slice(&bytes)
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "invalid local secret store"))
}

fn write_store_with_hook(
    path: &Path,
    values: &HashMap<String, String>,
    before_commit: impl FnOnce() -> io::Result<()>,
) -> io::Result<()> {
    create_private_parent(path)?;
    let mut file = private_atomic_file(path)?;
    serde_json::to_writer(&mut file, values)
        .map_err(|_| io::Error::other("failed to serialize local secret store"))?;
    file.write_all(b"\n")?;
    before_commit()?;
    file.commit()
}

fn create_private_parent(path: &Path) -> io::Result<()> {
    let parent = path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "store path has no parent"))?;
    let create_permissions = !parent.exists();
    fs::create_dir_all(parent)?;
    #[cfg(unix)]
    if create_permissions {
        use std::os::unix::fs::PermissionsExt;
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

#[cfg(not(unix))]
fn private_atomic_file(path: &Path) -> io::Result<AtomicWriteFile> {
    OpenOptions::new().open(path)
}

#[cfg(test)]
mod tests {
    use super::*;

    use tempfile::tempdir;
    use yakshed_domain::{ConnectionId, CredentialSlot, OperationId};

    const CANARY: &str = "local-file-canary-41c49f1a";

    fn config(path: &Path) -> SecretBackend {
        SecretBackend {
            id: SecretBackendId::new("dev-local").unwrap(),
            kind: LOCAL_FILE_BACKEND_KIND.to_owned(),
            account: None,
            path: Some(path.to_string_lossy().into_owned()),
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
        let backend =
            LocalFileBackend::from_config(&config(&temp.path().join("store/secrets.json")))
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
        let resolved = backend.resolve(&locator, &context()).await.unwrap();
        assert!(resolved.expose(|value| value == CANARY));
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
        let config = config(&temp.path().join("store/secrets.json"));
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

        let reopened = LocalFileBackend::from_config(&config).unwrap();
        assert!(
            reopened
                .resolve(&locator, &context())
                .await
                .unwrap()
                .expose(|value| value == CANARY)
        );
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn store_file_and_parent_have_private_permissions() {
        use std::os::unix::fs::PermissionsExt;

        let temp = tempdir().unwrap();
        let parent = temp.path().join("store");
        let path = parent.join("secrets.json");
        LocalFileBackend::from_config(&config(&path))
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
        let backend = LocalFileBackend::from_config(&config(&path)).unwrap();
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
        let mut replacement = HashMap::new();
        replacement.insert("key".to_owned(), "replacement-canary".to_owned());

        assert!(
            write_store_with_hook(&path, &replacement, || Err(io::Error::other("fault"))).is_err()
        );
        assert_eq!(fs::read(path).unwrap(), previous);
    }

    #[test]
    fn errors_never_expose_stored_values() {
        let temp = tempdir().unwrap();
        let path = temp.path().join("store/secrets.json");
        fs::create_dir_all(path.parent().unwrap()).unwrap();
        fs::write(&path, format!("not-json-{CANARY}")).unwrap();
        let backend = LocalFileBackend::from_config(&config(&path)).unwrap();
        let error = backend.load().unwrap_err();

        assert!(!format!("{error}").contains(CANARY));
        assert!(!format!("{error:?}").contains(CANARY));
    }
}
