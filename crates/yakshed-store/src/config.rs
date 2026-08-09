use std::{
    fs, io,
    io::Write,
    path::{Path, PathBuf},
    sync::{Arc, Mutex, RwLock},
};

use atomic_write_file::{AtomicWriteFile, OpenOptions};
use serde::{Deserialize, Serialize};
use thiserror::Error;
use yakshed_application::{AppConfig, ConfigChange, ConfigRevision, ConfigSnapshot, UiConfig};
use yakshed_domain::{
    Connection, ConnectionId, CredentialBinding, CredentialBindingRecord, CredentialSlot,
    ProviderStateRootId, SecretBackend, SecretBackendId, SecretLocator, SecretReference,
    ValidationError,
};

use crate::{AppPaths, PathError};

const CONFIG_FILE: &str = "config.toml";
const SCHEMA_VERSION: u32 = 1;
const MIGRATIONS: &[Migration] = &[];

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ConfigDto {
    schema_version: u32,
    #[serde(default)]
    connections: Vec<ConnectionDto>,
    #[serde(default)]
    secret_backends: Vec<SecretBackendDto>,
    #[serde(default)]
    ui: UiDto,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ConnectionDto {
    id: ConnectionId,
    name: String,
    harness: String,
    model_provider: String,
    provider_state: ProviderStateRootId,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    credentials: Vec<CredentialBindingDto>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(tag = "source", rename_all = "snake_case", deny_unknown_fields)]
enum CredentialBindingDto {
    Delegated {
        slot: CredentialSlot,
        authority: String,
    },
    Secret {
        slot: CredentialSlot,
        backend: String,
        locator: String,
    },
    Disabled {
        slot: CredentialSlot,
    },
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct SecretBackendDto {
    id: String,
    kind: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    account: Option<String>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct UiDto {
    theme: String,
}

impl Default for UiDto {
    fn default() -> Self {
        Self {
            theme: "system".to_owned(),
        }
    }
}

impl TryFrom<ConfigDto> for AppConfig {
    type Error = ValidationError;

    fn try_from(config: ConfigDto) -> Result<Self, Self::Error> {
        Ok(Self {
            connections: config
                .connections
                .into_iter()
                .map(TryInto::try_into)
                .collect::<Result<_, _>>()?,
            secret_backends: config
                .secret_backends
                .into_iter()
                .map(TryInto::try_into)
                .collect::<Result<_, _>>()?,
            ui: UiConfig {
                theme: config.ui.theme,
            },
        })
    }
}

impl From<&AppConfig> for ConfigDto {
    fn from(config: &AppConfig) -> Self {
        Self {
            schema_version: SCHEMA_VERSION,
            connections: config.connections.iter().map(Into::into).collect(),
            secret_backends: config.secret_backends.iter().map(Into::into).collect(),
            ui: UiDto {
                theme: config.ui.theme.clone(),
            },
        }
    }
}

impl TryFrom<ConnectionDto> for Connection {
    type Error = ValidationError;

    fn try_from(connection: ConnectionDto) -> Result<Self, Self::Error> {
        Ok(Self {
            id: connection.id,
            name: connection.name,
            harness: connection.harness,
            model_provider: connection.model_provider,
            provider_state: connection.provider_state,
            credentials: connection
                .credentials
                .into_iter()
                .map(TryInto::try_into)
                .collect::<Result<_, _>>()?,
        })
    }
}

impl From<&Connection> for ConnectionDto {
    fn from(connection: &Connection) -> Self {
        Self {
            id: connection.id,
            name: connection.name.clone(),
            harness: connection.harness.clone(),
            model_provider: connection.model_provider.clone(),
            provider_state: connection.provider_state.clone(),
            credentials: connection.credentials.iter().map(Into::into).collect(),
        }
    }
}

impl TryFrom<CredentialBindingDto> for CredentialBindingRecord {
    type Error = ValidationError;

    fn try_from(binding: CredentialBindingDto) -> Result<Self, Self::Error> {
        Ok(match binding {
            CredentialBindingDto::Delegated { slot, authority } => Self {
                slot,
                binding: CredentialBinding::Delegated { authority },
            },
            CredentialBindingDto::Secret {
                slot,
                backend,
                locator,
            } => Self {
                slot,
                binding: CredentialBinding::Secret {
                    reference: SecretReference {
                        backend_id: SecretBackendId::new(backend)?,
                        locator: SecretLocator::new(locator)?,
                    },
                },
            },
            CredentialBindingDto::Disabled { slot } => Self {
                slot,
                binding: CredentialBinding::Disabled,
            },
        })
    }
}

impl From<&CredentialBindingRecord> for CredentialBindingDto {
    fn from(record: &CredentialBindingRecord) -> Self {
        match &record.binding {
            CredentialBinding::Delegated { authority } => Self::Delegated {
                slot: record.slot.clone(),
                authority: authority.clone(),
            },
            CredentialBinding::Secret { reference } => Self::Secret {
                slot: record.slot.clone(),
                backend: reference.backend_id.as_str().to_owned(),
                locator: reference.locator.as_str().to_owned(),
            },
            CredentialBinding::Disabled => Self::Disabled {
                slot: record.slot.clone(),
            },
        }
    }
}

impl TryFrom<SecretBackendDto> for SecretBackend {
    type Error = ValidationError;

    fn try_from(backend: SecretBackendDto) -> Result<Self, Self::Error> {
        Ok(Self {
            id: SecretBackendId::new(backend.id)?,
            kind: backend.kind,
            account: backend.account,
        })
    }
}

impl From<&SecretBackend> for SecretBackendDto {
    fn from(backend: &SecretBackend) -> Self {
        Self {
            id: backend.id.as_str().to_owned(),
            kind: backend.kind.clone(),
            account: backend.account.clone(),
        }
    }
}

/// Thread-safe service owning `config.toml` and its revision.
pub struct ConfigStore {
    inner: Arc<StoreInner>,
}

struct StoreInner {
    config_path: PathBuf,
    state: RwLock<ConfigSnapshot>,
    updates: Mutex<()>,
    #[cfg(test)]
    next_write_fault: Mutex<Option<WriteFault>>,
}

impl ConfigStore {
    /// Opens or creates the schema-v1 config beneath the injected config root.
    pub fn open(paths: AppPaths) -> Result<Self, ConfigError> {
        paths.create_config_root()?;
        let config_path = paths.config_root.join(CONFIG_FILE);
        let config = if config_path.exists() {
            read_config(&config_path)?
        } else {
            let config = AppConfig::default();
            write_config(&config_path, &config)?;
            config
        };

        Ok(Self {
            inner: Arc::new(StoreInner {
                config_path,
                state: RwLock::new(ConfigSnapshot {
                    revision: ConfigRevision::INITIAL,
                    config,
                }),
                updates: Mutex::new(()),
                #[cfg(test)]
                next_write_fault: Mutex::new(None),
            }),
        })
    }

    /// Returns a consistent point-in-time copy of configuration state.
    pub fn snapshot(&self) -> ConfigSnapshot {
        self.inner
            .state
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
    }

    /// Applies one validated change if the caller's revision is current.
    pub async fn update(
        &self,
        expected_revision: ConfigRevision,
        change: ConfigChange,
    ) -> Result<ConfigSnapshot, ConfigError> {
        let inner = Arc::clone(&self.inner);
        tokio::task::spawn_blocking(move || inner.update(expected_revision, change))
            .await
            .map_err(|error| ConfigError::Worker(error.to_string()))?
    }
}

impl StoreInner {
    fn update(
        &self,
        expected_revision: ConfigRevision,
        change: ConfigChange,
    ) -> Result<ConfigSnapshot, ConfigError> {
        let _update = self
            .updates
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let current = self
            .state
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone();
        if current.revision != expected_revision {
            return Err(ConfigError::Conflict {
                expected: expected_revision,
                actual: current.revision,
            });
        }

        let mut config = current.config;
        apply_change(&mut config, change);
        config
            .validate()
            .map_err(|error| ConfigError::Validation(error.to_string()))?;
        let revision = current
            .revision
            .get()
            .checked_add(1)
            .ok_or_else(|| ConfigError::Validation("config revision overflow".to_owned()))?;

        #[cfg(test)]
        {
            let fault = self
                .next_write_fault
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .take();
            write_config_with_hook(&self.config_path, &config, || apply_fault(fault))?;
        }
        #[cfg(not(test))]
        write_config(&self.config_path, &config)?;

        let snapshot = ConfigSnapshot {
            revision: ConfigRevision::new(revision),
            config,
        };
        *self
            .state
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = snapshot.clone();
        Ok(snapshot)
    }
}

fn apply_change(config: &mut AppConfig, change: ConfigChange) {
    match change {
        ConfigChange::PutConnection(connection) => {
            config.connections.retain(|item| item.id != connection.id);
            config.connections.push(connection);
            config.connections.sort_by_key(|item| item.id);
        }
        ConfigChange::RemoveConnection(id) => config.connections.retain(|item| item.id != id),
        ConfigChange::PutSecretBackend(backend) => {
            config.secret_backends.retain(|item| item.id != backend.id);
            config.secret_backends.push(backend);
            config
                .secret_backends
                .sort_by(|left, right| left.id.cmp(&right.id));
        }
        ConfigChange::RemoveSecretBackend(id) => {
            config.secret_backends.retain(|item| item.id != id);
        }
        ConfigChange::SetUiTheme(theme) => config.ui.theme = theme,
        ConfigChange::Reset => *config = AppConfig::default(),
    }
}

fn read_config(path: &Path) -> Result<AppConfig, ConfigError> {
    let source = fs::read_to_string(path).map_err(|source| ConfigError::Io {
        path: path.to_owned(),
        source,
    })?;
    let value = toml::from_str::<toml::Value>(&source).map_err(ConfigError::Parse)?;
    let value = migrate(value)?;
    let config = AppConfig::try_from(value.try_into::<ConfigDto>().map_err(ConfigError::Parse)?)
        .map_err(|error| ConfigError::Validation(error.to_string()))?;
    config
        .validate()
        .map_err(|error| ConfigError::Validation(error.to_string()))?;
    Ok(config)
}

#[derive(Clone, Copy)]
struct Migration {
    from: u32,
    apply: fn(toml::Value) -> Result<toml::Value, ConfigError>,
}

fn migrate(value: toml::Value) -> Result<toml::Value, ConfigError> {
    let version = schema_version(&value)?;
    if version > SCHEMA_VERSION {
        return Err(ConfigError::UnsupportedSchema {
            found: version,
            supported: SCHEMA_VERSION,
        });
    }
    run_migrations(value, version, SCHEMA_VERSION, MIGRATIONS)
}

fn run_migrations(
    mut value: toml::Value,
    mut version: u32,
    target: u32,
    migrations: &[Migration],
) -> Result<toml::Value, ConfigError> {
    for migration in migrations {
        if version == target {
            break;
        }
        if migration.from < version {
            continue;
        }
        if migration.from != version {
            break;
        }
        value = (migration.apply)(value)?;
        version = schema_version(&value)?;
        if version != migration.from + 1 {
            return Err(ConfigError::Validation(format!(
                "migration from schema {} did not produce schema {}",
                migration.from,
                migration.from + 1
            )));
        }
    }
    if version == target {
        Ok(value)
    } else {
        Err(ConfigError::Validation(format!(
            "schema version {version} has no migration path to {target}"
        )))
    }
}

fn schema_version(value: &toml::Value) -> Result<u32, ConfigError> {
    let Some(version) = value
        .get("schema_version")
        .and_then(toml::Value::as_integer)
    else {
        return Err(ConfigError::Validation(
            "schema_version must be a positive integer".to_owned(),
        ));
    };
    u32::try_from(version).map_err(|_| {
        ConfigError::Validation("schema_version must be a positive integer".to_owned())
    })
}

fn write_config(path: &Path, config: &AppConfig) -> Result<(), ConfigError> {
    write_config_with_hook(path, config, || Ok(()))
}

fn write_config_with_hook(
    path: &Path,
    config: &AppConfig,
    before_commit: impl FnOnce() -> io::Result<()>,
) -> Result<(), ConfigError> {
    let serialized =
        toml::to_string_pretty(&ConfigDto::from(config)).map_err(ConfigError::Serialize)?;
    let mut file = private_atomic_file(path).map_err(|source| ConfigError::Io {
        path: path.to_owned(),
        source,
    })?;
    file.write_all(serialized.as_bytes())
        .map_err(|source| ConfigError::Io {
            path: path.to_owned(),
            source,
        })?;
    before_commit().map_err(|source| ConfigError::Io {
        path: path.to_owned(),
        source,
    })?;
    file.commit().map_err(|source| ConfigError::Io {
        path: path.to_owned(),
        source,
    })
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

/// Actionable config load/update failures.
#[derive(Debug, Error)]
pub enum ConfigError {
    #[error("failed to parse config: {0}")]
    Parse(#[source] toml::de::Error),
    #[error("config schema {found} is newer than supported schema {supported}")]
    UnsupportedSchema { found: u32, supported: u32 },
    #[error(
        "config revision conflict: expected {}, current {}",
        expected.get(),
        actual.get()
    )]
    Conflict {
        expected: ConfigRevision,
        actual: ConfigRevision,
    },
    #[error("invalid config: {0}")]
    Validation(String),
    #[error("failed to serialize config: {0}")]
    Serialize(#[source] toml::ser::Error),
    #[error("config worker failed: {0}")]
    Worker(String),
    #[error("filesystem operation failed for {path}: {source}")]
    Io {
        path: PathBuf,
        #[source]
        source: io::Error,
    },
}

impl From<PathError> for ConfigError {
    fn from(error: PathError) -> Self {
        match error {
            PathError::Io { path, source } => Self::Io { path, source },
            PathError::PlatformDirectoriesUnavailable => Self::Validation(error.to_string()),
        }
    }
}

#[cfg(test)]
enum WriteFault {
    Permission,
    Slow {
        started: Arc<std::sync::atomic::AtomicBool>,
        finished: Arc<std::sync::atomic::AtomicBool>,
        delay: std::time::Duration,
    },
}

#[cfg(test)]
fn apply_fault(fault: Option<WriteFault>) -> io::Result<()> {
    match fault {
        Some(WriteFault::Permission) => Err(io::Error::new(
            io::ErrorKind::PermissionDenied,
            "injected temp-file permission failure",
        )),
        Some(WriteFault::Slow {
            started,
            finished,
            delay,
        }) => {
            started.store(true, std::sync::atomic::Ordering::SeqCst);
            std::thread::sleep(delay);
            finished.store(true, std::sync::atomic::Ordering::SeqCst);
            Ok(())
        }
        None => Ok(()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicBool, Ordering};
    use tempfile::tempdir;

    fn set_version(mut value: toml::Value, version: i64) -> toml::Value {
        value
            .as_table_mut()
            .unwrap()
            .insert("schema_version".to_owned(), toml::Value::Integer(version));
        value
    }

    fn v0_to_v1(value: toml::Value) -> Result<toml::Value, ConfigError> {
        let mut value = set_version(value, 1);
        value.as_table_mut().unwrap().insert(
            "migration_trace".to_owned(),
            toml::Value::String("v0-v1".to_owned()),
        );
        Ok(value)
    }

    fn v1_to_v2(value: toml::Value) -> Result<toml::Value, ConfigError> {
        if value.get("migration_trace").and_then(toml::Value::as_str) != Some("v0-v1") {
            return Err(ConfigError::Validation(
                "v1 migration ran before v0 migration".to_owned(),
            ));
        }
        let mut value = set_version(value, 2);
        value.as_table_mut().unwrap().insert(
            "migration_trace".to_owned(),
            toml::Value::String("v0-v1-v2".to_owned()),
        );
        Ok(value)
    }

    #[test]
    fn migration_pipeline_applies_multiple_steps_in_order() {
        let value = toml::from_str::<toml::Value>("schema_version = 0\n").unwrap();
        let migrated = run_migrations(
            value,
            0,
            2,
            &[
                Migration {
                    from: 0,
                    apply: v0_to_v1,
                },
                Migration {
                    from: 1,
                    apply: v1_to_v2,
                },
            ],
        )
        .unwrap();

        assert_eq!(schema_version(&migrated).unwrap(), 2);
        assert_eq!(
            migrated
                .get("migration_trace")
                .and_then(toml::Value::as_str),
            Some("v0-v1-v2")
        );
    }

    #[test]
    fn credential_dto_has_only_closed_reference_shapes() {
        let delegated = CredentialBindingDto::Delegated {
            slot: CredentialSlot::new("codex.account").unwrap(),
            authority: "codex-app-server".to_owned(),
        };
        let CredentialBindingDto::Delegated {
            slot: _,
            authority: _,
        } = delegated
        else {
            panic!("delegated binding expected")
        };

        let secret = CredentialBindingDto::Secret {
            slot: CredentialSlot::new("anthropic.api_key").unwrap(),
            backend: "memory".to_owned(),
            locator: "connection/work/anthropic_api_key".to_owned(),
        };
        let CredentialBindingDto::Secret {
            slot: _,
            backend: _,
            locator: _,
        } = secret
        else {
            panic!("secret binding expected")
        };

        let disabled = CredentialBindingDto::Disabled {
            slot: CredentialSlot::new("unused").unwrap(),
        };
        let CredentialBindingDto::Disabled { slot: _ } = disabled else {
            panic!("disabled binding expected")
        };
    }

    #[tokio::test]
    async fn temp_permission_failure_preserves_previous_file_and_revision() {
        let temp = tempdir().unwrap();
        let paths = AppPaths::for_test(temp.path());
        let store = ConfigStore::open(paths.clone()).unwrap();
        let before = fs::read(paths.config_root.join(CONFIG_FILE)).unwrap();
        *store
            .inner
            .next_write_fault
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(WriteFault::Permission);

        assert!(matches!(
            store
                .update(
                    ConfigRevision::INITIAL,
                    ConfigChange::SetUiTheme("dark".into())
                )
                .await,
            Err(ConfigError::Io { .. })
        ));
        assert_eq!(
            fs::read(paths.config_root.join(CONFIG_FILE)).unwrap(),
            before
        );
        assert_eq!(store.snapshot().revision, ConfigRevision::INITIAL);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn delayed_write_does_not_block_unrelated_runtime_work() {
        let temp = tempdir().unwrap();
        let store = ConfigStore::open(AppPaths::for_test(temp.path())).unwrap();
        let started = Arc::new(AtomicBool::new(false));
        let finished = Arc::new(AtomicBool::new(false));
        *store
            .inner
            .next_write_fault
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(WriteFault::Slow {
            started: Arc::clone(&started),
            finished: Arc::clone(&finished),
            delay: std::time::Duration::from_millis(100),
        });

        let update = store.update(
            ConfigRevision::INITIAL,
            ConfigChange::SetUiTheme("dark".into()),
        );
        let unrelated = async {
            while !started.load(Ordering::SeqCst) {
                tokio::task::yield_now().await;
            }
            assert!(!finished.load(Ordering::SeqCst));
        };
        let (result, ()) = tokio::join!(update, unrelated);

        result.unwrap();
    }
}
