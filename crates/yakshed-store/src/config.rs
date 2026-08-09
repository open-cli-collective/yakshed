use std::{
    collections::HashSet,
    fs, io,
    io::Write,
    path::{Path, PathBuf},
    sync::RwLock,
};

use atomic_write_file::{AtomicWriteFile, OpenOptions};
use serde::{Deserialize, Serialize};
use thiserror::Error;
use yakshed_domain::ConnectionId;

use crate::{AppPaths, PathError};

const CONFIG_FILE: &str = "config.toml";
const SCHEMA_VERSION: u32 = 1;
type Migration = fn(toml::Value) -> toml::Value;
const MIGRATIONS: &[(u32, Migration)] = &[(1, migrate_v1)];

/// Complete non-secret contents of `config.toml`.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AppConfig {
    pub schema_version: u32,
    #[serde(default)]
    pub connections: Vec<ConnectionConfig>,
    #[serde(default)]
    pub secret_backends: Vec<SecretBackendConfig>,
    #[serde(default)]
    pub ui: UiConfig,
}

impl Default for AppConfig {
    fn default() -> Self {
        Self {
            schema_version: SCHEMA_VERSION,
            connections: Vec::new(),
            secret_backends: Vec::new(),
            ui: UiConfig::default(),
        }
    }
}

/// Non-secret connection definition.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ConnectionConfig {
    pub id: ConnectionId,
    pub name: String,
    pub harness: String,
    pub model_provider: String,
    pub provider_state: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub credentials: Vec<CredentialBindingConfig>,
}

/// A reference describing where and how a credential may be delivered.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CredentialBindingConfig {
    pub slot: String,
    pub source: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub backend: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub locator: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub delivery: Option<CredentialDelivery>,
}

/// Non-secret delivery metadata for a resolved credential.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CredentialDelivery {
    pub kind: String,
    pub variable: String,
}

/// Definition of an external secret backend; never a secret value.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SecretBackendConfig {
    pub id: String,
    pub kind: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub account: Option<String>,
}

/// User-interface preferences stored in config.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct UiConfig {
    pub theme: String,
}

impl Default for UiConfig {
    fn default() -> Self {
        Self {
            theme: "system".to_owned(),
        }
    }
}

/// Monotonic in-process config revision used for optimistic concurrency.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ConfigRevision(u64);

impl ConfigRevision {
    pub const INITIAL: Self = Self(0);

    pub const fn new(value: u64) -> Self {
        Self(value)
    }

    pub fn get(self) -> u64 {
        self.0
    }
}

/// Config plus the revision at which it was observed.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ConfigSnapshot {
    pub revision: ConfigRevision,
    pub config: AppConfig,
}

/// Validated config mutations available to application callers.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ConfigChange {
    PutConnection(ConnectionConfig),
    RemoveConnection(ConnectionId),
    PutSecretBackend(SecretBackendConfig),
    RemoveSecretBackend(String),
    SetUiTheme(String),
    Reset,
}

/// Thread-safe service owning `config.toml` and its revision.
pub struct ConfigStore {
    config_path: PathBuf,
    state: RwLock<ConfigSnapshot>,
    #[cfg(test)]
    fail_next_write: std::sync::atomic::AtomicBool,
}

impl ConfigStore {
    /// Opens or creates the schema-v1 config beneath the injected paths.
    pub fn open(paths: AppPaths) -> Result<Self, ConfigError> {
        paths.create_dirs()?;
        let config_path = paths.config_root.join(CONFIG_FILE);
        let config = if config_path.exists() {
            read_config(&config_path)?
        } else {
            let config = AppConfig::default();
            write_config(&config_path, &config, false)?;
            config
        };

        Ok(Self {
            config_path,
            state: RwLock::new(ConfigSnapshot {
                revision: ConfigRevision::INITIAL,
                config,
            }),
            #[cfg(test)]
            fail_next_write: std::sync::atomic::AtomicBool::new(false),
        })
    }

    /// Returns a consistent point-in-time copy of configuration state.
    pub fn snapshot(&self) -> ConfigSnapshot {
        self.state
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
        let mut state = self
            .state
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if state.revision != expected_revision {
            return Err(ConfigError::Conflict {
                expected: expected_revision,
                actual: state.revision,
            });
        }

        let mut config = state.config.clone();
        apply_change(&mut config, change);
        validate(&config)?;
        #[cfg(test)]
        let fail_before_commit = self
            .fail_next_write
            .swap(false, std::sync::atomic::Ordering::SeqCst);
        #[cfg(not(test))]
        let fail_before_commit = false;
        write_config(&self.config_path, &config, fail_before_commit)?;

        let snapshot = ConfigSnapshot {
            revision: ConfigRevision(state.revision.0 + 1),
            config,
        };
        *state = snapshot.clone();
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
    let config = value.try_into::<AppConfig>().map_err(ConfigError::Parse)?;
    validate(&config)?;
    Ok(config)
}

fn migrate(value: toml::Value) -> Result<toml::Value, ConfigError> {
    let Some(version) = value
        .get("schema_version")
        .and_then(toml::Value::as_integer)
    else {
        return Err(ConfigError::Validation(
            "schema_version must be a positive integer".to_owned(),
        ));
    };
    let version = u32::try_from(version).map_err(|_| {
        ConfigError::Validation("schema_version must be a positive integer".to_owned())
    })?;
    if version > SCHEMA_VERSION {
        return Err(ConfigError::UnsupportedSchema {
            found: version,
            supported: SCHEMA_VERSION,
        });
    }
    MIGRATIONS
        .iter()
        .find(|(schema, _)| *schema == version)
        .map(|(_, migrate)| migrate(value))
        .ok_or_else(|| {
            ConfigError::Validation(format!("schema version {version} has no migration path"))
        })
}

fn migrate_v1(value: toml::Value) -> toml::Value {
    value
}

fn validate(config: &AppConfig) -> Result<(), ConfigError> {
    if config.schema_version != SCHEMA_VERSION {
        return Err(ConfigError::Validation(format!(
            "schema_version must be {SCHEMA_VERSION}"
        )));
    }
    require_nonempty("ui.theme", &config.ui.theme)?;

    let mut backend_ids = HashSet::new();
    for backend in &config.secret_backends {
        require_nonempty("secret backend id", &backend.id)?;
        require_nonempty("secret backend kind", &backend.kind)?;
        if !backend_ids.insert(backend.id.as_str()) {
            return Err(ConfigError::Validation(format!(
                "duplicate secret backend id: {}",
                backend.id
            )));
        }
    }

    let mut connection_ids = HashSet::new();
    for connection in &config.connections {
        if !connection_ids.insert(connection.id) {
            return Err(ConfigError::Validation(format!(
                "duplicate connection id: {}",
                connection.id
            )));
        }
        require_nonempty("connection name", &connection.name)?;
        require_nonempty("connection harness", &connection.harness)?;
        require_nonempty("connection model_provider", &connection.model_provider)?;
        require_nonempty("connection provider_state", &connection.provider_state)?;
        for credential in &connection.credentials {
            validate_credential(credential, &backend_ids)?;
        }
    }
    Ok(())
}

fn validate_credential(
    credential: &CredentialBindingConfig,
    backend_ids: &HashSet<&str>,
) -> Result<(), ConfigError> {
    require_nonempty("credential slot", &credential.slot)?;
    match credential.source.as_str() {
        "secret" => {
            let backend = required("secret credential backend", credential.backend.as_deref())?;
            required("secret credential locator", credential.locator.as_deref())?;
            if !backend_ids.contains(backend) {
                return Err(ConfigError::Validation(format!(
                    "credential references unknown secret backend: {backend}"
                )));
            }
        }
        "delegated" | "disabled" => {
            if credential.backend.is_some() || credential.locator.is_some() {
                return Err(ConfigError::Validation(format!(
                    "{} credential cannot contain backend or locator",
                    credential.source
                )));
            }
        }
        other => {
            return Err(ConfigError::Validation(format!(
                "unsupported credential source: {other}"
            )));
        }
    }
    if let Some(delivery) = &credential.delivery {
        require_nonempty("credential delivery kind", &delivery.kind)?;
        require_nonempty("credential delivery variable", &delivery.variable)?;
    }
    Ok(())
}

fn require_nonempty(field: &str, value: &str) -> Result<(), ConfigError> {
    if value.trim().is_empty() {
        Err(ConfigError::Validation(format!("{field} cannot be empty")))
    } else {
        Ok(())
    }
}

fn required<'a>(field: &str, value: Option<&'a str>) -> Result<&'a str, ConfigError> {
    let value = value.ok_or_else(|| ConfigError::Validation(format!("{field} is required")))?;
    require_nonempty(field, value)?;
    Ok(value)
}

fn write_config(
    path: &Path,
    config: &AppConfig,
    fail_before_commit: bool,
) -> Result<(), ConfigError> {
    let serialized = toml::to_string_pretty(config).map_err(ConfigError::Serialize)?;
    let mut file = private_atomic_file(path).map_err(|source| ConfigError::Io {
        path: path.to_owned(),
        source,
    })?;
    file.write_all(serialized.as_bytes())
        .map_err(|source| ConfigError::Io {
            path: path.to_owned(),
            source,
        })?;
    if fail_before_commit {
        return Err(ConfigError::Io {
            path: path.to_owned(),
            source: io::Error::other("injected failure before atomic commit"),
        });
    }
    file.commit().map_err(|source| ConfigError::Io {
        path: path.to_owned(),
        source,
    })?;
    set_private_file_permissions(path)?;
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

#[cfg(unix)]
fn set_private_file_permissions(path: &Path) -> Result<(), ConfigError> {
    use std::os::unix::fs::PermissionsExt;

    fs::set_permissions(path, fs::Permissions::from_mode(0o600)).map_err(|source| ConfigError::Io {
        path: path.to_owned(),
        source,
    })
}

#[cfg(not(unix))]
fn set_private_file_permissions(_path: &Path) -> Result<(), ConfigError> {
    Ok(())
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
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn schema_v1_migration_is_identity() {
        let value = toml::from_str::<toml::Value>("schema_version = 1\n").unwrap();
        assert_eq!(migrate(value.clone()).unwrap(), value);
    }

    #[tokio::test]
    async fn failed_atomic_write_preserves_previous_file_and_revision() {
        let temp = tempdir().unwrap();
        let paths = AppPaths::for_test(temp.path());
        let store = ConfigStore::open(paths.clone()).unwrap();
        let before = fs::read(paths.config_root.join(CONFIG_FILE)).unwrap();
        store
            .fail_next_write
            .store(true, std::sync::atomic::Ordering::SeqCst);

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
}
