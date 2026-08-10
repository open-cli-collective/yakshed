//! Application use cases, orchestration, snapshots, revisions, and application-owned ports, independent of Tauri commands and provider wire protocols.

use std::{collections::HashSet, error::Error, fmt};

use yakshed_domain::{
    Connection, ConnectionId, CredentialBinding, SecretBackend, SecretBackendId,
    SecretBackendSettings,
};

pub const LOCAL_FILE_BACKEND_KIND: &str = "local-file";
pub const DEV_SECRETS_FEATURE: &str = "dev-secrets";

/// Canonical non-secret application configuration.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct AppConfig {
    pub connections: Vec<Connection>,
    pub secret_backends: Vec<SecretBackend>,
    pub ui: UiConfig,
}

impl AppConfig {
    pub fn validate(&self) -> Result<(), ConfigValidationError> {
        if self.ui.theme.trim().is_empty() {
            return Err(ConfigValidationError::invalid("ui.theme cannot be empty"));
        }

        let mut backend_ids = HashSet::new();
        let mut local_file_paths = HashSet::new();
        for backend in &self.secret_backends {
            backend
                .validate()
                .map_err(|error| ConfigValidationError::invalid(error.to_string()))?;
            validate_backend_configuration(backend)?;
            if let SecretBackendSettings::LocalFile { path } = &backend.settings
                && !local_file_paths.insert(path)
            {
                return Err(SecretBackendConfigurationError::DuplicateLocalFilePath {
                    backend: backend.id.clone(),
                }
                .into());
            }
            if !backend_ids.insert(&backend.id) {
                return Err(ConfigValidationError::invalid(format!(
                    "duplicate secret backend id: {}",
                    backend.id
                )));
            }
        }

        let mut connection_ids = HashSet::new();
        let mut provider_state_roots = HashSet::new();
        for connection in &self.connections {
            connection
                .validate()
                .map_err(|error| ConfigValidationError::invalid(error.to_string()))?;
            if !connection_ids.insert(connection.id) {
                return Err(ConfigValidationError::invalid(format!(
                    "duplicate connection id: {}",
                    connection.id
                )));
            }
            if !provider_state_roots.insert(&connection.provider_state) {
                return Err(ConfigValidationError::invalid(format!(
                    "duplicate provider state root: {}",
                    connection.provider_state
                )));
            }
            for credential in &connection.credentials {
                if let CredentialBinding::Secret { reference } = &credential.binding
                    && !backend_ids.contains(&reference.backend_id)
                {
                    return Err(ConfigValidationError::invalid(format!(
                        "credential references unknown secret backend: {}",
                        reference.backend_id
                    )));
                }
            }
        }
        Ok(())
    }
}

/// User-interface preferences stored in config.
#[derive(Clone, Debug, Eq, PartialEq)]
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

    pub const fn get(self) -> u64 {
        self.0
    }
}

/// Config plus the revision at which it was observed.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ConfigSnapshot {
    pub revision: ConfigRevision,
    pub config: AppConfig,
}

/// Validated configuration mutations available to application callers.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ConfigChange {
    PutConnection(Connection),
    RemoveConnection(ConnectionId),
    PutSecretBackend(SecretBackend),
    RemoveSecretBackend(SecretBackendId),
    SetUiTheme(String),
    Reset,
}

/// A violated invariant spanning canonical configuration values.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ConfigValidationError {
    Invalid(String),
    SecretBackend(SecretBackendConfigurationError),
}

impl ConfigValidationError {
    fn invalid(message: impl Into<String>) -> Self {
        Self::Invalid(message.into())
    }
}

impl fmt::Display for ConfigValidationError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Invalid(message) => message.fmt(formatter),
            Self::SecretBackend(error) => error.fmt(formatter),
        }
    }
}

impl Error for ConfigValidationError {}

impl From<SecretBackendConfigurationError> for ConfigValidationError {
    fn from(error: SecretBackendConfigurationError) -> Self {
        Self::SecretBackend(error)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum SecretBackendConfigurationError {
    MissingFeature {
        backend: SecretBackendId,
        feature: &'static str,
    },
    UnsupportedPlatform {
        backend: SecretBackendId,
        requirement: &'static str,
    },
    InvalidSettings {
        backend: SecretBackendId,
    },
    InvalidPath {
        backend: SecretBackendId,
    },
    WrongKind {
        backend: SecretBackendId,
    },
    DuplicateLocalFilePath {
        backend: SecretBackendId,
    },
}

impl fmt::Display for SecretBackendConfigurationError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::MissingFeature { backend, feature } => write!(
                formatter,
                "secret backend {backend} requires cargo feature {feature}"
            ),
            Self::UnsupportedPlatform {
                backend,
                requirement,
            } => write!(formatter, "secret backend {backend} requires {requirement}"),
            Self::InvalidSettings { backend } => {
                write!(formatter, "secret backend {backend} has invalid settings")
            }
            Self::InvalidPath { backend } => {
                write!(formatter, "secret backend {backend} has an invalid path")
            }
            Self::WrongKind { backend } => {
                write!(formatter, "secret backend {backend} is not local-file")
            }
            Self::DuplicateLocalFilePath { backend } => write!(
                formatter,
                "secret backend {backend} duplicates another local-file path"
            ),
        }
    }
}

impl Error for SecretBackendConfigurationError {}

pub fn validate_backend_configuration(
    backend: &SecretBackend,
) -> Result<(), SecretBackendConfigurationError> {
    if !matches!(backend.settings, SecretBackendSettings::LocalFile { .. }) {
        return Ok(());
    }
    #[cfg(not(feature = "dev-secrets"))]
    return Err(SecretBackendConfigurationError::MissingFeature {
        backend: backend.id.clone(),
        feature: DEV_SECRETS_FEATURE,
    });
    #[cfg(all(feature = "dev-secrets", not(unix)))]
    return Err(SecretBackendConfigurationError::UnsupportedPlatform {
        backend: backend.id.clone(),
        requirement: "Unix private file permissions",
    });
    #[cfg(all(feature = "dev-secrets", unix))]
    {
        backend
            .validate()
            .map_err(|_| SecretBackendConfigurationError::InvalidSettings {
                backend: backend.id.clone(),
            })?;
    }
    #[cfg(all(feature = "dev-secrets", unix))]
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use yakshed_domain::ProviderStateRootId;

    fn connection(id: &str) -> Connection {
        Connection {
            id: id.parse().unwrap(),
            name: id.to_owned(),
            harness: "codex".to_owned(),
            model_provider: "openai".to_owned(),
            provider_state: ProviderStateRootId::new("shared-codex").unwrap(),
            credentials: Vec::new(),
        }
    }

    #[test]
    fn config_rejects_connections_sharing_a_provider_state_root() {
        let config = AppConfig {
            connections: vec![
                connection("0193f26e-7a72-7d42-bf77-0de14c4cc111"),
                connection("0193f26e-7a72-7d42-bf77-0de14c4cc222"),
            ],
            ..AppConfig::default()
        };

        assert!(config.validate().is_err());
    }
}
