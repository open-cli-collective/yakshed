//! Application use cases, orchestration, snapshots, revisions, and application-owned ports, independent of Tauri commands and provider wire protocols.

use std::{collections::HashSet, error::Error, fmt};

use yakshed_domain::{Connection, ConnectionId, CredentialBinding, SecretBackend};

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
            return Err(ConfigValidationError("ui.theme cannot be empty".to_owned()));
        }

        let mut backend_ids = HashSet::new();
        for backend in &self.secret_backends {
            backend
                .validate()
                .map_err(|error| ConfigValidationError(error.to_string()))?;
            if !backend_ids.insert(backend.id.as_str()) {
                return Err(ConfigValidationError(format!(
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
                .map_err(|error| ConfigValidationError(error.to_string()))?;
            if !connection_ids.insert(connection.id) {
                return Err(ConfigValidationError(format!(
                    "duplicate connection id: {}",
                    connection.id
                )));
            }
            if !provider_state_roots.insert(&connection.provider_state) {
                return Err(ConfigValidationError(format!(
                    "duplicate provider state root: {}",
                    connection.provider_state
                )));
            }
            for credential in &connection.credentials {
                if let CredentialBinding::Secret { backend, .. } = &credential.binding
                    && !backend_ids.contains(backend.as_str())
                {
                    return Err(ConfigValidationError(format!(
                        "credential references unknown secret backend: {backend}"
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
    RemoveSecretBackend(String),
    SetUiTheme(String),
    Reset,
}

/// A violated invariant spanning canonical configuration values.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ConfigValidationError(String);

impl fmt::Display for ConfigValidationError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(formatter)
    }
}

impl Error for ConfigValidationError {}

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
