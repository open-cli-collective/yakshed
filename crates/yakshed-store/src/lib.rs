//! Storage infrastructure for application paths and non-secret configuration.

mod config;
mod paths;

pub use config::{
    AppConfig, ConfigChange, ConfigError, ConfigRevision, ConfigSnapshot, ConfigStore,
    ConnectionConfig, CredentialBindingConfig, CredentialDelivery, SecretBackendConfig, UiConfig,
};
pub use paths::{AppPaths, PathError};
