//! Domain values and invariants for IDs, work graphs, connections, artifacts, approvals, and lifecycle state, with no I/O or Tauri coupling.

use std::{error::Error, fmt, str::FromStr};

use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// Stable identity of a configured connection.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
#[serde(transparent)]
pub struct ConnectionId(Uuid);

impl FromStr for ConnectionId {
    type Err = uuid::Error;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        Uuid::parse_str(value).map(Self)
    }
}

impl fmt::Display for ConnectionId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(formatter)
    }
}

/// A configured harness/model-provider trust boundary.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Connection {
    pub id: ConnectionId,
    pub name: String,
    pub harness: String,
    pub model_provider: String,
    pub provider_state: String,
    pub credentials: Vec<CredentialBindingRecord>,
}

impl Connection {
    pub fn validate(&self) -> Result<(), ValidationError> {
        require_nonempty("connection name", &self.name)?;
        require_nonempty("connection harness", &self.harness)?;
        require_nonempty("connection model_provider", &self.model_provider)?;
        require_nonempty("connection provider_state", &self.provider_state)?;
        self.credentials
            .iter()
            .try_for_each(CredentialBindingRecord::validate)
    }
}

/// One credential slot and its non-secret binding.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CredentialBindingRecord {
    pub slot: String,
    pub binding: CredentialBinding,
}

impl CredentialBindingRecord {
    pub fn validate(&self) -> Result<(), ValidationError> {
        require_nonempty("credential slot", &self.slot)?;
        self.binding.validate()
    }
}

/// Closed set of canonical credential-reference forms.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum CredentialBinding {
    Delegated { authority: String },
    Secret { backend: String, locator: String },
    Disabled,
}

impl CredentialBinding {
    pub fn validate(&self) -> Result<(), ValidationError> {
        match self {
            Self::Delegated { authority } => {
                require_nonempty("delegated credential authority", authority)
            }
            Self::Secret { backend, locator } => {
                require_nonempty("secret credential backend", backend)?;
                require_nonempty("secret credential locator", locator)
            }
            Self::Disabled => Ok(()),
        }
    }
}

/// Configuration for a secret-reference backend, never a secret value.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SecretBackend {
    pub id: String,
    pub kind: String,
    pub account: Option<String>,
}

impl SecretBackend {
    pub fn validate(&self) -> Result<(), ValidationError> {
        require_nonempty("secret backend id", &self.id)?;
        require_nonempty("secret backend kind", &self.kind)
    }
}

fn require_nonempty(field: &'static str, value: &str) -> Result<(), ValidationError> {
    if value.trim().is_empty() {
        Err(ValidationError { field })
    } else {
        Ok(())
    }
}

/// A violated domain value invariant.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ValidationError {
    field: &'static str,
}

impl fmt::Display for ValidationError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{} cannot be empty", self.field)
    }
}

impl Error for ValidationError {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn credential_binding_variants_enforce_their_reference_fields() {
        assert!(CredentialBinding::Disabled.validate().is_ok());
        assert!(
            CredentialBinding::Delegated {
                authority: String::new()
            }
            .validate()
            .is_err()
        );
        assert!(
            CredentialBinding::Secret {
                backend: "memory".to_owned(),
                locator: String::new()
            }
            .validate()
            .is_err()
        );
    }
}
