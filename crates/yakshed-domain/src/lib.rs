//! Domain values and invariants for IDs, work graphs, connections, artifacts, approvals, and lifecycle state, with no I/O or Tauri coupling.

use std::{collections::HashSet, error::Error, fmt, str::FromStr};

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
    pub provider_state: ProviderStateRootId,
    pub credentials: Vec<CredentialBindingRecord>,
}

impl Connection {
    pub fn validate(&self) -> Result<(), ValidationError> {
        require_nonempty("connection name", &self.name)?;
        require_nonempty("connection harness", &self.harness)?;
        require_nonempty("connection model_provider", &self.model_provider)?;
        let mut slots = HashSet::new();
        for credential in &self.credentials {
            credential.validate()?;
            if !slots.insert(&credential.slot) {
                return Err(ValidationError(format!(
                    "duplicate credential slot: {}",
                    credential.slot
                )));
            }
        }
        Ok(())
    }
}

/// One credential slot and its non-secret binding.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CredentialBindingRecord {
    pub slot: CredentialSlot,
    pub binding: CredentialBinding,
}

impl CredentialBindingRecord {
    pub fn validate(&self) -> Result<(), ValidationError> {
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

/// Validated opaque credential-requirement identifier.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub struct CredentialSlot(String);

impl CredentialSlot {
    pub fn new(value: impl Into<String>) -> Result<Self, ValidationError> {
        let value = value.into();
        require_identifier("credential slot", &value)?;
        Ok(Self(value))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl TryFrom<String> for CredentialSlot {
    type Error = ValidationError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        Self::new(value)
    }
}

impl From<CredentialSlot> for String {
    fn from(value: CredentialSlot) -> Self {
        value.0
    }
}

impl fmt::Display for CredentialSlot {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(formatter)
    }
}

/// Correlates one application operation without exposing provider wire IDs.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct OperationId(String);

impl OperationId {
    pub fn new(value: impl Into<String>) -> Result<Self, ValidationError> {
        let value = value.into();
        require_identifier("operation id", &value)?;
        Ok(Self(value))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for OperationId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(formatter)
    }
}

/// Validated opaque identifier for one provider-owned state root.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub struct ProviderStateRootId(String);

impl ProviderStateRootId {
    pub fn new(value: impl Into<String>) -> Result<Self, ValidationError> {
        let value = value.into();
        require_identifier("provider state root id", &value)?;
        if value != value.to_ascii_lowercase() {
            return Err(ValidationError(
                "provider state root id must already be lowercase".to_owned(),
            ));
        }
        if matches!(value.as_str(), "." | "..") || value.ends_with(['.', ' ']) {
            return Err(ValidationError(
                "provider state root id must be one safe path component".to_owned(),
            ));
        }
        if is_windows_reserved_name(&value) {
            return Err(ValidationError(
                "provider state root id cannot be a Windows reserved device name".to_owned(),
            ));
        }
        Ok(Self(value))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl TryFrom<String> for ProviderStateRootId {
    type Error = ValidationError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        Self::new(value)
    }
}

impl From<ProviderStateRootId> for String {
    fn from(value: ProviderStateRootId) -> Self {
        value.0
    }
}

impl fmt::Display for ProviderStateRootId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(formatter)
    }
}

/// Stable identity of a configured secret backend.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub struct SecretBackendId(String);

impl SecretBackendId {
    pub fn new(value: impl Into<String>) -> Result<Self, ValidationError> {
        let value = value.into();
        require_identifier("secret backend id", &value)?;
        Ok(Self(value))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl TryFrom<String> for SecretBackendId {
    type Error = ValidationError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        Self::new(value)
    }
}

impl From<SecretBackendId> for String {
    fn from(value: SecretBackendId) -> Self {
        value.0
    }
}

impl fmt::Display for SecretBackendId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(formatter)
    }
}

/// Opaque backend-owned locator with only universal safety validation.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub struct SecretLocator(String);

impl SecretLocator {
    pub fn new(value: impl Into<String>) -> Result<Self, ValidationError> {
        let value = value.into();
        require_nonempty("secret locator", &value)?;
        if value.len() > 4096 {
            return Err(ValidationError(
                "secret locator exceeds 4096 bytes".to_owned(),
            ));
        }
        if value.chars().any(char::is_control) {
            return Err(ValidationError(
                "secret locator cannot contain control characters".to_owned(),
            ));
        }
        Ok(Self(value))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl TryFrom<String> for SecretLocator {
    type Error = ValidationError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        Self::new(value)
    }
}

impl From<SecretLocator> for String {
    fn from(value: SecretLocator) -> Self {
        value.0
    }
}

impl fmt::Display for SecretLocator {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(formatter)
    }
}

/// One normalized non-secret reference to backend-owned secret material.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct SecretReference {
    pub backend_id: SecretBackendId,
    pub locator: SecretLocator,
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
        Err(ValidationError(format!("{field} cannot be empty")))
    } else {
        Ok(())
    }
}

fn require_identifier(field: &'static str, value: &str) -> Result<(), ValidationError> {
    require_nonempty(field, value)?;
    if value
        .chars()
        .all(|character| character.is_ascii_alphanumeric() || matches!(character, '-' | '_' | '.'))
    {
        Ok(())
    } else {
        Err(ValidationError(format!(
            "{field} may contain only ASCII letters, digits, '.', '-' and '_'"
        )))
    }
}

fn is_windows_reserved_name(value: &str) -> bool {
    let stem = value.split('.').next().unwrap_or(value);
    matches!(stem, "con" | "prn" | "aux" | "nul")
        || ((stem.starts_with("com") || stem.starts_with("lpt"))
            && stem.len() == 4
            && matches!(stem.as_bytes()[3], b'1'..=b'9'))
}

/// A violated domain value invariant.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ValidationError(String);

impl fmt::Display for ValidationError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(formatter)
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

    #[test]
    fn connection_rejects_conflicting_bindings_for_the_same_slot() {
        let connection = Connection {
            id: "0193f26e-7a72-7d42-bf77-0de14c4cc222".parse().unwrap(),
            name: "Work".to_owned(),
            harness: "codex".to_owned(),
            model_provider: "openai".to_owned(),
            provider_state: ProviderStateRootId::new("work-codex").unwrap(),
            credentials: vec![
                CredentialBindingRecord {
                    slot: CredentialSlot::new("codex.account").unwrap(),
                    binding: CredentialBinding::Delegated {
                        authority: "codex-app-server".to_owned(),
                    },
                },
                CredentialBindingRecord {
                    slot: CredentialSlot::new("codex.account").unwrap(),
                    binding: CredentialBinding::Secret {
                        backend: "local-os".to_owned(),
                        locator: "connection/work/codex_account".to_owned(),
                    },
                },
            ],
        };

        assert!(connection.validate().is_err());
    }

    #[test]
    fn provider_state_root_id_rejects_empty_and_path_values() {
        assert!(ProviderStateRootId::new("").is_err());
        assert!(ProviderStateRootId::new("../shared-codex").is_err());
        assert!(ProviderStateRootId::new("work-codex").is_ok());
        assert!(ProviderStateRootId::new("WORK-CODEX").is_err());
        assert!(ProviderStateRootId::new("work.").is_err());
        assert!(ProviderStateRootId::new("work ").is_err());
        for reserved in [
            "CON", "con.txt", "prn", "aux.log", "nul", "com1", "com9.txt", "lpt1", "lpt9.log",
        ] {
            assert!(ProviderStateRootId::new(reserved).is_err(), "{reserved}");
        }
        assert!(ProviderStateRootId::new("com0").is_ok());
        assert!(ProviderStateRootId::new("com10").is_ok());
    }
}
