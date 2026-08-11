use serde::{Deserialize, Serialize};
use yakshed_application::{PublicConnection, PublicCredentialBinding, PublicCredentialSource};
use yakshed_domain::{
    ApprovalDecision, ConnectionId, CredentialSlot, ProviderStateRootId, SecretBackendId,
    SecretLocator,
};

use crate::{DesktopError, Result};

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ApprovalDecisionInput {
    Approved,
    Denied,
}

impl From<ApprovalDecisionInput> for ApprovalDecision {
    fn from(value: ApprovalDecisionInput) -> Self {
        match value {
            ApprovalDecisionInput::Approved => Self::Approved,
            ApprovalDecisionInput::Denied => Self::Denied,
        }
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ConnectionInput {
    pub id: ConnectionId,
    pub name: String,
    pub harness: String,
    pub model_provider: String,
    pub provider_state: String,
    pub credentials: Vec<CredentialBindingInput>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct CredentialBindingInput {
    pub slot: CredentialSlot,
    #[serde(flatten)]
    pub source: CredentialSourceInput,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "source", rename_all = "snake_case")]
pub enum CredentialSourceInput {
    Delegated { authority: String },
    Secret { backend: String, locator: String },
    Disabled,
}

impl ConnectionInput {
    pub fn into_public(self) -> Result<PublicConnection> {
        Ok(PublicConnection {
            id: self.id,
            name: self.name,
            harness: self.harness,
            model_provider: self.model_provider,
            provider_state: ProviderStateRootId::new(self.provider_state)
                .map_err(|error| invalid_input(error.to_string()))?,
            credentials: self
                .credentials
                .into_iter()
                .map(|binding| {
                    Ok(PublicCredentialBinding {
                        slot: binding.slot,
                        source: match binding.source {
                            CredentialSourceInput::Delegated { authority } => {
                                PublicCredentialSource::Delegated { authority }
                            }
                            CredentialSourceInput::Secret { backend, locator } => {
                                PublicCredentialSource::Secret {
                                    backend: SecretBackendId::new(backend)
                                        .map_err(|error| invalid_input(error.to_string()))?,
                                    locator: SecretLocator::new(locator)
                                        .map_err(|error| invalid_input(error.to_string()))?,
                                }
                            }
                            CredentialSourceInput::Disabled => PublicCredentialSource::Disabled,
                        },
                    })
                })
                .collect::<Result<_>>()?,
        })
    }
}

fn invalid_input(detail: String) -> DesktopError {
    DesktopError {
        code: crate::DesktopErrorCode::InvalidRequest,
        message: "invalid connection",
        detail: Some(detail),
    }
}
