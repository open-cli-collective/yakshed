//! Native macOS Keychain backend.
//!
//! Every backend uses service `dev.yakshed.YakShed` and account
//! `backend/<backend-id>/<locator>`. Backend IDs cannot contain `/`, so the backend boundary is
//! unambiguous while the opaque locator remains verbatim and distinct references cannot collide.

use std::sync::{Arc, OnceLock};

#[cfg(feature = "keychain-integration")]
use std::path::Path;

use async_trait::async_trait;
use secrecy::{ExposeSecret, SecretString};
use security_framework::item::{ItemClass, ItemSearchOptions};
use security_framework::os::macos::keychain::SecKeychain;

use crate::{
    DeleteSecretOutcome, LOCAL_OS_BACKEND_KIND, PutSecretOptions, PutSecretOutcome, ResolvedSecret,
    ResolvedSecretSource, SecretAccessContext, SecretAdministrator, SecretBackend,
    SecretBackendConfigurationError, SecretBackendDescriptor, SecretBackendId,
    SecretBackendSettings, SecretBackendStatus, SecretError, SecretLocator, SecretReferenceSummary,
    SecretResolver, validate_backend_configuration,
};

const SERVICE: &str = "dev.yakshed.YakShed";
const ERR_SEC_AUTH_FAILED: i32 = -25293;
const ERR_SEC_DUPLICATE_ITEM: i32 = -25299;
const ERR_SEC_ITEM_NOT_FOUND: i32 = -25300;
const ERR_SEC_NOT_AVAILABLE: i32 = -25291;
const ERR_SEC_NO_SUCH_KEYCHAIN: i32 = -25294;
const ERR_SEC_INTERACTION_NOT_ALLOWED: i32 = -25308;
const ERR_SEC_USER_CANCELED: i32 = -128;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum StoreError {
    NotFound,
    Duplicate,
    Unavailable,
    Locked,
    Denied,
    Failed,
}

trait KeychainStore: Send + Sync {
    fn probe(&self) -> Result<(), StoreError>;
    fn resolve(&self, service: &str, account: &str) -> Result<Vec<u8>, StoreError>;
    fn put(
        &self,
        service: &str,
        account: &str,
        value: &[u8],
        overwrite: bool,
    ) -> Result<bool, StoreError>;
    fn delete(&self, service: &str, account: &str) -> Result<bool, StoreError>;
}

struct NativeKeychain {
    keychain: OnceLock<Result<SecKeychain, StoreError>>,
}

impl NativeKeychain {
    fn default_keychain() -> Self {
        Self {
            keychain: OnceLock::new(),
        }
    }

    #[cfg(feature = "keychain-integration")]
    fn explicit(keychain: SecKeychain) -> Self {
        Self {
            keychain: OnceLock::from(Ok(keychain)),
        }
    }

    fn keychain(&self) -> Result<SecKeychain, StoreError> {
        self.keychain
            .get_or_init(|| SecKeychain::default().map_err(map_native_error))
            .clone()
    }
}

impl KeychainStore for NativeKeychain {
    fn probe(&self) -> Result<(), StoreError> {
        self.keychain().map(drop)
    }

    fn resolve(&self, service: &str, account: &str) -> Result<Vec<u8>, StoreError> {
        let keychain = self.keychain()?;
        keychain
            .find_generic_password(service, account)
            .map(|(password, _)| password.to_owned())
            .map_err(map_native_error)
    }

    fn put(
        &self,
        service: &str,
        account: &str,
        value: &[u8],
        overwrite: bool,
    ) -> Result<bool, StoreError> {
        let keychain = self.keychain()?;
        match keychain.find_generic_password(service, account) {
            Ok((_, mut item)) if overwrite => {
                item.set_password(value).map_err(map_native_error)?;
                Ok(true)
            }
            Ok(_) => Err(StoreError::Duplicate),
            Err(error) if error.code() == ERR_SEC_ITEM_NOT_FOUND => {
                keychain
                    .add_generic_password(service, account, value)
                    .map_err(map_native_error)?;
                Ok(false)
            }
            Err(error) => Err(map_native_error(error)),
        }
    }

    fn delete(&self, service: &str, account: &str) -> Result<bool, StoreError> {
        let keychain = self.keychain()?;
        let mut query = ItemSearchOptions::new();
        query
            .keychains(std::slice::from_ref(&keychain))
            .class(ItemClass::generic_password())
            .service(service)
            .account(account);
        match query.delete() {
            Ok(()) => Ok(true),
            Err(error) if error.code() == ERR_SEC_ITEM_NOT_FOUND => Ok(false),
            Err(error) => Err(map_native_error(error)),
        }
    }
}

fn map_native_error(error: security_framework::base::Error) -> StoreError {
    match error.code() {
        ERR_SEC_ITEM_NOT_FOUND => StoreError::NotFound,
        ERR_SEC_DUPLICATE_ITEM => StoreError::Duplicate,
        ERR_SEC_NOT_AVAILABLE | ERR_SEC_NO_SUCH_KEYCHAIN => StoreError::Unavailable,
        ERR_SEC_INTERACTION_NOT_ALLOWED => StoreError::Locked,
        ERR_SEC_AUTH_FAILED | ERR_SEC_USER_CANCELED => StoreError::Denied,
        _ => StoreError::Failed,
    }
}

pub struct LocalOsBackend {
    id: SecretBackendId,
    service: String,
    store: Arc<dyn KeychainStore>,
}

impl LocalOsBackend {
    pub fn from_config(config: &SecretBackend) -> Result<Self, SecretBackendConfigurationError> {
        if config.settings != SecretBackendSettings::LocalOs {
            return Err(SecretBackendConfigurationError::WrongKind {
                backend: config.id.clone(),
                expected: LOCAL_OS_BACKEND_KIND,
            });
        }
        validate_backend_configuration(config, crate::backend_capabilities())?;
        Ok(Self::with_store(
            config.id.clone(),
            Arc::new(NativeKeychain::default_keychain()),
        ))
    }

    #[cfg(feature = "keychain-integration")]
    pub fn for_keychain(
        id: SecretBackendId,
        path: &Path,
        password: &str,
    ) -> Result<Self, SecretError> {
        let mut keychain = SecKeychain::open(path)
            .map_err(map_native_error)
            .map_err(|error| map_store_error(&id, None, error))?;
        keychain
            .unlock(Some(password))
            .map_err(map_native_error)
            .map_err(|error| map_store_error(&id, None, error))?;
        Ok(Self::with_store(
            id,
            Arc::new(NativeKeychain::explicit(keychain)),
        ))
    }

    fn with_store(id: SecretBackendId, store: Arc<dyn KeychainStore>) -> Self {
        Self {
            service: SERVICE.to_owned(),
            id,
            store,
        }
    }

    fn account(&self, locator: &SecretLocator) -> String {
        format!("backend/{}/{}", self.id.as_str(), locator.as_str())
    }

    async fn run<T: Send + 'static>(
        &self,
        action: impl FnOnce(Arc<dyn KeychainStore>) -> Result<T, StoreError> + Send + 'static,
    ) -> Result<T, StoreError> {
        let store = Arc::clone(&self.store);
        tokio::task::spawn_blocking(move || action(store))
            .await
            .unwrap_or(Err(StoreError::Failed))
    }

    fn map_error(&self, locator: Option<&SecretLocator>, error: StoreError) -> SecretError {
        map_store_error(&self.id, locator, error)
    }
}

#[async_trait]
impl SecretResolver for LocalOsBackend {
    fn descriptor(&self) -> SecretBackendDescriptor {
        SecretBackendDescriptor {
            id: self.id.clone(),
            kind: LOCAL_OS_BACKEND_KIND.to_owned(),
        }
    }

    async fn probe(&self) -> Result<SecretBackendStatus, SecretError> {
        self.run(|store| store.probe())
            .await
            .map(|()| SecretBackendStatus::Available)
            .map_err(|error| self.map_error(None, error))
    }

    async fn resolve(
        &self,
        locator: &SecretLocator,
        _context: &SecretAccessContext,
    ) -> Result<ResolvedSecret, SecretError> {
        let service = self.service.clone();
        let account = self.account(locator);
        let bytes = self
            .run(move |store| store.resolve(&service, &account))
            .await
            .map_err(|error| self.map_error(Some(locator), error))?;
        let value = String::from_utf8(bytes).map_err(|_| SecretError::ProtocolViolation {
            backend: self.id.clone(),
            reason: "keychain value is not UTF-8".to_owned(),
        })?;
        Ok(ResolvedSecret::new(
            SecretString::from(value),
            ResolvedSecretSource {
                backend: self.id.clone(),
            },
            None,
        ))
    }
}

#[async_trait]
impl SecretAdministrator for LocalOsBackend {
    fn backend_id(&self) -> SecretBackendId {
        self.id.clone()
    }

    async fn put(
        &self,
        locator: &SecretLocator,
        value: &SecretString,
        options: PutSecretOptions,
    ) -> Result<PutSecretOutcome, SecretError> {
        let service = self.service.clone();
        let account = self.account(locator);
        let value = SecretString::from(value.expose_secret().to_owned());
        self.run(move |store| {
            store.put(
                &service,
                &account,
                value.expose_secret().as_bytes(),
                options.overwrite,
            )
        })
        .await
        .map(|replaced| {
            if replaced {
                PutSecretOutcome::Replaced
            } else {
                PutSecretOutcome::Written
            }
        })
        .map_err(|error| self.map_error(Some(locator), error))
    }

    async fn delete(&self, locator: &SecretLocator) -> Result<DeleteSecretOutcome, SecretError> {
        let service = self.service.clone();
        let account = self.account(locator);
        self.run(move |store| store.delete(&service, &account))
            .await
            .map(|deleted| {
                if deleted {
                    DeleteSecretOutcome::Deleted
                } else {
                    DeleteSecretOutcome::NotFound
                }
            })
            .map_err(|error| self.map_error(Some(locator), error))
    }
}

fn map_store_error(
    backend: &SecretBackendId,
    locator: Option<&SecretLocator>,
    error: StoreError,
) -> SecretError {
    match (error, locator) {
        (StoreError::NotFound, Some(locator)) => SecretError::NotFound {
            reference: SecretReferenceSummary {
                backend: backend.clone(),
                locator: locator.clone(),
            },
        },
        (StoreError::Duplicate, Some(locator)) => SecretError::AlreadyExists {
            reference: SecretReferenceSummary {
                backend: backend.clone(),
                locator: locator.clone(),
            },
        },
        (StoreError::NotFound, None) => SecretError::BackendUnavailable {
            backend: backend.clone(),
            remediation: None,
        },
        (StoreError::Duplicate, None) => SecretError::BackendFailure {
            backend: backend.clone(),
            redacted_message: "unexpected macOS Keychain duplicate".to_owned(),
        },
        (StoreError::Unavailable, _) => SecretError::BackendUnavailable {
            backend: backend.clone(),
            remediation: None,
        },
        (StoreError::Locked, _) => SecretError::Locked {
            backend: backend.clone(),
            remediation: None,
        },
        (StoreError::Denied, _) => SecretError::Denied {
            backend: backend.clone(),
            remediation: None,
        },
        (StoreError::Failed, _) => SecretError::BackendFailure {
            backend: backend.clone(),
            redacted_message: "macOS Keychain operation failed".to_owned(),
        },
    }
}

#[cfg(test)]
mod tests {
    use std::{collections::HashMap, sync::Mutex};

    use super::*;
    use crate::SecretAccessPurpose;
    use yakshed_domain::{CredentialSlot, OperationId};

    #[derive(Default)]
    struct FakeKeychain(Mutex<HashMap<(String, String), Vec<u8>>>);

    impl KeychainStore for FakeKeychain {
        fn probe(&self) -> Result<(), StoreError> {
            Ok(())
        }

        fn resolve(&self, service: &str, account: &str) -> Result<Vec<u8>, StoreError> {
            self.0
                .lock()
                .unwrap()
                .get(&(service.to_owned(), account.to_owned()))
                .cloned()
                .ok_or(StoreError::NotFound)
        }

        fn put(
            &self,
            service: &str,
            account: &str,
            value: &[u8],
            overwrite: bool,
        ) -> Result<bool, StoreError> {
            let mut values = self.0.lock().unwrap();
            let key = (service.to_owned(), account.to_owned());
            let existed = values.contains_key(&key);
            if existed && !overwrite {
                return Err(StoreError::Duplicate);
            }
            values.insert(key, value.to_owned());
            Ok(existed)
        }

        fn delete(&self, service: &str, account: &str) -> Result<bool, StoreError> {
            Ok(self
                .0
                .lock()
                .unwrap()
                .remove(&(service.to_owned(), account.to_owned()))
                .is_some())
        }
    }

    fn context() -> SecretAccessContext {
        SecretAccessContext {
            connection_id: "0193f26e-7a72-7000-8000-00000000aa01".parse().unwrap(),
            slot: CredentialSlot::new("provider.api-key").unwrap(),
            purpose: SecretAccessPurpose::StartHarness,
            request_id: OperationId::new("keychain-unit").unwrap(),
        }
    }

    #[tokio::test]
    async fn fake_store_covers_contract_and_redacts_values() {
        const CANARY: &str = "keychain-unit-canary-93be";
        let id = SecretBackendId::new("personal").unwrap();
        let backend = LocalOsBackend::with_store(id, Arc::new(FakeKeychain::default()));
        let locator = SecretLocator::new("connection/one/provider.api-key").unwrap();
        assert_eq!(backend.service, "dev.yakshed.YakShed");
        assert_eq!(
            backend.account(&locator),
            "backend/personal/connection/one/provider.api-key"
        );
        assert_eq!(
            backend.probe().await.unwrap(),
            SecretBackendStatus::Available
        );
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
        let duplicate = backend
            .put(
                &locator,
                &SecretString::from(CANARY.to_owned()),
                PutSecretOptions::NO_OVERWRITE,
            )
            .await
            .unwrap_err();
        assert!(!format!("{duplicate:?}").contains(CANARY));
        let resolved = backend.resolve(&locator, &context()).await.unwrap();
        assert!(resolved.expose(|value| value == CANARY));
        assert_eq!(
            backend
                .put(
                    &locator,
                    &SecretString::from("replacement".to_owned()),
                    PutSecretOptions::OVERWRITE,
                )
                .await
                .unwrap(),
            PutSecretOutcome::Replaced
        );
        assert_eq!(
            backend.delete(&locator).await.unwrap(),
            DeleteSecretOutcome::Deleted
        );
        assert_eq!(
            backend.delete(&locator).await.unwrap(),
            DeleteSecretOutcome::NotFound
        );
        let missing = match backend.resolve(&locator, &context()).await {
            Ok(_) => panic!("deleted secret resolved"),
            Err(error) => error,
        };
        assert!(matches!(missing, SecretError::NotFound { .. }));
        assert!(!format!("{missing:?}").contains(CANARY));
    }

    #[test]
    fn configured_backend_is_lazy_and_uses_local_os_kind() {
        let backend = LocalOsBackend::from_config(&SecretBackend {
            id: SecretBackendId::new("configured").unwrap(),
            settings: SecretBackendSettings::LocalOs,
        })
        .unwrap();
        assert_eq!(backend.descriptor().kind, LOCAL_OS_BACKEND_KIND);
        assert_eq!(backend.service, "dev.yakshed.YakShed");
    }
}
