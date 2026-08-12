#![cfg(target_os = "macos")]

use std::{path::PathBuf, process::Command};

use secrecy::SecretString;
use security_framework::os::macos::keychain::CreateOptions;
use tempfile::TempDir;
use yakshed_domain::{CredentialSlot, OperationId};
use yakshed_secrets::{
    DeleteSecretOutcome, LocalOsBackend, PutSecretOptions, PutSecretOutcome, SecretAccessContext,
    SecretAccessPurpose, SecretAdministrator, SecretBackendId, SecretError, SecretLocator,
    SecretResolver,
};

const PASSWORD: &str = "yakshed-temporary-keychain";
const CANARY: &str = "keychain-integration-canary-542be8";

struct TemporaryKeychain {
    _root: TempDir,
    path: PathBuf,
    deleted: bool,
}

impl TemporaryKeychain {
    fn create() -> Self {
        let root = tempfile::tempdir().unwrap();
        let path = root.path().join("integration.keychain-db");
        let keychain = CreateOptions::new()
            .password(PASSWORD)
            .create(&path)
            .unwrap();
        drop(keychain);
        Self {
            _root: root,
            path,
            deleted: false,
        }
    }

    fn destroy(mut self) {
        self.delete();
        assert!(
            !self.path.exists(),
            "temporary keychain file survived deletion"
        );
    }

    fn delete(&mut self) {
        if self.deleted {
            return;
        }
        let status = Command::new("/usr/bin/security")
            .arg("delete-keychain")
            .arg(&self.path)
            .status();
        self.deleted = true;
        assert!(status.is_ok_and(|status| status.success()));
    }
}

impl Drop for TemporaryKeychain {
    fn drop(&mut self) {
        if !self.deleted {
            let _ = Command::new("/usr/bin/security")
                .arg("delete-keychain")
                .arg(&self.path)
                .status();
        }
    }
}

fn context() -> SecretAccessContext {
    SecretAccessContext {
        connection_id: "0193f26e-7a72-7000-8000-00000000bb01".parse().unwrap(),
        slot: CredentialSlot::new("provider.api-key").unwrap(),
        purpose: SecretAccessPurpose::ValidateCredential,
        request_id: OperationId::new("keychain-integration").unwrap(),
    }
}

#[tokio::test(flavor = "current_thread")]
async fn temporary_keychain_round_trip_isolated_and_redacted() {
    let temporary = TemporaryKeychain::create();
    let backend = LocalOsBackend::for_keychain(
        SecretBackendId::new("integration").unwrap(),
        &temporary.path,
        PASSWORD,
    )
    .unwrap();
    let locator = SecretLocator::new("connection/test/provider.api-key").unwrap();

    assert!(backend.probe().await.is_ok());
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
    assert!(
        backend
            .resolve(&locator, &context())
            .await
            .unwrap()
            .expose(|value| value == "replacement")
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

    drop(backend);
    temporary.destroy();
}
