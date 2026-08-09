use std::fs;

use tempfile::tempdir;
use yakshed_domain::ConnectionId;
use yakshed_store::{
    AppConfig, AppPaths, ConfigChange, ConfigError, ConfigRevision, ConfigStore, ConnectionConfig,
    CredentialBindingConfig, CredentialDelivery, SecretBackendConfig,
};

fn connection() -> ConnectionConfig {
    ConnectionConfig {
        id: "0193f26e-7a72-7d42-bf77-0de14c4cc222"
            .parse::<ConnectionId>()
            .unwrap(),
        name: "Work".into(),
        harness: "mock".into(),
        model_provider: "anthropic".into(),
        provider_state: "work-test".into(),
        credentials: vec![CredentialBindingConfig {
            slot: "anthropic.api_key".into(),
            source: "secret".into(),
            backend: Some("memory".into()),
            locator: Some("connection/work/anthropic_api_key".into()),
            delivery: Some(CredentialDelivery {
                kind: "process_environment".into(),
                variable: "ANTHROPIC_API_KEY".into(),
            }),
        }],
    }
}

#[tokio::test]
async fn config_round_trips_through_disk() {
    let temp = tempdir().unwrap();
    let paths = AppPaths::for_test(temp.path());
    let store = ConfigStore::open(paths.clone()).unwrap();

    let updated = store
        .update(
            ConfigRevision::INITIAL,
            ConfigChange::PutSecretBackend(SecretBackendConfig {
                id: "memory".into(),
                kind: "memory".into(),
                account: None,
            }),
        )
        .await
        .unwrap();
    let updated = store
        .update(updated.revision, ConfigChange::PutConnection(connection()))
        .await
        .unwrap();

    let reopened = ConfigStore::open(paths).unwrap();
    assert_eq!(reopened.snapshot().config, updated.config);
}

#[cfg(unix)]
#[test]
fn config_file_is_private() {
    use std::os::unix::fs::PermissionsExt;

    let temp = tempdir().unwrap();
    let paths = AppPaths::for_test(temp.path());
    ConfigStore::open(paths.clone()).unwrap();

    assert_eq!(
        fs::metadata(paths.config_root.join("config.toml"))
            .unwrap()
            .permissions()
            .mode()
            & 0o777,
        0o600
    );
}

#[test]
fn newer_schema_is_rejected_without_rewrite() {
    let temp = tempdir().unwrap();
    let paths = AppPaths::for_test(temp.path());
    paths.create_dirs().unwrap();
    let config_path = paths.config_root.join("config.toml");
    let bytes = b"schema_version = 999\n";
    fs::write(&config_path, bytes).unwrap();

    assert!(matches!(
        ConfigStore::open(paths),
        Err(ConfigError::UnsupportedSchema {
            found: 999,
            supported: 1
        })
    ));
    assert_eq!(fs::read(config_path).unwrap(), bytes);
}

#[test]
fn malformed_config_has_a_parse_error() {
    let temp = tempdir().unwrap();
    let paths = AppPaths::for_test(temp.path());
    paths.create_dirs().unwrap();
    fs::write(paths.config_root.join("config.toml"), "not = [toml").unwrap();

    assert!(matches!(
        ConfigStore::open(paths),
        Err(ConfigError::Parse(_))
    ));
}

#[tokio::test]
async fn stale_revision_conflicts_without_mutation() {
    let temp = tempdir().unwrap();
    let store = ConfigStore::open(AppPaths::for_test(temp.path())).unwrap();
    let first = store
        .update(
            ConfigRevision::INITIAL,
            ConfigChange::SetUiTheme("dark".into()),
        )
        .await
        .unwrap();

    assert!(matches!(
        store
            .update(
                ConfigRevision::INITIAL,
                ConfigChange::SetUiTheme("light".into())
            )
            .await,
        Err(ConfigError::Conflict { .. })
    ));
    assert_eq!(store.snapshot(), first);
}

#[tokio::test]
async fn concurrent_updates_allow_exactly_one_revision() {
    let temp = tempdir().unwrap();
    let store = ConfigStore::open(AppPaths::for_test(temp.path())).unwrap();

    let (dark, light) = tokio::join!(
        store.update(
            ConfigRevision::INITIAL,
            ConfigChange::SetUiTheme("dark".into())
        ),
        store.update(
            ConfigRevision::INITIAL,
            ConfigChange::SetUiTheme("light".into())
        )
    );

    assert_eq!(usize::from(dark.is_ok()) + usize::from(light.is_ok()), 1);
    assert_eq!(
        usize::from(matches!(dark, Err(ConfigError::Conflict { .. })))
            + usize::from(matches!(light, Err(ConfigError::Conflict { .. }))),
        1
    );
    assert_eq!(store.snapshot().revision.get(), 1);
}

#[tokio::test]
async fn invalid_change_is_rejected_without_mutation() {
    let temp = tempdir().unwrap();
    let store = ConfigStore::open(AppPaths::for_test(temp.path())).unwrap();

    assert!(matches!(
        store
            .update(
                ConfigRevision::INITIAL,
                ConfigChange::SetUiTheme("  ".into())
            )
            .await,
        Err(ConfigError::Validation(_))
    ));
    assert_eq!(store.snapshot().revision, ConfigRevision::INITIAL);
}

#[tokio::test]
async fn remove_operations_are_persisted() {
    let temp = tempdir().unwrap();
    let paths = AppPaths::for_test(temp.path());
    let store = ConfigStore::open(paths.clone()).unwrap();
    let backend = SecretBackendConfig {
        id: "memory".into(),
        kind: "memory".into(),
        account: None,
    };
    let snapshot = store
        .update(
            ConfigRevision::INITIAL,
            ConfigChange::PutSecretBackend(backend),
        )
        .await
        .unwrap();
    let snapshot = store
        .update(snapshot.revision, ConfigChange::PutConnection(connection()))
        .await
        .unwrap();
    let snapshot = store
        .update(
            snapshot.revision,
            ConfigChange::RemoveConnection(connection().id),
        )
        .await
        .unwrap();
    store
        .update(
            snapshot.revision,
            ConfigChange::RemoveSecretBackend("memory".into()),
        )
        .await
        .unwrap();

    let reopened = ConfigStore::open(paths).unwrap().snapshot();
    assert!(reopened.config.connections.is_empty());
    assert!(reopened.config.secret_backends.is_empty());
}

#[tokio::test]
async fn reset_only_recreates_config_toml() {
    let temp = tempdir().unwrap();
    let paths = AppPaths::for_test(temp.path());
    let store = ConfigStore::open(paths.clone()).unwrap();
    paths.create_dirs().unwrap();
    let durable = paths.data_root.join("keep-me");
    let cached = paths.cache_root.join("keep-me");
    let runtime = paths.runtime_root.join("keep-me");
    for path in [&durable, &cached, &runtime] {
        fs::write(path, b"still here").unwrap();
    }

    let reset = store
        .update(ConfigRevision::INITIAL, ConfigChange::Reset)
        .await
        .unwrap();

    assert_eq!(reset.config, AppConfig::default());
    for path in [durable, cached, runtime] {
        assert_eq!(fs::read(path).unwrap(), b"still here");
    }
}

#[test]
fn credential_binding_serialization_has_only_reference_fields() {
    let binding = &connection().credentials[0];
    let CredentialBindingConfig {
        slot: _,
        source: _,
        backend: _,
        locator: _,
        delivery: _,
    } = binding;
    let value = serde_json::to_value(binding).unwrap();
    let keys = value.as_object().unwrap().keys().map(String::as_str);

    assert_eq!(
        keys.collect::<std::collections::BTreeSet<_>>(),
        ["backend", "delivery", "locator", "slot", "source"]
            .into_iter()
            .collect()
    );
}
