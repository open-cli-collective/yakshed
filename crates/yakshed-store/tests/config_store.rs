use std::fs;

use tempfile::tempdir;
use yakshed_application::{AppConfig, ConfigChange, ConfigRevision};
use yakshed_domain::{
    Connection, ConnectionId, CredentialBinding, CredentialBindingRecord, CredentialSlot,
    ProviderStateRootId, SecretBackend, SecretBackendId, SecretBackendSettings, SecretLocator,
    SecretReference,
};
use yakshed_store::{AppPaths, ConfigError, ConfigStore};

#[cfg(not(feature = "dev-secrets"))]
use yakshed_application::SecretBackendConfigurationError;

const LOCAL_FILE_CONFIG: &[u8] = br#"schema_version = 1

[[secret_backends]]
id = "dev-local"
kind = "local-file"
path = "/tmp/yakshed-dev-secrets.json"
"#;

fn connection() -> Connection {
    Connection {
        id: "0193f26e-7a72-7d42-bf77-0de14c4cc222"
            .parse::<ConnectionId>()
            .unwrap(),
        name: "Work".into(),
        harness: "mock".into(),
        model_provider: "anthropic".into(),
        provider_state: ProviderStateRootId::new("work-test").unwrap(),
        credentials: vec![CredentialBindingRecord {
            slot: CredentialSlot::new("anthropic.api_key").unwrap(),
            binding: CredentialBinding::Secret {
                reference: SecretReference {
                    backend_id: SecretBackendId::new("memory").unwrap(),
                    locator: SecretLocator::new("connection/work/anthropic_api_key").unwrap(),
                },
            },
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
            ConfigChange::PutSecretBackend(SecretBackend {
                id: SecretBackendId::new("memory").unwrap(),
                settings: SecretBackendSettings::Memory,
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

#[cfg(not(feature = "dev-secrets"))]
#[test]
fn default_build_rejects_persisted_local_file_config() {
    let temp = tempdir().unwrap();
    let paths = AppPaths::for_test(temp.path());
    paths.create_config_root().unwrap();
    fs::write(paths.config_root.join("config.toml"), LOCAL_FILE_CONFIG).unwrap();

    assert!(matches!(
        ConfigStore::open(paths),
        Err(ConfigError::SecretBackendConfiguration(
            SecretBackendConfigurationError::MissingFeature {
                feature: "dev-secrets",
                ..
            }
        ))
    ));
}

#[cfg(all(feature = "dev-secrets", unix))]
#[test]
fn feature_build_accepts_persisted_local_file_config() {
    let temp = tempdir().unwrap();
    let paths = AppPaths::for_test(temp.path());
    paths.create_config_root().unwrap();
    fs::write(paths.config_root.join("config.toml"), LOCAL_FILE_CONFIG).unwrap();

    assert_eq!(
        ConfigStore::open(paths)
            .unwrap()
            .snapshot()
            .config
            .secret_backends,
        vec![SecretBackend {
            id: SecretBackendId::new("dev-local").unwrap(),
            settings: SecretBackendSettings::LocalFile {
                path: "/tmp/yakshed-dev-secrets.json".into(),
            },
        }]
    );
}

#[cfg(all(feature = "dev-secrets", not(unix)))]
#[test]
fn feature_build_rejects_local_file_without_unix_permissions() {
    let temp = tempdir().unwrap();
    let paths = AppPaths::for_test(temp.path());
    paths.create_config_root().unwrap();
    fs::write(paths.config_root.join("config.toml"), LOCAL_FILE_CONFIG).unwrap();

    assert!(matches!(
        ConfigStore::open(paths),
        Err(ConfigError::SecretBackendConfiguration(
            yakshed_application::SecretBackendConfigurationError::UnsupportedPlatform { .. }
        ))
    ));
}

#[test]
fn invalid_backend_setting_combinations_fail_closed() {
    for backend in [
        "id = \"memory\"\nkind = \"memory\"\npath = \"unexpected\"\n",
        "id = \"dev-local\"\nkind = \"local-file\"\n",
    ] {
        let temp = tempdir().unwrap();
        let paths = AppPaths::for_test(temp.path());
        paths.create_config_root().unwrap();
        let config_path = paths.config_root.join("config.toml");
        let source = format!("schema_version = 1\n\n[[secret_backends]]\n{backend}");
        fs::write(&config_path, &source).unwrap();

        assert!(matches!(
            ConfigStore::open(paths),
            Err(ConfigError::Validation(_))
        ));
        assert_eq!(fs::read_to_string(config_path).unwrap(), source);
    }
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
    paths.create_config_root().unwrap();
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
    paths.create_config_root().unwrap();
    fs::write(paths.config_root.join("config.toml"), "not = [toml").unwrap();

    assert!(matches!(
        ConfigStore::open(paths),
        Err(ConfigError::Parse(_))
    ));
}

#[test]
fn invalid_persisted_backend_id_fails_closed_without_rewrite() {
    let temp = tempdir().unwrap();
    let paths = AppPaths::for_test(temp.path());
    paths.create_config_root().unwrap();
    let config_path = paths.config_root.join("config.toml");
    let bytes =
        b"schema_version = 1\n\n[[secret_backends]]\nid = \"bad backend\"\nkind = \"memory\"\n";
    fs::write(&config_path, bytes).unwrap();

    assert!(matches!(
        ConfigStore::open(paths),
        Err(ConfigError::Validation(_))
    ));
    assert_eq!(fs::read(config_path).unwrap(), bytes);
}

#[test]
fn invalid_persisted_secret_locator_fails_closed_without_rewrite() {
    let temp = tempdir().unwrap();
    let paths = AppPaths::for_test(temp.path());
    paths.create_config_root().unwrap();
    let config_path = paths.config_root.join("config.toml");
    let bytes = br#"schema_version = 1

[[secret_backends]]
id = "memory"
kind = "memory"

[[connections]]
id = "0193f26e-7a72-7d42-bf77-0de14c4cc222"
name = "Work"
harness = "mock"
model_provider = "anthropic"
provider_state = "work-test"

[[connections.credentials]]
slot = "anthropic.api_key"
source = "secret"
backend = "memory"
locator = "bad\nlocator"
"#;
    fs::write(&config_path, bytes).unwrap();

    assert!(matches!(
        ConfigStore::open(paths),
        Err(ConfigError::Validation(_))
    ));
    assert_eq!(fs::read(config_path).unwrap(), bytes);
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
    let backend = SecretBackend {
        id: SecretBackendId::new("memory").unwrap(),
        settings: SecretBackendSettings::Memory,
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
            ConfigChange::RemoveSecretBackend(SecretBackendId::new("memory").unwrap()),
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
    paths.create_cache_root().unwrap();
    paths.create_data_root().unwrap();
    paths.create_runtime_root().unwrap();
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
fn opening_config_creates_only_the_config_root() {
    let temp = tempdir().unwrap();
    let paths = AppPaths::for_test(temp.path());
    ConfigStore::open(paths.clone()).unwrap();

    assert!(paths.config_root.is_dir());
    assert!(!paths.cache_root.exists());
    assert!(!paths.data_root.exists());
    assert!(!paths.runtime_root.exists());
}

#[tokio::test]
async fn secrets_section_7_binding_shapes_round_trip_without_delivery_metadata() {
    const EXAMPLE: &str = r#"schema_version = 1

[[secret_backends]]
id = "local-os"
kind = "local-os"

[[secret_backends]]
id = "onepassword-work"
kind = "onepassword-cli"
account = "work"

[[connections]]
id = "0193f26e-7a72-7d42-bf77-0de14c4cc111"
name = "Home"
harness = "codex"
model_provider = "openai"
provider_state = "home-codex"

[[connections.credentials]]
slot = "codex.account"
source = "delegated"
authority = "codex-app-server"

[[connections]]
id = "0193f26e-7a72-7d42-bf77-0de14c4cc222"
name = "Work"
harness = "claude-code"
model_provider = "anthropic"
provider_state = "work-claude"

[[connections.credentials]]
slot = "anthropic.api_key"
source = "secret"
backend = "onepassword-work"
locator = "op://Engineering/YakShed Work/Anthropic API Key"

[[connections]]
id = "0193f26e-7a72-7d42-bf77-0de14c4cc333"
name = "Lab"
harness = "codex"
model_provider = "fireworks"
provider_state = "lab-codex"

[[connections.credentials]]
slot = "fireworks.api_key"
source = "secret"
backend = "local-os"
locator = "connection/0193f26e-7a72-7d42-bf77-0de14c4cc333/fireworks_api_key"
"#;

    let temp = tempdir().unwrap();
    let paths = AppPaths::for_test(temp.path());
    paths.create_config_root().unwrap();
    fs::write(paths.config_root.join("config.toml"), EXAMPLE).unwrap();
    let store = ConfigStore::open(paths.clone()).unwrap();
    let snapshot = store
        .update(
            ConfigRevision::INITIAL,
            ConfigChange::SetUiTheme("system".into()),
        )
        .await
        .unwrap();

    assert!(matches!(
        snapshot.config.connections[0].credentials[0].binding,
        CredentialBinding::Delegated { ref authority } if authority == "codex-app-server"
    ));
    let written = fs::read_to_string(paths.config_root.join("config.toml")).unwrap();
    assert!(written.contains("authority = \"codex-app-server\""));
    assert!(!written.contains("delivery"));
    assert_eq!(
        ConfigStore::open(paths).unwrap().snapshot().config,
        snapshot.config
    );
}
