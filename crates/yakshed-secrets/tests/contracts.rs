use std::{
    collections::HashMap,
    ffi::OsString,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    time::Duration,
};

use async_trait::async_trait;
use secrecy::SecretString;
use tokio::sync::{Barrier, Notify, Semaphore};
use yakshed_application::{AppConfig, ConfigValidationError};
use yakshed_domain::{
    Connection, ConnectionId, CredentialBinding, CredentialBindingRecord, CredentialSlot,
    OperationId, ProviderStateRootId,
};
use yakshed_secrets::{
    BrokerCancellation, CredentialBroker, CredentialResolution, DeleteSecretOutcome,
    InvalidBindingReason, PutSecretOptions, PutSecretOutcome, ResolvedSecret, SecretAccessContext,
    SecretAccessPurpose, SecretAdministrator, SecretAuditEvent, SecretAuditSink, SecretBackend,
    SecretBackendAvailability, SecretBackendCapability, SecretBackendDescriptor,
    SecretBackendHandle, SecretBackendId, SecretBackendSettings, SecretBackendStatus, SecretError,
    SecretLocator, SecretOperation, SecretReference, SecretResolver, backend_capabilities,
    shape_process_environment,
};

#[cfg(feature = "dev-secrets")]
use yakshed_secrets::{CredentialStatus, MemorySecretBackend, MemorySecretFault};

#[cfg(all(feature = "dev-secrets", any(target_os = "macos", target_os = "linux")))]
use yakshed_secrets::LocalFileBackend;
use yakshed_secrets::SecretBackendConfigurationError;

const CONNECTION_A: &str = "0193f26e-7a72-7d42-bf77-0de14c4cc111";
const CONNECTION_B: &str = "0193f26e-7a72-7d42-bf77-0de14c4cc222";
const CANARY: &str = "yakshed-canary-a7f91b3e-secret";

fn backend_id(value: &str) -> SecretBackendId {
    SecretBackendId::new(value).unwrap()
}

fn locator(value: &str) -> SecretLocator {
    SecretLocator::new(value).unwrap()
}

fn context(connection: &str, slot: &str, request: &str) -> SecretAccessContext {
    SecretAccessContext {
        connection_id: connection.parse().unwrap(),
        slot: CredentialSlot::new(slot).unwrap(),
        purpose: SecretAccessPurpose::StartHarness,
        request_id: OperationId::new(request).unwrap(),
    }
}

fn secret_binding(slot: &str, backend: &str, locator: &str) -> CredentialBindingRecord {
    CredentialBindingRecord {
        slot: CredentialSlot::new(slot).unwrap(),
        binding: CredentialBinding::Secret {
            reference: SecretReference {
                backend_id: backend_id(backend),
                locator: SecretLocator::new(locator).unwrap(),
            },
        },
    }
}

fn connection(id: &str, state: &str, credentials: Vec<CredentialBindingRecord>) -> Connection {
    Connection {
        id: id.parse().unwrap(),
        name: state.into(),
        harness: "mock".into(),
        model_provider: "test".into(),
        provider_state: ProviderStateRootId::new(state).unwrap(),
        credentials,
    }
}

#[cfg(feature = "dev-secrets")]
fn handle(backend: Arc<MemorySecretBackend>) -> SecretBackendHandle {
    SecretBackendHandle {
        resolver: backend.clone(),
        administrator: Some(backend),
    }
}

#[derive(Default)]
struct AuditLog(Mutex<Vec<SecretAuditEvent>>);

impl SecretAuditSink for AuditLog {
    fn record(&self, event: SecretAuditEvent) {
        self.0.lock().unwrap().push(event);
    }
}

#[cfg(feature = "dev-secrets")]
fn build_broker(
    backend: Arc<MemorySecretBackend>,
    connections: &[Connection],
    audit: Arc<dyn SecretAuditSink>,
    timeout: Duration,
) -> CredentialBroker {
    CredentialBroker::new(
        [(backend.descriptor().id, handle(backend))],
        connections,
        audit,
        timeout,
    )
    .unwrap()
}

#[test]
fn domain_reference_values_reject_unsafe_input() {
    assert!(SecretLocator::new("").is_err());
    assert!(SecretLocator::new("line\nbreak").is_err());
    assert!(SecretLocator::new("x".repeat(4097)).is_err());
    assert!(SecretBackendId::new("bad backend").is_err());
    assert_eq!(locator("opaque/path").as_str(), "opaque/path");
}

#[test]
#[cfg(not(feature = "dev-secrets"))]
fn validation_distinguishes_missing_feature_from_unknown_kind() {
    let config = AppConfig {
        secret_backends: vec![SecretBackend {
            id: backend_id("dev-local"),
            settings: SecretBackendSettings::LocalFile {
                path: "/tmp/yakshed-dev-secrets.json".to_owned(),
            },
        }],
        ..AppConfig::default()
    };

    assert!(matches!(
        config.validate(backend_capabilities()),
        Err(ConfigValidationError::SecretBackend(
            SecretBackendConfigurationError::MissingFeature {
                kind: "local-file",
                feature: "dev-secrets",
                ..
            }
        ))
    ));

    assert!(matches!(
        config.validate(&[SecretBackendCapability::available("memory")]),
        Err(ConfigValidationError::SecretBackend(
            SecretBackendConfigurationError::UnsupportedKind {
                kind: "local-file",
                ..
            }
        ))
    ));
}

#[test]
fn relative_local_file_path_is_rejected_by_validation() {
    let config = AppConfig {
        secret_backends: vec![SecretBackend {
            id: backend_id("dev-local"),
            settings: SecretBackendSettings::LocalFile {
                path: "relative/secrets.json".to_owned(),
            },
        }],
        ..AppConfig::default()
    };

    assert!(matches!(
        config.validate(&[SecretBackendCapability::available("local-file")]),
        Err(ConfigValidationError::SecretBackend(
            SecretBackendConfigurationError::AbsolutePathRequired { .. }
        ))
    ));
}

#[test]
fn build_capabilities_report_local_file_status() {
    let local_file = backend_capabilities()
        .iter()
        .find(|capability| capability.kind == "local-file")
        .unwrap();
    #[cfg(all(feature = "dev-secrets", any(target_os = "macos", target_os = "linux")))]
    assert_eq!(
        local_file.availability,
        SecretBackendAvailability::Available
    );
    #[cfg(not(feature = "dev-secrets"))]
    assert_eq!(
        local_file.availability,
        SecretBackendAvailability::MissingFeature {
            feature: "dev-secrets"
        }
    );
    #[cfg(all(
        feature = "dev-secrets",
        not(any(target_os = "macos", target_os = "linux"))
    ))]
    assert_eq!(
        local_file.availability,
        SecretBackendAvailability::UnsupportedPlatform
    );
}

#[test]
fn build_capabilities_report_local_os_platform_status() {
    let local_os = backend_capabilities()
        .iter()
        .find(|capability| capability.kind == "local-os")
        .unwrap();
    let config = AppConfig {
        secret_backends: vec![SecretBackend {
            id: backend_id("local-os"),
            settings: SecretBackendSettings::LocalOs,
        }],
        ..AppConfig::default()
    };

    #[cfg(target_os = "macos")]
    {
        assert_eq!(local_os.availability, SecretBackendAvailability::Available);
        assert!(config.validate(backend_capabilities()).is_ok());
    }
    #[cfg(not(target_os = "macos"))]
    {
        assert_eq!(
            local_os.availability,
            SecretBackendAvailability::UnsupportedPlatform
        );
        assert!(matches!(
            config.validate(backend_capabilities()),
            Err(ConfigValidationError::SecretBackend(
                SecretBackendConfigurationError::UnsupportedPlatform {
                    kind: "local-os",
                    ..
                }
            ))
        ));
    }
}

#[test]
fn build_capabilities_gate_memory_backend_configuration() {
    let memory = backend_capabilities()
        .iter()
        .find(|capability| capability.kind == "memory")
        .unwrap();
    let config = AppConfig {
        secret_backends: vec![SecretBackend {
            id: backend_id("memory"),
            settings: SecretBackendSettings::Memory,
        }],
        ..AppConfig::default()
    };

    #[cfg(feature = "dev-secrets")]
    {
        assert_eq!(memory.availability, SecretBackendAvailability::Available);
        assert!(config.validate(backend_capabilities()).is_ok());
    }
    #[cfg(not(feature = "dev-secrets"))]
    {
        assert_eq!(
            memory.availability,
            SecretBackendAvailability::MissingFeature {
                feature: "dev-secrets"
            }
        );
        assert!(matches!(
            config.validate(backend_capabilities()),
            Err(ConfigValidationError::SecretBackend(
                SecretBackendConfigurationError::MissingFeature {
                    kind: "memory",
                    feature: "dev-secrets",
                    ..
                }
            ))
        ));
    }
}

#[test]
fn validation_reports_compiled_but_unsupported_platform() {
    let config = AppConfig {
        secret_backends: vec![SecretBackend {
            id: backend_id("dev-local"),
            settings: SecretBackendSettings::LocalFile {
                path: "/tmp/yakshed-dev-secrets.json".to_owned(),
            },
        }],
        ..AppConfig::default()
    };

    assert!(matches!(
        config.validate(&[SecretBackendCapability {
            kind: "local-file",
            availability: SecretBackendAvailability::UnsupportedPlatform,
            access: yakshed_application::SecretBackendAccess::ReadWrite,
            validate_locator: None,
        }]),
        Err(ConfigValidationError::SecretBackend(
            SecretBackendConfigurationError::UnsupportedPlatform {
                kind: "local-file",
                ..
            }
        ))
    ));
}

#[cfg(all(feature = "dev-secrets", any(target_os = "macos", target_os = "linux")))]
#[test]
fn real_build_capabilities_accept_local_file_config() {
    let config = AppConfig {
        secret_backends: vec![SecretBackend {
            id: backend_id("dev-local"),
            settings: SecretBackendSettings::LocalFile {
                path: "/tmp/yakshed-dev-secrets.json".to_owned(),
            },
        }],
        ..AppConfig::default()
    };

    assert!(config.validate(backend_capabilities()).is_ok());
}

#[cfg(all(feature = "dev-secrets", any(target_os = "macos", target_os = "linux")))]
#[test]
fn relative_local_file_path_is_rejected_at_construction() {
    let config = SecretBackend {
        id: backend_id("dev-local"),
        settings: SecretBackendSettings::LocalFile {
            path: "relative/secrets.json".to_owned(),
        },
    };

    assert!(matches!(
        LocalFileBackend::from_config(&config),
        Err(SecretBackendConfigurationError::AbsolutePathRequired { .. })
    ));
}

#[cfg(all(feature = "dev-secrets", any(target_os = "macos", target_os = "linux")))]
#[tokio::test]
async fn local_file_read_only_rejection_is_audited() {
    let temp = tempfile::tempdir().unwrap();
    let config = SecretBackend {
        id: backend_id("dev-local"),
        settings: SecretBackendSettings::LocalFile {
            path: (temp
                .path()
                .join("store/secrets.json")
                .to_string_lossy()
                .into_owned()),
        },
    };
    let backend = Arc::new(LocalFileBackend::from_config(&config).unwrap());
    let connections = [connection(
        CONNECTION_A,
        "connection-a",
        vec![secret_binding("provider.api_key", "dev-local", "a/key")],
    )];
    let audit = Arc::new(AuditLog::default());
    let broker = CredentialBroker::new(
        [(
            backend_id("dev-local"),
            SecretBackendHandle {
                resolver: backend,
                administrator: None,
            },
        )],
        &connections,
        audit.clone(),
        Duration::from_secs(1),
    )
    .unwrap();
    let context = context(CONNECTION_A, "provider.api_key", "request-local-read-only");

    assert!(matches!(
        broker
            .put(
                &connections,
                &connections[0].credentials[0],
                &context,
                &SecretString::from(CANARY.to_owned()),
                PutSecretOptions::NO_OVERWRITE,
                &BrokerCancellation::default(),
            )
            .await,
        Err(SecretError::UnsupportedOperation {
            operation: SecretOperation::Put,
            ..
        })
    ));
    assert!(audit.0.lock().unwrap().iter().any(|event| {
        event.operation == SecretOperation::Put
            && event.outcome == yakshed_secrets::SecretAuditOutcome::Failed
            && event.backend.as_ref() == Some(&backend_id("dev-local"))
    }));
}

#[tokio::test]
#[cfg(feature = "dev-secrets")]
async fn memory_backend_maps_faults_and_preserves_uncertain_write() {
    let backend = MemorySecretBackend::new(backend_id("memory"));
    let ctx = context(CONNECTION_A, "provider.api_key", "request-a");
    let loc = locator("connection/a/key");

    assert_eq!(backend.descriptor().kind, "memory");
    assert_eq!(
        backend.probe().await.unwrap(),
        SecretBackendStatus::Available
    );
    backend.plan_faults([MemorySecretFault::NotFound]);
    assert!(matches!(
        backend.resolve(&loc, &ctx).await,
        Err(SecretError::NotFound { .. })
    ));
    backend.plan_faults([MemorySecretFault::Denied]);
    assert!(matches!(
        backend.resolve(&loc, &ctx).await,
        Err(SecretError::Denied { .. })
    ));
    backend.plan_faults([MemorySecretFault::FailNextWrite]);
    assert!(matches!(
        backend
            .put(
                &loc,
                &SecretString::from(CANARY.to_owned()),
                PutSecretOptions::NO_OVERWRITE,
            )
            .await,
        Err(SecretError::BackendFailure { .. })
    ));
    backend.plan_faults([MemorySecretFault::UncertainWrite]);
    assert!(matches!(
        backend
            .put(
                &loc,
                &SecretString::from(CANARY.to_owned()),
                PutSecretOptions::NO_OVERWRITE,
            )
            .await,
        Err(SecretError::TimedOut { .. })
    ));
    assert!(
        backend
            .resolve(&loc, &ctx)
            .await
            .unwrap()
            .expose(|value| value == CANARY)
    );
    assert_eq!(backend.remaining_faults(), 0);
}

#[test]
#[cfg(feature = "dev-secrets")]
fn duplicate_secret_references_are_rejected_across_bindings() {
    let connections = vec![
        connection(
            CONNECTION_A,
            "connection-a",
            vec![secret_binding("provider.api_key", "memory", "shared/key")],
        ),
        connection(
            CONNECTION_B,
            "connection-b",
            vec![secret_binding("provider.other_key", "memory", "shared/key")],
        ),
    ];
    let backend = Arc::new(MemorySecretBackend::new(backend_id("memory")));

    assert!(
        CredentialBroker::new(
            [(backend_id("memory"), handle(backend))],
            &connections,
            Arc::new(AuditLog::default()),
            Duration::from_secs(1),
        )
        .is_err()
    );
}

#[tokio::test]
#[cfg(feature = "dev-secrets")]
async fn duplicate_reference_introduced_later_cannot_delete_shared_secret() {
    let initial = vec![
        connection(
            CONNECTION_A,
            "connection-a",
            vec![secret_binding("provider.api_key", "memory", "a/key")],
        ),
        connection(
            CONNECTION_B,
            "connection-b",
            vec![secret_binding("provider.other_key", "memory", "b/key")],
        ),
    ];
    let backend = Arc::new(MemorySecretBackend::new(backend_id("memory")));
    let broker = build_broker(
        backend.clone(),
        &initial,
        Arc::new(AuditLog::default()),
        Duration::from_secs(1),
    );
    let ctx = context(CONNECTION_A, "provider.api_key", "request-delete");
    broker
        .put(
            &initial,
            &initial[0].credentials[0],
            &ctx,
            &SecretString::from(CANARY.to_owned()),
            PutSecretOptions::NO_OVERWRITE,
            &BrokerCancellation::default(),
        )
        .await
        .unwrap();

    let duplicated = vec![
        initial[0].clone(),
        connection(
            CONNECTION_B,
            "connection-b",
            vec![secret_binding("provider.other_key", "memory", "a/key")],
        ),
    ];
    assert!(
        broker
            .delete(
                &duplicated,
                &duplicated[0].credentials[0],
                &ctx,
                &BrokerCancellation::default(),
            )
            .await
            .is_err()
    );
    assert!(
        backend
            .resolve(&locator("a/key"), &ctx)
            .await
            .unwrap()
            .expose(|value| value == CANARY)
    );
}

#[test]
#[cfg(feature = "dev-secrets")]
fn backend_registry_rejects_mismatched_keys_and_duplicate_descriptor_ids() {
    let connections = [connection(CONNECTION_A, "connection-a", Vec::new())];
    let actual = Arc::new(MemorySecretBackend::new(backend_id("actual")));
    assert!(
        CredentialBroker::new(
            [(backend_id("registered"), handle(actual))],
            &connections,
            Arc::new(AuditLog::default()),
            Duration::from_secs(1),
        )
        .is_err()
    );

    let first = Arc::new(MemorySecretBackend::new(backend_id("duplicate")));
    let second = Arc::new(MemorySecretBackend::new(backend_id("duplicate")));
    assert!(
        CredentialBroker::new(
            [
                (backend_id("duplicate"), handle(first)),
                (backend_id("duplicate"), handle(second)),
            ],
            &connections,
            Arc::new(AuditLog::default()),
            Duration::from_secs(1),
        )
        .is_err()
    );
}

#[test]
#[cfg(feature = "dev-secrets")]
fn backend_registry_rejects_mismatched_resolver_and_administrator_ids() {
    let connections = [connection(CONNECTION_A, "connection-a", Vec::new())];
    let resolver = Arc::new(MemorySecretBackend::new(backend_id("resolver")));
    let administrator = Arc::new(MemorySecretBackend::new(backend_id("administrator")));

    assert!(
        CredentialBroker::new(
            [(
                backend_id("resolver"),
                SecretBackendHandle {
                    resolver,
                    administrator: Some(administrator),
                },
            )],
            &connections,
            Arc::new(AuditLog::default()),
            Duration::from_secs(1),
        )
        .is_err()
    );
}

#[tokio::test]
#[cfg(feature = "dev-secrets")]
async fn read_only_put_rejection_is_audited() {
    let backend = Arc::new(MemorySecretBackend::new(backend_id("memory")));
    let connections = [connection(
        CONNECTION_A,
        "connection-a",
        vec![secret_binding("provider.api_key", "memory", "a/key")],
    )];
    let audit = Arc::new(AuditLog::default());
    let broker = CredentialBroker::new(
        [(
            backend_id("memory"),
            SecretBackendHandle {
                resolver: backend,
                administrator: None,
            },
        )],
        &connections,
        audit.clone(),
        Duration::from_secs(1),
    )
    .unwrap();
    let context = context(CONNECTION_A, "provider.api_key", "request-read-only-put");

    assert!(matches!(
        broker
            .put(
                &connections,
                &connections[0].credentials[0],
                &context,
                &SecretString::from(CANARY.to_owned()),
                PutSecretOptions::NO_OVERWRITE,
                &BrokerCancellation::default(),
            )
            .await,
        Err(SecretError::UnsupportedOperation {
            operation: SecretOperation::Put,
            ..
        })
    ));
    assert!(audit.0.lock().unwrap().iter().any(|event| {
        event.operation == SecretOperation::Put
            && event.outcome == yakshed_secrets::SecretAuditOutcome::Failed
            && event.backend.as_ref() == Some(&backend_id("memory"))
    }));
}

#[tokio::test]
#[cfg(feature = "dev-secrets")]
async fn read_only_delete_rejection_is_audited() {
    let backend = Arc::new(MemorySecretBackend::new(backend_id("memory")));
    let connections = [connection(
        CONNECTION_A,
        "connection-a",
        vec![secret_binding("provider.api_key", "memory", "a/key")],
    )];
    let audit = Arc::new(AuditLog::default());
    let broker = CredentialBroker::new(
        [(
            backend_id("memory"),
            SecretBackendHandle {
                resolver: backend,
                administrator: None,
            },
        )],
        &connections,
        audit.clone(),
        Duration::from_secs(1),
    )
    .unwrap();
    let context = context(CONNECTION_A, "provider.api_key", "request-read-only-delete");

    assert!(matches!(
        broker
            .delete(
                &connections,
                &connections[0].credentials[0],
                &context,
                &BrokerCancellation::default(),
            )
            .await,
        Err(SecretError::UnsupportedOperation {
            operation: SecretOperation::Delete,
            ..
        })
    ));
    assert!(audit.0.lock().unwrap().iter().any(|event| {
        event.operation == SecretOperation::Delete
            && event.outcome == yakshed_secrets::SecretAuditOutcome::Failed
            && event.backend.as_ref() == Some(&backend_id("memory"))
    }));
}

#[tokio::test]
#[cfg(feature = "dev-secrets")]
async fn broker_maps_required_failures_and_uncertain_write_reconciliation() {
    let backend = Arc::new(MemorySecretBackend::new(backend_id("memory")));
    let connections = vec![connection(
        CONNECTION_A,
        "connection-a",
        vec![secret_binding("provider.api_key", "memory", "a/key")],
    )];
    let binding = &connections[0].credentials[0];
    let ctx = context(CONNECTION_A, "provider.api_key", "request-a");
    let audit = Arc::new(AuditLog::default());
    let broker = build_broker(
        backend.clone(),
        &connections,
        audit.clone(),
        Duration::from_millis(20),
    );

    assert_eq!(
        broker
            .status(&connections, binding, &ctx, &BrokerCancellation::default())
            .await
            .unwrap(),
        CredentialStatus::Missing
    );
    assert_eq!(
        broker
            .put(
                &connections,
                binding,
                &ctx,
                &SecretString::from(CANARY.to_owned()),
                PutSecretOptions::NO_OVERWRITE,
                &BrokerCancellation::default(),
            )
            .await
            .unwrap(),
        PutSecretOutcome::Written
    );
    assert!(matches!(
        broker
            .put(
                &connections,
                binding,
                &ctx,
                &SecretString::from("different-canary".to_owned()),
                PutSecretOptions::NO_OVERWRITE,
                &BrokerCancellation::default(),
            )
            .await,
        Err(SecretError::AlreadyExists { .. })
    ));
    backend.plan_faults([MemorySecretFault::Denied]);
    assert!(matches!(
        broker
            .resolve(&connections, binding, &ctx, &BrokerCancellation::default())
            .await,
        Err(SecretError::Denied { .. })
    ));
    backend.plan_faults([MemorySecretFault::UncertainWrite]);
    assert!(matches!(
        broker
            .put(
                &connections,
                binding,
                &ctx,
                &SecretString::from(CANARY.to_owned()),
                PutSecretOptions::OVERWRITE,
                &BrokerCancellation::default(),
            )
            .await,
        Err(SecretError::UncertainWrite { .. })
    ));
    assert!(audit.0.lock().unwrap().iter().any(|event| {
        event.operation == SecretOperation::Put
            && event.outcome == yakshed_secrets::SecretAuditOutcome::Uncertain
    }));
    assert_eq!(
        broker
            .status(&connections, binding, &ctx, &BrokerCancellation::default())
            .await
            .unwrap(),
        CredentialStatus::Present
    );

    let read_only = CredentialBroker::new(
        [(
            backend_id("memory"),
            SecretBackendHandle {
                resolver: backend.clone(),
                administrator: None,
            },
        )],
        &connections,
        Arc::new(AuditLog::default()),
        Duration::from_secs(1),
    )
    .unwrap();
    assert!(matches!(
        read_only
            .put(
                &connections,
                binding,
                &ctx,
                &SecretString::from(CANARY.to_owned()),
                PutSecretOptions::OVERWRITE,
                &BrokerCancellation::default(),
            )
            .await,
        Err(SecretError::UnsupportedOperation { .. })
    ));

    let unavailable = CredentialBroker::new(
        Vec::<(SecretBackendId, SecretBackendHandle)>::new(),
        &connections,
        Arc::new(AuditLog::default()),
        Duration::from_secs(1),
    )
    .unwrap();
    assert!(matches!(
        unavailable
            .resolve(&connections, binding, &ctx, &BrokerCancellation::default())
            .await,
        Err(SecretError::BackendUnavailable { .. })
    ));

    backend.plan_faults([MemorySecretFault::Timeout]);
    assert!(matches!(
        broker
            .resolve(&connections, binding, &ctx, &BrokerCancellation::default())
            .await,
        Err(SecretError::TimedOut { .. })
    ));
    assert!(!format!("{:?}", audit.0.lock().unwrap()).contains(CANARY));
}

struct PanicResolver(AtomicUsize);

#[async_trait]
impl SecretResolver for PanicResolver {
    fn descriptor(&self) -> SecretBackendDescriptor {
        SecretBackendDescriptor {
            id: backend_id("panic"),
            kind: "test".into(),
        }
    }

    async fn probe(&self) -> Result<SecretBackendStatus, SecretError> {
        self.0.fetch_add(1, Ordering::SeqCst);
        panic!("rejected binding touched backend")
    }

    async fn resolve(
        &self,
        _locator: &SecretLocator,
        _context: &SecretAccessContext,
    ) -> Result<ResolvedSecret, SecretError> {
        self.0.fetch_add(1, Ordering::SeqCst);
        panic!("rejected binding touched backend")
    }
}

#[tokio::test]
async fn current_binding_changes_and_detaches_take_effect_immediately() {
    let original = connection(
        CONNECTION_A,
        "connection-a",
        vec![secret_binding("provider.api_key", "panic", "a/key")],
    );
    let resolver = Arc::new(PanicResolver(AtomicUsize::new(0)));
    let broker = CredentialBroker::new(
        [(
            backend_id("panic"),
            SecretBackendHandle {
                resolver: resolver.clone(),
                administrator: None,
            },
        )],
        std::slice::from_ref(&original),
        Arc::new(AuditLog::default()),
        Duration::from_secs(1),
    )
    .unwrap();
    let ctx = context(CONNECTION_A, "provider.api_key", "request-change");
    let delegated_binding = CredentialBindingRecord {
        slot: ctx.slot.clone(),
        binding: CredentialBinding::Delegated {
            authority: "provider-login".into(),
        },
    };
    let changed = connection(
        CONNECTION_A,
        "connection-a",
        vec![delegated_binding.clone()],
    );

    assert!(matches!(
        broker
            .resolve(
                std::slice::from_ref(&changed),
                &delegated_binding,
                &ctx,
                &BrokerCancellation::default(),
            )
            .await
            .unwrap(),
        CredentialResolution::Delegated(_)
    ));
    assert!(matches!(
        broker
            .resolve(
                std::slice::from_ref(&changed),
                &original.credentials[0],
                &ctx,
                &BrokerCancellation::default(),
            )
            .await,
        Err(SecretError::InvalidBinding {
            reason: InvalidBindingReason::StaleBinding,
            ..
        })
    ));
    let detached = connection(CONNECTION_A, "connection-a", Vec::new());
    assert!(matches!(
        broker
            .resolve(
                std::slice::from_ref(&detached),
                &original.credentials[0],
                &ctx,
                &BrokerCancellation::default(),
            )
            .await,
        Err(SecretError::InvalidBinding {
            reason: InvalidBindingReason::UnknownSlot,
            ..
        })
    ));
    assert_eq!(resolver.0.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn disabled_and_invalid_scopes_fail_before_backend_access() {
    let disabled = CredentialBindingRecord {
        slot: CredentialSlot::new("provider.api_key").unwrap(),
        binding: CredentialBinding::Disabled,
    };
    let connections = [connection(
        CONNECTION_A,
        "connection-a",
        vec![disabled.clone()],
    )];
    let broker = CredentialBroker::new(
        Vec::<(SecretBackendId, SecretBackendHandle)>::new(),
        &connections,
        Arc::new(AuditLog::default()),
        Duration::from_secs(1),
    )
    .unwrap();

    assert!(matches!(
        broker
            .resolve(
                &connections,
                &disabled,
                &context(CONNECTION_A, "provider.api_key", "request-disabled"),
                &BrokerCancellation::default(),
            )
            .await,
        Err(SecretError::InvalidBinding {
            reason: InvalidBindingReason::Disabled,
            ..
        })
    ));
    assert!(matches!(
        broker
            .resolve(
                &connections,
                &disabled,
                &context(CONNECTION_B, "provider.api_key", "request-unknown"),
                &BrokerCancellation::default(),
            )
            .await,
        Err(SecretError::InvalidBinding {
            reason: InvalidBindingReason::UnknownConnection,
            ..
        })
    ));
}

#[tokio::test]
#[cfg(feature = "dev-secrets")]
async fn pre_cancelled_put_never_mutates_an_immediately_completing_backend() {
    let backend = Arc::new(MemorySecretBackend::new(backend_id("memory")));
    let connections = vec![connection(
        CONNECTION_A,
        "connection-a",
        vec![secret_binding("provider.api_key", "memory", "a/key")],
    )];
    let binding = &connections[0].credentials[0];
    let ctx = context(CONNECTION_A, "provider.api_key", "request-cancelled");
    let broker = build_broker(
        backend,
        &connections,
        Arc::new(AuditLog::default()),
        Duration::from_secs(1),
    );
    let cancellation = BrokerCancellation::default();
    cancellation.cancel();

    assert!(matches!(
        broker
            .put(
                &connections,
                binding,
                &ctx,
                &SecretString::from(CANARY.to_owned()),
                PutSecretOptions::NO_OVERWRITE,
                &cancellation,
            )
            .await,
        Err(SecretError::Cancelled { .. })
    ));
    assert_eq!(
        broker
            .status(&connections, binding, &ctx, &BrokerCancellation::default())
            .await
            .unwrap(),
        CredentialStatus::Missing
    );
}

struct ImmediateWriteBackend {
    writes: AtomicUsize,
}

struct SlowWriteBackend {
    writes: Arc<AtomicUsize>,
    started: Arc<Notify>,
    completed: Arc<Notify>,
}

#[async_trait]
impl SecretResolver for SlowWriteBackend {
    fn descriptor(&self) -> SecretBackendDescriptor {
        SecretBackendDescriptor {
            id: backend_id("slow"),
            kind: "test".into(),
        }
    }

    async fn probe(&self) -> Result<SecretBackendStatus, SecretError> {
        Ok(SecretBackendStatus::Available)
    }

    async fn resolve(
        &self,
        locator: &SecretLocator,
        _context: &SecretAccessContext,
    ) -> Result<ResolvedSecret, SecretError> {
        Err(SecretError::NotFound {
            reference: yakshed_secrets::SecretReferenceSummary::from(&SecretReference {
                backend_id: backend_id("slow"),
                locator: locator.clone(),
            }),
        })
    }
}

#[async_trait]
impl SecretAdministrator for SlowWriteBackend {
    fn backend_id(&self) -> SecretBackendId {
        backend_id("slow")
    }

    async fn put(
        &self,
        _locator: &SecretLocator,
        _value: &SecretString,
        _options: PutSecretOptions,
    ) -> Result<PutSecretOutcome, SecretError> {
        self.started.notify_waiters();
        let writes = Arc::clone(&self.writes);
        let completed = Arc::clone(&self.completed);
        tokio::task::spawn_blocking(move || {
            std::thread::sleep(Duration::from_millis(80));
            writes.fetch_add(1, Ordering::SeqCst);
            completed.notify_one();
        })
        .await
        .unwrap();
        Ok(PutSecretOutcome::Written)
    }

    async fn delete(&self, _locator: &SecretLocator) -> Result<DeleteSecretOutcome, SecretError> {
        Ok(DeleteSecretOutcome::NotFound)
    }
}

#[tokio::test]
async fn dispatched_mutation_cancellation_is_uncertain_and_audited() {
    let backend = Arc::new(SlowWriteBackend {
        writes: Arc::new(AtomicUsize::new(0)),
        started: Arc::new(Notify::new()),
        completed: Arc::new(Notify::new()),
    });
    let connections = Arc::new(vec![connection(
        CONNECTION_A,
        "connection-a",
        vec![secret_binding("provider.api_key", "slow", "a/key")],
    )]);
    let audit = Arc::new(AuditLog::default());
    let broker = Arc::new(
        CredentialBroker::new(
            [(
                backend_id("slow"),
                SecretBackendHandle {
                    resolver: backend.clone(),
                    administrator: Some(backend.clone()),
                },
            )],
            &connections,
            audit.clone(),
            Duration::from_secs(1),
        )
        .unwrap(),
    );
    let cancellation = BrokerCancellation::default();
    let started = backend.started.notified();
    let task = tokio::spawn({
        let broker = broker.clone();
        let cancellation = cancellation.clone();
        async move {
            broker
                .put(
                    &connections,
                    &connections[0].credentials[0],
                    &context(CONNECTION_A, "provider.api_key", "request-mid-save"),
                    &SecretString::from(CANARY.to_owned()),
                    PutSecretOptions::NO_OVERWRITE,
                    &cancellation,
                )
                .await
        }
    });
    started.await;
    cancellation.cancel();

    assert!(matches!(
        task.await.unwrap(),
        Err(SecretError::UncertainWrite { .. })
    ));
    backend.completed.notified().await;
    assert_eq!(backend.writes.load(Ordering::SeqCst), 1);
    assert!(audit.0.lock().unwrap().iter().any(|event| {
        event.operation == SecretOperation::Put
            && event.outcome == yakshed_secrets::SecretAuditOutcome::Uncertain
    }));
}

#[async_trait]
impl SecretResolver for ImmediateWriteBackend {
    fn descriptor(&self) -> SecretBackendDescriptor {
        SecretBackendDescriptor {
            id: backend_id("immediate"),
            kind: "test".into(),
        }
    }

    async fn probe(&self) -> Result<SecretBackendStatus, SecretError> {
        Ok(SecretBackendStatus::Available)
    }

    async fn resolve(
        &self,
        locator: &SecretLocator,
        _context: &SecretAccessContext,
    ) -> Result<ResolvedSecret, SecretError> {
        Err(SecretError::NotFound {
            reference: yakshed_secrets::SecretReferenceSummary::from(&SecretReference {
                backend_id: backend_id("immediate"),
                locator: locator.clone(),
            }),
        })
    }
}

#[async_trait]
impl SecretAdministrator for ImmediateWriteBackend {
    fn backend_id(&self) -> SecretBackendId {
        backend_id("immediate")
    }

    async fn put(
        &self,
        _locator: &SecretLocator,
        _value: &SecretString,
        _options: PutSecretOptions,
    ) -> Result<PutSecretOutcome, SecretError> {
        self.writes.fetch_add(1, Ordering::SeqCst);
        Ok(PutSecretOutcome::Written)
    }

    async fn delete(&self, _locator: &SecretLocator) -> Result<DeleteSecretOutcome, SecretError> {
        Ok(DeleteSecretOutcome::NotFound)
    }
}

#[tokio::test]
async fn cancellation_race_never_reports_cancelled_after_a_write() {
    let backend = Arc::new(ImmediateWriteBackend {
        writes: AtomicUsize::new(0),
    });
    let connections = Arc::new(vec![connection(
        CONNECTION_A,
        "connection-a",
        vec![secret_binding("provider.api_key", "immediate", "a/key")],
    )]);
    let broker = Arc::new(
        CredentialBroker::new(
            [(
                backend_id("immediate"),
                SecretBackendHandle {
                    resolver: backend.clone(),
                    administrator: Some(backend.clone()),
                },
            )],
            &connections,
            Arc::new(AuditLog::default()),
            Duration::from_secs(1),
        )
        .unwrap(),
    );

    for iteration in 0..300 {
        let before = backend.writes.load(Ordering::SeqCst);
        let barrier = Arc::new(Barrier::new(3));
        let cancellation = BrokerCancellation::default();
        let put = {
            let barrier = barrier.clone();
            let broker = broker.clone();
            let connections = connections.clone();
            let cancellation = cancellation.clone();
            tokio::spawn(async move {
                barrier.wait().await;
                broker
                    .put(
                        &connections,
                        &connections[0].credentials[0],
                        &context(
                            CONNECTION_A,
                            "provider.api_key",
                            &format!("race-{iteration}"),
                        ),
                        &SecretString::from(CANARY.to_owned()),
                        PutSecretOptions::OVERWRITE,
                        &cancellation,
                    )
                    .await
            })
        };
        let cancel = {
            let barrier = barrier.clone();
            let cancellation = cancellation.clone();
            tokio::spawn(async move {
                barrier.wait().await;
                cancellation.cancel();
            })
        };
        barrier.wait().await;
        let result = put.await.unwrap();
        cancel.await.unwrap();
        let after = backend.writes.load(Ordering::SeqCst);
        match result {
            Ok(_) => assert_eq!(after, before + 1),
            Err(SecretError::Cancelled { .. }) => assert_eq!(after, before),
            Err(SecretError::UncertainWrite { .. }) => assert!(after <= before + 1),
            Err(error) => panic!("unexpected race result: {error}"),
        }
    }
}

struct SequencedBackend {
    calls: AtomicUsize,
    first_completed: AtomicBool,
    first_started: Semaphore,
    release_first: Semaphore,
}

#[async_trait]
impl SecretResolver for SequencedBackend {
    fn descriptor(&self) -> SecretBackendDescriptor {
        SecretBackendDescriptor {
            id: backend_id("sequence"),
            kind: "test".into(),
        }
    }

    async fn probe(&self) -> Result<SecretBackendStatus, SecretError> {
        Ok(SecretBackendStatus::Available)
    }

    async fn resolve(
        &self,
        locator: &SecretLocator,
        _context: &SecretAccessContext,
    ) -> Result<ResolvedSecret, SecretError> {
        Err(SecretError::NotFound {
            reference: yakshed_secrets::SecretReferenceSummary::from(&SecretReference {
                backend_id: backend_id("sequence"),
                locator: locator.clone(),
            }),
        })
    }
}

#[async_trait]
impl SecretAdministrator for SequencedBackend {
    fn backend_id(&self) -> SecretBackendId {
        backend_id("sequence")
    }

    async fn put(
        &self,
        _locator: &SecretLocator,
        _value: &SecretString,
        _options: PutSecretOptions,
    ) -> Result<PutSecretOutcome, SecretError> {
        let call = self.calls.fetch_add(1, Ordering::SeqCst);
        if call == 0 {
            self.first_started.add_permits(1);
            self.release_first.acquire().await.unwrap().forget();
            self.first_completed.store(true, Ordering::SeqCst);
            Ok(PutSecretOutcome::Written)
        } else {
            assert!(self.first_completed.load(Ordering::SeqCst));
            Ok(PutSecretOutcome::Replaced)
        }
    }

    async fn delete(&self, _locator: &SecretLocator) -> Result<DeleteSecretOutcome, SecretError> {
        Ok(DeleteSecretOutcome::NotFound)
    }
}

#[tokio::test]
async fn same_reference_operations_serialize_and_second_observes_first_completion() {
    let backend = Arc::new(SequencedBackend {
        calls: AtomicUsize::new(0),
        first_completed: AtomicBool::new(false),
        first_started: Semaphore::new(0),
        release_first: Semaphore::new(0),
    });
    let connections = Arc::new(vec![connection(
        CONNECTION_A,
        "connection-a",
        vec![secret_binding("provider.api_key", "sequence", "shared/key")],
    )]);
    let broker = Arc::new(
        CredentialBroker::new(
            [(
                backend_id("sequence"),
                SecretBackendHandle {
                    resolver: backend.clone(),
                    administrator: Some(backend.clone()),
                },
            )],
            &connections,
            Arc::new(AuditLog::default()),
            Duration::from_secs(1),
        )
        .unwrap(),
    );
    let ctx = context(CONNECTION_A, "provider.api_key", "request-a");

    let first = {
        let broker = broker.clone();
        let connections = connections.clone();
        let ctx = ctx.clone();
        tokio::spawn(async move {
            broker
                .put(
                    &connections,
                    &connections[0].credentials[0],
                    &ctx,
                    &SecretString::from("first-canary".to_owned()),
                    PutSecretOptions::NO_OVERWRITE,
                    &BrokerCancellation::default(),
                )
                .await
        })
    };
    backend.first_started.acquire().await.unwrap().forget();
    let second = {
        let broker = broker.clone();
        let connections = connections.clone();
        tokio::spawn(async move {
            broker
                .put(
                    &connections,
                    &connections[0].credentials[0],
                    &ctx,
                    &SecretString::from("second-canary".to_owned()),
                    PutSecretOptions::OVERWRITE,
                    &BrokerCancellation::default(),
                )
                .await
        })
    };
    tokio::task::yield_now().await;
    assert_eq!(backend.calls.load(Ordering::SeqCst), 1);
    backend.release_first.add_permits(1);
    assert_eq!(first.await.unwrap().unwrap(), PutSecretOutcome::Written);
    assert_eq!(second.await.unwrap().unwrap(), PutSecretOutcome::Replaced);
}

#[tokio::test]
async fn pre_dispatch_mutation_cancellation_is_certain_and_does_not_call_backend() {
    let backend = Arc::new(SequencedBackend {
        calls: AtomicUsize::new(0),
        first_completed: AtomicBool::new(false),
        first_started: Semaphore::new(0),
        release_first: Semaphore::new(0),
    });
    let connections = Arc::new(vec![connection(
        CONNECTION_A,
        "connection-a",
        vec![secret_binding("provider.api_key", "sequence", "shared/key")],
    )]);
    let audit = Arc::new(AuditLog::default());
    let broker = Arc::new(
        CredentialBroker::new(
            [(
                backend_id("sequence"),
                SecretBackendHandle {
                    resolver: backend.clone(),
                    administrator: Some(backend.clone()),
                },
            )],
            &connections,
            audit.clone(),
            Duration::from_secs(1),
        )
        .unwrap(),
    );
    let first = {
        let broker = broker.clone();
        let connections = connections.clone();
        tokio::spawn(async move {
            broker
                .put(
                    &connections,
                    &connections[0].credentials[0],
                    &context(CONNECTION_A, "provider.api_key", "request-first"),
                    &SecretString::from("first-canary"),
                    PutSecretOptions::NO_OVERWRITE,
                    &BrokerCancellation::default(),
                )
                .await
        })
    };
    backend.first_started.acquire().await.unwrap().forget();

    let cancellation = BrokerCancellation::default();
    let second = {
        let broker = broker.clone();
        let connections = connections.clone();
        let cancellation = cancellation.clone();
        tokio::spawn(async move {
            broker
                .put(
                    &connections,
                    &connections[0].credentials[0],
                    &context(CONNECTION_A, "provider.api_key", "request-second"),
                    &SecretString::from("second-canary"),
                    PutSecretOptions::OVERWRITE,
                    &cancellation,
                )
                .await
        })
    };
    tokio::task::yield_now().await;
    cancellation.cancel();
    assert!(matches!(
        second.await.unwrap(),
        Err(SecretError::Cancelled { .. })
    ));
    assert_eq!(backend.calls.load(Ordering::SeqCst), 1);
    assert!(audit.0.lock().unwrap().iter().any(|event| {
        event.request_id == OperationId::new("request-second").unwrap()
            && event.outcome == yakshed_secrets::SecretAuditOutcome::Cancelled
    }));
    backend.release_first.add_permits(1);
    assert_eq!(first.await.unwrap().unwrap(), PutSecretOutcome::Written);
}

#[tokio::test]
#[cfg(feature = "dev-secrets")]
async fn connection_and_slot_namespaces_support_lifecycle_and_restart() {
    let connections = vec![
        connection(
            CONNECTION_A,
            "connection-a",
            vec![
                secret_binding("provider.api_key", "memory", "a/key"),
                secret_binding("provider.secondary_key", "memory", "a/secondary"),
            ],
        ),
        connection(
            CONNECTION_B,
            "connection-b",
            vec![secret_binding("provider.api_key", "memory", "b/key")],
        ),
    ];
    let backend = Arc::new(MemorySecretBackend::new(backend_id("memory")));
    let broker = build_broker(
        backend,
        &connections,
        Arc::new(AuditLog::default()),
        Duration::from_secs(1),
    );
    let contexts = [
        context(CONNECTION_A, "provider.api_key", "request-a"),
        context(CONNECTION_A, "provider.secondary_key", "request-b"),
        context(CONNECTION_B, "provider.api_key", "request-c"),
    ];
    let locations = [(0, 0), (0, 1), (1, 0)];
    let values = ["a-canary", "a-secondary-canary", "b-canary"];

    for ((connection_index, binding_index), (context, value)) in
        locations.into_iter().zip(contexts.iter().zip(values))
    {
        broker
            .put(
                &connections,
                &connections[connection_index].credentials[binding_index],
                context,
                &SecretString::from(value.to_owned()),
                PutSecretOptions::NO_OVERWRITE,
                &BrokerCancellation::default(),
            )
            .await
            .unwrap();
    }
    for ((connection_index, binding_index), (context, expected)) in
        locations.into_iter().zip(contexts.iter().zip(values))
    {
        let CredentialResolution::Secret(secret) = broker
            .resolve(
                &connections,
                &connections[connection_index].credentials[binding_index],
                context,
                &BrokerCancellation::default(),
            )
            .await
            .unwrap()
        else {
            panic!("expected secret-backed credential")
        };
        assert!(secret.expose(|value| value == expected));
    }
    assert_eq!(
        broker
            .put(
                &connections,
                &connections[0].credentials[0],
                &contexts[0],
                &SecretString::from("rotated-canary".to_owned()),
                PutSecretOptions::OVERWRITE,
                &BrokerCancellation::default(),
            )
            .await
            .unwrap(),
        PutSecretOutcome::Replaced
    );
    assert_eq!(
        broker
            .delete(
                &connections,
                &connections[0].credentials[0],
                &contexts[0],
                &BrokerCancellation::default(),
            )
            .await
            .unwrap(),
        DeleteSecretOutcome::Deleted
    );
    assert_eq!(
        broker
            .status(
                &connections,
                &connections[0].credentials[1],
                &contexts[1],
                &BrokerCancellation::default(),
            )
            .await
            .unwrap(),
        CredentialStatus::Present
    );

    let restarted = build_broker(
        Arc::new(MemorySecretBackend::new(backend_id("memory"))),
        &connections,
        Arc::new(AuditLog::default()),
        Duration::from_secs(1),
    );
    assert_eq!(
        restarted
            .status(
                &connections,
                &connections[1].credentials[0],
                &contexts[2],
                &BrokerCancellation::default(),
            )
            .await
            .unwrap(),
        CredentialStatus::Missing
    );
}

#[test]
fn delivery_curates_runtime_environment_and_drops_ambient_credentials() {
    let resolved = ResolvedSecret::new(
        SecretString::from(CANARY.to_owned()),
        yakshed_secrets::ResolvedSecretSource {
            backend: backend_id("memory"),
        },
        None,
    );
    let ambient = HashMap::from([
        (OsString::from("PATH"), OsString::from("/usr/bin")),
        (OsString::from("HOME"), OsString::from("/tmp/home")),
        (OsString::from("LANG"), OsString::from("en_US.UTF-8")),
        (
            OsString::from("OPENAI_API_KEY"),
            OsString::from("ambient-secret"),
        ),
        (OsString::from("UNRELATED"), OsString::from("drop-me")),
    ]);
    let environment = shape_process_environment(&ambient, "TEST_API_KEY", &resolved).unwrap();
    environment.expose(|environment| {
        assert_eq!(environment.len(), 4);
        assert!(environment.contains_key(&OsString::from("PATH")));
        assert!(environment.contains_key(&OsString::from("HOME")));
        assert!(environment.contains_key(&OsString::from("LANG")));
        assert!(environment.contains_key(&OsString::from("TEST_API_KEY")));
        assert!(!environment.contains_key(&OsString::from("OPENAI_API_KEY")));
        assert!(!environment.contains_key(&OsString::from("UNRELATED")));
    });
    assert!(shape_process_environment(&ambient, "BAD=NAME", &resolved).is_err());
}

#[test]
fn controlled_environment_application_clears_inherited_variables() {
    let resolved = ResolvedSecret::new(
        SecretString::from(CANARY.to_owned()),
        yakshed_secrets::ResolvedSecretSource {
            backend: backend_id("memory"),
        },
        None,
    );
    let environment = shape_process_environment(
        &HashMap::from([(OsString::from("PATH"), OsString::from("/usr/bin"))]),
        "TEST_API_KEY",
        &resolved,
    )
    .unwrap();
    let mut command = std::process::Command::new("/usr/bin/env");
    command.env("AMBIENT_FAKE_CREDENTIAL", "must-be-cleared");
    environment.apply_to(&mut command);
    let output = command.output().unwrap();
    assert!(output.status.success());
    let names = String::from_utf8(output.stdout)
        .unwrap()
        .lines()
        .filter_map(|line| line.split_once('=').map(|(name, _)| name.to_owned()))
        .collect::<Vec<_>>();
    assert!(names.iter().any(|name| name == "PATH"));
    assert!(names.iter().any(|name| name == "TEST_API_KEY"));
    assert!(!names.iter().any(|name| name == "AMBIENT_FAKE_CREDENTIAL"));
}

#[test]
fn formatted_errors_and_audit_metadata_never_include_canary_material() {
    let error = SecretError::BackendFailure {
        backend: backend_id("memory"),
        redacted_message: CANARY.into(),
    };
    assert!(!format!("{error} {error:?}").contains(CANARY));

    let event = SecretAuditEvent {
        connection_id: CONNECTION_A.parse::<ConnectionId>().unwrap(),
        slot: CredentialSlot::new("provider.api_key").unwrap(),
        purpose: SecretAccessPurpose::ValidateCredential,
        request_id: OperationId::new("request-a").unwrap(),
        backend: Some(backend_id("memory")),
        operation: SecretOperation::Resolve,
        outcome: yakshed_secrets::SecretAuditOutcome::Succeeded,
    };
    assert!(!format!("{event:?}").contains(CANARY));
}
