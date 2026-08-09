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
use tokio::sync::Semaphore;
use yakshed_domain::{ConnectionId, CredentialSlot, OperationId};
use yakshed_secrets::{
    BrokerCancellation, CredentialBinding, CredentialBindingRecord, CredentialBroker,
    CredentialDelivery, CredentialResolution, CredentialStatus, DelegatedAuthority,
    DeleteSecretOutcome, InvalidBindingReason, MemorySecretBackend, MemorySecretFault,
    PutSecretOptions, PutSecretOutcome, ResolvedSecret, SecretAccessContext, SecretAccessPurpose,
    SecretAdministrator, SecretAuditEvent, SecretAuditSink, SecretBackendDescriptor,
    SecretBackendHandle, SecretBackendId, SecretBackendStatus, SecretError, SecretLocator,
    SecretOperation, SecretReference, SecretResolver, shape_process_environment,
};

const CANARY: &str = "yakshed-canary-a7f91b3e-secret";

fn backend_id(value: &str) -> SecretBackendId {
    SecretBackendId::new(value).unwrap()
}

fn locator(value: &str) -> SecretLocator {
    SecretLocator::new(value).unwrap()
}

fn context(connection: &str, slot: &str, request: &str) -> SecretAccessContext {
    SecretAccessContext {
        connection_id: ConnectionId::new(connection).unwrap(),
        slot: CredentialSlot::new(slot).unwrap(),
        purpose: SecretAccessPurpose::StartHarness,
        request_id: OperationId::new(request).unwrap(),
    }
}

fn secret_binding(
    connection: &str,
    slot: &str,
    backend: &str,
    locator: &str,
) -> CredentialBindingRecord {
    CredentialBindingRecord {
        connection_id: ConnectionId::new(connection).unwrap(),
        slot: CredentialSlot::new(slot).unwrap(),
        binding: CredentialBinding::Secret {
            reference: SecretReference {
                backend_id: backend_id(backend),
                locator: self::locator(locator),
            },
        },
        delivery: CredentialDelivery::ProcessEnvironment {
            variable: "TEST_API_KEY".into(),
        },
    }
}

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

fn build_broker(
    backend: Arc<MemorySecretBackend>,
    bindings: Vec<CredentialBindingRecord>,
    audit: Arc<dyn SecretAuditSink>,
    timeout: Duration,
) -> CredentialBroker {
    CredentialBroker::new(
        HashMap::from([(backend.descriptor().id, handle(backend))]),
        bindings,
        audit,
        timeout,
    )
    .unwrap()
}

#[test]
fn locator_and_backend_validation_rejects_unsafe_values() {
    assert!(SecretLocator::new("").is_err());
    assert!(SecretLocator::new("line\nbreak").is_err());
    assert!(SecretLocator::new("x".repeat(4097)).is_err());
    assert!(SecretBackendId::new("bad backend").is_err());
    assert_eq!(locator("opaque/path").as_str(), "opaque/path");
}

#[tokio::test]
async fn memory_backend_maps_faults_and_preserves_uncertain_write() {
    let backend = MemorySecretBackend::new(backend_id("memory"));
    let ctx = context("connection-a", "provider.api_key", "request-a");
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
    backend.plan_faults([MemorySecretFault::LockedOrDenied]);
    assert!(matches!(
        backend.resolve(&loc, &ctx).await,
        Err(SecretError::LockedOrDenied { .. })
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

#[tokio::test]
async fn broker_maps_not_found_exists_unsupported_unavailable_timeout_and_cancellation() {
    let id = backend_id("memory");
    let backend = Arc::new(MemorySecretBackend::new(id.clone()));
    let binding = secret_binding("connection-a", "provider.api_key", "memory", "a/key");
    let ctx = context("connection-a", "provider.api_key", "request-a");
    let audit = Arc::new(AuditLog::default());
    let broker = build_broker(
        backend.clone(),
        vec![binding.clone()],
        audit.clone(),
        Duration::from_millis(20),
    );

    assert_eq!(
        broker
            .status(&ctx, &BrokerCancellation::default())
            .await
            .unwrap(),
        CredentialStatus::Missing
    );
    assert_eq!(
        broker
            .put(
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
                &ctx,
                &SecretString::from("different-canary".to_owned()),
                PutSecretOptions::NO_OVERWRITE,
                &BrokerCancellation::default(),
            )
            .await,
        Err(SecretError::AlreadyExists { .. })
    ));
    backend.plan_faults([MemorySecretFault::LockedOrDenied]);
    assert!(matches!(
        broker.resolve(&ctx, &BrokerCancellation::default()).await,
        Err(SecretError::LockedOrDenied { .. })
    ));
    backend.plan_faults([MemorySecretFault::UncertainWrite]);
    assert!(matches!(
        broker
            .put(
                &ctx,
                &SecretString::from(CANARY.to_owned()),
                PutSecretOptions::OVERWRITE,
                &BrokerCancellation::default(),
            )
            .await,
        Err(SecretError::TimedOut { .. })
    ));
    assert_eq!(
        broker
            .status(&ctx, &BrokerCancellation::default())
            .await
            .unwrap(),
        CredentialStatus::Present
    );

    let read_only = CredentialBroker::new(
        HashMap::from([(
            id.clone(),
            SecretBackendHandle {
                resolver: backend.clone(),
                administrator: None,
            },
        )]),
        [binding.clone()],
        Arc::new(AuditLog::default()),
        Duration::from_secs(1),
    )
    .unwrap();
    assert!(matches!(
        read_only
            .put(
                &ctx,
                &SecretString::from(CANARY.to_owned()),
                PutSecretOptions::OVERWRITE,
                &BrokerCancellation::default(),
            )
            .await,
        Err(SecretError::UnsupportedOperation { .. })
    ));

    let unavailable = CredentialBroker::new(
        HashMap::new(),
        [secret_binding(
            "connection-a",
            "provider.api_key",
            "missing-backend",
            "a/key",
        )],
        Arc::new(AuditLog::default()),
        Duration::from_secs(1),
    )
    .unwrap();
    assert!(matches!(
        unavailable
            .resolve(&ctx, &BrokerCancellation::default())
            .await,
        Err(SecretError::BackendUnavailable { .. })
    ));

    backend.plan_faults([MemorySecretFault::Timeout]);
    assert!(matches!(
        broker.resolve(&ctx, &BrokerCancellation::default()).await,
        Err(SecretError::TimedOut { .. })
    ));
    backend.plan_faults([MemorySecretFault::Timeout]);
    let cancellation = BrokerCancellation::default();
    cancellation.cancel();
    assert!(matches!(
        broker.resolve(&ctx, &cancellation).await,
        Err(SecretError::Cancelled { .. })
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
            writable: false,
        }
    }

    async fn probe(&self) -> Result<SecretBackendStatus, SecretError> {
        self.0.fetch_add(1, Ordering::SeqCst);
        panic!("delegated or rejected binding touched backend")
    }

    async fn resolve(
        &self,
        _locator: &SecretLocator,
        _context: &SecretAccessContext,
    ) -> Result<ResolvedSecret, SecretError> {
        self.0.fetch_add(1, Ordering::SeqCst);
        panic!("delegated or rejected binding touched backend")
    }
}

#[tokio::test]
async fn delegated_disabled_and_invalid_bindings_fail_closed_without_backend_access() {
    let resolver = Arc::new(PanicResolver(AtomicUsize::new(0)));
    let delegated = CredentialBindingRecord {
        connection_id: ConnectionId::new("delegated-connection").unwrap(),
        slot: CredentialSlot::new("codex.account").unwrap(),
        binding: CredentialBinding::Delegated {
            authority: DelegatedAuthority("codex-app-server".into()),
        },
        delivery: CredentialDelivery::HarnessManaged,
    };
    let disabled = CredentialBindingRecord {
        connection_id: ConnectionId::new("disabled-connection").unwrap(),
        slot: CredentialSlot::new("provider.api_key").unwrap(),
        binding: CredentialBinding::Disabled,
        delivery: CredentialDelivery::ProviderNative,
    };
    let broker = CredentialBroker::new(
        HashMap::from([(
            backend_id("panic"),
            SecretBackendHandle {
                resolver: resolver.clone(),
                administrator: None,
            },
        )]),
        [delegated, disabled],
        Arc::new(AuditLog::default()),
        Duration::from_secs(1),
    )
    .unwrap();

    assert!(matches!(
        broker
            .resolve(
                &context("delegated-connection", "codex.account", "request-d"),
                &BrokerCancellation::default(),
            )
            .await
            .unwrap(),
        CredentialResolution::Delegated(_)
    ));
    assert!(matches!(
        broker
            .resolve(
                &context("disabled-connection", "provider.api_key", "request-x"),
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
                &context("unknown", "provider.api_key", "request-u"),
                &BrokerCancellation::default(),
            )
            .await,
        Err(SecretError::InvalidBinding {
            reason: InvalidBindingReason::UnknownConnection,
            ..
        })
    ));
    assert!(matches!(
        broker
            .resolve(
                &context("delegated-connection", "wrong.slot", "request-s"),
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
            writable: true,
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
            reference: SecretReference {
                backend_id: backend_id("sequence"),
                locator: locator.clone(),
            }
            .summary(),
        })
    }
}

#[async_trait]
impl SecretAdministrator for SequencedBackend {
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
    let binding = secret_binding("connection-a", "provider.api_key", "sequence", "shared/key");
    let broker = Arc::new(
        CredentialBroker::new(
            HashMap::from([(
                backend_id("sequence"),
                SecretBackendHandle {
                    resolver: backend.clone(),
                    administrator: Some(backend.clone()),
                },
            )]),
            [binding],
            Arc::new(AuditLog::default()),
            Duration::from_secs(1),
        )
        .unwrap(),
    );
    let ctx = context("connection-a", "provider.api_key", "request-a");

    let first = {
        let broker = broker.clone();
        let ctx = ctx.clone();
        tokio::spawn(async move {
            broker
                .put(
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
        tokio::spawn(async move {
            broker
                .put(
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
async fn independent_namespaces_support_lifecycle_and_memory_restart_semantics() {
    let bindings = vec![
        secret_binding("connection-a", "provider.api_key", "memory", "a/key"),
        secret_binding("connection-b", "provider.api_key", "memory", "b/key"),
        secret_binding(
            "connection-a",
            "provider.secondary_key",
            "memory",
            "a/secondary-key",
        ),
    ];
    let backend = Arc::new(MemorySecretBackend::new(backend_id("memory")));
    let broker = build_broker(
        backend,
        bindings.clone(),
        Arc::new(AuditLog::default()),
        Duration::from_secs(1),
    );
    let a = context("connection-a", "provider.api_key", "request-a");
    let b = context("connection-b", "provider.api_key", "request-b");
    let secondary = context("connection-a", "provider.secondary_key", "request-c");

    for (context, value) in [
        (&a, "connection-a-canary"),
        (&b, "connection-b-canary"),
        (&secondary, "connection-a-secondary-canary"),
    ] {
        broker
            .put(
                context,
                &SecretString::from(value.to_owned()),
                PutSecretOptions::NO_OVERWRITE,
                &BrokerCancellation::default(),
            )
            .await
            .unwrap();
    }
    for (context, expected) in [
        (&a, "connection-a-canary"),
        (&b, "connection-b-canary"),
        (&secondary, "connection-a-secondary-canary"),
    ] {
        let CredentialResolution::Secret(secret) = broker
            .resolve(context, &BrokerCancellation::default())
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
                &a,
                &SecretString::from("rotated-a-canary".to_owned()),
                PutSecretOptions::OVERWRITE,
                &BrokerCancellation::default(),
            )
            .await
            .unwrap(),
        PutSecretOutcome::Replaced
    );
    assert_eq!(
        broker
            .delete(&a, &BrokerCancellation::default())
            .await
            .unwrap(),
        DeleteSecretOutcome::Deleted
    );
    assert_eq!(
        broker
            .status(&a, &BrokerCancellation::default())
            .await
            .unwrap(),
        CredentialStatus::Missing
    );
    assert_eq!(
        broker
            .status(&b, &BrokerCancellation::default())
            .await
            .unwrap(),
        CredentialStatus::Present
    );
    assert_eq!(
        broker
            .status(&secondary, &BrokerCancellation::default())
            .await
            .unwrap(),
        CredentialStatus::Present
    );

    let restarted = build_broker(
        Arc::new(MemorySecretBackend::new(backend_id("memory"))),
        bindings,
        Arc::new(AuditLog::default()),
        Duration::from_secs(1),
    );
    assert_eq!(
        restarted
            .status(&b, &BrokerCancellation::default())
            .await
            .unwrap(),
        CredentialStatus::Missing
    );
}

#[test]
fn delivery_is_pure_and_includes_only_the_deliberate_environment_and_credential() {
    let resolved = ResolvedSecret::new(
        SecretString::from(CANARY.to_owned()),
        yakshed_secrets::ResolvedSecretSource {
            backend: backend_id("memory"),
        },
        None,
    );
    let environment =
        shape_process_environment(&HashMap::new(), "TEST_API_KEY", &resolved).unwrap();
    environment.expose(|environment| {
        assert_eq!(environment.len(), 1);
        assert!(
            environment
                .get(&OsString::from("TEST_API_KEY"))
                .is_some_and(|value| value == CANARY)
        );
    });
    assert!(shape_process_environment(&HashMap::new(), "BAD=NAME", &resolved).is_err());
}

#[test]
fn formatted_errors_and_audit_metadata_never_include_canary_material() {
    let error = SecretError::BackendFailure {
        backend: backend_id("memory"),
        redacted_message: CANARY.into(),
    };
    let rendered = format!("{error} {error:?}");
    assert!(!rendered.contains(CANARY));

    let event = SecretAuditEvent {
        connection_id: ConnectionId::new("connection-a").unwrap(),
        slot: CredentialSlot::new("provider.api_key").unwrap(),
        purpose: SecretAccessPurpose::ValidateCredential,
        request_id: OperationId::new("request-a").unwrap(),
        backend: Some(backend_id("memory")),
        operation: SecretOperation::Resolve,
        outcome: yakshed_secrets::SecretAuditOutcome::Succeeded,
    };
    assert!(!format!("{event:?}").contains(CANARY));
}
