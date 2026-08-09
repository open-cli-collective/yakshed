use std::{
    collections::HashMap,
    ffi::OsString,
    future::Future,
    sync::{
        Arc, Mutex, Weak,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

use secrecy::SecretString;
use tokio::sync::{Mutex as AsyncMutex, Notify};
use yakshed_domain::{ConnectionId, CredentialSlot};

use crate::{
    CredentialBinding, CredentialBindingRecord, CredentialDelivery, DelegatedAuthority,
    DeleteSecretOutcome, InvalidBindingReason, PutSecretOptions, PutSecretOutcome, ResolvedSecret,
    SecretAccessContext, SecretAuditEvent, SecretAuditOutcome, SecretAuditSink,
    SecretBackendHandle, SecretBackendId, SecretError, SecretOperation, SecretReference,
};

#[derive(Clone, Default)]
pub struct BrokerCancellation {
    inner: Arc<CancellationState>,
}

#[derive(Default)]
struct CancellationState {
    cancelled: AtomicBool,
    notify: Notify,
}

impl BrokerCancellation {
    pub fn cancel(&self) {
        self.inner.cancelled.store(true, Ordering::Release);
        self.inner.notify.notify_waiters();
    }

    pub fn is_cancelled(&self) -> bool {
        self.inner.cancelled.load(Ordering::Acquire)
    }

    async fn cancelled(&self) {
        if self.is_cancelled() {
            return;
        }
        let notified = self.inner.notify.notified();
        if self.is_cancelled() {
            return;
        }
        notified.await;
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum CredentialStatus {
    Present,
    Missing,
    Delegated(DelegatedAuthority),
    Disabled,
}

pub enum CredentialResolution {
    Secret(ResolvedSecret),
    Delegated(DelegatedAuthority),
}

#[derive(Default)]
struct KeyedSecretLocks {
    locks: Mutex<HashMap<SecretReference, Weak<AsyncMutex<()>>>>,
}

impl KeyedSecretLocks {
    fn get(&self, reference: &SecretReference) -> Arc<AsyncMutex<()>> {
        let mut locks = self.locks.lock().unwrap();
        locks.retain(|_, lock| lock.strong_count() > 0);
        if let Some(lock) = locks.get(reference).and_then(Weak::upgrade) {
            return lock;
        }
        let lock = Arc::new(AsyncMutex::new(()));
        locks.insert(reference.clone(), Arc::downgrade(&lock));
        lock
    }
}

pub struct CredentialBroker {
    backends: HashMap<SecretBackendId, SecretBackendHandle>,
    bindings: HashMap<(ConnectionId, CredentialSlot), CredentialBindingRecord>,
    locks: KeyedSecretLocks,
    audit: Arc<dyn SecretAuditSink>,
    timeout: Duration,
}

impl CredentialBroker {
    pub fn new(
        backends: HashMap<SecretBackendId, SecretBackendHandle>,
        bindings: impl IntoIterator<Item = CredentialBindingRecord>,
        audit: Arc<dyn SecretAuditSink>,
        timeout: Duration,
    ) -> Result<Self, SecretError> {
        let mut indexed = HashMap::new();
        for binding in bindings {
            let key = (binding.connection_id.clone(), binding.slot.clone());
            if indexed.insert(key, binding).is_some() {
                return Err(SecretError::BackendFailure {
                    backend: SecretBackendId::new("broker").unwrap(),
                    redacted_message: "duplicate connection credential slot".into(),
                });
            }
        }
        Ok(Self {
            backends,
            bindings: indexed,
            locks: KeyedSecretLocks::default(),
            audit,
            timeout,
        })
    }

    fn binding(
        &self,
        context: &SecretAccessContext,
    ) -> Result<&CredentialBindingRecord, SecretError> {
        if !self
            .bindings
            .keys()
            .any(|(connection, _)| connection == &context.connection_id)
        {
            return Err(self.invalid(context, InvalidBindingReason::UnknownConnection));
        }
        self.bindings
            .get(&(context.connection_id.clone(), context.slot.clone()))
            .ok_or_else(|| self.invalid(context, InvalidBindingReason::UnknownSlot))
    }

    fn invalid(&self, context: &SecretAccessContext, reason: InvalidBindingReason) -> SecretError {
        SecretError::InvalidBinding {
            connection_id: context.connection_id.clone(),
            slot: context.slot.clone(),
            reason,
        }
    }

    fn backend(&self, reference: &SecretReference) -> Result<&SecretBackendHandle, SecretError> {
        self.backends
            .get(&reference.backend_id)
            .ok_or_else(|| SecretError::BackendUnavailable {
                backend: reference.backend_id.clone(),
                remediation: None,
            })
    }

    fn audited_binding(
        &self,
        context: &SecretAccessContext,
        operation: SecretOperation,
    ) -> Result<&CredentialBindingRecord, SecretError> {
        self.binding(context).inspect_err(|_| {
            self.audit(context, None, operation, SecretAuditOutcome::Rejected);
        })
    }

    fn audited_backend(
        &self,
        context: &SecretAccessContext,
        reference: &SecretReference,
        operation: SecretOperation,
    ) -> Result<&SecretBackendHandle, SecretError> {
        self.backend(reference).inspect_err(|_| {
            self.audit(
                context,
                Some(reference.backend_id.clone()),
                operation,
                SecretAuditOutcome::Failed,
            );
        })
    }

    fn audit(
        &self,
        context: &SecretAccessContext,
        backend: Option<SecretBackendId>,
        operation: SecretOperation,
        outcome: SecretAuditOutcome,
    ) {
        self.audit.record(SecretAuditEvent {
            connection_id: context.connection_id.clone(),
            slot: context.slot.clone(),
            purpose: context.purpose,
            request_id: context.request_id.clone(),
            backend,
            operation,
            outcome,
        });
    }

    async fn run<T>(
        &self,
        context: &SecretAccessContext,
        reference: &SecretReference,
        operation: SecretOperation,
        cancellation: &BrokerCancellation,
        action: impl Future<Output = Result<T, SecretError>>,
    ) -> Result<T, SecretError> {
        let backend = reference.backend_id.clone();
        let lock = self.locks.get(reference);
        let work = async {
            let _guard = lock.lock().await;
            action.await
        };
        let result = tokio::select! {
            _ = cancellation.cancelled() => Err(SecretError::Cancelled { backend: backend.clone() }),
            result = tokio::time::timeout(self.timeout, work) => result.unwrap_or_else(|_| Err(SecretError::TimedOut { backend: backend.clone() })),
        };
        let outcome = match &result {
            Ok(_) => SecretAuditOutcome::Succeeded,
            Err(SecretError::NotFound { .. }) => SecretAuditOutcome::NotFound,
            Err(SecretError::TimedOut { .. }) => SecretAuditOutcome::TimedOut,
            Err(SecretError::Cancelled { .. }) => SecretAuditOutcome::Cancelled,
            Err(_) => SecretAuditOutcome::Failed,
        };
        self.audit(context, Some(backend), operation, outcome);
        result
    }

    pub async fn status(
        &self,
        context: &SecretAccessContext,
        cancellation: &BrokerCancellation,
    ) -> Result<CredentialStatus, SecretError> {
        let binding = self.audited_binding(context, SecretOperation::Probe)?;
        match &binding.binding {
            CredentialBinding::Delegated { authority } => {
                self.audit(
                    context,
                    None,
                    SecretOperation::Probe,
                    SecretAuditOutcome::Delegated,
                );
                Ok(CredentialStatus::Delegated(authority.clone()))
            }
            CredentialBinding::Disabled => {
                self.audit(
                    context,
                    None,
                    SecretOperation::Probe,
                    SecretAuditOutcome::Rejected,
                );
                Ok(CredentialStatus::Disabled)
            }
            CredentialBinding::Secret { reference } => {
                let backend = self.audited_backend(context, reference, SecretOperation::Resolve)?;
                match self
                    .run(
                        context,
                        reference,
                        SecretOperation::Resolve,
                        cancellation,
                        backend.resolver.resolve(&reference.locator, context),
                    )
                    .await
                {
                    Ok(secret) => {
                        drop(secret);
                        Ok(CredentialStatus::Present)
                    }
                    Err(SecretError::NotFound { .. }) => Ok(CredentialStatus::Missing),
                    Err(error) => Err(error),
                }
            }
        }
    }

    pub async fn resolve(
        &self,
        context: &SecretAccessContext,
        cancellation: &BrokerCancellation,
    ) -> Result<CredentialResolution, SecretError> {
        let binding = self.audited_binding(context, SecretOperation::Resolve)?;
        match &binding.binding {
            CredentialBinding::Delegated { authority } => {
                self.audit(
                    context,
                    None,
                    SecretOperation::Resolve,
                    SecretAuditOutcome::Delegated,
                );
                Ok(CredentialResolution::Delegated(authority.clone()))
            }
            CredentialBinding::Disabled => {
                self.audit(
                    context,
                    None,
                    SecretOperation::Resolve,
                    SecretAuditOutcome::Rejected,
                );
                Err(self.invalid(context, InvalidBindingReason::Disabled))
            }
            CredentialBinding::Secret { reference } => {
                let backend = self.audited_backend(context, reference, SecretOperation::Resolve)?;
                self.run(
                    context,
                    reference,
                    SecretOperation::Resolve,
                    cancellation,
                    backend.resolver.resolve(&reference.locator, context),
                )
                .await
                .map(CredentialResolution::Secret)
            }
        }
    }

    pub async fn put(
        &self,
        context: &SecretAccessContext,
        value: &SecretString,
        options: PutSecretOptions,
        cancellation: &BrokerCancellation,
    ) -> Result<PutSecretOutcome, SecretError> {
        let binding = self.audited_binding(context, SecretOperation::Put)?;
        let CredentialBinding::Secret { reference } = &binding.binding else {
            self.audit(
                context,
                None,
                SecretOperation::Put,
                SecretAuditOutcome::Rejected,
            );
            return Err(self.invalid(
                context,
                if matches!(binding.binding, CredentialBinding::Disabled) {
                    InvalidBindingReason::Disabled
                } else {
                    InvalidBindingReason::NotSecretBacked
                },
            ));
        };
        let backend = self.audited_backend(context, reference, SecretOperation::Put)?;
        let administrator = backend.administrator.as_ref().ok_or_else(|| {
            self.audit(
                context,
                Some(reference.backend_id.clone()),
                SecretOperation::Put,
                SecretAuditOutcome::Failed,
            );
            SecretError::UnsupportedOperation {
                backend: reference.backend_id.clone(),
                operation: SecretOperation::Put,
            }
        })?;
        self.run(
            context,
            reference,
            SecretOperation::Put,
            cancellation,
            administrator.put(&reference.locator, value, options),
        )
        .await
    }

    pub async fn delete(
        &self,
        context: &SecretAccessContext,
        cancellation: &BrokerCancellation,
    ) -> Result<DeleteSecretOutcome, SecretError> {
        let binding = self.audited_binding(context, SecretOperation::Delete)?;
        let CredentialBinding::Secret { reference } = &binding.binding else {
            self.audit(
                context,
                None,
                SecretOperation::Delete,
                SecretAuditOutcome::Rejected,
            );
            return Err(self.invalid(
                context,
                if matches!(binding.binding, CredentialBinding::Disabled) {
                    InvalidBindingReason::Disabled
                } else {
                    InvalidBindingReason::NotSecretBacked
                },
            ));
        };
        let backend = self.audited_backend(context, reference, SecretOperation::Delete)?;
        let administrator = backend.administrator.as_ref().ok_or_else(|| {
            self.audit(
                context,
                Some(reference.backend_id.clone()),
                SecretOperation::Delete,
                SecretAuditOutcome::Failed,
            );
            SecretError::UnsupportedOperation {
                backend: reference.backend_id.clone(),
                operation: SecretOperation::Delete,
            }
        })?;
        self.run(
            context,
            reference,
            SecretOperation::Delete,
            cancellation,
            administrator.delete(&reference.locator),
        )
        .await
    }

    pub fn delivery(
        &self,
        context: &SecretAccessContext,
    ) -> Result<&CredentialDelivery, SecretError> {
        Ok(&self.binding(context)?.delivery)
    }
}

pub struct ChildProcessEnvironment {
    variables: HashMap<OsString, OsString>,
}

impl ChildProcessEnvironment {
    pub fn expose<R>(&self, use_environment: impl FnOnce(&HashMap<OsString, OsString>) -> R) -> R {
        use_environment(&self.variables)
    }
}

pub fn shape_process_environment(
    base: &HashMap<OsString, OsString>,
    variable: &str,
    secret: &ResolvedSecret,
) -> Result<ChildProcessEnvironment, &'static str> {
    if variable.is_empty()
        || variable.contains('=')
        || variable.contains('\0')
        || !variable.is_ascii()
    {
        return Err("invalid environment variable name");
    }
    let mut environment = base.clone();
    secret.expose(|value| {
        environment.insert(OsString::from(variable), OsString::from(value));
    });
    Ok(ChildProcessEnvironment {
        variables: environment,
    })
}
