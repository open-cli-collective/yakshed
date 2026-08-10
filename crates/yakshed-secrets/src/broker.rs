use std::{
    collections::{HashMap, HashSet},
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
use yakshed_domain::{Connection, CredentialBinding, CredentialBindingRecord};

use crate::{
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
        tokio::pin!(notified);
        notified.as_mut().enable();
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
    Delegated(String),
    Disabled,
}

pub enum CredentialResolution {
    Secret(ResolvedSecret),
    Delegated(String),
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
    locks: KeyedSecretLocks,
    audit: Arc<dyn SecretAuditSink>,
    timeout: Duration,
}

impl CredentialBroker {
    pub fn new(
        backends: impl IntoIterator<Item = (SecretBackendId, SecretBackendHandle)>,
        connections: &[Connection],
        audit: Arc<dyn SecretAuditSink>,
        timeout: Duration,
    ) -> Result<Self, SecretError> {
        Self::validate_bindings(connections)?;
        let mut registry = HashMap::new();
        for (registered_id, handle) in backends {
            let descriptor_id = handle.resolver.descriptor().id;
            if registered_id != descriptor_id {
                return Err(SecretError::BackendFailure {
                    backend: registered_id,
                    redacted_message: "backend registry key does not match descriptor".into(),
                });
            }
            if handle
                .administrator
                .as_ref()
                .is_some_and(|administrator| administrator.backend_id() != descriptor_id)
            {
                return Err(SecretError::BackendFailure {
                    backend: descriptor_id,
                    redacted_message: "administrator identity does not match resolver".into(),
                });
            }
            if registry.insert(descriptor_id.clone(), handle).is_some() {
                return Err(SecretError::BackendFailure {
                    backend: descriptor_id,
                    redacted_message: "duplicate backend ID".into(),
                });
            }
        }
        Ok(Self {
            backends: registry,
            locks: KeyedSecretLocks::default(),
            audit,
            timeout,
        })
    }

    pub fn validate_bindings(connections: &[Connection]) -> Result<(), SecretError> {
        let mut references = HashSet::new();
        for connection in connections {
            connection
                .validate()
                .map_err(|_| SecretError::BackendFailure {
                    backend: broker_id(),
                    redacted_message: "invalid connection credential configuration".into(),
                })?;
            for binding in &connection.credentials {
                if let Some(reference) = secret_reference(&binding.binding)
                    && !references.insert(reference.clone())
                {
                    return Err(SecretError::BackendFailure {
                        backend: broker_id(),
                        redacted_message: "duplicate secret reference".into(),
                    });
                }
            }
        }
        Ok(())
    }

    fn current_binding<'a>(
        &self,
        connections: &'a [Connection],
        supplied: &'a CredentialBindingRecord,
        context: &SecretAccessContext,
        operation: SecretOperation,
    ) -> Result<&'a CredentialBinding, SecretError> {
        let result = (|| {
            Self::validate_bindings(connections)?;
            let connection = connections
                .iter()
                .find(|connection| connection.id == context.connection_id)
                .ok_or_else(|| self.invalid(context, InvalidBindingReason::UnknownConnection))?;
            let current = connection
                .credentials
                .iter()
                .find(|binding| binding.slot == context.slot)
                .ok_or_else(|| self.invalid(context, InvalidBindingReason::UnknownSlot))?;
            if current != supplied {
                return Err(self.invalid(context, InvalidBindingReason::StaleBinding));
            }
            Ok(&current.binding)
        })();
        result.inspect_err(|_| {
            self.audit(context, None, operation, SecretAuditOutcome::Rejected);
        })
    }

    fn invalid(&self, context: &SecretAccessContext, reason: InvalidBindingReason) -> SecretError {
        SecretError::InvalidBinding {
            connection_id: context.connection_id,
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

    fn audited_backend(
        &self,
        reference: &SecretReference,
        context: &SecretAccessContext,
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
            connection_id: context.connection_id,
            slot: context.slot.clone(),
            purpose: context.purpose,
            request_id: context.request_id.clone(),
            backend,
            operation,
            outcome,
        });
    }

    async fn run<T, F>(
        &self,
        context: &SecretAccessContext,
        reference: &SecretReference,
        operation: SecretOperation,
        cancellation: &BrokerCancellation,
        action: impl FnOnce() -> F,
    ) -> Result<T, SecretError>
    where
        F: Future<Output = Result<T, SecretError>>,
    {
        let backend = reference.backend_id.clone();
        if cancellation.is_cancelled() {
            self.audit(
                context,
                Some(backend.clone()),
                operation,
                SecretAuditOutcome::Cancelled,
            );
            return Err(SecretError::Cancelled { backend });
        }
        let lock = self.locks.get(reference);
        let dispatched = Arc::new(AtomicBool::new(false));
        let worker_dispatched = Arc::clone(&dispatched);
        let work = async {
            let _guard = lock.lock().await;
            worker_dispatched.store(true, Ordering::Release);
            action().await
        };
        let result = tokio::select! {
            biased;
            _ = cancellation.cancelled() => Err(SecretError::Cancelled { backend: backend.clone() }),
            result = tokio::time::timeout(self.timeout, work) => result.unwrap_or_else(|_| Err(SecretError::TimedOut { backend: backend.clone() })),
        };
        let result = if matches!(operation, SecretOperation::Put | SecretOperation::Delete)
            && dispatched.load(Ordering::Acquire)
            && matches!(
                &result,
                Err(SecretError::TimedOut { .. } | SecretError::Cancelled { .. })
            ) {
            Err(SecretError::UncertainWrite {
                backend: backend.clone(),
            })
        } else {
            result
        };
        let outcome = match &result {
            Ok(_) => SecretAuditOutcome::Succeeded,
            Err(SecretError::NotFound { .. }) => SecretAuditOutcome::NotFound,
            Err(SecretError::UncertainWrite { .. }) => SecretAuditOutcome::Uncertain,
            Err(SecretError::TimedOut { .. }) => SecretAuditOutcome::TimedOut,
            Err(SecretError::Cancelled { .. }) => SecretAuditOutcome::Cancelled,
            Err(_) => SecretAuditOutcome::Failed,
        };
        self.audit(context, Some(backend), operation, outcome);
        result
    }

    pub async fn status(
        &self,
        connections: &[Connection],
        binding: &CredentialBindingRecord,
        context: &SecretAccessContext,
        cancellation: &BrokerCancellation,
    ) -> Result<CredentialStatus, SecretError> {
        match self.current_binding(connections, binding, context, SecretOperation::Probe)? {
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
            binding => {
                let reference = required_reference(binding)?;
                let backend = self.audited_backend(reference, context, SecretOperation::Resolve)?;
                match self
                    .run(
                        context,
                        reference,
                        SecretOperation::Resolve,
                        cancellation,
                        || backend.resolver.resolve(&reference.locator, context),
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
        connections: &[Connection],
        binding: &CredentialBindingRecord,
        context: &SecretAccessContext,
        cancellation: &BrokerCancellation,
    ) -> Result<CredentialResolution, SecretError> {
        match self.current_binding(connections, binding, context, SecretOperation::Resolve)? {
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
            binding => {
                let reference = required_reference(binding)?;
                let backend = self.audited_backend(reference, context, SecretOperation::Resolve)?;
                self.run(
                    context,
                    reference,
                    SecretOperation::Resolve,
                    cancellation,
                    || backend.resolver.resolve(&reference.locator, context),
                )
                .await
                .map(CredentialResolution::Secret)
            }
        }
    }

    pub async fn put(
        &self,
        connections: &[Connection],
        binding: &CredentialBindingRecord,
        context: &SecretAccessContext,
        value: &SecretString,
        options: PutSecretOptions,
        cancellation: &BrokerCancellation,
    ) -> Result<PutSecretOutcome, SecretError> {
        let current = self.current_binding(connections, binding, context, SecretOperation::Put)?;
        if !matches!(current, CredentialBinding::Secret { .. }) {
            self.audit(
                context,
                None,
                SecretOperation::Put,
                SecretAuditOutcome::Rejected,
            );
            return Err(self.invalid(
                context,
                if matches!(current, CredentialBinding::Disabled) {
                    InvalidBindingReason::Disabled
                } else {
                    InvalidBindingReason::NotSecretBacked
                },
            ));
        }
        let reference = required_reference(current)?;
        let backend = self.audited_backend(reference, context, SecretOperation::Put)?;
        let Some(administrator) = backend.administrator.as_ref() else {
            self.audit(
                context,
                Some(reference.backend_id.clone()),
                SecretOperation::Put,
                SecretAuditOutcome::Failed,
            );
            return Err(SecretError::UnsupportedOperation {
                backend: reference.backend_id.clone(),
                operation: SecretOperation::Put,
            });
        };
        self.run(
            context,
            reference,
            SecretOperation::Put,
            cancellation,
            || administrator.put(&reference.locator, value, options),
        )
        .await
    }

    pub async fn delete(
        &self,
        connections: &[Connection],
        binding: &CredentialBindingRecord,
        context: &SecretAccessContext,
        cancellation: &BrokerCancellation,
    ) -> Result<DeleteSecretOutcome, SecretError> {
        let current =
            self.current_binding(connections, binding, context, SecretOperation::Delete)?;
        if !matches!(current, CredentialBinding::Secret { .. }) {
            self.audit(
                context,
                None,
                SecretOperation::Delete,
                SecretAuditOutcome::Rejected,
            );
            return Err(self.invalid(
                context,
                if matches!(current, CredentialBinding::Disabled) {
                    InvalidBindingReason::Disabled
                } else {
                    InvalidBindingReason::NotSecretBacked
                },
            ));
        }
        let reference = required_reference(current)?;
        let backend = self.audited_backend(reference, context, SecretOperation::Delete)?;
        let Some(administrator) = backend.administrator.as_ref() else {
            self.audit(
                context,
                Some(reference.backend_id.clone()),
                SecretOperation::Delete,
                SecretAuditOutcome::Failed,
            );
            return Err(SecretError::UnsupportedOperation {
                backend: reference.backend_id.clone(),
                operation: SecretOperation::Delete,
            });
        };
        self.run(
            context,
            reference,
            SecretOperation::Delete,
            cancellation,
            || administrator.delete(&reference.locator),
        )
        .await
    }
}

fn broker_id() -> SecretBackendId {
    SecretBackendId::new("broker").unwrap()
}

fn secret_reference(binding: &CredentialBinding) -> Option<&SecretReference> {
    let CredentialBinding::Secret { reference } = binding else {
        return None;
    };
    Some(reference)
}

fn required_reference(binding: &CredentialBinding) -> Result<&SecretReference, SecretError> {
    secret_reference(binding).ok_or_else(|| SecretError::BackendFailure {
        backend: broker_id(),
        redacted_message: "credential is not secret-backed".into(),
    })
}

pub struct ChildProcessEnvironment {
    variables: HashMap<OsString, OsString>,
}

impl ChildProcessEnvironment {
    pub fn expose<R>(&self, use_environment: impl FnOnce(&HashMap<OsString, OsString>) -> R) -> R {
        use_environment(&self.variables)
    }

    pub fn apply_to(&self, command: &mut std::process::Command) {
        command.env_clear().envs(&self.variables);
    }
}

pub fn shape_process_environment(
    ambient: &HashMap<OsString, OsString>,
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
    let mut variables = ambient
        .iter()
        .filter(|(name, _)| name.to_str().is_some_and(is_runtime_environment_variable))
        .map(|(name, value)| (name.clone(), value.clone()))
        .collect::<HashMap<_, _>>();
    secret.expose(|value| {
        variables.insert(OsString::from(variable), OsString::from(value));
    });
    Ok(ChildProcessEnvironment { variables })
}

fn is_runtime_environment_variable(name: &str) -> bool {
    matches!(
        name,
        "PATH" | "HOME" | "LANG" | "LANGUAGE" | "TMPDIR" | "TMP" | "TEMP"
    ) || name.starts_with("LC_")
}
