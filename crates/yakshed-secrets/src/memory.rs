use std::{collections::HashMap, collections::VecDeque, future::pending, sync::Mutex};

use async_trait::async_trait;
use secrecy::{ExposeSecret, SecretString};

use crate::{
    DeleteSecretOutcome, PutSecretOptions, PutSecretOutcome, ResolvedSecret, ResolvedSecretSource,
    SecretAccessContext, SecretAdministrator, SecretBackendDescriptor, SecretBackendId,
    SecretBackendStatus, SecretError, SecretLocator, SecretOperation, SecretReferenceSummary,
    SecretResolver,
};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum MemorySecretFault {
    NotFound,
    LockedOrDenied,
    Timeout,
    FailNextWrite,
    UncertainWrite,
}

pub struct MemorySecretBackend {
    id: SecretBackendId,
    values: Mutex<HashMap<SecretLocator, SecretString>>,
    faults: Mutex<VecDeque<MemorySecretFault>>,
}

impl MemorySecretBackend {
    pub fn new(id: SecretBackendId) -> Self {
        Self {
            id,
            values: Mutex::new(HashMap::new()),
            faults: Mutex::new(VecDeque::new()),
        }
    }

    pub fn plan_faults(&self, faults: impl IntoIterator<Item = MemorySecretFault>) {
        self.faults.lock().unwrap().extend(faults);
    }

    pub fn remaining_faults(&self) -> usize {
        self.faults.lock().unwrap().len()
    }

    fn summary(&self, locator: &SecretLocator) -> SecretReferenceSummary {
        SecretReferenceSummary {
            backend: self.id.clone(),
            locator: locator.clone(),
        }
    }

    fn next_fault(&self, operation: SecretOperation) -> Option<MemorySecretFault> {
        let mut faults = self.faults.lock().unwrap();
        match faults.front() {
            Some(MemorySecretFault::NotFound) if operation == SecretOperation::Probe => None,
            Some(MemorySecretFault::FailNextWrite | MemorySecretFault::UncertainWrite)
                if operation != SecretOperation::Put =>
            {
                None
            }
            Some(_) => faults.pop_front(),
            None => None,
        }
    }

    async fn apply_common_fault(
        &self,
        locator: Option<&SecretLocator>,
        operation: SecretOperation,
    ) -> Result<Option<MemorySecretFault>, SecretError> {
        let Some(fault) = self.next_fault(operation) else {
            return Ok(None);
        };
        match fault {
            MemorySecretFault::NotFound => Err(SecretError::NotFound {
                reference: self.summary(locator.expect("locator required for not-found fault")),
            }),
            MemorySecretFault::LockedOrDenied => Err(SecretError::LockedOrDenied {
                backend: self.id.clone(),
                remediation: None,
            }),
            MemorySecretFault::Timeout => pending().await,
            write_fault => Ok(Some(write_fault)),
        }
    }
}

#[async_trait]
impl SecretResolver for MemorySecretBackend {
    fn descriptor(&self) -> SecretBackendDescriptor {
        SecretBackendDescriptor {
            id: self.id.clone(),
            kind: "memory".into(),
        }
    }

    async fn probe(&self) -> Result<SecretBackendStatus, SecretError> {
        self.apply_common_fault(None, SecretOperation::Probe)
            .await?;
        Ok(SecretBackendStatus::Available)
    }

    async fn resolve(
        &self,
        locator: &SecretLocator,
        _context: &SecretAccessContext,
    ) -> Result<ResolvedSecret, SecretError> {
        self.apply_common_fault(Some(locator), SecretOperation::Resolve)
            .await?;
        let values = self.values.lock().unwrap();
        let value = values.get(locator).ok_or_else(|| SecretError::NotFound {
            reference: self.summary(locator),
        })?;
        Ok(ResolvedSecret::new(
            SecretString::from(value.expose_secret().to_owned()),
            ResolvedSecretSource {
                backend: self.id.clone(),
            },
            None,
        ))
    }
}

#[async_trait]
impl SecretAdministrator for MemorySecretBackend {
    fn backend_id(&self) -> SecretBackendId {
        self.id.clone()
    }

    async fn put(
        &self,
        locator: &SecretLocator,
        value: &SecretString,
        options: PutSecretOptions,
    ) -> Result<PutSecretOutcome, SecretError> {
        let fault = self
            .apply_common_fault(Some(locator), SecretOperation::Put)
            .await?;
        if fault == Some(MemorySecretFault::FailNextWrite) {
            return Err(SecretError::BackendFailure {
                backend: self.id.clone(),
                redacted_message: "planned memory write failure".into(),
            });
        }

        let mut values = self.values.lock().unwrap();
        let existed = values.contains_key(locator);
        if existed && !options.overwrite {
            return Err(SecretError::AlreadyExists {
                reference: self.summary(locator),
            });
        }
        values.insert(
            locator.clone(),
            SecretString::from(value.expose_secret().to_owned()),
        );
        if fault == Some(MemorySecretFault::UncertainWrite) {
            return Err(SecretError::TimedOut {
                backend: self.id.clone(),
            });
        }
        Ok(if existed {
            PutSecretOutcome::Replaced
        } else {
            PutSecretOutcome::Written
        })
    }

    async fn delete(&self, locator: &SecretLocator) -> Result<DeleteSecretOutcome, SecretError> {
        self.apply_common_fault(Some(locator), SecretOperation::Delete)
            .await?;
        Ok(if self.values.lock().unwrap().remove(locator).is_some() {
            DeleteSecretOutcome::Deleted
        } else {
            DeleteSecretOutcome::NotFound
        })
    }
}
