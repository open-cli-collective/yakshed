use std::{
    ffi::OsString,
    io,
    os::unix::process::CommandExt as _,
    process::{ExitStatus, Stdio},
    time::Duration,
};

use async_trait::async_trait;
use secrecy::SecretString;
use tokio::{
    io::{AsyncRead, AsyncReadExt},
    process::{Child, Command},
};
use yakshed_domain::validate_onepassword_locator;

use crate::{
    ONEPASSWORD_BACKEND_KIND, ResolvedSecret, ResolvedSecretSource, SecretAccessContext,
    SecretBackend, SecretBackendConfigurationError, SecretBackendDescriptor, SecretBackendId,
    SecretBackendSettings, SecretBackendStatus, SecretError, SecretLocator, SecretReferenceSummary,
    SecretResolver, validate_backend_configuration,
};

const COMMAND_TIMEOUT: Duration = Duration::from_secs(10);
const CLEANUP_TIMEOUT: Duration = Duration::from_secs(2);
const MAX_OUTPUT_BYTES: usize = 64 * 1024;
const ENV_ALLOWLIST: &[&str] = &[
    "HOME",
    "PATH",
    "XDG_CONFIG_HOME",
    "OP_CONFIG_DIR",
    "OP_SERVICE_ACCOUNT_TOKEN",
    "OP_CONNECT_HOST",
    "OP_CONNECT_TOKEN",
    "SSH_AUTH_SOCK",
];

/// Resolve-only adapter using `op read --no-newline --force op://vault/item/field`.
pub struct OnePasswordBackend {
    id: SecretBackendId,
    executable: OsString,
    account: Option<String>,
    timeout: Duration,
}

impl OnePasswordBackend {
    pub fn from_config(config: &SecretBackend) -> Result<Self, SecretBackendConfigurationError> {
        let SecretBackendSettings::OnePassword {
            account,
            executable,
        } = &config.settings
        else {
            return Err(SecretBackendConfigurationError::WrongKind {
                backend: config.id.clone(),
                expected: ONEPASSWORD_BACKEND_KIND,
            });
        };
        validate_backend_configuration(config, crate::backend_capabilities())?;
        Ok(Self {
            id: config.id.clone(),
            executable: executable.as_deref().unwrap_or("op").into(),
            account: account.clone(),
            timeout: COMMAND_TIMEOUT,
        })
    }

    #[cfg(test)]
    fn with_timeout(mut self, timeout: Duration) -> Self {
        self.timeout = timeout;
        self
    }

    fn command(&self, operation: &str) -> Command {
        let mut command = Command::new(&self.executable);
        command
            .env_clear()
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .kill_on_drop(true);
        for name in ENV_ALLOWLIST {
            if let Some(value) = std::env::var_os(name) {
                command.env(name, value);
            }
        }
        command.env("NO_COLOR", "1");
        command.arg(operation);
        command.as_std_mut().process_group(0);
        command
    }

    async fn run(&self, operation: &str, args: &[&str]) -> Result<SecretBytes, SecretError> {
        let mut command = self.command(operation);
        command.args(args);
        if let Some(account) = &self.account {
            command.arg("--account").arg(account);
        }
        let mut child = command
            .spawn()
            .map_err(|_| SecretError::BackendUnavailable {
                backend: self.id.clone(),
                remediation: Some("install the 1Password CLI".to_owned()),
            })?;
        let process_group = child
            .id()
            .expect("spawned 1Password child has a process ID");
        let stdout = child
            .stdout
            .take()
            .expect("1Password stdout was configured as piped");
        let stderr = child
            .stderr
            .take()
            .expect("1Password stderr was configured as piped");
        let mut child = ManagedChild::new(child, process_group);
        let completed = tokio::time::timeout(self.timeout, async {
            let (status, stdout, stderr) =
                tokio::try_join!(child.wait(), read_bounded(stdout), read_bounded(stderr),)?;
            Ok::<_, io::Error>((status, stdout, stderr))
        })
        .await;
        let cleanup = child.kill_and_reap().await;
        if cleanup.is_err() {
            return Err(SecretError::BackendFailure {
                backend: self.id.clone(),
                redacted_message: "1Password process cleanup failed".to_owned(),
            });
        }
        match completed {
            Err(_) => Err(SecretError::TimedOut {
                backend: self.id.clone(),
            }),
            Ok(Err(_)) => Err(SecretError::BackendFailure {
                backend: self.id.clone(),
                redacted_message: "1Password process failed".to_owned(),
            }),
            Ok(Ok((status, stdout, _stderr))) if status.success() => Ok(stdout),
            Ok(Ok((status, _stdout, stderr))) => {
                Err(classify_exit(&self.id, status, stderr.as_slice()))
            }
        }
    }
}

#[async_trait]
impl SecretResolver for OnePasswordBackend {
    fn descriptor(&self) -> SecretBackendDescriptor {
        SecretBackendDescriptor {
            id: self.id.clone(),
            kind: ONEPASSWORD_BACKEND_KIND.to_owned(),
        }
    }

    async fn probe(&self) -> Result<SecretBackendStatus, SecretError> {
        let _output = self.run("account", &["get", "--format=json"]).await?;
        Ok(SecretBackendStatus::Available)
    }

    async fn resolve(
        &self,
        locator: &SecretLocator,
        _context: &SecretAccessContext,
    ) -> Result<ResolvedSecret, SecretError> {
        validate_onepassword_locator(locator).map_err(|_| SecretError::InvalidLocator {
            backend: self.id.clone(),
            reason: "locator must be an op secret reference".to_owned(),
        })?;
        let bytes = self
            .run("read", &["--no-newline", "--force", locator.as_str()])
            .await?;
        let value = bytes
            .into_secret_string()
            .map_err(|()| SecretError::ProtocolViolation {
                backend: self.id.clone(),
                reason: "1Password returned invalid secret bytes".to_owned(),
            })?;
        Ok(ResolvedSecret::new(
            value,
            ResolvedSecretSource {
                backend: self.id.clone(),
            },
            None,
        ))
    }
}

fn classify_exit(backend: &SecretBackendId, _status: ExitStatus, stderr: &[u8]) -> SecretError {
    if contains_ascii_case_insensitive(stderr, b"not signed in")
        || contains_ascii_case_insensitive(stderr, b"authentication required")
        || contains_ascii_case_insensitive(stderr, b"locked")
    {
        SecretError::Locked {
            backend: backend.clone(),
            remediation: Some("sign in to the 1Password CLI".to_owned()),
        }
    } else if contains_ascii_case_insensitive(stderr, b"not found")
        || contains_ascii_case_insensitive(stderr, b"isn't an item")
    {
        SecretError::NotFound {
            reference: SecretReferenceSummary {
                backend: backend.clone(),
                locator: SecretLocator::new("op://redacted/redacted/redacted")
                    .expect("redacted reference is valid"),
            },
        }
    } else {
        SecretError::BackendFailure {
            backend: backend.clone(),
            redacted_message: "1Password command failed".to_owned(),
        }
    }
}

fn contains_ascii_case_insensitive(haystack: &[u8], needle: &[u8]) -> bool {
    haystack.windows(needle.len()).any(|window| {
        window
            .iter()
            .zip(needle)
            .all(|(left, right)| left.eq_ignore_ascii_case(right))
    })
}

async fn read_bounded(input: impl AsyncRead + Unpin) -> io::Result<SecretBytes> {
    let mut bytes = SecretBytes(Vec::new());
    input
        .take((MAX_OUTPUT_BYTES + 1) as u64)
        .read_to_end(&mut bytes.0)
        .await?;
    if bytes.0.len() > MAX_OUTPUT_BYTES {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "output too large",
        ));
    }
    Ok(bytes)
}

struct SecretBytes(Vec<u8>);

impl SecretBytes {
    fn as_slice(&self) -> &[u8] {
        &self.0
    }

    fn into_secret_string(mut self) -> Result<SecretString, ()> {
        if self.0.is_empty() || std::str::from_utf8(&self.0).is_err() {
            return Err(());
        }
        let bytes = std::mem::take(&mut self.0);
        // SAFETY: UTF-8 was validated above; ownership moves directly into SecretString.
        Ok(SecretString::from(unsafe {
            String::from_utf8_unchecked(bytes)
        }))
    }
}

impl Drop for SecretBytes {
    fn drop(&mut self) {
        self.0.fill(0);
    }
}

struct ManagedChild {
    child: Option<Child>,
    process_group: u32,
}

impl ManagedChild {
    fn new(child: Child, process_group: u32) -> Self {
        Self {
            child: Some(child),
            process_group,
        }
    }

    async fn wait(&mut self) -> io::Result<ExitStatus> {
        self.child
            .as_mut()
            .expect("1Password child is armed")
            .wait()
            .await
    }

    async fn kill_and_reap(&mut self) -> io::Result<()> {
        kill_process_group(self.process_group);
        if let Some(mut child) = self.child.take() {
            let _ = child.start_kill();
            let _ = child.wait().await;
        }
        wait_for_process_group_exit(self.process_group).await
    }
}

impl Drop for ManagedChild {
    fn drop(&mut self) {
        let Some(mut child) = self.child.take() else {
            return;
        };
        kill_process_group(self.process_group);
        let _ = child.start_kill();
        if let Ok(runtime) = tokio::runtime::Handle::try_current() {
            runtime.spawn(async move {
                let _ = child.wait().await;
            });
        }
    }
}

fn kill_process_group(process_group: u32) {
    if let Ok(process_group) = i32::try_from(process_group) {
        // SAFETY: negative PID targets only the child-created process group.
        let _ = unsafe { libc::kill(-process_group, libc::SIGKILL) };
    }
}

async fn wait_for_process_group_exit(process_group: u32) -> io::Result<()> {
    let process_group = i32::try_from(process_group)
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "invalid process group"))?;
    tokio::time::timeout(CLEANUP_TIMEOUT, async move {
        loop {
            if unsafe { libc::kill(-process_group, 0) } == -1
                && io::Error::last_os_error().raw_os_error() == Some(libc::ESRCH)
            {
                return;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "process group survived cleanup"))
}

#[cfg(test)]
mod tests {
    use std::{fs, os::unix::fs::PermissionsExt as _, path::PathBuf};

    use serde_json::Value;
    use tempfile::TempDir;

    use super::*;
    use crate::SecretAccessPurpose;
    use yakshed_domain::{ConnectionId, CredentialSlot, OperationId};

    const CANARY: &str = "onepassword-secret-canary-731";

    struct FakeOp {
        _root: TempDir,
        executable: PathBuf,
    }

    impl FakeOp {
        fn new() -> Self {
            let root = tempfile::tempdir().unwrap();
            let executable = root.path().join("fake_op.py");
            fs::copy(
                concat!(env!("CARGO_MANIFEST_DIR"), "/../../scripts/fake_op.py"),
                &executable,
            )
            .unwrap();
            fs::set_permissions(&executable, fs::Permissions::from_mode(0o700)).unwrap();
            Self {
                _root: root,
                executable,
            }
        }

        fn backend(&self, account: Option<&str>) -> OnePasswordBackend {
            OnePasswordBackend::from_config(&SecretBackend {
                id: SecretBackendId::new("onepassword-test").unwrap(),
                settings: SecretBackendSettings::OnePassword {
                    account: account.map(str::to_owned),
                    executable: Some(self.executable.to_string_lossy().into_owned()),
                },
            })
            .unwrap()
        }

        fn invocation(&self) -> Value {
            serde_json::from_slice(
                &fs::read(self.executable.with_file_name("fake_op.invocation.json")).unwrap(),
            )
            .unwrap()
        }
    }

    fn locator(field: &str) -> SecretLocator {
        SecretLocator::new(format!("op://vault/item/{field}")).unwrap()
    }

    fn context() -> SecretAccessContext {
        SecretAccessContext {
            connection_id: "0193f26e-7a72-7000-8000-00000000cc01"
                .parse::<ConnectionId>()
                .unwrap(),
            slot: CredentialSlot::new("provider.api-key").unwrap(),
            purpose: SecretAccessPurpose::ValidateCredential,
            request_id: OperationId::new("onepassword-test").unwrap(),
        }
    }

    #[tokio::test]
    async fn fake_op_resolves_without_secret_in_argv_or_ambient_environment() {
        let fake = FakeOp::new();
        let backend = fake.backend(None);

        assert_eq!(
            backend.probe().await.unwrap(),
            SecretBackendStatus::Available
        );
        assert!(
            backend
                .resolve(&locator("password"), &context())
                .await
                .unwrap()
                .expose(|value| value == CANARY)
        );
        let invocation = fake.invocation();
        let rendered = invocation.to_string();
        assert!(rendered.contains("--no-newline"));
        assert!(rendered.contains("--force"));
        assert!(rendered.contains("op://vault/item/password"));
        assert!(!rendered.contains(CANARY));
        for name in invocation["environment"].as_array().unwrap() {
            let name = name.as_str().unwrap();
            assert!(
                name == "NO_COLOR"
                    || name == "LC_CTYPE"
                    || name == "__CF_USER_TEXT_ENCODING"
                    || ENV_ALLOWLIST.contains(&name),
                "unexpected fake-op environment variable: {name}"
            );
        }
    }

    #[tokio::test]
    async fn fake_op_maps_missing_locked_absent_and_malformed_without_leaks() {
        let fake = FakeOp::new();
        let backend = fake.backend(None);
        let missing = match backend.resolve(&locator("missing"), &context()).await {
            Ok(_) => panic!("missing secret resolved"),
            Err(error) => error,
        };
        assert!(matches!(missing, SecretError::NotFound { .. }));
        assert!(!format!("{missing:?}").contains("vault/item"));

        let locked = fake.backend(Some("locked")).probe().await.unwrap_err();
        assert!(matches!(locked, SecretError::Locked { .. }));
        assert!(!format!("{locked:?}").contains(CANARY));

        let absent = OnePasswordBackend::from_config(&SecretBackend {
            id: SecretBackendId::new("missing-op").unwrap(),
            settings: SecretBackendSettings::OnePassword {
                account: None,
                executable: Some(
                    fake.executable
                        .with_file_name("absent")
                        .to_string_lossy()
                        .into_owned(),
                ),
            },
        })
        .unwrap()
        .probe()
        .await
        .unwrap_err();
        assert!(matches!(absent, SecretError::BackendUnavailable { .. }));

        let malformed = match backend.resolve(&locator("malformed"), &context()).await {
            Ok(_) => panic!("malformed secret resolved"),
            Err(error) => error,
        };
        assert!(matches!(malformed, SecretError::ProtocolViolation { .. }));

        let failed = match backend.resolve(&locator("failure"), &context()).await {
            Ok(_) => panic!("failed secret resolved"),
            Err(error) => error,
        };
        assert!(matches!(failed, SecretError::BackendFailure { .. }));
        assert!(!format!("{failed:?}").contains(CANARY));
    }

    #[tokio::test]
    async fn hanging_fake_op_process_group_is_killed_and_reaped() {
        let fake = FakeOp::new();
        let backend = fake.backend(None).with_timeout(Duration::from_secs(1));
        let error = match backend.resolve(&locator("hang"), &context()).await {
            Ok(_) => panic!("hanging secret resolved"),
            Err(error) => error,
        };
        assert!(matches!(error, SecretError::TimedOut { .. }));
        let pids = fs::read_to_string(fake.executable.with_file_name("fake_op.pids")).unwrap();
        for pid in pids
            .split_whitespace()
            .map(|pid| pid.parse::<i32>().unwrap())
        {
            assert_eq!(unsafe { libc::kill(pid, 0) }, -1);
            assert_eq!(io::Error::last_os_error().raw_os_error(), Some(libc::ESRCH));
        }
    }

    #[test]
    fn capability_is_resolve_only_and_locator_shape_is_closed() {
        let capability = crate::backend_capabilities()
            .iter()
            .find(|capability| capability.kind == ONEPASSWORD_BACKEND_KIND)
            .unwrap();
        assert_eq!(capability.access, crate::SecretBackendAccess::ResolveOnly);
        assert!(validate_onepassword_locator(&locator("password")).is_ok());
        for invalid in [
            "vault/item/field",
            "op://vault/item",
            "op://vault/item/field/extra",
        ] {
            assert!(validate_onepassword_locator(&SecretLocator::new(invalid).unwrap()).is_err());
        }
    }
}
