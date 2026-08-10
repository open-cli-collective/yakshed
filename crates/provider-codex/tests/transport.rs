use std::{path::PathBuf, time::Duration};

use provider_codex::{CodexAdapter, CodexRuntimeKey, CodexRuntimeSpec};
use yakshed_domain::ApprovalDecision;
use yakshed_harness::{
    HarnessAdapter, HarnessCredentialDelivery, HarnessError, HarnessEvent, HarnessInput,
    HarnessRunTerminal, ProviderResponse, RunOptions, RuntimeHandle, RuntimePath, StartSessionSpec,
};

struct TestAdapter {
    adapter: CodexAdapter,
    runtime: RuntimeHandle,
    workspace: PathBuf,
    _root: tempfile::TempDir,
}

fn adapter(
    scenario: &str,
    max_frame_size: usize,
    pid_file: Option<&std::path::Path>,
) -> TestAdapter {
    let root = tempfile::tempdir().unwrap();
    let codex_home = root.path().join("codex-home");
    let workspace = root.path().join("workspace");
    std::fs::create_dir_all(&codex_home).unwrap();
    std::fs::create_dir_all(&workspace).unwrap();
    let runtime = RuntimeHandle::new("codex-transport-runtime").unwrap();
    let key = CodexRuntimeKey {
        connection_id: "0193f26e-7a72-7000-8000-00000000ccc2".parse().unwrap(),
        binary_digest: "fake-codex-schema-0.147.0".to_owned(),
        codex_home,
        execution_runtime: "local-test".to_owned(),
    };
    let script = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fake_codex.py");
    let mut args = vec![script.display().to_string(), scenario.to_owned()];
    if let Some(pid_file) = pid_file {
        args.push(pid_file.display().to_string());
    }
    let mut spec = CodexRuntimeSpec::local(runtime.clone(), key, PathBuf::from("python3"), args);
    spec.max_frame_size = max_frame_size;
    spec.redactions
        .push("YAKSHED_CREDENTIAL_CANARY_DO_NOT_EMIT".to_owned());
    TestAdapter {
        adapter: CodexAdapter::new(spec).unwrap(),
        runtime,
        workspace,
        _root: root,
    }
}

async fn session(test: &TestAdapter) -> yakshed_harness::ProviderSession {
    test.adapter
        .start_session(
            &test.runtime,
            StartSessionSpec {
                working_directory: RuntimePath::new(test.workspace.display().to_string()).unwrap(),
                title: "transport test".to_owned(),
            },
        )
        .await
        .unwrap()
}

async fn next(stream: &mut yakshed_harness::ProviderEventStream) -> HarnessEvent {
    tokio::time::timeout(Duration::from_secs(1), stream.recv())
        .await
        .unwrap()
        .unwrap()
}

#[test]
fn codex_declares_harness_managed_account_authentication() {
    let test = adapter("interruptible", 1024 * 1024, None);
    assert!(
        test.adapter
            .credential_requirements()
            .iter()
            .any(|requirement| {
                requirement.slot.as_str() == "codex.account"
                    && requirement.delivery == HarnessCredentialDelivery::HarnessManaged
            })
    );
}

#[tokio::test]
async fn split_frames_and_rapid_batches_preserve_event_order() {
    let test = adapter("transport_split_batch", 1024 * 1024, None);
    let mut stream = test.adapter.subscribe().unwrap();
    let session = session(&test).await;
    test.adapter
        .start_run(
            &session,
            HarnessInput::new("run").unwrap(),
            RunOptions::default(),
        )
        .await
        .unwrap();
    let mut kinds = Vec::new();
    loop {
        let event = next(&mut stream).await;
        kinds.push(event.to_string());
        if matches!(event, HarnessEvent::RunTerminal { .. }) {
            break;
        }
    }
    assert_eq!(
        kinds,
        [
            "run_accepted",
            "message_delta",
            "message_delta",
            "message_completed",
            "file_mutation",
            "command_output",
            "run_terminal",
        ]
    );
}

#[tokio::test]
async fn stderr_flood_is_continuously_drained_into_a_bounded_buffer() {
    let test = adapter("stderr_flood", 1024 * 1024, None);
    let mut stream = test.adapter.subscribe().unwrap();
    let session = session(&test).await;
    test.adapter
        .start_run(
            &session,
            HarnessInput::new("run").unwrap(),
            RunOptions::default(),
        )
        .await
        .unwrap();
    assert!(matches!(
        next(&mut stream).await,
        HarnessEvent::RunAccepted { .. }
    ));
    assert!(matches!(
        next(&mut stream).await,
        HarnessEvent::RunTerminal {
            state: HarnessRunTerminal::Completed,
            ..
        }
    ));
    let diagnostics = test.adapter.diagnostics().await.unwrap();
    assert!(!diagnostics.is_empty());
    assert!(diagnostics.len() <= 32);
    assert!(
        diagnostics
            .iter()
            .all(|diagnostic| { !diagnostic.contains("YAKSHED_CREDENTIAL_CANARY_DO_NOT_EMIT") })
    );
}

#[tokio::test]
async fn oversized_frame_is_bounded_preserved_and_does_not_stop_the_stream() {
    let test = adapter("oversized", 256, None);
    let mut stream = test.adapter.subscribe().unwrap();
    let session = session(&test).await;
    test.adapter
        .start_run(
            &session,
            HarnessInput::new("run").unwrap(),
            RunOptions::default(),
        )
        .await
        .unwrap();
    assert!(matches!(
        next(&mut stream).await,
        HarnessEvent::RunAccepted { .. }
    ));
    match next(&mut stream).await {
        HarnessEvent::MalformedNativePayload {
            item_type, native, ..
        } => {
            assert_eq!(item_type, "codex.oversized-frame");
            assert_eq!(native.sanitized_raw().len(), 256);
        }
        event => panic!("expected oversized-frame event, got {event:?}"),
    }
    assert!(matches!(
        next(&mut stream).await,
        HarnessEvent::RunTerminal {
            state: HarnessRunTerminal::Completed,
            ..
        }
    ));
}

#[tokio::test]
async fn disconnect_after_turn_start_write_is_outcome_unknown() {
    let test = adapter("early_before_ack", 1024 * 1024, None);
    let session = session(&test).await;
    assert!(matches!(
        test.adapter
            .start_run(
                &session,
                HarnessInput::new("run").unwrap(),
                RunOptions::default(),
            )
            .await,
        Err(HarnessError::OutcomeUnknown {
            operation: "turn/start"
        })
    ));
}

#[tokio::test]
async fn declined_approval_uses_the_pinned_response_shape() {
    let test = adapter("approval_declined", 1024 * 1024, None);
    let mut stream = test.adapter.subscribe().unwrap();
    let session = session(&test).await;
    test.adapter
        .start_run(
            &session,
            HarnessInput::new("run").unwrap(),
            RunOptions::default(),
        )
        .await
        .unwrap();
    assert!(matches!(
        next(&mut stream).await,
        HarnessEvent::RunAccepted { .. }
    ));
    let request = match next(&mut stream).await {
        HarnessEvent::ApprovalRequested { request, .. } => request,
        event => panic!("expected approval, got {event:?}"),
    };
    assert!(matches!(
        next(&mut stream).await,
        HarnessEvent::MessageDelta { .. }
    ));
    test.adapter
        .respond_to_request(
            request,
            ProviderResponse::Approval(ApprovalDecision::Denied),
        )
        .await
        .unwrap();
    assert!(matches!(
        next(&mut stream).await,
        HarnessEvent::RunTerminal {
            state: HarnessRunTerminal::Completed,
            ..
        }
    ));
}

#[cfg(unix)]
#[tokio::test]
async fn explicit_shutdown_kills_and_reaps_the_runtime_process() {
    let pid_root = tempfile::tempdir().unwrap();
    let pid_file = pid_root.path().join("fake.pid");
    let test = adapter("interruptible", 1024 * 1024, Some(&pid_file));
    test.adapter.capabilities(&test.runtime).await.unwrap();
    let pid = std::fs::read_to_string(pid_file)
        .unwrap()
        .parse::<libc::pid_t>()
        .unwrap();

    test.adapter.shutdown().await.unwrap();

    assert_eq!(unsafe { libc::kill(pid, 0) }, -1);
    assert_eq!(
        std::io::Error::last_os_error().raw_os_error(),
        Some(libc::ESRCH)
    );
    assert!(matches!(
        test.adapter.capabilities(&test.runtime).await,
        Err(HarnessError::Disconnected)
    ));
}
