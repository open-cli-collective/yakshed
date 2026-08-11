use std::{path::PathBuf, time::Duration};

use provider_codex::{CodexAdapter, CodexRuntimeKey, CodexRuntimeSpec};
use yakshed_domain::ApprovalDecision;
use yakshed_harness::{
    HarnessAdapter, HarnessCredentialDelivery, HarnessError, HarnessEvent, HarnessInput,
    HarnessRunTerminal, ProviderResponse, RunOptions, RuntimeHandle, RuntimePath, SessionQuery,
    StartSessionSpec,
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
    let test = adapter("oversized", 1024, None);
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
            assert_eq!(native.sanitized_raw().len(), 1024);
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
async fn unsupported_and_malformed_server_requests_are_rejected_and_stream_continues() {
    let test = adapter("request_boundary", 1024 * 1024, None);
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

    let mut unknown = false;
    let mut malformed = false;
    loop {
        match next(&mut stream).await {
            HarnessEvent::Unknown { item_type, .. } if item_type == "codex/future/request" => {
                unknown = true;
            }
            HarnessEvent::MalformedNativePayload { item_type, .. }
                if item_type == "item/tool/requestUserInput" =>
            {
                malformed = true;
            }
            HarnessEvent::RunTerminal { .. } => break,
            _ => {}
        }
    }
    assert!(unknown);
    assert!(malformed);
}

#[tokio::test]
async fn unknown_run_identity_never_projects_or_registers_an_approval() {
    let test = adapter("uncorrelated_identity", 1024 * 1024, None);
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

    let mut uncorrelated = false;
    loop {
        match next(&mut stream).await {
            HarnessEvent::Unknown {
                run: None,
                item_type,
                ..
            } if item_type == "item/agentMessage/delta" => uncorrelated = true,
            HarnessEvent::MessageDelta { .. } | HarnessEvent::ApprovalRequested { .. } => {
                panic!("uncorrelated provider data was projected onto the active run")
            }
            HarnessEvent::RunTerminal { .. } => break,
            _ => {}
        }
    }
    assert!(uncorrelated);
}

#[tokio::test]
async fn structurally_malformed_notification_is_visible_before_terminal() {
    let test = adapter("structural_malformed", 1024 * 1024, None);
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
        HarnessEvent::MalformedNativePayload { .. }
    ));
    assert!(matches!(
        next(&mut stream).await,
        HarnessEvent::RunTerminal { .. }
    ));
}

#[tokio::test]
async fn failed_provider_response_write_is_fatal_after_uncertain_reply() {
    let test = adapter("response_disconnect", 1024 * 1024, None);
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
    let response = ProviderResponse::Approval(ApprovalDecision::Approved);
    assert!(matches!(
        test.adapter
            .respond_to_request(request.clone(), response.clone())
            .await,
        Err(HarnessError::OutcomeUnknown {
            operation: "provider/request/respond"
        })
    ));
    assert!(matches!(
        next(&mut stream).await,
        HarnessEvent::RunTerminal {
            state: HarnessRunTerminal::Crashed { .. },
            ..
        }
    ));
    assert!(matches!(
        test.adapter.capabilities(&test.runtime).await,
        Err(HarnessError::Disconnected)
    ));
    assert!(matches!(
        test.adapter.respond_to_request(request, response).await,
        Err(HarnessError::NotFound {
            entity: "provider request",
            ..
        })
    ));
}

#[tokio::test]
async fn failed_client_request_write_settles_pending_work_and_disconnects() {
    let test = adapter("client_write_failure", 1024 * 1024, None);
    let mut stream = test.adapter.subscribe().unwrap();
    let session = session(&test).await;
    let run = test
        .adapter
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

    let listing = test.adapter.list_sessions(
        &test.runtime,
        SessionQuery {
            after: None,
            limit: 10,
        },
    );
    tokio::pin!(listing);
    loop {
        tokio::select! {
            result = &mut listing => panic!("listing unexpectedly settled: {result:?}"),
            event = next(&mut stream) => {
                if matches!(event, HarnessEvent::Unknown { ref item_type, .. } if item_type == "test/clientRequestPending") {
                    break;
                }
            }
        }
    }

    assert!(matches!(
        test.adapter
            .steer(&run, HarnessInput::new("steer").unwrap())
            .await,
        Err(HarnessError::OutcomeUnknown {
            operation: "turn/steer"
        })
    ));
    assert!(matches!(listing.await, Err(HarnessError::Disconnected)));
    assert!(matches!(
        next(&mut stream).await,
        HarnessEvent::RunTerminal {
            state: HarnessRunTerminal::Crashed { .. },
            ..
        }
    ));
    assert!(matches!(
        test.adapter.capabilities(&test.runtime).await,
        Err(HarnessError::Disconnected)
    ));
}

#[tokio::test]
async fn pinned_file_change_approval_without_started_at_reaches_the_client() {
    let test = adapter("file_approval", 1024 * 1024, None);
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
        event => panic!("expected file approval, got {event:?}"),
    };
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

#[tokio::test]
async fn malformed_mutation_ack_is_uncertain_without_a_phantom_run() {
    let test = adapter("malformed_turn_ack", 1024 * 1024, None);
    let mut stream = test.adapter.subscribe().unwrap();
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
    assert!(
        tokio::time::timeout(Duration::from_millis(10), stream.recv())
            .await
            .is_err()
    );
    test.adapter.capabilities(&test.runtime).await.unwrap();
}

#[tokio::test]
async fn empty_steer_ack_is_outcome_unknown() {
    let test = adapter("malformed_steer_ack", 1024 * 1024, None);
    let mut stream = test.adapter.subscribe().unwrap();
    let session = session(&test).await;
    let run = test
        .adapter
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
        test.adapter
            .steer(&run, HarnessInput::new("steer").unwrap())
            .await,
        Err(HarnessError::OutcomeUnknown {
            operation: "turn/steer"
        })
    ));
    test.adapter.shutdown().await.unwrap();
}

#[tokio::test]
async fn malformed_terminal_emits_visibility_and_terminal_before_retirement() {
    let test = adapter("malformed_terminal", 1024 * 1024, None);
    let mut stream = test.adapter.subscribe().unwrap();
    let session = session(&test).await;
    let run = test
        .adapter
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
        HarnessEvent::MalformedNativePayload { .. }
    ));
    assert!(matches!(
        next(&mut stream).await,
        HarnessEvent::RunTerminal {
            state: HarnessRunTerminal::Crashed { .. },
            ..
        }
    ));
    assert!(matches!(
        test.adapter
            .steer(&run, HarnessInput::new("too late").unwrap())
            .await,
        Err(HarnessError::NotFound { entity: "run", .. })
    ));
    test.adapter.shutdown().await.unwrap();
}

#[tokio::test]
async fn shutdown_settles_runs_mutations_and_provider_requests_before_acknowledging() {
    let test = adapter("shutdown_settlement", 1024 * 1024, None);
    let mut stream = test.adapter.subscribe().unwrap();
    let session = session(&test).await;
    test.adapter
        .start_run(
            &session,
            HarnessInput::new("first").unwrap(),
            RunOptions::default(),
        )
        .await
        .unwrap();
    assert!(matches!(
        next(&mut stream).await,
        HarnessEvent::RunAccepted { .. }
    ));
    let approval = loop {
        if let HarnessEvent::ApprovalRequested { request, .. } = next(&mut stream).await {
            break request;
        }
    };

    let second = test.adapter.start_run(
        &session,
        HarnessInput::new("second").unwrap(),
        RunOptions::default(),
    );
    tokio::pin!(second);
    loop {
        tokio::select! {
            result = &mut second => panic!("second turn unexpectedly settled: {result:?}"),
            event = next(&mut stream) => {
                if matches!(event, HarnessEvent::Unknown { ref item_type, .. } if item_type == "test/secondTurnReceived") {
                    break;
                }
            }
        }
    }

    let (shutdown, second) = tokio::join!(test.adapter.shutdown(), &mut second);
    shutdown.unwrap();
    assert!(matches!(
        second,
        Err(HarnessError::OutcomeUnknown {
            operation: "turn/start"
        })
    ));
    assert!(matches!(
        next(&mut stream).await,
        HarnessEvent::RunTerminal { .. }
    ));
    assert!(matches!(
        test.adapter
            .respond_to_request(
                approval,
                ProviderResponse::Approval(ApprovalDecision::Approved)
            )
            .await,
        Err(HarnessError::NotFound {
            entity: "provider request",
            ..
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
