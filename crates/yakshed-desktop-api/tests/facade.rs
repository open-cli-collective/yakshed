use std::{
    collections::HashMap,
    io::Read,
    sync::{Arc, Mutex as StdMutex},
    time::Duration,
};

use async_trait::async_trait;
use provider_mock::{MockHarness, MockHarnessFault, MockRunPlan, MockScriptStep};
use secrecy::SecretString;
use tokio::sync::{Mutex, broadcast};
use yakshed_application::{
    AppStore, ArtifactPort, ArtifactPortError, BeginApprovalResponse, CachePort, CachePortError,
    ConfigChange, ConfigPort, ConfigPortError, ConfigRevision, CreateProject, CreateRun,
    CreateWorkItem, IdGenerator, ListTimeline, NewTimelineItem, OpenArtifactCommand,
    OpenArtifactPayload, PendingApproval, PublicConnection, PublicCredentialBinding,
    PublicCredentialSource, PutConnectionCommand, SecretPort, SecretPortError,
    SetConnectionCredentialCommand, SystemClock, SystemIdGenerator, TimelineBatch, TransitionRun,
};
use yakshed_desktop_api::{
    ApiPorts, DesktopApi, DesktopErrorCode, FrontendEvent, FrontendEventKind, FrontendRunStatus,
};
use yakshed_domain::{
    ApprovalDecision, ArtifactId, ArtifactKind, Connection, ConnectionId, CredentialBinding,
    CredentialBindingRecord, CredentialSlot, NamespacedProviderId, ProviderStateRootId, RunId,
    RunStatus, SecretBackendId, SecretLocator, SecretReference, StreamCursor, UtcTimestamp,
    WorkItemId,
};
use yakshed_harness::{
    HarnessAdapter, HarnessCapabilities, HarnessError, HarnessEvent, HarnessInput,
    HarnessRunTerminal, ProviderEventStream, ProviderRequestHandle, ProviderRequestId,
    ProviderResponse, ProviderRunHandle, ProviderSession, RunOptions, RuntimeHandle, RuntimePath,
    StartSessionSpec,
};
use yakshed_secrets::{
    BrokerCancellation, CredentialBroker, MemorySecretBackend, NoopSecretAuditSink,
    PutSecretOptions, SecretAccessContext, SecretAccessPurpose, SecretBackendHandle, SecretError,
};
use yakshed_store::{
    AppPaths, ArtifactMetadata, ArtifactStore, CacheStore, ConfigStore, SqliteStore,
};

#[tokio::test]
async fn work_item_create_snapshot_round_trip() {
    let fixture = TestFixture::new().await;
    let api = fixture.api_no_run().await;
    let created = api
        .create_work_item(fixture.project_id, "task", None)
        .await
        .unwrap();
    let fetched = api
        .get_work_item_snapshot(created.work_item.id.parse().unwrap())
        .await
        .unwrap();
    let listed = api
        .list_work_items(fixture.project_id, None, 10)
        .await
        .unwrap();
    assert_eq!(created.work_item.id, fetched.work_item.id);
    assert_eq!(created.work_item.revision, fetched.work_item.revision);
    assert!(
        listed
            .items
            .iter()
            .any(|item| item.work_item.id == created.work_item.id)
    );
}

#[tokio::test]
async fn first_snapshot_waits_for_startup_reconciliation() {
    let fixture = TestFixture::new().await;
    let run = fixture
        .store
        .create_run(CreateRun {
            id: fixture.ids.next_run_id(),
            connection_id: fixture.connection_id,
            work_item_id: fixture.work_item_id,
            provider_run: None,
        })
        .await
        .unwrap();
    fixture
        .store
        .transition_run(TransitionRun {
            run_id: run.id,
            expected_current: RunStatus::Starting,
            target: RunStatus::OutcomeUnknown,
            provider_id: Some(NamespacedProviderId::new("mock", "restart-ready").unwrap()),
            occurred_at: UtcTimestamp::from_unix_millis(1),
            audit_event_id: fixture.ids.next_audit_event_id(),
        })
        .await
        .unwrap();
    let api = fixture.api_no_run().await;
    let snapshot = api
        .get_work_item_snapshot(fixture.work_item_id)
        .await
        .unwrap();
    assert!(snapshot.runs.iter().any(|item| {
        item.id == run.id.to_string() && item.status == FrontendRunStatus::Running
    }));
}

#[tokio::test]
async fn recovery_snapshot_exposes_bounded_cursors_for_all_runs_and_approvals() {
    let fixture = TestFixture::new().await;
    let mut runs = Vec::new();
    for _ in 0..201 {
        runs.push(
            fixture
                .store
                .create_run(CreateRun {
                    id: fixture.ids.next_run_id(),
                    connection_id: fixture.connection_id,
                    work_item_id: fixture.work_item_id,
                    provider_run: None,
                })
                .await
                .unwrap(),
        );
    }
    let approval_run = fixture
        .store
        .transition_run(TransitionRun {
            run_id: runs[0].id,
            expected_current: RunStatus::Starting,
            target: RunStatus::Running,
            provider_id: Some(NamespacedProviderId::new("mock", "paged-run").unwrap()),
            occurred_at: UtcTimestamp::from_unix_millis(1),
            audit_event_id: fixture.ids.next_audit_event_id(),
        })
        .await
        .unwrap();
    for index in 0..205 {
        fixture
            .store
            .record_pending_approval(PendingApproval {
                id: fixture.ids.next_approval_request_id(),
                run_id: approval_run.id,
                provider_id: NamespacedProviderId::new("mock", format!("paged/{index}")).unwrap(),
                kind: "command".to_owned(),
                summary: format!("approval {index}"),
            })
            .await
            .unwrap();
    }
    let api = fixture.api_no_run().await;
    api.reconcile_run(approval_run.id).await.unwrap();
    let mut run_after = None;
    let mut run_count = 0;
    let mut pinned_revision = None;
    loop {
        let snapshot = api
            .get_work_item_snapshot_page(fixture.work_item_id, run_after, 50, pinned_revision)
            .await
            .unwrap();
        pinned_revision = Some(snapshot.revision);
        assert!(snapshot.runs.len() <= 50);
        run_count += snapshot.runs.len();
        run_after = snapshot.next_run_after.map(|id| id.parse().unwrap());
        if run_after.is_none() {
            break;
        }
    }
    let mut approval_after = None;
    let mut approval_count = 0;
    loop {
        let page = api
            .get_run_approval_page(
                fixture.work_item_id,
                approval_run.id,
                approval_after,
                50,
                pinned_revision,
            )
            .await
            .unwrap();
        assert!(page.approvals.len() <= 50);
        approval_count += page.approvals.len();
        approval_after = page.next_after.map(|id| id.parse().unwrap());
        if approval_after.is_none() {
            break;
        }
    }
    assert_eq!(run_count, 201);
    assert_eq!(approval_count, 205);
    fixture
        .store
        .create_run(CreateRun {
            id: fixture.ids.next_run_id(),
            connection_id: fixture.connection_id,
            work_item_id: fixture.work_item_id,
            provider_run: None,
        })
        .await
        .unwrap();
    assert_eq!(
        api.get_work_item_snapshot_page(fixture.work_item_id, None, 50, pinned_revision,)
            .await
            .unwrap_err()
            .code,
        DesktopErrorCode::Conflict
    );
}

#[tokio::test]
async fn recovery_matches_user_input_responses_by_request_id() {
    let fixture = TestFixture::new().await;
    let run = fixture
        .store
        .create_run(CreateRun {
            id: fixture.ids.next_run_id(),
            connection_id: fixture.connection_id,
            work_item_id: fixture.work_item_id,
            provider_run: None,
        })
        .await
        .unwrap();
    let run = fixture
        .store
        .transition_run(TransitionRun {
            run_id: run.id,
            expected_current: RunStatus::Starting,
            target: RunStatus::Running,
            provider_id: Some(NamespacedProviderId::new("mock", "input-recovery").unwrap()),
            occurred_at: UtcTimestamp::from_unix_millis(1),
            audit_event_id: fixture.ids.next_audit_event_id(),
        })
        .await
        .unwrap();
    let first = fixture.ids.next_timeline_item_id();
    let second = fixture.ids.next_timeline_item_id();
    fixture
        .store
        .append_timeline_batch(TimelineBatch {
            batch_id: fixture.ids.next_timeline_batch_id(),
            connection_id: fixture.connection_id,
            run_id: run.id,
            source_namespace: "app".to_owned(),
            stream_id: run.id.to_string(),
            expected_stream_revision: StreamCursor::INITIAL,
            items: vec![
                NewTimelineItem {
                    id: first,
                    kind: "user_input_requested".to_owned(),
                    body: "first".to_owned(),
                    provider_id: Some(
                        NamespacedProviderId::new("mock", "input-recovery/request-1").unwrap(),
                    ),
                },
                NewTimelineItem {
                    id: second,
                    kind: "user_input_requested".to_owned(),
                    body: "second".to_owned(),
                    provider_id: Some(
                        NamespacedProviderId::new("mock", "input-recovery/request-2").unwrap(),
                    ),
                },
                NewTimelineItem {
                    id: fixture.ids.next_timeline_item_id(),
                    kind: "user_input_responded".to_owned(),
                    body: second.to_string(),
                    provider_id: None,
                },
            ],
        })
        .await
        .unwrap();
    let api = fixture.api_no_run().await;
    api.reconcile_run(run.id).await.unwrap();
    let pending = api
        .get_pending_user_input_page(fixture.work_item_id, run.id, None, 50, None)
        .await
        .unwrap();
    assert_eq!(pending.inputs.len(), 1);
    assert_eq!(pending.inputs[0].id, first.to_string());
    assert_eq!(pending.inputs[0].prompt, "first");
}

#[tokio::test]
async fn recovery_snapshot_exposes_uncertain_approval_response() {
    let fixture = TestFixture::new().await;
    let run = fixture
        .store
        .create_run(CreateRun {
            id: fixture.ids.next_run_id(),
            connection_id: fixture.connection_id,
            work_item_id: fixture.work_item_id,
            provider_run: None,
        })
        .await
        .unwrap();
    let run = fixture
        .store
        .transition_run(TransitionRun {
            run_id: run.id,
            expected_current: RunStatus::Starting,
            target: RunStatus::Running,
            provider_id: Some(NamespacedProviderId::new("mock", "approval-recovery").unwrap()),
            occurred_at: UtcTimestamp::from_unix_millis(1),
            audit_event_id: fixture.ids.next_audit_event_id(),
        })
        .await
        .unwrap();
    let approval = fixture
        .store
        .record_pending_approval(PendingApproval {
            id: fixture.ids.next_approval_request_id(),
            run_id: run.id,
            provider_id: NamespacedProviderId::new("mock", "approval-recovery/request-1").unwrap(),
            kind: "approval".to_owned(),
            summary: "uncertain".to_owned(),
        })
        .await
        .unwrap();
    fixture
        .store
        .begin_approval_response(BeginApprovalResponse {
            approval_id: approval.id,
            decision: ApprovalDecision::Approved,
            audit_event_id: fixture.ids.next_audit_event_id(),
        })
        .await
        .unwrap();
    let api = fixture.api_no_run().await;
    api.reconcile_run(run.id).await.unwrap();
    let approvals = api
        .get_run_approval_page(fixture.work_item_id, run.id, None, 50, None)
        .await
        .unwrap();
    assert_eq!(approvals.approvals[0].status, "responding");
    assert_eq!(approvals.approvals[0].decision.as_deref(), Some("approved"));
}

#[tokio::test]
async fn full_run_lifecycle_batches_chunked_deltas() {
    let fixture = TestFixture::new().await;
    let api = fixture
        .api_for_plan(MockRunPlan::new(vec![
            MockScriptStep::message("hel"),
            MockScriptStep::message("lo"),
            MockScriptStep::message_completed("hello"),
            MockScriptStep::command_output("git status", "clean"),
            MockScriptStep::file_mutation("notes.txt", "updated"),
            MockScriptStep::complete(),
        ]))
        .await;
    let run_id = api
        .start_run(fixture.work_item_id, fixture.connection_id, "hello")
        .await
        .unwrap();
    wait_for_run_completion(&fixture.store, run_id).await;
    let timeline = fixture
        .store
        .list_timeline_page(ListTimeline {
            run_id,
            after: None,
            limit: 20,
        })
        .await
        .unwrap()
        .items;
    assert!(
        timeline
            .iter()
            .any(|item| item.kind == "message_delta_batch")
    );
    assert!(timeline.iter().any(|item| item.kind == "message_completed"));
    assert!(
        timeline
            .iter()
            .any(|item| item.kind == "command_output_completed")
    );
    assert!(timeline.iter().any(|item| item.kind == "file_mutation"));
}

#[tokio::test]
async fn frontend_snapshots_round_trip_without_provider_identifiers() {
    let fixture = TestFixture::new().await;
    let api = fixture
        .api_for_plan(MockRunPlan::new(vec![
            MockScriptStep::message_completed("safe"),
            MockScriptStep::complete(),
        ]))
        .await;
    let run_id = api
        .start_run(fixture.work_item_id, fixture.connection_id, "boundary")
        .await
        .unwrap();
    wait_for_run_completion(&fixture.store, run_id).await;
    let snapshot = api
        .get_work_item_snapshot(fixture.work_item_id)
        .await
        .unwrap();
    let timeline = api
        .get_work_item_timeline_page(fixture.work_item_id, run_id, None, 50)
        .await
        .unwrap();
    let snapshot_json = serde_json::to_value(&snapshot).unwrap();
    let timeline_json = serde_json::to_value(&timeline).unwrap();
    let _: yakshed_desktop_api::WorkItemSnapshotEnvelope =
        serde_json::from_value(snapshot_json.clone()).unwrap();
    let _: yakshed_desktop_api::WorkItemTimelineEnvelope =
        serde_json::from_value(timeline_json.clone()).unwrap();
    assert!(!format!("{snapshot_json}{timeline_json}").contains("provider"));
}

#[tokio::test]
async fn approval_opened_resolved_continues_run() {
    let fixture = TestFixture::new().await;
    let request = "request-0001".parse::<ProviderRequestId>().unwrap();
    let api = fixture
        .api_for_plan(MockRunPlan::new(vec![
            MockScriptStep::approval(request.clone(), "run action"),
            MockScriptStep::await_response(request),
            MockScriptStep::message_completed("ok"),
            MockScriptStep::complete(),
        ]))
        .await;
    let mut events = api.subscribe_events();
    let run_id = api
        .start_run(fixture.work_item_id, fixture.connection_id, "approve me")
        .await
        .unwrap();
    let approval_opened = wait_for_matching(
        &mut events,
        |event| {
            event.work_item_id == fixture.work_item_id.to_string()
                && matches!(event.kind, FrontendEventKind::ApprovalOpened { .. })
        },
        Duration::from_secs(3),
    )
    .await;
    let FrontendEventKind::ApprovalOpened {
        run_id: opened_run_id,
        approval_id,
    } = approval_opened.kind
    else {
        unreachable!()
    };
    assert_eq!(opened_run_id, run_id.to_string());
    api.resolve_approval(
        approval_id.parse().unwrap(),
        yakshed_domain::ApprovalDecision::Approved,
    )
    .await
    .unwrap();
    let completed =
        wait_for_frontend_status(&mut events, fixture.work_item_id, RunStatus::Completed).await;
    assert_eq!(
        completed.kind,
        FrontendEventKind::RunStatusChanged {
            run_id: run_id.to_string(),
            status: FrontendRunStatus::Completed,
        }
    );
}

#[tokio::test]
async fn user_input_round_trip_and_run_continues() {
    let fixture = TestFixture::new().await;
    let request = "request-0002".parse::<ProviderRequestId>().unwrap();
    let api = fixture
        .api_for_plan(MockRunPlan::new(vec![
            MockScriptStep::user_input(request.clone(), "name?"),
            MockScriptStep::await_response(request),
            MockScriptStep::complete(),
        ]))
        .await;
    let mut events = api.subscribe_events();
    let run_id = api
        .start_run(fixture.work_item_id, fixture.connection_id, "ask")
        .await
        .unwrap();
    let opened = wait_for_matching(
        &mut events,
        |event| {
            event.work_item_id == fixture.work_item_id.to_string()
                && matches!(event.kind, FrontendEventKind::UserInputOpened { .. })
        },
        Duration::from_secs(3),
    )
    .await;
    let FrontendEventKind::UserInputOpened {
        run_id: opened_run_id,
        request_id,
        ..
    } = opened.kind
    else {
        unreachable!()
    };
    assert_eq!(opened_run_id, run_id.to_string());
    api.respond_user_input(request_id.parse().unwrap(), "response")
        .await
        .unwrap();
    let completed =
        wait_for_frontend_status(&mut events, fixture.work_item_id, RunStatus::Completed).await;
    assert_eq!(
        completed.kind,
        FrontendEventKind::RunStatusChanged {
            run_id: run_id.to_string(),
            status: FrontendRunStatus::Completed,
        }
    );
}

#[tokio::test]
async fn interrupt_stops_run() {
    let fixture = TestFixture::new().await;
    let api = fixture
        .api_for_plan(MockRunPlan::new(Vec::new()).with_fault(MockHarnessFault::NeverComplete))
        .await;
    let mut events = api.subscribe_events();
    let run_id = api
        .start_run(fixture.work_item_id, fixture.connection_id, "stay running")
        .await
        .unwrap();
    api.interrupt_run(run_id).await.unwrap();
    let interrupted =
        wait_for_frontend_status(&mut events, fixture.work_item_id, RunStatus::Interrupted).await;
    assert_eq!(
        interrupted.kind,
        FrontendEventKind::RunStatusChanged {
            run_id: run_id.to_string(),
            status: FrontendRunStatus::Interrupted,
        }
    );
}

#[tokio::test]
async fn crash_maps_to_failed_status_in_snapshot() {
    let fixture = TestFixture::new().await;
    let api = fixture
        .api_for_plan(
            MockRunPlan::new(Vec::new()).with_fault(MockHarnessFault::ExitAfterRunAccepted),
        )
        .await;
    let mut events = api.subscribe_events();
    let run_id = api
        .start_run(fixture.work_item_id, fixture.connection_id, "crash now")
        .await
        .unwrap();
    let failed =
        wait_for_frontend_status(&mut events, fixture.work_item_id, RunStatus::Failed).await;
    assert_eq!(
        failed.kind,
        FrontendEventKind::RunStatusChanged {
            run_id: run_id.to_string(),
            status: FrontendRunStatus::Failed,
        }
    );
    let run_snapshot = fixture.store.get_run(run_id).await.unwrap();
    assert_eq!(run_snapshot.status, RunStatus::Failed);
}

#[tokio::test]
async fn outcome_unknown_is_distinct_and_evented() {
    let fixture = TestFixture::new().await;
    let api = fixture
        .api_for_plan_with_unknown_interrupt(MockRunPlan::new(Vec::new()), true)
        .await;
    let mut events = api.subscribe_events();
    let run_id = api
        .start_run(fixture.work_item_id, fixture.connection_id, "maybe")
        .await
        .unwrap();
    let _ = api.interrupt_run(run_id).await.unwrap_err();
    let outcome = wait_for_matching(
        &mut events,
        |event| {
            event.work_item_id == fixture.work_item_id.to_string()
                && matches!(event.kind, FrontendEventKind::RunOutcomeUnknown { .. })
        },
        Duration::from_secs(2),
    )
    .await;
    let FrontendEventKind::RunOutcomeUnknown {
        run_id: outcome_run_id,
        ..
    } = outcome.kind
    else {
        unreachable!()
    };
    assert_eq!(outcome_run_id, run_id.to_string());
}

#[tokio::test]
async fn event_revisions_are_monotonic_and_overflow_recovers_via_snapshot() {
    let fixture = TestFixture::new().await;
    let api = fixture
        .api_for_plan(MockRunPlan::new(
            (0..48)
                .map(|index| MockScriptStep::file_mutation(format!("file-{index}"), "updated"))
                .chain(std::iter::once(MockScriptStep::complete()))
                .collect(),
        ))
        .await;
    let baseline = api
        .get_work_item_snapshot(fixture.work_item_id)
        .await
        .unwrap()
        .work_item
        .revision;
    let mut events = api.subscribe_events();
    let run_id = api
        .start_run(fixture.work_item_id, fixture.connection_id, "overflow")
        .await
        .unwrap();
    let snapshot = tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            let snapshot = api
                .get_work_item_snapshot(fixture.work_item_id)
                .await
                .unwrap();
            if snapshot.runs.iter().any(|run| {
                run.id == run_id.to_string() && run.status == FrontendRunStatus::Completed
            }) {
                break snapshot;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    for _ in 0..16 {
        tokio::task::yield_now().await;
    }
    assert!(matches!(
        events.recv().await,
        Err(broadcast::error::RecvError::Lagged(_))
    ));
    assert!(snapshot.work_item.revision > baseline);
    assert_eq!(snapshot.runs.len(), 1);
    let timeline = api
        .get_work_item_timeline_page(fixture.work_item_id, run_id, None, 50)
        .await
        .unwrap();
    assert_eq!(timeline.items.len(), 48);
}

#[tokio::test]
async fn set_connection_credential_is_write_only() {
    let fixture = TestFixture::new().await;
    let api = fixture.api_no_run().await;
    let value = "canary-secret-value".to_owned();
    let wrote = api
        .set_connection_credential(fixture.connection_id, fixture.slot(), value.clone(), false)
        .await
        .unwrap();
    assert!(!wrote.overwritten);
    let single = api.connection_get(fixture.connection_id).await.unwrap();
    let listed = api.list_connections().await.unwrap();
    let error = api
        .set_connection_credential(fixture.connection_id, fixture.slot(), value.clone(), false)
        .await
        .unwrap_err();
    let facade_payload = format!("{wrote:?}{single:?}{listed:?}{error:?}");
    assert!(!facade_payload.contains(&value));
    assert!(!tree_contains(fixture._temp.path(), value.as_bytes()));
}

fn tree_contains(path: &std::path::Path, needle: &[u8]) -> bool {
    std::fs::read_dir(path).unwrap().any(|entry| {
        let path = entry.unwrap().path();
        if path.is_dir() {
            tree_contains(&path, needle)
        } else {
            std::fs::read(path)
                .map(|bytes| bytes.windows(needle.len()).any(|window| window == needle))
                .unwrap_or(false)
        }
    })
}

#[tokio::test]
async fn secret_backend_failures_keep_stable_facade_codes() {
    let fixture = TestFixture::new().await;
    for (failure, expected) in [
        (SecretFailure::Uncertain, DesktopErrorCode::OutcomeUnknown),
        (
            SecretFailure::Unavailable,
            DesktopErrorCode::BackendUnavailable,
        ),
        (SecretFailure::Locked, DesktopErrorCode::InvalidRequest),
        (SecretFailure::Denied, DesktopErrorCode::Unsupported),
        (
            SecretFailure::AuthenticationRequired,
            DesktopErrorCode::Unsupported,
        ),
        (SecretFailure::AlreadyExists, DesktopErrorCode::Conflict),
    ] {
        let api = fixture
            .api_with_secret_port(Arc::new(FailingSecretPort(failure)))
            .await;
        let error = api
            .set_connection_credential(fixture.connection_id, fixture.slot(), "never echoed", false)
            .await
            .unwrap_err();
        assert_eq!(error.code, expected);
    }
}

#[tokio::test]
async fn artifact_list_and_open_round_trip() {
    let fixture = TestFixture::new().await;
    let api = fixture.api_no_run().await;
    let bytes = b"artifact-body".to_vec();
    let artifact = fixture
        .artifact_port
        .publish(
            fixture.work_item_id,
            None,
            ArtifactKind::File,
            "text/plain",
            &bytes,
        )
        .await
        .unwrap();
    let listed = api.list_artifacts(fixture.work_item_id).await.unwrap();
    assert_eq!(listed.artifacts.len(), 1);
    let first = &listed.artifacts[0];
    assert_eq!(first.id, artifact.id.to_string());
    let opened = api
        .open_artifact(fixture.work_item_id, artifact.id, bytes.len() as u64)
        .await
        .unwrap();
    assert_eq!(opened.bytes, bytes);
    assert_eq!(opened.artifact.id, artifact.id.to_string());
}

#[tokio::test]
async fn clear_cache_removes_entries() {
    let fixture = TestFixture::new().await;
    let api = fixture.api_no_run().await;
    fixture
        .cache_store
        .put("app", "cache-key", &serde_json::json!({ "foo": "bar" }))
        .unwrap();
    assert!(fixture.cache_store.exists("app", "cache-key").unwrap());
    api.clear_cache().await.unwrap();
    assert!(!fixture.cache_store.exists("app", "cache-key").unwrap());
}

#[tokio::test]
async fn event_revisions_are_contiguous_without_gap() {
    let fixture = TestFixture::new().await;
    let api = fixture
        .api_for_plan(MockRunPlan::new(
            (0..40)
                .map(|index| MockScriptStep::message(format!("m{index}")))
                .chain(std::iter::once(MockScriptStep::complete()))
                .collect(),
        ))
        .await;
    let mut events = api.subscribe_events();
    let run_id = api
        .start_run(fixture.work_item_id, fixture.connection_id, "batch")
        .await
        .unwrap();
    let mut revisions = Vec::new();
    let completed = wait_for_frontend_status_with_revisions(
        &mut events,
        &mut revisions,
        fixture.work_item_id,
        RunStatus::Completed,
    )
    .await;
    assert_eq!(
        completed.kind,
        FrontendEventKind::RunStatusChanged {
            run_id: run_id.to_string(),
            status: FrontendRunStatus::Completed,
        }
    );
    assert!(revisions.len() > 1);
    assert!(
        revisions.windows(2).all(|pair| pair[1] == pair[0] + 1),
        "front-end-visible revisions are monotonic"
    );
    let snapshot = api
        .get_work_item_snapshot(fixture.work_item_id)
        .await
        .unwrap();
    assert!(
        snapshot
            .runs
            .iter()
            .find(|run| run.id == run_id.to_string())
            .is_some_and(|run| run.status == FrontendRunStatus::Completed)
    );
    let timeline = api
        .get_work_item_timeline_page(fixture.work_item_id, run_id, None, 50)
        .await
        .unwrap();
    assert!(!timeline.items.is_empty());
}

#[tokio::test]
async fn open_artifact_rejects_unbounded_request() {
    let fixture = TestFixture::new().await;
    let api = fixture.api_no_run().await;
    let bytes = b"bounded".to_vec();
    let artifact = fixture
        .artifact_port
        .publish(
            fixture.work_item_id,
            None,
            ArtifactKind::File,
            "text/plain",
            &bytes,
        )
        .await
        .unwrap();
    let result = api
        .open_artifact(fixture.work_item_id, artifact.id, u64::MAX)
        .await;
    assert!(matches!(
        result,
        Err(yakshed_desktop_api::DesktopError {
            code: yakshed_desktop_api::DesktopErrorCode::InvalidRequest,
            ..
        })
    ));
}

#[tokio::test]
async fn text_commands_reject_oversized_payloads_before_supervisor_calls() {
    let fixture = TestFixture::new().await;
    let api = fixture.api_no_run().await;
    let run_error = api
        .start_run(
            fixture.work_item_id,
            fixture.connection_id,
            "x".repeat(yakshed_desktop_api::MAX_RUN_INPUT_BYTES + 1),
        )
        .await
        .unwrap_err();
    let steer_error = api
        .steer_run(
            fixture.ids.next_run_id(),
            "x".repeat(yakshed_desktop_api::MAX_STEER_INPUT_BYTES + 1),
        )
        .await
        .unwrap_err();
    let response_error = api
        .respond_user_input(
            fixture.ids.next_timeline_item_id(),
            "x".repeat(yakshed_desktop_api::MAX_USER_INPUT_RESPONSE_BYTES + 1),
        )
        .await
        .unwrap_err();
    assert_eq!(run_error.code, DesktopErrorCode::InvalidRequest);
    assert_eq!(steer_error.code, DesktopErrorCode::InvalidRequest);
    assert_eq!(response_error.code, DesktopErrorCode::InvalidRequest);
    assert_ne!(
        api.start_run(
            fixture.work_item_id,
            fixture.connection_id,
            "x".repeat(yakshed_desktop_api::MAX_RUN_INPUT_BYTES),
        )
        .await
        .unwrap_err()
        .code,
        DesktopErrorCode::InvalidRequest
    );
    assert_ne!(
        api.steer_run(
            fixture.ids.next_run_id(),
            "x".repeat(yakshed_desktop_api::MAX_STEER_INPUT_BYTES),
        )
        .await
        .unwrap_err()
        .code,
        DesktopErrorCode::InvalidRequest
    );
    let provider_request = "boundary-response".parse::<ProviderRequestId>().unwrap();
    let response_api = fixture
        .api_for_plan(MockRunPlan::new(vec![
            MockScriptStep::user_input(provider_request.clone(), "boundary?"),
            MockScriptStep::await_response(provider_request),
            MockScriptStep::complete(),
        ]))
        .await;
    let mut events = response_api.subscribe_events();
    response_api
        .start_run(fixture.work_item_id, fixture.connection_id, "boundary")
        .await
        .unwrap();
    let opened = wait_for_matching(
        &mut events,
        |event| matches!(event.kind, FrontendEventKind::UserInputOpened { .. }),
        Duration::from_secs(3),
    )
    .await;
    let FrontendEventKind::UserInputOpened { request_id, .. } = opened.kind else {
        unreachable!()
    };
    response_api
        .respond_user_input(
            request_id.parse().unwrap(),
            "x".repeat(yakshed_desktop_api::MAX_USER_INPUT_RESPONSE_BYTES),
        )
        .await
        .unwrap();
}

struct TestFixture {
    _temp: tempfile::TempDir,
    _paths: AppPaths,
    ids: Arc<dyn IdGenerator>,
    clock: Arc<dyn yakshed_application::Clock>,
    store: Arc<SqliteStore>,
    config_store: Arc<ConfigStore>,
    config_port: Arc<TestConfigPort>,
    secret_port: Arc<TestSecretPort>,
    cache_store: Arc<CacheStore>,
    cache_port: Arc<TestCachePort>,
    _artifact_store: Arc<ArtifactStore>,
    artifact_port: Arc<TestArtifactPort>,
    project_id: yakshed_domain::ProjectId,
    work_item_id: WorkItemId,
    connection_id: ConnectionId,
}

impl TestFixture {
    async fn new() -> Self {
        let _temp = tempfile::tempdir().unwrap();
        let paths = AppPaths::for_test(_temp.path());
        let ids: Arc<dyn IdGenerator> = Arc::new(SystemIdGenerator);
        let clock: Arc<dyn yakshed_application::Clock> = Arc::new(SystemClock);
        let store = Arc::new(
            SqliteStore::open(paths.clone(), clock.clone(), ids.clone())
                .await
                .unwrap(),
        );
        let project_id = ids.next_project_id();
        store
            .create_project(CreateProject {
                id: project_id,
                name: "desktop".to_owned(),
            })
            .await
            .unwrap();
        let work_item_id = ids.next_work_item_id();
        store
            .create_work_item(CreateWorkItem {
                id: work_item_id,
                project_id,
                title: "base".to_owned(),
                parent_id: None,
            })
            .await
            .unwrap();
        let config_store =
            Arc::new(ConfigStore::open(paths.clone(), TEST_SECRET_CAPABILITIES).unwrap());
        let connection_id = "0193f26e-7a72-7d42-bf77-0de14c4cc001".parse().unwrap();
        let config_port = Arc::new(TestConfigPort::new(config_store.clone()));
        let broker = new_memory_broker();
        let secret_port = Arc::new(TestSecretPort::new(config_store.clone(), broker));
        let cache_store = Arc::new(CacheStore::open(&paths).unwrap());
        let cache_port = Arc::new(TestCachePort {
            cache: cache_store.clone(),
        });
        let artifact_store = Arc::new(ArtifactStore::new(&paths, 1024 * 1024).unwrap());
        let artifact_port = Arc::new(TestArtifactPort::new(artifact_store.clone(), ids.clone()));
        config_port
            .put_connection(PutConnectionCommand {
                expected_config_revision: ConfigRevision::INITIAL,
                connection: test_connection(connection_id),
                ensure_memory_secret_backend: true,
            })
            .await
            .unwrap();
        Self {
            _temp,
            _paths: paths,
            ids,
            clock,
            store,
            config_store,
            config_port,
            secret_port,
            cache_store,
            cache_port,
            _artifact_store: artifact_store,
            artifact_port,
            project_id,
            work_item_id,
            connection_id,
        }
    }

    async fn api_no_run(&self) -> DesktopApi {
        self.api_with_secret_port(self.secret_port.clone()).await
    }

    async fn api_with_secret_port(&self, secrets: Arc<dyn SecretPort>) -> DesktopApi {
        DesktopApi::new(ApiPorts {
            store: self.store.clone(),
            harness: Arc::new(NoopRunHarness),
            clock: self.clock.clone(),
            ids: self.ids.clone(),
            config: self.config_port.clone(),
            secrets,
            cache: self.cache_port.clone(),
            artifacts: self.artifact_port.clone(),
        })
        .await
    }

    async fn api_for_plan(&self, plan: MockRunPlan) -> DesktopApi {
        let harness =
            MockPort::new(self.config_store.clone(), plan, self.connection_id, false).await;
        DesktopApi::new(ApiPorts {
            store: self.store.clone(),
            harness,
            clock: self.clock.clone(),
            ids: self.ids.clone(),
            config: self.config_port.clone(),
            secrets: self.secret_port.clone(),
            cache: self.cache_port.clone(),
            artifacts: self.artifact_port.clone(),
        })
        .await
    }

    async fn api_for_plan_with_unknown_interrupt(
        &self,
        plan: MockRunPlan,
        unknown_interrupt: bool,
    ) -> DesktopApi {
        let harness = MockPort::new(
            self.config_store.clone(),
            plan,
            self.connection_id,
            unknown_interrupt,
        )
        .await;
        DesktopApi::new(ApiPorts {
            store: self.store.clone(),
            harness,
            clock: self.clock.clone(),
            ids: self.ids.clone(),
            config: self.config_port.clone(),
            secrets: self.secret_port.clone(),
            cache: self.cache_port.clone(),
            artifacts: self.artifact_port.clone(),
        })
        .await
    }

    fn slot(&self) -> CredentialSlot {
        CredentialSlot::new("mock.api_key").unwrap()
    }
}

fn test_connection(connection_id: ConnectionId) -> Connection {
    Connection {
        id: connection_id,
        name: "mock".to_owned(),
        harness: "mock".to_owned(),
        model_provider: "mock-model".to_owned(),
        provider_state: ProviderStateRootId::new("mock-provider").unwrap(),
        credentials: vec![CredentialBindingRecord {
            slot: CredentialSlot::new("mock.api_key").unwrap(),
            binding: CredentialBinding::Secret {
                reference: SecretReference {
                    backend_id: SecretBackendId::new("memory").unwrap(),
                    locator: SecretLocator::new("connection/mock/api-key").unwrap(),
                },
            },
        }],
    }
}

fn new_memory_broker() -> Arc<CredentialBroker> {
    let backend_id = SecretBackendId::new("memory").unwrap();
    let backend = Arc::new(MemorySecretBackend::new(backend_id.clone()));
    Arc::new(
        CredentialBroker::new(
            [(
                backend_id,
                SecretBackendHandle {
                    resolver: backend.clone(),
                    administrator: Some(backend.clone()),
                },
            )],
            &[],
            Arc::new(NoopSecretAuditSink),
            Duration::from_millis(250),
        )
        .expect("memory secret broker should initialize"),
    )
}

const TEST_SECRET_CAPABILITIES: &[yakshed_application::SecretBackendCapability] =
    &[yakshed_application::SecretBackendCapability::available(
        "memory",
    )];

#[derive(Clone)]
struct TestConfigPort {
    store: Arc<ConfigStore>,
}

impl TestConfigPort {
    fn new(store: Arc<ConfigStore>) -> Self {
        Self { store }
    }
}

#[async_trait]
impl ConfigPort for TestConfigPort {
    async fn put_connection(
        &self,
        command: yakshed_application::PutConnectionCommand,
    ) -> Result<yakshed_application::ConfigSnapshot, ConfigPortError> {
        let needs_memory = command
            .connection
            .credentials
            .iter()
            .any(|binding| matches!(binding.binding, CredentialBinding::Secret { .. }))
            || command.ensure_memory_secret_backend;
        let secret_backends = if needs_memory {
            vec![yakshed_domain::SecretBackend {
                id: SecretBackendId::new("memory").unwrap(),
                settings: yakshed_domain::SecretBackendSettings::Memory,
            }]
        } else {
            Vec::new()
        };
        let change = if secret_backends.is_empty() {
            ConfigChange::PutConnection(command.connection)
        } else {
            ConfigChange::PutConnectionWithSecretBackends {
                connection: command.connection,
                secret_backends,
            }
        };
        self.store
            .update(command.expected_config_revision, change)
            .await
            .map_err(map_config_store_error)
    }

    async fn get_connection(
        &self,
        connection_id: ConnectionId,
    ) -> Result<yakshed_application::ConfigConnectionSnapshot, ConfigPortError> {
        let snapshot = self.store.snapshot();
        let connections = snapshot.config.connections;
        let Some(connection) = connections
            .into_iter()
            .find(|connection| connection.id == connection_id)
        else {
            return Err(ConfigPortError::NotFound);
        };
        Ok(yakshed_application::ConfigConnectionSnapshot {
            config_revision: snapshot.revision,
            connection: to_public_connection(connection),
        })
    }

    async fn list_connections(
        &self,
    ) -> Result<yakshed_application::PublicConnectionList, ConfigPortError> {
        let snapshot = self.store.snapshot();
        Ok(yakshed_application::PublicConnectionList {
            config_revision: snapshot.revision,
            connections: snapshot
                .config
                .connections
                .into_iter()
                .map(to_public_connection)
                .collect(),
        })
    }
}

#[derive(Clone)]
struct TestSecretPort {
    store: Arc<ConfigStore>,
    broker: Arc<CredentialBroker>,
}

#[derive(Clone, Copy)]
enum SecretFailure {
    Uncertain,
    Unavailable,
    Locked,
    Denied,
    AuthenticationRequired,
    AlreadyExists,
}

struct FailingSecretPort(SecretFailure);

#[async_trait]
impl SecretPort for FailingSecretPort {
    async fn set_connection_credential(
        &self,
        _command: SetConnectionCredentialCommand,
    ) -> Result<yakshed_application::SecretWriteOutcome, SecretPortError> {
        Err(match self.0 {
            SecretFailure::Uncertain => SecretPortError::UncertainWrite,
            SecretFailure::Unavailable => SecretPortError::BackendUnavailable,
            SecretFailure::Locked => SecretPortError::Locked,
            SecretFailure::Denied => SecretPortError::Denied,
            SecretFailure::AuthenticationRequired => SecretPortError::AuthenticationRequired,
            SecretFailure::AlreadyExists => SecretPortError::AlreadyExists,
        })
    }
}

impl TestSecretPort {
    fn new(store: Arc<ConfigStore>, broker: Arc<CredentialBroker>) -> Self {
        Self { store, broker }
    }
}

#[async_trait]
impl SecretPort for TestSecretPort {
    async fn set_connection_credential(
        &self,
        command: SetConnectionCredentialCommand,
    ) -> Result<yakshed_application::SecretWriteOutcome, SecretPortError> {
        let snapshot = self.store.snapshot();
        let connection = snapshot
            .config
            .connections
            .iter()
            .find(|connection| connection.id == command.connection_id)
            .ok_or(SecretPortError::ConnectionNotFound)?;
        let binding = connection
            .credentials
            .iter()
            .find(|binding| binding.slot == command.slot)
            .ok_or(SecretPortError::BindingNotFound)?;
        let context = SecretAccessContext {
            connection_id: connection.id,
            slot: command.slot,
            purpose: SecretAccessPurpose::ValidateCredential,
            request_id: yakshed_domain::OperationId::new("desktop-api-test").unwrap(),
        };
        let outcome = self
            .broker
            .put(
                std::slice::from_ref(connection),
                binding,
                &context,
                &SecretString::from(command.value.expose()),
                PutSecretOptions {
                    overwrite: command.overwrite,
                },
                &BrokerCancellation::default(),
            )
            .await
            .map_err(map_secret_error)?;
        Ok(yakshed_application::SecretWriteOutcome {
            overwritten: matches!(outcome, yakshed_secrets::PutSecretOutcome::Replaced),
        })
    }
}

#[derive(Clone)]
struct TestCachePort {
    cache: Arc<CacheStore>,
}

#[async_trait]
impl CachePort for TestCachePort {
    async fn clear(&self) -> Result<(), CachePortError> {
        self.cache.clear().map_err(|_| CachePortError::Failed)
    }
}

#[derive(Clone)]
struct TestArtifactPort {
    records: Arc<StdMutex<HashMap<WorkItemId, Vec<yakshed_domain::ArtifactRecord>>>>,
    by_id: Arc<StdMutex<HashMap<ArtifactId, yakshed_domain::ArtifactRecord>>>,
    store: Arc<ArtifactStore>,
    ids: Arc<dyn IdGenerator>,
}

impl TestArtifactPort {
    fn new(store: Arc<ArtifactStore>, ids: Arc<dyn IdGenerator>) -> Self {
        Self {
            records: Arc::new(StdMutex::new(HashMap::new())),
            by_id: Arc::new(StdMutex::new(HashMap::new())),
            store,
            ids,
        }
    }

    async fn publish(
        &self,
        work_item_id: WorkItemId,
        run_id: Option<RunId>,
        kind: ArtifactKind,
        media_type: &str,
        bytes: &[u8],
    ) -> Result<yakshed_domain::ArtifactRecord, ArtifactPortError> {
        let artifact = self
            .store
            .publish(
                bytes,
                ArtifactMetadata {
                    id: ArtifactId::try_from(self.ids.next_project_id().to_string()).unwrap(),
                    work_item_id,
                    run_id,
                    kind,
                    media_type: media_type.to_owned(),
                    provenance: yakshed_domain::ArtifactProvenance::new("desktop-api-test")
                        .unwrap(),
                },
            )
            .map_err(map_artifact_store_error)?;
        self.by_id
            .lock()
            .unwrap()
            .insert(artifact.id, artifact.clone());
        self.records
            .lock()
            .unwrap()
            .entry(work_item_id)
            .or_default()
            .push(artifact.clone());
        Ok(artifact)
    }
}

#[async_trait]
impl ArtifactPort for TestArtifactPort {
    async fn list_artifacts_for_work_item(
        &self,
        work_item_id: WorkItemId,
    ) -> Result<Vec<yakshed_domain::ArtifactRecord>, ArtifactPortError> {
        let mut records = self
            .records
            .lock()
            .unwrap()
            .get(&work_item_id)
            .cloned()
            .unwrap_or_default();
        records.sort_by_key(|artifact| artifact.id);
        Ok(records)
    }

    async fn open_artifact(
        &self,
        command: OpenArtifactCommand,
    ) -> Result<OpenArtifactPayload, ArtifactPortError> {
        let artifact = self
            .by_id
            .lock()
            .unwrap()
            .get(&command.artifact_id)
            .cloned()
            .ok_or(ArtifactPortError::NotFound)?;
        let mut reader = self
            .store
            .open(&artifact.digest, command.max_bytes)
            .map_err(map_artifact_store_error)?;
        let mut bytes = Vec::new();
        reader
            .read_to_end(&mut bytes)
            .map_err(|_| ArtifactPortError::Failed)?;
        Ok(OpenArtifactPayload { artifact, bytes })
    }
}

#[derive(Clone)]
struct NoopRunHarness;

#[async_trait]
impl yakshed_application::RunHarness for NoopRunHarness {
    async fn start_run(
        &self,
        _connection_id: ConnectionId,
        _correlation_id: RunId,
        _input: String,
    ) -> Result<yakshed_application::ProviderRunRef, yakshed_application::HarnessPortError> {
        Err(yakshed_application::HarnessPortError::Unsupported(
            "unused".to_owned(),
        ))
    }

    async fn steer(
        &self,
        _run: &yakshed_application::ProviderRunRef,
        _input: String,
    ) -> Result<(), yakshed_application::HarnessPortError> {
        Ok(())
    }

    async fn interrupt(
        &self,
        _run: &yakshed_application::ProviderRunRef,
    ) -> Result<(), yakshed_application::HarnessPortError> {
        Ok(())
    }

    async fn respond(
        &self,
        _request: yakshed_application::ProviderRequestRef,
        _response: yakshed_application::HarnessResponse,
    ) -> Result<(), yakshed_application::HarnessPortError> {
        Ok(())
    }

    async fn next_event(
        &self,
    ) -> Result<Option<yakshed_application::RunHarnessEvent>, yakshed_application::HarnessPortError>
    {
        std::future::pending().await
    }

    async fn reconnect(
        &self,
        _run: &yakshed_application::ProviderRunRef,
    ) -> Result<bool, yakshed_application::HarnessPortError> {
        Ok(true)
    }
}

struct MockPort {
    harness: Arc<MockHarness>,
    _runtime: RuntimeHandle,
    _session: ProviderSession,
    stream: Mutex<ProviderEventStream>,
    runs: Mutex<HashMap<yakshed_application::ProviderRunRef, ProviderRunHandle>>,
    unknown_interrupt: bool,
}

impl MockPort {
    async fn new(
        config_store: Arc<ConfigStore>,
        plan: MockRunPlan,
        connection_id: ConnectionId,
        unknown_interrupt: bool,
    ) -> Arc<Self> {
        let _ = config_store;
        let runtime = RuntimeHandle::new("mock-runtime").unwrap();
        let harness = Arc::new(
            MockHarness::new(HarnessCapabilities::default(), vec![plan], None).with_runtime(
                runtime.clone(),
                connection_id,
                None,
                Vec::new(),
            ),
        );
        let stream = harness.subscribe().unwrap();
        let session = harness
            .start_session(
                &runtime,
                StartSessionSpec {
                    working_directory: RuntimePath::new("mock-runtime://workspace").unwrap(),
                    title: "desktop-test".to_owned(),
                },
            )
            .await
            .unwrap();
        Arc::new(Self {
            harness,
            _runtime: runtime,
            _session: session,
            stream: Mutex::new(stream),
            runs: Mutex::new(HashMap::new()),
            unknown_interrupt,
        })
    }

    fn run_ref(run: &ProviderRunHandle) -> yakshed_application::ProviderRunRef {
        yakshed_application::ProviderRunRef::new("mock", run.to_string()).unwrap()
    }

    async fn native_run(
        &self,
        run: &yakshed_application::ProviderRunRef,
    ) -> Result<ProviderRunHandle, yakshed_application::HarnessPortError> {
        self.runs.lock().await.get(run).cloned().ok_or_else(|| {
            yakshed_application::HarnessPortError::NotFound(run.native_id().to_owned())
        })
    }
}

#[async_trait]
impl yakshed_application::RunHarness for MockPort {
    async fn start_run(
        &self,
        connection_id: ConnectionId,
        _correlation_id: RunId,
        input: String,
    ) -> Result<yakshed_application::ProviderRunRef, yakshed_application::HarnessPortError> {
        let run = self
            .harness
            .start_run(
                &self._session,
                HarnessInput::new(input).map_err(map_harness_error)?,
                RunOptions::default(),
            )
            .await
            .map_err(map_harness_error)?;
        let run_ref = Self::run_ref(&run);
        self.runs.lock().await.insert(run_ref.clone(), run);
        assert_eq!(connection_id, self._session.connection_id);
        Ok(run_ref)
    }

    async fn steer(
        &self,
        run: &yakshed_application::ProviderRunRef,
        input: String,
    ) -> Result<(), yakshed_application::HarnessPortError> {
        self.harness
            .steer(
                &self.native_run(run).await?,
                HarnessInput::new(input).map_err(map_harness_error)?,
            )
            .await
            .map_err(map_harness_error)
    }

    async fn interrupt(
        &self,
        run: &yakshed_application::ProviderRunRef,
    ) -> Result<(), yakshed_application::HarnessPortError> {
        if self.unknown_interrupt {
            return Err(yakshed_application::HarnessPortError::OutcomeUnknown {
                operation: "interrupt",
            });
        }
        self.harness
            .interrupt(&self.native_run(run).await?)
            .await
            .map_err(map_harness_error)
    }

    async fn respond(
        &self,
        request: yakshed_application::ProviderRequestRef,
        response: yakshed_application::HarnessResponse,
    ) -> Result<(), yakshed_application::HarnessPortError> {
        let run = self.native_run(&request.run).await?;
        let request =
            ProviderRequestHandle::new(run, request.native_id.parse().map_err(map_harness_error)?);
        let response = match response {
            yakshed_application::HarnessResponse::Approval(decision) => {
                ProviderResponse::Approval(decision)
            }
            yakshed_application::HarnessResponse::UserInput(input) => {
                ProviderResponse::UserInput(input)
            }
        };
        self.harness
            .respond_to_request(request, response)
            .await
            .map_err(map_harness_error)
    }

    async fn next_event(
        &self,
    ) -> Result<Option<yakshed_application::RunHarnessEvent>, yakshed_application::HarnessPortError>
    {
        Ok(self.stream.lock().await.recv().await.map(convert_event))
    }

    async fn reconnect(
        &self,
        run: &yakshed_application::ProviderRunRef,
    ) -> Result<bool, yakshed_application::HarnessPortError> {
        Ok(self.runs.lock().await.contains_key(run))
    }
}

fn convert_event(event: HarnessEvent) -> yakshed_application::RunHarnessEvent {
    match event {
        HarnessEvent::RunAccepted { run, .. } => {
            yakshed_application::RunHarnessEvent::RunAccepted {
                run: MockPort::run_ref(&run),
            }
        }
        HarnessEvent::MessageDelta { run, chunk, .. } => {
            yakshed_application::RunHarnessEvent::MessageDelta {
                run: MockPort::run_ref(&run),
                chunk,
            }
        }
        HarnessEvent::MessageCompleted { run, text, .. } => {
            yakshed_application::RunHarnessEvent::MessageCompleted {
                run: MockPort::run_ref(&run),
                text,
            }
        }
        HarnessEvent::ApprovalRequested {
            request, summary, ..
        } => yakshed_application::RunHarnessEvent::ApprovalRequested {
            request: provider_request_ref(&request),
            summary,
        },
        HarnessEvent::UserInputRequested {
            request, prompt, ..
        } => yakshed_application::RunHarnessEvent::UserInputRequested {
            request: provider_request_ref(&request),
            prompt,
        },
        HarnessEvent::FileMutation {
            run, path, summary, ..
        } => yakshed_application::RunHarnessEvent::FileMutation {
            run: MockPort::run_ref(&run),
            path,
            summary,
        },
        HarnessEvent::CommandOutputDelta {
            run,
            command,
            command_text,
            chunk,
            ..
        } => yakshed_application::RunHarnessEvent::CommandOutputDelta {
            run: MockPort::run_ref(&run),
            command: yakshed_application::ProviderCommandRef {
                run: MockPort::run_ref(command.run()),
                native_id: command.native_id().to_string(),
            },
            command_text,
            chunk,
        },
        HarnessEvent::CommandOutputCompleted {
            run,
            command,
            command_text,
            output,
            ..
        } => yakshed_application::RunHarnessEvent::CommandOutputCompleted {
            run: MockPort::run_ref(&run),
            command: yakshed_application::ProviderCommandRef {
                run: MockPort::run_ref(command.run()),
                native_id: command.native_id().to_string(),
            },
            command_text,
            output,
        },
        HarnessEvent::RunTerminal { run, state, .. } => {
            yakshed_application::RunHarnessEvent::RunTerminal {
                run: MockPort::run_ref(&run),
                state: match state {
                    HarnessRunTerminal::Completed => yakshed_application::RunTerminal::Completed,
                    HarnessRunTerminal::Failed { diagnostic } => {
                        yakshed_application::RunTerminal::Failed {
                            diagnostic: diagnostic.sanitized_text().to_owned(),
                        }
                    }
                    HarnessRunTerminal::Interrupted => {
                        yakshed_application::RunTerminal::Interrupted
                    }
                    HarnessRunTerminal::Crashed { diagnostic } => {
                        yakshed_application::RunTerminal::Crashed {
                            diagnostic: diagnostic.sanitized_text().to_owned(),
                        }
                    }
                },
            }
        }
        HarnessEvent::Unknown {
            run,
            item_type,
            native,
        } => yakshed_application::RunHarnessEvent::Unknown {
            run: run.map(|run| MockPort::run_ref(&run)),
            item_type,
            native: native.sanitized_raw().to_owned(),
        },
        HarnessEvent::MalformedNativePayload {
            run,
            item_type,
            native,
        } => yakshed_application::RunHarnessEvent::Malformed {
            run: run.map(|run| MockPort::run_ref(&run)),
            item_type,
            native: native.sanitized_raw().to_owned(),
        },
    }
}

fn provider_request_ref(
    request: &ProviderRequestHandle,
) -> yakshed_application::ProviderRequestRef {
    yakshed_application::ProviderRequestRef {
        run: MockPort::run_ref(request.run()),
        native_id: request.native_id().to_string(),
    }
}

async fn wait_for_frontend_status(
    events: &mut broadcast::Receiver<FrontendEvent>,
    work_item_id: WorkItemId,
    expected: RunStatus,
) -> FrontendEvent {
    wait_for_matching(
        events,
        |event| {
            event.work_item_id == work_item_id.to_string()
                && matches!(
                    event.kind,
                    FrontendEventKind::RunStatusChanged {
                        status: expected_status,
                        ..
                    } if expected_status == match expected {
                        RunStatus::Running => FrontendRunStatus::Running,
                        RunStatus::Completed => FrontendRunStatus::Completed,
                        RunStatus::Failed => FrontendRunStatus::Failed,
                        RunStatus::Interrupted => FrontendRunStatus::Interrupted,
                        RunStatus::Starting => FrontendRunStatus::Starting,
                        RunStatus::Disconnected => FrontendRunStatus::Disconnected,
                        RunStatus::OutcomeUnknown => FrontendRunStatus::OutcomeUnknown,
                    }
                )
        },
        Duration::from_secs(4),
    )
    .await
}

async fn wait_for_frontend_status_with_revisions(
    events: &mut broadcast::Receiver<FrontendEvent>,
    revisions: &mut Vec<u64>,
    work_item_id: WorkItemId,
    expected: RunStatus,
) -> FrontendEvent {
    loop {
        let event = wait_for_matching(
            events,
            |event| event.work_item_id == work_item_id.to_string(),
            Duration::from_secs(4),
        )
        .await;
        revisions.push(event.revision);
        if matches!(
            event.kind,
            FrontendEventKind::RunStatusChanged {
                status,
                ..
            } if status == match expected {
                RunStatus::Running => FrontendRunStatus::Running,
                RunStatus::Completed => FrontendRunStatus::Completed,
                RunStatus::Failed => FrontendRunStatus::Failed,
                RunStatus::Interrupted => FrontendRunStatus::Interrupted,
                RunStatus::Starting => FrontendRunStatus::Starting,
                RunStatus::Disconnected => FrontendRunStatus::Disconnected,
                RunStatus::OutcomeUnknown => FrontendRunStatus::OutcomeUnknown,
            }
        ) {
            return event;
        }
    }
}

async fn wait_for_matching(
    events: &mut broadcast::Receiver<FrontendEvent>,
    predicate: impl Fn(&FrontendEvent) -> bool,
    timeout: Duration,
) -> FrontendEvent {
    tokio::time::timeout(timeout, async {
        loop {
            let event = events.recv().await.unwrap();
            if predicate(&event) {
                break event;
            }
        }
    })
    .await
    .unwrap()
}

async fn wait_for_run_completion(store: &Arc<SqliteStore>, run_id: RunId) {
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            let snapshot = store.get_run(run_id).await.unwrap();
            if snapshot.status == RunStatus::Completed {
                break;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap();
}

fn map_config_store_error(error: yakshed_store::ConfigError) -> ConfigPortError {
    match error {
        yakshed_store::ConfigError::Conflict { expected, actual } => {
            ConfigPortError::Conflict { expected, actual }
        }
        yakshed_store::ConfigError::Validation(_)
        | yakshed_store::ConfigError::SecretBackendConfiguration(_) => ConfigPortError::Validation,
        yakshed_store::ConfigError::UnsupportedSchema { .. }
        | yakshed_store::ConfigError::Parse(_)
        | yakshed_store::ConfigError::Serialize(_)
        | yakshed_store::ConfigError::Worker(_)
        | yakshed_store::ConfigError::Io { .. } => ConfigPortError::Unavailable,
    }
}

fn map_secret_error(error: SecretError) -> SecretPortError {
    error.into()
}

fn map_artifact_store_error(error: yakshed_store::ArtifactError) -> ArtifactPortError {
    match error {
        yakshed_store::ArtifactError::BoundExceeded { .. } => ArtifactPortError::TooLarge,
        _ => ArtifactPortError::Failed,
    }
}

fn map_harness_error(error: HarnessError) -> yakshed_application::HarnessPortError {
    match error {
        HarnessError::InvalidInput(message) => {
            yakshed_application::HarnessPortError::InvalidInput(message)
        }
        HarnessError::NotFound { entity, id } => {
            yakshed_application::HarnessPortError::NotFound(format!("{entity}: {id}"))
        }
        HarnessError::Conflict(message) => yakshed_application::HarnessPortError::Conflict(message),
        HarnessError::Unsupported(message) => {
            yakshed_application::HarnessPortError::Unsupported(message.to_owned())
        }
        HarnessError::Overloaded => yakshed_application::HarnessPortError::Overloaded,
        HarnessError::Disconnected => yakshed_application::HarnessPortError::Disconnected,
        HarnessError::OutcomeUnknown { operation } => {
            yakshed_application::HarnessPortError::OutcomeUnknown { operation }
        }
        HarnessError::Closed => yakshed_application::HarnessPortError::Closed,
        HarnessError::Protocol { diagnostic } => {
            yakshed_application::HarnessPortError::Protocol(diagnostic.sanitized_text().to_owned())
        }
        HarnessError::Transport { diagnostic } => {
            yakshed_application::HarnessPortError::Transport(diagnostic.sanitized_text().to_owned())
        }
        HarnessError::Runtime { diagnostic } => {
            yakshed_application::HarnessPortError::Runtime(diagnostic.sanitized_text().to_owned())
        }
    }
}

fn to_public_connection(connection: yakshed_domain::Connection) -> PublicConnection {
    PublicConnection {
        id: connection.id,
        name: connection.name,
        harness: connection.harness,
        model_provider: connection.model_provider,
        provider_state: connection.provider_state,
        credentials: connection
            .credentials
            .into_iter()
            .map(|binding| {
                let source = match binding.binding {
                    CredentialBinding::Delegated { authority } => {
                        PublicCredentialSource::Delegated { authority }
                    }
                    CredentialBinding::Secret { reference } => PublicCredentialSource::Secret {
                        backend: reference.backend_id,
                        locator: reference.locator,
                    },
                    CredentialBinding::Disabled => PublicCredentialSource::Disabled,
                };
                PublicCredentialBinding {
                    slot: binding.slot,
                    source,
                }
            })
            .collect(),
    }
}
