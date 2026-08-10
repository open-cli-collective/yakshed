use std::path::PathBuf;

use provider_mock::{MockHarness, MockHarnessFault, MockRunPlan, MockScriptStep};
use yakshed_harness::{
    EVENT_BUFFER_CAPACITY, HarnessAdapter, HarnessCapabilities, HarnessEvent, HarnessInput,
    HarnessRunTerminal, ProviderRequestId, RunOptions, RuntimeHandle, StartSessionSpec,
};

fn runtime() -> RuntimeHandle {
    RuntimeHandle::new("mock-runtime").unwrap()
}

async fn session(mock: &MockHarness) -> yakshed_harness::ProviderSession {
    mock.start_session(
        &runtime(),
        StartSessionSpec {
            connection_id: "0193f26e-7a72-7000-8000-00000000aaa1".parse().unwrap(),
            working_directory: PathBuf::from("/mock/workspace"),
            title: "fault test".to_owned(),
        },
    )
    .await
    .unwrap()
}

async fn next(stream: &mut yakshed_harness::ProviderEventStream) -> HarnessEvent {
    stream.recv().await.unwrap()
}

#[tokio::test]
async fn delay_approval_is_released_without_a_timer_race() {
    let request_id = ProviderRequestId::new("delayed-request").unwrap();
    let mock = MockHarness::new(
        HarnessCapabilities::default(),
        vec![
            MockRunPlan::new(vec![
                MockScriptStep::approval(request_id, "delayed"),
                MockScriptStep::complete(),
            ])
            .with_fault(MockHarnessFault::DelayApproval),
        ],
        None,
    );
    let mut stream = mock.subscribe().unwrap();
    let session = session(&mock).await;
    mock.start_run(
        &session,
        HarnessInput::new("wait").unwrap(),
        RunOptions::default(),
    )
    .await
    .unwrap();
    assert!(matches!(
        next(&mut stream).await,
        HarnessEvent::RunAccepted { .. }
    ));

    mock.release_delayed_approval().await.unwrap();
    assert!(matches!(
        next(&mut stream).await,
        HarnessEvent::ApprovalRequested { .. }
    ));
    assert!(matches!(
        next(&mut stream).await,
        HarnessEvent::RunTerminal {
            state: HarnessRunTerminal::Completed,
            ..
        }
    ));
}

#[tokio::test]
async fn exit_after_file_mutation_stops_the_remaining_script() {
    let mock = MockHarness::new(
        HarnessCapabilities::default(),
        vec![
            MockRunPlan::new(vec![
                MockScriptStep::file_mutation("src/lib.rs", "changed"),
                MockScriptStep::command_output("cargo test", "must not be emitted"),
            ])
            .with_fault(MockHarnessFault::ExitAfterFileMutation),
        ],
        None,
    );
    let mut stream = mock.subscribe().unwrap();
    let session = session(&mock).await;
    mock.start_run(
        &session,
        HarnessInput::new("mutate").unwrap(),
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
        HarnessEvent::FileMutation { .. }
    ));
    assert!(matches!(
        next(&mut stream).await,
        HarnessEvent::RunTerminal {
            state: HarnessRunTerminal::Crashed { .. },
            ..
        }
    ));
}

#[tokio::test]
async fn malformed_native_payload_is_visible_and_preserved() {
    let mock = MockHarness::new(
        HarnessCapabilities::default(),
        vec![
            MockRunPlan::new(vec![MockScriptStep::complete()])
                .with_fault(MockHarnessFault::EmitMalformedNativePayload),
        ],
        None,
    );
    let mut stream = mock.subscribe().unwrap();
    let session = session(&mock).await;
    mock.start_run(
        &session,
        HarnessInput::new("malformed").unwrap(),
        RunOptions::default(),
    )
    .await
    .unwrap();
    assert!(matches!(
        next(&mut stream).await,
        HarnessEvent::RunAccepted { .. }
    ));
    match next(&mut stream).await {
        HarnessEvent::MalformedNativePayload { native, .. } => {
            assert_eq!(native.as_str(), "{not-json");
        }
        event => panic!("expected malformed native payload, got {event:?}"),
    }
}

#[tokio::test]
async fn bounded_event_buffer_backpressures_the_producer() {
    let steps = (0..EVENT_BUFFER_CAPACITY)
        .map(|index| MockScriptStep::message(format!("chunk-{index}")))
        .chain(std::iter::once(MockScriptStep::complete()))
        .collect();
    let mock = MockHarness::new(
        HarnessCapabilities::default(),
        vec![MockRunPlan::new(steps)],
        None,
    );
    let mut stream = mock.subscribe().unwrap();
    let session = session(&mock).await;
    let mut start = Box::pin(mock.start_run(
        &session,
        HarnessInput::new("fill buffer").unwrap(),
        RunOptions::default(),
    ));

    tokio::select! {
        biased;
        result = &mut start => panic!("producer completed without backpressure: {result:?}"),
        () = std::future::ready(()) => {}
    }

    let drain = async {
        let mut chunks = 0;
        loop {
            match next(&mut stream).await {
                HarnessEvent::MessageDelta { .. } => chunks += 1,
                HarnessEvent::RunTerminal { .. } => break chunks,
                HarnessEvent::RunAccepted { .. } => {}
                event => panic!("unexpected event while draining: {event:?}"),
            }
        }
    };
    let (run, chunks) = tokio::join!(start, drain);
    run.unwrap();
    assert_eq!(chunks, EVENT_BUFFER_CAPACITY);
}

#[tokio::test]
async fn scripted_events_keep_the_declared_legal_order() {
    let mock = MockHarness::new(
        HarnessCapabilities::default(),
        vec![MockRunPlan::new(vec![
            MockScriptStep::command_output("build", "first"),
            MockScriptStep::message("second"),
            MockScriptStep::file_mutation("later.rs", "third"),
            MockScriptStep::complete(),
        ])],
        None,
    );
    let mut stream = mock.subscribe().unwrap();
    let session = session(&mock).await;
    mock.start_run(
        &session,
        HarnessInput::new("ordered").unwrap(),
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
        HarnessEvent::CommandOutput { .. }
    ));
    assert!(matches!(
        next(&mut stream).await,
        HarnessEvent::MessageDelta { .. }
    ));
    assert!(matches!(
        next(&mut stream).await,
        HarnessEvent::FileMutation { .. }
    ));
    assert!(matches!(
        next(&mut stream).await,
        HarnessEvent::RunTerminal { .. }
    ));
}

#[tokio::test]
async fn a_run_fault_plan_is_scoped_and_consumed_once() {
    let mock = MockHarness::new(
        HarnessCapabilities::default(),
        vec![
            MockRunPlan::new(Vec::new()).with_fault(MockHarnessFault::ExitAfterRunAccepted),
            MockRunPlan::new(vec![MockScriptStep::complete()]),
        ],
        None,
    );
    let mut stream = mock.subscribe().unwrap();
    let session = session(&mock).await;
    for input in ["first", "second"] {
        mock.start_run(
            &session,
            HarnessInput::new(input).unwrap(),
            RunOptions::default(),
        )
        .await
        .unwrap();
    }

    assert!(matches!(
        next(&mut stream).await,
        HarnessEvent::RunAccepted { .. }
    ));
    assert!(matches!(
        next(&mut stream).await,
        HarnessEvent::RunTerminal {
            state: HarnessRunTerminal::Crashed { .. },
            ..
        }
    ));
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
}
