use provider_mock::{MockHarness, MockHarnessFault, MockRunPlan, MockScriptStep};
use yakshed_domain::ApprovalDecision;
use yakshed_harness::{
    EVENT_BUFFER_CAPACITY, HarnessAdapter, HarnessCapabilities, HarnessEvent, HarnessInput,
    HarnessRunTerminal, ProviderRequestId, ProviderResponse, RunOptions, RuntimeHandle,
    RuntimePath, SessionQuery, StartSessionSpec,
};

fn runtime() -> RuntimeHandle {
    RuntimeHandle::new("mock-runtime").unwrap()
}

fn mock(
    capabilities: HarnessCapabilities,
    plans: Vec<MockRunPlan>,
    fault: Option<MockHarnessFault>,
) -> MockHarness {
    MockHarness::new(capabilities, plans, None).with_runtime(
        runtime(),
        "0193f26e-7a72-7000-8000-00000000aaa1".parse().unwrap(),
        None,
        fault.into_iter().collect(),
    )
}

async fn session(mock: &MockHarness) -> yakshed_harness::ProviderSession {
    session_at(mock, &runtime()).await
}

async fn session_at(
    mock: &MockHarness,
    runtime: &RuntimeHandle,
) -> yakshed_harness::ProviderSession {
    mock.start_session(
        runtime,
        StartSessionSpec {
            working_directory: RuntimePath::new(format!("{}://workspace", runtime.as_str()))
                .unwrap(),
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
    let mock = mock(
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
    let mock = mock(
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
    match next(&mut stream).await {
        HarnessEvent::RunTerminal {
            state: HarnessRunTerminal::Crashed { .. },
            ..
        } => {}
        event => panic!("expected crashed terminal, got {event:?}"),
    }
}

#[tokio::test]
async fn malformed_native_payload_is_visible_and_preserved() {
    let mock = mock(
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
            assert_eq!(native.sanitized_raw(), "{not-json");
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
    let mock = mock(
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
    let mock = mock(
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
    let mock = mock(
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

#[tokio::test]
async fn session_pagination_crosses_9999_to_10000_in_creation_order() {
    let mock = mock(HarnessCapabilities::default(), Vec::new(), None);
    for _ in 0..10_001 {
        session(&mock).await;
    }

    let mut after = None;
    for _ in 0..99 {
        after = mock
            .list_sessions(&runtime(), SessionQuery { after, limit: 101 })
            .await
            .unwrap()
            .next;
    }
    let page = mock
        .list_sessions(&runtime(), SessionQuery { after, limit: 2 })
        .await
        .unwrap();
    assert_eq!(page.items[0].id.as_str(), "session-10000");
    assert_eq!(page.items[1].id.as_str(), "session-10001");
    assert!(page.next.is_none());
}

#[tokio::test]
async fn identical_native_ids_are_isolated_by_runtime_and_session_scope() {
    let request_id = ProviderRequestId::new("request-0001").unwrap();
    let plan = || {
        MockRunPlan::new(vec![
            MockScriptStep::approval(request_id.clone(), "approve"),
            MockScriptStep::await_response(request_id.clone()),
            MockScriptStep::complete(),
        ])
    };
    let runtime_a = RuntimeHandle::new("runtime-a").unwrap();
    let runtime_b = RuntimeHandle::new("runtime-b").unwrap();
    let mock = MockHarness::new(HarnessCapabilities::default(), vec![plan(), plan()], None)
        .with_runtime(
            runtime_a.clone(),
            "0193f26e-7a72-7000-8000-00000000aaa1".parse().unwrap(),
            None,
            Vec::new(),
        )
        .with_runtime(
            runtime_b.clone(),
            "0193f26e-7a72-7000-8000-00000000aaa2".parse().unwrap(),
            None,
            Vec::new(),
        );
    let mut stream = mock.subscribe().unwrap();
    let session_a = session_at(&mock, &runtime_a).await;
    let session_b = session_at(&mock, &runtime_b).await;
    assert_eq!(session_a.id, session_b.id);
    assert_ne!(session_a.connection_id, session_b.connection_id);

    let run_a = mock
        .start_run(
            &session_a,
            HarnessInput::new("a").unwrap(),
            RunOptions::default(),
        )
        .await
        .unwrap();
    let run_b = mock
        .start_run(
            &session_b,
            HarnessInput::new("b").unwrap(),
            RunOptions::default(),
        )
        .await
        .unwrap();
    assert_eq!(run_a.native_id(), run_b.native_id());
    assert_ne!(run_a, run_b);

    assert!(matches!(
        next(&mut stream).await,
        HarnessEvent::RunAccepted { run, .. } if run == run_a
    ));
    let request_a = match next(&mut stream).await {
        HarnessEvent::ApprovalRequested { request, .. } => request,
        event => panic!("expected first approval, got {event:?}"),
    };
    assert!(matches!(
        next(&mut stream).await,
        HarnessEvent::RunAccepted { run, .. } if run == run_b
    ));
    let request_b = match next(&mut stream).await {
        HarnessEvent::ApprovalRequested { request, .. } => request,
        event => panic!("expected second approval, got {event:?}"),
    };
    assert_eq!(request_a.native_id(), request_b.native_id());
    assert_ne!(request_a, request_b);
    assert_eq!(request_a.run(), &run_a);
    assert_eq!(request_b.run(), &run_b);

    mock.respond_to_request(
        request_a,
        ProviderResponse::Approval(ApprovalDecision::Approved),
    )
    .await
    .unwrap();
    assert!(matches!(
        next(&mut stream).await,
        HarnessEvent::RunTerminal { run, .. } if run == run_a
    ));
    mock.respond_to_request(
        request_b,
        ProviderResponse::Approval(ApprovalDecision::Denied),
    )
    .await
    .unwrap();
    assert!(matches!(
        next(&mut stream).await,
        HarnessEvent::RunTerminal { run, .. } if run == run_b
    ));
}

#[tokio::test]
async fn runtime_faults_and_capabilities_are_scoped_to_the_registered_runtime() {
    let runtime_a = RuntimeHandle::new("runtime-a").unwrap();
    let runtime_b = RuntimeHandle::new("runtime-b").unwrap();
    let capabilities_a = HarnessCapabilities {
        mid_run_steering: true,
        ..HarnessCapabilities::default()
    };
    let capabilities_b = HarnessCapabilities {
        user_input_requests: true,
        ..HarnessCapabilities::default()
    };
    let mock = MockHarness::new(HarnessCapabilities::default(), Vec::new(), None)
        .with_runtime(
            runtime_a.clone(),
            "0193f26e-7a72-7000-8000-00000000aaa1".parse().unwrap(),
            Some(capabilities_a),
            vec![MockHarnessFault::Overloaded],
        )
        .with_runtime(
            runtime_b.clone(),
            "0193f26e-7a72-7000-8000-00000000aaa2".parse().unwrap(),
            Some(capabilities_b),
            Vec::new(),
        );

    assert!(matches!(
        mock.capabilities(&runtime_a).await,
        Err(yakshed_harness::HarnessError::Overloaded)
    ));
    assert_eq!(mock.capabilities(&runtime_b).await.unwrap(), capabilities_b);
    assert_eq!(mock.capabilities(&runtime_a).await.unwrap(), capabilities_a);
}
