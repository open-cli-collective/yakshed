use yakshed_domain::{ApprovalDecision, ConnectionId};
use yakshed_harness::{
    HarnessAdapter, HarnessCapabilities, HarnessError, HarnessEvent, HarnessInput,
    HarnessRunTerminal, ProviderResponse, RunOptions, RuntimeHandle, RuntimePath, SessionQuery,
    StartSessionSpec,
};

#[derive(Clone, Copy)]
pub enum ContractScenario {
    ChunkedRun,
    ApprovalWhileStreaming,
    UserInputWhileStreaming,
    InterruptibleRun,
    CrashAfterAccepted,
    UnknownNativeItem,
    Overloaded,
    Disconnected,
    CredentialCanaryEvent,
    CredentialCanaryError,
}

pub trait HarnessContractFixture {
    type Adapter: HarnessAdapter;

    fn create(scenario: ContractScenario) -> Self;
    fn adapter(&self) -> &Self::Adapter;
    fn runtime(&self) -> &RuntimeHandle;
    fn expected_capabilities(&self) -> HarnessCapabilities;
    fn expected_unknown_item_type(&self) -> &str;
    fn expected_unknown_payload(&self) -> &str;
    fn credential_canary(&self) -> &str;
}

fn session_spec() -> StartSessionSpec {
    StartSessionSpec {
        connection_id: "0193f26e-7a72-7000-8000-00000000aaa1"
            .parse::<ConnectionId>()
            .unwrap(),
        working_directory: RuntimePath::new("contract-runtime://workspace").unwrap(),
        title: "contract session".to_owned(),
    }
}

async fn session<F: HarnessContractFixture>(fixture: &F) -> yakshed_harness::ProviderSession {
    fixture
        .adapter()
        .start_session(fixture.runtime(), session_spec())
        .await
        .unwrap()
}

async fn next(stream: &mut yakshed_harness::ProviderEventStream) -> HarnessEvent {
    tokio::time::timeout(std::time::Duration::from_millis(250), stream.recv())
        .await
        .expect("harness event timed out")
        .expect("harness event stream closed")
}

pub async fn capability_querying<F: HarnessContractFixture>() {
    let fixture = F::create(ContractScenario::ChunkedRun);
    let descriptor = fixture.adapter().descriptor();
    assert!(!descriptor.id.is_empty());
    assert!(!descriptor.name.is_empty());
    assert_eq!(
        fixture
            .adapter()
            .capabilities(fixture.runtime())
            .await
            .unwrap(),
        fixture.expected_capabilities()
    );
}

pub async fn steering_an_active_run_emits_normalized_input<F: HarnessContractFixture>() {
    let fixture = F::create(ContractScenario::InterruptibleRun);
    let mut stream = fixture.adapter().subscribe().unwrap();
    let session = session(&fixture).await;
    let returned_run = fixture
        .adapter()
        .start_run(
            &session,
            HarnessInput::new("start").unwrap(),
            RunOptions::default(),
        )
        .await
        .unwrap();
    let accepted_run = match next(&mut stream).await {
        HarnessEvent::RunAccepted { run, .. } => run,
        event => panic!("expected run acceptance, got {event:?}"),
    };
    assert_eq!(accepted_run, returned_run);
    fixture
        .adapter()
        .steer(&accepted_run, HarnessInput::new("new direction").unwrap())
        .await
        .unwrap();
    assert!(matches!(
        next(&mut stream).await,
        HarnessEvent::MessageDelta { run, chunk, .. }
            if run == accepted_run && chunk == "new direction"
    ));
    fixture.adapter().interrupt(&accepted_run).await.unwrap();
}

pub async fn session_start_list_and_resume<F: HarnessContractFixture>() {
    let fixture = F::create(ContractScenario::ChunkedRun);
    let started = session(&fixture).await;
    let second = session(&fixture).await;
    let third = session(&fixture).await;
    let first_page = fixture
        .adapter()
        .list_sessions(
            fixture.runtime(),
            SessionQuery {
                after: None,
                limit: 2,
            },
        )
        .await
        .unwrap();
    assert_eq!(first_page.items.len(), 2);
    assert_eq!(first_page.items[0].id, started.id);
    assert_eq!(first_page.items[1].id, second.id);
    let second_page = fixture
        .adapter()
        .list_sessions(
            fixture.runtime(),
            SessionQuery {
                after: first_page.next,
                limit: 2,
            },
        )
        .await
        .unwrap();
    assert_eq!(second_page.items.len(), 1);
    assert_eq!(second_page.items[0].id, third.id);
    assert_eq!(
        fixture
            .adapter()
            .resume_session(fixture.runtime(), &started.id)
            .await
            .unwrap(),
        started
    );
}

pub async fn run_lifecycle_streams_chunked_file_and_command_events<F: HarnessContractFixture>() {
    let fixture = F::create(ContractScenario::ChunkedRun);
    let mut stream = fixture.adapter().subscribe().unwrap();
    let session = session(&fixture).await;
    let returned_run = fixture
        .adapter()
        .start_run(
            &session,
            HarnessInput::new("do the work").unwrap(),
            RunOptions::default(),
        )
        .await
        .unwrap();
    let mut text = String::new();
    let mut finalized_text = None;
    let mut saw_file = false;
    let mut saw_command = false;
    let mut accepted_run = None;
    loop {
        match next(&mut stream).await {
            HarnessEvent::RunAccepted { run, .. } => {
                assert_eq!(run, returned_run);
                accepted_run = Some(run);
            }
            HarnessEvent::MessageDelta { run, chunk, .. } => {
                assert_eq!(Some(&run), accepted_run.as_ref());
                text.push_str(&chunk);
            }
            HarnessEvent::MessageCompleted {
                run, text: body, ..
            } => {
                assert_eq!(Some(&run), accepted_run.as_ref());
                finalized_text = Some(body);
            }
            HarnessEvent::FileMutation { run, .. } => {
                assert_eq!(Some(&run), accepted_run.as_ref());
                saw_file = true;
            }
            HarnessEvent::CommandOutput { run, .. } => {
                assert_eq!(Some(&run), accepted_run.as_ref());
                saw_command = true;
            }
            HarnessEvent::RunTerminal {
                run,
                state: HarnessRunTerminal::Completed,
                ..
            } => {
                assert_eq!(Some(&run), accepted_run.as_ref());
                break;
            }
            event => panic!("unexpected event: {event:?}"),
        }
    }
    assert_eq!(text, "hello");
    assert_eq!(finalized_text.as_deref(), Some("hello"));
    assert!(saw_file);
    assert!(saw_command);
}

pub async fn approval_response_does_not_block_event_stream<F: HarnessContractFixture>() {
    let fixture = F::create(ContractScenario::ApprovalWhileStreaming);
    let mut stream = fixture.adapter().subscribe().unwrap();
    let session = session(&fixture).await;
    let returned_run = fixture
        .adapter()
        .start_run(
            &session,
            HarnessInput::new("needs approval").unwrap(),
            RunOptions::default(),
        )
        .await
        .unwrap();
    let accepted_run = match next(&mut stream).await {
        HarnessEvent::RunAccepted { run, .. } => run,
        event => panic!("expected run acceptance, got {event:?}"),
    };
    assert_eq!(accepted_run, returned_run);
    let request = match next(&mut stream).await {
        HarnessEvent::ApprovalRequested { request, .. } => request,
        event => panic!("expected approval request, got {event:?}"),
    };
    assert_eq!(request.run(), &accepted_run);
    assert!(matches!(
        next(&mut stream).await,
        HarnessEvent::MessageDelta { run, chunk, .. }
            if run == accepted_run && chunk == "reader-still-live"
    ));
    fixture
        .adapter()
        .respond_to_request(
            request,
            ProviderResponse::Approval(ApprovalDecision::Approved),
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

pub async fn user_input_request_response_continues_run<F: HarnessContractFixture>() {
    let fixture = F::create(ContractScenario::UserInputWhileStreaming);
    let mut stream = fixture.adapter().subscribe().unwrap();
    let session = session(&fixture).await;
    let returned_run = fixture
        .adapter()
        .start_run(
            &session,
            HarnessInput::new("needs input").unwrap(),
            RunOptions::default(),
        )
        .await
        .unwrap();
    let accepted_run = match next(&mut stream).await {
        HarnessEvent::RunAccepted { run, .. } => run,
        event => panic!("expected run acceptance, got {event:?}"),
    };
    assert_eq!(accepted_run, returned_run);
    let request = match next(&mut stream).await {
        HarnessEvent::UserInputRequested {
            request, prompt, ..
        } => {
            assert!(!prompt.is_empty());
            request
        }
        event => panic!("expected user-input request, got {event:?}"),
    };
    assert_eq!(request.run(), &accepted_run);
    fixture
        .adapter()
        .respond_to_request(request, ProviderResponse::UserInput("blue".to_owned()))
        .await
        .unwrap();
    assert!(matches!(
        next(&mut stream).await,
        HarnessEvent::MessageDelta { run, chunk, .. }
            if run == accepted_run && chunk == "input-accepted"
    ));
    assert!(matches!(
        next(&mut stream).await,
        HarnessEvent::RunTerminal {
            run,
            state: HarnessRunTerminal::Completed,
            ..
        } if run == accepted_run
    ));
}

pub async fn interrupt_has_typed_terminal_semantics<F: HarnessContractFixture>() {
    let fixture = F::create(ContractScenario::InterruptibleRun);
    let mut stream = fixture.adapter().subscribe().unwrap();
    let session = session(&fixture).await;
    let returned_run = fixture
        .adapter()
        .start_run(
            &session,
            HarnessInput::new("never finish").unwrap(),
            RunOptions::default(),
        )
        .await
        .unwrap();
    let accepted_run = match next(&mut stream).await {
        HarnessEvent::RunAccepted { run, .. } => run,
        event => panic!("expected run acceptance, got {event:?}"),
    };
    assert_eq!(accepted_run, returned_run);
    fixture.adapter().interrupt(&accepted_run).await.unwrap();
    assert!(matches!(
        next(&mut stream).await,
        HarnessEvent::RunTerminal {
            run,
            state: HarnessRunTerminal::Interrupted,
            ..
        } if run == accepted_run
    ));
}

pub async fn crash_mid_run_is_a_typed_terminal_event<F: HarnessContractFixture>() {
    let fixture = F::create(ContractScenario::CrashAfterAccepted);
    let mut stream = fixture.adapter().subscribe().unwrap();
    let session = session(&fixture).await;
    let returned_run = fixture
        .adapter()
        .start_run(
            &session,
            HarnessInput::new("crash").unwrap(),
            RunOptions::default(),
        )
        .await
        .unwrap();
    let accepted_run = match next(&mut stream).await {
        HarnessEvent::RunAccepted { run, .. } => run,
        event => panic!("expected run acceptance, got {event:?}"),
    };
    assert_eq!(accepted_run, returned_run);
    assert!(matches!(
        next(&mut stream).await,
        HarnessEvent::RunTerminal {
            run,
            state: HarnessRunTerminal::Crashed { .. },
            ..
        } if run == accepted_run
    ));
}

pub async fn unknown_native_items_are_preserved<F: HarnessContractFixture>() {
    let fixture = F::create(ContractScenario::UnknownNativeItem);
    let mut stream = fixture.adapter().subscribe().unwrap();
    let session = session(&fixture).await;
    fixture
        .adapter()
        .start_run(
            &session,
            HarnessInput::new("unknown").unwrap(),
            RunOptions::default(),
        )
        .await
        .unwrap();
    assert!(matches!(
        next(&mut stream).await,
        HarnessEvent::RunAccepted { .. }
    ));
    match next(&mut stream).await {
        HarnessEvent::Unknown {
            item_type, native, ..
        } => {
            assert_eq!(item_type, fixture.expected_unknown_item_type());
            assert_eq!(native.sanitized_raw(), fixture.expected_unknown_payload());
        }
        event => panic!("expected unknown event, got {event:?}"),
    }
}

pub async fn unavailable_runtimes_return_typed_errors<F: HarnessContractFixture>() {
    let overloaded = F::create(ContractScenario::Overloaded);
    assert!(matches!(
        overloaded
            .adapter()
            .capabilities(overloaded.runtime())
            .await,
        Err(HarnessError::Overloaded)
    ));
    let disconnected = F::create(ContractScenario::Disconnected);
    assert!(matches!(
        disconnected
            .adapter()
            .capabilities(disconnected.runtime())
            .await,
        Err(HarnessError::Disconnected)
    ));
}

pub async fn credential_canary_is_redacted_from_events_and_errors<F: HarnessContractFixture>() {
    let fixture = F::create(ContractScenario::CredentialCanaryEvent);
    let canary = fixture.credential_canary().to_owned();
    let mut stream = fixture.adapter().subscribe().unwrap();
    let session = session(&fixture).await;
    fixture
        .adapter()
        .start_run(
            &session,
            HarnessInput::new("redact native payload").unwrap(),
            RunOptions::default(),
        )
        .await
        .unwrap();
    loop {
        let event = next(&mut stream).await;
        assert!(!event.native_payload().sanitized_raw().contains(&canary));
        assert!(!format!("{event:?}").contains(&canary));
        assert!(!format!("{event}").contains(&canary));
        if matches!(event, HarnessEvent::RunTerminal { .. }) {
            break;
        }
    }

    let error_fixture = F::create(ContractScenario::CredentialCanaryError);
    let error = error_fixture
        .adapter()
        .capabilities(error_fixture.runtime())
        .await
        .unwrap_err();
    assert!(!format!("{error}").contains(&canary));
    assert!(!format!("{error:?}").contains(&canary));
    if let HarnessError::Protocol { diagnostic } = error {
        assert!(!diagnostic.sanitized_text().contains(&canary));
    } else {
        panic!("expected sanitized protocol error");
    }
}
