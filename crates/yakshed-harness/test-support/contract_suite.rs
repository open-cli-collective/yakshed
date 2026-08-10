use std::path::PathBuf;

use yakshed_domain::{ApprovalDecision, ConnectionId};
use yakshed_harness::{
    HarnessAdapter, HarnessCapabilities, HarnessError, HarnessEvent, HarnessInput,
    HarnessRunTerminal, ProviderRequestId, ProviderResponse, RunOptions, RuntimeHandle,
    SessionQuery, StartSessionSpec,
};

#[derive(Clone, Copy)]
pub enum ContractScenario {
    ChunkedRun,
    ApprovalWhileStreaming,
    InterruptibleRun,
    CrashAfterAccepted,
    UnknownNativeItem,
    Overloaded,
    Disconnected,
}

pub trait HarnessContractFixture {
    type Adapter: HarnessAdapter;

    fn create(scenario: ContractScenario) -> Self;
    fn adapter(&self) -> &Self::Adapter;
    fn runtime(&self) -> &RuntimeHandle;
    fn expected_capabilities(&self) -> HarnessCapabilities;
}

fn session_spec() -> StartSessionSpec {
    StartSessionSpec {
        connection_id: "0193f26e-7a72-7000-8000-00000000aaa1"
            .parse::<ConnectionId>()
            .unwrap(),
        working_directory: PathBuf::from("/contract/workspace"),
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
    let run_id = fixture
        .adapter()
        .start_run(
            &session,
            HarnessInput::new("start").unwrap(),
            RunOptions::default(),
        )
        .await
        .unwrap();
    assert!(matches!(
        next(&mut stream).await,
        HarnessEvent::RunAccepted { .. }
    ));
    fixture
        .adapter()
        .steer(&run_id, HarnessInput::new("new direction").unwrap())
        .await
        .unwrap();
    assert!(matches!(
        next(&mut stream).await,
        HarnessEvent::MessageDelta { chunk, .. } if chunk == "new direction"
    ));
    fixture.adapter().interrupt(&run_id).await.unwrap();
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
                after: first_page.next_after,
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
    let run_id = fixture
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
    loop {
        match next(&mut stream).await {
            HarnessEvent::RunAccepted { run_id: event, .. } => assert_eq!(event, run_id),
            HarnessEvent::MessageDelta { chunk, .. } => text.push_str(&chunk),
            HarnessEvent::MessageCompleted { text, .. } => finalized_text = Some(text),
            HarnessEvent::FileMutation { .. } => saw_file = true,
            HarnessEvent::CommandOutput { .. } => saw_command = true,
            HarnessEvent::RunTerminal {
                state: HarnessRunTerminal::Completed,
                ..
            } => break,
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
    fixture
        .adapter()
        .start_run(
            &session,
            HarnessInput::new("needs approval").unwrap(),
            RunOptions::default(),
        )
        .await
        .unwrap();
    assert!(matches!(
        next(&mut stream).await,
        HarnessEvent::RunAccepted { .. }
    ));
    let request_id = match next(&mut stream).await {
        HarnessEvent::ApprovalRequested { request_id, .. } => request_id,
        event => panic!("expected approval request, got {event:?}"),
    };
    assert_eq!(
        next(&mut stream).await,
        HarnessEvent::MessageDelta {
            run_id: "run-0001".parse().unwrap(),
            chunk: "reader-still-live".to_owned(),
            native: yakshed_harness::NativePayload::new(
                r#"{"type":"message.delta","delta":"reader-still-live"}"#,
            ),
        }
    );
    fixture
        .adapter()
        .respond_to_request(
            request_id,
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

pub async fn interrupt_has_typed_terminal_semantics<F: HarnessContractFixture>() {
    let fixture = F::create(ContractScenario::InterruptibleRun);
    let mut stream = fixture.adapter().subscribe().unwrap();
    let session = session(&fixture).await;
    let run_id = fixture
        .adapter()
        .start_run(
            &session,
            HarnessInput::new("never finish").unwrap(),
            RunOptions::default(),
        )
        .await
        .unwrap();
    assert!(matches!(
        next(&mut stream).await,
        HarnessEvent::RunAccepted { .. }
    ));
    fixture.adapter().interrupt(&run_id).await.unwrap();
    assert!(matches!(
        next(&mut stream).await,
        HarnessEvent::RunTerminal {
            state: HarnessRunTerminal::Interrupted,
            ..
        }
    ));
}

pub async fn crash_mid_run_is_a_typed_terminal_event<F: HarnessContractFixture>() {
    let fixture = F::create(ContractScenario::CrashAfterAccepted);
    let mut stream = fixture.adapter().subscribe().unwrap();
    let session = session(&fixture).await;
    fixture
        .adapter()
        .start_run(
            &session,
            HarnessInput::new("crash").unwrap(),
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
            state: HarnessRunTerminal::Crashed { .. },
            ..
        }
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
            assert_eq!(item_type, "mock.future-item");
            assert_eq!(
                native.as_str(),
                r#"{"type":"mock.future-item","answer":42}"#
            );
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

pub fn approval_request_id() -> ProviderRequestId {
    "request-0001".parse().unwrap()
}
