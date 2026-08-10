#[path = "../../yakshed-harness/test-support/contract_suite.rs"]
mod contract_suite;

use contract_suite::{ContractScenario, HarnessContractFixture};
use provider_mock::{MockHarness, MockHarnessFault, MockRunPlan, MockScriptStep};
use yakshed_harness::{
    HarnessCapabilities, ProviderRequestId, RuntimeHandle, RuntimePath, StartSessionSpec,
};

struct MockFixture {
    adapter: MockHarness,
    runtime: RuntimeHandle,
}

const CREDENTIAL_CANARY: &str = "YAKSHED_CREDENTIAL_CANARY_DO_NOT_EMIT";

fn capabilities() -> HarnessCapabilities {
    HarnessCapabilities {
        persistent_sessions: true,
        session_listing: true,
        native_fork: false,
        session_archive: false,
        mid_run_steering: true,
        client_approvals: true,
        user_input_requests: true,
        structured_file_changes: true,
        command_output_streaming: true,
        native_subagent_lineage: false,
        images: false,
        skills: false,
        mcp: false,
        account_ui: false,
        model_discovery: true,
    }
}

impl HarnessContractFixture for MockFixture {
    type Adapter = MockHarness;

    fn create(scenario: ContractScenario) -> Self {
        let request_id = "request-0001".parse::<ProviderRequestId>().unwrap();
        let (plan, runtime_fault) = match scenario {
            ContractScenario::ChunkedRun => (
                MockRunPlan::new(vec![
                    MockScriptStep::message("hel"),
                    MockScriptStep::message("lo"),
                    MockScriptStep::message_completed("hello"),
                    MockScriptStep::file_mutation("src/main.rs", "updated"),
                    MockScriptStep::command_output("cargo test", "ok"),
                    MockScriptStep::complete(),
                ]),
                None,
            ),
            ContractScenario::ApprovalWhileStreaming => (
                MockRunPlan::new(vec![
                    MockScriptStep::approval(request_id.clone(), "run command"),
                    MockScriptStep::message("reader-still-live"),
                    MockScriptStep::await_response(request_id),
                    MockScriptStep::complete(),
                ]),
                None,
            ),
            ContractScenario::UserInputWhileStreaming => (
                MockRunPlan::new(vec![
                    MockScriptStep::user_input(request_id.clone(), "favorite color?"),
                    MockScriptStep::await_response(request_id),
                    MockScriptStep::message("input-accepted"),
                    MockScriptStep::complete(),
                ]),
                None,
            ),
            ContractScenario::InterruptibleRun => (
                MockRunPlan::new(Vec::new()).with_fault(MockHarnessFault::NeverComplete),
                None,
            ),
            ContractScenario::CrashAfterAccepted => (
                MockRunPlan::new(Vec::new()).with_fault(MockHarnessFault::ExitAfterRunAccepted),
                None,
            ),
            ContractScenario::UnknownNativeItem => (
                MockRunPlan::new(vec![MockScriptStep::complete()])
                    .with_fault(MockHarnessFault::EmitUnknownEvent),
                None,
            ),
            ContractScenario::MalformedNativeItem => (
                MockRunPlan::new(vec![
                    MockScriptStep::malformed("mock.malformed", "{not-json"),
                    MockScriptStep::complete(),
                ]),
                None,
            ),
            ContractScenario::Overloaded => (
                MockRunPlan::new(Vec::new()),
                Some(MockHarnessFault::Overloaded),
            ),
            ContractScenario::Disconnected => (
                MockRunPlan::new(Vec::new()),
                Some(MockHarnessFault::Disconnected),
            ),
            ContractScenario::CredentialCanaryEvent => (
                MockRunPlan::new(vec![
                    MockScriptStep::unknown(
                        "provider.native",
                        format!(r#"{{"credential":"{CREDENTIAL_CANARY}"}}"#),
                    ),
                    MockScriptStep::complete(),
                ]),
                None,
            ),
            ContractScenario::CredentialCanaryError => (
                MockRunPlan::new(Vec::new()),
                Some(MockHarnessFault::ProtocolFailure),
            ),
        };
        let runtime = RuntimeHandle::new("mock-runtime").unwrap();
        Self {
            adapter: MockHarness::new(capabilities(), vec![plan], None)
                .with_runtime(
                    runtime.clone(),
                    "0193f26e-7a72-7000-8000-00000000aaa1".parse().unwrap(),
                    None,
                    runtime_fault.into_iter().collect(),
                )
                .with_native_redaction(CREDENTIAL_CANARY),
            runtime,
        }
    }

    fn adapter(&self) -> &Self::Adapter {
        &self.adapter
    }

    fn runtime(&self) -> &RuntimeHandle {
        &self.runtime
    }

    fn session_spec(&self) -> StartSessionSpec {
        StartSessionSpec {
            working_directory: RuntimePath::new("mock-runtime://workspace").unwrap(),
            title: "mock contract session".to_owned(),
        }
    }

    fn expected_capabilities(&self) -> HarnessCapabilities {
        capabilities()
    }

    fn expected_unknown_item_type(&self) -> &str {
        "mock.future-item"
    }

    fn expected_unknown_payload(&self) -> &str {
        r#"{"type":"mock.future-item","answer":42}"#
    }

    fn expected_malformed_item_type(&self) -> &str {
        "mock.malformed"
    }

    fn expected_malformed_payload(&self) -> &str {
        "{not-json"
    }

    fn credential_canary(&self) -> &str {
        CREDENTIAL_CANARY
    }
}

#[tokio::test]
async fn capability_querying() {
    contract_suite::capability_querying::<MockFixture>().await;
}

#[tokio::test]
async fn session_start_list_and_resume() {
    contract_suite::session_start_list_and_resume::<MockFixture>().await;
}

#[tokio::test]
async fn steering_an_active_run_emits_normalized_input() {
    contract_suite::steering_an_active_run_emits_normalized_input::<MockFixture>().await;
}

#[tokio::test]
async fn run_lifecycle_streams_chunked_file_and_command_events() {
    contract_suite::run_lifecycle_streams_chunked_file_and_command_events::<MockFixture>().await;
}

#[tokio::test]
async fn approval_response_does_not_block_event_stream() {
    contract_suite::approval_response_does_not_block_event_stream::<MockFixture>().await;
}

#[tokio::test]
async fn user_input_request_response_continues_run() {
    contract_suite::user_input_request_response_continues_run::<MockFixture>().await;
}

#[tokio::test]
async fn interrupt_has_typed_terminal_semantics() {
    contract_suite::interrupt_has_typed_terminal_semantics::<MockFixture>().await;
}

#[tokio::test]
async fn crash_mid_run_is_a_typed_terminal_event() {
    contract_suite::crash_mid_run_is_a_typed_terminal_event::<MockFixture>().await;
}

#[tokio::test]
async fn unknown_native_items_are_preserved() {
    contract_suite::unknown_native_items_are_preserved::<MockFixture>().await;
}

#[tokio::test]
async fn malformed_native_items_are_preserved_and_stream_continues() {
    contract_suite::malformed_native_items_are_preserved_and_stream_continues::<MockFixture>()
        .await;
}

#[tokio::test]
async fn unavailable_runtimes_return_typed_errors() {
    contract_suite::unavailable_runtimes_return_typed_errors::<MockFixture>().await;
}

#[tokio::test]
async fn credential_canary_is_redacted_from_events_and_errors() {
    contract_suite::credential_canary_is_redacted_from_events_and_errors::<MockFixture>().await;
}
