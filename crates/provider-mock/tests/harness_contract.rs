#[path = "../../yakshed-harness/test-support/contract_suite.rs"]
mod contract_suite;

use contract_suite::{ContractScenario, HarnessContractFixture};
use provider_mock::{MockHarness, MockHarnessFault, MockRunPlan, MockScriptStep};
use yakshed_harness::{HarnessCapabilities, RuntimeHandle};

struct MockFixture {
    adapter: MockHarness,
    runtime: RuntimeHandle,
}

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
        let request_id = contract_suite::approval_request_id();
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
            ContractScenario::Overloaded => (
                MockRunPlan::new(Vec::new()),
                Some(MockHarnessFault::Overloaded),
            ),
            ContractScenario::Disconnected => (
                MockRunPlan::new(Vec::new()),
                Some(MockHarnessFault::Disconnected),
            ),
        };
        Self {
            adapter: MockHarness::new(capabilities(), vec![plan], runtime_fault),
            runtime: RuntimeHandle::new("mock-runtime").unwrap(),
        }
    }

    fn adapter(&self) -> &Self::Adapter {
        &self.adapter
    }

    fn runtime(&self) -> &RuntimeHandle {
        &self.runtime
    }

    fn expected_capabilities(&self) -> HarnessCapabilities {
        capabilities()
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
async fn unavailable_runtimes_return_typed_errors() {
    contract_suite::unavailable_runtimes_return_typed_errors::<MockFixture>().await;
}
