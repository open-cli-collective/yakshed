#[path = "../../yakshed-harness/test-support/contract_suite.rs"]
mod contract_suite;

use std::path::PathBuf;

use contract_suite::{ContractScenario, HarnessContractFixture};
use provider_codex::{CodexAdapter, CodexRuntimeKey, CodexRuntimeSpec};
use yakshed_harness::{HarnessCapabilities, RuntimeHandle, RuntimePath, StartSessionSpec};

const CREDENTIAL_CANARY: &str = "YAKSHED_CREDENTIAL_CANARY_DO_NOT_EMIT";

struct CodexFixture {
    adapter: CodexAdapter,
    runtime: RuntimeHandle,
    workspace: PathBuf,
    _root: tempfile::TempDir,
}

fn capabilities() -> HarnessCapabilities {
    HarnessCapabilities {
        persistent_sessions: true,
        session_listing: true,
        native_fork: false,
        session_archive: true,
        mid_run_steering: true,
        client_approvals: true,
        user_input_requests: true,
        structured_file_changes: true,
        command_output_streaming: true,
        native_subagent_lineage: true,
        images: true,
        skills: true,
        mcp: true,
        account_ui: true,
        model_discovery: true,
    }
}

impl HarnessContractFixture for CodexFixture {
    type Adapter = CodexAdapter;

    fn create(scenario: ContractScenario) -> Self {
        let scenario = match scenario {
            ContractScenario::ChunkedRun => "chunked",
            ContractScenario::ApprovalWhileStreaming => "approval",
            ContractScenario::UserInputWhileStreaming => "user_input",
            ContractScenario::InterruptibleRun => "interruptible",
            ContractScenario::CrashAfterAccepted => "crash",
            ContractScenario::UnknownNativeItem => "unknown",
            ContractScenario::MalformedNativeItem => "malformed",
            ContractScenario::Overloaded => "overloaded",
            ContractScenario::Disconnected => "disconnected",
            ContractScenario::CredentialCanaryEvent => "canary_event",
            ContractScenario::CredentialCanaryError => "canary_error",
        };
        let root = tempfile::tempdir().unwrap();
        let codex_home = root.path().join("codex-home");
        let workspace = root.path().join("workspace");
        std::fs::create_dir_all(&codex_home).unwrap();
        std::fs::create_dir_all(&workspace).unwrap();
        let runtime = RuntimeHandle::new("codex-test-runtime").unwrap();
        let key = CodexRuntimeKey {
            connection_id: "0193f26e-7a72-7000-8000-00000000ccc1".parse().unwrap(),
            binary_digest: "fake-codex-schema-0.147.0".to_owned(),
            codex_home,
            execution_runtime: "local-test".to_owned(),
        };
        let script = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fake_codex.py");
        let mut spec = CodexRuntimeSpec::local(
            runtime.clone(),
            key,
            PathBuf::from("python3"),
            vec![script.display().to_string(), scenario.to_owned()],
        );
        spec.redactions.push(CREDENTIAL_CANARY.to_owned());
        Self {
            adapter: CodexAdapter::new(spec).unwrap(),
            runtime,
            workspace,
            _root: root,
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
            working_directory: RuntimePath::new(self.workspace.display().to_string()).unwrap(),
            title: "codex contract session".to_owned(),
        }
    }

    fn expected_capabilities(&self) -> HarnessCapabilities {
        capabilities()
    }

    fn expected_unknown_item_type(&self) -> &str {
        "codex/future"
    }

    fn expected_unknown_payload(&self) -> &str {
        r#"{"method":"codex/future","params":{"threadId":"thread-1","turnId":"turn-1","answer":42}}"#
    }

    fn expected_malformed_item_type(&self) -> &str {
        "codex.malformed-frame"
    }

    fn expected_malformed_payload(&self) -> &str {
        "{not-json"
    }

    fn credential_canary(&self) -> &str {
        CREDENTIAL_CANARY
    }
}

macro_rules! contract_test {
    ($name:ident) => {
        #[tokio::test]
        async fn $name() {
            contract_suite::$name::<CodexFixture>().await;
        }
    };
}

contract_test!(capability_querying);
contract_test!(session_start_list_and_resume);
contract_test!(steering_an_active_run_emits_normalized_input);
contract_test!(run_lifecycle_streams_chunked_file_and_command_events);
contract_test!(approval_response_does_not_block_event_stream);
contract_test!(user_input_request_response_continues_run);
contract_test!(interrupt_has_typed_terminal_semantics);
contract_test!(crash_mid_run_is_a_typed_terminal_event);
contract_test!(unknown_native_items_are_preserved);
contract_test!(malformed_native_items_are_preserved_and_stream_continues);
contract_test!(unavailable_runtimes_return_typed_errors);
contract_test!(credential_canary_is_redacted_from_events_and_errors);
