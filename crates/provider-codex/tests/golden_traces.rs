use provider_codex::reducer::Reducer;
use yakshed_harness::{
    HarnessEvent, HarnessRunTerminal, NativePayload, ProviderRequestHandle, ProviderRequestId,
    ProviderRunHandle, ProviderRunId, ProviderSessionId, RuntimeHandle,
};

const TRACES: &[(&str, &str, &[&str])] = &[
    (
        "simple-answer",
        include_str!("../test-data/golden/simple-answer.jsonl"),
        &["delta:hello", "message:hello", "terminal:completed"],
    ),
    (
        "command-execution",
        include_str!("../test-data/golden/command-execution.jsonl"),
        &["command:cargo test:ok"],
    ),
    (
        "file-change",
        include_str!("../test-data/golden/file-change.jsonl"),
        &["file:src/main.rs:changed"],
    ),
    (
        "approval-accepted",
        include_str!("../test-data/golden/approval-accepted.jsonl"),
        &["approval:run tests"],
    ),
    (
        "approval-declined",
        include_str!("../test-data/golden/approval-declined.jsonl"),
        &["approval:change file"],
    ),
    (
        "user-input",
        include_str!("../test-data/golden/user-input.jsonl"),
        &["input:favorite color?"],
    ),
    (
        "steer",
        include_str!("../test-data/golden/steer.jsonl"),
        &["delta:new direction"],
    ),
    (
        "interrupt",
        include_str!("../test-data/golden/interrupt.jsonl"),
        &["terminal:interrupted"],
    ),
    (
        "unknown-event",
        include_str!("../test-data/golden/unknown-event.jsonl"),
        &["unknown:codex/futureItem"],
    ),
    (
        "malformed-frame",
        include_str!("../test-data/golden/malformed-frame.jsonl"),
        &["malformed:codex.malformed-frame"],
    ),
];

#[test]
fn golden_traces_reduce_to_normalized_snapshots() {
    for (name, trace, expected) in TRACES {
        let mut reducer = Reducer::default();
        let run = run();
        let mut actual = Vec::new();
        for line in trace.lines() {
            let event = match serde_json::from_str::<serde_json::Value>(line) {
                Ok(value) => {
                    let request = value.get("id").map(|id| {
                        ProviderRequestHandle::new(
                            run.clone(),
                            ProviderRequestId::new(id.as_str().unwrap()).unwrap(),
                        )
                    });
                    reducer.reduce(line.to_owned(), &value, Some(run.clone()), request)
                }
                Err(_) => Some(HarnessEvent::MalformedNativePayload {
                    run: Some(run.clone()),
                    item_type: "codex.malformed-frame".to_owned(),
                    native: NativePayload::sanitized(line),
                }),
            };
            if let Some(event) = event {
                assert_eq!(event.native_payload().sanitized_raw(), line, "{name}");
                actual.push(snapshot(event));
            }
        }
        assert_eq!(actual, *expected, "golden trace {name}");
    }
}

fn run() -> ProviderRunHandle {
    ProviderRunHandle::new(
        RuntimeHandle::new("golden-runtime").unwrap(),
        ProviderSessionId::new("thread-golden").unwrap(),
        ProviderRunId::new("turn-golden").unwrap(),
    )
}

fn snapshot(event: HarnessEvent) -> String {
    match event {
        HarnessEvent::MessageDelta { chunk, .. } => format!("delta:{chunk}"),
        HarnessEvent::MessageCompleted { text, .. } => format!("message:{text}"),
        HarnessEvent::CommandOutput { command, chunk, .. } => {
            format!("command:{command}:{chunk}")
        }
        HarnessEvent::FileMutation { path, summary, .. } => format!("file:{path}:{summary}"),
        HarnessEvent::ApprovalRequested { summary, .. } => format!("approval:{summary}"),
        HarnessEvent::UserInputRequested { prompt, .. } => format!("input:{prompt}"),
        HarnessEvent::RunTerminal { state, .. } => match state {
            HarnessRunTerminal::Completed => "terminal:completed".to_owned(),
            HarnessRunTerminal::Interrupted => "terminal:interrupted".to_owned(),
            HarnessRunTerminal::Failed { .. } => "terminal:failed".to_owned(),
            HarnessRunTerminal::Crashed { .. } => "terminal:crashed".to_owned(),
        },
        HarnessEvent::Unknown { item_type, .. } => format!("unknown:{item_type}"),
        HarnessEvent::MalformedNativePayload { item_type, .. } => {
            format!("malformed:{item_type}")
        }
        HarnessEvent::RunAccepted { .. } => "run:accepted".to_owned(),
    }
}
