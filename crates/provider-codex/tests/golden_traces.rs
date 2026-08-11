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
        &[
            "command_delta:golden-runtime/thread-golden/turn-golden/command-1:ok",
            "command_completed:golden-runtime/thread-golden/turn-golden/command-1:ok",
        ],
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
            let events = match serde_json::from_str::<serde_json::Value>(line) {
                Ok(value) => {
                    let request = value.get("id").map(|id| {
                        ProviderRequestHandle::new(
                            run.clone(),
                            ProviderRequestId::new(id.as_str().unwrap()).unwrap(),
                        )
                    });
                    reducer.reduce(line.to_owned(), &value, Some(run.clone()), request)
                }
                Err(_) => vec![HarnessEvent::MalformedNativePayload {
                    run: Some(run.clone()),
                    item_type: "codex.malformed-frame".to_owned(),
                    native: NativePayload::sanitized(line),
                }],
            };
            for event in events {
                assert_eq!(event.native_payload().sanitized_raw(), line, "{name}");
                actual.push(snapshot(event));
            }
        }
        assert_eq!(actual, *expected, "golden trace {name}");
    }
}

#[test]
fn reducer_scopes_colliding_item_ids_to_their_runs() {
    let mut reducer = Reducer::default();
    let run_a = run_named("turn-a");
    let run_b = run_named("turn-b");
    for (run, command) in [(&run_a, "command a"), (&run_b, "command b")] {
        let value = serde_json::json!({
            "method": "item/started",
            "params": {
                "item": {"id": "command-1", "type": "commandExecution", "command": command}
            }
        });
        assert!(
            reducer
                .reduce(value.to_string(), &value, Some(run.clone()), None)
                .is_empty()
        );
    }

    for (run, expected) in [(&run_a, "command a"), (&run_b, "command b")] {
        let value = serde_json::json!({
            "method": "item/commandExecution/outputDelta",
            "params": {"itemId": "command-1", "delta": "output"}
        });
        match reducer
            .reduce(value.to_string(), &value, Some(run.clone()), None)
            .into_iter()
            .next()
            .unwrap()
        {
            HarnessEvent::CommandOutputDelta {
                run: actual,
                command,
                command_text,
                ..
            } => {
                assert_eq!(actual, *run);
                assert_eq!(&*command_text, expected);
                assert_eq!(command.run(), run);
                assert_eq!(command.native_id().as_str(), "command-1");
            }
            event => panic!("expected command output, got {event:?}"),
        }
    }

    let completed = serde_json::json!({
        "method": "item/completed",
        "params": {
            "item": {
                "id": "command-1",
                "type": "commandExecution",
                "command": "command a",
                "aggregatedOutput": "output"
            }
        }
    });
    let completed_events =
        reducer.reduce(completed.to_string(), &completed, Some(run_a.clone()), None);
    assert!(matches!(
        completed_events.first(),
        Some(HarnessEvent::CommandOutputCompleted {
            command_text, ..
        }) if command_text == "command a"
    ));
    assert!(matches!(
        reducer
            .reduce(
                serde_json::json!({
                    "method": "item/commandExecution/outputDelta",
                    "params": {"itemId":"command-1","delta":"late"}
                })
                .to_string(),
                &serde_json::json!({
                    "method": "item/commandExecution/outputDelta",
                    "params": {"itemId":"command-1","delta":"late"}
                }),
                Some(run_a.clone()),
                None
            )
            .as_slice(),
        [HarnessEvent::MalformedNativePayload { .. }]
    ));

    let terminal = serde_json::json!({
        "method": "turn/completed",
        "params": {"turn": {"status": "completed"}}
    });
    reducer.reduce(terminal.to_string(), &terminal, Some(run_b.clone()), None);
    assert!(
        reducer
            .reduce(
                serde_json::json!({
                    "method": "item/completed",
                    "params": {
                        "item": {
                            "id": "command-1",
                            "type": "commandExecution",
                            "aggregatedOutput": "again",
                        }
                    }
                })
                .to_string(),
                &serde_json::json!({
                    "method": "item/completed",
                    "params": {
                        "item": {
                            "id": "command-1",
                            "type": "commandExecution",
                            "aggregatedOutput": "again",
                        }
                    }
                }),
                Some(run_b.clone()),
                None,
            )
            .into_iter()
            .all(|event| matches!(event, HarnessEvent::MalformedNativePayload { .. }))
    );
}

#[test]
fn command_delta_without_registered_command_is_malformed() {
    let mut reducer = Reducer::default();
    let run = run();
    let delta = serde_json::json!({
        "method": "item/commandExecution/outputDelta",
        "params": {"itemId": "command-1", "delta": "output"}
    });
    assert!(matches!(
        reducer
            .reduce(delta.to_string(), &delta, Some(run), None)
            .as_slice(),
        [HarnessEvent::MalformedNativePayload { .. }]
    ));
}

#[test]
fn command_completion_without_cached_and_native_command_is_malformed() {
    let mut reducer = Reducer::default();
    let run = run();
    let value = serde_json::json!({
        "method": "item/completed",
        "params": {
            "item": {
                "id": "command-1",
                "type": "commandExecution",
                "aggregatedOutput": "oops",
            }
        }
    });
    assert!(matches!(
        reducer
            .reduce(value.to_string(), &value, Some(run), None)
            .as_slice(),
        [HarnessEvent::MalformedNativePayload { .. }]
    ));
}

#[test]
fn command_completion_disagreeing_with_start_is_malformed() {
    let mut reducer = Reducer::default();
    let run = run();
    let start = serde_json::json!({
        "method": "item/started",
        "params": {
            "item": {
                "id": "command-1",
                "type": "commandExecution",
                "command": "cargo test"
            }
        }
    });
    assert!(
        reducer
            .reduce(start.to_string(), &start, Some(run.clone()), None)
            .is_empty()
    );
    let completion = serde_json::json!({
        "method": "item/completed",
        "params": {
            "item": {
                "id": "command-1",
                "type": "commandExecution",
                "command": "other command",
                "aggregatedOutput": "oops",
            }
        }
    });
    assert!(matches!(
        reducer
            .reduce(completion.to_string(), &completion, Some(run.clone()), None)
            .as_slice(),
        [HarnessEvent::MalformedNativePayload { .. }]
    ));
    assert!(matches!(
        reducer
            .reduce(completion.to_string(), &completion, Some(run), None)
            .as_slice(),
        [HarnessEvent::MalformedNativePayload { .. }]
    ));
}

#[test]
fn structurally_invalid_known_item_is_visible_and_recovery_continues() {
    let mut reducer = Reducer::default();
    let run = run();
    let malformed = serde_json::json!({
        "method": "item/agentMessage/delta",
        "params": {"itemId": "message-1"}
    });
    assert!(matches!(
        reducer
            .reduce(malformed.to_string(), &malformed, Some(run.clone()), None)
            .as_slice(),
        [HarnessEvent::MalformedNativePayload { .. }]
    ));

    let terminal = serde_json::json!({
        "method": "turn/completed",
        "params": {"turn": {"status": "completed"}}
    });
    assert!(matches!(
        reducer
            .reduce(terminal.to_string(), &terminal, Some(run), None)
            .as_slice(),
        [HarnessEvent::RunTerminal {
            state: HarnessRunTerminal::Completed,
            ..
        }]
    ));
}

#[test]
fn file_delta_stays_native_and_completion_emits_all_paths_in_order() {
    let mut reducer = Reducer::default();
    let run = run();
    let delta = serde_json::json!({
        "method": "item/fileChange/outputDelta",
        "params": {"itemId": "file-1", "delta": "provider detail"}
    });
    let completed = serde_json::json!({
        "method": "item/completed",
        "params": {
            "item": {
                "id": "file-1",
                "type": "fileChange",
                "changes": [
                    {"path": "src/lib.rs", "diff": "changed"},
                    {"path": "src/main.rs", "diff": "added"}
                ]
            }
        }
    });
    let events = [delta, completed]
        .into_iter()
        .flat_map(|value| reducer.reduce(value.to_string(), &value, Some(run.clone()), None))
        .collect::<Vec<_>>();

    assert!(!events.iter().any(|event| matches!(
        event,
        HarnessEvent::FileMutation { path, .. } if path == "file-1"
    )));
    let paths = events
        .iter()
        .filter_map(|event| match event {
            HarnessEvent::FileMutation { path, .. } => Some(path.as_str()),
            _ => None,
        })
        .collect::<Vec<_>>();
    assert_eq!(paths, ["src/lib.rs", "src/main.rs"]);
}

fn run() -> ProviderRunHandle {
    run_named("turn-golden")
}

fn run_named(turn: &str) -> ProviderRunHandle {
    ProviderRunHandle::new(
        RuntimeHandle::new("golden-runtime").unwrap(),
        ProviderSessionId::new("thread-golden").unwrap(),
        ProviderRunId::new(turn).unwrap(),
    )
}

fn snapshot(event: HarnessEvent) -> String {
    match event {
        HarnessEvent::MessageDelta { chunk, .. } => format!("delta:{chunk}"),
        HarnessEvent::MessageCompleted { text, .. } => format!("message:{text}"),
        HarnessEvent::CommandOutputDelta { command, chunk, .. } => {
            format!("command_delta:{command}:{chunk}")
        }
        HarnessEvent::CommandOutputCompleted {
            command, output, ..
        } => {
            format!("command_completed:{command}:{output}")
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
