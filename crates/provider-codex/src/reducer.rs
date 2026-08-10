//! Pure protocol-to-harness event reduction for pinned Codex v2 messages.

use std::collections::HashMap;

use serde_json::Value;
use yakshed_harness::{
    HarnessEvent, HarnessRunTerminal, NativePayload, ProviderRequestHandle, ProviderRunHandle,
    SanitizedDiagnostic,
};

#[derive(Default)]
pub struct Reducer {
    commands: HashMap<String, String>,
}

impl Reducer {
    pub fn reduce(
        &mut self,
        raw: String,
        message: &Value,
        run: Option<ProviderRunHandle>,
        request: Option<ProviderRequestHandle>,
    ) -> Option<HarnessEvent> {
        let method = message.get("method")?.as_str()?.to_owned();
        let params = message.get("params").unwrap_or(&Value::Null);
        let native = NativePayload::sanitized(raw);
        match method.as_str() {
            "item/started" => {
                let item = params.get("item")?;
                if item.get("type").and_then(Value::as_str) == Some("commandExecution")
                    && let (Some(id), Some(command)) = (
                        item.get("id").and_then(Value::as_str),
                        item.get("command").and_then(Value::as_str),
                    )
                {
                    self.commands.insert(id.to_owned(), command.to_owned());
                }
                None
            }
            "item/agentMessage/delta" => Some(HarnessEvent::MessageDelta {
                run: required_run(run, &native, &method)?,
                chunk: required(params, "delta", &native, &method)?,
                native,
            }),
            "item/commandExecution/outputDelta" => {
                let item_id = required(params, "itemId", &native, &method)?;
                Some(HarnessEvent::CommandOutput {
                    run: required_run(run, &native, &method)?,
                    command: self.commands.get(&item_id).cloned().unwrap_or_default(),
                    chunk: required(params, "delta", &native, &method)?,
                    native,
                })
            }
            "item/fileChange/outputDelta" => Some(HarnessEvent::FileMutation {
                run: required_run(run, &native, &method)?,
                path: required(params, "itemId", &native, &method)?,
                summary: required(params, "delta", &native, &method)?,
                native,
            }),
            "item/completed" => self.completed_item(params, run, native, method),
            "item/commandExecution/requestApproval" => Some(HarnessEvent::ApprovalRequested {
                request: required_request(request, &native, &method)?,
                summary: params
                    .get("reason")
                    .and_then(Value::as_str)
                    .or_else(|| params.get("command").and_then(Value::as_str))
                    .unwrap_or("Codex requests command approval")
                    .to_owned(),
                native,
            }),
            "item/fileChange/requestApproval" => Some(HarnessEvent::ApprovalRequested {
                request: required_request(request, &native, &method)?,
                summary: params
                    .get("reason")
                    .and_then(Value::as_str)
                    .unwrap_or("Codex requests file-change approval")
                    .to_owned(),
                native,
            }),
            "item/tool/requestUserInput" => {
                let prompt = params
                    .get("questions")
                    .and_then(Value::as_array)
                    .map(|questions| {
                        questions
                            .iter()
                            .filter_map(|question| question.get("question").and_then(Value::as_str))
                            .collect::<Vec<_>>()
                            .join("\n")
                    })
                    .filter(|prompt| !prompt.is_empty())
                    .unwrap_or_else(|| "Codex requests user input".to_owned());
                Some(HarnessEvent::UserInputRequested {
                    request: required_request(request, &native, &method)?,
                    prompt,
                    native,
                })
            }
            "turn/completed" => {
                let run = required_run(run, &native, &method)?;
                let status = params
                    .get("turn")
                    .and_then(|turn| turn.get("status"))
                    .and_then(Value::as_str);
                let state = match status {
                    Some("completed") => HarnessRunTerminal::Completed,
                    Some("interrupted") => HarnessRunTerminal::Interrupted,
                    Some("failed") => HarnessRunTerminal::Failed {
                        diagnostic: SanitizedDiagnostic::sanitized("Codex turn failed"),
                    },
                    _ => return Some(malformed(Some(run), method, native)),
                };
                Some(HarnessEvent::RunTerminal { run, state, native })
            }
            _ => Some(HarnessEvent::Unknown {
                run,
                item_type: method,
                native,
            }),
        }
    }

    fn completed_item(
        &mut self,
        params: &Value,
        run: Option<ProviderRunHandle>,
        native: NativePayload,
        method: String,
    ) -> Option<HarnessEvent> {
        let run = required_run(run, &native, &method)?;
        let item = params.get("item")?;
        match item.get("type").and_then(Value::as_str) {
            Some("agentMessage") => Some(HarnessEvent::MessageCompleted {
                run,
                text: required(item, "text", &native, &method)?,
                native,
            }),
            Some("fileChange") => {
                let changes = item.get("changes").and_then(Value::as_array)?;
                let first = changes.first()?;
                Some(HarnessEvent::FileMutation {
                    run,
                    path: required(first, "path", &native, &method)?,
                    summary: first
                        .get("diff")
                        .and_then(Value::as_str)
                        .unwrap_or("file changed")
                        .to_owned(),
                    native,
                })
            }
            Some("commandExecution") => Some(HarnessEvent::CommandOutput {
                run,
                command: item
                    .get("command")
                    .and_then(Value::as_str)
                    .unwrap_or_default()
                    .to_owned(),
                chunk: item
                    .get("aggregatedOutput")
                    .and_then(Value::as_str)
                    .unwrap_or_default()
                    .to_owned(),
                native,
            }),
            Some(item_type) => Some(HarnessEvent::Unknown {
                run: Some(run),
                item_type: item_type.to_owned(),
                native,
            }),
            None => Some(malformed(Some(run), method, native)),
        }
    }
}

fn required(value: &Value, field: &str, native: &NativePayload, method: &str) -> Option<String> {
    let _ = (native, method);
    value.get(field)?.as_str().map(str::to_owned)
}

fn required_run(
    run: Option<ProviderRunHandle>,
    _native: &NativePayload,
    _method: &str,
) -> Option<ProviderRunHandle> {
    run
}

fn required_request(
    request: Option<ProviderRequestHandle>,
    _native: &NativePayload,
    _method: &str,
) -> Option<ProviderRequestHandle> {
    request
}

fn malformed(
    run: Option<ProviderRunHandle>,
    item_type: String,
    native: NativePayload,
) -> HarnessEvent {
    HarnessEvent::MalformedNativePayload {
        run,
        item_type,
        native,
    }
}
