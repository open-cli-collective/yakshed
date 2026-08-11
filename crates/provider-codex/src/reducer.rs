//! Pure protocol-to-harness event reduction for pinned Codex v2 messages.

use std::collections::HashMap;

use serde_json::Value;
use yakshed_harness::{
    HarnessEvent, HarnessRunTerminal, NativePayload, ProviderRequestHandle, ProviderRunHandle,
    SanitizedDiagnostic,
};

#[derive(Default)]
pub struct Reducer {
    commands: HashMap<(ProviderRunHandle, String), String>,
}

impl Reducer {
    pub fn reduce(
        &mut self,
        raw: String,
        message: &Value,
        run: Option<ProviderRunHandle>,
        request: Option<ProviderRequestHandle>,
    ) -> Option<HarnessEvent> {
        let method = match message.get("method").and_then(Value::as_str) {
            Some(method) => method.to_owned(),
            None if message.get("id").is_some() => return None,
            None => "codex.missing-method".to_owned(),
        };
        let params = message.get("params").unwrap_or(&Value::Null);
        let native = NativePayload::sanitized(raw);
        macro_rules! require {
            ($value:expr) => {
                match $value {
                    Some(value) => value,
                    None => return Some(malformed(run, method, native)),
                }
            };
        }
        match method.as_str() {
            "item/started" => {
                let item = require!(params.get("item"));
                let item_type = require!(item.get("type").and_then(Value::as_str));
                let event_run = require!(run.clone());
                let id = require!(item.get("id").and_then(Value::as_str));
                if item_type == "commandExecution" {
                    let command = require!(item.get("command").and_then(Value::as_str));
                    self.commands
                        .insert((event_run, id.to_owned()), command.to_owned());
                    None
                } else if matches!(item_type, "agentMessage" | "fileChange") {
                    None
                } else {
                    Some(HarnessEvent::Unknown {
                        run: Some(event_run),
                        item_type: item_type.to_owned(),
                        native,
                    })
                }
            }
            "item/agentMessage/delta" => {
                let event_run = require!(run.clone());
                let chunk = require!(string(params, "delta"));
                Some(HarnessEvent::MessageDelta {
                    run: event_run,
                    chunk,
                    native,
                })
            }
            "item/commandExecution/outputDelta" => {
                let event_run = require!(run.clone());
                let item_id = require!(string(params, "itemId"));
                let chunk = require!(string(params, "delta"));
                Some(HarnessEvent::CommandOutput {
                    command: self
                        .commands
                        .get(&(event_run.clone(), item_id))
                        .cloned()
                        .unwrap_or_default(),
                    run: event_run,
                    chunk,
                    native,
                })
            }
            "item/fileChange/outputDelta" => {
                let event_run = require!(run.clone());
                require!(string(params, "itemId"));
                require!(string(params, "delta"));
                Some(HarnessEvent::Unknown {
                    run: Some(event_run),
                    item_type: method,
                    native,
                })
            }
            "item/completed" => self.completed_item(params, run, native, method),
            "item/commandExecution/requestApproval" => {
                let request = require!(request);
                Some(HarnessEvent::ApprovalRequested {
                    request,
                    summary: params
                        .get("reason")
                        .and_then(Value::as_str)
                        .or_else(|| params.get("command").and_then(Value::as_str))
                        .unwrap_or("Codex requests command approval")
                        .to_owned(),
                    native,
                })
            }
            "item/fileChange/requestApproval" => {
                let request = require!(request);
                Some(HarnessEvent::ApprovalRequested {
                    request,
                    summary: params
                        .get("reason")
                        .and_then(Value::as_str)
                        .unwrap_or("Codex requests file-change approval")
                        .to_owned(),
                    native,
                })
            }
            "item/tool/requestUserInput" => {
                let request = require!(request);
                let questions = require!(params.get("questions").and_then(Value::as_array));
                let prompts = questions
                    .iter()
                    .map(|question| question.get("question").and_then(Value::as_str))
                    .collect::<Option<Vec<_>>>();
                let prompt = require!(prompts).join("\n");
                if prompt.is_empty() {
                    return Some(malformed(run, method, native));
                }
                Some(HarnessEvent::UserInputRequested {
                    request,
                    prompt,
                    native,
                })
            }
            "turn/completed" => {
                let event_run = require!(run.clone());
                self.retire_run(&event_run);
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
                    _ => return Some(malformed(run, method, native)),
                };
                Some(HarnessEvent::RunTerminal {
                    run: event_run,
                    state,
                    native,
                })
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
        let Some(event_run) = run.clone() else {
            return Some(malformed(run, method, native));
        };
        let Some(item) = params.get("item") else {
            return Some(malformed(run, method, native));
        };
        let Some(item_id) = item.get("id").and_then(Value::as_str) else {
            return Some(malformed(run, method, native));
        };
        match item.get("type").and_then(Value::as_str) {
            Some("agentMessage") => match string(item, "text") {
                Some(text) => Some(HarnessEvent::MessageCompleted {
                    run: event_run,
                    text,
                    native,
                }),
                None => Some(malformed(run, method, native)),
            },
            Some("fileChange") => {
                let Some(first) = item
                    .get("changes")
                    .and_then(Value::as_array)
                    .and_then(|changes| changes.first())
                else {
                    return Some(malformed(run, method, native));
                };
                let Some(path) = string(first, "path") else {
                    return Some(malformed(run, method, native));
                };
                Some(HarnessEvent::FileMutation {
                    run: event_run,
                    path,
                    summary: first
                        .get("diff")
                        .and_then(Value::as_str)
                        .unwrap_or("file changed")
                        .to_owned(),
                    native,
                })
            }
            Some("commandExecution") => {
                self.commands
                    .remove(&(event_run.clone(), item_id.to_owned()));
                let Some(command) = string(item, "command") else {
                    return Some(malformed(run, method, native));
                };
                let Some(chunk) = string(item, "aggregatedOutput") else {
                    return Some(malformed(run, method, native));
                };
                Some(HarnessEvent::CommandOutput {
                    run: event_run,
                    command,
                    chunk,
                    native,
                })
            }
            Some(item_type) => Some(HarnessEvent::Unknown {
                run,
                item_type: item_type.to_owned(),
                native,
            }),
            None => Some(malformed(run, method, native)),
        }
    }

    pub(crate) fn retire_run(&mut self, run: &ProviderRunHandle) {
        self.commands
            .retain(|(command_run, _), _| command_run != run);
    }
}

fn string(value: &Value, field: &str) -> Option<String> {
    value.get(field)?.as_str().map(str::to_owned)
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
