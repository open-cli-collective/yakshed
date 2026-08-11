use std::{
    collections::{HashMap, HashSet, VecDeque},
    path::Path,
    process::Stdio,
    sync::{
        Arc,
        atomic::{AtomicU32, Ordering},
    },
};

use serde_json::{Value, json};
use tokio::{
    io::{AsyncRead, AsyncReadExt, AsyncWriteExt},
    process::{Child, ChildStderr, ChildStdin, ChildStdout, Command},
    sync::{mpsc, oneshot},
};
use yakshed_domain::ApprovalDecision;
use yakshed_harness::{
    HarnessError, HarnessEvent, HarnessEventSender, HarnessRunTerminal, NativePayload,
    ProviderRequestHandle, ProviderRequestId, ProviderResponse, ProviderRunHandle, ProviderRunId,
    ProviderSessionId, SanitizedDiagnostic,
};

use crate::{CodexRuntimeSpec, reducer::Reducer};

const ACTOR_CAPACITY: usize = 64;
const DIAGNOSTIC_CAPACITY: usize = 32;

pub enum RequestKind {
    EmptyObject,
    ThreadList,
    Session { expected_thread_id: Option<String> },
    StartRun { session_id: ProviderSessionId },
    TurnSteer { expected_turn_id: ProviderRunId },
}

enum CommandMessage {
    Request {
        method: &'static str,
        params: Value,
        mutation: bool,
        kind: RequestKind,
        reply: oneshot::Sender<Result<Value, HarnessError>>,
    },
    Respond {
        request: ProviderRequestHandle,
        response: ProviderResponse,
        reply: oneshot::Sender<Result<(), HarnessError>>,
    },
    Diagnostics(oneshot::Sender<Vec<String>>),
    RecordDiagnostic(String),
    Health(oneshot::Sender<()>),
    Shutdown(oneshot::Sender<()>),
}

pub struct RuntimeClient {
    commands: mpsc::Sender<CommandMessage>,
}

impl RuntimeClient {
    pub async fn request(
        &self,
        method: &'static str,
        params: Value,
        mutation: bool,
        kind: RequestKind,
    ) -> Result<Value, HarnessError> {
        let (reply, response) = oneshot::channel();
        self.commands
            .send(CommandMessage::Request {
                method,
                params,
                mutation,
                kind,
                reply,
            })
            .await
            .map_err(|_| HarnessError::Disconnected)?;
        response.await.map_err(|_| HarnessError::Disconnected)?
    }

    pub async fn respond(
        &self,
        request: ProviderRequestHandle,
        response: ProviderResponse,
    ) -> Result<(), HarnessError> {
        let (reply, result) = oneshot::channel();
        self.commands
            .send(CommandMessage::Respond {
                request: request.clone(),
                response,
                reply,
            })
            .await
            .map_err(|_| provider_request_not_found(&request))?;
        result
            .await
            .map_err(|_| provider_request_not_found(&request))?
    }

    pub async fn diagnostics(&self) -> Result<Vec<String>, HarnessError> {
        let (reply, result) = oneshot::channel();
        self.commands
            .send(CommandMessage::Diagnostics(reply))
            .await
            .map_err(|_| HarnessError::Disconnected)?;
        result.await.map_err(|_| HarnessError::Disconnected)
    }

    pub async fn record_diagnostic(&self, diagnostic: String) {
        let _ = self
            .commands
            .send(CommandMessage::RecordDiagnostic(diagnostic))
            .await;
    }

    pub async fn health(&self) -> Result<(), HarnessError> {
        let (reply, result) = oneshot::channel();
        self.commands
            .send(CommandMessage::Health(reply))
            .await
            .map_err(|_| HarnessError::Disconnected)?;
        result.await.map_err(|_| HarnessError::Disconnected)
    }

    pub async fn shutdown(&self) -> Result<(), HarnessError> {
        let (reply, result) = oneshot::channel();
        self.commands
            .send(CommandMessage::Shutdown(reply))
            .await
            .map_err(|_| HarnessError::Disconnected)?;
        result.await.map_err(|_| HarnessError::Disconnected)
    }
}

pub async fn start_runtime(
    spec: CodexRuntimeSpec,
    events: HarnessEventSender,
    process_group: Arc<AtomicU32>,
) -> Result<RuntimeClient, HarnessError> {
    let (commands, command_rx) = mpsc::channel(ACTOR_CAPACITY);
    let (ready_tx, ready_rx) = oneshot::channel();
    tokio::spawn(run_actor(spec, events, command_rx, ready_tx, process_group));
    ready_rx.await.map_err(|_| HarnessError::Disconnected)??;
    Ok(RuntimeClient { commands })
}

struct PendingClient {
    method: &'static str,
    mutation: bool,
    kind: RequestKind,
    reply: oneshot::Sender<Result<Value, HarnessError>>,
}

enum ServerRequestKind {
    Approval,
    UserInput { question_ids: Vec<String> },
}

struct PendingServer {
    rpc_id: Value,
    kind: ServerRequestKind,
    uncertain: bool,
}

enum Inbound {
    Frame { raw: String, value: Value },
    Malformed(String),
    Oversized(String),
    Stderr(String),
    Eof,
}

struct Sanitizer(Vec<String>);

impl Sanitizer {
    fn sanitize(&self, mut value: String) -> String {
        for secret in &self.0 {
            value = value.replace(secret, "[REDACTED]");
        }
        value
    }

    fn sanitize_value(&self, value: &mut Value) {
        match value {
            Value::String(text) => *text = self.sanitize(std::mem::take(text)),
            Value::Array(values) => {
                for value in values {
                    self.sanitize_value(value);
                }
            }
            Value::Object(values) => {
                for value in values.values_mut() {
                    self.sanitize_value(value);
                }
            }
            Value::Null | Value::Bool(_) | Value::Number(_) => {}
        }
    }
}

#[allow(clippy::too_many_arguments)]
async fn run_actor(
    spec: CodexRuntimeSpec,
    events: HarnessEventSender,
    mut commands: mpsc::Receiver<CommandMessage>,
    ready: oneshot::Sender<Result<(), HarnessError>>,
    process_group: Arc<AtomicU32>,
) {
    let sanitizer = Sanitizer(spec.redactions.clone());
    let spawned = spawn_child(&spec, Arc::clone(&process_group)).await;
    let (mut child, mut stdin, inbound) = match spawned {
        Ok(spawned) => spawned,
        Err(error) => {
            let _ = ready.send(Err(error));
            return;
        }
    };
    let (inbound_tx, mut inbound_rx) = mpsc::channel(ACTOR_CAPACITY);
    let stdout_task = tokio::spawn(read_stdout(
        inbound,
        inbound_tx.clone(),
        spec.max_frame_size,
    ));
    let stderr = child.take_stderr().expect("spawned child has stderr");
    let stderr_task = tokio::spawn(read_stderr(stderr, inbound_tx));
    let expected_home = normalize_codex_home(&spec.key.codex_home.to_string_lossy());

    let initialize = json!({
        "id": 1,
        "method": "initialize",
        "params": {
            "clientInfo": {"name": "yakshed", "version": env!("CARGO_PKG_VERSION")},
            "capabilities": {"experimentalApi": true}
        }
    });
    let initialized = async {
        write_frame(&mut stdin, &initialize).await?;
        loop {
            match inbound_rx.recv().await {
                Some(Inbound::Frame { value, .. }) if value.get("id") == Some(&json!(1)) => {
                    if let Some(error) = value.get("error") {
                        return Err(classify_protocol_error(error, &sanitizer));
                    }
                    if !value
                        .get("result")
                        .is_some_and(|result| valid_initialize_response(result, &expected_home))
                    {
                        return Err(HarnessError::Protocol {
                            diagnostic: SanitizedDiagnostic::sanitized(
                                "Codex initialize response did not match the pinned schema",
                            ),
                        });
                    }
                    write_frame(&mut stdin, &json!({"method": "initialized"})).await?;
                    return Ok(());
                }
                Some(Inbound::Stderr(_)) => {}
                Some(Inbound::Eof) | None => return Err(HarnessError::Disconnected),
                Some(_) => {}
            }
        }
    };
    match tokio::time::timeout(spec.startup_timeout, initialized).await {
        Ok(Ok(())) => {
            let _ = ready.send(Ok(()));
        }
        Ok(Err(error)) => {
            let _ = ready.send(Err(error));
            child.kill_and_reap().await;
            stdout_task.abort();
            stderr_task.abort();
            return;
        }
        Err(_) => {
            let _ = ready.send(Err(HarnessError::Transport {
                diagnostic: SanitizedDiagnostic::sanitized("Codex initialization timed out"),
            }));
            child.kill_and_reap().await;
            stdout_task.abort();
            stderr_task.abort();
            return;
        }
    }

    let mut next_id = 2_u64;
    let mut pending = HashMap::<u64, PendingClient>::new();
    let mut server_requests = HashMap::<ProviderRequestHandle, PendingServer>::new();
    let mut loaded_sessions = HashSet::<String>::new();
    let mut runs = HashMap::<(String, String), ProviderRunHandle>::new();
    let mut diagnostics = VecDeque::<String>::new();
    let mut reducer = Reducer::default();

    'actor: loop {
        tokio::select! {
            command = commands.recv() => {
                match command {
                    Some(CommandMessage::Request { method, params, mutation, kind, reply }) => {
                        if let Some((thread, turn)) = operation_run_identity(method, &params)
                            && !runs.contains_key(&(thread.to_owned(), turn.to_owned()))
                        {
                            let _ = reply.send(Err(HarnessError::NotFound {
                                entity: "run",
                                id: format!("{thread}/{turn}"),
                            }));
                            continue 'actor;
                        }
                        let id = next_id;
                        next_id += 1;
                        let frame = json!({"id": id, "method": method, "params": params});
                        if write_frame(&mut stdin, &frame).await.is_err() {
                            let error = if mutation {
                                HarnessError::OutcomeUnknown { operation: method }
                            } else {
                                HarnessError::Disconnected
                            };
                            let _ = reply.send(Err(error));
                            child.kill_and_reap().await;
                            settle_runtime(
                                &mut pending,
                                &mut server_requests,
                                &mut runs,
                                &mut reducer,
                                &events,
                                &sanitizer,
                                Settlement::Crashed,
                            ).await;
                            break 'actor;
                        } else {
                            pending.insert(id, PendingClient { method, mutation, kind, reply });
                        }
                    }
                    Some(CommandMessage::Respond { request, response, reply }) => {
                        let outcome = respond_to_server_request(
                            &mut stdin,
                            &mut server_requests,
                            &request,
                            response,
                        ).await;
                        match outcome {
                            ServerResponseOutcome::Complete(result) => {
                                let _ = reply.send(result);
                            }
                            ServerResponseOutcome::TransportFailed(error) => {
                                let _ = reply.send(Err(error));
                                child.kill_and_reap().await;
                                settle_runtime(
                                    &mut pending,
                                    &mut server_requests,
                                    &mut runs,
                                    &mut reducer,
                                    &events,
                                    &sanitizer,
                                    Settlement::Crashed,
                                ).await;
                                break 'actor;
                            }
                        }
                    }
                    Some(CommandMessage::Diagnostics(reply)) => {
                        let _ = reply.send(diagnostics.iter().cloned().collect());
                    }
                    Some(CommandMessage::RecordDiagnostic(diagnostic)) => {
                        push_diagnostic(&mut diagnostics, diagnostic);
                    }
                    Some(CommandMessage::Health(reply)) => {
                        let _ = reply.send(());
                    }
                    Some(CommandMessage::Shutdown(reply)) => {
                        settle_runtime(
                            &mut pending,
                            &mut server_requests,
                            &mut runs,
                            &mut reducer,
                            &events,
                            &sanitizer,
                            Settlement::Shutdown,
                        ).await;
                        child.kill_and_reap().await;
                        let _ = reply.send(());
                        break;
                    }
                    None => {
                        settle_runtime(
                            &mut pending,
                            &mut server_requests,
                            &mut runs,
                            &mut reducer,
                            &events,
                            &sanitizer,
                            Settlement::Shutdown,
                        ).await;
                        child.kill_and_reap().await;
                        break;
                    }
                }
            }
            inbound = inbound_rx.recv() => {
                match inbound {
                    Some(Inbound::Frame { raw, value }) => {
                        if handle_frame(
                            raw,
                            value,
                            &spec,
                            &sanitizer,
                            &mut stdin,
                            &events,
                            &mut diagnostics,
                            &mut pending,
                            &mut server_requests,
                            &mut loaded_sessions,
                            &mut runs,
                            &mut reducer,
                        ).await.is_err() {
                            child.kill_and_reap().await;
                            settle_runtime(
                                &mut pending,
                                &mut server_requests,
                                &mut runs,
                                &mut reducer,
                                &events,
                                &sanitizer,
                                Settlement::Crashed,
                            ).await;
                            break;
                        }
                    }
                    Some(Inbound::Malformed(raw)) => {
                        let raw = sanitizer.sanitize(raw);
                        push_diagnostic(&mut diagnostics, raw.clone());
                        let _ = events.send(HarnessEvent::MalformedNativePayload {
                            run: None,
                            item_type: "codex.malformed-frame".to_owned(),
                            native: NativePayload::sanitized(raw),
                        }).await;
                    }
                    Some(Inbound::Oversized(prefix)) => {
                        let prefix = sanitizer.sanitize(prefix);
                        push_diagnostic(&mut diagnostics, prefix.clone());
                        let _ = events.send(HarnessEvent::MalformedNativePayload {
                            run: None,
                            item_type: "codex.oversized-frame".to_owned(),
                            native: NativePayload::sanitized(prefix),
                        }).await;
                    }
                    Some(Inbound::Stderr(chunk)) => {
                        push_diagnostic(&mut diagnostics, sanitizer.sanitize(chunk));
                    }
                    Some(Inbound::Eof) | None => {
                        child.kill_and_reap().await;
                        settle_runtime(
                            &mut pending,
                            &mut server_requests,
                            &mut runs,
                            &mut reducer,
                            &events,
                            &sanitizer,
                            Settlement::Crashed,
                        ).await;
                        break;
                    }
                }
            }
        }
    }
    process_group.store(0, Ordering::Release);
    stdout_task.abort();
    stderr_task.abort();
}

fn valid_initialize_response(value: &Value, expected_home: &str) -> bool {
    let Some(result) = value.as_object() else {
        return false;
    };
    result
        .get("codexHome")
        .and_then(Value::as_str)
        .is_some_and(|path| {
            let normalized = normalize_codex_home(path);
            !normalized.is_empty() && Path::new(path).is_absolute() && normalized == expected_home
        })
        && result
            .get("platformFamily")
            .and_then(Value::as_str)
            .is_some()
        && result.get("platformOs").and_then(Value::as_str).is_some()
        && result.get("userAgent").and_then(Value::as_str).is_some()
}

fn is_root(path: &str) -> bool {
    if cfg!(windows) {
        path.len() == 3 && path.as_bytes()[1] == b':' && matches!(path.as_bytes()[2], b'/' | b'\\')
    } else {
        path == "/"
    }
}

fn normalize_codex_home(raw: &str) -> String {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return String::new();
    }
    let normalized = if is_root(trimmed) {
        trimmed
    } else {
        trimmed.trim_end_matches(['/', '\\'].as_ref())
    };
    if is_root(trimmed) {
        if cfg!(windows) {
            return normalized.to_ascii_lowercase();
        }
        return normalized.to_owned();
    }
    if cfg!(windows) {
        normalized.to_ascii_lowercase()
    } else {
        normalized.to_owned()
    }
}

#[allow(clippy::too_many_arguments)]
async fn handle_frame(
    raw: String,
    value: Value,
    spec: &CodexRuntimeSpec,
    sanitizer: &Sanitizer,
    stdin: &mut ChildStdin,
    events: &HarnessEventSender,
    diagnostics: &mut VecDeque<String>,
    pending: &mut HashMap<u64, PendingClient>,
    server_requests: &mut HashMap<ProviderRequestHandle, PendingServer>,
    loaded_sessions: &mut HashSet<String>,
    runs: &mut HashMap<(String, String), ProviderRunHandle>,
    reducer: &mut Reducer,
) -> Result<(), HarnessError> {
    if value.get("method").is_none() {
        let Some(id) = value.get("id").and_then(Value::as_u64) else {
            return Ok(());
        };
        let Some(pending_request) = pending.remove(&id) else {
            return Ok(());
        };
        if let Some(error) = value.get("error")
            && value.get("result").is_none()
        {
            let error = classify_protocol_error(error, sanitizer);
            push_diagnostic(diagnostics, error.to_string());
            let _ = pending_request.reply.send(Err(error));
            return Ok(());
        }
        let Some(mut result) = value.get("result").cloned() else {
            push_diagnostic(diagnostics, sanitizer.sanitize(raw));
            let error = malformed_ack_error(&pending_request);
            let _ = pending_request.reply.send(Err(error));
            return Ok(());
        };
        sanitizer.sanitize_value(&mut result);
        if value.get("error").is_some() || !valid_response_result(&pending_request.kind, &result) {
            push_diagnostic(diagnostics, sanitizer.sanitize(raw));
            let error = malformed_ack_error(&pending_request);
            let _ = pending_request.reply.send(Err(error));
            return Ok(());
        }
        match &pending_request.kind {
            RequestKind::Session { expected_thread_id } => {
                if let Some(thread_id) = result
                    .get("thread")
                    .and_then(|thread| thread.get("id"))
                    .and_then(Value::as_str)
                {
                    if let Some(expected) = expected_thread_id
                        && expected != thread_id
                    {
                        push_diagnostic(
                            diagnostics,
                            format!(
                                "session resume returned mismatched thread id: expected {expected}, got {thread_id}"
                            ),
                        );
                        let _ = pending_request.reply.send(Err(HarnessError::Protocol {
                            diagnostic: SanitizedDiagnostic::sanitized(
                                "session resume returned mismatched thread id",
                            ),
                        }));
                        return Ok(());
                    }
                    loaded_sessions.insert(thread_id.to_owned());
                }
            }
            RequestKind::StartRun { session_id } => {
                if let Some(turn_id) = result
                    .get("turn")
                    .and_then(|turn| turn.get("id"))
                    .and_then(Value::as_str)
                    && let Ok(native_id) = ProviderRunId::new(turn_id)
                {
                    let run =
                        ProviderRunHandle::new(spec.handle.clone(), session_id.clone(), native_id);
                    runs.insert(
                        (session_id.as_str().to_owned(), turn_id.to_owned()),
                        run.clone(),
                    );
                    let _ = events
                        .send(HarnessEvent::RunAccepted {
                            run,
                            native: NativePayload::sanitized(sanitizer.sanitize(raw)),
                        })
                        .await;
                }
            }
            RequestKind::EmptyObject | RequestKind::ThreadList | RequestKind::TurnSteer { .. } => {}
        }
        let _ = pending_request.reply.send(Ok(result));
        return Ok(());
    }

    let method = value
        .get("method")
        .and_then(Value::as_str)
        .unwrap_or("codex.unknown")
        .to_owned();
    let mut safe_value = value;
    sanitizer.sanitize_value(&mut safe_value);
    let run = run_for_message(&safe_value, runs);
    let sanitized = sanitizer.sanitize(raw);
    let native = NativePayload::sanitized(sanitized.clone());

    let request = if safe_value.get("id").is_some() {
        match validate_server_request(&safe_value, run.clone(), server_requests) {
            Ok((handle, pending_server)) => {
                server_requests.insert(handle.clone(), pending_server);
                Some(handle)
            }
            Err(rejection) => {
                write_protocol_error(
                    stdin,
                    safe_value.get("id").cloned().unwrap_or(Value::Null),
                    rejection.code,
                    rejection.message,
                )
                .await?;
                push_diagnostic(diagnostics, sanitized);
                let event = if rejection.code == -32601 {
                    HarnessEvent::Unknown {
                        run,
                        item_type: method,
                        native,
                    }
                } else {
                    HarnessEvent::MalformedNativePayload {
                        run,
                        item_type: method,
                        native,
                    }
                };
                let _ = events.send(event).await;
                return Ok(());
            }
        }
    } else {
        if message_has_run_identity(&safe_value) && run.is_none() {
            let _ = events
                .send(HarnessEvent::Unknown {
                    run: None,
                    item_type: method,
                    native,
                })
                .await;
            return Ok(());
        }
        None
    };
    let completes_run = method == "turn/completed";
    for event in reducer.reduce(sanitized, &safe_value, run.clone(), request) {
        match event {
            HarnessEvent::RunTerminal { run, state, native } => {
                emit_terminal_and_retire(
                    run,
                    state,
                    native,
                    events,
                    server_requests,
                    runs,
                    reducer,
                )
                .await;
            }
            malformed @ HarnessEvent::MalformedNativePayload { .. } if completes_run => {
                let _ = events.send(malformed).await;
                if let Some(run) = run.clone() {
                    emit_terminal_and_retire(
                        run,
                        HarnessRunTerminal::Crashed {
                            diagnostic: SanitizedDiagnostic::sanitized(
                                "Codex emitted a malformed terminal frame",
                            ),
                        },
                        NativePayload::sanitized("{\"event\":\"malformed-terminal-frame\"}"),
                        events,
                        server_requests,
                        runs,
                        reducer,
                    )
                    .await;
                }
            }
            event => {
                let _ = events.send(event).await;
            }
        }
    }
    Ok(())
}

fn malformed_ack_error(pending: &PendingClient) -> HarnessError {
    if pending.mutation {
        HarnessError::OutcomeUnknown {
            operation: pending.method,
        }
    } else {
        HarnessError::Protocol {
            diagnostic: SanitizedDiagnostic::sanitized(
                "Codex response did not match the pinned schema",
            ),
        }
    }
}

fn valid_response_result(kind: &RequestKind, result: &Value) -> bool {
    let Some(object) = result.as_object() else {
        return false;
    };
    match kind {
        RequestKind::EmptyObject => object.is_empty(),
        RequestKind::ThreadList => {
            object
                .get("data")
                .and_then(Value::as_array)
                .is_some_and(|threads| threads.iter().all(valid_thread))
                && object
                    .get("nextCursor")
                    .is_none_or(|cursor| cursor.is_null() || cursor.is_string())
        }
        RequestKind::Session { expected_thread_id } => {
            has_fields(
                object,
                &[
                    "approvalPolicy",
                    "approvalsReviewer",
                    "cwd",
                    "model",
                    "modelProvider",
                    "sandbox",
                    "thread",
                ],
            ) && object.get("cwd").and_then(Value::as_str).is_some()
                && object.get("model").and_then(Value::as_str).is_some()
                && object
                    .get("modelProvider")
                    .and_then(Value::as_str)
                    .is_some()
                && object.get("sandbox").and_then(Value::as_object).is_some()
                && object.get("thread").is_some_and(valid_thread)
                && expected_thread_id.as_ref().is_none_or(|expected| {
                    object
                        .get("thread")
                        .and_then(|thread| thread.get("id"))
                        .and_then(Value::as_str)
                        .is_some_and(|thread_id| thread_id == expected.as_str())
                })
        }
        RequestKind::StartRun { .. } => object.get("turn").is_some_and(valid_turn),
        RequestKind::TurnSteer { expected_turn_id } => object
            .get("turnId")
            .and_then(Value::as_str)
            .is_some_and(|turn_id| turn_id == expected_turn_id.as_str()),
    }
}

fn valid_thread(value: &Value) -> bool {
    let Some(thread) = value.as_object() else {
        return false;
    };
    has_fields(
        thread,
        &[
            "cliVersion",
            "createdAt",
            "cwd",
            "ephemeral",
            "id",
            "modelProvider",
            "preview",
            "sessionId",
            "source",
            "status",
            "turns",
            "updatedAt",
        ],
    ) && thread.get("cliVersion").and_then(Value::as_str).is_some()
        && thread.get("createdAt").and_then(Value::as_i64).is_some()
        && thread.get("cwd").and_then(Value::as_str).is_some()
        && thread.get("ephemeral").and_then(Value::as_bool).is_some()
        && thread
            .get("id")
            .and_then(Value::as_str)
            .is_some_and(|id| ProviderSessionId::new(id).is_ok())
        && thread
            .get("modelProvider")
            .and_then(Value::as_str)
            .is_some()
        && thread.get("preview").and_then(Value::as_str).is_some()
        && thread.get("sessionId").and_then(Value::as_str).is_some()
        && thread.get("source").is_some_and(|source| {
            source.is_string() || source.as_object().is_some_and(|source| !source.is_empty())
        })
        && thread
            .get("status")
            .and_then(Value::as_object)
            .and_then(|status| status.get("type"))
            .and_then(Value::as_str)
            .is_some_and(|status| matches!(status, "notLoaded" | "idle" | "systemError" | "active"))
        && thread
            .get("turns")
            .and_then(Value::as_array)
            .is_some_and(|turns| turns.iter().all(valid_turn))
        && thread.get("updatedAt").and_then(Value::as_i64).is_some()
}

fn valid_turn(value: &Value) -> bool {
    let Some(turn) = value.as_object() else {
        return false;
    };
    has_fields(turn, &["id", "items", "status"])
        && turn
            .get("id")
            .and_then(Value::as_str)
            .is_some_and(|id| ProviderRunId::new(id).is_ok())
        && turn.get("items").and_then(Value::as_array).is_some()
        && turn
            .get("status")
            .and_then(Value::as_str)
            .is_some_and(|status| {
                matches!(
                    status,
                    "completed" | "interrupted" | "failed" | "inProgress"
                )
            })
}

fn has_fields(object: &serde_json::Map<String, Value>, fields: &[&str]) -> bool {
    fields.iter().all(|field| object.contains_key(*field))
}

struct ServerRequestRejection {
    code: i64,
    message: &'static str,
}

fn validate_server_request(
    value: &Value,
    run: Option<ProviderRunHandle>,
    requests: &mut HashMap<ProviderRequestHandle, PendingServer>,
) -> Result<(ProviderRequestHandle, PendingServer), ServerRequestRejection> {
    let method = value
        .get("method")
        .and_then(Value::as_str)
        .ok_or(ServerRequestRejection {
            code: -32600,
            message: "invalid request",
        })?;
    let kind = match method {
        "item/commandExecution/requestApproval" => {
            validate_command_approval_params(value)?;
            ServerRequestKind::Approval
        }
        "item/fileChange/requestApproval" => {
            validate_file_approval_params(value)?;
            ServerRequestKind::Approval
        }
        "item/tool/requestUserInput" => ServerRequestKind::UserInput {
            question_ids: validate_user_input_params(value)?,
        },
        _ => {
            return Err(ServerRequestRejection {
                code: -32601,
                message: "method not supported",
            });
        }
    };
    let run = run.ok_or(ServerRequestRejection {
        code: -32602,
        message: "request does not identify an active run",
    })?;
    let rpc_id = value
        .get("id")
        .filter(|id| id.is_string() || id.is_i64() || id.is_u64())
        .cloned()
        .ok_or(ServerRequestRejection {
            code: -32600,
            message: "invalid request id",
        })?;
    let native_id = match &rpc_id {
        Value::String(value) => value.clone(),
        value => value.to_string(),
    };
    let request_id = ProviderRequestId::new(native_id).map_err(|_| ServerRequestRejection {
        code: -32600,
        message: "invalid request id",
    })?;
    let handle = ProviderRequestHandle::new(run, request_id);
    if requests.contains_key(&handle) {
        return Err(ServerRequestRejection {
            code: -32600,
            message: "duplicate request id",
        });
    }
    Ok((
        handle,
        PendingServer {
            rpc_id,
            kind,
            uncertain: false,
        },
    ))
}

fn validate_command_approval_params(value: &Value) -> Result<(), ServerRequestRejection> {
    let params = request_params(value)?;
    let valid = params.get("itemId").and_then(Value::as_str).is_some()
        && params.get("startedAtMs").and_then(Value::as_i64).is_some();
    if valid { Ok(()) } else { Err(invalid_params()) }
}

fn validate_file_approval_params(value: &Value) -> Result<(), ServerRequestRejection> {
    let params = request_params(value)?;
    params
        .get("itemId")
        .and_then(Value::as_str)
        .map(|_| ())
        .ok_or_else(invalid_params)
}

fn validate_user_input_params(value: &Value) -> Result<Vec<String>, ServerRequestRejection> {
    let params = request_params(value)?;
    if params.get("itemId").and_then(Value::as_str).is_none()
        || params.get("isBlocking").and_then(Value::as_bool).is_none()
    {
        return Err(invalid_params());
    }
    let questions = params
        .get("questions")
        .and_then(Value::as_array)
        .filter(|questions| !questions.is_empty())
        .ok_or_else(invalid_params)?;
    questions
        .iter()
        .map(|question| {
            if question.get("header").and_then(Value::as_str).is_none()
                || question.get("question").and_then(Value::as_str).is_none()
            {
                return Err(invalid_params());
            }
            question
                .get("id")
                .and_then(Value::as_str)
                .map(str::to_owned)
                .ok_or_else(invalid_params)
        })
        .collect()
}

fn request_params(
    value: &Value,
) -> Result<&serde_json::Map<String, Value>, ServerRequestRejection> {
    let params = value
        .get("params")
        .and_then(Value::as_object)
        .ok_or_else(invalid_params)?;
    if params.get("threadId").and_then(Value::as_str).is_none()
        || params.get("turnId").and_then(Value::as_str).is_none()
    {
        Err(invalid_params())
    } else {
        Ok(params)
    }
}

fn invalid_params() -> ServerRequestRejection {
    ServerRequestRejection {
        code: -32602,
        message: "invalid request parameters",
    }
}

async fn write_protocol_error(
    stdin: &mut ChildStdin,
    id: Value,
    code: i64,
    message: &'static str,
) -> Result<(), HarnessError> {
    write_frame(
        stdin,
        &json!({"id": id, "error": {"code": code, "message": message}}),
    )
    .await
}

enum ServerResponseOutcome {
    Complete(Result<(), HarnessError>),
    TransportFailed(HarnessError),
}

async fn respond_to_server_request(
    stdin: &mut ChildStdin,
    requests: &mut HashMap<ProviderRequestHandle, PendingServer>,
    request: &ProviderRequestHandle,
    response: ProviderResponse,
) -> ServerResponseOutcome {
    let Some(pending) = requests.get_mut(request) else {
        return ServerResponseOutcome::Complete(Err(HarnessError::NotFound {
            entity: "provider request",
            id: request.to_string(),
        }));
    };
    if pending.uncertain {
        return ServerResponseOutcome::Complete(Err(HarnessError::OutcomeUnknown {
            operation: "provider/request/respond",
        }));
    }
    let result = match (&pending.kind, response) {
        (ServerRequestKind::Approval, ProviderResponse::Approval(decision)) => json!({
            "decision": match decision {
                ApprovalDecision::Approved => "accept",
                ApprovalDecision::Denied => "decline",
            }
        }),
        (ServerRequestKind::UserInput { question_ids }, ProviderResponse::UserInput(answer)) => {
            let answers = question_ids
                .iter()
                .map(|id| (id.clone(), json!({"answers": [answer]})))
                .collect::<serde_json::Map<_, _>>();
            json!({"answers": answers})
        }
        _ => {
            return ServerResponseOutcome::Complete(Err(HarnessError::InvalidInput(
                "provider response does not match request kind".to_owned(),
            )));
        }
    };
    if write_frame(stdin, &json!({"id": pending.rpc_id, "result": result}))
        .await
        .is_err()
    {
        pending.uncertain = true;
        return ServerResponseOutcome::TransportFailed(HarnessError::OutcomeUnknown {
            operation: "provider/request/respond",
        });
    }
    requests.remove(request);
    ServerResponseOutcome::Complete(Ok(()))
}

fn operation_run_identity<'a>(method: &str, params: &'a Value) -> Option<(&'a str, &'a str)> {
    let thread = params.get("threadId")?.as_str()?;
    let turn = match method {
        "turn/steer" => params.get("expectedTurnId")?.as_str()?,
        "turn/interrupt" => params.get("turnId")?.as_str()?,
        _ => return None,
    };
    Some((thread, turn))
}

fn run_for_message(
    value: &Value,
    runs: &HashMap<(String, String), ProviderRunHandle>,
) -> Option<ProviderRunHandle> {
    let params = value.get("params")?;
    let thread = params.get("threadId")?.as_str()?;
    let turn = params
        .get("turnId")
        .and_then(Value::as_str)
        .or_else(|| params.get("turn")?.get("id")?.as_str())?;
    runs.get(&(thread.to_owned(), turn.to_owned())).cloned()
}

fn message_has_run_identity(value: &Value) -> bool {
    value
        .get("params")
        .and_then(Value::as_object)
        .is_some_and(|params| {
            params.contains_key("threadId")
                && (params.contains_key("turnId") || params.contains_key("turn"))
        })
}

async fn emit_terminal_and_retire(
    run: ProviderRunHandle,
    state: HarnessRunTerminal,
    native: NativePayload,
    events: &HarnessEventSender,
    server_requests: &mut HashMap<ProviderRequestHandle, PendingServer>,
    runs: &mut HashMap<(String, String), ProviderRunHandle>,
    reducer: &mut Reducer,
) {
    let _ = events
        .send(HarnessEvent::RunTerminal {
            run: run.clone(),
            state,
            native,
        })
        .await;
    runs.remove(&(
        run.session_id().as_str().to_owned(),
        run.native_id().as_str().to_owned(),
    ));
    server_requests.retain(|request, _| request.run() != &run);
    reducer.retire_run(&run);
}

#[derive(Clone, Copy)]
enum Settlement {
    Shutdown,
    Crashed,
}

async fn settle_runtime(
    pending: &mut HashMap<u64, PendingClient>,
    server_requests: &mut HashMap<ProviderRequestHandle, PendingServer>,
    runs: &mut HashMap<(String, String), ProviderRunHandle>,
    reducer: &mut Reducer,
    events: &HarnessEventSender,
    sanitizer: &Sanitizer,
    settlement: Settlement,
) {
    for (_, pending) in pending.drain() {
        let error = if pending.mutation {
            HarnessError::OutcomeUnknown {
                operation: pending.method,
            }
        } else {
            HarnessError::Disconnected
        };
        let _ = pending.reply.send(Err(error));
    }
    server_requests.clear();
    let active_runs = runs.values().cloned().collect::<Vec<_>>();
    for run in active_runs {
        let (state, native) = match settlement {
            Settlement::Shutdown => (
                HarnessRunTerminal::Interrupted,
                NativePayload::sanitized("{\"event\":\"runtime-shutdown\"}"),
            ),
            Settlement::Crashed => (
                HarnessRunTerminal::Crashed {
                    diagnostic: SanitizedDiagnostic::sanitized(
                        sanitizer.sanitize("Codex App Server disconnected".to_owned()),
                    ),
                },
                NativePayload::sanitized("{\"event\":\"process-exited\"}"),
            ),
        };
        emit_terminal_and_retire(run, state, native, events, server_requests, runs, reducer).await;
    }
}

fn provider_request_not_found(request: &ProviderRequestHandle) -> HarnessError {
    HarnessError::NotFound {
        entity: "provider request",
        id: request.to_string(),
    }
}

fn classify_protocol_error(error: &Value, sanitizer: &Sanitizer) -> HarnessError {
    let raw = error.to_string();
    if raw.contains("serverOverloaded") || raw.to_ascii_lowercase().contains("overload") {
        HarnessError::Overloaded
    } else {
        HarnessError::Protocol {
            diagnostic: SanitizedDiagnostic::sanitized(sanitizer.sanitize(raw)),
        }
    }
}

fn push_diagnostic(diagnostics: &mut VecDeque<String>, value: String) {
    if diagnostics.len() == DIAGNOSTIC_CAPACITY {
        diagnostics.pop_front();
    }
    diagnostics.push_back(value);
}

async fn write_frame(stdin: &mut ChildStdin, value: &Value) -> Result<(), HarnessError> {
    let mut bytes = serde_json::to_vec(value).map_err(|_| HarnessError::Protocol {
        diagnostic: SanitizedDiagnostic::sanitized("Codex request serialization failed"),
    })?;
    bytes.push(b'\n');
    stdin
        .write_all(&bytes)
        .await
        .map_err(|_| HarnessError::Transport {
            diagnostic: SanitizedDiagnostic::sanitized("Codex stdin write failed"),
        })?;
    stdin.flush().await.map_err(|_| HarnessError::Transport {
        diagnostic: SanitizedDiagnostic::sanitized("Codex stdin flush failed"),
    })
}

async fn read_stdout(
    stdout: impl AsyncRead + Unpin,
    sender: mpsc::Sender<Inbound>,
    max_frame_size: usize,
) {
    frame_stream(stdout, sender, max_frame_size).await;
}

async fn frame_stream(
    mut reader: impl AsyncRead + Unpin,
    sender: mpsc::Sender<Inbound>,
    max_frame_size: usize,
) {
    let mut read = [0_u8; 4096];
    let mut frame = Vec::new();
    let mut oversized = false;
    loop {
        let count = match reader.read(&mut read).await {
            Ok(0) => break,
            Ok(count) => count,
            Err(_) => break,
        };
        for byte in &read[..count] {
            if *byte == b'\n' {
                if !frame.is_empty() || oversized {
                    let raw = String::from_utf8_lossy(&frame).into_owned();
                    let message = if oversized {
                        Inbound::Oversized(raw)
                    } else {
                        match serde_json::from_slice(&frame) {
                            Ok(value) => Inbound::Frame { raw, value },
                            Err(_) => Inbound::Malformed(raw),
                        }
                    };
                    if sender.send(message).await.is_err() {
                        return;
                    }
                }
                frame.clear();
                oversized = false;
            } else if frame.len() < max_frame_size {
                frame.push(*byte);
            } else {
                oversized = true;
            }
        }
    }
    if !frame.is_empty() {
        let raw = String::from_utf8_lossy(&frame).into_owned();
        let message = if oversized {
            Inbound::Oversized(raw)
        } else {
            Inbound::Malformed(raw)
        };
        let _ = sender.send(message).await;
    }
    let _ = sender.send(Inbound::Eof).await;
}

async fn read_stderr(mut stderr: impl AsyncRead + Unpin, sender: mpsc::Sender<Inbound>) {
    let mut read = [0_u8; 4096];
    loop {
        match stderr.read(&mut read).await {
            Ok(0) | Err(_) => return,
            Ok(count) => {
                if sender
                    .send(Inbound::Stderr(
                        String::from_utf8_lossy(&read[..count]).into_owned(),
                    ))
                    .await
                    .is_err()
                {
                    return;
                }
            }
        }
    }
}

async fn spawn_child(
    spec: &CodexRuntimeSpec,
    process_group: Arc<AtomicU32>,
) -> Result<(ChildGuard, ChildStdin, ChildStdout), HarnessError> {
    let mut command = Command::new(&spec.program);
    command
        .args(&spec.args)
        .arg("app-server")
        .env("CODEX_HOME", &spec.key.codex_home)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true);
    #[cfg(unix)]
    command.process_group(0);
    let mut child = command.spawn().map_err(|_| HarnessError::Runtime {
        diagnostic: SanitizedDiagnostic::sanitized("Codex App Server could not start"),
    })?;
    let id = child.id().ok_or_else(|| HarnessError::Runtime {
        diagnostic: SanitizedDiagnostic::sanitized("Codex process ID unavailable"),
    })?;
    process_group.store(id, Ordering::Release);
    let stdin = child.stdin.take().ok_or_else(|| HarnessError::Runtime {
        diagnostic: SanitizedDiagnostic::sanitized("Codex stdin unavailable"),
    })?;
    let stdout = child.stdout.take().ok_or_else(|| HarnessError::Runtime {
        diagnostic: SanitizedDiagnostic::sanitized("Codex stdout unavailable"),
    })?;
    Ok((ChildGuard { child, id }, stdin, stdout))
}

struct ChildGuard {
    child: Child,
    id: u32,
}

impl ChildGuard {
    fn take_stderr(&mut self) -> Option<ChildStderr> {
        self.child.stderr.take()
    }

    async fn kill_and_reap(&mut self) {
        terminate_process_group(self.id);
        let _ = self.child.start_kill();
        let _ = self.child.wait().await;
    }
}

impl Drop for ChildGuard {
    fn drop(&mut self) {
        terminate_process_group(self.id);
        let _ = self.child.start_kill();
    }
}

pub fn terminate_process_group(process_group: u32) {
    if process_group == 0 {
        return;
    }
    #[cfg(unix)]
    if let Ok(process_group) = i32::try_from(process_group) {
        // SAFETY: the child was placed in its own process group at spawn.
        let _ = unsafe { libc::kill(-process_group, libc::SIGKILL) };
    }
}
