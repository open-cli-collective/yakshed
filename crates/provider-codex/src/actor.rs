use std::{
    collections::{HashMap, HashSet, VecDeque},
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
    Read,
    Session,
    StartRun { session_id: ProviderSessionId },
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
                request,
                response,
                reply,
            })
            .await
            .map_err(|_| HarnessError::Disconnected)?;
        result.await.map_err(|_| HarnessError::Disconnected)?
    }

    pub async fn diagnostics(&self) -> Result<Vec<String>, HarnessError> {
        let (reply, result) = oneshot::channel();
        self.commands
            .send(CommandMessage::Diagnostics(reply))
            .await
            .map_err(|_| HarnessError::Disconnected)?;
        result.await.map_err(|_| HarnessError::Disconnected)
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

    loop {
        tokio::select! {
            command = commands.recv() => {
                match command {
                    Some(CommandMessage::Request { method, params, mutation, kind, reply }) => {
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
                        } else {
                            pending.insert(id, PendingClient { method, mutation, kind, reply });
                        }
                    }
                    Some(CommandMessage::Respond { request, response, reply }) => {
                        let result = respond_to_server_request(
                            &mut stdin,
                            &mut server_requests,
                            &request,
                            response,
                        ).await;
                        let _ = reply.send(result);
                    }
                    Some(CommandMessage::Diagnostics(reply)) => {
                        let _ = reply.send(diagnostics.iter().cloned().collect());
                    }
                    Some(CommandMessage::Health(reply)) => {
                        let _ = reply.send(());
                    }
                    Some(CommandMessage::Shutdown(reply)) => {
                        child.kill_and_reap().await;
                        let _ = reply.send(());
                        break;
                    }
                    None => {
                        child.kill_and_reap().await;
                        break;
                    }
                }
            }
            inbound = inbound_rx.recv() => {
                match inbound {
                    Some(Inbound::Frame { raw, value }) => {
                        handle_frame(
                            raw,
                            value,
                            &spec,
                            &sanitizer,
                            &events,
                            &mut pending,
                            &mut server_requests,
                            &mut loaded_sessions,
                            &mut runs,
                            &mut reducer,
                        ).await;
                    }
                    Some(Inbound::Malformed(raw)) => {
                        let raw = sanitizer.sanitize(raw);
                        push_diagnostic(&mut diagnostics, raw.clone());
                        let run = sole_active_run(&runs);
                        let _ = events.send(HarnessEvent::MalformedNativePayload {
                            run,
                            item_type: "codex.malformed-frame".to_owned(),
                            native: NativePayload::sanitized(raw),
                        }).await;
                    }
                    Some(Inbound::Oversized(prefix)) => {
                        let prefix = sanitizer.sanitize(prefix);
                        push_diagnostic(&mut diagnostics, prefix.clone());
                        let run = sole_active_run(&runs);
                        let _ = events.send(HarnessEvent::MalformedNativePayload {
                            run,
                            item_type: "codex.oversized-frame".to_owned(),
                            native: NativePayload::sanitized(prefix),
                        }).await;
                    }
                    Some(Inbound::Stderr(chunk)) => {
                        push_diagnostic(&mut diagnostics, sanitizer.sanitize(chunk));
                    }
                    Some(Inbound::Eof) | None => {
                        child.kill_and_reap().await;
                        disconnect(
                            &mut pending,
                            &runs,
                            &events,
                            &sanitizer,
                            "Codex App Server disconnected".to_owned(),
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

#[allow(clippy::too_many_arguments)]
async fn handle_frame(
    raw: String,
    value: Value,
    spec: &CodexRuntimeSpec,
    sanitizer: &Sanitizer,
    events: &HarnessEventSender,
    pending: &mut HashMap<u64, PendingClient>,
    server_requests: &mut HashMap<ProviderRequestHandle, PendingServer>,
    loaded_sessions: &mut HashSet<String>,
    runs: &mut HashMap<(String, String), ProviderRunHandle>,
    reducer: &mut Reducer,
) {
    if value.get("method").is_none() {
        let Some(id) = value.get("id").and_then(Value::as_u64) else {
            return;
        };
        let Some(pending_request) = pending.remove(&id) else {
            return;
        };
        if let Some(error) = value.get("error") {
            let _ = pending_request
                .reply
                .send(Err(classify_protocol_error(error, sanitizer)));
            return;
        }
        let mut result = value.get("result").cloned().unwrap_or_else(|| json!({}));
        sanitizer.sanitize_value(&mut result);
        match &pending_request.kind {
            RequestKind::Session => {
                if let Some(thread_id) = result
                    .get("thread")
                    .and_then(|thread| thread.get("id"))
                    .and_then(Value::as_str)
                {
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
            RequestKind::Read => {}
        }
        let _ = pending_request.reply.send(Ok(result));
        return;
    }

    let method = value
        .get("method")
        .and_then(Value::as_str)
        .unwrap_or("codex.unknown")
        .to_owned();
    let run = run_for_message(&value, runs).or_else(|| sole_active_run(runs));
    let mut safe_value = value;
    sanitizer.sanitize_value(&mut safe_value);
    let request = if safe_value.get("id").is_some() {
        make_server_request(&safe_value, run.clone(), server_requests)
    } else {
        None
    };
    let sanitized = sanitizer.sanitize(raw);
    if let Some(event) = reducer.reduce(sanitized, &safe_value, run.clone(), request) {
        let terminal = matches!(event, HarnessEvent::RunTerminal { .. });
        let _ = events.send(event).await;
        if terminal && let Some(run) = run {
            runs.remove(&(
                run.session_id().as_str().to_owned(),
                run.native_id().as_str().to_owned(),
            ));
        }
    } else if method.starts_with("item/") && safe_value.get("params").is_none() {
        let _ = events
            .send(HarnessEvent::MalformedNativePayload {
                run,
                item_type: method,
                native: NativePayload::sanitized(sanitizer.sanitize(safe_value.to_string())),
            })
            .await;
    }
}

fn make_server_request(
    value: &Value,
    run: Option<ProviderRunHandle>,
    requests: &mut HashMap<ProviderRequestHandle, PendingServer>,
) -> Option<ProviderRequestHandle> {
    let run = run?;
    let method = value.get("method")?.as_str()?;
    let rpc_id = value.get("id")?.clone();
    let native_id = match &rpc_id {
        Value::String(value) => value.clone(),
        value => value.to_string(),
    };
    let handle = ProviderRequestHandle::new(run, ProviderRequestId::new(native_id).ok()?);
    let kind = match method {
        "item/commandExecution/requestApproval" | "item/fileChange/requestApproval" => {
            ServerRequestKind::Approval
        }
        "item/tool/requestUserInput" => ServerRequestKind::UserInput {
            question_ids: value
                .get("params")
                .and_then(|params| params.get("questions"))
                .and_then(Value::as_array)
                .into_iter()
                .flatten()
                .filter_map(|question| question.get("id").and_then(Value::as_str))
                .map(str::to_owned)
                .collect(),
        },
        _ => return None,
    };
    requests.insert(handle.clone(), PendingServer { rpc_id, kind });
    Some(handle)
}

async fn respond_to_server_request(
    stdin: &mut ChildStdin,
    requests: &mut HashMap<ProviderRequestHandle, PendingServer>,
    request: &ProviderRequestHandle,
    response: ProviderResponse,
) -> Result<(), HarnessError> {
    let pending = requests
        .get(request)
        .ok_or_else(|| HarnessError::NotFound {
            entity: "provider request",
            id: request.to_string(),
        })?;
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
            return Err(HarnessError::InvalidInput(
                "provider response does not match request kind".to_owned(),
            ));
        }
    };
    write_frame(stdin, &json!({"id": pending.rpc_id, "result": result})).await?;
    requests.remove(request);
    Ok(())
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

fn sole_active_run(
    runs: &HashMap<(String, String), ProviderRunHandle>,
) -> Option<ProviderRunHandle> {
    (runs.len() == 1)
        .then(|| runs.values().next().cloned())
        .flatten()
}

async fn disconnect(
    pending: &mut HashMap<u64, PendingClient>,
    runs: &HashMap<(String, String), ProviderRunHandle>,
    events: &HarnessEventSender,
    sanitizer: &Sanitizer,
    diagnostic: String,
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
    let diagnostic = sanitizer.sanitize(diagnostic);
    for run in runs.values() {
        let _ = events
            .send(HarnessEvent::RunTerminal {
                run: run.clone(),
                state: HarnessRunTerminal::Crashed {
                    diagnostic: SanitizedDiagnostic::sanitized(diagnostic.clone()),
                },
                native: NativePayload::sanitized("{\"event\":\"process-exited\"}"),
            })
            .await;
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
