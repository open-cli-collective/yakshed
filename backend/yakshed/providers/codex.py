"""Codex app-server adapter.

All provider-specific imports and transport details live here. The product
service sees only :class:`AdapterEvent` values. The reader shim is deliberately
small and pinned to the openai-codex 0.157.1 client: approval handlers in that
release run inline on the stdout reader, so a human approval must be resolved
on a worker while notifications continue to flow.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
import threading
import os
from queue import Queue
from typing import Any, Callable

from .base import AdapterDescriptor, AdapterEvent, Emit, RunContext


try:  # Keep demo mode usable when the optional frozen SDK is not installed.
    from openai_codex.api import Sandbox, Thread
    from openai_codex.client import CodexClient, CodexConfig
except ImportError:  # pragma: no cover - exercised by environments without extras
    Sandbox = Thread = CodexClient = CodexConfig = None


@dataclass
class _PendingApproval:
    event: threading.Event = field(default_factory=threading.Event)
    decision: str = "deny"
    answers: dict[str, Any] = field(default_factory=dict)
    run_id: str | None = None
    emit: Emit | None = None


class _DispatchingCodexClient(CodexClient if CodexClient is not None else object):
    """Keep app-server notifications flowing while an approval is pending."""

    def __init__(self, request_handler: Callable[[str, dict[str, Any] | None, Any], dict[str, Any]], raw_handler: Callable[[str, dict[str, Any]], None], *args: Any, **kwargs: Any) -> None:
        if CodexClient is None:  # pragma: no cover
            raise RuntimeError("openai-codex is not installed")
        self._request_handler = request_handler
        self._raw_handler = raw_handler
        super().__init__(*args, approval_handler=self._fail_closed, **kwargs)

    @staticmethod
    def _fail_closed(method: str, params: dict[str, Any] | None) -> dict[str, Any]:
        if method in {"item/commandExecution/requestApproval", "item/fileChange/requestApproval"}:
            return {"decision": "decline"}
        return {}

    def _reader_loop(self) -> None:  # pinned private seam; see module docstring
        try:
            while True:
                msg = self._read_message()
                if "method" in msg and "id" in msg:
                    request_id = msg["id"]
                    method = msg.get("method")
                    params = msg.get("params")
                    if isinstance(method, str):
                        threading.Thread(target=self._resolve_server_request, args=(method, params, request_id), daemon=True, name="yakshed-codex-approval").start()
                    else:
                        self._write_message({"id": request_id, "result": {}})
                    continue
                if "method" in msg and "id" not in msg:
                    method = msg.get("method")
                    if isinstance(method, str):
                        params = msg.get("params") if isinstance(msg.get("params"), dict) else {}
                        self._raw_handler(method, params)
                        self._router.route_notification(self._coerce_notification(method, params))
                    continue
                self._router.route_response(msg)
        except BaseException as exc:  # matches the pinned SDK's failure fan-out
            self._router.fail_all(exc)

    def _resolve_server_request(self, method: str, params: Any, request_id: Any) -> None:
        try:
            payload = params if isinstance(params, dict) else None
            result = self._request_handler(method, payload, request_id)
        except Exception:
            result = self._fail_closed(method, params if isinstance(params, dict) else None)
        try:
            self._write_message({"id": request_id, "result": result})
        except Exception:
            return


class CodexAdapter:
    descriptor = AdapterDescriptor(
        id="codex",
        name="OpenAI Codex",
        capabilities=("run", "resume", "interrupt", "approval", "subagents", "usage", "login"),
    )

    def __init__(self) -> None:
        self._client: Any = None
        self._lock = threading.RLock()
        # SDK startup/close are blocking RPCs.  Keep their serialization
        # separate from the lock used by the notification reader and run
        # controls so an approval/notification cannot deadlock startup.
        self._client_init_lock = threading.Lock()
        self._closed = False
        self._runs: dict[str, Any] = {}
        self._approvals: dict[str, _PendingApproval] = {}
        self._raw_events: list[tuple[str, dict[str, Any]]] = []
        self._delta_counts: dict[str, int] = {}
        self._cancelled: set[str] = set()
        self._login_handles: dict[str, Any] = {}
        self._child_routes: dict[str, Emit] = {}
        self._child_parent_ids: dict[str, str] = {}
        self._child_queue: Queue[tuple[str, str, dict[str, Any], Emit] | None] = Queue()
        self._child_worker: threading.Thread | None = None

    @staticmethod
    def secret_values() -> tuple[str, ...]:
        return tuple(value for name in ("OPENAI_API_KEY", "CODEX_API_KEY") if (value := os.environ.get(name)))

    @property
    def available(self) -> bool:
        return CodexClient is not None

    @staticmethod
    def _fail_closed(method: str, params: dict[str, Any] | None) -> dict[str, Any]:
        """Return the only safe wire response for unsupported requests."""
        return _DispatchingCodexClient._fail_closed(method, params)

    def _ensure_client(self) -> Any:
        if CodexClient is None or CodexConfig is None:
            raise RuntimeError("openai-codex==0.157.1 is not installed")
        with self._client_init_lock:
            with self._lock:
                if self._closed:
                    raise RuntimeError("Codex adapter is closed")
                if self._client is not None:
                    return self._client
            config = CodexConfig(client_name="yakshed", client_title="YakShed")
            client = _DispatchingCodexClient(self._handle_request, self._raw_event, config=config)
            try:
                # These calls start a subprocess and wait for JSON-RPC
                # responses.  They must stay outside self._lock.
                client.start()
                client.initialize()  # initialize() already emits initialized.
            except Exception:
                try:
                    client.close()
                except Exception:
                    pass
                raise
            with self._lock:
                if self._closed:
                    close_after_init = True
                else:
                    self._client = client
                    close_after_init = False
            if close_after_init:
                client.close()
                raise RuntimeError("Codex adapter is closed")
            return client

    @staticmethod
    def _sandbox_wire(mode: str) -> str:
        values = {"read_only": "read-only", "workspace_write": "workspace-write", "full_access": "danger-full-access"}
        if mode not in values:
            raise ValueError("invalid permission mode")
        return values[mode]

    def _raw_event(self, method: str, params: dict[str, Any]) -> None:
        child_emit: Emit | None = None
        with self._lock:
            self._raw_events.append((method, params))
            if len(self._raw_events) > 2000:
                del self._raw_events[:500]
            thread_id = params.get("threadId") if isinstance(params, dict) else None
            if isinstance(thread_id, str):
                child_emit = self._child_routes.get(thread_id)
        # Raw child notifications are not present in the parent handle's
        # stream.  Dispatch normalization off the SDK reader so a telemetry
        # burst cannot block approval/response routing.
        if child_emit is not None:
            self._enqueue_child_raw(thread_id, method, params, child_emit)

    def _register_child_route(self, child_thread_id: str, emit: Emit, parent_native_id: str | None = None) -> None:
        if not child_thread_id:
            return
        with self._lock:
            already_registered = child_thread_id in self._child_routes
            self._child_routes[child_thread_id] = emit
            if parent_native_id and child_thread_id != parent_native_id:
                self._child_parent_ids[child_thread_id] = parent_native_id
            buffered = [] if already_registered else [(method, params) for method, params in self._raw_events if params.get("threadId") == child_thread_id]
            if self._child_worker is None or not self._child_worker.is_alive():
                self._child_worker = threading.Thread(target=self._child_worker_loop, daemon=True, name="yakshed-codex-child-events")
                self._child_worker.start()
        for method, params in buffered:
            self._enqueue_child_raw(child_thread_id, method, params, emit)

    def _enqueue_child_raw(self, child_thread_id: str, method: str, params: dict[str, Any], emit: Emit) -> None:
        self._child_queue.put((child_thread_id, method, params, emit))

    def _child_worker_loop(self) -> None:
        while True:
            item = self._child_queue.get()
            try:
                if item is None:
                    return
                self._emit_child_raw(*item)
            finally:
                self._child_queue.task_done()

    def _emit_child_raw(self, child_thread_id: str, method: str, params: dict[str, Any], emit: Emit) -> None:
        try:
            mapped = self._map_notification(type("RawNotification", (), {"method": method, "payload": params})())
            events = mapped if isinstance(mapped, list) else [mapped]
            for event in events:
                if event is None:
                    continue
                mapped_child_id = event.payload.get("child_native_id")
                target_native_id = str(mapped_child_id or child_thread_id)
                with self._lock:
                    route_parent_id = self._child_parent_ids.get(child_thread_id)
                parent_native_id = str(event.payload.get("parent_native_id") or route_parent_id or child_thread_id)
                # A nested subagent event changes the target route. Preserve
                # that identity and remember its parent before ordinary
                # notifications from the nested thread arrive.
                if event.kind in {"subagent.started", "subagent.completed"}:
                    self._register_child_route(target_native_id, emit, parent_native_id)
                child_snapshot = self._read_child_thread(target_native_id) if event.kind in {"run.completed", "subagent.completed"} else None
                payload = {
                    **event.payload,
                    "child_native_id": target_native_id,
                    "parent_native_id": parent_native_id,
                    "child_event_kind": event.kind,
                    "child_native_key": event.native_key,
                    "visibility": "provider_stream",
                }
                if child_snapshot is not None:
                    payload["child_snapshot"] = child_snapshot
                emit(AdapterEvent("subagent.telemetry", f"{target_native_id}:{event.native_key}", payload))
        except Exception:
            # Unknown child notifications remain in the raw extension buffer;
            # a malformed provider event must never kill the reader thread.
            return

    def _read_child_thread(self, child_thread_id: str) -> dict[str, Any] | None:
        with self._lock:
            client = self._client
        if client is None:
            return None
        try:
            response = client.thread_read(child_thread_id, include_turns=True)
            raw_response = self._dump(response)
            thread = raw_response.get("thread") if isinstance(raw_response.get("thread"), dict) else {}
            reported_name = thread.get("name") or thread.get("agentNickname") or thread.get("agentRole") or thread.get("agentPath") or "Child agent"
            source_detail = thread.get("source")
            agent_path = thread.get("agentPath")
            if not agent_path and isinstance(source_detail, dict):
                agent_path = (((source_detail.get("subAgent") or {}).get("thread_spawn") or {}).get("agent_path"))
            normalized: dict[str, Any] = {
                "native_id": thread.get("id") or child_thread_id,
                "name": reported_name,
                "model": thread.get("model"),
                "provider": thread.get("modelProvider"),
                "parent_native_id": thread.get("parentThreadId"),
                "runtime_version": thread.get("cliVersion"),
                "workspace": thread.get("cwd"),
                "reported_start_at": self._reported_time(thread.get("createdAt")),
                "reported_end_at": self._reported_time(thread.get("updatedAt")),
                "metadata": {
                    "source": "codex",
                    "reported_name": reported_name,
                    "agent_nickname": thread.get("agentNickname"),
                    "agent_role": thread.get("agentRole"),
                    "agent_path": agent_path,
                    "source_detail": source_detail,
                    "raw": raw_response,
                },
                "turns": [],
            }
            turns = thread.get("turns") if isinstance(thread.get("turns"), list) else []
            for turn in turns:
                if not isinstance(turn, dict):
                    continue
                normalized_turn: dict[str, Any] = {
                    "native_id": turn.get("id"),
                    "state": self._normalize_terminal_state(turn.get("status")),
                    "reported_start_at": self._reported_time(turn.get("startedAt")),
                    "reported_end_at": self._reported_time(turn.get("completedAt")),
                    "metadata": {"source": "codex", "raw": turn},
                    "events": [],
                }
                items = turn.get("items") if isinstance(turn.get("items"), list) else []
                for index, item in enumerate(items):
                    if not isinstance(item, dict):
                        continue
                    item_type = str(item.get("type") or "provider")
                    event_kind = "assistant" if item_type in {"agentMessage", "message"} else "tool" if item_type in {"commandExecution", "mcpToolCall", "webSearch"} else "artifact" if item_type in {"fileChange", "imageGeneration", "plan"} else "provider"
                    artifact_kind = "plan" if item_type == "plan" else "diff" if item_type == "fileChange" else "image" if item_type == "imageGeneration" else None
                    normalized_turn["events"].append({
                        "kind": event_kind,
                        "native_key": str(item.get("id") or f"item-{index}"),
                        "payload": {
                            "message_id": item.get("id"),
                            "phase": "completed",
                            "text": self._item_text(item),
                            "artifact_kind": artifact_kind,
                            "name": "Plan" if artifact_kind == "plan" else "Changes" if artifact_kind == "diff" else "Generated image" if artifact_kind == "image" else item.get("type"),
                            "metadata": {"source": "codex", "raw": item},
                        },
                    })
                normalized["turns"].append(normalized_turn)
            return {"session": {key: value for key, value in normalized.items() if key != "turns"}, "runs": normalized["turns"]}
        except Exception:
            # Thread reconciliation is an enrichment path.  The raw child
            # event remains durable even when the provider cannot hydrate it.
            return None

    def _take_raw(self, method: str, payload: dict[str, Any] | None = None) -> dict[str, Any] | None:
        with self._lock:
            for index, (candidate, raw) in enumerate(self._raw_events):
                if candidate == method:
                    if payload:
                        identity_keys = tuple(key for key in ("threadId", "turnId", "itemId", "requestId") if key in payload)
                        # Every identity field present on the typed event must
                        # agree with the raw envelope.  Matching just one
                        # field can steal a concurrent turn's notification.
                        if identity_keys and not all(key in raw and raw[key] == payload[key] for key in identity_keys):
                            continue
                    self._raw_events.pop(index)
                    return raw
        return None

    def _handle_request(self, method: str, params: dict[str, Any] | None, request_id: Any) -> dict[str, Any]:
        supported = {"item/commandExecution/requestApproval", "item/fileChange/requestApproval"}
        if method not in supported:
            return self._fail_closed(method, params)
        approval_id = f"approval_codex_{request_id}"
        pending = _PendingApproval()
        with self._lock:
            self._approvals[approval_id] = pending
            thread_id = (params or {}).get("threadId") if isinstance(params, dict) else None
            candidates = []
            for run_id, entry in self._runs.items():
                thread = entry.get("thread")
                if thread_id is None or (thread is not None and thread.id == thread_id):
                    candidates.append((run_id, entry.get("emit")))
            # Never attach a request without an identity to an arbitrary
            # concurrent run. The SDK will receive a safe decline instead.
            if len(candidates) != 1 or candidates[0][1] is None:
                self._approvals.pop(approval_id, None)
                return self._fail_closed(method, params)
            pending.run_id, emit = candidates[0]
        if emit and not pending.event.is_set():
            pending.emit = emit
            request = params or {}
            description = request.get("description") or request.get("reason") or request.get("command") or request.get("path") or request.get("toolName") or request.get("serverName")
            if not isinstance(description, str):
                description = str(description) if description is not None else "Provider requested permission to continue."
            emit(AdapterEvent("approval", approval_id, {"approval_id": approval_id, "method": method, "description": description, "request": request, "options": ["approve", "deny"]}))
        while not pending.event.wait(0.05):
            continue
        with self._lock:
            self._approvals.pop(approval_id, None)
        if pending.emit:
            pending.emit(AdapterEvent("approval.resolved", f"{approval_id}.resolved", {"approval_id": approval_id, "decision": pending.decision}))
        return {"decision": "accept" if pending.decision == "approve" else "decline"}

    def _emit(self, emit: Emit, kind: str, native_key: str, payload: dict[str, Any]) -> None:
        raw = self._take_raw(payload.get("method", ""), payload) if payload.get("method") else None
        if raw is not None:
            payload = {**payload, "raw": raw}
        emit(AdapterEvent(kind, native_key, payload))

    @staticmethod
    def _dump(value: Any) -> dict[str, Any]:
        if value is None:
            return {}
        if hasattr(value, "model_dump"):
            try:
                dumped = value.model_dump(mode="json", by_alias=True, exclude_none=False)
                return dumped if isinstance(dumped, dict) else {"value": dumped}
            except Exception:
                pass
        if isinstance(value, dict):
            return value
        return {"value": str(value)}

    @staticmethod
    def _enum_value(value: Any) -> str | None:
        if value is None:
            return None
        raw = getattr(value, "value", value)
        return str(raw) if raw is not None else None

    def _map_collab_item(self, method: str, item: dict[str, Any], payload: dict[str, Any]) -> list[AdapterEvent]:
        """Turn one collab tool item into stable child-thread observations.

        A collab tool call can spawn/wait several children.  The tool-call id
        changes for each wait, so it is not a child identity.  The receiver
        thread id is the stable identity; agentsStates is the provider's last
        reported lifecycle, and is deliberately marked summary-only because
        the parent high-level stream does not include a child transcript.
        """
        receiver_ids = item.get("receiverThreadIds") or item.get("receiver_thread_ids") or []
        states = item.get("agentsStates") or item.get("agents_states") or {}
        if not isinstance(receiver_ids, list):
            receiver_ids = []
        if not isinstance(states, dict):
            states = {}
        # Some future payloads may omit receiverThreadIds while still carrying
        # the keyed state map; retaining those ids avoids losing children.
        receiver_ids = list(dict.fromkeys([str(value) for value in receiver_ids if value] + [str(key) for key in states if key]))
        events: list[AdapterEvent] = []
        raw = payload.get("raw", payload)
        for receiver_id in receiver_ids:
            state_obj = states.get(receiver_id) or states.get(str(receiver_id)) or {}
            if not isinstance(state_obj, dict):
                state_obj = {"status": state_obj}
            provider_state = self._enum_value(state_obj.get("status"))
            normalized = (provider_state or "unknown").lower().replace("_", "")
            terminal = normalized in {"completed", "errored", "interrupted", "shutdown", "notfound", "failed"}
            kind = "subagent.completed" if terminal else "subagent.started"
            state = {
                "pendinginit": "running",
                "running": "running",
                "completed": "completed",
                "errored": "failed",
                "failed": "failed",
                "interrupted": "interrupted",
                "shutdown": "interrupted",
                "notfound": "unknown",
                "unknown": "unknown",
            }.get(normalized, "unknown")
            events.append(
                AdapterEvent(
                    kind,
                    f"subagent:{receiver_id}",
                    {
                        "method": method,
                        "agent_id": receiver_id,
                        "child_native_id": receiver_id,
                        "parent_native_id": item.get("senderThreadId") or item.get("sender_thread_id"),
                        "provider_state": provider_state,
                        "state": state,
                        "visibility": "summary_only",
                        "name": item.get("agentNickname") or item.get("agentPath") or item.get("agent_path"),
                        "agent_path": item.get("agentPath") or item.get("agent_path"),
                        "model": item.get("model"),
                        "prompt": item.get("prompt"),
                        "tool": item.get("tool"),
                        "raw": raw,
                    },
                )
            )
        return events

    def _stream_key(self, native_key: str, family: str) -> str:
        with self._lock:
            key = f"{native_key}.{family}"
            self._delta_counts[key] = self._delta_counts.get(key, 0) + 1
            return f"{key}.{self._delta_counts[key]}"

    @staticmethod
    def _plan_content(payload: dict[str, Any]) -> str:
        explanation = payload.get("explanation")
        steps = payload.get("plan") or []
        lines = [str(explanation).strip()] if explanation else []
        if isinstance(steps, list):
            for step in steps:
                if not isinstance(step, dict):
                    lines.append(str(step))
                    continue
                status = str(step.get("status") or "pending").lower()
                marker = "x" if status == "completed" else ">" if status in {"inprogress", "in_progress"} else " "
                lines.append(f"- [{marker}] {step.get('step') or step.get('text') or ''}".rstrip())
        return "\n".join(lines).strip()

    @staticmethod
    def _changes_content(changes: Any) -> str:
        if isinstance(changes, str):
            return changes
        if changes is None:
            return ""
        try:
            import json

            return json.dumps(changes, ensure_ascii=False, indent=2, sort_keys=True)
        except (TypeError, ValueError):
            return str(changes)

    @staticmethod
    def _item_text(item: dict[str, Any]) -> str:
        for key in ("text", "output", "message", "content", "delta"):
            value = item.get(key)
            if isinstance(value, str):
                return value
            if isinstance(value, list):
                pieces = [part.get("text") for part in value if isinstance(part, dict) and isinstance(part.get("text"), str)]
                if pieces:
                    return "".join(pieces)
        return ""

    def _map_notification(self, notification: Any) -> AdapterEvent | list[AdapterEvent] | None:
        method = getattr(notification, "method", "provider/unknown")
        payload_obj = getattr(notification, "payload", None)
        payload = self._dump(payload_obj)
        raw = self._take_raw(method, payload)
        if raw is not None:
            payload["raw"] = raw
        item = payload.get("item") if isinstance(payload.get("item"), dict) else {}
        item_type = item.get("type")
        native_key = str(item.get("id") or payload.get("itemId") or payload.get("item_id") or payload.get("turnId") or payload.get("turn_id") or method)
        if method == "thread/started":
            return AdapterEvent("run.started", native_key, {"method": method, **payload})
        if method == "item/agentMessage/delta":
            with self._lock:
                self._delta_counts[native_key] = self._delta_counts.get(native_key, 0) + 1
                delta_key = f"{native_key}.delta.{self._delta_counts[native_key]}"
            delta = payload.get("delta") or payload.get("text") or ""
            return AdapterEvent("assistant", delta_key, {"method": method, **payload, "message_id": native_key, "phase": "delta", "text": delta, "delta": delta})
        if method in {"turn/plan/updated", "turn/plan/delta", "item/plan/delta"}:
            content = payload.get("delta") or self._plan_content(payload)
            return AdapterEvent("artifact", self._stream_key(native_key, "plan"), {"method": method, "kind": "plan", "name": "Plan", "mime": "text/markdown", "content": content, **payload})
        if method == "turn/started":
            turn = payload.get("turn") or {}
            reported = self._reported_time(turn.get("startedAt"))
            return AdapterEvent("run.metadata", native_key, {"method": method, "run_native_id": turn.get("id") or native_key, "reported_start_at": reported, **payload})
        if method in {"turn/diff/updated", "item/fileChange/outputDelta", "item/fileChange/patchUpdated"}:
            content = payload.get("diff") or payload.get("delta") or self._changes_content(payload.get("changes"))
            return AdapterEvent("artifact", self._stream_key(native_key, "diff"), {"method": method, "kind": "diff", "name": "Changes", "mime": "text/x-diff", "content": content, **payload})
        if method == "thread/tokenUsage/updated":
            usage = (payload.get("raw") or {}).get("tokenUsage") if isinstance(payload.get("raw"), dict) else None
            usage = usage or payload.get("tokenUsage") or payload.get("token_usage") or {}
            total = usage.get("total") or {}
            return AdapterEvent("usage", str(payload.get("threadId") or "session.total"), {"method": method, "basis": "session_total", "scope": "own", "coverage": "session", "input_tokens": total.get("inputTokens"), "output_tokens": total.get("outputTokens"), "reasoning_tokens": total.get("reasoningOutputTokens"), "cached_input_tokens": total.get("cachedInputTokens"), "cache_write_input_tokens": total.get("cacheWriteInputTokens"), "total_tokens": total.get("totalTokens"), "context_window": usage.get("modelContextWindow"), "rate_limits": payload.get("rateLimits") or (payload.get("raw") or {}).get("rateLimits", {}), "raw": payload.get("raw", payload)})
        if method == "item/completed" or method == "item/started":
            item_key = f"{native_key}.{method.rsplit('/', 1)[-1]}"
            if item_type == "agentMessage":
                return AdapterEvent("assistant", item_key, {"method": method, **item, "message_id": native_key, "phase": "completed" if method == "item/completed" else "started", "text": self._item_text(item), "raw": payload.get("raw", payload)})
            if item_type in {"commandExecution", "mcpToolCall", "webSearch"}:
                return AdapterEvent("tool", item_key, {"method": method, **item, "raw": payload.get("raw", payload)})
            if item_type in {"fileChange", "imageGeneration"}:
                content = item.get("diff") or item.get("output") or self._changes_content(item.get("changes"))
                return AdapterEvent("artifact", item_key, {"method": method, "kind": "diff" if item_type == "fileChange" else "image", "name": "Changes" if item_type == "fileChange" else "Generated image", "content": content, "raw": payload.get("raw", payload), **item})
            if item_type == "collabAgentToolCall":
                return self._map_collab_item(method, item, payload)
            if item_type == "subAgentActivity":
                child_native_id = str(item.get("agentThreadId") or item.get("agent_thread_id") or "")
                if not child_native_id:
                    return AdapterEvent("provider", native_key, {"method": method, "item": item, "raw": payload.get("raw", payload)})
                activity = str(self._enum_value(item.get("kind")) or "unknown").lower()
                terminal = activity in {"completed", "interrupted"}
                state = "completed" if activity == "completed" else "interrupted" if activity == "interrupted" else "running" if activity in {"started", "interacted"} else "unknown"
                return AdapterEvent("subagent.completed" if terminal else "subagent.started", f"subagent:{child_native_id}", {"method": method, "agent_id": child_native_id, "child_native_id": child_native_id, "parent_native_id": payload.get("threadId") or payload.get("thread_id"), "state": state, "provider_state": activity, "visibility": "summary_only", "name": item.get("agentNickname") or item.get("agentPath") or item.get("agent_path"), "agent_path": item.get("agentPath") or item.get("agent_path"), "raw": payload.get("raw", payload)})
            return AdapterEvent("provider", item_key, {"method": method, "item": item, "raw": payload.get("raw", payload)})
        if method == "turn/completed":
            turn = payload.get("turn") or {}
            status = str(turn.get("status") or "unknown").lower()
            kind = "run.completed" if status in {"completed", "complete", "success"} else "run.interrupted" if status in {"interrupted", "cancelled", "canceled"} else "run.failed" if status in {"failed", "error"} else "run.unknown"
            normalized: dict[str, Any] = {"method": method, "status": status, "run_native_id": turn.get("id") or native_key, "reported_start_at": self._reported_time(turn.get("startedAt")), "reported_end_at": self._reported_time(turn.get("completedAt")), **payload}
            error = turn.get("error")
            if isinstance(error, dict):
                message = error.get("message") or error.get("reason") or error.get("detail")
                if message is not None:
                    normalized["error"] = str(message)
                    normalized["message"] = str(message)
            elif error is not None:
                normalized["error"] = str(error)
                normalized["message"] = str(error)
            return AdapterEvent(kind, native_key, normalized)
        # Preserve every otherwise-unmodeled notification.  Repeated methods
        # (including future/unknown events) are distinct observations unless
        # the store receives an exact duplicate key.
        return AdapterEvent("provider", self._stream_key(native_key, "provider"), {"method": method, **payload})

    @staticmethod
    def _reported_time(value: Any) -> str | None:
        if value is None:
            return None
        try:
            number = float(value)
            if number > 10_000_000_000:
                number /= 1000
            return datetime.fromtimestamp(number, timezone.utc).isoformat(timespec="milliseconds")
        except (TypeError, ValueError, OverflowError, OSError):
            return str(value)

    @staticmethod
    def _normalize_terminal_state(value: Any) -> str:
        status = str(getattr(value, "value", value) or "unknown").lower()
        return {"completed": "completed", "complete": "completed", "success": "completed", "failed": "failed", "error": "failed", "interrupted": "interrupted", "cancelled": "interrupted", "canceled": "interrupted"}.get(status, "unknown")

    def start(self, context: RunContext, emit: Emit) -> None:
        threading.Thread(target=self._run, args=(context, emit), daemon=True, name=f"yakshed-codex-{context.run_id[:8]}").start()

    def _run(self, context: RunContext, emit: Emit) -> None:
        try:
            client = self._ensure_client()
            model = context.options.get("model")
            sandbox_name = str(context.options.get("sandbox") or "read_only")
            if Sandbox is None:
                raise RuntimeError("openai-codex is not installed")
            sandbox = getattr(Sandbox, sandbox_name, None)
            if sandbox is None:
                raise ValueError("invalid permission mode")
            sandbox_wire = self._sandbox_wire(sandbox_name)
            response_metadata: dict[str, Any]
            if context.native_id:
                resume_params: dict[str, Any] = {"cwd": context.workspace, "sandbox": sandbox_wire, "approvalPolicy": "on-request", "approvalsReviewer": "user"}
                if model:
                    resume_params["model"] = model
                started = client.thread_resume(context.native_id, resume_params)
                thread = Thread(client, started.thread.id)
                response_metadata = {"source": "codex", "event_type": "thread/resume", "response": self._dump(started)}
            else:
                params: dict[str, Any] = {"cwd": context.workspace, "sandbox": sandbox_wire, "approvalPolicy": "on-request", "approvalsReviewer": "user"}
                if model:
                    params["model"] = model
                started = client.thread_start(params)
                thread = Thread(client, started.thread.id)
                response_metadata = {"source": "codex", "event_type": "thread/start", "response": self._dump(started)}
            started_thread = getattr(started, "thread", None)
            effective_model = getattr(started, "model", None) or getattr(started_thread, "model", None) or model
            effective_provider = getattr(started, "model_provider", None)
            effective_approval_policy = self._enum_value(getattr(started, "approval_policy", None))
            effective_reviewer = self._enum_value(getattr(started, "approvals_reviewer", None))
            effective_options = {
                "model": effective_model,
                "model_provider": effective_provider,
                "sandbox": sandbox_name,
                "approval_policy": effective_approval_policy,
                "approvals_reviewer": effective_reviewer,
            }
            with self._lock:
                self._runs[context.run_id] = {"thread": thread, "emit": emit, "context": context}
            emit(AdapterEvent("run.started", "thread.started", {"native_id": thread.id, "runtime_version": getattr(client, "_runtime_version", None), "model": effective_model, "provider": effective_provider, "effective_options": effective_options, "reported_start_at": self._reported_time(getattr(started_thread, "created_at", None)), "reported_end_at": self._reported_time(getattr(started_thread, "updated_at", None)), "metadata": {**response_metadata, "provider_thread": {"name": getattr(started_thread, "name", None), "created_at": getattr(started_thread, "created_at", None), "updated_at": getattr(started_thread, "updated_at", None), "source": self._enum_value(getattr(started_thread, "source", None)), "cwd": str(getattr(started_thread, "cwd", ""))}}}))
            # An interrupt can arrive after thread/start (or resume) but
            # before Thread.turn has created a handle.  Do not start a turn
            # after cancellation was requested.
            with self._lock:
                if context.run_id in self._cancelled or self._closed:
                    cancelled_before_turn = True
                else:
                    cancelled_before_turn = False
            if cancelled_before_turn:
                emit(AdapterEvent("run.interrupted", "run.interrupted", {"reason": "user"}))
                return
            # The high-level approval_mode enum only exposes auto-review or
            # deny-all. The thread-level reviewer above is explicitly user;
            # omit approval_mode here so it cannot overwrite that choice.
            handle = thread.turn(context.prompt, cwd=context.workspace, model=model, approval_mode=None, sandbox=sandbox)
            with self._lock:
                entry = self._runs.get(context.run_id)
                if entry is None:
                    cancelled_after_turn = True
                else:
                    entry["handle"] = handle
                    cancelled_after_turn = context.run_id in self._cancelled or self._closed
            if cancelled_after_turn:
                try:
                    handle.interrupt()
                except Exception:
                    pass
            for notification in handle.stream():
                mapped = self._map_notification(notification)
                events = mapped if isinstance(mapped, list) else [mapped]
                for event in events:
                    if event:
                        if event.kind in {"subagent.started", "subagent.completed"} and event.payload.get("child_native_id"):
                            child_native_id = str(event.payload["child_native_id"])
                            self._register_child_route(child_native_id, emit, str(event.payload.get("parent_native_id") or context.native_id or "") or None)
                            if event.kind == "subagent.completed":
                                child_snapshot = self._read_child_thread(child_native_id)
                                if child_snapshot is not None:
                                    event = AdapterEvent(event.kind, event.native_key, {**event.payload, "child_snapshot": child_snapshot, "visibility": "snapshot"})
                        emit(event)
        except Exception as exc:
            with self._lock:
                cancelled = context.run_id in self._cancelled
            emit(AdapterEvent("run.interrupted" if cancelled else "run.failed", "run.interrupted" if cancelled else "run.failed", {"reason": "user" if cancelled else None, "error": None if cancelled else str(exc)}))
        finally:
            with self._lock:
                self._runs.pop(context.run_id, None)
                self._cancelled.discard(context.run_id)

    def interrupt(self, run_id: str) -> None:
        handle = None
        pending_events: list[threading.Event] = []
        with self._lock:
            self._cancelled.add(run_id)
            entry = self._runs.get(run_id)
            if entry:
                handle = entry.get("handle")
            for pending in self._approvals.values():
                if pending.run_id == run_id:
                    pending_events.append(pending.event)
        # SDK interrupt is a blocking RPC and the reader needs the adapter
        # lock to route its response.  Capture state under the lock, then do
        # all blocking/callback work outside it.
        if handle is not None:
            try:
                handle.interrupt()
            except Exception as exc:
                # Let the service retain `cancelling` rather than claiming a
                # stop that the provider rejected.  If the stream later
                # confirms interruption, its terminal event wins.
                raise RuntimeError("Codex interrupt request failed") from exc
        for event in pending_events:
            event.set()

    def respond(self, approval_id: str, decision: str, answers: dict[str, Any] | None = None) -> None:
        if decision not in {"approve", "deny"}:
            raise ValueError("decision must be approve or deny")
        with self._lock:
            pending = self._approvals.get(approval_id)
            if not pending:
                raise KeyError("approval not found or already resolved")
            pending.decision = decision
            pending.answers = answers or {}
            pending.event.set()

    def status(self) -> dict[str, Any]:
        if not self.available:
            return {"installed": False, "version": None, "authenticated": False, "capabilities": list(self.descriptor.capabilities), "error": "openai-codex==0.157.1 is not installed"}
        try:
            account = self._ensure_client().account_read()
            authenticated = account.account is not None
            return {"installed": True, "version": "0.157.1", "authenticated": authenticated, "capabilities": list(self.descriptor.capabilities)}
        except Exception:
            return {"installed": True, "version": "0.157.1", "authenticated": None, "capabilities": list(self.descriptor.capabilities), "error": "Codex account status unavailable"}

    def login(self) -> dict[str, Any]:
        if not self.available:
            return {"supported": False, "reason": "openai-codex==0.157.1 is not installed"}
        from openai_codex._login import start_chatgpt_login

        handle = start_chatgpt_login(self._ensure_client())
        with self._lock:
            self._login_handles[handle.login_id] = handle
        return {"supported": True, "login_id": handle.login_id, "auth_url": handle.auth_url, "login_url": handle.auth_url}

    def close(self) -> None:
        # Serialize with startup so an in-flight initialize cannot install a
        # live client after close.  The actual SDK close remains outside the
        # notification lock.
        with self._client_init_lock:
            with self._lock:
                self._closed = True
                pending_events = []
                for pending in self._approvals.values():
                    pending.decision = "deny"
                    pending_events.append(pending.event)
                self._login_handles.clear()
                self._child_routes.clear()
                self._child_parent_ids.clear()
                client, self._client = self._client, None
            for event in pending_events:
                event.set()
            self._child_queue.put(None)
            child_worker = self._child_worker
        if child_worker is not None and child_worker is not threading.current_thread():
            child_worker.join(timeout=1)
        if client is not None:
            try:
                client.close()
            except Exception:
                pass
