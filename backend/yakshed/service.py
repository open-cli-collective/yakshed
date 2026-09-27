from __future__ import annotations

import json
from pathlib import Path
import threading
from typing import Any, Callable

from .models import scrub
from .providers.base import Adapter, AdapterEvent, RunContext
from .providers.codex import CodexAdapter
from .providers.demo import DemoAdapter
from .store import Store


ResponseWriter = Callable[[dict[str, Any]], None]


class Service:
    """Product-facing JSONL service with one durable SQLite authority."""

    def __init__(self, data_dir: str | Path, *, demo: bool = False, writer: ResponseWriter | None = None, secrets: tuple[str, ...] = (), workspace_boundary: str | Path | None = None) -> None:
        self.data_dir = Path(data_dir).expanduser().resolve()
        self.data_dir.mkdir(parents=True, exist_ok=True)
        self.store = Store(self.data_dir / "yakshed.sqlite3", secrets=secrets)
        self.store.mark_active_runs_interrupted()
        self.demo_enabled = demo
        self.workspace_boundary = Path(workspace_boundary).expanduser().resolve() if workspace_boundary else None
        self.writer = writer
        self.revision = 0
        self._closing = False
        self._write_lock = threading.RLock()
        self._run_adapters: dict[str, Adapter] = {}
        self._run_contexts: dict[str, RunContext] = {}
        self._approval_adapters: dict[str, Adapter] = {}
        # A provider child is identified by its opaque native thread under the
        # parent session.  The parent run/tool-call is only an observation and
        # changes on resume or on a later wait call.
        self._subagent_links: dict[tuple[str, str], tuple[str, str]] = {}
        self.adapters: dict[str, Adapter] = {"codex": CodexAdapter()}
        if demo:
            self.adapters["demo"] = DemoAdapter()

    def close(self) -> None:
        with self._write_lock:
            if self._closing:
                return
            # Block provider callbacks while the SQLite connection is being
            # transitioned to closed.  Background adapter workers may still
            # wake after close, but they will observe _closing and return.
            self._closing = True
            self.store.mark_active_runs_interrupted()
        for adapter in self.adapters.values():
            adapter.close()
        self.store.close()

    def set_writer(self, writer: ResponseWriter | None) -> None:
        self.writer = writer

    def _notify(self) -> None:
        with self._write_lock:
            self.revision += 1
            if self.writer:
                self.writer({"event": "changed", "revision": self.revision, "active_count": self.store.active_count()})

    def _descriptor(self, adapter: Adapter) -> dict[str, Any]:
        descriptor = adapter.descriptor
        return {"id": descriptor.id, "name": descriptor.name, "capabilities": list(descriptor.capabilities), "models": list(descriptor.models)}

    def _error(self, exc: Exception) -> str:
        return scrub(str(exc), self.store.secrets) or exc.__class__.__name__

    @staticmethod
    def _params(params: Any) -> dict[str, Any]:
        if params is None:
            return {}
        if not isinstance(params, dict):
            raise ValueError("params must be an object")
        return params

    @staticmethod
    def _string(value: Any, name: str, *, required: bool = True) -> str | None:
        if value is None and not required:
            return None
        if not isinstance(value, str) or (required and not value.strip()):
            raise ValueError(f"{name} must be a string")
        return value

    @staticmethod
    def _boolean(params: dict[str, Any], name: str, default: bool) -> bool:
        value = params.get(name, default)
        if not isinstance(value, bool):
            raise ValueError(f"{name} must be boolean")
        return value

    def _workspace(self, raw: Any) -> str:
        if not isinstance(raw, str) or not raw.strip():
            raise ValueError("workspace is required")
        candidate = Path(raw).expanduser()
        if not candidate.is_absolute():
            raise ValueError("workspace must be an absolute path")
        try:
            resolved = candidate.resolve(strict=True)
        except FileNotFoundError as exc:
            raise ValueError("workspace must be an existing directory") from exc
        if not resolved.is_dir() or resolved == Path(resolved.anchor):
            raise ValueError("workspace must be a non-root directory")
        if self.workspace_boundary:
            root = self.workspace_boundary
            try:
                resolved.relative_to(root)
            except ValueError as exc:
                raise ValueError("workspace is outside the trusted workspace root") from exc
        return str(resolved)

    def _task_detail(self, task_id: str) -> dict[str, Any]:
        return self.store.detail(task_id)

    def _provider_event(self, context: RunContext, adapter: Adapter, event: AdapterEvent) -> None:
        with self._write_lock:
            if self._closing:
                return
            self._provider_event_locked(context, adapter, event)

    def _provider_event_locked(self, context: RunContext, adapter: Adapter, event: AdapterEvent) -> None:
        if self._closing:
            return
        payload = {"kind": event.kind, **event.payload}
        event_persisted = False
        if event.kind == "approval":
            approval_id = str(payload.get("approval_id") or event.native_key)
            # Persist the request before publishing the waiting state. A
            # renderer observing that state can then immediately fetch the
            # approval details without racing this callback's second write.
            self.store.append_event(context.task_id, context.session_id, context.run_id, event.native_key, event.kind, payload)
            event_persisted = True
            current_state = (self.store.run(context.run_id) or {}).get("state")
            if current_state in {"cancelling", "interrupted", "completed", "failed", "unknown"}:
                # A late provider request must never resurrect a terminal
                # run. Resolve the adapter-side waiter with a safe denial.
                try:
                    adapter.respond(approval_id, "deny")
                except Exception:
                    pass
            else:
                self._approval_adapters[approval_id] = adapter
                self.store.update_run(context.run_id, state="waiting")
                self.store.update_session(context.session_id, state="waiting")
        elif event.kind == "run.started":
            current = (self.store.run(context.run_id) or {}).get("state")
            if current not in {"cancelling", "interrupted", "completed", "failed", "unknown"}:
                native_id = payload.get("native_id")
                now = self.store.run(context.run_id) or {}
                effective = payload.get("effective_options") if isinstance(payload.get("effective_options"), dict) else None
                run_changes: dict[str, Any] = {"state": "running", "started_at": now.get("started_at") or self._timestamp()}
                if effective is not None:
                    run_changes["effective_options"] = effective
                if payload.get("run_native_id") is not None:
                    run_changes["native_id"] = payload["run_native_id"]
                if isinstance(payload.get("metadata"), dict):
                    run_changes["metadata"] = payload["metadata"]
                if payload.get("reported_start_at") is not None:
                    run_changes["reported_start_at"] = payload["reported_start_at"]
                if payload.get("reported_end_at") is not None:
                    run_changes["reported_end_at"] = payload["reported_end_at"]
                self.store.update_run(context.run_id, **run_changes)
                current_session = self.store.session(context.session_id) or {}
                session_changes: dict[str, Any] = {"state": "running", "started_at": current_session.get("started_at") or self._timestamp(), "runtime_version": payload.get("runtime_version")}
                if native_id is not None:
                    session_changes["native_id"] = native_id
                if payload.get("model") is not None:
                    session_changes["model"] = payload["model"]
                if payload.get("provider") is not None:
                    session_changes["provider"] = payload["provider"]
                if effective is not None:
                    session_changes["effective_options"] = effective
                if isinstance(payload.get("metadata"), dict):
                    session_changes["metadata"] = payload["metadata"]
                if current_session.get("reported_start_at") is None and payload.get("reported_start_at") is not None:
                    session_changes["reported_start_at"] = payload["reported_start_at"]
                if current_session.get("reported_end_at") is None and payload.get("reported_end_at") is not None:
                    session_changes["reported_end_at"] = payload["reported_end_at"]
                self.store.update_session(context.session_id, **session_changes)
        elif event.kind == "run.metadata":
            current = (self.store.run(context.run_id) or {}).get("state")
            if current not in {"cancelling", "interrupted", "completed", "failed", "unknown"}:
                self.store.update_run(context.run_id, native_id=payload.get("run_native_id") or event.native_key, reported_start_at=payload.get("reported_start_at"))
        elif event.kind in {"run.completed", "run.failed", "run.interrupted", "run.unknown"}:
            current = (self.store.run(context.run_id) or {}).get("state")
            if current in {"interrupted", "completed", "failed", "unknown"}:
                state = current
            elif current == "cancelling" and event.kind != "run.interrupted":
                state = "interrupted"
            else:
                state = {"run.completed": "completed", "run.failed": "failed", "run.interrupted": "interrupted", "run.unknown": "unknown"}[event.kind]
            run_changes = {"state": state, "ended_at": self._timestamp(), "error": payload.get("error"), "cancellation_reason": payload.get("reason")}
            if payload.get("run_native_id") is not None:
                run_changes["native_id"] = payload["run_native_id"]
            if payload.get("reported_start_at") is not None:
                run_changes["reported_start_at"] = payload["reported_start_at"]
            if payload.get("reported_end_at") is not None:
                run_changes["reported_end_at"] = payload["reported_end_at"]
            if payload.get("raw") is not None:
                run_changes["metadata"] = {"source": context.adapter_id, "event_type": payload.get("method"), "raw": payload.get("raw")}
            self.store.update_run(context.run_id, **run_changes)
            session_changes: dict[str, Any] = {"state": state, "ended_at": self._timestamp(), "last_error": payload.get("error"), "cancellation_reason": payload.get("reason")}
            current_session = self.store.session(context.session_id) or {}
            if current_session.get("reported_end_at") is None and payload.get("reported_end_at") is not None:
                session_changes["reported_end_at"] = payload["reported_end_at"]
            self.store.update_session(context.session_id, **session_changes)
        elif event.kind == "usage":
            self.store.upsert_usage(context.task_id, context.session_id, context.run_id, {**payload, "native_key": event.native_key})
        elif event.kind == "artifact":
            self.store.add_artifact(context.task_id, payload.get("kind", "artifact"), payload.get("name", event.native_key), payload.get("content"), payload.get("mime"), payload, context.run_id, context.session_id)
        elif event.kind == "subagent.telemetry":
            self._record_subagent_telemetry(context, event)
        elif event.kind == "subagent.started":
            child_task, child_session = self._link_subagent(context, event, state=self._subagent_state(event.payload, "running"))
            if isinstance(payload.get("child_snapshot"), dict):
                self._reconcile_child_snapshot(child_task, child_session, payload["child_snapshot"])
        elif event.kind == "subagent.completed":
            child_task, child_session = self._link_subagent(context, event, state=self._subagent_state(event.payload, "unknown"))
            if isinstance(payload.get("child_snapshot"), dict):
                self._reconcile_child_snapshot(child_task, child_session, payload["child_snapshot"])
        elif event.kind == "approval.resolved":
            if (self.store.run(context.run_id) or {}).get("state") not in {"cancelling", "interrupted", "completed", "failed", "unknown"}:
                self.store.update_run(context.run_id, state="running")
                self.store.update_session(context.session_id, state="running")
            self._approval_adapters.pop(str(payload.get("approval_id") or ""), None)

        if not event_persisted:
            self.store.append_event(context.task_id, context.session_id, context.run_id, event.native_key, event.kind, payload)
        if event.kind in {"run.completed", "run.failed", "run.interrupted", "run.unknown"}:
            self._run_adapters.pop(context.run_id, None)
            self._run_contexts.pop(context.run_id, None)
        self._notify()

    @staticmethod
    def _timestamp() -> str:
        from .models import now_iso

        return now_iso()

    @staticmethod
    def _subagent_state(payload: dict[str, Any], default: str) -> str:
        value = str(payload.get("state") or default).lower()
        return {
            "pending": "running",
            "pendinginit": "running",
            "running": "running",
            "completed": "completed",
            "errored": "failed",
            "failed": "failed",
            "interrupted": "interrupted",
            "shutdown": "interrupted",
            "notfound": "unknown",
            "unknown": "unknown",
        }.get(value, "unknown")

    def _link_subagent(self, context: RunContext, event: AdapterEvent, state: str) -> tuple[str, str]:
        native_id = str(event.payload.get("child_native_id") or event.payload.get("agent_id") or event.native_key)
        parent_session_id = context.session_id
        parent_task_id = context.task_id
        root_session_id = (self.store.session(context.session_id) or {}).get("root_session_id")
        # Later child telemetry may omit its parent. Reuse an already linked
        # native session in this work item's tree before using the current run
        # as the parent, avoiding duplicate grandchild siblings.
        existing_native = self.store.session_by_native(native_id, root_session_id=root_session_id)
        if existing_native and existing_native.get("parent_session_id"):
            parent_session_id = existing_native["parent_session_id"]
            parent_task_id = existing_native["task_id"]
        parent_native_id = event.payload.get("parent_native_id")
        if isinstance(parent_native_id, str) and parent_native_id:
            candidate_parent = self.store.session_by_native(parent_native_id, root_session_id=root_session_id)
            if candidate_parent:
                parent_session_id = candidate_parent["id"]
                parent_task_id = candidate_parent["task_id"]
        key = (parent_session_id, native_id)
        link = self._subagent_links.get(key)
        if link is None:
            existing = self.store.subagent_session(parent_session_id, native_id)
            if existing:
                link = (existing["task_id"], existing["id"])
            else:
                child_name = str(event.payload.get("name") or event.payload.get("agent_path") or event.payload.get("agent_nickname") or "Child task")
                session_name = str(event.payload.get("name") or event.payload.get("agent_path") or event.payload.get("agent_nickname") or "Child agent")
                child = self.store.create_task(child_name, parent_task_id=parent_task_id)
                child_session = self.store.create_session(child["id"], context.adapter_id, context.provider, event.payload.get("model"), {}, parent_session_id=parent_session_id, relation="subagent", name=session_name, connection_id=context.connection_id)
                link = (child["id"], child_session["id"])
            self._subagent_links[key] = link
        child_task, child_session = link
        current = self.store.session(child_session) or {}
        previous_state = current.get("state")
        if previous_state in {"completed", "failed", "interrupted"} and state in {"running", "starting", "queued"}:
            state = previous_state
        metadata = dict(current.get("metadata") or {})
        metadata.update({"visibility": event.payload.get("visibility") or metadata.get("visibility") or "summary_only", "source": context.adapter_id, "native_state": event.payload.get("provider_state") or event.payload.get("state") or state})
        if state == "unknown":
            metadata["incomplete"] = True
            metadata["reason"] = "provider exposed a child summary without a terminal state"
        self.store.update_session(child_session, state=state, native_id=native_id, metadata=metadata, ended_at=self._timestamp() if state in {"completed", "failed", "interrupted", "unknown"} else None)
        return child_task, child_session

    def _record_subagent_telemetry(self, context: RunContext, event: AdapterEvent) -> None:
        payload = event.payload
        child_kind = str(payload.get("child_event_kind") or "provider")
        if child_kind == "subagent.started":
            state = self._subagent_state(payload, "running")
        elif child_kind == "subagent.completed":
            state = self._subagent_state(payload, "unknown")
        elif child_kind == "run.completed":
            state = "completed"
        elif child_kind == "run.failed":
            state = "failed"
        elif child_kind == "run.interrupted":
            state = "interrupted"
        elif child_kind == "run.unknown":
            state = "unknown"
        else:
            state = "running"
        child_task, child_session = self._link_subagent(context, event, state=state)
        artifact_kind = payload.get("kind")
        child_payload = {**payload, "kind": child_kind, "parent_run_id": context.run_id}
        if child_kind == "usage":
            self.store.upsert_usage(child_task, child_session, None, {**child_payload, "native_key": event.native_key})
        elif child_kind == "artifact":
            self.store.add_artifact(child_task, artifact_kind or "artifact", child_payload.get("name", event.native_key), child_payload.get("content"), child_payload.get("mime"), child_payload, None, child_session)
        if isinstance(payload.get("child_snapshot"), dict):
            self._reconcile_child_snapshot(child_task, child_session, payload["child_snapshot"])
        self.store.append_event(child_task, child_session, None, event.native_key, child_kind, child_payload)

    def _reconcile_child_snapshot(self, task_id: str, session_id: str, response: dict[str, Any]) -> None:
        """Persist neutral child session/run observations supplied by an adapter."""
        session_info = response.get("session") if isinstance(response.get("session"), dict) else {}
        if not session_info:
            return
        session = self.store.session(session_id) or {}
        metadata = dict(session.get("metadata") or {})
        metadata.update(session_info.get("metadata") if isinstance(session_info.get("metadata"), dict) else {})
        session_changes: dict[str, Any] = {
            "native_id": session_info.get("native_id") or session.get("native_id"),
            "model": session_info.get("model") or session.get("model"),
            "provider": session_info.get("provider") or session.get("provider"),
            "runtime_version": session_info.get("runtime_version") or session.get("runtime_version"),
            "metadata": metadata,
            "reported_start_at": session_info.get("reported_start_at"),
            "reported_end_at": session_info.get("reported_end_at"),
        }
        self.store.update_session(session_id, **session_changes)
        runs = response.get("runs") if isinstance(response.get("runs"), list) else []
        for run_info in runs:
            if not isinstance(run_info, dict):
                continue
            native_id = str(run_info.get("native_id") or "")
            if not native_id:
                continue
            state = str(run_info.get("state") or "unknown")
            if state not in {"queued", "starting", "running", "waiting", "cancelling", "completed", "failed", "interrupted", "unknown"}:
                state = "unknown"
            run = self.store.run_for_native(session_id, native_id)
            if run is None:
                run = self.store.create_run(task_id, session_id, str(run_info.get("prompt") or "Provider child run"), run_info.get("requested_options") if isinstance(run_info.get("requested_options"), dict) else {})
                self.store.update_run(run["id"], native_id=native_id, started_at=self._timestamp())
            elif run.get("state") in {"completed", "failed", "interrupted"} and state in {"queued", "starting", "running", "waiting", "cancelling", "unknown"}:
                state = run["state"]
            run_changes: dict[str, Any] = {
                "state": state,
                "ended_at": self._timestamp() if state in {"completed", "failed", "interrupted", "unknown"} else None,
                "reported_start_at": run_info.get("reported_start_at"),
                "reported_end_at": run_info.get("reported_end_at"),
                "metadata": run_info.get("metadata") if isinstance(run_info.get("metadata"), dict) else {},
            }
            if isinstance(run_info.get("effective_options"), dict):
                run_changes["effective_options"] = run_info["effective_options"]
            self.store.update_run(run["id"], **run_changes)
            observations = run_info.get("events") if isinstance(run_info.get("events"), list) else []
            for observation in observations:
                if not isinstance(observation, dict):
                    continue
                event_kind = str(observation.get("kind") or "provider")
                native_key = str(observation.get("native_key") or f"child:{native_id}:{event_kind}")
                event_payload = observation.get("payload") if isinstance(observation.get("payload"), dict) else {}
                if event_kind == "usage":
                    self.store.upsert_usage(task_id, session_id, run["id"], {**event_payload, "native_key": native_key})
                elif event_kind == "artifact":
                    name = str(event_payload.get("name") or native_key)
                    content = event_payload.get("content") or event_payload.get("text")
                    if not self.store.artifact_exists(task_id, session_id, run["id"], name, content):
                        self.store.add_artifact(task_id, str(event_payload.get("artifact_kind") or "artifact"), name, content, event_payload.get("mime"), event_payload, run["id"], session_id)
                self.store.append_event(task_id, session_id, run["id"], native_key, event_kind, event_payload)

    def _adapter(self, adapter_id: str) -> Adapter:
        adapter = self.adapters.get(adapter_id)
        if not adapter:
            raise KeyError("adapter not available")
        if adapter_id == "demo" and not self.demo_enabled:
            raise ValueError("demo adapter is available only in explicit demo mode")
        return adapter

    def call(self, method: str, params: Any = None) -> Any:
        params = self._params(params)
        if method == "health":
            return {"ok": True, "revision": self.revision}
        if method == "stats":
            return self.store.stats()
        if method == "snapshot":
            search = params.get("search", "")
            if not isinstance(search, str):
                raise ValueError("search must be a string")
            result = self.store.snapshot(search, self._boolean(params, "include_archived", False))
            result["adapters"] = [self._descriptor(adapter) for adapter in self.adapters.values() if adapter.descriptor.id != "demo" or self.demo_enabled]
            result["revision"] = self.revision
            return result
        if method == "task.detail":
            return self._task_detail(self._string(params.get("task_id"), "task_id") or "")
        if method == "task.create":
            parent = params.get("parent_task_id")
            if parent is not None:
                parent = self._string(parent, "parent_task_id")
            task = self.store.create_task(self._string(params.get("title"), "title") or "", parent, params.get("labels"), params.get("permissions"))
            self._notify()
            return self._task_detail(task["id"])
        if method == "task.update":
            task = self.store.update_task(self._string(params.get("task_id"), "task_id") or "", self._params(params.get("changes")))
            self._notify()
            return task
        if method == "task.archive":
            task = self.store.archive_task(self._string(params.get("task_id"), "task_id") or "", self._boolean(params, "archived", True))
            self._notify()
            return task
        if method == "task.undo":
            task = self.store.archive_task(self._string(params.get("task_id"), "task_id") or "", False)
            self._notify()
            return task
        if method == "note.save":
            task_id = self._string(params.get("task_id"), "task_id") or ""
            body = params.get("body")
            if not isinstance(body, str):
                raise ValueError("body must be a string")
            note = self.store.save_note(task_id, body)
            self._notify()
            return note
        if method == "todo.create":
            todo = self.store.add_todo(self._string(params.get("task_id"), "task_id") or "", self._string(params.get("text"), "text") or "")
            self._notify()
            return todo
        if method == "todo.update":
            changes = {key: params[key] for key in ("text", "done", "note") if key in params}
            if "done" in changes and not isinstance(changes["done"], bool):
                raise ValueError("done must be boolean")
            todo = self.store.update_todo(self._string(params.get("todo_id"), "todo_id") or "", changes)
            self._notify()
            return todo
        if method == "todo.delete":
            self.store.delete_todo(self._string(params.get("todo_id"), "todo_id") or "")
            self._notify()
            return {"deleted": True}
        if method == "settings.update":
            allowed = {"theme", "mode", "stay_awake", "menubar", "sidebar_width", "reader_open", "rail_open", "send_shortcut", "default_permission"}
            changes = self._params(params.get("changes"))
            unknown = set(changes) - allowed
            if unknown:
                raise ValueError(f"unsupported settings: {sorted(unknown)}")
            for key, value in changes.items():
                if key in {"stay_awake", "menubar", "reader_open", "rail_open"} and not isinstance(value, bool):
                    raise ValueError(f"{key} must be boolean")
                if key == "theme" and value not in {"og", "ledger", "basalt", "mithril"}:
                    raise ValueError("unsupported theme")
                if key == "mode" and value not in {"compact", "comfortable"}:
                    raise ValueError("unsupported mode")
                if key == "send_shortcut" and value not in {"enter", "command_enter"}:
                    raise ValueError("unsupported send shortcut")
                if key == "default_permission" and value not in {"read_only", "workspace_write", "full_access"}:
                    raise ValueError("unsupported default permission")
                if key == "sidebar_width" and (not isinstance(value, int) or isinstance(value, bool) or not 220 <= value <= 600):
                    raise ValueError("sidebar_width must be an integer from 220 to 600")
                self.store.set_setting(key, value)
            self._notify()
            return self.store.snapshot()["settings"]
        if method == "connection.create":
            adapter_id = self._string(params.get("adapter"), "adapter") or ""
            adapter = self._adapter(adapter_id)
            name = params.get("name", adapter.descriptor.name)
            if not isinstance(name, str):
                raise ValueError("name must be a string")
            provider = params.get("provider", adapter_id)
            if not isinstance(provider, str) or not provider:
                raise ValueError("provider must be a string")
            model = params.get("model")
            if model is not None and not isinstance(model, str):
                raise ValueError("model must be a string")
            connection = self.store.create_connection(name, adapter_id, provider, model, self._params(params.get("options")))
            self._notify()
            return connection
        if method == "adapter.status":
            adapter = self._adapter(self._string(params.get("adapter"), "adapter") or "")
            return adapter.status() if hasattr(adapter, "status") else {"installed": True, "capabilities": list(adapter.descriptor.capabilities)}
        if method == "adapter.login":
            adapter = self._adapter(self._string(params.get("adapter"), "adapter") or "")
            if not hasattr(adapter, "login"):
                raise ValueError("adapter does not support login")
            return adapter.login()
        if method == "run.start":
            return self._start_run(params)
        if method == "run.interrupt":
            if not isinstance(params.get("run_id"), str):
                raise ValueError("run_id must be a string")
            run_id = params["run_id"]
            current = self.store.run(run_id)
            if not current:
                raise KeyError("run not found")
            if current["state"] in {"completed", "failed", "interrupted", "unknown"}:
                return current
            adapter = self._run_adapters.get(run_id)
            if not adapter:
                raise KeyError("run is not active")
            # Mark the request before the external SDK call.  A fast SDK can
            # deliver turn/completed(interrupted) synchronously; writing
            # cancelling afterwards would resurrect a terminal run.
            self.store.update_run(run_id, state="cancelling", cancellation_reason="user")
            run = self.store.run(run_id) or {}
            self.store.update_session(run["session_id"], state="cancelling", cancellation_reason="user")
            self._notify()
            try:
                adapter.interrupt(run_id)
            except Exception as exc:
                # Keep `cancelling` until a provider terminal event confirms
                # the outcome.  The UI must not report a stopped run when the
                # provider rejected or lost the interrupt request.
                self.store.update_run(run_id, error=self._error(exc))
                self._notify()
            return self.store.run(run_id)
        if method == "approval.respond":
            approval_id = self._string(params.get("approval_id"), "approval_id") or ""
            adapter = self._approval_adapters.get(approval_id)
            if not adapter:
                raise KeyError("approval not found")
            decision = self._string(params.get("decision"), "decision") or ""
            if decision not in {"approve", "deny"}:
                raise ValueError("decision must be approve or deny")
            answers = params.get("answers")
            if answers is not None and not isinstance(answers, dict):
                raise ValueError("answers must be an object")
            adapter.respond(approval_id, decision, answers)
            self._notify()
            return {"approval_id": approval_id, "decision": params.get("decision")}
        if method == "demo.seed":
            if not self.demo_enabled:
                raise ValueError("demo mode is disabled")
            return self._seed_demo()
        raise KeyError(f"unknown method: {method}")

    def _start_run(self, params: dict[str, Any]) -> dict[str, Any]:
        if not isinstance(params.get("task_id"), str) or not params["task_id"]:
            raise ValueError("task_id must be a string")
        task_id = params["task_id"]
        task = self.store.effective_task(task_id)
        if not task:
            raise KeyError("task not found")
        if "model" in params and params["model"] is not None and not isinstance(params["model"], str):
            raise ValueError("model must be a string")
        workspace = self._workspace(params.get("workspace"))
        if not isinstance(params.get("connection_id"), str) or not params["connection_id"]:
            raise ValueError("connection_id must be a string")
        connection = self.store.connection(params["connection_id"])
        if not connection:
            raise KeyError("connection not found")
        adapter = self._adapter(connection["adapter"])
        session_id = params.get("session_id")
        if session_id:
            if not isinstance(session_id, str):
                raise ValueError("session_id must be a string")
            session = self.store.session(str(session_id))
            if not session or session["task_id"] != task_id:
                raise KeyError("session not found for task")
            if session.get("connection_id") != connection["id"] or session.get("adapter") != connection["adapter"]:
                raise ValueError("provider switch requires a new session")
            if self.store.active_run_for_session(session_id) or session.get("state") in {"queued", "starting", "running", "waiting", "cancelling"}:
                raise ValueError("session already has an active run")
        else:
            options = {**connection.get("options", {}), **self._params(params.get("options"))}
            options["workspace"] = workspace
            model = params.get("model") or connection.get("model")
            if model is not None and not isinstance(model, str):
                raise ValueError("model must be a string")
            session_name = params.get("session_name", task["title"])
            if not isinstance(session_name, str) or not session_name.strip():
                raise ValueError("session_name must be a string")
            session = self.store.create_session(task_id, connection["adapter"], connection["provider"], model, options, name=session_name, connection_id=connection["id"])
            session_id = session["id"]
        prompt = (self._string(params.get("prompt"), "prompt") or "").strip()
        options = {**(session.get("requested_options") or {}), **self._params(params.get("options"))}
        # Workspace is a per-run continuity field owned by the service. Keep
        # it in both the session and run request options so restart/resume can
        # restore the same task workspace without relying on renderer state.
        options["workspace"] = workspace
        requested_model = params.get("model")
        if requested_model is not None and not isinstance(requested_model, str):
            raise ValueError("model must be a string")
        options["model"] = requested_model or session.get("model") or options.get("model")
        sandbox = task.get("effective_permissions", {}).get("mode", "read_only")
        if sandbox not in {"read_only", "workspace_write", "full_access"}:
            raise ValueError("invalid effective permission mode")
        options["sandbox"] = sandbox
        run = self.store.create_run(task_id, str(session_id), prompt, options)
        self.store.update_session(str(session_id), state="starting", effective_options=options, ended_at=None, last_error=None, cancellation_reason=None, reported_end_at=None)
        self.store.append_event(task_id, str(session_id), run["id"], f"user:{run['id']}", "user", {"kind": "user", "text": prompt})
        context = RunContext(run_id=run["id"], session_id=str(session_id), task_id=task_id, prompt=prompt, workspace=workspace, native_id=session.get("native_id"), adapter_id=connection["adapter"], provider=connection["provider"], connection_id=connection["id"], options=options)
        self._run_adapters[run["id"]] = adapter
        self._run_contexts[run["id"]] = context
        adapter.start(context, lambda event: self._provider_event(context, adapter, event))
        self._notify()
        return self.store.run(run["id"]) or run

    def _seed_demo(self) -> dict[str, Any]:
        root = self.store.create_task("Review workspace", labels={"area": "blue"}, permissions={"mode": "workspace_write"})
        child = self.store.create_task("Inspect fixture", root["id"], labels={"area": "amber"})
        self.store.save_note(root["id"], "Demo mode is isolated and uses deterministic fixture events.")
        self.store.add_todo(root["id"], "Approve the fixture run")
        self.store.create_connection("Demo", "demo", "demo", "demo-model", {})
        self._notify()
        return self.store.snapshot()


def serve(service: Service, input_stream: Any, output_stream: Any) -> None:
    output_lock = threading.RLock()

    def write(payload: dict[str, Any]) -> None:
        with output_lock:
            output_stream.write(json.dumps(payload, ensure_ascii=False, separators=(",", ":")) + "\n")
            output_stream.flush()

    service.set_writer(write)
    try:
        for line in input_stream:
            if not line.strip():
                continue
            request_id: str | None = None
            try:
                request = json.loads(line)
                if not isinstance(request, dict):
                    raise ValueError("request must be an object")
                request_id = request.get("id")
                if not isinstance(request_id, str):
                    raise ValueError("id must be a string")
                method = request.get("method")
                if not isinstance(method, str):
                    raise ValueError("method must be a string")
                result = service.call(method, request.get("params"))
                write({"id": request_id, "result": result})
            except Exception as exc:
                if request_id is not None:
                    write({"id": request_id, "error": {"message": service._error(exc)}})
    finally:
        service.close()
