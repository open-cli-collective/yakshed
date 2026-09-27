from __future__ import annotations

from dataclasses import dataclass, field
import threading
from typing import Any

from .base import AdapterDescriptor, AdapterEvent, Emit, RunContext


@dataclass
class _Approval:
    event: threading.Event = field(default_factory=threading.Event)
    decision: str = "deny"
    answers: dict[str, Any] = field(default_factory=dict)
    run_id: str | None = None


class DemoAdapter:
    """Deterministic adapter used only by explicit demo mode and tests."""

    descriptor = AdapterDescriptor(
        id="demo",
        name="Deterministic demo",
        capabilities=("run", "interrupt", "approval", "subagents", "usage", "artifacts"),
        models=("demo-model",),
    )

    def __init__(self, require_approval: bool = True) -> None:
        self.require_approval = require_approval
        self._runs: dict[str, threading.Event] = {}
        self._approvals: dict[str, _Approval] = {}
        self._cancelled: set[str] = set()
        self._lock = threading.RLock()

    def start(self, context: RunContext, emit: Emit) -> None:
        stop = threading.Event()
        with self._lock:
            self._runs[context.run_id] = stop
        threading.Thread(target=self._run, args=(context, stop, emit), daemon=True, name=f"yakshed-demo-{context.run_id[:8]}").start()

    def _send(self, emit: Emit, event_kind: str, native_key: str, **payload: Any) -> None:
        emit(AdapterEvent(kind=event_kind, native_key=native_key, payload=payload))

    def _run(self, context: RunContext, stop: threading.Event, emit: Emit) -> None:
        try:
            with self._lock:
                if context.run_id in self._cancelled:
                    self._cancelled.discard(context.run_id)
                    self._send(emit, "run.interrupted", "run.interrupted", reason="user")
                    return
            self._send(emit, "run.started", "run.started", model=context.options.get("model") or "demo-model", effective_options={**context.options, "model": context.options.get("model") or "demo-model"})
            self._send(emit, "assistant", "assistant.intro", text="I am ready to inspect the workspace.", phase="commentary")
            if stop.wait(0.03):
                self._send(emit, "run.interrupted", "run.interrupted", reason="user")
                return
            self._send(emit, "tool", "tool.inspect", name="workspace.inspect", status="started", workspace=context.workspace)
            self._send(emit, "tool", "tool.inspect.output", name="workspace.inspect", status="completed", output="Demo workspace inspection complete.")
            approval_id = f"approval_{context.run_id}"
            if self.require_approval:
                approval = _Approval()
                with self._lock:
                    self._approvals[approval_id] = approval
                    approval.run_id = context.run_id
                self._send(emit, "approval", approval_id, approval_id=approval_id, method="workspace.write", description="Allow the demo adapter to write its fixture artifact?", options=["approve", "deny"])
                self._send(emit, "tool", "tool.inspect.continued", name="workspace.inspect", status="waiting_for_approval", output="The event stream remains live while approval is pending.")
                while not approval.event.wait(0.05):
                    if stop.is_set():
                        approval.decision = "deny"
                        break
                with self._lock:
                    self._approvals.pop(approval_id, None)
                if approval.decision != "approve":
                    self._send(emit, "run.failed" if not stop.is_set() else "run.interrupted", "run.approval.done", reason=approval.decision)
                    return
                self._send(emit, "approval.resolved", f"{approval_id}.resolved", approval_id=approval_id, decision=approval.decision)
            if stop.wait(0.03):
                self._send(emit, "run.interrupted", "run.interrupted", reason="user")
                return
            child_id = f"subagent_{context.run_id}"
            self._send(emit, "subagent.started", child_id, agent_id=child_id, name="demo-child", prompt="Inspect one fixture", state="running")
            self._send(emit, "subagent.completed", child_id, agent_id=child_id, name="demo-child", state="completed", output="Fixture inspection complete.")
            self._send(emit, "artifact", "artifact.fixture", kind="text", name="demo-report.txt", mime="text/plain", content="Demo artifact for an explicitly selected fixture run.")
            self._send(emit, "usage", "usage.session", basis="session_total", scope="own", coverage="session", input_tokens=42, output_tokens=18, reasoning_tokens=7, cached_input_tokens=0, total_tokens=60, context_window=128000)
            self._send(emit, "assistant", "assistant.final", text="The deterministic fixture run completed.", phase="final")
            self._send(emit, "run.completed", "run.completed", status="completed")
        finally:
            with self._lock:
                self._runs.pop(context.run_id, None)
                self._cancelled.discard(context.run_id)

    def interrupt(self, run_id: str) -> None:
        with self._lock:
            self._cancelled.add(run_id)
            stop = self._runs.get(run_id)
            if stop:
                stop.set()
            for approval in self._approvals.values():
                if getattr(approval, "run_id", None) == run_id:
                    approval.event.set()

    def respond(self, approval_id: str, decision: str, answers: dict[str, Any] | None = None) -> None:
        if decision not in {"approve", "deny"}:
            raise ValueError("decision must be approve or deny")
        with self._lock:
            approval = self._approvals.get(approval_id)
            if not approval:
                raise KeyError("approval not found or already resolved")
            approval.decision = decision
            approval.answers = answers or {}
            approval.event.set()

    def close(self) -> None:
        with self._lock:
            for stop in self._runs.values():
                stop.set()
            for approval in self._approvals.values():
                approval.event.set()
