from __future__ import annotations

import json
import queue
import subprocess
import sys
import tempfile
import threading
import time
from types import SimpleNamespace
import unittest
from pathlib import Path

from backend.yakshed.providers.base import AdapterEvent, RunContext
from backend.yakshed.providers.codex import CodexAdapter
from backend.yakshed.service import Service


def wait_for(predicate, timeout: float = 2.0):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        value = predicate()
        if value:
            return value
        time.sleep(0.01)
    raise AssertionError("timed out waiting for service state")


class ServiceTest(unittest.TestCase):
    def setUp(self) -> None:
        self.directory = tempfile.TemporaryDirectory()
        self.service = Service(self.directory.name, demo=True)

    def tearDown(self) -> None:
        self.service.close()
        self.directory.cleanup()

    def test_demo_run_has_pending_approval_continued_events_and_child_lineage(self) -> None:
        task = self.service.call("task.create", {"title": "Root", "permissions": {"mode": "workspace_write"}})["task"]
        connection = self.service.call("connection.create", {"name": "Demo", "adapter": "demo"})
        run = self.service.call("run.start", {"task_id": task["id"], "connection_id": connection["id"], "prompt": "inspect", "workspace": self.directory.name})
        wait_for(lambda: self.service.store.run(run["id"])["state"] == "waiting")
        wait_for(lambda: (
            any(event["type"] == "approval" for event in self.service.store.events_for_task(task["id"]))
            and any(event["type"] == "tool" and event["payload"].get("status") == "waiting_for_approval" for event in self.service.store.events_for_task(task["id"]))
        ))
        self.assertEqual(self.service.call("snapshot", {})["tasks"][0]["status"], "waiting")
        self.assertEqual(self.service.call("snapshot", {})["active_count"], 1)
        events = self.service.store.events_for_task(task["id"])
        approval = next(event["payload"]["approval_id"] for event in events if event["type"] == "approval")
        self.assertTrue(any(event["payload"].get("status") == "waiting_for_approval" for event in events if event["type"] == "tool"))
        self.service.call("approval.respond", {"approval_id": approval, "decision": "approve"})
        wait_for(lambda: (
            self.service.store.run(run["id"])["state"] == "completed"
            and any(event["type"] == "run.completed" and event["run_id"] == run["id"] for event in self.service.store.events_for_task(task["id"]))
            and any(event["type"] == "subagent.completed" for event in self.service.store.events_for_task(task["id"]))
            and any(item["kind"] == "text" for item in self.service.store.artifacts(task["id"]))
        ))
        self.assertEqual(self.service.call("snapshot", {})["tasks"][0]["status"], "inbox")
        self.assertEqual(self.service.call("snapshot", {})["active_count"], 0)
        detail = self.service.call("task.detail", {"task_id": task["id"]})
        self.assertEqual(len(detail["children"]), 1)
        self.assertEqual(detail["sessions"][0]["adapter"], "demo")
        self.assertTrue(any(item["kind"] == "text" for item in detail["artifacts"]))

    def test_provider_switch_and_concurrent_session_are_rejected(self) -> None:
        task = self.service.call("task.create", {"title": "Root"})["task"]
        demo = self.service.call("connection.create", {"name": "Demo", "adapter": "demo"})
        run = self.service.call("run.start", {"task_id": task["id"], "connection_id": demo["id"], "prompt": "inspect", "workspace": self.directory.name})
        session_id = run["session_id"]
        with self.assertRaises(ValueError):
            self.service.call("run.start", {"task_id": task["id"], "connection_id": demo["id"], "session_id": session_id, "prompt": "again", "workspace": self.directory.name})
        self.service.call("run.interrupt", {"run_id": run["id"]})
        wait_for(lambda: self.service.store.run(run["id"])["state"] == "interrupted")
        other = self.service.call("connection.create", {"name": "Another demo", "adapter": "demo"})
        with self.assertRaises(ValueError):
            self.service.call("run.start", {"task_id": task["id"], "connection_id": other["id"], "session_id": session_id, "prompt": "switch", "workspace": self.directory.name})

    def test_active_unrepresented_session_counts_and_blocks_resume(self) -> None:
        root = self.service.call("task.create", {"title": "Root"})["task"]
        child = self.service.call("task.create", {"title": "Child", "parent_task_id": root["id"]})["task"]
        connection = self.service.call("connection.create", {"name": "Demo", "adapter": "demo"})
        parent = self.service.store.create_session(root["id"], "demo", "demo", "demo-model", {}, connection_id=connection["id"])
        child_session = self.service.store.create_session(child["id"], "demo", "demo", "demo-model", {}, parent_session_id=parent["id"], relation="subagent", connection_id=connection["id"])
        self.service.store.update_session(child_session["id"], state="running", native_id="child-native")
        self.assertEqual(self.service.call("snapshot", {})["active_count"], 1)
        with self.assertRaises(ValueError):
            self.service.call("run.start", {"task_id": child["id"], "connection_id": connection["id"], "session_id": child_session["id"], "prompt": "resume", "workspace": self.directory.name})

    def test_restart_marks_active_runs_interrupted(self) -> None:
        task = self.service.call("task.create", {"title": "Root"})["task"]
        connection = self.service.call("connection.create", {"name": "Demo", "adapter": "demo"})
        run = self.service.call("run.start", {"task_id": task["id"], "connection_id": connection["id"], "prompt": "inspect", "workspace": self.directory.name})
        self.assertEqual(run["requested_options"]["workspace"], str(Path(self.directory.name).resolve()))
        wait_for(lambda: self.service.store.run(run["id"])["state"] in {"running", "waiting"})
        self.service.close()
        restarted = Service(self.directory.name, demo=True)
        try:
            self.assertEqual(restarted.store.run(run["id"])["state"], "interrupted")
            self.assertEqual(restarted.store.run(run["id"])["requested_options"]["workspace"], str(Path(self.directory.name).resolve()))
        finally:
            restarted.close()

    def test_neutral_child_reconciliation_persists_session_turn_events_and_duration(self) -> None:
        task = self.service.call("task.create", {"title": "Root"})["task"]
        connection = self.service.call("connection.create", {"name": "Demo", "adapter": "demo"})
        session = self.service.store.create_session(task["id"], "demo", "demo", "demo-model", {}, connection_id=connection["id"])
        run = self.service.store.create_run(task["id"], session["id"], "parent", {})
        context = RunContext(run_id=run["id"], session_id=session["id"], task_id=task["id"], prompt="parent", workspace=self.directory.name, adapter_id="demo", provider="demo", connection_id=connection["id"])
        self.service._provider_event(context, self.service.adapters["demo"], AdapterEvent("subagent.completed", "child:done", {
            "child_native_id": "child-native",
            "state": "completed",
            "child_snapshot": {
                "session": {"native_id": "child-native", "name": "Child title", "model": "demo-model", "provider": "demo", "runtime_version": "test", "workspace": self.directory.name, "capabilities": {"resumable": False}, "reported_start_at": "2026-01-01T00:00:00+00:00", "reported_end_at": "2026-01-01T00:00:03+00:00", "metadata": {"visibility": "snapshot"}},
                "runs": [{"native_id": "child-turn", "state": "completed", "reported_start_at": "2026-01-01T00:00:00+00:00", "reported_end_at": "2026-01-01T00:00:03+00:00", "events": [{"kind": "assistant", "native_key": "message", "payload": {"message_id": "message", "phase": "completed", "text": "child result"}}]}],
            },
        }))
        detail = self.service.call("task.detail", {"task_id": task["id"]})
        self.assertEqual(len(detail["children"]), 1)
        child_detail = self.service.call("task.detail", {"task_id": detail["children"][0]["id"]})
        self.assertEqual(child_detail["sessions"][0]["name"], "Child title")
        self.assertEqual(child_detail["sessions"][0]["requested_options"]["workspace"], self.directory.name)
        self.assertEqual(child_detail["sessions"][0]["effective_options"]["workspace"], self.directory.name)
        self.assertEqual(child_detail["sessions"][0]["metadata"]["capabilities"]["resumable"], False)
        self.assertEqual(child_detail["runs"][0]["state"], "completed")
        self.assertEqual(child_detail["events"][0]["payload"]["text"], "child result")
        self.assertEqual(child_detail["duration"]["own_ms"], 3000)
        with self.assertRaisesRegex(ValueError, "This session is managed by its parent task and cannot accept direct prompts\\."):
            self.service.call("run.start", {
                "task_id": detail["children"][0]["id"],
                "connection_id": connection["id"],
                "session_id": child_detail["sessions"][0]["id"],
                "prompt": "resume child",
                "workspace": self.directory.name,
            })
        self.service.store.update_session(session["id"], native_id="parent-native")
        self.service._provider_event(context, self.service.adapters["demo"], AdapterEvent("subagent.started", "grandchild:start", {"child_native_id": "grandchild-native", "parent_native_id": "child-native", "state": "running"}))
        grandchild = self.service.call("task.detail", {"task_id": detail["children"][0]["id"]})
        self.assertEqual(len(grandchild["children"]), 1)
        # Later child telemetry may omit its parent. The durable native id
        # must still route it to the existing grandchild session.
        self.service._provider_event(context, self.service.adapters["demo"], AdapterEvent("subagent.telemetry", "grandchild:assistant", {
            "child_native_id": "grandchild-native", "child_event_kind": "assistant", "text": "still running",
        }))
        grandchild = self.service.call("task.detail", {"task_id": detail["children"][0]["id"]})
        self.assertEqual(len(grandchild["children"]), 1)

    def test_session_reported_times_do_not_become_resumed_run_duration(self) -> None:
        task = self.service.call("task.create", {"title": "Root"})["task"]
        connection = self.service.call("connection.create", {"name": "Demo", "adapter": "demo"})
        session = self.service.store.create_session(task["id"], "demo", "demo", "demo-model", {}, connection_id=connection["id"])
        run = self.service.store.create_run(task["id"], session["id"], "resume", {})
        context = RunContext(run_id=run["id"], session_id=session["id"], task_id=task["id"], prompt="resume", workspace=self.directory.name, adapter_id="demo", provider="demo", connection_id=connection["id"])
        self.service._provider_event(context, self.service.adapters["demo"], AdapterEvent("run.started", "thread.started", {
            "native_id": "provider-session",
            "session_reported_start_at": "2026-01-01T00:00:00+00:00",
        }))
        self.service._provider_event(context, self.service.adapters["demo"], AdapterEvent("run.failed", "run.failed", {"error": "turn could not start", "reported_end_at": "2026-01-01T00:00:03+00:00"}))
        stored_run = self.service.store.run(run["id"])
        stored_session = self.service.store.session(session["id"])
        self.assertIsNone(stored_run["reported_start_at"])
        self.assertEqual(stored_run["reported_end_at"], "2026-01-01T00:00:03+00:00")
        self.assertEqual(stored_session["reported_start_at"], "2026-01-01T00:00:00+00:00")
        self.assertEqual(stored_session["reported_end_at"], "2026-01-01T00:00:03+00:00")
        self.assertLess(self.service.store.duration_rollup(task["id"])["own_ms"], 10_000)

    def test_codex_child_raw_fixture_preserves_root_child_grandchild_chain(self) -> None:
        root = self.service.call("task.create", {"title": "Root"})["task"]
        connection = self.service.call("connection.create", {"name": "Codex", "adapter": "codex"})
        session = self.service.store.create_session(root["id"], "codex", "codex", None, {}, connection_id=connection["id"])
        self.service.store.update_session(session["id"], native_id="root-native")
        run = self.service.store.create_run(root["id"], session["id"], "parent", {})
        context = RunContext(run_id=run["id"], session_id=session["id"], task_id=root["id"], prompt="parent", workspace=self.directory.name, native_id="root-native", adapter_id="codex", provider="codex", connection_id=connection["id"])
        adapter = self.service.adapters["codex"]
        emit = lambda event: self.service._provider_event(context, adapter, event)
        self.service._provider_event(context, adapter, AdapterEvent("subagent.started", "child:start", {"child_native_id": "child-native", "parent_native_id": "root-native", "state": "running"}))
        adapter._register_child_route("child-native", emit, "root-native")
        adapter._emit_child_raw("child-native", "item/started", {"threadId": "child-native", "turnId": "child-turn", "item": {
            "type": "subAgentActivity", "id": "nested", "agentThreadId": "grandchild-native", "kind": "started", "agentPath": "/root/grandchild",
        }}, emit)
        child_task = self.service.call("task.detail", {"task_id": root["id"]})["children"][0]
        child_detail = self.service.call("task.detail", {"task_id": child_task["id"]})
        self.assertEqual(len(child_detail["children"]), 1)
        grandchild = child_detail["children"][0]
        self.assertEqual(grandchild["title"], "/root/grandchild")
        adapter._emit_child_raw("grandchild-native", "item/completed", {"threadId": "grandchild-native", "turnId": "grandchild-turn", "item": {
            "type": "agentMessage", "id": "grandchild-message", "text": "done",
        }}, emit)
        child_detail = self.service.call("task.detail", {"task_id": child_task["id"]})
        self.assertEqual(len(child_detail["children"]), 1)

    def test_fake_codex_driver_covers_new_resume_interrupt_and_approval_stream(self) -> None:
        import backend.yakshed.providers.codex as codex_module

        class FakeSandbox:
            read_only = "read-only"
            workspace_write = "workspace-write"
            full_access = "danger-full-access"

        class Notification:
            def __init__(self, method: str, payload: dict):
                self.method = method
                self.payload = payload

        class FakeHandle:
            def __init__(self, adapter: CodexAdapter, thread_id: str, mode: str, turn_id: str):
                self.adapter = adapter
                self.thread_id = thread_id
                self.mode = mode
                self.turn_id = turn_id
                self.interrupted = threading.Event()

            def interrupt(self) -> None:
                self.interrupted.set()

            def stream(self):
                yield Notification("turn/started", {"threadId": self.thread_id, "turn": {"id": self.turn_id, "startedAt": 1790512000}})
                if self.mode == "approval":
                    result: list[dict] = []
                    worker = threading.Thread(target=lambda: result.append(self.adapter._handle_request("item/commandExecution/requestApproval", {"threadId": self.thread_id, "command": "echo fixture"}, f"approval-{self.turn_id}")), daemon=True)
                    worker.start()
                    deadline = time.monotonic() + 2
                    while not self.adapter._approvals and time.monotonic() < deadline:
                        time.sleep(0.005)
                    yield Notification("item/agentMessage/delta", {"threadId": self.thread_id, "turnId": self.turn_id, "itemId": "approval-continued", "delta": "approval still streaming"})
                    worker.join(timeout=2)
                    yield Notification("item/completed", {"threadId": self.thread_id, "turnId": self.turn_id, "item": {"type": "agentMessage", "id": "approved-message", "text": "approved"}})
                elif self.mode == "interrupt":
                    self.interrupted.wait(2)
                    yield Notification("turn/completed", {"threadId": self.thread_id, "turn": {"id": self.turn_id, "status": "interrupted"}})
                    return
                else:
                    yield Notification("item/completed", {"threadId": self.thread_id, "turnId": self.turn_id, "item": {"type": "agentMessage", "id": f"message-{self.turn_id}", "text": "resumed"}})
                yield Notification("turn/completed", {"threadId": self.thread_id, "turn": {"id": self.turn_id, "status": "completed", "completedAt": 1790512002}})

        class FakeClient:
            def __init__(self, adapter: CodexAdapter):
                self.adapter = adapter
                self.starts: list[dict] = []
                self.resumes: list[tuple[str, dict]] = []
                self.turns: list[dict] = []
                self.mode = "approval"
                self.thread_id = "fake-native-session"
                self.turn_number = 0

            def _response(self, params: dict) -> SimpleNamespace:
                thread = SimpleNamespace(id=self.thread_id, model="fake-model", created_at=1790512000, updated_at=1790512002, name=None, cwd=params.get("cwd"), source=None)
                return SimpleNamespace(thread=thread, model="fake-model", model_provider="fake", approval_policy="on-request", approvals_reviewer="user")

            def thread_start(self, params: dict) -> SimpleNamespace:
                self.starts.append(params)
                return self._response(params)

            def thread_resume(self, native_id: str, params: dict) -> SimpleNamespace:
                self.resumes.append((native_id, params))
                return self._response(params)

            def make_handle(self, thread_id: str) -> FakeHandle:
                self.turn_number += 1
                turn_id = f"fake-turn-{self.turn_number}"
                self.turns.append({"id": turn_id, "mode": self.mode})
                return FakeHandle(self.adapter, thread_id, self.mode, turn_id)

        class FakeThread:
            def __init__(self, client: FakeClient, thread_id: str):
                self.client = client
                self.id = thread_id

            def turn(self, prompt: str, *, cwd: str, model: str | None, approval_mode: object, sandbox: object) -> FakeHandle:
                return self.client.make_handle(self.id)

        original_thread, original_sandbox = codex_module.Thread, codex_module.Sandbox
        adapter = self.service.adapters["codex"]
        fake_client = FakeClient(adapter)
        adapter._ensure_client = lambda: fake_client
        codex_module.Thread, codex_module.Sandbox = FakeThread, FakeSandbox
        try:
            task = self.service.call("task.create", {"title": "Fake SDK"})["task"]
            connection = self.service.call("connection.create", {"name": "Codex", "adapter": "codex"})
            common = {"task_id": task["id"], "connection_id": connection["id"], "workspace": self.directory.name}
            first = self.service.call("run.start", {**common, "prompt": "approve"})
            wait_for(lambda: self.service.store.run(first["id"])["state"] == "waiting")
            approval_events = [event for event in self.service.store.events_for_task(task["id"]) if event["type"] == "approval"]
            self.assertEqual(len(approval_events), 1)
            wait_for(lambda: any(event["payload"].get("text") == "approval still streaming" for event in self.service.store.events_for_task(task["id"])))
            self.service.call("approval.respond", {"approval_id": approval_events[0]["payload"]["approval_id"], "decision": "approve"})
            wait_for(lambda: self.service.store.run(first["id"])["state"] == "completed")
            self.assertEqual(fake_client.starts[0]["approvalPolicy"], "on-request")
            self.assertEqual(fake_client.starts[0]["approvalsReviewer"], "user")
            self.assertEqual(fake_client.starts[0]["sandbox"], "read-only")

            fake_client.mode = "completed"
            resumed = self.service.call("run.start", {**common, "session_id": first["session_id"], "prompt": "resume"})
            wait_for(lambda: self.service.store.run(resumed["id"])["state"] == "completed")
            self.assertEqual(fake_client.resumes[0][0], "fake-native-session")

            fake_client.mode = "interrupt"
            interrupted = self.service.call("run.start", {**common, "session_id": first["session_id"], "prompt": "interrupt"})
            wait_for(lambda: any(event["type"] == "run.metadata" and event["run_id"] == interrupted["id"] for event in self.service.store.events_for_task(task["id"])))
            self.service.call("run.interrupt", {"run_id": interrupted["id"]})
            wait_for(lambda: self.service.store.run(interrupted["id"])["state"] == "interrupted")
            self.assertEqual(len(fake_client.turns), 3)
            self.assertEqual(len({turn["id"] for turn in fake_client.turns}), 3)
        finally:
            codex_module.Thread, codex_module.Sandbox = original_thread, original_sandbox
    def test_note_can_be_cleared_through_product_rpc(self) -> None:
        task = self.service.call("task.create", {"title": "Root"})["task"]
        self.service.call("note.save", {"task_id": task["id"], "body": "draft"})
        self.assertEqual(self.service.call("note.save", {"task_id": task["id"], "body": ""})["body"], "")

    def test_jsonl_process_returns_responses_and_change_hints(self) -> None:
        process = subprocess.Popen([sys.executable, "-m", "backend.yakshed", "--data-dir", self.directory.name, "--demo"], stdin=subprocess.PIPE, stdout=subprocess.PIPE, text=True, bufsize=1)
        messages: queue.Queue[dict] = queue.Queue()
        reader = threading.Thread(target=lambda: [messages.put(json.loads(line)) for line in process.stdout], daemon=True)
        reader.start()

        def call(request_id: str, method: str, params: dict | None = None) -> tuple[dict, list[dict]]:
            process.stdin.write(json.dumps({"id": request_id, "method": method, "params": params or {}}) + "\n")
            process.stdin.flush()
            hints: list[dict] = []
            while True:
                message = messages.get(timeout=2)
                if message.get("id") == request_id:
                    return message, hints
                hints.append(message)

        try:
            snapshot, _ = call("snapshot", "snapshot")
            self.assertIn("adapters", snapshot["result"])
            task, hints = call("task", "task.create", {"title": "Process task"})
            self.assertEqual(task["id"], "task")
            self.assertTrue(any(message.get("event") == "changed" for message in hints))
        finally:
            process.stdin.close()
            process.terminate()
            process.wait(timeout=2)
            process.stdout.close()


if __name__ == "__main__":
    unittest.main()
