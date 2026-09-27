from __future__ import annotations

import threading
import time
import unittest

from backend.yakshed.providers import codex
from backend.yakshed.providers.base import AdapterEvent


class _Notification:
    def __init__(self, method: str, payload: dict):
        self.method = method
        self.payload = payload


class CodexAdapterTest(unittest.TestCase):
    def test_permission_modes_are_explicit_and_fail_closed(self) -> None:
        self.assertEqual(codex.CodexAdapter._sandbox_wire("read_only"), "read-only")
        self.assertEqual(codex.CodexAdapter._sandbox_wire("workspace_write"), "workspace-write")
        self.assertEqual(codex.CodexAdapter._sandbox_wire("full_access"), "danger-full-access")
        with self.assertRaises(ValueError):
            codex.CodexAdapter._sandbox_wire("ambient")

    def test_deltas_keep_identity_and_unknown_turn_status_stays_unknown(self) -> None:
        adapter = codex.CodexAdapter()
        first = adapter._map_notification(_Notification("item/agentMessage/delta", {"threadId": "thread", "turnId": "turn", "itemId": "message", "delta": "a"}))
        second = adapter._map_notification(_Notification("item/agentMessage/delta", {"threadId": "thread", "turnId": "turn", "itemId": "message", "delta": "b"}))
        self.assertNotEqual(first.native_key, second.native_key)
        self.assertEqual(first.payload["message_id"], "message")
        self.assertEqual(first.payload["phase"], "delta")
        final = adapter._map_notification(_Notification("item/completed", {"threadId": "thread", "turnId": "turn", "item": {"type": "agentMessage", "id": "message", "phase": "final_answer", "text": "ab"}}))
        self.assertEqual(final.payload["message_id"], "message")
        self.assertEqual(final.payload["phase"], "completed")
        self.assertEqual(final.payload["text"], "ab")
        unknown = adapter._map_notification(_Notification("turn/completed", {"threadId": "thread", "turn": {"id": "turn", "status": "future-status"}}))
        self.assertEqual(unknown.kind, "run.unknown")

    def test_approval_handler_waits_without_accepting_by_default(self) -> None:
        adapter = codex.CodexAdapter()
        received: list[AdapterEvent] = []
        adapter._runs["run"] = {"thread": type("Thread", (), {"id": "thread"})(), "emit": received.append}
        result: list[dict] = []
        worker = threading.Thread(target=lambda: result.append(adapter._handle_request("item/commandExecution/requestApproval", {"threadId": "thread", "command": "echo approval"}, 7)))
        worker.start()
        deadline = time.monotonic() + 1
        while not received and time.monotonic() < deadline:
            time.sleep(0.01)
        self.assertEqual(received[0].kind, "approval")
        self.assertEqual(received[0].payload["description"], "echo approval")
        approval_id = received[0].payload["approval_id"]
        self.assertTrue(worker.is_alive())
        adapter.respond(approval_id, "approve")
        worker.join(timeout=1)
        self.assertEqual(result, [{"decision": "accept"}])
        self.assertEqual(adapter._handle_request("future/approvalRequest", {}, 8), {})

    def test_collab_children_use_receiver_thread_identity_and_reported_state(self) -> None:
        adapter = codex.CodexAdapter()
        item = {
            "type": "collabAgentToolCall",
            "id": "tool-call-spawn",
            "tool": "spawnAgent",
            "senderThreadId": "parent-thread",
            "receiverThreadIds": ["child-a", "child-b"],
            "agentsStates": {
                "child-a": {"status": "running"},
                "child-b": {"status": "running"},
            },
        }
        started = adapter._map_notification(_Notification("item/completed", {"threadId": "parent-thread", "item": item}))
        self.assertIsInstance(started, list)
        self.assertEqual([event.payload["child_native_id"] for event in started], ["child-a", "child-b"])
        self.assertTrue(all(event.kind == "subagent.started" for event in started))
        self.assertTrue(all(event.native_key == f"subagent:{child}" for event, child in zip(started, ("child-a", "child-b"))))

        item["agentsStates"]["child-a"] = {"status": "completed"}
        item["agentsStates"]["child-b"] = {"status": "errored"}
        completed = adapter._map_notification(_Notification("item/completed", {"threadId": "parent-thread", "item": item}))
        self.assertEqual([event.kind for event in completed], ["subagent.completed", "subagent.completed"])
        self.assertEqual([event.payload["state"] for event in completed], ["completed", "failed"])

    def test_plans_and_diffs_are_reader_artifacts_with_text_content(self) -> None:
        adapter = codex.CodexAdapter()
        plan = adapter._map_notification(_Notification("turn/plan/updated", {"threadId": "thread", "turnId": "turn", "plan": [{"step": "Inspect", "status": "completed"}]}))
        diff = adapter._map_notification(_Notification("turn/diff/updated", {"threadId": "thread", "turnId": "turn", "diff": "@@ -1 +1 @@\n-old\n+new"}))
        self.assertEqual(plan.kind, "artifact")
        self.assertIn("Inspect", plan.payload["content"])
        self.assertEqual(diff.payload["content"], "@@ -1 +1 @@\n-old\n+new")

    def test_unknown_notifications_keep_distinct_raw_observations(self) -> None:
        adapter = codex.CodexAdapter()
        first = adapter._map_notification(_Notification("future/notification", {"threadId": "thread", "value": 1}))
        second = adapter._map_notification(_Notification("future/notification", {"threadId": "thread", "value": 2}))
        self.assertNotEqual(first.native_key, second.native_key)
        self.assertEqual(second.payload["value"], 2)

    def test_unknown_item_lifecycle_keeps_phase_specific_keys(self) -> None:
        adapter = codex.CodexAdapter()
        started = adapter._map_notification(_Notification("item/started", {"threadId": "thread", "item": {"type": "futureItem", "id": "future"}}))
        completed = adapter._map_notification(_Notification("item/completed", {"threadId": "thread", "item": {"type": "futureItem", "id": "future"}}))
        self.assertEqual(started.native_key, "future.started")
        self.assertEqual(completed.native_key, "future.completed")

    def test_child_raw_notifications_are_serialized_off_the_reader(self) -> None:
        adapter = codex.CodexAdapter()
        received: list[AdapterEvent] = []
        adapter._register_child_route("child", received.append, "parent")
        adapter._raw_event("turn/completed", {"threadId": "child", "turn": {"id": "child-turn", "status": "completed"}})
        deadline = time.monotonic() + 1
        while not received and time.monotonic() < deadline:
            time.sleep(0.01)
        self.assertEqual(received[0].kind, "subagent.telemetry")
        self.assertEqual(received[0].payload["child_native_id"], "child")
        adapter.close()

    def test_nested_child_raw_route_preserves_grandchild_identity(self) -> None:
        adapter = codex.CodexAdapter()
        received: list[AdapterEvent] = []
        adapter._register_child_route("child", received.append, "parent")
        adapter._raw_event("item/started", {"threadId": "child", "turnId": "child-turn", "item": {
            "type": "subAgentActivity", "id": "activity", "agentThreadId": "grandchild", "kind": "started",
        }})
        deadline = time.monotonic() + 1
        while (not received or received[0].payload.get("child_native_id") != "grandchild") and time.monotonic() < deadline:
            time.sleep(0.01)
        self.assertTrue(received)
        self.assertEqual(received[0].payload["child_native_id"], "grandchild")
        self.assertEqual(received[0].payload["parent_native_id"], "child")
        adapter._raw_event("turn/completed", {"threadId": "grandchild", "turn": {"id": "grandchild-turn", "status": "completed"}})
        deadline = time.monotonic() + 1
        while len(received) < 2 and time.monotonic() < deadline:
            time.sleep(0.01)
        self.assertGreaterEqual(len(received), 2)
        self.assertTrue(all(event.payload["child_native_id"] == "grandchild" for event in received[-2:]))
        self.assertTrue(all(event.payload["parent_native_id"] == "child" for event in received[-2:]))
        adapter.close()

    def test_failed_turn_maps_neutral_error_message(self) -> None:
        adapter = codex.CodexAdapter()
        event = adapter._map_notification(_Notification("turn/completed", {
            "threadId": "thread", "turn": {"id": "turn", "status": "failed", "error": {"message": "workspace denied"}},
        }))
        self.assertEqual(event.kind, "run.failed")
        self.assertEqual(event.payload["error"], "workspace denied")
        self.assertEqual(event.payload["message"], "workspace denied")

    def test_child_snapshot_uses_reported_agent_name_fallback(self) -> None:
        adapter = codex.CodexAdapter()

        class FakeClient:
            def thread_read(self, _native_id: str, *, include_turns: bool) -> dict:
                self.include_turns = include_turns
                return {"thread": {"id": "child", "agentNickname": "Scout", "turns": []}}

        adapter._client = FakeClient()
        snapshot = adapter._read_child_thread("child")
        self.assertEqual(snapshot["session"]["name"], "Scout")
        self.assertEqual(snapshot["session"]["capabilities"]["resumable"], False)


if __name__ == "__main__":
    unittest.main()
