from __future__ import annotations

import tempfile
import unittest
import sqlite3
from pathlib import Path

from backend.yakshed.store import Store


class StoreTest(unittest.TestCase):
    def setUp(self) -> None:
        self.directory = tempfile.TemporaryDirectory()
        self.store = Store(Path(self.directory.name) / "state.sqlite3", secrets=("CANARY",))

    def tearDown(self) -> None:
        self.store.close()
        self.directory.cleanup()

    def test_lineage_permissions_and_metadata_are_durable(self) -> None:
        root = self.store.create_task("Root", labels={"area": "blue"}, permissions={"mode": "workspace_write"})
        child = self.store.create_task("Child", root["id"], labels={"phase": "red"})
        self.assertEqual(child["effective_permissions"]["mode"], "workspace_write")
        self.assertEqual(child["effective_labels"], {"area": "blue", "phase": "red"})
        self.store.append_event(root["id"], None, None, "event", "provider", {"apiKey": "CANARY", "text": "CANARY"})
        self.store.append_event(root["id"], None, None, "event", "provider", {"apiKey": "CANARY", "text": "CANARY"})
        events = self.store.events_for_task(root["id"])
        self.assertEqual(len(events), 1)
        self.assertEqual(events[0]["payload"], {"apiKey": "[REDACTED]", "text": "[REDACTED]"})
        self.store.append_event(root["id"], None, None, "item", "assistant", {"text": "partial"})
        self.store.append_event(root["id"], None, None, "item", "assistant", {"text": "complete"})
        assistant = [event for event in self.store.events_for_task(root["id"]) if event["type"] == "assistant"]
        self.assertEqual(len(assistant), 1)
        self.assertEqual(assistant[0]["payload"]["text"], "complete")

    def test_cumulative_snapshots_are_replaced_and_child_usage_is_added_once(self) -> None:
        root = self.store.create_task("Root")
        child = self.store.create_task("Child", root["id"])
        session = self.store.create_session(root["id"], "demo", "demo", "demo", {})
        first = self.store.create_run(root["id"], session["id"], "one", {})
        second = self.store.create_run(root["id"], session["id"], "two", {})
        self.store.upsert_usage(root["id"], session["id"], first["id"], {"native_key": "thread", "basis": "session_total", "input_tokens": 100, "output_tokens": 10, "rate_limits": {"remaining": 5}, "observed_at": "2026-01-01T00:00:01+00:00"})
        self.store.upsert_usage(root["id"], session["id"], second["id"], {"native_key": "thread-after-resume", "basis": "session_total", "input_tokens": 200, "output_tokens": 20, "observed_at": "2026-01-01T00:00:02+00:00"})
        self.store.upsert_usage(root["id"], session["id"], first["id"], {"native_key": "turn", "basis": "turn", "input_tokens": 9, "output_tokens": 1})
        self.store.upsert_usage(child["id"], None, None, {"native_key": "child", "basis": "turn", "input_tokens": 50, "output_tokens": 5})
        self.assertEqual(self.store.usage_rollup(root["id"], include_descendants=False)["input_tokens"], 200)
        self.assertEqual(self.store.usage_rollup(root["id"])["input_tokens"], 250)
        self.assertEqual(self.store.usage_rollup(root["id"])["output_tokens"], 25)
        self.assertEqual(self.store.upsert_usage(root["id"], session["id"], first["id"], {"native_key": "thread", "basis": "session_total", "input_tokens": 100, "rate_limits": {"remaining": 5}})["rate_limits"], {"remaining": 5})

    def test_subtree_coverage_does_not_double_count_independent_sessions(self) -> None:
        root = self.store.create_task("Root")
        child = self.store.create_task("Child", root["id"])
        first = self.store.create_session(root["id"], "demo", "demo", "a", {})
        second = self.store.create_session(root["id"], "demo", "demo", "b", {})
        self.store.upsert_usage(root["id"], first["id"], None, {"native_key": "a", "basis": "turn", "scope": "subtree", "input_tokens": 10})
        self.store.upsert_usage(root["id"], second["id"], None, {"native_key": "b", "basis": "turn", "scope": "subtree", "input_tokens": 20})
        self.store.upsert_usage(child["id"], first["id"], None, {"native_key": "child", "basis": "turn", "input_tokens": 30})
        self.assertEqual(self.store.usage_rollup(root["id"])["input_tokens"], 30)

    def test_subtree_coverage_keeps_unrelated_own_session_rows(self) -> None:
        root = self.store.create_task("Root")
        first = self.store.create_session(root["id"], "demo", "demo", "a", {})
        second = self.store.create_session(root["id"], "demo", "demo", "b", {})
        self.store.upsert_usage(root["id"], first["id"], None, {"native_key": "a", "basis": "turn", "scope": "subtree", "input_tokens": 100})
        self.store.upsert_usage(root["id"], second["id"], None, {"native_key": "b", "basis": "turn", "scope": "own", "input_tokens": 50})
        self.assertEqual(self.store.usage_rollup(root["id"])["input_tokens"], 150)

    def test_subtree_coverage_suppresses_linked_child_session_but_keeps_unrelated_session(self) -> None:
        root = self.store.create_task("Root")
        child = self.store.create_task("Child", root["id"])
        parent_session = self.store.create_session(root["id"], "demo", "demo", "a", {})
        linked_child = self.store.create_session(child["id"], "demo", "demo", "a", {}, parent_session_id=parent_session["id"], relation="subagent", name="Child agent")
        unrelated = self.store.create_session(root["id"], "demo", "demo", "b", {})
        self.store.upsert_usage(root["id"], parent_session["id"], None, {"native_key": "parent", "basis": "turn", "scope": "subtree", "input_tokens": 100})
        self.store.upsert_usage(child["id"], linked_child["id"], None, {"native_key": "linked-child", "basis": "turn", "scope": "own", "input_tokens": 50})
        self.store.upsert_usage(root["id"], unrelated["id"], None, {"native_key": "unrelated", "basis": "turn", "scope": "own", "input_tokens": 25})
        self.assertEqual(self.store.usage_rollup(root["id"])["input_tokens"], 125)

    def test_nested_subtree_and_own_rollups_respect_scope_and_session_lineage(self) -> None:
        root = self.store.create_task("Root")
        child = self.store.create_task("Child", root["id"])
        parent_session = self.store.create_session(root["id"], "demo", "demo", "a", {})
        child_session = self.store.create_session(child["id"], "demo", "demo", "a", {}, parent_session_id=parent_session["id"], relation="subagent", name="Child agent")
        self.store.upsert_usage(root["id"], parent_session["id"], None, {"native_key": "parent", "basis": "turn", "scope": "subtree", "input_tokens": 100})
        self.store.upsert_usage(child["id"], child_session["id"], None, {"native_key": "child", "basis": "turn", "scope": "subtree", "input_tokens": 40})
        self.assertEqual(self.store.usage_rollup(root["id"])["input_tokens"], 100)
        own = self.store.usage_rollup(root["id"], include_descendants=False)
        self.assertIsNone(own["input_tokens"])
        self.assertTrue(own["incomplete"])

    def test_missing_session_usage_is_unknown_instead_of_zero(self) -> None:
        root = self.store.create_task("Root")
        self.store.create_session(root["id"], "demo", "demo", "demo", {})
        rollup = self.store.usage_rollup(root["id"])
        self.assertIsNone(rollup["input_tokens"])
        self.assertTrue(rollup["incomplete"])

    def test_duration_uses_provider_reported_times_when_available(self) -> None:
        root = self.store.create_task("Root")
        session = self.store.create_session(root["id"], "demo", "demo", "demo", {})
        run = self.store.create_run(root["id"], session["id"], "run", {})
        self.store.update_run(run["id"], state="completed", started_at="2026-01-01T00:00:00+00:00", ended_at="2026-01-01T00:00:00.001000+00:00", reported_start_at="2026-01-01T00:00:00+00:00", reported_end_at="2026-01-01T00:00:04+00:00")
        self.assertEqual(self.store.duration_rollup(root["id"])["own_ms"], 4000)

    def test_todo_note_and_settings_survive_reload(self) -> None:
        root = self.store.create_task("Root")
        note = self.store.save_note(root["id"], "first")
        self.assertEqual(self.store.save_note(root["id"], "second")["id"], note["id"])
        todo = self.store.add_todo(root["id"], "one")
        self.assertEqual(self.store.update_todo(todo["id"], {"text": "two", "note": "detail", "done": True})["note"], "detail")
        self.store.set_setting("theme", "ledger")
        self.assertEqual(self.store.snapshot()["settings"]["theme"], "ledger")
        self.store.close()
        self.store = Store(Path(self.directory.name) / "state.sqlite3", secrets=("CANARY",))
        self.assertEqual(self.store.snapshot()["settings"]["theme"], "ledger")
        self.assertEqual(self.store.notes(root["id"])[0]["body"], "second")
        self.assertEqual(self.store.todos(root["id"])[0]["text"], "two")

    def test_default_permission_is_durable_and_only_explicit_task_modes_override_it(self) -> None:
        self.store.set_setting("default_permission", "workspace_write")
        root = self.store.create_task("Root")
        child = self.store.create_task("Child", root["id"])
        restricted = self.store.create_task("Restricted", root["id"], permissions={"mode": "read_only"})
        self.assertEqual(root["effective_permissions"]["mode"], "workspace_write")
        self.assertEqual(child["effective_permissions"]["mode"], "workspace_write")
        self.assertEqual(restricted["effective_permissions"]["mode"], "read_only")
        self.store.set_setting("default_permission", "full_access")
        self.assertEqual(self.store.effective_task(root["id"])["effective_permissions"]["mode"], "full_access")
        self.assertEqual(self.store.effective_task(restricted["id"])["effective_permissions"]["mode"], "read_only")

    def test_unsupported_schema_is_rejected_before_ddl(self) -> None:
        path = Path(self.directory.name) / "future.sqlite3"
        db = sqlite3.connect(path)
        db.execute("CREATE TABLE schema_meta(version INTEGER NOT NULL)")
        db.execute("INSERT INTO schema_meta(version) VALUES (2)")
        db.commit()
        db.close()
        with self.assertRaises(RuntimeError):
            Store(path)
        db = sqlite3.connect(path)
        self.assertIsNone(db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='sessions'").fetchone())
        db.close()


if __name__ == "__main__":
    unittest.main()
