from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timezone
import sqlite3
from pathlib import Path
import threading
from typing import Any, Iterator

from .models import dumps, loads, new_id, now_iso, scrub


SCHEMA = """
CREATE TABLE IF NOT EXISTS schema_meta (version INTEGER NOT NULL);
CREATE TABLE IF NOT EXISTS tasks (
  id TEXT PRIMARY KEY,
  parent_task_id TEXT REFERENCES tasks(id),
  root_task_id TEXT NOT NULL,
  title TEXT NOT NULL,
  status TEXT NOT NULL DEFAULT 'inbox',
  archived INTEGER NOT NULL DEFAULT 0,
  archived_at TEXT,
  previous_status TEXT,
  labels_json TEXT NOT NULL DEFAULT '{}',
  permissions_json TEXT NOT NULL DEFAULT '{}',
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS tasks_parent_idx ON tasks(parent_task_id);
CREATE INDEX IF NOT EXISTS tasks_archive_idx ON tasks(archived, updated_at DESC);
CREATE TABLE IF NOT EXISTS connections (
  id TEXT PRIMARY KEY,
  name TEXT NOT NULL,
  adapter TEXT NOT NULL,
  provider TEXT NOT NULL,
  model TEXT,
  options_json TEXT NOT NULL DEFAULT '{}',
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS sessions (
  id TEXT PRIMARY KEY,
  task_id TEXT NOT NULL REFERENCES tasks(id),
  connection_id TEXT REFERENCES connections(id),
  parent_session_id TEXT REFERENCES sessions(id),
  root_session_id TEXT NOT NULL,
  relation TEXT NOT NULL DEFAULT 'root',
  name TEXT NOT NULL DEFAULT '',
  adapter TEXT NOT NULL,
  provider TEXT NOT NULL,
  model TEXT,
  native_id TEXT,
  requested_options_json TEXT NOT NULL DEFAULT '{}',
  effective_options_json TEXT NOT NULL DEFAULT '{}',
  runtime_version TEXT,
  metadata_json TEXT NOT NULL DEFAULT '{}',
  reported_start_at TEXT,
  reported_end_at TEXT,
  state TEXT NOT NULL DEFAULT 'idle',
  started_at TEXT,
  ended_at TEXT,
  last_error TEXT,
  cancellation_reason TEXT,
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS sessions_task_idx ON sessions(task_id, updated_at DESC);
CREATE TABLE IF NOT EXISTS runs (
  id TEXT PRIMARY KEY,
  task_id TEXT NOT NULL REFERENCES tasks(id),
  session_id TEXT NOT NULL REFERENCES sessions(id),
  native_id TEXT,
  prompt TEXT NOT NULL,
  requested_options_json TEXT NOT NULL DEFAULT '{}',
  effective_options_json TEXT NOT NULL DEFAULT '{}',
  state TEXT NOT NULL DEFAULT 'queued',
  started_at TEXT,
  ended_at TEXT,
  error TEXT,
  cancellation_reason TEXT,
  metadata_json TEXT NOT NULL DEFAULT '{}',
  reported_start_at TEXT,
  reported_end_at TEXT,
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS runs_task_idx ON runs(task_id, created_at DESC);
CREATE TABLE IF NOT EXISTS events (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  task_id TEXT NOT NULL REFERENCES tasks(id),
  session_id TEXT REFERENCES sessions(id),
  run_id TEXT REFERENCES runs(id),
  run_key TEXT NOT NULL,
  sequence INTEGER NOT NULL,
  native_key TEXT NOT NULL,
  type TEXT NOT NULL,
  payload_json TEXT NOT NULL,
  source TEXT NOT NULL,
  created_at TEXT NOT NULL,
  UNIQUE(run_key, native_key, type)
);
CREATE INDEX IF NOT EXISTS events_run_idx ON events(run_id, sequence);
CREATE TABLE IF NOT EXISTS usage (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  task_id TEXT NOT NULL REFERENCES tasks(id),
  session_id TEXT REFERENCES sessions(id),
  run_id TEXT REFERENCES runs(id),
  run_key TEXT NOT NULL,
  native_key TEXT NOT NULL,
  basis TEXT NOT NULL,
  scope TEXT NOT NULL DEFAULT 'own',
  coverage TEXT NOT NULL DEFAULT 'session',
  input_tokens INTEGER,
  output_tokens INTEGER,
  reasoning_tokens INTEGER,
  cached_input_tokens INTEGER,
  cache_write_input_tokens INTEGER,
  total_tokens INTEGER,
  context_window INTEGER,
  rate_limits_json TEXT NOT NULL DEFAULT '{}',
  cost REAL,
  currency TEXT,
  metadata_json TEXT NOT NULL DEFAULT '{}',
  observed_at TEXT NOT NULL,
  UNIQUE(run_key, native_key, basis, scope)
);
CREATE INDEX IF NOT EXISTS usage_task_idx ON usage(task_id, observed_at);
CREATE TABLE IF NOT EXISTS notes (
  id TEXT PRIMARY KEY,
  task_id TEXT NOT NULL REFERENCES tasks(id),
  body TEXT NOT NULL,
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS todos (
  id TEXT PRIMARY KEY,
  task_id TEXT NOT NULL REFERENCES tasks(id),
  text TEXT NOT NULL,
  note TEXT NOT NULL DEFAULT '',
  done INTEGER NOT NULL DEFAULT 0,
  position INTEGER NOT NULL DEFAULT 0,
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS artifacts (
  id TEXT PRIMARY KEY,
  task_id TEXT NOT NULL REFERENCES tasks(id),
  session_id TEXT REFERENCES sessions(id),
  run_id TEXT REFERENCES runs(id),
  kind TEXT NOT NULL,
  name TEXT NOT NULL,
  mime TEXT,
  content TEXT,
  path TEXT,
  metadata_json TEXT NOT NULL DEFAULT '{}',
  created_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS settings (
  key TEXT PRIMARY KEY,
  value_json TEXT NOT NULL,
  updated_at TEXT NOT NULL
);
"""


class Store:
    def __init__(self, path: str | Path, secrets: tuple[str, ...] = ()):
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.db = sqlite3.connect(self.path, check_same_thread=False)
        self.db.row_factory = sqlite3.Row
        self.db.execute("PRAGMA foreign_keys=ON")
        self.db.execute("PRAGMA journal_mode=WAL")
        self.db.execute("PRAGMA busy_timeout=5000")
        self.lock = threading.RLock()
        self.secrets = tuple(secret for secret in secrets if secret)
        with self._tx() as db:
            schema_table = db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='schema_meta'").fetchone()
            if schema_table:
                row = db.execute("SELECT version FROM schema_meta LIMIT 1").fetchone()
                if row is None or row["version"] != 1:
                    raise RuntimeError("unsupported or missing YakShed database schema version")
            else:
                # Only a genuinely fresh database receives schema writes.
                # Existing databases are version-checked before any DDL so a
                # future schema cannot be silently mutated or partially reset.
                db.executescript(SCHEMA)
                db.execute("INSERT INTO schema_meta(version) VALUES (1)")
            required = {
                "sessions": {"connection_id", "name", "metadata_json", "reported_start_at", "reported_end_at"},
                "runs": {"metadata_json", "reported_start_at", "reported_end_at"},
                "events": {"run_key"},
                "usage": {"run_key", "rate_limits_json"},
                "todos": {"note"},
            }
            for table, columns in required.items():
                existing = {item[1] for item in db.execute(f"PRAGMA table_info({table})").fetchall()}
                if not columns.issubset(existing):
                    raise RuntimeError(f"unsupported existing database schema in {table}; choose a fresh data directory")

    @contextmanager
    def _tx(self) -> Iterator[sqlite3.Connection]:
        with self.lock:
            try:
                self.db.execute("BEGIN")
                yield self.db
                self.db.commit()
            except Exception:
                self.db.rollback()
                raise

    def close(self) -> None:
        with self.lock:
            self.db.close()

    def _row(self, row: sqlite3.Row | None) -> dict[str, Any] | None:
        return dict(row) if row else None

    def _decode(self, row: dict[str, Any], *fields: str) -> dict[str, Any]:
        for field in fields:
            key = f"{field}_json"
            if key in row:
                row[field] = loads(row.pop(key), {} if field != "metadata" else {})
        return row

    def _task_row(self, row: sqlite3.Row | None) -> dict[str, Any] | None:
        if not row:
            return None
        return self._decode(dict(row), "labels", "permissions")

    def task(self, task_id: str) -> dict[str, Any] | None:
        with self.lock:
            return self._task_row(self.db.execute("SELECT * FROM tasks WHERE id=?", (task_id,)).fetchone())

    def _ancestors(self, task_id: str) -> list[dict[str, Any]]:
        rows: list[dict[str, Any]] = []
        current = task_id
        while current:
            row = self.task(current)
            if not row:
                break
            rows.append(row)
            current = row.get("parent_task_id")
        return list(reversed(rows))

    def effective_task(self, task_id: str) -> dict[str, Any] | None:
        task = self.task(task_id)
        if not task:
            return None
        chain = self._ancestors(task_id)
        labels: dict[str, Any] = {}
        permissions: dict[str, Any] = {}
        for item in chain:
            labels.update(item.get("labels") or {})
            permissions.update(item.get("permissions") or {})
        # A top-level task without an explicit override inherits the durable
        # workspace default.  Resolving this here keeps the setting useful for
        # tasks created before it changed while preserving explicit overrides
        # and ordinary child inheritance.
        default_permission = self.setting("default_permission", "read_only")
        if default_permission not in {"read_only", "workspace_write", "full_access"}:
            default_permission = "read_only"
        permissions.setdefault("mode", default_permission)
        # Keep the task projection useful to the renderer while retaining the
        # stored inbox/done status for idle work.  A child created from a
        # provider subagent may have a session but no local run, so include
        # both sources of runtime state.
        with self.lock:
            run_states = [row["state"] for row in self.db.execute("SELECT state FROM runs WHERE task_id=?", (task_id,)).fetchall()]
            session_states = [row["state"] for row in self.db.execute("SELECT state FROM sessions WHERE task_id=?", (task_id,)).fetchall()]
        if "waiting" in run_states or "waiting" in session_states:
            task["status"] = "waiting"
        elif any(state in {"queued", "starting", "running", "cancelling"} for state in (*run_states, *session_states)):
            task["status"] = "active"
        task["effective_labels"] = labels
        task["effective_permissions"] = permissions
        return task

    def subagent_session(self, parent_session_id: str, native_id: str) -> dict[str, Any] | None:
        """Return an existing child session for an opaque native session.

        A provider may report the same child on multiple parent runs, and a
        service restart loses its in-memory link cache. The native identifier
        is therefore the durable identity under the parent session.
        """
        if not native_id:
            return None
        with self.lock:
            row = self.db.execute(
                "SELECT * FROM sessions WHERE parent_session_id=? AND native_id=? ORDER BY created_at LIMIT 1",
                (parent_session_id, native_id),
            ).fetchone()
        return self._decode(self._row(row), "requested_options", "effective_options", "metadata") if row else None

    def session_by_native(self, native_id: str, root_session_id: str | None = None) -> dict[str, Any] | None:
        if not native_id:
            return None
        with self.lock:
            if root_session_id:
                row = self.db.execute("SELECT * FROM sessions WHERE native_id=? AND root_session_id=? ORDER BY created_at LIMIT 1", (native_id, root_session_id)).fetchone()
            else:
                row = self.db.execute("SELECT * FROM sessions WHERE native_id=? ORDER BY created_at LIMIT 1", (native_id,)).fetchone()
        return self._decode(self._row(row), "requested_options", "effective_options", "metadata") if row else None

    def session_descendants(self, session_id: str) -> set[str]:
        """Return a provider session and all linked child sessions."""
        with self.lock:
            rows = self.db.execute("SELECT id,parent_session_id FROM sessions").fetchall()
        children: dict[str | None, list[str]] = {}
        for row in rows:
            children.setdefault(row["parent_session_id"], []).append(row["id"])
        result = {session_id}
        pending = [session_id]
        while pending:
            current = pending.pop()
            for child in children.get(current, []):
                if child not in result:
                    result.add(child)
                    pending.append(child)
        return result

    def create_task(
        self,
        title: str,
        parent_task_id: str | None = None,
        labels: dict[str, Any] | None = None,
        permissions: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        title = title.strip()
        if not title or len(title) > 240:
            raise ValueError("title must be 1-240 characters")
        parent = self.task(parent_task_id) if parent_task_id else None
        if parent_task_id and not parent:
            raise KeyError("parent task not found")
        labels = labels or {}
        permissions = permissions or {}
        if not isinstance(labels, dict) or not isinstance(permissions, dict):
            raise ValueError("labels and permissions must be objects")
        if "mode" in permissions and permissions["mode"] not in {"read_only", "workspace_write", "full_access"}:
            raise ValueError("invalid permission mode")
        task_id = new_id("task_")
        timestamp = now_iso()
        root = parent["root_task_id"] if parent else task_id
        with self._tx() as db:
            db.execute(
                """INSERT INTO tasks(id,parent_task_id,root_task_id,title,labels_json,permissions_json,created_at,updated_at)
                   VALUES(?,?,?,?,?,?,?,?)""",
                (task_id, parent_task_id, root, scrub(title, self.secrets), dumps(scrub(labels or {}, self.secrets)), dumps(scrub(permissions or {}, self.secrets)), timestamp, timestamp),
            )
        return self.effective_task(task_id) or {}

    def update_task(self, task_id: str, changes: dict[str, Any]) -> dict[str, Any]:
        allowed = {"title", "status", "labels", "permissions"}
        unknown = set(changes) - allowed
        if unknown:
            raise ValueError(f"unsupported task fields: {sorted(unknown)}")
        if not self.task(task_id):
            raise KeyError("task not found")
        values: dict[str, Any] = {}
        for key, value in changes.items():
            if key in {"labels", "permissions"}:
                if not isinstance(value, dict):
                    raise ValueError(f"{key} must be an object")
                values[f"{key}_json"] = dumps(scrub(value, self.secrets))
            elif key == "title":
                if not isinstance(value, str) or not value.strip() or len(value) > 240:
                    raise ValueError("title must be 1-240 characters")
                values[key] = scrub(value.strip(), self.secrets)
            elif key == "status" and value not in {"active", "waiting", "inbox", "done"}:
                raise ValueError("invalid task status")
            else:
                values[key] = value
        if "permissions_json" in values:
            permissions = loads(values["permissions_json"], {})
            if "mode" in permissions and permissions["mode"] not in {"read_only", "workspace_write", "full_access"}:
                raise ValueError("invalid permission mode")
        values["updated_at"] = now_iso()
        with self._tx() as db:
            db.execute(
                f"UPDATE tasks SET {','.join(f'{key}=?' for key in values)} WHERE id=?",
                (*values.values(), task_id),
            )
        return self.effective_task(task_id) or {}

    def archive_task(self, task_id: str, archived: bool = True) -> dict[str, Any]:
        task = self.task(task_id)
        if not task:
            raise KeyError("task not found")
        timestamp = now_iso()
        with self._tx() as db:
            if archived:
                db.execute(
                    "UPDATE tasks SET archived=1,archived_at=?,previous_status=status,updated_at=? WHERE id=?",
                    (timestamp, timestamp, task_id),
                )
            else:
                db.execute(
                    "UPDATE tasks SET archived=0,archived_at=NULL,status=COALESCE(previous_status,status),previous_status=NULL,updated_at=? WHERE id=?",
                    (timestamp, task_id),
                )
        return self.effective_task(task_id) or {}

    def list_tasks(self, search: str = "", include_archived: bool = False) -> list[dict[str, Any]]:
        query = "SELECT * FROM tasks WHERE (? OR archived=0) AND (?='' OR title LIKE ? OR labels_json LIKE ?) ORDER BY updated_at DESC"
        pattern = f"%{search}%"
        with self.lock:
            rows = self.db.execute(query, (int(include_archived), search, pattern, pattern)).fetchall()
        return [self.effective_task(row["id"]) or {} for row in rows]

    def create_connection(self, name: str, adapter: str, provider: str, model: str | None, options: dict[str, Any]) -> dict[str, Any]:
        connection_id = new_id("conn_")
        timestamp = now_iso()
        with self._tx() as db:
            db.execute(
                "INSERT INTO connections VALUES(?,?,?,?,?,?,?,?)",
                (connection_id, scrub(name.strip() or adapter.title(), self.secrets), adapter, scrub(provider, self.secrets), scrub(model, self.secrets) if model else None, dumps(scrub(options, self.secrets)), timestamp, timestamp),
            )
        return self.connection(connection_id) or {}

    def connection(self, connection_id: str) -> dict[str, Any] | None:
        with self.lock:
            row = self._row(self.db.execute("SELECT * FROM connections WHERE id=?", (connection_id,)).fetchone())
        return self._decode(row, "options") if row else None

    def connections(self) -> list[dict[str, Any]]:
        with self.lock:
            rows = self.db.execute("SELECT * FROM connections ORDER BY name").fetchall()
        return [self._decode(dict(row), "options") for row in rows]

    def create_session(self, task_id: str, adapter: str, provider: str, model: str | None, options: dict[str, Any], parent_session_id: str | None = None, relation: str = "root", name: str | None = None, connection_id: str | None = None) -> dict[str, Any]:
        if not self.task(task_id):
            raise KeyError("task not found")
        parent = self.session(parent_session_id) if parent_session_id else None
        if parent_session_id and not parent:
            raise KeyError("parent session not found")
        session_id = new_id("sess_")
        root = parent["root_session_id"] if parent else session_id
        timestamp = now_iso()
        with self._tx() as db:
            db.execute(
                """INSERT INTO sessions(id,task_id,connection_id,parent_session_id,root_session_id,relation,name,adapter,provider,model,requested_options_json,created_at,updated_at)
                   VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                (session_id, task_id, connection_id, parent_session_id, root, relation, scrub(name or provider, self.secrets), adapter, scrub(provider, self.secrets), scrub(model, self.secrets) if model else None, dumps(scrub(options, self.secrets)), timestamp, timestamp),
            )
        return self.session(session_id) or {}

    def session(self, session_id: str) -> dict[str, Any] | None:
        with self.lock:
            row = self._row(self.db.execute("SELECT * FROM sessions WHERE id=?", (session_id,)).fetchone())
        return self._decode(row, "requested_options", "effective_options", "metadata") if row else None

    def sessions_for_task(self, task_id: str) -> list[dict[str, Any]]:
        with self.lock:
            rows = self.db.execute("SELECT * FROM sessions WHERE task_id=? ORDER BY created_at", (task_id,)).fetchall()
        return [self._decode(dict(row), "requested_options", "effective_options", "metadata") for row in rows]

    def update_session(self, session_id: str, **changes: Any) -> dict[str, Any]:
        if not self.session(session_id):
            raise KeyError("session not found")
        fields = {key: value for key, value in changes.items() if key in {"connection_id", "name", "adapter", "provider", "model", "native_id", "runtime_version", "metadata", "reported_start_at", "reported_end_at", "state", "started_at", "ended_at", "last_error", "cancellation_reason", "effective_options"}}
        if "name" in fields:
            fields["name"] = scrub(fields["name"], self.secrets)
        for key in ("adapter", "provider", "model"):
            if key in fields and fields[key] is not None:
                fields[key] = scrub(fields[key], self.secrets)
        for key in ("last_error", "cancellation_reason"):
            if key in fields:
                fields[key] = scrub(fields[key], self.secrets)
        if "effective_options" in fields:
            fields["effective_options_json"] = dumps(scrub(fields.pop("effective_options"), self.secrets))
        if "metadata" in fields:
            fields["metadata_json"] = dumps(scrub(fields.pop("metadata"), self.secrets))
        fields["updated_at"] = now_iso()
        with self._tx() as db:
            db.execute(f"UPDATE sessions SET {','.join(f'{k}=?' for k in fields)} WHERE id=?", (*fields.values(), session_id))
        return self.session(session_id) or {}

    def create_run(self, task_id: str, session_id: str, prompt: str, options: dict[str, Any]) -> dict[str, Any]:
        session = self.session(session_id)
        if not session or session["task_id"] != task_id:
            raise KeyError("session not found for task")
        run_id = new_id("run_")
        timestamp = now_iso()
        with self._tx() as db:
            db.execute(
                "INSERT INTO runs(id,task_id,session_id,prompt,requested_options_json,created_at,updated_at) VALUES(?,?,?,?,?,?,?)",
                (run_id, task_id, session_id, scrub(prompt, self.secrets), dumps(scrub(options, self.secrets)), timestamp, timestamp),
            )
        return self.run(run_id) or {}

    def run(self, run_id: str) -> dict[str, Any] | None:
        with self.lock:
            row = self._row(self.db.execute("SELECT * FROM runs WHERE id=?", (run_id,)).fetchone())
        return self._decode(row, "requested_options", "effective_options", "metadata") if row else None

    def run_for_native(self, session_id: str, native_id: str) -> dict[str, Any] | None:
        with self.lock:
            row = self.db.execute("SELECT * FROM runs WHERE session_id=? AND native_id=? ORDER BY created_at LIMIT 1", (session_id, native_id)).fetchone()
        return self._decode(self._row(row), "requested_options", "effective_options", "metadata") if row else None

    def active_run_for_session(self, session_id: str) -> dict[str, Any] | None:
        with self.lock:
            row = self.db.execute("SELECT * FROM runs WHERE session_id=? AND state IN ('queued','starting','running','waiting','cancelling') ORDER BY created_at DESC LIMIT 1", (session_id,)).fetchone()
        return self._decode(dict(row), "requested_options", "effective_options", "metadata") if row else None

    def update_run(self, run_id: str, **changes: Any) -> dict[str, Any]:
        if not self.run(run_id):
            raise KeyError("run not found")
        fields = {key: value for key, value in changes.items() if key in {"native_id", "state", "started_at", "ended_at", "error", "cancellation_reason", "metadata", "reported_start_at", "reported_end_at", "effective_options"}}
        for key in ("error", "cancellation_reason"):
            if key in fields:
                fields[key] = scrub(fields[key], self.secrets)
        if "effective_options" in fields:
            fields["effective_options_json"] = dumps(scrub(fields.pop("effective_options"), self.secrets))
        if "metadata" in fields:
            fields["metadata_json"] = dumps(scrub(fields.pop("metadata"), self.secrets))
        fields["updated_at"] = now_iso()
        with self._tx() as db:
            db.execute(f"UPDATE runs SET {','.join(f'{k}=?' for k in fields)} WHERE id=?", (*fields.values(), run_id))
        return self.run(run_id) or {}

    def append_event(self, task_id: str, session_id: str | None, run_id: str | None, native_key: str, event_type: str, payload: Any, source: str = "provider") -> dict[str, Any]:
        timestamp = now_iso()
        run_key = run_id or (f"session:{session_id}" if session_id else f"task:{task_id}")
        with self._tx() as db:
            sequence_row = db.execute("SELECT COALESCE(MAX(sequence),0)+1 AS n FROM events WHERE run_key=?", (run_key,)).fetchone()
            sequence = int(sequence_row["n"])
            db.execute(
                """INSERT INTO events(task_id,session_id,run_id,run_key,sequence,native_key,type,payload_json,source,created_at)
                   VALUES(?,?,?,?,?,?,?,?,?,?)
                   ON CONFLICT(run_key,native_key,type) DO UPDATE SET
                     payload_json=excluded.payload_json,source=excluded.source,created_at=excluded.created_at""",
                (task_id, session_id, run_id, run_key, sequence, native_key, event_type, dumps(scrub(payload, self.secrets)), source, timestamp),
            )
            row = db.execute("SELECT * FROM events WHERE run_key=? AND native_key=? AND type=? ORDER BY id DESC LIMIT 1", (run_key, native_key, event_type)).fetchone()
        result = dict(row) if row else {}
        result["payload"] = loads(result.pop("payload_json", "{}"), {})
        return result

    def events_for_task(self, task_id: str, limit: int = 400) -> list[dict[str, Any]]:
        with self.lock:
            rows = self.db.execute("SELECT * FROM events WHERE task_id=? ORDER BY id DESC LIMIT ?", (task_id, limit)).fetchall()
        result = []
        for row in reversed(rows):
            item = dict(row)
            item["payload"] = loads(item.pop("payload_json"), {})
            result.append(item)
        return result

    def upsert_usage(self, task_id: str, session_id: str | None, run_id: str | None, record: dict[str, Any]) -> dict[str, Any]:
        native_key = str(record.get("native_key") or "unknown")
        basis = str(record.get("basis") or "run")
        scope = str(record.get("scope") or "own")
        values = {key: record.get(key) for key in ("input_tokens", "output_tokens", "reasoning_tokens", "cached_input_tokens", "cache_write_input_tokens", "total_tokens", "context_window", "cost", "currency")}
        rate_limits = scrub(record.get("rate_limits") or {}, self.secrets)
        metadata = scrub(record.get("metadata") or {}, self.secrets)
        timestamp = record.get("observed_at") or now_iso()
        run_key = run_id or (f"session:{session_id}" if session_id else f"task:{task_id}")
        with self._tx() as db:
            db.execute(
                """INSERT INTO usage(task_id,session_id,run_id,run_key,native_key,basis,scope,coverage,input_tokens,output_tokens,reasoning_tokens,cached_input_tokens,cache_write_input_tokens,total_tokens,context_window,rate_limits_json,cost,currency,metadata_json,observed_at)
                   VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                   ON CONFLICT(run_key,native_key,basis,scope) DO UPDATE SET
                     input_tokens=excluded.input_tokens,output_tokens=excluded.output_tokens,reasoning_tokens=excluded.reasoning_tokens,
                     cached_input_tokens=excluded.cached_input_tokens,cache_write_input_tokens=excluded.cache_write_input_tokens,total_tokens=excluded.total_tokens,
                     context_window=excluded.context_window,rate_limits_json=excluded.rate_limits_json,cost=excluded.cost,currency=excluded.currency,metadata_json=excluded.metadata_json,observed_at=excluded.observed_at""",
                (task_id, session_id, run_id, run_key, native_key, basis, scope, str(record.get("coverage") or "session"), values["input_tokens"], values["output_tokens"], values["reasoning_tokens"], values["cached_input_tokens"], values["cache_write_input_tokens"], values["total_tokens"], values["context_window"], dumps(rate_limits), values["cost"], values["currency"], dumps(metadata), timestamp),
            )
            row = db.execute("SELECT * FROM usage WHERE run_key=? AND native_key=? AND basis=? AND scope=?", (run_key, native_key, basis, scope)).fetchone()
        result = dict(row) if row else {}
        result["metadata"] = loads(result.pop("metadata_json", "{}"), {})
        result["rate_limits"] = loads(result.pop("rate_limits_json", "{}"), {})
        return result

    def _descendants(self, task_id: str) -> set[str]:
        with self.lock:
            rows = self.db.execute("SELECT id,parent_task_id FROM tasks").fetchall()
        children: dict[str | None, list[str]] = {}
        for row in rows:
            children.setdefault(row["parent_task_id"], []).append(row["id"])
        result = {task_id}
        pending = [task_id]
        while pending:
            current = pending.pop()
            for child in children.get(current, []):
                if child not in result:
                    result.add(child)
                    pending.append(child)
        return result

    def usage_rollup(self, task_id: str, include_descendants: bool = True) -> dict[str, Any]:
        ids = self._descendants(task_id) if include_descendants else {task_id}
        placeholders = ",".join("?" for _ in ids)
        with self.lock:
            rows = [dict(row) for row in self.db.execute(f"SELECT * FROM usage WHERE task_id IN ({placeholders})", tuple(ids)).fetchall()]
        # A provider may emit cumulative session snapshots once per run and
        # again after resume. Keep the newest snapshot for each native session
        # and scope; per-run records remain additive.
        cumulative_basis = {"cumulative", "session_total", "session_cumulative"}
        selected: list[dict[str, Any]] = []
        latest: dict[tuple[Any, ...], dict[str, Any]] = {}
        cumulative_sessions: set[tuple[Any, ...]] = set()
        for row in rows:
            if row["basis"] in cumulative_basis:
                key = (row["session_id"] or row["task_id"], row["scope"])
                cumulative_sessions.add(key)
                previous = latest.get(key)
                if previous is None or (row["observed_at"], row["id"]) > (previous["observed_at"], previous["id"]):
                    latest[key] = row
            else:
                selected.append(row)
        selected.extend(latest.values())

        # A subtree request must not also add an own-only projection from a
        # provider child.  Conversely, an own request has no right to use a
        # parent subtree snapshot as its own usage.
        if not include_descendants:
            selected = [row for row in selected if row["scope"] == "own"]

        # Prefer the nearest subtree aggregate for each branch. A root
        # aggregate covers all descendants; a child aggregate covers that
        # child's branch when no ancestor aggregate was reported.
        subtree_rows = [row for row in selected if row["scope"] == "subtree"]
        chosen_subtrees: list[dict[str, Any]] = []
        for row in sorted(subtree_rows, key=lambda item: len(self._ancestors(item["task_id"]))):
            if any(
                existing["task_id"] != row["task_id"]
                and row["task_id"] in self._descendants(existing["task_id"])
                and row["session_id"]
                and existing["session_id"]
                and row["session_id"] in self.session_descendants(existing["session_id"])
                for existing in chosen_subtrees
            ):
                continue
            chosen_subtrees.append(row)
        covered: list[tuple[set[str], set[str]]] = []
        for row in chosen_subtrees:
            session_ids = self.session_descendants(row["session_id"]) if row["session_id"] else set()
            if row["session_id"]:
                session_ids.add(row["session_id"])
            covered.append((self._descendants(row["task_id"]), session_ids))
        totals = {key: 0 for key in ("input_tokens", "output_tokens", "reasoning_tokens", "cached_input_tokens", "cache_write_input_tokens", "total_tokens")}
        known = {key: False for key in totals}
        incomplete = False
        for row in selected:
            if row["basis"] not in cumulative_basis and (row["session_id"] or row["task_id"], row["scope"]) in cumulative_sessions:
                continue
            if row["scope"] == "own" and any(row["task_id"] in task_ids and row["session_id"] in session_ids for task_ids, session_ids in covered):
                continue
            if row["scope"] == "subtree" and row not in chosen_subtrees:
                continue
            for key in totals:
                value = row[key]
                if value is not None:
                    totals[key] += int(value)
                    known[key] = True
                else:
                    incomplete = True
        # A linked child session with no usage record is an unknown, not a
        # reported zero.  A declared subtree snapshot covers its descendants,
        # so those children are exempt from this missing-data marker.
        with self.lock:
            session_rows = self.db.execute(f"SELECT id FROM sessions WHERE task_id IN ({placeholders})", tuple(ids)).fetchall()
        observed_sessions = {
            row["session_id"]
            for row in rows
            if row.get("session_id") and (include_descendants or row["scope"] == "own")
        }
        covered_sessions = {session_id for _task_ids, session_ids in covered for session_id in session_ids}
        if any(row["id"] not in observed_sessions and row["id"] not in covered_sessions for row in session_rows):
            incomplete = True
        result = {key: (value if known[key] else None) for key, value in totals.items()}
        result["incomplete"] = incomplete
        return result

    def notes(self, task_id: str) -> list[dict[str, Any]]:
        with self.lock:
            return [dict(row) for row in self.db.execute("SELECT * FROM notes WHERE task_id=? ORDER BY updated_at DESC", (task_id,)).fetchall()]

    def add_note(self, task_id: str, body: str) -> dict[str, Any]:
        if not body.strip() or not self.task(task_id):
            raise ValueError("note requires a task and non-empty body")
        note = (new_id("note_"), task_id, scrub(body.strip(), self.secrets), now_iso(), now_iso())
        with self._tx() as db:
            db.execute("INSERT INTO notes VALUES(?,?,?,?,?)", note)
        return dict(zip(("id", "task_id", "body", "created_at", "updated_at"), note))

    def save_note(self, task_id: str, body: str) -> dict[str, Any]:
        if not self.task(task_id):
            raise ValueError("note requires a task")
        timestamp = now_iso()
        with self._tx() as db:
            row = db.execute("SELECT id,created_at FROM notes WHERE task_id=? ORDER BY created_at LIMIT 1", (task_id,)).fetchone()
            if row:
                db.execute("UPDATE notes SET body=?,updated_at=? WHERE id=?", (scrub(body, self.secrets), timestamp, row["id"]))
                note_id = row["id"]
                created_at = row["created_at"]
            else:
                note_id, created_at = new_id("note_"), timestamp
                db.execute("INSERT INTO notes VALUES(?,?,?,?,?)", (note_id, task_id, scrub(body, self.secrets), created_at, timestamp))
        return {"id": note_id, "task_id": task_id, "body": scrub(body, self.secrets), "created_at": created_at, "updated_at": timestamp}

    def todos(self, task_id: str) -> list[dict[str, Any]]:
        with self.lock:
            return [dict(row) for row in self.db.execute("SELECT * FROM todos WHERE task_id=? ORDER BY position,created_at", (task_id,)).fetchall()]

    def add_todo(self, task_id: str, text: str) -> dict[str, Any]:
        if not text.strip() or not self.task(task_id):
            raise ValueError("todo requires a task and non-empty text")
        with self.lock:
            position = self.db.execute("SELECT COALESCE(MAX(position),-1)+1 FROM todos WHERE task_id=?", (task_id,)).fetchone()[0]
        todo = (new_id("todo_"), task_id, scrub(text.strip(), self.secrets), "", 0, position, now_iso(), now_iso())
        with self._tx() as db:
            db.execute("INSERT INTO todos VALUES(?,?,?,?,?,?,?,?)", todo)
        return dict(zip(("id", "task_id", "text", "note", "done", "position", "created_at", "updated_at"), todo))

    def toggle_todo(self, todo_id: str, done: bool | None = None) -> dict[str, Any]:
        with self.lock:
            row = self.db.execute("SELECT * FROM todos WHERE id=?", (todo_id,)).fetchone()
        if not row:
            raise KeyError("todo not found")
        value = int(not row["done"]) if done is None else int(done)
        with self._tx() as db:
            db.execute("UPDATE todos SET done=?,updated_at=? WHERE id=?", (value, now_iso(), todo_id))
        result = dict(row)
        result.update(done=value)
        return result

    def update_todo(self, todo_id: str, changes: dict[str, Any]) -> dict[str, Any]:
        allowed = {"text", "note", "done"}
        if set(changes) - allowed:
            raise ValueError("unsupported todo fields")
        with self.lock:
            if not self.db.execute("SELECT 1 FROM todos WHERE id=?", (todo_id,)).fetchone():
                raise KeyError("todo not found")
        fields: dict[str, Any] = {}
        for key, value in changes.items():
            if key in {"text", "note"}:
                if not isinstance(value, str):
                    raise ValueError(f"{key} must be a string")
                fields[key] = scrub(value, self.secrets)
            else:
                fields[key] = int(bool(value))
        fields["updated_at"] = now_iso()
        with self._tx() as db:
            db.execute(f"UPDATE todos SET {','.join(f'{key}=?' for key in fields)} WHERE id=?", (*fields.values(), todo_id))
            row = db.execute("SELECT * FROM todos WHERE id=?", (todo_id,)).fetchone()
        return dict(row)

    def delete_todo(self, todo_id: str) -> None:
        with self._tx() as db:
            if not db.execute("SELECT 1 FROM todos WHERE id=?", (todo_id,)).fetchone():
                raise KeyError("todo not found")
            db.execute("DELETE FROM todos WHERE id=?", (todo_id,))

    def artifacts(self, task_id: str) -> list[dict[str, Any]]:
        with self.lock:
            rows = self.db.execute("SELECT * FROM artifacts WHERE task_id=? ORDER BY created_at DESC", (task_id,)).fetchall()
        return [self._decode(dict(row), "metadata") for row in rows]

    def add_artifact(self, task_id: str, kind: str, name: str, content: str | None = None, mime: str | None = None, metadata: dict[str, Any] | None = None, run_id: str | None = None, session_id: str | None = None) -> dict[str, Any]:
        artifact = (new_id("artifact_"), task_id, session_id, run_id, scrub(kind, self.secrets), scrub(name, self.secrets), scrub(mime, self.secrets) if mime else None, scrub(content, self.secrets) if content else None, None, dumps(scrub(metadata or {}, self.secrets)), now_iso())
        with self._tx() as db:
            db.execute("INSERT INTO artifacts VALUES(?,?,?,?,?,?,?,?,?,?,?)", artifact)
        result = dict(zip(("id", "task_id", "session_id", "run_id", "kind", "name", "mime", "content", "path", "metadata_json", "created_at"), artifact))
        return self._decode(result, "metadata")

    def artifact_exists(self, task_id: str, session_id: str | None, run_id: str | None, name: str, content: str | None) -> bool:
        with self.lock:
            row = self.db.execute(
                "SELECT 1 FROM artifacts WHERE task_id=? AND session_id IS ? AND run_id IS ? AND name=? AND content IS ? LIMIT 1",
                (task_id, session_id, run_id, scrub(name, self.secrets), scrub(content, self.secrets) if content is not None else None),
            ).fetchone()
        return row is not None

    def setting(self, key: str, default: Any = None) -> Any:
        with self.lock:
            row = self.db.execute("SELECT value_json FROM settings WHERE key=?", (key,)).fetchone()
        return loads(row[0], default) if row else default

    def set_setting(self, key: str, value: Any) -> Any:
        with self._tx() as db:
            db.execute("INSERT INTO settings(key,value_json,updated_at) VALUES(?,?,?) ON CONFLICT(key) DO UPDATE SET value_json=excluded.value_json,updated_at=excluded.updated_at", (key, dumps(value), now_iso()))
        return value

    def detail(self, task_id: str) -> dict[str, Any]:
        task = self.effective_task(task_id)
        if not task:
            raise KeyError("task not found")
        return {
            "task": task,
            "children": [item for item in self.list_tasks(include_archived=True) if item.get("parent_task_id") == task_id],
            "sessions": self.sessions_for_task(task_id),
            "runs": self.runs_for_task(task_id),
            "events": self.events_for_task(task_id),
            "notes": self.notes(task_id),
            "todos": self.todos(task_id),
            "artifacts": self.artifacts(task_id),
            "usage": {"own": self.usage_rollup(task_id, include_descendants=False), "subtree": self.usage_rollup(task_id, include_descendants=True)},
            "duration": self.duration_rollup(task_id),
        }

    def runs_for_task(self, task_id: str, include_descendants: bool = False) -> list[dict[str, Any]]:
        ids = self._descendants(task_id) if include_descendants else {task_id}
        placeholders = ",".join("?" for _ in ids)
        with self.lock:
            rows = self.db.execute(f"SELECT * FROM runs WHERE task_id IN ({placeholders}) ORDER BY created_at", tuple(ids)).fetchall()
        return [self._decode(dict(row), "requested_options", "effective_options", "metadata") for row in rows]

    @staticmethod
    def _duration_ms(runs: list[dict[str, Any]]) -> int:
        total = 0
        now = datetime.now(timezone.utc)
        for run in runs:
            start_value = run.get("reported_start_at") or run.get("started_at")
            end_value = run.get("reported_end_at") or run.get("ended_at")
            if not start_value:
                continue
            try:
                start = datetime.fromisoformat(start_value)
                end = datetime.fromisoformat(end_value) if end_value else now
                total += max(0, int((end - start).total_seconds() * 1000))
            except (TypeError, ValueError):
                continue
        return total

    def duration_rollup(self, task_id: str) -> dict[str, int]:
        own = self._duration_ms(self.runs_for_task(task_id))
        subtree = self._duration_ms(self.runs_for_task(task_id, include_descendants=True))
        return {"own_ms": own, "subtree_ms": subtree}

    def snapshot(self, search: str = "", include_archived: bool = False) -> dict[str, Any]:
        return {
            "tasks": self.list_tasks(search, include_archived),
            "connections": self.connections(),
            "settings": {
                "theme": self.setting("theme", "basalt"),
                "mode": self.setting("mode", "compact"),
                "sidebar_width": self.setting("sidebar_width", 306),
                "reader_open": self.setting("reader_open", False),
                "rail_open": self.setting("rail_open", True),
                "send_shortcut": self.setting("send_shortcut", "enter"),
                "stay_awake": self.setting("stay_awake", True),
                "menubar": self.setting("menubar", False),
                "default_permission": self.setting("default_permission", "read_only"),
            },
            "active_count": self.active_count(),
        }

    def stats(self) -> dict[str, Any]:
        with self.lock:
            task_counts = self.db.execute("SELECT COUNT(*) AS total, COALESCE(SUM(archived),0) AS archived FROM tasks").fetchone()
            session_count = self.db.execute("SELECT COUNT(*) AS total FROM sessions").fetchone()["total"]
            run_rows = self.db.execute("SELECT state,started_at,ended_at FROM runs").fetchall()
        durations = self._duration_ms([dict(row) for row in run_rows])
        states: dict[str, int] = {}
        for row in run_rows:
            states[row["state"]] = states.get(row["state"], 0) + 1
        return {"tasks": {"total": task_counts["total"], "archived": task_counts["archived"], "active": task_counts["total"] - task_counts["archived"]}, "sessions": {"total": session_count}, "runs": {"total": len(run_rows), "by_state": states}, "duration_ms": durations}

    def active_count(self) -> int:
        with self.lock:
            row = self.db.execute(
                """SELECT COUNT(*) AS total FROM (
                       SELECT session_id AS active_key
                       FROM runs
                       WHERE state IN ('queued','starting','running','waiting','cancelling')
                       UNION
                       SELECT id AS active_key
                       FROM sessions AS s
                       WHERE state IN ('queued','starting','running','waiting','cancelling')
                         AND NOT EXISTS (
                           SELECT 1 FROM runs AS r
                           WHERE r.session_id=s.id
                             AND r.state IN ('queued','starting','running','waiting','cancelling')
                         )
                   )"""
            ).fetchone()
        return int(row["total"])

    def mark_active_runs_interrupted(self) -> list[str]:
        with self._tx() as db:
            rows = db.execute("SELECT id,session_id FROM runs WHERE state IN ('queued','starting','running','waiting','cancelling')").fetchall()
            timestamp = now_iso()
            db.execute("UPDATE runs SET state='interrupted',ended_at=?,cancellation_reason='service_restart',updated_at=? WHERE state IN ('queued','starting','running','waiting','cancelling')", (timestamp, timestamp))
            db.execute("UPDATE sessions SET state='interrupted',ended_at=?,cancellation_reason='service_restart',updated_at=? WHERE state IN ('starting','running','waiting','cancelling')", (timestamp, timestamp))
        return [row["id"] for row in rows]
