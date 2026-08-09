//! SQLite-backed canonical durable work state, isolated behind one connection-owning thread.

use std::{
    fs,
    path::Path,
    str::FromStr,
    sync::{Arc, Mutex, mpsc},
    thread,
};

use async_trait::async_trait;
use rusqlite::{Connection, ErrorCode, OptionalExtension, Row, params};
use rusqlite_migration::{M, Migrations};
use sha2::{Digest, Sha256};
use tokio::sync::oneshot;
use yakshed_application::{
    AppStore, ApprovalPage, BeginApprovalResponse, Clock, ConfirmApprovalResponse, CreateProject,
    CreateRun, CreateWorkItem, GetStreamCursor, ListTimeline, ListWorkItems, PendingApproval,
    ProjectPage, RunPage, StoreError, StreamCursorState, TimelineBatch, TimelinePage,
    TransitionRun, WorkItemPage,
};
use yakshed_domain::{
    ApprovalDecision, ApprovalRequestId, ApprovalSnapshot, ApprovalStatus, ConnectionId,
    DataRevision, NamespacedProviderId, ProjectId, ProjectSnapshot, RunId, RunSnapshot, RunStatus,
    StreamCursor, TimelineBatchId, TimelineItemSnapshot, TimelineRevision, UtcTimestamp,
    WorkItemId, WorkItemSnapshot, WorkItemStatus,
};

use crate::AppPaths;

const DATABASE_FILE: &str = "yakshed.sqlite3";
const SCHEMA_VERSION: u32 = 1;
const MAX_PAGE_SIZE: u32 = 200;

const INITIAL_SCHEMA: &str = r#"
CREATE TABLE projects (
    id TEXT PRIMARY KEY NOT NULL,
    name TEXT NOT NULL CHECK (length(trim(name)) > 0),
    created_at_ms INTEGER NOT NULL
);

CREATE TABLE work_items (
    id TEXT PRIMARY KEY NOT NULL,
    project_id TEXT NOT NULL REFERENCES projects(id) ON DELETE RESTRICT,
    title TEXT NOT NULL CHECK (length(trim(title)) > 0),
    status TEXT NOT NULL CHECK (status IN ('ready', 'archived')),
    revision INTEGER NOT NULL CHECK (revision > 0),
    created_at_ms INTEGER NOT NULL,
    updated_at_ms INTEGER NOT NULL,
    archived_at_ms INTEGER
);
CREATE INDEX work_items_project_order ON work_items(project_id, status, id);

CREATE TABLE work_edges (
    parent_id TEXT NOT NULL REFERENCES work_items(id) ON DELETE CASCADE,
    child_id TEXT NOT NULL UNIQUE REFERENCES work_items(id) ON DELETE CASCADE,
    kind TEXT NOT NULL CHECK (kind = 'parent'),
    PRIMARY KEY (parent_id, child_id),
    CHECK (parent_id <> child_id)
);

CREATE TABLE runs (
    id TEXT PRIMARY KEY NOT NULL,
    connection_id TEXT NOT NULL,
    work_item_id TEXT NOT NULL REFERENCES work_items(id) ON DELETE RESTRICT,
    status TEXT NOT NULL CHECK (status IN ('running', 'completed', 'failed', 'interrupted')),
    provider_namespace TEXT,
    provider_run_id TEXT,
    created_at_ms INTEGER NOT NULL,
    ended_at_ms INTEGER,
    CHECK ((provider_namespace IS NULL) = (provider_run_id IS NULL)),
    CHECK ((status = 'running') = (ended_at_ms IS NULL)),
    UNIQUE (connection_id, id),
    UNIQUE (connection_id, provider_namespace, provider_run_id)
);

CREATE TABLE timeline_items (
    id TEXT PRIMARY KEY NOT NULL,
    connection_id TEXT NOT NULL,
    run_id TEXT NOT NULL,
    revision INTEGER NOT NULL CHECK (revision > 0),
    kind TEXT NOT NULL CHECK (length(trim(kind)) > 0),
    body TEXT NOT NULL,
    provider_namespace TEXT,
    provider_item_id TEXT,
    created_at_ms INTEGER NOT NULL,
    CHECK ((provider_namespace IS NULL) = (provider_item_id IS NULL)),
    FOREIGN KEY (connection_id, run_id) REFERENCES runs(connection_id, id) ON DELETE CASCADE,
    UNIQUE (run_id, revision),
    UNIQUE (connection_id, provider_namespace, provider_item_id)
);

CREATE TABLE approval_requests (
    id TEXT PRIMARY KEY NOT NULL,
    connection_id TEXT NOT NULL,
    run_id TEXT NOT NULL,
    provider_namespace TEXT NOT NULL,
    provider_approval_id TEXT NOT NULL,
    kind TEXT NOT NULL CHECK (length(trim(kind)) > 0),
    summary TEXT NOT NULL,
    status TEXT NOT NULL CHECK (status IN ('pending', 'responding', 'resolved', 'voided')),
    decision TEXT CHECK (decision IN ('approved', 'denied')),
    requested_at_ms INTEGER NOT NULL,
    response_started_at_ms INTEGER,
    resolved_at_ms INTEGER,
    voided_at_ms INTEGER,
    FOREIGN KEY (connection_id, run_id) REFERENCES runs(connection_id, id) ON DELETE CASCADE,
    UNIQUE (connection_id, provider_namespace, provider_approval_id),
    CHECK (
        (status = 'pending' AND decision IS NULL AND response_started_at_ms IS NULL AND resolved_at_ms IS NULL AND voided_at_ms IS NULL)
        OR (status = 'responding' AND decision IS NOT NULL AND response_started_at_ms IS NOT NULL AND resolved_at_ms IS NULL AND voided_at_ms IS NULL)
        OR (status = 'resolved' AND decision IS NOT NULL AND response_started_at_ms IS NOT NULL AND resolved_at_ms IS NOT NULL AND voided_at_ms IS NULL)
        OR (status = 'voided' AND resolved_at_ms IS NULL AND voided_at_ms IS NOT NULL
            AND ((decision IS NULL AND response_started_at_ms IS NULL)
                 OR (decision IS NOT NULL AND response_started_at_ms IS NOT NULL)))
    )
);

CREATE TABLE projection_cursors (
    connection_id TEXT NOT NULL,
    source_namespace TEXT NOT NULL,
    stream_id TEXT NOT NULL,
    run_id TEXT NOT NULL,
    cursor INTEGER NOT NULL CHECK (cursor >= 0),
    last_batch_id TEXT NOT NULL,
    last_start_cursor INTEGER NOT NULL CHECK (last_start_cursor >= 0),
    last_payload_digest TEXT NOT NULL,
    updated_at_ms INTEGER NOT NULL,
    FOREIGN KEY (connection_id, run_id) REFERENCES runs(connection_id, id) ON DELETE CASCADE,
    PRIMARY KEY (connection_id, source_namespace, stream_id)
);

CREATE TABLE audit_events (
    id TEXT PRIMARY KEY NOT NULL,
    event_type TEXT NOT NULL,
    entity_type TEXT NOT NULL,
    entity_id TEXT NOT NULL,
    body TEXT NOT NULL,
    created_at_ms INTEGER NOT NULL
);
"#;

type Job = Box<dyn FnOnce(&mut Worker) + Send + 'static>;

enum Message {
    Job(Job),
    Shutdown(oneshot::Sender<()>),
}

struct Actor {
    sender: Mutex<Option<mpsc::Sender<Message>>>,
    thread: Mutex<Option<thread::JoinHandle<()>>>,
}

struct Worker {
    connection: Connection,
    #[cfg(test)]
    fail_transition_after_update: bool,
}

/// Async facade over the dedicated SQLite worker.
pub struct SqliteStore {
    actor: Actor,
    clock: Arc<dyn Clock>,
}

impl SqliteStore {
    pub async fn open(paths: AppPaths, clock: Arc<dyn Clock>) -> Result<Self, StoreError> {
        let (sender, receiver) = mpsc::channel();
        let (ready_sender, ready_receiver) = oneshot::channel();
        let thread = thread::Builder::new()
            .name("yakshed-sqlite".to_owned())
            .spawn(move || match open_connection(&paths) {
                Ok(connection) => {
                    let _ = ready_sender.send(Ok(()));
                    run_worker(
                        Worker {
                            connection,
                            #[cfg(test)]
                            fail_transition_after_update: false,
                        },
                        receiver,
                    );
                }
                Err(error) => {
                    let _ = ready_sender.send(Err(error));
                }
            })
            .map_err(|error| StoreError::Open(error.to_string()))?;

        ready_receiver.await.map_err(|_| {
            StoreError::Open("database worker stopped during initialization".to_owned())
        })??;
        Ok(Self {
            actor: Actor {
                sender: Mutex::new(Some(sender)),
                thread: Mutex::new(Some(thread)),
            },
            clock,
        })
    }

    async fn call<T>(
        &self,
        operation: impl FnOnce(&mut Worker) -> Result<T, StoreError> + Send + 'static,
    ) -> Result<T, StoreError>
    where
        T: Send + 'static,
    {
        let (result_sender, result_receiver) = oneshot::channel();
        self.actor
            .sender
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .as_ref()
            .ok_or(StoreError::Closed)?
            .send(Message::Job(Box::new(move |worker| {
                let _ = result_sender.send(operation(worker));
            })))
            .map_err(|_| StoreError::Closed)?;
        result_receiver.await.map_err(|_| StoreError::Closed)?
    }

    #[cfg(test)]
    async fn fail_next_transition_after_update(&self) -> Result<(), StoreError> {
        self.call(|worker| {
            worker.fail_transition_after_update = true;
            Ok(())
        })
        .await
    }

    #[cfg(test)]
    async fn approval_status_and_audit_count(
        &self,
        id: ApprovalRequestId,
    ) -> Result<(String, u64), StoreError> {
        self.call(move |worker| {
            let status = worker
                .connection
                .query_row(
                    "SELECT status FROM approval_requests WHERE id = ?1",
                    [id.to_string()],
                    |row| row.get(0),
                )
                .map_err(map_database_error)?;
            let count: i64 = worker
                .connection
                .query_row("SELECT count(*) FROM audit_events", [], |row| row.get(0))
                .map_err(map_database_error)?;
            Ok((status, u64::try_from(count).unwrap()))
        })
        .await
    }

    #[cfg(test)]
    async fn run_and_approval_statuses(
        &self,
        run_id: RunId,
        approval_id: ApprovalRequestId,
    ) -> Result<(String, String, u64), StoreError> {
        self.call(move |worker| {
            let run_status = worker
                .connection
                .query_row(
                    "SELECT status FROM runs WHERE id = ?1",
                    [run_id.to_string()],
                    |row| row.get(0),
                )
                .map_err(map_database_error)?;
            let approval_status = worker
                .connection
                .query_row(
                    "SELECT status FROM approval_requests WHERE id = ?1",
                    [approval_id.to_string()],
                    |row| row.get(0),
                )
                .map_err(map_database_error)?;
            let count: i64 = worker
                .connection
                .query_row("SELECT count(*) FROM audit_events", [], |row| row.get(0))
                .map_err(map_database_error)?;
            Ok((run_status, approval_status, u64::try_from(count).unwrap()))
        })
        .await
    }
}

fn run_worker(mut worker: Worker, receiver: mpsc::Receiver<Message>) {
    while let Ok(message) = receiver.recv() {
        match message {
            Message::Job(job) => job(&mut worker),
            Message::Shutdown(done) => {
                drop(worker);
                let _ = done.send(());
                return;
            }
        }
    }
}

fn migrations() -> Migrations<'static> {
    Migrations::new(vec![
        M::up(INITIAL_SCHEMA).comment("initial durable work state"),
    ])
}

fn open_connection(paths: &AppPaths) -> Result<Connection, StoreError> {
    paths
        .create_data_root()
        .map_err(|error| StoreError::Open(error.to_string()))?;
    let database = paths.data_root.join(DATABASE_FILE);
    let existed = database.exists();
    let mut connection =
        Connection::open(&database).map_err(|error| classify_open(&database, error))?;
    if !existed {
        set_private_file_permissions(&database)?;
    }
    let version = connection
        .pragma_query_value(None, "user_version", |row| row.get::<_, u32>(0))
        .map_err(classify_integrity_or_open)?;
    if version > SCHEMA_VERSION {
        return Err(StoreError::UnsupportedNewerSchema {
            found: version,
            supported: SCHEMA_VERSION,
        });
    }

    connection
        .pragma_update(None, "foreign_keys", true)
        .map_err(classify_integrity_or_open)?;
    connection
        .pragma_update(None, "journal_mode", "WAL")
        .map_err(classify_integrity_or_open)?;
    connection
        .pragma_update(None, "synchronous", "FULL")
        .map_err(classify_integrity_or_open)?;
    connection
        .busy_timeout(std::time::Duration::from_millis(5_000))
        .map_err(classify_integrity_or_open)?;
    let integrity: String = connection
        .query_row("PRAGMA quick_check(1)", [], |row| row.get(0))
        .map_err(classify_integrity_or_open)?;
    if integrity != "ok" {
        return Err(StoreError::Integrity(integrity));
    }
    migrations()
        .to_latest(&mut connection)
        .map_err(|error| StoreError::Migration(error.to_string()))?;
    Ok(connection)
}

#[cfg(unix)]
fn set_private_file_permissions(path: &Path) -> Result<(), StoreError> {
    use std::os::unix::fs::PermissionsExt;

    fs::set_permissions(path, fs::Permissions::from_mode(0o600))
        .map_err(|error| StoreError::Open(format!("{}: {error}", path.display())))
}

#[cfg(not(unix))]
fn set_private_file_permissions(_path: &Path) -> Result<(), StoreError> {
    Ok(())
}

fn classify_open(path: &Path, error: rusqlite::Error) -> StoreError {
    if is_corruption(&error) {
        StoreError::Integrity(format!("{}: {error}", path.display()))
    } else {
        StoreError::Open(format!("{}: {error}", path.display()))
    }
}

fn classify_integrity_or_open(error: rusqlite::Error) -> StoreError {
    if is_corruption(&error) {
        StoreError::Integrity(error.to_string())
    } else {
        StoreError::Open(error.to_string())
    }
}

fn is_corruption(error: &rusqlite::Error) -> bool {
    error
        .sqlite_error_code()
        .is_some_and(|code| matches!(code, ErrorCode::DatabaseCorrupt | ErrorCode::NotADatabase))
}

fn map_database_error(error: rusqlite::Error) -> StoreError {
    if is_corruption(&error) {
        StoreError::Integrity(error.to_string())
    } else if error.sqlite_error_code() == Some(ErrorCode::ConstraintViolation) {
        StoreError::Conflict(error.to_string())
    } else if matches!(
        error,
        rusqlite::Error::FromSqlConversionFailure(..)
            | rusqlite::Error::IntegralValueOutOfRange(..)
            | rusqlite::Error::InvalidColumnType(..)
            | rusqlite::Error::InvalidQuery
    ) {
        StoreError::Integrity(error.to_string())
    } else {
        StoreError::Backend(error.to_string())
    }
}

fn validate_text(field: &str, value: &str) -> Result<(), StoreError> {
    if value.trim().is_empty() {
        Err(StoreError::Conflict(format!("{field} cannot be empty")))
    } else {
        Ok(())
    }
}

fn validate_app_id(field: &str, is_v7: bool) -> Result<(), StoreError> {
    if is_v7 {
        Ok(())
    } else {
        Err(StoreError::Conflict(format!("{field} must be a UUIDv7")))
    }
}

fn validate_page_size(limit: u32) -> Result<usize, StoreError> {
    if (1..=MAX_PAGE_SIZE).contains(&limit) {
        Ok(limit as usize)
    } else {
        Err(StoreError::Conflict(format!(
            "page size must be between 1 and {MAX_PAGE_SIZE}"
        )))
    }
}

fn parse_column<T>(row: &Row<'_>, column: usize) -> rusqlite::Result<T>
where
    T: FromStr,
    T::Err: std::error::Error + Send + Sync + 'static,
{
    let value: String = row.get(column)?;
    value.parse().map_err(|error| {
        rusqlite::Error::FromSqlConversionFailure(
            column,
            rusqlite::types::Type::Text,
            Box::new(error),
        )
    })
}

fn provider_id(
    namespace: Option<String>,
    value: Option<String>,
) -> rusqlite::Result<Option<NamespacedProviderId>> {
    match (namespace, value) {
        (None, None) => Ok(None),
        (Some(namespace), Some(value)) => NamespacedProviderId::new(namespace, value)
            .map(Some)
            .map_err(|error| {
                rusqlite::Error::FromSqlConversionFailure(
                    0,
                    rusqlite::types::Type::Text,
                    Box::new(error),
                )
            }),
        _ => Err(rusqlite::Error::InvalidQuery),
    }
}

const WORK_ITEM_SELECT: &str = "
SELECT w.id, w.project_id, w.title, w.status,
       (SELECT e.parent_id FROM work_edges e WHERE e.child_id = w.id),
       w.revision, w.created_at_ms, w.updated_at_ms
FROM work_items w";

fn work_item_from_row(row: &Row<'_>) -> rusqlite::Result<WorkItemSnapshot> {
    let status: String = row.get(3)?;
    Ok(WorkItemSnapshot {
        id: parse_column(row, 0)?,
        project_id: parse_column(row, 1)?,
        title: row.get(2)?,
        status: match status.as_str() {
            "ready" => WorkItemStatus::Ready,
            "archived" => WorkItemStatus::Archived,
            _ => return Err(rusqlite::Error::InvalidQuery),
        },
        parent_id: row
            .get::<_, Option<String>>(4)?
            .map(|value| value.parse())
            .transpose()
            .map_err(|error| {
                rusqlite::Error::FromSqlConversionFailure(
                    4,
                    rusqlite::types::Type::Text,
                    Box::new(error),
                )
            })?,
        revision: DataRevision::new(u64::try_from(row.get::<_, i64>(5)?).map_err(|error| {
            rusqlite::Error::FromSqlConversionFailure(
                5,
                rusqlite::types::Type::Integer,
                Box::new(error),
            )
        })?),
        created_at: UtcTimestamp::from_unix_millis(row.get(6)?),
        updated_at: UtcTimestamp::from_unix_millis(row.get(7)?),
    })
}

fn get_work_item(connection: &Connection, id: WorkItemId) -> Result<WorkItemSnapshot, StoreError> {
    connection
        .query_row(
            &format!("{WORK_ITEM_SELECT} WHERE w.id = ?1"),
            [id.to_string()],
            work_item_from_row,
        )
        .optional()
        .map_err(map_database_error)?
        .ok_or_else(|| StoreError::NotFound {
            entity: "work item",
            id: id.to_string(),
        })
}

fn decision_value(decision: ApprovalDecision) -> &'static str {
    match decision {
        ApprovalDecision::Approved => "approved",
        ApprovalDecision::Denied => "denied",
    }
}

fn decision_from_value(value: &str) -> rusqlite::Result<ApprovalDecision> {
    match value {
        "approved" => Ok(ApprovalDecision::Approved),
        "denied" => Ok(ApprovalDecision::Denied),
        _ => Err(rusqlite::Error::InvalidQuery),
    }
}

fn project_from_row(row: &Row<'_>) -> rusqlite::Result<ProjectSnapshot> {
    Ok(ProjectSnapshot {
        id: parse_column(row, 0)?,
        name: row.get(1)?,
        created_at: UtcTimestamp::from_unix_millis(row.get(2)?),
    })
}

const RUN_SELECT: &str = "
SELECT id, connection_id, work_item_id, status, provider_namespace,
       provider_run_id, created_at_ms, ended_at_ms
FROM runs";

fn run_from_row(row: &Row<'_>) -> rusqlite::Result<RunSnapshot> {
    Ok(RunSnapshot {
        id: parse_column(row, 0)?,
        connection_id: parse_column(row, 1)?,
        work_item_id: parse_column(row, 2)?,
        status: run_status_from_value(&row.get::<_, String>(3)?)?,
        provider_id: provider_id(row.get(4)?, row.get(5)?)?,
        created_at: UtcTimestamp::from_unix_millis(row.get(6)?),
        ended_at: row
            .get::<_, Option<i64>>(7)?
            .map(UtcTimestamp::from_unix_millis),
    })
}

fn get_run(connection: &Connection, id: RunId) -> Result<RunSnapshot, StoreError> {
    connection
        .query_row(
            &format!("{RUN_SELECT} WHERE id = ?1"),
            [id.to_string()],
            run_from_row,
        )
        .optional()
        .map_err(map_database_error)?
        .ok_or_else(|| StoreError::NotFound {
            entity: "run",
            id: id.to_string(),
        })
}

fn run_status_value(status: RunStatus) -> &'static str {
    match status {
        RunStatus::Running => "running",
        RunStatus::Completed => "completed",
        RunStatus::Failed => "failed",
        RunStatus::Interrupted => "interrupted",
    }
}

fn run_status_from_value(value: &str) -> rusqlite::Result<RunStatus> {
    match value {
        "running" => Ok(RunStatus::Running),
        "completed" => Ok(RunStatus::Completed),
        "failed" => Ok(RunStatus::Failed),
        "interrupted" => Ok(RunStatus::Interrupted),
        _ => Err(rusqlite::Error::InvalidQuery),
    }
}

const APPROVAL_SELECT: &str = "
SELECT id, connection_id, run_id, provider_namespace, provider_approval_id, kind, summary,
       status, decision, requested_at_ms, response_started_at_ms, resolved_at_ms, voided_at_ms
FROM approval_requests";

fn approval_from_row(row: &Row<'_>) -> rusqlite::Result<ApprovalSnapshot> {
    let status: String = row.get(7)?;
    let decision = row
        .get::<_, Option<String>>(8)?
        .map(|value| decision_from_value(&value))
        .transpose()?;
    let status = match (status.as_str(), decision) {
        ("pending", None) => ApprovalStatus::Pending,
        ("responding", Some(decision)) => ApprovalStatus::Responding { decision },
        ("resolved", Some(decision)) => ApprovalStatus::Resolved { decision },
        ("voided", decision) => ApprovalStatus::Voided { decision },
        _ => return Err(rusqlite::Error::InvalidQuery),
    };
    Ok(ApprovalSnapshot {
        id: parse_column(row, 0)?,
        connection_id: parse_column(row, 1)?,
        run_id: parse_column(row, 2)?,
        provider_id: NamespacedProviderId::new(row.get::<_, String>(3)?, row.get::<_, String>(4)?)
            .map_err(|error| {
                rusqlite::Error::FromSqlConversionFailure(
                    3,
                    rusqlite::types::Type::Text,
                    Box::new(error),
                )
            })?,
        kind: row.get(5)?,
        summary: row.get(6)?,
        status,
        requested_at: UtcTimestamp::from_unix_millis(row.get(9)?),
        response_started_at: row
            .get::<_, Option<i64>>(10)?
            .map(UtcTimestamp::from_unix_millis),
        resolved_at: row
            .get::<_, Option<i64>>(11)?
            .map(UtcTimestamp::from_unix_millis),
        voided_at: row
            .get::<_, Option<i64>>(12)?
            .map(UtcTimestamp::from_unix_millis),
    })
}

fn get_approval(
    connection: &Connection,
    id: ApprovalRequestId,
) -> Result<ApprovalSnapshot, StoreError> {
    connection
        .query_row(
            &format!("{APPROVAL_SELECT} WHERE id = ?1"),
            [id.to_string()],
            approval_from_row,
        )
        .optional()
        .map_err(map_database_error)?
        .ok_or_else(|| StoreError::NotFound {
            entity: "approval request",
            id: id.to_string(),
        })
}

fn list_approval_page(
    connection: &Connection,
    status: &'static str,
    after: Option<ApprovalRequestId>,
    limit: usize,
    fetch_limit: i64,
) -> Result<ApprovalPage, StoreError> {
    let sql = format!(
        "{APPROVAL_SELECT} WHERE status = ?1 AND (?2 IS NULL OR id > ?2) ORDER BY id LIMIT ?3"
    );
    let mut statement = connection.prepare(&sql).map_err(map_database_error)?;
    let after = after.map(|id| id.to_string());
    let mut items = statement
        .query_map(params![status, after, fetch_limit], approval_from_row)
        .map_err(map_database_error)?
        .collect::<Result<Vec<_>, _>>()
        .map_err(map_database_error)?;
    let has_more = items.len() > limit;
    items.truncate(limit);
    let next_after = has_more.then(|| items.last().map(|item| item.id)).flatten();
    Ok(ApprovalPage { items, next_after })
}

struct PersistedStreamCursor {
    connection_id: ConnectionId,
    run_id: RunId,
    cursor: StreamCursor,
    last_batch_id: TimelineBatchId,
    last_start_cursor: StreamCursor,
    last_payload_digest: String,
}

fn find_stream_cursor(
    connection: &Connection,
    connection_id: ConnectionId,
    source_namespace: &str,
    stream_id: &str,
) -> Result<Option<PersistedStreamCursor>, StoreError> {
    connection
        .query_row(
            "SELECT connection_id, run_id, cursor, last_batch_id, last_start_cursor,
                    last_payload_digest
             FROM projection_cursors
             WHERE connection_id = ?1 AND source_namespace = ?2 AND stream_id = ?3",
            params![connection_id.to_string(), source_namespace, stream_id],
            |row| {
                let cursor = u64::try_from(row.get::<_, i64>(2)?).map_err(|error| {
                    rusqlite::Error::FromSqlConversionFailure(
                        2,
                        rusqlite::types::Type::Integer,
                        Box::new(error),
                    )
                })?;
                let last_batch_id: TimelineBatchId = parse_column(row, 3)?;
                if !last_batch_id.is_v7() {
                    return Err(rusqlite::Error::InvalidQuery);
                }
                let last_start_cursor = u64::try_from(row.get::<_, i64>(4)?).map_err(|error| {
                    rusqlite::Error::FromSqlConversionFailure(
                        4,
                        rusqlite::types::Type::Integer,
                        Box::new(error),
                    )
                })?;
                Ok(PersistedStreamCursor {
                    connection_id: parse_column(row, 0)?,
                    run_id: parse_column(row, 1)?,
                    cursor: StreamCursor::new(cursor),
                    last_batch_id,
                    last_start_cursor: StreamCursor::new(last_start_cursor),
                    last_payload_digest: row.get(5)?,
                })
            },
        )
        .optional()
        .map_err(map_database_error)
}

/// SHA-256 over `yakshed.timeline-batch.v1\0`, the item count, then each ordered
/// item's UUID, kind, body, and optional provider namespace/value. Integers and
/// byte-string lengths are unsigned little-endian u64 values.
fn timeline_payload_digest(items: &[yakshed_application::NewTimelineItem]) -> String {
    fn add_bytes(hash: &mut Sha256, bytes: &[u8]) {
        hash.update((bytes.len() as u64).to_le_bytes());
        hash.update(bytes);
    }

    let mut hash = Sha256::new();
    hash.update(b"yakshed.timeline-batch.v1\0");
    hash.update((items.len() as u64).to_le_bytes());
    for item in items {
        add_bytes(&mut hash, item.id.to_string().as_bytes());
        add_bytes(&mut hash, item.kind.as_bytes());
        add_bytes(&mut hash, item.body.as_bytes());
        match &item.provider_id {
            Some(provider) => {
                hash.update([1]);
                add_bytes(&mut hash, provider.namespace().as_bytes());
                add_bytes(&mut hash, provider.value().as_bytes());
            }
            None => hash.update([0]),
        }
    }
    let mut encoded = String::with_capacity(64);
    for byte in hash.finalize() {
        use std::fmt::Write as _;
        write!(encoded, "{byte:02x}").expect("writing to a String cannot fail");
    }
    encoded
}

#[async_trait]
impl AppStore for SqliteStore {
    async fn create_project(&self, command: CreateProject) -> Result<ProjectSnapshot, StoreError> {
        validate_app_id("project id", command.id.is_v7())?;
        validate_text("project name", &command.name)?;
        let created_at = self.clock.now();
        self.call(move |worker| {
            let existing = worker
                .connection
                .query_row(
                    "SELECT id, name, created_at_ms FROM projects WHERE id = ?1",
                    [command.id.to_string()],
                    project_from_row,
                )
                .optional()
                .map_err(map_database_error)?;
            if let Some(existing) = existing {
                return if existing.name == command.name {
                    Ok(existing)
                } else {
                    Err(StoreError::Conflict(format!(
                        "project id already exists with different content: {}",
                        command.id
                    )))
                };
            }
            worker
                .connection
                .execute(
                    "INSERT INTO projects (id, name, created_at_ms) VALUES (?1, ?2, ?3)",
                    params![
                        command.id.to_string(),
                        command.name,
                        created_at.unix_millis()
                    ],
                )
                .map_err(map_database_error)?;
            Ok(ProjectSnapshot {
                id: command.id,
                name: command.name,
                created_at,
            })
        })
        .await
    }

    async fn list_projects(
        &self,
        after: Option<ProjectId>,
        limit: u32,
    ) -> Result<ProjectPage, StoreError> {
        let limit = validate_page_size(limit)?;
        let fetch_limit =
            i64::try_from(limit + 1).map_err(|error| StoreError::Conflict(error.to_string()))?;
        self.call(move |worker| {
            let after = after.map(|id| id.to_string());
            let mut statement = worker
                .connection
                .prepare(
                    "SELECT id, name, created_at_ms FROM projects
                     WHERE (?1 IS NULL OR id > ?1) ORDER BY id LIMIT ?2",
                )
                .map_err(map_database_error)?;
            let mut items = statement
                .query_map(params![after, fetch_limit], project_from_row)
                .map_err(map_database_error)?
                .collect::<Result<Vec<_>, _>>()
                .map_err(map_database_error)?;
            let has_more = items.len() > limit;
            items.truncate(limit);
            let next_after = has_more.then(|| items.last().map(|item| item.id)).flatten();
            Ok(ProjectPage { items, next_after })
        })
        .await
    }

    async fn create_work_item(
        &self,
        command: CreateWorkItem,
    ) -> Result<WorkItemSnapshot, StoreError> {
        validate_app_id("work item id", command.id.is_v7())?;
        validate_text("work item title", &command.title)?;
        let now = self.clock.now();
        self.call(move |worker| {
            let transaction = worker
                .connection
                .transaction()
                .map_err(map_database_error)?;
            let existing = transaction
                .query_row(
                    &format!("{WORK_ITEM_SELECT} WHERE w.id = ?1"),
                    [command.id.to_string()],
                    work_item_from_row,
                )
                .optional()
                .map_err(map_database_error)?;
            if let Some(existing) = existing {
                return if existing.project_id == command.project_id
                    && existing.title == command.title
                    && existing.parent_id == command.parent_id
                {
                    Ok(existing)
                } else {
                    Err(StoreError::Conflict(format!(
                        "work item id already exists with different content: {}",
                        command.id
                    )))
                };
            }
            let project_exists = transaction
                .query_row(
                    "SELECT 1 FROM projects WHERE id = ?1",
                    [command.project_id.to_string()],
                    |_| Ok(()),
                )
                .optional()
                .map_err(map_database_error)?
                .is_some();
            if !project_exists {
                return Err(StoreError::NotFound {
                    entity: "project",
                    id: command.project_id.to_string(),
                });
            }
            if let Some(parent_id) = command.parent_id {
                let parent: Option<(String, String)> = transaction
                    .query_row(
                        "SELECT project_id, status FROM work_items WHERE id = ?1",
                        [parent_id.to_string()],
                        |row| Ok((row.get(0)?, row.get(1)?)),
                    )
                    .optional()
                    .map_err(map_database_error)?;
                match parent {
                    None => {
                        return Err(StoreError::NotFound {
                            entity: "parent work item",
                            id: parent_id.to_string(),
                        });
                    }
                    Some((project, _)) if project != command.project_id.to_string() => {
                        return Err(StoreError::Conflict(
                            "parent and child must belong to the same project".to_owned(),
                        ));
                    }
                    Some((_, status)) if status == "archived" => {
                        return Err(StoreError::Conflict(
                            "cannot attach an active work item to an archived parent".to_owned(),
                        ));
                    }
                    Some(_) => {}
                }
            }
            transaction
                .execute(
                    "INSERT INTO work_items
                     (id, project_id, title, status, revision, created_at_ms, updated_at_ms)
                     VALUES (?1, ?2, ?3, 'ready', 1, ?4, ?4)",
                    params![
                        command.id.to_string(),
                        command.project_id.to_string(),
                        command.title,
                        now.unix_millis()
                    ],
                )
                .map_err(map_database_error)?;
            if let Some(parent_id) = command.parent_id {
                transaction
                    .execute(
                        "INSERT INTO work_edges (parent_id, child_id, kind)
                         VALUES (?1, ?2, 'parent')",
                        params![parent_id.to_string(), command.id.to_string()],
                    )
                    .map_err(map_database_error)?;
            }
            transaction.commit().map_err(map_database_error)?;
            Ok(WorkItemSnapshot {
                id: command.id,
                project_id: command.project_id,
                title: command.title,
                status: WorkItemStatus::Ready,
                parent_id: command.parent_id,
                revision: DataRevision::new(1),
                created_at: now,
                updated_at: now,
            })
        })
        .await
    }

    async fn get_work_item(&self, id: WorkItemId) -> Result<WorkItemSnapshot, StoreError> {
        self.call(move |worker| get_work_item(&worker.connection, id))
            .await
    }

    async fn list_work_items(&self, query: ListWorkItems) -> Result<WorkItemPage, StoreError> {
        let limit = validate_page_size(query.limit)?;
        let fetch_limit =
            i64::try_from(limit + 1).map_err(|error| StoreError::Conflict(error.to_string()))?;
        self.call(move |worker| {
            let sql = format!(
                "{WORK_ITEM_SELECT}
                 WHERE w.project_id = ?1
                   AND (?2 OR w.status <> 'archived')
                   AND (?3 IS NULL OR w.id > ?3)
                 ORDER BY w.id
                 LIMIT ?4"
            );
            let mut statement = worker
                .connection
                .prepare(&sql)
                .map_err(map_database_error)?;
            let after = query.after.map(|id| id.to_string());
            let mut items = statement
                .query_map(
                    params![
                        query.project_id.to_string(),
                        query.include_archived,
                        after,
                        fetch_limit
                    ],
                    work_item_from_row,
                )
                .map_err(map_database_error)?
                .collect::<Result<Vec<_>, _>>()
                .map_err(map_database_error)?;
            let has_more = items.len() > limit;
            items.truncate(limit);
            let next_after = if has_more {
                items.last().map(|item| item.id)
            } else {
                None
            };
            Ok(WorkItemPage { items, next_after })
        })
        .await
    }

    async fn archive_work_subtree(&self, root: WorkItemId) -> Result<u64, StoreError> {
        let now = self.clock.now();
        self.call(move |worker| {
            let transaction = worker
                .connection
                .transaction()
                .map_err(map_database_error)?;
            let exists = transaction
                .query_row(
                    "SELECT 1 FROM work_items WHERE id = ?1",
                    [root.to_string()],
                    |_| Ok(()),
                )
                .optional()
                .map_err(map_database_error)?
                .is_some();
            if !exists {
                return Err(StoreError::NotFound {
                    entity: "work item",
                    id: root.to_string(),
                });
            }
            let changed = transaction
                .execute(
                    "WITH RECURSIVE subtree(id) AS (
                         SELECT ?1
                         UNION ALL
                         SELECT e.child_id FROM work_edges e JOIN subtree s ON e.parent_id = s.id
                     )
                     UPDATE work_items
                     SET status = 'archived', revision = revision + 1,
                         archived_at_ms = ?2, updated_at_ms = ?2
                     WHERE id IN (SELECT id FROM subtree) AND status <> 'archived'",
                    params![root.to_string(), now.unix_millis()],
                )
                .map_err(map_database_error)?;
            transaction.commit().map_err(map_database_error)?;
            u64::try_from(changed).map_err(|error| StoreError::Backend(error.to_string()))
        })
        .await
    }

    async fn create_run(&self, command: CreateRun) -> Result<RunSnapshot, StoreError> {
        validate_app_id("run id", command.id.is_v7())?;
        let now = self.clock.now();
        self.call(move |worker| {
            let existing = worker
                .connection
                .query_row(
                    &format!("{RUN_SELECT} WHERE id = ?1"),
                    [command.id.to_string()],
                    run_from_row,
                )
                .optional()
                .map_err(map_database_error)?;
            if let Some(existing) = existing {
                return if existing.work_item_id == command.work_item_id
                    && existing.connection_id == command.connection_id
                    && existing.provider_id == command.provider_run
                {
                    Ok(existing)
                } else {
                    Err(StoreError::Conflict(format!(
                        "run id already exists with different content: {}",
                        command.id
                    )))
                };
            }
            let work_exists = worker
                .connection
                .query_row(
                    "SELECT 1 FROM work_items WHERE id = ?1",
                    [command.work_item_id.to_string()],
                    |_| Ok(()),
                )
                .optional()
                .map_err(map_database_error)?
                .is_some();
            if !work_exists {
                return Err(StoreError::NotFound {
                    entity: "work item",
                    id: command.work_item_id.to_string(),
                });
            }
            let (namespace, provider_run_id) =
                command
                    .provider_run
                    .as_ref()
                    .map_or((None, None), |provider| {
                        (
                            Some(provider.namespace().to_owned()),
                            Some(provider.value().to_owned()),
                        )
                    });
            worker
                .connection
                .execute(
                    "INSERT INTO runs
                     (id, connection_id, work_item_id, status, provider_namespace,
                      provider_run_id, created_at_ms)
                     VALUES (?1, ?2, ?3, 'running', ?4, ?5, ?6)",
                    params![
                        command.id.to_string(),
                        command.connection_id.to_string(),
                        command.work_item_id.to_string(),
                        namespace,
                        provider_run_id,
                        now.unix_millis()
                    ],
                )
                .map_err(map_database_error)?;
            Ok(RunSnapshot {
                id: command.id,
                connection_id: command.connection_id,
                work_item_id: command.work_item_id,
                status: RunStatus::Running,
                provider_id: command.provider_run,
                created_at: now,
                ended_at: None,
            })
        })
        .await
    }

    async fn get_run(&self, id: RunId) -> Result<RunSnapshot, StoreError> {
        self.call(move |worker| get_run(&worker.connection, id))
            .await
    }

    async fn transition_run(&self, command: TransitionRun) -> Result<RunSnapshot, StoreError> {
        validate_app_id("audit event id", command.audit_event_id.is_v7())?;
        if !command.expected_current.can_transition_to(command.target) {
            return Err(StoreError::Conflict(
                "illegal run state transition".to_owned(),
            ));
        }
        self.call(move |worker| {
            let transaction = worker
                .connection
                .transaction()
                .map_err(map_database_error)?;
            let changed = transaction
                .execute(
                    "UPDATE runs SET status = ?2, ended_at_ms = ?3
                     WHERE id = ?1 AND status = ?4",
                    params![
                        command.run_id.to_string(),
                        run_status_value(command.target),
                        command.occurred_at.unix_millis(),
                        run_status_value(command.expected_current)
                    ],
                )
                .map_err(map_database_error)?;
            if changed == 0 {
                let current = transaction
                    .query_row(
                        "SELECT status FROM runs WHERE id = ?1",
                        [command.run_id.to_string()],
                        |row| row.get::<_, String>(0),
                    )
                    .optional()
                    .map_err(map_database_error)?;
                return Err(match current {
                    Some(current) => StoreError::Conflict(format!(
                        "run state conflict: expected {}, current {current}",
                        run_status_value(command.expected_current)
                    )),
                    None => StoreError::NotFound {
                        entity: "run",
                        id: command.run_id.to_string(),
                    },
                });
            }
            let voided = transaction
                .execute(
                    "UPDATE approval_requests
                     SET status = 'voided', voided_at_ms = ?2
                     WHERE run_id = ?1 AND status IN ('pending', 'responding')",
                    params![
                        command.run_id.to_string(),
                        command.occurred_at.unix_millis()
                    ],
                )
                .map_err(map_database_error)?;
            #[cfg(test)]
            if std::mem::take(&mut worker.fail_transition_after_update) {
                return Err(StoreError::Backend(
                    "injected failure after run transition".to_owned(),
                ));
            }
            transaction
                .execute(
                    "INSERT INTO audit_events
                     (id, event_type, entity_type, entity_id, body, created_at_ms)
                     VALUES (?1, 'run_transitioned', 'run', ?2, ?3, ?4)",
                    params![
                        command.audit_event_id.to_string(),
                        command.run_id.to_string(),
                        format!(
                            "{}->{};voided_approvals={voided}",
                            run_status_value(command.expected_current),
                            run_status_value(command.target)
                        ),
                        command.occurred_at.unix_millis()
                    ],
                )
                .map_err(map_database_error)?;
            let snapshot = transaction
                .query_row(
                    &format!("{RUN_SELECT} WHERE id = ?1"),
                    [command.run_id.to_string()],
                    run_from_row,
                )
                .map_err(map_database_error)?;
            transaction.commit().map_err(map_database_error)?;
            Ok(snapshot)
        })
        .await
    }

    async fn list_runs_for_work_item(
        &self,
        work_item_id: WorkItemId,
        after: Option<RunId>,
        limit: u32,
    ) -> Result<RunPage, StoreError> {
        let limit = validate_page_size(limit)?;
        let fetch_limit =
            i64::try_from(limit + 1).map_err(|error| StoreError::Conflict(error.to_string()))?;
        self.call(move |worker| {
            let sql = format!(
                "{RUN_SELECT}
                 WHERE work_item_id = ?1 AND (?2 IS NULL OR id > ?2)
                 ORDER BY id LIMIT ?3"
            );
            let mut statement = worker
                .connection
                .prepare(&sql)
                .map_err(map_database_error)?;
            let after = after.map(|id| id.to_string());
            let mut items = statement
                .query_map(
                    params![work_item_id.to_string(), after, fetch_limit],
                    run_from_row,
                )
                .map_err(map_database_error)?
                .collect::<Result<Vec<_>, _>>()
                .map_err(map_database_error)?;
            let has_more = items.len() > limit;
            items.truncate(limit);
            let next_after = has_more.then(|| items.last().map(|item| item.id)).flatten();
            Ok(RunPage { items, next_after })
        })
        .await
    }

    async fn list_active_runs(
        &self,
        after: Option<RunId>,
        limit: u32,
    ) -> Result<RunPage, StoreError> {
        let limit = validate_page_size(limit)?;
        let fetch_limit =
            i64::try_from(limit + 1).map_err(|error| StoreError::Conflict(error.to_string()))?;
        self.call(move |worker| {
            let after = after.map(|id| id.to_string());
            let mut statement = worker
                .connection
                .prepare(&format!(
                    "{RUN_SELECT}
                     WHERE status = 'running' AND (?1 IS NULL OR id > ?1)
                     ORDER BY id LIMIT ?2"
                ))
                .map_err(map_database_error)?;
            let mut items = statement
                .query_map(params![after, fetch_limit], run_from_row)
                .map_err(map_database_error)?
                .collect::<Result<Vec<_>, _>>()
                .map_err(map_database_error)?;
            let has_more = items.len() > limit;
            items.truncate(limit);
            let next_after = has_more.then(|| items.last().map(|item| item.id)).flatten();
            Ok(RunPage { items, next_after })
        })
        .await
    }

    async fn append_timeline_batch(
        &self,
        batch: TimelineBatch,
    ) -> Result<StreamCursor, StoreError> {
        validate_app_id("timeline batch id", batch.batch_id.is_v7())?;
        validate_text("projection source namespace", &batch.source_namespace)?;
        validate_text("projection stream id", &batch.stream_id)?;
        for item in &batch.items {
            validate_app_id("timeline item id", item.id.is_v7())?;
            validate_text("timeline item kind", &item.kind)?;
        }
        let payload_digest = timeline_payload_digest(&batch.items);
        let now = self.clock.now();
        self.call(move |worker| {
            let transaction = worker
                .connection
                .transaction()
                .map_err(map_database_error)?;
            let run_connection = transaction
                .query_row(
                    "SELECT connection_id FROM runs WHERE id = ?1",
                    [batch.run_id.to_string()],
                    |row| parse_column::<ConnectionId>(row, 0),
                )
                .optional()
                .map_err(map_database_error)?;
            let Some(run_connection) = run_connection else {
                return Err(StoreError::NotFound {
                    entity: "run",
                    id: batch.run_id.to_string(),
                });
            };
            if run_connection != batch.connection_id {
                return Err(StoreError::Conflict(
                    "run belongs to a different connection".to_owned(),
                ));
            }
            let expected = i64::try_from(batch.expected_stream_revision.get())
                .map_err(|error| StoreError::Conflict(error.to_string()))?;
            let cursor = find_stream_cursor(
                &transaction,
                batch.connection_id,
                &batch.source_namespace,
                &batch.stream_id,
            )?;
            match cursor {
                Some(cursor)
                    if cursor.connection_id != batch.connection_id
                        || cursor.run_id != batch.run_id =>
                {
                    return Err(StoreError::Conflict(
                        "projection stream is already bound to a different run".to_owned(),
                    ));
                }
                Some(cursor)
                    if cursor.last_batch_id == batch.batch_id
                        && cursor.last_start_cursor == batch.expected_stream_revision
                        && cursor.last_payload_digest == payload_digest =>
                {
                    return Ok(cursor.cursor);
                }
                Some(cursor) if cursor.last_batch_id == batch.batch_id => {
                    return Err(StoreError::Conflict(
                        "timeline batch retry does not match its committed receipt".to_owned(),
                    ));
                }
                Some(cursor) if cursor.cursor.get() != batch.expected_stream_revision.get() => {
                    return Err(StoreError::Conflict(format!(
                        "projection cursor conflict: expected {expected}, current {}",
                        cursor.cursor.get()
                    )));
                }
                None if expected != 0 => {
                    return Err(StoreError::Conflict(format!(
                        "projection cursor conflict: expected {expected}, current 0"
                    )));
                }
                Some(_) | None => {}
            }
            let item_count = i64::try_from(batch.items.len())
                .map_err(|error| StoreError::Conflict(error.to_string()))?;
            let mut timeline_revision: i64 = transaction
                .query_row(
                    "SELECT coalesce(max(revision), 0) FROM timeline_items WHERE run_id = ?1",
                    [batch.run_id.to_string()],
                    |row| row.get(0),
                )
                .map_err(map_database_error)?;
            for item in batch.items {
                timeline_revision = timeline_revision
                    .checked_add(1)
                    .ok_or_else(|| StoreError::Conflict("timeline revision overflow".to_owned()))?;
                let (namespace, provider_item_id) =
                    item.provider_id.as_ref().map_or((None, None), |provider| {
                        (
                            Some(provider.namespace().to_owned()),
                            Some(provider.value().to_owned()),
                        )
                    });
                transaction
                    .execute(
                        "INSERT INTO timeline_items
                         (id, connection_id, run_id, revision, kind, body, provider_namespace,
                          provider_item_id, created_at_ms)
                         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)",
                        params![
                            item.id.to_string(),
                            batch.connection_id.to_string(),
                            batch.run_id.to_string(),
                            timeline_revision,
                            item.kind,
                            item.body,
                            namespace,
                            provider_item_id,
                            now.unix_millis()
                        ],
                    )
                    .map_err(map_database_error)?;
            }
            let stream_revision = expected
                .checked_add(item_count)
                .ok_or_else(|| StoreError::Conflict("projection cursor overflow".to_owned()))?;
            transaction
                .execute(
                    "INSERT INTO projection_cursors
                     (connection_id, source_namespace, stream_id, run_id, cursor, last_batch_id,
                      last_start_cursor, last_payload_digest, updated_at_ms)
                     VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)
                     ON CONFLICT(connection_id, source_namespace, stream_id) DO UPDATE SET
                         cursor = excluded.cursor,
                         last_batch_id = excluded.last_batch_id,
                         last_start_cursor = excluded.last_start_cursor,
                         last_payload_digest = excluded.last_payload_digest,
                         updated_at_ms = excluded.updated_at_ms",
                    params![
                        batch.connection_id.to_string(),
                        batch.source_namespace,
                        batch.stream_id,
                        batch.run_id.to_string(),
                        stream_revision,
                        batch.batch_id.to_string(),
                        expected,
                        payload_digest,
                        now.unix_millis()
                    ],
                )
                .map_err(map_database_error)?;
            transaction.commit().map_err(map_database_error)?;
            Ok(StreamCursor::new(
                u64::try_from(stream_revision)
                    .map_err(|error| StoreError::Backend(error.to_string()))?,
            ))
        })
        .await
    }

    async fn get_stream_cursor(
        &self,
        query: GetStreamCursor,
    ) -> Result<Option<StreamCursorState>, StoreError> {
        validate_text("projection source namespace", &query.source_namespace)?;
        validate_text("projection stream id", &query.stream_id)?;
        self.call(move |worker| {
            let cursor = find_stream_cursor(
                &worker.connection,
                query.connection_id,
                &query.source_namespace,
                &query.stream_id,
            )?;
            match cursor {
                Some(cursor)
                    if cursor.connection_id != query.connection_id
                        || cursor.run_id != query.run_id =>
                {
                    Err(StoreError::Conflict(
                        "projection stream is bound to a different run".to_owned(),
                    ))
                }
                Some(cursor) => Ok(Some(StreamCursorState {
                    cursor: cursor.cursor,
                    last_batch_id: cursor.last_batch_id,
                })),
                None => Ok(None),
            }
        })
        .await
    }

    async fn list_timeline_page(&self, query: ListTimeline) -> Result<TimelinePage, StoreError> {
        let limit = validate_page_size(query.limit)?;
        let fetch_limit =
            i64::try_from(limit + 1).map_err(|error| StoreError::Conflict(error.to_string()))?;
        self.call(move |worker| {
            let after = query
                .after
                .map(|revision| i64::try_from(revision.get()))
                .transpose()
                .map_err(|error| StoreError::Conflict(error.to_string()))?;
            let mut statement = worker
                .connection
                .prepare(
                    "SELECT id, connection_id, run_id, revision, kind, body, provider_namespace,
                            provider_item_id, created_at_ms
                     FROM timeline_items
                     WHERE run_id = ?1 AND (?2 IS NULL OR revision > ?2)
                     ORDER BY revision
                     LIMIT ?3",
                )
                .map_err(map_database_error)?;
            let mut items = statement
                .query_map(
                    params![query.run_id.to_string(), after, fetch_limit],
                    |row| {
                        let revision: i64 = row.get(3)?;
                        Ok(TimelineItemSnapshot {
                            id: parse_column(row, 0)?,
                            connection_id: parse_column(row, 1)?,
                            run_id: parse_column(row, 2)?,
                            revision: TimelineRevision::new(u64::try_from(revision).map_err(
                                |error| {
                                    rusqlite::Error::FromSqlConversionFailure(
                                        3,
                                        rusqlite::types::Type::Integer,
                                        Box::new(error),
                                    )
                                },
                            )?),
                            kind: row.get(4)?,
                            body: row.get(5)?,
                            provider_id: provider_id(row.get(6)?, row.get(7)?)?,
                            created_at: UtcTimestamp::from_unix_millis(row.get(8)?),
                        })
                    },
                )
                .map_err(map_database_error)?
                .collect::<Result<Vec<_>, _>>()
                .map_err(map_database_error)?;
            let has_more = items.len() > limit;
            items.truncate(limit);
            let next_after = if has_more {
                items.last().map(|item| item.revision)
            } else {
                None
            };
            Ok(TimelinePage { items, next_after })
        })
        .await
    }

    async fn record_pending_approval(
        &self,
        approval: PendingApproval,
    ) -> Result<ApprovalSnapshot, StoreError> {
        validate_app_id("approval request id", approval.id.is_v7())?;
        validate_text("approval kind", &approval.kind)?;
        let now = self.clock.now();
        self.call(move |worker| {
            let transaction = worker
                .connection
                .transaction()
                .map_err(map_database_error)?;
            let run = transaction
                .query_row(
                    "SELECT connection_id, status FROM runs WHERE id = ?1",
                    [approval.run_id.to_string()],
                    |row| {
                        Ok((
                            parse_column::<ConnectionId>(row, 0)?,
                            row.get::<_, String>(1)?,
                        ))
                    },
                )
                .optional()
                .map_err(map_database_error)?;
            let Some((connection_id, run_status)) = run else {
                return Err(StoreError::NotFound {
                    entity: "run",
                    id: approval.run_id.to_string(),
                });
            };
            if run_status != "running" {
                return Err(StoreError::Conflict(
                    "cannot record an approval for a terminal run".to_owned(),
                ));
            }
            let existing = transaction
                .query_row(
                    &format!("{APPROVAL_SELECT} WHERE id = ?1"),
                    [approval.id.to_string()],
                    approval_from_row,
                )
                .optional()
                .map_err(map_database_error)?;
            if let Some(existing) = existing {
                return if existing.run_id == approval.run_id
                    && existing.provider_id == approval.provider_id
                    && existing.kind == approval.kind
                    && existing.summary == approval.summary
                {
                    Ok(existing)
                } else {
                    Err(StoreError::Conflict(format!(
                        "approval id already exists with different content: {}",
                        approval.id
                    )))
                };
            }
            transaction
                .execute(
                    "INSERT INTO approval_requests
                     (id, connection_id, run_id, provider_namespace, provider_approval_id, kind,
                      summary, status, requested_at_ms)
                     VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, 'pending', ?8)",
                    params![
                        approval.id.to_string(),
                        connection_id.to_string(),
                        approval.run_id.to_string(),
                        approval.provider_id.namespace(),
                        approval.provider_id.value(),
                        approval.kind,
                        approval.summary,
                        now.unix_millis()
                    ],
                )
                .map_err(map_database_error)?;
            let snapshot = ApprovalSnapshot {
                id: approval.id,
                connection_id,
                run_id: approval.run_id,
                provider_id: approval.provider_id,
                kind: approval.kind,
                summary: approval.summary,
                status: ApprovalStatus::Pending,
                requested_at: now,
                response_started_at: None,
                resolved_at: None,
                voided_at: None,
            };
            transaction.commit().map_err(map_database_error)?;
            Ok(snapshot)
        })
        .await
    }

    async fn list_pending_approvals(
        &self,
        after: Option<ApprovalRequestId>,
        limit: u32,
    ) -> Result<ApprovalPage, StoreError> {
        let limit = validate_page_size(limit)?;
        let fetch_limit =
            i64::try_from(limit + 1).map_err(|error| StoreError::Conflict(error.to_string()))?;
        self.call(move |worker| {
            list_approval_page(&worker.connection, "pending", after, limit, fetch_limit)
        })
        .await
    }

    async fn list_approvals_for_run(
        &self,
        run_id: RunId,
        after: Option<ApprovalRequestId>,
        limit: u32,
    ) -> Result<ApprovalPage, StoreError> {
        let limit = validate_page_size(limit)?;
        let fetch_limit =
            i64::try_from(limit + 1).map_err(|error| StoreError::Conflict(error.to_string()))?;
        self.call(move |worker| {
            let sql = format!(
                "{APPROVAL_SELECT}
                 WHERE run_id = ?1 AND (?2 IS NULL OR id > ?2)
                 ORDER BY id LIMIT ?3"
            );
            let mut statement = worker
                .connection
                .prepare(&sql)
                .map_err(map_database_error)?;
            let after = after.map(|id| id.to_string());
            let mut items = statement
                .query_map(
                    params![run_id.to_string(), after, fetch_limit],
                    approval_from_row,
                )
                .map_err(map_database_error)?
                .collect::<Result<Vec<_>, _>>()
                .map_err(map_database_error)?;
            let has_more = items.len() > limit;
            items.truncate(limit);
            let next_after = has_more.then(|| items.last().map(|item| item.id)).flatten();
            Ok(ApprovalPage { items, next_after })
        })
        .await
    }

    async fn begin_approval_response(
        &self,
        response: BeginApprovalResponse,
    ) -> Result<ApprovalSnapshot, StoreError> {
        validate_app_id("audit event id", response.audit_event_id.is_v7())?;
        let now = self.clock.now();
        self.call(move |worker| {
            let transaction = worker
                .connection
                .transaction()
                .map_err(map_database_error)?;
            let changed = transaction
                .execute(
                    "UPDATE approval_requests
                     SET status = 'responding', decision = ?2, response_started_at_ms = ?3
                     WHERE id = ?1 AND status = 'pending'",
                    params![
                        response.approval_id.to_string(),
                        decision_value(response.decision),
                        now.unix_millis()
                    ],
                )
                .map_err(map_database_error)?;
            if changed == 0 {
                let exists = transaction
                    .query_row(
                        "SELECT 1 FROM approval_requests WHERE id = ?1",
                        [response.approval_id.to_string()],
                        |_| Ok(()),
                    )
                    .optional()
                    .map_err(map_database_error)?
                    .is_some();
                return Err(if exists {
                    StoreError::Conflict("approval response has already begun".to_owned())
                } else {
                    StoreError::NotFound {
                        entity: "approval request",
                        id: response.approval_id.to_string(),
                    }
                });
            }
            #[cfg(test)]
            if std::mem::take(&mut worker.fail_transition_after_update) {
                return Err(StoreError::Backend(
                    "injected failure after approval update".to_owned(),
                ));
            }
            transaction
                .execute(
                    "INSERT INTO audit_events
                     (id, event_type, entity_type, entity_id, body, created_at_ms)
                     VALUES (?1, 'approval_response_begun', 'approval_request', ?2, ?3, ?4)",
                    params![
                        response.audit_event_id.to_string(),
                        response.approval_id.to_string(),
                        decision_value(response.decision),
                        now.unix_millis()
                    ],
                )
                .map_err(map_database_error)?;
            let snapshot = get_approval(&transaction, response.approval_id)?;
            transaction.commit().map_err(map_database_error)?;
            Ok(snapshot)
        })
        .await
    }

    async fn confirm_approval_response(
        &self,
        response: ConfirmApprovalResponse,
    ) -> Result<ApprovalSnapshot, StoreError> {
        validate_app_id("audit event id", response.audit_event_id.is_v7())?;
        let now = self.clock.now();
        self.call(move |worker| {
            let transaction = worker
                .connection
                .transaction()
                .map_err(map_database_error)?;
            let changed = transaction
                .execute(
                    "UPDATE approval_requests
                     SET status = 'resolved', resolved_at_ms = ?2
                     WHERE id = ?1 AND status = 'responding'",
                    params![response.approval_id.to_string(), now.unix_millis()],
                )
                .map_err(map_database_error)?;
            if changed == 0 {
                let exists = transaction
                    .query_row(
                        "SELECT 1 FROM approval_requests WHERE id = ?1",
                        [response.approval_id.to_string()],
                        |_| Ok(()),
                    )
                    .optional()
                    .map_err(map_database_error)?
                    .is_some();
                return Err(if exists {
                    StoreError::Conflict(
                        "approval response is not awaiting confirmation".to_owned(),
                    )
                } else {
                    StoreError::NotFound {
                        entity: "approval request",
                        id: response.approval_id.to_string(),
                    }
                });
            }
            #[cfg(test)]
            if std::mem::take(&mut worker.fail_transition_after_update) {
                return Err(StoreError::Backend(
                    "injected failure after approval update".to_owned(),
                ));
            }
            let snapshot = get_approval(&transaction, response.approval_id)?;
            let decision = match snapshot.status {
                ApprovalStatus::Resolved { decision } => decision,
                _ => {
                    return Err(StoreError::Integrity(
                        "invalid approval transition".to_owned(),
                    ));
                }
            };
            transaction
                .execute(
                    "INSERT INTO audit_events
                     (id, event_type, entity_type, entity_id, body, created_at_ms)
                     VALUES (?1, 'approval_response_confirmed', 'approval_request', ?2, ?3, ?4)",
                    params![
                        response.audit_event_id.to_string(),
                        response.approval_id.to_string(),
                        decision_value(decision),
                        now.unix_millis()
                    ],
                )
                .map_err(map_database_error)?;
            transaction.commit().map_err(map_database_error)?;
            Ok(snapshot)
        })
        .await
    }

    async fn list_unconfirmed_approval_responses(
        &self,
        after: Option<ApprovalRequestId>,
        limit: u32,
    ) -> Result<ApprovalPage, StoreError> {
        let limit = validate_page_size(limit)?;
        let fetch_limit =
            i64::try_from(limit + 1).map_err(|error| StoreError::Conflict(error.to_string()))?;
        self.call(move |worker| {
            list_approval_page(&worker.connection, "responding", after, limit, fetch_limit)
        })
        .await
    }

    async fn shutdown(&self) -> Result<(), StoreError> {
        let (done_sender, done_receiver) = oneshot::channel();
        self.actor
            .sender
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take()
            .ok_or(StoreError::Closed)?
            .send(Message::Shutdown(done_sender))
            .map_err(|_| StoreError::Closed)?;
        done_receiver.await.map_err(|_| StoreError::Closed)?;
        let thread = self
            .actor
            .thread
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take();
        if let Some(thread) = thread {
            tokio::task::spawn_blocking(move || thread.join())
                .await
                .map_err(|error| StoreError::Backend(error.to_string()))?
                .map_err(|_| StoreError::Backend("database worker panicked".to_owned()))?;
        }
        Ok(())
    }
}

impl Drop for SqliteStore {
    fn drop(&mut self) {
        if let Ok(sender) = self.actor.sender.get_mut()
            && let Some(sender) = sender.take()
        {
            let (done, _) = oneshot::channel();
            let _ = sender.send(Message::Shutdown(done));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use yakshed_application::{IdGenerator, SystemIdGenerator};

    struct FixedClock;

    impl Clock for FixedClock {
        fn now(&self) -> UtcTimestamp {
            UtcTimestamp::from_unix_millis(42)
        }
    }

    #[tokio::test]
    async fn approval_and_run_transitions_commit_audits_or_roll_back_together() {
        let temp = tempfile::tempdir().unwrap();
        let ids = SystemIdGenerator;
        let store = SqliteStore::open(AppPaths::for_test(temp.path()), Arc::new(FixedClock))
            .await
            .unwrap();
        let project = store
            .create_project(CreateProject {
                id: ids.next_project_id(),
                name: "p".into(),
            })
            .await
            .unwrap();
        let work = store
            .create_work_item(CreateWorkItem {
                id: ids.next_work_item_id(),
                project_id: project.id,
                title: "w".into(),
                parent_id: None,
            })
            .await
            .unwrap();
        let run = store
            .create_run(CreateRun {
                id: ids.next_run_id(),
                connection_id: "0193f26e-7a72-7d42-bf77-0de14c4cc222".parse().unwrap(),
                work_item_id: work.id,
                provider_run: None,
            })
            .await
            .unwrap();
        let approval = store
            .record_pending_approval(PendingApproval {
                id: ids.next_approval_request_id(),
                run_id: run.id,
                provider_id: NamespacedProviderId::new("mock", "approval-1").unwrap(),
                kind: "command".into(),
                summary: "test".into(),
            })
            .await
            .unwrap();
        store.fail_next_transition_after_update().await.unwrap();

        assert!(
            store
                .begin_approval_response(BeginApprovalResponse {
                    approval_id: approval.id,
                    decision: ApprovalDecision::Approved,
                    audit_event_id: ids.next_audit_event_id(),
                })
                .await
                .is_err()
        );
        assert_eq!(
            store
                .approval_status_and_audit_count(approval.id)
                .await
                .unwrap(),
            ("pending".to_owned(), 0)
        );
        store
            .begin_approval_response(BeginApprovalResponse {
                approval_id: approval.id,
                decision: ApprovalDecision::Approved,
                audit_event_id: ids.next_audit_event_id(),
            })
            .await
            .unwrap();
        assert_eq!(
            store
                .approval_status_and_audit_count(approval.id)
                .await
                .unwrap(),
            ("responding".to_owned(), 1)
        );

        store.fail_next_transition_after_update().await.unwrap();
        assert!(
            store
                .confirm_approval_response(ConfirmApprovalResponse {
                    approval_id: approval.id,
                    audit_event_id: ids.next_audit_event_id(),
                })
                .await
                .is_err()
        );
        assert_eq!(
            store
                .approval_status_and_audit_count(approval.id)
                .await
                .unwrap(),
            ("responding".to_owned(), 1)
        );
        store
            .confirm_approval_response(ConfirmApprovalResponse {
                approval_id: approval.id,
                audit_event_id: ids.next_audit_event_id(),
            })
            .await
            .unwrap();
        assert_eq!(
            store
                .approval_status_and_audit_count(approval.id)
                .await
                .unwrap(),
            ("resolved".to_owned(), 2)
        );

        let pending = store
            .record_pending_approval(PendingApproval {
                id: ids.next_approval_request_id(),
                run_id: run.id,
                provider_id: NamespacedProviderId::new("mock", "approval-2").unwrap(),
                kind: "command".into(),
                summary: "still pending".into(),
            })
            .await
            .unwrap();
        store.fail_next_transition_after_update().await.unwrap();
        assert!(
            store
                .transition_run(TransitionRun {
                    run_id: run.id,
                    expected_current: RunStatus::Running,
                    target: RunStatus::Failed,
                    occurred_at: UtcTimestamp::from_unix_millis(43),
                    audit_event_id: ids.next_audit_event_id(),
                })
                .await
                .is_err()
        );
        assert_eq!(
            store
                .run_and_approval_statuses(run.id, pending.id)
                .await
                .unwrap(),
            ("running".to_owned(), "pending".to_owned(), 2)
        );
        store
            .transition_run(TransitionRun {
                run_id: run.id,
                expected_current: RunStatus::Running,
                target: RunStatus::Failed,
                occurred_at: UtcTimestamp::from_unix_millis(43),
                audit_event_id: ids.next_audit_event_id(),
            })
            .await
            .unwrap();
        assert_eq!(
            store
                .run_and_approval_statuses(run.id, pending.id)
                .await
                .unwrap(),
            ("failed".to_owned(), "voided".to_owned(), 3)
        );
    }
}
