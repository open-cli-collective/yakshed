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
use tokio::sync::oneshot;
use yakshed_application::{
    AppStore, ApprovalResolution, Clock, CreateProject, CreateRun, CreateWorkItem, IdGenerator,
    ListTimeline, ListWorkItems, PendingApproval, StoreError, TimelineBatch, TimelinePage,
    WorkItemPage,
};
#[cfg(test)]
use yakshed_domain::ApprovalRequestId;
use yakshed_domain::{
    ApprovalDecision, ApprovalSnapshot, ApprovalStatus, DataRevision, NamespacedProviderId,
    ProjectSnapshot, ProjectionRevision, RunSnapshot, RunStatus, TimelineItemSnapshot,
    UtcTimestamp, WorkItemId, WorkItemSnapshot, WorkItemStatus,
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
    work_item_id TEXT NOT NULL REFERENCES work_items(id) ON DELETE RESTRICT,
    status TEXT NOT NULL CHECK (status = 'running'),
    provider_namespace TEXT,
    provider_run_id TEXT,
    created_at_ms INTEGER NOT NULL,
    CHECK ((provider_namespace IS NULL) = (provider_run_id IS NULL)),
    UNIQUE (provider_namespace, provider_run_id)
);

CREATE TABLE timeline_items (
    id TEXT PRIMARY KEY NOT NULL,
    run_id TEXT NOT NULL REFERENCES runs(id) ON DELETE CASCADE,
    revision INTEGER NOT NULL CHECK (revision > 0),
    kind TEXT NOT NULL CHECK (length(trim(kind)) > 0),
    body TEXT NOT NULL,
    provider_namespace TEXT,
    provider_item_id TEXT,
    created_at_ms INTEGER NOT NULL,
    CHECK ((provider_namespace IS NULL) = (provider_item_id IS NULL)),
    UNIQUE (run_id, revision),
    UNIQUE (provider_namespace, provider_item_id)
);

CREATE TABLE approval_requests (
    id TEXT PRIMARY KEY NOT NULL,
    run_id TEXT NOT NULL REFERENCES runs(id) ON DELETE CASCADE,
    provider_namespace TEXT NOT NULL,
    provider_approval_id TEXT NOT NULL,
    kind TEXT NOT NULL CHECK (length(trim(kind)) > 0),
    summary TEXT NOT NULL,
    status TEXT NOT NULL CHECK (status IN ('pending', 'approved', 'denied')),
    requested_at_ms INTEGER NOT NULL,
    resolved_at_ms INTEGER,
    UNIQUE (provider_namespace, provider_approval_id)
);

CREATE TABLE projection_cursors (
    source_namespace TEXT NOT NULL,
    stream_id TEXT NOT NULL,
    revision INTEGER NOT NULL CHECK (revision >= 0),
    updated_at_ms INTEGER NOT NULL,
    PRIMARY KEY (source_namespace, stream_id)
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
    fail_resolution_after_update: bool,
}

/// Async facade over the dedicated SQLite worker.
pub struct SqliteStore {
    actor: Actor,
    clock: Arc<dyn Clock>,
    ids: Arc<dyn IdGenerator>,
}

impl SqliteStore {
    pub async fn open(
        paths: AppPaths,
        clock: Arc<dyn Clock>,
        ids: Arc<dyn IdGenerator>,
    ) -> Result<Self, StoreError> {
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
                            fail_resolution_after_update: false,
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
            ids,
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
    async fn fail_next_resolution_after_update(&self) -> Result<(), StoreError> {
        self.call(|worker| {
            worker.fail_resolution_after_update = true;
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
    match error.sqlite_error_code() {
        Some(ErrorCode::DatabaseCorrupt | ErrorCode::NotADatabase) => {
            StoreError::Integrity(error.to_string())
        }
        Some(ErrorCode::ConstraintViolation) => StoreError::Integrity(error.to_string()),
        _ => StoreError::Backend(error.to_string()),
    }
}

fn validate_text(field: &str, value: &str) -> Result<(), StoreError> {
    if value.trim().is_empty() {
        Err(StoreError::Conflict(format!("{field} cannot be empty")))
    } else {
        Ok(())
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

#[async_trait]
impl AppStore for SqliteStore {
    async fn create_project(&self, command: CreateProject) -> Result<ProjectSnapshot, StoreError> {
        validate_text("project name", &command.name)?;
        let id = self.ids.next_project_id();
        let created_at = self.clock.now();
        self.call(move |worker| {
            worker
                .connection
                .execute(
                    "INSERT INTO projects (id, name, created_at_ms) VALUES (?1, ?2, ?3)",
                    params![id.to_string(), command.name, created_at.unix_millis()],
                )
                .map_err(map_database_error)?;
            Ok(ProjectSnapshot {
                id,
                name: command.name,
                created_at,
            })
        })
        .await
    }

    async fn create_work_item(
        &self,
        command: CreateWorkItem,
    ) -> Result<WorkItemSnapshot, StoreError> {
        validate_text("work item title", &command.title)?;
        let id = self.ids.next_work_item_id();
        let now = self.clock.now();
        self.call(move |worker| {
            let transaction = worker
                .connection
                .transaction()
                .map_err(map_database_error)?;
            if let Some(parent_id) = command.parent_id {
                let parent_project: Option<String> = transaction
                    .query_row(
                        "SELECT project_id FROM work_items WHERE id = ?1",
                        [parent_id.to_string()],
                        |row| row.get(0),
                    )
                    .optional()
                    .map_err(map_database_error)?;
                match parent_project {
                    None => {
                        return Err(StoreError::Integrity(format!(
                            "parent work item does not exist: {parent_id}"
                        )));
                    }
                    Some(project) if project != command.project_id.to_string() => {
                        return Err(StoreError::Conflict(
                            "parent and child must belong to the same project".to_owned(),
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
                        id.to_string(),
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
                        params![parent_id.to_string(), id.to_string()],
                    )
                    .map_err(map_database_error)?;
            }
            transaction.commit().map_err(map_database_error)?;
            Ok(WorkItemSnapshot {
                id,
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
        let id = self.ids.next_run_id();
        let now = self.clock.now();
        self.call(move |worker| {
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
                     (id, work_item_id, status, provider_namespace, provider_run_id, created_at_ms)
                     VALUES (?1, ?2, 'running', ?3, ?4, ?5)",
                    params![
                        id.to_string(),
                        command.work_item_id.to_string(),
                        namespace,
                        provider_run_id,
                        now.unix_millis()
                    ],
                )
                .map_err(map_database_error)?;
            Ok(RunSnapshot {
                id,
                work_item_id: command.work_item_id,
                status: RunStatus::Running,
                provider_id: command.provider_run,
                created_at: now,
            })
        })
        .await
    }

    async fn append_timeline_batch(
        &self,
        batch: TimelineBatch,
    ) -> Result<ProjectionRevision, StoreError> {
        validate_text("projection source namespace", &batch.source_namespace)?;
        validate_text("projection stream id", &batch.stream_id)?;
        for item in &batch.items {
            validate_text("timeline item kind", &item.kind)?;
        }
        let now = self.clock.now();
        let ids: Vec<_> = batch
            .items
            .iter()
            .map(|_| self.ids.next_timeline_item_id())
            .collect();
        self.call(move |worker| {
            let transaction = worker
                .connection
                .transaction()
                .map_err(map_database_error)?;
            let run_exists = transaction
                .query_row(
                    "SELECT 1 FROM runs WHERE id = ?1",
                    [batch.run_id.to_string()],
                    |_| Ok(()),
                )
                .optional()
                .map_err(map_database_error)?
                .is_some();
            if !run_exists {
                return Err(StoreError::NotFound {
                    entity: "run",
                    id: batch.run_id.to_string(),
                });
            }
            let mut revision: i64 = transaction
                .query_row(
                    "SELECT revision FROM projection_cursors
                     WHERE source_namespace = ?1 AND stream_id = ?2",
                    params![batch.source_namespace, batch.stream_id],
                    |row| row.get(0),
                )
                .optional()
                .map_err(map_database_error)?
                .unwrap_or(0);
            let timeline_revision: i64 = transaction
                .query_row(
                    "SELECT coalesce(max(revision), 0) FROM timeline_items WHERE run_id = ?1",
                    [batch.run_id.to_string()],
                    |row| row.get(0),
                )
                .map_err(map_database_error)?;
            revision = revision.max(timeline_revision);
            for (id, item) in ids.into_iter().zip(batch.items) {
                revision = revision.checked_add(1).ok_or_else(|| {
                    StoreError::Conflict("projection revision overflow".to_owned())
                })?;
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
                         (id, run_id, revision, kind, body, provider_namespace,
                          provider_item_id, created_at_ms)
                         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
                        params![
                            id.to_string(),
                            batch.run_id.to_string(),
                            revision,
                            item.kind,
                            item.body,
                            namespace,
                            provider_item_id,
                            now.unix_millis()
                        ],
                    )
                    .map_err(map_database_error)?;
            }
            transaction
                .execute(
                    "INSERT INTO projection_cursors
                     (source_namespace, stream_id, revision, updated_at_ms)
                     VALUES (?1, ?2, ?3, ?4)
                     ON CONFLICT(source_namespace, stream_id) DO UPDATE SET
                         revision = excluded.revision,
                         updated_at_ms = excluded.updated_at_ms",
                    params![
                        batch.source_namespace,
                        batch.stream_id,
                        revision,
                        now.unix_millis()
                    ],
                )
                .map_err(map_database_error)?;
            transaction.commit().map_err(map_database_error)?;
            Ok(ProjectionRevision::new(
                u64::try_from(revision).map_err(|error| StoreError::Backend(error.to_string()))?,
            ))
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
                    "SELECT id, run_id, revision, kind, body, provider_namespace,
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
                        let revision: i64 = row.get(2)?;
                        Ok(TimelineItemSnapshot {
                            id: parse_column(row, 0)?,
                            run_id: parse_column(row, 1)?,
                            revision: ProjectionRevision::new(u64::try_from(revision).map_err(
                                |error| {
                                    rusqlite::Error::FromSqlConversionFailure(
                                        2,
                                        rusqlite::types::Type::Integer,
                                        Box::new(error),
                                    )
                                },
                            )?),
                            kind: row.get(3)?,
                            body: row.get(4)?,
                            provider_id: provider_id(row.get(5)?, row.get(6)?)?,
                            created_at: UtcTimestamp::from_unix_millis(row.get(7)?),
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
        validate_text("approval kind", &approval.kind)?;
        let id = self.ids.next_approval_request_id();
        let now = self.clock.now();
        self.call(move |worker| {
            worker
                .connection
                .execute(
                    "INSERT INTO approval_requests
                     (id, run_id, provider_namespace, provider_approval_id, kind,
                      summary, status, requested_at_ms)
                     VALUES (?1, ?2, ?3, ?4, ?5, ?6, 'pending', ?7)",
                    params![
                        id.to_string(),
                        approval.run_id.to_string(),
                        approval.provider_id.namespace(),
                        approval.provider_id.value(),
                        approval.kind,
                        approval.summary,
                        now.unix_millis()
                    ],
                )
                .map_err(map_database_error)?;
            Ok(ApprovalSnapshot {
                id,
                run_id: approval.run_id,
                provider_id: approval.provider_id,
                kind: approval.kind,
                summary: approval.summary,
                status: ApprovalStatus::Pending,
                requested_at: now,
                resolved_at: None,
            })
        })
        .await
    }

    async fn resolve_approval(&self, resolution: ApprovalResolution) -> Result<(), StoreError> {
        let now = self.clock.now();
        let audit_id = self.ids.next_audit_event_id();
        self.call(move |worker| {
            let transaction = worker
                .connection
                .transaction()
                .map_err(map_database_error)?;
            let changed = transaction
                .execute(
                    "UPDATE approval_requests
                     SET status = ?2, resolved_at_ms = ?3
                     WHERE id = ?1 AND status = 'pending'",
                    params![
                        resolution.approval_id.to_string(),
                        decision_value(resolution.decision),
                        now.unix_millis()
                    ],
                )
                .map_err(map_database_error)?;
            if changed == 0 {
                let exists = transaction
                    .query_row(
                        "SELECT 1 FROM approval_requests WHERE id = ?1",
                        [resolution.approval_id.to_string()],
                        |_| Ok(()),
                    )
                    .optional()
                    .map_err(map_database_error)?
                    .is_some();
                return Err(if exists {
                    StoreError::Conflict("approval is already resolved".to_owned())
                } else {
                    StoreError::NotFound {
                        entity: "approval request",
                        id: resolution.approval_id.to_string(),
                    }
                });
            }
            #[cfg(test)]
            if std::mem::take(&mut worker.fail_resolution_after_update) {
                return Err(StoreError::Backend(
                    "injected failure after approval update".to_owned(),
                ));
            }
            transaction
                .execute(
                    "INSERT INTO audit_events
                     (id, event_type, entity_type, entity_id, body, created_at_ms)
                     VALUES (?1, 'approval_resolved', 'approval_request', ?2, ?3, ?4)",
                    params![
                        audit_id.to_string(),
                        resolution.approval_id.to_string(),
                        decision_value(resolution.decision),
                        now.unix_millis()
                    ],
                )
                .map_err(map_database_error)?;
            transaction.commit().map_err(map_database_error)
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
    use yakshed_application::SystemIdGenerator;

    struct FixedClock;

    impl Clock for FixedClock {
        fn now(&self) -> UtcTimestamp {
            UtcTimestamp::from_unix_millis(42)
        }
    }

    #[tokio::test]
    async fn resolve_failure_rolls_back_approval_and_audit_together() {
        let temp = tempfile::tempdir().unwrap();
        let store = SqliteStore::open(
            AppPaths::for_test(temp.path()),
            Arc::new(FixedClock),
            Arc::new(SystemIdGenerator),
        )
        .await
        .unwrap();
        let project = store
            .create_project(CreateProject { name: "p".into() })
            .await
            .unwrap();
        let work = store
            .create_work_item(CreateWorkItem {
                project_id: project.id,
                title: "w".into(),
                parent_id: None,
            })
            .await
            .unwrap();
        let run = store
            .create_run(CreateRun {
                work_item_id: work.id,
                provider_run: None,
            })
            .await
            .unwrap();
        let approval = store
            .record_pending_approval(PendingApproval {
                run_id: run.id,
                provider_id: NamespacedProviderId::new("mock", "approval-1").unwrap(),
                kind: "command".into(),
                summary: "test".into(),
            })
            .await
            .unwrap();
        store.fail_next_resolution_after_update().await.unwrap();

        assert!(
            store
                .resolve_approval(ApprovalResolution {
                    approval_id: approval.id,
                    decision: ApprovalDecision::Approved,
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
    }
}
