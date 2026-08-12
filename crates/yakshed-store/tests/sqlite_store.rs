use std::{fs, sync::Arc};

use tempfile::TempDir;
use yakshed_application::{
    AppStore, BeginApprovalResponse, Clock, ConfigChange, ConfigRevision, ConfirmApprovalResponse,
    CreateProject, CreateRun, CreateWorkItem, GetStreamCursor, IdGenerator, ListTimeline,
    ListWorkItems, NewTimelineItem, PendingApproval, StoreError, TimelineBatch, TransitionRun,
};
use yakshed_domain::{
    ApprovalDecision, ApprovalRequestId, ApprovalStatus, ArtifactId, ArtifactKind,
    ArtifactProvenance, ArtifactRecord, AuditEventId, ConnectionId, ContentDigest,
    NamespacedProviderId, ProjectId, ProviderRunIdentity, RunId, RunSnapshot, RunStatus,
    StreamCursor, TimelineBatchId, TimelineItemId, UtcTimestamp, WorkItemId, WorkItemStatus,
};
use yakshed_store::{AppPaths, ConfigStore, SqliteStore};

struct TestIds(std::sync::atomic::AtomicU64);

impl TestIds {
    fn new() -> Self {
        Self(std::sync::atomic::AtomicU64::new(1))
    }

    fn next_uuid(&self) -> uuid::Uuid {
        let value = self.0.fetch_add(1, std::sync::atomic::Ordering::SeqCst) as u128;
        uuid::Uuid::from_u128(0x0193_f26e_7a72_7000_8000_0000_0000_0000 + value)
    }
}

impl IdGenerator for TestIds {
    fn next_project_id(&self) -> ProjectId {
        self.next_uuid().try_into().unwrap()
    }

    fn next_work_item_id(&self) -> WorkItemId {
        self.next_uuid().try_into().unwrap()
    }

    fn next_run_id(&self) -> RunId {
        self.next_uuid().try_into().unwrap()
    }

    fn next_timeline_item_id(&self) -> TimelineItemId {
        self.next_uuid().try_into().unwrap()
    }

    fn next_timeline_batch_id(&self) -> TimelineBatchId {
        self.next_uuid().try_into().unwrap()
    }

    fn next_approval_request_id(&self) -> ApprovalRequestId {
        self.next_uuid().try_into().unwrap()
    }

    fn next_audit_event_id(&self) -> AuditEventId {
        self.next_uuid().try_into().unwrap()
    }
}

struct FixedClock;

impl Clock for FixedClock {
    fn now(&self) -> UtcTimestamp {
        UtcTimestamp::from_unix_millis(1_735_689_600_123)
    }
}

struct Context {
    _temp: TempDir,
    paths: AppPaths,
    ids: Arc<TestIds>,
    store: Arc<SqliteStore>,
}

impl Context {
    async fn open() -> Self {
        let temp = tempfile::tempdir().unwrap();
        let paths = AppPaths::for_test(temp.path());
        let ids = Arc::new(TestIds::new());
        let store = SqliteStore::open(paths.clone(), Arc::new(FixedClock), ids.clone())
            .await
            .unwrap();
        Self {
            _temp: temp,
            paths,
            ids,
            store: Arc::new(store),
        }
    }

    async fn project(&self) -> ProjectId {
        self.store
            .create_project(CreateProject {
                id: self.ids.next_project_id(),
                name: "YakShed".into(),
            })
            .await
            .unwrap()
            .id
    }
}

fn missing_work_item() -> WorkItemId {
    "0193f26e-7a72-7000-8000-00000000ffff".parse().unwrap()
}

fn connection_a() -> ConnectionId {
    "0193f26e-7a72-7000-8000-00000000aaa1".parse().unwrap()
}

fn connection_b() -> ConnectionId {
    "0193f26e-7a72-7000-8000-00000000bbb2".parse().unwrap()
}

fn provider_run(value: impl Into<String>) -> ProviderRunIdentity {
    ProviderRunIdentity::new("mock", "runtime", "session", value).unwrap()
}

async fn accept_run(store: &SqliteStore, ids: &TestIds, run: RunSnapshot) -> RunSnapshot {
    let provider_id = run
        .provider_id
        .clone()
        .or_else(|| Some(provider_run(format!("run/{}", run.id))));
    store
        .transition_run(TransitionRun {
            run_id: run.id,
            expected_current: RunStatus::Starting,
            target: RunStatus::Running,
            provider_id,
            occurred_at: UtcTimestamp::from_unix_millis(1),
            audit_event_id: ids.next_audit_event_id(),
        })
        .await
        .unwrap()
}

#[tokio::test]
async fn empty_database_migrates_to_v5_before_ready() {
    let context = Context::open().await;
    let database = context.paths.data_root.join("yakshed.sqlite3");
    context.store.shutdown().await.unwrap();

    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        assert_eq!(
            fs::metadata(&database).unwrap().permissions().mode() & 0o777,
            0o600
        );
    }

    let connection = rusqlite::Connection::open(database).unwrap();
    assert_eq!(
        connection
            .pragma_query_value(None, "user_version", |row| row.get::<_, u32>(0))
            .unwrap(),
        5
    );
    for table in [
        "projects",
        "work_items",
        "work_edges",
        "runs",
        "timeline_items",
        "approval_requests",
        "projection_cursors",
        "audit_events",
        "artifacts",
    ] {
        assert!(
            connection
                .query_row(
                    "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?1",
                    [table],
                    |_| Ok(()),
                )
                .is_ok(),
            "missing {table}"
        );
    }
}

#[tokio::test]
async fn v2_migration_preserves_run_children() {
    let context = Context::open().await;
    let project_id = context.project().await;
    let work = context
        .store
        .create_work_item(CreateWorkItem {
            id: context.ids.next_work_item_id(),
            project_id,
            title: "migration".into(),
            parent_id: None,
        })
        .await
        .unwrap();
    let run = context
        .store
        .create_run(CreateRun {
            id: context.ids.next_run_id(),
            connection_id: connection_a(),
            work_item_id: work.id,
            provider_run: None,
        })
        .await
        .unwrap();
    let run = accept_run(&context.store, &context.ids, run).await;
    context
        .store
        .append_timeline_batch(TimelineBatch {
            batch_id: context.ids.next_timeline_batch_id(),
            connection_id: connection_a(),
            run_id: run.id,
            source_namespace: "mock".into(),
            stream_id: "migration".into(),
            expected_stream_revision: StreamCursor::INITIAL,
            items: vec![NewTimelineItem {
                id: context.ids.next_timeline_item_id(),
                kind: "message".into(),
                body: "preserved".into(),
                provider_id: None,
            }],
        })
        .await
        .unwrap();
    let approval = context
        .store
        .record_pending_approval(PendingApproval {
            id: context.ids.next_approval_request_id(),
            run_id: run.id,
            provider_id: NamespacedProviderId::new("mock", "migration-approval").unwrap(),
            kind: "command".into(),
            summary: "preserved".into(),
        })
        .await
        .unwrap();
    context.store.shutdown().await.unwrap();

    let database = context.paths.data_root.join("yakshed.sqlite3");
    let connection = rusqlite::Connection::open(&database).unwrap();
    connection
        .pragma_update(None, "foreign_keys", false)
        .unwrap();
    connection
        .execute_batch(
            "CREATE TABLE runs_v1 (
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
            INSERT INTO runs_v1 (
                id, connection_id, work_item_id, status, provider_namespace,
                provider_run_id, created_at_ms, ended_at_ms
            )
            SELECT
                id, connection_id, work_item_id, status, provider_namespace,
                provider_run_id, created_at_ms, ended_at_ms
            FROM runs;
            DROP TABLE runs;
            ALTER TABLE runs_v1 RENAME TO runs;
            DROP TABLE artifacts;
            PRAGMA user_version = 1;",
        )
        .unwrap();
    drop(connection);

    let reopened = SqliteStore::open(
        context.paths.clone(),
        Arc::new(FixedClock),
        context.ids.clone(),
    )
    .await
    .unwrap();
    assert_eq!(
        reopened
            .list_timeline_page(ListTimeline {
                run_id: run.id,
                after: None,
                limit: 10,
            })
            .await
            .unwrap()
            .items
            .len(),
        1
    );
    assert_eq!(
        reopened
            .list_approvals_for_run(run.id, None, 10)
            .await
            .unwrap()
            .items[0]
            .id,
        approval.id
    );
}

#[tokio::test]
async fn non_v7_id_in_durable_row_is_classified_as_integrity() {
    let temp = tempfile::tempdir().unwrap();
    let paths = AppPaths::for_test(temp.path());
    let ids = Arc::new(TestIds::new());
    let store = SqliteStore::open(paths.clone(), Arc::new(FixedClock), ids.clone())
        .await
        .unwrap();
    store.shutdown().await.unwrap();
    let connection = rusqlite::Connection::open(paths.data_root.join("yakshed.sqlite3")).unwrap();
    connection
        .execute(
            "INSERT INTO projects (id, name, created_at_ms) VALUES (?1, 'bad', 1)",
            ["550e8400-e29b-41d4-a716-446655440000"],
        )
        .unwrap();
    drop(connection);

    let reopened = SqliteStore::open(paths, Arc::new(FixedClock), ids)
        .await
        .unwrap();
    assert!(matches!(
        reopened.list_projects(None, 10).await,
        Err(StoreError::Integrity(_))
    ));
}

#[tokio::test]
async fn newer_schema_is_rejected_without_modification() {
    let temp = tempfile::tempdir().unwrap();
    let paths = AppPaths::for_test(temp.path());
    paths.create_data_root().unwrap();
    let database = paths.data_root.join("yakshed.sqlite3");
    let connection = rusqlite::Connection::open(&database).unwrap();
    connection.pragma_update(None, "user_version", 99).unwrap();
    drop(connection);
    let before = fs::read(&database).unwrap();

    assert!(matches!(
        SqliteStore::open(paths, Arc::new(FixedClock), Arc::new(TestIds::new())).await,
        Err(StoreError::UnsupportedNewerSchema {
            found: 99,
            supported: 5
        })
    ));
    assert_eq!(fs::read(database).unwrap(), before);
}

#[tokio::test]
async fn incompatible_prior_schema_is_classified_as_migration_failure_and_retained() {
    let temp = tempfile::tempdir().unwrap();
    let paths = AppPaths::for_test(temp.path());
    paths.create_data_root().unwrap();
    let database = paths.data_root.join("yakshed.sqlite3");
    let connection = rusqlite::Connection::open(&database).unwrap();
    connection
        .execute("CREATE TABLE projects (legacy INTEGER)", [])
        .unwrap();
    connection
        .execute("INSERT INTO projects VALUES (7)", [])
        .unwrap();
    drop(connection);

    assert!(matches!(
        SqliteStore::open(paths, Arc::new(FixedClock), Arc::new(TestIds::new())).await,
        Err(StoreError::Migration(_))
    ));
    let connection = rusqlite::Connection::open(database).unwrap();
    assert_eq!(
        connection
            .query_row("SELECT legacy FROM projects", [], |row| row
                .get::<_, i64>(0))
            .unwrap(),
        7
    );
}

#[tokio::test]
async fn unusable_data_root_is_classified_as_open_failure() {
    let temp = tempfile::tempdir().unwrap();
    let paths = AppPaths::for_test(temp.path());
    fs::write(&paths.data_root, b"not a directory").unwrap();

    assert!(matches!(
        SqliteStore::open(paths, Arc::new(FixedClock), Arc::new(TestIds::new())).await,
        Err(StoreError::Open(_))
    ));
}

#[tokio::test]
async fn missing_references_are_not_found_without_partial_work() {
    let context = Context::open().await;
    let project_id = context.project().await;
    assert!(matches!(
        context
            .store
            .create_work_item(CreateWorkItem {
                id: context.ids.next_work_item_id(),
                project_id,
                title: "orphan child".into(),
                parent_id: Some(missing_work_item()),
            })
            .await,
        Err(StoreError::NotFound { .. })
    ));
    assert!(
        context
            .store
            .list_work_items(ListWorkItems::for_project(project_id, 50))
            .await
            .unwrap()
            .items
            .is_empty()
    );
    assert!(matches!(
        context
            .store
            .create_run(CreateRun {
                id: context.ids.next_run_id(),
                connection_id: connection_a(),
                work_item_id: missing_work_item(),
                provider_run: None,
            })
            .await,
        Err(StoreError::NotFound { .. })
    ));
    assert!(matches!(
        context
            .store
            .create_work_item(CreateWorkItem {
                id: context.ids.next_work_item_id(),
                project_id: "0193f26e-7a72-7000-8000-00000000fffe".parse().unwrap(),
                title: "missing project".into(),
                parent_id: None,
            })
            .await,
        Err(StoreError::NotFound {
            entity: "project",
            ..
        })
    ));
}

#[tokio::test]
async fn duplicate_provider_id_is_a_conflict() {
    let context = Context::open().await;
    let project_id = context.project().await;
    let work = context
        .store
        .create_work_item(CreateWorkItem {
            id: context.ids.next_work_item_id(),
            project_id,
            title: "provider conflict".into(),
            parent_id: None,
        })
        .await
        .unwrap();
    let provider_id = provider_run("same-run");
    context
        .store
        .create_run(CreateRun {
            id: context.ids.next_run_id(),
            connection_id: connection_a(),
            work_item_id: work.id,
            provider_run: Some(provider_id.clone()),
        })
        .await
        .unwrap();

    assert!(matches!(
        context
            .store
            .create_run(CreateRun {
                id: context.ids.next_run_id(),
                connection_id: connection_a(),
                work_item_id: work.id,
                provider_run: Some(provider_id),
            })
            .await,
        Err(StoreError::Conflict(_))
    ));
}

#[tokio::test]
async fn artifact_run_must_belong_to_the_same_work_item() {
    let context = Context::open().await;
    let project_id = context.project().await;
    let work_a = context
        .store
        .create_work_item(CreateWorkItem {
            id: context.ids.next_work_item_id(),
            project_id,
            title: "A".into(),
            parent_id: None,
        })
        .await
        .unwrap();
    let work_b = context
        .store
        .create_work_item(CreateWorkItem {
            id: context.ids.next_work_item_id(),
            project_id,
            title: "B".into(),
            parent_id: None,
        })
        .await
        .unwrap();
    let run = context
        .store
        .create_run(CreateRun {
            id: context.ids.next_run_id(),
            connection_id: connection_a(),
            work_item_id: work_b.id,
            provider_run: None,
        })
        .await
        .unwrap();

    assert!(matches!(
        context
            .store
            .put_artifact_metadata(ArtifactRecord {
                id: ArtifactId::new_v7(),
                work_item_id: work_a.id,
                run_id: Some(run.id),
                kind: ArtifactKind::Plan,
                digest: ContentDigest::new("0".repeat(64)).unwrap(),
                byte_len: 1,
                media_type: "text/plain".to_owned(),
                provenance: ArtifactProvenance::new("test").unwrap(),
            })
            .await,
        Err(StoreError::Conflict(_))
    ));
}

#[tokio::test]
async fn provider_ids_and_streams_are_scoped_by_connection() {
    let context = Context::open().await;
    let project_id = context.project().await;
    let work = context
        .store
        .create_work_item(CreateWorkItem {
            id: context.ids.next_work_item_id(),
            project_id,
            title: "scoped providers".into(),
            parent_id: None,
        })
        .await
        .unwrap();
    let provider_run = provider_run("same-native-run");
    let run_a = context
        .store
        .create_run(CreateRun {
            id: context.ids.next_run_id(),
            connection_id: connection_a(),
            work_item_id: work.id,
            provider_run: Some(provider_run.clone()),
        })
        .await
        .unwrap();
    let run_b = context
        .store
        .create_run(CreateRun {
            id: context.ids.next_run_id(),
            connection_id: connection_b(),
            work_item_id: work.id,
            provider_run: Some(provider_run),
        })
        .await
        .unwrap();
    let run_a = accept_run(&context.store, &context.ids, run_a).await;
    let run_b = accept_run(&context.store, &context.ids, run_b).await;
    assert_eq!(run_a.connection_id, connection_a());
    assert_eq!(run_b.connection_id, connection_b());
    assert_eq!(run_a.provider_id.as_ref().unwrap().runtime_id(), "runtime");
    assert_eq!(run_a.provider_id.as_ref().unwrap().session_id(), "session");
    assert_eq!(
        run_a.provider_id.as_ref().unwrap().run_id(),
        "same-native-run"
    );

    for (connection_id, run_id) in [(connection_a(), run_a.id), (connection_b(), run_b.id)] {
        context
            .store
            .append_timeline_batch(TimelineBatch {
                batch_id: context.ids.next_timeline_batch_id(),
                connection_id,
                run_id,
                source_namespace: "mock".into(),
                stream_id: "same-native-stream".into(),
                expected_stream_revision: StreamCursor::INITIAL,
                items: vec![NewTimelineItem {
                    id: context.ids.next_timeline_item_id(),
                    kind: "event".into(),
                    body: "same provider item".into(),
                    provider_id: Some(
                        NamespacedProviderId::new("mock", "same-native-item").unwrap(),
                    ),
                }],
            })
            .await
            .unwrap();
    }
    assert!(matches!(
        context
            .store
            .append_timeline_batch(TimelineBatch {
                batch_id: context.ids.next_timeline_batch_id(),
                connection_id: connection_b(),
                run_id: run_a.id,
                source_namespace: "mock".into(),
                stream_id: "same-native-stream".into(),
                expected_stream_revision: StreamCursor::new(1),
                items: Vec::new(),
            })
            .await,
        Err(StoreError::Conflict(_))
    ));

    for (connection_id, run_id) in [(connection_a(), run_a.id), (connection_b(), run_b.id)] {
        let approval = context
            .store
            .record_pending_approval(PendingApproval {
                id: context.ids.next_approval_request_id(),
                run_id,
                provider_id: NamespacedProviderId::new("mock", "same-native-approval").unwrap(),
                kind: "command".into(),
                summary: "scoped".into(),
            })
            .await
            .unwrap();
        assert_eq!(approval.connection_id, connection_id);
    }
}

#[tokio::test]
async fn repeated_create_commands_are_idempotent_by_supplied_id() {
    let context = Context::open().await;
    let project_command = CreateProject {
        id: context.ids.next_project_id(),
        name: "idempotent".into(),
    };
    let project = context
        .store
        .create_project(project_command.clone())
        .await
        .unwrap();
    assert_eq!(
        context.store.create_project(project_command).await.unwrap(),
        project
    );
    assert!(matches!(
        context
            .store
            .create_project(CreateProject {
                id: project.id,
                name: "different".into(),
            })
            .await,
        Err(StoreError::Conflict(_))
    ));
    let work_command = CreateWorkItem {
        id: context.ids.next_work_item_id(),
        project_id: project.id,
        title: "same command".into(),
        parent_id: None,
    };
    let work = context
        .store
        .create_work_item(work_command.clone())
        .await
        .unwrap();
    assert_eq!(
        context.store.create_work_item(work_command).await.unwrap(),
        work
    );
    let run_command = CreateRun {
        id: context.ids.next_run_id(),
        connection_id: connection_a(),
        work_item_id: work.id,
        provider_run: None,
    };
    let run = context.store.create_run(run_command.clone()).await.unwrap();
    assert_eq!(context.store.create_run(run_command).await.unwrap(), run);
    let run = accept_run(&context.store, &context.ids, run).await;
    let approval_command = PendingApproval {
        id: context.ids.next_approval_request_id(),
        run_id: run.id,
        provider_id: NamespacedProviderId::new("mock", "idempotent-approval").unwrap(),
        kind: "command".into(),
        summary: "same command".into(),
    };
    let approval = context
        .store
        .record_pending_approval(approval_command.clone())
        .await
        .unwrap();
    assert_eq!(
        context
            .store
            .record_pending_approval(approval_command)
            .await
            .unwrap(),
        approval
    );
    assert_eq!(
        context
            .store
            .list_projects(None, 10)
            .await
            .unwrap()
            .items
            .len(),
        1
    );
    assert_eq!(
        context
            .store
            .list_work_items(ListWorkItems::for_project(project.id, 10))
            .await
            .unwrap()
            .items
            .len(),
        1
    );
    assert_eq!(
        context
            .store
            .list_active_runs(None, 10)
            .await
            .unwrap()
            .items
            .len(),
        1
    );
    assert_eq!(
        context
            .store
            .list_pending_approvals(None, 10)
            .await
            .unwrap()
            .items
            .len(),
        1
    );
}

#[test]
fn app_id_construction_rejects_non_v7_uuid() {
    assert!(ProjectId::try_from(uuid::Uuid::nil()).is_err());
}

#[tokio::test]
async fn archive_subtree_preserves_archived_records() {
    let context = Context::open().await;
    let project_id = context.project().await;
    let root = context
        .store
        .create_work_item(CreateWorkItem {
            id: context.ids.next_work_item_id(),
            project_id,
            title: "root".into(),
            parent_id: None,
        })
        .await
        .unwrap();
    let child = context
        .store
        .create_work_item(CreateWorkItem {
            id: context.ids.next_work_item_id(),
            project_id,
            title: "child".into(),
            parent_id: Some(root.id),
        })
        .await
        .unwrap();
    let grandchild = context
        .store
        .create_work_item(CreateWorkItem {
            id: context.ids.next_work_item_id(),
            project_id,
            title: "grandchild".into(),
            parent_id: Some(child.id),
        })
        .await
        .unwrap();

    assert_eq!(root.revision.get(), 1);
    assert_eq!(
        context.store.archive_work_subtree(root.id).await.unwrap(),
        3
    );
    for id in [root.id, child.id, grandchild.id] {
        let archived = context.store.get_work_item(id).await.unwrap();
        assert_eq!(archived.status, WorkItemStatus::Archived);
        assert_eq!(archived.revision.get(), 2);
    }
    assert!(matches!(
        context
            .store
            .create_work_item(CreateWorkItem {
                id: context.ids.next_work_item_id(),
                project_id,
                title: "too late".into(),
                parent_id: Some(root.id),
            })
            .await,
        Err(StoreError::Conflict(_))
    ));
    assert!(
        context
            .store
            .list_work_items(ListWorkItems::for_project(project_id, 10))
            .await
            .unwrap()
            .items
            .is_empty()
    );
    assert_eq!(
        context
            .store
            .list_work_items(ListWorkItems {
                include_archived: true,
                ..ListWorkItems::for_project(project_id, 10)
            })
            .await
            .unwrap()
            .items
            .len(),
        3
    );
}

#[tokio::test]
async fn archived_work_item_rejects_new_run_but_allows_idempotent_retry() {
    let context = Context::open().await;
    let project_id = context.project().await;
    let work = context
        .store
        .create_work_item(CreateWorkItem {
            id: context.ids.next_work_item_id(),
            project_id,
            title: "archived run target".into(),
            parent_id: None,
        })
        .await
        .unwrap();
    let original = CreateRun {
        id: context.ids.next_run_id(),
        connection_id: connection_a(),
        work_item_id: work.id,
        provider_run: None,
    };
    let run = context.store.create_run(original.clone()).await.unwrap();
    context.store.archive_work_subtree(work.id).await.unwrap();

    assert_eq!(context.store.create_run(original).await.unwrap(), run);
    assert!(matches!(
        context
            .store
            .create_run(CreateRun {
                id: context.ids.next_run_id(),
                connection_id: connection_a(),
                work_item_id: work.id,
                provider_run: None,
            })
            .await,
        Err(StoreError::Conflict(_))
    ));
}

#[tokio::test]
async fn pagination_is_stable_and_timeline_ordinals_are_independent_from_stream_cursors() {
    let context = Context::open().await;
    let project_id = context.project().await;
    for title in ["one", "two", "three"] {
        context
            .store
            .create_work_item(CreateWorkItem {
                id: context.ids.next_work_item_id(),
                project_id,
                title: title.into(),
                parent_id: None,
            })
            .await
            .unwrap();
    }
    let first = context
        .store
        .list_work_items(ListWorkItems::for_project(project_id, 2))
        .await
        .unwrap();
    let second = context
        .store
        .list_work_items(ListWorkItems {
            after: first.next_after,
            ..ListWorkItems::for_project(project_id, 2)
        })
        .await
        .unwrap();
    assert_eq!(first.items.len(), 2);
    assert_eq!(second.items.len(), 1);
    assert!(first.items[0].id < first.items[1].id);
    assert_eq!(
        first.items[0].created_at,
        UtcTimestamp::from_unix_millis(1_735_689_600_123)
    );

    let run = context
        .store
        .create_run(CreateRun {
            id: context.ids.next_run_id(),
            connection_id: connection_a(),
            work_item_id: first.items[0].id,
            provider_run: None,
        })
        .await
        .unwrap();
    let first_batch = TimelineBatch {
        batch_id: context.ids.next_timeline_batch_id(),
        connection_id: connection_a(),
        run_id: run.id,
        source_namespace: "mock".into(),
        stream_id: "session/opaque".into(),
        expected_stream_revision: StreamCursor::INITIAL,
        items: vec![
            NewTimelineItem {
                id: context.ids.next_timeline_item_id(),
                kind: "message".into(),
                body: "first".into(),
                provider_id: None,
            },
            NewTimelineItem {
                id: context.ids.next_timeline_item_id(),
                kind: "message".into(),
                body: "second".into(),
                provider_id: None,
            },
        ],
    };
    let cursor_two = context
        .store
        .append_timeline_batch(first_batch.clone())
        .await
        .unwrap();
    assert_eq!(
        context
            .store
            .append_timeline_batch(first_batch.clone())
            .await
            .unwrap(),
        cursor_two
    );
    let mut divergent = first_batch.clone();
    divergent.items[0].body = "different".into();
    assert!(matches!(
        context.store.append_timeline_batch(divergent).await,
        Err(StoreError::Conflict(_))
    ));
    let mut wrong_expected = first_batch;
    wrong_expected.expected_stream_revision = StreamCursor::new(1);
    assert!(matches!(
        context.store.append_timeline_batch(wrong_expected).await,
        Err(StoreError::Conflict(_))
    ));
    assert!(matches!(
        context
            .store
            .append_timeline_batch(TimelineBatch {
                batch_id: context.ids.next_timeline_batch_id(),
                connection_id: connection_a(),
                run_id: run.id,
                source_namespace: "mock".into(),
                stream_id: "session/opaque".into(),
                expected_stream_revision: StreamCursor::INITIAL,
                items: vec![NewTimelineItem {
                    id: context.ids.next_timeline_item_id(),
                    kind: "message".into(),
                    body: "replayed".into(),
                    provider_id: None,
                }],
            })
            .await,
        Err(StoreError::Conflict(_))
    ));
    let other_run = context
        .store
        .create_run(CreateRun {
            id: context.ids.next_run_id(),
            connection_id: connection_a(),
            work_item_id: first.items[0].id,
            provider_run: None,
        })
        .await
        .unwrap();
    assert!(matches!(
        context
            .store
            .append_timeline_batch(TimelineBatch {
                batch_id: context.ids.next_timeline_batch_id(),
                connection_id: connection_a(),
                run_id: other_run.id,
                source_namespace: "mock".into(),
                stream_id: "session/opaque".into(),
                expected_stream_revision: cursor_two,
                items: vec![NewTimelineItem {
                    id: context.ids.next_timeline_item_id(),
                    kind: "message".into(),
                    body: "wrong run".into(),
                    provider_id: None,
                }],
            })
            .await,
        Err(StoreError::Conflict(_))
    ));
    let opaque = NamespacedProviderId::new("mock", "turn/α:opaque").unwrap();
    let cursor_three = context
        .store
        .append_timeline_batch(TimelineBatch {
            batch_id: context.ids.next_timeline_batch_id(),
            connection_id: connection_a(),
            run_id: run.id,
            source_namespace: "mock".into(),
            stream_id: "session/opaque".into(),
            expected_stream_revision: cursor_two,
            items: vec![NewTimelineItem {
                id: context.ids.next_timeline_item_id(),
                kind: "provider_event".into(),
                body: "third".into(),
                provider_id: Some(opaque.clone()),
            }],
        })
        .await
        .unwrap();
    let other_cursor = context
        .store
        .append_timeline_batch(TimelineBatch {
            batch_id: context.ids.next_timeline_batch_id(),
            connection_id: connection_a(),
            run_id: run.id,
            source_namespace: "other-provider".into(),
            stream_id: "other-stream".into(),
            expected_stream_revision: StreamCursor::INITIAL,
            items: vec![NewTimelineItem {
                id: context.ids.next_timeline_item_id(),
                kind: "message".into(),
                body: "fourth".into(),
                provider_id: None,
            }],
        })
        .await
        .unwrap();
    assert_eq!(cursor_two.get(), 2);
    assert_eq!(cursor_three.get(), 3);
    assert_eq!(other_cursor.get(), 1);
    let timeline = context
        .store
        .list_timeline_page(ListTimeline {
            run_id: run.id,
            after: None,
            limit: 10,
        })
        .await
        .unwrap();
    assert_eq!(timeline.items.len(), 4);
    assert_eq!(timeline.items[0].body, "first");
    assert_eq!(timeline.items[1].body, "second");
    assert_eq!(
        timeline
            .items
            .iter()
            .map(|item| item.revision.get())
            .collect::<Vec<_>>(),
        vec![1, 2, 3, 4]
    );
    assert_eq!(timeline.items[2].provider_id.as_ref(), Some(&opaque));
}

#[tokio::test]
async fn corruption_is_classified_and_original_file_is_retained() {
    let temp = tempfile::tempdir().unwrap();
    let paths = AppPaths::for_test(temp.path());
    paths.create_data_root().unwrap();
    let database = paths.data_root.join("yakshed.sqlite3");
    let garbage = b"not a sqlite database";
    fs::write(&database, garbage).unwrap();

    assert!(matches!(
        SqliteStore::open(paths, Arc::new(FixedClock), Arc::new(TestIds::new())).await,
        Err(StoreError::Integrity(_))
    ));
    assert_eq!(fs::read(database).unwrap(), garbage);
}

#[tokio::test]
async fn concurrent_callers_serialize_and_shutdown_is_typed() {
    let context = Context::open().await;
    let project_id = context.project().await;
    let create = |title: &'static str| {
        context.store.create_work_item(CreateWorkItem {
            id: context.ids.next_work_item_id(),
            project_id,
            title: title.into(),
            parent_id: None,
        })
    };
    let (one, two, three) = tokio::time::timeout(std::time::Duration::from_secs(2), async {
        tokio::join!(create("one"), create("two"), create("three"))
    })
    .await
    .unwrap();
    assert!(one.is_ok() && two.is_ok() && three.is_ok());

    context.store.shutdown().await.unwrap();
    assert!(matches!(
        context.store.get_work_item(one.unwrap().id).await,
        Err(StoreError::Closed)
    ));
}

#[tokio::test]
async fn second_open_conflicts_without_reconciling_live_run() {
    let temp = tempfile::tempdir().unwrap();
    let paths = AppPaths::for_test(temp.path());
    let ids = Arc::new(TestIds::new());
    let store = SqliteStore::open(paths.clone(), Arc::new(FixedClock), ids.clone())
        .await
        .unwrap();
    let project = store
        .create_project(CreateProject {
            id: ids.next_project_id(),
            name: "lease owner".into(),
        })
        .await
        .unwrap();
    let work = store
        .create_work_item(CreateWorkItem {
            id: ids.next_work_item_id(),
            project_id: project.id,
            title: "live work".into(),
            parent_id: None,
        })
        .await
        .unwrap();
    let run = store
        .create_run(CreateRun {
            id: ids.next_run_id(),
            connection_id: connection_a(),
            work_item_id: work.id,
            provider_run: None,
        })
        .await
        .unwrap();
    let run = accept_run(&store, &ids, run).await;

    match SqliteStore::open(paths.clone(), Arc::new(FixedClock), ids.clone()).await {
        Err(StoreError::Conflict(_)) => {}
        Err(error) => panic!("unexpected second-open error: {error:?}"),
        Ok(_) => panic!("second store unexpectedly acquired the database lease"),
    }
    assert_eq!(
        store.get_run(run.id).await.unwrap().status,
        RunStatus::Running
    );
    assert_eq!(
        store.list_active_runs(None, 10).await.unwrap().items.len(),
        1
    );

    store.shutdown().await.unwrap();
    let reopened = SqliteStore::open(paths, Arc::new(FixedClock), ids)
        .await
        .unwrap();
    assert_eq!(
        reopened.get_run(run.id).await.unwrap().status,
        RunStatus::Running
    );
}

#[tokio::test]
async fn reopen_preserves_dangling_start_for_application_reconciliation() {
    let temp = tempfile::tempdir().unwrap();
    let paths = AppPaths::for_test(temp.path());
    let ids = Arc::new(TestIds::new());
    let store = SqliteStore::open(paths.clone(), Arc::new(FixedClock), ids.clone())
        .await
        .unwrap();
    let project = store
        .create_project(CreateProject {
            id: ids.next_project_id(),
            name: "dangling start".into(),
        })
        .await
        .unwrap();
    let work = store
        .create_work_item(CreateWorkItem {
            id: ids.next_work_item_id(),
            project_id: project.id,
            title: "dangling start".into(),
            parent_id: None,
        })
        .await
        .unwrap();
    let run = store
        .create_run(CreateRun {
            id: ids.next_run_id(),
            connection_id: connection_a(),
            work_item_id: work.id,
            provider_run: None,
        })
        .await
        .unwrap();
    store.shutdown().await.unwrap();

    let reopened = SqliteStore::open(paths, Arc::new(FixedClock), ids)
        .await
        .unwrap();
    assert_eq!(
        reopened.get_run(run.id).await.unwrap().status,
        RunStatus::Starting
    );
}

#[tokio::test]
async fn config_reset_does_not_remove_sqlite_work_data() {
    let context = Context::open().await;
    let config = ConfigStore::open(context.paths.clone(), &[]).unwrap();
    let project_id = context.project().await;
    let work = context
        .store
        .create_work_item(CreateWorkItem {
            id: context.ids.next_work_item_id(),
            project_id,
            title: "durable".into(),
            parent_id: None,
        })
        .await
        .unwrap();

    config
        .update(ConfigRevision::INITIAL, ConfigChange::Reset)
        .await
        .unwrap();
    assert_eq!(context.store.get_work_item(work.id).await.unwrap(), work);
}

#[tokio::test]
async fn approval_response_transitions_are_application_shaped() {
    let context = Context::open().await;
    let project_id = context.project().await;
    let work = context
        .store
        .create_work_item(CreateWorkItem {
            id: context.ids.next_work_item_id(),
            project_id,
            title: "approve".into(),
            parent_id: None,
        })
        .await
        .unwrap();
    let run = context
        .store
        .create_run(CreateRun {
            id: context.ids.next_run_id(),
            connection_id: connection_a(),
            work_item_id: work.id,
            provider_run: None,
        })
        .await
        .unwrap();
    let run = accept_run(&context.store, &context.ids, run).await;
    let approval = context
        .store
        .record_pending_approval(PendingApproval {
            id: context.ids.next_approval_request_id(),
            run_id: run.id,
            provider_id: NamespacedProviderId::new("mock", "approval/raw-123").unwrap(),
            kind: "command".into(),
            summary: "Run tests".into(),
        })
        .await
        .unwrap();

    assert!(matches!(
        context
            .store
            .confirm_approval_response(ConfirmApprovalResponse {
                approval_id: approval.id,
                audit_event_id: context.ids.next_audit_event_id(),
            })
            .await,
        Err(StoreError::Conflict(_))
    ));
    let responding = context
        .store
        .begin_approval_response(BeginApprovalResponse {
            approval_id: approval.id,
            decision: ApprovalDecision::Approved,
            audit_event_id: context.ids.next_audit_event_id(),
        })
        .await
        .unwrap();
    assert_eq!(
        responding.status,
        ApprovalStatus::Responding {
            decision: ApprovalDecision::Approved
        }
    );
    let resolved = context
        .store
        .confirm_approval_response(ConfirmApprovalResponse {
            approval_id: approval.id,
            audit_event_id: context.ids.next_audit_event_id(),
        })
        .await
        .unwrap();
    assert_eq!(
        resolved.status,
        ApprovalStatus::Resolved {
            decision: ApprovalDecision::Approved
        }
    );
    assert!(matches!(
        context
            .store
            .confirm_approval_response(ConfirmApprovalResponse {
                approval_id: approval.id,
                audit_event_id: context.ids.next_audit_event_id(),
            })
            .await,
        Err(StoreError::Conflict(_))
    ));
}

#[tokio::test]
async fn recoverable_run_states_keep_pending_approval_and_no_end_time() {
    let context = Context::open().await;
    let project_id = context.project().await;
    let work = context
        .store
        .create_work_item(CreateWorkItem {
            id: context.ids.next_work_item_id(),
            project_id,
            title: "recoverable run".into(),
            parent_id: None,
        })
        .await
        .unwrap();
    let run = context
        .store
        .create_run(CreateRun {
            id: context.ids.next_run_id(),
            connection_id: connection_a(),
            work_item_id: work.id,
            provider_run: None,
        })
        .await
        .unwrap();
    let run = accept_run(&context.store, &context.ids, run).await;
    let approval = context
        .store
        .record_pending_approval(PendingApproval {
            id: context.ids.next_approval_request_id(),
            run_id: run.id,
            provider_id: NamespacedProviderId::new("mock", "recoverable-approval").unwrap(),
            kind: "command".into(),
            summary: "keep me".into(),
        })
        .await
        .unwrap();

    let disconnected = context
        .store
        .transition_run(TransitionRun {
            run_id: run.id,
            expected_current: RunStatus::Running,
            target: RunStatus::Disconnected,
            provider_id: None,
            occurred_at: UtcTimestamp::from_unix_millis(50),
            audit_event_id: context.ids.next_audit_event_id(),
        })
        .await
        .unwrap();
    assert_eq!(disconnected.ended_at, None);
    let uncertain = context
        .store
        .transition_run(TransitionRun {
            run_id: run.id,
            expected_current: RunStatus::Disconnected,
            target: RunStatus::OutcomeUnknown,
            provider_id: None,
            occurred_at: UtcTimestamp::from_unix_millis(51),
            audit_event_id: context.ids.next_audit_event_id(),
        })
        .await
        .unwrap();
    assert_eq!(uncertain.ended_at, None);
    context
        .store
        .transition_run(TransitionRun {
            run_id: run.id,
            expected_current: RunStatus::OutcomeUnknown,
            target: RunStatus::Running,
            provider_id: None,
            occurred_at: UtcTimestamp::from_unix_millis(52),
            audit_event_id: context.ids.next_audit_event_id(),
        })
        .await
        .unwrap();
    let approvals = context
        .store
        .list_approvals_for_run(run.id, None, 10)
        .await
        .unwrap();
    assert_eq!(approvals.items[0].id, approval.id);
    assert_eq!(approvals.items[0].status, ApprovalStatus::Pending);
    context
        .store
        .begin_approval_response(BeginApprovalResponse {
            approval_id: approval.id,
            decision: ApprovalDecision::Approved,
            audit_event_id: context.ids.next_audit_event_id(),
        })
        .await
        .unwrap();
}

#[tokio::test]
async fn terminal_run_transition_reconciles_approvals_and_filters_active_runs() {
    let context = Context::open().await;
    let project_id = context.project().await;
    let work = context
        .store
        .create_work_item(CreateWorkItem {
            id: context.ids.next_work_item_id(),
            project_id,
            title: "terminal run".into(),
            parent_id: None,
        })
        .await
        .unwrap();
    let run = context
        .store
        .create_run(CreateRun {
            id: context.ids.next_run_id(),
            connection_id: connection_a(),
            work_item_id: work.id,
            provider_run: None,
        })
        .await
        .unwrap();
    let run = accept_run(&context.store, &context.ids, run).await;
    let pending = context
        .store
        .record_pending_approval(PendingApproval {
            id: context.ids.next_approval_request_id(),
            run_id: run.id,
            provider_id: NamespacedProviderId::new("mock", "void-on-failure").unwrap(),
            kind: "command".into(),
            summary: "cannot proceed".into(),
        })
        .await
        .unwrap();
    let responding = context
        .store
        .record_pending_approval(PendingApproval {
            id: context.ids.next_approval_request_id(),
            run_id: run.id,
            provider_id: NamespacedProviderId::new("mock", "in-flight").unwrap(),
            kind: "command".into(),
            summary: "already dispatched".into(),
        })
        .await
        .unwrap();
    let responding = context
        .store
        .begin_approval_response(BeginApprovalResponse {
            approval_id: responding.id,
            decision: ApprovalDecision::Approved,
            audit_event_id: context.ids.next_audit_event_id(),
        })
        .await
        .unwrap();
    assert!(matches!(
        context
            .store
            .transition_run(TransitionRun {
                run_id: run.id,
                expected_current: RunStatus::Running,
                target: RunStatus::Running,
                provider_id: None,
                occurred_at: UtcTimestamp::from_unix_millis(50),
                audit_event_id: context.ids.next_audit_event_id(),
            })
            .await,
        Err(StoreError::Conflict(_))
    ));
    let failed = context
        .store
        .transition_run(TransitionRun {
            run_id: run.id,
            expected_current: RunStatus::Running,
            target: RunStatus::Failed,
            provider_id: None,
            occurred_at: UtcTimestamp::from_unix_millis(51),
            audit_event_id: context.ids.next_audit_event_id(),
        })
        .await
        .unwrap();
    assert_eq!(failed.status, RunStatus::Failed);
    assert_eq!(failed.ended_at, Some(UtcTimestamp::from_unix_millis(51)));
    assert!(
        context
            .store
            .list_active_runs(None, 10)
            .await
            .unwrap()
            .items
            .is_empty()
    );
    assert_eq!(
        context
            .store
            .list_unconfirmed_approval_responses(None, 10)
            .await
            .unwrap()
            .items,
        vec![responding.clone()]
    );
    let resolved = context
        .store
        .confirm_approval_response(ConfirmApprovalResponse {
            approval_id: responding.id,
            audit_event_id: context.ids.next_audit_event_id(),
        })
        .await
        .unwrap();
    assert_eq!(
        resolved.status,
        ApprovalStatus::Resolved {
            decision: ApprovalDecision::Approved
        }
    );
    let history = context
        .store
        .list_approvals_for_run(run.id, None, 10)
        .await
        .unwrap();
    assert_eq!(history.items[0].id, pending.id);
    assert_eq!(
        history.items[0].status,
        ApprovalStatus::Voided { decision: None }
    );
    assert_eq!(history.items[1], resolved);
    assert!(
        context
            .store
            .list_pending_approvals(None, 10)
            .await
            .unwrap()
            .items
            .is_empty()
    );
    assert!(matches!(
        context
            .store
            .transition_run(TransitionRun {
                run_id: run.id,
                expected_current: RunStatus::Running,
                target: RunStatus::Interrupted,
                provider_id: None,
                occurred_at: UtcTimestamp::from_unix_millis(52),
                audit_event_id: context.ids.next_audit_event_id(),
            })
            .await,
        Err(StoreError::Conflict(_))
    ));
}

#[tokio::test]
async fn terminal_run_allows_existing_approval_retry_but_rejects_new_approval() {
    let context = Context::open().await;
    let project_id = context.project().await;
    let work = context
        .store
        .create_work_item(CreateWorkItem {
            id: context.ids.next_work_item_id(),
            project_id,
            title: "closed run".into(),
            parent_id: None,
        })
        .await
        .unwrap();
    let run = context
        .store
        .create_run(CreateRun {
            id: context.ids.next_run_id(),
            connection_id: connection_a(),
            work_item_id: work.id,
            provider_run: None,
        })
        .await
        .unwrap();
    let run = accept_run(&context.store, &context.ids, run).await;
    let original_command = PendingApproval {
        id: context.ids.next_approval_request_id(),
        run_id: run.id,
        provider_id: NamespacedProviderId::new("mock", "before-terminal").unwrap(),
        kind: "command".into(),
        summary: "recorded while running".into(),
    };
    context
        .store
        .record_pending_approval(original_command.clone())
        .await
        .unwrap();
    context
        .store
        .transition_run(TransitionRun {
            run_id: run.id,
            expected_current: RunStatus::Running,
            target: RunStatus::Completed,
            provider_id: None,
            occurred_at: UtcTimestamp::from_unix_millis(60),
            audit_event_id: context.ids.next_audit_event_id(),
        })
        .await
        .unwrap();

    let retried = context
        .store
        .record_pending_approval(original_command)
        .await
        .unwrap();
    assert_eq!(retried.status, ApprovalStatus::Voided { decision: None });

    assert!(matches!(
        context
            .store
            .record_pending_approval(PendingApproval {
                id: context.ids.next_approval_request_id(),
                run_id: run.id,
                provider_id: NamespacedProviderId::new("mock", "late").unwrap(),
                kind: "command".into(),
                summary: "too late".into(),
            })
            .await,
        Err(StoreError::Conflict(_))
    ));
    assert!(
        context
            .store
            .list_pending_approvals(None, 10)
            .await
            .unwrap()
            .items
            .is_empty()
    );
    assert!(
        context
            .store
            .list_unconfirmed_approval_responses(None, 10)
            .await
            .unwrap()
            .items
            .is_empty()
    );
    assert_eq!(
        context
            .store
            .list_approvals_for_run(run.id, None, 10)
            .await
            .unwrap()
            .items,
        vec![retried]
    );
}

#[tokio::test]
async fn reopen_preserves_orphaned_run_and_approval_state() {
    let temp = tempfile::tempdir().unwrap();
    let paths = AppPaths::for_test(temp.path());
    let ids = Arc::new(TestIds::new());
    let store = SqliteStore::open(paths.clone(), Arc::new(FixedClock), ids.clone())
        .await
        .unwrap();
    let project = store
        .create_project(CreateProject {
            id: ids.next_project_id(),
            name: "orphan recovery".into(),
        })
        .await
        .unwrap();
    let work = store
        .create_work_item(CreateWorkItem {
            id: ids.next_work_item_id(),
            project_id: project.id,
            title: "orphaned run".into(),
            parent_id: None,
        })
        .await
        .unwrap();
    let run = store
        .create_run(CreateRun {
            id: ids.next_run_id(),
            connection_id: connection_a(),
            work_item_id: work.id,
            provider_run: None,
        })
        .await
        .unwrap();
    let run = accept_run(&store, &ids, run).await;
    let pending = store
        .record_pending_approval(PendingApproval {
            id: ids.next_approval_request_id(),
            run_id: run.id,
            provider_id: NamespacedProviderId::new("mock", "orphan-pending").unwrap(),
            kind: "command".into(),
            summary: "not dispatched".into(),
        })
        .await
        .unwrap();
    let responding = store
        .record_pending_approval(PendingApproval {
            id: ids.next_approval_request_id(),
            run_id: run.id,
            provider_id: NamespacedProviderId::new("mock", "orphan-responding").unwrap(),
            kind: "command".into(),
            summary: "already dispatched".into(),
        })
        .await
        .unwrap();
    let responding = store
        .begin_approval_response(BeginApprovalResponse {
            approval_id: responding.id,
            decision: ApprovalDecision::Approved,
            audit_event_id: ids.next_audit_event_id(),
        })
        .await
        .unwrap();
    drop(store);

    let reopened = SqliteStore::open(paths, Arc::new(FixedClock), ids.clone())
        .await
        .unwrap();
    assert_eq!(
        reopened
            .list_active_runs(None, 10)
            .await
            .unwrap()
            .items
            .len(),
        1
    );
    let interrupted = reopened.get_run(run.id).await.unwrap();
    assert_eq!(interrupted.status, RunStatus::Running);
    assert_eq!(interrupted.ended_at, None);
    let history = reopened
        .list_approvals_for_run(run.id, None, 10)
        .await
        .unwrap();
    assert_eq!(history.items[0].id, pending.id);
    assert_eq!(history.items[0].status, ApprovalStatus::Pending);
    assert_eq!(history.items[1].status, responding.status);
    assert_eq!(
        reopened
            .list_unconfirmed_approval_responses(None, 10)
            .await
            .unwrap()
            .items[0]
            .id,
        responding.id
    );
    let resolved = reopened
        .confirm_approval_response(ConfirmApprovalResponse {
            approval_id: responding.id,
            audit_event_id: ids.next_audit_event_id(),
        })
        .await
        .unwrap();
    assert_eq!(
        resolved.status,
        ApprovalStatus::Resolved {
            decision: ApprovalDecision::Approved
        }
    );
}

#[tokio::test]
async fn reopened_store_serves_all_durable_state_written_before_shutdown() {
    let temp = tempfile::tempdir().unwrap();
    let paths = AppPaths::for_test(temp.path());
    let ids = Arc::new(TestIds::new());
    let store = SqliteStore::open(paths.clone(), Arc::new(FixedClock), ids.clone())
        .await
        .unwrap();
    let project = store
        .create_project(CreateProject {
            id: ids.next_project_id(),
            name: "restart".into(),
        })
        .await
        .unwrap();
    let second_project = store
        .create_project(CreateProject {
            id: ids.next_project_id(),
            name: "restart two".into(),
        })
        .await
        .unwrap();
    let root = store
        .create_work_item(CreateWorkItem {
            id: ids.next_work_item_id(),
            project_id: project.id,
            title: "root".into(),
            parent_id: None,
        })
        .await
        .unwrap();
    let child = store
        .create_work_item(CreateWorkItem {
            id: ids.next_work_item_id(),
            project_id: project.id,
            title: "child".into(),
            parent_id: Some(root.id),
        })
        .await
        .unwrap();
    let run = store
        .create_run(CreateRun {
            id: ids.next_run_id(),
            connection_id: connection_a(),
            work_item_id: child.id,
            provider_run: Some(provider_run("run/restart")),
        })
        .await
        .unwrap();
    let second_run = store
        .create_run(CreateRun {
            id: ids.next_run_id(),
            connection_id: connection_a(),
            work_item_id: child.id,
            provider_run: Some(provider_run("run/restart-2")),
        })
        .await
        .unwrap();
    let run = accept_run(&store, &ids, run).await;
    let second_run = accept_run(&store, &ids, second_run).await;
    let first_batch_id = ids.next_timeline_batch_id();
    let restart_batch = TimelineBatch {
        batch_id: first_batch_id,
        connection_id: connection_a(),
        run_id: run.id,
        source_namespace: "mock".into(),
        stream_id: "restart-stream".into(),
        expected_stream_revision: StreamCursor::INITIAL,
        items: vec![
            NewTimelineItem {
                id: ids.next_timeline_item_id(),
                kind: "message".into(),
                body: "one".into(),
                provider_id: None,
            },
            NewTimelineItem {
                id: ids.next_timeline_item_id(),
                kind: "message".into(),
                body: "two".into(),
                provider_id: None,
            },
        ],
    };
    assert_eq!(
        store
            .append_timeline_batch(restart_batch.clone())
            .await
            .unwrap()
            .get(),
        2
    );
    let pending = store
        .record_pending_approval(PendingApproval {
            id: ids.next_approval_request_id(),
            run_id: run.id,
            provider_id: NamespacedProviderId::new("mock", "approval/pending").unwrap(),
            kind: "command".into(),
            summary: "pending".into(),
        })
        .await
        .unwrap();
    let responding = store
        .record_pending_approval(PendingApproval {
            id: ids.next_approval_request_id(),
            run_id: run.id,
            provider_id: NamespacedProviderId::new("mock", "approval/responding").unwrap(),
            kind: "command".into(),
            summary: "responding".into(),
        })
        .await
        .unwrap();
    store
        .begin_approval_response(BeginApprovalResponse {
            approval_id: responding.id,
            decision: ApprovalDecision::Approved,
            audit_event_id: ids.next_audit_event_id(),
        })
        .await
        .unwrap();
    let resolved = store
        .record_pending_approval(PendingApproval {
            id: ids.next_approval_request_id(),
            run_id: run.id,
            provider_id: NamespacedProviderId::new("mock", "approval/resolved").unwrap(),
            kind: "command".into(),
            summary: "resolved".into(),
        })
        .await
        .unwrap();
    store
        .begin_approval_response(BeginApprovalResponse {
            approval_id: resolved.id,
            decision: ApprovalDecision::Denied,
            audit_event_id: ids.next_audit_event_id(),
        })
        .await
        .unwrap();
    store
        .confirm_approval_response(ConfirmApprovalResponse {
            approval_id: resolved.id,
            audit_event_id: ids.next_audit_event_id(),
        })
        .await
        .unwrap();
    assert_eq!(
        store
            .list_approvals_for_run(run.id, None, 10)
            .await
            .unwrap()
            .items
            .iter()
            .map(|approval| approval.status)
            .collect::<Vec<_>>(),
        vec![
            ApprovalStatus::Pending,
            ApprovalStatus::Responding {
                decision: ApprovalDecision::Approved,
            },
            ApprovalStatus::Resolved {
                decision: ApprovalDecision::Denied,
            },
        ]
    );
    let terminal_run = store
        .transition_run(TransitionRun {
            run_id: run.id,
            expected_current: RunStatus::Running,
            target: RunStatus::Completed,
            provider_id: None,
            occurred_at: UtcTimestamp::from_unix_millis(98),
            audit_event_id: ids.next_audit_event_id(),
        })
        .await
        .unwrap();
    let second_terminal_run = store
        .transition_run(TransitionRun {
            run_id: second_run.id,
            expected_current: RunStatus::Running,
            target: RunStatus::Completed,
            provider_id: None,
            occurred_at: UtcTimestamp::from_unix_millis(99),
            audit_event_id: ids.next_audit_event_id(),
        })
        .await
        .unwrap();
    store.shutdown().await.unwrap();

    let reopened = SqliteStore::open(paths, Arc::new(FixedClock), ids.clone())
        .await
        .unwrap();
    let first_projects = reopened.list_projects(None, 1).await.unwrap();
    let second_projects = reopened
        .list_projects(first_projects.next_after, 1)
        .await
        .unwrap();
    assert_eq!(first_projects.items, vec![project.clone()]);
    assert_eq!(second_projects.items, vec![second_project]);
    let work = reopened
        .list_work_items(ListWorkItems::for_project(project.id, 10))
        .await
        .unwrap();
    assert_eq!(work.items.len(), 2);
    assert_eq!(work.items[1].parent_id, Some(root.id));
    assert!(
        reopened
            .list_active_runs(None, 10)
            .await
            .unwrap()
            .items
            .is_empty()
    );
    assert_eq!(reopened.get_run(run.id).await.unwrap(), terminal_run);
    let first_runs = reopened
        .list_runs_for_work_item(child.id, None, 1)
        .await
        .unwrap();
    let second_runs = reopened
        .list_runs_for_work_item(child.id, first_runs.next_after, 1)
        .await
        .unwrap();
    assert_eq!(first_runs.items, vec![terminal_run.clone()]);
    assert_eq!(second_runs.items, vec![second_terminal_run]);
    let first_approvals = reopened
        .list_approvals_for_run(run.id, None, 1)
        .await
        .unwrap();
    let second_approvals = reopened
        .list_approvals_for_run(run.id, first_approvals.next_after, 1)
        .await
        .unwrap();
    let third_approvals = reopened
        .list_approvals_for_run(run.id, second_approvals.next_after, 1)
        .await
        .unwrap();
    let approval_history = [
        first_approvals.items[0].clone(),
        second_approvals.items[0].clone(),
        third_approvals.items[0].clone(),
    ];
    assert_eq!(
        approval_history
            .iter()
            .map(|approval| approval.id)
            .collect::<Vec<_>>(),
        vec![pending.id, responding.id, resolved.id]
    );
    assert_eq!(
        approval_history[0].status,
        ApprovalStatus::Voided { decision: None }
    );
    assert_eq!(
        approval_history[1].status,
        ApprovalStatus::Responding {
            decision: ApprovalDecision::Approved
        }
    );
    assert_eq!(
        approval_history[2].status,
        ApprovalStatus::Resolved {
            decision: ApprovalDecision::Denied
        }
    );
    let first_page = reopened
        .list_timeline_page(ListTimeline {
            run_id: run.id,
            after: None,
            limit: 1,
        })
        .await
        .unwrap();
    let second_page = reopened
        .list_timeline_page(ListTimeline {
            run_id: run.id,
            after: first_page.next_after,
            limit: 1,
        })
        .await
        .unwrap();
    assert_eq!(first_page.items[0].body, "one");
    assert_eq!(second_page.items[0].body, "two");
    let recovered_cursor = reopened
        .get_stream_cursor(GetStreamCursor {
            connection_id: connection_a(),
            run_id: run.id,
            source_namespace: "mock".into(),
            stream_id: "restart-stream".into(),
        })
        .await
        .unwrap()
        .unwrap();
    assert_eq!(recovered_cursor.cursor.get(), 2);
    assert_eq!(recovered_cursor.last_batch_id, first_batch_id);
    assert_eq!(
        reopened
            .append_timeline_batch(restart_batch)
            .await
            .unwrap()
            .get(),
        2
    );
    assert_eq!(
        reopened
            .list_timeline_page(ListTimeline {
                run_id: run.id,
                after: None,
                limit: 10,
            })
            .await
            .unwrap()
            .items
            .len(),
        2
    );
}
