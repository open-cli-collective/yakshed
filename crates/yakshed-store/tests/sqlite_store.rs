use std::{fs, sync::Arc};

use tempfile::TempDir;
use yakshed_application::{
    AppStore, ApprovalResolution, Clock, ConfigChange, ConfigRevision, CreateProject, CreateRun,
    CreateWorkItem, IdGenerator, ListTimeline, ListWorkItems, NewTimelineItem, PendingApproval,
    StoreError, TimelineBatch,
};
use yakshed_domain::{
    ApprovalDecision, ApprovalRequestId, AuditEventId, NamespacedProviderId, ProjectId, RunId,
    TimelineItemId, UtcTimestamp, WorkItemId, WorkItemStatus,
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
        self.next_uuid().into()
    }

    fn next_work_item_id(&self) -> WorkItemId {
        self.next_uuid().into()
    }

    fn next_run_id(&self) -> RunId {
        self.next_uuid().into()
    }

    fn next_timeline_item_id(&self) -> TimelineItemId {
        self.next_uuid().into()
    }

    fn next_approval_request_id(&self) -> ApprovalRequestId {
        self.next_uuid().into()
    }

    fn next_audit_event_id(&self) -> AuditEventId {
        self.next_uuid().into()
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
    store: Arc<SqliteStore>,
}

impl Context {
    async fn open() -> Self {
        let temp = tempfile::tempdir().unwrap();
        let paths = AppPaths::for_test(temp.path());
        let store = SqliteStore::open(
            paths.clone(),
            Arc::new(FixedClock),
            Arc::new(TestIds::new()),
        )
        .await
        .unwrap();
        Self {
            _temp: temp,
            paths,
            store: Arc::new(store),
        }
    }

    async fn project(&self) -> ProjectId {
        self.store
            .create_project(CreateProject {
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

#[tokio::test]
async fn empty_database_migrates_to_v1_before_ready() {
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
        1
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
            supported: 1
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
async fn foreign_keys_reject_orphan_edges_and_runs_without_partial_work() {
    let context = Context::open().await;
    let project_id = context.project().await;
    assert!(matches!(
        context
            .store
            .create_work_item(CreateWorkItem {
                project_id,
                title: "orphan child".into(),
                parent_id: Some(missing_work_item()),
            })
            .await,
        Err(StoreError::Integrity(_))
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
                work_item_id: missing_work_item(),
                provider_run: None,
            })
            .await,
        Err(StoreError::Integrity(_))
    ));
}

#[tokio::test]
async fn archive_subtree_preserves_archived_records() {
    let context = Context::open().await;
    let project_id = context.project().await;
    let root = context
        .store
        .create_work_item(CreateWorkItem {
            project_id,
            title: "root".into(),
            parent_id: None,
        })
        .await
        .unwrap();
    let child = context
        .store
        .create_work_item(CreateWorkItem {
            project_id,
            title: "child".into(),
            parent_id: Some(root.id),
        })
        .await
        .unwrap();
    let grandchild = context
        .store
        .create_work_item(CreateWorkItem {
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
async fn pagination_is_stable_and_projection_revisions_are_monotonic() {
    let context = Context::open().await;
    let project_id = context.project().await;
    for title in ["one", "two", "three"] {
        context
            .store
            .create_work_item(CreateWorkItem {
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
            work_item_id: first.items[0].id,
            provider_run: None,
        })
        .await
        .unwrap();
    let revision_two = context
        .store
        .append_timeline_batch(TimelineBatch {
            run_id: run.id,
            source_namespace: "mock".into(),
            stream_id: "session/opaque".into(),
            items: vec![
                NewTimelineItem {
                    kind: "message".into(),
                    body: "first".into(),
                    provider_id: None,
                },
                NewTimelineItem {
                    kind: "message".into(),
                    body: "second".into(),
                    provider_id: None,
                },
            ],
        })
        .await
        .unwrap();
    let opaque = NamespacedProviderId::new("mock", "turn/α:opaque").unwrap();
    let revision_three = context
        .store
        .append_timeline_batch(TimelineBatch {
            run_id: run.id,
            source_namespace: "mock".into(),
            stream_id: "session/opaque".into(),
            items: vec![NewTimelineItem {
                kind: "provider_event".into(),
                body: "third".into(),
                provider_id: Some(opaque.clone()),
            }],
        })
        .await
        .unwrap();
    let revision_four = context
        .store
        .append_timeline_batch(TimelineBatch {
            run_id: run.id,
            source_namespace: "other-provider".into(),
            stream_id: "other-stream".into(),
            items: vec![NewTimelineItem {
                kind: "message".into(),
                body: "fourth".into(),
                provider_id: None,
            }],
        })
        .await
        .unwrap();
    assert!(revision_four > revision_three && revision_three > revision_two);
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
async fn config_reset_does_not_remove_sqlite_work_data() {
    let context = Context::open().await;
    let config = ConfigStore::open(context.paths.clone()).unwrap();
    let project_id = context.project().await;
    let work = context
        .store
        .create_work_item(CreateWorkItem {
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
async fn approval_resolution_is_application_shaped() {
    let context = Context::open().await;
    let project_id = context.project().await;
    let work = context
        .store
        .create_work_item(CreateWorkItem {
            project_id,
            title: "approve".into(),
            parent_id: None,
        })
        .await
        .unwrap();
    let run = context
        .store
        .create_run(CreateRun {
            work_item_id: work.id,
            provider_run: None,
        })
        .await
        .unwrap();
    let approval = context
        .store
        .record_pending_approval(PendingApproval {
            run_id: run.id,
            provider_id: NamespacedProviderId::new("mock", "approval/raw-123").unwrap(),
            kind: "command".into(),
            summary: "Run tests".into(),
        })
        .await
        .unwrap();

    context
        .store
        .resolve_approval(ApprovalResolution {
            approval_id: approval.id,
            decision: ApprovalDecision::Approved,
        })
        .await
        .unwrap();
}
