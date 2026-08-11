//! Thin Tauri command and event shell over `yakshed_desktop_api::DesktopApi`.

use std::{future::Future, ops::Deref};

use serde::{Deserialize, Serialize};
use tokio::sync::broadcast;
use yakshed_desktop_api::{
    ApiPorts, ApprovalDecisionInput, ApprovalRequestId, ArtifactId, ArtifactListEnvelope,
    ConfigRevision, ConnectionEnvelope, ConnectionId, ConnectionInput, ConnectionListEnvelope,
    DesktopApi, FrontendEvent, FrontendRunSnapshot, OpenArtifactEnvelope,
    PendingUserInputPageEnvelope, ProjectId, RunApprovalPageEnvelope, RunId, SecretWriteEnvelope,
    TimelineItemId, WorkItemId, WorkItemListEnvelope, WorkItemSnapshotEnvelope,
    WorkItemTimelineEnvelope,
};

include!("roster.rs");

pub const FRONTEND_EVENT_NAME: &str = "yakshed:frontend-event";

macro_rules! command_names {
    ($($command:ident),+ $(,)?) => {
        &[ $(stringify!($command)),+ ]
    };
}

pub const COMMANDS: &[&str] = command_roster!(command_names);

pub struct ShellState(DesktopApi);

impl Deref for ShellState {
    type Target = DesktopApi;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct StartupError {
    pub code: StartupErrorCode,
    pub message: String,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum StartupErrorCode {
    PersistenceError,
    InternalError,
}

impl StartupError {
    pub fn persistence(message: impl Into<String>) -> Self {
        Self {
            code: StartupErrorCode::PersistenceError,
            message: message.into(),
        }
    }
}

pub async fn initialize(
    ports: impl Future<Output = std::result::Result<ApiPorts, StartupError>>,
) -> std::result::Result<ShellState, StartupError> {
    Ok(ShellState(DesktopApi::new(ports.await?).await))
}

macro_rules! command {
    ($name:ident($state:ident; $($argument:ident: $type:ty),* $(,)?) -> $output:ty $body:block) => {
        #[cfg(target_os = "macos")]
        #[tauri::command]
        async fn $name(
            $state: tauri::State<'_, ShellState>,
            $($argument: $type),*
        ) -> yakshed_desktop_api::Result<$output> $body

        #[cfg(not(target_os = "macos"))]
        async fn $name(
            $state: &ShellState,
            $($argument: $type),*
        ) -> yakshed_desktop_api::Result<$output> $body
    };
}

command!(create_project(state; id: ProjectId, name: String) -> () {
    state.create_project(id, name).await
});
command!(create_work_item(state; project_id: ProjectId, title: String, parent_id: Option<WorkItemId>) -> WorkItemSnapshotEnvelope {
    state.create_work_item(project_id, title, parent_id).await
});
command!(list_work_items(state; project_id: ProjectId, after: Option<WorkItemId>, limit: u32) -> WorkItemListEnvelope {
    state.list_work_items(project_id, after, limit).await
});
command!(get_work_item_snapshot(state; id: WorkItemId) -> WorkItemSnapshotEnvelope {
    state.get_work_item_snapshot(id).await
});
command!(get_work_item_snapshot_page(state; id: WorkItemId, after: Option<RunId>, limit: u32, expected_revision: Option<u64>) -> WorkItemSnapshotEnvelope {
    state.get_work_item_snapshot_page(id, after, limit, expected_revision).await
});
command!(get_work_item_timeline_page(state; work_item_id: WorkItemId, run_id: RunId, after: Option<u64>, limit: u32) -> WorkItemTimelineEnvelope {
    state.get_work_item_timeline_page(
        work_item_id,
        run_id,
        after.map(yakshed_desktop_api::TimelineRevision::new),
        limit,
    ).await
});
command!(get_work_item_timeline_page_at_revision(state; work_item_id: WorkItemId, run_id: RunId, after: Option<u64>, limit: u32, expected_revision: Option<u64>) -> WorkItemTimelineEnvelope {
    state.get_work_item_timeline_page_at_revision(
        work_item_id,
        run_id,
        after.map(yakshed_desktop_api::TimelineRevision::new),
        limit,
        expected_revision,
    ).await
});
command!(get_run_approval_page(state; work_item_id: WorkItemId, run_id: RunId, after: Option<ApprovalRequestId>, limit: u32, expected_revision: Option<u64>) -> RunApprovalPageEnvelope {
    state.get_run_approval_page(work_item_id, run_id, after, limit, expected_revision).await
});
command!(get_pending_user_input_page(state; work_item_id: WorkItemId, run_id: RunId, after: Option<TimelineItemId>, limit: u32, expected_revision: Option<u64>) -> PendingUserInputPageEnvelope {
    state.get_pending_user_input_page(work_item_id, run_id, after, limit, expected_revision).await
});
command!(start_run(state; work_item_id: WorkItemId, connection_id: ConnectionId, input: String) -> RunId {
    state.start_run(work_item_id, connection_id, input).await
});
command!(steer_run(state; run_id: RunId, message: String) -> () {
    state.steer_run(run_id, message).await
});
command!(interrupt_run(state; run_id: RunId) -> () {
    state.interrupt_run(run_id).await
});
command!(reconcile_run(state; run_id: RunId) -> FrontendRunSnapshot {
    state.reconcile_run(run_id).await
});
command!(resolve_approval(state; approval_id: ApprovalRequestId, decision: ApprovalDecisionInput) -> () {
    state.resolve_approval(approval_id, decision.into()).await
});
command!(respond_user_input(state; request_id: TimelineItemId, response: String) -> () {
    state.respond_user_input(request_id, response).await
});
#[cfg(target_os = "macos")]
#[tauri::command]
async fn connection_put(
    state: tauri::State<'_, ShellState>,
    expected_config_revision: u64,
    connection: ConnectionInput,
    ensure_memory_secret_backend: bool,
) -> yakshed_desktop_api::Result<ConnectionEnvelope> {
    state
        .connection_put(
            ConfigRevision::new(expected_config_revision),
            connection.into_public()?,
            ensure_memory_secret_backend,
        )
        .await
}

#[cfg(not(target_os = "macos"))]
async fn connection_put(
    state: &ShellState,
    expected_config_revision: u64,
    connection: ConnectionInput,
    ensure_memory_secret_backend: bool,
) -> yakshed_desktop_api::Result<ConnectionEnvelope> {
    state
        .connection_put(
            ConfigRevision::new(expected_config_revision),
            connection.into_public()?,
            ensure_memory_secret_backend,
        )
        .await
}
command!(connection_get(state; id: ConnectionId) -> ConnectionEnvelope {
    state.connection_get(id).await
});
command!(list_connections(state;) -> ConnectionListEnvelope {
    state.list_connections().await
});
command!(set_connection_credential(state; connection_id: ConnectionId, slot: yakshed_desktop_api::CredentialSlot, value: String, overwrite: bool) -> SecretWriteEnvelope {
    state.set_connection_credential(connection_id, slot, value, overwrite).await
});
command!(list_artifacts(state; work_item_id: WorkItemId) -> ArtifactListEnvelope {
    state.list_artifacts(work_item_id).await
});
command!(open_artifact(state; work_item_id: WorkItemId, artifact_id: ArtifactId, max_bytes: u64) -> OpenArtifactEnvelope {
    state.open_artifact(work_item_id, artifact_id, max_bytes).await
});
command!(clear_cache(state;) -> () {
    state.clear_cache().await
});

pub async fn forward_events(
    mut events: broadcast::Receiver<FrontendEvent>,
    mut emit: impl FnMut(&FrontendEvent) -> bool,
) {
    loop {
        match events.recv().await {
            Ok(event) if emit(&event) => {}
            Ok(_) | Err(broadcast::error::RecvError::Closed) => break,
            Err(broadcast::error::RecvError::Lagged(_)) => continue,
        }
    }
}

#[cfg(target_os = "macos")]
pub fn configure<R: tauri::Runtime>(
    builder: tauri::Builder<R>,
    state: ShellState,
) -> tauri::Builder<R> {
    use tauri::Emitter;

    let events = state.subscribe_events();
    macro_rules! register {
        ($($command:ident),+ $(,)?) => {
            builder
                .manage(state)
                .invoke_handler(tauri::generate_handler![$($command),+])
        };
    }
    command_roster!(register).setup(move |app| {
        let app = app.handle().clone();
        tauri::async_runtime::spawn(forward_events(events, move |event| {
            app.emit(FRONTEND_EVENT_NAME, event).is_ok()
        }));
        Ok(())
    })
}

#[cfg(target_os = "macos")]
pub fn app_builder(state: ShellState) -> tauri::Builder<tauri::Wry> {
    configure(tauri::Builder::default(), state)
}
