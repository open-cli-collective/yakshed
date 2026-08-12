use std::{future::Future, ops::Deref};

use tokio::sync::broadcast;
use yakshed_desktop_api::{
    ApiPorts, ApprovalDecisionInput, ArtifactListEnvelope, ConfigRevision, ConnectionEnvelope,
    ConnectionInput, ConnectionListEnvelope, CredentialSlot, DesktopApi, DesktopErrorCode,
    FrontendAccountStatus, FrontendEvent, FrontendRunSnapshot, OpenArtifactEnvelope,
    PendingUserInputPageEnvelope, RunApprovalPageEnvelope, RunId, SecretWriteEnvelope,
    WorkItemListEnvelope, WorkItemSnapshotEnvelope, WorkItemTimelineEnvelope,
};

pub use yakshed_desktop_api::{StartupError, StartupErrorCode};

pub const FRONTEND_EVENT_NAME: &str = "yakshed:frontend-event";

pub struct ShellState(DesktopApi);

impl Deref for ShellState {
    type Target = DesktopApi;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

pub async fn initialize(
    ports: impl Future<Output = std::result::Result<ApiPorts, StartupError>>,
) -> std::result::Result<ShellState, StartupError> {
    let api = DesktopApi::new(ports.await?).await?;
    Ok(ShellState(api))
}

macro_rules! command {
    ($name:ident($state:ident; $($argument:ident: $type:ty),* $(,)?) -> $output:ty $body:block) => {
        #[tauri::command]
        async fn $name(
            $state: tauri::State<'_, ShellState>,
            $($argument: $type),*
        ) -> yakshed_desktop_api::Result<$output> $body
    };
}

fn invalid_request(detail: String) -> yakshed_desktop_api::DesktopError {
    yakshed_desktop_api::DesktopError {
        code: DesktopErrorCode::InvalidRequest,
        message: "invalid request argument",
        detail: Some(detail),
    }
}

fn parse_domain_id<T>(field: &'static str, raw: String) -> yakshed_desktop_api::Result<T>
where
    T: std::str::FromStr,
    <T as std::str::FromStr>::Err: ToString,
{
    raw.parse::<T>()
        .map_err(|error| invalid_request(format!("{field}: {}", error.to_string())))
}

fn parse_approval_decision(value: String) -> yakshed_desktop_api::Result<ApprovalDecisionInput> {
    match value.as_str() {
        "approved" => Ok(ApprovalDecisionInput::Approved),
        "denied" => Ok(ApprovalDecisionInput::Denied),
        _ => Err(invalid_request(format!("decision: {value}"))),
    }
}

command!(create_project(state; id: String, name: String) -> () {
    state.create_project(parse_domain_id("id", id)?, name).await
});
command!(create_work_item(state; project_id: String, title: String, parent_id: Option<String>) -> WorkItemSnapshotEnvelope {
    state
        .create_work_item(
            parse_domain_id("project_id", project_id)?,
            title,
            parent_id
                .map(|raw| parse_domain_id("parent_id", raw))
                .transpose()?,
        )
        .await
});
command!(list_work_items(state; project_id: String, after: Option<String>, limit: u32) -> WorkItemListEnvelope {
    state
        .list_work_items(
            parse_domain_id("project_id", project_id)?,
            after.map(|raw| parse_domain_id("after", raw)).transpose()?,
            limit,
        )
        .await
});
command!(get_work_item_snapshot(state; id: String) -> WorkItemSnapshotEnvelope {
    state.get_work_item_snapshot(parse_domain_id("id", id)?).await
});
command!(get_work_item_snapshot_page(state; id: String, after: Option<String>, limit: u32, expected_revision: Option<u64>) -> WorkItemSnapshotEnvelope {
    state
        .get_work_item_snapshot_page(
            parse_domain_id("id", id)?,
            after.map(|raw| parse_domain_id("after", raw)).transpose()?,
            limit,
            expected_revision,
        )
        .await
});
command!(get_work_item_timeline_page(state; work_item_id: String, run_id: String, after: Option<u64>, limit: u32) -> WorkItemTimelineEnvelope {
    let work_item_id = parse_domain_id("work_item_id", work_item_id)?;
    let run_id = parse_domain_id("run_id", run_id)?;
    state.get_work_item_timeline_page(
        work_item_id,
        run_id,
        after.map(yakshed_desktop_api::TimelineRevision::new),
        limit,
    ).await
});
command!(get_work_item_timeline_page_at_revision(state; work_item_id: String, run_id: String, after: Option<u64>, limit: u32, expected_revision: Option<u64>) -> WorkItemTimelineEnvelope {
    let work_item_id = parse_domain_id("work_item_id", work_item_id)?;
    let run_id = parse_domain_id("run_id", run_id)?;
    state.get_work_item_timeline_page_at_revision(
        work_item_id,
        run_id,
        after.map(yakshed_desktop_api::TimelineRevision::new),
        limit,
        expected_revision,
    ).await
});
command!(get_run_approval_page(state; work_item_id: String, run_id: String, after: Option<String>, limit: u32, expected_revision: Option<u64>) -> RunApprovalPageEnvelope {
    state
        .get_run_approval_page(
            parse_domain_id("work_item_id", work_item_id)?,
            parse_domain_id("run_id", run_id)?,
            after.map(|raw| parse_domain_id("after", raw)).transpose()?,
            limit,
            expected_revision,
        )
        .await
});
command!(get_pending_user_input_page(state; work_item_id: String, run_id: String, after: Option<String>, limit: u32, expected_revision: Option<u64>) -> PendingUserInputPageEnvelope {
    state
        .get_pending_user_input_page(
            parse_domain_id("work_item_id", work_item_id)?,
            parse_domain_id("run_id", run_id)?,
            after.map(|raw| parse_domain_id("after", raw)).transpose()?,
            limit,
            expected_revision,
        )
        .await
});
command!(start_run(state; work_item_id: String, connection_id: String, input: String) -> RunId {
    state
        .start_run(
            parse_domain_id("work_item_id", work_item_id)?,
            parse_domain_id("connection_id", connection_id)?,
            input,
        )
        .await
});
command!(steer_run(state; run_id: String, message: String) -> () {
    state.steer_run(parse_domain_id("run_id", run_id)?, message).await
});
command!(interrupt_run(state; run_id: String) -> () {
    state.interrupt_run(parse_domain_id("run_id", run_id)?).await
});
command!(reconcile_run(state; run_id: String) -> FrontendRunSnapshot {
    state.reconcile_run(parse_domain_id("run_id", run_id)?).await
});
command!(resolve_approval(state; approval_id: String, decision: String) -> () {
    state
        .resolve_approval(
            parse_domain_id("approval_id", approval_id)?,
            parse_approval_decision(decision)?.into(),
        )
        .await
});
command!(respond_user_input(state; request_id: String, response: String) -> () {
    state
        .respond_user_input(parse_domain_id("request_id", request_id)?, response)
        .await
});
#[tauri::command]
async fn connection_put(
    state: tauri::State<'_, ShellState>,
    expected_config_revision: u64,
    connection: ConnectionInput,
) -> yakshed_desktop_api::Result<ConnectionEnvelope> {
    state
        .connection_put(
            ConfigRevision::new(expected_config_revision),
            connection.into_public()?,
        )
        .await
}
command!(connection_get(state; id: String) -> ConnectionEnvelope {
    state.connection_get(parse_domain_id("id", id)?).await
});
command!(list_connections(state;) -> ConnectionListEnvelope {
    state.list_connections().await
});
command!(set_connection_credential(
    state;
    connection_id: String,
    slot: String,
    value: String,
    overwrite: bool
) -> SecretWriteEnvelope {
    state
        .set_connection_credential(
            parse_domain_id("connection_id", connection_id)?,
            CredentialSlot::new(slot).map_err(|error| invalid_request(format!("slot: {error}")))?,
            value,
            overwrite,
        )
        .await
    });
command!(account_status(state; connection_id: String) -> FrontendAccountStatus {
    state.account_status(parse_domain_id("connection_id", connection_id)?).await
});
command!(account_login_start(state; connection_id: String) -> FrontendAccountStatus {
    state.account_login_start(parse_domain_id("connection_id", connection_id)?).await
});
command!(account_logout(state; connection_id: String) -> () {
    state.account_logout(parse_domain_id("connection_id", connection_id)?).await
});
command!(list_artifacts(state; work_item_id: String) -> ArtifactListEnvelope {
    state
        .list_artifacts(parse_domain_id("work_item_id", work_item_id)?)
        .await
});
command!(open_artifact(state; work_item_id: String, artifact_id: String, max_bytes: u64) -> OpenArtifactEnvelope {
    state
        .open_artifact(
            parse_domain_id("work_item_id", work_item_id)?,
            parse_domain_id("artifact_id", artifact_id)?,
            max_bytes,
        )
        .await
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

pub fn app_builder(state: ShellState) -> tauri::Builder<tauri::Wry> {
    configure(tauri::Builder::default(), state)
}
