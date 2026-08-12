use std::collections::BTreeSet;

use serde_json::Value;

const EXPECTED_COMMANDS: &[&str] = &[
    "create_project",
    "create_work_item",
    "list_work_items",
    "get_work_item_snapshot",
    "get_work_item_snapshot_page",
    "get_work_item_timeline_page",
    "get_work_item_timeline_page_at_revision",
    "get_run_approval_page",
    "get_pending_user_input_page",
    "start_run",
    "steer_run",
    "interrupt_run",
    "reconcile_run",
    "resolve_approval",
    "respond_user_input",
    "connection_put",
    "connection_get",
    "list_connections",
    "set_connection_credential",
    "list_artifacts",
    "open_artifact",
    "clear_cache",
];

#[test]
fn registered_command_roster_is_exact() {
    assert_eq!(yakshed_tauri::COMMANDS, EXPECTED_COMMANDS);
}

#[test]
fn hardened_config_is_exact() {
    let config: Value =
        serde_json::from_str(include_str!("../../yakshed-desktop/tauri.conf.json")).unwrap();
    assert_eq!(
        config["build"],
        serde_json::json!({
            "beforeDevCommand": "npm --prefix yakshed-tauri run dev",
            "beforeBuildCommand": "npm --prefix yakshed-tauri run build",
            "devUrl": "http://127.0.0.1:5173",
            "frontendDist": "frontend"
        })
    );
    assert_eq!(config["app"]["withGlobalTauri"], true);
    assert_eq!(config["app"]["windows"].as_array().unwrap().len(), 1);
    assert_eq!(config["app"]["windows"][0]["label"], "main");
    assert_eq!(config["app"]["windows"][0]["devtools"], false);
    assert_eq!(
        config["app"]["security"]["csp"],
        "default-src 'self'; connect-src ipc: http://ipc.localhost; img-src 'self' data:; style-src 'self'; script-src 'self'"
    );
    let capabilities = config["app"]["security"]["capabilities"]
        .as_array()
        .unwrap();
    assert_eq!(capabilities.len(), 1);
    assert_eq!(capabilities[0]["windows"], serde_json::json!(["main"]));
    let actual: BTreeSet<_> = capabilities[0]["permissions"]
        .as_array()
        .unwrap()
        .iter()
        .map(|permission| permission.as_str().unwrap().to_owned())
        .collect();
    let expected: BTreeSet<_> = EXPECTED_COMMANDS
        .iter()
        .map(|command| format!("allow-{}", command.replace('_', "-")))
        .chain([
            "core:event:allow-listen".to_owned(),
            "core:event:allow-unlisten".to_owned(),
        ])
        .collect();
    assert_eq!(actual, expected);
    assert!(
        actual
            .iter()
            .all(|permission| !permission.contains(':') || permission.starts_with("core:event:"))
    );
    assert_eq!(config["bundle"]["active"], true);
    assert_eq!(config["bundle"]["targets"], serde_json::json!(["app"]));
}

#[test]
fn shell_manifest_has_only_desktop_api_as_a_workspace_runtime_dependency() {
    let manifest: toml::Value = toml::from_str(include_str!("../Cargo.toml")).unwrap();
    let dependencies = manifest["dependencies"].as_table().unwrap();
    let workspace_crates: Vec<_> = dependencies
        .iter()
        .filter_map(|(name, value)| {
            value
                .get("workspace")
                .and_then(toml::Value::as_bool)
                .is_some_and(|workspace| workspace)
                .then_some(name.as_str())
        })
        .filter(|name| name.starts_with("yakshed-") || name.starts_with("provider-"))
        .collect();
    assert_eq!(workspace_crates, ["yakshed-desktop-api"]);
}

#[test]
fn desktop_manifest_is_the_only_full_graph_composition_root() {
    let manifest: toml::Value =
        toml::from_str(include_str!("../../yakshed-desktop/Cargo.toml")).unwrap();
    let dependencies = manifest["target"]["cfg(target_os = \"macos\")"]["dependencies"]
        .as_table()
        .unwrap();
    let actual: BTreeSet<_> = dependencies
        .keys()
        .filter(|name| name.starts_with("yakshed-") || name.starts_with("provider-"))
        .map(String::as_str)
        .collect();
    let expected = BTreeSet::from([
        "provider-codex",
        "yakshed-application",
        "yakshed-desktop-api",
        "yakshed-domain",
        "yakshed-harness",
        "yakshed-secrets",
        "yakshed-store",
        "yakshed-tauri",
    ]);
    assert_eq!(actual, expected);
}
