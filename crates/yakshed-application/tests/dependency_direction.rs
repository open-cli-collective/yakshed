use std::{collections::HashSet, process::Command};

use serde_json::Value;

#[test]
fn workspace_dependency_direction_is_preserved() {
    let output = Command::new(env!("CARGO"))
        .args(["metadata", "--format-version", "1", "--no-deps"])
        .current_dir(concat!(env!("CARGO_MANIFEST_DIR"), "/../.."))
        .output()
        .expect("cargo metadata should run");
    assert!(output.status.success(), "cargo metadata failed");

    let metadata: Value = serde_json::from_slice(&output.stdout).expect("valid cargo metadata");
    let packages = metadata["packages"].as_array().expect("packages array");
    let internal: HashSet<_> = packages
        .iter()
        .filter_map(|package| package["name"].as_str())
        .collect();

    for package in packages {
        let name = package["name"].as_str().expect("package name");
        let dependencies: HashSet<_> = package["dependencies"]
            .as_array()
            .expect("dependencies array")
            .iter()
            .filter_map(|dependency| dependency["name"].as_str())
            .collect();

        if name == "yakshed-domain" {
            assert!(
                dependencies.is_disjoint(&internal),
                "domain must have zero internal dependencies"
            );
        }
        if matches!(name, "yakshed-domain" | "yakshed-application") {
            for forbidden in ["tauri", "provider-codex", "provider-mock"] {
                assert!(
                    !dependencies.contains(forbidden),
                    "{name} must not depend on {forbidden}"
                );
            }
        }
        if matches!(name, "provider-codex" | "provider-mock") {
            for forbidden in ["yakshed-store", "yakshed-secrets", "yakshed-desktop-api"] {
                assert!(
                    !dependencies.contains(forbidden),
                    "{name} must not depend on {forbidden}"
                );
            }
        }
    }
}
