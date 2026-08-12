use std::{
    collections::{HashMap, HashSet, VecDeque},
    path::Path,
    process::Command,
};

use serde_json::Value;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Layer {
    Domain,
    Application,
    Infra,
    Provider,
    DesktopApi,
    Tauri,
    Desktop,
    Tools,
}

struct Package {
    name: String,
    layer: Option<Layer>,
    dependencies: Vec<String>,
}

struct Graph {
    packages: HashMap<String, Package>,
}

impl Graph {
    fn from_metadata(metadata: &Value) -> Self {
        let workspace_members: HashSet<_> = metadata["workspace_members"]
            .as_array()
            .expect("workspace_members array")
            .iter()
            .map(|id| id.as_str().expect("workspace package ID"))
            .collect();
        let workspace_root =
            Path::new(metadata["workspace_root"].as_str().expect("workspace root"));
        let resolved_dependencies: HashMap<_, _> = metadata["resolve"]["nodes"]
            .as_array()
            .expect("resolve nodes")
            .iter()
            .map(|node| {
                let id = node["id"].as_str().expect("resolved package ID");
                let dependencies: Vec<(String, String)> = node["deps"]
                    .as_array()
                    .expect("resolved dependencies")
                    .iter()
                    .map(|dependency| {
                        (
                            dependency["name"]
                                .as_str()
                                .expect("resolved dependency name")
                                .to_owned(),
                            dependency["pkg"]
                                .as_str()
                                .expect("resolved dependency ID")
                                .to_owned(),
                        )
                    })
                    .collect();
                (id, dependencies)
            })
            .collect();

        let packages = metadata["packages"]
            .as_array()
            .expect("packages array")
            .iter()
            .map(|package| {
                let id = package["id"].as_str().expect("package ID");
                let name = package["name"].as_str().expect("package name");
                let production_dependency_names: HashSet<_> = package["dependencies"]
                    .as_array()
                    .expect("package dependencies")
                    .iter()
                    .filter(|dependency| dependency["kind"].as_str() != Some("dev"))
                    .map(|dependency| {
                        dependency["rename"]
                            .as_str()
                            .or_else(|| dependency["name"].as_str())
                            .expect("dependency name")
                    })
                    .collect();
                let layer = workspace_members.contains(id).then(|| {
                    classify(
                        name,
                        Path::new(
                            package["manifest_path"]
                                .as_str()
                                .expect("package manifest path"),
                        )
                        .strip_prefix(workspace_root)
                        .expect("workspace manifest path"),
                    )
                });
                (
                    id.to_owned(),
                    Package {
                        name: name.to_owned(),
                        layer,
                        dependencies: resolved_dependencies
                            .get(id)
                            .expect("package in resolve graph")
                            .iter()
                            .filter(|(name, _)| production_dependency_names.contains(name.as_str()))
                            .map(|(_, id)| id.clone())
                            .collect(),
                    },
                )
            })
            .collect();

        Self { packages }
    }

    fn find_path(
        &self,
        start: &str,
        forbidden: impl Fn(&str, &Package) -> bool,
    ) -> Option<Vec<String>> {
        let mut queue = VecDeque::from([start.to_owned()]);
        let mut parents = HashMap::from([(start.to_owned(), None::<String>)]);

        while let Some(id) = queue.pop_front() {
            for dependency in &self.packages[&id].dependencies {
                if parents.contains_key(dependency) {
                    continue;
                }
                parents.insert(dependency.clone(), Some(id.clone()));
                if forbidden(dependency, &self.packages[dependency]) {
                    let mut path = vec![dependency.clone()];
                    while let Some(Some(parent)) = parents.get(path.last().unwrap()) {
                        path.push(parent.clone());
                    }
                    path.reverse();
                    return Some(path);
                }
                queue.push_back(dependency.clone());
            }
        }
        None
    }

    fn display_path(&self, path: &[String]) -> String {
        path.iter()
            .map(|id| self.packages[id].name.as_str())
            .collect::<Vec<_>>()
            .join(" -> ")
    }

    fn violations(&self) -> Vec<String> {
        let mut violations = Vec::new();

        for (id, package) in self
            .packages
            .iter()
            .filter(|(_, package)| package.layer.is_some())
        {
            let source = package.layer.unwrap();
            if let Some(path) = self.find_path(id, |_, target| {
                target
                    .layer
                    .is_some_and(|target| !allows_reachable_layer(source, target))
            }) {
                violations.push(format!(
                    "forbidden layer dependency: {}",
                    self.display_path(&path)
                ));
            }

            if source == Layer::Provider
                && let Some(path) = self.find_path(id, |_, target| {
                    matches!(
                        target.name.as_str(),
                        "yakshed-store" | "yakshed-secrets" | "yakshed-desktop-api"
                    )
                })
            {
                violations.push(format!(
                    "provider reaches forbidden infrastructure: {}",
                    self.display_path(&path)
                ));
            }

            if matches!(source, Layer::Domain | Layer::Application)
                && let Some(path) = self.find_path(id, |_, target| target.name == "tauri")
            {
                violations.push(format!(
                    "core layer reaches Tauri: {}",
                    self.display_path(&path)
                ));
            }

            if !matches!(source, Layer::Tauri | Layer::Desktop)
                && let Some(path) = self.find_path(id, |_, target| target.name == "tauri")
            {
                violations.push(format!(
                    "pre-existing package reaches Tauri: {}",
                    self.display_path(&path)
                ));
            }

            if source == Layer::Tauri {
                for dependency in &package.dependencies {
                    let target = &self.packages[dependency];
                    if target.layer.is_some() && target.layer != Some(Layer::DesktopApi) {
                        violations.push(format!(
                            "Tauri shell directly depends on workspace package: {} -> {}",
                            package.name, target.name
                        ));
                    }
                }
            }

            if source != Layer::Desktop
                && let Some(path) = self.find_path(id, |_, target| {
                    matches!(
                        target.name.as_str(),
                        "provider-codex" | "yakshed-store" | "yakshed-secrets"
                    )
                })
                && !matches!(source, Layer::Provider | Layer::Infra | Layer::Tools)
            {
                violations.push(format!(
                    "non-composition package reaches production graph: {}",
                    self.display_path(&path)
                ));
            }
        }

        violations
    }
}

fn classify(name: &str, manifest_path: &Path) -> Layer {
    match name {
        "yakshed-domain" => Layer::Domain,
        "yakshed-application" => Layer::Application,
        "yakshed-store" | "yakshed-secrets" | "yakshed-harness" => Layer::Infra,
        name if name.starts_with("provider-") => Layer::Provider,
        "yakshed-desktop-api" => Layer::DesktopApi,
        "yakshed-tauri" => Layer::Tauri,
        "yakshed-desktop" => Layer::Desktop,
        _ if manifest_path.starts_with("tools") => Layer::Tools,
        _ => panic!("unclassified workspace package: {name}"),
    }
}

fn allows_reachable_layer(source: Layer, target: Layer) -> bool {
    match source {
        Layer::Domain => false,
        Layer::Application => target == Layer::Domain,
        Layer::Infra => matches!(target, Layer::Domain | Layer::Application),
        Layer::Provider => matches!(target, Layer::Domain | Layer::Application | Layer::Infra),
        Layer::DesktopApi => matches!(target, Layer::Domain | Layer::Application),
        Layer::Tauri => matches!(
            target,
            Layer::DesktopApi | Layer::Application | Layer::Domain
        ),
        Layer::Desktop => target != Layer::Tools,
        Layer::Tools => target != Layer::Tools,
    }
}

#[test]
fn workspace_dependency_direction_is_preserved() {
    let output = Command::new(env!("CARGO"))
        .args(["metadata", "--format-version", "1", "--locked"])
        .current_dir(concat!(env!("CARGO_MANIFEST_DIR"), "/../.."))
        .output()
        .expect("cargo metadata should run");
    assert!(
        output.status.success(),
        "cargo metadata failed:\n{}",
        String::from_utf8_lossy(&output.stderr)
    );

    let graph = Graph::from_metadata(
        &serde_json::from_slice(&output.stdout).expect("valid cargo metadata"),
    );
    let violations = graph.violations();
    assert!(
        violations.is_empty(),
        "forbidden dependency paths:\n{}",
        violations.join("\n")
    );
}

#[cfg(test)]
mod mutation_checks {
    use super::*;

    fn graph(nodes: &[(&str, Option<Layer>, &[&str])]) -> Graph {
        Graph {
            packages: nodes
                .iter()
                .map(|(name, layer, dependencies)| {
                    (
                        (*name).to_owned(),
                        Package {
                            name: (*name).to_owned(),
                            layer: *layer,
                            dependencies: dependencies
                                .iter()
                                .map(|dependency| (*dependency).to_owned())
                                .collect(),
                        },
                    )
                })
                .collect(),
        }
    }

    #[test]
    fn catches_domain_reaching_internal_package_transitively() {
        let graph = graph(&[
            ("yakshed-domain", Some(Layer::Domain), &["bridge"]),
            ("bridge", None, &["yakshed-store"]),
            ("yakshed-store", Some(Layer::Infra), &[]),
        ]);

        assert!(
            graph
                .violations()
                .iter()
                .any(|violation| violation.contains("yakshed-domain -> bridge -> yakshed-store"))
        );
    }

    #[test]
    fn catches_application_reaching_tauri_transitively() {
        let graph = graph(&[
            (
                "yakshed-application",
                Some(Layer::Application),
                &["some-adapter"],
            ),
            ("some-adapter", None, &["tauri"]),
            ("tauri", None, &[]),
        ]);

        assert!(graph.violations().iter().any(|violation| violation
            .contains("yakshed-application -> some-adapter -> tauri")));
    }

    #[test]
    fn catches_future_provider_reaching_store_transitively() {
        let graph = graph(&[
            ("provider-claude", Some(Layer::Provider), &["some-adapter"]),
            ("some-adapter", None, &["yakshed-store"]),
            ("yakshed-store", Some(Layer::Infra), &[]),
        ]);

        assert!(graph.violations().iter().any(|violation| {
            violation.contains("provider-claude -> some-adapter -> yakshed-store")
        }));
    }

    #[test]
    fn catches_tauri_leaking_into_any_pre_existing_crate() {
        let graph = graph(&[
            ("yakshed-desktop-api", Some(Layer::DesktopApi), &["tauri"]),
            ("tauri", None, &[]),
        ]);

        assert!(
            graph
                .violations()
                .iter()
                .any(|violation| violation.contains("yakshed-desktop-api -> tauri"))
        );
    }

    #[test]
    fn catches_tauri_shell_direct_workspace_dependency_bypass() {
        let graph = graph(&[
            ("yakshed-tauri", Some(Layer::Tauri), &["yakshed-store"]),
            ("yakshed-store", Some(Layer::Infra), &[]),
        ]);

        assert!(graph.violations().iter().any(|violation| violation
            .contains("Tauri shell directly depends on workspace package: yakshed-tauri -> yakshed-store")));
    }
}
