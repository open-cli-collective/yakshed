include!("src/roster.rs");

#[cfg(target_os = "macos")]
fn main() {
    macro_rules! build {
        ($($command:ident),+ $(,)?) => {
            tauri_build::try_build(
                tauri_build::Attributes::new().app_manifest(
                    tauri_build::AppManifest::new().commands(&[$(stringify!($command)),+]),
                ),
            )
            .expect("Tauri build configuration should be valid");
        };
    }
    command_roster!(build);
}

#[cfg(not(target_os = "macos"))]
fn main() {}
