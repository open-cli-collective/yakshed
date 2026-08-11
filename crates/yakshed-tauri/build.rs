#[cfg(target_os = "macos")]
include!("src/roster.rs");

fn main() {
    let target_os = std::env::var("CARGO_CFG_TARGET_OS").ok();
    if target_os.as_deref() != Some("macos") {
        return;
    }

    #[cfg(target_os = "macos")]
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
    #[cfg(target_os = "macos")]
    command_roster!(build);
}
