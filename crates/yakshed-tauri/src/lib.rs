//! Thin Tauri command and event shell over `yakshed_desktop_api::DesktopApi`.

include!("roster.rs");

macro_rules! command_names {
    ($($command:ident),+ $(,)?) => {
        &[ $(stringify!($command)),+ ]
    };
}

pub const COMMANDS: &[&str] = command_roster!(command_names);

#[cfg(target_os = "macos")]
mod macos;

#[cfg(target_os = "macos")]
pub use macos::*;
