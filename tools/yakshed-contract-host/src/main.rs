//! Test-only composition host for the application, storage, secrets, desktop facade, and mock harness.

use std::process::ExitCode;

fn main() -> ExitCode {
    let mut args = std::env::args().skip(1);
    if matches!(
        (args.next().as_deref(), args.next()),
        (Some("--version"), None)
    ) {
        println!("{} {}", env!("CARGO_PKG_NAME"), env!("CARGO_PKG_VERSION"));
        ExitCode::SUCCESS
    } else {
        eprintln!("contract host not yet implemented");
        ExitCode::FAILURE
    }
}
