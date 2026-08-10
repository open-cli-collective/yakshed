use std::process::Command;

#[test]
fn version_is_state_free_and_invalid_launches_fail_without_stdout() {
    let binary = env!("CARGO_BIN_EXE_yakshed-contract-host");
    let version = Command::new(binary)
        .arg("--version")
        .output()
        .expect("contract host should run");
    assert!(version.status.success());
    assert_eq!(
        String::from_utf8(version.stdout).unwrap(),
        format!("yakshed-contract-host {}\n", env!("CARGO_PKG_VERSION"))
    );

    for args in [&[][..], &["--version", "extra"][..], &["--frobnicate"][..]] {
        let rejected = Command::new(binary)
            .args(args)
            .output()
            .expect("contract host should run");
        assert!(
            !rejected.status.success(),
            "{args:?} unexpectedly succeeded"
        );
        assert!(rejected.stdout.is_empty());
        assert!(
            String::from_utf8(rejected.stderr)
                .unwrap()
                .starts_with("contract host argument error:")
        );
    }
}
