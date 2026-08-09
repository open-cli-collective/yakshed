use std::process::Command;

#[test]
fn version_is_the_only_implemented_invocation() {
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
        let stub = Command::new(binary)
            .args(args)
            .output()
            .expect("contract host should run");
        assert!(!stub.status.success(), "{args:?} unexpectedly succeeded");
        assert_eq!(
            String::from_utf8(stub.stderr).unwrap(),
            "contract host not yet implemented\n"
        );
    }
}
