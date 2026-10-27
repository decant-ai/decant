use std::process::Command;

#[test]
fn version_and_investigation_help_work_without_environment() {
    for args in [
        vec!["--version"],
        vec!["investigate", "--help"],
        vec!["doctor", "--help"],
    ] {
        let out = Command::new(env!("CARGO_BIN_EXE_decant"))
            .args(args)
            .env_clear()
            .output()
            .unwrap();
        assert!(out.status.success());
        assert!(!out.stdout.is_empty());
        assert!(out.stderr.is_empty());
    }
}

#[cfg(not(feature = "full"))]
#[test]
fn investigation_build_help_and_unavailable_commands_are_exact_and_value_safe() {
    let help = Command::new(env!("CARGO_BIN_EXE_decant"))
        .arg("--help")
        .env_clear()
        .output()
        .unwrap();
    assert!(help.status.success());
    assert_eq!(help.stdout, b"Decant read-only investigator (investigation-only build)\n\nUsage:\n  decant connect grafana --help\n  decant doctor --help\n  decant investigate --help\n  decant --help\n  decant --version\n\nFile, streaming and MCP commands require the full build.\n");
    for command in ["evidence", "demo", "ingest", "serve"] {
        let out = Command::new(env!("CARGO_BIN_EXE_decant"))
            .args([command, "SYNTHETIC_PROTECTED"])
            .env_clear()
            .output()
            .unwrap();
        assert_eq!(out.status.code(), Some(2));
        assert!(out.stdout.is_empty());
        assert_eq!(out.stderr, b"error: command unavailable in investigation-only build\nhint: install the full build for file, streaming and MCP commands\n");
    }
    let out = Command::new(env!("CARGO_BIN_EXE_decant"))
        .arg("SYNTHETIC_PROTECTED")
        .env_clear()
        .output()
        .unwrap();
    assert_eq!(out.status.code(), Some(2));
    assert!(out.stdout.is_empty());
    assert_eq!(out.stderr, b"error: invalid usage; run decant --help\n");
}
