#[path = "../../decant-backend/tests/support/mod.rs"]
mod support;
use std::{
    fs,
    io::Write,
    process::{Command, Output, Stdio},
};
use support::{Reply, Server, TempDir};

const DISCOVERY: &str = r#"[{"uid":"metrics","type":"prometheus"},{"uid":"traces","type":"tempo"},{"uid":"logs","type":"loki"}]"#;
struct Case {
    dir: TempDir,
    server: Server,
}
impl Case {
    fn new(replies: Vec<Reply>) -> Self {
        let mut dir = TempDir::new();
        dir.0 = fs::canonicalize(&dir.0).unwrap();
        let server = Server::new(replies);
        fs::write(dir.0.join("ca.pem"), &server.ca).unwrap();
        Self { dir, server }
    }
    fn run(&self, args: &[&str], input: &str) -> Output {
        let mut command = Command::new(env!("CARGO_BIN_EXE_decant"));
        command
            .args(args)
            .current_dir(&self.dir.0)
            .env("SYNTHETIC_GRAFANA_TOKEN", "SYNTHETIC_SECRET_TOKEN")
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
        let mut child = command.spawn().unwrap();
        child
            .stdin
            .take()
            .unwrap()
            .write_all(input.as_bytes())
            .unwrap();
        child.wait_with_output().unwrap()
    }
    fn connect(&self, extra: &[&str], input: &str) -> Output {
        let mut args = vec![
            "connect",
            "grafana",
            "--url",
            &self.server.url,
            "--token-env",
            "SYNTHETIC_GRAFANA_TOKEN",
            "--ca-file",
            "ca.pem",
        ];
        args.extend_from_slice(extra);
        self.run(&args, input)
    }
    fn profile(&self) -> std::path::PathBuf {
        self.dir.0.join(".decant/profiles/production.json")
    }
}
const EXPLICIT: &[&str] = &[
    "--yes",
    "--prometheus-uid",
    "metrics",
    "--service-label",
    "service",
    "--service",
    "checkout",
    "--environment-label",
    "environment",
    "--environment",
    "prod",
];

#[test]
fn guided_setup_and_query_diagnostics_work_without_json_editing() {
    let case = Case::new(vec![
        Reply::json(200, DISCOVERY),
        Reply::query_json(
            r#"{"status":"success","data":{"resultType":"vector","result":[{"metric":{},"value":["__TIME__","2"]}]}}"#,
        ),
    ]);
    let output = case.connect(
        &[],
        "metrics\n\n\nservice\ncheckout\nenvironment\nprod\nyes\n",
    );
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let profile = fs::read(case.profile()).unwrap();
    assert!(!String::from_utf8_lossy(&profile).contains("SYNTHETIC_SECRET_TOKEN"));
    assert!(String::from_utf8_lossy(&profile).contains("SYNTHETIC_GRAFANA_TOKEN"));
    let doctor = case.run(&["doctor", "--profile", "production"], "");
    assert!(
        doctor.status.success(),
        "{}",
        String::from_utf8_lossy(&doctor.stderr)
    );
    let report: serde_json::Value = serde_json::from_slice(&doctor.stdout).unwrap();
    assert_eq!(report["ready"], true);
    assert_eq!(report["sources"][1]["status"], "not_configured");
    assert!(doctor.stderr.is_empty());
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        assert_eq!(
            fs::metadata(case.profile()).unwrap().permissions().mode() & 0o777,
            0o600
        );
        assert_eq!(
            fs::metadata(case.profile().parent().unwrap())
                .unwrap()
                .permissions()
                .mode()
                & 0o777,
            0o700
        );
    }
}
#[test]
fn explicit_setup_and_denied_doctor_have_safe_output_and_no_overwrite() {
    let case = Case::new(vec![
        Reply::json(200, DISCOVERY),
        Reply::json(403, "SYNTHETIC_SECRET_BODY"),
        Reply::json(200, DISCOVERY),
    ]);
    assert!(case.connect(EXPLICIT, "").status.success());
    let saved = fs::read(case.profile()).unwrap();
    let doctor = case.run(&["doctor", "--profile", "production"], "");
    assert_eq!(doctor.status.code(), Some(1));
    let report: serde_json::Value = serde_json::from_slice(&doctor.stdout).unwrap();
    assert_eq!(report["sources"][0]["reason"], "permission_denied");
    assert!(!String::from_utf8_lossy(&doctor.stdout).contains("SYNTHETIC_SECRET"));
    assert!(doctor.stderr.is_empty());
    let duplicate = case.connect(EXPLICIT, "");
    assert_eq!(duplicate.status.code(), Some(1));
    assert!(duplicate.stdout.is_empty());
    assert_eq!(fs::read(case.profile()).unwrap(), saved);
}
#[test]
fn confirmation_incomplete_arguments_eof_and_scope_errors_do_not_save() {
    let case = Case::new(vec![
        Reply::json(200, DISCOVERY),
        Reply::json(200, DISCOVERY),
    ]);
    assert!(!case.connect(&["--yes"], "").status.success());
    assert!(case.server.requests.lock().unwrap().is_empty());
    assert!(
        !case
            .connect(
                &[],
                "metrics\n\n\nservice\ncheckout\nenvironment\nprod\nno\n"
            )
            .status
            .success()
    );
    assert!(!case.connect(&[], "").status.success());
    assert!(!case.profile().exists());
    let invalid = case.run(&["doctor", "--profile", "../SYNTHETIC_SECRET"], "");
    assert!(invalid.stdout.is_empty());
    assert!(!String::from_utf8_lossy(&invalid.stderr).contains("SYNTHETIC_SECRET"));
}
#[test]
fn profile_symlink_permissions_and_oversize_fail_before_network() {
    let case = Case::new(vec![Reply::json(200, DISCOVERY)]);
    assert!(case.connect(EXPLICIT, "").status.success());
    fs::write(case.profile(), vec![b'x'; 16 * 1024 + 1]).unwrap();
    assert!(!case.run(&["doctor"], "").status.success());
    #[cfg(unix)]
    {
        use std::os::unix::fs::{PermissionsExt, symlink};
        let original = case.profile();
        let backup = case.dir.0.join("profile-backup.json");
        fs::rename(&original, &backup).unwrap();
        symlink(&backup, &original).unwrap();
        assert!(!case.run(&["doctor"], "").status.success());
        fs::set_permissions(
            original.parent().unwrap(),
            fs::Permissions::from_mode(0o755),
        )
        .unwrap();
        assert!(!case.run(&["doctor"], "").status.success());
    }
    assert_eq!(case.server.requests.lock().unwrap().len(), 1);
}
#[test]
fn help_is_offline_and_unknown_options_do_not_echo_values() {
    let case = Case::new(vec![]);
    for args in [
        &["connect", "grafana", "--help"][..],
        &["doctor", "--help"][..],
    ] {
        assert!(case.run(args, "").status.success());
    }
    let out = case.run(&["doctor", "--token", "SYNTHETIC_SECRET"], "");
    assert_eq!(out.status.code(), Some(2));
    assert!(!String::from_utf8_lossy(&out.stderr).contains("SYNTHETIC_SECRET"));
    assert!(case.server.requests.lock().unwrap().is_empty());
}
