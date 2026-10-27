#[path = "../../decant-backend/tests/support/mod.rs"]
mod support;
use serde_json::{Value, json};
use std::{
    fs,
    io::Write,
    path::Path,
    process::{Command, Output, Stdio},
};
use support::{Reply, Server, TempDir};

const DISCOVERY: &str = r#"[{"uid":"metrics","type":"prometheus"}]"#;
const CONFIGURE: &[&str] = &[
    "investigate",
    "configure",
    "--yes",
    "--request-counter",
    "http_requests_total",
    "--error-label",
    "status",
    "--error-values",
    "500",
    "--duration-bucket",
    "http_duration_seconds_bucket",
    "--duration-unit",
    "seconds",
];
fn run(dir: &Path, args: &[&str], input: &str, credentials: bool) -> Output {
    let copied = dir.join("decant-under-test");
    let mut command = Command::new(if copied.is_file() {
        copied.as_os_str()
    } else {
        std::ffi::OsStr::new(env!("CARGO_BIN_EXE_decant"))
    });
    command
        .env_clear()
        .args(args)
        .current_dir(dir)
        .env_remove("SYNTHETIC_UNSET_MODEL_KEY")
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    if credentials {
        command.env("SYNTHETIC_GRAFANA_TOKEN", "SYNTHETIC_SECRET_TOKEN");
    } else {
        command.env_remove("SYNTHETIC_GRAFANA_TOKEN");
    }
    let mut child = command.spawn().unwrap();
    child
        .stdin
        .take()
        .unwrap()
        .write_all(input.as_bytes())
        .unwrap();
    child.wait_with_output().unwrap()
}
fn metrics() -> Vec<Reply> {
    ["100","10","0.2","0","2","200","10","0.8","0","2"].into_iter().map(|value|Reply::query_json(&json!({"status":"success","data":{"resultType":"vector","result":[{"metric":{},"value":["__TIME__",value]}]},"synthetic_ignored":"SYNTHETIC_PROTECTED_RESPONSE"}).to_string())).collect()
}
struct Case {
    dir: TempDir,
    server: Server,
}
impl Case {
    fn new(replies: Vec<Reply>) -> Self {
        Self::with_sources(replies, false)
    }
    fn with_sources(mut replies: Vec<Reply>, all: bool) -> Self {
        Self::with_binary(&mut replies, all, false)
    }
    fn with_binary(replies: &mut Vec<Reply>, all: bool, copy_binary: bool) -> Self {
        let mut dir = TempDir::new();
        dir.0 = fs::canonicalize(&dir.0).unwrap();
        if copy_binary {
            fs::copy(
                env!("CARGO_BIN_EXE_decant"),
                dir.0.join("decant-under-test"),
            )
            .unwrap();
        }
        replies.insert(0, Reply::json(200, if all {r#"[{"uid":"metrics","type":"prometheus"},{"uid":"traces","type":"tempo"},{"uid":"logs","type":"loki"}]"#}else{DISCOVERY}));
        let server = Server::new(std::mem::take(replies));
        fs::write(dir.0.join("ca.pem"), &server.ca).unwrap();
        let mut args = vec![
            "connect",
            "grafana",
            "--url",
            &server.url,
            "--token-env",
            "SYNTHETIC_GRAFANA_TOKEN",
            "--ca-file",
            "ca.pem",
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
        if all {
            args.extend_from_slice(&["--tempo-uid", "traces", "--loki-uid", "logs"]);
        }
        let out = run(&dir.0, &args, "", true);
        assert!(out.status.success());
        Self { dir, server }
    }
    fn configure(&self) {
        assert!(run(&self.dir.0, CONFIGURE, "", false).status.success());
    }
    fn investigate(&self, extra: &[&str]) -> Output {
        let mut args = vec![
            "investigate",
            "--since",
            "5m",
            "--until",
            "1000",
            "--format",
            "json",
        ];
        args.extend_from_slice(extra);
        run(&self.dir.0, &args, "", true)
    }
    fn bundles(&self) -> Vec<std::path::PathBuf> {
        fs::read_dir(self.dir.0.join(".decant/investigations"))
            .unwrap()
            .map(|e| e.unwrap().path())
            .filter(|p| p.extension().is_some_and(|s| s == "bundle"))
            .collect()
    }
}
fn report(out: &Output) -> Value {
    assert!(
        out.status.success(),
        "CLI failed with safe code {}",
        out.status.code().unwrap_or(-1)
    );
    serde_json::from_slice(&out.stdout).unwrap()
}
fn id(v: &Value) -> &str {
    v["bundle_id"].as_str().unwrap()
}
#[test]
fn ask_question_bounds_consent_and_credentials_precede_backend_calls() {
    let case = Case::new(vec![]);
    case.configure();
    let base = [
        "investigate",
        "ask",
        "--question",
        "Why is checkout slower?",
        "--model",
        "synthetic-model",
        "--model-key-env",
        "SYNTHETIC_UNSET_MODEL_KEY",
    ];
    let denied = run(&case.dir.0, &base, "", true);
    assert_eq!(denied.status.code(), Some(1));
    assert!(denied.stderr == b"error: investigation failed (model_egress_confirmation_required)\n");
    let mut permitted = base.to_vec();
    permitted.push("--allow-model-egress");
    let missing = run(&case.dir.0, &permitted, "", true);
    assert_eq!(missing.status.code(), Some(1));
    assert!(missing.stdout.is_empty());
    assert!(missing.stderr == b"error: investigation failed (credential_unavailable)\n");
    for q in [
        "".to_owned(),
        "SYNTHETIC_QUESTION_SECRET".repeat(60),
        "SYNTHETIC_QUESTION_SECRET\n".to_owned(),
    ] {
        let out = run(
            &case.dir.0,
            &["investigate", "ask", "--question", &q],
            "",
            false,
        );
        assert_eq!(out.status.code(), Some(2));
        assert!(out.stdout.is_empty());
        assert!(out.stderr == b"error: investigation failed (invalid_usage)\n");
    }
    let ambiguous = run(
        &case.dir.0,
        &[
            "investigate",
            "ask",
            "--question",
            "Why slower?",
            "--focus",
            "errors",
        ],
        "",
        false,
    );
    assert_eq!(ambiguous.status.code(), Some(2));
    let id = format!("sha256:v1:{}", "a".repeat(64));
    let stale = run(
        &case.dir.0,
        &[
            "investigate",
            "ask",
            "--question",
            "Why slower?",
            "--bundle",
            &id,
        ],
        "",
        false,
    );
    assert_eq!(stale.status.code(), Some(2)); // Explicit cached time cut is mandatory.
    assert_eq!(case.server.requests.lock().unwrap().len(), 1); // Connect discovery only.
    let help = run(&case.dir.0, &["investigate", "ask", "--help"], "", false);
    assert!(help.status.success());
    assert!(String::from_utf8_lossy(&help.stdout).contains("At most two model calls"));
}
#[test]
fn guided_setup_and_scoped_acquisition_without_json_editing() {
    let case = Case::new(metrics());
    let setup = run(
        &case.dir.0,
        &["investigate", "configure"],
        "http_requests_total\nstatus\n500\nhttp_duration_seconds_bucket\nseconds\nyes\n",
        false,
    );
    assert!(setup.status.success());
    let out = case.investigate(&["--service", "checkout", "--environment", "prod"]);
    let v = report(&out);
    assert_eq!(v["format"], "decant-investigation-run");
    assert_eq!(v["version"], 1);
    assert_eq!(v["reasoning_status"], "not_requested");
    assert_eq!(v["model"], Value::Null);
    assert_eq!(v["backend"]["requests"], 10);
    assert_eq!(
        v["assessment"]["findings"]["latency_p95_direction"],
        "increase"
    );
    assert_eq!(
        v["assessment"]["findings"]["selected_error_fraction_direction"],
        "decrease"
    );
    assert!(out.stderr == format!("evidence saved: {}\n", id(&v)).as_bytes());
    assert!(!String::from_utf8_lossy(&out.stdout).contains("SYNTHETIC_"));
    let requests = case.server.requests.lock().unwrap();
    assert_eq!(requests.len(), 11);
    for (i, request) in requests[1..].iter().enumerate() {
        assert!(request.starts_with("GET /api/datasources/proxy/uid/metrics/api/v1/query?"));
        assert!(request.contains("service%3D%22checkout%22"));
        assert!(request.contains("environment%3D%22prod%22"));
        assert!(request.contains("%5B300s%5D"));
        assert!(request.contains(if i < 5 { "time=700" } else { "time=1000" }));
    }
    let path = case.bundles().pop().unwrap();
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        assert_eq!(
            fs::metadata(&path).unwrap().permissions().mode() & 0o777,
            0o600
        );
        assert_eq!(
            fs::metadata(path.parent().unwrap())
                .unwrap()
                .permissions()
                .mode()
                & 0o777,
            0o700
        );
    }
}

#[test]
fn guided_optional_tempo_and_loki_use_same_cut_without_body_queries() {
    let mut replies = metrics();
    replies.extend(vec![Reply::json(200, r#"{"traces":[]}"#); 2]);
    for (i, count) in ["100", "10", "200", "40"].iter().enumerate() {
        replies.push(Reply::json(200,&json!({"status":"success","data":{"resultType":"vector","result":[{"metric":{},"value":[if i<2{700}else{1000},count]}]}}).to_string()));
    }
    let case = Case::with_sources(replies, true);
    let out = run(
        &case.dir.0,
        &["investigate", "configure"],
        "http_requests_total\nstatus\n500\nhttp_duration_seconds_bucket\nseconds\nyes\nyes\nlevel\nerror\nyes\n",
        false,
    );
    assert!(out.status.success());
    let v = report(&case.investigate(&[]));
    assert_eq!(v["backend"]["requests"], 16);
    assert_eq!(v["assessment"]["checks"][1]["status"], "partial");
    assert_eq!(v["assessment"]["checks"][3]["status"], "checked");
    assert_eq!(
        v["assessment"]["findings"]["selected_error_log_count_direction"],
        "increase"
    );
    let requests = case.server.requests.lock().unwrap();
    assert_eq!(requests.len(), 17);
    assert!(!requests.iter().any(|r| r.contains("query_range")));
    assert!(requests[13].contains("time=700000000000"));
}

#[test]
fn duplicate_store_owner_rejects_before_backend_access() {
    use decant_cli::investigation::BundleStoreV1;
    let case = Case::new(vec![]);
    case.configure();
    let path = case.dir.0.join(".decant/investigations");
    let owner = BundleStoreV1::open(&path).unwrap();
    let out = case.investigate(&[]);
    assert_eq!(out.status.code(), Some(1));
    assert!(out.stdout.is_empty());
    assert!(out.stderr == b"error: investigation failed (bundle_store_in_use)\n");
    assert_eq!(case.server.requests.lock().unwrap().len(), 1);
    assert!(BundleStoreV1::open(&path).is_err());
    drop(owner);
    assert!(BundleStoreV1::open(&path).is_ok());
}

#[cfg(unix)]
#[test]
fn settings_permissions_size_and_symlinks_fail_before_network() {
    use std::os::unix::fs::{PermissionsExt, symlink};
    let case = Case::new(vec![]);
    case.configure();
    let path = case
        .dir
        .0
        .join(".decant/profiles/production.investigation.json");
    let old = fs::read(&path).unwrap();
    fs::set_permissions(&path, fs::Permissions::from_mode(0o644)).unwrap();
    assert_eq!(case.investigate(&[]).status.code(), Some(1));
    fs::set_permissions(&path, fs::Permissions::from_mode(0o600)).unwrap();
    fs::write(&path, vec![b' '; 16385]).unwrap();
    assert_eq!(case.investigate(&[]).status.code(), Some(1));
    fs::write(&path, old).unwrap();
    let moved = path.with_extension("saved");
    fs::rename(&path, &moved).unwrap();
    symlink(&moved, &path).unwrap();
    assert_eq!(case.investigate(&[]).status.code(), Some(1));
    assert_eq!(case.server.requests.lock().unwrap().len(), 1);
}
#[test]
fn offline_followup_needs_no_backend_profile_or_credentials_and_keeps_identity() {
    let case = Case::new(metrics());
    case.configure();
    let v = report(&case.investigate(&[]));
    let bundle = id(&v).to_owned();
    let report_id = v["report_id"].clone();
    let old = fs::read(case.bundles().pop().unwrap()).unwrap();
    let Case { dir, server } = case;
    drop(server);
    // Rename only our synthetic profile directory to prove offline independence.
    fs::rename(
        dir.0.join(".decant/profiles"),
        dir.0.join("profiles-unused"),
    )
    .unwrap();
    let out = run(
        &dir.0,
        &[
            "investigate",
            "follow-up",
            "--bundle",
            &bundle,
            "--focus",
            "errors",
            "--format",
            "json",
        ],
        "",
        false,
    );
    let next = report(&out);
    assert_eq!(next["report_id"], report_id);
    assert_eq!(next["bundle_id"], bundle);
    assert_eq!(next["backend"]["requests"], 0);
    assert_eq!(next["assessment"]["focus"], "errors");
    let md = run(
        &dir.0,
        &["investigate", "follow-up", "--bundle", &bundle],
        "",
        false,
    );
    assert!(md.status.success());
    let md = String::from_utf8(md.stdout).unwrap();
    assert!(md.contains("0.2 | 0.8"));
    assert!(md.contains("TrafficMix: unavailable"));
    assert!(md.contains("not significance or causal proof"));
    let original = run(
        &dir.0,
        &[
            "investigate",
            "report",
            "--bundle",
            &bundle,
            "--format",
            "json",
        ],
        "",
        false,
    );
    assert!(original.status.success());
    let original: Value = serde_json::from_slice(&original.stdout).unwrap();
    assert_eq!(original["format"], "decant-investigation");
    assert_eq!(original["id"], report_id);
    assert!(original["document"]["sections"][0]["facts"]["queries"].is_array());
    let path = dir
        .0
        .join(".decant/investigations")
        .join(format!("{}.bundle", &bundle[10..]));
    assert!(old == fs::read(path).unwrap());
}
#[test]
fn explicit_refresh_requires_later_cut_and_preserves_prior_bytes() {
    let case = Case::new([metrics(), metrics()].concat());
    case.configure();
    let first = report(&case.investigate(&[]));
    let old_path = case.bundles().pop().unwrap();
    let old = fs::read(&old_path).unwrap();
    let rejected = run(
        &case.dir.0,
        &[
            "investigate",
            "refresh",
            "--bundle",
            id(&first),
            "--until",
            "1000",
            "--since",
            "5m",
        ],
        "",
        true,
    );
    assert_eq!(rejected.status.code(), Some(1));
    assert!(rejected.stdout.is_empty());
    assert_eq!(case.server.requests.lock().unwrap().len(), 11);
    let out = run(
        &case.dir.0,
        &[
            "investigate",
            "refresh",
            "--bundle",
            id(&first),
            "--until",
            "1100",
            "--since",
            "5m",
            "--format",
            "json",
        ],
        "",
        true,
    );
    let new = report(&out);
    assert_ne!(new["bundle_id"], first["bundle_id"]);
    assert_ne!(new["report_id"], first["report_id"]);
    assert_eq!(new["parent_report_id"], first["report_id"]);
    assert!(old == fs::read(old_path).unwrap());
    assert_eq!(case.bundles().len(), 2);
}
#[test]
fn reason_requires_explicit_model_and_egress_before_backend_access() {
    let case = Case::new(vec![]);
    case.configure();
    for args in [
        vec!["--reason"],
        vec!["--model", "synthetic-model"],
        vec![
            "--reason",
            "--model",
            "synthetic-model",
            "--model-key-env",
            "SYNTHETIC_UNSET_MODEL_KEY",
        ],
    ] {
        let out = case.investigate(&args);
        assert!(!out.status.success());
        assert!(out.stdout.is_empty());
        assert!(!String::from_utf8_lossy(&out.stderr).contains("SYNTHETIC_"));
    }
    assert_eq!(case.server.requests.lock().unwrap().len(), 1);
}
#[test]
fn unavailable_model_configuration_keeps_saved_evidence_and_returns_partial_status() {
    let case = Case::new(metrics());
    case.configure();
    let out = case.investigate(&[
        "--reason",
        "--model",
        "synthetic-model",
        "--model-key-env",
        "SYNTHETIC_UNSET_MODEL_KEY",
        "--allow-model-egress",
    ]);
    assert_eq!(out.status.code(), Some(1));
    let v: Value = serde_json::from_slice(&out.stdout).unwrap();
    assert_eq!(
        v["reasoning_status"],
        "provider_unavailable_or_budget_exhausted"
    );
    assert_eq!(
        v["assessment"]["findings"]["latency_p95_direction"],
        "increase"
    );
    assert_eq!(case.bundles().len(), 1);
    let follow = run(
        &case.dir.0,
        &[
            "investigate",
            "follow-up",
            "--bundle",
            id(&v),
            "--format",
            "json",
        ],
        "",
        false,
    );
    assert!(follow.status.success());
}
#[test]
fn denied_and_empty_backends_are_reports_with_gaps_not_health_claims() {
    for replies in [
        vec![Reply::json(403, "SYNTHETIC_PROTECTED_RESPONSE"); 10],
        vec![
            Reply::query_json(r#"{"status":"success","data":{"resultType":"vector","result":[]}}"#);
            10
        ],
    ] {
        let case = Case::new(replies);
        case.configure();
        let out = case.investigate(&[]);
        let v = report(&out);
        assert_eq!(
            v["assessment"]["findings"]["latency_p95_direction"],
            "unknown"
        );
        assert_eq!(v["assessment"]["checks"][0]["status"], "unavailable");
        assert_eq!(v["assessment"]["qualifications"]["health_verdict"], false);
        assert!(!String::from_utf8_lossy(&out.stdout).contains("SYNTHETIC_PROTECTED"));
    }
}
#[test]
fn invalid_duplicate_and_oversized_arguments_do_not_access_network() {
    let case = Case::new(vec![]);
    case.configure();
    for extra in [
        vec!["--since", "0m"],
        vec!["--format", "xml"],
        vec!["--service", "other"],
        vec!["--environment", "other"],
        vec!["--arbitrary-query", "SYNTHETIC_PROTECTED"],
        vec!["--until", "-1"],
        vec!["--focus", "shell"],
    ] {
        let out = case.investigate(&extra);
        assert!(!out.status.success());
        assert!(out.stdout.is_empty());
        assert!(!String::from_utf8_lossy(&out.stderr).contains("SYNTHETIC_PROTECTED"));
    }
    let large = "x".repeat(4097);
    let out = run(
        &case.dir.0,
        &["investigate", "--profile", &large],
        "",
        false,
    );
    assert_eq!(out.status.code(), Some(2));
    let out = run(
        &case.dir.0,
        &["investigate", "follow-up", "--bundle", "bad", "--refresh"],
        "",
        false,
    );
    assert_eq!(out.status.code(), Some(2));
    assert_eq!(case.server.requests.lock().unwrap().len(), 1);
}
#[test]
fn configure_is_private_no_overwrite_and_malformed_settings_fail_closed() {
    let case = Case::new(vec![]);
    case.configure();
    let path = case
        .dir
        .0
        .join(".decant/profiles/production.investigation.json");
    let old = fs::read(&path).unwrap();
    assert!(!String::from_utf8_lossy(&old).contains("SYNTHETIC_SECRET_TOKEN"));
    assert_eq!(
        run(&case.dir.0, CONFIGURE, "", false).status.code(),
        Some(1)
    );
    assert!(old == fs::read(&path).unwrap());
    let mut v: Value = serde_json::from_slice(&old).unwrap();
    v["unknown"] = json!("SYNTHETIC_PROTECTED");
    fs::write(&path, serde_json::to_vec(&v).unwrap()).unwrap();
    let out = case.investigate(&[]);
    assert_eq!(out.status.code(), Some(1));
    assert!(out.stdout.is_empty());
    assert!(!String::from_utf8_lossy(&out.stderr).contains("SYNTHETIC_PROTECTED"));
    assert_eq!(case.server.requests.lock().unwrap().len(), 1);
}
#[test]
fn corrupt_or_substituted_bundle_fails_without_fallback_or_mutation() {
    let case = Case::new(metrics());
    case.configure();
    let v = report(&case.investigate(&[]));
    let path = case.bundles().pop().unwrap();
    let wrong = format!("sha256:v1:{}", "f".repeat(64));
    let out = run(
        &case.dir.0,
        &["investigate", "follow-up", "--bundle", &wrong],
        "",
        false,
    );
    assert_eq!(out.status.code(), Some(1));
    assert!(out.stdout.is_empty());
    let mut bytes = fs::read(&path).unwrap();
    bytes.push(0);
    fs::write(&path, &bytes).unwrap();
    let out = run(
        &case.dir.0,
        &["investigate", "follow-up", "--bundle", id(&v)],
        "",
        false,
    );
    assert_eq!(out.status.code(), Some(1));
    assert!(out.stdout.is_empty());
    assert!(bytes == fs::read(path).unwrap());
}
#[test]
fn subcommand_help_is_offline_and_explicit() {
    let dir = TempDir::new();
    for args in [
        vec!["investigate", "--help"],
        vec!["investigate", "configure", "--help"],
        vec!["investigate", "follow-up", "--help"],
        vec!["investigate", "refresh", "--help"],
    ] {
        let out = run(&dir.0, &args, "", false);
        assert!(out.status.success());
        assert!(out.stderr.is_empty());
        let text = String::from_utf8(out.stdout).unwrap();
        assert!(text.contains("--allow-model-egress"));
        assert!(text.contains("No default model"));
        assert!(text.contains("Refresh needs a later cut"));
    }
}
#[cfg(unix)]
#[test]
fn interrupt_cancels_network_and_store_can_be_reopened() {
    cancel_and_reopen("-INT");
}
#[cfg(unix)]
#[test]
fn termination_cancels_network_and_store_can_be_reopened() {
    cancel_and_reopen("-TERM");
}
#[cfg(unix)]
#[test]
fn abrupt_termination_releases_store_without_partial_bundle() {
    cancel_and_reopen("-KILL");
}
#[cfg(unix)]
fn cancel_and_reopen(signal: &str) {
    use std::sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    };
    use std::time::{Duration, Instant};
    let mut first = Reply::query_json(
        r#"{"status":"success","data":{"resultType":"vector","result":[{"metric":{},"value":["__TIME__","1"]}]}}"#,
    );
    let body_gate = Arc::new(AtomicBool::new(false));
    first.body_gate = Some(body_gate.clone());
    let mut replies = vec![first];
    replies.extend(metrics());
    let case = Case::new(replies);
    case.configure();
    let mut child = Command::new(env!("CARGO_BIN_EXE_decant"))
        .args(["investigate", "--since", "5m", "--until", "1000"])
        .current_dir(&case.dir.0)
        .env("SYNTHETIC_GRAFANA_TOKEN", "SYNTHETIC_SECRET_TOKEN")
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    let until = Instant::now() + Duration::from_secs(5);
    while case.server.requests.lock().unwrap().len() < 2 && Instant::now() < until {
        std::thread::sleep(Duration::from_millis(5));
    }
    assert_eq!(case.server.requests.lock().unwrap().len(), 2);
    assert!(
        Command::new("kill")
            .args([signal, &child.id().to_string()])
            .status()
            .unwrap()
            .success()
    );
    let until = Instant::now() + Duration::from_secs(5);
    while child.try_wait().unwrap().is_none() && Instant::now() < until {
        std::thread::sleep(Duration::from_millis(5));
    }
    if child.try_wait().unwrap().is_none() {
        child.kill().unwrap();
        child.wait().unwrap();
        body_gate.store(true, Ordering::Release);
        panic!("interrupted CLI did not exit within the test deadline");
    }
    let out = child.wait_with_output().unwrap();
    if signal == "-KILL" {
        use std::os::unix::process::ExitStatusExt;
        assert_eq!(out.status.signal(), Some(9));
        assert!(out.stderr.is_empty());
    } else {
        assert_eq!(out.status.code(), Some(130));
        assert!(String::from_utf8_lossy(&out.stderr).contains("cancelled"));
    }
    assert!(out.stdout.is_empty());
    assert!(case.bundles().is_empty());
    body_gate.store(true, Ordering::Release);
    let resumed = case.investigate(&[]);
    assert!(resumed.status.success());
    assert_eq!(case.bundles().len(), 1);
}

#[test]
fn copied_binary_completes_guided_capture_and_offline_replay_without_source() {
    let case = Case::with_binary(&mut metrics(), false, true);
    case.configure();
    let captured = report(&case.investigate(&[]));
    let bundle = case.bundles().pop().unwrap();
    let bytes = fs::read(&bundle).unwrap();
    let Case { dir, server } = case;
    drop(server);
    let first = run(
        &dir.0,
        &[
            "investigate",
            "report",
            "--bundle",
            id(&captured),
            "--format",
            "json",
        ],
        "",
        false,
    );
    let second = run(
        &dir.0,
        &[
            "investigate",
            "report",
            "--bundle",
            id(&captured),
            "--format",
            "json",
        ],
        "",
        false,
    );
    assert!(first.status.success() && second.status.success());
    assert!(first.stdout == second.stdout);
    assert!(bytes == fs::read(bundle).unwrap());
    let followup = run(
        &dir.0,
        &[
            "investigate",
            "follow-up",
            "--bundle",
            id(&captured),
            "--focus",
            "gaps",
        ],
        "",
        false,
    );
    assert!(followup.status.success());
}
