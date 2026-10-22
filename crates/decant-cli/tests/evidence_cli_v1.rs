use std::{
    fs,
    path::PathBuf,
    process::{Command, Output},
    sync::{
        Mutex, MutexGuard,
        atomic::{AtomicUsize, Ordering},
    },
};
static NEXT: AtomicUsize = AtomicUsize::new(0);
// Linux can reject exec with ETXTBSY when a concurrent process spawn briefly
// inherits another test's copy-in-progress writable descriptor. Keep each
// copied-binary fixture and all of its child processes in one ownership scope;
// do not retry failed execs or weaken the empty-environment/process assertions.
static PROCESS_FIXTURE_OWNER: Mutex<()> = Mutex::new(());
struct Case {
    root: PathBuf,
    _process_owner: MutexGuard<'static, ()>,
}
impl Case {
    fn new() -> Self {
        let process_owner = PROCESS_FIXTURE_OWNER
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let root = std::env::temp_dir().join(format!(
            "decant-evidence-cli-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        fs::create_dir(&root).unwrap();
        fs::copy(env!("CARGO_BIN_EXE_decant"), root.join("decant")).unwrap();
        fs::write(
            root.join("capture.jsonl"),
            include_bytes!("../../../examples/evidence/trace-capture-v1.jsonl"),
        )
        .unwrap();
        Self {
            root,
            _process_owner: process_owner,
        }
    }
    fn run(&self, args: &[&str]) -> Output {
        Command::new(self.root.join("decant"))
            .arg("evidence")
            .args(args)
            .current_dir(&self.root)
            .env_clear()
            .output()
            .unwrap()
    }
    fn init(&self) {
        let out = self.run(&["init"]);
        assert!(out.status.success());
        assert!(out.stderr.is_empty());
        fs::write(self.root.join("config.json"), out.stdout).unwrap();
    }
}
impl Drop for Case {
    fn drop(&mut self) {
        fs::remove_dir_all(&self.root).unwrap();
    }
}

#[test]
fn copied_binary_fixture_holds_process_ownership_through_execution() {
    let case = Case::new();
    assert!(PROCESS_FIXTURE_OWNER.try_lock().is_err());
    assert!(case.run(&["--help"]).status.success());
    assert!(PROCESS_FIXTURE_OWNER.try_lock().is_err());
}

#[test]
fn copied_binary_runs_own_data_flow_with_empty_environment_and_no_services() {
    let case = Case::new();
    case.init();
    assert!(
        case.run(&["validate", "--config", "config.json"])
            .status
            .success()
    );
    let first = case.run(&[
        "analyze",
        "--file",
        "capture.jsonl",
        "--config",
        "config.json",
    ]);
    assert!(first.status.success());
    assert!(first.stderr.is_empty());
    assert_eq!(
        first.stdout,
        case.run(&[
            "analyze",
            "--file",
            "capture.jsonl",
            "--config",
            "config.json"
        ])
        .stdout
    );
    fs::write(case.root.join("report.json"), &first.stdout).unwrap();
    let doc: serde_json::Value = serde_json::from_slice(&first.stdout).unwrap();
    assert_eq!(doc["format"], "decant-evidence");
    assert_eq!(doc["version"], 1);
    assert_eq!(doc["source_cut"]["input"]["capture"]["decoded_spans"], 4);
    assert_eq!(doc["source_cut"]["input"]["capture"]["omitted"][2], 4);
    assert_eq!(
        doc["services"][0]["current"]["p95_duration_ns"],
        200_000_000
    );
    assert!(
        case.run(&["verify", "--file", "report.json"])
            .status
            .success()
    );
    let markdown = case.run(&["render", "--file", "report.json"]);
    assert!(markdown.status.success());
    assert!(markdown.stderr.is_empty());
    assert!(markdown.stdout.len() <= 65536);
    let text = String::from_utf8(markdown.stdout).unwrap();
    assert!(text.contains("20000000 → 200000000"));
    assert!(text.contains("unknown / withheld"));
    assert!(!text.contains("synthetic-private-detail"));
    assert!(!text.contains("synthetic-operation-not-for-output"));
    let demo = case.run(&["demo"]);
    assert!(demo.status.success());
    assert!(demo.stdout.starts_with(b"SYNTHETIC DEMONSTRATION"));
    assert!(case.run(&["--help"]).status.success());
}

#[test]
fn malformed_oversized_nonregular_and_conflicting_inputs_return_no_result() {
    let case = Case::new();
    case.init();
    for (file, bytes) in [
        ("private-config", vec![b'x'; 16385]),
        ("private-report", b"SYNTHETIC-PROTECTED-INPUT".to_vec()),
    ] {
        fs::write(case.root.join(file), bytes).unwrap();
    }
    for args in [
        vec!["validate", "--config", "private-config"],
        vec!["verify", "--file", "private-report"],
        vec!["analyze", "--file", ".", "--config", "config.json"],
        vec![
            "analyze",
            "--file",
            "missing-private-file",
            "--config",
            "config.json",
        ],
    ] {
        let out = case.run(&args);
        assert_eq!(out.status.code(), Some(1));
        assert!(out.stdout.is_empty());
        assert!(!String::from_utf8(out.stderr).unwrap().contains("private"));
    }
    let original = include_str!("../../../examples/evidence/trace-capture-v1.jsonl");
    let bad = format!(
        "{original}{}\n",
        original
            .lines()
            .last()
            .unwrap()
            .replace("600200000000", "600300000000")
    );
    fs::write(case.root.join("conflict.jsonl"), bad).unwrap();
    let out = case.run(&[
        "analyze",
        "--file",
        "conflict.jsonl",
        "--config",
        "config.json",
    ]);
    assert_eq!(out.status.code(), Some(1));
    assert!(out.stdout.is_empty());
    assert_eq!(
        out.stderr,
        b"error: evidence contains conflicting retained span identities\n"
    );
    let usage = case.run(&["--private-argument"]);
    assert_eq!(usage.status.code(), Some(2));
    assert!(!String::from_utf8(usage.stderr).unwrap().contains("private"));
}

#[test]
fn unknown_policy_keys_fail_closed() {
    let case = Case::new();
    case.init();
    let mut config: serde_json::Value =
        serde_json::from_slice(&fs::read(case.root.join("config.json")).unwrap()).unwrap();
    config["recipe"]["policy"]["allow_raw"] = true.into();
    fs::write(
        case.root.join("config.json"),
        serde_json::to_vec(&config).unwrap(),
    )
    .unwrap();
    let out = case.run(&["validate", "--config", "config.json"]);
    assert_eq!(out.status.code(), Some(1));
    assert!(out.stdout.is_empty());
    assert_eq!(out.stderr, b"error: evidence configuration invalid\n");
}

#[test]
fn subcommand_help_is_discoverable_and_value_free() {
    let case = Case::new();
    fs::write(
        case.root.join("kafka.json"),
        include_bytes!("../../../examples/evidence/kafka-config-v1.json"),
    )
    .unwrap();
    let validated = case.run(&["validate-kafka", "--config", "kafka.json"]);
    assert!(validated.status.success());
    assert_eq!(
        validated.stdout,
        b"valid: Kafka evidence configuration v1 (source not contacted)\n"
    );
    assert!(validated.stderr.is_empty());
    for command in [
        "demo",
        "init",
        "validate",
        "analyze",
        "render",
        "verify",
        "serve",
        "serve-kafka",
        "validate-kafka",
    ] {
        let output = case.run(&[command, "--help"]);
        assert!(output.status.success());
        assert!(output.stderr.is_empty());
        assert!(
            String::from_utf8(output.stdout)
                .unwrap()
                .contains("serve-kafka --config")
        );
    }
}

#[cfg(unix)]
#[test]
fn sigint_and_sigterm_before_handshake_exit_without_waiting_for_stdin() {
    use std::{
        io::{BufRead, BufReader},
        process::Stdio,
        sync::mpsc,
        time::{Duration, Instant},
    };
    let case = Case::new();
    case.init();
    let output = case.run(&[
        "analyze",
        "--file",
        "capture.jsonl",
        "--config",
        "config.json",
    ]);
    assert!(output.status.success());
    fs::write(case.root.join("report.json"), output.stdout).unwrap();
    for (signal, slow_output) in [("-INT", false), ("-TERM", false), ("-TERM", true)] {
        struct Owned(std::process::Child);
        impl Drop for Owned {
            fn drop(&mut self) {
                let _ = self.0.kill();
                let _ = self.0.wait();
            }
        }
        let mut child = Owned(
            Command::new(case.root.join("decant"))
                .args(["evidence", "serve", "--file", "report.json"])
                .current_dir(&case.root)
                .stdin(Stdio::piped())
                .stdout(Stdio::piped())
                .stderr(Stdio::piped())
                .spawn()
                .unwrap(),
        );
        let stderr = child.0.stderr.take().unwrap();
        let (sender, receiver) = mpsc::sync_channel(1);
        let reader = std::thread::spawn(move || {
            let mut line = String::new();
            BufReader::new(stderr).read_line(&mut line).unwrap();
            sender.send(line).unwrap();
        });
        assert_eq!(
            receiver.recv_timeout(Duration::from_secs(5)).unwrap(),
            "info: evidence MCP ready\n"
        );
        reader.join().unwrap();
        let mut stdout_reader = BufReader::new(child.0.stdout.take().unwrap());
        if slow_output {
            use std::io::Write;
            child.0.stdin.as_mut().unwrap().write_all(b"{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"initialize\",\"params\":{\"protocolVersion\":\"2025-11-25\",\"capabilities\":{},\"clientInfo\":{\"name\":\"slow-synthetic-client\",\"version\":\"1\"}}}\n").unwrap();
            let (sender, receiver) = mpsc::sync_channel(1);
            let handshake = std::thread::spawn(move || {
                let mut line = String::new();
                stdout_reader.read_line(&mut line).unwrap();
                sender.send(line).unwrap();
                stdout_reader
            });
            assert!(
                receiver
                    .recv_timeout(Duration::from_secs(5))
                    .unwrap()
                    .contains("2025-11-25")
            );
            stdout_reader = handshake.join().unwrap();
            let stdin = child.0.stdin.as_mut().unwrap();
            stdin
                .write_all(b"{\"jsonrpc\":\"2.0\",\"method\":\"notifications/initialized\"}\n")
                .unwrap();
            for id in 2..18 {
                writeln!(stdin, "{{\"jsonrpc\":\"2.0\",\"id\":{id},\"method\":\"tools/call\",\"params\":{{\"name\":\"decant_evidence_snapshot\",\"arguments\":{{}}}}}}").unwrap();
            }
            stdin.flush().unwrap();
            std::thread::sleep(Duration::from_millis(200));
        }
        assert!(
            Command::new("/bin/kill")
                .args([signal, &child.0.id().to_string()])
                .status()
                .unwrap()
                .success()
        );
        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            if let Some(status) = child.0.try_wait().unwrap() {
                assert!(status.success());
                break;
            }
            assert!(Instant::now() < deadline, "signal shutdown hung");
            std::thread::sleep(Duration::from_millis(10));
        }
        use std::io::Read;
        let mut stdout = Vec::new();
        stdout_reader.read_to_end(&mut stdout).unwrap();
        if !slow_output {
            assert!(stdout.is_empty());
        }
    }
}
