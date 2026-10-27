use std::ffi::{OsStr, OsString};
use std::fs;
use std::path::{Path, PathBuf};
use std::process::{Command, Output};
use std::sync::atomic::{AtomicUsize, Ordering};

const PROCESSING_ERROR: &[u8] = b"error: Kafka JSON processing failed\n";
const USAGE_ERROR: &[u8] = b"error: invalid usage\n\nUsage:\n  decant demo sre\n  decant ingest otlp-jsonl --file <PATH>\n  decant ingest kafka-json --config <PATH>\n  decant serve kafka-mcp --config <PATH>\n  decant --help\n  decant --version\n";
const HELP_TEXT: &[u8] = b"Decant local-first context compiler\n\nUsage:\n  decant connect grafana --help\n  decant doctor --help\n  decant investigate --help\n  decant evidence --help\n  decant evidence demo\n  decant demo sre\n  decant ingest otlp-jsonl --file <PATH>\n  decant ingest kafka-json --config <PATH>\n  decant serve kafka-mcp --config <PATH>\n  decant --help\n  decant --version\n";

static NEXT_CASE: AtomicUsize = AtomicUsize::new(0);

struct TempCase {
    root: PathBuf,
}

impl TempCase {
    fn new() -> Self {
        let ordinal = NEXT_CASE.fetch_add(1, Ordering::Relaxed);
        let root = std::env::temp_dir().join(format!(
            "decant-kafka-cli-v0-{}-{ordinal}",
            std::process::id()
        ));
        fs::create_dir(&root).unwrap();
        Self { root }
    }

    fn file(&self, name: impl AsRef<Path>, bytes: &[u8]) -> PathBuf {
        let path = self.root.join(name);
        fs::write(&path, bytes).unwrap();
        path
    }
}

impl Drop for TempCase {
    fn drop(&mut self) {
        fs::remove_dir_all(&self.root).unwrap();
    }
}

fn run(arguments: &[&OsStr]) -> Output {
    Command::new(env!("CARGO_BIN_EXE_decant"))
        .args(arguments)
        .output()
        .unwrap()
}

fn run_config(path: &Path) -> Output {
    run(&[
        OsStr::new("ingest"),
        OsStr::new("kafka-json"),
        OsStr::new("--config"),
        path.as_os_str(),
    ])
}

fn valid_config(checkpoint_directory: &Path) -> Vec<u8> {
    serde_json::to_vec(&serde_json::json!({
        "format": "decant-kafka-live-config",
        "version": 0,
        "bootstrap_servers": "127.0.0.1:9092",
        "topic": "decant-events",
        "source_id": "live-source",
        "expected_cluster_id": "cluster-v0",
        "expected_topic_id": "AQEBAQEBAQEBAQEBAQEBAQ",
        "partitions": [{"partition": 0, "start_offset": 0}],
        "checkpoint_directory": checkpoint_directory,
        "epoch_start_ns": 0,
        "epoch_end_ns": 1000,
        "accepted_identity_limit": 100,
        "accepted_event_byte_limit": 100000,
        "poll_timeout_ms": 10,
        "idle_poll_limit": 2,
        "max_records": 3
    }))
    .unwrap()
}

fn valid_config_v1(checkpoint_directory: &Path) -> Vec<u8> {
    let mut document: serde_json::Value =
        serde_json::from_slice(&valid_config(checkpoint_directory)).unwrap();
    document["version"] = serde_json::json!(1);
    document["context"] = serde_json::json!({
        "format": "decant-kafka-activity-context-request",
        "version": 0,
        "byte_budget": 512,
    });
    serde_json::to_vec(&document).unwrap()
}

fn assert_processing_failure(output: &Output) {
    assert_eq!(output.status.code(), Some(1));
    assert!(output.stdout.is_empty());
    assert_eq!(output.stderr, PROCESSING_ERROR);
}

#[test]
fn exact_command_is_recognized_and_config_failures_are_value_safe() {
    let case = TempCase::new();
    assert_processing_failure(&run_config(&case.root.join("private-missing.json")));
    assert_processing_failure(&run_config(&case.file("malformed.json", b"{private}")));
    assert_processing_failure(&run_config(
        &case.file("oversized.json", &vec![b'x'; 64 * 1_024 + 1]),
    ));

    let missing_checkpoint = case.root.join("private-missing-checkpoint");
    let valid = case.file("valid.json", &valid_config(&missing_checkpoint));
    assert_processing_failure(&run_config(&valid));
    let valid_v1 = case.file("valid-v1.json", &valid_config_v1(&missing_checkpoint));
    assert_processing_failure(&run_config(&valid_v1));

    let mut invalid_v1: serde_json::Value =
        serde_json::from_slice(&valid_config_v1(&missing_checkpoint)).unwrap();
    invalid_v1["context"]["byte_budget"] = serde_json::json!(511);
    let invalid_v1 = case.file("invalid-v1.json", &serde_json::to_vec(&invalid_v1).unwrap());
    assert_processing_failure(&run_config(&invalid_v1));
    assert!(
        !PROCESSING_ERROR
            .windows(7)
            .any(|window| window == b"private")
    );
}

#[test]
fn invalid_command_shapes_keep_the_frozen_usage_contract() {
    for arguments in [
        vec![OsStr::new("ingest"), OsStr::new("kafka-json")],
        vec![
            OsStr::new("ingest"),
            OsStr::new("kafka-json"),
            OsStr::new("--private-wrong"),
            OsStr::new("private-path"),
        ],
        vec![
            OsStr::new("ingest"),
            OsStr::new("kafka-json"),
            OsStr::new("--config"),
            OsStr::new("private-path"),
            OsStr::new("private-extra"),
        ],
    ] {
        let output = run(&arguments);
        assert_eq!(output.status.code(), Some(2));
        assert!(output.stdout.is_empty());
        assert_eq!(output.stderr, USAGE_ERROR);
        assert!(!output.stderr.windows(7).any(|window| window == b"private"));
    }

    let help = run(&[OsStr::new("--help")]);
    assert_eq!(help.status.code(), Some(0));
    assert_eq!(help.stdout, HELP_TEXT);
    assert!(help.stderr.is_empty());
}

#[cfg(unix)]
#[test]
fn non_utf8_config_path_is_accepted_without_becoming_diagnostic_output() {
    use std::os::unix::ffi::OsStringExt;

    let case = TempCase::new();
    let name = OsString::from_vec(vec![b'c', b'f', b'g', b'-', 0xff, b'.', b'j']);
    let path = case.root.join(&name);
    match fs::write(&path, b"{private}") {
        Ok(()) => {}
        Err(error)
            if (cfg!(target_os = "macos") && error.raw_os_error() == Some(92))
                || matches!(
                    error.kind(),
                    std::io::ErrorKind::PermissionDenied
                        | std::io::ErrorKind::InvalidInput
                        | std::io::ErrorKind::Unsupported
                ) =>
        {
            // macOS EILSEQ (92) means this filesystem cannot create the name.
            // Still exercise the non-UTF-8 argument and value-safe missing-file path.
            let output = run_config(&path);
            assert_processing_failure(&output);
            assert!(!output.stderr.contains(&0xff));
            return;
        }
        Err(error) => panic!("unexpected non-UTF-8 fixture creation error: {error:?}"),
    }
    let output = run_config(&path);
    assert_processing_failure(&output);
    assert!(!output.stderr.contains(&0xff));
}
