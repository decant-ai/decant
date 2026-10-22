use std::{
    ffi::OsStr,
    fs,
    path::{Path, PathBuf},
    process::{Command, Output},
    sync::atomic::{AtomicUsize, Ordering},
};

const SERVICE_ERROR: &[u8] = b"error: Kafka MCP service failed\n";
const USAGE_ERROR: &[u8] = b"error: invalid usage\n\nUsage:\n  decant demo sre\n  decant ingest otlp-jsonl --file <PATH>\n  decant ingest kafka-json --config <PATH>\n  decant serve kafka-mcp --config <PATH>\n  decant --help\n  decant --version\n";

static NEXT_CASE: AtomicUsize = AtomicUsize::new(0);

struct TempCase(PathBuf);

impl TempCase {
    fn new() -> Self {
        let ordinal = NEXT_CASE.fetch_add(1, Ordering::Relaxed);
        let path = std::env::temp_dir().join(format!(
            "decant-kafka-mcp-cli-{}-{ordinal}",
            std::process::id()
        ));
        fs::create_dir(&path).unwrap();
        Self(path)
    }

    fn file(&self, name: &str, bytes: &[u8]) -> PathBuf {
        let path = self.0.join(name);
        fs::write(&path, bytes).unwrap();
        path
    }
}

impl Drop for TempCase {
    fn drop(&mut self) {
        fs::remove_dir_all(&self.0).unwrap();
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
        OsStr::new("serve"),
        OsStr::new("kafka-mcp"),
        OsStr::new("--config"),
        path.as_os_str(),
    ])
}

#[test]
fn command_is_exact_and_preflight_failures_never_write_mcp_stdout() {
    let case = TempCase::new();
    for path in [
        case.0.join("private-missing.json"),
        case.file("malformed.json", b"{private}"),
        case.file("oversized.json", &vec![b'x'; 64 * 1_024 + 1]),
    ] {
        let output = run_config(&path);
        assert_eq!(output.status.code(), Some(1));
        assert!(output.stdout.is_empty());
        assert_eq!(output.stderr, SERVICE_ERROR);
        assert!(!output.stderr.windows(7).any(|value| value == b"private"));
    }
}

#[test]
fn invalid_service_command_shapes_keep_value_safe_usage() {
    for arguments in [
        vec![OsStr::new("serve"), OsStr::new("kafka-mcp")],
        vec![
            OsStr::new("serve"),
            OsStr::new("kafka-mcp"),
            OsStr::new("--private"),
            OsStr::new("private-path"),
        ],
        vec![
            OsStr::new("serve"),
            OsStr::new("kafka-mcp"),
            OsStr::new("--config"),
            OsStr::new("private-path"),
            OsStr::new("private-extra"),
        ],
    ] {
        let output = run(&arguments);
        assert_eq!(output.status.code(), Some(2));
        assert!(output.stdout.is_empty());
        assert_eq!(output.stderr, USAGE_ERROR);
        assert!(!output.stderr.windows(7).any(|value| value == b"private"));
    }
}
