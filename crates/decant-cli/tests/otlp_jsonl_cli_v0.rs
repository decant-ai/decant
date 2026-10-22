use std::ffi::{OsStr, OsString};
use std::fs;
use std::path::{Path, PathBuf};
use std::process::{Command, Output};
use std::sync::atomic::{AtomicUsize, Ordering};

use decant_jsonl::MAX_JSONL_RECORD_BYTES_V0;

const PROCESSING_ERROR: &[u8] = b"error: OTLP JSONL processing failed\n";
const USAGE_ERROR: &[u8] = b"error: invalid usage\n\nUsage:\n  decant demo sre\n  decant ingest otlp-jsonl --file <PATH>\n  decant ingest kafka-json --config <PATH>\n  decant serve kafka-mcp --config <PATH>\n  decant --help\n  decant --version\n";
const REPRESENTATIVE: &[u8] = concat!(
    "{\"resourceSpans\":[{\"resource\":{\"attributes\":[",
    "{\"key\":\"service.name\",\"value\":{\"stringValue\":\"private-service\"}},",
    "{\"key\":\"ignored\",\"value\":{\"stringValue\":\"private-resource-value\"}}],",
    "\"droppedAttributesCount\":4},\"scopeSpans\":[{\"scope\":{\"attributes\":[{}],",
    "\"droppedAttributesCount\":5},\"spans\":[",
    "{\"traceId\":\"00000000000000000000000000000001\",",
    "\"spanId\":\"0000000000000001\",\"name\":\"private-span-one\",",
    "\"startTimeUnixNano\":\"100\",\"endTimeUnixNano\":\"110\",",
    "\"status\":{\"code\":0,\"message\":\"\"},\"attributes\":[{}],",
    "\"events\":[{}],\"links\":[{}],\"droppedAttributesCount\":1,",
    "\"droppedEventsCount\":2,\"droppedLinksCount\":3},",
    "{\"traceId\":\"00000000000000000000000000000002\",",
    "\"spanId\":\"0000000000000002\",\"name\":\"private-span-two\",",
    "\"startTimeUnixNano\":\"120\",\"endTimeUnixNano\":\"150\",",
    "\"status\":{\"code\":2,\"message\":\"private-status-message\"},",
    "\"attributes\":[{}],\"events\":[{}],\"links\":[{}],",
    "\"droppedAttributesCount\":1,\"droppedEventsCount\":2,",
    "\"droppedLinksCount\":3}]}]}]}\n"
)
.as_bytes();

static NEXT_CASE: AtomicUsize = AtomicUsize::new(0);

struct TempCase {
    root: PathBuf,
}

impl TempCase {
    fn new() -> Self {
        let ordinal = NEXT_CASE.fetch_add(1, Ordering::Relaxed);
        let root = std::env::temp_dir().join(format!(
            "decant-otlp-cli-v0-{}-{ordinal}",
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

fn run_file(path: &Path) -> Output {
    run(&[
        OsStr::new("ingest"),
        OsStr::new("otlp-jsonl"),
        OsStr::new("--file"),
        path.as_os_str(),
    ])
}

fn assert_processing_failure(output: &Output) {
    assert_eq!(output.status.code(), Some(1));
    assert!(output.stdout.is_empty());
    assert_eq!(output.stderr, PROCESSING_ERROR);
}

#[test]
fn representative_file_is_bounded_reconciled_and_byte_identical() {
    let case = TempCase::new();
    let path = case.file("representative.jsonl", REPRESENTATIVE);
    let first = run_file(&path);
    let second = run_file(&path);

    assert_eq!(first.status.code(), Some(0));
    assert_eq!(second.status.code(), Some(0));
    assert!(first.stderr.is_empty());
    assert!(second.stderr.is_empty());
    assert_eq!(first.stdout, second.stdout);
    assert!(first.stdout.len() <= 64 * 1_024);
    let text = std::str::from_utf8(&first.stdout).unwrap();
    assert!(text.starts_with("{\"format\":\"decant-otlp-jsonl-run\",\"version\":0,"));
    assert!(text.ends_with("}\n"));
    assert!(text.contains(&format!(
        "\"input\":{{\"records\":1,\"bytes\":{}}}",
        REPRESENTATIVE.len()
    )));
    assert!(text.contains("\"decode\":{\"spans\":2"));
    assert!(text.contains(
        "\"resource_attributes\":1,\"scope_attributes\":1,\"span_attributes\":2,\"span_events\":2,\"span_links\":2,\"status_messages\":1"
    ));
    assert!(text.contains(
        "\"resource_attributes\":4,\"scope_attributes\":5,\"span_attributes\":2,\"span_events\":4,\"span_links\":6"
    ));
    assert!(text.contains(
        "\"canonical_events\":2,\"enqueued_events\":2,\"received_events\":2,\"accepted\":2,\"duplicate_noops\":0,\"rejected\":0,\"late_rejected\":0"
    ));
    assert!(text.contains("\"accepted_identities\":2"));
    assert!(text.contains("\"finalized\":true"));
    assert!(text.contains("\"context\":{\"format\":\"decant-otlp-context\""));
    assert!(text.contains("\"total\":2"));
    assert!(text.contains("\"status\":{\"unset\":1,\"ok\":0,\"error\":1}"));
    assert!(
        text.contains("\"error_rate_ppm\":{\"value\":500000,\"remainder\":0,\"denominator\":2}")
    );
    assert!(text.contains("sha256:v1:"));
    for raw in [
        "private-service",
        "private-resource-value",
        "private-span-one",
        "private-span-two",
        "private-status-message",
        "00000000000000000000000000000001",
    ] {
        assert!(!text.contains(raw), "raw input leaked: {raw}");
    }
}

#[cfg(unix)]
#[test]
fn non_utf8_file_path_is_accepted_without_becoming_output() {
    use std::os::unix::ffi::OsStringExt;

    let case = TempCase::new();
    let name = OsString::from_vec(vec![b'c', b'a', b'p', b'-', 0xff, b'.', b'j']);
    let path = case.root.join(&name);
    match fs::write(&path, b"{\"resourceSpans\":[]}\n") {
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
            // macOS EILSEQ: the filesystem rejects creation, but the CLI must
            // still accept the argument and fail without echoing its bytes.
            let output = run_file(&path);
            assert_eq!(output.status.code(), Some(1));
            assert!(output.stdout.is_empty());
            assert_eq!(output.stderr, b"error: OTLP JSONL processing failed\n");
            return;
        }
        Err(error) => panic!("unexpected non-UTF-8 fixture creation error: {error:?}"),
    }
    let output = run_file(&path);
    assert_eq!(output.status.code(), Some(0));
    assert!(output.stderr.is_empty());
    assert!(!output.stdout.contains(&0xff));
}

#[test]
fn invalid_command_shapes_are_fixed_and_do_not_echo_tokens() {
    for arguments in [
        vec![
            OsStr::new("ingest"),
            OsStr::new("otlp-jsonl"),
            OsStr::new("--file"),
        ],
        vec![
            OsStr::new("ingest"),
            OsStr::new("otlp-jsonl"),
            OsStr::new("--wrong-private-token"),
            OsStr::new("private-path"),
        ],
        vec![
            OsStr::new("ingest"),
            OsStr::new("otlp-jsonl"),
            OsStr::new("--file"),
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

#[test]
fn file_and_input_failures_have_empty_stdout_and_one_safe_category() {
    let case = TempCase::new();
    assert_processing_failure(&run_file(&case.root.join("private-missing.jsonl")));
    assert_processing_failure(&run_file(&case.root));

    for (name, bytes) in [
        ("malformed.jsonl", b"{private-not-json}\n".as_slice()),
        ("wrong-signal.jsonl", b"{\"resourceLogs\":[]}\n".as_slice()),
        ("missing-lf.jsonl", b"{\"resourceSpans\":[]}".as_slice()),
    ] {
        assert_processing_failure(&run_file(&case.file(name, bytes)));
    }

    let oversized = vec![b'x'; MAX_JSONL_RECORD_BYTES_V0 + 1];
    assert_processing_failure(&run_file(&case.file("oversized.jsonl", &oversized)));
}
