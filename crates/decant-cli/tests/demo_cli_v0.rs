use std::ffi::OsString;
use std::process::{Command, Output};

const GOLDEN: &[u8] = include_bytes!("golden/sre-demo-v0.json");
const HELP_TEXT: &[u8] = b"Decant local-first context compiler\n\nUsage:\n  decant connect grafana --help\n  decant doctor --help\n  decant evidence --help\n  decant evidence demo\n  decant demo sre\n  decant ingest otlp-jsonl --file <PATH>\n  decant ingest kafka-json --config <PATH>\n  decant serve kafka-mcp --config <PATH>\n  decant --help\n  decant --version\n";
const VERSION_TEXT: &[u8] = b"decant 0.1.0-alpha.1\n";
const USAGE_ERROR_TEXT: &[u8] =
    b"error: invalid usage\n\nUsage:\n  decant demo sre\n  decant ingest otlp-jsonl --file <PATH>\n  decant ingest kafka-json --config <PATH>\n  decant serve kafka-mcp --config <PATH>\n  decant --help\n  decant --version\n";

fn run(arguments: &[&str]) -> Output {
    Command::new(env!("CARGO_BIN_EXE_decant"))
        .args(arguments)
        .output()
        .unwrap()
}

fn assert_usage_failure(output: &Output) {
    assert_eq!(output.status.code(), Some(2));
    assert!(output.stdout.is_empty());
    assert_eq!(output.stderr, USAGE_ERROR_TEXT);
}

#[test]
fn demo_binary_is_repeatedly_byte_identical_to_the_golden_output() {
    let first = run(&["demo", "sre"]);
    let second = run(&["demo", "sre"]);
    assert_eq!(first.status.code(), Some(0));
    assert_eq!(second.status.code(), Some(0));
    assert!(first.stderr.is_empty());
    assert!(second.stderr.is_empty());
    assert_eq!(first.stdout, GOLDEN);
    assert_eq!(second.stdout, GOLDEN);
    assert_eq!(first.stdout, second.stdout);
}

#[test]
fn golden_output_carries_the_frozen_accounting_and_context_facts() {
    assert!(GOLDEN.len() <= 32 * 1_024);
    let text = std::str::from_utf8(GOLDEN).unwrap();
    assert!(text.starts_with(
        "{\"format\":\"decant-sre-demo\",\"version\":0,\"scenario\":\"sre-v0-primary\""
    ));
    assert!(text.ends_with("}\n"));
    assert!(text.contains("\"data_deliveries\":100000,\"terminal_controls\":1"));
    assert!(text.contains("\"event_limit\":1,\"accounted_byte_limit\":16777216"));
    assert!(text.contains(
        "\"accepted\":98750,\"duplicate_noops\":1000,\"rejected\":250,\"late_rejected\":250"
    ));
    assert!(text.contains("\"context\":{\"format\":\"decant-context\""));
    assert!(text.contains("\"candidate_count\":2,\"included_count\":2,\"omitted_count\":0"));
    assert!(text.contains("\"numerator\":480,\"denominator\":15000"));
    assert!(text.contains("\"rate_ppm\":32000,\"remainder\":0"));
    assert!(text.contains("\"value\":180"));
    assert!(text.contains("\"value\":720"));
    assert!(text.contains("payments-v2-region-b-09"));
    assert!(text.contains("\"numerator_evidence_count\":3"));
    assert_eq!(
        text.matches("\"provenance\":\"decant://event/v0/sre-v0/synthetic/")
            .count(),
        6
    );
    assert!(text.contains("sha256:v0:"));
    assert!(!text.contains("must-not-enter-context"));
    assert!(!text.contains("request_ordinal"));
    assert!(!text.contains("caused"));
    assert!(!text.contains("statistically representative"));
    assert!(!text.contains("benchmark"));
}

#[test]
fn help_and_version_are_exact_successful_static_outputs() {
    let help = run(&["--help"]);
    assert_eq!(help.status.code(), Some(0));
    assert_eq!(help.stdout, HELP_TEXT);
    assert!(help.stderr.is_empty());

    let version = run(&["--version"]);
    assert_eq!(version.status.code(), Some(0));
    assert_eq!(version.stdout, VERSION_TEXT);
    assert!(version.stderr.is_empty());
}

#[test]
fn invalid_usage_is_fixed_and_does_not_echo_argument_content() {
    for arguments in [
        Vec::<&str>::new(),
        vec!["demo"],
        vec!["demo", "sre", "extra"],
        vec!["synthetic-private-probe"],
    ] {
        let output = run(&arguments);
        assert_usage_failure(&output);
        assert!(
            !output
                .stderr
                .windows(23)
                .any(|window| window == b"synthetic-private-probe")
        );
    }
}

#[cfg(unix)]
#[test]
fn non_utf8_argument_is_rejected_without_echoing_it() {
    use std::os::unix::ffi::OsStringExt;

    let probe = OsString::from_vec(vec![b'x', 0xff, b'y']);
    let output = Command::new(env!("CARGO_BIN_EXE_decant"))
        .arg(probe)
        .output()
        .unwrap();
    assert_usage_failure(&output);
    assert!(!output.stderr.contains(&0xff));
}
