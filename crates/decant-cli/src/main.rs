//! Private command-line composition root for Decant's deterministic local demo.

#![forbid(unsafe_code)]

mod demo;
mod evidence;
mod kafka_json;
mod kafka_mcp;
mod lifecycle;
mod otlp_jsonl;

use std::env;
use std::ffi::OsStr;
use std::io::{self, Write};
use std::process::ExitCode;

const HELP_TEXT: &[u8] = b"Decant local-first context compiler\n\nUsage:\n  decant evidence --help\n  decant evidence demo\n  decant demo sre\n  decant ingest otlp-jsonl --file <PATH>\n  decant ingest kafka-json --config <PATH>\n  decant serve kafka-mcp --config <PATH>\n  decant --help\n  decant --version\n";
const VERSION_TEXT: &str = concat!("decant ", env!("CARGO_PKG_VERSION"), "\n");
const USAGE_ERROR_TEXT: &[u8] =
    b"error: invalid usage\n\nUsage:\n  decant demo sre\n  decant ingest otlp-jsonl --file <PATH>\n  decant ingest kafka-json --config <PATH>\n  decant serve kafka-mcp --config <PATH>\n  decant --help\n  decant --version\n";
const INTERNAL_ERROR_TEXT: &[u8] = b"error: demo execution failed\n";
const OTLP_JSONL_ERROR_TEXT: &[u8] = b"error: OTLP JSONL processing failed\n";
const KAFKA_JSON_ERROR_TEXT: &[u8] = b"error: Kafka JSON processing failed\n";
const KAFKA_MCP_ERROR_TEXT: &[u8] = b"error: Kafka MCP service failed\n";
const KAFKA_MCP_CHECKPOINT_IN_USE_TEXT: &[u8] = b"error: Kafka MCP service failed (checkpoint_in_use)\nhint: stop the existing Decant MCP server before restarting; do not delete its lock file\n";

fn main() -> ExitCode {
    let mut arguments = env::args_os();
    let _program = arguments.next();
    let first = arguments.next();
    if first.as_deref() == Some(OsStr::new("evidence")) {
        return evidence::run(arguments);
    }
    let second = arguments.next();
    let third = arguments.next();
    let fourth = arguments.next();
    let fifth = arguments.next();

    if third.as_deref() == Some(OsStr::new("--help"))
        && fourth.is_none()
        && matches!((first.as_deref(), second.as_deref()),
            (Some(a), Some(b)) if (a == "serve" && b == "kafka-mcp")
                || (a == "ingest" && (b == "kafka-json" || b == "otlp-jsonl"))
                || (a == "demo" && b == "sre"))
    {
        return write_success(HELP_TEXT);
    }

    if first.as_deref() == Some(OsStr::new("demo"))
        && second.as_deref() == Some(OsStr::new("sre"))
        && third.is_none()
    {
        return match demo::run_sre_demo_v0() {
            Ok(bytes) => write_success(&bytes),
            Err(_error) => internal_failure(),
        };
    }
    if first.as_deref() == Some(OsStr::new("ingest"))
        && second.as_deref() == Some(OsStr::new("otlp-jsonl"))
        && third.as_deref() == Some(OsStr::new("--file"))
        && fifth.is_none()
        && let Some(path) = fourth.as_deref()
    {
        return match otlp_jsonl::run_otlp_jsonl_file_v0(path) {
            Ok(bytes) => write_otlp_success(&bytes),
            Err(_error) => otlp_jsonl_failure(),
        };
    }
    if first.as_deref() == Some(OsStr::new("serve"))
        && second.as_deref() == Some(OsStr::new("kafka-mcp"))
        && third.as_deref() == Some(OsStr::new("--config"))
        && fifth.is_none()
        && let Some(path) = fourth.as_deref()
    {
        return match kafka_mcp::run_kafka_mcp_config_file_v0(path) {
            Ok(()) => ExitCode::SUCCESS,
            Err(error) => kafka_mcp_failure(error),
        };
    }
    if first.as_deref() == Some(OsStr::new("ingest"))
        && second.as_deref() == Some(OsStr::new("kafka-json"))
        && third.as_deref() == Some(OsStr::new("--config"))
        && fifth.is_none()
        && let Some(path) = fourth.as_deref()
    {
        return match kafka_json::run_kafka_json_config_file_v0(path) {
            Ok(bytes) => write_kafka_success(&bytes),
            Err(_error) => kafka_json_failure(),
        };
    }
    if first.as_deref() == Some(OsStr::new("--help")) && second.is_none() && third.is_none() {
        return write_success(HELP_TEXT);
    }
    if first.as_deref() == Some(OsStr::new("--version")) && second.is_none() && third.is_none() {
        return write_success(VERSION_TEXT.as_bytes());
    }

    let _result = io::stderr().lock().write_all(USAGE_ERROR_TEXT);
    ExitCode::from(2)
}

fn write_success(bytes: &[u8]) -> ExitCode {
    if io::stdout().lock().write_all(bytes).is_ok() {
        ExitCode::SUCCESS
    } else {
        internal_failure()
    }
}

fn internal_failure() -> ExitCode {
    let _result = io::stderr().lock().write_all(INTERNAL_ERROR_TEXT);
    ExitCode::from(1)
}

fn write_otlp_success(bytes: &[u8]) -> ExitCode {
    if write_otlp_bytes(&mut io::stdout().lock(), bytes).is_ok() {
        ExitCode::SUCCESS
    } else {
        otlp_jsonl_failure()
    }
}

fn write_otlp_bytes(writer: &mut impl Write, bytes: &[u8]) -> io::Result<()> {
    writer.write_all(bytes)
}

fn otlp_jsonl_failure() -> ExitCode {
    let _result = io::stderr().lock().write_all(OTLP_JSONL_ERROR_TEXT);
    ExitCode::from(1)
}

fn write_kafka_success(bytes: &[u8]) -> ExitCode {
    if write_kafka_bytes(&mut io::stdout().lock(), bytes).is_ok() {
        ExitCode::SUCCESS
    } else {
        kafka_json_failure()
    }
}

fn write_kafka_bytes(writer: &mut impl Write, bytes: &[u8]) -> io::Result<()> {
    writer.write_all(bytes)
}

fn kafka_json_failure() -> ExitCode {
    let _result = io::stderr().lock().write_all(KAFKA_JSON_ERROR_TEXT);
    ExitCode::from(1)
}

fn kafka_mcp_failure(error: kafka_mcp::KafkaMcpCliErrorV0) -> ExitCode {
    let bytes = match error {
        kafka_mcp::KafkaMcpCliErrorV0::CheckpointInUse => KAFKA_MCP_CHECKPOINT_IN_USE_TEXT,
        _ => KAFKA_MCP_ERROR_TEXT,
    };
    let _result = io::stderr().lock().write_all(bytes);
    ExitCode::from(1)
}

#[cfg(test)]
mod tests {
    use super::*;

    struct RefusingWriter {
        accepted: Vec<u8>,
    }

    impl Write for RefusingWriter {
        fn write(&mut self, _buffer: &[u8]) -> io::Result<usize> {
            Err(io::Error::other("synthetic refusal"))
        }

        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }

    #[test]
    fn otlp_output_refusal_writes_no_document_prefix() {
        let mut writer = RefusingWriter {
            accepted: Vec::new(),
        };
        assert!(write_otlp_bytes(&mut writer, b"private-output").is_err());
        assert!(writer.accepted.is_empty());
    }

    #[test]
    fn kafka_output_refusal_writes_no_document_prefix() {
        let mut writer = RefusingWriter {
            accepted: Vec::new(),
        };
        assert!(write_kafka_bytes(&mut writer, b"private-output").is_err());
        assert!(writer.accepted.is_empty());
    }
}
