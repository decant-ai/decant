use decant_compiler::{
    evidence::{
        CaptureAccountingV1, EvidenceDocumentV1, EvidenceInputV1, EvidenceRequestV1,
        MAX_EVIDENCE_BYTES_V1, compile_evidence_with_input_v1,
    },
    evidence_markdown::render_evidence_markdown_v1,
};
use decant_ir::SourceIdV0;
use decant_jsonl::JsonlLimitsV0;
use decant_mcp::{
    DecantMcpBoundedTransportV0,
    evidence::{EvidenceServerV1, EvidenceViewV1},
};
use decant_otlp_ingest::{OtlpJsonlIngestConfigV0, ingest_otlp_jsonl_evidence_v1};
use rmcp::ServiceExt;
use serde::{Deserialize, Serialize};
use std::{
    ffi::{OsStr, OsString},
    fs::File,
    io::{BufReader, Read, Write},
    process::ExitCode,
};

mod kafka;

pub(crate) const HELP: &[u8] = b"Decant evidence: local production-debugging reports\n\nUsage:\n  decant evidence demo\n  decant evidence init\n  decant evidence validate --config <PATH>\n  decant evidence analyze --file <PATH> --config <PATH>\n  decant evidence render --file <REPORT>\n  decant evidence verify --file <REPORT>\n  decant evidence serve --file <REPORT>\n  decant evidence validate-kafka --config <PATH>\n  decant evidence serve-kafka --config <PATH>\n  decant evidence --help\n\ninit/analyze write JSON to stdout; render/demo write Markdown. Review allowed service labels before sharing. Capture commands need no Kafka, LLM, account, or runtime network. serve-kafka connects only to your configured source. Services handle SIGINT/SIGTERM on Unix; see docs/release/MCP_COMPATIBILITY.md.\n";

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Config {
    format: String,
    version: u16,
    recipe: EvidenceRequestV1,
}
impl Default for Config {
    fn default() -> Self {
        Self {
            format: "decant-evidence-config".into(),
            version: 1,
            recipe: EvidenceRequestV1::default(),
        }
    }
}

#[derive(Clone, Copy)]
enum Failure {
    Usage,
    Input,
    Config,
    Conflict,
    Report,
    Output,
    Mcp,
    CheckpointInUse,
    Recovery,
    Client,
    Source,
    Assignment,
    Processing,
    Signal,
    Shutdown,
}
impl Failure {
    fn bytes(self) -> &'static [u8] {
        match self {
            Self::Usage => b"error: invalid evidence usage; run decant evidence --help\n",
            Self::Input => b"error: evidence input invalid or exceeds supported limits\n",
            Self::Config => b"error: evidence configuration invalid\n",
            Self::Conflict => b"error: evidence contains conflicting retained span identities\n",
            Self::Report => b"error: evidence report invalid or cannot fit configured bounds\n",
            Self::Output => b"error: evidence output failed\n",
            Self::Mcp => b"error: evidence MCP service failed\n",
            Self::CheckpointInUse => b"error: Kafka MCP service failed (checkpoint_in_use)\nhint: stop the existing Decant MCP server before restarting; do not delete its lock file\n",
            Self::Recovery => b"error: Kafka evidence checkpoint recovery failed\nhint: verify checkpoint ownership, permissions and matching configuration; do not delete or bypass the latest checkpoint\n",
            Self::Client => b"error: Kafka evidence client configuration failed\nhint: review security settings and required credential references\n",
            Self::Source => b"error: Kafka evidence source validation failed\nhint: verify connectivity, authorization and configured cluster/topic identity\n",
            Self::Assignment => b"error: Kafka evidence partition assignment failed\n",
            Self::Processing => b"error: Kafka evidence processing or checkpoint publication failed\nhint: inspect authorized input and storage locally; the pending suffix was not acknowledged to Kafka\n",
            Self::Signal => b"error: Decant signal listener failed\n",
            Self::Shutdown => b"error: Decant worker shutdown failed or exceeded 30 seconds\nhint: pending checkpoint durability is unknown; use strict recovery on restart\n",
        }
    }
}

pub(crate) fn run(arguments: impl Iterator<Item = OsString>) -> ExitCode {
    let args: Vec<_> = arguments.take(9).collect();
    match execute(&args) {
        Ok(bytes) => {
            if std::io::stdout().lock().write_all(&bytes).is_ok() {
                ExitCode::SUCCESS
            } else {
                failure(Failure::Output)
            }
        }
        Err(error) => failure(error),
    }
}
fn failure(error: Failure) -> ExitCode {
    let _ = std::io::stderr().lock().write_all(error.bytes());
    ExitCode::from(if matches!(error, Failure::Usage) {
        2
    } else {
        1
    })
}
fn is(value: &OsStr, expected: &str) -> bool {
    value == OsStr::new(expected)
}

fn execute(args: &[OsString]) -> Result<Vec<u8>, Failure> {
    match args {
        [command] if is(command, "--help") => Ok(HELP.to_vec()),
        [command, help]
            if is(help, "--help")
                && [
                    "demo",
                    "init",
                    "validate",
                    "analyze",
                    "render",
                    "verify",
                    "serve",
                    "serve-kafka",
                    "validate-kafka",
                ]
                .iter()
                .any(|name| is(command, name)) =>
        {
            Ok(HELP.to_vec())
        }
        [command] if is(command, "init") => json_line(&Config::default()),
        [command] if is(command, "demo") => {
            let mut config = Config::default();
            config.recipe.policy.allow_service_versions = true;
            let report = analyze(
                std::io::Cursor::new(include_bytes!(
                    "../../../examples/evidence/trace-capture-v1.jsonl"
                )),
                &config,
            )?;
            let mut output =
                b"SYNTHETIC DEMONSTRATION - not production incident evidence.\n\n".to_vec();
            output.extend(render_evidence_markdown_v1(&report).map_err(|_| Failure::Report)?);
            Ok(output)
        }
        [command, flag, path] if is(command, "validate") && is(flag, "--config") => {
            load_config(path)?;
            Ok(b"valid: evidence configuration v1\n".to_vec())
        }
        [command, flag, path] if is(command, "validate-kafka") && is(flag, "--config") => {
            crate::kafka_json::evidence::parse(&bounded_file(path, 64 * 1024)?)
                .map_err(|_| Failure::Config)?;
            Ok(b"valid: Kafka evidence configuration v1 (source not contacted)\n".to_vec())
        }
        [command, flag, path] if is(command, "serve-kafka") && is(flag, "--config") => {
            let config = crate::kafka_json::evidence::parse(&bounded_file(path, 64 * 1024)?)
                .map_err(|_| Failure::Config)?;
            kafka::run(config)?;
            Ok(Vec::new())
        }
        [command, file, path, config_flag, config_path]
            if is(command, "analyze") && is(file, "--file") && is(config_flag, "--config") =>
        {
            let config = load_config(config_path)?;
            let file = regular_file(path)?;
            let report = analyze(BufReader::with_capacity(8192, file), &config)?;
            let mut bytes = report.to_bytes().map_err(|_| Failure::Report)?;
            bytes.push(b'\n');
            Ok(bytes)
        }
        [command, flag, path] if is(flag, "--file") => {
            if !["render", "verify", "serve"]
                .iter()
                .any(|name| is(command, name))
            {
                return Err(Failure::Usage);
            }
            let report = load_report(path)?;
            if is(command, "render") {
                render_evidence_markdown_v1(&report).map_err(|_| Failure::Report)
            } else if is(command, "verify") {
                Ok(
                    b"verified: canonical report integrity; source truth is not verified\n"
                        .to_vec(),
                )
            } else {
                serve(report)?;
                Ok(Vec::new())
            }
        }
        _ => Err(Failure::Usage),
    }
}

fn regular_file(path: &OsStr) -> Result<File, Failure> {
    // Check type before opening to avoid blocking on FIFOs/devices. Recheck the
    // opened handle to catch ordinary replacement; hostile local races are out
    // of the trusted-host model (no arbitrary file paths from MCP).
    if !std::fs::metadata(path)
        .map_err(|_| Failure::Input)?
        .is_file()
    {
        return Err(Failure::Input);
    }
    let file = File::open(path).map_err(|_| Failure::Input)?;
    if !file.metadata().map_err(|_| Failure::Input)?.is_file() {
        return Err(Failure::Input);
    }
    Ok(file)
}
fn bounded_file(path: &OsStr, limit: usize) -> Result<Vec<u8>, Failure> {
    let file = regular_file(path)?;
    if file.metadata().map_err(|_| Failure::Input)?.len() > limit as u64 {
        return Err(Failure::Input);
    }
    let mut bytes = Vec::with_capacity(limit + 1);
    file.take(limit as u64 + 1)
        .read_to_end(&mut bytes)
        .map_err(|_| Failure::Input)?;
    if bytes.len() > limit {
        return Err(Failure::Input);
    }
    Ok(bytes)
}
fn load_config(path: &OsStr) -> Result<Config, Failure> {
    let config: Config =
        serde_json::from_slice(&bounded_file(path, 16384)?).map_err(|_| Failure::Config)?;
    if config.format != "decant-evidence-config" || config.version != 1 {
        return Err(Failure::Config);
    }
    config.recipe.validate().map_err(|_| Failure::Config)?;
    Ok(config)
}
fn load_report(path: &OsStr) -> Result<EvidenceDocumentV1, Failure> {
    let bytes = bounded_file(path, MAX_EVIDENCE_BYTES_V1 + 1)?;
    EvidenceDocumentV1::from_verified_bytes(bytes.strip_suffix(b"\n").unwrap_or(&bytes))
        .map_err(|_| Failure::Report)
}
fn json_line(value: &impl Serialize) -> Result<Vec<u8>, Failure> {
    let mut bytes =
        decant_compiler::evidence::bounded_json(value, 16384).map_err(|_| Failure::Output)?;
    bytes.push(b'\n');
    Ok(bytes)
}

fn analyze(reader: impl std::io::BufRead, config: &Config) -> Result<EvidenceDocumentV1, Failure> {
    let ingestion = OtlpJsonlIngestConfigV0::new(
        0,
        SourceIdV0::new("otel-evidence-v1").map_err(|_| Failure::Config)?,
        JsonlLimitsV0::maximum(),
    )
    .map_err(|_| Failure::Config)?;
    let result = ingest_otlp_jsonl_evidence_v1(reader, &ingestion).map_err(|_| Failure::Input)?;
    let usage = result.usage();
    let decode = usage.decode();
    let input = EvidenceInputV1 {
        capture: Some(CaptureAccountingV1 {
            records: usage.input_records(),
            bytes: usage.input_bytes(),
            decoded_spans: decode.decoded_spans(),
            omitted: [
                decode.resource_attributes_omitted(),
                decode.scope_attributes_omitted(),
                decode.span_attributes_omitted(),
                decode.span_events_omitted(),
                decode.span_links_omitted(),
                decode.status_messages_omitted(),
            ],
            upstream_dropped: [
                decode.resource_dropped_attributes_reported(),
                decode.scope_dropped_attributes_reported(),
                decode.span_dropped_attributes_reported(),
                decode.span_dropped_events_reported(),
                decode.span_dropped_links_reported(),
            ],
        }),
        source_progress_hash: None,
    };
    compile_evidence_with_input_v1(result.admission(), &config.recipe, input).map_err(|error| {
        match error {
            decant_state::sre_evidence::EvidenceErrorV1::ConflictingSpan => Failure::Conflict,
            _ => Failure::Report,
        }
    })
}
fn serve(report: EvidenceDocumentV1) -> Result<(), Failure> {
    let view = EvidenceViewV1::new(&report).map_err(|_| Failure::Report)?;
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|_| Failure::Mcp)?;
    let result = runtime.block_on(async {
        let mut signal = crate::lifecycle::Shutdown::new().map_err(|_| Failure::Signal)?;
        let server = EvidenceServerV1::new(view);
        let transport = DecantMcpBoundedTransportV0::new(tokio::io::stdin(), crate::lifecycle::stdout().map_err(|_| Failure::Output)?);
        std::io::stderr().lock().write_all(b"info: evidence MCP ready\n").map_err(|_| Failure::Output)?;
        let initialize = server.serve(transport);
        tokio::pin!(initialize);
        let running = tokio::select! {
            result = &mut initialize => result.map_err(|_| Failure::Mcp)?,
            result = signal.wait() => return result.map_err(|_| Failure::Signal),
        };
        let cancellation = running.cancellation_token();
        tokio::select! {
            result = running.waiting() => result.map(|_| ()).map_err(|_| Failure::Mcp),
            result = signal.wait() => { cancellation.cancel(); result.map_err(|_| Failure::Signal) },
        }
    });
    runtime.shutdown_background();
    result
}
