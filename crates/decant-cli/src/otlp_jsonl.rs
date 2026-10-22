use std::ffi::OsStr;
use std::fmt::{self, Write};
use std::fs::File;
use std::io::BufReader;
use std::str;

use decant_compiler::compile_otlp_capture_context_v0;
use decant_ir::SourceIdV0;
use decant_jsonl::JsonlLimitsV0;
use decant_otlp_ingest::{
    OTLP_JSONL_INGEST_VERSION_V0, OtlpJsonlIngestConfigV0, ingest_otlp_jsonl_v0,
};
use decant_state::RejectionReasonV0;

pub(crate) const OTLP_JSONL_FILE_BUFFER_BYTES_V0: usize = 8 * 1_024;
pub(crate) const MAX_OTLP_JSONL_RUN_OUTPUT_BYTES_V0: usize = 64 * 1_024;

const SOURCE_ID: &str = "otlp-jsonl-v0";

#[derive(Debug, Eq, PartialEq)]
pub(crate) enum OtlpJsonlCliErrorV0 {
    FileOpen,
    Configuration,
    Ingestion,
    Compiler,
    AccountingInvariant,
    ContextEncoding,
    OutputAllocation,
    OutputCapacity,
}

pub(crate) fn run_otlp_jsonl_file_v0(path: &OsStr) -> Result<Vec<u8>, OtlpJsonlCliErrorV0> {
    let file = File::open(path).map_err(|_| OtlpJsonlCliErrorV0::FileOpen)?;
    let reader = BufReader::with_capacity(OTLP_JSONL_FILE_BUFFER_BYTES_V0, file);
    run_otlp_jsonl_reader_v0(reader)
}

fn run_otlp_jsonl_reader_v0<R: std::io::BufRead>(
    reader: R,
) -> Result<Vec<u8>, OtlpJsonlCliErrorV0> {
    let source_id = SourceIdV0::new(SOURCE_ID).map_err(|_| OtlpJsonlCliErrorV0::Configuration)?;
    let config = OtlpJsonlIngestConfigV0::new(
        OTLP_JSONL_INGEST_VERSION_V0,
        source_id,
        JsonlLimitsV0::maximum(),
    )
    .map_err(|_| OtlpJsonlCliErrorV0::Configuration)?;
    let result =
        ingest_otlp_jsonl_v0(reader, &config).map_err(|_| OtlpJsonlCliErrorV0::Ingestion)?;
    let context = compile_otlp_capture_context_v0(result.admission())
        .map_err(|_| OtlpJsonlCliErrorV0::Compiler)?;
    verify_accounting(&result, &context)?;
    encode_run(&result, context.as_bytes())
}

fn verify_accounting(
    result: &decant_otlp_ingest::OtlpJsonlIngestResultV0,
    context: &decant_compiler::OtlpCaptureContextV0,
) -> Result<(), OtlpJsonlCliErrorV0> {
    let usage = result.usage();
    let classified = usage
        .accepted_events()
        .checked_add(usage.duplicate_noops())
        .and_then(|value| value.checked_add(usage.late_rejections()))
        .ok_or(OtlpJsonlCliErrorV0::AccountingInvariant)?;
    let admission_usage = result.admission().usage();
    let accepted_identities = u64::try_from(admission_usage.accepted_identities())
        .map_err(|_| OtlpJsonlCliErrorV0::AccountingInvariant)?;
    let late_evidence = result
        .admission()
        .rejection_summary()
        .range(RejectionReasonV0::Late);
    if usage.decode().decoded_spans() != usage.canonical_events()
        || usage.canonical_events() != usage.enqueued_events()
        || usage.canonical_events() != usage.received_events()
        || usage.canonical_events() != classified
        || usage.accepted_events() != accepted_identities
        || usage.accepted_events() != context.total_spans()
        || late_evidence.count() != usage.late_rejections()
        || late_evidence.count_saturated()
        || !result.admission().is_finalized()
    {
        return Err(OtlpJsonlCliErrorV0::AccountingInvariant);
    }
    Ok(())
}

fn encode_run(
    result: &decant_otlp_ingest::OtlpJsonlIngestResultV0,
    context_bytes: &[u8],
) -> Result<Vec<u8>, OtlpJsonlCliErrorV0> {
    let usage = result.usage();
    let decode = usage.decode();
    let transport = usage
        .transport()
        .ok_or(OtlpJsonlCliErrorV0::AccountingInvariant)?;
    let transport_limits = transport.limits();
    let admission_usage = result.admission().usage();
    let late_evidence = result
        .admission()
        .rejection_summary()
        .range(RejectionReasonV0::Late);
    let context =
        str::from_utf8(context_bytes).map_err(|_| OtlpJsonlCliErrorV0::ContextEncoding)?;

    let mut output = BoundedOutputV0::new(MAX_OTLP_JSONL_RUN_OUTPUT_BYTES_V0)?;
    write!(
        &mut output,
        concat!(
            "{{\"format\":\"decant-otlp-jsonl-run\",\"version\":0,",
            "\"input\":{{\"records\":{},\"bytes\":{}}},",
            "\"decode\":{{\"spans\":{},\"omitted\":{{",
            "\"resource_attributes\":{},\"scope_attributes\":{},",
            "\"span_attributes\":{},\"span_events\":{},\"span_links\":{},",
            "\"status_messages\":{}}},\"upstream_dropped\":{{",
            "\"resource_attributes\":{},\"scope_attributes\":{},",
            "\"span_attributes\":{},\"span_events\":{},\"span_links\":{}}}}},",
            "\"transport\":{{\"event_limit\":{},\"accounted_byte_limit\":{},",
            "\"high_water_events\":{},\"high_water_accounted_bytes\":{}}},",
            "\"admission\":{{\"canonical_events\":{},\"enqueued_events\":{},",
            "\"received_events\":{},\"accepted\":{},\"duplicate_noops\":{},",
            "\"rejected\":{},\"late_rejected\":{},",
            "\"late_rejection_evidence_count\":{},\"accepted_identities\":{},",
            "\"accepted_event_bytes\":{},\"finalized\":true}},\"context\":"
        ),
        usage.input_records(),
        usage.input_bytes(),
        decode.decoded_spans(),
        decode.resource_attributes_omitted(),
        decode.scope_attributes_omitted(),
        decode.span_attributes_omitted(),
        decode.span_events_omitted(),
        decode.span_links_omitted(),
        decode.status_messages_omitted(),
        decode.resource_dropped_attributes_reported(),
        decode.scope_dropped_attributes_reported(),
        decode.span_dropped_attributes_reported(),
        decode.span_dropped_events_reported(),
        decode.span_dropped_links_reported(),
        transport_limits.event_limit(),
        transport_limits.accounted_byte_limit(),
        transport.high_water_events(),
        transport.high_water_accounted_bytes(),
        usage.canonical_events(),
        usage.enqueued_events(),
        usage.received_events(),
        usage.accepted_events(),
        usage.duplicate_noops(),
        usage.late_rejections(),
        usage.late_rejections(),
        late_evidence.count(),
        admission_usage.accepted_identities(),
        admission_usage.accepted_event_bytes(),
    )
    .map_err(|_| OtlpJsonlCliErrorV0::OutputCapacity)?;
    output
        .write_str(context)
        .map_err(|_| OtlpJsonlCliErrorV0::OutputCapacity)?;
    output
        .write_str("}\n")
        .map_err(|_| OtlpJsonlCliErrorV0::OutputCapacity)?;
    Ok(output.finish())
}

struct BoundedOutputV0 {
    bytes: Vec<u8>,
    maximum: usize,
}

impl BoundedOutputV0 {
    fn new(maximum: usize) -> Result<Self, OtlpJsonlCliErrorV0> {
        let mut bytes = Vec::new();
        bytes
            .try_reserve_exact(maximum)
            .map_err(|_| OtlpJsonlCliErrorV0::OutputAllocation)?;
        Ok(Self { bytes, maximum })
    }

    fn finish(self) -> Vec<u8> {
        self.bytes
    }
}

impl Write for BoundedOutputV0 {
    fn write_str(&mut self, value: &str) -> fmt::Result {
        let next_len = self
            .bytes
            .len()
            .checked_add(value.len())
            .ok_or(fmt::Error)?;
        if next_len > self.maximum {
            return Err(fmt::Error);
        }
        self.bytes.extend_from_slice(value.as_bytes());
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::io::Cursor;

    use super::*;

    const EMPTY_TRACE_LINE: &[u8] = b"{\"resourceSpans\":[]}\n";

    #[test]
    fn bounded_writer_and_in_process_run_are_exact() {
        let mut writer = BoundedOutputV0::new(3).unwrap();
        writer.write_str("abc").unwrap();
        assert_eq!(writer.write_str("d"), Err(fmt::Error));
        assert_eq!(writer.finish(), b"abc");

        let first = run_otlp_jsonl_reader_v0(Cursor::new(EMPTY_TRACE_LINE)).unwrap();
        let second = run_otlp_jsonl_reader_v0(Cursor::new(EMPTY_TRACE_LINE)).unwrap();
        assert_eq!(first, second);
        assert!(first.len() <= MAX_OTLP_JSONL_RUN_OUTPUT_BYTES_V0);
        assert!(first.ends_with(b"}\n"));
    }

    #[test]
    fn internal_error_categories_never_carry_input_values() {
        let errors = [
            OtlpJsonlCliErrorV0::FileOpen,
            OtlpJsonlCliErrorV0::Configuration,
            OtlpJsonlCliErrorV0::Ingestion,
            OtlpJsonlCliErrorV0::Compiler,
            OtlpJsonlCliErrorV0::AccountingInvariant,
            OtlpJsonlCliErrorV0::ContextEncoding,
            OtlpJsonlCliErrorV0::OutputAllocation,
            OtlpJsonlCliErrorV0::OutputCapacity,
        ];
        for error in errors {
            let diagnostic = format!("{error:?}");
            assert!(!diagnostic.contains("private"));
            assert!(!diagnostic.contains('/'));
        }
    }
}
