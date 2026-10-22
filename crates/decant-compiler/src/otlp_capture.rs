//! Fixed bounded context for one finalized OTLP JSONL capture.

use std::cmp::Ordering;
use std::error::Error;
use std::fmt;

use decant_ir::{EventEnvelopeV0, SourcePositionKindV0, SourcePositionV0};
use decant_state::{AdmissionStateHashErrorV1, AdmissionStateV0, hash_admission_state_v1};

use super::{BoundedJsonWriter, JsonWriteError};

pub const OTLP_CAPTURE_CONTEXT_VERSION_V0: u16 = 0;
pub const MAX_OTLP_CAPTURE_CONTEXT_OUTPUT_BYTES_V0: usize = 16 * 1_024;
pub const MAX_OTLP_CAPTURE_DISTINCT_SERVICES_V0: usize = 4_096;
pub const MAX_OTLP_CAPTURE_ERROR_EVIDENCE_V0: usize = 3;

const OTLP_SPAN_SCHEMA_ID_V0: &str = "otlp-span-v0";
const OTLP_UNCLASSIFIED_SENSITIVITY_BITS_V0: u64 = u64::MAX;
const PARTS_PER_MILLION_V0: u128 = 1_000_000;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum OtlpCaptureContractIssueV0 {
    Schema,
    SourcePosition,
    Key,
    IngestTime,
    Sensitivity,
    FieldSet,
    Duration,
    ServiceName,
    SpanId,
    SpanName,
    Status,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum OtlpCaptureArithmeticFieldV0 {
    TotalSpans,
    StatusCount,
    DurationSum,
    ServiceCount,
    ErrorRate,
    NumericConversion,
}

#[derive(Clone, Eq, PartialEq)]
pub enum OtlpCaptureContextErrorV0 {
    SourceNotFinalized,
    UnexpectedSourcePositionKind,
    InvalidEventContract { issue: OtlpCaptureContractIssueV0 },
    DistinctServiceCapacity { maximum: usize, attempted: usize },
    ArithmeticOverflow { field: OtlpCaptureArithmeticFieldV0 },
    AdmissionStateHash(AdmissionStateHashErrorV1),
    OutputCapacity { maximum: usize, attempted: usize },
    OutputAllocation,
    EncodingInvariant,
}

impl fmt::Debug for OtlpCaptureContextErrorV0 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::SourceNotFinalized => formatter.write_str("SourceNotFinalized"),
            Self::UnexpectedSourcePositionKind => {
                formatter.write_str("UnexpectedSourcePositionKind")
            }
            Self::InvalidEventContract { issue } => formatter
                .debug_struct("InvalidEventContract")
                .field("issue", issue)
                .finish(),
            Self::DistinctServiceCapacity { maximum, attempted } => formatter
                .debug_struct("DistinctServiceCapacity")
                .field("maximum", maximum)
                .field("attempted", attempted)
                .finish(),
            Self::ArithmeticOverflow { field } => formatter
                .debug_struct("ArithmeticOverflow")
                .field("field", field)
                .finish(),
            Self::AdmissionStateHash(error) => formatter
                .debug_tuple("AdmissionStateHash")
                .field(error)
                .finish(),
            Self::OutputCapacity { maximum, attempted } => formatter
                .debug_struct("OutputCapacity")
                .field("maximum", maximum)
                .field("attempted", attempted)
                .finish(),
            Self::OutputAllocation => formatter.write_str("OutputAllocation"),
            Self::EncodingInvariant => formatter.write_str("EncodingInvariant"),
        }
    }
}

impl fmt::Display for OtlpCaptureContextErrorV0 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(self, formatter)
    }
}

impl Error for OtlpCaptureContextErrorV0 {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::AdmissionStateHash(error) => Some(error),
            _ => None,
        }
    }
}

#[derive(Clone, Eq, PartialEq)]
pub struct OtlpCaptureContextV0 {
    bytes: Vec<u8>,
    total_spans: u64,
    error_spans: u64,
    service_names_present: u64,
    distinct_services: usize,
    error_evidence: usize,
}

impl OtlpCaptureContextV0 {
    pub fn as_bytes(&self) -> &[u8] {
        &self.bytes
    }

    pub const fn total_spans(&self) -> u64 {
        self.total_spans
    }

    pub const fn error_spans(&self) -> u64 {
        self.error_spans
    }

    pub const fn service_names_present(&self) -> u64 {
        self.service_names_present
    }

    pub const fn distinct_services(&self) -> usize {
        self.distinct_services
    }

    pub const fn error_evidence(&self) -> usize {
        self.error_evidence
    }
}

impl fmt::Debug for OtlpCaptureContextV0 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("OtlpCaptureContextV0")
            .field("byte_len", &self.bytes.len())
            .field("total_spans", &self.total_spans)
            .field("error_spans", &self.error_spans)
            .field("service_names_present", &self.service_names_present)
            .field("distinct_services", &self.distinct_services)
            .field("error_evidence", &self.error_evidence)
            .finish()
    }
}

#[derive(Clone, Copy)]
struct ErrorEvidence<'a> {
    duration_ns: u64,
    source_position: SourcePositionV0,
    provenance: &'a str,
}

#[derive(Default)]
struct Aggregates<'a> {
    total: u64,
    unset: u64,
    ok: u64,
    error: u64,
    duration_sum: u128,
    duration_min: Option<u64>,
    duration_max: Option<u64>,
    service_present: u64,
    service_missing: u64,
    distinct_services: Vec<&'a str>,
    error_evidence: Vec<ErrorEvidence<'a>>,
}

pub fn compile_otlp_capture_context_v0(
    state: &AdmissionStateV0,
) -> Result<OtlpCaptureContextV0, OtlpCaptureContextErrorV0> {
    if !state.is_finalized() {
        return Err(OtlpCaptureContextErrorV0::SourceNotFinalized);
    }
    if state.config().source_position_kind() != SourcePositionKindV0::OtlpFile {
        return Err(OtlpCaptureContextErrorV0::UnexpectedSourcePositionKind);
    }

    let mut aggregates = Aggregates::default();
    aggregates
        .distinct_services
        .try_reserve_exact(MAX_OTLP_CAPTURE_DISTINCT_SERVICES_V0)
        .map_err(|_| OtlpCaptureContextErrorV0::OutputAllocation)?;
    aggregates
        .error_evidence
        .try_reserve_exact(MAX_OTLP_CAPTURE_ERROR_EVIDENCE_V0)
        .map_err(|_| OtlpCaptureContextErrorV0::OutputAllocation)?;

    for event in state.accepted_events() {
        aggregate_event(event, &mut aggregates)?;
    }

    aggregates.error_evidence.sort_by(compare_evidence);
    let state_hash =
        hash_admission_state_v1(state).map_err(OtlpCaptureContextErrorV0::AdmissionStateHash)?;
    let bytes = encode_context(&aggregates, state_hash.as_bytes())?;

    Ok(OtlpCaptureContextV0 {
        bytes,
        total_spans: aggregates.total,
        error_spans: aggregates.error,
        service_names_present: aggregates.service_present,
        distinct_services: aggregates.distinct_services.len(),
        error_evidence: aggregates.error_evidence.len(),
    })
}

fn aggregate_event<'a>(
    event: &'a EventEnvelopeV0,
    aggregates: &mut Aggregates<'a>,
) -> Result<(), OtlpCaptureContextErrorV0> {
    validate_envelope(event)?;
    let fields = parse_fields(event)?;

    aggregates.total =
        checked_increment(aggregates.total, OtlpCaptureArithmeticFieldV0::TotalSpans)?;
    aggregates.duration_sum = aggregates
        .duration_sum
        .checked_add(u128::from(fields.duration_ns))
        .ok_or(OtlpCaptureContextErrorV0::ArithmeticOverflow {
            field: OtlpCaptureArithmeticFieldV0::DurationSum,
        })?;
    aggregates.duration_min = Some(
        aggregates
            .duration_min
            .map_or(fields.duration_ns, |value| value.min(fields.duration_ns)),
    );
    aggregates.duration_max = Some(
        aggregates
            .duration_max
            .map_or(fields.duration_ns, |value| value.max(fields.duration_ns)),
    );

    match fields.service_name {
        Some(service_name) => {
            aggregates.service_present = checked_increment(
                aggregates.service_present,
                OtlpCaptureArithmeticFieldV0::ServiceCount,
            )?;
            insert_distinct_service(&mut aggregates.distinct_services, service_name)?;
        }
        None => {
            aggregates.service_missing = checked_increment(
                aggregates.service_missing,
                OtlpCaptureArithmeticFieldV0::ServiceCount,
            )?;
        }
    }

    match fields.status_code {
        0 => {
            aggregates.unset =
                checked_increment(aggregates.unset, OtlpCaptureArithmeticFieldV0::StatusCount)?;
        }
        1 => {
            aggregates.ok =
                checked_increment(aggregates.ok, OtlpCaptureArithmeticFieldV0::StatusCount)?;
        }
        2 => {
            aggregates.error =
                checked_increment(aggregates.error, OtlpCaptureArithmeticFieldV0::StatusCount)?;
            retain_error_evidence(
                &mut aggregates.error_evidence,
                ErrorEvidence {
                    duration_ns: fields.duration_ns,
                    source_position: event.source_position(),
                    provenance: event.provenance().as_str(),
                },
            );
        }
        _ => {
            return Err(OtlpCaptureContextErrorV0::InvalidEventContract {
                issue: OtlpCaptureContractIssueV0::Status,
            });
        }
    }
    Ok(())
}

fn validate_envelope(event: &EventEnvelopeV0) -> Result<(), OtlpCaptureContextErrorV0> {
    if event.schema_id().as_str() != OTLP_SPAN_SCHEMA_ID_V0 {
        return invalid_contract(OtlpCaptureContractIssueV0::Schema);
    }
    if event.source_position().kind() != SourcePositionKindV0::OtlpFile {
        return invalid_contract(OtlpCaptureContractIssueV0::SourcePosition);
    }
    if event.key().map(<[u8]>::len) != Some(16) {
        return invalid_contract(OtlpCaptureContractIssueV0::Key);
    }
    if event.ingest_time_ns() != 0 {
        return invalid_contract(OtlpCaptureContractIssueV0::IngestTime);
    }
    if event.sensitivity().bits() != OTLP_UNCLASSIFIED_SENSITIVITY_BITS_V0 {
        return invalid_contract(OtlpCaptureContractIssueV0::Sensitivity);
    }
    Ok(())
}

struct ParsedFields<'a> {
    duration_ns: u64,
    service_name: Option<&'a str>,
    status_code: u64,
}

fn parse_fields(event: &EventEnvelopeV0) -> Result<ParsedFields<'_>, OtlpCaptureContextErrorV0> {
    let mut fields = event.payload().fields();
    let duration_ns = expect_field(&mut fields, "duration.ns")?.as_u64().ok_or(
        OtlpCaptureContextErrorV0::InvalidEventContract {
            issue: OtlpCaptureContractIssueV0::Duration,
        },
    )?;

    let (name, value) = fields
        .next()
        .ok_or(OtlpCaptureContextErrorV0::InvalidEventContract {
            issue: OtlpCaptureContractIssueV0::FieldSet,
        })?;
    let service_name = if name == "service.name" {
        let service_name =
            value
                .as_utf8()
                .ok_or(OtlpCaptureContextErrorV0::InvalidEventContract {
                    issue: OtlpCaptureContractIssueV0::ServiceName,
                })?;
        let span_id = expect_field(&mut fields, "span.id")?;
        validate_span_id(span_id.as_bytes())?;
        Some(service_name)
    } else if name == "span.id" {
        validate_span_id(value.as_bytes())?;
        None
    } else {
        return invalid_contract(OtlpCaptureContractIssueV0::FieldSet);
    };

    expect_field(&mut fields, "span.name")?.as_utf8().ok_or(
        OtlpCaptureContextErrorV0::InvalidEventContract {
            issue: OtlpCaptureContractIssueV0::SpanName,
        },
    )?;
    let status_code = expect_field(&mut fields, "status.code")?.as_u64().ok_or(
        OtlpCaptureContextErrorV0::InvalidEventContract {
            issue: OtlpCaptureContractIssueV0::Status,
        },
    )?;
    if fields.next().is_some() {
        return invalid_contract(OtlpCaptureContractIssueV0::FieldSet);
    }

    Ok(ParsedFields {
        duration_ns,
        service_name,
        status_code,
    })
}

fn expect_field<'a>(
    fields: &mut impl Iterator<Item = (&'a str, &'a decant_ir::ScalarValueV0)>,
    expected: &str,
) -> Result<&'a decant_ir::ScalarValueV0, OtlpCaptureContextErrorV0> {
    match fields.next() {
        Some((actual, value)) if actual == expected => Ok(value),
        _ => invalid_contract(OtlpCaptureContractIssueV0::FieldSet),
    }
}

fn validate_span_id(value: Option<&[u8]>) -> Result<(), OtlpCaptureContextErrorV0> {
    if value.map(<[u8]>::len) == Some(8) {
        Ok(())
    } else {
        invalid_contract(OtlpCaptureContractIssueV0::SpanId)
    }
}

fn invalid_contract<T>(issue: OtlpCaptureContractIssueV0) -> Result<T, OtlpCaptureContextErrorV0> {
    Err(OtlpCaptureContextErrorV0::InvalidEventContract { issue })
}

fn checked_increment(
    value: u64,
    field: OtlpCaptureArithmeticFieldV0,
) -> Result<u64, OtlpCaptureContextErrorV0> {
    value
        .checked_add(1)
        .ok_or(OtlpCaptureContextErrorV0::ArithmeticOverflow { field })
}

fn insert_distinct_service<'a>(
    services: &mut Vec<&'a str>,
    service_name: &'a str,
) -> Result<(), OtlpCaptureContextErrorV0> {
    match services.binary_search(&service_name) {
        Ok(_) => Ok(()),
        Err(index) => {
            if services.len() == MAX_OTLP_CAPTURE_DISTINCT_SERVICES_V0 {
                return Err(OtlpCaptureContextErrorV0::DistinctServiceCapacity {
                    maximum: MAX_OTLP_CAPTURE_DISTINCT_SERVICES_V0,
                    attempted: MAX_OTLP_CAPTURE_DISTINCT_SERVICES_V0 + 1,
                });
            }
            services.insert(index, service_name);
            Ok(())
        }
    }
}

fn retain_error_evidence<'a>(evidence: &mut Vec<ErrorEvidence<'a>>, candidate: ErrorEvidence<'a>) {
    if evidence.len() < MAX_OTLP_CAPTURE_ERROR_EVIDENCE_V0 {
        evidence.push(candidate);
        return;
    }
    let Some(worst_index) = evidence
        .iter()
        .enumerate()
        .max_by(|(_, left), (_, right)| compare_evidence(left, right))
        .map(|(index, _)| index)
    else {
        return;
    };
    if compare_evidence(&candidate, &evidence[worst_index]) == Ordering::Less {
        evidence[worst_index] = candidate;
    }
}

fn compare_evidence(left: &ErrorEvidence<'_>, right: &ErrorEvidence<'_>) -> Ordering {
    right
        .duration_ns
        .cmp(&left.duration_ns)
        .then_with(|| left.source_position.cmp(&right.source_position))
}

fn encode_context(
    aggregates: &Aggregates<'_>,
    state_hash: &[u8; 32],
) -> Result<Vec<u8>, OtlpCaptureContextErrorV0> {
    let mut writer = BoundedJsonWriter::new(MAX_OTLP_CAPTURE_CONTEXT_OUTPUT_BYTES_V0);
    writer.push_static(
        b"{\"format\":\"decant-otlp-context\",\"version\":0,\"status\":{\"source_finalized\":true},\"spans\":{\"total\":",
    )?;
    writer.push_u64(aggregates.total)?;
    writer.push_static(b",\"status\":{\"unset\":")?;
    writer.push_u64(aggregates.unset)?;
    writer.push_static(b",\"ok\":")?;
    writer.push_u64(aggregates.ok)?;
    writer.push_static(b",\"error\":")?;
    writer.push_u64(aggregates.error)?;
    writer.push_static(b"},\"error_rate_ppm\":")?;
    encode_error_rate(&mut writer, aggregates)?;
    writer.push_static(b",\"duration_ns\":")?;
    encode_duration(&mut writer, aggregates)?;
    writer.push_static(b",\"services\":{\"present\":")?;
    writer.push_u64(aggregates.service_present)?;
    writer.push_static(b",\"missing\":")?;
    writer.push_u64(aggregates.service_missing)?;
    writer.push_static(b",\"distinct\":")?;
    writer.push_usize(aggregates.distinct_services.len())?;
    writer.push_static(b"}},\"evidence\":{\"error_spans\":[")?;
    for (index, evidence) in aggregates.error_evidence.iter().enumerate() {
        if index != 0 {
            writer.push_static(b",")?;
        }
        writer.push_static(b"{\"duration_ns\":")?;
        writer.push_u64(evidence.duration_ns)?;
        writer.push_static(b",\"status_code\":2,\"provenance\":")?;
        writer.push_json_string(evidence.provenance)?;
        writer.push_static(b"}")?;
    }
    writer.push_static(b"]},\"receipt\":{\"admission_state_hash\":\"sha256:v1:")?;
    writer.push_hex(state_hash)?;
    writer.push_static(b"\"}}")?;
    Ok(writer.finish())
}

fn encode_error_rate(
    writer: &mut BoundedJsonWriter,
    aggregates: &Aggregates<'_>,
) -> Result<(), OtlpCaptureContextErrorV0> {
    if aggregates.total == 0 {
        return writer.push_static(b"null").map_err(Into::into);
    }
    let numerator = u128::from(aggregates.error)
        .checked_mul(PARTS_PER_MILLION_V0)
        .ok_or(OtlpCaptureContextErrorV0::ArithmeticOverflow {
            field: OtlpCaptureArithmeticFieldV0::ErrorRate,
        })?;
    let denominator = u128::from(aggregates.total);
    let value = u64::try_from(numerator / denominator).map_err(|_| {
        OtlpCaptureContextErrorV0::ArithmeticOverflow {
            field: OtlpCaptureArithmeticFieldV0::NumericConversion,
        }
    })?;
    let remainder = u64::try_from(numerator % denominator).map_err(|_| {
        OtlpCaptureContextErrorV0::ArithmeticOverflow {
            field: OtlpCaptureArithmeticFieldV0::NumericConversion,
        }
    })?;
    writer.push_static(b"{\"value\":")?;
    writer.push_u64(value)?;
    writer.push_static(b",\"remainder\":")?;
    writer.push_u64(remainder)?;
    writer.push_static(b",\"denominator\":")?;
    writer.push_u64(aggregates.total)?;
    writer.push_static(b"}")?;
    Ok(())
}

fn encode_duration(
    writer: &mut BoundedJsonWriter,
    aggregates: &Aggregates<'_>,
) -> Result<(), OtlpCaptureContextErrorV0> {
    let (minimum, maximum) = match (aggregates.duration_min, aggregates.duration_max) {
        (Some(minimum), Some(maximum)) => (minimum, maximum),
        (None, None) => return writer.push_static(b"null").map_err(Into::into),
        _ => return Err(OtlpCaptureContextErrorV0::EncodingInvariant),
    };
    let denominator = u128::from(aggregates.total);
    let quotient = u64::try_from(aggregates.duration_sum / denominator).map_err(|_| {
        OtlpCaptureContextErrorV0::ArithmeticOverflow {
            field: OtlpCaptureArithmeticFieldV0::NumericConversion,
        }
    })?;
    let remainder = u64::try_from(aggregates.duration_sum % denominator).map_err(|_| {
        OtlpCaptureContextErrorV0::ArithmeticOverflow {
            field: OtlpCaptureArithmeticFieldV0::NumericConversion,
        }
    })?;

    writer.push_static(b"{\"minimum\":")?;
    writer.push_u64(minimum)?;
    writer.push_static(b",\"maximum\":")?;
    writer.push_u64(maximum)?;
    writer.push_static(b",\"average\":{\"quotient\":")?;
    writer.push_u64(quotient)?;
    writer.push_static(b",\"remainder\":")?;
    writer.push_u64(remainder)?;
    writer.push_static(b",\"denominator\":")?;
    writer.push_u64(aggregates.total)?;
    writer.push_static(b"}}")?;
    Ok(())
}

impl From<JsonWriteError> for OtlpCaptureContextErrorV0 {
    fn from(error: JsonWriteError) -> Self {
        match error {
            JsonWriteError::Capacity { maximum, attempted } => {
                Self::OutputCapacity { maximum, attempted }
            }
            JsonWriteError::Arithmetic => Self::ArithmeticOverflow {
                field: OtlpCaptureArithmeticFieldV0::NumericConversion,
            },
            JsonWriteError::Allocation => Self::OutputAllocation,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use decant_ir::{
        CanonicalPayloadV0, EventEnvelopeV0, ScalarValueV0, SchemaIdV0, SensitivityBitmapV0,
        SourceIdV0, SourcePositionV0,
    };

    use super::*;

    fn envelope(position: SourcePositionV0, ingest_time_ns: i64) -> EventEnvelopeV0 {
        let mut fields = BTreeMap::new();
        fields.insert("duration.ns".to_owned(), ScalarValueV0::from_u64(1));
        fields.insert("span.id".to_owned(), ScalarValueV0::bytes(&[1; 8]).unwrap());
        fields.insert(
            "span.name".to_owned(),
            ScalarValueV0::utf8("private-span-name").unwrap(),
        );
        fields.insert("status.code".to_owned(), ScalarValueV0::from_u64(0));
        EventEnvelopeV0::new(
            0,
            SourceIdV0::new("otlp-compiler-v0").unwrap(),
            SchemaIdV0::new(OTLP_SPAN_SCHEMA_ID_V0).unwrap(),
            position,
            Some(&[2; 16]),
            1,
            ingest_time_ns,
            SensitivityBitmapV0::from_bits(u64::MAX),
            CanonicalPayloadV0::try_from_fields(fields).unwrap(),
        )
        .unwrap()
    }

    #[test]
    fn direct_envelope_validation_rejects_nonzero_ingest_and_wrong_position_safely() {
        let wrong_ingest = envelope(SourcePositionV0::otlp_file(1, 0, 0, 0).unwrap(), 9);
        assert_eq!(
            validate_envelope(&wrong_ingest),
            Err(OtlpCaptureContextErrorV0::InvalidEventContract {
                issue: OtlpCaptureContractIssueV0::IngestTime,
            })
        );

        let wrong_position = envelope(SourcePositionV0::Synthetic { sequence: 1 }, 0);
        let error = validate_envelope(&wrong_position).unwrap_err();
        assert_eq!(
            error,
            OtlpCaptureContextErrorV0::InvalidEventContract {
                issue: OtlpCaptureContractIssueV0::SourcePosition,
            }
        );
        assert!(!format!("{error:?} {error}").contains("private-span-name"));
    }
}
