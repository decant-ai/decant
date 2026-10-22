//! Deterministic canonical mapping for already decoded OTLP file trace lines.
//!
//! This private V0 boundary performs no file or network access and does not
//! admit the resulting events into Decant state.

#![forbid(unsafe_code)]

use std::collections::BTreeMap;
use std::error::Error;
use std::fmt;

use decant_ir::{
    CanonicalPayloadV0, ENVELOPE_VERSION_V0, EventEnvelopeV0, ScalarValueV0, SchemaIdV0,
    SensitivityBitmapV0, SourceIdV0, SourcePositionV0, ValidationErrorV0,
};
use decant_otlp_json::{DecodeUsageV0, DecodedTraceLineV0, OtlpSpanStatusV0};

pub const OTLP_FILE_CANONICALIZER_VERSION_V0: u16 = 0;
pub const OTLP_SPAN_SCHEMA_ID_V0: &str = "otlp-span-v0";
pub const UNCLASSIFIED_OTLP_SENSITIVITY_BITS_V0: u64 = u64::MAX;

const DURATION_FIELD: &str = "duration.ns";
const SERVICE_NAME_FIELD: &str = "service.name";
const SPAN_ID_FIELD: &str = "span.id";
const SPAN_NAME_FIELD: &str = "span.name";
const STATUS_CODE_FIELD: &str = "status.code";

#[derive(Clone, Eq, PartialEq)]
pub struct OtlpFileCanonicalizerConfigV0 {
    version: u16,
    source_id: SourceIdV0,
}

impl OtlpFileCanonicalizerConfigV0 {
    pub fn new(version: u16, source_id: SourceIdV0) -> Result<Self, OtlpCanonicalizationErrorV0> {
        if version != OTLP_FILE_CANONICALIZER_VERSION_V0 {
            return Err(OtlpCanonicalizationErrorV0::UnsupportedVersion { actual: version });
        }
        Ok(Self { version, source_id })
    }

    pub const fn version(&self) -> u16 {
        self.version
    }

    pub fn source_id(&self) -> &SourceIdV0 {
        &self.source_id
    }
}

impl fmt::Debug for OtlpFileCanonicalizerConfigV0 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("OtlpFileCanonicalizerConfigV0")
            .field("version", &self.version)
            .field("source_id", &self.source_id)
            .finish()
    }
}

#[derive(Clone, Eq, PartialEq)]
pub enum OtlpCanonicalizationErrorV0 {
    UnsupportedVersion { actual: u16 },
    InvalidSourcePosition(ValidationErrorV0),
    InvalidCanonicalEvent(ValidationErrorV0),
    AllocationFailure { requested_events: usize },
}

impl fmt::Debug for OtlpCanonicalizationErrorV0 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::UnsupportedVersion { actual } => formatter
                .debug_struct("UnsupportedVersion")
                .field("actual", actual)
                .finish(),
            Self::InvalidSourcePosition(error) => formatter
                .debug_tuple("InvalidSourcePosition")
                .field(error)
                .finish(),
            Self::InvalidCanonicalEvent(error) => formatter
                .debug_tuple("InvalidCanonicalEvent")
                .field(error)
                .finish(),
            Self::AllocationFailure { requested_events } => formatter
                .debug_struct("AllocationFailure")
                .field("requested_events", requested_events)
                .finish(),
        }
    }
}

impl fmt::Display for OtlpCanonicalizationErrorV0 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(self, formatter)
    }
}

impl Error for OtlpCanonicalizationErrorV0 {}

#[derive(Clone, Eq, PartialEq)]
pub struct CanonicalizedOtlpTraceLineV0 {
    line_number: u64,
    events: Vec<EventEnvelopeV0>,
    decode_usage: DecodeUsageV0,
}

impl CanonicalizedOtlpTraceLineV0 {
    pub const fn line_number(&self) -> u64 {
        self.line_number
    }

    pub fn events(&self) -> impl ExactSizeIterator<Item = &EventEnvelopeV0> {
        self.events.iter()
    }

    pub fn len(&self) -> usize {
        self.events.len()
    }

    pub fn is_empty(&self) -> bool {
        self.events.is_empty()
    }

    pub const fn decode_usage(&self) -> DecodeUsageV0 {
        self.decode_usage
    }
}

impl fmt::Debug for CanonicalizedOtlpTraceLineV0 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("CanonicalizedOtlpTraceLineV0")
            .field("line_number", &"<redacted>")
            .field("event_count", &self.events.len())
            .field("decode_usage", &self.decode_usage)
            .finish()
    }
}

pub fn canonicalize_otlp_trace_line_v0(
    config: &OtlpFileCanonicalizerConfigV0,
    decoded: &DecodedTraceLineV0,
) -> Result<CanonicalizedOtlpTraceLineV0, OtlpCanonicalizationErrorV0> {
    SourcePositionV0::otlp_file(decoded.line_number(), 0, 0, 0)
        .map_err(OtlpCanonicalizationErrorV0::InvalidSourcePosition)?;

    let schema_id = SchemaIdV0::new(OTLP_SPAN_SCHEMA_ID_V0)
        .map_err(OtlpCanonicalizationErrorV0::InvalidCanonicalEvent)?;
    let mut events = Vec::new();
    events.try_reserve_exact(decoded.len()).map_err(|_| {
        OtlpCanonicalizationErrorV0::AllocationFailure {
            requested_events: decoded.len(),
        }
    })?;

    for span in decoded.spans() {
        let capture = span.position();
        let source_position = SourcePositionV0::otlp_file(
            capture.line_number(),
            capture.resource_spans_index(),
            capture.scope_spans_index(),
            capture.span_index(),
        )
        .map_err(OtlpCanonicalizationErrorV0::InvalidSourcePosition)?;

        let mut fields = BTreeMap::new();
        fields.insert(
            DURATION_FIELD.to_owned(),
            ScalarValueV0::from_u64(span.duration_ns()),
        );
        if let Some(service_name) = span.service_name() {
            fields.insert(
                SERVICE_NAME_FIELD.to_owned(),
                ScalarValueV0::utf8(service_name)
                    .map_err(OtlpCanonicalizationErrorV0::InvalidCanonicalEvent)?,
            );
        }
        fields.insert(
            SPAN_ID_FIELD.to_owned(),
            ScalarValueV0::bytes(span.span_id())
                .map_err(OtlpCanonicalizationErrorV0::InvalidCanonicalEvent)?,
        );
        fields.insert(
            SPAN_NAME_FIELD.to_owned(),
            ScalarValueV0::utf8(span.name())
                .map_err(OtlpCanonicalizationErrorV0::InvalidCanonicalEvent)?,
        );
        fields.insert(
            STATUS_CODE_FIELD.to_owned(),
            ScalarValueV0::from_u64(status_code(span.status())),
        );
        let payload = CanonicalPayloadV0::try_from_fields(fields)
            .map_err(OtlpCanonicalizationErrorV0::InvalidCanonicalEvent)?;

        events.push(
            EventEnvelopeV0::new(
                ENVELOPE_VERSION_V0,
                config.source_id.clone(),
                schema_id.clone(),
                source_position,
                Some(span.trace_id()),
                span.start_time_unix_ns(),
                0,
                SensitivityBitmapV0::from_bits(UNCLASSIFIED_OTLP_SENSITIVITY_BITS_V0),
                payload,
            )
            .map_err(OtlpCanonicalizationErrorV0::InvalidCanonicalEvent)?,
        );
    }

    Ok(CanonicalizedOtlpTraceLineV0 {
        line_number: decoded.line_number(),
        events,
        decode_usage: decoded.usage(),
    })
}

const fn status_code(status: OtlpSpanStatusV0) -> u64 {
    match status {
        OtlpSpanStatusV0::Unset => 0,
        OtlpSpanStatusV0::Ok => 1,
        OtlpSpanStatusV0::Error => 2,
    }
}

/// V1 changes the projection/schema, not the bounded event-batch container V0.
pub fn canonicalize_otlp_trace_line_v1(
    source_id: &SourceIdV0,
    decoded: &decant_otlp_json::evidence::DecodedTraceLineV1,
) -> Result<CanonicalizedOtlpTraceLineV0, OtlpCanonicalizationErrorV0> {
    let invalid = OtlpCanonicalizationErrorV0::InvalidCanonicalEvent;
    let schema = SchemaIdV0::new("sre-span-v1").map_err(invalid)?;
    let mut events = Vec::new();
    events
        .try_reserve_exact(decoded.spans().len())
        .map_err(|_| OtlpCanonicalizationErrorV0::AllocationFailure {
            requested_events: decoded.spans().len(),
        })?;
    let hex = |bytes: &[u8]| {
        bytes
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect::<String>()
    };
    for span in decoded.spans() {
        let base = span.base();
        let position = base.position();
        let mut fields = BTreeMap::from([
            (
                "trace.id".into(),
                ScalarValueV0::utf8(&hex(base.trace_id())).map_err(invalid)?,
            ),
            (
                "span.id".into(),
                ScalarValueV0::utf8(&hex(base.span_id())).map_err(invalid)?,
            ),
            ("span.kind".into(), ScalarValueV0::from_u64(span.kind())),
            (
                "duration.ns".into(),
                ScalarValueV0::from_u64(base.duration_ns()),
            ),
            (
                "status.code".into(),
                ScalarValueV0::from_u64(status_code(base.status())),
            ),
        ]);
        for (name, value) in [
            ("service.name", base.service_name()),
            ("service.version", span.service_version()),
        ] {
            if let Some(value) = value {
                fields.insert(name.into(), ScalarValueV0::utf8(value).map_err(invalid)?);
            }
        }
        events.push(
            EventEnvelopeV0::new(
                0,
                source_id.clone(),
                schema.clone(),
                SourcePositionV0::otlp_file(
                    position.line_number(),
                    position.resource_spans_index(),
                    position.scope_spans_index(),
                    position.span_index(),
                )
                .map_err(invalid)?,
                Some(base.trace_id()),
                base.start_time_unix_ns(),
                0,
                SensitivityBitmapV0::from_bits(u64::MAX),
                CanonicalPayloadV0::try_from_fields(fields).map_err(invalid)?,
            )
            .map_err(invalid)?,
        );
    }
    Ok(CanonicalizedOtlpTraceLineV0 {
        line_number: decoded.line_number(),
        events,
        decode_usage: decoded.usage(),
    })
}
