//! Bounded decoding for one OpenTelemetry OTLP JSON file trace record.
//!
//! This private V0 crate stops at protocol decoding. It does not read files,
//! construct Decant canonical events, or define source checkpoint semantics.

#![forbid(unsafe_code)]

pub mod evidence;

use std::error::Error;
use std::fmt;

use serde::de::{IgnoredAny, MapAccess, SeqAccess, Visitor};
use serde::{Deserialize, Deserializer};

pub const MAX_OTLP_JSON_LINE_BYTES_V0: usize = 4 * 1024 * 1024;
pub const MAX_RESOURCE_SPANS_PER_LINE_V0: usize = 128;
pub const MAX_SCOPE_SPANS_PER_RESOURCE_V0: usize = 128;
pub const MAX_SPANS_PER_SCOPE_V0: usize = 4_096;
pub const MAX_SPANS_PER_LINE_V0: usize = 4_096;
pub const MAX_RESOURCE_ATTRIBUTES_V0: usize = 128;
pub const MAX_SCOPE_ATTRIBUTES_V0: usize = 128;
pub const MAX_SPAN_ATTRIBUTES_V0: usize = 128;
pub const MAX_SPAN_EVENTS_V0: usize = 128;
pub const MAX_SPAN_LINKS_V0: usize = 128;
pub const MAX_SERVICE_NAME_BYTES_V0: usize = 256;
pub const MAX_SPAN_NAME_BYTES_V0: usize = 256;

const TRACE_ID_BYTES: usize = 16;
const SPAN_ID_BYTES: usize = 8;
const SERVICE_NAME_ATTRIBUTE: &str = "service.name";

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum StructuralBoundV0 {
    ResourceSpansPerLine,
    ScopeSpansPerResource,
    SpansPerScope,
    SpansPerLine,
    ResourceAttributes,
    ScopeAttributes,
    SpanAttributes,
    SpanEvents,
    SpanLinks,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum IdentifierFieldV0 {
    TraceId,
    SpanId,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum IdentifierProblemV0 {
    Missing,
    InvalidLength,
    InvalidHex,
    AllZero,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TimestampFieldV0 {
    StartTimeUnixNano,
    EndTimeUnixNano,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ServiceNameProblemV0 {
    Duplicate,
    WrongValueKind,
    Empty,
    TooLong,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AccountingFieldV0 {
    TotalSpans,
    ResourceAttributesOmitted,
    ScopeAttributesOmitted,
    SpanAttributesOmitted,
    SpanEventsOmitted,
    SpanLinksOmitted,
    StatusMessagesOmitted,
    ResourceDroppedAttributesReported,
    ScopeDroppedAttributesReported,
    SpanDroppedAttributesReported,
    SpanDroppedEventsReported,
    SpanDroppedLinksReported,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum OtlpSignalV0 {
    Metrics,
    Logs,
    Profiles,
}

#[derive(Clone, Eq, PartialEq)]
pub enum DecodeErrorV0 {
    InputTooLarge {
        maximum: usize,
        actual: usize,
    },
    InvalidLineNumber,
    MissingTerminalLineFeed,
    CarriageReturn,
    EmbeddedLineFeed,
    MalformedJson,
    UnsupportedSignal {
        signal: OtlpSignalV0,
    },
    TooMany {
        bound: StructuralBoundV0,
        maximum: usize,
        actual: usize,
    },
    InvalidIdentifier {
        field: IdentifierFieldV0,
        problem: IdentifierProblemV0,
        input_byte_len: usize,
    },
    InvalidTimestamp {
        field: TimestampFieldV0,
    },
    TimestampOutOfRange {
        field: TimestampFieldV0,
    },
    EndBeforeStart,
    InvalidStatusCode {
        actual: u32,
    },
    InvalidServiceName {
        problem: ServiceNameProblemV0,
        byte_len: usize,
    },
    InvalidSpanNameLength {
        maximum: usize,
        actual: usize,
    },
    AccountingOverflow {
        field: AccountingFieldV0,
    },
    AllocationFailure {
        requested_spans: usize,
    },
}

impl fmt::Debug for DecodeErrorV0 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InputTooLarge { maximum, actual } => formatter
                .debug_struct("InputTooLarge")
                .field("maximum", maximum)
                .field("actual", actual)
                .finish(),
            Self::InvalidLineNumber => formatter.write_str("InvalidLineNumber"),
            Self::MissingTerminalLineFeed => formatter.write_str("MissingTerminalLineFeed"),
            Self::CarriageReturn => formatter.write_str("CarriageReturn"),
            Self::EmbeddedLineFeed => formatter.write_str("EmbeddedLineFeed"),
            Self::MalformedJson => formatter.write_str("MalformedJson"),
            Self::UnsupportedSignal { signal } => formatter
                .debug_struct("UnsupportedSignal")
                .field("signal", signal)
                .finish(),
            Self::TooMany {
                bound,
                maximum,
                actual,
            } => formatter
                .debug_struct("TooMany")
                .field("bound", bound)
                .field("maximum", maximum)
                .field("actual", actual)
                .finish(),
            Self::InvalidIdentifier {
                field,
                problem,
                input_byte_len,
            } => formatter
                .debug_struct("InvalidIdentifier")
                .field("field", field)
                .field("problem", problem)
                .field("input_byte_len", input_byte_len)
                .finish(),
            Self::InvalidTimestamp { field } => formatter
                .debug_struct("InvalidTimestamp")
                .field("field", field)
                .finish(),
            Self::TimestampOutOfRange { field } => formatter
                .debug_struct("TimestampOutOfRange")
                .field("field", field)
                .finish(),
            Self::EndBeforeStart => formatter.write_str("EndBeforeStart"),
            Self::InvalidStatusCode { actual } => formatter
                .debug_struct("InvalidStatusCode")
                .field("actual", actual)
                .finish(),
            Self::InvalidServiceName { problem, byte_len } => formatter
                .debug_struct("InvalidServiceName")
                .field("problem", problem)
                .field("byte_len", byte_len)
                .finish(),
            Self::InvalidSpanNameLength { maximum, actual } => formatter
                .debug_struct("InvalidSpanNameLength")
                .field("maximum", maximum)
                .field("actual", actual)
                .finish(),
            Self::AccountingOverflow { field } => formatter
                .debug_struct("AccountingOverflow")
                .field("field", field)
                .finish(),
            Self::AllocationFailure { requested_spans } => formatter
                .debug_struct("AllocationFailure")
                .field("requested_spans", requested_spans)
                .finish(),
        }
    }
}

impl fmt::Display for DecodeErrorV0 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(self, formatter)
    }
}

impl Error for DecodeErrorV0 {}

#[derive(Clone, Copy, Eq, PartialEq)]
pub struct CapturePositionV0 {
    line_number: u64,
    resource_spans_index: u32,
    scope_spans_index: u32,
    span_index: u32,
}

impl CapturePositionV0 {
    pub const fn line_number(self) -> u64 {
        self.line_number
    }

    pub const fn resource_spans_index(self) -> u32 {
        self.resource_spans_index
    }

    pub const fn scope_spans_index(self) -> u32 {
        self.scope_spans_index
    }

    pub const fn span_index(self) -> u32 {
        self.span_index
    }
}

impl fmt::Debug for CapturePositionV0 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("CapturePositionV0(<redacted>)")
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum OtlpSpanStatusV0 {
    Unset,
    Ok,
    Error,
}

impl OtlpSpanStatusV0 {
    pub const fn is_error(self) -> bool {
        matches!(self, Self::Error)
    }
}

#[derive(Clone, Eq, PartialEq)]
pub struct DecodedSpanV0 {
    position: CapturePositionV0,
    service_name: Option<String>,
    name: String,
    trace_id: [u8; TRACE_ID_BYTES],
    span_id: [u8; SPAN_ID_BYTES],
    start_time_unix_ns: i64,
    end_time_unix_ns: i64,
    duration_ns: u64,
    status: OtlpSpanStatusV0,
}

impl DecodedSpanV0 {
    pub const fn position(&self) -> CapturePositionV0 {
        self.position
    }

    pub fn service_name(&self) -> Option<&str> {
        self.service_name.as_deref()
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub const fn trace_id(&self) -> &[u8; TRACE_ID_BYTES] {
        &self.trace_id
    }

    pub const fn span_id(&self) -> &[u8; SPAN_ID_BYTES] {
        &self.span_id
    }

    pub const fn start_time_unix_ns(&self) -> i64 {
        self.start_time_unix_ns
    }

    pub const fn end_time_unix_ns(&self) -> i64 {
        self.end_time_unix_ns
    }

    pub const fn duration_ns(&self) -> u64 {
        self.duration_ns
    }

    pub const fn status(&self) -> OtlpSpanStatusV0 {
        self.status
    }
}

impl fmt::Debug for DecodedSpanV0 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("DecodedSpanV0")
            .field("position", &self.position)
            .field("service_name_present", &self.service_name.is_some())
            .field("name_byte_len", &self.name.len())
            .field("trace_id", &"<redacted>")
            .field("span_id", &"<redacted>")
            .field("start_time_unix_ns", &"<redacted>")
            .field("end_time_unix_ns", &"<redacted>")
            .field("duration_ns", &"<redacted>")
            .field("status", &self.status)
            .finish()
    }
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct DecodeUsageV0 {
    decoded_spans: u64,
    resource_attributes_omitted: u64,
    scope_attributes_omitted: u64,
    span_attributes_omitted: u64,
    span_events_omitted: u64,
    span_links_omitted: u64,
    status_messages_omitted: u64,
    resource_dropped_attributes_reported: u64,
    scope_dropped_attributes_reported: u64,
    span_dropped_attributes_reported: u64,
    span_dropped_events_reported: u64,
    span_dropped_links_reported: u64,
}

impl DecodeUsageV0 {
    pub const fn decoded_spans(self) -> u64 {
        self.decoded_spans
    }

    pub const fn resource_attributes_omitted(self) -> u64 {
        self.resource_attributes_omitted
    }

    pub const fn scope_attributes_omitted(self) -> u64 {
        self.scope_attributes_omitted
    }

    pub const fn span_attributes_omitted(self) -> u64 {
        self.span_attributes_omitted
    }

    pub const fn span_events_omitted(self) -> u64 {
        self.span_events_omitted
    }

    pub const fn span_links_omitted(self) -> u64 {
        self.span_links_omitted
    }

    pub const fn status_messages_omitted(self) -> u64 {
        self.status_messages_omitted
    }

    pub const fn resource_dropped_attributes_reported(self) -> u64 {
        self.resource_dropped_attributes_reported
    }

    pub const fn scope_dropped_attributes_reported(self) -> u64 {
        self.scope_dropped_attributes_reported
    }

    pub const fn span_dropped_attributes_reported(self) -> u64 {
        self.span_dropped_attributes_reported
    }

    pub const fn span_dropped_events_reported(self) -> u64 {
        self.span_dropped_events_reported
    }

    pub const fn span_dropped_links_reported(self) -> u64 {
        self.span_dropped_links_reported
    }
}

#[derive(Clone, Eq, PartialEq)]
pub struct DecodedTraceLineV0 {
    line_number: u64,
    spans: Vec<DecodedSpanV0>,
    usage: DecodeUsageV0,
}

impl DecodedTraceLineV0 {
    pub const fn line_number(&self) -> u64 {
        self.line_number
    }

    pub fn spans(&self) -> impl ExactSizeIterator<Item = &DecodedSpanV0> {
        self.spans.iter()
    }

    pub fn len(&self) -> usize {
        self.spans.len()
    }

    pub fn is_empty(&self) -> bool {
        self.spans.is_empty()
    }

    pub const fn usage(&self) -> DecodeUsageV0 {
        self.usage
    }
}

impl fmt::Debug for DecodedTraceLineV0 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("DecodedTraceLineV0")
            .field("line_number", &"<redacted>")
            .field("span_count", &self.spans.len())
            .field("usage", &self.usage)
            .finish()
    }
}

#[derive(Deserialize)]
struct WireTracesDataV0 {
    #[serde(default, rename = "resourceSpans")]
    resource_spans: Vec<WireResourceSpansV0>,
    #[serde(default, rename = "resourceMetrics")]
    resource_metrics: WirePresenceV0,
    #[serde(default, rename = "resourceLogs")]
    resource_logs: WirePresenceV0,
    #[serde(default, rename = "resourceProfiles")]
    resource_profiles: WirePresenceV0,
}

#[derive(Deserialize)]
struct WireResourceSpansV0 {
    #[serde(default)]
    resource: Option<WireResourceV0>,
    #[serde(default, rename = "scopeSpans")]
    scope_spans: Vec<WireScopeSpansV0>,
}

#[derive(Deserialize)]
struct WireResourceV0 {
    #[serde(default)]
    attributes: Vec<WireKeyValueV0>,
    #[serde(default, rename = "droppedAttributesCount")]
    dropped_attributes_count: u32,
}

#[derive(Deserialize)]
struct WireKeyValueV0 {
    #[serde(default)]
    key: String,
    #[serde(default)]
    value: Option<WireAnyValueV0>,
}

#[derive(Deserialize)]
struct WireAnyValueV0 {
    #[serde(default, rename = "stringValue")]
    string_value: WireStringValueV0,
    #[serde(default, rename = "stringValueStrindex")]
    string_value_strindex: WirePresenceV0,
    #[serde(default, rename = "boolValue")]
    bool_value: WirePresenceV0,
    #[serde(default, rename = "intValue")]
    int_value: WirePresenceV0,
    #[serde(default, rename = "doubleValue")]
    double_value: WirePresenceV0,
    #[serde(default, rename = "arrayValue")]
    array_value: WirePresenceV0,
    #[serde(default, rename = "kvlistValue")]
    kvlist_value: WirePresenceV0,
    #[serde(default, rename = "bytesValue")]
    bytes_value: WirePresenceV0,
}

impl WireAnyValueV0 {
    const fn has_other_value_kind(&self) -> bool {
        self.string_value_strindex.is_present()
            || self.bool_value.is_present()
            || self.int_value.is_present()
            || self.double_value.is_present()
            || self.array_value.is_present()
            || self.kvlist_value.is_present()
            || self.bytes_value.is_present()
    }
}

#[derive(Default)]
struct WirePresenceV0(bool);

impl WirePresenceV0 {
    const fn is_present(&self) -> bool {
        self.0
    }
}

impl<'de> Deserialize<'de> for WirePresenceV0 {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        IgnoredAny::deserialize(deserializer)?;
        Ok(Self(true))
    }
}

#[derive(Default)]
enum WireStringValueV0 {
    #[default]
    Missing,
    String(String),
    NonString,
}

impl<'de> Deserialize<'de> for WireStringValueV0 {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_any(WireStringValueVisitorV0)
    }
}

struct WireStringValueVisitorV0;

impl<'de> Visitor<'de> for WireStringValueVisitorV0 {
    type Value = WireStringValueV0;

    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("an OTLP AnyValue member")
    }

    fn visit_str<E>(self, value: &str) -> Result<Self::Value, E> {
        Ok(WireStringValueV0::String(value.to_owned()))
    }

    fn visit_borrowed_str<E>(self, value: &'de str) -> Result<Self::Value, E> {
        Ok(WireStringValueV0::String(value.to_owned()))
    }

    fn visit_string<E>(self, value: String) -> Result<Self::Value, E> {
        Ok(WireStringValueV0::String(value))
    }

    fn visit_bool<E>(self, _value: bool) -> Result<Self::Value, E> {
        Ok(WireStringValueV0::NonString)
    }

    fn visit_i64<E>(self, _value: i64) -> Result<Self::Value, E> {
        Ok(WireStringValueV0::NonString)
    }

    fn visit_u64<E>(self, _value: u64) -> Result<Self::Value, E> {
        Ok(WireStringValueV0::NonString)
    }

    fn visit_f64<E>(self, _value: f64) -> Result<Self::Value, E> {
        Ok(WireStringValueV0::NonString)
    }

    fn visit_none<E>(self) -> Result<Self::Value, E> {
        Ok(WireStringValueV0::NonString)
    }

    fn visit_unit<E>(self) -> Result<Self::Value, E> {
        Ok(WireStringValueV0::NonString)
    }

    fn visit_seq<A>(self, mut sequence: A) -> Result<Self::Value, A::Error>
    where
        A: SeqAccess<'de>,
    {
        while sequence.next_element::<IgnoredAny>()?.is_some() {}
        Ok(WireStringValueV0::NonString)
    }

    fn visit_map<A>(self, mut map: A) -> Result<Self::Value, A::Error>
    where
        A: MapAccess<'de>,
    {
        while map.next_entry::<IgnoredAny, IgnoredAny>()?.is_some() {}
        Ok(WireStringValueV0::NonString)
    }
}

#[derive(Deserialize)]
struct WireScopeSpansV0 {
    #[serde(default)]
    scope: Option<WireScopeV0>,
    #[serde(default)]
    spans: Vec<WireSpanV0>,
}

#[derive(Deserialize)]
struct WireScopeV0 {
    #[serde(default)]
    attributes: Vec<IgnoredAny>,
    #[serde(default, rename = "droppedAttributesCount")]
    dropped_attributes_count: u32,
}

#[derive(Deserialize)]
struct WireSpanV0 {
    #[serde(default, rename = "traceId")]
    trace_id: Option<String>,
    #[serde(default, rename = "spanId")]
    span_id: Option<String>,
    #[serde(default)]
    name: Option<String>,
    #[serde(default, rename = "startTimeUnixNano")]
    start_time_unix_nano: Option<WireU64V0>,
    #[serde(default, rename = "endTimeUnixNano")]
    end_time_unix_nano: Option<WireU64V0>,
    #[serde(default)]
    status: Option<WireStatusV0>,
    #[serde(default)]
    attributes: Vec<IgnoredAny>,
    #[serde(default)]
    events: Vec<IgnoredAny>,
    #[serde(default)]
    links: Vec<IgnoredAny>,
    #[serde(default, rename = "droppedAttributesCount")]
    dropped_attributes_count: u32,
    #[serde(default, rename = "droppedEventsCount")]
    dropped_events_count: u32,
    #[serde(default, rename = "droppedLinksCount")]
    dropped_links_count: u32,
}

#[derive(Deserialize)]
struct WireStatusV0 {
    #[serde(default)]
    code: u32,
    #[serde(default)]
    message: Option<String>,
}

#[derive(Deserialize)]
#[serde(untagged)]
enum WireU64V0 {
    Number(u64),
    Decimal(String),
}

impl WireU64V0 {
    fn parse(self, field: TimestampFieldV0) -> Result<u64, DecodeErrorV0> {
        match self {
            Self::Number(value) => Ok(value),
            Self::Decimal(value) => {
                if value.is_empty() || !value.bytes().all(|byte| byte.is_ascii_digit()) {
                    return Err(DecodeErrorV0::InvalidTimestamp { field });
                }
                value
                    .parse()
                    .map_err(|_| DecodeErrorV0::InvalidTimestamp { field })
            }
        }
    }
}

pub fn decode_otlp_json_trace_record_v0(
    line_number: u64,
    input: &[u8],
) -> Result<DecodedTraceLineV0, DecodeErrorV0> {
    if input.len() > MAX_OTLP_JSON_LINE_BYTES_V0 {
        return Err(DecodeErrorV0::InputTooLarge {
            maximum: MAX_OTLP_JSON_LINE_BYTES_V0,
            actual: input.len(),
        });
    }
    if line_number == 0 {
        return Err(DecodeErrorV0::InvalidLineNumber);
    }
    let Some(json) = input.strip_suffix(b"\n") else {
        return Err(DecodeErrorV0::MissingTerminalLineFeed);
    };
    if json.contains(&b'\r') {
        return Err(DecodeErrorV0::CarriageReturn);
    }
    if json.contains(&b'\n') {
        return Err(DecodeErrorV0::EmbeddedLineFeed);
    }

    let wire: WireTracesDataV0 =
        serde_json::from_slice(json).map_err(|_| DecodeErrorV0::MalformedJson)?;
    reject_non_trace_signal(&wire)?;
    let (total_spans, usage) = validate_structure_and_usage(&wire)?;

    let mut spans = Vec::new();
    spans
        .try_reserve_exact(total_spans)
        .map_err(|_| DecodeErrorV0::AllocationFailure {
            requested_spans: total_spans,
        })?;

    for (resource_index, resource_spans) in wire.resource_spans.into_iter().enumerate() {
        let service_name = decode_service_name(resource_spans.resource)?;
        for (scope_index, scope_spans) in resource_spans.scope_spans.into_iter().enumerate() {
            for (span_index, span) in scope_spans.spans.into_iter().enumerate() {
                spans.push(decode_span(
                    line_number,
                    resource_index,
                    scope_index,
                    span_index,
                    service_name.as_deref(),
                    span,
                )?);
            }
        }
    }

    Ok(DecodedTraceLineV0 {
        line_number,
        spans,
        usage,
    })
}

fn reject_non_trace_signal(wire: &WireTracesDataV0) -> Result<(), DecodeErrorV0> {
    if wire.resource_metrics.is_present() {
        return Err(DecodeErrorV0::UnsupportedSignal {
            signal: OtlpSignalV0::Metrics,
        });
    }
    if wire.resource_logs.is_present() {
        return Err(DecodeErrorV0::UnsupportedSignal {
            signal: OtlpSignalV0::Logs,
        });
    }
    if wire.resource_profiles.is_present() {
        return Err(DecodeErrorV0::UnsupportedSignal {
            signal: OtlpSignalV0::Profiles,
        });
    }
    Ok(())
}

fn validate_structure_and_usage(
    wire: &WireTracesDataV0,
) -> Result<(usize, DecodeUsageV0), DecodeErrorV0> {
    check_bound(
        StructuralBoundV0::ResourceSpansPerLine,
        MAX_RESOURCE_SPANS_PER_LINE_V0,
        wire.resource_spans.len(),
    )?;

    let mut total_spans = 0usize;
    let mut usage = DecodeUsageV0::default();
    for resource_spans in &wire.resource_spans {
        if let Some(resource) = &resource_spans.resource {
            check_bound(
                StructuralBoundV0::ResourceAttributes,
                MAX_RESOURCE_ATTRIBUTES_V0,
                resource.attributes.len(),
            )?;
            let omitted = resource
                .attributes
                .iter()
                .filter(|attribute| attribute.key != SERVICE_NAME_ATTRIBUTE)
                .count();
            checked_usage_add(
                &mut usage.resource_attributes_omitted,
                omitted,
                AccountingFieldV0::ResourceAttributesOmitted,
            )?;
            checked_usage_add_u32(
                &mut usage.resource_dropped_attributes_reported,
                resource.dropped_attributes_count,
                AccountingFieldV0::ResourceDroppedAttributesReported,
            )?;
        }

        check_bound(
            StructuralBoundV0::ScopeSpansPerResource,
            MAX_SCOPE_SPANS_PER_RESOURCE_V0,
            resource_spans.scope_spans.len(),
        )?;
        for scope_spans in &resource_spans.scope_spans {
            if let Some(scope) = &scope_spans.scope {
                check_bound(
                    StructuralBoundV0::ScopeAttributes,
                    MAX_SCOPE_ATTRIBUTES_V0,
                    scope.attributes.len(),
                )?;
                checked_usage_add(
                    &mut usage.scope_attributes_omitted,
                    scope.attributes.len(),
                    AccountingFieldV0::ScopeAttributesOmitted,
                )?;
                checked_usage_add_u32(
                    &mut usage.scope_dropped_attributes_reported,
                    scope.dropped_attributes_count,
                    AccountingFieldV0::ScopeDroppedAttributesReported,
                )?;
            }

            check_bound(
                StructuralBoundV0::SpansPerScope,
                MAX_SPANS_PER_SCOPE_V0,
                scope_spans.spans.len(),
            )?;
            total_spans = total_spans.checked_add(scope_spans.spans.len()).ok_or(
                DecodeErrorV0::AccountingOverflow {
                    field: AccountingFieldV0::TotalSpans,
                },
            )?;
            check_bound(
                StructuralBoundV0::SpansPerLine,
                MAX_SPANS_PER_LINE_V0,
                total_spans,
            )?;

            for span in &scope_spans.spans {
                check_bound(
                    StructuralBoundV0::SpanAttributes,
                    MAX_SPAN_ATTRIBUTES_V0,
                    span.attributes.len(),
                )?;
                check_bound(
                    StructuralBoundV0::SpanEvents,
                    MAX_SPAN_EVENTS_V0,
                    span.events.len(),
                )?;
                check_bound(
                    StructuralBoundV0::SpanLinks,
                    MAX_SPAN_LINKS_V0,
                    span.links.len(),
                )?;
                checked_usage_add(
                    &mut usage.span_attributes_omitted,
                    span.attributes.len(),
                    AccountingFieldV0::SpanAttributesOmitted,
                )?;
                checked_usage_add(
                    &mut usage.span_events_omitted,
                    span.events.len(),
                    AccountingFieldV0::SpanEventsOmitted,
                )?;
                checked_usage_add(
                    &mut usage.span_links_omitted,
                    span.links.len(),
                    AccountingFieldV0::SpanLinksOmitted,
                )?;
                if span
                    .status
                    .as_ref()
                    .and_then(|status| status.message.as_ref())
                    .is_some_and(|message| !message.is_empty())
                {
                    checked_usage_add(
                        &mut usage.status_messages_omitted,
                        1,
                        AccountingFieldV0::StatusMessagesOmitted,
                    )?;
                }
                checked_usage_add_u32(
                    &mut usage.span_dropped_attributes_reported,
                    span.dropped_attributes_count,
                    AccountingFieldV0::SpanDroppedAttributesReported,
                )?;
                checked_usage_add_u32(
                    &mut usage.span_dropped_events_reported,
                    span.dropped_events_count,
                    AccountingFieldV0::SpanDroppedEventsReported,
                )?;
                checked_usage_add_u32(
                    &mut usage.span_dropped_links_reported,
                    span.dropped_links_count,
                    AccountingFieldV0::SpanDroppedLinksReported,
                )?;
            }
        }
    }
    usage.decoded_spans =
        u64::try_from(total_spans).map_err(|_| DecodeErrorV0::AccountingOverflow {
            field: AccountingFieldV0::TotalSpans,
        })?;
    Ok((total_spans, usage))
}

fn check_bound(
    bound: StructuralBoundV0,
    maximum: usize,
    actual: usize,
) -> Result<(), DecodeErrorV0> {
    if actual > maximum {
        return Err(DecodeErrorV0::TooMany {
            bound,
            maximum,
            actual,
        });
    }
    Ok(())
}

fn checked_usage_add(
    current: &mut u64,
    added: usize,
    field: AccountingFieldV0,
) -> Result<(), DecodeErrorV0> {
    let added = u64::try_from(added).map_err(|_| DecodeErrorV0::AccountingOverflow { field })?;
    *current = current
        .checked_add(added)
        .ok_or(DecodeErrorV0::AccountingOverflow { field })?;
    Ok(())
}

fn checked_usage_add_u32(
    current: &mut u64,
    added: u32,
    field: AccountingFieldV0,
) -> Result<(), DecodeErrorV0> {
    *current = current
        .checked_add(u64::from(added))
        .ok_or(DecodeErrorV0::AccountingOverflow { field })?;
    Ok(())
}

fn decode_service_name(resource: Option<WireResourceV0>) -> Result<Option<String>, DecodeErrorV0> {
    let Some(resource) = resource else {
        return Ok(None);
    };
    let mut service_name = None;
    for attribute in resource.attributes {
        if attribute.key != SERVICE_NAME_ATTRIBUTE {
            continue;
        }
        if service_name.is_some() {
            return Err(DecodeErrorV0::InvalidServiceName {
                problem: ServiceNameProblemV0::Duplicate,
                byte_len: 0,
            });
        }
        let Some(value) = attribute.value else {
            return Err(DecodeErrorV0::InvalidServiceName {
                problem: ServiceNameProblemV0::WrongValueKind,
                byte_len: 0,
            });
        };
        if value.has_other_value_kind() {
            return Err(DecodeErrorV0::InvalidServiceName {
                problem: ServiceNameProblemV0::WrongValueKind,
                byte_len: 0,
            });
        }
        let WireStringValueV0::String(value) = value.string_value else {
            return Err(DecodeErrorV0::InvalidServiceName {
                problem: ServiceNameProblemV0::WrongValueKind,
                byte_len: 0,
            });
        };
        if value.is_empty() {
            return Err(DecodeErrorV0::InvalidServiceName {
                problem: ServiceNameProblemV0::Empty,
                byte_len: 0,
            });
        }
        if value.len() > MAX_SERVICE_NAME_BYTES_V0 {
            return Err(DecodeErrorV0::InvalidServiceName {
                problem: ServiceNameProblemV0::TooLong,
                byte_len: value.len(),
            });
        }
        service_name = Some(value);
    }
    Ok(service_name)
}

fn decode_span(
    line_number: u64,
    resource_index: usize,
    scope_index: usize,
    span_index: usize,
    service_name: Option<&str>,
    span: WireSpanV0,
) -> Result<DecodedSpanV0, DecodeErrorV0> {
    let trace_id = decode_identifier::<TRACE_ID_BYTES>(span.trace_id, IdentifierFieldV0::TraceId)?;
    let span_id = decode_identifier::<SPAN_ID_BYTES>(span.span_id, IdentifierFieldV0::SpanId)?;
    let name = span.name.unwrap_or_default();
    if name.is_empty() || name.len() > MAX_SPAN_NAME_BYTES_V0 {
        return Err(DecodeErrorV0::InvalidSpanNameLength {
            maximum: MAX_SPAN_NAME_BYTES_V0,
            actual: name.len(),
        });
    }

    let start_time_unix_ns = decode_timestamp(
        span.start_time_unix_nano,
        TimestampFieldV0::StartTimeUnixNano,
    )?;
    let end_time_unix_ns =
        decode_timestamp(span.end_time_unix_nano, TimestampFieldV0::EndTimeUnixNano)?;
    if end_time_unix_ns < start_time_unix_ns {
        return Err(DecodeErrorV0::EndBeforeStart);
    }
    let duration_ns = u64::try_from(end_time_unix_ns - start_time_unix_ns)
        .map_err(|_| DecodeErrorV0::EndBeforeStart)?;

    let status_code = span.status.map_or(0, |status| status.code);
    let status = match status_code {
        0 => OtlpSpanStatusV0::Unset,
        1 => OtlpSpanStatusV0::Ok,
        2 => OtlpSpanStatusV0::Error,
        actual => return Err(DecodeErrorV0::InvalidStatusCode { actual }),
    };

    Ok(DecodedSpanV0 {
        position: CapturePositionV0 {
            line_number,
            resource_spans_index: u32::try_from(resource_index).map_err(|_| {
                DecodeErrorV0::AccountingOverflow {
                    field: AccountingFieldV0::TotalSpans,
                }
            })?,
            scope_spans_index: u32::try_from(scope_index).map_err(|_| {
                DecodeErrorV0::AccountingOverflow {
                    field: AccountingFieldV0::TotalSpans,
                }
            })?,
            span_index: u32::try_from(span_index).map_err(|_| {
                DecodeErrorV0::AccountingOverflow {
                    field: AccountingFieldV0::TotalSpans,
                }
            })?,
        },
        service_name: service_name.map(str::to_owned),
        name,
        trace_id,
        span_id,
        start_time_unix_ns,
        end_time_unix_ns,
        duration_ns,
        status,
    })
}

fn decode_timestamp(
    input: Option<WireU64V0>,
    field: TimestampFieldV0,
) -> Result<i64, DecodeErrorV0> {
    let value = input
        .ok_or(DecodeErrorV0::InvalidTimestamp { field })?
        .parse(field)?;
    i64::try_from(value).map_err(|_| DecodeErrorV0::TimestampOutOfRange { field })
}

fn decode_identifier<const N: usize>(
    input: Option<String>,
    field: IdentifierFieldV0,
) -> Result<[u8; N], DecodeErrorV0> {
    let Some(input) = input else {
        return Err(DecodeErrorV0::InvalidIdentifier {
            field,
            problem: IdentifierProblemV0::Missing,
            input_byte_len: 0,
        });
    };
    let expected_len = N.checked_mul(2).ok_or(DecodeErrorV0::InvalidIdentifier {
        field,
        problem: IdentifierProblemV0::InvalidLength,
        input_byte_len: input.len(),
    })?;
    if input.len() != expected_len {
        return Err(DecodeErrorV0::InvalidIdentifier {
            field,
            problem: IdentifierProblemV0::InvalidLength,
            input_byte_len: input.len(),
        });
    }

    let mut decoded = [0u8; N];
    let (pairs, remainder) = input.as_bytes().as_chunks::<2>();
    debug_assert!(remainder.is_empty());
    for (index, pair) in pairs.iter().enumerate() {
        let high = decode_hex_nibble(pair[0]).ok_or(DecodeErrorV0::InvalidIdentifier {
            field,
            problem: IdentifierProblemV0::InvalidHex,
            input_byte_len: input.len(),
        })?;
        let low = decode_hex_nibble(pair[1]).ok_or(DecodeErrorV0::InvalidIdentifier {
            field,
            problem: IdentifierProblemV0::InvalidHex,
            input_byte_len: input.len(),
        })?;
        decoded[index] = (high << 4) | low;
    }
    if decoded.iter().all(|byte| *byte == 0) {
        return Err(DecodeErrorV0::InvalidIdentifier {
            field,
            problem: IdentifierProblemV0::AllZero,
            input_byte_len: input.len(),
        });
    }
    Ok(decoded)
}

const fn decode_hex_nibble(byte: u8) -> Option<u8> {
    match byte {
        b'0'..=b'9' => Some(byte - b'0'),
        b'a'..=b'f' => Some(byte - b'a' + 10),
        b'A'..=b'F' => Some(byte - b'A' + 10),
        _ => None,
    }
}
