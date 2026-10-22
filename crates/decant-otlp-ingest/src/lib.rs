//! Bounded composition from OTLP trace JSONL records to finalized Decant state.

#![forbid(unsafe_code)]

use std::error::Error;
use std::fmt;
use std::io::BufRead;

use decant_ir::{SourceIdV0, SourcePositionKindV0};
use decant_jsonl::{BoundedJsonlReaderV0, JsonlLimitsV0, JsonlReadErrorV0};
use decant_otlp_adapter::{
    OTLP_FILE_CANONICALIZER_VERSION_V0, OtlpCanonicalizationErrorV0, OtlpFileCanonicalizerConfigV0,
    canonicalize_otlp_trace_line_v0,
};
use decant_otlp_json::{DecodeErrorV0, DecodeUsageV0, decode_otlp_json_trace_record_v0};
use decant_runtime::{
    ChannelLimitsV0, ChannelUsageV0, MAX_CHANNEL_ACCOUNTED_BYTES_V0, TryReceiveErrorV0,
    bounded_event_channel_v0,
};
use decant_state::{
    ADMISSION_VERSION_V0, AdmissionConfigErrorV0, AdmissionConfigV0, AdmissionErrorV0,
    AdmissionLimitsV0, AdmissionStateV0, ApplyOutcomeV0, FinalizeOutcomeV0, RejectionReasonV0,
};

pub const OTLP_JSONL_INGEST_VERSION_V0: u16 = 0;
pub const OTLP_JSONL_EPOCH_START_NS_V0: i64 = i64::MIN;
pub const OTLP_JSONL_EPOCH_END_NS_V0: i64 = i64::MAX;

#[derive(Clone, Eq, PartialEq)]
pub enum OtlpJsonlIngestConfigErrorV0 {
    UnsupportedVersion { actual: u16 },
    Canonicalizer(OtlpCanonicalizationErrorV0),
    Admission(AdmissionConfigErrorV0),
}

impl fmt::Debug for OtlpJsonlIngestConfigErrorV0 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::UnsupportedVersion { actual } => formatter
                .debug_struct("UnsupportedVersion")
                .field("actual", actual)
                .finish(),
            Self::Canonicalizer(error) => {
                formatter.debug_tuple("Canonicalizer").field(error).finish()
            }
            Self::Admission(error) => formatter.debug_tuple("Admission").field(error).finish(),
        }
    }
}

impl fmt::Display for OtlpJsonlIngestConfigErrorV0 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(self, formatter)
    }
}

impl Error for OtlpJsonlIngestConfigErrorV0 {}

#[derive(Clone, Eq, PartialEq)]
pub struct OtlpJsonlIngestConfigV0 {
    version: u16,
    source_id: SourceIdV0,
    jsonl_limits: JsonlLimitsV0,
    canonicalizer: OtlpFileCanonicalizerConfigV0,
    admission: AdmissionConfigV0,
}

impl OtlpJsonlIngestConfigV0 {
    pub fn new(
        version: u16,
        source_id: SourceIdV0,
        jsonl_limits: JsonlLimitsV0,
    ) -> Result<Self, OtlpJsonlIngestConfigErrorV0> {
        build_config(
            version,
            source_id,
            jsonl_limits,
            AdmissionLimitsV0::maximum(),
        )
    }

    pub const fn version(&self) -> u16 {
        self.version
    }

    pub fn source_id(&self) -> &SourceIdV0 {
        &self.source_id
    }

    pub const fn jsonl_limits(&self) -> JsonlLimitsV0 {
        self.jsonl_limits
    }
}

fn build_config(
    version: u16,
    source_id: SourceIdV0,
    jsonl_limits: JsonlLimitsV0,
    admission_limits: AdmissionLimitsV0,
) -> Result<OtlpJsonlIngestConfigV0, OtlpJsonlIngestConfigErrorV0> {
    if version != OTLP_JSONL_INGEST_VERSION_V0 {
        return Err(OtlpJsonlIngestConfigErrorV0::UnsupportedVersion { actual: version });
    }
    let canonicalizer =
        OtlpFileCanonicalizerConfigV0::new(OTLP_FILE_CANONICALIZER_VERSION_V0, source_id.clone())
            .map_err(OtlpJsonlIngestConfigErrorV0::Canonicalizer)?;
    let admission = AdmissionConfigV0::new_with_source_position_kind(
        ADMISSION_VERSION_V0,
        source_id.clone(),
        SourcePositionKindV0::OtlpFile,
        OTLP_JSONL_EPOCH_START_NS_V0,
        OTLP_JSONL_EPOCH_END_NS_V0,
        admission_limits,
    )
    .map_err(OtlpJsonlIngestConfigErrorV0::Admission)?;
    Ok(OtlpJsonlIngestConfigV0 {
        version,
        source_id,
        jsonl_limits,
        canonicalizer,
        admission,
    })
}

impl fmt::Debug for OtlpJsonlIngestConfigV0 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("OtlpJsonlIngestConfigV0")
            .field("version", &self.version)
            .field("source_id", &self.source_id)
            .field("jsonl_limits", &self.jsonl_limits)
            .field("admission", &self.admission)
            .finish_non_exhaustive()
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum OtlpDecodeAccountingFieldV0 {
    DecodedSpans,
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

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct OtlpDecodeTotalsV0 {
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

macro_rules! decode_total_accessors {
    ($($name:ident),+ $(,)?) => {
        $(pub const fn $name(self) -> u64 { self.$name })+
    };
}

impl OtlpDecodeTotalsV0 {
    decode_total_accessors!(
        decoded_spans,
        resource_attributes_omitted,
        scope_attributes_omitted,
        span_attributes_omitted,
        span_events_omitted,
        span_links_omitted,
        status_messages_omitted,
        resource_dropped_attributes_reported,
        scope_dropped_attributes_reported,
        span_dropped_attributes_reported,
        span_dropped_events_reported,
        span_dropped_links_reported,
    );

    fn add(&mut self, usage: DecodeUsageV0) -> Result<(), OtlpJsonlIngestErrorV0> {
        add_decode_counter(
            &mut self.decoded_spans,
            usage.decoded_spans(),
            OtlpDecodeAccountingFieldV0::DecodedSpans,
        )?;
        add_decode_counter(
            &mut self.resource_attributes_omitted,
            usage.resource_attributes_omitted(),
            OtlpDecodeAccountingFieldV0::ResourceAttributesOmitted,
        )?;
        add_decode_counter(
            &mut self.scope_attributes_omitted,
            usage.scope_attributes_omitted(),
            OtlpDecodeAccountingFieldV0::ScopeAttributesOmitted,
        )?;
        add_decode_counter(
            &mut self.span_attributes_omitted,
            usage.span_attributes_omitted(),
            OtlpDecodeAccountingFieldV0::SpanAttributesOmitted,
        )?;
        add_decode_counter(
            &mut self.span_events_omitted,
            usage.span_events_omitted(),
            OtlpDecodeAccountingFieldV0::SpanEventsOmitted,
        )?;
        add_decode_counter(
            &mut self.span_links_omitted,
            usage.span_links_omitted(),
            OtlpDecodeAccountingFieldV0::SpanLinksOmitted,
        )?;
        add_decode_counter(
            &mut self.status_messages_omitted,
            usage.status_messages_omitted(),
            OtlpDecodeAccountingFieldV0::StatusMessagesOmitted,
        )?;
        add_decode_counter(
            &mut self.resource_dropped_attributes_reported,
            usage.resource_dropped_attributes_reported(),
            OtlpDecodeAccountingFieldV0::ResourceDroppedAttributesReported,
        )?;
        add_decode_counter(
            &mut self.scope_dropped_attributes_reported,
            usage.scope_dropped_attributes_reported(),
            OtlpDecodeAccountingFieldV0::ScopeDroppedAttributesReported,
        )?;
        add_decode_counter(
            &mut self.span_dropped_attributes_reported,
            usage.span_dropped_attributes_reported(),
            OtlpDecodeAccountingFieldV0::SpanDroppedAttributesReported,
        )?;
        add_decode_counter(
            &mut self.span_dropped_events_reported,
            usage.span_dropped_events_reported(),
            OtlpDecodeAccountingFieldV0::SpanDroppedEventsReported,
        )?;
        add_decode_counter(
            &mut self.span_dropped_links_reported,
            usage.span_dropped_links_reported(),
            OtlpDecodeAccountingFieldV0::SpanDroppedLinksReported,
        )
    }
}

fn add_decode_counter(
    current: &mut u64,
    value: u64,
    field: OtlpDecodeAccountingFieldV0,
) -> Result<(), OtlpJsonlIngestErrorV0> {
    *current = current
        .checked_add(value)
        .ok_or(OtlpJsonlIngestErrorV0::DecodeAccountingOverflow { field })?;
    Ok(())
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct OtlpJsonlIngestUsageV0 {
    input_records: u64,
    input_bytes: u64,
    canonical_events: u64,
    enqueued_events: u64,
    received_events: u64,
    accepted_events: u64,
    duplicate_noops: u64,
    late_rejections: u64,
    decode: OtlpDecodeTotalsV0,
    transport: Option<ChannelUsageV0>,
}

impl OtlpJsonlIngestUsageV0 {
    pub const fn input_records(self) -> u64 {
        self.input_records
    }
    pub const fn input_bytes(self) -> u64 {
        self.input_bytes
    }
    pub const fn canonical_events(self) -> u64 {
        self.canonical_events
    }
    pub const fn enqueued_events(self) -> u64 {
        self.enqueued_events
    }
    pub const fn received_events(self) -> u64 {
        self.received_events
    }
    pub const fn accepted_events(self) -> u64 {
        self.accepted_events
    }
    pub const fn duplicate_noops(self) -> u64 {
        self.duplicate_noops
    }
    pub const fn late_rejections(self) -> u64 {
        self.late_rejections
    }
    pub const fn decode(self) -> OtlpDecodeTotalsV0 {
        self.decode
    }
    pub const fn transport(self) -> Option<ChannelUsageV0> {
        self.transport
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum OtlpIngestAccountingFieldV0 {
    CanonicalEvents,
    EnqueuedEvents,
    ReceivedEvents,
    AcceptedEvents,
    DuplicateNoOps,
    LateRejections,
}

#[derive(Clone, Eq, PartialEq)]
pub enum OtlpJsonlIngestErrorV0 {
    Jsonl(JsonlReadErrorV0),
    Decode {
        line_number: u64,
        error: DecodeErrorV0,
    },
    Canonicalization {
        line_number: u64,
        error: OtlpCanonicalizationErrorV0,
    },
    ChannelConfiguration,
    ChannelEnqueue {
        line_number: u64,
    },
    ChannelReceive {
        line_number: u64,
    },
    AdmissionApply {
        line_number: u64,
        error: AdmissionErrorV0,
    },
    UnexpectedAdmissionOutcome {
        line_number: u64,
        reason: RejectionReasonV0,
    },
    Finalization,
    DecodeAccountingOverflow {
        field: OtlpDecodeAccountingFieldV0,
    },
    IngestAccountingOverflow {
        field: OtlpIngestAccountingFieldV0,
    },
    AccountingInvariant,
}

impl fmt::Debug for OtlpJsonlIngestErrorV0 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Jsonl(error) => formatter.debug_tuple("Jsonl").field(error).finish(),
            Self::Decode { line_number, error } => formatter
                .debug_struct("Decode")
                .field("line_number", line_number)
                .field("error", error)
                .finish(),
            Self::Canonicalization { line_number, error } => formatter
                .debug_struct("Canonicalization")
                .field("line_number", line_number)
                .field("error", error)
                .finish(),
            Self::ChannelConfiguration => formatter.write_str("ChannelConfiguration"),
            Self::ChannelEnqueue { line_number } => formatter
                .debug_struct("ChannelEnqueue")
                .field("line_number", line_number)
                .finish(),
            Self::ChannelReceive { line_number } => formatter
                .debug_struct("ChannelReceive")
                .field("line_number", line_number)
                .finish(),
            Self::AdmissionApply { line_number, error } => formatter
                .debug_struct("AdmissionApply")
                .field("line_number", line_number)
                .field("error", error)
                .finish(),
            Self::UnexpectedAdmissionOutcome {
                line_number,
                reason,
            } => formatter
                .debug_struct("UnexpectedAdmissionOutcome")
                .field("line_number", line_number)
                .field("reason", reason)
                .finish(),
            Self::Finalization => formatter.write_str("Finalization"),
            Self::DecodeAccountingOverflow { field } => formatter
                .debug_struct("DecodeAccountingOverflow")
                .field("field", field)
                .finish(),
            Self::IngestAccountingOverflow { field } => formatter
                .debug_struct("IngestAccountingOverflow")
                .field("field", field)
                .finish(),
            Self::AccountingInvariant => formatter.write_str("AccountingInvariant"),
        }
    }
}

impl fmt::Display for OtlpJsonlIngestErrorV0 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(self, formatter)
    }
}

impl Error for OtlpJsonlIngestErrorV0 {}

pub struct OtlpJsonlIngestResultV0 {
    admission: AdmissionStateV0,
    usage: OtlpJsonlIngestUsageV0,
}

impl OtlpJsonlIngestResultV0 {
    pub const fn admission(&self) -> &AdmissionStateV0 {
        &self.admission
    }

    pub const fn usage(&self) -> OtlpJsonlIngestUsageV0 {
        self.usage
    }

    pub fn into_admission(self) -> AdmissionStateV0 {
        self.admission
    }
}

impl fmt::Debug for OtlpJsonlIngestResultV0 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("OtlpJsonlIngestResultV0")
            .field("usage", &self.usage)
            .field("admission", &"<redacted>")
            .finish()
    }
}

pub fn ingest_otlp_jsonl_v0<R: BufRead>(
    input: R,
    config: &OtlpJsonlIngestConfigV0,
) -> Result<OtlpJsonlIngestResultV0, OtlpJsonlIngestErrorV0> {
    ingest_with_admission_config(input, config, config.admission.clone())
}

fn ingest_with_admission_config<R: BufRead>(
    input: R,
    config: &OtlpJsonlIngestConfigV0,
    admission_config: AdmissionConfigV0,
) -> Result<OtlpJsonlIngestResultV0, OtlpJsonlIngestErrorV0> {
    ingest_with_projection(input, config, admission_config, false)
}

/// Evidence projection V1; framing, admission, and accounting stay unchanged.
pub fn ingest_otlp_jsonl_evidence_v1<R: BufRead>(
    input: R,
    config: &OtlpJsonlIngestConfigV0,
) -> Result<OtlpJsonlIngestResultV0, OtlpJsonlIngestErrorV0> {
    ingest_with_projection(input, config, config.admission.clone(), true)
}

fn ingest_with_projection<R: BufRead>(
    input: R,
    config: &OtlpJsonlIngestConfigV0,
    admission_config: AdmissionConfigV0,
    evidence: bool,
) -> Result<OtlpJsonlIngestResultV0, OtlpJsonlIngestErrorV0> {
    let channel_limits = ChannelLimitsV0::new(1, MAX_CHANNEL_ACCOUNTED_BYTES_V0)
        .map_err(|_| OtlpJsonlIngestErrorV0::ChannelConfiguration)?;
    let (mut sender, mut receiver) = bounded_event_channel_v0(channel_limits);
    let mut reader = BoundedJsonlReaderV0::new(input, config.jsonl_limits);
    let mut admission = AdmissionStateV0::new(admission_config);
    let mut usage = OtlpJsonlIngestUsageV0::default();

    while let Some(record) = reader
        .next_record()
        .map_err(OtlpJsonlIngestErrorV0::Jsonl)?
    {
        let line_number = record.line_number();
        let canonical = if evidence {
            let decoded = decant_otlp_json::evidence::decode_otlp_json_trace_record_v1(
                line_number,
                record.as_bytes(),
            )
            .map_err(|error| OtlpJsonlIngestErrorV0::Decode { line_number, error })?;
            decant_otlp_adapter::canonicalize_otlp_trace_line_v1(config.source_id(), &decoded)
        } else {
            let decoded = decode_otlp_json_trace_record_v0(line_number, record.as_bytes())
                .map_err(|error| OtlpJsonlIngestErrorV0::Decode { line_number, error })?;
            canonicalize_otlp_trace_line_v0(&config.canonicalizer, &decoded)
        }
        .map_err(|error| OtlpJsonlIngestErrorV0::Canonicalization { line_number, error })?;
        usage.decode.add(canonical.decode_usage())?;
        add_ingest_counter(
            &mut usage.canonical_events,
            u64::try_from(canonical.len()).map_err(|_| {
                OtlpJsonlIngestErrorV0::IngestAccountingOverflow {
                    field: OtlpIngestAccountingFieldV0::CanonicalEvents,
                }
            })?,
            OtlpIngestAccountingFieldV0::CanonicalEvents,
        )?;

        for event in canonical.events().cloned() {
            sender
                .try_enqueue(event)
                .map_err(|_| OtlpJsonlIngestErrorV0::ChannelEnqueue { line_number })?;
            add_ingest_counter(
                &mut usage.enqueued_events,
                1,
                OtlpIngestAccountingFieldV0::EnqueuedEvents,
            )?;
            let received = receiver
                .try_receive()
                .map_err(|_| OtlpJsonlIngestErrorV0::ChannelReceive { line_number })?;
            add_ingest_counter(
                &mut usage.received_events,
                1,
                OtlpIngestAccountingFieldV0::ReceivedEvents,
            )?;
            match admission
                .apply_event_v1(&received)
                .map_err(|error| OtlpJsonlIngestErrorV0::AdmissionApply { line_number, error })?
            {
                ApplyOutcomeV0::Accepted => add_ingest_counter(
                    &mut usage.accepted_events,
                    1,
                    OtlpIngestAccountingFieldV0::AcceptedEvents,
                )?,
                ApplyOutcomeV0::DuplicateNoOp => add_ingest_counter(
                    &mut usage.duplicate_noops,
                    1,
                    OtlpIngestAccountingFieldV0::DuplicateNoOps,
                )?,
                ApplyOutcomeV0::Rejected(RejectionReasonV0::Late) => add_ingest_counter(
                    &mut usage.late_rejections,
                    1,
                    OtlpIngestAccountingFieldV0::LateRejections,
                )?,
                ApplyOutcomeV0::Rejected(reason) => {
                    return Err(OtlpJsonlIngestErrorV0::UnexpectedAdmissionOutcome {
                        line_number,
                        reason,
                    });
                }
            }
        }
    }

    let jsonl_usage = reader.usage();
    usage.input_records = jsonl_usage.completed_records();
    usage.input_bytes = jsonl_usage.consumed_bytes();
    let transport = sender.usage();
    usage.transport = Some(transport);
    if receiver.try_receive() != Err(TryReceiveErrorV0::Empty)
        || transport != receiver.usage()
        || transport.limits() != channel_limits
        || transport.current_events() != 0
        || transport.current_accounted_bytes() != 0
    {
        return Err(OtlpJsonlIngestErrorV0::AccountingInvariant);
    }

    if admission.finalize_source(config.source_id(), OTLP_JSONL_EPOCH_END_NS_V0)
        != FinalizeOutcomeV0::Finalized
    {
        return Err(OtlpJsonlIngestErrorV0::Finalization);
    }
    verify_accounting(&admission, usage)?;
    Ok(OtlpJsonlIngestResultV0 { admission, usage })
}

fn add_ingest_counter(
    current: &mut u64,
    value: u64,
    field: OtlpIngestAccountingFieldV0,
) -> Result<(), OtlpJsonlIngestErrorV0> {
    *current = current
        .checked_add(value)
        .ok_or(OtlpJsonlIngestErrorV0::IngestAccountingOverflow { field })?;
    Ok(())
}

fn verify_accounting(
    admission: &AdmissionStateV0,
    usage: OtlpJsonlIngestUsageV0,
) -> Result<(), OtlpJsonlIngestErrorV0> {
    let classified = usage
        .accepted_events
        .checked_add(usage.duplicate_noops)
        .and_then(|count| count.checked_add(usage.late_rejections))
        .ok_or(OtlpJsonlIngestErrorV0::AccountingInvariant)?;
    let accepted = usize::try_from(usage.accepted_events)
        .map_err(|_| OtlpJsonlIngestErrorV0::AccountingInvariant)?;
    let transport = usage
        .transport
        .ok_or(OtlpJsonlIngestErrorV0::AccountingInvariant)?;
    let expected_high_water = usize::from(usage.canonical_events > 0);
    if usage.decode.decoded_spans != usage.canonical_events
        || usage.enqueued_events != usage.canonical_events
        || usage.received_events != usage.canonical_events
        || classified != usage.canonical_events
        || admission.usage().accepted_identities() != accepted
        || !admission.is_finalized()
        || transport.high_water_events() != expected_high_water
        || (expected_high_water == 0) != (transport.high_water_accounted_bytes() == 0)
    {
        return Err(OtlpJsonlIngestErrorV0::AccountingInvariant);
    }
    for reason in RejectionReasonV0::ALL {
        let range = admission.rejection_summary().range(reason);
        let expected = if reason == RejectionReasonV0::Late {
            usage.late_rejections
        } else {
            0
        };
        if range.count() != expected || range.count_saturated() {
            return Err(OtlpJsonlIngestErrorV0::AccountingInvariant);
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::io::Cursor;

    use decant_ir::SourceIdV0;
    use decant_jsonl::JsonlLimitsV0;
    use decant_state::{AdmissionLimitsV0, RejectionReasonV0};

    use super::*;

    #[test]
    fn capacity_rejection_is_a_whole_run_error() {
        let source = SourceIdV0::new("capacity-test").unwrap();
        let config = build_config(
            OTLP_JSONL_INGEST_VERSION_V0,
            source,
            JsonlLimitsV0::maximum(),
            AdmissionLimitsV0::new(1, 1_024 * 1_024).unwrap(),
        )
        .unwrap();
        let line = b"{\"resourceSpans\":[{\"scopeSpans\":[{\"spans\":[{\"traceId\":\"00000000000000000000000000000001\",\"spanId\":\"0000000000000001\",\"name\":\"a\",\"startTimeUnixNano\":\"1\",\"endTimeUnixNano\":\"2\"},{\"traceId\":\"00000000000000000000000000000002\",\"spanId\":\"0000000000000002\",\"name\":\"b\",\"startTimeUnixNano\":\"2\",\"endTimeUnixNano\":\"3\"}]}]}]}\n";
        assert!(matches!(
            ingest_with_admission_config(Cursor::new(line), &config, config.admission.clone()),
            Err(OtlpJsonlIngestErrorV0::UnexpectedAdmissionOutcome {
                reason: RejectionReasonV0::AcceptedIdentityCapacity,
                ..
            })
        ));
    }
}
