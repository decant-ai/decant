use std::collections::BTreeMap;
use std::error::Error;
use std::fmt;
use std::str;

use decant_ir::{
    CanonicalPayloadV0, ENVELOPE_VERSION_V0, EventEnvelopeV0, MAX_FIELD_NAME_BYTES,
    MAX_IDENTIFIER_BYTES, MAX_KEY_BYTES, MAX_PAYLOAD_FIELDS, MAX_SCALAR_BYTES, ScalarKindV0,
    ScalarValueV0, SchemaIdV0, SensitivityBitmapV0, SourceIdV0, SourcePositionKindV0,
    SourcePositionV0, ValidationErrorV0,
};

use crate::{
    ADMISSION_VERSION_V0, ALLOWED_LATENESS_NS_V0, AdmissionConfigErrorV0, AdmissionConfigV0,
    AdmissionErrorV0, AdmissionLimitsV0, AdmissionStateV0, MAX_ACCEPTED_EVENT_BYTES_V0,
    MAX_ACCEPTED_IDENTITIES_V0, REJECTION_CLASS_COUNT_V0, RejectionRangeV0, RejectionReasonV0,
    RejectionSummaryV0, accepted_event_accounted_bytes_v0,
};

pub const ADMISSION_SNAPSHOT_VERSION_V0: u16 = 0;
pub const MAX_ADMISSION_SNAPSHOT_BYTES_V0: usize = MAX_ACCEPTED_EVENT_BYTES_V0 + 1_024;
pub const ADMISSION_SNAPSHOT_VERSION_V1: u16 = 1;
pub const MAX_ADMISSION_SNAPSHOT_BYTES_V1: usize = MAX_ADMISSION_SNAPSHOT_BYTES_V0;

const SNAPSHOT_MAGIC_V0: &[u8; 8] = b"DECANTST";
const SOURCE_POSITION_SYNTHETIC_TAG_V0: u8 = 0;
const SOURCE_POSITION_OTLP_FILE_TAG_V1: u8 = 1;
const SOURCE_POSITION_KAFKA_TAG_V1: u8 = 2;
const SCALAR_NULL_TAG_V0: u8 = 0;
const SCALAR_BOOL_TAG_V0: u8 = 1;
const SCALAR_I64_TAG_V0: u8 = 2;
const SCALAR_U64_TAG_V0: u8 = 3;
const SCALAR_UTF8_TAG_V0: u8 = 4;
const SCALAR_BYTES_TAG_V0: u8 = 5;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AdmissionSnapshotFieldV0 {
    Magic,
    SnapshotVersion,
    AdmissionVersion,
    AllowedLateness,
    SourceId,
    Epoch,
    AdmissionLimits,
    Finality,
    RejectionSummary,
    RejectionRange,
    EventCount,
    EventRecord,
    EnvelopeVersion,
    SourcePosition,
    SchemaId,
    Key,
    EventTime,
    Sensitivity,
    Payload,
    FieldName,
    Scalar,
    AcceptedEventBytes,
    MaximumEventTime,
    Watermark,
    EndOfDocument,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AdmissionSnapshotOrderV0 {
    RejectionClasses,
    AcceptedEvents,
    PayloadFields,
}

#[derive(Clone, Eq, PartialEq)]
pub enum AdmissionSnapshotEncodeErrorV0 {
    ArithmeticOverflow { field: AdmissionSnapshotFieldV0 },
    SnapshotTooLarge { maximum: usize, attempted: usize },
    StateInvariant { field: AdmissionSnapshotFieldV0 },
}

impl fmt::Debug for AdmissionSnapshotEncodeErrorV0 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ArithmeticOverflow { field } => formatter
                .debug_struct("ArithmeticOverflow")
                .field("field", field)
                .finish(),
            Self::SnapshotTooLarge { maximum, attempted } => formatter
                .debug_struct("SnapshotTooLarge")
                .field("maximum", maximum)
                .field("attempted", attempted)
                .finish(),
            Self::StateInvariant { field } => formatter
                .debug_struct("StateInvariant")
                .field("field", field)
                .finish(),
        }
    }
}

impl fmt::Display for AdmissionSnapshotEncodeErrorV0 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(self, formatter)
    }
}

impl Error for AdmissionSnapshotEncodeErrorV0 {}

#[derive(Clone, Eq, PartialEq)]
pub enum AdmissionSnapshotDecodeErrorV0 {
    DocumentTooLarge {
        maximum: usize,
        actual: usize,
    },
    Truncated {
        field: AdmissionSnapshotFieldV0,
    },
    InvalidMagic,
    UnsupportedSnapshotVersion {
        actual: u16,
    },
    UnsupportedAdmissionVersion {
        actual: u16,
    },
    AllowedLatenessMismatch,
    InvalidBoolean {
        field: AdmissionSnapshotFieldV0,
    },
    InvalidTag {
        field: AdmissionSnapshotFieldV0,
    },
    InvalidUtf8 {
        field: AdmissionSnapshotFieldV0,
    },
    InvalidValue {
        field: AdmissionSnapshotFieldV0,
    },
    NonCanonicalOrder {
        order: AdmissionSnapshotOrderV0,
    },
    CapacityExceeded {
        field: AdmissionSnapshotFieldV0,
        maximum: usize,
        attempted: usize,
    },
    RecordLengthMismatch,
    TrailingBytes,
    ArithmeticOverflow {
        field: AdmissionSnapshotFieldV0,
    },
    InvalidConfiguration(AdmissionConfigErrorV0),
    InvalidEvent(ValidationErrorV0),
    InvalidAccounting(AdmissionErrorV0),
}

impl fmt::Debug for AdmissionSnapshotDecodeErrorV0 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::DocumentTooLarge { maximum, actual } => formatter
                .debug_struct("DocumentTooLarge")
                .field("maximum", maximum)
                .field("actual", actual)
                .finish(),
            Self::Truncated { field } => formatter
                .debug_struct("Truncated")
                .field("field", field)
                .finish(),
            Self::InvalidMagic => formatter.write_str("InvalidMagic"),
            Self::UnsupportedSnapshotVersion { actual } => formatter
                .debug_struct("UnsupportedSnapshotVersion")
                .field("actual", actual)
                .finish(),
            Self::UnsupportedAdmissionVersion { actual } => formatter
                .debug_struct("UnsupportedAdmissionVersion")
                .field("actual", actual)
                .finish(),
            Self::AllowedLatenessMismatch => formatter.write_str("AllowedLatenessMismatch"),
            Self::InvalidBoolean { field } => formatter
                .debug_struct("InvalidBoolean")
                .field("field", field)
                .finish(),
            Self::InvalidTag { field } => formatter
                .debug_struct("InvalidTag")
                .field("field", field)
                .finish(),
            Self::InvalidUtf8 { field } => formatter
                .debug_struct("InvalidUtf8")
                .field("field", field)
                .finish(),
            Self::InvalidValue { field } => formatter
                .debug_struct("InvalidValue")
                .field("field", field)
                .finish(),
            Self::NonCanonicalOrder { order } => formatter
                .debug_struct("NonCanonicalOrder")
                .field("order", order)
                .finish(),
            Self::CapacityExceeded {
                field,
                maximum,
                attempted,
            } => formatter
                .debug_struct("CapacityExceeded")
                .field("field", field)
                .field("maximum", maximum)
                .field("attempted", attempted)
                .finish(),
            Self::RecordLengthMismatch => formatter.write_str("RecordLengthMismatch"),
            Self::TrailingBytes => formatter.write_str("TrailingBytes"),
            Self::ArithmeticOverflow { field } => formatter
                .debug_struct("ArithmeticOverflow")
                .field("field", field)
                .finish(),
            Self::InvalidConfiguration(error) => formatter
                .debug_tuple("InvalidConfiguration")
                .field(error)
                .finish(),
            Self::InvalidEvent(error) => {
                formatter.debug_tuple("InvalidEvent").field(error).finish()
            }
            Self::InvalidAccounting(error) => formatter
                .debug_tuple("InvalidAccounting")
                .field(error)
                .finish(),
        }
    }
}

impl fmt::Display for AdmissionSnapshotDecodeErrorV0 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(self, formatter)
    }
}

impl Error for AdmissionSnapshotDecodeErrorV0 {}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AdmissionSnapshotFieldV1 {
    Magic,
    SnapshotVersion,
    AdmissionVersion,
    AllowedLateness,
    SourceId,
    SourcePositionKind,
    Epoch,
    AdmissionLimits,
    Finality,
    RejectionSummary,
    RejectionRange,
    EventCount,
    EventRecord,
    EnvelopeVersion,
    SourcePosition,
    SchemaId,
    Key,
    EventTime,
    Sensitivity,
    Payload,
    FieldName,
    Scalar,
    AcceptedEventBytes,
    MaximumEventTime,
    Watermark,
    EndOfDocument,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AdmissionSnapshotOrderV1 {
    RejectionClasses,
    AcceptedEvents,
    PayloadFields,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AdmissionSnapshotExpectedConfigFieldV1 {
    SourceId,
    SourcePositionKind,
    Epoch,
    AdmissionLimits,
}

#[derive(Clone, Eq, PartialEq)]
pub enum AdmissionSnapshotEncodeErrorV1 {
    ArithmeticOverflow { field: AdmissionSnapshotFieldV1 },
    SnapshotTooLarge { maximum: usize, attempted: usize },
    StateInvariant { field: AdmissionSnapshotFieldV1 },
}

impl fmt::Debug for AdmissionSnapshotEncodeErrorV1 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ArithmeticOverflow { field } => formatter
                .debug_struct("ArithmeticOverflow")
                .field("field", field)
                .finish(),
            Self::SnapshotTooLarge { maximum, attempted } => formatter
                .debug_struct("SnapshotTooLarge")
                .field("maximum", maximum)
                .field("attempted", attempted)
                .finish(),
            Self::StateInvariant { field } => formatter
                .debug_struct("StateInvariant")
                .field("field", field)
                .finish(),
        }
    }
}

impl fmt::Display for AdmissionSnapshotEncodeErrorV1 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(self, formatter)
    }
}

impl Error for AdmissionSnapshotEncodeErrorV1 {}

#[derive(Clone, Eq, PartialEq)]
pub enum AdmissionSnapshotDecodeErrorV1 {
    DocumentTooLarge {
        maximum: usize,
        actual: usize,
    },
    Truncated {
        field: AdmissionSnapshotFieldV1,
    },
    InvalidMagic,
    UnsupportedSnapshotVersion {
        actual: u16,
    },
    UnsupportedAdmissionVersion {
        actual: u16,
    },
    AllowedLatenessMismatch,
    InvalidBoolean {
        field: AdmissionSnapshotFieldV1,
    },
    InvalidTag {
        field: AdmissionSnapshotFieldV1,
    },
    InvalidUtf8 {
        field: AdmissionSnapshotFieldV1,
    },
    InvalidValue {
        field: AdmissionSnapshotFieldV1,
    },
    NonCanonicalOrder {
        order: AdmissionSnapshotOrderV1,
    },
    CapacityExceeded {
        field: AdmissionSnapshotFieldV1,
        maximum: usize,
        attempted: usize,
    },
    ExpectedConfigurationMismatch {
        field: AdmissionSnapshotExpectedConfigFieldV1,
    },
    RecordLengthMismatch,
    TrailingBytes,
    ArithmeticOverflow {
        field: AdmissionSnapshotFieldV1,
    },
    InvalidConfiguration(AdmissionConfigErrorV0),
    InvalidEvent(ValidationErrorV0),
    InvalidAccounting(AdmissionErrorV0),
}

impl fmt::Debug for AdmissionSnapshotDecodeErrorV1 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::DocumentTooLarge { maximum, actual } => formatter
                .debug_struct("DocumentTooLarge")
                .field("maximum", maximum)
                .field("actual", actual)
                .finish(),
            Self::Truncated { field } => formatter
                .debug_struct("Truncated")
                .field("field", field)
                .finish(),
            Self::InvalidMagic => formatter.write_str("InvalidMagic"),
            Self::UnsupportedSnapshotVersion { actual } => formatter
                .debug_struct("UnsupportedSnapshotVersion")
                .field("actual", actual)
                .finish(),
            Self::UnsupportedAdmissionVersion { actual } => formatter
                .debug_struct("UnsupportedAdmissionVersion")
                .field("actual", actual)
                .finish(),
            Self::AllowedLatenessMismatch => formatter.write_str("AllowedLatenessMismatch"),
            Self::InvalidBoolean { field } => formatter
                .debug_struct("InvalidBoolean")
                .field("field", field)
                .finish(),
            Self::InvalidTag { field } => formatter
                .debug_struct("InvalidTag")
                .field("field", field)
                .finish(),
            Self::InvalidUtf8 { field } => formatter
                .debug_struct("InvalidUtf8")
                .field("field", field)
                .finish(),
            Self::InvalidValue { field } => formatter
                .debug_struct("InvalidValue")
                .field("field", field)
                .finish(),
            Self::NonCanonicalOrder { order } => formatter
                .debug_struct("NonCanonicalOrder")
                .field("order", order)
                .finish(),
            Self::CapacityExceeded {
                field,
                maximum,
                attempted,
            } => formatter
                .debug_struct("CapacityExceeded")
                .field("field", field)
                .field("maximum", maximum)
                .field("attempted", attempted)
                .finish(),
            Self::ExpectedConfigurationMismatch { field } => formatter
                .debug_struct("ExpectedConfigurationMismatch")
                .field("field", field)
                .finish(),
            Self::RecordLengthMismatch => formatter.write_str("RecordLengthMismatch"),
            Self::TrailingBytes => formatter.write_str("TrailingBytes"),
            Self::ArithmeticOverflow { field } => formatter
                .debug_struct("ArithmeticOverflow")
                .field("field", field)
                .finish(),
            Self::InvalidConfiguration(error) => formatter
                .debug_tuple("InvalidConfiguration")
                .field(error)
                .finish(),
            Self::InvalidEvent(error) => {
                formatter.debug_tuple("InvalidEvent").field(error).finish()
            }
            Self::InvalidAccounting(error) => formatter
                .debug_tuple("InvalidAccounting")
                .field(error)
                .finish(),
        }
    }
}

impl fmt::Display for AdmissionSnapshotDecodeErrorV1 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(self, formatter)
    }
}

impl Error for AdmissionSnapshotDecodeErrorV1 {}

const fn field_v1(field: AdmissionSnapshotFieldV0) -> AdmissionSnapshotFieldV1 {
    match field {
        AdmissionSnapshotFieldV0::Magic => AdmissionSnapshotFieldV1::Magic,
        AdmissionSnapshotFieldV0::SnapshotVersion => AdmissionSnapshotFieldV1::SnapshotVersion,
        AdmissionSnapshotFieldV0::AdmissionVersion => AdmissionSnapshotFieldV1::AdmissionVersion,
        AdmissionSnapshotFieldV0::AllowedLateness => AdmissionSnapshotFieldV1::AllowedLateness,
        AdmissionSnapshotFieldV0::SourceId => AdmissionSnapshotFieldV1::SourceId,
        AdmissionSnapshotFieldV0::Epoch => AdmissionSnapshotFieldV1::Epoch,
        AdmissionSnapshotFieldV0::AdmissionLimits => AdmissionSnapshotFieldV1::AdmissionLimits,
        AdmissionSnapshotFieldV0::Finality => AdmissionSnapshotFieldV1::Finality,
        AdmissionSnapshotFieldV0::RejectionSummary => AdmissionSnapshotFieldV1::RejectionSummary,
        AdmissionSnapshotFieldV0::RejectionRange => AdmissionSnapshotFieldV1::RejectionRange,
        AdmissionSnapshotFieldV0::EventCount => AdmissionSnapshotFieldV1::EventCount,
        AdmissionSnapshotFieldV0::EventRecord => AdmissionSnapshotFieldV1::EventRecord,
        AdmissionSnapshotFieldV0::EnvelopeVersion => AdmissionSnapshotFieldV1::EnvelopeVersion,
        AdmissionSnapshotFieldV0::SourcePosition => AdmissionSnapshotFieldV1::SourcePosition,
        AdmissionSnapshotFieldV0::SchemaId => AdmissionSnapshotFieldV1::SchemaId,
        AdmissionSnapshotFieldV0::Key => AdmissionSnapshotFieldV1::Key,
        AdmissionSnapshotFieldV0::EventTime => AdmissionSnapshotFieldV1::EventTime,
        AdmissionSnapshotFieldV0::Sensitivity => AdmissionSnapshotFieldV1::Sensitivity,
        AdmissionSnapshotFieldV0::Payload => AdmissionSnapshotFieldV1::Payload,
        AdmissionSnapshotFieldV0::FieldName => AdmissionSnapshotFieldV1::FieldName,
        AdmissionSnapshotFieldV0::Scalar => AdmissionSnapshotFieldV1::Scalar,
        AdmissionSnapshotFieldV0::AcceptedEventBytes => {
            AdmissionSnapshotFieldV1::AcceptedEventBytes
        }
        AdmissionSnapshotFieldV0::MaximumEventTime => AdmissionSnapshotFieldV1::MaximumEventTime,
        AdmissionSnapshotFieldV0::Watermark => AdmissionSnapshotFieldV1::Watermark,
        AdmissionSnapshotFieldV0::EndOfDocument => AdmissionSnapshotFieldV1::EndOfDocument,
    }
}

const fn order_v1(order: AdmissionSnapshotOrderV0) -> AdmissionSnapshotOrderV1 {
    match order {
        AdmissionSnapshotOrderV0::RejectionClasses => AdmissionSnapshotOrderV1::RejectionClasses,
        AdmissionSnapshotOrderV0::AcceptedEvents => AdmissionSnapshotOrderV1::AcceptedEvents,
        AdmissionSnapshotOrderV0::PayloadFields => AdmissionSnapshotOrderV1::PayloadFields,
    }
}

impl From<AdmissionSnapshotEncodeErrorV0> for AdmissionSnapshotEncodeErrorV1 {
    fn from(error: AdmissionSnapshotEncodeErrorV0) -> Self {
        match error {
            AdmissionSnapshotEncodeErrorV0::ArithmeticOverflow { field } => {
                Self::ArithmeticOverflow {
                    field: field_v1(field),
                }
            }
            AdmissionSnapshotEncodeErrorV0::SnapshotTooLarge { maximum, attempted } => {
                Self::SnapshotTooLarge { maximum, attempted }
            }
            AdmissionSnapshotEncodeErrorV0::StateInvariant { field } => Self::StateInvariant {
                field: field_v1(field),
            },
        }
    }
}

impl From<AdmissionSnapshotDecodeErrorV0> for AdmissionSnapshotDecodeErrorV1 {
    fn from(error: AdmissionSnapshotDecodeErrorV0) -> Self {
        match error {
            AdmissionSnapshotDecodeErrorV0::DocumentTooLarge { maximum, actual } => {
                Self::DocumentTooLarge { maximum, actual }
            }
            AdmissionSnapshotDecodeErrorV0::Truncated { field } => Self::Truncated {
                field: field_v1(field),
            },
            AdmissionSnapshotDecodeErrorV0::InvalidMagic => Self::InvalidMagic,
            AdmissionSnapshotDecodeErrorV0::UnsupportedSnapshotVersion { actual } => {
                Self::UnsupportedSnapshotVersion { actual }
            }
            AdmissionSnapshotDecodeErrorV0::UnsupportedAdmissionVersion { actual } => {
                Self::UnsupportedAdmissionVersion { actual }
            }
            AdmissionSnapshotDecodeErrorV0::AllowedLatenessMismatch => {
                Self::AllowedLatenessMismatch
            }
            AdmissionSnapshotDecodeErrorV0::InvalidBoolean { field } => Self::InvalidBoolean {
                field: field_v1(field),
            },
            AdmissionSnapshotDecodeErrorV0::InvalidTag { field } => Self::InvalidTag {
                field: field_v1(field),
            },
            AdmissionSnapshotDecodeErrorV0::InvalidUtf8 { field } => Self::InvalidUtf8 {
                field: field_v1(field),
            },
            AdmissionSnapshotDecodeErrorV0::InvalidValue { field } => Self::InvalidValue {
                field: field_v1(field),
            },
            AdmissionSnapshotDecodeErrorV0::NonCanonicalOrder { order } => {
                Self::NonCanonicalOrder {
                    order: order_v1(order),
                }
            }
            AdmissionSnapshotDecodeErrorV0::CapacityExceeded {
                field,
                maximum,
                attempted,
            } => Self::CapacityExceeded {
                field: field_v1(field),
                maximum,
                attempted,
            },
            AdmissionSnapshotDecodeErrorV0::RecordLengthMismatch => Self::RecordLengthMismatch,
            AdmissionSnapshotDecodeErrorV0::TrailingBytes => Self::TrailingBytes,
            AdmissionSnapshotDecodeErrorV0::ArithmeticOverflow { field } => {
                Self::ArithmeticOverflow {
                    field: field_v1(field),
                }
            }
            AdmissionSnapshotDecodeErrorV0::InvalidConfiguration(error) => {
                Self::InvalidConfiguration(error)
            }
            AdmissionSnapshotDecodeErrorV0::InvalidEvent(error) => Self::InvalidEvent(error),
            AdmissionSnapshotDecodeErrorV0::InvalidAccounting(error) => {
                Self::InvalidAccounting(error)
            }
        }
    }
}

pub fn encode_admission_snapshot_v0(
    state: &AdmissionStateV0,
) -> Result<Vec<u8>, AdmissionSnapshotEncodeErrorV0> {
    validate_state_for_encoding(state)?;
    let initial_capacity = state
        .accepted_event_bytes
        .checked_add(1_024)
        .ok_or(AdmissionSnapshotEncodeErrorV0::ArithmeticOverflow {
            field: AdmissionSnapshotFieldV0::AcceptedEventBytes,
        })?
        .min(MAX_ADMISSION_SNAPSHOT_BYTES_V0);
    let mut writer = SnapshotWriterV0::with_capacity(initial_capacity);

    writer.write_bytes(SNAPSHOT_MAGIC_V0)?;
    writer.write_u16(ADMISSION_SNAPSHOT_VERSION_V0)?;
    writer.write_u16(state.config.version())?;
    writer.write_i64(ALLOWED_LATENESS_NS_V0)?;
    writer.write_length_prefixed(state.config.source_id().as_str().as_bytes())?;
    writer.write_i64(state.config.epoch_start_ns())?;
    writer.write_i64(state.config.epoch_end_ns())?;
    writer.write_u64(usize_to_u64(
        state.config.limits().accepted_identity_limit(),
        AdmissionSnapshotFieldV0::AdmissionLimits,
    )?)?;
    writer.write_u64(usize_to_u64(
        state.config.limits().accepted_event_byte_limit(),
        AdmissionSnapshotFieldV0::AdmissionLimits,
    )?)?;
    writer.write_bool(state.finalized)?;
    writer.write_u32(usize_to_u32(
        REJECTION_CLASS_COUNT_V0,
        AdmissionSnapshotFieldV0::RejectionSummary,
    )?)?;
    for (tag, reason) in RejectionReasonV0::ALL.into_iter().enumerate() {
        writer.write_u8(u8::try_from(tag).map_err(|_| {
            AdmissionSnapshotEncodeErrorV0::ArithmeticOverflow {
                field: AdmissionSnapshotFieldV0::RejectionSummary,
            }
        })?)?;
        encode_rejection_range(&mut writer, state.rejection_summary.range(reason))?;
    }
    writer.write_u32(usize_to_u32(
        state.accepted_events.len(),
        AdmissionSnapshotFieldV0::EventCount,
    )?)?;
    for event in state.accepted_events.values() {
        encode_event(&mut writer, event)?;
    }
    Ok(writer.finish())
}

pub fn encode_admission_snapshot_v1(
    state: &AdmissionStateV0,
) -> Result<Vec<u8>, AdmissionSnapshotEncodeErrorV1> {
    validate_state_for_encoding_with_kind(state, state.config.source_position_kind())?;
    let initial_capacity = state
        .accepted_event_bytes
        .checked_add(1_024)
        .ok_or(AdmissionSnapshotEncodeErrorV1::ArithmeticOverflow {
            field: AdmissionSnapshotFieldV1::AcceptedEventBytes,
        })?
        .min(MAX_ADMISSION_SNAPSHOT_BYTES_V1);
    let mut writer = SnapshotWriterV0::with_capacity(initial_capacity);

    writer.write_bytes(SNAPSHOT_MAGIC_V0)?;
    writer.write_u16(ADMISSION_SNAPSHOT_VERSION_V1)?;
    writer.write_u16(state.config.version())?;
    writer.write_i64(ALLOWED_LATENESS_NS_V0)?;
    writer.write_length_prefixed(state.config.source_id().as_str().as_bytes())?;
    writer.write_u8(source_position_kind_tag_v1(
        state.config.source_position_kind(),
    ))?;
    writer.write_i64(state.config.epoch_start_ns())?;
    writer.write_i64(state.config.epoch_end_ns())?;
    writer.write_u64(usize_to_u64(
        state.config.limits().accepted_identity_limit(),
        AdmissionSnapshotFieldV0::AdmissionLimits,
    )?)?;
    writer.write_u64(usize_to_u64(
        state.config.limits().accepted_event_byte_limit(),
        AdmissionSnapshotFieldV0::AdmissionLimits,
    )?)?;
    writer.write_bool(state.finalized)?;
    writer.write_u32(usize_to_u32(
        REJECTION_CLASS_COUNT_V0,
        AdmissionSnapshotFieldV0::RejectionSummary,
    )?)?;
    for (tag, reason) in RejectionReasonV0::ALL.into_iter().enumerate() {
        writer.write_u8(u8::try_from(tag).map_err(|_| {
            AdmissionSnapshotEncodeErrorV1::ArithmeticOverflow {
                field: AdmissionSnapshotFieldV1::RejectionSummary,
            }
        })?)?;
        encode_rejection_range(&mut writer, state.rejection_summary.range(reason))?;
    }
    writer.write_u32(usize_to_u32(
        state.accepted_events.len(),
        AdmissionSnapshotFieldV0::EventCount,
    )?)?;
    for event in state.accepted_events.values() {
        encode_event_v1(&mut writer, event)?;
    }
    Ok(writer.finish())
}

pub fn restore_admission_snapshot_v0(
    input: &[u8],
) -> Result<AdmissionStateV0, AdmissionSnapshotDecodeErrorV0> {
    if input.len() > MAX_ADMISSION_SNAPSHOT_BYTES_V0 {
        return Err(AdmissionSnapshotDecodeErrorV0::DocumentTooLarge {
            maximum: MAX_ADMISSION_SNAPSHOT_BYTES_V0,
            actual: input.len(),
        });
    }
    let mut cursor = SnapshotCursorV0::new(input);
    if cursor.read_exact(SNAPSHOT_MAGIC_V0.len(), AdmissionSnapshotFieldV0::Magic)?
        != SNAPSHOT_MAGIC_V0
    {
        return Err(AdmissionSnapshotDecodeErrorV0::InvalidMagic);
    }
    let snapshot_version = cursor.read_u16(AdmissionSnapshotFieldV0::SnapshotVersion)?;
    if snapshot_version != ADMISSION_SNAPSHOT_VERSION_V0 {
        return Err(AdmissionSnapshotDecodeErrorV0::UnsupportedSnapshotVersion {
            actual: snapshot_version,
        });
    }
    let admission_version = cursor.read_u16(AdmissionSnapshotFieldV0::AdmissionVersion)?;
    if admission_version != ADMISSION_VERSION_V0 {
        return Err(
            AdmissionSnapshotDecodeErrorV0::UnsupportedAdmissionVersion {
                actual: admission_version,
            },
        );
    }
    if cursor.read_i64(AdmissionSnapshotFieldV0::AllowedLateness)? != ALLOWED_LATENESS_NS_V0 {
        return Err(AdmissionSnapshotDecodeErrorV0::AllowedLatenessMismatch);
    }
    let source_id = SourceIdV0::new(
        cursor.read_bounded_str(AdmissionSnapshotFieldV0::SourceId, MAX_IDENTIFIER_BYTES)?,
    )
    .map_err(AdmissionSnapshotDecodeErrorV0::InvalidEvent)?;
    let epoch_start_ns = cursor.read_i64(AdmissionSnapshotFieldV0::Epoch)?;
    let epoch_end_ns = cursor.read_i64(AdmissionSnapshotFieldV0::Epoch)?;
    let accepted_identity_limit = cursor.read_usize(AdmissionSnapshotFieldV0::AdmissionLimits)?;
    let accepted_event_byte_limit = cursor.read_usize(AdmissionSnapshotFieldV0::AdmissionLimits)?;
    let limits = AdmissionLimitsV0::new(accepted_identity_limit, accepted_event_byte_limit)
        .map_err(AdmissionSnapshotDecodeErrorV0::InvalidConfiguration)?;
    let config = AdmissionConfigV0::new(
        admission_version,
        source_id,
        epoch_start_ns,
        epoch_end_ns,
        limits,
    )
    .map_err(AdmissionSnapshotDecodeErrorV0::InvalidConfiguration)?;
    decode_state_body(&mut cursor, config, decode_event)
}

pub fn restore_admission_snapshot_v1(
    input: &[u8],
    expected_config: &AdmissionConfigV0,
) -> Result<AdmissionStateV0, AdmissionSnapshotDecodeErrorV1> {
    if input.len() > MAX_ADMISSION_SNAPSHOT_BYTES_V1 {
        return Err(AdmissionSnapshotDecodeErrorV1::DocumentTooLarge {
            maximum: MAX_ADMISSION_SNAPSHOT_BYTES_V1,
            actual: input.len(),
        });
    }
    let mut cursor = SnapshotCursorV0::new(input);
    if cursor.read_exact(SNAPSHOT_MAGIC_V0.len(), AdmissionSnapshotFieldV0::Magic)?
        != SNAPSHOT_MAGIC_V0
    {
        return Err(AdmissionSnapshotDecodeErrorV1::InvalidMagic);
    }
    let snapshot_version = cursor.read_u16(AdmissionSnapshotFieldV0::SnapshotVersion)?;
    if snapshot_version != ADMISSION_SNAPSHOT_VERSION_V1 {
        return Err(AdmissionSnapshotDecodeErrorV1::UnsupportedSnapshotVersion {
            actual: snapshot_version,
        });
    }
    let admission_version = cursor.read_u16(AdmissionSnapshotFieldV0::AdmissionVersion)?;
    if admission_version != ADMISSION_VERSION_V0 {
        return Err(
            AdmissionSnapshotDecodeErrorV1::UnsupportedAdmissionVersion {
                actual: admission_version,
            },
        );
    }
    if cursor.read_i64(AdmissionSnapshotFieldV0::AllowedLateness)? != ALLOWED_LATENESS_NS_V0 {
        return Err(AdmissionSnapshotDecodeErrorV1::AllowedLatenessMismatch);
    }
    let source_id = SourceIdV0::new(
        cursor.read_bounded_str(AdmissionSnapshotFieldV0::SourceId, MAX_IDENTIFIER_BYTES)?,
    )
    .map_err(AdmissionSnapshotDecodeErrorV1::InvalidEvent)?;
    let source_position_kind =
        decode_source_position_kind_v1(read_source_position_kind_tag_v1(&mut cursor)?)?;
    let epoch_start_ns = cursor.read_i64(AdmissionSnapshotFieldV0::Epoch)?;
    let epoch_end_ns = cursor.read_i64(AdmissionSnapshotFieldV0::Epoch)?;
    let accepted_identity_limit = cursor.read_usize(AdmissionSnapshotFieldV0::AdmissionLimits)?;
    let accepted_event_byte_limit = cursor.read_usize(AdmissionSnapshotFieldV0::AdmissionLimits)?;
    let limits = AdmissionLimitsV0::new(accepted_identity_limit, accepted_event_byte_limit)
        .map_err(AdmissionSnapshotDecodeErrorV1::InvalidConfiguration)?;
    let config = AdmissionConfigV0::new_with_source_position_kind(
        admission_version,
        source_id,
        source_position_kind,
        epoch_start_ns,
        epoch_end_ns,
        limits,
    )
    .map_err(AdmissionSnapshotDecodeErrorV1::InvalidConfiguration)?;
    validate_expected_config_v1(&config, expected_config)?;

    decode_state_body(&mut cursor, config, decode_event_v1).map_err(Into::into)
}

fn decode_state_body(
    cursor: &mut SnapshotCursorV0<'_>,
    config: AdmissionConfigV0,
    event_decoder: fn(
        &[u8],
        &AdmissionConfigV0,
    ) -> Result<EventEnvelopeV0, AdmissionSnapshotDecodeErrorV0>,
) -> Result<AdmissionStateV0, AdmissionSnapshotDecodeErrorV0> {
    let limits = config.limits();
    let finalized = cursor.read_bool(AdmissionSnapshotFieldV0::Finality)?;
    let rejection_summary = decode_rejection_summary(cursor)?;

    let event_count = cursor.read_count(AdmissionSnapshotFieldV0::EventCount)?;
    if event_count > limits.accepted_identity_limit() {
        return Err(AdmissionSnapshotDecodeErrorV0::CapacityExceeded {
            field: AdmissionSnapshotFieldV0::EventCount,
            maximum: limits.accepted_identity_limit(),
            attempted: event_count,
        });
    }
    if event_count > MAX_ACCEPTED_IDENTITIES_V0 {
        return Err(AdmissionSnapshotDecodeErrorV0::CapacityExceeded {
            field: AdmissionSnapshotFieldV0::EventCount,
            maximum: MAX_ACCEPTED_IDENTITIES_V0,
            attempted: event_count,
        });
    }

    let mut accepted_events = BTreeMap::new();
    let mut accepted_event_bytes = 0_usize;
    let mut maximum_accepted_event_time_ns: Option<i64> = None;
    let mut previous_position = None;
    for _ in 0..event_count {
        let record_length = cursor.read_count(AdmissionSnapshotFieldV0::EventRecord)?;
        let record = cursor.read_exact(record_length, AdmissionSnapshotFieldV0::EventRecord)?;
        let event = event_decoder(record, &config)?;
        let position = event.source_position();
        if previous_position.is_some_and(|previous| position <= previous) {
            return Err(AdmissionSnapshotDecodeErrorV0::NonCanonicalOrder {
                order: AdmissionSnapshotOrderV0::AcceptedEvents,
            });
        }
        previous_position = Some(position);
        if !(config.epoch_start_ns()..config.epoch_end_ns()).contains(&event.event_time_ns()) {
            return Err(AdmissionSnapshotDecodeErrorV0::InvalidValue {
                field: AdmissionSnapshotFieldV0::EventTime,
            });
        }
        let event_bytes = accepted_event_accounted_bytes_v0(&event)
            .map_err(AdmissionSnapshotDecodeErrorV0::InvalidAccounting)?;
        let prospective_bytes = accepted_event_bytes.checked_add(event_bytes).ok_or(
            AdmissionSnapshotDecodeErrorV0::ArithmeticOverflow {
                field: AdmissionSnapshotFieldV0::AcceptedEventBytes,
            },
        )?;
        if prospective_bytes > limits.accepted_event_byte_limit() {
            return Err(AdmissionSnapshotDecodeErrorV0::CapacityExceeded {
                field: AdmissionSnapshotFieldV0::AcceptedEventBytes,
                maximum: limits.accepted_event_byte_limit(),
                attempted: prospective_bytes,
            });
        }
        maximum_accepted_event_time_ns = Some(
            maximum_accepted_event_time_ns.map_or(event.event_time_ns(), |current| {
                current.max(event.event_time_ns())
            }),
        );
        if accepted_events.insert(position, event).is_some() {
            return Err(AdmissionSnapshotDecodeErrorV0::NonCanonicalOrder {
                order: AdmissionSnapshotOrderV0::AcceptedEvents,
            });
        }
        accepted_event_bytes = prospective_bytes;
    }
    if !cursor.is_finished() {
        return Err(AdmissionSnapshotDecodeErrorV0::TrailingBytes);
    }

    let watermark_ns = if finalized {
        Some(config.epoch_end_ns())
    } else {
        maximum_accepted_event_time_ns.map(|maximum| maximum.saturating_sub(ALLOWED_LATENESS_NS_V0))
    };
    Ok(AdmissionStateV0 {
        config,
        accepted_events,
        accepted_event_bytes,
        maximum_accepted_event_time_ns,
        watermark_ns,
        finalized,
        rejection_summary,
    })
}

const fn source_position_kind_tag_v1(kind: SourcePositionKindV0) -> u8 {
    match kind {
        SourcePositionKindV0::Synthetic => SOURCE_POSITION_SYNTHETIC_TAG_V0,
        SourcePositionKindV0::OtlpFile => SOURCE_POSITION_OTLP_FILE_TAG_V1,
        SourcePositionKindV0::Kafka => SOURCE_POSITION_KAFKA_TAG_V1,
    }
}

fn decode_source_position_kind_v1(
    tag: u8,
) -> Result<SourcePositionKindV0, AdmissionSnapshotDecodeErrorV1> {
    match tag {
        SOURCE_POSITION_SYNTHETIC_TAG_V0 => Ok(SourcePositionKindV0::Synthetic),
        SOURCE_POSITION_OTLP_FILE_TAG_V1 => Ok(SourcePositionKindV0::OtlpFile),
        SOURCE_POSITION_KAFKA_TAG_V1 => Ok(SourcePositionKindV0::Kafka),
        _ => Err(AdmissionSnapshotDecodeErrorV1::InvalidTag {
            field: AdmissionSnapshotFieldV1::SourcePositionKind,
        }),
    }
}

fn read_source_position_kind_tag_v1(
    cursor: &mut SnapshotCursorV0<'_>,
) -> Result<u8, AdmissionSnapshotDecodeErrorV1> {
    cursor
        .read_u8(AdmissionSnapshotFieldV0::SourcePosition)
        .map_err(|error| match error {
            AdmissionSnapshotDecodeErrorV0::Truncated { .. } => {
                AdmissionSnapshotDecodeErrorV1::Truncated {
                    field: AdmissionSnapshotFieldV1::SourcePositionKind,
                }
            }
            AdmissionSnapshotDecodeErrorV0::ArithmeticOverflow { .. } => {
                AdmissionSnapshotDecodeErrorV1::ArithmeticOverflow {
                    field: AdmissionSnapshotFieldV1::SourcePositionKind,
                }
            }
            other => other.into(),
        })
}

fn validate_expected_config_v1(
    serialized: &AdmissionConfigV0,
    expected: &AdmissionConfigV0,
) -> Result<(), AdmissionSnapshotDecodeErrorV1> {
    if serialized.source_id() != expected.source_id() {
        return Err(
            AdmissionSnapshotDecodeErrorV1::ExpectedConfigurationMismatch {
                field: AdmissionSnapshotExpectedConfigFieldV1::SourceId,
            },
        );
    }
    if serialized.source_position_kind() != expected.source_position_kind() {
        return Err(
            AdmissionSnapshotDecodeErrorV1::ExpectedConfigurationMismatch {
                field: AdmissionSnapshotExpectedConfigFieldV1::SourcePositionKind,
            },
        );
    }
    if serialized.epoch_start_ns() != expected.epoch_start_ns()
        || serialized.epoch_end_ns() != expected.epoch_end_ns()
    {
        return Err(
            AdmissionSnapshotDecodeErrorV1::ExpectedConfigurationMismatch {
                field: AdmissionSnapshotExpectedConfigFieldV1::Epoch,
            },
        );
    }
    if serialized.limits() != expected.limits() {
        return Err(
            AdmissionSnapshotDecodeErrorV1::ExpectedConfigurationMismatch {
                field: AdmissionSnapshotExpectedConfigFieldV1::AdmissionLimits,
            },
        );
    }
    Ok(())
}

fn validate_state_for_encoding(
    state: &AdmissionStateV0,
) -> Result<(), AdmissionSnapshotEncodeErrorV0> {
    validate_state_for_encoding_with_kind(state, SourcePositionKindV0::Synthetic)
}

fn validate_state_for_encoding_with_kind(
    state: &AdmissionStateV0,
    required_kind: SourcePositionKindV0,
) -> Result<(), AdmissionSnapshotEncodeErrorV0> {
    if state.config.source_position_kind() != required_kind {
        return Err(AdmissionSnapshotEncodeErrorV0::StateInvariant {
            field: AdmissionSnapshotFieldV0::SourcePosition,
        });
    }
    if state.accepted_events.len() > state.config.limits().accepted_identity_limit() {
        return Err(AdmissionSnapshotEncodeErrorV0::StateInvariant {
            field: AdmissionSnapshotFieldV0::EventCount,
        });
    }
    let mut calculated_bytes = 0_usize;
    let mut calculated_maximum: Option<i64> = None;
    for (position, event) in &state.accepted_events {
        if position.kind() != required_kind {
            return Err(AdmissionSnapshotEncodeErrorV0::StateInvariant {
                field: AdmissionSnapshotFieldV0::SourcePosition,
            });
        }
        if event.source_id() != state.config.source_id()
            || event.source_position() != *position
            || event.ingest_time_ns() != 0
            || !(state.config.epoch_start_ns()..state.config.epoch_end_ns())
                .contains(&event.event_time_ns())
        {
            return Err(AdmissionSnapshotEncodeErrorV0::StateInvariant {
                field: AdmissionSnapshotFieldV0::EventRecord,
            });
        }
        let event_bytes = accepted_event_accounted_bytes_v0(event).map_err(|_| {
            AdmissionSnapshotEncodeErrorV0::StateInvariant {
                field: AdmissionSnapshotFieldV0::AcceptedEventBytes,
            }
        })?;
        calculated_bytes = calculated_bytes.checked_add(event_bytes).ok_or(
            AdmissionSnapshotEncodeErrorV0::ArithmeticOverflow {
                field: AdmissionSnapshotFieldV0::AcceptedEventBytes,
            },
        )?;
        calculated_maximum = Some(calculated_maximum.map_or(event.event_time_ns(), |current| {
            current.max(event.event_time_ns())
        }));
    }
    if calculated_bytes != state.accepted_event_bytes
        || calculated_bytes > state.config.limits().accepted_event_byte_limit()
    {
        return Err(AdmissionSnapshotEncodeErrorV0::StateInvariant {
            field: AdmissionSnapshotFieldV0::AcceptedEventBytes,
        });
    }
    if calculated_maximum != state.maximum_accepted_event_time_ns {
        return Err(AdmissionSnapshotEncodeErrorV0::StateInvariant {
            field: AdmissionSnapshotFieldV0::MaximumEventTime,
        });
    }
    let expected_watermark = if state.finalized {
        Some(state.config.epoch_end_ns())
    } else {
        calculated_maximum.map(|maximum| maximum.saturating_sub(ALLOWED_LATENESS_NS_V0))
    };
    if expected_watermark != state.watermark_ns {
        return Err(AdmissionSnapshotEncodeErrorV0::StateInvariant {
            field: AdmissionSnapshotFieldV0::Watermark,
        });
    }
    for reason in RejectionReasonV0::ALL {
        if !rejection_range_is_valid(state.rejection_summary.range(reason)) {
            return Err(AdmissionSnapshotEncodeErrorV0::StateInvariant {
                field: AdmissionSnapshotFieldV0::RejectionRange,
            });
        }
    }
    Ok(())
}

fn encode_rejection_range(
    writer: &mut SnapshotWriterV0,
    range: RejectionRangeV0,
) -> Result<(), AdmissionSnapshotEncodeErrorV0> {
    writer.write_u64(range.count())?;
    writer.write_bool(range.count_saturated())?;
    let has_range = range.minimum_attempted_time_ns().is_some();
    writer.write_bool(has_range)?;
    if has_range {
        writer.write_i64(range.minimum_attempted_time_ns().ok_or(
            AdmissionSnapshotEncodeErrorV0::StateInvariant {
                field: AdmissionSnapshotFieldV0::RejectionRange,
            },
        )?)?;
        writer.write_i64(range.maximum_attempted_time_ns().ok_or(
            AdmissionSnapshotEncodeErrorV0::StateInvariant {
                field: AdmissionSnapshotFieldV0::RejectionRange,
            },
        )?)?;
    }
    Ok(())
}

fn decode_rejection_summary(
    cursor: &mut SnapshotCursorV0<'_>,
) -> Result<RejectionSummaryV0, AdmissionSnapshotDecodeErrorV0> {
    let class_count = cursor.read_count(AdmissionSnapshotFieldV0::RejectionSummary)?;
    if class_count != REJECTION_CLASS_COUNT_V0 {
        return Err(AdmissionSnapshotDecodeErrorV0::InvalidValue {
            field: AdmissionSnapshotFieldV0::RejectionSummary,
        });
    }
    let mut ranges = [RejectionRangeV0::EMPTY; REJECTION_CLASS_COUNT_V0];
    for (expected_tag, range) in ranges.iter_mut().enumerate() {
        let actual_tag = cursor.read_u8(AdmissionSnapshotFieldV0::RejectionSummary)?;
        if usize::from(actual_tag) != expected_tag {
            return Err(AdmissionSnapshotDecodeErrorV0::NonCanonicalOrder {
                order: AdmissionSnapshotOrderV0::RejectionClasses,
            });
        }
        let count = cursor.read_u64(AdmissionSnapshotFieldV0::RejectionRange)?;
        let count_saturated = cursor.read_bool(AdmissionSnapshotFieldV0::RejectionRange)?;
        let has_range = cursor.read_bool(AdmissionSnapshotFieldV0::RejectionRange)?;
        let (minimum_attempted_time_ns, maximum_attempted_time_ns) = if has_range {
            (
                Some(cursor.read_i64(AdmissionSnapshotFieldV0::RejectionRange)?),
                Some(cursor.read_i64(AdmissionSnapshotFieldV0::RejectionRange)?),
            )
        } else {
            (None, None)
        };
        let decoded = RejectionRangeV0 {
            count,
            count_saturated,
            minimum_attempted_time_ns,
            maximum_attempted_time_ns,
        };
        if !rejection_range_is_valid(decoded) {
            return Err(AdmissionSnapshotDecodeErrorV0::InvalidValue {
                field: AdmissionSnapshotFieldV0::RejectionRange,
            });
        }
        *range = decoded;
    }
    Ok(RejectionSummaryV0 { ranges })
}

fn rejection_range_is_valid(range: RejectionRangeV0) -> bool {
    match (
        range.count(),
        range.count_saturated(),
        range.minimum_attempted_time_ns(),
        range.maximum_attempted_time_ns(),
    ) {
        (0, false, None, None) => true,
        (u64::MAX, _, Some(minimum), Some(maximum)) => minimum <= maximum,
        (1.., false, Some(minimum), Some(maximum)) => minimum <= maximum,
        _ => false,
    }
}

fn encode_event(
    writer: &mut SnapshotWriterV0,
    event: &EventEnvelopeV0,
) -> Result<(), AdmissionSnapshotEncodeErrorV0> {
    let length_offset = writer.len();
    writer.write_u32(0)?;
    let body_start = writer.len();
    writer.write_u16(event.envelope_version())?;
    let SourcePositionV0::Synthetic { sequence } = event.source_position() else {
        return Err(AdmissionSnapshotEncodeErrorV0::StateInvariant {
            field: AdmissionSnapshotFieldV0::SourcePosition,
        });
    };
    writer.write_u8(SOURCE_POSITION_SYNTHETIC_TAG_V0)?;
    writer.write_u64(sequence)?;
    encode_event_remainder(writer, event, length_offset, body_start)
}

fn encode_event_v1(
    writer: &mut SnapshotWriterV0,
    event: &EventEnvelopeV0,
) -> Result<(), AdmissionSnapshotEncodeErrorV1> {
    let length_offset = writer.len();
    writer.write_u32(0)?;
    let body_start = writer.len();
    writer.write_u16(event.envelope_version())?;
    match event.source_position() {
        SourcePositionV0::Synthetic { sequence } => {
            writer.write_u8(SOURCE_POSITION_SYNTHETIC_TAG_V0)?;
            writer.write_u64(sequence)?;
        }
        SourcePositionV0::OtlpFile { position } => {
            writer.write_u8(SOURCE_POSITION_OTLP_FILE_TAG_V1)?;
            writer.write_u64(position.line_number())?;
            writer.write_u32(position.resource_spans_index())?;
            writer.write_u32(position.scope_spans_index())?;
            writer.write_u32(position.span_index())?;
        }
        SourcePositionV0::Kafka { position } => {
            writer.write_u8(SOURCE_POSITION_KAFKA_TAG_V1)?;
            writer.write_i32(position.partition())?;
            writer.write_i64(position.offset())?;
        }
    }
    encode_event_remainder(writer, event, length_offset, body_start).map_err(Into::into)
}

fn encode_event_remainder(
    writer: &mut SnapshotWriterV0,
    event: &EventEnvelopeV0,
    length_offset: usize,
    body_start: usize,
) -> Result<(), AdmissionSnapshotEncodeErrorV0> {
    writer.write_length_prefixed(event.schema_id().as_str().as_bytes())?;
    writer.write_bool(event.key().is_some())?;
    if let Some(key) = event.key() {
        writer.write_length_prefixed(key)?;
    }
    writer.write_i64(event.event_time_ns())?;
    writer.write_u64(event.sensitivity().bits())?;
    writer.write_u32(usize_to_u32(
        event.payload().fields().len(),
        AdmissionSnapshotFieldV0::Payload,
    )?)?;
    for (name, value) in event.payload().fields() {
        writer.write_length_prefixed(name.as_bytes())?;
        encode_scalar(writer, value)?;
    }
    let body_length = writer.len().checked_sub(body_start).ok_or(
        AdmissionSnapshotEncodeErrorV0::ArithmeticOverflow {
            field: AdmissionSnapshotFieldV0::EventRecord,
        },
    )?;
    writer.overwrite_u32(
        length_offset,
        usize_to_u32(body_length, AdmissionSnapshotFieldV0::EventRecord)?,
    );
    Ok(())
}

fn encode_scalar(
    writer: &mut SnapshotWriterV0,
    value: &ScalarValueV0,
) -> Result<(), AdmissionSnapshotEncodeErrorV0> {
    match value.kind() {
        ScalarKindV0::Null => writer.write_u8(SCALAR_NULL_TAG_V0),
        ScalarKindV0::Bool => {
            writer.write_u8(SCALAR_BOOL_TAG_V0)?;
            writer.write_bool(value.as_bool().ok_or(
                AdmissionSnapshotEncodeErrorV0::StateInvariant {
                    field: AdmissionSnapshotFieldV0::Scalar,
                },
            )?)
        }
        ScalarKindV0::I64 => {
            writer.write_u8(SCALAR_I64_TAG_V0)?;
            writer.write_i64(value.as_i64().ok_or(
                AdmissionSnapshotEncodeErrorV0::StateInvariant {
                    field: AdmissionSnapshotFieldV0::Scalar,
                },
            )?)
        }
        ScalarKindV0::U64 => {
            writer.write_u8(SCALAR_U64_TAG_V0)?;
            writer.write_u64(value.as_u64().ok_or(
                AdmissionSnapshotEncodeErrorV0::StateInvariant {
                    field: AdmissionSnapshotFieldV0::Scalar,
                },
            )?)
        }
        ScalarKindV0::Utf8 => {
            writer.write_u8(SCALAR_UTF8_TAG_V0)?;
            writer.write_length_prefixed(
                value
                    .as_utf8()
                    .ok_or(AdmissionSnapshotEncodeErrorV0::StateInvariant {
                        field: AdmissionSnapshotFieldV0::Scalar,
                    })?
                    .as_bytes(),
            )
        }
        ScalarKindV0::Bytes => {
            writer.write_u8(SCALAR_BYTES_TAG_V0)?;
            writer.write_length_prefixed(value.as_bytes().ok_or(
                AdmissionSnapshotEncodeErrorV0::StateInvariant {
                    field: AdmissionSnapshotFieldV0::Scalar,
                },
            )?)
        }
    }
}

fn decode_event(
    input: &[u8],
    config: &AdmissionConfigV0,
) -> Result<EventEnvelopeV0, AdmissionSnapshotDecodeErrorV0> {
    let mut cursor = SnapshotCursorV0::new(input);
    let envelope_version = cursor.read_u16(AdmissionSnapshotFieldV0::EnvelopeVersion)?;
    if envelope_version != ENVELOPE_VERSION_V0 {
        return Err(AdmissionSnapshotDecodeErrorV0::InvalidValue {
            field: AdmissionSnapshotFieldV0::EnvelopeVersion,
        });
    }
    if cursor.read_u8(AdmissionSnapshotFieldV0::SourcePosition)? != SOURCE_POSITION_SYNTHETIC_TAG_V0
    {
        return Err(AdmissionSnapshotDecodeErrorV0::InvalidTag {
            field: AdmissionSnapshotFieldV0::SourcePosition,
        });
    }
    let sequence = cursor.read_u64(AdmissionSnapshotFieldV0::SourcePosition)?;
    decode_event_remainder(
        cursor,
        config,
        envelope_version,
        SourcePositionV0::Synthetic { sequence },
    )
}

fn decode_event_v1(
    input: &[u8],
    config: &AdmissionConfigV0,
) -> Result<EventEnvelopeV0, AdmissionSnapshotDecodeErrorV0> {
    let mut cursor = SnapshotCursorV0::new(input);
    let envelope_version = cursor.read_u16(AdmissionSnapshotFieldV0::EnvelopeVersion)?;
    if envelope_version != ENVELOPE_VERSION_V0 {
        return Err(AdmissionSnapshotDecodeErrorV0::InvalidValue {
            field: AdmissionSnapshotFieldV0::EnvelopeVersion,
        });
    }
    let tag = cursor.read_u8(AdmissionSnapshotFieldV0::SourcePosition)?;
    let source_position = match tag {
        SOURCE_POSITION_SYNTHETIC_TAG_V0 => SourcePositionV0::Synthetic {
            sequence: cursor.read_u64(AdmissionSnapshotFieldV0::SourcePosition)?,
        },
        SOURCE_POSITION_OTLP_FILE_TAG_V1 => SourcePositionV0::otlp_file(
            cursor.read_u64(AdmissionSnapshotFieldV0::SourcePosition)?,
            cursor.read_u32(AdmissionSnapshotFieldV0::SourcePosition)?,
            cursor.read_u32(AdmissionSnapshotFieldV0::SourcePosition)?,
            cursor.read_u32(AdmissionSnapshotFieldV0::SourcePosition)?,
        )
        .map_err(AdmissionSnapshotDecodeErrorV0::InvalidEvent)?,
        SOURCE_POSITION_KAFKA_TAG_V1 => SourcePositionV0::kafka(
            cursor.read_i32(AdmissionSnapshotFieldV0::SourcePosition)?,
            cursor.read_i64(AdmissionSnapshotFieldV0::SourcePosition)?,
        )
        .map_err(AdmissionSnapshotDecodeErrorV0::InvalidEvent)?,
        _ => {
            return Err(AdmissionSnapshotDecodeErrorV0::InvalidTag {
                field: AdmissionSnapshotFieldV0::SourcePosition,
            });
        }
    };
    if source_position.kind() != config.source_position_kind() {
        return Err(AdmissionSnapshotDecodeErrorV0::InvalidValue {
            field: AdmissionSnapshotFieldV0::SourcePosition,
        });
    }
    decode_event_remainder(cursor, config, envelope_version, source_position)
}

fn decode_event_remainder(
    mut cursor: SnapshotCursorV0<'_>,
    config: &AdmissionConfigV0,
    envelope_version: u16,
    source_position: SourcePositionV0,
) -> Result<EventEnvelopeV0, AdmissionSnapshotDecodeErrorV0> {
    let schema_id = SchemaIdV0::new(
        cursor.read_bounded_str(AdmissionSnapshotFieldV0::SchemaId, MAX_IDENTIFIER_BYTES)?,
    )
    .map_err(AdmissionSnapshotDecodeErrorV0::InvalidEvent)?;
    let key = if cursor.read_bool(AdmissionSnapshotFieldV0::Key)? {
        Some(
            cursor
                .read_bounded_bytes(AdmissionSnapshotFieldV0::Key, MAX_KEY_BYTES)?
                .to_vec(),
        )
    } else {
        None
    };
    let event_time_ns = cursor.read_i64(AdmissionSnapshotFieldV0::EventTime)?;
    let sensitivity =
        SensitivityBitmapV0::from_bits(cursor.read_u64(AdmissionSnapshotFieldV0::Sensitivity)?);
    let field_count = cursor.read_count(AdmissionSnapshotFieldV0::Payload)?;
    if field_count > MAX_PAYLOAD_FIELDS {
        return Err(AdmissionSnapshotDecodeErrorV0::CapacityExceeded {
            field: AdmissionSnapshotFieldV0::Payload,
            maximum: MAX_PAYLOAD_FIELDS,
            attempted: field_count,
        });
    }
    let mut fields = BTreeMap::new();
    let mut previous_name: Option<String> = None;
    for _ in 0..field_count {
        let name = cursor
            .read_bounded_str(AdmissionSnapshotFieldV0::FieldName, MAX_FIELD_NAME_BYTES)?
            .to_owned();
        if previous_name
            .as_deref()
            .is_some_and(|previous| name.as_str() <= previous)
        {
            return Err(AdmissionSnapshotDecodeErrorV0::NonCanonicalOrder {
                order: AdmissionSnapshotOrderV0::PayloadFields,
            });
        }
        let value = decode_scalar(&mut cursor)?;
        previous_name = Some(name.clone());
        fields.insert(name, value);
    }
    if !cursor.is_finished() {
        return Err(AdmissionSnapshotDecodeErrorV0::RecordLengthMismatch);
    }
    let payload = CanonicalPayloadV0::try_from_fields(fields)
        .map_err(AdmissionSnapshotDecodeErrorV0::InvalidEvent)?;
    EventEnvelopeV0::new(
        envelope_version,
        config.source_id().clone(),
        schema_id,
        source_position,
        key.as_deref(),
        event_time_ns,
        0,
        sensitivity,
        payload,
    )
    .map_err(AdmissionSnapshotDecodeErrorV0::InvalidEvent)
}

fn decode_scalar(
    cursor: &mut SnapshotCursorV0<'_>,
) -> Result<ScalarValueV0, AdmissionSnapshotDecodeErrorV0> {
    match cursor.read_u8(AdmissionSnapshotFieldV0::Scalar)? {
        SCALAR_NULL_TAG_V0 => Ok(ScalarValueV0::null()),
        SCALAR_BOOL_TAG_V0 => Ok(ScalarValueV0::from_bool(
            cursor.read_bool(AdmissionSnapshotFieldV0::Scalar)?,
        )),
        SCALAR_I64_TAG_V0 => Ok(ScalarValueV0::from_i64(
            cursor.read_i64(AdmissionSnapshotFieldV0::Scalar)?,
        )),
        SCALAR_U64_TAG_V0 => Ok(ScalarValueV0::from_u64(
            cursor.read_u64(AdmissionSnapshotFieldV0::Scalar)?,
        )),
        SCALAR_UTF8_TAG_V0 => ScalarValueV0::utf8(
            cursor.read_bounded_str(AdmissionSnapshotFieldV0::Scalar, MAX_SCALAR_BYTES)?,
        )
        .map_err(AdmissionSnapshotDecodeErrorV0::InvalidEvent),
        SCALAR_BYTES_TAG_V0 => ScalarValueV0::bytes(
            cursor.read_bounded_bytes(AdmissionSnapshotFieldV0::Scalar, MAX_SCALAR_BYTES)?,
        )
        .map_err(AdmissionSnapshotDecodeErrorV0::InvalidEvent),
        _ => Err(AdmissionSnapshotDecodeErrorV0::InvalidTag {
            field: AdmissionSnapshotFieldV0::Scalar,
        }),
    }
}

fn usize_to_u32(
    value: usize,
    field: AdmissionSnapshotFieldV0,
) -> Result<u32, AdmissionSnapshotEncodeErrorV0> {
    u32::try_from(value).map_err(|_| AdmissionSnapshotEncodeErrorV0::ArithmeticOverflow { field })
}

fn usize_to_u64(
    value: usize,
    field: AdmissionSnapshotFieldV0,
) -> Result<u64, AdmissionSnapshotEncodeErrorV0> {
    u64::try_from(value).map_err(|_| AdmissionSnapshotEncodeErrorV0::ArithmeticOverflow { field })
}

struct SnapshotWriterV0 {
    bytes: Vec<u8>,
}

impl SnapshotWriterV0 {
    fn with_capacity(capacity: usize) -> Self {
        Self {
            bytes: Vec::with_capacity(capacity),
        }
    }

    fn len(&self) -> usize {
        self.bytes.len()
    }

    fn finish(self) -> Vec<u8> {
        self.bytes
    }

    fn write_bytes(&mut self, value: &[u8]) -> Result<(), AdmissionSnapshotEncodeErrorV0> {
        let attempted = self.bytes.len().checked_add(value.len()).ok_or(
            AdmissionSnapshotEncodeErrorV0::ArithmeticOverflow {
                field: AdmissionSnapshotFieldV0::EndOfDocument,
            },
        )?;
        if attempted > MAX_ADMISSION_SNAPSHOT_BYTES_V0 {
            return Err(AdmissionSnapshotEncodeErrorV0::SnapshotTooLarge {
                maximum: MAX_ADMISSION_SNAPSHOT_BYTES_V0,
                attempted,
            });
        }
        self.bytes.extend_from_slice(value);
        Ok(())
    }

    fn write_u8(&mut self, value: u8) -> Result<(), AdmissionSnapshotEncodeErrorV0> {
        self.write_bytes(&[value])
    }

    fn write_u16(&mut self, value: u16) -> Result<(), AdmissionSnapshotEncodeErrorV0> {
        self.write_bytes(&value.to_be_bytes())
    }

    fn write_u32(&mut self, value: u32) -> Result<(), AdmissionSnapshotEncodeErrorV0> {
        self.write_bytes(&value.to_be_bytes())
    }

    fn write_u64(&mut self, value: u64) -> Result<(), AdmissionSnapshotEncodeErrorV0> {
        self.write_bytes(&value.to_be_bytes())
    }

    fn write_i32(&mut self, value: i32) -> Result<(), AdmissionSnapshotEncodeErrorV0> {
        self.write_bytes(&value.to_be_bytes())
    }

    fn write_i64(&mut self, value: i64) -> Result<(), AdmissionSnapshotEncodeErrorV0> {
        self.write_bytes(&value.to_be_bytes())
    }

    fn write_bool(&mut self, value: bool) -> Result<(), AdmissionSnapshotEncodeErrorV0> {
        self.write_u8(u8::from(value))
    }

    fn write_length_prefixed(
        &mut self,
        value: &[u8],
    ) -> Result<(), AdmissionSnapshotEncodeErrorV0> {
        self.write_u32(usize_to_u32(
            value.len(),
            AdmissionSnapshotFieldV0::EndOfDocument,
        )?)?;
        self.write_bytes(value)
    }

    fn overwrite_u32(&mut self, offset: usize, value: u32) {
        self.bytes[offset..offset + 4].copy_from_slice(&value.to_be_bytes());
    }
}

struct SnapshotCursorV0<'a> {
    input: &'a [u8],
    offset: usize,
}

impl<'a> SnapshotCursorV0<'a> {
    const fn new(input: &'a [u8]) -> Self {
        Self { input, offset: 0 }
    }

    fn is_finished(&self) -> bool {
        self.offset == self.input.len()
    }

    fn read_exact(
        &mut self,
        length: usize,
        field: AdmissionSnapshotFieldV0,
    ) -> Result<&'a [u8], AdmissionSnapshotDecodeErrorV0> {
        let end = self
            .offset
            .checked_add(length)
            .ok_or(AdmissionSnapshotDecodeErrorV0::ArithmeticOverflow { field })?;
        let value = self
            .input
            .get(self.offset..end)
            .ok_or(AdmissionSnapshotDecodeErrorV0::Truncated { field })?;
        self.offset = end;
        Ok(value)
    }

    fn read_u8(
        &mut self,
        field: AdmissionSnapshotFieldV0,
    ) -> Result<u8, AdmissionSnapshotDecodeErrorV0> {
        Ok(self.read_exact(1, field)?[0])
    }

    fn read_u16(
        &mut self,
        field: AdmissionSnapshotFieldV0,
    ) -> Result<u16, AdmissionSnapshotDecodeErrorV0> {
        let bytes = self.read_exact(2, field)?;
        Ok(u16::from_be_bytes([bytes[0], bytes[1]]))
    }

    fn read_u32(
        &mut self,
        field: AdmissionSnapshotFieldV0,
    ) -> Result<u32, AdmissionSnapshotDecodeErrorV0> {
        let bytes = self.read_exact(4, field)?;
        Ok(u32::from_be_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]))
    }

    fn read_u64(
        &mut self,
        field: AdmissionSnapshotFieldV0,
    ) -> Result<u64, AdmissionSnapshotDecodeErrorV0> {
        let bytes = self.read_exact(8, field)?;
        Ok(u64::from_be_bytes([
            bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5], bytes[6], bytes[7],
        ]))
    }

    fn read_i32(
        &mut self,
        field: AdmissionSnapshotFieldV0,
    ) -> Result<i32, AdmissionSnapshotDecodeErrorV0> {
        let bytes = self.read_exact(4, field)?;
        Ok(i32::from_be_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]))
    }

    fn read_i64(
        &mut self,
        field: AdmissionSnapshotFieldV0,
    ) -> Result<i64, AdmissionSnapshotDecodeErrorV0> {
        let bytes = self.read_exact(8, field)?;
        Ok(i64::from_be_bytes([
            bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5], bytes[6], bytes[7],
        ]))
    }

    fn read_bool(
        &mut self,
        field: AdmissionSnapshotFieldV0,
    ) -> Result<bool, AdmissionSnapshotDecodeErrorV0> {
        match self.read_u8(field)? {
            0 => Ok(false),
            1 => Ok(true),
            _ => Err(AdmissionSnapshotDecodeErrorV0::InvalidBoolean { field }),
        }
    }

    fn read_count(
        &mut self,
        field: AdmissionSnapshotFieldV0,
    ) -> Result<usize, AdmissionSnapshotDecodeErrorV0> {
        usize::try_from(self.read_u32(field)?)
            .map_err(|_| AdmissionSnapshotDecodeErrorV0::ArithmeticOverflow { field })
    }

    fn read_usize(
        &mut self,
        field: AdmissionSnapshotFieldV0,
    ) -> Result<usize, AdmissionSnapshotDecodeErrorV0> {
        usize::try_from(self.read_u64(field)?)
            .map_err(|_| AdmissionSnapshotDecodeErrorV0::ArithmeticOverflow { field })
    }

    fn read_bounded_bytes(
        &mut self,
        field: AdmissionSnapshotFieldV0,
        maximum: usize,
    ) -> Result<&'a [u8], AdmissionSnapshotDecodeErrorV0> {
        let length = self.read_count(field)?;
        if length > maximum {
            return Err(AdmissionSnapshotDecodeErrorV0::CapacityExceeded {
                field,
                maximum,
                attempted: length,
            });
        }
        self.read_exact(length, field)
    }

    fn read_bounded_str(
        &mut self,
        field: AdmissionSnapshotFieldV0,
        maximum: usize,
    ) -> Result<&'a str, AdmissionSnapshotDecodeErrorV0> {
        str::from_utf8(self.read_bounded_bytes(field, maximum)?)
            .map_err(|_| AdmissionSnapshotDecodeErrorV0::InvalidUtf8 { field })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn state() -> AdmissionStateV0 {
        AdmissionStateV0::new(
            AdmissionConfigV0::new(
                ADMISSION_VERSION_V0,
                SourceIdV0::new("source-v0").unwrap(),
                0,
                100,
                AdmissionLimitsV0::maximum(),
            )
            .unwrap(),
        )
    }

    #[test]
    fn encoder_rejects_inconsistent_derived_ledgers() {
        let mut wrong_bytes = state();
        wrong_bytes.accepted_event_bytes = 1;
        assert_eq!(
            encode_admission_snapshot_v0(&wrong_bytes),
            Err(AdmissionSnapshotEncodeErrorV0::StateInvariant {
                field: AdmissionSnapshotFieldV0::AcceptedEventBytes,
            })
        );

        let mut wrong_maximum = state();
        wrong_maximum.maximum_accepted_event_time_ns = Some(0);
        assert_eq!(
            encode_admission_snapshot_v0(&wrong_maximum),
            Err(AdmissionSnapshotEncodeErrorV0::StateInvariant {
                field: AdmissionSnapshotFieldV0::MaximumEventTime,
            })
        );

        let mut wrong_watermark = state();
        wrong_watermark.watermark_ns = Some(0);
        assert_eq!(
            encode_admission_snapshot_v0(&wrong_watermark),
            Err(AdmissionSnapshotEncodeErrorV0::StateInvariant {
                field: AdmissionSnapshotFieldV0::Watermark,
            })
        );
    }

    #[test]
    fn snapshot_v0_rejects_a_non_synthetic_internal_position() {
        let mut external = state();
        let event = EventEnvelopeV0::new(
            ENVELOPE_VERSION_V0,
            SourceIdV0::new("source-v0").unwrap(),
            SchemaIdV0::new("schema-v0").unwrap(),
            SourcePositionV0::otlp_file(1, 0, 0, 0).unwrap(),
            None,
            50,
            0,
            SensitivityBitmapV0::from_bits(0),
            CanonicalPayloadV0::try_from_fields(BTreeMap::new()).unwrap(),
        )
        .unwrap();
        external
            .accepted_events
            .insert(event.source_position(), event);

        assert_eq!(
            encode_admission_snapshot_v0(&external),
            Err(AdmissionSnapshotEncodeErrorV0::StateInvariant {
                field: AdmissionSnapshotFieldV0::SourcePosition,
            })
        );
    }

    #[test]
    fn snapshot_v1_rejects_mixed_internal_kinds_and_inconsistent_ledgers() {
        let mut mixed = AdmissionStateV0::new(
            AdmissionConfigV0::new_with_source_position_kind(
                ADMISSION_VERSION_V0,
                SourceIdV0::new("source-v0").unwrap(),
                SourcePositionKindV0::OtlpFile,
                0,
                100,
                AdmissionLimitsV0::maximum(),
            )
            .unwrap(),
        );
        let event = EventEnvelopeV0::new(
            ENVELOPE_VERSION_V0,
            SourceIdV0::new("source-v0").unwrap(),
            SchemaIdV0::new("schema-v0").unwrap(),
            SourcePositionV0::kafka(1, 1).unwrap(),
            None,
            50,
            0,
            SensitivityBitmapV0::from_bits(0),
            CanonicalPayloadV0::try_from_fields(BTreeMap::new()).unwrap(),
        )
        .unwrap();
        mixed.accepted_events.insert(event.source_position(), event);
        assert_eq!(
            encode_admission_snapshot_v1(&mixed),
            Err(AdmissionSnapshotEncodeErrorV1::StateInvariant {
                field: AdmissionSnapshotFieldV1::SourcePosition,
            })
        );

        let mut wrong_bytes = state();
        wrong_bytes.accepted_event_bytes = 1;
        assert_eq!(
            encode_admission_snapshot_v1(&wrong_bytes),
            Err(AdmissionSnapshotEncodeErrorV1::StateInvariant {
                field: AdmissionSnapshotFieldV1::AcceptedEventBytes,
            })
        );

        let mut invalid_rejection = state();
        invalid_rejection.rejection_summary.ranges[0] = RejectionRangeV0 {
            count: 0,
            count_saturated: true,
            minimum_attempted_time_ns: None,
            maximum_attempted_time_ns: None,
        };
        assert_eq!(
            encode_admission_snapshot_v1(&invalid_rejection),
            Err(AdmissionSnapshotEncodeErrorV1::StateInvariant {
                field: AdmissionSnapshotFieldV1::RejectionRange,
            })
        );
    }

    #[test]
    fn saturated_rejection_ranges_round_trip_and_invalid_ranges_do_not_encode() {
        let mut saturated = state();
        saturated.rejection_summary.ranges[0] = RejectionRangeV0 {
            count: u64::MAX,
            count_saturated: true,
            minimum_attempted_time_ns: Some(i64::MIN),
            maximum_attempted_time_ns: Some(i64::MAX),
        };
        let restored =
            restore_admission_snapshot_v0(&encode_admission_snapshot_v0(&saturated).unwrap())
                .unwrap();
        assert_eq!(restored, saturated);

        let mut invalid = state();
        invalid.rejection_summary.ranges[0] = RejectionRangeV0 {
            count: 0,
            count_saturated: true,
            minimum_attempted_time_ns: None,
            maximum_attempted_time_ns: None,
        };
        assert_eq!(
            encode_admission_snapshot_v0(&invalid),
            Err(AdmissionSnapshotEncodeErrorV0::StateInvariant {
                field: AdmissionSnapshotFieldV0::RejectionRange,
            })
        );
    }
}
