//! Bounded canonical event types for Decant's first in-memory slice.
//!
//! Invariant-bearing fields are private and have no mutable access path.
//!
//! ```compile_fail
//! use decant_ir::SourceIdV0;
//! let _ = SourceIdV0(String::from("synthetic-source"));
//! ```
//!
//! ```compile_fail
//! use std::collections::BTreeMap;
//! use decant_ir::{
//!     CanonicalPayloadV0, EventEnvelopeV0, ScalarValueV0, SchemaIdV0,
//!     SensitivityBitmapV0, SourceIdV0, SourcePositionV0,
//! };
//! let event = EventEnvelopeV0::new(
//!     0,
//!     SourceIdV0::new("source").unwrap(),
//!     SchemaIdV0::new("schema").unwrap(),
//!     SourcePositionV0::Synthetic { sequence: 1 },
//!     None,
//!     0,
//!     0,
//!     SensitivityBitmapV0::from_bits(0),
//!     CanonicalPayloadV0::try_from_fields(
//!         BTreeMap::<String, ScalarValueV0>::new(),
//!     ).unwrap(),
//! ).unwrap();
//! let _ = EventEnvelopeV0 { ..event };
//! ```
//!
//! ```compile_fail
//! use std::collections::BTreeMap;
//! use decant_ir::{CanonicalPayloadV0, ScalarValueV0};
//! let mut payload = CanonicalPayloadV0::try_from_fields(
//!     BTreeMap::<String, ScalarValueV0>::new(),
//! ).unwrap();
//! payload.fields.clear();
//! ```
//!
//! ```compile_fail
//! use std::collections::BTreeMap;
//! use decant_ir::{
//!     CanonicalPayloadV0, EventEnvelopeV0, ScalarValueV0, SchemaIdV0,
//!     SensitivityBitmapV0, SourceIdV0, SourcePositionV0,
//! };
//! fn event(source: &str, sequence: u64) -> EventEnvelopeV0 {
//!     EventEnvelopeV0::new(
//!         0,
//!         SourceIdV0::new(source).unwrap(),
//!         SchemaIdV0::new("schema").unwrap(),
//!         SourcePositionV0::Synthetic { sequence },
//!         None,
//!         0,
//!         0,
//!         SensitivityBitmapV0::from_bits(0),
//!         CanonicalPayloadV0::try_from_fields(
//!             BTreeMap::<String, ScalarValueV0>::new(),
//!         ).unwrap(),
//!     ).unwrap()
//! }
//! let mut target = event("source-a", 1);
//! let injected = event("source-b", 2).provenance().clone();
//! target.provenance = injected;
//! ```
//!
//! ```compile_fail
//! use decant_ir::OtlpFilePositionV0;
//! let _ = OtlpFilePositionV0 {
//!     line_number: 1,
//!     resource_spans_index: 0,
//!     scope_spans_index: 0,
//!     span_index: 0,
//! };
//! ```
//!
//! ```compile_fail
//! use decant_ir::KafkaPositionV0;
//! let _ = KafkaPositionV0 {
//!     partition: 0,
//!     offset: 0,
//! };
//! ```

#![forbid(unsafe_code)]

use std::cmp::Ordering;
use std::collections::BTreeMap;
use std::error::Error;
use std::fmt;

pub const ENVELOPE_VERSION_V0: u16 = 0;
pub const MAX_IDENTIFIER_BYTES: usize = 64;
pub const MAX_KEY_BYTES: usize = 256;
pub const MAX_PAYLOAD_FIELDS: usize = 64;
pub const MAX_FIELD_NAME_BYTES: usize = 64;
pub const MAX_SCALAR_BYTES: usize = 4_096;
pub const MAX_LOGICAL_PAYLOAD_BYTES: usize = 16_384;
pub const MAX_OTLP_FILE_LINE_NUMBER_V0: u64 = 1 << 38;
pub const MAX_OTLP_FILE_RESOURCE_SPANS_INDEX_V0: u32 = 127;
pub const MAX_OTLP_FILE_SCOPE_SPANS_INDEX_V0: u32 = 127;
pub const MAX_OTLP_FILE_SPAN_INDEX_V0: u32 = 4_095;
pub const MAX_KAFKA_PARTITION_V0: i32 = 65_535;
pub const MAX_KAFKA_OFFSET_V0: i64 = (1_i64 << 48) - 1;

const MIN_IDENTIFIER_BYTES: usize = 1;
const MIN_FIELD_NAME_BYTES: usize = 1;
const FIELD_COUNT_WIDTH: usize = 4;
const FIELD_NAME_LENGTH_WIDTH: usize = 4;
const SCALAR_TAG_WIDTH: usize = 1;
const VARIABLE_SCALAR_LENGTH_WIDTH: usize = 4;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum IdentifierKindV0 {
    Source,
    Schema,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ScalarKindV0 {
    Null,
    Bool,
    I64,
    U64,
    Utf8,
    Bytes,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum LengthComponentV0 {
    FieldCount,
    FieldNameLength,
    FieldName,
    ScalarTag,
    ScalarContent,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum OtlpFilePositionFieldV0 {
    LineNumber,
    ResourceSpansIndex,
    ScopeSpansIndex,
    SpanIndex,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum OtlpFilePositionProblemV0 {
    Zero,
    AboveMaximum,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum KafkaPositionFieldV0 {
    Partition,
    Offset,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum KafkaPositionProblemV0 {
    Negative,
    AboveMaximum,
}

#[derive(Clone, Eq, PartialEq)]
pub enum ValidationErrorV0 {
    UnsupportedEnvelopeVersion {
        actual: u16,
    },
    InvalidIdentifierLength {
        kind: IdentifierKindV0,
        minimum: usize,
        maximum: usize,
        actual: usize,
    },
    InvalidIdentifierSyntax {
        kind: IdentifierKindV0,
    },
    KeyTooLong {
        maximum: usize,
        actual: usize,
    },
    TooManyFields {
        maximum: usize,
        actual: usize,
    },
    InvalidFieldNameLength {
        minimum: usize,
        maximum: usize,
        actual: usize,
    },
    InvalidFieldNameSyntax,
    ScalarTooLong {
        kind: ScalarKindV0,
        maximum: usize,
        actual: usize,
    },
    PayloadTooLarge {
        maximum: usize,
        actual: usize,
    },
    LengthOverflow {
        component: LengthComponentV0,
    },
    InvalidOtlpFilePosition {
        field: OtlpFilePositionFieldV0,
        problem: OtlpFilePositionProblemV0,
    },
    InvalidKafkaPosition {
        field: KafkaPositionFieldV0,
        problem: KafkaPositionProblemV0,
    },
}

impl fmt::Debug for ValidationErrorV0 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::UnsupportedEnvelopeVersion { actual } => formatter
                .debug_struct("UnsupportedEnvelopeVersion")
                .field("actual", actual)
                .finish(),
            Self::InvalidIdentifierLength {
                kind,
                minimum,
                maximum,
                actual,
            } => formatter
                .debug_struct("InvalidIdentifierLength")
                .field("kind", kind)
                .field("minimum", minimum)
                .field("maximum", maximum)
                .field("actual", actual)
                .finish(),
            Self::InvalidIdentifierSyntax { kind } => formatter
                .debug_struct("InvalidIdentifierSyntax")
                .field("kind", kind)
                .finish(),
            Self::KeyTooLong { maximum, actual } => formatter
                .debug_struct("KeyTooLong")
                .field("maximum", maximum)
                .field("actual", actual)
                .finish(),
            Self::TooManyFields { maximum, actual } => formatter
                .debug_struct("TooManyFields")
                .field("maximum", maximum)
                .field("actual", actual)
                .finish(),
            Self::InvalidFieldNameLength {
                minimum,
                maximum,
                actual,
            } => formatter
                .debug_struct("InvalidFieldNameLength")
                .field("minimum", minimum)
                .field("maximum", maximum)
                .field("actual", actual)
                .finish(),
            Self::InvalidFieldNameSyntax => formatter.write_str("InvalidFieldNameSyntax"),
            Self::ScalarTooLong {
                kind,
                maximum,
                actual,
            } => formatter
                .debug_struct("ScalarTooLong")
                .field("kind", kind)
                .field("maximum", maximum)
                .field("actual", actual)
                .finish(),
            Self::PayloadTooLarge { maximum, actual } => formatter
                .debug_struct("PayloadTooLarge")
                .field("maximum", maximum)
                .field("actual", actual)
                .finish(),
            Self::LengthOverflow { component } => formatter
                .debug_struct("LengthOverflow")
                .field("component", component)
                .finish(),
            Self::InvalidOtlpFilePosition { field, problem } => formatter
                .debug_struct("InvalidOtlpFilePosition")
                .field("field", field)
                .field("problem", problem)
                .finish(),
            Self::InvalidKafkaPosition { field, problem } => formatter
                .debug_struct("InvalidKafkaPosition")
                .field("field", field)
                .field("problem", problem)
                .finish(),
        }
    }
}

impl fmt::Display for ValidationErrorV0 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(self, formatter)
    }
}

impl Error for ValidationErrorV0 {}

#[derive(Clone, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct SourceIdV0(String);

impl SourceIdV0 {
    pub fn new(value: &str) -> Result<Self, ValidationErrorV0> {
        validate_identifier(value.as_bytes(), IdentifierKindV0::Source)?;
        Ok(Self(value.to_owned()))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Debug for SourceIdV0 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("SourceIdV0")
            .field("byte_len", &self.0.len())
            .finish()
    }
}

#[derive(Clone, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct SchemaIdV0(String);

impl SchemaIdV0 {
    pub fn new(value: &str) -> Result<Self, ValidationErrorV0> {
        validate_identifier(value.as_bytes(), IdentifierKindV0::Schema)?;
        Ok(Self(value.to_owned()))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Debug for SchemaIdV0 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("SchemaIdV0")
            .field("byte_len", &self.0.len())
            .finish()
    }
}

fn validate_identifier(value: &[u8], kind: IdentifierKindV0) -> Result<(), ValidationErrorV0> {
    if !(MIN_IDENTIFIER_BYTES..=MAX_IDENTIFIER_BYTES).contains(&value.len()) {
        return Err(ValidationErrorV0::InvalidIdentifierLength {
            kind,
            minimum: MIN_IDENTIFIER_BYTES,
            maximum: MAX_IDENTIFIER_BYTES,
            actual: value.len(),
        });
    }

    let valid_first = value[0].is_ascii_lowercase() || value[0].is_ascii_digit();
    let valid_rest = value[1..].iter().all(|byte| {
        byte.is_ascii_lowercase() || byte.is_ascii_digit() || matches!(byte, b'.' | b'_' | b'-')
    });

    if !valid_first || !valid_rest {
        return Err(ValidationErrorV0::InvalidIdentifierSyntax { kind });
    }

    Ok(())
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum SourcePositionKindV0 {
    Synthetic,
    OtlpFile,
    Kafka,
}

#[derive(Clone, Copy, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct OtlpFilePositionV0 {
    line_number: u64,
    resource_spans_index: u32,
    scope_spans_index: u32,
    span_index: u32,
}

impl OtlpFilePositionV0 {
    pub fn new(
        line_number: u64,
        resource_spans_index: u32,
        scope_spans_index: u32,
        span_index: u32,
    ) -> Result<Self, ValidationErrorV0> {
        if line_number == 0 {
            return Err(ValidationErrorV0::InvalidOtlpFilePosition {
                field: OtlpFilePositionFieldV0::LineNumber,
                problem: OtlpFilePositionProblemV0::Zero,
            });
        }
        if line_number > MAX_OTLP_FILE_LINE_NUMBER_V0 {
            return Err(ValidationErrorV0::InvalidOtlpFilePosition {
                field: OtlpFilePositionFieldV0::LineNumber,
                problem: OtlpFilePositionProblemV0::AboveMaximum,
            });
        }
        validate_otlp_file_index(
            resource_spans_index,
            MAX_OTLP_FILE_RESOURCE_SPANS_INDEX_V0,
            OtlpFilePositionFieldV0::ResourceSpansIndex,
        )?;
        validate_otlp_file_index(
            scope_spans_index,
            MAX_OTLP_FILE_SCOPE_SPANS_INDEX_V0,
            OtlpFilePositionFieldV0::ScopeSpansIndex,
        )?;
        validate_otlp_file_index(
            span_index,
            MAX_OTLP_FILE_SPAN_INDEX_V0,
            OtlpFilePositionFieldV0::SpanIndex,
        )?;
        Ok(Self {
            line_number,
            resource_spans_index,
            scope_spans_index,
            span_index,
        })
    }

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

    pub const fn source_ordering_ordinal(self) -> u64 {
        ((self.line_number - 1) << 26)
            | ((self.resource_spans_index as u64) << 19)
            | ((self.scope_spans_index as u64) << 12)
            | self.span_index as u64
    }
}

impl fmt::Debug for OtlpFilePositionV0 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("OtlpFilePositionV0(<redacted>)")
    }
}

fn validate_otlp_file_index(
    actual: u32,
    maximum: u32,
    field: OtlpFilePositionFieldV0,
) -> Result<(), ValidationErrorV0> {
    if actual > maximum {
        return Err(ValidationErrorV0::InvalidOtlpFilePosition {
            field,
            problem: OtlpFilePositionProblemV0::AboveMaximum,
        });
    }
    Ok(())
}

#[derive(Clone, Copy, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct KafkaPositionV0 {
    partition: i32,
    offset: i64,
}

impl KafkaPositionV0 {
    pub fn new(partition: i32, offset: i64) -> Result<Self, ValidationErrorV0> {
        validate_kafka_coordinate(
            i64::from(partition),
            i64::from(MAX_KAFKA_PARTITION_V0),
            KafkaPositionFieldV0::Partition,
        )?;
        validate_kafka_coordinate(offset, MAX_KAFKA_OFFSET_V0, KafkaPositionFieldV0::Offset)?;
        Ok(Self { partition, offset })
    }

    pub const fn partition(self) -> i32 {
        self.partition
    }

    pub const fn offset(self) -> i64 {
        self.offset
    }

    pub const fn source_ordering_ordinal(self) -> u64 {
        ((self.partition as u64) << 48) | self.offset as u64
    }
}

impl fmt::Debug for KafkaPositionV0 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("KafkaPositionV0(<redacted>)")
    }
}

fn validate_kafka_coordinate(
    actual: i64,
    maximum: i64,
    field: KafkaPositionFieldV0,
) -> Result<(), ValidationErrorV0> {
    if actual < 0 {
        return Err(ValidationErrorV0::InvalidKafkaPosition {
            field,
            problem: KafkaPositionProblemV0::Negative,
        });
    }
    if actual > maximum {
        return Err(ValidationErrorV0::InvalidKafkaPosition {
            field,
            problem: KafkaPositionProblemV0::AboveMaximum,
        });
    }
    Ok(())
}

#[derive(Clone, Copy, Eq, Hash, PartialEq)]
pub enum SourcePositionV0 {
    Synthetic { sequence: u64 },
    OtlpFile { position: OtlpFilePositionV0 },
    Kafka { position: KafkaPositionV0 },
}

impl fmt::Debug for SourcePositionV0 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Synthetic { .. } => {
                formatter.write_str("SourcePositionV0::Synthetic { sequence: <redacted> }")
            }
            Self::OtlpFile { .. } => {
                formatter.write_str("SourcePositionV0::OtlpFile { position: <redacted> }")
            }
            Self::Kafka { .. } => {
                formatter.write_str("SourcePositionV0::Kafka { position: <redacted> }")
            }
        }
    }
}

impl Ord for SourcePositionV0 {
    fn cmp(&self, other: &Self) -> Ordering {
        match (self, other) {
            (Self::Synthetic { sequence: left }, Self::Synthetic { sequence: right }) => {
                left.cmp(right)
            }
            (Self::Synthetic { .. }, Self::OtlpFile { .. }) => Ordering::Less,
            (Self::OtlpFile { .. }, Self::Synthetic { .. }) => Ordering::Greater,
            (Self::OtlpFile { position: left }, Self::OtlpFile { position: right }) => {
                left.cmp(right)
            }
            (Self::Synthetic { .. } | Self::OtlpFile { .. }, Self::Kafka { .. }) => Ordering::Less,
            (Self::Kafka { .. }, Self::Synthetic { .. } | Self::OtlpFile { .. }) => {
                Ordering::Greater
            }
            (Self::Kafka { position: left }, Self::Kafka { position: right }) => left.cmp(right),
        }
    }
}

impl PartialOrd for SourcePositionV0 {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl SourcePositionV0 {
    pub fn otlp_file(
        line_number: u64,
        resource_spans_index: u32,
        scope_spans_index: u32,
        span_index: u32,
    ) -> Result<Self, ValidationErrorV0> {
        Ok(Self::OtlpFile {
            position: OtlpFilePositionV0::new(
                line_number,
                resource_spans_index,
                scope_spans_index,
                span_index,
            )?,
        })
    }

    pub fn kafka(partition: i32, offset: i64) -> Result<Self, ValidationErrorV0> {
        Ok(Self::Kafka {
            position: KafkaPositionV0::new(partition, offset)?,
        })
    }

    pub const fn kind(self) -> SourcePositionKindV0 {
        match self {
            Self::Synthetic { .. } => SourcePositionKindV0::Synthetic,
            Self::OtlpFile { .. } => SourcePositionKindV0::OtlpFile,
            Self::Kafka { .. } => SourcePositionKindV0::Kafka,
        }
    }

    pub const fn otlp_file_position(self) -> Option<OtlpFilePositionV0> {
        match self {
            Self::Synthetic { .. } => None,
            Self::OtlpFile { position } => Some(position),
            Self::Kafka { .. } => None,
        }
    }

    pub const fn kafka_position(self) -> Option<KafkaPositionV0> {
        match self {
            Self::Synthetic { .. } | Self::OtlpFile { .. } => None,
            Self::Kafka { position } => Some(position),
        }
    }

    pub const fn source_ordering_ordinal(self) -> u64 {
        match self {
            Self::Synthetic { sequence } => sequence,
            Self::OtlpFile { position } => position.source_ordering_ordinal(),
            Self::Kafka { position } => position.source_ordering_ordinal(),
        }
    }

    pub const fn synthetic_sequence(self) -> u64 {
        self.source_ordering_ordinal()
    }
}

#[derive(Clone, Copy, Eq, PartialEq)]
pub struct SensitivityBitmapV0(u64);

impl SensitivityBitmapV0 {
    pub const fn from_bits(bits: u64) -> Self {
        Self(bits)
    }

    pub const fn bits(self) -> u64 {
        self.0
    }
}

impl fmt::Debug for SensitivityBitmapV0 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("SensitivityBitmapV0(<redacted>)")
    }
}

#[derive(Clone, Eq, PartialEq)]
enum ScalarInnerV0 {
    Null,
    Bool(bool),
    I64(i64),
    U64(u64),
    Utf8(String),
    Bytes(Vec<u8>),
}

#[derive(Clone, Eq, PartialEq)]
pub struct ScalarValueV0(ScalarInnerV0);

impl ScalarValueV0 {
    pub const fn null() -> Self {
        Self(ScalarInnerV0::Null)
    }

    pub const fn from_bool(value: bool) -> Self {
        Self(ScalarInnerV0::Bool(value))
    }

    pub const fn from_i64(value: i64) -> Self {
        Self(ScalarInnerV0::I64(value))
    }

    pub const fn from_u64(value: u64) -> Self {
        Self(ScalarInnerV0::U64(value))
    }

    pub fn utf8(value: &str) -> Result<Self, ValidationErrorV0> {
        validate_scalar_length(ScalarKindV0::Utf8, value.len())?;
        Ok(Self(ScalarInnerV0::Utf8(value.to_owned())))
    }

    pub fn bytes(value: &[u8]) -> Result<Self, ValidationErrorV0> {
        validate_scalar_length(ScalarKindV0::Bytes, value.len())?;
        Ok(Self(ScalarInnerV0::Bytes(value.to_vec())))
    }

    pub const fn kind(&self) -> ScalarKindV0 {
        match &self.0 {
            ScalarInnerV0::Null => ScalarKindV0::Null,
            ScalarInnerV0::Bool(_) => ScalarKindV0::Bool,
            ScalarInnerV0::I64(_) => ScalarKindV0::I64,
            ScalarInnerV0::U64(_) => ScalarKindV0::U64,
            ScalarInnerV0::Utf8(_) => ScalarKindV0::Utf8,
            ScalarInnerV0::Bytes(_) => ScalarKindV0::Bytes,
        }
    }

    pub const fn as_bool(&self) -> Option<bool> {
        match &self.0 {
            ScalarInnerV0::Bool(value) => Some(*value),
            _ => None,
        }
    }

    pub const fn as_i64(&self) -> Option<i64> {
        match &self.0 {
            ScalarInnerV0::I64(value) => Some(*value),
            _ => None,
        }
    }

    pub const fn as_u64(&self) -> Option<u64> {
        match &self.0 {
            ScalarInnerV0::U64(value) => Some(*value),
            _ => None,
        }
    }

    pub fn as_utf8(&self) -> Option<&str> {
        match &self.0 {
            ScalarInnerV0::Utf8(value) => Some(value),
            _ => None,
        }
    }

    pub fn as_bytes(&self) -> Option<&[u8]> {
        match &self.0 {
            ScalarInnerV0::Bytes(value) => Some(value),
            _ => None,
        }
    }

    fn logical_content_len(&self) -> usize {
        match &self.0 {
            ScalarInnerV0::Null => 0,
            ScalarInnerV0::Bool(_) => 1,
            ScalarInnerV0::I64(_) | ScalarInnerV0::U64(_) => 8,
            ScalarInnerV0::Utf8(value) => VARIABLE_SCALAR_LENGTH_WIDTH + value.len(),
            ScalarInnerV0::Bytes(value) => VARIABLE_SCALAR_LENGTH_WIDTH + value.len(),
        }
    }
}

impl fmt::Debug for ScalarValueV0 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ScalarValueV0")
            .field("kind", &self.kind())
            .field("logical_content_len", &self.logical_content_len())
            .finish()
    }
}

fn validate_scalar_length(kind: ScalarKindV0, actual: usize) -> Result<(), ValidationErrorV0> {
    if actual > MAX_SCALAR_BYTES {
        return Err(ValidationErrorV0::ScalarTooLong {
            kind,
            maximum: MAX_SCALAR_BYTES,
            actual,
        });
    }
    Ok(())
}

#[derive(Clone, Eq, Hash, Ord, PartialEq, PartialOrd)]
struct FieldNameV0(String);

impl FieldNameV0 {
    fn new(value: &str) -> Result<Self, ValidationErrorV0> {
        let bytes = value.as_bytes();
        if !(MIN_FIELD_NAME_BYTES..=MAX_FIELD_NAME_BYTES).contains(&bytes.len()) {
            return Err(ValidationErrorV0::InvalidFieldNameLength {
                minimum: MIN_FIELD_NAME_BYTES,
                maximum: MAX_FIELD_NAME_BYTES,
                actual: bytes.len(),
            });
        }

        let valid_first = bytes[0].is_ascii_lowercase() || bytes[0] == b'_';
        let valid_rest = bytes[1..].iter().all(|byte| {
            byte.is_ascii_lowercase() || byte.is_ascii_digit() || matches!(byte, b'.' | b'_' | b'-')
        });

        if !valid_first || !valid_rest {
            return Err(ValidationErrorV0::InvalidFieldNameSyntax);
        }

        Ok(Self(value.to_owned()))
    }

    fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Debug for FieldNameV0 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("FieldNameV0")
            .field("byte_len", &self.0.len())
            .finish()
    }
}

#[derive(Clone, Eq, PartialEq)]
pub struct CanonicalPayloadV0 {
    fields: BTreeMap<FieldNameV0, ScalarValueV0>,
    logical_len: usize,
}

impl CanonicalPayloadV0 {
    pub fn try_from_fields(
        fields: BTreeMap<String, ScalarValueV0>,
    ) -> Result<Self, ValidationErrorV0> {
        if fields.len() > MAX_PAYLOAD_FIELDS {
            return Err(ValidationErrorV0::TooManyFields {
                maximum: MAX_PAYLOAD_FIELDS,
                actual: fields.len(),
            });
        }

        let mut canonical_fields = BTreeMap::new();
        for (name, value) in fields {
            canonical_fields.insert(FieldNameV0::new(&name)?, value);
        }

        let logical_len = calculate_logical_len(&canonical_fields)?;
        if logical_len > MAX_LOGICAL_PAYLOAD_BYTES {
            return Err(ValidationErrorV0::PayloadTooLarge {
                maximum: MAX_LOGICAL_PAYLOAD_BYTES,
                actual: logical_len,
            });
        }

        Ok(Self {
            fields: canonical_fields,
            logical_len,
        })
    }

    pub fn fields(&self) -> impl ExactSizeIterator<Item = (&str, &ScalarValueV0)> {
        self.fields
            .iter()
            .map(|(name, value)| (name.as_str(), value))
    }

    pub fn logical_len(&self) -> usize {
        self.logical_len
    }
}

impl fmt::Debug for CanonicalPayloadV0 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("CanonicalPayloadV0")
            .field("field_count", &self.fields.len())
            .field("logical_len", &self.logical_len)
            .finish()
    }
}

fn calculate_logical_len(
    fields: &BTreeMap<FieldNameV0, ScalarValueV0>,
) -> Result<usize, ValidationErrorV0> {
    let mut length = FIELD_COUNT_WIDTH;
    for (name, value) in fields {
        length = checked_length_add(
            length,
            FIELD_NAME_LENGTH_WIDTH,
            LengthComponentV0::FieldNameLength,
        )?;
        length = checked_length_add(length, name.0.len(), LengthComponentV0::FieldName)?;
        length = checked_length_add(length, SCALAR_TAG_WIDTH, LengthComponentV0::ScalarTag)?;
        length = checked_length_add(
            length,
            value.logical_content_len(),
            LengthComponentV0::ScalarContent,
        )?;
    }
    Ok(length)
}

fn checked_length_add(
    current: usize,
    added: usize,
    component: LengthComponentV0,
) -> Result<usize, ValidationErrorV0> {
    current
        .checked_add(added)
        .ok_or(ValidationErrorV0::LengthOverflow { component })
}

#[derive(Clone, Eq, PartialEq)]
pub struct ProvenanceIdV0(String);

impl ProvenanceIdV0 {
    fn derive(source_id: &SourceIdV0, source_position: SourcePositionV0) -> Self {
        match source_position {
            SourcePositionV0::Synthetic { sequence } => Self(format!(
                "decant://event/v0/{}/synthetic/{sequence}",
                source_id.as_str()
            )),
            SourcePositionV0::OtlpFile { position } => Self(format!(
                "decant://event/v0/{}/otlp-file/{}/{}/{}/{}",
                source_id.as_str(),
                position.line_number(),
                position.resource_spans_index(),
                position.scope_spans_index(),
                position.span_index()
            )),
            SourcePositionV0::Kafka { position } => Self(format!(
                "decant://event/v0/{}/kafka/{}/{}",
                source_id.as_str(),
                position.partition(),
                position.offset()
            )),
        }
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Debug for ProvenanceIdV0 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ProvenanceIdV0")
            .field("byte_len", &self.0.len())
            .finish()
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RedeliveryClassificationV0 {
    Distinct,
    ExactRedelivery,
    ConflictingRedelivery,
}

#[derive(Clone, Eq, PartialEq)]
pub struct EventEnvelopeV0 {
    envelope_version: u16,
    source_id: SourceIdV0,
    schema_id: SchemaIdV0,
    source_position: SourcePositionV0,
    key: Option<Vec<u8>>,
    event_time_ns: i64,
    ingest_time_ns: i64,
    sensitivity: SensitivityBitmapV0,
    payload: CanonicalPayloadV0,
    provenance: ProvenanceIdV0,
}

impl EventEnvelopeV0 {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        envelope_version: u16,
        source_id: SourceIdV0,
        schema_id: SchemaIdV0,
        source_position: SourcePositionV0,
        key: Option<&[u8]>,
        event_time_ns: i64,
        ingest_time_ns: i64,
        sensitivity: SensitivityBitmapV0,
        payload: CanonicalPayloadV0,
    ) -> Result<Self, ValidationErrorV0> {
        if envelope_version != ENVELOPE_VERSION_V0 {
            return Err(ValidationErrorV0::UnsupportedEnvelopeVersion {
                actual: envelope_version,
            });
        }

        if let Some(key) = key
            && key.len() > MAX_KEY_BYTES
        {
            return Err(ValidationErrorV0::KeyTooLong {
                maximum: MAX_KEY_BYTES,
                actual: key.len(),
            });
        }

        let provenance = ProvenanceIdV0::derive(&source_id, source_position);
        Ok(Self {
            envelope_version,
            source_id,
            schema_id,
            source_position,
            key: key.map(<[u8]>::to_vec),
            event_time_ns,
            ingest_time_ns,
            sensitivity,
            payload,
            provenance,
        })
    }

    pub fn envelope_version(&self) -> u16 {
        self.envelope_version
    }

    pub fn source_id(&self) -> &SourceIdV0 {
        &self.source_id
    }

    pub fn schema_id(&self) -> &SchemaIdV0 {
        &self.schema_id
    }

    pub fn source_position(&self) -> SourcePositionV0 {
        self.source_position
    }

    pub fn key(&self) -> Option<&[u8]> {
        self.key.as_deref()
    }

    pub fn event_time_ns(&self) -> i64 {
        self.event_time_ns
    }

    pub fn ingest_time_ns(&self) -> i64 {
        self.ingest_time_ns
    }

    pub fn sensitivity(&self) -> SensitivityBitmapV0 {
        self.sensitivity
    }

    pub fn payload(&self) -> &CanonicalPayloadV0 {
        &self.payload
    }

    pub fn provenance(&self) -> &ProvenanceIdV0 {
        &self.provenance
    }

    pub fn classify_redelivery(&self, other: &Self) -> RedeliveryClassificationV0 {
        if self.source_id != other.source_id || self.source_position != other.source_position {
            return RedeliveryClassificationV0::Distinct;
        }

        if self.schema_id == other.schema_id
            && self.key == other.key
            && self.event_time_ns == other.event_time_ns
            && self.sensitivity == other.sensitivity
            && self.payload == other.payload
        {
            RedeliveryClassificationV0::ExactRedelivery
        } else {
            RedeliveryClassificationV0::ConflictingRedelivery
        }
    }
}

impl fmt::Debug for EventEnvelopeV0 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("EventEnvelopeV0")
            .field("envelope_version", &self.envelope_version)
            .field("source_id", &self.source_id)
            .field("schema_id", &self.schema_id)
            .field("source_position", &self.source_position)
            .field("key_byte_len", &self.key.as_ref().map(Vec::len))
            .field("event_time_ns", &"<redacted>")
            .field("ingest_time_ns", &"<redacted>")
            .field("sensitivity", &self.sensitivity)
            .field("payload", &self.payload)
            .field("provenance", &self.provenance)
            .finish()
    }
}
