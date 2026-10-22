//! Bounded decoding for the private Decant Kafka event JSON value V0.

use std::collections::BTreeMap;
use std::error::Error;
use std::fmt;

use decant_ir::{CanonicalPayloadV0, ScalarValueV0, SchemaIdV0, ValidationErrorV0};
use serde::Deserialize;

use crate::MAX_KAFKA_RECORD_VALUE_BYTES_V0;

pub const DECANT_KAFKA_EVENT_FORMAT_V0: &str = "decant-kafka-event";
pub const DECANT_KAFKA_EVENT_VALUE_VERSION_V0: u16 = 0;
pub const MAX_KAFKA_JSON_VALUE_BYTES_V0: usize = MAX_KAFKA_RECORD_VALUE_BYTES_V0;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum KafkaJsonBinaryFieldV0 {
    Scalar,
}

#[derive(Clone, Eq, PartialEq)]
pub enum KafkaEventValueDecodeErrorV0 {
    TombstoneUnsupported,
    InputTooLarge {
        maximum: usize,
        actual: usize,
    },
    MalformedJson,
    UnsupportedFormat,
    UnsupportedVersion {
        actual: u16,
    },
    MissingEventTime,
    DuplicatePayloadField {
        field_count: usize,
    },
    InvalidHex {
        field: KafkaJsonBinaryFieldV0,
        byte_len: usize,
    },
    CanonicalValidation {
        category: ValidationErrorV0,
    },
}

impl fmt::Debug for KafkaEventValueDecodeErrorV0 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::TombstoneUnsupported => formatter.write_str("TombstoneUnsupported"),
            Self::InputTooLarge { maximum, actual } => formatter
                .debug_struct("InputTooLarge")
                .field("maximum", maximum)
                .field("actual", actual)
                .finish(),
            Self::MalformedJson => formatter.write_str("MalformedJson"),
            Self::UnsupportedFormat => formatter.write_str("UnsupportedFormat"),
            Self::UnsupportedVersion { actual } => formatter
                .debug_struct("UnsupportedVersion")
                .field("actual", actual)
                .finish(),
            Self::MissingEventTime => formatter.write_str("MissingEventTime"),
            Self::DuplicatePayloadField { field_count } => formatter
                .debug_struct("DuplicatePayloadField")
                .field("field_count", field_count)
                .finish(),
            Self::InvalidHex { field, byte_len } => formatter
                .debug_struct("InvalidHex")
                .field("field", field)
                .field("byte_len", byte_len)
                .finish(),
            Self::CanonicalValidation { category } => formatter
                .debug_struct("CanonicalValidation")
                .field("category", category)
                .finish(),
        }
    }
}

impl fmt::Display for KafkaEventValueDecodeErrorV0 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(self, formatter)
    }
}

impl Error for KafkaEventValueDecodeErrorV0 {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::CanonicalValidation { category } => Some(category),
            _ => None,
        }
    }
}

impl From<ValidationErrorV0> for KafkaEventValueDecodeErrorV0 {
    fn from(category: ValidationErrorV0) -> Self {
        Self::CanonicalValidation { category }
    }
}

#[derive(Clone, Eq, PartialEq)]
pub struct DecodedKafkaEventV0 {
    schema_id: SchemaIdV0,
    event_time_ns: i64,
    payload: CanonicalPayloadV0,
}

impl DecodedKafkaEventV0 {
    pub fn schema_id(&self) -> &SchemaIdV0 {
        &self.schema_id
    }

    pub const fn event_time_ns(&self) -> i64 {
        self.event_time_ns
    }

    pub fn payload(&self) -> &CanonicalPayloadV0 {
        &self.payload
    }

    pub fn into_parts(self) -> (SchemaIdV0, i64, CanonicalPayloadV0) {
        (self.schema_id, self.event_time_ns, self.payload)
    }
}

impl fmt::Debug for DecodedKafkaEventV0 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("DecodedKafkaEventV0")
            .field("schema_id", &self.schema_id)
            .field("event_time_ns", &"<redacted>")
            .field("payload_fields", &self.payload.fields().len())
            .field("payload_bytes", &self.payload.logical_len())
            .finish()
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct WireEventV0 {
    record_format: String,
    record_version: u16,
    schema_id: String,
    #[serde(default)]
    event_time_ns: Option<i64>,
    payload: Vec<WirePayloadFieldV0>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct WirePayloadFieldV0 {
    name: String,
    value: WireScalarV0,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields, tag = "kind", content = "value")]
enum WireScalarV0 {
    #[serde(rename = "null")]
    Null,
    #[serde(rename = "bool")]
    Bool(bool),
    #[serde(rename = "i64")]
    I64(i64),
    #[serde(rename = "u64")]
    U64(u64),
    #[serde(rename = "utf8")]
    Utf8(String),
    #[serde(rename = "bytes")]
    Bytes(String),
}

pub fn decode_decant_kafka_event_value_v0(
    value: Option<&[u8]>,
) -> Result<DecodedKafkaEventV0, KafkaEventValueDecodeErrorV0> {
    let value = value.ok_or(KafkaEventValueDecodeErrorV0::TombstoneUnsupported)?;
    if value.len() > MAX_KAFKA_JSON_VALUE_BYTES_V0 {
        return Err(KafkaEventValueDecodeErrorV0::InputTooLarge {
            maximum: MAX_KAFKA_JSON_VALUE_BYTES_V0,
            actual: value.len(),
        });
    }

    let wire: WireEventV0 =
        serde_json::from_slice(value).map_err(|_| KafkaEventValueDecodeErrorV0::MalformedJson)?;
    if wire.record_format != DECANT_KAFKA_EVENT_FORMAT_V0 {
        return Err(KafkaEventValueDecodeErrorV0::UnsupportedFormat);
    }
    if wire.record_version != DECANT_KAFKA_EVENT_VALUE_VERSION_V0 {
        return Err(KafkaEventValueDecodeErrorV0::UnsupportedVersion {
            actual: wire.record_version,
        });
    }
    let event_time_ns = wire
        .event_time_ns
        .ok_or(KafkaEventValueDecodeErrorV0::MissingEventTime)?;
    let field_count = wire.payload.len();
    let mut fields = BTreeMap::new();
    for field in wire.payload {
        let scalar = scalar_from_wire(field.value)?;
        if fields.insert(field.name, scalar).is_some() {
            return Err(KafkaEventValueDecodeErrorV0::DuplicatePayloadField { field_count });
        }
    }

    Ok(DecodedKafkaEventV0 {
        schema_id: SchemaIdV0::new(&wire.schema_id)?,
        event_time_ns,
        payload: CanonicalPayloadV0::try_from_fields(fields)?,
    })
}

fn scalar_from_wire(wire: WireScalarV0) -> Result<ScalarValueV0, KafkaEventValueDecodeErrorV0> {
    match wire {
        WireScalarV0::Null => Ok(ScalarValueV0::null()),
        WireScalarV0::Bool(value) => Ok(ScalarValueV0::from_bool(value)),
        WireScalarV0::I64(value) => Ok(ScalarValueV0::from_i64(value)),
        WireScalarV0::U64(value) => Ok(ScalarValueV0::from_u64(value)),
        WireScalarV0::Utf8(value) => Ok(ScalarValueV0::utf8(&value)?),
        WireScalarV0::Bytes(value) => Ok(ScalarValueV0::bytes(&decode_lower_hex(&value)?)?),
    }
}

fn decode_lower_hex(value: &str) -> Result<Vec<u8>, KafkaEventValueDecodeErrorV0> {
    let bytes = value.as_bytes();
    if !bytes.len().is_multiple_of(2)
        || !bytes
            .iter()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(byte))
    {
        return Err(KafkaEventValueDecodeErrorV0::InvalidHex {
            field: KafkaJsonBinaryFieldV0::Scalar,
            byte_len: bytes.len(),
        });
    }
    let (pairs, remainder) = bytes.as_chunks::<2>();
    debug_assert!(remainder.is_empty());
    pairs
        .iter()
        .map(|pair| Ok((hex_nibble(pair[0])? << 4) | hex_nibble(pair[1])?))
        .collect()
}

fn hex_nibble(byte: u8) -> Result<u8, KafkaEventValueDecodeErrorV0> {
    match byte {
        b'0'..=b'9' => Ok(byte - b'0'),
        b'a'..=b'f' => Ok(byte - b'a' + 10),
        _ => Err(KafkaEventValueDecodeErrorV0::InvalidHex {
            field: KafkaJsonBinaryFieldV0::Scalar,
            byte_len: 1,
        }),
    }
}
