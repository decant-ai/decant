use std::collections::BTreeMap;
use std::error::Error;
use std::fmt;

use decant_ir::{
    CanonicalPayloadV0, EventEnvelopeV0, ScalarKindV0, ScalarValueV0, SchemaIdV0,
    SensitivityBitmapV0, SourceIdV0, SourcePositionV0, ValidationErrorV0,
};
use serde::{Deserialize, Serialize};

pub const MAX_FIXTURE_DOCUMENT_BYTES: usize = 262_144;
const RECORD_FORMAT_V0: &str = "decant-fixture-record";
const RECORD_VERSION_V0: u16 = 0;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum BinaryFieldV0 {
    Key,
    Scalar,
}

#[derive(Clone, Eq, PartialEq)]
pub enum FixtureCodecErrorV0 {
    DocumentTooLarge {
        maximum: usize,
        actual: usize,
    },
    MissingTerminalLineFeed,
    MalformedJson,
    UnsupportedRecordFormat,
    UnsupportedRecordVersion {
        actual: u16,
    },
    UnsupportedSourcePosition,
    MissingEventTime,
    InvalidHex {
        field: BinaryFieldV0,
        byte_len: usize,
    },
    DuplicatePayloadField {
        field_count: usize,
    },
    EnvelopeValidation(ValidationErrorV0),
    NonCanonicalEncoding,
    EncodingFailure,
}

impl fmt::Debug for FixtureCodecErrorV0 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::DocumentTooLarge { maximum, actual } => formatter
                .debug_struct("DocumentTooLarge")
                .field("maximum", maximum)
                .field("actual", actual)
                .finish(),
            Self::MissingTerminalLineFeed => formatter.write_str("MissingTerminalLineFeed"),
            Self::MalformedJson => formatter.write_str("MalformedJson"),
            Self::UnsupportedRecordFormat => formatter.write_str("UnsupportedRecordFormat"),
            Self::UnsupportedRecordVersion { actual } => formatter
                .debug_struct("UnsupportedRecordVersion")
                .field("actual", actual)
                .finish(),
            Self::UnsupportedSourcePosition => formatter.write_str("UnsupportedSourcePosition"),
            Self::MissingEventTime => formatter.write_str("MissingEventTime"),
            Self::InvalidHex { field, byte_len } => formatter
                .debug_struct("InvalidHex")
                .field("field", field)
                .field("byte_len", byte_len)
                .finish(),
            Self::DuplicatePayloadField { field_count } => formatter
                .debug_struct("DuplicatePayloadField")
                .field("field_count", field_count)
                .finish(),
            Self::EnvelopeValidation(error) => formatter
                .debug_tuple("EnvelopeValidation")
                .field(error)
                .finish(),
            Self::NonCanonicalEncoding => formatter.write_str("NonCanonicalEncoding"),
            Self::EncodingFailure => formatter.write_str("EncodingFailure"),
        }
    }
}

impl fmt::Display for FixtureCodecErrorV0 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(self, formatter)
    }
}

impl Error for FixtureCodecErrorV0 {}

impl From<ValidationErrorV0> for FixtureCodecErrorV0 {
    fn from(error: ValidationErrorV0) -> Self {
        Self::EnvelopeValidation(error)
    }
}

#[derive(Clone, Eq, PartialEq)]
pub struct EndOfInputV0 {
    source_id: SourceIdV0,
    finalized_through_ns: i64,
}

impl EndOfInputV0 {
    pub fn new(
        source_id: SourceIdV0,
        finalized_through_ns: i64,
    ) -> Result<Self, ValidationErrorV0> {
        Ok(Self {
            source_id,
            finalized_through_ns,
        })
    }

    pub fn source_id(&self) -> &SourceIdV0 {
        &self.source_id
    }

    pub fn finalized_through_ns(&self) -> i64 {
        self.finalized_through_ns
    }
}

impl fmt::Debug for EndOfInputV0 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("EndOfInputV0")
            .field("source_id", &self.source_id)
            .field("finalized_through_ns", &"<redacted>")
            .finish()
    }
}

#[derive(Clone, Eq, PartialEq)]
pub enum FixtureRecordV0 {
    Event(EventEnvelopeV0),
    EndOfInput(EndOfInputV0),
}

impl fmt::Debug for FixtureRecordV0 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Event(event) => formatter.debug_tuple("Event").field(event).finish(),
            Self::EndOfInput(control) => {
                formatter.debug_tuple("EndOfInput").field(control).finish()
            }
        }
    }
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields, tag = "record_type")]
enum WireRecordV0 {
    #[serde(rename = "event")]
    Event {
        record_format: String,
        record_version: u16,
        envelope_version: u16,
        source_id: String,
        schema_id: String,
        source_position: WireSourcePositionV0,
        key_hex: Option<String>,
        #[serde(default)]
        event_time_ns: Option<i64>,
        ingest_time_ns: i64,
        sensitivity_bits: u64,
        payload: Vec<WirePayloadFieldV0>,
    },
    #[serde(rename = "end_of_input")]
    EndOfInput {
        record_format: String,
        record_version: u16,
        source_id: String,
        finalized_through_ns: i64,
    },
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct WireSourcePositionV0 {
    kind: String,
    sequence: u64,
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct WirePayloadFieldV0 {
    name: String,
    value: WireScalarV0,
}

#[derive(Deserialize, Serialize)]
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

pub fn encode_fixture_record_v0(record: &FixtureRecordV0) -> Result<Vec<u8>, FixtureCodecErrorV0> {
    let wire = wire_from_record(record)?;
    let mut bytes = serde_json::to_vec(&wire).map_err(|_| FixtureCodecErrorV0::EncodingFailure)?;
    bytes.push(b'\n');
    if bytes.len() > MAX_FIXTURE_DOCUMENT_BYTES {
        return Err(FixtureCodecErrorV0::DocumentTooLarge {
            maximum: MAX_FIXTURE_DOCUMENT_BYTES,
            actual: bytes.len(),
        });
    }
    Ok(bytes)
}

pub fn decode_fixture_record_v0(input: &[u8]) -> Result<FixtureRecordV0, FixtureCodecErrorV0> {
    if input.len() > MAX_FIXTURE_DOCUMENT_BYTES {
        return Err(FixtureCodecErrorV0::DocumentTooLarge {
            maximum: MAX_FIXTURE_DOCUMENT_BYTES,
            actual: input.len(),
        });
    }
    let Some(json) = input.strip_suffix(b"\n") else {
        return Err(FixtureCodecErrorV0::MissingTerminalLineFeed);
    };

    let wire: WireRecordV0 =
        serde_json::from_slice(json).map_err(|_| FixtureCodecErrorV0::MalformedJson)?;
    let record = record_from_wire(wire)?;
    if encode_fixture_record_v0(&record)? != input {
        return Err(FixtureCodecErrorV0::NonCanonicalEncoding);
    }
    Ok(record)
}

fn wire_from_record(record: &FixtureRecordV0) -> Result<WireRecordV0, FixtureCodecErrorV0> {
    Ok(match record {
        FixtureRecordV0::Event(event) => {
            let SourcePositionV0::Synthetic { sequence } = event.source_position() else {
                return Err(FixtureCodecErrorV0::UnsupportedSourcePosition);
            };
            WireRecordV0::Event {
                record_format: RECORD_FORMAT_V0.to_owned(),
                record_version: RECORD_VERSION_V0,
                envelope_version: event.envelope_version(),
                source_id: event.source_id().as_str().to_owned(),
                schema_id: event.schema_id().as_str().to_owned(),
                source_position: WireSourcePositionV0 {
                    kind: "synthetic".to_owned(),
                    sequence,
                },
                key_hex: event.key().map(encode_hex),
                event_time_ns: Some(event.event_time_ns()),
                ingest_time_ns: event.ingest_time_ns(),
                sensitivity_bits: event.sensitivity().bits(),
                payload: event
                    .payload()
                    .fields()
                    .map(|(name, value)| WirePayloadFieldV0 {
                        name: name.to_owned(),
                        value: wire_from_scalar(value),
                    })
                    .collect(),
            }
        }
        FixtureRecordV0::EndOfInput(control) => WireRecordV0::EndOfInput {
            record_format: RECORD_FORMAT_V0.to_owned(),
            record_version: RECORD_VERSION_V0,
            source_id: control.source_id().as_str().to_owned(),
            finalized_through_ns: control.finalized_through_ns(),
        },
    })
}

#[allow(clippy::too_many_lines)]
fn record_from_wire(wire: WireRecordV0) -> Result<FixtureRecordV0, FixtureCodecErrorV0> {
    match wire {
        WireRecordV0::Event {
            record_format,
            record_version,
            envelope_version,
            source_id,
            schema_id,
            source_position,
            key_hex,
            event_time_ns,
            ingest_time_ns,
            sensitivity_bits,
            payload,
        } => {
            validate_header(&record_format, record_version)?;
            if source_position.kind != "synthetic" {
                return Err(FixtureCodecErrorV0::UnsupportedSourcePosition);
            }
            let event_time_ns = event_time_ns.ok_or(FixtureCodecErrorV0::MissingEventTime)?;
            let key = key_hex
                .as_deref()
                .map(|value| decode_hex(value, BinaryFieldV0::Key))
                .transpose()?;
            let field_count = payload.len();
            let mut fields = BTreeMap::new();
            for field in payload {
                let value = scalar_from_wire(field.value)?;
                if fields.insert(field.name, value).is_some() {
                    return Err(FixtureCodecErrorV0::DuplicatePayloadField { field_count });
                }
            }
            let payload = CanonicalPayloadV0::try_from_fields(fields)?;
            let event = EventEnvelopeV0::new(
                envelope_version,
                SourceIdV0::new(&source_id)?,
                SchemaIdV0::new(&schema_id)?,
                SourcePositionV0::Synthetic {
                    sequence: source_position.sequence,
                },
                key.as_deref(),
                event_time_ns,
                ingest_time_ns,
                SensitivityBitmapV0::from_bits(sensitivity_bits),
                payload,
            )?;
            Ok(FixtureRecordV0::Event(event))
        }
        WireRecordV0::EndOfInput {
            record_format,
            record_version,
            source_id,
            finalized_through_ns,
        } => {
            validate_header(&record_format, record_version)?;
            Ok(FixtureRecordV0::EndOfInput(EndOfInputV0::new(
                SourceIdV0::new(&source_id)?,
                finalized_through_ns,
            )?))
        }
    }
}

fn validate_header(record_format: &str, record_version: u16) -> Result<(), FixtureCodecErrorV0> {
    if record_format != RECORD_FORMAT_V0 {
        return Err(FixtureCodecErrorV0::UnsupportedRecordFormat);
    }
    if record_version != RECORD_VERSION_V0 {
        return Err(FixtureCodecErrorV0::UnsupportedRecordVersion {
            actual: record_version,
        });
    }
    Ok(())
}

fn wire_from_scalar(value: &ScalarValueV0) -> WireScalarV0 {
    match value.kind() {
        ScalarKindV0::Null => WireScalarV0::Null,
        ScalarKindV0::Bool => WireScalarV0::Bool(value.as_bool().expect("kind checked")),
        ScalarKindV0::I64 => WireScalarV0::I64(value.as_i64().expect("kind checked")),
        ScalarKindV0::U64 => WireScalarV0::U64(value.as_u64().expect("kind checked")),
        ScalarKindV0::Utf8 => WireScalarV0::Utf8(value.as_utf8().expect("kind checked").to_owned()),
        ScalarKindV0::Bytes => {
            WireScalarV0::Bytes(encode_hex(value.as_bytes().expect("kind checked")))
        }
    }
}

fn scalar_from_wire(wire: WireScalarV0) -> Result<ScalarValueV0, FixtureCodecErrorV0> {
    match wire {
        WireScalarV0::Null => Ok(ScalarValueV0::null()),
        WireScalarV0::Bool(value) => Ok(ScalarValueV0::from_bool(value)),
        WireScalarV0::I64(value) => Ok(ScalarValueV0::from_i64(value)),
        WireScalarV0::U64(value) => Ok(ScalarValueV0::from_u64(value)),
        WireScalarV0::Utf8(value) => Ok(ScalarValueV0::utf8(&value)?),
        WireScalarV0::Bytes(value) => Ok(ScalarValueV0::bytes(&decode_hex(
            &value,
            BinaryFieldV0::Scalar,
        )?)?),
    }
}

fn encode_hex(bytes: &[u8]) -> String {
    const DIGITS: &[u8; 16] = b"0123456789abcdef";
    let mut output = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        output.push(char::from(DIGITS[usize::from(byte >> 4)]));
        output.push(char::from(DIGITS[usize::from(byte & 0x0f)]));
    }
    output
}

fn decode_hex(value: &str, field: BinaryFieldV0) -> Result<Vec<u8>, FixtureCodecErrorV0> {
    let bytes = value.as_bytes();
    if !bytes.len().is_multiple_of(2)
        || !bytes
            .iter()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(byte))
    {
        return Err(FixtureCodecErrorV0::InvalidHex {
            field,
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

fn hex_nibble(byte: u8) -> Result<u8, FixtureCodecErrorV0> {
    match byte {
        b'0'..=b'9' => Ok(byte - b'0'),
        b'a'..=b'f' => Ok(byte - b'a' + 10),
        _ => Err(FixtureCodecErrorV0::InvalidHex {
            field: BinaryFieldV0::Scalar,
            byte_len: 1,
        }),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn lowercase_hex_is_a_bijection_at_boundaries() {
        for bytes in [vec![], vec![0], vec![0x0f, 0x10, 0xab, 0xff]] {
            assert_eq!(
                decode_hex(&encode_hex(&bytes), BinaryFieldV0::Key),
                Ok(bytes)
            );
        }
    }

    #[test]
    fn envelope_version_constant_is_the_wire_default() {
        assert_eq!(decant_ir::ENVELOPE_VERSION_V0, 0);
    }
}
