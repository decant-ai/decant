//! Bounded configuration and deterministic identities for flat Kafka JSON objects.

use std::error::Error;
use std::fmt;

use decant_ir::{MAX_FIELD_NAME_BYTES, SchemaIdV0, SourceIdV0, ValidationErrorV0};
use sha2::{Digest, Sha256};

pub const KAFKA_JSON_OBJECT_MAPPING_FORMAT_V0: &str = "decant-kafka-json-object-mapping";
pub const KAFKA_JSON_OBJECT_MAPPING_VERSION_V0: u16 = 0;
pub const MAX_KAFKA_JSON_OBJECT_SOURCE_FIELD_BYTES_V0: usize = 128;
pub const MAX_KAFKA_JSON_OBJECT_PROJECTIONS_V0: usize = 64;
pub const MAX_KAFKA_JSON_OBJECT_MAPPING_PREIMAGE_BYTES_V0: usize = 16 * 1024;

const MAPPING_DOMAIN_V0: &[u8] = b"decant-kafka-json-object-mapping-v0";
const SOURCE_DOMAIN_V0: &[u8] = b"decant-kafka-json-object-source-v0";
const DIGEST_BYTES: usize = 32;
const HEX_DIGEST_BYTES: usize = DIGEST_BYTES * 2;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum KafkaJsonEventTimeUnitV0 {
    Nanoseconds,
    Microseconds,
    Milliseconds,
    Seconds,
}

impl KafkaJsonEventTimeUnitV0 {
    pub const fn multiplier_ns(self) -> i64 {
        match self {
            Self::Nanoseconds => 1,
            Self::Microseconds => 1_000,
            Self::Milliseconds => 1_000_000,
            Self::Seconds => 1_000_000_000,
        }
    }

    const fn tag(self) -> u8 {
        match self {
            Self::Nanoseconds => 0,
            Self::Microseconds => 1,
            Self::Milliseconds => 2,
            Self::Seconds => 3,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum KafkaJsonScalarKindV0 {
    Null,
    Bool,
    I64,
    U64,
    Utf8,
}

impl KafkaJsonScalarKindV0 {
    const fn tag(self) -> u8 {
        match self {
            Self::Null => 0,
            Self::Bool => 1,
            Self::I64 => 2,
            Self::U64 => 3,
            Self::Utf8 => 4,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum KafkaJsonObjectMappingNameV0 {
    EventTimeSource,
    ProjectionSource,
}

#[derive(Clone, Eq, PartialEq)]
pub enum KafkaJsonObjectMappingErrorV0 {
    UnsupportedVersion {
        actual: u16,
    },
    InvalidSourceNameLength {
        name: KafkaJsonObjectMappingNameV0,
        minimum: usize,
        maximum: usize,
        actual: usize,
    },
    InvalidSourceNameSyntax {
        name: KafkaJsonObjectMappingNameV0,
    },
    InvalidTargetName {
        category: ValidationErrorV0,
    },
    TooManyProjections {
        maximum: usize,
        actual: usize,
    },
    DuplicateProjectionSource,
    DuplicateProjectionTarget,
    EncodingLengthOverflow,
    EncodingTooLarge {
        maximum: usize,
        actual: usize,
    },
    AllocationFailed,
    DerivedIdentifier {
        category: ValidationErrorV0,
    },
}

impl fmt::Debug for KafkaJsonObjectMappingErrorV0 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::UnsupportedVersion { actual } => formatter
                .debug_struct("UnsupportedVersion")
                .field("actual", actual)
                .finish(),
            Self::InvalidSourceNameLength {
                name,
                minimum,
                maximum,
                actual,
            } => formatter
                .debug_struct("InvalidSourceNameLength")
                .field("name", name)
                .field("minimum", minimum)
                .field("maximum", maximum)
                .field("actual", actual)
                .finish(),
            Self::InvalidSourceNameSyntax { name } => formatter
                .debug_struct("InvalidSourceNameSyntax")
                .field("name", name)
                .finish(),
            Self::InvalidTargetName { category } => formatter
                .debug_struct("InvalidTargetName")
                .field("category", category)
                .finish(),
            Self::TooManyProjections { maximum, actual } => formatter
                .debug_struct("TooManyProjections")
                .field("maximum", maximum)
                .field("actual", actual)
                .finish(),
            Self::DuplicateProjectionSource => formatter.write_str("DuplicateProjectionSource"),
            Self::DuplicateProjectionTarget => formatter.write_str("DuplicateProjectionTarget"),
            Self::EncodingLengthOverflow => formatter.write_str("EncodingLengthOverflow"),
            Self::EncodingTooLarge { maximum, actual } => formatter
                .debug_struct("EncodingTooLarge")
                .field("maximum", maximum)
                .field("actual", actual)
                .finish(),
            Self::AllocationFailed => formatter.write_str("AllocationFailed"),
            Self::DerivedIdentifier { category } => formatter
                .debug_struct("DerivedIdentifier")
                .field("category", category)
                .finish(),
        }
    }
}

impl fmt::Display for KafkaJsonObjectMappingErrorV0 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(self, formatter)
    }
}

impl Error for KafkaJsonObjectMappingErrorV0 {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::InvalidTargetName { category } | Self::DerivedIdentifier { category } => {
                Some(category)
            }
            _ => None,
        }
    }
}

#[derive(Clone, Eq, PartialEq)]
pub struct KafkaJsonObjectProjectionV0 {
    source_field: String,
    target_field: String,
    kind: KafkaJsonScalarKindV0,
}

impl KafkaJsonObjectProjectionV0 {
    pub fn new(
        source_field: &str,
        target_field: &str,
        kind: KafkaJsonScalarKindV0,
    ) -> Result<Self, KafkaJsonObjectMappingErrorV0> {
        validate_source_name(source_field, KafkaJsonObjectMappingNameV0::ProjectionSource)?;
        validate_target_name(target_field)?;
        Ok(Self {
            source_field: copy_string(source_field)?,
            target_field: copy_string(target_field)?,
            kind,
        })
    }

    pub fn source_field(&self) -> &str {
        &self.source_field
    }

    pub fn target_field(&self) -> &str {
        &self.target_field
    }

    pub const fn kind(&self) -> KafkaJsonScalarKindV0 {
        self.kind
    }
}

impl fmt::Debug for KafkaJsonObjectProjectionV0 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("KafkaJsonObjectProjectionV0")
            .field("source_field_bytes", &self.source_field.len())
            .field("target_field_bytes", &self.target_field.len())
            .field("kind", &self.kind)
            .finish()
    }
}

#[derive(Clone, Eq, PartialEq)]
pub struct KafkaJsonObjectMappingV0 {
    source_namespace: SourceIdV0,
    event_time_source_field: String,
    event_time_unit: KafkaJsonEventTimeUnitV0,
    projections: Vec<KafkaJsonObjectProjectionV0>,
    canonical_preimage: Vec<u8>,
    mapping_digest: [u8; DIGEST_BYTES],
    schema_id: SchemaIdV0,
    effective_source_id: SourceIdV0,
}

impl KafkaJsonObjectMappingV0 {
    pub fn new(
        version: u16,
        source_namespace: SourceIdV0,
        event_time_source_field: &str,
        event_time_unit: KafkaJsonEventTimeUnitV0,
        mut projections: Vec<KafkaJsonObjectProjectionV0>,
    ) -> Result<Self, KafkaJsonObjectMappingErrorV0> {
        if version != KAFKA_JSON_OBJECT_MAPPING_VERSION_V0 {
            return Err(KafkaJsonObjectMappingErrorV0::UnsupportedVersion { actual: version });
        }
        validate_source_name(
            event_time_source_field,
            KafkaJsonObjectMappingNameV0::EventTimeSource,
        )?;
        if projections.len() > MAX_KAFKA_JSON_OBJECT_PROJECTIONS_V0 {
            return Err(KafkaJsonObjectMappingErrorV0::TooManyProjections {
                maximum: MAX_KAFKA_JSON_OBJECT_PROJECTIONS_V0,
                actual: projections.len(),
            });
        }
        for left in 0..projections.len() {
            for right in (left + 1)..projections.len() {
                if projections[left].source_field == projections[right].source_field {
                    return Err(KafkaJsonObjectMappingErrorV0::DuplicateProjectionSource);
                }
                if projections[left].target_field == projections[right].target_field {
                    return Err(KafkaJsonObjectMappingErrorV0::DuplicateProjectionTarget);
                }
            }
        }
        projections.sort_by(|left, right| {
            left.target_field
                .cmp(&right.target_field)
                .then_with(|| left.source_field.cmp(&right.source_field))
        });

        let event_time_source_field = copy_string(event_time_source_field)?;
        let canonical_preimage =
            encode_mapping_preimage(&event_time_source_field, event_time_unit, &projections)?;
        let mapping_digest = sha256(&canonical_preimage);
        let schema_id = SchemaIdV0::new(&lower_hex(&mapping_digest)?)
            .map_err(|category| KafkaJsonObjectMappingErrorV0::DerivedIdentifier { category })?;
        let effective_source_digest = effective_source_digest(&source_namespace, &mapping_digest)?;
        let effective_source_id = SourceIdV0::new(&lower_hex(&effective_source_digest)?)
            .map_err(|category| KafkaJsonObjectMappingErrorV0::DerivedIdentifier { category })?;

        Ok(Self {
            source_namespace,
            event_time_source_field,
            event_time_unit,
            projections,
            canonical_preimage,
            mapping_digest,
            schema_id,
            effective_source_id,
        })
    }

    pub const fn version(&self) -> u16 {
        KAFKA_JSON_OBJECT_MAPPING_VERSION_V0
    }

    pub fn event_time_source_field(&self) -> &str {
        &self.event_time_source_field
    }

    pub const fn event_time_unit(&self) -> KafkaJsonEventTimeUnitV0 {
        self.event_time_unit
    }

    pub fn projections(&self) -> &[KafkaJsonObjectProjectionV0] {
        &self.projections
    }

    pub fn canonical_preimage(&self) -> &[u8] {
        &self.canonical_preimage
    }

    pub const fn mapping_digest(&self) -> &[u8; DIGEST_BYTES] {
        &self.mapping_digest
    }

    pub fn schema_id(&self) -> &SchemaIdV0 {
        &self.schema_id
    }

    pub fn effective_source_id(&self) -> &SourceIdV0 {
        &self.effective_source_id
    }
}

impl fmt::Debug for KafkaJsonObjectMappingV0 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("KafkaJsonObjectMappingV0")
            .field("version", &self.version())
            .field(
                "source_namespace_bytes",
                &self.source_namespace.as_str().len(),
            )
            .field(
                "event_time_source_field_bytes",
                &self.event_time_source_field.len(),
            )
            .field("event_time_unit", &self.event_time_unit)
            .field("projection_count", &self.projections.len())
            .field("canonical_preimage_bytes", &self.canonical_preimage.len())
            .field("mapping_digest", &"<redacted>")
            .field("schema_id", &"<redacted>")
            .field("effective_source_id", &"<redacted>")
            .finish()
    }
}

fn validate_source_name(
    value: &str,
    name: KafkaJsonObjectMappingNameV0,
) -> Result<(), KafkaJsonObjectMappingErrorV0> {
    if !(1..=MAX_KAFKA_JSON_OBJECT_SOURCE_FIELD_BYTES_V0).contains(&value.len()) {
        return Err(KafkaJsonObjectMappingErrorV0::InvalidSourceNameLength {
            name,
            minimum: 1,
            maximum: MAX_KAFKA_JSON_OBJECT_SOURCE_FIELD_BYTES_V0,
            actual: value.len(),
        });
    }
    if value.chars().any(char::is_control) {
        return Err(KafkaJsonObjectMappingErrorV0::InvalidSourceNameSyntax { name });
    }
    Ok(())
}

fn validate_target_name(value: &str) -> Result<(), KafkaJsonObjectMappingErrorV0> {
    let bytes = value.as_bytes();
    if !(1..=MAX_FIELD_NAME_BYTES).contains(&bytes.len()) {
        return Err(KafkaJsonObjectMappingErrorV0::InvalidTargetName {
            category: ValidationErrorV0::InvalidFieldNameLength {
                minimum: 1,
                maximum: MAX_FIELD_NAME_BYTES,
                actual: bytes.len(),
            },
        });
    }
    let valid_first = bytes[0].is_ascii_lowercase() || bytes[0] == b'_';
    let valid_rest = bytes[1..].iter().all(|byte| {
        byte.is_ascii_lowercase() || byte.is_ascii_digit() || matches!(byte, b'.' | b'_' | b'-')
    });
    if !valid_first || !valid_rest {
        return Err(KafkaJsonObjectMappingErrorV0::InvalidTargetName {
            category: ValidationErrorV0::InvalidFieldNameSyntax,
        });
    }
    Ok(())
}

fn copy_string(value: &str) -> Result<String, KafkaJsonObjectMappingErrorV0> {
    let mut copy = String::new();
    copy.try_reserve_exact(value.len())
        .map_err(|_| KafkaJsonObjectMappingErrorV0::AllocationFailed)?;
    copy.push_str(value);
    Ok(copy)
}

fn encode_mapping_preimage(
    event_time_source_field: &str,
    event_time_unit: KafkaJsonEventTimeUnitV0,
    projections: &[KafkaJsonObjectProjectionV0],
) -> Result<Vec<u8>, KafkaJsonObjectMappingErrorV0> {
    let mut length = MAPPING_DOMAIN_V0.len();
    length = checked_add(length, 2)?;
    length = checked_add(length, 2)?;
    length = checked_add(length, event_time_source_field.len())?;
    length = checked_add(length, 1)?;
    length = checked_add(length, 2)?;
    for projection in projections {
        length = checked_add(length, 2)?;
        length = checked_add(length, projection.source_field.len())?;
        length = checked_add(length, 2)?;
        length = checked_add(length, projection.target_field.len())?;
        length = checked_add(length, 1)?;
    }
    if length > MAX_KAFKA_JSON_OBJECT_MAPPING_PREIMAGE_BYTES_V0 {
        return Err(KafkaJsonObjectMappingErrorV0::EncodingTooLarge {
            maximum: MAX_KAFKA_JSON_OBJECT_MAPPING_PREIMAGE_BYTES_V0,
            actual: length,
        });
    }
    let mut output = Vec::new();
    output
        .try_reserve_exact(length)
        .map_err(|_| KafkaJsonObjectMappingErrorV0::AllocationFailed)?;
    output.extend_from_slice(MAPPING_DOMAIN_V0);
    output.extend_from_slice(&KAFKA_JSON_OBJECT_MAPPING_VERSION_V0.to_be_bytes());
    append_string(&mut output, event_time_source_field)?;
    output.push(event_time_unit.tag());
    output.extend_from_slice(
        &u16::try_from(projections.len())
            .map_err(|_| KafkaJsonObjectMappingErrorV0::EncodingLengthOverflow)?
            .to_be_bytes(),
    );
    for projection in projections {
        append_string(&mut output, &projection.source_field)?;
        append_string(&mut output, &projection.target_field)?;
        output.push(projection.kind.tag());
    }
    debug_assert_eq!(output.len(), length);
    Ok(output)
}

fn append_string(output: &mut Vec<u8>, value: &str) -> Result<(), KafkaJsonObjectMappingErrorV0> {
    let length = u16::try_from(value.len())
        .map_err(|_| KafkaJsonObjectMappingErrorV0::EncodingLengthOverflow)?;
    output.extend_from_slice(&length.to_be_bytes());
    output.extend_from_slice(value.as_bytes());
    Ok(())
}

fn checked_add(left: usize, right: usize) -> Result<usize, KafkaJsonObjectMappingErrorV0> {
    left.checked_add(right)
        .ok_or(KafkaJsonObjectMappingErrorV0::EncodingLengthOverflow)
}

fn sha256(bytes: &[u8]) -> [u8; DIGEST_BYTES] {
    let output = Sha256::digest(bytes);
    let mut digest = [0_u8; DIGEST_BYTES];
    digest.copy_from_slice(&output);
    digest
}

fn effective_source_digest(
    source_namespace: &SourceIdV0,
    mapping_digest: &[u8; DIGEST_BYTES],
) -> Result<[u8; DIGEST_BYTES], KafkaJsonObjectMappingErrorV0> {
    let namespace = source_namespace.as_str().as_bytes();
    let namespace_length = u16::try_from(namespace.len())
        .map_err(|_| KafkaJsonObjectMappingErrorV0::EncodingLengthOverflow)?;
    let mut hasher = Sha256::new();
    hasher.update(SOURCE_DOMAIN_V0);
    hasher.update(namespace_length.to_be_bytes());
    hasher.update(namespace);
    hasher.update(mapping_digest);
    let output = hasher.finalize();
    let mut digest = [0_u8; DIGEST_BYTES];
    digest.copy_from_slice(&output);
    Ok(digest)
}

fn lower_hex(bytes: &[u8; DIGEST_BYTES]) -> Result<String, KafkaJsonObjectMappingErrorV0> {
    const DIGITS: &[u8; 16] = b"0123456789abcdef";
    let mut output = String::new();
    output
        .try_reserve_exact(HEX_DIGEST_BYTES)
        .map_err(|_| KafkaJsonObjectMappingErrorV0::AllocationFailed)?;
    for byte in bytes {
        output.push(char::from(DIGITS[usize::from(byte >> 4)]));
        output.push(char::from(DIGITS[usize::from(byte & 0x0f)]));
    }
    Ok(output)
}
