//! Entity-key-aware identity for flat Kafka JSON object mappings.

use std::error::Error;
use std::fmt;

use decant_ir::{MAX_FIELD_NAME_BYTES, SchemaIdV0, SourceIdV0, ValidationErrorV0};
use sha2::{Digest, Sha256};

use crate::{KafkaJsonObjectMappingV0, KafkaJsonScalarKindV0};

pub const KAFKA_JSON_OBJECT_MAPPING_VERSION_V1: u16 = 1;
pub const MAX_KAFKA_JSON_OBJECT_MAPPING_PREIMAGE_BYTES_V1: usize = 16 * 1024;

const MAPPING_DOMAIN_V1: &[u8] = b"decant-kafka-json-object-mapping-v1";
const SOURCE_DOMAIN_V1: &[u8] = b"decant-kafka-json-object-source-v1";
const DIGEST_BYTES: usize = 32;
const HEX_DIGEST_BYTES: usize = DIGEST_BYTES * 2;

#[derive(Clone, Eq, PartialEq)]
pub enum KafkaJsonEntityKeySourceV1 {
    KafkaRecordKey,
    Utf8Projection { target_field: String },
}

impl KafkaJsonEntityKeySourceV1 {
    pub const fn kafka_record_key() -> Self {
        Self::KafkaRecordKey
    }

    pub fn utf8_projection(target_field: &str) -> Result<Self, KafkaJsonObjectMappingErrorV1> {
        validate_target_name(target_field)?;
        Ok(Self::Utf8Projection {
            target_field: copy_string(target_field)?,
        })
    }

    pub fn projection_target(&self) -> Option<&str> {
        match self {
            Self::KafkaRecordKey => None,
            Self::Utf8Projection { target_field } => Some(target_field),
        }
    }

    const fn tag(&self) -> u8 {
        match self {
            Self::KafkaRecordKey => 0,
            Self::Utf8Projection { .. } => 1,
        }
    }
}

impl fmt::Debug for KafkaJsonEntityKeySourceV1 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::KafkaRecordKey => formatter.write_str("KafkaRecordKey"),
            Self::Utf8Projection { target_field } => formatter
                .debug_struct("Utf8Projection")
                .field("target_field_bytes", &target_field.len())
                .finish(),
        }
    }
}

#[derive(Clone, Eq, PartialEq)]
pub enum KafkaJsonObjectMappingErrorV1 {
    UnsupportedVersion { actual: u16 },
    InvalidProjectionTarget { category: ValidationErrorV0 },
    UnknownProjectionTarget,
    ProjectionTargetNotUtf8,
    EncodingLengthOverflow,
    EncodingTooLarge { maximum: usize, actual: usize },
    AllocationFailed,
    DerivedIdentifier { category: ValidationErrorV0 },
}

impl fmt::Debug for KafkaJsonObjectMappingErrorV1 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::UnsupportedVersion { actual } => formatter
                .debug_struct("UnsupportedVersion")
                .field("actual", actual)
                .finish(),
            Self::InvalidProjectionTarget { category } => formatter
                .debug_struct("InvalidProjectionTarget")
                .field("category", category)
                .finish(),
            Self::UnknownProjectionTarget => formatter.write_str("UnknownProjectionTarget"),
            Self::ProjectionTargetNotUtf8 => formatter.write_str("ProjectionTargetNotUtf8"),
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

impl fmt::Display for KafkaJsonObjectMappingErrorV1 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(self, formatter)
    }
}

impl Error for KafkaJsonObjectMappingErrorV1 {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::InvalidProjectionTarget { category } | Self::DerivedIdentifier { category } => {
                Some(category)
            }
            _ => None,
        }
    }
}

#[derive(Clone, Eq, PartialEq)]
pub struct KafkaJsonObjectMappingV1 {
    base: KafkaJsonObjectMappingV0,
    entity_key_source: KafkaJsonEntityKeySourceV1,
    canonical_preimage: Vec<u8>,
    mapping_digest: [u8; DIGEST_BYTES],
    schema_id: SchemaIdV0,
    effective_source_id: SourceIdV0,
}

impl KafkaJsonObjectMappingV1 {
    pub fn new(
        version: u16,
        base: KafkaJsonObjectMappingV0,
        entity_key_source: KafkaJsonEntityKeySourceV1,
    ) -> Result<Self, KafkaJsonObjectMappingErrorV1> {
        if version != KAFKA_JSON_OBJECT_MAPPING_VERSION_V1 {
            return Err(KafkaJsonObjectMappingErrorV1::UnsupportedVersion { actual: version });
        }
        if let Some(target) = entity_key_source.projection_target() {
            let projection = base
                .projections()
                .iter()
                .find(|projection| projection.target_field() == target)
                .ok_or(KafkaJsonObjectMappingErrorV1::UnknownProjectionTarget)?;
            if projection.kind() != KafkaJsonScalarKindV0::Utf8 {
                return Err(KafkaJsonObjectMappingErrorV1::ProjectionTargetNotUtf8);
            }
        }

        let canonical_preimage = encode_mapping_preimage(&base, &entity_key_source)?;
        let mapping_digest = sha256(&canonical_preimage);
        let schema_id = SchemaIdV0::new(&lower_hex(&mapping_digest)?)
            .map_err(|category| KafkaJsonObjectMappingErrorV1::DerivedIdentifier { category })?;
        let effective_source_digest =
            effective_source_digest(base.effective_source_id(), &mapping_digest)?;
        let effective_source_id = SourceIdV0::new(&lower_hex(&effective_source_digest)?)
            .map_err(|category| KafkaJsonObjectMappingErrorV1::DerivedIdentifier { category })?;

        Ok(Self {
            base,
            entity_key_source,
            canonical_preimage,
            mapping_digest,
            schema_id,
            effective_source_id,
        })
    }

    pub const fn version(&self) -> u16 {
        KAFKA_JSON_OBJECT_MAPPING_VERSION_V1
    }

    pub fn base(&self) -> &KafkaJsonObjectMappingV0 {
        &self.base
    }

    pub fn entity_key_source(&self) -> &KafkaJsonEntityKeySourceV1 {
        &self.entity_key_source
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

impl fmt::Debug for KafkaJsonObjectMappingV1 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("KafkaJsonObjectMappingV1")
            .field("version", &self.version())
            .field("base", &self.base)
            .field("entity_key_source", &self.entity_key_source)
            .field("canonical_preimage_bytes", &self.canonical_preimage.len())
            .field("mapping_digest", &"<redacted>")
            .field("schema_id", &"<redacted>")
            .field("effective_source_id", &"<redacted>")
            .finish()
    }
}

fn validate_target_name(value: &str) -> Result<(), KafkaJsonObjectMappingErrorV1> {
    let bytes = value.as_bytes();
    if !(1..=MAX_FIELD_NAME_BYTES).contains(&bytes.len()) {
        return Err(KafkaJsonObjectMappingErrorV1::InvalidProjectionTarget {
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
        return Err(KafkaJsonObjectMappingErrorV1::InvalidProjectionTarget {
            category: ValidationErrorV0::InvalidFieldNameSyntax,
        });
    }
    Ok(())
}

fn encode_mapping_preimage(
    base: &KafkaJsonObjectMappingV0,
    entity_key_source: &KafkaJsonEntityKeySourceV1,
) -> Result<Vec<u8>, KafkaJsonObjectMappingErrorV1> {
    let target_len = entity_key_source.projection_target().map_or(0, str::len);
    let length = MAPPING_DOMAIN_V1
        .len()
        .checked_add(2 + DIGEST_BYTES + 1 + 2)
        .and_then(|length| length.checked_add(target_len))
        .ok_or(KafkaJsonObjectMappingErrorV1::EncodingLengthOverflow)?;
    if length > MAX_KAFKA_JSON_OBJECT_MAPPING_PREIMAGE_BYTES_V1 {
        return Err(KafkaJsonObjectMappingErrorV1::EncodingTooLarge {
            maximum: MAX_KAFKA_JSON_OBJECT_MAPPING_PREIMAGE_BYTES_V1,
            actual: length,
        });
    }
    let mut output = Vec::new();
    output
        .try_reserve_exact(length)
        .map_err(|_| KafkaJsonObjectMappingErrorV1::AllocationFailed)?;
    output.extend_from_slice(MAPPING_DOMAIN_V1);
    output.extend_from_slice(&KAFKA_JSON_OBJECT_MAPPING_VERSION_V1.to_be_bytes());
    output.extend_from_slice(base.mapping_digest());
    output.push(entity_key_source.tag());
    let target_len = u16::try_from(target_len)
        .map_err(|_| KafkaJsonObjectMappingErrorV1::EncodingLengthOverflow)?;
    output.extend_from_slice(&target_len.to_be_bytes());
    if let Some(target) = entity_key_source.projection_target() {
        output.extend_from_slice(target.as_bytes());
    }
    debug_assert_eq!(output.len(), length);
    Ok(output)
}

fn effective_source_digest(
    base_source_id: &SourceIdV0,
    mapping_digest: &[u8; DIGEST_BYTES],
) -> Result<[u8; DIGEST_BYTES], KafkaJsonObjectMappingErrorV1> {
    let source = base_source_id.as_str().as_bytes();
    let source_len = u16::try_from(source.len())
        .map_err(|_| KafkaJsonObjectMappingErrorV1::EncodingLengthOverflow)?;
    let mut hasher = Sha256::new();
    hasher.update(SOURCE_DOMAIN_V1);
    hasher.update(source_len.to_be_bytes());
    hasher.update(source);
    hasher.update(mapping_digest);
    let output = hasher.finalize();
    let mut digest = [0_u8; DIGEST_BYTES];
    digest.copy_from_slice(&output);
    Ok(digest)
}

fn sha256(bytes: &[u8]) -> [u8; DIGEST_BYTES] {
    let output = Sha256::digest(bytes);
    let mut digest = [0_u8; DIGEST_BYTES];
    digest.copy_from_slice(&output);
    digest
}

fn copy_string(value: &str) -> Result<String, KafkaJsonObjectMappingErrorV1> {
    let mut copy = String::new();
    copy.try_reserve_exact(value.len())
        .map_err(|_| KafkaJsonObjectMappingErrorV1::AllocationFailed)?;
    copy.push_str(value);
    Ok(copy)
}

fn lower_hex(bytes: &[u8; DIGEST_BYTES]) -> Result<String, KafkaJsonObjectMappingErrorV1> {
    const DIGITS: &[u8; 16] = b"0123456789abcdef";
    let mut output = String::new();
    output
        .try_reserve_exact(HEX_DIGEST_BYTES)
        .map_err(|_| KafkaJsonObjectMappingErrorV1::AllocationFailed)?;
    for byte in bytes {
        output.push(char::from(DIGITS[usize::from(byte >> 4)]));
        output.push(char::from(DIGITS[usize::from(byte & 0x0f)]));
    }
    Ok(output)
}
