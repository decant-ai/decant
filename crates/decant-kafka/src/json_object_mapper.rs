//! Validated mapping from one flat JSON Kafka record to one canonical event.

use std::collections::BTreeMap;
use std::error::Error;
use std::fmt;

use decant_ir::{CanonicalPayloadV0, ScalarValueV0, ValidationErrorV0};
use serde::de::{IgnoredAny, MapAccess, Visitor};
use serde::{Deserialize, Deserializer};
use serde_json::Value;

use crate::{
    KafkaCanonicalEventAssemblyErrorV0, KafkaClusterIdV0, KafkaFetchedRecordV0,
    KafkaJsonEntityKeySourceV1, KafkaJsonObjectMappingV0, KafkaJsonObjectMappingV1,
    KafkaJsonScalarKindV0, KafkaMappedEventV0, KafkaSourceContractV0,
    KafkaSourceObservationErrorV0, KafkaTopicIdV0, KafkaTransportMetadataOmissionV0,
    MAX_KAFKA_RECORD_VALUE_BYTES_V0, assemble_validated_kafka_canonical_event_v0,
};

pub const KAFKA_JSON_OBJECT_RECORD_MAPPER_VERSION_V0: u16 = 0;
pub const KAFKA_JSON_OBJECT_RECORD_MAPPER_VERSION_V1: u16 = 1;
pub const MAX_KAFKA_JSON_OBJECT_MEMBERS_V0: usize = 256;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum KafkaJsonObjectValueRoleV0 {
    EventTime,
    Projection,
}

#[derive(Clone, Eq, PartialEq)]
pub enum KafkaJsonObjectRecordMapErrorV0 {
    SourceObservation {
        category: KafkaSourceObservationErrorV0,
    },
    TombstoneUnsupported,
    InputTooLarge {
        maximum: usize,
        actual: usize,
    },
    MalformedJsonObject,
    TooManyObjectMembers {
        maximum: usize,
    },
    DuplicateObjectMember,
    MissingValue {
        role: KafkaJsonObjectValueRoleV0,
    },
    InvalidValueType {
        role: KafkaJsonObjectValueRoleV0,
        expected: Option<KafkaJsonScalarKindV0>,
    },
    EventTimeOverflow,
    CanonicalPayload {
        category: ValidationErrorV0,
    },
    CanonicalAssembly {
        category: KafkaCanonicalEventAssemblyErrorV0,
    },
}

impl fmt::Debug for KafkaJsonObjectRecordMapErrorV0 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::SourceObservation { category } => formatter
                .debug_struct("SourceObservation")
                .field("category", category)
                .finish(),
            Self::TombstoneUnsupported => formatter.write_str("TombstoneUnsupported"),
            Self::InputTooLarge { maximum, actual } => formatter
                .debug_struct("InputTooLarge")
                .field("maximum", maximum)
                .field("actual", actual)
                .finish(),
            Self::MalformedJsonObject => formatter.write_str("MalformedJsonObject"),
            Self::TooManyObjectMembers { maximum } => formatter
                .debug_struct("TooManyObjectMembers")
                .field("maximum", maximum)
                .finish(),
            Self::DuplicateObjectMember => formatter.write_str("DuplicateObjectMember"),
            Self::MissingValue { role } => formatter
                .debug_struct("MissingValue")
                .field("role", role)
                .finish(),
            Self::InvalidValueType { role, expected } => formatter
                .debug_struct("InvalidValueType")
                .field("role", role)
                .field("expected", expected)
                .finish(),
            Self::EventTimeOverflow => formatter.write_str("EventTimeOverflow"),
            Self::CanonicalPayload { category } => formatter
                .debug_struct("CanonicalPayload")
                .field("category", category)
                .finish(),
            Self::CanonicalAssembly { category } => formatter
                .debug_struct("CanonicalAssembly")
                .field("category", category)
                .finish(),
        }
    }
}

impl fmt::Display for KafkaJsonObjectRecordMapErrorV0 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(self, formatter)
    }
}

impl Error for KafkaJsonObjectRecordMapErrorV0 {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::SourceObservation { category } => Some(category),
            Self::CanonicalPayload { category } => Some(category),
            Self::CanonicalAssembly { category } => Some(category),
            _ => None,
        }
    }
}

#[derive(Clone, Eq, PartialEq)]
pub enum KafkaJsonObjectRecordMapErrorV1 {
    SourceObservation {
        category: KafkaSourceObservationErrorV0,
    },
    ObjectMapping {
        category: KafkaJsonObjectRecordMapErrorV0,
    },
    MissingValidatedProjection,
    CanonicalAssembly {
        category: KafkaCanonicalEventAssemblyErrorV0,
    },
}

impl fmt::Debug for KafkaJsonObjectRecordMapErrorV1 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::SourceObservation { category } => formatter
                .debug_struct("SourceObservation")
                .field("category", category)
                .finish(),
            Self::ObjectMapping { category } => formatter
                .debug_struct("ObjectMapping")
                .field("category", category)
                .finish(),
            Self::MissingValidatedProjection => formatter.write_str("MissingValidatedProjection"),
            Self::CanonicalAssembly { category } => formatter
                .debug_struct("CanonicalAssembly")
                .field("category", category)
                .finish(),
        }
    }
}

impl fmt::Display for KafkaJsonObjectRecordMapErrorV1 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(self, formatter)
    }
}

impl Error for KafkaJsonObjectRecordMapErrorV1 {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::SourceObservation { category } => Some(category),
            Self::ObjectMapping { category } => Some(category),
            Self::MissingValidatedProjection => None,
            Self::CanonicalAssembly { category } => Some(category),
        }
    }
}

pub fn map_json_object_kafka_record_v0(
    mapping: &KafkaJsonObjectMappingV0,
    source_contract: &KafkaSourceContractV0,
    observed_source_id: &decant_ir::SourceIdV0,
    observed_cluster_id: &KafkaClusterIdV0,
    observed_topic_id: KafkaTopicIdV0,
    record: &KafkaFetchedRecordV0,
) -> Result<KafkaMappedEventV0, KafkaJsonObjectRecordMapErrorV0> {
    source_contract
        .validate_observed(
            mapping.effective_source_id(),
            observed_cluster_id,
            observed_topic_id,
        )
        .map_err(|category| KafkaJsonObjectRecordMapErrorV0::SourceObservation { category })?;
    source_contract
        .validate_observed(observed_source_id, observed_cluster_id, observed_topic_id)
        .map_err(|category| KafkaJsonObjectRecordMapErrorV0::SourceObservation { category })?;

    let (event_time_ns, payload) = decode_json_object(mapping, record)?;
    let event = assemble_validated_kafka_canonical_event_v0(
        source_contract,
        observed_source_id,
        observed_cluster_id,
        observed_topic_id,
        record.position(),
        mapping.schema_id().clone(),
        record.key(),
        event_time_ns,
        payload,
    )
    .map_err(|category| KafkaJsonObjectRecordMapErrorV0::CanonicalAssembly { category })?;

    Ok(KafkaMappedEventV0::from_parts(
        event,
        KafkaTransportMetadataOmissionV0::from_record(record),
    ))
}

pub fn map_json_object_kafka_record_v1(
    mapping: &KafkaJsonObjectMappingV1,
    source_contract: &KafkaSourceContractV0,
    observed_source_id: &decant_ir::SourceIdV0,
    observed_cluster_id: &KafkaClusterIdV0,
    observed_topic_id: KafkaTopicIdV0,
    record: &KafkaFetchedRecordV0,
) -> Result<KafkaMappedEventV0, KafkaJsonObjectRecordMapErrorV1> {
    source_contract
        .validate_observed(
            mapping.effective_source_id(),
            observed_cluster_id,
            observed_topic_id,
        )
        .map_err(|category| KafkaJsonObjectRecordMapErrorV1::SourceObservation { category })?;
    source_contract
        .validate_observed(observed_source_id, observed_cluster_id, observed_topic_id)
        .map_err(|category| KafkaJsonObjectRecordMapErrorV1::SourceObservation { category })?;

    let (event_time_ns, payload) = decode_json_object(mapping.base(), record)
        .map_err(|category| KafkaJsonObjectRecordMapErrorV1::ObjectMapping { category })?;
    let projected_key = match mapping.entity_key_source() {
        KafkaJsonEntityKeySourceV1::KafkaRecordKey => None,
        KafkaJsonEntityKeySourceV1::Utf8Projection { target_field } => Some(
            payload
                .fields()
                .find_map(|(name, value)| {
                    (name == target_field)
                        .then(|| value.as_utf8())
                        .flatten()
                        .map(str::as_bytes)
                })
                .ok_or(KafkaJsonObjectRecordMapErrorV1::MissingValidatedProjection)?
                .to_vec(),
        ),
    };
    let key = match mapping.entity_key_source() {
        KafkaJsonEntityKeySourceV1::KafkaRecordKey => record.key(),
        KafkaJsonEntityKeySourceV1::Utf8Projection { .. } => projected_key.as_deref(),
    };
    let event = assemble_validated_kafka_canonical_event_v0(
        source_contract,
        observed_source_id,
        observed_cluster_id,
        observed_topic_id,
        record.position(),
        mapping.schema_id().clone(),
        key,
        event_time_ns,
        payload,
    )
    .map_err(|category| KafkaJsonObjectRecordMapErrorV1::CanonicalAssembly { category })?;

    Ok(KafkaMappedEventV0::from_parts(
        event,
        KafkaTransportMetadataOmissionV0::from_record(record),
    ))
}

fn decode_json_object(
    mapping: &KafkaJsonObjectMappingV0,
    record: &KafkaFetchedRecordV0,
) -> Result<(i64, CanonicalPayloadV0), KafkaJsonObjectRecordMapErrorV0> {
    let value = record
        .value()
        .ok_or(KafkaJsonObjectRecordMapErrorV0::TombstoneUnsupported)?;
    if value.len() > MAX_KAFKA_RECORD_VALUE_BYTES_V0 {
        return Err(KafkaJsonObjectRecordMapErrorV0::InputTooLarge {
            maximum: MAX_KAFKA_RECORD_VALUE_BYTES_V0,
            actual: value.len(),
        });
    }
    let object: BoundedJsonObjectV0 = serde_json::from_slice(value)
        .map_err(|_| KafkaJsonObjectRecordMapErrorV0::MalformedJsonObject)?;
    if object.too_many_members {
        return Err(KafkaJsonObjectRecordMapErrorV0::TooManyObjectMembers {
            maximum: MAX_KAFKA_JSON_OBJECT_MEMBERS_V0,
        });
    }
    if object.duplicate_member {
        return Err(KafkaJsonObjectRecordMapErrorV0::DuplicateObjectMember);
    }

    let event_time_value = object
        .members
        .get(mapping.event_time_source_field())
        .ok_or(KafkaJsonObjectRecordMapErrorV0::MissingValue {
            role: KafkaJsonObjectValueRoleV0::EventTime,
        })?;
    let event_time = json_integer_i64(event_time_value).ok_or(
        KafkaJsonObjectRecordMapErrorV0::InvalidValueType {
            role: KafkaJsonObjectValueRoleV0::EventTime,
            expected: None,
        },
    )?;
    let event_time_ns = event_time
        .checked_mul(mapping.event_time_unit().multiplier_ns())
        .ok_or(KafkaJsonObjectRecordMapErrorV0::EventTimeOverflow)?;

    let mut fields = BTreeMap::new();
    for projection in mapping.projections() {
        let value = object.members.get(projection.source_field()).ok_or(
            KafkaJsonObjectRecordMapErrorV0::MissingValue {
                role: KafkaJsonObjectValueRoleV0::Projection,
            },
        )?;
        let scalar = scalar_from_json(value, projection.kind())
            .map_err(|category| KafkaJsonObjectRecordMapErrorV0::CanonicalPayload { category })?
            .ok_or(KafkaJsonObjectRecordMapErrorV0::InvalidValueType {
                role: KafkaJsonObjectValueRoleV0::Projection,
                expected: Some(projection.kind()),
            })?;
        fields.insert(projection.target_field().to_owned(), scalar);
    }
    let payload = CanonicalPayloadV0::try_from_fields(fields)
        .map_err(|category| KafkaJsonObjectRecordMapErrorV0::CanonicalPayload { category })?;
    Ok((event_time_ns, payload))
}

fn json_integer_i64(value: &Value) -> Option<i64> {
    value
        .as_i64()
        .or_else(|| value.as_u64().and_then(|value| i64::try_from(value).ok()))
}

fn scalar_from_json(
    value: &Value,
    kind: KafkaJsonScalarKindV0,
) -> Result<Option<ScalarValueV0>, ValidationErrorV0> {
    match kind {
        KafkaJsonScalarKindV0::Null if value.is_null() => Ok(Some(ScalarValueV0::null())),
        KafkaJsonScalarKindV0::Bool => Ok(value.as_bool().map(ScalarValueV0::from_bool)),
        KafkaJsonScalarKindV0::I64 => Ok(value.as_i64().map(ScalarValueV0::from_i64)),
        KafkaJsonScalarKindV0::U64 => Ok(value.as_u64().map(ScalarValueV0::from_u64)),
        KafkaJsonScalarKindV0::Utf8 => value.as_str().map(ScalarValueV0::utf8).transpose(),
        _ => Ok(None),
    }
}

struct BoundedJsonObjectV0 {
    members: BTreeMap<String, Value>,
    duplicate_member: bool,
    too_many_members: bool,
}

impl<'de> Deserialize<'de> for BoundedJsonObjectV0 {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_map(BoundedJsonObjectVisitorV0)
    }
}

struct BoundedJsonObjectVisitorV0;

impl<'de> Visitor<'de> for BoundedJsonObjectVisitorV0 {
    type Value = BoundedJsonObjectV0;

    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("a bounded JSON object")
    }

    fn visit_map<A>(self, mut map: A) -> Result<Self::Value, A::Error>
    where
        A: MapAccess<'de>,
    {
        let mut members = BTreeMap::new();
        let mut duplicate_member = false;
        let mut too_many_members = false;
        while let Some(key) = map.next_key::<String>()? {
            if members.contains_key(&key) {
                map.next_value::<IgnoredAny>()?;
                duplicate_member = true;
            } else if members.len() >= MAX_KAFKA_JSON_OBJECT_MEMBERS_V0 {
                map.next_value::<IgnoredAny>()?;
                too_many_members = true;
            } else {
                let value = map.next_value::<Value>()?;
                members.insert(key, value);
            }
        }
        Ok(BoundedJsonObjectV0 {
            members,
            duplicate_member,
            too_many_members,
        })
    }
}
