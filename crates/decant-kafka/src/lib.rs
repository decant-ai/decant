//! Bounded Kafka source-incarnation contracts for Decant.
//!
//! This private package performs no broker or network operations.
//!
//! ```compile_fail
//! use decant_kafka::KafkaClusterIdV0;
//! let _ = KafkaClusterIdV0(String::from("cluster-id"));
//! ```
//!
//! ```compile_fail
//! use decant_kafka::KafkaTopicIdV0;
//! let _ = KafkaTopicIdV0([1; 16]);
//! ```
//!
//! ```compile_fail
//! use decant_ir::SourceIdV0;
//! use decant_kafka::{KafkaClusterIdV0, KafkaSourceContractV0, KafkaTopicIdV0};
//! let _ = KafkaSourceContractV0 {
//!     version: 0,
//!     source_id: SourceIdV0::new("source-v0").unwrap(),
//!     cluster_id: KafkaClusterIdV0::new("cluster-v0").unwrap(),
//!     topic_id: KafkaTopicIdV0::from_bytes([1; 16]).unwrap(),
//! };
//! ```
//!
//! ```compile_fail
//! use std::collections::BTreeMap;
//! use decant_ir::SourceIdV0;
//! use decant_kafka::{
//!     KafkaClusterIdV0, KafkaPartitionFrontierV0, KafkaSourceContractV0, KafkaTopicIdV0,
//! };
//! let source_contract = KafkaSourceContractV0::new(
//!     0,
//!     SourceIdV0::new("source-v0").unwrap(),
//!     KafkaClusterIdV0::new("cluster-v0").unwrap(),
//!     KafkaTopicIdV0::from_bytes([1; 16]).unwrap(),
//! ).unwrap();
//! let _ = KafkaPartitionFrontierV0 {
//!     version: 0,
//!     source_contract,
//!     next_offsets: BTreeMap::new(),
//! };
//! ```

#![forbid(unsafe_code)]

mod frontier_hash;
mod frontier_snapshot;
mod json_object_mapper;
mod json_object_mapping;
mod json_object_mapping_v1;
mod json_value;
mod mapper;
mod offset_commit_plan;
mod record;

use std::collections::BTreeMap;
use std::error::Error;
use std::fmt;

use decant_ir::{
    CanonicalPayloadV0, ENVELOPE_VERSION_V0, EventEnvelopeV0, KafkaPositionV0, MAX_KAFKA_OFFSET_V0,
    SchemaIdV0, SensitivityBitmapV0, SourceIdV0, SourcePositionV0, ValidationErrorV0,
};

pub use frontier_hash::{
    KAFKA_FRONTIER_HASH_ALGORITHM_V0, KAFKA_FRONTIER_HASH_DOMAIN_V0,
    KAFKA_FRONTIER_HASH_TEXT_BYTES_V0, KAFKA_FRONTIER_HASH_VERSION_V0, KafkaFrontierHashErrorV0,
    KafkaFrontierHashV0, hash_kafka_partition_frontier_v0,
};
pub use frontier_snapshot::{
    KAFKA_FRONTIER_SNAPSHOT_VERSION_V0, KafkaFrontierSnapshotDecodeErrorV0,
    KafkaFrontierSnapshotEncodeErrorV0, KafkaFrontierSnapshotExpectedContractFieldV0,
    KafkaFrontierSnapshotFieldV0, MAX_KAFKA_FRONTIER_SNAPSHOT_BYTES_V0,
    encode_kafka_partition_frontier_snapshot_v0, restore_kafka_partition_frontier_snapshot_v0,
};
pub use json_object_mapper::{
    KAFKA_JSON_OBJECT_RECORD_MAPPER_VERSION_V0, KAFKA_JSON_OBJECT_RECORD_MAPPER_VERSION_V1,
    KafkaJsonObjectRecordMapErrorV0, KafkaJsonObjectRecordMapErrorV1, KafkaJsonObjectValueRoleV0,
    MAX_KAFKA_JSON_OBJECT_MEMBERS_V0, map_json_object_kafka_record_v0,
    map_json_object_kafka_record_v1,
};
pub use json_object_mapping::{
    KAFKA_JSON_OBJECT_MAPPING_FORMAT_V0, KAFKA_JSON_OBJECT_MAPPING_VERSION_V0,
    KafkaJsonEventTimeUnitV0, KafkaJsonObjectMappingErrorV0, KafkaJsonObjectMappingNameV0,
    KafkaJsonObjectMappingV0, KafkaJsonObjectProjectionV0, KafkaJsonScalarKindV0,
    MAX_KAFKA_JSON_OBJECT_MAPPING_PREIMAGE_BYTES_V0, MAX_KAFKA_JSON_OBJECT_PROJECTIONS_V0,
    MAX_KAFKA_JSON_OBJECT_SOURCE_FIELD_BYTES_V0,
};
pub use json_object_mapping_v1::{
    KAFKA_JSON_OBJECT_MAPPING_VERSION_V1, KafkaJsonEntityKeySourceV1,
    KafkaJsonObjectMappingErrorV1, KafkaJsonObjectMappingV1,
    MAX_KAFKA_JSON_OBJECT_MAPPING_PREIMAGE_BYTES_V1,
};
pub use json_value::{
    DECANT_KAFKA_EVENT_FORMAT_V0, DECANT_KAFKA_EVENT_VALUE_VERSION_V0, DecodedKafkaEventV0,
    KafkaEventValueDecodeErrorV0, KafkaJsonBinaryFieldV0, MAX_KAFKA_JSON_VALUE_BYTES_V0,
    decode_decant_kafka_event_value_v0,
};
pub use mapper::{
    KAFKA_DECANT_JSON_RECORD_MAPPER_VERSION_V0, KafkaDecantJsonRecordMapErrorV0,
    KafkaMappedEventV0, KafkaTransportMetadataOmissionV0, map_decant_json_kafka_record_v0,
};
pub use offset_commit_plan::{
    KAFKA_OFFSET_COMMIT_PLAN_VERSION_V0, KafkaOffsetCommitPlanErrorV0, KafkaOffsetCommitPlanV0,
    build_kafka_offset_commit_plan_v0,
};
pub use record::{
    KAFKA_FETCHED_RECORD_VERSION_V0, KafkaFetchedRecordV0, KafkaRecordBoundV0, KafkaRecordHeaderV0,
    KafkaRecordTimestampKindV0, KafkaRecordTimestampV0, KafkaRecordValidationErrorV0,
    MAX_KAFKA_FETCHED_RECORD_BYTES_V0, MAX_KAFKA_RECORD_HEADER_BYTES_V0,
    MAX_KAFKA_RECORD_HEADER_KEY_BYTES_V0, MAX_KAFKA_RECORD_HEADER_VALUE_BYTES_V0,
    MAX_KAFKA_RECORD_HEADERS_V0, MAX_KAFKA_RECORD_KEY_BYTES_V0, MAX_KAFKA_RECORD_VALUE_BYTES_V0,
};

pub const KAFKA_SOURCE_CONTRACT_VERSION_V0: u16 = 0;
pub const KAFKA_PARTITION_FRONTIER_VERSION_V0: u16 = 0;
pub const KAFKA_CANONICAL_EVENT_ASSEMBLER_VERSION_V0: u16 = 0;
pub const UNCLASSIFIED_KAFKA_SENSITIVITY_BITS_V0: u64 = u64::MAX;
pub const MAX_KAFKA_CLUSTER_ID_BYTES_V0: usize = 64;
pub const MAX_KAFKA_ASSIGNED_PARTITIONS_V0: usize = 1_024;
pub const KAFKA_TOPIC_ID_BYTES_V0: usize = 16;

const MIN_KAFKA_CLUSTER_ID_BYTES_V0: usize = 1;

#[derive(Clone, Eq, PartialEq)]
pub enum KafkaSourceContractErrorV0 {
    UnsupportedVersion {
        actual: u16,
    },
    InvalidClusterIdLength {
        minimum: usize,
        maximum: usize,
        actual: usize,
    },
    InvalidClusterIdSyntax,
    UnknownTopicId,
}

impl fmt::Debug for KafkaSourceContractErrorV0 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::UnsupportedVersion { actual } => formatter
                .debug_struct("UnsupportedVersion")
                .field("actual", actual)
                .finish(),
            Self::InvalidClusterIdLength {
                minimum,
                maximum,
                actual,
            } => formatter
                .debug_struct("InvalidClusterIdLength")
                .field("minimum", minimum)
                .field("maximum", maximum)
                .field("actual", actual)
                .finish(),
            Self::InvalidClusterIdSyntax => formatter.write_str("InvalidClusterIdSyntax"),
            Self::UnknownTopicId => formatter.write_str("UnknownTopicId"),
        }
    }
}

impl fmt::Display for KafkaSourceContractErrorV0 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(self, formatter)
    }
}

impl Error for KafkaSourceContractErrorV0 {}

#[derive(Clone, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct KafkaClusterIdV0(String);

impl KafkaClusterIdV0 {
    pub fn new(value: &str) -> Result<Self, KafkaSourceContractErrorV0> {
        if !(MIN_KAFKA_CLUSTER_ID_BYTES_V0..=MAX_KAFKA_CLUSTER_ID_BYTES_V0).contains(&value.len()) {
            return Err(KafkaSourceContractErrorV0::InvalidClusterIdLength {
                minimum: MIN_KAFKA_CLUSTER_ID_BYTES_V0,
                maximum: MAX_KAFKA_CLUSTER_ID_BYTES_V0,
                actual: value.len(),
            });
        }
        if !value.bytes().all(|byte| byte.is_ascii_graphic()) {
            return Err(KafkaSourceContractErrorV0::InvalidClusterIdSyntax);
        }
        Ok(Self(value.to_owned()))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Debug for KafkaClusterIdV0 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("KafkaClusterIdV0")
            .field("byte_len", &self.0.len())
            .finish()
    }
}

#[derive(Clone, Copy, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct KafkaTopicIdV0([u8; KAFKA_TOPIC_ID_BYTES_V0]);

impl KafkaTopicIdV0 {
    pub fn from_bytes(
        bytes: [u8; KAFKA_TOPIC_ID_BYTES_V0],
    ) -> Result<Self, KafkaSourceContractErrorV0> {
        if bytes == [0; KAFKA_TOPIC_ID_BYTES_V0] {
            return Err(KafkaSourceContractErrorV0::UnknownTopicId);
        }
        Ok(Self(bytes))
    }

    pub const fn as_bytes(&self) -> &[u8; KAFKA_TOPIC_ID_BYTES_V0] {
        &self.0
    }
}

impl fmt::Debug for KafkaTopicIdV0 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("KafkaTopicIdV0(<redacted>)")
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum KafkaSourceObservationErrorV0 {
    SourceMismatch,
    ClusterMismatch,
    TopicIncarnationMismatch,
}

impl fmt::Display for KafkaSourceObservationErrorV0 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(self, formatter)
    }
}

impl Error for KafkaSourceObservationErrorV0 {}

#[derive(Clone, Eq, PartialEq)]
pub enum KafkaCanonicalEventAssemblyErrorV0 {
    SourceObservation {
        category: KafkaSourceObservationErrorV0,
    },
    CanonicalEventValidation {
        category: ValidationErrorV0,
    },
}

impl fmt::Debug for KafkaCanonicalEventAssemblyErrorV0 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::SourceObservation { category } => formatter
                .debug_struct("SourceObservation")
                .field("category", category)
                .finish(),
            Self::CanonicalEventValidation { category } => formatter
                .debug_struct("CanonicalEventValidation")
                .field("category", category)
                .finish(),
        }
    }
}

impl fmt::Display for KafkaCanonicalEventAssemblyErrorV0 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(self, formatter)
    }
}

impl Error for KafkaCanonicalEventAssemblyErrorV0 {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::SourceObservation { category } => Some(category),
            Self::CanonicalEventValidation { category } => Some(category),
        }
    }
}

#[derive(Clone, Eq, PartialEq)]
pub struct KafkaSourceContractV0 {
    version: u16,
    source_id: SourceIdV0,
    cluster_id: KafkaClusterIdV0,
    topic_id: KafkaTopicIdV0,
}

impl KafkaSourceContractV0 {
    pub fn new(
        version: u16,
        source_id: SourceIdV0,
        cluster_id: KafkaClusterIdV0,
        topic_id: KafkaTopicIdV0,
    ) -> Result<Self, KafkaSourceContractErrorV0> {
        if version != KAFKA_SOURCE_CONTRACT_VERSION_V0 {
            return Err(KafkaSourceContractErrorV0::UnsupportedVersion { actual: version });
        }
        Ok(Self {
            version,
            source_id,
            cluster_id,
            topic_id,
        })
    }

    pub const fn version(&self) -> u16 {
        self.version
    }

    pub fn source_id(&self) -> &SourceIdV0 {
        &self.source_id
    }

    pub fn cluster_id(&self) -> &KafkaClusterIdV0 {
        &self.cluster_id
    }

    pub const fn topic_id(&self) -> KafkaTopicIdV0 {
        self.topic_id
    }

    pub fn validate_observed(
        &self,
        source_id: &SourceIdV0,
        cluster_id: &KafkaClusterIdV0,
        topic_id: KafkaTopicIdV0,
    ) -> Result<(), KafkaSourceObservationErrorV0> {
        if source_id != &self.source_id {
            return Err(KafkaSourceObservationErrorV0::SourceMismatch);
        }
        if cluster_id != &self.cluster_id {
            return Err(KafkaSourceObservationErrorV0::ClusterMismatch);
        }
        if topic_id != self.topic_id {
            return Err(KafkaSourceObservationErrorV0::TopicIncarnationMismatch);
        }
        Ok(())
    }
}

impl fmt::Debug for KafkaSourceContractV0 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("KafkaSourceContractV0")
            .field("version", &self.version)
            .field("source_id", &self.source_id)
            .field("cluster_id", &self.cluster_id)
            .field("topic_id", &self.topic_id)
            .finish()
    }
}

#[allow(
    clippy::too_many_arguments,
    reason = "source observation and caller-resolved canonical event semantics remain explicit"
)]
pub fn assemble_validated_kafka_canonical_event_v0(
    source_contract: &KafkaSourceContractV0,
    observed_source_id: &SourceIdV0,
    observed_cluster_id: &KafkaClusterIdV0,
    observed_topic_id: KafkaTopicIdV0,
    position: KafkaPositionV0,
    schema_id: SchemaIdV0,
    key: Option<&[u8]>,
    event_time_ns: i64,
    payload: CanonicalPayloadV0,
) -> Result<EventEnvelopeV0, KafkaCanonicalEventAssemblyErrorV0> {
    source_contract
        .validate_observed(observed_source_id, observed_cluster_id, observed_topic_id)
        .map_err(|category| KafkaCanonicalEventAssemblyErrorV0::SourceObservation { category })?;

    EventEnvelopeV0::new(
        ENVELOPE_VERSION_V0,
        source_contract.source_id().clone(),
        schema_id,
        SourcePositionV0::Kafka { position },
        key,
        event_time_ns,
        0,
        SensitivityBitmapV0::from_bits(UNCLASSIFIED_KAFKA_SENSITIVITY_BITS_V0),
        payload,
    )
    .map_err(|category| KafkaCanonicalEventAssemblyErrorV0::CanonicalEventValidation { category })
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum KafkaOffsetObservationV0 {
    Advanced,
    BeforeFrontier,
    Gap,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum KafkaOffsetClassificationV0 {
    Expected,
    BeforeFrontier,
    Gap,
}

impl fmt::Display for KafkaOffsetObservationV0 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(self, formatter)
    }
}

#[derive(Clone, Eq, PartialEq)]
pub enum KafkaPartitionFrontierErrorV0 {
    UnsupportedVersion {
        actual: u16,
    },
    EmptyAssignment,
    TooManyPartitions {
        maximum: usize,
        actual: usize,
    },
    DuplicatePartition,
    SourceObservation {
        category: KafkaSourceObservationErrorV0,
    },
    UnknownPartition,
}

impl fmt::Debug for KafkaPartitionFrontierErrorV0 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::UnsupportedVersion { actual } => formatter
                .debug_struct("UnsupportedVersion")
                .field("actual", actual)
                .finish(),
            Self::EmptyAssignment => formatter.write_str("EmptyAssignment"),
            Self::TooManyPartitions { maximum, actual } => formatter
                .debug_struct("TooManyPartitions")
                .field("maximum", maximum)
                .field("actual", actual)
                .finish(),
            Self::DuplicatePartition => formatter.write_str("DuplicatePartition"),
            Self::SourceObservation { category } => formatter
                .debug_struct("SourceObservation")
                .field("category", category)
                .finish(),
            Self::UnknownPartition => formatter.write_str("UnknownPartition"),
        }
    }
}

impl fmt::Display for KafkaPartitionFrontierErrorV0 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(self, formatter)
    }
}

impl Error for KafkaPartitionFrontierErrorV0 {}

#[derive(Clone, Eq, PartialEq)]
pub struct KafkaPartitionFrontierV0 {
    version: u16,
    source_contract: KafkaSourceContractV0,
    next_offsets: BTreeMap<i32, i64>,
}

impl KafkaPartitionFrontierV0 {
    pub fn new(
        version: u16,
        source_contract: KafkaSourceContractV0,
        starting_positions: &[KafkaPositionV0],
    ) -> Result<Self, KafkaPartitionFrontierErrorV0> {
        if version != KAFKA_PARTITION_FRONTIER_VERSION_V0 {
            return Err(KafkaPartitionFrontierErrorV0::UnsupportedVersion { actual: version });
        }
        if starting_positions.is_empty() {
            return Err(KafkaPartitionFrontierErrorV0::EmptyAssignment);
        }
        if starting_positions.len() > MAX_KAFKA_ASSIGNED_PARTITIONS_V0 {
            return Err(KafkaPartitionFrontierErrorV0::TooManyPartitions {
                maximum: MAX_KAFKA_ASSIGNED_PARTITIONS_V0,
                actual: starting_positions.len(),
            });
        }

        let mut next_offsets = BTreeMap::new();
        for position in starting_positions {
            if next_offsets
                .insert(position.partition(), position.offset())
                .is_some()
            {
                return Err(KafkaPartitionFrontierErrorV0::DuplicatePartition);
            }
        }

        Ok(Self {
            version,
            source_contract,
            next_offsets,
        })
    }

    pub const fn version(&self) -> u16 {
        self.version
    }

    pub const fn source_contract(&self) -> &KafkaSourceContractV0 {
        &self.source_contract
    }

    pub fn assignment_count(&self) -> usize {
        self.next_offsets.len()
    }

    pub fn next_offset(&self, partition: i32) -> Option<i64> {
        self.next_offsets.get(&partition).copied()
    }

    pub fn iter(&self) -> impl DoubleEndedIterator<Item = (i32, i64)> + ExactSizeIterator + '_ {
        self.next_offsets
            .iter()
            .map(|(partition, next_offset)| (*partition, *next_offset))
    }

    pub fn observe(
        &mut self,
        source_id: &SourceIdV0,
        cluster_id: &KafkaClusterIdV0,
        topic_id: KafkaTopicIdV0,
        position: KafkaPositionV0,
    ) -> Result<KafkaOffsetObservationV0, KafkaPartitionFrontierErrorV0> {
        match self.classify(source_id, cluster_id, topic_id, position)? {
            KafkaOffsetClassificationV0::Expected => {
                self.advance_classified_expected(position);
                Ok(KafkaOffsetObservationV0::Advanced)
            }
            KafkaOffsetClassificationV0::BeforeFrontier => {
                Ok(KafkaOffsetObservationV0::BeforeFrontier)
            }
            KafkaOffsetClassificationV0::Gap => Ok(KafkaOffsetObservationV0::Gap),
        }
    }

    pub fn classify(
        &self,
        source_id: &SourceIdV0,
        cluster_id: &KafkaClusterIdV0,
        topic_id: KafkaTopicIdV0,
        position: KafkaPositionV0,
    ) -> Result<KafkaOffsetClassificationV0, KafkaPartitionFrontierErrorV0> {
        self.source_contract
            .validate_observed(source_id, cluster_id, topic_id)
            .map_err(|category| KafkaPartitionFrontierErrorV0::SourceObservation { category })?;

        let Some(next_offset) = self.next_offsets.get(&position.partition()) else {
            return Err(KafkaPartitionFrontierErrorV0::UnknownPartition);
        };

        if position.offset() < *next_offset {
            return Ok(KafkaOffsetClassificationV0::BeforeFrontier);
        }
        if position.offset() > *next_offset {
            return Ok(KafkaOffsetClassificationV0::Gap);
        }

        Ok(KafkaOffsetClassificationV0::Expected)
    }

    pub(crate) fn advance_classified_expected(&mut self, position: KafkaPositionV0) {
        let next_offset = self
            .next_offsets
            .get_mut(&position.partition())
            .expect("position was classified against this frontier");
        assert!(
            *next_offset == position.offset(),
            "only an expected position may advance the frontier"
        );
        debug_assert!(*next_offset <= MAX_KAFKA_OFFSET_V0);
        *next_offset += 1;
    }

    pub(crate) fn from_restored_next_offsets(
        version: u16,
        source_contract: KafkaSourceContractV0,
        entries: Vec<(i32, i64)>,
    ) -> Self {
        Self {
            version,
            source_contract,
            next_offsets: entries.into_iter().collect(),
        }
    }
}

impl fmt::Debug for KafkaPartitionFrontierV0 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("KafkaPartitionFrontierV0")
            .field("version", &self.version)
            .field("assignment_count", &self.next_offsets.len())
            .finish()
    }
}
