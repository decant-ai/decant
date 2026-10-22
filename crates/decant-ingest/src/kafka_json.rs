//! End-to-end in-memory ingestion for one bounded Decant JSON Kafka record.

use std::error::Error;
use std::fmt;

use decant_ir::SourceIdV0;
use decant_kafka::{
    KafkaClusterIdV0, KafkaDecantJsonRecordMapErrorV0, KafkaFetchedRecordV0,
    KafkaJsonObjectMappingV0, KafkaJsonObjectMappingV1, KafkaJsonObjectRecordMapErrorV0,
    KafkaJsonObjectRecordMapErrorV1, KafkaPartitionFrontierV0, KafkaTopicIdV0,
    KafkaTransportMetadataOmissionV0, map_decant_json_kafka_record_v0,
    map_json_object_kafka_record_v0, map_json_object_kafka_record_v1,
};
use decant_state::AdmissionStateV0;

use crate::{
    KafkaCanonicalAdmissionErrorV0, KafkaCanonicalAdmissionOutcomeV0,
    admit_expected_kafka_canonical_event_v0,
};

pub const KAFKA_JSON_INGESTION_VERSION_V0: u16 = 0;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct KafkaJsonIngestionOutcomeV0 {
    admission: KafkaCanonicalAdmissionOutcomeV0,
    omitted_transport_metadata: KafkaTransportMetadataOmissionV0,
}

impl KafkaJsonIngestionOutcomeV0 {
    pub const fn admission(self) -> KafkaCanonicalAdmissionOutcomeV0 {
        self.admission
    }

    pub const fn omitted_transport_metadata(self) -> KafkaTransportMetadataOmissionV0 {
        self.omitted_transport_metadata
    }
}

#[derive(Clone, Eq, PartialEq)]
pub enum KafkaJsonIngestionErrorV0 {
    Mapping {
        category: KafkaDecantJsonRecordMapErrorV0,
    },
    Admission {
        category: KafkaCanonicalAdmissionErrorV0,
    },
}

impl fmt::Debug for KafkaJsonIngestionErrorV0 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Mapping { category } => formatter
                .debug_struct("Mapping")
                .field("category", category)
                .finish(),
            Self::Admission { category } => formatter
                .debug_struct("Admission")
                .field("category", category)
                .finish(),
        }
    }
}

impl fmt::Display for KafkaJsonIngestionErrorV0 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(self, formatter)
    }
}

impl Error for KafkaJsonIngestionErrorV0 {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Mapping { category } => Some(category),
            Self::Admission { category } => Some(category),
        }
    }
}

#[derive(Clone, Eq, PartialEq)]
pub enum KafkaJsonObjectIngestionErrorV0 {
    MappingBinding,
    Mapping {
        category: KafkaJsonObjectRecordMapErrorV0,
    },
    Admission {
        category: KafkaCanonicalAdmissionErrorV0,
    },
}

impl fmt::Debug for KafkaJsonObjectIngestionErrorV0 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::MappingBinding => formatter.write_str("MappingBinding"),
            Self::Mapping { category } => formatter
                .debug_struct("Mapping")
                .field("category", category)
                .finish(),
            Self::Admission { category } => formatter
                .debug_struct("Admission")
                .field("category", category)
                .finish(),
        }
    }
}

impl fmt::Display for KafkaJsonObjectIngestionErrorV0 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(self, formatter)
    }
}

impl Error for KafkaJsonObjectIngestionErrorV0 {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Mapping { category } => Some(category),
            Self::Admission { category } => Some(category),
            Self::MappingBinding => None,
        }
    }
}

#[derive(Clone, Eq, PartialEq)]
pub enum KafkaJsonObjectIngestionErrorV1 {
    MappingBinding,
    Mapping {
        category: KafkaJsonObjectRecordMapErrorV1,
    },
    Admission {
        category: KafkaCanonicalAdmissionErrorV0,
    },
}

impl fmt::Debug for KafkaJsonObjectIngestionErrorV1 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::MappingBinding => formatter.write_str("MappingBinding"),
            Self::Mapping { category } => formatter
                .debug_struct("Mapping")
                .field("category", category)
                .finish(),
            Self::Admission { category } => formatter
                .debug_struct("Admission")
                .field("category", category)
                .finish(),
        }
    }
}

impl fmt::Display for KafkaJsonObjectIngestionErrorV1 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(self, formatter)
    }
}

impl Error for KafkaJsonObjectIngestionErrorV1 {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Mapping { category } => Some(category),
            Self::Admission { category } => Some(category),
            Self::MappingBinding => None,
        }
    }
}

pub fn ingest_decant_json_kafka_record_v0(
    frontier: &mut KafkaPartitionFrontierV0,
    admission: &mut AdmissionStateV0,
    observed_source_id: &SourceIdV0,
    observed_cluster_id: &KafkaClusterIdV0,
    observed_topic_id: KafkaTopicIdV0,
    record: &KafkaFetchedRecordV0,
) -> Result<KafkaJsonIngestionOutcomeV0, KafkaJsonIngestionErrorV0> {
    let mapped = map_decant_json_kafka_record_v0(
        frontier.source_contract(),
        observed_source_id,
        observed_cluster_id,
        observed_topic_id,
        record,
    )
    .map_err(|category| KafkaJsonIngestionErrorV0::Mapping { category })?;
    let (event, omitted_transport_metadata) = mapped.into_parts();
    let admission = admit_expected_kafka_canonical_event_v0(
        frontier,
        admission,
        observed_source_id,
        observed_cluster_id,
        observed_topic_id,
        &event,
    )
    .map_err(|category| KafkaJsonIngestionErrorV0::Admission { category })?;
    Ok(KafkaJsonIngestionOutcomeV0 {
        admission,
        omitted_transport_metadata,
    })
}

pub fn ingest_json_object_kafka_record_v0(
    mapping: &KafkaJsonObjectMappingV0,
    frontier: &mut KafkaPartitionFrontierV0,
    admission: &mut AdmissionStateV0,
    observed_source_id: &SourceIdV0,
    observed_cluster_id: &KafkaClusterIdV0,
    observed_topic_id: KafkaTopicIdV0,
    record: &KafkaFetchedRecordV0,
) -> Result<KafkaJsonIngestionOutcomeV0, KafkaJsonObjectIngestionErrorV0> {
    if mapping.effective_source_id() != frontier.source_contract().source_id()
        || mapping.effective_source_id() != admission.config().source_id()
    {
        return Err(KafkaJsonObjectIngestionErrorV0::MappingBinding);
    }
    let mapped = map_json_object_kafka_record_v0(
        mapping,
        frontier.source_contract(),
        observed_source_id,
        observed_cluster_id,
        observed_topic_id,
        record,
    )
    .map_err(|category| KafkaJsonObjectIngestionErrorV0::Mapping { category })?;
    let (event, omitted_transport_metadata) = mapped.into_parts();
    let admission = admit_expected_kafka_canonical_event_v0(
        frontier,
        admission,
        observed_source_id,
        observed_cluster_id,
        observed_topic_id,
        &event,
    )
    .map_err(|category| KafkaJsonObjectIngestionErrorV0::Admission { category })?;
    Ok(KafkaJsonIngestionOutcomeV0 {
        admission,
        omitted_transport_metadata,
    })
}

pub fn ingest_json_object_kafka_record_v1(
    mapping: &KafkaJsonObjectMappingV1,
    frontier: &mut KafkaPartitionFrontierV0,
    admission: &mut AdmissionStateV0,
    observed_source_id: &SourceIdV0,
    observed_cluster_id: &KafkaClusterIdV0,
    observed_topic_id: KafkaTopicIdV0,
    record: &KafkaFetchedRecordV0,
) -> Result<KafkaJsonIngestionOutcomeV0, KafkaJsonObjectIngestionErrorV1> {
    if mapping.effective_source_id() != frontier.source_contract().source_id()
        || mapping.effective_source_id() != admission.config().source_id()
    {
        return Err(KafkaJsonObjectIngestionErrorV1::MappingBinding);
    }
    let mapped = map_json_object_kafka_record_v1(
        mapping,
        frontier.source_contract(),
        observed_source_id,
        observed_cluster_id,
        observed_topic_id,
        record,
    )
    .map_err(|category| KafkaJsonObjectIngestionErrorV1::Mapping { category })?;
    let (event, omitted_transport_metadata) = mapped.into_parts();
    let admission = admit_expected_kafka_canonical_event_v0(
        frontier,
        admission,
        observed_source_id,
        observed_cluster_id,
        observed_topic_id,
        &event,
    )
    .map_err(|category| KafkaJsonObjectIngestionErrorV1::Admission { category })?;
    Ok(KafkaJsonIngestionOutcomeV0 {
        admission,
        omitted_transport_metadata,
    })
}
