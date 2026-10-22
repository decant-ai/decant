//! Private single-process composition of adapter progress and semantic state.

#![forbid(unsafe_code)]

mod checkpoint;
mod checkpoint_generation;
mod checkpoint_hash;
mod ingestion_replay;
mod kafka_json;
mod replay_journal;

use std::error::Error;
use std::fmt;

use decant_ir::{EventEnvelopeV0, SourceIdV0, SourcePositionKindV0};
use decant_kafka::{
    KafkaClusterIdV0, KafkaOffsetClassificationV0, KafkaOffsetObservationV0,
    KafkaPartitionFrontierErrorV0, KafkaPartitionFrontierV0, KafkaTopicIdV0,
};
use decant_state::{AdmissionErrorV0, AdmissionStateV0, ApplyOutcomeV0};

pub use checkpoint::{
    KAFKA_INGESTION_CHECKPOINT_VERSION_V0, KafkaIngestionCheckpointDecodeErrorV0,
    KafkaIngestionCheckpointEncodeErrorV0, KafkaIngestionCheckpointFieldV0,
    MAX_KAFKA_INGESTION_CHECKPOINT_BYTES_V0, encode_kafka_ingestion_checkpoint_v0,
    restore_kafka_ingestion_checkpoint_v0,
    validate_kafka_ingestion_checkpoint_expected_contract_v0,
};
pub use checkpoint_generation::{
    KAFKA_CHECKPOINT_GENERATION_ARTIFACT_VERSION_V0,
    KafkaCheckpointGenerationArtifactDecodeErrorV0, KafkaCheckpointGenerationArtifactEncodeErrorV0,
    KafkaCheckpointGenerationArtifactFieldV0, KafkaCheckpointGenerationErrorV0,
    KafkaCheckpointGenerationRestoreV0, KafkaCheckpointGenerationV0,
    MAX_KAFKA_CHECKPOINT_GENERATION_ARTIFACT_BYTES_V0,
    encode_kafka_checkpoint_generation_artifact_v0,
    restore_kafka_checkpoint_generation_artifact_v0,
};
pub use checkpoint_hash::{
    KAFKA_INGESTION_CHECKPOINT_HASH_ALGORITHM_V0, KAFKA_INGESTION_CHECKPOINT_HASH_DOMAIN_V0,
    KAFKA_INGESTION_CHECKPOINT_HASH_TEXT_BYTES_V0, KAFKA_INGESTION_CHECKPOINT_HASH_VERSION_V0,
    KafkaIngestionCheckpointHashErrorV0, KafkaIngestionCheckpointHashV0,
    hash_kafka_ingestion_checkpoint_v0,
};
pub use ingestion_replay::{
    KafkaJsonIngestionReplayErrorV0, KafkaJsonIngestionReplayOutcomeV0,
    KafkaJsonIngestionReplayReportV0, replay_kafka_json_ingestion_journal_v0,
    replay_kafka_json_ingestion_journal_with_interruption_v0,
};
pub use kafka_json::{
    KAFKA_JSON_INGESTION_VERSION_V0, KafkaJsonIngestionErrorV0, KafkaJsonIngestionOutcomeV0,
    KafkaJsonObjectIngestionErrorV0, KafkaJsonObjectIngestionErrorV1,
    ingest_decant_json_kafka_record_v0, ingest_json_object_kafka_record_v0,
    ingest_json_object_kafka_record_v1,
};
pub use replay_journal::{
    KAFKA_JSON_INGESTION_REPLAY_JOURNAL_VERSION_V0, KafkaJsonIngestionReplayAccountingFieldV0,
    KafkaJsonIngestionReplayAppendErrorV0, KafkaJsonIngestionReplayConfigErrorV0,
    KafkaJsonIngestionReplayInputV0, KafkaJsonIngestionReplayJournalV0,
    KafkaJsonIngestionReplayLimitFieldV0, KafkaJsonIngestionReplayLimitsV0,
    KafkaJsonIngestionReplayUsageV0, MAX_KAFKA_JSON_INGESTION_REPLAY_BYTES_V0,
    MAX_KAFKA_JSON_INGESTION_REPLAY_INPUTS_V0,
    kafka_json_ingestion_replay_input_accounted_bytes_v0,
};

pub const KAFKA_CANONICAL_ADMISSION_VERSION_V0: u16 = 0;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum KafkaCanonicalAdmissionOutcomeV0 {
    Advanced { admission: ApplyOutcomeV0 },
    BeforeFrontier,
    Gap,
}

#[derive(Clone, Eq, PartialEq)]
pub enum KafkaCanonicalAdmissionErrorV0 {
    EventSourceMismatch,
    UnsupportedEventPosition {
        actual: SourcePositionKindV0,
    },
    Frontier {
        category: KafkaPartitionFrontierErrorV0,
    },
    Admission {
        category: AdmissionErrorV0,
    },
    FrontierTransitionInvariant,
}

impl fmt::Debug for KafkaCanonicalAdmissionErrorV0 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::EventSourceMismatch => formatter.write_str("EventSourceMismatch"),
            Self::UnsupportedEventPosition { actual } => formatter
                .debug_struct("UnsupportedEventPosition")
                .field("actual", actual)
                .finish(),
            Self::Frontier { category } => formatter
                .debug_struct("Frontier")
                .field("category", category)
                .finish(),
            Self::Admission { category } => formatter
                .debug_struct("Admission")
                .field("category", category)
                .finish(),
            Self::FrontierTransitionInvariant => formatter.write_str("FrontierTransitionInvariant"),
        }
    }
}

impl fmt::Display for KafkaCanonicalAdmissionErrorV0 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(self, formatter)
    }
}

impl Error for KafkaCanonicalAdmissionErrorV0 {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Frontier { category } => Some(category),
            Self::Admission { category } => Some(category),
            Self::EventSourceMismatch
            | Self::UnsupportedEventPosition { .. }
            | Self::FrontierTransitionInvariant => None,
        }
    }
}

pub fn admit_expected_kafka_canonical_event_v0(
    frontier: &mut KafkaPartitionFrontierV0,
    admission: &mut AdmissionStateV0,
    observed_source_id: &SourceIdV0,
    observed_cluster_id: &KafkaClusterIdV0,
    observed_topic_id: KafkaTopicIdV0,
    event: &EventEnvelopeV0,
) -> Result<KafkaCanonicalAdmissionOutcomeV0, KafkaCanonicalAdmissionErrorV0> {
    frontier
        .source_contract()
        .validate_observed(observed_source_id, observed_cluster_id, observed_topic_id)
        .map_err(|category| KafkaCanonicalAdmissionErrorV0::Frontier {
            category: KafkaPartitionFrontierErrorV0::SourceObservation { category },
        })?;

    if event.source_id() != frontier.source_contract().source_id() {
        return Err(KafkaCanonicalAdmissionErrorV0::EventSourceMismatch);
    }
    let position = event.source_position().kafka_position().ok_or(
        KafkaCanonicalAdmissionErrorV0::UnsupportedEventPosition {
            actual: event.source_position().kind(),
        },
    )?;

    match frontier
        .classify(
            observed_source_id,
            observed_cluster_id,
            observed_topic_id,
            position,
        )
        .map_err(|category| KafkaCanonicalAdmissionErrorV0::Frontier { category })?
    {
        KafkaOffsetClassificationV0::BeforeFrontier => {
            Ok(KafkaCanonicalAdmissionOutcomeV0::BeforeFrontier)
        }
        KafkaOffsetClassificationV0::Gap => Ok(KafkaCanonicalAdmissionOutcomeV0::Gap),
        KafkaOffsetClassificationV0::Expected => {
            let mut prepared_frontier = frontier.clone();
            let prepared = prepared_frontier
                .observe(
                    observed_source_id,
                    observed_cluster_id,
                    observed_topic_id,
                    position,
                )
                .map_err(|category| KafkaCanonicalAdmissionErrorV0::Frontier { category })?;
            if prepared != KafkaOffsetObservationV0::Advanced {
                return Err(KafkaCanonicalAdmissionErrorV0::FrontierTransitionInvariant);
            }
            let admission = admission
                .apply_event_v1(event)
                .map_err(|category| KafkaCanonicalAdmissionErrorV0::Admission { category })?;
            *frontier = prepared_frontier;
            Ok(KafkaCanonicalAdmissionOutcomeV0::Advanced { admission })
        }
    }
}
