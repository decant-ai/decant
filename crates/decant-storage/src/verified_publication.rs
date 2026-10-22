//! Publish one local checkpoint and strictly reread it before returning a candidate.

use std::error::Error;
use std::fmt;

use decant_ingest::{
    KafkaCheckpointGenerationV0, KafkaIngestionCheckpointDecodeErrorV0,
    KafkaIngestionCheckpointHashErrorV0, KafkaIngestionCheckpointHashV0,
    hash_kafka_ingestion_checkpoint_v0, validate_kafka_ingestion_checkpoint_expected_contract_v0,
};
use decant_kafka::{
    KafkaFrontierHashErrorV0, KafkaFrontierHashV0, KafkaPartitionFrontierV0, KafkaSourceContractV0,
    hash_kafka_partition_frontier_v0,
};
use decant_state::{AdmissionConfigV0, AdmissionStateV0};

use crate::{
    LocalCheckpointPublicationErrorV0, LocalCheckpointPublicationFaultPointV0,
    LocalCheckpointPublicationVisibilityV0, LocalCheckpointStoreV0,
    LocalKafkaCheckpointCommitCandidateErrorV0, LocalKafkaCheckpointCommitCandidateV0,
    publish_local_kafka_checkpoint_generation_v0,
    publish_local_kafka_checkpoint_generation_with_fault_v0,
    recover_latest_local_kafka_checkpoint_commit_candidate_v0,
};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum VerifiedLocalKafkaCheckpointPublicationStatusV0 {
    NotPublished,
    Uncertain,
    Published,
}

#[derive(Clone, Eq, PartialEq)]
pub enum VerifiedLocalKafkaCheckpointPublicationErrorV0 {
    ExpectedContract(KafkaIngestionCheckpointDecodeErrorV0),
    WriterLockMismatch,
    AdmissionConfigMismatch,
    SourceContractMismatch,
    CheckpointHash(KafkaIngestionCheckpointHashErrorV0),
    FrontierHash(KafkaFrontierHashErrorV0),
    Publication(LocalCheckpointPublicationErrorV0),
    CandidateRecovery(LocalKafkaCheckpointCommitCandidateErrorV0),
    UnexpectedRecoveredGeneration {
        attempted: KafkaCheckpointGenerationV0,
        recovered: KafkaCheckpointGenerationV0,
    },
    CheckpointHashMismatch,
    FrontierHashMismatch,
    ArtifactBytesMismatch {
        published: usize,
        recovered: usize,
    },
}

impl VerifiedLocalKafkaCheckpointPublicationErrorV0 {
    pub const fn publication_status(&self) -> VerifiedLocalKafkaCheckpointPublicationStatusV0 {
        match self {
            Self::ExpectedContract(_)
            | Self::WriterLockMismatch
            | Self::AdmissionConfigMismatch
            | Self::SourceContractMismatch
            | Self::CheckpointHash(_)
            | Self::FrontierHash(_) => {
                VerifiedLocalKafkaCheckpointPublicationStatusV0::NotPublished
            }
            Self::Publication(error) => match error.visibility() {
                LocalCheckpointPublicationVisibilityV0::NotPublished => {
                    VerifiedLocalKafkaCheckpointPublicationStatusV0::NotPublished
                }
                LocalCheckpointPublicationVisibilityV0::Uncertain => {
                    VerifiedLocalKafkaCheckpointPublicationStatusV0::Uncertain
                }
            },
            Self::CandidateRecovery(_)
            | Self::UnexpectedRecoveredGeneration { .. }
            | Self::CheckpointHashMismatch
            | Self::FrontierHashMismatch
            | Self::ArtifactBytesMismatch { .. } => {
                VerifiedLocalKafkaCheckpointPublicationStatusV0::Published
            }
        }
    }
}

impl fmt::Debug for VerifiedLocalKafkaCheckpointPublicationErrorV0 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ExpectedContract(error) => formatter
                .debug_tuple("ExpectedContract")
                .field(error)
                .finish(),
            Self::WriterLockMismatch => formatter.write_str("WriterLockMismatch"),
            Self::AdmissionConfigMismatch => formatter.write_str("AdmissionConfigMismatch"),
            Self::SourceContractMismatch => formatter.write_str("SourceContractMismatch"),
            Self::CheckpointHash(error) => formatter
                .debug_tuple("CheckpointHash")
                .field(error)
                .finish(),
            Self::FrontierHash(error) => {
                formatter.debug_tuple("FrontierHash").field(error).finish()
            }
            Self::Publication(error) => formatter.debug_tuple("Publication").field(error).finish(),
            Self::CandidateRecovery(error) => formatter
                .debug_tuple("CandidateRecovery")
                .field(error)
                .finish(),
            Self::UnexpectedRecoveredGeneration {
                attempted,
                recovered,
            } => formatter
                .debug_struct("UnexpectedRecoveredGeneration")
                .field("attempted", attempted)
                .field("recovered", recovered)
                .finish(),
            Self::CheckpointHashMismatch => formatter.write_str("CheckpointHashMismatch"),
            Self::FrontierHashMismatch => formatter.write_str("FrontierHashMismatch"),
            Self::ArtifactBytesMismatch {
                published,
                recovered,
            } => formatter
                .debug_struct("ArtifactBytesMismatch")
                .field("published", published)
                .field("recovered", recovered)
                .finish(),
        }
    }
}

impl fmt::Display for VerifiedLocalKafkaCheckpointPublicationErrorV0 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(self, formatter)
    }
}

impl Error for VerifiedLocalKafkaCheckpointPublicationErrorV0 {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::ExpectedContract(error) => Some(error),
            Self::CheckpointHash(error) => Some(error),
            Self::FrontierHash(error) => Some(error),
            Self::Publication(error) => Some(error),
            Self::CandidateRecovery(error) => Some(error),
            Self::WriterLockMismatch
            | Self::AdmissionConfigMismatch
            | Self::SourceContractMismatch
            | Self::UnexpectedRecoveredGeneration { .. }
            | Self::CheckpointHashMismatch
            | Self::FrontierHashMismatch
            | Self::ArtifactBytesMismatch { .. } => None,
        }
    }
}

#[allow(
    clippy::too_many_arguments,
    reason = "the held writer guard and trusted checkpoint expectations remain explicit"
)]
pub fn publish_and_verify_local_kafka_checkpoint_commit_candidate_with_writer_lock_v0(
    writer_lock: &crate::LocalCheckpointWriterLockV0,
    store: &LocalCheckpointStoreV0,
    generation: KafkaCheckpointGenerationV0,
    admission: &AdmissionStateV0,
    frontier: &KafkaPartitionFrontierV0,
    expected_admission_config: &AdmissionConfigV0,
    expected_source_contract: &KafkaSourceContractV0,
) -> Result<LocalKafkaCheckpointCommitCandidateV0, VerifiedLocalKafkaCheckpointPublicationErrorV0> {
    if !writer_lock.protects(store) {
        return Err(VerifiedLocalKafkaCheckpointPublicationErrorV0::WriterLockMismatch);
    }
    publish_and_verify_local_kafka_checkpoint_commit_candidate_core_v0(
        store,
        generation,
        admission,
        frontier,
        expected_admission_config,
        expected_source_contract,
        None,
    )
}

pub fn publish_and_verify_local_kafka_checkpoint_commit_candidate_v0(
    store: &LocalCheckpointStoreV0,
    generation: KafkaCheckpointGenerationV0,
    admission: &AdmissionStateV0,
    frontier: &KafkaPartitionFrontierV0,
    expected_admission_config: &AdmissionConfigV0,
    expected_source_contract: &KafkaSourceContractV0,
) -> Result<LocalKafkaCheckpointCommitCandidateV0, VerifiedLocalKafkaCheckpointPublicationErrorV0> {
    publish_and_verify_local_kafka_checkpoint_commit_candidate_core_v0(
        store,
        generation,
        admission,
        frontier,
        expected_admission_config,
        expected_source_contract,
        None,
    )
}

#[allow(
    clippy::too_many_arguments,
    reason = "the diagnostic fault remains explicit alongside the production coordinator inputs"
)]
pub fn publish_and_verify_local_kafka_checkpoint_commit_candidate_with_fault_v0(
    store: &LocalCheckpointStoreV0,
    generation: KafkaCheckpointGenerationV0,
    admission: &AdmissionStateV0,
    frontier: &KafkaPartitionFrontierV0,
    expected_admission_config: &AdmissionConfigV0,
    expected_source_contract: &KafkaSourceContractV0,
    fault_point: LocalCheckpointPublicationFaultPointV0,
) -> Result<LocalKafkaCheckpointCommitCandidateV0, VerifiedLocalKafkaCheckpointPublicationErrorV0> {
    publish_and_verify_local_kafka_checkpoint_commit_candidate_core_v0(
        store,
        generation,
        admission,
        frontier,
        expected_admission_config,
        expected_source_contract,
        Some(fault_point),
    )
}

#[allow(
    clippy::too_many_arguments,
    reason = "trusted expectations and optional diagnostic injection remain explicit"
)]
fn publish_and_verify_local_kafka_checkpoint_commit_candidate_core_v0(
    store: &LocalCheckpointStoreV0,
    generation: KafkaCheckpointGenerationV0,
    admission: &AdmissionStateV0,
    frontier: &KafkaPartitionFrontierV0,
    expected_admission_config: &AdmissionConfigV0,
    expected_source_contract: &KafkaSourceContractV0,
    fault_point: Option<LocalCheckpointPublicationFaultPointV0>,
) -> Result<LocalKafkaCheckpointCommitCandidateV0, VerifiedLocalKafkaCheckpointPublicationErrorV0> {
    validate_kafka_ingestion_checkpoint_expected_contract_v0(
        expected_admission_config,
        expected_source_contract,
    )
    .map_err(VerifiedLocalKafkaCheckpointPublicationErrorV0::ExpectedContract)?;
    if admission.config() != expected_admission_config {
        return Err(VerifiedLocalKafkaCheckpointPublicationErrorV0::AdmissionConfigMismatch);
    }
    if frontier.source_contract() != expected_source_contract {
        return Err(VerifiedLocalKafkaCheckpointPublicationErrorV0::SourceContractMismatch);
    }

    let expected_checkpoint_hash = hash_kafka_ingestion_checkpoint_v0(admission, frontier)
        .map_err(VerifiedLocalKafkaCheckpointPublicationErrorV0::CheckpointHash)?;
    let expected_frontier_hash = hash_kafka_partition_frontier_v0(frontier)
        .map_err(VerifiedLocalKafkaCheckpointPublicationErrorV0::FrontierHash)?;

    let receipt = match fault_point {
        Some(point) => publish_local_kafka_checkpoint_generation_with_fault_v0(
            store, generation, admission, frontier, point,
        ),
        None => {
            publish_local_kafka_checkpoint_generation_v0(store, generation, admission, frontier)
        }
    }
    .map_err(VerifiedLocalKafkaCheckpointPublicationErrorV0::Publication)?;

    let candidate = recover_latest_local_kafka_checkpoint_commit_candidate_v0(
        store,
        expected_admission_config,
        expected_source_contract,
    )
    .map_err(VerifiedLocalKafkaCheckpointPublicationErrorV0::CandidateRecovery)?;
    verify_candidate(
        generation,
        receipt.artifact_bytes(),
        expected_checkpoint_hash,
        expected_frontier_hash,
        &candidate,
    )?;
    Ok(candidate)
}

fn verify_candidate(
    attempted_generation: KafkaCheckpointGenerationV0,
    published_artifact_bytes: usize,
    expected_checkpoint_hash: KafkaIngestionCheckpointHashV0,
    expected_frontier_hash: KafkaFrontierHashV0,
    candidate: &LocalKafkaCheckpointCommitCandidateV0,
) -> Result<(), VerifiedLocalKafkaCheckpointPublicationErrorV0> {
    if candidate.generation() != attempted_generation {
        return Err(
            VerifiedLocalKafkaCheckpointPublicationErrorV0::UnexpectedRecoveredGeneration {
                attempted: attempted_generation,
                recovered: candidate.generation(),
            },
        );
    }
    if candidate.checkpoint_hash() != expected_checkpoint_hash {
        return Err(VerifiedLocalKafkaCheckpointPublicationErrorV0::CheckpointHashMismatch);
    }
    if candidate.offset_commit_plan().frontier_hash() != expected_frontier_hash {
        return Err(VerifiedLocalKafkaCheckpointPublicationErrorV0::FrontierHashMismatch);
    }
    if candidate.artifact_bytes() != published_artifact_bytes {
        return Err(
            VerifiedLocalKafkaCheckpointPublicationErrorV0::ArtifactBytesMismatch {
                published: published_artifact_bytes,
                recovered: candidate.artifact_bytes(),
            },
        );
    }
    Ok(())
}
