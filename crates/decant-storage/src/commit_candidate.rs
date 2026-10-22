//! Strictly recovered local checkpoint candidates for a later Kafka adapter.
//!
//! A candidate is not authorization to execute a broker commit. It proves only
//! that the represented coordinates came from the strict local recovery path.

use std::error::Error;
use std::fmt;

use decant_ingest::{KafkaCheckpointGenerationV0, KafkaIngestionCheckpointHashV0};
use decant_kafka::{
    KafkaOffsetCommitPlanErrorV0, KafkaOffsetCommitPlanV0, KafkaPartitionFrontierV0,
    KafkaSourceContractV0, build_kafka_offset_commit_plan_v0,
};
use decant_state::{AdmissionConfigV0, AdmissionStateV0};

use crate::{
    LocalCheckpointRecoveryErrorV0, LocalCheckpointRecoveryV0, LocalCheckpointStoreV0,
    restore_latest_local_kafka_checkpoint_generation_v0,
};

pub struct LocalKafkaCheckpointCommitCandidateV0 {
    recovery: LocalCheckpointRecoveryV0,
    offset_commit_plan: KafkaOffsetCommitPlanV0,
}

impl LocalKafkaCheckpointCommitCandidateV0 {
    pub const fn generation(&self) -> KafkaCheckpointGenerationV0 {
        self.recovery.generation()
    }

    pub const fn checkpoint_hash(&self) -> KafkaIngestionCheckpointHashV0 {
        self.recovery.checkpoint_hash()
    }

    pub const fn admission(&self) -> &AdmissionStateV0 {
        self.recovery.admission()
    }

    pub const fn frontier(&self) -> &KafkaPartitionFrontierV0 {
        self.recovery.frontier()
    }

    pub const fn offset_commit_plan(&self) -> &KafkaOffsetCommitPlanV0 {
        &self.offset_commit_plan
    }

    pub const fn artifact_bytes(&self) -> usize {
        self.recovery.artifact_bytes()
    }

    pub const fn pending_entries(&self) -> usize {
        self.recovery.pending_entries()
    }

    pub fn into_recovery(self) -> LocalCheckpointRecoveryV0 {
        self.recovery
    }
}

impl fmt::Debug for LocalKafkaCheckpointCommitCandidateV0 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("LocalKafkaCheckpointCommitCandidateV0")
            .field("generation", &self.generation())
            .field("checkpoint_hash", &self.checkpoint_hash())
            .field("offset_commit_plan", &self.offset_commit_plan)
            .field("artifact_bytes", &self.artifact_bytes())
            .field("pending_entries", &self.pending_entries())
            .field("admission", &"<redacted>")
            .field("frontier", &"<redacted>")
            .finish()
    }
}

#[derive(Clone, Eq, PartialEq)]
pub enum LocalKafkaCheckpointCommitCandidateErrorV0 {
    Recovery(LocalCheckpointRecoveryErrorV0),
    OffsetCommitPlan(KafkaOffsetCommitPlanErrorV0),
}

impl fmt::Debug for LocalKafkaCheckpointCommitCandidateErrorV0 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Recovery(error) => formatter.debug_tuple("Recovery").field(error).finish(),
            Self::OffsetCommitPlan(error) => formatter
                .debug_tuple("OffsetCommitPlan")
                .field(error)
                .finish(),
        }
    }
}

impl fmt::Display for LocalKafkaCheckpointCommitCandidateErrorV0 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(self, formatter)
    }
}

impl Error for LocalKafkaCheckpointCommitCandidateErrorV0 {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Recovery(error) => Some(error),
            Self::OffsetCommitPlan(error) => Some(error),
        }
    }
}

pub fn recover_latest_local_kafka_checkpoint_commit_candidate_v0(
    store: &LocalCheckpointStoreV0,
    expected_admission_config: &AdmissionConfigV0,
    expected_source_contract: &KafkaSourceContractV0,
) -> Result<LocalKafkaCheckpointCommitCandidateV0, LocalKafkaCheckpointCommitCandidateErrorV0> {
    let recovery = restore_latest_local_kafka_checkpoint_generation_v0(
        store,
        expected_admission_config,
        expected_source_contract,
    )
    .map_err(LocalKafkaCheckpointCommitCandidateErrorV0::Recovery)?;
    let offset_commit_plan = build_kafka_offset_commit_plan_v0(recovery.frontier())
        .map_err(LocalKafkaCheckpointCommitCandidateErrorV0::OffsetCommitPlan)?;
    Ok(LocalKafkaCheckpointCommitCandidateV0 {
        recovery,
        offset_commit_plan,
    })
}
