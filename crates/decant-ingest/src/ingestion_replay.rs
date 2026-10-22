//! Deterministic raw Kafka JSON replay with one in-memory interruption boundary.

use std::error::Error;
use std::fmt;

use decant_ir::SourcePositionKindV0;
use decant_kafka::KafkaPartitionFrontierV0;
use decant_state::{AdmissionConfigV0, AdmissionStateV0};

use crate::{
    KafkaIngestionCheckpointDecodeErrorV0, KafkaIngestionCheckpointEncodeErrorV0,
    KafkaJsonIngestionErrorV0, KafkaJsonIngestionOutcomeV0, KafkaJsonIngestionReplayJournalV0,
    encode_kafka_ingestion_checkpoint_v0, ingest_decant_json_kafka_record_v0,
    restore_kafka_ingestion_checkpoint_v0,
};

pub type KafkaJsonIngestionReplayOutcomeV0 =
    Result<KafkaJsonIngestionOutcomeV0, KafkaJsonIngestionErrorV0>;

pub struct KafkaJsonIngestionReplayReportV0 {
    state: AdmissionStateV0,
    frontier: KafkaPartitionFrontierV0,
    outcomes: Vec<KafkaJsonIngestionReplayOutcomeV0>,
}

impl KafkaJsonIngestionReplayReportV0 {
    pub const fn state(&self) -> &AdmissionStateV0 {
        &self.state
    }

    pub const fn frontier(&self) -> &KafkaPartitionFrontierV0 {
        &self.frontier
    }

    pub fn outcomes(&self) -> &[KafkaJsonIngestionReplayOutcomeV0] {
        &self.outcomes
    }

    pub fn into_parts(
        self,
    ) -> (
        AdmissionStateV0,
        KafkaPartitionFrontierV0,
        Vec<KafkaJsonIngestionReplayOutcomeV0>,
    ) {
        (self.state, self.frontier, self.outcomes)
    }
}

impl fmt::Debug for KafkaJsonIngestionReplayReportV0 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("KafkaJsonIngestionReplayReportV0")
            .field("outcome_count", &self.outcomes.len())
            .field("state_usage", &self.state.usage())
            .field(
                "frontier_assignment_count",
                &self.frontier.assignment_count(),
            )
            .finish_non_exhaustive()
    }
}

#[derive(Clone, Eq, PartialEq)]
pub enum KafkaJsonIngestionReplayErrorV0 {
    UnsupportedInitialSourcePositionKind { actual: SourcePositionKindV0 },
    InitialSourceMismatch,
    InterruptionOutOfRange { maximum: usize, actual: usize },
    OutcomeAllocationFailed { requested: usize },
    CheckpointEncoding(KafkaIngestionCheckpointEncodeErrorV0),
    CheckpointDecoding(KafkaIngestionCheckpointDecodeErrorV0),
}

impl fmt::Debug for KafkaJsonIngestionReplayErrorV0 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::UnsupportedInitialSourcePositionKind { actual } => formatter
                .debug_struct("UnsupportedInitialSourcePositionKind")
                .field("actual", actual)
                .finish(),
            Self::InitialSourceMismatch => formatter.write_str("InitialSourceMismatch"),
            Self::InterruptionOutOfRange { maximum, actual } => formatter
                .debug_struct("InterruptionOutOfRange")
                .field("maximum", maximum)
                .field("actual", actual)
                .finish(),
            Self::OutcomeAllocationFailed { requested } => formatter
                .debug_struct("OutcomeAllocationFailed")
                .field("requested", requested)
                .finish(),
            Self::CheckpointEncoding(error) => formatter
                .debug_tuple("CheckpointEncoding")
                .field(error)
                .finish(),
            Self::CheckpointDecoding(error) => formatter
                .debug_tuple("CheckpointDecoding")
                .field(error)
                .finish(),
        }
    }
}

impl fmt::Display for KafkaJsonIngestionReplayErrorV0 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(self, formatter)
    }
}

impl Error for KafkaJsonIngestionReplayErrorV0 {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::CheckpointEncoding(error) => Some(error),
            Self::CheckpointDecoding(error) => Some(error),
            _ => None,
        }
    }
}

pub fn replay_kafka_json_ingestion_journal_v0(
    admission_config: &AdmissionConfigV0,
    initial_frontier: &KafkaPartitionFrontierV0,
    journal: &KafkaJsonIngestionReplayJournalV0,
) -> Result<KafkaJsonIngestionReplayReportV0, KafkaJsonIngestionReplayErrorV0> {
    let (mut state, mut frontier) = initialize(admission_config, initial_frontier)?;
    let mut outcomes = allocate_outcomes(journal.len())?;
    execute_inputs(&mut state, &mut frontier, journal.inputs(), &mut outcomes);
    Ok(KafkaJsonIngestionReplayReportV0 {
        state,
        frontier,
        outcomes,
    })
}

pub fn replay_kafka_json_ingestion_journal_with_interruption_v0(
    admission_config: &AdmissionConfigV0,
    initial_frontier: &KafkaPartitionFrontierV0,
    journal: &KafkaJsonIngestionReplayJournalV0,
    interruption_after_inputs: usize,
) -> Result<KafkaJsonIngestionReplayReportV0, KafkaJsonIngestionReplayErrorV0> {
    let (mut state, mut frontier) = initialize(admission_config, initial_frontier)?;
    if interruption_after_inputs > journal.len() {
        return Err(KafkaJsonIngestionReplayErrorV0::InterruptionOutOfRange {
            maximum: journal.len(),
            actual: interruption_after_inputs,
        });
    }
    let mut outcomes = allocate_outcomes(journal.len())?;
    let (prefix, suffix) = journal.inputs().split_at(interruption_after_inputs);
    execute_inputs(&mut state, &mut frontier, prefix, &mut outcomes);

    let checkpoint = encode_kafka_ingestion_checkpoint_v0(&state, &frontier)
        .map_err(KafkaJsonIngestionReplayErrorV0::CheckpointEncoding)?;
    (state, frontier) = restore_kafka_ingestion_checkpoint_v0(
        &checkpoint,
        admission_config,
        initial_frontier.source_contract(),
    )
    .map_err(KafkaJsonIngestionReplayErrorV0::CheckpointDecoding)?;

    execute_inputs(&mut state, &mut frontier, suffix, &mut outcomes);
    Ok(KafkaJsonIngestionReplayReportV0 {
        state,
        frontier,
        outcomes,
    })
}

fn initialize(
    admission_config: &AdmissionConfigV0,
    initial_frontier: &KafkaPartitionFrontierV0,
) -> Result<(AdmissionStateV0, KafkaPartitionFrontierV0), KafkaJsonIngestionReplayErrorV0> {
    if admission_config.source_position_kind() != SourcePositionKindV0::Kafka {
        return Err(
            KafkaJsonIngestionReplayErrorV0::UnsupportedInitialSourcePositionKind {
                actual: admission_config.source_position_kind(),
            },
        );
    }
    if admission_config.source_id() != initial_frontier.source_contract().source_id() {
        return Err(KafkaJsonIngestionReplayErrorV0::InitialSourceMismatch);
    }
    Ok((
        AdmissionStateV0::new(admission_config.clone()),
        initial_frontier.clone(),
    ))
}

fn allocate_outcomes(
    requested: usize,
) -> Result<Vec<KafkaJsonIngestionReplayOutcomeV0>, KafkaJsonIngestionReplayErrorV0> {
    let mut outcomes = Vec::new();
    outcomes
        .try_reserve_exact(requested)
        .map_err(|_| KafkaJsonIngestionReplayErrorV0::OutcomeAllocationFailed { requested })?;
    Ok(outcomes)
}

fn execute_inputs(
    state: &mut AdmissionStateV0,
    frontier: &mut KafkaPartitionFrontierV0,
    inputs: &[crate::KafkaJsonIngestionReplayInputV0],
    outcomes: &mut Vec<KafkaJsonIngestionReplayOutcomeV0>,
) {
    for input in inputs {
        outcomes.push(ingest_decant_json_kafka_record_v0(
            frontier,
            state,
            input.observed_source_id(),
            input.observed_cluster_id(),
            input.observed_topic_id(),
            input.record(),
        ));
    }
}
