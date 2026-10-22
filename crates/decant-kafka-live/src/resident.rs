//! Client-neutral lifecycle for one resident, statically owned Kafka source.

use std::error::Error;
use std::fmt;

use decant_kafka::{
    KafkaFetchedRecordV0, KafkaJsonObjectMappingV1, KafkaPartitionFrontierV0, KafkaSourceContractV0,
};
use decant_state::{
    AdmissionConfigV0, AdmissionEpochRolloverErrorV0, AdmissionPhysicalRetentionErrorV0,
    AdmissionStateV0, EventTimeRetentionConfigV0, FinalizeOutcomeV0,
};
use decant_storage::{
    LocalCheckpointPruneErrorV0, LocalCheckpointPruneReceiptV0, LocalCheckpointStoreV0,
    LocalCheckpointWriterLockErrorV0, LocalCheckpointWriterLockV0,
    VerifiedLocalKafkaCheckpointPublicationErrorV0, acquire_local_checkpoint_writer_lock_v0,
    prune_verified_local_kafka_checkpoints_v0,
    publish_and_verify_local_kafka_checkpoint_commit_candidate_with_writer_lock_v0,
};

use crate::{
    KafkaStaticCheckpointSessionV0, KafkaStaticSessionBootstrapErrorV0,
    KafkaStaticSessionCheckpointV0, KafkaStaticSessionProcessErrorV0, KafkaStaticSessionProcessV0,
};

pub const KAFKA_RESIDENT_LIFECYCLE_VERSION_V0: u16 = 0;
pub const MAX_KAFKA_RESIDENT_RECORDS_BETWEEN_CHECKPOINTS_V0: usize = 1_000_000;
pub const MAX_KAFKA_RESIDENT_IDLE_OBSERVATIONS_BETWEEN_CHECKPOINTS_V0: usize = 10_000;
pub const MAX_KAFKA_RESIDENT_RETAINED_CHECKPOINT_GENERATIONS_V0: usize = 253;
pub const MAX_KAFKA_RESIDENT_CONTEXT_BYTES_V0: usize = 16 * 1_024;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum KafkaResidentLifecycleFieldV0 {
    RecordsBetweenCheckpoints,
    IdleObservationsBetweenCheckpoints,
    RetainedCheckpointGenerations,
}

#[derive(Clone, Eq, PartialEq)]
pub enum KafkaResidentLifecycleConfigErrorV0 {
    UnsupportedVersion {
        actual: u16,
    },
    Zero {
        field: KafkaResidentLifecycleFieldV0,
    },
    AboveMaximum {
        field: KafkaResidentLifecycleFieldV0,
        maximum: usize,
        actual: usize,
    },
}

impl fmt::Debug for KafkaResidentLifecycleConfigErrorV0 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::UnsupportedVersion { actual } => formatter
                .debug_struct("UnsupportedVersion")
                .field("actual", actual)
                .finish(),
            Self::Zero { field } => formatter
                .debug_struct("Zero")
                .field("field", field)
                .finish(),
            Self::AboveMaximum {
                field,
                maximum,
                actual,
            } => formatter
                .debug_struct("AboveMaximum")
                .field("field", field)
                .field("maximum", maximum)
                .field("actual", actual)
                .finish(),
        }
    }
}

impl fmt::Display for KafkaResidentLifecycleConfigErrorV0 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(self, formatter)
    }
}

impl Error for KafkaResidentLifecycleConfigErrorV0 {}

#[derive(Clone, Copy, Eq, PartialEq)]
pub struct KafkaResidentLifecycleConfigV0 {
    records_between_checkpoints: usize,
    idle_observations_between_checkpoints: usize,
    retained_checkpoint_generations: usize,
    physical_retention: EventTimeRetentionConfigV0,
}

impl KafkaResidentLifecycleConfigV0 {
    pub fn new(
        version: u16,
        records_between_checkpoints: usize,
        idle_observations_between_checkpoints: usize,
        retained_checkpoint_generations: usize,
        physical_retention: EventTimeRetentionConfigV0,
    ) -> Result<Self, KafkaResidentLifecycleConfigErrorV0> {
        if version != KAFKA_RESIDENT_LIFECYCLE_VERSION_V0 {
            return Err(KafkaResidentLifecycleConfigErrorV0::UnsupportedVersion {
                actual: version,
            });
        }
        validate_bound(
            records_between_checkpoints,
            MAX_KAFKA_RESIDENT_RECORDS_BETWEEN_CHECKPOINTS_V0,
            KafkaResidentLifecycleFieldV0::RecordsBetweenCheckpoints,
        )?;
        validate_bound(
            idle_observations_between_checkpoints,
            MAX_KAFKA_RESIDENT_IDLE_OBSERVATIONS_BETWEEN_CHECKPOINTS_V0,
            KafkaResidentLifecycleFieldV0::IdleObservationsBetweenCheckpoints,
        )?;
        validate_bound(
            retained_checkpoint_generations,
            MAX_KAFKA_RESIDENT_RETAINED_CHECKPOINT_GENERATIONS_V0,
            KafkaResidentLifecycleFieldV0::RetainedCheckpointGenerations,
        )?;
        Ok(Self {
            records_between_checkpoints,
            idle_observations_between_checkpoints,
            retained_checkpoint_generations,
            physical_retention,
        })
    }

    pub const fn records_between_checkpoints(self) -> usize {
        self.records_between_checkpoints
    }

    pub const fn idle_observations_between_checkpoints(self) -> usize {
        self.idle_observations_between_checkpoints
    }

    pub const fn retained_checkpoint_generations(self) -> usize {
        self.retained_checkpoint_generations
    }

    pub const fn physical_retention(self) -> EventTimeRetentionConfigV0 {
        self.physical_retention
    }
}

impl fmt::Debug for KafkaResidentLifecycleConfigV0 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("KafkaResidentLifecycleConfigV0")
            .field("version", &KAFKA_RESIDENT_LIFECYCLE_VERSION_V0)
            .field(
                "records_between_checkpoints",
                &self.records_between_checkpoints,
            )
            .field(
                "idle_observations_between_checkpoints",
                &self.idle_observations_between_checkpoints,
            )
            .field(
                "retained_checkpoint_generations",
                &self.retained_checkpoint_generations,
            )
            .field("physical_retention", &self.physical_retention)
            .finish()
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum KafkaResidentCompilerErrorV0 {
    Refused,
}

impl fmt::Display for KafkaResidentCompilerErrorV0 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(self, formatter)
    }
}

impl Error for KafkaResidentCompilerErrorV0 {}

#[derive(Clone, Eq, PartialEq)]
pub enum KafkaResidentBootstrapErrorV0 {
    WriterLock(LocalCheckpointWriterLockErrorV0),
    Session(KafkaStaticSessionBootstrapErrorV0),
    Compiler(KafkaResidentCompilerErrorV0),
    EmptyContext,
    ContextTooLarge { maximum: usize, actual: usize },
}

impl fmt::Debug for KafkaResidentBootstrapErrorV0 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::WriterLock(error) => formatter.debug_tuple("WriterLock").field(error).finish(),
            Self::Session(error) => formatter.debug_tuple("Session").field(error).finish(),
            Self::Compiler(error) => formatter.debug_tuple("Compiler").field(error).finish(),
            Self::EmptyContext => formatter.write_str("EmptyContext"),
            Self::ContextTooLarge { maximum, actual } => formatter
                .debug_struct("ContextTooLarge")
                .field("maximum", maximum)
                .field("actual", actual)
                .finish(),
        }
    }
}

impl fmt::Display for KafkaResidentBootstrapErrorV0 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(self, formatter)
    }
}

impl Error for KafkaResidentBootstrapErrorV0 {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::WriterLock(error) => Some(error),
            Self::Session(error) => Some(error),
            Self::Compiler(error) => Some(error),
            Self::EmptyContext | Self::ContextTooLarge { .. } => None,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum KafkaResidentAccountingFieldV0 {
    RecordsSinceCheckpoint,
    IdleObservationsSinceCheckpoint,
    ContextsCompiled,
    PublishedCheckpoints,
    PrunedCheckpointGenerations,
    PhysicallyRemovedIdentities,
    PhysicallyRemovedEventBytes,
}

#[derive(Clone, Eq, PartialEq)]
pub enum KafkaResidentOperationErrorV0 {
    Stopped,
    Faulted,
    RejectedAdmission,
    Processing(KafkaStaticSessionProcessErrorV0),
    Accounting {
        field: KafkaResidentAccountingFieldV0,
    },
    Compiler(KafkaResidentCompilerErrorV0),
    EmptyContext,
    ContextTooLarge {
        maximum: usize,
        actual: usize,
    },
    PhysicalRetention(AdmissionPhysicalRetentionErrorV0),
    EpochRollover(AdmissionEpochRolloverErrorV0),
    GenerationExhausted,
    Checkpoint(VerifiedLocalKafkaCheckpointPublicationErrorV0),
    Prune(LocalCheckpointPruneErrorV0),
}

impl fmt::Debug for KafkaResidentOperationErrorV0 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Stopped => formatter.write_str("Stopped"),
            Self::Faulted => formatter.write_str("Faulted"),
            Self::RejectedAdmission => formatter.write_str("RejectedAdmission"),
            Self::Processing(error) => formatter.debug_tuple("Processing").field(error).finish(),
            Self::Accounting { field } => formatter
                .debug_struct("Accounting")
                .field("field", field)
                .finish(),
            Self::Compiler(error) => formatter.debug_tuple("Compiler").field(error).finish(),
            Self::EmptyContext => formatter.write_str("EmptyContext"),
            Self::ContextTooLarge { maximum, actual } => formatter
                .debug_struct("ContextTooLarge")
                .field("maximum", maximum)
                .field("actual", actual)
                .finish(),
            Self::PhysicalRetention(error) => formatter
                .debug_tuple("PhysicalRetention")
                .field(error)
                .finish(),
            Self::EpochRollover(error) => {
                formatter.debug_tuple("EpochRollover").field(error).finish()
            }
            Self::GenerationExhausted => formatter.write_str("GenerationExhausted"),
            Self::Checkpoint(error) => formatter.debug_tuple("Checkpoint").field(error).finish(),
            Self::Prune(error) => formatter.debug_tuple("Prune").field(error).finish(),
        }
    }
}

impl fmt::Display for KafkaResidentOperationErrorV0 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(self, formatter)
    }
}

impl Error for KafkaResidentOperationErrorV0 {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Processing(error) => Some(error),
            Self::Compiler(error) => Some(error),
            Self::PhysicalRetention(error) => Some(error),
            Self::EpochRollover(error) => Some(error),
            Self::Checkpoint(error) => Some(error),
            Self::Prune(error) => Some(error),
            Self::Stopped
            | Self::Faulted
            | Self::RejectedAdmission
            | Self::Accounting { .. }
            | Self::EmptyContext
            | Self::ContextTooLarge { .. }
            | Self::GenerationExhausted => None,
        }
    }
}

#[derive(Clone, Eq, PartialEq)]
pub struct KafkaResidentContextV0 {
    bytes: Vec<u8>,
}

impl KafkaResidentContextV0 {
    pub fn as_bytes(&self) -> &[u8] {
        &self.bytes
    }
}

impl fmt::Debug for KafkaResidentContextV0 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("KafkaResidentContextV0")
            .field("bytes", &self.bytes.len())
            .finish()
    }
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct KafkaResidentUsageV0 {
    contexts_compiled: usize,
    published_checkpoints: usize,
    pruned_checkpoint_generations: usize,
    physically_removed_identities: usize,
    physically_removed_event_bytes: usize,
}

impl KafkaResidentUsageV0 {
    pub const fn contexts_compiled(self) -> usize {
        self.contexts_compiled
    }

    pub const fn published_checkpoints(self) -> usize {
        self.published_checkpoints
    }

    pub const fn pruned_checkpoint_generations(self) -> usize {
        self.pruned_checkpoint_generations
    }

    pub const fn physically_removed_identities(self) -> usize {
        self.physically_removed_identities
    }

    pub const fn physically_removed_event_bytes(self) -> usize {
        self.physically_removed_event_bytes
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct KafkaResidentProcessV0 {
    process: KafkaStaticSessionProcessV0,
    checkpoint: Option<KafkaStaticSessionCheckpointV0>,
    pruning: Option<LocalCheckpointPruneReceiptV0>,
}

impl KafkaResidentProcessV0 {
    pub const fn process(self) -> KafkaStaticSessionProcessV0 {
        self.process
    }

    pub const fn checkpoint(self) -> Option<KafkaStaticSessionCheckpointV0> {
        self.checkpoint
    }

    pub const fn pruning(self) -> Option<LocalCheckpointPruneReceiptV0> {
        self.pruning
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum KafkaResidentStatusV0 {
    Running,
    Faulted,
    Stopped,
}

pub struct KafkaResidentSessionV0 {
    store: LocalCheckpointStoreV0,
    writer_lock: LocalCheckpointWriterLockV0,
    session: KafkaStaticCheckpointSessionV0,
    config: KafkaResidentLifecycleConfigV0,
    context: KafkaResidentContextV0,
    records_since_checkpoint: usize,
    idle_observations_since_checkpoint: usize,
    usage: KafkaResidentUsageV0,
    dirty: bool,
    status: KafkaResidentStatusV0,
    compile_on_admission: bool,
}

impl KafkaResidentSessionV0 {
    pub fn bootstrap<F>(
        store: LocalCheckpointStoreV0,
        admission_config: AdmissionConfigV0,
        source_contract: KafkaSourceContractV0,
        configured_starting_positions: &[decant_ir::KafkaPositionV0],
        config: KafkaResidentLifecycleConfigV0,
        compiler: F,
    ) -> Result<Self, KafkaResidentBootstrapErrorV0>
    where
        F: FnOnce(&AdmissionStateV0) -> Result<Vec<u8>, KafkaResidentCompilerErrorV0>,
    {
        Self::bootstrap_inner(
            store,
            admission_config,
            source_contract,
            configured_starting_positions,
            config,
            true,
            |state, _| compiler(state),
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn bootstrap_inner<F>(
        store: LocalCheckpointStoreV0,
        admission_config: AdmissionConfigV0,
        source_contract: KafkaSourceContractV0,
        configured_starting_positions: &[decant_ir::KafkaPositionV0],
        config: KafkaResidentLifecycleConfigV0,
        compile_on_admission: bool,
        compiler: F,
    ) -> Result<Self, KafkaResidentBootstrapErrorV0>
    where
        F: FnOnce(
            &AdmissionStateV0,
            &KafkaPartitionFrontierV0,
        ) -> Result<Vec<u8>, KafkaResidentCompilerErrorV0>,
    {
        let writer_lock = acquire_local_checkpoint_writer_lock_v0(&store)
            .map_err(KafkaResidentBootstrapErrorV0::WriterLock)?;
        let session = KafkaStaticCheckpointSessionV0::bootstrap(
            &store,
            admission_config,
            source_contract,
            configured_starting_positions,
        )
        .map_err(KafkaResidentBootstrapErrorV0::Session)?;
        let context = compiler(session.admission(), session.frontier())
            .map_err(KafkaResidentBootstrapErrorV0::Compiler)
            .and_then(validate_bootstrap_context)?;
        Ok(Self {
            store,
            writer_lock,
            session,
            config,
            context,
            records_since_checkpoint: 0,
            idle_observations_since_checkpoint: 0,
            usage: KafkaResidentUsageV0 {
                contexts_compiled: 1,
                ..KafkaResidentUsageV0::default()
            },
            dirty: false,
            status: KafkaResidentStatusV0::Running,
            compile_on_admission,
        })
    }

    pub fn context(&self) -> Option<&KafkaResidentContextV0> {
        if self.status == KafkaResidentStatusV0::Faulted {
            None
        } else {
            Some(&self.context)
        }
    }

    pub const fn session(&self) -> &KafkaStaticCheckpointSessionV0 {
        &self.session
    }

    pub const fn usage(&self) -> KafkaResidentUsageV0 {
        self.usage
    }

    pub const fn records_since_checkpoint(&self) -> usize {
        self.records_since_checkpoint
    }

    pub const fn idle_observations_since_checkpoint(&self) -> usize {
        self.idle_observations_since_checkpoint
    }

    pub const fn has_unpublished_progress(&self) -> bool {
        self.dirty
    }

    pub const fn status(&self) -> KafkaResidentStatusV0 {
        self.status
    }

    pub const fn is_stopped(&self) -> bool {
        matches!(self.status, KafkaResidentStatusV0::Stopped)
    }

    pub fn process_json_object_record_v1<F>(
        &mut self,
        mapping: &KafkaJsonObjectMappingV1,
        record: &KafkaFetchedRecordV0,
        compiler: F,
    ) -> Result<KafkaResidentProcessV0, KafkaResidentOperationErrorV0>
    where
        F: FnOnce(&AdmissionStateV0) -> Result<Vec<u8>, KafkaResidentCompilerErrorV0>,
    {
        let result =
            self.process_json_object_record_v1_inner(mapping, record, |state, _| compiler(state));
        if result.is_err() && self.status == KafkaResidentStatusV0::Running {
            self.status = KafkaResidentStatusV0::Faulted;
        }
        result
    }

    fn process_json_object_record_v1_inner<F>(
        &mut self,
        mapping: &KafkaJsonObjectMappingV1,
        record: &KafkaFetchedRecordV0,
        compiler: F,
    ) -> Result<KafkaResidentProcessV0, KafkaResidentOperationErrorV0>
    where
        F: FnOnce(
            &AdmissionStateV0,
            &KafkaPartitionFrontierV0,
        ) -> Result<Vec<u8>, KafkaResidentCompilerErrorV0>,
    {
        self.ensure_running()?;
        let next_records = checked_add(
            self.records_since_checkpoint,
            1,
            KafkaResidentAccountingFieldV0::RecordsSinceCheckpoint,
        )?;
        checked_add(
            self.usage.contexts_compiled,
            1,
            KafkaResidentAccountingFieldV0::ContextsCompiled,
        )?;
        let advanced_before = self.session.advanced_since_checkpoint;
        let process = self
            .session
            .process_json_object_record_v1(mapping, record)
            .map_err(KafkaResidentOperationErrorV0::Processing)?;
        if !self.compile_on_admission
            && matches!(process, KafkaStaticSessionProcessV0::Rejected { reason } if reason != decant_state::RejectionReasonV0::Late)
        {
            return Err(KafkaResidentOperationErrorV0::RejectedAdmission);
        }
        self.dirty |= self.session.advanced_since_checkpoint != advanced_before;
        self.records_since_checkpoint = next_records;
        self.idle_observations_since_checkpoint = 0;
        if self.records_since_checkpoint >= self.config.records_between_checkpoints {
            let (checkpoint, pruning) = self.compile_checkpoint_and_prune(compiler)?;
            Ok(KafkaResidentProcessV0 {
                process,
                checkpoint: Some(checkpoint),
                pruning,
            })
        } else {
            if self.compile_on_admission {
                self.context = compiler(self.session.admission(), self.session.frontier())
                    .map_err(KafkaResidentOperationErrorV0::Compiler)
                    .and_then(validate_operation_context)?;
                self.usage.contexts_compiled += 1;
            }
            Ok(KafkaResidentProcessV0 {
                process,
                checkpoint: None,
                pruning: None,
            })
        }
    }

    pub fn observe_idle<F>(
        &mut self,
        compiler: F,
    ) -> Result<Option<KafkaStaticSessionCheckpointV0>, KafkaResidentOperationErrorV0>
    where
        F: FnOnce(&AdmissionStateV0) -> Result<Vec<u8>, KafkaResidentCompilerErrorV0>,
    {
        let result = self.observe_idle_inner(|state, _| compiler(state));
        if result.is_err() && self.status == KafkaResidentStatusV0::Running {
            self.status = KafkaResidentStatusV0::Faulted;
        }
        result
    }

    fn observe_idle_inner<F>(
        &mut self,
        compiler: F,
    ) -> Result<Option<KafkaStaticSessionCheckpointV0>, KafkaResidentOperationErrorV0>
    where
        F: FnOnce(
            &AdmissionStateV0,
            &KafkaPartitionFrontierV0,
        ) -> Result<Vec<u8>, KafkaResidentCompilerErrorV0>,
    {
        self.ensure_running()?;
        self.idle_observations_since_checkpoint = checked_add(
            self.idle_observations_since_checkpoint,
            1,
            KafkaResidentAccountingFieldV0::IdleObservationsSinceCheckpoint,
        )?;
        if self.idle_observations_since_checkpoint
            < self.config.idle_observations_between_checkpoints
        {
            return Ok(None);
        }
        if !self.dirty {
            self.idle_observations_since_checkpoint = 0;
            return Ok(None);
        }
        let (checkpoint, _) = self.compile_checkpoint_and_prune(compiler)?;
        Ok(Some(checkpoint))
    }

    pub fn graceful_shutdown<F>(
        &mut self,
        compiler: F,
    ) -> Result<Option<KafkaStaticSessionCheckpointV0>, KafkaResidentOperationErrorV0>
    where
        F: FnOnce(&AdmissionStateV0) -> Result<Vec<u8>, KafkaResidentCompilerErrorV0>,
    {
        self.graceful_shutdown_inner(|state, _| compiler(state))
    }

    fn graceful_shutdown_inner<F>(
        &mut self,
        compiler: F,
    ) -> Result<Option<KafkaStaticSessionCheckpointV0>, KafkaResidentOperationErrorV0>
    where
        F: FnOnce(
            &AdmissionStateV0,
            &KafkaPartitionFrontierV0,
        ) -> Result<Vec<u8>, KafkaResidentCompilerErrorV0>,
    {
        match self.status {
            KafkaResidentStatusV0::Stopped => return Ok(None),
            KafkaResidentStatusV0::Faulted => return Err(KafkaResidentOperationErrorV0::Faulted),
            KafkaResidentStatusV0::Running => {}
        }
        let result = if !self.dirty {
            None
        } else {
            let (checkpoint, _) = match self.compile_checkpoint_and_prune(compiler) {
                Ok(value) => value,
                Err(error) => {
                    self.status = KafkaResidentStatusV0::Faulted;
                    return Err(error);
                }
            };
            Some(checkpoint)
        };
        self.status = KafkaResidentStatusV0::Stopped;
        Ok(result)
    }

    pub fn finalize_current_epoch<F>(
        &mut self,
        compiler: F,
    ) -> Result<FinalizeOutcomeV0, KafkaResidentOperationErrorV0>
    where
        F: FnOnce(&AdmissionStateV0) -> Result<Vec<u8>, KafkaResidentCompilerErrorV0>,
    {
        let result = self.finalize_current_epoch_inner(compiler);
        if result.is_err() && self.status == KafkaResidentStatusV0::Running {
            self.status = KafkaResidentStatusV0::Faulted;
        }
        result
    }

    fn finalize_current_epoch_inner<F>(
        &mut self,
        compiler: F,
    ) -> Result<FinalizeOutcomeV0, KafkaResidentOperationErrorV0>
    where
        F: FnOnce(&AdmissionStateV0) -> Result<Vec<u8>, KafkaResidentCompilerErrorV0>,
    {
        self.ensure_running()?;
        let source = self.session.admission_config.source_id().clone();
        let end = self.session.admission_config.epoch_end_ns();
        let mut next = self.session.admission.clone();
        let outcome = next.finalize_source(&source, end);
        let context = compiler(&next)
            .map_err(KafkaResidentOperationErrorV0::Compiler)
            .and_then(validate_operation_context)?;
        let contexts_compiled = checked_add(
            self.usage.contexts_compiled,
            1,
            KafkaResidentAccountingFieldV0::ContextsCompiled,
        )?;
        self.dirty |= next != self.session.admission;
        self.session.admission = next;
        self.context = context;
        self.usage.contexts_compiled = contexts_compiled;
        Ok(outcome)
    }

    pub fn rollover_adjacent_epoch<F>(
        &mut self,
        next_config: AdmissionConfigV0,
        compiler: F,
    ) -> Result<KafkaStaticSessionCheckpointV0, KafkaResidentOperationErrorV0>
    where
        F: FnOnce(&AdmissionStateV0) -> Result<Vec<u8>, KafkaResidentCompilerErrorV0>,
    {
        let result = self.rollover_adjacent_epoch_inner(next_config, compiler);
        if result.is_err() && self.status == KafkaResidentStatusV0::Running {
            self.status = KafkaResidentStatusV0::Faulted;
        }
        result
    }

    fn rollover_adjacent_epoch_inner<F>(
        &mut self,
        next_config: AdmissionConfigV0,
        compiler: F,
    ) -> Result<KafkaStaticSessionCheckpointV0, KafkaResidentOperationErrorV0>
    where
        F: FnOnce(&AdmissionStateV0) -> Result<Vec<u8>, KafkaResidentCompilerErrorV0>,
    {
        self.ensure_running()?;
        let next = self
            .session
            .admission
            .begin_adjacent_kafka_epoch_v0(next_config.clone())
            .map_err(KafkaResidentOperationErrorV0::EpochRollover)?;
        let context = compiler(&next)
            .map_err(KafkaResidentOperationErrorV0::Compiler)
            .and_then(validate_operation_context)?;
        let removed_identities = self.session.admission.usage().accepted_identities();
        let removed_event_bytes = self.session.admission.usage().accepted_event_bytes();
        self.preflight_maintenance(removed_identities, removed_event_bytes)?;
        let generation = self
            .session
            .next_generation
            .ok_or(KafkaResidentOperationErrorV0::GenerationExhausted)?;
        let candidate =
            publish_and_verify_local_kafka_checkpoint_commit_candidate_with_writer_lock_v0(
                &self.writer_lock,
                &self.store,
                generation,
                &next,
                &self.session.frontier,
                &next_config,
                &self.session.source_contract,
            )
            .map_err(KafkaResidentOperationErrorV0::Checkpoint)?;
        self.session.admission_config = next_config;
        self.session.admission = next;
        let checkpoint = self.install_candidate(generation, &candidate);
        self.context = context;
        self.records_since_checkpoint = 0;
        self.idle_observations_since_checkpoint = 0;
        self.dirty = false;
        self.usage.contexts_compiled += 1;
        self.usage.published_checkpoints += 1;
        self.usage.physically_removed_identities += removed_identities;
        self.usage.physically_removed_event_bytes += removed_event_bytes;
        self.prune_installed_checkpoint()?;
        Ok(checkpoint)
    }

    fn compile_checkpoint_and_prune<F>(
        &mut self,
        compiler: F,
    ) -> Result<
        (
            KafkaStaticSessionCheckpointV0,
            Option<LocalCheckpointPruneReceiptV0>,
        ),
        KafkaResidentOperationErrorV0,
    >
    where
        F: FnOnce(
            &AdmissionStateV0,
            &KafkaPartitionFrontierV0,
        ) -> Result<Vec<u8>, KafkaResidentCompilerErrorV0>,
    {
        let retention_view = self
            .session
            .admission
            .project_event_time_retention(self.config.physical_retention);
        let cutoff = retention_view.cutoff_event_time_ns();
        let expired = retention_view.usage().expired_events_excluded();
        let physical = if expired > 0 {
            let cutoff = cutoff.ok_or(KafkaResidentOperationErrorV0::PhysicalRetention(
                AdmissionPhysicalRetentionErrorV0::MissingWatermark,
            ))?;
            Some(
                self.session
                    .admission
                    .physically_retain_from_event_time_v0(cutoff)
                    .map_err(KafkaResidentOperationErrorV0::PhysicalRetention)?,
            )
        } else {
            None
        };
        let target = physical
            .as_ref()
            .map_or(&self.session.admission, |retention| {
                retention.retained_state()
            });
        let context = compiler(target, self.session.frontier())
            .map_err(KafkaResidentOperationErrorV0::Compiler)
            .and_then(validate_operation_context)?;
        self.preflight_maintenance(
            physical
                .as_ref()
                .map_or(0, |retention| retention.removed_identities()),
            physical
                .as_ref()
                .map_or(0, |retention| retention.removed_event_bytes()),
        )?;
        if !self.dirty && physical.is_none() {
            self.context = context;
            self.records_since_checkpoint = 0;
            self.idle_observations_since_checkpoint = 0;
            self.usage.contexts_compiled += 1;
            return Ok((
                KafkaStaticSessionCheckpointV0::Unchanged {
                    latest_generation: self.session.latest_generation,
                    latest_checkpoint_hash: self.session.latest_checkpoint_hash,
                },
                None,
            ));
        }

        let generation = self
            .session
            .next_generation
            .ok_or(KafkaResidentOperationErrorV0::GenerationExhausted)?;
        let candidate =
            publish_and_verify_local_kafka_checkpoint_commit_candidate_with_writer_lock_v0(
                &self.writer_lock,
                &self.store,
                generation,
                target,
                &self.session.frontier,
                &self.session.admission_config,
                &self.session.source_contract,
            )
            .map_err(KafkaResidentOperationErrorV0::Checkpoint)?;
        let removed_identities = physical
            .as_ref()
            .map_or(0, |retention| retention.removed_identities());
        let removed_event_bytes = physical
            .as_ref()
            .map_or(0, |retention| retention.removed_event_bytes());
        if let Some(physical) = physical {
            self.session.admission = physical.into_retained_state();
        }
        let checkpoint = self.install_candidate(generation, &candidate);
        self.context = context;
        self.records_since_checkpoint = 0;
        self.idle_observations_since_checkpoint = 0;
        self.dirty = false;
        self.usage.contexts_compiled += 1;
        self.usage.published_checkpoints += 1;
        self.usage.physically_removed_identities += removed_identities;
        self.usage.physically_removed_event_bytes += removed_event_bytes;
        let pruning = self.prune_installed_checkpoint()?;
        Ok((checkpoint, Some(pruning)))
    }

    fn install_candidate(
        &mut self,
        generation: decant_ingest::KafkaCheckpointGenerationV0,
        candidate: &decant_storage::LocalKafkaCheckpointCommitCandidateV0,
    ) -> KafkaStaticSessionCheckpointV0 {
        let checkpoint_hash = candidate.checkpoint_hash();
        self.session.latest_generation = Some(generation);
        self.session.latest_checkpoint_hash = Some(checkpoint_hash);
        self.session.next_generation = generation
            .get()
            .checked_add(1)
            .and_then(|value| decant_ingest::KafkaCheckpointGenerationV0::new(value).ok());
        self.session.advanced_since_checkpoint = 0;
        KafkaStaticSessionCheckpointV0::Published {
            generation,
            checkpoint_hash,
            artifact_bytes: candidate.artifact_bytes(),
            pending_entries: candidate.pending_entries(),
        }
    }

    fn prune_installed_checkpoint(
        &mut self,
    ) -> Result<LocalCheckpointPruneReceiptV0, KafkaResidentOperationErrorV0> {
        let pruning = prune_verified_local_kafka_checkpoints_v0(
            &self.writer_lock,
            &self.store,
            self.config.retained_checkpoint_generations,
            &self.session.admission_config,
            &self.session.source_contract,
        )
        .map_err(KafkaResidentOperationErrorV0::Prune)?;
        self.usage.pruned_checkpoint_generations = checked_add(
            self.usage.pruned_checkpoint_generations,
            pruning.removed_generations(),
            KafkaResidentAccountingFieldV0::PrunedCheckpointGenerations,
        )?;
        Ok(pruning)
    }

    fn preflight_maintenance(
        &self,
        removed_identities: usize,
        removed_event_bytes: usize,
    ) -> Result<(), KafkaResidentOperationErrorV0> {
        checked_add(
            self.usage.contexts_compiled,
            1,
            KafkaResidentAccountingFieldV0::ContextsCompiled,
        )?;
        checked_add(
            self.usage.published_checkpoints,
            1,
            KafkaResidentAccountingFieldV0::PublishedCheckpoints,
        )?;
        checked_add(
            self.usage.pruned_checkpoint_generations,
            decant_storage::MAX_LOCAL_CHECKPOINT_DIRECTORY_ENTRIES_V0,
            KafkaResidentAccountingFieldV0::PrunedCheckpointGenerations,
        )?;
        checked_add(
            self.usage.physically_removed_identities,
            removed_identities,
            KafkaResidentAccountingFieldV0::PhysicallyRemovedIdentities,
        )?;
        checked_add(
            self.usage.physically_removed_event_bytes,
            removed_event_bytes,
            KafkaResidentAccountingFieldV0::PhysicallyRemovedEventBytes,
        )?;
        Ok(())
    }

    fn ensure_running(&self) -> Result<(), KafkaResidentOperationErrorV0> {
        match self.status {
            KafkaResidentStatusV0::Running => Ok(()),
            KafkaResidentStatusV0::Faulted => Err(KafkaResidentOperationErrorV0::Faulted),
            KafkaResidentStatusV0::Stopped => Err(KafkaResidentOperationErrorV0::Stopped),
        }
    }
}

/// Cut-only compiler scheduling. Uncheckpointed state is never exposed as a
/// report. The callback must validate the entire bounded report before returning.
pub struct KafkaCheckpointCutResidentV1(KafkaResidentSessionV0);

impl KafkaCheckpointCutResidentV1 {
    pub fn bootstrap<F>(
        store: LocalCheckpointStoreV0,
        admission_config: AdmissionConfigV0,
        source_contract: KafkaSourceContractV0,
        starting_positions: &[decant_ir::KafkaPositionV0],
        config: KafkaResidentLifecycleConfigV0,
        compiler: F,
    ) -> Result<Self, KafkaResidentBootstrapErrorV0>
    where
        F: FnOnce(
            &AdmissionStateV0,
            &KafkaPartitionFrontierV0,
        ) -> Result<Vec<u8>, KafkaResidentCompilerErrorV0>,
    {
        KafkaResidentSessionV0::bootstrap_inner(
            store,
            admission_config,
            source_contract,
            starting_positions,
            config,
            false,
            compiler,
        )
        .map(Self)
    }

    pub fn context(&self) -> Option<&KafkaResidentContextV0> {
        self.0.session.latest_generation()?;
        self.0.context()
    }
    pub fn session(&self) -> &KafkaStaticCheckpointSessionV0 {
        self.0.session()
    }
    pub fn usage(&self) -> KafkaResidentUsageV0 {
        self.0.usage()
    }
    pub fn records_since_checkpoint(&self) -> usize {
        self.0.records_since_checkpoint()
    }
    pub fn status(&self) -> KafkaResidentStatusV0 {
        self.0.status()
    }

    pub fn process_json_object_record_v1<F>(
        &mut self,
        mapping: &KafkaJsonObjectMappingV1,
        record: &KafkaFetchedRecordV0,
        compiler: F,
    ) -> Result<KafkaResidentProcessV0, KafkaResidentOperationErrorV0>
    where
        F: FnOnce(
            &AdmissionStateV0,
            &KafkaPartitionFrontierV0,
        ) -> Result<Vec<u8>, KafkaResidentCompilerErrorV0>,
    {
        let result = self
            .0
            .process_json_object_record_v1_inner(mapping, record, compiler);
        self.fault_on_error(&result);
        result
    }
    pub fn observe_idle<F>(
        &mut self,
        compiler: F,
    ) -> Result<Option<KafkaStaticSessionCheckpointV0>, KafkaResidentOperationErrorV0>
    where
        F: FnOnce(
            &AdmissionStateV0,
            &KafkaPartitionFrontierV0,
        ) -> Result<Vec<u8>, KafkaResidentCompilerErrorV0>,
    {
        let result = self.0.observe_idle_inner(compiler);
        self.fault_on_error(&result);
        result
    }
    pub fn graceful_shutdown<F>(
        &mut self,
        compiler: F,
    ) -> Result<Option<KafkaStaticSessionCheckpointV0>, KafkaResidentOperationErrorV0>
    where
        F: FnOnce(
            &AdmissionStateV0,
            &KafkaPartitionFrontierV0,
        ) -> Result<Vec<u8>, KafkaResidentCompilerErrorV0>,
    {
        let result = self.0.graceful_shutdown_inner(compiler);
        self.fault_on_error(&result);
        result
    }
    fn fault_on_error<T>(&mut self, result: &Result<T, KafkaResidentOperationErrorV0>) {
        if result.is_err() && self.0.status == KafkaResidentStatusV0::Running {
            self.0.status = KafkaResidentStatusV0::Faulted;
        }
    }
}

impl fmt::Debug for KafkaResidentSessionV0 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("KafkaResidentSessionV0")
            .field("config", &self.config)
            .field("context", &self.context)
            .field("records_since_checkpoint", &self.records_since_checkpoint)
            .field(
                "idle_observations_since_checkpoint",
                &self.idle_observations_since_checkpoint,
            )
            .field("usage", &self.usage)
            .field("dirty", &self.dirty)
            .field("status", &self.status)
            .field("store", &"<redacted>")
            .field("session", &"<redacted>")
            .finish()
    }
}

fn validate_bound(
    actual: usize,
    maximum: usize,
    field: KafkaResidentLifecycleFieldV0,
) -> Result<(), KafkaResidentLifecycleConfigErrorV0> {
    if actual == 0 {
        return Err(KafkaResidentLifecycleConfigErrorV0::Zero { field });
    }
    if actual > maximum {
        return Err(KafkaResidentLifecycleConfigErrorV0::AboveMaximum {
            field,
            maximum,
            actual,
        });
    }
    Ok(())
}

fn validate_bootstrap_context(
    bytes: Vec<u8>,
) -> Result<KafkaResidentContextV0, KafkaResidentBootstrapErrorV0> {
    if bytes.is_empty() {
        return Err(KafkaResidentBootstrapErrorV0::EmptyContext);
    }
    if bytes.len() > MAX_KAFKA_RESIDENT_CONTEXT_BYTES_V0 {
        return Err(KafkaResidentBootstrapErrorV0::ContextTooLarge {
            maximum: MAX_KAFKA_RESIDENT_CONTEXT_BYTES_V0,
            actual: bytes.len(),
        });
    }
    Ok(KafkaResidentContextV0 { bytes })
}

fn validate_operation_context(
    bytes: Vec<u8>,
) -> Result<KafkaResidentContextV0, KafkaResidentOperationErrorV0> {
    if bytes.is_empty() {
        return Err(KafkaResidentOperationErrorV0::EmptyContext);
    }
    if bytes.len() > MAX_KAFKA_RESIDENT_CONTEXT_BYTES_V0 {
        return Err(KafkaResidentOperationErrorV0::ContextTooLarge {
            maximum: MAX_KAFKA_RESIDENT_CONTEXT_BYTES_V0,
            actual: bytes.len(),
        });
    }
    Ok(KafkaResidentContextV0 { bytes })
}

fn checked_add(
    current: usize,
    added: usize,
    field: KafkaResidentAccountingFieldV0,
) -> Result<usize, KafkaResidentOperationErrorV0> {
    current
        .checked_add(added)
        .ok_or(KafkaResidentOperationErrorV0::Accounting { field })
}
