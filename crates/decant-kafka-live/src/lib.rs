//! Client-neutral orchestration for one finite, statically assigned Kafka session.

#![forbid(unsafe_code)]

mod resident;

use std::error::Error;
use std::fmt;

use decant_ingest::{
    KafkaCanonicalAdmissionOutcomeV0, KafkaCheckpointGenerationErrorV0,
    KafkaCheckpointGenerationV0, KafkaIngestionCheckpointDecodeErrorV0,
    KafkaIngestionCheckpointHashV0, KafkaJsonIngestionErrorV0, KafkaJsonIngestionOutcomeV0,
    KafkaJsonObjectIngestionErrorV0, KafkaJsonObjectIngestionErrorV1,
    ingest_decant_json_kafka_record_v0, ingest_json_object_kafka_record_v0,
    ingest_json_object_kafka_record_v1, validate_kafka_ingestion_checkpoint_expected_contract_v0,
};
use decant_ir::KafkaPositionV0;
use decant_kafka::{
    KafkaFetchedRecordV0, KafkaJsonObjectMappingV0, KafkaJsonObjectMappingV1,
    KafkaPartitionFrontierErrorV0, KafkaPartitionFrontierV0, KafkaRecordTimestampKindV0,
    KafkaSourceContractV0,
};
use decant_state::{AdmissionConfigV0, AdmissionStateV0, ApplyOutcomeV0, RejectionReasonV0};
use decant_storage::{
    LocalCheckpointCatalogErrorV0, LocalCheckpointRecoveryErrorV0, LocalCheckpointStoreV0,
    VerifiedLocalKafkaCheckpointPublicationErrorV0,
    publish_and_verify_local_kafka_checkpoint_commit_candidate_v0,
    restore_latest_local_kafka_checkpoint_generation_v0,
};

pub use resident::{
    KAFKA_RESIDENT_LIFECYCLE_VERSION_V0, KafkaCheckpointCutResidentV1,
    KafkaResidentAccountingFieldV0, KafkaResidentBootstrapErrorV0, KafkaResidentCompilerErrorV0,
    KafkaResidentContextV0, KafkaResidentLifecycleConfigErrorV0, KafkaResidentLifecycleConfigV0,
    KafkaResidentLifecycleFieldV0, KafkaResidentOperationErrorV0, KafkaResidentProcessV0,
    KafkaResidentSessionV0, KafkaResidentStatusV0, KafkaResidentUsageV0,
    MAX_KAFKA_RESIDENT_CONTEXT_BYTES_V0,
    MAX_KAFKA_RESIDENT_IDLE_OBSERVATIONS_BETWEEN_CHECKPOINTS_V0,
    MAX_KAFKA_RESIDENT_RECORDS_BETWEEN_CHECKPOINTS_V0,
    MAX_KAFKA_RESIDENT_RETAINED_CHECKPOINT_GENERATIONS_V0,
};

pub const KAFKA_STATIC_CHECKPOINT_SESSION_VERSION_V0: u16 = 0;

#[derive(Clone, Eq, PartialEq)]
pub enum KafkaStaticSessionBootstrapErrorV0 {
    ExpectedContract(KafkaIngestionCheckpointDecodeErrorV0),
    ConfiguredFrontier(KafkaPartitionFrontierErrorV0),
    Catalog(LocalCheckpointCatalogErrorV0),
    PendingWithoutPublished { pending_entries: usize },
    Recovery(LocalCheckpointRecoveryErrorV0),
    PartitionSetMismatch,
    Generation(KafkaCheckpointGenerationErrorV0),
    GenerationExhausted { latest: KafkaCheckpointGenerationV0 },
}

impl fmt::Debug for KafkaStaticSessionBootstrapErrorV0 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ExpectedContract(error) => formatter
                .debug_tuple("ExpectedContract")
                .field(error)
                .finish(),
            Self::ConfiguredFrontier(error) => formatter
                .debug_tuple("ConfiguredFrontier")
                .field(error)
                .finish(),
            Self::Catalog(error) => formatter.debug_tuple("Catalog").field(error).finish(),
            Self::PendingWithoutPublished { pending_entries } => formatter
                .debug_struct("PendingWithoutPublished")
                .field("pending_entries", pending_entries)
                .finish(),
            Self::Recovery(error) => formatter.debug_tuple("Recovery").field(error).finish(),
            Self::PartitionSetMismatch => formatter.write_str("PartitionSetMismatch"),
            Self::Generation(error) => formatter.debug_tuple("Generation").field(error).finish(),
            Self::GenerationExhausted { latest } => formatter
                .debug_struct("GenerationExhausted")
                .field("latest", latest)
                .finish(),
        }
    }
}

impl fmt::Display for KafkaStaticSessionBootstrapErrorV0 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(self, formatter)
    }
}

impl Error for KafkaStaticSessionBootstrapErrorV0 {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::ExpectedContract(error) => Some(error),
            Self::ConfiguredFrontier(error) => Some(error),
            Self::Catalog(error) => Some(error),
            Self::Recovery(error) => Some(error),
            Self::Generation(error) => Some(error),
            Self::PendingWithoutPublished { .. }
            | Self::PartitionSetMismatch
            | Self::GenerationExhausted { .. } => None,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum KafkaStaticSessionAccountingFieldV0 {
    ProcessedRecords,
    AdvancedRecords,
    AcceptedRecords,
    DuplicateNoOpRecords,
    RejectedRecords,
    BeforeFrontierRecords,
    OmittedHeaderCount,
    OmittedHeaderBytes,
    OmittedTimestampNotAvailableRecords,
    OmittedTimestampCreateTimeRecords,
    OmittedTimestampLogAppendTimeRecords,
    AdvancedSinceCheckpoint,
}

#[derive(Clone, Eq, PartialEq)]
pub enum KafkaStaticSessionProcessErrorV0 {
    Accounting {
        field: KafkaStaticSessionAccountingFieldV0,
    },
    Ingestion(KafkaJsonIngestionErrorV0),
    JsonObjectIngestion(KafkaJsonObjectIngestionErrorV0),
    JsonObjectIngestionV1(KafkaJsonObjectIngestionErrorV1),
    Gap,
}

impl fmt::Debug for KafkaStaticSessionProcessErrorV0 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Accounting { field } => formatter
                .debug_struct("Accounting")
                .field("field", field)
                .finish(),
            Self::Ingestion(error) => formatter.debug_tuple("Ingestion").field(error).finish(),
            Self::JsonObjectIngestion(error) => formatter
                .debug_tuple("JsonObjectIngestion")
                .field(error)
                .finish(),
            Self::JsonObjectIngestionV1(error) => formatter
                .debug_tuple("JsonObjectIngestionV1")
                .field(error)
                .finish(),
            Self::Gap => formatter.write_str("Gap"),
        }
    }
}

impl fmt::Display for KafkaStaticSessionProcessErrorV0 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(self, formatter)
    }
}

impl Error for KafkaStaticSessionProcessErrorV0 {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Ingestion(error) => Some(error),
            Self::JsonObjectIngestion(error) => Some(error),
            Self::JsonObjectIngestionV1(error) => Some(error),
            Self::Accounting { .. } | Self::Gap => None,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum KafkaStaticSessionProcessV0 {
    Accepted,
    DuplicateNoOp,
    Rejected { reason: RejectionReasonV0 },
    BeforeFrontier,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct KafkaStaticSessionUsageV0 {
    processed_records: usize,
    advanced_records: usize,
    accepted_records: usize,
    duplicate_noop_records: usize,
    rejected_records: usize,
    before_frontier_records: usize,
    omitted_header_count: usize,
    omitted_header_bytes: usize,
    omitted_timestamp_not_available_records: usize,
    omitted_timestamp_create_time_records: usize,
    omitted_timestamp_log_append_time_records: usize,
}

impl KafkaStaticSessionUsageV0 {
    pub const fn processed_records(self) -> usize {
        self.processed_records
    }

    pub const fn advanced_records(self) -> usize {
        self.advanced_records
    }

    pub const fn accepted_records(self) -> usize {
        self.accepted_records
    }

    pub const fn duplicate_noop_records(self) -> usize {
        self.duplicate_noop_records
    }

    pub const fn rejected_records(self) -> usize {
        self.rejected_records
    }

    pub const fn before_frontier_records(self) -> usize {
        self.before_frontier_records
    }

    pub const fn omitted_header_count(self) -> usize {
        self.omitted_header_count
    }

    pub const fn omitted_header_bytes(self) -> usize {
        self.omitted_header_bytes
    }

    pub const fn omitted_timestamp_not_available_records(self) -> usize {
        self.omitted_timestamp_not_available_records
    }

    pub const fn omitted_timestamp_create_time_records(self) -> usize {
        self.omitted_timestamp_create_time_records
    }

    pub const fn omitted_timestamp_log_append_time_records(self) -> usize {
        self.omitted_timestamp_log_append_time_records
    }

    fn preflight(
        self,
        record: &KafkaFetchedRecordV0,
    ) -> Result<(), KafkaStaticSessionProcessErrorV0> {
        checked_add(
            self.processed_records,
            1,
            KafkaStaticSessionAccountingFieldV0::ProcessedRecords,
        )?;
        checked_add(
            self.advanced_records,
            1,
            KafkaStaticSessionAccountingFieldV0::AdvancedRecords,
        )?;
        checked_add(
            self.accepted_records,
            1,
            KafkaStaticSessionAccountingFieldV0::AcceptedRecords,
        )?;
        checked_add(
            self.duplicate_noop_records,
            1,
            KafkaStaticSessionAccountingFieldV0::DuplicateNoOpRecords,
        )?;
        checked_add(
            self.rejected_records,
            1,
            KafkaStaticSessionAccountingFieldV0::RejectedRecords,
        )?;
        checked_add(
            self.before_frontier_records,
            1,
            KafkaStaticSessionAccountingFieldV0::BeforeFrontierRecords,
        )?;
        checked_add(
            self.omitted_header_count,
            record.headers().len(),
            KafkaStaticSessionAccountingFieldV0::OmittedHeaderCount,
        )?;
        checked_add(
            self.omitted_header_bytes,
            record.header_bytes(),
            KafkaStaticSessionAccountingFieldV0::OmittedHeaderBytes,
        )?;
        let (current, field) = self.timestamp_counter(record.timestamp().kind());
        checked_add(current, 1, field)?;
        Ok(())
    }

    fn with_outcome(
        self,
        outcome: KafkaStaticSessionProcessV0,
        header_count: usize,
        header_bytes: usize,
        timestamp_kind: KafkaRecordTimestampKindV0,
    ) -> Result<Self, KafkaStaticSessionProcessErrorV0> {
        let mut next = self;
        next.processed_records = checked_add(
            next.processed_records,
            1,
            KafkaStaticSessionAccountingFieldV0::ProcessedRecords,
        )?;
        match outcome {
            KafkaStaticSessionProcessV0::Accepted => {
                next.advanced_records = checked_add(
                    next.advanced_records,
                    1,
                    KafkaStaticSessionAccountingFieldV0::AdvancedRecords,
                )?;
                next.accepted_records = checked_add(
                    next.accepted_records,
                    1,
                    KafkaStaticSessionAccountingFieldV0::AcceptedRecords,
                )?;
            }
            KafkaStaticSessionProcessV0::DuplicateNoOp => {
                next.advanced_records = checked_add(
                    next.advanced_records,
                    1,
                    KafkaStaticSessionAccountingFieldV0::AdvancedRecords,
                )?;
                next.duplicate_noop_records = checked_add(
                    next.duplicate_noop_records,
                    1,
                    KafkaStaticSessionAccountingFieldV0::DuplicateNoOpRecords,
                )?;
            }
            KafkaStaticSessionProcessV0::Rejected { .. } => {
                next.advanced_records = checked_add(
                    next.advanced_records,
                    1,
                    KafkaStaticSessionAccountingFieldV0::AdvancedRecords,
                )?;
                next.rejected_records = checked_add(
                    next.rejected_records,
                    1,
                    KafkaStaticSessionAccountingFieldV0::RejectedRecords,
                )?;
            }
            KafkaStaticSessionProcessV0::BeforeFrontier => {
                next.before_frontier_records = checked_add(
                    next.before_frontier_records,
                    1,
                    KafkaStaticSessionAccountingFieldV0::BeforeFrontierRecords,
                )?;
            }
        }
        next.omitted_header_count = checked_add(
            next.omitted_header_count,
            header_count,
            KafkaStaticSessionAccountingFieldV0::OmittedHeaderCount,
        )?;
        next.omitted_header_bytes = checked_add(
            next.omitted_header_bytes,
            header_bytes,
            KafkaStaticSessionAccountingFieldV0::OmittedHeaderBytes,
        )?;
        let (counter, field) = next.timestamp_counter_mut(timestamp_kind);
        *counter = checked_add(*counter, 1, field)?;
        Ok(next)
    }

    fn timestamp_counter(
        self,
        kind: KafkaRecordTimestampKindV0,
    ) -> (usize, KafkaStaticSessionAccountingFieldV0) {
        match kind {
            KafkaRecordTimestampKindV0::NotAvailable => (
                self.omitted_timestamp_not_available_records,
                KafkaStaticSessionAccountingFieldV0::OmittedTimestampNotAvailableRecords,
            ),
            KafkaRecordTimestampKindV0::CreateTime => (
                self.omitted_timestamp_create_time_records,
                KafkaStaticSessionAccountingFieldV0::OmittedTimestampCreateTimeRecords,
            ),
            KafkaRecordTimestampKindV0::LogAppendTime => (
                self.omitted_timestamp_log_append_time_records,
                KafkaStaticSessionAccountingFieldV0::OmittedTimestampLogAppendTimeRecords,
            ),
        }
    }

    fn timestamp_counter_mut(
        &mut self,
        kind: KafkaRecordTimestampKindV0,
    ) -> (&mut usize, KafkaStaticSessionAccountingFieldV0) {
        match kind {
            KafkaRecordTimestampKindV0::NotAvailable => (
                &mut self.omitted_timestamp_not_available_records,
                KafkaStaticSessionAccountingFieldV0::OmittedTimestampNotAvailableRecords,
            ),
            KafkaRecordTimestampKindV0::CreateTime => (
                &mut self.omitted_timestamp_create_time_records,
                KafkaStaticSessionAccountingFieldV0::OmittedTimestampCreateTimeRecords,
            ),
            KafkaRecordTimestampKindV0::LogAppendTime => (
                &mut self.omitted_timestamp_log_append_time_records,
                KafkaStaticSessionAccountingFieldV0::OmittedTimestampLogAppendTimeRecords,
            ),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum KafkaStaticSessionCheckpointV0 {
    Unchanged {
        latest_generation: Option<KafkaCheckpointGenerationV0>,
        latest_checkpoint_hash: Option<KafkaIngestionCheckpointHashV0>,
    },
    Published {
        generation: KafkaCheckpointGenerationV0,
        checkpoint_hash: KafkaIngestionCheckpointHashV0,
        artifact_bytes: usize,
        pending_entries: usize,
    },
}

impl KafkaStaticSessionCheckpointV0 {
    pub const fn was_published(self) -> bool {
        matches!(self, Self::Published { .. })
    }

    pub const fn generation(self) -> Option<KafkaCheckpointGenerationV0> {
        match self {
            Self::Unchanged {
                latest_generation, ..
            } => latest_generation,
            Self::Published { generation, .. } => Some(generation),
        }
    }

    pub const fn checkpoint_hash(self) -> Option<KafkaIngestionCheckpointHashV0> {
        match self {
            Self::Unchanged {
                latest_checkpoint_hash,
                ..
            } => latest_checkpoint_hash,
            Self::Published {
                checkpoint_hash, ..
            } => Some(checkpoint_hash),
        }
    }
}

#[derive(Clone, Eq, PartialEq)]
pub enum KafkaStaticSessionCheckpointErrorV0 {
    GenerationExhausted,
    Publication(VerifiedLocalKafkaCheckpointPublicationErrorV0),
}

impl fmt::Debug for KafkaStaticSessionCheckpointErrorV0 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::GenerationExhausted => formatter.write_str("GenerationExhausted"),
            Self::Publication(error) => formatter.debug_tuple("Publication").field(error).finish(),
        }
    }
}

impl fmt::Display for KafkaStaticSessionCheckpointErrorV0 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(self, formatter)
    }
}

impl Error for KafkaStaticSessionCheckpointErrorV0 {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Publication(error) => Some(error),
            Self::GenerationExhausted => None,
        }
    }
}

pub struct KafkaStaticCheckpointSessionV0 {
    admission_config: AdmissionConfigV0,
    source_contract: KafkaSourceContractV0,
    admission: AdmissionStateV0,
    frontier: KafkaPartitionFrontierV0,
    usage: KafkaStaticSessionUsageV0,
    advanced_since_checkpoint: usize,
    latest_generation: Option<KafkaCheckpointGenerationV0>,
    latest_checkpoint_hash: Option<KafkaIngestionCheckpointHashV0>,
    next_generation: Option<KafkaCheckpointGenerationV0>,
}

impl KafkaStaticCheckpointSessionV0 {
    pub fn bootstrap(
        store: &LocalCheckpointStoreV0,
        admission_config: AdmissionConfigV0,
        source_contract: KafkaSourceContractV0,
        configured_starting_positions: &[KafkaPositionV0],
    ) -> Result<Self, KafkaStaticSessionBootstrapErrorV0> {
        validate_kafka_ingestion_checkpoint_expected_contract_v0(
            &admission_config,
            &source_contract,
        )
        .map_err(KafkaStaticSessionBootstrapErrorV0::ExpectedContract)?;
        let configured_frontier = KafkaPartitionFrontierV0::new(
            0,
            source_contract.clone(),
            configured_starting_positions,
        )
        .map_err(KafkaStaticSessionBootstrapErrorV0::ConfiguredFrontier)?;
        let catalog = store
            .catalog()
            .map_err(KafkaStaticSessionBootstrapErrorV0::Catalog)?;

        if catalog.latest_published().is_none() {
            if !catalog.pending().is_empty() {
                return Err(
                    KafkaStaticSessionBootstrapErrorV0::PendingWithoutPublished {
                        pending_entries: catalog.pending().len(),
                    },
                );
            }
            let next_generation = KafkaCheckpointGenerationV0::new(1)
                .map_err(KafkaStaticSessionBootstrapErrorV0::Generation)?;
            return Ok(Self {
                admission: AdmissionStateV0::new(admission_config.clone()),
                admission_config,
                source_contract,
                frontier: configured_frontier,
                usage: KafkaStaticSessionUsageV0::default(),
                advanced_since_checkpoint: 0,
                latest_generation: None,
                latest_checkpoint_hash: None,
                next_generation: Some(next_generation),
            });
        }

        let recovery = restore_latest_local_kafka_checkpoint_generation_v0(
            store,
            &admission_config,
            &source_contract,
        )
        .map_err(KafkaStaticSessionBootstrapErrorV0::Recovery)?;
        if !same_partition_set(recovery.frontier(), &configured_frontier) {
            return Err(KafkaStaticSessionBootstrapErrorV0::PartitionSetMismatch);
        }
        let latest_generation = recovery.generation();
        let next_value = latest_generation.get().checked_add(1).ok_or(
            KafkaStaticSessionBootstrapErrorV0::GenerationExhausted {
                latest: latest_generation,
            },
        )?;
        let next_generation = KafkaCheckpointGenerationV0::new(next_value)
            .map_err(KafkaStaticSessionBootstrapErrorV0::Generation)?;
        let latest_checkpoint_hash = recovery.checkpoint_hash();
        let (admission, frontier) = recovery.into_parts();
        Ok(Self {
            admission_config,
            source_contract,
            admission,
            frontier,
            usage: KafkaStaticSessionUsageV0::default(),
            advanced_since_checkpoint: 0,
            latest_generation: Some(latest_generation),
            latest_checkpoint_hash: Some(latest_checkpoint_hash),
            next_generation: Some(next_generation),
        })
    }

    pub const fn admission(&self) -> &AdmissionStateV0 {
        &self.admission
    }

    pub const fn frontier(&self) -> &KafkaPartitionFrontierV0 {
        &self.frontier
    }

    pub const fn usage(&self) -> KafkaStaticSessionUsageV0 {
        self.usage
    }

    pub const fn advanced_since_checkpoint(&self) -> usize {
        self.advanced_since_checkpoint
    }

    pub const fn latest_generation(&self) -> Option<KafkaCheckpointGenerationV0> {
        self.latest_generation
    }

    pub const fn next_generation(&self) -> Option<KafkaCheckpointGenerationV0> {
        self.next_generation
    }

    pub fn process_record(
        &mut self,
        record: &KafkaFetchedRecordV0,
    ) -> Result<KafkaStaticSessionProcessV0, KafkaStaticSessionProcessErrorV0> {
        self.preflight_record(record)?;
        let outcome = ingest_decant_json_kafka_record_v0(
            &mut self.frontier,
            &mut self.admission,
            self.source_contract.source_id(),
            self.source_contract.cluster_id(),
            self.source_contract.topic_id(),
            record,
        )
        .map_err(KafkaStaticSessionProcessErrorV0::Ingestion)?;
        self.finish_record(outcome)
    }

    pub fn process_json_object_record(
        &mut self,
        mapping: &KafkaJsonObjectMappingV0,
        record: &KafkaFetchedRecordV0,
    ) -> Result<KafkaStaticSessionProcessV0, KafkaStaticSessionProcessErrorV0> {
        self.preflight_record(record)?;
        let outcome = ingest_json_object_kafka_record_v0(
            mapping,
            &mut self.frontier,
            &mut self.admission,
            self.source_contract.source_id(),
            self.source_contract.cluster_id(),
            self.source_contract.topic_id(),
            record,
        )
        .map_err(KafkaStaticSessionProcessErrorV0::JsonObjectIngestion)?;
        self.finish_record(outcome)
    }

    pub fn process_json_object_record_v1(
        &mut self,
        mapping: &KafkaJsonObjectMappingV1,
        record: &KafkaFetchedRecordV0,
    ) -> Result<KafkaStaticSessionProcessV0, KafkaStaticSessionProcessErrorV0> {
        self.preflight_record(record)?;
        let outcome = ingest_json_object_kafka_record_v1(
            mapping,
            &mut self.frontier,
            &mut self.admission,
            self.source_contract.source_id(),
            self.source_contract.cluster_id(),
            self.source_contract.topic_id(),
            record,
        )
        .map_err(KafkaStaticSessionProcessErrorV0::JsonObjectIngestionV1)?;
        self.finish_record(outcome)
    }

    fn preflight_record(
        &self,
        record: &KafkaFetchedRecordV0,
    ) -> Result<(), KafkaStaticSessionProcessErrorV0> {
        self.usage.preflight(record)?;
        checked_add(
            self.advanced_since_checkpoint,
            1,
            KafkaStaticSessionAccountingFieldV0::AdvancedSinceCheckpoint,
        )?;
        Ok(())
    }

    fn finish_record(
        &mut self,
        outcome: KafkaJsonIngestionOutcomeV0,
    ) -> Result<KafkaStaticSessionProcessV0, KafkaStaticSessionProcessErrorV0> {
        let process = match outcome.admission() {
            KafkaCanonicalAdmissionOutcomeV0::Advanced {
                admission: ApplyOutcomeV0::Accepted,
            } => KafkaStaticSessionProcessV0::Accepted,
            KafkaCanonicalAdmissionOutcomeV0::Advanced {
                admission: ApplyOutcomeV0::DuplicateNoOp,
            } => KafkaStaticSessionProcessV0::DuplicateNoOp,
            KafkaCanonicalAdmissionOutcomeV0::Advanced {
                admission: ApplyOutcomeV0::Rejected(reason),
            } => KafkaStaticSessionProcessV0::Rejected { reason },
            KafkaCanonicalAdmissionOutcomeV0::BeforeFrontier => {
                KafkaStaticSessionProcessV0::BeforeFrontier
            }
            KafkaCanonicalAdmissionOutcomeV0::Gap => {
                return Err(KafkaStaticSessionProcessErrorV0::Gap);
            }
        };
        let omission = outcome.omitted_transport_metadata();
        self.usage = self.usage.with_outcome(
            process,
            omission.header_count(),
            omission.header_bytes(),
            omission.timestamp_kind(),
        )?;
        if !matches!(process, KafkaStaticSessionProcessV0::BeforeFrontier) {
            self.advanced_since_checkpoint = checked_add(
                self.advanced_since_checkpoint,
                1,
                KafkaStaticSessionAccountingFieldV0::AdvancedSinceCheckpoint,
            )?;
        }
        Ok(process)
    }

    pub fn checkpoint(
        &mut self,
        store: &LocalCheckpointStoreV0,
    ) -> Result<KafkaStaticSessionCheckpointV0, KafkaStaticSessionCheckpointErrorV0> {
        if self.advanced_since_checkpoint == 0 {
            return Ok(KafkaStaticSessionCheckpointV0::Unchanged {
                latest_generation: self.latest_generation,
                latest_checkpoint_hash: self.latest_checkpoint_hash,
            });
        }
        let generation = self
            .next_generation
            .ok_or(KafkaStaticSessionCheckpointErrorV0::GenerationExhausted)?;
        let candidate = publish_and_verify_local_kafka_checkpoint_commit_candidate_v0(
            store,
            generation,
            &self.admission,
            &self.frontier,
            &self.admission_config,
            &self.source_contract,
        )
        .map_err(KafkaStaticSessionCheckpointErrorV0::Publication)?;
        let checkpoint_hash = candidate.checkpoint_hash();
        let artifact_bytes = candidate.artifact_bytes();
        let pending_entries = candidate.pending_entries();
        self.latest_generation = Some(generation);
        self.latest_checkpoint_hash = Some(checkpoint_hash);
        self.next_generation = generation
            .get()
            .checked_add(1)
            .and_then(|value| KafkaCheckpointGenerationV0::new(value).ok());
        self.advanced_since_checkpoint = 0;
        Ok(KafkaStaticSessionCheckpointV0::Published {
            generation,
            checkpoint_hash,
            artifact_bytes,
            pending_entries,
        })
    }
}

impl fmt::Debug for KafkaStaticCheckpointSessionV0 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("KafkaStaticCheckpointSessionV0")
            .field("version", &KAFKA_STATIC_CHECKPOINT_SESSION_VERSION_V0)
            .field("usage", &self.usage)
            .field("advanced_since_checkpoint", &self.advanced_since_checkpoint)
            .field("latest_generation", &self.latest_generation)
            .field("next_generation", &self.next_generation)
            .field("admission", &"<redacted>")
            .field("frontier", &"<redacted>")
            .finish()
    }
}

fn same_partition_set(
    recovered: &KafkaPartitionFrontierV0,
    configured: &KafkaPartitionFrontierV0,
) -> bool {
    recovered.assignment_count() == configured.assignment_count()
        && recovered.iter().zip(configured.iter()).all(
            |((recovered_partition, _), (configured_partition, _))| {
                recovered_partition == configured_partition
            },
        )
}

fn checked_add(
    current: usize,
    added: usize,
    field: KafkaStaticSessionAccountingFieldV0,
) -> Result<usize, KafkaStaticSessionProcessErrorV0> {
    current
        .checked_add(added)
        .ok_or(KafkaStaticSessionProcessErrorV0::Accounting { field })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn duplicate_noop_has_an_explicit_checked_accounting_path() {
        let usage = KafkaStaticSessionUsageV0::default()
            .with_outcome(
                KafkaStaticSessionProcessV0::DuplicateNoOp,
                2,
                19,
                KafkaRecordTimestampKindV0::CreateTime,
            )
            .unwrap();
        assert_eq!(usage.processed_records(), 1);
        assert_eq!(usage.advanced_records(), 1);
        assert_eq!(usage.duplicate_noop_records(), 1);
        assert_eq!(usage.omitted_header_count(), 2);
        assert_eq!(usage.omitted_header_bytes(), 19);
        assert_eq!(usage.omitted_timestamp_create_time_records(), 1);
    }
}
