//! Strict recovery of the greatest published local checkpoint generation.

use std::error::Error;
use std::fmt;
use std::fs::File;
use std::io::{self, Read};

use decant_ingest::{
    KafkaCheckpointGenerationArtifactDecodeErrorV0, KafkaCheckpointGenerationRestoreV0,
    KafkaCheckpointGenerationV0, KafkaIngestionCheckpointDecodeErrorV0,
    KafkaIngestionCheckpointHashV0, MAX_KAFKA_CHECKPOINT_GENERATION_ARTIFACT_BYTES_V0,
    restore_kafka_checkpoint_generation_artifact_v0,
    validate_kafka_ingestion_checkpoint_expected_contract_v0,
};
use decant_kafka::{KafkaPartitionFrontierV0, KafkaSourceContractV0};
use decant_state::{AdmissionConfigV0, AdmissionStateV0};

use crate::{LocalCheckpointCatalogErrorV0, LocalCheckpointStoreV0, published_file_name_v0};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum LocalCheckpointRecoveryIoOperationV0 {
    OpenPublished,
    InspectPublished,
    ReadPublished,
}

#[derive(Clone, Eq, PartialEq)]
pub enum LocalCheckpointRecoveryErrorV0 {
    ExpectedContract(KafkaIngestionCheckpointDecodeErrorV0),
    Catalog(LocalCheckpointCatalogErrorV0),
    NoPublishedGeneration {
        pending_entries: usize,
    },
    Io {
        operation: LocalCheckpointRecoveryIoOperationV0,
        kind: io::ErrorKind,
    },
    OpenedArtifactNotRegularFile {
        generation: KafkaCheckpointGenerationV0,
    },
    ArtifactLengthChanged {
        generation: KafkaCheckpointGenerationV0,
        catalog_bytes: u64,
        observed_bytes: u64,
    },
    ArtifactLengthOverflow {
        generation: KafkaCheckpointGenerationV0,
    },
    AllocationRefused {
        requested: usize,
    },
    ArtifactExceededReadBound {
        generation: KafkaCheckpointGenerationV0,
        maximum: usize,
    },
    ArtifactDecode(KafkaCheckpointGenerationArtifactDecodeErrorV0),
}

impl fmt::Debug for LocalCheckpointRecoveryErrorV0 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ExpectedContract(error) => formatter
                .debug_tuple("ExpectedContract")
                .field(error)
                .finish(),
            Self::Catalog(error) => formatter.debug_tuple("Catalog").field(error).finish(),
            Self::NoPublishedGeneration { pending_entries } => formatter
                .debug_struct("NoPublishedGeneration")
                .field("pending_entries", pending_entries)
                .finish(),
            Self::Io { operation, kind } => formatter
                .debug_struct("Io")
                .field("operation", operation)
                .field("kind", kind)
                .finish(),
            Self::OpenedArtifactNotRegularFile { generation } => formatter
                .debug_struct("OpenedArtifactNotRegularFile")
                .field("generation", generation)
                .finish(),
            Self::ArtifactLengthChanged {
                generation,
                catalog_bytes,
                observed_bytes,
            } => formatter
                .debug_struct("ArtifactLengthChanged")
                .field("generation", generation)
                .field("catalog_bytes", catalog_bytes)
                .field("observed_bytes", observed_bytes)
                .finish(),
            Self::ArtifactLengthOverflow { generation } => formatter
                .debug_struct("ArtifactLengthOverflow")
                .field("generation", generation)
                .finish(),
            Self::AllocationRefused { requested } => formatter
                .debug_struct("AllocationRefused")
                .field("requested", requested)
                .finish(),
            Self::ArtifactExceededReadBound {
                generation,
                maximum,
            } => formatter
                .debug_struct("ArtifactExceededReadBound")
                .field("generation", generation)
                .field("maximum", maximum)
                .finish(),
            Self::ArtifactDecode(error) => formatter
                .debug_tuple("ArtifactDecode")
                .field(error)
                .finish(),
        }
    }
}

impl fmt::Display for LocalCheckpointRecoveryErrorV0 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(self, formatter)
    }
}

impl Error for LocalCheckpointRecoveryErrorV0 {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::ExpectedContract(error) => Some(error),
            Self::Catalog(error) => Some(error),
            Self::ArtifactDecode(error) => Some(error),
            _ => None,
        }
    }
}

pub struct LocalCheckpointRecoveryV0 {
    restored: KafkaCheckpointGenerationRestoreV0,
    artifact_bytes: usize,
    pending_entries: usize,
}

impl LocalCheckpointRecoveryV0 {
    pub const fn generation(&self) -> KafkaCheckpointGenerationV0 {
        self.restored.generation()
    }
    pub const fn checkpoint_hash(&self) -> KafkaIngestionCheckpointHashV0 {
        self.restored.checkpoint_hash()
    }
    pub const fn admission(&self) -> &AdmissionStateV0 {
        self.restored.admission()
    }
    pub const fn frontier(&self) -> &KafkaPartitionFrontierV0 {
        self.restored.frontier()
    }
    pub const fn artifact_bytes(&self) -> usize {
        self.artifact_bytes
    }
    pub const fn pending_entries(&self) -> usize {
        self.pending_entries
    }
    pub fn into_parts(self) -> (AdmissionStateV0, KafkaPartitionFrontierV0) {
        self.restored.into_parts()
    }
}

impl fmt::Debug for LocalCheckpointRecoveryV0 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("LocalCheckpointRecoveryV0")
            .field("generation", &self.generation())
            .field("checkpoint_hash", &self.checkpoint_hash())
            .field("artifact_bytes", &self.artifact_bytes)
            .field("pending_entries", &self.pending_entries)
            .field("admission", &"<redacted>")
            .field("frontier", &"<redacted>")
            .finish()
    }
}

pub fn restore_latest_local_kafka_checkpoint_generation_v0(
    store: &LocalCheckpointStoreV0,
    expected_admission_config: &AdmissionConfigV0,
    expected_source_contract: &KafkaSourceContractV0,
) -> Result<LocalCheckpointRecoveryV0, LocalCheckpointRecoveryErrorV0> {
    validate_kafka_ingestion_checkpoint_expected_contract_v0(
        expected_admission_config,
        expected_source_contract,
    )
    .map_err(LocalCheckpointRecoveryErrorV0::ExpectedContract)?;
    let catalog = store
        .catalog()
        .map_err(LocalCheckpointRecoveryErrorV0::Catalog)?;
    let pending_entries = catalog.pending().len();
    let published = catalog
        .latest_published()
        .ok_or(LocalCheckpointRecoveryErrorV0::NoPublishedGeneration { pending_entries })?;
    let generation = published.generation();
    let catalog_bytes = published.artifact_bytes();
    let path = store
        .canonical_root()
        .join(published_file_name_v0(generation));
    let mut file = File::open(&path).map_err(|error| {
        recovery_io_error(LocalCheckpointRecoveryIoOperationV0::OpenPublished, &error)
    })?;
    validate_opened_metadata(&file, generation, catalog_bytes)?;
    let requested = usize::try_from(catalog_bytes)
        .map_err(|_| LocalCheckpointRecoveryErrorV0::ArtifactLengthOverflow { generation })?;
    let mut artifact = Vec::new();
    artifact
        .try_reserve_exact(requested)
        .map_err(|_| LocalCheckpointRecoveryErrorV0::AllocationRefused { requested })?;
    let read_limit = u64::try_from(MAX_KAFKA_CHECKPOINT_GENERATION_ARTIFACT_BYTES_V0)
        .expect("artifact maximum fits u64")
        .checked_add(1)
        .expect("artifact maximum plus one fits u64");
    {
        let mut bounded = file.by_ref().take(read_limit);
        bounded.read_to_end(&mut artifact).map_err(|error| {
            recovery_io_error(LocalCheckpointRecoveryIoOperationV0::ReadPublished, &error)
        })?;
    }
    if artifact.len() > MAX_KAFKA_CHECKPOINT_GENERATION_ARTIFACT_BYTES_V0 {
        return Err(LocalCheckpointRecoveryErrorV0::ArtifactExceededReadBound {
            generation,
            maximum: MAX_KAFKA_CHECKPOINT_GENERATION_ARTIFACT_BYTES_V0,
        });
    }
    if u64::try_from(artifact.len()).ok() != Some(catalog_bytes) {
        return Err(LocalCheckpointRecoveryErrorV0::ArtifactLengthChanged {
            generation,
            catalog_bytes,
            observed_bytes: u64::try_from(artifact.len()).unwrap_or(u64::MAX),
        });
    }
    validate_opened_metadata(&file, generation, catalog_bytes)?;
    let restored = restore_kafka_checkpoint_generation_artifact_v0(
        &artifact,
        generation,
        expected_admission_config,
        expected_source_contract,
    )
    .map_err(LocalCheckpointRecoveryErrorV0::ArtifactDecode)?;
    Ok(LocalCheckpointRecoveryV0 {
        restored,
        artifact_bytes: artifact.len(),
        pending_entries,
    })
}

fn validate_opened_metadata(
    file: &File,
    generation: KafkaCheckpointGenerationV0,
    catalog_bytes: u64,
) -> Result<(), LocalCheckpointRecoveryErrorV0> {
    let metadata = file.metadata().map_err(|error| {
        recovery_io_error(
            LocalCheckpointRecoveryIoOperationV0::InspectPublished,
            &error,
        )
    })?;
    if !metadata.is_file() {
        return Err(LocalCheckpointRecoveryErrorV0::OpenedArtifactNotRegularFile { generation });
    }
    if metadata.len() != catalog_bytes {
        return Err(LocalCheckpointRecoveryErrorV0::ArtifactLengthChanged {
            generation,
            catalog_bytes,
            observed_bytes: metadata.len(),
        });
    }
    Ok(())
}

fn recovery_io_error(
    operation: LocalCheckpointRecoveryIoOperationV0,
    error: &io::Error,
) -> LocalCheckpointRecoveryErrorV0 {
    LocalCheckpointRecoveryErrorV0::Io {
        operation,
        kind: error.kind(),
    }
}
