//! Destructive pruning of a strictly verified local checkpoint prefix.

use std::error::Error;
use std::fmt;
use std::fs::{self, File};
use std::io;

use decant_ingest::KafkaCheckpointGenerationV0;
use decant_kafka::KafkaSourceContractV0;
use decant_state::AdmissionConfigV0;

use crate::{
    LocalCheckpointCatalogErrorV0, LocalCheckpointRecoveryErrorV0, LocalCheckpointStoreV0,
    LocalCheckpointWriterLockV0, MAX_LOCAL_CHECKPOINT_DIRECTORY_ENTRIES_V0, published_file_name_v0,
    restore_latest_local_kafka_checkpoint_generation_v0,
};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum LocalCheckpointPruneIoOperationV0 {
    RemovePublished,
    OpenDirectoryForSync,
    SyncDirectory,
}

#[derive(Clone, Eq, PartialEq)]
pub enum LocalCheckpointPruneErrorV0 {
    WriterLockMismatch,
    RetainCountOutOfRange {
        maximum: usize,
        actual: usize,
    },
    Catalog(LocalCheckpointCatalogErrorV0),
    PendingEntries {
        count: usize,
    },
    NoPublishedGeneration,
    LatestRecovery(LocalCheckpointRecoveryErrorV0),
    UnexpectedRecoveredGeneration {
        catalog: KafkaCheckpointGenerationV0,
        recovered: KafkaCheckpointGenerationV0,
    },
    Io {
        operation: LocalCheckpointPruneIoOperationV0,
        kind: io::ErrorKind,
        removed_before_failure: usize,
    },
}

impl fmt::Debug for LocalCheckpointPruneErrorV0 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::WriterLockMismatch => formatter.write_str("WriterLockMismatch"),
            Self::RetainCountOutOfRange { maximum, actual } => formatter
                .debug_struct("RetainCountOutOfRange")
                .field("maximum", maximum)
                .field("actual", actual)
                .finish(),
            Self::Catalog(error) => formatter.debug_tuple("Catalog").field(error).finish(),
            Self::PendingEntries { count } => formatter
                .debug_struct("PendingEntries")
                .field("count", count)
                .finish(),
            Self::NoPublishedGeneration => formatter.write_str("NoPublishedGeneration"),
            Self::LatestRecovery(error) => formatter
                .debug_tuple("LatestRecovery")
                .field(error)
                .finish(),
            Self::UnexpectedRecoveredGeneration { catalog, recovered } => formatter
                .debug_struct("UnexpectedRecoveredGeneration")
                .field("catalog", catalog)
                .field("recovered", recovered)
                .finish(),
            Self::Io {
                operation,
                kind,
                removed_before_failure,
            } => formatter
                .debug_struct("Io")
                .field("operation", operation)
                .field("kind", kind)
                .field("removed_before_failure", removed_before_failure)
                .finish(),
        }
    }
}

impl fmt::Display for LocalCheckpointPruneErrorV0 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(self, formatter)
    }
}

impl Error for LocalCheckpointPruneErrorV0 {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Catalog(error) => Some(error),
            Self::LatestRecovery(error) => Some(error),
            _ => None,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct LocalCheckpointPruneReceiptV0 {
    latest_generation: KafkaCheckpointGenerationV0,
    retained_generations: usize,
    removed_generations: usize,
}

impl LocalCheckpointPruneReceiptV0 {
    pub const fn latest_generation(self) -> KafkaCheckpointGenerationV0 {
        self.latest_generation
    }

    pub const fn retained_generations(self) -> usize {
        self.retained_generations
    }

    pub const fn removed_generations(self) -> usize {
        self.removed_generations
    }
}

pub fn prune_verified_local_kafka_checkpoints_v0(
    writer_lock: &LocalCheckpointWriterLockV0,
    store: &LocalCheckpointStoreV0,
    retain_latest: usize,
    expected_admission_config: &AdmissionConfigV0,
    expected_source_contract: &KafkaSourceContractV0,
) -> Result<LocalCheckpointPruneReceiptV0, LocalCheckpointPruneErrorV0> {
    if !writer_lock.protects(store) {
        return Err(LocalCheckpointPruneErrorV0::WriterLockMismatch);
    }
    if !(1..=MAX_LOCAL_CHECKPOINT_DIRECTORY_ENTRIES_V0).contains(&retain_latest) {
        return Err(LocalCheckpointPruneErrorV0::RetainCountOutOfRange {
            maximum: MAX_LOCAL_CHECKPOINT_DIRECTORY_ENTRIES_V0,
            actual: retain_latest,
        });
    }
    let catalog = store
        .catalog()
        .map_err(LocalCheckpointPruneErrorV0::Catalog)?;
    if !catalog.pending().is_empty() {
        return Err(LocalCheckpointPruneErrorV0::PendingEntries {
            count: catalog.pending().len(),
        });
    }
    let latest = catalog
        .latest_published()
        .ok_or(LocalCheckpointPruneErrorV0::NoPublishedGeneration)?
        .generation();
    let recovery = restore_latest_local_kafka_checkpoint_generation_v0(
        store,
        expected_admission_config,
        expected_source_contract,
    )
    .map_err(LocalCheckpointPruneErrorV0::LatestRecovery)?;
    if recovery.generation() != latest {
        return Err(LocalCheckpointPruneErrorV0::UnexpectedRecoveredGeneration {
            catalog: latest,
            recovered: recovery.generation(),
        });
    }

    let remove_count = catalog.published().len().saturating_sub(retain_latest);
    let retained_generations = catalog.published().len() - remove_count;
    if remove_count == 0 {
        return Ok(LocalCheckpointPruneReceiptV0 {
            latest_generation: latest,
            retained_generations,
            removed_generations: 0,
        });
    }
    let mut removed = 0_usize;
    for entry in catalog.published().iter().take(remove_count) {
        let path = store
            .canonical_root()
            .join(published_file_name_v0(entry.generation()));
        fs::remove_file(path).map_err(|error| LocalCheckpointPruneErrorV0::Io {
            operation: LocalCheckpointPruneIoOperationV0::RemovePublished,
            kind: error.kind(),
            removed_before_failure: removed,
        })?;
        removed += 1;
    }
    let directory =
        File::open(store.canonical_root()).map_err(|error| LocalCheckpointPruneErrorV0::Io {
            operation: LocalCheckpointPruneIoOperationV0::OpenDirectoryForSync,
            kind: error.kind(),
            removed_before_failure: removed,
        })?;
    directory
        .sync_all()
        .map_err(|error| LocalCheckpointPruneErrorV0::Io {
            operation: LocalCheckpointPruneIoOperationV0::SyncDirectory,
            kind: error.kind(),
            removed_before_failure: removed,
        })?;
    Ok(LocalCheckpointPruneReceiptV0 {
        latest_generation: latest,
        retained_generations,
        removed_generations: removed,
    })
}
