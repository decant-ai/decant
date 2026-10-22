//! Single-writer local publication of one immutable checkpoint generation.

use std::error::Error;
use std::fmt;
use std::fs::{self, File, OpenOptions};
use std::io::{self, Write};

use decant_ingest::{
    KafkaCheckpointGenerationArtifactEncodeErrorV0, KafkaCheckpointGenerationV0,
    encode_kafka_checkpoint_generation_artifact_v0,
};
use decant_kafka::KafkaPartitionFrontierV0;
use decant_state::AdmissionStateV0;

use crate::{
    LocalCheckpointCatalogErrorV0, LocalCheckpointStoreV0,
    MAX_LOCAL_CHECKPOINT_DIRECTORY_ENTRIES_V0, pending_file_name_v0, published_file_name_v0,
};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum LocalCheckpointPublicationVisibilityV0 {
    NotPublished,
    Uncertain,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum LocalCheckpointPublicationIoOperationV0 {
    CreatePending,
    WritePending,
    SyncPending,
    PublishLink,
    RemovePending,
    OpenDirectoryForSync,
    SyncDirectory,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum LocalCheckpointPublicationFaultPointV0 {
    AfterPendingCreation,
    AfterPartialPendingWrite,
    AfterCompletePendingWrite,
    AfterPendingSync,
    AfterPublishedLink,
    AfterPendingRemoval,
    AfterDirectorySyncBeforeReceipt,
}

#[derive(Clone, Eq, PartialEq)]
pub enum LocalCheckpointPublicationErrorV0 {
    Catalog(LocalCheckpointCatalogErrorV0),
    GenerationNotAfterLatest {
        latest: KafkaCheckpointGenerationV0,
        attempted: KafkaCheckpointGenerationV0,
    },
    GenerationAlreadyPending {
        generation: KafkaCheckpointGenerationV0,
    },
    InsufficientDirectoryEntryHeadroom {
        current: usize,
        required: usize,
        maximum: usize,
    },
    ArtifactEncoding(KafkaCheckpointGenerationArtifactEncodeErrorV0),
    PublishedGenerationConflict {
        generation: KafkaCheckpointGenerationV0,
    },
    InjectedFault {
        point: LocalCheckpointPublicationFaultPointV0,
        visibility: LocalCheckpointPublicationVisibilityV0,
    },
    Io {
        operation: LocalCheckpointPublicationIoOperationV0,
        kind: io::ErrorKind,
        visibility: LocalCheckpointPublicationVisibilityV0,
    },
}

impl LocalCheckpointPublicationErrorV0 {
    pub const fn visibility(&self) -> LocalCheckpointPublicationVisibilityV0 {
        match self {
            Self::Io { visibility, .. } => *visibility,
            Self::InjectedFault { visibility, .. } => *visibility,
            Self::PublishedGenerationConflict { .. } => {
                LocalCheckpointPublicationVisibilityV0::Uncertain
            }
            Self::Catalog(_)
            | Self::GenerationNotAfterLatest { .. }
            | Self::GenerationAlreadyPending { .. }
            | Self::InsufficientDirectoryEntryHeadroom { .. }
            | Self::ArtifactEncoding(_) => LocalCheckpointPublicationVisibilityV0::NotPublished,
        }
    }
}

impl fmt::Debug for LocalCheckpointPublicationErrorV0 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Catalog(error) => formatter.debug_tuple("Catalog").field(error).finish(),
            Self::GenerationNotAfterLatest { latest, attempted } => formatter
                .debug_struct("GenerationNotAfterLatest")
                .field("latest", latest)
                .field("attempted", attempted)
                .finish(),
            Self::GenerationAlreadyPending { generation } => formatter
                .debug_struct("GenerationAlreadyPending")
                .field("generation", generation)
                .finish(),
            Self::InsufficientDirectoryEntryHeadroom {
                current,
                required,
                maximum,
            } => formatter
                .debug_struct("InsufficientDirectoryEntryHeadroom")
                .field("current", current)
                .field("required", required)
                .field("maximum", maximum)
                .finish(),
            Self::ArtifactEncoding(error) => formatter
                .debug_tuple("ArtifactEncoding")
                .field(error)
                .finish(),
            Self::PublishedGenerationConflict { generation } => formatter
                .debug_struct("PublishedGenerationConflict")
                .field("generation", generation)
                .finish(),
            Self::InjectedFault { point, visibility } => formatter
                .debug_struct("InjectedFault")
                .field("point", point)
                .field("visibility", visibility)
                .finish(),
            Self::Io {
                operation,
                kind,
                visibility,
            } => formatter
                .debug_struct("Io")
                .field("operation", operation)
                .field("kind", kind)
                .field("visibility", visibility)
                .finish(),
        }
    }
}

impl fmt::Display for LocalCheckpointPublicationErrorV0 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(self, formatter)
    }
}

impl Error for LocalCheckpointPublicationErrorV0 {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Catalog(error) => Some(error),
            Self::ArtifactEncoding(error) => Some(error),
            _ => None,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct LocalCheckpointPublicationReceiptV0 {
    generation: KafkaCheckpointGenerationV0,
    artifact_bytes: usize,
}

impl LocalCheckpointPublicationReceiptV0 {
    pub const fn generation(&self) -> KafkaCheckpointGenerationV0 {
        self.generation
    }

    pub const fn artifact_bytes(&self) -> usize {
        self.artifact_bytes
    }
}

pub fn publish_local_kafka_checkpoint_generation_v0(
    store: &LocalCheckpointStoreV0,
    generation: KafkaCheckpointGenerationV0,
    admission: &AdmissionStateV0,
    frontier: &KafkaPartitionFrontierV0,
) -> Result<LocalCheckpointPublicationReceiptV0, LocalCheckpointPublicationErrorV0> {
    publish_local_kafka_checkpoint_generation_core_v0(store, generation, admission, frontier, None)
}

pub fn publish_local_kafka_checkpoint_generation_with_fault_v0(
    store: &LocalCheckpointStoreV0,
    generation: KafkaCheckpointGenerationV0,
    admission: &AdmissionStateV0,
    frontier: &KafkaPartitionFrontierV0,
    fault_point: LocalCheckpointPublicationFaultPointV0,
) -> Result<LocalCheckpointPublicationReceiptV0, LocalCheckpointPublicationErrorV0> {
    publish_local_kafka_checkpoint_generation_core_v0(
        store,
        generation,
        admission,
        frontier,
        Some(fault_point),
    )
}

fn publish_local_kafka_checkpoint_generation_core_v0(
    store: &LocalCheckpointStoreV0,
    generation: KafkaCheckpointGenerationV0,
    admission: &AdmissionStateV0,
    frontier: &KafkaPartitionFrontierV0,
    fault_point: Option<LocalCheckpointPublicationFaultPointV0>,
) -> Result<LocalCheckpointPublicationReceiptV0, LocalCheckpointPublicationErrorV0> {
    let catalog = store
        .catalog()
        .map_err(LocalCheckpointPublicationErrorV0::Catalog)?;
    if let Some(latest) = catalog.latest_published()
        && generation <= latest.generation()
    {
        return Err(
            LocalCheckpointPublicationErrorV0::GenerationNotAfterLatest {
                latest: latest.generation(),
                attempted: generation,
            },
        );
    }
    if catalog
        .pending()
        .iter()
        .any(|entry| entry.generation() == generation)
    {
        return Err(LocalCheckpointPublicationErrorV0::GenerationAlreadyPending { generation });
    }
    const REQUIRED_HEADROOM: usize = 2;
    if catalog.total_entries() > MAX_LOCAL_CHECKPOINT_DIRECTORY_ENTRIES_V0 - REQUIRED_HEADROOM {
        return Err(
            LocalCheckpointPublicationErrorV0::InsufficientDirectoryEntryHeadroom {
                current: catalog.total_entries(),
                required: REQUIRED_HEADROOM,
                maximum: MAX_LOCAL_CHECKPOINT_DIRECTORY_ENTRIES_V0,
            },
        );
    }

    let artifact = encode_kafka_checkpoint_generation_artifact_v0(generation, admission, frontier)
        .map_err(LocalCheckpointPublicationErrorV0::ArtifactEncoding)?;
    let pending_path = store
        .canonical_root()
        .join(pending_file_name_v0(generation));
    let published_path = store
        .canonical_root()
        .join(published_file_name_v0(generation));
    let mut pending_file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&pending_path)
        .map_err(|error| {
            publication_io_error(
                LocalCheckpointPublicationIoOperationV0::CreatePending,
                &error,
                LocalCheckpointPublicationVisibilityV0::NotPublished,
            )
        })?;
    inject_if_requested(
        fault_point,
        LocalCheckpointPublicationFaultPointV0::AfterPendingCreation,
        LocalCheckpointPublicationVisibilityV0::NotPublished,
    )?;
    if fault_point == Some(LocalCheckpointPublicationFaultPointV0::AfterPartialPendingWrite) {
        let proper_prefix_length = artifact.len() / 2;
        debug_assert!(proper_prefix_length > 0 && proper_prefix_length < artifact.len());
        pending_file
            .write_all(&artifact[..proper_prefix_length])
            .map_err(|error| {
                publication_io_error(
                    LocalCheckpointPublicationIoOperationV0::WritePending,
                    &error,
                    LocalCheckpointPublicationVisibilityV0::NotPublished,
                )
            })?;
        return Err(LocalCheckpointPublicationErrorV0::InjectedFault {
            point: LocalCheckpointPublicationFaultPointV0::AfterPartialPendingWrite,
            visibility: LocalCheckpointPublicationVisibilityV0::NotPublished,
        });
    }
    pending_file.write_all(&artifact).map_err(|error| {
        publication_io_error(
            LocalCheckpointPublicationIoOperationV0::WritePending,
            &error,
            LocalCheckpointPublicationVisibilityV0::NotPublished,
        )
    })?;
    inject_if_requested(
        fault_point,
        LocalCheckpointPublicationFaultPointV0::AfterCompletePendingWrite,
        LocalCheckpointPublicationVisibilityV0::NotPublished,
    )?;
    pending_file.sync_all().map_err(|error| {
        publication_io_error(
            LocalCheckpointPublicationIoOperationV0::SyncPending,
            &error,
            LocalCheckpointPublicationVisibilityV0::NotPublished,
        )
    })?;
    inject_if_requested(
        fault_point,
        LocalCheckpointPublicationFaultPointV0::AfterPendingSync,
        LocalCheckpointPublicationVisibilityV0::NotPublished,
    )?;
    drop(pending_file);

    if let Err(error) = fs::hard_link(&pending_path, &published_path) {
        if error.kind() == io::ErrorKind::AlreadyExists {
            return Err(
                LocalCheckpointPublicationErrorV0::PublishedGenerationConflict { generation },
            );
        }
        return Err(publication_io_error(
            LocalCheckpointPublicationIoOperationV0::PublishLink,
            &error,
            LocalCheckpointPublicationVisibilityV0::NotPublished,
        ));
    }
    inject_if_requested(
        fault_point,
        LocalCheckpointPublicationFaultPointV0::AfterPublishedLink,
        LocalCheckpointPublicationVisibilityV0::Uncertain,
    )?;
    fs::remove_file(&pending_path).map_err(|error| {
        publication_io_error(
            LocalCheckpointPublicationIoOperationV0::RemovePending,
            &error,
            LocalCheckpointPublicationVisibilityV0::Uncertain,
        )
    })?;
    inject_if_requested(
        fault_point,
        LocalCheckpointPublicationFaultPointV0::AfterPendingRemoval,
        LocalCheckpointPublicationVisibilityV0::Uncertain,
    )?;
    let directory = File::open(store.canonical_root()).map_err(|error| {
        publication_io_error(
            LocalCheckpointPublicationIoOperationV0::OpenDirectoryForSync,
            &error,
            LocalCheckpointPublicationVisibilityV0::Uncertain,
        )
    })?;
    directory.sync_all().map_err(|error| {
        publication_io_error(
            LocalCheckpointPublicationIoOperationV0::SyncDirectory,
            &error,
            LocalCheckpointPublicationVisibilityV0::Uncertain,
        )
    })?;
    inject_if_requested(
        fault_point,
        LocalCheckpointPublicationFaultPointV0::AfterDirectorySyncBeforeReceipt,
        LocalCheckpointPublicationVisibilityV0::Uncertain,
    )?;
    Ok(LocalCheckpointPublicationReceiptV0 {
        generation,
        artifact_bytes: artifact.len(),
    })
}

fn inject_if_requested(
    requested: Option<LocalCheckpointPublicationFaultPointV0>,
    current: LocalCheckpointPublicationFaultPointV0,
    visibility: LocalCheckpointPublicationVisibilityV0,
) -> Result<(), LocalCheckpointPublicationErrorV0> {
    if requested == Some(current) {
        return Err(LocalCheckpointPublicationErrorV0::InjectedFault {
            point: current,
            visibility,
        });
    }
    Ok(())
}

fn publication_io_error(
    operation: LocalCheckpointPublicationIoOperationV0,
    error: &io::Error,
    visibility: LocalCheckpointPublicationVisibilityV0,
) -> LocalCheckpointPublicationErrorV0 {
    LocalCheckpointPublicationErrorV0::Io {
        operation,
        kind: error.kind(),
        visibility,
    }
}
