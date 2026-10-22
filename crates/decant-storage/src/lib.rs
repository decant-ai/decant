//! Bounded local checkpoint namespace inspection.

#![forbid(unsafe_code)]

mod commit_candidate;
mod locking;
mod pruning;
mod publication;
mod recovery;
mod verified_publication;

use std::error::Error;
use std::ffi::OsStr;
use std::fmt;
use std::fs;
use std::io;
use std::path::{Path, PathBuf};

use decant_ingest::{
    KafkaCheckpointGenerationErrorV0, KafkaCheckpointGenerationV0,
    MAX_KAFKA_CHECKPOINT_GENERATION_ARTIFACT_BYTES_V0,
};

pub use commit_candidate::{
    LocalKafkaCheckpointCommitCandidateErrorV0, LocalKafkaCheckpointCommitCandidateV0,
    recover_latest_local_kafka_checkpoint_commit_candidate_v0,
};
pub use locking::{
    LocalCheckpointWriterLockErrorV0, LocalCheckpointWriterLockIoOperationV0,
    LocalCheckpointWriterLockV0, acquire_local_checkpoint_writer_lock_v0,
};
pub use pruning::{
    LocalCheckpointPruneErrorV0, LocalCheckpointPruneIoOperationV0, LocalCheckpointPruneReceiptV0,
    prune_verified_local_kafka_checkpoints_v0,
};
pub use publication::{
    LocalCheckpointPublicationErrorV0, LocalCheckpointPublicationFaultPointV0,
    LocalCheckpointPublicationIoOperationV0, LocalCheckpointPublicationReceiptV0,
    LocalCheckpointPublicationVisibilityV0, publish_local_kafka_checkpoint_generation_v0,
    publish_local_kafka_checkpoint_generation_with_fault_v0,
};
pub use recovery::{
    LocalCheckpointRecoveryErrorV0, LocalCheckpointRecoveryIoOperationV0,
    LocalCheckpointRecoveryV0, restore_latest_local_kafka_checkpoint_generation_v0,
};
pub use verified_publication::{
    VerifiedLocalKafkaCheckpointPublicationErrorV0,
    VerifiedLocalKafkaCheckpointPublicationStatusV0,
    publish_and_verify_local_kafka_checkpoint_commit_candidate_v0,
    publish_and_verify_local_kafka_checkpoint_commit_candidate_with_fault_v0,
    publish_and_verify_local_kafka_checkpoint_commit_candidate_with_writer_lock_v0,
};

pub const LOCAL_CHECKPOINT_STORE_VERSION_V0: u16 = 0;
pub const MAX_LOCAL_CHECKPOINT_ROOT_PATH_BYTES_V0: usize = 4_096;
pub const MAX_LOCAL_CHECKPOINT_DIRECTORY_ENTRIES_V0: usize = 256;
pub const LOCAL_CHECKPOINT_WRITER_LOCK_FILE_V0: &str = "decant-writer-v0.lock";

const PUBLISHED_FILE_PREFIX_V0: &str = "checkpoint-";
const PUBLISHED_FILE_SUFFIX_V0: &str = ".dcp";
const PENDING_FILE_SUFFIX_V0: &str = ".pending";
const GENERATION_DIGITS_V0: usize = 20;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum LocalCheckpointIoOperationV0 {
    InspectRoot,
    CanonicalizeRoot,
    ReadDirectory,
    ReadDirectoryEntry,
    InspectEntry,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum LocalCheckpointCatalogEntryKindV0 {
    Published,
    Pending,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct LocalCheckpointCatalogEntryV0 {
    generation: KafkaCheckpointGenerationV0,
    kind: LocalCheckpointCatalogEntryKindV0,
    artifact_bytes: u64,
}

impl LocalCheckpointCatalogEntryV0 {
    pub const fn generation(&self) -> KafkaCheckpointGenerationV0 {
        self.generation
    }

    pub const fn kind(&self) -> LocalCheckpointCatalogEntryKindV0 {
        self.kind
    }

    pub const fn artifact_bytes(&self) -> u64 {
        self.artifact_bytes
    }
}

#[derive(Clone, Eq, PartialEq)]
pub enum LocalCheckpointCatalogErrorV0 {
    PathTooLong {
        maximum: usize,
        actual: usize,
    },
    RootIsSymlink,
    RootNotDirectory,
    Io {
        operation: LocalCheckpointIoOperationV0,
        kind: io::ErrorKind,
    },
    TooManyDirectoryEntries {
        maximum: usize,
    },
    AllocationRefused,
    NonUtf8EntryName,
    UnknownEntryName,
    InvalidGenerationName,
    ZeroGeneration,
    EntryNotRegularFile {
        generation: KafkaCheckpointGenerationV0,
        kind: LocalCheckpointCatalogEntryKindV0,
    },
    WriterLockNotRegularFile,
    WriterLockNotEmpty {
        actual: u64,
    },
    ArtifactTooLarge {
        generation: KafkaCheckpointGenerationV0,
        kind: LocalCheckpointCatalogEntryKindV0,
        maximum: usize,
        actual: u64,
    },
}

impl fmt::Debug for LocalCheckpointCatalogErrorV0 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::PathTooLong { maximum, actual } => formatter
                .debug_struct("PathTooLong")
                .field("maximum", maximum)
                .field("actual", actual)
                .finish(),
            Self::RootIsSymlink => formatter.write_str("RootIsSymlink"),
            Self::RootNotDirectory => formatter.write_str("RootNotDirectory"),
            Self::Io { operation, kind } => formatter
                .debug_struct("Io")
                .field("operation", operation)
                .field("kind", kind)
                .finish(),
            Self::TooManyDirectoryEntries { maximum } => formatter
                .debug_struct("TooManyDirectoryEntries")
                .field("maximum", maximum)
                .finish(),
            Self::AllocationRefused => formatter.write_str("AllocationRefused"),
            Self::NonUtf8EntryName => formatter.write_str("NonUtf8EntryName"),
            Self::UnknownEntryName => formatter.write_str("UnknownEntryName"),
            Self::InvalidGenerationName => formatter.write_str("InvalidGenerationName"),
            Self::ZeroGeneration => formatter.write_str("ZeroGeneration"),
            Self::EntryNotRegularFile { generation, kind } => formatter
                .debug_struct("EntryNotRegularFile")
                .field("generation", generation)
                .field("kind", kind)
                .finish(),
            Self::WriterLockNotRegularFile => formatter.write_str("WriterLockNotRegularFile"),
            Self::WriterLockNotEmpty { actual } => formatter
                .debug_struct("WriterLockNotEmpty")
                .field("actual", actual)
                .finish(),
            Self::ArtifactTooLarge {
                generation,
                kind,
                maximum,
                actual,
            } => formatter
                .debug_struct("ArtifactTooLarge")
                .field("generation", generation)
                .field("kind", kind)
                .field("maximum", maximum)
                .field("actual", actual)
                .finish(),
        }
    }
}

impl fmt::Display for LocalCheckpointCatalogErrorV0 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(self, formatter)
    }
}

impl Error for LocalCheckpointCatalogErrorV0 {}

pub struct LocalCheckpointStoreV0 {
    canonical_root: PathBuf,
}

impl LocalCheckpointStoreV0 {
    pub fn open(root: &Path) -> Result<Self, LocalCheckpointCatalogErrorV0> {
        validate_path_length(root)?;
        let metadata = fs::symlink_metadata(root)
            .map_err(|error| io_error(LocalCheckpointIoOperationV0::InspectRoot, &error))?;
        if metadata.file_type().is_symlink() {
            return Err(LocalCheckpointCatalogErrorV0::RootIsSymlink);
        }
        if !metadata.is_dir() {
            return Err(LocalCheckpointCatalogErrorV0::RootNotDirectory);
        }
        let canonical_root = fs::canonicalize(root)
            .map_err(|error| io_error(LocalCheckpointIoOperationV0::CanonicalizeRoot, &error))?;
        validate_path_length(&canonical_root)?;
        Ok(Self { canonical_root })
    }

    pub fn catalog(&self) -> Result<LocalCheckpointCatalogV0, LocalCheckpointCatalogErrorV0> {
        scan_catalog(&self.canonical_root)
    }

    pub(crate) fn canonical_root(&self) -> &Path {
        &self.canonical_root
    }
}

impl fmt::Debug for LocalCheckpointStoreV0 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("LocalCheckpointStoreV0")
            .field("version", &LOCAL_CHECKPOINT_STORE_VERSION_V0)
            .field("canonical_root", &"<redacted>")
            .finish()
    }
}

pub struct LocalCheckpointCatalogV0 {
    published: Vec<LocalCheckpointCatalogEntryV0>,
    pending: Vec<LocalCheckpointCatalogEntryV0>,
    directory_entries: usize,
    writer_lock_present: bool,
}

impl LocalCheckpointCatalogV0 {
    pub fn published(&self) -> &[LocalCheckpointCatalogEntryV0] {
        &self.published
    }

    pub fn pending(&self) -> &[LocalCheckpointCatalogEntryV0] {
        &self.pending
    }

    pub fn latest_published(&self) -> Option<&LocalCheckpointCatalogEntryV0> {
        self.published.last()
    }

    pub fn total_entries(&self) -> usize {
        self.directory_entries
    }

    pub const fn writer_lock_present(&self) -> bool {
        self.writer_lock_present
    }
}

impl fmt::Debug for LocalCheckpointCatalogV0 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("LocalCheckpointCatalogV0")
            .field("published_entries", &self.published.len())
            .field("pending_entries", &self.pending.len())
            .field("writer_lock_present", &self.writer_lock_present)
            .finish()
    }
}

pub(crate) fn published_file_name_v0(generation: KafkaCheckpointGenerationV0) -> String {
    format!(
        "{PUBLISHED_FILE_PREFIX_V0}{:020}{PUBLISHED_FILE_SUFFIX_V0}",
        generation.get()
    )
}

pub(crate) fn pending_file_name_v0(generation: KafkaCheckpointGenerationV0) -> String {
    format!(
        "{PUBLISHED_FILE_PREFIX_V0}{:020}{PENDING_FILE_SUFFIX_V0}",
        generation.get()
    )
}

fn scan_catalog(root: &Path) -> Result<LocalCheckpointCatalogV0, LocalCheckpointCatalogErrorV0> {
    let directory = fs::read_dir(root)
        .map_err(|error| io_error(LocalCheckpointIoOperationV0::ReadDirectory, &error))?;
    let mut published = Vec::new();
    let mut pending = Vec::new();
    published
        .try_reserve_exact(MAX_LOCAL_CHECKPOINT_DIRECTORY_ENTRIES_V0)
        .map_err(|_| LocalCheckpointCatalogErrorV0::AllocationRefused)?;
    pending
        .try_reserve_exact(MAX_LOCAL_CHECKPOINT_DIRECTORY_ENTRIES_V0)
        .map_err(|_| LocalCheckpointCatalogErrorV0::AllocationRefused)?;
    let mut total = 0_usize;
    let mut writer_lock_present = false;
    for entry in directory {
        total =
            total
                .checked_add(1)
                .ok_or(LocalCheckpointCatalogErrorV0::TooManyDirectoryEntries {
                    maximum: MAX_LOCAL_CHECKPOINT_DIRECTORY_ENTRIES_V0,
                })?;
        if total > MAX_LOCAL_CHECKPOINT_DIRECTORY_ENTRIES_V0 {
            return Err(LocalCheckpointCatalogErrorV0::TooManyDirectoryEntries {
                maximum: MAX_LOCAL_CHECKPOINT_DIRECTORY_ENTRIES_V0,
            });
        }
        let entry = entry
            .map_err(|error| io_error(LocalCheckpointIoOperationV0::ReadDirectoryEntry, &error))?;
        let entry_name = entry.file_name();
        let file_type = entry
            .file_type()
            .map_err(|error| io_error(LocalCheckpointIoOperationV0::InspectEntry, &error))?;
        if entry_name == OsStr::new(LOCAL_CHECKPOINT_WRITER_LOCK_FILE_V0) {
            if !file_type.is_file() {
                return Err(LocalCheckpointCatalogErrorV0::WriterLockNotRegularFile);
            }
            let metadata = entry
                .metadata()
                .map_err(|error| io_error(LocalCheckpointIoOperationV0::InspectEntry, &error))?;
            if metadata.len() != 0 {
                return Err(LocalCheckpointCatalogErrorV0::WriterLockNotEmpty {
                    actual: metadata.len(),
                });
            }
            writer_lock_present = true;
            continue;
        }
        let (generation, kind) = parse_entry_name(&entry_name)?;
        if !file_type.is_file() {
            return Err(LocalCheckpointCatalogErrorV0::EntryNotRegularFile { generation, kind });
        }
        let metadata = entry
            .metadata()
            .map_err(|error| io_error(LocalCheckpointIoOperationV0::InspectEntry, &error))?;
        let artifact_bytes = metadata.len();
        if artifact_bytes
            > u64::try_from(MAX_KAFKA_CHECKPOINT_GENERATION_ARTIFACT_BYTES_V0)
                .expect("artifact maximum fits u64")
        {
            return Err(LocalCheckpointCatalogErrorV0::ArtifactTooLarge {
                generation,
                kind,
                maximum: MAX_KAFKA_CHECKPOINT_GENERATION_ARTIFACT_BYTES_V0,
                actual: artifact_bytes,
            });
        }
        let catalog_entry = LocalCheckpointCatalogEntryV0 {
            generation,
            kind,
            artifact_bytes,
        };
        match kind {
            LocalCheckpointCatalogEntryKindV0::Published => published.push(catalog_entry),
            LocalCheckpointCatalogEntryKindV0::Pending => pending.push(catalog_entry),
        }
    }
    published.sort_unstable_by_key(LocalCheckpointCatalogEntryV0::generation);
    pending.sort_unstable_by_key(LocalCheckpointCatalogEntryV0::generation);
    Ok(LocalCheckpointCatalogV0 {
        published,
        pending,
        directory_entries: total,
        writer_lock_present,
    })
}

fn parse_entry_name(
    name: &OsStr,
) -> Result<
    (
        KafkaCheckpointGenerationV0,
        LocalCheckpointCatalogEntryKindV0,
    ),
    LocalCheckpointCatalogErrorV0,
> {
    let name = name
        .to_str()
        .ok_or(LocalCheckpointCatalogErrorV0::NonUtf8EntryName)?;
    let (digits, kind) = if let Some(inner) = name
        .strip_prefix(PUBLISHED_FILE_PREFIX_V0)
        .and_then(|inner| inner.strip_suffix(PUBLISHED_FILE_SUFFIX_V0))
    {
        (inner, LocalCheckpointCatalogEntryKindV0::Published)
    } else if let Some(inner) = name
        .strip_prefix(PUBLISHED_FILE_PREFIX_V0)
        .and_then(|inner| inner.strip_suffix(PENDING_FILE_SUFFIX_V0))
    {
        (inner, LocalCheckpointCatalogEntryKindV0::Pending)
    } else {
        return Err(LocalCheckpointCatalogErrorV0::UnknownEntryName);
    };
    if digits.len() != GENERATION_DIGITS_V0 || !digits.bytes().all(|byte| byte.is_ascii_digit()) {
        return Err(LocalCheckpointCatalogErrorV0::InvalidGenerationName);
    }
    let value = digits
        .parse::<u64>()
        .map_err(|_| LocalCheckpointCatalogErrorV0::InvalidGenerationName)?;
    let generation = KafkaCheckpointGenerationV0::new(value).map_err(
        |KafkaCheckpointGenerationErrorV0::Zero| LocalCheckpointCatalogErrorV0::ZeroGeneration,
    )?;
    Ok((generation, kind))
}

fn validate_path_length(path: &Path) -> Result<(), LocalCheckpointCatalogErrorV0> {
    let actual = path.as_os_str().as_encoded_bytes().len();
    if actual > MAX_LOCAL_CHECKPOINT_ROOT_PATH_BYTES_V0 {
        return Err(LocalCheckpointCatalogErrorV0::PathTooLong {
            maximum: MAX_LOCAL_CHECKPOINT_ROOT_PATH_BYTES_V0,
            actual,
        });
    }
    Ok(())
}

fn io_error(
    operation: LocalCheckpointIoOperationV0,
    error: &io::Error,
) -> LocalCheckpointCatalogErrorV0 {
    LocalCheckpointCatalogErrorV0::Io {
        operation,
        kind: error.kind(),
    }
}
