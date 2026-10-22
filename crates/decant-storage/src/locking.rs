//! Process-lifetime exclusive ownership of one local checkpoint namespace.

use std::error::Error;
use std::fmt;
use std::fs::{self, File, OpenOptions, TryLockError};
use std::io;
use std::path::{Path, PathBuf};

use crate::{
    LOCAL_CHECKPOINT_WRITER_LOCK_FILE_V0, LocalCheckpointCatalogErrorV0, LocalCheckpointStoreV0,
    MAX_LOCAL_CHECKPOINT_DIRECTORY_ENTRIES_V0,
};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum LocalCheckpointWriterLockIoOperationV0 {
    InspectLock,
    CreateLock,
    OpenLock,
    InspectOpenedLock,
    AcquireExclusive,
}

#[derive(Clone, Eq, PartialEq)]
pub enum LocalCheckpointWriterLockErrorV0 {
    Catalog(LocalCheckpointCatalogErrorV0),
    DirectoryFull {
        maximum: usize,
    },
    LockIsSymlink,
    LockNotRegularFile,
    LockNotEmpty {
        actual: u64,
    },
    AlreadyHeld,
    Io {
        operation: LocalCheckpointWriterLockIoOperationV0,
        kind: io::ErrorKind,
    },
}

impl fmt::Debug for LocalCheckpointWriterLockErrorV0 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Catalog(error) => formatter.debug_tuple("Catalog").field(error).finish(),
            Self::DirectoryFull { maximum } => formatter
                .debug_struct("DirectoryFull")
                .field("maximum", maximum)
                .finish(),
            Self::LockIsSymlink => formatter.write_str("LockIsSymlink"),
            Self::LockNotRegularFile => formatter.write_str("LockNotRegularFile"),
            Self::LockNotEmpty { actual } => formatter
                .debug_struct("LockNotEmpty")
                .field("actual", actual)
                .finish(),
            Self::AlreadyHeld => formatter.write_str("AlreadyHeld"),
            Self::Io { operation, kind } => formatter
                .debug_struct("Io")
                .field("operation", operation)
                .field("kind", kind)
                .finish(),
        }
    }
}

impl fmt::Display for LocalCheckpointWriterLockErrorV0 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(self, formatter)
    }
}

impl Error for LocalCheckpointWriterLockErrorV0 {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Catalog(error) => Some(error),
            _ => None,
        }
    }
}

#[must_use = "the writer lock is released when this guard is dropped"]
pub struct LocalCheckpointWriterLockV0 {
    canonical_root: PathBuf,
    _file: File,
}

impl LocalCheckpointWriterLockV0 {
    pub(crate) fn protects(&self, store: &LocalCheckpointStoreV0) -> bool {
        self.canonical_root == store.canonical_root()
    }
}

impl fmt::Debug for LocalCheckpointWriterLockV0 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("LocalCheckpointWriterLockV0")
            .field("version", &0)
            .field("canonical_root", &"<redacted>")
            .finish()
    }
}

pub fn acquire_local_checkpoint_writer_lock_v0(
    store: &LocalCheckpointStoreV0,
) -> Result<LocalCheckpointWriterLockV0, LocalCheckpointWriterLockErrorV0> {
    let catalog = store
        .catalog()
        .map_err(LocalCheckpointWriterLockErrorV0::Catalog)?;
    if !catalog.writer_lock_present()
        && catalog.total_entries() >= MAX_LOCAL_CHECKPOINT_DIRECTORY_ENTRIES_V0
    {
        return Err(LocalCheckpointWriterLockErrorV0::DirectoryFull {
            maximum: MAX_LOCAL_CHECKPOINT_DIRECTORY_ENTRIES_V0,
        });
    }
    let path = store
        .canonical_root()
        .join(LOCAL_CHECKPOINT_WRITER_LOCK_FILE_V0);
    let file = match OpenOptions::new()
        .read(true)
        .write(true)
        .create_new(true)
        .open(&path)
    {
        Ok(file) => file,
        Err(error) if error.kind() == io::ErrorKind::AlreadyExists => open_existing_lock(&path)?,
        Err(error) => {
            return Err(lock_io_error(
                LocalCheckpointWriterLockIoOperationV0::CreateLock,
                &error,
            ));
        }
    };
    validate_opened_lock(&file)?;
    file.try_lock().map_err(|error| match error {
        TryLockError::WouldBlock => LocalCheckpointWriterLockErrorV0::AlreadyHeld,
        TryLockError::Error(error) => lock_io_error(
            LocalCheckpointWriterLockIoOperationV0::AcquireExclusive,
            &error,
        ),
    })?;
    validate_opened_lock(&file)?;
    Ok(LocalCheckpointWriterLockV0 {
        canonical_root: store.canonical_root().to_path_buf(),
        _file: file,
    })
}

fn open_existing_lock(path: &Path) -> Result<File, LocalCheckpointWriterLockErrorV0> {
    let metadata = fs::symlink_metadata(path).map_err(|error| {
        lock_io_error(LocalCheckpointWriterLockIoOperationV0::InspectLock, &error)
    })?;
    if metadata.file_type().is_symlink() {
        return Err(LocalCheckpointWriterLockErrorV0::LockIsSymlink);
    }
    if !metadata.is_file() {
        return Err(LocalCheckpointWriterLockErrorV0::LockNotRegularFile);
    }
    if metadata.len() != 0 {
        return Err(LocalCheckpointWriterLockErrorV0::LockNotEmpty {
            actual: metadata.len(),
        });
    }
    OpenOptions::new()
        .read(true)
        .write(true)
        .open(path)
        .map_err(|error| lock_io_error(LocalCheckpointWriterLockIoOperationV0::OpenLock, &error))
}

fn validate_opened_lock(file: &File) -> Result<(), LocalCheckpointWriterLockErrorV0> {
    let metadata = file.metadata().map_err(|error| {
        lock_io_error(
            LocalCheckpointWriterLockIoOperationV0::InspectOpenedLock,
            &error,
        )
    })?;
    if !metadata.is_file() {
        return Err(LocalCheckpointWriterLockErrorV0::LockNotRegularFile);
    }
    if metadata.len() != 0 {
        return Err(LocalCheckpointWriterLockErrorV0::LockNotEmpty {
            actual: metadata.len(),
        });
    }
    Ok(())
}

fn lock_io_error(
    operation: LocalCheckpointWriterLockIoOperationV0,
    error: &io::Error,
) -> LocalCheckpointWriterLockErrorV0 {
    LocalCheckpointWriterLockErrorV0::Io {
        operation,
        kind: error.kind(),
    }
}
