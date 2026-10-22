use std::fs::{self, File};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use decant_storage::{
    LOCAL_CHECKPOINT_WRITER_LOCK_FILE_V0, LocalCheckpointCatalogErrorV0, LocalCheckpointStoreV0,
    LocalCheckpointWriterLockErrorV0, MAX_LOCAL_CHECKPOINT_DIRECTORY_ENTRIES_V0,
    acquire_local_checkpoint_writer_lock_v0,
};

static NEXT_TEMP: AtomicU64 = AtomicU64::new(0);

struct TestDirectory(PathBuf);

impl TestDirectory {
    fn new(label: &str) -> Self {
        let sequence = NEXT_TEMP.fetch_add(1, Ordering::Relaxed);
        let path = std::env::temp_dir().join(format!(
            "decant-writer-lock-{label}-{}-{sequence}",
            std::process::id()
        ));
        fs::create_dir(&path).unwrap();
        Self(path)
    }

    fn path(&self) -> &Path {
        &self.0
    }
}

impl Drop for TestDirectory {
    fn drop(&mut self) {
        fs::remove_dir_all(&self.0).unwrap();
    }
}

#[test]
fn writer_lock_consumes_one_exact_bounded_directory_entry() {
    let exact = TestDirectory::new("exact-directory-bound");
    for generation in 1..u64::try_from(MAX_LOCAL_CHECKPOINT_DIRECTORY_ENTRIES_V0).unwrap() {
        File::create(
            exact
                .path()
                .join(format!("checkpoint-{generation:020}.dcp")),
        )
        .unwrap();
    }
    let store = LocalCheckpointStoreV0::open(exact.path()).unwrap();
    let lock = acquire_local_checkpoint_writer_lock_v0(&store).unwrap();
    assert_eq!(
        store.catalog().unwrap().total_entries(),
        MAX_LOCAL_CHECKPOINT_DIRECTORY_ENTRIES_V0
    );
    drop(lock);

    let full = TestDirectory::new("full-directory-bound");
    for generation in 1..=u64::try_from(MAX_LOCAL_CHECKPOINT_DIRECTORY_ENTRIES_V0).unwrap() {
        File::create(full.path().join(format!("checkpoint-{generation:020}.dcp"))).unwrap();
    }
    let store = LocalCheckpointStoreV0::open(full.path()).unwrap();
    assert_eq!(
        acquire_local_checkpoint_writer_lock_v0(&store).unwrap_err(),
        LocalCheckpointWriterLockErrorV0::DirectoryFull {
            maximum: MAX_LOCAL_CHECKPOINT_DIRECTORY_ENTRIES_V0
        }
    );
}

#[test]
fn persistent_zero_byte_lock_excludes_a_second_writer_and_releases_with_handle() {
    let directory = TestDirectory::new("exclusive");
    let store = LocalCheckpointStoreV0::open(directory.path()).unwrap();
    let first = acquire_local_checkpoint_writer_lock_v0(&store).unwrap();
    let catalog = store.catalog().unwrap();
    assert!(catalog.writer_lock_present());
    assert_eq!(catalog.total_entries(), 1);
    assert_eq!(
        fs::metadata(directory.path().join(LOCAL_CHECKPOINT_WRITER_LOCK_FILE_V0))
            .unwrap()
            .len(),
        0
    );
    assert_eq!(
        acquire_local_checkpoint_writer_lock_v0(&store).unwrap_err(),
        LocalCheckpointWriterLockErrorV0::AlreadyHeld
    );
    let debug = format!("{first:?}");
    assert!(!debug.contains(directory.path().to_str().unwrap()));

    drop(first);
    let reacquired = acquire_local_checkpoint_writer_lock_v0(&store).unwrap();
    assert!(store.catalog().unwrap().writer_lock_present());
    drop(reacquired);
}

#[test]
fn malformed_lock_entries_fail_closed_without_path_disclosure() {
    let nonempty = TestDirectory::new("nonempty");
    let mut file =
        File::create(nonempty.path().join(LOCAL_CHECKPOINT_WRITER_LOCK_FILE_V0)).unwrap();
    file.write_all(b"private-lock-owner").unwrap();
    let store = LocalCheckpointStoreV0::open(nonempty.path()).unwrap();
    let error = acquire_local_checkpoint_writer_lock_v0(&store).unwrap_err();
    assert!(matches!(
        error,
        LocalCheckpointWriterLockErrorV0::Catalog(
            LocalCheckpointCatalogErrorV0::WriterLockNotEmpty { .. }
        )
    ));
    let debug = format!("{error:?}");
    assert!(!debug.contains("private-lock-owner"));
    assert!(!debug.contains(nonempty.path().to_str().unwrap()));

    #[cfg(unix)]
    {
        use std::os::unix::fs::symlink;

        let linked = TestDirectory::new("symlink");
        let target = linked.path().join("private-target");
        File::create(&target).unwrap();
        symlink(
            &target,
            linked.path().join(LOCAL_CHECKPOINT_WRITER_LOCK_FILE_V0),
        )
        .unwrap();
        let store = LocalCheckpointStoreV0::open(linked.path()).unwrap();
        assert_eq!(
            acquire_local_checkpoint_writer_lock_v0(&store).unwrap_err(),
            LocalCheckpointWriterLockErrorV0::Catalog(
                LocalCheckpointCatalogErrorV0::WriterLockNotRegularFile
            )
        );
    }
}
