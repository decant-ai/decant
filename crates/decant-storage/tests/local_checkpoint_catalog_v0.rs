use std::fs::{self, File};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use decant_ingest::MAX_KAFKA_CHECKPOINT_GENERATION_ARTIFACT_BYTES_V0;
use decant_storage::{
    LOCAL_CHECKPOINT_STORE_VERSION_V0, LocalCheckpointCatalogEntryKindV0,
    LocalCheckpointCatalogErrorV0, LocalCheckpointStoreV0,
    MAX_LOCAL_CHECKPOINT_DIRECTORY_ENTRIES_V0, MAX_LOCAL_CHECKPOINT_ROOT_PATH_BYTES_V0,
};

static NEXT_TEMP: AtomicU64 = AtomicU64::new(0);

struct TestDirectory {
    path: PathBuf,
}

impl TestDirectory {
    fn new(label: &str) -> Self {
        let sequence = NEXT_TEMP.fetch_add(1, Ordering::Relaxed);
        let path = std::env::temp_dir().join(format!(
            "decant-storage-{label}-{}-{sequence}",
            std::process::id()
        ));
        fs::create_dir(&path).unwrap();
        Self { path }
    }

    fn path(&self) -> &Path {
        &self.path
    }

    fn file(&self, name: &str, contents: &[u8]) {
        let mut file = File::create(self.path.join(name)).unwrap();
        file.write_all(contents).unwrap();
    }
}

impl Drop for TestDirectory {
    fn drop(&mut self) {
        fs::remove_dir_all(&self.path).unwrap();
    }
}

fn published(generation: u64) -> String {
    format!("checkpoint-{generation:020}.dcp")
}
fn pending(generation: u64) -> String {
    format!("checkpoint-{generation:020}.pending")
}

#[test]
fn empty_and_mixed_catalogs_are_bounded_sorted_and_repeatable() {
    assert_eq!(LOCAL_CHECKPOINT_STORE_VERSION_V0, 0);
    let directory = TestDirectory::new("catalog");
    let store = LocalCheckpointStoreV0::open(directory.path()).unwrap();
    assert_eq!(store.catalog().unwrap().total_entries(), 0);
    directory.file(&published(10), b"ten");
    directory.file(&pending(2), b"pending");
    directory.file(&published(2), b"two");
    let first = store.catalog().unwrap();
    assert_eq!(first.total_entries(), 3);
    assert_eq!(
        first
            .published()
            .iter()
            .map(|entry| entry.generation().get())
            .collect::<Vec<_>>(),
        vec![2, 10]
    );
    assert_eq!(
        first
            .pending()
            .iter()
            .map(|entry| entry.generation().get())
            .collect::<Vec<_>>(),
        vec![2]
    );
    assert_eq!(
        first.published()[0].kind(),
        LocalCheckpointCatalogEntryKindV0::Published
    );
    assert_eq!(
        first.pending()[0].kind(),
        LocalCheckpointCatalogEntryKindV0::Pending
    );
    assert_eq!(first.latest_published().unwrap().generation().get(), 10);
    assert_eq!(first.published()[0].artifact_bytes(), 3);
    let second = store.catalog().unwrap();
    assert_eq!(format!("{first:?}"), format!("{second:?}"));
    assert!(!format!("{store:?}").contains(directory.path().to_str().unwrap()));
}

#[test]
fn root_validation_is_explicit_and_redacted() {
    let directory = TestDirectory::new("roots");
    let missing = directory.path().join("synthetic-secret-missing");
    let error = LocalCheckpointStoreV0::open(&missing).unwrap_err();
    assert!(!format!("{error:?}").contains("synthetic-secret"));
    let plain_file = directory.path().join("plain");
    File::create(&plain_file).unwrap();
    assert_eq!(
        LocalCheckpointStoreV0::open(&plain_file).unwrap_err(),
        LocalCheckpointCatalogErrorV0::RootNotDirectory
    );
    let too_long = PathBuf::from("x".repeat(MAX_LOCAL_CHECKPOINT_ROOT_PATH_BYTES_V0 + 1));
    assert!(matches!(
        LocalCheckpointStoreV0::open(&too_long),
        Err(LocalCheckpointCatalogErrorV0::PathTooLong { .. })
    ));

    #[cfg(unix)]
    {
        use std::os::unix::fs::symlink;
        let link = directory.path().join("root-link");
        symlink(directory.path(), &link).unwrap();
        assert_eq!(
            LocalCheckpointStoreV0::open(&link).unwrap_err(),
            LocalCheckpointCatalogErrorV0::RootIsSymlink
        );
    }
}

#[test]
fn malformed_unknown_and_non_regular_entries_fail_closed() {
    for (label, name, expected) in [
        (
            "unknown",
            "README",
            LocalCheckpointCatalogErrorV0::UnknownEntryName,
        ),
        (
            "short",
            "checkpoint-1.dcp",
            LocalCheckpointCatalogErrorV0::InvalidGenerationName,
        ),
        (
            "zero",
            "checkpoint-00000000000000000000.dcp",
            LocalCheckpointCatalogErrorV0::ZeroGeneration,
        ),
    ] {
        let directory = TestDirectory::new(label);
        directory.file(name, b"");
        let store = LocalCheckpointStoreV0::open(directory.path()).unwrap();
        assert_eq!(store.catalog().unwrap_err(), expected);
    }
    let directory = TestDirectory::new("subdirectory");
    fs::create_dir(directory.path().join(published(1))).unwrap();
    let store = LocalCheckpointStoreV0::open(directory.path()).unwrap();
    assert!(matches!(
        store.catalog(),
        Err(LocalCheckpointCatalogErrorV0::EntryNotRegularFile { .. })
    ));
}

#[test]
fn artifact_metadata_and_directory_count_bounds_are_exact() {
    let oversized_directory = TestDirectory::new("oversized");
    let oversized = File::create(oversized_directory.path().join(published(1))).unwrap();
    oversized
        .set_len(u64::try_from(MAX_KAFKA_CHECKPOINT_GENERATION_ARTIFACT_BYTES_V0 + 1).unwrap())
        .unwrap();
    let store = LocalCheckpointStoreV0::open(oversized_directory.path()).unwrap();
    assert!(matches!(
        store.catalog(),
        Err(LocalCheckpointCatalogErrorV0::ArtifactTooLarge { .. })
    ));

    let exact_directory = TestDirectory::new("exact-count");
    for generation in 1..=u64::try_from(MAX_LOCAL_CHECKPOINT_DIRECTORY_ENTRIES_V0).unwrap() {
        exact_directory.file(&published(generation), b"");
    }
    let store = LocalCheckpointStoreV0::open(exact_directory.path()).unwrap();
    assert_eq!(
        store.catalog().unwrap().total_entries(),
        MAX_LOCAL_CHECKPOINT_DIRECTORY_ENTRIES_V0
    );
    exact_directory.file(&pending(1), b"");
    assert_eq!(
        store.catalog().unwrap_err(),
        LocalCheckpointCatalogErrorV0::TooManyDirectoryEntries {
            maximum: MAX_LOCAL_CHECKPOINT_DIRECTORY_ENTRIES_V0
        }
    );
}
