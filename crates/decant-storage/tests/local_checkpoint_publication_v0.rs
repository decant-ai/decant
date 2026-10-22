use std::fs::{self, File};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use decant_ingest::{KafkaCheckpointGenerationV0, encode_kafka_checkpoint_generation_artifact_v0};
use decant_ir::{KafkaPositionV0, SourceIdV0, SourcePositionKindV0};
use decant_kafka::{
    KafkaClusterIdV0, KafkaPartitionFrontierV0, KafkaSourceContractV0, KafkaTopicIdV0,
};
use decant_state::{AdmissionConfigV0, AdmissionLimitsV0, AdmissionStateV0};
use decant_storage::{
    LocalCheckpointPublicationErrorV0, LocalCheckpointPublicationVisibilityV0,
    LocalCheckpointStoreV0, MAX_LOCAL_CHECKPOINT_DIRECTORY_ENTRIES_V0,
    publish_local_kafka_checkpoint_generation_v0,
};

static NEXT_TEMP: AtomicU64 = AtomicU64::new(0);

struct TestDirectory {
    path: PathBuf,
}

impl TestDirectory {
    fn new(label: &str) -> Self {
        let sequence = NEXT_TEMP.fetch_add(1, Ordering::Relaxed);
        let path = std::env::temp_dir().join(format!(
            "decant-publication-{label}-{}-{sequence}",
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
        if self.path.exists() {
            fs::remove_dir_all(&self.path).unwrap();
        }
    }
}

fn published(generation: u64) -> String {
    format!("checkpoint-{generation:020}.dcp")
}
fn pending(generation: u64) -> String {
    format!("checkpoint-{generation:020}.pending")
}

fn cut() -> (AdmissionStateV0, KafkaPartitionFrontierV0) {
    let source = SourceIdV0::new("synthetic-secret-source").unwrap();
    let admission = AdmissionStateV0::new(
        AdmissionConfigV0::new_with_source_position_kind(
            0,
            source.clone(),
            SourcePositionKindV0::Kafka,
            0,
            100,
            AdmissionLimitsV0::maximum(),
        )
        .unwrap(),
    );
    let frontier = KafkaPartitionFrontierV0::new(
        0,
        KafkaSourceContractV0::new(
            0,
            source,
            KafkaClusterIdV0::new("cluster-v0").unwrap(),
            KafkaTopicIdV0::from_bytes([7; 16]).unwrap(),
        )
        .unwrap(),
        &[KafkaPositionV0::new(0, 0).unwrap()],
    )
    .unwrap();
    (admission, frontier)
}

#[test]
fn complete_publication_is_exact_monotonic_and_has_no_pending_name() {
    let directory = TestDirectory::new("success");
    let store = LocalCheckpointStoreV0::open(directory.path()).unwrap();
    let (admission, frontier) = cut();
    for value in [1, 3] {
        let generation = KafkaCheckpointGenerationV0::new(value).unwrap();
        let receipt =
            publish_local_kafka_checkpoint_generation_v0(&store, generation, &admission, &frontier)
                .unwrap();
        let expected =
            encode_kafka_checkpoint_generation_artifact_v0(generation, &admission, &frontier)
                .unwrap();
        let mut actual = Vec::new();
        File::open(directory.path().join(published(value)))
            .unwrap()
            .read_to_end(&mut actual)
            .unwrap();
        assert_eq!(actual, expected);
        assert_eq!(receipt.generation(), generation);
        assert_eq!(receipt.artifact_bytes(), expected.len());
        assert!(!directory.path().join(pending(value)).exists());
    }
    let catalog = store.catalog().unwrap();
    assert_eq!(
        catalog
            .published()
            .iter()
            .map(|entry| entry.generation().get())
            .collect::<Vec<_>>(),
        vec![1, 3]
    );
    assert!(catalog.pending().is_empty());
}

#[test]
fn same_older_and_pending_generations_refuse_without_mutation() {
    let directory = TestDirectory::new("refusals");
    let store = LocalCheckpointStoreV0::open(directory.path()).unwrap();
    let (admission, frontier) = cut();
    let current = KafkaCheckpointGenerationV0::new(5).unwrap();
    publish_local_kafka_checkpoint_generation_v0(&store, current, &admission, &frontier).unwrap();
    for attempted in [5, 4] {
        assert!(matches!(
            publish_local_kafka_checkpoint_generation_v0(
                &store,
                KafkaCheckpointGenerationV0::new(attempted).unwrap(),
                &admission,
                &frontier
            ),
            Err(LocalCheckpointPublicationErrorV0::GenerationNotAfterLatest { .. })
        ));
    }
    directory.file(&pending(6), b"partial");
    let error = publish_local_kafka_checkpoint_generation_v0(
        &store,
        KafkaCheckpointGenerationV0::new(6).unwrap(),
        &admission,
        &frontier,
    )
    .unwrap_err();
    assert!(matches!(
        error,
        LocalCheckpointPublicationErrorV0::GenerationAlreadyPending { .. }
    ));
    assert_eq!(
        error.visibility(),
        LocalCheckpointPublicationVisibilityV0::NotPublished
    );
    assert_eq!(
        fs::read(directory.path().join(pending(6))).unwrap(),
        b"partial"
    );
    assert!(!directory.path().join(published(6)).exists());
}

#[test]
fn hard_link_conflict_never_overwrites_published_bytes() {
    let directory = TestDirectory::new("conflict");
    let store = LocalCheckpointStoreV0::open(directory.path()).unwrap();
    let (admission, frontier) = cut();
    directory.file(&published(7), b"existing");
    // A published generation is discovered by preflight, so an equal attempt is rejected before I/O.
    let error = publish_local_kafka_checkpoint_generation_v0(
        &store,
        KafkaCheckpointGenerationV0::new(7).unwrap(),
        &admission,
        &frontier,
    )
    .unwrap_err();
    assert!(matches!(
        error,
        LocalCheckpointPublicationErrorV0::GenerationNotAfterLatest { .. }
    ));
    assert_eq!(
        fs::read(directory.path().join(published(7))).unwrap(),
        b"existing"
    );
}

#[test]
fn directory_headroom_and_missing_root_fail_before_success_receipt() {
    let directory = TestDirectory::new("headroom");
    for generation in 1..u64::try_from(MAX_LOCAL_CHECKPOINT_DIRECTORY_ENTRIES_V0).unwrap() {
        directory.file(&published(generation), b"");
    }
    let store = LocalCheckpointStoreV0::open(directory.path()).unwrap();
    let (admission, frontier) = cut();
    let error = publish_local_kafka_checkpoint_generation_v0(
        &store,
        KafkaCheckpointGenerationV0::new(
            u64::try_from(MAX_LOCAL_CHECKPOINT_DIRECTORY_ENTRIES_V0).unwrap(),
        )
        .unwrap(),
        &admission,
        &frontier,
    )
    .unwrap_err();
    assert!(matches!(
        error,
        LocalCheckpointPublicationErrorV0::InsufficientDirectoryEntryHeadroom { .. }
    ));

    let missing = TestDirectory::new("missing");
    let missing_path = missing.path().to_path_buf();
    let missing_store = LocalCheckpointStoreV0::open(&missing_path).unwrap();
    fs::remove_dir(&missing_path).unwrap();
    let error = publish_local_kafka_checkpoint_generation_v0(
        &missing_store,
        KafkaCheckpointGenerationV0::new(1).unwrap(),
        &admission,
        &frontier,
    )
    .unwrap_err();
    assert!(matches!(
        error,
        LocalCheckpointPublicationErrorV0::Catalog(_)
    ));
    assert_eq!(
        error.visibility(),
        LocalCheckpointPublicationVisibilityV0::NotPublished
    );
    assert!(!format!("{error:?}").contains("synthetic-secret"));
}
