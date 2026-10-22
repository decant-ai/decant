use std::fs::{self, File, OpenOptions};
use std::io::{Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use decant_ingest::KafkaCheckpointGenerationV0;
use decant_ir::{KafkaPositionV0, SourceIdV0, SourcePositionKindV0};
use decant_kafka::{
    KafkaClusterIdV0, KafkaPartitionFrontierV0, KafkaSourceContractV0, KafkaTopicIdV0,
};
use decant_state::{AdmissionConfigV0, AdmissionLimitsV0, AdmissionStateV0};
use decant_storage::{
    LocalCheckpointPruneErrorV0, LocalCheckpointStoreV0, MAX_LOCAL_CHECKPOINT_DIRECTORY_ENTRIES_V0,
    acquire_local_checkpoint_writer_lock_v0, prune_verified_local_kafka_checkpoints_v0,
    publish_local_kafka_checkpoint_generation_v0,
    restore_latest_local_kafka_checkpoint_generation_v0,
};

static NEXT_TEMP: AtomicU64 = AtomicU64::new(0);

struct TestDirectory(PathBuf);

impl TestDirectory {
    fn new(label: &str) -> Self {
        let sequence = NEXT_TEMP.fetch_add(1, Ordering::Relaxed);
        let path = std::env::temp_dir().join(format!(
            "decant-prune-{label}-{}-{sequence}",
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

fn contract() -> (
    AdmissionConfigV0,
    AdmissionStateV0,
    KafkaSourceContractV0,
    KafkaPartitionFrontierV0,
) {
    let source = SourceIdV0::new("private-prune-source").unwrap();
    let admission_config = AdmissionConfigV0::new_with_source_position_kind(
        0,
        source.clone(),
        SourcePositionKindV0::Kafka,
        0,
        100,
        AdmissionLimitsV0::maximum(),
    )
    .unwrap();
    let source_contract = KafkaSourceContractV0::new(
        0,
        source,
        KafkaClusterIdV0::new("private-prune-cluster").unwrap(),
        KafkaTopicIdV0::from_bytes([0x51; 16]).unwrap(),
    )
    .unwrap();
    let frontier = KafkaPartitionFrontierV0::new(
        0,
        source_contract.clone(),
        &[KafkaPositionV0::new(0, 0).unwrap()],
    )
    .unwrap();
    (
        admission_config.clone(),
        AdmissionStateV0::new(admission_config),
        source_contract,
        frontier,
    )
}

fn publish_three(
    store: &LocalCheckpointStoreV0,
    admission: &AdmissionStateV0,
    frontier: &KafkaPartitionFrontierV0,
) {
    for generation in 1..=3 {
        publish_local_kafka_checkpoint_generation_v0(
            store,
            KafkaCheckpointGenerationV0::new(generation).unwrap(),
            admission,
            frontier,
        )
        .unwrap();
    }
}

fn generations(store: &LocalCheckpointStoreV0) -> Vec<u64> {
    store
        .catalog()
        .unwrap()
        .published()
        .iter()
        .map(|entry| entry.generation().get())
        .collect()
}

#[test]
fn verified_pruning_keeps_exact_newest_suffix_and_latest_recovery() {
    let directory = TestDirectory::new("success");
    let store = LocalCheckpointStoreV0::open(directory.path()).unwrap();
    let lock = acquire_local_checkpoint_writer_lock_v0(&store).unwrap();
    let (admission_config, admission, source_contract, frontier) = contract();
    publish_three(&store, &admission, &frontier);
    let latest_before = restore_latest_local_kafka_checkpoint_generation_v0(
        &store,
        &admission_config,
        &source_contract,
    )
    .unwrap();

    let receipt = prune_verified_local_kafka_checkpoints_v0(
        &lock,
        &store,
        2,
        &admission_config,
        &source_contract,
    )
    .unwrap();
    assert_eq!(receipt.latest_generation().get(), 3);
    assert_eq!(receipt.retained_generations(), 2);
    assert_eq!(receipt.removed_generations(), 1);
    assert_eq!(generations(&store), vec![2, 3]);
    let latest_after = restore_latest_local_kafka_checkpoint_generation_v0(
        &store,
        &admission_config,
        &source_contract,
    )
    .unwrap();
    assert_eq!(latest_after.generation(), latest_before.generation());
    assert_eq!(
        latest_after.checkpoint_hash(),
        latest_before.checkpoint_hash()
    );
    assert_eq!(
        latest_after.artifact_bytes(),
        latest_before.artifact_bytes()
    );
}

#[test]
fn pruning_requires_matching_lock_bounds_and_unambiguous_catalog() {
    let left_directory = TestDirectory::new("left");
    let right_directory = TestDirectory::new("right");
    let left = LocalCheckpointStoreV0::open(left_directory.path()).unwrap();
    let right = LocalCheckpointStoreV0::open(right_directory.path()).unwrap();
    let left_lock = acquire_local_checkpoint_writer_lock_v0(&left).unwrap();
    let right_lock = acquire_local_checkpoint_writer_lock_v0(&right).unwrap();
    let (admission_config, admission, source_contract, frontier) = contract();
    publish_three(&left, &admission, &frontier);

    assert_eq!(
        prune_verified_local_kafka_checkpoints_v0(
            &right_lock,
            &left,
            1,
            &admission_config,
            &source_contract,
        )
        .unwrap_err(),
        LocalCheckpointPruneErrorV0::WriterLockMismatch
    );
    for retain in [0, MAX_LOCAL_CHECKPOINT_DIRECTORY_ENTRIES_V0 + 1] {
        assert!(matches!(
            prune_verified_local_kafka_checkpoints_v0(
                &left_lock,
                &left,
                retain,
                &admission_config,
                &source_contract,
            ),
            Err(LocalCheckpointPruneErrorV0::RetainCountOutOfRange { .. })
        ));
    }
    File::create(
        left_directory
            .path()
            .join("checkpoint-00000000000000000004.pending"),
    )
    .unwrap();
    assert_eq!(
        prune_verified_local_kafka_checkpoints_v0(
            &left_lock,
            &left,
            1,
            &admission_config,
            &source_contract,
        )
        .unwrap_err(),
        LocalCheckpointPruneErrorV0::PendingEntries { count: 1 }
    );
    assert_eq!(generations(&left), vec![1, 2, 3]);

    assert_eq!(
        prune_verified_local_kafka_checkpoints_v0(
            &right_lock,
            &right,
            1,
            &admission_config,
            &source_contract,
        )
        .unwrap_err(),
        LocalCheckpointPruneErrorV0::NoPublishedGeneration
    );
}

#[test]
fn corrupt_latest_refuses_before_any_generation_is_removed() {
    let directory = TestDirectory::new("corrupt");
    let store = LocalCheckpointStoreV0::open(directory.path()).unwrap();
    let lock = acquire_local_checkpoint_writer_lock_v0(&store).unwrap();
    let (admission_config, admission, source_contract, frontier) = contract();
    publish_three(&store, &admission, &frontier);
    let latest = directory.path().join("checkpoint-00000000000000000003.dcp");
    let mut file = OpenOptions::new().write(true).open(latest).unwrap();
    file.seek(SeekFrom::Start(0)).unwrap();
    file.write_all(b"BROKEN!").unwrap();
    file.sync_all().unwrap();

    assert!(matches!(
        prune_verified_local_kafka_checkpoints_v0(
            &lock,
            &store,
            1,
            &admission_config,
            &source_contract,
        ),
        Err(LocalCheckpointPruneErrorV0::LatestRecovery(_))
    ));
    assert_eq!(generations(&store), vec![1, 2, 3]);
}
