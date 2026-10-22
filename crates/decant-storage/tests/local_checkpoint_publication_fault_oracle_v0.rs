use std::fs::{self, File};
use std::io::Read;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use decant_ingest::{KafkaCheckpointGenerationV0, encode_kafka_checkpoint_generation_artifact_v0};
use decant_ir::{KafkaPositionV0, SourceIdV0, SourcePositionKindV0};
use decant_kafka::{
    KafkaClusterIdV0, KafkaPartitionFrontierV0, KafkaSourceContractV0, KafkaTopicIdV0,
};
use decant_state::{AdmissionConfigV0, AdmissionLimitsV0, AdmissionStateV0};
use decant_storage::{
    LocalCheckpointPublicationErrorV0, LocalCheckpointPublicationFaultPointV0,
    LocalCheckpointPublicationVisibilityV0, LocalCheckpointRecoveryErrorV0, LocalCheckpointStoreV0,
    publish_local_kafka_checkpoint_generation_v0,
    publish_local_kafka_checkpoint_generation_with_fault_v0,
    restore_latest_local_kafka_checkpoint_generation_v0,
};

static NEXT_TEMP: AtomicU64 = AtomicU64::new(0);

struct TestDirectory {
    path: PathBuf,
}
impl TestDirectory {
    fn new(label: &str) -> Self {
        let sequence = NEXT_TEMP.fetch_add(1, Ordering::Relaxed);
        let path = std::env::temp_dir().join(format!(
            "decant-fault-{label}-{}-{sequence}",
            std::process::id()
        ));
        fs::create_dir(&path).unwrap();
        Self { path }
    }
    fn path(&self) -> &Path {
        &self.path
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

fn cut() -> (
    AdmissionConfigV0,
    AdmissionStateV0,
    KafkaPartitionFrontierV0,
) {
    let source = SourceIdV0::new("synthetic-secret-source").unwrap();
    let config = AdmissionConfigV0::new_with_source_position_kind(
        0,
        source.clone(),
        SourcePositionKindV0::Kafka,
        0,
        100,
        AdmissionLimitsV0::maximum(),
    )
    .unwrap();
    let admission = AdmissionStateV0::new(config.clone());
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
    (config, admission, frontier)
}

fn read(path: &Path) -> Vec<u8> {
    let mut bytes = Vec::new();
    File::open(path).unwrap().read_to_end(&mut bytes).unwrap();
    bytes
}

#[test]
fn every_pre_link_fault_leaves_only_explicit_pending_evidence() {
    let points = [
        LocalCheckpointPublicationFaultPointV0::AfterPendingCreation,
        LocalCheckpointPublicationFaultPointV0::AfterPartialPendingWrite,
        LocalCheckpointPublicationFaultPointV0::AfterCompletePendingWrite,
        LocalCheckpointPublicationFaultPointV0::AfterPendingSync,
    ];
    for _ in 0..2 {
        for point in points {
            let directory = TestDirectory::new("pre-link");
            let store = LocalCheckpointStoreV0::open(directory.path()).unwrap();
            let (config, admission, frontier) = cut();
            let generation = KafkaCheckpointGenerationV0::new(1).unwrap();
            let expected =
                encode_kafka_checkpoint_generation_artifact_v0(generation, &admission, &frontier)
                    .unwrap();
            let error = publish_local_kafka_checkpoint_generation_with_fault_v0(
                &store, generation, &admission, &frontier, point,
            )
            .unwrap_err();
            assert_eq!(
                error,
                LocalCheckpointPublicationErrorV0::InjectedFault {
                    point,
                    visibility: LocalCheckpointPublicationVisibilityV0::NotPublished
                }
            );
            assert_eq!(
                error.visibility(),
                LocalCheckpointPublicationVisibilityV0::NotPublished
            );
            assert!(!directory.path().join(published(1)).exists());
            let pending_bytes = read(&directory.path().join(pending(1)));
            match point {
                LocalCheckpointPublicationFaultPointV0::AfterPendingCreation => {
                    assert!(pending_bytes.is_empty())
                }
                LocalCheckpointPublicationFaultPointV0::AfterPartialPendingWrite => {
                    assert!(!pending_bytes.is_empty());
                    assert!(pending_bytes.len() < expected.len());
                    assert_eq!(pending_bytes, expected[..expected.len() / 2]);
                }
                LocalCheckpointPublicationFaultPointV0::AfterCompletePendingWrite
                | LocalCheckpointPublicationFaultPointV0::AfterPendingSync => {
                    assert_eq!(pending_bytes, expected)
                }
                _ => unreachable!(),
            }
            assert_eq!(store.catalog().unwrap().pending().len(), 1);
            assert_eq!(
                restore_latest_local_kafka_checkpoint_generation_v0(
                    &store,
                    &config,
                    frontier.source_contract()
                )
                .unwrap_err(),
                LocalCheckpointRecoveryErrorV0::NoPublishedGeneration { pending_entries: 1 }
            );
            assert!(!format!("{error:?}").contains("synthetic-secret"));
        }
    }
}

#[test]
fn every_post_link_fault_has_a_complete_strictly_recoverable_generation() {
    let points = [
        (
            LocalCheckpointPublicationFaultPointV0::AfterPublishedLink,
            1_usize,
        ),
        (
            LocalCheckpointPublicationFaultPointV0::AfterPendingRemoval,
            0,
        ),
        (
            LocalCheckpointPublicationFaultPointV0::AfterDirectorySyncBeforeReceipt,
            0,
        ),
    ];
    for _ in 0..2 {
        for (point, expected_pending) in points {
            let directory = TestDirectory::new("post-link");
            let store = LocalCheckpointStoreV0::open(directory.path()).unwrap();
            let (config, admission, frontier) = cut();
            let generation = KafkaCheckpointGenerationV0::new(1).unwrap();
            let expected =
                encode_kafka_checkpoint_generation_artifact_v0(generation, &admission, &frontier)
                    .unwrap();
            let error = publish_local_kafka_checkpoint_generation_with_fault_v0(
                &store, generation, &admission, &frontier, point,
            )
            .unwrap_err();
            assert_eq!(
                error,
                LocalCheckpointPublicationErrorV0::InjectedFault {
                    point,
                    visibility: LocalCheckpointPublicationVisibilityV0::Uncertain
                }
            );
            assert_eq!(read(&directory.path().join(published(1))), expected);
            let recovered = restore_latest_local_kafka_checkpoint_generation_v0(
                &store,
                &config,
                frontier.source_contract(),
            )
            .unwrap();
            assert_eq!(recovered.generation(), generation);
            assert_eq!(recovered.pending_entries(), expected_pending);
            assert_eq!(recovered.artifact_bytes(), expected.len());
            assert_eq!(
                encode_kafka_checkpoint_generation_artifact_v0(
                    recovered.generation(),
                    recovered.admission(),
                    recovered.frontier(),
                )
                .unwrap(),
                expected
            );
            assert_eq!(
                directory.path().join(pending(1)).exists(),
                expected_pending == 1
            );
            assert!(matches!(
                publish_local_kafka_checkpoint_generation_v0(
                    &store, generation, &admission, &frontier
                ),
                Err(LocalCheckpointPublicationErrorV0::GenerationNotAfterLatest { .. })
            ));
        }
    }
}

#[test]
fn ordinary_publication_still_returns_receipt_and_recovers_exactly() {
    for _ in 0..2 {
        let directory = TestDirectory::new("ordinary");
        let store = LocalCheckpointStoreV0::open(directory.path()).unwrap();
        let (config, admission, frontier) = cut();
        let generation = KafkaCheckpointGenerationV0::new(1).unwrap();
        let expected =
            encode_kafka_checkpoint_generation_artifact_v0(generation, &admission, &frontier)
                .unwrap();
        let receipt =
            publish_local_kafka_checkpoint_generation_v0(&store, generation, &admission, &frontier)
                .unwrap();
        assert_eq!(receipt.artifact_bytes(), expected.len());
        assert_eq!(read(&directory.path().join(published(1))), expected);
        let recovered = restore_latest_local_kafka_checkpoint_generation_v0(
            &store,
            &config,
            frontier.source_contract(),
        )
        .unwrap();
        assert_eq!(recovered.generation(), receipt.generation());
        assert_eq!(recovered.pending_entries(), 0);
    }
}
