use std::fs::{self, File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use decant_ingest::{
    KafkaCheckpointGenerationV0, MAX_KAFKA_CHECKPOINT_GENERATION_ARTIFACT_BYTES_V0,
    encode_kafka_checkpoint_generation_artifact_v0,
};
use decant_ir::{KafkaPositionV0, SourceIdV0, SourcePositionKindV0};
use decant_kafka::{
    KafkaClusterIdV0, KafkaPartitionFrontierV0, KafkaSourceContractV0, KafkaTopicIdV0,
};
use decant_state::{
    AdmissionConfigV0, AdmissionLimitsV0, AdmissionStateV0, encode_admission_snapshot_v1,
};
use decant_storage::{
    LocalCheckpointRecoveryErrorV0, LocalCheckpointStoreV0,
    publish_local_kafka_checkpoint_generation_v0,
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
            "decant-recovery-{label}-{}-{sequence}",
            std::process::id()
        ));
        fs::create_dir(&path).unwrap();
        Self { path }
    }
    fn path(&self) -> &Path {
        &self.path
    }
    fn file(&self, name: &str, bytes: &[u8]) {
        fs::write(self.path.join(name), bytes).unwrap();
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

fn expected_config(source: &str, kind: SourcePositionKindV0) -> AdmissionConfigV0 {
    AdmissionConfigV0::new_with_source_position_kind(
        0,
        SourceIdV0::new(source).unwrap(),
        kind,
        0,
        100,
        AdmissionLimitsV0::maximum(),
    )
    .unwrap()
}

fn source_contract(source: &str) -> KafkaSourceContractV0 {
    KafkaSourceContractV0::new(
        0,
        SourceIdV0::new(source).unwrap(),
        KafkaClusterIdV0::new("cluster-v0").unwrap(),
        KafkaTopicIdV0::from_bytes([7; 16]).unwrap(),
    )
    .unwrap()
}

fn cut() -> (
    AdmissionConfigV0,
    AdmissionStateV0,
    KafkaPartitionFrontierV0,
) {
    let config = expected_config("synthetic-secret-source", SourcePositionKindV0::Kafka);
    let admission = AdmissionStateV0::new(config.clone());
    let frontier = KafkaPartitionFrontierV0::new(
        0,
        source_contract("synthetic-secret-source"),
        &[KafkaPositionV0::new(0, 0).unwrap()],
    )
    .unwrap();
    (config, admission, frontier)
}

#[test]
fn empty_and_pending_only_roots_have_explicit_no_published_result() {
    let directory = TestDirectory::new("none");
    let store = LocalCheckpointStoreV0::open(directory.path()).unwrap();
    let (config, _, frontier) = cut();
    assert_eq!(
        restore_latest_local_kafka_checkpoint_generation_v0(
            &store,
            &config,
            frontier.source_contract()
        )
        .unwrap_err(),
        LocalCheckpointRecoveryErrorV0::NoPublishedGeneration { pending_entries: 0 }
    );
    directory.file(&pending(1), b"partial");
    assert_eq!(
        restore_latest_local_kafka_checkpoint_generation_v0(
            &store,
            &config,
            frontier.source_contract()
        )
        .unwrap_err(),
        LocalCheckpointRecoveryErrorV0::NoPublishedGeneration { pending_entries: 1 }
    );
}

#[test]
fn greatest_published_generation_restores_exactly_and_reports_pending() {
    let directory = TestDirectory::new("latest");
    let store = LocalCheckpointStoreV0::open(directory.path()).unwrap();
    let (config, admission, frontier) = cut();
    for value in [1, 3] {
        publish_local_kafka_checkpoint_generation_v0(
            &store,
            KafkaCheckpointGenerationV0::new(value).unwrap(),
            &admission,
            &frontier,
        )
        .unwrap();
    }
    directory.file(&pending(4), b"partial");
    let recovered = restore_latest_local_kafka_checkpoint_generation_v0(
        &store,
        &config,
        frontier.source_contract(),
    )
    .unwrap();
    assert_eq!(recovered.generation().get(), 3);
    assert_eq!(recovered.pending_entries(), 1);
    assert_eq!(
        encode_admission_snapshot_v1(recovered.admission()).unwrap(),
        encode_admission_snapshot_v1(&admission).unwrap()
    );
    let expected = encode_kafka_checkpoint_generation_artifact_v0(
        recovered.generation(),
        recovered.admission(),
        recovered.frontier(),
    )
    .unwrap();
    assert_eq!(recovered.artifact_bytes(), expected.len());
    let repeated = restore_latest_local_kafka_checkpoint_generation_v0(
        &store,
        &config,
        frontier.source_contract(),
    )
    .unwrap();
    assert_eq!(repeated.checkpoint_hash(), recovered.checkpoint_hash());
    let debug = format!("{recovered:?}");
    assert!(debug.contains("<redacted>"));
    assert!(!debug.contains("synthetic-secret"));
}

#[test]
fn corrupt_latest_never_falls_back_to_valid_older_generation() {
    let directory = TestDirectory::new("corrupt-latest");
    let store = LocalCheckpointStoreV0::open(directory.path()).unwrap();
    let (config, admission, frontier) = cut();
    for value in [1, 2] {
        publish_local_kafka_checkpoint_generation_v0(
            &store,
            KafkaCheckpointGenerationV0::new(value).unwrap(),
            &admission,
            &frontier,
        )
        .unwrap();
    }
    let path = directory.path().join(published(2));
    let mut file = OpenOptions::new()
        .read(true)
        .write(true)
        .open(path)
        .unwrap();
    file.seek(SeekFrom::Start(30)).unwrap();
    let mut byte = [0_u8; 1];
    file.read_exact(&mut byte).unwrap();
    file.seek(SeekFrom::Start(30)).unwrap();
    file.write_all(&[byte[0] ^ 1]).unwrap();
    file.sync_all().unwrap();
    assert!(matches!(
        restore_latest_local_kafka_checkpoint_generation_v0(
            &store,
            &config,
            frontier.source_contract()
        ),
        Err(LocalCheckpointRecoveryErrorV0::ArtifactDecode(_))
    ));
    assert!(directory.path().join(published(1)).exists());
}

#[test]
fn malformed_latest_and_namespace_errors_fail_closed() {
    for (label, bytes) in [
        ("truncated", vec![0; 10]),
        ("trailing", {
            let (_, admission, frontier) = cut();
            let mut bytes = encode_kafka_checkpoint_generation_artifact_v0(
                KafkaCheckpointGenerationV0::new(1).unwrap(),
                &admission,
                &frontier,
            )
            .unwrap();
            bytes.push(0);
            bytes
        }),
    ] {
        let directory = TestDirectory::new(label);
        directory.file(&published(1), &bytes);
        let store = LocalCheckpointStoreV0::open(directory.path()).unwrap();
        let (config, _, frontier) = cut();
        assert!(matches!(
            restore_latest_local_kafka_checkpoint_generation_v0(
                &store,
                &config,
                frontier.source_contract()
            ),
            Err(LocalCheckpointRecoveryErrorV0::ArtifactDecode(_))
        ));
    }

    let directory = TestDirectory::new("unknown");
    directory.file("README", b"");
    let store = LocalCheckpointStoreV0::open(directory.path()).unwrap();
    let (config, _, frontier) = cut();
    assert!(matches!(
        restore_latest_local_kafka_checkpoint_generation_v0(
            &store,
            &config,
            frontier.source_contract()
        ),
        Err(LocalCheckpointRecoveryErrorV0::Catalog(_))
    ));

    let oversized_directory = TestDirectory::new("oversized");
    let oversized = File::create(oversized_directory.path().join(published(1))).unwrap();
    oversized
        .set_len(u64::try_from(MAX_KAFKA_CHECKPOINT_GENERATION_ARTIFACT_BYTES_V0 + 1).unwrap())
        .unwrap();
    let oversized_store = LocalCheckpointStoreV0::open(oversized_directory.path()).unwrap();
    assert!(matches!(
        restore_latest_local_kafka_checkpoint_generation_v0(
            &oversized_store,
            &config,
            frontier.source_contract()
        ),
        Err(LocalCheckpointRecoveryErrorV0::Catalog(_))
    ));
}

#[test]
fn trusted_expectations_precede_untrusted_directory_state() {
    let directory = TestDirectory::new("precedence");
    directory.file("README", b"");
    let store = LocalCheckpointStoreV0::open(directory.path()).unwrap();
    let (_, _, frontier) = cut();
    let error = restore_latest_local_kafka_checkpoint_generation_v0(
        &store,
        &expected_config("synthetic-secret-source", SourcePositionKindV0::Synthetic),
        frontier.source_contract(),
    )
    .unwrap_err();
    assert!(matches!(
        error,
        LocalCheckpointRecoveryErrorV0::ExpectedContract(_)
    ));
    assert!(!format!("{error:?}").contains("synthetic-secret"));
}
