use std::fs::{self, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use decant_ingest::KafkaCheckpointGenerationV0;
use decant_ir::{KafkaPositionV0, SourceIdV0, SourcePositionKindV0};
use decant_kafka::{
    KafkaClusterIdV0, KafkaOffsetObservationV0, KafkaPartitionFrontierV0, KafkaSourceContractV0,
    KafkaTopicIdV0, hash_kafka_partition_frontier_v0,
};
use decant_state::{AdmissionConfigV0, AdmissionLimitsV0, AdmissionStateV0};
use decant_storage::{
    LocalCheckpointRecoveryErrorV0, LocalCheckpointStoreV0,
    LocalKafkaCheckpointCommitCandidateErrorV0, publish_local_kafka_checkpoint_generation_v0,
    recover_latest_local_kafka_checkpoint_commit_candidate_v0,
};

static NEXT_TEMP: AtomicU64 = AtomicU64::new(0);

struct TestDirectory {
    path: PathBuf,
}

impl TestDirectory {
    fn new(label: &str) -> Self {
        let sequence = NEXT_TEMP.fetch_add(1, Ordering::Relaxed);
        let path = std::env::temp_dir().join(format!(
            "decant-commit-candidate-{label}-{}-{sequence}",
            std::process::id()
        ));
        fs::create_dir(&path).unwrap();
        Self { path }
    }

    fn path(&self) -> &Path {
        &self.path
    }

    fn write(&self, name: &str, bytes: &[u8]) {
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

fn source() -> SourceIdV0 {
    SourceIdV0::new("candidate-source-secret").unwrap()
}

fn cluster() -> KafkaClusterIdV0 {
    KafkaClusterIdV0::new("candidate-cluster-secret").unwrap()
}

fn topic() -> KafkaTopicIdV0 {
    KafkaTopicIdV0::from_bytes([0x5a; 16]).unwrap()
}

fn config(kind: SourcePositionKindV0) -> AdmissionConfigV0 {
    AdmissionConfigV0::new_with_source_position_kind(
        0,
        source(),
        kind,
        0,
        100,
        AdmissionLimitsV0::maximum(),
    )
    .unwrap()
}

fn contract() -> KafkaSourceContractV0 {
    KafkaSourceContractV0::new(0, source(), cluster(), topic()).unwrap()
}

fn cut() -> (
    AdmissionConfigV0,
    AdmissionStateV0,
    KafkaPartitionFrontierV0,
) {
    let config = config(SourcePositionKindV0::Kafka);
    let admission = AdmissionStateV0::new(config.clone());
    let frontier = KafkaPartitionFrontierV0::new(
        0,
        contract(),
        &[
            KafkaPositionV0::new(9, 90).unwrap(),
            KafkaPositionV0::new(1, 10).unwrap(),
        ],
    )
    .unwrap();
    (config, admission, frontier)
}

#[test]
fn latest_published_cut_yields_exact_repeatable_candidate() {
    let directory = TestDirectory::new("exact");
    let store = LocalCheckpointStoreV0::open(directory.path()).unwrap();
    let (config, admission, frontier) = cut();
    for generation in [1, 3] {
        publish_local_kafka_checkpoint_generation_v0(
            &store,
            KafkaCheckpointGenerationV0::new(generation).unwrap(),
            &admission,
            &frontier,
        )
        .unwrap();
    }

    let first = recover_latest_local_kafka_checkpoint_commit_candidate_v0(
        &store,
        &config,
        frontier.source_contract(),
    )
    .unwrap();
    let second = recover_latest_local_kafka_checkpoint_commit_candidate_v0(
        &store,
        &config,
        frontier.source_contract(),
    )
    .unwrap();

    assert_eq!(first.generation().get(), 3);
    assert_eq!(first.pending_entries(), 0);
    assert_eq!(
        first.offset_commit_plan().iter().collect::<Vec<_>>(),
        vec![(1, 10), (9, 90)]
    );
    assert_eq!(
        first.offset_commit_plan().frontier_hash(),
        hash_kafka_partition_frontier_v0(first.frontier()).unwrap()
    );
    assert_eq!(
        first.offset_commit_plan().source_contract(),
        first.frontier().source_contract()
    );
    assert_eq!(first.generation(), second.generation());
    assert_eq!(first.checkpoint_hash(), second.checkpoint_hash());
    assert_eq!(first.offset_commit_plan(), second.offset_commit_plan());
    assert_eq!(first.artifact_bytes(), second.artifact_bytes());
    assert_eq!(first.pending_entries(), second.pending_entries());
}

#[test]
fn candidate_comes_from_recovery_not_a_later_live_frontier() {
    let directory = TestDirectory::new("frozen");
    let store = LocalCheckpointStoreV0::open(directory.path()).unwrap();
    let (config, admission, mut live_frontier) = cut();
    publish_local_kafka_checkpoint_generation_v0(
        &store,
        KafkaCheckpointGenerationV0::new(1).unwrap(),
        &admission,
        &live_frontier,
    )
    .unwrap();
    assert_eq!(
        live_frontier.observe(
            &source(),
            &cluster(),
            topic(),
            KafkaPositionV0::new(1, 10).unwrap()
        ),
        Ok(KafkaOffsetObservationV0::Advanced)
    );

    let candidate = recover_latest_local_kafka_checkpoint_commit_candidate_v0(
        &store,
        &config,
        live_frontier.source_contract(),
    )
    .unwrap();
    assert_eq!(live_frontier.next_offset(1), Some(11));
    assert_eq!(candidate.frontier().next_offset(1), Some(10));
    assert_eq!(candidate.offset_commit_plan().next_offset(1), Some(10));
}

#[test]
fn pending_evidence_is_visible_but_never_selected() {
    let directory = TestDirectory::new("pending");
    let store = LocalCheckpointStoreV0::open(directory.path()).unwrap();
    let (config, admission, frontier) = cut();
    let empty = recover_latest_local_kafka_checkpoint_commit_candidate_v0(
        &store,
        &config,
        frontier.source_contract(),
    )
    .unwrap_err();
    assert_eq!(
        empty,
        LocalKafkaCheckpointCommitCandidateErrorV0::Recovery(
            LocalCheckpointRecoveryErrorV0::NoPublishedGeneration { pending_entries: 0 }
        )
    );

    directory.write(&pending(1), b"partial-secret-pending");
    let pending_only = recover_latest_local_kafka_checkpoint_commit_candidate_v0(
        &store,
        &config,
        frontier.source_contract(),
    )
    .unwrap_err();
    assert_eq!(
        pending_only,
        LocalKafkaCheckpointCommitCandidateErrorV0::Recovery(
            LocalCheckpointRecoveryErrorV0::NoPublishedGeneration { pending_entries: 1 }
        )
    );

    fs::remove_file(directory.path().join(pending(1))).unwrap();
    publish_local_kafka_checkpoint_generation_v0(
        &store,
        KafkaCheckpointGenerationV0::new(1).unwrap(),
        &admission,
        &frontier,
    )
    .unwrap();
    directory.write(&pending(2), b"newer-partial-secret");
    let candidate = recover_latest_local_kafka_checkpoint_commit_candidate_v0(
        &store,
        &config,
        frontier.source_contract(),
    )
    .unwrap();
    assert_eq!(candidate.generation().get(), 1);
    assert_eq!(candidate.pending_entries(), 1);
}

#[test]
fn corrupt_latest_never_falls_back_to_an_older_candidate() {
    let directory = TestDirectory::new("corrupt");
    let store = LocalCheckpointStoreV0::open(directory.path()).unwrap();
    let (config, admission, frontier) = cut();
    for generation in [1, 2] {
        publish_local_kafka_checkpoint_generation_v0(
            &store,
            KafkaCheckpointGenerationV0::new(generation).unwrap(),
            &admission,
            &frontier,
        )
        .unwrap();
    }
    let mut file = OpenOptions::new()
        .read(true)
        .write(true)
        .open(directory.path().join(published(2)))
        .unwrap();
    file.seek(SeekFrom::Start(30)).unwrap();
    let mut byte = [0_u8; 1];
    file.read_exact(&mut byte).unwrap();
    file.seek(SeekFrom::Start(30)).unwrap();
    file.write_all(&[byte[0] ^ 1]).unwrap();
    file.sync_all().unwrap();

    assert!(matches!(
        recover_latest_local_kafka_checkpoint_commit_candidate_v0(
            &store,
            &config,
            frontier.source_contract()
        ),
        Err(LocalKafkaCheckpointCommitCandidateErrorV0::Recovery(
            LocalCheckpointRecoveryErrorV0::ArtifactDecode(_)
        ))
    ));
    assert!(directory.path().join(published(1)).exists());
}

#[test]
fn expected_contract_precedes_directory_and_diagnostics_are_value_safe() {
    let directory = TestDirectory::new("precedence");
    directory.write("README-secret-path", b"secret-content");
    let store = LocalCheckpointStoreV0::open(directory.path()).unwrap();
    let (_, _, frontier) = cut();
    let error = recover_latest_local_kafka_checkpoint_commit_candidate_v0(
        &store,
        &config(SourcePositionKindV0::Synthetic),
        frontier.source_contract(),
    )
    .unwrap_err();
    assert!(matches!(
        error,
        LocalKafkaCheckpointCommitCandidateErrorV0::Recovery(
            LocalCheckpointRecoveryErrorV0::ExpectedContract(_)
        )
    ));
    let debug = format!("{error:?}");
    for secret in [
        "candidate-source-secret",
        "candidate-cluster-secret",
        "README-secret-path",
        "secret-content",
    ] {
        assert!(!debug.contains(secret));
    }
}

#[test]
fn successful_candidate_debug_redacts_state_coordinates_and_digests() {
    let directory = TestDirectory::new("debug");
    let store = LocalCheckpointStoreV0::open(directory.path()).unwrap();
    let (config, admission, frontier) = cut();
    publish_local_kafka_checkpoint_generation_v0(
        &store,
        KafkaCheckpointGenerationV0::new(1).unwrap(),
        &admission,
        &frontier,
    )
    .unwrap();
    let candidate = recover_latest_local_kafka_checkpoint_commit_candidate_v0(
        &store,
        &config,
        frontier.source_contract(),
    )
    .unwrap();
    let checkpoint_hash = candidate.checkpoint_hash().to_canonical_string();
    let frontier_hash = candidate
        .offset_commit_plan()
        .frontier_hash()
        .to_canonical_string();
    let debug = format!("{candidate:?}");

    assert!(debug.contains("<redacted>"));
    for secret in [
        "candidate-source-secret",
        "candidate-cluster-secret",
        "90",
        checkpoint_hash.as_str(),
        frontier_hash.as_str(),
    ] {
        assert!(!debug.contains(secret));
    }
}
