use std::fs;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use decant_ingest::{
    KafkaCheckpointGenerationV0, encode_kafka_checkpoint_generation_artifact_v0,
    hash_kafka_ingestion_checkpoint_v0,
};
use decant_ir::{KafkaPositionV0, SourceIdV0, SourcePositionKindV0};
use decant_kafka::{
    KafkaClusterIdV0, KafkaPartitionFrontierV0, KafkaSourceContractV0, KafkaTopicIdV0,
    hash_kafka_partition_frontier_v0,
};
use decant_state::{AdmissionConfigV0, AdmissionLimitsV0, AdmissionStateV0};
use decant_storage::{
    LocalCheckpointPublicationErrorV0, LocalCheckpointPublicationFaultPointV0,
    LocalCheckpointPublicationVisibilityV0, LocalCheckpointRecoveryErrorV0, LocalCheckpointStoreV0,
    LocalKafkaCheckpointCommitCandidateErrorV0, VerifiedLocalKafkaCheckpointPublicationErrorV0,
    VerifiedLocalKafkaCheckpointPublicationStatusV0, acquire_local_checkpoint_writer_lock_v0,
    publish_and_verify_local_kafka_checkpoint_commit_candidate_v0,
    publish_and_verify_local_kafka_checkpoint_commit_candidate_with_fault_v0,
    publish_and_verify_local_kafka_checkpoint_commit_candidate_with_writer_lock_v0,
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
            "decant-verified-publication-{label}-{}-{sequence}",
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

fn source() -> SourceIdV0 {
    SourceIdV0::new("verified-source-secret").unwrap()
}

fn cluster(value: &str) -> KafkaClusterIdV0 {
    KafkaClusterIdV0::new(value).unwrap()
}

fn topic(byte: u8) -> KafkaTopicIdV0 {
    KafkaTopicIdV0::from_bytes([byte; 16]).unwrap()
}

fn admission_config(epoch_end_ns: i64, kind: SourcePositionKindV0) -> AdmissionConfigV0 {
    AdmissionConfigV0::new_with_source_position_kind(
        0,
        source(),
        kind,
        0,
        epoch_end_ns,
        AdmissionLimitsV0::maximum(),
    )
    .unwrap()
}

fn contract(cluster_id: &str, topic_byte: u8) -> KafkaSourceContractV0 {
    KafkaSourceContractV0::new(0, source(), cluster(cluster_id), topic(topic_byte)).unwrap()
}

fn cut() -> (
    AdmissionConfigV0,
    AdmissionStateV0,
    KafkaPartitionFrontierV0,
) {
    let config = admission_config(100, SourcePositionKindV0::Kafka);
    let admission = AdmissionStateV0::new(config.clone());
    let frontier = KafkaPartitionFrontierV0::new(
        0,
        contract("verified-cluster-secret", 0x5a),
        &[
            KafkaPositionV0::new(9, 90).unwrap(),
            KafkaPositionV0::new(1, 10).unwrap(),
        ],
    )
    .unwrap();
    (config, admission, frontier)
}

#[test]
fn locked_publication_requires_the_guard_for_the_same_store() {
    let protected_directory = TestDirectory::new("locked-protected");
    let other_directory = TestDirectory::new("locked-other");
    let protected_store = LocalCheckpointStoreV0::open(protected_directory.path()).unwrap();
    let other_store = LocalCheckpointStoreV0::open(other_directory.path()).unwrap();
    let other_guard = acquire_local_checkpoint_writer_lock_v0(&other_store).unwrap();
    let (config, admission, frontier) = cut();

    let error = publish_and_verify_local_kafka_checkpoint_commit_candidate_with_writer_lock_v0(
        &other_guard,
        &protected_store,
        KafkaCheckpointGenerationV0::new(1).unwrap(),
        &admission,
        &frontier,
        &config,
        frontier.source_contract(),
    )
    .unwrap_err();
    assert_eq!(
        error,
        VerifiedLocalKafkaCheckpointPublicationErrorV0::WriterLockMismatch
    );
    assert_eq!(
        error.publication_status(),
        VerifiedLocalKafkaCheckpointPublicationStatusV0::NotPublished
    );
    assert!(protected_store.catalog().unwrap().published().is_empty());
}

#[test]
fn ordinary_publication_returns_only_the_exact_strictly_recovered_candidate() {
    let directory = TestDirectory::new("ordinary");
    let store = LocalCheckpointStoreV0::open(directory.path()).unwrap();
    let (config, admission, frontier) = cut();

    for generation_value in [1, 3] {
        let generation = KafkaCheckpointGenerationV0::new(generation_value).unwrap();
        let candidate = publish_and_verify_local_kafka_checkpoint_commit_candidate_v0(
            &store,
            generation,
            &admission,
            &frontier,
            &config,
            frontier.source_contract(),
        )
        .unwrap();
        let artifact =
            encode_kafka_checkpoint_generation_artifact_v0(generation, &admission, &frontier)
                .unwrap();

        assert_eq!(candidate.generation(), generation);
        assert_eq!(
            candidate.checkpoint_hash(),
            hash_kafka_ingestion_checkpoint_v0(&admission, &frontier).unwrap()
        );
        assert_eq!(
            candidate.offset_commit_plan().frontier_hash(),
            hash_kafka_partition_frontier_v0(&frontier).unwrap()
        );
        assert_eq!(
            candidate.offset_commit_plan().iter().collect::<Vec<_>>(),
            vec![(1, 10), (9, 90)]
        );
        assert_eq!(candidate.artifact_bytes(), artifact.len());
        assert_eq!(candidate.pending_entries(), 0);
    }
    assert_eq!(
        store
            .catalog()
            .unwrap()
            .published()
            .iter()
            .map(|entry| entry.generation().get())
            .collect::<Vec<_>>(),
        vec![1, 3]
    );
}

#[test]
fn trusted_and_live_contract_mismatches_fail_before_mutation() {
    let directory = TestDirectory::new("preflight");
    let store = LocalCheckpointStoreV0::open(directory.path()).unwrap();
    let (config, admission, frontier) = cut();
    let generation = KafkaCheckpointGenerationV0::new(1).unwrap();

    let cases = [
        (
            admission_config(100, SourcePositionKindV0::Synthetic),
            frontier.source_contract().clone(),
            "expected-contract",
        ),
        (
            admission_config(101, SourcePositionKindV0::Kafka),
            frontier.source_contract().clone(),
            "admission-config",
        ),
        (
            config.clone(),
            contract("other-verified-cluster-secret", 0xa5),
            "source-contract",
        ),
    ];
    for (expected_config, expected_contract, expected_category) in cases {
        let error = publish_and_verify_local_kafka_checkpoint_commit_candidate_v0(
            &store,
            generation,
            &admission,
            &frontier,
            &expected_config,
            &expected_contract,
        )
        .unwrap_err();
        match expected_category {
            "expected-contract" => assert!(matches!(
                error,
                VerifiedLocalKafkaCheckpointPublicationErrorV0::ExpectedContract(_)
            )),
            "admission-config" => assert_eq!(
                error,
                VerifiedLocalKafkaCheckpointPublicationErrorV0::AdmissionConfigMismatch
            ),
            "source-contract" => assert_eq!(
                error,
                VerifiedLocalKafkaCheckpointPublicationErrorV0::SourceContractMismatch
            ),
            _ => unreachable!(),
        }
        assert_eq!(
            error.publication_status(),
            VerifiedLocalKafkaCheckpointPublicationStatusV0::NotPublished
        );
        assert_eq!(store.catalog().unwrap().total_entries(), 0);
    }
}

#[test]
fn same_generation_publication_error_returns_no_candidate() {
    let directory = TestDirectory::new("same");
    let store = LocalCheckpointStoreV0::open(directory.path()).unwrap();
    let (config, admission, frontier) = cut();
    let generation = KafkaCheckpointGenerationV0::new(1).unwrap();
    publish_and_verify_local_kafka_checkpoint_commit_candidate_v0(
        &store,
        generation,
        &admission,
        &frontier,
        &config,
        frontier.source_contract(),
    )
    .unwrap();

    let error = publish_and_verify_local_kafka_checkpoint_commit_candidate_v0(
        &store,
        generation,
        &admission,
        &frontier,
        &config,
        frontier.source_contract(),
    )
    .unwrap_err();
    assert!(matches!(
        error,
        VerifiedLocalKafkaCheckpointPublicationErrorV0::Publication(
            LocalCheckpointPublicationErrorV0::GenerationNotAfterLatest { .. }
        )
    ));
    assert_eq!(
        error.publication_status(),
        VerifiedLocalKafkaCheckpointPublicationStatusV0::NotPublished
    );
}

#[test]
fn every_fault_returns_no_inline_candidate_and_requires_separate_recovery() {
    let points = [
        (
            LocalCheckpointPublicationFaultPointV0::AfterPendingCreation,
            LocalCheckpointPublicationVisibilityV0::NotPublished,
            None,
        ),
        (
            LocalCheckpointPublicationFaultPointV0::AfterPartialPendingWrite,
            LocalCheckpointPublicationVisibilityV0::NotPublished,
            None,
        ),
        (
            LocalCheckpointPublicationFaultPointV0::AfterCompletePendingWrite,
            LocalCheckpointPublicationVisibilityV0::NotPublished,
            None,
        ),
        (
            LocalCheckpointPublicationFaultPointV0::AfterPendingSync,
            LocalCheckpointPublicationVisibilityV0::NotPublished,
            None,
        ),
        (
            LocalCheckpointPublicationFaultPointV0::AfterPublishedLink,
            LocalCheckpointPublicationVisibilityV0::Uncertain,
            Some(1_usize),
        ),
        (
            LocalCheckpointPublicationFaultPointV0::AfterPendingRemoval,
            LocalCheckpointPublicationVisibilityV0::Uncertain,
            Some(0),
        ),
        (
            LocalCheckpointPublicationFaultPointV0::AfterDirectorySyncBeforeReceipt,
            LocalCheckpointPublicationVisibilityV0::Uncertain,
            Some(0),
        ),
    ];

    for _ in 0..2 {
        for (point, visibility, recovered_pending) in points {
            let directory = TestDirectory::new("fault");
            let store = LocalCheckpointStoreV0::open(directory.path()).unwrap();
            let (config, admission, frontier) = cut();
            let generation = KafkaCheckpointGenerationV0::new(1).unwrap();
            let error = publish_and_verify_local_kafka_checkpoint_commit_candidate_with_fault_v0(
                &store,
                generation,
                &admission,
                &frontier,
                &config,
                frontier.source_contract(),
                point,
            )
            .unwrap_err();
            assert_eq!(
                error,
                VerifiedLocalKafkaCheckpointPublicationErrorV0::Publication(
                    LocalCheckpointPublicationErrorV0::InjectedFault { point, visibility }
                )
            );
            let expected_status = match visibility {
                LocalCheckpointPublicationVisibilityV0::NotPublished => {
                    VerifiedLocalKafkaCheckpointPublicationStatusV0::NotPublished
                }
                LocalCheckpointPublicationVisibilityV0::Uncertain => {
                    VerifiedLocalKafkaCheckpointPublicationStatusV0::Uncertain
                }
            };
            assert_eq!(error.publication_status(), expected_status);

            let reconciliation = recover_latest_local_kafka_checkpoint_commit_candidate_v0(
                &store,
                &config,
                frontier.source_contract(),
            );
            match recovered_pending {
                None => assert_eq!(
                    reconciliation.unwrap_err(),
                    LocalKafkaCheckpointCommitCandidateErrorV0::Recovery(
                        LocalCheckpointRecoveryErrorV0::NoPublishedGeneration {
                            pending_entries: 1
                        }
                    )
                ),
                Some(pending_entries) => {
                    let candidate = reconciliation.unwrap();
                    assert_eq!(candidate.generation(), generation);
                    assert_eq!(candidate.pending_entries(), pending_entries);
                    assert_eq!(
                        candidate.checkpoint_hash(),
                        hash_kafka_ingestion_checkpoint_v0(&admission, &frontier).unwrap()
                    );
                    assert_eq!(
                        candidate.offset_commit_plan().frontier_hash(),
                        hash_kafka_partition_frontier_v0(&frontier).unwrap()
                    );
                }
            }
        }
    }
}

#[test]
fn errors_after_a_success_receipt_are_explicitly_published() {
    let errors = [
        VerifiedLocalKafkaCheckpointPublicationErrorV0::CandidateRecovery(
            LocalKafkaCheckpointCommitCandidateErrorV0::Recovery(
                LocalCheckpointRecoveryErrorV0::NoPublishedGeneration { pending_entries: 0 },
            ),
        ),
        VerifiedLocalKafkaCheckpointPublicationErrorV0::UnexpectedRecoveredGeneration {
            attempted: KafkaCheckpointGenerationV0::new(1).unwrap(),
            recovered: KafkaCheckpointGenerationV0::new(2).unwrap(),
        },
        VerifiedLocalKafkaCheckpointPublicationErrorV0::CheckpointHashMismatch,
        VerifiedLocalKafkaCheckpointPublicationErrorV0::FrontierHashMismatch,
        VerifiedLocalKafkaCheckpointPublicationErrorV0::ArtifactBytesMismatch {
            published: 1,
            recovered: 2,
        },
    ];
    for error in errors {
        assert_eq!(
            error.publication_status(),
            VerifiedLocalKafkaCheckpointPublicationStatusV0::Published
        );
    }
}

#[test]
fn success_and_error_diagnostics_are_value_safe() {
    let directory = TestDirectory::new("debug");
    let store = LocalCheckpointStoreV0::open(directory.path()).unwrap();
    let (config, admission, frontier) = cut();
    let candidate = publish_and_verify_local_kafka_checkpoint_commit_candidate_v0(
        &store,
        KafkaCheckpointGenerationV0::new(1).unwrap(),
        &admission,
        &frontier,
        &config,
        frontier.source_contract(),
    )
    .unwrap();
    let success_debug = format!("{candidate:?}");
    let error = publish_and_verify_local_kafka_checkpoint_commit_candidate_v0(
        &store,
        KafkaCheckpointGenerationV0::new(1).unwrap(),
        &admission,
        &frontier,
        &config,
        frontier.source_contract(),
    )
    .unwrap_err();
    let error_debug = format!("{error:?}");
    let checkpoint_hash = candidate.checkpoint_hash().to_canonical_string();
    let frontier_hash = candidate
        .offset_commit_plan()
        .frontier_hash()
        .to_canonical_string();

    for debug in [success_debug, error_debug] {
        for secret in [
            "verified-source-secret",
            "verified-cluster-secret",
            "90",
            checkpoint_hash.as_str(),
            frontier_hash.as_str(),
            directory.path().to_string_lossy().as_ref(),
        ] {
            assert!(!debug.contains(secret));
        }
    }
}
