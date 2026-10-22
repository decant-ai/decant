use std::fs;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use decant_ingest::{KafkaCheckpointGenerationV0, hash_kafka_ingestion_checkpoint_v0};
use decant_ir::{KafkaPositionV0, SourceIdV0, SourcePositionKindV0};
use decant_kafka::{
    KAFKA_PARTITION_FRONTIER_VERSION_V0, KAFKA_SOURCE_CONTRACT_VERSION_V0, KafkaClusterIdV0,
    KafkaFetchedRecordV0, KafkaPartitionFrontierV0, KafkaRecordHeaderV0, KafkaRecordTimestampV0,
    KafkaSourceContractV0, KafkaTopicIdV0,
};
use decant_kafka_live::{
    KafkaStaticCheckpointSessionV0, KafkaStaticSessionBootstrapErrorV0,
    KafkaStaticSessionCheckpointErrorV0, KafkaStaticSessionCheckpointV0,
    KafkaStaticSessionProcessErrorV0, KafkaStaticSessionProcessV0,
};
use decant_state::{
    ADMISSION_VERSION_V0, AdmissionConfigV0, AdmissionLimitsV0, AdmissionStateV0, RejectionReasonV0,
};
use decant_storage::{
    LocalCheckpointPublicationFaultPointV0, LocalCheckpointStoreV0,
    VerifiedLocalKafkaCheckpointPublicationErrorV0, publish_local_kafka_checkpoint_generation_v0,
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
            "decant-static-session-{label}-{}-{sequence}",
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
    SourceIdV0::new("live-source-v0").unwrap()
}

fn contract() -> KafkaSourceContractV0 {
    KafkaSourceContractV0::new(
        KAFKA_SOURCE_CONTRACT_VERSION_V0,
        source(),
        KafkaClusterIdV0::new("live-cluster-v0").unwrap(),
        KafkaTopicIdV0::from_bytes([9; 16]).unwrap(),
    )
    .unwrap()
}

fn admission_config() -> AdmissionConfigV0 {
    AdmissionConfigV0::new_with_source_position_kind(
        ADMISSION_VERSION_V0,
        source(),
        SourcePositionKindV0::Kafka,
        0,
        100_000_000_000,
        AdmissionLimitsV0::maximum(),
    )
    .unwrap()
}

fn position(partition: i32, offset: i64) -> KafkaPositionV0 {
    KafkaPositionV0::new(partition, offset).unwrap()
}

fn frontier(starts: &[KafkaPositionV0]) -> KafkaPartitionFrontierV0 {
    KafkaPartitionFrontierV0::new(KAFKA_PARTITION_FRONTIER_VERSION_V0, contract(), starts).unwrap()
}

fn value(event_time_ns: i64, amount: u64) -> Vec<u8> {
    format!(
        "{{\"record_format\":\"decant-kafka-event\",\"record_version\":0,\"schema_id\":\"payments.event.v0\",\"event_time_ns\":{event_time_ns},\"payload\":[{{\"name\":\"amount\",\"value\":{{\"kind\":\"u64\",\"value\":{amount}}}}}]}}"
    )
    .into_bytes()
}

fn record(
    offset: i64,
    event_time_ns: i64,
    amount: u64,
    timestamp: KafkaRecordTimestampV0,
) -> KafkaFetchedRecordV0 {
    let value = value(event_time_ns, amount);
    KafkaFetchedRecordV0::new(
        position(0, offset),
        Some(b"synthetic-key"),
        Some(&value),
        timestamp,
        &[KafkaRecordHeaderV0::new("synthetic-header", Some(b"omitted")).unwrap()],
    )
    .unwrap()
}

fn open(directory: &TestDirectory) -> LocalCheckpointStoreV0 {
    LocalCheckpointStoreV0::open(directory.path()).unwrap()
}

#[test]
fn empty_pending_and_published_startup_paths_are_exact() {
    let empty_directory = TestDirectory::new("empty");
    let empty_store = open(&empty_directory);
    let starts = [position(0, 4), position(2, 9)];
    let mut empty = KafkaStaticCheckpointSessionV0::bootstrap(
        &empty_store,
        admission_config(),
        contract(),
        &starts,
    )
    .unwrap();
    assert_eq!(
        empty.frontier().iter().collect::<Vec<_>>(),
        vec![(0, 4), (2, 9)]
    );
    assert_eq!(empty.latest_generation(), None);
    assert_eq!(empty.next_generation().unwrap().get(), 1);
    assert_eq!(
        empty.checkpoint(&empty_store).unwrap(),
        KafkaStaticSessionCheckpointV0::Unchanged {
            latest_generation: None,
            latest_checkpoint_hash: None,
        }
    );

    let pending_directory = TestDirectory::new("pending");
    let pending_store = open(&pending_directory);
    let pending_admission = AdmissionStateV0::new(admission_config());
    let pending_frontier = frontier(&[position(0, 0)]);
    publish_local_kafka_checkpoint_generation_with_fault_v0(
        &pending_store,
        KafkaCheckpointGenerationV0::new(1).unwrap(),
        &pending_admission,
        &pending_frontier,
        LocalCheckpointPublicationFaultPointV0::AfterPendingSync,
    )
    .unwrap_err();
    assert_eq!(
        KafkaStaticCheckpointSessionV0::bootstrap(
            &pending_store,
            admission_config(),
            contract(),
            &[position(0, 0)],
        )
        .unwrap_err(),
        KafkaStaticSessionBootstrapErrorV0::PendingWithoutPublished { pending_entries: 1 }
    );

    let published_directory = TestDirectory::new("published");
    let published_store = open(&published_directory);
    let published_admission = AdmissionStateV0::new(admission_config());
    publish_local_kafka_checkpoint_generation_v0(
        &published_store,
        KafkaCheckpointGenerationV0::new(3).unwrap(),
        &published_admission,
        &frontier(&[position(0, 5), position(2, 8)]),
    )
    .unwrap();
    let published_frontier = frontier(&[position(0, 7), position(2, 11)]);
    let generation = KafkaCheckpointGenerationV0::new(4).unwrap();
    publish_local_kafka_checkpoint_generation_v0(
        &published_store,
        generation,
        &published_admission,
        &published_frontier,
    )
    .unwrap();
    let recovered = KafkaStaticCheckpointSessionV0::bootstrap(
        &published_store,
        admission_config(),
        contract(),
        &[position(0, 0), position(2, 0)],
    )
    .unwrap();
    assert_eq!(
        recovered.frontier().iter().collect::<Vec<_>>(),
        vec![(0, 7), (2, 11)]
    );
    assert_eq!(recovered.latest_generation(), Some(generation));
    assert_eq!(recovered.next_generation().unwrap().get(), 5);
}

#[test]
fn recovery_rejects_partition_contract_corruption_catalog_and_generation_maximum() {
    let mismatch_directory = TestDirectory::new("partition-mismatch");
    let mismatch_store = open(&mismatch_directory);
    publish_local_kafka_checkpoint_generation_v0(
        &mismatch_store,
        KafkaCheckpointGenerationV0::new(1).unwrap(),
        &AdmissionStateV0::new(admission_config()),
        &frontier(&[position(0, 7), position(2, 11)]),
    )
    .unwrap();
    assert_eq!(
        KafkaStaticCheckpointSessionV0::bootstrap(
            &mismatch_store,
            admission_config(),
            contract(),
            &[position(0, 0)],
        )
        .unwrap_err(),
        KafkaStaticSessionBootstrapErrorV0::PartitionSetMismatch
    );

    let wrong_config = AdmissionConfigV0::new_with_source_position_kind(
        ADMISSION_VERSION_V0,
        source(),
        SourcePositionKindV0::Kafka,
        -1,
        100_000_000_000,
        AdmissionLimitsV0::maximum(),
    )
    .unwrap();
    assert!(matches!(
        KafkaStaticCheckpointSessionV0::bootstrap(
            &mismatch_store,
            wrong_config,
            contract(),
            &[position(0, 0), position(2, 0)],
        ),
        Err(KafkaStaticSessionBootstrapErrorV0::Recovery(_))
    ));

    let other_source = SourceIdV0::new("other-live-source-v0").unwrap();
    let other_config = AdmissionConfigV0::new_with_source_position_kind(
        ADMISSION_VERSION_V0,
        other_source.clone(),
        SourcePositionKindV0::Kafka,
        0,
        100_000_000_000,
        AdmissionLimitsV0::maximum(),
    )
    .unwrap();
    let other_contract = KafkaSourceContractV0::new(
        KAFKA_SOURCE_CONTRACT_VERSION_V0,
        other_source,
        KafkaClusterIdV0::new("other-live-cluster-v0").unwrap(),
        KafkaTopicIdV0::from_bytes([8; 16]).unwrap(),
    )
    .unwrap();
    let source_error = KafkaStaticCheckpointSessionV0::bootstrap(
        &mismatch_store,
        other_config,
        other_contract,
        &[position(0, 0), position(2, 0)],
    )
    .unwrap_err();
    assert!(matches!(
        &source_error,
        KafkaStaticSessionBootstrapErrorV0::Recovery(_)
    ));
    assert!(!format!("{source_error:?}").contains("live-source-v0"));

    let corrupt_directory = TestDirectory::new("corrupt");
    let corrupt_store = open(&corrupt_directory);
    publish_local_kafka_checkpoint_generation_v0(
        &corrupt_store,
        KafkaCheckpointGenerationV0::new(1).unwrap(),
        &AdmissionStateV0::new(admission_config()),
        &frontier(&[position(0, 0)]),
    )
    .unwrap();
    fs::write(
        corrupt_directory
            .path()
            .join("checkpoint-00000000000000000001.dcp"),
        b"corrupt",
    )
    .unwrap();
    assert!(matches!(
        KafkaStaticCheckpointSessionV0::bootstrap(
            &corrupt_store,
            admission_config(),
            contract(),
            &[position(0, 0)],
        ),
        Err(KafkaStaticSessionBootstrapErrorV0::Recovery(_))
    ));

    let catalog_directory = TestDirectory::new("catalog");
    fs::write(catalog_directory.path().join("unexpected"), b"x").unwrap();
    let catalog_store = open(&catalog_directory);
    assert!(matches!(
        KafkaStaticCheckpointSessionV0::bootstrap(
            &catalog_store,
            admission_config(),
            contract(),
            &[position(0, 0)],
        ),
        Err(KafkaStaticSessionBootstrapErrorV0::Catalog(_))
    ));

    let maximum_directory = TestDirectory::new("maximum");
    let maximum_store = open(&maximum_directory);
    let maximum = KafkaCheckpointGenerationV0::new(u64::MAX).unwrap();
    publish_local_kafka_checkpoint_generation_v0(
        &maximum_store,
        maximum,
        &AdmissionStateV0::new(admission_config()),
        &frontier(&[position(0, 0)]),
    )
    .unwrap();
    assert_eq!(
        KafkaStaticCheckpointSessionV0::bootstrap(
            &maximum_store,
            admission_config(),
            contract(),
            &[position(0, 0)],
        )
        .unwrap_err(),
        KafkaStaticSessionBootstrapErrorV0::GenerationExhausted { latest: maximum }
    );
}

#[test]
fn processing_accounting_gap_and_checkpoint_publication_are_exact() {
    let directory = TestDirectory::new("process");
    let store = open(&directory);
    let mut session = KafkaStaticCheckpointSessionV0::bootstrap(
        &store,
        admission_config(),
        contract(),
        &[position(0, 10)],
    )
    .unwrap();
    let accepted = record(
        10,
        60_000_000_000,
        1,
        KafkaRecordTimestampV0::CreateTime { unix_ms: 1 },
    );
    let rejected = record(
        11,
        0,
        2,
        KafkaRecordTimestampV0::LogAppendTime { unix_ms: 2 },
    );
    let before = record(10, 60_000_000_000, 1, KafkaRecordTimestampV0::NotAvailable);
    assert_eq!(
        session.process_record(&accepted).unwrap(),
        KafkaStaticSessionProcessV0::Accepted
    );
    assert_eq!(
        session.process_record(&rejected).unwrap(),
        KafkaStaticSessionProcessV0::Rejected {
            reason: RejectionReasonV0::Late,
        }
    );
    assert_eq!(
        session.process_record(&before).unwrap(),
        KafkaStaticSessionProcessV0::BeforeFrontier
    );
    let usage_before_failures = session.usage();
    let frontier_before_failures = session.frontier().iter().collect::<Vec<_>>();
    assert_eq!(
        session
            .process_record(&record(
                13,
                60_000_000_003,
                3,
                KafkaRecordTimestampV0::NotAvailable,
            ))
            .unwrap_err(),
        KafkaStaticSessionProcessErrorV0::Gap
    );
    let malformed = KafkaFetchedRecordV0::new(
        position(0, 12),
        None,
        Some(b"not-json"),
        KafkaRecordTimestampV0::NotAvailable,
        &[],
    )
    .unwrap();
    assert!(matches!(
        session.process_record(&malformed),
        Err(KafkaStaticSessionProcessErrorV0::Ingestion(_))
    ));
    assert_eq!(session.usage(), usage_before_failures);
    assert_eq!(
        session.frontier().iter().collect::<Vec<_>>(),
        frontier_before_failures
    );

    let usage = session.usage();
    assert_eq!(usage.processed_records(), 3);
    assert_eq!(usage.advanced_records(), 2);
    assert_eq!(usage.accepted_records(), 1);
    assert_eq!(usage.duplicate_noop_records(), 0);
    assert_eq!(usage.rejected_records(), 1);
    assert_eq!(usage.before_frontier_records(), 1);
    assert_eq!(usage.omitted_header_count(), 3);
    assert_eq!(
        usage.omitted_header_bytes(),
        accepted.header_bytes() + rejected.header_bytes() + before.header_bytes()
    );
    assert_eq!(usage.omitted_timestamp_create_time_records(), 1);
    assert_eq!(usage.omitted_timestamp_log_append_time_records(), 1);
    assert_eq!(usage.omitted_timestamp_not_available_records(), 1);
    assert_eq!(session.advanced_since_checkpoint(), 2);

    let published = session.checkpoint(&store).unwrap();
    assert!(published.was_published());
    assert_eq!(published.generation().unwrap().get(), 1);
    assert_eq!(session.advanced_since_checkpoint(), 0);
    assert_eq!(session.latest_generation().unwrap().get(), 1);
    assert_eq!(session.next_generation().unwrap().get(), 2);
    let recovery = restore_latest_local_kafka_checkpoint_generation_v0(
        &store,
        &admission_config(),
        &contract(),
    )
    .unwrap();
    assert_eq!(
        recovery.checkpoint_hash(),
        published.checkpoint_hash().unwrap()
    );
    assert_eq!(recovery.frontier().next_offset(0), Some(12));
    assert_eq!(
        session.checkpoint(&store).unwrap(),
        KafkaStaticSessionCheckpointV0::Unchanged {
            latest_generation: Some(KafkaCheckpointGenerationV0::new(1).unwrap()),
            latest_checkpoint_hash: published.checkpoint_hash(),
        }
    );
}

#[test]
fn failed_publication_keeps_dirty_progress_and_generation_unchanged() {
    let directory = TestDirectory::new("publication-failure");
    let store = open(&directory);
    let mut session = KafkaStaticCheckpointSessionV0::bootstrap(
        &store,
        admission_config(),
        contract(),
        &[position(0, 0)],
    )
    .unwrap();
    publish_local_kafka_checkpoint_generation_with_fault_v0(
        &store,
        KafkaCheckpointGenerationV0::new(1).unwrap(),
        session.admission(),
        session.frontier(),
        LocalCheckpointPublicationFaultPointV0::AfterPendingSync,
    )
    .unwrap_err();
    session
        .process_record(&record(0, 10, 1, KafkaRecordTimestampV0::NotAvailable))
        .unwrap();
    assert!(matches!(
        session.checkpoint(&store),
        Err(KafkaStaticSessionCheckpointErrorV0::Publication(
            VerifiedLocalKafkaCheckpointPublicationErrorV0::Publication(_)
        ))
    ));
    assert_eq!(session.latest_generation(), None);
    assert_eq!(session.next_generation().unwrap().get(), 1);
    assert_eq!(session.advanced_since_checkpoint(), 1);
}

#[test]
fn every_record_boundary_restart_matches_uninterrupted_checkpoint_hash() {
    let records: Vec<_> = (0_i64..4)
        .map(|offset| {
            record(
                offset,
                100 + offset,
                u64::try_from(offset).unwrap(),
                KafkaRecordTimestampV0::NotAvailable,
            )
        })
        .collect();

    let uninterrupted_directory = TestDirectory::new("uninterrupted");
    let uninterrupted_store = open(&uninterrupted_directory);
    let mut uninterrupted = KafkaStaticCheckpointSessionV0::bootstrap(
        &uninterrupted_store,
        admission_config(),
        contract(),
        &[position(0, 0)],
    )
    .unwrap();
    for record in &records {
        uninterrupted.process_record(record).unwrap();
    }
    let expected_hash = uninterrupted
        .checkpoint(&uninterrupted_store)
        .unwrap()
        .checkpoint_hash()
        .unwrap();

    for boundary in 0..=records.len() {
        let directory = TestDirectory::new("interrupted");
        let store = open(&directory);
        let mut first = KafkaStaticCheckpointSessionV0::bootstrap(
            &store,
            admission_config(),
            contract(),
            &[position(0, 0)],
        )
        .unwrap();
        for record in &records[..boundary] {
            first.process_record(record).unwrap();
        }
        first.checkpoint(&store).unwrap();
        drop(first);

        let mut resumed = KafkaStaticCheckpointSessionV0::bootstrap(
            &store,
            admission_config(),
            contract(),
            &[position(0, 0)],
        )
        .unwrap();
        assert_eq!(
            resumed.frontier().next_offset(0),
            Some(i64::try_from(boundary).unwrap())
        );
        for record in &records[boundary..] {
            resumed.process_record(record).unwrap();
        }
        let final_checkpoint = resumed.checkpoint(&store).unwrap();
        let final_hash = final_checkpoint.checkpoint_hash().unwrap_or_else(|| {
            hash_kafka_ingestion_checkpoint_v0(resumed.admission(), resumed.frontier()).unwrap()
        });
        assert_eq!(final_hash, expected_hash, "boundary {boundary}");
        let strict = restore_latest_local_kafka_checkpoint_generation_v0(
            &store,
            &admission_config(),
            &contract(),
        )
        .unwrap();
        assert_eq!(
            strict.checkpoint_hash(),
            expected_hash,
            "boundary {boundary}"
        );
    }
}
