use decant_ingest::{
    KafkaCanonicalAdmissionOutcomeV0, KafkaJsonIngestionReplayErrorV0,
    KafkaJsonIngestionReplayInputV0, KafkaJsonIngestionReplayJournalV0,
    KafkaJsonIngestionReplayLimitsV0, encode_kafka_ingestion_checkpoint_v0,
    replay_kafka_json_ingestion_journal_v0,
    replay_kafka_json_ingestion_journal_with_interruption_v0,
};
use decant_ir::{KafkaPositionV0, SourceIdV0, SourcePositionKindV0};
use decant_kafka::{
    KafkaClusterIdV0, KafkaFetchedRecordV0, KafkaPartitionFrontierV0, KafkaRecordTimestampV0,
    KafkaSourceContractV0, KafkaTopicIdV0, encode_kafka_partition_frontier_snapshot_v0,
    hash_kafka_partition_frontier_v0,
};
use decant_state::{
    AdmissionConfigV0, AdmissionLimitsV0, ApplyOutcomeV0, RejectionReasonV0,
    encode_admission_snapshot_v1, hash_admission_state_v1,
};

fn source(name: &str) -> SourceIdV0 {
    SourceIdV0::new(name).unwrap()
}

fn cluster(name: &str) -> KafkaClusterIdV0 {
    KafkaClusterIdV0::new(name).unwrap()
}

fn topic(byte: u8) -> KafkaTopicIdV0 {
    KafkaTopicIdV0::from_bytes([byte; 16]).unwrap()
}

fn contract(name: &str) -> KafkaSourceContractV0 {
    KafkaSourceContractV0::new(0, source(name), cluster("cluster-v0"), topic(9)).unwrap()
}

fn config(name: &str, kind: SourcePositionKindV0) -> AdmissionConfigV0 {
    AdmissionConfigV0::new_with_source_position_kind(
        0,
        source(name),
        kind,
        0,
        100,
        AdmissionLimitsV0::maximum(),
    )
    .unwrap()
}

fn frontier(name: &str) -> KafkaPartitionFrontierV0 {
    KafkaPartitionFrontierV0::new(0, contract(name), &[KafkaPositionV0::new(0, 0).unwrap()])
        .unwrap()
}

fn json(event_time_ns: i64, amount: u64) -> Vec<u8> {
    format!(
        "{{\"record_format\":\"decant-kafka-event\",\"record_version\":0,\"schema_id\":\"event.v0\",\"event_time_ns\":{event_time_ns},\"payload\":[{{\"name\":\"amount\",\"value\":{{\"kind\":\"u64\",\"value\":{amount}}}}}]}}"
    )
    .into_bytes()
}

fn input(
    observed_source: &str,
    observed_cluster: &str,
    observed_topic: u8,
    offset: i64,
    value: Option<Vec<u8>>,
) -> KafkaJsonIngestionReplayInputV0 {
    KafkaJsonIngestionReplayInputV0::new(
        source(observed_source),
        cluster(observed_cluster),
        topic(observed_topic),
        KafkaFetchedRecordV0::new(
            KafkaPositionV0::new(0, offset).unwrap(),
            Some(b"secret-key"),
            value.as_deref(),
            KafkaRecordTimestampV0::NotAvailable,
            &[],
        )
        .unwrap(),
    )
}

fn mixed_journal() -> KafkaJsonIngestionReplayJournalV0 {
    let mut journal =
        KafkaJsonIngestionReplayJournalV0::new(0, KafkaJsonIngestionReplayLimitsV0::maximum())
            .unwrap();
    let inputs = [
        input("wrong-source", "cluster-v0", 9, 0, Some(json(50, 0))),
        input("source-v0", "cluster-v0", 9, 0, Some(b"{bad".to_vec())),
        input("source-v0", "cluster-v0", 9, 0, None),
        input("source-v0", "cluster-v0", 9, 2, Some(json(50, 2))),
        input("source-v0", "cluster-v0", 9, 0, Some(json(50, 3))),
        input("source-v0", "cluster-v0", 9, 0, Some(json(50, 3))),
        input("source-v0", "cluster-v0", 9, 1, Some(json(101, 4))),
        input("source-v0", "cluster-v0", 9, 2, Some(json(60, 5))),
    ];
    for input in inputs {
        journal.try_push(input).unwrap();
    }
    journal
}

#[test]
fn mixed_trace_retains_every_success_and_error_outcome() {
    let config = config("source-v0", SourcePositionKindV0::Kafka);
    let frontier = frontier("source-v0");
    let journal = mixed_journal();
    let report = replay_kafka_json_ingestion_journal_v0(&config, &frontier, &journal).unwrap();
    assert_eq!(report.outcomes().len(), 8);
    assert!(report.outcomes()[0].is_err());
    assert!(report.outcomes()[1].is_err());
    assert!(report.outcomes()[2].is_err());
    assert!(matches!(
        report.outcomes()[3].as_ref().unwrap().admission(),
        KafkaCanonicalAdmissionOutcomeV0::Gap
    ));
    assert!(matches!(
        report.outcomes()[4].as_ref().unwrap().admission(),
        KafkaCanonicalAdmissionOutcomeV0::Advanced {
            admission: ApplyOutcomeV0::Accepted
        }
    ));
    assert!(matches!(
        report.outcomes()[5].as_ref().unwrap().admission(),
        KafkaCanonicalAdmissionOutcomeV0::BeforeFrontier
    ));
    assert!(matches!(
        report.outcomes()[6].as_ref().unwrap().admission(),
        KafkaCanonicalAdmissionOutcomeV0::Advanced {
            admission: ApplyOutcomeV0::Rejected(RejectionReasonV0::OutsideEpoch)
        }
    ));
    assert!(matches!(
        report.outcomes()[7].as_ref().unwrap().admission(),
        KafkaCanonicalAdmissionOutcomeV0::Advanced {
            admission: ApplyOutcomeV0::Accepted
        }
    ));
    assert_eq!(report.frontier().next_offset(0), Some(3));
    assert_eq!(report.state().usage().accepted_identities(), 2);
}

#[test]
fn every_interruption_boundary_matches_uninterrupted_execution() {
    let config = config("source-v0", SourcePositionKindV0::Kafka);
    let frontier = frontier("source-v0");
    let journal = mixed_journal();
    let uninterrupted =
        replay_kafka_json_ingestion_journal_v0(&config, &frontier, &journal).unwrap();
    let expected_state = encode_admission_snapshot_v1(uninterrupted.state()).unwrap();
    let expected_state_hash = hash_admission_state_v1(uninterrupted.state()).unwrap();
    let expected_frontier =
        encode_kafka_partition_frontier_snapshot_v0(uninterrupted.frontier()).unwrap();
    let expected_frontier_hash =
        hash_kafka_partition_frontier_v0(uninterrupted.frontier()).unwrap();
    let expected_checkpoint =
        encode_kafka_ingestion_checkpoint_v0(uninterrupted.state(), uninterrupted.frontier())
            .unwrap();

    for interruption in 0..=journal.len() {
        let interrupted = replay_kafka_json_ingestion_journal_with_interruption_v0(
            &config,
            &frontier,
            &journal,
            interruption,
        )
        .unwrap();
        assert_eq!(interrupted.outcomes(), uninterrupted.outcomes());
        assert_eq!(
            encode_admission_snapshot_v1(interrupted.state()).unwrap(),
            expected_state,
            "state snapshot mismatch after interruption {interruption}"
        );
        assert_eq!(
            hash_admission_state_v1(interrupted.state()).unwrap(),
            expected_state_hash
        );
        assert_eq!(
            encode_kafka_partition_frontier_snapshot_v0(interrupted.frontier()).unwrap(),
            expected_frontier
        );
        assert_eq!(
            hash_kafka_partition_frontier_v0(interrupted.frontier()).unwrap(),
            expected_frontier_hash
        );
        assert_eq!(
            encode_kafka_ingestion_checkpoint_v0(interrupted.state(), interrupted.frontier())
                .unwrap(),
            expected_checkpoint
        );
    }
}

#[test]
fn empty_and_repeated_replays_are_exact() {
    let config = config("source-v0", SourcePositionKindV0::Kafka);
    let frontier = frontier("source-v0");
    let empty =
        KafkaJsonIngestionReplayJournalV0::new(0, KafkaJsonIngestionReplayLimitsV0::maximum())
            .unwrap();
    let empty_report =
        replay_kafka_json_ingestion_journal_with_interruption_v0(&config, &frontier, &empty, 0)
            .unwrap();
    assert!(empty_report.outcomes().is_empty());
    assert_eq!(empty_report.frontier(), &frontier);

    let journal = mixed_journal();
    let left = replay_kafka_json_ingestion_journal_v0(&config, &frontier, &journal).unwrap();
    let right = replay_kafka_json_ingestion_journal_v0(&config, &frontier, &journal).unwrap();
    assert_eq!(left.outcomes(), right.outcomes());
    assert_eq!(
        encode_admission_snapshot_v1(left.state()).unwrap(),
        encode_admission_snapshot_v1(right.state()).unwrap()
    );
    assert_eq!(
        hash_kafka_partition_frontier_v0(left.frontier()).unwrap(),
        hash_kafka_partition_frontier_v0(right.frontier()).unwrap()
    );
}

#[test]
fn initial_configuration_and_interruption_errors_are_typed() {
    let frontier = frontier("source-v0");
    let journal = mixed_journal();
    assert_eq!(
        replay_kafka_json_ingestion_journal_v0(
            &config("source-v0", SourcePositionKindV0::Synthetic),
            &frontier,
            &journal,
        )
        .unwrap_err(),
        KafkaJsonIngestionReplayErrorV0::UnsupportedInitialSourcePositionKind {
            actual: SourcePositionKindV0::Synthetic,
        }
    );
    assert_eq!(
        replay_kafka_json_ingestion_journal_v0(
            &config("other-source", SourcePositionKindV0::Kafka),
            &frontier,
            &journal,
        )
        .unwrap_err(),
        KafkaJsonIngestionReplayErrorV0::InitialSourceMismatch
    );
    assert_eq!(
        replay_kafka_json_ingestion_journal_with_interruption_v0(
            &config("source-v0", SourcePositionKindV0::Kafka),
            &frontier,
            &journal,
            journal.len() + 1,
        )
        .unwrap_err(),
        KafkaJsonIngestionReplayErrorV0::InterruptionOutOfRange {
            maximum: journal.len(),
            actual: journal.len() + 1,
        }
    );
}

#[test]
fn report_and_errors_do_not_disclose_raw_or_identity_values() {
    let config = config("source-v0", SourcePositionKindV0::Kafka);
    let frontier = frontier("source-v0");
    let journal = mixed_journal();
    let report = replay_kafka_json_ingestion_journal_v0(&config, &frontier, &journal).unwrap();
    let rendered = format!("{report:?}");
    for secret in ["source-v0", "wrong-source", "secret-key", "event.v0"] {
        assert!(!rendered.contains(secret));
    }
    let error = replay_kafka_json_ingestion_journal_with_interruption_v0(
        &config,
        &frontier,
        &journal,
        journal.len() + 1,
    )
    .unwrap_err();
    assert!(!format!("{error:?}").contains("source-v0"));
}
