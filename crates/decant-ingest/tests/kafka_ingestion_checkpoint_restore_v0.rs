use decant_ingest::{
    KafkaIngestionCheckpointDecodeErrorV0, KafkaIngestionCheckpointFieldV0,
    MAX_KAFKA_INGESTION_CHECKPOINT_BYTES_V0, encode_kafka_ingestion_checkpoint_v0,
    ingest_decant_json_kafka_record_v0, restore_kafka_ingestion_checkpoint_v0,
};
use decant_ir::{KafkaPositionV0, SourceIdV0, SourcePositionKindV0};
use decant_kafka::{
    KafkaClusterIdV0, KafkaFetchedRecordV0, KafkaPartitionFrontierV0, KafkaRecordTimestampV0,
    KafkaSourceContractV0, KafkaTopicIdV0, MAX_KAFKA_FRONTIER_SNAPSHOT_BYTES_V0,
    encode_kafka_partition_frontier_snapshot_v0, hash_kafka_partition_frontier_v0,
};
use decant_state::{
    AdmissionConfigV0, AdmissionLimitsV0, AdmissionStateV0, MAX_ADMISSION_SNAPSHOT_BYTES_V1,
    encode_admission_snapshot_v1, hash_admission_state_v1,
};

fn source(name: &str) -> SourceIdV0 {
    SourceIdV0::new(name).unwrap()
}

fn cluster() -> KafkaClusterIdV0 {
    KafkaClusterIdV0::new("cluster-v0").unwrap()
}

fn topic() -> KafkaTopicIdV0 {
    KafkaTopicIdV0::from_bytes([8; 16]).unwrap()
}

fn contract(name: &str) -> KafkaSourceContractV0 {
    KafkaSourceContractV0::new(0, source(name), cluster(), topic()).unwrap()
}

fn admission_config(name: &str, kind: SourcePositionKindV0, end_ns: i64) -> AdmissionConfigV0 {
    AdmissionConfigV0::new_with_source_position_kind(
        0,
        source(name),
        kind,
        0,
        end_ns,
        AdmissionLimitsV0::maximum(),
    )
    .unwrap()
}

fn system(
    name: &str,
) -> (
    AdmissionConfigV0,
    AdmissionStateV0,
    KafkaPartitionFrontierV0,
) {
    let config = admission_config(name, SourcePositionKindV0::Kafka, 100);
    let state = AdmissionStateV0::new(config.clone());
    let frontier =
        KafkaPartitionFrontierV0::new(0, contract(name), &[KafkaPositionV0::new(0, 0).unwrap()])
            .unwrap();
    (config, state, frontier)
}

fn record(offset: i64, event_time_ns: i64, amount: u64) -> KafkaFetchedRecordV0 {
    let value = format!(
        "{{\"record_format\":\"decant-kafka-event\",\"record_version\":0,\"schema_id\":\"event.v0\",\"event_time_ns\":{event_time_ns},\"payload\":[{{\"name\":\"amount\",\"value\":{{\"kind\":\"u64\",\"value\":{amount}}}}}]}}"
    );
    KafkaFetchedRecordV0::new(
        KafkaPositionV0::new(0, offset).unwrap(),
        Some(b"key"),
        Some(value.as_bytes()),
        KafkaRecordTimestampV0::NotAvailable,
        &[],
    )
    .unwrap()
}

fn ingest(
    state: &mut AdmissionStateV0,
    frontier: &mut KafkaPartitionFrontierV0,
    record: &KafkaFetchedRecordV0,
) -> Result<decant_ingest::KafkaJsonIngestionOutcomeV0, decant_ingest::KafkaJsonIngestionErrorV0> {
    ingest_decant_json_kafka_record_v0(
        frontier,
        state,
        &source("source-v0"),
        &cluster(),
        topic(),
        record,
    )
}

#[test]
fn round_trip_reencode_and_continued_ingestion_are_exact() {
    let (config, mut uninterrupted_state, mut uninterrupted_frontier) = system("source-v0");
    let accepted = record(0, 50, 1);
    let rejected = record(1, 101, 2);
    ingest(
        &mut uninterrupted_state,
        &mut uninterrupted_frontier,
        &accepted,
    )
    .unwrap();
    ingest(
        &mut uninterrupted_state,
        &mut uninterrupted_frontier,
        &rejected,
    )
    .unwrap();

    let checkpoint =
        encode_kafka_ingestion_checkpoint_v0(&uninterrupted_state, &uninterrupted_frontier)
            .unwrap();
    let (mut restored_state, mut restored_frontier) = restore_kafka_ingestion_checkpoint_v0(
        &checkpoint,
        &config,
        uninterrupted_frontier.source_contract(),
    )
    .unwrap();
    assert_eq!(
        encode_kafka_ingestion_checkpoint_v0(&restored_state, &restored_frontier).unwrap(),
        checkpoint
    );
    assert_eq!(
        encode_admission_snapshot_v1(&restored_state).unwrap(),
        encode_admission_snapshot_v1(&uninterrupted_state).unwrap()
    );
    assert_eq!(
        hash_admission_state_v1(&restored_state).unwrap(),
        hash_admission_state_v1(&uninterrupted_state).unwrap()
    );
    assert_eq!(
        encode_kafka_partition_frontier_snapshot_v0(&restored_frontier).unwrap(),
        encode_kafka_partition_frontier_snapshot_v0(&uninterrupted_frontier).unwrap()
    );
    assert_eq!(
        hash_kafka_partition_frontier_v0(&restored_frontier).unwrap(),
        hash_kafka_partition_frontier_v0(&uninterrupted_frontier).unwrap()
    );

    let suffix = record(2, 60, 3);
    let uninterrupted_outcome = ingest(
        &mut uninterrupted_state,
        &mut uninterrupted_frontier,
        &suffix,
    );
    let restored_outcome = ingest(&mut restored_state, &mut restored_frontier, &suffix);
    assert_eq!(restored_outcome, uninterrupted_outcome);
    assert_eq!(
        encode_admission_snapshot_v1(&restored_state).unwrap(),
        encode_admission_snapshot_v1(&uninterrupted_state).unwrap()
    );
    assert_eq!(
        hash_kafka_partition_frontier_v0(&restored_frontier).unwrap(),
        hash_kafka_partition_frontier_v0(&uninterrupted_frontier).unwrap()
    );
}

#[test]
fn expected_configuration_is_checked_before_input_shape() {
    let (_, _, frontier) = system("source-v0");
    assert_eq!(
        restore_kafka_ingestion_checkpoint_v0(
            &vec![0_u8; MAX_KAFKA_INGESTION_CHECKPOINT_BYTES_V0 + 1],
            &admission_config("source-v0", SourcePositionKindV0::Synthetic, 100),
            frontier.source_contract(),
        )
        .unwrap_err(),
        KafkaIngestionCheckpointDecodeErrorV0::ExpectedAdmissionSourcePositionKind {
            actual: SourcePositionKindV0::Synthetic,
        }
    );
    assert_eq!(
        restore_kafka_ingestion_checkpoint_v0(
            &[],
            &admission_config("other-source", SourcePositionKindV0::Kafka, 100),
            frontier.source_contract(),
        )
        .unwrap_err(),
        KafkaIngestionCheckpointDecodeErrorV0::ExpectedSourceMismatch
    );
}

#[test]
fn every_prefix_oversize_trailing_and_header_defect_fail_closed() {
    let (config, state, frontier) = system("source-v0");
    let checkpoint = encode_kafka_ingestion_checkpoint_v0(&state, &frontier).unwrap();
    for length in 0..checkpoint.len() {
        assert!(
            restore_kafka_ingestion_checkpoint_v0(
                &checkpoint[..length],
                &config,
                frontier.source_contract(),
            )
            .is_err(),
            "prefix {length} unexpectedly restored"
        );
    }
    let oversized = vec![0_u8; MAX_KAFKA_INGESTION_CHECKPOINT_BYTES_V0 + 1];
    assert!(matches!(
        restore_kafka_ingestion_checkpoint_v0(&oversized, &config, frontier.source_contract()),
        Err(KafkaIngestionCheckpointDecodeErrorV0::DocumentTooLarge { .. })
    ));
    let mut trailing = checkpoint.clone();
    trailing.push(0);
    assert_eq!(
        restore_kafka_ingestion_checkpoint_v0(&trailing, &config, frontier.source_contract())
            .unwrap_err(),
        KafkaIngestionCheckpointDecodeErrorV0::TrailingBytes
    );
    for (index, expected) in [
        (8, KafkaIngestionCheckpointFieldV0::CheckpointVersion),
        (
            10,
            KafkaIngestionCheckpointFieldV0::AdmissionSnapshotVersion,
        ),
        (12, KafkaIngestionCheckpointFieldV0::FrontierSnapshotVersion),
    ] {
        let mut wrong = checkpoint.clone();
        wrong[index..index + 2].copy_from_slice(&9_u16.to_be_bytes());
        let error =
            restore_kafka_ingestion_checkpoint_v0(&wrong, &config, frontier.source_contract())
                .unwrap_err();
        match expected {
            KafkaIngestionCheckpointFieldV0::CheckpointVersion => assert!(matches!(
                error,
                KafkaIngestionCheckpointDecodeErrorV0::UnsupportedCheckpointVersion { .. }
            )),
            KafkaIngestionCheckpointFieldV0::AdmissionSnapshotVersion => assert!(matches!(
                error,
                KafkaIngestionCheckpointDecodeErrorV0::UnsupportedAdmissionSnapshotVersion { .. }
            )),
            KafkaIngestionCheckpointFieldV0::FrontierSnapshotVersion => assert!(matches!(
                error,
                KafkaIngestionCheckpointDecodeErrorV0::UnsupportedFrontierSnapshotVersion { .. }
            )),
            _ => unreachable!(),
        }
    }
}

#[test]
fn component_lengths_corruption_and_expected_config_mismatch_are_typed() {
    let (config, state, frontier) = system("source-v0");
    let checkpoint = encode_kafka_ingestion_checkpoint_v0(&state, &frontier).unwrap();
    let mut bad_admission_length = checkpoint.clone();
    bad_admission_length[14..22].copy_from_slice(
        &u64::try_from(MAX_ADMISSION_SNAPSHOT_BYTES_V1 + 1)
            .unwrap()
            .to_be_bytes(),
    );
    assert!(matches!(
        restore_kafka_ingestion_checkpoint_v0(
            &bad_admission_length,
            &config,
            frontier.source_contract()
        ),
        Err(
            KafkaIngestionCheckpointDecodeErrorV0::InvalidComponentLength {
                field: KafkaIngestionCheckpointFieldV0::AdmissionSnapshot,
                ..
            }
        )
    ));
    let mut bad_frontier_length = checkpoint.clone();
    bad_frontier_length[22..26].copy_from_slice(
        &u32::try_from(MAX_KAFKA_FRONTIER_SNAPSHOT_BYTES_V0 + 1)
            .unwrap()
            .to_be_bytes(),
    );
    assert!(matches!(
        restore_kafka_ingestion_checkpoint_v0(
            &bad_frontier_length,
            &config,
            frontier.source_contract()
        ),
        Err(
            KafkaIngestionCheckpointDecodeErrorV0::InvalidComponentLength {
                field: KafkaIngestionCheckpointFieldV0::FrontierSnapshot,
                ..
            }
        )
    ));

    let admission_length =
        usize::try_from(u64::from_be_bytes(checkpoint[14..22].try_into().unwrap())).unwrap();
    let frontier_start = 26 + admission_length;
    let mut bad_frontier = checkpoint.clone();
    bad_frontier[frontier_start] ^= 0xff;
    assert!(matches!(
        restore_kafka_ingestion_checkpoint_v0(&bad_frontier, &config, frontier.source_contract()),
        Err(KafkaIngestionCheckpointDecodeErrorV0::FrontierSnapshot(_))
    ));
    let mut bad_admission = checkpoint;
    bad_admission[26] ^= 0xff;
    assert!(matches!(
        restore_kafka_ingestion_checkpoint_v0(&bad_admission, &config, frontier.source_contract()),
        Err(KafkaIngestionCheckpointDecodeErrorV0::AdmissionSnapshot(_))
    ));
    assert!(matches!(
        restore_kafka_ingestion_checkpoint_v0(
            &bad_admission,
            &admission_config("source-v0", SourcePositionKindV0::Kafka, 200),
            frontier.source_contract()
        ),
        Err(KafkaIngestionCheckpointDecodeErrorV0::AdmissionSnapshot(_))
    ));
}

#[test]
fn individually_valid_but_inconsistent_components_are_rejected() {
    let (config, mut state, mut frontier) = system("source-v0");
    ingest(&mut state, &mut frontier, &record(0, 50, 1)).unwrap();
    let mut checkpoint = encode_kafka_ingestion_checkpoint_v0(&state, &frontier).unwrap();
    let length = checkpoint.len();
    checkpoint[length - 8..].copy_from_slice(&0_i64.to_be_bytes());
    assert_eq!(
        restore_kafka_ingestion_checkpoint_v0(&checkpoint, &config, frontier.source_contract(),)
            .unwrap_err(),
        KafkaIngestionCheckpointDecodeErrorV0::AcceptedEventNotBeforeFrontier
    );
}

#[test]
fn diagnostics_do_not_disclose_component_bytes_or_identity_values() {
    let (config, state, frontier) = system("source-v0");
    let mut checkpoint = encode_kafka_ingestion_checkpoint_v0(&state, &frontier).unwrap();
    checkpoint[26..38].copy_from_slice(b"secret-value");
    let rendered = format!(
        "{:?}",
        restore_kafka_ingestion_checkpoint_v0(&checkpoint, &config, frontier.source_contract(),)
            .unwrap_err()
    );
    assert!(!rendered.contains("secret-value"));
    assert!(!rendered.contains("source-v0"));
}
