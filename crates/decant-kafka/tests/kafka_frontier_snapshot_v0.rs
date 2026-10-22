use decant_ir::{KafkaPositionV0, MAX_KAFKA_OFFSET_V0, SourceIdV0};
use decant_kafka::{
    KAFKA_PARTITION_FRONTIER_VERSION_V0, KAFKA_SOURCE_CONTRACT_VERSION_V0, KafkaClusterIdV0,
    KafkaFrontierSnapshotDecodeErrorV0, KafkaFrontierSnapshotExpectedContractFieldV0,
    KafkaPartitionFrontierV0, KafkaSourceContractV0, KafkaTopicIdV0,
    MAX_KAFKA_ASSIGNED_PARTITIONS_V0, MAX_KAFKA_FRONTIER_SNAPSHOT_BYTES_V0,
    encode_kafka_partition_frontier_snapshot_v0, restore_kafka_partition_frontier_snapshot_v0,
};

fn contract(source: &str, cluster: &str, topic_byte: u8) -> KafkaSourceContractV0 {
    KafkaSourceContractV0::new(
        KAFKA_SOURCE_CONTRACT_VERSION_V0,
        SourceIdV0::new(source).unwrap(),
        KafkaClusterIdV0::new(cluster).unwrap(),
        KafkaTopicIdV0::from_bytes([topic_byte; 16]).unwrap(),
    )
    .unwrap()
}

fn frontier(entries: &[(i32, i64)]) -> KafkaPartitionFrontierV0 {
    KafkaPartitionFrontierV0::new(
        KAFKA_PARTITION_FRONTIER_VERSION_V0,
        contract("source", "cluster", 1),
        &entries
            .iter()
            .map(|(partition, offset)| KafkaPositionV0::new(*partition, *offset).unwrap())
            .collect::<Vec<_>>(),
    )
    .unwrap()
}

#[test]
fn single_partition_golden_bytes_are_exact() {
    let bytes = encode_kafka_partition_frontier_snapshot_v0(&frontier(&[(0, 7)])).unwrap();
    let mut expected = Vec::new();
    expected.extend_from_slice(b"DECKFRT0");
    expected.extend_from_slice(&0_u16.to_be_bytes());
    expected.extend_from_slice(&0_u16.to_be_bytes());
    expected.extend_from_slice(&0_u16.to_be_bytes());
    expected.extend_from_slice(&6_u16.to_be_bytes());
    expected.extend_from_slice(b"source");
    expected.extend_from_slice(&7_u16.to_be_bytes());
    expected.extend_from_slice(b"cluster");
    expected.extend_from_slice(&[1_u8; 16]);
    expected.extend_from_slice(&1_u16.to_be_bytes());
    expected.extend_from_slice(&0_i32.to_be_bytes());
    expected.extend_from_slice(&7_i64.to_be_bytes());
    assert_eq!(bytes, expected);
}

#[test]
fn canonical_order_round_trip_and_repeated_encoding_are_exact() {
    let source = frontier(&[(9, 3), (1, 8), (5, 13)]);
    let first = encode_kafka_partition_frontier_snapshot_v0(&source).unwrap();
    let second = encode_kafka_partition_frontier_snapshot_v0(&source).unwrap();
    assert_eq!(first, second);
    let restored =
        restore_kafka_partition_frontier_snapshot_v0(&first, source.source_contract()).unwrap();
    assert_eq!(restored, source);
    assert_eq!(
        restored.iter().collect::<Vec<_>>(),
        vec![(1, 8), (5, 13), (9, 3)]
    );
    assert_eq!(
        encode_kafka_partition_frontier_snapshot_v0(&restored).unwrap(),
        first
    );
}

#[test]
fn terminal_next_offset_round_trips_but_is_not_a_record_position() {
    let mut source = frontier(&[(0, MAX_KAFKA_OFFSET_V0)]);
    let source_id = source.source_contract().source_id().clone();
    let cluster_id = source.source_contract().cluster_id().clone();
    let topic_id = source.source_contract().topic_id();
    source
        .observe(
            &source_id,
            &cluster_id,
            topic_id,
            KafkaPositionV0::new(0, MAX_KAFKA_OFFSET_V0).unwrap(),
        )
        .unwrap();
    assert_eq!(source.next_offset(0), Some(MAX_KAFKA_OFFSET_V0 + 1));
    let bytes = encode_kafka_partition_frontier_snapshot_v0(&source).unwrap();
    assert_eq!(
        restore_kafka_partition_frontier_snapshot_v0(&bytes, source.source_contract()).unwrap(),
        source
    );
    assert!(KafkaPositionV0::new(0, MAX_KAFKA_OFFSET_V0 + 1).is_err());
}

#[test]
fn expected_contract_mismatch_precedes_assignment_decoding() {
    let source = frontier(&[(0, 1)]);
    let mut bytes = encode_kafka_partition_frontier_snapshot_v0(&source).unwrap();
    bytes.truncate(bytes.len() - 12);
    assert_eq!(
        restore_kafka_partition_frontier_snapshot_v0(
            &bytes,
            &contract("other-source", "cluster", 1),
        )
        .unwrap_err(),
        KafkaFrontierSnapshotDecodeErrorV0::ExpectedSourceContractMismatch {
            field: KafkaFrontierSnapshotExpectedContractFieldV0::SourceId,
        }
    );
    assert_eq!(
        restore_kafka_partition_frontier_snapshot_v0(
            &encode_kafka_partition_frontier_snapshot_v0(&source).unwrap(),
            &contract("source", "other-cluster", 1),
        )
        .unwrap_err(),
        KafkaFrontierSnapshotDecodeErrorV0::ExpectedSourceContractMismatch {
            field: KafkaFrontierSnapshotExpectedContractFieldV0::ClusterId,
        }
    );
    assert_eq!(
        restore_kafka_partition_frontier_snapshot_v0(
            &encode_kafka_partition_frontier_snapshot_v0(&source).unwrap(),
            &contract("source", "cluster", 2),
        )
        .unwrap_err(),
        KafkaFrontierSnapshotDecodeErrorV0::ExpectedSourceContractMismatch {
            field: KafkaFrontierSnapshotExpectedContractFieldV0::TopicId,
        }
    );
}

#[test]
fn count_order_and_coordinate_defects_fail_closed() {
    let source = frontier(&[(1, 8), (2, 9)]);
    let bytes = encode_kafka_partition_frontier_snapshot_v0(&source).unwrap();
    let count_index = 8 + 2 + 2 + 2 + 2 + 6 + 2 + 7 + 16;

    let mut empty = bytes.clone();
    empty[count_index..count_index + 2].copy_from_slice(&0_u16.to_be_bytes());
    empty.truncate(count_index + 2);
    assert_eq!(
        restore_kafka_partition_frontier_snapshot_v0(&empty, source.source_contract()).unwrap_err(),
        KafkaFrontierSnapshotDecodeErrorV0::EmptyAssignment
    );

    let mut too_many = bytes.clone();
    too_many[count_index..count_index + 2].copy_from_slice(
        &u16::try_from(MAX_KAFKA_ASSIGNED_PARTITIONS_V0 + 1)
            .unwrap()
            .to_be_bytes(),
    );
    assert!(matches!(
        restore_kafka_partition_frontier_snapshot_v0(&too_many, source.source_contract()),
        Err(KafkaFrontierSnapshotDecodeErrorV0::TooManyAssignments { .. })
    ));

    let first_entry = count_index + 2;
    let mut duplicate = bytes.clone();
    duplicate[first_entry + 12..first_entry + 16].copy_from_slice(&1_i32.to_be_bytes());
    assert_eq!(
        restore_kafka_partition_frontier_snapshot_v0(&duplicate, source.source_contract())
            .unwrap_err(),
        KafkaFrontierSnapshotDecodeErrorV0::NonCanonicalPartitionOrder
    );

    let mut descending = bytes.clone();
    descending[first_entry..first_entry + 4].copy_from_slice(&2_i32.to_be_bytes());
    descending[first_entry + 12..first_entry + 16].copy_from_slice(&1_i32.to_be_bytes());
    assert_eq!(
        restore_kafka_partition_frontier_snapshot_v0(&descending, source.source_contract())
            .unwrap_err(),
        KafkaFrontierSnapshotDecodeErrorV0::NonCanonicalPartitionOrder
    );

    let mut invalid_partition = bytes.clone();
    invalid_partition[first_entry..first_entry + 4].copy_from_slice(&(-1_i32).to_be_bytes());
    assert!(matches!(
        restore_kafka_partition_frontier_snapshot_v0(&invalid_partition, source.source_contract()),
        Err(KafkaFrontierSnapshotDecodeErrorV0::InvalidCoordinate { .. })
    ));

    let mut invalid_offset = bytes;
    invalid_offset[first_entry + 4..first_entry + 12]
        .copy_from_slice(&(MAX_KAFKA_OFFSET_V0 + 2).to_be_bytes());
    assert!(matches!(
        restore_kafka_partition_frontier_snapshot_v0(&invalid_offset, source.source_contract()),
        Err(KafkaFrontierSnapshotDecodeErrorV0::InvalidCoordinate { .. })
    ));
}

#[test]
fn hostile_document_shapes_are_rejected_without_value_disclosure() {
    let source = frontier(&[(0, 7)]);
    let bytes = encode_kafka_partition_frontier_snapshot_v0(&source).unwrap();
    for length in 0..bytes.len() {
        assert!(
            restore_kafka_partition_frontier_snapshot_v0(
                &bytes[..length],
                source.source_contract()
            )
            .is_err(),
            "prefix {length} unexpectedly restored"
        );
    }
    let mut trailing = bytes;
    trailing.push(0);
    assert_eq!(
        restore_kafka_partition_frontier_snapshot_v0(&trailing, source.source_contract())
            .unwrap_err(),
        KafkaFrontierSnapshotDecodeErrorV0::TrailingBytes
    );
    let oversized = vec![0_u8; MAX_KAFKA_FRONTIER_SNAPSHOT_BYTES_V0 + 1];
    assert!(matches!(
        restore_kafka_partition_frontier_snapshot_v0(&oversized, source.source_contract()),
        Err(KafkaFrontierSnapshotDecodeErrorV0::DocumentTooLarge { .. })
    ));
    let rendered = format!(
        "{:?}",
        restore_kafka_partition_frontier_snapshot_v0(
            b"synthetic-secret-value",
            source.source_contract(),
        )
        .unwrap_err()
    );
    assert!(!rendered.contains("synthetic-secret-value"));
}
