use decant_ir::{KafkaPositionV0, SourceIdV0};
use decant_kafka::{
    KAFKA_FRONTIER_HASH_ALGORITHM_V0, KAFKA_FRONTIER_HASH_DOMAIN_V0,
    KAFKA_FRONTIER_HASH_TEXT_BYTES_V0, KAFKA_FRONTIER_HASH_VERSION_V0,
    KAFKA_PARTITION_FRONTIER_VERSION_V0, KAFKA_SOURCE_CONTRACT_VERSION_V0, KafkaClusterIdV0,
    KafkaPartitionFrontierV0, KafkaSourceContractV0, KafkaTopicIdV0,
    hash_kafka_partition_frontier_v0,
};

fn frontier(
    source: &str,
    cluster: &str,
    topic_byte: u8,
    entries: &[(i32, i64)],
) -> KafkaPartitionFrontierV0 {
    KafkaPartitionFrontierV0::new(
        KAFKA_PARTITION_FRONTIER_VERSION_V0,
        KafkaSourceContractV0::new(
            KAFKA_SOURCE_CONTRACT_VERSION_V0,
            SourceIdV0::new(source).unwrap(),
            KafkaClusterIdV0::new(cluster).unwrap(),
            KafkaTopicIdV0::from_bytes([topic_byte; 16]).unwrap(),
        )
        .unwrap(),
        &entries
            .iter()
            .map(|(partition, offset)| KafkaPositionV0::new(*partition, *offset).unwrap())
            .collect::<Vec<_>>(),
    )
    .unwrap()
}

#[test]
fn constants_text_and_one_partition_vector_are_exact() {
    assert_eq!(KAFKA_FRONTIER_HASH_VERSION_V0, 0);
    assert_eq!(KAFKA_FRONTIER_HASH_ALGORITHM_V0, "sha256");
    assert_eq!(KAFKA_FRONTIER_HASH_DOMAIN_V0.len(), 29);
    let hash =
        hash_kafka_partition_frontier_v0(&frontier("source", "cluster", 1, &[(0, 7)])).unwrap();
    let text = hash.to_canonical_string();
    assert_eq!(text.len(), KAFKA_FRONTIER_HASH_TEXT_BYTES_V0);
    assert_eq!(hash.as_bytes().len(), 32);
    assert_eq!(
        text,
        "sha256:v0:843a608b38cd64ad7580937e67a653b5490c7418507480d72c4a5f27107c6086"
    );
}

#[test]
fn construction_order_is_irrelevant_and_multi_partition_vector_is_exact() {
    let left = frontier("source", "cluster", 1, &[(9, 3), (1, 8), (5, 13)]);
    let right = frontier("source", "cluster", 1, &[(5, 13), (9, 3), (1, 8)]);
    let left_hash = hash_kafka_partition_frontier_v0(&left).unwrap();
    assert_eq!(left_hash, hash_kafka_partition_frontier_v0(&right).unwrap());
    assert_eq!(
        left_hash.to_canonical_string(),
        "sha256:v0:93774f5d065bf48fa8024383b75a1db81228812918b8fbc322cd465310ec5e92"
    );
}

#[test]
fn every_covered_identity_or_coordinate_change_changes_tested_hash() {
    let baseline =
        hash_kafka_partition_frontier_v0(&frontier("source", "cluster", 1, &[(0, 7), (1, 8)]))
            .unwrap();
    let variants = [
        frontier("other", "cluster", 1, &[(0, 7), (1, 8)]),
        frontier("source", "other", 1, &[(0, 7), (1, 8)]),
        frontier("source", "cluster", 2, &[(0, 7), (1, 8)]),
        frontier("source", "cluster", 1, &[(0, 7), (2, 8)]),
        frontier("source", "cluster", 1, &[(0, 7), (1, 9)]),
    ];
    for variant in variants {
        assert_ne!(
            baseline,
            hash_kafka_partition_frontier_v0(&variant).unwrap()
        );
    }
}

#[test]
fn repeated_hashing_is_exact_and_debug_redacts_digest() {
    let source = frontier("synthetic-secret-source", "cluster", 1, &[(0, 7)]);
    let first = hash_kafka_partition_frontier_v0(&source).unwrap();
    let second = hash_kafka_partition_frontier_v0(&source).unwrap();
    assert_eq!(first, second);
    let debug = format!("{first:?}");
    assert!(debug.contains("<redacted>"));
    assert!(!debug.contains(&first.to_string()));
    assert!(!debug.contains("synthetic-secret-source"));
}
