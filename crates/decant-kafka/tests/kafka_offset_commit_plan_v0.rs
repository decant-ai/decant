use decant_ir::{KafkaPositionV0, MAX_KAFKA_OFFSET_V0, SourceIdV0};
use decant_kafka::{
    KAFKA_OFFSET_COMMIT_PLAN_VERSION_V0, KAFKA_PARTITION_FRONTIER_VERSION_V0,
    KAFKA_SOURCE_CONTRACT_VERSION_V0, KafkaClusterIdV0, KafkaOffsetObservationV0,
    KafkaPartitionFrontierV0, KafkaSourceContractV0, KafkaTopicIdV0,
    MAX_KAFKA_ASSIGNED_PARTITIONS_V0, build_kafka_offset_commit_plan_v0,
    hash_kafka_partition_frontier_v0,
};

fn source() -> SourceIdV0 {
    SourceIdV0::new("commit-plan-source-secret").unwrap()
}

fn cluster() -> KafkaClusterIdV0 {
    KafkaClusterIdV0::new("commit-plan-cluster-secret").unwrap()
}

fn topic() -> KafkaTopicIdV0 {
    KafkaTopicIdV0::from_bytes([0x5a; 16]).unwrap()
}

fn position(partition: i32, offset: i64) -> KafkaPositionV0 {
    KafkaPositionV0::new(partition, offset).unwrap()
}

fn frontier(entries: &[(i32, i64)]) -> KafkaPartitionFrontierV0 {
    KafkaPartitionFrontierV0::new(
        KAFKA_PARTITION_FRONTIER_VERSION_V0,
        KafkaSourceContractV0::new(
            KAFKA_SOURCE_CONTRACT_VERSION_V0,
            source(),
            cluster(),
            topic(),
        )
        .unwrap(),
        &entries
            .iter()
            .map(|(partition, offset)| position(*partition, *offset))
            .collect::<Vec<_>>(),
    )
    .unwrap()
}

#[test]
fn plan_is_exact_canonical_and_source_incarnation_bound() {
    let input = frontier(&[(9, 90), (1, 10), (5, 50)]);
    let reordered = frontier(&[(5, 50), (9, 90), (1, 10)]);
    let plan = build_kafka_offset_commit_plan_v0(&input).unwrap();

    assert_eq!(plan.version(), KAFKA_OFFSET_COMMIT_PLAN_VERSION_V0);
    assert_eq!(plan.assignment_count(), 3);
    assert_eq!(
        plan.iter().collect::<Vec<_>>(),
        vec![(1, 10), (5, 50), (9, 90)]
    );
    assert_eq!(plan.next_offset(5), Some(50));
    assert_eq!(plan.next_offset(7), None);
    assert_eq!(plan.source_contract().source_id(), &source());
    assert_eq!(plan.source_contract().cluster_id(), &cluster());
    assert_eq!(plan.source_contract().topic_id(), topic());
    assert_eq!(
        plan.frontier_hash(),
        hash_kafka_partition_frontier_v0(&input).unwrap()
    );
    assert_eq!(plan, build_kafka_offset_commit_plan_v0(&reordered).unwrap());
}

#[test]
fn plan_is_frozen_when_the_source_frontier_advances() {
    let mut input = frontier(&[(1, 10), (2, 20)]);
    let plan = build_kafka_offset_commit_plan_v0(&input).unwrap();
    let frozen_hash = plan.frontier_hash();

    assert_eq!(
        input.observe(&source(), &cluster(), topic(), position(1, 10)),
        Ok(KafkaOffsetObservationV0::Advanced)
    );
    assert_eq!(input.next_offset(1), Some(11));
    assert_eq!(plan.next_offset(1), Some(10));
    assert_eq!(plan.frontier_hash(), frozen_hash);
    assert_ne!(
        plan.frontier_hash(),
        hash_kafka_partition_frontier_v0(&input).unwrap()
    );
}

#[test]
fn maximum_assignment_and_terminal_next_offset_remain_bounded_and_exact() {
    let entries = (0..MAX_KAFKA_ASSIGNED_PARTITIONS_V0)
        .map(|partition| (partition as i32, partition as i64))
        .collect::<Vec<_>>();
    let maximum = build_kafka_offset_commit_plan_v0(&frontier(&entries)).unwrap();
    assert_eq!(maximum.assignment_count(), MAX_KAFKA_ASSIGNED_PARTITIONS_V0);
    assert_eq!(
        maximum.iter().last(),
        Some((
            MAX_KAFKA_ASSIGNED_PARTITIONS_V0 as i32 - 1,
            MAX_KAFKA_ASSIGNED_PARTITIONS_V0 as i64 - 1
        ))
    );

    let mut terminal = frontier(&[(65_535, MAX_KAFKA_OFFSET_V0)]);
    assert_eq!(
        terminal.observe(
            &source(),
            &cluster(),
            topic(),
            position(65_535, MAX_KAFKA_OFFSET_V0)
        ),
        Ok(KafkaOffsetObservationV0::Advanced)
    );
    let terminal_plan = build_kafka_offset_commit_plan_v0(&terminal).unwrap();
    assert_eq!(
        terminal_plan.next_offset(65_535),
        Some(MAX_KAFKA_OFFSET_V0 + 1)
    );
}

#[test]
fn plan_diagnostics_are_value_safe() {
    let plan = build_kafka_offset_commit_plan_v0(&frontier(&[(12_345, 234_567)])).unwrap();
    let canonical_hash = plan.frontier_hash().to_canonical_string();
    let debug = format!("{plan:?}");

    assert!(debug.contains("KafkaOffsetCommitPlanV0"));
    assert!(debug.contains("assignment_count"));
    assert!(debug.contains("<redacted>"));
    for secret in [
        "commit-plan-source-secret",
        "commit-plan-cluster-secret",
        "12345",
        "234567",
        canonical_hash.as_str(),
    ] {
        assert!(!debug.contains(secret));
    }
}
