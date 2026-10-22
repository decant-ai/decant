use std::any::Any;

use decant_ir::{KafkaPositionV0, MAX_KAFKA_OFFSET_V0, SourceIdV0};
use decant_kafka::{
    KAFKA_PARTITION_FRONTIER_VERSION_V0, KafkaClusterIdV0, KafkaOffsetObservationV0,
    KafkaPartitionFrontierErrorV0, KafkaPartitionFrontierV0, KafkaSourceContractV0,
    KafkaSourceObservationErrorV0, KafkaTopicIdV0, MAX_KAFKA_ASSIGNED_PARTITIONS_V0,
};

fn source(value: &str) -> SourceIdV0 {
    SourceIdV0::new(value).unwrap()
}

fn cluster(value: &str) -> KafkaClusterIdV0 {
    KafkaClusterIdV0::new(value).unwrap()
}

fn topic(byte: u8) -> KafkaTopicIdV0 {
    KafkaTopicIdV0::from_bytes([byte; 16]).unwrap()
}

fn position(partition: i32, offset: i64) -> KafkaPositionV0 {
    KafkaPositionV0::new(partition, offset).unwrap()
}

fn source_contract() -> KafkaSourceContractV0 {
    KafkaSourceContractV0::new(
        0,
        source("orders-source-secret"),
        cluster("cluster-source-secret"),
        topic(0x5a),
    )
    .unwrap()
}

fn frontier(starts: &[KafkaPositionV0]) -> KafkaPartitionFrontierV0 {
    KafkaPartitionFrontierV0::new(
        KAFKA_PARTITION_FRONTIER_VERSION_V0,
        source_contract(),
        starts,
    )
    .unwrap()
}

fn observe(
    frontier: &mut KafkaPartitionFrontierV0,
    position: KafkaPositionV0,
) -> Result<KafkaOffsetObservationV0, KafkaPartitionFrontierErrorV0> {
    frontier.observe(
        &source("orders-source-secret"),
        &cluster("cluster-source-secret"),
        topic(0x5a),
        position,
    )
}

#[test]
fn version_and_assignment_boundaries_are_exact() {
    assert_eq!(
        KafkaPartitionFrontierV0::new(0, source_contract(), &[]),
        Err(KafkaPartitionFrontierErrorV0::EmptyAssignment)
    );
    for invalid_version in [1, u16::MAX] {
        assert_eq!(
            KafkaPartitionFrontierV0::new(invalid_version, source_contract(), &[]),
            Err(KafkaPartitionFrontierErrorV0::UnsupportedVersion {
                actual: invalid_version,
            })
        );
    }

    let one = frontier(&[position(7, 41)]);
    assert_eq!(one.version(), KAFKA_PARTITION_FRONTIER_VERSION_V0);
    assert_eq!(one.assignment_count(), 1);
    assert_eq!(one.next_offset(7), Some(41));
    assert_eq!(one.next_offset(0), None);
    assert_eq!(
        one.source_contract().source_id(),
        &source("orders-source-secret")
    );

    let exact: Vec<_> = (0..MAX_KAFKA_ASSIGNED_PARTITIONS_V0)
        .map(|partition| position(partition as i32, partition as i64 + 1))
        .collect();
    let maximum = frontier(&exact);
    assert_eq!(maximum.assignment_count(), MAX_KAFKA_ASSIGNED_PARTITIONS_V0);
    assert_eq!(maximum.next_offset(0), Some(1));
    assert_eq!(
        maximum.next_offset(MAX_KAFKA_ASSIGNED_PARTITIONS_V0 as i32 - 1),
        Some(MAX_KAFKA_ASSIGNED_PARTITIONS_V0 as i64)
    );

    let one_past: Vec<_> = (0..=MAX_KAFKA_ASSIGNED_PARTITIONS_V0)
        .map(|partition| position(partition as i32, 0))
        .collect();
    assert_eq!(
        KafkaPartitionFrontierV0::new(0, source_contract(), &one_past),
        Err(KafkaPartitionFrontierErrorV0::TooManyPartitions {
            maximum: MAX_KAFKA_ASSIGNED_PARTITIONS_V0,
            actual: MAX_KAFKA_ASSIGNED_PARTITIONS_V0 + 1,
        })
    );
}

#[test]
fn duplicate_partitions_reject_and_input_order_is_canonical() {
    for duplicate in [
        vec![position(3, 10), position(3, 10)],
        vec![position(3, 10), position(3, 11)],
    ] {
        assert_eq!(
            KafkaPartitionFrontierV0::new(0, source_contract(), &duplicate),
            Err(KafkaPartitionFrontierErrorV0::DuplicatePartition)
        );
    }

    let ascending = [position(1, 10), position(5, 50), position(9, 90)];
    let reversed = [position(9, 90), position(5, 50), position(1, 10)];
    let left = frontier(&ascending);
    let right = frontier(&reversed);
    assert_eq!(left, right);
    assert_eq!(
        left.iter().collect::<Vec<_>>(),
        vec![(1, 10), (5, 50), (9, 90)]
    );
    assert_eq!(
        left.iter().rev().collect::<Vec<_>>(),
        vec![(9, 90), (5, 50), (1, 10)]
    );
}

#[test]
fn exact_before_and_gap_transitions_are_non_buffering_and_partition_local() {
    let mut current = frontier(&[position(1, 10), position(2, 20)]);

    for (observed, expected) in [
        (position(1, 9), KafkaOffsetObservationV0::BeforeFrontier),
        (position(1, 11), KafkaOffsetObservationV0::Gap),
    ] {
        let before = current.clone();
        assert_eq!(observe(&mut current, observed), Ok(expected));
        assert_eq!(current, before);
    }

    assert_eq!(
        observe(&mut current, position(1, 10)),
        Ok(KafkaOffsetObservationV0::Advanced)
    );
    assert_eq!(current.next_offset(1), Some(11));
    assert_eq!(current.next_offset(2), Some(20));

    let before = current.clone();
    assert_eq!(
        observe(&mut current, position(1, 12)),
        Ok(KafkaOffsetObservationV0::Gap)
    );
    assert_eq!(current, before);
    assert_eq!(
        observe(&mut current, position(1, 11)),
        Ok(KafkaOffsetObservationV0::Advanced)
    );
    assert_eq!(current.next_offset(1), Some(12));

    let before = current.clone();
    assert_eq!(
        observe(&mut current, position(1, 11)),
        Ok(KafkaOffsetObservationV0::BeforeFrontier)
    );
    assert_eq!(current, before);
}

#[test]
fn source_incarnation_and_partition_precedence_is_fail_closed() {
    let mut current = frontier(&[position(1, 10)]);
    let cases = [
        (
            source("other-source-secret"),
            cluster("other-cluster-secret"),
            topic(0xa5),
            position(99, 12),
            KafkaPartitionFrontierErrorV0::SourceObservation {
                category: KafkaSourceObservationErrorV0::SourceMismatch,
            },
        ),
        (
            source("orders-source-secret"),
            cluster("other-cluster-secret"),
            topic(0xa5),
            position(99, 12),
            KafkaPartitionFrontierErrorV0::SourceObservation {
                category: KafkaSourceObservationErrorV0::ClusterMismatch,
            },
        ),
        (
            source("orders-source-secret"),
            cluster("cluster-source-secret"),
            topic(0xa5),
            position(99, 12),
            KafkaPartitionFrontierErrorV0::SourceObservation {
                category: KafkaSourceObservationErrorV0::TopicIncarnationMismatch,
            },
        ),
        (
            source("orders-source-secret"),
            cluster("cluster-source-secret"),
            topic(0x5a),
            position(99, 12),
            KafkaPartitionFrontierErrorV0::UnknownPartition,
        ),
    ];

    for (observed_source, observed_cluster, observed_topic, observed_position, expected) in cases {
        let before = current.clone();
        assert_eq!(
            current.observe(
                &observed_source,
                &observed_cluster,
                observed_topic,
                observed_position,
            ),
            Err(expected)
        );
        assert_eq!(current, before);
    }

    let before = current.clone();
    assert_eq!(
        observe(&mut current, position(1, 12)),
        Ok(KafkaOffsetObservationV0::Gap)
    );
    assert_eq!(current, before);
}

#[test]
fn maximum_offset_advances_to_a_terminal_v0_sentinel_without_overflow() {
    let mut current = frontier(&[position(65_535, MAX_KAFKA_OFFSET_V0)]);
    assert_eq!(
        observe(&mut current, position(65_535, MAX_KAFKA_OFFSET_V0)),
        Ok(KafkaOffsetObservationV0::Advanced)
    );
    assert_eq!(current.next_offset(65_535), Some(MAX_KAFKA_OFFSET_V0 + 1));

    let before = current.clone();
    assert_eq!(
        observe(&mut current, position(65_535, MAX_KAFKA_OFFSET_V0)),
        Ok(KafkaOffsetObservationV0::BeforeFrontier)
    );
    assert_eq!(current, before);
}

#[test]
fn diagnostics_and_assertion_panics_do_not_expose_identity_or_coordinates() {
    let configured = frontier(&[position(12_345, 234_567)]);
    let diagnostic = format!(
        "{configured:?} {} {} {} {} {} {} {} {} {} {} {}",
        KafkaOffsetObservationV0::Advanced,
        KafkaOffsetObservationV0::BeforeFrontier,
        KafkaOffsetObservationV0::Gap,
        KafkaPartitionFrontierErrorV0::UnsupportedVersion { actual: 1 },
        KafkaPartitionFrontierErrorV0::EmptyAssignment,
        KafkaPartitionFrontierErrorV0::TooManyPartitions {
            maximum: MAX_KAFKA_ASSIGNED_PARTITIONS_V0,
            actual: MAX_KAFKA_ASSIGNED_PARTITIONS_V0 + 1,
        },
        KafkaPartitionFrontierErrorV0::DuplicatePartition,
        KafkaPartitionFrontierErrorV0::UnknownPartition,
        KafkaPartitionFrontierErrorV0::SourceObservation {
            category: KafkaSourceObservationErrorV0::SourceMismatch,
        },
        KafkaPartitionFrontierErrorV0::SourceObservation {
            category: KafkaSourceObservationErrorV0::ClusterMismatch,
        },
        KafkaPartitionFrontierErrorV0::SourceObservation {
            category: KafkaSourceObservationErrorV0::TopicIncarnationMismatch,
        },
    );
    assert_safe(&diagnostic);

    let different = frontier(&[position(54_321, 765_432)]);
    let panic = std::panic::catch_unwind(|| assert_eq!(configured, different)).unwrap_err();
    assert_safe(&panic_text(panic));
}

#[test]
fn repeated_traces_are_identical_and_never_change_assignment_size() {
    let expected = run_trace();
    for _ in 0..128 {
        assert_eq!(run_trace(), expected);
        assert_eq!(expected.assignment_count(), 3);
    }
}

fn run_trace() -> KafkaPartitionFrontierV0 {
    let mut current = frontier(&[position(7, 70), position(3, 30), position(5, 50)]);
    for observed in [
        position(3, 29),
        position(5, 51),
        position(7, 70),
        position(3, 30),
        position(5, 50),
    ] {
        observe(&mut current, observed).unwrap();
    }
    current
}

fn assert_safe(diagnostic: &str) {
    for secret in [
        "orders-source-secret",
        "other-source-secret",
        "cluster-source-secret",
        "other-cluster-secret",
        "5a5a5a5a",
        "a5a5a5a5",
        "12345",
        "234567",
        "54321",
        "765432",
    ] {
        assert!(!diagnostic.contains(secret), "leaked {secret}");
    }
}

fn panic_text(payload: Box<dyn Any + Send>) -> String {
    if let Some(message) = payload.downcast_ref::<String>() {
        message.clone()
    } else if let Some(message) = payload.downcast_ref::<&str>() {
        (*message).to_owned()
    } else {
        String::new()
    }
}
