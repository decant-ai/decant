use decant_ir::{KafkaPositionV0, SourceIdV0};
use decant_kafka::{
    KafkaClusterIdV0, KafkaOffsetClassificationV0, KafkaOffsetObservationV0,
    KafkaPartitionFrontierErrorV0, KafkaPartitionFrontierV0, KafkaSourceContractV0,
    KafkaSourceObservationErrorV0, KafkaTopicIdV0,
};

fn contract() -> KafkaSourceContractV0 {
    KafkaSourceContractV0::new(
        0,
        SourceIdV0::new("source-v0").unwrap(),
        KafkaClusterIdV0::new("cluster-v0").unwrap(),
        KafkaTopicIdV0::from_bytes([7; 16]).unwrap(),
    )
    .unwrap()
}

fn observed() -> (SourceIdV0, KafkaClusterIdV0, KafkaTopicIdV0) {
    (
        SourceIdV0::new("source-v0").unwrap(),
        KafkaClusterIdV0::new("cluster-v0").unwrap(),
        KafkaTopicIdV0::from_bytes([7; 16]).unwrap(),
    )
}

#[test]
fn expected_before_and_gap_classification_never_mutate() {
    let mut frontier = KafkaPartitionFrontierV0::new(
        0,
        contract(),
        &[
            KafkaPositionV0::new(0, 10).unwrap(),
            KafkaPositionV0::new(1, 20).unwrap(),
        ],
    )
    .unwrap();
    let (source, cluster, topic) = observed();

    for _ in 0..3 {
        assert_eq!(
            frontier.classify(
                &source,
                &cluster,
                topic,
                KafkaPositionV0::new(0, 10).unwrap(),
            ),
            Ok(KafkaOffsetClassificationV0::Expected)
        );
        assert_eq!(
            frontier.classify(
                &source,
                &cluster,
                topic,
                KafkaPositionV0::new(0, 9).unwrap(),
            ),
            Ok(KafkaOffsetClassificationV0::BeforeFrontier)
        );
        assert_eq!(
            frontier.classify(
                &source,
                &cluster,
                topic,
                KafkaPositionV0::new(0, 11).unwrap(),
            ),
            Ok(KafkaOffsetClassificationV0::Gap)
        );
        assert_eq!(frontier.next_offset(0), Some(10));
        assert_eq!(frontier.next_offset(1), Some(20));
    }

    assert_eq!(
        frontier.observe(
            &source,
            &cluster,
            topic,
            KafkaPositionV0::new(0, 10).unwrap(),
        ),
        Ok(KafkaOffsetObservationV0::Advanced)
    );
    assert_eq!(frontier.next_offset(0), Some(11));
    assert_eq!(frontier.next_offset(1), Some(20));
}

#[test]
fn source_precedence_and_unknown_partition_are_fail_closed() {
    let frontier =
        KafkaPartitionFrontierV0::new(0, contract(), &[KafkaPositionV0::new(0, 10).unwrap()])
            .unwrap();
    let (source, cluster, topic) = observed();
    let other_source = SourceIdV0::new("other-v0").unwrap();
    assert_eq!(
        frontier.classify(
            &other_source,
            &cluster,
            topic,
            KafkaPositionV0::new(99, 1).unwrap(),
        ),
        Err(KafkaPartitionFrontierErrorV0::SourceObservation {
            category: KafkaSourceObservationErrorV0::SourceMismatch,
        })
    );
    assert_eq!(
        frontier.classify(
            &source,
            &cluster,
            topic,
            KafkaPositionV0::new(99, 1).unwrap(),
        ),
        Err(KafkaPartitionFrontierErrorV0::UnknownPartition)
    );
    assert_eq!(frontier.next_offset(0), Some(10));
}

#[test]
fn classify_then_observe_is_deterministic_at_maximum_offset() {
    let maximum = decant_ir::MAX_KAFKA_OFFSET_V0;
    let mut frontier =
        KafkaPartitionFrontierV0::new(0, contract(), &[KafkaPositionV0::new(0, maximum).unwrap()])
            .unwrap();
    let (source, cluster, topic) = observed();
    let position = KafkaPositionV0::new(0, maximum).unwrap();
    assert_eq!(
        frontier.classify(&source, &cluster, topic, position),
        Ok(KafkaOffsetClassificationV0::Expected)
    );
    assert_eq!(frontier.next_offset(0), Some(maximum));
    assert_eq!(
        frontier.observe(&source, &cluster, topic, position),
        Ok(KafkaOffsetObservationV0::Advanced)
    );
    assert_eq!(frontier.next_offset(0), Some(maximum + 1));
    assert_eq!(
        frontier.classify(&source, &cluster, topic, position),
        Ok(KafkaOffsetClassificationV0::BeforeFrontier)
    );
}

#[test]
fn classification_diagnostics_hide_identity_and_coordinates() {
    let frontier =
        KafkaPartitionFrontierV0::new(0, contract(), &[KafkaPositionV0::new(0, 10).unwrap()])
            .unwrap();
    let bad_source = SourceIdV0::new("secret-source").unwrap();
    let (_, cluster, topic) = observed();
    let error = frontier
        .classify(
            &bad_source,
            &cluster,
            topic,
            KafkaPositionV0::new(7, 777).unwrap(),
        )
        .unwrap_err();
    let diagnostic = format!("{error:?}");
    for probe in ["secret-source", "cluster-v0", "777"] {
        assert!(!diagnostic.contains(probe));
    }
}
