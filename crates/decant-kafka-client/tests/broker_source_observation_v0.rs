use decant_ir::SourceIdV0;
use decant_kafka::{
    KAFKA_SOURCE_CONTRACT_VERSION_V0, KafkaClusterIdV0, KafkaSourceContractV0,
    KafkaSourceObservationErrorV0, KafkaTopicIdV0,
};
use decant_kafka_client::{
    KafkaBrokerSourceObservationErrorV0, validate_kafka_source_observation_v0,
};

fn source(value: &str) -> SourceIdV0 {
    SourceIdV0::new(value).unwrap()
}

fn cluster(value: &str) -> KafkaClusterIdV0 {
    KafkaClusterIdV0::new(value).unwrap()
}

fn topic(fill: u8) -> KafkaTopicIdV0 {
    KafkaTopicIdV0::from_bytes([fill; 16]).unwrap()
}

fn contract() -> KafkaSourceContractV0 {
    KafkaSourceContractV0::new(
        KAFKA_SOURCE_CONTRACT_VERSION_V0,
        source("expected-source"),
        cluster("expected-cluster"),
        topic(1),
    )
    .unwrap()
}

#[test]
fn exact_observation_is_retained_but_debug_is_redacted() {
    let observation = validate_kafka_source_observation_v0(
        &contract(),
        &source("expected-source"),
        cluster("expected-cluster"),
        topic(1),
    )
    .unwrap();
    assert_eq!(observation.cluster_id(), &cluster("expected-cluster"));
    assert_eq!(observation.topic_id(), topic(1));
    let debug = format!("{observation:?}");
    assert!(!debug.contains("expected"));
    assert!(!debug.contains("0101"));
}

#[test]
fn mismatch_precedence_is_source_then_cluster_then_topic() {
    let vectors = [
        (
            source("other-source"),
            cluster("other-cluster"),
            topic(2),
            KafkaSourceObservationErrorV0::SourceMismatch,
        ),
        (
            source("expected-source"),
            cluster("other-cluster"),
            topic(2),
            KafkaSourceObservationErrorV0::ClusterMismatch,
        ),
        (
            source("expected-source"),
            cluster("expected-cluster"),
            topic(2),
            KafkaSourceObservationErrorV0::TopicIncarnationMismatch,
        ),
    ];
    for (observed_source, observed_cluster, observed_topic, expected) in vectors {
        assert_eq!(
            validate_kafka_source_observation_v0(
                &contract(),
                &observed_source,
                observed_cluster,
                observed_topic,
            ),
            Err(KafkaBrokerSourceObservationErrorV0::SourceObservation(
                expected
            ))
        );
    }
}

#[test]
fn every_public_error_category_is_value_safe() {
    let errors = [
        KafkaBrokerSourceObservationErrorV0::InvalidTopicName,
        KafkaBrokerSourceObservationErrorV0::InvalidTimeout,
        KafkaBrokerSourceObservationErrorV0::ClusterIdUnavailable,
        KafkaBrokerSourceObservationErrorV0::SourceObservation(
            KafkaSourceObservationErrorV0::TopicIncarnationMismatch,
        ),
    ];
    for error in errors {
        let diagnostic = format!("{error:?} {error}");
        assert!(!diagnostic.contains("expected-source"));
        assert!(!diagnostic.contains("expected-cluster"));
        assert!(!diagnostic.contains("private-topic"));
    }
}
