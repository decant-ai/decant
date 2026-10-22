use std::any::Any;

use decant_ir::SourceIdV0;
use decant_kafka::{
    KAFKA_SOURCE_CONTRACT_VERSION_V0, KAFKA_TOPIC_ID_BYTES_V0, KafkaClusterIdV0,
    KafkaSourceContractErrorV0, KafkaSourceContractV0, KafkaSourceObservationErrorV0,
    KafkaTopicIdV0, MAX_KAFKA_CLUSTER_ID_BYTES_V0,
};

fn source(value: &str) -> SourceIdV0 {
    SourceIdV0::new(value).unwrap()
}

fn cluster(value: &str) -> KafkaClusterIdV0 {
    KafkaClusterIdV0::new(value).unwrap()
}

fn topic(byte: u8) -> KafkaTopicIdV0 {
    KafkaTopicIdV0::from_bytes([byte; KAFKA_TOPIC_ID_BYTES_V0]).unwrap()
}

fn contract() -> KafkaSourceContractV0 {
    KafkaSourceContractV0::new(
        KAFKA_SOURCE_CONTRACT_VERSION_V0,
        source("payments-kafka-v0"),
        cluster("cluster-incarnation-v0"),
        topic(0x5a),
    )
    .unwrap()
}

#[test]
fn cluster_id_boundaries_and_syntax_are_exact() {
    assert_eq!(KafkaClusterIdV0::new("a").unwrap().as_str(), "a");
    let maximum = "z".repeat(MAX_KAFKA_CLUSTER_ID_BYTES_V0);
    assert_eq!(KafkaClusterIdV0::new(&maximum).unwrap().as_str(), maximum);

    assert_eq!(
        KafkaClusterIdV0::new(""),
        Err(KafkaSourceContractErrorV0::InvalidClusterIdLength {
            minimum: 1,
            maximum: MAX_KAFKA_CLUSTER_ID_BYTES_V0,
            actual: 0,
        })
    );
    assert_eq!(
        KafkaClusterIdV0::new(&"z".repeat(MAX_KAFKA_CLUSTER_ID_BYTES_V0 + 1)),
        Err(KafkaSourceContractErrorV0::InvalidClusterIdLength {
            minimum: 1,
            maximum: MAX_KAFKA_CLUSTER_ID_BYTES_V0,
            actual: MAX_KAFKA_CLUSTER_ID_BYTES_V0 + 1,
        })
    );
    for invalid in ["has space", "has\nline", "naïve"] {
        assert_eq!(
            KafkaClusterIdV0::new(invalid),
            Err(KafkaSourceContractErrorV0::InvalidClusterIdSyntax)
        );
        assert!(!format!("{:?}", KafkaClusterIdV0::new(invalid)).contains(invalid));
    }
}

#[test]
fn topic_id_and_contract_versions_are_exact() {
    assert_eq!(
        KafkaTopicIdV0::from_bytes([0; KAFKA_TOPIC_ID_BYTES_V0]),
        Err(KafkaSourceContractErrorV0::UnknownTopicId)
    );
    let one = KafkaTopicIdV0::from_bytes([0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1]).unwrap();
    assert_eq!(one.as_bytes()[KAFKA_TOPIC_ID_BYTES_V0 - 1], 1);
    assert_eq!(topic(0xff).as_bytes(), &[0xff; KAFKA_TOPIC_ID_BYTES_V0]);

    let valid = contract();
    assert_eq!(valid.version(), KAFKA_SOURCE_CONTRACT_VERSION_V0);
    assert_eq!(valid.source_id(), &source("payments-kafka-v0"));
    assert_eq!(valid.cluster_id(), &cluster("cluster-incarnation-v0"));
    assert_eq!(valid.topic_id(), topic(0x5a));
    for invalid_version in [1, u16::MAX] {
        assert_eq!(
            KafkaSourceContractV0::new(
                invalid_version,
                source("payments-kafka-v0"),
                cluster("cluster-incarnation-v0"),
                topic(0x5a),
            ),
            Err(KafkaSourceContractErrorV0::UnsupportedVersion {
                actual: invalid_version,
            })
        );
    }
}

#[test]
fn observed_identity_mismatch_precedence_is_fail_closed() {
    let configured = contract();
    assert_eq!(
        configured.validate_observed(
            &source("payments-kafka-v0"),
            &cluster("cluster-incarnation-v0"),
            topic(0x5a),
        ),
        Ok(())
    );

    for (observed_source, observed_cluster, observed_topic, expected) in [
        (
            source("other-kafka-v0"),
            cluster("other-cluster-v0"),
            topic(0xa5),
            KafkaSourceObservationErrorV0::SourceMismatch,
        ),
        (
            source("payments-kafka-v0"),
            cluster("other-cluster-v0"),
            topic(0xa5),
            KafkaSourceObservationErrorV0::ClusterMismatch,
        ),
        (
            source("payments-kafka-v0"),
            cluster("cluster-incarnation-v0"),
            topic(0xa5),
            KafkaSourceObservationErrorV0::TopicIncarnationMismatch,
        ),
    ] {
        assert_eq!(
            configured.validate_observed(&observed_source, &observed_cluster, observed_topic,),
            Err(expected)
        );
    }
}

#[test]
fn diagnostics_and_assertion_panics_do_not_expose_identity_values() {
    let configured = contract();
    let diagnostic = format!(
        "{configured:?} {:?} {:?}",
        configured.validate_observed(
            &source("source-secret-v0"),
            &cluster("cluster-secret-v0"),
            topic(0xa5),
        ),
        KafkaSourceContractErrorV0::UnknownTopicId,
    );
    assert_safe(&diagnostic);

    let different = KafkaSourceContractV0::new(
        0,
        source("source-secret-v0"),
        cluster("cluster-secret-v0"),
        topic(0xa5),
    )
    .unwrap();
    let panic = std::panic::catch_unwind(|| assert_eq!(configured, different)).unwrap_err();
    assert_safe(&panic_text(panic));
}

#[test]
fn repeated_construction_and_validation_are_identical() {
    let expected = contract();
    for _ in 0..128 {
        let candidate = contract();
        assert_eq!(candidate, expected);
        assert_eq!(
            candidate.validate_observed(
                &source("payments-kafka-v0"),
                &cluster("cluster-incarnation-v0"),
                topic(0x5a),
            ),
            Ok(())
        );
    }
}

fn assert_safe(diagnostic: &str) {
    for secret in [
        "payments-kafka-v0",
        "source-secret-v0",
        "cluster-incarnation-v0",
        "cluster-secret-v0",
        "5a5a5a5a",
        "a5a5a5a5",
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
