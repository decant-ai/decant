use decant_ir::SourceIdV0;
use decant_kafka::{
    KAFKA_PARTITION_FRONTIER_VERSION_V0, KAFKA_SOURCE_CONTRACT_VERSION_V0, KafkaClusterIdV0,
    KafkaPartitionFrontierV0, KafkaSourceContractV0, KafkaTopicIdV0,
    MAX_KAFKA_ASSIGNED_PARTITIONS_V0,
};
use decant_kafka_client::{
    KafkaSaslScramMechanismV0, KafkaSecretEnvironmentReferenceV0, KafkaStaticConsumerErrorV0,
    KafkaStaticConsumerV0, KafkaStaticPollErrorV0, KafkaStaticSecurityConfigV0,
    KafkaStaticSourceConfigErrorV0, KafkaStaticSourceConfigV0, KafkaTlsTrustV0,
    kafka_static_client_config_v0,
};
use rdkafka::consumer::Consumer;
use rdkafka::topic_partition_list::Offset;
use std::time::Duration;

fn contract() -> KafkaSourceContractV0 {
    KafkaSourceContractV0::new(
        KAFKA_SOURCE_CONTRACT_VERSION_V0,
        SourceIdV0::new("static-source").unwrap(),
        KafkaClusterIdV0::new("static-cluster").unwrap(),
        KafkaTopicIdV0::from_bytes([3; 16]).unwrap(),
    )
    .unwrap()
}

fn config(starts: &[(i32, i64)]) -> KafkaStaticSourceConfigV0 {
    KafkaStaticSourceConfigV0::new("127.0.0.1:9092", "decant-events", contract(), starts).unwrap()
}

#[test]
fn configuration_boundaries_are_exact_and_value_safe() {
    assert_eq!(
        KafkaStaticSourceConfigV0::new("", "decant-events", contract(), &[(0, 0)]),
        Err(KafkaStaticSourceConfigErrorV0::InvalidBootstrapServers)
    );
    assert_eq!(
        KafkaStaticSourceConfigV0::new("user@host:9092", "decant-events", contract(), &[(0, 0)]),
        Err(KafkaStaticSourceConfigErrorV0::InvalidBootstrapServers)
    );
    assert_eq!(
        KafkaStaticSourceConfigV0::new("host:9092", "..", contract(), &[(0, 0)]),
        Err(KafkaStaticSourceConfigErrorV0::InvalidTopicName)
    );
    assert!(
        KafkaStaticSourceConfigV0::new(&"h".repeat(1_024), &"t".repeat(249), contract(), &[(0, 0)])
            .is_ok()
    );
    assert_eq!(
        KafkaStaticSourceConfigV0::new(&"h".repeat(1_025), "decant-events", contract(), &[(0, 0)]),
        Err(KafkaStaticSourceConfigErrorV0::InvalidBootstrapServers)
    );
    assert_eq!(
        KafkaStaticSourceConfigV0::new("host:9092", &"t".repeat(250), contract(), &[(0, 0)]),
        Err(KafkaStaticSourceConfigErrorV0::InvalidTopicName)
    );
    assert_eq!(
        KafkaStaticSourceConfigV0::new("host:9092", "decant-events", contract(), &[]),
        Err(KafkaStaticSourceConfigErrorV0::EmptyAssignment)
    );
    assert_eq!(
        KafkaStaticSourceConfigV0::new("host:9092", "decant-events", contract(), &[(1, 0), (0, 0)],),
        Err(KafkaStaticSourceConfigErrorV0::UnsortedOrDuplicatePartition)
    );
    let starts = vec![(0, 0); MAX_KAFKA_ASSIGNED_PARTITIONS_V0 + 1];
    assert_eq!(
        KafkaStaticSourceConfigV0::new("host:9092", "decant-events", contract(), &starts),
        Err(KafkaStaticSourceConfigErrorV0::TooManyPartitions {
            maximum: MAX_KAFKA_ASSIGNED_PARTITIONS_V0,
            actual: MAX_KAFKA_ASSIGNED_PARTITIONS_V0 + 1,
        })
    );
    let debug = format!("{:?}", config(&[(0, 0)]));
    assert!(!debug.contains("127.0.0.1"));
    assert!(!debug.contains("decant-events"));
    assert!(!debug.contains("static-source"));
}

#[test]
fn native_automation_security_and_queue_bounds_are_frozen() {
    let config = config(&[(0, 0)]);
    let native = kafka_static_client_config_v0(&config);
    let expected = [
        ("security.protocol", "plaintext"),
        ("enable.auto.commit", "false"),
        ("enable.auto.offset.store", "false"),
        ("allow.auto.create.topics", "false"),
        ("auto.offset.reset", "error"),
        ("enable.partition.eof", "true"),
        ("queued.max.messages.kbytes", "1024"),
        ("fetch.max.bytes", "1048576"),
        ("max.partition.fetch.bytes", "131072"),
        ("receive.message.max.bytes", "1049088"),
        ("statistics.interval.ms", "0"),
    ];
    for (key, value) in expected {
        assert_eq!(native.get(key), Some(value));
    }
}

#[test]
fn assignment_requires_exact_source_contract_and_partition_set() {
    let config = config(&[(0, 4), (2, 9)]);
    let consumer = KafkaStaticConsumerV0::new(config.clone()).unwrap();
    let exact = KafkaPartitionFrontierV0::new(
        KAFKA_PARTITION_FRONTIER_VERSION_V0,
        contract(),
        config.starting_positions(),
    )
    .unwrap();
    consumer.assign_frontier(&exact).unwrap();
    let assignment = consumer.native_consumer().assignment().unwrap();
    assert_eq!(assignment.count(), 2);
    assert_eq!(
        assignment
            .find_partition("decant-events", 0)
            .unwrap()
            .offset(),
        Offset::Offset(4)
    );
    assert_eq!(
        assignment
            .find_partition("decant-events", 2)
            .unwrap()
            .offset(),
        Offset::Offset(9)
    );

    let missing = KafkaPartitionFrontierV0::new(
        KAFKA_PARTITION_FRONTIER_VERSION_V0,
        contract(),
        &[config.starting_positions()[0]],
    )
    .unwrap();
    assert_eq!(
        consumer.assign_frontier(&missing),
        Err(KafkaStaticConsumerErrorV0::AssignmentMismatch)
    );

    let other_contract = KafkaSourceContractV0::new(
        0,
        SourceIdV0::new("other-source").unwrap(),
        KafkaClusterIdV0::new("static-cluster").unwrap(),
        KafkaTopicIdV0::from_bytes([3; 16]).unwrap(),
    )
    .unwrap();
    let other =
        KafkaPartitionFrontierV0::new(0, other_contract, config.starting_positions()).unwrap();
    assert_eq!(
        consumer.assign_frontier(&other),
        Err(KafkaStaticConsumerErrorV0::SourceContractMismatch)
    );
}

#[test]
fn poll_timeout_is_positive_and_exactly_bounded() {
    let consumer = KafkaStaticConsumerV0::new(config(&[(0, 0)])).unwrap();
    assert_eq!(
        consumer.poll(Duration::ZERO),
        Err(KafkaStaticPollErrorV0::InvalidTimeout)
    );
    assert_eq!(
        consumer.poll(Duration::from_millis(10_000) + Duration::from_nanos(1)),
        Err(KafkaStaticPollErrorV0::InvalidTimeout)
    );
}

#[test]
fn tls_and_tls_scram_clients_construct_without_network_or_secret_disclosure() {
    let source = config(&[(0, 0)]);
    let tls = KafkaStaticSecurityConfigV0::tls(0, KafkaTlsTrustV0::system_roots()).unwrap();
    let tls_consumer =
        KafkaStaticConsumerV0::new_with_security_resolver(source.clone(), tls, |_| None).unwrap();
    assert_eq!(
        tls_consumer.security().mode(),
        decant_kafka_client::KafkaStaticSecurityModeV0::Tls
    );

    let sasl = KafkaStaticSecurityConfigV0::tls_sasl_scram(
        0,
        KafkaTlsTrustV0::system_roots(),
        KafkaSaslScramMechanismV0::Sha256,
        KafkaSecretEnvironmentReferenceV0::new("DECANT_KAFKA_USER").unwrap(),
        KafkaSecretEnvironmentReferenceV0::new("DECANT_KAFKA_PASSWORD").unwrap(),
    )
    .unwrap();
    let sasl_consumer = KafkaStaticConsumerV0::new_with_security_resolver(source, sasl, |name| {
        Some(format!("synthetic-{name}"))
    })
    .unwrap();
    let debug = format!("{sasl_consumer:?}");
    assert!(!debug.contains("DECANT_KAFKA"));
    assert!(!debug.contains("synthetic-"));
}
