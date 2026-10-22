use std::error::Error;
use std::fmt;
use std::time::Duration;

use decant_ir::{KafkaPositionV0, ValidationErrorV0};
use decant_kafka::{
    KafkaFetchedRecordV0, KafkaPartitionFrontierV0, KafkaSourceContractV0,
    MAX_KAFKA_ASSIGNED_PARTITIONS_V0,
};
use rdkafka::ClientConfig;
use rdkafka::consumer::{BaseConsumer, Consumer};
use rdkafka::error::KafkaError;
use rdkafka::topic_partition_list::{Offset, TopicPartitionList};

use crate::{
    KAFKA_SECURITY_MAX_SECRET_VALUE_BYTES_V0, KafkaRecordAdapterErrorV0,
    KafkaStaticSecurityConfigV0, convert_kafka_message_v0,
};

pub const KAFKA_STATIC_CONSUMER_MAX_BOOTSTRAP_SERVERS_BYTES_V0: usize = 1_024;
pub const KAFKA_STATIC_CONSUMER_MAX_TOPIC_NAME_BYTES_V0: usize = 249;
pub const KAFKA_STATIC_CONSUMER_MAX_POLL_TIMEOUT_MS_V0: u64 = 10_000;

const KAFKA_NATIVE_QUEUED_MAX_MESSAGES_KBYTES_V0: &str = "1024";
const KAFKA_NATIVE_FETCH_MAX_BYTES_V0: &str = "1048576";
const KAFKA_NATIVE_MAX_PARTITION_FETCH_BYTES_V0: &str = "131072";
const KAFKA_NATIVE_RECEIVE_MESSAGE_MAX_BYTES_V0: &str = "1049088";

#[derive(Clone, Eq, PartialEq)]
pub enum KafkaStaticSourceConfigErrorV0 {
    InvalidBootstrapServers,
    InvalidTopicName,
    EmptyAssignment,
    TooManyPartitions { maximum: usize, actual: usize },
    UnsortedOrDuplicatePartition,
    InvalidPosition(ValidationErrorV0),
}

impl fmt::Debug for KafkaStaticSourceConfigErrorV0 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidBootstrapServers => formatter.write_str("InvalidBootstrapServers"),
            Self::InvalidTopicName => formatter.write_str("InvalidTopicName"),
            Self::EmptyAssignment => formatter.write_str("EmptyAssignment"),
            Self::TooManyPartitions { maximum, actual } => formatter
                .debug_struct("TooManyPartitions")
                .field("maximum", maximum)
                .field("actual", actual)
                .finish(),
            Self::UnsortedOrDuplicatePartition => {
                formatter.write_str("UnsortedOrDuplicatePartition")
            }
            Self::InvalidPosition(error) => formatter
                .debug_tuple("InvalidPosition")
                .field(error)
                .finish(),
        }
    }
}

impl fmt::Display for KafkaStaticSourceConfigErrorV0 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(self, formatter)
    }
}

impl Error for KafkaStaticSourceConfigErrorV0 {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::InvalidPosition(error) => Some(error),
            _ => None,
        }
    }
}

#[derive(Clone, Eq, PartialEq)]
pub struct KafkaStaticSourceConfigV0 {
    bootstrap_servers: String,
    topic_name: String,
    source_contract: KafkaSourceContractV0,
    starting_positions: Vec<KafkaPositionV0>,
}

impl KafkaStaticSourceConfigV0 {
    pub fn new(
        bootstrap_servers: &str,
        topic_name: &str,
        source_contract: KafkaSourceContractV0,
        starting_positions: &[(i32, i64)],
    ) -> Result<Self, KafkaStaticSourceConfigErrorV0> {
        validate_bootstrap_servers(bootstrap_servers)?;
        validate_topic_name(topic_name)?;
        if starting_positions.is_empty() {
            return Err(KafkaStaticSourceConfigErrorV0::EmptyAssignment);
        }
        if starting_positions.len() > MAX_KAFKA_ASSIGNED_PARTITIONS_V0 {
            return Err(KafkaStaticSourceConfigErrorV0::TooManyPartitions {
                maximum: MAX_KAFKA_ASSIGNED_PARTITIONS_V0,
                actual: starting_positions.len(),
            });
        }
        let mut positions = Vec::new();
        positions
            .try_reserve_exact(starting_positions.len())
            .map_err(|_| KafkaStaticSourceConfigErrorV0::TooManyPartitions {
                maximum: MAX_KAFKA_ASSIGNED_PARTITIONS_V0,
                actual: starting_positions.len(),
            })?;
        let mut previous_partition = None;
        for &(partition, offset) in starting_positions {
            if previous_partition.is_some_and(|previous| partition <= previous) {
                return Err(KafkaStaticSourceConfigErrorV0::UnsortedOrDuplicatePartition);
            }
            positions.push(
                KafkaPositionV0::new(partition, offset)
                    .map_err(KafkaStaticSourceConfigErrorV0::InvalidPosition)?,
            );
            previous_partition = Some(partition);
        }
        Ok(Self {
            bootstrap_servers: bootstrap_servers.to_owned(),
            topic_name: topic_name.to_owned(),
            source_contract,
            starting_positions: positions,
        })
    }

    pub fn bootstrap_servers(&self) -> &str {
        &self.bootstrap_servers
    }

    pub fn topic_name(&self) -> &str {
        &self.topic_name
    }

    pub const fn source_contract(&self) -> &KafkaSourceContractV0 {
        &self.source_contract
    }

    pub fn starting_positions(&self) -> &[KafkaPositionV0] {
        &self.starting_positions
    }

    pub fn contains_partition(&self, partition: i32) -> bool {
        self.starting_positions
            .binary_search_by_key(&partition, |position| position.partition())
            .is_ok()
    }
}

impl fmt::Debug for KafkaStaticSourceConfigV0 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("KafkaStaticSourceConfigV0")
            .field("bootstrap_servers", &"<redacted>")
            .field("topic_name", &"<redacted>")
            .field("source_contract", &"<redacted>")
            .field("assignment_count", &self.starting_positions.len())
            .finish()
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum KafkaStaticPollErrorV0 {
    InvalidTimeout,
    BrokerFailure,
    Record,
}

impl fmt::Display for KafkaStaticPollErrorV0 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(self, formatter)
    }
}

impl Error for KafkaStaticPollErrorV0 {}

#[derive(Clone, Eq, PartialEq)]
pub enum KafkaStaticPollV0 {
    Data(KafkaFetchedRecordV0),
    Idle,
    PartitionEof { partition: i32 },
}

impl fmt::Debug for KafkaStaticPollV0 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Data(record) => formatter.debug_tuple("Data").field(record).finish(),
            Self::Idle => formatter.write_str("Idle"),
            Self::PartitionEof { partition } => formatter
                .debug_struct("PartitionEof")
                .field("partition", partition)
                .finish(),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum KafkaStaticConsumerErrorV0 {
    SecretUnavailable,
    SecretInvalid,
    ClientCreation,
    SourceContractMismatch,
    AssignmentMismatch,
    AssignmentBuild,
    AssignmentActivation,
}

impl fmt::Display for KafkaStaticConsumerErrorV0 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(self, formatter)
    }
}

impl Error for KafkaStaticConsumerErrorV0 {}

pub struct KafkaStaticConsumerV0 {
    consumer: BaseConsumer,
    config: KafkaStaticSourceConfigV0,
    security: KafkaStaticSecurityConfigV0,
}

impl KafkaStaticConsumerV0 {
    pub fn new(config: KafkaStaticSourceConfigV0) -> Result<Self, KafkaStaticConsumerErrorV0> {
        Self::new_with_security_resolver(config, KafkaStaticSecurityConfigV0::Plaintext, |_| None)
    }

    pub fn new_with_security_resolver<F>(
        config: KafkaStaticSourceConfigV0,
        security: KafkaStaticSecurityConfigV0,
        resolver: F,
    ) -> Result<Self, KafkaStaticConsumerErrorV0>
    where
        F: FnMut(&str) -> Option<String>,
    {
        let consumer = kafka_static_client_config_with_security_v0(&config, &security, resolver)?
            .create()
            .map_err(|_| KafkaStaticConsumerErrorV0::ClientCreation)?;
        Ok(Self {
            consumer,
            config,
            security,
        })
    }

    pub const fn config(&self) -> &KafkaStaticSourceConfigV0 {
        &self.config
    }

    pub const fn security(&self) -> &KafkaStaticSecurityConfigV0 {
        &self.security
    }

    pub const fn native_consumer(&self) -> &BaseConsumer {
        &self.consumer
    }

    pub fn assign_frontier(
        &self,
        frontier: &KafkaPartitionFrontierV0,
    ) -> Result<(), KafkaStaticConsumerErrorV0> {
        if frontier.source_contract() != self.config.source_contract() {
            return Err(KafkaStaticConsumerErrorV0::SourceContractMismatch);
        }
        if frontier.assignment_count() != self.config.starting_positions.len()
            || !frontier
                .iter()
                .all(|(partition, _)| self.config.contains_partition(partition))
        {
            return Err(KafkaStaticConsumerErrorV0::AssignmentMismatch);
        }
        let mut assignment = TopicPartitionList::with_capacity(frontier.assignment_count());
        for (partition, next_offset) in frontier.iter() {
            assignment
                .add_partition_offset(
                    self.config.topic_name(),
                    partition,
                    Offset::Offset(next_offset),
                )
                .map_err(|_| KafkaStaticConsumerErrorV0::AssignmentBuild)?;
        }
        self.consumer
            .assign(&assignment)
            .map_err(|_| KafkaStaticConsumerErrorV0::AssignmentActivation)
    }

    pub fn poll(&self, timeout: Duration) -> Result<KafkaStaticPollV0, KafkaStaticPollErrorV0> {
        validate_poll_timeout(timeout)?;
        match self.consumer.poll(timeout) {
            None => Ok(KafkaStaticPollV0::Idle),
            Some(Err(error)) => classify_native_poll_error_v0(error),
            Some(Ok(message)) => {
                convert_kafka_message_v0(&message, self.config.topic_name(), |p| {
                    self.config.contains_partition(p)
                })
                .map(KafkaStaticPollV0::Data)
                .map_err(|_error: KafkaRecordAdapterErrorV0| KafkaStaticPollErrorV0::Record)
            }
        }
    }
}

fn classify_native_poll_error_v0(
    error: KafkaError,
) -> Result<KafkaStaticPollV0, KafkaStaticPollErrorV0> {
    match error {
        KafkaError::PartitionEOF(partition) => Ok(KafkaStaticPollV0::PartitionEof { partition }),
        _ => Err(KafkaStaticPollErrorV0::BrokerFailure),
    }
}

impl fmt::Debug for KafkaStaticConsumerV0 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("KafkaStaticConsumerV0")
            .field("config", &self.config)
            .finish_non_exhaustive()
    }
}

pub fn kafka_static_client_config_v0(config: &KafkaStaticSourceConfigV0) -> ClientConfig {
    let mut client = kafka_static_base_client_config_v0(config);
    client.set("security.protocol", "plaintext");
    client
}

fn kafka_static_client_config_with_security_v0<F>(
    config: &KafkaStaticSourceConfigV0,
    security: &KafkaStaticSecurityConfigV0,
    mut resolver: F,
) -> Result<ClientConfig, KafkaStaticConsumerErrorV0>
where
    F: FnMut(&str) -> Option<String>,
{
    let mut client = kafka_static_base_client_config_v0(config);
    apply_security_v0(&mut client, security, &mut resolver)?;
    Ok(client)
}

fn kafka_static_base_client_config_v0(config: &KafkaStaticSourceConfigV0) -> ClientConfig {
    let mut client = ClientConfig::new();
    client
        .set("bootstrap.servers", config.bootstrap_servers())
        .set("group.id", "decant-static-v0")
        .set("client.id", "decant-static-v0")
        .set("enable.auto.commit", "false")
        .set("enable.auto.offset.store", "false")
        .set("allow.auto.create.topics", "false")
        .set("auto.offset.reset", "error")
        .set("enable.partition.eof", "true")
        .set(
            "queued.max.messages.kbytes",
            KAFKA_NATIVE_QUEUED_MAX_MESSAGES_KBYTES_V0,
        )
        .set("fetch.max.bytes", KAFKA_NATIVE_FETCH_MAX_BYTES_V0)
        .set(
            "max.partition.fetch.bytes",
            KAFKA_NATIVE_MAX_PARTITION_FETCH_BYTES_V0,
        )
        .set(
            "receive.message.max.bytes",
            KAFKA_NATIVE_RECEIVE_MESSAGE_MAX_BYTES_V0,
        )
        .set("statistics.interval.ms", "0");
    client
}

fn apply_security_v0<F>(
    client: &mut ClientConfig,
    security: &KafkaStaticSecurityConfigV0,
    resolver: &mut F,
) -> Result<(), KafkaStaticConsumerErrorV0>
where
    F: FnMut(&str) -> Option<String>,
{
    match security {
        KafkaStaticSecurityConfigV0::Plaintext => {
            client.set("security.protocol", "plaintext");
        }
        KafkaStaticSecurityConfigV0::Tls { trust } => {
            apply_tls_v0(client, trust);
            client.set("security.protocol", "ssl");
        }
        KafkaStaticSecurityConfigV0::TlsSaslScram {
            trust,
            mechanism,
            username,
            password,
        } => {
            let username = resolve_secret_v0(username.name(), resolver)?;
            let password = resolve_secret_v0(password.name(), resolver)?;
            apply_tls_v0(client, trust);
            client
                .set("security.protocol", "sasl_ssl")
                .set("sasl.mechanism", mechanism.native_name())
                .set("sasl.username", username)
                .set("sasl.password", password);
        }
    }
    Ok(())
}

fn apply_tls_v0(client: &mut ClientConfig, trust: &crate::KafkaTlsTrustV0) {
    client
        .set("enable.ssl.certificate.verification", "true")
        .set("ssl.endpoint.identification.algorithm", "https");
    if let Some(location) = trust.ca_location() {
        client.set("ssl.ca.location", location);
    }
}

fn resolve_secret_v0<F>(
    reference: &str,
    resolver: &mut F,
) -> Result<String, KafkaStaticConsumerErrorV0>
where
    F: FnMut(&str) -> Option<String>,
{
    let value = resolver(reference).ok_or(KafkaStaticConsumerErrorV0::SecretUnavailable)?;
    if value.is_empty()
        || value.len() > KAFKA_SECURITY_MAX_SECRET_VALUE_BYTES_V0
        || value.as_bytes().contains(&0)
    {
        return Err(KafkaStaticConsumerErrorV0::SecretInvalid);
    }
    Ok(value)
}

fn validate_bootstrap_servers(value: &str) -> Result<(), KafkaStaticSourceConfigErrorV0> {
    if value.is_empty()
        || value.len() > KAFKA_STATIC_CONSUMER_MAX_BOOTSTRAP_SERVERS_BYTES_V0
        || !value.bytes().all(|byte| byte.is_ascii_graphic())
        || value.contains('@')
        || value.contains('/')
        || value.contains('\\')
        || value.split(',').any(str::is_empty)
    {
        return Err(KafkaStaticSourceConfigErrorV0::InvalidBootstrapServers);
    }
    Ok(())
}

fn validate_topic_name(value: &str) -> Result<(), KafkaStaticSourceConfigErrorV0> {
    if value.is_empty()
        || value.len() > KAFKA_STATIC_CONSUMER_MAX_TOPIC_NAME_BYTES_V0
        || matches!(value, "." | "..")
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'-'))
    {
        return Err(KafkaStaticSourceConfigErrorV0::InvalidTopicName);
    }
    Ok(())
}

fn validate_poll_timeout(timeout: Duration) -> Result<(), KafkaStaticPollErrorV0> {
    if timeout.is_zero()
        || timeout > Duration::from_millis(KAFKA_STATIC_CONSUMER_MAX_POLL_TIMEOUT_MS_V0)
    {
        return Err(KafkaStaticPollErrorV0::InvalidTimeout);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{KafkaSaslScramMechanismV0, KafkaSecretEnvironmentReferenceV0, KafkaTlsTrustV0};

    #[test]
    fn native_eof_and_broker_failures_map_to_value_safe_categories() {
        assert_eq!(
            classify_native_poll_error_v0(KafkaError::PartitionEOF(7)),
            Ok(KafkaStaticPollV0::PartitionEof { partition: 7 })
        );
        let error = classify_native_poll_error_v0(KafkaError::ClientCreation(
            "synthetic-private-broker-detail".to_owned(),
        ))
        .unwrap_err();
        assert_eq!(error, KafkaStaticPollErrorV0::BrokerFailure);
        assert!(!format!("{error:?}").contains("synthetic-private-broker-detail"));
    }

    #[test]
    fn tls_and_scram_native_configuration_is_exact_and_secret_failures_are_safe() {
        let source = KafkaStaticSourceConfigV0::new(
            "broker.invalid:9093",
            "private-topic",
            decant_kafka::KafkaSourceContractV0::new(
                0,
                decant_ir::SourceIdV0::new("private-source").unwrap(),
                decant_kafka::KafkaClusterIdV0::new("private-cluster").unwrap(),
                decant_kafka::KafkaTopicIdV0::from_bytes([7; 16]).unwrap(),
            )
            .unwrap(),
            &[(0, 0)],
        )
        .unwrap();
        let tls = KafkaStaticSecurityConfigV0::tls(
            0,
            KafkaTlsTrustV0::absolute_ca_location("/private/ca.pem").unwrap(),
        )
        .unwrap();
        let native = kafka_static_client_config_with_security_v0(&source, &tls, |_| None).unwrap();
        assert_eq!(native.get("security.protocol"), Some("ssl"));
        assert_eq!(
            native.get("enable.ssl.certificate.verification"),
            Some("true")
        );
        assert_eq!(
            native.get("ssl.endpoint.identification.algorithm"),
            Some("https")
        );
        assert_eq!(native.get("ssl.ca.location"), Some("/private/ca.pem"));

        let sasl = KafkaStaticSecurityConfigV0::tls_sasl_scram(
            0,
            KafkaTlsTrustV0::system_roots(),
            KafkaSaslScramMechanismV0::Sha512,
            KafkaSecretEnvironmentReferenceV0::new("DECANT_KAFKA_USER").unwrap(),
            KafkaSecretEnvironmentReferenceV0::new("DECANT_KAFKA_PASSWORD").unwrap(),
        )
        .unwrap();
        let mut resolved = Vec::new();
        let native = kafka_static_client_config_with_security_v0(&source, &sasl, |name| {
            resolved.push(name.to_owned());
            match name {
                "DECANT_KAFKA_USER" => Some("private-user".to_owned()),
                "DECANT_KAFKA_PASSWORD" => Some("private-password".to_owned()),
                _ => None,
            }
        })
        .unwrap();
        assert_eq!(resolved, ["DECANT_KAFKA_USER", "DECANT_KAFKA_PASSWORD"]);
        assert_eq!(native.get("security.protocol"), Some("sasl_ssl"));
        assert_eq!(native.get("sasl.mechanism"), Some("SCRAM-SHA-512"));
        assert_eq!(native.get("sasl.username"), Some("private-user"));
        assert_eq!(native.get("sasl.password"), Some("private-password"));

        assert_eq!(
            kafka_static_client_config_with_security_v0(&source, &sasl, |_| None).unwrap_err(),
            KafkaStaticConsumerErrorV0::SecretUnavailable
        );
        assert_eq!(
            kafka_static_client_config_with_security_v0(&source, &sasl, |_| Some(String::new()))
                .unwrap_err(),
            KafkaStaticConsumerErrorV0::SecretInvalid
        );
        for invalid in [
            "x".repeat(KAFKA_SECURITY_MAX_SECRET_VALUE_BYTES_V0 + 1),
            "synthetic\0secret".to_owned(),
        ] {
            assert_eq!(
                kafka_static_client_config_with_security_v0(&source, &sasl, |_| {
                    Some(invalid.clone())
                })
                .unwrap_err(),
                KafkaStaticConsumerErrorV0::SecretInvalid
            );
        }
        assert!(
            kafka_static_client_config_with_security_v0(&source, &sasl, |_| Some(
                "x".repeat(KAFKA_SECURITY_MAX_SECRET_VALUE_BYTES_V0)
            ))
            .is_ok()
        );
        for diagnostic in [
            format!("{sasl:?}"),
            format!("{:?}", KafkaStaticConsumerErrorV0::SecretUnavailable),
        ] {
            for secret in [
                "DECANT_KAFKA_USER",
                "DECANT_KAFKA_PASSWORD",
                "private-user",
                "private-password",
            ] {
                assert!(!diagnostic.contains(secret));
            }
        }
    }
}
