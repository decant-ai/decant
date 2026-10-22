use std::error::Error;
use std::fmt;
use std::time::Duration;

use decant_ir::SourceIdV0;
use decant_kafka::{
    KafkaClusterIdV0, KafkaSourceContractErrorV0, KafkaSourceContractV0,
    KafkaSourceObservationErrorV0, KafkaTopicIdV0,
};
use rdkafka::consumer::{BaseConsumer, Consumer, ConsumerContext};

use crate::topic_description_ffi::{KafkaTopicDescriptionFfiErrorV0, describe_kafka_topic_id_v0};

pub const KAFKA_BROKER_IDENTITY_MAX_TIMEOUT_MS_V0: u64 = 30_000;
const MAX_KAFKA_TOPIC_NAME_BYTES_V0: usize = 249;

#[derive(Clone, Eq, PartialEq)]
pub struct KafkaBrokerSourceObservationV0 {
    cluster_id: KafkaClusterIdV0,
    topic_id: KafkaTopicIdV0,
}

impl KafkaBrokerSourceObservationV0 {
    pub const fn cluster_id(&self) -> &KafkaClusterIdV0 {
        &self.cluster_id
    }

    pub const fn topic_id(&self) -> KafkaTopicIdV0 {
        self.topic_id
    }
}

impl fmt::Debug for KafkaBrokerSourceObservationV0 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("KafkaBrokerSourceObservationV0")
            .field("cluster_id", &"<redacted>")
            .field("topic_id", &"<redacted>")
            .finish()
    }
}

#[derive(Clone, Eq, PartialEq)]
pub enum KafkaBrokerSourceObservationErrorV0 {
    InvalidTopicName,
    InvalidTimeout,
    ClusterIdUnavailable,
    ClusterId(KafkaSourceContractErrorV0),
    TopicDescription(KafkaTopicDescriptionFfiErrorV0),
    SourceObservation(KafkaSourceObservationErrorV0),
}

impl fmt::Debug for KafkaBrokerSourceObservationErrorV0 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidTopicName => formatter.write_str("InvalidTopicName"),
            Self::InvalidTimeout => formatter.write_str("InvalidTimeout"),
            Self::ClusterIdUnavailable => formatter.write_str("ClusterIdUnavailable"),
            Self::ClusterId(error) => formatter.debug_tuple("ClusterId").field(error).finish(),
            Self::TopicDescription(error) => formatter
                .debug_tuple("TopicDescription")
                .field(error)
                .finish(),
            Self::SourceObservation(error) => formatter
                .debug_tuple("SourceObservation")
                .field(error)
                .finish(),
        }
    }
}

impl fmt::Display for KafkaBrokerSourceObservationErrorV0 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(self, formatter)
    }
}

impl Error for KafkaBrokerSourceObservationErrorV0 {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::ClusterId(error) => Some(error),
            Self::TopicDescription(error) => Some(error),
            Self::SourceObservation(error) => Some(error),
            Self::InvalidTopicName | Self::InvalidTimeout | Self::ClusterIdUnavailable => None,
        }
    }
}

pub fn validate_kafka_source_observation_v0(
    expected: &KafkaSourceContractV0,
    observed_source_id: &SourceIdV0,
    observed_cluster_id: KafkaClusterIdV0,
    observed_topic_id: KafkaTopicIdV0,
) -> Result<KafkaBrokerSourceObservationV0, KafkaBrokerSourceObservationErrorV0> {
    expected
        .validate_observed(observed_source_id, &observed_cluster_id, observed_topic_id)
        .map_err(KafkaBrokerSourceObservationErrorV0::SourceObservation)?;
    Ok(KafkaBrokerSourceObservationV0 {
        cluster_id: observed_cluster_id,
        topic_id: observed_topic_id,
    })
}

pub fn observe_and_validate_kafka_source_v0<C: ConsumerContext>(
    consumer: &BaseConsumer<C>,
    topic_name: &str,
    expected: &KafkaSourceContractV0,
    observed_source_id: &SourceIdV0,
    timeout: Duration,
) -> Result<KafkaBrokerSourceObservationV0, KafkaBrokerSourceObservationErrorV0> {
    validate_topic_name(topic_name)?;
    let timeout_ms = validate_timeout(timeout)?;
    let cluster_id = consumer
        .client()
        .fetch_cluster_id(timeout)
        .ok_or(KafkaBrokerSourceObservationErrorV0::ClusterIdUnavailable)?;
    let cluster_id = KafkaClusterIdV0::new(&cluster_id)
        .map_err(KafkaBrokerSourceObservationErrorV0::ClusterId)?;
    let topic_id =
        describe_kafka_topic_id_v0(consumer.client().native_ptr(), topic_name, timeout_ms)
            .map_err(KafkaBrokerSourceObservationErrorV0::TopicDescription)?;
    validate_kafka_source_observation_v0(expected, observed_source_id, cluster_id, topic_id)
}

fn validate_topic_name(topic_name: &str) -> Result<(), KafkaBrokerSourceObservationErrorV0> {
    if topic_name.is_empty()
        || topic_name.len() > MAX_KAFKA_TOPIC_NAME_BYTES_V0
        || topic_name.as_bytes().contains(&0)
    {
        return Err(KafkaBrokerSourceObservationErrorV0::InvalidTopicName);
    }
    Ok(())
}

fn validate_timeout(timeout: Duration) -> Result<i32, KafkaBrokerSourceObservationErrorV0> {
    let timeout_ms = timeout.as_millis();
    if timeout_ms == 0 || timeout_ms > u128::from(KAFKA_BROKER_IDENTITY_MAX_TIMEOUT_MS_V0) {
        return Err(KafkaBrokerSourceObservationErrorV0::InvalidTimeout);
    }
    i32::try_from(timeout_ms).map_err(|_| KafkaBrokerSourceObservationErrorV0::InvalidTimeout)
}
