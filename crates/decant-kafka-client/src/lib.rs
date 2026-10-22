//! Private native Kafka client boundary for Decant.
//!
//! Broker behavior is added only by later approved tasks.

#![deny(unsafe_code)]

mod identity;
mod record_adapter;
mod security;
mod static_consumer;
#[allow(unsafe_code)]
mod topic_description_ffi;

pub use identity::{
    KAFKA_BROKER_IDENTITY_MAX_TIMEOUT_MS_V0, KafkaBrokerSourceObservationErrorV0,
    KafkaBrokerSourceObservationV0, observe_and_validate_kafka_source_v0,
    validate_kafka_source_observation_v0,
};
pub use record_adapter::{KafkaRecordAdapterErrorV0, convert_kafka_message_v0};
pub use security::{
    KAFKA_SECURITY_CONFIG_VERSION_V0, KAFKA_SECURITY_MAX_CA_LOCATION_BYTES_V0,
    KAFKA_SECURITY_MAX_SECRET_ENV_NAME_BYTES_V0, KAFKA_SECURITY_MAX_SECRET_VALUE_BYTES_V0,
    KafkaSaslScramMechanismV0, KafkaSecretEnvironmentReferenceV0, KafkaStaticSecurityConfigErrorV0,
    KafkaStaticSecurityConfigV0, KafkaStaticSecurityModeV0, KafkaTlsTrustV0,
};
pub use static_consumer::{
    KAFKA_STATIC_CONSUMER_MAX_BOOTSTRAP_SERVERS_BYTES_V0,
    KAFKA_STATIC_CONSUMER_MAX_POLL_TIMEOUT_MS_V0, KAFKA_STATIC_CONSUMER_MAX_TOPIC_NAME_BYTES_V0,
    KafkaStaticConsumerErrorV0, KafkaStaticConsumerV0, KafkaStaticPollErrorV0, KafkaStaticPollV0,
    KafkaStaticSourceConfigErrorV0, KafkaStaticSourceConfigV0, kafka_static_client_config_v0,
};
pub use topic_description_ffi::KafkaTopicDescriptionFfiErrorV0;

pub const KAFKA_CLIENT_BOUNDARY_VERSION_V0: u16 = 0;
pub const RUST_RDKAFKA_CRATE_VERSION_V0: &str = "0.39.0";

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct KafkaClientDependencyVersionsV0 {
    rust_wrapper: &'static str,
    native_version_number: i32,
    native_version: String,
}

impl KafkaClientDependencyVersionsV0 {
    pub const fn rust_wrapper(&self) -> &'static str {
        self.rust_wrapper
    }

    pub const fn native_version_number(&self) -> i32 {
        self.native_version_number
    }

    pub fn native_version(&self) -> &str {
        &self.native_version
    }
}

pub fn kafka_client_dependency_versions_v0() -> KafkaClientDependencyVersionsV0 {
    let (native_version_number, native_version) = rdkafka::util::get_rdkafka_version();
    KafkaClientDependencyVersionsV0 {
        rust_wrapper: RUST_RDKAFKA_CRATE_VERSION_V0,
        native_version_number,
        native_version,
    }
}
