use std::ffi::OsStr;
use std::fmt::{self, Write as _};
use std::fs::File;
use std::io::Read;
use std::path::PathBuf;
use std::time::Duration;

use decant_compiler::{
    KAFKA_MATERIAL_CHANGE_CONTEXT_VERSION_V1, KafkaActivityContextRequestV0,
    KafkaActivityContextV0, KafkaEntityKeyEgressActionV0, KafkaFieldEgressActionV0,
    KafkaFieldEgressPolicyV0, KafkaFieldEgressRuleV0, KafkaGroupedRatioContextRequestV0,
    KafkaGroupedRatioContextV0, KafkaMaterialChangeContextV0, KafkaMaterialChangeRequestV0,
    compile_kafka_activity_context_v0, compile_kafka_grouped_ratio_context_v0,
    compile_kafka_material_change_context_v1,
};
use decant_ir::{SourceIdV0, SourcePositionKindV0};
use decant_kafka::{
    KAFKA_JSON_OBJECT_MAPPING_FORMAT_V0, KAFKA_SOURCE_CONTRACT_VERSION_V0, KafkaClusterIdV0,
    KafkaJsonEntityKeySourceV1, KafkaJsonEventTimeUnitV0, KafkaJsonObjectMappingV0,
    KafkaJsonObjectMappingV1, KafkaJsonObjectProjectionV0, KafkaJsonScalarKindV0,
    KafkaSourceContractV0, KafkaTopicIdV0,
};
use decant_kafka_client::{
    KafkaSaslScramMechanismV0, KafkaSecretEnvironmentReferenceV0, KafkaStaticConsumerV0,
    KafkaStaticPollV0, KafkaStaticSecurityConfigV0, KafkaStaticSourceConfigV0, KafkaTlsTrustV0,
    observe_and_validate_kafka_source_v0,
};
use decant_kafka_live::{
    KafkaResidentLifecycleConfigV0, KafkaStaticCheckpointSessionV0, KafkaStaticSessionCheckpointV0,
    KafkaStaticSessionUsageV0,
};
use decant_state::{
    ADMISSION_VERSION_V0, AdmissionConfigV0, AdmissionLimitsV0, EVENT_TIME_RETENTION_VERSION_V0,
    EventTimeRetentionConfigV0,
};
use decant_storage::{LocalCheckpointStoreV0, MAX_LOCAL_CHECKPOINT_ROOT_PATH_BYTES_V0};
use serde::Deserialize;

pub(crate) mod evidence;

pub(crate) const MAX_KAFKA_LIVE_CONFIG_BYTES_V0: usize = 64 * 1_024;
pub(crate) const MAX_KAFKA_LIVE_OUTPUT_BYTES_V0: usize = 16 * 1_024;
pub(crate) const MAX_KAFKA_LIVE_OUTPUT_BYTES_V1: usize = 32 * 1_024;
pub(crate) const MAX_KAFKA_LIVE_OUTPUT_BYTES_V2: usize = 32 * 1_024;
pub(crate) const MAX_KAFKA_LIVE_OUTPUT_BYTES_V3: usize = 32 * 1_024;
pub(crate) const MAX_KAFKA_LIVE_IDLE_POLL_LIMIT_V0: usize = 10_000;
pub(crate) const MAX_KAFKA_LIVE_IDLE_WAIT_MS_V0: u64 = 5 * 60 * 1_000;
pub(crate) const MAX_KAFKA_LIVE_RECORDS_V0: usize = 1_000_000;

const KAFKA_LIVE_CONFIG_FORMAT_V0: &str = "decant-kafka-live-config";
const KAFKA_LIVE_CONFIG_VERSION_V0: u16 = 0;
const KAFKA_LIVE_CONFIG_VERSION_V1: u16 = 1;
const KAFKA_LIVE_CONFIG_VERSION_V2: u16 = 2;
const KAFKA_LIVE_CONFIG_VERSION_V3: u16 = 3;
const KAFKA_LIVE_CONFIG_VERSION_V4: u16 = 4;
const KAFKA_MCP_SERVICE_CONFIG_FORMAT_V0: &str = "decant-kafka-mcp-service-config";
const KAFKA_MCP_SERVICE_CONFIG_VERSION_V0: u16 = 0;
const KAFKA_ACTIVITY_CONTEXT_REQUEST_FORMAT_V0: &str = "decant-kafka-activity-context-request";
const KAFKA_GROUPED_RATIO_CONTEXT_REQUEST_FORMAT_V0: &str =
    "decant-kafka-grouped-ratio-context-request";
const KAFKA_FIELD_EGRESS_POLICY_FORMAT_V0: &str = "decant-kafka-field-egress-policy";
const KAFKA_MATERIAL_CHANGE_CONTEXT_REQUEST_FORMAT_V1: &str =
    "decant-kafka-material-change-context-request";

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum KafkaJsonCliErrorV0 {
    ConfigFileOpen,
    ConfigFileRead,
    ConfigTooLarge,
    ConfigAllocation,
    ConfigSyntax,
    ConfigContract,
    CheckpointStore,
    SessionBootstrap,
    ClientCreation,
    SourceObservation,
    Assignment,
    Poll,
    Processing,
    Accounting,
    Context,
    Checkpoint,
    OutputAllocation,
    OutputCapacity,
}

#[derive(Deserialize)]
struct KafkaLiveConfigVersionProbeV0 {
    format: String,
    version: u16,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct KafkaLiveConfigDocumentV0 {
    format: String,
    version: u16,
    bootstrap_servers: String,
    topic: String,
    source_id: String,
    expected_cluster_id: String,
    expected_topic_id: String,
    partitions: Vec<KafkaLivePartitionDocumentV0>,
    checkpoint_directory: String,
    epoch_start_ns: i64,
    epoch_end_ns: i64,
    accepted_identity_limit: usize,
    accepted_event_byte_limit: usize,
    poll_timeout_ms: u64,
    idle_poll_limit: usize,
    max_records: usize,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct KafkaLiveConfigDocumentV1 {
    format: String,
    version: u16,
    bootstrap_servers: String,
    topic: String,
    source_id: String,
    expected_cluster_id: String,
    expected_topic_id: String,
    partitions: Vec<KafkaLivePartitionDocumentV0>,
    checkpoint_directory: String,
    epoch_start_ns: i64,
    epoch_end_ns: i64,
    accepted_identity_limit: usize,
    accepted_event_byte_limit: usize,
    poll_timeout_ms: u64,
    idle_poll_limit: usize,
    max_records: usize,
    context: KafkaActivityContextRequestDocumentV0,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct KafkaLiveConfigDocumentV2 {
    format: String,
    version: u16,
    bootstrap_servers: String,
    topic: String,
    source_namespace: String,
    expected_cluster_id: String,
    expected_topic_id: String,
    partitions: Vec<KafkaLivePartitionDocumentV0>,
    checkpoint_directory: String,
    epoch_start_ns: i64,
    epoch_end_ns: i64,
    accepted_identity_limit: usize,
    accepted_event_byte_limit: usize,
    poll_timeout_ms: u64,
    idle_poll_limit: usize,
    max_records: usize,
    context: KafkaActivityContextRequestDocumentV0,
    mapping: KafkaJsonObjectMappingDocumentV0,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct KafkaLiveConfigDocumentV3 {
    format: String,
    version: u16,
    bootstrap_servers: String,
    topic: String,
    source_namespace: String,
    expected_cluster_id: String,
    expected_topic_id: String,
    partitions: Vec<KafkaLivePartitionDocumentV0>,
    checkpoint_directory: String,
    epoch_start_ns: i64,
    epoch_end_ns: i64,
    accepted_identity_limit: usize,
    accepted_event_byte_limit: usize,
    poll_timeout_ms: u64,
    idle_poll_limit: usize,
    max_records: usize,
    context: KafkaGroupedRatioContextRequestDocumentV0,
    egress: KafkaFieldEgressPolicyDocumentV0,
    mapping: KafkaJsonObjectMappingDocumentV0,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct KafkaLiveConfigDocumentV4 {
    format: String,
    version: u16,
    bootstrap_servers: String,
    topic: String,
    source_namespace: String,
    expected_cluster_id: String,
    expected_topic_id: String,
    partitions: Vec<KafkaLivePartitionDocumentV0>,
    checkpoint_directory: String,
    epoch_start_ns: i64,
    epoch_end_ns: i64,
    accepted_identity_limit: usize,
    accepted_event_byte_limit: usize,
    poll_timeout_ms: u64,
    idle_poll_limit: usize,
    max_records: usize,
    context: KafkaMaterialChangeRequestDocumentV1,
    egress: KafkaFieldEgressPolicyDocumentV0,
    mapping: KafkaJsonObjectMappingDocumentV1,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct KafkaMcpServiceConfigDocumentV0 {
    format: String,
    version: u16,
    bootstrap_servers: String,
    topic: String,
    source_namespace: String,
    expected_cluster_id: String,
    expected_topic_id: String,
    partitions: Vec<KafkaLivePartitionDocumentV0>,
    checkpoint_directory: String,
    epoch_start_ns: i64,
    epoch_end_ns: i64,
    accepted_identity_limit: usize,
    accepted_event_byte_limit: usize,
    poll_timeout_ms: u64,
    lifecycle: KafkaMcpLifecycleDocumentV0,
    security: KafkaMcpSecurityDocumentV0,
    context: KafkaMaterialChangeRequestDocumentV1,
    egress: KafkaFieldEgressPolicyDocumentV0,
    mapping: KafkaJsonObjectMappingDocumentV1,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct KafkaMcpLifecycleDocumentV0 {
    version: u16,
    records_between_checkpoints: usize,
    idle_observations_between_checkpoints: usize,
    retained_checkpoint_generations: usize,
}

#[derive(Deserialize)]
#[serde(tag = "mode", deny_unknown_fields)]
enum KafkaMcpSecurityDocumentV0 {
    #[serde(rename = "plaintext")]
    Plaintext { version: u16 },
    #[serde(rename = "tls")]
    Tls {
        version: u16,
        ca_location: Option<String>,
    },
    #[serde(rename = "tls_sasl_scram")]
    TlsSaslScram {
        version: u16,
        ca_location: Option<String>,
        mechanism: KafkaMcpScramMechanismDocumentV0,
        username_env: String,
        password_env: String,
    },
}

#[derive(Clone, Copy, Deserialize)]
enum KafkaMcpScramMechanismDocumentV0 {
    #[serde(rename = "sha256")]
    Sha256,
    #[serde(rename = "sha512")]
    Sha512,
}

impl From<KafkaMcpScramMechanismDocumentV0> for KafkaSaslScramMechanismV0 {
    fn from(value: KafkaMcpScramMechanismDocumentV0) -> Self {
        match value {
            KafkaMcpScramMechanismDocumentV0::Sha256 => Self::Sha256,
            KafkaMcpScramMechanismDocumentV0::Sha512 => Self::Sha512,
        }
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct KafkaJsonObjectMappingDocumentV0 {
    format: String,
    version: u16,
    event_time: KafkaJsonEventTimeDocumentV0,
    payload: Vec<KafkaJsonProjectionDocumentV0>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct KafkaJsonObjectMappingDocumentV1 {
    format: String,
    version: u16,
    entity_key: KafkaJsonEntityKeyDocumentV1,
    event_time: KafkaJsonEventTimeDocumentV0,
    payload: Vec<KafkaJsonProjectionDocumentV0>,
}

#[derive(Deserialize)]
#[serde(tag = "source", deny_unknown_fields)]
enum KafkaJsonEntityKeyDocumentV1 {
    #[serde(rename = "kafka_record_key")]
    KafkaRecordKey,
    #[serde(rename = "utf8_projection")]
    Utf8Projection { field: String },
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct KafkaJsonEventTimeDocumentV0 {
    field: String,
    unit: KafkaJsonEventTimeUnitDocumentV0,
}

#[derive(Clone, Copy, Deserialize)]
enum KafkaJsonEventTimeUnitDocumentV0 {
    #[serde(rename = "nanoseconds")]
    Nanoseconds,
    #[serde(rename = "microseconds")]
    Microseconds,
    #[serde(rename = "milliseconds")]
    Milliseconds,
    #[serde(rename = "seconds")]
    Seconds,
}

impl From<KafkaJsonEventTimeUnitDocumentV0> for KafkaJsonEventTimeUnitV0 {
    fn from(value: KafkaJsonEventTimeUnitDocumentV0) -> Self {
        match value {
            KafkaJsonEventTimeUnitDocumentV0::Nanoseconds => Self::Nanoseconds,
            KafkaJsonEventTimeUnitDocumentV0::Microseconds => Self::Microseconds,
            KafkaJsonEventTimeUnitDocumentV0::Milliseconds => Self::Milliseconds,
            KafkaJsonEventTimeUnitDocumentV0::Seconds => Self::Seconds,
        }
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct KafkaJsonProjectionDocumentV0 {
    source: String,
    target: String,
    #[serde(rename = "type")]
    kind: KafkaJsonScalarKindDocumentV0,
}

#[derive(Clone, Copy, Deserialize)]
enum KafkaJsonScalarKindDocumentV0 {
    #[serde(rename = "null")]
    Null,
    #[serde(rename = "bool")]
    Bool,
    #[serde(rename = "i64")]
    I64,
    #[serde(rename = "u64")]
    U64,
    #[serde(rename = "utf8")]
    Utf8,
}

impl From<KafkaJsonScalarKindDocumentV0> for KafkaJsonScalarKindV0 {
    fn from(value: KafkaJsonScalarKindDocumentV0) -> Self {
        match value {
            KafkaJsonScalarKindDocumentV0::Null => Self::Null,
            KafkaJsonScalarKindDocumentV0::Bool => Self::Bool,
            KafkaJsonScalarKindDocumentV0::I64 => Self::I64,
            KafkaJsonScalarKindDocumentV0::U64 => Self::U64,
            KafkaJsonScalarKindDocumentV0::Utf8 => Self::Utf8,
        }
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct KafkaActivityContextRequestDocumentV0 {
    format: String,
    version: u16,
    byte_budget: usize,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct KafkaGroupedRatioContextRequestDocumentV0 {
    format: String,
    version: u16,
    fact_id: String,
    group_field: String,
    predicate: KafkaGroupedRatioPredicateDocumentV0,
    lookback_ns: u64,
    evidence_limit: usize,
    byte_budget: usize,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct KafkaMaterialChangeRequestDocumentV1 {
    format: String,
    version: u16,
    view_id: String,
    retention_ns: u64,
    discriminator_field: String,
    value_field: String,
    item_limit: usize,
    byte_budget: usize,
    entity_key_egress: KafkaEntityKeyEgressActionDocumentV0,
}

#[derive(Clone, Copy, Deserialize)]
enum KafkaEntityKeyEgressActionDocumentV0 {
    #[serde(rename = "allow_lower_hex")]
    AllowLowerHex,
    #[serde(rename = "deny")]
    Deny,
}

impl From<KafkaEntityKeyEgressActionDocumentV0> for KafkaEntityKeyEgressActionV0 {
    fn from(value: KafkaEntityKeyEgressActionDocumentV0) -> Self {
        match value {
            KafkaEntityKeyEgressActionDocumentV0::AllowLowerHex => Self::AllowHex,
            KafkaEntityKeyEgressActionDocumentV0::Deny => Self::Deny,
        }
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct KafkaGroupedRatioPredicateDocumentV0 {
    field: String,
    equals: bool,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct KafkaFieldEgressPolicyDocumentV0 {
    format: String,
    version: u16,
    fields: Vec<KafkaFieldEgressRuleDocumentV0>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct KafkaFieldEgressRuleDocumentV0 {
    field: String,
    action: KafkaFieldEgressActionDocumentV0,
}

#[derive(Clone, Copy, Deserialize)]
enum KafkaFieldEgressActionDocumentV0 {
    #[serde(rename = "allow")]
    Allow,
    #[serde(rename = "deny")]
    Deny,
}

impl From<KafkaFieldEgressActionDocumentV0> for KafkaFieldEgressActionV0 {
    fn from(value: KafkaFieldEgressActionDocumentV0) -> Self {
        match value {
            KafkaFieldEgressActionDocumentV0::Allow => Self::Allow,
            KafkaFieldEgressActionDocumentV0::Deny => Self::Deny,
        }
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct KafkaLivePartitionDocumentV0 {
    partition: i32,
    start_offset: i64,
}

struct KafkaLiveConfigV0 {
    source: KafkaStaticSourceConfigV0,
    admission: AdmissionConfigV0,
    checkpoint_directory: PathBuf,
    poll_timeout: Duration,
    idle_poll_limit: usize,
    max_records: usize,
    input: KafkaLiveInputContractV0,
    output: KafkaLiveOutputContractV0,
}

pub(crate) struct KafkaMcpServiceConfigV0 {
    pub(crate) source: KafkaStaticSourceConfigV0,
    pub(crate) admission: AdmissionConfigV0,
    pub(crate) checkpoint_directory: PathBuf,
    pub(crate) poll_timeout: Duration,
    pub(crate) mapping: KafkaJsonObjectMappingV1,
    pub(crate) context: KafkaMaterialChangeRequestV0,
    pub(crate) lifecycle: KafkaResidentLifecycleConfigV0,
    pub(crate) security: KafkaStaticSecurityConfigV0,
}

impl fmt::Debug for KafkaMcpServiceConfigV0 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("KafkaMcpServiceConfigV0")
            .field("source", &"<redacted>")
            .field("admission", &"<redacted>")
            .field("checkpoint_directory", &"<redacted>")
            .field("poll_timeout_ms", &self.poll_timeout.as_millis())
            .field("mapping", &"<redacted>")
            .field("context", &self.context)
            .field("lifecycle", &self.lifecycle)
            .field("security", &self.security)
            .finish()
    }
}

impl fmt::Debug for KafkaLiveConfigV0 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("KafkaLiveConfigV0")
            .field("source", &"<redacted>")
            .field("admission", &"<redacted>")
            .field("checkpoint_directory", &"<redacted>")
            .field("poll_timeout_ms", &self.poll_timeout.as_millis())
            .field("idle_poll_limit", &self.idle_poll_limit)
            .field("max_records", &self.max_records)
            .field("input", &"<redacted>")
            .field("output", &self.output)
            .finish()
    }
}

enum KafkaLiveInputContractV0 {
    DecantJson,
    JsonObject { mapping: KafkaJsonObjectMappingV0 },
    JsonObjectV1 { mapping: KafkaJsonObjectMappingV1 },
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum KafkaLiveOutputContractV0 {
    V0,
    V1 {
        context: KafkaActivityContextRequestV0,
    },
    V2 {
        context: KafkaGroupedRatioContextRequestV0,
    },
    V3 {
        context: KafkaMaterialChangeRequestV0,
    },
}

struct KafkaLiveConfigFieldsV0 {
    bootstrap_servers: String,
    topic: String,
    expected_cluster_id: String,
    expected_topic_id: String,
    partitions: Vec<KafkaLivePartitionDocumentV0>,
    checkpoint_directory: String,
    epoch_start_ns: i64,
    epoch_end_ns: i64,
    accepted_identity_limit: usize,
    accepted_event_byte_limit: usize,
    poll_timeout_ms: u64,
    idle_poll_limit: usize,
    max_records: usize,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum KafkaLiveTerminationV0 {
    MaxRecords,
    IdleLimit,
}

impl KafkaLiveTerminationV0 {
    const fn as_str(self) -> &'static str {
        match self {
            Self::MaxRecords => "max_records",
            Self::IdleLimit => "idle_limit",
        }
    }
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
struct KafkaLivePollUsageV0 {
    data_records: usize,
    idle_observations: usize,
    partition_eof_observations: usize,
}

pub(crate) fn run_kafka_json_config_file_v0(path: &OsStr) -> Result<Vec<u8>, KafkaJsonCliErrorV0> {
    let config = load_kafka_live_config_v0(path)?;
    run_kafka_live_config_v0(config)
}

pub(crate) fn load_kafka_mcp_service_config_v0(
    path: &OsStr,
) -> Result<KafkaMcpServiceConfigV0, KafkaJsonCliErrorV0> {
    let file = File::open(path).map_err(|_| KafkaJsonCliErrorV0::ConfigFileOpen)?;
    let bytes = read_bounded_config_v0(file)?;
    parse_kafka_mcp_service_config_v0(&bytes)
}

pub(crate) fn parse_kafka_mcp_service_config_v0(
    bytes: &[u8],
) -> Result<KafkaMcpServiceConfigV0, KafkaJsonCliErrorV0> {
    if bytes.len() > MAX_KAFKA_LIVE_CONFIG_BYTES_V0 {
        return Err(KafkaJsonCliErrorV0::ConfigTooLarge);
    }
    let document: KafkaMcpServiceConfigDocumentV0 =
        serde_json::from_slice(bytes).map_err(|_| KafkaJsonCliErrorV0::ConfigSyntax)?;
    if document.format != KAFKA_MCP_SERVICE_CONFIG_FORMAT_V0
        || document.version != KAFKA_MCP_SERVICE_CONFIG_VERSION_V0
        || document.context.format != KAFKA_MATERIAL_CHANGE_CONTEXT_REQUEST_FORMAT_V1
        || document.context.version != KAFKA_MATERIAL_CHANGE_CONTEXT_VERSION_V1
        || document.egress.format != KAFKA_FIELD_EGRESS_POLICY_FORMAT_V0
        || document.mapping.format != KAFKA_JSON_OBJECT_MAPPING_FORMAT_V0
    {
        return Err(KafkaJsonCliErrorV0::ConfigContract);
    }
    let mapping = build_json_object_mapping_v1(&document.source_namespace, document.mapping)?;
    let policy = build_field_policy(document.egress)?;
    validate_policy_covers_mapping(mapping.base(), &policy)?;
    validate_material_change_fields(
        &mapping,
        &policy,
        &document.context.discriminator_field,
        &document.context.value_field,
        document.context.entity_key_egress,
    )?;
    let retention = EventTimeRetentionConfigV0::new(
        EVENT_TIME_RETENTION_VERSION_V0,
        document.context.retention_ns,
    )
    .map_err(|_| KafkaJsonCliErrorV0::ConfigContract)?;
    let context = KafkaMaterialChangeRequestV0::new(
        document.context.version,
        &document.context.view_id,
        mapping.schema_id().clone(),
        retention,
        &document.context.discriminator_field,
        &document.context.value_field,
        document.context.item_limit,
        document.context.byte_budget,
        policy,
        document.context.entity_key_egress.into(),
    )
    .map_err(|_| KafkaJsonCliErrorV0::ConfigContract)?;
    let lifecycle = KafkaResidentLifecycleConfigV0::new(
        document.lifecycle.version,
        document.lifecycle.records_between_checkpoints,
        document.lifecycle.idle_observations_between_checkpoints,
        document.lifecycle.retained_checkpoint_generations,
        context.retention(),
    )
    .map_err(|_| KafkaJsonCliErrorV0::ConfigContract)?;
    let security = build_kafka_mcp_security_v0(document.security)?;
    let (source, admission, checkpoint_directory, poll_timeout) = build_kafka_source_config_v0(
        document.bootstrap_servers,
        document.topic,
        document.expected_cluster_id,
        document.expected_topic_id,
        document.partitions,
        document.checkpoint_directory,
        document.epoch_start_ns,
        document.epoch_end_ns,
        document.accepted_identity_limit,
        document.accepted_event_byte_limit,
        document.poll_timeout_ms,
        mapping.effective_source_id().clone(),
    )?;
    Ok(KafkaMcpServiceConfigV0 {
        source,
        admission,
        checkpoint_directory,
        poll_timeout,
        mapping,
        context,
        lifecycle,
        security,
    })
}

fn build_kafka_mcp_security_v0(
    document: KafkaMcpSecurityDocumentV0,
) -> Result<KafkaStaticSecurityConfigV0, KafkaJsonCliErrorV0> {
    match document {
        KafkaMcpSecurityDocumentV0::Plaintext { version } => {
            KafkaStaticSecurityConfigV0::plaintext(version)
        }
        KafkaMcpSecurityDocumentV0::Tls {
            version,
            ca_location,
        } => KafkaStaticSecurityConfigV0::tls(version, build_kafka_tls_trust_v0(ca_location)?),
        KafkaMcpSecurityDocumentV0::TlsSaslScram {
            version,
            ca_location,
            mechanism,
            username_env,
            password_env,
        } => KafkaStaticSecurityConfigV0::tls_sasl_scram(
            version,
            build_kafka_tls_trust_v0(ca_location)?,
            mechanism.into(),
            KafkaSecretEnvironmentReferenceV0::new(&username_env)
                .map_err(|_| KafkaJsonCliErrorV0::ConfigContract)?,
            KafkaSecretEnvironmentReferenceV0::new(&password_env)
                .map_err(|_| KafkaJsonCliErrorV0::ConfigContract)?,
        ),
    }
    .map_err(|_| KafkaJsonCliErrorV0::ConfigContract)
}

fn build_kafka_tls_trust_v0(
    ca_location: Option<String>,
) -> Result<KafkaTlsTrustV0, KafkaJsonCliErrorV0> {
    match ca_location {
        Some(location) => KafkaTlsTrustV0::absolute_ca_location(&location)
            .map_err(|_| KafkaJsonCliErrorV0::ConfigContract),
        None => Ok(KafkaTlsTrustV0::system_roots()),
    }
}

fn load_kafka_live_config_v0(path: &OsStr) -> Result<KafkaLiveConfigV0, KafkaJsonCliErrorV0> {
    let file = File::open(path).map_err(|_| KafkaJsonCliErrorV0::ConfigFileOpen)?;
    let bytes = read_bounded_config_v0(file)?;
    parse_kafka_live_config(&bytes)
}

fn read_bounded_config_v0(mut file: File) -> Result<Vec<u8>, KafkaJsonCliErrorV0> {
    let read_bound = MAX_KAFKA_LIVE_CONFIG_BYTES_V0
        .checked_add(1)
        .ok_or(KafkaJsonCliErrorV0::ConfigTooLarge)?;
    let mut bytes = Vec::new();
    bytes
        .try_reserve_exact(read_bound)
        .map_err(|_| KafkaJsonCliErrorV0::ConfigAllocation)?;
    file.by_ref()
        .take(u64::try_from(read_bound).map_err(|_| KafkaJsonCliErrorV0::ConfigTooLarge)?)
        .read_to_end(&mut bytes)
        .map_err(|_| KafkaJsonCliErrorV0::ConfigFileRead)?;
    if bytes.len() > MAX_KAFKA_LIVE_CONFIG_BYTES_V0 {
        return Err(KafkaJsonCliErrorV0::ConfigTooLarge);
    }
    Ok(bytes)
}

fn parse_kafka_live_config_v0(bytes: &[u8]) -> Result<KafkaLiveConfigV0, KafkaJsonCliErrorV0> {
    if bytes.len() > MAX_KAFKA_LIVE_CONFIG_BYTES_V0 {
        return Err(KafkaJsonCliErrorV0::ConfigTooLarge);
    }
    let document: KafkaLiveConfigDocumentV0 =
        serde_json::from_slice(bytes).map_err(|_| KafkaJsonCliErrorV0::ConfigSyntax)?;
    if document.format != KAFKA_LIVE_CONFIG_FORMAT_V0
        || document.version != KAFKA_LIVE_CONFIG_VERSION_V0
    {
        return Err(KafkaJsonCliErrorV0::ConfigContract);
    }
    build_kafka_live_config(
        KafkaLiveConfigFieldsV0 {
            bootstrap_servers: document.bootstrap_servers,
            topic: document.topic,
            expected_cluster_id: document.expected_cluster_id,
            expected_topic_id: document.expected_topic_id,
            partitions: document.partitions,
            checkpoint_directory: document.checkpoint_directory,
            epoch_start_ns: document.epoch_start_ns,
            epoch_end_ns: document.epoch_end_ns,
            accepted_identity_limit: document.accepted_identity_limit,
            accepted_event_byte_limit: document.accepted_event_byte_limit,
            poll_timeout_ms: document.poll_timeout_ms,
            idle_poll_limit: document.idle_poll_limit,
            max_records: document.max_records,
        },
        SourceIdV0::new(&document.source_id).map_err(|_| KafkaJsonCliErrorV0::ConfigContract)?,
        KafkaLiveInputContractV0::DecantJson,
        KafkaLiveOutputContractV0::V0,
    )
}

fn parse_kafka_live_config(bytes: &[u8]) -> Result<KafkaLiveConfigV0, KafkaJsonCliErrorV0> {
    if bytes.len() > MAX_KAFKA_LIVE_CONFIG_BYTES_V0 {
        return Err(KafkaJsonCliErrorV0::ConfigTooLarge);
    }
    let probe: KafkaLiveConfigVersionProbeV0 =
        serde_json::from_slice(bytes).map_err(|_| KafkaJsonCliErrorV0::ConfigSyntax)?;
    if probe.format != KAFKA_LIVE_CONFIG_FORMAT_V0 {
        return Err(KafkaJsonCliErrorV0::ConfigContract);
    }
    match probe.version {
        KAFKA_LIVE_CONFIG_VERSION_V0 => parse_kafka_live_config_v0(bytes),
        KAFKA_LIVE_CONFIG_VERSION_V1 => parse_kafka_live_config_v1(bytes),
        KAFKA_LIVE_CONFIG_VERSION_V2 => parse_kafka_live_config_v2(bytes),
        KAFKA_LIVE_CONFIG_VERSION_V3 => parse_kafka_live_config_v3(bytes),
        KAFKA_LIVE_CONFIG_VERSION_V4 => parse_kafka_live_config_v4(bytes),
        _ => Err(KafkaJsonCliErrorV0::ConfigContract),
    }
}

fn parse_kafka_live_config_v1(bytes: &[u8]) -> Result<KafkaLiveConfigV0, KafkaJsonCliErrorV0> {
    if bytes.len() > MAX_KAFKA_LIVE_CONFIG_BYTES_V0 {
        return Err(KafkaJsonCliErrorV0::ConfigTooLarge);
    }
    let document: KafkaLiveConfigDocumentV1 =
        serde_json::from_slice(bytes).map_err(|_| KafkaJsonCliErrorV0::ConfigSyntax)?;
    if document.format != KAFKA_LIVE_CONFIG_FORMAT_V0
        || document.version != KAFKA_LIVE_CONFIG_VERSION_V1
        || document.context.format != KAFKA_ACTIVITY_CONTEXT_REQUEST_FORMAT_V0
    {
        return Err(KafkaJsonCliErrorV0::ConfigContract);
    }
    let context =
        KafkaActivityContextRequestV0::new(document.context.version, document.context.byte_budget)
            .map_err(|_| KafkaJsonCliErrorV0::ConfigContract)?;
    build_kafka_live_config(
        KafkaLiveConfigFieldsV0 {
            bootstrap_servers: document.bootstrap_servers,
            topic: document.topic,
            expected_cluster_id: document.expected_cluster_id,
            expected_topic_id: document.expected_topic_id,
            partitions: document.partitions,
            checkpoint_directory: document.checkpoint_directory,
            epoch_start_ns: document.epoch_start_ns,
            epoch_end_ns: document.epoch_end_ns,
            accepted_identity_limit: document.accepted_identity_limit,
            accepted_event_byte_limit: document.accepted_event_byte_limit,
            poll_timeout_ms: document.poll_timeout_ms,
            idle_poll_limit: document.idle_poll_limit,
            max_records: document.max_records,
        },
        SourceIdV0::new(&document.source_id).map_err(|_| KafkaJsonCliErrorV0::ConfigContract)?,
        KafkaLiveInputContractV0::DecantJson,
        KafkaLiveOutputContractV0::V1 { context },
    )
}

fn parse_kafka_live_config_v2(bytes: &[u8]) -> Result<KafkaLiveConfigV0, KafkaJsonCliErrorV0> {
    if bytes.len() > MAX_KAFKA_LIVE_CONFIG_BYTES_V0 {
        return Err(KafkaJsonCliErrorV0::ConfigTooLarge);
    }
    let document: KafkaLiveConfigDocumentV2 =
        serde_json::from_slice(bytes).map_err(|_| KafkaJsonCliErrorV0::ConfigSyntax)?;
    if document.format != KAFKA_LIVE_CONFIG_FORMAT_V0
        || document.version != KAFKA_LIVE_CONFIG_VERSION_V2
        || document.context.format != KAFKA_ACTIVITY_CONTEXT_REQUEST_FORMAT_V0
        || document.mapping.format != KAFKA_JSON_OBJECT_MAPPING_FORMAT_V0
    {
        return Err(KafkaJsonCliErrorV0::ConfigContract);
    }
    let context =
        KafkaActivityContextRequestV0::new(document.context.version, document.context.byte_budget)
            .map_err(|_| KafkaJsonCliErrorV0::ConfigContract)?;
    let mapping = build_json_object_mapping(&document.source_namespace, document.mapping)?;
    let effective_source_id = mapping.effective_source_id().clone();
    build_kafka_live_config(
        KafkaLiveConfigFieldsV0 {
            bootstrap_servers: document.bootstrap_servers,
            topic: document.topic,
            expected_cluster_id: document.expected_cluster_id,
            expected_topic_id: document.expected_topic_id,
            partitions: document.partitions,
            checkpoint_directory: document.checkpoint_directory,
            epoch_start_ns: document.epoch_start_ns,
            epoch_end_ns: document.epoch_end_ns,
            accepted_identity_limit: document.accepted_identity_limit,
            accepted_event_byte_limit: document.accepted_event_byte_limit,
            poll_timeout_ms: document.poll_timeout_ms,
            idle_poll_limit: document.idle_poll_limit,
            max_records: document.max_records,
        },
        effective_source_id,
        KafkaLiveInputContractV0::JsonObject { mapping },
        KafkaLiveOutputContractV0::V1 { context },
    )
}

fn parse_kafka_live_config_v3(bytes: &[u8]) -> Result<KafkaLiveConfigV0, KafkaJsonCliErrorV0> {
    if bytes.len() > MAX_KAFKA_LIVE_CONFIG_BYTES_V0 {
        return Err(KafkaJsonCliErrorV0::ConfigTooLarge);
    }
    let document: KafkaLiveConfigDocumentV3 =
        serde_json::from_slice(bytes).map_err(|_| KafkaJsonCliErrorV0::ConfigSyntax)?;
    if document.format != KAFKA_LIVE_CONFIG_FORMAT_V0
        || document.version != KAFKA_LIVE_CONFIG_VERSION_V3
        || document.context.format != KAFKA_GROUPED_RATIO_CONTEXT_REQUEST_FORMAT_V0
        || document.egress.format != KAFKA_FIELD_EGRESS_POLICY_FORMAT_V0
        || document.mapping.format != KAFKA_JSON_OBJECT_MAPPING_FORMAT_V0
    {
        return Err(KafkaJsonCliErrorV0::ConfigContract);
    }
    let mapping = build_json_object_mapping(&document.source_namespace, document.mapping)?;
    let mut rules = Vec::new();
    rules
        .try_reserve_exact(document.egress.fields.len())
        .map_err(|_| KafkaJsonCliErrorV0::ConfigAllocation)?;
    for rule in document.egress.fields {
        rules.push(
            KafkaFieldEgressRuleV0::new(&rule.field, rule.action.into())
                .map_err(|_| KafkaJsonCliErrorV0::ConfigContract)?,
        );
    }
    let policy = KafkaFieldEgressPolicyV0::new(document.egress.version, rules)
        .map_err(|_| KafkaJsonCliErrorV0::ConfigContract)?;
    validate_policy_covers_mapping(&mapping, &policy)?;
    validate_semantic_field_kinds(
        &mapping,
        &document.context.group_field,
        &document.context.predicate.field,
    )?;
    let context = KafkaGroupedRatioContextRequestV0::new(
        document.context.version,
        mapping.schema_id().clone(),
        &document.context.fact_id,
        &document.context.group_field,
        &document.context.predicate.field,
        document.context.predicate.equals,
        document.context.lookback_ns,
        document.context.evidence_limit,
        document.context.byte_budget,
        policy,
    )
    .map_err(|_| KafkaJsonCliErrorV0::ConfigContract)?;
    let effective_source_id = mapping.effective_source_id().clone();
    build_kafka_live_config(
        KafkaLiveConfigFieldsV0 {
            bootstrap_servers: document.bootstrap_servers,
            topic: document.topic,
            expected_cluster_id: document.expected_cluster_id,
            expected_topic_id: document.expected_topic_id,
            partitions: document.partitions,
            checkpoint_directory: document.checkpoint_directory,
            epoch_start_ns: document.epoch_start_ns,
            epoch_end_ns: document.epoch_end_ns,
            accepted_identity_limit: document.accepted_identity_limit,
            accepted_event_byte_limit: document.accepted_event_byte_limit,
            poll_timeout_ms: document.poll_timeout_ms,
            idle_poll_limit: document.idle_poll_limit,
            max_records: document.max_records,
        },
        effective_source_id,
        KafkaLiveInputContractV0::JsonObject { mapping },
        KafkaLiveOutputContractV0::V2 { context },
    )
}

fn parse_kafka_live_config_v4(bytes: &[u8]) -> Result<KafkaLiveConfigV0, KafkaJsonCliErrorV0> {
    if bytes.len() > MAX_KAFKA_LIVE_CONFIG_BYTES_V0 {
        return Err(KafkaJsonCliErrorV0::ConfigTooLarge);
    }
    let document: KafkaLiveConfigDocumentV4 =
        serde_json::from_slice(bytes).map_err(|_| KafkaJsonCliErrorV0::ConfigSyntax)?;
    if document.format != KAFKA_LIVE_CONFIG_FORMAT_V0
        || document.version != KAFKA_LIVE_CONFIG_VERSION_V4
        || document.context.format != KAFKA_MATERIAL_CHANGE_CONTEXT_REQUEST_FORMAT_V1
        || document.context.version != KAFKA_MATERIAL_CHANGE_CONTEXT_VERSION_V1
        || document.egress.format != KAFKA_FIELD_EGRESS_POLICY_FORMAT_V0
        || document.mapping.format != KAFKA_JSON_OBJECT_MAPPING_FORMAT_V0
    {
        return Err(KafkaJsonCliErrorV0::ConfigContract);
    }
    let mapping = build_json_object_mapping_v1(&document.source_namespace, document.mapping)?;
    let policy = build_field_policy(document.egress)?;
    validate_policy_covers_mapping(mapping.base(), &policy)?;
    validate_material_change_fields(
        &mapping,
        &policy,
        &document.context.discriminator_field,
        &document.context.value_field,
        document.context.entity_key_egress,
    )?;
    let retention = EventTimeRetentionConfigV0::new(
        EVENT_TIME_RETENTION_VERSION_V0,
        document.context.retention_ns,
    )
    .map_err(|_| KafkaJsonCliErrorV0::ConfigContract)?;
    let context = KafkaMaterialChangeRequestV0::new(
        document.context.version,
        &document.context.view_id,
        mapping.schema_id().clone(),
        retention,
        &document.context.discriminator_field,
        &document.context.value_field,
        document.context.item_limit,
        document.context.byte_budget,
        policy,
        document.context.entity_key_egress.into(),
    )
    .map_err(|_| KafkaJsonCliErrorV0::ConfigContract)?;
    let effective_source_id = mapping.effective_source_id().clone();
    build_kafka_live_config(
        KafkaLiveConfigFieldsV0 {
            bootstrap_servers: document.bootstrap_servers,
            topic: document.topic,
            expected_cluster_id: document.expected_cluster_id,
            expected_topic_id: document.expected_topic_id,
            partitions: document.partitions,
            checkpoint_directory: document.checkpoint_directory,
            epoch_start_ns: document.epoch_start_ns,
            epoch_end_ns: document.epoch_end_ns,
            accepted_identity_limit: document.accepted_identity_limit,
            accepted_event_byte_limit: document.accepted_event_byte_limit,
            poll_timeout_ms: document.poll_timeout_ms,
            idle_poll_limit: document.idle_poll_limit,
            max_records: document.max_records,
        },
        effective_source_id,
        KafkaLiveInputContractV0::JsonObjectV1 { mapping },
        KafkaLiveOutputContractV0::V3 { context },
    )
}

fn build_json_object_mapping(
    source_namespace: &str,
    document: KafkaJsonObjectMappingDocumentV0,
) -> Result<KafkaJsonObjectMappingV0, KafkaJsonCliErrorV0> {
    let source_namespace =
        SourceIdV0::new(source_namespace).map_err(|_| KafkaJsonCliErrorV0::ConfigContract)?;
    let mut projections = Vec::new();
    projections
        .try_reserve_exact(document.payload.len())
        .map_err(|_| KafkaJsonCliErrorV0::ConfigAllocation)?;
    for projection in document.payload {
        projections.push(
            KafkaJsonObjectProjectionV0::new(
                &projection.source,
                &projection.target,
                projection.kind.into(),
            )
            .map_err(|_| KafkaJsonCliErrorV0::ConfigContract)?,
        );
    }
    KafkaJsonObjectMappingV0::new(
        document.version,
        source_namespace,
        &document.event_time.field,
        document.event_time.unit.into(),
        projections,
    )
    .map_err(|_| KafkaJsonCliErrorV0::ConfigContract)
}

fn build_json_object_mapping_v1(
    source_namespace: &str,
    document: KafkaJsonObjectMappingDocumentV1,
) -> Result<KafkaJsonObjectMappingV1, KafkaJsonCliErrorV0> {
    let KafkaJsonObjectMappingDocumentV1 {
        version,
        entity_key,
        event_time,
        payload,
        ..
    } = document;
    let base = build_json_object_mapping(
        source_namespace,
        KafkaJsonObjectMappingDocumentV0 {
            format: KAFKA_JSON_OBJECT_MAPPING_FORMAT_V0.to_owned(),
            version: 0,
            event_time,
            payload,
        },
    )?;
    let entity_key = match entity_key {
        KafkaJsonEntityKeyDocumentV1::KafkaRecordKey => {
            KafkaJsonEntityKeySourceV1::kafka_record_key()
        }
        KafkaJsonEntityKeyDocumentV1::Utf8Projection { field } => {
            KafkaJsonEntityKeySourceV1::utf8_projection(&field)
                .map_err(|_| KafkaJsonCliErrorV0::ConfigContract)?
        }
    };
    KafkaJsonObjectMappingV1::new(version, base, entity_key)
        .map_err(|_| KafkaJsonCliErrorV0::ConfigContract)
}

fn build_field_policy(
    document: KafkaFieldEgressPolicyDocumentV0,
) -> Result<KafkaFieldEgressPolicyV0, KafkaJsonCliErrorV0> {
    let mut rules = Vec::new();
    rules
        .try_reserve_exact(document.fields.len())
        .map_err(|_| KafkaJsonCliErrorV0::ConfigAllocation)?;
    for rule in document.fields {
        rules.push(
            KafkaFieldEgressRuleV0::new(&rule.field, rule.action.into())
                .map_err(|_| KafkaJsonCliErrorV0::ConfigContract)?,
        );
    }
    KafkaFieldEgressPolicyV0::new(document.version, rules)
        .map_err(|_| KafkaJsonCliErrorV0::ConfigContract)
}

fn validate_policy_covers_mapping(
    mapping: &KafkaJsonObjectMappingV0,
    policy: &KafkaFieldEgressPolicyV0,
) -> Result<(), KafkaJsonCliErrorV0> {
    if mapping.projections().len() != policy.rules().len()
        || mapping
            .projections()
            .iter()
            .any(|projection| policy.action_for(projection.target_field()).is_none())
    {
        return Err(KafkaJsonCliErrorV0::ConfigContract);
    }
    Ok(())
}

fn validate_semantic_field_kinds(
    mapping: &KafkaJsonObjectMappingV0,
    group_field: &str,
    predicate_field: &str,
) -> Result<(), KafkaJsonCliErrorV0> {
    let group_kind = mapping
        .projections()
        .iter()
        .find(|projection| projection.target_field() == group_field)
        .map(KafkaJsonObjectProjectionV0::kind);
    let predicate_kind = mapping
        .projections()
        .iter()
        .find(|projection| projection.target_field() == predicate_field)
        .map(KafkaJsonObjectProjectionV0::kind);
    if group_kind != Some(KafkaJsonScalarKindV0::Utf8)
        || predicate_kind != Some(KafkaJsonScalarKindV0::Bool)
    {
        return Err(KafkaJsonCliErrorV0::ConfigContract);
    }
    Ok(())
}

fn validate_material_change_fields(
    mapping: &KafkaJsonObjectMappingV1,
    policy: &KafkaFieldEgressPolicyV0,
    discriminator_field: &str,
    value_field: &str,
    key_egress: KafkaEntityKeyEgressActionDocumentV0,
) -> Result<(), KafkaJsonCliErrorV0> {
    let discriminator_kind = mapping
        .base()
        .projections()
        .iter()
        .find(|projection| projection.target_field() == discriminator_field)
        .map(KafkaJsonObjectProjectionV0::kind);
    let value_kind = mapping
        .base()
        .projections()
        .iter()
        .find(|projection| projection.target_field() == value_field)
        .map(KafkaJsonObjectProjectionV0::kind);
    if discriminator_kind != Some(KafkaJsonScalarKindV0::Utf8) || value_kind.is_none() {
        return Err(KafkaJsonCliErrorV0::ConfigContract);
    }
    if matches!(
        key_egress,
        KafkaEntityKeyEgressActionDocumentV0::AllowLowerHex
    ) && mapping
        .entity_key_source()
        .projection_target()
        .is_some_and(|field| policy.action_for(field) != Some(KafkaFieldEgressActionV0::Allow))
    {
        return Err(KafkaJsonCliErrorV0::ConfigContract);
    }
    Ok(())
}

fn build_kafka_live_config(
    fields: KafkaLiveConfigFieldsV0,
    source_id: SourceIdV0,
    input: KafkaLiveInputContractV0,
    output: KafkaLiveOutputContractV0,
) -> Result<KafkaLiveConfigV0, KafkaJsonCliErrorV0> {
    let idle_poll_limit_u64 =
        u64::try_from(fields.idle_poll_limit).map_err(|_| KafkaJsonCliErrorV0::ConfigContract)?;
    let idle_wait_ms = fields
        .poll_timeout_ms
        .checked_mul(idle_poll_limit_u64)
        .ok_or(KafkaJsonCliErrorV0::ConfigContract)?;
    if fields.idle_poll_limit == 0
        || fields.idle_poll_limit > MAX_KAFKA_LIVE_IDLE_POLL_LIMIT_V0
        || idle_wait_ms > MAX_KAFKA_LIVE_IDLE_WAIT_MS_V0
        || fields.max_records == 0
        || fields.max_records > MAX_KAFKA_LIVE_RECORDS_V0
    {
        return Err(KafkaJsonCliErrorV0::ConfigContract);
    }
    let (source, admission, checkpoint_directory, poll_timeout) = build_kafka_source_config_v0(
        fields.bootstrap_servers,
        fields.topic,
        fields.expected_cluster_id,
        fields.expected_topic_id,
        fields.partitions,
        fields.checkpoint_directory,
        fields.epoch_start_ns,
        fields.epoch_end_ns,
        fields.accepted_identity_limit,
        fields.accepted_event_byte_limit,
        fields.poll_timeout_ms,
        source_id,
    )?;
    Ok(KafkaLiveConfigV0 {
        source,
        admission,
        checkpoint_directory,
        poll_timeout,
        idle_poll_limit: fields.idle_poll_limit,
        max_records: fields.max_records,
        input,
        output,
    })
}

#[allow(clippy::too_many_arguments)]
fn build_kafka_source_config_v0(
    bootstrap_servers: String,
    topic: String,
    expected_cluster_id: String,
    expected_topic_id: String,
    partitions: Vec<KafkaLivePartitionDocumentV0>,
    checkpoint_directory: String,
    epoch_start_ns: i64,
    epoch_end_ns: i64,
    accepted_identity_limit: usize,
    accepted_event_byte_limit: usize,
    poll_timeout_ms: u64,
    source_id: SourceIdV0,
) -> Result<
    (
        KafkaStaticSourceConfigV0,
        AdmissionConfigV0,
        PathBuf,
        Duration,
    ),
    KafkaJsonCliErrorV0,
> {
    if checkpoint_directory.is_empty()
        || checkpoint_directory.len() > MAX_LOCAL_CHECKPOINT_ROOT_PATH_BYTES_V0
        || checkpoint_directory.as_bytes().contains(&0)
        || poll_timeout_ms == 0
        || poll_timeout_ms > decant_kafka_client::KAFKA_STATIC_CONSUMER_MAX_POLL_TIMEOUT_MS_V0
    {
        return Err(KafkaJsonCliErrorV0::ConfigContract);
    }
    let cluster_id = KafkaClusterIdV0::new(&expected_cluster_id)
        .map_err(|_| KafkaJsonCliErrorV0::ConfigContract)?;
    let topic_id = decode_kafka_topic_id_v0(&expected_topic_id)?;
    let source_contract = KafkaSourceContractV0::new(
        KAFKA_SOURCE_CONTRACT_VERSION_V0,
        source_id.clone(),
        cluster_id,
        topic_id,
    )
    .map_err(|_| KafkaJsonCliErrorV0::ConfigContract)?;
    let limits = AdmissionLimitsV0::new(accepted_identity_limit, accepted_event_byte_limit)
        .map_err(|_| KafkaJsonCliErrorV0::ConfigContract)?;
    let admission = AdmissionConfigV0::new_with_source_position_kind(
        ADMISSION_VERSION_V0,
        source_id,
        SourcePositionKindV0::Kafka,
        epoch_start_ns,
        epoch_end_ns,
        limits,
    )
    .map_err(|_| KafkaJsonCliErrorV0::ConfigContract)?;

    let mut starts = Vec::new();
    starts
        .try_reserve_exact(partitions.len())
        .map_err(|_| KafkaJsonCliErrorV0::ConfigAllocation)?;
    for partition in partitions {
        starts.push((partition.partition, partition.start_offset));
    }
    let source =
        KafkaStaticSourceConfigV0::new(&bootstrap_servers, &topic, source_contract, &starts)
            .map_err(|_| KafkaJsonCliErrorV0::ConfigContract)?;

    Ok((
        source,
        admission,
        PathBuf::from(checkpoint_directory),
        Duration::from_millis(poll_timeout_ms),
    ))
}

fn decode_kafka_topic_id_v0(value: &str) -> Result<KafkaTopicIdV0, KafkaJsonCliErrorV0> {
    if value.len() != 22 {
        return Err(KafkaJsonCliErrorV0::ConfigContract);
    }
    let mut bytes = [0_u8; 16];
    let mut output_index = 0_usize;
    let mut residual = 0_u16;
    let mut residual_bits = 0_u8;
    for byte in value.bytes() {
        let sextet = match byte {
            b'A'..=b'Z' => byte - b'A',
            b'a'..=b'z' => byte - b'a' + 26,
            b'0'..=b'9' => byte - b'0' + 52,
            b'-' => 62,
            b'_' => 63,
            _ => return Err(KafkaJsonCliErrorV0::ConfigContract),
        };
        residual = (residual << 6) | u16::from(sextet);
        residual_bits += 6;
        if residual_bits >= 8 {
            residual_bits -= 8;
            if output_index >= bytes.len() {
                return Err(KafkaJsonCliErrorV0::ConfigContract);
            }
            bytes[output_index] = (residual >> residual_bits) as u8;
            output_index += 1;
            residual &= (1_u16 << residual_bits) - 1;
        }
    }
    if output_index != bytes.len() || residual_bits != 4 || residual != 0 {
        return Err(KafkaJsonCliErrorV0::ConfigContract);
    }
    KafkaTopicIdV0::from_bytes(bytes).map_err(|_| KafkaJsonCliErrorV0::ConfigContract)
}

fn run_kafka_live_config_v0(config: KafkaLiveConfigV0) -> Result<Vec<u8>, KafkaJsonCliErrorV0> {
    let KafkaLiveConfigV0 {
        source,
        admission,
        checkpoint_directory,
        poll_timeout,
        idle_poll_limit,
        max_records,
        input,
        output,
    } = config;
    let store = LocalCheckpointStoreV0::open(&checkpoint_directory)
        .map_err(|_| KafkaJsonCliErrorV0::CheckpointStore)?;
    let source_contract = source.source_contract().clone();
    let mut session = KafkaStaticCheckpointSessionV0::bootstrap(
        &store,
        admission,
        source_contract.clone(),
        source.starting_positions(),
    )
    .map_err(|_| KafkaJsonCliErrorV0::SessionBootstrap)?;

    let consumer =
        KafkaStaticConsumerV0::new(source).map_err(|_| KafkaJsonCliErrorV0::ClientCreation)?;
    observe_and_validate_kafka_source_v0(
        consumer.native_consumer(),
        consumer.config().topic_name(),
        &source_contract,
        source_contract.source_id(),
        poll_timeout,
    )
    .map_err(|_| KafkaJsonCliErrorV0::SourceObservation)?;
    consumer
        .assign_frontier(session.frontier())
        .map_err(|_| KafkaJsonCliErrorV0::Assignment)?;

    let mut poll_usage = KafkaLivePollUsageV0::default();
    let mut consecutive_idle = 0_usize;
    let termination = loop {
        if poll_usage.data_records >= max_records {
            break KafkaLiveTerminationV0::MaxRecords;
        }
        match consumer
            .poll(poll_timeout)
            .map_err(|_| KafkaJsonCliErrorV0::Poll)?
        {
            KafkaStaticPollV0::Data(record) => {
                match &input {
                    KafkaLiveInputContractV0::DecantJson => session.process_record(&record),
                    KafkaLiveInputContractV0::JsonObject { mapping } => {
                        session.process_json_object_record(mapping, &record)
                    }
                    KafkaLiveInputContractV0::JsonObjectV1 { mapping } => {
                        session.process_json_object_record_v1(mapping, &record)
                    }
                }
                .map_err(|_| KafkaJsonCliErrorV0::Processing)?;
                poll_usage.data_records = poll_usage
                    .data_records
                    .checked_add(1)
                    .ok_or(KafkaJsonCliErrorV0::Accounting)?;
                consecutive_idle = 0;
            }
            KafkaStaticPollV0::Idle => {
                poll_usage.idle_observations = poll_usage
                    .idle_observations
                    .checked_add(1)
                    .ok_or(KafkaJsonCliErrorV0::Accounting)?;
                consecutive_idle = consecutive_idle
                    .checked_add(1)
                    .ok_or(KafkaJsonCliErrorV0::Accounting)?;
            }
            KafkaStaticPollV0::PartitionEof { .. } => {
                poll_usage.partition_eof_observations = poll_usage
                    .partition_eof_observations
                    .checked_add(1)
                    .ok_or(KafkaJsonCliErrorV0::Accounting)?;
                consecutive_idle = consecutive_idle
                    .checked_add(1)
                    .ok_or(KafkaJsonCliErrorV0::Accounting)?;
            }
        }
        if consecutive_idle >= idle_poll_limit {
            break KafkaLiveTerminationV0::IdleLimit;
        }
    };

    finish_kafka_live_run_v0(&mut session, &store, termination, poll_usage, output)
}

fn finish_kafka_live_run_v0(
    session: &mut KafkaStaticCheckpointSessionV0,
    store: &LocalCheckpointStoreV0,
    termination: KafkaLiveTerminationV0,
    poll_usage: KafkaLivePollUsageV0,
    output: KafkaLiveOutputContractV0,
) -> Result<Vec<u8>, KafkaJsonCliErrorV0> {
    match output {
        KafkaLiveOutputContractV0::V0 => {
            let checkpoint = session
                .checkpoint(store)
                .map_err(|_| KafkaJsonCliErrorV0::Checkpoint)?;
            verify_run_accounting_v0(poll_usage, session.usage())?;
            encode_kafka_live_run_v0(termination, poll_usage, session.usage(), checkpoint)
        }
        KafkaLiveOutputContractV0::V1 { context } => {
            verify_run_accounting_v0(poll_usage, session.usage())?;
            let context = compile_kafka_activity_context_v0(session.admission(), context)
                .map_err(|_| KafkaJsonCliErrorV0::Context)?;
            let checkpoint = session
                .checkpoint(store)
                .map_err(|_| KafkaJsonCliErrorV0::Checkpoint)?;
            encode_kafka_live_run_v1(
                termination,
                poll_usage,
                session.usage(),
                checkpoint,
                &context,
            )
        }
        KafkaLiveOutputContractV0::V2 { context } => {
            verify_run_accounting_v0(poll_usage, session.usage())?;
            let context = compile_kafka_grouped_ratio_context_v0(session.admission(), &context)
                .map_err(|_| KafkaJsonCliErrorV0::Context)?;
            let checkpoint = session
                .checkpoint(store)
                .map_err(|_| KafkaJsonCliErrorV0::Checkpoint)?;
            encode_kafka_live_run_v2(
                termination,
                poll_usage,
                session.usage(),
                checkpoint,
                &context,
            )
        }
        KafkaLiveOutputContractV0::V3 { context } => {
            verify_run_accounting_v0(poll_usage, session.usage())?;
            let context = compile_kafka_material_change_context_v1(session.admission(), &context)
                .map_err(|_| KafkaJsonCliErrorV0::Context)?;
            let checkpoint = session
                .checkpoint(store)
                .map_err(|_| KafkaJsonCliErrorV0::Checkpoint)?;
            encode_kafka_live_run_v3(
                termination,
                poll_usage,
                session.usage(),
                checkpoint,
                &context,
            )
        }
    }
}

fn verify_run_accounting_v0(
    poll: KafkaLivePollUsageV0,
    session: KafkaStaticSessionUsageV0,
) -> Result<(), KafkaJsonCliErrorV0> {
    let classified_frontier = session
        .advanced_records()
        .checked_add(session.before_frontier_records())
        .ok_or(KafkaJsonCliErrorV0::Accounting)?;
    let classified_admission = session
        .accepted_records()
        .checked_add(session.duplicate_noop_records())
        .and_then(|value| value.checked_add(session.rejected_records()))
        .ok_or(KafkaJsonCliErrorV0::Accounting)?;
    if poll.data_records != session.processed_records()
        || classified_frontier != session.processed_records()
        || classified_admission != session.advanced_records()
    {
        return Err(KafkaJsonCliErrorV0::Accounting);
    }
    Ok(())
}

fn encode_kafka_live_run_v0(
    termination: KafkaLiveTerminationV0,
    poll: KafkaLivePollUsageV0,
    usage: KafkaStaticSessionUsageV0,
    checkpoint: KafkaStaticSessionCheckpointV0,
) -> Result<Vec<u8>, KafkaJsonCliErrorV0> {
    let mut output = BoundedOutputV0::new(MAX_KAFKA_LIVE_OUTPUT_BYTES_V0)?;
    write!(
        &mut output,
        concat!(
            "{{\"format\":\"decant-kafka-live-run\",\"version\":0,",
            "\"termination\":\"{}\",\"poll\":{{\"data_records\":{},",
            "\"idle_observations\":{},\"partition_eof_observations\":{}}},",
            "\"admission\":{{\"processed\":{},\"advanced\":{},\"accepted\":{},",
            "\"duplicate_noops\":{},\"rejected\":{},\"before_frontier\":{}}},",
            "\"omitted_transport_metadata\":{{\"header_count\":{},\"header_bytes\":{},",
            "\"timestamp_not_available_records\":{},\"timestamp_create_time_records\":{},",
            "\"timestamp_log_append_time_records\":{}}},\"checkpoint\":"
        ),
        termination.as_str(),
        poll.data_records,
        poll.idle_observations,
        poll.partition_eof_observations,
        usage.processed_records(),
        usage.advanced_records(),
        usage.accepted_records(),
        usage.duplicate_noop_records(),
        usage.rejected_records(),
        usage.before_frontier_records(),
        usage.omitted_header_count(),
        usage.omitted_header_bytes(),
        usage.omitted_timestamp_not_available_records(),
        usage.omitted_timestamp_create_time_records(),
        usage.omitted_timestamp_log_append_time_records(),
    )
    .map_err(|_| KafkaJsonCliErrorV0::OutputCapacity)?;
    match checkpoint {
        KafkaStaticSessionCheckpointV0::Unchanged {
            latest_generation,
            latest_checkpoint_hash,
        } => match (latest_generation, latest_checkpoint_hash) {
            (Some(generation), Some(hash)) => write!(
                &mut output,
                "{{\"published\":false,\"generation\":{},\"hash\":\"{}\",\"artifact_bytes\":null,\"pending_entries\":null}}",
                generation.get(),
                hash,
            ),
            (None, None) => output.write_str(
                "{\"published\":false,\"generation\":null,\"hash\":null,\"artifact_bytes\":null,\"pending_entries\":null}",
            ),
            _ => return Err(KafkaJsonCliErrorV0::Accounting),
        },
        KafkaStaticSessionCheckpointV0::Published {
            generation,
            checkpoint_hash,
            artifact_bytes,
            pending_entries,
        } => write!(
            &mut output,
            "{{\"published\":true,\"generation\":{},\"hash\":\"{}\",\"artifact_bytes\":{},\"pending_entries\":{}}}",
            generation.get(),
            checkpoint_hash,
            artifact_bytes,
            pending_entries,
        ),
    }
    .map_err(|_| KafkaJsonCliErrorV0::OutputCapacity)?;
    output
        .write_str(",\"broker_offsets_committed\":false,\"context_compiled\":false}\n")
        .map_err(|_| KafkaJsonCliErrorV0::OutputCapacity)?;
    Ok(output.finish())
}

fn encode_kafka_live_run_v1(
    termination: KafkaLiveTerminationV0,
    poll: KafkaLivePollUsageV0,
    usage: KafkaStaticSessionUsageV0,
    checkpoint: KafkaStaticSessionCheckpointV0,
    context: &KafkaActivityContextV0,
) -> Result<Vec<u8>, KafkaJsonCliErrorV0> {
    let mut output = BoundedOutputV0::new(MAX_KAFKA_LIVE_OUTPUT_BYTES_V1)?;
    write!(
        &mut output,
        concat!(
            "{{\"format\":\"decant-kafka-live-run\",\"version\":1,",
            "\"termination\":\"{}\",\"poll\":{{\"data_records\":{},",
            "\"idle_observations\":{},\"partition_eof_observations\":{}}},",
            "\"admission\":{{\"processed\":{},\"advanced\":{},\"accepted\":{},",
            "\"duplicate_noops\":{},\"rejected\":{},\"before_frontier\":{}}},",
            "\"omitted_transport_metadata\":{{\"header_count\":{},\"header_bytes\":{},",
            "\"timestamp_not_available_records\":{},\"timestamp_create_time_records\":{},",
            "\"timestamp_log_append_time_records\":{}}},\"checkpoint\":"
        ),
        termination.as_str(),
        poll.data_records,
        poll.idle_observations,
        poll.partition_eof_observations,
        usage.processed_records(),
        usage.advanced_records(),
        usage.accepted_records(),
        usage.duplicate_noop_records(),
        usage.rejected_records(),
        usage.before_frontier_records(),
        usage.omitted_header_count(),
        usage.omitted_header_bytes(),
        usage.omitted_timestamp_not_available_records(),
        usage.omitted_timestamp_create_time_records(),
        usage.omitted_timestamp_log_append_time_records(),
    )
    .map_err(|_| KafkaJsonCliErrorV0::OutputCapacity)?;
    write_checkpoint_v0(&mut output, checkpoint)?;
    output
        .write_str(",\"broker_offsets_committed\":false,\"context_compiled\":true,\"context\":")
        .map_err(|_| KafkaJsonCliErrorV0::OutputCapacity)?;
    output
        .write_bytes(context.as_bytes())
        .map_err(|_| KafkaJsonCliErrorV0::OutputCapacity)?;
    output
        .write_str("}\n")
        .map_err(|_| KafkaJsonCliErrorV0::OutputCapacity)?;
    Ok(output.finish())
}

fn encode_kafka_live_run_v2(
    termination: KafkaLiveTerminationV0,
    poll: KafkaLivePollUsageV0,
    usage: KafkaStaticSessionUsageV0,
    checkpoint: KafkaStaticSessionCheckpointV0,
    context: &KafkaGroupedRatioContextV0,
) -> Result<Vec<u8>, KafkaJsonCliErrorV0> {
    let mut output = BoundedOutputV0::new(MAX_KAFKA_LIVE_OUTPUT_BYTES_V2)?;
    write!(
        &mut output,
        concat!(
            "{{\"format\":\"decant-kafka-live-run\",\"version\":2,",
            "\"termination\":\"{}\",\"poll\":{{\"data_records\":{},",
            "\"idle_observations\":{},\"partition_eof_observations\":{}}},",
            "\"admission\":{{\"processed\":{},\"advanced\":{},\"accepted\":{},",
            "\"duplicate_noops\":{},\"rejected\":{},\"before_frontier\":{}}},",
            "\"omitted_transport_metadata\":{{\"header_count\":{},\"header_bytes\":{},",
            "\"timestamp_not_available_records\":{},\"timestamp_create_time_records\":{},",
            "\"timestamp_log_append_time_records\":{}}},\"checkpoint\":"
        ),
        termination.as_str(),
        poll.data_records,
        poll.idle_observations,
        poll.partition_eof_observations,
        usage.processed_records(),
        usage.advanced_records(),
        usage.accepted_records(),
        usage.duplicate_noop_records(),
        usage.rejected_records(),
        usage.before_frontier_records(),
        usage.omitted_header_count(),
        usage.omitted_header_bytes(),
        usage.omitted_timestamp_not_available_records(),
        usage.omitted_timestamp_create_time_records(),
        usage.omitted_timestamp_log_append_time_records(),
    )
    .map_err(|_| KafkaJsonCliErrorV0::OutputCapacity)?;
    write_checkpoint_v0(&mut output, checkpoint)?;
    output
        .write_str(",\"broker_offsets_committed\":false,\"context_compiled\":true,\"context\":")
        .map_err(|_| KafkaJsonCliErrorV0::OutputCapacity)?;
    output
        .write_bytes(context.as_bytes())
        .map_err(|_| KafkaJsonCliErrorV0::OutputCapacity)?;
    output
        .write_str("}\n")
        .map_err(|_| KafkaJsonCliErrorV0::OutputCapacity)?;
    Ok(output.finish())
}

fn encode_kafka_live_run_v3(
    termination: KafkaLiveTerminationV0,
    poll: KafkaLivePollUsageV0,
    usage: KafkaStaticSessionUsageV0,
    checkpoint: KafkaStaticSessionCheckpointV0,
    context: &KafkaMaterialChangeContextV0,
) -> Result<Vec<u8>, KafkaJsonCliErrorV0> {
    let mut output = BoundedOutputV0::new(MAX_KAFKA_LIVE_OUTPUT_BYTES_V3)?;
    write!(
        &mut output,
        concat!(
            "{{\"format\":\"decant-kafka-live-run\",\"version\":3,",
            "\"termination\":\"{}\",\"poll\":{{\"data_records\":{},",
            "\"idle_observations\":{},\"partition_eof_observations\":{}}},",
            "\"admission\":{{\"processed\":{},\"advanced\":{},\"accepted\":{},",
            "\"duplicate_noops\":{},\"rejected\":{},\"before_frontier\":{}}},",
            "\"omitted_transport_metadata\":{{\"header_count\":{},\"header_bytes\":{},",
            "\"timestamp_not_available_records\":{},\"timestamp_create_time_records\":{},",
            "\"timestamp_log_append_time_records\":{}}},\"checkpoint\":"
        ),
        termination.as_str(),
        poll.data_records,
        poll.idle_observations,
        poll.partition_eof_observations,
        usage.processed_records(),
        usage.advanced_records(),
        usage.accepted_records(),
        usage.duplicate_noop_records(),
        usage.rejected_records(),
        usage.before_frontier_records(),
        usage.omitted_header_count(),
        usage.omitted_header_bytes(),
        usage.omitted_timestamp_not_available_records(),
        usage.omitted_timestamp_create_time_records(),
        usage.omitted_timestamp_log_append_time_records(),
    )
    .map_err(|_| KafkaJsonCliErrorV0::OutputCapacity)?;
    write_checkpoint_v0(&mut output, checkpoint)?;
    output
        .write_str(
            ",\"broker_offsets_committed\":false,\"source_ownership\":\"static_partitions\",\"context_compiled\":true,\"context\":",
        )
        .map_err(|_| KafkaJsonCliErrorV0::OutputCapacity)?;
    output
        .write_bytes(context.as_bytes())
        .map_err(|_| KafkaJsonCliErrorV0::OutputCapacity)?;
    output
        .write_str("}\n")
        .map_err(|_| KafkaJsonCliErrorV0::OutputCapacity)?;
    Ok(output.finish())
}

fn write_checkpoint_v0(
    output: &mut BoundedOutputV0,
    checkpoint: KafkaStaticSessionCheckpointV0,
) -> Result<(), KafkaJsonCliErrorV0> {
    match checkpoint {
        KafkaStaticSessionCheckpointV0::Unchanged {
            latest_generation,
            latest_checkpoint_hash,
        } => match (latest_generation, latest_checkpoint_hash) {
            (Some(generation), Some(hash)) => write!(
                output,
                "{{\"published\":false,\"generation\":{},\"hash\":\"{}\",\"artifact_bytes\":null,\"pending_entries\":null}}",
                generation.get(),
                hash,
            ),
            (None, None) => output.write_str(
                "{\"published\":false,\"generation\":null,\"hash\":null,\"artifact_bytes\":null,\"pending_entries\":null}",
            ),
            _ => return Err(KafkaJsonCliErrorV0::Accounting),
        },
        KafkaStaticSessionCheckpointV0::Published {
            generation,
            checkpoint_hash,
            artifact_bytes,
            pending_entries,
        } => write!(
            output,
            "{{\"published\":true,\"generation\":{},\"hash\":\"{}\",\"artifact_bytes\":{},\"pending_entries\":{}}}",
            generation.get(),
            checkpoint_hash,
            artifact_bytes,
            pending_entries,
        ),
    }
    .map_err(|_| KafkaJsonCliErrorV0::OutputCapacity)
}

struct BoundedOutputV0 {
    bytes: Vec<u8>,
    maximum: usize,
}

impl BoundedOutputV0 {
    fn new(maximum: usize) -> Result<Self, KafkaJsonCliErrorV0> {
        let mut bytes = Vec::new();
        bytes
            .try_reserve_exact(maximum)
            .map_err(|_| KafkaJsonCliErrorV0::OutputAllocation)?;
        Ok(Self { bytes, maximum })
    }

    fn finish(self) -> Vec<u8> {
        self.bytes
    }

    fn write_bytes(&mut self, value: &[u8]) -> fmt::Result {
        let next_len = self
            .bytes
            .len()
            .checked_add(value.len())
            .ok_or(fmt::Error)?;
        if next_len > self.maximum {
            return Err(fmt::Error);
        }
        self.bytes.extend_from_slice(value);
        Ok(())
    }
}

impl fmt::Write for BoundedOutputV0 {
    fn write_str(&mut self, value: &str) -> fmt::Result {
        let next_len = self
            .bytes
            .len()
            .checked_add(value.len())
            .ok_or(fmt::Error)?;
        if next_len > self.maximum {
            return Err(fmt::Error);
        }
        self.bytes.extend_from_slice(value.as_bytes());
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::sync::atomic::{AtomicU64, Ordering};

    use decant_ingest::KafkaCheckpointGenerationV0;
    use decant_ir::{KafkaPositionV0, SchemaIdV0};
    use decant_kafka::{
        KafkaFetchedRecordV0, KafkaPartitionFrontierV0, KafkaRecordHeaderV0, KafkaRecordTimestampV0,
    };
    use decant_kafka_live::KafkaStaticSessionCheckpointV0;
    use decant_state::{AdmissionStateV0, MAX_ACCEPTED_EVENT_BYTES_V0, MAX_ACCEPTED_IDENTITIES_V0};

    use super::*;

    static NEXT_TEMP: AtomicU64 = AtomicU64::new(0);

    fn document() -> Vec<u8> {
        concat!(
            "{\"format\":\"decant-kafka-live-config\",\"version\":0,",
            "\"bootstrap_servers\":\"127.0.0.1:9092\",\"topic\":\"decant-events\",",
            "\"source_id\":\"live-source\",\"expected_cluster_id\":\"cluster-v0\",",
            "\"expected_topic_id\":\"AQEBAQEBAQEBAQEBAQEBAQ\",",
            "\"partitions\":[{\"partition\":0,\"start_offset\":0}],",
            "\"checkpoint_directory\":\"/tmp/decant-checkpoints\",",
            "\"epoch_start_ns\":0,\"epoch_end_ns\":1000,",
            "\"accepted_identity_limit\":100,\"accepted_event_byte_limit\":100000,",
            "\"poll_timeout_ms\":10,\"idle_poll_limit\":2,\"max_records\":3}"
        )
        .as_bytes()
        .to_vec()
    }

    fn with_field(name: &str, value: serde_json::Value) -> Vec<u8> {
        let mut changed: serde_json::Value = serde_json::from_slice(&document()).unwrap();
        changed[name] = value;
        serde_json::to_vec(&changed).unwrap()
    }

    fn document_v1(byte_budget: usize) -> Vec<u8> {
        let mut changed: serde_json::Value = serde_json::from_slice(&document()).unwrap();
        changed["version"] = serde_json::json!(1);
        changed["context"] = serde_json::json!({
            "format": "decant-kafka-activity-context-request",
            "version": 0,
            "byte_budget": byte_budget,
        });
        serde_json::to_vec(&changed).unwrap()
    }

    fn document_v2(byte_budget: usize) -> Vec<u8> {
        let mut changed: serde_json::Value =
            serde_json::from_slice(&document_v1(byte_budget)).unwrap();
        changed["version"] = serde_json::json!(2);
        changed.as_object_mut().unwrap().remove("source_id");
        changed["source_namespace"] = serde_json::json!("private-source-namespace");
        changed["epoch_end_ns"] = serde_json::json!(1_000_000_000_i64);
        changed["mapping"] = serde_json::json!({
            "format": "decant-kafka-json-object-mapping",
            "version": 0,
            "event_time": {
                "field": "observed_time",
                "unit": "milliseconds",
            },
            "payload": [
                {"source": "raw_ok", "target": "ok", "type": "bool"},
                {"source": "raw_service", "target": "service", "type": "utf8"},
            ],
        });
        serde_json::to_vec(&changed).unwrap()
    }

    fn document_v3(byte_budget: usize) -> Vec<u8> {
        let mut changed: serde_json::Value =
            serde_json::from_slice(&document_v2(byte_budget)).unwrap();
        changed["version"] = serde_json::json!(3);
        changed["context"] = serde_json::json!({
            "format": "decant-kafka-grouped-ratio-context-request",
            "version": 0,
            "fact_id": "error-rate-by-service",
            "group_field": "service",
            "predicate": {"field": "ok", "equals": false},
            "lookback_ns": 600_000_000_u64,
            "evidence_limit": 3,
            "byte_budget": byte_budget,
        });
        changed["mapping"]["payload"] = serde_json::json!([
            {"source": "raw_ok", "target": "ok", "type": "bool"},
            {"source": "raw_secret", "target": "secret", "type": "utf8"},
            {"source": "raw_service", "target": "service", "type": "utf8"},
        ]);
        changed["egress"] = serde_json::json!({
            "format": "decant-kafka-field-egress-policy",
            "version": 0,
            "fields": [
                {"field": "service", "action": "allow"},
                {"field": "ok", "action": "allow"},
                {"field": "secret", "action": "deny"},
            ],
        });
        serde_json::to_vec(&changed).unwrap()
    }

    fn document_v4(byte_budget: usize) -> Vec<u8> {
        let mut changed: serde_json::Value =
            serde_json::from_slice(&document_v3(byte_budget)).unwrap();
        changed["version"] = serde_json::json!(4);
        changed["context"] = serde_json::json!({
            "format": "decant-kafka-material-change-context-request",
            "version": 1,
            "view_id": "service-latency",
            "retention_ns": 600_000_000_u64,
            "discriminator_field": "series",
            "value_field": "value",
            "item_limit": 16,
            "byte_budget": byte_budget,
            "entity_key_egress": "allow_lower_hex",
        });
        changed["mapping"] = serde_json::json!({
            "format": "decant-kafka-json-object-mapping",
            "version": 1,
            "entity_key": {"source": "utf8_projection", "field": "service"},
            "event_time": {
                "field": "observed_time",
                "unit": "milliseconds",
            },
            "payload": [
                {"source": "raw_secret", "target": "secret", "type": "utf8"},
                {"source": "raw_series", "target": "series", "type": "utf8"},
                {"source": "raw_service", "target": "service", "type": "utf8"},
                {"source": "raw_value", "target": "value", "type": "u64"},
            ],
        });
        changed["egress"] = serde_json::json!({
            "format": "decant-kafka-field-egress-policy",
            "version": 0,
            "fields": [
                {"field": "secret", "action": "deny"},
                {"field": "series", "action": "allow"},
                {"field": "service", "action": "allow"},
                {"field": "value", "action": "allow"},
            ],
        });
        serde_json::to_vec(&changed).unwrap()
    }

    fn mcp_service_document(security: serde_json::Value) -> Vec<u8> {
        let mut changed: serde_json::Value = serde_json::from_slice(&document_v4(4_096)).unwrap();
        changed["format"] = serde_json::json!("decant-kafka-mcp-service-config");
        changed["version"] = serde_json::json!(0);
        changed.as_object_mut().unwrap().remove("idle_poll_limit");
        changed.as_object_mut().unwrap().remove("max_records");
        changed["lifecycle"] = serde_json::json!({
            "version": 0,
            "records_between_checkpoints": 2,
            "idle_observations_between_checkpoints": 3,
            "retained_checkpoint_generations": 4,
        });
        changed["security"] = security;
        serde_json::to_vec(&changed).unwrap()
    }

    fn test_directory(label: &str) -> PathBuf {
        let sequence = NEXT_TEMP.fetch_add(1, Ordering::Relaxed);
        let path = std::env::temp_dir().join(format!(
            "decant-kafka-cli-context-{label}-{}-{sequence}",
            std::process::id()
        ));
        fs::create_dir(&path).unwrap();
        path
    }

    fn mapped_record(offset: i64, event_time_ms: i64) -> KafkaFetchedRecordV0 {
        let value = format!(
            concat!(
                "{{\"observed_time\":{},\"raw_ok\":true,",
                "\"raw_service\":\"private-selected-value\",",
                "\"private_unselected\":\"private-unselected-value\"}}"
            ),
            event_time_ms
        );
        let headers =
            [KafkaRecordHeaderV0::new("private-header", Some(b"private-header-value")).unwrap()];
        KafkaFetchedRecordV0::new(
            KafkaPositionV0::new(0, offset).unwrap(),
            Some(b"private-key"),
            Some(value.as_bytes()),
            KafkaRecordTimestampV0::CreateTime {
                unix_ms: event_time_ms,
            },
            &headers,
        )
        .unwrap()
    }

    fn semantic_record(
        offset: i64,
        event_time_ms: i64,
        service: &str,
        ok: bool,
        secret: &str,
    ) -> KafkaFetchedRecordV0 {
        let value = format!(
            "{{\"observed_time\":{event_time_ms},\"raw_ok\":{ok},\"raw_secret\":\"{secret}\",\"raw_service\":\"{service}\",\"private_unselected\":\"private-unselected-value\"}}"
        );
        let headers =
            [KafkaRecordHeaderV0::new("private-header", Some(b"private-header-value")).unwrap()];
        KafkaFetchedRecordV0::new(
            KafkaPositionV0::new(0, offset).unwrap(),
            Some(b"private-key"),
            Some(value.as_bytes()),
            KafkaRecordTimestampV0::CreateTime {
                unix_ms: event_time_ms,
            },
            &headers,
        )
        .unwrap()
    }

    fn material_record(offset: i64, value: u64) -> KafkaFetchedRecordV0 {
        let bytes = format!(
            "{{\"observed_time\":{},\"raw_secret\":\"private-material-secret\",\"raw_series\":\"latency\",\"raw_service\":\"checkout\",\"raw_value\":{value},\"private_unselected\":\"private-unselected-value\"}}",
            100 + offset
        );
        let headers =
            [KafkaRecordHeaderV0::new("private-header", Some(b"private-header-value")).unwrap()];
        KafkaFetchedRecordV0::new(
            KafkaPositionV0::new(0, offset).unwrap(),
            Some(b"private-ignored-key"),
            Some(bytes.as_bytes()),
            KafkaRecordTimestampV0::CreateTime { unix_ms: 100 },
            &headers,
        )
        .unwrap()
    }

    #[test]
    fn fixed_topic_id_decoder_accepts_only_canonical_nonzero_uuid_text() {
        assert_eq!(
            decode_kafka_topic_id_v0("AQEBAQEBAQEBAQEBAQEBAQ")
                .unwrap()
                .as_bytes(),
            &[1; 16]
        );
        assert_eq!(
            decode_kafka_topic_id_v0("_____________________w")
                .unwrap()
                .as_bytes(),
            &[u8::MAX; 16]
        );
        for invalid in [
            "AQEBAQEBAQEBAQEBAQEBAR",
            "AQEBAQEBAQEBAQEBAQEBA=",
            "AQEBAQEBAQEBAQEBAQEBA",
            "AAAAAAAAAAAAAAAAAAAAAA",
        ] {
            assert_eq!(
                decode_kafka_topic_id_v0(invalid),
                Err(KafkaJsonCliErrorV0::ConfigContract)
            );
        }
    }

    #[test]
    fn strict_config_parses_exact_shape_and_redacts_values() {
        let config = parse_kafka_live_config_v0(&document()).unwrap();
        assert_eq!(config.source.starting_positions()[0].partition(), 0);
        assert_eq!(config.source.starting_positions()[0].offset(), 0);
        assert_eq!(config.poll_timeout, Duration::from_millis(10));
        assert_eq!(config.idle_poll_limit, 2);
        assert_eq!(config.max_records, 3);
        let debug = format!("{config:?}");
        assert!(!debug.contains("127.0.0.1"));
        assert!(!debug.contains("decant-events"));
        assert!(!debug.contains("live-source"));
        assert!(!debug.contains("/tmp/"));

        let mut unknown = document();
        let close = unknown.pop().unwrap();
        assert_eq!(close, b'}');
        unknown.extend_from_slice(b",\"private_unknown\":1}");
        assert_eq!(
            parse_kafka_live_config_v0(&unknown).unwrap_err(),
            KafkaJsonCliErrorV0::ConfigSyntax
        );
        assert_eq!(
            parse_kafka_live_config_v0(&vec![b'x'; MAX_KAFKA_LIVE_CONFIG_BYTES_V0 + 1])
                .unwrap_err(),
            KafkaJsonCliErrorV0::ConfigTooLarge
        );
    }

    #[test]
    fn mcp_service_config_is_strict_bounded_and_secret_referenced() {
        let plaintext = parse_kafka_mcp_service_config_v0(&mcp_service_document(
            serde_json::json!({"mode":"plaintext","version":0}),
        ))
        .unwrap();
        assert_eq!(plaintext.poll_timeout, Duration::from_millis(10));
        assert_eq!(plaintext.context.view_id(), "service-latency");
        assert_eq!(plaintext.lifecycle.records_between_checkpoints(), 2);
        assert_eq!(
            plaintext.lifecycle.physical_retention(),
            plaintext.context.retention()
        );
        assert_eq!(
            plaintext.security.mode(),
            decant_kafka_client::KafkaStaticSecurityModeV0::Plaintext
        );

        for security in [
            serde_json::json!({"mode":"tls","version":0,"ca_location":null}),
            serde_json::json!({
                "mode":"tls_sasl_scram",
                "version":0,
                "ca_location":"/private/synthetic-ca.pem",
                "mechanism":"sha512",
                "username_env":"DECANT_TEST_KAFKA_USER",
                "password_env":"DECANT_TEST_KAFKA_PASSWORD"
            }),
        ] {
            assert!(parse_kafka_mcp_service_config_v0(&mcp_service_document(security)).is_ok());
        }

        let mut unknown: serde_json::Value = serde_json::from_slice(&mcp_service_document(
            serde_json::json!({"mode":"plaintext","version":0}),
        ))
        .unwrap();
        unknown["private_unknown"] = serde_json::json!("private-value");
        assert_eq!(
            parse_kafka_mcp_service_config_v0(&serde_json::to_vec(&unknown).unwrap()).unwrap_err(),
            KafkaJsonCliErrorV0::ConfigSyntax
        );
        assert_eq!(
            parse_kafka_mcp_service_config_v0(&vec![b'x'; MAX_KAFKA_LIVE_CONFIG_BYTES_V0 + 1])
                .unwrap_err(),
            KafkaJsonCliErrorV0::ConfigTooLarge
        );
        let debug = format!("{plaintext:?}");
        for denied in [
            "127.0.0.1",
            "decant-events",
            "/tmp/",
            "service-latency",
            "private",
        ] {
            assert!(!debug.contains(denied));
        }
    }

    #[test]
    fn mcp_service_config_rejects_raw_or_invalid_secret_shapes() {
        for security in [
            serde_json::json!({
                "mode":"tls_sasl_scram","version":0,"ca_location":null,
                "mechanism":"sha256","username":"private-user","password":"private-password"
            }),
            serde_json::json!({
                "mode":"tls_sasl_scram","version":0,"ca_location":null,
                "mechanism":"sha256","username_env":"lowercase","password_env":"DECANT_PASSWORD"
            }),
            serde_json::json!({
                "mode":"tls_sasl_scram","version":0,"ca_location":null,
                "mechanism":"sha256","username_env":"DECANT_SAME","password_env":"DECANT_SAME"
            }),
            serde_json::json!({"mode":"tls","version":0,"ca_location":"relative.pem"}),
        ] {
            assert!(parse_kafka_mcp_service_config_v0(&mcp_service_document(security)).is_err());
        }

        let mut invalid: serde_json::Value = serde_json::from_slice(&mcp_service_document(
            serde_json::json!({"mode":"plaintext","version":0}),
        ))
        .unwrap();
        invalid["lifecycle"]["records_between_checkpoints"] = serde_json::json!(0);
        assert_eq!(
            parse_kafka_mcp_service_config_v0(&serde_json::to_vec(&invalid).unwrap()).unwrap_err(),
            KafkaJsonCliErrorV0::ConfigContract
        );
    }

    #[test]
    fn config_versions_dispatch_to_exact_strict_documents() {
        assert!(matches!(
            parse_kafka_live_config(&document()).unwrap().output,
            KafkaLiveOutputContractV0::V0
        ));
        let v1 = parse_kafka_live_config(&document_v1(512)).unwrap();
        assert_eq!(
            v1.output,
            KafkaLiveOutputContractV0::V1 {
                context: KafkaActivityContextRequestV0::new(0, 512).unwrap(),
            }
        );
        let v2 = parse_kafka_live_config(&document_v2(512)).unwrap();
        assert_eq!(
            v2.output,
            KafkaLiveOutputContractV0::V1 {
                context: KafkaActivityContextRequestV0::new(0, 512).unwrap(),
            }
        );
        let KafkaLiveInputContractV0::JsonObject { mapping } = &v2.input else {
            panic!("V2 must select JSON-object mapping");
        };
        assert_eq!(
            mapping.schema_id().as_str(),
            "2efb7cb9f6816c71fb5f3371ef8903d70a41afe80f80929fc37a64d59896dac6"
        );
        assert_eq!(
            mapping.effective_source_id().as_str(),
            "8a272b4ed412e12d1d7e4cd4810bf8dc59d08e82962db7fa8c897cbbb1211991"
        );
        assert_eq!(
            v2.source.source_contract().source_id(),
            mapping.effective_source_id()
        );
        let v3 = parse_kafka_live_config(&document_v3(4_096)).unwrap();
        let KafkaLiveOutputContractV0::V2 { context } = &v3.output else {
            panic!("V3 must select grouped-ratio output");
        };
        assert_eq!(context.fact_id(), "error-rate-by-service");
        assert_eq!(context.group_field(), "service");
        assert_eq!(context.predicate_field(), "ok");
        assert!(!context.predicate_value());
        assert_eq!(context.policy().allowed_fields(), 2);
        assert_eq!(context.policy().denied_fields(), 1);
        let v4 = parse_kafka_live_config(&document_v4(4_096)).unwrap();
        let KafkaLiveInputContractV0::JsonObjectV1 { mapping } = &v4.input else {
            panic!("V4 must select entity-key-aware JSON-object mapping");
        };
        assert_eq!(
            mapping.entity_key_source().projection_target(),
            Some("service")
        );
        let KafkaLiveOutputContractV0::V3 { context } = &v4.output else {
            panic!("V4 must select material-change output");
        };
        assert_eq!(context.version(), 1);
        assert_eq!(context.view_id(), "service-latency");
        assert_eq!(context.discriminator_field(), "series");
        assert_eq!(context.value_field(), "value");
        assert_eq!(
            context.entity_key_action(),
            KafkaEntityKeyEgressActionV0::AllowHex
        );

        let mut v0_with_context: serde_json::Value = serde_json::from_slice(&document()).unwrap();
        v0_with_context["context"] = serde_json::json!({
            "format": "decant-kafka-activity-context-request",
            "version": 0,
            "byte_budget": 512,
        });
        assert_eq!(
            parse_kafka_live_config(&serde_json::to_vec(&v0_with_context).unwrap()).unwrap_err(),
            KafkaJsonCliErrorV0::ConfigSyntax
        );

        let mut v1_missing: serde_json::Value = serde_json::from_slice(&document()).unwrap();
        v1_missing["version"] = serde_json::json!(1);
        assert_eq!(
            parse_kafka_live_config(&serde_json::to_vec(&v1_missing).unwrap()).unwrap_err(),
            KafkaJsonCliErrorV0::ConfigSyntax
        );
        let mut v1_unknown_nested: serde_json::Value =
            serde_json::from_slice(&document_v1(512)).unwrap();
        v1_unknown_nested["context"]["private_unknown"] = serde_json::json!(1);
        assert_eq!(
            parse_kafka_live_config(&serde_json::to_vec(&v1_unknown_nested).unwrap()).unwrap_err(),
            KafkaJsonCliErrorV0::ConfigSyntax
        );
        let mut unknown_version: serde_json::Value = serde_json::from_slice(&document()).unwrap();
        unknown_version["version"] = serde_json::json!(5);
        assert_eq!(
            parse_kafka_live_config(&serde_json::to_vec(&unknown_version).unwrap()).unwrap_err(),
            KafkaJsonCliErrorV0::ConfigContract
        );
        assert_eq!(
            parse_kafka_live_config(
                br#"{"format":"decant-kafka-live-config","version":0,"version":1}"#
            )
            .unwrap_err(),
            KafkaJsonCliErrorV0::ConfigSyntax
        );
    }

    #[test]
    fn v2_mapping_document_is_strict_and_reuses_task_070_validation() {
        let valid = document_v2(512);
        assert!(parse_kafka_live_config(&valid).is_ok());

        let mut unknown_top: serde_json::Value = serde_json::from_slice(&valid).unwrap();
        unknown_top["private_unknown"] = serde_json::json!(1);
        let mut mixed_source: serde_json::Value = serde_json::from_slice(&valid).unwrap();
        mixed_source["source_id"] = serde_json::json!("old-source");
        let mut missing_namespace: serde_json::Value = serde_json::from_slice(&valid).unwrap();
        missing_namespace
            .as_object_mut()
            .unwrap()
            .remove("source_namespace");
        let mut unknown_mapping: serde_json::Value = serde_json::from_slice(&valid).unwrap();
        unknown_mapping["mapping"]["private_unknown"] = serde_json::json!(1);
        let mut missing_event_field: serde_json::Value = serde_json::from_slice(&valid).unwrap();
        missing_event_field["mapping"]["event_time"]
            .as_object_mut()
            .unwrap()
            .remove("field");
        let mut unknown_unit: serde_json::Value = serde_json::from_slice(&valid).unwrap();
        unknown_unit["mapping"]["event_time"]["unit"] = serde_json::json!("minutes");
        let mut unknown_kind: serde_json::Value = serde_json::from_slice(&valid).unwrap();
        unknown_kind["mapping"]["payload"][0]["type"] = serde_json::json!("number");
        for invalid in [
            unknown_top,
            mixed_source,
            missing_namespace,
            unknown_mapping,
            missing_event_field,
            unknown_unit,
            unknown_kind,
        ] {
            assert_eq!(
                parse_kafka_live_config(&serde_json::to_vec(&invalid).unwrap()).unwrap_err(),
                KafkaJsonCliErrorV0::ConfigSyntax
            );
        }

        let mut wrong_mapping_version: serde_json::Value = serde_json::from_slice(&valid).unwrap();
        wrong_mapping_version["mapping"]["version"] = serde_json::json!(1);
        let mut duplicate_source: serde_json::Value = serde_json::from_slice(&valid).unwrap();
        duplicate_source["mapping"]["payload"][1]["source"] = serde_json::json!("raw_ok");
        let mut duplicate_target: serde_json::Value = serde_json::from_slice(&valid).unwrap();
        duplicate_target["mapping"]["payload"][1]["target"] = serde_json::json!("ok");
        let mut invalid_event_name: serde_json::Value = serde_json::from_slice(&valid).unwrap();
        invalid_event_name["mapping"]["event_time"]["field"] = serde_json::json!("");
        for invalid in [
            wrong_mapping_version,
            duplicate_source,
            duplicate_target,
            invalid_event_name,
        ] {
            assert_eq!(
                parse_kafka_live_config(&serde_json::to_vec(&invalid).unwrap()).unwrap_err(),
                KafkaJsonCliErrorV0::ConfigContract
            );
        }
    }

    #[test]
    fn v2_translates_every_mapping_unit_and_scalar_kind() {
        let mut document: serde_json::Value = serde_json::from_slice(&document_v2(512)).unwrap();
        for unit in ["nanoseconds", "microseconds", "milliseconds", "seconds"] {
            document["mapping"]["event_time"]["unit"] = serde_json::json!(unit);
            assert!(parse_kafka_live_config(&serde_json::to_vec(&document).unwrap()).is_ok());
        }
        document["mapping"]["payload"] = serde_json::json!([
            {"source": "a", "target": "a", "type": "null"},
            {"source": "b", "target": "b", "type": "bool"},
            {"source": "c", "target": "c", "type": "i64"},
            {"source": "d", "target": "d", "type": "u64"},
            {"source": "e", "target": "e", "type": "utf8"},
        ]);
        assert!(parse_kafka_live_config(&serde_json::to_vec(&document).unwrap()).is_ok());

        document["mapping"]["payload"] = serde_json::Value::Array(
            (0..=decant_kafka::MAX_KAFKA_JSON_OBJECT_PROJECTIONS_V0)
                .map(|index| {
                    serde_json::json!({
                        "source": format!("source_{index}"),
                        "target": format!("target_{index}"),
                        "type": "u64",
                    })
                })
                .collect(),
        );
        assert_eq!(
            parse_kafka_live_config(&serde_json::to_vec(&document).unwrap()).unwrap_err(),
            KafkaJsonCliErrorV0::ConfigContract
        );
    }

    #[test]
    fn v3_policy_is_complete_strict_and_matches_semantic_field_kinds() {
        let valid = document_v3(4_096);
        assert!(parse_kafka_live_config(&valid).is_ok());

        let mut missing_rule: serde_json::Value = serde_json::from_slice(&valid).unwrap();
        missing_rule["egress"]["fields"]
            .as_array_mut()
            .unwrap()
            .pop();
        let mut unknown_rule: serde_json::Value = serde_json::from_slice(&valid).unwrap();
        unknown_rule["egress"]["fields"][2]["field"] = serde_json::json!("not_mapped");
        let mut duplicate_rule: serde_json::Value = serde_json::from_slice(&valid).unwrap();
        duplicate_rule["egress"]["fields"][2]["field"] = serde_json::json!("service");
        let mut denied_group: serde_json::Value = serde_json::from_slice(&valid).unwrap();
        denied_group["egress"]["fields"][0]["action"] = serde_json::json!("deny");
        let mut wrong_group_kind: serde_json::Value = serde_json::from_slice(&valid).unwrap();
        wrong_group_kind["mapping"]["payload"][2]["type"] = serde_json::json!("u64");
        let mut wrong_predicate_kind: serde_json::Value = serde_json::from_slice(&valid).unwrap();
        wrong_predicate_kind["mapping"]["payload"][0]["type"] = serde_json::json!("utf8");
        for invalid in [
            missing_rule,
            unknown_rule,
            duplicate_rule,
            denied_group,
            wrong_group_kind,
            wrong_predicate_kind,
        ] {
            assert_eq!(
                parse_kafka_live_config(&serde_json::to_vec(&invalid).unwrap()).unwrap_err(),
                KafkaJsonCliErrorV0::ConfigContract
            );
        }

        for budget in [512, 16 * 1_024] {
            assert!(parse_kafka_live_config(&document_v3(budget)).is_ok());
        }
        for budget in [511, 16 * 1_024 + 1] {
            assert_eq!(
                parse_kafka_live_config(&document_v3(budget)).unwrap_err(),
                KafkaJsonCliErrorV0::ConfigContract
            );
        }
        let mut unknown_context: serde_json::Value = serde_json::from_slice(&valid).unwrap();
        unknown_context["context"]["private_unknown"] = serde_json::json!(1);
        assert_eq!(
            parse_kafka_live_config(&serde_json::to_vec(&unknown_context).unwrap()).unwrap_err(),
            KafkaJsonCliErrorV0::ConfigSyntax
        );
    }

    #[test]
    fn v4_entity_key_material_change_and_policy_contract_is_strict() {
        let valid = document_v4(4_096);
        assert!(parse_kafka_live_config(&valid).is_ok());

        let mut denied_selected: serde_json::Value = serde_json::from_slice(&valid).unwrap();
        denied_selected["egress"]["fields"][3]["action"] = serde_json::json!("deny");
        let mut denied_derived_key: serde_json::Value = serde_json::from_slice(&valid).unwrap();
        denied_derived_key["egress"]["fields"][2]["action"] = serde_json::json!("deny");
        let mut wrong_discriminator: serde_json::Value = serde_json::from_slice(&valid).unwrap();
        wrong_discriminator["mapping"]["payload"][1]["type"] = serde_json::json!("u64");
        let mut missing_value: serde_json::Value = serde_json::from_slice(&valid).unwrap();
        missing_value["context"]["value_field"] = serde_json::json!("not_mapped");
        let mut wrong_mapping_version: serde_json::Value = serde_json::from_slice(&valid).unwrap();
        wrong_mapping_version["mapping"]["version"] = serde_json::json!(0);
        let mut missing_key_source: serde_json::Value = serde_json::from_slice(&valid).unwrap();
        missing_key_source["mapping"]["entity_key"]
            .as_object_mut()
            .unwrap()
            .remove("source");
        let mut wrong_context_version: serde_json::Value = serde_json::from_slice(&valid).unwrap();
        wrong_context_version["context"]["version"] = serde_json::json!(0);
        for invalid in [
            denied_selected,
            denied_derived_key,
            wrong_discriminator,
            missing_value,
            wrong_mapping_version,
            missing_key_source,
            wrong_context_version,
        ] {
            assert!(parse_kafka_live_config(&serde_json::to_vec(&invalid).unwrap()).is_err());
        }

        let mut denied_key_is_valid: serde_json::Value = serde_json::from_slice(&valid).unwrap();
        denied_key_is_valid["egress"]["fields"][2]["action"] = serde_json::json!("deny");
        denied_key_is_valid["context"]["entity_key_egress"] = serde_json::json!("deny");
        assert!(
            parse_kafka_live_config(&serde_json::to_vec(&denied_key_is_valid).unwrap()).is_ok()
        );

        for budget in [768, 16 * 1_024] {
            assert!(parse_kafka_live_config(&document_v4(budget)).is_ok());
        }
        for budget in [767, 16 * 1_024 + 1] {
            assert_eq!(
                parse_kafka_live_config(&document_v4(budget)).unwrap_err(),
                KafkaJsonCliErrorV0::ConfigContract
            );
        }
    }

    #[test]
    fn context_request_boundaries_are_exact_and_strict() {
        for valid in [512, 16 * 1_024] {
            assert!(parse_kafka_live_config(&document_v1(valid)).is_ok());
        }
        for invalid in [511, 16 * 1_024 + 1] {
            assert_eq!(
                parse_kafka_live_config(&document_v1(invalid)).unwrap_err(),
                KafkaJsonCliErrorV0::ConfigContract
            );
        }
        for (name, value) in [
            ("format", serde_json::json!("private-wrong")),
            ("version", serde_json::json!(1)),
        ] {
            let mut changed: serde_json::Value = serde_json::from_slice(&document_v1(512)).unwrap();
            changed["context"][name] = value;
            assert_eq!(
                parse_kafka_live_config(&serde_json::to_vec(&changed).unwrap()).unwrap_err(),
                KafkaJsonCliErrorV0::ConfigContract
            );
        }
    }

    #[test]
    fn every_operational_bound_is_nonzero_and_capped() {
        for invalid in [
            with_field("format", serde_json::json!("wrong")),
            with_field("version", serde_json::json!(1)),
            with_field("bootstrap_servers", serde_json::json!("")),
            with_field("bootstrap_servers", serde_json::json!("h".repeat(1_025))),
            with_field("topic", serde_json::json!("..")),
            with_field("topic", serde_json::json!("t".repeat(250))),
            with_field("source_id", serde_json::json!("invalid source")),
            with_field("expected_cluster_id", serde_json::json!("")),
            with_field(
                "expected_topic_id",
                serde_json::json!("AAAAAAAAAAAAAAAAAAAAAA"),
            ),
            with_field("partitions", serde_json::json!([])),
            with_field(
                "partitions",
                serde_json::json!([{"partition": 0, "start_offset": -1}]),
            ),
            with_field(
                "partitions",
                serde_json::json!([
                    {"partition": 0, "start_offset": 0},
                    {"partition": 0, "start_offset": 1}
                ]),
            ),
            with_field("checkpoint_directory", serde_json::json!("")),
            with_field(
                "checkpoint_directory",
                serde_json::json!("p".repeat(MAX_LOCAL_CHECKPOINT_ROOT_PATH_BYTES_V0 + 1)),
            ),
            with_field("epoch_end_ns", serde_json::json!(0)),
            with_field("accepted_identity_limit", serde_json::json!(0)),
            with_field(
                "accepted_identity_limit",
                serde_json::json!(MAX_ACCEPTED_IDENTITIES_V0 + 1),
            ),
            with_field("accepted_event_byte_limit", serde_json::json!(0)),
            with_field(
                "accepted_event_byte_limit",
                serde_json::json!(MAX_ACCEPTED_EVENT_BYTES_V0 + 1),
            ),
            with_field("poll_timeout_ms", serde_json::json!(0)),
            with_field("poll_timeout_ms", serde_json::json!(10_001)),
            with_field("idle_poll_limit", serde_json::json!(0)),
            with_field("idle_poll_limit", serde_json::json!(10_001)),
            with_field("max_records", serde_json::json!(0)),
            with_field("max_records", serde_json::json!(1_000_001)),
        ] {
            assert_eq!(
                parse_kafka_live_config_v0(&invalid).unwrap_err(),
                KafkaJsonCliErrorV0::ConfigContract
            );
        }

        let partitions: Vec<_> = (0..=decant_kafka::MAX_KAFKA_ASSIGNED_PARTITIONS_V0)
            .map(|partition| serde_json::json!({"partition": partition, "start_offset": 0}))
            .collect();
        assert_eq!(
            parse_kafka_live_config_v0(&with_field(
                "partitions",
                serde_json::Value::Array(partitions)
            ))
            .unwrap_err(),
            KafkaJsonCliErrorV0::ConfigContract
        );

        let mut exact_wait: serde_json::Value = serde_json::from_slice(&document()).unwrap();
        exact_wait["poll_timeout_ms"] = serde_json::json!(10_000);
        exact_wait["idle_poll_limit"] = serde_json::json!(30);
        assert!(parse_kafka_live_config_v0(&serde_json::to_vec(&exact_wait).unwrap()).is_ok());
        exact_wait["idle_poll_limit"] = serde_json::json!(31);
        assert_eq!(
            parse_kafka_live_config_v0(&serde_json::to_vec(&exact_wait).unwrap()).unwrap_err(),
            KafkaJsonCliErrorV0::ConfigContract
        );

        let mut exact_document = document();
        exact_document.resize(MAX_KAFKA_LIVE_CONFIG_BYTES_V0, b' ');
        assert!(parse_kafka_live_config_v0(&exact_document).is_ok());
        exact_document.push(b' ');
        assert_eq!(
            parse_kafka_live_config_v0(&exact_document).unwrap_err(),
            KafkaJsonCliErrorV0::ConfigTooLarge
        );
    }

    #[test]
    fn output_is_bounded_reconciled_and_carries_explicit_non_claims() {
        let config = parse_kafka_live_config_v0(&document()).unwrap();
        let frontier = KafkaPartitionFrontierV0::new(
            0,
            config.source.source_contract().clone(),
            config.source.starting_positions(),
        )
        .unwrap();
        let admission = AdmissionStateV0::new(config.admission);
        let hash =
            decant_ingest::hash_kafka_ingestion_checkpoint_v0(&admission, &frontier).unwrap();
        let output = encode_kafka_live_run_v0(
            KafkaLiveTerminationV0::IdleLimit,
            KafkaLivePollUsageV0 {
                data_records: 0,
                idle_observations: 2,
                partition_eof_observations: 0,
            },
            KafkaStaticSessionUsageV0::default(),
            KafkaStaticSessionCheckpointV0::Unchanged {
                latest_generation: Some(KafkaCheckpointGenerationV0::new(1).unwrap()),
                latest_checkpoint_hash: Some(hash),
            },
        )
        .unwrap();
        let expected = format!(
            concat!(
                "{{\"format\":\"decant-kafka-live-run\",\"version\":0,",
                "\"termination\":\"idle_limit\",\"poll\":{{\"data_records\":0,",
                "\"idle_observations\":2,\"partition_eof_observations\":0}},",
                "\"admission\":{{\"processed\":0,\"advanced\":0,\"accepted\":0,",
                "\"duplicate_noops\":0,\"rejected\":0,\"before_frontier\":0}},",
                "\"omitted_transport_metadata\":{{\"header_count\":0,\"header_bytes\":0,",
                "\"timestamp_not_available_records\":0,\"timestamp_create_time_records\":0,",
                "\"timestamp_log_append_time_records\":0}},",
                "\"checkpoint\":{{\"published\":false,\"generation\":1,",
                "\"hash\":\"{}\",\"artifact_bytes\":null,\"pending_entries\":null}},",
                "\"broker_offsets_committed\":false,\"context_compiled\":false}}\n"
            ),
            hash
        );
        assert_eq!(output, expected.as_bytes());
        assert!(output.len() <= MAX_KAFKA_LIVE_OUTPUT_BYTES_V0);
        let text = std::str::from_utf8(&output).unwrap();
        assert!(!text.contains("live-source"));
        let parsed: serde_json::Value = serde_json::from_slice(&output).unwrap();
        assert_eq!(parsed["poll"]["data_records"], 0);
    }

    #[test]
    fn v1_output_is_exact_bounded_and_embeds_the_compiler_object() {
        let config = parse_kafka_live_config_v1(&document_v1(512)).unwrap();
        let frontier = KafkaPartitionFrontierV0::new(
            0,
            config.source.source_contract().clone(),
            config.source.starting_positions(),
        )
        .unwrap();
        let admission = AdmissionStateV0::new(config.admission);
        let hash =
            decant_ingest::hash_kafka_ingestion_checkpoint_v0(&admission, &frontier).unwrap();
        let context = compile_kafka_activity_context_v0(
            &admission,
            KafkaActivityContextRequestV0::new(0, 512).unwrap(),
        )
        .unwrap();
        let output = encode_kafka_live_run_v1(
            KafkaLiveTerminationV0::IdleLimit,
            KafkaLivePollUsageV0 {
                data_records: 0,
                idle_observations: 2,
                partition_eof_observations: 0,
            },
            KafkaStaticSessionUsageV0::default(),
            KafkaStaticSessionCheckpointV0::Unchanged {
                latest_generation: Some(KafkaCheckpointGenerationV0::new(1).unwrap()),
                latest_checkpoint_hash: Some(hash),
            },
            &context,
        )
        .unwrap();
        let expected = format!(
            concat!(
                "{{\"format\":\"decant-kafka-live-run\",\"version\":1,",
                "\"termination\":\"idle_limit\",\"poll\":{{\"data_records\":0,",
                "\"idle_observations\":2,\"partition_eof_observations\":0}},",
                "\"admission\":{{\"processed\":0,\"advanced\":0,\"accepted\":0,",
                "\"duplicate_noops\":0,\"rejected\":0,\"before_frontier\":0}},",
                "\"omitted_transport_metadata\":{{\"header_count\":0,\"header_bytes\":0,",
                "\"timestamp_not_available_records\":0,\"timestamp_create_time_records\":0,",
                "\"timestamp_log_append_time_records\":0}},",
                "\"checkpoint\":{{\"published\":false,\"generation\":1,",
                "\"hash\":\"{}\",\"artifact_bytes\":null,\"pending_entries\":null}},",
                "\"broker_offsets_committed\":false,\"context_compiled\":true,\"context\":{}}}\n"
            ),
            hash,
            std::str::from_utf8(context.as_bytes()).unwrap(),
        );
        assert_eq!(output, expected.as_bytes());
        assert!(output.len() <= MAX_KAFKA_LIVE_OUTPUT_BYTES_V1);
        let parsed: serde_json::Value = serde_json::from_slice(&output).unwrap();
        assert_eq!(parsed["version"], 1);
        assert_eq!(parsed["context_compiled"], true);
        assert_eq!(parsed["broker_offsets_committed"], false);
        assert_eq!(parsed["context"]["format"], "decant-kafka-activity-context");
    }

    #[test]
    fn v4_broker_free_session_publishes_and_recovers_material_change_context() {
        let directory = test_directory("material-v4");
        let mut document: serde_json::Value = serde_json::from_slice(&document_v4(4_096)).unwrap();
        document["checkpoint_directory"] = serde_json::json!(directory);
        let config = parse_kafka_live_config(&serde_json::to_vec(&document).unwrap()).unwrap();
        let store = LocalCheckpointStoreV0::open(&config.checkpoint_directory).unwrap();
        let source_contract = config.source.source_contract().clone();
        let starts = config.source.starting_positions().to_vec();
        let KafkaLiveInputContractV0::JsonObjectV1 { mapping } = config.input else {
            panic!("expected mapping V1");
        };
        let output_contract = config.output;
        let mut session = KafkaStaticCheckpointSessionV0::bootstrap(
            &store,
            config.admission,
            source_contract.clone(),
            &starts,
        )
        .unwrap();
        for (offset, value) in [(0, 10), (1, 20)] {
            assert_eq!(
                session
                    .process_json_object_record_v1(&mapping, &material_record(offset, value))
                    .unwrap(),
                decant_kafka_live::KafkaStaticSessionProcessV0::Accepted
            );
        }
        let output = finish_kafka_live_run_v0(
            &mut session,
            &store,
            KafkaLiveTerminationV0::MaxRecords,
            KafkaLivePollUsageV0 {
                data_records: 2,
                idle_observations: 0,
                partition_eof_observations: 0,
            },
            output_contract,
        )
        .unwrap();
        let parsed: serde_json::Value = serde_json::from_slice(&output).unwrap();
        assert_eq!(parsed["version"], 3);
        assert_eq!(parsed["checkpoint"]["published"], true);
        assert_eq!(parsed["broker_offsets_committed"], false);
        assert_eq!(parsed["source_ownership"], "static_partitions");
        assert_eq!(parsed["context"]["version"], 1);
        assert_eq!(parsed["context"]["changes"]["candidate_count"], 1);
        assert_eq!(
            parsed["context"]["changes"]["items"][0]["entity_key"]["value"],
            "636865636b6f7574"
        );
        let context = parsed["context"].clone();

        let recovered_config =
            parse_kafka_live_config(&serde_json::to_vec(&document).unwrap()).unwrap();
        let KafkaLiveInputContractV0::JsonObjectV1 {
            mapping: recovered_mapping,
        } = recovered_config.input
        else {
            panic!("expected recovered mapping V1");
        };
        assert_eq!(mapping, recovered_mapping);
        let mut recovered = KafkaStaticCheckpointSessionV0::bootstrap(
            &store,
            recovered_config.admission,
            source_contract,
            &starts,
        )
        .unwrap();
        let recovered_output = finish_kafka_live_run_v0(
            &mut recovered,
            &store,
            KafkaLiveTerminationV0::IdleLimit,
            KafkaLivePollUsageV0::default(),
            recovered_config.output,
        )
        .unwrap();
        let recovered_parsed: serde_json::Value =
            serde_json::from_slice(&recovered_output).unwrap();
        assert_eq!(recovered_parsed["checkpoint"]["published"], false);
        assert_eq!(recovered_parsed["context"], context);
        let text = std::str::from_utf8(&recovered_output).unwrap();
        for forbidden in [
            "private-material-secret",
            "private-unselected-value",
            "private-header",
            "private-ignored-key",
            directory.to_str().unwrap(),
        ] {
            assert!(!text.contains(forbidden));
        }
        fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn v4_material_compiler_failure_precedes_checkpoint_publication() {
        let directory = test_directory("material-v4-compile-before-publish");
        let mut document: serde_json::Value = serde_json::from_slice(&document_v4(4_096)).unwrap();
        document["checkpoint_directory"] = serde_json::json!(directory);
        let config = parse_kafka_live_config(&serde_json::to_vec(&document).unwrap()).unwrap();
        let store = LocalCheckpointStoreV0::open(&config.checkpoint_directory).unwrap();
        let source_contract = config.source.source_contract().clone();
        let starts = config.source.starting_positions().to_vec();
        let KafkaLiveInputContractV0::JsonObjectV1 { mapping } = config.input else {
            panic!("expected mapping V1");
        };
        let mut session = KafkaStaticCheckpointSessionV0::bootstrap(
            &store,
            config.admission,
            source_contract,
            &starts,
        )
        .unwrap();
        for (offset, value) in [(0, 10), (1, 20)] {
            session
                .process_json_object_record_v1(&mapping, &material_record(offset, value))
                .unwrap();
        }
        let mismatched = KafkaMaterialChangeRequestV0::new(
            KAFKA_MATERIAL_CHANGE_CONTEXT_VERSION_V1,
            "service-latency",
            SchemaIdV0::new("different-schema").unwrap(),
            EventTimeRetentionConfigV0::new(EVENT_TIME_RETENTION_VERSION_V0, 600_000_000).unwrap(),
            "series",
            "value",
            16,
            4_096,
            KafkaFieldEgressPolicyV0::new(
                0,
                vec![
                    KafkaFieldEgressRuleV0::new("secret", KafkaFieldEgressActionV0::Deny).unwrap(),
                    KafkaFieldEgressRuleV0::new("series", KafkaFieldEgressActionV0::Allow).unwrap(),
                    KafkaFieldEgressRuleV0::new("service", KafkaFieldEgressActionV0::Allow)
                        .unwrap(),
                    KafkaFieldEgressRuleV0::new("value", KafkaFieldEgressActionV0::Allow).unwrap(),
                ],
            )
            .unwrap(),
            KafkaEntityKeyEgressActionV0::AllowHex,
        )
        .unwrap();

        assert_eq!(
            finish_kafka_live_run_v0(
                &mut session,
                &store,
                KafkaLiveTerminationV0::MaxRecords,
                KafkaLivePollUsageV0 {
                    data_records: 2,
                    idle_observations: 0,
                    partition_eof_observations: 0,
                },
                KafkaLiveOutputContractV0::V3 {
                    context: mismatched,
                },
            ),
            Err(KafkaJsonCliErrorV0::Context)
        );
        assert!(store.catalog().unwrap().published().is_empty());
        assert!(store.catalog().unwrap().pending().is_empty());
        assert_eq!(session.latest_generation(), None);
        assert_eq!(session.advanced_since_checkpoint(), 2);
        fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn compiler_failure_precedes_checkpoint_publication() {
        let directory = test_directory("compile-before-publish");
        let store = LocalCheckpointStoreV0::open(&directory).unwrap();
        let mut document: serde_json::Value = serde_json::from_slice(&document_v1(512)).unwrap();
        document["accepted_identity_limit"] = serde_json::json!(MAX_ACCEPTED_IDENTITIES_V0);
        document["accepted_event_byte_limit"] = serde_json::json!(MAX_ACCEPTED_EVENT_BYTES_V0);
        document["checkpoint_directory"] = serde_json::json!(directory);
        let config = parse_kafka_live_config(&serde_json::to_vec(&document).unwrap()).unwrap();
        let output = config.output;
        let source_contract = config.source.source_contract().clone();
        let starts = config.source.starting_positions().to_vec();
        let mut session = KafkaStaticCheckpointSessionV0::bootstrap(
            &store,
            config.admission,
            source_contract,
            &starts,
        )
        .unwrap();
        for index in 0..=decant_compiler::MAX_KAFKA_ACTIVITY_DISTINCT_SCHEMAS_V0 {
            let value = format!(
                "{{\"record_format\":\"decant-kafka-event\",\"record_version\":0,\"schema_id\":\"schema-{index:04}\",\"event_time_ns\":100,\"payload\":[]}}"
            );
            let record = KafkaFetchedRecordV0::new(
                KafkaPositionV0::new(0, index as i64).unwrap(),
                Some(b"private-key"),
                Some(value.as_bytes()),
                KafkaRecordTimestampV0::NotAvailable,
                &[],
            )
            .unwrap();
            session.process_record(&record).unwrap();
        }
        let processed = session.usage().processed_records();
        assert_eq!(
            finish_kafka_live_run_v0(
                &mut session,
                &store,
                KafkaLiveTerminationV0::MaxRecords,
                KafkaLivePollUsageV0 {
                    data_records: processed,
                    idle_observations: 0,
                    partition_eof_observations: 0,
                },
                output,
            ),
            Err(KafkaJsonCliErrorV0::Context)
        );
        assert!(store.catalog().unwrap().published().is_empty());
        assert!(store.catalog().unwrap().pending().is_empty());
        assert_eq!(session.latest_generation(), None);
        assert_eq!(session.advanced_since_checkpoint(), processed);
        fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn v1_success_and_idle_recovery_keep_exact_nested_context() {
        let directory = test_directory("success-idle");
        let store = LocalCheckpointStoreV0::open(&directory).unwrap();
        let mut document: serde_json::Value = serde_json::from_slice(&document_v1(512)).unwrap();
        document["checkpoint_directory"] = serde_json::json!(directory);
        let config = parse_kafka_live_config(&serde_json::to_vec(&document).unwrap()).unwrap();
        let output_contract = config.output;
        let source_contract = config.source.source_contract().clone();
        let starts = config.source.starting_positions().to_vec();
        let admission_config = config.admission.clone();
        let mut first = KafkaStaticCheckpointSessionV0::bootstrap(
            &store,
            config.admission,
            source_contract.clone(),
            &starts,
        )
        .unwrap();
        let value = b"{\"record_format\":\"decant-kafka-event\",\"record_version\":0,\"schema_id\":\"schema-a\",\"event_time_ns\":100,\"payload\":[{\"name\":\"private.field\",\"value\":{\"kind\":\"utf8\",\"value\":\"private-value\"}}]}";
        let record = KafkaFetchedRecordV0::new(
            KafkaPositionV0::new(0, 0).unwrap(),
            Some(b"private-key"),
            Some(value),
            KafkaRecordTimestampV0::NotAvailable,
            &[],
        )
        .unwrap();
        assert!(first.process_record(&record).is_ok());
        let first_output = finish_kafka_live_run_v0(
            &mut first,
            &store,
            KafkaLiveTerminationV0::MaxRecords,
            KafkaLivePollUsageV0 {
                data_records: 1,
                idle_observations: 0,
                partition_eof_observations: 0,
            },
            output_contract.clone(),
        )
        .unwrap();
        let first_json: serde_json::Value = serde_json::from_slice(&first_output).unwrap();
        assert_eq!(first_json["checkpoint"]["published"], true);
        assert_eq!(first_json["checkpoint"]["generation"], 1);
        assert_eq!(first_json["context"]["activity"]["accepted_events"], 1);

        let mut recovered = KafkaStaticCheckpointSessionV0::bootstrap(
            &store,
            admission_config,
            source_contract,
            &starts,
        )
        .unwrap();
        let recovered_output = finish_kafka_live_run_v0(
            &mut recovered,
            &store,
            KafkaLiveTerminationV0::IdleLimit,
            KafkaLivePollUsageV0 {
                data_records: 0,
                idle_observations: 2,
                partition_eof_observations: 0,
            },
            output_contract.clone(),
        )
        .unwrap();
        let recovered_json: serde_json::Value = serde_json::from_slice(&recovered_output).unwrap();
        assert_eq!(recovered_json["checkpoint"]["published"], false);
        assert_eq!(recovered_json["checkpoint"]["generation"], 1);
        assert_eq!(recovered_json["context"], first_json["context"]);
        for private in ["private.field", "private-value", "private-key"] {
            assert!(
                !std::str::from_utf8(&first_output)
                    .unwrap()
                    .contains(private)
            );
            assert!(
                !std::str::from_utf8(&recovered_output)
                    .unwrap()
                    .contains(private)
            );
        }
        fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn v2_mapped_prefix_suffix_idle_and_changed_mapping_are_checkpoint_bound() {
        let directory = test_directory("mapped-v2");
        let mut first_document: serde_json::Value =
            serde_json::from_slice(&document_v2(2_048)).unwrap();
        first_document["checkpoint_directory"] = serde_json::json!(directory);
        let first_config =
            parse_kafka_live_config(&serde_json::to_vec(&first_document).unwrap()).unwrap();
        let output_contract = first_config.output;
        let first_source_contract = first_config.source.source_contract().clone();
        let starts = first_config.source.starting_positions().to_vec();
        let KafkaLiveInputContractV0::JsonObject {
            mapping: first_mapping,
        } = first_config.input
        else {
            panic!("V2 must select JSON-object mapping");
        };
        let expected_schema = first_mapping.schema_id().as_str().to_owned();
        let expected_source = first_mapping.effective_source_id().as_str().to_owned();
        let store = LocalCheckpointStoreV0::open(&directory).unwrap();
        let mut first = KafkaStaticCheckpointSessionV0::bootstrap(
            &store,
            first_config.admission,
            first_source_contract,
            &starts,
        )
        .unwrap();
        first
            .process_json_object_record(&first_mapping, &mapped_record(0, 100))
            .unwrap();
        let first_output = finish_kafka_live_run_v0(
            &mut first,
            &store,
            KafkaLiveTerminationV0::MaxRecords,
            KafkaLivePollUsageV0 {
                data_records: 1,
                idle_observations: 0,
                partition_eof_observations: 0,
            },
            output_contract.clone(),
        )
        .unwrap();
        let first_json: serde_json::Value = serde_json::from_slice(&first_output).unwrap();
        assert_eq!(first_json["version"], 1);
        assert_eq!(first_json["checkpoint"]["generation"], 1);
        assert_eq!(first_json["context_compiled"], true);
        assert_eq!(first_json["broker_offsets_committed"], false);
        assert_eq!(first_json["context"]["activity"]["accepted_events"], 1);
        assert_eq!(
            first_json["context"]["schemas"]["items"][0]["schema_id"],
            expected_schema
        );

        let mut reordered_document = first_document.clone();
        reordered_document["mapping"]["payload"]
            .as_array_mut()
            .unwrap()
            .reverse();
        let reordered_config =
            parse_kafka_live_config(&serde_json::to_vec(&reordered_document).unwrap()).unwrap();
        let reordered_source_contract = reordered_config.source.source_contract().clone();
        let KafkaLiveInputContractV0::JsonObject {
            mapping: reordered_mapping,
        } = reordered_config.input
        else {
            panic!("V2 must select JSON-object mapping");
        };
        assert_eq!(
            reordered_mapping.effective_source_id(),
            first_mapping.effective_source_id()
        );
        let mut suffix = KafkaStaticCheckpointSessionV0::bootstrap(
            &store,
            reordered_config.admission,
            reordered_source_contract,
            &starts,
        )
        .unwrap();
        suffix
            .process_json_object_record(&reordered_mapping, &mapped_record(1, 101))
            .unwrap();
        let suffix_output = finish_kafka_live_run_v0(
            &mut suffix,
            &store,
            KafkaLiveTerminationV0::MaxRecords,
            KafkaLivePollUsageV0 {
                data_records: 1,
                idle_observations: 0,
                partition_eof_observations: 0,
            },
            output_contract.clone(),
        )
        .unwrap();
        let suffix_json: serde_json::Value = serde_json::from_slice(&suffix_output).unwrap();
        assert_eq!(suffix_json["checkpoint"]["generation"], 2);
        assert_eq!(suffix_json["context"]["activity"]["accepted_events"], 2);

        let idle_config =
            parse_kafka_live_config(&serde_json::to_vec(&first_document).unwrap()).unwrap();
        let idle_source_contract = idle_config.source.source_contract().clone();
        let mut idle = KafkaStaticCheckpointSessionV0::bootstrap(
            &store,
            idle_config.admission,
            idle_source_contract,
            &starts,
        )
        .unwrap();
        let idle_output = finish_kafka_live_run_v0(
            &mut idle,
            &store,
            KafkaLiveTerminationV0::IdleLimit,
            KafkaLivePollUsageV0 {
                data_records: 0,
                idle_observations: 2,
                partition_eof_observations: 0,
            },
            output_contract,
        )
        .unwrap();
        let idle_json: serde_json::Value = serde_json::from_slice(&idle_output).unwrap();
        assert_eq!(idle_json["checkpoint"]["published"], false);
        assert_eq!(idle_json["checkpoint"]["generation"], 2);
        assert_eq!(idle_json["context"], suffix_json["context"]);

        for output in [&first_output, &suffix_output, &idle_output] {
            let text = std::str::from_utf8(output).unwrap();
            assert!(text.contains(&expected_schema));
            assert!(text.contains(&expected_source));
            for private in [
                "private-source-namespace",
                "observed_time",
                "raw_ok",
                "raw_service",
                "private-selected-value",
                "private_unselected",
                "private-unselected-value",
                "private-key",
                "private-header",
                "private-header-value",
                "cluster-v0",
                "/tmp/",
            ] {
                assert!(!text.contains(private));
            }
        }

        let mut changed_document = first_document;
        changed_document["mapping"]["event_time"]["unit"] = serde_json::json!("microseconds");
        let changed =
            parse_kafka_live_config(&serde_json::to_vec(&changed_document).unwrap()).unwrap();
        assert_eq!(
            run_kafka_live_config_v0(changed),
            Err(KafkaJsonCliErrorV0::SessionBootstrap)
        );
        assert_eq!(store.catalog().unwrap().published().len(), 2);
        fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn v3_compiles_policy_gated_payload_ratios_and_recovers_exact_context() {
        let directory = test_directory("v3-grouped-ratio");
        let mut document: serde_json::Value = serde_json::from_slice(&document_v3(4_096)).unwrap();
        document["checkpoint_directory"] = serde_json::json!(directory);
        let encoded = serde_json::to_vec(&document).unwrap();
        let config = parse_kafka_live_config(&encoded).unwrap();
        let output_contract = config.output.clone();
        let source_contract = config.source.source_contract().clone();
        let starts = config.source.starting_positions().to_vec();
        let KafkaLiveInputContractV0::JsonObject { mapping } = config.input else {
            panic!("V3 must select JSON-object mapping");
        };
        let store = LocalCheckpointStoreV0::open(&directory).unwrap();
        let mut session = KafkaStaticCheckpointSessionV0::bootstrap(
            &store,
            config.admission,
            source_contract,
            &starts,
        )
        .unwrap();
        for record in [
            semantic_record(0, 100, "payments", true, "private-denied-sentinel"),
            semantic_record(1, 200, "checkout", true, "private-denied-sentinel"),
            semantic_record(2, 300, "payments", false, "private-denied-sentinel"),
            semantic_record(3, 400, "payments", false, "private-denied-sentinel"),
        ] {
            session
                .process_json_object_record(&mapping, &record)
                .unwrap();
        }
        let output = finish_kafka_live_run_v0(
            &mut session,
            &store,
            KafkaLiveTerminationV0::MaxRecords,
            KafkaLivePollUsageV0 {
                data_records: 4,
                idle_observations: 0,
                partition_eof_observations: 0,
            },
            output_contract.clone(),
        )
        .unwrap();
        let parsed: serde_json::Value = serde_json::from_slice(&output).unwrap();
        assert_eq!(parsed["version"], 2);
        assert_eq!(parsed["checkpoint"]["published"], true);
        assert_eq!(parsed["checkpoint"]["generation"], 1);
        assert_eq!(
            parsed["context"]["format"],
            "decant-kafka-grouped-ratio-context"
        );
        assert_eq!(parsed["context"]["groups"]["candidate_count"], 2);
        assert_eq!(
            parsed["context"]["groups"]["items"][0]["group"]["value"],
            "checkout"
        );
        assert_eq!(
            parsed["context"]["groups"]["items"][1]["group"]["value"],
            "payments"
        );
        assert_eq!(
            parsed["context"]["groups"]["items"][1]["ratio"]["numerator"],
            2
        );
        assert_eq!(
            parsed["context"]["groups"]["items"][1]["ratio"]["denominator"],
            3
        );
        assert_eq!(
            parsed["context"]["groups"]["items"][1]["ratio"]["rate_ppm"],
            666_666
        );
        assert_eq!(parsed["context"]["policy"]["denied_field_count"], 1);

        let recovered_config = parse_kafka_live_config(&encoded).unwrap();
        let recovered_contract = recovered_config.source.source_contract().clone();
        let mut recovered = KafkaStaticCheckpointSessionV0::bootstrap(
            &store,
            recovered_config.admission,
            recovered_contract,
            &starts,
        )
        .unwrap();
        let recovered_output = finish_kafka_live_run_v0(
            &mut recovered,
            &store,
            KafkaLiveTerminationV0::IdleLimit,
            KafkaLivePollUsageV0 {
                data_records: 0,
                idle_observations: 2,
                partition_eof_observations: 0,
            },
            output_contract,
        )
        .unwrap();
        let recovered_json: serde_json::Value = serde_json::from_slice(&recovered_output).unwrap();
        assert_eq!(recovered_json["checkpoint"]["published"], false);
        assert_eq!(recovered_json["checkpoint"]["generation"], 1);
        assert_eq!(recovered_json["context"], parsed["context"]);

        for bytes in [&output, &recovered_output] {
            let text = std::str::from_utf8(bytes).unwrap();
            for forbidden in [
                "private-denied-sentinel",
                "private-unselected-value",
                "private-key",
                "private-header",
                "private-header-value",
                "raw_secret",
                "\"secret\"",
                "cluster-v0",
                "/tmp/",
            ] {
                assert!(!text.contains(forbidden));
            }
        }
        fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn v3_compiler_capacity_failure_precedes_checkpoint_publication() {
        let directory = test_directory("v3-compile-before-publish");
        let mut document: serde_json::Value = serde_json::from_slice(&document_v3(4_096)).unwrap();
        document["checkpoint_directory"] = serde_json::json!(directory);
        document["accepted_identity_limit"] = serde_json::json!(5_000);
        document["accepted_event_byte_limit"] = serde_json::json!(16 * 1_024 * 1_024);
        let config = parse_kafka_live_config(&serde_json::to_vec(&document).unwrap()).unwrap();
        let output_contract = config.output;
        let source_contract = config.source.source_contract().clone();
        let starts = config.source.starting_positions().to_vec();
        let KafkaLiveInputContractV0::JsonObject { mapping } = config.input else {
            panic!("V3 must select JSON-object mapping");
        };
        let store = LocalCheckpointStoreV0::open(&directory).unwrap();
        let mut session = KafkaStaticCheckpointSessionV0::bootstrap(
            &store,
            config.admission,
            source_contract,
            &starts,
        )
        .unwrap();
        for index in 0..=decant_state::MAX_GROUPED_INTERVAL_RATIO_GROUPS_V0 {
            let record = semantic_record(
                index as i64,
                1,
                &format!("service-{index:04}"),
                true,
                "private-denied-sentinel",
            );
            session
                .process_json_object_record(&mapping, &record)
                .unwrap();
        }
        assert_eq!(
            finish_kafka_live_run_v0(
                &mut session,
                &store,
                KafkaLiveTerminationV0::MaxRecords,
                KafkaLivePollUsageV0 {
                    data_records: decant_state::MAX_GROUPED_INTERVAL_RATIO_GROUPS_V0 + 1,
                    idle_observations: 0,
                    partition_eof_observations: 0,
                },
                output_contract,
            ),
            Err(KafkaJsonCliErrorV0::Context)
        );
        assert!(store.catalog().unwrap().published().is_empty());
        assert!(store.catalog().unwrap().pending().is_empty());
        assert_eq!(session.latest_generation(), None);
        fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn error_categories_cannot_carry_config_or_record_values() {
        let errors = [
            KafkaJsonCliErrorV0::ConfigFileOpen,
            KafkaJsonCliErrorV0::ConfigFileRead,
            KafkaJsonCliErrorV0::ConfigTooLarge,
            KafkaJsonCliErrorV0::ConfigAllocation,
            KafkaJsonCliErrorV0::ConfigSyntax,
            KafkaJsonCliErrorV0::ConfigContract,
            KafkaJsonCliErrorV0::CheckpointStore,
            KafkaJsonCliErrorV0::SessionBootstrap,
            KafkaJsonCliErrorV0::ClientCreation,
            KafkaJsonCliErrorV0::SourceObservation,
            KafkaJsonCliErrorV0::Assignment,
            KafkaJsonCliErrorV0::Poll,
            KafkaJsonCliErrorV0::Processing,
            KafkaJsonCliErrorV0::Accounting,
            KafkaJsonCliErrorV0::Context,
            KafkaJsonCliErrorV0::Checkpoint,
            KafkaJsonCliErrorV0::OutputAllocation,
            KafkaJsonCliErrorV0::OutputCapacity,
        ];
        for error in errors {
            let diagnostic = format!("{error:?}");
            assert!(!diagnostic.contains("private"));
            assert!(!diagnostic.contains('/'));
        }
    }
}
