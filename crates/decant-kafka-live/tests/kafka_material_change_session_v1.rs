use std::fs;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use decant_compiler::{
    KafkaEntityKeyEgressActionV0, KafkaFieldEgressActionV0, KafkaFieldEgressPolicyV0,
    KafkaFieldEgressRuleV0, KafkaMaterialChangeRequestV0, compile_kafka_material_change_context_v1,
};
use decant_ir::{KafkaPositionV0, SourceIdV0, SourcePositionKindV0};
use decant_kafka::{
    KafkaClusterIdV0, KafkaFetchedRecordV0, KafkaJsonEntityKeySourceV1, KafkaJsonEventTimeUnitV0,
    KafkaJsonObjectMappingV0, KafkaJsonObjectMappingV1, KafkaJsonObjectProjectionV0,
    KafkaJsonScalarKindV0, KafkaRecordTimestampV0, KafkaSourceContractV0, KafkaTopicIdV0,
};
use decant_kafka_live::{
    KafkaStaticCheckpointSessionV0, KafkaStaticSessionCheckpointV0, KafkaStaticSessionProcessV0,
};
use decant_state::{
    AdmissionConfigV0, AdmissionLimitsV0, EVENT_TIME_RETENTION_VERSION_V0,
    EventTimeRetentionConfigV0,
};
use decant_storage::LocalCheckpointStoreV0;

static NEXT_TEMP: AtomicU64 = AtomicU64::new(0);

struct TestDirectory(PathBuf);

impl TestDirectory {
    fn new() -> Self {
        let sequence = NEXT_TEMP.fetch_add(1, Ordering::Relaxed);
        let path = std::env::temp_dir().join(format!(
            "decant-material-session-{}-{sequence}",
            std::process::id()
        ));
        fs::create_dir(&path).unwrap();
        Self(path)
    }

    fn path(&self) -> &Path {
        &self.0
    }
}

impl Drop for TestDirectory {
    fn drop(&mut self) {
        fs::remove_dir_all(&self.0).unwrap();
    }
}

fn mapping() -> KafkaJsonObjectMappingV1 {
    let base = KafkaJsonObjectMappingV0::new(
        0,
        SourceIdV0::new("private-material-namespace").unwrap(),
        "time",
        KafkaJsonEventTimeUnitV0::Nanoseconds,
        vec![
            KafkaJsonObjectProjectionV0::new("raw_series", "series", KafkaJsonScalarKindV0::Utf8)
                .unwrap(),
            KafkaJsonObjectProjectionV0::new("raw_service", "service", KafkaJsonScalarKindV0::Utf8)
                .unwrap(),
            KafkaJsonObjectProjectionV0::new("raw_value", "value", KafkaJsonScalarKindV0::U64)
                .unwrap(),
            KafkaJsonObjectProjectionV0::new("raw_secret", "secret", KafkaJsonScalarKindV0::Utf8)
                .unwrap(),
        ],
    )
    .unwrap();
    KafkaJsonObjectMappingV1::new(
        1,
        base,
        KafkaJsonEntityKeySourceV1::utf8_projection("service").unwrap(),
    )
    .unwrap()
}

fn cluster() -> KafkaClusterIdV0 {
    KafkaClusterIdV0::new("private-material-cluster").unwrap()
}

fn topic() -> KafkaTopicIdV0 {
    KafkaTopicIdV0::from_bytes([0x81; 16]).unwrap()
}

fn source_contract(mapping: &KafkaJsonObjectMappingV1) -> KafkaSourceContractV0 {
    KafkaSourceContractV0::new(0, mapping.effective_source_id().clone(), cluster(), topic())
        .unwrap()
}

fn admission_config(mapping: &KafkaJsonObjectMappingV1) -> AdmissionConfigV0 {
    AdmissionConfigV0::new_with_source_position_kind(
        0,
        mapping.effective_source_id().clone(),
        SourcePositionKindV0::Kafka,
        0,
        1_000_000,
        AdmissionLimitsV0::maximum(),
    )
    .unwrap()
}

fn record(offset: i64, value: u64) -> KafkaFetchedRecordV0 {
    let bytes = format!(
        "{{\"time\":{},\"raw_series\":\"latency\",\"raw_service\":\"checkout\",\"raw_value\":{value},\"raw_secret\":\"private-denied\"}}",
        100 + offset
    );
    KafkaFetchedRecordV0::new(
        KafkaPositionV0::new(0, offset).unwrap(),
        Some(b"private-ignored-broker-key"),
        Some(bytes.as_bytes()),
        KafkaRecordTimestampV0::NotAvailable,
        &[],
    )
    .unwrap()
}

fn context(
    session: &KafkaStaticCheckpointSessionV0,
    mapping: &KafkaJsonObjectMappingV1,
) -> Vec<u8> {
    let policy = KafkaFieldEgressPolicyV0::new(
        0,
        vec![
            KafkaFieldEgressRuleV0::new("series", KafkaFieldEgressActionV0::Allow).unwrap(),
            KafkaFieldEgressRuleV0::new("service", KafkaFieldEgressActionV0::Allow).unwrap(),
            KafkaFieldEgressRuleV0::new("value", KafkaFieldEgressActionV0::Allow).unwrap(),
            KafkaFieldEgressRuleV0::new("secret", KafkaFieldEgressActionV0::Deny).unwrap(),
        ],
    )
    .unwrap();
    let request = KafkaMaterialChangeRequestV0::new(
        1,
        "checkout-latency",
        mapping.schema_id().clone(),
        EventTimeRetentionConfigV0::new(EVENT_TIME_RETENTION_VERSION_V0, 100_000).unwrap(),
        "series",
        "value",
        8,
        16 * 1_024,
        policy,
        KafkaEntityKeyEgressActionV0::AllowHex,
    )
    .unwrap();
    compile_kafka_material_change_context_v1(session.admission(), &request)
        .unwrap()
        .into_bytes()
}

fn bootstrap(
    store: &LocalCheckpointStoreV0,
    mapping: &KafkaJsonObjectMappingV1,
) -> KafkaStaticCheckpointSessionV0 {
    KafkaStaticCheckpointSessionV0::bootstrap(
        store,
        admission_config(mapping),
        source_contract(mapping),
        &[KafkaPositionV0::new(0, 0).unwrap()],
    )
    .unwrap()
}

#[test]
fn mapping_v1_prefix_restart_suffix_and_idle_context_are_exact() {
    let directory = TestDirectory::new();
    let store = LocalCheckpointStoreV0::open(directory.path()).unwrap();
    let mapping = mapping();
    let mut prefix = bootstrap(&store, &mapping);
    for (offset, value) in [(0, 10), (1, 20)] {
        assert_eq!(
            prefix
                .process_json_object_record_v1(&mapping, &record(offset, value))
                .unwrap(),
            KafkaStaticSessionProcessV0::Accepted
        );
    }
    let prefix_context = context(&prefix, &mapping);
    let generation_1 = prefix.checkpoint(&store).unwrap();
    assert_eq!(generation_1.generation().unwrap().get(), 1);

    let mut suffix = bootstrap(&store, &mapping);
    assert_eq!(context(&suffix, &mapping), prefix_context);
    assert_eq!(
        suffix
            .process_json_object_record_v1(&mapping, &record(2, 30))
            .unwrap(),
        KafkaStaticSessionProcessV0::Accepted
    );
    let suffix_context = context(&suffix, &mapping);
    assert_ne!(suffix_context, prefix_context);
    let generation_2 = suffix.checkpoint(&store).unwrap();
    assert_eq!(generation_2.generation().unwrap().get(), 2);

    let mut idle = bootstrap(&store, &mapping);
    assert_eq!(context(&idle, &mapping), suffix_context);
    assert_eq!(
        idle.checkpoint(&store).unwrap(),
        KafkaStaticSessionCheckpointV0::Unchanged {
            latest_generation: generation_2.generation(),
            latest_checkpoint_hash: generation_2.checkpoint_hash(),
        }
    );
    let text = std::str::from_utf8(&suffix_context).unwrap();
    assert!(text.contains("\"value\":\"636865636b6f7574\""));
    assert!(text.contains("\"value\":{\"kind\":\"u64\",\"value\":20}"));
    assert!(text.contains("\"value\":{\"kind\":\"u64\",\"value\":30}"));
    assert!(text.contains("/kafka/0/1"));
    assert!(text.contains("/kafka/0/2"));
    for forbidden in [
        "private-denied",
        "private-ignored-broker-key",
        "private-material-cluster",
    ] {
        assert!(!text.contains(forbidden));
    }
}
