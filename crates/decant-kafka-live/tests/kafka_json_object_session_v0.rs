use std::fs;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use decant_compiler::{
    KafkaActivityContextRequestV0, MAX_KAFKA_ACTIVITY_CONTEXT_OUTPUT_BYTES_V0,
    compile_kafka_activity_context_v0,
};
use decant_ir::{KafkaPositionV0, SourceIdV0, SourcePositionKindV0};
use decant_kafka::{
    KafkaClusterIdV0, KafkaFetchedRecordV0, KafkaJsonEventTimeUnitV0, KafkaJsonObjectMappingV0,
    KafkaJsonObjectProjectionV0, KafkaJsonScalarKindV0, KafkaRecordTimestampV0,
    KafkaSourceContractV0, KafkaTopicIdV0,
};
use decant_kafka_live::{
    KafkaStaticCheckpointSessionV0, KafkaStaticSessionBootstrapErrorV0,
    KafkaStaticSessionCheckpointV0, KafkaStaticSessionProcessErrorV0, KafkaStaticSessionProcessV0,
};
use decant_state::{AdmissionConfigV0, AdmissionLimitsV0, encode_admission_snapshot_v1};
use decant_storage::LocalCheckpointStoreV0;

static NEXT_TEMP: AtomicU64 = AtomicU64::new(0);

struct TestDirectory(PathBuf);

impl TestDirectory {
    fn new(label: &str) -> Self {
        let sequence = NEXT_TEMP.fetch_add(1, Ordering::Relaxed);
        let path = std::env::temp_dir().join(format!(
            "decant-json-object-session-{label}-{}-{sequence}",
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

fn mapping_contract(reordered: bool, changed: bool) -> KafkaJsonObjectMappingV0 {
    let mut projections = vec![
        KafkaJsonObjectProjectionV0::new("amount", "amount", KafkaJsonScalarKindV0::U64).unwrap(),
        KafkaJsonObjectProjectionV0::new(
            if changed { "other_service" } else { "service" },
            "service",
            KafkaJsonScalarKindV0::Utf8,
        )
        .unwrap(),
    ];
    if reordered {
        projections.reverse();
    }
    KafkaJsonObjectMappingV0::new(
        0,
        SourceIdV0::new("mapped-session-namespace").unwrap(),
        "time_ms",
        KafkaJsonEventTimeUnitV0::Milliseconds,
        projections,
    )
    .unwrap()
}

fn cluster() -> KafkaClusterIdV0 {
    KafkaClusterIdV0::new("mapped-session-cluster").unwrap()
}

fn topic() -> KafkaTopicIdV0 {
    KafkaTopicIdV0::from_bytes([0x73; 16]).unwrap()
}

fn contract(mapping: &KafkaJsonObjectMappingV0) -> KafkaSourceContractV0 {
    KafkaSourceContractV0::new(0, mapping.effective_source_id().clone(), cluster(), topic())
        .unwrap()
}

fn config(mapping: &KafkaJsonObjectMappingV0) -> AdmissionConfigV0 {
    AdmissionConfigV0::new_with_source_position_kind(
        0,
        mapping.effective_source_id().clone(),
        SourcePositionKindV0::Kafka,
        0,
        10_000_000_000,
        AdmissionLimitsV0::maximum(),
    )
    .unwrap()
}

fn record(offset: i64) -> KafkaFetchedRecordV0 {
    let value = format!(
        "{{\"time_ms\":{},\"amount\":{},\"service\":\"private-service\",\"other_service\":\"private-other\",\"unselected_secret\":\"private-secret\"}}",
        1_000 + offset,
        offset + 1
    );
    KafkaFetchedRecordV0::new(
        KafkaPositionV0::new(0, offset).unwrap(),
        Some(b"private-key"),
        Some(value.as_bytes()),
        KafkaRecordTimestampV0::CreateTime { unix_ms: 123 },
        &[],
    )
    .unwrap()
}

fn bootstrap(
    store: &LocalCheckpointStoreV0,
    mapping: &KafkaJsonObjectMappingV0,
) -> Result<KafkaStaticCheckpointSessionV0, KafkaStaticSessionBootstrapErrorV0> {
    KafkaStaticCheckpointSessionV0::bootstrap(
        store,
        config(mapping),
        contract(mapping),
        &[KafkaPositionV0::new(0, 0).unwrap()],
    )
}

fn context(session: &KafkaStaticCheckpointSessionV0) -> Vec<u8> {
    compile_kafka_activity_context_v0(
        session.admission(),
        KafkaActivityContextRequestV0::new(0, MAX_KAFKA_ACTIVITY_CONTEXT_OUTPUT_BYTES_V0).unwrap(),
    )
    .unwrap()
    .as_bytes()
    .to_vec()
}

#[test]
fn mapping_bound_prefix_suffix_and_idle_recovery_are_exact() {
    let directory = TestDirectory::new("recovery");
    let store = LocalCheckpointStoreV0::open(directory.path()).unwrap();
    let mapping = mapping_contract(false, false);
    let mut prefix = bootstrap(&store, &mapping).unwrap();
    for offset in 0..3 {
        assert_eq!(
            prefix
                .process_json_object_record(&mapping, &record(offset))
                .unwrap(),
            KafkaStaticSessionProcessV0::Accepted
        );
    }
    let prefix_context = context(&prefix);
    let generation_1 = prefix.checkpoint(&store).unwrap();
    assert_eq!(generation_1.generation().unwrap().get(), 1);

    let equivalent = mapping_contract(true, false);
    let mut suffix = bootstrap(&store, &equivalent).unwrap();
    assert_eq!(context(&suffix), prefix_context);
    assert_eq!(
        suffix
            .process_json_object_record(&equivalent, &record(3))
            .unwrap(),
        KafkaStaticSessionProcessV0::Accepted
    );
    let suffix_context = context(&suffix);
    let generation_2 = suffix.checkpoint(&store).unwrap();
    assert_eq!(generation_2.generation().unwrap().get(), 2);

    let mut idle = bootstrap(&store, &mapping).unwrap();
    assert_eq!(context(&idle), suffix_context);
    assert_eq!(
        idle.checkpoint(&store).unwrap(),
        KafkaStaticSessionCheckpointV0::Unchanged {
            latest_generation: generation_2.generation(),
            latest_checkpoint_hash: generation_2.checkpoint_hash(),
        }
    );
    let text = std::str::from_utf8(&suffix_context).unwrap();
    for private in [
        "mapped-session-namespace",
        "private-service",
        "private-other",
        "private-secret",
        "private-key",
        "mapped-session-cluster",
    ] {
        assert!(!text.contains(private));
    }
}

#[test]
fn semantic_mapping_change_refuses_existing_checkpoint() {
    let directory = TestDirectory::new("changed");
    let store = LocalCheckpointStoreV0::open(directory.path()).unwrap();
    let mapping = mapping_contract(false, false);
    let mut session = bootstrap(&store, &mapping).unwrap();
    assert_eq!(
        session
            .process_json_object_record(&mapping, &record(0))
            .unwrap(),
        KafkaStaticSessionProcessV0::Accepted
    );
    session.checkpoint(&store).unwrap();

    let changed = mapping_contract(false, true);
    assert_ne!(mapping.effective_source_id(), changed.effective_source_id());
    assert!(matches!(
        bootstrap(&store, &changed),
        Err(KafkaStaticSessionBootstrapErrorV0::Recovery(_))
    ));
}

#[test]
fn mapped_processing_failure_leaves_session_and_store_unchanged() {
    let directory = TestDirectory::new("atomic");
    let store = LocalCheckpointStoreV0::open(directory.path()).unwrap();
    let mapping = mapping_contract(false, false);
    let mut session = bootstrap(&store, &mapping).unwrap();
    let before_state = encode_admission_snapshot_v1(session.admission()).unwrap();
    let before_frontier = session.frontier().iter().collect::<Vec<_>>();
    let before_usage = session.usage();
    let malformed = KafkaFetchedRecordV0::new(
        KafkaPositionV0::new(0, 0).unwrap(),
        Some(b"private-key"),
        Some(b"private-invalid-json"),
        KafkaRecordTimestampV0::NotAvailable,
        &[],
    )
    .unwrap();
    assert!(matches!(
        session.process_json_object_record(&mapping, &malformed),
        Err(KafkaStaticSessionProcessErrorV0::JsonObjectIngestion(_))
    ));
    assert_eq!(
        encode_admission_snapshot_v1(session.admission()).unwrap(),
        before_state
    );
    assert_eq!(
        session.frontier().iter().collect::<Vec<_>>(),
        before_frontier
    );
    assert_eq!(session.usage(), before_usage);
    assert_eq!(session.advanced_since_checkpoint(), 0);
    assert_eq!(
        session.checkpoint(&store).unwrap(),
        KafkaStaticSessionCheckpointV0::Unchanged {
            latest_generation: None,
            latest_checkpoint_hash: None,
        }
    );
    assert!(store.catalog().unwrap().published().is_empty());
}
