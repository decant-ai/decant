use std::fs;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use decant_compiler::{
    KAFKA_ACTIVITY_CONTEXT_VERSION_V0, KafkaActivityContextRequestV0,
    MAX_KAFKA_ACTIVITY_CONTEXT_OUTPUT_BYTES_V0, compile_kafka_activity_context_v0,
};
use decant_ingest::KafkaIngestionCheckpointHashV0;
use decant_ir::{KafkaPositionV0, SourceIdV0, SourcePositionKindV0};
use decant_kafka::{
    KAFKA_SOURCE_CONTRACT_VERSION_V0, KafkaClusterIdV0, KafkaFetchedRecordV0, KafkaRecordHeaderV0,
    KafkaRecordTimestampV0, KafkaSourceContractV0, KafkaTopicIdV0,
};
use decant_kafka_live::{
    KafkaStaticCheckpointSessionV0, KafkaStaticSessionBootstrapErrorV0,
    KafkaStaticSessionCheckpointV0, KafkaStaticSessionProcessV0,
};
use decant_state::{
    ADMISSION_VERSION_V0, AdmissionConfigV0, AdmissionLimitsV0, AdmissionStateHashV1,
    hash_admission_state_v1,
};
use decant_storage::LocalCheckpointStoreV0;

static NEXT_TEMP: AtomicU64 = AtomicU64::new(0);

const SOURCE_ID: &str = "activity-recovery-source-v0";
const PRIVATE_PAYLOAD_FIELD: &str = "private.payload.field";
const PRIVATE_PAYLOAD_VALUE: &str = "private-payload-value";
const PRIVATE_KEY: &[u8] = b"private-kafka-key";
const PRIVATE_HEADER_VALUE: &[u8] = b"private-header-value";

struct TestDirectory {
    path: PathBuf,
}

impl TestDirectory {
    fn new(label: &str) -> Self {
        let sequence = NEXT_TEMP.fetch_add(1, Ordering::Relaxed);
        let path = std::env::temp_dir().join(format!(
            "decant-kafka-activity-recovery-{label}-{}-{sequence}",
            std::process::id()
        ));
        fs::create_dir(&path).unwrap();
        Self { path }
    }

    fn path(&self) -> &Path {
        &self.path
    }
}

impl Drop for TestDirectory {
    fn drop(&mut self) {
        fs::remove_dir_all(&self.path).unwrap();
    }
}

fn source() -> SourceIdV0 {
    SourceIdV0::new(SOURCE_ID).unwrap()
}

fn contract() -> KafkaSourceContractV0 {
    KafkaSourceContractV0::new(
        KAFKA_SOURCE_CONTRACT_VERSION_V0,
        source(),
        KafkaClusterIdV0::new("activity-recovery-cluster-v0").unwrap(),
        KafkaTopicIdV0::from_bytes([0x45; 16]).unwrap(),
    )
    .unwrap()
}

fn admission_config() -> AdmissionConfigV0 {
    AdmissionConfigV0::new_with_source_position_kind(
        ADMISSION_VERSION_V0,
        source(),
        SourcePositionKindV0::Kafka,
        0,
        10_000,
        AdmissionLimitsV0::maximum(),
    )
    .unwrap()
}

fn position(offset: i64) -> KafkaPositionV0 {
    KafkaPositionV0::new(0, offset).unwrap()
}

fn value(event_time_ns: i64) -> Vec<u8> {
    format!(
        "{{\"record_format\":\"decant-kafka-event\",\"record_version\":0,\"schema_id\":\"orders.activity.v0\",\"event_time_ns\":{event_time_ns},\"payload\":[{{\"name\":\"{PRIVATE_PAYLOAD_FIELD}\",\"value\":{{\"kind\":\"utf8\",\"value\":\"{PRIVATE_PAYLOAD_VALUE}\"}}}}]}}"
    )
    .into_bytes()
}

fn record(offset: i64) -> KafkaFetchedRecordV0 {
    let value = value(100 + offset);
    KafkaFetchedRecordV0::new(
        position(offset),
        Some(PRIVATE_KEY),
        Some(&value),
        KafkaRecordTimestampV0::CreateTime {
            unix_ms: 1_000 + offset,
        },
        &[KafkaRecordHeaderV0::new("private-header", Some(PRIVATE_HEADER_VALUE)).unwrap()],
    )
    .unwrap()
}

fn open(directory: &TestDirectory) -> LocalCheckpointStoreV0 {
    LocalCheckpointStoreV0::open(directory.path()).unwrap()
}

fn compile(session: &KafkaStaticCheckpointSessionV0) -> (Vec<u8>, AdmissionStateHashV1, usize) {
    let context = compile_kafka_activity_context_v0(
        session.admission(),
        KafkaActivityContextRequestV0::new(
            KAFKA_ACTIVITY_CONTEXT_VERSION_V0,
            MAX_KAFKA_ACTIVITY_CONTEXT_OUTPUT_BYTES_V0,
        )
        .unwrap(),
    )
    .unwrap();
    (
        context.as_bytes().to_vec(),
        hash_admission_state_v1(session.admission()).unwrap(),
        context.accepted_events() as usize,
    )
}

struct OracleResult {
    prefix_context: Vec<u8>,
    suffix_context: Vec<u8>,
    prefix_state_hash: AdmissionStateHashV1,
    suffix_state_hash: AdmissionStateHashV1,
    prefix_checkpoint_hash: KafkaIngestionCheckpointHashV0,
    suffix_checkpoint_hash: KafkaIngestionCheckpointHashV0,
}

fn run_oracle(label: &str) -> OracleResult {
    let directory = TestDirectory::new(label);
    let store = open(&directory);
    let mut prefix = KafkaStaticCheckpointSessionV0::bootstrap(
        &store,
        admission_config(),
        contract(),
        &[position(0)],
    )
    .unwrap();
    for offset in 0..3 {
        assert_eq!(
            prefix.process_record(&record(offset)).unwrap(),
            KafkaStaticSessionProcessV0::Accepted
        );
    }
    let (prefix_context, prefix_state_hash, prefix_count) = compile(&prefix);
    assert_eq!(prefix_count, 3);
    let generation_1 = prefix.checkpoint(&store).unwrap();
    assert!(generation_1.was_published());
    assert_eq!(generation_1.generation().unwrap().get(), 1);
    let prefix_checkpoint_hash = generation_1.checkpoint_hash().unwrap();

    let mut recovered_prefix = KafkaStaticCheckpointSessionV0::bootstrap(
        &store,
        admission_config(),
        contract(),
        &[position(0)],
    )
    .unwrap();
    let recovered_prefix_context = compile(&recovered_prefix);
    assert_eq!(recovered_prefix_context.0, prefix_context);
    assert_eq!(recovered_prefix_context.1, prefix_state_hash);
    assert_eq!(recovered_prefix_context.2, 3);
    assert_eq!(
        recovered_prefix.checkpoint(&store).unwrap(),
        KafkaStaticSessionCheckpointV0::Unchanged {
            latest_generation: generation_1.generation(),
            latest_checkpoint_hash: generation_1.checkpoint_hash(),
        }
    );

    assert_eq!(
        recovered_prefix.process_record(&record(3)).unwrap(),
        KafkaStaticSessionProcessV0::Accepted
    );
    let (suffix_context, suffix_state_hash, suffix_count) = compile(&recovered_prefix);
    assert_eq!(suffix_count, 4);
    assert_ne!(suffix_context, prefix_context);
    assert_ne!(suffix_state_hash, prefix_state_hash);
    let generation_2 = recovered_prefix.checkpoint(&store).unwrap();
    assert!(generation_2.was_published());
    assert_eq!(generation_2.generation().unwrap().get(), 2);
    let suffix_checkpoint_hash = generation_2.checkpoint_hash().unwrap();
    assert_ne!(suffix_checkpoint_hash, prefix_checkpoint_hash);

    let mut recovered_suffix = KafkaStaticCheckpointSessionV0::bootstrap(
        &store,
        admission_config(),
        contract(),
        &[position(0)],
    )
    .unwrap();
    let recovered_suffix_context = compile(&recovered_suffix);
    assert_eq!(recovered_suffix_context.0, suffix_context);
    assert_eq!(recovered_suffix_context.1, suffix_state_hash);
    assert_eq!(recovered_suffix_context.2, 4);
    assert_eq!(
        recovered_suffix.checkpoint(&store).unwrap(),
        KafkaStaticSessionCheckpointV0::Unchanged {
            latest_generation: generation_2.generation(),
            latest_checkpoint_hash: generation_2.checkpoint_hash(),
        }
    );

    for context in [&prefix_context, &suffix_context] {
        let text = std::str::from_utf8(context).unwrap();
        for private in [
            PRIVATE_PAYLOAD_FIELD,
            PRIVATE_PAYLOAD_VALUE,
            "private-kafka-key",
            "private-header-value",
            "activity-recovery-cluster-v0",
        ] {
            assert!(
                !text.contains(private),
                "private fixture value leaked: {private}"
            );
        }
    }

    OracleResult {
        prefix_context,
        suffix_context,
        prefix_state_hash,
        suffix_state_hash,
        prefix_checkpoint_hash,
        suffix_checkpoint_hash,
    }
}

#[test]
fn context_is_exact_across_verified_generations_and_strict_recovery() {
    let first = run_oracle("first");
    let second = run_oracle("second");

    assert_eq!(first.prefix_context, second.prefix_context);
    assert_eq!(first.suffix_context, second.suffix_context);
    assert_eq!(first.prefix_state_hash, second.prefix_state_hash);
    assert_eq!(first.suffix_state_hash, second.suffix_state_hash);
    assert_eq!(first.prefix_checkpoint_hash, second.prefix_checkpoint_hash);
    assert_eq!(first.suffix_checkpoint_hash, second.suffix_checkpoint_hash);
}

#[test]
fn corrupt_latest_generation_refuses_without_falling_back_or_compiling() {
    let directory = TestDirectory::new("corrupt-latest");
    let store = open(&directory);
    let mut session = KafkaStaticCheckpointSessionV0::bootstrap(
        &store,
        admission_config(),
        contract(),
        &[position(0)],
    )
    .unwrap();
    for offset in 0..3 {
        session.process_record(&record(offset)).unwrap();
    }
    assert_eq!(
        session
            .checkpoint(&store)
            .unwrap()
            .generation()
            .unwrap()
            .get(),
        1
    );
    session.process_record(&record(3)).unwrap();
    assert_eq!(
        session
            .checkpoint(&store)
            .unwrap()
            .generation()
            .unwrap()
            .get(),
        2
    );

    fs::write(
        directory.path().join("checkpoint-00000000000000000002.dcp"),
        b"corrupt-latest-generation",
    )
    .unwrap();
    let error = KafkaStaticCheckpointSessionV0::bootstrap(
        &store,
        admission_config(),
        contract(),
        &[position(0)],
    )
    .unwrap_err();
    assert!(matches!(
        error,
        KafkaStaticSessionBootstrapErrorV0::Recovery(_)
    ));
    let diagnostic = format!("{error:?} {error}");
    for private in [
        PRIVATE_PAYLOAD_FIELD,
        PRIVATE_PAYLOAD_VALUE,
        "private-kafka-key",
        "private-header-value",
        "activity-recovery-cluster-v0",
    ] {
        assert!(!diagnostic.contains(private));
    }

    assert!(
        directory
            .path()
            .join("checkpoint-00000000000000000001.dcp")
            .is_file()
    );
}
