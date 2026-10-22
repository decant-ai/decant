use decant_ingest::{
    KAFKA_INGESTION_CHECKPOINT_HASH_ALGORITHM_V0, KAFKA_INGESTION_CHECKPOINT_HASH_DOMAIN_V0,
    KAFKA_INGESTION_CHECKPOINT_HASH_TEXT_BYTES_V0, KAFKA_INGESTION_CHECKPOINT_HASH_VERSION_V0,
    KafkaIngestionCheckpointEncodeErrorV0, KafkaIngestionCheckpointHashErrorV0,
    hash_kafka_ingestion_checkpoint_v0,
};
use decant_ir::{KafkaPositionV0, SourceIdV0, SourcePositionKindV0};
use decant_kafka::{
    KafkaClusterIdV0, KafkaPartitionFrontierV0, KafkaSourceContractV0, KafkaTopicIdV0,
};
use decant_state::{AdmissionConfigV0, AdmissionLimitsV0, AdmissionStateV0};

fn state(source: &str) -> AdmissionStateV0 {
    state_with_rejection_end(source, 100)
}

fn state_with_rejection_end(source: &str, rejection_end_ns: i64) -> AdmissionStateV0 {
    AdmissionStateV0::new(
        AdmissionConfigV0::new_with_source_position_kind(
            0,
            SourceIdV0::new(source).unwrap(),
            SourcePositionKindV0::Kafka,
            0,
            rejection_end_ns,
            AdmissionLimitsV0::maximum(),
        )
        .unwrap(),
    )
}

fn frontier(source: &str, entries: &[(i32, i64)]) -> KafkaPartitionFrontierV0 {
    KafkaPartitionFrontierV0::new(
        0,
        KafkaSourceContractV0::new(
            0,
            SourceIdV0::new(source).unwrap(),
            KafkaClusterIdV0::new("cluster-v0").unwrap(),
            KafkaTopicIdV0::from_bytes([7; 16]).unwrap(),
        )
        .unwrap(),
        &entries
            .iter()
            .map(|(partition, offset)| KafkaPositionV0::new(*partition, *offset).unwrap())
            .collect::<Vec<_>>(),
    )
    .unwrap()
}

#[test]
fn constants_text_and_empty_cut_vector_are_exact() {
    assert_eq!(KAFKA_INGESTION_CHECKPOINT_HASH_VERSION_V0, 0);
    assert_eq!(KAFKA_INGESTION_CHECKPOINT_HASH_ALGORITHM_V0, "sha256");
    assert_eq!(KAFKA_INGESTION_CHECKPOINT_HASH_DOMAIN_V0.len(), 41);
    let hash =
        hash_kafka_ingestion_checkpoint_v0(&state("source-v0"), &frontier("source-v0", &[(0, 0)]))
            .unwrap();
    assert_eq!(hash.as_bytes().len(), 32);
    assert_eq!(
        hash.to_string().len(),
        KAFKA_INGESTION_CHECKPOINT_HASH_TEXT_BYTES_V0
    );
    assert_eq!(
        hash.to_canonical_string(),
        "sha256:v0:cf141f5e7a878e873bd02f42b7c836349441669a18d48a97b092d1537f97a503"
    );
}

#[test]
fn repeated_equal_cuts_match_and_frontier_changes_are_covered() {
    let admission = state("source-v0");
    let baseline =
        hash_kafka_ingestion_checkpoint_v0(&admission, &frontier("source-v0", &[(3, 9), (1, 4)]))
            .unwrap();
    assert_eq!(
        baseline,
        hash_kafka_ingestion_checkpoint_v0(&admission, &frontier("source-v0", &[(1, 4), (3, 9)]),)
            .unwrap()
    );
    assert_ne!(
        baseline,
        hash_kafka_ingestion_checkpoint_v0(&admission, &frontier("source-v0", &[(1, 4), (3, 10)]),)
            .unwrap()
    );
    assert_ne!(
        baseline,
        hash_kafka_ingestion_checkpoint_v0(
            &state_with_rejection_end("source-v0", 101),
            &frontier("source-v0", &[(1, 4), (3, 9)]),
        )
        .unwrap()
    );
    assert_eq!(
        baseline.to_canonical_string(),
        "sha256:v0:eb82871ac4c86579f6649f178c7c3e08cbca667614c6d9cbf5529d947e26e84b"
    );
}

#[test]
fn checkpoint_errors_are_typed_and_debug_redacts_digest() {
    let admission = state("source-v0");
    let hash =
        hash_kafka_ingestion_checkpoint_v0(&admission, &frontier("source-v0", &[(0, 0)])).unwrap();
    let debug = format!("{hash:?}");
    assert!(debug.contains("<redacted>"));
    assert!(!debug.contains(&hash.to_string()));
    assert!(!debug.contains("source-v0"));

    assert_eq!(
        hash_kafka_ingestion_checkpoint_v0(&admission, &frontier("other-source", &[(0, 0)]),)
            .unwrap_err(),
        KafkaIngestionCheckpointHashErrorV0::CheckpointEncoding(
            KafkaIngestionCheckpointEncodeErrorV0::AdmissionSourceMismatch,
        )
    );
}
