use decant_ingest::{
    KAFKA_CHECKPOINT_GENERATION_ARTIFACT_VERSION_V0, KAFKA_INGESTION_CHECKPOINT_HASH_VERSION_V0,
    KAFKA_INGESTION_CHECKPOINT_VERSION_V0, KafkaCheckpointGenerationArtifactDecodeErrorV0,
    KafkaCheckpointGenerationErrorV0, KafkaCheckpointGenerationV0,
    MAX_KAFKA_CHECKPOINT_GENERATION_ARTIFACT_BYTES_V0, MAX_KAFKA_INGESTION_CHECKPOINT_BYTES_V0,
    encode_kafka_checkpoint_generation_artifact_v0, encode_kafka_ingestion_checkpoint_v0,
    hash_kafka_ingestion_checkpoint_v0, restore_kafka_checkpoint_generation_artifact_v0,
};
use decant_ir::{KafkaPositionV0, SourceIdV0, SourcePositionKindV0};
use decant_kafka::{
    KafkaClusterIdV0, KafkaPartitionFrontierV0, KafkaSourceContractV0, KafkaTopicIdV0,
};
use decant_state::{
    AdmissionConfigV0, AdmissionLimitsV0, AdmissionStateV0, encode_admission_snapshot_v1,
};

fn config(source: &str, kind: SourcePositionKindV0) -> AdmissionConfigV0 {
    AdmissionConfigV0::new_with_source_position_kind(
        0,
        SourceIdV0::new(source).unwrap(),
        kind,
        0,
        100,
        AdmissionLimitsV0::maximum(),
    )
    .unwrap()
}

fn contract(source: &str) -> KafkaSourceContractV0 {
    KafkaSourceContractV0::new(
        0,
        SourceIdV0::new(source).unwrap(),
        KafkaClusterIdV0::new("cluster-v0").unwrap(),
        KafkaTopicIdV0::from_bytes([7; 16]).unwrap(),
    )
    .unwrap()
}

fn frontier(source: &str) -> KafkaPartitionFrontierV0 {
    KafkaPartitionFrontierV0::new(
        0,
        contract(source),
        &[
            KafkaPositionV0::new(3, 9).unwrap(),
            KafkaPositionV0::new(1, 4).unwrap(),
        ],
    )
    .unwrap()
}

#[test]
fn generation_and_exact_artifact_layout_are_frozen() {
    assert_eq!(
        KafkaCheckpointGenerationV0::new(0),
        Err(KafkaCheckpointGenerationErrorV0::Zero)
    );
    assert_eq!(
        KafkaCheckpointGenerationV0::new(u64::MAX).unwrap().get(),
        u64::MAX
    );
    assert_eq!(KAFKA_CHECKPOINT_GENERATION_ARTIFACT_VERSION_V0, 0);

    let generation = KafkaCheckpointGenerationV0::new(7).unwrap();
    let admission = AdmissionStateV0::new(config("source-v0", SourcePositionKindV0::Kafka));
    let frontier = frontier("source-v0");
    let checkpoint = encode_kafka_ingestion_checkpoint_v0(&admission, &frontier).unwrap();
    let hash = hash_kafka_ingestion_checkpoint_v0(&admission, &frontier).unwrap();
    let artifact =
        encode_kafka_checkpoint_generation_artifact_v0(generation, &admission, &frontier).unwrap();
    let mut expected = Vec::new();
    expected.extend_from_slice(b"DECKGEN0");
    expected.extend_from_slice(&KAFKA_CHECKPOINT_GENERATION_ARTIFACT_VERSION_V0.to_be_bytes());
    expected.extend_from_slice(&7_u64.to_be_bytes());
    expected.extend_from_slice(&KAFKA_INGESTION_CHECKPOINT_VERSION_V0.to_be_bytes());
    expected.extend_from_slice(&KAFKA_INGESTION_CHECKPOINT_HASH_VERSION_V0.to_be_bytes());
    expected.extend_from_slice(&u64::try_from(checkpoint.len()).unwrap().to_be_bytes());
    expected.extend_from_slice(hash.as_bytes());
    expected.extend_from_slice(&checkpoint);
    assert_eq!(artifact, expected);
    assert!(artifact.len() <= MAX_KAFKA_CHECKPOINT_GENERATION_ARTIFACT_BYTES_V0);
}

#[test]
fn strict_restore_round_trips_and_reencodes_exactly() {
    let generation = KafkaCheckpointGenerationV0::new(7).unwrap();
    let expected_config = config("source-v0", SourcePositionKindV0::Kafka);
    let admission = AdmissionStateV0::new(expected_config.clone());
    let frontier = frontier("source-v0");
    let artifact =
        encode_kafka_checkpoint_generation_artifact_v0(generation, &admission, &frontier).unwrap();
    let restored = restore_kafka_checkpoint_generation_artifact_v0(
        &artifact,
        generation,
        &expected_config,
        frontier.source_contract(),
    )
    .unwrap();
    assert_eq!(restored.generation(), generation);
    assert_eq!(
        restored.checkpoint_hash(),
        hash_kafka_ingestion_checkpoint_v0(&admission, &frontier).unwrap()
    );
    assert_eq!(
        encode_admission_snapshot_v1(restored.admission()).unwrap(),
        encode_admission_snapshot_v1(&admission).unwrap()
    );
    assert_eq!(
        encode_kafka_checkpoint_generation_artifact_v0(
            restored.generation(),
            restored.admission(),
            restored.frontier(),
        )
        .unwrap(),
        artifact
    );
    let debug = format!("{restored:?}");
    assert!(debug.contains("<redacted>"));
    assert!(!debug.contains("source-v0"));
}

#[test]
fn every_prefix_oversize_trailing_and_header_mutation_fail_closed() {
    let generation = KafkaCheckpointGenerationV0::new(7).unwrap();
    let expected_config = config("source-v0", SourcePositionKindV0::Kafka);
    let admission = AdmissionStateV0::new(expected_config.clone());
    let frontier = frontier("source-v0");
    let artifact =
        encode_kafka_checkpoint_generation_artifact_v0(generation, &admission, &frontier).unwrap();
    for length in 0..artifact.len() {
        assert!(
            restore_kafka_checkpoint_generation_artifact_v0(
                &artifact[..length],
                generation,
                &expected_config,
                frontier.source_contract()
            )
            .is_err()
        );
    }
    let mut oversized = vec![0; MAX_KAFKA_CHECKPOINT_GENERATION_ARTIFACT_BYTES_V0 + 1];
    assert!(matches!(
        restore_kafka_checkpoint_generation_artifact_v0(
            &oversized,
            generation,
            &expected_config,
            frontier.source_contract()
        ),
        Err(KafkaCheckpointGenerationArtifactDecodeErrorV0::DocumentTooLarge { .. })
    ));
    oversized.clear();
    let mut trailing = artifact.clone();
    trailing.push(0);
    assert_eq!(
        restore_kafka_checkpoint_generation_artifact_v0(
            &trailing,
            generation,
            &expected_config,
            frontier.source_contract()
        )
        .unwrap_err(),
        KafkaCheckpointGenerationArtifactDecodeErrorV0::TrailingBytes
    );

    for (range, value) in [
        (0..8, vec![0; 8]),
        (8..10, 1_u16.to_be_bytes().to_vec()),
        (10..18, 8_u64.to_be_bytes().to_vec()),
        (18..20, 1_u16.to_be_bytes().to_vec()),
        (20..22, 1_u16.to_be_bytes().to_vec()),
        (
            22..30,
            u64::try_from(MAX_KAFKA_INGESTION_CHECKPOINT_BYTES_V0 + 1)
                .unwrap()
                .to_be_bytes()
                .to_vec(),
        ),
    ] {
        let mut wrong = artifact.clone();
        wrong[range].copy_from_slice(&value);
        assert!(
            restore_kafka_checkpoint_generation_artifact_v0(
                &wrong,
                generation,
                &expected_config,
                frontier.source_contract()
            )
            .is_err()
        );
    }
    let mut wrong_hash = artifact;
    wrong_hash[30] ^= 1;
    assert_eq!(
        restore_kafka_checkpoint_generation_artifact_v0(
            &wrong_hash,
            generation,
            &expected_config,
            frontier.source_contract()
        )
        .unwrap_err(),
        KafkaCheckpointGenerationArtifactDecodeErrorV0::CheckpointHashMismatch
    );
}

#[test]
fn trusted_expectations_precede_bytes_and_recomputed_corruption_reaches_semantic_decoder() {
    let generation = KafkaCheckpointGenerationV0::new(7).unwrap();
    let expected_config = config("source-v0", SourcePositionKindV0::Kafka);
    let admission = AdmissionStateV0::new(expected_config.clone());
    let frontier = frontier("source-v0");
    let artifact =
        encode_kafka_checkpoint_generation_artifact_v0(generation, &admission, &frontier).unwrap();
    assert!(matches!(
        restore_kafka_checkpoint_generation_artifact_v0(
            &[],
            generation,
            &config("source-v0", SourcePositionKindV0::Synthetic),
            frontier.source_contract()
        ),
        Err(KafkaCheckpointGenerationArtifactDecodeErrorV0::ExpectedContract(_))
    ));
    assert!(matches!(
        restore_kafka_checkpoint_generation_artifact_v0(
            &[],
            generation,
            &expected_config,
            &contract("other-source")
        ),
        Err(KafkaCheckpointGenerationArtifactDecodeErrorV0::ExpectedContract(_))
    ));

    let mut corrupted = artifact;
    corrupted[62] ^= 1;
    let checkpoint = &corrupted[62..];
    let digest = {
        use sha2::{Digest, Sha256};
        let mut hasher = Sha256::new();
        hasher.update(41_u32.to_be_bytes());
        hasher.update(b"decant:kafka-ingestion-checkpoint-hash:v0");
        hasher.update(u64::try_from(checkpoint.len()).unwrap().to_be_bytes());
        hasher.update(checkpoint);
        hasher.finalize()
    };
    corrupted[30..62].copy_from_slice(&digest);
    assert!(matches!(
        restore_kafka_checkpoint_generation_artifact_v0(
            &corrupted,
            generation,
            &expected_config,
            frontier.source_contract()
        ),
        Err(KafkaCheckpointGenerationArtifactDecodeErrorV0::Checkpoint(
            _
        ))
    ));
}
