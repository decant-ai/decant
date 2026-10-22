use std::collections::BTreeMap;

use decant_ingest::{
    KAFKA_INGESTION_CHECKPOINT_VERSION_V0, KafkaIngestionCheckpointEncodeErrorV0,
    MAX_KAFKA_INGESTION_CHECKPOINT_BYTES_V0, admit_expected_kafka_canonical_event_v0,
    encode_kafka_ingestion_checkpoint_v0,
};
use decant_ir::{
    CanonicalPayloadV0, EventEnvelopeV0, KafkaPositionV0, SchemaIdV0, SensitivityBitmapV0,
    SourceIdV0, SourcePositionKindV0, SourcePositionV0,
};
use decant_kafka::{
    KafkaClusterIdV0, KafkaPartitionFrontierV0, KafkaSourceContractV0, KafkaTopicIdV0,
    encode_kafka_partition_frontier_snapshot_v0,
};
use decant_state::{
    AdmissionConfigV0, AdmissionLimitsV0, AdmissionStateV0, ApplyOutcomeV0,
    encode_admission_snapshot_v1,
};

fn source(name: &str) -> SourceIdV0 {
    SourceIdV0::new(name).unwrap()
}

fn cluster() -> KafkaClusterIdV0 {
    KafkaClusterIdV0::new("cluster-v0").unwrap()
}

fn topic() -> KafkaTopicIdV0 {
    KafkaTopicIdV0::from_bytes([7; 16]).unwrap()
}

fn contract(name: &str) -> KafkaSourceContractV0 {
    KafkaSourceContractV0::new(0, source(name), cluster(), topic()).unwrap()
}

fn state(name: &str, kind: SourcePositionKindV0) -> AdmissionStateV0 {
    AdmissionStateV0::new(
        AdmissionConfigV0::new_with_source_position_kind(
            0,
            source(name),
            kind,
            0,
            100,
            AdmissionLimitsV0::maximum(),
        )
        .unwrap(),
    )
}

fn frontier(name: &str, entries: &[(i32, i64)]) -> KafkaPartitionFrontierV0 {
    KafkaPartitionFrontierV0::new(
        0,
        contract(name),
        &entries
            .iter()
            .map(|(partition, offset)| KafkaPositionV0::new(*partition, *offset).unwrap())
            .collect::<Vec<_>>(),
    )
    .unwrap()
}

fn event(name: &str, partition: i32, offset: i64, event_time_ns: i64) -> EventEnvelopeV0 {
    EventEnvelopeV0::new(
        0,
        source(name),
        SchemaIdV0::new("event.v0").unwrap(),
        SourcePositionV0::Kafka {
            position: KafkaPositionV0::new(partition, offset).unwrap(),
        },
        Some(b"key"),
        event_time_ns,
        0,
        SensitivityBitmapV0::from_bits(u64::MAX),
        CanonicalPayloadV0::try_from_fields(BTreeMap::new()).unwrap(),
    )
    .unwrap()
}

#[test]
fn exact_header_lengths_and_component_bytes_are_canonical() {
    assert_eq!(KAFKA_INGESTION_CHECKPOINT_VERSION_V0, 0);
    let admission = state("source-v0", SourcePositionKindV0::Kafka);
    let frontier = frontier("source-v0", &[(3, 9), (1, 4)]);
    let admission_bytes = encode_admission_snapshot_v1(&admission).unwrap();
    let frontier_bytes = encode_kafka_partition_frontier_snapshot_v0(&frontier).unwrap();
    let checkpoint = encode_kafka_ingestion_checkpoint_v0(&admission, &frontier).unwrap();

    let mut expected = Vec::new();
    expected.extend_from_slice(b"DECKPT00");
    expected.extend_from_slice(&0_u16.to_be_bytes());
    expected.extend_from_slice(&1_u16.to_be_bytes());
    expected.extend_from_slice(&0_u16.to_be_bytes());
    expected.extend_from_slice(&u64::try_from(admission_bytes.len()).unwrap().to_be_bytes());
    expected.extend_from_slice(&u32::try_from(frontier_bytes.len()).unwrap().to_be_bytes());
    expected.extend_from_slice(&admission_bytes);
    expected.extend_from_slice(&frontier_bytes);
    assert_eq!(checkpoint, expected);
    assert!(checkpoint.len() <= MAX_KAFKA_INGESTION_CHECKPOINT_BYTES_V0);
    assert_eq!(
        encode_kafka_ingestion_checkpoint_v0(&admission, &frontier).unwrap(),
        checkpoint
    );
}

#[test]
fn accepted_and_recorded_rejection_progress_are_both_consistent() {
    let mut admission = state("source-v0", SourcePositionKindV0::Kafka);
    let mut frontier = frontier("source-v0", &[(0, 0)]);
    let accepted = event("source-v0", 0, 0, 50);
    assert!(matches!(
        admit_expected_kafka_canonical_event_v0(
            &mut frontier,
            &mut admission,
            &source("source-v0"),
            &cluster(),
            topic(),
            &accepted,
        )
        .unwrap(),
        decant_ingest::KafkaCanonicalAdmissionOutcomeV0::Advanced {
            admission: ApplyOutcomeV0::Accepted
        }
    ));
    let rejected = event("source-v0", 0, 1, 101);
    assert!(matches!(
        admit_expected_kafka_canonical_event_v0(
            &mut frontier,
            &mut admission,
            &source("source-v0"),
            &cluster(),
            topic(),
            &rejected,
        )
        .unwrap(),
        decant_ingest::KafkaCanonicalAdmissionOutcomeV0::Advanced {
            admission: ApplyOutcomeV0::Rejected(_)
        }
    ));
    assert_eq!(frontier.next_offset(0), Some(2));
    assert_eq!(admission.usage().accepted_identities(), 1);
    assert!(encode_kafka_ingestion_checkpoint_v0(&admission, &frontier).is_ok());
}

#[test]
fn kind_and_source_binding_fail_before_component_encoding() {
    let synthetic = state("source-v0", SourcePositionKindV0::Synthetic);
    let kafka_frontier = frontier("source-v0", &[(0, 0)]);
    assert_eq!(
        encode_kafka_ingestion_checkpoint_v0(&synthetic, &kafka_frontier).unwrap_err(),
        KafkaIngestionCheckpointEncodeErrorV0::UnsupportedAdmissionSourcePositionKind {
            actual: SourcePositionKindV0::Synthetic,
        }
    );
    let kafka = state("source-v0", SourcePositionKindV0::Kafka);
    assert_eq!(
        encode_kafka_ingestion_checkpoint_v0(&kafka, &frontier("other-source", &[(0, 0)]))
            .unwrap_err(),
        KafkaIngestionCheckpointEncodeErrorV0::AdmissionSourceMismatch
    );
}

#[test]
fn accepted_events_must_be_assigned_and_strictly_behind_frontier() {
    let mut unknown = state("source-v0", SourcePositionKindV0::Kafka);
    assert_eq!(
        unknown
            .apply_event_v1(&event("source-v0", 1, 0, 50))
            .unwrap(),
        ApplyOutcomeV0::Accepted
    );
    assert_eq!(
        encode_kafka_ingestion_checkpoint_v0(&unknown, &frontier("source-v0", &[(0, 0)]))
            .unwrap_err(),
        KafkaIngestionCheckpointEncodeErrorV0::AcceptedEventUnknownPartition
    );

    let mut not_behind = state("source-v0", SourcePositionKindV0::Kafka);
    assert_eq!(
        not_behind
            .apply_event_v1(&event("source-v0", 0, 0, 50))
            .unwrap(),
        ApplyOutcomeV0::Accepted
    );
    assert_eq!(
        encode_kafka_ingestion_checkpoint_v0(&not_behind, &frontier("source-v0", &[(0, 0)]))
            .unwrap_err(),
        KafkaIngestionCheckpointEncodeErrorV0::AcceptedEventNotBeforeFrontier
    );
}

#[test]
fn success_and_failure_do_not_mutate_either_component() {
    let admission = state("source-v0", SourcePositionKindV0::Kafka);
    let source_frontier = frontier("source-v0", &[(0, 0)]);
    let state_before = encode_admission_snapshot_v1(&admission).unwrap();
    let frontier_before = encode_kafka_partition_frontier_snapshot_v0(&source_frontier).unwrap();
    encode_kafka_ingestion_checkpoint_v0(&admission, &source_frontier).unwrap();
    assert_eq!(
        encode_admission_snapshot_v1(&admission).unwrap(),
        state_before
    );
    assert_eq!(
        encode_kafka_partition_frontier_snapshot_v0(&source_frontier).unwrap(),
        frontier_before
    );

    let wrong = frontier("other-source", &[(0, 0)]);
    let wrong_before = encode_kafka_partition_frontier_snapshot_v0(&wrong).unwrap();
    assert!(encode_kafka_ingestion_checkpoint_v0(&admission, &wrong).is_err());
    assert_eq!(
        encode_admission_snapshot_v1(&admission).unwrap(),
        state_before
    );
    assert_eq!(
        encode_kafka_partition_frontier_snapshot_v0(&wrong).unwrap(),
        wrong_before
    );
}
