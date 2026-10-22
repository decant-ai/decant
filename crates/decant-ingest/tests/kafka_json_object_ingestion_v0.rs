use decant_ingest::{
    KafkaCanonicalAdmissionOutcomeV0, KafkaJsonObjectIngestionErrorV0,
    ingest_json_object_kafka_record_v0,
};
use decant_ir::{KafkaPositionV0, SourceIdV0, SourcePositionKindV0};
use decant_kafka::{
    KafkaClusterIdV0, KafkaFetchedRecordV0, KafkaJsonEventTimeUnitV0, KafkaJsonObjectMappingV0,
    KafkaJsonObjectProjectionV0, KafkaJsonScalarKindV0, KafkaPartitionFrontierV0,
    KafkaRecordTimestampV0, KafkaSourceContractV0, KafkaTopicIdV0, map_json_object_kafka_record_v0,
};
use decant_state::{
    AdmissionConfigV0, AdmissionLimitsV0, AdmissionStateV0, ApplyOutcomeV0, RejectionReasonV0,
    encode_admission_snapshot_v1,
};

fn mapping(source_field: &str) -> KafkaJsonObjectMappingV0 {
    KafkaJsonObjectMappingV0::new(
        0,
        SourceIdV0::new("mapped-ingestion-namespace").unwrap(),
        "time",
        KafkaJsonEventTimeUnitV0::Nanoseconds,
        vec![
            KafkaJsonObjectProjectionV0::new(source_field, "value", KafkaJsonScalarKindV0::U64)
                .unwrap(),
        ],
    )
    .unwrap()
}

fn cluster() -> KafkaClusterIdV0 {
    KafkaClusterIdV0::new("mapped-ingestion-cluster").unwrap()
}

fn topic() -> KafkaTopicIdV0 {
    KafkaTopicIdV0::from_bytes([0x72; 16]).unwrap()
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
        100_000_000_000,
        AdmissionLimitsV0::maximum(),
    )
    .unwrap()
}

fn system(
    mapping: &KafkaJsonObjectMappingV0,
    start: i64,
) -> (KafkaPartitionFrontierV0, AdmissionStateV0) {
    (
        KafkaPartitionFrontierV0::new(
            0,
            contract(mapping),
            &[KafkaPositionV0::new(0, start).unwrap()],
        )
        .unwrap(),
        AdmissionStateV0::new(config(mapping)),
    )
}

fn record(offset: i64, time: i64, value: u64) -> KafkaFetchedRecordV0 {
    let bytes = format!("{{\"time\":{time},\"input\":{value},\"private\":\"hidden\"}}");
    KafkaFetchedRecordV0::new(
        KafkaPositionV0::new(0, offset).unwrap(),
        Some(b"private-key"),
        Some(bytes.as_bytes()),
        KafkaRecordTimestampV0::CreateTime { unix_ms: 99 },
        &[],
    )
    .unwrap()
}

fn ingest(
    mapping: &KafkaJsonObjectMappingV0,
    frontier: &mut KafkaPartitionFrontierV0,
    admission: &mut AdmissionStateV0,
    record: &KafkaFetchedRecordV0,
) -> Result<decant_ingest::KafkaJsonIngestionOutcomeV0, KafkaJsonObjectIngestionErrorV0> {
    ingest_json_object_kafka_record_v0(
        mapping,
        frontier,
        admission,
        mapping.effective_source_id(),
        &cluster(),
        topic(),
        record,
    )
}

#[test]
fn mapped_records_reuse_exact_terminal_frontier_and_admission_semantics() {
    let mapping = mapping("input");
    let (mut frontier, mut admission) = system(&mapping, 10);
    assert_eq!(
        ingest(
            &mapping,
            &mut frontier,
            &mut admission,
            &record(10, 60_000_000_000, 1),
        )
        .unwrap()
        .admission(),
        KafkaCanonicalAdmissionOutcomeV0::Advanced {
            admission: ApplyOutcomeV0::Accepted,
        }
    );
    assert_eq!(frontier.next_offset(0), Some(11));

    assert_eq!(
        ingest(
            &mapping,
            &mut frontier,
            &mut admission,
            &record(10, 60_000_000_000, 1),
        )
        .unwrap()
        .admission(),
        KafkaCanonicalAdmissionOutcomeV0::BeforeFrontier
    );
    assert_eq!(
        ingest(&mapping, &mut frontier, &mut admission, &record(12, 501, 2))
            .unwrap()
            .admission(),
        KafkaCanonicalAdmissionOutcomeV0::Gap
    );
    assert_eq!(frontier.next_offset(0), Some(11));

    assert_eq!(
        ingest(&mapping, &mut frontier, &mut admission, &record(11, 0, 3))
            .unwrap()
            .admission(),
        KafkaCanonicalAdmissionOutcomeV0::Advanced {
            admission: ApplyOutcomeV0::Rejected(RejectionReasonV0::Late),
        }
    );
    assert_eq!(frontier.next_offset(0), Some(12));
}

#[test]
fn duplicate_noop_path_is_identical_when_state_already_contains_the_position() {
    let mapping = mapping("input");
    let raw = record(10, 500, 1);
    let mut admission = AdmissionStateV0::new(config(&mapping));
    let mapped = map_json_object_kafka_record_v0(
        &mapping,
        &contract(&mapping),
        mapping.effective_source_id(),
        &cluster(),
        topic(),
        &raw,
    )
    .unwrap();
    assert_eq!(
        admission.apply_event_v1(mapped.event()).unwrap(),
        ApplyOutcomeV0::Accepted
    );
    let mut frontier = KafkaPartitionFrontierV0::new(
        0,
        contract(&mapping),
        &[KafkaPositionV0::new(0, 10).unwrap()],
    )
    .unwrap();
    assert_eq!(
        ingest(&mapping, &mut frontier, &mut admission, &raw)
            .unwrap()
            .admission(),
        KafkaCanonicalAdmissionOutcomeV0::Advanced {
            admission: ApplyOutcomeV0::DuplicateNoOp,
        }
    );
    assert_eq!(frontier.next_offset(0), Some(11));
}

#[test]
fn mapping_binding_and_parse_failures_are_atomic_and_value_safe() {
    let base_mapping = mapping("input");
    let changed = mapping("other_input");
    let (mut frontier, mut admission) = system(&base_mapping, 10);
    let before_frontier = frontier.iter().collect::<Vec<_>>();
    let before_state = encode_admission_snapshot_v1(&admission).unwrap();
    let private = KafkaFetchedRecordV0::new(
        KafkaPositionV0::new(0, 10).unwrap(),
        Some(b"private-key"),
        Some(b"private-invalid-json"),
        KafkaRecordTimestampV0::NotAvailable,
        &[],
    )
    .unwrap();
    let error = ingest(&changed, &mut frontier, &mut admission, &private).unwrap_err();
    assert_eq!(error, KafkaJsonObjectIngestionErrorV0::MappingBinding);
    assert_eq!(frontier.iter().collect::<Vec<_>>(), before_frontier);
    assert_eq!(
        encode_admission_snapshot_v1(&admission).unwrap(),
        before_state
    );
    let diagnostic = format!("{error:?}");
    for secret in ["other_input", "private-key", "private-invalid-json"] {
        assert!(!diagnostic.contains(secret));
    }
}
