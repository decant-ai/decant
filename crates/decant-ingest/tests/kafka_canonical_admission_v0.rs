use std::collections::BTreeMap;

use decant_ingest::{
    KAFKA_CANONICAL_ADMISSION_VERSION_V0, KafkaCanonicalAdmissionErrorV0,
    KafkaCanonicalAdmissionOutcomeV0, admit_expected_kafka_canonical_event_v0,
};
use decant_ir::{
    CanonicalPayloadV0, EventEnvelopeV0, KafkaPositionV0, ScalarValueV0, SchemaIdV0, SourceIdV0,
    SourcePositionKindV0, SourcePositionV0,
};
use decant_kafka::{
    KafkaClusterIdV0, KafkaPartitionFrontierV0, KafkaSourceContractV0,
    KafkaSourceObservationErrorV0, KafkaTopicIdV0, assemble_validated_kafka_canonical_event_v0,
};
use decant_state::{
    AdmissionConfigV0, AdmissionErrorV0, AdmissionLimitsV0, AdmissionStateV0, ApplyOutcomeV0,
    RejectionReasonV0, hash_admission_state_v1,
};

fn source() -> SourceIdV0 {
    SourceIdV0::new("source-v0").unwrap()
}

fn cluster() -> KafkaClusterIdV0 {
    KafkaClusterIdV0::new("cluster-v0").unwrap()
}

fn topic() -> KafkaTopicIdV0 {
    KafkaTopicIdV0::from_bytes([4; 16]).unwrap()
}

fn contract() -> KafkaSourceContractV0 {
    KafkaSourceContractV0::new(0, source(), cluster(), topic()).unwrap()
}

fn frontier(offset: i64) -> KafkaPartitionFrontierV0 {
    KafkaPartitionFrontierV0::new(0, contract(), &[KafkaPositionV0::new(0, offset).unwrap()])
        .unwrap()
}

fn admission_state(kind: SourcePositionKindV0) -> AdmissionStateV0 {
    AdmissionStateV0::new(
        AdmissionConfigV0::new_with_source_position_kind(
            0,
            source(),
            kind,
            0,
            100_000_000_000,
            AdmissionLimitsV0::maximum(),
        )
        .unwrap(),
    )
}

fn event(offset: i64, event_time_ns: i64) -> EventEnvelopeV0 {
    let mut fields = BTreeMap::new();
    fields.insert("count".to_owned(), ScalarValueV0::from_u64(1));
    assemble_validated_kafka_canonical_event_v0(
        &contract(),
        &source(),
        &cluster(),
        topic(),
        KafkaPositionV0::new(0, offset).unwrap(),
        SchemaIdV0::new("schema.v0").unwrap(),
        Some(b"key"),
        event_time_ns,
        CanonicalPayloadV0::try_from_fields(fields).unwrap(),
    )
    .unwrap()
}

#[test]
fn expected_acceptance_advances_frontier_after_state() {
    assert_eq!(KAFKA_CANONICAL_ADMISSION_VERSION_V0, 0);
    let mut frontier = frontier(10);
    let mut state = admission_state(SourcePositionKindV0::Kafka);
    assert_eq!(
        admit_expected_kafka_canonical_event_v0(
            &mut frontier,
            &mut state,
            &source(),
            &cluster(),
            topic(),
            &event(10, 100),
        ),
        Ok(KafkaCanonicalAdmissionOutcomeV0::Advanced {
            admission: ApplyOutcomeV0::Accepted,
        })
    );
    assert_eq!(frontier.next_offset(0), Some(11));
    assert_eq!(state.usage().accepted_identities(), 1);
}

#[test]
fn explicit_admission_rejection_is_terminal_and_advances() {
    let mut frontier = frontier(10);
    let mut state = admission_state(SourcePositionKindV0::Kafka);
    let first = event(10, 60_000_000_000);
    assert!(matches!(
        admit_expected_kafka_canonical_event_v0(
            &mut frontier,
            &mut state,
            &source(),
            &cluster(),
            topic(),
            &first,
        ),
        Ok(KafkaCanonicalAdmissionOutcomeV0::Advanced { .. })
    ));
    let late = event(11, 0);
    assert_eq!(
        admit_expected_kafka_canonical_event_v0(
            &mut frontier,
            &mut state,
            &source(),
            &cluster(),
            topic(),
            &late,
        ),
        Ok(KafkaCanonicalAdmissionOutcomeV0::Advanced {
            admission: ApplyOutcomeV0::Rejected(RejectionReasonV0::Late),
        })
    );
    assert_eq!(frontier.next_offset(0), Some(12));
    assert_eq!(state.usage().accepted_identities(), 1);
    assert_eq!(
        state
            .rejection_summary()
            .range(RejectionReasonV0::Late)
            .count(),
        1
    );
}

#[test]
fn before_and_gap_do_not_touch_frontier_or_state() {
    for (offset, expected) in [
        (9, KafkaCanonicalAdmissionOutcomeV0::BeforeFrontier),
        (11, KafkaCanonicalAdmissionOutcomeV0::Gap),
    ] {
        let mut frontier = frontier(10);
        let mut state = admission_state(SourcePositionKindV0::Kafka);
        let before_hash = hash_admission_state_v1(&state).unwrap();
        assert_eq!(
            admit_expected_kafka_canonical_event_v0(
                &mut frontier,
                &mut state,
                &source(),
                &cluster(),
                topic(),
                &event(offset, 100),
            ),
            Ok(expected)
        );
        assert_eq!(frontier.next_offset(0), Some(10));
        assert_eq!(hash_admission_state_v1(&state).unwrap(), before_hash);
    }
}

#[test]
fn observation_event_and_admission_errors_leave_frontier_unchanged() {
    let mut frontier = frontier(10);
    let mut state = admission_state(SourcePositionKindV0::Kafka);
    let before_hash = hash_admission_state_v1(&state).unwrap();
    let bad_source = SourceIdV0::new("other-v0").unwrap();
    assert!(matches!(
        admit_expected_kafka_canonical_event_v0(
            &mut frontier,
            &mut state,
            &bad_source,
            &cluster(),
            topic(),
            &event(10, 100),
        ),
        Err(KafkaCanonicalAdmissionErrorV0::Frontier {
            category: decant_kafka::KafkaPartitionFrontierErrorV0::SourceObservation {
                category: KafkaSourceObservationErrorV0::SourceMismatch,
            },
        })
    ));
    assert_eq!(frontier.next_offset(0), Some(10));
    assert_eq!(hash_admission_state_v1(&state).unwrap(), before_hash);

    let mut fields = BTreeMap::new();
    fields.insert("count".to_owned(), ScalarValueV0::from_u64(1));
    let wrong_event_source = EventEnvelopeV0::new(
        0,
        SourceIdV0::new("other-v0").unwrap(),
        SchemaIdV0::new("schema.v0").unwrap(),
        SourcePositionV0::kafka(0, 10).unwrap(),
        None,
        100,
        0,
        decant_ir::SensitivityBitmapV0::from_bits(u64::MAX),
        CanonicalPayloadV0::try_from_fields(fields).unwrap(),
    )
    .unwrap();
    assert_eq!(
        admit_expected_kafka_canonical_event_v0(
            &mut frontier,
            &mut state,
            &source(),
            &cluster(),
            topic(),
            &wrong_event_source,
        ),
        Err(KafkaCanonicalAdmissionErrorV0::EventSourceMismatch)
    );
    assert_eq!(frontier.next_offset(0), Some(10));
    assert_eq!(hash_admission_state_v1(&state).unwrap(), before_hash);

    let mut wrong_state = admission_state(SourcePositionKindV0::Synthetic);
    assert!(matches!(
        admit_expected_kafka_canonical_event_v0(
            &mut frontier,
            &mut wrong_state,
            &source(),
            &cluster(),
            topic(),
            &event(10, 100),
        ),
        Err(KafkaCanonicalAdmissionErrorV0::Admission {
            category: AdmissionErrorV0::UnexpectedSourcePositionKind { .. },
        })
    ));
    assert_eq!(frontier.next_offset(0), Some(10));
}

#[test]
fn non_kafka_event_is_rejected_before_state_and_diagnostics_are_value_safe() {
    let mut fields = BTreeMap::new();
    fields.insert(
        "secret-field".to_owned(),
        ScalarValueV0::utf8("secret-value").unwrap(),
    );
    let synthetic = EventEnvelopeV0::new(
        0,
        source(),
        SchemaIdV0::new("secret-schema").unwrap(),
        SourcePositionV0::Synthetic { sequence: 777 },
        Some(b"secret-key"),
        999,
        0,
        decant_ir::SensitivityBitmapV0::from_bits(u64::MAX),
        CanonicalPayloadV0::try_from_fields(fields).unwrap(),
    )
    .unwrap();
    let mut frontier = frontier(10);
    let mut state = admission_state(SourcePositionKindV0::Kafka);
    let error = admit_expected_kafka_canonical_event_v0(
        &mut frontier,
        &mut state,
        &source(),
        &cluster(),
        topic(),
        &synthetic,
    )
    .unwrap_err();
    assert_eq!(
        error,
        KafkaCanonicalAdmissionErrorV0::UnsupportedEventPosition {
            actual: SourcePositionKindV0::Synthetic,
        }
    );
    let diagnostic = format!("{error:?}");
    for probe in [
        "secret-field",
        "secret-value",
        "secret-schema",
        "secret-key",
        "777",
        "999",
    ] {
        assert!(!diagnostic.contains(probe));
    }
    assert_eq!(frontier.next_offset(0), Some(10));
    assert_eq!(state.usage().accepted_identities(), 0);
}
