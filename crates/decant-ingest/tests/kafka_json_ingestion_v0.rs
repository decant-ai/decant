use decant_ingest::{
    KAFKA_JSON_INGESTION_VERSION_V0, KafkaCanonicalAdmissionOutcomeV0, KafkaJsonIngestionErrorV0,
    ingest_decant_json_kafka_record_v0,
};
use decant_ir::{KafkaPositionV0, SourceIdV0, SourcePositionKindV0, SourcePositionV0};
use decant_kafka::{
    KafkaClusterIdV0, KafkaFetchedRecordV0, KafkaPartitionFrontierV0, KafkaRecordHeaderV0,
    KafkaRecordTimestampKindV0, KafkaRecordTimestampV0, KafkaSourceContractV0, KafkaTopicIdV0,
};
use decant_state::{
    AdmissionConfigV0, AdmissionLimitsV0, AdmissionStateV0, ApplyOutcomeV0, RejectionReasonV0,
    encode_admission_snapshot_v1, hash_admission_state_v1,
};

type RunReceipt = (
    Vec<decant_ingest::KafkaJsonIngestionOutcomeV0>,
    Vec<u8>,
    String,
    Vec<(i32, i64)>,
);

fn source() -> SourceIdV0 {
    SourceIdV0::new("source-v0").unwrap()
}

fn cluster() -> KafkaClusterIdV0 {
    KafkaClusterIdV0::new("cluster-v0").unwrap()
}

fn topic() -> KafkaTopicIdV0 {
    KafkaTopicIdV0::from_bytes([6; 16]).unwrap()
}

fn contract() -> KafkaSourceContractV0 {
    KafkaSourceContractV0::new(0, source(), cluster(), topic()).unwrap()
}

fn system(start: i64) -> (KafkaPartitionFrontierV0, AdmissionStateV0) {
    let frontier =
        KafkaPartitionFrontierV0::new(0, contract(), &[KafkaPositionV0::new(0, start).unwrap()])
            .unwrap();
    let state = AdmissionStateV0::new(
        AdmissionConfigV0::new_with_source_position_kind(
            0,
            source(),
            SourcePositionKindV0::Kafka,
            0,
            100_000_000_000,
            AdmissionLimitsV0::maximum(),
        )
        .unwrap(),
    );
    (frontier, state)
}

fn value(event_time_ns: i64, amount: u64) -> Vec<u8> {
    format!(
        "{{\"record_format\":\"decant-kafka-event\",\"record_version\":0,\"schema_id\":\"payments.event.v0\",\"event_time_ns\":{event_time_ns},\"payload\":[{{\"name\":\"amount\",\"value\":{{\"kind\":\"u64\",\"value\":{amount}}}}}]}}"
    )
    .into_bytes()
}

fn record(offset: i64, event_time_ns: i64, amount: u64) -> KafkaFetchedRecordV0 {
    let value = value(event_time_ns, amount);
    KafkaFetchedRecordV0::new(
        KafkaPositionV0::new(0, offset).unwrap(),
        Some(b"payment-key"),
        Some(&value),
        KafkaRecordTimestampV0::CreateTime { unix_ms: 123 },
        &[KafkaRecordHeaderV0::new("trace", Some(b"not-canonical")).unwrap()],
    )
    .unwrap()
}

#[test]
fn expected_raw_record_reaches_canonical_state_and_returns_omission_ledger() {
    assert_eq!(KAFKA_JSON_INGESTION_VERSION_V0, 0);
    let (mut frontier, mut state) = system(10);
    let raw = record(10, 100, 7);
    let outcome = ingest_decant_json_kafka_record_v0(
        &mut frontier,
        &mut state,
        &source(),
        &cluster(),
        topic(),
        &raw,
    )
    .unwrap();
    assert_eq!(
        outcome.admission(),
        KafkaCanonicalAdmissionOutcomeV0::Advanced {
            admission: ApplyOutcomeV0::Accepted,
        }
    );
    assert_eq!(outcome.omitted_transport_metadata().header_count(), 1);
    assert_eq!(
        outcome.omitted_transport_metadata().header_bytes(),
        raw.header_bytes()
    );
    assert_eq!(
        outcome.omitted_transport_metadata().timestamp_kind(),
        KafkaRecordTimestampKindV0::CreateTime
    );
    assert_eq!(frontier.next_offset(0), Some(11));
    let accepted: Vec<_> = state.accepted_events().collect();
    assert_eq!(accepted.len(), 1);
    assert_eq!(
        accepted[0].source_position(),
        SourcePositionV0::kafka(0, 10).unwrap()
    );
    assert_eq!(accepted[0].key(), Some(&b"payment-key"[..]));
    assert_eq!(accepted[0].event_time_ns(), 100);
    assert_eq!(accepted[0].sensitivity().bits(), u64::MAX);
    assert_eq!(
        accepted[0].provenance().as_str(),
        "decant://event/v0/source-v0/kafka/0/10"
    );
}

#[test]
fn malformed_and_tombstone_records_leave_state_and_frontier_exact() {
    for raw in [
        KafkaFetchedRecordV0::new(
            KafkaPositionV0::new(0, 10).unwrap(),
            None,
            Some(b"not json"),
            KafkaRecordTimestampV0::NotAvailable,
            &[],
        )
        .unwrap(),
        KafkaFetchedRecordV0::new(
            KafkaPositionV0::new(0, 10).unwrap(),
            None,
            None,
            KafkaRecordTimestampV0::NotAvailable,
            &[],
        )
        .unwrap(),
    ] {
        let (mut frontier, mut state) = system(10);
        let before_hash = hash_admission_state_v1(&state).unwrap();
        assert!(matches!(
            ingest_decant_json_kafka_record_v0(
                &mut frontier,
                &mut state,
                &source(),
                &cluster(),
                topic(),
                &raw,
            ),
            Err(KafkaJsonIngestionErrorV0::Mapping { .. })
        ));
        assert_eq!(frontier.next_offset(0), Some(10));
        assert_eq!(hash_admission_state_v1(&state).unwrap(), before_hash);
    }
}

#[test]
fn valid_before_and_gap_records_are_decoded_but_do_not_mutate() {
    for (offset, expected) in [
        (9, KafkaCanonicalAdmissionOutcomeV0::BeforeFrontier),
        (11, KafkaCanonicalAdmissionOutcomeV0::Gap),
    ] {
        let (mut frontier, mut state) = system(10);
        let before = encode_admission_snapshot_v1(&state).unwrap();
        let raw = record(offset, 100, 7);
        let outcome = ingest_decant_json_kafka_record_v0(
            &mut frontier,
            &mut state,
            &source(),
            &cluster(),
            topic(),
            &raw,
        )
        .unwrap();
        assert_eq!(outcome.admission(), expected);
        assert_eq!(outcome.omitted_transport_metadata().header_count(), 1);
        assert_eq!(frontier.next_offset(0), Some(10));
        assert_eq!(encode_admission_snapshot_v1(&state).unwrap(), before);
    }
}

#[test]
fn recorded_rejection_is_terminal_in_the_raw_pipeline() {
    let (mut frontier, mut state) = system(10);
    for raw in [record(10, 60_000_000_000, 1), record(11, 0, 2)] {
        let outcome = ingest_decant_json_kafka_record_v0(
            &mut frontier,
            &mut state,
            &source(),
            &cluster(),
            topic(),
            &raw,
        )
        .unwrap();
        if raw.position().offset() == 11 {
            assert_eq!(
                outcome.admission(),
                KafkaCanonicalAdmissionOutcomeV0::Advanced {
                    admission: ApplyOutcomeV0::Rejected(RejectionReasonV0::Late),
                }
            );
        }
    }
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
fn repeated_runs_converge_and_source_errors_are_value_safe() {
    fn run() -> RunReceipt {
        let (mut frontier, mut state) = system(10);
        let mut outcomes = Vec::new();
        for raw in [record(10, 100, 1), record(11, 101, 2)] {
            outcomes.push(
                ingest_decant_json_kafka_record_v0(
                    &mut frontier,
                    &mut state,
                    &source(),
                    &cluster(),
                    topic(),
                    &raw,
                )
                .unwrap(),
            );
        }
        (
            outcomes,
            encode_admission_snapshot_v1(&state).unwrap(),
            hash_admission_state_v1(&state)
                .unwrap()
                .to_canonical_string(),
            frontier.iter().collect(),
        )
    }
    assert_eq!(run(), run());

    let (mut frontier, mut state) = system(10);
    let bad_source = SourceIdV0::new("secret-source").unwrap();
    let raw = KafkaFetchedRecordV0::new(
        KafkaPositionV0::new(0, 10).unwrap(),
        Some(b"secret-key"),
        Some(b"secret-value"),
        KafkaRecordTimestampV0::CreateTime { unix_ms: 777 },
        &[],
    )
    .unwrap();
    let error = ingest_decant_json_kafka_record_v0(
        &mut frontier,
        &mut state,
        &bad_source,
        &cluster(),
        topic(),
        &raw,
    )
    .unwrap_err();
    let diagnostic = format!("{error:?}");
    for probe in ["secret-source", "secret-key", "secret-value", "777"] {
        assert!(!diagnostic.contains(probe));
    }
    assert_eq!(frontier.next_offset(0), Some(10));
    assert_eq!(state.usage().accepted_identities(), 0);
}
