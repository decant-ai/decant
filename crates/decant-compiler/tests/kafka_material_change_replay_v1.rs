use std::collections::BTreeMap;

use decant_compiler::{
    KAFKA_FIELD_EGRESS_POLICY_VERSION_V0, KAFKA_MATERIAL_CHANGE_CONTEXT_VERSION_V0,
    KAFKA_MATERIAL_CHANGE_CONTEXT_VERSION_V1, KafkaEntityKeyEgressActionV0,
    KafkaFieldEgressActionV0, KafkaFieldEgressPolicyV0, KafkaFieldEgressRuleV0,
    KafkaMaterialChangeErrorV0, KafkaMaterialChangeRequestV0,
    MAX_KAFKA_MATERIAL_CHANGE_OUTPUT_BYTES_V0, MIN_KAFKA_MATERIAL_CHANGE_OUTPUT_BYTES_V1,
    compile_kafka_material_change_context_v0, compile_kafka_material_change_context_v1,
};
use decant_ir::{
    CanonicalPayloadV0, EventEnvelopeV0, ScalarValueV0, SchemaIdV0, SensitivityBitmapV0,
    SourceIdV0, SourcePositionKindV0, SourcePositionV0,
};
use decant_state::{
    ADMISSION_VERSION_V0, AdmissionConfigV0, AdmissionLimitsV0, AdmissionStateV0, ApplyOutcomeV0,
    EVENT_TIME_RETENTION_VERSION_V0, EventTimeRetentionConfigV0, FinalizeOutcomeV0,
    encode_admission_snapshot_v1, restore_admission_snapshot_v1,
};

const EPOCH_END: i64 = 1_000_000;

fn source() -> SourceIdV0 {
    SourceIdV0::new("private-replay-source").unwrap()
}

fn schema() -> SchemaIdV0 {
    SchemaIdV0::new("private-replay-schema").unwrap()
}

fn config() -> AdmissionConfigV0 {
    AdmissionConfigV0::new_with_source_position_kind(
        ADMISSION_VERSION_V0,
        source(),
        SourcePositionKindV0::Kafka,
        0,
        EPOCH_END,
        AdmissionLimitsV0::maximum(),
    )
    .unwrap()
}

fn policy() -> KafkaFieldEgressPolicyV0 {
    KafkaFieldEgressPolicyV0::new(
        KAFKA_FIELD_EGRESS_POLICY_VERSION_V0,
        vec![
            KafkaFieldEgressRuleV0::new("series", KafkaFieldEgressActionV0::Allow).unwrap(),
            KafkaFieldEgressRuleV0::new("value", KafkaFieldEgressActionV0::Allow).unwrap(),
            KafkaFieldEgressRuleV0::new("secret", KafkaFieldEgressActionV0::Deny).unwrap(),
        ],
    )
    .unwrap()
}

fn request(version: u16, budget: usize) -> KafkaMaterialChangeRequestV0 {
    KafkaMaterialChangeRequestV0::new(
        version,
        "replay-view",
        schema(),
        EventTimeRetentionConfigV0::new(EVENT_TIME_RETENTION_VERSION_V0, 100_000).unwrap(),
        "series",
        "value",
        8,
        budget,
        policy(),
        KafkaEntityKeyEgressActionV0::Deny,
    )
    .unwrap()
}

fn event(offset: i64, time: i64, key: &[u8], series: &str, value: u64) -> EventEnvelopeV0 {
    EventEnvelopeV0::new(
        0,
        source(),
        schema(),
        SourcePositionV0::kafka(0, offset).unwrap(),
        Some(key),
        time,
        0,
        SensitivityBitmapV0::from_bits(u64::MAX),
        CanonicalPayloadV0::try_from_fields(BTreeMap::from([
            ("series".to_owned(), ScalarValueV0::utf8(series).unwrap()),
            ("value".to_owned(), ScalarValueV0::from_u64(value)),
            (
                "secret".to_owned(),
                ScalarValueV0::utf8("private-replay-secret").unwrap(),
            ),
        ]))
        .unwrap(),
    )
    .unwrap()
}

fn apply(events: &[EventEnvelopeV0]) -> AdmissionStateV0 {
    let mut state = AdmissionStateV0::new(config());
    for event in events {
        assert_eq!(state.apply_event_v1(event), Ok(ApplyOutcomeV0::Accepted));
    }
    state
}

#[test]
fn compiler_entry_points_reject_cross_version_requests() {
    let state = AdmissionStateV0::new(config());
    assert_eq!(
        compile_kafka_material_change_context_v0(
            &state,
            &request(
                KAFKA_MATERIAL_CHANGE_CONTEXT_VERSION_V1,
                MIN_KAFKA_MATERIAL_CHANGE_OUTPUT_BYTES_V1,
            ),
        ),
        Err(KafkaMaterialChangeErrorV0::RequestVersion {
            expected: 0,
            actual: 1,
        })
    );
    assert_eq!(
        compile_kafka_material_change_context_v1(
            &state,
            &request(
                KAFKA_MATERIAL_CHANGE_CONTEXT_VERSION_V0,
                MAX_KAFKA_MATERIAL_CHANGE_OUTPUT_BYTES_V0,
            ),
        ),
        Err(KafkaMaterialChangeErrorV0::RequestVersion {
            expected: 1,
            actual: 0,
        })
    );
}

#[test]
fn v0_stays_receipt_free_and_v1_receipt_is_canonical_and_bounded() {
    let events = [
        event(0, 10, b"private-key", "status", 1),
        event(1, 20, b"private-key", "status", 2),
    ];
    let state = apply(&events);
    let v0 = compile_kafka_material_change_context_v0(
        &state,
        &request(0, MAX_KAFKA_MATERIAL_CHANGE_OUTPUT_BYTES_V0),
    )
    .unwrap();
    assert!(v0.admission_state_hash().is_none());
    assert!(std::str::from_utf8(v0.as_bytes()).unwrap().starts_with(
        "{\"format\":\"decant-kafka-material-change-context\",\"version\":0,\"view_id\":"
    ));
    assert!(
        !v0.as_bytes()
            .windows(b"receipt".len())
            .any(|value| value == b"receipt")
    );

    let v1 = compile_kafka_material_change_context_v1(
        &state,
        &request(1, MAX_KAFKA_MATERIAL_CHANGE_OUTPUT_BYTES_V0),
    )
    .unwrap();
    let receipt = v1.admission_state_hash().unwrap().to_canonical_string();
    assert_eq!(receipt.len(), "sha256:v1:".len() + 64);
    assert!(receipt.starts_with("sha256:v1:"));
    assert!(
        receipt["sha256:v1:".len()..]
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    );
    assert!(
        std::str::from_utf8(v1.as_bytes())
            .unwrap()
            .contains(&receipt)
    );
    assert!(v1.as_bytes().len() <= MAX_KAFKA_MATERIAL_CHANGE_OUTPUT_BYTES_V0);
}

#[test]
fn live_restore_permutation_and_fresh_replay_are_byte_identical() {
    let events = [
        event(0, 10, b"key-a", "status", 1),
        event(1, 20, b"key-a", "status", 2),
        event(2, 30, b"key-b", "status", 7),
        event(3, 40, b"key-b", "status", 8),
    ];
    let live = apply(&events);
    let restored =
        restore_admission_snapshot_v1(&encode_admission_snapshot_v1(&live).unwrap(), &config())
            .unwrap();
    let permuted = apply(&[
        events[3].clone(),
        events[1].clone(),
        events[0].clone(),
        events[2].clone(),
    ]);
    let replayed = apply(&events);
    let request = request(1, MAX_KAFKA_MATERIAL_CHANGE_OUTPUT_BYTES_V0);
    let compile = |state: &AdmissionStateV0| {
        compile_kafka_material_change_context_v1(state, &request)
            .unwrap()
            .into_bytes()
    };
    let expected = compile(&live);
    assert_eq!(expected, compile(&restored));
    assert_eq!(expected, compile(&permuted));
    assert_eq!(expected, compile(&replayed));
}

#[test]
fn duplicate_noop_preserves_receipt_while_content_and_finalization_change_it() {
    let events = [
        event(0, 10, b"key", "status", 1),
        event(1, 20, b"key", "status", 2),
    ];
    let mut baseline = apply(&events);
    let request = request(1, MAX_KAFKA_MATERIAL_CHANGE_OUTPUT_BYTES_V0);
    let before = compile_kafka_material_change_context_v1(&baseline, &request)
        .unwrap()
        .admission_state_hash()
        .copied()
        .unwrap();
    assert_eq!(
        baseline.apply_event_v1(&events[1]),
        Ok(ApplyOutcomeV0::DuplicateNoOp)
    );
    let duplicate = compile_kafka_material_change_context_v1(&baseline, &request)
        .unwrap()
        .admission_state_hash()
        .copied()
        .unwrap();
    assert_eq!(before, duplicate);

    let changed = apply(&[events[0].clone(), event(1, 20, b"key", "status", 3)]);
    assert_ne!(
        before,
        compile_kafka_material_change_context_v1(&changed, &request)
            .unwrap()
            .admission_state_hash()
            .copied()
            .unwrap()
    );

    assert_eq!(
        baseline.finalize_source(&source(), EPOCH_END),
        FinalizeOutcomeV0::Finalized
    );
    assert_ne!(
        before,
        compile_kafka_material_change_context_v1(&baseline, &request)
            .unwrap()
            .admission_state_hash()
            .copied()
            .unwrap()
    );
}

#[test]
fn corrupt_snapshot_never_reaches_compilation() {
    let state = apply(&[
        event(0, 10, b"key", "status", 1),
        event(1, 20, b"key", "status", 2),
    ]);
    let mut snapshot = encode_admission_snapshot_v1(&state).unwrap();
    snapshot.pop();
    assert!(restore_admission_snapshot_v1(&snapshot, &config()).is_err());
}
