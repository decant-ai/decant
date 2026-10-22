use std::any::Any;

use decant_ir::{
    CanonicalPayloadV0, ENVELOPE_VERSION_V0, KafkaPositionV0, MAX_KEY_BYTES, ScalarValueV0,
    SchemaIdV0, SensitivityBitmapV0, SourceIdV0, SourcePositionV0, ValidationErrorV0,
};
use decant_kafka::{
    KAFKA_CANONICAL_EVENT_ASSEMBLER_VERSION_V0, KAFKA_PARTITION_FRONTIER_VERSION_V0,
    KAFKA_SOURCE_CONTRACT_VERSION_V0, KAFKA_TOPIC_ID_BYTES_V0, KafkaCanonicalEventAssemblyErrorV0,
    KafkaClusterIdV0, KafkaPartitionFrontierV0, KafkaSourceContractV0,
    KafkaSourceObservationErrorV0, KafkaTopicIdV0, UNCLASSIFIED_KAFKA_SENSITIVITY_BITS_V0,
    assemble_validated_kafka_canonical_event_v0,
};
use decant_state::{
    ADMISSION_REPLAY_VERSION_V0, ADMISSION_VERSION_V0, AdmissionConfigV0, AdmissionLimitsV0,
    AdmissionReplayInputV0, AdmissionReplayJournalV0, AdmissionReplayLimitsV0,
    AdmissionReplayOutcomeV1, AdmissionStateV0, ApplyOutcomeV0, encode_admission_snapshot_v1,
    hash_admission_state_v1, replay_admission_journal_v1,
    replay_admission_journal_with_interruption_v1, restore_admission_snapshot_v1,
};

const SOURCE: &str = "payments-kafka-v0";
const CLUSTER: &str = "cluster-incarnation-v0";

fn source(value: &str) -> SourceIdV0 {
    SourceIdV0::new(value).unwrap()
}

fn cluster(value: &str) -> KafkaClusterIdV0 {
    KafkaClusterIdV0::new(value).unwrap()
}

fn topic(byte: u8) -> KafkaTopicIdV0 {
    KafkaTopicIdV0::from_bytes([byte; KAFKA_TOPIC_ID_BYTES_V0]).unwrap()
}

fn contract() -> KafkaSourceContractV0 {
    KafkaSourceContractV0::new(
        KAFKA_SOURCE_CONTRACT_VERSION_V0,
        source(SOURCE),
        cluster(CLUSTER),
        topic(0x5a),
    )
    .unwrap()
}

fn payload(fields: impl IntoIterator<Item = (&'static str, ScalarValueV0)>) -> CanonicalPayloadV0 {
    CanonicalPayloadV0::try_from_fields(
        fields
            .into_iter()
            .map(|(name, value)| (name.to_owned(), value))
            .collect(),
    )
    .unwrap()
}

fn assemble(
    position: KafkaPositionV0,
    key: Option<&[u8]>,
    event_time_ns: i64,
    canonical_payload: CanonicalPayloadV0,
) -> Result<decant_ir::EventEnvelopeV0, KafkaCanonicalEventAssemblyErrorV0> {
    assemble_validated_kafka_canonical_event_v0(
        &contract(),
        &source(SOURCE),
        &cluster(CLUSTER),
        topic(0x5a),
        position,
        SchemaIdV0::new("payment-outcome-v0").unwrap(),
        key,
        event_time_ns,
        canonical_payload,
    )
}

#[test]
fn exact_observation_assembles_complete_conservative_envelope() {
    assert_eq!(KAFKA_CANONICAL_EVENT_ASSEMBLER_VERSION_V0, 0);
    assert_eq!(UNCLASSIFIED_KAFKA_SENSITIVITY_BITS_V0, u64::MAX);

    let expected_payload = payload([
        ("outcome", ScalarValueV0::utf8("error").unwrap()),
        ("retryable", ScalarValueV0::from_bool(true)),
    ]);
    let event = assemble(
        KafkaPositionV0::new(23, 42).unwrap(),
        Some(b"payments-v2/region-b"),
        1_767_226_800_000_000_000,
        expected_payload.clone(),
    )
    .unwrap();

    assert_eq!(event.envelope_version(), ENVELOPE_VERSION_V0);
    assert_eq!(event.source_id(), &source(SOURCE));
    assert_eq!(event.schema_id().as_str(), "payment-outcome-v0");
    assert_eq!(
        event.source_position(),
        SourcePositionV0::Kafka {
            position: KafkaPositionV0::new(23, 42).unwrap(),
        }
    );
    assert_eq!(event.key(), Some(b"payments-v2/region-b".as_slice()));
    assert_eq!(event.event_time_ns(), 1_767_226_800_000_000_000);
    assert_eq!(event.ingest_time_ns(), 0);
    assert_eq!(
        event.sensitivity(),
        SensitivityBitmapV0::from_bits(u64::MAX)
    );
    assert_eq!(event.payload(), &expected_payload);
    assert_eq!(
        event.provenance().as_str(),
        "decant://event/v0/payments-kafka-v0/kafka/23/42"
    );
}

#[test]
fn source_incarnation_precedence_runs_before_event_validation() {
    let configured = contract();
    let oversized_key = vec![0x5a; MAX_KEY_BYTES + 1];
    let canonical_payload = payload([]);
    let position = KafkaPositionV0::new(0, 0).unwrap();
    let schema_id = SchemaIdV0::new("schema-v0").unwrap();

    for (observed_source, observed_cluster, observed_topic, expected) in [
        (
            source("wrong-source-probe"),
            cluster("wrong-cluster-probe"),
            topic(0xa5),
            KafkaSourceObservationErrorV0::SourceMismatch,
        ),
        (
            source(SOURCE),
            cluster("wrong-cluster-probe"),
            topic(0xa5),
            KafkaSourceObservationErrorV0::ClusterMismatch,
        ),
        (
            source(SOURCE),
            cluster(CLUSTER),
            topic(0xa5),
            KafkaSourceObservationErrorV0::TopicIncarnationMismatch,
        ),
    ] {
        assert_eq!(
            assemble_validated_kafka_canonical_event_v0(
                &configured,
                &observed_source,
                &observed_cluster,
                observed_topic,
                position,
                schema_id.clone(),
                Some(&oversized_key),
                998_877_665,
                canonical_payload.clone(),
            ),
            Err(KafkaCanonicalEventAssemblyErrorV0::SourceObservation { category: expected })
        );
    }

    assert_eq!(
        assemble_validated_kafka_canonical_event_v0(
            &configured,
            &source(SOURCE),
            &cluster(CLUSTER),
            topic(0x5a),
            position,
            schema_id,
            Some(&oversized_key),
            998_877_665,
            canonical_payload,
        ),
        Err(
            KafkaCanonicalEventAssemblyErrorV0::CanonicalEventValidation {
                category: ValidationErrorV0::KeyTooLong {
                    maximum: MAX_KEY_BYTES,
                    actual: MAX_KEY_BYTES + 1,
                },
            },
        )
    );
}

#[test]
fn key_boundaries_scalar_kinds_and_canonical_payload_pass_through() {
    let maximum_key = vec![0x7f; MAX_KEY_BYTES];
    assert!(
        assemble(
            KafkaPositionV0::new(0, 0).unwrap(),
            Some(&maximum_key),
            i64::MIN,
            payload([]),
        )
        .is_ok()
    );

    let unkeyed = assemble(KafkaPositionV0::new(0, 1).unwrap(), None, 0, payload([])).unwrap();
    let empty_key = assemble(
        KafkaPositionV0::new(0, 2).unwrap(),
        Some(b""),
        0,
        payload([]),
    )
    .unwrap();
    assert_eq!(unkeyed.key(), None);
    assert_eq!(empty_key.key(), Some(b"".as_slice()));
    assert_ne!(unkeyed, empty_key);

    let canonical_payload = payload([
        ("z_bytes", ScalarValueV0::bytes(&[0, 1, 2]).unwrap()),
        ("a_null", ScalarValueV0::null()),
        ("m_bool", ScalarValueV0::from_bool(false)),
        ("c_i64", ScalarValueV0::from_i64(i64::MIN)),
        ("d_u64", ScalarValueV0::from_u64(u64::MAX)),
        ("e_utf8", ScalarValueV0::utf8("canonical-value").unwrap()),
    ]);
    let event = assemble(
        KafkaPositionV0::new(0, 3).unwrap(),
        Some(b""),
        i64::MAX,
        canonical_payload.clone(),
    )
    .unwrap();
    assert_eq!(event.payload(), &canonical_payload);
    assert_eq!(
        event
            .payload()
            .fields()
            .map(|(name, _)| name)
            .collect::<Vec<_>>(),
        ["a_null", "c_i64", "d_u64", "e_utf8", "m_bool", "z_bytes"]
    );
}

#[test]
fn assembly_is_repeatable_value_safe_and_does_not_touch_frontier() {
    let configured = contract();
    let starting = KafkaPositionV0::new(7, 41).unwrap();
    let frontier = KafkaPartitionFrontierV0::new(
        KAFKA_PARTITION_FRONTIER_VERSION_V0,
        configured.clone(),
        &[starting],
    )
    .unwrap();
    let before: Vec<_> = frontier.iter().collect();
    let expected = assemble_validated_kafka_canonical_event_v0(
        &configured,
        &source(SOURCE),
        &cluster(CLUSTER),
        topic(0x5a),
        KafkaPositionV0::new(7, 42).unwrap(),
        SchemaIdV0::new("probe-schema-v0").unwrap(),
        Some(b"secret-key-probe"),
        998_877_665,
        payload([(
            "secret_field",
            ScalarValueV0::utf8("secret-payload-probe").unwrap(),
        )]),
    )
    .unwrap();
    for _ in 0..128 {
        let candidate = assemble_validated_kafka_canonical_event_v0(
            &configured,
            &source(SOURCE),
            &cluster(CLUSTER),
            topic(0x5a),
            KafkaPositionV0::new(7, 42).unwrap(),
            SchemaIdV0::new("probe-schema-v0").unwrap(),
            Some(b"secret-key-probe"),
            998_877_665,
            payload([(
                "secret_field",
                ScalarValueV0::utf8("secret-payload-probe").unwrap(),
            )]),
        )
        .unwrap();
        assert_eq!(candidate, expected);
    }
    assert_eq!(frontier.iter().collect::<Vec<_>>(), before);

    let error = KafkaCanonicalEventAssemblyErrorV0::SourceObservation {
        category: KafkaSourceObservationErrorV0::TopicIncarnationMismatch,
    };
    let panic = std::panic::catch_unwind(|| {
        assert_eq!(
            expected,
            assemble(
                KafkaPositionV0::new(7, 43).unwrap(),
                Some(b"different-key-probe"),
                998_877_666,
                payload([]),
            )
            .unwrap()
        );
    })
    .unwrap_err();
    let diagnostics = format!(
        "{configured:?} {expected:?} {error:?} {error} {}",
        panic_text(panic)
    );
    for probe in [
        SOURCE,
        CLUSTER,
        "secret-key-probe",
        "different-key-probe",
        "secret-payload-probe",
        "998877665",
        "decant://",
        "5a5a5a5a",
    ] {
        assert!(!diagnostics.contains(probe), "leaked {probe}");
    }
}

#[test]
fn assembled_events_flow_through_admission_snapshot_hash_and_replay_v1() {
    let configured = contract();
    let events: Vec<_> = (0..3)
        .map(|offset| {
            assemble_validated_kafka_canonical_event_v0(
                &configured,
                &source(SOURCE),
                &cluster(CLUSTER),
                topic(0x5a),
                KafkaPositionV0::new(3, offset).unwrap(),
                SchemaIdV0::new("payment-outcome-v0").unwrap(),
                Some(b"payments-v2/region-b"),
                50 + offset,
                payload([("value", ScalarValueV0::from_i64(offset))]),
            )
            .unwrap()
        })
        .collect();

    let admission_config = AdmissionConfigV0::new_with_source_position_kind(
        ADMISSION_VERSION_V0,
        source(SOURCE),
        decant_ir::SourcePositionKindV0::Kafka,
        0,
        100,
        AdmissionLimitsV0::maximum(),
    )
    .unwrap();
    let mut direct = AdmissionStateV0::new(admission_config.clone());
    let mut journal = AdmissionReplayJournalV0::new(
        ADMISSION_REPLAY_VERSION_V0,
        AdmissionReplayLimitsV0::maximum(),
    )
    .unwrap();
    for event in &events {
        assert_eq!(direct.apply_event_v1(event), Ok(ApplyOutcomeV0::Accepted));
        journal
            .try_push(AdmissionReplayInputV0::Event(event.clone()))
            .unwrap();
    }

    let snapshot = encode_admission_snapshot_v1(&direct).unwrap();
    let hash = hash_admission_state_v1(&direct).unwrap();
    let restored = restore_admission_snapshot_v1(&snapshot, &admission_config).unwrap();
    assert_eq!(restored, direct);
    assert_eq!(encode_admission_snapshot_v1(&restored).unwrap(), snapshot);
    assert_eq!(hash_admission_state_v1(&restored).unwrap(), hash);

    let replayed = replay_admission_journal_v1(&admission_config, &journal).unwrap();
    assert_eq!(
        replayed.outcomes(),
        &[
            AdmissionReplayOutcomeV1::Event(Ok(ApplyOutcomeV0::Accepted)),
            AdmissionReplayOutcomeV1::Event(Ok(ApplyOutcomeV0::Accepted)),
            AdmissionReplayOutcomeV1::Event(Ok(ApplyOutcomeV0::Accepted)),
        ]
    );
    assert_eq!(replayed.state(), &direct);
    assert_eq!(
        encode_admission_snapshot_v1(replayed.state()).unwrap(),
        snapshot
    );
    assert_eq!(hash_admission_state_v1(replayed.state()).unwrap(), hash);
    for interruption_after in 0..=journal.len() {
        let interrupted = replay_admission_journal_with_interruption_v1(
            &admission_config,
            &journal,
            interruption_after,
        )
        .unwrap();
        assert_eq!(interrupted.outcomes(), replayed.outcomes());
        assert_eq!(interrupted.state(), replayed.state());
        assert_eq!(
            encode_admission_snapshot_v1(interrupted.state()).unwrap(),
            snapshot
        );
        assert_eq!(hash_admission_state_v1(interrupted.state()).unwrap(), hash);
    }
}

fn panic_text(payload: Box<dyn Any + Send>) -> String {
    if let Some(message) = payload.downcast_ref::<String>() {
        message.clone()
    } else if let Some(message) = payload.downcast_ref::<&str>() {
        (*message).to_owned()
    } else {
        String::new()
    }
}
