use std::collections::BTreeMap;

use decant_fixtures::{
    FixtureRecordV0, GENERATOR_VERSION_V0, SreGeneratorV0, decode_scenario_manifest_v0,
};
use decant_ir::{
    CanonicalPayloadV0, EventEnvelopeV0, ScalarValueV0, SchemaIdV0, SensitivityBitmapV0,
    SourceIdV0, SourcePositionV0,
};
use decant_state::{
    ADMISSION_SNAPSHOT_VERSION_V0, ADMISSION_VERSION_V0, AdmissionConfigV0, AdmissionLimitsV0,
    AdmissionSnapshotDecodeErrorV0, AdmissionSnapshotFieldV0, AdmissionSnapshotOrderV0,
    AdmissionStateV0, ApplyOutcomeV0, EVENT_TIME_RETENTION_VERSION_V0, EventTimeRetentionConfigV0,
    FinalizeOutcomeV0, MAX_ACCEPTED_EVENT_BYTES_V0, MAX_ACCEPTED_IDENTITIES_V0,
    MAX_ADMISSION_SNAPSHOT_BYTES_V0, MAX_PROJECTED_FIELD_CHANGE_ENTRIES_V0,
    MAX_PROJECTED_FIELD_CHANGE_INDEX_BYTES_V0, PRECEDING_EVENT_SELECTOR_VERSION_V0,
    PROJECTED_FIELD_CHANGE_VERSION_V0, PrecedingEventSelectorConfigV0,
    ProjectedFieldChangeConfigV0, ProjectedFieldChangeLimitsV0, REJECTION_CLASS_COUNT_V0,
    RejectionReasonV0, accepted_event_accounted_bytes_v0, encode_admission_snapshot_v0,
    restore_admission_snapshot_v0,
};

const SECOND_NS: i64 = 1_000_000_000;
const MINUTE_NS: i64 = 60 * SECOND_NS;
const PRIMARY_START_NS: i64 = 1_767_225_600_000_000_000;
const PRIMARY_END_NS: i64 = 1_767_226_800_000_000_000;
const PRIMARY_SCENARIO: &[u8] =
    include_bytes!("../../decant-fixtures/contracts/sre-v0/scenario.json");
const GOLDEN_HEX: &str = include_str!("golden/admission-snapshot-v0.hex");

fn source(value: &str) -> SourceIdV0 {
    SourceIdV0::new(value).unwrap()
}

fn schema(value: &str) -> SchemaIdV0 {
    SchemaIdV0::new(value).unwrap()
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

#[allow(clippy::too_many_arguments)]
fn event(
    source_id: &str,
    sequence: u64,
    schema_id: &str,
    key: Option<&[u8]>,
    event_time_ns: i64,
    ingest_time_ns: i64,
    sensitivity: u64,
    payload: CanonicalPayloadV0,
) -> EventEnvelopeV0 {
    EventEnvelopeV0::new(
        0,
        source(source_id),
        schema(schema_id),
        SourcePositionV0::Synthetic { sequence },
        key,
        event_time_ns,
        ingest_time_ns,
        SensitivityBitmapV0::from_bits(sensitivity),
        payload,
    )
    .unwrap()
}

fn value_event(
    source_id: &str,
    sequence: u64,
    schema_id: &str,
    key: Option<&[u8]>,
    event_time_ns: i64,
    ingest_time_ns: i64,
    value: u64,
) -> EventEnvelopeV0 {
    event(
        source_id,
        sequence,
        schema_id,
        key,
        event_time_ns,
        ingest_time_ns,
        value,
        payload([("value", ScalarValueV0::from_u64(value))]),
    )
}

fn config(
    source_id: &str,
    epoch_start_ns: i64,
    epoch_end_ns: i64,
    limits: AdmissionLimitsV0,
) -> AdmissionConfigV0 {
    AdmissionConfigV0::new(
        ADMISSION_VERSION_V0,
        source(source_id),
        epoch_start_ns,
        epoch_end_ns,
        limits,
    )
    .unwrap()
}

fn state(source_id: &str, epoch_start_ns: i64, epoch_end_ns: i64) -> AdmissionStateV0 {
    AdmissionStateV0::new(config(
        source_id,
        epoch_start_ns,
        epoch_end_ns,
        AdmissionLimitsV0::maximum(),
    ))
}

fn sequence(event: &EventEnvelopeV0) -> u64 {
    event.source_position().synthetic_sequence()
}

fn to_hex(bytes: &[u8]) -> String {
    const DIGITS: &[u8; 16] = b"0123456789abcdef";
    let mut encoded = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        encoded.push(char::from(DIGITS[usize::from(byte >> 4)]));
        encoded.push(char::from(DIGITS[usize::from(byte & 0x0f)]));
    }
    encoded
}

fn golden_state() -> AdmissionStateV0 {
    let mut admission = state("source-v0", 0, 1_000);
    assert_eq!(
        admission.apply_event(&event(
            "source-v0",
            7,
            "schema-v0",
            Some(b"k"),
            10,
            99,
            5,
            payload([
                ("flag", ScalarValueV0::from_bool(true)),
                ("value", ScalarValueV0::from_u64(9)),
            ]),
        )),
        Ok(ApplyOutcomeV0::Accepted)
    );
    assert_eq!(
        admission.apply_event(&value_event(
            "source-v0",
            8,
            "schema-v0",
            Some(b"k"),
            1_000,
            1_000,
            10,
        )),
        Ok(ApplyOutcomeV0::Rejected(RejectionReasonV0::OutsideEpoch))
    );
    assert_eq!(
        admission.finalize_source(&source("source-v0"), 1_000),
        FinalizeOutcomeV0::Finalized
    );
    admission
}

#[test]
fn empty_open_and_finalized_states_round_trip_exactly() {
    let open = state("source-v0", i64::MIN, i64::MAX);
    assert_eq!(open.maximum_accepted_event_time_ns(), None);
    assert_eq!(open.watermark_ns(), None);
    let open_bytes = encode_admission_snapshot_v0(&open).unwrap();
    let open_restored = restore_admission_snapshot_v0(&open_bytes).unwrap();
    assert_eq!(open_restored, open);
    assert_eq!(
        encode_admission_snapshot_v0(&open_restored).unwrap(),
        open_bytes
    );

    let mut finalized = state("source-v0", -10, 10);
    assert_eq!(
        finalized.finalize_source(&source("source-v0"), 10),
        FinalizeOutcomeV0::Finalized
    );
    let finalized_bytes = encode_admission_snapshot_v0(&finalized).unwrap();
    let finalized_restored = restore_admission_snapshot_v0(&finalized_bytes).unwrap();
    assert_eq!(finalized_restored, finalized);
    assert_eq!(finalized_restored.watermark_ns(), Some(10));
    assert_ne!(open_bytes, finalized_bytes);
}

#[test]
fn all_scalar_key_identifier_and_timestamp_boundaries_round_trip() {
    let source_id = "s".repeat(64);
    let schema_id = "z".repeat(64);
    let field_name = format!("f{}", "x".repeat(63));
    let mut fields = BTreeMap::from([
        ("a_null".to_owned(), ScalarValueV0::null()),
        ("b_bool".to_owned(), ScalarValueV0::from_bool(true)),
        ("c_i64".to_owned(), ScalarValueV0::from_i64(i64::MIN)),
        ("d_u64".to_owned(), ScalarValueV0::from_u64(u64::MAX)),
        (
            "e_utf8".to_owned(),
            ScalarValueV0::utf8(&"u".repeat(4_096)).unwrap(),
        ),
        (
            "f_bytes".to_owned(),
            ScalarValueV0::bytes(&vec![0xa5; 4_096]).unwrap(),
        ),
    ]);
    fields.insert(field_name, ScalarValueV0::from_u64(1));
    let mut admission = state(&source_id, i64::MIN, i64::MAX);
    for candidate in [
        event(
            &source_id,
            1,
            &schema_id,
            None,
            i64::MIN,
            i64::MIN,
            u64::MAX,
            CanonicalPayloadV0::try_from_fields(fields).unwrap(),
        ),
        value_event(&source_id, 2, &schema_id, Some(b""), 0, 77, 2),
        value_event(
            &source_id,
            3,
            &schema_id,
            Some(&vec![0xff; 256]),
            i64::MAX - 1,
            i64::MAX,
            3,
        ),
    ] {
        assert_eq!(
            admission.apply_event(&candidate),
            Ok(ApplyOutcomeV0::Accepted)
        );
    }

    let bytes = encode_admission_snapshot_v0(&admission).unwrap();
    assert!(bytes.len() <= MAX_ADMISSION_SNAPSHOT_BYTES_V0);
    let restored = restore_admission_snapshot_v0(&bytes).unwrap();
    assert_eq!(restored, admission);
    assert!(
        restored
            .accepted_events()
            .all(|accepted| accepted.ingest_time_ns() == 0)
    );
    assert_eq!(
        restored
            .accepted_events()
            .map(|accepted| accepted.key().map(<[u8]>::to_vec))
            .collect::<Vec<_>>(),
        vec![None, Some(vec![]), Some(vec![0xff; 256])]
    );
}

#[test]
fn every_public_rejection_class_round_trips() {
    let mut cases = Vec::new();

    let mut unexpected = state("source-v0", 0, 100);
    unexpected
        .apply_event(&value_event(
            "other-v0",
            1,
            "schema-v0",
            Some(b"k"),
            10,
            10,
            1,
        ))
        .unwrap();
    cases.push((unexpected, RejectionReasonV0::UnexpectedSource));

    let mut finalized = state("source-v0", 0, 100);
    finalized.finalize_source(&source("source-v0"), 100);
    finalized
        .apply_event(&value_event(
            "source-v0",
            1,
            "schema-v0",
            Some(b"k"),
            10,
            10,
            1,
        ))
        .unwrap();
    cases.push((finalized, RejectionReasonV0::EpochFinalized));

    let mut conflict = state("source-v0", 0, 100);
    conflict
        .apply_event(&value_event(
            "source-v0",
            1,
            "schema-v0",
            Some(b"k"),
            10,
            10,
            1,
        ))
        .unwrap();
    conflict
        .apply_event(&value_event(
            "source-v0",
            1,
            "schema-v1",
            Some(b"k"),
            10,
            10,
            1,
        ))
        .unwrap();
    cases.push((conflict, RejectionReasonV0::ConflictingRedelivery));

    let mut outside = state("source-v0", 0, 100);
    outside
        .apply_event(&value_event(
            "source-v0",
            1,
            "schema-v0",
            Some(b"k"),
            100,
            100,
            1,
        ))
        .unwrap();
    cases.push((outside, RejectionReasonV0::OutsideEpoch));

    let mut late = state("source-v0", 0, 200 * SECOND_NS);
    late.apply_event(&value_event(
        "source-v0",
        1,
        "schema-v0",
        Some(b"k"),
        90 * SECOND_NS,
        90 * SECOND_NS,
        1,
    ))
    .unwrap();
    late.apply_event(&value_event(
        "source-v0",
        2,
        "schema-v0",
        Some(b"k"),
        0,
        91 * SECOND_NS,
        2,
    ))
    .unwrap();
    cases.push((late, RejectionReasonV0::Late));

    let mut identity_capacity = AdmissionStateV0::new(config(
        "source-v0",
        0,
        100,
        AdmissionLimitsV0::new(1, MAX_ACCEPTED_EVENT_BYTES_V0).unwrap(),
    ));
    identity_capacity
        .apply_event(&value_event(
            "source-v0",
            1,
            "schema-v0",
            Some(b"k"),
            10,
            10,
            1,
        ))
        .unwrap();
    identity_capacity
        .apply_event(&value_event(
            "source-v0",
            2,
            "schema-v0",
            Some(b"k"),
            20,
            20,
            2,
        ))
        .unwrap();
    cases.push((
        identity_capacity,
        RejectionReasonV0::AcceptedIdentityCapacity,
    ));

    let first = value_event("source-v0", 1, "schema-v0", Some(b"k"), 10, 10, 1);
    let first_bytes = accepted_event_accounted_bytes_v0(&first).unwrap();
    let mut byte_capacity = AdmissionStateV0::new(config(
        "source-v0",
        0,
        100,
        AdmissionLimitsV0::new(MAX_ACCEPTED_IDENTITIES_V0, first_bytes).unwrap(),
    ));
    byte_capacity.apply_event(&first).unwrap();
    byte_capacity
        .apply_event(&value_event(
            "source-v0",
            2,
            "schema-v0",
            Some(b"k"),
            20,
            20,
            2,
        ))
        .unwrap();
    cases.push((byte_capacity, RejectionReasonV0::AcceptedEventByteCapacity));

    let mut invalid_finalization = state("source-v0", 0, 100);
    invalid_finalization.finalize_source(&source("source-v0"), 99);
    cases.push((invalid_finalization, RejectionReasonV0::InvalidFinalization));

    assert_eq!(cases.len(), REJECTION_CLASS_COUNT_V0);
    for (admission, reason) in cases {
        let range = admission.rejection_summary().range(reason);
        assert_eq!(range.count(), 1, "missing {reason:?} rejection");
        let restored =
            restore_admission_snapshot_v0(&encode_admission_snapshot_v0(&admission).unwrap())
                .unwrap();
        assert_eq!(restored, admission);
        assert_eq!(restored.rejection_summary().range(reason), range);
    }
}

#[test]
fn frozen_golden_bytes_and_restore_are_exact() {
    assert_eq!(ADMISSION_SNAPSHOT_VERSION_V0, 0);
    let expected = golden_state();
    let bytes = encode_admission_snapshot_v0(&expected).unwrap();
    assert_eq!(to_hex(&bytes), GOLDEN_HEX.trim());
    let restored = restore_admission_snapshot_v0(&bytes).unwrap();
    assert_eq!(restored, expected);
    assert_eq!(encode_admission_snapshot_v0(&restored).unwrap(), bytes);
}

#[test]
fn permitted_arrival_permutations_converge_to_identical_bytes() {
    let orders = [
        [0, 1, 2, 3],
        [0, 1, 3, 2],
        [0, 2, 1, 3],
        [0, 2, 3, 1],
        [0, 3, 1, 2],
        [0, 3, 2, 1],
        [1, 0, 2, 3],
        [1, 0, 3, 2],
        [1, 2, 0, 3],
        [1, 2, 3, 0],
        [1, 3, 0, 2],
        [1, 3, 2, 0],
        [2, 0, 1, 3],
        [2, 0, 3, 1],
        [2, 1, 0, 3],
        [2, 1, 3, 0],
        [2, 3, 0, 1],
        [2, 3, 1, 0],
        [3, 0, 1, 2],
        [3, 0, 2, 1],
        [3, 1, 0, 2],
        [3, 1, 2, 0],
        [3, 2, 0, 1],
        [3, 2, 1, 0],
    ];
    let candidates = [
        value_event("source-v0", 4, "schema-v0", Some(b"k"), 90, 90, 4),
        value_event("source-v0", 1, "schema-v0", Some(b"k"), 60, 60, 1),
        value_event("source-v0", 3, "schema-v0", Some(b"k"), 80, 80, 3),
        value_event("source-v0", 2, "schema-v0", Some(b"k"), 70, 70, 2),
    ];
    let mut expected = None;
    for order in orders {
        let mut admission = state("source-v0", 0, 200);
        for index in order {
            assert_eq!(
                admission.apply_event(&candidates[index]),
                Ok(ApplyOutcomeV0::Accepted)
            );
        }
        admission.finalize_source(&source("source-v0"), 200);
        let bytes = encode_admission_snapshot_v0(&admission).unwrap();
        assert_eq!(encode_admission_snapshot_v0(&admission).unwrap(), bytes);
        if let Some(expected) = &expected {
            assert_eq!(&bytes, expected);
        } else {
            expected = Some(bytes);
        }
    }
}

#[test]
fn duplicate_is_a_byte_no_op_but_rejections_are_recoverable_state() {
    let original = value_event("source-v0", 1, "schema-v0", Some(b"k"), 10, 10, 1);
    let mut admission = state("source-v0", 0, 200);
    admission.apply_event(&original).unwrap();
    let accepted = encode_admission_snapshot_v0(&admission).unwrap();
    let redelivery = value_event("source-v0", 1, "schema-v0", Some(b"k"), 10, i64::MAX, 1);
    assert_eq!(
        admission.apply_event(&redelivery),
        Ok(ApplyOutcomeV0::DuplicateNoOp)
    );
    assert_eq!(encode_admission_snapshot_v0(&admission).unwrap(), accepted);

    let conflict = value_event("source-v0", 1, "schema-v1", Some(b"k"), 10, 10, 1);
    assert_eq!(
        admission.apply_event(&conflict),
        Ok(ApplyOutcomeV0::Rejected(
            RejectionReasonV0::ConflictingRedelivery
        ))
    );
    let rejected = encode_admission_snapshot_v0(&admission).unwrap();
    assert_ne!(rejected, accepted);
    assert_eq!(
        restore_admission_snapshot_v0(&rejected)
            .unwrap()
            .rejection_summary()
            .range(RejectionReasonV0::ConflictingRedelivery)
            .count(),
        1
    );
}

#[test]
fn restored_open_state_continues_with_identical_outcomes() {
    let mut uninterrupted = state("source-v0", 0, 200);
    uninterrupted
        .apply_event(&value_event(
            "source-v0",
            1,
            "schema-v0",
            Some(b"k"),
            50,
            50,
            1,
        ))
        .unwrap();
    uninterrupted
        .apply_event(&value_event(
            "source-v0",
            2,
            "schema-v0",
            Some(b"k"),
            90,
            90,
            2,
        ))
        .unwrap();
    let mut restored =
        restore_admission_snapshot_v0(&encode_admission_snapshot_v0(&uninterrupted).unwrap())
            .unwrap();

    let suffix = [
        value_event("source-v0", 2, "schema-v0", Some(b"k"), 90, 999, 2),
        value_event("source-v0", 2, "schema-v1", Some(b"k"), 90, 90, 2),
        value_event("source-v0", 3, "schema-v0", Some(b"k"), 59, 91, 3),
        value_event("source-v0", 4, "schema-v0", Some(b"k"), 100, 100, 4),
        value_event("other-v0", 5, "schema-v0", Some(b"k"), 110, 110, 5),
    ];
    for candidate in suffix {
        assert_eq!(
            uninterrupted.apply_event(&candidate),
            restored.apply_event(&candidate)
        );
        assert_eq!(uninterrupted, restored);
    }
    assert_eq!(
        uninterrupted.finalize_source(&source("source-v0"), 199),
        restored.finalize_source(&source("source-v0"), 199)
    );
    assert_eq!(
        uninterrupted.finalize_source(&source("source-v0"), 200),
        restored.finalize_source(&source("source-v0"), 200)
    );
    assert_eq!(uninterrupted, restored);
}

#[test]
fn primary_sre_state_and_hero_evidence_round_trip() {
    let manifest = decode_scenario_manifest_v0(PRIMARY_SCENARIO).unwrap();
    let generator = SreGeneratorV0::new(GENERATOR_VERSION_V0, &manifest, "sre-v0-primary").unwrap();
    let mut admission = state("sre-v0", PRIMARY_START_NS, PRIMARY_END_NS);
    let mut accepted = 0;
    let mut duplicates = 0;
    let mut late = 0;
    for record in generator {
        match record.unwrap() {
            FixtureRecordV0::Event(candidate) => match admission.apply_event(&candidate).unwrap() {
                ApplyOutcomeV0::Accepted => accepted += 1,
                ApplyOutcomeV0::DuplicateNoOp => duplicates += 1,
                ApplyOutcomeV0::Rejected(RejectionReasonV0::Late) => late += 1,
                ApplyOutcomeV0::Rejected(other) => panic!("unexpected rejection: {other:?}"),
            },
            FixtureRecordV0::EndOfInput(control) => assert_eq!(
                admission.finalize_source(control.source_id(), control.finalized_through_ns()),
                FinalizeOutcomeV0::Finalized
            ),
        }
    }
    assert_eq!((accepted, duplicates, late), (98_750, 1_000, 250));

    let bytes = encode_admission_snapshot_v0(&admission).unwrap();
    let restored = restore_admission_snapshot_v0(&bytes).unwrap();
    assert_eq!(restored, admission);
    assert_eq!(restored.watermark_ns(), Some(PRIMARY_END_NS));
    assert_eq!(restored.usage().accepted_identities(), 98_750);
    assert_eq!(
        restored
            .rejection_summary()
            .range(RejectionReasonV0::Late)
            .count(),
        250
    );

    let retention = EventTimeRetentionConfigV0::new(
        EVENT_TIME_RETENTION_VERSION_V0,
        (PRIMARY_END_NS - PRIMARY_START_NS) as u64,
    )
    .unwrap();
    let change_config = ProjectedFieldChangeConfigV0::new(
        PROJECTED_FIELD_CHANGE_VERSION_V0,
        retention,
        schema("metric-sample-v0"),
        "metric_name",
        "metric_value",
        ProjectedFieldChangeLimitsV0::new(
            MAX_PROJECTED_FIELD_CHANGE_ENTRIES_V0,
            MAX_PROJECTED_FIELD_CHANGE_INDEX_BYTES_V0,
        )
        .unwrap(),
    )
    .unwrap();
    let original_changes = admission
        .materialize_projected_field_changes(change_config.clone())
        .unwrap();
    let restored_changes = restored
        .materialize_projected_field_changes(change_config)
        .unwrap();
    assert_eq!(original_changes.usage(), restored_changes.usage());
    let original_evidence = original_changes
        .entries()
        .map(|(key, discriminator, observation)| {
            (
                key.to_vec(),
                discriminator.to_owned(),
                observation
                    .previous_event()
                    .provenance()
                    .as_str()
                    .to_owned(),
                observation.current_event().provenance().as_str().to_owned(),
            )
        })
        .collect::<Vec<_>>();
    let restored_evidence = restored_changes
        .entries()
        .map(|(key, discriminator, observation)| {
            (
                key.to_vec(),
                discriminator.to_owned(),
                observation
                    .previous_event()
                    .provenance()
                    .as_str()
                    .to_owned(),
                observation.current_event().provenance().as_str().to_owned(),
            )
        })
        .collect::<Vec<_>>();
    assert_eq!(original_evidence, restored_evidence);

    let selector_config = PrecedingEventSelectorConfigV0::new(
        PRECEDING_EVENT_SELECTOR_VERSION_V0,
        retention,
        schema("deployment-v0"),
        b"payments-v2/region-b",
        PRIMARY_END_NS,
        (15 * MINUTE_NS) as u64,
    )
    .unwrap();
    let original_candidate = admission
        .select_preceding_event(selector_config.clone())
        .unwrap()
        .candidate()
        .unwrap()
        .provenance()
        .as_str()
        .to_owned();
    let restored_candidate = restored
        .select_preceding_event(selector_config)
        .unwrap()
        .candidate()
        .unwrap()
        .provenance()
        .as_str()
        .to_owned();
    assert_eq!(original_candidate, restored_candidate);
}

#[derive(Debug)]
struct SnapshotLayout {
    source_start: usize,
    finality: usize,
    first_rejection_tag: usize,
    first_rejection_count: usize,
    first_rejection_saturated: usize,
    event_count: usize,
    records: Vec<(usize, usize)>,
}

fn read_u32(bytes: &[u8], offset: usize) -> usize {
    u32::from_be_bytes(bytes[offset..offset + 4].try_into().unwrap()) as usize
}

fn snapshot_layout(bytes: &[u8]) -> SnapshotLayout {
    let source_length = read_u32(bytes, 20);
    let source_start = 24;
    let finality = source_start + source_length + 8 + 8 + 8 + 8;
    let rejection_count = read_u32(bytes, finality + 1);
    let first_rejection_tag = finality + 1 + 4;
    let first_rejection_count = first_rejection_tag + 1;
    let first_rejection_saturated = first_rejection_count + 8;
    let mut cursor = first_rejection_tag;
    for _ in 0..rejection_count {
        cursor += 1 + 8 + 1;
        let has_range = bytes[cursor];
        cursor += 1;
        if has_range == 1 {
            cursor += 16;
        }
    }
    let event_count = cursor;
    let count = read_u32(bytes, event_count);
    cursor += 4;
    let mut records = Vec::with_capacity(count);
    for _ in 0..count {
        let body_length = read_u32(bytes, cursor);
        records.push((cursor, cursor + 4 + body_length));
        cursor += 4 + body_length;
    }
    assert_eq!(cursor, bytes.len());
    SnapshotLayout {
        source_start,
        finality,
        first_rejection_tag,
        first_rejection_count,
        first_rejection_saturated,
        event_count,
        records,
    }
}

#[derive(Debug)]
struct EventLayout {
    source_position_tag: usize,
    event_time: usize,
    first_scalar_tag: usize,
}

fn event_layout(bytes: &[u8], record_start: usize) -> EventLayout {
    let mut cursor = record_start + 4;
    cursor += 2;
    let source_position_tag = cursor;
    cursor += 1 + 8;
    let schema_length = read_u32(bytes, cursor);
    cursor += 4 + schema_length;
    let has_key = bytes[cursor];
    cursor += 1;
    if has_key == 1 {
        let key_length = read_u32(bytes, cursor);
        cursor += 4 + key_length;
    }
    let event_time = cursor;
    cursor += 8 + 8;
    let field_count = read_u32(bytes, cursor);
    assert!(field_count > 0);
    cursor += 4;
    let field_length = read_u32(bytes, cursor);
    cursor += 4 + field_length;
    EventLayout {
        source_position_tag,
        event_time,
        first_scalar_tag: cursor,
    }
}

fn replace_u16(bytes: &mut [u8], offset: usize, value: u16) {
    bytes[offset..offset + 2].copy_from_slice(&value.to_be_bytes());
}

fn replace_u32(bytes: &mut [u8], offset: usize, value: u32) {
    bytes[offset..offset + 4].copy_from_slice(&value.to_be_bytes());
}

fn replace_u64(bytes: &mut [u8], offset: usize, value: u64) {
    bytes[offset..offset + 8].copy_from_slice(&value.to_be_bytes());
}

fn replace_i64(bytes: &mut [u8], offset: usize, value: i64) {
    bytes[offset..offset + 8].copy_from_slice(&value.to_be_bytes());
}

#[test]
fn truncation_trailing_and_global_size_bounds_fail_closed() {
    let bytes = encode_admission_snapshot_v0(&golden_state()).unwrap();
    for length in 0..bytes.len() {
        assert!(
            restore_admission_snapshot_v0(&bytes[..length]).is_err(),
            "prefix length {length} unexpectedly restored"
        );
    }
    let mut trailing = bytes;
    trailing.push(0);
    assert_eq!(
        restore_admission_snapshot_v0(&trailing),
        Err(AdmissionSnapshotDecodeErrorV0::TrailingBytes)
    );

    let oversized = vec![0_u8; MAX_ADMISSION_SNAPSHOT_BYTES_V0 + 1];
    assert_eq!(
        restore_admission_snapshot_v0(&oversized),
        Err(AdmissionSnapshotDecodeErrorV0::DocumentTooLarge {
            maximum: MAX_ADMISSION_SNAPSHOT_BYTES_V0,
            actual: MAX_ADMISSION_SNAPSHOT_BYTES_V0 + 1,
        })
    );
}

#[test]
fn hostile_header_range_and_event_mutations_are_rejected() {
    let original = encode_admission_snapshot_v0(&golden_state()).unwrap();
    let layout = snapshot_layout(&original);
    let event = event_layout(&original, layout.records[0].0);

    let mut cases = Vec::new();
    let mut invalid_magic = original.clone();
    invalid_magic[0] ^= 1;
    cases.push(invalid_magic);
    let mut snapshot_version = original.clone();
    replace_u16(&mut snapshot_version, 8, 1);
    cases.push(snapshot_version);
    let mut admission_version = original.clone();
    replace_u16(&mut admission_version, 10, 1);
    cases.push(admission_version);
    let mut lateness = original.clone();
    replace_i64(&mut lateness, 12, 0);
    cases.push(lateness);
    let mut source_length = original.clone();
    replace_u32(&mut source_length, 20, u32::MAX);
    cases.push(source_length);
    let mut source_utf8 = original.clone();
    source_utf8[layout.source_start] = 0xff;
    cases.push(source_utf8);
    let mut finality = original.clone();
    finality[layout.finality] = 2;
    cases.push(finality);
    let mut rejection_order = original.clone();
    rejection_order[layout.first_rejection_tag] = 1;
    cases.push(rejection_order);
    let mut invalid_empty_range = original.clone();
    invalid_empty_range[layout.first_rejection_saturated] = 1;
    cases.push(invalid_empty_range);
    let mut too_many_events = original.clone();
    replace_u32(&mut too_many_events, layout.event_count, u32::MAX);
    cases.push(too_many_events);
    let mut source_position = original.clone();
    source_position[event.source_position_tag] = 1;
    cases.push(source_position);
    let mut scalar_tag = original.clone();
    scalar_tag[event.first_scalar_tag] = 0xff;
    cases.push(scalar_tag);
    let mut outside_epoch = original.clone();
    replace_i64(&mut outside_epoch, event.event_time, 1_000);
    cases.push(outside_epoch);
    let mut short_record = original.clone();
    let record_length = read_u32(&short_record, layout.records[0].0);
    replace_u32(
        &mut short_record,
        layout.records[0].0,
        (record_length - 1) as u32,
    );
    cases.push(short_record);
    let mut zero_identity_limit = original.clone();
    replace_u64(&mut zero_identity_limit, layout.finality - 16, 0);
    cases.push(zero_identity_limit);
    let mut one_byte_limit = original.clone();
    replace_u64(&mut one_byte_limit, layout.finality - 8, 1);
    cases.push(one_byte_limit);

    for candidate in cases {
        assert!(restore_admission_snapshot_v0(&candidate).is_err());
    }

    let mut invalid_count_range = original;
    replace_u64(&mut invalid_count_range, layout.first_rejection_count, 1);
    assert!(restore_admission_snapshot_v0(&invalid_count_range).is_err());
}

#[test]
fn descending_event_and_payload_field_orders_are_rejected() {
    let mut two_events = state("source-v0", 0, 100);
    for candidate in [
        value_event("source-v0", 1, "schema-v0", Some(b"k"), 10, 10, 1),
        value_event("source-v0", 2, "schema-v0", Some(b"k"), 20, 20, 2),
    ] {
        two_events.apply_event(&candidate).unwrap();
    }
    let bytes = encode_admission_snapshot_v0(&two_events).unwrap();
    let layout = snapshot_layout(&bytes);
    let first = layout.records[0];
    let second = layout.records[1];
    let mut reversed = bytes[..first.0].to_vec();
    reversed.extend_from_slice(&bytes[second.0..second.1]);
    reversed.extend_from_slice(&bytes[first.0..first.1]);
    assert_eq!(
        restore_admission_snapshot_v0(&reversed),
        Err(AdmissionSnapshotDecodeErrorV0::NonCanonicalOrder {
            order: AdmissionSnapshotOrderV0::AcceptedEvents,
        })
    );

    let mut field_order = state("source-v0", 0, 100);
    field_order
        .apply_event(&event(
            "source-v0",
            1,
            "schema-v0",
            Some(b"k"),
            10,
            10,
            0,
            payload([
                ("aa", ScalarValueV0::from_u64(1)),
                ("bb", ScalarValueV0::from_u64(2)),
            ]),
        ))
        .unwrap();
    let mut bytes = encode_admission_snapshot_v0(&field_order).unwrap();
    let record = snapshot_layout(&bytes).records[0];
    let mut cursor = event_layout(&bytes, record.0).first_scalar_tag;
    let first_field_start = cursor - 4 - 2;
    cursor += 1 + 8;
    let second_field_start = cursor;
    let second_name_length = read_u32(&bytes, second_field_start);
    let second_field_end = second_field_start + 4 + second_name_length + 1 + 8;
    let first_field = bytes[first_field_start..second_field_start].to_vec();
    let second_field = bytes[second_field_start..second_field_end].to_vec();
    bytes.splice(first_field_start..second_field_end, second_field);
    bytes.splice(
        first_field_start + (second_field_end - second_field_start)
            ..first_field_start + (second_field_end - second_field_start),
        first_field,
    );
    assert_eq!(
        restore_admission_snapshot_v0(&bytes),
        Err(AdmissionSnapshotDecodeErrorV0::NonCanonicalOrder {
            order: AdmissionSnapshotOrderV0::PayloadFields,
        })
    );
}

#[test]
fn diagnostics_are_content_independent_and_do_not_echo_snapshot_values() {
    fn diagnostic(
        source_id: &str,
        schema_id: &str,
        key: &[u8],
        text: &str,
        times: (i64, i64),
        sensitivity: u64,
    ) -> String {
        let mut admission = state(source_id, 0, 9_000_000_000);
        admission
            .apply_event(&event(
                source_id,
                71,
                schema_id,
                Some(key),
                times.0,
                times.1,
                sensitivity,
                payload([("secret", ScalarValueV0::utf8(text).unwrap())]),
            ))
            .unwrap();
        let mut malformed = encode_admission_snapshot_v0(&admission).unwrap();
        malformed[0] ^= 1;
        format!(
            "{admission:?}|{:?}",
            restore_admission_snapshot_v0(&malformed).unwrap_err()
        )
    }

    let first = diagnostic(
        "source-secret-a",
        "schema-secret-a",
        b"key-secret-a",
        "payload-secret-a",
        (4_242_424_242, 4_242_424_243),
        0xfeed_feed_feed_fe01,
    );
    let second = diagnostic(
        "source-secret-b",
        "schema-secret-b",
        b"key-secret-b",
        "payload-secret-b",
        (4_343_434_343, 4_343_434_344),
        0xdead_dead_dead_de02,
    );
    assert_eq!(first, second);
    for probe in [
        "source-secret-a",
        "schema-secret-a",
        "key-secret-a",
        "payload-secret-a",
        "decant://event/v0/source-secret-a/synthetic/71",
        "71",
        "4242424242",
        "4242424243",
        "feedfeedfeedfe01",
    ] {
        assert!(!first.contains(probe), "diagnostic exposed {probe}");
        assert!(
            !first.contains(&to_hex(probe.as_bytes())),
            "diagnostic exposed hex for {probe}"
        );
    }
}

#[test]
fn public_snapshot_error_categories_are_typed() {
    assert_eq!(
        restore_admission_snapshot_v0(b"not-a-snapshot"),
        Err(AdmissionSnapshotDecodeErrorV0::InvalidMagic)
    );
    let mut version = encode_admission_snapshot_v0(&golden_state()).unwrap();
    replace_u16(&mut version, 8, 7);
    assert_eq!(
        restore_admission_snapshot_v0(&version),
        Err(AdmissionSnapshotDecodeErrorV0::UnsupportedSnapshotVersion { actual: 7 })
    );
    let too_short = &b"DECANTST"[..7];
    assert_eq!(
        restore_admission_snapshot_v0(too_short),
        Err(AdmissionSnapshotDecodeErrorV0::Truncated {
            field: AdmissionSnapshotFieldV0::Magic,
        })
    );
}

#[test]
fn accepted_event_sequence_and_provenance_are_exact_after_restore() {
    let admission = golden_state();
    let restored =
        restore_admission_snapshot_v0(&encode_admission_snapshot_v0(&admission).unwrap()).unwrap();
    let original = admission.accepted_events().next().unwrap();
    let decoded = restored.accepted_events().next().unwrap();
    assert_eq!(sequence(original), 7);
    assert_eq!(sequence(decoded), 7);
    assert_eq!(decoded, original);
    assert_eq!(decoded.ingest_time_ns(), 0);
    assert_eq!(decoded.provenance(), original.provenance());
}
