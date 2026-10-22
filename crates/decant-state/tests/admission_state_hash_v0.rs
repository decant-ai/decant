use std::collections::BTreeSet;
use std::fmt::Write;

use decant_fixtures::{
    FixtureRecordV0, GENERATOR_VERSION_V0, SreGeneratorV0, decode_scenario_manifest_v0,
};
use decant_ir::{
    CanonicalPayloadV0, EventEnvelopeV0, ScalarValueV0, SchemaIdV0, SensitivityBitmapV0,
    SourceIdV0, SourcePositionV0,
};
use decant_state::{
    ADMISSION_STATE_HASH_ALGORITHM_V0, ADMISSION_STATE_HASH_DOMAIN_V0,
    ADMISSION_STATE_HASH_TEXT_BYTES_V0, ADMISSION_STATE_HASH_VERSION_V0, ADMISSION_VERSION_V0,
    AdmissionConfigV0, AdmissionLimitsV0, AdmissionSnapshotEncodeErrorV0, AdmissionSnapshotFieldV0,
    AdmissionStateHashErrorV0, AdmissionStateV0, ApplyOutcomeV0, FinalizeOutcomeV0,
    MAX_ACCEPTED_EVENT_BYTES_V0, MAX_ACCEPTED_IDENTITIES_V0, REJECTION_CLASS_COUNT_V0,
    RejectionReasonV0, accepted_event_accounted_bytes_v0, encode_admission_snapshot_v0,
    hash_admission_state_v0, restore_admission_snapshot_v0,
};

const SECOND_NS: i64 = 1_000_000_000;
const PRIMARY_START_NS: i64 = 1_767_225_600_000_000_000;
const PRIMARY_END_NS: i64 = 1_767_226_800_000_000_000;
const PRIMARY_SCENARIO: &[u8] =
    include_bytes!("../../decant-fixtures/contracts/sre-v0/scenario.json");
const SNAPSHOT_GOLDEN_HEX: &str = include_str!("golden/admission-snapshot-v0.hex");
const HASH_GOLDEN_PREIMAGE_HEX: &str = include_str!("golden/admission-state-hash-v0-preimage.hex");
const HASH_GOLDEN_VECTORS: &str = include_str!("golden/admission-state-hash-v0.txt");

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

fn accept(admission: &mut AdmissionStateV0, candidate: &EventEnvelopeV0) {
    assert_eq!(
        admission.apply_event(candidate),
        Ok(ApplyOutcomeV0::Accepted)
    );
}

fn hash_text(admission: &AdmissionStateV0) -> String {
    hash_admission_state_v0(admission)
        .unwrap()
        .to_canonical_string()
}

fn copy_state(admission: &AdmissionStateV0) -> AdmissionStateV0 {
    restore_admission_snapshot_v0(&encode_admission_snapshot_v0(admission).unwrap()).unwrap()
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
    accept(
        &mut admission,
        &event(
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
        ),
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

fn scenario_state(scenario_id: &str) -> AdmissionStateV0 {
    let manifest = decode_scenario_manifest_v0(PRIMARY_SCENARIO).unwrap();
    let generator = SreGeneratorV0::new(GENERATOR_VERSION_V0, &manifest, scenario_id).unwrap();
    let mut admission = state("sre-v0", PRIMARY_START_NS, PRIMARY_END_NS);
    for record in generator {
        match record.unwrap() {
            FixtureRecordV0::Event(candidate) => match admission.apply_event(&candidate).unwrap() {
                ApplyOutcomeV0::Accepted
                | ApplyOutcomeV0::DuplicateNoOp
                | ApplyOutcomeV0::Rejected(RejectionReasonV0::Late) => {}
                ApplyOutcomeV0::Rejected(other) => panic!("unexpected rejection: {other:?}"),
            },
            FixtureRecordV0::EndOfInput(control) => assert_eq!(
                admission.finalize_source(control.source_id(), control.finalized_through_ns()),
                FinalizeOutcomeV0::Finalized
            ),
        }
    }
    admission
}

fn vector_document() -> String {
    let mut document = String::new();
    let open = state("source-v0", 0, 100);
    writeln!(document, "empty-open={}", hash_text(&open)).unwrap();

    let mut finalized = state("source-v0", 0, 100);
    assert_eq!(
        finalized.finalize_source(&source("source-v0"), 100),
        FinalizeOutcomeV0::Finalized
    );
    writeln!(document, "empty-finalized={}", hash_text(&finalized)).unwrap();
    writeln!(document, "golden={}", hash_text(&golden_state())).unwrap();

    for scenario_id in [
        "sre-v0-primary",
        "sre-v0-region-swap",
        "sre-v0-service-swap",
        "sre-v0-no-regression",
    ] {
        writeln!(
            document,
            "{scenario_id}={}",
            hash_text(&scenario_state(scenario_id))
        )
        .unwrap();
    }
    document
}

#[test]
fn golden_state_preimage_and_hash_are_exact() {
    let snapshot = encode_admission_snapshot_v0(&golden_state()).unwrap();
    assert_eq!(to_hex(&snapshot), SNAPSHOT_GOLDEN_HEX.trim());

    let mut preimage =
        Vec::with_capacity(4 + ADMISSION_STATE_HASH_DOMAIN_V0.len() + 8 + snapshot.len());
    preimage.extend_from_slice(
        &u32::try_from(ADMISSION_STATE_HASH_DOMAIN_V0.len())
            .unwrap()
            .to_be_bytes(),
    );
    preimage.extend_from_slice(ADMISSION_STATE_HASH_DOMAIN_V0);
    preimage.extend_from_slice(&u64::try_from(snapshot.len()).unwrap().to_be_bytes());
    preimage.extend_from_slice(&snapshot);
    assert_eq!(to_hex(&preimage), HASH_GOLDEN_PREIMAGE_HEX.trim());
    assert_eq!(
        hash_text(&golden_state()),
        "sha256:v0:4714fb0be02f29a5c9d3be2625238fd0823fc544d51a1cba50e1bfa2e084cfd8"
    );
}

#[test]
fn committed_empty_golden_and_sre_vectors_are_exact() {
    let actual = vector_document();
    assert_eq!(actual, HASH_GOLDEN_VECTORS);
    let mut values = BTreeSet::new();
    for line in actual.lines() {
        let (_, value) = line.split_once('=').unwrap();
        assert_eq!(value.len(), ADMISSION_STATE_HASH_TEXT_BYTES_V0);
        assert!(value.starts_with(&format!(
            "{ADMISSION_STATE_HASH_ALGORITHM_V0}:v{ADMISSION_STATE_HASH_VERSION_V0}:"
        )));
        assert!(
            value
                .bytes()
                .skip(10)
                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
        );
        assert!(values.insert(value));
    }
    assert_eq!(values.len(), 7);
}

#[test]
fn repeated_hash_and_snapshot_restore_are_identical() {
    let admission = golden_state();
    let first = hash_admission_state_v0(&admission).unwrap();
    let second = hash_admission_state_v0(&admission).unwrap();
    assert_eq!(first, second);
    assert_eq!(first.to_string(), second.to_string());

    let restored =
        restore_admission_snapshot_v0(&encode_admission_snapshot_v0(&admission).unwrap()).unwrap();
    assert_eq!(hash_admission_state_v0(&restored).unwrap(), first);
}

#[test]
fn all_twenty_four_accepted_arrival_permutations_converge() {
    let candidates = [
        value_event("source-v0", 1, "schema-v0", Some(b"a"), 10, 10, 1),
        value_event("source-v0", 2, "schema-v0", Some(b"b"), 20, 20, 2),
        value_event("source-v0", 3, "schema-v0", Some(b"c"), 30, 30, 3),
        value_event("source-v0", 4, "schema-v0", Some(b"d"), 40, 40, 4),
    ];
    let mut hashes = BTreeSet::new();
    let mut permutations = 0;
    for first in 0..4 {
        for second in 0..4 {
            for third in 0..4 {
                for fourth in 0..4 {
                    let order = [first, second, third, fourth];
                    if order.iter().copied().collect::<BTreeSet<_>>().len() != 4 {
                        continue;
                    }
                    let mut admission = state("source-v0", 0, 100);
                    for index in order {
                        accept(&mut admission, &candidates[index]);
                    }
                    hashes.insert(hash_admission_state_v0(&admission).unwrap());
                    permutations += 1;
                }
            }
        }
    }
    assert_eq!(permutations, 24);
    assert_eq!(hashes.len(), 1);
}

#[test]
fn exact_and_ingest_only_redelivery_are_hash_no_ops() {
    let original = value_event("source-v0", 1, "schema-v0", Some(b"k"), 10, 10, 1);
    let mut admission = state("source-v0", 0, 100);
    accept(&mut admission, &original);
    let accepted_hash = hash_admission_state_v0(&admission).unwrap();

    assert_eq!(
        admission.apply_event(&original),
        Ok(ApplyOutcomeV0::DuplicateNoOp)
    );
    assert_eq!(hash_admission_state_v0(&admission).unwrap(), accepted_hash);

    let ingest_only = value_event("source-v0", 1, "schema-v0", Some(b"k"), 10, 99, 1);
    assert_eq!(
        admission.apply_event(&ingest_only),
        Ok(ApplyOutcomeV0::DuplicateNoOp)
    );
    assert_eq!(hash_admission_state_v0(&admission).unwrap(), accepted_hash);
}

fn assert_rejection_is_only_state_change(
    before: &AdmissionStateV0,
    after: &AdmissionStateV0,
    reason: RejectionReasonV0,
) {
    assert_eq!(before.config(), after.config());
    assert_eq!(before.usage(), after.usage());
    assert_eq!(before.is_finalized(), after.is_finalized());
    assert_eq!(before.watermark_ns(), after.watermark_ns());
    assert_eq!(
        before.accepted_events().collect::<Vec<_>>(),
        after.accepted_events().collect::<Vec<_>>()
    );
    assert_eq!(before.rejection_summary().range(reason).count(), 0);
    assert_eq!(after.rejection_summary().range(reason).count(), 1);
    let after_hash = hash_admission_state_v0(after).unwrap();
    assert_eq!(hash_admission_state_v0(after).unwrap(), after_hash);
    assert_ne!(hash_admission_state_v0(before).unwrap(), after_hash);
}

#[test]
fn every_public_rejection_class_changes_only_its_summary_and_hash() {
    let mut seen = BTreeSet::new();

    let mut after = state("source-v0", 0, 100);
    let before = copy_state(&after);
    assert_eq!(
        after.apply_event(&value_event("other-v0", 1, "schema-v0", None, 10, 10, 1)),
        Ok(ApplyOutcomeV0::Rejected(
            RejectionReasonV0::UnexpectedSource
        ))
    );
    assert_rejection_is_only_state_change(&before, &after, RejectionReasonV0::UnexpectedSource);
    seen.insert(RejectionReasonV0::UnexpectedSource as u8);

    let mut after = state("source-v0", 0, 100);
    assert_eq!(
        after.finalize_source(&source("source-v0"), 100),
        FinalizeOutcomeV0::Finalized
    );
    let before = copy_state(&after);
    assert_eq!(
        after.apply_event(&value_event("source-v0", 1, "schema-v0", None, 10, 10, 1)),
        Ok(ApplyOutcomeV0::Rejected(RejectionReasonV0::EpochFinalized))
    );
    assert_rejection_is_only_state_change(&before, &after, RejectionReasonV0::EpochFinalized);
    seen.insert(RejectionReasonV0::EpochFinalized as u8);

    let mut after = state("source-v0", 0, 100);
    accept(
        &mut after,
        &value_event("source-v0", 1, "schema-v0", None, 10, 10, 1),
    );
    let before = copy_state(&after);
    assert_eq!(
        after.apply_event(&value_event("source-v0", 1, "schema-v1", None, 10, 10, 1)),
        Ok(ApplyOutcomeV0::Rejected(
            RejectionReasonV0::ConflictingRedelivery
        ))
    );
    assert_rejection_is_only_state_change(
        &before,
        &after,
        RejectionReasonV0::ConflictingRedelivery,
    );
    seen.insert(RejectionReasonV0::ConflictingRedelivery as u8);

    let mut after = state("source-v0", 0, 100);
    let before = copy_state(&after);
    assert_eq!(
        after.apply_event(&value_event("source-v0", 1, "schema-v0", None, 100, 100, 1)),
        Ok(ApplyOutcomeV0::Rejected(RejectionReasonV0::OutsideEpoch))
    );
    assert_rejection_is_only_state_change(&before, &after, RejectionReasonV0::OutsideEpoch);
    seen.insert(RejectionReasonV0::OutsideEpoch as u8);

    let mut after = state("source-v0", 0, 200 * SECOND_NS);
    accept(
        &mut after,
        &value_event(
            "source-v0",
            1,
            "schema-v0",
            None,
            90 * SECOND_NS,
            90 * SECOND_NS,
            1,
        ),
    );
    let before = copy_state(&after);
    assert_eq!(
        after.apply_event(&value_event(
            "source-v0",
            2,
            "schema-v0",
            None,
            0,
            91 * SECOND_NS,
            2
        )),
        Ok(ApplyOutcomeV0::Rejected(RejectionReasonV0::Late))
    );
    assert_rejection_is_only_state_change(&before, &after, RejectionReasonV0::Late);
    seen.insert(RejectionReasonV0::Late as u8);

    let mut after = AdmissionStateV0::new(config(
        "source-v0",
        0,
        100,
        AdmissionLimitsV0::new(1, MAX_ACCEPTED_EVENT_BYTES_V0).unwrap(),
    ));
    accept(
        &mut after,
        &value_event("source-v0", 1, "schema-v0", None, 10, 10, 1),
    );
    let before = copy_state(&after);
    assert_eq!(
        after.apply_event(&value_event("source-v0", 2, "schema-v0", None, 20, 20, 2)),
        Ok(ApplyOutcomeV0::Rejected(
            RejectionReasonV0::AcceptedIdentityCapacity
        ))
    );
    assert_rejection_is_only_state_change(
        &before,
        &after,
        RejectionReasonV0::AcceptedIdentityCapacity,
    );
    seen.insert(RejectionReasonV0::AcceptedIdentityCapacity as u8);

    let first = value_event("source-v0", 1, "schema-v0", None, 10, 10, 1);
    let first_bytes = accepted_event_accounted_bytes_v0(&first).unwrap();
    let mut after = AdmissionStateV0::new(config(
        "source-v0",
        0,
        100,
        AdmissionLimitsV0::new(MAX_ACCEPTED_IDENTITIES_V0, first_bytes).unwrap(),
    ));
    accept(&mut after, &first);
    let before = copy_state(&after);
    assert_eq!(
        after.apply_event(&value_event("source-v0", 2, "schema-v0", None, 20, 20, 2)),
        Ok(ApplyOutcomeV0::Rejected(
            RejectionReasonV0::AcceptedEventByteCapacity
        ))
    );
    assert_rejection_is_only_state_change(
        &before,
        &after,
        RejectionReasonV0::AcceptedEventByteCapacity,
    );
    seen.insert(RejectionReasonV0::AcceptedEventByteCapacity as u8);

    let mut after = state("source-v0", 0, 100);
    let before = copy_state(&after);
    assert_eq!(
        after.finalize_source(&source("source-v0"), 99),
        FinalizeOutcomeV0::Rejected(RejectionReasonV0::InvalidFinalization)
    );
    assert_rejection_is_only_state_change(&before, &after, RejectionReasonV0::InvalidFinalization);
    seen.insert(RejectionReasonV0::InvalidFinalization as u8);

    assert_eq!(seen.len(), REJECTION_CLASS_COUNT_V0);
}

fn state_with(candidate: EventEnvelopeV0) -> AdmissionStateV0 {
    let mut admission = state("source-v0", -100, 100);
    accept(&mut admission, &candidate);
    admission
}

#[test]
fn exercised_configuration_and_event_semantics_have_distinct_hashes() {
    let mut hashes = BTreeSet::new();
    let mut insert = |label: &str, admission: AdmissionStateV0| {
        assert!(
            hashes.insert(hash_admission_state_v0(&admission).unwrap()),
            "duplicate hash for exercised variant {label}"
        );
    };

    insert("empty", state("source-v0", -100, 100));
    insert("source", state("source-v1", -100, 100));
    insert("epoch-start", state("source-v0", -99, 100));
    insert("epoch-end", state("source-v0", -100, 101));
    insert(
        "identity-limit",
        AdmissionStateV0::new(config(
            "source-v0",
            -100,
            100,
            AdmissionLimitsV0::new(1, MAX_ACCEPTED_EVENT_BYTES_V0).unwrap(),
        )),
    );
    insert(
        "byte-limit",
        AdmissionStateV0::new(config(
            "source-v0",
            -100,
            100,
            AdmissionLimitsV0::new(MAX_ACCEPTED_IDENTITIES_V0, 1).unwrap(),
        )),
    );
    let mut finalized = state("source-v0", -100, 100);
    assert_eq!(
        finalized.finalize_source(&source("source-v0"), 100),
        FinalizeOutcomeV0::Finalized
    );
    insert("finalized", finalized);

    let variants = [
        (
            "baseline",
            event(
                "source-v0",
                1,
                "schema-v0",
                None,
                0,
                0,
                0,
                payload([("value", ScalarValueV0::from_u64(1))]),
            ),
        ),
        (
            "sequence",
            event(
                "source-v0",
                2,
                "schema-v0",
                None,
                0,
                0,
                0,
                payload([("value", ScalarValueV0::from_u64(1))]),
            ),
        ),
        (
            "schema",
            event(
                "source-v0",
                1,
                "schema-v1",
                None,
                0,
                0,
                0,
                payload([("value", ScalarValueV0::from_u64(1))]),
            ),
        ),
        (
            "key-empty",
            event(
                "source-v0",
                1,
                "schema-v0",
                Some(b""),
                0,
                0,
                0,
                payload([("value", ScalarValueV0::from_u64(1))]),
            ),
        ),
        (
            "key-bytes",
            event(
                "source-v0",
                1,
                "schema-v0",
                Some(b"k"),
                0,
                0,
                0,
                payload([("value", ScalarValueV0::from_u64(1))]),
            ),
        ),
        (
            "event-time",
            event(
                "source-v0",
                1,
                "schema-v0",
                None,
                1,
                0,
                0,
                payload([("value", ScalarValueV0::from_u64(1))]),
            ),
        ),
        (
            "sensitivity",
            event(
                "source-v0",
                1,
                "schema-v0",
                None,
                0,
                0,
                1,
                payload([("value", ScalarValueV0::from_u64(1))]),
            ),
        ),
        (
            "field-name",
            event(
                "source-v0",
                1,
                "schema-v0",
                None,
                0,
                0,
                0,
                payload([("other", ScalarValueV0::from_u64(1))]),
            ),
        ),
        (
            "null",
            event(
                "source-v0",
                1,
                "schema-v0",
                None,
                0,
                0,
                0,
                payload([("value", ScalarValueV0::null())]),
            ),
        ),
        (
            "bool",
            event(
                "source-v0",
                1,
                "schema-v0",
                None,
                0,
                0,
                0,
                payload([("value", ScalarValueV0::from_bool(true))]),
            ),
        ),
        (
            "i64",
            event(
                "source-v0",
                1,
                "schema-v0",
                None,
                0,
                0,
                0,
                payload([("value", ScalarValueV0::from_i64(1))]),
            ),
        ),
        (
            "u64-value",
            event(
                "source-v0",
                1,
                "schema-v0",
                None,
                0,
                0,
                0,
                payload([("value", ScalarValueV0::from_u64(2))]),
            ),
        ),
        (
            "utf8",
            event(
                "source-v0",
                1,
                "schema-v0",
                None,
                0,
                0,
                0,
                payload([("value", ScalarValueV0::utf8("1").unwrap())]),
            ),
        ),
        (
            "bytes",
            event(
                "source-v0",
                1,
                "schema-v0",
                None,
                0,
                0,
                0,
                payload([("value", ScalarValueV0::bytes(b"1").unwrap())]),
            ),
        ),
        (
            "payload-cardinality",
            event(
                "source-v0",
                1,
                "schema-v0",
                None,
                0,
                0,
                0,
                payload([
                    ("extra", ScalarValueV0::from_bool(false)),
                    ("value", ScalarValueV0::from_u64(1)),
                ]),
            ),
        ),
    ];
    for (label, candidate) in variants {
        insert(label, state_with(candidate));
    }
    assert_eq!(hashes.len(), 22);
}

#[test]
fn debug_and_errors_do_not_expose_digest_or_content_probes() {
    let admission = golden_state();
    let hash = hash_admission_state_v0(&admission).unwrap();
    let digest_hex = to_hex(hash.as_bytes());
    let debug = format!("{hash:?}");
    assert!(!debug.contains(&digest_hex));
    assert!(debug.contains("<redacted>"));

    let error = AdmissionStateHashErrorV0::SnapshotEncoding(
        AdmissionSnapshotEncodeErrorV0::StateInvariant {
            field: AdmissionSnapshotFieldV0::AcceptedEventBytes,
        },
    );
    let probes = [
        "source-v0",
        "schema-v0",
        "flag",
        "value",
        "decant://event/v0/source-v0/synthetic/7",
        "4714fb0be02f29a5c9d3be2625238fd0823fc544d51a1cba50e1bfa2e084cfd8",
    ];
    for rendered in [format!("{error:?}"), error.to_string(), debug] {
        for probe in probes {
            assert!(!rendered.contains(probe));
        }
    }
}
