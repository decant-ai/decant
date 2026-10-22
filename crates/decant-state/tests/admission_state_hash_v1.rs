use std::collections::BTreeSet;
use std::fmt::Write;

use decant_ir::{
    CanonicalPayloadV0, EventEnvelopeV0, ScalarValueV0, SchemaIdV0, SensitivityBitmapV0,
    SourceIdV0, SourcePositionKindV0, SourcePositionV0,
};
use decant_state::{
    ADMISSION_STATE_HASH_ALGORITHM_V0, ADMISSION_STATE_HASH_ALGORITHM_V1,
    ADMISSION_STATE_HASH_DOMAIN_V0, ADMISSION_STATE_HASH_DOMAIN_V1,
    ADMISSION_STATE_HASH_TEXT_BYTES_V1, ADMISSION_STATE_HASH_VERSION_V0,
    ADMISSION_STATE_HASH_VERSION_V1, ADMISSION_VERSION_V0, AdmissionConfigV0, AdmissionErrorV0,
    AdmissionLimitsV0, AdmissionSnapshotEncodeErrorV0, AdmissionSnapshotEncodeErrorV1,
    AdmissionSnapshotFieldV0, AdmissionSnapshotFieldV1, AdmissionStateHashErrorV0,
    AdmissionStateHashErrorV1, AdmissionStateV0, ApplyOutcomeV0, FinalizeOutcomeV0,
    MAX_ACCEPTED_EVENT_BYTES_V0, MAX_ACCEPTED_IDENTITIES_V0, RejectionReasonV0,
    encode_admission_snapshot_v1, hash_admission_state_v0, hash_admission_state_v1,
    restore_admission_snapshot_v1,
};
use sha2::{Digest, Sha256};

const SYNTHETIC_SNAPSHOT: &str = include_str!("golden/admission-snapshot-v1-synthetic.hex");
const OTLP_FILE_SNAPSHOT: &str = include_str!("golden/admission-snapshot-v1-otlp-file.hex");
const KAFKA_SNAPSHOT: &str = include_str!("golden/admission-snapshot-v1-kafka.hex");
const HASH_GOLDEN_VECTORS: &str = include_str!("golden/admission-state-hash-v1.txt");

fn source(value: &str) -> SourceIdV0 {
    SourceIdV0::new(value).unwrap()
}

fn config_with(
    source_id: &str,
    kind: SourcePositionKindV0,
    epoch_start_ns: i64,
    epoch_end_ns: i64,
    limits: AdmissionLimitsV0,
) -> AdmissionConfigV0 {
    AdmissionConfigV0::new_with_source_position_kind(
        ADMISSION_VERSION_V0,
        source(source_id),
        kind,
        epoch_start_ns,
        epoch_end_ns,
        limits,
    )
    .unwrap()
}

fn config(source_id: &str, kind: SourcePositionKindV0) -> AdmissionConfigV0 {
    config_with(source_id, kind, 0, 100, AdmissionLimitsV0::maximum())
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
    position: SourcePositionV0,
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
        SchemaIdV0::new(schema_id).unwrap(),
        position,
        key,
        event_time_ns,
        ingest_time_ns,
        SensitivityBitmapV0::from_bits(sensitivity),
        payload,
    )
    .unwrap()
}

fn value_event(sequence: u64, ingest_time_ns: i64, value: u64) -> EventEnvelopeV0 {
    event(
        "source-v0",
        SourcePositionV0::Synthetic { sequence },
        "schema-v0",
        Some(b"k"),
        i64::try_from(sequence).unwrap() * 10,
        ingest_time_ns,
        value,
        payload([("value", ScalarValueV0::from_u64(value))]),
    )
}

fn canonical_event(source_id: &str, position: SourcePositionV0) -> EventEnvelopeV0 {
    event(
        source_id,
        position,
        "schema-v0",
        Some(b"k"),
        50,
        999,
        5,
        payload([
            ("flag", ScalarValueV0::from_bool(true)),
            ("value", ScalarValueV0::from_u64(9)),
        ]),
    )
}

fn from_hex(input: &str) -> Vec<u8> {
    let bytes = input.trim().as_bytes();
    let (pairs, remainder) = bytes.as_chunks::<2>();
    assert!(remainder.is_empty());
    pairs
        .iter()
        .map(|pair| {
            let text = std::str::from_utf8(pair.as_slice()).unwrap();
            u8::from_str_radix(text, 16).unwrap()
        })
        .collect()
}

fn to_hex(bytes: &[u8]) -> String {
    const DIGITS: &[u8; 16] = b"0123456789abcdef";
    let mut output = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        output.push(char::from(DIGITS[usize::from(byte >> 4)]));
        output.push(char::from(DIGITS[usize::from(byte & 0x0f)]));
    }
    output
}

fn golden_cases() -> [(&'static str, SourcePositionKindV0, &'static str); 3] {
    [
        (
            "synthetic",
            SourcePositionKindV0::Synthetic,
            SYNTHETIC_SNAPSHOT,
        ),
        (
            "otlp-file",
            SourcePositionKindV0::OtlpFile,
            OTLP_FILE_SNAPSHOT,
        ),
        ("kafka", SourcePositionKindV0::Kafka, KAFKA_SNAPSHOT),
    ]
}

fn restore_golden(kind: SourcePositionKindV0, golden: &str) -> AdmissionStateV0 {
    restore_admission_snapshot_v1(&from_hex(golden), &config("source-v0", kind)).unwrap()
}

fn hash_text(state: &AdmissionStateV0) -> String {
    hash_admission_state_v1(state)
        .unwrap()
        .to_canonical_string()
}

fn vector_document() -> String {
    let mut document = String::new();
    for (label, kind, golden) in golden_cases() {
        writeln!(
            document,
            "{label}={}",
            hash_text(&restore_golden(kind, golden))
        )
        .unwrap();
    }
    document
}

fn independent_preimage(snapshot: &[u8]) -> Vec<u8> {
    let literal_domain = b"decant:admission-state-hash:v1";
    let mut preimage = Vec::with_capacity(4 + literal_domain.len() + 8 + snapshot.len());
    preimage.extend_from_slice(&30_u32.to_be_bytes());
    preimage.extend_from_slice(literal_domain);
    preimage.extend_from_slice(&u64::try_from(snapshot.len()).unwrap().to_be_bytes());
    preimage.extend_from_slice(snapshot);
    preimage
}

fn read_u32(input: &[u8], offset: usize) -> u32 {
    u32::from_be_bytes(input[offset..offset + 4].try_into().unwrap())
}

fn event_coordinate_start(snapshot: &[u8]) -> usize {
    let mut cursor = 8 + 2 + 2 + 8;
    let source_length = usize::try_from(read_u32(snapshot, cursor)).unwrap();
    cursor += 4 + source_length;
    cursor += 1 + 8 + 8 + 8 + 8 + 1;
    let rejection_count = usize::try_from(read_u32(snapshot, cursor)).unwrap();
    cursor += 4 + rejection_count * (1 + 8 + 1 + 1);
    assert_eq!(read_u32(snapshot, cursor), 1);
    cursor += 4;
    let record_length = usize::try_from(read_u32(snapshot, cursor)).unwrap();
    cursor += 4;
    let body_start = cursor;
    cursor += 2;
    assert!(record_length > 3);
    assert!(snapshot[body_start + 2] <= 2);
    cursor + 1
}

fn restore_with_coordinate_mutation(
    kind: SourcePositionKindV0,
    golden: &str,
    coordinate_offset: usize,
    replacement: &[u8],
) -> AdmissionStateV0 {
    let mut snapshot = from_hex(golden);
    let start = event_coordinate_start(&snapshot) + coordinate_offset;
    snapshot[start..start + replacement.len()].copy_from_slice(replacement);
    restore_admission_snapshot_v1(&snapshot, &config("source-v0", kind)).unwrap()
}

#[test]
fn three_kind_golden_preimages_and_hash_vectors_are_exact() {
    assert_eq!(ADMISSION_STATE_HASH_ALGORITHM_V1, "sha256");
    assert_eq!(ADMISSION_STATE_HASH_VERSION_V1, 1);
    assert_eq!(ADMISSION_STATE_HASH_DOMAIN_V1.len(), 30);
    assert_eq!(
        ADMISSION_STATE_HASH_DOMAIN_V1,
        b"decant:admission-state-hash:v1"
    );
    assert_ne!(
        ADMISSION_STATE_HASH_DOMAIN_V1,
        ADMISSION_STATE_HASH_DOMAIN_V0
    );
    assert_eq!(vector_document(), HASH_GOLDEN_VECTORS);

    for (label, kind, golden) in golden_cases() {
        let snapshot = from_hex(golden);
        let state = restore_golden(kind, golden);
        assert_eq!(encode_admission_snapshot_v1(&state).unwrap(), snapshot);

        let preimage = independent_preimage(&snapshot);
        let independent_digest = Sha256::digest(&preimage);
        let actual = hash_admission_state_v1(&state).unwrap();
        assert_eq!(actual.as_bytes().as_slice(), independent_digest.as_slice());

        let expected = HASH_GOLDEN_VECTORS
            .lines()
            .find_map(|line| line.strip_prefix(&format!("{label}=")))
            .unwrap();
        assert_eq!(actual.to_canonical_string(), expected);
        assert_eq!(expected.len(), ADMISSION_STATE_HASH_TEXT_BYTES_V1);
        assert!(expected.starts_with("sha256:v1:"));
        assert!(
            expected
                .bytes()
                .skip(10)
                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
        );
    }
}

#[test]
fn repeated_hash_and_snapshot_restore_are_identical() {
    for (_, kind, golden) in golden_cases() {
        let state = restore_golden(kind, golden);
        let first = hash_admission_state_v1(&state).unwrap();
        assert_eq!(hash_admission_state_v1(&state).unwrap(), first);

        let snapshot = encode_admission_snapshot_v1(&state).unwrap();
        let restored = restore_admission_snapshot_v1(&snapshot, state.config()).unwrap();
        assert_eq!(hash_admission_state_v1(&restored).unwrap(), first);
        assert_eq!(encode_admission_snapshot_v1(&restored).unwrap(), snapshot);
    }
}

#[test]
fn empty_open_and_finalized_hashes_are_deterministic_for_every_kind() {
    let mut all_hashes = BTreeSet::new();
    for kind in [
        SourcePositionKindV0::Synthetic,
        SourcePositionKindV0::OtlpFile,
        SourcePositionKindV0::Kafka,
    ] {
        let admission_config = config("source-v0", kind);
        let mut state = AdmissionStateV0::new(admission_config.clone());
        let open = hash_admission_state_v1(&state).unwrap();
        assert_eq!(hash_admission_state_v1(&state).unwrap(), open);
        assert!(all_hashes.insert(open));

        assert_eq!(
            state.finalize_source(&source("source-v0"), 100),
            FinalizeOutcomeV0::Finalized
        );
        let finalized = hash_admission_state_v1(&state).unwrap();
        assert_ne!(finalized, open);
        assert!(all_hashes.insert(finalized));

        let snapshot = encode_admission_snapshot_v1(&state).unwrap();
        let restored = restore_admission_snapshot_v1(&snapshot, &admission_config).unwrap();
        assert_eq!(hash_admission_state_v1(&restored).unwrap(), finalized);
    }
    assert_eq!(all_hashes.len(), 6);
}

#[test]
fn accepted_permutations_and_redelivery_converge() {
    let candidates = [
        value_event(1, 1, 1),
        value_event(2, 2, 2),
        value_event(3, 3, 3),
        value_event(4, 4, 4),
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
                    let mut state =
                        AdmissionStateV0::new(config("source-v0", SourcePositionKindV0::Synthetic));
                    for index in order {
                        assert_eq!(
                            state.apply_event(&candidates[index]),
                            Ok(ApplyOutcomeV0::Accepted)
                        );
                    }
                    hashes.insert(hash_admission_state_v1(&state).unwrap());
                    permutations += 1;
                }
            }
        }
    }
    assert_eq!(permutations, 24);
    assert_eq!(hashes.len(), 1);

    let original = value_event(1, 10, 1);
    let mut state = AdmissionStateV0::new(config("source-v0", SourcePositionKindV0::Synthetic));
    assert_eq!(state.apply_event(&original), Ok(ApplyOutcomeV0::Accepted));
    let accepted_hash = hash_admission_state_v1(&state).unwrap();
    assert_eq!(
        state.apply_event(&original),
        Ok(ApplyOutcomeV0::DuplicateNoOp)
    );
    assert_eq!(hash_admission_state_v1(&state).unwrap(), accepted_hash);

    let ingest_only = value_event(1, 99, 1);
    assert_eq!(
        state.apply_event(&ingest_only),
        Ok(ApplyOutcomeV0::DuplicateNoOp)
    );
    assert_eq!(hash_admission_state_v1(&state).unwrap(), accepted_hash);
}

fn state_with(candidate: EventEnvelopeV0) -> AdmissionStateV0 {
    let mut state = AdmissionStateV0::new(config_with(
        "source-v0",
        SourcePositionKindV0::Synthetic,
        -100,
        100,
        AdmissionLimitsV0::maximum(),
    ));
    assert_eq!(state.apply_event(&candidate), Ok(ApplyOutcomeV0::Accepted));
    state
}

#[test]
fn exercised_configuration_positions_events_and_rejection_are_distinct() {
    let mut hashes = BTreeSet::new();
    let mut insert = |label: &str, state: AdmissionStateV0| {
        assert!(
            hashes.insert(hash_admission_state_v1(&state).unwrap()),
            "duplicate exercised hash for {label}"
        );
    };

    insert(
        "empty",
        AdmissionStateV0::new(config_with(
            "source-v0",
            SourcePositionKindV0::Synthetic,
            -100,
            100,
            AdmissionLimitsV0::maximum(),
        )),
    );
    insert(
        "source",
        AdmissionStateV0::new(config_with(
            "source-v1",
            SourcePositionKindV0::Synthetic,
            -100,
            100,
            AdmissionLimitsV0::maximum(),
        )),
    );
    insert(
        "kind-otlp",
        AdmissionStateV0::new(config_with(
            "source-v0",
            SourcePositionKindV0::OtlpFile,
            -100,
            100,
            AdmissionLimitsV0::maximum(),
        )),
    );
    insert(
        "kind-kafka",
        AdmissionStateV0::new(config_with(
            "source-v0",
            SourcePositionKindV0::Kafka,
            -100,
            100,
            AdmissionLimitsV0::maximum(),
        )),
    );
    insert(
        "epoch",
        AdmissionStateV0::new(config_with(
            "source-v0",
            SourcePositionKindV0::Synthetic,
            -99,
            100,
            AdmissionLimitsV0::maximum(),
        )),
    );
    insert(
        "identity-limit",
        AdmissionStateV0::new(config_with(
            "source-v0",
            SourcePositionKindV0::Synthetic,
            -100,
            100,
            AdmissionLimitsV0::new(1, MAX_ACCEPTED_EVENT_BYTES_V0).unwrap(),
        )),
    );
    insert(
        "byte-limit",
        AdmissionStateV0::new(config_with(
            "source-v0",
            SourcePositionKindV0::Synthetic,
            -100,
            100,
            AdmissionLimitsV0::new(MAX_ACCEPTED_IDENTITIES_V0, 1).unwrap(),
        )),
    );

    for (label, state) in [
        (
            "synthetic-sequence",
            restore_with_coordinate_mutation(
                SourcePositionKindV0::Synthetic,
                SYNTHETIC_SNAPSHOT,
                0,
                &8_u64.to_be_bytes(),
            ),
        ),
        (
            "otlp-line",
            restore_with_coordinate_mutation(
                SourcePositionKindV0::OtlpFile,
                OTLP_FILE_SNAPSHOT,
                0,
                &8_u64.to_be_bytes(),
            ),
        ),
        (
            "otlp-resource",
            restore_with_coordinate_mutation(
                SourcePositionKindV0::OtlpFile,
                OTLP_FILE_SNAPSHOT,
                8,
                &2_u32.to_be_bytes(),
            ),
        ),
        (
            "otlp-scope",
            restore_with_coordinate_mutation(
                SourcePositionKindV0::OtlpFile,
                OTLP_FILE_SNAPSHOT,
                12,
                &3_u32.to_be_bytes(),
            ),
        ),
        (
            "otlp-span",
            restore_with_coordinate_mutation(
                SourcePositionKindV0::OtlpFile,
                OTLP_FILE_SNAPSHOT,
                16,
                &4_u32.to_be_bytes(),
            ),
        ),
        (
            "kafka-partition",
            restore_with_coordinate_mutation(
                SourcePositionKindV0::Kafka,
                KAFKA_SNAPSHOT,
                0,
                &4_i32.to_be_bytes(),
            ),
        ),
        (
            "kafka-offset",
            restore_with_coordinate_mutation(
                SourcePositionKindV0::Kafka,
                KAFKA_SNAPSHOT,
                4,
                &8_i64.to_be_bytes(),
            ),
        ),
    ] {
        insert(label, state);
    }

    let variants = [
        (
            "event-baseline",
            event(
                "source-v0",
                SourcePositionV0::Synthetic { sequence: 1 },
                "schema-v0",
                None,
                0,
                0,
                0,
                payload([("value", ScalarValueV0::from_u64(1))]),
            ),
        ),
        (
            "event-schema",
            event(
                "source-v0",
                SourcePositionV0::Synthetic { sequence: 1 },
                "schema-v1",
                None,
                0,
                0,
                0,
                payload([("value", ScalarValueV0::from_u64(1))]),
            ),
        ),
        (
            "event-key",
            event(
                "source-v0",
                SourcePositionV0::Synthetic { sequence: 1 },
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
                SourcePositionV0::Synthetic { sequence: 1 },
                "schema-v0",
                None,
                1,
                0,
                0,
                payload([("value", ScalarValueV0::from_u64(1))]),
            ),
        ),
        (
            "event-sensitivity",
            event(
                "source-v0",
                SourcePositionV0::Synthetic { sequence: 1 },
                "schema-v0",
                None,
                0,
                0,
                1,
                payload([("value", ScalarValueV0::from_u64(1))]),
            ),
        ),
        (
            "event-field",
            event(
                "source-v0",
                SourcePositionV0::Synthetic { sequence: 1 },
                "schema-v0",
                None,
                0,
                0,
                0,
                payload([("other", ScalarValueV0::from_u64(1))]),
            ),
        ),
        (
            "event-scalar",
            event(
                "source-v0",
                SourcePositionV0::Synthetic { sequence: 1 },
                "schema-v0",
                None,
                0,
                0,
                0,
                payload([("value", ScalarValueV0::from_bool(true))]),
            ),
        ),
    ];
    for (label, candidate) in variants {
        insert(label, state_with(candidate));
    }

    let mut rejected = AdmissionStateV0::new(config_with(
        "source-v0",
        SourcePositionKindV0::Synthetic,
        -100,
        100,
        AdmissionLimitsV0::maximum(),
    ));
    assert_eq!(
        rejected.apply_event(&event(
            "other-v0",
            SourcePositionV0::Synthetic { sequence: 1 },
            "schema-v0",
            None,
            0,
            0,
            0,
            payload([("value", ScalarValueV0::from_u64(1))]),
        )),
        Ok(ApplyOutcomeV0::Rejected(
            RejectionReasonV0::UnexpectedSource
        ))
    );
    insert("rejection", rejected);
    assert_eq!(hashes.len(), 22);
}

#[test]
fn hash_v0_is_exact_and_external_application_stays_disabled() {
    assert_eq!(ADMISSION_STATE_HASH_ALGORITHM_V0, "sha256");
    assert_eq!(ADMISSION_STATE_HASH_VERSION_V0, 0);
    assert_eq!(
        ADMISSION_STATE_HASH_DOMAIN_V0,
        b"decant:admission-state-hash:v0"
    );
    let synthetic = AdmissionStateV0::new(config("source-v0", SourcePositionKindV0::Synthetic));
    assert_eq!(
        hash_admission_state_v0(&synthetic)
            .unwrap()
            .to_canonical_string(),
        "sha256:v0:b35def9b78655450ac653ceb0bf03126a2d6b8dc119b611ebb7f1e9c6aaa315f"
    );
    assert_ne!(
        hash_admission_state_v0(&synthetic).unwrap().as_bytes(),
        hash_admission_state_v1(&synthetic).unwrap().as_bytes()
    );

    let mut kafka = restore_golden(SourcePositionKindV0::Kafka, KAFKA_SNAPSHOT);
    assert_eq!(
        hash_admission_state_v0(&kafka),
        Err(AdmissionStateHashErrorV0::SnapshotEncoding(
            AdmissionSnapshotEncodeErrorV0::StateInvariant {
                field: AdmissionSnapshotFieldV0::SourcePosition,
            }
        ))
    );
    let before = hash_admission_state_v1(&kafka).unwrap();
    assert_eq!(
        kafka.apply_event(&canonical_event(
            "source-v0",
            SourcePositionV0::kafka(3, 8).unwrap(),
        )),
        Err(AdmissionErrorV0::UnsupportedSourcePosition {
            kind: SourcePositionKindV0::Kafka,
        })
    );
    assert_eq!(hash_admission_state_v1(&kafka).unwrap(), before);
}

#[test]
fn debug_and_errors_are_content_independent() {
    let state = restore_golden(SourcePositionKindV0::Synthetic, SYNTHETIC_SNAPSHOT);
    let hash = hash_admission_state_v1(&state).unwrap();
    let digest_hex = to_hex(hash.as_bytes());
    let debug = format!("{hash:?}");
    assert!(debug.contains("<redacted>"));
    assert!(!debug.contains(&digest_hex));

    let error = AdmissionStateHashErrorV1::SnapshotEncoding(
        AdmissionSnapshotEncodeErrorV1::StateInvariant {
            field: AdmissionSnapshotFieldV1::AcceptedEventBytes,
        },
    );
    for rendered in [format!("{error:?}"), error.to_string(), debug] {
        for probe in [
            "source-v0",
            "schema-v0",
            "flag",
            "value",
            "decant://event/v0/source-v0/synthetic/7",
            &digest_hex,
        ] {
            assert!(!rendered.contains(probe));
        }
    }
}
