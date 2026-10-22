use std::any::Any;

use decant_ir::{MAX_FIELD_NAME_BYTES, SourceIdV0, ValidationErrorV0};
use decant_kafka::{
    KAFKA_JSON_OBJECT_MAPPING_VERSION_V0, KafkaJsonEventTimeUnitV0, KafkaJsonObjectMappingErrorV0,
    KafkaJsonObjectMappingNameV0, KafkaJsonObjectMappingV0, KafkaJsonObjectProjectionV0,
    KafkaJsonScalarKindV0, MAX_KAFKA_JSON_OBJECT_MAPPING_PREIMAGE_BYTES_V0,
    MAX_KAFKA_JSON_OBJECT_PROJECTIONS_V0, MAX_KAFKA_JSON_OBJECT_SOURCE_FIELD_BYTES_V0,
};

fn projection(
    source: &str,
    target: &str,
    kind: KafkaJsonScalarKindV0,
) -> KafkaJsonObjectProjectionV0 {
    KafkaJsonObjectProjectionV0::new(source, target, kind).unwrap()
}

fn mapping(
    namespace: &str,
    unit: KafkaJsonEventTimeUnitV0,
    projections: Vec<KafkaJsonObjectProjectionV0>,
) -> KafkaJsonObjectMappingV0 {
    KafkaJsonObjectMappingV0::new(
        KAFKA_JSON_OBJECT_MAPPING_VERSION_V0,
        SourceIdV0::new(namespace).unwrap(),
        "observed_time",
        unit,
        projections,
    )
    .unwrap()
}

#[test]
fn canonical_order_and_identity_vectors_are_exact() {
    let first = mapping(
        "private-source-namespace",
        KafkaJsonEventTimeUnitV0::Milliseconds,
        vec![
            projection("raw_service", "service", KafkaJsonScalarKindV0::Utf8),
            projection("raw_ok", "ok", KafkaJsonScalarKindV0::Bool),
        ],
    );
    let reordered = mapping(
        "private-source-namespace",
        KafkaJsonEventTimeUnitV0::Milliseconds,
        vec![
            projection("raw_ok", "ok", KafkaJsonScalarKindV0::Bool),
            projection("raw_service", "service", KafkaJsonScalarKindV0::Utf8),
        ],
    );

    assert_eq!(first, reordered);
    assert_eq!(first.projections()[0].target_field(), "ok");
    assert_eq!(first.projections()[1].target_field(), "service");
    assert_eq!(first.canonical_preimage(), reordered.canonical_preimage());
    assert_eq!(first.mapping_digest(), reordered.mapping_digest());
    assert_eq!(first.schema_id(), reordered.schema_id());
    assert_eq!(first.effective_source_id(), reordered.effective_source_id());

    assert_eq!(
        hex(first.canonical_preimage()),
        "646563616e742d6b61666b612d6a736f6e2d6f626a6563742d6d617070696e672d76300000000d6f627365727665645f74696d6502000200067261775f6f6b00026f6b01000b7261775f7365727669636500077365727669636504"
    );
    assert_eq!(
        hex(first.mapping_digest()),
        "2efb7cb9f6816c71fb5f3371ef8903d70a41afe80f80929fc37a64d59896dac6"
    );
    assert_eq!(
        first.schema_id().as_str(),
        "2efb7cb9f6816c71fb5f3371ef8903d70a41afe80f80929fc37a64d59896dac6"
    );
    assert_eq!(
        first.effective_source_id().as_str(),
        "8a272b4ed412e12d1d7e4cd4810bf8dc59d08e82962db7fa8c897cbbb1211991"
    );
}

#[test]
fn semantic_changes_change_receipts_and_namespace_changes_only_source() {
    let base = mapping(
        "namespace-a",
        KafkaJsonEventTimeUnitV0::Nanoseconds,
        vec![projection("value", "value", KafkaJsonScalarKindV0::I64)],
    );
    let other_namespace = mapping(
        "namespace-b",
        KafkaJsonEventTimeUnitV0::Nanoseconds,
        vec![projection("value", "value", KafkaJsonScalarKindV0::I64)],
    );
    assert_eq!(base.mapping_digest(), other_namespace.mapping_digest());
    assert_eq!(base.schema_id(), other_namespace.schema_id());
    assert_ne!(
        base.effective_source_id(),
        other_namespace.effective_source_id()
    );

    for changed in [
        mapping(
            "namespace-a",
            KafkaJsonEventTimeUnitV0::Microseconds,
            vec![projection("value", "value", KafkaJsonScalarKindV0::I64)],
        ),
        KafkaJsonObjectMappingV0::new(
            0,
            SourceIdV0::new("namespace-a").unwrap(),
            "other_time",
            KafkaJsonEventTimeUnitV0::Nanoseconds,
            vec![projection("value", "value", KafkaJsonScalarKindV0::I64)],
        )
        .unwrap(),
        mapping(
            "namespace-a",
            KafkaJsonEventTimeUnitV0::Nanoseconds,
            vec![projection("value", "value", KafkaJsonScalarKindV0::U64)],
        ),
    ] {
        assert_ne!(base.mapping_digest(), changed.mapping_digest());
        assert_ne!(base.schema_id(), changed.schema_id());
        assert_ne!(base.effective_source_id(), changed.effective_source_id());
    }
}

#[test]
fn versions_names_duplicates_and_counts_are_bounded() {
    assert_eq!(
        KafkaJsonObjectMappingV0::new(
            1,
            SourceIdV0::new("namespace").unwrap(),
            "time",
            KafkaJsonEventTimeUnitV0::Seconds,
            Vec::new(),
        ),
        Err(KafkaJsonObjectMappingErrorV0::UnsupportedVersion { actual: 1 })
    );
    for invalid in [
        "",
        &"x".repeat(MAX_KAFKA_JSON_OBJECT_SOURCE_FIELD_BYTES_V0 + 1),
        "bad\nname",
    ] {
        let result = KafkaJsonObjectMappingV0::new(
            0,
            SourceIdV0::new("namespace").unwrap(),
            invalid,
            KafkaJsonEventTimeUnitV0::Seconds,
            Vec::new(),
        );
        assert!(result.is_err());
        if !invalid.is_empty() {
            assert!(!format!("{result:?}").contains(invalid));
        }
    }
    assert_eq!(
        KafkaJsonObjectProjectionV0::new("", "valid", KafkaJsonScalarKindV0::Null),
        Err(KafkaJsonObjectMappingErrorV0::InvalidSourceNameLength {
            name: KafkaJsonObjectMappingNameV0::ProjectionSource,
            minimum: 1,
            maximum: MAX_KAFKA_JSON_OBJECT_SOURCE_FIELD_BYTES_V0,
            actual: 0,
        })
    );
    assert_eq!(
        KafkaJsonObjectProjectionV0::new("source", "Bad", KafkaJsonScalarKindV0::Null),
        Err(KafkaJsonObjectMappingErrorV0::InvalidTargetName {
            category: ValidationErrorV0::InvalidFieldNameSyntax,
        })
    );
    assert_eq!(
        KafkaJsonObjectProjectionV0::new(
            "source",
            &"x".repeat(MAX_FIELD_NAME_BYTES + 1),
            KafkaJsonScalarKindV0::Null,
        ),
        Err(KafkaJsonObjectMappingErrorV0::InvalidTargetName {
            category: ValidationErrorV0::InvalidFieldNameLength {
                minimum: 1,
                maximum: MAX_FIELD_NAME_BYTES,
                actual: MAX_FIELD_NAME_BYTES + 1,
            },
        })
    );

    let duplicate_source = KafkaJsonObjectMappingV0::new(
        0,
        SourceIdV0::new("namespace").unwrap(),
        "time",
        KafkaJsonEventTimeUnitV0::Seconds,
        vec![
            projection("same", "first", KafkaJsonScalarKindV0::Bool),
            projection("same", "second", KafkaJsonScalarKindV0::Bool),
        ],
    );
    assert_eq!(
        duplicate_source,
        Err(KafkaJsonObjectMappingErrorV0::DuplicateProjectionSource)
    );
    let duplicate_target = KafkaJsonObjectMappingV0::new(
        0,
        SourceIdV0::new("namespace").unwrap(),
        "time",
        KafkaJsonEventTimeUnitV0::Seconds,
        vec![
            projection("first", "same", KafkaJsonScalarKindV0::Bool),
            projection("second", "same", KafkaJsonScalarKindV0::Bool),
        ],
    );
    assert_eq!(
        duplicate_target,
        Err(KafkaJsonObjectMappingErrorV0::DuplicateProjectionTarget)
    );

    let maximum = (0..MAX_KAFKA_JSON_OBJECT_PROJECTIONS_V0)
        .map(|index| {
            projection(
                &format!("source_{index}"),
                &format!("target_{index}"),
                KafkaJsonScalarKindV0::Null,
            )
        })
        .collect();
    let accepted = mapping("namespace", KafkaJsonEventTimeUnitV0::Seconds, maximum);
    assert_eq!(
        accepted.projections().len(),
        MAX_KAFKA_JSON_OBJECT_PROJECTIONS_V0
    );
    assert!(accepted.canonical_preimage().len() <= MAX_KAFKA_JSON_OBJECT_MAPPING_PREIMAGE_BYTES_V0);

    let too_many = (0..=MAX_KAFKA_JSON_OBJECT_PROJECTIONS_V0)
        .map(|index| {
            projection(
                &format!("source_{index}"),
                &format!("target_{index}"),
                KafkaJsonScalarKindV0::Null,
            )
        })
        .collect();
    assert_eq!(
        KafkaJsonObjectMappingV0::new(
            0,
            SourceIdV0::new("namespace").unwrap(),
            "time",
            KafkaJsonEventTimeUnitV0::Seconds,
            too_many,
        ),
        Err(KafkaJsonObjectMappingErrorV0::TooManyProjections {
            maximum: MAX_KAFKA_JSON_OBJECT_PROJECTIONS_V0,
            actual: MAX_KAFKA_JSON_OBJECT_PROJECTIONS_V0 + 1,
        })
    );
}

#[test]
fn all_units_and_kinds_have_distinct_stable_encodings() {
    let units = [
        KafkaJsonEventTimeUnitV0::Nanoseconds,
        KafkaJsonEventTimeUnitV0::Microseconds,
        KafkaJsonEventTimeUnitV0::Milliseconds,
        KafkaJsonEventTimeUnitV0::Seconds,
    ];
    assert_eq!(
        units.map(KafkaJsonEventTimeUnitV0::multiplier_ns),
        [1, 1_000, 1_000_000, 1_000_000_000]
    );
    let unit_hashes = units.map(|unit| {
        mapping("namespace", unit, Vec::new())
            .mapping_digest()
            .to_owned()
    });
    for left in 0..unit_hashes.len() {
        for right in (left + 1)..unit_hashes.len() {
            assert_ne!(unit_hashes[left], unit_hashes[right]);
        }
    }

    let kinds = [
        KafkaJsonScalarKindV0::Null,
        KafkaJsonScalarKindV0::Bool,
        KafkaJsonScalarKindV0::I64,
        KafkaJsonScalarKindV0::U64,
        KafkaJsonScalarKindV0::Utf8,
    ];
    let kind_hashes = kinds.map(|kind| {
        mapping(
            "namespace",
            KafkaJsonEventTimeUnitV0::Seconds,
            vec![projection("value", "value", kind)],
        )
        .mapping_digest()
        .to_owned()
    });
    for left in 0..kind_hashes.len() {
        for right in (left + 1)..kind_hashes.len() {
            assert_ne!(kind_hashes[left], kind_hashes[right]);
        }
    }
}

#[test]
fn diagnostics_and_assertion_panics_redact_configuration_names() {
    let contract = mapping(
        "private-source-namespace",
        KafkaJsonEventTimeUnitV0::Seconds,
        vec![projection(
            "private-input-field",
            "private_target",
            KafkaJsonScalarKindV0::Utf8,
        )],
    );
    let diagnostic = format!("{contract:?}");
    assert_safe(&diagnostic);

    let different = mapping(
        "other-namespace",
        KafkaJsonEventTimeUnitV0::Seconds,
        Vec::new(),
    );
    let panic = std::panic::catch_unwind(|| assert_eq!(contract, different)).unwrap_err();
    assert_safe(&panic_text(panic));
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}

fn assert_safe(text: &str) {
    for secret in [
        "private-source-namespace",
        "private-input-field",
        "private_target",
        "other-namespace",
    ] {
        assert!(!text.contains(secret), "leaked {secret}");
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
