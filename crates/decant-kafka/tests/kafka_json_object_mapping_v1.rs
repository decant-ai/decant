use std::any::Any;

use decant_ir::SourceIdV0;
use decant_kafka::{
    KAFKA_JSON_OBJECT_MAPPING_VERSION_V0, KAFKA_JSON_OBJECT_MAPPING_VERSION_V1,
    KafkaJsonEntityKeySourceV1, KafkaJsonEventTimeUnitV0, KafkaJsonObjectMappingErrorV1,
    KafkaJsonObjectMappingV0, KafkaJsonObjectMappingV1, KafkaJsonObjectProjectionV0,
    KafkaJsonScalarKindV0,
};

fn base(projections: Vec<KafkaJsonObjectProjectionV0>) -> KafkaJsonObjectMappingV0 {
    KafkaJsonObjectMappingV0::new(
        KAFKA_JSON_OBJECT_MAPPING_VERSION_V0,
        SourceIdV0::new("private-namespace").unwrap(),
        "time",
        KafkaJsonEventTimeUnitV0::Milliseconds,
        projections,
    )
    .unwrap()
}

fn projection(
    source: &str,
    target: &str,
    kind: KafkaJsonScalarKindV0,
) -> KafkaJsonObjectProjectionV0 {
    KafkaJsonObjectProjectionV0::new(source, target, kind).unwrap()
}

#[test]
fn canonical_identity_vectors_are_exact_and_projection_order_independent() {
    let left = base(vec![
        projection("raw_value", "value", KafkaJsonScalarKindV0::U64),
        projection("raw_service", "service", KafkaJsonScalarKindV0::Utf8),
    ]);
    let right = base(vec![
        projection("raw_service", "service", KafkaJsonScalarKindV0::Utf8),
        projection("raw_value", "value", KafkaJsonScalarKindV0::U64),
    ]);
    let left = KafkaJsonObjectMappingV1::new(
        1,
        left,
        KafkaJsonEntityKeySourceV1::utf8_projection("service").unwrap(),
    )
    .unwrap();
    let right = KafkaJsonObjectMappingV1::new(
        1,
        right,
        KafkaJsonEntityKeySourceV1::utf8_projection("service").unwrap(),
    )
    .unwrap();

    assert_eq!(left, right);
    assert_eq!(left.version(), KAFKA_JSON_OBJECT_MAPPING_VERSION_V1);
    assert_eq!(
        hex(left.canonical_preimage()),
        "646563616e742d6b61666b612d6a736f6e2d6f626a6563742d6d617070696e672d76310001f758bdab35a70d4f646a28ca6ef7530cd5a3a7340b9b8862db34f6d3e7d4632301000773657276696365"
    );
    assert_eq!(
        hex(left.mapping_digest()),
        "8f7e0bea013839357a50960d128eb2e964e75b0b5bd0d72267f27832d956f119"
    );
    assert_eq!(
        left.schema_id().as_str(),
        "8f7e0bea013839357a50960d128eb2e964e75b0b5bd0d72267f27832d956f119"
    );
    assert_eq!(
        left.effective_source_id().as_str(),
        "0ef10e717adc3afa0513476b828595262842379b06974b52f93b92a1a97d54ac"
    );
}

#[test]
fn entity_key_semantics_change_mapping_schema_and_source_identities() {
    let kafka = KafkaJsonObjectMappingV1::new(
        1,
        base(vec![
            projection("raw_service", "service", KafkaJsonScalarKindV0::Utf8),
            projection("raw_region", "region", KafkaJsonScalarKindV0::Utf8),
        ]),
        KafkaJsonEntityKeySourceV1::kafka_record_key(),
    )
    .unwrap();
    let service = KafkaJsonObjectMappingV1::new(
        1,
        base(vec![
            projection("raw_service", "service", KafkaJsonScalarKindV0::Utf8),
            projection("raw_region", "region", KafkaJsonScalarKindV0::Utf8),
        ]),
        KafkaJsonEntityKeySourceV1::utf8_projection("service").unwrap(),
    )
    .unwrap();
    let region = KafkaJsonObjectMappingV1::new(
        1,
        base(vec![
            projection("raw_service", "service", KafkaJsonScalarKindV0::Utf8),
            projection("raw_region", "region", KafkaJsonScalarKindV0::Utf8),
        ]),
        KafkaJsonEntityKeySourceV1::utf8_projection("region").unwrap(),
    )
    .unwrap();

    for changed in [&service, &region] {
        assert_ne!(kafka.mapping_digest(), changed.mapping_digest());
        assert_ne!(kafka.schema_id(), changed.schema_id());
        assert_ne!(kafka.effective_source_id(), changed.effective_source_id());
    }
    assert_ne!(service.mapping_digest(), region.mapping_digest());
}

#[test]
fn versions_and_projection_contract_fail_closed_without_value_disclosure() {
    assert_eq!(
        KafkaJsonObjectMappingV1::new(
            0,
            base(vec![]),
            KafkaJsonEntityKeySourceV1::kafka_record_key(),
        ),
        Err(KafkaJsonObjectMappingErrorV1::UnsupportedVersion { actual: 0 })
    );
    let unknown = "private-unknown-field";
    let result = KafkaJsonObjectMappingV1::new(
        1,
        base(vec![projection(
            "raw_service",
            "service",
            KafkaJsonScalarKindV0::Utf8,
        )]),
        KafkaJsonEntityKeySourceV1::utf8_projection(unknown).unwrap(),
    );
    assert_eq!(
        result,
        Err(KafkaJsonObjectMappingErrorV1::UnknownProjectionTarget)
    );
    assert!(!format!("{result:?}").contains(unknown));

    let non_utf8 = KafkaJsonObjectMappingV1::new(
        1,
        base(vec![projection(
            "raw_value",
            "value",
            KafkaJsonScalarKindV0::U64,
        )]),
        KafkaJsonEntityKeySourceV1::utf8_projection("value").unwrap(),
    );
    assert_eq!(
        non_utf8,
        Err(KafkaJsonObjectMappingErrorV1::ProjectionTargetNotUtf8)
    );

    let panic = std::panic::catch_unwind(|| {
        assert_eq!(
            KafkaJsonEntityKeySourceV1::utf8_projection("private\nfield"),
            Ok(KafkaJsonEntityKeySourceV1::kafka_record_key())
        );
    })
    .unwrap_err();
    assert!(!panic_text(panic).contains("private"));
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
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
