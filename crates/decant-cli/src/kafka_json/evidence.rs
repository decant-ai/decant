//! Additive, recipe-restricted Kafka configuration. Reuse the V0 source/security
//! builders without widening their public interfaces or accepting V0 egress.
use super::*;
use decant_compiler::evidence::{EvidenceRequestV1, bounded_json, digest};

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Document {
    format: String,
    version: u16,
    bootstrap_servers: String,
    topic: String,
    source_namespace: String,
    expected_cluster_id: String,
    expected_topic_id: String,
    partitions: Vec<KafkaLivePartitionDocumentV0>,
    checkpoint_directory: String,
    epoch_start_ns: i64,
    epoch_end_ns: i64,
    accepted_identity_limit: usize,
    accepted_event_byte_limit: usize,
    poll_timeout_ms: u64,
    lifecycle: KafkaMcpLifecycleDocumentV0,
    security: KafkaMcpSecurityDocumentV0,
    recipe: EvidenceRequestV1,
    mapping: KafkaJsonObjectMappingDocumentV1,
}

pub(crate) struct KafkaEvidenceConfigV1 {
    pub source: KafkaStaticSourceConfigV0,
    pub admission: AdmissionConfigV0,
    pub checkpoint_directory: PathBuf,
    pub poll_timeout: Duration,
    pub mapping: KafkaJsonObjectMappingV1,
    pub recipe: EvidenceRequestV1,
    pub lifecycle: KafkaResidentLifecycleConfigV0,
    pub security: KafkaStaticSecurityConfigV0,
}

pub(crate) fn parse(bytes: &[u8]) -> Result<KafkaEvidenceConfigV1, KafkaJsonCliErrorV0> {
    let invalid = |_| KafkaJsonCliErrorV0::ConfigContract;
    if bytes.len() > MAX_KAFKA_LIVE_CONFIG_BYTES_V0 {
        return Err(KafkaJsonCliErrorV0::ConfigTooLarge);
    }
    let doc: Document =
        serde_json::from_slice(bytes).map_err(|_| KafkaJsonCliErrorV0::ConfigSyntax)?;
    if doc.format != "decant-kafka-evidence-config"
        || doc.version != 1
        || doc.mapping.format != KAFKA_JSON_OBJECT_MAPPING_FORMAT_V0
    {
        return Err(KafkaJsonCliErrorV0::ConfigContract);
    }
    doc.recipe.validate().map_err(invalid)?;
    SourceIdV0::new(&doc.source_namespace).map_err(|_| KafkaJsonCliErrorV0::ConfigContract)?;
    // Bind the complete recipe/policy/budget into expected checkpoint identity.
    // A changed recipe requires an explicit separate checkpoint directory.
    let binding = bounded_json(
        &(&doc.source_namespace, &doc.recipe),
        MAX_KAFKA_LIVE_CONFIG_BYTES_V0,
    )
    .map_err(invalid)?;
    let namespace = digest(b"decant:kafka-evidence-source:v1\0", &binding);
    let mapping = build_json_object_mapping_v1(&namespace, doc.mapping)?;
    validate_mapping(&mapping)?;
    let retention = EventTimeRetentionConfigV0::new(0, doc.recipe.retention_ns)
        .map_err(|_| KafkaJsonCliErrorV0::ConfigContract)?;
    let lifecycle = KafkaResidentLifecycleConfigV0::new(
        doc.lifecycle.version,
        doc.lifecycle.records_between_checkpoints,
        doc.lifecycle.idle_observations_between_checkpoints,
        doc.lifecycle.retained_checkpoint_generations,
        retention,
    )
    .map_err(|_| KafkaJsonCliErrorV0::ConfigContract)?;
    let security = build_kafka_mcp_security_v0(doc.security)?;
    let (source, admission, checkpoint_directory, poll_timeout) = build_kafka_source_config_v0(
        doc.bootstrap_servers,
        doc.topic,
        doc.expected_cluster_id,
        doc.expected_topic_id,
        doc.partitions,
        doc.checkpoint_directory,
        doc.epoch_start_ns,
        doc.epoch_end_ns,
        doc.accepted_identity_limit,
        doc.accepted_event_byte_limit,
        doc.poll_timeout_ms,
        mapping.effective_source_id().clone(),
    )?;
    Ok(KafkaEvidenceConfigV1 {
        source,
        admission,
        checkpoint_directory,
        poll_timeout,
        mapping,
        recipe: doc.recipe,
        lifecycle,
        security,
    })
}

fn validate_mapping(mapping: &KafkaJsonObjectMappingV1) -> Result<(), KafkaJsonCliErrorV0> {
    use KafkaJsonScalarKindV0::{Null, U64, Utf8};
    let fields = mapping.base().projections();
    for field in fields {
        let valid = match field.target_field() {
            "trace.id" | "span.id" => field.kind() == Utf8,
            "span.kind" | "status.code" | "duration.ns" => field.kind() == U64,
            "service.name" | "service.version" => matches!(field.kind(), Utf8 | Null),
            _ => false,
        };
        if !valid {
            return Err(KafkaJsonCliErrorV0::ConfigContract);
        }
    }
    for required in [
        "trace.id",
        "span.id",
        "span.kind",
        "status.code",
        "duration.ns",
    ] {
        if !fields.iter().any(|field| field.target_field() == required) {
            return Err(KafkaJsonCliErrorV0::ConfigContract);
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::{Value, json};
    fn example() -> Value {
        serde_json::from_str(include_str!(
            "../../../../examples/evidence/kafka-config-v1.json"
        ))
        .unwrap()
    }
    #[test]
    fn recipe_mapping_and_security_are_strict_and_identity_bound() {
        let document = example();
        let original = parse(&serde_json::to_vec(&document).unwrap()).unwrap();
        for field in ["output_bytes", "retention_ns", "window_ns"] {
            let mut changed = document.clone();
            changed["recipe"][field] = json!(changed["recipe"][field].as_u64().unwrap() - 1);
            let parsed = parse(&serde_json::to_vec(&changed).unwrap()).unwrap();
            assert_ne!(
                original.mapping.effective_source_id(),
                parsed.mapping.effective_source_id()
            );
        }
        let mut changed = document.clone();
        changed["recipe"]["policy"]["allow_service_versions"] = json!(true);
        assert_ne!(
            original.mapping.effective_source_id(),
            parse(&serde_json::to_vec(&changed).unwrap())
                .unwrap()
                .mapping
                .effective_source_id()
        );
        for (pointer, value) in [
            ("/version", json!(0)),
            ("/mapping/format", json!("unexpected")),
            ("/mapping/payload/0/type", json!("u64")),
            ("/mapping/payload/4/target", json!("raw.body")),
            ("/recipe/policy/allow_service_names", json!(false)),
        ] {
            let mut changed = document.clone();
            *changed.pointer_mut(pointer).unwrap() = value;
            assert!(parse(&serde_json::to_vec(&changed).unwrap()).is_err());
        }
        let mut changed = document.clone();
        changed["security"]["password"] = json!("synthetic-secret");
        assert!(parse(&serde_json::to_vec(&changed).unwrap()).is_err());
        changed = document.clone();
        changed["unexpected"] = json!(true);
        assert!(parse(&serde_json::to_vec(&changed).unwrap()).is_err());
        changed = document.clone();
        changed["mapping"]["payload"]
            .as_array_mut()
            .unwrap()
            .remove(0);
        assert!(parse(&serde_json::to_vec(&changed).unwrap()).is_err());
        changed = document;
        changed["mapping"]["payload"].as_array_mut().unwrap().pop();
        assert!(parse(&serde_json::to_vec(&changed).unwrap()).is_ok()); // absent version is explicit
    }
}
