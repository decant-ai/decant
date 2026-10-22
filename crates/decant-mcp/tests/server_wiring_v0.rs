use std::collections::BTreeMap;

use decant_compiler::{
    KAFKA_FIELD_EGRESS_POLICY_VERSION_V0, KAFKA_MATERIAL_CHANGE_CONTEXT_VERSION_V1,
    KafkaEntityKeyEgressActionV0, KafkaFieldEgressActionV0, KafkaFieldEgressPolicyV0,
    KafkaFieldEgressRuleV0, KafkaMaterialChangeContextV0, KafkaMaterialChangeRequestV0,
    MAX_KAFKA_MATERIAL_CHANGE_OUTPUT_BYTES_V0, compile_kafka_material_change_context_v1,
};
use decant_ir::{
    CanonicalPayloadV0, EventEnvelopeV0, ScalarValueV0, SchemaIdV0, SensitivityBitmapV0,
    SourceIdV0, SourcePositionKindV0, SourcePositionV0,
};
use decant_mcp::{
    DECANT_CONTEXT_SNAPSHOT_TOOL_V0, DECANT_EXPLAIN_PROVENANCE_TOOL_V0,
    DECANT_MATERIAL_CHANGES_TOOL_V0, DecantMcpBoundedTransportV0, DecantMcpServerV0,
    DecantMcpViewErrorV0, DecantMcpViewV0,
};
use decant_state::{
    ADMISSION_VERSION_V0, AdmissionConfigV0, AdmissionLimitsV0, AdmissionStateV0, ApplyOutcomeV0,
    EVENT_TIME_RETENTION_VERSION_V0, EventTimeRetentionConfigV0,
};
use rmcp::ServiceExt;
use serde_json::{Value, json};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader, ReadHalf, WriteHalf, duplex, split};

const VIEW_ID: &str = "mcp-material-view";

fn source() -> SourceIdV0 {
    SourceIdV0::new("mcp-test-source").unwrap()
}

fn schema() -> SchemaIdV0 {
    SchemaIdV0::new("mcp-test-schema").unwrap()
}

fn event(offset: i64, time: i64, value: u64) -> EventEnvelopeV0 {
    EventEnvelopeV0::new(
        0,
        source(),
        schema(),
        SourcePositionV0::kafka(0, offset).unwrap(),
        Some(b"synthetic-key"),
        time,
        0,
        SensitivityBitmapV0::from_bits(u64::MAX),
        CanonicalPayloadV0::try_from_fields(BTreeMap::from([
            ("series".to_owned(), ScalarValueV0::utf8("latency").unwrap()),
            ("value".to_owned(), ScalarValueV0::from_u64(value)),
            (
                "secret".to_owned(),
                ScalarValueV0::utf8("private-mcp-secret").unwrap(),
            ),
        ]))
        .unwrap(),
    )
    .unwrap()
}

fn compile(values: &[(i64, i64, u64)]) -> KafkaMaterialChangeContextV0 {
    let config = AdmissionConfigV0::new_with_source_position_kind(
        ADMISSION_VERSION_V0,
        source(),
        SourcePositionKindV0::Kafka,
        0,
        1_000_000,
        AdmissionLimitsV0::maximum(),
    )
    .unwrap();
    let mut state = AdmissionStateV0::new(config);
    for (offset, time, value) in values {
        assert_eq!(
            state.apply_event_v1(&event(*offset, *time, *value)),
            Ok(ApplyOutcomeV0::Accepted)
        );
    }
    let policy = KafkaFieldEgressPolicyV0::new(
        KAFKA_FIELD_EGRESS_POLICY_VERSION_V0,
        vec![
            KafkaFieldEgressRuleV0::new("series", KafkaFieldEgressActionV0::Allow).unwrap(),
            KafkaFieldEgressRuleV0::new("value", KafkaFieldEgressActionV0::Allow).unwrap(),
            KafkaFieldEgressRuleV0::new("secret", KafkaFieldEgressActionV0::Deny).unwrap(),
        ],
    )
    .unwrap();
    let request = KafkaMaterialChangeRequestV0::new(
        KAFKA_MATERIAL_CHANGE_CONTEXT_VERSION_V1,
        VIEW_ID,
        schema(),
        EventTimeRetentionConfigV0::new(EVENT_TIME_RETENTION_VERSION_V0, 100_000).unwrap(),
        "series",
        "value",
        8,
        MAX_KAFKA_MATERIAL_CHANGE_OUTPUT_BYTES_V0,
        policy,
        KafkaEntityKeyEgressActionV0::Deny,
    )
    .unwrap();
    compile_kafka_material_change_context_v1(&state, &request).unwrap()
}

#[tokio::test]
async fn official_server_lists_and_executes_all_bounded_tools_over_decant_transport() {
    let initial = compile(&[(0, 10, 10), (1, 20, 20)]);
    let view = DecantMcpViewV0::new(VIEW_ID, &initial).unwrap();
    let server = DecantMcpServerV0::new(view.clone());
    let usage_server = server.clone();
    let (client_io, server_io) = duplex(256 * 1024);
    let (server_read, server_write) = split(server_io);
    let transport = DecantMcpBoundedTransportV0::new(server_read, server_write);
    let server_task = tokio::spawn(async move {
        let running = server.serve(transport).await.unwrap();
        running.waiting().await.unwrap();
    });
    let (client_read, client_write) = split(client_io);
    let mut client_read = BufReader::new(client_read);
    let mut client_write = client_write;

    let initialize = exchange(
        &mut client_read,
        &mut client_write,
        json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "initialize",
            "params": {
                "protocolVersion": "2025-11-25",
                "capabilities": {},
                "clientInfo": {"name": "bounded-test-client", "version": "0"}
            }
        }),
    )
    .await;
    assert_eq!(initialize["result"]["protocolVersion"], "2025-11-25");
    assert_eq!(initialize["result"]["serverInfo"]["name"], "decant");
    assert!(initialize["result"]["capabilities"]["tools"].is_object());
    assert!(initialize["result"]["capabilities"]["resources"].is_null());
    send(
        &mut client_write,
        json!({
            "jsonrpc": "2.0",
            "method": "notifications/initialized"
        }),
    )
    .await;

    let listed = exchange(
        &mut client_read,
        &mut client_write,
        json!({"jsonrpc":"2.0","id":2,"method":"tools/list"}),
    )
    .await;
    assert_eq!(
        listed["result"]["tools"]
            .as_array()
            .unwrap()
            .iter()
            .map(|tool| tool["name"].as_str().unwrap())
            .collect::<Vec<_>>(),
        [
            DECANT_CONTEXT_SNAPSHOT_TOOL_V0,
            DECANT_MATERIAL_CHANGES_TOOL_V0,
            DECANT_EXPLAIN_PROVENANCE_TOOL_V0
        ]
    );

    let snapshot = call(
        &mut client_read,
        &mut client_write,
        3,
        DECANT_CONTEXT_SNAPSHOT_TOOL_V0,
        None,
    )
    .await;
    assert_eq!(
        snapshot["result"]["structuredContent"]["context_json"],
        std::str::from_utf8(initial.as_bytes()).unwrap()
    );
    assert!(!snapshot.to_string().contains("private-mcp-secret"));

    let material = call(
        &mut client_read,
        &mut client_write,
        4,
        DECANT_MATERIAL_CHANGES_TOOL_V0,
        Some(json!({})),
    )
    .await;
    assert_eq!(
        material["result"]["structuredContent"]["format"],
        "decant-mcp-material-changes"
    );

    let provenance = call(
        &mut client_read,
        &mut client_write,
        5,
        DECANT_EXPLAIN_PROVENANCE_TOOL_V0,
        Some(json!({"provenance_uri":"decant://event/v0/mcp-test-source/kafka/0/0"})),
    )
    .await;
    assert_eq!(provenance["result"]["structuredContent"]["found"], true);
    let explanation: Value = serde_json::from_str(
        provenance["result"]["structuredContent"]["explanation_json"]
            .as_str()
            .unwrap(),
    )
    .unwrap();
    assert_eq!(explanation["references"][0]["change_index"], 0);
    assert_eq!(explanation["references"][0]["role"], "previous");

    let absent = call(
        &mut client_read,
        &mut client_write,
        6,
        DECANT_EXPLAIN_PROVENANCE_TOOL_V0,
        Some(json!({"provenance_uri":"decant://event/v0/unknown-private/kafka/0/9"})),
    )
    .await;
    assert_eq!(absent["result"]["structuredContent"]["found"], false);
    assert!(!absent.to_string().contains("unknown-private"));

    let replacement = compile(&[(0, 10, 10), (1, 20, 20), (2, 30, 30)]);
    view.publish(&replacement).unwrap();
    let updated = call(
        &mut client_read,
        &mut client_write,
        7,
        DECANT_CONTEXT_SNAPSHOT_TOOL_V0,
        None,
    )
    .await;
    assert_eq!(
        updated["result"]["structuredContent"]["context_json"],
        std::str::from_utf8(replacement.as_bytes()).unwrap()
    );

    let invalid = call(
        &mut client_read,
        &mut client_write,
        8,
        DECANT_CONTEXT_SNAPSHOT_TOOL_V0,
        Some(json!({"field_name":"private-attacker-field"})),
    )
    .await;
    assert_eq!(invalid["error"]["code"], -32602);
    assert!(!invalid.to_string().contains("private-attacker-field"));

    let custom = exchange(
        &mut client_read,
        &mut client_write,
        json!({"jsonrpc":"2.0","id":9,"method":"private-attacker-method","params":{}}),
    )
    .await;
    assert_eq!(custom["error"]["code"], -32601);
    assert!(!custom.to_string().contains("private-attacker-method"));

    assert_eq!(usage_server.usage().tool_calls, 6);
    assert_eq!(usage_server.usage().successful_tool_calls, 5);
    assert_eq!(usage_server.usage().rejected_tool_calls, 1);
    assert_eq!(usage_server.usage().snapshot_calls, 2);
    assert_eq!(usage_server.usage().material_change_calls, 1);
    assert_eq!(usage_server.usage().provenance_calls, 2);

    client_write.shutdown().await.unwrap();
    server_task.await.unwrap();
}

#[test]
fn terminal_unavailability_hides_context_and_cannot_be_reversed() {
    let initial = compile(&[(0, 10, 10), (1, 20, 20)]);
    let view = DecantMcpViewV0::new(VIEW_ID, &initial).unwrap();
    view.mark_unavailable().unwrap();
    assert_eq!(
        view.publish(&initial).unwrap_err(),
        DecantMcpViewErrorV0::Unavailable
    );
}

#[test]
fn view_refuses_mismatched_server_identity_without_exposing_it() {
    let context = compile(&[(0, 10, 10), (1, 20, 20)]);
    let error = DecantMcpViewV0::new("different-view", &context).unwrap_err();
    assert_eq!(error, DecantMcpViewErrorV0::ViewMismatch);
    assert!(!format!("{error:?} {error}").contains(VIEW_ID));
}

async fn call(
    read: &mut BufReader<ReadHalf<tokio::io::DuplexStream>>,
    write: &mut WriteHalf<tokio::io::DuplexStream>,
    id: u64,
    name: &str,
    arguments: Option<Value>,
) -> Value {
    let mut params = serde_json::Map::new();
    params.insert("name".to_owned(), Value::String(name.to_owned()));
    if let Some(arguments) = arguments {
        params.insert("arguments".to_owned(), arguments);
    }
    exchange(
        read,
        write,
        json!({"jsonrpc":"2.0","id":id,"method":"tools/call","params":params}),
    )
    .await
}

async fn exchange(
    read: &mut BufReader<ReadHalf<tokio::io::DuplexStream>>,
    write: &mut WriteHalf<tokio::io::DuplexStream>,
    request: Value,
) -> Value {
    send(write, request).await;
    let mut line = String::new();
    read.read_line(&mut line).await.unwrap();
    assert!(!line.is_empty());
    serde_json::from_str(&line).unwrap()
}

async fn send(write: &mut WriteHalf<tokio::io::DuplexStream>, request: Value) {
    let mut bytes = serde_json::to_vec(&request).unwrap();
    bytes.push(b'\n');
    write.write_all(&bytes).await.unwrap();
    write.flush().await.unwrap();
}
