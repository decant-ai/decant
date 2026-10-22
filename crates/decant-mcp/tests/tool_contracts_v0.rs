use rmcp::model::JsonObject;
use serde_json::{Map, Value, json};

use decant_mcp::{
    DECANT_CONTEXT_SNAPSHOT_TOOL_V0, DECANT_EXPLAIN_PROVENANCE_TOOL_V0,
    DECANT_MATERIAL_CHANGES_TOOL_V0, DECANT_MCP_CONTEXT_MAX_BYTES_V0,
    DECANT_MCP_PROVENANCE_EXPLANATION_MAX_BYTES_V0, DECANT_MCP_PROVENANCE_URI_MAX_BYTES_V0,
    DecantMcpToolCallErrorV0, DecantMcpToolCallV0, decant_mcp_tools_v0,
    decode_decant_mcp_tool_call_v0,
};

#[test]
fn tool_catalog_is_exact_read_only_and_deterministic() {
    let first = decant_mcp_tools_v0();
    let second = decant_mcp_tools_v0();
    assert_eq!(first, second);
    assert_eq!(
        first
            .iter()
            .map(|tool| tool.name.as_ref())
            .collect::<Vec<_>>(),
        [
            DECANT_CONTEXT_SNAPSHOT_TOOL_V0,
            DECANT_MATERIAL_CHANGES_TOOL_V0,
            DECANT_EXPLAIN_PROVENANCE_TOOL_V0,
        ]
    );

    for tool in &first {
        let annotations = tool.annotations.as_ref().expect("annotations");
        assert_eq!(annotations.read_only_hint, Some(true));
        assert_eq!(annotations.destructive_hint, Some(false));
        assert_eq!(annotations.idempotent_hint, Some(true));
        assert_eq!(annotations.open_world_hint, Some(false));
        assert!(tool.icons.is_none());
        assert!(tool.meta.is_none());
        assert!(tool.output_schema.is_some());
    }

    assert_eq!(
        serde_json::to_vec(&first).expect("serialize first catalog"),
        serde_json::to_vec(&second).expect("serialize second catalog")
    );
}

#[test]
fn catalog_schemas_are_strict_bounded_and_agent_cannot_select_fields() {
    let tools = decant_mcp_tools_v0();
    for tool in &tools[..2] {
        assert_eq!(tool.input_schema.as_ref(), &empty_object_schema());
        let output = tool.output_schema.as_ref().expect("output schema");
        assert_eq!(output["additionalProperties"], false);
        assert_eq!(
            output["properties"]["context_json"]["maxLength"],
            DECANT_MCP_CONTEXT_MAX_BYTES_V0
        );
        let encoded = serde_json::to_string(&tool.input_schema).expect("serialize input schema");
        assert!(!encoded.contains("field_name"));
        assert!(!encoded.contains("policy"));
        assert!(!encoded.contains("mapping"));
    }

    let provenance = &tools[2];
    assert_eq!(provenance.input_schema["additionalProperties"], false);
    assert_eq!(
        provenance.input_schema["properties"]["provenance_uri"]["maxLength"],
        DECANT_MCP_PROVENANCE_URI_MAX_BYTES_V0
    );
    assert_eq!(
        provenance.output_schema.as_ref().expect("output schema")["properties"]["explanation_json"]
            ["maxLength"],
        DECANT_MCP_PROVENANCE_EXPLANATION_MAX_BYTES_V0
    );
}

#[test]
fn call_decoder_enforces_schema_independently_and_errors_are_value_safe() {
    assert_eq!(
        decode_decant_mcp_tool_call_v0(DECANT_CONTEXT_SNAPSHOT_TOOL_V0, None),
        Ok(DecantMcpToolCallV0::ContextSnapshot)
    );
    assert_eq!(
        decode_decant_mcp_tool_call_v0(DECANT_MATERIAL_CHANGES_TOOL_V0, Some(JsonObject::new())),
        Ok(DecantMcpToolCallV0::MaterialChanges)
    );
    assert_eq!(
        decode_decant_mcp_tool_call_v0(
            DECANT_CONTEXT_SNAPSHOT_TOOL_V0,
            Some(object(json!({"field_name": "private"})))
        ),
        Err(DecantMcpToolCallErrorV0::ArgumentsNotAllowed)
    );

    let valid_uri = "decant://event/v0/kafka-source/kafka/0/42";
    let decoded = decode_decant_mcp_tool_call_v0(
        DECANT_EXPLAIN_PROVENANCE_TOOL_V0,
        Some(object(json!({"provenance_uri": valid_uri}))),
    )
    .expect("valid provenance call");
    let DecantMcpToolCallV0::ExplainProvenance { provenance_uri } = decoded else {
        panic!("expected provenance call")
    };
    assert_eq!(provenance_uri.as_str(), valid_uri);

    for (arguments, expected) in [
        (None, DecantMcpToolCallErrorV0::ArgumentsRequired),
        (
            Some(object(json!({"provenance_uri": valid_uri, "extra": 1}))),
            DecantMcpToolCallErrorV0::UnexpectedArgument,
        ),
        (
            Some(object(json!({"provenance_uri": 7}))),
            DecantMcpToolCallErrorV0::ProvenanceUriNotString,
        ),
        (
            Some(object(json!({"provenance_uri": "decant://wrong"}))),
            DecantMcpToolCallErrorV0::ProvenanceUriInvalid,
        ),
        (
            Some(object(json!({"provenance_uri": "decant://event/v0/a b"}))),
            DecantMcpToolCallErrorV0::ProvenanceUriInvalid,
        ),
    ] {
        let error = decode_decant_mcp_tool_call_v0(DECANT_EXPLAIN_PROVENANCE_TOOL_V0, arguments)
            .expect_err("invalid call");
        assert_eq!(error, expected);
        let diagnostic = format!("{error:?} {error}");
        assert!(!diagnostic.contains("kafka-source"));
        assert!(!diagnostic.contains("a b"));
    }

    let oversized = format!(
        "decant://event/v0/{}",
        "x".repeat(DECANT_MCP_PROVENANCE_URI_MAX_BYTES_V0)
    );
    assert_eq!(
        decode_decant_mcp_tool_call_v0(
            DECANT_EXPLAIN_PROVENANCE_TOOL_V0,
            Some(object(json!({"provenance_uri": oversized})))
        ),
        Err(DecantMcpToolCallErrorV0::ProvenanceUriInvalid)
    );
    assert_eq!(
        decode_decant_mcp_tool_call_v0("attacker-secret-tool-name", None),
        Err(DecantMcpToolCallErrorV0::UnknownTool)
    );
}

fn empty_object_schema() -> JsonObject {
    object(json!({
        "$schema": "https://json-schema.org/draft/2020-12/schema",
        "additionalProperties": false,
        "properties": {},
        "type": "object"
    }))
}

fn object(value: Value) -> Map<String, Value> {
    value.as_object().expect("object fixture").clone()
}
