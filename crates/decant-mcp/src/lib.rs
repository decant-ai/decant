//! Bounded, read-only MCP contracts for Decant.

#![deny(unsafe_code)]

use std::{error::Error, fmt, sync::Arc};

use rmcp::model::{JsonObject, Tool, ToolAnnotations};
use serde_json::{Value, json};

pub mod evidence;
mod server;
mod transport;

pub use server::{
    DECANT_MCP_IMPLEMENTATION_VERSION_V0, DECANT_MCP_MAX_PROVENANCE_REFERENCES_V0,
    DecantMcpServerV0, DecantMcpUsageV0, DecantMcpViewErrorV0, DecantMcpViewV0,
};
pub use transport::{
    DECANT_MCP_INPUT_JSON_LINE_MAX_BYTES_V0, DECANT_MCP_MAX_IN_FLIGHT_REQUESTS_V0,
    DECANT_MCP_OUTPUT_JSON_LINE_MAX_BYTES_V0, DecantMcpBoundedTransportErrorV0,
    DecantMcpBoundedTransportV0,
};

pub const DECANT_MCP_CONTRACT_VERSION_V0: u16 = 0;
pub const DECANT_MCP_PROTOCOL_VERSION_V0: &str = "2025-11-25";
pub const DECANT_MCP_CONTEXT_MAX_BYTES_V0: usize = 16 * 1024;
pub const DECANT_MCP_PROVENANCE_EXPLANATION_MAX_BYTES_V0: usize = 4 * 1024;
pub const DECANT_MCP_PROVENANCE_URI_MAX_BYTES_V0: usize = 512;
pub const DECANT_MCP_VIEW_ID_MAX_BYTES_V0: usize = 128;

pub const DECANT_CONTEXT_SNAPSHOT_TOOL_V0: &str = "decant_context_snapshot";
pub const DECANT_MATERIAL_CHANGES_TOOL_V0: &str = "decant_material_changes";
pub const DECANT_EXPLAIN_PROVENANCE_TOOL_V0: &str = "decant_explain_provenance";

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum DecantMcpToolCallV0 {
    ContextSnapshot,
    MaterialChanges,
    ExplainProvenance {
        provenance_uri: DecantMcpProvenanceUriV0,
    },
}

#[derive(Clone, Eq, PartialEq)]
pub struct DecantMcpProvenanceUriV0(String);

impl DecantMcpProvenanceUriV0 {
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Debug for DecantMcpProvenanceUriV0 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("DecantMcpProvenanceUriV0")
            .field("byte_len", &self.0.len())
            .finish()
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DecantMcpToolCallErrorV0 {
    UnknownTool,
    ArgumentsRequired,
    ArgumentsNotAllowed,
    UnexpectedArgument,
    ProvenanceUriNotString,
    ProvenanceUriInvalid,
}

impl fmt::Display for DecantMcpToolCallErrorV0 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(self, formatter)
    }
}

impl Error for DecantMcpToolCallErrorV0 {}

pub fn decant_mcp_tools_v0() -> Vec<Tool> {
    vec![
        read_only_tool(
            DECANT_CONTEXT_SNAPSHOT_TOOL_V0,
            "Decant context snapshot",
            "Returns the current bounded, policy-filtered compiled context for the configured Decant view. The result may be provisional when its source epoch is not finalized.",
            empty_input_schema(),
            context_output_schema("decant-mcp-context-snapshot"),
        ),
        read_only_tool(
            DECANT_MATERIAL_CHANGES_TOOL_V0,
            "Decant material changes",
            "Returns the current bounded exact-difference material-change context for the configured Decant view. Changes are observations, not anomalies or causal claims.",
            empty_input_schema(),
            context_output_schema("decant-mcp-material-changes"),
        ),
        read_only_tool(
            DECANT_EXPLAIN_PROVENANCE_TOOL_V0,
            "Explain Decant provenance",
            "Returns bounded, policy-filtered context evidence that already references one canonical Decant provenance URI. Absence does not prove that a source event does not exist.",
            provenance_input_schema(),
            provenance_output_schema(),
        ),
    ]
}

pub fn decode_decant_mcp_tool_call_v0(
    tool_name: &str,
    arguments: Option<JsonObject>,
) -> Result<DecantMcpToolCallV0, DecantMcpToolCallErrorV0> {
    match tool_name {
        DECANT_CONTEXT_SNAPSHOT_TOOL_V0 => {
            require_empty_arguments(arguments)?;
            Ok(DecantMcpToolCallV0::ContextSnapshot)
        }
        DECANT_MATERIAL_CHANGES_TOOL_V0 => {
            require_empty_arguments(arguments)?;
            Ok(DecantMcpToolCallV0::MaterialChanges)
        }
        DECANT_EXPLAIN_PROVENANCE_TOOL_V0 => {
            let arguments = arguments.ok_or(DecantMcpToolCallErrorV0::ArgumentsRequired)?;
            if arguments.len() != 1 || !arguments.contains_key("provenance_uri") {
                return Err(DecantMcpToolCallErrorV0::UnexpectedArgument);
            }
            let raw_uri = arguments
                .get("provenance_uri")
                .and_then(Value::as_str)
                .ok_or(DecantMcpToolCallErrorV0::ProvenanceUriNotString)?;
            if !valid_provenance_uri(raw_uri) {
                return Err(DecantMcpToolCallErrorV0::ProvenanceUriInvalid);
            }
            Ok(DecantMcpToolCallV0::ExplainProvenance {
                provenance_uri: DecantMcpProvenanceUriV0(raw_uri.to_owned()),
            })
        }
        _ => Err(DecantMcpToolCallErrorV0::UnknownTool),
    }
}

fn require_empty_arguments(arguments: Option<JsonObject>) -> Result<(), DecantMcpToolCallErrorV0> {
    if arguments.is_some_and(|arguments| !arguments.is_empty()) {
        return Err(DecantMcpToolCallErrorV0::ArgumentsNotAllowed);
    }
    Ok(())
}

fn valid_provenance_uri(uri: &str) -> bool {
    const PREFIX: &str = "decant://event/v0/";
    uri.len() > PREFIX.len()
        && uri.len() <= DECANT_MCP_PROVENANCE_URI_MAX_BYTES_V0
        && uri.starts_with(PREFIX)
        && uri.bytes().all(|byte| byte.is_ascii_graphic())
}

fn read_only_tool(
    name: &'static str,
    title: &'static str,
    description: &'static str,
    input_schema: Arc<JsonObject>,
    output_schema: Arc<JsonObject>,
) -> Tool {
    Tool::new(name, description, input_schema)
        .with_title(title)
        .with_raw_output_schema(output_schema)
        .with_annotations(
            ToolAnnotations::new()
                .read_only(true)
                .destructive(false)
                .idempotent(true)
                .open_world(false),
        )
}

fn empty_input_schema() -> Arc<JsonObject> {
    object(json!({
        "$schema": "https://json-schema.org/draft/2020-12/schema",
        "additionalProperties": false,
        "properties": {},
        "type": "object"
    }))
}

fn provenance_input_schema() -> Arc<JsonObject> {
    object(json!({
        "$schema": "https://json-schema.org/draft/2020-12/schema",
        "additionalProperties": false,
        "properties": {
            "provenance_uri": {
                "maxLength": DECANT_MCP_PROVENANCE_URI_MAX_BYTES_V0,
                "minLength": 19,
                "pattern": "^decant://event/v0/\\S+$",
                "type": "string"
            }
        },
        "required": ["provenance_uri"],
        "type": "object"
    }))
}

fn context_output_schema(format: &'static str) -> Arc<JsonObject> {
    object(json!({
        "$schema": "https://json-schema.org/draft/2020-12/schema",
        "additionalProperties": false,
        "properties": {
            "context_json": {
                "maxLength": DECANT_MCP_CONTEXT_MAX_BYTES_V0,
                "type": "string"
            },
            "format": { "const": format },
            "version": { "const": DECANT_MCP_CONTRACT_VERSION_V0 },
            "view_id": {
                "maxLength": DECANT_MCP_VIEW_ID_MAX_BYTES_V0,
                "minLength": 1,
                "pattern": "^[a-z0-9][a-z0-9._-]*$",
                "type": "string"
            }
        },
        "required": ["format", "version", "view_id", "context_json"],
        "type": "object"
    }))
}

fn provenance_output_schema() -> Arc<JsonObject> {
    object(json!({
        "$schema": "https://json-schema.org/draft/2020-12/schema",
        "additionalProperties": false,
        "properties": {
            "explanation_json": {
                "maxLength": DECANT_MCP_PROVENANCE_EXPLANATION_MAX_BYTES_V0,
                "type": "string"
            },
            "format": { "const": "decant-mcp-provenance-explanation" },
            "found": { "type": "boolean" },
            "version": { "const": DECANT_MCP_CONTRACT_VERSION_V0 },
            "view_id": {
                "maxLength": DECANT_MCP_VIEW_ID_MAX_BYTES_V0,
                "minLength": 1,
                "pattern": "^[a-z0-9][a-z0-9._-]*$",
                "type": "string"
            }
        },
        "required": ["format", "version", "view_id", "found", "explanation_json"],
        "type": "object"
    }))
}

fn object(value: Value) -> Arc<JsonObject> {
    let Value::Object(object) = value else {
        unreachable!("all Decant MCP schemas are object literals")
    };
    Arc::new(object)
}
