use std::{
    borrow::Cow,
    error::Error,
    fmt,
    sync::{
        Arc, RwLock,
        atomic::{AtomicUsize, Ordering},
    },
};

use decant_compiler::KafkaMaterialChangeContextV0;
use rmcp::{
    ErrorData, ServerHandler,
    model::{
        CallToolRequestParams, CallToolResponse, CallToolResult, CustomRequest, CustomResult,
        ErrorCode, Implementation, ListToolsResult, PaginatedRequestParams, ProtocolVersion,
        ServerCapabilities, ServerInfo, Tool,
    },
    service::{RequestContext, RoleServer},
};
use serde_json::{Map, Value, json};

use crate::{
    DECANT_MCP_CONTEXT_MAX_BYTES_V0, DECANT_MCP_PROVENANCE_EXPLANATION_MAX_BYTES_V0,
    DECANT_MCP_VIEW_ID_MAX_BYTES_V0, DecantMcpToolCallV0, decant_mcp_tools_v0,
    decode_decant_mcp_tool_call_v0,
};

pub const DECANT_MCP_IMPLEMENTATION_VERSION_V0: &str = "0.1.0-alpha.1";
pub const DECANT_MCP_MAX_PROVENANCE_REFERENCES_V0: usize = 16;

#[derive(Clone)]
pub struct DecantMcpViewV0 {
    view_id: Arc<str>,
    current: Arc<RwLock<Option<Arc<ValidatedContextV0>>>>,
}

#[derive(Debug)]
struct ValidatedContextV0 {
    json: String,
    parsed: Value,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DecantMcpViewErrorV0 {
    InvalidViewId,
    ContextTooLarge,
    InvalidCompilerContext,
    ViewMismatch,
    Unavailable,
    LockUnavailable,
    Serialization,
}

impl fmt::Display for DecantMcpViewErrorV0 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(self, formatter)
    }
}

impl Error for DecantMcpViewErrorV0 {}

impl DecantMcpViewV0 {
    pub fn new(
        view_id: &str,
        context: &KafkaMaterialChangeContextV0,
    ) -> Result<Self, DecantMcpViewErrorV0> {
        validate_view_id(view_id)?;
        let current = validate_context(view_id, context)?;
        Ok(Self {
            view_id: Arc::from(view_id),
            current: Arc::new(RwLock::new(Some(Arc::new(current)))),
        })
    }

    pub fn publish(
        &self,
        context: &KafkaMaterialChangeContextV0,
    ) -> Result<(), DecantMcpViewErrorV0> {
        let replacement = Arc::new(validate_context(&self.view_id, context)?);
        let mut current = self
            .current
            .write()
            .map_err(|_| DecantMcpViewErrorV0::LockUnavailable)?;
        if current.is_none() {
            return Err(DecantMcpViewErrorV0::Unavailable);
        }
        *current = Some(replacement);
        Ok(())
    }

    pub fn mark_unavailable(&self) -> Result<(), DecantMcpViewErrorV0> {
        let mut current = self
            .current
            .write()
            .map_err(|_| DecantMcpViewErrorV0::LockUnavailable)?;
        *current = None;
        Ok(())
    }

    pub fn view_id(&self) -> &str {
        &self.view_id
    }

    fn snapshot(&self) -> Result<Arc<ValidatedContextV0>, DecantMcpViewErrorV0> {
        self.current
            .read()
            .map_err(|_| DecantMcpViewErrorV0::LockUnavailable)?
            .as_ref()
            .cloned()
            .ok_or(DecantMcpViewErrorV0::Unavailable)
    }

    fn context_result(&self, format: &'static str) -> Result<Value, DecantMcpViewErrorV0> {
        let snapshot = self.snapshot()?;
        Ok(json!({
            "context_json": snapshot.json,
            "format": format,
            "version": 0,
            "view_id": self.view_id.as_ref()
        }))
    }

    fn provenance_result(&self, provenance_uri: &str) -> Result<Value, DecantMcpViewErrorV0> {
        let snapshot = self.snapshot()?;
        let mut references = Vec::new();
        let mut matched_count = 0usize;
        if let Some(items) = snapshot
            .parsed
            .get("changes")
            .and_then(|changes| changes.get("items"))
            .and_then(Value::as_array)
        {
            for (change_index, item) in items.iter().enumerate() {
                for role in ["previous", "current"] {
                    if item
                        .get(role)
                        .and_then(|observation| observation.get("provenance"))
                        .and_then(Value::as_str)
                        == Some(provenance_uri)
                    {
                        matched_count = matched_count
                            .checked_add(1)
                            .ok_or(DecantMcpViewErrorV0::Serialization)?;
                        if references.len() < DECANT_MCP_MAX_PROVENANCE_REFERENCES_V0 {
                            references.push(json!({
                                "change_index": change_index,
                                "role": role
                            }));
                        }
                    }
                }
            }
        }
        let explanation = json!({
            "format": "decant-provenance-explanation",
            "version": 0,
            "found": matched_count > 0,
            "references": references,
            "omitted_count": matched_count.saturating_sub(DECANT_MCP_MAX_PROVENANCE_REFERENCES_V0)
        });
        let explanation_json =
            serde_json::to_string(&explanation).map_err(|_| DecantMcpViewErrorV0::Serialization)?;
        if explanation_json.len() > DECANT_MCP_PROVENANCE_EXPLANATION_MAX_BYTES_V0 {
            return Err(DecantMcpViewErrorV0::Serialization);
        }
        Ok(json!({
            "explanation_json": explanation_json,
            "format": "decant-mcp-provenance-explanation",
            "found": matched_count > 0,
            "version": 0,
            "view_id": self.view_id.as_ref()
        }))
    }
}

impl fmt::Debug for DecantMcpViewV0 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("DecantMcpViewV0")
            .field("view_id_bytes", &self.view_id.len())
            .finish_non_exhaustive()
    }
}

#[derive(Clone, Debug)]
pub struct DecantMcpServerV0 {
    view: DecantMcpViewV0,
    usage: Arc<DecantMcpUsageInnerV0>,
}

#[derive(Debug, Default)]
struct DecantMcpUsageInnerV0 {
    tool_calls: AtomicUsize,
    successful_tool_calls: AtomicUsize,
    rejected_tool_calls: AtomicUsize,
    snapshot_calls: AtomicUsize,
    material_change_calls: AtomicUsize,
    provenance_calls: AtomicUsize,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct DecantMcpUsageV0 {
    pub tool_calls: usize,
    pub successful_tool_calls: usize,
    pub rejected_tool_calls: usize,
    pub snapshot_calls: usize,
    pub material_change_calls: usize,
    pub provenance_calls: usize,
}

impl DecantMcpServerV0 {
    pub fn new(view: DecantMcpViewV0) -> Self {
        Self {
            view,
            usage: Arc::new(DecantMcpUsageInnerV0::default()),
        }
    }

    pub fn view(&self) -> &DecantMcpViewV0 {
        &self.view
    }

    pub fn usage(&self) -> DecantMcpUsageV0 {
        DecantMcpUsageV0 {
            tool_calls: self.usage.tool_calls.load(Ordering::Relaxed),
            successful_tool_calls: self.usage.successful_tool_calls.load(Ordering::Relaxed),
            rejected_tool_calls: self.usage.rejected_tool_calls.load(Ordering::Relaxed),
            snapshot_calls: self.usage.snapshot_calls.load(Ordering::Relaxed),
            material_change_calls: self.usage.material_change_calls.load(Ordering::Relaxed),
            provenance_calls: self.usage.provenance_calls.load(Ordering::Relaxed),
        }
    }

    fn execute(&self, request: CallToolRequestParams) -> Result<CallToolResponse, ErrorData> {
        saturating_increment(&self.usage.tool_calls);
        let result = self.execute_inner(request);
        saturating_increment(if result.is_ok() {
            &self.usage.successful_tool_calls
        } else {
            &self.usage.rejected_tool_calls
        });
        result
    }

    fn execute_inner(&self, request: CallToolRequestParams) -> Result<CallToolResponse, ErrorData> {
        if request.input_responses.is_some() || request.request_state.is_some() {
            return Err(ErrorData::invalid_params(
                "unsupported Decant tool continuation",
                None,
            ));
        }
        let call =
            decode_decant_mcp_tool_call_v0(&request.name, request.arguments).map_err(|error| {
                match error {
                    crate::DecantMcpToolCallErrorV0::UnknownTool => {
                        ErrorData::new(ErrorCode::METHOD_NOT_FOUND, "unknown Decant tool", None)
                    }
                    _ => ErrorData::invalid_params("invalid Decant tool arguments", None),
                }
            })?;
        let value = match call {
            DecantMcpToolCallV0::ContextSnapshot => {
                saturating_increment(&self.usage.snapshot_calls);
                self.view.context_result("decant-mcp-context-snapshot")
            }
            DecantMcpToolCallV0::MaterialChanges => {
                saturating_increment(&self.usage.material_change_calls);
                self.view.context_result("decant-mcp-material-changes")
            }
            DecantMcpToolCallV0::ExplainProvenance { provenance_uri } => {
                saturating_increment(&self.usage.provenance_calls);
                self.view.provenance_result(provenance_uri.as_str())
            }
        }
        .map_err(|_| ErrorData::internal_error("Decant view unavailable", None))?;
        Ok(CallToolResult::structured(value).into())
    }
}

fn saturating_increment(value: &AtomicUsize) {
    let _ = value.fetch_update(Ordering::Relaxed, Ordering::Relaxed, |current| {
        Some(current.saturating_add(1))
    });
}

impl ServerHandler for DecantMcpServerV0 {
    fn get_info(&self) -> ServerInfo {
        ServerInfo::new(ServerCapabilities::builder().enable_tools().build())
            .with_protocol_version(ProtocolVersion::V_2025_11_25)
            .with_server_info(
                Implementation::new("decant", DECANT_MCP_IMPLEMENTATION_VERSION_V0)
                    .with_title("Decant"),
            )
            .with_instructions(
                "Read-only bounded context from one server-configured Decant view. Material changes are exact observations, not anomaly or causal claims. Results may be provisional.",
            )
    }

    fn supported_protocol_versions(&self) -> Cow<'static, [ProtocolVersion]> {
        Cow::Owned(vec![ProtocolVersion::V_2025_11_25])
    }

    fn list_tools(
        &self,
        request: Option<PaginatedRequestParams>,
        _context: RequestContext<RoleServer>,
    ) -> impl Future<Output = Result<ListToolsResult, ErrorData>> + Send + '_ {
        std::future::ready(if request.is_some_and(|request| request.cursor.is_some()) {
            Err(ErrorData::invalid_params(
                "Decant tool catalog is not paginated",
                None,
            ))
        } else {
            Ok(ListToolsResult::with_all_items(decant_mcp_tools_v0()))
        })
    }

    fn get_tool(&self, name: &str) -> Option<Tool> {
        decant_mcp_tools_v0()
            .into_iter()
            .find(|tool| tool.name == name)
    }

    fn call_tool(
        &self,
        request: CallToolRequestParams,
        _context: RequestContext<RoleServer>,
    ) -> impl Future<Output = Result<CallToolResponse, ErrorData>> + Send + '_ {
        std::future::ready(self.execute(request))
    }

    fn on_custom_request(
        &self,
        _request: CustomRequest,
        _context: RequestContext<RoleServer>,
    ) -> impl Future<Output = Result<CustomResult, ErrorData>> + Send + '_ {
        std::future::ready(Err(ErrorData::new(
            ErrorCode::METHOD_NOT_FOUND,
            "unsupported Decant method",
            None,
        )))
    }
}

fn validate_view_id(value: &str) -> Result<(), DecantMcpViewErrorV0> {
    let bytes = value.as_bytes();
    if bytes.is_empty()
        || bytes.len() > DECANT_MCP_VIEW_ID_MAX_BYTES_V0
        || !(bytes[0].is_ascii_lowercase() || bytes[0].is_ascii_digit())
        || !bytes[1..].iter().all(|byte| {
            byte.is_ascii_lowercase() || byte.is_ascii_digit() || matches!(byte, b'.' | b'_' | b'-')
        })
    {
        return Err(DecantMcpViewErrorV0::InvalidViewId);
    }
    Ok(())
}

fn validate_context(
    view_id: &str,
    context: &KafkaMaterialChangeContextV0,
) -> Result<ValidatedContextV0, DecantMcpViewErrorV0> {
    if context.as_bytes().len() > DECANT_MCP_CONTEXT_MAX_BYTES_V0 {
        return Err(DecantMcpViewErrorV0::ContextTooLarge);
    }
    let json = std::str::from_utf8(context.as_bytes())
        .map_err(|_| DecantMcpViewErrorV0::InvalidCompilerContext)?
        .to_owned();
    let parsed: Value =
        serde_json::from_str(&json).map_err(|_| DecantMcpViewErrorV0::InvalidCompilerContext)?;
    let object = parsed
        .as_object()
        .ok_or(DecantMcpViewErrorV0::InvalidCompilerContext)?;
    validate_compiler_shape(object)?;
    if object.get("view_id").and_then(Value::as_str) != Some(view_id) {
        return Err(DecantMcpViewErrorV0::ViewMismatch);
    }
    Ok(ValidatedContextV0 { json, parsed })
}

fn validate_compiler_shape(object: &Map<String, Value>) -> Result<(), DecantMcpViewErrorV0> {
    if object.get("format").and_then(Value::as_str) != Some("decant-kafka-material-change-context")
        || object.get("version").and_then(Value::as_u64) != Some(1)
        || object
            .get("changes")
            .and_then(|changes| changes.get("items"))
            .and_then(Value::as_array)
            .is_none()
    {
        return Err(DecantMcpViewErrorV0::InvalidCompilerContext);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn view_id_validation_is_exact_and_safe() {
        for valid in ["a", "0", "material.changes_v0"] {
            assert_eq!(validate_view_id(valid), Ok(()));
        }
        for invalid in ["", "Upper", "under space", "_leading"] {
            assert_eq!(
                validate_view_id(invalid),
                Err(DecantMcpViewErrorV0::InvalidViewId)
            );
        }
        assert_eq!(
            validate_view_id(&"a".repeat(DECANT_MCP_VIEW_ID_MAX_BYTES_V0 + 1)),
            Err(DecantMcpViewErrorV0::InvalidViewId)
        );
    }

    #[test]
    fn errors_are_content_free() {
        for error in [
            DecantMcpViewErrorV0::InvalidViewId,
            DecantMcpViewErrorV0::ContextTooLarge,
            DecantMcpViewErrorV0::InvalidCompilerContext,
            DecantMcpViewErrorV0::ViewMismatch,
            DecantMcpViewErrorV0::LockUnavailable,
            DecantMcpViewErrorV0::Serialization,
        ] {
            let diagnostic = format!("{error:?} {error}");
            assert!(!diagnostic.contains("private"));
        }
    }
}
