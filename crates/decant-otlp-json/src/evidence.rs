//! Additive evidence projection. The V0 decoder remains unchanged.

use serde::Deserialize;

use crate::{
    DecodeErrorV0, DecodeUsageV0, DecodedSpanV0, WireResourceV0, WireStringValueV0,
    decode_otlp_json_trace_record_v0,
};

pub struct DecodedSpanV1 {
    base: DecodedSpanV0,
    service_version: Option<String>,
    kind: u64,
}

impl DecodedSpanV1 {
    pub fn base(&self) -> &DecodedSpanV0 {
        &self.base
    }
    pub fn service_version(&self) -> Option<&str> {
        self.service_version.as_deref()
    }
    pub fn kind(&self) -> u64 {
        self.kind
    }
}

pub struct DecodedTraceLineV1 {
    line_number: u64,
    spans: Vec<DecodedSpanV1>,
    usage: DecodeUsageV0,
}

impl DecodedTraceLineV1 {
    pub fn line_number(&self) -> u64 {
        self.line_number
    }
    pub fn spans(&self) -> impl ExactSizeIterator<Item = &DecodedSpanV1> {
        self.spans.iter()
    }
    pub fn usage(&self) -> DecodeUsageV0 {
        self.usage
    }
}

#[derive(Deserialize)]
struct TraceMetadata {
    #[serde(default, rename = "resourceSpans")]
    resources: Vec<ResourceMetadata>,
}
#[derive(Deserialize)]
struct ResourceMetadata {
    #[serde(default)]
    resource: Option<WireResourceV0>,
    #[serde(default, rename = "scopeSpans")]
    scopes: Vec<ScopeMetadata>,
}
#[derive(Deserialize)]
struct ScopeMetadata {
    #[serde(default)]
    spans: Vec<SpanMetadata>,
}
#[derive(Deserialize)]
struct SpanMetadata {
    #[serde(default)]
    kind: u64,
}

pub fn decode_otlp_json_trace_record_v1(
    line_number: u64,
    input: &[u8],
) -> Result<DecodedTraceLineV1, DecodeErrorV0> {
    // Byte/framing/structural/identity bounds precede the supplementary pass.
    // Neither pass constructs arbitrary recursive Value trees.
    let base = decode_otlp_json_trace_record_v0(line_number, input)?;
    let extra: TraceMetadata =
        serde_json::from_slice(input).map_err(|_| DecodeErrorV0::MalformedJson)?;
    let mut usage = base.usage;
    let mut spans = Vec::new();
    spans
        .try_reserve_exact(base.spans.len())
        .map_err(|_| DecodeErrorV0::AllocationFailure {
            requested_spans: base.spans.len(),
        })?;
    let mut originals = base.spans.into_iter();
    for resource in extra.resources {
        let mut version = None;
        if let Some(resource) = resource.resource {
            for attribute in resource.attributes {
                if attribute.key != "service.version" {
                    continue;
                }
                if version.is_some() {
                    return Err(DecodeErrorV0::MalformedJson);
                }
                let value = attribute.value.ok_or(DecodeErrorV0::MalformedJson)?;
                if value.has_other_value_kind() {
                    return Err(DecodeErrorV0::MalformedJson);
                }
                let WireStringValueV0::String(value) = value.string_value else {
                    return Err(DecodeErrorV0::MalformedJson);
                };
                if value.is_empty() || value.len() > 256 {
                    return Err(DecodeErrorV0::MalformedJson);
                }
                version = Some(value);
            }
        }
        if version.is_some() {
            usage.resource_attributes_omitted = usage
                .resource_attributes_omitted
                .checked_sub(1)
                .ok_or(DecodeErrorV0::MalformedJson)?;
        }
        for scope in resource.scopes {
            for span in scope.spans {
                if span.kind > 5 {
                    return Err(DecodeErrorV0::MalformedJson);
                }
                spans.push(DecodedSpanV1 {
                    base: originals.next().ok_or(DecodeErrorV0::MalformedJson)?,
                    service_version: version.clone(),
                    kind: span.kind,
                });
            }
        }
    }
    if originals.next().is_some() {
        return Err(DecodeErrorV0::MalformedJson);
    }
    Ok(DecodedTraceLineV1 {
        line_number,
        spans,
        usage,
    })
}
