//! Pure retained observed-span projection. No clocks, transport, or serialization.

use std::collections::BTreeMap;

use decant_ir::{EventEnvelopeV0, ScalarValueV0};

use crate::{AdmissionStateV0, MAX_ACCEPTED_IDENTITIES_V0};

pub const FIVE_MINUTES_NS: u64 = 300_000_000_000;
pub const MAX_HORIZON_NS: u64 = 900_000_000_000;
pub const MAX_SERVICES: usize = 256;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum EvidenceErrorV1 {
    Configuration,
    EventContract,
    ConflictingSpan,
    Capacity,
    Arithmetic,
    Encoding,
    Verification,
    Policy,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RecipeV1 {
    pub window_ns: u64,
    pub retention_ns: u64,
    pub max_services: usize,
}

impl Default for RecipeV1 {
    fn default() -> Self {
        Self {
            window_ns: FIVE_MINUTES_NS,
            retention_ns: 2 * FIVE_MINUTES_NS,
            max_services: MAX_SERVICES,
        }
    }
}

impl RecipeV1 {
    pub fn validate(self) -> Result<(), EvidenceErrorV1> {
        if self.window_ns == 0
            || self.window_ns > MAX_HORIZON_NS
            || self.retention_ns == 0
            || self.retention_ns > MAX_HORIZON_NS
            || self.max_services == 0
            || self.max_services > MAX_SERVICES
        {
            return Err(EvidenceErrorV1::Configuration);
        }
        Ok(())
    }
}

// Deliberately no Debug: labels, IDs and source references can be sensitive.
pub struct IntervalV1<'a> {
    pub count: u64,
    pub errors: u64,
    pub unset_status: u64,
    pub p95_ns: Option<u64>,
    pub duration_sum_ns: u128,
    pub missing_versions: u64,
    pub first_version: Option<&'a str>,
    pub last_version: Option<&'a str>,
    pub version_transitions: u64,
    pub evidence: Vec<&'a str>,
}

pub struct ServiceV1<'a> {
    pub name: &'a str,
    pub baseline: IntervalV1<'a>,
    pub current: IntervalV1<'a>,
}

pub struct ReductionV1<'a> {
    pub anchor_ns: Option<i64>,
    pub retention_cutoff_ns: Option<i64>,
    pub expired: usize,
    pub duplicates: usize,
    pub non_server: usize,
    pub missing_service: usize,
    pub outside_windows: usize,
    pub services: Vec<ServiceV1<'a>>,
}

#[derive(Clone, Copy)]
struct Span<'a> {
    event: &'a EventEnvelopeV0,
    trace: &'a str,
    id: &'a str,
    service: Option<&'a str>,
    version: Option<&'a str>,
    kind: u64,
    status: u64,
    duration: u64,
}

fn field<'a>(event: &'a EventEnvelopeV0, name: &str) -> Option<&'a ScalarValueV0> {
    event
        .payload()
        .fields()
        .find_map(|(key, value)| (key == name).then_some(value))
}

fn label<'a>(event: &'a EventEnvelopeV0, name: &str) -> Result<Option<&'a str>, EvidenceErrorV1> {
    match field(event, name) {
        None => Ok(None),
        Some(value) if value.kind() == decant_ir::ScalarKindV0::Null => Ok(None),
        Some(value) => match value.as_utf8() {
            Some(text) if !text.is_empty() && text.len() <= 256 => Ok(Some(text)),
            _ => Err(EvidenceErrorV1::EventContract),
        },
    }
}

pub fn validate_span_v1(event: &EventEnvelopeV0) -> Result<(), EvidenceErrorV1> {
    parse(event).map(|_| ())
}

fn parse(event: &EventEnvelopeV0) -> Result<Span<'_>, EvidenceErrorV1> {
    let id = |name, length| {
        let value = field(event, name)
            .and_then(ScalarValueV0::as_utf8)
            .ok_or(EvidenceErrorV1::EventContract)?;
        if value.len() != length
            || !value
                .bytes()
                .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
            || value.bytes().all(|b| b == b'0')
        {
            return Err(EvidenceErrorV1::EventContract);
        }
        Ok(value)
    };
    let number = |name| {
        field(event, name)
            .and_then(ScalarValueV0::as_u64)
            .ok_or(EvidenceErrorV1::EventContract)
    };
    let span = Span {
        event,
        trace: id("trace.id", 32)?,
        id: id("span.id", 16)?,
        service: label(event, "service.name")?,
        version: label(event, "service.version")?,
        kind: number("span.kind")?,
        status: number("status.code")?,
        duration: number("duration.ns")?,
    };
    if span.kind > 5 || span.status > 2 {
        return Err(EvidenceErrorV1::EventContract);
    }
    Ok(span)
}

fn equivalent(a: Span<'_>, b: Span<'_>) -> bool {
    a.event.event_time_ns() == b.event.event_time_ns()
        && a.service == b.service
        && a.version == b.version
        && a.kind == b.kind
        && a.status == b.status
        && a.duration == b.duration
}

pub fn reduce_sre_evidence_v1(
    state: &AdmissionStateV0,
    recipe: RecipeV1,
) -> Result<ReductionV1<'_>, EvidenceErrorV1> {
    recipe.validate()?;
    let anchor = state
        .maximum_accepted_event_time_ns()
        .map(|time| time.checked_add(1).ok_or(EvidenceErrorV1::Arithmetic))
        .transpose()?;
    let cutoff = anchor.map(|time| time.saturating_sub(recipe.retention_ns as i64));
    let mut result = ReductionV1 {
        anchor_ns: anchor,
        retention_cutoff_ns: cutoff,
        expired: 0,
        duplicates: 0,
        non_server: 0,
        missing_service: 0,
        outside_windows: 0,
        services: Vec::new(),
    };
    let Some(end) = anchor else { return Ok(result) };
    let middle = end.saturating_sub(recipe.window_ns as i64);
    let start = middle.saturating_sub(recipe.window_ns as i64);
    // Each map/vector is bounded by the admission identity cap; no per-service
    // multiplication of the quantile capacity. Borrow input labels, never copy payloads.
    let mut identities = BTreeMap::<(&str, &str), Span<'_>>::new();
    for event in state.accepted_events() {
        if cutoff.is_some_and(|cutoff| event.event_time_ns() < cutoff) {
            result.expired += 1;
            continue;
        }
        let span = parse(event)?;
        if let Some(previous) = identities.get_mut(&(span.trace, span.id)) {
            if !equivalent(*previous, span) {
                return Err(EvidenceErrorV1::ConflictingSpan);
            }
            result.duplicates += 1;
            if crate::event_rank_v0(event) < crate::event_rank_v0(previous.event) {
                *previous = span;
            }
        } else {
            if identities.len() >= MAX_ACCEPTED_IDENTITIES_V0 {
                return Err(EvidenceErrorV1::Capacity);
            }
            identities.insert((span.trace, span.id), span);
        }
    }
    let mut groups = BTreeMap::<&str, Vec<Span<'_>>>::new();
    for span in identities.values() {
        if span.kind != 2 {
            result.non_server += 1;
            continue;
        }
        let Some(service) = span.service else {
            result.missing_service += 1;
            continue;
        };
        if span.event.event_time_ns() < start {
            result.outside_windows += 1;
            continue;
        }
        if !groups.contains_key(service) && groups.len() >= recipe.max_services {
            return Err(EvidenceErrorV1::Capacity);
        }
        groups.entry(service).or_default().push(*span);
    }
    for (name, mut spans) in groups {
        spans.sort_by_key(|span| crate::event_rank_v0(span.event));
        let split = spans.partition_point(|span| span.event.event_time_ns() < middle);
        result.services.push(ServiceV1 {
            name,
            baseline: interval(&spans[..split]),
            current: interval(&spans[split..]),
        });
    }
    Ok(result)
}

fn interval<'a>(spans: &[Span<'a>]) -> IntervalV1<'a> {
    let mut durations: Vec<_> = spans
        .iter()
        .map(|span| {
            (
                span.duration,
                crate::event_rank_v0(span.event),
                span.event.provenance().as_str(),
            )
        })
        .collect();
    durations.sort_unstable();
    let rank = (95 * durations.len()).div_ceil(100).saturating_sub(1);
    let p95 = durations.get(rank);
    let mut evidence = Vec::with_capacity(3);
    for reference in [
        spans.first().map(|s| s.event.provenance().as_str()),
        spans.last().map(|s| s.event.provenance().as_str()),
        p95.map(|s| s.2),
    ]
    .into_iter()
    .flatten()
    {
        if !evidence.contains(&reference) {
            evidence.push(reference);
        }
    }
    let mut transitions = 0;
    let mut first = None;
    let mut last = None;
    for version in spans.iter().filter_map(|span| span.version) {
        if last.is_some_and(|previous| previous != version) {
            transitions += 1;
        }
        first.get_or_insert(version);
        last = Some(version);
    }
    IntervalV1 {
        count: spans.len() as u64,
        errors: spans.iter().filter(|span| span.status == 2).count() as u64,
        unset_status: spans.iter().filter(|span| span.status == 0).count() as u64,
        p95_ns: p95.map(|item| item.0),
        duration_sum_ns: spans.iter().map(|span| u128::from(span.duration)).sum(),
        missing_versions: spans.iter().filter(|span| span.version.is_none()).count() as u64,
        first_version: first,
        last_version: last,
        version_transitions: transitions,
        evidence,
    }
}
