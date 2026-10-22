//! Bounded retention-aware selection of one temporally preceding event.

use std::error::Error;
use std::fmt;

use decant_ir::{EventEnvelopeV0, MAX_KEY_BYTES, SchemaIdV0};

use crate::{
    AdmissionStateV0, EventTimeRetentionConfigV0, EventTimeRetentionUsageV0, event_rank_v0,
};

pub const PRECEDING_EVENT_SELECTOR_VERSION_V0: u16 = 0;

#[derive(Clone, Eq, PartialEq)]
pub enum PrecedingEventSelectorConfigErrorV0 {
    UnsupportedVersion { actual: u16 },
    KeyTooLong { maximum: usize, actual: usize },
    ZeroLookback,
}

impl fmt::Debug for PrecedingEventSelectorConfigErrorV0 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::UnsupportedVersion { actual } => formatter
                .debug_struct("UnsupportedVersion")
                .field("actual", actual)
                .finish(),
            Self::KeyTooLong { maximum, actual } => formatter
                .debug_struct("KeyTooLong")
                .field("maximum", maximum)
                .field("actual", actual)
                .finish(),
            Self::ZeroLookback => formatter.write_str("ZeroLookback"),
        }
    }
}

impl fmt::Display for PrecedingEventSelectorConfigErrorV0 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(self, formatter)
    }
}

impl Error for PrecedingEventSelectorConfigErrorV0 {}

#[derive(Clone, Eq, PartialEq)]
pub struct PrecedingEventSelectorConfigV0 {
    version: u16,
    retention: EventTimeRetentionConfigV0,
    candidate_schema_id: SchemaIdV0,
    key: Vec<u8>,
    anchor_event_time_ns: i64,
    lookback_ns: u64,
}

impl PrecedingEventSelectorConfigV0 {
    pub fn new(
        version: u16,
        retention: EventTimeRetentionConfigV0,
        candidate_schema_id: SchemaIdV0,
        key: &[u8],
        anchor_event_time_ns: i64,
        lookback_ns: u64,
    ) -> Result<Self, PrecedingEventSelectorConfigErrorV0> {
        if version != PRECEDING_EVENT_SELECTOR_VERSION_V0 {
            return Err(PrecedingEventSelectorConfigErrorV0::UnsupportedVersion {
                actual: version,
            });
        }
        if key.len() > MAX_KEY_BYTES {
            return Err(PrecedingEventSelectorConfigErrorV0::KeyTooLong {
                maximum: MAX_KEY_BYTES,
                actual: key.len(),
            });
        }
        if lookback_ns == 0 {
            return Err(PrecedingEventSelectorConfigErrorV0::ZeroLookback);
        }
        Ok(Self {
            version,
            retention,
            candidate_schema_id,
            key: key.to_vec(),
            anchor_event_time_ns,
            lookback_ns,
        })
    }

    pub const fn version(&self) -> u16 {
        self.version
    }

    pub const fn retention(&self) -> EventTimeRetentionConfigV0 {
        self.retention
    }

    pub const fn candidate_schema_id(&self) -> &SchemaIdV0 {
        &self.candidate_schema_id
    }

    pub fn key(&self) -> &[u8] {
        &self.key
    }

    pub const fn anchor_event_time_ns(&self) -> i64 {
        self.anchor_event_time_ns
    }

    pub const fn lookback_ns(&self) -> u64 {
        self.lookback_ns
    }
}

impl fmt::Debug for PrecedingEventSelectorConfigV0 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("PrecedingEventSelectorConfigV0")
            .field("version", &self.version)
            .field("retention", &self.retention)
            .field("candidate_schema_id", &"<redacted>")
            .field("key_byte_len", &self.key.len())
            .field("anchor_event_time_ns", &"<redacted>")
            .field("lookback_ns", &self.lookback_ns)
            .finish()
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PrecedingEventSelectorAccountingComponentV0 {
    RequestedIntervalStart,
    RetainedEventsConsidered,
    OtherSchemaEvents,
    MatchingSchemaEvents,
    UnkeyedMatchingEvents,
    DifferentKeyMatchingEvents,
    SameKeyEvents,
    BeforeRequestedIntervalEvents,
    AtOrAfterAnchorEvents,
    EligibleCandidateEvents,
}

#[derive(Clone, Eq, PartialEq)]
pub enum PrecedingEventSelectorErrorV0 {
    AnchorOutsideEpoch,
    RequestedEvidenceExpired,
    ArithmeticOverflow {
        component: PrecedingEventSelectorAccountingComponentV0,
    },
    UsageInvariant,
}

impl fmt::Debug for PrecedingEventSelectorErrorV0 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::AnchorOutsideEpoch => formatter.write_str("AnchorOutsideEpoch"),
            Self::RequestedEvidenceExpired => formatter.write_str("RequestedEvidenceExpired"),
            Self::ArithmeticOverflow { component } => formatter
                .debug_struct("ArithmeticOverflow")
                .field("component", component)
                .finish(),
            Self::UsageInvariant => formatter.write_str("UsageInvariant"),
        }
    }
}

impl fmt::Display for PrecedingEventSelectorErrorV0 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(self, formatter)
    }
}

impl Error for PrecedingEventSelectorErrorV0 {}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct PrecedingEventSelectorUsageV0 {
    retained_events_considered: usize,
    other_schema_events_omitted: usize,
    matching_schema_events: usize,
    unkeyed_matching_events_omitted: usize,
    different_key_matching_events_omitted: usize,
    same_key_events: usize,
    before_requested_interval_events_omitted: usize,
    at_or_after_anchor_events_omitted: usize,
    eligible_candidate_events: usize,
}

impl PrecedingEventSelectorUsageV0 {
    pub const fn retained_events_considered(self) -> usize {
        self.retained_events_considered
    }

    pub const fn other_schema_events_omitted(self) -> usize {
        self.other_schema_events_omitted
    }

    pub const fn matching_schema_events(self) -> usize {
        self.matching_schema_events
    }

    pub const fn unkeyed_matching_events_omitted(self) -> usize {
        self.unkeyed_matching_events_omitted
    }

    pub const fn different_key_matching_events_omitted(self) -> usize {
        self.different_key_matching_events_omitted
    }

    pub const fn same_key_events(self) -> usize {
        self.same_key_events
    }

    pub const fn before_requested_interval_events_omitted(self) -> usize {
        self.before_requested_interval_events_omitted
    }

    pub const fn at_or_after_anchor_events_omitted(self) -> usize {
        self.at_or_after_anchor_events_omitted
    }

    pub const fn eligible_candidate_events(self) -> usize {
        self.eligible_candidate_events
    }
}

/// A fixed-result selector that keeps admission immutable.
///
/// ```compile_fail
/// use decant_ir::EventEnvelopeV0;
/// use decant_state::{AdmissionStateV0, PrecedingEventSelectorConfigV0};
/// fn cannot_mutate_while_view_is_live(
///     admission: &mut AdmissionStateV0,
///     event: &EventEnvelopeV0,
///     config: PrecedingEventSelectorConfigV0,
/// ) {
///     let view = admission.select_preceding_event(config).unwrap();
///     admission.apply_event(event).unwrap();
///     let _ = view.usage();
/// }
/// ```
///
/// ```compile_fail
/// use decant_ir::SourceIdV0;
/// use decant_state::{AdmissionStateV0, PrecedingEventSelectorConfigV0};
/// fn cannot_finalize_while_view_is_live(
///     admission: &mut AdmissionStateV0,
///     source_id: &SourceIdV0,
///     finalized_through_ns: i64,
///     config: PrecedingEventSelectorConfigV0,
/// ) {
///     let view = admission.select_preceding_event(config).unwrap();
///     admission.finalize_source(source_id, finalized_through_ns);
///     let _ = view.usage();
/// }
/// ```
pub struct PrecedingEventSelectorViewV0<'a> {
    admission: &'a AdmissionStateV0,
    config: PrecedingEventSelectorConfigV0,
    requested_start_event_time_ns: i64,
    retention_cutoff_event_time_ns: Option<i64>,
    retention_usage: EventTimeRetentionUsageV0,
    usage: PrecedingEventSelectorUsageV0,
    candidate: Option<&'a EventEnvelopeV0>,
}

impl AdmissionStateV0 {
    pub fn select_preceding_event(
        &self,
        config: PrecedingEventSelectorConfigV0,
    ) -> Result<PrecedingEventSelectorViewV0<'_>, PrecedingEventSelectorErrorV0> {
        let epoch_start_ns = self.config().epoch_start_ns();
        let epoch_end_ns = self.config().epoch_end_ns();
        if config.anchor_event_time_ns < epoch_start_ns
            || config.anchor_event_time_ns > epoch_end_ns
        {
            return Err(PrecedingEventSelectorErrorV0::AnchorOutsideEpoch);
        }

        let requested_start_event_time_ns = requested_start_event_time_ns(
            epoch_start_ns,
            config.anchor_event_time_ns,
            config.lookback_ns,
        )?;
        let retention = self.project_event_time_retention(config.retention);
        let retention_cutoff_event_time_ns = retention.cutoff_event_time_ns();
        if retention_cutoff_event_time_ns
            .is_some_and(|cutoff| cutoff > requested_start_event_time_ns)
        {
            return Err(PrecedingEventSelectorErrorV0::RequestedEvidenceExpired);
        }
        let retention_usage = retention.usage();
        let mut usage = PrecedingEventSelectorUsageV0 {
            retained_events_considered: 0,
            other_schema_events_omitted: 0,
            matching_schema_events: 0,
            unkeyed_matching_events_omitted: 0,
            different_key_matching_events_omitted: 0,
            same_key_events: 0,
            before_requested_interval_events_omitted: 0,
            at_or_after_anchor_events_omitted: 0,
            eligible_candidate_events: 0,
        };
        let mut candidate = None;

        for event in retention.events() {
            checked_increment(
                &mut usage.retained_events_considered,
                PrecedingEventSelectorAccountingComponentV0::RetainedEventsConsidered,
            )?;
            if event.schema_id() != &config.candidate_schema_id {
                checked_increment(
                    &mut usage.other_schema_events_omitted,
                    PrecedingEventSelectorAccountingComponentV0::OtherSchemaEvents,
                )?;
                continue;
            }
            checked_increment(
                &mut usage.matching_schema_events,
                PrecedingEventSelectorAccountingComponentV0::MatchingSchemaEvents,
            )?;
            let Some(key) = event.key() else {
                checked_increment(
                    &mut usage.unkeyed_matching_events_omitted,
                    PrecedingEventSelectorAccountingComponentV0::UnkeyedMatchingEvents,
                )?;
                continue;
            };
            if key != config.key.as_slice() {
                checked_increment(
                    &mut usage.different_key_matching_events_omitted,
                    PrecedingEventSelectorAccountingComponentV0::DifferentKeyMatchingEvents,
                )?;
                continue;
            }
            checked_increment(
                &mut usage.same_key_events,
                PrecedingEventSelectorAccountingComponentV0::SameKeyEvents,
            )?;
            if event.event_time_ns() < requested_start_event_time_ns {
                checked_increment(
                    &mut usage.before_requested_interval_events_omitted,
                    PrecedingEventSelectorAccountingComponentV0::BeforeRequestedIntervalEvents,
                )?;
                continue;
            }
            if event.event_time_ns() >= config.anchor_event_time_ns {
                checked_increment(
                    &mut usage.at_or_after_anchor_events_omitted,
                    PrecedingEventSelectorAccountingComponentV0::AtOrAfterAnchorEvents,
                )?;
                continue;
            }
            checked_increment(
                &mut usage.eligible_candidate_events,
                PrecedingEventSelectorAccountingComponentV0::EligibleCandidateEvents,
            )?;
            if candidate.is_none_or(|current| event_rank_v0(event) > event_rank_v0(current)) {
                candidate = Some(event);
            }
        }

        verify_usage(retention_usage, usage)?;
        Ok(PrecedingEventSelectorViewV0 {
            admission: self,
            config,
            requested_start_event_time_ns,
            retention_cutoff_event_time_ns,
            retention_usage,
            usage,
            candidate,
        })
    }
}

impl<'a> PrecedingEventSelectorViewV0<'a> {
    pub const fn config(&self) -> &PrecedingEventSelectorConfigV0 {
        &self.config
    }

    pub const fn requested_start_event_time_ns(&self) -> i64 {
        self.requested_start_event_time_ns
    }

    pub const fn anchor_event_time_ns(&self) -> i64 {
        self.config.anchor_event_time_ns
    }

    pub const fn retention_cutoff_event_time_ns(&self) -> Option<i64> {
        self.retention_cutoff_event_time_ns
    }

    pub const fn retention_usage(&self) -> EventTimeRetentionUsageV0 {
        self.retention_usage
    }

    pub const fn usage(&self) -> PrecedingEventSelectorUsageV0 {
        self.usage
    }

    pub const fn candidate(&self) -> Option<&'a EventEnvelopeV0> {
        self.candidate
    }

    pub const fn source_watermark_ns(&self) -> Option<i64> {
        self.admission.watermark_ns()
    }

    pub const fn is_source_finalized(&self) -> bool {
        self.admission.is_finalized()
    }
}

impl fmt::Debug for PrecedingEventSelectorViewV0<'_> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("PrecedingEventSelectorViewV0")
            .field("config", &self.config)
            .field("retention_usage", &self.retention_usage)
            .field("usage", &self.usage)
            .field("has_requested_start_event_time", &true)
            .field(
                "has_retention_cutoff_event_time",
                &self.retention_cutoff_event_time_ns.is_some(),
            )
            .field("has_candidate", &self.candidate.is_some())
            .field(
                "has_source_watermark",
                &self.source_watermark_ns().is_some(),
            )
            .field("source_finalized", &self.is_source_finalized())
            .finish()
    }
}

fn requested_start_event_time_ns(
    epoch_start_ns: i64,
    anchor_event_time_ns: i64,
    lookback_ns: u64,
) -> Result<i64, PrecedingEventSelectorErrorV0> {
    let requested = i128::from(anchor_event_time_ns) - i128::from(lookback_ns);
    i64::try_from(requested.max(i128::from(epoch_start_ns))).map_err(|_| {
        PrecedingEventSelectorErrorV0::ArithmeticOverflow {
            component: PrecedingEventSelectorAccountingComponentV0::RequestedIntervalStart,
        }
    })
}

fn checked_increment(
    current: &mut usize,
    component: PrecedingEventSelectorAccountingComponentV0,
) -> Result<(), PrecedingEventSelectorErrorV0> {
    *current = current
        .checked_add(1)
        .ok_or(PrecedingEventSelectorErrorV0::ArithmeticOverflow { component })?;
    Ok(())
}

fn verify_usage(
    retention_usage: EventTimeRetentionUsageV0,
    usage: PrecedingEventSelectorUsageV0,
) -> Result<(), PrecedingEventSelectorErrorV0> {
    let schema_classified = usage
        .other_schema_events_omitted
        .checked_add(usage.matching_schema_events)
        .ok_or(PrecedingEventSelectorErrorV0::UsageInvariant)?;
    let matching_classified = usage
        .unkeyed_matching_events_omitted
        .checked_add(usage.different_key_matching_events_omitted)
        .and_then(|count| count.checked_add(usage.same_key_events))
        .ok_or(PrecedingEventSelectorErrorV0::UsageInvariant)?;
    let same_key_classified = usage
        .before_requested_interval_events_omitted
        .checked_add(usage.at_or_after_anchor_events_omitted)
        .and_then(|count| count.checked_add(usage.eligible_candidate_events))
        .ok_or(PrecedingEventSelectorErrorV0::UsageInvariant)?;
    if usage.retained_events_considered != retention_usage.retained_events()
        || usage.retained_events_considered != schema_classified
        || usage.matching_schema_events != matching_classified
        || usage.same_key_events != same_key_classified
    {
        return Err(PrecedingEventSelectorErrorV0::UsageInvariant);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn checked_increment_overflow_is_typed_and_non_mutating() {
        let mut current = usize::MAX;
        assert_eq!(
            checked_increment(
                &mut current,
                PrecedingEventSelectorAccountingComponentV0::EligibleCandidateEvents,
            ),
            Err(PrecedingEventSelectorErrorV0::ArithmeticOverflow {
                component: PrecedingEventSelectorAccountingComponentV0::EligibleCandidateEvents,
            })
        );
        assert_eq!(current, usize::MAX);
    }
}
