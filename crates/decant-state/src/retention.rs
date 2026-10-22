//! Deterministic event-time retention projection over admitted events.

use std::error::Error;
use std::fmt;

use decant_ir::{EventEnvelopeV0, SourcePositionV0};

use crate::AdmissionStateV0;

pub const EVENT_TIME_RETENTION_VERSION_V0: u16 = 0;

#[derive(Clone, Eq, PartialEq)]
pub enum EventTimeRetentionConfigErrorV0 {
    UnsupportedVersion { actual: u16 },
    ZeroRetentionHorizon,
}

impl fmt::Debug for EventTimeRetentionConfigErrorV0 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::UnsupportedVersion { actual } => formatter
                .debug_struct("UnsupportedVersion")
                .field("actual", actual)
                .finish(),
            Self::ZeroRetentionHorizon => formatter.write_str("ZeroRetentionHorizon"),
        }
    }
}

impl fmt::Display for EventTimeRetentionConfigErrorV0 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(self, formatter)
    }
}

impl Error for EventTimeRetentionConfigErrorV0 {}

#[derive(Clone, Copy, Eq, PartialEq)]
pub struct EventTimeRetentionConfigV0 {
    version: u16,
    retention_horizon_ns: u64,
}

impl EventTimeRetentionConfigV0 {
    pub fn new(
        version: u16,
        retention_horizon_ns: u64,
    ) -> Result<Self, EventTimeRetentionConfigErrorV0> {
        if version != EVENT_TIME_RETENTION_VERSION_V0 {
            return Err(EventTimeRetentionConfigErrorV0::UnsupportedVersion { actual: version });
        }
        if retention_horizon_ns == 0 {
            return Err(EventTimeRetentionConfigErrorV0::ZeroRetentionHorizon);
        }
        Ok(Self {
            version,
            retention_horizon_ns,
        })
    }

    pub const fn version(self) -> u16 {
        self.version
    }

    pub const fn retention_horizon_ns(self) -> u64 {
        self.retention_horizon_ns
    }
}

impl fmt::Debug for EventTimeRetentionConfigV0 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("EventTimeRetentionConfigV0")
            .field("version", &self.version)
            .field("retention_horizon_ns", &self.retention_horizon_ns)
            .finish()
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct EventTimeRetentionUsageV0 {
    accepted_events_considered: usize,
    retained_events: usize,
    expired_events_excluded: usize,
}

impl EventTimeRetentionUsageV0 {
    pub const fn accepted_events_considered(self) -> usize {
        self.accepted_events_considered
    }

    pub const fn retained_events(self) -> usize {
        self.retained_events
    }

    pub const fn expired_events_excluded(self) -> usize {
        self.expired_events_excluded
    }
}

/// A fixed-size retention view that keeps its admission ledger immutable.
///
/// ```compile_fail
/// use decant_ir::EventEnvelopeV0;
/// use decant_state::{AdmissionStateV0, EventTimeRetentionConfigV0};
/// fn cannot_mutate_while_view_is_live(
///     admission: &mut AdmissionStateV0,
///     event: &EventEnvelopeV0,
///     config: EventTimeRetentionConfigV0,
/// ) {
///     let view = admission.project_event_time_retention(config);
///     admission.apply_event(event).unwrap();
///     let _ = view.usage();
/// }
/// ```
///
/// ```compile_fail
/// use decant_ir::SourceIdV0;
/// use decant_state::{AdmissionStateV0, EventTimeRetentionConfigV0};
/// fn cannot_finalize_while_view_is_live(
///     admission: &mut AdmissionStateV0,
///     source_id: &SourceIdV0,
///     finalized_through_ns: i64,
///     config: EventTimeRetentionConfigV0,
/// ) {
///     let view = admission.project_event_time_retention(config);
///     admission.finalize_source(source_id, finalized_through_ns);
///     let _ = view.usage();
/// }
/// ```
pub struct EventTimeRetentionViewV0<'a> {
    admission: &'a AdmissionStateV0,
    config: EventTimeRetentionConfigV0,
    cutoff_event_time_ns: Option<i64>,
    usage: EventTimeRetentionUsageV0,
}

impl AdmissionStateV0 {
    pub fn project_event_time_retention<'a>(
        &'a self,
        config: EventTimeRetentionConfigV0,
    ) -> EventTimeRetentionViewV0<'a> {
        let cutoff_event_time_ns = self.watermark_ns.map(|watermark_ns| {
            cutoff_event_time_ns(
                self.config.epoch_start_ns,
                watermark_ns,
                config.retention_horizon_ns,
            )
        });
        let accepted_events_considered = self.accepted_events.len();
        let retained_events = self
            .accepted_events
            .values()
            .filter(|event| event_is_retained(event, cutoff_event_time_ns))
            .count();
        let expired_events_excluded = accepted_events_considered - retained_events;

        EventTimeRetentionViewV0 {
            admission: self,
            config,
            cutoff_event_time_ns,
            usage: EventTimeRetentionUsageV0 {
                accepted_events_considered,
                retained_events,
                expired_events_excluded,
            },
        }
    }
}

impl<'a> EventTimeRetentionViewV0<'a> {
    pub const fn config(&self) -> EventTimeRetentionConfigV0 {
        self.config
    }

    pub const fn usage(&self) -> EventTimeRetentionUsageV0 {
        self.usage
    }

    pub const fn cutoff_event_time_ns(&self) -> Option<i64> {
        self.cutoff_event_time_ns
    }

    pub const fn source_watermark_ns(&self) -> Option<i64> {
        self.admission.watermark_ns()
    }

    pub const fn is_source_finalized(&self) -> bool {
        self.admission.is_finalized()
    }

    pub fn get(&self, source_position: SourcePositionV0) -> Option<&'a EventEnvelopeV0> {
        self.admission
            .accepted_events
            .get(&source_position)
            .filter(|event| event_is_retained(event, self.cutoff_event_time_ns))
    }

    pub fn events(&self) -> impl Iterator<Item = &'a EventEnvelopeV0> + '_ {
        self.admission
            .accepted_events
            .values()
            .filter(|event| event_is_retained(event, self.cutoff_event_time_ns))
    }
}

impl fmt::Debug for EventTimeRetentionViewV0<'_> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("EventTimeRetentionViewV0")
            .field("config", &self.config)
            .field("usage", &self.usage)
            .field(
                "has_cutoff_event_time",
                &self.cutoff_event_time_ns.is_some(),
            )
            .field(
                "has_source_watermark",
                &self.source_watermark_ns().is_some(),
            )
            .field("source_finalized", &self.is_source_finalized())
            .finish()
    }
}

fn cutoff_event_time_ns(epoch_start_ns: i64, watermark_ns: i64, retention_horizon_ns: u64) -> i64 {
    let unclamped = i128::from(watermark_ns) - i128::from(retention_horizon_ns);
    i64::try_from(unclamped)
        .unwrap_or(i64::MIN)
        .max(epoch_start_ns)
}

fn event_is_retained(event: &EventEnvelopeV0, cutoff_event_time_ns: Option<i64>) -> bool {
    cutoff_event_time_ns.is_none_or(|cutoff| event.event_time_ns() >= cutoff)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cutoff_clamps_signed_underflow_to_epoch_start() {
        assert_eq!(
            cutoff_event_time_ns(i64::MIN + 7, i64::MIN, u64::MAX),
            i64::MIN + 7
        );
    }
}
