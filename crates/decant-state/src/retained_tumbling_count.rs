//! Retention-aware tumbling count-window composition.

use std::error::Error;
use std::fmt;

use decant_ir::{EventEnvelopeV0, SchemaIdV0, SourcePositionV0};

use crate::tumbling_count::materialize_tumbling_count_from_events;
use crate::{
    AdmissionStateV0, CountMaterializationErrorV0, CountWindowObservationV0,
    EventTimeRetentionConfigV0, EventTimeRetentionUsageV0, TumblingCountConfigV0,
    TumblingCountUsageV0, TumblingCountViewV0,
};

pub const RETAINED_TUMBLING_COUNT_VERSION_V0: u16 = 0;

#[derive(Clone, Eq, PartialEq)]
pub enum RetainedTumblingCountConfigErrorV0 {
    UnsupportedVersion { actual: u16 },
}

impl fmt::Debug for RetainedTumblingCountConfigErrorV0 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::UnsupportedVersion { actual } => formatter
                .debug_struct("UnsupportedVersion")
                .field("actual", actual)
                .finish(),
        }
    }
}

impl fmt::Display for RetainedTumblingCountConfigErrorV0 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(self, formatter)
    }
}

impl Error for RetainedTumblingCountConfigErrorV0 {}

#[derive(Clone, Copy, Eq, PartialEq)]
pub struct RetainedTumblingCountConfigV0 {
    version: u16,
    retention: EventTimeRetentionConfigV0,
    count: TumblingCountConfigV0,
}

impl RetainedTumblingCountConfigV0 {
    pub fn new(
        version: u16,
        retention: EventTimeRetentionConfigV0,
        count: TumblingCountConfigV0,
    ) -> Result<Self, RetainedTumblingCountConfigErrorV0> {
        if version != RETAINED_TUMBLING_COUNT_VERSION_V0 {
            return Err(RetainedTumblingCountConfigErrorV0::UnsupportedVersion { actual: version });
        }
        Ok(Self {
            version,
            retention,
            count,
        })
    }

    pub const fn version(self) -> u16 {
        self.version
    }

    pub const fn retention(self) -> EventTimeRetentionConfigV0 {
        self.retention
    }

    pub const fn count(self) -> TumblingCountConfigV0 {
        self.count
    }
}

impl fmt::Debug for RetainedTumblingCountConfigV0 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RetainedTumblingCountConfigV0")
            .field("version", &self.version)
            .field("retention", &self.retention)
            .field("count", &self.count)
            .finish()
    }
}

#[derive(Clone, Copy, Eq, PartialEq)]
pub struct RetainedCountWindowObservationV0 {
    count_window: CountWindowObservationV0,
    retained_interval_start_ns: i64,
    cutoff_partial: bool,
}

impl RetainedCountWindowObservationV0 {
    pub const fn window_ordinal(self) -> u64 {
        self.count_window.window_ordinal()
    }

    pub const fn window_start_ns(self) -> i64 {
        self.count_window.window_start_ns()
    }

    pub const fn window_end_ns(self) -> i64 {
        self.count_window.window_end_ns()
    }

    pub const fn retained_interval_start_ns(self) -> i64 {
        self.retained_interval_start_ns
    }

    pub const fn count(self) -> u64 {
        self.count_window.count()
    }

    pub const fn is_cutoff_partial(self) -> bool {
        self.cutoff_partial
    }

    pub const fn is_finalized(self) -> bool {
        self.count_window.is_finalized()
    }
}

impl fmt::Debug for RetainedCountWindowObservationV0 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RetainedCountWindowObservationV0")
            .field("window_ordinal", &"<redacted>")
            .field("window_start_ns", &"<redacted>")
            .field("window_end_ns", &"<redacted>")
            .field("retained_interval_start_ns", &"<redacted>")
            .field("count", &self.count())
            .field("cutoff_partial", &self.cutoff_partial)
            .field("finalized", &self.is_finalized())
            .finish()
    }
}

/// A bounded retained count view reconstructed from admitted canonical events.
///
/// ```compile_fail
/// use decant_ir::EventEnvelopeV0;
/// use decant_state::{AdmissionStateV0, RetainedTumblingCountConfigV0};
/// fn cannot_mutate_while_view_is_live(
///     admission: &mut AdmissionStateV0,
///     event: &EventEnvelopeV0,
///     config: RetainedTumblingCountConfigV0,
/// ) {
///     let view = admission.materialize_retained_tumbling_count(config).unwrap();
///     admission.apply_event(event).unwrap();
///     let _ = view.count_usage();
/// }
/// ```
///
/// ```compile_fail
/// use decant_ir::SourceIdV0;
/// use decant_state::{AdmissionStateV0, RetainedTumblingCountConfigV0};
/// fn cannot_finalize_while_view_is_live(
///     admission: &mut AdmissionStateV0,
///     source_id: &SourceIdV0,
///     finalized_through_ns: i64,
///     config: RetainedTumblingCountConfigV0,
/// ) {
///     let view = admission.materialize_retained_tumbling_count(config).unwrap();
///     admission.finalize_source(source_id, finalized_through_ns);
///     let _ = view.count_usage();
/// }
/// ```
pub struct RetainedTumblingCountViewV0<'a> {
    config: RetainedTumblingCountConfigV0,
    retention_cutoff_event_time_ns: Option<i64>,
    retention_usage: EventTimeRetentionUsageV0,
    count: TumblingCountViewV0<'a>,
}

impl AdmissionStateV0 {
    pub fn materialize_retained_tumbling_count<'a>(
        &'a self,
        config: RetainedTumblingCountConfigV0,
    ) -> Result<RetainedTumblingCountViewV0<'a>, CountMaterializationErrorV0> {
        let retention = self.project_event_time_retention(config.retention);
        let retention_cutoff_event_time_ns = retention.cutoff_event_time_ns();
        let retention_usage = retention.usage();
        let count = materialize_tumbling_count_from_events(self, config.count, retention.events())?;
        if count.usage().accepted_events_considered() != retention_usage.retained_events() {
            return Err(CountMaterializationErrorV0::IndexInvariant);
        }

        Ok(RetainedTumblingCountViewV0 {
            config,
            retention_cutoff_event_time_ns,
            retention_usage,
            count,
        })
    }
}

impl<'a> RetainedTumblingCountViewV0<'a> {
    pub const fn config(&self) -> RetainedTumblingCountConfigV0 {
        self.config
    }

    pub const fn retention_cutoff_event_time_ns(&self) -> Option<i64> {
        self.retention_cutoff_event_time_ns
    }

    pub const fn retention_usage(&self) -> EventTimeRetentionUsageV0 {
        self.retention_usage
    }

    pub fn count_usage(&self) -> TumblingCountUsageV0 {
        self.count.usage()
    }

    pub fn get(
        &self,
        schema_id: &SchemaIdV0,
        key: &[u8],
        window_ordinal: u64,
    ) -> Option<RetainedCountWindowObservationV0> {
        self.count
            .get(schema_id, key, window_ordinal)
            .map(|observation| self.retain_observation(observation))
    }

    pub fn entries(
        &self,
    ) -> impl Iterator<Item = (&SchemaIdV0, &[u8], RetainedCountWindowObservationV0)> + '_ {
        self.count
            .entries()
            .map(move |(schema_id, key, observation)| {
                (schema_id, key, self.retain_observation(observation))
            })
    }

    pub fn contributors(
        &self,
        schema_id: &SchemaIdV0,
        key: &[u8],
        window_ordinal: u64,
    ) -> Option<impl ExactSizeIterator<Item = (SourcePositionV0, &'a EventEnvelopeV0)> + '_> {
        self.count.contributors(schema_id, key, window_ordinal)
    }

    pub const fn source_watermark_ns(&self) -> Option<i64> {
        self.count.source_watermark_ns()
    }

    pub const fn is_source_finalized(&self) -> bool {
        self.count.is_source_finalized()
    }

    fn retain_observation(
        &self,
        count_window: CountWindowObservationV0,
    ) -> RetainedCountWindowObservationV0 {
        let window_start_ns = count_window.window_start_ns();
        let window_end_ns = count_window.window_end_ns();
        let retained_interval_start_ns = self
            .retention_cutoff_event_time_ns
            .map_or(window_start_ns, |cutoff_ns| cutoff_ns.max(window_start_ns));
        let cutoff_partial = self
            .retention_cutoff_event_time_ns
            .is_some_and(|cutoff_ns| window_start_ns < cutoff_ns && cutoff_ns < window_end_ns);

        RetainedCountWindowObservationV0 {
            count_window,
            retained_interval_start_ns,
            cutoff_partial,
        }
    }
}

impl fmt::Debug for RetainedTumblingCountViewV0<'_> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RetainedTumblingCountViewV0")
            .field("config", &self.config)
            .field("retention_usage", &self.retention_usage)
            .field("count_usage", &self.count_usage())
            .field(
                "has_retention_cutoff_event_time",
                &self.retention_cutoff_event_time_ns.is_some(),
            )
            .field(
                "has_source_watermark",
                &self.source_watermark_ns().is_some(),
            )
            .field("source_finalized", &self.is_source_finalized())
            .finish()
    }
}
