//! Retention-aware latest-by-schema-and-key composition.

use std::error::Error;
use std::fmt;

use decant_ir::{EventEnvelopeV0, SchemaIdV0};

use crate::latest::materialize_latest_by_key_from_events;
use crate::{
    AdmissionStateV0, EventTimeRetentionConfigV0, EventTimeRetentionUsageV0, LatestByKeyConfigV0,
    LatestByKeyUsageV0, LatestByKeyViewV0, LatestMaterializationErrorV0,
};

pub const RETAINED_LATEST_BY_KEY_VERSION_V0: u16 = 0;

#[derive(Clone, Eq, PartialEq)]
pub enum RetainedLatestByKeyConfigErrorV0 {
    UnsupportedVersion { actual: u16 },
}

impl fmt::Debug for RetainedLatestByKeyConfigErrorV0 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::UnsupportedVersion { actual } => formatter
                .debug_struct("UnsupportedVersion")
                .field("actual", actual)
                .finish(),
        }
    }
}

impl fmt::Display for RetainedLatestByKeyConfigErrorV0 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(self, formatter)
    }
}

impl Error for RetainedLatestByKeyConfigErrorV0 {}

#[derive(Clone, Copy, Eq, PartialEq)]
pub struct RetainedLatestByKeyConfigV0 {
    version: u16,
    retention: EventTimeRetentionConfigV0,
    latest: LatestByKeyConfigV0,
}

impl RetainedLatestByKeyConfigV0 {
    pub fn new(
        version: u16,
        retention: EventTimeRetentionConfigV0,
        latest: LatestByKeyConfigV0,
    ) -> Result<Self, RetainedLatestByKeyConfigErrorV0> {
        if version != RETAINED_LATEST_BY_KEY_VERSION_V0 {
            return Err(RetainedLatestByKeyConfigErrorV0::UnsupportedVersion { actual: version });
        }
        Ok(Self {
            version,
            retention,
            latest,
        })
    }

    pub const fn version(self) -> u16 {
        self.version
    }

    pub const fn retention(self) -> EventTimeRetentionConfigV0 {
        self.retention
    }

    pub const fn latest(self) -> LatestByKeyConfigV0 {
        self.latest
    }
}

impl fmt::Debug for RetainedLatestByKeyConfigV0 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RetainedLatestByKeyConfigV0")
            .field("version", &self.version)
            .field("retention", &self.retention)
            .field("latest", &self.latest)
            .finish()
    }
}

/// A bounded latest view reconstructed only from retained admitted events.
///
/// ```compile_fail
/// use decant_ir::EventEnvelopeV0;
/// use decant_state::{AdmissionStateV0, RetainedLatestByKeyConfigV0};
/// fn cannot_mutate_while_view_is_live(
///     admission: &mut AdmissionStateV0,
///     event: &EventEnvelopeV0,
///     config: RetainedLatestByKeyConfigV0,
/// ) {
///     let view = admission.materialize_retained_latest_by_key(config).unwrap();
///     admission.apply_event(event).unwrap();
///     let _ = view.latest_usage();
/// }
/// ```
///
/// ```compile_fail
/// use decant_ir::SourceIdV0;
/// use decant_state::{AdmissionStateV0, RetainedLatestByKeyConfigV0};
/// fn cannot_finalize_while_view_is_live(
///     admission: &mut AdmissionStateV0,
///     source_id: &SourceIdV0,
///     finalized_through_ns: i64,
///     config: RetainedLatestByKeyConfigV0,
/// ) {
///     let view = admission.materialize_retained_latest_by_key(config).unwrap();
///     admission.finalize_source(source_id, finalized_through_ns);
///     let _ = view.latest_usage();
/// }
/// ```
pub struct RetainedLatestByKeyViewV0<'a> {
    config: RetainedLatestByKeyConfigV0,
    retention_cutoff_event_time_ns: Option<i64>,
    retention_usage: EventTimeRetentionUsageV0,
    latest: LatestByKeyViewV0<'a>,
}

impl AdmissionStateV0 {
    pub fn materialize_retained_latest_by_key<'a>(
        &'a self,
        config: RetainedLatestByKeyConfigV0,
    ) -> Result<RetainedLatestByKeyViewV0<'a>, LatestMaterializationErrorV0> {
        let retention = self.project_event_time_retention(config.retention);
        let retention_cutoff_event_time_ns = retention.cutoff_event_time_ns();
        let retention_usage = retention.usage();
        let latest =
            materialize_latest_by_key_from_events(self, config.latest, retention.events())?;
        if latest.usage().accepted_events_considered() != retention_usage.retained_events() {
            return Err(LatestMaterializationErrorV0::IndexInvariant);
        }

        Ok(RetainedLatestByKeyViewV0 {
            config,
            retention_cutoff_event_time_ns,
            retention_usage,
            latest,
        })
    }
}

impl<'a> RetainedLatestByKeyViewV0<'a> {
    pub const fn config(&self) -> RetainedLatestByKeyConfigV0 {
        self.config
    }

    pub const fn retention_cutoff_event_time_ns(&self) -> Option<i64> {
        self.retention_cutoff_event_time_ns
    }

    pub const fn retention_usage(&self) -> EventTimeRetentionUsageV0 {
        self.retention_usage
    }

    pub fn latest_usage(&self) -> LatestByKeyUsageV0 {
        self.latest.usage()
    }

    pub fn get(&self, schema_id: &SchemaIdV0, key: &[u8]) -> Option<&EventEnvelopeV0> {
        self.latest.get(schema_id, key)
    }

    pub fn entries(&self) -> impl Iterator<Item = (&SchemaIdV0, &[u8], &EventEnvelopeV0)> + '_ {
        self.latest.entries()
    }

    pub const fn source_watermark_ns(&self) -> Option<i64> {
        self.latest.source_watermark_ns()
    }

    pub const fn is_source_finalized(&self) -> bool {
        self.latest.is_source_finalized()
    }
}

impl fmt::Debug for RetainedLatestByKeyViewV0<'_> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RetainedLatestByKeyViewV0")
            .field("config", &self.config)
            .field("retention_usage", &self.retention_usage)
            .field("latest_usage", &self.latest_usage())
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
