//! Retention-aware changed-by-schema-and-key composition.

use std::error::Error;
use std::fmt;

use decant_ir::SchemaIdV0;

use crate::changed::materialize_changed_by_key_from_events;
use crate::{
    AdmissionStateV0, ChangeObservationV0, ChangedByKeyConfigV0, ChangedByKeyUsageV0,
    ChangedByKeyViewV0, ChangedMaterializationErrorV0, EventTimeRetentionConfigV0,
    EventTimeRetentionUsageV0,
};

pub const RETAINED_CHANGED_BY_KEY_VERSION_V0: u16 = 0;

#[derive(Clone, Eq, PartialEq)]
pub enum RetainedChangedByKeyConfigErrorV0 {
    UnsupportedVersion { actual: u16 },
}

impl fmt::Debug for RetainedChangedByKeyConfigErrorV0 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::UnsupportedVersion { actual } => formatter
                .debug_struct("UnsupportedVersion")
                .field("actual", actual)
                .finish(),
        }
    }
}

impl fmt::Display for RetainedChangedByKeyConfigErrorV0 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(self, formatter)
    }
}

impl Error for RetainedChangedByKeyConfigErrorV0 {}

#[derive(Clone, Copy, Eq, PartialEq)]
pub struct RetainedChangedByKeyConfigV0 {
    version: u16,
    retention: EventTimeRetentionConfigV0,
    changed: ChangedByKeyConfigV0,
}

impl RetainedChangedByKeyConfigV0 {
    pub fn new(
        version: u16,
        retention: EventTimeRetentionConfigV0,
        changed: ChangedByKeyConfigV0,
    ) -> Result<Self, RetainedChangedByKeyConfigErrorV0> {
        if version != RETAINED_CHANGED_BY_KEY_VERSION_V0 {
            return Err(RetainedChangedByKeyConfigErrorV0::UnsupportedVersion { actual: version });
        }
        Ok(Self {
            version,
            retention,
            changed,
        })
    }

    pub const fn version(self) -> u16 {
        self.version
    }

    pub const fn retention(self) -> EventTimeRetentionConfigV0 {
        self.retention
    }

    pub const fn changed(self) -> ChangedByKeyConfigV0 {
        self.changed
    }
}

impl fmt::Debug for RetainedChangedByKeyConfigV0 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RetainedChangedByKeyConfigV0")
            .field("version", &self.version)
            .field("retention", &self.retention)
            .field("changed", &self.changed)
            .finish()
    }
}

/// A bounded changed view reconstructed only from retained admitted events.
///
/// ```compile_fail
/// use decant_ir::EventEnvelopeV0;
/// use decant_state::{AdmissionStateV0, RetainedChangedByKeyConfigV0};
/// fn cannot_mutate_while_view_is_live(
///     admission: &mut AdmissionStateV0,
///     event: &EventEnvelopeV0,
///     config: RetainedChangedByKeyConfigV0,
/// ) {
///     let view = admission.materialize_retained_changed_by_key(config).unwrap();
///     admission.apply_event(event).unwrap();
///     let _ = view.changed_usage();
/// }
/// ```
///
/// ```compile_fail
/// use decant_ir::SourceIdV0;
/// use decant_state::{AdmissionStateV0, RetainedChangedByKeyConfigV0};
/// fn cannot_finalize_while_view_is_live(
///     admission: &mut AdmissionStateV0,
///     source_id: &SourceIdV0,
///     finalized_through_ns: i64,
///     config: RetainedChangedByKeyConfigV0,
/// ) {
///     let view = admission.materialize_retained_changed_by_key(config).unwrap();
///     admission.finalize_source(source_id, finalized_through_ns);
///     let _ = view.changed_usage();
/// }
/// ```
pub struct RetainedChangedByKeyViewV0<'a> {
    config: RetainedChangedByKeyConfigV0,
    retention_cutoff_event_time_ns: Option<i64>,
    retention_usage: EventTimeRetentionUsageV0,
    changed: ChangedByKeyViewV0<'a>,
}

impl AdmissionStateV0 {
    pub fn materialize_retained_changed_by_key<'a>(
        &'a self,
        config: RetainedChangedByKeyConfigV0,
    ) -> Result<RetainedChangedByKeyViewV0<'a>, ChangedMaterializationErrorV0> {
        let retention = self.project_event_time_retention(config.retention);
        let retention_cutoff_event_time_ns = retention.cutoff_event_time_ns();
        let retention_usage = retention.usage();
        let changed =
            materialize_changed_by_key_from_events(self, config.changed, retention.events())?;
        if changed.usage().accepted_events_considered() != retention_usage.retained_events() {
            return Err(ChangedMaterializationErrorV0::IndexInvariant);
        }

        Ok(RetainedChangedByKeyViewV0 {
            config,
            retention_cutoff_event_time_ns,
            retention_usage,
            changed,
        })
    }
}

impl<'a> RetainedChangedByKeyViewV0<'a> {
    pub const fn config(&self) -> RetainedChangedByKeyConfigV0 {
        self.config
    }

    pub const fn retention_cutoff_event_time_ns(&self) -> Option<i64> {
        self.retention_cutoff_event_time_ns
    }

    pub const fn retention_usage(&self) -> EventTimeRetentionUsageV0 {
        self.retention_usage
    }

    pub fn changed_usage(&self) -> ChangedByKeyUsageV0 {
        self.changed.usage()
    }

    pub fn is_tracked(&self, schema_id: &SchemaIdV0, key: &[u8]) -> bool {
        self.changed.is_tracked(schema_id, key)
    }

    pub fn get(&self, schema_id: &SchemaIdV0, key: &[u8]) -> Option<ChangeObservationV0<'a>> {
        self.changed.get(schema_id, key)
    }

    pub fn entries(
        &self,
    ) -> impl Iterator<Item = (&SchemaIdV0, &[u8], ChangeObservationV0<'a>)> + '_ {
        self.changed.entries()
    }

    pub const fn source_watermark_ns(&self) -> Option<i64> {
        self.changed.source_watermark_ns()
    }

    pub const fn is_source_finalized(&self) -> bool {
        self.changed.is_source_finalized()
    }
}

impl fmt::Debug for RetainedChangedByKeyViewV0<'_> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RetainedChangedByKeyViewV0")
            .field("config", &self.config)
            .field("retention_usage", &self.retention_usage)
            .field("changed_usage", &self.changed_usage())
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
