//! Retention-aware projected scalar-change materialization.

use std::collections::BTreeMap;
use std::error::Error;
use std::fmt;

use decant_ir::{EventEnvelopeV0, MAX_FIELD_NAME_BYTES, ScalarKindV0, ScalarValueV0, SchemaIdV0};

use crate::{
    AdmissionStateV0, EventTimeRetentionConfigV0, EventTimeRetentionUsageV0, event_rank_v0,
};

pub const PROJECTED_FIELD_CHANGE_VERSION_V0: u16 = 0;
pub const MAX_PROJECTED_FIELD_CHANGE_ENTRIES_V0: usize = 100_000;
pub const MAX_PROJECTED_FIELD_CHANGE_INDEX_BYTES_V0: usize = 64 * 1_024 * 1_024;

const LENGTH_PREFIX_WIDTH: usize = 4;
const PREVIOUS_PRESENCE_WIDTH: usize = 1;
const SYNTHETIC_SEQUENCE_WIDTH: usize = 8;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ProjectedFieldRoleV0 {
    Discriminator,
    Value,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ProjectedFieldChangeLimitFieldV0 {
    IndexEntries,
    IndexBytes,
}

#[derive(Clone, Eq, PartialEq)]
pub enum ProjectedFieldChangeConfigErrorV0 {
    UnsupportedVersion {
        actual: u16,
    },
    InvalidFieldNameLength {
        role: ProjectedFieldRoleV0,
        minimum: usize,
        maximum: usize,
        actual: usize,
    },
    InvalidFieldNameSyntax {
        role: ProjectedFieldRoleV0,
    },
    DuplicateFieldRoles,
    ZeroLimit {
        field: ProjectedFieldChangeLimitFieldV0,
    },
    AboveMaximum {
        field: ProjectedFieldChangeLimitFieldV0,
        maximum: usize,
        actual: usize,
    },
}

impl fmt::Debug for ProjectedFieldChangeConfigErrorV0 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::UnsupportedVersion { actual } => formatter
                .debug_struct("UnsupportedVersion")
                .field("actual", actual)
                .finish(),
            Self::InvalidFieldNameLength {
                role,
                minimum,
                maximum,
                actual,
            } => formatter
                .debug_struct("InvalidFieldNameLength")
                .field("role", role)
                .field("minimum", minimum)
                .field("maximum", maximum)
                .field("actual", actual)
                .finish(),
            Self::InvalidFieldNameSyntax { role } => formatter
                .debug_struct("InvalidFieldNameSyntax")
                .field("role", role)
                .finish(),
            Self::DuplicateFieldRoles => formatter.write_str("DuplicateFieldRoles"),
            Self::ZeroLimit { field } => formatter
                .debug_struct("ZeroLimit")
                .field("field", field)
                .finish(),
            Self::AboveMaximum {
                field,
                maximum,
                actual,
            } => formatter
                .debug_struct("AboveMaximum")
                .field("field", field)
                .field("maximum", maximum)
                .field("actual", actual)
                .finish(),
        }
    }
}

impl fmt::Display for ProjectedFieldChangeConfigErrorV0 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(self, formatter)
    }
}

impl Error for ProjectedFieldChangeConfigErrorV0 {}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ProjectedFieldChangeLimitsV0 {
    index_entry_limit: usize,
    index_byte_limit: usize,
}

impl ProjectedFieldChangeLimitsV0 {
    pub fn new(
        index_entry_limit: usize,
        index_byte_limit: usize,
    ) -> Result<Self, ProjectedFieldChangeConfigErrorV0> {
        validate_limit(
            index_entry_limit,
            MAX_PROJECTED_FIELD_CHANGE_ENTRIES_V0,
            ProjectedFieldChangeLimitFieldV0::IndexEntries,
        )?;
        validate_limit(
            index_byte_limit,
            MAX_PROJECTED_FIELD_CHANGE_INDEX_BYTES_V0,
            ProjectedFieldChangeLimitFieldV0::IndexBytes,
        )?;
        Ok(Self {
            index_entry_limit,
            index_byte_limit,
        })
    }

    pub const fn maximum() -> Self {
        Self {
            index_entry_limit: MAX_PROJECTED_FIELD_CHANGE_ENTRIES_V0,
            index_byte_limit: MAX_PROJECTED_FIELD_CHANGE_INDEX_BYTES_V0,
        }
    }

    pub const fn index_entry_limit(self) -> usize {
        self.index_entry_limit
    }

    pub const fn index_byte_limit(self) -> usize {
        self.index_byte_limit
    }
}

fn validate_limit(
    actual: usize,
    maximum: usize,
    field: ProjectedFieldChangeLimitFieldV0,
) -> Result<(), ProjectedFieldChangeConfigErrorV0> {
    if actual == 0 {
        return Err(ProjectedFieldChangeConfigErrorV0::ZeroLimit { field });
    }
    if actual > maximum {
        return Err(ProjectedFieldChangeConfigErrorV0::AboveMaximum {
            field,
            maximum,
            actual,
        });
    }
    Ok(())
}

#[derive(Clone, Eq, PartialEq)]
struct ProjectedFieldNameV0(String);

impl ProjectedFieldNameV0 {
    fn new(
        role: ProjectedFieldRoleV0,
        value: &str,
    ) -> Result<Self, ProjectedFieldChangeConfigErrorV0> {
        let bytes = value.as_bytes();
        if !(1..=MAX_FIELD_NAME_BYTES).contains(&bytes.len()) {
            return Err(ProjectedFieldChangeConfigErrorV0::InvalidFieldNameLength {
                role,
                minimum: 1,
                maximum: MAX_FIELD_NAME_BYTES,
                actual: bytes.len(),
            });
        }
        let valid_first = bytes[0].is_ascii_lowercase() || bytes[0] == b'_';
        let valid_rest = bytes[1..].iter().all(|byte| {
            byte.is_ascii_lowercase() || byte.is_ascii_digit() || matches!(byte, b'.' | b'_' | b'-')
        });
        if !valid_first || !valid_rest {
            return Err(ProjectedFieldChangeConfigErrorV0::InvalidFieldNameSyntax { role });
        }
        Ok(Self(value.to_owned()))
    }

    fn as_str(&self) -> &str {
        &self.0
    }
}

#[derive(Clone, Eq, PartialEq)]
pub struct ProjectedFieldChangeConfigV0 {
    version: u16,
    retention: EventTimeRetentionConfigV0,
    schema_id: SchemaIdV0,
    discriminator_field: ProjectedFieldNameV0,
    value_field: ProjectedFieldNameV0,
    limits: ProjectedFieldChangeLimitsV0,
}

impl ProjectedFieldChangeConfigV0 {
    pub fn new(
        version: u16,
        retention: EventTimeRetentionConfigV0,
        schema_id: SchemaIdV0,
        discriminator_field: &str,
        value_field: &str,
        limits: ProjectedFieldChangeLimitsV0,
    ) -> Result<Self, ProjectedFieldChangeConfigErrorV0> {
        if version != PROJECTED_FIELD_CHANGE_VERSION_V0 {
            return Err(ProjectedFieldChangeConfigErrorV0::UnsupportedVersion { actual: version });
        }
        let discriminator_field =
            ProjectedFieldNameV0::new(ProjectedFieldRoleV0::Discriminator, discriminator_field)?;
        let value_field = ProjectedFieldNameV0::new(ProjectedFieldRoleV0::Value, value_field)?;
        if discriminator_field == value_field {
            return Err(ProjectedFieldChangeConfigErrorV0::DuplicateFieldRoles);
        }
        Ok(Self {
            version,
            retention,
            schema_id,
            discriminator_field,
            value_field,
            limits,
        })
    }

    pub const fn version(&self) -> u16 {
        self.version
    }

    pub const fn retention(&self) -> EventTimeRetentionConfigV0 {
        self.retention
    }

    pub const fn schema_id(&self) -> &SchemaIdV0 {
        &self.schema_id
    }

    pub fn discriminator_field(&self) -> &str {
        self.discriminator_field.as_str()
    }

    pub fn value_field(&self) -> &str {
        self.value_field.as_str()
    }

    pub const fn limits(&self) -> ProjectedFieldChangeLimitsV0 {
        self.limits
    }
}

impl fmt::Debug for ProjectedFieldChangeConfigV0 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ProjectedFieldChangeConfigV0")
            .field("version", &self.version)
            .field("schema_id", &"<redacted>")
            .field(
                "discriminator_field_byte_len",
                &self.discriminator_field.0.len(),
            )
            .field("value_field_byte_len", &self.value_field.0.len())
            .field("retention", &self.retention)
            .field("limits", &self.limits)
            .finish()
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ProjectedFieldContractIssueV0 {
    Missing,
    ExpectedUtf8 { actual: ScalarKindV0 },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ProjectedFieldChangeAccountingComponentV0 {
    Key,
    Discriminator,
    PreviousPresence,
    CurrentSequence,
    PreviousSequence,
    IndexLedger,
    ChangedLedger,
    AcceptedEventsConsidered,
    MatchingSchemaEvents,
    OtherSchemaEvents,
    UnkeyedMatchingEvents,
    KeyedMatchingEvents,
}

#[derive(Clone, Eq, PartialEq)]
pub enum ProjectedFieldChangeMaterializationErrorV0 {
    ArithmeticOverflow {
        component: ProjectedFieldChangeAccountingComponentV0,
    },
    SchemaContract {
        role: ProjectedFieldRoleV0,
        issue: ProjectedFieldContractIssueV0,
    },
    IndexInvariant,
    IndexEntryCapacity {
        maximum: usize,
        attempted: usize,
    },
    IndexByteCapacity {
        maximum: usize,
        attempted: usize,
    },
}

impl fmt::Debug for ProjectedFieldChangeMaterializationErrorV0 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ArithmeticOverflow { component } => formatter
                .debug_struct("ArithmeticOverflow")
                .field("component", component)
                .finish(),
            Self::SchemaContract { role, issue } => formatter
                .debug_struct("SchemaContract")
                .field("role", role)
                .field("issue", issue)
                .finish(),
            Self::IndexInvariant => formatter.write_str("IndexInvariant"),
            Self::IndexEntryCapacity { maximum, attempted } => formatter
                .debug_struct("IndexEntryCapacity")
                .field("maximum", maximum)
                .field("attempted", attempted)
                .finish(),
            Self::IndexByteCapacity { maximum, attempted } => formatter
                .debug_struct("IndexByteCapacity")
                .field("maximum", maximum)
                .field("attempted", attempted)
                .finish(),
        }
    }
}

impl fmt::Display for ProjectedFieldChangeMaterializationErrorV0 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(self, formatter)
    }
}

impl Error for ProjectedFieldChangeMaterializationErrorV0 {}

pub fn projected_field_change_entry_accounted_bytes_v0(
    key: &[u8],
    discriminator: &str,
    has_previous_distinct: bool,
) -> Result<usize, ProjectedFieldChangeMaterializationErrorV0> {
    let mut bytes = 0;
    checked_add(
        &mut bytes,
        LENGTH_PREFIX_WIDTH,
        ProjectedFieldChangeAccountingComponentV0::Key,
    )?;
    checked_add(
        &mut bytes,
        key.len(),
        ProjectedFieldChangeAccountingComponentV0::Key,
    )?;
    checked_add(
        &mut bytes,
        LENGTH_PREFIX_WIDTH,
        ProjectedFieldChangeAccountingComponentV0::Discriminator,
    )?;
    checked_add(
        &mut bytes,
        discriminator.len(),
        ProjectedFieldChangeAccountingComponentV0::Discriminator,
    )?;
    checked_add(
        &mut bytes,
        PREVIOUS_PRESENCE_WIDTH,
        ProjectedFieldChangeAccountingComponentV0::PreviousPresence,
    )?;
    checked_add(
        &mut bytes,
        SYNTHETIC_SEQUENCE_WIDTH,
        ProjectedFieldChangeAccountingComponentV0::CurrentSequence,
    )?;
    if has_previous_distinct {
        checked_add(
            &mut bytes,
            SYNTHETIC_SEQUENCE_WIDTH,
            ProjectedFieldChangeAccountingComponentV0::PreviousSequence,
        )?;
    }
    Ok(bytes)
}

fn checked_add(
    current: &mut usize,
    added: usize,
    component: ProjectedFieldChangeAccountingComponentV0,
) -> Result<(), ProjectedFieldChangeMaterializationErrorV0> {
    *current = current
        .checked_add(added)
        .ok_or(ProjectedFieldChangeMaterializationErrorV0::ArithmeticOverflow { component })?;
    Ok(())
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ProjectedFieldChangeUsageV0 {
    limits: ProjectedFieldChangeLimitsV0,
    tracked_series: usize,
    changed_series: usize,
    index_bytes: usize,
    accepted_events_considered: usize,
    matching_schema_events: usize,
    other_schema_events_omitted: usize,
    unkeyed_matching_events_omitted: usize,
    keyed_matching_events_processed: usize,
}

impl ProjectedFieldChangeUsageV0 {
    pub const fn limits(self) -> ProjectedFieldChangeLimitsV0 {
        self.limits
    }

    pub const fn tracked_series(self) -> usize {
        self.tracked_series
    }

    pub const fn changed_series(self) -> usize {
        self.changed_series
    }

    pub const fn index_bytes(self) -> usize {
        self.index_bytes
    }

    pub const fn accepted_events_considered(self) -> usize {
        self.accepted_events_considered
    }

    pub const fn matching_schema_events(self) -> usize {
        self.matching_schema_events
    }

    pub const fn other_schema_events_omitted(self) -> usize {
        self.other_schema_events_omitted
    }

    pub const fn unkeyed_matching_events_omitted(self) -> usize {
        self.unkeyed_matching_events_omitted
    }

    pub const fn keyed_matching_events_processed(self) -> usize {
        self.keyed_matching_events_processed
    }
}

#[derive(Clone, Copy)]
pub struct ProjectedFieldChangeObservationV0<'a> {
    previous_event: &'a EventEnvelopeV0,
    previous_value: &'a ScalarValueV0,
    current_event: &'a EventEnvelopeV0,
    current_value: &'a ScalarValueV0,
}

impl<'a> ProjectedFieldChangeObservationV0<'a> {
    pub const fn previous_event(self) -> &'a EventEnvelopeV0 {
        self.previous_event
    }

    pub const fn previous_value(self) -> &'a ScalarValueV0 {
        self.previous_value
    }

    pub const fn current_event(self) -> &'a EventEnvelopeV0 {
        self.current_event
    }

    pub const fn current_value(self) -> &'a ScalarValueV0 {
        self.current_value
    }
}

impl fmt::Debug for ProjectedFieldChangeObservationV0<'_> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ProjectedFieldChangeObservationV0")
            .field("has_previous", &true)
            .field("previous_kind", &self.previous_value.kind())
            .field("has_current", &true)
            .field("current_kind", &self.current_value.kind())
            .finish()
    }
}

/// A bounded projected-change view that keeps admission immutable.
///
/// ```compile_fail
/// use decant_ir::EventEnvelopeV0;
/// use decant_state::{AdmissionStateV0, ProjectedFieldChangeConfigV0};
/// fn cannot_mutate_while_view_is_live(
///     admission: &mut AdmissionStateV0,
///     event: &EventEnvelopeV0,
///     config: ProjectedFieldChangeConfigV0,
/// ) {
///     let view = admission.materialize_projected_field_changes(config).unwrap();
///     admission.apply_event(event).unwrap();
///     let _ = view.usage();
/// }
/// ```
///
/// ```compile_fail
/// use decant_ir::SourceIdV0;
/// use decant_state::{AdmissionStateV0, ProjectedFieldChangeConfigV0};
/// fn cannot_finalize_while_view_is_live(
///     admission: &mut AdmissionStateV0,
///     source_id: &SourceIdV0,
///     finalized_through_ns: i64,
///     config: ProjectedFieldChangeConfigV0,
/// ) {
///     let view = admission.materialize_projected_field_changes(config).unwrap();
///     admission.finalize_source(source_id, finalized_through_ns);
///     let _ = view.usage();
/// }
/// ```
pub struct ProjectedFieldChangeViewV0<'a> {
    admission: &'a AdmissionStateV0,
    config: ProjectedFieldChangeConfigV0,
    retention_cutoff_event_time_ns: Option<i64>,
    retention_usage: EventTimeRetentionUsageV0,
    tracked: BTreeMap<Vec<u8>, BTreeMap<&'a str, TrackedProjectedValueV0<'a>>>,
    tracked_series: usize,
    changed_series: usize,
    index_bytes: usize,
    accepted_events_considered: usize,
    matching_schema_events: usize,
    other_schema_events_omitted: usize,
    unkeyed_matching_events_omitted: usize,
    keyed_matching_events_processed: usize,
}

#[derive(Clone, Copy)]
struct RankedProjectedValueV0<'a> {
    event: &'a EventEnvelopeV0,
    value: &'a ScalarValueV0,
}

#[derive(Clone, Copy)]
struct TrackedProjectedValueV0<'a> {
    current: RankedProjectedValueV0<'a>,
    previous_distinct: Option<RankedProjectedValueV0<'a>>,
}

impl AdmissionStateV0 {
    pub fn materialize_projected_field_changes(
        &self,
        config: ProjectedFieldChangeConfigV0,
    ) -> Result<ProjectedFieldChangeViewV0<'_>, ProjectedFieldChangeMaterializationErrorV0> {
        let retention = self.project_event_time_retention(config.retention);
        let retention_cutoff_event_time_ns = retention.cutoff_event_time_ns();
        let retention_usage = retention.usage();
        let mut view = ProjectedFieldChangeViewV0 {
            admission: self,
            config,
            retention_cutoff_event_time_ns,
            retention_usage,
            tracked: BTreeMap::new(),
            tracked_series: 0,
            changed_series: 0,
            index_bytes: 0,
            accepted_events_considered: 0,
            matching_schema_events: 0,
            other_schema_events_omitted: 0,
            unkeyed_matching_events_omitted: 0,
            keyed_matching_events_processed: 0,
        };

        for event in retention.events() {
            view.accepted_events_considered = checked_increment(
                view.accepted_events_considered,
                ProjectedFieldChangeAccountingComponentV0::AcceptedEventsConsidered,
            )?;
            if event.schema_id() != &view.config.schema_id {
                view.other_schema_events_omitted = checked_increment(
                    view.other_schema_events_omitted,
                    ProjectedFieldChangeAccountingComponentV0::OtherSchemaEvents,
                )?;
                continue;
            }
            view.matching_schema_events = checked_increment(
                view.matching_schema_events,
                ProjectedFieldChangeAccountingComponentV0::MatchingSchemaEvents,
            )?;
            let Some(key) = event.key() else {
                view.unkeyed_matching_events_omitted = checked_increment(
                    view.unkeyed_matching_events_omitted,
                    ProjectedFieldChangeAccountingComponentV0::UnkeyedMatchingEvents,
                )?;
                continue;
            };
            view.keyed_matching_events_processed = checked_increment(
                view.keyed_matching_events_processed,
                ProjectedFieldChangeAccountingComponentV0::KeyedMatchingEvents,
            )?;

            let discriminator = payload_field(event, view.config.discriminator_field()).ok_or(
                ProjectedFieldChangeMaterializationErrorV0::SchemaContract {
                    role: ProjectedFieldRoleV0::Discriminator,
                    issue: ProjectedFieldContractIssueV0::Missing,
                },
            )?;
            let discriminator = discriminator.as_utf8().ok_or(
                ProjectedFieldChangeMaterializationErrorV0::SchemaContract {
                    role: ProjectedFieldRoleV0::Discriminator,
                    issue: ProjectedFieldContractIssueV0::ExpectedUtf8 {
                        actual: discriminator.kind(),
                    },
                },
            )?;
            let value = payload_field(event, view.config.value_field()).ok_or(
                ProjectedFieldChangeMaterializationErrorV0::SchemaContract {
                    role: ProjectedFieldRoleV0::Value,
                    issue: ProjectedFieldContractIssueV0::Missing,
                },
            )?;

            let existing = view
                .tracked
                .get(key)
                .and_then(|by_discriminator| by_discriminator.get(discriminator))
                .copied();
            if let Some(existing) = existing {
                let updated = update_tracked_value(existing, event, value);
                let became_changed =
                    existing.previous_distinct.is_none() && updated.previous_distinct.is_some();
                let (prospective_changed_series, prospective_bytes) = if became_changed {
                    let changed_series = checked_increment(
                        view.changed_series,
                        ProjectedFieldChangeAccountingComponentV0::ChangedLedger,
                    )?;
                    let index_bytes = view
                        .index_bytes
                        .checked_add(SYNTHETIC_SEQUENCE_WIDTH)
                        .ok_or(
                            ProjectedFieldChangeMaterializationErrorV0::ArithmeticOverflow {
                                component: ProjectedFieldChangeAccountingComponentV0::IndexLedger,
                            },
                        )?;
                    if index_bytes > view.config.limits.index_byte_limit {
                        return Err(
                            ProjectedFieldChangeMaterializationErrorV0::IndexByteCapacity {
                                maximum: view.config.limits.index_byte_limit,
                                attempted: index_bytes,
                            },
                        );
                    }
                    (changed_series, index_bytes)
                } else {
                    (view.changed_series, view.index_bytes)
                };

                let Some(tracked) = view
                    .tracked
                    .get_mut(key)
                    .and_then(|by_discriminator| by_discriminator.get_mut(discriminator))
                else {
                    return Err(ProjectedFieldChangeMaterializationErrorV0::IndexInvariant);
                };
                *tracked = updated;
                view.changed_series = prospective_changed_series;
                view.index_bytes = prospective_bytes;
                continue;
            }

            let entry_bytes =
                projected_field_change_entry_accounted_bytes_v0(key, discriminator, false)?;
            let prospective_entries = checked_increment(
                view.tracked_series,
                ProjectedFieldChangeAccountingComponentV0::IndexLedger,
            )?;
            let prospective_bytes = view.index_bytes.checked_add(entry_bytes).ok_or(
                ProjectedFieldChangeMaterializationErrorV0::ArithmeticOverflow {
                    component: ProjectedFieldChangeAccountingComponentV0::IndexLedger,
                },
            )?;
            if prospective_entries > view.config.limits.index_entry_limit {
                return Err(
                    ProjectedFieldChangeMaterializationErrorV0::IndexEntryCapacity {
                        maximum: view.config.limits.index_entry_limit,
                        attempted: prospective_entries,
                    },
                );
            }
            if prospective_bytes > view.config.limits.index_byte_limit {
                return Err(
                    ProjectedFieldChangeMaterializationErrorV0::IndexByteCapacity {
                        maximum: view.config.limits.index_byte_limit,
                        attempted: prospective_bytes,
                    },
                );
            }

            view.tracked.entry(key.to_vec()).or_default().insert(
                discriminator,
                TrackedProjectedValueV0 {
                    current: ranked_value(event, value),
                    previous_distinct: None,
                },
            );
            view.tracked_series = prospective_entries;
            view.index_bytes = prospective_bytes;
        }

        view.verify_usage_invariants()?;
        Ok(view)
    }
}

impl<'a> ProjectedFieldChangeViewV0<'a> {
    pub const fn config(&self) -> &ProjectedFieldChangeConfigV0 {
        &self.config
    }

    pub const fn retention_cutoff_event_time_ns(&self) -> Option<i64> {
        self.retention_cutoff_event_time_ns
    }

    pub const fn retention_usage(&self) -> EventTimeRetentionUsageV0 {
        self.retention_usage
    }

    pub const fn usage(&self) -> ProjectedFieldChangeUsageV0 {
        ProjectedFieldChangeUsageV0 {
            limits: self.config.limits,
            tracked_series: self.tracked_series,
            changed_series: self.changed_series,
            index_bytes: self.index_bytes,
            accepted_events_considered: self.accepted_events_considered,
            matching_schema_events: self.matching_schema_events,
            other_schema_events_omitted: self.other_schema_events_omitted,
            unkeyed_matching_events_omitted: self.unkeyed_matching_events_omitted,
            keyed_matching_events_processed: self.keyed_matching_events_processed,
        }
    }

    pub fn is_tracked(&self, key: &[u8], discriminator: &str) -> bool {
        self.tracked
            .get(key)
            .is_some_and(|by_discriminator| by_discriminator.contains_key(discriminator))
    }

    pub fn get(
        &self,
        key: &[u8],
        discriminator: &str,
    ) -> Option<ProjectedFieldChangeObservationV0<'a>> {
        let tracked = self.tracked.get(key)?.get(discriminator)?;
        Some(ProjectedFieldChangeObservationV0 {
            previous_event: tracked.previous_distinct?.event,
            previous_value: tracked.previous_distinct?.value,
            current_event: tracked.current.event,
            current_value: tracked.current.value,
        })
    }

    pub fn entries(
        &self,
    ) -> impl Iterator<Item = (&[u8], &str, ProjectedFieldChangeObservationV0<'a>)> + '_ {
        self.tracked
            .iter()
            .flat_map(move |(key, by_discriminator)| {
                by_discriminator
                    .iter()
                    .filter_map(move |(discriminator, tracked)| {
                        Some((
                            key.as_slice(),
                            *discriminator,
                            ProjectedFieldChangeObservationV0 {
                                previous_event: tracked.previous_distinct?.event,
                                previous_value: tracked.previous_distinct?.value,
                                current_event: tracked.current.event,
                                current_value: tracked.current.value,
                            },
                        ))
                    })
            })
    }

    pub const fn source_watermark_ns(&self) -> Option<i64> {
        self.admission.watermark_ns()
    }

    pub const fn is_source_finalized(&self) -> bool {
        self.admission.is_finalized()
    }

    fn verify_usage_invariants(&self) -> Result<(), ProjectedFieldChangeMaterializationErrorV0> {
        let classified = self
            .matching_schema_events
            .checked_add(self.other_schema_events_omitted)
            .ok_or(ProjectedFieldChangeMaterializationErrorV0::IndexInvariant)?;
        let matching = self
            .unkeyed_matching_events_omitted
            .checked_add(self.keyed_matching_events_processed)
            .ok_or(ProjectedFieldChangeMaterializationErrorV0::IndexInvariant)?;
        if self.accepted_events_considered != self.retention_usage.retained_events()
            || self.accepted_events_considered != classified
            || self.matching_schema_events != matching
        {
            return Err(ProjectedFieldChangeMaterializationErrorV0::IndexInvariant);
        }
        Ok(())
    }
}

impl fmt::Debug for ProjectedFieldChangeViewV0<'_> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ProjectedFieldChangeViewV0")
            .field("config", &self.config)
            .field("retention_usage", &self.retention_usage)
            .field("usage", &self.usage())
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

fn checked_increment(
    current: usize,
    component: ProjectedFieldChangeAccountingComponentV0,
) -> Result<usize, ProjectedFieldChangeMaterializationErrorV0> {
    current
        .checked_add(1)
        .ok_or(ProjectedFieldChangeMaterializationErrorV0::ArithmeticOverflow { component })
}

fn payload_field<'a>(event: &'a EventEnvelopeV0, field_name: &str) -> Option<&'a ScalarValueV0> {
    event
        .payload()
        .fields()
        .find_map(|(name, value)| (name == field_name).then_some(value))
}

fn update_tracked_value<'a>(
    mut tracked: TrackedProjectedValueV0<'a>,
    event: &'a EventEnvelopeV0,
    value: &'a ScalarValueV0,
) -> TrackedProjectedValueV0<'a> {
    let candidate = ranked_value(event, value);
    if candidate.value == tracked.current.value {
        if rank(candidate) > rank(tracked.current) {
            tracked.current = candidate;
        }
        return tracked;
    }

    if rank(candidate) > rank(tracked.current) {
        tracked.previous_distinct = Some(tracked.current);
        tracked.current = candidate;
        return tracked;
    }

    if tracked
        .previous_distinct
        .is_none_or(|previous| rank(candidate) > rank(previous))
    {
        tracked.previous_distinct = Some(candidate);
    }
    tracked
}

fn ranked_value<'a>(
    event: &'a EventEnvelopeV0,
    value: &'a ScalarValueV0,
) -> RankedProjectedValueV0<'a> {
    RankedProjectedValueV0 { event, value }
}

fn rank(observation: RankedProjectedValueV0<'_>) -> (i64, decant_ir::SourcePositionV0) {
    event_rank_v0(observation.event)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn accounting_overflow_is_typed() {
        let mut bytes = usize::MAX;
        assert_eq!(
            checked_add(
                &mut bytes,
                1,
                ProjectedFieldChangeAccountingComponentV0::IndexLedger,
            ),
            Err(
                ProjectedFieldChangeMaterializationErrorV0::ArithmeticOverflow {
                    component: ProjectedFieldChangeAccountingComponentV0::IndexLedger,
                }
            )
        );
        assert_eq!(bytes, usize::MAX);
    }
}
