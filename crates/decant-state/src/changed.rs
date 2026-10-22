//! Bounded changed-by-schema-and-key materialization over admitted events.

use std::collections::BTreeMap;
use std::error::Error;
use std::fmt;

use decant_ir::{EventEnvelopeV0, SchemaIdV0};

use crate::{AdmissionStateV0, event_rank_v0};

pub const CHANGED_BY_KEY_VERSION_V0: u16 = 0;
pub const MAX_CHANGED_INDEX_ENTRIES_V0: usize = 100_000;
pub const MAX_CHANGED_INDEX_BYTES_V0: usize = 64 * 1_024 * 1_024;

const LENGTH_PREFIX_WIDTH: usize = 4;
const PREVIOUS_PRESENCE_WIDTH: usize = 1;
const SYNTHETIC_SEQUENCE_WIDTH: usize = 8;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ChangedByKeyLimitFieldV0 {
    IndexEntries,
    IndexBytes,
}

#[derive(Clone, Eq, PartialEq)]
pub enum ChangedByKeyConfigErrorV0 {
    UnsupportedVersion {
        actual: u16,
    },
    ZeroLimit {
        field: ChangedByKeyLimitFieldV0,
    },
    AboveMaximum {
        field: ChangedByKeyLimitFieldV0,
        maximum: usize,
        actual: usize,
    },
}

impl fmt::Debug for ChangedByKeyConfigErrorV0 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::UnsupportedVersion { actual } => formatter
                .debug_struct("UnsupportedVersion")
                .field("actual", actual)
                .finish(),
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

impl fmt::Display for ChangedByKeyConfigErrorV0 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(self, formatter)
    }
}

impl Error for ChangedByKeyConfigErrorV0 {}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ChangedByKeyLimitsV0 {
    index_entry_limit: usize,
    index_byte_limit: usize,
}

impl ChangedByKeyLimitsV0 {
    pub fn new(
        index_entry_limit: usize,
        index_byte_limit: usize,
    ) -> Result<Self, ChangedByKeyConfigErrorV0> {
        validate_limit(
            index_entry_limit,
            MAX_CHANGED_INDEX_ENTRIES_V0,
            ChangedByKeyLimitFieldV0::IndexEntries,
        )?;
        validate_limit(
            index_byte_limit,
            MAX_CHANGED_INDEX_BYTES_V0,
            ChangedByKeyLimitFieldV0::IndexBytes,
        )?;
        Ok(Self {
            index_entry_limit,
            index_byte_limit,
        })
    }

    pub const fn maximum() -> Self {
        Self {
            index_entry_limit: MAX_CHANGED_INDEX_ENTRIES_V0,
            index_byte_limit: MAX_CHANGED_INDEX_BYTES_V0,
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
    field: ChangedByKeyLimitFieldV0,
) -> Result<(), ChangedByKeyConfigErrorV0> {
    if actual == 0 {
        return Err(ChangedByKeyConfigErrorV0::ZeroLimit { field });
    }
    if actual > maximum {
        return Err(ChangedByKeyConfigErrorV0::AboveMaximum {
            field,
            maximum,
            actual,
        });
    }
    Ok(())
}

#[derive(Clone, Copy, Eq, PartialEq)]
pub struct ChangedByKeyConfigV0 {
    version: u16,
    limits: ChangedByKeyLimitsV0,
}

impl ChangedByKeyConfigV0 {
    pub fn new(
        version: u16,
        limits: ChangedByKeyLimitsV0,
    ) -> Result<Self, ChangedByKeyConfigErrorV0> {
        if version != CHANGED_BY_KEY_VERSION_V0 {
            return Err(ChangedByKeyConfigErrorV0::UnsupportedVersion { actual: version });
        }
        Ok(Self { version, limits })
    }

    pub const fn version(self) -> u16 {
        self.version
    }

    pub const fn limits(self) -> ChangedByKeyLimitsV0 {
        self.limits
    }
}

impl fmt::Debug for ChangedByKeyConfigV0 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ChangedByKeyConfigV0")
            .field("version", &self.version)
            .field("limits", &self.limits)
            .finish()
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ChangedIndexAccountingComponentV0 {
    SchemaId,
    Key,
    PreviousPresence,
    CurrentSequence,
    PreviousSequence,
    IndexLedger,
    ChangedLedger,
    AcceptedEventsConsidered,
    UnkeyedCount,
}

#[derive(Clone, Eq, PartialEq)]
pub enum ChangedMaterializationErrorV0 {
    ArithmeticOverflow {
        component: ChangedIndexAccountingComponentV0,
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

impl fmt::Debug for ChangedMaterializationErrorV0 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ArithmeticOverflow { component } => formatter
                .debug_struct("ArithmeticOverflow")
                .field("component", component)
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

impl fmt::Display for ChangedMaterializationErrorV0 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(self, formatter)
    }
}

impl Error for ChangedMaterializationErrorV0 {}

pub fn changed_index_entry_accounted_bytes_v0(
    schema_id: &SchemaIdV0,
    key: &[u8],
    has_previous_distinct: bool,
) -> Result<usize, ChangedMaterializationErrorV0> {
    let mut bytes = 0;
    checked_add(
        &mut bytes,
        LENGTH_PREFIX_WIDTH,
        ChangedIndexAccountingComponentV0::SchemaId,
    )?;
    checked_add(
        &mut bytes,
        schema_id.as_str().len(),
        ChangedIndexAccountingComponentV0::SchemaId,
    )?;
    checked_add(
        &mut bytes,
        LENGTH_PREFIX_WIDTH,
        ChangedIndexAccountingComponentV0::Key,
    )?;
    checked_add(
        &mut bytes,
        key.len(),
        ChangedIndexAccountingComponentV0::Key,
    )?;
    checked_add(
        &mut bytes,
        PREVIOUS_PRESENCE_WIDTH,
        ChangedIndexAccountingComponentV0::PreviousPresence,
    )?;
    checked_add(
        &mut bytes,
        SYNTHETIC_SEQUENCE_WIDTH,
        ChangedIndexAccountingComponentV0::CurrentSequence,
    )?;
    if has_previous_distinct {
        checked_add(
            &mut bytes,
            SYNTHETIC_SEQUENCE_WIDTH,
            ChangedIndexAccountingComponentV0::PreviousSequence,
        )?;
    }
    Ok(bytes)
}

fn checked_add(
    current: &mut usize,
    added: usize,
    component: ChangedIndexAccountingComponentV0,
) -> Result<(), ChangedMaterializationErrorV0> {
    *current = current
        .checked_add(added)
        .ok_or(ChangedMaterializationErrorV0::ArithmeticOverflow { component })?;
    Ok(())
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ChangedByKeyUsageV0 {
    limits: ChangedByKeyLimitsV0,
    tracked_entries: usize,
    changed_entries: usize,
    index_bytes: usize,
    accepted_events_considered: usize,
    unkeyed_events_omitted: usize,
}

impl ChangedByKeyUsageV0 {
    pub const fn limits(self) -> ChangedByKeyLimitsV0 {
        self.limits
    }

    pub const fn tracked_entries(self) -> usize {
        self.tracked_entries
    }

    pub const fn changed_entries(self) -> usize {
        self.changed_entries
    }

    pub const fn index_bytes(self) -> usize {
        self.index_bytes
    }

    pub const fn accepted_events_considered(self) -> usize {
        self.accepted_events_considered
    }

    pub const fn unkeyed_events_omitted(self) -> usize {
        self.unkeyed_events_omitted
    }
}

#[derive(Clone, Copy)]
pub struct ChangeObservationV0<'a> {
    previous: &'a EventEnvelopeV0,
    current: &'a EventEnvelopeV0,
}

impl<'a> ChangeObservationV0<'a> {
    pub const fn previous(self) -> &'a EventEnvelopeV0 {
        self.previous
    }

    pub const fn current(self) -> &'a EventEnvelopeV0 {
        self.current
    }
}

impl fmt::Debug for ChangeObservationV0<'_> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ChangeObservationV0")
            .field("has_previous", &true)
            .field("has_current", &true)
            .finish()
    }
}

/// A bounded changed view that keeps its admission ledger immutable.
///
/// ```compile_fail
/// use decant_ir::EventEnvelopeV0;
/// use decant_state::{AdmissionStateV0, ChangedByKeyConfigV0};
/// fn cannot_mutate_while_view_is_live(
///     admission: &mut AdmissionStateV0,
///     event: &EventEnvelopeV0,
///     config: ChangedByKeyConfigV0,
/// ) {
///     let view = admission.materialize_changed_by_key(config).unwrap();
///     admission.apply_event(event).unwrap();
///     let _ = view.usage();
/// }
/// ```
pub struct ChangedByKeyViewV0<'a> {
    admission: &'a AdmissionStateV0,
    config: ChangedByKeyConfigV0,
    tracked: BTreeMap<SchemaIdV0, BTreeMap<Vec<u8>, TrackedValueV0<'a>>>,
    tracked_entries: usize,
    changed_entries: usize,
    index_bytes: usize,
    accepted_events_considered: usize,
    unkeyed_events_omitted: usize,
}

#[derive(Clone, Copy)]
struct RankedEventV0<'a> {
    event: &'a EventEnvelopeV0,
}

#[derive(Clone, Copy)]
struct TrackedValueV0<'a> {
    current: RankedEventV0<'a>,
    previous_distinct: Option<RankedEventV0<'a>>,
}

impl AdmissionStateV0 {
    pub fn materialize_changed_by_key<'a>(
        &'a self,
        config: ChangedByKeyConfigV0,
    ) -> Result<ChangedByKeyViewV0<'a>, ChangedMaterializationErrorV0> {
        materialize_changed_by_key_from_events(self, config, self.accepted_events.values())
    }
}

pub(crate) fn materialize_changed_by_key_from_events<'a>(
    admission: &'a AdmissionStateV0,
    config: ChangedByKeyConfigV0,
    events: impl Iterator<Item = &'a EventEnvelopeV0>,
) -> Result<ChangedByKeyViewV0<'a>, ChangedMaterializationErrorV0> {
    let mut view = ChangedByKeyViewV0 {
        admission,
        config,
        tracked: BTreeMap::new(),
        tracked_entries: 0,
        changed_entries: 0,
        index_bytes: 0,
        accepted_events_considered: 0,
        unkeyed_events_omitted: 0,
    };

    for event in events {
        view.accepted_events_considered = view.accepted_events_considered.checked_add(1).ok_or(
            ChangedMaterializationErrorV0::ArithmeticOverflow {
                component: ChangedIndexAccountingComponentV0::AcceptedEventsConsidered,
            },
        )?;
        let Some(key) = event.key() else {
            view.unkeyed_events_omitted = view.unkeyed_events_omitted.checked_add(1).ok_or(
                ChangedMaterializationErrorV0::ArithmeticOverflow {
                    component: ChangedIndexAccountingComponentV0::UnkeyedCount,
                },
            )?;
            continue;
        };

        let schema_id = event.schema_id();
        let existing = view
            .tracked
            .get(schema_id)
            .and_then(|by_key| by_key.get(key))
            .copied();
        if let Some(existing) = existing {
            let updated = update_tracked_value(existing, event);
            let became_changed =
                existing.previous_distinct.is_none() && updated.previous_distinct.is_some();
            let (prospective_changed_entries, prospective_bytes) = if became_changed {
                let changed_entries = view.changed_entries.checked_add(1).ok_or(
                    ChangedMaterializationErrorV0::ArithmeticOverflow {
                        component: ChangedIndexAccountingComponentV0::ChangedLedger,
                    },
                )?;
                let index_bytes = view
                    .index_bytes
                    .checked_add(SYNTHETIC_SEQUENCE_WIDTH)
                    .ok_or(ChangedMaterializationErrorV0::ArithmeticOverflow {
                        component: ChangedIndexAccountingComponentV0::IndexLedger,
                    })?;
                if index_bytes > config.limits.index_byte_limit {
                    return Err(ChangedMaterializationErrorV0::IndexByteCapacity {
                        maximum: config.limits.index_byte_limit,
                        attempted: index_bytes,
                    });
                }
                (changed_entries, index_bytes)
            } else {
                (view.changed_entries, view.index_bytes)
            };

            let Some(tracked) = view
                .tracked
                .get_mut(schema_id)
                .and_then(|by_key| by_key.get_mut(key))
            else {
                return Err(ChangedMaterializationErrorV0::IndexInvariant);
            };
            *tracked = updated;
            view.changed_entries = prospective_changed_entries;
            view.index_bytes = prospective_bytes;
            continue;
        }

        let entry_bytes = changed_index_entry_accounted_bytes_v0(schema_id, key, false)?;
        let prospective_entries = view.tracked_entries.checked_add(1).ok_or(
            ChangedMaterializationErrorV0::ArithmeticOverflow {
                component: ChangedIndexAccountingComponentV0::IndexLedger,
            },
        )?;
        let prospective_bytes = view.index_bytes.checked_add(entry_bytes).ok_or(
            ChangedMaterializationErrorV0::ArithmeticOverflow {
                component: ChangedIndexAccountingComponentV0::IndexLedger,
            },
        )?;
        if prospective_entries > config.limits.index_entry_limit {
            return Err(ChangedMaterializationErrorV0::IndexEntryCapacity {
                maximum: config.limits.index_entry_limit,
                attempted: prospective_entries,
            });
        }
        if prospective_bytes > config.limits.index_byte_limit {
            return Err(ChangedMaterializationErrorV0::IndexByteCapacity {
                maximum: config.limits.index_byte_limit,
                attempted: prospective_bytes,
            });
        }

        view.tracked.entry(schema_id.clone()).or_default().insert(
            key.to_vec(),
            TrackedValueV0 {
                current: ranked_event(event),
                previous_distinct: None,
            },
        );
        view.tracked_entries = prospective_entries;
        view.index_bytes = prospective_bytes;
    }

    Ok(view)
}

impl<'a> ChangedByKeyViewV0<'a> {
    pub const fn config(&self) -> ChangedByKeyConfigV0 {
        self.config
    }

    pub fn usage(&self) -> ChangedByKeyUsageV0 {
        ChangedByKeyUsageV0 {
            limits: self.config.limits,
            tracked_entries: self.tracked_entries,
            changed_entries: self.changed_entries,
            index_bytes: self.index_bytes,
            accepted_events_considered: self.accepted_events_considered,
            unkeyed_events_omitted: self.unkeyed_events_omitted,
        }
    }

    pub fn is_tracked(&self, schema_id: &SchemaIdV0, key: &[u8]) -> bool {
        self.tracked
            .get(schema_id)
            .is_some_and(|by_key| by_key.contains_key(key))
    }

    pub fn get(&self, schema_id: &SchemaIdV0, key: &[u8]) -> Option<ChangeObservationV0<'a>> {
        let tracked = self.tracked.get(schema_id)?.get(key)?;
        Some(ChangeObservationV0 {
            previous: tracked.previous_distinct?.event,
            current: tracked.current.event,
        })
    }

    pub fn entries(
        &self,
    ) -> impl Iterator<Item = (&SchemaIdV0, &[u8], ChangeObservationV0<'a>)> + '_ {
        self.tracked.iter().flat_map(move |(schema_id, by_key)| {
            by_key.iter().filter_map(move |(key, tracked)| {
                Some((
                    schema_id,
                    key.as_slice(),
                    ChangeObservationV0 {
                        previous: tracked.previous_distinct?.event,
                        current: tracked.current.event,
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
}

impl fmt::Debug for ChangedByKeyViewV0<'_> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ChangedByKeyViewV0")
            .field("config", &self.config)
            .field("usage", &self.usage())
            .field(
                "has_source_watermark",
                &self.source_watermark_ns().is_some(),
            )
            .field("source_finalized", &self.is_source_finalized())
            .finish()
    }
}

fn update_tracked_value<'a>(
    mut tracked: TrackedValueV0<'a>,
    event: &'a EventEnvelopeV0,
) -> TrackedValueV0<'a> {
    let candidate = ranked_event(event);
    if event.payload() == tracked.current.event.payload() {
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

fn ranked_event(event: &EventEnvelopeV0) -> RankedEventV0<'_> {
    RankedEventV0 { event }
}

fn rank(observation: RankedEventV0<'_>) -> (i64, decant_ir::SourcePositionV0) {
    event_rank_v0(observation.event)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn accounting_overflow_is_typed_and_non_mutating() {
        let mut current = usize::MAX;
        assert_eq!(
            checked_add(
                &mut current,
                1,
                ChangedIndexAccountingComponentV0::PreviousSequence,
            ),
            Err(ChangedMaterializationErrorV0::ArithmeticOverflow {
                component: ChangedIndexAccountingComponentV0::PreviousSequence,
            })
        );
        assert_eq!(current, usize::MAX);
    }
}
