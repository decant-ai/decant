//! Bounded latest-by-schema-and-key materialization over admitted events.

use std::collections::BTreeMap;
use std::error::Error;
use std::fmt;

use decant_ir::{EventEnvelopeV0, SchemaIdV0};

use crate::{AdmissionStateV0, event_rank_v0};

pub const LATEST_BY_KEY_VERSION_V0: u16 = 0;
pub const MAX_LATEST_INDEX_ENTRIES_V0: usize = 100_000;
pub const MAX_LATEST_INDEX_BYTES_V0: usize = 64 * 1_024 * 1_024;

const LENGTH_PREFIX_WIDTH: usize = 4;
const SYNTHETIC_SEQUENCE_WIDTH: usize = 8;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum LatestByKeyLimitFieldV0 {
    IndexEntries,
    IndexBytes,
}

#[derive(Clone, Eq, PartialEq)]
pub enum LatestByKeyConfigErrorV0 {
    UnsupportedVersion {
        actual: u16,
    },
    ZeroLimit {
        field: LatestByKeyLimitFieldV0,
    },
    AboveMaximum {
        field: LatestByKeyLimitFieldV0,
        maximum: usize,
        actual: usize,
    },
}

impl fmt::Debug for LatestByKeyConfigErrorV0 {
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

impl fmt::Display for LatestByKeyConfigErrorV0 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(self, formatter)
    }
}

impl Error for LatestByKeyConfigErrorV0 {}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct LatestByKeyLimitsV0 {
    index_entry_limit: usize,
    index_byte_limit: usize,
}

impl LatestByKeyLimitsV0 {
    pub fn new(
        index_entry_limit: usize,
        index_byte_limit: usize,
    ) -> Result<Self, LatestByKeyConfigErrorV0> {
        validate_limit(
            index_entry_limit,
            MAX_LATEST_INDEX_ENTRIES_V0,
            LatestByKeyLimitFieldV0::IndexEntries,
        )?;
        validate_limit(
            index_byte_limit,
            MAX_LATEST_INDEX_BYTES_V0,
            LatestByKeyLimitFieldV0::IndexBytes,
        )?;
        Ok(Self {
            index_entry_limit,
            index_byte_limit,
        })
    }

    pub const fn maximum() -> Self {
        Self {
            index_entry_limit: MAX_LATEST_INDEX_ENTRIES_V0,
            index_byte_limit: MAX_LATEST_INDEX_BYTES_V0,
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
    field: LatestByKeyLimitFieldV0,
) -> Result<(), LatestByKeyConfigErrorV0> {
    if actual == 0 {
        return Err(LatestByKeyConfigErrorV0::ZeroLimit { field });
    }
    if actual > maximum {
        return Err(LatestByKeyConfigErrorV0::AboveMaximum {
            field,
            maximum,
            actual,
        });
    }
    Ok(())
}

#[derive(Clone, Copy, Eq, PartialEq)]
pub struct LatestByKeyConfigV0 {
    version: u16,
    limits: LatestByKeyLimitsV0,
}

impl LatestByKeyConfigV0 {
    pub fn new(
        version: u16,
        limits: LatestByKeyLimitsV0,
    ) -> Result<Self, LatestByKeyConfigErrorV0> {
        if version != LATEST_BY_KEY_VERSION_V0 {
            return Err(LatestByKeyConfigErrorV0::UnsupportedVersion { actual: version });
        }
        Ok(Self { version, limits })
    }

    pub const fn version(self) -> u16 {
        self.version
    }

    pub const fn limits(self) -> LatestByKeyLimitsV0 {
        self.limits
    }
}

impl fmt::Debug for LatestByKeyConfigV0 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("LatestByKeyConfigV0")
            .field("version", &self.version)
            .field("limits", &self.limits)
            .finish()
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum LatestIndexAccountingComponentV0 {
    SchemaId,
    Key,
    WinningSequence,
    IndexLedger,
    AcceptedEventsConsidered,
    UnkeyedCount,
}

#[derive(Clone, Eq, PartialEq)]
pub enum LatestMaterializationErrorV0 {
    ArithmeticOverflow {
        component: LatestIndexAccountingComponentV0,
    },
    IndexEntryCapacity {
        maximum: usize,
        attempted: usize,
    },
    IndexByteCapacity {
        maximum: usize,
        attempted: usize,
    },
    IndexInvariant,
}

impl fmt::Debug for LatestMaterializationErrorV0 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ArithmeticOverflow { component } => formatter
                .debug_struct("ArithmeticOverflow")
                .field("component", component)
                .finish(),
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
            Self::IndexInvariant => formatter.write_str("IndexInvariant"),
        }
    }
}

impl fmt::Display for LatestMaterializationErrorV0 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(self, formatter)
    }
}

impl Error for LatestMaterializationErrorV0 {}

pub fn latest_index_entry_accounted_bytes_v0(
    schema_id: &SchemaIdV0,
    key: &[u8],
) -> Result<usize, LatestMaterializationErrorV0> {
    let mut bytes = 0;
    checked_add(
        &mut bytes,
        LENGTH_PREFIX_WIDTH,
        LatestIndexAccountingComponentV0::SchemaId,
    )?;
    checked_add(
        &mut bytes,
        schema_id.as_str().len(),
        LatestIndexAccountingComponentV0::SchemaId,
    )?;
    checked_add(
        &mut bytes,
        LENGTH_PREFIX_WIDTH,
        LatestIndexAccountingComponentV0::Key,
    )?;
    checked_add(&mut bytes, key.len(), LatestIndexAccountingComponentV0::Key)?;
    checked_add(
        &mut bytes,
        SYNTHETIC_SEQUENCE_WIDTH,
        LatestIndexAccountingComponentV0::WinningSequence,
    )?;
    Ok(bytes)
}

fn checked_add(
    current: &mut usize,
    added: usize,
    component: LatestIndexAccountingComponentV0,
) -> Result<(), LatestMaterializationErrorV0> {
    *current = current
        .checked_add(added)
        .ok_or(LatestMaterializationErrorV0::ArithmeticOverflow { component })?;
    Ok(())
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct LatestByKeyUsageV0 {
    limits: LatestByKeyLimitsV0,
    indexed_entries: usize,
    index_bytes: usize,
    accepted_events_considered: usize,
    unkeyed_events_omitted: usize,
}

impl LatestByKeyUsageV0 {
    pub const fn limits(self) -> LatestByKeyLimitsV0 {
        self.limits
    }

    pub const fn indexed_entries(self) -> usize {
        self.indexed_entries
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

/// A bounded materialized view that keeps its admission ledger immutable.
///
/// ```compile_fail
/// use decant_ir::EventEnvelopeV0;
/// use decant_state::{AdmissionStateV0, LatestByKeyConfigV0};
/// fn cannot_mutate_while_view_is_live(
///     admission: &mut AdmissionStateV0,
///     event: &EventEnvelopeV0,
///     config: LatestByKeyConfigV0,
/// ) {
///     let view = admission.materialize_latest_by_key(config).unwrap();
///     admission.apply_event(event).unwrap();
///     let _ = view.usage();
/// }
/// ```
pub struct LatestByKeyViewV0<'a> {
    admission: &'a AdmissionStateV0,
    config: LatestByKeyConfigV0,
    winners: BTreeMap<SchemaIdV0, BTreeMap<Vec<u8>, LatestWinnerV0<'a>>>,
    indexed_entries: usize,
    index_bytes: usize,
    accepted_events_considered: usize,
    unkeyed_events_omitted: usize,
}

#[derive(Clone, Copy)]
struct LatestWinnerV0<'a> {
    event: &'a EventEnvelopeV0,
}

impl AdmissionStateV0 {
    pub fn materialize_latest_by_key<'a>(
        &'a self,
        config: LatestByKeyConfigV0,
    ) -> Result<LatestByKeyViewV0<'a>, LatestMaterializationErrorV0> {
        materialize_latest_by_key_from_events(self, config, self.accepted_events.values())
    }
}

pub(crate) fn materialize_latest_by_key_from_events<'a>(
    admission: &'a AdmissionStateV0,
    config: LatestByKeyConfigV0,
    events: impl Iterator<Item = &'a EventEnvelopeV0>,
) -> Result<LatestByKeyViewV0<'a>, LatestMaterializationErrorV0> {
    let mut view = LatestByKeyViewV0 {
        admission,
        config,
        winners: BTreeMap::new(),
        indexed_entries: 0,
        index_bytes: 0,
        accepted_events_considered: 0,
        unkeyed_events_omitted: 0,
    };

    for event in events {
        view.accepted_events_considered = view.accepted_events_considered.checked_add(1).ok_or(
            LatestMaterializationErrorV0::ArithmeticOverflow {
                component: LatestIndexAccountingComponentV0::AcceptedEventsConsidered,
            },
        )?;

        let Some(key) = event.key() else {
            view.unkeyed_events_omitted = view.unkeyed_events_omitted.checked_add(1).ok_or(
                LatestMaterializationErrorV0::ArithmeticOverflow {
                    component: LatestIndexAccountingComponentV0::UnkeyedCount,
                },
            )?;
            continue;
        };

        let schema_id = event.schema_id();
        if let Some(existing) = view
            .winners
            .get_mut(schema_id)
            .and_then(|by_key| by_key.get_mut(key))
        {
            if event_rank_v0(event) > event_rank_v0(existing.event) {
                *existing = LatestWinnerV0 { event };
            }
            continue;
        }

        let entry_bytes = latest_index_entry_accounted_bytes_v0(schema_id, key)?;
        let prospective_entries = view.indexed_entries.checked_add(1).ok_or(
            LatestMaterializationErrorV0::ArithmeticOverflow {
                component: LatestIndexAccountingComponentV0::IndexLedger,
            },
        )?;
        let prospective_bytes = view.index_bytes.checked_add(entry_bytes).ok_or(
            LatestMaterializationErrorV0::ArithmeticOverflow {
                component: LatestIndexAccountingComponentV0::IndexLedger,
            },
        )?;
        if prospective_entries > config.limits.index_entry_limit {
            return Err(LatestMaterializationErrorV0::IndexEntryCapacity {
                maximum: config.limits.index_entry_limit,
                attempted: prospective_entries,
            });
        }
        if prospective_bytes > config.limits.index_byte_limit {
            return Err(LatestMaterializationErrorV0::IndexByteCapacity {
                maximum: config.limits.index_byte_limit,
                attempted: prospective_bytes,
            });
        }

        view.winners
            .entry(schema_id.clone())
            .or_default()
            .insert(key.to_vec(), LatestWinnerV0 { event });
        view.indexed_entries = prospective_entries;
        view.index_bytes = prospective_bytes;
    }

    Ok(view)
}

impl<'a> LatestByKeyViewV0<'a> {
    pub const fn config(&self) -> LatestByKeyConfigV0 {
        self.config
    }

    pub fn usage(&self) -> LatestByKeyUsageV0 {
        LatestByKeyUsageV0 {
            limits: self.config.limits,
            indexed_entries: self.indexed_entries,
            index_bytes: self.index_bytes,
            accepted_events_considered: self.accepted_events_considered,
            unkeyed_events_omitted: self.unkeyed_events_omitted,
        }
    }

    pub fn get(&self, schema_id: &SchemaIdV0, key: &[u8]) -> Option<&EventEnvelopeV0> {
        Some(self.winners.get(schema_id)?.get(key)?.event)
    }

    pub fn entries(&self) -> impl Iterator<Item = (&SchemaIdV0, &[u8], &EventEnvelopeV0)> + '_ {
        self.winners.iter().flat_map(move |(schema_id, by_key)| {
            by_key
                .iter()
                .map(move |(key, winner)| (schema_id, key.as_slice(), winner.event))
        })
    }

    pub const fn source_watermark_ns(&self) -> Option<i64> {
        self.admission.watermark_ns()
    }

    pub const fn is_source_finalized(&self) -> bool {
        self.admission.is_finalized()
    }
}

impl fmt::Debug for LatestByKeyViewV0<'_> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("LatestByKeyViewV0")
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn accounting_overflow_is_typed_and_non_mutating() {
        let mut current = usize::MAX;
        assert_eq!(
            checked_add(&mut current, 1, LatestIndexAccountingComponentV0::Key,),
            Err(LatestMaterializationErrorV0::ArithmeticOverflow {
                component: LatestIndexAccountingComponentV0::Key,
            })
        );
        assert_eq!(current, usize::MAX);
    }
}
