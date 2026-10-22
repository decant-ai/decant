//! Bounded tumbling count-window materialization over admitted events.

use std::collections::BTreeMap;
use std::error::Error;
use std::fmt;

use decant_ir::{EventEnvelopeV0, SchemaIdV0, SourcePositionV0};

use crate::AdmissionStateV0;

pub const TUMBLING_COUNT_VERSION_V0: u16 = 0;
pub const MAX_TUMBLING_COUNT_BUCKETS_V0: usize = 100_000;
pub const MAX_TUMBLING_COUNT_INDEX_BYTES_V0: usize = 64 * 1_024 * 1_024;

const LENGTH_PREFIX_WIDTH: usize = 4;
const WINDOW_ORDINAL_WIDTH: usize = 8;
const CONTRIBUTOR_COUNT_PREFIX_WIDTH: usize = 4;
const CONTRIBUTOR_SEQUENCE_WIDTH: usize = 8;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TumblingCountLimitFieldV0 {
    Buckets,
    IndexBytes,
}

#[derive(Clone, Eq, PartialEq)]
pub enum TumblingCountConfigErrorV0 {
    UnsupportedVersion {
        actual: u16,
    },
    ZeroWindowWidth,
    ZeroLimit {
        field: TumblingCountLimitFieldV0,
    },
    AboveMaximum {
        field: TumblingCountLimitFieldV0,
        maximum: usize,
        actual: usize,
    },
}

impl fmt::Debug for TumblingCountConfigErrorV0 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::UnsupportedVersion { actual } => formatter
                .debug_struct("UnsupportedVersion")
                .field("actual", actual)
                .finish(),
            Self::ZeroWindowWidth => formatter.write_str("ZeroWindowWidth"),
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

impl fmt::Display for TumblingCountConfigErrorV0 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(self, formatter)
    }
}

impl Error for TumblingCountConfigErrorV0 {}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct TumblingCountLimitsV0 {
    bucket_limit: usize,
    index_byte_limit: usize,
}

impl TumblingCountLimitsV0 {
    pub fn new(
        bucket_limit: usize,
        index_byte_limit: usize,
    ) -> Result<Self, TumblingCountConfigErrorV0> {
        validate_limit(
            bucket_limit,
            MAX_TUMBLING_COUNT_BUCKETS_V0,
            TumblingCountLimitFieldV0::Buckets,
        )?;
        validate_limit(
            index_byte_limit,
            MAX_TUMBLING_COUNT_INDEX_BYTES_V0,
            TumblingCountLimitFieldV0::IndexBytes,
        )?;
        Ok(Self {
            bucket_limit,
            index_byte_limit,
        })
    }

    pub const fn maximum() -> Self {
        Self {
            bucket_limit: MAX_TUMBLING_COUNT_BUCKETS_V0,
            index_byte_limit: MAX_TUMBLING_COUNT_INDEX_BYTES_V0,
        }
    }

    pub const fn bucket_limit(self) -> usize {
        self.bucket_limit
    }

    pub const fn index_byte_limit(self) -> usize {
        self.index_byte_limit
    }
}

fn validate_limit(
    actual: usize,
    maximum: usize,
    field: TumblingCountLimitFieldV0,
) -> Result<(), TumblingCountConfigErrorV0> {
    if actual == 0 {
        return Err(TumblingCountConfigErrorV0::ZeroLimit { field });
    }
    if actual > maximum {
        return Err(TumblingCountConfigErrorV0::AboveMaximum {
            field,
            maximum,
            actual,
        });
    }
    Ok(())
}

#[derive(Clone, Copy, Eq, PartialEq)]
pub struct TumblingCountConfigV0 {
    version: u16,
    window_width_ns: u64,
    limits: TumblingCountLimitsV0,
}

impl TumblingCountConfigV0 {
    pub fn new(
        version: u16,
        window_width_ns: u64,
        limits: TumblingCountLimitsV0,
    ) -> Result<Self, TumblingCountConfigErrorV0> {
        if version != TUMBLING_COUNT_VERSION_V0 {
            return Err(TumblingCountConfigErrorV0::UnsupportedVersion { actual: version });
        }
        if window_width_ns == 0 {
            return Err(TumblingCountConfigErrorV0::ZeroWindowWidth);
        }
        Ok(Self {
            version,
            window_width_ns,
            limits,
        })
    }

    pub const fn version(self) -> u16 {
        self.version
    }

    pub const fn window_width_ns(self) -> u64 {
        self.window_width_ns
    }

    pub const fn limits(self) -> TumblingCountLimitsV0 {
        self.limits
    }
}

impl fmt::Debug for TumblingCountConfigV0 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("TumblingCountConfigV0")
            .field("version", &self.version)
            .field("window_width_ns", &self.window_width_ns)
            .field("limits", &self.limits)
            .finish()
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CountIndexAccountingComponentV0 {
    AcceptedEventsConsidered,
    SchemaId,
    Key,
    WindowOrdinal,
    ContributorCount,
    ContributorSequence,
    BucketLedger,
    ContributionLedger,
    BucketCount,
    UnkeyedCount,
    WindowArithmetic,
}

#[derive(Clone, Eq, PartialEq)]
pub enum CountMaterializationErrorV0 {
    ArithmeticOverflow {
        component: CountIndexAccountingComponentV0,
    },
    AdmissionEpochInvariant,
    IndexInvariant,
    BucketCapacity {
        maximum: usize,
        attempted: usize,
    },
    IndexByteCapacity {
        maximum: usize,
        attempted: usize,
    },
}

impl fmt::Debug for CountMaterializationErrorV0 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ArithmeticOverflow { component } => formatter
                .debug_struct("ArithmeticOverflow")
                .field("component", component)
                .finish(),
            Self::AdmissionEpochInvariant => formatter.write_str("AdmissionEpochInvariant"),
            Self::IndexInvariant => formatter.write_str("IndexInvariant"),
            Self::BucketCapacity { maximum, attempted } => formatter
                .debug_struct("BucketCapacity")
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

impl fmt::Display for CountMaterializationErrorV0 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(self, formatter)
    }
}

impl Error for CountMaterializationErrorV0 {}

pub fn tumbling_count_bucket_accounted_bytes_v0(
    schema_id: &SchemaIdV0,
    key: &[u8],
) -> Result<usize, CountMaterializationErrorV0> {
    let mut bytes = 0;
    checked_add(
        &mut bytes,
        LENGTH_PREFIX_WIDTH,
        CountIndexAccountingComponentV0::SchemaId,
    )?;
    checked_add(
        &mut bytes,
        schema_id.as_str().len(),
        CountIndexAccountingComponentV0::SchemaId,
    )?;
    checked_add(
        &mut bytes,
        LENGTH_PREFIX_WIDTH,
        CountIndexAccountingComponentV0::Key,
    )?;
    checked_add(&mut bytes, key.len(), CountIndexAccountingComponentV0::Key)?;
    checked_add(
        &mut bytes,
        WINDOW_ORDINAL_WIDTH,
        CountIndexAccountingComponentV0::WindowOrdinal,
    )?;
    checked_add(
        &mut bytes,
        CONTRIBUTOR_COUNT_PREFIX_WIDTH,
        CountIndexAccountingComponentV0::ContributorCount,
    )?;
    Ok(bytes)
}

pub const fn tumbling_count_contribution_accounted_bytes_v0() -> usize {
    CONTRIBUTOR_SEQUENCE_WIDTH
}

fn checked_add(
    current: &mut usize,
    added: usize,
    component: CountIndexAccountingComponentV0,
) -> Result<(), CountMaterializationErrorV0> {
    *current = current
        .checked_add(added)
        .ok_or(CountMaterializationErrorV0::ArithmeticOverflow { component })?;
    Ok(())
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct TumblingCountUsageV0 {
    limits: TumblingCountLimitsV0,
    represented_buckets: usize,
    contributions: usize,
    index_bytes: usize,
    accepted_events_considered: usize,
    unkeyed_events_omitted: usize,
}

impl TumblingCountUsageV0 {
    pub const fn limits(self) -> TumblingCountLimitsV0 {
        self.limits
    }

    pub const fn represented_buckets(self) -> usize {
        self.represented_buckets
    }

    pub const fn contributions(self) -> usize {
        self.contributions
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

#[derive(Clone, Copy, Eq, PartialEq)]
pub struct CountWindowObservationV0 {
    window_ordinal: u64,
    window_start_ns: i64,
    window_end_ns: i64,
    count: u64,
    finalized: bool,
}

impl CountWindowObservationV0 {
    pub const fn window_ordinal(self) -> u64 {
        self.window_ordinal
    }

    pub const fn window_start_ns(self) -> i64 {
        self.window_start_ns
    }

    pub const fn window_end_ns(self) -> i64 {
        self.window_end_ns
    }

    pub const fn count(self) -> u64 {
        self.count
    }

    pub const fn is_finalized(self) -> bool {
        self.finalized
    }
}

impl fmt::Debug for CountWindowObservationV0 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("CountWindowObservationV0")
            .field("window_ordinal", &"<redacted>")
            .field("window_start_ns", &"<redacted>")
            .field("window_end_ns", &"<redacted>")
            .field("count", &self.count)
            .field("finalized", &self.finalized)
            .finish()
    }
}

/// A bounded count-window view that keeps its admission ledger immutable.
///
/// ```compile_fail
/// use decant_ir::EventEnvelopeV0;
/// use decant_state::{AdmissionStateV0, TumblingCountConfigV0};
/// fn cannot_mutate_while_view_is_live(
///     admission: &mut AdmissionStateV0,
///     event: &EventEnvelopeV0,
///     config: TumblingCountConfigV0,
/// ) {
///     let view = admission.materialize_tumbling_count(config).unwrap();
///     admission.apply_event(event).unwrap();
///     let _ = view.usage();
/// }
/// ```
pub struct TumblingCountViewV0<'a> {
    admission: &'a AdmissionStateV0,
    config: TumblingCountConfigV0,
    buckets: BTreeMap<SchemaIdV0, BTreeMap<Vec<u8>, BTreeMap<u64, CountBucketV0<'a>>>>,
    represented_buckets: usize,
    contributions: usize,
    index_bytes: usize,
    accepted_events_considered: usize,
    unkeyed_events_omitted: usize,
}

struct CountBucketV0<'a> {
    window_start_ns: i64,
    window_end_ns: i64,
    count: u64,
    contributors: Vec<CountContributionV0<'a>>,
}

#[derive(Clone, Copy)]
struct CountContributionV0<'a> {
    event: &'a EventEnvelopeV0,
}

impl AdmissionStateV0 {
    pub fn materialize_tumbling_count<'a>(
        &'a self,
        config: TumblingCountConfigV0,
    ) -> Result<TumblingCountViewV0<'a>, CountMaterializationErrorV0> {
        materialize_tumbling_count_from_events(self, config, self.accepted_events.values())
    }
}

pub(crate) fn materialize_tumbling_count_from_events<'a>(
    admission: &'a AdmissionStateV0,
    config: TumblingCountConfigV0,
    events: impl Iterator<Item = &'a EventEnvelopeV0>,
) -> Result<TumblingCountViewV0<'a>, CountMaterializationErrorV0> {
    let mut view = TumblingCountViewV0 {
        admission,
        config,
        buckets: BTreeMap::new(),
        represented_buckets: 0,
        contributions: 0,
        index_bytes: 0,
        accepted_events_considered: 0,
        unkeyed_events_omitted: 0,
    };

    for event in events {
        view.accepted_events_considered = view.accepted_events_considered.checked_add(1).ok_or(
            CountMaterializationErrorV0::ArithmeticOverflow {
                component: CountIndexAccountingComponentV0::AcceptedEventsConsidered,
            },
        )?;

        let Some(key) = event.key() else {
            view.unkeyed_events_omitted = view.unkeyed_events_omitted.checked_add(1).ok_or(
                CountMaterializationErrorV0::ArithmeticOverflow {
                    component: CountIndexAccountingComponentV0::UnkeyedCount,
                },
            )?;
            continue;
        };

        let schema_id = event.schema_id();
        let (window_ordinal, window_start_ns, window_end_ns) = window_coordinate(
            admission.config.epoch_start_ns(),
            admission.config.epoch_end_ns(),
            event.event_time_ns(),
            config.window_width_ns,
        )?;
        let existing_count = view
            .buckets
            .get(schema_id)
            .and_then(|by_key| by_key.get(key))
            .and_then(|by_window| by_window.get(&window_ordinal))
            .map(|bucket| bucket.count);

        let prospective_contributions = view.contributions.checked_add(1).ok_or(
            CountMaterializationErrorV0::ArithmeticOverflow {
                component: CountIndexAccountingComponentV0::ContributionLedger,
            },
        )?;

        if let Some(count) = existing_count {
            let prospective_count =
                count
                    .checked_add(1)
                    .ok_or(CountMaterializationErrorV0::ArithmeticOverflow {
                        component: CountIndexAccountingComponentV0::BucketCount,
                    })?;
            let prospective_bytes = view
                .index_bytes
                .checked_add(CONTRIBUTOR_SEQUENCE_WIDTH)
                .ok_or(CountMaterializationErrorV0::ArithmeticOverflow {
                    component: CountIndexAccountingComponentV0::ContributionLedger,
                })?;
            if prospective_bytes > config.limits.index_byte_limit {
                return Err(CountMaterializationErrorV0::IndexByteCapacity {
                    maximum: config.limits.index_byte_limit,
                    attempted: prospective_bytes,
                });
            }

            let Some(bucket) = view
                .buckets
                .get_mut(schema_id)
                .and_then(|by_key| by_key.get_mut(key))
                .and_then(|by_window| by_window.get_mut(&window_ordinal))
            else {
                return Err(CountMaterializationErrorV0::IndexInvariant);
            };
            bucket.contributors.push(CountContributionV0 { event });
            bucket.count = prospective_count;
            view.contributions = prospective_contributions;
            view.index_bytes = prospective_bytes;
            continue;
        }

        let base_bytes = tumbling_count_bucket_accounted_bytes_v0(schema_id, key)?;
        let entry_bytes = base_bytes.checked_add(CONTRIBUTOR_SEQUENCE_WIDTH).ok_or(
            CountMaterializationErrorV0::ArithmeticOverflow {
                component: CountIndexAccountingComponentV0::ContributionLedger,
            },
        )?;
        let prospective_buckets = view.represented_buckets.checked_add(1).ok_or(
            CountMaterializationErrorV0::ArithmeticOverflow {
                component: CountIndexAccountingComponentV0::BucketLedger,
            },
        )?;
        let prospective_bytes = view.index_bytes.checked_add(entry_bytes).ok_or(
            CountMaterializationErrorV0::ArithmeticOverflow {
                component: CountIndexAccountingComponentV0::BucketLedger,
            },
        )?;
        if prospective_buckets > config.limits.bucket_limit {
            return Err(CountMaterializationErrorV0::BucketCapacity {
                maximum: config.limits.bucket_limit,
                attempted: prospective_buckets,
            });
        }
        if prospective_bytes > config.limits.index_byte_limit {
            return Err(CountMaterializationErrorV0::IndexByteCapacity {
                maximum: config.limits.index_byte_limit,
                attempted: prospective_bytes,
            });
        }

        view.buckets
            .entry(schema_id.clone())
            .or_default()
            .entry(key.to_vec())
            .or_default()
            .insert(
                window_ordinal,
                CountBucketV0 {
                    window_start_ns,
                    window_end_ns,
                    count: 1,
                    contributors: vec![CountContributionV0 { event }],
                },
            );
        view.represented_buckets = prospective_buckets;
        view.contributions = prospective_contributions;
        view.index_bytes = prospective_bytes;
    }

    Ok(view)
}

impl<'a> TumblingCountViewV0<'a> {
    pub const fn config(&self) -> TumblingCountConfigV0 {
        self.config
    }

    pub fn usage(&self) -> TumblingCountUsageV0 {
        TumblingCountUsageV0 {
            limits: self.config.limits,
            represented_buckets: self.represented_buckets,
            contributions: self.contributions,
            index_bytes: self.index_bytes,
            accepted_events_considered: self.accepted_events_considered,
            unkeyed_events_omitted: self.unkeyed_events_omitted,
        }
    }

    pub fn get(
        &self,
        schema_id: &SchemaIdV0,
        key: &[u8],
        window_ordinal: u64,
    ) -> Option<CountWindowObservationV0> {
        let bucket = self
            .buckets
            .get(schema_id)?
            .get(key)?
            .get(&window_ordinal)?;
        Some(self.observe(window_ordinal, bucket))
    }

    pub fn entries(
        &self,
    ) -> impl Iterator<Item = (&SchemaIdV0, &[u8], CountWindowObservationV0)> + '_ {
        self.buckets.iter().flat_map(move |(schema_id, by_key)| {
            by_key.iter().flat_map(move |(key, by_window)| {
                by_window.iter().map(move |(window_ordinal, bucket)| {
                    (
                        schema_id,
                        key.as_slice(),
                        self.observe(*window_ordinal, bucket),
                    )
                })
            })
        })
    }

    pub fn contributors(
        &self,
        schema_id: &SchemaIdV0,
        key: &[u8],
        window_ordinal: u64,
    ) -> Option<impl ExactSizeIterator<Item = (SourcePositionV0, &'a EventEnvelopeV0)> + '_> {
        let contributors = &self
            .buckets
            .get(schema_id)?
            .get(key)?
            .get(&window_ordinal)?
            .contributors;
        Some(
            contributors
                .iter()
                .map(|contribution| (contribution.event.source_position(), contribution.event)),
        )
    }

    pub const fn source_watermark_ns(&self) -> Option<i64> {
        self.admission.watermark_ns()
    }

    pub const fn is_source_finalized(&self) -> bool {
        self.admission.is_finalized()
    }

    fn observe(&self, window_ordinal: u64, bucket: &CountBucketV0<'_>) -> CountWindowObservationV0 {
        CountWindowObservationV0 {
            window_ordinal,
            window_start_ns: bucket.window_start_ns,
            window_end_ns: bucket.window_end_ns,
            count: bucket.count,
            finalized: self
                .source_watermark_ns()
                .is_some_and(|watermark_ns| watermark_ns >= bucket.window_end_ns),
        }
    }
}

impl fmt::Debug for TumblingCountViewV0<'_> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("TumblingCountViewV0")
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

fn window_coordinate(
    epoch_start_ns: i64,
    epoch_end_ns: i64,
    event_time_ns: i64,
    window_width_ns: u64,
) -> Result<(u64, i64, i64), CountMaterializationErrorV0> {
    if event_time_ns < epoch_start_ns || event_time_ns >= epoch_end_ns {
        return Err(CountMaterializationErrorV0::AdmissionEpochInvariant);
    }

    let offset = i128::from(event_time_ns)
        .checked_sub(i128::from(epoch_start_ns))
        .ok_or(CountMaterializationErrorV0::ArithmeticOverflow {
            component: CountIndexAccountingComponentV0::WindowArithmetic,
        })?;
    let width = i128::from(window_width_ns);
    let ordinal_wide = offset / width;
    let window_ordinal = u64::try_from(ordinal_wide).map_err(|_| {
        CountMaterializationErrorV0::ArithmeticOverflow {
            component: CountIndexAccountingComponentV0::WindowArithmetic,
        }
    })?;
    let window_offset =
        ordinal_wide
            .checked_mul(width)
            .ok_or(CountMaterializationErrorV0::ArithmeticOverflow {
                component: CountIndexAccountingComponentV0::WindowArithmetic,
            })?;
    let window_start_wide = i128::from(epoch_start_ns)
        .checked_add(window_offset)
        .ok_or(CountMaterializationErrorV0::ArithmeticOverflow {
            component: CountIndexAccountingComponentV0::WindowArithmetic,
        })?;
    let unclamped_end = window_start_wide.checked_add(width).ok_or(
        CountMaterializationErrorV0::ArithmeticOverflow {
            component: CountIndexAccountingComponentV0::WindowArithmetic,
        },
    )?;
    let window_end_wide = unclamped_end.min(i128::from(epoch_end_ns));
    let window_start_ns = i64::try_from(window_start_wide).map_err(|_| {
        CountMaterializationErrorV0::ArithmeticOverflow {
            component: CountIndexAccountingComponentV0::WindowArithmetic,
        }
    })?;
    let window_end_ns = i64::try_from(window_end_wide).map_err(|_| {
        CountMaterializationErrorV0::ArithmeticOverflow {
            component: CountIndexAccountingComponentV0::WindowArithmetic,
        }
    })?;

    Ok((window_ordinal, window_start_ns, window_end_ns))
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
                CountIndexAccountingComponentV0::ContributionLedger,
            ),
            Err(CountMaterializationErrorV0::ArithmeticOverflow {
                component: CountIndexAccountingComponentV0::ContributionLedger,
            })
        );
        assert_eq!(current, usize::MAX);
    }

    #[test]
    fn invalid_epoch_membership_is_typed() {
        assert_eq!(
            window_coordinate(0, 10, 10, 1),
            Err(CountMaterializationErrorV0::AdmissionEpochInvariant)
        );
    }
}
