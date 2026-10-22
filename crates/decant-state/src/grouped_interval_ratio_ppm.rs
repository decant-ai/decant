//! Bounded retention-aware interval ratios grouped by a declared UTF-8 payload field.

use std::collections::BTreeMap;
use std::error::Error;
use std::fmt;

use decant_ir::{EventEnvelopeV0, MAX_FIELD_NAME_BYTES, ScalarKindV0, ScalarValueV0, SchemaIdV0};

use crate::{
    AdmissionStateV0, EventTimeRetentionConfigV0, EventTimeRetentionUsageV0, PARTS_PER_MILLION_V0,
};

pub const GROUPED_INTERVAL_RATIO_PPM_VERSION_V0: u16 = 0;
pub const MAX_GROUPED_INTERVAL_RATIO_GROUPS_V0: usize = 4_096;
pub const MAX_GROUPED_INTERVAL_RATIO_INDEX_BYTES_V0: usize = 1_024 * 1_024;
pub const MAX_GROUPED_INTERVAL_RATIO_EVIDENCE_V0: usize = 3;

const LENGTH_PREFIX_WIDTH: usize = 4;
const COUNT_WIDTH: usize = 8;
const EVIDENCE_RANK_WIDTH: usize = 8 + 1 + 4 + 8;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum GroupedIntervalRatioLimitFieldV0 {
    Groups,
    IndexBytes,
}

#[derive(Clone, Eq, PartialEq)]
pub enum GroupedIntervalRatioConfigErrorV0 {
    UnsupportedVersion {
        actual: u16,
    },
    InvalidFieldName,
    DuplicateFieldRoles,
    InvalidInterval,
    ZeroLimit {
        field: GroupedIntervalRatioLimitFieldV0,
    },
    AboveMaximum {
        field: GroupedIntervalRatioLimitFieldV0,
        maximum: usize,
        actual: usize,
    },
    EvidenceLimitAboveMaximum {
        maximum: usize,
        actual: usize,
    },
}

impl fmt::Debug for GroupedIntervalRatioConfigErrorV0 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::UnsupportedVersion { actual } => formatter
                .debug_struct("UnsupportedVersion")
                .field("actual", actual)
                .finish(),
            Self::InvalidFieldName => formatter.write_str("InvalidFieldName"),
            Self::DuplicateFieldRoles => formatter.write_str("DuplicateFieldRoles"),
            Self::InvalidInterval => formatter.write_str("InvalidInterval"),
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
            Self::EvidenceLimitAboveMaximum { maximum, actual } => formatter
                .debug_struct("EvidenceLimitAboveMaximum")
                .field("maximum", maximum)
                .field("actual", actual)
                .finish(),
        }
    }
}

impl fmt::Display for GroupedIntervalRatioConfigErrorV0 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(self, formatter)
    }
}

impl Error for GroupedIntervalRatioConfigErrorV0 {}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct GroupedIntervalRatioLimitsV0 {
    group_limit: usize,
    index_byte_limit: usize,
}

impl GroupedIntervalRatioLimitsV0 {
    pub fn new(
        group_limit: usize,
        index_byte_limit: usize,
    ) -> Result<Self, GroupedIntervalRatioConfigErrorV0> {
        validate_limit(
            group_limit,
            MAX_GROUPED_INTERVAL_RATIO_GROUPS_V0,
            GroupedIntervalRatioLimitFieldV0::Groups,
        )?;
        validate_limit(
            index_byte_limit,
            MAX_GROUPED_INTERVAL_RATIO_INDEX_BYTES_V0,
            GroupedIntervalRatioLimitFieldV0::IndexBytes,
        )?;
        Ok(Self {
            group_limit,
            index_byte_limit,
        })
    }

    pub const fn maximum() -> Self {
        Self {
            group_limit: MAX_GROUPED_INTERVAL_RATIO_GROUPS_V0,
            index_byte_limit: MAX_GROUPED_INTERVAL_RATIO_INDEX_BYTES_V0,
        }
    }

    pub const fn group_limit(self) -> usize {
        self.group_limit
    }

    pub const fn index_byte_limit(self) -> usize {
        self.index_byte_limit
    }
}

#[derive(Clone, Eq, PartialEq)]
pub struct GroupedIntervalRatioPpmConfigV0 {
    version: u16,
    retention: EventTimeRetentionConfigV0,
    schema_id: SchemaIdV0,
    group_field: String,
    predicate_field: String,
    predicate_value: bool,
    interval_start_ns: i64,
    interval_end_ns: i64,
    evidence_limit: usize,
    limits: GroupedIntervalRatioLimitsV0,
}

impl GroupedIntervalRatioPpmConfigV0 {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        version: u16,
        retention: EventTimeRetentionConfigV0,
        schema_id: SchemaIdV0,
        group_field: &str,
        predicate_field: &str,
        predicate_value: bool,
        interval_start_ns: i64,
        interval_end_ns: i64,
        evidence_limit: usize,
        limits: GroupedIntervalRatioLimitsV0,
    ) -> Result<Self, GroupedIntervalRatioConfigErrorV0> {
        if version != GROUPED_INTERVAL_RATIO_PPM_VERSION_V0 {
            return Err(GroupedIntervalRatioConfigErrorV0::UnsupportedVersion { actual: version });
        }
        validate_field_name(group_field)?;
        validate_field_name(predicate_field)?;
        if group_field == predicate_field {
            return Err(GroupedIntervalRatioConfigErrorV0::DuplicateFieldRoles);
        }
        if interval_start_ns >= interval_end_ns {
            return Err(GroupedIntervalRatioConfigErrorV0::InvalidInterval);
        }
        if evidence_limit > MAX_GROUPED_INTERVAL_RATIO_EVIDENCE_V0 {
            return Err(
                GroupedIntervalRatioConfigErrorV0::EvidenceLimitAboveMaximum {
                    maximum: MAX_GROUPED_INTERVAL_RATIO_EVIDENCE_V0,
                    actual: evidence_limit,
                },
            );
        }
        Ok(Self {
            version,
            retention,
            schema_id,
            group_field: group_field.to_owned(),
            predicate_field: predicate_field.to_owned(),
            predicate_value,
            interval_start_ns,
            interval_end_ns,
            evidence_limit,
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

    pub fn group_field(&self) -> &str {
        &self.group_field
    }

    pub fn predicate_field(&self) -> &str {
        &self.predicate_field
    }

    pub const fn predicate_value(&self) -> bool {
        self.predicate_value
    }

    pub const fn interval_start_ns(&self) -> i64 {
        self.interval_start_ns
    }

    pub const fn interval_end_ns(&self) -> i64 {
        self.interval_end_ns
    }

    pub const fn evidence_limit(&self) -> usize {
        self.evidence_limit
    }

    pub const fn limits(&self) -> GroupedIntervalRatioLimitsV0 {
        self.limits
    }
}

impl fmt::Debug for GroupedIntervalRatioPpmConfigV0 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("GroupedIntervalRatioPpmConfigV0")
            .field("version", &self.version)
            .field("retention", &self.retention)
            .field("schema_id", &"<redacted>")
            .field("group_field_bytes", &self.group_field.len())
            .field("predicate_field_bytes", &self.predicate_field.len())
            .field("predicate_value", &"<redacted>")
            .field("has_interval", &true)
            .field("evidence_limit", &self.evidence_limit)
            .field("limits", &self.limits)
            .finish()
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum GroupedIntervalRatioFieldRoleV0 {
    Group,
    Predicate,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum GroupedIntervalRatioFieldIssueV0 {
    Missing,
    WrongKind {
        expected: ScalarKindV0,
        actual: ScalarKindV0,
    },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum GroupedIntervalRatioAccountingV0 {
    RetainedEvents,
    OtherSchemaEvents,
    BeforeIntervalEvents,
    AtOrAfterIntervalEvents,
    DenominatorEvents,
    NumeratorEvents,
    GroupLedger,
    IndexLedger,
    Ratio,
}

#[derive(Clone, Eq, PartialEq)]
pub enum GroupedIntervalRatioMaterializationErrorV0 {
    IntervalOutsideEpoch,
    RequestedIntervalExpired,
    FieldContract {
        role: GroupedIntervalRatioFieldRoleV0,
        issue: GroupedIntervalRatioFieldIssueV0,
    },
    GroupCapacity {
        maximum: usize,
        attempted: usize,
    },
    IndexByteCapacity {
        maximum: usize,
        attempted: usize,
    },
    ArithmeticOverflow {
        component: GroupedIntervalRatioAccountingV0,
    },
    Allocation,
    UsageInvariant,
}

impl fmt::Debug for GroupedIntervalRatioMaterializationErrorV0 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::IntervalOutsideEpoch => formatter.write_str("IntervalOutsideEpoch"),
            Self::RequestedIntervalExpired => formatter.write_str("RequestedIntervalExpired"),
            Self::FieldContract { role, issue } => formatter
                .debug_struct("FieldContract")
                .field("role", role)
                .field("issue", issue)
                .finish(),
            Self::GroupCapacity { maximum, attempted } => formatter
                .debug_struct("GroupCapacity")
                .field("maximum", maximum)
                .field("attempted", attempted)
                .finish(),
            Self::IndexByteCapacity { maximum, attempted } => formatter
                .debug_struct("IndexByteCapacity")
                .field("maximum", maximum)
                .field("attempted", attempted)
                .finish(),
            Self::ArithmeticOverflow { component } => formatter
                .debug_struct("ArithmeticOverflow")
                .field("component", component)
                .finish(),
            Self::Allocation => formatter.write_str("Allocation"),
            Self::UsageInvariant => formatter.write_str("UsageInvariant"),
        }
    }
}

impl fmt::Display for GroupedIntervalRatioMaterializationErrorV0 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(self, formatter)
    }
}

impl Error for GroupedIntervalRatioMaterializationErrorV0 {}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct GroupedIntervalRatioUsageV0 {
    retained_events_considered: usize,
    other_schema_events_omitted: usize,
    before_interval_events_omitted: usize,
    at_or_after_interval_events_omitted: usize,
    denominator_events: usize,
    numerator_events: usize,
    groups: usize,
    index_bytes: usize,
}

impl GroupedIntervalRatioUsageV0 {
    pub const fn retained_events_considered(self) -> usize {
        self.retained_events_considered
    }

    pub const fn other_schema_events_omitted(self) -> usize {
        self.other_schema_events_omitted
    }

    pub const fn before_interval_events_omitted(self) -> usize {
        self.before_interval_events_omitted
    }

    pub const fn at_or_after_interval_events_omitted(self) -> usize {
        self.at_or_after_interval_events_omitted
    }

    pub const fn denominator_events(self) -> usize {
        self.denominator_events
    }

    pub const fn numerator_events(self) -> usize {
        self.numerator_events
    }

    pub const fn groups(self) -> usize {
        self.groups
    }

    pub const fn index_bytes(self) -> usize {
        self.index_bytes
    }
}

pub struct GroupedIntervalRatioAggregateV0<'a> {
    numerator: u64,
    denominator: u64,
    rate_ppm: u64,
    remainder: u64,
    numerator_evidence: Vec<&'a EventEnvelopeV0>,
    numerator_evidence_omitted: usize,
}

impl<'a> GroupedIntervalRatioAggregateV0<'a> {
    pub const fn numerator(&self) -> u64 {
        self.numerator
    }

    pub const fn denominator(&self) -> u64 {
        self.denominator
    }

    pub const fn rate_ppm(&self) -> u64 {
        self.rate_ppm
    }

    pub const fn remainder(&self) -> u64 {
        self.remainder
    }

    pub fn numerator_evidence(&self) -> impl ExactSizeIterator<Item = &'a EventEnvelopeV0> + '_ {
        self.numerator_evidence.iter().copied()
    }

    pub const fn numerator_evidence_omitted(&self) -> usize {
        self.numerator_evidence_omitted
    }
}

impl fmt::Debug for GroupedIntervalRatioAggregateV0<'_> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("GroupedIntervalRatioAggregateV0")
            .field("numerator", &self.numerator)
            .field("denominator", &self.denominator)
            .field("rate_ppm", &self.rate_ppm)
            .field("remainder", &self.remainder)
            .field("evidence_count", &self.numerator_evidence.len())
            .field("evidence_omitted", &self.numerator_evidence_omitted)
            .finish()
    }
}

pub struct GroupedIntervalRatioPpmViewV0<'a> {
    admission: &'a AdmissionStateV0,
    config: GroupedIntervalRatioPpmConfigV0,
    retention_cutoff_event_time_ns: Option<i64>,
    retention_usage: EventTimeRetentionUsageV0,
    usage: GroupedIntervalRatioUsageV0,
    groups: BTreeMap<String, GroupedIntervalRatioAggregateV0<'a>>,
}

impl AdmissionStateV0 {
    pub fn materialize_grouped_interval_ratio_ppm(
        &self,
        config: GroupedIntervalRatioPpmConfigV0,
    ) -> Result<GroupedIntervalRatioPpmViewV0<'_>, GroupedIntervalRatioMaterializationErrorV0> {
        if config.interval_start_ns < self.config().epoch_start_ns()
            || config.interval_end_ns > self.config().epoch_end_ns()
        {
            return Err(GroupedIntervalRatioMaterializationErrorV0::IntervalOutsideEpoch);
        }
        let retention = self.project_event_time_retention(config.retention);
        let cutoff = retention.cutoff_event_time_ns();
        if cutoff.is_some_and(|value| value > config.interval_start_ns) {
            return Err(GroupedIntervalRatioMaterializationErrorV0::RequestedIntervalExpired);
        }
        let retention_usage = retention.usage();
        let mut groups = BTreeMap::new();
        let mut usage = GroupedIntervalRatioUsageV0 {
            retained_events_considered: 0,
            other_schema_events_omitted: 0,
            before_interval_events_omitted: 0,
            at_or_after_interval_events_omitted: 0,
            denominator_events: 0,
            numerator_events: 0,
            groups: 0,
            index_bytes: 0,
        };

        for event in retention.events() {
            increment(
                &mut usage.retained_events_considered,
                GroupedIntervalRatioAccountingV0::RetainedEvents,
            )?;
            if event.schema_id() != &config.schema_id {
                increment(
                    &mut usage.other_schema_events_omitted,
                    GroupedIntervalRatioAccountingV0::OtherSchemaEvents,
                )?;
                continue;
            }
            let group = required_field(
                event,
                &config.group_field,
                GroupedIntervalRatioFieldRoleV0::Group,
            )?;
            let Some(group) = group.as_utf8() else {
                return Err(GroupedIntervalRatioMaterializationErrorV0::FieldContract {
                    role: GroupedIntervalRatioFieldRoleV0::Group,
                    issue: GroupedIntervalRatioFieldIssueV0::WrongKind {
                        expected: ScalarKindV0::Utf8,
                        actual: group.kind(),
                    },
                });
            };
            let predicate = required_field(
                event,
                &config.predicate_field,
                GroupedIntervalRatioFieldRoleV0::Predicate,
            )?;
            let Some(predicate) = predicate.as_bool() else {
                return Err(GroupedIntervalRatioMaterializationErrorV0::FieldContract {
                    role: GroupedIntervalRatioFieldRoleV0::Predicate,
                    issue: GroupedIntervalRatioFieldIssueV0::WrongKind {
                        expected: ScalarKindV0::Bool,
                        actual: predicate.kind(),
                    },
                });
            };
            if event.event_time_ns() < config.interval_start_ns {
                increment(
                    &mut usage.before_interval_events_omitted,
                    GroupedIntervalRatioAccountingV0::BeforeIntervalEvents,
                )?;
                continue;
            }
            if event.event_time_ns() >= config.interval_end_ns {
                increment(
                    &mut usage.at_or_after_interval_events_omitted,
                    GroupedIntervalRatioAccountingV0::AtOrAfterIntervalEvents,
                )?;
                continue;
            }

            if !groups.contains_key(group) {
                let attempted_groups = usage.groups.checked_add(1).ok_or(
                    GroupedIntervalRatioMaterializationErrorV0::ArithmeticOverflow {
                        component: GroupedIntervalRatioAccountingV0::GroupLedger,
                    },
                )?;
                if attempted_groups > config.limits.group_limit {
                    return Err(GroupedIntervalRatioMaterializationErrorV0::GroupCapacity {
                        maximum: config.limits.group_limit,
                        attempted: attempted_groups,
                    });
                }
                let entry_bytes = group_accounted_bytes(group, config.evidence_limit)?;
                let attempted_bytes = usage.index_bytes.checked_add(entry_bytes).ok_or(
                    GroupedIntervalRatioMaterializationErrorV0::ArithmeticOverflow {
                        component: GroupedIntervalRatioAccountingV0::IndexLedger,
                    },
                )?;
                if attempted_bytes > config.limits.index_byte_limit {
                    return Err(
                        GroupedIntervalRatioMaterializationErrorV0::IndexByteCapacity {
                            maximum: config.limits.index_byte_limit,
                            attempted: attempted_bytes,
                        },
                    );
                }
                let mut evidence = Vec::new();
                evidence
                    .try_reserve_exact(config.evidence_limit)
                    .map_err(|_| GroupedIntervalRatioMaterializationErrorV0::Allocation)?;
                groups.insert(
                    group.to_owned(),
                    GroupedIntervalRatioAggregateV0 {
                        numerator: 0,
                        denominator: 0,
                        rate_ppm: 0,
                        remainder: 0,
                        numerator_evidence: evidence,
                        numerator_evidence_omitted: 0,
                    },
                );
                usage.groups = attempted_groups;
                usage.index_bytes = attempted_bytes;
            }

            let aggregate = groups
                .get_mut(group)
                .ok_or(GroupedIntervalRatioMaterializationErrorV0::UsageInvariant)?;
            aggregate.denominator = aggregate.denominator.checked_add(1).ok_or(
                GroupedIntervalRatioMaterializationErrorV0::ArithmeticOverflow {
                    component: GroupedIntervalRatioAccountingV0::DenominatorEvents,
                },
            )?;
            increment(
                &mut usage.denominator_events,
                GroupedIntervalRatioAccountingV0::DenominatorEvents,
            )?;
            if predicate == config.predicate_value {
                aggregate.numerator = aggregate.numerator.checked_add(1).ok_or(
                    GroupedIntervalRatioMaterializationErrorV0::ArithmeticOverflow {
                        component: GroupedIntervalRatioAccountingV0::NumeratorEvents,
                    },
                )?;
                increment(
                    &mut usage.numerator_events,
                    GroupedIntervalRatioAccountingV0::NumeratorEvents,
                )?;
                select_evidence(
                    &mut aggregate.numerator_evidence,
                    config.evidence_limit,
                    event,
                );
            }
        }

        for aggregate in groups.values_mut() {
            let scaled = aggregate
                .numerator
                .checked_mul(PARTS_PER_MILLION_V0)
                .ok_or(
                    GroupedIntervalRatioMaterializationErrorV0::ArithmeticOverflow {
                        component: GroupedIntervalRatioAccountingV0::Ratio,
                    },
                )?;
            aggregate.rate_ppm = scaled / aggregate.denominator;
            aggregate.remainder = scaled % aggregate.denominator;
            aggregate.numerator_evidence_omitted = usize::try_from(aggregate.numerator)
                .map_err(|_| GroupedIntervalRatioMaterializationErrorV0::UsageInvariant)?
                .checked_sub(aggregate.numerator_evidence.len())
                .ok_or(GroupedIntervalRatioMaterializationErrorV0::UsageInvariant)?;
        }
        let classified = usage
            .other_schema_events_omitted
            .checked_add(usage.before_interval_events_omitted)
            .and_then(|value| value.checked_add(usage.at_or_after_interval_events_omitted))
            .and_then(|value| value.checked_add(usage.denominator_events))
            .ok_or(GroupedIntervalRatioMaterializationErrorV0::UsageInvariant)?;
        if classified != usage.retained_events_considered
            || groups.len() != usage.groups
            || usage.numerator_events > usage.denominator_events
        {
            return Err(GroupedIntervalRatioMaterializationErrorV0::UsageInvariant);
        }

        Ok(GroupedIntervalRatioPpmViewV0 {
            admission: self,
            config,
            retention_cutoff_event_time_ns: cutoff,
            retention_usage,
            usage,
            groups,
        })
    }
}

impl<'a> GroupedIntervalRatioPpmViewV0<'a> {
    pub const fn config(&self) -> &GroupedIntervalRatioPpmConfigV0 {
        &self.config
    }

    pub const fn retention_cutoff_event_time_ns(&self) -> Option<i64> {
        self.retention_cutoff_event_time_ns
    }

    pub const fn retention_usage(&self) -> EventTimeRetentionUsageV0 {
        self.retention_usage
    }

    pub const fn usage(&self) -> GroupedIntervalRatioUsageV0 {
        self.usage
    }

    pub fn groups(
        &self,
    ) -> impl ExactSizeIterator<Item = (&str, &GroupedIntervalRatioAggregateV0<'a>)> + '_ {
        self.groups
            .iter()
            .map(|(group, aggregate)| (group.as_str(), aggregate))
    }

    pub const fn source_watermark_ns(&self) -> Option<i64> {
        self.admission.watermark_ns()
    }

    pub const fn is_source_finalized(&self) -> bool {
        self.admission.is_finalized()
    }
}

impl fmt::Debug for GroupedIntervalRatioPpmViewV0<'_> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("GroupedIntervalRatioPpmViewV0")
            .field("config", &self.config)
            .field("retention_usage", &self.retention_usage)
            .field("usage", &self.usage)
            .field("has_cutoff", &self.retention_cutoff_event_time_ns.is_some())
            .field("has_watermark", &self.source_watermark_ns().is_some())
            .field("source_finalized", &self.is_source_finalized())
            .finish()
    }
}

fn validate_limit(
    value: usize,
    maximum: usize,
    field: GroupedIntervalRatioLimitFieldV0,
) -> Result<(), GroupedIntervalRatioConfigErrorV0> {
    if value == 0 {
        return Err(GroupedIntervalRatioConfigErrorV0::ZeroLimit { field });
    }
    if value > maximum {
        return Err(GroupedIntervalRatioConfigErrorV0::AboveMaximum {
            field,
            maximum,
            actual: value,
        });
    }
    Ok(())
}

fn validate_field_name(value: &str) -> Result<(), GroupedIntervalRatioConfigErrorV0> {
    if value.is_empty()
        || value.len() > MAX_FIELD_NAME_BYTES
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-' | b'.'))
    {
        return Err(GroupedIntervalRatioConfigErrorV0::InvalidFieldName);
    }
    Ok(())
}

fn required_field<'a>(
    event: &'a EventEnvelopeV0,
    field: &str,
    role: GroupedIntervalRatioFieldRoleV0,
) -> Result<&'a ScalarValueV0, GroupedIntervalRatioMaterializationErrorV0> {
    event
        .payload()
        .fields()
        .find_map(|(candidate, value)| (candidate == field).then_some(value))
        .ok_or(GroupedIntervalRatioMaterializationErrorV0::FieldContract {
            role,
            issue: GroupedIntervalRatioFieldIssueV0::Missing,
        })
}

fn group_accounted_bytes(
    group: &str,
    evidence_limit: usize,
) -> Result<usize, GroupedIntervalRatioMaterializationErrorV0> {
    LENGTH_PREFIX_WIDTH
        .checked_add(group.len())
        .and_then(|value| value.checked_add(COUNT_WIDTH * 2))
        .and_then(|value| value.checked_add(EVIDENCE_RANK_WIDTH * evidence_limit))
        .ok_or(
            GroupedIntervalRatioMaterializationErrorV0::ArithmeticOverflow {
                component: GroupedIntervalRatioAccountingV0::IndexLedger,
            },
        )
}

fn increment(
    value: &mut usize,
    component: GroupedIntervalRatioAccountingV0,
) -> Result<(), GroupedIntervalRatioMaterializationErrorV0> {
    *value = value
        .checked_add(1)
        .ok_or(GroupedIntervalRatioMaterializationErrorV0::ArithmeticOverflow { component })?;
    Ok(())
}

fn select_evidence<'a>(
    selected: &mut Vec<&'a EventEnvelopeV0>,
    limit: usize,
    event: &'a EventEnvelopeV0,
) {
    if limit == 0 {
        return;
    }
    if selected.len() < limit {
        selected.push(event);
    } else {
        let event_rank = (event.event_time_ns(), event.source_position());
        let last_rank = selected
            .last()
            .map(|candidate| (candidate.event_time_ns(), candidate.source_position()));
        if last_rank.is_some_and(|rank| event_rank < rank) {
            let last = selected.len() - 1;
            selected[last] = event;
        } else {
            return;
        }
    }
    selected.sort_by_key(|candidate| (candidate.event_time_ns(), candidate.source_position()));
}
