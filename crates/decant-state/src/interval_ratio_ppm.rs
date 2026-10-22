//! Bounded retention-aware integer ratio materialization over one exact interval.

use std::error::Error;
use std::fmt;

use decant_ir::{EventEnvelopeV0, MAX_FIELD_NAME_BYTES, MAX_KEY_BYTES, ScalarValueV0, SchemaIdV0};

use crate::{
    AdmissionStateV0, EventTimeRetentionConfigV0, EventTimeRetentionUsageV0, event_rank_v0,
};

pub const INTERVAL_RATIO_PPM_VERSION_V0: u16 = 0;
pub const PARTS_PER_MILLION_V0: u64 = 1_000_000;
pub const MAX_INTERVAL_RATIO_EXAMPLES_V0: usize = 16;

#[derive(Clone, Eq, PartialEq)]
pub enum IntervalRatioPpmConfigErrorV0 {
    UnsupportedVersion {
        actual: u16,
    },
    KeyTooLong {
        maximum: usize,
        actual: usize,
    },
    InvalidFieldNameLength {
        minimum: usize,
        maximum: usize,
        actual: usize,
    },
    InvalidFieldNameSyntax,
    InvalidInterval,
    ExampleLimitAboveMaximum {
        maximum: usize,
        actual: usize,
    },
}

impl fmt::Debug for IntervalRatioPpmConfigErrorV0 {
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
            Self::InvalidFieldNameLength {
                minimum,
                maximum,
                actual,
            } => formatter
                .debug_struct("InvalidFieldNameLength")
                .field("minimum", minimum)
                .field("maximum", maximum)
                .field("actual", actual)
                .finish(),
            Self::InvalidFieldNameSyntax => formatter.write_str("InvalidFieldNameSyntax"),
            Self::InvalidInterval => formatter.write_str("InvalidInterval"),
            Self::ExampleLimitAboveMaximum { maximum, actual } => formatter
                .debug_struct("ExampleLimitAboveMaximum")
                .field("maximum", maximum)
                .field("actual", actual)
                .finish(),
        }
    }
}

impl fmt::Display for IntervalRatioPpmConfigErrorV0 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(self, formatter)
    }
}

impl Error for IntervalRatioPpmConfigErrorV0 {}

#[derive(Clone, Eq, PartialEq)]
pub struct IntervalRatioPpmConfigV0 {
    version: u16,
    retention: EventTimeRetentionConfigV0,
    schema_id: SchemaIdV0,
    key: Vec<u8>,
    predicate_field: String,
    predicate_value: ScalarValueV0,
    interval_start_ns: i64,
    interval_end_ns: i64,
    example_limit: usize,
}

impl IntervalRatioPpmConfigV0 {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        version: u16,
        retention: EventTimeRetentionConfigV0,
        schema_id: SchemaIdV0,
        key: &[u8],
        predicate_field: &str,
        predicate_value: ScalarValueV0,
        interval_start_ns: i64,
        interval_end_ns: i64,
        example_limit: usize,
    ) -> Result<Self, IntervalRatioPpmConfigErrorV0> {
        if version != INTERVAL_RATIO_PPM_VERSION_V0 {
            return Err(IntervalRatioPpmConfigErrorV0::UnsupportedVersion { actual: version });
        }
        if key.len() > MAX_KEY_BYTES {
            return Err(IntervalRatioPpmConfigErrorV0::KeyTooLong {
                maximum: MAX_KEY_BYTES,
                actual: key.len(),
            });
        }
        validate_field_name(predicate_field)?;
        if interval_start_ns >= interval_end_ns {
            return Err(IntervalRatioPpmConfigErrorV0::InvalidInterval);
        }
        if example_limit > MAX_INTERVAL_RATIO_EXAMPLES_V0 {
            return Err(IntervalRatioPpmConfigErrorV0::ExampleLimitAboveMaximum {
                maximum: MAX_INTERVAL_RATIO_EXAMPLES_V0,
                actual: example_limit,
            });
        }
        Ok(Self {
            version,
            retention,
            schema_id,
            key: key.to_vec(),
            predicate_field: predicate_field.to_owned(),
            predicate_value,
            interval_start_ns,
            interval_end_ns,
            example_limit,
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

    pub fn key(&self) -> &[u8] {
        &self.key
    }

    pub fn predicate_field(&self) -> &str {
        &self.predicate_field
    }

    pub const fn predicate_value(&self) -> &ScalarValueV0 {
        &self.predicate_value
    }

    pub const fn interval_start_ns(&self) -> i64 {
        self.interval_start_ns
    }

    pub const fn interval_end_ns(&self) -> i64 {
        self.interval_end_ns
    }

    pub const fn example_limit(&self) -> usize {
        self.example_limit
    }
}

impl fmt::Debug for IntervalRatioPpmConfigV0 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("IntervalRatioPpmConfigV0")
            .field("version", &self.version)
            .field("retention", &self.retention)
            .field("schema_id", &"<redacted>")
            .field("key_byte_len", &self.key.len())
            .field("predicate_field", &"<redacted>")
            .field("predicate_value", &"<redacted>")
            .field("has_interval_start", &true)
            .field("has_interval_end", &true)
            .field("example_limit", &self.example_limit)
            .finish()
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum IntervalRatioPpmAccountingComponentV0 {
    RetainedEventsConsidered,
    OtherSchemaEvents,
    MatchingSchemaEvents,
    UnkeyedMatchingEvents,
    DifferentKeyMatchingEvents,
    SameKeyEvents,
    BeforeIntervalEvents,
    AtOrAfterIntervalEvents,
    DenominatorEvents,
    NumeratorEvents,
    NumeratorExamplesSelected,
    NumeratorExamplesOmitted,
    ScaledNumerator,
    CountConversion,
}

#[derive(Clone, Eq, PartialEq)]
pub enum IntervalRatioPpmMaterializationErrorV0 {
    IntervalOutsideEpoch,
    RequestedIntervalExpired,
    PredicateFieldMissing,
    PredicateFieldKindMismatch,
    ExampleAllocation,
    ArithmeticOverflow {
        component: IntervalRatioPpmAccountingComponentV0,
    },
    UsageInvariant,
}

impl fmt::Debug for IntervalRatioPpmMaterializationErrorV0 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::IntervalOutsideEpoch => formatter.write_str("IntervalOutsideEpoch"),
            Self::RequestedIntervalExpired => formatter.write_str("RequestedIntervalExpired"),
            Self::PredicateFieldMissing => formatter.write_str("PredicateFieldMissing"),
            Self::PredicateFieldKindMismatch => formatter.write_str("PredicateFieldKindMismatch"),
            Self::ExampleAllocation => formatter.write_str("ExampleAllocation"),
            Self::ArithmeticOverflow { component } => formatter
                .debug_struct("ArithmeticOverflow")
                .field("component", component)
                .finish(),
            Self::UsageInvariant => formatter.write_str("UsageInvariant"),
        }
    }
}

impl fmt::Display for IntervalRatioPpmMaterializationErrorV0 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(self, formatter)
    }
}

impl Error for IntervalRatioPpmMaterializationErrorV0 {}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct IntervalRatioPpmObservationV0 {
    numerator: u64,
    denominator: u64,
    rate_ppm: u64,
    remainder: u64,
}

impl IntervalRatioPpmObservationV0 {
    pub const fn numerator(self) -> u64 {
        self.numerator
    }

    pub const fn denominator(self) -> u64 {
        self.denominator
    }

    pub const fn rate_ppm(self) -> u64 {
        self.rate_ppm
    }

    pub const fn remainder(self) -> u64 {
        self.remainder
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct IntervalRatioPpmUsageV0 {
    retained_events_considered: usize,
    other_schema_events_omitted: usize,
    matching_schema_events: usize,
    unkeyed_matching_events_omitted: usize,
    different_key_matching_events_omitted: usize,
    same_key_events: usize,
    before_interval_events_omitted: usize,
    at_or_after_interval_events_omitted: usize,
    denominator_events: usize,
    numerator_events: usize,
    numerator_examples_selected: usize,
    numerator_examples_omitted: usize,
}

impl IntervalRatioPpmUsageV0 {
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

    pub const fn numerator_examples_selected(self) -> usize {
        self.numerator_examples_selected
    }

    pub const fn numerator_examples_omitted(self) -> usize {
        self.numerator_examples_omitted
    }
}

/// A bounded borrowed ratio result that keeps admission immutable.
///
/// ```compile_fail
/// use decant_ir::EventEnvelopeV0;
/// use decant_state::{AdmissionStateV0, IntervalRatioPpmConfigV0};
/// fn cannot_mutate_while_view_is_live(
///     admission: &mut AdmissionStateV0,
///     event: &EventEnvelopeV0,
///     config: IntervalRatioPpmConfigV0,
/// ) {
///     let view = admission.materialize_interval_ratio_ppm(config).unwrap();
///     admission.apply_event(event).unwrap();
///     let _ = view.usage();
/// }
/// ```
///
/// ```compile_fail
/// use decant_ir::SourceIdV0;
/// use decant_state::{AdmissionStateV0, IntervalRatioPpmConfigV0};
/// fn cannot_finalize_while_view_is_live(
///     admission: &mut AdmissionStateV0,
///     source_id: &SourceIdV0,
///     finalized_through_ns: i64,
///     config: IntervalRatioPpmConfigV0,
/// ) {
///     let view = admission.materialize_interval_ratio_ppm(config).unwrap();
///     admission.finalize_source(source_id, finalized_through_ns);
///     let _ = view.usage();
/// }
/// ```
pub struct IntervalRatioPpmViewV0<'a> {
    admission: &'a AdmissionStateV0,
    config: IntervalRatioPpmConfigV0,
    retention_cutoff_event_time_ns: Option<i64>,
    retention_usage: EventTimeRetentionUsageV0,
    usage: IntervalRatioPpmUsageV0,
    observation: Option<IntervalRatioPpmObservationV0>,
    numerator_examples: Vec<&'a EventEnvelopeV0>,
}

impl AdmissionStateV0 {
    pub fn materialize_interval_ratio_ppm(
        &self,
        config: IntervalRatioPpmConfigV0,
    ) -> Result<IntervalRatioPpmViewV0<'_>, IntervalRatioPpmMaterializationErrorV0> {
        if config.interval_start_ns < self.config().epoch_start_ns()
            || config.interval_end_ns > self.config().epoch_end_ns()
        {
            return Err(IntervalRatioPpmMaterializationErrorV0::IntervalOutsideEpoch);
        }

        let retention = self.project_event_time_retention(config.retention);
        let retention_cutoff_event_time_ns = retention.cutoff_event_time_ns();
        if retention_cutoff_event_time_ns.is_some_and(|cutoff| cutoff > config.interval_start_ns) {
            return Err(IntervalRatioPpmMaterializationErrorV0::RequestedIntervalExpired);
        }
        let retention_usage = retention.usage();
        let mut numerator_examples = Vec::new();
        numerator_examples
            .try_reserve_exact(config.example_limit)
            .map_err(|_| IntervalRatioPpmMaterializationErrorV0::ExampleAllocation)?;
        let mut usage = IntervalRatioPpmUsageV0 {
            retained_events_considered: 0,
            other_schema_events_omitted: 0,
            matching_schema_events: 0,
            unkeyed_matching_events_omitted: 0,
            different_key_matching_events_omitted: 0,
            same_key_events: 0,
            before_interval_events_omitted: 0,
            at_or_after_interval_events_omitted: 0,
            denominator_events: 0,
            numerator_events: 0,
            numerator_examples_selected: 0,
            numerator_examples_omitted: 0,
        };

        for event in retention.events() {
            checked_increment(
                &mut usage.retained_events_considered,
                IntervalRatioPpmAccountingComponentV0::RetainedEventsConsidered,
            )?;
            if event.schema_id() != &config.schema_id {
                checked_increment(
                    &mut usage.other_schema_events_omitted,
                    IntervalRatioPpmAccountingComponentV0::OtherSchemaEvents,
                )?;
                continue;
            }
            checked_increment(
                &mut usage.matching_schema_events,
                IntervalRatioPpmAccountingComponentV0::MatchingSchemaEvents,
            )?;
            let Some(key) = event.key() else {
                checked_increment(
                    &mut usage.unkeyed_matching_events_omitted,
                    IntervalRatioPpmAccountingComponentV0::UnkeyedMatchingEvents,
                )?;
                continue;
            };
            if key != config.key.as_slice() {
                checked_increment(
                    &mut usage.different_key_matching_events_omitted,
                    IntervalRatioPpmAccountingComponentV0::DifferentKeyMatchingEvents,
                )?;
                continue;
            }
            checked_increment(
                &mut usage.same_key_events,
                IntervalRatioPpmAccountingComponentV0::SameKeyEvents,
            )?;
            if event.event_time_ns() < config.interval_start_ns {
                checked_increment(
                    &mut usage.before_interval_events_omitted,
                    IntervalRatioPpmAccountingComponentV0::BeforeIntervalEvents,
                )?;
                continue;
            }
            if event.event_time_ns() >= config.interval_end_ns {
                checked_increment(
                    &mut usage.at_or_after_interval_events_omitted,
                    IntervalRatioPpmAccountingComponentV0::AtOrAfterIntervalEvents,
                )?;
                continue;
            }
            checked_increment(
                &mut usage.denominator_events,
                IntervalRatioPpmAccountingComponentV0::DenominatorEvents,
            )?;
            let value = payload_field(event, &config.predicate_field)
                .ok_or(IntervalRatioPpmMaterializationErrorV0::PredicateFieldMissing)?;
            if value.kind() != config.predicate_value.kind() {
                return Err(IntervalRatioPpmMaterializationErrorV0::PredicateFieldKindMismatch);
            }
            if value == &config.predicate_value {
                checked_increment(
                    &mut usage.numerator_events,
                    IntervalRatioPpmAccountingComponentV0::NumeratorEvents,
                )?;
                select_example(&mut numerator_examples, config.example_limit, event);
            }
        }

        usage.numerator_examples_selected = numerator_examples.len();
        usage.numerator_examples_omitted = usage
            .numerator_events
            .checked_sub(usage.numerator_examples_selected)
            .ok_or(IntervalRatioPpmMaterializationErrorV0::UsageInvariant)?;
        verify_usage(retention_usage, usage)?;
        let observation = ratio_observation(usage.numerator_events, usage.denominator_events)?;

        Ok(IntervalRatioPpmViewV0 {
            admission: self,
            config,
            retention_cutoff_event_time_ns,
            retention_usage,
            usage,
            observation,
            numerator_examples,
        })
    }
}

impl<'a> IntervalRatioPpmViewV0<'a> {
    pub const fn config(&self) -> &IntervalRatioPpmConfigV0 {
        &self.config
    }

    pub const fn retention_cutoff_event_time_ns(&self) -> Option<i64> {
        self.retention_cutoff_event_time_ns
    }

    pub const fn retention_usage(&self) -> EventTimeRetentionUsageV0 {
        self.retention_usage
    }

    pub const fn usage(&self) -> IntervalRatioPpmUsageV0 {
        self.usage
    }

    pub const fn observation(&self) -> Option<IntervalRatioPpmObservationV0> {
        self.observation
    }

    pub fn numerator_examples(&self) -> impl ExactSizeIterator<Item = &'a EventEnvelopeV0> + '_ {
        self.numerator_examples.iter().copied()
    }

    pub fn denominator_events(&self) -> impl Iterator<Item = &'a EventEnvelopeV0> + '_ {
        self.admission.accepted_events.values().filter(|event| {
            event_is_retained(event, self.retention_cutoff_event_time_ns)
                && event_is_denominator(event, &self.config)
        })
    }

    pub fn numerator_events(&self) -> impl Iterator<Item = &'a EventEnvelopeV0> + '_ {
        self.denominator_events().filter(|event| {
            payload_field(event, &self.config.predicate_field)
                .is_some_and(|value| value == &self.config.predicate_value)
        })
    }

    pub const fn source_watermark_ns(&self) -> Option<i64> {
        self.admission.watermark_ns()
    }

    pub const fn is_source_finalized(&self) -> bool {
        self.admission.is_finalized()
    }
}

impl fmt::Debug for IntervalRatioPpmViewV0<'_> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("IntervalRatioPpmViewV0")
            .field("config", &self.config)
            .field("retention_usage", &self.retention_usage)
            .field("usage", &self.usage)
            .field("has_observation", &self.observation.is_some())
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

fn validate_field_name(field_name: &str) -> Result<(), IntervalRatioPpmConfigErrorV0> {
    let bytes = field_name.as_bytes();
    if !(1..=MAX_FIELD_NAME_BYTES).contains(&bytes.len()) {
        return Err(IntervalRatioPpmConfigErrorV0::InvalidFieldNameLength {
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
        return Err(IntervalRatioPpmConfigErrorV0::InvalidFieldNameSyntax);
    }
    Ok(())
}

fn checked_increment(
    current: &mut usize,
    component: IntervalRatioPpmAccountingComponentV0,
) -> Result<(), IntervalRatioPpmMaterializationErrorV0> {
    *current = current
        .checked_add(1)
        .ok_or(IntervalRatioPpmMaterializationErrorV0::ArithmeticOverflow { component })?;
    Ok(())
}

fn ratio_observation(
    numerator: usize,
    denominator: usize,
) -> Result<Option<IntervalRatioPpmObservationV0>, IntervalRatioPpmMaterializationErrorV0> {
    if denominator == 0 {
        return Ok(None);
    }
    let numerator = u64::try_from(numerator).map_err(|_| {
        IntervalRatioPpmMaterializationErrorV0::ArithmeticOverflow {
            component: IntervalRatioPpmAccountingComponentV0::CountConversion,
        }
    })?;
    let denominator = u64::try_from(denominator).map_err(|_| {
        IntervalRatioPpmMaterializationErrorV0::ArithmeticOverflow {
            component: IntervalRatioPpmAccountingComponentV0::CountConversion,
        }
    })?;
    let scaled_numerator = numerator.checked_mul(PARTS_PER_MILLION_V0).ok_or(
        IntervalRatioPpmMaterializationErrorV0::ArithmeticOverflow {
            component: IntervalRatioPpmAccountingComponentV0::ScaledNumerator,
        },
    )?;
    Ok(Some(IntervalRatioPpmObservationV0 {
        numerator,
        denominator,
        rate_ppm: scaled_numerator / denominator,
        remainder: scaled_numerator % denominator,
    }))
}

fn select_example<'a>(
    examples: &mut Vec<&'a EventEnvelopeV0>,
    limit: usize,
    candidate: &'a EventEnvelopeV0,
) {
    if limit == 0 {
        return;
    }
    let rank = event_rank_v0(candidate);
    let index = examples.partition_point(|current| event_rank_v0(current) < rank);
    if examples.len() < limit {
        examples.insert(index, candidate);
    } else if index < limit {
        examples.pop();
        examples.insert(index, candidate);
    }
}

fn verify_usage(
    retention_usage: EventTimeRetentionUsageV0,
    usage: IntervalRatioPpmUsageV0,
) -> Result<(), IntervalRatioPpmMaterializationErrorV0> {
    let schema_classified = usage
        .other_schema_events_omitted
        .checked_add(usage.matching_schema_events)
        .ok_or(IntervalRatioPpmMaterializationErrorV0::UsageInvariant)?;
    let matching_schema_classified = usage
        .unkeyed_matching_events_omitted
        .checked_add(usage.different_key_matching_events_omitted)
        .and_then(|count| count.checked_add(usage.same_key_events))
        .ok_or(IntervalRatioPpmMaterializationErrorV0::UsageInvariant)?;
    let same_key_classified = usage
        .before_interval_events_omitted
        .checked_add(usage.at_or_after_interval_events_omitted)
        .and_then(|count| count.checked_add(usage.denominator_events))
        .ok_or(IntervalRatioPpmMaterializationErrorV0::UsageInvariant)?;
    let examples_classified = usage
        .numerator_examples_selected
        .checked_add(usage.numerator_examples_omitted)
        .ok_or(IntervalRatioPpmMaterializationErrorV0::UsageInvariant)?;
    if usage.retained_events_considered != retention_usage.retained_events()
        || usage.retained_events_considered != schema_classified
        || usage.matching_schema_events != matching_schema_classified
        || usage.same_key_events != same_key_classified
        || usage.numerator_events > usage.denominator_events
        || usage.numerator_events != examples_classified
    {
        return Err(IntervalRatioPpmMaterializationErrorV0::UsageInvariant);
    }
    Ok(())
}

fn event_is_retained(event: &EventEnvelopeV0, cutoff_event_time_ns: Option<i64>) -> bool {
    cutoff_event_time_ns.is_none_or(|cutoff| event.event_time_ns() >= cutoff)
}

fn event_is_denominator(event: &EventEnvelopeV0, config: &IntervalRatioPpmConfigV0) -> bool {
    event.schema_id() == &config.schema_id
        && event.key() == Some(config.key.as_slice())
        && event.event_time_ns() >= config.interval_start_ns
        && event.event_time_ns() < config.interval_end_ns
}

fn payload_field<'a>(event: &'a EventEnvelopeV0, field_name: &str) -> Option<&'a ScalarValueV0> {
    event
        .payload()
        .fields()
        .find_map(|(candidate, value)| (candidate == field_name).then_some(value))
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
                IntervalRatioPpmAccountingComponentV0::NumeratorEvents,
            ),
            Err(IntervalRatioPpmMaterializationErrorV0::ArithmeticOverflow {
                component: IntervalRatioPpmAccountingComponentV0::NumeratorEvents,
            })
        );
        assert_eq!(current, usize::MAX);
    }

    #[cfg(target_pointer_width = "64")]
    #[test]
    fn scaled_numerator_overflow_is_typed() {
        assert_eq!(
            ratio_observation(usize::MAX, 1),
            Err(IntervalRatioPpmMaterializationErrorV0::ArithmeticOverflow {
                component: IntervalRatioPpmAccountingComponentV0::ScaledNumerator,
            })
        );
    }
}
