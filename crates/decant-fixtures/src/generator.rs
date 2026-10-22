use std::collections::{BTreeMap, BTreeSet, BinaryHeap};
use std::error::Error;
use std::fmt;

use decant_ir::{
    CanonicalPayloadV0, ENVELOPE_VERSION_V0, EventEnvelopeV0, ScalarValueV0, SchemaIdV0,
    SensitivityBitmapV0, SourceIdV0, SourcePositionV0, ValidationErrorV0,
};

use crate::codec::{EndOfInputV0, FixtureRecordV0};
use crate::contracts::{GeneratorSpecV0, GeneratorTransformV0, ScenarioManifestV0};

pub const GENERATOR_VERSION_V0: u16 = 0;
pub const SRE_V0_UNIQUE_EVENTS: usize = 99_000;
pub const SRE_V0_DATA_DELIVERIES: usize = 100_000;

const PAYMENT_START: u64 = 0;
const PAYMENT_END: u64 = 60_000;
const ACCEPTED_DISTRACTOR_START: u64 = PAYMENT_END;
const ACCEPTED_DISTRACTOR_END: u64 = 98_250;
const LATE_DISTRACTOR_START: u64 = ACCEPTED_DISTRACTOR_END;
const LATE_DISTRACTOR_END: u64 = 98_500;
const METRIC_START: u64 = LATE_DISTRACTOR_END;
const METRIC_END: u64 = 98_980;
const DEPLOYMENT_START: u64 = METRIC_END;
const UNIQUE_END: u64 = 99_000;

const DUPLICATE_COUNT: usize = 1_000;
const DELAYED_COUNT: usize = 2_000;
const DEPLOYMENT_COUNT: usize = 20;
const NANOS_PER_SECOND: i64 = 1_000_000_000;
const FIVE_MINUTES_NS: i64 = 300 * NANOS_PER_SECOND;
const TEN_MINUTES_NS: i64 = 600 * NANOS_PER_SECOND;
const TWENTY_MINUTES_NS: i64 = 1_200 * NANOS_PER_SECOND;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum GeneratorContractFieldV0 {
    Counts,
    Interval,
    Timing,
    Rates,
    Latency,
    ErrorPattern,
    Deployments,
    Selection,
}

#[derive(Clone, Eq, PartialEq)]
pub enum GeneratorErrorV0 {
    UnsupportedGeneratorVersion { actual: u16 },
    UnknownScenario { identifier_byte_len: usize },
    ContractMismatch { field: GeneratorContractFieldV0 },
    ArithmeticOverflow,
    IrValidation(ValidationErrorV0),
}

impl fmt::Debug for GeneratorErrorV0 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::UnsupportedGeneratorVersion { actual } => formatter
                .debug_struct("UnsupportedGeneratorVersion")
                .field("actual", actual)
                .finish(),
            Self::UnknownScenario {
                identifier_byte_len,
            } => formatter
                .debug_struct("UnknownScenario")
                .field("identifier_byte_len", identifier_byte_len)
                .finish(),
            Self::ContractMismatch { field } => formatter
                .debug_struct("ContractMismatch")
                .field("field", field)
                .finish(),
            Self::ArithmeticOverflow => formatter.write_str("ArithmeticOverflow"),
            Self::IrValidation(error) => {
                formatter.debug_tuple("IrValidation").field(error).finish()
            }
        }
    }
}

impl fmt::Display for GeneratorErrorV0 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(self, formatter)
    }
}

impl Error for GeneratorErrorV0 {}

impl From<ValidationErrorV0> for GeneratorErrorV0 {
    fn from(error: ValidationErrorV0) -> Self {
        Self::IrValidation(error)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct GeneratorUsageV0 {
    schedule_entries: usize,
    duplicate_sequences: usize,
    delayed_sequences: usize,
    deployment_definitions: usize,
    pending_records: usize,
}

impl GeneratorUsageV0 {
    pub fn schedule_entries(self) -> usize {
        self.schedule_entries
    }

    pub fn duplicate_sequences(self) -> usize {
        self.duplicate_sequences
    }

    pub fn delayed_sequences(self) -> usize {
        self.delayed_sequences
    }

    pub fn deployment_definitions(self) -> usize {
        self.deployment_definitions
    }

    pub fn pending_records(self) -> usize {
        self.pending_records
    }
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
struct ScheduleEntryV0 {
    ingest_time_ns: i64,
    source_sequence: u64,
}

pub struct SreGeneratorV0 {
    spec: GeneratorSpecV0,
    source_id: SourceIdV0,
    schedule: Vec<ScheduleEntryV0>,
    duplicate_sequences: BTreeSet<u64>,
    delayed_sequences: BTreeSet<u64>,
    next_schedule_index: usize,
    pending_duplicate: Option<EventEnvelopeV0>,
    emitted_data_records: usize,
    terminal_emitted: bool,
    failed: bool,
}

impl fmt::Debug for SreGeneratorV0 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("SreGeneratorV0")
            .field("schedule_entries", &self.schedule.len())
            .field("duplicate_sequences", &self.duplicate_sequences.len())
            .field("delayed_sequences", &self.delayed_sequences.len())
            .field("next_schedule_index", &self.next_schedule_index)
            .field(
                "pending_records",
                &usize::from(self.pending_duplicate.is_some()),
            )
            .field("emitted_data_records", &self.emitted_data_records)
            .field("terminal_emitted", &self.terminal_emitted)
            .field("failed", &self.failed)
            .finish()
    }
}

impl SreGeneratorV0 {
    pub fn new(
        generator_version: u16,
        manifest: &ScenarioManifestV0,
        scenario_id: &str,
    ) -> Result<Self, GeneratorErrorV0> {
        if generator_version != GENERATOR_VERSION_V0 {
            return Err(GeneratorErrorV0::UnsupportedGeneratorVersion {
                actual: generator_version,
            });
        }
        let spec =
            manifest
                .generator_spec(scenario_id)
                .ok_or(GeneratorErrorV0::UnknownScenario {
                    identifier_byte_len: scenario_id.len(),
                })?;
        Self::from_spec(spec)
    }

    fn from_spec(spec: GeneratorSpecV0) -> Result<Self, GeneratorErrorV0> {
        validate_spec(&spec)?;
        let source_id = SourceIdV0::new(&spec.source_id)?;
        let accepted_sequences = accepted_sequences();
        let duplicate_sequences = select_top_k(
            accepted_sequences.clone(),
            spec.seed,
            spec.duplicate_domain,
            DUPLICATE_COUNT,
        )?;
        let delayed_sequences = select_top_k(
            accepted_sequences,
            spec.seed,
            spec.reorder_domain,
            DELAYED_COUNT,
        )?;

        let mut schedule = Vec::with_capacity(SRE_V0_UNIQUE_EVENTS);
        for source_sequence in PAYMENT_START..UNIQUE_END {
            let event_time_ns = event_time_for_sequence(&spec, source_sequence)?;
            let ingest_time_ns =
                if (LATE_DISTRACTOR_START..LATE_DISTRACTOR_END).contains(&source_sequence) {
                    late_ingest_time(&spec, source_sequence)?
                } else if delayed_sequences.contains(&source_sequence) {
                    event_time_ns
                        .checked_add(delay_ns(&spec, source_sequence)?)
                        .ok_or(GeneratorErrorV0::ArithmeticOverflow)?
                } else {
                    event_time_ns
                };
            schedule.push(ScheduleEntryV0 {
                ingest_time_ns,
                source_sequence,
            });
        }
        schedule.sort_unstable();

        Ok(Self {
            spec,
            source_id,
            schedule,
            duplicate_sequences,
            delayed_sequences,
            next_schedule_index: 0,
            pending_duplicate: None,
            emitted_data_records: 0,
            terminal_emitted: false,
            failed: false,
        })
    }

    pub fn usage(&self) -> GeneratorUsageV0 {
        GeneratorUsageV0 {
            schedule_entries: self.schedule.len(),
            duplicate_sequences: self.duplicate_sequences.len(),
            delayed_sequences: self.delayed_sequences.len(),
            deployment_definitions: self.spec.deployments.len(),
            pending_records: usize::from(self.pending_duplicate.is_some()),
        }
    }

    pub fn emitted_data_records(&self) -> usize {
        self.emitted_data_records
    }

    fn next_record(&mut self) -> Result<Option<FixtureRecordV0>, GeneratorErrorV0> {
        if let Some(duplicate) = self.pending_duplicate.take() {
            self.emitted_data_records += 1;
            return Ok(Some(FixtureRecordV0::Event(duplicate)));
        }

        if let Some(entry) = self.schedule.get(self.next_schedule_index).copied() {
            self.next_schedule_index += 1;
            let event = build_event(&self.spec, &self.source_id, entry)?;
            if self.duplicate_sequences.contains(&entry.source_sequence) {
                self.pending_duplicate = Some(event.clone());
            }
            self.emitted_data_records += 1;
            return Ok(Some(FixtureRecordV0::Event(event)));
        }

        if !self.terminal_emitted {
            self.terminal_emitted = true;
            let control = EndOfInputV0::new(self.source_id.clone(), self.spec.interval_end_ns)?;
            return Ok(Some(FixtureRecordV0::EndOfInput(control)));
        }

        Ok(None)
    }
}

impl Iterator for SreGeneratorV0 {
    type Item = Result<FixtureRecordV0, GeneratorErrorV0>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.failed {
            return None;
        }
        match self.next_record() {
            Ok(Some(record)) => Some(Ok(record)),
            Ok(None) => None,
            Err(error) => {
                self.failed = true;
                Some(Err(error))
            }
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        if self.failed {
            return (0, Some(0));
        }
        let total = SRE_V0_DATA_DELIVERIES + 1;
        let emitted = self.emitted_data_records + usize::from(self.terminal_emitted);
        let remaining = total.saturating_sub(emitted);
        (0, Some(remaining))
    }
}

fn accepted_sequences() -> impl Clone + Iterator<Item = u64> {
    (PAYMENT_START..LATE_DISTRACTOR_START).chain(METRIC_START..UNIQUE_END)
}

fn select_top_k<I>(
    candidates: I,
    seed: u64,
    domain: u64,
    count: usize,
) -> Result<BTreeSet<u64>, GeneratorErrorV0>
where
    I: IntoIterator<Item = u64>,
{
    let mut selected = BinaryHeap::with_capacity(count);
    for source_sequence in candidates {
        let candidate = (mix_v0(seed ^ domain ^ source_sequence), source_sequence);
        if selected.len() < count {
            selected.push(candidate);
        } else if selected
            .peek()
            .is_some_and(|greatest| candidate < *greatest)
        {
            selected.pop();
            selected.push(candidate);
        }
    }
    if selected.len() != count {
        return Err(GeneratorErrorV0::ContractMismatch {
            field: GeneratorContractFieldV0::Selection,
        });
    }
    Ok(selected
        .into_iter()
        .map(|(_, source_sequence)| source_sequence)
        .collect())
}

fn mix_v0(mut value: u64) -> u64 {
    value = value.wrapping_add(0x9E37_79B9_7F4A_7C15);
    value = (value ^ (value >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    value = (value ^ (value >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    value ^ (value >> 31)
}

fn delay_ns(spec: &GeneratorSpecV0, source_sequence: u64) -> Result<i64, GeneratorErrorV0> {
    let seconds = 1 + (mix_v0(spec.seed ^ spec.reorder_domain ^ source_sequence) % 25);
    i64::try_from(seconds)
        .ok()
        .and_then(|value| value.checked_mul(NANOS_PER_SECOND))
        .ok_or(GeneratorErrorV0::ArithmeticOverflow)
}

fn event_time_for_sequence(
    spec: &GeneratorSpecV0,
    source_sequence: u64,
) -> Result<i64, GeneratorErrorV0> {
    match source_sequence {
        PAYMENT_START..PAYMENT_END => {
            let ordinal = source_sequence / 2;
            let interval = NANOS_PER_SECOND
                .checked_div(
                    i64::try_from(spec.rates.requests_per_second_per_region)
                        .map_err(|_| GeneratorErrorV0::ArithmeticOverflow)?,
                )
                .ok_or(GeneratorErrorV0::ArithmeticOverflow)?;
            checked_time(spec.interval_start_ns, ordinal, interval)
        }
        ACCEPTED_DISTRACTOR_START..ACCEPTED_DISTRACTOR_END => scaled_time(
            spec.interval_start_ns,
            source_sequence - ACCEPTED_DISTRACTOR_START,
            spec.counts.accepted_distractors,
            spec.interval_end_ns - spec.interval_start_ns,
        ),
        LATE_DISTRACTOR_START..LATE_DISTRACTOR_END => scaled_time(
            spec.interval_start_ns,
            source_sequence - LATE_DISTRACTOR_START,
            spec.counts.late_distractors,
            spec.timing.late_event_time_before_ns - spec.interval_start_ns,
        ),
        METRIC_START..METRIC_END => {
            let sample = (source_sequence - METRIC_START) / 4;
            checked_time(spec.interval_start_ns, sample, 10 * NANOS_PER_SECOND)
        }
        DEPLOYMENT_START..UNIQUE_END => spec
            .deployments
            .get(usize::try_from(source_sequence - DEPLOYMENT_START).unwrap_or(usize::MAX))
            .map(|deployment| deployment.event_time_ns)
            .ok_or(GeneratorErrorV0::ContractMismatch {
                field: GeneratorContractFieldV0::Deployments,
            }),
        _ => Err(GeneratorErrorV0::ContractMismatch {
            field: GeneratorContractFieldV0::Counts,
        }),
    }
}

fn late_ingest_time(spec: &GeneratorSpecV0, source_sequence: u64) -> Result<i64, GeneratorErrorV0> {
    scaled_time(
        spec.timing.late_delivery_after_ns,
        source_sequence - LATE_DISTRACTOR_START,
        spec.counts.late_distractors,
        spec.interval_end_ns - spec.timing.late_delivery_after_ns,
    )
}

fn checked_time(start: i64, ordinal: u64, interval_ns: i64) -> Result<i64, GeneratorErrorV0> {
    let ordinal = i64::try_from(ordinal).map_err(|_| GeneratorErrorV0::ArithmeticOverflow)?;
    start
        .checked_add(
            ordinal
                .checked_mul(interval_ns)
                .ok_or(GeneratorErrorV0::ArithmeticOverflow)?,
        )
        .ok_or(GeneratorErrorV0::ArithmeticOverflow)
}

fn scaled_time(
    start: i64,
    index: u64,
    count: u64,
    duration_ns: i64,
) -> Result<i64, GeneratorErrorV0> {
    if count == 0 || duration_ns < 0 {
        return Err(GeneratorErrorV0::ArithmeticOverflow);
    }
    let offset = u128::from(index)
        .checked_mul(u128::try_from(duration_ns).map_err(|_| GeneratorErrorV0::ArithmeticOverflow)?)
        .and_then(|value| value.checked_div(u128::from(count)))
        .and_then(|value| i64::try_from(value).ok())
        .ok_or(GeneratorErrorV0::ArithmeticOverflow)?;
    start
        .checked_add(offset)
        .ok_or(GeneratorErrorV0::ArithmeticOverflow)
}

fn build_event(
    spec: &GeneratorSpecV0,
    source_id: &SourceIdV0,
    entry: ScheduleEntryV0,
) -> Result<EventEnvelopeV0, GeneratorErrorV0> {
    match entry.source_sequence {
        PAYMENT_START..PAYMENT_END => build_payment(spec, source_id, entry),
        ACCEPTED_DISTRACTOR_START..ACCEPTED_DISTRACTOR_END => {
            build_distractor(spec, source_id, entry, false)
        }
        LATE_DISTRACTOR_START..LATE_DISTRACTOR_END => {
            build_distractor(spec, source_id, entry, true)
        }
        METRIC_START..METRIC_END => build_metric(spec, source_id, entry),
        DEPLOYMENT_START..UNIQUE_END => build_deployment(spec, source_id, entry),
        _ => Err(GeneratorErrorV0::ContractMismatch {
            field: GeneratorContractFieldV0::Counts,
        }),
    }
}

fn build_payment(
    spec: &GeneratorSpecV0,
    source_id: &SourceIdV0,
    entry: ScheduleEntryV0,
) -> Result<EventEnvelopeV0, GeneratorErrorV0> {
    let ordinal = entry.source_sequence / 2;
    let original_region = if entry.source_sequence.is_multiple_of(2) {
        "region-a"
    } else {
        "region-b"
    };
    let service_id = transformed_service(spec.transform, "payments-v2");
    let region_id = transformed_region(spec.transform, original_region);
    let event_time_ns = event_time_for_sequence(spec, entry.source_sequence)?;
    let post_change = event_time_ns >= spec.interval_start_ns + TEN_MINUTES_NS;
    let affected = original_region == "region-b"
        && post_change
        && spec.transform != GeneratorTransformV0::NoRegression;
    let offset = ordinal % spec.errors.base_denominator;
    let (outcome, error_code) = if offset < spec.errors.base_internal_error_numerator {
        ("error", Some("internal_error"))
    } else if affected
        && offset
            < spec.errors.base_internal_error_numerator
                + spec.errors.post_region_b_upstream_timeout_numerator
    {
        ("error", Some("upstream_timeout"))
    } else {
        ("ok", None)
    };
    let latency_ms = if affected && ordinal.is_multiple_of(50) {
        spec.latency.post_region_b_p99_ms
    } else {
        spec.latency.pre_region_a_p99_ms
    };

    let fields = [
        ("error_code", optional_text(error_code)?),
        ("event_kind", text("payment_outcome")?),
        ("latency_ms", ScalarValueV0::from_u64(latency_ms)),
        ("outcome", text(outcome)?),
        ("region_id", text(&region_id)?),
        ("request_ordinal", ScalarValueV0::from_u64(ordinal)),
        ("service_id", text(&service_id)?),
    ];
    envelope(
        source_id,
        "payment-outcome-v0",
        entry,
        event_time_ns,
        &service_id,
        &region_id,
        fields,
    )
}

fn build_distractor(
    spec: &GeneratorSpecV0,
    source_id: &SourceIdV0,
    entry: ScheduleEntryV0,
    deliberately_late: bool,
) -> Result<EventEnvelopeV0, GeneratorErrorV0> {
    let index = if deliberately_late {
        entry.source_sequence - LATE_DISTRACTOR_START
    } else {
        entry.source_sequence - ACCEPTED_DISTRACTOR_START
    };
    let services = ["catalog-v1", "checkout-v1", "search-v1"];
    let original_service = services[usize::try_from(index % 3).unwrap_or(0)];
    let original_region = if (index / 3).is_multiple_of(2) {
        "region-a"
    } else {
        "region-b"
    };
    let service_id = transformed_service(spec.transform, original_service);
    let region_id = transformed_region(spec.transform, original_region);
    let event_time_ns = event_time_for_sequence(spec, entry.source_sequence)?;
    let fields = [
        ("event_kind", text("distractor_request")?),
        (
            "fixture_class",
            text(if deliberately_late {
                "late_candidate"
            } else {
                "accepted_distractor"
            })?,
        ),
        ("outcome", text("ok")?),
        ("region_id", text(&region_id)?),
        ("request_ordinal", ScalarValueV0::from_u64(index)),
        ("service_id", text(&service_id)?),
    ];
    envelope(
        source_id,
        "distractor-request-v0",
        entry,
        event_time_ns,
        &service_id,
        &region_id,
        fields,
    )
}

fn build_metric(
    spec: &GeneratorSpecV0,
    source_id: &SourceIdV0,
    entry: ScheduleEntryV0,
) -> Result<EventEnvelopeV0, GeneratorErrorV0> {
    let index = entry.source_sequence - METRIC_START;
    let sample = index / 4;
    let series = index % 4;
    let original_region = if series < 2 { "region-a" } else { "region-b" };
    let is_error_rate = series.is_multiple_of(2);
    let service_id = transformed_service(spec.transform, "payments-v2");
    let region_id = transformed_region(spec.transform, original_region);
    let event_time_ns = event_time_for_sequence(spec, entry.source_sequence)?;
    let post_change = event_time_ns >= spec.interval_start_ns + TEN_MINUTES_NS;
    let affected = original_region == "region-b"
        && post_change
        && spec.transform != GeneratorTransformV0::NoRegression;
    let (metric_name, metric_value) = if is_error_rate {
        (
            "error_rate_ppm",
            if affected {
                spec.rates.post_region_b_ppm
            } else if post_change && original_region == "region-a" {
                spec.rates.post_region_a_ppm
            } else if original_region == "region-a" {
                spec.rates.pre_region_a_ppm
            } else {
                spec.rates.pre_region_b_ppm
            },
        )
    } else {
        (
            "p99_latency_ms",
            if affected {
                spec.latency.post_region_b_p99_ms
            } else if post_change && original_region == "region-a" {
                spec.latency.post_region_a_p99_ms
            } else if original_region == "region-a" {
                spec.latency.pre_region_a_p99_ms
            } else {
                spec.latency.pre_region_b_p99_ms
            },
        )
    };
    let fields = [
        ("event_kind", text("metric")?),
        ("metric_name", text(metric_name)?),
        ("metric_value", ScalarValueV0::from_u64(metric_value)),
        ("region_id", text(&region_id)?),
        ("sample_ordinal", ScalarValueV0::from_u64(sample)),
        ("service_id", text(&service_id)?),
    ];
    envelope(
        source_id,
        "metric-sample-v0",
        entry,
        event_time_ns,
        &service_id,
        &region_id,
        fields,
    )
}

fn build_deployment(
    spec: &GeneratorSpecV0,
    source_id: &SourceIdV0,
    entry: ScheduleEntryV0,
) -> Result<EventEnvelopeV0, GeneratorErrorV0> {
    let index = usize::try_from(entry.source_sequence - DEPLOYMENT_START)
        .map_err(|_| GeneratorErrorV0::ArithmeticOverflow)?;
    let deployment = spec
        .deployments
        .get(index)
        .ok_or(GeneratorErrorV0::ContractMismatch {
            field: GeneratorContractFieldV0::Deployments,
        })?;
    let service_id = transformed_service(spec.transform, &deployment.service_id);
    let region_id = transformed_region(spec.transform, &deployment.region_id);
    let deployment_id = transformed_deployment_id(spec.transform, &deployment.deployment_id);
    let fields = [
        (
            "causal_claim",
            ScalarValueV0::from_bool(deployment.causal_claim),
        ),
        ("deployment_id", text(&deployment_id)?),
        ("event_kind", text("deployment")?),
        (
            "notable_temporal_evidence",
            ScalarValueV0::from_bool(deployment.notable_temporal_evidence),
        ),
        ("region_id", text(&region_id)?),
        ("service_id", text(&service_id)?),
    ];
    envelope(
        source_id,
        "deployment-v0",
        entry,
        deployment.event_time_ns,
        &service_id,
        &region_id,
        fields,
    )
}

#[allow(clippy::too_many_arguments)]
fn envelope<const N: usize>(
    source_id: &SourceIdV0,
    schema_id: &str,
    entry: ScheduleEntryV0,
    event_time_ns: i64,
    service_id: &str,
    region_id: &str,
    fields: [(&str, ScalarValueV0); N],
) -> Result<EventEnvelopeV0, GeneratorErrorV0> {
    let payload = CanonicalPayloadV0::try_from_fields(
        fields
            .into_iter()
            .map(|(name, value)| (name.to_owned(), value))
            .collect::<BTreeMap<_, _>>(),
    )?;
    let key = format!("{service_id}/{region_id}");
    Ok(EventEnvelopeV0::new(
        ENVELOPE_VERSION_V0,
        source_id.clone(),
        SchemaIdV0::new(schema_id)?,
        SourcePositionV0::Synthetic {
            sequence: entry.source_sequence,
        },
        Some(key.as_bytes()),
        event_time_ns,
        entry.ingest_time_ns,
        SensitivityBitmapV0::from_bits(0),
        payload,
    )?)
}

fn text(value: &str) -> Result<ScalarValueV0, GeneratorErrorV0> {
    Ok(ScalarValueV0::utf8(value)?)
}

fn optional_text(value: Option<&str>) -> Result<ScalarValueV0, GeneratorErrorV0> {
    value.map_or(Ok(ScalarValueV0::null()), text)
}

fn transformed_region(transform: GeneratorTransformV0, region: &str) -> String {
    match (transform, region) {
        (GeneratorTransformV0::AffectedRegionSwap, "region-a") => "region-b".to_owned(),
        (GeneratorTransformV0::AffectedRegionSwap, "region-b") => "region-a".to_owned(),
        _ => region.to_owned(),
    }
}

fn transformed_service(transform: GeneratorTransformV0, service: &str) -> String {
    match (transform, service) {
        (GeneratorTransformV0::AffectedServiceDeploymentSwap, "payments-v2") => {
            "checkout-v1".to_owned()
        }
        (GeneratorTransformV0::AffectedServiceDeploymentSwap, "checkout-v1") => {
            "payments-v2".to_owned()
        }
        _ => service.to_owned(),
    }
}

fn transformed_deployment_id(transform: GeneratorTransformV0, deployment_id: &str) -> String {
    match transform {
        GeneratorTransformV0::AffectedRegionSwap => deployment_id
            .replace("region-a", "region-temporary")
            .replace("region-b", "region-a")
            .replace("region-temporary", "region-b"),
        GeneratorTransformV0::AffectedServiceDeploymentSwap => deployment_id
            .replace("payments-v2", "service-temporary")
            .replace("checkout-v1", "payments-v2")
            .replace("service-temporary", "checkout-v1"),
        GeneratorTransformV0::Primary | GeneratorTransformV0::NoRegression => {
            deployment_id.to_owned()
        }
    }
}

fn validate_spec(spec: &GeneratorSpecV0) -> Result<(), GeneratorErrorV0> {
    let counts = spec.counts;
    let count_match = counts.payment_outcomes == 60_000
        && counts.accepted_distractors == 38_250
        && counts.late_distractors == 250
        && counts.metric_events == 480
        && counts.deployment_events == 20
        && counts.exact_duplicate_deliveries == 1_000
        && counts.data_deliveries == 100_000
        && counts.accepted_unique_events == 98_750
        && counts.delayed_accepted_deliveries == 2_000
        && counts.terminal_controls == 1
        && counts.payment_outcomes
            + counts.accepted_distractors
            + counts.late_distractors
            + counts.metric_events
            + counts.deployment_events
            == u64::try_from(SRE_V0_UNIQUE_EVENTS).unwrap_or(u64::MAX)
        && counts.exact_duplicate_deliveries
            + u64::try_from(SRE_V0_UNIQUE_EVENTS).unwrap_or(u64::MAX)
            == counts.data_deliveries;
    if !count_match {
        return Err(GeneratorErrorV0::ContractMismatch {
            field: GeneratorContractFieldV0::Counts,
        });
    }
    if spec.interval_end_ns.checked_sub(spec.interval_start_ns) != Some(TWENTY_MINUTES_NS) {
        return Err(GeneratorErrorV0::ContractMismatch {
            field: GeneratorContractFieldV0::Interval,
        });
    }
    if spec.timing.late_event_time_before_ns != spec.interval_start_ns + FIVE_MINUTES_NS
        || spec.timing.late_delivery_after_ns != spec.interval_start_ns + 3 * FIVE_MINUTES_NS
        || spec.timing.delayed_min_ns != NANOS_PER_SECOND
        || spec.timing.delayed_max_ns != 25 * NANOS_PER_SECOND
    {
        return Err(GeneratorErrorV0::ContractMismatch {
            field: GeneratorContractFieldV0::Timing,
        });
    }
    if spec.rates.requests_per_second_per_region != 25
        || spec.rates.pre_region_a_ppm != 4_000
        || spec.rates.pre_region_b_ppm != 4_000
        || spec.rates.post_region_a_ppm != 4_000
        || spec.rates.post_region_b_ppm != 32_000
        || spec.rates.post_global_ppm != 18_000
    {
        return Err(GeneratorErrorV0::ContractMismatch {
            field: GeneratorContractFieldV0::Rates,
        });
    }
    if spec.latency.pre_region_a_p99_ms != 180
        || spec.latency.pre_region_b_p99_ms != 180
        || spec.latency.post_region_a_p99_ms != 180
        || spec.latency.post_region_b_p99_ms != 720
        || spec.latency.percentile_basis_points != 9_900
    {
        return Err(GeneratorErrorV0::ContractMismatch {
            field: GeneratorContractFieldV0::Latency,
        });
    }
    if spec.errors.base_internal_error_numerator != 1
        || spec.errors.base_denominator != 250
        || spec.errors.post_region_b_upstream_timeout_numerator != 7
        || spec.errors.post_region_b_denominator != 250
        || spec.errors.base_denominator != spec.errors.post_region_b_denominator
    {
        return Err(GeneratorErrorV0::ContractMismatch {
            field: GeneratorContractFieldV0::ErrorPattern,
        });
    }
    if spec.deployments.len() != DEPLOYMENT_COUNT
        || spec.deployments.iter().any(|deployment| {
            deployment.causal_claim
                || deployment.event_time_ns < spec.interval_start_ns
                || deployment.event_time_ns >= spec.interval_end_ns
        })
    {
        return Err(GeneratorErrorV0::ContractMismatch {
            field: GeneratorContractFieldV0::Deployments,
        });
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::decode_scenario_manifest_v0;

    fn primary_spec() -> GeneratorSpecV0 {
        let manifest =
            decode_scenario_manifest_v0(include_bytes!("../contracts/sre-v0/scenario.json"))
                .unwrap();
        manifest.generator_spec("sre-v0-primary").unwrap()
    }

    #[test]
    fn injected_count_and_arithmetic_contract_errors_fail_before_iteration() {
        let mut counts = primary_spec();
        counts.counts.payment_outcomes += 1;
        assert_eq!(
            SreGeneratorV0::from_spec(counts).unwrap_err(),
            GeneratorErrorV0::ContractMismatch {
                field: GeneratorContractFieldV0::Counts
            }
        );

        let mut interval = primary_spec();
        interval.interval_end_ns = i64::MAX;
        assert_eq!(
            SreGeneratorV0::from_spec(interval).unwrap_err(),
            GeneratorErrorV0::ContractMismatch {
                field: GeneratorContractFieldV0::Interval
            }
        );
    }

    #[test]
    fn top_k_selection_is_bounded_and_uses_total_tuple_order() {
        let selected = select_top_k(0..10, 42, 7, 3).unwrap();
        let mut reference = (0..10)
            .map(|sequence| (mix_v0(42 ^ 7 ^ sequence), sequence))
            .collect::<Vec<_>>();
        reference.sort_unstable();
        assert_eq!(
            selected,
            reference
                .into_iter()
                .take(3)
                .map(|(_, sequence)| sequence)
                .collect()
        );
    }

    #[test]
    fn checked_time_arithmetic_overflow_is_typed() {
        assert_eq!(
            checked_time(i64::MAX, 1, 1),
            Err(GeneratorErrorV0::ArithmeticOverflow)
        );
        assert_eq!(
            scaled_time(i64::MAX, 1, 1, 1),
            Err(GeneratorErrorV0::ArithmeticOverflow)
        );
    }
}
