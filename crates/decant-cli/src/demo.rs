use std::fmt::{self, Write};
use std::str;

use decant_compiler::{
    CONTEXT_COMPILER_VERSION_V0, ContextCompileRequestV0, ContextFactIdV0, ContextScopePredicateV0,
    IntervalRatioPpmFactSpecV0, MAX_CONTEXT_LOOKBACK_NS_V0, MAX_CONTEXT_OUTPUT_BYTES_V0,
    PrecedingEvidenceSpecV0, ProjectedChangeFactSpecV0, compile_context_v0,
};
use decant_fixtures::{
    FixtureRecordV0, GENERATOR_VERSION_V0, SRE_V0_DATA_DELIVERIES, SRE_V0_UNIQUE_EVENTS,
    SreGeneratorV0, decode_scenario_manifest_v0,
};
use decant_ir::{ScalarValueV0, SchemaIdV0, SourceIdV0};
use decant_runtime::{
    ChannelLimitsV0, MAX_CHANNEL_ACCOUNTED_BYTES_V0, TryReceiveErrorV0, bounded_event_channel_v0,
};
use decant_state::{
    ADMISSION_VERSION_V0, AdmissionConfigV0, AdmissionLimitsV0, AdmissionStateV0, ApplyOutcomeV0,
    EVENT_TIME_RETENTION_VERSION_V0, EventTimeRetentionConfigV0, FinalizeOutcomeV0,
    PROJECTED_FIELD_CHANGE_VERSION_V0, ProjectedFieldChangeConfigV0, ProjectedFieldChangeLimitsV0,
    REJECTION_CLASS_COUNT_V0, RejectionReasonV0,
};

pub(crate) const MAX_DEMO_OUTPUT_BYTES_V0: usize = 32 * 1_024;

const SCENARIO_ID: &str = "sre-v0-primary";
const SOURCE_ID: &str = "sre-v0";
const PRIMARY_START_NS: i64 = 1_767_225_600_000_000_000;
const PRIMARY_END_NS: i64 = 1_767_226_800_000_000_000;
const MINUTE_NS: i64 = 60_000_000_000;
const KEY: &[u8] = b"payments-v2/region-b";
const EXPECTED_ACCEPTED: usize = 98_750;
const EXPECTED_DUPLICATE_NOOPS: usize = 1_000;
const EXPECTED_LATE_REJECTED: usize = 250;
const EXPECTED_TERMINAL_CONTROLS: usize = 1;
const SCENARIO_DOCUMENT: &[u8] =
    include_bytes!("../../decant-fixtures/contracts/sre-v0/scenario.json");

#[derive(Debug)]
pub(crate) enum DemoErrorV0 {
    ScenarioContract,
    GeneratorConfiguration,
    GeneratorRecord,
    ChannelConfiguration,
    ChannelEnqueue,
    ChannelReceive,
    AdmissionConfiguration,
    AdmissionApply,
    UnexpectedAdmissionOutcome,
    UnexpectedFinalizationOutcome,
    TerminalOrdering,
    AccountingInvariant,
    CompilerConfiguration,
    CompilerExecution,
    CompilerResponseInvariant,
    CompilerEncodingInvariant,
    OutputAllocation,
    OutputCapacity,
}

#[derive(Default)]
struct DemoCountsV0 {
    data_deliveries: usize,
    enqueued: usize,
    received: usize,
    terminal_controls: usize,
    accepted: usize,
    duplicate_noops: usize,
    rejected: usize,
    late_rejected: usize,
}

pub(crate) fn run_sre_demo_v0() -> Result<Vec<u8>, DemoErrorV0> {
    let manifest = decode_scenario_manifest_v0(SCENARIO_DOCUMENT)
        .map_err(|_| DemoErrorV0::ScenarioContract)?;
    let mut generator = SreGeneratorV0::new(GENERATOR_VERSION_V0, &manifest, SCENARIO_ID)
        .map_err(|_| DemoErrorV0::GeneratorConfiguration)?;
    verify_initial_generator_usage(&generator)?;

    let channel_limits = ChannelLimitsV0::new(1, MAX_CHANNEL_ACCOUNTED_BYTES_V0)
        .map_err(|_| DemoErrorV0::ChannelConfiguration)?;
    let (mut sender, mut receiver) = bounded_event_channel_v0(channel_limits);
    let mut admission = build_admission()?;
    let mut counts = DemoCountsV0::default();
    let mut terminal_seen = false;

    for record in generator.by_ref() {
        if terminal_seen {
            return Err(DemoErrorV0::TerminalOrdering);
        }
        match record.map_err(|_| DemoErrorV0::GeneratorRecord)? {
            FixtureRecordV0::Event(event) => {
                increment(&mut counts.data_deliveries)?;
                let receipt = sender
                    .try_enqueue(event)
                    .map_err(|_| DemoErrorV0::ChannelEnqueue)?;
                if receipt.usage().current_events() != 1 {
                    return Err(DemoErrorV0::AccountingInvariant);
                }
                increment(&mut counts.enqueued)?;
                let received = receiver
                    .try_receive()
                    .map_err(|_| DemoErrorV0::ChannelReceive)?;
                increment(&mut counts.received)?;
                match admission
                    .apply_event(&received)
                    .map_err(|_| DemoErrorV0::AdmissionApply)?
                {
                    ApplyOutcomeV0::Accepted => increment(&mut counts.accepted)?,
                    ApplyOutcomeV0::DuplicateNoOp => increment(&mut counts.duplicate_noops)?,
                    ApplyOutcomeV0::Rejected(RejectionReasonV0::Late) => {
                        increment(&mut counts.rejected)?;
                        increment(&mut counts.late_rejected)?;
                    }
                    ApplyOutcomeV0::Rejected(_) => {
                        return Err(DemoErrorV0::UnexpectedAdmissionOutcome);
                    }
                }
            }
            FixtureRecordV0::EndOfInput(control) => {
                terminal_seen = true;
                increment(&mut counts.terminal_controls)?;
                if receiver.try_receive() != Err(TryReceiveErrorV0::Empty) {
                    return Err(DemoErrorV0::TerminalOrdering);
                }
                if admission.finalize_source(control.source_id(), control.finalized_through_ns())
                    != FinalizeOutcomeV0::Finalized
                {
                    return Err(DemoErrorV0::UnexpectedFinalizationOutcome);
                }
            }
        }
    }

    if !terminal_seen {
        return Err(DemoErrorV0::TerminalOrdering);
    }
    verify_generator_completion(&generator)?;
    let transport_usage = sender.usage();
    if transport_usage != receiver.usage()
        || transport_usage.limits() != channel_limits
        || transport_usage.current_events() != 0
        || transport_usage.current_accounted_bytes() != 0
        || transport_usage.high_water_events() != 1
        || transport_usage.high_water_accounted_bytes() == 0
        || transport_usage.high_water_accounted_bytes() > channel_limits.accounted_byte_limit()
    {
        return Err(DemoErrorV0::AccountingInvariant);
    }
    verify_admission_accounting(&admission, &counts)?;

    let request = build_compiler_request()?;
    let response =
        compile_context_v0(&admission, &request).map_err(|_| DemoErrorV0::CompilerExecution)?;
    if response.candidate_count() != 2
        || response.included_count() != 2
        || response.omitted_count() != 0
        || !response.source_finalized()
        || !response.query_closed()
    {
        return Err(DemoErrorV0::CompilerResponseInvariant);
    }
    let context =
        str::from_utf8(response.as_bytes()).map_err(|_| DemoErrorV0::CompilerEncodingInvariant)?;

    let admission_usage = admission.usage();
    let mut output = BoundedOutputV0::new(MAX_DEMO_OUTPUT_BYTES_V0)?;
    write!(
        &mut output,
        "{{\"format\":\"decant-sre-demo\",\"version\":0,\"scenario\":\"sre-v0-primary\",\"input\":{{\"data_deliveries\":{},\"terminal_controls\":{}}},\"transport\":{{\"event_limit\":{},\"accounted_byte_limit\":{},\"high_water_events\":{},\"high_water_accounted_bytes\":{}}},\"admission\":{{\"accepted\":{},\"duplicate_noops\":{},\"rejected\":{},\"late_rejected\":{},\"accepted_event_bytes\":{},\"finalized\":true}},\"context\":" ,
        counts.data_deliveries,
        counts.terminal_controls,
        channel_limits.event_limit(),
        channel_limits.accounted_byte_limit(),
        transport_usage.high_water_events(),
        transport_usage.high_water_accounted_bytes(),
        counts.accepted,
        counts.duplicate_noops,
        counts.rejected,
        counts.late_rejected,
        admission_usage.accepted_event_bytes(),
    )
    .map_err(|_| DemoErrorV0::OutputCapacity)?;
    output
        .write_str(context)
        .map_err(|_| DemoErrorV0::OutputCapacity)?;
    output
        .write_str("}\n")
        .map_err(|_| DemoErrorV0::OutputCapacity)?;
    Ok(output.finish())
}

fn build_admission() -> Result<AdmissionStateV0, DemoErrorV0> {
    let source_id = SourceIdV0::new(SOURCE_ID).map_err(|_| DemoErrorV0::AdmissionConfiguration)?;
    let config = AdmissionConfigV0::new(
        ADMISSION_VERSION_V0,
        source_id,
        PRIMARY_START_NS,
        PRIMARY_END_NS,
        AdmissionLimitsV0::maximum(),
    )
    .map_err(|_| DemoErrorV0::AdmissionConfiguration)?;
    Ok(AdmissionStateV0::new(config))
}

fn build_compiler_request() -> Result<ContextCompileRequestV0, DemoErrorV0> {
    let retained =
        EventTimeRetentionConfigV0::new(EVENT_TIME_RETENTION_VERSION_V0, (25 * MINUTE_NS) as u64)
            .map_err(|_| DemoErrorV0::CompilerConfiguration)?;
    let projected_change = ProjectedFieldChangeConfigV0::new(
        PROJECTED_FIELD_CHANGE_VERSION_V0,
        retained,
        schema("metric-sample-v0")?,
        "metric_name",
        "metric_value",
        ProjectedFieldChangeLimitsV0::maximum(),
    )
    .map_err(|_| DemoErrorV0::CompilerConfiguration)?;
    let preceding = PrecedingEvidenceSpecV0::new(
        retained,
        schema("deployment-v0")?,
        (15 * MINUTE_NS) as u64,
        "deployment_id",
    )
    .map_err(|_| DemoErrorV0::CompilerConfiguration)?;
    let projected_fact = ProjectedChangeFactSpecV0::new(
        ContextFactIdV0::new("p99-latency").map_err(|_| DemoErrorV0::CompilerConfiguration)?,
        projected_change,
        "p99_latency_ms",
        (15 * MINUTE_NS) as u64,
        Some(preceding),
    )
    .map_err(|_| DemoErrorV0::CompilerConfiguration)?;
    let ratio_retention = EventTimeRetentionConfigV0::new(
        EVENT_TIME_RETENTION_VERSION_V0,
        (PRIMARY_END_NS - PRIMARY_START_NS) as u64,
    )
    .map_err(|_| DemoErrorV0::CompilerConfiguration)?;
    let ratio_fact = IntervalRatioPpmFactSpecV0::new(
        ContextFactIdV0::new("payment-error-rate")
            .map_err(|_| DemoErrorV0::CompilerConfiguration)?,
        ratio_retention,
        schema("payment-outcome-v0")?,
        "outcome",
        ScalarValueV0::utf8("error").map_err(|_| DemoErrorV0::CompilerConfiguration)?,
        3,
    )
    .map_err(|_| DemoErrorV0::CompilerConfiguration)?;
    let scope = vec![
        ContextScopePredicateV0::new("service_id", "payments-v2")
            .map_err(|_| DemoErrorV0::CompilerConfiguration)?,
        ContextScopePredicateV0::new("region_id", "region-b")
            .map_err(|_| DemoErrorV0::CompilerConfiguration)?,
    ];
    ContextCompileRequestV0::new_with_interval_ratio_facts(
        CONTEXT_COMPILER_VERSION_V0,
        PRIMARY_END_NS,
        MAX_CONTEXT_LOOKBACK_NS_V0,
        MAX_CONTEXT_OUTPUT_BYTES_V0,
        KEY,
        scope,
        vec![projected_fact],
        vec![ratio_fact],
    )
    .map_err(|_| DemoErrorV0::CompilerConfiguration)
}

fn schema(value: &str) -> Result<SchemaIdV0, DemoErrorV0> {
    SchemaIdV0::new(value).map_err(|_| DemoErrorV0::CompilerConfiguration)
}

fn verify_initial_generator_usage(generator: &SreGeneratorV0) -> Result<(), DemoErrorV0> {
    let usage = generator.usage();
    if usage.schedule_entries() != SRE_V0_UNIQUE_EVENTS
        || usage.duplicate_sequences() != EXPECTED_DUPLICATE_NOOPS
        || usage.delayed_sequences() != 2_000
        || usage.deployment_definitions() != 20
        || usage.pending_records() != 0
    {
        return Err(DemoErrorV0::AccountingInvariant);
    }
    Ok(())
}

fn verify_generator_completion(generator: &SreGeneratorV0) -> Result<(), DemoErrorV0> {
    if generator.emitted_data_records() != SRE_V0_DATA_DELIVERIES
        || generator.usage().pending_records() != 0
    {
        return Err(DemoErrorV0::AccountingInvariant);
    }
    Ok(())
}

fn verify_admission_accounting(
    admission: &AdmissionStateV0,
    counts: &DemoCountsV0,
) -> Result<(), DemoErrorV0> {
    let classified = counts
        .accepted
        .checked_add(counts.duplicate_noops)
        .and_then(|value| value.checked_add(counts.rejected))
        .ok_or(DemoErrorV0::AccountingInvariant)?;
    if counts.data_deliveries != SRE_V0_DATA_DELIVERIES
        || counts.enqueued != counts.data_deliveries
        || counts.received != counts.data_deliveries
        || counts.terminal_controls != EXPECTED_TERMINAL_CONTROLS
        || counts.accepted != EXPECTED_ACCEPTED
        || counts.duplicate_noops != EXPECTED_DUPLICATE_NOOPS
        || counts.rejected != EXPECTED_LATE_REJECTED
        || counts.late_rejected != EXPECTED_LATE_REJECTED
        || classified != counts.data_deliveries
        || !admission.is_finalized()
    {
        return Err(DemoErrorV0::AccountingInvariant);
    }
    let usage = admission.usage();
    if usage.accepted_identities() != counts.accepted
        || usage.rejection_classes() != REJECTION_CLASS_COUNT_V0
    {
        return Err(DemoErrorV0::AccountingInvariant);
    }
    for reason in RejectionReasonV0::ALL {
        let range = admission.rejection_summary().range(reason);
        let expected = if reason == RejectionReasonV0::Late {
            EXPECTED_LATE_REJECTED as u64
        } else {
            0
        };
        if range.count() != expected || range.count_saturated() {
            return Err(DemoErrorV0::AccountingInvariant);
        }
    }
    Ok(())
}

fn increment(value: &mut usize) -> Result<(), DemoErrorV0> {
    *value = value
        .checked_add(1)
        .ok_or(DemoErrorV0::AccountingInvariant)?;
    Ok(())
}

struct BoundedOutputV0 {
    bytes: Vec<u8>,
    maximum: usize,
}

impl BoundedOutputV0 {
    fn new(maximum: usize) -> Result<Self, DemoErrorV0> {
        let mut bytes = Vec::new();
        bytes
            .try_reserve_exact(maximum)
            .map_err(|_| DemoErrorV0::OutputAllocation)?;
        Ok(Self { bytes, maximum })
    }

    fn finish(self) -> Vec<u8> {
        self.bytes
    }
}

impl Write for BoundedOutputV0 {
    fn write_str(&mut self, value: &str) -> fmt::Result {
        let next_len = self
            .bytes
            .len()
            .checked_add(value.len())
            .ok_or(fmt::Error)?;
        if next_len > self.maximum {
            return Err(fmt::Error);
        }
        self.bytes.extend_from_slice(value.as_bytes());
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bounded_writer_accepts_exact_capacity_and_refuses_before_crossing_it() {
        let mut output = BoundedOutputV0::new(3).unwrap();
        output.write_str("abc").unwrap();
        let before = output.bytes.clone();
        assert_eq!(output.write_str("d"), Err(fmt::Error));
        assert_eq!(output.bytes, before);
    }

    #[test]
    fn internal_error_diagnostics_are_fixed_categories() {
        let errors = [
            DemoErrorV0::ScenarioContract,
            DemoErrorV0::GeneratorConfiguration,
            DemoErrorV0::GeneratorRecord,
            DemoErrorV0::ChannelConfiguration,
            DemoErrorV0::ChannelEnqueue,
            DemoErrorV0::ChannelReceive,
            DemoErrorV0::AdmissionConfiguration,
            DemoErrorV0::AdmissionApply,
            DemoErrorV0::UnexpectedAdmissionOutcome,
            DemoErrorV0::UnexpectedFinalizationOutcome,
            DemoErrorV0::TerminalOrdering,
            DemoErrorV0::AccountingInvariant,
            DemoErrorV0::CompilerConfiguration,
            DemoErrorV0::CompilerExecution,
            DemoErrorV0::CompilerResponseInvariant,
            DemoErrorV0::CompilerEncodingInvariant,
            DemoErrorV0::OutputAllocation,
            DemoErrorV0::OutputCapacity,
        ];
        let diagnostics = errors.map(|error| format!("{error:?}"));
        assert_eq!(
            diagnostics,
            [
                "ScenarioContract",
                "GeneratorConfiguration",
                "GeneratorRecord",
                "ChannelConfiguration",
                "ChannelEnqueue",
                "ChannelReceive",
                "AdmissionConfiguration",
                "AdmissionApply",
                "UnexpectedAdmissionOutcome",
                "UnexpectedFinalizationOutcome",
                "TerminalOrdering",
                "AccountingInvariant",
                "CompilerConfiguration",
                "CompilerExecution",
                "CompilerResponseInvariant",
                "CompilerEncodingInvariant",
                "OutputAllocation",
                "OutputCapacity",
            ]
        );
    }

    #[test]
    fn repeated_in_process_demo_executions_are_byte_identical_and_bounded() {
        let first = run_sre_demo_v0().unwrap();
        let second = run_sre_demo_v0().unwrap();
        assert_eq!(first, second);
        assert!(first.len() <= MAX_DEMO_OUTPUT_BYTES_V0);
        assert!(first.ends_with(b"}\n"));
    }
}
