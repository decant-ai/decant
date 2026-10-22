//! Bounded, outcome-bearing replay for one in-memory admission epoch.

use std::error::Error;
use std::fmt;

use decant_ir::{EventEnvelopeV0, SourceIdV0};

use crate::{
    AdmissionConfigV0, AdmissionErrorV0, AdmissionSnapshotDecodeErrorV0,
    AdmissionSnapshotDecodeErrorV1, AdmissionSnapshotEncodeErrorV0, AdmissionSnapshotEncodeErrorV1,
    AdmissionStateV0, ApplyOutcomeV0, FinalizeOutcomeV0, MAX_ACCEPTED_EVENT_BYTES_V0,
    accepted_event_accounted_bytes_v0, encode_admission_snapshot_v0, encode_admission_snapshot_v1,
    restore_admission_snapshot_v0, restore_admission_snapshot_v1,
};

pub const ADMISSION_REPLAY_VERSION_V0: u16 = 0;
pub const ADMISSION_REPLAY_TRANSITION_VERSION_V1: u16 = 1;
pub const MAX_ADMISSION_REPLAY_INPUTS_V0: usize = 100_001;
pub const MAX_ADMISSION_REPLAY_BYTES_V0: usize = MAX_ACCEPTED_EVENT_BYTES_V0;

const INPUT_TAG_WIDTH: usize = 1;
const LENGTH_PREFIX_WIDTH: usize = 4;
const TIMESTAMP_WIDTH: usize = 8;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AdmissionReplayLimitFieldV0 {
    Inputs,
    Bytes,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AdmissionReplayAccountingFieldV0 {
    Inputs,
    Bytes,
    FinalizeSource,
}

#[derive(Clone, Eq, PartialEq)]
pub enum AdmissionReplayConfigErrorV0 {
    UnsupportedVersion {
        actual: u16,
    },
    ZeroLimit {
        field: AdmissionReplayLimitFieldV0,
    },
    AboveMaximum {
        field: AdmissionReplayLimitFieldV0,
        maximum: usize,
        actual: usize,
    },
}

impl fmt::Debug for AdmissionReplayConfigErrorV0 {
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

impl fmt::Display for AdmissionReplayConfigErrorV0 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(self, formatter)
    }
}

impl Error for AdmissionReplayConfigErrorV0 {}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct AdmissionReplayLimitsV0 {
    input_limit: usize,
    byte_limit: usize,
}

impl AdmissionReplayLimitsV0 {
    pub fn new(
        input_limit: usize,
        byte_limit: usize,
    ) -> Result<Self, AdmissionReplayConfigErrorV0> {
        validate_limit(
            input_limit,
            MAX_ADMISSION_REPLAY_INPUTS_V0,
            AdmissionReplayLimitFieldV0::Inputs,
        )?;
        validate_limit(
            byte_limit,
            MAX_ADMISSION_REPLAY_BYTES_V0,
            AdmissionReplayLimitFieldV0::Bytes,
        )?;
        Ok(Self {
            input_limit,
            byte_limit,
        })
    }

    pub const fn maximum() -> Self {
        Self {
            input_limit: MAX_ADMISSION_REPLAY_INPUTS_V0,
            byte_limit: MAX_ADMISSION_REPLAY_BYTES_V0,
        }
    }

    pub const fn input_limit(self) -> usize {
        self.input_limit
    }

    pub const fn byte_limit(self) -> usize {
        self.byte_limit
    }
}

fn validate_limit(
    actual: usize,
    maximum: usize,
    field: AdmissionReplayLimitFieldV0,
) -> Result<(), AdmissionReplayConfigErrorV0> {
    if actual == 0 {
        return Err(AdmissionReplayConfigErrorV0::ZeroLimit { field });
    }
    if actual > maximum {
        return Err(AdmissionReplayConfigErrorV0::AboveMaximum {
            field,
            maximum,
            actual,
        });
    }
    Ok(())
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AdmissionReplayInputKindV0 {
    Event,
    FinalizeSource,
}

#[derive(Clone, Eq, PartialEq)]
pub enum AdmissionReplayInputV0 {
    Event(EventEnvelopeV0),
    FinalizeSource {
        source_id: SourceIdV0,
        finalized_through_ns: i64,
    },
}

impl AdmissionReplayInputV0 {
    pub const fn kind(&self) -> AdmissionReplayInputKindV0 {
        match self {
            Self::Event(_) => AdmissionReplayInputKindV0::Event,
            Self::FinalizeSource { .. } => AdmissionReplayInputKindV0::FinalizeSource,
        }
    }

    pub fn as_event(&self) -> Option<&EventEnvelopeV0> {
        match self {
            Self::Event(event) => Some(event),
            Self::FinalizeSource { .. } => None,
        }
    }

    pub fn as_finalization(&self) -> Option<(&SourceIdV0, i64)> {
        match self {
            Self::Event(_) => None,
            Self::FinalizeSource {
                source_id,
                finalized_through_ns,
            } => Some((source_id, *finalized_through_ns)),
        }
    }
}

impl fmt::Debug for AdmissionReplayInputV0 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("AdmissionReplayInputV0")
            .field("kind", &self.kind())
            .finish_non_exhaustive()
    }
}

#[derive(Clone, Eq, PartialEq)]
pub enum AdmissionReplayAppendErrorV0 {
    InputAccounting(AdmissionErrorV0),
    ArithmeticOverflow {
        field: AdmissionReplayAccountingFieldV0,
    },
    CapacityExceeded {
        field: AdmissionReplayLimitFieldV0,
        maximum: usize,
        attempted: usize,
    },
    AllocationFailed,
}

impl fmt::Debug for AdmissionReplayAppendErrorV0 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InputAccounting(error) => formatter
                .debug_tuple("InputAccounting")
                .field(error)
                .finish(),
            Self::ArithmeticOverflow { field } => formatter
                .debug_struct("ArithmeticOverflow")
                .field("field", field)
                .finish(),
            Self::CapacityExceeded {
                field,
                maximum,
                attempted,
            } => formatter
                .debug_struct("CapacityExceeded")
                .field("field", field)
                .field("maximum", maximum)
                .field("attempted", attempted)
                .finish(),
            Self::AllocationFailed => formatter.write_str("AllocationFailed"),
        }
    }
}

impl fmt::Display for AdmissionReplayAppendErrorV0 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(self, formatter)
    }
}

impl Error for AdmissionReplayAppendErrorV0 {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::InputAccounting(error) => Some(error),
            Self::ArithmeticOverflow { .. }
            | Self::CapacityExceeded { .. }
            | Self::AllocationFailed => None,
        }
    }
}

pub fn admission_replay_input_accounted_bytes_v0(
    input: &AdmissionReplayInputV0,
) -> Result<usize, AdmissionReplayAppendErrorV0> {
    match input {
        AdmissionReplayInputV0::Event(event) => {
            let event_bytes = accepted_event_accounted_bytes_v0(event)
                .map_err(AdmissionReplayAppendErrorV0::InputAccounting)?;
            INPUT_TAG_WIDTH.checked_add(event_bytes).ok_or(
                AdmissionReplayAppendErrorV0::ArithmeticOverflow {
                    field: AdmissionReplayAccountingFieldV0::Bytes,
                },
            )
        }
        AdmissionReplayInputV0::FinalizeSource { source_id, .. } => INPUT_TAG_WIDTH
            .checked_add(LENGTH_PREFIX_WIDTH)
            .and_then(|bytes| bytes.checked_add(source_id.as_str().len()))
            .and_then(|bytes| bytes.checked_add(TIMESTAMP_WIDTH))
            .ok_or(AdmissionReplayAppendErrorV0::ArithmeticOverflow {
                field: AdmissionReplayAccountingFieldV0::FinalizeSource,
            }),
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct AdmissionReplayUsageV0 {
    limits: AdmissionReplayLimitsV0,
    inputs: usize,
    bytes: usize,
}

impl AdmissionReplayUsageV0 {
    pub const fn limits(self) -> AdmissionReplayLimitsV0 {
        self.limits
    }

    pub const fn inputs(self) -> usize {
        self.inputs
    }

    pub const fn bytes(self) -> usize {
        self.bytes
    }
}

#[derive(Clone, Eq, PartialEq)]
pub struct AdmissionReplayJournalV0 {
    version: u16,
    limits: AdmissionReplayLimitsV0,
    inputs: Vec<AdmissionReplayInputV0>,
    accounted_bytes: usize,
}

impl AdmissionReplayJournalV0 {
    pub fn new(
        version: u16,
        limits: AdmissionReplayLimitsV0,
    ) -> Result<Self, AdmissionReplayConfigErrorV0> {
        if version != ADMISSION_REPLAY_VERSION_V0 {
            return Err(AdmissionReplayConfigErrorV0::UnsupportedVersion { actual: version });
        }
        Ok(Self {
            version,
            limits,
            inputs: Vec::new(),
            accounted_bytes: 0,
        })
    }

    pub const fn version(&self) -> u16 {
        self.version
    }

    pub const fn limits(&self) -> AdmissionReplayLimitsV0 {
        self.limits
    }

    pub fn len(&self) -> usize {
        self.inputs.len()
    }

    pub fn is_empty(&self) -> bool {
        self.inputs.is_empty()
    }

    pub fn usage(&self) -> AdmissionReplayUsageV0 {
        AdmissionReplayUsageV0 {
            limits: self.limits,
            inputs: self.inputs.len(),
            bytes: self.accounted_bytes,
        }
    }

    pub fn inputs(&self) -> &[AdmissionReplayInputV0] {
        &self.inputs
    }

    #[allow(
        clippy::result_large_err,
        reason = "the refused owned input must be returned without another allocation"
    )]
    pub fn try_push(
        &mut self,
        input: AdmissionReplayInputV0,
    ) -> Result<(), (AdmissionReplayAppendErrorV0, AdmissionReplayInputV0)> {
        let input_bytes = match admission_replay_input_accounted_bytes_v0(&input) {
            Ok(bytes) => bytes,
            Err(error) => return Err((error, input)),
        };
        let prospective_inputs = match self.inputs.len().checked_add(1) {
            Some(inputs) => inputs,
            None => {
                return Err((
                    AdmissionReplayAppendErrorV0::ArithmeticOverflow {
                        field: AdmissionReplayAccountingFieldV0::Inputs,
                    },
                    input,
                ));
            }
        };
        let prospective_bytes = match self.accounted_bytes.checked_add(input_bytes) {
            Some(bytes) => bytes,
            None => {
                return Err((
                    AdmissionReplayAppendErrorV0::ArithmeticOverflow {
                        field: AdmissionReplayAccountingFieldV0::Bytes,
                    },
                    input,
                ));
            }
        };
        if prospective_inputs > self.limits.input_limit {
            return Err((
                AdmissionReplayAppendErrorV0::CapacityExceeded {
                    field: AdmissionReplayLimitFieldV0::Inputs,
                    maximum: self.limits.input_limit,
                    attempted: prospective_inputs,
                },
                input,
            ));
        }
        if prospective_bytes > self.limits.byte_limit {
            return Err((
                AdmissionReplayAppendErrorV0::CapacityExceeded {
                    field: AdmissionReplayLimitFieldV0::Bytes,
                    maximum: self.limits.byte_limit,
                    attempted: prospective_bytes,
                },
                input,
            ));
        }
        if self.inputs.try_reserve_exact(1).is_err() {
            return Err((AdmissionReplayAppendErrorV0::AllocationFailed, input));
        }
        self.inputs.push(input);
        self.accounted_bytes = prospective_bytes;
        Ok(())
    }
}

impl fmt::Debug for AdmissionReplayJournalV0 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("AdmissionReplayJournalV0")
            .field("version", &self.version)
            .field("usage", &self.usage())
            .finish_non_exhaustive()
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum AdmissionReplayOutcomeV0 {
    Event(Result<ApplyOutcomeV0, AdmissionErrorV0>),
    FinalizeSource(FinalizeOutcomeV0),
}

pub struct AdmissionReplayReportV0 {
    state: AdmissionStateV0,
    outcomes: Vec<AdmissionReplayOutcomeV0>,
}

impl AdmissionReplayReportV0 {
    pub fn state(&self) -> &AdmissionStateV0 {
        &self.state
    }

    pub fn outcomes(&self) -> &[AdmissionReplayOutcomeV0] {
        &self.outcomes
    }

    pub fn into_state(self) -> AdmissionStateV0 {
        self.state
    }
}

impl fmt::Debug for AdmissionReplayReportV0 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("AdmissionReplayReportV0")
            .field("outcome_count", &self.outcomes.len())
            .field("state_usage", &self.state.usage())
            .field("finalized", &self.state.is_finalized())
            .finish_non_exhaustive()
    }
}

#[derive(Clone, Eq, PartialEq)]
pub enum AdmissionReplayErrorV0 {
    InterruptionOutOfRange { maximum: usize, actual: usize },
    OutcomeAllocationFailed { requested: usize },
    SnapshotEncoding(AdmissionSnapshotEncodeErrorV0),
    SnapshotDecoding(AdmissionSnapshotDecodeErrorV0),
}

impl fmt::Debug for AdmissionReplayErrorV0 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InterruptionOutOfRange { maximum, actual } => formatter
                .debug_struct("InterruptionOutOfRange")
                .field("maximum", maximum)
                .field("actual", actual)
                .finish(),
            Self::OutcomeAllocationFailed { requested } => formatter
                .debug_struct("OutcomeAllocationFailed")
                .field("requested", requested)
                .finish(),
            Self::SnapshotEncoding(error) => formatter
                .debug_tuple("SnapshotEncoding")
                .field(error)
                .finish(),
            Self::SnapshotDecoding(error) => formatter
                .debug_tuple("SnapshotDecoding")
                .field(error)
                .finish(),
        }
    }
}

impl fmt::Display for AdmissionReplayErrorV0 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(self, formatter)
    }
}

impl Error for AdmissionReplayErrorV0 {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::SnapshotEncoding(error) => Some(error),
            Self::SnapshotDecoding(error) => Some(error),
            Self::InterruptionOutOfRange { .. } | Self::OutcomeAllocationFailed { .. } => None,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum AdmissionReplayOutcomeV1 {
    Event(Result<ApplyOutcomeV0, AdmissionErrorV0>),
    FinalizeSource(FinalizeOutcomeV0),
}

pub struct AdmissionReplayReportV1 {
    state: AdmissionStateV0,
    outcomes: Vec<AdmissionReplayOutcomeV1>,
}

impl AdmissionReplayReportV1 {
    pub fn state(&self) -> &AdmissionStateV0 {
        &self.state
    }

    pub fn outcomes(&self) -> &[AdmissionReplayOutcomeV1] {
        &self.outcomes
    }

    pub fn into_state(self) -> AdmissionStateV0 {
        self.state
    }
}

impl fmt::Debug for AdmissionReplayReportV1 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("AdmissionReplayReportV1")
            .field("outcome_count", &self.outcomes.len())
            .field("state_usage", &self.state.usage())
            .field("finalized", &self.state.is_finalized())
            .finish_non_exhaustive()
    }
}

#[derive(Clone, Eq, PartialEq)]
pub enum AdmissionReplayErrorV1 {
    InterruptionOutOfRange { maximum: usize, actual: usize },
    OutcomeAllocationFailed { requested: usize },
    SnapshotEncoding(AdmissionSnapshotEncodeErrorV1),
    SnapshotDecoding(AdmissionSnapshotDecodeErrorV1),
}

impl fmt::Debug for AdmissionReplayErrorV1 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InterruptionOutOfRange { maximum, actual } => formatter
                .debug_struct("InterruptionOutOfRange")
                .field("maximum", maximum)
                .field("actual", actual)
                .finish(),
            Self::OutcomeAllocationFailed { requested } => formatter
                .debug_struct("OutcomeAllocationFailed")
                .field("requested", requested)
                .finish(),
            Self::SnapshotEncoding(error) => formatter
                .debug_tuple("SnapshotEncoding")
                .field(error)
                .finish(),
            Self::SnapshotDecoding(error) => formatter
                .debug_tuple("SnapshotDecoding")
                .field(error)
                .finish(),
        }
    }
}

impl fmt::Display for AdmissionReplayErrorV1 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(self, formatter)
    }
}

impl Error for AdmissionReplayErrorV1 {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::SnapshotEncoding(error) => Some(error),
            Self::SnapshotDecoding(error) => Some(error),
            Self::InterruptionOutOfRange { .. } | Self::OutcomeAllocationFailed { .. } => None,
        }
    }
}

pub fn replay_admission_journal_v0(
    config: &AdmissionConfigV0,
    journal: &AdmissionReplayJournalV0,
) -> Result<AdmissionReplayReportV0, AdmissionReplayErrorV0> {
    replay(config, journal, None)
}

pub fn replay_admission_journal_with_interruption_v0(
    config: &AdmissionConfigV0,
    journal: &AdmissionReplayJournalV0,
    interruption_after: usize,
) -> Result<AdmissionReplayReportV0, AdmissionReplayErrorV0> {
    if interruption_after > journal.len() {
        return Err(AdmissionReplayErrorV0::InterruptionOutOfRange {
            maximum: journal.len(),
            actual: interruption_after,
        });
    }
    replay(config, journal, Some(interruption_after))
}

pub fn replay_admission_journal_v1(
    config: &AdmissionConfigV0,
    journal: &AdmissionReplayJournalV0,
) -> Result<AdmissionReplayReportV1, AdmissionReplayErrorV1> {
    replay_v1(config, journal, None)
}

pub fn replay_admission_journal_with_interruption_v1(
    config: &AdmissionConfigV0,
    journal: &AdmissionReplayJournalV0,
    interruption_after: usize,
) -> Result<AdmissionReplayReportV1, AdmissionReplayErrorV1> {
    if interruption_after > journal.len() {
        return Err(AdmissionReplayErrorV1::InterruptionOutOfRange {
            maximum: journal.len(),
            actual: interruption_after,
        });
    }
    replay_v1(config, journal, Some(interruption_after))
}

fn replay(
    config: &AdmissionConfigV0,
    journal: &AdmissionReplayJournalV0,
    interruption_after: Option<usize>,
) -> Result<AdmissionReplayReportV0, AdmissionReplayErrorV0> {
    let mut outcomes = Vec::new();
    outcomes.try_reserve_exact(journal.len()).map_err(|_| {
        AdmissionReplayErrorV0::OutcomeAllocationFailed {
            requested: journal.len(),
        }
    })?;
    let mut state = AdmissionStateV0::new(config.clone());
    for (index, input) in journal.inputs().iter().enumerate() {
        if interruption_after == Some(index) {
            state = round_trip_state(&state)?;
        }
        outcomes.push(apply_input(&mut state, input));
    }
    if interruption_after == Some(journal.len()) {
        state = round_trip_state(&state)?;
    }
    Ok(AdmissionReplayReportV0 { state, outcomes })
}

fn apply_input(
    state: &mut AdmissionStateV0,
    input: &AdmissionReplayInputV0,
) -> AdmissionReplayOutcomeV0 {
    match input {
        AdmissionReplayInputV0::Event(event) => {
            AdmissionReplayOutcomeV0::Event(state.apply_event(event))
        }
        AdmissionReplayInputV0::FinalizeSource {
            source_id,
            finalized_through_ns,
        } => AdmissionReplayOutcomeV0::FinalizeSource(
            state.finalize_source(source_id, *finalized_through_ns),
        ),
    }
}

fn round_trip_state(state: &AdmissionStateV0) -> Result<AdmissionStateV0, AdmissionReplayErrorV0> {
    let snapshot =
        encode_admission_snapshot_v0(state).map_err(AdmissionReplayErrorV0::SnapshotEncoding)?;
    restore_admission_snapshot_v0(&snapshot).map_err(AdmissionReplayErrorV0::SnapshotDecoding)
}

fn replay_v1(
    config: &AdmissionConfigV0,
    journal: &AdmissionReplayJournalV0,
    interruption_after: Option<usize>,
) -> Result<AdmissionReplayReportV1, AdmissionReplayErrorV1> {
    let mut outcomes = Vec::new();
    outcomes.try_reserve_exact(journal.len()).map_err(|_| {
        AdmissionReplayErrorV1::OutcomeAllocationFailed {
            requested: journal.len(),
        }
    })?;
    let mut state = AdmissionStateV0::new(config.clone());
    for (index, input) in journal.inputs().iter().enumerate() {
        if interruption_after == Some(index) {
            state = round_trip_state_v1(&state, config)?;
        }
        outcomes.push(apply_input_v1(&mut state, input));
    }
    if interruption_after == Some(journal.len()) {
        state = round_trip_state_v1(&state, config)?;
    }
    Ok(AdmissionReplayReportV1 { state, outcomes })
}

fn apply_input_v1(
    state: &mut AdmissionStateV0,
    input: &AdmissionReplayInputV0,
) -> AdmissionReplayOutcomeV1 {
    match input {
        AdmissionReplayInputV0::Event(event) => {
            AdmissionReplayOutcomeV1::Event(state.apply_event_v1(event))
        }
        AdmissionReplayInputV0::FinalizeSource {
            source_id,
            finalized_through_ns,
        } => AdmissionReplayOutcomeV1::FinalizeSource(
            state.finalize_source(source_id, *finalized_through_ns),
        ),
    }
}

fn round_trip_state_v1(
    state: &AdmissionStateV0,
    expected_config: &AdmissionConfigV0,
) -> Result<AdmissionStateV0, AdmissionReplayErrorV1> {
    let snapshot =
        encode_admission_snapshot_v1(state).map_err(AdmissionReplayErrorV1::SnapshotEncoding)?;
    restore_admission_snapshot_v1(&snapshot, expected_config)
        .map_err(AdmissionReplayErrorV1::SnapshotDecoding)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn finalization_accounting_uses_checked_components() {
        let input = AdmissionReplayInputV0::FinalizeSource {
            source_id: SourceIdV0::new("s").unwrap(),
            finalized_through_ns: 99,
        };
        assert_eq!(
            admission_replay_input_accounted_bytes_v0(&input),
            Ok(INPUT_TAG_WIDTH + LENGTH_PREFIX_WIDTH + 1 + TIMESTAMP_WIDTH)
        );
    }
}
