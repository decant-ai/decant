//! Deterministic semantic admission for one finite Decant V0 epoch.
//!
//! Transport insertion is not semantic acceptance. This crate classifies
//! canonical events before any materialized operator exists.

#![forbid(unsafe_code)]

pub mod sre_evidence;

mod changed;
mod grouped_interval_ratio_ppm;
mod interval_ratio_ppm;
mod latest;
mod preceding_event;
mod projected_field_change;
mod replay;
mod retained_changed;
mod retained_latest;
mod retained_tumbling_count;
mod retention;
mod snapshot;
mod state_hash;
mod tumbling_count;

use std::collections::BTreeMap;
use std::error::Error;
use std::fmt;

use decant_ir::{
    EventEnvelopeV0, RedeliveryClassificationV0, SourceIdV0, SourcePositionKindV0,
    SourcePositionV0, ValidationErrorV0,
};

pub use changed::{
    CHANGED_BY_KEY_VERSION_V0, ChangeObservationV0, ChangedByKeyConfigErrorV0,
    ChangedByKeyConfigV0, ChangedByKeyLimitFieldV0, ChangedByKeyLimitsV0, ChangedByKeyUsageV0,
    ChangedByKeyViewV0, ChangedIndexAccountingComponentV0, ChangedMaterializationErrorV0,
    MAX_CHANGED_INDEX_BYTES_V0, MAX_CHANGED_INDEX_ENTRIES_V0,
    changed_index_entry_accounted_bytes_v0,
};

pub use grouped_interval_ratio_ppm::{
    GROUPED_INTERVAL_RATIO_PPM_VERSION_V0, GroupedIntervalRatioAccountingV0,
    GroupedIntervalRatioAggregateV0, GroupedIntervalRatioConfigErrorV0,
    GroupedIntervalRatioFieldIssueV0, GroupedIntervalRatioFieldRoleV0,
    GroupedIntervalRatioLimitFieldV0, GroupedIntervalRatioLimitsV0,
    GroupedIntervalRatioMaterializationErrorV0, GroupedIntervalRatioPpmConfigV0,
    GroupedIntervalRatioPpmViewV0, GroupedIntervalRatioUsageV0,
    MAX_GROUPED_INTERVAL_RATIO_EVIDENCE_V0, MAX_GROUPED_INTERVAL_RATIO_GROUPS_V0,
    MAX_GROUPED_INTERVAL_RATIO_INDEX_BYTES_V0,
};

pub use interval_ratio_ppm::{
    INTERVAL_RATIO_PPM_VERSION_V0, IntervalRatioPpmAccountingComponentV0,
    IntervalRatioPpmConfigErrorV0, IntervalRatioPpmConfigV0,
    IntervalRatioPpmMaterializationErrorV0, IntervalRatioPpmObservationV0, IntervalRatioPpmUsageV0,
    IntervalRatioPpmViewV0, MAX_INTERVAL_RATIO_EXAMPLES_V0, PARTS_PER_MILLION_V0,
};

pub use latest::{
    LATEST_BY_KEY_VERSION_V0, LatestByKeyConfigErrorV0, LatestByKeyConfigV0,
    LatestByKeyLimitFieldV0, LatestByKeyLimitsV0, LatestByKeyUsageV0, LatestByKeyViewV0,
    LatestIndexAccountingComponentV0, LatestMaterializationErrorV0, MAX_LATEST_INDEX_BYTES_V0,
    MAX_LATEST_INDEX_ENTRIES_V0, latest_index_entry_accounted_bytes_v0,
};
pub use preceding_event::{
    PRECEDING_EVENT_SELECTOR_VERSION_V0, PrecedingEventSelectorAccountingComponentV0,
    PrecedingEventSelectorConfigErrorV0, PrecedingEventSelectorConfigV0,
    PrecedingEventSelectorErrorV0, PrecedingEventSelectorUsageV0, PrecedingEventSelectorViewV0,
};
pub use projected_field_change::{
    MAX_PROJECTED_FIELD_CHANGE_ENTRIES_V0, MAX_PROJECTED_FIELD_CHANGE_INDEX_BYTES_V0,
    PROJECTED_FIELD_CHANGE_VERSION_V0, ProjectedFieldChangeAccountingComponentV0,
    ProjectedFieldChangeConfigErrorV0, ProjectedFieldChangeConfigV0,
    ProjectedFieldChangeLimitFieldV0, ProjectedFieldChangeLimitsV0,
    ProjectedFieldChangeMaterializationErrorV0, ProjectedFieldChangeObservationV0,
    ProjectedFieldChangeUsageV0, ProjectedFieldChangeViewV0, ProjectedFieldContractIssueV0,
    ProjectedFieldRoleV0, projected_field_change_entry_accounted_bytes_v0,
};
pub use replay::{
    ADMISSION_REPLAY_TRANSITION_VERSION_V1, ADMISSION_REPLAY_VERSION_V0,
    AdmissionReplayAccountingFieldV0, AdmissionReplayAppendErrorV0, AdmissionReplayConfigErrorV0,
    AdmissionReplayErrorV0, AdmissionReplayErrorV1, AdmissionReplayInputKindV0,
    AdmissionReplayInputV0, AdmissionReplayJournalV0, AdmissionReplayLimitFieldV0,
    AdmissionReplayLimitsV0, AdmissionReplayOutcomeV0, AdmissionReplayOutcomeV1,
    AdmissionReplayReportV0, AdmissionReplayReportV1, AdmissionReplayUsageV0,
    MAX_ADMISSION_REPLAY_BYTES_V0, MAX_ADMISSION_REPLAY_INPUTS_V0,
    admission_replay_input_accounted_bytes_v0, replay_admission_journal_v0,
    replay_admission_journal_v1, replay_admission_journal_with_interruption_v0,
    replay_admission_journal_with_interruption_v1,
};
pub use retained_changed::{
    RETAINED_CHANGED_BY_KEY_VERSION_V0, RetainedChangedByKeyConfigErrorV0,
    RetainedChangedByKeyConfigV0, RetainedChangedByKeyViewV0,
};
pub use retained_latest::{
    RETAINED_LATEST_BY_KEY_VERSION_V0, RetainedLatestByKeyConfigErrorV0,
    RetainedLatestByKeyConfigV0, RetainedLatestByKeyViewV0,
};
pub use retained_tumbling_count::{
    RETAINED_TUMBLING_COUNT_VERSION_V0, RetainedCountWindowObservationV0,
    RetainedTumblingCountConfigErrorV0, RetainedTumblingCountConfigV0, RetainedTumblingCountViewV0,
};
pub use retention::{
    EVENT_TIME_RETENTION_VERSION_V0, EventTimeRetentionConfigErrorV0, EventTimeRetentionConfigV0,
    EventTimeRetentionUsageV0, EventTimeRetentionViewV0,
};
pub use snapshot::{
    ADMISSION_SNAPSHOT_VERSION_V0, ADMISSION_SNAPSHOT_VERSION_V1, AdmissionSnapshotDecodeErrorV0,
    AdmissionSnapshotDecodeErrorV1, AdmissionSnapshotEncodeErrorV0, AdmissionSnapshotEncodeErrorV1,
    AdmissionSnapshotExpectedConfigFieldV1, AdmissionSnapshotFieldV0, AdmissionSnapshotFieldV1,
    AdmissionSnapshotOrderV0, AdmissionSnapshotOrderV1, MAX_ADMISSION_SNAPSHOT_BYTES_V0,
    MAX_ADMISSION_SNAPSHOT_BYTES_V1, encode_admission_snapshot_v0, encode_admission_snapshot_v1,
    restore_admission_snapshot_v0, restore_admission_snapshot_v1,
};
pub use state_hash::{
    ADMISSION_STATE_HASH_ALGORITHM_V0, ADMISSION_STATE_HASH_ALGORITHM_V1,
    ADMISSION_STATE_HASH_DOMAIN_V0, ADMISSION_STATE_HASH_DOMAIN_V1,
    ADMISSION_STATE_HASH_TEXT_BYTES_V0, ADMISSION_STATE_HASH_TEXT_BYTES_V1,
    ADMISSION_STATE_HASH_VERSION_V0, ADMISSION_STATE_HASH_VERSION_V1, AdmissionStateHashErrorV0,
    AdmissionStateHashErrorV1, AdmissionStateHashV0, AdmissionStateHashV1, hash_admission_state_v0,
    hash_admission_state_v1,
};
pub use tumbling_count::{
    CountIndexAccountingComponentV0, CountMaterializationErrorV0, CountWindowObservationV0,
    MAX_TUMBLING_COUNT_BUCKETS_V0, MAX_TUMBLING_COUNT_INDEX_BYTES_V0, TUMBLING_COUNT_VERSION_V0,
    TumblingCountConfigErrorV0, TumblingCountConfigV0, TumblingCountLimitFieldV0,
    TumblingCountLimitsV0, TumblingCountUsageV0, TumblingCountViewV0,
    tumbling_count_bucket_accounted_bytes_v0, tumbling_count_contribution_accounted_bytes_v0,
};

pub const ADMISSION_VERSION_V0: u16 = 0;
pub const ADMISSION_EVENT_TRANSITION_VERSION_V1: u16 = 1;
pub const ALLOWED_LATENESS_NS_V0: i64 = 30_000_000_000;
pub const MAX_ACCEPTED_IDENTITIES_V0: usize = 100_000;
pub const MAX_ACCEPTED_EVENT_BYTES_V0: usize = 128 * 1_024 * 1_024;
pub const REJECTION_CLASS_COUNT_V0: usize = 8;

const ENVELOPE_VERSION_WIDTH: usize = 2;
const LENGTH_PREFIX_WIDTH: usize = 4;
const SOURCE_POSITION_TAG_WIDTH: usize = 1;
const SYNTHETIC_SEQUENCE_WIDTH: usize = 8;
const OTLP_FILE_LINE_NUMBER_WIDTH: usize = 8;
const OTLP_FILE_INDEX_WIDTH: usize = 4;
const KAFKA_PARTITION_WIDTH: usize = 4;
const KAFKA_OFFSET_WIDTH: usize = 8;
const KEY_PRESENCE_WIDTH: usize = 1;
const TIMESTAMP_WIDTH: usize = 8;
const SENSITIVITY_WIDTH: usize = 8;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AdmissionLimitFieldV0 {
    AcceptedIdentities,
    AcceptedEventBytes,
}

#[derive(Clone, Eq, PartialEq)]
pub enum AdmissionConfigErrorV0 {
    UnsupportedVersion {
        actual: u16,
    },
    InvalidEpoch,
    ZeroLimit {
        field: AdmissionLimitFieldV0,
    },
    AboveMaximum {
        field: AdmissionLimitFieldV0,
        maximum: usize,
        actual: usize,
    },
}

impl fmt::Debug for AdmissionConfigErrorV0 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::UnsupportedVersion { actual } => formatter
                .debug_struct("UnsupportedVersion")
                .field("actual", actual)
                .finish(),
            Self::InvalidEpoch => formatter.write_str("InvalidEpoch"),
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

impl fmt::Display for AdmissionConfigErrorV0 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(self, formatter)
    }
}

impl Error for AdmissionConfigErrorV0 {}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct AdmissionLimitsV0 {
    accepted_identity_limit: usize,
    accepted_event_byte_limit: usize,
}

impl AdmissionLimitsV0 {
    pub fn new(
        accepted_identity_limit: usize,
        accepted_event_byte_limit: usize,
    ) -> Result<Self, AdmissionConfigErrorV0> {
        validate_limit(
            accepted_identity_limit,
            MAX_ACCEPTED_IDENTITIES_V0,
            AdmissionLimitFieldV0::AcceptedIdentities,
        )?;
        validate_limit(
            accepted_event_byte_limit,
            MAX_ACCEPTED_EVENT_BYTES_V0,
            AdmissionLimitFieldV0::AcceptedEventBytes,
        )?;
        Ok(Self {
            accepted_identity_limit,
            accepted_event_byte_limit,
        })
    }

    pub const fn maximum() -> Self {
        Self {
            accepted_identity_limit: MAX_ACCEPTED_IDENTITIES_V0,
            accepted_event_byte_limit: MAX_ACCEPTED_EVENT_BYTES_V0,
        }
    }

    pub const fn accepted_identity_limit(self) -> usize {
        self.accepted_identity_limit
    }

    pub const fn accepted_event_byte_limit(self) -> usize {
        self.accepted_event_byte_limit
    }
}

fn validate_limit(
    actual: usize,
    maximum: usize,
    field: AdmissionLimitFieldV0,
) -> Result<(), AdmissionConfigErrorV0> {
    if actual == 0 {
        return Err(AdmissionConfigErrorV0::ZeroLimit { field });
    }
    if actual > maximum {
        return Err(AdmissionConfigErrorV0::AboveMaximum {
            field,
            maximum,
            actual,
        });
    }
    Ok(())
}

#[derive(Clone, Eq, PartialEq)]
/// Immutable admission configuration for one finite source epoch.
///
/// Invariant-bearing fields cannot be changed outside this crate:
///
/// ```compile_fail
/// use decant_ir::SourcePositionKindV0;
/// use decant_state::AdmissionConfigV0;
///
/// let mut config: AdmissionConfigV0 = todo!();
/// config.source_position_kind = SourcePositionKindV0::Kafka;
/// ```
pub struct AdmissionConfigV0 {
    version: u16,
    source_id: SourceIdV0,
    source_position_kind: SourcePositionKindV0,
    epoch_start_ns: i64,
    epoch_end_ns: i64,
    limits: AdmissionLimitsV0,
}

impl AdmissionConfigV0 {
    pub fn new(
        version: u16,
        source_id: SourceIdV0,
        epoch_start_ns: i64,
        epoch_end_ns: i64,
        limits: AdmissionLimitsV0,
    ) -> Result<Self, AdmissionConfigErrorV0> {
        Self::new_with_source_position_kind(
            version,
            source_id,
            SourcePositionKindV0::Synthetic,
            epoch_start_ns,
            epoch_end_ns,
            limits,
        )
    }

    pub fn new_with_source_position_kind(
        version: u16,
        source_id: SourceIdV0,
        source_position_kind: SourcePositionKindV0,
        epoch_start_ns: i64,
        epoch_end_ns: i64,
        limits: AdmissionLimitsV0,
    ) -> Result<Self, AdmissionConfigErrorV0> {
        if version != ADMISSION_VERSION_V0 {
            return Err(AdmissionConfigErrorV0::UnsupportedVersion { actual: version });
        }
        if epoch_start_ns >= epoch_end_ns {
            return Err(AdmissionConfigErrorV0::InvalidEpoch);
        }
        Ok(Self {
            version,
            source_id,
            source_position_kind,
            epoch_start_ns,
            epoch_end_ns,
            limits,
        })
    }

    pub const fn version(&self) -> u16 {
        self.version
    }

    pub fn source_id(&self) -> &SourceIdV0 {
        &self.source_id
    }

    pub const fn source_position_kind(&self) -> SourcePositionKindV0 {
        self.source_position_kind
    }

    pub const fn epoch_start_ns(&self) -> i64 {
        self.epoch_start_ns
    }

    pub const fn epoch_end_ns(&self) -> i64 {
        self.epoch_end_ns
    }

    pub const fn limits(&self) -> AdmissionLimitsV0 {
        self.limits
    }
}

impl fmt::Debug for AdmissionConfigV0 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("AdmissionConfigV0")
            .field("version", &self.version)
            .field("source_id", &self.source_id)
            .field("source_position_kind", &self.source_position_kind)
            .field("epoch_start_ns", &"<redacted>")
            .field("epoch_end_ns", &"<redacted>")
            .field("limits", &self.limits)
            .finish()
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AccountingComponentV0 {
    EnvelopeVersion,
    SourceId,
    SchemaId,
    SourcePosition,
    Key,
    Timestamps,
    Sensitivity,
    Payload,
    Provenance,
    AcceptedLedger,
}

#[derive(Clone, Eq, PartialEq)]
pub enum AdmissionErrorV0 {
    ArithmeticOverflow {
        component: AccountingComponentV0,
    },
    IdentityInvariantViolation,
    UnexpectedSourcePositionKind {
        expected: SourcePositionKindV0,
        actual: SourcePositionKindV0,
    },
    UnsupportedSourcePosition {
        kind: SourcePositionKindV0,
    },
    IrValidation(ValidationErrorV0),
}

impl fmt::Debug for AdmissionErrorV0 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ArithmeticOverflow { component } => formatter
                .debug_struct("ArithmeticOverflow")
                .field("component", component)
                .finish(),
            Self::IdentityInvariantViolation => formatter.write_str("IdentityInvariantViolation"),
            Self::UnexpectedSourcePositionKind { expected, actual } => formatter
                .debug_struct("UnexpectedSourcePositionKind")
                .field("expected", expected)
                .field("actual", actual)
                .finish(),
            Self::UnsupportedSourcePosition { kind } => formatter
                .debug_struct("UnsupportedSourcePosition")
                .field("kind", kind)
                .finish(),
            Self::IrValidation(error) => {
                formatter.debug_tuple("IrValidation").field(error).finish()
            }
        }
    }
}

impl fmt::Display for AdmissionErrorV0 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(self, formatter)
    }
}

impl Error for AdmissionErrorV0 {}

impl From<ValidationErrorV0> for AdmissionErrorV0 {
    fn from(error: ValidationErrorV0) -> Self {
        Self::IrValidation(error)
    }
}

pub fn accepted_event_accounted_bytes_v0(
    event: &EventEnvelopeV0,
) -> Result<usize, AdmissionErrorV0> {
    let mut bytes = 0;
    checked_add(
        &mut bytes,
        ENVELOPE_VERSION_WIDTH,
        AccountingComponentV0::EnvelopeVersion,
    )?;
    checked_add(
        &mut bytes,
        LENGTH_PREFIX_WIDTH,
        AccountingComponentV0::SourceId,
    )?;
    checked_add(
        &mut bytes,
        event.source_id().as_str().len(),
        AccountingComponentV0::SourceId,
    )?;
    checked_add(
        &mut bytes,
        LENGTH_PREFIX_WIDTH,
        AccountingComponentV0::SchemaId,
    )?;
    checked_add(
        &mut bytes,
        event.schema_id().as_str().len(),
        AccountingComponentV0::SchemaId,
    )?;
    match event.source_position() {
        SourcePositionV0::Synthetic { .. } => {
            checked_add(
                &mut bytes,
                SOURCE_POSITION_TAG_WIDTH,
                AccountingComponentV0::SourcePosition,
            )?;
            checked_add(
                &mut bytes,
                SYNTHETIC_SEQUENCE_WIDTH,
                AccountingComponentV0::SourcePosition,
            )?;
        }
        SourcePositionV0::OtlpFile { .. } => {
            checked_add(
                &mut bytes,
                SOURCE_POSITION_TAG_WIDTH,
                AccountingComponentV0::SourcePosition,
            )?;
            checked_add(
                &mut bytes,
                OTLP_FILE_LINE_NUMBER_WIDTH,
                AccountingComponentV0::SourcePosition,
            )?;
            for _ in 0..3 {
                checked_add(
                    &mut bytes,
                    OTLP_FILE_INDEX_WIDTH,
                    AccountingComponentV0::SourcePosition,
                )?;
            }
        }
        SourcePositionV0::Kafka { .. } => {
            checked_add(
                &mut bytes,
                SOURCE_POSITION_TAG_WIDTH,
                AccountingComponentV0::SourcePosition,
            )?;
            checked_add(
                &mut bytes,
                KAFKA_PARTITION_WIDTH,
                AccountingComponentV0::SourcePosition,
            )?;
            checked_add(
                &mut bytes,
                KAFKA_OFFSET_WIDTH,
                AccountingComponentV0::SourcePosition,
            )?;
        }
    }
    checked_add(&mut bytes, KEY_PRESENCE_WIDTH, AccountingComponentV0::Key)?;
    if let Some(key) = event.key() {
        checked_add(&mut bytes, LENGTH_PREFIX_WIDTH, AccountingComponentV0::Key)?;
        checked_add(&mut bytes, key.len(), AccountingComponentV0::Key)?;
    }
    checked_add(
        &mut bytes,
        TIMESTAMP_WIDTH,
        AccountingComponentV0::Timestamps,
    )?;
    checked_add(
        &mut bytes,
        TIMESTAMP_WIDTH,
        AccountingComponentV0::Timestamps,
    )?;
    checked_add(
        &mut bytes,
        SENSITIVITY_WIDTH,
        AccountingComponentV0::Sensitivity,
    )?;
    checked_add(
        &mut bytes,
        LENGTH_PREFIX_WIDTH,
        AccountingComponentV0::Payload,
    )?;
    checked_add(
        &mut bytes,
        event.payload().logical_len(),
        AccountingComponentV0::Payload,
    )?;
    checked_add(
        &mut bytes,
        LENGTH_PREFIX_WIDTH,
        AccountingComponentV0::Provenance,
    )?;
    checked_add(
        &mut bytes,
        event.provenance().as_str().len(),
        AccountingComponentV0::Provenance,
    )?;
    Ok(bytes)
}

pub(crate) fn event_rank_v0(event: &EventEnvelopeV0) -> (i64, SourcePositionV0) {
    (event.event_time_ns(), event.source_position())
}

fn checked_add(
    current: &mut usize,
    added: usize,
    component: AccountingComponentV0,
) -> Result<(), AdmissionErrorV0> {
    *current = current
        .checked_add(added)
        .ok_or(AdmissionErrorV0::ArithmeticOverflow { component })?;
    Ok(())
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RejectionReasonV0 {
    UnexpectedSource,
    EpochFinalized,
    ConflictingRedelivery,
    OutsideEpoch,
    Late,
    AcceptedIdentityCapacity,
    AcceptedEventByteCapacity,
    InvalidFinalization,
}

impl RejectionReasonV0 {
    pub const ALL: [Self; REJECTION_CLASS_COUNT_V0] = [
        Self::UnexpectedSource,
        Self::EpochFinalized,
        Self::ConflictingRedelivery,
        Self::OutsideEpoch,
        Self::Late,
        Self::AcceptedIdentityCapacity,
        Self::AcceptedEventByteCapacity,
        Self::InvalidFinalization,
    ];

    const fn index(self) -> usize {
        match self {
            Self::UnexpectedSource => 0,
            Self::EpochFinalized => 1,
            Self::ConflictingRedelivery => 2,
            Self::OutsideEpoch => 3,
            Self::Late => 4,
            Self::AcceptedIdentityCapacity => 5,
            Self::AcceptedEventByteCapacity => 6,
            Self::InvalidFinalization => 7,
        }
    }
}

#[derive(Clone, Copy, Eq, PartialEq)]
pub struct RejectionRangeV0 {
    count: u64,
    count_saturated: bool,
    minimum_attempted_time_ns: Option<i64>,
    maximum_attempted_time_ns: Option<i64>,
}

impl RejectionRangeV0 {
    const EMPTY: Self = Self {
        count: 0,
        count_saturated: false,
        minimum_attempted_time_ns: None,
        maximum_attempted_time_ns: None,
    };

    pub const fn count(self) -> u64 {
        self.count
    }

    pub const fn count_saturated(self) -> bool {
        self.count_saturated
    }

    pub const fn minimum_attempted_time_ns(self) -> Option<i64> {
        self.minimum_attempted_time_ns
    }

    pub const fn maximum_attempted_time_ns(self) -> Option<i64> {
        self.maximum_attempted_time_ns
    }

    fn record(&mut self, attempted_time_ns: i64) {
        if self.count == u64::MAX {
            self.count_saturated = true;
        } else {
            self.count += 1;
        }
        self.minimum_attempted_time_ns = Some(
            self.minimum_attempted_time_ns
                .map_or(attempted_time_ns, |current| current.min(attempted_time_ns)),
        );
        self.maximum_attempted_time_ns = Some(
            self.maximum_attempted_time_ns
                .map_or(attempted_time_ns, |current| current.max(attempted_time_ns)),
        );
    }
}

impl fmt::Debug for RejectionRangeV0 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RejectionRangeV0")
            .field("count", &self.count)
            .field("count_saturated", &self.count_saturated)
            .field(
                "has_attempted_time_range",
                &self.minimum_attempted_time_ns.is_some(),
            )
            .finish()
    }
}

#[derive(Clone, Eq, PartialEq)]
pub struct RejectionSummaryV0 {
    ranges: [RejectionRangeV0; REJECTION_CLASS_COUNT_V0],
}

impl RejectionSummaryV0 {
    fn new() -> Self {
        Self {
            ranges: [RejectionRangeV0::EMPTY; REJECTION_CLASS_COUNT_V0],
        }
    }

    pub const fn class_count(&self) -> usize {
        REJECTION_CLASS_COUNT_V0
    }

    pub const fn range(&self, reason: RejectionReasonV0) -> RejectionRangeV0 {
        self.ranges[reason.index()]
    }

    fn record(&mut self, reason: RejectionReasonV0, attempted_time_ns: i64) {
        self.ranges[reason.index()].record(attempted_time_ns);
    }
}

impl fmt::Debug for RejectionSummaryV0 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let safe_ranges = RejectionReasonV0::ALL.map(|reason| (reason, self.range(reason)));
        formatter
            .debug_struct("RejectionSummaryV0")
            .field("ranges", &safe_ranges)
            .finish()
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ApplyOutcomeV0 {
    Accepted,
    DuplicateNoOp,
    Rejected(RejectionReasonV0),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum FinalizeOutcomeV0 {
    Finalized,
    AlreadyFinalizedNoOp,
    Rejected(RejectionReasonV0),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct AdmissionUsageV0 {
    limits: AdmissionLimitsV0,
    accepted_identities: usize,
    accepted_event_bytes: usize,
    rejection_classes: usize,
}

impl AdmissionUsageV0 {
    pub const fn limits(self) -> AdmissionLimitsV0 {
        self.limits
    }

    pub const fn accepted_identities(self) -> usize {
        self.accepted_identities
    }

    pub const fn accepted_event_bytes(self) -> usize {
        self.accepted_event_bytes
    }

    pub const fn rejection_classes(self) -> usize {
        self.rejection_classes
    }
}

#[derive(Clone, Eq, PartialEq)]
pub struct AdmissionStateV0 {
    config: AdmissionConfigV0,
    accepted_events: BTreeMap<SourcePositionV0, EventEnvelopeV0>,
    accepted_event_bytes: usize,
    maximum_accepted_event_time_ns: Option<i64>,
    watermark_ns: Option<i64>,
    finalized: bool,
    rejection_summary: RejectionSummaryV0,
}

#[derive(Clone, Eq, PartialEq)]
pub enum AdmissionPhysicalRetentionErrorV0 {
    NonKafkaSource { actual: SourcePositionKindV0 },
    CutoffOutsideEpoch,
    MissingWatermark,
    CutoffAfterWatermark,
    Accounting(AdmissionErrorV0),
}

impl fmt::Debug for AdmissionPhysicalRetentionErrorV0 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NonKafkaSource { actual } => formatter
                .debug_struct("NonKafkaSource")
                .field("actual", actual)
                .finish(),
            Self::CutoffOutsideEpoch => formatter.write_str("CutoffOutsideEpoch"),
            Self::MissingWatermark => formatter.write_str("MissingWatermark"),
            Self::CutoffAfterWatermark => formatter.write_str("CutoffAfterWatermark"),
            Self::Accounting(error) => formatter.debug_tuple("Accounting").field(error).finish(),
        }
    }
}

impl fmt::Display for AdmissionPhysicalRetentionErrorV0 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(self, formatter)
    }
}

impl Error for AdmissionPhysicalRetentionErrorV0 {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Accounting(error) => Some(error),
            Self::NonKafkaSource { .. }
            | Self::CutoffOutsideEpoch
            | Self::MissingWatermark
            | Self::CutoffAfterWatermark => None,
        }
    }
}

pub struct AdmissionPhysicalRetentionV0 {
    retained: AdmissionStateV0,
    cutoff_event_time_ns: i64,
    removed_identities: usize,
    removed_event_bytes: usize,
}

impl AdmissionPhysicalRetentionV0 {
    pub const fn cutoff_event_time_ns(&self) -> i64 {
        self.cutoff_event_time_ns
    }

    pub const fn removed_identities(&self) -> usize {
        self.removed_identities
    }

    pub const fn removed_event_bytes(&self) -> usize {
        self.removed_event_bytes
    }

    pub fn retained_state(&self) -> &AdmissionStateV0 {
        &self.retained
    }

    pub fn into_retained_state(self) -> AdmissionStateV0 {
        self.retained
    }
}

impl fmt::Debug for AdmissionPhysicalRetentionV0 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("AdmissionPhysicalRetentionV0")
            .field("cutoff_event_time_ns", &"<redacted>")
            .field("removed_identities", &self.removed_identities)
            .field("removed_event_bytes", &self.removed_event_bytes)
            .field("retained_usage", &self.retained.usage())
            .finish()
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AdmissionEpochRolloverErrorV0 {
    NonKafkaSource,
    CurrentEpochNotFinalized,
    AdmissionVersionChanged,
    SourceIdentityChanged,
    SourcePositionKindChanged,
    AdmissionLimitsChanged,
    EpochsNotAdjacent,
}

impl fmt::Display for AdmissionEpochRolloverErrorV0 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(self, formatter)
    }
}

impl Error for AdmissionEpochRolloverErrorV0 {}

impl AdmissionStateV0 {
    pub fn new(config: AdmissionConfigV0) -> Self {
        Self {
            config,
            accepted_events: BTreeMap::new(),
            accepted_event_bytes: 0,
            maximum_accepted_event_time_ns: None,
            watermark_ns: None,
            finalized: false,
            rejection_summary: RejectionSummaryV0::new(),
        }
    }

    pub fn config(&self) -> &AdmissionConfigV0 {
        &self.config
    }

    pub fn usage(&self) -> AdmissionUsageV0 {
        AdmissionUsageV0 {
            limits: self.config.limits,
            accepted_identities: self.accepted_events.len(),
            accepted_event_bytes: self.accepted_event_bytes,
            rejection_classes: REJECTION_CLASS_COUNT_V0,
        }
    }

    pub const fn maximum_accepted_event_time_ns(&self) -> Option<i64> {
        self.maximum_accepted_event_time_ns
    }

    pub const fn watermark_ns(&self) -> Option<i64> {
        self.watermark_ns
    }

    pub const fn is_finalized(&self) -> bool {
        self.finalized
    }

    pub fn rejection_summary(&self) -> &RejectionSummaryV0 {
        &self.rejection_summary
    }

    pub fn accepted_events(&self) -> impl Iterator<Item = &EventEnvelopeV0> {
        self.accepted_events.values()
    }

    pub fn physically_retain_from_event_time_v0(
        &self,
        cutoff_event_time_ns: i64,
    ) -> Result<AdmissionPhysicalRetentionV0, AdmissionPhysicalRetentionErrorV0> {
        if self.config.source_position_kind != SourcePositionKindV0::Kafka {
            return Err(AdmissionPhysicalRetentionErrorV0::NonKafkaSource {
                actual: self.config.source_position_kind,
            });
        }
        if !(self.config.epoch_start_ns..=self.config.epoch_end_ns).contains(&cutoff_event_time_ns)
        {
            return Err(AdmissionPhysicalRetentionErrorV0::CutoffOutsideEpoch);
        }
        let watermark = self
            .watermark_ns
            .ok_or(AdmissionPhysicalRetentionErrorV0::MissingWatermark)?;
        if cutoff_event_time_ns > watermark {
            return Err(AdmissionPhysicalRetentionErrorV0::CutoffAfterWatermark);
        }

        let mut retained_event_bytes = 0_usize;
        let mut retained_maximum = None;
        let mut retained_identities = 0_usize;
        for event in self
            .accepted_events
            .values()
            .filter(|event| event.event_time_ns() >= cutoff_event_time_ns)
        {
            let event_bytes = accepted_event_accounted_bytes_v0(event)
                .map_err(AdmissionPhysicalRetentionErrorV0::Accounting)?;
            retained_event_bytes = retained_event_bytes.checked_add(event_bytes).ok_or(
                AdmissionPhysicalRetentionErrorV0::Accounting(
                    AdmissionErrorV0::ArithmeticOverflow {
                        component: AccountingComponentV0::AcceptedLedger,
                    },
                ),
            )?;
            retained_identities = retained_identities.checked_add(1).ok_or(
                AdmissionPhysicalRetentionErrorV0::Accounting(
                    AdmissionErrorV0::ArithmeticOverflow {
                        component: AccountingComponentV0::AcceptedLedger,
                    },
                ),
            )?;
            retained_maximum = Some(
                retained_maximum.map_or(event.event_time_ns(), |current: i64| {
                    current.max(event.event_time_ns())
                }),
            );
        }
        let removed_identities = self
            .accepted_events
            .len()
            .checked_sub(retained_identities)
            .ok_or(AdmissionPhysicalRetentionErrorV0::Accounting(
                AdmissionErrorV0::ArithmeticOverflow {
                    component: AccountingComponentV0::AcceptedLedger,
                },
            ))?;
        let removed_event_bytes = self
            .accepted_event_bytes
            .checked_sub(retained_event_bytes)
            .ok_or(AdmissionPhysicalRetentionErrorV0::Accounting(
                AdmissionErrorV0::ArithmeticOverflow {
                    component: AccountingComponentV0::AcceptedLedger,
                },
            ))?;

        let mut retained = self.clone();
        retained
            .accepted_events
            .retain(|_, event| event.event_time_ns() >= cutoff_event_time_ns);
        retained.accepted_event_bytes = retained_event_bytes;
        retained.maximum_accepted_event_time_ns = retained_maximum;
        retained.watermark_ns = if retained.finalized {
            Some(retained.config.epoch_end_ns)
        } else {
            retained_maximum.map(|maximum| maximum.saturating_sub(ALLOWED_LATENESS_NS_V0))
        };
        Ok(AdmissionPhysicalRetentionV0 {
            retained,
            cutoff_event_time_ns,
            removed_identities,
            removed_event_bytes,
        })
    }

    pub fn begin_adjacent_kafka_epoch_v0(
        &self,
        next_config: AdmissionConfigV0,
    ) -> Result<Self, AdmissionEpochRolloverErrorV0> {
        if self.config.source_position_kind != SourcePositionKindV0::Kafka {
            return Err(AdmissionEpochRolloverErrorV0::NonKafkaSource);
        }
        if !self.finalized {
            return Err(AdmissionEpochRolloverErrorV0::CurrentEpochNotFinalized);
        }
        if next_config.version != self.config.version {
            return Err(AdmissionEpochRolloverErrorV0::AdmissionVersionChanged);
        }
        if next_config.source_id != self.config.source_id {
            return Err(AdmissionEpochRolloverErrorV0::SourceIdentityChanged);
        }
        if next_config.source_position_kind != self.config.source_position_kind {
            return Err(AdmissionEpochRolloverErrorV0::SourcePositionKindChanged);
        }
        if next_config.limits != self.config.limits {
            return Err(AdmissionEpochRolloverErrorV0::AdmissionLimitsChanged);
        }
        if next_config.epoch_start_ns != self.config.epoch_end_ns {
            return Err(AdmissionEpochRolloverErrorV0::EpochsNotAdjacent);
        }
        Ok(Self::new(next_config))
    }

    pub fn apply_event(
        &mut self,
        event: &EventEnvelopeV0,
    ) -> Result<ApplyOutcomeV0, AdmissionErrorV0> {
        let attempted_time_ns = event.event_time_ns();
        if event.source_id() != &self.config.source_id {
            return Ok(self.reject_event(RejectionReasonV0::UnexpectedSource, attempted_time_ns));
        }
        let expected_kind = self.config.source_position_kind;
        let actual_kind = event.source_position().kind();
        if expected_kind != SourcePositionKindV0::Synthetic && actual_kind != expected_kind {
            return Err(AdmissionErrorV0::UnexpectedSourcePositionKind {
                expected: expected_kind,
                actual: actual_kind,
            });
        }
        if actual_kind != SourcePositionKindV0::Synthetic {
            return Err(AdmissionErrorV0::UnsupportedSourcePosition { kind: actual_kind });
        }
        if expected_kind != SourcePositionKindV0::Synthetic {
            return Err(AdmissionErrorV0::UnexpectedSourcePositionKind {
                expected: expected_kind,
                actual: actual_kind,
            });
        }
        self.apply_event_semantics(event)
    }

    pub fn apply_event_v1(
        &mut self,
        event: &EventEnvelopeV0,
    ) -> Result<ApplyOutcomeV0, AdmissionErrorV0> {
        let attempted_time_ns = event.event_time_ns();
        if event.source_id() != &self.config.source_id {
            return Ok(self.reject_event(RejectionReasonV0::UnexpectedSource, attempted_time_ns));
        }
        let expected_kind = self.config.source_position_kind;
        let actual_kind = event.source_position().kind();
        if actual_kind != expected_kind {
            return Err(AdmissionErrorV0::UnexpectedSourcePositionKind {
                expected: expected_kind,
                actual: actual_kind,
            });
        }
        self.apply_event_semantics(event)
    }

    fn apply_event_semantics(
        &mut self,
        event: &EventEnvelopeV0,
    ) -> Result<ApplyOutcomeV0, AdmissionErrorV0> {
        let attempted_time_ns = event.event_time_ns();
        if self.finalized {
            return Ok(self.reject_event(RejectionReasonV0::EpochFinalized, attempted_time_ns));
        }

        let source_position = event.source_position();
        if let Some(classification) = self
            .accepted_events
            .get(&source_position)
            .map(|retained| retained.classify_redelivery(event))
        {
            return match classification {
                RedeliveryClassificationV0::ExactRedelivery => Ok(ApplyOutcomeV0::DuplicateNoOp),
                RedeliveryClassificationV0::ConflictingRedelivery => {
                    Ok(self
                        .reject_event(RejectionReasonV0::ConflictingRedelivery, attempted_time_ns))
                }
                RedeliveryClassificationV0::Distinct => {
                    Err(AdmissionErrorV0::IdentityInvariantViolation)
                }
            };
        }

        if !(self.config.epoch_start_ns..self.config.epoch_end_ns).contains(&attempted_time_ns) {
            return Ok(self.reject_event(RejectionReasonV0::OutsideEpoch, attempted_time_ns));
        }
        if self
            .watermark_ns
            .is_some_and(|watermark| attempted_time_ns < watermark)
        {
            return Ok(self.reject_event(RejectionReasonV0::Late, attempted_time_ns));
        }

        let accounted_bytes = accepted_event_accounted_bytes_v0(event)?;
        let normalized_event = normalize_event(event)?;
        let prospective_entries = self.accepted_events.len().checked_add(1).ok_or(
            AdmissionErrorV0::ArithmeticOverflow {
                component: AccountingComponentV0::AcceptedLedger,
            },
        )?;
        let prospective_bytes = self
            .accepted_event_bytes
            .checked_add(accounted_bytes)
            .ok_or(AdmissionErrorV0::ArithmeticOverflow {
                component: AccountingComponentV0::AcceptedLedger,
            })?;
        if prospective_entries > self.config.limits.accepted_identity_limit {
            return Ok(self.reject_event(
                RejectionReasonV0::AcceptedIdentityCapacity,
                attempted_time_ns,
            ));
        }
        if prospective_bytes > self.config.limits.accepted_event_byte_limit {
            return Ok(self.reject_event(
                RejectionReasonV0::AcceptedEventByteCapacity,
                attempted_time_ns,
            ));
        }

        let next_maximum = self
            .maximum_accepted_event_time_ns
            .map_or(attempted_time_ns, |current| current.max(attempted_time_ns));
        let next_watermark = next_maximum.saturating_sub(ALLOWED_LATENESS_NS_V0);
        match self.accepted_events.entry(source_position) {
            std::collections::btree_map::Entry::Vacant(entry) => {
                entry.insert(normalized_event);
            }
            std::collections::btree_map::Entry::Occupied(_) => {
                return Err(AdmissionErrorV0::IdentityInvariantViolation);
            }
        }
        self.accepted_event_bytes = prospective_bytes;
        self.maximum_accepted_event_time_ns = Some(next_maximum);
        self.watermark_ns = Some(next_watermark);
        Ok(ApplyOutcomeV0::Accepted)
    }

    pub fn finalize_source(
        &mut self,
        source_id: &SourceIdV0,
        finalized_through_ns: i64,
    ) -> FinalizeOutcomeV0 {
        if source_id != &self.config.source_id {
            return self
                .reject_finalization(RejectionReasonV0::UnexpectedSource, finalized_through_ns);
        }
        if self.finalized {
            return if finalized_through_ns == self.config.epoch_end_ns {
                FinalizeOutcomeV0::AlreadyFinalizedNoOp
            } else {
                self.reject_finalization(
                    RejectionReasonV0::InvalidFinalization,
                    finalized_through_ns,
                )
            };
        }
        if finalized_through_ns != self.config.epoch_end_ns {
            return self
                .reject_finalization(RejectionReasonV0::InvalidFinalization, finalized_through_ns);
        }
        self.finalized = true;
        self.watermark_ns = Some(self.config.epoch_end_ns);
        FinalizeOutcomeV0::Finalized
    }

    fn reject_event(
        &mut self,
        reason: RejectionReasonV0,
        attempted_time_ns: i64,
    ) -> ApplyOutcomeV0 {
        self.rejection_summary.record(reason, attempted_time_ns);
        ApplyOutcomeV0::Rejected(reason)
    }

    fn reject_finalization(
        &mut self,
        reason: RejectionReasonV0,
        attempted_time_ns: i64,
    ) -> FinalizeOutcomeV0 {
        self.rejection_summary.record(reason, attempted_time_ns);
        FinalizeOutcomeV0::Rejected(reason)
    }
}

impl fmt::Debug for AdmissionStateV0 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("AdmissionStateV0")
            .field("config", &self.config)
            .field("usage", &self.usage())
            .field(
                "has_maximum_accepted_event_time",
                &self.maximum_accepted_event_time_ns.is_some(),
            )
            .field("has_watermark", &self.watermark_ns.is_some())
            .field("finalized", &self.finalized)
            .field("rejection_summary", &self.rejection_summary)
            .finish()
    }
}

fn normalize_event(event: &EventEnvelopeV0) -> Result<EventEnvelopeV0, AdmissionErrorV0> {
    Ok(EventEnvelopeV0::new(
        event.envelope_version(),
        event.source_id().clone(),
        event.schema_id().clone(),
        event.source_position(),
        event.key(),
        event.event_time_ns(),
        0,
        event.sensitivity(),
        event.payload().clone(),
    )?)
}

#[cfg(test)]
mod tests {
    use super::*;
    use decant_ir::{CanonicalPayloadV0, SchemaIdV0, SensitivityBitmapV0};

    fn event_at(position: SourcePositionV0) -> EventEnvelopeV0 {
        EventEnvelopeV0::new(
            0,
            SourceIdV0::new("source-v0").unwrap(),
            SchemaIdV0::new("schema-v0").unwrap(),
            position,
            None,
            50,
            0,
            SensitivityBitmapV0::from_bits(0),
            CanonicalPayloadV0::try_from_fields(BTreeMap::new()).unwrap(),
        )
        .unwrap()
    }

    #[test]
    fn accounting_overflow_is_typed_and_non_mutating() {
        let mut current = usize::MAX;
        assert_eq!(
            checked_add(&mut current, 1, AccountingComponentV0::Payload),
            Err(AdmissionErrorV0::ArithmeticOverflow {
                component: AccountingComponentV0::Payload,
            })
        );
        assert_eq!(current, usize::MAX);
    }

    #[test]
    fn rejection_count_saturation_is_explicit() {
        let mut range = RejectionRangeV0 {
            count: u64::MAX,
            count_saturated: false,
            minimum_attempted_time_ns: Some(0),
            maximum_attempted_time_ns: Some(0),
        };
        range.record(1);
        assert_eq!(range.count(), u64::MAX);
        assert!(range.count_saturated());
        assert_eq!(range.minimum_attempted_time_ns(), Some(0));
        assert_eq!(range.maximum_attempted_time_ns(), Some(1));
    }

    #[test]
    fn shared_event_rank_keeps_tagged_positions_distinct() {
        let synthetic = event_at(SourcePositionV0::Synthetic { sequence: 0 });
        let external = event_at(SourcePositionV0::otlp_file(1, 0, 0, 0).unwrap());
        assert_eq!(
            synthetic.source_position().source_ordering_ordinal(),
            external.source_position().source_ordering_ordinal()
        );
        assert_ne!(event_rank_v0(&synthetic), event_rank_v0(&external));
        assert!(event_rank_v0(&synthetic) < event_rank_v0(&external));
    }
}
