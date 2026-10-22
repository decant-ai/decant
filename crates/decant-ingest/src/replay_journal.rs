//! Bounded raw-call journal for current Kafka JSON ingestion inputs.

use std::error::Error;
use std::fmt;

use decant_ir::SourceIdV0;
use decant_kafka::{KafkaClusterIdV0, KafkaFetchedRecordV0, KafkaTopicIdV0};

pub const KAFKA_JSON_INGESTION_REPLAY_JOURNAL_VERSION_V0: u16 = 0;
pub const MAX_KAFKA_JSON_INGESTION_REPLAY_INPUTS_V0: usize = 100_001;
pub const MAX_KAFKA_JSON_INGESTION_REPLAY_BYTES_V0: usize = 128 * 1_024 * 1_024;

const LENGTH_PREFIX_BYTES_V0: usize = 4;
const TOPIC_ID_BYTES_V0: usize = 16;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum KafkaJsonIngestionReplayLimitFieldV0 {
    Inputs,
    Bytes,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum KafkaJsonIngestionReplayAccountingFieldV0 {
    Inputs,
    Bytes,
    ObservedIdentity,
}

#[derive(Clone, Eq, PartialEq)]
pub enum KafkaJsonIngestionReplayConfigErrorV0 {
    UnsupportedVersion {
        actual: u16,
    },
    ZeroLimit {
        field: KafkaJsonIngestionReplayLimitFieldV0,
    },
    AboveMaximum {
        field: KafkaJsonIngestionReplayLimitFieldV0,
        maximum: usize,
        actual: usize,
    },
}

impl fmt::Debug for KafkaJsonIngestionReplayConfigErrorV0 {
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

impl fmt::Display for KafkaJsonIngestionReplayConfigErrorV0 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(self, formatter)
    }
}

impl Error for KafkaJsonIngestionReplayConfigErrorV0 {}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct KafkaJsonIngestionReplayLimitsV0 {
    input_limit: usize,
    byte_limit: usize,
}

impl KafkaJsonIngestionReplayLimitsV0 {
    pub fn new(
        input_limit: usize,
        byte_limit: usize,
    ) -> Result<Self, KafkaJsonIngestionReplayConfigErrorV0> {
        validate_limit(
            input_limit,
            MAX_KAFKA_JSON_INGESTION_REPLAY_INPUTS_V0,
            KafkaJsonIngestionReplayLimitFieldV0::Inputs,
        )?;
        validate_limit(
            byte_limit,
            MAX_KAFKA_JSON_INGESTION_REPLAY_BYTES_V0,
            KafkaJsonIngestionReplayLimitFieldV0::Bytes,
        )?;
        Ok(Self {
            input_limit,
            byte_limit,
        })
    }

    pub const fn maximum() -> Self {
        Self {
            input_limit: MAX_KAFKA_JSON_INGESTION_REPLAY_INPUTS_V0,
            byte_limit: MAX_KAFKA_JSON_INGESTION_REPLAY_BYTES_V0,
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
    field: KafkaJsonIngestionReplayLimitFieldV0,
) -> Result<(), KafkaJsonIngestionReplayConfigErrorV0> {
    if actual == 0 {
        return Err(KafkaJsonIngestionReplayConfigErrorV0::ZeroLimit { field });
    }
    if actual > maximum {
        return Err(KafkaJsonIngestionReplayConfigErrorV0::AboveMaximum {
            field,
            maximum,
            actual,
        });
    }
    Ok(())
}

#[derive(Clone, Eq, PartialEq)]
pub struct KafkaJsonIngestionReplayInputV0 {
    observed_source_id: SourceIdV0,
    observed_cluster_id: KafkaClusterIdV0,
    observed_topic_id: KafkaTopicIdV0,
    record: KafkaFetchedRecordV0,
}

impl KafkaJsonIngestionReplayInputV0 {
    pub fn new(
        observed_source_id: SourceIdV0,
        observed_cluster_id: KafkaClusterIdV0,
        observed_topic_id: KafkaTopicIdV0,
        record: KafkaFetchedRecordV0,
    ) -> Self {
        Self {
            observed_source_id,
            observed_cluster_id,
            observed_topic_id,
            record,
        }
    }

    pub fn observed_source_id(&self) -> &SourceIdV0 {
        &self.observed_source_id
    }

    pub fn observed_cluster_id(&self) -> &KafkaClusterIdV0 {
        &self.observed_cluster_id
    }

    pub const fn observed_topic_id(&self) -> KafkaTopicIdV0 {
        self.observed_topic_id
    }

    pub const fn record(&self) -> &KafkaFetchedRecordV0 {
        &self.record
    }
}

impl fmt::Debug for KafkaJsonIngestionReplayInputV0 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("KafkaJsonIngestionReplayInputV0")
            .field("record_logical_bytes", &self.record.logical_bytes())
            .finish_non_exhaustive()
    }
}

pub fn kafka_json_ingestion_replay_input_accounted_bytes_v0(
    input: &KafkaJsonIngestionReplayInputV0,
) -> Result<usize, KafkaJsonIngestionReplayAppendErrorV0> {
    LENGTH_PREFIX_BYTES_V0
        .checked_add(input.observed_source_id.as_str().len())
        .and_then(|bytes| bytes.checked_add(LENGTH_PREFIX_BYTES_V0))
        .and_then(|bytes| bytes.checked_add(input.observed_cluster_id.as_str().len()))
        .and_then(|bytes| bytes.checked_add(TOPIC_ID_BYTES_V0))
        .and_then(|bytes| bytes.checked_add(input.record.logical_bytes()))
        .ok_or(KafkaJsonIngestionReplayAppendErrorV0::ArithmeticOverflow {
            field: KafkaJsonIngestionReplayAccountingFieldV0::ObservedIdentity,
        })
}

#[derive(Clone, Eq, PartialEq)]
pub enum KafkaJsonIngestionReplayAppendErrorV0 {
    ArithmeticOverflow {
        field: KafkaJsonIngestionReplayAccountingFieldV0,
    },
    CapacityExceeded {
        field: KafkaJsonIngestionReplayLimitFieldV0,
        maximum: usize,
        attempted: usize,
    },
    AllocationFailed,
}

impl fmt::Debug for KafkaJsonIngestionReplayAppendErrorV0 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
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

impl fmt::Display for KafkaJsonIngestionReplayAppendErrorV0 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(self, formatter)
    }
}

impl Error for KafkaJsonIngestionReplayAppendErrorV0 {}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct KafkaJsonIngestionReplayUsageV0 {
    limits: KafkaJsonIngestionReplayLimitsV0,
    inputs: usize,
    bytes: usize,
}

impl KafkaJsonIngestionReplayUsageV0 {
    pub const fn limits(self) -> KafkaJsonIngestionReplayLimitsV0 {
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
pub struct KafkaJsonIngestionReplayJournalV0 {
    version: u16,
    limits: KafkaJsonIngestionReplayLimitsV0,
    inputs: Vec<KafkaJsonIngestionReplayInputV0>,
    accounted_bytes: usize,
}

impl KafkaJsonIngestionReplayJournalV0 {
    pub fn new(
        version: u16,
        limits: KafkaJsonIngestionReplayLimitsV0,
    ) -> Result<Self, KafkaJsonIngestionReplayConfigErrorV0> {
        if version != KAFKA_JSON_INGESTION_REPLAY_JOURNAL_VERSION_V0 {
            return Err(KafkaJsonIngestionReplayConfigErrorV0::UnsupportedVersion {
                actual: version,
            });
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

    pub const fn limits(&self) -> KafkaJsonIngestionReplayLimitsV0 {
        self.limits
    }

    pub fn len(&self) -> usize {
        self.inputs.len()
    }

    pub fn is_empty(&self) -> bool {
        self.inputs.is_empty()
    }

    pub fn inputs(&self) -> &[KafkaJsonIngestionReplayInputV0] {
        &self.inputs
    }

    pub fn usage(&self) -> KafkaJsonIngestionReplayUsageV0 {
        KafkaJsonIngestionReplayUsageV0 {
            limits: self.limits,
            inputs: self.inputs.len(),
            bytes: self.accounted_bytes,
        }
    }

    #[allow(
        clippy::result_large_err,
        reason = "the refused owned input must be returned without another allocation"
    )]
    pub fn try_push(
        &mut self,
        input: KafkaJsonIngestionReplayInputV0,
    ) -> Result<
        (),
        (
            KafkaJsonIngestionReplayAppendErrorV0,
            KafkaJsonIngestionReplayInputV0,
        ),
    > {
        let input_bytes = match kafka_json_ingestion_replay_input_accounted_bytes_v0(&input) {
            Ok(bytes) => bytes,
            Err(error) => return Err((error, input)),
        };
        let prospective_inputs = match self.inputs.len().checked_add(1) {
            Some(inputs) => inputs,
            None => {
                return Err((
                    KafkaJsonIngestionReplayAppendErrorV0::ArithmeticOverflow {
                        field: KafkaJsonIngestionReplayAccountingFieldV0::Inputs,
                    },
                    input,
                ));
            }
        };
        let prospective_bytes = match self.accounted_bytes.checked_add(input_bytes) {
            Some(bytes) => bytes,
            None => {
                return Err((
                    KafkaJsonIngestionReplayAppendErrorV0::ArithmeticOverflow {
                        field: KafkaJsonIngestionReplayAccountingFieldV0::Bytes,
                    },
                    input,
                ));
            }
        };
        if prospective_inputs > self.limits.input_limit {
            return Err((
                KafkaJsonIngestionReplayAppendErrorV0::CapacityExceeded {
                    field: KafkaJsonIngestionReplayLimitFieldV0::Inputs,
                    maximum: self.limits.input_limit,
                    attempted: prospective_inputs,
                },
                input,
            ));
        }
        if prospective_bytes > self.limits.byte_limit {
            return Err((
                KafkaJsonIngestionReplayAppendErrorV0::CapacityExceeded {
                    field: KafkaJsonIngestionReplayLimitFieldV0::Bytes,
                    maximum: self.limits.byte_limit,
                    attempted: prospective_bytes,
                },
                input,
            ));
        }
        if self.inputs.try_reserve_exact(1).is_err() {
            return Err((
                KafkaJsonIngestionReplayAppendErrorV0::AllocationFailed,
                input,
            ));
        }
        self.inputs.push(input);
        self.accounted_bytes = prospective_bytes;
        Ok(())
    }
}

impl fmt::Debug for KafkaJsonIngestionReplayJournalV0 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("KafkaJsonIngestionReplayJournalV0")
            .field("version", &self.version)
            .field("usage", &self.usage())
            .finish_non_exhaustive()
    }
}
