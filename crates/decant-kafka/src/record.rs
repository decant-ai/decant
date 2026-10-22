//! Bounded in-memory representation of one fetched Kafka data record.

use std::error::Error;
use std::fmt;

use decant_ir::KafkaPositionV0;

pub const KAFKA_FETCHED_RECORD_VERSION_V0: u16 = 0;
pub const MAX_KAFKA_RECORD_KEY_BYTES_V0: usize = 256;
pub const MAX_KAFKA_RECORD_VALUE_BYTES_V0: usize = 64 * 1024;
pub const MAX_KAFKA_RECORD_HEADERS_V0: usize = 64;
pub const MAX_KAFKA_RECORD_HEADER_KEY_BYTES_V0: usize = 128;
pub const MAX_KAFKA_RECORD_HEADER_VALUE_BYTES_V0: usize = 4 * 1024;
pub const MAX_KAFKA_RECORD_HEADER_BYTES_V0: usize = 64 * 1024;
pub const MAX_KAFKA_FETCHED_RECORD_BYTES_V0: usize = 128 * 1024;

const POSITION_BYTES: usize = 12;
const OPTIONAL_TAG_BYTES: usize = 1;
const LENGTH_PREFIX_BYTES: usize = 4;
const TIMESTAMP_KIND_BYTES: usize = 1;
const TIMESTAMP_VALUE_BYTES: usize = 8;
const HEADER_COUNT_BYTES: usize = 4;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum KafkaRecordTimestampKindV0 {
    NotAvailable,
    CreateTime,
    LogAppendTime,
}

#[derive(Clone, Copy, Eq, PartialEq)]
pub enum KafkaRecordTimestampV0 {
    NotAvailable,
    CreateTime { unix_ms: i64 },
    LogAppendTime { unix_ms: i64 },
}

impl KafkaRecordTimestampV0 {
    pub const fn kind(self) -> KafkaRecordTimestampKindV0 {
        match self {
            Self::NotAvailable => KafkaRecordTimestampKindV0::NotAvailable,
            Self::CreateTime { .. } => KafkaRecordTimestampKindV0::CreateTime,
            Self::LogAppendTime { .. } => KafkaRecordTimestampKindV0::LogAppendTime,
        }
    }

    pub const fn unix_ms(self) -> Option<i64> {
        match self {
            Self::NotAvailable => None,
            Self::CreateTime { unix_ms } | Self::LogAppendTime { unix_ms } => Some(unix_ms),
        }
    }
}

impl fmt::Debug for KafkaRecordTimestampV0 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("KafkaRecordTimestampV0")
            .field("kind", &self.kind())
            .finish()
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum KafkaRecordBoundV0 {
    KeyBytes,
    ValueBytes,
    Headers,
    HeaderKeyBytes,
    HeaderValueBytes,
    HeaderBytes,
    RecordBytes,
}

#[derive(Clone, Eq, PartialEq)]
pub enum KafkaRecordValidationErrorV0 {
    EmptyHeaderKey,
    BoundExceeded {
        bound: KafkaRecordBoundV0,
        maximum: usize,
        actual: usize,
    },
    AccountingOverflow {
        bound: KafkaRecordBoundV0,
    },
    AllocationFailed {
        bound: KafkaRecordBoundV0,
    },
}

impl fmt::Debug for KafkaRecordValidationErrorV0 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::EmptyHeaderKey => formatter.write_str("EmptyHeaderKey"),
            Self::BoundExceeded {
                bound,
                maximum,
                actual,
            } => formatter
                .debug_struct("BoundExceeded")
                .field("bound", bound)
                .field("maximum", maximum)
                .field("actual", actual)
                .finish(),
            Self::AccountingOverflow { bound } => formatter
                .debug_struct("AccountingOverflow")
                .field("bound", bound)
                .finish(),
            Self::AllocationFailed { bound } => formatter
                .debug_struct("AllocationFailed")
                .field("bound", bound)
                .finish(),
        }
    }
}

impl fmt::Display for KafkaRecordValidationErrorV0 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(self, formatter)
    }
}

impl Error for KafkaRecordValidationErrorV0 {}

#[derive(Clone, Eq, PartialEq)]
pub struct KafkaRecordHeaderV0 {
    key: String,
    value: Option<Vec<u8>>,
    logical_bytes: usize,
}

impl KafkaRecordHeaderV0 {
    pub fn new(key: &str, value: Option<&[u8]>) -> Result<Self, KafkaRecordValidationErrorV0> {
        if key.is_empty() {
            return Err(KafkaRecordValidationErrorV0::EmptyHeaderKey);
        }
        check_bound(
            key.len(),
            MAX_KAFKA_RECORD_HEADER_KEY_BYTES_V0,
            KafkaRecordBoundV0::HeaderKeyBytes,
        )?;
        if let Some(value) = value {
            check_bound(
                value.len(),
                MAX_KAFKA_RECORD_HEADER_VALUE_BYTES_V0,
                KafkaRecordBoundV0::HeaderValueBytes,
            )?;
        }

        let logical_bytes =
            header_logical_bytes(key.len(), value.map_or(0, <[u8]>::len), value.is_some())?;
        let key = copy_string(key, KafkaRecordBoundV0::HeaderKeyBytes)?;
        let value = value
            .map(|bytes| copy_bytes(bytes, KafkaRecordBoundV0::HeaderValueBytes))
            .transpose()?;
        Ok(Self {
            key,
            value,
            logical_bytes,
        })
    }

    pub fn key(&self) -> &str {
        &self.key
    }

    pub fn value(&self) -> Option<&[u8]> {
        self.value.as_deref()
    }

    pub const fn logical_bytes(&self) -> usize {
        self.logical_bytes
    }
}

impl fmt::Debug for KafkaRecordHeaderV0 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("KafkaRecordHeaderV0")
            .field("key_bytes", &self.key.len())
            .field("has_value", &self.value.is_some())
            .field("value_bytes", &self.value.as_ref().map_or(0, Vec::len))
            .finish()
    }
}

#[derive(Clone, Eq, PartialEq)]
pub struct KafkaFetchedRecordV0 {
    position: KafkaPositionV0,
    key: Option<Vec<u8>>,
    value: Option<Vec<u8>>,
    timestamp: KafkaRecordTimestampV0,
    headers: Vec<KafkaRecordHeaderV0>,
    header_bytes: usize,
    logical_bytes: usize,
}

impl KafkaFetchedRecordV0 {
    pub fn new(
        position: KafkaPositionV0,
        key: Option<&[u8]>,
        value: Option<&[u8]>,
        timestamp: KafkaRecordTimestampV0,
        headers: &[KafkaRecordHeaderV0],
    ) -> Result<Self, KafkaRecordValidationErrorV0> {
        if let Some(key) = key {
            check_bound(
                key.len(),
                MAX_KAFKA_RECORD_KEY_BYTES_V0,
                KafkaRecordBoundV0::KeyBytes,
            )?;
        }
        if let Some(value) = value {
            check_bound(
                value.len(),
                MAX_KAFKA_RECORD_VALUE_BYTES_V0,
                KafkaRecordBoundV0::ValueBytes,
            )?;
        }
        check_bound(
            headers.len(),
            MAX_KAFKA_RECORD_HEADERS_V0,
            KafkaRecordBoundV0::Headers,
        )?;

        let header_bytes = headers.iter().try_fold(0_usize, |total, header| {
            total.checked_add(header.logical_bytes()).ok_or(
                KafkaRecordValidationErrorV0::AccountingOverflow {
                    bound: KafkaRecordBoundV0::HeaderBytes,
                },
            )
        })?;
        check_bound(
            header_bytes,
            MAX_KAFKA_RECORD_HEADER_BYTES_V0,
            KafkaRecordBoundV0::HeaderBytes,
        )?;

        let logical_bytes = record_logical_bytes(
            key.map_or(0, <[u8]>::len),
            key.is_some(),
            value.map_or(0, <[u8]>::len),
            value.is_some(),
            timestamp,
            header_bytes,
        )?;
        check_bound(
            logical_bytes,
            MAX_KAFKA_FETCHED_RECORD_BYTES_V0,
            KafkaRecordBoundV0::RecordBytes,
        )?;

        let key = key
            .map(|bytes| copy_bytes(bytes, KafkaRecordBoundV0::KeyBytes))
            .transpose()?;
        let value = value
            .map(|bytes| copy_bytes(bytes, KafkaRecordBoundV0::ValueBytes))
            .transpose()?;
        let mut owned_headers = Vec::new();
        owned_headers
            .try_reserve_exact(headers.len())
            .map_err(|_| KafkaRecordValidationErrorV0::AllocationFailed {
                bound: KafkaRecordBoundV0::Headers,
            })?;
        owned_headers.extend_from_slice(headers);

        Ok(Self {
            position,
            key,
            value,
            timestamp,
            headers: owned_headers,
            header_bytes,
            logical_bytes,
        })
    }

    pub const fn position(&self) -> KafkaPositionV0 {
        self.position
    }

    pub fn key(&self) -> Option<&[u8]> {
        self.key.as_deref()
    }

    pub fn value(&self) -> Option<&[u8]> {
        self.value.as_deref()
    }

    pub const fn is_tombstone(&self) -> bool {
        self.value.is_none()
    }

    pub const fn timestamp(&self) -> KafkaRecordTimestampV0 {
        self.timestamp
    }

    pub fn headers(&self) -> &[KafkaRecordHeaderV0] {
        &self.headers
    }

    pub const fn header_bytes(&self) -> usize {
        self.header_bytes
    }

    pub const fn logical_bytes(&self) -> usize {
        self.logical_bytes
    }
}

impl fmt::Debug for KafkaFetchedRecordV0 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("KafkaFetchedRecordV0")
            .field("has_key", &self.key.is_some())
            .field("key_bytes", &self.key.as_ref().map_or(0, Vec::len))
            .field("is_tombstone", &self.is_tombstone())
            .field("value_bytes", &self.value.as_ref().map_or(0, Vec::len))
            .field("timestamp_kind", &self.timestamp.kind())
            .field("header_count", &self.headers.len())
            .field("header_bytes", &self.header_bytes)
            .field("logical_bytes", &self.logical_bytes)
            .finish()
    }
}

fn check_bound(
    actual: usize,
    maximum: usize,
    bound: KafkaRecordBoundV0,
) -> Result<(), KafkaRecordValidationErrorV0> {
    if actual > maximum {
        return Err(KafkaRecordValidationErrorV0::BoundExceeded {
            bound,
            maximum,
            actual,
        });
    }
    Ok(())
}

fn copy_bytes(
    source: &[u8],
    bound: KafkaRecordBoundV0,
) -> Result<Vec<u8>, KafkaRecordValidationErrorV0> {
    let mut output = Vec::new();
    output
        .try_reserve_exact(source.len())
        .map_err(|_| KafkaRecordValidationErrorV0::AllocationFailed { bound })?;
    output.extend_from_slice(source);
    Ok(output)
}

fn copy_string(
    source: &str,
    bound: KafkaRecordBoundV0,
) -> Result<String, KafkaRecordValidationErrorV0> {
    let mut output = String::new();
    output
        .try_reserve_exact(source.len())
        .map_err(|_| KafkaRecordValidationErrorV0::AllocationFailed { bound })?;
    output.push_str(source);
    Ok(output)
}

fn header_logical_bytes(
    key_bytes: usize,
    value_bytes: usize,
    has_value: bool,
) -> Result<usize, KafkaRecordValidationErrorV0> {
    LENGTH_PREFIX_BYTES
        .checked_add(key_bytes)
        .and_then(|total| total.checked_add(OPTIONAL_TAG_BYTES))
        .and_then(|total| total.checked_add(usize::from(has_value) * LENGTH_PREFIX_BYTES))
        .and_then(|total| total.checked_add(value_bytes))
        .ok_or(KafkaRecordValidationErrorV0::AccountingOverflow {
            bound: KafkaRecordBoundV0::HeaderBytes,
        })
}

fn record_logical_bytes(
    key_bytes: usize,
    has_key: bool,
    value_bytes: usize,
    has_value: bool,
    timestamp: KafkaRecordTimestampV0,
    header_bytes: usize,
) -> Result<usize, KafkaRecordValidationErrorV0> {
    POSITION_BYTES
        .checked_add(OPTIONAL_TAG_BYTES)
        .and_then(|total| total.checked_add(usize::from(has_key) * LENGTH_PREFIX_BYTES))
        .and_then(|total| total.checked_add(key_bytes))
        .and_then(|total| total.checked_add(OPTIONAL_TAG_BYTES))
        .and_then(|total| total.checked_add(usize::from(has_value) * LENGTH_PREFIX_BYTES))
        .and_then(|total| total.checked_add(value_bytes))
        .and_then(|total| total.checked_add(TIMESTAMP_KIND_BYTES))
        .and_then(|total| {
            total.checked_add(
                usize::from(timestamp.kind() != KafkaRecordTimestampKindV0::NotAvailable)
                    * TIMESTAMP_VALUE_BYTES,
            )
        })
        .and_then(|total| total.checked_add(HEADER_COUNT_BYTES))
        .and_then(|total| total.checked_add(header_bytes))
        .ok_or(KafkaRecordValidationErrorV0::AccountingOverflow {
            bound: KafkaRecordBoundV0::RecordBytes,
        })
}
