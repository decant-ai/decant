//! Bounded protocol-neutral framing for LF-terminated records.
//!
//! This private V0 crate neither parses JSON nor opens filesystem paths.

#![forbid(unsafe_code)]

use std::error::Error;
use std::fmt;
use std::io::BufRead;

pub const JSONL_READER_VERSION_V0: u16 = 0;
pub const MAX_JSONL_RECORD_BYTES_V0: usize = 4 * 1_024 * 1_024;
pub const MAX_JSONL_RECORDS_V0: usize = 100_000;
pub const MAX_JSONL_INPUT_BYTES_V0: usize = 256 * 1_024 * 1_024;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum JsonlLimitFieldV0 {
    RecordBytes,
    Records,
    InputBytes,
}

#[derive(Clone, Eq, PartialEq)]
pub enum JsonlConfigErrorV0 {
    UnsupportedVersion {
        actual: u16,
    },
    Zero {
        field: JsonlLimitFieldV0,
    },
    AboveMaximum {
        field: JsonlLimitFieldV0,
        maximum: usize,
        actual: usize,
    },
    RecordLimitExceedsInputLimit {
        record_byte_limit: usize,
        input_byte_limit: usize,
    },
}

impl fmt::Debug for JsonlConfigErrorV0 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::UnsupportedVersion { actual } => formatter
                .debug_struct("UnsupportedVersion")
                .field("actual", actual)
                .finish(),
            Self::Zero { field } => formatter
                .debug_struct("Zero")
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
            Self::RecordLimitExceedsInputLimit {
                record_byte_limit,
                input_byte_limit,
            } => formatter
                .debug_struct("RecordLimitExceedsInputLimit")
                .field("record_byte_limit", record_byte_limit)
                .field("input_byte_limit", input_byte_limit)
                .finish(),
        }
    }
}

impl fmt::Display for JsonlConfigErrorV0 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(self, formatter)
    }
}

impl Error for JsonlConfigErrorV0 {}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct JsonlLimitsV0 {
    record_byte_limit: usize,
    record_limit: usize,
    input_byte_limit: usize,
}

impl JsonlLimitsV0 {
    pub fn new(
        version: u16,
        record_byte_limit: usize,
        record_limit: usize,
        input_byte_limit: usize,
    ) -> Result<Self, JsonlConfigErrorV0> {
        if version != JSONL_READER_VERSION_V0 {
            return Err(JsonlConfigErrorV0::UnsupportedVersion { actual: version });
        }
        validate_limit(
            record_byte_limit,
            MAX_JSONL_RECORD_BYTES_V0,
            JsonlLimitFieldV0::RecordBytes,
        )?;
        validate_limit(
            record_limit,
            MAX_JSONL_RECORDS_V0,
            JsonlLimitFieldV0::Records,
        )?;
        validate_limit(
            input_byte_limit,
            MAX_JSONL_INPUT_BYTES_V0,
            JsonlLimitFieldV0::InputBytes,
        )?;
        if record_byte_limit > input_byte_limit {
            return Err(JsonlConfigErrorV0::RecordLimitExceedsInputLimit {
                record_byte_limit,
                input_byte_limit,
            });
        }
        Ok(Self {
            record_byte_limit,
            record_limit,
            input_byte_limit,
        })
    }

    pub const fn maximum() -> Self {
        Self {
            record_byte_limit: MAX_JSONL_RECORD_BYTES_V0,
            record_limit: MAX_JSONL_RECORDS_V0,
            input_byte_limit: MAX_JSONL_INPUT_BYTES_V0,
        }
    }

    pub const fn record_byte_limit(self) -> usize {
        self.record_byte_limit
    }

    pub const fn record_limit(self) -> usize {
        self.record_limit
    }

    pub const fn input_byte_limit(self) -> usize {
        self.input_byte_limit
    }
}

fn validate_limit(
    actual: usize,
    maximum: usize,
    field: JsonlLimitFieldV0,
) -> Result<(), JsonlConfigErrorV0> {
    if actual == 0 {
        return Err(JsonlConfigErrorV0::Zero { field });
    }
    if actual > maximum {
        return Err(JsonlConfigErrorV0::AboveMaximum {
            field,
            maximum,
            actual,
        });
    }
    Ok(())
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct JsonlUsageV0 {
    completed_records: u64,
    consumed_bytes: u64,
}

impl JsonlUsageV0 {
    pub const fn completed_records(self) -> u64 {
        self.completed_records
    }

    pub const fn consumed_bytes(self) -> u64 {
        self.consumed_bytes
    }
}

#[derive(Clone, Eq, PartialEq)]
pub struct JsonlRecordV0 {
    line_number: u64,
    bytes: Vec<u8>,
}

impl JsonlRecordV0 {
    pub const fn line_number(&self) -> u64 {
        self.line_number
    }

    pub fn as_bytes(&self) -> &[u8] {
        &self.bytes
    }

    pub fn into_bytes(self) -> Vec<u8> {
        self.bytes
    }
}

impl fmt::Debug for JsonlRecordV0 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("JsonlRecordV0")
            .field("line_number", &self.line_number)
            .field("byte_len", &self.bytes.len())
            .field("bytes", &"<redacted>")
            .finish()
    }
}

#[derive(Clone, Eq, PartialEq)]
pub enum JsonlReadErrorV0 {
    Io,
    MissingTerminalLineFeed {
        line_number: u64,
        fragment_bytes: usize,
    },
    RecordBytesExceeded {
        line_number: u64,
        maximum: usize,
        attempted: usize,
    },
    RecordCountExceeded {
        maximum: usize,
    },
    InputBytesExceeded {
        maximum: usize,
        attempted: u64,
    },
    LineNumberOverflow,
    UsageOverflow,
    AllocationFailure {
        attempted_record_bytes: usize,
    },
    ReaderFailed,
}

impl fmt::Debug for JsonlReadErrorV0 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io => formatter.write_str("Io"),
            Self::MissingTerminalLineFeed {
                line_number,
                fragment_bytes,
            } => formatter
                .debug_struct("MissingTerminalLineFeed")
                .field("line_number", line_number)
                .field("fragment_bytes", fragment_bytes)
                .finish(),
            Self::RecordBytesExceeded {
                line_number,
                maximum,
                attempted,
            } => formatter
                .debug_struct("RecordBytesExceeded")
                .field("line_number", line_number)
                .field("maximum", maximum)
                .field("attempted", attempted)
                .finish(),
            Self::RecordCountExceeded { maximum } => formatter
                .debug_struct("RecordCountExceeded")
                .field("maximum", maximum)
                .finish(),
            Self::InputBytesExceeded { maximum, attempted } => formatter
                .debug_struct("InputBytesExceeded")
                .field("maximum", maximum)
                .field("attempted", attempted)
                .finish(),
            Self::LineNumberOverflow => formatter.write_str("LineNumberOverflow"),
            Self::UsageOverflow => formatter.write_str("UsageOverflow"),
            Self::AllocationFailure {
                attempted_record_bytes,
            } => formatter
                .debug_struct("AllocationFailure")
                .field("attempted_record_bytes", attempted_record_bytes)
                .finish(),
            Self::ReaderFailed => formatter.write_str("ReaderFailed"),
        }
    }
}

impl fmt::Display for JsonlReadErrorV0 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(self, formatter)
    }
}

impl Error for JsonlReadErrorV0 {}

pub struct BoundedJsonlReaderV0<R> {
    inner: R,
    limits: JsonlLimitsV0,
    usage: JsonlUsageV0,
    eof: bool,
    failed: bool,
}

impl<R: BufRead> BoundedJsonlReaderV0<R> {
    pub const fn new(inner: R, limits: JsonlLimitsV0) -> Self {
        Self {
            inner,
            limits,
            usage: JsonlUsageV0 {
                completed_records: 0,
                consumed_bytes: 0,
            },
            eof: false,
            failed: false,
        }
    }

    pub const fn limits(&self) -> JsonlLimitsV0 {
        self.limits
    }

    pub const fn usage(&self) -> JsonlUsageV0 {
        self.usage
    }

    pub fn into_inner(self) -> R {
        self.inner
    }

    pub fn next_record(&mut self) -> Result<Option<JsonlRecordV0>, JsonlReadErrorV0> {
        if self.failed {
            return Err(JsonlReadErrorV0::ReaderFailed);
        }
        if self.eof {
            return Ok(None);
        }

        let line_number = self
            .usage
            .completed_records
            .checked_add(1)
            .ok_or_else(|| self.fail(JsonlReadErrorV0::LineNumberOverflow))?;
        let mut record = Vec::new();

        loop {
            let (take, terminal, available_empty) = {
                let available = match self.inner.fill_buf() {
                    Ok(available) => available,
                    Err(_) => {
                        self.failed = true;
                        return Err(JsonlReadErrorV0::Io);
                    }
                };
                if available.is_empty() {
                    (0, false, true)
                } else if let Some(index) = available.iter().position(|byte| *byte == b'\n') {
                    (index + 1, true, false)
                } else {
                    (available.len(), false, false)
                }
            };

            if available_empty {
                self.eof = true;
                if record.is_empty() {
                    return Ok(None);
                }
                return Err(self.fail(JsonlReadErrorV0::MissingTerminalLineFeed {
                    line_number,
                    fragment_bytes: record.len(),
                }));
            }

            if record.is_empty()
                && self.usage.completed_records
                    >= u64::try_from(self.limits.record_limit).unwrap_or(u64::MAX)
            {
                return Err(self.fail(JsonlReadErrorV0::RecordCountExceeded {
                    maximum: self.limits.record_limit,
                }));
            }

            let attempted_record_bytes = record.len().checked_add(take).ok_or_else(|| {
                self.fail(JsonlReadErrorV0::RecordBytesExceeded {
                    line_number,
                    maximum: self.limits.record_byte_limit,
                    attempted: usize::MAX,
                })
            })?;
            if attempted_record_bytes > self.limits.record_byte_limit {
                return Err(self.fail(JsonlReadErrorV0::RecordBytesExceeded {
                    line_number,
                    maximum: self.limits.record_byte_limit,
                    attempted: attempted_record_bytes,
                }));
            }

            let take_u64 =
                u64::try_from(take).map_err(|_| self.fail(JsonlReadErrorV0::UsageOverflow))?;
            let attempted_input_bytes = self
                .usage
                .consumed_bytes
                .checked_add(take_u64)
                .ok_or_else(|| self.fail(JsonlReadErrorV0::UsageOverflow))?;
            if attempted_input_bytes
                > u64::try_from(self.limits.input_byte_limit).unwrap_or(u64::MAX)
            {
                return Err(self.fail(JsonlReadErrorV0::InputBytesExceeded {
                    maximum: self.limits.input_byte_limit,
                    attempted: attempted_input_bytes,
                }));
            }

            record.try_reserve_exact(take).map_err(|_| {
                self.fail(JsonlReadErrorV0::AllocationFailure {
                    attempted_record_bytes,
                })
            })?;
            {
                let available = match self.inner.fill_buf() {
                    Ok(available) => available,
                    Err(_) => {
                        self.failed = true;
                        return Err(JsonlReadErrorV0::Io);
                    }
                };
                record.extend_from_slice(&available[..take]);
            }
            self.inner.consume(take);
            self.usage.consumed_bytes = attempted_input_bytes;

            if terminal {
                self.usage.completed_records = line_number;
                return Ok(Some(JsonlRecordV0 {
                    line_number,
                    bytes: record,
                }));
            }
        }
    }

    fn fail(&mut self, error: JsonlReadErrorV0) -> JsonlReadErrorV0 {
        self.failed = true;
        error
    }
}

impl<R> fmt::Debug for BoundedJsonlReaderV0<R> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("BoundedJsonlReaderV0")
            .field("limits", &self.limits)
            .field("usage", &self.usage)
            .field("eof", &self.eof)
            .field("failed", &self.failed)
            .field("inner", &"<redacted>")
            .finish()
    }
}
