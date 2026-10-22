use std::error::Error;
use std::fmt;

use decant_ir::{KafkaPositionV0, ValidationErrorV0};
use decant_kafka::{
    KafkaFetchedRecordV0, KafkaRecordHeaderV0, KafkaRecordTimestampV0,
    KafkaRecordValidationErrorV0, MAX_KAFKA_RECORD_HEADERS_V0,
};
use rdkafka::message::{Headers, Message, Timestamp};

#[derive(Clone, Eq, PartialEq)]
pub enum KafkaRecordAdapterErrorV0 {
    TopicMismatch,
    UnassignedPartition,
    Position(ValidationErrorV0),
    TooManyHeaders { maximum: usize, actual: usize },
    Header(KafkaRecordValidationErrorV0),
    HeaderAllocation,
    Record(KafkaRecordValidationErrorV0),
}

impl fmt::Debug for KafkaRecordAdapterErrorV0 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::TopicMismatch => formatter.write_str("TopicMismatch"),
            Self::UnassignedPartition => formatter.write_str("UnassignedPartition"),
            Self::Position(error) => formatter.debug_tuple("Position").field(error).finish(),
            Self::TooManyHeaders { maximum, actual } => formatter
                .debug_struct("TooManyHeaders")
                .field("maximum", maximum)
                .field("actual", actual)
                .finish(),
            Self::Header(error) => formatter.debug_tuple("Header").field(error).finish(),
            Self::HeaderAllocation => formatter.write_str("HeaderAllocation"),
            Self::Record(error) => formatter.debug_tuple("Record").field(error).finish(),
        }
    }
}

impl fmt::Display for KafkaRecordAdapterErrorV0 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(self, formatter)
    }
}

impl Error for KafkaRecordAdapterErrorV0 {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Position(error) => Some(error),
            Self::Header(error) | Self::Record(error) => Some(error),
            Self::TopicMismatch
            | Self::UnassignedPartition
            | Self::TooManyHeaders { .. }
            | Self::HeaderAllocation => None,
        }
    }
}

pub fn convert_kafka_message_v0<M: Message>(
    message: &M,
    expected_topic: &str,
    is_assigned_partition: impl FnOnce(i32) -> bool,
) -> Result<KafkaFetchedRecordV0, KafkaRecordAdapterErrorV0> {
    if message.topic() != expected_topic {
        return Err(KafkaRecordAdapterErrorV0::TopicMismatch);
    }
    let partition = message.partition();
    if !is_assigned_partition(partition) {
        return Err(KafkaRecordAdapterErrorV0::UnassignedPartition);
    }
    let position = KafkaPositionV0::new(partition, message.offset())
        .map_err(KafkaRecordAdapterErrorV0::Position)?;
    let timestamp = match message.timestamp() {
        Timestamp::NotAvailable => KafkaRecordTimestampV0::NotAvailable,
        Timestamp::CreateTime(unix_ms) => KafkaRecordTimestampV0::CreateTime { unix_ms },
        Timestamp::LogAppendTime(unix_ms) => KafkaRecordTimestampV0::LogAppendTime { unix_ms },
    };

    let header_count = message.headers().map_or(0, Headers::count);
    if header_count > MAX_KAFKA_RECORD_HEADERS_V0 {
        return Err(KafkaRecordAdapterErrorV0::TooManyHeaders {
            maximum: MAX_KAFKA_RECORD_HEADERS_V0,
            actual: header_count,
        });
    }
    let mut headers = Vec::new();
    headers
        .try_reserve_exact(header_count)
        .map_err(|_| KafkaRecordAdapterErrorV0::HeaderAllocation)?;
    if let Some(native_headers) = message.headers() {
        for header in native_headers.iter() {
            headers.push(
                KafkaRecordHeaderV0::new(header.key, header.value)
                    .map_err(KafkaRecordAdapterErrorV0::Header)?,
            );
        }
    }

    KafkaFetchedRecordV0::new(
        position,
        message.key(),
        message.payload(),
        timestamp,
        &headers,
    )
    .map_err(KafkaRecordAdapterErrorV0::Record)
}
