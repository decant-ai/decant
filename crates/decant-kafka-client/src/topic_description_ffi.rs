//! Isolated safe wrapper over librdkafka DescribeTopics topic-ID access.

use std::error::Error;
use std::ffi::{CStr, CString};
use std::fmt;

use decant_kafka::{KafkaSourceContractErrorV0, KafkaTopicIdV0};
use rdkafka::bindings as ffi;
use rdkafka::types::RDKafka;

const ERROR_BUFFER_BYTES: usize = 512;

#[derive(Clone, Eq, PartialEq)]
pub enum KafkaTopicDescriptionFfiErrorV0 {
    InvalidTopicName,
    TopicCollectionAllocation,
    AdminOptionsAllocation,
    RequestTimeoutConfiguration,
    QueueAllocation,
    RequestTimeout,
    RequestFailure,
    MissingResult,
    UnexpectedDescriptionCount { actual: usize },
    MissingDescriptionArray,
    MissingDescription,
    DescriptionFailure,
    MissingReturnedTopicName,
    ReturnedTopicNameMismatch,
    MissingTopicId,
    InvalidTopicId(KafkaSourceContractErrorV0),
}

impl fmt::Debug for KafkaTopicDescriptionFfiErrorV0 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidTopicName => formatter.write_str("InvalidTopicName"),
            Self::TopicCollectionAllocation => formatter.write_str("TopicCollectionAllocation"),
            Self::AdminOptionsAllocation => formatter.write_str("AdminOptionsAllocation"),
            Self::RequestTimeoutConfiguration => formatter.write_str("RequestTimeoutConfiguration"),
            Self::QueueAllocation => formatter.write_str("QueueAllocation"),
            Self::RequestTimeout => formatter.write_str("RequestTimeout"),
            Self::RequestFailure => formatter.write_str("RequestFailure"),
            Self::MissingResult => formatter.write_str("MissingResult"),
            Self::UnexpectedDescriptionCount { actual } => formatter
                .debug_struct("UnexpectedDescriptionCount")
                .field("actual", actual)
                .finish(),
            Self::MissingDescriptionArray => formatter.write_str("MissingDescriptionArray"),
            Self::MissingDescription => formatter.write_str("MissingDescription"),
            Self::DescriptionFailure => formatter.write_str("DescriptionFailure"),
            Self::MissingReturnedTopicName => formatter.write_str("MissingReturnedTopicName"),
            Self::ReturnedTopicNameMismatch => formatter.write_str("ReturnedTopicNameMismatch"),
            Self::MissingTopicId => formatter.write_str("MissingTopicId"),
            Self::InvalidTopicId(error) => formatter
                .debug_tuple("InvalidTopicId")
                .field(error)
                .finish(),
        }
    }
}

impl fmt::Display for KafkaTopicDescriptionFfiErrorV0 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(self, formatter)
    }
}

impl Error for KafkaTopicDescriptionFfiErrorV0 {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::InvalidTopicId(error) => Some(error),
            _ => None,
        }
    }
}

pub(crate) fn describe_kafka_topic_id_v0(
    client: *mut RDKafka,
    topic_name: &str,
    timeout_ms: i32,
) -> Result<KafkaTopicIdV0, KafkaTopicDescriptionFfiErrorV0> {
    let topic =
        CString::new(topic_name).map_err(|_| KafkaTopicDescriptionFfiErrorV0::InvalidTopicName)?;
    let mut topic_names = [topic.as_ptr()];

    // SAFETY: the one-element pointer array remains alive until the returned
    // collection is destroyed, and CString guarantees a terminating NUL.
    let topics = unsafe {
        TopicCollection::new(ffi::rd_kafka_TopicCollection_of_topic_names(
            topic_names.as_mut_ptr(),
            topic_names.len(),
        ))
    }
    .ok_or(KafkaTopicDescriptionFfiErrorV0::TopicCollectionAllocation)?;

    // SAFETY: `client` belongs to the borrowed BaseConsumer and remains alive
    // for the complete synchronous request; the enum matches this operation.
    let options = unsafe {
        AdminOptions::new(ffi::rd_kafka_AdminOptions_new(
            client,
            ffi::rd_kafka_admin_op_t::RD_KAFKA_ADMIN_OP_DESCRIBETOPICS,
        ))
    }
    .ok_or(KafkaTopicDescriptionFfiErrorV0::AdminOptionsAllocation)?;
    let mut error_buffer = [0_i8; ERROR_BUFFER_BYTES];
    // SAFETY: options is non-null and owned; the writable error buffer has the
    // exact length passed and is not inspected or exposed.
    let timeout_result = unsafe {
        ffi::rd_kafka_AdminOptions_set_request_timeout(
            options.0,
            timeout_ms,
            error_buffer.as_mut_ptr(),
            error_buffer.len(),
        )
    };
    if timeout_result != ffi::rd_kafka_resp_err_t::RD_KAFKA_RESP_ERR_NO_ERROR {
        return Err(KafkaTopicDescriptionFfiErrorV0::RequestTimeoutConfiguration);
    }

    // SAFETY: client remains alive and librdkafka returns an independently
    // reference-counted queue that this guard destroys exactly once.
    let queue = unsafe { Queue::new(ffi::rd_kafka_queue_new(client)) }
        .ok_or(KafkaTopicDescriptionFfiErrorV0::QueueAllocation)?;

    // SAFETY: all pointers are non-null, operation-matched, and alive for the
    // call. librdkafka enqueues completion on `queue` and retains no Rust data.
    unsafe { ffi::rd_kafka_DescribeTopics(client, topics.0, options.0, queue.0) };
    // SAFETY: queue is valid and exclusively borrowed for this finite poll.
    let event = unsafe { Event::new(ffi::rd_kafka_queue_poll(queue.0, timeout_ms)) }
        .ok_or(KafkaTopicDescriptionFfiErrorV0::RequestTimeout)?;
    // SAFETY: event is a valid live librdkafka event pointer.
    let event_error = unsafe { ffi::rd_kafka_event_error(event.0) };
    if event_error != ffi::rd_kafka_resp_err_t::RD_KAFKA_RESP_ERR_NO_ERROR {
        return Err(KafkaTopicDescriptionFfiErrorV0::RequestFailure);
    }
    topic_id_from_event(&event, &topic)
}

fn topic_id_from_event(
    event: &Event,
    expected_topic: &CStr,
) -> Result<KafkaTopicIdV0, KafkaTopicDescriptionFfiErrorV0> {
    // SAFETY: the event is alive, successful, and expected to carry this
    // operation result. A mismatched event is represented by null and refused.
    let result = unsafe { ffi::rd_kafka_event_DescribeTopics_result(event.0) };
    if result.is_null() {
        return Err(KafkaTopicDescriptionFfiErrorV0::MissingResult);
    }
    let mut count = 0_usize;
    // SAFETY: result is borrowed from the still-live event; `count` is a valid
    // writable out-parameter and the returned array lives with the event.
    let descriptions = unsafe { ffi::rd_kafka_DescribeTopics_result_topics(result, &mut count) };
    if count != 1 {
        return Err(KafkaTopicDescriptionFfiErrorV0::UnexpectedDescriptionCount { actual: count });
    }
    if descriptions.is_null() {
        return Err(KafkaTopicDescriptionFfiErrorV0::MissingDescriptionArray);
    }
    // SAFETY: the result declared exactly one entry and the array is non-null.
    let description = unsafe { *descriptions };
    if description.is_null() {
        return Err(KafkaTopicDescriptionFfiErrorV0::MissingDescription);
    }
    // SAFETY: description is borrowed from the live event. A non-null error
    // pointer denotes a per-topic failure and is never dereferenced/logged.
    if !unsafe { ffi::rd_kafka_TopicDescription_error(description) }.is_null() {
        return Err(KafkaTopicDescriptionFfiErrorV0::DescriptionFailure);
    }
    // SAFETY: description remains alive; the returned name pointer, when
    // non-null, is NUL-terminated and valid for the event lifetime.
    let returned_name = unsafe { ffi::rd_kafka_TopicDescription_name(description) };
    if returned_name.is_null() {
        return Err(KafkaTopicDescriptionFfiErrorV0::MissingReturnedTopicName);
    }
    // SAFETY: non-null returned topic names are documented C strings owned by
    // the result and remain alive while `event` is borrowed.
    if unsafe { CStr::from_ptr(returned_name) } != expected_topic {
        return Err(KafkaTopicDescriptionFfiErrorV0::ReturnedTopicNameMismatch);
    }
    // SAFETY: description is valid and alive; null is handled before UUID use.
    let topic_id = unsafe { ffi::rd_kafka_TopicDescription_topic_id(description) };
    if topic_id.is_null() {
        return Err(KafkaTopicDescriptionFfiErrorV0::MissingTopicId);
    }
    // SAFETY: topic_id is a valid UUID borrowed from the live description.
    let most = unsafe { ffi::rd_kafka_Uuid_most_significant_bits(topic_id) };
    // SAFETY: same valid borrowed UUID as above.
    let least = unsafe { ffi::rd_kafka_Uuid_least_significant_bits(topic_id) };
    KafkaTopicIdV0::from_bytes(uuid_words_to_bytes(most, least))
        .map_err(KafkaTopicDescriptionFfiErrorV0::InvalidTopicId)
}

fn uuid_words_to_bytes(most: i64, least: i64) -> [u8; 16] {
    let mut output = [0_u8; 16];
    output[..8].copy_from_slice(&most.to_be_bytes());
    output[8..].copy_from_slice(&least.to_be_bytes());
    output
}

struct TopicCollection(*mut ffi::rd_kafka_TopicCollection_t);

impl TopicCollection {
    unsafe fn new(pointer: *mut ffi::rd_kafka_TopicCollection_t) -> Option<Self> {
        (!pointer.is_null()).then_some(Self(pointer))
    }
}

impl Drop for TopicCollection {
    fn drop(&mut self) {
        // SAFETY: the guard owns one non-null collection and drops it once.
        unsafe { ffi::rd_kafka_TopicCollection_destroy(self.0) };
    }
}

struct AdminOptions(*mut ffi::rd_kafka_AdminOptions_t);

impl AdminOptions {
    unsafe fn new(pointer: *mut ffi::rd_kafka_AdminOptions_t) -> Option<Self> {
        (!pointer.is_null()).then_some(Self(pointer))
    }
}

impl Drop for AdminOptions {
    fn drop(&mut self) {
        // SAFETY: the guard owns one non-null options object and drops it once.
        unsafe { ffi::rd_kafka_AdminOptions_destroy(self.0) };
    }
}

struct Queue(*mut ffi::rd_kafka_queue_t);

impl Queue {
    unsafe fn new(pointer: *mut ffi::rd_kafka_queue_t) -> Option<Self> {
        (!pointer.is_null()).then_some(Self(pointer))
    }
}

impl Drop for Queue {
    fn drop(&mut self) {
        // SAFETY: the guard owns one non-null queue and drops it once.
        unsafe { ffi::rd_kafka_queue_destroy(self.0) };
    }
}

struct Event(*mut ffi::rd_kafka_event_t);

impl Event {
    unsafe fn new(pointer: *mut ffi::rd_kafka_event_t) -> Option<Self> {
        (!pointer.is_null()).then_some(Self(pointer))
    }
}

impl Drop for Event {
    fn drop(&mut self) {
        // SAFETY: the guard owns one non-null event and drops it once.
        unsafe { ffi::rd_kafka_event_destroy(self.0) };
    }
}

#[cfg(test)]
mod tests {
    use super::uuid_words_to_bytes;

    #[test]
    fn uuid_words_use_exact_network_order_and_preserve_signed_bits() {
        assert_eq!(
            uuid_words_to_bytes(0, 1),
            [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1]
        );
        assert_eq!(
            uuid_words_to_bytes(i64::MIN, -1),
            [
                0x80, 0, 0, 0, 0, 0, 0, 0, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff
            ]
        );
        assert_eq!(
            uuid_words_to_bytes(0x0102_0304_0506_0708, 0x1112_1314_1516_1718),
            [1, 2, 3, 4, 5, 6, 7, 8, 17, 18, 19, 20, 21, 22, 23, 24]
        );
    }
}
