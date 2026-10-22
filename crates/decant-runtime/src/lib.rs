//! Bounded synchronous event transport for Decant's first in-memory slice.
//!
//! An event reported as enqueued has crossed only the in-memory transport
//! boundary. Semantic acceptance belongs to the future state engine.

#![forbid(unsafe_code)]

use std::collections::VecDeque;
use std::error::Error;
use std::fmt;
use std::sync::{Arc, Condvar, Mutex, MutexGuard, PoisonError};

use decant_ir::{EventEnvelopeV0, SourcePositionV0};

pub const MAX_CHANNEL_EVENTS_V0: usize = 1_024;
pub const MAX_CHANNEL_ACCOUNTED_BYTES_V0: usize = 16 * 1_024 * 1_024;

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
pub enum ChannelLimitFieldV0 {
    Events,
    AccountedBytes,
}

#[derive(Clone, Eq, PartialEq)]
pub enum ChannelConfigErrorV0 {
    Zero {
        field: ChannelLimitFieldV0,
    },
    AboveMaximum {
        field: ChannelLimitFieldV0,
        maximum: usize,
        actual: usize,
    },
}

impl fmt::Debug for ChannelConfigErrorV0 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
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
        }
    }
}

impl fmt::Display for ChannelConfigErrorV0 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(self, formatter)
    }
}

impl Error for ChannelConfigErrorV0 {}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ChannelLimitsV0 {
    event_limit: usize,
    accounted_byte_limit: usize,
}

impl ChannelLimitsV0 {
    pub fn new(
        event_limit: usize,
        accounted_byte_limit: usize,
    ) -> Result<Self, ChannelConfigErrorV0> {
        validate_limit(
            event_limit,
            MAX_CHANNEL_EVENTS_V0,
            ChannelLimitFieldV0::Events,
        )?;
        validate_limit(
            accounted_byte_limit,
            MAX_CHANNEL_ACCOUNTED_BYTES_V0,
            ChannelLimitFieldV0::AccountedBytes,
        )?;
        Ok(Self {
            event_limit,
            accounted_byte_limit,
        })
    }

    pub const fn maximum() -> Self {
        Self {
            event_limit: MAX_CHANNEL_EVENTS_V0,
            accounted_byte_limit: MAX_CHANNEL_ACCOUNTED_BYTES_V0,
        }
    }

    pub const fn event_limit(self) -> usize {
        self.event_limit
    }

    pub const fn accounted_byte_limit(self) -> usize {
        self.accounted_byte_limit
    }
}

fn validate_limit(
    actual: usize,
    maximum: usize,
    field: ChannelLimitFieldV0,
) -> Result<(), ChannelConfigErrorV0> {
    if actual == 0 {
        return Err(ChannelConfigErrorV0::Zero { field });
    }
    if actual > maximum {
        return Err(ChannelConfigErrorV0::AboveMaximum {
            field,
            maximum,
            actual,
        });
    }
    Ok(())
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum FootprintComponentV0 {
    EnvelopeVersion,
    SourceId,
    SchemaId,
    SourcePosition,
    Key,
    Timestamps,
    Sensitivity,
    Payload,
    Provenance,
}

#[derive(Clone, Eq, PartialEq)]
pub struct FootprintErrorV0 {
    component: FootprintComponentV0,
}

impl FootprintErrorV0 {
    pub const fn component(&self) -> FootprintComponentV0 {
        self.component
    }
}

impl fmt::Debug for FootprintErrorV0 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("FootprintErrorV0")
            .field("component", &self.component)
            .finish()
    }
}

impl fmt::Display for FootprintErrorV0 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(self, formatter)
    }
}

impl Error for FootprintErrorV0 {}

pub fn event_accounted_bytes_v0(event: &EventEnvelopeV0) -> Result<usize, FootprintErrorV0> {
    let mut bytes = 0;
    checked_add(
        &mut bytes,
        ENVELOPE_VERSION_WIDTH,
        FootprintComponentV0::EnvelopeVersion,
    )?;
    checked_add(
        &mut bytes,
        LENGTH_PREFIX_WIDTH,
        FootprintComponentV0::SourceId,
    )?;
    checked_add(
        &mut bytes,
        event.source_id().as_str().len(),
        FootprintComponentV0::SourceId,
    )?;
    checked_add(
        &mut bytes,
        LENGTH_PREFIX_WIDTH,
        FootprintComponentV0::SchemaId,
    )?;
    checked_add(
        &mut bytes,
        event.schema_id().as_str().len(),
        FootprintComponentV0::SchemaId,
    )?;
    match event.source_position() {
        SourcePositionV0::Synthetic { .. } => {
            checked_add(
                &mut bytes,
                SOURCE_POSITION_TAG_WIDTH,
                FootprintComponentV0::SourcePosition,
            )?;
            checked_add(
                &mut bytes,
                SYNTHETIC_SEQUENCE_WIDTH,
                FootprintComponentV0::SourcePosition,
            )?;
        }
        SourcePositionV0::OtlpFile { .. } => {
            checked_add(
                &mut bytes,
                SOURCE_POSITION_TAG_WIDTH,
                FootprintComponentV0::SourcePosition,
            )?;
            checked_add(
                &mut bytes,
                OTLP_FILE_LINE_NUMBER_WIDTH,
                FootprintComponentV0::SourcePosition,
            )?;
            for _ in 0..3 {
                checked_add(
                    &mut bytes,
                    OTLP_FILE_INDEX_WIDTH,
                    FootprintComponentV0::SourcePosition,
                )?;
            }
        }
        SourcePositionV0::Kafka { .. } => {
            checked_add(
                &mut bytes,
                SOURCE_POSITION_TAG_WIDTH,
                FootprintComponentV0::SourcePosition,
            )?;
            checked_add(
                &mut bytes,
                KAFKA_PARTITION_WIDTH,
                FootprintComponentV0::SourcePosition,
            )?;
            checked_add(
                &mut bytes,
                KAFKA_OFFSET_WIDTH,
                FootprintComponentV0::SourcePosition,
            )?;
        }
    }
    checked_add(&mut bytes, KEY_PRESENCE_WIDTH, FootprintComponentV0::Key)?;
    if let Some(key) = event.key() {
        checked_add(&mut bytes, LENGTH_PREFIX_WIDTH, FootprintComponentV0::Key)?;
        checked_add(&mut bytes, key.len(), FootprintComponentV0::Key)?;
    }
    checked_add(
        &mut bytes,
        TIMESTAMP_WIDTH,
        FootprintComponentV0::Timestamps,
    )?;
    checked_add(
        &mut bytes,
        TIMESTAMP_WIDTH,
        FootprintComponentV0::Timestamps,
    )?;
    checked_add(
        &mut bytes,
        SENSITIVITY_WIDTH,
        FootprintComponentV0::Sensitivity,
    )?;
    checked_add(
        &mut bytes,
        LENGTH_PREFIX_WIDTH,
        FootprintComponentV0::Payload,
    )?;
    checked_add(
        &mut bytes,
        event.payload().logical_len(),
        FootprintComponentV0::Payload,
    )?;
    checked_add(
        &mut bytes,
        LENGTH_PREFIX_WIDTH,
        FootprintComponentV0::Provenance,
    )?;
    checked_add(
        &mut bytes,
        event.provenance().as_str().len(),
        FootprintComponentV0::Provenance,
    )?;
    Ok(bytes)
}

fn checked_add(
    current: &mut usize,
    added: usize,
    component: FootprintComponentV0,
) -> Result<(), FootprintErrorV0> {
    *current = current
        .checked_add(added)
        .ok_or(FootprintErrorV0 { component })?;
    Ok(())
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ChannelUsageV0 {
    limits: ChannelLimitsV0,
    current_events: usize,
    current_accounted_bytes: usize,
    high_water_events: usize,
    high_water_accounted_bytes: usize,
}

impl ChannelUsageV0 {
    pub const fn limits(self) -> ChannelLimitsV0 {
        self.limits
    }

    pub const fn current_events(self) -> usize {
        self.current_events
    }

    pub const fn current_accounted_bytes(self) -> usize {
        self.current_accounted_bytes
    }

    pub const fn high_water_events(self) -> usize {
        self.high_water_events
    }

    pub const fn high_water_accounted_bytes(self) -> usize {
        self.high_water_accounted_bytes
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct EnqueueReceiptV0 {
    usage: ChannelUsageV0,
}

impl EnqueueReceiptV0 {
    pub const fn usage(self) -> ChannelUsageV0 {
        self.usage
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum EnqueueErrorKindV0 {
    Full,
    EventTooLarge,
    ReceiverClosed,
    AccountingOverflow,
}

pub struct EnqueueErrorV0 {
    kind: EnqueueErrorKindV0,
    event: EventEnvelopeV0,
    event_accounted_bytes: Option<usize>,
    usage: ChannelUsageV0,
}

impl EnqueueErrorV0 {
    pub const fn kind(&self) -> EnqueueErrorKindV0 {
        self.kind
    }

    pub const fn event_accounted_bytes(&self) -> Option<usize> {
        self.event_accounted_bytes
    }

    pub const fn usage(&self) -> ChannelUsageV0 {
        self.usage
    }

    pub fn into_event(self) -> EventEnvelopeV0 {
        self.event
    }
}

impl fmt::Debug for EnqueueErrorV0 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("EnqueueErrorV0")
            .field("kind", &self.kind)
            .field("event_accounted_bytes", &self.event_accounted_bytes)
            .field("usage", &self.usage)
            .finish()
    }
}

impl fmt::Display for EnqueueErrorV0 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(self, formatter)
    }
}

impl Error for EnqueueErrorV0 {}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ReceiveErrorV0 {
    Closed,
}

impl fmt::Display for ReceiveErrorV0 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(self, formatter)
    }
}

impl Error for ReceiveErrorV0 {}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TryReceiveErrorV0 {
    Empty,
    Closed,
}

impl fmt::Display for TryReceiveErrorV0 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(self, formatter)
    }
}

impl Error for TryReceiveErrorV0 {}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RecoveryErrorV0 {
    ReceiverStillConnected,
    AlreadyRecovered,
}

impl fmt::Display for RecoveryErrorV0 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(self, formatter)
    }
}

impl Error for RecoveryErrorV0 {}

struct QueuedEventV0 {
    event: EventEnvelopeV0,
    accounted_bytes: usize,
}

pub struct RecoveredEventsV0 {
    events: VecDeque<QueuedEventV0>,
    remaining_accounted_bytes: usize,
}

impl RecoveredEventsV0 {
    pub fn remaining_accounted_bytes(&self) -> usize {
        self.remaining_accounted_bytes
    }
}

impl fmt::Debug for RecoveredEventsV0 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RecoveredEventsV0")
            .field("remaining_events", &self.events.len())
            .field("remaining_accounted_bytes", &self.remaining_accounted_bytes)
            .finish()
    }
}

impl Iterator for RecoveredEventsV0 {
    type Item = EventEnvelopeV0;

    fn next(&mut self) -> Option<Self::Item> {
        let queued = self.events.pop_front()?;
        self.remaining_accounted_bytes = self
            .remaining_accounted_bytes
            .checked_sub(queued.accounted_bytes)
            .expect("recovered event byte ledger invariant violated");
        Some(queued.event)
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let remaining = self.events.len();
        (remaining, Some(remaining))
    }
}

impl ExactSizeIterator for RecoveredEventsV0 {}

struct ChannelStateV0 {
    queue: VecDeque<QueuedEventV0>,
    current_accounted_bytes: usize,
    high_water_events: usize,
    high_water_accounted_bytes: usize,
    sender_connected: bool,
    receiver_connected: bool,
    accepting_enqueues: bool,
    recovery_taken: bool,
}

struct ChannelInnerV0 {
    limits: ChannelLimitsV0,
    state: Mutex<ChannelStateV0>,
    can_enqueue: Condvar,
    can_receive: Condvar,
}

pub struct EventSenderV0 {
    inner: Arc<ChannelInnerV0>,
}

pub struct EventReceiverV0 {
    inner: Arc<ChannelInnerV0>,
}

pub fn bounded_event_channel_v0(limits: ChannelLimitsV0) -> (EventSenderV0, EventReceiverV0) {
    let state = ChannelStateV0 {
        queue: VecDeque::with_capacity(limits.event_limit()),
        current_accounted_bytes: 0,
        high_water_events: 0,
        high_water_accounted_bytes: 0,
        sender_connected: true,
        receiver_connected: true,
        accepting_enqueues: true,
        recovery_taken: false,
    };
    let inner = Arc::new(ChannelInnerV0 {
        limits,
        state: Mutex::new(state),
        can_enqueue: Condvar::new(),
        can_receive: Condvar::new(),
    });
    (
        EventSenderV0 {
            inner: Arc::clone(&inner),
        },
        EventReceiverV0 { inner },
    )
}

impl EventSenderV0 {
    // The non-enqueue path must return the owned event without adding a heap
    // allocation to the backpressure path.
    #[allow(clippy::result_large_err)]
    pub fn enqueue(&mut self, event: EventEnvelopeV0) -> Result<EnqueueReceiptV0, EnqueueErrorV0> {
        let event_accounted_bytes = match event_accounted_bytes_v0(&event) {
            Ok(bytes) => bytes,
            Err(_) => {
                let state = lock_state(&self.inner);
                return Err(enqueue_error(
                    EnqueueErrorKindV0::AccountingOverflow,
                    event,
                    None,
                    &self.inner,
                    &state,
                ));
            }
        };
        let mut state = lock_state(&self.inner);
        loop {
            if !state.receiver_connected || !state.accepting_enqueues {
                return Err(enqueue_error(
                    EnqueueErrorKindV0::ReceiverClosed,
                    event,
                    Some(event_accounted_bytes),
                    &self.inner,
                    &state,
                ));
            }
            if event_accounted_bytes > self.inner.limits.accounted_byte_limit() {
                return Err(enqueue_error(
                    EnqueueErrorKindV0::EventTooLarge,
                    event,
                    Some(event_accounted_bytes),
                    &self.inner,
                    &state,
                ));
            }
            if has_capacity(&self.inner, &state, event_accounted_bytes) {
                return Ok(enqueue_event(
                    &self.inner,
                    &mut state,
                    event,
                    event_accounted_bytes,
                ));
            }
            state = self
                .inner
                .can_enqueue
                .wait(state)
                .unwrap_or_else(PoisonError::into_inner);
        }
    }

    // The non-enqueue path must return the owned event without adding a heap
    // allocation to the backpressure path.
    #[allow(clippy::result_large_err)]
    pub fn try_enqueue(
        &mut self,
        event: EventEnvelopeV0,
    ) -> Result<EnqueueReceiptV0, EnqueueErrorV0> {
        let event_accounted_bytes = match event_accounted_bytes_v0(&event) {
            Ok(bytes) => bytes,
            Err(_) => {
                let state = lock_state(&self.inner);
                return Err(enqueue_error(
                    EnqueueErrorKindV0::AccountingOverflow,
                    event,
                    None,
                    &self.inner,
                    &state,
                ));
            }
        };
        let mut state = lock_state(&self.inner);
        let kind = if !state.receiver_connected || !state.accepting_enqueues {
            Some(EnqueueErrorKindV0::ReceiverClosed)
        } else if event_accounted_bytes > self.inner.limits.accounted_byte_limit() {
            Some(EnqueueErrorKindV0::EventTooLarge)
        } else if !has_capacity(&self.inner, &state, event_accounted_bytes) {
            Some(EnqueueErrorKindV0::Full)
        } else {
            None
        };
        if let Some(kind) = kind {
            return Err(enqueue_error(
                kind,
                event,
                Some(event_accounted_bytes),
                &self.inner,
                &state,
            ));
        }
        Ok(enqueue_event(
            &self.inner,
            &mut state,
            event,
            event_accounted_bytes,
        ))
    }

    pub fn usage(&self) -> ChannelUsageV0 {
        let state = lock_state(&self.inner);
        usage(&self.inner, &state)
    }

    pub fn recover_undelivered(&mut self) -> Result<RecoveredEventsV0, RecoveryErrorV0> {
        let mut state = lock_state(&self.inner);
        if state.receiver_connected {
            return Err(RecoveryErrorV0::ReceiverStillConnected);
        }
        if state.recovery_taken {
            return Err(RecoveryErrorV0::AlreadyRecovered);
        }
        state.recovery_taken = true;
        let events = std::mem::take(&mut state.queue);
        let remaining_accounted_bytes = state.current_accounted_bytes;
        state.current_accounted_bytes = 0;
        Ok(RecoveredEventsV0 {
            events,
            remaining_accounted_bytes,
        })
    }
}

impl fmt::Debug for EventSenderV0 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("EventSenderV0")
            .field("usage", &self.usage())
            .finish()
    }
}

impl Drop for EventSenderV0 {
    fn drop(&mut self) {
        let mut state = lock_state(&self.inner);
        state.sender_connected = false;
        self.inner.can_receive.notify_all();
    }
}

impl EventReceiverV0 {
    pub fn receive(&mut self) -> Result<EventEnvelopeV0, ReceiveErrorV0> {
        let mut state = lock_state(&self.inner);
        loop {
            if !state.queue.is_empty() {
                return Ok(dequeue_event(&self.inner, &mut state));
            }
            if !state.sender_connected || !state.accepting_enqueues {
                return Err(ReceiveErrorV0::Closed);
            }
            state = self
                .inner
                .can_receive
                .wait(state)
                .unwrap_or_else(PoisonError::into_inner);
        }
    }

    pub fn try_receive(&mut self) -> Result<EventEnvelopeV0, TryReceiveErrorV0> {
        let mut state = lock_state(&self.inner);
        if !state.queue.is_empty() {
            return Ok(dequeue_event(&self.inner, &mut state));
        }
        if !state.sender_connected || !state.accepting_enqueues {
            Err(TryReceiveErrorV0::Closed)
        } else {
            Err(TryReceiveErrorV0::Empty)
        }
    }

    pub fn close(&mut self) -> ChannelUsageV0 {
        let mut state = lock_state(&self.inner);
        state.accepting_enqueues = false;
        self.inner.can_enqueue.notify_all();
        usage(&self.inner, &state)
    }

    pub fn usage(&self) -> ChannelUsageV0 {
        let state = lock_state(&self.inner);
        usage(&self.inner, &state)
    }
}

impl fmt::Debug for EventReceiverV0 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("EventReceiverV0")
            .field("usage", &self.usage())
            .finish()
    }
}

impl Drop for EventReceiverV0 {
    fn drop(&mut self) {
        let mut state = lock_state(&self.inner);
        state.receiver_connected = false;
        state.accepting_enqueues = false;
        self.inner.can_enqueue.notify_all();
    }
}

fn has_capacity(
    inner: &ChannelInnerV0,
    state: &ChannelStateV0,
    event_accounted_bytes: usize,
) -> bool {
    state.queue.len() < inner.limits.event_limit()
        && state
            .current_accounted_bytes
            .checked_add(event_accounted_bytes)
            .is_some_and(|bytes| bytes <= inner.limits.accounted_byte_limit())
}

fn enqueue_event(
    inner: &ChannelInnerV0,
    state: &mut ChannelStateV0,
    event: EventEnvelopeV0,
    event_accounted_bytes: usize,
) -> EnqueueReceiptV0 {
    let next_accounted_bytes = state
        .current_accounted_bytes
        .checked_add(event_accounted_bytes)
        .expect("channel byte ledger invariant violated");
    state.queue.push_back(QueuedEventV0 {
        event,
        accounted_bytes: event_accounted_bytes,
    });
    state.current_accounted_bytes = next_accounted_bytes;
    state.high_water_events = state.high_water_events.max(state.queue.len());
    state.high_water_accounted_bytes = state
        .high_water_accounted_bytes
        .max(state.current_accounted_bytes);
    let receipt = EnqueueReceiptV0 {
        usage: usage(inner, state),
    };
    inner.can_receive.notify_one();
    receipt
}

fn dequeue_event(inner: &ChannelInnerV0, state: &mut ChannelStateV0) -> EventEnvelopeV0 {
    let accounted_bytes = state
        .queue
        .front()
        .expect("non-empty channel queue invariant violated")
        .accounted_bytes;
    let next_accounted_bytes = state
        .current_accounted_bytes
        .checked_sub(accounted_bytes)
        .expect("channel byte ledger invariant violated");
    let queued = state
        .queue
        .pop_front()
        .expect("non-empty channel queue invariant violated");
    state.current_accounted_bytes = next_accounted_bytes;
    inner.can_enqueue.notify_one();
    queued.event
}

fn enqueue_error(
    kind: EnqueueErrorKindV0,
    event: EventEnvelopeV0,
    event_accounted_bytes: Option<usize>,
    inner: &ChannelInnerV0,
    state: &ChannelStateV0,
) -> EnqueueErrorV0 {
    EnqueueErrorV0 {
        kind,
        event,
        event_accounted_bytes,
        usage: usage(inner, state),
    }
}

fn usage(inner: &ChannelInnerV0, state: &ChannelStateV0) -> ChannelUsageV0 {
    ChannelUsageV0 {
        limits: inner.limits,
        current_events: state.queue.len(),
        current_accounted_bytes: state.current_accounted_bytes,
        high_water_events: state.high_water_events,
        high_water_accounted_bytes: state.high_water_accounted_bytes,
    }
}

fn lock_state(inner: &ChannelInnerV0) -> MutexGuard<'_, ChannelStateV0> {
    inner.state.lock().unwrap_or_else(PoisonError::into_inner)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn footprint_addition_overflow_is_typed_and_non_mutating() {
        let mut current = usize::MAX;
        assert_eq!(
            checked_add(&mut current, 1, FootprintComponentV0::Payload),
            Err(FootprintErrorV0 {
                component: FootprintComponentV0::Payload,
            })
        );
        assert_eq!(current, usize::MAX);
    }
}
