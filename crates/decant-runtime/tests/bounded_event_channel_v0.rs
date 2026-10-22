use std::collections::BTreeMap;
use std::fmt::Write as _;
use std::sync::mpsc::TryRecvError;

use decant_ir::{
    CanonicalPayloadV0, ENVELOPE_VERSION_V0, EventEnvelopeV0, MAX_KEY_BYTES, MAX_SCALAR_BYTES,
    ScalarKindV0, ScalarValueV0, SchemaIdV0, SensitivityBitmapV0, SourceIdV0, SourcePositionV0,
};
use decant_runtime::{
    ChannelConfigErrorV0, ChannelLimitFieldV0, ChannelLimitsV0, EnqueueErrorKindV0,
    MAX_CHANNEL_ACCOUNTED_BYTES_V0, MAX_CHANNEL_EVENTS_V0, ReceiveErrorV0, RecoveryErrorV0,
    TryReceiveErrorV0, bounded_event_channel_v0, event_accounted_bytes_v0,
};

fn event(
    sequence: u64,
    key: Option<&[u8]>,
    fields: impl IntoIterator<Item = (&'static str, ScalarValueV0)>,
) -> EventEnvelopeV0 {
    EventEnvelopeV0::new(
        ENVELOPE_VERSION_V0,
        SourceIdV0::new("sre-v0").unwrap(),
        SchemaIdV0::new("runtime-test-v0").unwrap(),
        SourcePositionV0::Synthetic { sequence },
        key,
        -17,
        23,
        SensitivityBitmapV0::from_bits(0),
        CanonicalPayloadV0::try_from_fields(
            fields
                .into_iter()
                .map(|(name, value)| (name.to_owned(), value))
                .collect::<BTreeMap<_, _>>(),
        )
        .unwrap(),
    )
    .unwrap()
}

fn small_event(sequence: u64) -> EventEnvelopeV0 {
    event(
        sequence,
        Some(b"payments-v2/region-b"),
        [("value", ScalarValueV0::from_u64(sequence))],
    )
}

fn otlp_event() -> EventEnvelopeV0 {
    EventEnvelopeV0::new(
        ENVELOPE_VERSION_V0,
        SourceIdV0::new("otel-demo-capture").unwrap(),
        SchemaIdV0::new("otlp-span-v0").unwrap(),
        SourcePositionV0::otlp_file(7, 1, 2, 3).unwrap(),
        Some(&[4; 16]),
        100,
        0,
        SensitivityBitmapV0::from_bits(u64::MAX),
        CanonicalPayloadV0::try_from_fields(BTreeMap::from([(
            "span.name".to_owned(),
            ScalarValueV0::utf8("charge").unwrap(),
        )]))
        .unwrap(),
    )
    .unwrap()
}

fn kafka_event() -> EventEnvelopeV0 {
    EventEnvelopeV0::new(
        ENVELOPE_VERSION_V0,
        SourceIdV0::new("payments-kafka-v0").unwrap(),
        SchemaIdV0::new("kafka-json-v0").unwrap(),
        SourcePositionV0::kafka(23, 42).unwrap(),
        Some(b"payments-v2/region-b"),
        100,
        0,
        SensitivityBitmapV0::from_bits(u64::MAX),
        CanonicalPayloadV0::try_from_fields(BTreeMap::from([(
            "outcome".to_owned(),
            ScalarValueV0::utf8("error").unwrap(),
        )]))
        .unwrap(),
    )
    .unwrap()
}

fn sequence(event: &EventEnvelopeV0) -> u64 {
    event.source_position().synthetic_sequence()
}

fn independent_scalar_content_len(value: &ScalarValueV0) -> usize {
    match value.kind() {
        ScalarKindV0::Null => 0,
        ScalarKindV0::Bool => 1,
        ScalarKindV0::I64 | ScalarKindV0::U64 => 8,
        ScalarKindV0::Utf8 => 4 + value.as_utf8().unwrap().len(),
        ScalarKindV0::Bytes => 4 + value.as_bytes().unwrap().len(),
    }
}

fn independent_footprint(event: &EventEnvelopeV0) -> usize {
    let payload_bytes = 4 + event
        .payload()
        .fields()
        .map(|(name, value)| 4 + name.len() + 1 + independent_scalar_content_len(value))
        .sum::<usize>();
    let source_position_bytes = match event.source_position() {
        SourcePositionV0::Synthetic { .. } => 1 + 8,
        SourcePositionV0::OtlpFile { .. } => 1 + 8 + 4 + 4 + 4,
        SourcePositionV0::Kafka { .. } => 1 + 4 + 8,
    };
    2 + 4
        + event.source_id().as_str().len()
        + 4
        + event.schema_id().as_str().len()
        + source_position_bytes
        + 1
        + event.key().map_or(0, |key| 4 + key.len())
        + 8
        + 8
        + 8
        + 4
        + payload_bytes
        + 4
        + event.provenance().as_str().len()
}

#[test]
fn otlp_file_position_is_accounted_exactly_and_preserved_by_the_channel() {
    let event = otlp_event();
    let footprint = event_accounted_bytes_v0(&event).unwrap();
    assert_eq!(footprint, independent_footprint(&event));

    let (mut sender, mut receiver) =
        bounded_event_channel_v0(ChannelLimitsV0::new(1, footprint).unwrap());
    sender.try_enqueue(event.clone()).unwrap();
    assert_eq!(sender.usage().current_accounted_bytes(), footprint);
    let received = receiver.try_receive().unwrap();
    assert_eq!(received, event);
    let position = received.source_position().otlp_file_position().unwrap();
    assert_eq!(position.line_number(), 7);
    assert_eq!(position.resource_spans_index(), 1);
    assert_eq!(position.scope_spans_index(), 2);
    assert_eq!(position.span_index(), 3);
    assert_eq!(receiver.usage().current_accounted_bytes(), 0);
}

#[test]
fn kafka_position_is_accounted_exactly_and_preserved_by_the_channel() {
    let event = kafka_event();
    let footprint = event_accounted_bytes_v0(&event).unwrap();
    assert_eq!(footprint, independent_footprint(&event));

    let (mut sender, mut receiver) =
        bounded_event_channel_v0(ChannelLimitsV0::new(1, footprint).unwrap());
    sender.try_enqueue(event.clone()).unwrap();
    assert_eq!(sender.usage().current_accounted_bytes(), footprint);
    let received = receiver.try_receive().unwrap();
    assert_eq!(received, event);
    let position = received.source_position().kafka_position().unwrap();
    assert_eq!(position.partition(), 23);
    assert_eq!(position.offset(), 42);
    assert_eq!(receiver.usage().current_accounted_bytes(), 0);
}

#[test]
fn channel_limit_boundaries_are_exact() {
    assert_eq!(ChannelLimitsV0::new(1, 1).unwrap().event_limit(), 1);
    assert_eq!(
        ChannelLimitsV0::new(MAX_CHANNEL_EVENTS_V0, MAX_CHANNEL_ACCOUNTED_BYTES_V0).unwrap(),
        ChannelLimitsV0::maximum()
    );

    for (events, bytes, field) in [
        (0, 1, ChannelLimitFieldV0::Events),
        (1, 0, ChannelLimitFieldV0::AccountedBytes),
    ] {
        assert_eq!(
            ChannelLimitsV0::new(events, bytes),
            Err(ChannelConfigErrorV0::Zero { field })
        );
    }
    assert!(matches!(
        ChannelLimitsV0::new(MAX_CHANNEL_EVENTS_V0 + 1, 1),
        Err(ChannelConfigErrorV0::AboveMaximum {
            field: ChannelLimitFieldV0::Events,
            actual,
            ..
        }) if actual == MAX_CHANNEL_EVENTS_V0 + 1
    ));
    assert!(matches!(
        ChannelLimitsV0::new(1, MAX_CHANNEL_ACCOUNTED_BYTES_V0 + 1),
        Err(ChannelConfigErrorV0::AboveMaximum {
            field: ChannelLimitFieldV0::AccountedBytes,
            actual,
            ..
        }) if actual == MAX_CHANNEL_ACCOUNTED_BYTES_V0 + 1
    ));
}

#[test]
fn accounted_bytes_match_an_independent_formula() {
    let maximum_key = vec![b'k'; MAX_KEY_BYTES];
    let maximum_text = "t".repeat(MAX_SCALAR_BYTES);
    let values = [
        ("bytes", ScalarValueV0::bytes(b"opaque").unwrap()),
        ("flag", ScalarValueV0::from_bool(true)),
        ("negative", ScalarValueV0::from_i64(-99)),
        ("none", ScalarValueV0::null()),
        ("positive", ScalarValueV0::from_u64(99)),
        ("text", ScalarValueV0::utf8(&maximum_text).unwrap()),
    ];
    for candidate in [
        event(7, None, values.clone()),
        event(7, Some(&[]), values.clone()),
        event(7, Some(&maximum_key), values),
    ] {
        assert_eq!(
            event_accounted_bytes_v0(&candidate).unwrap(),
            independent_footprint(&candidate)
        );
    }
}

#[test]
fn exact_event_byte_limit_accepts_and_one_less_returns_the_event() {
    let candidate = small_event(1);
    let footprint = event_accounted_bytes_v0(&candidate).unwrap();

    let (mut sender, mut receiver) =
        bounded_event_channel_v0(ChannelLimitsV0::new(1, footprint).unwrap());
    let receipt = sender.enqueue(candidate.clone()).unwrap();
    assert_eq!(receipt.usage().current_accounted_bytes(), footprint);
    assert_eq!(receiver.receive().unwrap(), candidate);

    let (mut sender, receiver) =
        bounded_event_channel_v0(ChannelLimitsV0::new(1, footprint - 1).unwrap());
    let error = sender.try_enqueue(candidate.clone()).unwrap_err();
    assert_eq!(error.kind(), EnqueueErrorKindV0::EventTooLarge);
    assert_eq!(error.event_accounted_bytes(), Some(footprint));
    assert_eq!(error.usage().current_events(), 0);
    assert_eq!(error.usage().current_accounted_bytes(), 0);
    assert_eq!(error.into_event(), candidate);
    assert_eq!(receiver.usage().current_events(), 0);
}

#[test]
fn maximum_entry_cap_is_exact_and_full_is_non_mutating() {
    let (mut sender, mut receiver) = bounded_event_channel_v0(ChannelLimitsV0::maximum());
    for source_sequence in 0..MAX_CHANNEL_EVENTS_V0 as u64 {
        sender.enqueue(small_event(source_sequence)).unwrap();
    }
    let before = sender.usage();
    assert_eq!(before.current_events(), MAX_CHANNEL_EVENTS_V0);
    assert_eq!(before.high_water_events(), MAX_CHANNEL_EVENTS_V0);

    let rejected = small_event(MAX_CHANNEL_EVENTS_V0 as u64);
    let error = sender.try_enqueue(rejected.clone()).unwrap_err();
    assert_eq!(error.kind(), EnqueueErrorKindV0::Full);
    assert_eq!(error.usage(), before);
    assert_eq!(error.into_event(), rejected);
    assert_eq!(sender.usage(), before);

    for expected in 0..MAX_CHANNEL_EVENTS_V0 as u64 {
        assert_eq!(sequence(&receiver.receive().unwrap()), expected);
    }
    assert_eq!(receiver.usage().current_events(), 0);
    assert_eq!(receiver.usage().current_accounted_bytes(), 0);
}

#[test]
fn byte_cap_is_exact_and_dequeue_releases_the_cached_footprint() {
    let first = small_event(1);
    let second = small_event(2);
    let third = small_event(3);
    let first_bytes = event_accounted_bytes_v0(&first).unwrap();
    let second_bytes = event_accounted_bytes_v0(&second).unwrap();
    let third_bytes = event_accounted_bytes_v0(&third).unwrap();
    assert_eq!(first_bytes, second_bytes);
    assert_eq!(second_bytes, third_bytes);

    let exact_sum = first_bytes + second_bytes;
    let (mut sender, mut receiver) =
        bounded_event_channel_v0(ChannelLimitsV0::new(3, exact_sum).unwrap());
    sender.enqueue(first).unwrap();
    sender.enqueue(second).unwrap();
    let full_usage = sender.usage();
    assert_eq!(full_usage.current_accounted_bytes(), exact_sum);
    assert_eq!(full_usage.high_water_accounted_bytes(), exact_sum);

    let error = sender.try_enqueue(third).unwrap_err();
    assert_eq!(error.kind(), EnqueueErrorKindV0::Full);
    assert_eq!(error.usage(), full_usage);
    let third = error.into_event();
    assert_eq!(sequence(&receiver.receive().unwrap()), 1);
    assert_eq!(receiver.usage().current_accounted_bytes(), second_bytes);
    sender.try_enqueue(third).unwrap();
    assert_eq!(receiver.usage().current_accounted_bytes(), exact_sum);
    assert_eq!(sequence(&receiver.receive().unwrap()), 2);
    assert_eq!(sequence(&receiver.receive().unwrap()), 3);
    let empty = receiver.usage();
    assert_eq!(empty.current_accounted_bytes(), 0);
    assert_eq!(empty.high_water_accounted_bytes(), exact_sum);
}

#[test]
fn blocking_enqueue_completes_only_after_capacity_is_released() {
    let first = small_event(1);
    let second = small_event(2);
    let bytes = event_accounted_bytes_v0(&first).unwrap();
    let (mut sender, mut receiver) =
        bounded_event_channel_v0(ChannelLimitsV0::new(1, bytes).unwrap());
    sender.enqueue(first).unwrap();

    let (started_tx, started_rx) = std::sync::mpsc::channel();
    let (completed_tx, completed_rx) = std::sync::mpsc::channel();
    let producer = std::thread::spawn(move || {
        started_tx.send(()).unwrap();
        let result = sender.enqueue(second);
        completed_tx.send(result).unwrap();
    });

    started_rx.recv().unwrap();
    assert!(matches!(completed_rx.try_recv(), Err(TryRecvError::Empty)));
    assert_eq!(sequence(&receiver.receive().unwrap()), 1);
    let receipt = completed_rx.recv().unwrap().unwrap();
    assert_eq!(receipt.usage().current_events(), 1);
    producer.join().unwrap();
    assert_eq!(sequence(&receiver.receive().unwrap()), 2);
    assert_eq!(receiver.receive(), Err(ReceiveErrorV0::Closed));
}

#[test]
fn mixed_sizes_remain_fifo_and_ledger_high_water_is_monotonic() {
    let first = event(1, None, [("value", ScalarValueV0::null())]);
    let second = event(
        2,
        Some(b"longer-key"),
        [("value", ScalarValueV0::utf8("longer-value").unwrap())],
    );
    let third = event(
        3,
        Some(b"k"),
        [("value", ScalarValueV0::bytes(b"bytes").unwrap())],
    );
    let sizes =
        [&first, &second, &third].map(|candidate| event_accounted_bytes_v0(candidate).unwrap());
    let total = sizes.iter().sum();
    let (mut sender, mut receiver) =
        bounded_event_channel_v0(ChannelLimitsV0::new(3, total).unwrap());

    for candidate in [first, second, third] {
        sender.enqueue(candidate).unwrap();
    }
    let peak = sender.usage();
    assert_eq!(peak.current_accounted_bytes(), total);
    assert_eq!(peak.high_water_accounted_bytes(), total);
    for (expected_sequence, expected_remaining) in [(1, sizes[1] + sizes[2]), (2, sizes[2]), (3, 0)]
    {
        assert_eq!(sequence(&receiver.receive().unwrap()), expected_sequence);
        let usage = receiver.usage();
        assert_eq!(usage.current_accounted_bytes(), expected_remaining);
        assert_eq!(usage.high_water_accounted_bytes(), total);
    }
}

#[test]
fn explicit_receiver_close_rejects_new_events_and_drains_existing_fifo() {
    let (mut sender, mut receiver) = bounded_event_channel_v0(ChannelLimitsV0::maximum());
    sender.enqueue(small_event(1)).unwrap();
    receiver.close();

    let rejected = small_event(2);
    let error = sender.try_enqueue(rejected.clone()).unwrap_err();
    assert_eq!(error.kind(), EnqueueErrorKindV0::ReceiverClosed);
    assert_eq!(error.into_event(), rejected);
    assert_eq!(sequence(&receiver.receive().unwrap()), 1);
    assert_eq!(receiver.try_receive(), Err(TryReceiveErrorV0::Closed));
    assert_eq!(receiver.receive(), Err(ReceiveErrorV0::Closed));
}

#[test]
fn sender_drop_allows_fifo_drain_before_closed() {
    let (mut sender, mut receiver) = bounded_event_channel_v0(ChannelLimitsV0::maximum());
    sender.enqueue(small_event(1)).unwrap();
    sender.enqueue(small_event(2)).unwrap();
    drop(sender);

    assert_eq!(sequence(&receiver.receive().unwrap()), 1);
    assert_eq!(sequence(&receiver.receive().unwrap()), 2);
    assert_eq!(receiver.receive(), Err(ReceiveErrorV0::Closed));
}

#[test]
fn receiver_drop_wakes_blocked_sender_and_preserves_queued_events_for_recovery() {
    let first = small_event(1);
    let second = small_event(2);
    let third = small_event(3);
    let bytes = event_accounted_bytes_v0(&first).unwrap();
    let (mut sender, receiver) =
        bounded_event_channel_v0(ChannelLimitsV0::new(2, 2 * bytes).unwrap());
    sender.enqueue(first).unwrap();
    sender.enqueue(second).unwrap();

    let (started_tx, started_rx) = std::sync::mpsc::channel();
    let producer = std::thread::spawn(move || {
        started_tx.send(()).unwrap();
        let error = sender.enqueue(third).unwrap_err();
        assert_eq!(error.kind(), EnqueueErrorKindV0::ReceiverClosed);
        let returned = error.into_event();
        let mut recovered = sender.recover_undelivered().unwrap();
        assert_eq!(recovered.len(), 2);
        assert_eq!(recovered.remaining_accounted_bytes(), 2 * bytes);
        let queued_first = recovered.next().unwrap();
        assert_eq!(recovered.remaining_accounted_bytes(), bytes);
        let queued_second = recovered.next().unwrap();
        assert_eq!(recovered.remaining_accounted_bytes(), 0);
        assert_eq!(recovered.next(), None);
        assert!(matches!(
            sender.recover_undelivered(),
            Err(RecoveryErrorV0::AlreadyRecovered)
        ));
        (returned, queued_first, queued_second, sender.usage())
    });

    started_rx.recv().unwrap();
    drop(receiver);
    let (returned, queued_first, queued_second, usage) = producer.join().unwrap();
    assert_eq!(sequence(&returned), 3);
    assert_eq!(sequence(&queued_first), 1);
    assert_eq!(sequence(&queued_second), 2);
    assert_eq!(usage.current_events(), 0);
    assert_eq!(usage.current_accounted_bytes(), 0);
}

#[test]
fn diagnostics_are_content_independent_and_value_safe() {
    const FIRST: &str = "SYNTHETIC_SECRET_DO_NOT_LOG_AA";
    const SECOND: &str = "SYNTHETIC_SECRET_DO_NOT_LOG_BB";
    assert_eq!(FIRST.len(), SECOND.len());

    let first = secret_diagnostics(FIRST);
    let second = secret_diagnostics(SECOND);
    assert_eq!(first, second);
    for diagnostic in &first {
        assert_diagnostic_safe(diagnostic, &[FIRST, SECOND]);
    }
}

fn secret_event(secret: &str) -> EventEnvelopeV0 {
    event(
        7,
        Some(secret.as_bytes()),
        [("secret", ScalarValueV0::utf8(secret).unwrap())],
    )
}

fn secret_diagnostics(secret: &str) -> Vec<String> {
    let candidate = secret_event(secret);
    let footprint = event_accounted_bytes_v0(&candidate).unwrap();

    let (mut full_sender, _full_receiver) =
        bounded_event_channel_v0(ChannelLimitsV0::new(1, MAX_CHANNEL_ACCOUNTED_BYTES_V0).unwrap());
    full_sender.enqueue(small_event(1)).unwrap();
    let full = full_sender.try_enqueue(candidate.clone()).unwrap_err();

    let (mut large_sender, _large_receiver) =
        bounded_event_channel_v0(ChannelLimitsV0::new(1, footprint - 1).unwrap());
    let too_large = large_sender.try_enqueue(candidate.clone()).unwrap_err();

    let (mut closed_sender, closed_receiver) = bounded_event_channel_v0(ChannelLimitsV0::maximum());
    drop(closed_receiver);
    let closed = closed_sender.try_enqueue(candidate).unwrap_err();

    let (_empty_sender, mut empty_receiver) = bounded_event_channel_v0(ChannelLimitsV0::maximum());
    let empty = empty_receiver.try_receive().unwrap_err();

    let (mut recovery_sender, _recovery_receiver) =
        bounded_event_channel_v0(ChannelLimitsV0::maximum());
    let recovery = recovery_sender.recover_undelivered().unwrap_err();

    vec![
        format!("{full:?}"),
        full.to_string(),
        format!("{too_large:?}"),
        too_large.to_string(),
        format!("{closed:?}"),
        closed.to_string(),
        format!("{empty:?}"),
        empty.to_string(),
        format!("{recovery:?}"),
        recovery.to_string(),
    ]
}

fn assert_diagnostic_safe(diagnostic: &str, secrets: &[&str]) {
    for secret in secrets {
        let escaped: String = secret.chars().flat_map(char::escape_default).collect();
        let decimal_comma = secret
            .as_bytes()
            .iter()
            .map(u8::to_string)
            .collect::<Vec<_>>()
            .join(",");
        let decimal_debug = format!("{:?}", secret.as_bytes());
        let hex_lower = hex(secret.as_bytes(), false);
        let hex_upper = hex(secret.as_bytes(), true);
        let base64 = base64(secret.as_bytes());
        let prefix = &secret[..secret.len().min(8)];

        for recoverable in [
            *secret,
            escaped.as_str(),
            decimal_comma.as_str(),
            decimal_debug.as_str(),
            hex_lower.as_str(),
            hex_upper.as_str(),
            base64.as_str(),
            prefix,
        ] {
            assert!(
                !diagnostic.contains(recoverable),
                "diagnostic contained a recoverable fixture representation"
            );
        }
    }
}

fn hex(bytes: &[u8], uppercase: bool) -> String {
    let mut encoded = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        if uppercase {
            write!(&mut encoded, "{byte:02X}").unwrap();
        } else {
            write!(&mut encoded, "{byte:02x}").unwrap();
        }
    }
    encoded
}

fn base64(bytes: &[u8]) -> String {
    const ALPHABET: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut encoded = String::with_capacity(bytes.len().div_ceil(3) * 4);

    for chunk in bytes.chunks(3) {
        let first = chunk[0];
        let second = chunk.get(1).copied().unwrap_or(0);
        let third = chunk.get(2).copied().unwrap_or(0);
        let combined = (u32::from(first) << 16) | (u32::from(second) << 8) | u32::from(third);

        encoded.push(char::from(ALPHABET[((combined >> 18) & 0x3f) as usize]));
        encoded.push(char::from(ALPHABET[((combined >> 12) & 0x3f) as usize]));
        if chunk.len() > 1 {
            encoded.push(char::from(ALPHABET[((combined >> 6) & 0x3f) as usize]));
        } else {
            encoded.push('=');
        }
        if chunk.len() > 2 {
            encoded.push(char::from(ALPHABET[(combined & 0x3f) as usize]));
        } else {
            encoded.push('=');
        }
    }

    encoded
}
