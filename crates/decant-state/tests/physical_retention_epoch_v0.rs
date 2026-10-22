use std::collections::BTreeMap;

use decant_ir::{
    CanonicalPayloadV0, EventEnvelopeV0, KafkaPositionV0, SchemaIdV0, SensitivityBitmapV0,
    SourceIdV0, SourcePositionKindV0, SourcePositionV0,
};
use decant_state::{
    ADMISSION_VERSION_V0, AdmissionConfigV0, AdmissionEpochRolloverErrorV0, AdmissionLimitsV0,
    AdmissionPhysicalRetentionErrorV0, AdmissionStateV0, ApplyOutcomeV0, FinalizeOutcomeV0,
    RejectionReasonV0, encode_admission_snapshot_v1, restore_admission_snapshot_v1,
};

const SECOND: i64 = 1_000_000_000;

fn source() -> SourceIdV0 {
    SourceIdV0::new("private-retention-source").unwrap()
}

fn limits() -> AdmissionLimitsV0 {
    AdmissionLimitsV0::new(32, 64 * 1_024).unwrap()
}

fn config(start: i64, end: i64) -> AdmissionConfigV0 {
    AdmissionConfigV0::new_with_source_position_kind(
        ADMISSION_VERSION_V0,
        source(),
        SourcePositionKindV0::Kafka,
        start,
        end,
        limits(),
    )
    .unwrap()
}

fn event(offset: i64, time: i64) -> EventEnvelopeV0 {
    EventEnvelopeV0::new(
        0,
        source(),
        SchemaIdV0::new("private-retention-schema").unwrap(),
        SourcePositionV0::Kafka {
            position: KafkaPositionV0::new(0, offset).unwrap(),
        },
        Some(format!("private-key-{offset}").as_bytes()),
        time,
        99,
        SensitivityBitmapV0::from_bits(u64::MAX),
        CanonicalPayloadV0::try_from_fields(BTreeMap::new()).unwrap(),
    )
    .unwrap()
}

fn populated(order: [usize; 3]) -> AdmissionStateV0 {
    let events = [
        event(0, 10 * SECOND),
        event(1, 50 * SECOND),
        event(2, 80 * SECOND),
    ];
    let mut state = AdmissionStateV0::new(config(0, 200 * SECOND));
    for index in order {
        assert_eq!(
            state.apply_event_v1(&events[index]),
            Ok(ApplyOutcomeV0::Accepted)
        );
    }
    assert_eq!(
        state.apply_event_v1(&event(3, -1)),
        Ok(ApplyOutcomeV0::Rejected(RejectionReasonV0::OutsideEpoch))
    );
    state
}

#[test]
fn exact_cutoff_releases_only_older_events_and_round_trips_canonically() {
    let state = populated([0, 1, 2]);
    assert_eq!(state.watermark_ns(), Some(50 * SECOND));
    let before = encode_admission_snapshot_v1(&state).unwrap();
    let before_usage = state.usage();

    let retained = state
        .physically_retain_from_event_time_v0(50 * SECOND)
        .unwrap();
    assert_eq!(retained.cutoff_event_time_ns(), 50 * SECOND);
    assert_eq!(retained.removed_identities(), 1);
    assert!(retained.removed_event_bytes() > 0);
    assert_eq!(
        retained.removed_identities() + retained.retained_state().usage().accepted_identities(),
        before_usage.accepted_identities()
    );
    assert_eq!(
        retained.removed_event_bytes() + retained.retained_state().usage().accepted_event_bytes(),
        before_usage.accepted_event_bytes()
    );
    assert_eq!(
        retained
            .retained_state()
            .accepted_events()
            .map(EventEnvelopeV0::event_time_ns)
            .collect::<Vec<_>>(),
        vec![50 * SECOND, 80 * SECOND]
    );
    assert_eq!(retained.retained_state().watermark_ns(), Some(50 * SECOND));
    assert_eq!(
        retained
            .retained_state()
            .rejection_summary()
            .range(RejectionReasonV0::OutsideEpoch)
            .count(),
        1
    );

    let encoded = encode_admission_snapshot_v1(retained.retained_state()).unwrap();
    let restored =
        restore_admission_snapshot_v1(&encoded, retained.retained_state().config()).unwrap();
    assert_eq!(encode_admission_snapshot_v1(&restored).unwrap(), encoded);
    assert_eq!(encode_admission_snapshot_v1(&state).unwrap(), before);
    let debug = format!("{retained:?}");
    for private in ["private-retention", &(50 * SECOND).to_string()] {
        assert!(!debug.contains(private));
    }
}

#[test]
fn permitted_arrival_orders_produce_identical_retained_bytes_and_receipts() {
    let left = populated([0, 1, 2])
        .physically_retain_from_event_time_v0(50 * SECOND)
        .unwrap();
    let right = populated([0, 2, 1])
        .physically_retain_from_event_time_v0(50 * SECOND)
        .unwrap();
    assert_eq!(left.removed_identities(), right.removed_identities());
    assert_eq!(left.removed_event_bytes(), right.removed_event_bytes());
    assert_eq!(
        encode_admission_snapshot_v1(left.retained_state()).unwrap(),
        encode_admission_snapshot_v1(right.retained_state()).unwrap()
    );
}

#[test]
fn invalid_retention_requests_are_non_mutating_and_value_safe() {
    let state = populated([0, 1, 2]);
    let before = encode_admission_snapshot_v1(&state).unwrap();
    for (cutoff, expected) in [
        (-1, AdmissionPhysicalRetentionErrorV0::CutoffOutsideEpoch),
        (
            50 * SECOND + 1,
            AdmissionPhysicalRetentionErrorV0::CutoffAfterWatermark,
        ),
        (
            200 * SECOND + 1,
            AdmissionPhysicalRetentionErrorV0::CutoffOutsideEpoch,
        ),
    ] {
        let error = state
            .physically_retain_from_event_time_v0(cutoff)
            .unwrap_err();
        assert_eq!(error, expected);
        assert!(!format!("{error:?}").contains(&cutoff.to_string()));
    }
    assert_eq!(encode_admission_snapshot_v1(&state).unwrap(), before);

    let empty = AdmissionStateV0::new(config(0, 100));
    assert_eq!(
        empty.physically_retain_from_event_time_v0(0).unwrap_err(),
        AdmissionPhysicalRetentionErrorV0::MissingWatermark
    );
    let synthetic =
        AdmissionStateV0::new(AdmissionConfigV0::new(0, source(), 0, 100, limits()).unwrap());
    assert_eq!(
        synthetic
            .physically_retain_from_event_time_v0(0)
            .unwrap_err(),
        AdmissionPhysicalRetentionErrorV0::NonKafkaSource {
            actual: SourcePositionKindV0::Synthetic
        }
    );
}

#[test]
fn finalized_epoch_can_release_all_events_and_roll_only_to_exact_adjacent_config() {
    let mut current = populated([0, 1, 2]);
    assert_eq!(
        current.finalize_source(&source(), 200 * SECOND),
        FinalizeOutcomeV0::Finalized
    );
    let empty_finalized = current
        .physically_retain_from_event_time_v0(200 * SECOND)
        .unwrap();
    assert_eq!(empty_finalized.removed_identities(), 3);
    assert_eq!(
        empty_finalized
            .retained_state()
            .usage()
            .accepted_identities(),
        0
    );
    assert!(empty_finalized.retained_state().is_finalized());
    encode_admission_snapshot_v1(empty_finalized.retained_state()).unwrap();

    let next_config = config(200 * SECOND, 300 * SECOND);
    let next = current
        .begin_adjacent_kafka_epoch_v0(next_config.clone())
        .unwrap();
    assert_eq!(next.config(), &next_config);
    assert_eq!(next.usage().accepted_identities(), 0);
    assert_eq!(next.watermark_ns(), None);
    assert!(!next.is_finalized());

    let open = populated([0, 1, 2]);
    assert_eq!(
        open.begin_adjacent_kafka_epoch_v0(next_config.clone())
            .unwrap_err(),
        AdmissionEpochRolloverErrorV0::CurrentEpochNotFinalized
    );
    let wrong_source = AdmissionConfigV0::new_with_source_position_kind(
        0,
        SourceIdV0::new("private-other-source").unwrap(),
        SourcePositionKindV0::Kafka,
        200 * SECOND,
        300 * SECOND,
        limits(),
    )
    .unwrap();
    assert_eq!(
        current
            .begin_adjacent_kafka_epoch_v0(wrong_source)
            .unwrap_err(),
        AdmissionEpochRolloverErrorV0::SourceIdentityChanged
    );
    assert_eq!(
        current
            .begin_adjacent_kafka_epoch_v0(config(200 * SECOND + 1, 300 * SECOND))
            .unwrap_err(),
        AdmissionEpochRolloverErrorV0::EpochsNotAdjacent
    );
    let wrong_kind =
        AdmissionConfigV0::new(0, source(), 200 * SECOND, 300 * SECOND, limits()).unwrap();
    assert_eq!(
        current
            .begin_adjacent_kafka_epoch_v0(wrong_kind)
            .unwrap_err(),
        AdmissionEpochRolloverErrorV0::SourcePositionKindChanged
    );
    let wrong_limits = AdmissionConfigV0::new_with_source_position_kind(
        0,
        source(),
        SourcePositionKindV0::Kafka,
        200 * SECOND,
        300 * SECOND,
        AdmissionLimitsV0::new(31, 64 * 1_024).unwrap(),
    )
    .unwrap();
    assert_eq!(
        current
            .begin_adjacent_kafka_epoch_v0(wrong_limits)
            .unwrap_err(),
        AdmissionEpochRolloverErrorV0::AdmissionLimitsChanged
    );

    let mut synthetic =
        AdmissionStateV0::new(AdmissionConfigV0::new(0, source(), 0, 100, limits()).unwrap());
    assert_eq!(
        synthetic.finalize_source(&source(), 100),
        FinalizeOutcomeV0::Finalized
    );
    assert_eq!(
        synthetic
            .begin_adjacent_kafka_epoch_v0(
                AdmissionConfigV0::new(0, source(), 100, 200, limits()).unwrap()
            )
            .unwrap_err(),
        AdmissionEpochRolloverErrorV0::NonKafkaSource
    );
}
