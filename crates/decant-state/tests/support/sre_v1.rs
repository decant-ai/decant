use decant_ir::{
    CanonicalPayloadV0, EventEnvelopeV0, ScalarValueV0, SchemaIdV0, SensitivityBitmapV0,
    SourceIdV0, SourcePositionV0,
};
use decant_state::{AdmissionConfigV0, AdmissionLimitsV0, AdmissionStateV0};
use std::collections::BTreeMap;

pub fn event(
    position: u64,
    identity: u64,
    seconds: i64,
    duration: u64,
    status: u64,
    service: &str,
    version: &str,
) -> EventEnvelopeV0 {
    let fields = BTreeMap::from([
        (
            "trace.id".into(),
            ScalarValueV0::utf8("00000000000000000000000000000001").unwrap(),
        ),
        (
            "span.id".into(),
            ScalarValueV0::utf8(&format!("{identity:016x}")).unwrap(),
        ),
        ("span.kind".into(), ScalarValueV0::from_u64(2)),
        ("service.name".into(), ScalarValueV0::utf8(service).unwrap()),
        (
            "service.version".into(),
            ScalarValueV0::utf8(version).unwrap(),
        ),
        ("duration.ns".into(), ScalarValueV0::from_u64(duration)),
        ("status.code".into(), ScalarValueV0::from_u64(status)),
        (
            "private.raw".into(),
            ScalarValueV0::utf8("SYNTHETIC-RAW-NEVER-EMIT").unwrap(),
        ),
    ]);
    EventEnvelopeV0::new(
        0,
        SourceIdV0::new("synthetic-evidence").unwrap(),
        SchemaIdV0::new("sre-span-v1").unwrap(),
        SourcePositionV0::Synthetic { sequence: position },
        None,
        seconds * 1_000_000_000,
        0,
        SensitivityBitmapV0::from_bits(u64::MAX),
        CanonicalPayloadV0::try_from_fields(fields).unwrap(),
    )
    .unwrap()
}

pub fn empty() -> AdmissionStateV0 {
    AdmissionStateV0::new(
        AdmissionConfigV0::new(
            0,
            SourceIdV0::new("synthetic-evidence").unwrap(),
            0,
            2_000_000_000_000,
            AdmissionLimitsV0::maximum(),
        )
        .unwrap(),
    )
}

pub fn fixture() -> AdmissionStateV0 {
    let mut state = empty();
    for (pos, time, duration, status, version) in [
        (1, 100, 10, 1, "v1"),
        (2, 200, 20, 2, "v1"),
        (3, 400, 100, 0, "v2"),
        (4, 600, 200, 2, "v2"),
    ] {
        assert_eq!(
            state
                .apply_event_v1(&event(
                    pos, pos, time, duration, status, "checkout", version
                ))
                .unwrap(),
            decant_state::ApplyOutcomeV0::Accepted
        );
    }
    state
}
