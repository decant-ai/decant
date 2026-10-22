#[path = "support/sre_v1.rs"]
mod support;

use decant_state::sre_evidence::{EvidenceErrorV1, RecipeV1, reduce_sre_evidence_v1};
use support::{empty, event, fixture};

#[test]
fn independent_before_after_oracle_and_duplicate_conflict() {
    let mut state = fixture();
    let reduced = reduce_sre_evidence_v1(&state, RecipeV1::default()).unwrap();
    assert_eq!(reduced.anchor_ns, Some(600_000_000_001));
    let row = &reduced.services[0];
    assert_eq!(
        (row.baseline.count, row.baseline.errors, row.baseline.p95_ns),
        (2, 1, Some(20))
    );
    assert_eq!(
        (row.current.count, row.current.errors, row.current.p95_ns),
        (2, 1, Some(200))
    );
    assert_eq!(row.current.duration_sum_ns, 300);
    assert_eq!(row.current.unset_status, 1);
    assert_eq!(row.baseline.last_version, Some("v1"));
    assert_eq!(row.current.last_version, Some("v2"));
    state
        .apply_event_v1(&event(5, 4, 600, 200, 2, "checkout", "v2"))
        .unwrap();
    let reduced = reduce_sre_evidence_v1(&state, RecipeV1::default()).unwrap();
    assert_eq!(reduced.duplicates, 1);
    assert_eq!(reduced.services[0].current.count, 2);
    state
        .apply_event_v1(&event(6, 4, 600, 201, 2, "checkout", "v2"))
        .unwrap();
    assert!(matches!(
        reduce_sre_evidence_v1(&state, RecipeV1::default()),
        Err(EvidenceErrorV1::ConflictingSpan)
    ));
}

#[test]
fn p95_uses_nearest_rank_not_interpolation_and_stable_representative() {
    for size in 1..=101 {
        let mut state = empty();
        for pos in (1..=size).rev() {
            state
                .apply_event_v1(&event(pos, pos, 1, pos, 1, "service", "v1"))
                .unwrap();
        }
        let reduced = reduce_sre_evidence_v1(&state, RecipeV1::default()).unwrap();
        assert_eq!(
            reduced.services[0].current.p95_ns,
            Some((95 * size).div_ceil(100))
        );
        assert_eq!(reduced.services[0].current.count, size);
    }
}

#[test]
fn capacity_empty_retention_and_boundary_windows() {
    assert!(
        reduce_sre_evidence_v1(&empty(), RecipeV1::default())
            .unwrap()
            .services
            .is_empty()
    );
    let mut state = empty();
    // End is 601s + 1ns: the 301s event is still baseline, not current.
    for (pos, time, name) in [(1, 1, "a"), (2, 301, "a"), (3, 601, "b")] {
        state
            .apply_event_v1(&event(pos, pos, time, 1, 1, name, "v1"))
            .unwrap();
    }
    let r = reduce_sre_evidence_v1(&state, RecipeV1::default()).unwrap();
    assert_eq!(r.expired, 1);
    assert_eq!(r.services[0].baseline.count, 1);
    assert_eq!(r.services[0].current.count, 0);
    assert!(matches!(
        reduce_sre_evidence_v1(
            &state,
            RecipeV1 {
                max_services: 1,
                ..RecipeV1::default()
            }
        ),
        Err(EvidenceErrorV1::Capacity)
    ));
}

#[test]
fn missing_information_non_server_and_non_server_conflict_are_explicit() {
    use decant_ir::{CanonicalPayloadV0, EventEnvelopeV0, ScalarValueV0};
    use std::collections::BTreeMap;
    let make = |position, duration, kind, service: bool| {
        let input = event(position, 1, 1, duration, 0, "service", "v1");
        let mut fields: BTreeMap<_, _> = input
            .payload()
            .fields()
            .map(|(k, v)| (k.to_owned(), v.clone()))
            .collect();
        fields.remove("service.version");
        if !service {
            fields.remove("service.name");
        }
        fields.insert("span.kind".into(), ScalarValueV0::from_u64(kind));
        EventEnvelopeV0::new(
            0,
            input.source_id().clone(),
            input.schema_id().clone(),
            input.source_position(),
            None,
            input.event_time_ns(),
            0,
            input.sensitivity(),
            CanonicalPayloadV0::try_from_fields(fields).unwrap(),
        )
        .unwrap()
    };
    let mut state = empty();
    state.apply_event_v1(&make(1, 1, 2, true)).unwrap();
    let r = reduce_sre_evidence_v1(&state, RecipeV1::default()).unwrap();
    assert_eq!(r.services[0].current.missing_versions, 1);
    assert_eq!(r.services[0].current.last_version, None);
    let mut state = empty();
    state.apply_event_v1(&make(1, 1, 2, false)).unwrap();
    assert_eq!(
        reduce_sre_evidence_v1(&state, RecipeV1::default())
            .unwrap()
            .missing_service,
        1
    );
    let mut state = empty();
    state.apply_event_v1(&make(1, 1, 1, true)).unwrap();
    assert_eq!(
        reduce_sre_evidence_v1(&state, RecipeV1::default())
            .unwrap()
            .non_server,
        1
    );
    state.apply_event_v1(&make(2, 2, 1, true)).unwrap();
    assert!(matches!(
        reduce_sre_evidence_v1(&state, RecipeV1::default()),
        Err(EvidenceErrorV1::ConflictingSpan)
    ));
}
