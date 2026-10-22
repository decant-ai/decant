use std::collections::{BTreeMap, BTreeSet};

use decant_fixtures::{
    GENERATOR_VERSION_V0, GeneratorErrorV0, SRE_V0_DATA_DELIVERIES, SRE_V0_UNIQUE_EVENTS,
    ScenarioManifestV0, SreGeneratorV0, decode_fixture_record_v0, decode_scenario_manifest_v0,
    encode_fixture_record_v0,
};
use decant_ir::{EventEnvelopeV0, ScalarValueV0};

const SCENARIO_BYTES: &[u8] = include_bytes!("../contracts/sre-v0/scenario.json");
const START_NS: i64 = 1_767_225_600_000_000_000;
const END_NS: i64 = 1_767_226_800_000_000_000;
const CHANGE_NS: i64 = START_NS + 600_000_000_000;
const DUPLICATE_DOMAIN: u64 = 4_923_930_056_176_386_388;
const REORDER_DOMAIN: u64 = 5_928_231_699_254_432_339;

#[derive(Default)]
struct RateGroup {
    total: u64,
    errors: u64,
    latencies: Vec<u64>,
}

#[derive(Default)]
struct StreamSummary {
    data_records: usize,
    terminal_controls: usize,
    unique_sequences: BTreeSet<u64>,
    duplicate_sequences: BTreeSet<u64>,
    delayed_sequences: BTreeSet<u64>,
    class_counts: [usize; 5],
    groups: BTreeMap<(String, String, bool), RateGroup>,
    notable_deployments: Vec<(String, String, String, i64, bool)>,
    maximum_pending_records: usize,
}

fn manifest() -> ScenarioManifestV0 {
    decode_scenario_manifest_v0(SCENARIO_BYTES).unwrap()
}

fn field<'a>(event: &'a EventEnvelopeV0, name: &str) -> &'a ScalarValueV0 {
    event
        .payload()
        .fields()
        .find_map(|(candidate, value)| (candidate == name).then_some(value))
        .unwrap()
}

fn text_field<'a>(event: &'a EventEnvelopeV0, name: &str) -> &'a str {
    field(event, name).as_utf8().unwrap()
}

fn analyze(scenario_id: &str, verify_canonical: bool) -> StreamSummary {
    let manifest = manifest();
    let mut generator = SreGeneratorV0::new(GENERATOR_VERSION_V0, &manifest, scenario_id).unwrap();
    let usage = generator.usage();
    assert_eq!(usage.schedule_entries(), SRE_V0_UNIQUE_EVENTS);
    assert_eq!(usage.duplicate_sequences(), 1_000);
    assert_eq!(usage.delayed_sequences(), 2_000);
    assert_eq!(usage.deployment_definitions(), 20);
    assert_eq!(usage.pending_records(), 0);
    assert_eq!(generator.size_hint(), (0, Some(100_001)));

    let mut summary = StreamSummary::default();
    let mut previous_event: Option<(u64, Vec<u8>)> = None;
    let mut previous_unique_order: Option<(i64, u64)> = None;
    let mut saw_terminal = false;

    while let Some(record) = generator.next() {
        let record = record.unwrap();
        let encoded = encode_fixture_record_v0(&record).unwrap();
        if verify_canonical {
            let decoded = decode_fixture_record_v0(&encoded).unwrap();
            assert_eq!(encode_fixture_record_v0(&decoded).unwrap(), encoded);
        }
        summary.maximum_pending_records = summary
            .maximum_pending_records
            .max(generator.usage().pending_records());

        match record {
            decant_fixtures::FixtureRecordV0::Event(event) => {
                assert!(!saw_terminal);
                summary.data_records += 1;
                assert_eq!(event.source_id().as_str(), "sre-v0");
                assert_eq!(event.sensitivity().bits(), 0);
                assert!(event.key().is_some());
                let sequence = event.source_position().synthetic_sequence();
                let inserted = summary.unique_sequences.insert(sequence);
                if !inserted {
                    summary.duplicate_sequences.insert(sequence);
                    let (previous_sequence, previous_bytes) = previous_event.as_ref().unwrap();
                    assert_eq!(*previous_sequence, sequence);
                    assert_eq!(*previous_bytes, encoded);
                } else {
                    let order = (event.ingest_time_ns(), sequence);
                    assert!(previous_unique_order.is_none_or(|previous| previous <= order));
                    previous_unique_order = Some(order);
                    let class_index = match sequence {
                        0..60_000 => {
                            assert_eq!(event.schema_id().as_str(), "payment-outcome-v0");
                            0
                        }
                        60_000..98_250 => {
                            assert_eq!(event.schema_id().as_str(), "distractor-request-v0");
                            1
                        }
                        98_250..98_500 => {
                            assert_eq!(event.schema_id().as_str(), "distractor-request-v0");
                            2
                        }
                        98_500..98_980 => {
                            assert_eq!(event.schema_id().as_str(), "metric-sample-v0");
                            3
                        }
                        98_980..99_000 => {
                            assert_eq!(event.schema_id().as_str(), "deployment-v0");
                            4
                        }
                        _ => panic!("source sequence outside frozen range"),
                    };
                    summary.class_counts[class_index] += 1;

                    if (98_250..98_500).contains(&sequence) {
                        assert!(
                            (START_NS..START_NS + 300_000_000_000).contains(&event.event_time_ns())
                        );
                        assert!(
                            (START_NS + 900_000_000_000..END_NS).contains(&event.ingest_time_ns())
                        );
                    } else {
                        let delay = event.ingest_time_ns() - event.event_time_ns();
                        assert!(delay >= 0);
                        if delay > 0 {
                            assert!((1_000_000_000..=25_000_000_000).contains(&delay));
                            summary.delayed_sequences.insert(sequence);
                        }
                    }

                    if sequence < 60_000 {
                        let service = text_field(&event, "service_id").to_owned();
                        let region = text_field(&event, "region_id").to_owned();
                        let post_change = event.event_time_ns() >= CHANGE_NS;
                        let group = summary
                            .groups
                            .entry((service, region, post_change))
                            .or_default();
                        group.total += 1;
                        group.errors += u64::from(text_field(&event, "outcome") == "error");
                        group
                            .latencies
                            .push(field(&event, "latency_ms").as_u64().unwrap());
                    }

                    if sequence >= 98_980
                        && field(&event, "notable_temporal_evidence")
                            .as_bool()
                            .unwrap()
                    {
                        summary.notable_deployments.push((
                            text_field(&event, "deployment_id").to_owned(),
                            text_field(&event, "service_id").to_owned(),
                            text_field(&event, "region_id").to_owned(),
                            event.event_time_ns(),
                            field(&event, "causal_claim").as_bool().unwrap(),
                        ));
                    }
                }
                previous_event = Some((sequence, encoded));
            }
            decant_fixtures::FixtureRecordV0::EndOfInput(control) => {
                assert!(!saw_terminal);
                saw_terminal = true;
                summary.terminal_controls += 1;
                assert_eq!(control.source_id().as_str(), "sre-v0");
                assert_eq!(control.finalized_through_ns(), END_NS);
            }
        }
    }

    assert!(saw_terminal);
    assert_eq!(generator.next(), None);
    assert_eq!(generator.emitted_data_records(), SRE_V0_DATA_DELIVERIES);
    assert_eq!(generator.size_hint(), (0, Some(0)));
    summary
}

fn ppm(group: &RateGroup) -> u64 {
    group.errors * 1_000_000 / group.total
}

fn nearest_rank(group: &RateGroup, basis_points: usize) -> u64 {
    let mut values = group.latencies.clone();
    values.sort_unstable();
    let rank = (basis_points * values.len()).div_ceil(10_000);
    values[rank - 1]
}

fn group<'a>(summary: &'a StreamSummary, service: &str, region: &str, post: bool) -> &'a RateGroup {
    summary
        .groups
        .get(&(service.to_owned(), region.to_owned(), post))
        .unwrap()
}

fn independent_mix(mut value: u64) -> u64 {
    value = value.wrapping_add(0x9E37_79B9_7F4A_7C15);
    value = (value ^ (value >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    value = (value ^ (value >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    value ^ (value >> 31)
}

fn accepted_sequences() -> impl Iterator<Item = u64> {
    (0..98_250).chain(98_500..99_000)
}

fn independent_top_k(domain: u64, count: usize) -> BTreeSet<u64> {
    let mut candidates = accepted_sequences()
        .map(|sequence| (independent_mix(42 ^ domain ^ sequence), sequence))
        .collect::<Vec<_>>();
    candidates.sort_unstable();
    assert!(candidates.windows(2).all(|window| window[0] <= window[1]));
    candidates
        .into_iter()
        .take(count)
        .map(|(_, sequence)| sequence)
        .collect()
}

#[test]
fn primary_stream_matches_counts_order_selection_and_canonical_contracts() {
    let summary = analyze("sre-v0-primary", true);
    assert_eq!(summary.data_records, 100_000);
    assert_eq!(summary.terminal_controls, 1);
    assert_eq!(summary.unique_sequences.len(), 99_000);
    assert_eq!(summary.duplicate_sequences.len(), 1_000);
    assert_eq!(summary.delayed_sequences.len(), 2_000);
    assert_eq!(summary.class_counts, [60_000, 38_250, 250, 480, 20]);
    assert_eq!(summary.maximum_pending_records, 1);
    assert_eq!(
        summary.duplicate_sequences,
        independent_top_k(DUPLICATE_DOMAIN, 1_000)
    );
    assert_eq!(
        summary.delayed_sequences,
        independent_top_k(REORDER_DOMAIN, 2_000)
    );
}

#[test]
fn primary_rates_percentiles_and_temporal_deployment_match_independent_calculation() {
    let summary = analyze("sre-v0-primary", false);
    let pre_a = group(&summary, "payments-v2", "region-a", false);
    let pre_b = group(&summary, "payments-v2", "region-b", false);
    let post_a = group(&summary, "payments-v2", "region-a", true);
    let post_b = group(&summary, "payments-v2", "region-b", true);
    assert_eq!((ppm(pre_a), ppm(pre_b)), (4_000, 4_000));
    assert_eq!((ppm(post_a), ppm(post_b)), (4_000, 32_000));
    assert_eq!(
        (post_a.errors + post_b.errors) * 1_000_000 / (post_a.total + post_b.total),
        18_000
    );
    assert_eq!(
        (
            nearest_rank(pre_a, 9_900),
            nearest_rank(pre_b, 9_900),
            nearest_rank(post_a, 9_900),
            nearest_rank(post_b, 9_900),
        ),
        (180, 180, 180, 720)
    );
    assert_eq!(
        summary.notable_deployments,
        vec![(
            "payments-v2-region-b-09".to_owned(),
            "payments-v2".to_owned(),
            "region-b".to_owned(),
            START_NS + 540_000_000_000,
            false,
        )]
    );
}

#[test]
fn holdouts_move_effects_and_preserve_cardinality() {
    let region_swap = analyze("sre-v0-region-swap", false);
    assert_eq!(
        ppm(group(&region_swap, "payments-v2", "region-a", true)),
        32_000
    );
    assert_eq!(
        nearest_rank(group(&region_swap, "payments-v2", "region-a", true), 9_900),
        720
    );
    assert_eq!(
        region_swap.notable_deployments[0].0,
        "payments-v2-region-a-09"
    );
    assert_eq!(region_swap.notable_deployments[0].1, "payments-v2");
    assert_eq!(region_swap.notable_deployments[0].2, "region-a");

    let service_swap = analyze("sre-v0-service-swap", false);
    assert_eq!(
        ppm(group(&service_swap, "checkout-v1", "region-b", true)),
        32_000
    );
    assert_eq!(
        service_swap.notable_deployments[0].0,
        "checkout-v1-region-b-09"
    );
    assert_eq!(service_swap.notable_deployments[0].1, "checkout-v1");
    assert_eq!(service_swap.notable_deployments[0].2, "region-b");

    let no_regression = analyze("sre-v0-no-regression", false);
    let post_b = group(&no_regression, "payments-v2", "region-b", true);
    assert_eq!(ppm(post_b), 4_000);
    assert_eq!(nearest_rank(post_b, 9_900), 180);
    assert_eq!(no_regression.notable_deployments[0].1, "payments-v2");
    assert_eq!(no_regression.notable_deployments[0].2, "region-b");

    for summary in [region_swap, service_swap, no_regression] {
        assert_eq!(summary.data_records, 100_000);
        assert_eq!(summary.unique_sequences.len(), 99_000);
        assert_eq!(summary.duplicate_sequences.len(), 1_000);
        assert_eq!(summary.delayed_sequences.len(), 2_000);
        assert_eq!(summary.terminal_controls, 1);
    }
}

#[test]
fn every_scenario_is_byte_identical_across_independent_generators() {
    let manifest = manifest();
    for scenario_id in [
        "sre-v0-primary",
        "sre-v0-region-swap",
        "sre-v0-service-swap",
        "sre-v0-no-regression",
    ] {
        let left = SreGeneratorV0::new(GENERATOR_VERSION_V0, &manifest, scenario_id).unwrap();
        let right = SreGeneratorV0::new(GENERATOR_VERSION_V0, &manifest, scenario_id).unwrap();
        let mut compared = 0;
        for (left, right) in left.zip(right) {
            let left = encode_fixture_record_v0(&left.unwrap()).unwrap();
            let right = encode_fixture_record_v0(&right.unwrap()).unwrap();
            assert_eq!(left, right);
            let decoded = decode_fixture_record_v0(&left).unwrap();
            assert_eq!(encode_fixture_record_v0(&decoded).unwrap(), left);
            compared += 1;
        }
        assert_eq!(compared, 100_001);
    }
}

#[test]
fn version_and_unknown_scenario_errors_are_typed_and_value_safe() {
    let manifest = manifest();
    assert!(matches!(
        SreGeneratorV0::new(1, &manifest, "sre-v0-primary"),
        Err(GeneratorErrorV0::UnsupportedGeneratorVersion { actual: 1 })
    ));

    let first = SreGeneratorV0::new(GENERATOR_VERSION_V0, &manifest, "private-a").unwrap_err();
    let second = SreGeneratorV0::new(GENERATOR_VERSION_V0, &manifest, "private-b").unwrap_err();
    assert_eq!(format!("{first:?}"), format!("{second:?}"));
    assert_eq!(first.to_string(), second.to_string());
    for diagnostic in [format!("{first:?}"), first.to_string()] {
        assert!(!diagnostic.contains("private"));
        assert!(!diagnostic.contains("sre-v0"));
    }
}
