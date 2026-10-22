use std::any::TypeId;

use decant_fixtures::{
    BaselineManifestV0, ContractErrorV0, MAX_FIXTURE_DOCUMENT_BYTES, OracleManifestV0,
    decode_baseline_manifest_v0, decode_oracle_manifest_v0, decode_scenario_manifest_v0,
    decode_scoring_manifest_v0, validate_sre_contract_links_v0,
};

const SCENARIO: &[u8] = include_bytes!("../contracts/sre-v0/scenario.json");
const ORACLE: &[u8] = include_bytes!("../contracts/sre-v0/oracle.json");
const BASELINES: &[u8] = include_bytes!("../contracts/sre-v0/baselines.json");
const SCORING: &[u8] = include_bytes!("../contracts/sre-v0/scoring.json");

#[test]
fn committed_sre_v0_contracts_decode_and_cross_validate() {
    let scenario = decode_scenario_manifest_v0(SCENARIO).unwrap();
    let oracle = decode_oracle_manifest_v0(ORACLE).unwrap();
    let baselines = decode_baseline_manifest_v0(BASELINES).unwrap();
    let scoring = decode_scoring_manifest_v0(SCORING).unwrap();

    assert_eq!(scenario.scenario_count(), 4);
    assert_eq!(scenario.holdout_count(), 3);
    assert_eq!(scenario.primary_query_count(), 2);
    assert_eq!(scenario.mixer_vectors().len(), 10);
    assert_eq!(oracle.scenario_count(), 4);
    assert_eq!(baselines.enabled_count(), 3);
    assert!(scoring.causal_claim_is_hard_failure());

    validate_sre_contract_links_v0(&scenario, &oracle).unwrap();
}

#[test]
fn primary_workload_arithmetic_reconciles_without_terminal_control() {
    let counts = decode_scenario_manifest_v0(SCENARIO)
        .unwrap()
        .primary_counts()
        .unwrap();
    assert_eq!(counts.unique_events(), 99_000);
    assert_eq!(counts.exact_duplicate_deliveries(), 1_000);
    assert_eq!(counts.data_deliveries(), 100_000);
    assert_eq!(counts.accepted_unique_events(), 98_750);
    assert_eq!(counts.terminal_controls(), 1);
    assert_eq!(
        counts.unique_events() + counts.exact_duplicate_deliveries(),
        counts.data_deliveries()
    );
}

fn independent_mix(mut value: u64) -> u64 {
    value = value.wrapping_add(0x9E37_79B9_7F4A_7C15);
    value = (value ^ (value >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    value = (value ^ (value >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    value ^ (value >> 31)
}

#[test]
fn frozen_mixer_vectors_match_an_independent_reference() {
    let scenario = decode_scenario_manifest_v0(SCENARIO).unwrap();
    for vector in scenario.mixer_vectors() {
        assert_eq!(
            independent_mix(vector.seed() ^ vector.domain_constant() ^ vector.source_sequence()),
            vector.expected_score()
        );
    }
}

fn independent_ppm(numerator: u64, denominator: u64) -> u64 {
    numerator * 1_000_000 / denominator
}

fn independent_nearest_rank(mut values: Vec<u64>, basis_points: usize) -> u64 {
    values.sort_unstable();
    let rank = (basis_points * values.len()).div_ceil(10_000);
    values[rank - 1]
}

#[test]
fn rate_and_percentile_boundaries_match_independent_integer_references() {
    assert_eq!(independent_ppm(1, 250), 4_000);
    assert_eq!(independent_ppm(8, 250), 32_000);
    assert_eq!(independent_ppm(9, 500), 18_000);
    assert_eq!(independent_nearest_rank(vec![180, 720], 9_900), 720);
    assert_eq!(independent_nearest_rank(vec![1, 2, 3, 4], 2_500), 1);
    assert_eq!(independent_nearest_rank(vec![1, 2, 3, 4], 2_501), 2);
}

#[test]
fn oracle_and_baseline_contracts_are_distinct_types() {
    assert_ne!(
        TypeId::of::<OracleManifestV0>(),
        TypeId::of::<BaselineManifestV0>()
    );
}

#[test]
fn declared_provenance_decoys_fail_support_validation() {
    let oracle = decode_oracle_manifest_v0(ORACLE).unwrap();
    let results = oracle
        .support_vectors()
        .map(|(predicate, candidate, expected)| (predicate.allows(candidate), expected))
        .collect::<Vec<_>>();
    assert!(results.contains(&(true, true)));
    assert!(results.iter().filter(|(_, expected)| !expected).count() >= 2);
    assert!(results.iter().all(|(actual, expected)| actual == expected));
}

#[test]
fn manifest_cap_is_checked_before_parsing() {
    let mut exact = vec![b'x'; MAX_FIXTURE_DOCUMENT_BYTES];
    *exact.last_mut().unwrap() = b'\n';
    assert!(!matches!(
        decode_scenario_manifest_v0(&exact),
        Err(ContractErrorV0::DocumentTooLarge { .. })
    ));

    let input = vec![b'x'; MAX_FIXTURE_DOCUMENT_BYTES + 1];
    assert_eq!(
        decode_scenario_manifest_v0(&input),
        Err(ContractErrorV0::DocumentTooLarge {
            maximum: MAX_FIXTURE_DOCUMENT_BYTES,
            actual: MAX_FIXTURE_DOCUMENT_BYTES + 1,
        })
    );
}

#[test]
fn manifest_versions_unknown_fields_and_noncanonical_bytes_fail_closed() {
    let version = std::str::from_utf8(SCORING).unwrap().replacen(
        "\"manifest_version\":0",
        "\"manifest_version\":1",
        1,
    );
    assert!(matches!(
        decode_scoring_manifest_v0(version.as_bytes()),
        Err(ContractErrorV0::UnsupportedManifestVersion { actual: 1, .. })
    ));

    let unknown = std::str::from_utf8(BASELINES).unwrap().replacen(
        "\"rag_enabled\":false",
        "\"unknown\":0,\"rag_enabled\":false",
        1,
    );
    assert!(matches!(
        decode_baseline_manifest_v0(unknown.as_bytes()),
        Err(ContractErrorV0::MalformedJson { .. })
    ));

    let whitespace = std::str::from_utf8(ORACLE).unwrap().replacen('{', "{ ", 1);
    assert!(matches!(
        decode_oracle_manifest_v0(whitespace.as_bytes()),
        Err(ContractErrorV0::NonCanonicalEncoding { .. })
    ));
}

#[test]
fn manifest_diagnostics_do_not_echo_invalid_values() {
    let first = b"{\"private-a\":true}\n";
    let second = b"{\"private-b\":true}\n";
    let first_error = decode_oracle_manifest_v0(first).unwrap_err();
    let second_error = decode_oracle_manifest_v0(second).unwrap_err();
    assert_eq!(format!("{first_error:?}"), format!("{second_error:?}"));
    assert!(!first_error.to_string().contains("private"));
}

#[test]
fn malformed_manifest_byte_corpus_never_panics() {
    for length in 0..=512 {
        let mut input = (0..length)
            .map(|index| ((index * 17 + length) % 256) as u8)
            .collect::<Vec<_>>();
        if let Some(last) = input.last_mut() {
            *last = b'\n';
        }
        assert!(std::panic::catch_unwind(|| decode_scoring_manifest_v0(&input)).is_ok());
    }
}
