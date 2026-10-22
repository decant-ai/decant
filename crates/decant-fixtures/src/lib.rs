//! Private, bounded fixture and SRE evaluation contracts for Decant.
//!
//! These V0 formats are repository-internal and carry no compatibility promise.

#![forbid(unsafe_code)]

mod codec;
mod contracts;
mod generator;

pub use codec::{
    BinaryFieldV0, EndOfInputV0, FixtureCodecErrorV0, FixtureRecordV0, MAX_FIXTURE_DOCUMENT_BYTES,
    decode_fixture_record_v0, encode_fixture_record_v0,
};
pub use contracts::{
    BaselineManifestV0, ContractBoundV0, ContractErrorV0, ContractKindV0, MixerVectorV0,
    OracleManifestV0, ProvenanceCandidateV0, ScenarioManifestV0, ScoringManifestV0,
    SupportPredicateV0, decode_baseline_manifest_v0, decode_oracle_manifest_v0,
    decode_scenario_manifest_v0, decode_scoring_manifest_v0, validate_sre_contract_links_v0,
};
pub use generator::{
    GENERATOR_VERSION_V0, GeneratorContractFieldV0, GeneratorErrorV0, GeneratorUsageV0,
    SRE_V0_DATA_DELIVERIES, SRE_V0_UNIQUE_EVENTS, SreGeneratorV0,
};
