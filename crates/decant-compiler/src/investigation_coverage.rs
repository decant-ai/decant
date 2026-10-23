//! Pure, bounded accounting for explicitly declared investigation probes.
//!
//! This is an internal compiler record, not an investigation report, CLI schema
//! or checkpoint format. A complete response (including zero rows) means only
//! that the caller recorded complete response coverage. It does not establish
//! service health, population completeness, an atomic snapshot or causality.
//! Callers must supply an explicit outcome for every declared probe.

use std::{error::Error, fmt, marker::PhantomData};

use serde::{Deserialize, Deserializer, Serialize, de};

use crate::evidence::{bounded_json, digest};

pub const MAX_INVESTIGATION_PROBES_V1: usize = 32;
pub const MAX_INVESTIGATION_COVERAGE_BYTES_V1: usize = 16 * 1024;
const HASH_DOMAIN: &[u8] = b"decant-investigation-coverage:v1\0";

/// Fixed diagnostics only: never retain input values or an underlying error.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum InvestigationCoverageErrorV1 {
    InputTooLarge,
    InvalidInput,
    EmptyProbeSet,
    DuplicateProbeId,
    InvalidInterval,
    OutcomeMismatch,
    Encoding,
}

impl fmt::Display for InvestigationCoverageErrorV1 {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::InputTooLarge => "investigation coverage input exceeds byte limit",
            Self::InvalidInput => "invalid investigation coverage input",
            Self::EmptyProbeSet => "investigation coverage requires declared probes",
            Self::DuplicateProbeId => "duplicate investigation probe identifier",
            Self::InvalidInterval => "invalid investigation probe interval",
            Self::OutcomeMismatch => "investigation outcomes do not match declared probes",
            Self::Encoding => "investigation coverage encoding failed",
        })
    }
}

impl Error for InvestigationCoverageErrorV1 {}

/// Validated, immutable coverage bytes and their domain-separated identity.
pub struct InvestigationCoverageV1 {
    complete: bool,
    canonical_bytes: Vec<u8>,
    hash: String,
}

impl InvestigationCoverageV1 {
    /// Whether every declared probe has a complete response, not a diagnosis.
    pub fn is_complete(&self) -> bool {
        self.complete
    }

    /// Fixed-order compact JSON, sorted by probe ID, without a trailing LF.
    pub fn canonical_bytes(&self) -> &[u8] {
        &self.canonical_bytes
    }

    /// SHA-256 of `decant-investigation-coverage:v1\0` followed by canonical bytes.
    /// This identifies supplied coverage, not the truth of backend observations.
    pub fn hash(&self) -> &str {
        &self.hash
    }
}

impl fmt::Debug for InvestigationCoverageV1 {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("InvestigationCoverageV1")
            .finish_non_exhaustive()
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Input {
    version: u16,
    #[serde(deserialize_with = "bounded_records")]
    probes: Vec<Probe>,
    #[serde(deserialize_with = "bounded_records")]
    outcomes: Vec<RecordedOutcome>,
}

#[derive(Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(try_from = "String")]
struct ProbeId(String);

impl TryFrom<String> for ProbeId {
    type Error = &'static str;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        if value.len() != 32
            || !value
                .bytes()
                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
        {
            return Err("invalid probe identifier");
        }
        Ok(Self(value))
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Probe {
    probe_id: ProbeId,
    interval: Object<Interval>,
}

/// Requested half-open interval, not a statement about source completeness.
#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct Interval {
    start_ns: i64,
    end_ns: i64,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RecordedOutcome {
    probe_id: ProbeId,
    outcome: Outcome,
}

#[derive(Deserialize, Serialize)]
#[serde(tag = "status", rename_all = "snake_case", deny_unknown_fields)]
enum Outcome {
    Complete {
        observed_rows: u64,
    },
    Partial {
        observed_rows: u64,
        reason: PartialReason,
    },
    Unavailable {
        reason: UnavailableReason,
    },
    Unattempted {
        reason: UnattemptedReason,
    },
}

#[derive(Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
enum PartialReason {
    ResponseTruncated,
    BackendPartial,
}

#[derive(Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
enum UnavailableReason {
    PermissionDenied,
    Timeout,
    BackendUnavailable,
    InvalidResponse,
    RequestFailed,
}

#[derive(Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
enum UnattemptedReason {
    NotScheduled,
    BudgetExhausted,
    Cancelled,
}

#[derive(Serialize)]
struct Document {
    format: &'static str,
    version: u16,
    complete: bool,
    probes: Vec<ProbeCoverage>,
}

#[derive(Serialize)]
struct ProbeCoverage {
    probe_id: ProbeId,
    interval: Object<Interval>,
    outcome: Outcome,
}

// Serde's derived structs also accept positional sequences. Require JSON objects
// at every record boundary so the named-field contract has no alternate shape.
#[derive(Serialize)]
struct Object<T>(T);

impl<'de, T: Deserialize<'de>> Deserialize<'de> for Object<T> {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct ObjectVisitor<T>(PhantomData<T>);

        impl<'de, T: Deserialize<'de>> de::Visitor<'de> for ObjectVisitor<T> {
            type Value = Object<T>;

            fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str("coverage object")
            }

            fn visit_map<A: de::MapAccess<'de>>(self, map: A) -> Result<Self::Value, A::Error> {
                T::deserialize(de::value::MapAccessDeserializer::new(map)).map(Object)
            }
        }

        deserializer.deserialize_map(ObjectVisitor(PhantomData))
    }
}

// Do not trust a sequence's size hint or deserialize an unbounded Vec and then
// check its length. The byte cap also bounds temporary parser/string allocation.
fn bounded_records<'de, D, T>(deserializer: D) -> Result<Vec<T>, D::Error>
where
    D: Deserializer<'de>,
    T: Deserialize<'de>,
{
    struct RecordsVisitor<T>(PhantomData<T>);

    impl<'de, T: Deserialize<'de>> de::Visitor<'de> for RecordsVisitor<T> {
        type Value = Vec<T>;

        fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
            formatter.write_str("at most 32 coverage records")
        }

        fn visit_seq<A: de::SeqAccess<'de>>(self, mut sequence: A) -> Result<Vec<T>, A::Error> {
            let mut records = Vec::with_capacity(MAX_INVESTIGATION_PROBES_V1);
            while let Some(Object(record)) = sequence.next_element::<Object<T>>()? {
                if records.len() == MAX_INVESTIGATION_PROBES_V1 {
                    return Err(de::Error::custom("too many coverage records"));
                }
                records.push(record);
            }
            Ok(records)
        }
    }

    deserializer.deserialize_seq(RecordsVisitor(PhantomData))
}

/// Compile version-1 declarations and outcomes without network, clock or model
/// access. Unknown/duplicate fields and status-specific metadata fail closed.
/// Probe IDs are exactly 32 lowercase hex characters. Both arrays contain at
/// most 32 records; every nonempty declaration set needs one outcome per probe.
pub fn compile_investigation_coverage_v1(
    input: &[u8],
) -> Result<InvestigationCoverageV1, InvestigationCoverageErrorV1> {
    use InvestigationCoverageErrorV1 as E;

    if input.len() > MAX_INVESTIGATION_COVERAGE_BYTES_V1 {
        return Err(E::InputTooLarge);
    }
    // Preserve serde_json's recursion limit and discard its value-bearing errors.
    let Object(mut input): Object<Input> =
        serde_json::from_slice(input).map_err(|_| E::InvalidInput)?;
    if input.version != 1 {
        return Err(E::InvalidInput);
    }
    if input.probes.is_empty() {
        return Err(E::EmptyProbeSet);
    }
    input
        .probes
        .sort_unstable_by(|a, b| a.probe_id.cmp(&b.probe_id));
    input
        .outcomes
        .sort_unstable_by(|a, b| a.probe_id.cmp(&b.probe_id));
    if input
        .probes
        .windows(2)
        .any(|pair| pair[0].probe_id == pair[1].probe_id)
        || input
            .outcomes
            .windows(2)
            .any(|pair| pair[0].probe_id == pair[1].probe_id)
    {
        return Err(E::DuplicateProbeId);
    }
    for probe in &input.probes {
        if !probe
            .interval
            .0
            .end_ns
            .checked_sub(probe.interval.0.start_ns)
            .is_some_and(|duration| duration > 0)
        {
            return Err(E::InvalidInterval);
        }
    }
    if input.probes.len() != input.outcomes.len()
        || input
            .probes
            .iter()
            .zip(&input.outcomes)
            .any(|(probe, outcome)| probe.probe_id != outcome.probe_id)
    {
        return Err(E::OutcomeMismatch);
    }
    let complete = input
        .outcomes
        .iter()
        .all(|record| matches!(record.outcome, Outcome::Complete { .. }));
    let document = Document {
        format: "decant-investigation-coverage",
        version: 1,
        complete,
        probes: input
            .probes
            .into_iter()
            .zip(input.outcomes)
            .map(|(probe, record)| ProbeCoverage {
                probe_id: probe.probe_id,
                interval: probe.interval,
                outcome: record.outcome,
            })
            .collect(),
    };
    // At most 32 fixed-shape records: bounded IDs, integers and closed enums.
    // No caller-provided text, nested objects or new collections reach encoding.
    let canonical_bytes =
        bounded_json(&document, MAX_INVESTIGATION_COVERAGE_BYTES_V1).map_err(|_| E::Encoding)?;
    let hash = format!("sha256:v1:{}", digest(HASH_DOMAIN, &canonical_bytes));
    Ok(InvestigationCoverageV1 {
        complete,
        canonical_bytes,
        hash,
    })
}
