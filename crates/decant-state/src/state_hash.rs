use std::error::Error;
use std::fmt;

use sha2::{Digest, Sha256};

use crate::{
    AdmissionSnapshotEncodeErrorV0, AdmissionSnapshotEncodeErrorV1, AdmissionStateV0,
    encode_admission_snapshot_v0, encode_admission_snapshot_v1,
};

pub const ADMISSION_STATE_HASH_VERSION_V0: u16 = 0;
pub const ADMISSION_STATE_HASH_ALGORITHM_V0: &str = "sha256";
pub const ADMISSION_STATE_HASH_DOMAIN_V0: &[u8; 30] = b"decant:admission-state-hash:v0";
pub const ADMISSION_STATE_HASH_TEXT_BYTES_V0: usize = 74;
pub const ADMISSION_STATE_HASH_VERSION_V1: u16 = 1;
pub const ADMISSION_STATE_HASH_ALGORITHM_V1: &str = "sha256";
pub const ADMISSION_STATE_HASH_DOMAIN_V1: &[u8; 30] = b"decant:admission-state-hash:v1";
pub const ADMISSION_STATE_HASH_TEXT_BYTES_V1: usize = 74;

const ADMISSION_STATE_HASH_DOMAIN_LENGTH_V0: u32 = 30;
const ADMISSION_STATE_HASH_DIGEST_BYTES_V0: usize = 32;
const ADMISSION_STATE_HASH_DOMAIN_LENGTH_V1: u32 = 30;
const ADMISSION_STATE_HASH_DIGEST_BYTES_V1: usize = 32;

#[derive(Clone, Copy, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct AdmissionStateHashV0 {
    digest: [u8; ADMISSION_STATE_HASH_DIGEST_BYTES_V0],
}

impl AdmissionStateHashV0 {
    pub const fn as_bytes(&self) -> &[u8; ADMISSION_STATE_HASH_DIGEST_BYTES_V0] {
        &self.digest
    }

    pub fn to_canonical_string(self) -> String {
        self.to_string()
    }
}

impl fmt::Debug for AdmissionStateHashV0 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("AdmissionStateHashV0")
            .field("algorithm", &ADMISSION_STATE_HASH_ALGORITHM_V0)
            .field("version", &ADMISSION_STATE_HASH_VERSION_V0)
            .field("digest", &"<redacted>")
            .finish()
    }
}

impl fmt::Display for AdmissionStateHashV0 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "{ADMISSION_STATE_HASH_ALGORITHM_V0}:v{ADMISSION_STATE_HASH_VERSION_V0}:"
        )?;
        for byte in self.digest {
            write!(formatter, "{byte:02x}")?;
        }
        Ok(())
    }
}

#[derive(Clone, Eq, PartialEq)]
pub enum AdmissionStateHashErrorV0 {
    SnapshotEncoding(AdmissionSnapshotEncodeErrorV0),
    SnapshotLengthOverflow,
}

impl fmt::Debug for AdmissionStateHashErrorV0 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::SnapshotEncoding(error) => formatter
                .debug_tuple("SnapshotEncoding")
                .field(error)
                .finish(),
            Self::SnapshotLengthOverflow => formatter.write_str("SnapshotLengthOverflow"),
        }
    }
}

impl fmt::Display for AdmissionStateHashErrorV0 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(self, formatter)
    }
}

impl Error for AdmissionStateHashErrorV0 {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::SnapshotEncoding(error) => Some(error),
            Self::SnapshotLengthOverflow => None,
        }
    }
}

pub fn hash_admission_state_v0(
    state: &AdmissionStateV0,
) -> Result<AdmissionStateHashV0, AdmissionStateHashErrorV0> {
    let snapshot =
        encode_admission_snapshot_v0(state).map_err(AdmissionStateHashErrorV0::SnapshotEncoding)?;
    let snapshot_length = u64::try_from(snapshot.len())
        .map_err(|_| AdmissionStateHashErrorV0::SnapshotLengthOverflow)?;

    let mut hasher = Sha256::new();
    hasher.update(ADMISSION_STATE_HASH_DOMAIN_LENGTH_V0.to_be_bytes());
    hasher.update(ADMISSION_STATE_HASH_DOMAIN_V0);
    hasher.update(snapshot_length.to_be_bytes());
    hasher.update(&snapshot);

    let output = hasher.finalize();
    let mut digest = [0_u8; ADMISSION_STATE_HASH_DIGEST_BYTES_V0];
    digest.copy_from_slice(&output);
    Ok(AdmissionStateHashV0 { digest })
}

#[derive(Clone, Copy, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct AdmissionStateHashV1 {
    digest: [u8; ADMISSION_STATE_HASH_DIGEST_BYTES_V1],
}

impl AdmissionStateHashV1 {
    pub const fn as_bytes(&self) -> &[u8; ADMISSION_STATE_HASH_DIGEST_BYTES_V1] {
        &self.digest
    }

    pub fn to_canonical_string(self) -> String {
        self.to_string()
    }
}

impl fmt::Debug for AdmissionStateHashV1 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("AdmissionStateHashV1")
            .field("algorithm", &ADMISSION_STATE_HASH_ALGORITHM_V1)
            .field("version", &ADMISSION_STATE_HASH_VERSION_V1)
            .field("digest", &"<redacted>")
            .finish()
    }
}

impl fmt::Display for AdmissionStateHashV1 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "{ADMISSION_STATE_HASH_ALGORITHM_V1}:v{ADMISSION_STATE_HASH_VERSION_V1}:"
        )?;
        for byte in self.digest {
            write!(formatter, "{byte:02x}")?;
        }
        Ok(())
    }
}

#[derive(Clone, Eq, PartialEq)]
pub enum AdmissionStateHashErrorV1 {
    SnapshotEncoding(AdmissionSnapshotEncodeErrorV1),
    SnapshotLengthOverflow,
}

impl fmt::Debug for AdmissionStateHashErrorV1 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::SnapshotEncoding(error) => formatter
                .debug_tuple("SnapshotEncoding")
                .field(error)
                .finish(),
            Self::SnapshotLengthOverflow => formatter.write_str("SnapshotLengthOverflow"),
        }
    }
}

impl fmt::Display for AdmissionStateHashErrorV1 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(self, formatter)
    }
}

impl Error for AdmissionStateHashErrorV1 {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::SnapshotEncoding(error) => Some(error),
            Self::SnapshotLengthOverflow => None,
        }
    }
}

pub fn hash_admission_state_v1(
    state: &AdmissionStateV0,
) -> Result<AdmissionStateHashV1, AdmissionStateHashErrorV1> {
    let snapshot =
        encode_admission_snapshot_v1(state).map_err(AdmissionStateHashErrorV1::SnapshotEncoding)?;
    let snapshot_length = u64::try_from(snapshot.len())
        .map_err(|_| AdmissionStateHashErrorV1::SnapshotLengthOverflow)?;

    let mut hasher = Sha256::new();
    hasher.update(ADMISSION_STATE_HASH_DOMAIN_LENGTH_V1.to_be_bytes());
    hasher.update(ADMISSION_STATE_HASH_DOMAIN_V1);
    hasher.update(snapshot_length.to_be_bytes());
    hasher.update(&snapshot);

    let output = hasher.finalize();
    let mut digest = [0_u8; ADMISSION_STATE_HASH_DIGEST_BYTES_V1];
    digest.copy_from_slice(&output);
    Ok(AdmissionStateHashV1 { digest })
}

#[cfg(test)]
mod tests {
    use decant_ir::SourceIdV0;

    use super::*;
    use crate::{
        ADMISSION_VERSION_V0, AdmissionConfigV0, AdmissionLimitsV0, AdmissionSnapshotFieldV0,
    };

    fn empty_state() -> AdmissionStateV0 {
        AdmissionStateV0::new(
            AdmissionConfigV0::new(
                ADMISSION_VERSION_V0,
                SourceIdV0::new("source-v0").unwrap(),
                0,
                100,
                AdmissionLimitsV0::maximum(),
            )
            .unwrap(),
        )
    }

    #[test]
    fn sha256_matches_standard_abc_known_answer() {
        let output = Sha256::digest(b"abc");
        assert_eq!(
            output.as_slice(),
            &[
                0xba, 0x78, 0x16, 0xbf, 0x8f, 0x01, 0xcf, 0xea, 0x41, 0x41, 0x40, 0xde, 0x5d, 0xae,
                0x22, 0x23, 0xb0, 0x03, 0x61, 0xa3, 0x96, 0x17, 0x7a, 0x9c, 0xb4, 0x10, 0xff, 0x61,
                0xf2, 0x00, 0x15, 0xad,
            ]
        );
    }

    #[test]
    fn domain_and_text_widths_are_frozen() {
        assert_eq!(
            ADMISSION_STATE_HASH_DOMAIN_V0.len(),
            usize::try_from(ADMISSION_STATE_HASH_DOMAIN_LENGTH_V0).unwrap()
        );
        assert_eq!(
            hash_admission_state_v0(&empty_state())
                .unwrap()
                .to_canonical_string()
                .len(),
            ADMISSION_STATE_HASH_TEXT_BYTES_V0
        );
        assert_eq!(
            ADMISSION_STATE_HASH_DOMAIN_V1.len(),
            usize::try_from(ADMISSION_STATE_HASH_DOMAIN_LENGTH_V1).unwrap()
        );
        assert_eq!(
            hash_admission_state_v1(&empty_state())
                .unwrap()
                .to_canonical_string()
                .len(),
            ADMISSION_STATE_HASH_TEXT_BYTES_V1
        );
    }

    #[test]
    fn inconsistent_state_returns_snapshot_error_without_a_digest() {
        let mut state = empty_state();
        state.accepted_event_bytes = 1;
        assert_eq!(
            hash_admission_state_v0(&state),
            Err(AdmissionStateHashErrorV0::SnapshotEncoding(
                AdmissionSnapshotEncodeErrorV0::StateInvariant {
                    field: AdmissionSnapshotFieldV0::AcceptedEventBytes,
                }
            ))
        );
        assert_eq!(
            hash_admission_state_v1(&state),
            Err(AdmissionStateHashErrorV1::SnapshotEncoding(
                AdmissionSnapshotEncodeErrorV1::StateInvariant {
                    field: crate::AdmissionSnapshotFieldV1::AcceptedEventBytes,
                }
            ))
        );
    }
}
