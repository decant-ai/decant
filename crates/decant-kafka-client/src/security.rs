//! Validated TLS and externally referenced SASL/SCRAM configuration.

use std::error::Error;
use std::fmt;
use std::path::Path;

pub const KAFKA_SECURITY_CONFIG_VERSION_V0: u16 = 0;
pub const KAFKA_SECURITY_MAX_CA_LOCATION_BYTES_V0: usize = 4 * 1_024;
pub const KAFKA_SECURITY_MAX_SECRET_ENV_NAME_BYTES_V0: usize = 128;
pub const KAFKA_SECURITY_MAX_SECRET_VALUE_BYTES_V0: usize = 4 * 1_024;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum KafkaStaticSecurityModeV0 {
    Plaintext,
    Tls,
    TlsSaslScram,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum KafkaSaslScramMechanismV0 {
    Sha256,
    Sha512,
}

impl KafkaSaslScramMechanismV0 {
    pub(crate) const fn native_name(self) -> &'static str {
        match self {
            Self::Sha256 => "SCRAM-SHA-256",
            Self::Sha512 => "SCRAM-SHA-512",
        }
    }
}

#[derive(Clone, Eq, PartialEq)]
pub struct KafkaTlsTrustV0 {
    ca_location: Option<String>,
}

impl KafkaTlsTrustV0 {
    pub fn system_roots() -> Self {
        Self { ca_location: None }
    }

    pub fn absolute_ca_location(location: &str) -> Result<Self, KafkaStaticSecurityConfigErrorV0> {
        if location.is_empty()
            || location.len() > KAFKA_SECURITY_MAX_CA_LOCATION_BYTES_V0
            || location
                .bytes()
                .any(|byte| byte == 0 || byte.is_ascii_control())
            || !Path::new(location).is_absolute()
        {
            return Err(KafkaStaticSecurityConfigErrorV0::InvalidCaLocation);
        }
        Ok(Self {
            ca_location: Some(location.to_owned()),
        })
    }

    pub const fn uses_system_roots(&self) -> bool {
        self.ca_location.is_none()
    }

    pub(crate) fn ca_location(&self) -> Option<&str> {
        self.ca_location.as_deref()
    }
}

impl fmt::Debug for KafkaTlsTrustV0 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("KafkaTlsTrustV0")
            .field("uses_system_roots", &self.uses_system_roots())
            .finish()
    }
}

#[derive(Clone, Eq, PartialEq)]
pub struct KafkaSecretEnvironmentReferenceV0 {
    name: String,
}

impl KafkaSecretEnvironmentReferenceV0 {
    pub fn new(name: &str) -> Result<Self, KafkaStaticSecurityConfigErrorV0> {
        let valid = !name.is_empty()
            && name.len() <= KAFKA_SECURITY_MAX_SECRET_ENV_NAME_BYTES_V0
            && name.bytes().enumerate().all(|(index, byte)| match byte {
                b'A'..=b'Z' | b'_' => true,
                b'0'..=b'9' => index > 0,
                _ => false,
            });
        if !valid {
            return Err(KafkaStaticSecurityConfigErrorV0::InvalidSecretEnvironmentReference);
        }
        Ok(Self {
            name: name.to_owned(),
        })
    }

    pub fn name(&self) -> &str {
        &self.name
    }
}

impl fmt::Debug for KafkaSecretEnvironmentReferenceV0 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("KafkaSecretEnvironmentReferenceV0(<redacted>)")
    }
}

#[derive(Clone, Eq, PartialEq)]
pub enum KafkaStaticSecurityConfigV0 {
    Plaintext,
    Tls {
        trust: KafkaTlsTrustV0,
    },
    TlsSaslScram {
        trust: KafkaTlsTrustV0,
        mechanism: KafkaSaslScramMechanismV0,
        username: KafkaSecretEnvironmentReferenceV0,
        password: KafkaSecretEnvironmentReferenceV0,
    },
}

impl KafkaStaticSecurityConfigV0 {
    pub fn plaintext(version: u16) -> Result<Self, KafkaStaticSecurityConfigErrorV0> {
        validate_version(version)?;
        Ok(Self::Plaintext)
    }

    pub fn tls(
        version: u16,
        trust: KafkaTlsTrustV0,
    ) -> Result<Self, KafkaStaticSecurityConfigErrorV0> {
        validate_version(version)?;
        Ok(Self::Tls { trust })
    }

    pub fn tls_sasl_scram(
        version: u16,
        trust: KafkaTlsTrustV0,
        mechanism: KafkaSaslScramMechanismV0,
        username: KafkaSecretEnvironmentReferenceV0,
        password: KafkaSecretEnvironmentReferenceV0,
    ) -> Result<Self, KafkaStaticSecurityConfigErrorV0> {
        validate_version(version)?;
        if username == password {
            return Err(KafkaStaticSecurityConfigErrorV0::DuplicateSecretEnvironmentReference);
        }
        Ok(Self::TlsSaslScram {
            trust,
            mechanism,
            username,
            password,
        })
    }

    pub const fn mode(&self) -> KafkaStaticSecurityModeV0 {
        match self {
            Self::Plaintext => KafkaStaticSecurityModeV0::Plaintext,
            Self::Tls { .. } => KafkaStaticSecurityModeV0::Tls,
            Self::TlsSaslScram { .. } => KafkaStaticSecurityModeV0::TlsSaslScram,
        }
    }

    pub const fn scram_mechanism(&self) -> Option<KafkaSaslScramMechanismV0> {
        match self {
            Self::TlsSaslScram { mechanism, .. } => Some(*mechanism),
            Self::Plaintext | Self::Tls { .. } => None,
        }
    }
}

impl fmt::Debug for KafkaStaticSecurityConfigV0 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("KafkaStaticSecurityConfigV0")
            .field("version", &KAFKA_SECURITY_CONFIG_VERSION_V0)
            .field("mode", &self.mode())
            .field("scram_mechanism", &self.scram_mechanism())
            .finish()
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum KafkaStaticSecurityConfigErrorV0 {
    UnsupportedVersion { actual: u16 },
    InvalidCaLocation,
    InvalidSecretEnvironmentReference,
    DuplicateSecretEnvironmentReference,
}

impl fmt::Display for KafkaStaticSecurityConfigErrorV0 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(self, formatter)
    }
}

impl Error for KafkaStaticSecurityConfigErrorV0 {}

fn validate_version(version: u16) -> Result<(), KafkaStaticSecurityConfigErrorV0> {
    if version != KAFKA_SECURITY_CONFIG_VERSION_V0 {
        return Err(KafkaStaticSecurityConfigErrorV0::UnsupportedVersion { actual: version });
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn versions_paths_references_and_debug_are_fail_closed() {
        assert_eq!(
            KafkaStaticSecurityConfigV0::plaintext(1).unwrap_err(),
            KafkaStaticSecurityConfigErrorV0::UnsupportedVersion { actual: 1 }
        );
        for invalid in ["", "relative/ca.pem", "/private\nca.pem"] {
            assert_eq!(
                KafkaTlsTrustV0::absolute_ca_location(invalid).unwrap_err(),
                KafkaStaticSecurityConfigErrorV0::InvalidCaLocation
            );
        }
        assert!(KafkaTlsTrustV0::absolute_ca_location("/private/ca.pem").is_ok());
        assert!(
            KafkaTlsTrustV0::absolute_ca_location(&format!(
                "/{}",
                "a".repeat(KAFKA_SECURITY_MAX_CA_LOCATION_BYTES_V0 - 1)
            ))
            .is_ok()
        );
        assert_eq!(
            KafkaTlsTrustV0::absolute_ca_location(&format!(
                "/{}",
                "a".repeat(KAFKA_SECURITY_MAX_CA_LOCATION_BYTES_V0)
            ))
            .unwrap_err(),
            KafkaStaticSecurityConfigErrorV0::InvalidCaLocation
        );
        for invalid in ["", "lowercase", "9PREFIX", "DASH-NAME"] {
            assert_eq!(
                KafkaSecretEnvironmentReferenceV0::new(invalid).unwrap_err(),
                KafkaStaticSecurityConfigErrorV0::InvalidSecretEnvironmentReference
            );
        }
        assert!(
            KafkaSecretEnvironmentReferenceV0::new(
                &"A".repeat(KAFKA_SECURITY_MAX_SECRET_ENV_NAME_BYTES_V0)
            )
            .is_ok()
        );
        assert_eq!(
            KafkaSecretEnvironmentReferenceV0::new(
                &"A".repeat(KAFKA_SECURITY_MAX_SECRET_ENV_NAME_BYTES_V0 + 1)
            )
            .unwrap_err(),
            KafkaStaticSecurityConfigErrorV0::InvalidSecretEnvironmentReference
        );
        let reference = KafkaSecretEnvironmentReferenceV0::new("DECANT_KAFKA_USER").unwrap();
        assert_eq!(reference.name(), "DECANT_KAFKA_USER");
        assert!(!format!("{reference:?}").contains("DECANT_KAFKA_USER"));

        let duplicate = KafkaStaticSecurityConfigV0::tls_sasl_scram(
            0,
            KafkaTlsTrustV0::system_roots(),
            KafkaSaslScramMechanismV0::Sha256,
            reference.clone(),
            reference,
        )
        .unwrap_err();
        assert_eq!(
            duplicate,
            KafkaStaticSecurityConfigErrorV0::DuplicateSecretEnvironmentReference
        );
        let security = KafkaStaticSecurityConfigV0::tls(
            0,
            KafkaTlsTrustV0::absolute_ca_location("/private/ca.pem").unwrap(),
        )
        .unwrap();
        let debug = format!("{security:?}");
        assert!(!debug.contains("/private/ca.pem"));
    }
}
