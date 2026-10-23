//! Bounded, read-only backend access. No ambient network configuration.
#![forbid(unsafe_code)]

mod bounded_json;
mod evidence;
pub use evidence::{ArtifactDomainV1, ReadV1, artifact_hash_v1};
pub mod grafana;
pub mod loki;
pub mod metrics;
pub mod tempo;
pub mod transport;
