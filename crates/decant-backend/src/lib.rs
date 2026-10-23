//! Bounded, read-only backend access. No ambient network configuration.
#![forbid(unsafe_code)]

mod bounded_json;
mod evidence;
pub mod grafana;
pub mod metrics;
pub mod tempo;
pub mod transport;
