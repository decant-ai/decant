use decant_kafka_client::{
    KAFKA_CLIENT_BOUNDARY_VERSION_V0, RUST_RDKAFKA_CRATE_VERSION_V0,
    kafka_client_dependency_versions_v0,
};

#[test]
fn exact_wrapper_and_linked_native_versions_are_visible_without_broker_state() {
    assert_eq!(KAFKA_CLIENT_BOUNDARY_VERSION_V0, 0);
    assert_eq!(RUST_RDKAFKA_CRATE_VERSION_V0, "0.39.0");

    let versions = kafka_client_dependency_versions_v0();
    assert_eq!(versions.rust_wrapper(), "0.39.0");
    assert!(versions.native_version_number() > 0);
    assert!(!versions.native_version().is_empty());

    let diagnostic = format!("{versions:?}");
    assert!(!diagnostic.contains("broker"));
    assert!(!diagnostic.contains("topic"));
    assert!(!diagnostic.contains("source"));
}
