use super::*;
use crate::kafka_json::evidence::parse;
use decant_ir::KafkaPositionV0;
use decant_kafka::{KafkaFetchedRecordV0, KafkaRecordTimestampV0};
use decant_kafka_live::{KafkaResidentOperationErrorV0, KafkaResidentStatusV0};
use serde_json::{Value, json};
use std::{fs, path::PathBuf, sync::atomic::AtomicU64};

static NEXT: AtomicU64 = AtomicU64::new(0);
struct Directory(PathBuf);
impl Directory {
    fn new() -> Self {
        let path = std::env::temp_dir().join(format!(
            "decant-evidence-cut-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        fs::create_dir(&path).unwrap();
        Self(path)
    }
    fn config(&self, limit: usize) -> KafkaEvidenceConfigV1 {
        let mut doc: Value = serde_json::from_str(include_str!(
            "../../../../../examples/evidence/kafka-config-v1.json"
        ))
        .unwrap();
        doc["checkpoint_directory"] = json!(self.0);
        doc["accepted_identity_limit"] = json!(limit);
        doc["lifecycle"]["records_between_checkpoints"] = json!(2);
        doc["lifecycle"]["idle_observations_between_checkpoints"] = json!(1);
        doc["recipe"]["policy"]["allow_service_versions"] = json!(true);
        parse(&serde_json::to_vec(&doc).unwrap()).unwrap()
    }
}
impl Drop for Directory {
    fn drop(&mut self) {
        fs::remove_dir_all(&self.0).unwrap();
    }
}
fn bootstrap(
    config: &KafkaEvidenceConfigV1,
) -> Result<KafkaCheckpointCutResidentV1, KafkaResidentBootstrapErrorV0> {
    KafkaCheckpointCutResidentV1::bootstrap(
        LocalCheckpointStoreV0::open(&config.checkpoint_directory).unwrap(),
        config.admission.clone(),
        config.source.source_contract().clone(),
        config.source.starting_positions(),
        config.lifecycle,
        |state, frontier| compile(state, frontier, &config.recipe),
    )
}
fn record(index: usize) -> KafkaFetchedRecordV0 {
    let line = include_str!("../../../../../examples/evidence/kafka-spans-v1.jsonl")
        .lines()
        .nth(index)
        .unwrap();
    record_bytes(index as i64, line.as_bytes())
}
fn record_bytes(offset: i64, bytes: &[u8]) -> KafkaFetchedRecordV0 {
    KafkaFetchedRecordV0::new(
        KafkaPositionV0::new(0, offset).unwrap(),
        None,
        Some(bytes),
        KafkaRecordTimestampV0::NotAvailable,
        &[],
    )
    .unwrap()
}
fn apply(session: &mut KafkaCheckpointCutResidentV1, config: &KafkaEvidenceConfigV1, index: usize) {
    session
        .process_json_object_record_v1(&config.mapping, &record(index), |state, frontier| {
            compile(state, frontier, &config.recipe)
        })
        .unwrap();
}
#[test]
fn verified_cut_recovery_matches_uninterrupted_and_noncut_never_compiles() {
    let first = Directory::new();
    let second = Directory::new();
    let a = first.config(100);
    let b = second.config(100);
    let mut uninterrupted = bootstrap(&a).unwrap();
    let mut restarted = bootstrap(&b).unwrap();
    assert!(uninterrupted.context().is_none());
    uninterrupted
        .process_json_object_record_v1(&a.mapping, &record(0), |_, _| {
            panic!("noncut compiler call")
        })
        .unwrap();
    assert!(uninterrupted.context().is_none());
    for index in 1..4 {
        apply(&mut uninterrupted, &a, index);
    }
    assert_eq!(uninterrupted.usage().contexts_compiled(), 3); // bootstrap + two cuts
    apply(&mut restarted, &b, 0);
    apply(&mut restarted, &b, 1);
    let cut = restarted.context().unwrap().as_bytes().to_vec();
    apply(&mut restarted, &b, 2); // uncheckpointed suffix lost, not served
    assert_eq!(restarted.context().unwrap().as_bytes(), cut);
    drop(restarted);
    let mut restarted = bootstrap(&b).unwrap();
    assert_eq!(restarted.context().unwrap().as_bytes(), cut);
    apply(&mut restarted, &b, 2);
    apply(&mut restarted, &b, 3);
    assert_eq!(
        restarted.context().unwrap().as_bytes(),
        uninterrupted.context().unwrap().as_bytes()
    );
    let report =
        EvidenceDocumentV1::from_verified_bytes(restarted.context().unwrap().as_bytes()).unwrap();
    assert_eq!(
        report.source_cut.input.source_progress_hash.unwrap(),
        hash_kafka_partition_frontier_v0(restarted.session().frontier())
            .unwrap()
            .to_string()
    );
    let service = &report.services[0];
    assert_eq!(
        (
            service.baseline.observed_server_spans,
            service.baseline.error_spans,
            service.baseline.p95_duration_ns
        ),
        (2, 1, Some(20_000_000))
    );
    assert_eq!(
        (
            service.current.observed_server_spans,
            service.current.error_spans,
            service.current.p95_duration_ns
        ),
        (2, 1, Some(200_000_000))
    );
    assert!(
        !String::from_utf8_lossy(restarted.context().unwrap().as_bytes())
            .contains("synthetic-private-detail")
    );
    assert!(
        !String::from_utf8_lossy(restarted.context().unwrap().as_bytes())
            .contains("0000000000000001")
    );
}

#[test]
fn conflict_capacity_and_compile_failure_never_publish_or_revive() {
    for mode in 0..3 {
        let directory = Directory::new();
        let config = directory.config(if mode == 0 { 1 } else { 100 });
        let mut session = bootstrap(&config).unwrap();
        apply(&mut session, &config, 0);
        let result = match mode {
            0 => session.process_json_object_record_v1(&config.mapping, &record(1), |_, _| {
                panic!("rejected suffix compiled")
            }),
            1 => {
                let mut span: Value = serde_json::from_slice(record(0).value().unwrap()).unwrap();
                span["duration_ns"] = json!(99);
                session.process_json_object_record_v1(
                    &config.mapping,
                    &record_bytes(1, &serde_json::to_vec(&span).unwrap()),
                    |state, frontier| compile(state, frontier, &config.recipe),
                )
            }
            _ => session.process_json_object_record_v1(&config.mapping, &record(1), |_, _| {
                Err(KafkaResidentCompilerErrorV0::Refused)
            }),
        };
        assert!(result.is_err());
        if mode == 0 {
            assert_eq!(
                result.unwrap_err(),
                KafkaResidentOperationErrorV0::RejectedAdmission
            );
        }
        assert_eq!(session.status(), KafkaResidentStatusV0::Faulted);
        assert!(session.context().is_none());
        assert!(session.session().latest_generation().is_none());
        assert!(
            session
                .graceful_shutdown(|_, _| panic!("fault flushed"))
                .is_err()
        );
        drop(session);
        let restored = bootstrap(&config).unwrap();
        assert!(restored.context().is_none());
        assert_eq!(
            restored.session().admission().usage().accepted_identities(),
            0
        );
    }
}

#[test]
fn idle_shutdown_recovery_and_changed_recipe_are_strict() {
    let directory = Directory::new();
    let config = directory.config(100);
    let mut session = bootstrap(&config).unwrap();
    apply(&mut session, &config, 0);
    session
        .observe_idle(|state, frontier| compile(state, frontier, &config.recipe))
        .unwrap();
    assert!(session.context().is_some());
    apply(&mut session, &config, 1);
    session
        .graceful_shutdown(|state, frontier| compile(state, frontier, &config.recipe))
        .unwrap();
    let bytes = session.context().unwrap().as_bytes().to_vec();
    drop(session);
    let restored = bootstrap(&config).unwrap();
    assert_eq!(restored.context().unwrap().as_bytes(), bytes);
    drop(restored);
    let mut document: Value = serde_json::from_str(include_str!(
        "../../../../../examples/evidence/kafka-config-v1.json"
    ))
    .unwrap();
    document["checkpoint_directory"] = json!(directory.0);
    let changed = parse(&serde_json::to_vec(&document).unwrap()).unwrap();
    assert!(bootstrap(&changed).is_err());
    let latest = fs::read_dir(&directory.0)
        .unwrap()
        .map(|entry| entry.unwrap().path())
        .filter(|path| path.extension().is_some_and(|ext| ext == "dcp"))
        .max()
        .unwrap();
    fs::write(latest, b"synthetic-corrupt").unwrap();
    assert!(bootstrap(&config).is_err());
}

#[test]
fn late_suffix_is_accounted_and_not_reordered_into_the_recipe() {
    let directory = Directory::new();
    let config = directory.config(100);
    let mut session = bootstrap(&config).unwrap();
    for index in 0..4 {
        apply(&mut session, &config, index);
    }
    let late = record_bytes(4, record(0).value().unwrap());
    let outcome = session
        .process_json_object_record_v1(&config.mapping, &late, |_, _| panic!("noncut compiled"))
        .unwrap();
    assert_eq!(
        outcome.process(),
        decant_kafka_live::KafkaStaticSessionProcessV0::Rejected {
            reason: decant_state::RejectionReasonV0::Late
        }
    );
    session
        .observe_idle(|state, frontier| compile(state, frontier, &config.recipe))
        .unwrap();
    let report =
        EvidenceDocumentV1::from_verified_bytes(session.context().unwrap().as_bytes()).unwrap();
    assert_eq!(report.gaps.rejections.iter().sum::<u64>(), 1);
    assert_eq!(report.source_cut.accepted_retained_entries, 4);
    assert_eq!(report.gaps.logical_duplicates_folded, 0);
}
