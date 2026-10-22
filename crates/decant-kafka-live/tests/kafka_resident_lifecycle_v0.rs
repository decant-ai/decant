use std::fs;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use decant_ir::{KafkaPositionV0, SourceIdV0, SourcePositionKindV0};
use decant_kafka::{
    KafkaClusterIdV0, KafkaFetchedRecordV0, KafkaJsonEntityKeySourceV1, KafkaJsonEventTimeUnitV0,
    KafkaJsonObjectMappingV0, KafkaJsonObjectMappingV1, KafkaJsonObjectProjectionV0,
    KafkaJsonScalarKindV0, KafkaRecordTimestampV0, KafkaSourceContractV0, KafkaTopicIdV0,
};
use decant_kafka_live::{
    KAFKA_RESIDENT_LIFECYCLE_VERSION_V0, KafkaResidentBootstrapErrorV0,
    KafkaResidentCompilerErrorV0, KafkaResidentLifecycleConfigErrorV0,
    KafkaResidentLifecycleConfigV0, KafkaResidentLifecycleFieldV0, KafkaResidentOperationErrorV0,
    KafkaResidentSessionV0, KafkaResidentStatusV0, MAX_KAFKA_RESIDENT_CONTEXT_BYTES_V0,
    MAX_KAFKA_RESIDENT_RECORDS_BETWEEN_CHECKPOINTS_V0,
};
use decant_state::{
    ADMISSION_VERSION_V0, AdmissionConfigV0, AdmissionLimitsV0, AdmissionStateV0,
    EVENT_TIME_RETENTION_VERSION_V0, EventTimeRetentionConfigV0, FinalizeOutcomeV0,
    hash_admission_state_v1,
};
use decant_storage::{LocalCheckpointStoreV0, LocalCheckpointWriterLockErrorV0};

const SECOND_NS: i64 = 1_000_000_000;
static NEXT_TEMP: AtomicU64 = AtomicU64::new(0);

struct TestDirectory(PathBuf);

impl TestDirectory {
    fn new(label: &str) -> Self {
        let sequence = NEXT_TEMP.fetch_add(1, Ordering::Relaxed);
        let path = std::env::temp_dir().join(format!(
            "decant-resident-{label}-{}-{sequence}",
            std::process::id()
        ));
        fs::create_dir(&path).unwrap();
        Self(path)
    }

    fn path(&self) -> &Path {
        &self.0
    }
}

impl Drop for TestDirectory {
    fn drop(&mut self) {
        fs::remove_dir_all(&self.0).unwrap();
    }
}

fn mapping() -> KafkaJsonObjectMappingV1 {
    let base = KafkaJsonObjectMappingV0::new(
        0,
        SourceIdV0::new("private-resident-mapping").unwrap(),
        "time_ns",
        KafkaJsonEventTimeUnitV0::Nanoseconds,
        vec![
            KafkaJsonObjectProjectionV0::new("device", "device", KafkaJsonScalarKindV0::Utf8)
                .unwrap(),
            KafkaJsonObjectProjectionV0::new("reading", "reading", KafkaJsonScalarKindV0::U64)
                .unwrap(),
        ],
    )
    .unwrap();
    KafkaJsonObjectMappingV1::new(
        1,
        base,
        KafkaJsonEntityKeySourceV1::utf8_projection("device").unwrap(),
    )
    .unwrap()
}

fn source_contract(mapping: &KafkaJsonObjectMappingV1) -> KafkaSourceContractV0 {
    KafkaSourceContractV0::new(
        0,
        mapping.effective_source_id().clone(),
        KafkaClusterIdV0::new("private-resident-cluster").unwrap(),
        KafkaTopicIdV0::from_bytes([0x91; 16]).unwrap(),
    )
    .unwrap()
}

fn admission_config(mapping: &KafkaJsonObjectMappingV1) -> AdmissionConfigV0 {
    admission_config_for_epoch(mapping, 0, 300 * SECOND_NS)
}

fn admission_config_for_epoch(
    mapping: &KafkaJsonObjectMappingV1,
    start_ns: i64,
    end_ns: i64,
) -> AdmissionConfigV0 {
    AdmissionConfigV0::new_with_source_position_kind(
        ADMISSION_VERSION_V0,
        mapping.effective_source_id().clone(),
        SourcePositionKindV0::Kafka,
        start_ns,
        end_ns,
        AdmissionLimitsV0::maximum(),
    )
    .unwrap()
}

fn lifecycle(
    records_between_checkpoints: usize,
    idle_observations_between_checkpoints: usize,
    retained_checkpoint_generations: usize,
) -> KafkaResidentLifecycleConfigV0 {
    KafkaResidentLifecycleConfigV0::new(
        KAFKA_RESIDENT_LIFECYCLE_VERSION_V0,
        records_between_checkpoints,
        idle_observations_between_checkpoints,
        retained_checkpoint_generations,
        EventTimeRetentionConfigV0::new(EVENT_TIME_RETENTION_VERSION_V0, 20 * SECOND_NS as u64)
            .unwrap(),
    )
    .unwrap()
}

fn record(offset: i64, time_ns: i64, reading: u64) -> KafkaFetchedRecordV0 {
    let value = format!("{{\"time_ns\":{time_ns},\"device\":\"pump-a\",\"reading\":{reading}}}");
    KafkaFetchedRecordV0::new(
        KafkaPositionV0::new(0, offset).unwrap(),
        None,
        Some(value.as_bytes()),
        KafkaRecordTimestampV0::NotAvailable,
        &[],
    )
    .unwrap()
}

fn compile_state_hash(state: &AdmissionStateV0) -> Result<Vec<u8>, KafkaResidentCompilerErrorV0> {
    Ok(hash_admission_state_v1(state)
        .unwrap()
        .to_canonical_string()
        .into_bytes())
}

fn bootstrap(
    directory: &TestDirectory,
    mapping: &KafkaJsonObjectMappingV1,
    config: AdmissionConfigV0,
    lifecycle: KafkaResidentLifecycleConfigV0,
) -> Result<KafkaResidentSessionV0, KafkaResidentBootstrapErrorV0> {
    KafkaResidentSessionV0::bootstrap(
        LocalCheckpointStoreV0::open(directory.path()).unwrap(),
        config,
        source_contract(mapping),
        &[KafkaPositionV0::new(0, 0).unwrap()],
        lifecycle,
        compile_state_hash,
    )
}

#[test]
fn lifecycle_config_is_strictly_versioned_and_bounded() {
    let retention = EventTimeRetentionConfigV0::new(EVENT_TIME_RETENTION_VERSION_V0, 1).unwrap();
    assert_eq!(
        KafkaResidentLifecycleConfigV0::new(1, 1, 1, 1, retention).unwrap_err(),
        KafkaResidentLifecycleConfigErrorV0::UnsupportedVersion { actual: 1 }
    );
    assert_eq!(
        KafkaResidentLifecycleConfigV0::new(0, 0, 1, 1, retention).unwrap_err(),
        KafkaResidentLifecycleConfigErrorV0::Zero {
            field: KafkaResidentLifecycleFieldV0::RecordsBetweenCheckpoints
        }
    );
    assert_eq!(
        KafkaResidentLifecycleConfigV0::new(
            0,
            MAX_KAFKA_RESIDENT_RECORDS_BETWEEN_CHECKPOINTS_V0 + 1,
            1,
            1,
            retention,
        )
        .unwrap_err(),
        KafkaResidentLifecycleConfigErrorV0::AboveMaximum {
            field: KafkaResidentLifecycleFieldV0::RecordsBetweenCheckpoints,
            maximum: MAX_KAFKA_RESIDENT_RECORDS_BETWEEN_CHECKPOINTS_V0,
            actual: MAX_KAFKA_RESIDENT_RECORDS_BETWEEN_CHECKPOINTS_V0 + 1,
        }
    );
}

#[test]
fn record_and_idle_cadence_publish_compact_prune_and_recover_exact_context() {
    let directory = TestDirectory::new("cadence");
    let mapping = mapping();
    let admission = admission_config(&mapping);
    let resident_config = lifecycle(2, 2, 1);
    let mut resident = bootstrap(&directory, &mapping, admission.clone(), resident_config).unwrap();
    let empty_context = resident.context().unwrap().as_bytes().to_vec();

    let first = resident
        .process_json_object_record_v1(&mapping, &record(0, 10 * SECOND_NS, 10), compile_state_hash)
        .unwrap();
    assert!(first.checkpoint().is_none());
    assert_ne!(resident.context().unwrap().as_bytes(), empty_context);
    assert!(resident.has_unpublished_progress());

    let second = resident
        .process_json_object_record_v1(&mapping, &record(1, 50 * SECOND_NS, 20), compile_state_hash)
        .unwrap();
    assert_eq!(second.checkpoint().unwrap().generation().unwrap().get(), 1);
    assert_eq!(second.pruning().unwrap().removed_generations(), 0);
    assert!(!resident.has_unpublished_progress());

    let third = resident
        .process_json_object_record_v1(
            &mapping,
            &record(2, 100 * SECOND_NS, 30),
            compile_state_hash,
        )
        .unwrap();
    assert!(third.checkpoint().is_none());
    assert!(resident.observe_idle(compile_state_hash).unwrap().is_none());
    let idle_checkpoint = resident.observe_idle(compile_state_hash).unwrap().unwrap();
    assert_eq!(idle_checkpoint.generation().unwrap().get(), 2);
    assert_eq!(
        resident.session().admission().usage().accepted_identities(),
        2
    );
    assert_eq!(resident.usage().physically_removed_identities(), 1);
    assert_eq!(resident.usage().pruned_checkpoint_generations(), 1);
    assert_eq!(
        LocalCheckpointStoreV0::open(directory.path())
            .unwrap()
            .catalog()
            .unwrap()
            .published()
            .len(),
        1
    );
    assert!(resident.observe_idle(compile_state_hash).unwrap().is_none());
    assert!(resident.observe_idle(compile_state_hash).unwrap().is_none());
    assert_eq!(resident.usage().contexts_compiled(), 5);
    assert_eq!(resident.usage().published_checkpoints(), 2);
    let recovered_context = resident.context().unwrap().as_bytes().to_vec();
    assert_eq!(
        resident.graceful_shutdown(compile_state_hash).unwrap(),
        None
    );
    assert_eq!(resident.status(), KafkaResidentStatusV0::Stopped);
    assert_eq!(
        resident.graceful_shutdown(|_| unreachable!()).unwrap(),
        None
    );
    drop(resident);

    let recovered = bootstrap(&directory, &mapping, admission, resident_config).unwrap();
    assert_eq!(recovered.context().unwrap().as_bytes(), recovered_context);
    assert_eq!(recovered.session().latest_generation().unwrap().get(), 2);
}

#[test]
fn writer_lock_and_terminal_compiler_failure_stop_further_intake() {
    let directory = TestDirectory::new("fault");
    let mapping = mapping();
    let admission = admission_config(&mapping);
    let resident_config = lifecycle(10, 2, 2);
    let mut resident = bootstrap(&directory, &mapping, admission.clone(), resident_config).unwrap();
    assert_eq!(
        bootstrap(&directory, &mapping, admission.clone(), resident_config).unwrap_err(),
        KafkaResidentBootstrapErrorV0::WriterLock(LocalCheckpointWriterLockErrorV0::AlreadyHeld)
    );

    assert_eq!(
        resident
            .process_json_object_record_v1(&mapping, &record(0, SECOND_NS, 1), |_| {
                Err(KafkaResidentCompilerErrorV0::Refused)
            })
            .unwrap_err(),
        KafkaResidentOperationErrorV0::Compiler(KafkaResidentCompilerErrorV0::Refused)
    );
    assert_eq!(resident.status(), KafkaResidentStatusV0::Faulted);
    assert!(resident.context().is_none());
    assert!(resident.has_unpublished_progress());
    assert_eq!(
        resident
            .process_json_object_record_v1(
                &mapping,
                &record(1, 2 * SECOND_NS, 2),
                compile_state_hash,
            )
            .unwrap_err(),
        KafkaResidentOperationErrorV0::Faulted
    );
    assert_eq!(
        resident.graceful_shutdown(compile_state_hash).unwrap_err(),
        KafkaResidentOperationErrorV0::Faulted
    );
    assert!(
        LocalCheckpointStoreV0::open(directory.path())
            .unwrap()
            .catalog()
            .unwrap()
            .published()
            .is_empty()
    );
    drop(resident);

    let restarted = bootstrap(&directory, &mapping, admission, resident_config).unwrap();
    assert_eq!(restarted.session().frontier().next_offset(0), Some(0));
    assert_eq!(
        restarted
            .session()
            .admission()
            .usage()
            .accepted_identities(),
        0
    );
}

#[test]
fn graceful_shutdown_and_explicit_epoch_rollover_are_persisted() {
    let directory = TestDirectory::new("rollover");
    let mapping = mapping();
    let first_epoch = admission_config(&mapping);
    let resident_config = lifecycle(10, 10, 2);
    let mut shutdown =
        bootstrap(&directory, &mapping, first_epoch.clone(), resident_config).unwrap();
    shutdown
        .process_json_object_record_v1(&mapping, &record(0, 10 * SECOND_NS, 10), compile_state_hash)
        .unwrap();
    let checkpoint = shutdown
        .graceful_shutdown(compile_state_hash)
        .unwrap()
        .unwrap();
    assert_eq!(checkpoint.generation().unwrap().get(), 1);
    assert!(shutdown.is_stopped());
    drop(shutdown);

    let mut resident = bootstrap(&directory, &mapping, first_epoch, resident_config).unwrap();
    assert_eq!(
        resident.finalize_current_epoch(compile_state_hash).unwrap(),
        FinalizeOutcomeV0::Finalized
    );
    assert!(resident.has_unpublished_progress());
    let next_epoch = admission_config_for_epoch(&mapping, 300 * SECOND_NS, 600 * SECOND_NS);
    let rollover = resident
        .rollover_adjacent_epoch(next_epoch.clone(), compile_state_hash)
        .unwrap();
    assert_eq!(rollover.generation().unwrap().get(), 2);
    assert_eq!(resident.session().admission().config(), &next_epoch);
    assert_eq!(
        resident.session().admission().usage().accepted_identities(),
        0
    );
    assert!(!resident.has_unpublished_progress());
    let rollover_context = resident.context().unwrap().as_bytes().to_vec();
    drop(resident);

    let recovered = bootstrap(&directory, &mapping, next_epoch, resident_config).unwrap();
    assert_eq!(recovered.context().unwrap().as_bytes(), rollover_context);
    assert_eq!(recovered.session().latest_generation().unwrap().get(), 2);
}

#[test]
fn failed_finalization_compile_is_atomic_and_oversized_bootstrap_releases_lock() {
    let directory = TestDirectory::new("atomic");
    let mapping = mapping();
    let admission = admission_config(&mapping);
    let resident_config = lifecycle(10, 10, 2);
    let mut resident = bootstrap(&directory, &mapping, admission.clone(), resident_config).unwrap();
    assert_eq!(
        resident
            .finalize_current_epoch(|_| Err(KafkaResidentCompilerErrorV0::Refused))
            .unwrap_err(),
        KafkaResidentOperationErrorV0::Compiler(KafkaResidentCompilerErrorV0::Refused)
    );
    assert!(!resident.session().admission().is_finalized());
    drop(resident);

    assert_eq!(
        KafkaResidentSessionV0::bootstrap(
            LocalCheckpointStoreV0::open(directory.path()).unwrap(),
            admission.clone(),
            source_contract(&mapping),
            &[KafkaPositionV0::new(0, 0).unwrap()],
            resident_config,
            |_| Ok(vec![0; MAX_KAFKA_RESIDENT_CONTEXT_BYTES_V0 + 1]),
        )
        .unwrap_err(),
        KafkaResidentBootstrapErrorV0::ContextTooLarge {
            maximum: MAX_KAFKA_RESIDENT_CONTEXT_BYTES_V0,
            actual: MAX_KAFKA_RESIDENT_CONTEXT_BYTES_V0 + 1,
        }
    );
    assert!(bootstrap(&directory, &mapping, admission, resident_config).is_ok());
}
