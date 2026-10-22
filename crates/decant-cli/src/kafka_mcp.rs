use std::{
    env,
    ffi::OsStr,
    fmt,
    io::{self, Write},
    path::Path,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
        mpsc,
    },
    thread,
};

use decant_compiler::{
    KafkaMaterialChangeContextV0, KafkaMaterialChangeRequestV0,
    compile_kafka_material_change_context_v1,
};
use decant_ir::KafkaPositionV0;
use decant_kafka::{KafkaFetchedRecordV0, KafkaJsonObjectMappingV1, KafkaSourceContractV0};
use decant_kafka_client::{
    KafkaStaticConsumerV0, KafkaStaticPollV0, observe_and_validate_kafka_source_v0,
};
use decant_kafka_live::{
    KafkaResidentBootstrapErrorV0, KafkaResidentCompilerErrorV0, KafkaResidentLifecycleConfigV0,
    KafkaResidentProcessV0, KafkaResidentSessionV0, KafkaResidentUsageV0,
    KafkaStaticSessionUsageV0,
};
use decant_mcp::{
    DecantMcpBoundedTransportV0, DecantMcpServerV0, DecantMcpUsageV0, DecantMcpViewV0,
};
use decant_state::AdmissionStateV0;
use decant_storage::{LocalCheckpointStoreV0, LocalCheckpointWriterLockErrorV0};
use rmcp::ServiceExt;
use tokio::sync::oneshot;

use crate::kafka_json::{KafkaMcpServiceConfigV0, load_kafka_mcp_service_config_v0};

pub(crate) const KAFKA_MCP_DIAGNOSTIC_FORMAT_V0: &str = "decant-kafka-mcp-service-diagnostic";

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum KafkaMcpCliErrorV0 {
    Config,
    CheckpointStore,
    CheckpointInUse,
    ResidentBootstrap,
    Context,
    ClientCreation,
    SourceObservation,
    Assignment,
    Thread,
    Runtime,
    Mcp,
    Worker,
    Diagnostic,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum KafkaMcpWorkerFaultV0 {
    Poll,
    Processing,
    Context,
    Accounting,
    Diagnostic,
    Shutdown,
}

impl KafkaMcpWorkerFaultV0 {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Poll => "poll",
            Self::Processing => "processing",
            Self::Context => "context",
            Self::Accounting => "accounting",
            Self::Diagnostic => "diagnostic",
            Self::Shutdown => "shutdown",
        }
    }
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
struct KafkaMcpPollUsageV0 {
    data_records: usize,
    idle_observations: usize,
    partition_eof_observations: usize,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct KafkaMcpWorkerSnapshotV0 {
    poll: KafkaMcpPollUsageV0,
    admission: KafkaStaticSessionUsageV0,
    resident: KafkaResidentUsageV0,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum KafkaMcpWorkerOutcomeV0 {
    Stopped(KafkaMcpWorkerSnapshotV0),
    Faulted {
        fault: KafkaMcpWorkerFaultV0,
        snapshot: KafkaMcpWorkerSnapshotV0,
    },
}

impl KafkaMcpWorkerOutcomeV0 {
    const fn snapshot(self) -> KafkaMcpWorkerSnapshotV0 {
        match self {
            Self::Stopped(snapshot) | Self::Faulted { snapshot, .. } => snapshot,
        }
    }
}

struct KafkaMcpResidentV0 {
    session: KafkaResidentSessionV0,
    mapping: KafkaJsonObjectMappingV1,
    request: KafkaMaterialChangeRequestV0,
    view: DecantMcpViewV0,
}

impl KafkaMcpResidentV0 {
    fn bootstrap(
        config: KafkaMcpServiceConfigV0,
    ) -> Result<(Self, KafkaStaticConsumerV0), KafkaMcpCliErrorV0> {
        let KafkaMcpServiceConfigV0 {
            source,
            admission,
            checkpoint_directory,
            poll_timeout,
            mapping,
            context,
            lifecycle,
            security,
        } = config;
        let source_contract = source.source_contract().clone();
        let starting_positions = source.starting_positions().to_vec();
        let resident = Self::bootstrap_state(
            &checkpoint_directory,
            admission,
            source_contract.clone(),
            &starting_positions,
            lifecycle,
            mapping,
            context,
        )?;
        let consumer =
            KafkaStaticConsumerV0::new_with_security_resolver(source, security, |name| {
                env::var(name).ok()
            })
            .map_err(|_| KafkaMcpCliErrorV0::ClientCreation)?;
        observe_and_validate_kafka_source_v0(
            consumer.native_consumer(),
            consumer.config().topic_name(),
            &source_contract,
            source_contract.source_id(),
            poll_timeout,
        )
        .map_err(|_| KafkaMcpCliErrorV0::SourceObservation)?;
        consumer
            .assign_frontier(resident.session.session().frontier())
            .map_err(|_| KafkaMcpCliErrorV0::Assignment)?;
        Ok((resident, consumer))
    }

    #[allow(clippy::too_many_arguments)]
    fn bootstrap_state(
        checkpoint_directory: &Path,
        admission: decant_state::AdmissionConfigV0,
        source_contract: KafkaSourceContractV0,
        starting_positions: &[KafkaPositionV0],
        lifecycle: KafkaResidentLifecycleConfigV0,
        mapping: KafkaJsonObjectMappingV1,
        context: KafkaMaterialChangeRequestV0,
    ) -> Result<Self, KafkaMcpCliErrorV0> {
        let store = LocalCheckpointStoreV0::open(checkpoint_directory)
            .map_err(|_| KafkaMcpCliErrorV0::CheckpointStore)?;
        let mut initial = None;
        let session = KafkaResidentSessionV0::bootstrap(
            store,
            admission,
            source_contract,
            starting_positions,
            lifecycle,
            |state| compile_for_resident_v0(state, &context, &mut initial),
        )
        .map_err(|error| match error {
            KafkaResidentBootstrapErrorV0::WriterLock(
                LocalCheckpointWriterLockErrorV0::AlreadyHeld,
            ) => KafkaMcpCliErrorV0::CheckpointInUse,
            _ => KafkaMcpCliErrorV0::ResidentBootstrap,
        })?;
        let initial = initial.ok_or(KafkaMcpCliErrorV0::Context)?;
        let view = DecantMcpViewV0::new(context.view_id(), &initial)
            .map_err(|_| KafkaMcpCliErrorV0::Context)?;
        Ok(Self {
            session,
            mapping,
            request: context,
            view,
        })
    }

    fn process_record(
        &mut self,
        record: &KafkaFetchedRecordV0,
    ) -> Result<KafkaResidentProcessV0, KafkaMcpWorkerFaultV0> {
        let mut compiled = None;
        let result = self
            .session
            .process_json_object_record_v1(&self.mapping, record, |state| {
                compile_for_resident_v0(state, &self.request, &mut compiled)
            });
        let process = result.map_err(|_| {
            self.hide_context();
            KafkaMcpWorkerFaultV0::Processing
        })?;
        self.publish_compiled(compiled)?;
        Ok(process)
    }

    fn observe_idle(&mut self) -> Result<bool, KafkaMcpWorkerFaultV0> {
        let mut compiled = None;
        let checkpoint = self
            .session
            .observe_idle(|state| compile_for_resident_v0(state, &self.request, &mut compiled))
            .map_err(|_| {
                self.hide_context();
                KafkaMcpWorkerFaultV0::Processing
            })?;
        if compiled.is_some() {
            self.publish_compiled(compiled)?;
        }
        Ok(checkpoint.is_some())
    }

    fn graceful_shutdown(&mut self) -> Result<(), KafkaMcpWorkerFaultV0> {
        let mut compiled = None;
        self.session
            .graceful_shutdown(|state| compile_for_resident_v0(state, &self.request, &mut compiled))
            .map_err(|_| KafkaMcpWorkerFaultV0::Shutdown)?;
        if compiled.is_some() {
            self.publish_compiled(compiled)?;
        }
        Ok(())
    }

    fn publish_compiled(
        &self,
        compiled: Option<KafkaMaterialChangeContextV0>,
    ) -> Result<(), KafkaMcpWorkerFaultV0> {
        let context = compiled.ok_or(KafkaMcpWorkerFaultV0::Context)?;
        self.view.publish(&context).map_err(|_| {
            self.hide_context();
            KafkaMcpWorkerFaultV0::Context
        })
    }

    fn hide_context(&self) {
        let _ = self.view.mark_unavailable();
    }

    fn snapshot(&self, poll: KafkaMcpPollUsageV0) -> KafkaMcpWorkerSnapshotV0 {
        KafkaMcpWorkerSnapshotV0 {
            poll,
            admission: self.session.session().usage(),
            resident: self.session.usage(),
        }
    }
}

fn compile_for_resident_v0(
    state: &AdmissionStateV0,
    request: &KafkaMaterialChangeRequestV0,
    output: &mut Option<KafkaMaterialChangeContextV0>,
) -> Result<Vec<u8>, KafkaResidentCompilerErrorV0> {
    let context = compile_kafka_material_change_context_v1(state, request)
        .map_err(|_| KafkaResidentCompilerErrorV0::Refused)?;
    let mut bytes = Vec::new();
    bytes
        .try_reserve_exact(context.as_bytes().len())
        .map_err(|_| KafkaResidentCompilerErrorV0::Refused)?;
    bytes.extend_from_slice(context.as_bytes());
    *output = Some(context);
    Ok(bytes)
}

pub(crate) fn run_kafka_mcp_config_file_v0(path: &OsStr) -> Result<(), KafkaMcpCliErrorV0> {
    let config = load_kafka_mcp_service_config_v0(path).map_err(|_| KafkaMcpCliErrorV0::Config)?;
    run_kafka_mcp_service_v0(config)
}

fn run_kafka_mcp_service_v0(config: KafkaMcpServiceConfigV0) -> Result<(), KafkaMcpCliErrorV0> {
    let poll_timeout = config.poll_timeout;
    let (resident, consumer) = KafkaMcpResidentV0::bootstrap(config)?;
    let view = resident.view.clone();
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|_| KafkaMcpCliErrorV0::Runtime)?;
    let signal = {
        let _entered = runtime.enter();
        crate::lifecycle::Shutdown::new().map_err(|_| KafkaMcpCliErrorV0::Runtime)?
    };
    let stop = Arc::new(AtomicBool::new(false));
    let worker_stop = Arc::clone(&stop);
    let (worker_sender, worker_receiver) = oneshot::channel();
    let (start_sender, start_receiver) = mpsc::sync_channel(0);
    let worker = thread::Builder::new()
        .name("decant-kafka-static-v0".to_owned())
        .spawn(move || {
            if start_receiver.recv().is_err() {
                return KafkaMcpWorkerOutcomeV0::Stopped(
                    resident.snapshot(KafkaMcpPollUsageV0::default()),
                );
            }
            let outcome = run_kafka_worker_v0(resident, consumer, poll_timeout, &worker_stop);
            let _ = worker_sender.send(outcome);
            outcome
        })
        .map_err(|_| KafkaMcpCliErrorV0::Thread)?;

    if let Err(error) = emit_ready_diagnostic_v0() {
        stop.store(true, Ordering::Release);
        drop(start_sender);
        let _ = worker.join();
        return Err(error);
    }
    if start_sender.send(()).is_err() {
        stop.store(true, Ordering::Release);
        let _ = worker.join();
        return Err(KafkaMcpCliErrorV0::Worker);
    }
    let server = DecantMcpServerV0::new(view);
    let usage_server = server.clone();
    let (mcp_result, early_worker) =
        runtime.block_on(run_mcp_stdio_v0(server, worker_receiver, signal));
    stop.store(true, Ordering::Release);
    let joined = crate::lifecycle::join(worker, crate::lifecycle::SHUTDOWN_DEADLINE);
    runtime.shutdown_background();
    let joined = joined.map_err(|_| KafkaMcpCliErrorV0::Worker)?;
    let worker_outcome = early_worker.unwrap_or(joined);
    emit_terminal_diagnostic_v0(worker_outcome, usage_server.usage(), mcp_result.is_ok())?;
    mcp_result?;
    if matches!(worker_outcome, KafkaMcpWorkerOutcomeV0::Faulted { .. }) {
        return Err(KafkaMcpCliErrorV0::Worker);
    }
    Ok(())
}

async fn run_mcp_stdio_v0(
    server: DecantMcpServerV0,
    mut worker: oneshot::Receiver<KafkaMcpWorkerOutcomeV0>,
    mut signal: crate::lifecycle::Shutdown,
) -> (
    Result<(), KafkaMcpCliErrorV0>,
    Option<KafkaMcpWorkerOutcomeV0>,
) {
    let output = match crate::lifecycle::stdout() {
        Ok(output) => output,
        Err(_) => return (Err(KafkaMcpCliErrorV0::Mcp), None),
    };
    let transport = DecantMcpBoundedTransportV0::new(tokio::io::stdin(), output);
    let initialize = server.serve(transport);
    tokio::pin!(initialize);
    let running = tokio::select! {
        result = &mut initialize => match result {
            Ok(running) => running,
            Err(_) => return (Err(KafkaMcpCliErrorV0::Mcp), None),
        },
        outcome = &mut worker => return match outcome {
            Ok(outcome) => (Err(KafkaMcpCliErrorV0::Worker), Some(outcome)),
            Err(_) => (Err(KafkaMcpCliErrorV0::Worker), None),
        },
        result = signal.wait() => return (result.map_err(|_| KafkaMcpCliErrorV0::Runtime), None),
    };
    let cancellation = running.cancellation_token();
    let waiting = running.waiting();
    tokio::pin!(waiting);
    tokio::select! {
        result = &mut waiting => (
            result.map(|_| ()).map_err(|_| KafkaMcpCliErrorV0::Mcp),
            None,
        ),
        outcome = &mut worker => {
            cancellation.cancel();
            match outcome {
                Ok(outcome) => (Err(KafkaMcpCliErrorV0::Worker), Some(outcome)),
                Err(_) => (Err(KafkaMcpCliErrorV0::Worker), None),
            }
        },
        result = signal.wait() => { cancellation.cancel(); (result.map_err(|_| KafkaMcpCliErrorV0::Runtime), None) },
    }
}

fn run_kafka_worker_v0(
    mut resident: KafkaMcpResidentV0,
    consumer: KafkaStaticConsumerV0,
    poll_timeout: std::time::Duration,
    stop: &AtomicBool,
) -> KafkaMcpWorkerOutcomeV0 {
    let mut poll = KafkaMcpPollUsageV0::default();
    while !stop.load(Ordering::Acquire) {
        let observation = match consumer.poll(poll_timeout) {
            Ok(observation) => observation,
            Err(_) => return fault_worker_v0(&mut resident, poll, KafkaMcpWorkerFaultV0::Poll),
        };
        let result = match observation {
            KafkaStaticPollV0::Data(record) => {
                poll.data_records = match poll.data_records.checked_add(1) {
                    Some(value) => value,
                    None => {
                        return fault_worker_v0(
                            &mut resident,
                            poll,
                            KafkaMcpWorkerFaultV0::Accounting,
                        );
                    }
                };
                resident
                    .process_record(&record)
                    .map(|process| process.checkpoint().is_some())
            }
            KafkaStaticPollV0::Idle => {
                poll.idle_observations = match poll.idle_observations.checked_add(1) {
                    Some(value) => value,
                    None => {
                        return fault_worker_v0(
                            &mut resident,
                            poll,
                            KafkaMcpWorkerFaultV0::Accounting,
                        );
                    }
                };
                resident.observe_idle()
            }
            KafkaStaticPollV0::PartitionEof { .. } => {
                poll.partition_eof_observations =
                    match poll.partition_eof_observations.checked_add(1) {
                        Some(value) => value,
                        None => {
                            return fault_worker_v0(
                                &mut resident,
                                poll,
                                KafkaMcpWorkerFaultV0::Accounting,
                            );
                        }
                    };
                resident.observe_idle()
            }
        };
        let checkpointed = match result {
            Ok(checkpointed) => checkpointed,
            Err(fault) => return fault_worker_v0(&mut resident, poll, fault),
        };
        if checkpointed && emit_checkpoint_diagnostic_v0(resident.snapshot(poll)).is_err() {
            return fault_worker_v0(&mut resident, poll, KafkaMcpWorkerFaultV0::Diagnostic);
        }
    }
    match resident.graceful_shutdown() {
        Ok(()) => KafkaMcpWorkerOutcomeV0::Stopped(resident.snapshot(poll)),
        Err(fault) => {
            resident.hide_context();
            KafkaMcpWorkerOutcomeV0::Faulted {
                fault,
                snapshot: resident.snapshot(poll),
            }
        }
    }
}

fn fault_worker_v0(
    resident: &mut KafkaMcpResidentV0,
    poll: KafkaMcpPollUsageV0,
    fault: KafkaMcpWorkerFaultV0,
) -> KafkaMcpWorkerOutcomeV0 {
    resident.hide_context();
    if matches!(
        fault,
        KafkaMcpWorkerFaultV0::Poll
            | KafkaMcpWorkerFaultV0::Context
            | KafkaMcpWorkerFaultV0::Accounting
            | KafkaMcpWorkerFaultV0::Diagnostic
    ) {
        let mut compiled = None;
        let _ = resident.session.graceful_shutdown(|state| {
            compile_for_resident_v0(state, &resident.request, &mut compiled)
        });
    }
    KafkaMcpWorkerOutcomeV0::Faulted {
        fault,
        snapshot: resident.snapshot(poll),
    }
}

fn emit_ready_diagnostic_v0() -> Result<(), KafkaMcpCliErrorV0> {
    write_stderr_v0(
        b"{\"format\":\"decant-kafka-mcp-service-diagnostic\",\"version\":0,\"kind\":\"ready\",\"source_ownership\":\"static_partitions\",\"broker_offsets_committed\":false}\n",
    )
}

fn emit_checkpoint_diagnostic_v0(
    snapshot: KafkaMcpWorkerSnapshotV0,
) -> Result<(), KafkaMcpCliErrorV0> {
    let mut bytes = Vec::with_capacity(384);
    writeln!(
        &mut bytes,
        "{{\"format\":\"{}\",\"version\":0,\"kind\":\"checkpoint\",\"data_records\":{},\"accepted_records\":{},\"rejected_records\":{},\"published_checkpoints\":{},\"pruned_checkpoint_generations\":{}}}",
        KAFKA_MCP_DIAGNOSTIC_FORMAT_V0,
        snapshot.poll.data_records,
        snapshot.admission.accepted_records(),
        snapshot.admission.rejected_records(),
        snapshot.resident.published_checkpoints(),
        snapshot.resident.pruned_checkpoint_generations(),
    )
    .map_err(|_| KafkaMcpCliErrorV0::Diagnostic)?;
    write_stderr_v0(&bytes)
}

fn emit_terminal_diagnostic_v0(
    outcome: KafkaMcpWorkerOutcomeV0,
    mcp: DecantMcpUsageV0,
    mcp_clean: bool,
) -> Result<(), KafkaMcpCliErrorV0> {
    let snapshot = outcome.snapshot();
    let (kind, termination) = match (outcome, mcp_clean) {
        (KafkaMcpWorkerOutcomeV0::Stopped(_), true) => ("stopped", "mcp_closed"),
        (KafkaMcpWorkerOutcomeV0::Stopped(_), false) => ("fault", "mcp"),
        (KafkaMcpWorkerOutcomeV0::Faulted { fault, .. }, _) => ("fault", fault.as_str()),
    };
    let mut bytes = Vec::with_capacity(1_024);
    writeln!(
        &mut bytes,
        concat!(
            "{{\"format\":\"{}\",\"version\":0,\"kind\":\"{}\",\"termination\":\"{}\",",
            "\"poll\":{{\"data_records\":{},\"idle_observations\":{},\"partition_eof_observations\":{}}},",
            "\"admission\":{{\"processed\":{},\"accepted\":{},\"duplicate_noops\":{},\"rejected\":{},\"before_frontier\":{}}},",
            "\"resident\":{{\"contexts_compiled\":{},\"published_checkpoints\":{},\"pruned_checkpoint_generations\":{},",
            "\"physically_removed_identities\":{},\"physically_removed_event_bytes\":{}}},",
            "\"mcp\":{{\"tool_calls\":{},\"successful_tool_calls\":{},\"rejected_tool_calls\":{},",
            "\"snapshot_calls\":{},\"material_change_calls\":{},\"provenance_calls\":{}}},",
            "\"source_ownership\":\"static_partitions\",\"broker_offsets_committed\":false}}"
        ),
        KAFKA_MCP_DIAGNOSTIC_FORMAT_V0,
        kind,
        termination,
        snapshot.poll.data_records,
        snapshot.poll.idle_observations,
        snapshot.poll.partition_eof_observations,
        snapshot.admission.processed_records(),
        snapshot.admission.accepted_records(),
        snapshot.admission.duplicate_noop_records(),
        snapshot.admission.rejected_records(),
        snapshot.admission.before_frontier_records(),
        snapshot.resident.contexts_compiled(),
        snapshot.resident.published_checkpoints(),
        snapshot.resident.pruned_checkpoint_generations(),
        snapshot.resident.physically_removed_identities(),
        snapshot.resident.physically_removed_event_bytes(),
        mcp.tool_calls,
        mcp.successful_tool_calls,
        mcp.rejected_tool_calls,
        mcp.snapshot_calls,
        mcp.material_change_calls,
        mcp.provenance_calls,
    )
    .map_err(|_| KafkaMcpCliErrorV0::Diagnostic)?;
    write_stderr_v0(&bytes)
}

fn write_stderr_v0(bytes: &[u8]) -> Result<(), KafkaMcpCliErrorV0> {
    io::stderr()
        .lock()
        .write_all(bytes)
        .map_err(|_| KafkaMcpCliErrorV0::Diagnostic)
}

impl fmt::Display for KafkaMcpCliErrorV0 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(self, formatter)
    }
}

#[cfg(test)]
mod tests {
    use std::{
        fs,
        path::Path,
        sync::atomic::{AtomicUsize, Ordering},
    };

    use decant_kafka::KafkaRecordTimestampV0;
    use decant_kafka_client::{
        KafkaSaslScramMechanismV0, KafkaSecretEnvironmentReferenceV0, KafkaStaticSecurityConfigV0,
        KafkaTlsTrustV0,
    };
    use decant_storage::{
        LOCAL_CHECKPOINT_WRITER_LOCK_FILE_V0, acquire_local_checkpoint_writer_lock_v0,
    };

    use super::*;

    static NEXT_TEMP: AtomicUsize = AtomicUsize::new(0);

    struct TestDirectory(std::path::PathBuf);

    impl TestDirectory {
        fn new(label: &str) -> Self {
            let sequence = NEXT_TEMP.fetch_add(1, Ordering::Relaxed);
            let directory = std::env::temp_dir().join(format!(
                "decant-kafka-mcp-{label}-{}-{sequence}",
                std::process::id()
            ));
            fs::create_dir(&directory).unwrap();
            Self(directory)
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

    fn service_config(
        directory: &Path,
        records_between_checkpoints: usize,
        retention_ns: u64,
    ) -> KafkaMcpServiceConfigV0 {
        let mut document: serde_json::Value =
            serde_json::from_slice(&fs::read("../../examples/kafka-mcp/config-v0.json").unwrap())
                .unwrap();
        document["checkpoint_directory"] = serde_json::json!(directory);
        document["security"] = serde_json::json!({"mode":"plaintext","version":0});
        document["lifecycle"]["records_between_checkpoints"] =
            serde_json::json!(records_between_checkpoints);
        document["context"]["retention_ns"] = serde_json::json!(retention_ns);
        crate::kafka_json::parse_kafka_mcp_service_config_v0(
            &serde_json::to_vec(&document).unwrap(),
        )
        .unwrap()
    }

    fn bootstrap_test_resident(config: KafkaMcpServiceConfigV0) -> KafkaMcpResidentV0 {
        let source_contract = config.source.source_contract().clone();
        let starts = config.source.starting_positions().to_vec();
        KafkaMcpResidentV0::bootstrap_state(
            &config.checkpoint_directory,
            config.admission,
            source_contract,
            &starts,
            config.lifecycle,
            config.mapping,
            config.context,
        )
        .unwrap()
    }

    fn test_record(offset: i64, value: u64) -> KafkaFetchedRecordV0 {
        test_record_at(offset, value, 1_000_000_000_i64 + offset)
    }

    fn test_record_at(offset: i64, value: u64, event_time_ns: i64) -> KafkaFetchedRecordV0 {
        let bytes = format!(
            "{{\"service_name\":\"checkout\",\"metric_name\":\"latency\",\"metric_value\":{value},\"authorization\":\"private-denied-sentinel\",\"observed_time_unix_nano\":{}}}",
            event_time_ns
        );
        KafkaFetchedRecordV0::new(
            KafkaPositionV0::new(0, offset).unwrap(),
            None,
            Some(bytes.as_bytes()),
            KafkaRecordTimestampV0::NotAvailable,
            &[],
        )
        .unwrap()
    }

    #[test]
    fn error_and_fault_diagnostics_are_value_safe() {
        for error in [
            KafkaMcpCliErrorV0::Config,
            KafkaMcpCliErrorV0::CheckpointStore,
            KafkaMcpCliErrorV0::CheckpointInUse,
            KafkaMcpCliErrorV0::ResidentBootstrap,
            KafkaMcpCliErrorV0::Context,
            KafkaMcpCliErrorV0::ClientCreation,
            KafkaMcpCliErrorV0::SourceObservation,
            KafkaMcpCliErrorV0::Assignment,
            KafkaMcpCliErrorV0::Thread,
            KafkaMcpCliErrorV0::Runtime,
            KafkaMcpCliErrorV0::Mcp,
            KafkaMcpCliErrorV0::Worker,
            KafkaMcpCliErrorV0::Diagnostic,
        ] {
            assert!(!format!("{error:?} {error}").contains("private"));
        }
        for fault in [
            KafkaMcpWorkerFaultV0::Poll,
            KafkaMcpWorkerFaultV0::Processing,
            KafkaMcpWorkerFaultV0::Context,
            KafkaMcpWorkerFaultV0::Accounting,
            KafkaMcpWorkerFaultV0::Diagnostic,
            KafkaMcpWorkerFaultV0::Shutdown,
        ] {
            assert!(!format!("{fault:?}").contains("private"));
        }
    }

    #[test]
    fn diagnostic_format_is_fixed_and_contains_no_paths_or_values() {
        assert_eq!(
            KAFKA_MCP_DIAGNOSTIC_FORMAT_V0,
            "decant-kafka-mcp-service-diagnostic"
        );
        let manifest = include_str!("../Cargo.toml");
        assert!(!manifest.contains("tracing-subscriber"));
    }

    #[test]
    fn repository_example_is_strict_and_contains_references_not_secrets() {
        let bytes = fs::read("../../examples/kafka-mcp/config-v0.json").unwrap();
        let config = crate::kafka_json::parse_kafka_mcp_service_config_v0(&bytes).unwrap();
        assert_eq!(config.context.view_id(), "service-latency");
        let text = std::str::from_utf8(&bytes).unwrap();
        assert!(text.contains("DECANT_KAFKA_USERNAME"));
        assert!(text.contains("DECANT_KAFKA_PASSWORD"));
        assert!(!text.contains("private-password"));
    }

    #[test]
    fn no_unbounded_service_artifact_is_created_by_config_parse() {
        let path = Path::new("../../examples/kafka-mcp/config-v0.json");
        assert!(path.is_file());
    }

    #[test]
    fn resident_pipeline_publishes_exact_policy_filtered_context_and_checkpoint() {
        let directory = TestDirectory::new("pipeline");
        let mut resident =
            bootstrap_test_resident(service_config(directory.path(), 2, 3_600_000_000_000));
        assert!(
            resident
                .process_record(&test_record(0, 10))
                .unwrap()
                .checkpoint()
                .is_none()
        );
        assert!(
            resident
                .process_record(&test_record(1, 20))
                .unwrap()
                .checkpoint()
                .is_some()
        );
        let expected = compile_kafka_material_change_context_v1(
            resident.session.session().admission(),
            &resident.request,
        )
        .unwrap();
        assert_eq!(
            resident.session.context().unwrap().as_bytes(),
            expected.as_bytes()
        );
        assert!(
            !std::str::from_utf8(expected.as_bytes())
                .unwrap()
                .contains("private-denied-sentinel")
        );
        assert_eq!(resident.session.usage().published_checkpoints(), 1);
        assert_eq!(resident.graceful_shutdown(), Ok(()));
    }

    #[test]
    fn abrupt_drop_recovers_last_verified_cut_and_redelivery_rebuilds_exact_context() {
        let directory = TestDirectory::new("abrupt-recovery");
        let retention = 3_600_000_000_000;
        let mut first = bootstrap_test_resident(service_config(directory.path(), 2, retention));
        first.process_record(&test_record(0, 10)).unwrap();
        first.process_record(&test_record(1, 20)).unwrap();
        first.process_record(&test_record(2, 30)).unwrap();
        let pre_kill_context = first.session.context().unwrap().as_bytes().to_vec();
        assert!(first.session.has_unpublished_progress());
        drop(first);

        let mut restarted = bootstrap_test_resident(service_config(directory.path(), 2, retention));
        assert_eq!(
            restarted.session.session().frontier().next_offset(0),
            Some(2)
        );
        assert_ne!(
            restarted.session.context().unwrap().as_bytes(),
            pre_kill_context
        );
        restarted.process_record(&test_record(2, 30)).unwrap();
        assert_eq!(
            restarted.session.context().unwrap().as_bytes(),
            pre_kill_context
        );
        restarted.graceful_shutdown().unwrap();
    }

    #[test]
    fn held_owner_precedes_client_setup_and_release_recovers_the_same_lock_and_cut() {
        let directory = TestDirectory::new("private-ownership-precedence");
        let retention = 3_600_000_000_000;
        let mut first = bootstrap_test_resident(service_config(directory.path(), 2, retention));
        first.process_record(&test_record(0, 10)).unwrap();
        first.process_record(&test_record(1, 20)).unwrap();
        let checkpoint_path = directory.path().join("checkpoint-00000000000000000001.dcp");
        let checkpoint_before = fs::read(&checkpoint_path).unwrap();
        let context_before = first.session.context().unwrap().as_bytes().to_vec();
        let lock_path = directory.path().join(LOCAL_CHECKPOINT_WRITER_LOCK_FILE_V0);
        let lock_before = fs::metadata(&lock_path).unwrap();

        // These process-local test names are never installed in the environment.
        // Missing SCRAM values fail before the native consumer's create() call.
        let sequence = NEXT_TEMP.fetch_add(1, Ordering::Relaxed);
        let username = format!(
            "DECANT_TASK098_ABSENT_USER_{}_{sequence}",
            std::process::id()
        );
        let password = format!(
            "DECANT_TASK098_ABSENT_PASS_{}_{sequence}",
            std::process::id()
        );
        assert!(env::var_os(&username).is_none());
        assert!(env::var_os(&password).is_none());
        let security = KafkaStaticSecurityConfigV0::tls_sasl_scram(
            0,
            KafkaTlsTrustV0::system_roots(),
            KafkaSaslScramMechanismV0::Sha512,
            KafkaSecretEnvironmentReferenceV0::new(&username).unwrap(),
            KafkaSecretEnvironmentReferenceV0::new(&password).unwrap(),
        )
        .unwrap();
        let client_config = || {
            let mut config = service_config(directory.path(), 2, retention);
            config.security = security.clone();
            config
        };

        assert!(matches!(
            KafkaMcpResidentV0::bootstrap(client_config()),
            Err(KafkaMcpCliErrorV0::CheckpointInUse)
        ));
        assert!(fs::read(&checkpoint_path).unwrap() == checkpoint_before);
        assert!(first.session.context().unwrap().as_bytes() == context_before);
        assert_eq!(first.session.session().frontier().next_offset(0), Some(2));
        let store = LocalCheckpointStoreV0::open(directory.path()).unwrap();
        assert!(matches!(
            acquire_local_checkpoint_writer_lock_v0(&store),
            Err(LocalCheckpointWriterLockErrorV0::AlreadyHeld)
        ));

        // The rejected start does not poison the live owner.
        first.process_record(&test_record(2, 30)).unwrap();
        first.graceful_shutdown().unwrap();
        let expected_context = first.session.context().unwrap().as_bytes().to_vec();
        drop(first);

        // With exactly the same config and no owner, recovery succeeds and
        // startup reaches the deliberate client setup failure, without Kafka.
        assert!(matches!(
            KafkaMcpResidentV0::bootstrap(client_config()),
            Err(KafkaMcpCliErrorV0::ClientCreation)
        ));
        let recovered = bootstrap_test_resident(service_config(directory.path(), 2, retention));
        assert!(recovered.session.context().unwrap().as_bytes() == expected_context);
        assert_eq!(
            recovered.session.session().frontier().next_offset(0),
            Some(3)
        );
        let lock_after = fs::metadata(&lock_path).unwrap();
        assert_eq!(lock_before.len(), 0);
        assert_eq!(lock_after.len(), 0);
        #[cfg(unix)]
        {
            use std::os::unix::fs::MetadataExt;
            assert_eq!(lock_before.dev(), lock_after.dev());
            assert_eq!(lock_before.ino(), lock_after.ino());
        }
    }

    #[test]
    fn corrupt_latest_checkpoint_fails_closed_without_context_or_fallback() {
        let directory = TestDirectory::new("corrupt-recovery");
        let retention = 3_600_000_000_000;
        let mut first = bootstrap_test_resident(service_config(directory.path(), 2, retention));
        first.process_record(&test_record(0, 10)).unwrap();
        first.process_record(&test_record(1, 20)).unwrap();
        drop(first);
        fs::write(
            directory.path().join("checkpoint-00000000000000000001.dcp"),
            b"private-corrupt-checkpoint",
        )
        .unwrap();

        let config = service_config(directory.path(), 2, retention);
        let source_contract = config.source.source_contract().clone();
        let starts = config.source.starting_positions().to_vec();
        assert!(matches!(
            KafkaMcpResidentV0::bootstrap_state(
                &config.checkpoint_directory,
                config.admission,
                source_contract,
                &starts,
                config.lifecycle,
                config.mapping,
                config.context,
            ),
            Err(KafkaMcpCliErrorV0::ResidentBootstrap)
        ));
    }

    #[test]
    fn fixed_endurance_trace_bounds_state_context_and_checkpoint_files() {
        const RECORDS: i64 = 500;
        let directory = TestDirectory::new("endurance");
        let mut resident = bootstrap_test_resident(service_config(directory.path(), 10, 10));
        for offset in 0..RECORDS {
            resident
                .process_record(&test_record_at(
                    offset,
                    (offset & 1) as u64,
                    1_000_000_000_i64 + offset * 1_000_000_000_i64,
                ))
                .unwrap();
        }
        assert_eq!(resident.session.usage().published_checkpoints(), 50);
        assert_eq!(
            resident
                .session
                .session()
                .admission()
                .usage()
                .accepted_identities(),
            31
        );
        assert!(resident.session.context().unwrap().as_bytes().len() <= 16 * 1_024);
        assert!(
            !std::str::from_utf8(resident.session.context().unwrap().as_bytes())
                .unwrap()
                .contains("private-denied-sentinel")
        );
        let catalog = LocalCheckpointStoreV0::open(directory.path())
            .unwrap()
            .catalog()
            .unwrap();
        assert_eq!(catalog.published().len(), 3);
        assert!(catalog.pending().is_empty());
        resident.graceful_shutdown().unwrap();
    }
}
