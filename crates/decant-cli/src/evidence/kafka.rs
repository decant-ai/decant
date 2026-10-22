use super::Failure;
use crate::kafka_json::evidence::KafkaEvidenceConfigV1;
use decant_compiler::evidence::{
    EvidenceDocumentV1, EvidenceInputV1, EvidenceRequestV1, compile_evidence_with_input_v1,
};
use decant_kafka::{KafkaPartitionFrontierV0, hash_kafka_partition_frontier_v0};
use decant_kafka_client::{
    KafkaStaticConsumerV0, KafkaStaticPollV0, observe_and_validate_kafka_source_v0,
};
use decant_kafka_live::{
    KafkaCheckpointCutResidentV1, KafkaResidentBootstrapErrorV0, KafkaResidentCompilerErrorV0,
};
use decant_mcp::{
    DecantMcpBoundedTransportV0,
    evidence::{EvidenceServerV1, EvidenceViewV1},
};
use decant_state::AdmissionStateV0;
use decant_storage::{LocalCheckpointStoreV0, LocalCheckpointWriterLockErrorV0};
use rmcp::ServiceExt;
use std::{
    io::Write,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    thread,
};
use tokio::sync::oneshot;

#[cfg(test)]
mod tests;

fn compile(
    state: &AdmissionStateV0,
    frontier: &KafkaPartitionFrontierV0,
    recipe: &EvidenceRequestV1,
) -> Result<Vec<u8>, KafkaResidentCompilerErrorV0> {
    let refused = |_| KafkaResidentCompilerErrorV0::Refused;
    let progress = hash_kafka_partition_frontier_v0(frontier)
        .map_err(|_| KafkaResidentCompilerErrorV0::Refused)?
        .to_string();
    let report = compile_evidence_with_input_v1(
        state,
        recipe,
        EvidenceInputV1 {
            capture: None,
            source_progress_hash: Some(progress),
        },
    )
    .map_err(refused)?;
    let bytes = report.to_bytes().map_err(refused)?;
    EvidenceDocumentV1::from_verified_bytes(&bytes).map_err(refused)?;
    Ok(bytes)
}

fn publish(session: &KafkaCheckpointCutResidentV1, view: &EvidenceViewV1) -> Result<(), Failure> {
    if let (Some(context), Some(generation)) =
        (session.context(), session.session().latest_generation())
    {
        let report = EvidenceDocumentV1::from_verified_bytes(context.as_bytes())
            .map_err(|_| Failure::Report)?;
        view.publish_checkpoint(&report, generation.get())
            .map_err(|_| Failure::Mcp)?;
    }
    Ok(())
}

pub(super) fn run(config: KafkaEvidenceConfigV1) -> Result<(), Failure> {
    let KafkaEvidenceConfigV1 {
        source,
        admission,
        checkpoint_directory,
        poll_timeout,
        mapping,
        recipe,
        lifecycle,
        security,
    } = config;
    let contract = source.source_contract().clone();
    let store =
        LocalCheckpointStoreV0::open(&checkpoint_directory).map_err(|_| Failure::Recovery)?;
    let session = KafkaCheckpointCutResidentV1::bootstrap(
        store,
        admission,
        contract.clone(),
        source.starting_positions(),
        lifecycle,
        |state, frontier| compile(state, frontier, &recipe),
    )
    .map_err(|error| match error {
        KafkaResidentBootstrapErrorV0::WriterLock(
            LocalCheckpointWriterLockErrorV0::AlreadyHeld,
        ) => Failure::CheckpointInUse,
        _ => Failure::Recovery,
    })?;
    let view = EvidenceViewV1::unpublished();
    publish(&session, &view)?;
    // Ownership and strict recovery always precede native client construction.
    let consumer = KafkaStaticConsumerV0::new_with_security_resolver(source, security, |name| {
        std::env::var(name).ok()
    })
    .map_err(|_| Failure::Client)?;
    observe_and_validate_kafka_source_v0(
        consumer.native_consumer(),
        consumer.config().topic_name(),
        &contract,
        contract.source_id(),
        poll_timeout,
    )
    .map_err(|_| Failure::Source)?;
    consumer
        .assign_frontier(session.session().frontier())
        .map_err(|_| Failure::Assignment)?;
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|_| Failure::Mcp)?;
    let stop = Arc::new(AtomicBool::new(false));
    let worker_stop = Arc::clone(&stop);
    let worker_view = view.clone();
    let (sender, receiver) = oneshot::channel();
    let worker = thread::Builder::new()
        .name("decant-evidence-source".into())
        .spawn(move || {
            // Also withdraw evidence if an unexpected unwind closes the worker.
            struct Withdraw(EvidenceViewV1);
            impl Drop for Withdraw {
                fn drop(&mut self) {
                    let _ = self.0.mark_unavailable();
                }
            }
            let _withdraw = Withdraw(worker_view.clone());
            let mut session = session;
            let compiler = |state: &AdmissionStateV0, frontier: &KafkaPartitionFrontierV0| {
                compile(state, frontier, &recipe)
            };
            let result = (|| {
                while !worker_stop.load(Ordering::Acquire) {
                    let checkpoint = match consumer
                        .poll(poll_timeout)
                        .map_err(|_| Failure::Source)?
                    {
                        KafkaStaticPollV0::Data(record) => session
                            .process_json_object_record_v1(&mapping, &record, compiler)
                            .map_err(|_| Failure::Processing)?
                            .checkpoint(),
                        KafkaStaticPollV0::Idle | KafkaStaticPollV0::PartitionEof { .. } => session
                            .observe_idle(compiler)
                            .map_err(|_| Failure::Processing)?,
                    };
                    if checkpoint.is_some_and(|cut| cut.was_published()) {
                        publish(&session, &worker_view)?;
                    }
                    worker_view
                        .observe_pending(session.records_since_checkpoint())
                        .map_err(|_| Failure::Mcp)?;
                }
                // This is unreachable after poll/parser/compiler/storage failure.
                session
                    .graceful_shutdown(compiler)
                    .map_err(|_| Failure::Processing)?;
                Ok(())
            })();
            if result.is_err() {
                let _ = worker_view.mark_unavailable();
            }
            let _ = sender.send(result);
            result
        })
        .map_err(|_| Failure::Mcp)?;
    let result = runtime.block_on(serve(EvidenceServerV1::live(view), receiver));
    stop.store(true, Ordering::Release);
    let joined = crate::lifecycle::join(worker, crate::lifecycle::SHUTDOWN_DEADLINE)
        .map_err(|_| Failure::Shutdown);
    // Tokio's blocking stdin read cannot be cancelled by dropping its future.
    // Never wait for that OS read when a source failure terminates this command.
    runtime.shutdown_background();
    joined??;
    result
}

async fn serve(
    server: EvidenceServerV1,
    mut worker: oneshot::Receiver<Result<(), Failure>>,
) -> Result<(), Failure> {
    let mut signal = crate::lifecycle::Shutdown::new().map_err(|_| Failure::Signal)?;
    std::io::stderr()
        .lock()
        .write_all(b"info: evidence MCP ready\n")
        .map_err(|_| Failure::Output)?;
    let transport = DecantMcpBoundedTransportV0::new(
        tokio::io::stdin(),
        crate::lifecycle::stdout().map_err(|_| Failure::Output)?,
    );
    let initialize = server.serve(transport);
    tokio::pin!(initialize);
    let running = tokio::select! {
        result = &mut initialize => result.map_err(|_| Failure::Mcp)?,
        _ = &mut worker => return Err(Failure::Mcp),
        result = signal.wait() => return result.map_err(|_| Failure::Signal),
    };
    let cancellation = running.cancellation_token();
    let waiting = running.waiting();
    tokio::pin!(waiting);
    tokio::select! {
        result = &mut waiting => result.map(|_| ()).map_err(|_| Failure::Mcp),
        _ = &mut worker => { cancellation.cancel(); Err(Failure::Mcp) },
        result = signal.wait() => { cancellation.cancel(); result.map_err(|_| Failure::Signal) },
    }
}
