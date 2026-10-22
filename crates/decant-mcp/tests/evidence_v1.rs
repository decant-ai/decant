#[path = "../../decant-state/tests/support/sre_v1.rs"]
mod support;
use decant_compiler::evidence::{EvidenceRequestV1, compile_evidence_v1};
use decant_mcp::{
    DecantMcpBoundedTransportV0,
    evidence::{EXPLAIN_TOOL_V1, EvidenceServerV1, EvidenceViewV1, SNAPSHOT_TOOL_V1},
};
use rmcp::ServiceExt;
use serde_json::{Value, json};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader, duplex, split};

#[tokio::test]
async fn official_sdk_client_negotiates_and_reads_exact_report() {
    use rmcp::model::{CallToolRequestParams, ClientInfo};
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        for requested in ["2025-11-25", "2026-07-28"] {
            let report = compile_evidence_v1(&support::fixture(), &EvidenceRequestV1::default()).unwrap();
            let server = EvidenceServerV1::new(EvidenceViewV1::new(&report).unwrap());
            let (client_io, server_io) = duplex(256 * 1024);
            let (read, write) = split(server_io);
            let serving = tokio::spawn(async move { server.serve(DecantMcpBoundedTransportV0::new(read, write)).await.unwrap().waiting().await.unwrap(); });
            let info: ClientInfo = serde_json::from_value(json!({"protocolVersion":requested,"capabilities":{},"clientInfo":{"name":"official-sdk-oracle","version":"3.2.0"}})).unwrap();
            let client = info.serve(client_io).await.unwrap();
            assert_eq!(client.peer_info().unwrap().protocol_version.as_str(), "2025-11-25");
            assert_eq!(client.list_tools(None).await.unwrap().tools.len(), 2);
            let snapshot = client.call_tool(CallToolRequestParams::new(SNAPSHOT_TOOL_V1)).await.unwrap();
            assert_eq!(snapshot.structured_content.unwrap()["report_id"], report.report_id);
            let arguments = json!({"report_id":report.report_id,"provenance_uri":report.services[0].baseline.selected_references[0]}).as_object().unwrap().clone();
            let explanation = client.call_tool(CallToolRequestParams::new(EXPLAIN_TOOL_V1).with_arguments(arguments)).await.unwrap();
            assert_eq!(explanation.structured_content.unwrap()["status"], "present_in_report");
            client.cancel().await.unwrap();
            serving.await.unwrap();
        }
    }).await.unwrap();
}

#[test]
fn live_publication_is_atomic_cut_bound_and_faults_are_permanent() {
    use decant_compiler::evidence::{EvidenceInputV1, compile_evidence_with_input_v1};
    let report = compile_evidence_with_input_v1(
        &support::fixture(),
        &EvidenceRequestV1::default(),
        EvidenceInputV1 {
            capture: None,
            source_progress_hash: Some(format!("sha256:v0:{}", "0".repeat(64))),
        },
    )
    .unwrap();
    let view = EvidenceViewV1::unpublished();
    assert!(view.snapshot().is_err());
    assert_eq!(view.live_status().unwrap()["status"], "awaiting_checkpoint");
    view.observe_pending(1).unwrap();
    assert!(view.snapshot().is_err());
    view.publish_checkpoint(&report, 1).unwrap();
    let status = view.live_status().unwrap();
    assert_eq!(status["report_id"], report.report_id);
    assert_eq!(status["checkpoint_generation"], 1);
    assert_eq!(status["pending_records"], 0);
    assert_eq!(status["source_recency"], "unknown");
    view.observe_pending(9).unwrap();
    assert_eq!(
        view.snapshot().unwrap().to_bytes().unwrap(),
        report.to_bytes().unwrap()
    );
    assert_eq!(view.live_status().unwrap()["pending_records"], 9);
    assert!(view.publish_checkpoint(&report, 1).is_err());
    view.mark_unavailable().unwrap();
    assert!(view.snapshot().is_err());
    assert!(view.publish_checkpoint(&report, 2).is_err());
    assert!(view.observe_pending(0).is_err());
    assert!(view.live_status().unwrap().get("report_id").is_none());
}

#[tokio::test]
async fn sdk_server_snapshot_is_bounded_and_lookup_is_exact_report_only() {
    let report = compile_evidence_v1(&support::fixture(), &EvidenceRequestV1::default()).unwrap();
    let reference = report.services[0].baseline.selected_references[0].clone();
    let report_id = report.report_id.clone();
    let view = EvidenceViewV1::new(&report).unwrap();
    let server = EvidenceServerV1::new(view.clone());
    let (client, backend) = duplex(256 * 1024);
    let (read, write) = split(backend);
    let task = tokio::spawn(async move {
        server
            .serve(DecantMcpBoundedTransportV0::new(read, write))
            .await
            .unwrap()
            .waiting()
            .await
            .unwrap();
    });
    let (read, mut write) = split(client);
    let mut read = BufReader::new(read);
    async fn exchange(
        read: &mut BufReader<tokio::io::ReadHalf<tokio::io::DuplexStream>>,
        write: &mut tokio::io::WriteHalf<tokio::io::DuplexStream>,
        value: Value,
    ) -> Value {
        let mut bytes = serde_json::to_vec(&value).unwrap();
        bytes.push(b'\n');
        write.write_all(&bytes).await.unwrap();
        let mut line = String::new();
        read.read_line(&mut line).await.unwrap();
        assert!(line.len() <= 128 * 1024);
        serde_json::from_str(&line).unwrap()
    }
    let init = exchange(&mut read,&mut write,json!({"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2025-11-25","capabilities":{},"clientInfo":{"name":"evidence-wire-test","version":"1"}}})).await;
    assert_eq!(init["result"]["serverInfo"]["name"], "decant-evidence");
    write
        .write_all(b"{\"jsonrpc\":\"2.0\",\"method\":\"notifications/initialized\"}\n")
        .await
        .unwrap();
    let listed = exchange(
        &mut read,
        &mut write,
        json!({"jsonrpc":"2.0","id":2,"method":"tools/list"}),
    )
    .await;
    assert_eq!(listed["result"]["tools"].as_array().unwrap().len(), 2);
    let snapshot = exchange(&mut read,&mut write,json!({"jsonrpc":"2.0","id":3,"method":"tools/call","params":{"name":SNAPSHOT_TOOL_V1,"arguments":{}}})).await;
    assert_eq!(
        snapshot["result"]["structuredContent"]["report_id"],
        report_id
    );
    assert_eq!(
        snapshot["result"]["structuredContent"]["report_json"],
        String::from_utf8(report.to_bytes().unwrap()).unwrap()
    );
    let found = exchange(&mut read,&mut write,json!({"jsonrpc":"2.0","id":4,"method":"tools/call","params":{"name":EXPLAIN_TOOL_V1,"arguments":{"report_id":report_id,"provenance_uri":reference}}})).await;
    assert_eq!(
        found["result"]["structuredContent"]["status"],
        "present_in_report"
    );
    let absent = exchange(&mut read,&mut write,json!({"jsonrpc":"2.0","id":5,"method":"tools/call","params":{"name":EXPLAIN_TOOL_V1,"arguments":{"report_id":"0".repeat(64),"provenance_uri":reference}}})).await;
    assert_eq!(
        absent["result"]["structuredContent"]["status"],
        "unavailable"
    );
    assert!(
        absent["result"]["structuredContent"]
            .get("provenance_uri")
            .is_none()
    );
    let bad = exchange(&mut read,&mut write,json!({"jsonrpc":"2.0","id":6,"method":"tools/call","params":{"name":SNAPSHOT_TOOL_V1,"arguments":{"file":"private"}}})).await;
    assert!(bad.get("error").is_some());
    assert!(!bad.to_string().contains("private"));
    view.mark_unavailable().unwrap();
    assert!(view.publish(&report).is_err());
    let dead = exchange(
        &mut read,
        &mut write,
        json!({"jsonrpc":"2.0","id":7,"method":"tools/call","params":{"name":SNAPSHOT_TOOL_V1}}),
    )
    .await;
    assert_eq!(dead["result"]["structuredContent"]["status"], "unavailable");
    write.shutdown().await.unwrap();
    task.await.unwrap();
}
