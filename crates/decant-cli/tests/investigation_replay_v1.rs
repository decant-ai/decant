#[path = "../../decant-backend/tests/support/mod.rs"]
mod support;
use decant_backend::{
    ArtifactDomainV1, artifact_hash_v1,
    grafana::{GrafanaV1, ProfileV1, ScopeV1},
    loki::{LogPolicyV1, LokiPlanV1, LokiRecipeV1},
    metrics::{MetricsPlanV1, MetricsRecipeV1},
    tempo::{TempoPlanV1, TempoRecipeV1, TracePolicyV1, WindowV1},
    transport::{BackendErrorV1 as BackendError, MAX_RESPONSE_BYTES},
};
use decant_cli::investigation::{
    BundleStoreV1, CapturedInvestigationV1, InvestigationConfigV1, InvestigationErrorV1 as E,
    ReplayBundleV1,
};
use serde_json::{Value, json};
use support::{Reply, Server, TempDir};
const TRACE: &str = "01010101010101010101010101010101";
fn config(url: &str) -> InvestigationConfigV1 {
    let window = WindowV1 {
        end_seconds: 1000,
        window_seconds: 300,
    };
    InvestigationConfigV1 {
        profile: ProfileV1 {
            format: "decant-grafana-profile".into(),
            version: 1,
            url: url.into(),
            token_env: "SYNTHETIC_PRIVATE_ENV_REFERENCE".into(),
            ca_file: Some("/SYNTHETIC_PRIVATE_CA".into()),
            prometheus_uid: "metrics".into(),
            tempo_uid: Some("traces".into()),
            loki_uid: Some("logs".into()),
            scope: ScopeV1 {
                service_label: "service".into(),
                service: "checkout".into(),
                environment_label: "environment".into(),
                environment: "prod".into(),
            },
        },
        metrics: MetricsRecipeV1 {
            request_counter: "http_requests_total".into(),
            error_label: "status".into(),
            error_values: vec!["500".into()],
            duration_bucket: "http_duration_seconds_bucket".into(),
            duration_unit: "seconds".into(),
            window_seconds: 300,
            end_seconds: 1000,
        },
        tempo: Some(TempoRecipeV1 {
            window,
            traces_per_window: 1,
            policy: TracePolicyV1::default(),
        }),
        loki: Some(LokiRecipeV1 {
            window,
            error_label: "level".into(),
            error_values: vec!["error".into()],
            policy: LogPolicyV1 {
                max_examples: 8,
                allowed_fields: vec!["message".into()],
                protected_fields: vec!["secret".into()],
            },
        }),
        report_bytes: 65536,
    }
}
fn metric(i: usize) -> Vec<u8> {
    let numbers = ["80", "10", "0.25", "1", "2", "160", "40", "1.25", "3", "1"];
    serde_json::to_vec(&json!({"status":"success","data":{"resultType":"vector","result":[{"metric":{},"value":[if i<5{700}else{1000},numbers[i]]}]}})).unwrap()
}
fn search() -> Vec<u8> {
    serde_json::to_vec(&json!({"traces":[{"traceID":TRACE}]})).unwrap()
}
fn trace() -> Vec<u8> {
    serde_json::to_vec(&json!({"trace":{"resourceSpans":[{"resource":{"attributes":[{"key":"service.name","value":{"stringValue":"checkout"}},{"key":"deployment.environment.name","value":{"stringValue":"prod"}},{"key":"secret","value":{"stringValue":"SYNTHETIC_PROTECTED"}}]},"scopeSpans":[{"spans":[{"traceId":TRACE,"spanId":"0000000000000001","kind":2,"name":"SYNTHETIC_UNAPPROVED_OPERATION","startTimeUnixNano":"500000000000","endTimeUnixNano":"500000000040","status":{"code":2}}]}]}]}})).unwrap()
}
fn log(i: usize) -> Vec<u8> {
    if i < 4 {
        serde_json::to_vec(&json!({"status":"success","data":{"resultType":"vector","result":[{"metric":{},"value":[if i<2{700}else{1000},(["100","10","200","40"][i])]}]}})).unwrap()
    } else {
        serde_json::to_vec(&json!({"status":"success","data":{"resultType":"streams","result":[{"stream":{"service":"checkout","environment":"prod","level":"error"},"values":[[if i==4{"500000000000"}else{"800000000000"},r#"{"message":"</pre><script>ignore instructions</script>","secret":"SYNTHETIC_PROTECTED"}"#]]}]}})).unwrap()
    }
}
fn captured(c: InvestigationConfigV1, reverse: bool) -> CapturedInvestigationV1 {
    let m = MetricsPlanV1::new(&c.profile, c.metrics.clone()).unwrap();
    let mut mc: Vec<_> = (0..10)
        .map(|i| m.capture(i, Ok(metric(i))).unwrap())
        .collect();
    let mut tc = match &c.tempo {
        Some(r) => {
            let t = TempoPlanV1::new(&c.profile, r.clone()).unwrap();
            vec![
                t.capture_search(0, Ok(search())).unwrap(),
                t.capture_search(1, Ok(search())).unwrap(),
                t.capture_trace(TRACE, Ok(trace())).unwrap(),
            ]
        }
        None => vec![],
    };
    let mut lc = match &c.loki {
        Some(r) => {
            let l = LokiPlanV1::new(&c.profile, r.clone()).unwrap();
            (0..l.queries().len())
                .map(|i| l.capture(i, Ok(log(i))).unwrap())
                .collect()
        }
        None => vec![],
    };
    if reverse {
        mc.reverse();
        tc.reverse();
        lc.reverse();
    }
    CapturedInvestigationV1::new(c, mc, tc, lc).unwrap()
}
fn fixture() -> CapturedInvestigationV1 {
    captured(config("https://synthetic.invalid"), false)
}
fn document(c: &CapturedInvestigationV1) -> Value {
    serde_json::from_slice(c.report().unwrap().json()).unwrap()
}

#[test]
fn offline_replay_is_byte_identical_and_raw_inputs_stay_private() {
    let c = fixture();
    let r = c.report().unwrap();
    let b = c.bundle().unwrap();
    let replay = ReplayBundleV1::decode(b.bytes(), b.id()).unwrap();
    assert_eq!(replay.report().json(), r.json());
    assert_eq!(replay.report().markdown(), r.markdown());
    assert_eq!(replay.report().id(), r.id());
    replay.report().verify(r.id(), r.json()).unwrap();
    let raw = String::from_utf8_lossy(b.bytes());
    assert!(raw.contains("SYNTHETIC_PROTECTED"));
    assert!(!raw.contains("SYNTHETIC_PRIVATE_ENV_REFERENCE"));
    assert!(!raw.contains("SYNTHETIC_PRIVATE_CA"));
    for output in [
        String::from_utf8_lossy(r.json()).into_owned(),
        r.markdown().into(),
        format!("{c:?} {r:?} {b:?}"),
    ] {
        assert!(!output.contains("SYNTHETIC_PROTECTED"));
        assert!(!output.contains("SYNTHETIC_UNAPPROVED_OPERATION"));
    }
    assert!(!r.markdown().contains("<script>"));
    assert!(r.markdown().contains("&lt;script&gt;"));
    assert!(r.json().len() <= 65536 && r.markdown().len() <= 65536);
}
#[test]
fn capture_permutations_are_canonical_and_windows_are_exact() {
    let a = fixture().bundle().unwrap();
    let b = captured(config("https://synthetic.invalid"), true)
        .bundle()
        .unwrap();
    assert!(a.bytes() == b.bytes());
    assert_eq!(a.id(), b.id());
    let d: Value = serde_json::from_slice(a.report().json()).unwrap();
    assert_eq!(d["format"], "decant-investigation");
    assert_eq!(d["version"], 1);
    let coverage = &d["document"]["coverage"];
    assert_eq!(coverage["response_coverage_complete"], false);
    let id = coverage["probe_map"]
        .as_array()
        .unwrap()
        .iter()
        .find(|p| p["signal"] == "metrics" && p["request_index"] == 0)
        .unwrap()["probe_id"]
        .clone();
    let p = coverage["record"]["probes"]
        .as_array()
        .unwrap()
        .iter()
        .find(|p| p["probe_id"] == id)
        .unwrap();
    assert_eq!(p["interval"]["start_ns"], 400_000_000_001u64);
    assert_eq!(p["interval"]["end_ns"], 700_000_000_001u64);
    assert!(
        coverage["record"]["probes"]
            .as_array()
            .unwrap()
            .iter()
            .any(|p| p["outcome"]["status"] == "partial")
    );
}
#[test]
fn different_cut_or_policy_cannot_substitute_report_or_references() {
    let c = fixture();
    let a = c.bundle().unwrap();
    let mut config = config("https://synthetic.invalid");
    config
        .tempo
        .as_mut()
        .unwrap()
        .policy
        .operation_names
        .push("SYNTHETIC_UNAPPROVED_OPERATION".into());
    let b = captured(config, false).bundle().unwrap();
    assert_ne!(a.id(), b.id());
    assert_eq!(
        ReplayBundleV1::decode(b.bytes(), a.id()).unwrap_err(),
        E::IdentityMismatch
    );
    assert_eq!(
        a.report()
            .verify(b.report().id(), b.report().json())
            .unwrap_err(),
        E::IdentityMismatch
    );
    let mut substituted: Value = serde_json::from_slice(a.report().json()).unwrap();
    substituted["document"]["sections"][1]["facts"]["groups"][0]["baseline"]["examples"][0] =
        json!("sha256:v1:forged_reference");
    assert_eq!(
        a.report()
            .verify(a.report().id(), &serde_json::to_vec(&substituted).unwrap())
            .unwrap_err(),
        E::IdentityMismatch
    );
    let mut inconsistent = config_for_metrics_only();
    inconsistent.tempo = Some(TempoRecipeV1 {
        window: WindowV1 {
            end_seconds: 1001,
            window_seconds: 300,
        },
        traces_per_window: 1,
        policy: TracePolicyV1::default(),
    });
    assert_eq!(inconsistent.validate().unwrap_err(), E::InvalidInput);
    assert!(CapturedInvestigationV1::new(inconsistent, vec![], vec![], vec![]).is_err());
}
fn config_for_metrics_only() -> InvestigationConfigV1 {
    let mut c = config("https://synthetic.invalid");
    c.tempo = None;
    c.loki = None;
    c.profile.tempo_uid = None;
    c.profile.loki_uid = None;
    c
}
#[test]
fn absent_sources_are_unattempted_and_budget_omissions_are_explicit() {
    let d = document(&captured(config_for_metrics_only(), false));
    assert_eq!(d["document"]["sections"][1]["omission"], "not_configured");
    assert_eq!(
        d["document"]["coverage"]["response_coverage_complete"],
        false
    );
    let mut c = config("https://synthetic.invalid");
    c.report_bytes = 18000;
    let r = captured(c, false).report().unwrap();
    let d: Value = serde_json::from_slice(r.json()).unwrap();
    assert!(
        d["document"]["sections"]
            .as_array()
            .unwrap()
            .iter()
            .any(|s| s["omission"] == "report_budget")
    );
    assert!(r.markdown().len() <= 18000);
    assert!(
        d["document"]["coverage"]["record"]["probes"]
            .as_array()
            .unwrap()
            .len()
            >= 19
    );
    let mut invalid = config_for_metrics_only();
    invalid.report_bytes = 65537;
    assert!(CapturedInvestigationV1::new(invalid, vec![], vec![], vec![]).is_err());
}
#[test]
fn corruption_truncation_and_trailing_bytes_fail_without_value_diagnostics() {
    let b = fixture().bundle().unwrap();
    for at in [0, 25, b.bytes().len() - 1] {
        let mut bytes = b.bytes().to_vec();
        bytes[at] ^= 1;
        let e = ReplayBundleV1::decode(&bytes, b.id()).unwrap_err();
        assert!(!format!("{e:?} {e}").contains("SYNTHETIC"));
    }
    for len in [0, 1, 25, b.bytes().len() - 1] {
        assert!(ReplayBundleV1::decode(&b.bytes()[..len], b.id()).is_err());
    }
    let mut bytes = b.bytes().to_vec();
    bytes.push(0);
    let id = artifact_hash_v1(ArtifactDomainV1::Bundle, &bytes).unwrap();
    assert!(ReplayBundleV1::decode(&bytes, &id).is_err());
}
fn modified_manifest(b: &ReplayBundleV1, f: impl FnOnce(&mut Value)) -> Vec<u8> {
    let prefix = b"DECANT-INVESTIGATION-V1\n";
    let at = prefix.len();
    let len = u32::from_be_bytes(b.bytes()[at..at + 4].try_into().unwrap()) as usize;
    let header = std::str::from_utf8(&b.bytes()[at + 4..at + 4 + len]).unwrap();
    let original: Value = serde_json::from_str(header).unwrap();
    let mut v = original.clone();
    f(&mut v);
    // Preserve the canonical producer field order so the decoder reaches the
    // intended semantic check, not merely the noncanonical-JSON check.
    let mut manifest = header.to_owned();
    for key in ["version", "report_id"] {
        if original[key] != v[key] {
            manifest = manifest.replacen(
                &format!("\"{key}\":{}", original[key]),
                &format!("\"{key}\":{}", v[key]),
                1,
            );
        }
    }
    for key in ["length", "plan", "attempted", "response_hash"] {
        if original["records"][0][key] != v["records"][0][key] {
            manifest = manifest.replacen(
                &format!("\"{key}\":{}", original["records"][0][key]),
                &format!("\"{key}\":{}", v["records"][0][key]),
                1,
            );
        }
    }
    if v.get("extra").is_some() {
        manifest.pop();
        manifest.push_str(&format!(",\"extra\":{}}}", v["extra"]));
    }
    let manifest = manifest.into_bytes();
    let mut out = prefix.to_vec();
    out.extend_from_slice(&(manifest.len() as u32).to_be_bytes());
    out.extend_from_slice(&manifest);
    out.extend_from_slice(&b.bytes()[at + 4 + len..]);
    out
}
#[test]
fn untrusted_manifest_shapes_fail_even_with_recomputed_outer_checksum() {
    let b = fixture().bundle().unwrap();
    for field in [
        "version",
        "extra",
        "length",
        "plan",
        "attempted",
        "hash",
        "report",
    ] {
        let bytes = modified_manifest(&b, |m| match field {
            "version" => m["version"] = json!(2),
            "extra" => m["extra"] = json!("SYNTHETIC_SECRET"),
            "length" => m["records"][0]["length"] = json!(MAX_RESPONSE_BYTES + 1),
            "plan" => m["records"][0]["plan"] = json!("x".repeat(74)),
            "attempted" => m["records"][0]["attempted"] = json!(false),
            "hash" => m["records"][0]["response_hash"] = json!("forged"),
            _ => m["report_id"] = json!("forged"),
        });
        let id = artifact_hash_v1(ArtifactDomainV1::Bundle, &bytes).unwrap();
        let error = ReplayBundleV1::decode(&bytes, &id).unwrap_err();
        assert_eq!(
            error,
            if ["plan", "hash", "report"].contains(&field) {
                E::IdentityMismatch
            } else {
                E::InvalidBundle
            }
        );
    }
}
#[tokio::test]
async fn actual_tls_capture_to_offline_report_uses_one_bounded_session() {
    let mut replies: Vec<_> = (0..10)
        .map(|i| Reply::json(200, std::str::from_utf8(&metric(i)).unwrap()))
        .collect();
    replies.extend([
        Reply::json(200, std::str::from_utf8(&search()).unwrap()),
        Reply::json(200, std::str::from_utf8(&search()).unwrap()),
        Reply::json(200, std::str::from_utf8(&trace()).unwrap()),
    ]);
    replies.extend((0..6).map(|i| Reply::json(200, std::str::from_utf8(&log(i)).unwrap())));
    let server = Server::new(replies);
    let c = config(&server.url);
    let mut backend = GrafanaV1::new(c.profile.clone(), "synthetic", Some(&server.ca)).unwrap();
    let captured = CapturedInvestigationV1::acquire(c, &mut backend)
        .await
        .unwrap();
    let report = captured.report().unwrap();
    let bundle = captured.bundle().unwrap();
    assert_eq!(server.requests.lock().unwrap().len(), 19);
    assert_eq!(
        ReplayBundleV1::decode(bundle.bytes(), bundle.id())
            .unwrap()
            .report()
            .json(),
        report.json()
    );
    assert_eq!(server.requests.lock().unwrap().len(), 19);
}
#[test]
fn empty_failed_partial_and_unattempted_coverage_remain_distinct() {
    let c = config_for_metrics_only();
    let m = MetricsPlanV1::new(&c.profile, c.metrics.clone()).unwrap();
    let mut captures: Vec<_> = (0..10)
        .map(|i| m.capture(i, Ok(metric(i))).unwrap())
        .collect();
    captures[0] = m
        .capture(
            0,
            Ok(br#"{"status":"success","data":{"resultType":"vector","result":[]}}"#.to_vec()),
        )
        .unwrap();
    captures[1] = m.capture(1, Err(BackendError::PermissionDenied)).unwrap();
    let mut partial: Value = serde_json::from_slice(&metric(2)).unwrap();
    partial["warnings"] = json!(["SYNTHETIC_SECRET"]);
    captures[2] = m
        .capture(2, Ok(serde_json::to_vec(&partial).unwrap()))
        .unwrap();
    let d = document(&CapturedInvestigationV1::new(c, captures, vec![], vec![]).unwrap());
    let rows = d["document"]["coverage"]["record"]["probes"]
        .as_array()
        .unwrap();
    for status in ["complete", "unavailable", "partial", "unattempted"] {
        assert!(rows.iter().any(|r| r["outcome"]["status"] == status));
    }
    assert!(
        rows.iter()
            .any(|r| r["outcome"] == json!({"status":"complete","observed_rows":0}))
    );
    assert!(!d.to_string().contains("SYNTHETIC_SECRET"));
}

#[cfg(unix)]
fn secure_write(path: &std::path::Path, bytes: &[u8]) {
    use std::{fs::OpenOptions, io::Write, os::unix::fs::OpenOptionsExt};
    let mut f = OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .open(path)
        .unwrap();
    f.write_all(bytes).unwrap();
}
fn bundle_path(root: &std::path::Path, id: &str) -> std::path::PathBuf {
    root.join(format!("{}.bundle", &id[10..]))
}
#[cfg(unix)]
#[test]
fn store_lock_permissions_idempotence_and_reopen() {
    use std::os::unix::fs::PermissionsExt;
    let temp = TempDir::new();
    let root = temp.0.join("bundles");
    let bundle = fixture().bundle().unwrap();
    let mut store = BundleStoreV1::open(&root).unwrap();
    assert_eq!(BundleStoreV1::open(&root).unwrap_err(), E::StoreBusy);
    store.put(&bundle).unwrap();
    let before = std::fs::read(bundle_path(&root, bundle.id())).unwrap();
    store.put(&bundle).unwrap();
    assert!(std::fs::read(bundle_path(&root, bundle.id())).unwrap() == before);
    assert_eq!(
        std::fs::metadata(&root).unwrap().permissions().mode() & 0o777,
        0o700
    );
    assert_eq!(
        std::fs::metadata(bundle_path(&root, bundle.id()))
            .unwrap()
            .permissions()
            .mode()
            & 0o777,
        0o600
    );
    drop(store);
    assert!(root.join("writer.lock").exists());
    let store = BundleStoreV1::open(&root).unwrap();
    assert_eq!(
        store.get(bundle.id()).unwrap().report().id(),
        bundle.report().id()
    );
    assert!(store.get("../../SYNTHETIC_SECRET").is_err());
    std::fs::set_permissions(&root, std::fs::Permissions::from_mode(0o755)).unwrap();
    assert_eq!(store.get(bundle.id()).unwrap_err(), E::StoreUnavailable);
    std::fs::set_permissions(&root, std::fs::Permissions::from_mode(0o700)).unwrap();
}
#[cfg(unix)]
#[test]
fn interrupted_or_corrupt_store_file_is_not_deleted_or_overwritten() {
    let temp = TempDir::new();
    let root = temp.0.join("bundles");
    let b = fixture().bundle().unwrap();
    let mut store = BundleStoreV1::open(&root).unwrap();
    let path = bundle_path(&root, b.id());
    secure_write(&path, b"partial SYNTHETIC_SECRET");
    let before = std::fs::read(&path).unwrap();
    assert!(store.put(&b).is_err());
    assert!(store.get(b.id()).is_err());
    assert!(std::fs::read(&path).unwrap() == before);
    drop(store);
    assert!(BundleStoreV1::open(&root).unwrap().get(b.id()).is_err());
}
#[cfg(unix)]
#[test]
fn store_quota_and_entry_limit_refuse_without_deleting_evidence() {
    use std::{fs::OpenOptions, os::unix::fs::OpenOptionsExt};
    let temp = TempDir::new();
    let root = temp.0.join("bundles");
    let b = fixture().bundle().unwrap();
    let mut store = BundleStoreV1::open(&root).unwrap();
    for i in 0..4 {
        OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(0o600)
            .open(root.join(format!("{i:064x}.bundle")))
            .unwrap()
            .set_len(64 * 1024 * 1024)
            .unwrap();
    }
    assert_eq!(store.put(&b).unwrap_err(), E::StoreQuota);
    assert_eq!(std::fs::read_dir(&root).unwrap().count(), 5);
    drop(store);
    let root = temp.0.join("many");
    let mut store = BundleStoreV1::open(&root).unwrap();
    for i in 0..64 {
        secure_write(&root.join(format!("{i:064x}.bundle")), b"");
    }
    assert_eq!(store.put(&b).unwrap_err(), E::StoreQuota);
    secure_write(&root.join(format!("{:064x}.bundle", 65)), b"");
    drop(store);
    assert_eq!(BundleStoreV1::open(&root).unwrap_err(), E::StoreQuota);
}
#[cfg(unix)]
#[test]
fn store_rejects_symlinks_hardlinks_and_nonprivate_permissions() {
    use std::os::unix::fs::{PermissionsExt, symlink};
    let temp = TempDir::new();
    let target = temp.0.join("target");
    std::fs::create_dir(&target).unwrap();
    symlink(&target, temp.0.join("link")).unwrap();
    assert!(BundleStoreV1::open(&temp.0.join("link")).is_err());
    let root = temp.0.join("bundles");
    let store = BundleStoreV1::open(&root).unwrap();
    drop(store);
    let lock = root.join("writer.lock");
    std::fs::hard_link(&lock, temp.0.join("another-lock")).unwrap();
    assert!(BundleStoreV1::open(&root).is_err());
    std::fs::set_permissions(&target, std::fs::Permissions::from_mode(0o755)).unwrap();
    assert!(BundleStoreV1::open(&target).is_err());
}

#[test]
fn exact_aggregate_capture_bound_and_over_limit_fail_before_reduction() {
    let mut c = config("https://synthetic.invalid");
    c.tempo = None;
    let m = MetricsPlanV1::new(&c.profile, c.metrics.clone()).unwrap();
    let l = LokiPlanV1::new(&c.profile, c.loki.clone().unwrap()).unwrap();
    let metrics = || {
        (0..10)
            .map(|i| m.capture(i, Ok(vec![b' '; MAX_RESPONSE_BYTES])).unwrap())
            .collect()
    };
    let logs = || {
        (0..6)
            .map(|i| l.capture(i, Ok(vec![b' '; MAX_RESPONSE_BYTES])).unwrap())
            .collect()
    };
    let bounded = CapturedInvestigationV1::new(c.clone(), metrics(), vec![], logs()).unwrap();
    drop(bounded);
    let t = TempoPlanV1::new(
        &c.profile,
        TempoRecipeV1 {
            window: WindowV1 {
                end_seconds: 1000,
                window_seconds: 300,
            },
            traces_per_window: 1,
            policy: TracePolicyV1::default(),
        },
    )
    .unwrap();
    let extra = vec![t.capture_search(0, Ok(vec![0; 1])).unwrap()];
    assert_eq!(
        CapturedInvestigationV1::new(c, metrics(), extra, logs()).unwrap_err(),
        E::InvalidInput
    );
}

#[test]
fn header_caps_duplicate_fields_and_deep_metadata_are_rejected() {
    let b = fixture().bundle().unwrap();
    let magic = b"DECANT-INVESTIGATION-V1\n";
    let wrap = |header: Vec<u8>| {
        let mut bytes = magic.to_vec();
        bytes.extend_from_slice(&(header.len() as u32).to_be_bytes());
        bytes.extend_from_slice(&header);
        bytes
    };
    let mut duplicate = modified_manifest(&b, |m| m["extra"] = json!(0));
    let start = magic.len() + 4;
    let len = u32::from_be_bytes(duplicate[magic.len()..start].try_into().unwrap()) as usize;
    let header = std::str::from_utf8(&duplicate[start..start + len])
        .unwrap()
        .replace("\"extra\":0", "\"version\":1");
    duplicate = wrap(header.into_bytes());
    let deep = format!("{{\"format\":{}0{}}}", "[".repeat(130), "]".repeat(130)).into_bytes();
    for bytes in [
        wrap(vec![b' '; 65536]),
        wrap(vec![b' '; 65537]),
        duplicate,
        wrap(deep),
    ] {
        let id = artifact_hash_v1(ArtifactDomainV1::Bundle, &bytes).unwrap();
        assert_eq!(
            ReplayBundleV1::decode(&bytes, &id).unwrap_err(),
            E::InvalidBundle
        );
    }
    assert_eq!(
        ReplayBundleV1::decode(&vec![0; 64 * 1024 * 1024 + 65536 + 65], "irrelevant").unwrap_err(),
        E::InvalidBundle
    );
}

#[test]
fn configuration_bounds_and_unattempted_capture_validation_are_exact() {
    for budget in [4095, 4096, 65536, 65537] {
        let mut c = config_for_metrics_only();
        c.report_bytes = budget;
        assert_eq!(c.validate().is_ok(), (4096..=65536).contains(&budget));
    }
    let c = config_for_metrics_only();
    let p = MetricsPlanV1::new(&c.profile, c.metrics).unwrap();
    assert!(
        p.capture(0, Ok(metric(0)))
            .unwrap()
            .as_read()
            .with_attempted(false)
            .is_err()
    );
    assert!(
        p.capture(0, Err(BackendError::PermissionDenied))
            .unwrap()
            .as_read()
            .with_attempted(false)
            .is_err()
    );
    let read = p
        .capture(0, Err(BackendError::BudgetExhausted))
        .unwrap()
        .as_read()
        .with_attempted(false)
        .unwrap();
    assert!(!p.restore_capture(&read).unwrap().as_read().attempted());
}

#[cfg(unix)]
#[test]
fn ownership_is_enforced_across_processes() {
    let temp = TempDir::new();
    let root = temp.0.join("bundles");
    let _owner = BundleStoreV1::open(&root).unwrap();
    let out = std::process::Command::new(std::env::current_exe().unwrap())
        .args(["--ignored", "--exact", "store_child_lock_check"])
        .env("DECANT_SYNTHETIC_STORE_ROOT", &root)
        .output()
        .unwrap();
    assert!(out.status.success());
}
#[cfg(unix)]
#[test]
#[ignore = "invoked in an isolated child by ownership_is_enforced_across_processes"]
fn store_child_lock_check() {
    let root = std::env::var_os("DECANT_SYNTHETIC_STORE_ROOT").expect("synthetic test root");
    assert_eq!(
        BundleStoreV1::open(std::path::Path::new(&root)).unwrap_err(),
        E::StoreBusy
    );
}
