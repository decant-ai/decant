use crate::{grafana, lifecycle};
use decant_backend::{
    ArtifactDomainV1, artifact_hash_v1,
    grafana::{GrafanaV1, ProfileV1, identifier, label},
    loki::{LogPolicyV1, LokiRecipeV1},
    metrics::MetricsRecipeV1,
    model::{
        AnthropicV1,
        two_stage::{RESPONSE_ALLOWANCE, TwoStageAnthropicV1, validate_question},
    },
    tempo::{TempoRecipeV1, TracePolicyV1, WindowV1},
    transport::{MAX_TOTAL_BYTES, UsageV1},
};
use decant_cli::{
    investigation::{
        BundleStoreV1, CapturedInvestigationV1, InvestigationConfigV1, InvestigationErrorV1,
        ReplayBundleV1,
    },
    investigator::{FocusV1, PreparedInvestigationV1, StopV1},
};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::{
    collections::BTreeMap,
    ffi::OsString,
    fs,
    io::{self, IsTerminal, Write},
    path::{Path, PathBuf},
    process::ExitCode,
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use tokio::{io::AsyncWriteExt, time::Instant};

const HELP: &str = "Read-only Grafana investigation\n\n  decant investigate configure --profile <NAME> [--profile-dir <PATH>]\n  decant investigate --profile <NAME> [--since 15m] [--until <EPOCH_SECONDS>]\n  decant investigate follow-up --bundle <ID> [--focus <FOCUS>]\n  decant investigate refresh --bundle <ID> --profile <NAME> [--since 15m]\n\nCommon run options: --store-dir <PATH> (default .decant/investigations),\n  --format markdown|json, --focus overview|latency|errors|dependencies|gaps.\nFresh/refresh: --profile-dir <PATH> (default .decant/profiles),\n  --service <VALUE>, --environment <VALUE> (must match the confirmed profile).\nConfigure prompts for mappings. Noninteractive: --yes --request-counter <NAME>\n  --error-label <NAME> --error-values <CSV> --duration-bucket <NAME>\n  --duration-unit seconds; optional --tempo yes|no --logs yes|no\n  --log-error-label <NAME> --log-error-values <CSV>. No raw log bodies.\nReasoning is opt-in: --reason --model <MODEL> --model-key-env <NAME>\n  --allow-model-egress (required without an interactive confirmation).\nNumeric/qualified evidence is sent to Anthropic and may incur charges.\nNo default model. Six model calls per invocation; no automatic retries.\nFollow-up reuses saved evidence without Grafana access. Refresh needs a later cut.\nPrivate bundles contain sensitive data; do not share them as reports.\nExit 0 means report produced, not service healthy. Errors: 1; usage: 2; signal: 130.\n";
#[derive(Clone, Copy, Debug)]
enum Failure {
    Usage,
    Configuration,
    Io,
    Credentials,
    Confirmation,
    Cancelled,
    Deadline,
    Store(InvestigationErrorV1),
}
impl Failure {
    fn code(self) -> &'static str {
        match self {
            Self::Usage => "invalid_usage",
            Self::Configuration => "invalid_configuration",
            Self::Io => "private_file_io_failed",
            Self::Credentials => "credential_unavailable",
            Self::Confirmation => "model_egress_confirmation_required",
            Self::Cancelled => "cancelled",
            Self::Deadline => "deadline_exceeded",
            Self::Store(InvestigationErrorV1::StoreBusy) => "bundle_store_in_use",
            Self::Store(InvestigationErrorV1::StoreQuota) => "bundle_store_quota",
            Self::Store(_) => "investigation_evidence_failed",
        }
    }
}
impl From<InvestigationErrorV1> for Failure {
    fn from(e: InvestigationErrorV1) -> Self {
        Self::Store(e)
    }
}
type Options = BTreeMap<String, String>;
fn get<'a>(o: &'a Options, k: &str, default: &'a str) -> &'a str {
    o.get(k).map(String::as_str).unwrap_or(default)
}
fn required<'a>(o: &'a Options, k: &str) -> Result<&'a str, Failure> {
    o.get(k).map(String::as_str).ok_or(Failure::Usage)
}
fn choice(o: &Options, k: &str, prompt: &str, required: bool) -> Result<String, Failure> {
    if let Some(v) = o.get(k) {
        return Ok(v.clone());
    }
    if o.contains_key("--yes") {
        return if required {
            Err(Failure::Configuration)
        } else {
            Ok("no".into())
        };
    }
    grafana::prompt(prompt).map_err(|_| Failure::Configuration)
}
fn csv(value: &str) -> Result<Vec<String>, Failure> {
    let result: Vec<_> = value.split(',').map(str::to_owned).collect();
    if result.is_empty() || result.len() > 16 || result.iter().any(|s| s.is_empty() || s.len() > 32)
    {
        return Err(Failure::Configuration);
    }
    Ok(result)
}
fn yes_no(value: &str) -> Result<bool, Failure> {
    match value {
        "yes" => Ok(true),
        "no" | "" => Ok(false),
        _ => Err(Failure::Configuration),
    }
}
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Setup {
    format: String,
    version: u8,
    profile_id: String,
    metrics: MetricsRecipeV1,
    tempo: bool,
    logs: Option<LogSelection>,
}
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct LogSelection {
    error_label: String,
    error_values: Vec<String>,
}
fn profile_id(profile: &ProfileV1) -> Result<String, Failure> {
    artifact_hash_v1(
        ArtifactDomainV1::Configuration,
        &profile.encode().map_err(|_| Failure::Configuration)?,
    )
    .map_err(|_| Failure::Configuration)
}
impl Setup {
    fn config(
        &self,
        profile: ProfileV1,
        end: u64,
        seconds: u32,
    ) -> Result<InvestigationConfigV1, Failure> {
        if self.format != "decant-investigation-setup"
            || self.version != 1
            || self.profile_id != profile_id(&profile)?
            || self.metrics.window_seconds != 60
            || self.metrics.end_seconds != 120
        {
            return Err(Failure::Configuration);
        }
        let window = WindowV1 {
            end_seconds: end,
            window_seconds: seconds,
        };
        let mut metrics = self.metrics.clone();
        metrics.end_seconds = end;
        metrics.window_seconds = seconds;
        let config = InvestigationConfigV1 {
            profile,
            metrics,
            tempo: self.tempo.then_some(TempoRecipeV1 {
                window,
                traces_per_window: 4,
                policy: TracePolicyV1::default(),
            }),
            loki: self.logs.as_ref().map(|r| LokiRecipeV1 {
                window,
                error_label: r.error_label.clone(),
                error_values: r.error_values.clone(),
                policy: LogPolicyV1::default(),
            }),
            report_bytes: 65536,
        };
        config.validate().map_err(|_| Failure::Configuration)?;
        Ok(config)
    }
}
fn configure(o: &Options) -> Result<Vec<u8>, Failure> {
    let dir = Path::new(get(o, "--profile-dir", ".decant/profiles"));
    let name = get(o, "--profile", "production");
    let profile = grafana::load_profile(dir, name).map_err(|_| Failure::Configuration)?;
    let metrics = MetricsRecipeV1 {
        request_counter: choice(
            o,
            "--request-counter",
            "Request counter metric (not a precomputed rate)",
            true,
        )?,
        error_label: choice(o, "--error-label", "Counter label selecting errors", true)?,
        error_values: csv(&choice(
            o,
            "--error-values",
            "Exact error label values, comma-separated",
            true,
        )?)?,
        duration_bucket: choice(
            o,
            "--duration-bucket",
            "Classic histogram bucket metric ending in _bucket",
            true,
        )?,
        duration_unit: choice(
            o,
            "--duration-unit",
            "Histogram bucket values are in which unit? Type seconds to confirm",
            true,
        )?,
        end_seconds: 120,
        window_seconds: 60,
    };
    let tempo = if profile.tempo_uid.is_some() {
        yes_no(&choice(
            o,
            "--tempo",
            "Include sampled Tempo traces? yes/no",
            false,
        )?)?
    } else {
        if o.contains_key("--tempo") {
            return Err(Failure::Configuration);
        }
        false
    };
    let logs = if profile.loki_uid.is_some()
        && yes_no(&choice(
            o,
            "--logs",
            "Include Loki counts (no raw log bodies)? yes/no",
            false,
        )?)? {
        Some(LogSelection {
            error_label: choice(
                o,
                "--log-error-label",
                "Indexed Loki label selecting errors",
                true,
            )?,
            error_values: csv(&choice(
                o,
                "--log-error-values",
                "Exact Loki error label values, comma-separated",
                true,
            )?)?,
        })
    } else {
        None
    };
    if (profile.loki_uid.is_none() && o.contains_key("--logs"))
        || (logs.is_none()
            && (o.contains_key("--log-error-label") || o.contains_key("--log-error-values")))
    {
        return Err(Failure::Configuration);
    }
    let setup = Setup {
        format: "decant-investigation-setup".into(),
        version: 1,
        profile_id: profile_id(&profile)?,
        metrics,
        tempo,
        logs,
    };
    setup.config(profile, 120, 60)?;
    if !o.contains_key("--yes")
        && grafana::prompt("Save these explicit mappings? Type yes")
            .map_err(|_| Failure::Confirmation)?
            != "yes"
    {
        return Err(Failure::Confirmation);
    }
    let bytes = serde_json::to_vec(&setup).map_err(|_| Failure::Configuration)?;
    if bytes.len() > 16 * 1024 {
        return Err(Failure::Configuration);
    }
    grafana::save(dir, &format!("{name}.investigation"), &bytes).map_err(|_| Failure::Io)?;
    Ok(b"{\"format\":\"decant-investigation-configure\",\"version\":1,\"saved\":true,\"query_access_verified\":false}\n".to_vec())
}
fn private_settings(path: &Path) -> Result<Vec<u8>, Failure> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::{MetadataExt, PermissionsExt};
        let m = fs::symlink_metadata(path).map_err(|_| Failure::Io)?;
        if !m.is_file()
            || m.file_type().is_symlink()
            || m.nlink() != 1
            || m.permissions().mode() & 0o077 != 0
        {
            return Err(Failure::Io);
        }
    }
    grafana::bounded_file(path, 16 * 1024).map_err(|_| Failure::Io)
}
fn parse(args: impl Iterator<Item = OsString>) -> Result<(String, Options, bool), Failure> {
    let mut args = args;
    let mut items = Vec::new();
    for arg in args.by_ref() {
        if items.len() == 64 {
            return Err(Failure::Usage);
        }
        let s = arg.into_string().map_err(|_| Failure::Usage)?;
        if s.len() > 4096 {
            return Err(Failure::Usage);
        }
        items.push(s);
    }
    let mode = if items.first().is_some_and(|s| {
        ["configure", "follow-up", "refresh", "report", "ask"].contains(&s.as_str())
    }) {
        items.remove(0)
    } else {
        "run".into()
    };
    if items == ["--help"] {
        return Ok((mode, Options::new(), true));
    }
    let configure = mode == "configure";
    let follow = mode == "follow-up" || mode == "report";
    let common = ["--profile", "--profile-dir"];
    let setup = [
        "--yes",
        "--request-counter",
        "--error-label",
        "--error-values",
        "--duration-bucket",
        "--duration-unit",
        "--tempo",
        "--logs",
        "--log-error-label",
        "--log-error-values",
    ];
    let run = [
        "--store-dir",
        "--format",
        "--focus",
        "--reason",
        "--model",
        "--model-key-env",
        "--allow-model-egress",
    ];
    let fresh = ["--since", "--until", "--service", "--environment"];
    let mut options = Options::new();
    let mut iter = items.into_iter();
    while let Some(key) = iter.next() {
        let allowed = if mode == "report" {
            ["--bundle", "--store-dir", "--format"].contains(&key.as_str())
        } else if configure {
            common.contains(&key.as_str()) || setup.contains(&key.as_str())
        } else {
            run.contains(&key.as_str())
                || (!follow && (common.contains(&key.as_str()) || fresh.contains(&key.as_str())))
                || ((follow || mode == "refresh" || mode == "ask") && key == "--bundle")
                || (mode == "ask" && key == "--question")
        };
        if !allowed {
            return Err(Failure::Usage);
        }
        let value = if ["--yes", "--reason", "--allow-model-egress"].contains(&key.as_str()) {
            "yes".into()
        } else {
            iter.next().ok_or(Failure::Usage)?
        };
        if options.insert(key, value).is_some() {
            return Err(Failure::Usage);
        }
    }
    Ok((mode, options, false))
}
struct Reason {
    model: String,
    key_env: String,
}
struct Job {
    question: Option<String>,
    mode: String,
    store: PathBuf,
    bundle: Option<String>,
    config: Option<InvestigationConfigV1>,
    focus: FocusV1,
    json: bool,
    reason: Option<Reason>,
}
fn prepare(mode: String, o: Options) -> Result<Job, Failure> {
    let question = if mode == "ask" {
        if o.contains_key("--focus")
            || o.contains_key("--reason")
            || (o.contains_key("--bundle") && !o.contains_key("--until"))
        {
            return Err(Failure::Usage);
        }
        let q = required(&o, "--question")?;
        validate_question(q).map_err(|_| Failure::Usage)?;
        Some(q.to_owned())
    } else {
        None
    };
    let focus = match get(&o, "--focus", "overview") {
        "overview" => FocusV1::Overview,
        "latency" => FocusV1::Latency,
        "errors" => FocusV1::Errors,
        "dependencies" => FocusV1::Dependencies,
        "gaps" => FocusV1::Gaps,
        _ => return Err(Failure::Usage),
    };
    let json = match get(&o, "--format", "markdown") {
        "markdown" => false,
        "json" => true,
        _ => return Err(Failure::Usage),
    };
    let bundle = if mode == "follow-up"
        || mode == "refresh"
        || mode == "report"
        || (mode == "ask" && o.contains_key("--bundle"))
    {
        let id = required(&o, "--bundle")?;
        if !decant_backend::model::identity(id) {
            return Err(Failure::Configuration);
        }
        Some(id.into())
    } else {
        None
    };
    let config = if mode == "follow-up" || mode == "report" {
        None
    } else {
        let name = get(&o, "--profile", "production");
        if !identifier(name) {
            return Err(Failure::Configuration);
        }
        let dir = Path::new(get(&o, "--profile-dir", ".decant/profiles"));
        let profile = grafana::load_profile(dir, name).map_err(|_| Failure::Configuration)?;
        if o.get("--service")
            .is_some_and(|v| v != &profile.scope.service)
            || o.get("--environment")
                .is_some_and(|v| v != &profile.scope.environment)
        {
            return Err(Failure::Configuration);
        }
        let setup: Setup = serde_json::from_slice(&private_settings(
            &dir.join(format!("{name}.investigation.json")),
        )?)
        .map_err(|_| Failure::Configuration)?;
        let since = get(&o, "--since", "15m");
        let minutes: u32 = since
            .strip_suffix('m')
            .and_then(|s| s.parse().ok())
            .filter(|n| (1..=60).contains(n))
            .ok_or(Failure::Usage)?;
        let end = match o.get("--until") {
            Some(s) => s.parse().map_err(|_| Failure::Usage)?,
            None => SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map_err(|_| Failure::Configuration)?
                .as_secs(),
        };
        Some(setup.config(profile, end, minutes * 60)?)
    };
    let reason = if o.contains_key("--reason") || mode == "ask" {
        let model = required(&o, "--model")?;
        let key_env = required(&o, "--model-key-env")?;
        if model.is_empty()
            || model.len() > 128
            || !model
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || b"-_.".contains(&b))
            || !label(key_env)
        {
            return Err(Failure::Configuration);
        }
        let prompt = if mode == "ask" {
            "Send your question and projected numeric evidence to Anthropic (at most two billable calls)? Do not include secrets. Type yes"
        } else {
            "Send projected numeric evidence to Anthropic (up to six billable calls)? Type yes"
        };
        if !o.contains_key("--allow-model-egress")
            && (!io::stdin().is_terminal()
                || grafana::prompt(prompt).map_err(|_| Failure::Confirmation)? != "yes")
        {
            return Err(Failure::Confirmation);
        }
        Some(Reason {
            model: model.into(),
            key_env: key_env.into(),
        })
    } else {
        if ["--model", "--model-key-env", "--allow-model-egress"]
            .iter()
            .any(|k| o.contains_key(*k))
        {
            return Err(Failure::Usage);
        }
        None
    };
    Ok(Job {
        question,
        mode,
        store: PathBuf::from(get(&o, "--store-dir", ".decant/investigations")),
        bundle,
        config,
        focus,
        json,
        reason,
    })
}
async fn execute(job: Job) -> Result<(Vec<u8>, u8), Failure> {
    if job.mode == "ask" {
        return execute_ask(job).await;
    }
    let started = Instant::now();
    let deadline = started + Duration::from_secs(300);
    grafana::directory(&job.store, true).map_err(|_| Failure::Io)?;
    let mut store = BundleStoreV1::open(&job.store)?;
    let prior = job.bundle.as_ref().map(|id| store.get(id)).transpose()?;
    let parent = if job.mode == "refresh" {
        prior.as_ref().map(|b| b.report().id().to_owned())
    } else {
        None
    };
    let mut backend_usage = UsageV1::default();
    let bundle: ReplayBundleV1 = if let Some(config) = job.config {
        if let Some(old) = &prior {
            let v: Value =
                serde_json::from_slice(old.report().json()).map_err(|_| Failure::Configuration)?;
            let old_end = v["document"]["window"]["end_seconds"]
                .as_u64()
                .ok_or(Failure::Configuration)?;
            if config.metrics.end_seconds <= old_end
                || serde_json::to_value(&config.profile.scope)
                    .map_err(|_| Failure::Configuration)?
                    != v["document"]["scope"]
            {
                return Err(Failure::Configuration);
            }
        }
        let key = grafana::token(&config.profile.token_env).map_err(|_| Failure::Credentials)?;
        let ca = grafana::read_ca(config.profile.ca_file.as_deref()).map_err(|_| Failure::Io)?;
        let mut backend = GrafanaV1::new(config.profile.clone(), &key, ca.as_deref())
            .map_err(|_| Failure::Configuration)?;
        let captured = tokio::time::timeout_at(
            deadline,
            CapturedInvestigationV1::acquire(config, &mut backend),
        )
        .await
        .map_err(|_| Failure::Deadline)??;
        backend_usage = backend.usage();
        drop(backend);
        drop(key);
        let bundle = captured.bundle()?;
        store.put(&bundle)?;
        bundle
    } else {
        prior.ok_or(Failure::Configuration)?
    };
    // This stable identity is safe to disclose and survives later model errors
    // or cancellation. No raw path, credential or backend value is logged.
    writeln!(io::stderr().lock(), "evidence saved: {}", bundle.id()).map_err(|_| Failure::Io)?;
    if job.mode == "report" {
        let bytes = if job.json {
            bundle.report().json().to_vec()
        } else {
            bundle.report().markdown().as_bytes().to_vec()
        };
        return Ok((bytes, 0));
    }
    let prepared = PreparedInvestigationV1::new(bundle.report())?;
    let mut reason_status = "not_requested";
    let mut model_name = None;
    let assessment = if let Some(reason) = job.reason {
        model_name = Some(reason.model.clone());
        let remaining = deadline.saturating_duration_since(Instant::now());
        let bytes = MAX_TOTAL_BYTES.saturating_sub(backend_usage.received_bytes);
        let provider = if bytes == 0 || remaining.is_zero() {
            None
        } else {
            grafana::token(&reason.key_env)
                .ok()
                .and_then(|key| AnthropicV1::new(&reason.model, &key, remaining).ok())
                .and_then(|p| p.with_response_allowance(bytes).ok())
        };
        if let Some(mut provider) = provider {
            let result = prepared
                .reason(
                    job.focus,
                    &mut provider,
                    deadline.saturating_duration_since(Instant::now()),
                )
                .await?;
            reason_status = if result.stop() == StopV1::Completed {
                "completed"
            } else {
                "stopped"
            };
            result
        } else {
            reason_status = "provider_unavailable_or_budget_exhausted";
            prepared.evidence(job.focus)?
        }
    } else {
        prepared.evidence(job.focus)?
    };
    let status = if ["not_requested", "completed"].contains(&reason_status) {
        0
    } else {
        1
    };
    let bytes = if job.json {
        let value: Value =
            serde_json::from_slice(assessment.json()).map_err(|_| Failure::Configuration)?;
        let mut output=serde_json::to_vec(&json!({"format":"decant-investigation-run","version":1,"bundle_id":bundle.id(),"report_id":bundle.report().id(),"parent_report_id":parent,"reasoning_status":reason_status,"model":model_name,"backend":{"requests":backend_usage.attempts,"received_body_bytes":backend_usage.received_bytes},"assessment":value})).map_err(|_|Failure::Configuration)?;
        output.push(b'\n');
        output
    } else {
        format!(
            "Bundle: `{}`\nParent report: `{}`\nReasoning: {reason_status}\n\n{}",
            bundle.id(),
            parent.as_deref().unwrap_or("none"),
            assessment.markdown()
        )
        .into_bytes()
    };
    if bytes.len() > 64 * 1024 {
        return Err(Failure::Store(InvestigationErrorV1::OutputLimit));
    }
    Ok((bytes, status))
}
async fn execute_ask(job: Job) -> Result<(Vec<u8>, u8), Failure> {
    let deadline = Instant::now() + Duration::from_secs(300);
    let config = job.config.ok_or(Failure::Configuration)?;
    grafana::directory(&job.store, true).map_err(|_| Failure::Io)?;
    let mut store = BundleStoreV1::open(&job.store)?;
    let seed = job
        .bundle
        .as_ref()
        .map(|id| store.get(id).and_then(|b| b.captured()))
        .transpose()?;
    if let Some(s) = &seed {
        decant_cli::investigation::adaptive::validate_seed(&config, s)?;
    }
    let reason = job.reason.ok_or(Failure::Configuration)?;
    let model_key = grafana::token(&reason.key_env).map_err(|_| Failure::Credentials)?;
    let mut provider = TwoStageAnthropicV1::new(
        &reason.model,
        &model_key,
        deadline.saturating_duration_since(Instant::now()),
    )
    .map_err(|_| Failure::Configuration)?;
    drop(model_key);
    let key = grafana::token(&config.profile.token_env).map_err(|_| Failure::Credentials)?;
    let ca = grafana::read_ca(config.profile.ca_file.as_deref()).map_err(|_| Failure::Io)?;
    let mut backend = GrafanaV1::new(config.profile.clone(), &key, ca.as_deref())
        .and_then(|b| b.with_response_allowance(MAX_TOTAL_BYTES - RESPONSE_ALLOWANCE))
        .map_err(|_| Failure::Configuration)?;
    drop(key);
    let answer = decant_cli::question::investigate(
        decant_cli::question::QuestionRequestV1 {
            question: job.question.as_deref().ok_or(Failure::Usage)?,
            config,
            seed,
            remaining: deadline.saturating_duration_since(Instant::now()),
        },
        &mut backend,
        &mut provider,
        |b| {
            store.put(b)?;
            writeln!(io::stderr().lock(), "evidence saved: {}", b.id())
                .map_err(|_| InvestigationErrorV1::StoreUnavailable)?;
            Ok(())
        },
    )
    .await?;
    Ok((
        if job.json {
            answer.json().to_vec()
        } else {
            answer.markdown().as_bytes().to_vec()
        },
        if answer.completed() { 0 } else { 1 },
    ))
}
pub fn run(args: impl Iterator<Item = OsString>) -> ExitCode {
    let result = (|| {
        let (mode, o, help) = parse(args)?;
        if help {
            io::stdout()
                .lock()
                .write_all(HELP.as_bytes())
                .map_err(|_| Failure::Io)?;
            io::stdout().lock().write_all(b"\n  decant investigate report --bundle <ID> [--format markdown|json] [--store-dir <PATH>]\nRecompute, verify and render the original evidence report offline.\n").map_err(|_| Failure::Io)?;
            io::stdout().lock().write_all(b"\n  decant investigate ask --question <TEXT> --profile <NAME> --model <MODEL>\n    --model-key-env <NAME> --allow-model-egress [--since 15m] [--until <SECONDS>]\nAt most two model calls: plan, deterministic conditional acquisition, explain.\nSends the question and numeric evidence; never include secrets in the question.\nOptional --bundle <ID> reuses compatible evidence; requires explicit --until\nand matching --since. No automatic refresh. Latency/error recipes only.\n").map_err(|_|Failure::Io)?;
            return Ok(0);
        }
        if mode == "configure" {
            let bytes = configure(&o)?;
            io::stdout()
                .lock()
                .write_all(&bytes)
                .map_err(|_| Failure::Io)?;
            return Ok(0);
        }
        let job = prepare(mode, o)?;
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(|_| Failure::Configuration)?;
        let result=runtime.block_on(async {
            let mut shutdown=lifecycle::Shutdown::new().map_err(|_|Failure::Configuration)?;
            tokio::select! {
                _=shutdown.wait()=>Err(Failure::Cancelled),
                result=async {
                    let (bytes,status)=execute(job).await?;
                    let mut stdout=lifecycle::stdout().map_err(|_|Failure::Io)?;
                    tokio::time::timeout(Duration::from_secs(5),async {stdout.write_all(&bytes).await?;stdout.flush().await}).await.map_err(|_|Failure::Deadline)?.map_err(|_|Failure::Io)?;
                    Ok(status)
                }=>result,
            }
        });
        runtime.shutdown_timeout(Duration::from_secs(1));
        result
    })();
    match result {
        Ok(status) => ExitCode::from(status),
        Err(error) => {
            let _ = writeln!(
                io::stderr().lock(),
                "error: investigation failed ({})",
                error.code()
            );
            ExitCode::from(match error {
                Failure::Usage => 2,
                Failure::Cancelled => 130,
                _ => 1,
            })
        }
    }
}
