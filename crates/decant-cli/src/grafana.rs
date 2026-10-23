use decant_backend::{
    grafana::{
        DatasourceV1, GrafanaV1, ProfileV1, ScopeV1, confirm_sources_v1, discover_v1, identifier,
        label,
    },
    transport::{BackendErrorV1, HttpSessionV1, LimitsV1},
};
use std::{
    collections::BTreeMap,
    ffi::{OsStr, OsString},
    fs::{self, File, OpenOptions},
    io::{self, BufRead, Read, Write},
    path::{Component, Path, PathBuf},
    process::ExitCode,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

const HELP: &str = "Grafana connection and query-access diagnostics\n\n+  decant connect grafana --url <HTTPS_URL> --token-env <NAME> [--profile <NAME>]\n+  decant doctor --profile <NAME> [--profile-dir <PATH>]\n\n+Connect guides datasource and scope selection, then asks for confirmation.\n+For noninteractive setup supply --yes, --prometheus-uid, --service-label,\n+--service, --environment-label, and --environment. Optional: --tempo-uid,\n+--loki-uid, --ca-file, --profile-dir. Profiles default to .decant/profiles.\n+Tempo uses resource.service.name and resource.deployment.environment.name;\n+selecting Tempo confirms these mappings. Loki shares the supplied label mapping.\n+Only read queries are sent. Doctor readiness is not service health.\n";

#[derive(Clone, Copy, Debug)]
enum Failure {
    Usage,
    Configuration,
    Credentials,
    ProfileIo,
    Declined,
    Backend(BackendErrorV1),
}
impl Failure {
    fn text(self) -> String {
        let code = match self {
            Self::Usage => "invalid_usage".into(),
            Self::Configuration => "invalid_configuration".into(),
            Self::Credentials => "credential_unavailable".into(),
            Self::ProfileIo => "profile_io_failed".into(),
            Self::Declined => "confirmation_required".into(),
            Self::Backend(e) => e.to_string(),
        };
        format!("error: Grafana command failed ({code})\n")
    }
}
impl From<BackendErrorV1> for Failure {
    fn from(e: BackendErrorV1) -> Self {
        Self::Backend(e)
    }
}

pub fn run(command: &OsStr, args: impl Iterator<Item = OsString>) -> ExitCode {
    let result = execute(command, args);
    match result {
        Ok((bytes, ready)) => {
            if io::stdout().lock().write_all(&bytes).is_err() {
                return ExitCode::from(1);
            }
            if ready {
                ExitCode::SUCCESS
            } else {
                ExitCode::from(1)
            }
        }
        Err(error) => {
            let _ = io::stderr().lock().write_all(error.text().as_bytes());
            ExitCode::from(if matches!(error, Failure::Usage) {
                2
            } else {
                1
            })
        }
    }
}
fn execute(
    command: &OsStr,
    args: impl Iterator<Item = OsString>,
) -> Result<(Vec<u8>, bool), Failure> {
    let mut values = Vec::new();
    for arg in args {
        if values.len() == 32 {
            return Err(Failure::Usage);
        }
        let arg = arg.into_string().map_err(|_| Failure::Usage)?;
        if arg.len() > 4096 {
            return Err(Failure::Usage);
        }
        values.push(arg);
    }
    if values == ["--help"] || (command == "connect" && values == ["grafana", "--help"]) {
        return Ok((HELP.as_bytes().to_vec(), true));
    }
    let connect = command == "connect";
    let flags = if connect {
        if values.first().map(String::as_str) != Some("grafana") {
            return Err(Failure::Usage);
        }
        &values[1..]
    } else {
        &values[..]
    };
    let mut options = BTreeMap::new();
    let mut iter = flags.iter();
    while let Some(key) = iter.next() {
        if ![
            "--url",
            "--token-env",
            "--profile",
            "--profile-dir",
            "--ca-file",
            "--prometheus-uid",
            "--tempo-uid",
            "--loki-uid",
            "--service-label",
            "--service",
            "--environment-label",
            "--environment",
            "--yes",
        ]
        .contains(&key.as_str())
            || (!connect && !["--profile", "--profile-dir"].contains(&key.as_str()))
        {
            return Err(Failure::Usage);
        }
        let value = if key == "--yes" {
            "yes"
        } else {
            iter.next().ok_or(Failure::Usage)?.as_str()
        };
        if options.insert(key.as_str(), value).is_some() {
            return Err(Failure::Usage);
        }
    }
    let name = options.get("--profile").copied().unwrap_or("production");
    if !identifier(name) {
        return Err(Failure::Configuration);
    }
    let directory = PathBuf::from(
        options
            .get("--profile-dir")
            .copied()
            .unwrap_or(".decant/profiles"),
    );
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|_| Failure::Configuration)?;
    let result = runtime.block_on(async {
        if connect {
            connect_profile(options, &directory, name).await
        } else {
            doctor_profile(&directory, name).await
        }
    });
    // Cancelling a request does not require unbounded waiting for DNS workers.
    runtime.shutdown_timeout(Duration::from_secs(1));
    result
}
fn token(name: &str) -> Result<String, Failure> {
    if !label(name) {
        return Err(Failure::Configuration);
    }
    let token = std::env::var(name).map_err(|_| Failure::Credentials)?;
    if token.is_empty() || token.len() > 4096 {
        return Err(Failure::Credentials);
    }
    Ok(token)
}
fn bounded_file(path: &Path, cap: usize) -> Result<Vec<u8>, Failure> {
    if !fs::symlink_metadata(path)
        .map_err(|_| Failure::ProfileIo)?
        .is_file()
    {
        return Err(Failure::ProfileIo);
    }
    let file = File::open(path).map_err(|_| Failure::ProfileIo)?;
    if !file.metadata().map_err(|_| Failure::ProfileIo)?.is_file() {
        return Err(Failure::ProfileIo);
    }
    let mut bytes = Vec::new();
    file.take((cap + 1) as u64)
        .read_to_end(&mut bytes)
        .map_err(|_| Failure::ProfileIo)?;
    if bytes.len() > cap {
        return Err(Failure::ProfileIo);
    }
    Ok(bytes)
}
fn read_ca(path: Option<&str>) -> Result<Option<Vec<u8>>, Failure> {
    path.map(|p| bounded_file(Path::new(p), 64 * 1024))
        .transpose()
}
fn prompt(label: &str) -> Result<String, Failure> {
    let mut stderr = io::stderr().lock();
    write!(stderr, "{label}: ")
        .and_then(|_| stderr.flush())
        .map_err(|_| Failure::ProfileIo)?;
    let mut line = Vec::new();
    io::stdin()
        .lock()
        .take(1025)
        .read_until(b'\n', &mut line)
        .map_err(|_| Failure::Usage)?;
    if line.len() > 1024 || !line.ends_with(b"\n") {
        return Err(Failure::Declined);
    }
    let text = String::from_utf8(line).map_err(|_| Failure::Usage)?;
    Ok(text.trim_end_matches(['\n', '\r']).to_owned())
}
fn choose(
    options: &BTreeMap<&str, &str>,
    key: &str,
    question: &str,
    required: bool,
) -> Result<String, Failure> {
    if let Some(v) = options.get(key) {
        return Ok((*v).into());
    }
    if options.contains_key("--yes") {
        if required {
            return Err(Failure::Configuration);
        }
        return Ok(String::new());
    }
    prompt(question)
}
async fn connect_profile(
    options: BTreeMap<&str, &str>,
    dir: &Path,
    name: &str,
) -> Result<(Vec<u8>, bool), Failure> {
    let url = options.get("--url").ok_or(Failure::Usage)?;
    let env = options.get("--token-env").ok_or(Failure::Usage)?;
    // Require all noninteractive choices before credentials or network access.
    if options.contains_key("--yes")
        && [
            "--prometheus-uid",
            "--service-label",
            "--service",
            "--environment-label",
            "--environment",
        ]
        .iter()
        .any(|k| !options.contains_key(k))
    {
        return Err(Failure::Configuration);
    }
    let bearer = token(env)?;
    let ca = read_ca(options.get("--ca-file").copied())?;
    let mut http = HttpSessionV1::new(url, &bearer, ca.as_deref(), LimitsV1::default())?;
    let sources = discover_v1(&mut http).await?;
    if !options.contains_key("--yes") {
        let mut output = io::stderr().lock();
        writeln!(output, "Available supported datasources (UID / type):")
            .map_err(|_| Failure::ProfileIo)?;
        for DatasourceV1 { uid, kind } in &sources {
            writeln!(output, "  {uid} / {kind}").map_err(|_| Failure::ProfileIo)?;
        }
    }
    let prometheus_uid = choose(&options, "--prometheus-uid", "Prometheus UID", true)?;
    let tempo = choose(
        &options,
        "--tempo-uid",
        "Tempo UID (blank to omit; confirms resource.service.name + resource.deployment.environment.name)",
        false,
    )?;
    let loki = choose(
        &options,
        "--loki-uid",
        "Loki UID (blank to omit; shares metrics scope labels)",
        false,
    )?;
    let scope = ScopeV1 {
        service_label: choose(&options, "--service-label", "Service label", true)?,
        service: choose(&options, "--service", "Service value", true)?,
        environment_label: choose(&options, "--environment-label", "Environment label", true)?,
        environment: choose(&options, "--environment", "Environment value", true)?,
    };
    let ca_file = options
        .get("--ca-file")
        .map(|p| {
            fs::canonicalize(p)
                .map_err(|_| Failure::ProfileIo)?
                .into_os_string()
                .into_string()
                .map_err(|_| Failure::ProfileIo)
        })
        .transpose()?;
    let profile = ProfileV1 {
        format: "decant-grafana-profile".into(),
        version: 1,
        url: (*url).into(),
        token_env: (*env).into(),
        ca_file,
        prometheus_uid,
        tempo_uid: (!tempo.is_empty()).then_some(tempo),
        loki_uid: (!loki.is_empty()).then_some(loki),
        scope,
    };
    confirm_sources_v1(&profile, &sources)?;
    if !options.contains_key("--yes") {
        let mut out = io::stderr().lock();
        // JSON-escape operator values before showing a confirmation summary.
        writeln!(
            out,
            "Scope: {}={} and {}={}",
            profile.scope.service_label,
            serde_json::to_string(&profile.scope.service).map_err(|_| Failure::Configuration)?,
            profile.scope.environment_label,
            serde_json::to_string(&profile.scope.environment)
                .map_err(|_| Failure::Configuration)?
        )
        .map_err(|_| Failure::ProfileIo)?;
        drop(out);
        if prompt("Save this confirmed profile? Type yes")? != "yes" {
            return Err(Failure::Declined);
        }
    }
    save(dir, name, &profile.encode()?)?;
    Ok((b"{\"format\":\"decant-grafana-connect\",\"version\":1,\"saved\":true,\"query_access_verified\":false}\n".to_vec(),true))
}
fn directory(path: &Path, create: bool) -> Result<(), Failure> {
    let mut cursor = PathBuf::new();
    for component in path.components() {
        if matches!(component, Component::ParentDir) {
            return Err(Failure::ProfileIo);
        }
        cursor.push(component);
        match fs::symlink_metadata(&cursor) {
            Ok(meta) if meta.is_dir() => (),
            Err(e) if create && e.kind() == io::ErrorKind::NotFound => {
                let mut builder = fs::DirBuilder::new();
                #[cfg(unix)]
                {
                    use std::os::unix::fs::DirBuilderExt;
                    builder.mode(0o700);
                }
                builder.create(&cursor).map_err(|_| Failure::ProfileIo)?;
            }
            _ => return Err(Failure::ProfileIo),
        }
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        if fs::metadata(path)
            .map_err(|_| Failure::ProfileIo)?
            .permissions()
            .mode()
            & 0o077
            != 0
        {
            return Err(Failure::ProfileIo);
        }
    }
    Ok(())
}
fn save(dir: &Path, name: &str, bytes: &[u8]) -> Result<(), Failure> {
    directory(dir, true)?;
    let mut options = OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    let mut file = options
        .open(dir.join(format!("{name}.json")))
        .map_err(|_| Failure::ProfileIo)?;
    // Failed writes leave a fail-closed partial file; never overwrite it silently.
    file.write_all(bytes)
        .and_then(|_| file.sync_all())
        .map_err(|_| Failure::ProfileIo)?;
    Ok(())
}
async fn doctor_profile(dir: &Path, name: &str) -> Result<(Vec<u8>, bool), Failure> {
    directory(dir, false)?;
    let path = dir.join(format!("{name}.json"));
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        if fs::symlink_metadata(&path)
            .map_err(|_| Failure::ProfileIo)?
            .permissions()
            .mode()
            & 0o077
            != 0
        {
            return Err(Failure::ProfileIo);
        }
    }
    let profile = ProfileV1::decode(&bounded_file(&path, 16 * 1024)?)?;
    let bearer = token(&profile.token_env)?;
    let ca = read_ca(profile.ca_file.as_deref())?;
    let end = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_| Failure::Configuration)?
        .as_secs();
    let report = GrafanaV1::new(profile, &bearer, ca.as_deref())?
        .doctor(end)
        .await?;
    let mut bytes = serde_json::to_vec(&report).map_err(|_| Failure::Configuration)?;
    if bytes.len() > 16 * 1024 {
        return Err(Failure::Configuration);
    }
    bytes.push(b'\n');
    Ok((bytes, report.ready))
}
