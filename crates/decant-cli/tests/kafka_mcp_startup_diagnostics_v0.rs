use std::{
    ffi::OsString,
    fs,
    path::{Path, PathBuf},
    process::{Command, Output},
    sync::atomic::{AtomicUsize, Ordering},
};

use decant_storage::{
    LOCAL_CHECKPOINT_WRITER_LOCK_FILE_V0, LocalCheckpointStoreV0, LocalCheckpointWriterLockErrorV0,
    acquire_local_checkpoint_writer_lock_v0,
};

const OWNERSHIP_ERROR: &[u8] = b"error: Kafka MCP service failed (checkpoint_in_use)\nhint: stop the existing Decant MCP server before restarting; do not delete its lock file\n";
const SERVICE_ERROR: &[u8] = b"error: Kafka MCP service failed\n";
const PRIVATE_MARKER: &str = "synthetic-private-task098";
const USERNAME_ENV: &str = "DECANT_TASK098_PRIVATE_USERNAME";
const PASSWORD_ENV: &str = "DECANT_TASK098_PRIVATE_PASSWORD";
const ENVIRONMENT_SENTINEL: &str = "synthetic-task098-environment-value";

static NEXT_CASE: AtomicUsize = AtomicUsize::new(0);

struct TempCase(PathBuf);

impl TempCase {
    fn new() -> Self {
        let ordinal = NEXT_CASE.fetch_add(1, Ordering::Relaxed);
        let path = std::env::temp_dir().join(format!(
            "decant-startup-{PRIVATE_MARKER}-{}-{ordinal}",
            std::process::id()
        ));
        fs::create_dir(&path).unwrap();
        Self(path)
    }

    fn checkpoints(&self) -> PathBuf {
        let path = self.0.join("checkpoints");
        fs::create_dir(&path).unwrap();
        path
    }

    fn config(&self, checkpoints: &Path) -> PathBuf {
        let mut document: serde_json::Value =
            serde_json::from_str(include_str!("../../../examples/kafka-mcp/config-v0.json"))
                .unwrap();
        document["checkpoint_directory"] = serde_json::json!(checkpoints);
        document["bootstrap_servers"] = serde_json::json!(format!("{PRIVATE_MARKER}.invalid:9092"));
        document["topic"] = serde_json::json!(format!("{PRIVATE_MARKER}-topic"));
        document["expected_cluster_id"] = serde_json::json!(format!("{PRIVATE_MARKER}-cluster"));
        document["context"]["view_id"] = serde_json::json!(format!("{PRIVATE_MARKER}-view"));
        document["security"]["username_env"] = serde_json::json!(USERNAME_ENV);
        document["security"]["password_env"] = serde_json::json!(PASSWORD_ENV);
        let path = self.0.join("config.json");
        fs::write(&path, serde_json::to_vec(&document).unwrap()).unwrap();
        path
    }
}

impl Drop for TempCase {
    fn drop(&mut self) {
        fs::remove_dir_all(&self.0).unwrap();
    }
}

fn run_config(path: &Path) -> Output {
    Command::new(env!("CARGO_BIN_EXE_decant"))
        .args(["serve", "kafka-mcp", "--config"])
        .arg(path)
        .env(USERNAME_ENV, ENVIRONMENT_SENTINEL)
        // If bootstrap reaches client setup, missing credentials fail before
        // a native client is created. No test needs a reachable broker.
        .env_remove(PASSWORD_ENV)
        .output()
        .unwrap()
}

fn assert_failure(output: &Output, expected: &[u8]) {
    assert_eq!(output.status.code(), Some(1));
    assert!(output.stdout.is_empty(), "unexpected MCP stdout");
    // Never dump unexpected captured bytes into a failing test's diagnostics.
    assert!(output.stderr == expected, "unexpected service diagnostic");
    for sentinel in [
        PRIVATE_MARKER,
        USERNAME_ENV,
        PASSWORD_ENV,
        ENVIRONMENT_SENTINEL,
    ] {
        assert!(
            !output
                .stderr
                .windows(sentinel.len())
                .any(|bytes| bytes == sentinel.as_bytes()),
            "sensitive sentinel in service diagnostic"
        );
    }
}

fn directory_bytes(directory: &Path) -> Vec<(OsString, Vec<u8>)> {
    let mut entries: Vec<_> = fs::read_dir(directory)
        .unwrap()
        .map(|entry| {
            let entry = entry.unwrap();
            (entry.file_name(), fs::read(entry.path()).unwrap())
        })
        .collect();
    entries.sort_by(|left, right| left.0.cmp(&right.0));
    entries
}

#[test]
fn held_owner_reports_exact_error_without_touching_checkpoint_files() {
    let case = TempCase::new();
    let checkpoints = case.checkpoints();
    let config = case.config(&checkpoints);
    // Ownership must win even over an unreadable checkpoint's contents:
    // the second process must not recover or rewrite the current owner's data.
    fs::write(
        checkpoints.join("checkpoint-00000000000000000001.dcp"),
        PRIVATE_MARKER.as_bytes(),
    )
    .unwrap();
    let store = LocalCheckpointStoreV0::open(&checkpoints).unwrap();
    let owner = acquire_local_checkpoint_writer_lock_v0(&store).unwrap();
    let before = directory_bytes(&checkpoints);

    for _ in 0..2 {
        assert_failure(&run_config(&config), OWNERSHIP_ERROR);
        assert!(
            directory_bytes(&checkpoints) == before,
            "checkpoint files changed"
        );
        assert!(matches!(
            acquire_local_checkpoint_writer_lock_v0(&store),
            Err(LocalCheckpointWriterLockErrorV0::AlreadyHeld)
        ));
    }

    drop(owner);
    let next_owner = acquire_local_checkpoint_writer_lock_v0(&store).unwrap();
    assert!(
        directory_bytes(&checkpoints) == before,
        "lock file was replaced"
    );
    drop(next_owner);
}

#[test]
fn malformed_lock_entry_remains_generic_and_is_not_deleted() {
    let case = TempCase::new();
    let checkpoints = case.checkpoints();
    let config = case.config(&checkpoints);
    fs::write(
        checkpoints.join(LOCAL_CHECKPOINT_WRITER_LOCK_FILE_V0),
        PRIVATE_MARKER.as_bytes(),
    )
    .unwrap();
    let before = directory_bytes(&checkpoints);
    assert_failure(&run_config(&config), SERVICE_ERROR);
    assert!(
        directory_bytes(&checkpoints) == before,
        "lock entry changed"
    );
}

#[test]
fn corrupt_checkpoint_without_an_owner_remains_generic_and_unchanged() {
    let case = TempCase::new();
    let checkpoints = case.checkpoints();
    let config = case.config(&checkpoints);
    let store = LocalCheckpointStoreV0::open(&checkpoints).unwrap();
    drop(acquire_local_checkpoint_writer_lock_v0(&store).unwrap());
    fs::write(
        checkpoints.join("checkpoint-00000000000000000001.dcp"),
        PRIVATE_MARKER.as_bytes(),
    )
    .unwrap();
    let before = directory_bytes(&checkpoints);
    assert_failure(&run_config(&config), SERVICE_ERROR);
    assert!(
        directory_bytes(&checkpoints) == before,
        "checkpoint files changed"
    );
}

#[test]
fn malformed_config_keeps_the_generic_value_safe_error() {
    let case = TempCase::new();
    let config = case.0.join("config.json");
    fs::write(&config, format!("{{{PRIVATE_MARKER}")).unwrap();
    assert_failure(&run_config(&config), SERVICE_ERROR);
}
