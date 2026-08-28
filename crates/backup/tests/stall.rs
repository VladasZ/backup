use std::env;
use std::fs;
use std::os::unix::fs::PermissionsExt;
use std::path::Path;
use std::process::{Command, Output};
use std::time::{Duration, Instant};

const BINARY: &str = env!("CARGO_BIN_EXE_backup");
const STALL_BOUND: Duration = Duration::from_secs(30);

fn backup(root: &Path, arguments: &[&str]) -> Output {
    let path = format!(
        "{}:{}",
        root.join("bin").display(),
        env::var("PATH").unwrap_or_default()
    );
    Command::new(BINARY)
        .arg("--config")
        .arg(root.join("config.toml"))
        .args(arguments)
        .env("HOME", root.join("home"))
        .env("XDG_CONFIG_HOME", root.join("home"))
        .env("XDG_DATA_HOME", root.join("state"))
        .env("XDG_STATE_HOME", root.join("state"))
        .env("PATH", path)
        .env("BACKUP_STALL_TIMEOUT_SECONDS", "1")
        .output()
        .expect("run backup")
}

#[test]
fn a_stalled_remote_is_disconnected_and_kept_for_retry() {
    let sandbox = tempfile::tempdir().expect("create sandbox");
    let root = sandbox.path();
    for directory in ["home", "state", "source", "bin"] {
        fs::create_dir_all(root.join(directory)).expect("create sandbox directory");
    }
    fs::write(root.join("source/hello.txt"), "content").expect("write source file");

    // An ssh that connects and then never reads or writes anything, like a remote
    // whose disk hung while the connection stays alive.
    let shim = root.join("bin/ssh");
    fs::write(&shim, "#!/bin/sh\nexec sleep 600\n").expect("write ssh shim");
    fs::set_permissions(&shim, PermissionsExt::from_mode(0o755)).expect("make shim runnable");

    fs::write(
        root.join("config.toml"),
        format!(
            r#"[[backup]]
name = "documents"
source = "{}"
destinations = ["ssh://stalled-host/srv/backups"]
cron = "0 2 * * *"
"#,
            root.join("source").display()
        ),
    )
    .expect("write configuration");

    let started = Instant::now();
    let output = backup(root, &["run", "documents"]);
    let elapsed = started.elapsed();
    let stderr = String::from_utf8_lossy(&output.stderr);

    assert!(
        elapsed < STALL_BOUND,
        "the stalled remote was not disconnected within {STALL_BOUND:?}"
    );
    assert!(
        output.status.success(),
        "a stalled remote should be staged for retry, not fail the run:\n{stderr}"
    );
    assert!(
        stderr.contains("made no progress"),
        "the stall reason was lost: {stderr}"
    );

    let status = backup(root, &["status"]);
    assert!(status.status.success());
    let status = String::from_utf8_lossy(&status.stdout);
    assert!(
        status.contains("1 destination(s) pending"),
        "the stalled delivery was not kept for retry:\n{status}"
    );
}
