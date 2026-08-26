use std::fs;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Output, Stdio};
use std::thread::sleep;
use std::time::{Duration, Instant};

use chrono::{Timelike, Utc};
use tempfile::{TempDir, tempdir};

const BINARY: &str = env!("CARGO_BIN_EXE_backup");
const ARCHIVE_WAIT: Duration = Duration::from_secs(20);
const IDLE_WATCH: Duration = Duration::from_millis(2500);

struct Sandbox {
    root: TempDir,
}

impl Sandbox {
    fn new() -> Self {
        let root = tempdir().expect("create sandbox");
        for directory in ["home", "state", "source", "destination"] {
            fs::create_dir_all(root.path().join(directory)).expect("create sandbox directory");
        }
        Self { root }
    }

    fn path(&self, name: &str) -> PathBuf {
        self.root.path().join(name)
    }

    fn source(&self) -> PathBuf {
        self.path("source")
    }

    fn destination(&self) -> PathBuf {
        self.path("destination")
    }

    fn write_config(&self, contents: &str) {
        fs::write(self.path("config.toml"), contents).expect("write configuration");
    }

    fn command(&self, arguments: &[&str]) -> Command {
        let mut command = Command::new(BINARY);
        command
            .arg("--config")
            .arg(self.path("config.toml"))
            .args(arguments)
            .env("HOME", self.path("home"))
            .env("XDG_CONFIG_HOME", self.path("home"))
            .env("XDG_DATA_HOME", self.path("state"))
            .env("XDG_STATE_HOME", self.path("state"));
        command
    }

    fn run(&self, arguments: &[&str]) -> Output {
        let output = self.command(arguments).output().expect("run backup");
        assert!(
            output.status.success(),
            "backup {arguments:?} failed: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        output
    }
}

struct Daemon {
    child: Child,
}

impl Daemon {
    fn start(sandbox: &Sandbox) -> Self {
        let child = sandbox
            .command(&["daemon"])
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .expect("start daemon");
        Self { child }
    }
}

impl Drop for Daemon {
    fn drop(&mut self) {
        if let Err(error) = self.child.kill() {
            eprintln!("could not stop the test daemon: {error}");
        }
        if let Err(error) = self.child.wait() {
            eprintln!("could not reap the test daemon: {error}");
        }
    }
}

fn job_config(name: &str, sandbox: &Sandbox, cron: &str, extra: &str) -> String {
    format!(
        r#"[[backup]]
name = "{name}"
source = "{}"
destinations = ["{}"]
cron = "{cron}"
{extra}
"#,
        sandbox.source().display(),
        sandbox.destination().display()
    )
}

fn cron_half_a_day_ago() -> String {
    let hour = (Utc::now().hour() + 12) % 24;
    format!("0 {hour} * * *")
}

fn archives(destination: &Path) -> Vec<String> {
    let Ok(entries) = fs::read_dir(destination) else {
        return Vec::new();
    };
    let mut names: Vec<String> = entries
        .map(|entry| {
            entry
                .expect("read destination entry")
                .file_name()
                .to_string_lossy()
                .into_owned()
        })
        .filter(|name| name.ends_with(".tar.lz4"))
        .collect();
    names.sort();
    names
}

fn wait_for_archives(destination: &Path) -> Vec<String> {
    let deadline = Instant::now() + ARCHIVE_WAIT;
    while Instant::now() < deadline {
        let found = archives(destination);
        if !found.is_empty() {
            return found;
        }
        sleep(Duration::from_millis(50));
    }
    panic!(
        "no archive appeared at {} within {ARCHIVE_WAIT:?}",
        destination.display()
    );
}

#[test]
fn run_delivers_an_archive_that_lists_verifies_and_restores() {
    let sandbox = Sandbox::new();
    fs::create_dir_all(sandbox.source().join("nested")).expect("create nested source directory");
    fs::write(sandbox.source().join("keep.txt"), "keep me").expect("write source file");
    fs::write(sandbox.source().join("nested/deep.txt"), "deeper").expect("write nested file");
    fs::write(sandbox.source().join("skip.tmp"), "skip me").expect("write excluded file");
    sandbox.write_config(&job_config(
        "documents",
        &sandbox,
        "0 2 * * *",
        r#"exclude = ["*.tmp"]"#,
    ));

    sandbox.run(&["validate"]);
    sandbox.run(&["run", "documents"]);

    let delivered = archives(&sandbox.destination());
    assert_eq!(delivered.len(), 1);
    assert!(delivered[0].starts_with("documents-"));
    assert!(delivered[0].ends_with(".tar.lz4"));

    let listed = sandbox.run(&["list", "documents"]);
    assert!(String::from_utf8_lossy(&listed.stdout).contains(&delivered[0]));

    sandbox.run(&["verify", "documents"]);

    let restored = sandbox.path("restored");
    sandbox.run(&[
        "restore",
        "documents",
        "--to",
        restored.to_str().expect("restore path is valid UTF-8"),
        "--yes",
    ]);

    assert_eq!(
        fs::read_to_string(restored.join("keep.txt")).expect("read restored file"),
        "keep me"
    );
    assert_eq!(
        fs::read_to_string(restored.join("nested/deep.txt")).expect("read restored nested file"),
        "deeper"
    );
    assert!(!restored.join("skip.tmp").exists());
}

#[test]
fn daemon_handles_a_due_slot_once_and_then_stays_idle() {
    let sandbox = Sandbox::new();
    fs::write(sandbox.source().join("keep.txt"), "keep me").expect("write source file");
    sandbox.write_config(&job_config(
        "documents",
        &sandbox,
        &cron_half_a_day_ago(),
        "",
    ));

    let daemon = Daemon::start(&sandbox);
    let first = wait_for_archives(&sandbox.destination());
    assert_eq!(first.len(), 1, "more than one archive appeared: {first:?}");

    sleep(IDLE_WATCH);

    assert_eq!(
        archives(&sandbox.destination()),
        first,
        "the daemon backed up a slot it had already handled"
    );
    drop(daemon);
}

#[test]
fn retention_keeps_only_the_configured_number_of_archives() {
    let sandbox = Sandbox::new();
    fs::write(sandbox.source().join("keep.txt"), "keep me").expect("write source file");
    sandbox.write_config(&job_config(
        "documents",
        &sandbox,
        "0 2 * * *",
        "retention = { count = 2 }",
    ));

    for _ in 0..4 {
        sandbox.run(&["run", "documents"]);
    }

    assert_eq!(archives(&sandbox.destination()).len(), 2);
}

#[test]
fn one_job_does_not_prune_a_job_whose_name_starts_with_its_own() {
    let sandbox = Sandbox::new();
    fs::write(sandbox.source().join("keep.txt"), "keep me").expect("write source file");
    let shared = sandbox.destination();
    sandbox.write_config(&format!(
        r#"[[backup]]
name = "docs"
source = "{source}"
destinations = ["{shared}"]
cron = "0 2 * * *"
retention = {{ count = 1 }}

[[backup]]
name = "docs-archive"
source = "{source}"
destinations = ["{shared}"]
cron = "0 3 * * *"
retention = {{ count = 1 }}
"#,
        source = sandbox.source().display(),
        shared = shared.display()
    ));

    sandbox.run(&["validate"]);
    sandbox.run(&["run", "docs-archive"]);
    sandbox.run(&["run", "docs"]);

    let names = archives(&shared);
    assert_eq!(
        names.len(),
        2,
        "an archive was pruned by the other job: {names:?}"
    );
    assert!(names.iter().any(|name| name.starts_with("docs-2")));
    assert!(names.iter().any(|name| name.starts_with("docs-archive-")));

    let listed = String::from_utf8_lossy(&sandbox.run(&["list", "docs"]).stdout).into_owned();
    assert!(
        !listed.contains("docs-archive-"),
        "list leaked the other job: {listed}"
    );
}

#[test]
fn validate_rejects_an_invalid_cron() {
    let sandbox = Sandbox::new();
    sandbox.write_config(&job_config("documents", &sandbox, "every tuesday", ""));

    let output = sandbox
        .command(&["validate"])
        .output()
        .expect("run backup validate");

    assert!(!output.status.success());
    assert!(
        String::from_utf8_lossy(&output.stderr).contains("cron"),
        "unexpected error: {}",
        String::from_utf8_lossy(&output.stderr)
    );
}
