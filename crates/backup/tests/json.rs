use std::fs;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Output, Stdio};
use std::thread::sleep;
use std::time::{Duration, Instant};

use chrono::{Timelike, Utc};
use serde::Deserialize;
use tempfile::{TempDir, tempdir};

const BINARY: &str = env!("CARGO_BIN_EXE_backup");
const HEALTHY_WAIT: Duration = Duration::from_secs(20);

#[derive(Debug, Deserialize)]
struct Envelope<T> {
    ok: bool,
    error: Option<String>,
    data: T,
}

#[derive(Debug, Deserialize)]
#[serde(tag = "event", rename_all = "snake_case")]
enum Event {
    BackupStarted {
        job: String,
        archive: String,
    },
    DestinationCompleted {
        destination: String,
    },

    #[serde(other)]
    Other,
}

#[derive(Debug, Deserialize)]
struct RunData {
    job: String,
    archive: String,
    size: u64,
    delivered: usize,
    failed: usize,
    staged: bool,
}

#[derive(Debug, Deserialize)]
struct StatusRow {}

#[derive(Debug, Deserialize)]
struct ListRow {
    destination: String,
    name: String,
    size: u64,
    checksum_missing: bool,
}

#[derive(Debug, Deserialize)]
struct HealthData {
    healthy: bool,
    daemon_running: bool,
    jobs: Vec<JobData>,
}

#[derive(Debug, Deserialize)]
struct JobData {
    name: String,
    healthy: bool,
    problems: Vec<String>,
}

struct Sandbox {
    root: TempDir,
}

impl Sandbox {
    fn new() -> Self {
        let root = tempdir().expect("create sandbox");
        for directory in ["home", "state", "source", "destination"] {
            fs::create_dir_all(root.path().join(directory)).expect("create sandbox directory");
        }
        fs::write(root.path().join("source/keep.txt"), "keep me").expect("write source file");
        Self { root }
    }

    fn path(&self, name: &str) -> PathBuf {
        self.root.path().join(name)
    }

    fn write_config(&self, cron: &str) {
        fs::write(
            self.path("config.toml"),
            format!(
                r#"[[backup]]
name = "documents"
source = "{}"
destinations = ["{}"]
cron = "{cron}"
"#,
                self.path("source").display(),
                self.path("destination").display()
            ),
        )
        .expect("write configuration");
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

fn stdout_lines(output: &Output) -> Vec<String> {
    String::from_utf8_lossy(&output.stdout)
        .lines()
        .map(str::to_owned)
        .collect()
}

fn parse<T: for<'de> Deserialize<'de>>(line: &str) -> T {
    serde_json::from_str(line).unwrap_or_else(|error| panic!("bad JSON line {line:?}: {error}"))
}

fn cron_half_a_day_ago() -> String {
    let hour = (Utc::now().hour() + 12) % 24;
    format!("0 {hour} * * *")
}

#[test]
fn json_run_streams_events_and_ends_with_a_result_envelope() {
    let sandbox = Sandbox::new();
    sandbox.write_config("0 2 * * *");

    let output = sandbox.run(&["--json", "run", "documents"]);
    let lines = stdout_lines(&output);
    assert!(lines.len() >= 3, "expected events and a result: {lines:?}");

    let events: Vec<Event> = lines[..lines.len() - 1]
        .iter()
        .map(|line| parse(line))
        .collect();
    assert!(matches!(
        &events[0],
        Event::BackupStarted { job, archive } if job == "documents" && archive.starts_with("documents-")
    ));
    let destination = sandbox.path("destination").display().to_string();
    assert!(events.iter().any(
        |event| matches!(event, Event::DestinationCompleted { destination: seen } if *seen == destination)
    ));

    let envelope: Envelope<RunData> = parse(lines.last().expect("result line"));
    assert!(envelope.ok);
    assert!(envelope.error.is_none());
    assert_eq!(envelope.data.job, "documents");
    assert!(envelope.data.archive.starts_with("documents-"));
    assert!(envelope.data.size > 0);
    assert_eq!(envelope.data.delivered, 1);
    assert_eq!(envelope.data.failed, 0);
    assert!(!envelope.data.staged);
}

#[test]
fn json_status_list_and_verify_report_structured_data() {
    let sandbox = Sandbox::new();
    sandbox.write_config("0 2 * * *");
    sandbox.run(&["run", "documents"]);

    let status = sandbox.run(&["--json", "status"]);
    let envelope: Envelope<Vec<StatusRow>> = parse(&stdout_lines(&status)[0]);
    assert!(envelope.ok);
    assert!(envelope.data.is_empty());

    let list = sandbox.run(&["--json", "list", "documents"]);
    let envelope: Envelope<Vec<ListRow>> = parse(&stdout_lines(&list)[0]);
    assert!(envelope.ok);
    assert_eq!(envelope.data.len(), 1);
    let row = &envelope.data[0];
    assert!(row.name.starts_with("documents-"));
    assert!(row.size > 0);
    assert!(!row.checksum_missing);
    assert_eq!(
        row.destination,
        sandbox.path("destination").display().to_string()
    );

    let verify = sandbox.run(&["--json", "verify", "documents"]);
    let lines = stdout_lines(&verify);
    #[derive(Deserialize)]
    struct VerifyData {
        verified: usize,
    }
    let envelope: Envelope<VerifyData> = parse(lines.last().expect("verify result"));
    assert!(envelope.ok);
    assert_eq!(envelope.data.verified, 1);
}

#[test]
fn json_failures_use_the_error_envelope() {
    let sandbox = Sandbox::new();
    sandbox.write_config("0 2 * * *");

    let output = sandbox
        .command(&["--json", "run", "missing"])
        .output()
        .expect("run backup");

    assert!(!output.status.success());
    let envelope: Envelope<()> = parse(&stdout_lines(&output)[0]);
    assert!(!envelope.ok);
    assert!(
        envelope
            .error
            .as_deref()
            .is_some_and(|error| error.contains("unknown backup job")),
        "unexpected error: {:?}",
        envelope.error
    );
}

#[test]
fn health_is_unhealthy_without_a_daemon_and_with_a_missed_slot() {
    let sandbox = Sandbox::new();
    sandbox.write_config(&cron_half_a_day_ago());

    let output = sandbox
        .command(&["health"])
        .output()
        .expect("run backup health");

    assert_eq!(output.status.code(), Some(1));
    let text = String::from_utf8_lossy(&output.stdout);
    assert!(
        text.contains("daemon: not running"),
        "missing daemon line:\n{text}"
    );
    assert!(
        text.contains("has not completed"),
        "missing slot problem:\n{text}"
    );
    assert!(text.contains("unhealthy"), "missing verdict:\n{text}");

    let json = sandbox
        .command(&["--json", "health"])
        .output()
        .expect("run backup health json");
    assert_eq!(json.status.code(), Some(1));
    let envelope: Envelope<HealthData> = parse(&stdout_lines(&json)[0]);
    assert!(envelope.ok);
    assert!(!envelope.data.healthy);
    assert!(!envelope.data.daemon_running);
    assert_eq!(envelope.data.jobs[0].name, "documents");
    assert!(!envelope.data.jobs[0].healthy);
    assert!(!envelope.data.jobs[0].problems.is_empty());
}

struct Daemon {
    child: Child,
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

#[test]
fn health_is_healthy_with_a_running_daemon_and_handled_slots() {
    let sandbox = Sandbox::new();
    sandbox.write_config(&cron_half_a_day_ago());

    let child = sandbox
        .command(&["daemon"])
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .expect("start daemon");
    let daemon = Daemon { child };

    let deadline = Instant::now() + HEALTHY_WAIT;
    let mut last = String::new();
    while Instant::now() < deadline {
        let output = sandbox
            .command(&["--json", "health"])
            .output()
            .expect("run backup health");
        last = String::from_utf8_lossy(&output.stdout).into_owned();
        if output.status.success() {
            let envelope: Envelope<HealthData> = parse(last.lines().next().expect("health line"));
            assert!(envelope.data.healthy);
            assert!(envelope.data.daemon_running);
            assert!(envelope.data.jobs.iter().all(|job| job.healthy));
            drop(daemon);
            return;
        }
        sleep(Duration::from_millis(200));
    }
    panic!("health did not become healthy within {HEALTHY_WAIT:?}; last output:\n{last}");
}

#[test]
fn json_is_rejected_for_unsupported_commands() {
    let sandbox = Sandbox::new();
    sandbox.write_config("0 2 * * *");

    let output = sandbox
        .command(&["--json", "logs"])
        .output()
        .expect("run backup logs");

    assert!(!output.status.success());
    let envelope: Envelope<()> = parse(&stdout_lines(&output)[0]);
    assert!(!envelope.ok);
    assert!(
        envelope
            .error
            .as_deref()
            .is_some_and(|error| error.contains("not supported"))
    );
}

fn archives(destination: &Path) -> Vec<String> {
    let Ok(entries) = fs::read_dir(destination) else {
        return Vec::new();
    };
    entries
        .map(|entry| {
            entry
                .expect("read destination entry")
                .file_name()
                .to_string_lossy()
                .into_owned()
        })
        .filter(|name| name.ends_with(".tar.lz4"))
        .collect()
}

#[test]
fn text_output_is_unchanged_without_the_flag() {
    let sandbox = Sandbox::new();
    sandbox.write_config("0 2 * * *");

    let run = sandbox.run(&["run", "documents"]);
    assert!(String::from_utf8_lossy(&run.stdout).contains("completed or staged for retry"));
    assert_eq!(archives(&sandbox.path("destination")).len(), 1);

    let status = sandbox.run(&["status"]);
    assert!(String::from_utf8_lossy(&status.stdout).contains("no pending deliveries"));
}
