use std::fs;
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::process::{Command, Output, Stdio};
use std::thread::sleep;
use std::time::{Duration, Instant};

const BINARY: &str = env!("CARGO_BIN_EXE_backup");
const IMAGE: &str = "backup-test-sshd:1";
const CONTAINER: &str = "backup-test-sshd";
const READY_WAIT: Duration = Duration::from_secs(60);

fn workspace_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .ancestors()
        .nth(2)
        .expect("workspace root")
        .to_path_buf()
}

fn run(program: &str, arguments: &[&str]) -> Output {
    Command::new(program)
        .args(arguments)
        .output()
        .unwrap_or_else(|error| panic!("run {program} {arguments:?}: {error}"))
}

fn require(program: &str, arguments: &[&str]) -> String {
    let output = run(program, arguments);
    assert!(
        output.status.success(),
        "{program} {arguments:?} failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    String::from_utf8_lossy(&output.stdout).trim().to_owned()
}

fn docker_available() -> bool {
    Command::new("docker")
        .arg("info")
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .map(|status| status.success())
        .unwrap_or(false)
}

fn build_image() {
    let context = workspace_root().join("target/ssh-test");
    fs::create_dir_all(&context).expect("create image context");
    fs::write(
        context.join("Dockerfile"),
        r#"FROM debian:stable-slim
RUN apt-get update \
 && apt-get install -y --no-install-recommends openssh-server \
 && rm -rf /var/lib/apt/lists/*
RUN mkdir -p /run/sshd /root/.ssh /srv && chmod 700 /root/.ssh
RUN sed -i 's/^#*PermitRootLogin.*/PermitRootLogin prohibit-password/' /etc/ssh/sshd_config
CMD ["/usr/sbin/sshd", "-D", "-e"]
"#,
    )
    .expect("write Dockerfile");
    require(
        "docker",
        &[
            "build",
            "-t",
            IMAGE,
            context.to_str().expect("context path"),
        ],
    );
}

fn agent_binary() -> PathBuf {
    if cfg!(target_os = "linux") {
        return PathBuf::from(BINARY);
    }
    let root = workspace_root();
    let target = root.join("target/linux-agent");
    let built = target.join("release/backup");
    if !built.exists() {
        require(
            "docker",
            &[
                "run",
                "--rm",
                "-v",
                &format!("{}:/src", root.display()),
                "-v",
                "backup-ssh-test-cargo:/usr/local/cargo/registry",
                "-w",
                "/src",
                "rust:1.97.1-slim",
                "cargo",
                "build",
                "--release",
                "--target-dir",
                "/src/target/linux-agent",
            ],
        );
    }
    assert!(
        built.exists(),
        "no Linux agent binary at {}",
        built.display()
    );
    built
}

struct RemoteHost {
    port: u16,
    key: PathBuf,
}

impl RemoteHost {
    fn start(sandbox: &Path) -> Self {
        build_image();
        let key = sandbox.join("id_ed25519");
        require(
            "ssh-keygen",
            &[
                "-q",
                "-t",
                "ed25519",
                "-N",
                "",
                "-f",
                key.to_str().expect("key path"),
            ],
        );

        run("docker", &["rm", "-f", CONTAINER]);
        require(
            "docker",
            &[
                "run",
                "-d",
                "--name",
                CONTAINER,
                "-p",
                "127.0.0.1::22",
                IMAGE,
            ],
        );
        let published = require("docker", &["port", CONTAINER, "22"]);
        let port = published
            .lines()
            .next()
            .and_then(|line| line.rsplit(':').next())
            .and_then(|value| value.parse().ok())
            .unwrap_or_else(|| panic!("no published port in {published:?}"));

        let public = format!("{}.pub", key.display());
        require(
            "docker",
            &[
                "cp",
                &public,
                &format!("{CONTAINER}:/root/.ssh/authorized_keys"),
            ],
        );
        require(
            "docker",
            &[
                "exec",
                CONTAINER,
                "sh",
                "-c",
                "chown root:root /root/.ssh/authorized_keys && chmod 600 /root/.ssh/authorized_keys",
            ],
        );
        let agent = agent_binary();
        require(
            "docker",
            &[
                "cp",
                agent.to_str().expect("agent path"),
                &format!("{CONTAINER}:/usr/local/bin/backup"),
            ],
        );

        let host = Self { port, key };
        host.write_ssh_config(sandbox);
        host.wait_until_ready(sandbox);
        host
    }

    fn wait_until_ready(&self, sandbox: &Path) {
        let deadline = Instant::now() + READY_WAIT;
        while Instant::now() < deadline {
            let ok = Command::new("ssh")
                .args(self.ssh_arguments(sandbox))
                .args(["root@127.0.0.1", "backup", "--version"])
                .stdout(Stdio::null())
                .stderr(Stdio::null())
                .status()
                .map(|status| status.success())
                .unwrap_or(false);
            if ok {
                return;
            }
            sleep(Duration::from_millis(200));
        }
        panic!("remote host did not become reachable within {READY_WAIT:?}");
    }

    fn ssh_arguments(&self, sandbox: &Path) -> Vec<String> {
        vec![
            "-F".to_owned(),
            sandbox
                .join("ssh_config")
                .to_str()
                .expect("ssh config path")
                .to_owned(),
            "-p".to_owned(),
            self.port.to_string(),
        ]
    }

    fn write_ssh_config(&self, sandbox: &Path) {
        let config = sandbox.join("ssh_config");
        fs::write(
            &config,
            format!(
                r#"Host *
  IdentityFile {}
  IdentitiesOnly yes
  StrictHostKeyChecking no
  UserKnownHostsFile /dev/null
  LogLevel ERROR
"#,
                self.key.display()
            ),
        )
        .expect("write ssh config");

        let bin = sandbox.join("bin");
        fs::create_dir_all(&bin).expect("create sandbox bin");
        let real = require("sh", &["-c", "command -v ssh"]);
        let shim = bin.join("ssh");
        fs::write(
            &shim,
            format!("#!/bin/sh\nexec {real} -F {} \"$@\"\n", config.display()),
        )
        .expect("write ssh shim");
        fs::set_permissions(&shim, PermissionsExt::from_mode(0o755))
            .expect("make ssh shim runnable");
    }

    fn uri(&self, path: &str) -> String {
        format!("ssh://root@127.0.0.1:{}{path}", self.port)
    }

    fn exec(&self, script: &str) -> String {
        require("docker", &["exec", CONTAINER, "sh", "-c", script])
    }
}

impl Drop for RemoteHost {
    fn drop(&mut self) {
        let output = run("docker", &["rm", "-f", CONTAINER]);
        if !output.status.success() {
            eprintln!(
                "could not remove the test container: {}",
                String::from_utf8_lossy(&output.stderr)
            );
        }
    }
}

fn shim_path(sandbox: &Path) -> String {
    let existing = std::env::var("PATH").unwrap_or_default();
    format!("{}:{existing}", sandbox.join("bin").display())
}

fn backup(sandbox: &Path, arguments: &[&str]) -> Output {
    Command::new(BINARY)
        .arg("--config")
        .arg(sandbox.join("config.toml"))
        .args(arguments)
        .env("HOME", sandbox.join("home"))
        .env("XDG_CONFIG_HOME", sandbox.join("home"))
        .env("XDG_DATA_HOME", sandbox.join("state"))
        .env("XDG_STATE_HOME", sandbox.join("state"))
        .env("PATH", shim_path(sandbox))
        .output()
        .expect("run backup")
}

fn backup_ok(sandbox: &Path, arguments: &[&str]) -> String {
    let output = backup(sandbox, arguments);
    assert!(
        output.status.success(),
        "backup {arguments:?} failed:\n{}",
        String::from_utf8_lossy(&output.stderr)
    );
    String::from_utf8_lossy(&output.stdout).into_owned()
}

fn local_archives(destination: &Path) -> Vec<String> {
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

struct Case {
    name: &'static str,
    source: String,
    destination: String,
}

fn write_config(sandbox: &Path, case: &Case) {
    let body = format!(
        r#"[[backup]]
name = "{}"
source = "{}"
destinations = ["{}"]
cron = "0 2 * * *"
"#,
        case.name, case.source, case.destination
    );
    fs::write(sandbox.join("config.toml"), body).expect("write configuration");
}

#[test]
fn every_combination_of_local_and_ssh_endpoints_round_trips() {
    if !docker_available() {
        eprintln!("skipping: docker is not available");
        return;
    }

    let sandbox = tempfile::tempdir().expect("create sandbox");
    let root = sandbox.path().to_path_buf();
    for directory in ["home", "state", "source", "destination", "restored"] {
        fs::create_dir_all(root.join(directory)).expect("create sandbox directory");
    }
    fs::write(root.join("source/hello.txt"), "local content").expect("write local source");

    let remote = RemoteHost::start(&root);
    remote.exec("mkdir -p /srv/source && printf 'remote content' > /srv/source/hello.txt");

    let local_source = root.join("source").display().to_string();
    let local_destination = root.join("destination").display().to_string();
    let cases = [
        Case {
            name: "local-local",
            source: local_source.clone(),
            destination: local_destination.clone(),
        },
        Case {
            name: "ssh-local",
            source: remote.uri("/srv/source"),
            destination: local_destination.clone(),
        },
        Case {
            name: "local-ssh",
            source: local_source.clone(),
            destination: remote.uri("/srv/local-ssh"),
        },
        Case {
            name: "ssh-ssh",
            source: remote.uri("/srv/source"),
            destination: remote.uri("/srv/ssh-ssh"),
        },
    ];

    for case in &cases {
        write_config(&root, case);
        backup_ok(&root, &["validate"]);
        backup_ok(&root, &["run", case.name]);

        let listed = backup_ok(&root, &["list", case.name]);
        assert!(
            listed.contains(case.name),
            "{}: list showed no archive:\n{listed}",
            case.name
        );

        backup_ok(&root, &["verify", case.name]);

        let target = root.join("restored").join(case.name);
        backup_ok(
            &root,
            &[
                "restore",
                case.name,
                "--to",
                target.to_str().expect("restore path"),
                "--yes",
            ],
        );

        let expected = if case.source.starts_with("ssh://") {
            "remote content"
        } else {
            "local content"
        };
        assert_eq!(
            fs::read_to_string(target.join("hello.txt")).expect("read restored file"),
            expected,
            "{}: restored the wrong content",
            case.name
        );

        let status = backup_ok(&root, &["status"]);
        assert!(
            status.contains("no pending deliveries"),
            "{}: deliveries were left pending:\n{status}",
            case.name
        );
    }

    assert_eq!(local_archives(&root.join("destination")).len(), 2);
    assert_eq!(
        remote
            .exec("ls /srv/local-ssh/*.tar.lz4 /srv/ssh-ssh/*.tar.lz4 | wc -l")
            .trim(),
        "2"
    );
}
