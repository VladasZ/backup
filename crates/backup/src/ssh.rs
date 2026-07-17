use std::fs::{self, File};
use std::io::{BufRead, BufReader, ErrorKind, Read, Write, copy};
use std::path::Path;
use std::process::{Child, ChildStdout, Command, Stdio};
use std::thread::{self, JoinHandle};

use anyhow::{Context, Result, anyhow, bail};
use serde::de::DeserializeOwned;
use serde_json::{Value, from_str, from_value, to_writer};

use crate::archive::{Artifact, verify_checksum, write_checksum};
use crate::config::{BackupJob, CompressionConfig};
use crate::destination::ArchiveInfo;
use crate::location::SshLocation;
use crate::protocol::{
    AgentRequest, PROTOCOL_VERSION, PingResponse, RESPONSE_PREFIX, ResponseEnvelope,
    WireArchiveInfo, WireArtifact,
};

pub fn validate_agent(remote: &SshLocation) -> Result<()> {
    let response: PingResponse = simple_request(remote, &AgentRequest::Ping)?;
    if response.protocol != PROTOCOL_VERSION {
        bail!(
            "remote backup protocol {} is incompatible with local protocol {PROTOCOL_VERSION}",
            response.protocol
        );
    }
    Ok(())
}

pub fn validate_source(remote: &SshLocation) -> Result<()> {
    let response: Value = simple_request(
        remote,
        &AgentRequest::ValidateSource {
            path: remote.path.clone(),
        },
    )?;
    drop(response);
    Ok(())
}

pub fn validate_destination(remote: &SshLocation) -> Result<()> {
    let response: Value = simple_request(
        remote,
        &AgentRequest::ValidateDestination {
            path: remote.path.clone(),
        },
    )?;
    drop(response);
    Ok(())
}

pub fn create_remote_archive(
    job: &BackupJob,
    compression: &CompressionConfig,
    remote: &SshLocation,
    staging: &Path,
) -> Result<Artifact> {
    fs::create_dir_all(staging)?;
    let request = AgentRequest::Create {
        job: job.name.clone(),
        source: remote.path.clone(),
        exclude: job.exclude.clone(),
        compression: compression.clone(),
    };
    let (mut child, mut output, stderr) = spawn_stream(remote, &request)?;
    let wire: WireArtifact = match read_response(&mut output) {
        Ok(wire) => wire,
        Err(error) => {
            terminate(&mut child)?;
            let stderr = join_stderr(stderr)?;
            bail!("read remote archive response: {error:#}; SSH stderr: {stderr}");
        }
    };
    let final_path = staging.join(&wire.name);
    let partial_path = staging.join(format!(".{}.partial", wire.name));
    remove_if_present(&partial_path)?;
    let receive_result = (|| {
        let mut file = File::create(&partial_path)?;
        let copied = copy(&mut output.by_ref().take(wire.size), &mut file)?;
        file.flush()?;
        file.sync_all()?;
        if copied != wire.size {
            bail!(
                "remote archive ended after {copied} bytes, expected {}",
                wire.size
            );
        }
        verify_checksum(&partial_path, &wire.checksum)
    })();
    if let Err(error) = receive_result {
        terminate(&mut child)?;
        let stderr = join_stderr(stderr)?;
        remove_if_present(&partial_path)?;
        bail!("receive remote archive: {error:#}; SSH stderr: {stderr}");
    }
    let status = child.wait()?;
    let stderr = join_stderr(stderr)?;
    if !status.success() {
        remove_if_present(&partial_path)?;
        bail!("remote archive command failed: {stderr}");
    }
    fs::rename(&partial_path, &final_path)?;
    let checksum_path = write_checksum(&final_path, &wire.checksum)?;
    File::open(staging)?.sync_all()?;
    Ok(Artifact {
        name: wire.name,
        path: final_path,
        checksum_path,
        checksum: wire.checksum,
        size: wire.size,
        created_at: wire.created_at,
    })
}

pub fn deliver_remote(artifact: &Artifact, remote: &SshLocation, job: &BackupJob) -> Result<()> {
    let request = AgentRequest::Receive {
        artifact: WireArtifact::from_artifact(artifact),
        destination: remote.path.clone(),
        job: job.clone(),
    };
    let mut child = ssh_command(remote)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .context("start SSH destination agent")?;
    let stderr = drain_stderr(&mut child)?;
    let send_result = (|| {
        let mut input = child.stdin.take().context("SSH stdin is unavailable")?;
        write_request(&mut input, &request)?;
        let mut file = File::open(&artifact.path)?;
        let copied = copy(&mut file, &mut input)?;
        if copied != artifact.size {
            bail!("local staged archive changed while sending");
        }
        input.flush()?;
        Ok(())
    })();
    let output = child.wait_with_output()?;
    let stderr = join_stderr(stderr)?;
    send_result?;
    if !output.status.success() {
        bail!("remote destination command failed: {stderr}");
    }
    let mut reader = BufReader::new(output.stdout.as_slice());
    let response: Value = read_response(&mut reader)?;
    drop(response);
    Ok(())
}

pub fn list_remote(remote: &SshLocation, job: &str) -> Result<Vec<ArchiveInfo>> {
    let archives: Vec<WireArchiveInfo> = simple_request(
        remote,
        &AgentRequest::List {
            destination: remote.path.clone(),
            job: job.to_owned(),
        },
    )?;
    Ok(archives
        .into_iter()
        .map(|archive| ArchiveInfo {
            path: remote.path.join(&archive.name),
            name: archive.name,
            checksum: archive.checksum,
            size: archive.size,
            modified: archive.modified,
        })
        .collect())
}

pub fn fetch_remote(
    remote: &SshLocation,
    archive: &Path,
    checksum: &str,
    destination: &Path,
) -> Result<()> {
    let request = AgentRequest::Send {
        archive: archive.to_path_buf(),
        checksum: checksum.to_owned(),
    };
    let (mut child, mut output, stderr) = spawn_stream(remote, &request)?;
    let wire: WireArtifact = match read_response(&mut output) {
        Ok(wire) => wire,
        Err(error) => {
            terminate(&mut child)?;
            let stderr = join_stderr(stderr)?;
            bail!("read remote download response: {error:#}; SSH stderr: {stderr}");
        }
    };
    let receive_result = (|| {
        let mut file = File::create(destination)?;
        let copied = copy(&mut output.by_ref().take(wire.size), &mut file)?;
        file.flush()?;
        file.sync_all()?;
        if copied != wire.size {
            bail!("downloaded {copied} bytes, expected {}", wire.size);
        }
        verify_checksum(destination, &wire.checksum)
    })();
    if let Err(error) = receive_result {
        terminate(&mut child)?;
        let stderr = join_stderr(stderr)?;
        remove_if_present(destination)?;
        bail!("download remote archive: {error:#}; SSH stderr: {stderr}");
    }
    let status = child.wait()?;
    let stderr = join_stderr(stderr)?;
    if !status.success() {
        remove_if_present(destination)?;
        bail!("remote archive download failed: {stderr}");
    }
    Ok(())
}

pub fn restore_remote(artifact: &Artifact, remote: &SshLocation) -> Result<()> {
    let request = AgentRequest::Restore {
        artifact: WireArtifact::from_artifact(artifact),
        target: remote.path.clone(),
    };
    send_archive_request(remote, artifact, &request)
}

pub fn verify_remote(remote: &SshLocation, archive: &Path, checksum: &str) -> Result<()> {
    let response: Value = simple_request(
        remote,
        &AgentRequest::Verify {
            archive: archive.to_path_buf(),
            checksum: checksum.to_owned(),
        },
    )?;
    drop(response);
    Ok(())
}

pub fn prune_remote(remote: &SshLocation, job: &BackupJob) -> Result<()> {
    let response: Value = simple_request(
        remote,
        &AgentRequest::Prune {
            destination: remote.path.clone(),
            job: job.clone(),
        },
    )?;
    drop(response);
    Ok(())
}

fn send_archive_request(
    remote: &SshLocation,
    artifact: &Artifact,
    request: &AgentRequest,
) -> Result<()> {
    let mut child = ssh_command(remote)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()?;
    let stderr = drain_stderr(&mut child)?;
    let send_result = (|| {
        let mut input = child.stdin.take().context("SSH stdin is unavailable")?;
        write_request(&mut input, request)?;
        let mut file = File::open(&artifact.path)?;
        let copied = copy(&mut file, &mut input)?;
        if copied != artifact.size {
            bail!("local staged archive changed while sending");
        }
        input.flush()?;
        Ok(())
    })();
    let output = child.wait_with_output()?;
    let stderr = join_stderr(stderr)?;
    send_result?;
    if !output.status.success() {
        bail!("remote restore failed: {stderr}");
    }
    let mut reader = BufReader::new(output.stdout.as_slice());
    let response: Value = read_response(&mut reader)?;
    drop(response);
    Ok(())
}

fn simple_request<T: DeserializeOwned>(remote: &SshLocation, request: &AgentRequest) -> Result<T> {
    let mut child = ssh_command(remote)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .context("start SSH agent")?;
    {
        let mut input = child.stdin.take().context("SSH stdin is unavailable")?;
        write_request(&mut input, request)?;
    }
    let output = child.wait_with_output()?;
    if !output.status.success() {
        bail!(
            "SSH agent failed: {}",
            String::from_utf8_lossy(&output.stderr).trim()
        );
    }
    let mut reader = BufReader::new(output.stdout.as_slice());
    read_response(&mut reader)
}

fn spawn_stream(
    remote: &SshLocation,
    request: &AgentRequest,
) -> Result<(Child, BufReader<ChildStdout>, JoinHandle<Result<String>>)> {
    let mut child = ssh_command(remote)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()?;
    let stderr = drain_stderr(&mut child)?;
    {
        let mut input = child.stdin.take().context("SSH stdin is unavailable")?;
        write_request(&mut input, request)?;
    }
    let output = child.stdout.take().context("SSH stdout is unavailable")?;
    Ok((child, BufReader::new(output), stderr))
}

fn ssh_command(remote: &SshLocation) -> Command {
    let mut command = Command::new("ssh");
    command.args([
        "-T",
        "-o",
        "BatchMode=yes",
        "-o",
        "ConnectTimeout=15",
        "-o",
        "ServerAliveInterval=30",
        "-o",
        "ServerAliveCountMax=3",
    ]);
    if let Some(port) = remote.port {
        command.arg("-p").arg(port.to_string());
    }
    command.arg(remote.target()).args(["backup", "agent"]);
    command
}

fn write_request(writer: &mut dyn Write, request: &AgentRequest) -> Result<()> {
    to_writer(&mut *writer, request)?;
    writeln!(writer)?;
    writer.flush()?;
    Ok(())
}

fn read_response<T: DeserializeOwned>(reader: &mut dyn BufRead) -> Result<T> {
    let mut line = String::new();
    let mut bytes = 0;
    loop {
        line.clear();
        let read = reader.read_line(&mut line)?;
        if read == 0 {
            bail!("agent closed the connection without a response");
        }
        bytes += read;
        if bytes > 64 * 1024 {
            bail!("agent response prefix was not found within 64 KiB");
        }
        if let Some(json) = line.strip_prefix(RESPONSE_PREFIX) {
            let envelope: ResponseEnvelope = from_str(json)?;
            if !envelope.ok {
                bail!(
                    "remote agent error: {}",
                    envelope.error.unwrap_or_else(|| "unknown error".to_owned())
                );
            }
            let data = envelope.data.context("agent response has no data")?;
            return from_value(data).context("decode agent response");
        }
    }
}

fn drain_stderr(child: &mut Child) -> Result<JoinHandle<Result<String>>> {
    let mut stderr = child.stderr.take().context("SSH stderr is unavailable")?;
    Ok(thread::spawn(move || {
        let mut message = String::new();
        stderr.read_to_string(&mut message)?;
        Ok(message)
    }))
}

fn join_stderr(handle: JoinHandle<Result<String>>) -> Result<String> {
    handle
        .join()
        .map_err(|_| anyhow!("SSH stderr reader thread panicked"))?
}

fn terminate(child: &mut Child) -> Result<()> {
    match child.kill() {
        Ok(()) => {}
        Err(error) if error.kind() == ErrorKind::InvalidInput => {}
        Err(error) => return Err(error).context("terminate SSH command"),
    }
    child.wait().context("wait for terminated SSH command")?;
    Ok(())
}

fn remove_if_present(path: &Path) -> Result<()> {
    match fs::remove_file(path) {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error).with_context(|| format!("remove {}", path.display())),
    }
}
