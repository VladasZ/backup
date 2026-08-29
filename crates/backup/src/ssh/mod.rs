mod deploy;
mod session;
mod sink;
mod stall;

use std::fs::{self, File};
use std::io::{ErrorKind, Read, copy};
use std::path::Path;

use anyhow::{Context, Result, bail};
use serde_json::Value;

use crate::archive::{
    Artifact, StreamOutcome, Tee, abort_with, create_private, verify_checksum, warn_changed,
};
use crate::config::BackupJob;
use crate::destination::ArchiveInfo;
use crate::location::SshLocation;
use crate::protocol::{
    AgentRequest, PROTOCOL_VERSION, PingResponse, StreamHeader, StreamTrailer, WireArchiveInfo,
    WireArtifact, copy_frames,
};

use session::{Session, SshStream, read_response, simple_request};
pub use sink::SshSink;

const SEND_CHUNK: usize = 1024 * 1024;

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

pub struct RemoteStream {
    job: String,
    stream: SshStream,
    pub header: StreamHeader,
}

impl RemoteStream {
    pub fn start(job: &BackupJob, remote: &SshLocation) -> Result<Self> {
        let request = AgentRequest::Create {
            job: job.name.clone(),
            source: remote.path.clone(),
            exclude: job.exclude.clone(),
            pre: job.pre.clone(),
        };
        let mut stream = SshStream::spawn(remote, &request)?;
        let header: StreamHeader = match read_response(&mut stream.reader()) {
            Ok(header) => header,
            Err(error) => {
                stream.terminate()?;
                bail!(
                    "read remote archive response: {error:#}; {}",
                    stream.failure_detail()?
                );
            }
        };
        Ok(Self {
            job: job.name.clone(),
            stream,
            header,
        })
    }

    pub fn pump(mut self, mut tee: Tee) -> Result<StreamOutcome> {
        let copied = copy_frames(&mut self.stream.reader(), &mut tee);
        let received =
            copied.and_then(|_| read_response::<StreamTrailer>(&mut self.stream.reader()));
        let trailer = match received {
            Ok(trailer) => trailer,
            Err(error) => {
                let error = abort_with(tee, error);
                self.stream.terminate()?;
                bail!(
                    "receive remote archive: {error:#}; {}",
                    self.stream.failure_detail()?
                );
            }
        };
        let status = self.stream.wait()?;
        if !status.success() {
            tee.abort();
            bail!(
                "remote archive command failed: {}",
                self.stream.failure_detail()?
            );
        }
        let checksum = tee.checksum();
        let size = tee.size();
        if checksum != trailer.checksum || size != trailer.size {
            tee.abort();
            bail!(
                "remote archive stream does not match its trailer: got {size} bytes {checksum}, expected {} bytes {}",
                trailer.size,
                trailer.checksum
            );
        }
        warn_changed(&self.job, &trailer.changed);
        let sinks = tee.complete(&checksum);
        Ok(StreamOutcome {
            checksum,
            size,
            changed: trailer.changed,
            sinks,
        })
    }
}

pub fn deliver_remote(artifact: &Artifact, remote: &SshLocation, job: &BackupJob) -> Result<()> {
    let mut sink = SshSink::open(remote, &artifact.name, job)?;
    let mut file = File::open(&artifact.path)
        .with_context(|| format!("open staged archive {}", artifact.path.display()))?;
    let mut buffer = vec![0; SEND_CHUNK];
    let mut sent = 0u64;
    loop {
        let read = file.read(&mut buffer)?;
        if read == 0 {
            break;
        }
        if let Err(error) = sink.write_all(&buffer[..read]) {
            let detail = sink.abort().unwrap_or_else(|| error.to_string());
            bail!(
                "send archive to {remote}: {detail}",
                remote = remote.target()
            );
        }
        sent += read as u64;
    }
    if sent != artifact.size {
        sink.abort();
        bail!("local staged archive changed while sending");
    }
    sink.finish(&artifact.checksum, artifact.size)
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
            created: archive.created,
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
    let mut stream = SshStream::spawn(remote, &request)?;
    let wire: WireArtifact = match read_response(&mut stream.reader()) {
        Ok(wire) => wire,
        Err(error) => {
            stream.terminate()?;
            bail!(
                "read remote download response: {error:#}; {}",
                stream.failure_detail()?
            );
        }
    };
    let receive_result = (|| {
        let mut file = create_private(destination)?;
        let copied = copy(&mut stream.reader().take(wire.size), &mut file)?;
        file.sync_all()?;
        if copied != wire.size {
            bail!("downloaded {copied} bytes, expected {}", wire.size);
        }
        verify_checksum(destination, &wire.checksum)
    })();
    if let Err(error) = receive_result {
        stream.terminate()?;
        remove_if_present(destination)?;
        bail!(
            "download remote archive: {error:#}; {}",
            stream.failure_detail()?
        );
    }
    let status = stream.wait()?;
    if !status.success() {
        remove_if_present(destination)?;
        bail!(
            "remote archive download failed: {}",
            stream.failure_detail()?
        );
    }
    Ok(())
}

pub fn restore_remote(artifact: &Artifact, remote: &SshLocation) -> Result<()> {
    let request = AgentRequest::Restore {
        artifact: WireArtifact::from_artifact(artifact),
        target: remote.path.clone(),
    };
    let mut session = Session::start(remote, &request)?;
    let sent = (|| {
        let mut file = File::open(&artifact.path)?;
        let copied = copy(&mut file, session.stdin()?)?;
        if copied != artifact.size {
            bail!("local staged archive changed while sending");
        }
        Ok(())
    })();
    if let Err(error) = sent {
        return Err(session.abort().map_or(error, |detail| {
            anyhow::anyhow!("remote restore failed: {detail}")
        }));
    }
    session.finish()?;
    Ok(())
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

fn remove_if_present(path: &Path) -> Result<()> {
    match fs::remove_file(path) {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error).with_context(|| format!("remove {}", path.display())),
    }
}
