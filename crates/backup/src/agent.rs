use std::fs::{self, File, OpenOptions};
use std::io::{BufRead, BufReader, ErrorKind, Read, Write, copy, sink, stdin, stdout};
use std::path::{Path, PathBuf};

use anyhow::{Context, Result, bail};
use serde::Serialize;
use serde_json::{Value, from_str, to_value, to_writer};
use tracing::{error, warn};
use uuid::Uuid;

use crate::archive::{
    Artifact, create_local_archive, ensure_not_symlink, restore_archive, verify_archive,
    verify_checksum, write_checksum,
};
use crate::config::BackupJob;
use crate::destination::{apply_retention, list_local};
use crate::location::Location;
use crate::paths::AppPaths;
use crate::protocol::{
    AgentRequest, PROTOCOL_VERSION, PingResponse, RESPONSE_PREFIX, ResponseEnvelope,
    WireArchiveInfo, WireArtifact,
};
use crate::storage::{require_staging_reserve, warn_if_high};

pub fn run(paths: &AppPaths) -> Result<()> {
    paths.ensure()?;
    let input = stdin();
    let mut reader = BufReader::new(input.lock());
    let mut request_line = String::new();
    reader
        .read_line(&mut request_line)
        .context("read agent request")?;
    let request: AgentRequest = from_str(&request_line).context("parse agent request")?;
    if let Err(error) = handle(request, &mut reader, paths) {
        error!(%error, "agent operation failed");
        write_error(&format!("{error:#}"))?;
    }
    Ok(())
}

fn handle(request: AgentRequest, reader: &mut dyn BufRead, paths: &AppPaths) -> Result<()> {
    match request {
        AgentRequest::Ping => write_success(&PingResponse {
            protocol: PROTOCOL_VERSION,
            version: env!("CARGO_PKG_VERSION").to_owned(),
        }),
        AgentRequest::ValidateSource { path } => {
            ensure_not_symlink(&path)?;
            let metadata =
                fs::metadata(&path).with_context(|| format!("read source {}", path.display()))?;
            if !metadata.is_dir() && !metadata.is_file() {
                bail!(
                    "source {} is not a regular file or directory",
                    path.display()
                );
            }
            warn_if_high(&path, "source")?;
            write_success(&Value::Null)
        }
        AgentRequest::ValidateDestination { path } => {
            validate_destination_path(&path)?;
            write_success(&Value::Null)
        }
        AgentRequest::Create {
            job,
            source,
            exclude,
        } => create_and_stream(job, source, exclude, paths),
        AgentRequest::Receive {
            artifact,
            destination,
            job,
        } => receive_archive(reader, &artifact, &destination, &job),
        AgentRequest::List { destination, job } => {
            let archives = list_local(&destination, &job)?
                .into_iter()
                .map(|archive| WireArchiveInfo {
                    name: archive.name,
                    checksum: archive.checksum,
                    size: archive.size,
                    created: archive.created,
                })
                .collect::<Vec<_>>();
            write_success(&archives)
        }
        AgentRequest::Send { archive, checksum } => stream_existing(&archive, &checksum),
        AgentRequest::Restore { artifact, target } => {
            let temporary =
                paths
                    .staging
                    .join(format!(".restore-{}-{}", Uuid::new_v4(), artifact.name));
            receive_to_path(reader, &artifact, &temporary)?;
            let restore_result = restore_archive(&temporary, &target);
            let cleanup_result = remove_if_present(&temporary);
            restore_result?;
            cleanup_result?;
            write_success(&Value::Null)
        }
        AgentRequest::Verify { archive, checksum } => {
            verify_checksum(&archive, &checksum)?;
            verify_archive(&archive)?;
            write_success(&Value::Null)
        }
        AgentRequest::Prune { destination, job } => {
            apply_retention(&destination, &job)?;
            write_success(&Value::Null)
        }
    }
}

fn validate_destination_path(path: &Path) -> Result<()> {
    fs::create_dir_all(path).with_context(|| format!("create destination {}", path.display()))?;
    if !fs::metadata(path)?.is_dir() {
        bail!("destination {} is not a directory", path.display());
    }
    warn_if_high(path, "destination")?;
    let probe = path.join(format!(".backup-write-test-{}", Uuid::new_v4()));
    let file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&probe)
        .with_context(|| format!("destination {} is not writable", path.display()))?;
    file.sync_all()?;
    fs::remove_file(&probe).with_context(|| format!("remove write probe {}", probe.display()))
}

fn create_and_stream(
    name: String,
    source: PathBuf,
    exclude: Vec<String>,
    paths: &AppPaths,
) -> Result<()> {
    require_staging_reserve(&paths.staging)?;
    warn_if_high(&source, "source")?;
    let job = BackupJob {
        name,
        source: Location::Local(source.clone()),
        destinations: vec![Location::Local(paths.staging.clone())],
        cron: "0 0 * * *".to_owned(),
        retention: None,
        exclude,
    };
    let artifact = create_local_archive(&job, &source, &paths.job_staging(&job.name))?;
    // Open the archive, then unlink both files at once. On Unix the open handle keeps the data
    // readable while we stream it, and the disk space is freed the moment this process ends, even
    // if ssh is killed mid-stream. So an interrupted transfer never leaks into remote staging.
    let file =
        File::open(&artifact.path).with_context(|| format!("open {}", artifact.path.display()))?;
    remove_if_present(&artifact.path)?;
    remove_if_present(&artifact.checksum_path)?;
    stream_open_archive(file, &WireArtifact::from_artifact(&artifact))
}

fn stream_existing(archive: &Path, checksum: &str) -> Result<()> {
    verify_checksum(archive, checksum)?;
    let metadata = fs::metadata(archive)?;
    let name = archive
        .file_name()
        .context("archive has no file name")?
        .to_string_lossy()
        .into_owned();
    let artifact = Artifact {
        name,
        path: archive.to_path_buf(),
        checksum_path: PathBuf::new(),
        checksum: checksum.to_owned(),
        size: metadata.len(),
        created_at: metadata.modified()?.into(),
    };
    stream_artifact(&artifact)
}

fn stream_artifact(artifact: &Artifact) -> Result<()> {
    let file = File::open(&artifact.path)?;
    stream_open_archive(file, &WireArtifact::from_artifact(artifact))
}

fn stream_open_archive(mut file: File, wire: &WireArtifact) -> Result<()> {
    write_success(wire)?;
    let mut output = stdout().lock();
    let copied = copy(&mut file, &mut output)?;
    if copied != wire.size {
        bail!(
            "archive changed while streaming: expected {} bytes, sent {copied}",
            wire.size
        );
    }
    output.flush()?;
    Ok(())
}

fn receive_archive(
    reader: &mut dyn BufRead,
    artifact: &WireArtifact,
    destination: &Path,
    job: &BackupJob,
) -> Result<()> {
    fs::create_dir_all(destination)
        .with_context(|| format!("create destination {}", destination.display()))?;
    warn_if_high(destination, "destination")?;
    let target = destination.join(&artifact.name);
    if target.exists() {
        match verify_checksum(&target, &artifact.checksum) {
            Ok(()) => {
                let copied = copy(&mut reader.take(artifact.size), &mut sink())?;
                if copied != artifact.size {
                    bail!("received {copied} bytes, expected {}", artifact.size);
                }
            }
            Err(error) => {
                warn!(
                    path = %target.display(),
                    %error,
                    "replacing corrupt destination archive"
                );
                let partial =
                    destination.join(format!(".{}-{}.partial", artifact.name, Uuid::new_v4()));
                receive_to_path(reader, artifact, &partial)?;
                fs::rename(&partial, &target)?;
            }
        }
    } else {
        let partial = destination.join(format!(".{}-{}.partial", artifact.name, Uuid::new_v4()));
        receive_to_path(reader, artifact, &partial)?;
        fs::rename(&partial, &target)?;
    }
    write_checksum(&target, &artifact.checksum)?;
    File::open(destination)?.sync_all()?;
    apply_retention(destination, job)?;
    write_success(&Value::Null)
}

fn receive_to_path(reader: &mut dyn BufRead, artifact: &WireArtifact, path: &Path) -> Result<()> {
    remove_if_present(path)?;
    let receive_result = (|| {
        let mut file = File::create(path).with_context(|| format!("create {}", path.display()))?;
        let copied = copy(&mut reader.take(artifact.size), &mut file)?;
        file.flush()?;
        file.sync_all()?;
        if copied != artifact.size {
            bail!("received {copied} bytes, expected {}", artifact.size);
        }
        verify_checksum(path, &artifact.checksum)
    })();
    if receive_result.is_err() {
        remove_if_present(path)?;
    }
    receive_result
}

fn write_success<T: Serialize>(data: &T) -> Result<()> {
    write_response(&ResponseEnvelope {
        ok: true,
        error: None,
        data: to_value(data)?,
    })
}

fn write_error(error: &str) -> Result<()> {
    write_response(&ResponseEnvelope {
        ok: false,
        error: Some(error.to_owned()),
        data: Value::Null,
    })
}

fn write_response(response: &ResponseEnvelope) -> Result<()> {
    let mut output = stdout().lock();
    write!(output, "{RESPONSE_PREFIX}")?;
    to_writer(&mut output, response)?;
    writeln!(output)?;
    output.flush()?;
    Ok(())
}

fn remove_if_present(path: &Path) -> Result<()> {
    match fs::remove_file(path) {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error).with_context(|| format!("remove {}", path.display())),
    }
}
