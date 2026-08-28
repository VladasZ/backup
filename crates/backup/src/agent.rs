use std::fs::{self, File, OpenOptions};
use std::io::{self, BufRead, BufReader, ErrorKind, Read, Write, copy, stdin, stdout};
use std::path::{Path, PathBuf};

use anyhow::{Context, Result, bail};
use chrono::Utc;
use serde::Serialize;
use serde_json::{Value, from_str, to_value, to_writer};
use tracing::{error, info, warn};
use uuid::Uuid;

use crate::archive::{
    Artifact, HashingWriter, Sink, SinkId, SourceScanner, Tee, archive_name, create_private,
    ensure_not_symlink, pump_local, restore_archive, verify_archive, verify_checksum,
    write_checksum,
};
use crate::config::BackupJob;
use crate::destination::{apply_retention, belongs_to_job, list_local, sweep_stale_partials};
use crate::location::Location;
use crate::paths::AppPaths;
use crate::protocol::{
    AgentRequest, PROTOCOL_VERSION, PingResponse, RESPONSE_PREFIX, ResponseEnvelope, StreamHeader,
    StreamTrailer, WireArchiveInfo, WireArtifact, copy_frames, write_end_frame, write_frame,
};
use crate::storage::warn_if_high;

pub fn run(paths: &AppPaths) -> Result<()> {
    paths.ensure()?;
    sweep_stale_partials(&paths.staging);
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
        } => create_and_stream(job, source, exclude),
        AgentRequest::Receive {
            name,
            destination,
            job,
        } => receive_stream(reader, &name, &destination, &job),
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

struct FrameSink;

impl Sink for FrameSink {
    fn id(&self) -> SinkId {
        SinkId::Stream
    }

    fn write_all(&mut self, bytes: &[u8]) -> io::Result<()> {
        write_frame(&mut stdout().lock(), bytes)
    }

    fn finish(self: Box<Self>, _checksum: &str, _size: u64) -> Result<()> {
        let mut output = stdout().lock();
        write_end_frame(&mut output)?;
        output.flush()?;
        Ok(())
    }

    fn abort(self: Box<Self>) -> Option<String> {
        let mut output = stdout().lock();
        if let Err(error) = write_end_frame(&mut output).and_then(|()| output.flush()) {
            warn!(%error, "could not close the archive stream");
        }
        None
    }
}

fn create_and_stream(name: String, source: PathBuf, exclude: Vec<String>) -> Result<()> {
    warn_if_high(&source, "source")?;
    let scanner = SourceScanner::new(&source, &exclude)?;
    let created_at = Utc::now();
    let archive = archive_name(&name, created_at);
    let job = BackupJob {
        name,
        source: Location::Local(source),
        destinations: vec![Location::Local(PathBuf::from("/"))],
        cron: "0 0 * * *".to_owned(),
        retention: None,
        exclude,
    };
    write_success(&StreamHeader {
        name: archive,
        created_at,
    })?;
    let outcome = pump_local(&job, &scanner, Tee::new(vec![Box::new(FrameSink)]))?;
    if let Some(failed) = outcome.sinks.iter().find(|sink| sink.error.is_some()) {
        bail!("{failed}");
    }
    write_success(&StreamTrailer {
        checksum: outcome.checksum,
        size: outcome.size,
        changed: outcome.changed,
    })
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
    let mut file = File::open(&artifact.path)?;
    let wire = WireArtifact::from_artifact(&artifact);
    write_success(&wire)?;
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

fn receive_stream(
    reader: &mut dyn BufRead,
    name: &str,
    destination: &Path,
    job: &BackupJob,
) -> Result<()> {
    if !belongs_to_job(name, &job.name) {
        bail!(
            "archive name {name:?} does not belong to job {:?}",
            job.name
        );
    }
    fs::create_dir_all(destination)
        .with_context(|| format!("create destination {}", destination.display()))?;
    warn_if_high(destination, "destination")?;
    sweep_stale_partials(destination);
    let partial = destination.join(format!(".{name}-{}.partial", Uuid::new_v4()));
    let received = (|| {
        let file = create_private(&partial)?;
        let mut writer = HashingWriter::new(file);
        let size = copy_frames(reader, &mut writer)?;
        writer.inner.sync_all()?;
        let mut line = String::new();
        reader.read_line(&mut line)?;
        let trailer: StreamTrailer = from_str(&line).context("parse stream trailer")?;
        let checksum = writer.checksum();
        if trailer.checksum != checksum || trailer.size != size {
            bail!(
                "received {size} bytes {checksum}, expected {} bytes {}",
                trailer.size,
                trailer.checksum
            );
        }
        Ok(checksum)
    })();
    let checksum = match received {
        Ok(checksum) => checksum,
        Err(error) => {
            remove_if_present(&partial)?;
            return Err(error);
        }
    };
    let target = destination.join(name);
    if target.exists() && verify_checksum(&target, &checksum).is_ok() {
        info!(path = %target.display(), "destination already holds this archive");
        remove_if_present(&partial)?;
    } else {
        fs::rename(&partial, &target)?;
    }
    write_checksum(&target, &checksum)?;
    File::open(destination)?.sync_all()?;
    apply_retention(destination, job)?;
    write_success(&Value::Null)
}

fn receive_to_path(reader: &mut dyn BufRead, artifact: &WireArtifact, path: &Path) -> Result<()> {
    remove_if_present(path)?;
    let receive_result = (|| {
        let mut file = create_private(path)?;
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
        protocol: PROTOCOL_VERSION,
    })
}

fn write_error(error: &str) -> Result<()> {
    write_response(&ResponseEnvelope {
        ok: false,
        error: Some(error.to_owned()),
        data: Value::Null,
        protocol: PROTOCOL_VERSION,
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
