mod deploy;
mod session;
mod sink;
mod stall;

use std::collections::{HashSet, VecDeque};
use std::io::Read;

use anyhow::{Context, Error, Result, anyhow, bail};
use serde_json::Value;
use tracing::warn;

use crate::archive::{ChunkEvent, Produced, warn_changed};
use crate::config::BackupJob;
use crate::location::SshLocation;
use crate::protocol::{
    AgentRequest, ChunkCount, FrameReader, PROTOCOL_VERSION, PingResponse, Record, StreamHeader,
    StreamTrailer, read_ids, read_record, write_end_frame, write_frame, write_ids,
};
use crate::store::check::StoreCheck;
use crate::store::digest::Digest;
use crate::store::import::ImportReport;
use crate::store::recipe::{Recipe, RecipeChunk, RecipeInfo};
use crate::stream::ChunkSource;
use crate::transfer::Fanout;

use session::{Session, SshStream, read_response, simple_request};
pub use sink::{SshRepair, SshSink};

const RESTORE_FRAME: usize = 1024 * 1024;

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

pub fn chunk_ids(remote: &SshLocation) -> Result<HashSet<Digest>> {
    let request = AgentRequest::Chunks {
        destination: remote.path.clone(),
    };
    let mut stream = SshStream::spawn(remote, &request, &[])?;
    let received = (|| {
        let count: ChunkCount = read_response(&mut stream.reader())?;
        let ids = read_ids(&mut stream.reader())?;
        if ids.len() != count.count {
            bail!("received {} chunk ids, expected {}", ids.len(), count.count);
        }
        Ok(ids)
    })();
    finish_stream(&mut stream, received, "list remote chunks").map(|ids| ids.into_iter().collect())
}

/// A backup made on the remote source by its agent. Only chunks some
/// destination lacks cross the connection.
pub struct RemoteStream {
    job: String,
    stream: SshStream,
    pub header: StreamHeader,
}

impl RemoteStream {
    pub fn start(job: &BackupJob, remote: &SshLocation, known: &HashSet<Digest>) -> Result<Self> {
        let request = AgentRequest::Create {
            job: job.name.clone(),
            source: remote.path.clone(),
            exclude: job.exclude.clone(),
            pre: job.pre.clone(),
        };
        let mut payload = Vec::new();
        write_ids(&mut payload, known)?;
        let mut stream = SshStream::spawn(remote, &request, &payload)?;
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

    pub fn pump(mut self, fanout: &mut Fanout) -> Result<Produced> {
        let received = (|| {
            let mut chunks = Vec::new();
            let mut reader = self.stream.reader();
            let mut frames = FrameReader::new(&mut reader);
            while let Some(record) = read_record(&mut frames)? {
                match record {
                    Record::Data { id, bytes } => {
                        chunks.push(RecipeChunk {
                            id,
                            size: u32::try_from(bytes.len())?,
                        });
                        fanout.accept(ChunkEvent::Data { id, bytes: &bytes })?;
                    }
                    Record::Reference { id, size } => {
                        chunks.push(RecipeChunk { id, size });
                        fanout.accept(ChunkEvent::Reference { id, size })?;
                    }
                    Record::Missing { id } => bail!("the source sent a missing record for {id}"),
                }
            }
            let trailer: StreamTrailer = read_response(&mut self.stream.reader())?;
            Ok((chunks, trailer))
        })();
        let (chunks, trailer) =
            finish_stream(&mut self.stream, received, "receive remote archive")?;
        let size: u64 = chunks.iter().map(|chunk| u64::from(chunk.size)).sum();
        if size != trailer.size {
            bail!(
                "remote archive stream has {size} bytes, its trailer says {}",
                trailer.size
            );
        }
        warn_changed(&self.job, &trailer.changed);
        Ok(Produced {
            chunks,
            size,
            checksum: trailer.checksum,
            changed: trailer.changed,
        })
    }
}

/// Reads chunks from a remote store. After `prefetch`, chunks asked for in
/// that order stream over one connection. Any other chunk is fetched alone.
pub struct RemoteSource {
    remote: SshLocation,
    sequence: Option<(SshStream, VecDeque<Digest>)>,
}

impl RemoteSource {
    pub fn new(remote: &SshLocation) -> Self {
        Self {
            remote: remote.clone(),
            sequence: None,
        }
    }

    pub fn prefetch(&mut self, ids: &[Digest]) -> Result<()> {
        self.close()?;
        if ids.is_empty() {
            return Ok(());
        }
        let stream = start_read(&self.remote, ids)?;
        self.sequence = Some((stream, ids.iter().copied().collect()));
        Ok(())
    }

    fn close(&mut self) -> Result<()> {
        if let Some((mut stream, _)) = self.sequence.take() {
            stream.terminate()?;
        }
        Ok(())
    }

    fn next_in_sequence(&mut self, id: &Digest) -> Option<Result<Vec<u8>>> {
        let (stream, pending) = self.sequence.as_mut()?;
        if pending.front() != Some(id) {
            return None;
        }
        pending.pop_front();
        let read = read_one(stream, id);
        let ended = pending.is_empty();
        if read.is_err() || ended {
            let Some((mut stream, _)) = self.sequence.take() else {
                return Some(read);
            };
            let closed = if ended && read.is_ok() {
                FrameReader::new(&mut stream.reader())
                    .finish()
                    .map_err(Error::from)
                    .and_then(|()| stream.wait().map(drop))
            } else {
                stream.terminate()
            };
            if let Err(error) = closed {
                return Some(Err(error));
            }
        }
        Some(read)
    }
}

impl ChunkSource for RemoteSource {
    fn describe(&self) -> String {
        format!(
            "ssh://{}{}",
            self.remote.target(),
            self.remote.path.display()
        )
    }

    fn read_chunk(&mut self, id: &Digest) -> Result<Vec<u8>> {
        if let Some(read) = self.next_in_sequence(id) {
            return read;
        }
        let mut stream = start_read(&self.remote, &[*id])?;
        let read = read_one(&mut stream, id);
        let read = read.and_then(|bytes| {
            FrameReader::new(&mut stream.reader()).finish()?;
            Ok(bytes)
        });
        finish_stream(&mut stream, read, "read a remote chunk")
    }
}

impl Drop for RemoteSource {
    fn drop(&mut self) {
        if let Err(error) = self.close() {
            warn!(error = %format!("{error:#}"), "could not close a remote chunk stream");
        }
    }
}

fn start_read(remote: &SshLocation, ids: &[Digest]) -> Result<SshStream> {
    let request = AgentRequest::ReadChunks {
        destination: remote.path.clone(),
    };
    let mut payload = Vec::new();
    write_ids(&mut payload, ids)?;
    let mut stream = SshStream::spawn(remote, &request, &payload)?;
    // The agent answers before the records, so an error such as a missing
    // store arrives as a message and not as a broken record stream.
    let opened = read_response::<Value>(&mut stream.reader()).map(drop);
    finish_stream_on_error(&mut stream, opened, "read remote chunks")?;
    Ok(stream)
}

fn finish_stream_on_error(stream: &mut SshStream, result: Result<()>, what: &str) -> Result<()> {
    if let Err(error) = result {
        stream.terminate()?;
        bail!("{what}: {error:#}; {}", stream.failure_detail()?);
    }
    Ok(())
}

// Each record is framed on its own, so a fresh frame reader per record starts
// on a frame boundary.
fn read_one(stream: &mut SshStream, id: &Digest) -> Result<Vec<u8>> {
    let mut reader = stream.reader();
    let mut frames = FrameReader::new(&mut reader);
    match read_record(&mut frames)? {
        Some(Record::Data { id: got, bytes }) if got == *id => Ok(bytes),
        Some(Record::Missing { id: got }) if got == *id => {
            bail!("the remote store cannot read chunk {id}")
        }
        Some(_) => bail!("the remote store sent a different chunk than {id}"),
        None => bail!("the remote store ended before chunk {id}"),
    }
}

fn finish_stream<T>(stream: &mut SshStream, received: Result<T>, what: &str) -> Result<T> {
    match received {
        Ok(value) => {
            let status = stream.wait()?;
            if !status.success() {
                bail!("{what} failed: {}", stream.failure_detail()?);
            }
            Ok(value)
        }
        Err(error) => {
            stream.terminate()?;
            bail!("{what}: {error:#}; {}", stream.failure_detail()?)
        }
    }
}

pub fn list(remote: &SshLocation, job: Option<&str>) -> Result<Vec<RecipeInfo>> {
    simple_request(
        remote,
        &AgentRequest::List {
            destination: remote.path.clone(),
            job: job.map(str::to_owned),
        },
    )
}

pub fn read_recipe(remote: &SshLocation, name: &str) -> Result<Recipe> {
    simple_request(
        remote,
        &AgentRequest::ReadRecipe {
            destination: remote.path.clone(),
            name: name.to_owned(),
        },
    )
}

pub fn restore(reader: &mut dyn Read, target: &SshLocation) -> Result<()> {
    let request = AgentRequest::Restore {
        target: target.path.clone(),
    };
    let mut session = Session::start_watched(target, &request)?;
    let sent = (|| {
        let mut buffer = vec![0; RESTORE_FRAME];
        loop {
            let read = reader.read(&mut buffer)?;
            if read == 0 {
                break;
            }
            write_frame(session.stdin()?, &buffer[..read])?;
            session.bump();
        }
        write_end_frame(session.stdin()?)?;
        Ok(())
    })();
    if let Err(error) = sent {
        return Err(session
            .abort()
            .map_or(error, |detail| anyhow!("remote restore failed: {detail}")));
    }
    session.finish().context("remote restore failed")?;
    Ok(())
}

pub fn check(remote: &SshLocation) -> Result<StoreCheck> {
    simple_request(
        remote,
        &AgentRequest::Check {
            destination: remote.path.clone(),
        },
    )
}

pub fn verify_archive(remote: &SshLocation, name: &str) -> Result<()> {
    let response: Value = simple_request(
        remote,
        &AgentRequest::VerifyArchive {
            destination: remote.path.clone(),
            name: name.to_owned(),
        },
    )?;
    drop(response);
    Ok(())
}

pub fn prune(remote: &SshLocation, job: &BackupJob) -> Result<()> {
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

pub fn legacy(remote: &SshLocation) -> Result<Vec<String>> {
    simple_request(
        remote,
        &AgentRequest::Legacy {
            destination: remote.path.clone(),
        },
    )
}

pub fn import(remote: &SshLocation, name: &str) -> Result<ImportReport> {
    simple_request(
        remote,
        &AgentRequest::Import {
            destination: remote.path.clone(),
            name: name.to_owned(),
        },
    )
}
