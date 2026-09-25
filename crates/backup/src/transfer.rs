use std::collections::HashSet;
use std::fmt::{Display, Formatter, Result as FmtResult};
use std::path::Path;

use anyhow::{Result, anyhow, bail};
use tracing::{info, warn};

use crate::archive::ChunkEvent;
use crate::config::BackupJob;
use crate::location::Location;
use crate::output::{Event, emit};
use crate::ssh::SshSink;
use crate::store::digest::Digest;
use crate::store::gc::apply_retention;
use crate::store::recipe::Recipe;
use crate::store::{Store, StoreWriter};

const PROGRESS_STEP: u64 = 64 * 1024 * 1024;

/// One destination receiving a backup.
pub trait ChunkSink {
    fn location(&self) -> &Location;
    fn known(&self) -> &HashSet<Digest>;
    fn put(&mut self, id: &Digest, bytes: &[u8]) -> Result<()>;
    fn finish(self: Box<Self>, recipe: &Recipe) -> Result<()>;
    fn abort(self: Box<Self>) -> Option<String>;
}

pub fn open_sink(destination: &Location, job: &BackupJob) -> Result<Box<dyn ChunkSink>> {
    match destination {
        Location::Local(path) => LocalSink::open(path, job),
        Location::Ssh(remote) => SshSink::open(remote, job),
    }
}

#[derive(Debug)]
pub struct SinkOutcome {
    pub destination: Location,
    pub error: Option<String>,
}

impl Display for SinkOutcome {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> FmtResult {
        match &self.error {
            Some(error) => write!(formatter, "{}: {error}", self.destination),
            None => write!(formatter, "{}: ok", self.destination),
        }
    }
}

/// Sends every chunk to every destination that lacks it. A destination that
/// fails is dropped and the others go on, and only when all have failed does
/// the backup fail.
pub struct Fanout {
    live: Vec<Box<dyn ChunkSink>>,
    failed: Vec<SinkOutcome>,
    streamed: u64,
    reported: u64,
}

impl Fanout {
    pub fn new(sinks: Vec<Box<dyn ChunkSink>>) -> Self {
        Self {
            live: sinks,
            failed: Vec::new(),
            streamed: 0,
            reported: 0,
        }
    }

    /// Chunks every destination already holds. The source sends only a
    /// reference for these.
    pub fn known(&self) -> HashSet<Digest> {
        let mut sinks = self.live.iter();
        let Some(first) = sinks.next() else {
            return HashSet::new();
        };
        let mut known = first.known().clone();
        for sink in sinks {
            known.retain(|id| sink.known().contains(id));
        }
        known
    }

    pub fn accept(&mut self, event: ChunkEvent<'_>) -> Result<()> {
        self.streamed += event.size();
        if self.streamed - self.reported >= PROGRESS_STEP {
            self.reported = self.streamed;
            emit(&Event::Progress {
                bytes: self.streamed,
            });
        }
        let id = event.id();
        let mut index = 0;
        while index < self.live.len() {
            let sink = &mut self.live[index];
            let result = if sink.known().contains(&id) {
                Ok(())
            } else {
                match &event {
                    ChunkEvent::Data { bytes, .. } => sink.put(&id, bytes),
                    ChunkEvent::Reference { .. } => {
                        Err(anyhow!("the destination does not hold chunk {id}"))
                    }
                }
            };
            match result {
                Ok(()) => index += 1,
                Err(error) => {
                    let sink = self.live.remove(index);
                    let destination = sink.location().clone();
                    let detail = sink.abort().unwrap_or_else(|| format!("{error:#}"));
                    warn!(%destination, error = %detail, "destination failed; continuing with the others");
                    self.failed.push(SinkOutcome {
                        destination,
                        error: Some(detail),
                    });
                }
            }
        }
        if self.live.is_empty() {
            bail!("every destination failed: {}", describe(&self.failed));
        }
        Ok(())
    }

    pub fn complete(self, recipe: &Recipe) -> Vec<SinkOutcome> {
        let mut outcomes = self.failed;
        for sink in self.live {
            let destination = sink.location().clone();
            let error = sink.finish(recipe).err().map(|error| format!("{error:#}"));
            outcomes.push(SinkOutcome { destination, error });
        }
        outcomes
    }

    pub fn abort(self) -> Vec<SinkOutcome> {
        for sink in self.live {
            if let Some(detail) = sink.abort() {
                warn!(error = %detail, "destination reported an error while aborting");
            }
        }
        self.failed
    }
}

pub fn describe(outcomes: &[SinkOutcome]) -> String {
    outcomes
        .iter()
        .map(ToString::to_string)
        .collect::<Vec<_>>()
        .join("; ")
}

pub struct LocalSink {
    location: Location,
    writer: StoreWriter,
    known: HashSet<Digest>,
    job: BackupJob,
}

impl LocalSink {
    pub fn open(path: &Path, job: &BackupJob) -> Result<Box<dyn ChunkSink>> {
        let mut store = Store::open(path)?;
        let known = store.chunk_ids()?;
        Ok(Box::new(Self {
            location: Location::Local(path.to_path_buf()),
            writer: store.into_writer(),
            known,
            job: job.clone(),
        }))
    }
}

impl ChunkSink for LocalSink {
    fn location(&self) -> &Location {
        &self.location
    }

    fn known(&self) -> &HashSet<Digest> {
        &self.known
    }

    fn put(&mut self, id: &Digest, bytes: &[u8]) -> Result<()> {
        self.writer.put(id, bytes)?;
        self.known.insert(*id);
        Ok(())
    }

    fn finish(self: Box<Self>, recipe: &Recipe) -> Result<()> {
        let mut store = self.writer.finish()?;
        store.ensure_complete(recipe)?;
        store.write_recipe(recipe)?;
        info!(
            job = self.job.name,
            destination = %self.location,
            archive = recipe.name,
            "delivered archive"
        );
        if let Err(error) = apply_retention(store, &self.job) {
            warn!(
                job = self.job.name,
                destination = %self.location,
                error = %format!("{error:#}"),
                "cleanup after delivery failed; the archive was still delivered"
            );
        }
        Ok(())
    }

    // Packs already written stay without an index file. The next load indexes
    // them and cleanup deletes them once no recipe uses them.
    fn abort(self: Box<Self>) -> Option<String> {
        None
    }
}
