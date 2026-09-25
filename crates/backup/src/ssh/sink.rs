use std::collections::HashSet;
use std::io::Write;

use anyhow::{Error, Result, anyhow};
use serde_json::to_writer;

use super::chunk_ids;
use super::session::Session;
use crate::config::BackupJob;
use crate::location::{Location, SshLocation};
use crate::protocol::{AgentRequest, encode_data, write_end_frame, write_frame};
use crate::store::digest::Digest;
use crate::store::recipe::Recipe;
use crate::transfer::ChunkSink;

pub struct SshSink {
    location: Location,
    session: Session,
    known: HashSet<Digest>,
}

impl SshSink {
    pub fn open(remote: &SshLocation, job: &BackupJob) -> Result<Box<dyn ChunkSink>> {
        let known = chunk_ids(remote)?;
        let request = AgentRequest::Receive {
            destination: remote.path.clone(),
            job: job.clone(),
        };
        let session = Session::start_watched(remote, &request)?;
        Ok(Box::new(Self {
            location: Location::Ssh(remote.clone()),
            session,
            known,
        }))
    }
}

impl ChunkSink for SshSink {
    fn location(&self) -> &Location {
        &self.location
    }

    fn known(&self) -> &HashSet<Digest> {
        &self.known
    }

    fn put(&mut self, id: &Digest, bytes: &[u8]) -> Result<()> {
        let record = encode_data(id, bytes)?;
        write_frame(self.session.stdin()?, &record)?;
        self.session.bump();
        self.known.insert(*id);
        Ok(())
    }

    fn finish(mut self: Box<Self>, recipe: &Recipe) -> Result<()> {
        let sent = (|| {
            let stdin = self.session.stdin()?;
            write_end_frame(stdin)?;
            to_writer(&mut *stdin, recipe)?;
            writeln!(stdin)?;
            stdin.flush()?;
            Ok(())
        })();
        if let Err(error) = sent {
            return Err(self.session.abort().map_or(error, |detail| anyhow!(detail)));
        }
        self.session.finish()?;
        Ok(())
    }

    fn abort(self: Box<Self>) -> Option<String> {
        self.session.abort()
    }
}

/// Sends good copies of bad chunks to a remote store, which then retires its
/// damaged packs.
pub struct SshRepair {
    session: Session,
}

impl SshRepair {
    pub fn open(remote: &SshLocation, damaged: &[Digest], recipes: &[Recipe]) -> Result<Self> {
        let request = AgentRequest::Repair {
            destination: remote.path.clone(),
            damaged: damaged.to_vec(),
            recipes: recipes.to_vec(),
        };
        Ok(Self {
            session: Session::start_watched(remote, &request)?,
        })
    }

    pub fn put(&mut self, id: &Digest, bytes: &[u8]) -> Result<()> {
        let record = encode_data(id, bytes)?;
        write_frame(self.session.stdin()?, &record)?;
        self.session.bump();
        Ok(())
    }

    pub fn finish(mut self) -> Result<()> {
        let sent = self
            .session
            .stdin()
            .and_then(|stdin| write_end_frame(stdin).map_err(Error::from));
        if let Err(error) = sent {
            return Err(self.session.abort().map_or(error, |detail| anyhow!(detail)));
        }
        self.session.finish()?;
        Ok(())
    }
}
