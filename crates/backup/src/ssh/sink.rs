use std::io::{self, Write};

use anyhow::Result;
use serde_json::to_writer;

use super::session::Session;
use crate::archive::{Sink, SinkId};
use crate::config::BackupJob;
use crate::location::{Location, SshLocation};
use crate::protocol::{AgentRequest, StreamTrailer, write_end_frame, write_frame};

pub struct SshSink {
    remote: SshLocation,
    session: Session,
}

impl SshSink {
    pub fn open(remote: &SshLocation, name: &str, job: &BackupJob) -> Result<Box<dyn Sink>> {
        let request = AgentRequest::Receive {
            name: name.to_owned(),
            destination: remote.path.clone(),
            job: job.clone(),
        };
        let session = Session::start(remote, &request)?;
        Ok(Box::new(Self {
            remote: remote.clone(),
            session,
        }))
    }
}

impl Sink for SshSink {
    fn id(&self) -> SinkId {
        SinkId::Destination(Location::Ssh(self.remote.clone()))
    }

    fn write_all(&mut self, bytes: &[u8]) -> io::Result<()> {
        let stdin = self.session.stdin().map_err(io::Error::other)?;
        write_frame(stdin, bytes)
    }

    fn finish(mut self: Box<Self>, checksum: &str, size: u64) -> Result<()> {
        let trailer = StreamTrailer {
            checksum: checksum.to_owned(),
            size,
            changed: Vec::new(),
        };
        let sent = (|| {
            let stdin = self.session.stdin()?;
            write_end_frame(stdin)?;
            to_writer(&mut *stdin, &trailer)?;
            writeln!(stdin)?;
            stdin.flush()?;
            Ok(())
        })();
        if let Err(error) = sent {
            return Err(self
                .session
                .abort()
                .map_or(error, |detail| anyhow::anyhow!(detail)));
        }
        self.session.finish()?;
        Ok(())
    }

    fn abort(self: Box<Self>) -> Option<String> {
        self.session.abort()
    }
}
