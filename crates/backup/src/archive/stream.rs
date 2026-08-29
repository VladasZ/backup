use std::fmt::{Display, Formatter, Result as FmtResult};
use std::io::{self, Write};
use std::path::PathBuf;

use anyhow::{Error, Result};
use blake3::Hasher;
use tracing::warn;

use super::catalog::{SourceScanner, Visit, changed_paths};
use super::format::ArchiveWriter;
use crate::config::BackupJob;
use crate::location::Location;
use crate::output::{Event, emit};

const CHANGED_PATHS_IN_LOG: usize = 20;
const PROGRESS_STEP: u64 = 64 * 1024 * 1024;

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum SinkId {
    Destination(Location),
    Staging,
    Stream,
}

impl Display for SinkId {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> FmtResult {
        match self {
            Self::Destination(location) => write!(formatter, "{location}"),
            Self::Staging => write!(formatter, "staging"),
            Self::Stream => write!(formatter, "stream"),
        }
    }
}

pub trait Sink: Send {
    fn id(&self) -> SinkId;
    fn write_all(&mut self, bytes: &[u8]) -> io::Result<()>;
    fn finish(self: Box<Self>, checksum: &str, size: u64) -> Result<()>;
    fn abort(self: Box<Self>) -> Option<String>;
}

#[derive(Debug)]
pub struct SinkOutcome {
    pub id: SinkId,
    pub error: Option<String>,
}

impl Display for SinkOutcome {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> FmtResult {
        match &self.error {
            Some(error) => write!(formatter, "{}: {error}", self.id),
            None => write!(formatter, "{}: ok", self.id),
        }
    }
}

#[derive(Debug)]
pub struct StreamOutcome {
    pub checksum: String,
    pub size: u64,
    pub changed: Vec<PathBuf>,
    pub sinks: Vec<SinkOutcome>,
}

pub struct Tee {
    live: Vec<Box<dyn Sink>>,
    failed: Vec<SinkOutcome>,
    hasher: Hasher,
    size: u64,
    reported: u64,
}

impl Tee {
    pub fn new(sinks: Vec<Box<dyn Sink>>) -> Self {
        Self {
            live: sinks,
            failed: Vec::new(),
            hasher: Hasher::new(),
            size: 0,
            reported: 0,
        }
    }

    pub fn checksum(&self) -> String {
        self.hasher.finalize().to_hex().to_string()
    }

    pub fn size(&self) -> u64 {
        self.size
    }

    pub fn complete(self, checksum: &str) -> Vec<SinkOutcome> {
        let mut outcomes = self.failed;
        for sink in self.live {
            let id = sink.id();
            let error = sink.finish(checksum, self.size).err();
            outcomes.push(SinkOutcome {
                id,
                error: error.map(|error| format!("{error:#}")),
            });
        }
        outcomes
    }

    pub fn abort(self) -> Vec<SinkOutcome> {
        for sink in self.live {
            sink.abort();
        }
        self.failed
    }

    fn fan_out(&mut self, bytes: &[u8]) -> io::Result<()> {
        let mut index = 0;
        while index < self.live.len() {
            match self.live[index].write_all(bytes) {
                Ok(()) => index += 1,
                Err(error) => {
                    let sink = self.live.remove(index);
                    let id = sink.id();
                    let detail = sink.abort().unwrap_or_else(|| error.to_string());
                    warn!(sink = %id, error = %detail, "output failed; continuing with the others");
                    self.failed.push(SinkOutcome {
                        id,
                        error: Some(detail),
                    });
                }
            }
        }
        if self.live.is_empty() {
            return Err(io::Error::other(format!(
                "every output failed: {}",
                describe(&self.failed)
            )));
        }
        Ok(())
    }
}

impl Write for Tee {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        self.fan_out(bytes)?;
        self.hasher.update(bytes);
        self.size += bytes.len() as u64;
        if self.size - self.reported >= PROGRESS_STEP {
            self.reported = self.size;
            emit(&Event::Progress { bytes: self.size });
        }
        Ok(bytes.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

pub fn pump_local(job: &BackupJob, scanner: &SourceScanner, mut tee: Tee) -> Result<StreamOutcome> {
    let produced = (|| {
        let mut writer = ArchiveWriter::new(&mut tee);
        let before = scanner.walk(&mut |entry| writer.append(entry))?;
        writer.finish()?;
        Ok(before)
    })();
    let before = match produced {
        Ok(before) => before,
        Err(error) => return Err(abort_with(tee, error)),
    };
    for mount in &before.skipped_mounts {
        warn!(job = job.name, path = %mount.display(), "skipping nested mount");
    }
    for special in &before.skipped_special {
        warn!(job = job.name, path = %special.display(), "skipping special file");
    }
    for unreadable in &before.skipped_unreadable {
        warn!(job = job.name, path = %unreadable.path.display(), reason = unreadable.reason, "skipping unreadable entry");
    }
    let changed = match scanner.walk(&mut |_| Ok(Visit::Stored)) {
        Ok(after) => changed_paths(&before.fingerprints, &after.fingerprints),
        Err(error) => {
            warn!(job = job.name, %error, "could not rescan the source after archiving; consistency is unknown");
            Vec::new()
        }
    };
    warn_changed(&job.name, &changed);
    let checksum = tee.checksum();
    let size = tee.size();
    let sinks = tee.complete(&checksum);
    Ok(StreamOutcome {
        checksum,
        size,
        changed,
        sinks,
    })
}

pub fn abort_with(tee: Tee, error: Error) -> Error {
    let failed = tee.abort();
    if failed.is_empty() {
        return error;
    }
    error.context(format!("outputs failed: {}", describe(&failed)))
}

pub fn warn_changed(job: &str, changed: &[PathBuf]) {
    if changed.is_empty() {
        return;
    }
    let shown: Vec<String> = changed
        .iter()
        .take(CHANGED_PATHS_IN_LOG)
        .map(|path| path.display().to_string())
        .collect();
    warn!(
        job,
        count = changed.len(),
        paths = ?shown,
        "source changed while it was being archived; the archive may be inconsistent"
    );
}

fn describe(outcomes: &[SinkOutcome]) -> String {
    outcomes
        .iter()
        .map(ToString::to_string)
        .collect::<Vec<_>>()
        .join("; ")
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::io;
    use std::path::PathBuf;
    use std::sync::{Arc, Mutex};

    use anyhow::{Result, bail};
    use tempfile::tempdir;

    use super::{Sink, SinkId, Tee, pump_local};
    use crate::archive::SourceScanner;
    use crate::config::BackupJob;
    use crate::location::Location;

    struct MemorySink {
        id: SinkId,
        bytes: Arc<Mutex<Vec<u8>>>,
        fail_after: Option<usize>,
        on_first_write: Option<Box<dyn FnOnce() + Send>>,
    }

    impl Sink for MemorySink {
        fn id(&self) -> SinkId {
            self.id.clone()
        }

        fn write_all(&mut self, bytes: &[u8]) -> io::Result<()> {
            if let Some(hook) = self.on_first_write.take() {
                hook();
            }
            let mut stored = self.bytes.lock().unwrap();
            stored.extend_from_slice(bytes);
            if self.fail_after.is_some_and(|limit| stored.len() > limit) {
                return Err(io::Error::other("disk full"));
            }
            Ok(())
        }

        fn finish(self: Box<Self>, _checksum: &str, size: u64) -> Result<()> {
            if self.bytes.lock().unwrap().len() as u64 != size {
                bail!("size mismatch");
            }
            Ok(())
        }

        fn abort(self: Box<Self>) -> Option<String> {
            self.bytes.lock().unwrap().clear();
            None
        }
    }

    fn job(source: &std::path::Path) -> BackupJob {
        BackupJob {
            name: "job".to_owned(),
            source: Location::Local(source.to_path_buf()),
            destinations: vec![Location::Local(PathBuf::from("/unused"))],
            cron: "0 0 * * *".to_owned(),
            retention: None,
            pre: None,
            exclude: Vec::new(),
        }
    }

    fn sink(
        name: &str,
        fail_after: Option<usize>,
        hook: Option<Box<dyn FnOnce() + Send>>,
    ) -> (Box<dyn Sink>, Arc<Mutex<Vec<u8>>>) {
        let bytes = Arc::new(Mutex::new(Vec::new()));
        let sink = MemorySink {
            id: SinkId::Destination(Location::Local(PathBuf::from(format!("/{name}")))),
            bytes: Arc::clone(&bytes),
            fail_after,
            on_first_write: hook,
        };
        (Box::new(sink), bytes)
    }

    #[test]
    fn one_failing_output_does_not_stop_the_others() {
        let temporary = tempdir().unwrap();
        let source = temporary.path().join("source");
        fs::create_dir(&source).unwrap();
        fs::write(source.join("data.bin"), vec![7u8; 200_000]).unwrap();
        let scanner = SourceScanner::new(&source, &[]).unwrap();
        let (good, good_bytes) = sink("good", None, None);
        let (bad, bad_bytes) = sink("bad", Some(10), None);

        let outcome = pump_local(&job(&source), &scanner, Tee::new(vec![bad, good])).unwrap();

        assert_eq!(outcome.size, good_bytes.lock().unwrap().len() as u64);
        assert!(bad_bytes.lock().unwrap().is_empty());
        let failed: Vec<_> = outcome
            .sinks
            .iter()
            .filter(|sink| sink.error.is_some())
            .collect();
        assert_eq!(failed.len(), 1);
        assert_eq!(
            failed[0].id,
            SinkId::Destination(Location::Local(PathBuf::from("/bad")))
        );
        assert!(outcome.changed.is_empty());
    }

    #[test]
    fn every_output_failing_is_an_error() {
        let temporary = tempdir().unwrap();
        let source = temporary.path().join("source");
        fs::create_dir(&source).unwrap();
        fs::write(source.join("data.bin"), vec![7u8; 2000]).unwrap();
        let scanner = SourceScanner::new(&source, &[]).unwrap();
        let (bad, _) = sink("bad", Some(0), None);

        let error = pump_local(&job(&source), &scanner, Tee::new(vec![bad])).unwrap_err();

        assert!(format!("{error:#}").contains("every output failed"));
    }

    #[test]
    fn a_source_change_during_the_pass_is_reported_not_fatal() {
        let temporary = tempdir().unwrap();
        let source = temporary.path().join("source");
        fs::create_dir(&source).unwrap();
        let busy = source.join("busy.txt");
        fs::write(&busy, "first").unwrap();
        fs::write(source.join("quiet.txt"), "quiet").unwrap();
        let scanner = SourceScanner::new(&source, &[]).unwrap();
        let changed_file = busy.clone();
        let hook: Box<dyn FnOnce() + Send> = Box::new(move || {
            fs::write(&changed_file, "second version, longer").unwrap();
        });
        let (memory, _) = sink("memory", None, Some(hook));

        let outcome = pump_local(&job(&source), &scanner, Tee::new(vec![memory])).unwrap();

        assert_eq!(outcome.changed, vec![PathBuf::from("busy.txt")]);
        assert!(outcome.sinks.iter().all(|sink| sink.error.is_none()));
    }
}
