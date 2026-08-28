use std::fs::{self, OpenOptions};
use std::io::{Error, ErrorKind, Result as IoResult, Write, stderr};
use std::mem::take;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use tracing_subscriber::EnvFilter;
use tracing_subscriber::fmt::MakeWriter;
use tracing_subscriber::util::SubscriberInitExt;

use crate::lock::AppLock;

const LOG_FILE_BYTES: u64 = 10 * 1024 * 1024;
const ROTATED_LOG_FILES: usize = 9;

// Several processes can write this file at once: the daemon, and one remote
// agent per incoming SSH connection. Each event takes a file lock, rotates if
// the file is full, appends, and closes. No process holds an open handle
// across a rotation, so concurrent writers cannot clobber each other.
#[derive(Clone)]
struct LogWriter {
    path: PathBuf,
    lock: PathBuf,
    limit: u64,
}

impl LogWriter {
    fn new(path: &Path) -> Self {
        Self {
            path: path.to_path_buf(),
            lock: PathBuf::from(format!("{}.lock", path.display())),
            limit: LOG_FILE_BYTES,
        }
    }

    fn append(&self, bytes: &[u8]) -> Result<()> {
        let lock = AppLock::exclusive(&self.lock)?;
        self.rotate_if_full()?;
        let mut file = OpenOptions::new()
            .append(true)
            .create(true)
            .open(&self.path)
            .with_context(|| format!("open log file {}", self.path.display()))?;
        file.write_all(bytes)
            .with_context(|| format!("write log file {}", self.path.display()))?;
        drop(lock);
        Ok(())
    }

    fn rotate_if_full(&self) -> Result<()> {
        let length = match fs::metadata(&self.path) {
            Ok(metadata) => metadata.len(),
            Err(error) if error.kind() == ErrorKind::NotFound => return Ok(()),
            Err(error) => {
                return Err(error)
                    .with_context(|| format!("read log file {}", self.path.display()));
            }
        };
        if length < self.limit {
            return Ok(());
        }
        let rotated = |index: usize| PathBuf::from(format!("{}.{index}", self.path.display()));
        let oldest = rotated(ROTATED_LOG_FILES);
        match fs::remove_file(&oldest) {
            Ok(()) => {}
            Err(error) if error.kind() == ErrorKind::NotFound => {}
            Err(error) => {
                return Err(error).with_context(|| format!("remove {}", oldest.display()));
            }
        }
        for index in (1..ROTATED_LOG_FILES).rev() {
            let from = rotated(index);
            if from.exists() {
                fs::rename(&from, rotated(index + 1))?;
            }
        }
        fs::rename(&self.path, rotated(1))
            .with_context(|| format!("rotate log file {}", self.path.display()))?;
        Ok(())
    }
}

// One event is buffered whole and appended in a single locked write, so lines
// from concurrent processes never interleave inside one line.
struct EventWriter {
    writer: LogWriter,
    buffer: Vec<u8>,
}

impl EventWriter {
    fn commit(&mut self) -> Result<()> {
        if self.buffer.is_empty() {
            return Ok(());
        }
        let bytes = take(&mut self.buffer);
        self.writer.append(&bytes)
    }
}

impl Write for EventWriter {
    fn write(&mut self, buffer: &[u8]) -> IoResult<usize> {
        self.buffer.extend_from_slice(buffer);
        Ok(buffer.len())
    }

    fn flush(&mut self) -> IoResult<()> {
        self.commit().map_err(Error::other)
    }
}

impl Drop for EventWriter {
    fn drop(&mut self) {
        if let Err(error) = self.commit() {
            eprintln!("error: could not write to the log file: {error:#}");
        }
    }
}

impl<'writer> MakeWriter<'writer> for LogWriter {
    type Writer = EventWriter;

    fn make_writer(&'writer self) -> Self::Writer {
        EventWriter {
            writer: self.clone(),
            buffer: Vec::new(),
        }
    }
}

pub fn initialize(file: Option<&Path>) -> Result<()> {
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    let builder = tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_ansi(false)
        .with_target(false);
    match file {
        Some(path) => builder
            .with_writer(LogWriter::new(path))
            .finish()
            .try_init(),
        None => builder.with_writer(stderr).finish().try_init(),
    }
    .context("initialize logging")
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::path::PathBuf;

    use tempfile::tempdir;

    use super::LogWriter;

    #[test]
    fn a_full_log_rotates_and_the_oldest_file_is_dropped() {
        let temporary = tempdir().unwrap();
        let path = temporary.path().join("backup.log");
        let writer = LogWriter {
            path: path.clone(),
            lock: PathBuf::from(format!("{}.lock", path.display())),
            limit: 10,
        };
        let rotated = |index: usize| PathBuf::from(format!("{}.{index}", path.display()));

        writer.append(b"first line\n").unwrap();
        writer.append(b"second line\n").unwrap();
        writer.append(b"third\n").unwrap();

        assert_eq!(fs::read_to_string(&path).unwrap(), "third\n");
        assert_eq!(fs::read_to_string(rotated(1)).unwrap(), "second line\n");
        assert_eq!(fs::read_to_string(rotated(2)).unwrap(), "first line\n");
    }

    #[test]
    fn only_the_configured_number_of_rotated_files_is_kept() {
        let temporary = tempdir().unwrap();
        let path = temporary.path().join("backup.log");
        let writer = LogWriter {
            path: path.clone(),
            lock: PathBuf::from(format!("{}.lock", path.display())),
            limit: 1,
        };
        for index in 0..12 {
            writer.append(format!("line {index}\n").as_bytes()).unwrap();
        }
        let rotated = |index: usize| PathBuf::from(format!("{}.{index}", path.display()));

        assert_eq!(fs::read_to_string(&path).unwrap(), "line 11\n");
        assert_eq!(fs::read_to_string(rotated(9)).unwrap(), "line 2\n");
        assert!(!rotated(10).exists());
    }
}
