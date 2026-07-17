use std::io::{Error, Result as IoResult, Write};
use std::path::Path;
use std::sync::{Arc, Mutex};

use anyhow::{Context, Result};
use file_rotate::compression::Compression;
use file_rotate::suffix::AppendCount;
use file_rotate::{ContentLimit, FileRotate};
use tracing_subscriber::EnvFilter;
use tracing_subscriber::fmt::MakeWriter;
use tracing_subscriber::util::SubscriberInitExt;

const LOG_FILE_BYTES: usize = 10 * 1024 * 1024;
const ROTATED_LOG_FILES: usize = 9;

#[derive(Clone)]
struct LogWriter {
    file: Arc<Mutex<FileRotate<AppendCount>>>,
}

struct LogGuard {
    file: Arc<Mutex<FileRotate<AppendCount>>>,
}

impl LogWriter {
    fn new(path: &Path) -> Self {
        let file = FileRotate::new(
            path,
            AppendCount::new(ROTATED_LOG_FILES),
            ContentLimit::Bytes(LOG_FILE_BYTES),
            Compression::None,
            None,
        );
        Self {
            file: Arc::new(Mutex::new(file)),
        }
    }
}

impl<'writer> MakeWriter<'writer> for LogWriter {
    type Writer = LogGuard;

    fn make_writer(&'writer self) -> Self::Writer {
        LogGuard {
            file: Arc::clone(&self.file),
        }
    }
}

impl Write for LogGuard {
    fn write(&mut self, buffer: &[u8]) -> IoResult<usize> {
        self.file
            .lock()
            .map_err(|_| Error::other("log writer lock is poisoned"))?
            .write(buffer)
    }

    fn flush(&mut self) -> IoResult<()> {
        self.file
            .lock()
            .map_err(|_| Error::other("log writer lock is poisoned"))?
            .flush()
    }
}

pub fn initialize(path: &Path) -> Result<()> {
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_ansi(false)
        .with_target(false)
        .with_writer(LogWriter::new(path))
        .finish()
        .try_init()
        .context("initialize logging")
}
