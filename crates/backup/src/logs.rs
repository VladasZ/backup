use std::fs::{self, File};
use std::io::{ErrorKind, Seek, SeekFrom, Write, copy, stdout};
use std::os::unix::fs::MetadataExt;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread;
use std::time::Duration;

use anyhow::{Context, Result};

const ROTATED_LOG_FILES: usize = 9;
const FOLLOW_INTERVAL: Duration = Duration::from_millis(500);

pub fn show(path: &Path, follow: bool) -> Result<()> {
    let mut output = stdout().lock();
    for log in log_paths(path) {
        if log.exists() {
            let mut file =
                File::open(&log).with_context(|| format!("open log file {}", log.display()))?;
            copy(&mut file, &mut output)?;
        }
    }
    output.flush()?;
    drop(output);
    if follow {
        follow_log(path)?;
    }
    Ok(())
}

fn log_paths(path: &Path) -> Vec<PathBuf> {
    let mut paths = (1..=ROTATED_LOG_FILES)
        .rev()
        .map(|suffix| PathBuf::from(format!("{}.{suffix}", path.display())))
        .collect::<Vec<_>>();
    paths.push(path.to_path_buf());
    paths
}

fn follow_log(path: &Path) -> Result<()> {
    let stopping = Arc::new(AtomicBool::new(false));
    let signal = Arc::clone(&stopping);
    ctrlc::set_handler(move || signal.store(true, Ordering::SeqCst))?;
    let mut identity = file_identity(path)?;
    let mut position = fs::metadata(path)
        .map(|metadata| metadata.len())
        .unwrap_or(0);
    while !stopping.load(Ordering::SeqCst) {
        thread::sleep(FOLLOW_INTERVAL);
        let next_identity = file_identity(path)?;
        if next_identity != identity {
            identity = next_identity;
            position = 0;
        }
        let Ok(mut file) = File::open(path) else {
            continue;
        };
        let length = file.metadata()?.len();
        if length < position {
            position = 0;
        }
        file.seek(SeekFrom::Start(position))?;
        let mut output = stdout().lock();
        let copied = copy(&mut file, &mut output)?;
        output.flush()?;
        position += copied;
    }
    Ok(())
}

fn file_identity(path: &Path) -> Result<Option<(u64, u64)>> {
    match fs::metadata(path) {
        Ok(metadata) => Ok(Some((metadata.dev(), metadata.ino()))),
        Err(error) if error.kind() == ErrorKind::NotFound => Ok(None),
        Err(error) => Err(error).with_context(|| format!("read log file {}", path.display())),
    }
}
