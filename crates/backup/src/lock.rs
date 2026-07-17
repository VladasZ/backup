use std::fs::{File, OpenOptions};
use std::io::ErrorKind;
use std::path::Path;

use anyhow::{Context, Result, bail};
use fs2::FileExt;

pub struct AppLock {
    file: File,
}

impl AppLock {
    pub fn exclusive(path: &Path) -> Result<Self> {
        let file = open(path)?;
        file.lock_exclusive()
            .with_context(|| format!("lock {}", path.display()))?;
        Ok(Self { file })
    }

    pub fn shared(path: &Path) -> Result<Self> {
        let file = open(path)?;
        file.lock_shared()
            .with_context(|| format!("lock {}", path.display()))?;
        Ok(Self { file })
    }

    pub fn try_exclusive(path: &Path, conflict: &str) -> Result<Self> {
        let file = open(path)?;
        match file.try_lock_exclusive() {
            Ok(()) => Ok(Self { file }),
            Err(error) if error.kind() == ErrorKind::WouldBlock => bail!("{conflict}"),
            Err(error) => Err(error).with_context(|| format!("lock {}", path.display())),
        }
    }
}

impl Drop for AppLock {
    fn drop(&mut self) {
        if let Err(error) = FileExt::unlock(&self.file) {
            tracing::error!(%error, "failed to release application lock");
        }
    }
}

fn open(path: &Path) -> Result<File> {
    OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .open(path)
        .with_context(|| format!("open lock file {}", path.display()))
}
