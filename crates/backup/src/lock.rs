use std::fs::{File, OpenOptions, TryLockError};
use std::path::Path;

use anyhow::{Context, Result, bail};

pub struct AppLock {
    file: File,
}

impl AppLock {
    pub fn exclusive(path: &Path) -> Result<Self> {
        let file = open(path)?;
        file.lock()
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
        match file.try_lock() {
            Ok(()) => Ok(Self { file }),
            Err(TryLockError::WouldBlock) => bail!("{conflict}"),
            Err(TryLockError::Error(error)) => {
                Err(error).with_context(|| format!("lock {}", path.display()))
            }
        }
    }
}

// The probe briefly holds the lock itself, so a daemon starting at that exact
// moment can fail to acquire it. Health checks run rarely, the window is tiny.
pub fn is_locked(path: &Path) -> Result<bool> {
    let file = open(path)?;
    match file.try_lock() {
        Ok(()) => Ok(false),
        Err(TryLockError::WouldBlock) => Ok(true),
        Err(TryLockError::Error(error)) => {
            Err(error).with_context(|| format!("probe lock {}", path.display()))
        }
    }
}

impl Drop for AppLock {
    fn drop(&mut self) {
        if let Err(error) = self.file.unlock() {
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
