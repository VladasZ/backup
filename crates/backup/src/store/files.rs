use std::fs::{self, File};
use std::io::{ErrorKind, Write};
use std::path::{Path, PathBuf};
use std::time::{Duration, SystemTime};

use anyhow::{Context, Result};
use tracing::{info, warn};
use uuid::Uuid;

use crate::archive::create_private;

const STALE_PARTIAL_AGE: Duration = Duration::from_secs(24 * 60 * 60);

// Every store file is written under a hidden partial name, synced, and renamed
// into place, then the directory is synced so the rename itself survives a
// power cut. A reader never sees half a file under a real name.
pub fn write_atomic(path: &Path, bytes: &[u8]) -> Result<()> {
    let directory = path
        .parent()
        .context("store file has no parent directory")?;
    fs::create_dir_all(directory)
        .with_context(|| format!("create directory {}", directory.display()))?;
    let name = path
        .file_name()
        .context("store file has no name")?
        .to_string_lossy();
    let partial = directory.join(format!(".{name}.{}.partial", Uuid::new_v4()));
    let written = (|| {
        let mut file = create_private(&partial)?;
        file.write_all(bytes)
            .with_context(|| format!("write {}", partial.display()))?;
        file.sync_all()
            .with_context(|| format!("sync {}", partial.display()))?;
        fs::rename(&partial, path).with_context(|| format!("publish {}", path.display()))?;
        sync_directory(directory)
    })();
    if written.is_err() {
        remove_if_present(&partial)?;
    }
    written
}

pub fn sync_directory(directory: &Path) -> Result<()> {
    File::open(directory)
        .and_then(|handle| handle.sync_all())
        .with_context(|| format!("sync directory {}", directory.display()))
}

pub fn remove_if_present(path: &Path) -> Result<()> {
    match fs::remove_file(path) {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error).with_context(|| format!("remove {}", path.display())),
    }
}

pub fn read(path: &Path) -> Result<Vec<u8>> {
    fs::read(path).with_context(|| format!("read {}", path.display()))
}

/// Files in `directory` whose names end with `suffix`, ignoring hidden names,
/// which are partial writes.
pub fn list(directory: &Path, suffix: &str) -> Result<Vec<PathBuf>> {
    let entries = match fs::read_dir(directory) {
        Ok(entries) => entries,
        Err(error) if error.kind() == ErrorKind::NotFound => return Ok(Vec::new()),
        Err(error) => {
            return Err(error).with_context(|| format!("read directory {}", directory.display()));
        }
    };
    let mut found = Vec::new();
    for entry in entries {
        let entry = entry.with_context(|| format!("read directory {}", directory.display()))?;
        let name = entry.file_name().to_string_lossy().into_owned();
        if name.starts_with('.') || !name.ends_with(suffix) {
            continue;
        }
        found.push(entry.path());
    }
    found.sort();
    Ok(found)
}

pub fn modified_before(path: &Path, cutoff: SystemTime) -> Result<bool> {
    let modified = fs::metadata(path)
        .and_then(|metadata| metadata.modified())
        .with_context(|| format!("read the age of {}", path.display()))?;
    Ok(modified < cutoff)
}

// A crashed run or a killed SSH connection leaves partial files behind, and a
// retry writes under a new name, so nothing else removes them. An active
// partial keeps a fresh modification time, so the age guard never removes a
// write that is still running.
pub fn sweep_stale_partials(directory: &Path) {
    let entries = match fs::read_dir(directory) {
        Ok(entries) => entries,
        Err(error) if error.kind() == ErrorKind::NotFound => return,
        Err(error) => {
            warn!(directory = %directory.display(), %error, "could not scan for stale partial files");
            return;
        }
    };
    let cutoff = SystemTime::now() - STALE_PARTIAL_AGE;
    for entry in entries {
        let entry = match entry {
            Ok(entry) => entry,
            Err(error) => {
                warn!(directory = %directory.display(), %error, "could not read a directory entry");
                continue;
            }
        };
        if !is_temporary_name(&entry.file_name().to_string_lossy()) {
            continue;
        }
        let path = entry.path();
        match modified_before(&path, cutoff) {
            Ok(false) => {}
            Ok(true) => match fs::remove_file(&path) {
                Ok(()) => info!(path = %path.display(), "removed stale partial file"),
                Err(error) => {
                    warn!(path = %path.display(), %error, "could not remove stale partial file");
                }
            },
            Err(error) => warn!(%error, "could not read the age of a partial file"),
        }
    }
}

fn is_temporary_name(name: &str) -> bool {
    (name.starts_with('.') && name.ends_with(".partial")) || name.starts_with(".backup-write-test-")
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::os::unix::fs::PermissionsExt;
    use std::time::{Duration, SystemTime};

    use tempfile::tempdir;

    use super::{list, sweep_stale_partials, write_atomic};

    #[test]
    fn an_atomic_write_is_private_and_leaves_no_partial() {
        let temporary = tempdir().unwrap();
        let path = temporary.path().join("nested/file.bin");
        write_atomic(&path, b"payload").unwrap();
        assert_eq!(fs::read(&path).unwrap(), b"payload");
        let mode = fs::metadata(&path).unwrap().permissions().mode();
        assert_eq!(mode & 0o777, 0o600);
        assert_eq!(
            fs::read_dir(temporary.path().join("nested"))
                .unwrap()
                .count(),
            1
        );
    }

    #[test]
    fn only_stale_temporary_files_are_swept() {
        let temporary = tempdir().unwrap();
        let directory = temporary.path();
        let stale = directory.join(".pack.partial");
        let probe = directory.join(".backup-write-test-x");
        let fresh = directory.join(".new.partial");
        let real = directory.join("real.pack");
        for path in [&stale, &probe, &fresh, &real] {
            fs::write(path, "x").unwrap();
        }
        let old = SystemTime::now() - Duration::from_secs(2 * 24 * 60 * 60);
        for path in [&stale, &probe, &real] {
            fs::File::open(path).unwrap().set_modified(old).unwrap();
        }

        sweep_stale_partials(directory);

        assert!(!stale.exists());
        assert!(!probe.exists());
        assert!(fresh.exists(), "a fresh partial was removed");
        assert!(real.exists(), "a real file was removed");
        assert_eq!(list(directory, ".pack").unwrap(), vec![real]);
    }
}
