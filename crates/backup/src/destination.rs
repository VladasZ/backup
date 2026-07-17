use std::fs::{self, File, OpenOptions};
use std::io::ErrorKind;
use std::path::{Path, PathBuf};
use std::time::SystemTime;

use anyhow::{Context, Result};
use chrono::{DateTime, Duration, Utc};
use tracing::{info, warn};

use crate::archive::{Artifact, read_checksum, verify_checksum, write_checksum};
use crate::config::{BackupJob, RetentionConfig};
use crate::storage::warn_if_high;

#[derive(Clone, Debug)]
pub struct ArchiveInfo {
    pub name: String,
    pub path: PathBuf,
    pub checksum: String,
    pub size: u64,
    pub modified: DateTime<Utc>,
}

pub fn deliver_local(artifact: &Artifact, destination: &Path, job: &BackupJob) -> Result<()> {
    fs::create_dir_all(destination)
        .with_context(|| format!("create destination {}", destination.display()))?;
    warn_if_high(destination, "destination")?;
    let target = destination.join(&artifact.name);
    if target.exists() {
        match verify_checksum(&target, &artifact.checksum) {
            Ok(()) => {
                ensure_checksum_sidecar(&target, &artifact.checksum)?;
                apply_retention(destination, job)?;
                return Ok(());
            }
            Err(error) => {
                warn!(
                    path = %target.display(),
                    %error,
                    "replacing corrupt destination archive"
                );
            }
        }
    }

    let partial = destination.join(format!(".{}.partial", artifact.name));
    remove_if_present(&partial)?;
    fs::copy(&artifact.path, &partial).with_context(|| {
        format!(
            "copy archive {} to {}",
            artifact.path.display(),
            partial.display()
        )
    })?;
    OpenOptions::new().read(true).open(&partial)?.sync_all()?;
    verify_checksum(&partial, &artifact.checksum)?;
    fs::rename(&partial, &target)
        .with_context(|| format!("publish destination archive {}", target.display()))?;
    write_checksum(&target, &artifact.checksum)?;
    File::open(destination)?.sync_all()?;
    info!(
        job = job.name,
        destination = %destination.display(),
        archive = artifact.name,
        "delivered archive"
    );
    apply_retention(destination, job)
}

pub fn list_local(destination: &Path, job: &str) -> Result<Vec<ArchiveInfo>> {
    if !destination.exists() {
        return Ok(Vec::new());
    }
    let prefix = format!("{job}-");
    let mut archives = Vec::new();
    for entry in fs::read_dir(destination)
        .with_context(|| format!("read destination {}", destination.display()))?
    {
        let entry = entry?;
        let metadata = entry.metadata()?;
        if !metadata.is_file() {
            continue;
        }
        let name = entry.file_name().to_string_lossy().into_owned();
        if !name.starts_with(&prefix) || !is_archive_name(&name) {
            continue;
        }
        let modified = metadata.modified().unwrap_or(SystemTime::UNIX_EPOCH);
        let path = entry.path();
        let checksum = read_checksum(&checksum_path(&path))
            .with_context(|| format!("read checksum for destination archive {}", path.display()))?;
        archives.push(ArchiveInfo {
            name,
            path,
            checksum,
            size: metadata.len(),
            modified: DateTime::from(modified),
        });
    }
    archives.sort_by(|left, right| {
        right
            .modified
            .cmp(&left.modified)
            .then_with(|| right.name.cmp(&left.name))
    });
    Ok(archives)
}

pub fn apply_retention(destination: &Path, job: &BackupJob) -> Result<()> {
    let Some(retention) = &job.retention else {
        return Ok(());
    };
    let archives = list_local(destination, &job.name)?;
    let removals = retention_removals(&archives, retention)?;
    for archive in removals {
        let sidecar = checksum_path(&archive.path);
        fs::remove_file(&archive.path)
            .with_context(|| format!("remove retained archive {}", archive.path.display()))?;
        remove_if_present(&sidecar)?;
        info!(
            job = job.name,
            archive = archive.name,
            destination = %destination.display(),
            "removed archive due to retention"
        );
    }
    Ok(())
}

fn retention_removals<'archive>(
    archives: &'archive [ArchiveInfo],
    retention: &RetentionConfig,
) -> Result<Vec<&'archive ArchiveInfo>> {
    if let Some(count) = retention.count {
        return Ok(archives.iter().skip(count).collect());
    }
    let Some(age) = retention.age_duration()? else {
        return Ok(Vec::new());
    };
    let age = Duration::from_std(age).context("retention age is too large")?;
    let cutoff = Utc::now() - age;
    Ok(archives
        .iter()
        .filter(|archive| archive.modified < cutoff)
        .collect())
}

fn ensure_checksum_sidecar(archive: &Path, checksum: &str) -> Result<()> {
    let sidecar = checksum_path(archive);
    if sidecar.exists() {
        let existing = read_checksum(&sidecar)?;
        if existing != checksum {
            warn!(
                path = %sidecar.display(),
                "repairing checksum sidecar that disagrees with verified archive"
            );
            write_checksum(archive, checksum)?;
        }
        return Ok(());
    }
    write_checksum(archive, checksum)?;
    Ok(())
}

pub fn checksum_path(archive: &Path) -> PathBuf {
    PathBuf::from(format!("{}.blake3", archive.display()))
}

pub(crate) fn is_archive_name(name: &str) -> bool {
    [".tar", ".tar.gz", ".tar.zst", ".tar.zpaq"]
        .iter()
        .any(|extension| name.ends_with(extension))
}

fn remove_if_present(path: &Path) -> Result<()> {
    match fs::remove_file(path) {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == ErrorKind::NotFound => Ok(()),
        Err(error) => {
            warn!(path = %path.display(), %error, "failed to remove partial file");
            Err(error).with_context(|| format!("remove {}", path.display()))
        }
    }
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::path::PathBuf;

    use chrono::{Duration, Utc};
    use tempfile::tempdir;

    use super::{ArchiveInfo, checksum_path, deliver_local, retention_removals};
    use crate::archive::{Artifact, checksum_file, read_checksum};
    use crate::config::{BackupJob, RetentionConfig};
    use crate::location::Location;

    #[test]
    fn count_retention_keeps_newest_archives() {
        let directory = tempdir().unwrap();
        let archives: Vec<_> = (0..4)
            .map(|offset| ArchiveInfo {
                name: format!("job-{offset}.tar"),
                path: directory.path().join(format!("job-{offset}.tar")),
                checksum: "unused".to_owned(),
                size: 1,
                modified: Utc::now() - Duration::minutes(offset),
            })
            .collect();
        let retention = RetentionConfig {
            count: Some(2),
            age: None,
        };
        let removals = retention_removals(&archives, &retention).unwrap();
        assert_eq!(removals.len(), 2);
        assert_eq!(removals[0].name, "job-2.tar");
    }

    #[test]
    fn delivery_replaces_a_corrupt_copy_and_repairs_its_sidecar() {
        let temporary = tempdir().unwrap();
        let staging = temporary.path().join("staging");
        let destination = temporary.path().join("destination");
        fs::create_dir_all(&staging).unwrap();
        fs::create_dir_all(&destination).unwrap();
        let archive_path = staging.join("job-archive.tar");
        fs::write(&archive_path, "healthy archive").unwrap();
        let checksum = checksum_file(&archive_path).unwrap();
        let artifact = Artifact {
            name: "job-archive.tar".to_owned(),
            path: archive_path,
            checksum_path: PathBuf::new(),
            checksum: checksum.clone(),
            size: 15,
            created_at: Utc::now(),
        };
        let target = destination.join(&artifact.name);
        fs::write(&target, "corrupt").unwrap();
        fs::write(checksum_path(&target), "wrong  job-archive.tar\n").unwrap();
        let job = BackupJob {
            name: "job".to_owned(),
            source: Location::Local(PathBuf::from("/source")),
            destinations: vec![Location::Local(destination.clone())],
            cron: "0 0 * * *".to_owned(),
            retention: None,
            exclude: Vec::new(),
        };

        deliver_local(&artifact, &destination, &job).unwrap();

        assert_eq!(fs::read_to_string(&target).unwrap(), "healthy archive");
        assert_eq!(read_checksum(&checksum_path(&target)).unwrap(), checksum);
    }
}
