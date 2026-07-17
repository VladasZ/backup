mod catalog;
mod checksum;
mod format;
mod restore;

use std::fs::{self, File};
use std::io::ErrorKind;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result, bail};
use chrono::{DateTime, SecondsFormat, Utc};
use tracing::{info, warn};
use uuid::Uuid;

use crate::config::{BackupJob, CompressionConfig};

use catalog::SourceScanner;
pub use catalog::ensure_not_symlink;
pub use checksum::{checksum_file, read_checksum, verify_checksum, write_checksum};
use format::write_compressed_tar;
pub use restore::{copy_archive_contents, restore_archive, verify_archive};

const MAX_SOURCE_ATTEMPTS: usize = 5;

#[derive(Clone, Debug)]
pub struct Artifact {
    pub name: String,
    pub path: PathBuf,
    pub checksum_path: PathBuf,
    pub checksum: String,
    pub size: u64,
    pub created_at: DateTime<Utc>,
}

pub fn create_local_archive(
    job: &BackupJob,
    compression: &CompressionConfig,
    source: &Path,
    staging: &Path,
) -> Result<Artifact> {
    fs::create_dir_all(staging)
        .with_context(|| format!("create staging directory {}", staging.display()))?;
    let scanner = SourceScanner::new(source, &job.exclude)?;
    let created_at = Utc::now();
    let archive_id = Uuid::new_v4();
    let timestamp = created_at.to_rfc3339_opts(SecondsFormat::Secs, true);
    let name = format!(
        "{}-{timestamp}-{archive_id}.{}",
        job.name,
        compression.extension()
    );
    let final_path = staging.join(&name);
    let partial_path = staging.join(format!(".{name}.partial"));
    remove_if_present(&partial_path)?;

    let mut warned_mounts = Vec::new();
    for attempt in 1..=MAX_SOURCE_ATTEMPTS {
        let before = scanner.scan()?;
        for mount in &before.skipped_mounts {
            if !warned_mounts.contains(mount) {
                warn!(
                    job = job.name,
                    path = %mount.display(),
                    "skipping nested mount"
                );
                warned_mounts.push(mount.clone());
            }
        }
        let archive_result =
            write_compressed_tar(&partial_path, source, &before.records, compression);
        let after = scanner.scan()?;
        if before.records == after.records {
            if let Err(error) = archive_result {
                remove_if_present(&partial_path)?;
                return Err(error);
            }
            fs::rename(&partial_path, &final_path).with_context(|| {
                format!(
                    "publish archive {} as {}",
                    partial_path.display(),
                    final_path.display()
                )
            })?;
            let checksum = checksum_file(&final_path)?;
            let checksum_path = checksum::write_checksum(&final_path, &checksum)?;
            File::open(staging)?.sync_all()?;
            let size = fs::metadata(&final_path)?.len();
            info!(
                job = job.name,
                archive = %final_path.display(),
                size,
                "created stable archive"
            );
            return Ok(Artifact {
                name,
                path: final_path,
                checksum_path,
                checksum,
                size,
                created_at,
            });
        }
        remove_if_present(&partial_path)?;
        warn!(
            job = job.name,
            attempt, "source changed while it was being archived; discarding attempt"
        );
    }
    bail!(
        "job {:?} source changed during all {MAX_SOURCE_ATTEMPTS} attempts",
        job.name
    )
}

fn remove_if_present(path: &Path) -> Result<()> {
    match fs::remove_file(path) {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error).with_context(|| format!("remove {}", path.display())),
    }
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::os::unix::fs::{MetadataExt, symlink};
    use std::path::PathBuf;

    use tempfile::tempdir;

    use super::{create_local_archive, read_checksum, restore_archive, verify_archive};
    use crate::config::{BackupJob, CompressionAlgorithm, CompressionConfig, RetentionConfig};
    use crate::location::Location;

    #[test]
    fn creates_and_restores_every_compression_format() {
        for compression in [
            CompressionConfig {
                algorithm: CompressionAlgorithm::None,
                level: None,
            },
            CompressionConfig {
                algorithm: CompressionAlgorithm::Gzip,
                level: None,
            },
            CompressionConfig {
                algorithm: CompressionAlgorithm::Zstd,
                level: None,
            },
            CompressionConfig {
                algorithm: CompressionAlgorithm::Zpaq,
                level: Some(3),
            },
        ] {
            let temporary = tempdir().unwrap();
            let source = temporary.path().join("source");
            let staging = temporary.path().join("staging");
            let restored = temporary.path().join("restored");
            fs::create_dir_all(source.join("nested")).unwrap();
            fs::write(source.join("hello.txt"), "hello world").unwrap();
            fs::write(source.join("nested/data.bin"), [1, 2, 3, 4]).unwrap();
            fs::hard_link(source.join("hello.txt"), source.join("hard-link")).unwrap();
            symlink("hello.txt", source.join("symbolic-link")).unwrap();
            let job = BackupJob {
                name: "test".to_owned(),
                source: Location::Local(source.clone()),
                destinations: vec![Location::Local(PathBuf::from("/unused"))],
                cron: "0 0 * * *".to_owned(),
                retention: Some(RetentionConfig {
                    count: Some(2),
                    age: None,
                }),
                exclude: Vec::new(),
            };

            let artifact = create_local_archive(&job, &compression, &source, &staging).unwrap();
            let expected = read_checksum(&artifact.checksum_path).unwrap();
            assert_eq!(expected, artifact.checksum);
            verify_archive(&artifact.path).unwrap();
            restore_archive(&artifact.path, &restored).unwrap();

            assert_eq!(
                fs::read_to_string(restored.join("hello.txt")).unwrap(),
                "hello world"
            );
            assert_eq!(
                fs::read(restored.join("nested/data.bin")).unwrap(),
                [1, 2, 3, 4]
            );
            assert_eq!(
                fs::read_link(restored.join("symbolic-link")).unwrap(),
                PathBuf::from("hello.txt")
            );
            assert_eq!(
                fs::metadata(restored.join("hello.txt")).unwrap().ino(),
                fs::metadata(restored.join("hard-link")).unwrap().ino()
            );
        }
    }
}
