mod catalog;
mod checksum;
mod format;
mod restore;
mod staging;
mod stream;

use std::path::PathBuf;

use chrono::{DateTime, SecondsFormat, Utc};

use crate::config::ARCHIVE_EXTENSION;

pub use catalog::{SourceScanner, ensure_not_symlink};
pub use checksum::{HashingWriter, checksum_file, read_checksum, verify_checksum, write_checksum};
pub use restore::{copy_archive_contents, restore_archive, verify_archive};
pub use staging::StagingSink;
pub use stream::{
    Sink, SinkId, SinkOutcome, StreamOutcome, Tee, abort_with, pump_local, warn_changed,
};

#[derive(Clone, Debug)]
pub struct Artifact {
    pub name: String,
    pub path: PathBuf,
    pub checksum_path: PathBuf,
    pub checksum: String,
    pub size: u64,
    pub created_at: DateTime<Utc>,
}

pub fn archive_name(job: &str, created_at: DateTime<Utc>) -> String {
    let timestamp = created_at.to_rfc3339_opts(SecondsFormat::Secs, true);
    format!(
        "{job}-{timestamp}-{}.{ARCHIVE_EXTENSION}",
        uuid::Uuid::new_v4()
    )
}

#[cfg(test)]
pub fn create_local_archive(
    job: &crate::config::BackupJob,
    source: &std::path::Path,
    staging: &std::path::Path,
) -> anyhow::Result<Artifact> {
    use anyhow::bail;

    let scanner = SourceScanner::new(source, &job.exclude)?;
    let created_at = Utc::now();
    let name = archive_name(&job.name, created_at);
    let sink = StagingSink::open(staging, &name)?;
    let outcome = pump_local(job, &scanner, Tee::new(vec![sink]))?;
    if let Some(failed) = outcome.sinks.iter().find(|sink| sink.error.is_some()) {
        bail!("{failed}");
    }
    let path = staging.join(&name);
    Ok(Artifact {
        checksum_path: crate::destination::checksum_path(&path),
        name,
        path,
        checksum: outcome.checksum,
        size: outcome.size,
        created_at,
    })
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::os::unix::fs::{MetadataExt, PermissionsExt, symlink};
    use std::path::{Path, PathBuf};

    use tempfile::tempdir;

    use super::{create_local_archive, read_checksum, restore_archive, verify_archive};
    use crate::config::{BackupJob, RetentionConfig};
    use crate::location::Location;

    fn job(source: &Path) -> BackupJob {
        BackupJob {
            name: "test".to_owned(),
            source: Location::Local(source.to_path_buf()),
            destinations: vec![Location::Local(PathBuf::from("/unused"))],
            cron: "0 0 * * *".to_owned(),
            retention: None,
            exclude: Vec::new(),
        }
    }

    #[test]
    fn link_targets_longer_than_a_tar_header_field_round_trip() {
        let temporary = tempdir().unwrap();
        let source = temporary.path().join("source");
        let restored = temporary.path().join("restored");
        let deep = source.join("a".repeat(60)).join("b".repeat(60));
        fs::create_dir_all(&deep).unwrap();
        fs::write(deep.join("file.txt"), "linked").unwrap();
        fs::hard_link(deep.join("file.txt"), source.join("hard-link")).unwrap();
        let target = PathBuf::from("/").join("x".repeat(150)).join("target");
        symlink(&target, source.join("symbolic-link")).unwrap();

        let artifact =
            create_local_archive(&job(&source), &source, &temporary.path().join("staging"))
                .unwrap();
        verify_archive(&artifact.path).unwrap();
        restore_archive(&artifact.path, &restored).unwrap();

        assert_eq!(
            fs::read_link(restored.join("symbolic-link")).unwrap(),
            target
        );
        assert_eq!(
            fs::metadata(restored.join("hard-link")).unwrap().ino(),
            fs::metadata(
                restored
                    .join("a".repeat(60))
                    .join("b".repeat(60))
                    .join("file.txt")
            )
            .unwrap()
            .ino()
        );
    }

    #[test]
    fn an_unreadable_file_is_skipped_and_the_rest_is_archived() {
        let temporary = tempdir().unwrap();
        let source = temporary.path().join("source");
        let restored = temporary.path().join("restored");
        fs::create_dir_all(&source).unwrap();
        fs::write(source.join("readable.txt"), "fine").unwrap();
        let locked = source.join("locked.txt");
        fs::write(&locked, "secret").unwrap();
        fs::set_permissions(&locked, PermissionsExt::from_mode(0o000)).unwrap();
        if fs::read(&locked).is_ok() {
            eprintln!("skipping: this user can read a mode 000 file");
            return;
        }
        fs::write(source.join("zebra.txt"), "after").unwrap();

        let artifact =
            create_local_archive(&job(&source), &source, &temporary.path().join("staging"))
                .unwrap();
        verify_archive(&artifact.path).unwrap();
        restore_archive(&artifact.path, &restored).unwrap();

        assert_eq!(
            fs::read_to_string(restored.join("readable.txt")).unwrap(),
            "fine"
        );
        assert_eq!(
            fs::read_to_string(restored.join("zebra.txt")).unwrap(),
            "after"
        );
        assert!(!restored.join("locked.txt").exists());
    }

    #[test]
    fn creates_and_restores_an_archive() {
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

        let artifact = create_local_archive(&job, &source, &staging).unwrap();
        let expected = read_checksum(&artifact.checksum_path).unwrap();
        assert_eq!(expected, artifact.checksum);
        assert_eq!(fs::metadata(&artifact.path).unwrap().len(), artifact.size);
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
