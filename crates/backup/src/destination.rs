use std::fs::{self, File, OpenOptions};
use std::io::{self, ErrorKind, Write};
use std::path::{Path, PathBuf};
use std::time::{Duration as StdDuration, SystemTime};

use anyhow::{Context, Result};
use chrono::{DateTime, Duration, Utc};
use tracing::{info, warn};

use crate::archive::{Artifact, Sink, SinkId, read_checksum, verify_checksum, write_checksum};
use crate::config::{BackupJob, RetentionConfig};
use crate::location::Location;
use crate::retention::milestone_keepers;
use crate::storage::warn_if_high;

pub struct LocalSink {
    destination: PathBuf,
    partial: PathBuf,
    target: PathBuf,
    file: File,
    job: BackupJob,
}

impl LocalSink {
    pub fn open(destination: &Path, name: &str, job: &BackupJob) -> Result<Box<dyn Sink>> {
        fs::create_dir_all(destination)
            .with_context(|| format!("create destination {}", destination.display()))?;
        warn_if_high(destination, "destination")?;
        sweep_stale_partials(destination);
        let partial = destination.join(format!(".{name}.partial"));
        remove_if_present(&partial)?;
        let file =
            File::create(&partial).with_context(|| format!("create {}", partial.display()))?;
        Ok(Box::new(Self {
            destination: destination.to_path_buf(),
            partial,
            target: destination.join(name),
            file,
            job: job.clone(),
        }))
    }
}

impl Sink for LocalSink {
    fn id(&self) -> SinkId {
        SinkId::Destination(Location::Local(self.destination.clone()))
    }

    fn write_all(&mut self, bytes: &[u8]) -> io::Result<()> {
        self.file.write_all(bytes)
    }

    fn finish(self: Box<Self>, checksum: &str, _size: u64) -> Result<()> {
        let result = (|| {
            self.file.sync_all()?;
            verify_checksum(&self.partial, checksum)?;
            publish(&self.partial, &self.target, checksum)?;
            Ok(())
        })();
        if result.is_err() {
            remove_if_present(&self.partial)?;
            return result;
        }
        info!(
            job = self.job.name,
            destination = %self.destination.display(),
            archive = %self.target.display(),
            "delivered archive"
        );
        prune_best_effort(&self.destination, &self.job);
        Ok(())
    }

    fn abort(self: Box<Self>) -> Option<String> {
        drop(self.file);
        if let Err(error) = remove_if_present(&self.partial) {
            warn!(%error, "could not remove partial destination archive");
        }
        None
    }
}

fn publish(partial: &Path, target: &Path, checksum: &str) -> Result<()> {
    fs::rename(partial, target)
        .with_context(|| format!("publish destination archive {}", target.display()))?;
    write_checksum(target, checksum)?;
    let directory = target
        .parent()
        .context("destination archive has no parent directory")?;
    File::open(directory)?.sync_all()?;
    Ok(())
}

#[derive(Clone, Debug)]
pub struct ArchiveInfo {
    pub name: String,
    pub path: PathBuf,
    pub checksum: Option<String>,
    pub size: u64,
    pub created: DateTime<Utc>,
}

const ARCHIVE_SUFFIX: &str = ".tar.lz4";

// Archive names are "<job>-<RFC3339 seconds>-<uuid>.tar.lz4". Both trailing parts have a fixed
// width, so the job name is whatever is left after removing them, even when it contains a dash.
const UUID_LEN: usize = 36;
const TIMESTAMP_LEN: usize = 20;

pub fn deliver_local(artifact: &Artifact, destination: &Path, job: &BackupJob) -> Result<()> {
    fs::create_dir_all(destination)
        .with_context(|| format!("create destination {}", destination.display()))?;
    warn_if_high(destination, "destination")?;
    sweep_stale_partials(destination);
    let target = destination.join(&artifact.name);
    if target.exists() {
        match verify_checksum(&target, &artifact.checksum) {
            Ok(()) => {
                ensure_checksum_file(&target, &artifact.checksum)?;
                prune_best_effort(destination, job);
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
    publish(&partial, &target, &artifact.checksum)?;
    info!(
        job = job.name,
        destination = %destination.display(),
        archive = artifact.name,
        "delivered archive"
    );
    prune_best_effort(destination, job);
    Ok(())
}

fn prune_best_effort(destination: &Path, job: &BackupJob) {
    if let Err(error) = apply_retention(destination, job) {
        warn!(
            job = job.name,
            destination = %destination.display(),
            %error,
            "cleanup after delivery failed; the archive was still delivered"
        );
    }
}

pub fn list_local(destination: &Path, job: &str) -> Result<Vec<ArchiveInfo>> {
    let mut archives = scan_archives(destination, job)?;
    for archive in &mut archives {
        archive.checksum = read_archive_checksum(&archive.path);
    }
    Ok(archives)
}

fn scan_archives(destination: &Path, job: &str) -> Result<Vec<ArchiveInfo>> {
    if !destination.exists() {
        return Ok(Vec::new());
    }
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
        let Some(parsed) = parse_archive_name(&name) else {
            continue;
        };
        if parsed.job != job {
            continue;
        }
        let created = parsed.created;
        archives.push(ArchiveInfo {
            name,
            path: entry.path(),
            checksum: None,
            size: metadata.len(),
            created,
        });
    }
    archives.sort_by(|left, right| {
        right
            .created
            .cmp(&left.created)
            .then_with(|| right.name.cmp(&left.name))
    });
    Ok(archives)
}

pub(crate) struct ParsedArchive<'name> {
    pub job: &'name str,
    pub created: DateTime<Utc>,
}

pub(crate) fn parse_archive_name(name: &str) -> Option<ParsedArchive<'_>> {
    let rest = name.strip_suffix(ARCHIVE_SUFFIX)?;
    let rest = rest.get(..rest.len().checked_sub(UUID_LEN + 1)?)?;
    let split = rest.len().checked_sub(TIMESTAMP_LEN + 1)?;
    let (job, stamp) = rest.split_at(split);
    let created = DateTime::parse_from_rfc3339(stamp.strip_prefix('-')?)
        .ok()?
        .with_timezone(&Utc);
    Some(ParsedArchive { job, created })
}

fn read_archive_checksum(archive: &Path) -> Option<String> {
    let path = checksum_path(archive);
    if !path.exists() {
        return None;
    }
    match read_checksum(&path) {
        Ok(checksum) => Some(checksum),
        Err(error) => {
            warn!(path = %path.display(), %error, "could not read checksum file; treating archive as unchecked");
            None
        }
    }
}

pub fn apply_retention(destination: &Path, job: &BackupJob) -> Result<()> {
    let Some(retention) = &job.retention else {
        return Ok(());
    };
    let archives = scan_archives(destination, &job.name)?;
    let removals = retention_removals(&archives, retention, Utc::now())?;
    for archive in removals {
        let checksum_file = checksum_path(&archive.path);
        fs::remove_file(&archive.path)
            .with_context(|| format!("remove retained archive {}", archive.path.display()))?;
        remove_if_present(&checksum_file)?;
        info!(
            job = job.name,
            archive = archive.name,
            destination = %destination.display(),
            "removed archive due to retention"
        );
    }
    Ok(())
}

// Milestone keepers, one archive per age bucket, are exempt from the
// configured rule, so the rule only ever counts and removes the rest.
fn retention_removals<'archive>(
    archives: &'archive [ArchiveInfo],
    retention: &RetentionConfig,
    now: DateTime<Utc>,
) -> Result<Vec<&'archive ArchiveInfo>> {
    let created: Vec<_> = archives.iter().map(|archive| archive.created).collect();
    let keepers = milestone_keepers(&created, now);
    let candidates = archives
        .iter()
        .enumerate()
        .filter(|(index, _)| !keepers.contains(index))
        .map(|(_, archive)| archive);
    if let Some(count) = retention.count {
        return Ok(candidates.skip(count).collect());
    }
    let Some(age) = retention.age_duration()? else {
        return Ok(Vec::new());
    };
    let age = Duration::from_std(age).context("retention age is too large")?;
    let cutoff = now - age;
    Ok(candidates
        .filter(|archive| archive.created < cutoff)
        .collect())
}

fn ensure_checksum_file(archive: &Path, checksum: &str) -> Result<()> {
    let checksum_file = checksum_path(archive);
    if checksum_file.exists() {
        let existing = read_checksum(&checksum_file)?;
        if existing != checksum {
            warn!(
                path = %checksum_file.display(),
                "repairing checksum file that disagrees with verified archive"
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

pub(crate) fn belongs_to_job(name: &str, job: &str) -> bool {
    parse_archive_name(name).is_some_and(|parsed| parsed.job == job)
}

const STALE_PARTIAL_AGE: StdDuration = StdDuration::from_secs(24 * 60 * 60);

// A crashed run or a killed SSH connection leaves partial files behind, and a
// retried archive gets a new name, so nothing else ever removes them. An active
// partial keeps a fresh modification time while bytes land, so the age guard
// never removes a transfer that is still running.
pub(crate) fn sweep_stale_partials(directory: &Path) {
    let entries = match fs::read_dir(directory) {
        Ok(entries) => entries,
        Err(error) => {
            warn!(directory = %directory.display(), %error, "could not scan for stale partial files");
            return;
        }
    };
    let now = SystemTime::now();
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
        let stale = entry
            .metadata()
            .and_then(|metadata| Ok((metadata.is_file(), metadata.modified()?)))
            .map(|(is_file, modified)| {
                is_file && now.duration_since(modified).unwrap_or_default() >= STALE_PARTIAL_AGE
            });
        match stale {
            Ok(false) => {}
            Ok(true) => match fs::remove_file(&path) {
                Ok(()) => info!(path = %path.display(), "removed stale partial file"),
                Err(error) => {
                    warn!(path = %path.display(), %error, "could not remove stale partial file");
                }
            },
            Err(error) => {
                warn!(path = %path.display(), %error, "could not read the age of a partial file");
            }
        }
    }
}

fn is_temporary_name(name: &str) -> bool {
    (name.starts_with('.') && name.ends_with(".partial"))
        || name.starts_with(".restore-")
        || name.starts_with(".backup-write-test-")
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
    use std::cmp::Reverse;
    use std::fs;
    use std::path::PathBuf;
    use std::time::{Duration as StdDuration, SystemTime};

    use chrono::{DateTime, Duration, Utc};
    use tempfile::tempdir;

    use super::{
        ArchiveInfo, apply_retention, checksum_path, deliver_local, list_local, parse_archive_name,
        retention_removals, sweep_stale_partials,
    };
    use crate::archive::{Artifact, checksum_file, read_checksum};
    use crate::config::{BackupJob, RetentionConfig};
    use crate::location::Location;

    fn archive_name(hour: u32, tag: char) -> String {
        let group = |count: usize| std::iter::repeat_n(tag, count).collect::<String>();
        let uuid = format!(
            "{}-{}-{}-{}-{}",
            group(8),
            group(4),
            group(4),
            group(4),
            group(12)
        );
        format!("job-2026-07-17T{hour:02}:00:00Z-{uuid}.tar.lz4")
    }

    #[test]
    fn only_stale_temporary_files_are_swept() {
        let temporary = tempdir().unwrap();
        let directory = temporary.path();
        let stale_partial = directory.join(".doc.tar.lz4.partial");
        let stale_restore = directory.join(".restore-x-doc.tar.lz4");
        let stale_probe = directory.join(".backup-write-test-x");
        let fresh_partial = directory.join(".new.tar.lz4.partial");
        let archive = directory.join(archive_name(1, 'a'));
        for path in [
            &stale_partial,
            &stale_restore,
            &stale_probe,
            &fresh_partial,
            &archive,
        ] {
            fs::write(path, "x").unwrap();
        }
        let old = SystemTime::now() - StdDuration::from_secs(2 * 24 * 60 * 60);
        for path in [&stale_partial, &stale_restore, &stale_probe, &archive] {
            fs::File::open(path).unwrap().set_modified(old).unwrap();
        }

        sweep_stale_partials(directory);

        assert!(!stale_partial.exists());
        assert!(!stale_restore.exists());
        assert!(!stale_probe.exists());
        assert!(fresh_partial.exists(), "a fresh partial was removed");
        assert!(archive.exists(), "a real archive was removed");
    }

    #[test]
    fn missing_checksum_on_a_sibling_does_not_break_delivery_cleanup_or_list() {
        let temporary = tempdir().unwrap();
        let staging = temporary.path().join("staging");
        let destination = temporary.path().join("destination");
        fs::create_dir_all(&staging).unwrap();
        fs::create_dir_all(&destination).unwrap();

        let orphan = archive_name(1, '0');
        fs::write(destination.join(&orphan), "old archive").unwrap();

        let fresh = archive_name(2, '1');
        let archive_path = staging.join(&fresh);
        fs::write(&archive_path, "new archive").unwrap();
        let checksum = checksum_file(&archive_path).unwrap();
        let artifact = Artifact {
            name: fresh.clone(),
            path: archive_path,
            checksum_path: PathBuf::new(),
            checksum,
            size: 11,
            created_at: Utc::now(),
        };
        let job = BackupJob {
            name: "job".to_owned(),
            source: Location::Local(PathBuf::from("/source")),
            destinations: vec![Location::Local(destination.clone())],
            cron: "0 0 * * *".to_owned(),
            retention: Some(RetentionConfig {
                count: Some(5),
                age: None,
            }),
            exclude: Vec::new(),
        };

        deliver_local(&artifact, &destination, &job).unwrap();
        assert!(destination.join(&fresh).exists());

        let archives = list_local(&destination, "job").unwrap();
        assert_eq!(archives.len(), 2);
        assert!(
            archives
                .iter()
                .find(|archive| archive.name == orphan)
                .unwrap()
                .checksum
                .is_none()
        );
        assert!(
            archives
                .iter()
                .find(|archive| archive.name == fresh)
                .unwrap()
                .checksum
                .is_some()
        );
    }

    #[test]
    fn newest_is_chosen_by_name_timestamp_not_file_time() {
        let temporary = tempdir().unwrap();
        let newer_name = archive_name(5, 'a');
        let older_name = archive_name(1, 'b');
        fs::write(temporary.path().join(&newer_name), "a").unwrap();
        fs::write(temporary.path().join(&older_name), "b").unwrap();

        // Give the older-named archive the newest file time, the exact case retention used to
        // misread. Ordering must still follow the timestamp in the name, not the file time.
        fs::File::open(temporary.path().join(&older_name))
            .unwrap()
            .set_modified(SystemTime::now())
            .unwrap();
        fs::File::open(temporary.path().join(&newer_name))
            .unwrap()
            .set_modified(SystemTime::now() - StdDuration::from_secs(3600))
            .unwrap();

        let archives = list_local(temporary.path(), "job").unwrap();
        assert_eq!(archives[0].name, newer_name);
        assert_eq!(archives[1].name, older_name);
    }

    #[test]
    fn retention_leaves_a_job_whose_name_starts_with_this_one_alone() {
        let temporary = tempdir().unwrap();
        let shared = temporary.path();
        let mine = "docs-2026-07-17T01:00:00Z-00000000-0000-0000-0000-000000000000.tar.lz4";
        let theirs =
            "docs-archive-2026-07-17T02:00:00Z-11111111-1111-1111-1111-111111111111.tar.lz4";
        fs::write(shared.join(mine), "mine").unwrap();
        fs::write(shared.join(theirs), "theirs").unwrap();
        let job = BackupJob {
            name: "docs".to_owned(),
            source: Location::Local(PathBuf::from("/source")),
            destinations: vec![Location::Local(shared.to_path_buf())],
            cron: "0 0 * * *".to_owned(),
            retention: Some(RetentionConfig {
                count: Some(1),
                age: None,
            }),
            exclude: Vec::new(),
        };

        assert_eq!(list_local(shared, "docs").unwrap().len(), 1);
        apply_retention(shared, &job).unwrap();

        assert!(shared.join(mine).exists());
        assert!(shared.join(theirs).exists());
    }

    #[test]
    fn a_job_name_containing_dashes_still_parses() {
        let name = "my-nice-job-2026-07-17T02:00:00Z-11111111-1111-1111-1111-111111111111.tar.lz4";
        let parsed = parse_archive_name(name).unwrap();
        assert_eq!(parsed.job, "my-nice-job");
        assert_eq!(parsed.created.to_rfc3339(), "2026-07-17T02:00:00+00:00");
        assert!(parse_archive_name("not-an-archive.txt").is_none());
    }

    #[test]
    fn count_retention_keeps_newest_archives() {
        let directory = tempdir().unwrap();
        let archives: Vec<_> = (0..4)
            .map(|offset| ArchiveInfo {
                name: format!("job-{offset}.tar.lz4"),
                path: directory.path().join(format!("job-{offset}.tar.lz4")),
                checksum: None,
                size: 1,
                created: Utc::now() - Duration::minutes(offset),
            })
            .collect();
        let retention = RetentionConfig {
            count: Some(2),
            age: None,
        };
        let removals = retention_removals(&archives, &retention, Utc::now()).unwrap();
        assert_eq!(removals.len(), 2);
        assert_eq!(removals[0].name, "job-2.tar.lz4");
    }

    fn aged_archive(days: i64, now: DateTime<Utc>) -> ArchiveInfo {
        ArchiveInfo {
            name: format!("job-{days}.tar.lz4"),
            path: PathBuf::from(format!("job-{days}.tar.lz4")),
            checksum: None,
            size: 1,
            created: now - Duration::days(days),
        }
    }

    #[test]
    fn count_retention_spares_one_keeper_per_age_bucket() {
        let now = Utc::now();
        let mut archives: Vec<_> = [0, 1, 2, 8, 10, 20, 40, 100, 400, 800]
            .into_iter()
            .map(|days| aged_archive(days, now))
            .collect();
        archives.sort_by_key(|archive| Reverse(archive.created));
        let retention = RetentionConfig {
            count: Some(2),
            age: None,
        };

        let removals = retention_removals(&archives, &retention, now).unwrap();

        // 0 and 1 survive as the newest two. 10, 20, 40, 100, 400, and 800
        // are the oldest archives of their buckets. Only 2 and 8 go.
        let mut removed: Vec<_> = removals
            .iter()
            .map(|archive| archive.name.clone())
            .collect();
        removed.sort();
        assert_eq!(removed, ["job-2.tar.lz4", "job-8.tar.lz4"]);
    }

    #[test]
    fn age_retention_spares_one_keeper_per_age_bucket() {
        let now = Utc::now();
        let mut archives: Vec<_> = [0, 8, 10, 400]
            .into_iter()
            .map(|days| aged_archive(days, now))
            .collect();
        archives.sort_by_key(|archive| Reverse(archive.created));
        let retention = RetentionConfig {
            count: None,
            age: Some("5d".to_owned()),
        };

        let removals = retention_removals(&archives, &retention, now).unwrap();

        assert_eq!(removals.len(), 1);
        assert_eq!(removals[0].name, "job-8.tar.lz4");
    }

    #[test]
    fn delivery_replaces_a_corrupt_copy_and_repairs_its_checksum_file() {
        let temporary = tempdir().unwrap();
        let staging = temporary.path().join("staging");
        let destination = temporary.path().join("destination");
        fs::create_dir_all(&staging).unwrap();
        fs::create_dir_all(&destination).unwrap();
        let archive_path = staging.join("job-archive.tar.lz4");
        fs::write(&archive_path, "healthy archive").unwrap();
        let checksum = checksum_file(&archive_path).unwrap();
        let artifact = Artifact {
            name: "job-archive.tar.lz4".to_owned(),
            path: archive_path,
            checksum_path: PathBuf::new(),
            checksum: checksum.clone(),
            size: 15,
            created_at: Utc::now(),
        };
        let target = destination.join(&artifact.name);
        fs::write(&target, "corrupt").unwrap();
        fs::write(checksum_path(&target), "wrong  job-archive.tar.lz4\n").unwrap();
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
