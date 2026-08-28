use std::collections::HashSet;
use std::fs::{self, OpenOptions};
use std::io::{self, BufRead, Write};
use std::path::{Path, PathBuf};

use anyhow::{Context, Result, bail};
use chrono::{DateTime, Utc};
use serde::Serialize;
use tracing::warn;
use uuid::Uuid;

use crate::archive::{
    Artifact, checksum_file, ensure_not_symlink, restore_archive, verify_archive, verify_checksum,
    write_checksum,
};
use crate::config::{BackupJob, Config};
use crate::destination::{ArchiveInfo, apply_retention, list_local};
use crate::location::{Location, SshLocation};
use crate::output::{Event, emit};
use crate::paths::AppPaths;
use crate::ssh::{
    fetch_remote, list_remote, prune_remote, restore_remote, validate_agent, validate_destination,
    validate_source, verify_remote,
};
use crate::storage::warn_if_high;

#[derive(Clone, Debug)]
struct LocatedArchive {
    destination: Location,
    archive: ArchiveInfo,
}

type RemoteIdentity = (String, Option<String>, Option<u16>);

pub fn validate(config: &Config) -> Result<()> {
    let mut agents = HashSet::new();
    for job in &config.jobs {
        match &job.source {
            Location::Local(source) => validate_local_source(source)?,
            Location::Ssh(remote) => {
                validate_remote_once(remote, &mut agents)?;
                validate_source(remote)?;
            }
        }
        for destination in &job.destinations {
            match destination {
                Location::Local(path) => validate_local_destination(path)?,
                Location::Ssh(remote) => {
                    validate_remote_once(remote, &mut agents)?;
                    validate_destination(remote)?;
                }
            }
        }
    }
    Ok(())
}

#[derive(Debug, Serialize)]
pub struct ArchiveEntry {
    pub job: String,
    pub destination: String,
    pub name: String,
    pub size: u64,
    pub created: DateTime<Utc>,
    pub checksum_missing: bool,
}

#[derive(Debug, Serialize)]
pub struct RestoreReport {
    pub archive: String,
    pub target: String,
}

#[derive(Debug, Serialize)]
pub struct PruneEntry {
    pub job: String,
    pub destination: String,
}

pub fn list(config: &Config, job_name: Option<&str>) -> Result<Vec<ArchiveEntry>> {
    let jobs: Vec<_> = match job_name {
        Some(name) => vec![config.job(name)?],
        None => config.jobs.iter().collect(),
    };
    let mut entries = Vec::new();
    for job in jobs {
        entries.extend(list_archives(job)?.into_iter().map(|located| ArchiveEntry {
            job: job.name.clone(),
            destination: located.destination.to_string(),
            checksum_missing: located.archive.checksum.is_none(),
            name: located.archive.name,
            size: located.archive.size,
            created: located.archive.created,
        }));
    }
    Ok(entries)
}

pub fn restore(
    job: &BackupJob,
    archive_name: &str,
    target: &Location,
    yes: bool,
    paths: &AppPaths,
) -> Result<Option<RestoreReport>> {
    let candidates = select_archives(job, archive_name)?;
    if !yes && !confirm_restore(target)? {
        println!("restore cancelled");
        return Ok(None);
    }
    let mut failures = Vec::new();
    for located in candidates {
        let checksum = match resolve_checksum(&located, yes)? {
            Some(checksum) => checksum,
            None => {
                emit(&Event::RestoreCopyRejected {
                    destination: located.destination.to_string(),
                    error: "checksum file missing".to_owned(),
                });
                failures.push(format!("{}: checksum file missing", located.destination));
                continue;
            }
        };
        let (artifact, temporary) = match materialize(&located, &checksum, job, paths) {
            Ok(materialized) => materialized,
            Err(error) => {
                warn!(
                    destination = %located.destination,
                    %error,
                    "could not read archive copy"
                );
                emit(&Event::RestoreCopyRejected {
                    destination: located.destination.to_string(),
                    error: format!("{error:#}"),
                });
                failures.push(format!("{}: {error:#}", located.destination));
                continue;
            }
        };
        let verification = verify_checksum(&artifact.path, &checksum)
            .and_then(|()| verify_archive(&artifact.path));
        if let Err(error) = verification {
            if temporary {
                remove_temporary(&artifact);
            }
            warn!(
                destination = %located.destination,
                %error,
                "archive copy failed verification"
            );
            emit(&Event::RestoreCopyRejected {
                destination: located.destination.to_string(),
                error: format!("{error:#}"),
            });
            failures.push(format!("{}: {error:#}", located.destination));
            continue;
        }
        let result = match target {
            Location::Local(path) => restore_archive(&artifact.path, path),
            Location::Ssh(remote) => restore_remote(&artifact, remote),
        };
        if temporary {
            remove_temporary(&artifact);
        }
        result?;
        emit(&Event::Restored {
            archive: located.archive.name.clone(),
            target: target.to_string(),
        });
        return Ok(Some(RestoreReport {
            archive: located.archive.name,
            target: target.to_string(),
        }));
    }
    bail!(
        "every copy of archive {archive_name:?} failed: {}",
        failures.join("; ")
    )
}

pub fn verify(
    config: &Config,
    job_name: Option<&str>,
    archive_name: Option<&str>,
) -> Result<usize> {
    let jobs: Vec<_> = match job_name {
        Some(name) => vec![config.job(name)?],
        None => config.jobs.iter().collect(),
    };
    let mut verified = 0usize;
    let mut failures = Vec::new();
    for job in jobs {
        let archives = list_archives(job)?;
        // "latest" resolves per job, since the archives are listed newest first.
        let wanted = match archive_name {
            Some("latest") => archives.first().map(|located| located.archive.name.clone()),
            Some(name) => Some(name.to_owned()),
            None => None,
        };
        for located in archives {
            if wanted
                .as_deref()
                .is_some_and(|name| name != located.archive.name)
            {
                continue;
            }
            match verify_one(&located) {
                Ok(()) => {
                    emit(&Event::Verified {
                        archive: located.archive.name.clone(),
                        destination: located.destination.to_string(),
                    });
                    verified += 1;
                }
                Err(error) => {
                    warn!(
                        archive = located.archive.name,
                        destination = %located.destination,
                        %error,
                        "archive verification failed"
                    );
                    emit(&Event::VerifyFailed {
                        archive: located.archive.name.clone(),
                        destination: located.destination.to_string(),
                        error: format!("{error:#}"),
                    });
                    failures.push(format!(
                        "{} at {}: {error:#}",
                        located.archive.name, located.destination
                    ));
                }
            }
        }
    }
    if !failures.is_empty() {
        bail!(
            "{} archive(s) failed verification: {}",
            failures.len(),
            failures.join("; ")
        );
    }
    if verified == 0 {
        bail!("no matching archives found");
    }
    Ok(verified)
}

fn verify_one(located: &LocatedArchive) -> Result<()> {
    let checksum = located
        .archive
        .checksum
        .as_deref()
        .context("checksum file is missing")?;
    match &located.destination {
        Location::Local(_) => {
            verify_checksum(&located.archive.path, checksum)?;
            verify_archive(&located.archive.path)
        }
        Location::Ssh(remote) => verify_remote(remote, &located.archive.path, checksum),
    }
}

pub fn prune(config: &Config, job_name: Option<&str>) -> Result<Vec<PruneEntry>> {
    let jobs: Vec<_> = match job_name {
        Some(name) => vec![config.job(name)?],
        None => config.jobs.iter().collect(),
    };
    let mut pruned = Vec::new();
    for job in jobs {
        for destination in &job.destinations {
            match destination {
                Location::Local(path) => apply_retention(path, job)?,
                Location::Ssh(remote) => prune_remote(remote, job)?,
            }
            pruned.push(PruneEntry {
                job: job.name.clone(),
                destination: destination.to_string(),
            });
        }
    }
    Ok(pruned)
}

fn validate_local_source(source: &Path) -> Result<()> {
    ensure_not_symlink(source)?;
    let metadata =
        fs::metadata(source).with_context(|| format!("read source {}", source.display()))?;
    if !metadata.is_dir() && !metadata.is_file() {
        bail!(
            "source {} is not a regular file or directory",
            source.display()
        );
    }
    warn_if_high(source, "source")?;
    Ok(())
}

fn validate_local_destination(destination: &Path) -> Result<()> {
    fs::create_dir_all(destination)
        .with_context(|| format!("create destination {}", destination.display()))?;
    if !fs::metadata(destination)?.is_dir() {
        bail!("destination {} is not a directory", destination.display());
    }
    warn_if_high(destination, "destination")?;
    let probe = destination.join(format!(".backup-write-test-{}", Uuid::new_v4()));
    let file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&probe)
        .with_context(|| format!("destination {} is not writable", destination.display()))?;
    file.sync_all()?;
    fs::remove_file(&probe).with_context(|| format!("remove write probe {}", probe.display()))
}

fn validate_remote_once(
    remote: &SshLocation,
    validated: &mut HashSet<RemoteIdentity>,
) -> Result<()> {
    let identity = (remote.host.clone(), remote.user.clone(), remote.port);
    if validated.insert(identity) {
        validate_agent(remote)?;
    }
    Ok(())
}

fn list_archives(job: &BackupJob) -> Result<Vec<LocatedArchive>> {
    let mut located = Vec::new();
    let mut reachable = 0usize;
    let mut failures = Vec::new();
    for destination in &job.destinations {
        let result = match destination {
            Location::Local(path) => list_local(path, &job.name),
            Location::Ssh(remote) => list_remote(remote, &job.name),
        };
        match result {
            Ok(archives) => {
                reachable += 1;
                located.extend(archives.into_iter().map(|archive| LocatedArchive {
                    destination: destination.clone(),
                    archive,
                }));
            }
            Err(error) => {
                warn!(%destination, %error, "could not list backup destination");
                failures.push(format!("{destination}: {error:#}"));
            }
        }
    }
    if reachable == 0 {
        bail!("all destinations failed: {}", failures.join("; "));
    }
    located.sort_by(|left, right| {
        right
            .archive
            .created
            .cmp(&left.archive.created)
            .then_with(|| right.archive.name.cmp(&left.archive.name))
    });
    Ok(located)
}

fn select_archives(job: &BackupJob, name: &str) -> Result<Vec<LocatedArchive>> {
    let archives = list_archives(job)?;
    let selected_name = if name == "latest" {
        archives
            .first()
            .map(|located| located.archive.name.clone())
            .with_context(|| format!("no archives found for job {:?}", job.name))?
    } else {
        name.to_owned()
    };
    let mut selected = archives
        .into_iter()
        .filter(|located| located.archive.name == selected_name)
        .collect::<Vec<_>>();
    if selected.is_empty() {
        bail!("archive {name:?} was not found for job {:?}", job.name);
    }
    selected.sort_by_key(|located| !located.destination.is_local());
    Ok(selected)
}

fn resolve_checksum(located: &LocatedArchive, yes: bool) -> Result<Option<String>> {
    if let Some(checksum) = &located.archive.checksum {
        return Ok(Some(checksum.clone()));
    }
    match &located.destination {
        Location::Ssh(_) => {
            warn!(
                destination = %located.destination,
                archive = located.archive.name,
                "checksum file is missing on this remote copy; skipping it"
            );
            Ok(None)
        }
        Location::Local(_) => {
            if !yes && !confirm_missing_checksum(located)? {
                return Ok(None);
            }
            let checksum = checksum_file(&located.archive.path)?;
            Ok(Some(checksum))
        }
    }
}

fn confirm_missing_checksum(located: &LocatedArchive) -> Result<bool> {
    print!(
        "The checksum file for {} at {} is missing, so it cannot be checked against a stored fingerprint. The whole archive will still be read to confirm it is not truncated. Restore it anyway? [y/N] ",
        located.archive.name, located.destination
    );
    io::stdout().flush()?;
    let mut answer = String::new();
    io::stdin().lock().read_line(&mut answer)?;
    Ok(matches!(
        answer.trim().to_ascii_lowercase().as_str(),
        "y" | "yes"
    ))
}

fn materialize(
    located: &LocatedArchive,
    checksum: &str,
    job: &BackupJob,
    paths: &AppPaths,
) -> Result<(Artifact, bool)> {
    let Location::Ssh(remote) = &located.destination else {
        return Ok((
            Artifact {
                name: located.archive.name.clone(),
                path: located.archive.path.clone(),
                checksum_path: PathBuf::new(),
                checksum: checksum.to_owned(),
                size: located.archive.size,
                created_at: located.archive.created,
            },
            false,
        ));
    };
    let staging = paths.job_staging(&job.name);
    fs::create_dir_all(&staging)?;
    let path = staging.join(format!(
        ".restore-{}-{}",
        Uuid::new_v4(),
        located.archive.name
    ));
    fetch_remote(remote, &located.archive.path, checksum, &path)?;
    let checksum_path = write_checksum(&path, checksum)?;
    Ok((
        Artifact {
            name: located.archive.name.clone(),
            path,
            checksum_path,
            checksum: checksum.to_owned(),
            size: located.archive.size,
            created_at: located.archive.created,
        },
        true,
    ))
}

fn confirm_restore(target: &Location) -> Result<bool> {
    print!("Restore will overwrite existing files at {target}. Continue? [y/N] ");
    io::stdout().flush()?;
    let mut answer = String::new();
    io::stdin().lock().read_line(&mut answer)?;
    Ok(matches!(
        answer.trim().to_ascii_lowercase().as_str(),
        "y" | "yes"
    ))
}

fn remove_temporary(artifact: &Artifact) {
    for path in [&artifact.path, &artifact.checksum_path] {
        if let Err(error) = fs::remove_file(path) {
            warn!(path = %path.display(), %error, "failed to remove temporary restore file");
        }
    }
}

#[cfg(test)]
mod tests {
    use std::fs;

    use tempfile::tempdir;

    use super::{restore, verify};
    use crate::archive::create_local_archive;
    use crate::config::{BackupJob, Config};
    use crate::destination::deliver_local;
    use crate::location::Location;
    use crate::paths::AppPaths;

    #[test]
    fn verify_latest_checks_only_the_newest_archive() {
        let temporary = tempdir().unwrap();
        let source = temporary.path().join("source");
        let staging = temporary.path().join("staging");
        let destination = temporary.path().join("destination");
        fs::create_dir_all(&source).unwrap();
        fs::write(source.join("file.txt"), "content").unwrap();
        let job = BackupJob {
            name: "docs".to_owned(),
            source: Location::Local(source.clone()),
            destinations: vec![Location::Local(destination.clone())],
            cron: "0 2 * * *".to_owned(),
            retention: None,
            exclude: Vec::new(),
        };

        let mut old = create_local_archive(&job, &source, &staging).unwrap();
        let old_name = "docs-2026-01-01T00:00:00Z-00000000-0000-0000-0000-000000000000.tar.lz4";
        let old_path = staging.join(old_name);
        fs::rename(&old.path, &old_path).unwrap();
        old.name = old_name.to_owned();
        old.path = old_path;
        deliver_local(&old, &destination, &job).unwrap();

        let fresh = create_local_archive(&job, &source, &staging).unwrap();
        deliver_local(&fresh, &destination, &job).unwrap();

        fs::write(destination.join(old_name), "corrupt").unwrap();
        let config = Config { jobs: vec![job] };

        assert_eq!(verify(&config, Some("docs"), Some("latest")).unwrap(), 1);
        assert!(verify(&config, Some("docs"), None).is_err());
    }

    #[test]
    fn restore_falls_back_to_another_destination_when_a_copy_is_corrupt() {
        let temporary = tempdir().unwrap();
        let source = temporary.path().join("source");
        let staging = temporary.path().join("staging");
        let first = temporary.path().join("first");
        let second = temporary.path().join("second");
        let target = temporary.path().join("restored");
        fs::create_dir_all(&source).unwrap();
        fs::write(source.join("file.txt"), "healthy").unwrap();
        let job = BackupJob {
            name: "documents".to_owned(),
            source: Location::Local(source.clone()),
            destinations: vec![
                Location::Local(first.clone()),
                Location::Local(second.clone()),
            ],
            cron: "0 2 * * *".to_owned(),
            retention: None,
            exclude: Vec::new(),
        };
        let artifact = create_local_archive(&job, &source, &staging).unwrap();
        deliver_local(&artifact, &first, &job).unwrap();
        deliver_local(&artifact, &second, &job).unwrap();
        fs::write(first.join(&artifact.name), "corrupt").unwrap();
        let state = temporary.path().join("state");
        let paths = AppPaths {
            config: temporary.path().join("config.toml"),
            database: state.join("state.redb"),
            daemon_lock: state.join("daemon.lock"),
            operation_lock: state.join("operation.lock"),
            staging: state.join("staging"),
            log_file: state.join("logs/backup.log"),
            log_directory: state.join("logs"),
            state,
        };

        restore(
            &job,
            "latest",
            &Location::Local(target.clone()),
            true,
            &paths,
        )
        .unwrap();

        assert_eq!(
            fs::read_to_string(target.join("file.txt")).unwrap(),
            "healthy"
        );
    }
}
