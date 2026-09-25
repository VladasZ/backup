use std::collections::HashSet;
use std::fs::{self, OpenOptions};
use std::io::{self, BufRead, Read, Write, copy};
use std::path::Path;

use anyhow::{Context, Result, bail};
use chrono::{DateTime, Utc};
use lz4_flex::frame::FrameEncoder;
use serde::Serialize;
use tracing::warn;
use uuid::Uuid;

use crate::archive::{create_private, ensure_not_symlink, parse_archive_name, restore_stream};
use crate::config::{BackupJob, Config};
use crate::destination::{self, verify_archive};
use crate::location::{Location, SshLocation};
use crate::output::{Event, emit};
use crate::repair::{destinations_with_peers, open_sources, verify_destination};
use crate::ssh::{self, validate_agent, validate_destination, validate_source};
use crate::store::files::sync_directory;
use crate::store::recipe::{Recipe, RecipeInfo};
use crate::stream::{RecipeStream, SourceRef};

#[derive(Clone, Debug)]
struct LocatedArchive {
    destination: Location,
    archive: RecipeInfo,
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
    // Every backup now carries its checksum inside its sealed recipe. The
    // field stays for readers of the JSON listing that still expect it.
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
            name: located.archive.name,
            size: located.archive.size,
            created: located.archive.created,
            checksum_missing: false,
        }));
    }
    Ok(entries)
}

/// The recipe and the destinations to read it from, local ones first.
fn open_archive(job: &BackupJob, archive_name: &str) -> Result<(Recipe, Vec<Location>)> {
    let candidates = select_archives(job, archive_name)?;
    let mut failures = Vec::new();
    for located in &candidates {
        match destination::read_recipe(&located.destination, &located.archive.name) {
            Ok(recipe) => {
                let order = candidates
                    .iter()
                    .map(|candidate| candidate.destination.clone())
                    .collect();
                return Ok((recipe, order));
            }
            Err(error) => {
                warn!(destination = %located.destination, error = %format!("{error:#}"), "could not read the recipe");
                emit(&Event::RestoreCopyRejected {
                    destination: located.destination.to_string(),
                    error: format!("{error:#}"),
                });
                failures.push(format!("{}: {error:#}", located.destination));
            }
        }
    }
    bail!(
        "every copy of archive {archive_name:?} failed: {}",
        failures.join("; ")
    )
}

fn with_stream<T>(
    job: &BackupJob,
    archive_name: &str,
    operation: impl FnOnce(&Recipe, &mut dyn Read) -> Result<T>,
) -> Result<T> {
    let (recipe, order) = open_archive(job, archive_name)?;
    let ids: Vec<_> = recipe.chunks.iter().map(|chunk| chunk.id).collect();
    let mut sources = open_sources(&order, &ids)?;
    let mut sources: Vec<SourceRef<'_>> =
        sources.iter_mut().map(|source| source.as_mut()).collect();
    let mut stream = RecipeStream::new(&mut sources, &recipe);
    operation(&recipe, &mut stream)
}

pub fn restore(
    job: &BackupJob,
    archive_name: &str,
    target: &Location,
    yes: bool,
) -> Result<Option<RestoreReport>> {
    select_archives(job, archive_name)?;
    if !yes && !confirm_restore(target)? {
        println!("restore cancelled");
        return Ok(None);
    }
    let name = with_stream(job, archive_name, |recipe, stream| {
        match target {
            Location::Local(path) => restore_stream(stream, path)?,
            Location::Ssh(remote) => ssh::restore(stream, remote)?,
        }
        Ok(recipe.name.clone())
    })?;
    emit(&Event::Restored {
        archive: name.clone(),
        target: target.to_string(),
    });
    Ok(Some(RestoreReport {
        archive: name,
        target: target.to_string(),
    }))
}

#[derive(Debug, Serialize)]
pub struct ExportReport {
    pub archive: String,
    pub file: String,
    pub size: u64,
}

/// Writes one backup as a plain `.tar.lz4` that `lz4` and `tar` open without
/// this tool.
pub fn export(job: &BackupJob, archive_name: &str, file: &Path) -> Result<ExportReport> {
    let directory = file
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .unwrap_or(Path::new("."));
    let name = file.file_name().context("export target has no file name")?;
    let partial = directory.join(format!(
        ".{}.{}.partial",
        name.to_string_lossy(),
        Uuid::new_v4()
    ));
    let written = with_stream(job, archive_name, |recipe, stream| {
        let mut encoder = FrameEncoder::new(create_private(&partial)?);
        copy(stream, &mut encoder).context("write the export")?;
        let output = encoder.finish().context("finish the LZ4 frame")?;
        output.sync_all()?;
        Ok(recipe.name.clone())
    });
    let archive = match written {
        Ok(archive) => archive,
        Err(error) => {
            if let Err(cleanup) = fs::remove_file(&partial) {
                warn!(path = %partial.display(), %cleanup, "could not remove the partial export");
            }
            return Err(error);
        }
    };
    fs::rename(&partial, file).with_context(|| format!("publish {}", file.display()))?;
    sync_directory(directory)?;
    Ok(ExportReport {
        archive,
        file: file.display().to_string(),
        size: fs::metadata(file)?.len(),
    })
}

/// Reads every destination of the selected jobs in full and repairs damage
/// from the other destinations. With an archive name it also rebuilds that
/// backup from each destination alone and reads it as a tar.
pub fn verify(
    config: &Config,
    job_name: Option<&str>,
    archive_name: Option<&str>,
) -> Result<usize> {
    let jobs: Vec<_> = match job_name {
        Some(name) => vec![config.job(name)?],
        None => config.jobs.iter().collect(),
    };
    let peers = destinations_with_peers(config);
    let mut verified = 0usize;
    let mut failures = Vec::new();
    for job in jobs {
        let wanted = match archive_name {
            Some("latest") => Some(
                list_archives(job)?
                    .first()
                    .map(|located| located.archive.name.clone())
                    .with_context(|| format!("no archives found for job {:?}", job.name))?,
            ),
            Some(name) => Some(name.to_owned()),
            None => None,
        };
        for destination in &job.destinations {
            let others = peers
                .iter()
                .find(|(location, _)| location == destination)
                .map(|(_, others)| others.clone())
                .unwrap_or_default();
            let outcome = match verify_destination(destination, &others) {
                Ok(outcome) => outcome,
                Err(error) => {
                    failures.push(format!("{destination}: {error:#}"));
                    continue;
                }
            };
            failures.extend(
                outcome
                    .problems
                    .iter()
                    .map(|problem| format!("{destination}: {problem}")),
            );
            let mut intact: Vec<&String> = outcome
                .check
                .intact
                .iter()
                .filter(|name| {
                    parse_archive_name(name).is_some_and(|parsed| parsed.job == job.name)
                })
                .collect();
            if let Some(wanted) = &wanted {
                intact.retain(|name| *name == wanted);
                if intact.is_empty() {
                    failures.push(format!("{destination}: {wanted} is missing or damaged"));
                    continue;
                }
                if let Err(error) = verify_archive(destination, wanted) {
                    emit(&Event::VerifyFailed {
                        archive: wanted.clone(),
                        destination: destination.to_string(),
                        error: format!("{error:#}"),
                    });
                    failures.push(format!("{wanted} at {destination}: {error:#}"));
                    continue;
                }
            }
            for name in intact {
                emit(&Event::Verified {
                    archive: name.clone(),
                    destination: destination.to_string(),
                });
                verified += 1;
            }
        }
    }
    if !failures.is_empty() {
        bail!(
            "{} problem(s) found: {}",
            failures.len(),
            failures.join("; ")
        );
    }
    if verified == 0 {
        bail!("no matching archives found");
    }
    Ok(verified)
}

pub fn prune(config: &Config, job_name: Option<&str>) -> Result<Vec<PruneEntry>> {
    let jobs: Vec<_> = match job_name {
        Some(name) => vec![config.job(name)?],
        None => config.jobs.iter().collect(),
    };
    let mut pruned = Vec::new();
    for job in jobs {
        for destination in &job.destinations {
            destination::prune(destination, job)?;
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
    Ok(())
}

fn validate_local_destination(destination: &Path) -> Result<()> {
    fs::create_dir_all(destination)
        .with_context(|| format!("create destination {}", destination.display()))?;
    if !fs::metadata(destination)?.is_dir() {
        bail!("destination {} is not a directory", destination.display());
    }
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
        match destination::list(destination, Some(&job.name)) {
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

#[cfg(test)]
mod tests {
    use std::fs::{self, File};
    use std::path::Path;

    use lz4_flex::frame::FrameDecoder;
    use tar::Archive;
    use tempfile::tempdir;

    use super::{export, restore, verify};
    use crate::config::{BackupJob, Config};
    use crate::location::Location;
    use crate::paths::AppPaths;
    use crate::runner::Runner;
    use crate::store::index::pack_files;

    fn setup(root: &Path) -> (Config, AppPaths) {
        let source = root.join("source");
        fs::create_dir_all(&source).unwrap();
        let data: Vec<u8> = (0..3_000_000u32)
            .map(|value| (value.wrapping_mul(2_654_435_761) >> 11) as u8)
            .collect();
        fs::write(source.join("data.bin"), data).unwrap();
        let state = root.join("state");
        fs::create_dir_all(&state).unwrap();
        let paths = AppPaths {
            config: root.join("config.toml"),
            database: state.join("state.redb"),
            daemon_lock: state.join("daemon.lock"),
            operation_lock: state.join("operation.lock"),
            log_file: state.join("logs/backup.log"),
            log_directory: state.join("logs"),
            state,
        };
        let job = BackupJob {
            name: "documents".to_owned(),
            source: Location::Local(source),
            destinations: vec![
                Location::Local(root.join("first")),
                Location::Local(root.join("second")),
            ],
            cron: "0 2 * * *".to_owned(),
            retention: None,
            pre: None,
            exclude: Vec::new(),
        };
        (Config { jobs: vec![job] }, paths)
    }

    fn wreck_packs(root: &Path) {
        for (_, path) in pack_files(root).unwrap() {
            let mut bytes = fs::read(&path).unwrap();
            let step = bytes.len() / 8;
            for index in 1..8 {
                bytes[index * step] ^= 0xff;
            }
            fs::write(&path, bytes).unwrap();
        }
    }

    #[test]
    fn restore_takes_a_chunk_from_another_destination_when_one_copy_is_damaged() {
        let temporary = tempdir().unwrap();
        let (config, paths) = setup(temporary.path());
        let mut runner = Runner::new(config.clone(), paths).unwrap();
        runner.run_named("documents").unwrap();
        wreck_packs(&temporary.path().join("first"));

        let target = temporary.path().join("restored");
        restore(
            &config.jobs[0],
            "latest",
            &Location::Local(target.clone()),
            true,
        )
        .unwrap();

        assert_eq!(
            fs::read(target.join("data.bin")).unwrap(),
            fs::read(temporary.path().join("source/data.bin")).unwrap()
        );
    }

    #[test]
    fn verify_repairs_a_damaged_destination_from_the_other() {
        let temporary = tempdir().unwrap();
        let (config, paths) = setup(temporary.path());
        let mut runner = Runner::new(config.clone(), paths).unwrap();
        runner.run_named("documents").unwrap();
        wreck_packs(&temporary.path().join("first"));

        assert_eq!(verify(&config, Some("documents"), None).unwrap(), 2);
        // A second pass finds nothing left to repair.
        assert_eq!(
            verify(&config, Some("documents"), Some("latest")).unwrap(),
            2
        );
    }

    #[test]
    fn export_writes_a_plain_tar_lz4() {
        let temporary = tempdir().unwrap();
        let (config, paths) = setup(temporary.path());
        let mut runner = Runner::new(config.clone(), paths).unwrap();
        runner.run_named("documents").unwrap();

        let file = temporary.path().join("export.tar.lz4");
        let report = export(&config.jobs[0], "latest", &file).unwrap();
        assert!(report.archive.starts_with("documents-"));

        let target = temporary.path().join("unpacked");
        Archive::new(FrameDecoder::new(File::open(&file).unwrap()))
            .unpack(&target)
            .unwrap();
        assert_eq!(
            fs::read(target.join("data.bin")).unwrap(),
            fs::read(temporary.path().join("source/data.bin")).unwrap()
        );
    }
}
