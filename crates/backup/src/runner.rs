use std::fs;
use std::io::ErrorKind;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};

use anyhow::{Context, Result, bail};
use chrono::{Duration, Utc};
use serde::Serialize;
use tracing::{error, info, warn};

use crate::archive::{
    Artifact, SinkId, SourceScanner, StagingSink, StreamOutcome, Tee, archive_name, checksum_file,
    pump_local, read_checksum, verify_archive, verify_checksum, write_checksum,
};
use crate::config::{BackupJob, Config};
use crate::destination::{
    LocalSink, belongs_to_job, checksum_path, parse_archive_name, sweep_stale_partials,
};
use crate::location::Location;
use crate::lock::AppLock;
use crate::output::{Event, emit};
use crate::paths::AppPaths;
use crate::pre;
use crate::ssh::{RemoteStream, SshSink};
use crate::state::{DeliveryResult, DeliveryStatus, PendingDelivery, State};
use crate::transport::deliver;

pub(crate) const HISTORY_DAYS: i64 = 30;

#[derive(Clone, Debug, Serialize)]
pub struct RunReport {
    pub job: String,
    pub archive: String,
    pub size: u64,
    pub checksum: String,
    pub delivered: usize,
    pub failed: usize,
    pub staged: bool,
}

pub struct Runner {
    pub config: Config,
    pub paths: AppPaths,
    pub state: State,
}

enum Producer {
    Local(SourceScanner),
    Remote(RemoteStream),
}

impl Runner {
    pub fn new(config: Config, paths: AppPaths) -> Result<Self> {
        paths.ensure()?;
        let state = State::open(&paths.database)?;
        Ok(Self {
            config,
            paths,
            state,
        })
    }

    pub fn run_named(&mut self, name: &str) -> Result<RunReport> {
        self.recover_staging()?;
        let job = self.config.job(name)?.clone();
        self.run_job(&job)
    }

    pub fn recover_staging(&mut self) -> Result<()> {
        let operation_lock = AppLock::exclusive(&self.paths.operation_lock)?;
        let jobs = self.config.jobs.clone();
        for job in jobs {
            self.recover_job_staging(&job)?;
        }
        drop(operation_lock);
        Ok(())
    }

    pub fn run_job(&mut self, job: &BackupJob) -> Result<RunReport> {
        let operation_lock = AppLock::exclusive(&self.paths.operation_lock)?;
        let result = self.run_job_locked(job);
        drop(operation_lock);
        result
    }

    fn run_job_locked(&mut self, job: &BackupJob) -> Result<RunReport> {
        info!(job = job.name, source = %job.source, "starting backup");
        // A remote source runs its own pre command inside the agent, next to the
        // files it touches.
        if let (Location::Local(_), Some(command)) = (&job.source, &job.pre) {
            pre::run(&job.name, command)?;
        }
        let started = Utc::now();
        let (name, created_at, producer) = match &job.source {
            Location::Local(source) => {
                let scanner = SourceScanner::new(source, &job.exclude)?;
                (
                    archive_name(&job.name, started),
                    started,
                    Producer::Local(scanner),
                )
            }
            Location::Ssh(remote) => {
                let stream = RemoteStream::start(job, remote)?;
                (
                    stream.header.name.clone(),
                    stream.header.created_at,
                    Producer::Remote(stream),
                )
            }
        };

        emit(&Event::BackupStarted {
            job: job.name.clone(),
            archive: name.clone(),
        });
        let mut results = Vec::new();
        let mut sinks = Vec::new();
        for destination in &job.destinations {
            let opened = match destination {
                Location::Local(path) => LocalSink::open(path, &name, job),
                Location::Ssh(remote) => SshSink::open(remote, &name, job),
            };
            match opened {
                Ok(sink) => sinks.push(sink),
                Err(error) => {
                    error!(job = job.name, %destination, error = %format!("{error:#}"), "destination failed to open");
                    results.push(DeliveryResult {
                        destination: destination.clone(),
                        status: DeliveryStatus::Failed(format!("{error:#}")),
                    });
                }
            }
        }
        let staging = self.paths.job_staging(&job.name);
        if needs_staging(job) {
            match StagingSink::open(&staging, &name) {
                Ok(sink) => sinks.push(sink),
                Err(error) => {
                    warn!(job = job.name, error = %format!("{error:#}"), "cannot stage the archive; failed destinations will not be retried from a copy");
                }
            }
        }
        if sinks.is_empty() {
            bail!(
                "job {:?}: every destination failed to open: {}",
                job.name,
                describe(&results)
            );
        }

        let outcome = match producer {
            Producer::Local(scanner) => pump_local(job, &scanner, Tee::new(sinks))?,
            Producer::Remote(stream) => stream.pump(Tee::new(sinks))?,
        };
        let report = self.record_outcome(job, &name, created_at, outcome, results, &staging)?;
        if report.staged {
            self.cleanup_completed()?;
        } else {
            self.complete_unstaged()?;
        }
        Ok(report)
    }

    fn record_outcome(
        &mut self,
        job: &BackupJob,
        name: &str,
        created_at: chrono::DateTime<Utc>,
        outcome: StreamOutcome,
        mut results: Vec<DeliveryResult>,
        staging: &Path,
    ) -> Result<RunReport> {
        let mut staged = false;
        for sink in outcome.sinks {
            match sink.id {
                SinkId::Staging => match sink.error {
                    None => staged = true,
                    Some(error) => warn!(job = job.name, %error, "staging copy failed"),
                },
                SinkId::Destination(destination) => results.push(DeliveryResult {
                    destination,
                    status: sink
                        .error
                        .map_or(DeliveryStatus::Delivered, DeliveryStatus::Failed),
                }),
                SinkId::Stream => {}
            }
        }
        let failed = results
            .iter()
            .filter(|result| result.status != DeliveryStatus::Delivered)
            .count();
        for result in &results {
            match &result.status {
                DeliveryStatus::Delivered => {
                    info!(
                        job = job.name,
                        destination = %result.destination,
                        archive = name,
                        "destination completed"
                    );
                    emit(&Event::DestinationCompleted {
                        destination: result.destination.to_string(),
                    });
                }
                DeliveryStatus::Failed(error) => {
                    error!(
                        job = job.name,
                        destination = %result.destination,
                        archive = name,
                        %error,
                        "destination failed"
                    );
                    emit(&Event::DestinationFailed {
                        destination: result.destination.to_string(),
                        error: error.clone(),
                    });
                }
                DeliveryStatus::Pending => {}
            }
        }
        let delivered = results.len() - failed;
        let path = staging.join(name);
        let artifact = Artifact {
            name: name.to_owned(),
            checksum_path: checksum_path(&path),
            path,
            checksum: outcome.checksum,
            size: outcome.size,
            created_at,
        };
        if failed > 0 && !staged {
            // The copies that were delivered are recorded and the run closed
            // before the failure is reported, so history shows them even though
            // the scheduler will retry the slot with a fresh archive.
            if delivered > 0 {
                let run_id = self.state.register_run(&artifact, job, false, &results)?;
                self.state.mark_run_complete(run_id)?;
            }
            bail!(
                "job {:?}: {failed} destination(s) failed and no staged copy exists for retry: {}",
                job.name,
                describe(&results)
            );
        }
        self.state.register_run(&artifact, job, staged, &results)?;
        info!(
            job = job.name,
            archive = name,
            size = outcome.size,
            delivered,
            failed,
            staged,
            "backup finished"
        );
        Ok(RunReport {
            job: job.name.clone(),
            archive: name.to_owned(),
            size: artifact.size,
            checksum: artifact.checksum,
            delivered,
            failed,
            staged,
        })
    }

    pub fn process_due_deliveries(&mut self) -> Result<()> {
        self.process_due_deliveries_until(None)
    }

    pub fn process_due_deliveries_until(&mut self, stopping: Option<&AtomicBool>) -> Result<()> {
        let operation_lock = AppLock::exclusive(&self.paths.operation_lock)?;
        let result = self.process_due_deliveries_locked(stopping);
        drop(operation_lock);
        result
    }

    fn process_due_deliveries_locked(&mut self, stopping: Option<&AtomicBool>) -> Result<()> {
        let deliveries = self.state.due_deliveries(Utc::now())?;
        for pending in deliveries {
            if stopping.is_some_and(|flag| flag.load(Ordering::SeqCst)) {
                break;
            }
            self.process_delivery(&pending)?;
        }
        self.cleanup_completed()
    }

    fn process_delivery(&self, pending: &PendingDelivery) -> Result<()> {
        match deliver(&pending.artifact, &pending.destination, &pending.job) {
            Ok(()) => {
                self.state
                    .mark_delivered(pending.run_id, &pending.destination)?;
                info!(
                    job = pending.job.name,
                    destination = %pending.destination,
                    archive = pending.artifact.name,
                    "destination completed"
                );
            }
            Err(delivery_error) => {
                let message = format!("{delivery_error:#}");
                let retry_at = self.state.mark_delivery_failed(
                    pending.run_id,
                    &pending.destination,
                    pending.attempts,
                    &message,
                )?;
                error!(
                    job = pending.job.name,
                    destination = %pending.destination,
                    archive = pending.artifact.name,
                    retry_at = %retry_at,
                    error = %delivery_error,
                    "destination failed; delivery remains staged"
                );
            }
        }
        Ok(())
    }

    fn recover_job_staging(&mut self, job: &BackupJob) -> Result<()> {
        let directory = self.paths.job_staging(&job.name);
        if !directory.exists() {
            return Ok(());
        }
        sweep_stale_partials(&directory);
        for entry in fs::read_dir(&directory)? {
            let entry = entry?;
            let path = entry.path();
            if !entry.file_type()?.is_file() || self.state.has_archive(&path)? {
                continue;
            }
            let name = entry.file_name().to_string_lossy().into_owned();
            if !belongs_to_job(&name, &job.name) {
                continue;
            }
            let artifact = match recovered_artifact(path, name) {
                Ok(artifact) => artifact,
                Err(error) => {
                    warn!(job = job.name, %error, "could not recover staged archive; preserving it");
                    continue;
                }
            };
            self.state.register_run(&artifact, job, true, &[])?;
            info!(
                job = job.name,
                archive = artifact.name,
                "recovered staged archive"
            );
        }
        Ok(())
    }

    fn cleanup_completed(&mut self) -> Result<()> {
        for completed in self.state.complete_ready_runs()? {
            if completed.staged {
                remove_staged(&completed.archive)?;
                remove_staged(&completed.checksum)?;
            }
            self.state.mark_run_complete(completed.run_id)?;
        }
        Ok(())
    }

    fn complete_unstaged(&mut self) -> Result<()> {
        for completed in self.state.complete_ready_runs()? {
            if !completed.staged {
                self.state.mark_run_complete(completed.run_id)?;
            }
        }
        Ok(())
    }

    // Removes the run rows before the staged files, so a failure in between
    // leaves an orphaned staged file, which recovery re-registers for a job
    // still in the configuration, never a delivery row whose archive is gone.
    pub fn forget(&mut self, job: &str, clear_schedule: bool) -> Result<Vec<String>> {
        let operation_lock = AppLock::exclusive(&self.paths.operation_lock)?;
        let forgotten = self.state.forget_job(job, clear_schedule)?;
        let mut cancelled = Vec::new();
        for run in forgotten {
            if run.staged {
                remove_staged(&run.archive)?;
                remove_staged(&run.checksum)?;
            }
            info!(
                job,
                archive = run.archive_name,
                "cancelled pending deliveries"
            );
            cancelled.push(run.archive_name);
        }
        drop(operation_lock);
        Ok(cancelled)
    }

    pub fn purge_history(&mut self) -> Result<()> {
        let purged = self
            .state
            .purge_completed(Utc::now() - Duration::days(HISTORY_DAYS))?;
        if purged > 0 {
            info!(
                purged,
                "removed completed runs older than {HISTORY_DAYS} days"
            );
        }
        Ok(())
    }
}

fn needs_staging(job: &BackupJob) -> bool {
    job.destinations.len() > 1
        || job
            .destinations
            .iter()
            .any(|destination| !destination.is_local())
}

fn describe(results: &[DeliveryResult]) -> String {
    results
        .iter()
        .filter_map(|result| match &result.status {
            DeliveryStatus::Failed(error) => Some(format!("{}: {error}", result.destination)),
            _ => None,
        })
        .collect::<Vec<_>>()
        .join("; ")
}

fn recovered_artifact(path: PathBuf, name: String) -> Result<Artifact> {
    let checksum_file_path = checksum_path(&path);
    let checksum = if checksum_file_path.exists() {
        let checksum = read_checksum(&checksum_file_path)?;
        verify_checksum(&path, &checksum)?;
        checksum
    } else {
        let checksum = checksum_file(&path)?;
        write_checksum(&path, &checksum)?;
        checksum
    };
    verify_archive(&path)?;
    let metadata = fs::metadata(&path)?;
    let created_at = parse_archive_name(&name)
        .context("archive name has no timestamp")?
        .created;
    Ok(Artifact {
        name,
        path,
        checksum_path: checksum_file_path,
        checksum,
        size: metadata.len(),
        created_at,
    })
}

fn remove_staged(path: &Path) -> Result<()> {
    match fs::remove_file(path) {
        Ok(()) => {
            info!(path = %path.display(), "removed completed staged file");
            Ok(())
        }
        Err(error) if error.kind() == ErrorKind::NotFound => {
            warn!(path = %path.display(), "completed staged file was already absent");
            Ok(())
        }
        Err(error) => Err(error).with_context(|| format!("remove staged file {}", path.display())),
    }
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::path::Path;
    use std::time::{Duration as StdDuration, SystemTime};

    use chrono::Utc;
    use tempfile::tempdir;

    use super::Runner;
    use crate::archive::{create_local_archive, restore_archive, verify_archive};
    use crate::config::{BackupJob, Config};
    use crate::destination::list_local;
    use crate::location::Location;
    use crate::paths::AppPaths;

    fn paths(root: &Path) -> AppPaths {
        let state = root.join("state");
        fs::create_dir_all(&state).unwrap();
        AppPaths {
            config: root.join("config.toml"),
            database: state.join("state.redb"),
            daemon_lock: state.join("daemon.lock"),
            operation_lock: state.join("operation.lock"),
            staging: state.join("staging"),
            log_file: state.join("logs/backup.log"),
            log_directory: state.join("logs"),
            state,
        }
    }

    fn job(source: &Path, destinations: Vec<Location>) -> BackupJob {
        BackupJob {
            name: "documents".to_owned(),
            source: Location::Local(source.to_path_buf()),
            destinations,
            cron: "0 2 * * *".to_owned(),
            retention: None,
            pre: None,
            exclude: vec!["*.tmp".to_owned()],
        }
    }

    fn source(root: &Path) -> std::path::PathBuf {
        let source = root.join("source");
        fs::create_dir_all(&source).unwrap();
        fs::write(source.join("keep.txt"), "keep").unwrap();
        fs::write(source.join("ignored.tmp"), "ignore").unwrap();
        source
    }

    #[test]
    fn a_single_local_destination_is_written_without_staging() {
        let temporary = tempdir().unwrap();
        let source = source(temporary.path());
        let destination = temporary.path().join("destination");
        let paths = paths(temporary.path());
        let staging = paths.staging.clone();
        let config = Config {
            jobs: vec![job(&source, vec![Location::Local(destination.clone())])],
        };
        let mut runner = Runner::new(config, paths).unwrap();

        runner.run_named("documents").unwrap();

        let archives = list_local(&destination, "documents").unwrap();
        assert_eq!(archives.len(), 1);
        verify_archive(&archives[0].path).unwrap();
        let restored = temporary.path().join("restored");
        restore_archive(&archives[0].path, &restored).unwrap();
        assert_eq!(
            fs::read_to_string(restored.join("keep.txt")).unwrap(),
            "keep"
        );
        assert!(!restored.join("ignored.tmp").exists());
        assert!(runner.state.status().unwrap().is_empty());
        assert_eq!(runner.state.history().unwrap().len(), 1);
        let staged: Vec<_> = fs::read_dir(staging.join("documents"))
            .map(|entries| entries.collect())
            .unwrap_or_default();
        assert!(staged.is_empty(), "staging was used: {staged:?}");
    }

    #[test]
    fn two_destinations_stage_a_copy_and_keep_the_failed_one_pending() {
        let temporary = tempdir().unwrap();
        let source = source(temporary.path());
        let good = temporary.path().join("good");
        let bad = temporary.path().join("bad");
        fs::write(&bad, "a file where a directory is expected").unwrap();
        let paths = paths(temporary.path());
        let staging = paths.staging.clone();
        let config = Config {
            jobs: vec![job(
                &source,
                vec![Location::Local(good.clone()), Location::Local(bad.clone())],
            )],
        };
        let mut runner = Runner::new(config, paths).unwrap();

        runner.run_named("documents").unwrap();

        assert_eq!(list_local(&good, "documents").unwrap().len(), 1);
        let status = runner.state.status().unwrap();
        assert_eq!(status.len(), 1);
        assert_eq!(status[0].pending_destinations, 1);
        let staged = list_local(&staging.join("documents"), "documents").unwrap();
        assert_eq!(staged.len(), 1);
        assert!(staged[0].checksum.is_some());

        fs::remove_file(&bad).unwrap();
        let later = chrono::Utc::now() + chrono::Duration::seconds(61);
        let due = runner.state.due_deliveries(later).unwrap();
        assert_eq!(due.len(), 1);
        runner.process_delivery(&due[0]).unwrap();
        runner.cleanup_completed().unwrap();

        assert_eq!(list_local(&bad, "documents").unwrap().len(), 1);
        assert!(runner.state.status().unwrap().is_empty());
        assert!(
            list_local(&staging.join("documents"), "documents")
                .unwrap()
                .is_empty()
        );
    }

    #[test]
    fn recovery_takes_the_archive_time_from_its_name_and_sweeps_stale_partials() {
        let temporary = tempdir().unwrap();
        let source = source(temporary.path());
        let destination = temporary.path().join("destination");
        let paths = paths(temporary.path());
        let staging = paths.staging.join("documents");
        let job = job(&source, vec![Location::Local(destination)]);

        let artifact = create_local_archive(&job, &source, &staging).unwrap();
        let renamed = "documents-20260102T030405Z-00000000-0000-0000-0000-000000000000.tar.lz4";
        fs::rename(&artifact.path, staging.join(renamed)).unwrap();
        fs::remove_file(&artifact.checksum_path).unwrap();
        let stale = staging.join(".old.tar.lz4.partial");
        fs::write(&stale, "x").unwrap();
        fs::File::open(&stale)
            .unwrap()
            .set_modified(SystemTime::now() - StdDuration::from_secs(2 * 24 * 60 * 60))
            .unwrap();

        let config = Config { jobs: vec![job] };
        let mut runner = Runner::new(config, paths).unwrap();
        runner.recover_staging().unwrap();

        assert!(!stale.exists(), "the stale partial survived recovery");
        let due = runner.state.due_deliveries(Utc::now()).unwrap();
        assert_eq!(due.len(), 1);
        assert_eq!(
            due[0].artifact.created_at.to_rfc3339(),
            "2026-01-02T03:04:05+00:00"
        );
    }

    #[test]
    fn forget_cancels_pending_deliveries_and_removes_the_staged_archive() {
        let temporary = tempdir().unwrap();
        let source = source(temporary.path());
        let good = temporary.path().join("good");
        let bad = temporary.path().join("bad");
        fs::write(&bad, "a file where a directory is expected").unwrap();
        let paths = paths(temporary.path());
        let staging = paths.staging.clone();
        let config = Config {
            jobs: vec![job(
                &source,
                vec![Location::Local(good), Location::Local(bad)],
            )],
        };
        let mut runner = Runner::new(config, paths).unwrap();
        runner.run_named("documents").unwrap();
        assert_eq!(runner.state.status().unwrap().len(), 1);

        let cancelled = runner.forget("documents", false).unwrap();

        assert_eq!(cancelled.len(), 1);
        assert!(runner.state.status().unwrap().is_empty());
        assert!(
            list_local(&staging.join("documents"), "documents")
                .unwrap()
                .is_empty()
        );

        // The staged file is gone, so recovery cannot resurrect the run.
        runner.recover_staging().unwrap();
        assert!(runner.state.status().unwrap().is_empty());
        assert!(runner.forget("documents", false).unwrap().is_empty());
    }

    #[test]
    fn delivered_copies_are_recorded_when_staging_and_another_destination_fail() {
        let temporary = tempdir().unwrap();
        let source = source(temporary.path());
        let good = temporary.path().join("good");
        let bad = temporary.path().join("bad");
        fs::write(&bad, "a file where a directory is expected").unwrap();
        let paths = paths(temporary.path());
        fs::create_dir_all(&paths.staging).unwrap();
        fs::write(
            paths.staging.join("documents"),
            "blocks the staging directory",
        )
        .unwrap();
        let config = Config {
            jobs: vec![job(
                &source,
                vec![Location::Local(good.clone()), Location::Local(bad)],
            )],
        };
        let mut runner = Runner::new(config, paths).unwrap();
        let job = runner.config.jobs[0].clone();

        let error = runner.run_job(&job).unwrap_err();

        assert!(format!("{error:#}").contains("no staged copy exists"));
        assert_eq!(list_local(&good, "documents").unwrap().len(), 1);
        assert!(runner.state.status().unwrap().is_empty());
        assert_eq!(runner.state.history().unwrap().len(), 1);
    }

    #[test]
    fn a_failed_single_destination_without_staging_fails_the_run() {
        let temporary = tempdir().unwrap();
        let source = source(temporary.path());
        let bad = temporary.path().join("bad");
        fs::write(&bad, "a file where a directory is expected").unwrap();
        let paths = paths(temporary.path());
        let config = Config {
            jobs: vec![job(&source, vec![Location::Local(bad)])],
        };
        let mut runner = Runner::new(config, paths).unwrap();

        let error = runner.run_named("documents").unwrap_err();

        assert!(format!("{error:#}").contains("every destination failed to open"));
        assert!(runner.state.status().unwrap().is_empty());
    }
}
