use std::sync::atomic::{AtomicBool, Ordering};

use anyhow::{Context, Result, bail};
use chrono::{DateTime, Duration, Utc};
use serde::Serialize;
use tracing::{error, info};

use crate::archive::{Artifact, Produced, SourceScanner, archive_name, produce};
use crate::config::{BackupJob, Config};
use crate::location::Location;
use crate::lock::AppLock;
use crate::output::{Event, emit};
use crate::paths::AppPaths;
use crate::pre;
use crate::repair::copy_archive;
use crate::ssh::RemoteStream;
use crate::state::{DeliveryResult, DeliveryStatus, PendingDelivery, State};
use crate::store::recipe::Recipe;
use crate::transfer::{Fanout, SinkOutcome, describe, open_sink};

pub(crate) const HISTORY_DAYS: i64 = 30;

#[derive(Clone, Debug, Serialize)]
pub struct RunReport {
    pub job: String,
    pub archive: String,
    pub size: u64,
    pub checksum: String,
    pub delivered: usize,
    pub failed: usize,
}

pub struct Runner {
    pub config: Config,
    pub paths: AppPaths,
    pub state: State,
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
        let job = self.config.job(name)?.clone();
        self.run_job(&job)
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
        let mut results = Vec::new();
        let mut sinks = Vec::new();
        for destination in &job.destinations {
            match open_sink(destination, job) {
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
        if sinks.is_empty() {
            bail!(
                "job {:?}: every destination failed to open: {}",
                job.name,
                describe_results(&results)
            );
        }
        let mut fanout = Fanout::new(sinks);
        let mut known = fanout.known();
        let produced = (|| -> Result<(String, DateTime<Utc>, Produced)> {
            match &job.source {
                Location::Local(source) => {
                    let created = Utc::now();
                    let name = archive_name(&job.name, created);
                    emit(&Event::BackupStarted {
                        job: job.name.clone(),
                        archive: name.clone(),
                    });
                    let scanner = SourceScanner::new(source, &job.exclude)?;
                    let produced = produce(&job.name, &scanner, &mut known, &mut |event| {
                        fanout.accept(event)
                    })?;
                    Ok((name, created, produced))
                }
                Location::Ssh(remote) => {
                    let stream = RemoteStream::start(job, remote, &known)?;
                    let name = stream.header.name.clone();
                    let created = stream.header.created_at;
                    emit(&Event::BackupStarted {
                        job: job.name.clone(),
                        archive: name.clone(),
                    });
                    Ok((name, created, stream.pump(&mut fanout)?))
                }
            }
        })();
        let (name, created, produced) = match produced {
            Ok(produced) => produced,
            Err(error) => {
                let failed = fanout.abort();
                if failed.is_empty() {
                    return Err(error);
                }
                return Err(error.context(format!("destinations failed: {}", describe(&failed))));
            }
        };
        let recipe = Recipe {
            job: job.name.clone(),
            name,
            created,
            size: produced.size,
            checksum: produced.checksum,
            chunks: produced.chunks,
        };
        let outcomes = fanout.complete(&recipe);
        self.record_outcome(job, &recipe, outcomes, results)
    }

    fn record_outcome(
        &mut self,
        job: &BackupJob,
        recipe: &Recipe,
        outcomes: Vec<SinkOutcome>,
        mut results: Vec<DeliveryResult>,
    ) -> Result<RunReport> {
        results.extend(outcomes.into_iter().map(|outcome| {
            DeliveryResult {
                destination: outcome.destination,
                status: outcome
                    .error
                    .map_or(DeliveryStatus::Delivered, DeliveryStatus::Failed),
            }
        }));
        for result in &results {
            match &result.status {
                DeliveryStatus::Delivered => {
                    info!(job = job.name, destination = %result.destination, archive = recipe.name, "destination completed");
                    emit(&Event::DestinationCompleted {
                        destination: result.destination.to_string(),
                    });
                }
                DeliveryStatus::Failed(error) => {
                    error!(job = job.name, destination = %result.destination, archive = recipe.name, %error, "destination failed");
                    emit(&Event::DestinationFailed {
                        destination: result.destination.to_string(),
                        error: error.clone(),
                    });
                }
                DeliveryStatus::Pending => {}
            }
        }
        let failed = results
            .iter()
            .filter(|result| result.status != DeliveryStatus::Delivered)
            .count();
        let delivered = results.len() - failed;
        if delivered == 0 {
            bail!(
                "job {:?}: every destination failed: {}",
                job.name,
                describe_results(&results)
            );
        }
        // A destination that failed is filled later by copying the backup
        // from one that has it, so it gets a pending delivery.
        let artifact = Artifact {
            name: recipe.name.clone(),
            checksum: recipe.checksum.clone(),
            size: recipe.size,
            created_at: recipe.created,
        };
        self.state.register_run(&artifact, job, &results)?;
        self.complete_ready_runs()?;
        info!(
            job = job.name,
            archive = recipe.name,
            size = recipe.size,
            delivered,
            failed,
            "backup finished"
        );
        Ok(RunReport {
            job: job.name.clone(),
            archive: recipe.name.clone(),
            size: recipe.size,
            checksum: recipe.checksum.clone(),
            delivered,
            failed,
        })
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
        self.complete_ready_runs()
    }

    fn process_delivery(&self, pending: &PendingDelivery) -> Result<()> {
        match copy_archive(&pending.job, &pending.artifact.name, &pending.destination) {
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
                    error = %message,
                    "destination failed; the backup will be copied from another destination later"
                );
            }
        }
        Ok(())
    }

    fn complete_ready_runs(&mut self) -> Result<()> {
        for completed in self.state.complete_ready_runs()? {
            self.state.mark_run_complete(completed.run_id)?;
        }
        Ok(())
    }

    pub fn forget(&mut self, job: &str, clear_schedule: bool) -> Result<Vec<String>> {
        let operation_lock = AppLock::exclusive(&self.paths.operation_lock)?;
        let forgotten = self.state.forget_job(job, clear_schedule)?;
        drop(operation_lock);
        let mut cancelled = Vec::new();
        for run in forgotten {
            info!(
                job,
                archive = run.archive_name,
                "cancelled pending deliveries"
            );
            cancelled.push(run.archive_name);
        }
        Ok(cancelled)
    }

    pub fn purge_history(&mut self) -> Result<()> {
        let purged = self
            .state
            .purge_completed(Utc::now() - Duration::days(HISTORY_DAYS))
            .context("purge history")?;
        if purged > 0 {
            info!(
                purged,
                "removed completed runs older than {HISTORY_DAYS} days"
            );
        }
        Ok(())
    }
}

fn describe_results(results: &[DeliveryResult]) -> String {
    results
        .iter()
        .filter_map(|result| match &result.status {
            DeliveryStatus::Failed(error) => Some(format!("{}: {error}", result.destination)),
            _ => None,
        })
        .collect::<Vec<_>>()
        .join("; ")
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::path::{Path, PathBuf};

    use chrono::{Duration, Utc};
    use tempfile::tempdir;

    use super::Runner;
    use crate::config::{BackupJob, Config};
    use crate::destination::list;
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

    fn source(root: &Path) -> PathBuf {
        let source = root.join("source");
        fs::create_dir_all(&source).unwrap();
        fs::write(source.join("keep.txt"), "keep").unwrap();
        fs::write(source.join("ignored.tmp"), "ignore").unwrap();
        source
    }

    #[test]
    fn a_failed_destination_is_filled_later_from_one_that_succeeded() {
        let temporary = tempdir().unwrap();
        let source = source(temporary.path());
        let good = temporary.path().join("good");
        let bad = temporary.path().join("bad");
        fs::write(&bad, "a file where a directory is expected").unwrap();
        let config = Config {
            jobs: vec![job(
                &source,
                vec![Location::Local(good.clone()), Location::Local(bad.clone())],
            )],
        };
        let mut runner = Runner::new(config, paths(temporary.path())).unwrap();

        let report = runner.run_named("documents").unwrap();
        assert_eq!((report.delivered, report.failed), (1, 1));
        assert_eq!(
            list(&Location::Local(good), Some("documents"))
                .unwrap()
                .len(),
            1
        );
        assert_eq!(runner.state.status().unwrap()[0].pending_destinations, 1);

        fs::remove_file(&bad).unwrap();
        let due = runner
            .state
            .due_deliveries(Utc::now() + Duration::seconds(61))
            .unwrap();
        assert_eq!(due.len(), 1);
        runner.process_delivery(&due[0]).unwrap();
        runner.complete_ready_runs().unwrap();

        let copied = list(&Location::Local(bad), Some("documents")).unwrap();
        assert_eq!(copied.len(), 1);
        assert_eq!(copied[0].name, report.archive);
        assert!(runner.state.status().unwrap().is_empty());
    }

    #[test]
    fn a_run_whose_every_destination_fails_is_an_error() {
        let temporary = tempdir().unwrap();
        let source = source(temporary.path());
        let bad = temporary.path().join("bad");
        fs::write(&bad, "a file where a directory is expected").unwrap();
        let config = Config {
            jobs: vec![job(&source, vec![Location::Local(bad)])],
        };
        let mut runner = Runner::new(config, paths(temporary.path())).unwrap();

        let error = runner.run_named("documents").unwrap_err();

        assert!(format!("{error:#}").contains("every destination failed"));
        assert!(runner.state.status().unwrap().is_empty());
    }

    #[test]
    fn forget_cancels_pending_deliveries() {
        let temporary = tempdir().unwrap();
        let source = source(temporary.path());
        let good = temporary.path().join("good");
        let bad = temporary.path().join("bad");
        fs::write(&bad, "a file where a directory is expected").unwrap();
        let config = Config {
            jobs: vec![job(
                &source,
                vec![Location::Local(good), Location::Local(bad)],
            )],
        };
        let mut runner = Runner::new(config, paths(temporary.path())).unwrap();
        runner.run_named("documents").unwrap();

        assert_eq!(runner.forget("documents", false).unwrap().len(), 1);
        assert!(runner.state.status().unwrap().is_empty());
    }
}
