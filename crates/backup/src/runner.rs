use std::fs;
use std::io::ErrorKind;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};

use anyhow::{Context, Result};
use chrono::Utc;
use tracing::{error, info, warn};

use crate::archive::{
    Artifact, checksum_file, create_local_archive, read_checksum, verify_archive, verify_checksum,
    write_checksum,
};
use crate::config::{BackupJob, Config};
use crate::destination::{checksum_path, is_archive_name};
use crate::location::Location;
use crate::lock::AppLock;
use crate::paths::AppPaths;
use crate::ssh::create_remote_archive;
use crate::state::{PendingDelivery, State};
use crate::storage::{require_staging_reserve, warn_if_high};
use crate::transport::deliver;

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

    pub fn run_named(&mut self, name: &str) -> Result<()> {
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

    pub fn run_job(&mut self, job: &BackupJob) -> Result<()> {
        let operation_lock = AppLock::exclusive(&self.paths.operation_lock)?;
        let result = self.run_job_locked(job);
        drop(operation_lock);
        result
    }

    fn run_job_locked(&mut self, job: &BackupJob) -> Result<()> {
        require_staging_reserve(&self.paths.staging)?;
        info!(job = job.name, source = %job.source, "starting backup");
        let staging = self.paths.job_staging(&job.name);
        let artifact = match &job.source {
            Location::Local(source) => {
                warn_if_high(source, "source")?;
                create_local_archive(job, &self.config.compression, source, &staging)?
            }
            Location::Ssh(remote) => {
                create_remote_archive(job, &self.config.compression, remote, &staging)?
            }
        };
        let run_id = self.state.register_run(&artifact, job)?;
        for pending in self.state.deliveries_for_run(run_id)? {
            self.process_delivery(&pending)?;
        }
        self.cleanup_completed()
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
        let prefix = format!("{}-", job.name);
        for entry in fs::read_dir(&directory)? {
            let entry = entry?;
            let path = entry.path();
            if !entry.file_type()?.is_file() || self.state.has_archive(&path)? {
                continue;
            }
            let name = entry.file_name().to_string_lossy().into_owned();
            if !name.starts_with(&prefix) || !is_archive_name(&name) {
                continue;
            }
            let artifact = match recovered_artifact(path, name) {
                Ok(artifact) => artifact,
                Err(error) => {
                    warn!(job = job.name, %error, "could not recover staged archive; preserving it");
                    continue;
                }
            };
            self.state.register_run(&artifact, job)?;
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
            remove_staged(&completed.archive)?;
            remove_staged(&completed.checksum)?;
            self.state.mark_run_complete(completed.run_id)?;
        }
        Ok(())
    }

    pub fn print_status(&self) -> Result<()> {
        let status = self.state.status()?;
        if status.is_empty() {
            println!("no pending deliveries");
            return Ok(());
        }
        for line in status {
            println!(
                "{}  {}  {}  {} destination(s) pending",
                line.created_at.to_rfc3339(),
                line.job,
                line.archive,
                line.pending_destinations
            );
        }
        Ok(())
    }
}

fn recovered_artifact(path: PathBuf, name: String) -> Result<Artifact> {
    let sidecar = checksum_path(&path);
    let checksum = if sidecar.exists() {
        let checksum = read_checksum(&sidecar)?;
        verify_checksum(&path, &checksum)?;
        checksum
    } else {
        let checksum = checksum_file(&path)?;
        write_checksum(&path, &checksum)?;
        checksum
    };
    verify_archive(&path)?;
    let metadata = fs::metadata(&path)?;
    Ok(Artifact {
        name,
        path,
        checksum_path: sidecar,
        checksum,
        size: metadata.len(),
        created_at: metadata.modified()?.into(),
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

    use tempfile::tempdir;

    use super::Runner;
    use crate::archive::{restore_archive, verify_archive};
    use crate::config::{BackupJob, CompressionAlgorithm, CompressionConfig, Config};
    use crate::destination::list_local;
    use crate::location::Location;
    use crate::paths::AppPaths;
    use crate::storage::usage;

    #[test]
    fn runs_a_local_job_from_archive_through_delivery() {
        let temporary = tempdir().unwrap();
        let source = temporary.path().join("source");
        let destination = temporary.path().join("destination");
        let state = temporary.path().join("state");
        fs::create_dir_all(&source).unwrap();
        fs::create_dir_all(&destination).unwrap();
        fs::create_dir_all(&state).unwrap();
        let storage = usage(&state).unwrap();
        if storage.available < storage.required_reserve {
            return;
        }
        fs::write(source.join("keep.txt"), "keep").unwrap();
        fs::write(source.join("ignored.tmp"), "ignore").unwrap();
        let paths = AppPaths {
            config: temporary.path().join("config.toml"),
            database: state.join("state.sqlite3"),
            daemon_lock: state.join("daemon.lock"),
            operation_lock: state.join("operation.lock"),
            staging: state.join("staging"),
            log_file: state.join("logs/backup.log"),
            log_directory: state.join("logs"),
            state,
        };
        let config = Config {
            compression: CompressionConfig {
                algorithm: CompressionAlgorithm::None,
                level: None,
            },
            jobs: vec![BackupJob {
                name: "documents".to_owned(),
                source: Location::Local(source),
                destinations: vec![Location::Local(destination.clone())],
                cron: "0 2 * * *".to_owned(),
                retention: None,
                exclude: vec!["*.tmp".to_owned()],
            }],
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
    }
}
