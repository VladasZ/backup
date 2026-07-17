use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, mpsc};
use std::time::Duration;

use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use notify::{Event, RecursiveMode, Result as NotifyResult, Watcher, recommended_watcher};
use tracing::{error, info, warn};

use crate::config::{BackupJob, Config};
use crate::lock::AppLock;
use crate::paths::AppPaths;
use crate::runner::Runner;

const LOOP_INTERVAL: Duration = Duration::from_secs(1);

pub fn run(config: Config, paths: AppPaths) -> Result<()> {
    let daemon_lock = AppLock::try_exclusive(
        &paths.daemon_lock,
        "another backup daemon is already running",
    )?;
    let stopping = Arc::new(AtomicBool::new(false));
    let signal = Arc::clone(&stopping);
    ctrlc::set_handler(move || signal.store(true, Ordering::SeqCst))
        .context("install shutdown signal handler")?;

    let (events, receiver) = mpsc::channel::<NotifyResult<Event>>();
    let mut watcher = recommended_watcher(events).context("create config watcher")?;
    let watch_path = paths
        .config
        .parent()
        .context("configuration path has no parent directory")?;
    watcher
        .watch(watch_path, RecursiveMode::NonRecursive)
        .with_context(|| format!("watch configuration directory {}", watch_path.display()))?;

    let mut runner = Runner::new(config, paths)?;
    runner.recover_staging()?;
    info!(config = %runner.paths.config.display(), "backup daemon started");
    while !stopping.load(Ordering::SeqCst) {
        if let Err(error) = runner.process_due_deliveries_until(Some(&stopping)) {
            error!(%error, "failed to process pending deliveries");
        }
        if stopping.load(Ordering::SeqCst) {
            break;
        }
        run_due_jobs(&mut runner, &stopping)?;

        match receiver.recv_timeout(LOOP_INTERVAL) {
            Ok(event) => {
                let mut reload = event_requires_reload(event, &runner.paths.config);
                while let Ok(event) = receiver.try_recv() {
                    reload |= event_requires_reload(event, &runner.paths.config);
                }
                if reload {
                    reload_config(&mut runner);
                }
            }
            Err(mpsc::RecvTimeoutError::Timeout) => {}
            Err(mpsc::RecvTimeoutError::Disconnected) => {
                warn!("configuration watcher stopped; stopping daemon");
                break;
            }
        }
    }
    info!("backup daemon stopped");
    drop(watcher);
    drop(daemon_lock);
    Ok(())
}

fn run_due_jobs(runner: &mut Runner, stopping: &AtomicBool) -> Result<()> {
    let now = Utc::now();
    let jobs = runner.config.jobs.clone();
    for job in jobs {
        if stopping.load(Ordering::SeqCst) {
            break;
        }
        let last = runner.state.last_scheduled(&job.name)?;
        let Some(latest) = unhandled_slot(&job, last, now)? else {
            continue;
        };

        runner.state.set_last_scheduled(&job.name, latest)?;
        info!(job = job.name, scheduled_at = %latest, "queueing scheduled backup");
        if let Err(error) = runner.run_job(&job) {
            error!(job = job.name, %error, "scheduled backup failed");
        }
    }
    Ok(())
}

fn unhandled_slot(
    job: &BackupJob,
    last: Option<DateTime<Utc>>,
    now: DateTime<Utc>,
) -> Result<Option<DateTime<Utc>>> {
    let latest = job
        .schedule()?
        .find_previous_occurrence(&now, true)
        .with_context(|| format!("find latest schedule for job {:?}", job.name))?;
    Ok((!last.is_some_and(|last| last >= latest)).then_some(latest))
}

fn affects_config(event: &Event, config: &Path) -> bool {
    event.paths.is_empty() || event.paths.iter().any(|path| path == config)
}

fn event_requires_reload(event: NotifyResult<Event>, config: &Path) -> bool {
    event
        .map(|event| affects_config(&event, config))
        .unwrap_or_else(|error| {
            warn!(%error, "configuration watcher reported an error");
            false
        })
}

fn reload_config(runner: &mut Runner) {
    match Config::load(&runner.paths.config) {
        Ok(config) => {
            runner.config = config;
            info!(jobs = runner.config.jobs.len(), "reloaded configuration");
        }
        Err(error) => {
            error!(%error, "configuration reload failed; keeping active configuration");
        }
    }
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use chrono::{TimeZone, Utc};

    use super::unhandled_slot;
    use crate::config::BackupJob;
    use crate::location::Location;

    #[test]
    fn collapses_multiple_missed_slots_into_the_latest_one() {
        let job = BackupJob {
            name: "hourly".to_owned(),
            source: Location::Local(PathBuf::from("/source")),
            destinations: vec![Location::Local(PathBuf::from("/destination"))],
            cron: "0 * * * *".to_owned(),
            retention: None,
            exclude: Vec::new(),
        };
        let now = Utc.with_ymd_and_hms(2026, 7, 17, 12, 45, 0).unwrap();
        let last = Utc.with_ymd_and_hms(2026, 7, 17, 8, 0, 0).unwrap();
        let latest = Utc.with_ymd_and_hms(2026, 7, 17, 12, 0, 0).unwrap();
        assert_eq!(unhandled_slot(&job, Some(last), now).unwrap(), Some(latest));
        assert_eq!(unhandled_slot(&job, Some(latest), now).unwrap(), None);
        assert_eq!(unhandled_slot(&job, None, now).unwrap(), Some(latest));
    }
}
