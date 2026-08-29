use std::collections::HashMap;
use std::collections::hash_map::Entry;
use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, mpsc};
use std::time::Duration;

use anyhow::{Context, Result};
use chrono::{DateTime, Duration as ChronoDuration, Timelike, Utc};
use notify::{Event, RecursiveMode, Result as NotifyResult, Watcher, recommended_watcher};
use tracing::{error, info, warn};

use crate::config::{BackupJob, Config};
use crate::lock::AppLock;
use crate::paths::AppPaths;
use crate::runner::Runner;
use crate::state::BackupRetry;

const LOOP_INTERVAL: Duration = Duration::from_secs(1);
const DELIVERY_RECHECK_SECONDS: i64 = 60;
const PURGE_INTERVAL_HOURS: i64 = 1;
const STATE_ERROR_RETRY_SECONDS: i64 = 60;

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
    let mut next_delivery_check = Utc::now();
    let mut last_purge: Option<DateTime<Utc>> = None;
    let mut jobs_retry_at: Option<DateTime<Utc>> = None;
    let mut schedule_cache: HashMap<String, JobSchedule> = HashMap::new();
    while !stopping.load(Ordering::SeqCst) {
        let now = Utc::now();
        if last_purge.is_none_or(|last| now - last >= ChronoDuration::hours(PURGE_INTERVAL_HOURS)) {
            if let Err(error) = runner.purge_history() {
                error!(%error, "failed to purge old history");
            }
            last_purge = Some(now);
        }
        if next_delivery_check <= now {
            if let Err(error) = runner.process_due_deliveries_until(Some(&stopping)) {
                error!(%error, "failed to process pending deliveries");
            }
            next_delivery_check = next_check(&runner);
        }
        if stopping.load(Ordering::SeqCst) {
            break;
        }
        if jobs_retry_at.is_none_or(|at| now >= at) {
            match run_due_jobs(&mut runner, &stopping, &mut schedule_cache) {
                Ok(true) => next_delivery_check = next_check(&runner),
                Ok(false) => {}
                // A state database error must not kill the daemon. The pause
                // keeps a persistent error from rerunning a backup every tick.
                Err(error) => {
                    error!(%error, "failed to run scheduled backups; retrying in a minute");
                    jobs_retry_at =
                        Some(Utc::now() + ChronoDuration::seconds(STATE_ERROR_RETRY_SECONDS));
                }
            }
        }

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

// Other processes such as `backup run` can add pending deliveries at any time, so an idle
// daemon still looks again after a minute even when its own state showed nothing due.
fn next_check(runner: &Runner) -> DateTime<Utc> {
    let fallback = Utc::now() + ChronoDuration::seconds(DELIVERY_RECHECK_SECONDS);
    match runner.state.next_due() {
        Ok(due) => due.map_or(fallback, |due| due.min(fallback)),
        Err(error) => {
            error!(%error, "failed to read the next delivery time");
            fallback
        }
    }
}

// The daemon is the only writer of schedules and backup retries, so both are
// cached and idle ticks never open the state database.
struct JobSchedule {
    last: Option<DateTime<Utc>>,
    retry: Option<BackupRetry>,
}

fn run_due_jobs(
    runner: &mut Runner,
    stopping: &AtomicBool,
    cache: &mut HashMap<String, JobSchedule>,
) -> Result<bool> {
    let now = Utc::now();
    let mut ran = false;
    let jobs = runner.config.jobs.clone();
    for job in jobs {
        if stopping.load(Ordering::SeqCst) {
            break;
        }
        let schedule = match cache.entry(job.name.clone()) {
            Entry::Occupied(entry) => entry.into_mut(),
            Entry::Vacant(entry) => entry.insert(JobSchedule {
                last: runner.state.last_scheduled(&job.name)?,
                retry: runner.state.backup_retry(&job.name)?,
            }),
        };
        let unhandled = unhandled_slot(&job, schedule.last, now)?;
        let Some(slot) = due_backup(unhandled, schedule.retry, now) else {
            continue;
        };

        info!(job = job.name, scheduled_at = %slot, "queueing scheduled backup");
        ran = true;
        match runner.run_job(&job) {
            Ok(_) => {
                runner.state.set_last_scheduled(&job.name, slot)?;
                runner.state.clear_backup_retry(&job.name)?;
                schedule.last = Some(slot);
                schedule.retry = None;
            }
            Err(error) => {
                let previous_attempts = schedule
                    .retry
                    .filter(|retry| retry.slot == slot)
                    .map_or(0, |retry| retry.attempts);
                let retry_at =
                    runner
                        .state
                        .record_backup_failure(&job.name, slot, previous_attempts)?;
                schedule.retry = Some(BackupRetry {
                    slot,
                    attempts: previous_attempts + 1,
                    next_retry: retry_at,
                });
                error!(job = job.name, %error, retry_at = %retry_at, "scheduled backup failed; will retry");
            }
        }
    }
    Ok(ran)
}

fn due_backup(
    unhandled: Option<DateTime<Utc>>,
    retry: Option<BackupRetry>,
    now: DateTime<Utc>,
) -> Option<DateTime<Utc>> {
    let slot = unhandled?;
    if let Some(retry) = retry
        && retry.slot == slot
        && now < retry.next_retry
    {
        return None;
    }
    Some(slot)
}

pub(crate) fn unhandled_slot(
    job: &BackupJob,
    last: Option<DateTime<Utc>>,
    now: DateTime<Utc>,
) -> Result<Option<DateTime<Utc>>> {
    let latest = job
        .schedule()?
        .find_previous_occurrence(&now, true)
        .with_context(|| format!("find latest schedule for job {:?}", job.name))?;
    let latest = latest
        .with_nanosecond(0)
        .context("cron slot has no whole second")?;
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

    use chrono::{DateTime, Duration, TimeZone, Timelike, Utc};

    use super::{due_backup, unhandled_slot};
    use crate::config::BackupJob;
    use crate::location::Location;
    use crate::state::BackupRetry;

    #[test]
    fn scheduled_backup_backs_off_then_retries() {
        let slot = Utc.with_ymd_and_hms(2026, 7, 17, 2, 0, 0).unwrap();
        let now = Utc.with_ymd_and_hms(2026, 7, 17, 2, 0, 30).unwrap();

        assert_eq!(due_backup(Some(slot), None, now), Some(slot));

        let waiting = BackupRetry {
            slot,
            attempts: 1,
            next_retry: now + Duration::seconds(60),
        };
        assert_eq!(due_backup(Some(slot), Some(waiting), now), None);

        let elapsed = BackupRetry {
            slot,
            attempts: 1,
            next_retry: now - Duration::seconds(1),
        };
        assert_eq!(due_backup(Some(slot), Some(elapsed), now), Some(slot));

        let newer = slot + Duration::hours(1);
        assert_eq!(due_backup(Some(newer), Some(waiting), now), Some(newer));

        assert_eq!(due_backup(None, Some(waiting), now), None);
    }

    #[test]
    fn a_handled_slot_stays_handled_when_the_clock_has_a_sub_second_part() {
        let job = BackupJob {
            name: "documents".to_owned(),
            source: Location::Local(PathBuf::from("/source")),
            destinations: vec![Location::Local(PathBuf::from("/destination"))],
            cron: "0 2 * * *".to_owned(),
            retention: None,
            pre: None,
            exclude: Vec::new(),
        };
        let now = Utc
            .with_ymd_and_hms(2026, 7, 17, 18, 1, 52)
            .unwrap()
            .with_nanosecond(644_228_000)
            .unwrap();
        let slot = unhandled_slot(&job, None, now).unwrap().unwrap();

        assert_eq!(slot.nanosecond(), 0);

        let stored = DateTime::from_timestamp(slot.timestamp(), 0).unwrap();
        let later = now + Duration::milliseconds(900);
        assert_eq!(unhandled_slot(&job, Some(stored), later).unwrap(), None);
    }

    #[test]
    fn collapses_multiple_missed_slots_into_the_latest_one() {
        let job = BackupJob {
            name: "hourly".to_owned(),
            source: Location::Local(PathBuf::from("/source")),
            destinations: vec![Location::Local(PathBuf::from("/destination"))],
            cron: "0 * * * *".to_owned(),
            retention: None,
            pre: None,
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
