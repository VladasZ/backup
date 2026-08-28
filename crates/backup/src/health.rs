use std::collections::BTreeMap;
use std::path::Path;

use anyhow::Result;
use chrono::{DateTime, Duration, Utc};
use serde::Serialize;

use crate::config::Config;
use crate::daemon::unhandled_slot;
use crate::lock::is_locked;
use crate::state::State;

const GRACE_SECONDS: i64 = 3600;

#[derive(Debug, Serialize)]
pub struct HealthReport {
    pub healthy: bool,
    pub daemon_running: bool,
    pub jobs: Vec<JobHealth>,
}

#[derive(Debug, Serialize)]
pub struct JobHealth {
    pub name: String,
    pub healthy: bool,
    pub problems: Vec<String>,
}

pub fn check(
    config: &Config,
    state: &State,
    daemon_lock: &Path,
    now: DateTime<Utc>,
) -> Result<HealthReport> {
    let daemon_running = is_locked(daemon_lock)?;
    let pending = state.status()?;
    let grace = Duration::seconds(GRACE_SECONDS);
    let mut jobs = Vec::new();
    for job in &config.jobs {
        let mut problems = Vec::new();
        let last = state.last_scheduled(&job.name)?;
        if let Some(slot) = unhandled_slot(job, last, now)?
            && now - slot > grace
        {
            problems.push(format!(
                "scheduled backup for slot {} has not completed",
                slot.to_rfc3339()
            ));
        }
        for line in pending.iter().filter(|line| line.job == job.name) {
            if now - line.created_at > grace {
                problems.push(format!(
                    "{} destination(s) still pending for archive {}",
                    line.pending_destinations, line.archive
                ));
            }
        }
        jobs.push(JobHealth {
            name: job.name.clone(),
            healthy: problems.is_empty(),
            problems,
        });
    }
    let mut orphans: BTreeMap<String, Vec<String>> = BTreeMap::new();
    for line in &pending {
        if config.jobs.iter().any(|job| job.name == line.job) {
            continue;
        }
        orphans.entry(line.job.clone()).or_default().push(format!(
            "archive {} has {} destination(s) pending but the job is not in the configuration",
            line.archive, line.pending_destinations
        ));
    }
    for (name, problems) in orphans {
        jobs.push(JobHealth {
            name,
            healthy: false,
            problems,
        });
    }
    let healthy = daemon_running && jobs.iter().all(|job| job.healthy);
    Ok(HealthReport {
        healthy,
        daemon_running,
        jobs,
    })
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use chrono::{Duration, TimeZone, Utc};
    use tempfile::tempdir;

    use super::check;
    use crate::archive::Artifact;
    use crate::config::{BackupJob, Config};
    use crate::location::Location;
    use crate::state::State;

    fn config() -> Config {
        Config {
            jobs: vec![BackupJob {
                name: "documents".to_owned(),
                source: Location::Local(PathBuf::from("/source")),
                destinations: vec![Location::Local(PathBuf::from("/destination"))],
                cron: "0 2 * * *".to_owned(),
                retention: None,
                exclude: Vec::new(),
            }],
        }
    }

    #[test]
    fn a_missed_slot_and_a_stopped_daemon_are_unhealthy() {
        let temporary = tempdir().unwrap();
        let state = State::open(&temporary.path().join("state.redb")).unwrap();
        let lock = temporary.path().join("daemon.lock");
        let now = Utc.with_ymd_and_hms(2026, 8, 28, 12, 0, 0).unwrap();

        let report = check(&config(), &state, &lock, now).unwrap();

        assert!(!report.healthy);
        assert!(!report.daemon_running);
        assert!(!report.jobs[0].healthy);
        assert!(report.jobs[0].problems[0].contains("has not completed"));
    }

    #[test]
    fn an_unhandled_slot_within_the_grace_period_is_not_a_problem() {
        let temporary = tempdir().unwrap();
        let state = State::open(&temporary.path().join("state.redb")).unwrap();
        let lock = temporary.path().join("daemon.lock");
        let now = Utc.with_ymd_and_hms(2026, 8, 28, 2, 30, 0).unwrap();

        let report = check(&config(), &state, &lock, now).unwrap();

        assert!(report.jobs[0].healthy);
    }

    #[test]
    fn a_handled_slot_is_healthy_and_an_old_pending_delivery_is_not() {
        let temporary = tempdir().unwrap();
        let mut state = State::open(&temporary.path().join("state.redb")).unwrap();
        let lock = temporary.path().join("daemon.lock");
        let now = Utc.with_ymd_and_hms(2026, 8, 28, 12, 0, 0).unwrap();
        let slot = Utc.with_ymd_and_hms(2026, 8, 28, 2, 0, 0).unwrap();
        state.set_last_scheduled("documents", slot).unwrap();

        let report = check(&config(), &state, &lock, now).unwrap();
        assert!(report.jobs[0].healthy);

        let job = config().jobs[0].clone();
        let artifact = Artifact {
            name: "documents-archive.tar".to_owned(),
            path: temporary.path().join("archive.tar"),
            checksum_path: temporary.path().join("archive.tar.blake3"),
            checksum: "abc".to_owned(),
            size: 12,
            created_at: now - Duration::hours(2),
        };
        state.register_run(&artifact, &job, true, &[]).unwrap();

        let report = check(&config(), &state, &lock, now).unwrap();
        assert!(!report.jobs[0].healthy);
        assert!(report.jobs[0].problems[0].contains("still pending"));
    }
}
