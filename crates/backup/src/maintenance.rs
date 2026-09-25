//! Work the daemon does between scheduled backups: a full verify of every
//! destination once a week, and importing archives from the old format one
//! at a time. Each step is short enough that a backup coming due waits at
//! most for one of them.

use std::collections::{HashSet, VecDeque};

use anyhow::Result;
use chrono::{DateTime, Duration, Utc};
use tracing::{error, info, warn};

use crate::destination;
use crate::location::Location;
use crate::lock::AppLock;
use crate::repair::{destinations_with_peers, verify_destination};
use crate::runner::Runner;

pub const VERIFY_EVERY_DAYS: i64 = 7;
const IMPORT_RESCAN_MINUTES: i64 = 60;
// A failed import is tried again after a day, since the cause can be as
// passing as a remote that was offline.
const IMPORT_RETRY_HOURS: i64 = 24;

#[derive(Default)]
pub struct Maintenance {
    imports: VecDeque<(Location, String)>,
    next_scan: Option<DateTime<Utc>>,
}

impl Maintenance {
    /// Runs at most one verify or one import. Returns whether it did work.
    pub fn step(&mut self, runner: &mut Runner, now: DateTime<Utc>) -> Result<bool> {
        if let Some((destination, peers)) = due_verification(runner, now)? {
            verify(runner, &destination, &peers, now)?;
            return Ok(true);
        }
        if self.imports.is_empty() && self.next_scan.is_none_or(|next| now >= next) {
            self.scan(runner, now)?;
            self.next_scan = Some(now + Duration::minutes(IMPORT_RESCAN_MINUTES));
        }
        let Some((destination, name)) = self.imports.pop_front() else {
            return Ok(false);
        };
        import(runner, &destination, &name)?;
        Ok(true)
    }

    fn scan(&mut self, runner: &Runner, now: DateTime<Utc>) -> Result<()> {
        let failed: HashSet<(String, String)> = runner
            .state
            .import_failures()?
            .into_iter()
            .filter(|failure| now - failure.at < Duration::hours(IMPORT_RETRY_HOURS))
            .map(|failure| (failure.destination, failure.archive))
            .collect();
        for (destination, _) in destinations_with_peers(&runner.config) {
            match destination::legacy(&destination) {
                Ok(names) => {
                    for name in names {
                        if !failed.contains(&(destination.to_string(), name.clone())) {
                            self.imports.push_back((destination.clone(), name));
                        }
                    }
                }
                Err(error) => {
                    warn!(%destination, error = %format!("{error:#}"), "could not look for old archives to import");
                }
            }
        }
        if !self.imports.is_empty() {
            info!(
                archives = self.imports.len(),
                "found old archives to import"
            );
        }
        Ok(())
    }
}

// A destination seen for the first time starts its clock now, so it is
// verified a week later rather than at once.
fn due_verification(
    runner: &Runner,
    now: DateTime<Utc>,
) -> Result<Option<(Location, Vec<Location>)>> {
    for (destination, peers) in destinations_with_peers(&runner.config) {
        match runner.state.last_verified(&destination)? {
            None => runner.state.set_last_verified(&destination, now)?,
            Some(last) if now - last >= Duration::days(VERIFY_EVERY_DAYS) => {
                return Ok(Some((destination, peers)));
            }
            Some(_) => {}
        }
    }
    Ok(None)
}

fn verify(
    runner: &mut Runner,
    destination: &Location,
    peers: &[Location],
    now: DateTime<Utc>,
) -> Result<()> {
    let operation_lock = AppLock::exclusive(&runner.paths.operation_lock)?;
    info!(%destination, "starting the weekly verify");
    let outcome = verify_destination(destination, peers);
    drop(operation_lock);
    // The attempt is recorded whatever the result, so a destination that
    // cannot be read is retried next week, not every second. What went wrong
    // stays in the state database for health to report.
    runner.state.set_last_verified(destination, now)?;
    match outcome {
        Ok(outcome) if outcome.problems.is_empty() => {
            runner.state.set_verify_problem(destination, None)?;
            info!(%destination, packs = outcome.check.packs, repaired = outcome.repaired, "weekly verify finished");
        }
        Ok(outcome) => {
            let problem = outcome.problems.join("; ");
            error!(%destination, %problem, "weekly verify left damage it could not repair");
            runner
                .state
                .set_verify_problem(destination, Some(&problem))?;
        }
        Err(verify_error) => {
            let problem = format!("verify failed: {verify_error:#}");
            error!(%destination, %problem, "weekly verify failed");
            runner
                .state
                .set_verify_problem(destination, Some(&problem))?;
        }
    }
    Ok(())
}

fn import(runner: &mut Runner, destination: &Location, name: &str) -> Result<()> {
    let operation_lock = AppLock::exclusive(&runner.paths.operation_lock)?;
    let imported = destination::import(destination, name);
    drop(operation_lock);
    match imported {
        Ok(report) => {
            info!(%destination, archive = report.name, size = report.size, "imported an old archive");
            runner.state.clear_import_failure(destination, name)?;
        }
        Err(import_error) => {
            let message = format!("{import_error:#}");
            error!(%destination, archive = name, error = %message, "could not import an old archive; it is kept as it is");
            runner
                .state
                .record_import_failure(destination, name, &message)?;
        }
    }
    Ok(())
}
