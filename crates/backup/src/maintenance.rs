//! Work the daemon does between scheduled backups: a full verify of every
//! destination once a week, and importing archives from the old format one
//! at a time. Each step is short enough that a backup coming due waits at
//! most for one of them.

use std::collections::{HashMap, HashSet, VecDeque};

use anyhow::Result;
use chrono::{DateTime, Duration, Utc};
use tracing::{error, info, warn};

use crate::archive::parse_archive_name;
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

// The daemon is the only writer of verify times, so they are cached and idle
// ticks never open the state database.
#[derive(Default)]
pub struct Maintenance {
    imports: VecDeque<(Vec<Location>, String)>,
    next_scan: Option<DateTime<Utc>>,
    verified: HashMap<Location, DateTime<Utc>>,
}

impl Maintenance {
    /// Runs at most one verify or one import. Returns whether it did work.
    pub fn step(&mut self, runner: &mut Runner, now: DateTime<Utc>) -> Result<bool> {
        if let Some((destination, peers)) = self.due_verification(runner, now)? {
            verify(runner, &destination, &peers, now)?;
            self.verified.insert(destination, now);
            return Ok(true);
        }
        if self.imports.is_empty() && self.next_scan.is_none_or(|next| now >= next) {
            self.scan(runner, now)?;
            self.next_scan = Some(now + Duration::minutes(IMPORT_RESCAN_MINUTES));
        }
        let Some((destinations, name)) = self.imports.pop_front() else {
            return Ok(false);
        };
        import(runner, &destinations, &name)?;
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
        // The same archive usually sits in several destinations. They are
        // grouped by name so it is read once and written to all of them.
        let mut groups: Vec<(Vec<Location>, String)> = Vec::new();
        for (destination, _) in destinations_with_peers(&runner.config) {
            match destination::legacy(&destination) {
                Ok(names) => {
                    for name in names {
                        if failed.contains(&(destination.to_string(), name.clone())) {
                            continue;
                        }
                        match groups.iter_mut().find(|(_, known)| *known == name) {
                            Some((locations, _)) => locations.push(destination.clone()),
                            None => groups.push((vec![destination.clone()], name)),
                        }
                    }
                }
                Err(error) => {
                    warn!(%destination, error = %format!("{error:#}"), "could not look for old archives to import");
                }
            }
        }
        groups.sort_by_key(|(_, name)| created(name));
        self.imports.extend(groups);
        if !self.imports.is_empty() {
            info!(
                archives = self.imports.len(),
                "found old archives to import"
            );
        }
        Ok(())
    }
}

impl Maintenance {
    // A destination seen for the first time starts its clock now, so it is
    // verified a week later rather than at once.
    fn due_verification(
        &mut self,
        runner: &Runner,
        now: DateTime<Utc>,
    ) -> Result<Option<(Location, Vec<Location>)>> {
        for (destination, peers) in destinations_with_peers(&runner.config) {
            let last = match self.verified.get(&destination) {
                Some(last) => *last,
                None => {
                    let last = match runner.state.last_verified(&destination)? {
                        Some(last) => last,
                        None => {
                            runner.state.set_last_verified(&destination, now)?;
                            now
                        }
                    };
                    self.verified.insert(destination.clone(), last);
                    last
                }
            };
            if now - last >= Duration::days(VERIFY_EVERY_DAYS) {
                return Ok(Some((destination, peers)));
            }
        }
        Ok(None)
    }
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

fn import(runner: &mut Runner, destinations: &[Location], name: &str) -> Result<()> {
    let operation_lock = AppLock::exclusive(&runner.paths.operation_lock)?;
    let imported = destination::import(destinations, name);
    drop(operation_lock);
    match imported {
        Ok(report) => {
            info!(
                archive = report.name,
                size = report.size,
                destinations = destinations.len(),
                "imported an old archive"
            );
            for destination in destinations {
                runner.state.clear_import_failure(destination, name)?;
            }
        }
        Err(import_error) => {
            let message = format!("{import_error:#}");
            error!(archive = name, error = %message, "could not import an old archive; it is kept as it is");
            for destination in destinations {
                runner
                    .state
                    .record_import_failure(destination, name, &message)?;
            }
        }
    }
    Ok(())
}

fn created(name: &str) -> Option<DateTime<Utc>> {
    parse_archive_name(name).map(|parsed| parsed.created)
}
