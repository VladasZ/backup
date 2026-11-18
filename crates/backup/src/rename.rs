//! Renames a backup job: its store in every destination and its state. The
//! configuration is not written here, the program that owns it writes the new
//! name after this ran. Either name may be the one in the configuration, so a
//! rename that stopped halfway, or whose configuration was written already,
//! finishes when it runs again.

use anyhow::{Result, bail};
use serde::Serialize;
use tracing::info;

use crate::archive::parse_archive_name;
use crate::config::{Config, check_job_name};
use crate::destination;
use crate::location::Location;
use crate::state::State;

#[derive(Debug, Serialize)]
pub struct RenamedDestination {
    pub from: Location,
    pub to: Location,
    pub moved: bool,
    pub recipes: usize,
}

#[derive(Debug, Serialize)]
pub struct JobRename {
    pub job: String,
    pub renamed_to: String,
    pub destinations: Vec<RenamedDestination>,
}

pub fn rename(config: &Config, state: &State, old: &str, new: &str) -> Result<JobRename> {
    if old == new {
        bail!("the job is already named {new}");
    }
    check_job_name(new)?;
    let moves = moves(config, old, new)?;
    // A waiting delivery holds the old folder of its destination and would
    // write a store there again.
    if state.has_pending(old)? {
        bail!("job {old} has deliveries still waiting, let them finish or forget them first");
    }
    for (from, to) in &moves {
        for location in [from, to] {
            let waiting = destination::legacy(location)?
                .iter()
                .any(|name| parse_archive_name(name).is_some_and(|parsed| parsed.job == old));
            if waiting {
                bail!(
                    "{location} still holds old archives of {old} to import, try again after the import"
                );
            }
        }
    }
    let mut destinations = Vec::new();
    for (from, to) in &moves {
        let report = destination::rename(from, to, old, new)?;
        info!(job = old, renamed_to = new, from = %from, to = %to, moved = report.moved, recipes = report.recipes, "renamed a store");
        destinations.push(RenamedDestination {
            from: from.clone(),
            to: to.clone(),
            moved: report.moved,
            recipes: report.recipes,
        });
    }
    state.rename_job(old, new, &moves)?;
    Ok(JobRename {
        job: old.to_owned(),
        renamed_to: new.to_owned(),
        destinations,
    })
}

/// Each destination of the job paired with its folder under the new name. A
/// folder named after the job follows the name, a shared one stays.
fn moves(config: &Config, old: &str, new: &str) -> Result<Vec<(Location, Location)>> {
    let find = |name: &str| config.jobs.iter().find(|job| job.name == name);
    match (find(old), find(new)) {
        (Some(_), Some(_)) => bail!("the configuration has both {old} and {new}"),
        (None, None) => bail!("the configuration has neither {old} nor {new}"),
        (Some(job), None) => Ok(job
            .destinations
            .iter()
            .map(|destination| (destination.clone(), follow(destination, old, new)))
            .collect()),
        (None, Some(job)) => Ok(job
            .destinations
            .iter()
            .map(|destination| (follow(destination, new, old), destination.clone()))
            .collect()),
    }
}

fn follow(destination: &Location, from: &str, to: &str) -> Location {
    if destination.folder_name() == Some(from) {
        destination.sibling(to)
    } else {
        destination.clone()
    }
}

#[cfg(test)]
mod tests;
