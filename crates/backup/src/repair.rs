//! Copying a backup between destinations and repairing a destination from
//! the others. Both read chunks with fallback, so one damaged or missing chunk
//! in one place is taken from the next.

use std::collections::HashSet;

use anyhow::{Result, anyhow, bail};
use serde::Serialize;
use tracing::{info, warn};

use crate::config::{BackupJob, Config};
use crate::destination::{self, Repair, open_source};
use crate::location::Location;
use crate::store::check::StoreCheck;
use crate::store::digest::Digest;
use crate::store::recipe::Recipe;
use crate::stream::{ChunkSource, SourceRef, read_any};
use crate::transfer::open_sink;

/// Copies one backup into `target` from any other destination of the job.
pub fn copy_archive(job: &BackupJob, name: &str, target: &Location) -> Result<()> {
    let others: Vec<Location> = job
        .destinations
        .iter()
        .filter(|destination| *destination != target)
        .cloned()
        .collect();
    let (recipe, order) = find_recipe(&others, name)?;
    let mut sink = open_sink(target, job)?;
    let needed = unique_missing(&recipe, sink.known());
    let mut sources = open_sources(&order, &needed)?;
    let mut sources: Vec<SourceRef<'_>> =
        sources.iter_mut().map(|source| source.as_mut()).collect();
    for id in &needed {
        let copied = read_any(&mut sources, id).and_then(|bytes| sink.put(id, &bytes));
        if let Err(error) = copied {
            return Err(sink.abort().map_or(error, |detail| anyhow!(detail)));
        }
    }
    sink.finish(&recipe)?;
    info!(job = job.name, archive = name, destination = %target, chunks = needed.len(), "copied a backup from another destination");
    Ok(())
}

/// The recipe from the first destination that can read it, and the
/// destinations to read chunks from, that one first.
fn find_recipe(destinations: &[Location], name: &str) -> Result<(Recipe, Vec<Location>)> {
    let mut failures = Vec::new();
    for (index, destination) in destinations.iter().enumerate() {
        match destination::read_recipe(destination, name) {
            Ok(recipe) => {
                let mut order = vec![destination.clone()];
                order.extend(
                    destinations
                        .iter()
                        .enumerate()
                        .filter(|(other, _)| *other != index)
                        .map(|(_, other)| other.clone()),
                );
                return Ok((recipe, order));
            }
            Err(error) => failures.push(format!("{destination}: {error:#}")),
        }
    }
    if failures.is_empty() {
        bail!("no other destination holds {name}");
    }
    bail!("no destination could read {name}: {}", failures.join("; "))
}

fn unique_missing(recipe: &Recipe, known: &HashSet<Digest>) -> Vec<Digest> {
    let mut seen = HashSet::new();
    recipe
        .chunks
        .iter()
        .map(|chunk| chunk.id)
        .filter(|id| !known.contains(id) && seen.insert(*id))
        .collect()
}

/// Sources for every destination that opens, the first one set up to
/// stream `order`.
pub fn open_sources(
    destinations: &[Location],
    order: &[Digest],
) -> Result<Vec<Box<dyn ChunkSource>>> {
    let mut sources = Vec::new();
    let mut failures = Vec::new();
    for destination in destinations {
        let order = if sources.is_empty() { order } else { &[] };
        match open_source(destination, order) {
            Ok(source) => sources.push(source),
            Err(error) => {
                warn!(%destination, error = %format!("{error:#}"), "could not open a destination to read from");
                failures.push(format!("{destination}: {error:#}"));
            }
        }
    }
    if sources.is_empty() {
        bail!("no destination could be opened: {}", failures.join("; "));
    }
    Ok(sources)
}

#[derive(Debug, Default, Serialize)]
pub struct VerifyOutcome {
    pub check: StoreCheck,
    pub repaired: bool,
    pub problems: Vec<String>,
}

/// Reads everything in a destination. Damage its own parity cannot fix is
/// repaired from `peers`, the other destinations of the jobs that use it,
/// and the destination is read again to confirm the repair.
pub fn verify_destination(target: &Location, peers: &[Location]) -> Result<VerifyOutcome> {
    let check = destination::check(target)?;
    if check.is_clean() {
        return Ok(VerifyOutcome {
            check,
            ..VerifyOutcome::default()
        });
    }
    warn!(
        destination = %target,
        damaged_packs = check.damaged_packs.len(),
        bad_chunks = check.bad_chunks.len(),
        bad_recipes = check.bad_recipes.len(),
        "verify found damage; repairing from the other destinations"
    );
    let mut problems = Vec::new();
    if (!check.bad_chunks.is_empty() || !check.damaged_packs.is_empty())
        && let Err(error) = repair_chunks(target, peers, &check)
    {
        problems.push(format!("chunk repair failed: {error:#}"));
    }
    for name in &check.bad_recipes {
        let restored = find_recipe(peers, name).and_then(|(recipe, order)| {
            let needed = unique_missing(&recipe, &destination::chunk_ids(target)?);
            let mut sources = open_sources(&order, &needed)?;
            let mut sources: Vec<SourceRef<'_>> =
                sources.iter_mut().map(|source| source.as_mut()).collect();
            let mut repair = Repair::open(target, &[], &[recipe])?;
            for id in &needed {
                let bytes = read_any(&mut sources, id)?;
                repair.put(id, &bytes)?;
            }
            repair.finish()
        });
        if let Err(error) = restored {
            problems.push(format!("recipe {name} could not be restored: {error:#}"));
        }
    }
    let after = destination::check(target)?;
    for name in after.incomplete.iter().chain(&after.bad_recipes) {
        problems.push(format!("backup {name} is still damaged at {target}"));
    }
    if !after.damaged_packs.is_empty() {
        problems.push(format!(
            "{} damaged pack(s) remain at {target}",
            after.damaged_packs.len()
        ));
    }
    Ok(VerifyOutcome {
        check: after,
        repaired: true,
        problems,
    })
}

fn repair_chunks(target: &Location, peers: &[Location], check: &StoreCheck) -> Result<()> {
    let mut repair = Repair::open(target, &check.damaged_packs, &[])?;
    let mut sources = open_sources(peers, &check.bad_chunks)?;
    let mut sources: Vec<SourceRef<'_>> =
        sources.iter_mut().map(|source| source.as_mut()).collect();
    let mut failures = Vec::new();
    for id in &check.bad_chunks {
        match read_any(&mut sources, id) {
            Ok(bytes) => repair.put(id, &bytes)?,
            Err(error) => failures.push(format!("{error:#}")),
        }
    }
    repair.finish()?;
    if !failures.is_empty() {
        bail!(
            "{} chunk(s) have no good copy: {}",
            failures.len(),
            failures.join("; ")
        );
    }
    Ok(())
}

/// Every destination in the configuration, each with the other destinations
/// of every job that uses it.
pub fn destinations_with_peers(config: &Config) -> Vec<(Location, Vec<Location>)> {
    let mut found: Vec<(Location, Vec<Location>)> = Vec::new();
    for job in &config.jobs {
        for destination in &job.destinations {
            let index = match found.iter().position(|(known, _)| known == destination) {
                Some(index) => index,
                None => {
                    found.push((destination.clone(), Vec::new()));
                    found.len() - 1
                }
            };
            for peer in &job.destinations {
                if peer != destination && !found[index].1.contains(peer) {
                    found[index].1.push(peer.clone());
                }
            }
        }
    }
    found
}
