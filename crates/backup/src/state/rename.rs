//! Moves the state of a job to a new job name. Runs and their deliveries get
//! the new name and folders. The rows the daemon reads to decide what is due,
//! the schedule, the backup retry and the verify times, are copied and not
//! moved: the daemon keeps the old job until the configuration names the new
//! one, and neither name may look due in between.

use anyhow::Result;
use redb::{ReadableTable, WriteTransaction};

use super::State;
use super::records::RunRecord;
use super::store::{
    DELIVERIES, IMPORT_FAILURES, RETRIES, RUNS, SCHEDULES, VERIFIED, VERIFY_PROBLEMS, decode,
    encode,
};
use crate::archive::rename_archive;
use crate::location::Location;

impl State {
    /// `moves` pairs each old destination of the job with its new one. A run
    /// again after a stop changes nothing a first run already changed.
    pub fn rename_job(&self, old: &str, new: &str, moves: &[(Location, Location)]) -> Result<()> {
        self.store.write(|transaction| {
            let renamed = rename_runs(transaction, old, new, moves)?;
            move_deliveries(transaction, &renamed, moves)?;
            copy_job_rows(transaction, old, new)?;
            copy_destination_rows(transaction, old, new, moves)
        })
    }

    /// Whether a run of the job still waits for a destination.
    pub fn has_pending(&self, job: &str) -> Result<bool> {
        Ok(self
            .status()?
            .iter()
            .any(|line| line.job == job && line.pending_destinations > 0))
    }
}

fn rename_runs(
    transaction: &WriteTransaction,
    old: &str,
    new: &str,
    moves: &[(Location, Location)],
) -> Result<Vec<String>> {
    let found: Vec<(String, RunRecord)> = {
        let table = transaction.open_table(RUNS)?;
        let mut found = Vec::new();
        for entry in table.iter()? {
            let (key, value) = entry?;
            let record: RunRecord = decode(value.value())?;
            if record.job.name == old {
                found.push((key.value().to_owned(), record));
            }
        }
        found
    };
    let mut table = transaction.open_table(RUNS)?;
    let mut renamed = Vec::new();
    for (key, mut record) in found {
        record.job.name = new.to_owned();
        for destination in &mut record.job.destinations {
            if let Some((_, to)) = moves.iter().find(|(from, _)| from == destination) {
                *destination = to.clone();
            }
        }
        if let Some(name) = rename_archive(&record.archive_name, old, new) {
            record.archive_name = name;
        }
        table.insert(key.as_str(), encode(&record)?.as_str())?;
        renamed.push(key);
    }
    Ok(renamed)
}

fn move_deliveries(
    transaction: &WriteTransaction,
    runs: &[String],
    moves: &[(Location, Location)],
) -> Result<()> {
    let mut table = transaction.open_table(DELIVERIES)?;
    for run in runs {
        for (from, to) in moves.iter().filter(|(from, to)| from != to) {
            let (from, to) = (from.to_string(), to.to_string());
            let removed = table
                .remove((run.as_str(), from.as_str()))?
                .map(|value| value.value().to_owned());
            if let Some(value) = removed {
                table.insert((run.as_str(), to.as_str()), value.as_str())?;
            }
        }
    }
    Ok(())
}

fn copy_job_rows(transaction: &WriteTransaction, old: &str, new: &str) -> Result<()> {
    let mut schedules = transaction.open_table(SCHEDULES)?;
    let last = schedules.get(old)?.map(|value| value.value());
    let present = schedules.get(new)?.is_some();
    if let Some(last) = last
        && !present
    {
        schedules.insert(new, last)?;
    }
    let mut retries = transaction.open_table(RETRIES)?;
    let retry = retries.get(old)?.map(|value| value.value().to_owned());
    let present = retries.get(new)?.is_some();
    if let Some(retry) = retry
        && !present
    {
        retries.insert(new, retry.as_str())?;
    }
    Ok(())
}

fn copy_destination_rows(
    transaction: &WriteTransaction,
    old: &str,
    new: &str,
    moves: &[(Location, Location)],
) -> Result<()> {
    let mut verified = transaction.open_table(VERIFIED)?;
    let mut problems = transaction.open_table(VERIFY_PROBLEMS)?;
    let mut failures = transaction.open_table(IMPORT_FAILURES)?;
    for (from, to) in moves.iter().filter(|(from, to)| from != to) {
        let (from, to) = (from.to_string(), to.to_string());
        let at = verified.get(from.as_str())?.map(|value| value.value());
        let present = verified.get(to.as_str())?.is_some();
        if let Some(at) = at
            && !present
        {
            verified.insert(to.as_str(), at)?;
        }
        let problem = problems
            .get(from.as_str())?
            .map(|value| value.value().to_owned());
        let present = problems.get(to.as_str())?.is_some();
        if let Some(problem) = problem
            && !present
        {
            problems.insert(to.as_str(), problem.as_str())?;
        }
        let mut found = Vec::new();
        for entry in failures.range((from.as_str(), "")..(from.as_str(), "\u{10FFFF}"))? {
            let (key, value) = entry?;
            found.push((key.value().1.to_owned(), value.value().to_owned()));
        }
        for (archive, failure) in found {
            let archive = rename_archive(&archive, old, new).unwrap_or(archive);
            let present = failures.get((to.as_str(), archive.as_str()))?.is_some();
            if !present {
                failures.insert((to.as_str(), archive.as_str()), failure.as_str())?;
            }
        }
    }
    Ok(())
}
