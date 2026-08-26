mod records;
mod store;

#[cfg(test)]
mod tests;

use std::cmp::Reverse;
use std::path::Path;
use std::str::FromStr;

use anyhow::{Context, Result};
use chrono::{DateTime, Duration, Utc};
use uuid::Uuid;

use crate::archive::Artifact;
use crate::config::BackupJob;
use crate::location::Location;

pub use records::{
    BackupRetry, CompletedRun, DeliveryResult, DeliveryStatus, HistoryLine, PendingDelivery,
    StatusLine,
};
use records::{DeliveryRecord, RetryRecord, RunRecord};
use redb::ReadableTable;

use store::{DELIVERIES, RETRIES, RUNS, SCHEDULES, Store, decode, deliveries, encode, runs};

const RETRY_SECONDS: [i64; 4] = [60, 300, 900, 3600];

fn retry_delay(previous_attempts: u32) -> i64 {
    let index = usize::try_from(previous_attempts).unwrap_or(usize::MAX);
    RETRY_SECONDS[index.min(RETRY_SECONDS.len() - 1)]
}

fn timestamp(value: i64, what: &str) -> Result<DateTime<Utc>> {
    DateTime::from_timestamp(value, 0).with_context(|| format!("state contains an invalid {what}"))
}

pub struct State {
    store: Store,
}

impl State {
    pub fn open(path: &Path) -> Result<Self> {
        Ok(Self {
            store: Store::open(path)?,
        })
    }

    pub fn register_run(
        &mut self,
        artifact: &Artifact,
        job: &BackupJob,
        staged: bool,
        results: &[DeliveryResult],
    ) -> Result<Uuid> {
        let run_id = Uuid::new_v4();
        let record = RunRecord {
            job: job.clone(),
            archive_name: artifact.name.clone(),
            archive_path: artifact.path.clone(),
            checksum_path: artifact.checksum_path.clone(),
            checksum: artifact.checksum.clone(),
            size: artifact.size,
            created_at: artifact.created_at.timestamp(),
            staged,
            completed_at: None,
        };
        let now = Utc::now().timestamp();
        let key = run_id.to_string();
        self.store.write(|transaction| {
            transaction
                .open_table(RUNS)?
                .insert(key.as_str(), encode(&record)?.as_str())?;
            let mut table = transaction.open_table(DELIVERIES)?;
            for destination in &job.destinations {
                let status = results
                    .iter()
                    .find(|result| result.destination == *destination)
                    .map_or(DeliveryStatus::Pending, |result| result.status.clone());
                let delivery = match status {
                    DeliveryStatus::Delivered => DeliveryRecord {
                        delivered: true,
                        attempts: 1,
                        next_retry: now,
                        last_error: None,
                    },
                    DeliveryStatus::Failed(error) => DeliveryRecord {
                        delivered: false,
                        attempts: 1,
                        next_retry: now + retry_delay(0),
                        last_error: Some(error),
                    },
                    DeliveryStatus::Pending => DeliveryRecord {
                        delivered: false,
                        attempts: 0,
                        next_retry: now,
                        last_error: None,
                    },
                };
                table.insert(
                    (key.as_str(), destination.to_string().as_str()),
                    encode(&delivery)?.as_str(),
                )?;
            }
            Ok(())
        })?;
        Ok(run_id)
    }

    pub fn has_archive(&self, path: &Path) -> Result<bool> {
        self.store.read(|transaction| {
            for (_, value) in runs(transaction)? {
                let record: RunRecord = decode(&value)?;
                if record.archive_path == path {
                    return Ok(true);
                }
            }
            Ok(false)
        })
    }

    pub fn due_deliveries(&self, now: DateTime<Utc>) -> Result<Vec<PendingDelivery>> {
        self.pending(|_, delivery| delivery.next_retry <= now.timestamp())
    }

    pub fn next_due(&self) -> Result<Option<DateTime<Utc>>> {
        self.store.read(|transaction| {
            let mut earliest: Option<i64> = None;
            for (_, value) in deliveries(transaction)? {
                let delivery: DeliveryRecord = decode(&value)?;
                if delivery.delivered {
                    continue;
                }
                earliest = Some(earliest.map_or(delivery.next_retry, |current| {
                    current.min(delivery.next_retry)
                }));
            }
            earliest
                .map(|value| timestamp(value, "delivery retry timestamp"))
                .transpose()
        })
    }

    pub fn deliveries_for_run(&self, run_id: Uuid) -> Result<Vec<PendingDelivery>> {
        let wanted = run_id.to_string();
        self.pending(move |id, _| id == wanted)
    }

    fn pending(
        &self,
        keep: impl Fn(&str, &DeliveryRecord) -> bool,
    ) -> Result<Vec<PendingDelivery>> {
        self.store.read(|transaction| {
            let mut found = Vec::new();
            let runs: Vec<_> = runs(transaction)?;
            for ((run_id, destination), value) in deliveries(transaction)? {
                let delivery: DeliveryRecord = decode(&value)?;
                if delivery.delivered || !keep(&run_id, &delivery) {
                    continue;
                }
                let Some((_, run)) = runs.iter().find(|(id, _)| *id == run_id) else {
                    continue;
                };
                let record: RunRecord = decode(run)?;
                found.push(PendingDelivery {
                    run_id: Uuid::parse_str(&run_id)?,
                    artifact: Artifact {
                        name: record.archive_name,
                        path: record.archive_path,
                        checksum_path: record.checksum_path,
                        checksum: record.checksum,
                        size: record.size,
                        created_at: timestamp(record.created_at, "run timestamp")?,
                    },
                    job: record.job,
                    destination: Location::from_str(&destination)?,
                    attempts: delivery.attempts,
                });
            }
            found.sort_by(|left, right| {
                left.artifact
                    .created_at
                    .cmp(&right.artifact.created_at)
                    .then_with(|| {
                        left.destination
                            .to_string()
                            .cmp(&right.destination.to_string())
                    })
            });
            Ok(found)
        })
    }

    pub fn mark_delivered(&self, run_id: Uuid, destination: &Location) -> Result<()> {
        self.update_delivery(run_id, destination, |delivery| {
            delivery.delivered = true;
            delivery.last_error = None;
        })
    }

    pub fn mark_delivery_failed(
        &self,
        run_id: Uuid,
        destination: &Location,
        attempts: u32,
        error: &str,
    ) -> Result<DateTime<Utc>> {
        let retry_at = Utc::now() + Duration::seconds(retry_delay(attempts));
        self.update_delivery(run_id, destination, |delivery| {
            delivery.attempts += 1;
            delivery.next_retry = retry_at.timestamp();
            delivery.last_error = Some(error.to_owned());
        })?;
        Ok(retry_at)
    }

    fn update_delivery(
        &self,
        run_id: Uuid,
        destination: &Location,
        change: impl FnOnce(&mut DeliveryRecord),
    ) -> Result<()> {
        let run = run_id.to_string();
        let target = destination.to_string();
        self.store.write(|transaction| {
            let mut table = transaction.open_table(DELIVERIES)?;
            let key = (run.as_str(), target.as_str());
            let Some(existing) = table.get(key)? else {
                return Ok(());
            };
            let mut delivery: DeliveryRecord = decode(existing.value())?;
            drop(existing);
            change(&mut delivery);
            table.insert(key, encode(&delivery)?.as_str())?;
            Ok(())
        })
    }

    pub fn complete_ready_runs(&self) -> Result<Vec<CompletedRun>> {
        self.store.read(|transaction| {
            let deliveries = deliveries(transaction)?;
            let mut ready = Vec::new();
            for (run_id, value) in runs(transaction)? {
                let record: RunRecord = decode(&value)?;
                if record.completed_at.is_some() {
                    continue;
                }
                let mut outstanding = false;
                for ((id, _), delivery) in &deliveries {
                    if *id != run_id {
                        continue;
                    }
                    let delivery: DeliveryRecord = decode(delivery)?;
                    if !delivery.delivered {
                        outstanding = true;
                        break;
                    }
                }
                if outstanding {
                    continue;
                }
                ready.push(CompletedRun {
                    run_id: Uuid::parse_str(&run_id)?,
                    archive: record.archive_path,
                    checksum: record.checksum_path,
                    staged: record.staged,
                });
            }
            Ok(ready)
        })
    }

    pub fn mark_run_complete(&self, run_id: Uuid) -> Result<()> {
        let key = run_id.to_string();
        self.store.write(|transaction| {
            let mut table = transaction.open_table(RUNS)?;
            let Some(existing) = table.get(key.as_str())? else {
                return Ok(());
            };
            let mut record: RunRecord = decode(existing.value())?;
            drop(existing);
            record.completed_at = Some(Utc::now().timestamp());
            table.insert(key.as_str(), encode(&record)?.as_str())?;
            Ok(())
        })
    }

    pub fn purge_completed(&self, before: DateTime<Utc>) -> Result<usize> {
        let cutoff = before.timestamp();
        self.store.write(|transaction| {
            let expired: Vec<String> = {
                let runs = transaction.open_table(RUNS)?;
                let mut expired = Vec::new();
                for entry in runs.iter()? {
                    let (key, value) = entry?;
                    let record: RunRecord = decode(value.value())?;
                    if record
                        .completed_at
                        .is_some_and(|completed| completed < cutoff)
                    {
                        expired.push(key.value().to_owned());
                    }
                }
                expired
            };
            let mut runs = transaction.open_table(RUNS)?;
            let mut deliveries = transaction.open_table(DELIVERIES)?;
            for run_id in &expired {
                runs.remove(run_id.as_str())?;
                let destinations: Vec<String> = deliveries
                    .range((run_id.as_str(), "")..(run_id.as_str(), "\u{10FFFF}"))?
                    .map(|entry| entry.map(|(key, _)| key.value().1.to_owned()))
                    .collect::<Result<_, _>>()?;
                for destination in destinations {
                    deliveries.remove((run_id.as_str(), destination.as_str()))?;
                }
            }
            Ok(expired.len())
        })
    }

    pub fn history(&self) -> Result<Vec<HistoryLine>> {
        self.store.read(|transaction| {
            let mut lines = Vec::new();
            for (_, value) in runs(transaction)? {
                let record: RunRecord = decode(&value)?;
                let Some(completed) = record.completed_at else {
                    continue;
                };
                lines.push(HistoryLine {
                    job: record.job.name,
                    archive: record.archive_name,
                    size: record.size,
                    created_at: timestamp(record.created_at, "run timestamp")?,
                    completed_at: timestamp(completed, "completion timestamp")?,
                });
            }
            lines.sort_by_key(|line| Reverse(line.completed_at));
            Ok(lines)
        })
    }

    pub fn last_scheduled(&self, job: &str) -> Result<Option<DateTime<Utc>>> {
        self.store.read(|transaction| {
            let table = transaction.open_table(SCHEDULES)?;
            let Some(value) = table.get(job)? else {
                return Ok(None);
            };
            Ok(Some(timestamp(value.value(), "schedule timestamp")?))
        })
    }

    pub fn set_last_scheduled(&self, job: &str, value: DateTime<Utc>) -> Result<()> {
        self.store.write(|transaction| {
            transaction
                .open_table(SCHEDULES)?
                .insert(job, value.timestamp())?;
            Ok(())
        })
    }

    pub fn backup_retry(&self, job: &str) -> Result<Option<BackupRetry>> {
        self.store.read(|transaction| {
            let table = transaction.open_table(RETRIES)?;
            let Some(value) = table.get(job)? else {
                return Ok(None);
            };
            let record: RetryRecord = decode(value.value())?;
            Ok(Some(BackupRetry {
                slot: timestamp(record.slot, "backup retry slot")?,
                attempts: record.attempts,
                next_retry: timestamp(record.next_retry, "backup retry timestamp")?,
            }))
        })
    }

    pub fn record_backup_failure(
        &self,
        job: &str,
        slot: DateTime<Utc>,
        previous_attempts: u32,
    ) -> Result<DateTime<Utc>> {
        let next_retry = Utc::now() + Duration::seconds(retry_delay(previous_attempts));
        let record = RetryRecord {
            slot: slot.timestamp(),
            attempts: previous_attempts + 1,
            next_retry: next_retry.timestamp(),
        };
        self.store.write(|transaction| {
            transaction
                .open_table(RETRIES)?
                .insert(job, encode(&record)?.as_str())?;
            Ok(())
        })?;
        Ok(next_retry)
    }

    pub fn clear_backup_retry(&self, job: &str) -> Result<()> {
        self.store.write(|transaction| {
            transaction.open_table(RETRIES)?.remove(job)?;
            Ok(())
        })
    }

    pub fn status(&self) -> Result<Vec<StatusLine>> {
        self.store.read(|transaction| {
            let deliveries = deliveries(transaction)?;
            let mut lines = Vec::new();
            for (run_id, value) in runs(transaction)? {
                let record: RunRecord = decode(&value)?;
                if record.completed_at.is_some() {
                    continue;
                }
                let mut pending_destinations = 0usize;
                for ((id, _), delivery) in &deliveries {
                    if *id != run_id {
                        continue;
                    }
                    let delivery: DeliveryRecord = decode(delivery)?;
                    if !delivery.delivered {
                        pending_destinations += 1;
                    }
                }
                lines.push(StatusLine {
                    job: record.job.name,
                    archive: record.archive_name,
                    created_at: timestamp(record.created_at, "run timestamp")?,
                    pending_destinations,
                });
            }
            lines.sort_by_key(|line| line.created_at);
            Ok(lines)
        })
    }
}
