use std::path::{Path, PathBuf};
use std::str::FromStr;

use anyhow::{Context, Result};
use chrono::{DateTime, Duration, Utc};
use rusqlite::{Connection, OptionalExtension, Result as SqlResult, params};
use serde_json::{from_str, to_string};
use uuid::Uuid;

use crate::archive::Artifact;
use crate::config::BackupJob;
use crate::location::Location;

const RETRY_SECONDS: [i64; 4] = [60, 300, 900, 3600];

pub struct State {
    connection: Connection,
}

#[derive(Clone, Debug)]
pub struct PendingDelivery {
    pub run_id: Uuid,
    pub artifact: Artifact,
    pub job: BackupJob,
    pub destination: Location,
    pub attempts: u32,
}

#[derive(Clone, Debug)]
pub struct CompletedRun {
    pub run_id: Uuid,
    pub archive: PathBuf,
    pub checksum: PathBuf,
}

#[derive(Clone, Debug)]
pub struct StatusLine {
    pub job: String,
    pub archive: String,
    pub created_at: DateTime<Utc>,
    pub pending_destinations: usize,
}

impl State {
    pub fn open(path: &Path) -> Result<Self> {
        let connection = Connection::open(path)
            .with_context(|| format!("open state database {}", path.display()))?;
        connection.pragma_update(None, "journal_mode", "WAL")?;
        connection.pragma_update(None, "foreign_keys", true)?;
        connection.execute_batch(
            r#"
CREATE TABLE IF NOT EXISTS schedules (
    job TEXT PRIMARY KEY,
    last_scheduled INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS runs (
    id TEXT PRIMARY KEY,
    job TEXT NOT NULL,
    job_json TEXT NOT NULL,
    archive_name TEXT NOT NULL,
    archive_path TEXT NOT NULL,
    checksum_path TEXT NOT NULL,
    checksum TEXT NOT NULL,
    size INTEGER NOT NULL,
    created_at INTEGER NOT NULL,
    status TEXT NOT NULL CHECK (status IN ('pending', 'complete'))
);

CREATE TABLE IF NOT EXISTS deliveries (
    run_id TEXT NOT NULL REFERENCES runs(id) ON DELETE CASCADE,
    destination TEXT NOT NULL,
    status TEXT NOT NULL CHECK (status IN ('pending', 'delivered')),
    attempts INTEGER NOT NULL,
    next_retry INTEGER NOT NULL,
    last_error TEXT,
    PRIMARY KEY (run_id, destination)
);

CREATE INDEX IF NOT EXISTS deliveries_due
ON deliveries(status, next_retry);
"#,
        )?;
        Ok(Self { connection })
    }

    pub fn register_run(&mut self, artifact: &Artifact, job: &BackupJob) -> Result<Uuid> {
        let run_id = Uuid::new_v4();
        let job_json = to_string(job)?;
        let size =
            i64::try_from(artifact.size).context("archive is too large for state database")?;
        let transaction = self.connection.transaction()?;
        transaction.execute(
            r#"INSERT INTO runs (
id, job, job_json, archive_name, archive_path, checksum_path,
checksum, size, created_at, status
) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, 'pending')"#,
            params![
                run_id.to_string(),
                job.name,
                job_json,
                artifact.name,
                artifact.path.to_string_lossy(),
                artifact.checksum_path.to_string_lossy(),
                artifact.checksum,
                size,
                artifact.created_at.timestamp(),
            ],
        )?;
        for destination in &job.destinations {
            transaction.execute(
                r#"INSERT INTO deliveries (
run_id, destination, status, attempts, next_retry
) VALUES (?1, ?2, 'pending', 0, ?3)"#,
                params![
                    run_id.to_string(),
                    destination.to_string(),
                    Utc::now().timestamp()
                ],
            )?;
        }
        transaction.commit()?;
        Ok(run_id)
    }

    pub fn has_archive(&self, path: &Path) -> Result<bool> {
        let found = self
            .connection
            .query_row(
                "SELECT 1 FROM runs WHERE archive_path = ?1 LIMIT 1",
                [path.to_string_lossy()],
                |row| row.get::<_, i64>(0),
            )
            .optional()?;
        Ok(found.is_some())
    }

    pub fn due_deliveries(&self, now: DateTime<Utc>) -> Result<Vec<PendingDelivery>> {
        let mut statement = self.connection.prepare(
            r#"SELECT
r.id, r.job_json, r.archive_name, r.archive_path, r.checksum_path,
r.checksum, r.size, r.created_at, d.destination, d.attempts
FROM deliveries d
JOIN runs r ON r.id = d.run_id
WHERE d.status = 'pending' AND d.next_retry <= ?1
ORDER BY r.created_at, d.destination"#,
        )?;
        let rows = statement.query_map([now.timestamp()], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)?,
                row.get::<_, String>(3)?,
                row.get::<_, String>(4)?,
                row.get::<_, String>(5)?,
                row.get::<_, i64>(6)?,
                row.get::<_, i64>(7)?,
                row.get::<_, String>(8)?,
                row.get::<_, u32>(9)?,
            ))
        })?;
        let mut deliveries = Vec::new();
        for row in rows {
            let (
                run_id,
                job_json,
                archive_name,
                archive_path,
                checksum_path,
                checksum,
                size,
                created_at,
                destination,
                attempts,
            ) = row?;
            deliveries.push(PendingDelivery {
                run_id: Uuid::parse_str(&run_id)?,
                artifact: Artifact {
                    name: archive_name,
                    path: PathBuf::from(archive_path),
                    checksum_path: PathBuf::from(checksum_path),
                    checksum,
                    size: u64::try_from(size).context("state contains a negative archive size")?,
                    created_at: DateTime::from_timestamp(created_at, 0)
                        .context("state contains an invalid run timestamp")?,
                },
                job: from_str(&job_json)?,
                destination: Location::from_str(&destination)?,
                attempts,
            });
        }
        Ok(deliveries)
    }

    pub fn deliveries_for_run(&self, run_id: Uuid) -> Result<Vec<PendingDelivery>> {
        let mut statement = self.connection.prepare(
            r#"SELECT
r.id, r.job_json, r.archive_name, r.archive_path, r.checksum_path,
r.checksum, r.size, r.created_at, d.destination, d.attempts
FROM deliveries d
JOIN runs r ON r.id = d.run_id
WHERE d.status = 'pending' AND d.run_id = ?1
ORDER BY d.destination"#,
        )?;
        let rows = statement.query_map([run_id.to_string()], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)?,
                row.get::<_, String>(3)?,
                row.get::<_, String>(4)?,
                row.get::<_, String>(5)?,
                row.get::<_, i64>(6)?,
                row.get::<_, i64>(7)?,
                row.get::<_, String>(8)?,
                row.get::<_, u32>(9)?,
            ))
        })?;
        let mut deliveries = Vec::new();
        for row in rows {
            let (
                run_id,
                job_json,
                archive_name,
                archive_path,
                checksum_path,
                checksum,
                size,
                created_at,
                destination,
                attempts,
            ) = row?;
            deliveries.push(PendingDelivery {
                run_id: Uuid::parse_str(&run_id)?,
                artifact: Artifact {
                    name: archive_name,
                    path: PathBuf::from(archive_path),
                    checksum_path: PathBuf::from(checksum_path),
                    checksum,
                    size: u64::try_from(size).context("state contains a negative archive size")?,
                    created_at: DateTime::from_timestamp(created_at, 0)
                        .context("state contains an invalid run timestamp")?,
                },
                job: from_str(&job_json)?,
                destination: Location::from_str(&destination)?,
                attempts,
            });
        }
        Ok(deliveries)
    }

    pub fn mark_delivered(&self, run_id: Uuid, destination: &Location) -> Result<()> {
        self.connection.execute(
            r#"UPDATE deliveries
SET status = 'delivered', last_error = NULL
WHERE run_id = ?1 AND destination = ?2"#,
            params![run_id.to_string(), destination.to_string()],
        )?;
        Ok(())
    }

    pub fn mark_delivery_failed(
        &self,
        run_id: Uuid,
        destination: &Location,
        attempts: u32,
        error: &str,
    ) -> Result<DateTime<Utc>> {
        let index = usize::try_from(attempts).unwrap_or(usize::MAX);
        let seconds = RETRY_SECONDS[index.min(RETRY_SECONDS.len() - 1)];
        let retry_at = Utc::now() + Duration::seconds(seconds);
        self.connection.execute(
            r#"UPDATE deliveries
SET attempts = attempts + 1, next_retry = ?3, last_error = ?4
WHERE run_id = ?1 AND destination = ?2"#,
            params![
                run_id.to_string(),
                destination.to_string(),
                retry_at.timestamp(),
                error
            ],
        )?;
        Ok(retry_at)
    }

    pub fn complete_ready_runs(&self) -> Result<Vec<CompletedRun>> {
        let mut statement = self.connection.prepare(
            r#"SELECT r.id, r.archive_path, r.checksum_path
FROM runs r
WHERE r.status = 'pending'
AND NOT EXISTS (
    SELECT 1 FROM deliveries d
    WHERE d.run_id = r.id AND d.status != 'delivered'
)"#,
        )?;
        let rows = statement
            .query_map([], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, String>(2)?,
                ))
            })?
            .collect::<SqlResult<Vec<_>>>()?;
        rows.into_iter()
            .map(|(run_id, archive, checksum)| {
                Ok(CompletedRun {
                    run_id: Uuid::parse_str(&run_id)?,
                    archive: PathBuf::from(archive),
                    checksum: PathBuf::from(checksum),
                })
            })
            .collect()
    }

    pub fn mark_run_complete(&self, run_id: Uuid) -> Result<()> {
        self.connection.execute(
            "UPDATE runs SET status = 'complete' WHERE id = ?1",
            [run_id.to_string()],
        )?;
        Ok(())
    }

    pub fn last_scheduled(&self, job: &str) -> Result<Option<DateTime<Utc>>> {
        let timestamp = self
            .connection
            .query_row(
                "SELECT last_scheduled FROM schedules WHERE job = ?1",
                [job],
                |row| row.get::<_, i64>(0),
            )
            .optional()?;
        timestamp
            .map(|value| {
                DateTime::from_timestamp(value, 0)
                    .context("state contains an invalid schedule timestamp")
            })
            .transpose()
    }

    pub fn set_last_scheduled(&self, job: &str, value: DateTime<Utc>) -> Result<()> {
        self.connection.execute(
            r#"INSERT INTO schedules (job, last_scheduled) VALUES (?1, ?2)
ON CONFLICT(job) DO UPDATE SET last_scheduled = excluded.last_scheduled"#,
            params![job, value.timestamp()],
        )?;
        Ok(())
    }

    pub fn status(&self) -> Result<Vec<StatusLine>> {
        let mut statement = self.connection.prepare(
            r#"SELECT
r.job, r.archive_name, r.created_at,
SUM(CASE WHEN d.status = 'pending' THEN 1 ELSE 0 END)
FROM runs r
JOIN deliveries d ON d.run_id = r.id
WHERE r.status = 'pending'
GROUP BY r.id
ORDER BY r.created_at"#,
        )?;
        let rows = statement.query_map([], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, i64>(2)?,
                row.get::<_, i64>(3)?,
            ))
        })?;
        let mut status = Vec::new();
        for row in rows {
            let (job, archive, created_at, pending_destinations) = row?;
            status.push(StatusLine {
                job,
                archive,
                created_at: DateTime::from_timestamp(created_at, 0)
                    .context("state contains an invalid run timestamp")?,
                pending_destinations: usize::try_from(pending_destinations)
                    .context("state contains a negative pending destination count")?,
            });
        }
        Ok(status)
    }
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use chrono::{Duration, Timelike, Utc};
    use tempfile::tempdir;

    use super::State;
    use crate::archive::Artifact;
    use crate::config::BackupJob;
    use crate::location::Location;

    #[test]
    fn tracks_each_destination_and_completes_only_after_all_deliveries() {
        let temporary = tempdir().unwrap();
        let mut state = State::open(&temporary.path().join("state.sqlite3")).unwrap();
        let first = Location::Local(PathBuf::from("/first"));
        let second = Location::Local(PathBuf::from("/second"));
        let job = BackupJob {
            name: "documents".to_owned(),
            source: Location::Local(PathBuf::from("/source")),
            destinations: vec![first.clone(), second.clone()],
            cron: "0 2 * * *".to_owned(),
            retention: None,
            exclude: Vec::new(),
        };
        let artifact = Artifact {
            name: "documents-archive.tar".to_owned(),
            path: temporary.path().join("archive.tar"),
            checksum_path: temporary.path().join("archive.tar.blake3"),
            checksum: "abc".to_owned(),
            size: 12,
            created_at: Utc::now(),
        };
        let run_id = state.register_run(&artifact, &job).unwrap();
        let due = state.due_deliveries(Utc::now()).unwrap();
        assert_eq!(due.len(), 2);

        state.mark_delivered(run_id, &first).unwrap();
        assert!(state.complete_ready_runs().unwrap().is_empty());
        let retry_at = state
            .mark_delivery_failed(run_id, &second, 0, "offline")
            .unwrap();
        assert!(
            state
                .due_deliveries(retry_at - Duration::seconds(1))
                .unwrap()
                .is_empty()
        );
        let retry = state.due_deliveries(retry_at).unwrap();
        assert_eq!(retry.len(), 1);
        assert_eq!(retry[0].destination, second);
        assert_eq!(retry[0].attempts, 1);

        state.mark_delivered(run_id, &second).unwrap();
        let ready = state.complete_ready_runs().unwrap();
        assert_eq!(ready.len(), 1);
        assert_eq!(ready[0].run_id, run_id);
        state.mark_run_complete(run_id).unwrap();
        assert!(state.status().unwrap().is_empty());
    }

    #[test]
    fn stores_last_handled_schedule_in_utc() {
        let temporary = tempdir().unwrap();
        let state = State::open(&temporary.path().join("state.sqlite3")).unwrap();
        let scheduled = Utc::now()
            .with_nanosecond(0)
            .expect("valid timestamp without nanoseconds");
        state.set_last_scheduled("documents", scheduled).unwrap();
        assert_eq!(state.last_scheduled("documents").unwrap(), Some(scheduled));
    }
}
